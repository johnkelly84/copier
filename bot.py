import asyncio
import os
import sys
import time
from datetime import datetime
import redis

from flask import Flask
from threading import Thread
from waitress import serve

from pyrogram import Client, filters
from pyrogram.errors import exceptions
from dotenv import load_dotenv

# --- CONFIGURATION & INITIALIZATION ---
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID"))
REDIS_URL = os.getenv("REDIS_URL")
USER_SESSION_STRING = os.getenv("USER_SESSION_STRING")

# --- DUAL CLIENT SETUP ---
# The bot client that performs all actions
bot_app = Client("copier_bot_instance", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# The user client, used ONLY for looking up chats reliably.
# It authenticates using the session string, avoiding interactive login on the server.
user_client = Client("user_session_instance", session_string=USER_SESSION_STRING, api_id=API_ID, api_hash=API_HASH)

# --- STATE MANAGEMENT ---
active_jobs = {}
redis_client = None

def init_redis():
    global redis_client
    if not REDIS_URL:
        print("FATAL: REDIS_URL is not set.")
        sys.exit(1)
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
        print("INFO: Redis connection successful.")
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        sys.exit(1)

# ############################################################################
# --- COPIER CLASS (Now uses both clients) ---
# ############################################################################
class TelegramCopier:
    def __init__(self, bot_client: Client, user_client: Client, status_message, redis_conn):
        self.bot_client = bot_client
        self.user_client = user_client # <-- Store the user client
        self.status_message = status_message
        self.redis = redis_conn
        self.log_buffer = []
        self.last_update_time = 0

    # _log, _get_existing_mappings, _add_mapping methods are UNCHANGED
    async def _log(self, message):
        timestamp = datetime.now().strftime('%H:%M:%S')
        safe_message = message.replace('`', "'").replace('*', '').replace('_', '').replace('[', '(').replace(']', ')')
        self.log_buffer.append(f"{timestamp}: {safe_message}")
        if len(self.log_buffer) > 15: self.log_buffer.pop(0)
        current_time = time.time()
        if current_time - self.last_update_time > 3:
            try:
                log_text = "\n".join(self.log_buffer)
                await self.status_message.edit_text(f"Copy Job in Progress...\n\n---\n{log_text}", parse_mode=None)
            except exceptions.FloodWait as e:
                await asyncio.sleep(e.value)
            except Exception: pass
    
    def _get_existing_mappings(self, source_chat_id, ids_to_check):
        if not ids_to_check: return set()
        redis_set_key = f"copier_map:{source_chat_id}"
        pipe = self.redis.pipeline()
        for msg_id in ids_to_check: pipe.sismember(redis_set_key, str(msg_id))
        results = pipe.execute()
        return {ids_to_check[i] for i, exists in enumerate(results) if exists}

    def _add_mapping(self, source_chat_id, source_message_id):
        try:
            redis_set_key = f"copier_map:{source_chat_id}"
            self.redis.sadd(redis_set_key, str(source_message_id))
        except Exception as e: asyncio.create_task(self._log(f"Redis Error: {e}"))

    async def run_copy_task(self, source_input, target_input, start_id, end_id, delay):
        try:
            # Step 1: Use the USER CLIENT to reliably get chat objects
            await self._log("Resolving chats using user session...")
            try:
                # This is the key change. The user client will not fail with PEER_ID_INVALID.
                source_chat = await self.user_client.get_chat(source_input)
                target_chat = await self.user_client.get_chat(target_input)
            except Exception as e:
                await self._log(f"ERROR: Could not access chats via user session. Ensure your user account is in the private chats. Details: {e}")
                return

            await self._log(f"Source: {source_chat.title}")
            await self._log(f"Target: {target_chat.title}")

            # All subsequent operations use the BOT client
            if not target_chat.is_forum:
                await self._log("ERROR: Target chat must have Topics enabled.")
                return

            # Step 2: Find or Create Topic (using the BOT client)
            source_title = source_chat.title
            target_topic_id = None
            await self._log(f"Searching for topic: '{source_title}'...")
            async for topic in self.bot_client.get_forum_topics(target_chat.id):
                if topic.title == source_title:
                    target_topic_id = topic.id
                    await self._log(f"Found existing topic (ID: {topic.id})")
                    break
            if not target_topic_id:
                try:
                    new_topic = await self.bot_client.create_forum_topic(target_chat.id, source_title)
                    target_topic_id = new_topic.id
                    await self._log(f"Created new topic (ID: {target_topic_id})")
                except Exception as e:
                    await self._log(f"ERROR: Bot failed to create topic. Check bot permissions. Details: {e}")
                    return

            # Step 3: Pre-check and Copy Loop (using the BOT client)
            all_possible_ids = list(range(start_id, end_id + 1))
            existing_ids = self._get_existing_mappings(source_chat.id, all_possible_ids)
            ids_to_process = [mid for mid in all_possible_ids if mid not in existing_ids]
            
            if not ids_to_process:
                await self._log("All messages in range already copied.")
                return

            total_to_process = len(ids_to_process)
            await self._log(f"Starting copy of {total_to_process} messages...")
            success, failed = 0, 0
            for i, msg_id in enumerate(ids_to_process):
                try:
                    if asyncio.current_task().cancelled():
                        await self._log("Task cancelled. Stopping.")
                        break
                    # The actual copy is done by the BOT client
                    await self.bot_client.copy_message(target_chat.id, source_chat.id, msg_id, reply_to_message_id=target_topic_id)
                    self._add_mapping(source_chat.id, msg_id)
                    success += 1
                    if i % 10 == 0 or i == total_to_process - 1:
                        await self._log(f"Progress: {i+1}/{total_to_process}...")
                except exceptions.FloodWait as e:
                    failed += 1
                    await self._log(f"FloodWait for {e.value}s. Sleeping...")
                    await asyncio.sleep(e.value + 1)
                except Exception as e:
                    failed += 1
                    await self._log(f"Failed ID {msg_id}. Reason: {e}")
                await asyncio.sleep(delay)
            
            summary = f"Batch Complete!\n\nSuccess: {success}\nFailed: {failed}"
            await self._log(summary)
        except asyncio.CancelledError:
            await self._log("Job was cancelled externally.")
            raise
        except Exception as e:
            await self._log(f"A critical error occurred: {e}")
            raise


# --- BOT COMMAND HANDLERS (use bot_app) ---
admin_filter = filters.user(ADMIN_USER_ID)
@bot_app.on_message(filters.command("start") & admin_filter)
async def start_handler(c, m): await m.reply_text("Admin, welcome! Use /help.")
# ... other handlers ...
@bot_app.on_message(filters.command("help") & admin_filter)
async def help_handler(c, m): await m.reply_text("`/copy <src> <tgt> <range>`\n`/cancel`\n`/status`")
@bot_app.on_message(filters.command("status") & admin_filter)
async def status_handler(c, m): await m.reply_text("A job is running." if ADMIN_USER_ID in active_jobs else "No active job.")
@bot_app.on_message(filters.command("cancel") & admin_filter)
async def cancel_handler(c, m):
    if ADMIN_USER_ID in active_jobs:
        active_jobs[ADMIN_USER_ID].cancel()
        await m.reply_text("Cancellation request sent.")
    else:
        await m.reply_text("No job to cancel.")

@bot_app.on_message(filters.command("copy") & admin_filter)
async def copy_handler(client, message): # client here is the bot_app
    if ADMIN_USER_ID in active_jobs:
        await message.reply_text("Job already in progress. Use /cancel first.")
        return
    try:
        _, source, target, id_range = message.text.split()
        start_id, end_id = map(int, id_range.split('-'))
        if start_id > end_id:
            await message.reply_text("ERROR: Start ID > End ID.")
            return
    except ValueError:
        await message.reply_text("Invalid format. Use: `/copy <src> <tgt> <start-end>`")
        return

    status_message = await message.reply_text("Job accepted. Initializing...")
    copier = TelegramCopier(bot_client=client, user_client=user_client, status_message=status_message, redis_conn=redis_client)
    task = asyncio.create_task(copier.run_copy_task(source, target, start_id, end_id, delay=2.0))
    active_jobs[ADMIN_USER_ID] = task

    try:
        await task
        final_text = status_message.text + "\n\n---\nJob Finished!"
        await status_message.edit_text(final_text, parse_mode=None)
    except asyncio.CancelledError:
        final_text = status_message.text + "\n\n---\nJob Cancelled by User!"
        await status_message.edit_text(final_text, parse_mode=None)
    except Exception as e:
        final_text = status_message.text + f"\n\n---\nAn unexpected error occurred:\n{e}"
        await status_message.edit_text(final_text, parse_mode=None)
    finally:
        if ADMIN_USER_ID in active_jobs:
            del active_jobs[ADMIN_USER_ID]

# --- WEB SERVER FOR KEEP-ALIVE ---
web_app = Flask(__name__)
@web_app.route('/')
def index(): return "Bot is alive!", 200
def run_web_server(): serve(web_app, host="0.0.0.0", port=10000)

# --- MAIN EXECUTION BLOCK ---
async def main():
    init_redis()
    web_thread = Thread(target=run_web_server)
    web_thread.daemon = True
    web_thread.start()
    print("Starting clients...")
    await user_client.start()
    await bot_app.start()
    print("Bot and web server are running.")
    await asyncio.Event().wait() # Keep the main function alive

if __name__ == "__main__":
    asyncio.run(main())
