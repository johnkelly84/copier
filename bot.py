import asyncio
import os
import sys
import time
from datetime import datetime

# New import for Redis
import redis

from pyrogram import Client, filters
from pyrogram.errors import exceptions
from dotenv import load_dotenv

# --- CONFIGURATION & INITIALIZATION ---
load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_USER_ID = int(os.getenv("ADMIN_USER_ID"))
REDIS_URL = os.getenv("REDIS_URL") # <-- New environment variable

# --- PYROGRAM BOT CLIENT ---
app = Client("copier_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- STATE MANAGEMENT ---
active_jobs = {}

# ############################################################################
# --- REDIS INITIALIZATION ---
# ############################################################################

# Global Redis client
redis_client = None

def init_redis():
    """Initializes the Redis client and tests the connection."""
    global redis_client
    if not REDIS_URL:
        print("FATAL: REDIS_URL environment variable is not set.")
        sys.exit(1)
    try:
        # decode_responses=True makes the client return strings instead of bytes
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        # Test the connection
        redis_client.ping()
        print("INFO: Redis connection successful.")
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        sys.exit(1)

# ############################################################################
# --- COPIER: BOT-INTEGRATED WORKER CLASS (Updated for Redis) ---
# ############################################################################
class TelegramCopier:
    def __init__(self, bot_client: Client, status_message, redis_conn):
        self.bot_client = bot_client
        self.status_message = status_message
        self.redis = redis_conn # Use the global redis client
        self.log_buffer = []
        self.last_update_time = 0

    async def _log(self, message):
        # ... (this method remains exactly the same) ...
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.log_buffer.append(f"`{timestamp}`: {message}")
        if len(self.log_buffer) > 15: self.log_buffer.pop(0)
        current_time = time.time()
        if current_time - self.last_update_time > 3:
            try:
                log_text = "\n".join(self.log_buffer)
                await self.status_message.edit_text(
                    f"**🚀 Copy Job in Progress...**\n\n---\n{log_text}"
                )
                self.last_update_time = current_time
            except exceptions.FloodWait as e:
                print(f"WARN: Flood wait of {e.value}s on status update.")
                await asyncio.sleep(e.value)
            except Exception as e: print(f"Error updating status message: {e}")

    def _get_existing_mappings(self, source_chat_id, ids_to_check):
        """Checks for existing message IDs in a Redis Set using a pipeline for efficiency."""
        if not ids_to_check:
            return set()
            
        redis_set_key = f"copier_map:{source_chat_id}"
        
        # Use a pipeline to send all checks in one network round-trip
        pipe = self.redis.pipeline()
        for msg_id in ids_to_check:
            pipe.sismember(redis_set_key, str(msg_id))
        
        results = pipe.execute() # Returns a list of booleans [True, False, True, ...]
        
        existing_ids = {
            ids_to_check[i] for i, exists in enumerate(results) if exists
        }
        return existing_ids

    def _add_mapping(self, source_chat_id, source_message_id):
        """Adds a message ID to the Redis Set for the source chat."""
        try:
            redis_set_key = f"copier_map:{source_chat_id}"
            self.redis.sadd(redis_set_key, str(source_message_id))
        except Exception as e:
            asyncio.create_task(self._log(f"Redis Error: {e}"))

    # The main run_copy_task method is slightly adjusted for the new Redis methods
    async def run_copy_task(self, source_input, target_input, start_id, end_id, delay):
        try:
            # 1. Get Chat Objects & Verify
            try:
                source_chat = await self.bot_client.get_chat(source_input)
                target_chat = await self.bot_client.get_chat(target_input)
            except Exception as e:
                await self._log(f"❌ **Error:** Could not access chats. Details: `{e}`")
                return

            await self._log(f"✅ Source: **{source_chat.title}**")
            await self._log(f"✅ Target: **{target_chat.title}**")

            if not target_chat.is_forum:
                await self._log("❌ **Error:** Target chat must have Topics enabled.")
                return

            # 2. Find or Create Topic
            # ... (This logic is exactly the same as before) ...
            source_title = source_chat.title
            target_topic_id = None
            await self._log(f"🔎 Searching for topic: '{source_title}'...")
            async for topic in self.bot_client.get_forum_topics(target_chat.id):
                if topic.title == source_title:
                    target_topic_id = topic.id
                    await self._log(f"✅ Found existing topic (ID: {topic.id})")
                    break
            if not target_topic_id:
                # ... (create topic logic is the same) ...
                try:
                    new_topic = await self.bot_client.create_forum_topic(target_chat.id, source_title)
                    target_topic_id = new_topic.id
                    await self._log(f"✅ Created new topic (ID: {target_topic_id})")
                except Exception as e:
                    await self._log(f"❌ **Error:** Failed to create topic. Details: `{e}`")
                    return

            # 3. Pre-check and Copy Loop (Updated for Redis)
            all_possible_ids = list(range(start_id, end_id + 1))
            existing_ids = self._get_existing_mappings(source_chat.id, all_possible_ids)
            ids_to_process = [mid for mid in all_possible_ids if mid not in existing_ids]
            
            if not ids_to_process:
                await self._log("✅ All messages in range already copied.")
                return

            total_to_process = len(ids_to_process)
            await self._log(f"▶️ Starting copy of **{total_to_process}** messages...")
            
            # ... (The rest of the copy loop logic is exactly the same) ...
            success, failed = 0, 0
            for i, msg_id in enumerate(ids_to_process):
                try:
                    if asyncio.current_task().cancelled():
                        await self._log("Task cancelled. Stopping.")
                        break
                    await self.bot_client.copy_message(target_chat.id, source_chat.id, msg_id, reply_to_message_id=target_topic_id)
                    self._add_mapping(source_chat.id, msg_id)
                    success += 1
                    if i % 10 == 0 or i == total_to_process - 1:
                        await self._log(f"Progress: {i+1}/{total_to_process}...")
                except exceptions.FloodWait as e:
                    failed += 1
                    await self._log(f"⏳ FloodWait for {e.value}s. Sleeping...")
                    await asyncio.sleep(e.value + 1)
                except Exception as e:
                    failed += 1
                    await self._log(f"⚠️ Failed ID {msg_id}. Reason: `{e}`")
                await asyncio.sleep(delay)
            
            summary = f"**🏁 Batch Complete!**\n\n- **Success:** {success}\n- **Failed:** {failed}"
            await self._log(summary)
        except asyncio.CancelledError:
            await self._log("Job was cancelled externally.")
            raise

# --- BOT COMMAND HANDLERS & MAIN BLOCK ---
# The command handlers are almost identical, just updated to pass the redis_client

admin_filter = filters.user(ADMIN_USER_ID)

# ... /start, /help, /status, /cancel handlers are EXACTLY the same ...
@app.on_message(filters.command("start") & admin_filter)
async def start_handler(c, m): await m.reply_text("👋 Admin, welcome! Use /help.")

@app.on_message(filters.command("help") & admin_filter)
async def help_handler(c, m): await m.reply_text("`/copy <src> <tgt> <range>`\n`/cancel`\n`/status`")

@app.on_message(filters.command("status") & admin_filter)
async def status_handler(c, m): await m.reply_text("✅ Job running." if ADMIN_USER_ID in active_jobs else "ℹ️ No active job.")

@app.on_message(filters.command("cancel") & admin_filter)
async def cancel_handler(c, m):
    if ADMIN_USER_ID in active_jobs:
        active_jobs[ADMIN_USER_ID].cancel()
        await m.reply_text("✅ Cancellation request sent.")
    else:
        await m.reply_text("❌ No job to cancel.")

@app.on_message(filters.command("copy") & admin_filter)
async def copy_handler(client, message):
    if ADMIN_USER_ID in active_jobs:
        await message.reply_text("❌ Job already in progress. Use /cancel first.")
        return

    try:
        _, source, target, id_range = message.text.split()
        start_id, end_id = map(int, id_range.split('-'))
        if start_id > end_id:
            await message.reply_text("❌ **Error:** Start ID > End ID.")
            return
    except ValueError:
        await message.reply_text("❌ **Invalid format.** Use: `/copy <src> <tgt> <start-end>`")
        return

    status_message = await message.reply_text("✅ Job accepted. Initializing...")
    
    # Pass the global redis_client to the copier instance
    copier = TelegramCopier(bot_client=client, status_message=status_message, redis_conn=redis_client)
    
    task = asyncio.create_task(
        copier.run_copy_task(source, target, start_id, end_id, delay=2.0)
    )
    active_jobs[ADMIN_USER_ID] = task

    try:
        await task
        final_text = status_message.text + "\n\n--- \n**✅ Job Finished!**"
        await status_message.edit_text(final_text)
    except asyncio.CancelledError:
        final_text = status_message.text + "\n\n--- \n**🛑 Job Cancelled by User!**"
        await status_message.edit_text(final_text)
    except Exception as e:
        final_text = status_message.text + f"\n\n--- \n**💥 An unexpected error occurred:**\n`{e}`"
        await status_message.edit_text(final_text)
    finally:
        if ADMIN_USER_ID in active_jobs:
            del active_jobs[ADMIN_USER_ID]

if __name__ == "__main__":
    init_redis() # Changed from init_database()
    print("Bot is starting...")
    app.run()
    print("Bot has stopped.")
