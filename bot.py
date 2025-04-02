import discord
from discord.ext import commands, tasks
import asyncio
import os
import time
import json
import logging
import sqlite3
import threading
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("discord_file_chunker")

try:
    TOKEN = open("token", "r").read().strip()
except Exception as e:
    logger.critical(f"Error reading token file: {e}")
    exit(1)

GUILD_ID = 1229863699318046791
CATEGORY_ID = 1361935509080768513
SESSION_TIMEOUT = 3600

intents = discord.Intents.default()
intents.message_content = True
intents.dm_messages = True
intents.guilds = True
intents.reactions = True

bot = commands.Bot(command_prefix='!', intents=intents)

class Database:
    def __init__(self, db_path="file_chunks.db"):
        self.db_path = db_path
        self.conn = None
        self.lock = threading.Lock()
        self.setup_database()

    def get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)
        return self.conn

    def setup_database(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                is_complete INTEGER DEFAULT 0
            )
            ''')

            cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                url TEXT NOT NULL,
                index_num INTEGER NOT NULL,
                filename TEXT NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions (session_id)
            )
            ''')

            conn.commit()
        except Exception as e:
            logger.error(f"Database setup error: {e}")

    def create_session(self, session_id, user_id, timestamp):
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO sessions (session_id, user_id, timestamp) VALUES (?, ?, ?)",
                    (session_id, str(user_id), timestamp)
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error creating session: {e}")
                conn.rollback()
                return False

    def add_chunk(self, session_id, url, index, filename):
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO chunks (session_id, url, index_num, filename) VALUES (?, ?, ?, ?)",
                    (session_id, url, index, filename)
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error adding chunk: {e}")
                conn.rollback()
                return False

    def get_session_chunks(self, session_id):
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT url, index_num, filename FROM chunks WHERE session_id = ? ORDER BY index_num",
                    (session_id,)
                )
                chunks = []
                for row in cursor.fetchall():
                    chunks.append({
                        "url": row[0],
                        "index": row[1],
                        "filename": row[2]
                    })
                return chunks
            except Exception as e:
                logger.error(f"Error getting chunks: {e}")
                return []

    def mark_session_complete(self, session_id):
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE sessions SET is_complete = 1 WHERE session_id = ?",
                    (session_id,)
                )
                conn.commit()
                return True
            except Exception as e:
                logger.error(f"Error marking session complete: {e}")
                conn.rollback()
                return False

    def get_expired_sessions(self, timeout_seconds):
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cutoff = int(time.time()) - timeout_seconds
                cursor.execute(
                    "SELECT session_id FROM sessions WHERE timestamp < ? AND is_complete = 0",
                    (cutoff,)
                )
                return [row[0] for row in cursor.fetchall()]
            except Exception as e:
                logger.error(f"Error getting expired sessions: {e}")
                return []

    def get_user_session(self, user_id):
        with self.lock:
            try:
                conn = self.get_connection()
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT session_id FROM sessions WHERE user_id = ? AND is_complete = 0",
                    (str(user_id),)
                )
                result = cursor.fetchone()
                return result[0] if result else None
            except Exception as e:
                logger.error(f"Error getting user session: {e}")
                return None
    
    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

db = Database()

active_sessions = {}

@bot.event
async def on_ready():
    logger.info(f'{bot.user} has connected to Discord!')
    logger.info(f'Serving in {len(bot.guilds)} guilds')
    cleanup_sessions.start()

@tasks.loop(minutes=5.0)
async def cleanup_sessions():
    try:
        expired_sessions = db.get_expired_sessions(SESSION_TIMEOUT)
        for session_id in expired_sessions:
            db.mark_session_complete(session_id)
            logger.info(f"Cleaned up expired session: {session_id}")

        current_time = time.time()
        to_remove = []
        for user_id, session_data in active_sessions.items():
            if current_time - session_data['timestamp'] > SESSION_TIMEOUT:
                to_remove.append(user_id)

        for user_id in to_remove:
            del active_sessions[user_id]
            logger.info(f"Cleaned up memory cache for user: {user_id}")

    except Exception as e:
        logger.error(f"Error in cleanup task: {e}")

@bot.event
async def on_message(message):
    if message.author == bot.user:
        return

    if not isinstance(message.channel, discord.DMChannel):
        return

    user_id = message.author.id

    try:
        if message.content.lower() == 'start':
            existing_session = db.get_user_session(user_id)
            if existing_session:
                logger.info(f"User {user_id} already has active session: {existing_session}")
                await message.channel.send("You already have an active recording session. Send 'stop' to complete it first.")
                return

            session_id = f"{int(time.time())}_{user_id}"
            timestamp = int(time.time())

            success = db.create_session(session_id, user_id, timestamp)
            if not success:
                await message.channel.send("Failed to start recording session. Please try again.")
                return

            active_sessions[user_id] = {
                'session_id': session_id,
                'timestamp': timestamp
            }

            logger.info(f"Started session for user {user_id}: {session_id}")
            await message.channel.send("Started recording uploads")
            return

        elif 'stop' in message.content.lower():
            client_session_id = message.content.lower().split('stop')[1].strip()
            session_id = None
            if user_id in active_sessions:
                session_id = active_sessions[user_id]['session_id']
            else:
                session_id = db.get_user_session(user_id)

            if not session_id:
                await message.channel.send("No active recording session found. Start one with 'start'")
                return

            await message.channel.send("Building downloader, this may take a moment...")

            chunks = db.get_session_chunks(session_id)

            if not chunks:
                await message.channel.send("No chunks were recorded in this session")
                db.mark_session_complete(session_id)
                if user_id in active_sessions:
                    del active_sessions[user_id]
                return

            try:
                guild = bot.get_guild(GUILD_ID)
                if not guild:
                    await message.channel.send("Could not find the specified guild")
                    return

                category = guild.get_channel(CATEGORY_ID)
                if not category:
                    await message.channel.send("Could not find the specified category")
                    return

                channel_name = str(user_id)
                try:
                    existing_channel = discord.utils.get(guild.text_channels, name=channel_name)
                    if existing_channel:
                        channel = existing_channel
                    else:
                        channel = await guild.create_text_channel(
                            name=channel_name,
                            category=category
                        )
                except discord.errors.Forbidden:
                    await message.channel.send("I don't have permission to create channels")
                    return
                except Exception as e:
                    logger.error(f"Error creating channel: {e}")
                    await message.channel.send(f"Error creating channel: {str(e)}")
                    return

                original_filename = None
                for chunk in chunks:
                    filename = chunk["filename"]
                    if ".part" in filename:
                        original_filename = filename.split(".part")[0]
                        break

                if not original_filename:
                    original_filename = f"merged_file_{user_id}_{int(time.time())}"
                    logger.warning(f"Could not determine original filename, using: {original_filename}")

                py_script = generate_pythonw_script(chunks, original_filename)

                timestamp = int(time.time())
                script_filename = f"downloader_{user_id}_{timestamp}.pyw"
                with open(script_filename, 'w', encoding='utf-8') as f:
                    f.write(py_script)

                file = discord.File(script_filename, filename=script_filename)

                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        script_message = await channel.send(file=file)
                        break
                    except discord.errors.HTTPException as e:
                        if attempt < max_retries - 1:
                            logger.warning(f"Rate limited, retrying in 5 seconds. Error: {e}")
                            await asyncio.sleep(5)
                        else:
                            raise

                cdn_link = script_message.attachments[0].url

                await message.channel.send(f"{client_session_id}:{cdn_link}")

                os.remove(script_filename)
                db.mark_session_complete(session_id)
                if user_id in active_sessions:
                    del active_sessions[user_id]

                logger.info(f"Successfully processed session {session_id} for user {user_id}")

            except Exception as e:
                logger.error(f"Error processing session: {e}")
                await message.channel.send(f"An error occurred: {str(e)}")

        elif message.attachments:
            session_id = None
            if user_id in active_sessions:
                session_id = active_sessions[user_id]['session_id']
            else:
                session_id = db.get_user_session(user_id)

            if not session_id:
                await message.channel.send("You need to start a recording session before uploading files. Send 'start' to begin a new session.")
                await message.add_reaction('❌')
                return

            for attachment in message.attachments:
                if '.part' in attachment.filename:
                    try:
                        parts = attachment.filename.split('.part')
                        if len(parts) >= 2:
                            try:
                                index = int(parts[1])
                            except ValueError:
                                logger.error(f"Invalid part number format: {attachment.filename}")
                                await message.add_reaction('❌')
                                continue

                            success = db.add_chunk(session_id, attachment.url, index, attachment.filename)

                            if success:
                                await message.add_reaction('✅')
                            else:
                                await message.add_reaction('❌')
                        else:
                            logger.error(f"Invalid filename format: {attachment.filename}")
                            await message.add_reaction('❌')

                    except Exception as e:
                        logger.error(f"Error processing attachment: {e}")
                        await message.add_reaction('❌')

    except Exception as e:
        logger.error(f"Unhandled error in message processing: {e}")
        try:
            await message.channel.send("An error occurred while processing your request")
        except:
            pass

def generate_pythonw_script(chunks, original_filename):
    chunks_json = json.dumps(chunks)
    
    script = f"""
import requests
import json
import os

chunks = {chunks_json}
original_filename = "{original_filename}"

def get_nonconflicting_path(base_path):
    if not os.path.exists(base_path):
        return base_path

    name, ext = os.path.splitext(base_path)
    counter = 1
    while True:
        new_path = f"{{name}} ({{counter}}){{ext}}"
        if not os.path.exists(new_path):
            return new_path
        counter += 1

def download_and_reconstruct():
    print("Starting download...")
    chunks_data = []

    for i, chunk in enumerate(chunks):
        print(f"Downloading chunk {{i+1}} of {{len(chunks)}}...")
        response = requests.get(chunk['url'], timeout=30)
        chunks_data.append(response.content)

    print("All chunks downloaded. Reconstructing file...")

    output_dir = os.path.join(os.path.expanduser("~"), "Downloads")
    output_path = os.path.join(output_dir, original_filename)

    if not os.path.exists(output_dir):
        output_path = original_filename

    output_path = get_nonconflicting_path(output_path)

    with open(output_path, 'wb') as f:
        for chunk_data in chunks_data:
            f.write(chunk_data)

    print(f"File successfully saved to: {{output_path}}")

if __name__ == "__main__":
    download_and_reconstruct()
"""
    return script

@bot.event
async def on_error(event, *args, **kwargs):
    logger.error(f"Discord event error in {event}: {args}")

def main():
    try:
        logger.info("Starting Discord File Chunker Bot")
        bot.run(TOKEN)
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
    finally:
        db.close()
        logger.info("Bot shutdown complete")

if __name__ == "__main__":
    main()
