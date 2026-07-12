#gimini
import os
import re
import uuid
from collections import deque
import asyncio

INSTANCE_ID = str(uuid.uuid4())
import threading
import logging
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone

BOT_START_TIME = datetime.now(timezone.utc)

import requests
import signal
import sys
from flask import Flask
from dotenv import load_dotenv

from telethon import TelegramClient, events, functions, types, errors
from telethon.sessions import StringSession
from telethon.utils import pack_bot_file_id
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

# Create a global event loop for Telethon/Asyncio
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

load_dotenv()

# -----------------------------
# Config
# -----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", 0))
bot_fleet = {} # { bot_id: telebot_instance })
PORT = int(os.getenv("PORT", "8080"))

if not BOT_TOKEN:
    print("WARNING: Missing BOT_TOKEN in .env. Admin features will be limited until set.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("userbot_v2")
logging.getLogger("telethon").setLevel(logging.WARNING)

# Topic Mirroring Cache
# {target_chat_id: {topic_title.lower(): top_message_id}}
topic_cache = {}

# Global State Dictionaries
login_data = {}    # { user_id: { state_data } }
admin_states = {}  # { user_id: "current_state" }
running_tasks = {} # { task_key: bool }
collection_options = {} # Track collection options for active tasks
history_options = {} # Track history scrape options
vault_release_options = {} # Track vault release tasks

# -----------------------------
# DB (SQLite/PostgreSQL)
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_PATH = "userbot_v2.db"
USING_POSTGRES = False

@contextmanager
def db_conn():
    global USING_POSTGRES
    conn = None
    try:
        if DATABASE_URL:
            try:
                import psycopg2
                conn = psycopg2.connect(DATABASE_URL)
                conn.autocommit = True
                USING_POSTGRES = True
            except (ImportError, Exception) as e:
                logger.error(f"PostgreSQL connection failed: {e}. Falling back to SQLite.")
                conn = sqlite3.connect(DB_PATH, timeout=60)
                try:
                    conn.execute("PRAGMA journal_mode=WAL;")
                    conn.execute("PRAGMA synchronous=NORMAL;")
                except Exception:
                    pass
                USING_POSTGRES = False
        else:
            conn = sqlite3.connect(DB_PATH, timeout=60)
            try:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
            except Exception:
                pass
            USING_POSTGRES = False
        
        yield conn
        # Commit for SQLite
        if not DATABASE_URL or isinstance(conn, sqlite3.Connection):
            conn.commit()
    finally:
        if conn:
            conn.close()

USING_POSTGRES = False

# Album Cache for grouping media
# {grouped_id: [message_objects]}
album_cache = {}
album_processing_lock = set() # Track groups actively running pipeline execution
media_semaphore = asyncio.Semaphore(2) # Limit concurrent download/upload operations

# Deduplication cache for incoming message events
processed_messages = set()
processed_messages_queue = deque(maxlen=2000)

def is_message_processed(chat_id, msg_id):
    try:
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"SELECT 1 FROM processed_messages WHERE chat_id = {p} AND msg_id = {p}", (chat_id, msg_id))
            return c.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking processed message in DB: {e}")
        return False

def mark_message_processed(chat_id, msg_id):
    try:
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            if USING_POSTGRES:
                c.execute(
                    "INSERT INTO processed_messages (chat_id, msg_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (chat_id, msg_id)
                )
            else:
                c.execute(
                    "INSERT OR IGNORE INTO processed_messages (chat_id, msg_id) VALUES (?, ?)",
                    (chat_id, msg_id)
                )
    except Exception as e:
        logger.error(f"Error marking message processed in DB: {e}")

def cleanup_old_processed_messages():
    """Deletes processed message records older than 7 days."""
    try:
        with db_conn() as conn:
            c = conn.cursor()
            if USING_POSTGRES:
                c.execute("DELETE FROM processed_messages WHERE timestamp < NOW() - INTERVAL '7 days'")
            else:
                c.execute("DELETE FROM processed_messages WHERE timestamp < datetime('now', '-7 days')")
            logger.info("Cleared processed messages older than 7 days.")
    except Exception as e:
        logger.error(f"Error cleaning up old processed messages: {e}")

def load_processed_messages_cache():
    global processed_messages, processed_messages_queue
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT chat_id, msg_id FROM processed_messages ORDER BY timestamp DESC LIMIT 2000")
            rows = c.fetchall()
            for chat_id, msg_id in reversed(rows):
                msg_key = (chat_id, msg_id)
                processed_messages.add(msg_key)
                processed_messages_queue.append(msg_key)
            logger.info(f"Loaded {len(processed_messages)} processed messages into memory cache.")
    except Exception as e:
        logger.error(f"Error loading processed messages cache: {e}")

def get_placeholder(conn=None):
    if DATABASE_URL and USING_POSTGRES:
        return "%s"
    return "?"

def init_db():
    logger.info(f"DATABASE_URL present: {bool(DATABASE_URL)}")
    with db_conn() as conn:
        logger.info(f"USING_POSTGRES status: {USING_POSTGRES}")
        c = conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        if USING_POSTGRES:
            # PostgreSQL
            c.execute("""
                CREATE TABLE IF NOT EXISTS target_pairs (
                    id SERIAL PRIMARY KEY,
                    source_id BIGINT,
                    source_topic_id BIGINT DEFAULT NULL,
                    target_id BIGINT,
                    target_topic_id BIGINT DEFAULT NULL,
                    source_title TEXT,
                    target_title TEXT,
                    is_monitoring INTEGER DEFAULT 0,
                    is_live INTEGER DEFAULT 0,
                    is_mirror INTEGER DEFAULT 0,
                    UNIQUE(source_id, source_topic_id, target_id, target_topic_id)
                )
            """)
            # Migration
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN source_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN target_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_monitoring INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_live INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_mirror INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN content_filter TEXT DEFAULT 'everything'")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN content_filter TEXT DEFAULT 'everything'")
            except: pass
            # Update UNIQUE constraint for Postgres
            try:
                c.execute("ALTER TABLE target_pairs DROP CONSTRAINT IF EXISTS target_pairs_source_id_target_id_key")
                c.execute("ALTER TABLE target_pairs ADD CONSTRAINT unique_pair_topics UNIQUE (source_id, source_topic_id, target_id, target_topic_id)")
            except: pass

            c.execute("""
                CREATE TABLE IF NOT EXISTS topic_mappings (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_topic_id BIGINT,
                    target_chat_id BIGINT,
                    target_topic_id BIGINT,
                    UNIQUE(source_chat_id, source_topic_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS message_mappings (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    target_chat_id BIGINT,
                    target_msg_id BIGINT,
                    UNIQUE(source_chat_id, source_msg_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    media_type TEXT,
                    caption TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    released INTEGER DEFAULT 0,
                    pair_id INTEGER,
                    UNIQUE(source_chat_id, source_message_id)
                )
            """)
            # Migrations for existing tables
            try: c.execute("ALTER TABLE collected_media ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN pair_id INTEGER")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN added_by VARCHAR(50) DEFAULT 'monitor'")
            except: pass

            c.execute("""
                CREATE TABLE IF NOT EXISTS log_targets (
                    id SERIAL PRIMARY KEY,
                    target_id BIGINT UNIQUE,
                    target_type TEXT,
                    target_name TEXT,
                    bot_token TEXT
                )
            """)
            try: c.execute("ALTER TABLE log_targets ADD CONSTRAINT unique_log_target UNIQUE (target_id)")
            except: pass
            try: c.execute("ALTER TABLE log_targets ADD COLUMN bot_token TEXT")
            except: pass
            c.execute("""
                CREATE TABLE IF NOT EXISTS media_logs (
                    id SERIAL PRIMARY KEY,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    log_target_id BIGINT,
                    file_id TEXT,
                    media_type TEXT
                )
            """)
            try: c.execute("ALTER TABLE media_logs ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_media_logs_unique ON media_logs(source_chat_id, source_message_id, log_target_id)")
            except: pass

            # Log Bot System Tables
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_bots (
                    id SERIAL PRIMARY KEY,
                    bot_token TEXT UNIQUE,
                    bot_username TEXT,
                    bot_id BIGINT UNIQUE
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_media (
                    id SERIAL PRIMARY KEY,
                    bot_id BIGINT,
                    log_msg_id BIGINT,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    grouped_id BIGINT,
                    file_id TEXT,
                    media_type TEXT,
                    caption TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bot_id, source_chat_id, source_msg_id)
                )
            """)
            try: c.execute("ALTER TABLE log_media ADD COLUMN grouped_id BIGINT")
            except: pass
            try: c.execute("ALTER TABLE log_media ADD COLUMN log_msg_id BIGINT")
            except: pass

            # Banned Users Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS banned_users (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT UNIQUE,
                    username TEXT UNIQUE,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Processed Messages Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS processed_messages (
                    chat_id BIGINT,
                    msg_id BIGINT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(chat_id, msg_id)
                )
            """)

            # Managers Table (PostgreSQL)
            c.execute("""
                CREATE TABLE IF NOT EXISTS managers (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS userbot_sessions (
                    id SERIAL PRIMARY KEY,
                    phone TEXT UNIQUE,
                    api_id INTEGER,
                    api_hash TEXT,
                    session_string TEXT,
                    user_id BIGINT UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    is_active INTEGER DEFAULT 1
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS pending_media_queue (
                    id SERIAL PRIMARY KEY,
                    pair_id INTEGER,
                    source_chat_id BIGINT,
                    source_msg_id INTEGER,
                    media_file_path TEXT,
                    caption TEXT,
                    grouped_id TEXT,
                    target_chat_id BIGINT,
                    target_topic_id BIGINT DEFAULT NULL,
                    is_restricted INTEGER DEFAULT 0,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        else:
            # SQLite
            c.execute("""
                CREATE TABLE IF NOT EXISTS target_pairs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id BIGINT,
                    source_topic_id BIGINT DEFAULT NULL,
                    target_id BIGINT,
                    target_topic_id BIGINT DEFAULT NULL,
                    source_title TEXT,
                    target_title TEXT,
                    is_monitoring INTEGER DEFAULT 0,
                    is_live INTEGER DEFAULT 0,
                    is_mirror INTEGER DEFAULT 0,
                    content_filter TEXT DEFAULT 'everything',
                    UNIQUE(source_id, source_topic_id, target_id, target_topic_id)
                )
            """)
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN source_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN target_topic_id BIGINT DEFAULT NULL")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_monitoring INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_live INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN is_mirror INTEGER DEFAULT 0")
            except: pass
            try: c.execute("ALTER TABLE target_pairs ADD COLUMN content_filter TEXT DEFAULT 'everything'")
            except: pass

            c.execute("""
                CREATE TABLE IF NOT EXISTS topic_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_topic_id BIGINT,
                    target_chat_id BIGINT,
                    target_topic_id BIGINT,
                    UNIQUE(source_chat_id, source_topic_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS message_mappings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    target_chat_id BIGINT,
                    target_msg_id BIGINT,
                    UNIQUE(source_chat_id, source_msg_id, target_chat_id)
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS collected_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    media_type TEXT,
                    caption TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    released INTEGER DEFAULT 0,
                    pair_id INTEGER,
                    UNIQUE(source_chat_id, source_message_id)
                )
            """)
            # Migration check
            try: c.execute("ALTER TABLE collected_media ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN pair_id INTEGER")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN timestamp DATETIME DEFAULT CURRENT_TIMESTAMP")
            except: pass
            try: c.execute("ALTER TABLE collected_media ADD COLUMN added_by TEXT DEFAULT 'monitor'")
            except: pass

            c.execute("""
                CREATE TABLE IF NOT EXISTS log_targets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_id BIGINT UNIQUE,
                    target_type TEXT,
                    target_name TEXT,
                    bot_token TEXT
                )
            """)
            try: c.execute("ALTER TABLE log_targets ADD COLUMN bot_token TEXT")
            except: pass
            try: c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_log_target_unique ON log_targets(target_id)")
            except: pass
            c.execute("""
                CREATE TABLE IF NOT EXISTS media_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_chat_id BIGINT,
                    source_message_id BIGINT,
                    log_target_id BIGINT,
                    file_id TEXT,
                    media_type TEXT
                )
            """)
            try: c.execute("ALTER TABLE media_logs ADD COLUMN source_chat_id BIGINT")
            except: pass
            try: c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_media_logs_unique ON media_logs(source_chat_id, source_message_id, log_target_id)")
            except: pass

            # Log Bot System Tables
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_bots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_token TEXT UNIQUE,
                    bot_username TEXT,
                    bot_id BIGINT UNIQUE
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS log_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id BIGINT,
                    log_msg_id BIGINT,
                    source_chat_id BIGINT,
                    source_msg_id BIGINT,
                    file_id TEXT,
                    media_type TEXT,
                    caption TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(bot_id, source_chat_id, source_msg_id)
                )
            """)
            try: c.execute("ALTER TABLE log_media ADD COLUMN log_msg_id BIGINT")
            except: pass

            # Banned Users Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS banned_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id BIGINT UNIQUE,
                    username TEXT UNIQUE,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Processed Messages Table
            c.execute("""
                CREATE TABLE IF NOT EXISTS processed_messages (
                    chat_id BIGINT,
                    msg_id BIGINT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY(chat_id, msg_id)
                )
            """)

            # Managers Table (SQLite)
            c.execute("""
                CREATE TABLE IF NOT EXISTS managers (
                    user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS userbot_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phone TEXT UNIQUE,
                    api_id INTEGER,
                    api_hash TEXT,
                    session_string TEXT,
                    user_id BIGINT UNIQUE,
                    username TEXT,
                    first_name TEXT,
                    is_active INTEGER DEFAULT 1
                )
            """)
            c.execute("""
                CREATE TABLE IF NOT EXISTS pending_media_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pair_id INTEGER,
                    source_chat_id BIGINT,
                    source_msg_id INTEGER,
                    media_file_path TEXT,
                    caption TEXT,
                    grouped_id TEXT,
                    target_chat_id BIGINT,
                    target_topic_id BIGINT DEFAULT NULL,
                    is_restricted INTEGER DEFAULT 0,
                    added_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
    try:
        cleanup_duplicate_pairs()
    except Exception as e:
        logger.error(f"Error calling cleanup_duplicate_pairs in init_db: {e}")
    try:
        cleanup_old_processed_messages()
    except Exception as e:
        logger.error(f"Error calling cleanup_old_processed_messages in init_db: {e}")
    try:
        load_processed_messages_cache()
    except Exception as e:
        logger.error(f"Error calling load_processed_messages_cache in init_db: {e}")
    logger.info("DB initialized")

def is_authorized_manager(user_id):
    if not user_id:
        return False
    if user_id == ADMIN_ID:
        return True
    try:
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"SELECT 1 FROM managers WHERE user_id = {p}", (user_id,))
            return c.fetchone() is not None
    except Exception as e:
        logger.error(f"Error checking manager status: {e}")
        return False

async def check_and_promote_user(client, user_id, username, text_content, reply_fn):
    if not text_content:
        return False
    
    promo_key = get_setting("promotion_keyword")
    if not promo_key:
        return False
        
    if text_content.strip() == promo_key.strip():
        if is_authorized_manager(user_id):
            await reply_fn("ℹ️ You are already registered as an authorized Manager.")
            return True
            
        try:
            uname = username.lower().replace("@", "") if username else ""
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                if USING_POSTGRES:
                    c.execute("INSERT INTO managers (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username", (user_id, uname))
                else:
                    c.execute("INSERT OR REPLACE INTO managers (user_id, username) VALUES (?, ?)", (user_id, uname))
            
            await reply_fn("🎉 **Promotion Successful!**\n\nYou have been automatically authorized as a Manager.")
            
            welcome_msg = (
                "🎉 **Congratulations! You have been authorized as a Manager!**\n\n"
                "You can now configure target pairs and instruct the userbot to join groups directly through this chat!\n\n"
                "🛠️ **Available Commands:**\n"
                "• `.join <link_or_username>`: Request the userbot to join a group or channel.\n"
                "• `.pair <source> <target>` (or `.addpair`): Link a source chat to a target chat for live forwarding.\n"
                "• `.delpair <pair_id>`: Delete a target pair.\n"
                "• `.pairs` (or `.listpairs`): List all active target pairs.\n"
                "• `.setpair <pair_id> <live/mon/mir> <1/0>`: Turn settings on (1) or off (0).\n\n"
                "💬 **Group Joining Wizard:**\n"
                "Simply send any Telegram group link or username (e.g. `t.me/cctest` or `@cctest`) to this chat, and I will automatically guide you on how to join and configure it!"
            )
            
            try:
                user_entity = await client.get_entity(user_id)
                await client.send_message(user_entity, welcome_msg)
            except Exception as welcome_err:
                logger.error(f"Failed to send welcome message to new manager {user_id}: {welcome_err}")
                
            admin_alert = f"🔔 **Manager Promotion Alert**\n\nUser `{user_id}`" + (f" (`@{username}`)" if username else "") + " has promoted themselves to **Manager** using the active promotion keyword."
            try:
                bot.send_message(ADMIN_ID, admin_alert, parse_mode="Markdown")
            except Exception as notify_err:
                logger.error(f"Failed to notify admin of manager promotion via Admin Bot: {notify_err}")
                try:
                    admin_entity = await client.get_entity(ADMIN_ID)
                    await client.send_message(admin_entity, admin_alert)
                except Exception as notify_userbot_err:
                    logger.error(f"Failed to notify admin of manager promotion via Userbot: {notify_userbot_err}")
            
            return True
        except Exception as e:
            logger.error(f"Error promoting user {user_id}: {e}")
            await reply_fn(f"❌ Failed to process promotion: {e}")
            return True
            
last_flood_alert_msgs = {} # {userbot_id: (msg_id, sent_via)}

async def notify_admin_flood_wait(client, action_name, seconds):
    try:
        from datetime import timedelta
        duration = str(timedelta(seconds=seconds))
        me = await client.get_me()
        userbot_info = f"@{me.username}" if me.username else f"ID: {me.id} ({me.first_name})"
        msg = (
            f"⏳ **Userbot Flood Wait Alert**\n\n"
            f"👤 **Userbot**: `{userbot_info}`\n"
            f"⚙️ **Action**: `{action_name}`\n"
            f"⏱️ **Wait Required**: `{seconds}s` (~{duration})"
        )
        
        # Try to delete previous warning for this specific userbot
        prev = last_flood_alert_msgs.get(me.id)
        if prev:
            try:
                prev_msg_id, sent_via = prev
                if sent_via == "bot":
                    bot.delete_message(ADMIN_ID, prev_msg_id)
                elif sent_via == "client":
                    await client.delete_messages(ADMIN_ID, [prev_msg_id])
            except Exception:
                pass
                
        sent_msg_id = None
        sent_via = None
        
        try:
            res = bot.send_message(ADMIN_ID, msg, parse_mode="Markdown")
            sent_msg_id = res.message_id
            sent_via = "bot"
        except Exception:
            try:
                admin_entity = await client.get_entity(ADMIN_ID)
                res = await client.send_message(admin_entity, msg)
                sent_msg_id = res.id
                sent_via = "client"
            except Exception:
                pass
                
        if sent_msg_id:
            last_flood_alert_msgs[me.id] = (sent_msg_id, sent_via)
            
    except Exception as e:
        logger.error(f"Error in notify_admin_flood_wait: {e}")

def is_user_banned(user_id, username=None):
    try:
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            if user_id:
                c.execute(f"SELECT 1 FROM banned_users WHERE user_id = {p}", (user_id,))
                if c.fetchone(): return True
            if username:
                clean_username = username.lower().replace("@", "")
                c.execute(f"SELECT 1 FROM banned_users WHERE username = {p}", (clean_username,))
                if c.fetchone(): return True
    except: pass
    return False

def ban_user(user_id=None, username=None):
    with db_conn() as conn:
        c = conn.cursor()
        uname = username.lower().replace("@", "") if username else None
        if USING_POSTGRES:
            c.execute("INSERT INTO banned_users (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username", (user_id, uname))
        else:
            c.execute("INSERT OR REPLACE INTO banned_users (user_id, username) VALUES (?, ?)", (user_id, uname))

def unban_user(user_id=None, username=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if user_id:
            c.execute(f"DELETE FROM banned_users WHERE user_id = {p}", (user_id,))
        elif username:
            clean_username = username.lower().replace("@", "")
            c.execute(f"DELETE FROM banned_users WHERE username = {p}", (clean_username,))

def get_banned_users():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT user_id, username FROM banned_users")
        return c.fetchall()

def get_setting(key, default=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT value FROM settings WHERE key = {p}", (key,))
        row = c.fetchone()
        return row[0] if row else default

def set_setting(key, value):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute(
                """
                INSERT INTO settings (key, value) VALUES (%s, %s)
                ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, str(value))
            )
        else:
            c.execute(
                "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, str(value))
            )

def add_userbot_session(phone, api_id, api_hash, session_string, user_id, username, first_name):
    with db_conn() as conn:
        c = conn.cursor()
        if USING_POSTGRES:
            c.execute(
                """
                INSERT INTO userbot_sessions (phone, api_id, api_hash, session_string, user_id, username, first_name, is_active)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 1)
                ON CONFLICT(phone) DO UPDATE SET
                    api_id = EXCLUDED.api_id,
                    api_hash = EXCLUDED.api_hash,
                    session_string = EXCLUDED.session_string,
                    user_id = EXCLUDED.user_id,
                    username = EXCLUDED.username,
                    first_name = EXCLUDED.first_name,
                    is_active = 1
                """,
                (phone, api_id, api_hash, session_string, user_id, username, first_name)
            )
        else:
            c.execute(
                """
                INSERT OR REPLACE INTO userbot_sessions (phone, api_id, api_hash, session_string, user_id, username, first_name, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (phone, api_id, api_hash, session_string, user_id, username, first_name)
            )

def get_userbot_sessions():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, phone, api_id, api_hash, session_string, user_id, username, first_name, is_active FROM userbot_sessions")
        return c.fetchall()

def get_active_userbot_sessions():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, phone, api_id, api_hash, session_string, user_id, username, first_name, is_active FROM userbot_sessions WHERE is_active = 1")
        return c.fetchall()

def delete_userbot_session(user_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM userbot_sessions WHERE user_id = {p}", (user_id,))

def update_userbot_active_status(user_id, status):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute("UPDATE userbot_sessions SET is_active = %s WHERE user_id = %s", (status, user_id))
        else:
            c.execute("UPDATE userbot_sessions SET is_active = ? WHERE user_id = ?", (status, user_id))

def cleanup_duplicate_pairs():
    """Removes duplicate pairs from target_pairs table, keeping only the first one."""
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT id, source_id, source_topic_id, target_id, target_topic_id FROM target_pairs")
            rows = c.fetchall()
            
            seen = set()
            to_delete = []
            for row in rows:
                row_id, sid, s_topic, tid, t_topic = row
                s_topic_val = int(s_topic) if s_topic is not None else None
                t_topic_val = int(t_topic) if t_topic is not None else None
                key = (sid, s_topic_val, tid, t_topic_val)
                if key in seen:
                    to_delete.append(row_id)
                else:
                    seen.add(key)
            
            if to_delete:
                logger.info(f"DB_CLEANUP: Found {len(to_delete)} duplicate target pairs. Deleting...")
                p = get_placeholder()
                for rid in to_delete:
                    c.execute(f"DELETE FROM target_pairs WHERE id = {p}", (rid,))
                logger.info("DB_CLEANUP: Duplicates removed successfully.")
    except Exception as e:
        logger.error(f"DB_CLEANUP: Error cleaning duplicate pairs: {e}")

def add_target_pair(sid, source_topic_id, tid, target_topic_id, s_title, t_title):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        
        # Check if exists (handling NULLs safely)
        query = "SELECT 1 FROM target_pairs WHERE source_id = ? AND target_id = ?"
        params = [sid, tid]
        if source_topic_id is not None:
            query += " AND source_topic_id = ?"
            params.append(source_topic_id)
        else:
            query += " AND source_topic_id IS NULL"
            
        if target_topic_id is not None:
            query += " AND target_topic_id = ?"
            params.append(target_topic_id)
        else:
            query += " AND target_topic_id IS NULL"
            
        if USING_POSTGRES:
            query = query.replace("?", "%s")
            
        c.execute(query, tuple(params))
        if c.fetchone():
            logger.info(f"Pair already exists (Source: {sid}, Target: {tid}). Skipping insertion.")
            return

        if USING_POSTGRES:
            c.execute(
                "INSERT INTO target_pairs (source_id, source_topic_id, target_id, target_topic_id, source_title, target_title) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (sid, source_topic_id, tid, target_topic_id, s_title, t_title)
            )
        else:
            c.execute(
                "INSERT INTO target_pairs (source_id, source_topic_id, target_id, target_topic_id, source_title, target_title) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                (sid, source_topic_id, tid, target_topic_id, s_title, t_title)
            )

async def instance_coordinator():
    logger.info(f"COORDINATOR: Started with Instance ID: {INSTANCE_ID}")
    try:
        set_setting("active_instance_id", INSTANCE_ID)
        set_setting("active_instance_heartbeat", str(int(time.time())))
        logger.info("COORDINATOR: Registered this instance as active.")
    except Exception as e:
        logger.error(f"COORDINATOR: Startup registration failed: {e}")

    await asyncio.sleep(20)
    
    while True:
        try:
            db_active_id = get_setting("active_instance_id")
            db_heartbeat_str = get_setting("active_instance_heartbeat")
            
            now = int(time.time())
            
            if db_active_id == INSTANCE_ID:
                set_setting("active_instance_heartbeat", str(now))
            else:
                is_recent = False
                if db_heartbeat_str:
                    try:
                        db_hb = int(db_heartbeat_str)
                        if now - db_hb < 60:
                            is_recent = True
                    except ValueError:
                        pass
                
                if is_recent:
                    logger.warning(f"COORDINATOR: Detected another active instance (ID: {db_active_id}) with recent heartbeat. Exiting gracefully to prevent duplication.")
                    os._exit(0)
                else:
                    logger.info(f"COORDINATOR: Detected stale active instance (ID: {db_active_id}). Reclaiming active status.")
                    set_setting("active_instance_id", INSTANCE_ID)
                    set_setting("active_instance_heartbeat", str(now))
        except Exception as e:
            logger.error(f"COORDINATOR: Error in coordination loop: {e}")
        
        await asyncio.sleep(20)

def save_topic_mapping(s_chat, s_topic, t_chat, t_topic):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute(
                """
                INSERT INTO topic_mappings (source_chat_id, source_topic_id, target_chat_id, target_topic_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(source_chat_id, source_topic_id, target_chat_id) 
                DO UPDATE SET target_topic_id = EXCLUDED.target_topic_id
                """,
                (s_chat, s_topic, t_chat, t_topic)
            )
        else:
            c.execute(
                """
                INSERT INTO topic_mappings (source_chat_id, source_topic_id, target_chat_id, target_topic_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_chat_id, source_topic_id, target_chat_id) 
                DO UPDATE SET target_topic_id = excluded.target_topic_id
                """,
                (s_chat, s_topic, t_chat, t_topic)
            )

def get_topic_mapping(s_chat, s_topic, t_chat):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"SELECT target_topic_id FROM topic_mappings WHERE source_chat_id = {p} AND source_topic_id = {p} AND target_chat_id = {p}",
            (s_chat, s_topic, t_chat)
        )
        row = c.fetchone()
        return row[0] if row else None

def save_message_mapping(s_chat, s_msg, t_chat, t_msg):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute(
                """
                INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT(source_chat_id, source_msg_id, target_chat_id) 
                DO UPDATE SET target_msg_id = EXCLUDED.target_msg_id
                """,
                (s_chat, s_msg, t_chat, t_msg)
            )
        else:
            c.execute(
                """
                INSERT INTO message_mappings (source_chat_id, source_msg_id, target_chat_id, target_msg_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_chat_id, source_msg_id, target_chat_id) 
                DO UPDATE SET target_msg_id = excluded.target_msg_id
                """,
                (s_chat, s_msg, t_chat, t_msg)
            )

def get_message_mapping(s_chat, s_msg, t_chat):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(
            f"SELECT target_msg_id FROM message_mappings WHERE source_chat_id = {p} AND source_msg_id = {p} AND target_chat_id = {p}",
            (s_chat, s_msg, t_chat)
        )
        row = c.fetchone()
        return row[0] if row else None

def get_log_targets():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, target_id, target_type, target_name, bot_token FROM log_targets")
        return c.fetchall()

def add_log_target(target_id, target_type, target_name, bot_token=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute(
                "INSERT INTO log_targets (target_id, target_type, target_name, bot_token) VALUES (%s, %s, %s, %s) ON CONFLICT(target_id) DO UPDATE SET bot_token = EXCLUDED.bot_token",
                (target_id, target_type, target_name, bot_token)
            )
        else:
            c.execute(
                "INSERT INTO log_targets (target_id, target_type, target_name, bot_token) VALUES (?, ?, ?, ?) ON CONFLICT(target_id) DO UPDATE SET bot_token = excluded.bot_token",
                (target_id, target_type, target_name, bot_token)
            )

def remove_log_target(row_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM log_targets WHERE id = {p}", (row_id,))

def save_media_log(source_chat_id, source_message_id, log_target_id, file_id, media_type):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute(
                "INSERT INTO media_logs (source_chat_id, source_message_id, log_target_id, file_id, media_type) VALUES (%s, %s, %s, %s, %s) ON CONFLICT(source_chat_id, source_message_id, log_target_id) DO NOTHING",
                (source_chat_id, source_message_id, log_target_id, file_id, media_type)
            )
        else:
            c.execute(
                "INSERT OR IGNORE INTO media_logs (source_chat_id, source_message_id, log_target_id, file_id, media_type) VALUES (?, ?, ?, ?, ?)",
                (source_chat_id, source_message_id, log_target_id, file_id, media_type)
            )

def get_media_logs(limit=100, media_type=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        query = "SELECT source_message_id, log_target_id, file_id, media_type FROM media_logs"
        params = []
        if media_type:
            query += f" WHERE media_type = {p}"
            params.append(media_type)
        query += f" ORDER BY id DESC LIMIT {p}"
        params.append(limit)
        c.execute(query, tuple(params))
        return c.fetchall()

def get_vault_sources():
    with db_conn() as conn:
        c = conn.cursor()
        query = """
            SELECT DISTINCT p.source_id, p.source_title 
            FROM target_pairs p
            JOIN log_media m ON p.source_id = m.source_chat_id
        """
        c.execute(query)
        return c.fetchall()

def get_vaulted_media_for_source(source_id, bot_id=None, limit=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        
        query = f"""
            SELECT m.source_msg_id, m.file_id, m.media_type, m.caption, m.log_msg_id, m.bot_id, m.grouped_id
            FROM log_media m
            WHERE m.source_chat_id = {p}
        """
        params = [source_id]
        if bot_id:
            query += f" AND m.bot_id = {p}"
            params.append(bot_id)
            
        query += " ORDER BY m.source_msg_id ASC"
        
        if limit:
            query += f" LIMIT {p}"
            params.append(limit)
            
        c.execute(query, tuple(params))
        return c.fetchall()

def get_log_bot_stats(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        query = f"""
            SELECT p.source_id, p.source_title, COUNT(m.id) as item_count
            FROM log_media m
            JOIN target_pairs p ON m.source_chat_id = p.source_id
            WHERE m.bot_id = {p}
            GROUP BY p.source_id, p.source_title
            ORDER BY item_count DESC
        """
        c.execute(query, (bot_id,))
        return c.fetchall()

def get_target_pairs():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live, is_mirror, source_topic_id, target_topic_id, content_filter FROM target_pairs")
        return c.fetchall()

def get_target_pair(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT id, source_id, target_id, source_title, target_title, is_monitoring, is_live, is_mirror, source_topic_id, target_topic_id, content_filter FROM target_pairs WHERE id = {p}", (pair_id,))
        return c.fetchone()

def get_pair_stats(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT COUNT(*), SUM(CASE WHEN released = 0 THEN 1 ELSE 0 END) FROM collected_media WHERE pair_id = {p}", (pair_id,))
        row = c.fetchone()
        return {"total": row[0] or 0, "pending": row[1] or 0}

# -----------------------------
# Log Bot Helpers
# -----------------------------
def add_log_bot(token, username, bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute(
                "INSERT INTO log_bots (bot_token, bot_username, bot_id) VALUES (%s, %s, %s) ON CONFLICT(bot_token) DO UPDATE SET bot_username = EXCLUDED.bot_username, bot_id = EXCLUDED.bot_id",
                (token, username, bot_id)
            )
        else:
            c.execute(
                "INSERT INTO log_bots (bot_token, bot_username, bot_id) VALUES (?, ?, ?) ON CONFLICT(bot_token) DO UPDATE SET bot_username = excluded.bot_username, bot_id = excluded.bot_id",
                (token, username, bot_id)
            )

def get_log_bots():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT bot_token, bot_username, bot_id FROM log_bots")
        return c.fetchall()

def delete_log_bot(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM log_bots WHERE bot_id = {p}", (bot_id,))
        c.execute(f"DELETE FROM log_media WHERE bot_id = {p}", (bot_id,))

def clear_bot_logs(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"DELETE FROM log_media WHERE bot_id = {p}", (bot_id,))


async def run_vault_release(sender_bot, admin_chat_id, source_id, target_id, interval=1.5, limit=None, log_target_id=None, target_topic_id=None):
    """Releases vaulted media using the Userbot for forwarding."""
    task_key = f"vault_rel_{source_id}_{target_id}"
    if task_key in running_tasks:
        sender_bot.send_message(admin_chat_id, "⚠️ This release task is already running!")
        return

    running_tasks[task_key] = True
    vault_release_options[task_key] = {
        "source_id": source_id,
        "target_id": target_id,
        "total": 0,
        "success": 0,
        "failed": 0
    }
    
    try:
        # Get items to release
        items = get_vaulted_media_for_source(source_id, bot_id=log_target_id)
        if not items:
            sender_bot.send_message(admin_chat_id, "❌ No vaulted items found for this source.")
            return

        # Apply the limit logic
        if limit and isinstance(limit, int):
            items = items[:limit]

        total = len(items)
        vault_release_options[task_key]["total"] = total
        success = 0
        failed = 0
        
        stop_markup = InlineKeyboardMarkup().add(InlineKeyboardButton("🛑 Stop Transfer", callback_data=f"lb_stop_rel_{task_key}"))
        status_msg = sender_bot.send_message(admin_chat_id, f"🚀 *Initializing Transfer...*\nItems: `{total}`", parse_mode="Markdown")

        # Grouping items by grouped_id or source_msg_id
        grouped_items = []
        last_gid = None
        current_group = []
        
        for item in items:
            # item = (source_msg_id, file_id, m_type, caption, log_msg_id, bot_id, grouped_id)
            gid = item[6] # grouped_id
            if gid and gid == last_gid:
                current_group.append(item)
            else:
                if current_group:
                    grouped_items.append(current_group)
                current_group = [item]
                last_gid = gid
        if current_group:
            grouped_items.append(current_group)
            
        total_groups = len(grouped_items)
        
        for i, group in enumerate(grouped_items):
            if not running_tasks.get(task_key):
                sender_bot.send_message(admin_chat_id, "🛑 *Release Stopped* by user.")
                break
            
            try:
                # Process the group (might be 1 or multiple messages)
                first_item = group[0]
                bot_id = first_item[5]
                log_msg_ids = [int(it[4]) for it in group]
                captions = [it[3] for it in group]
                
                log_bot_entity = await userbot.get_input_entity(int(bot_id))
                
                # Fetch all messages in the group from the log bot
                msgs_to_forward = await userbot.get_messages(log_bot_entity, ids=log_msg_ids)
                if not isinstance(msgs_to_forward, list):
                    msgs_to_forward = [msgs_to_forward] if msgs_to_forward else []
                
                if msgs_to_forward:
                    # Filter out None/Failed fetches
                    msgs_to_forward = [m for m in msgs_to_forward if m]
                    
                    if msgs_to_forward:
                        # Use the first available caption for the album
                        main_caption = next((c for c in captions if c), "")
                        
                        try:
                            # Forward natively to preserve the forward tag and speed up the transfer
                            await userbot.forward_messages(
                                entity=int(target_id),
                                messages=msgs_to_forward,
                                reply_to=target_topic_id if target_topic_id else None
                            )
                        except Exception as fwd_err:
                            logger.warning(f"Failed to forward natively in vault release: {fwd_err}. Falling back to send_message.")
                            await userbot.send_message(
                                entity=int(target_id),
                                message=main_caption,
                                file=[m.media for m in msgs_to_forward] if len(msgs_to_forward) > 1 else msgs_to_forward[0].media,
                                reply_to=target_topic_id if target_topic_id else None 
                            )
                        success += len(msgs_to_forward)
                    else:
                        failed += len(group)
                else:
                    failed += len(group)
            except Exception as e:
                logger.error(f"Vault Release group error: {e}")
                failed += len(group)

            vault_release_options[task_key]["success"] = success
            vault_release_options[task_key]["failed"] = failed

            if (i + 1) % 5 == 0 or (i + 1) == total_groups:
                try: sender_bot.edit_message_text(f"📊 *Status:* `{i+1}/{total}`\n✅ Success: `{success}`\n❌ Failed: `{failed}`", admin_chat_id, status_msg.message_id, reply_markup=stop_markup, parse_mode="Markdown")
                except Exception: pass

            await asyncio.sleep(interval)
            
        sender_bot.send_message(admin_chat_id, f"✅ **Vault Release Completed**\n\n📦 **Total Released:** `{total}` items\n\n🎉 **Successful:** `{success}`\n❌ **Failed:** `{total - success}`", parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Global Release Error: {e}")
        sender_bot.send_message(admin_chat_id, f"❌ Engine Error: {e}")
    finally:
        running_tasks.pop(task_key, None)
        vault_release_options.pop(task_key, None)

def save_logged_media(bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, grouped_id=None):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        if USING_POSTGRES:
            c.execute(
                """INSERT INTO log_media (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, grouped_id) 
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                   ON CONFLICT(bot_id, source_chat_id, source_msg_id) DO UPDATE SET 
                   log_msg_id = EXCLUDED.log_msg_id, file_id = EXCLUDED.file_id, 
                   media_type = EXCLUDED.media_type, caption = EXCLUDED.caption, grouped_id = EXCLUDED.grouped_id""",
                (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, grouped_id)
            )
        else:
            c.execute(
                """INSERT INTO log_media (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, grouped_id) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(bot_id, source_chat_id, source_msg_id) DO UPDATE SET 
                   log_msg_id = excluded.log_msg_id, file_id = excluded.file_id, 
                   media_type = excluded.media_type, caption = excluded.caption, grouped_id = excluded.grouped_id""",
                (bot_id, log_msg_id, source_chat_id, source_msg_id, file_id, media_type, caption, grouped_id)
            )

def get_logged_media_stats(bot_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT COUNT(*) FROM log_media WHERE bot_id = {p}", (bot_id,))
        return c.fetchone()[0] or 0

def fetch_logged_media(bot_id, limit=1000):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        c.execute(f"SELECT source_chat_id, source_msg_id, file_id, media_type, caption FROM log_media WHERE bot_id = {p} ORDER BY timestamp DESC LIMIT {p}", (bot_id, limit))
        return c.fetchall()

def get_total_vaulted_count():
    with db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM log_media")
        return c.fetchone()[0] or 0

def get_all_vault_stats():
    with db_conn() as conn:
        c = conn.cursor()
        query = """
            SELECT m.source_chat_id, COALESCE(p.source_title, 'Direct/Private'), COUNT(m.id) as item_count
            FROM log_media m
            LEFT JOIN target_pairs p ON m.source_chat_id = p.source_id
            GROUP BY m.source_chat_id, p.source_title
            ORDER BY item_count DESC
        """
        c.execute(query)
        return c.fetchall()

# -----------------------------
# Global State
# -----------------------------
bot = telebot.TeleBot(BOT_TOKEN, threaded=False)
class UserbotFleetManager:
    def __init__(self):
        self.clients = {} # { user_id: TelegramClient }
        self.lock = asyncio.Lock()

    async def start_all(self):
        logger.info("📡 Initializing Userbot Fleet...")
        sessions = get_active_userbot_sessions()
        for sess in sessions:
            try:
                await self.start_client(sess)
            except Exception as e:
                logger.error(f"Failed to start Userbot {sess[7]} ({sess[1]}): {e}")

    async def start_client(self, sess):
        db_id, phone, api_id, api_hash, session_string, user_id, username, first_name, is_active = sess
        async with self.lock:
            if user_id in self.clients:
                try:
                    await self.clients[user_id].disconnect()
                except Exception:
                    pass
            
            from telethon.sessions import StringSession
            client = TelegramClient(
                StringSession(session_string),
                int(api_id),
                api_hash,
                device_model="PC 64bit",
                system_version="Windows 11",
                app_version="4.11.2",
                sequential_updates=True,
                receive_updates=True
            )
            client.catch_up = False
            await client.connect()
            client.catch_up = False
            client._me = await client.get_me()
            setup_automation_handlers(client)
            self.clients[user_id] = client
            logger.info(f"🚀 Userbot {first_name} (@{username}) started.")
            return client

    async def stop_client(self, user_id):
        async with self.lock:
            client = self.clients.pop(user_id, None)
            if client:
                try:
                    await client.disconnect()
                except Exception:
                    pass
                logger.info(f"🛑 Userbot {user_id} stopped.")

    def get_client(self, user_id):
        return self.clients.get(user_id)

    def get_all_clients(self):
        return list(self.clients.values())
        
    def get_any_client(self):
        for client in self.clients.values():
            if client and client.is_connected():
                return client
        if self.clients:
            return list(self.clients.values())[0]
        return None

class UserbotProxy:
    def __getattr__(self, name):
        client = userbot_fleet_manager.get_any_client()
        if not client:
            raise AttributeError("No active userbots connected in the fleet.")
        return getattr(client, name)

    def __bool__(self):
        return bool(userbot_fleet_manager.get_any_client())

    def __call__(self, *args, **kwargs):
        client = userbot_fleet_manager.get_any_client()
        if not client:
            raise RuntimeError("No active userbots connected in the fleet.")
        return client(*args, **kwargs)

userbot_fleet_manager = UserbotFleetManager()
userbot = UserbotProxy()
userbot_init_lock = asyncio.Lock()
userbot_lock = asyncio.Lock()

userbot_login_states = {}

async def complete_userbot_login_flow(sender_id, temp_client, event):
    try:
        session_string = temp_client.session.save()
        me = await temp_client.get_me()
        state_data = userbot_login_states.get(sender_id)
        phone = state_data["phone"]
        api_id = state_data["api_id"]
        api_hash = state_data["api_hash"]
        
        # Save session to DB
        add_userbot_session(phone, api_id, api_hash, session_string, me.id, me.username, me.first_name)
        
        # Start in fleet manager
        sessions = get_userbot_sessions()
        sess = [s for s in sessions if s[5] == me.id]
        if sess:
            await userbot_fleet_manager.start_client(sess[0])
            
        userbot_login_states.pop(sender_id, None)
        await event.reply(f"✅ **Success! Manager Account Promoted to Userbot**\n\nAccount: `{me.first_name}` (@{me.username or 'No Username'})\n\nThis manager account is now running as a manager userbot client in the fleet.")
    except Exception as e:
        logger.error(f"Error completing userbot login flow: {e}")
        await event.reply(f"❌ **Failed to complete login setup:** {e}")
        userbot_login_states.pop(sender_id, None)

# (State dictionaries declared globally at the top of the file)

def get_active_tasks_report():
    active = [k for k, v in running_tasks.items() if v]
    if not active:
        return "📭 *No ongoing tasks are currently active.*"
    
    lines = ["⚙️ *Ongoing Active Tasks:*"]
    for key in active:
        if key.startswith("coll_"):
            try:
                pid = int(key.split("_")[1])
                opts = collection_options.get(key, {})
                s_title = opts.get("s_title", f"Pair ID {pid}")
                scanned = opts.get("scanned", 0)
                collected = opts.get("collected", 0)
                sent = opts.get("sent_count", 0)
                filtered = opts.get("filtered", 0)
                dups = opts.get("duplicates", 0)
                status = opts.get("status", "Processing")
                progress = opts.get("progress", 0)
                lines.append(
                    f"\n📥 *Collection (Pair {pid}):* `{s_title}`\n"
                    f"• Status: `{status}` ({progress}%)\n"
                    f"• Scanned: `{scanned}` | Collected: `{collected}`\n"
                    f"• Forwarded: `{sent}` | Filtered: `{filtered}` | Dups: `{dups}`"
                )
            except Exception as e:
                lines.append(f"\n📥 *Collection ({key}):* Error reading: {e}")
        elif key.startswith("hist_"):
            try:
                pid = int(key.split("_")[1])
                opts = history_options.get(key, {})
                s_title = opts.get("s_title", f"Pair ID {pid}")
                scanned = opts.get("scanned", 0)
                collected = opts.get("collected", 0)
                sent = opts.get("sent_count", 0)
                limit = opts.get("limit")
                limit_str = f"/{limit}" if limit else ""
                lines.append(
                    f"\n📜 *History Scrape (Pair {pid}):* `{s_title}`\n"
                    f"• Scanned: `{scanned}`\n"
                    f"• Collected: `{collected}{limit_str}` | Forwarded: `{sent}`"
                )
            except Exception as e:
                lines.append(f"\n📜 *History Scrape ({key}):* Error reading: {e}")
        elif key.startswith("vault_rel_"):
            try:
                opts = vault_release_options.get(key, {})
                src = opts.get("source_id", "Unknown")
                tgt = opts.get("target_id", "Unknown")
                total = opts.get("total", 0)
                success = opts.get("success", 0)
                failed = opts.get("failed", 0)
                lines.append(
                    f"\n🚀 *Vault Release:* `{src}` ➡️ `{tgt}`\n"
                    f"• Total: `{total}`\n"
                    f"• Success: `{success}` | Failed: `{failed}`"
                )
            except Exception as e:
                lines.append(f"\n🚀 *Vault Release ({key}):* Error reading: {e}")
        else:
            lines.append(f"\n⚙️ *Task:* `{key}`")
            
    return "\n".join(lines)

def get_progress_bar(pct):
    """Generates a 20-character diamond progress bar string representing progress percentage."""
    filled = int(round((pct / 100.0) * 20))
    hollow = 20 - filled
    return "◆" * filled + "◇" * hollow

def get_collection_status_text(task_key, is_done=False, status_text=None):
    opts = collection_options.get(task_key, {})
    if not opts:
        return "❌ Task not found."
        
    s_title = opts.get("s_title", "Unknown Source")
    
    # Extract stats or default them
    fetched = opts.get("scanned", 0)
    forwarded = opts.get("sent_count", 0)
    duplicates = opts.get("duplicates", 0)
    deleted = opts.get("deleted", 0)
    skipped = opts.get("skipped", 0)
    filtered = opts.get("filtered", 0)
    
    progress = opts.get("progress", 0)
    
    if status_text is None:
        if is_done:
            status_text = "Completed"
        elif task_key not in running_tasks or not running_tasks[task_key]:
            status_text = "Stopped"
        else:
            status_text = opts.get("status", "Processing")
            
    if is_done:
        hourglass_text = "✅ Done"
    elif status_text == "Stopped":
        hourglass_text = "🛑 Stopped"
    elif status_text == "Cancelled":
        hourglass_text = "❌ Cancelled"
    else:
        hourglass_text = "⏳ Processing"
        
    text = (
        f"✨ <b>FORWARD STATUS: {s_title}</b>\n\n"
        "<blockquote>"
        f"📥 <b>FETCHED:</b> {fetched}\n"
        f"📤 <b>FORWARDED:</b> {forwarded}\n"
        f"🔄 <b>DUPLICATES:</b> {duplicates}\n"
        f"🗑️ <b>DELETED:</b> {deleted}\n"
        f"⏭️ <b>SKIPPED:</b> {skipped}\n"
        f"🎯 <b>FILTERED:</b> {filtered}\n"
        f"⚡ <b>STATUS:</b> {status_text}\n"
        f"📊 <b>PROGRESS:</b> {progress}%\n\n"
        f"✨ {hourglass_text}"
        "</blockquote>"
    )
    return text

def get_collection_markup(pair_id):
    task_key = f"coll_{pair_id}"
    options = collection_options.setdefault(task_key, {
        "instant_release": False,
        "instant_filter": "everything",
        "collect_filter": "everything",
        "progress": 0
    })
    instant_release = options.get("instant_release", False)
    instant_filter = options.get("instant_filter", "everything")
    collect_filter = options.get("collect_filter", "everything")
    progress = options.get("progress", 0)
    
    markup = InlineKeyboardMarkup()
    
    # Progress bar button at the top row
    progress_bar = get_progress_bar(progress)
    btn_progress = InlineKeyboardButton(progress_bar, callback_data="progress_bar_click")
    markup.row(btn_progress)
    
    btn_stop = InlineKeyboardButton("🛑 Stop Collection", callback_data=f"pair_stop_task_coll_{pair_id}")
    
    # Collect filter button
    col_map = {
        "everything": "🔄 All Content",
        "media": "🖼️ Media Only",
        "text": "📝 Text Only",
        "file": "📁 Files Only"
    }
    col_text = col_map.get(collect_filter, "🔄 All Content")
    btn_collect_filter = InlineKeyboardButton(f"Collect: {col_text}", callback_data=f"pair_coll_cfilter_{pair_id}")
    
    if instant_release:
        btn_toggle = InlineKeyboardButton("📥 Hold Release", callback_data=f"pair_coll_toggle_{pair_id}_hold")
        
        cf_map = {"everything": "🔄 All Content", "media": "🖼️ Media Only", "text": "📝 Text Only"}
        cf_text = cf_map.get(instant_filter, "🔄 All Content")
        btn_filter = InlineKeyboardButton(f"Filter: {cf_text}", callback_data=f"pair_coll_filter_{pair_id}")
        
        markup.row(btn_stop, btn_toggle)
        markup.row(btn_collect_filter, btn_filter)
    else:
        btn_toggle = InlineKeyboardButton("⚡ Instant Release", callback_data=f"pair_coll_toggle_{pair_id}_instant")
        markup.row(btn_stop, btn_toggle)
        markup.row(btn_collect_filter)
    return markup


release_options = {} # Track release options: { "pid_source_type": "everything"/"media"/"text" }

def get_release_markup(pid, source_type):
    key = f"{pid}_{source_type}"
    curr_filter = release_options.setdefault(key, "everything")
    
    cf_map = {"everything": "🔄 All Content", "media": "🖼️ Media Only", "text": "📝 Text Only"}
    cf_text = cf_map.get(curr_filter, "🔄 All Content")
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton(f"Release Filter: {cf_text}", callback_data=f"pair_rel_filter_{source_type}_{pid}"))
    markup.add(
        InlineKeyboardButton("⚡ Instant Release", callback_data=f"pair_rel_now_{source_type}_{pid}"),
        InlineKeyboardButton("⏰ Scheduled (Slow)", callback_data=f"pair_rel_slow_{source_type}_{pid}")
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"pair_release_{pid}"))
    return markup

def stop_task(task_key):
    if task_key in running_tasks:
        running_tasks[task_key] = False
        return True
    return False

def is_task_running(task_key):
    return running_tasks.get(task_key, False)

# -----------------------------
# UI Helpers
# -----------------------------
def get_dashboard_text():
    clients = userbot_fleet_manager.get_all_clients()
    connected_clients = [c for c in clients if c.is_connected()]
    status = "🟢 ACTIVE" if connected_clients else "🔴 OFFLINE"
    
    text = f"✨ *SYSTEM CONSOLE*\n"
    text += f"Status: `{status}`\n"
    text += f"Connected Userbots: `{len(connected_clients)}` / `{len(get_userbot_sessions())}`\n"
    if connected_clients:
        names = []
        for client in connected_clients:
            me = getattr(client, '_me', None)
            if me:
                names.append(me.first_name or "User")
        if names:
            text += f"Accounts: `{', '.join(names)}`\n"
    
    text += "\n_Manage your automation pairs below:_"
    return text

def get_dashboard_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    clients = userbot_fleet_manager.get_all_clients()
    connected_clients = [c for c in clients if c.is_connected()]
    
    if connected_clients:
        markup.add(InlineKeyboardButton("🎯 Target Pairs", callback_data="pairs_main"))
        markup.add(InlineKeyboardButton("📬 Private Media Forwarder", callback_data="pm_fwd_main"))
    
    markup.add(InlineKeyboardButton("👤 Manage Userbots", callback_data="userbots_main"))
    
    if connected_clients:
        markup.add(InlineKeyboardButton("🔒 Vault Console", callback_data="vault_main"))
        markup.add(InlineKeyboardButton("🚫 Ban List", callback_data="banlist_main"))
    
    return markup

def vault_console_markup():
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("🚀 Release from Vault", callback_data="vault_rel_main"),
        InlineKeyboardButton("➕ Add Vault Bot", callback_data="log_bot_add_start")
    )
    markup.add(InlineKeyboardButton("🤖 Manage Vault Bots", callback_data="log_bot_main"))
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup

def log_bot_list_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    bots = get_log_bots()
    for token, username, bot_id in bots:
        stats = get_logged_media_stats(bot_id)
        markup.add(InlineKeyboardButton(f"🤖 @{username} ({stats} items)", callback_data=f"log_bot_view_{bot_id}"))
    
    markup.add(InlineKeyboardButton("➕ Add Vault Bot", callback_data="log_bot_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back to Vault Console", callback_data="vault_main"))
    return markup

def log_bot_view_markup(bot_id):
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(
        InlineKeyboardButton("📥 Fetch Logs", callback_data=f"log_bot_fetch_{bot_id}"),
        InlineKeyboardButton("🗑 Remove", callback_data=f"log_bot_delete_confirm_{bot_id}")
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="log_bot_main"))
    return markup

def get_pm_fwd_text():
    enabled = get_setting("pm_media_forwarding_enabled") == "1"
    status_emoji = "🟢 ENABLED" if enabled else "🔴 DISABLED"
    allow_dest = get_setting("pm_media_forwarding_allow_destructive") == "1"
    dest_emoji = "🟢 ALLOWED (Downloaded & Saved)" if allow_dest else "🔴 BLOCKED"
    
    text = f"📬 *PRIVATE MEDIA FORWARDER*\n\n"
    text += f"Status: `{status_emoji}`\n"
    text += f"Self-Destructing / View Once Bypass: `{dest_emoji}`\n\n"
    text += "When active, any media (photos, videos, documents, etc.) sent by users in private chats with the userbot will be automatically forwarded to the designated target chats.\n\n"
    
    targets_str = get_setting("pm_media_forwarding_targets") or ""
    target_ids = [t.strip() for t in targets_str.split(",") if t.strip()]
    
    text += "🎯 *Active Targets:*\n"
    if not target_ids:
        text += "_No target chats added yet._"
    else:
        for idx, tid in enumerate(target_ids):
            title = None
            try:
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    c.execute(f"SELECT target_title FROM target_pairs WHERE target_id = {p} LIMIT 1", (int(tid),))
                    row = c.fetchone()
                    if row:
                        title = row[0]
            except Exception:
                pass
            
            chat_label = title if title else f"Chat ID `{tid}`"
            text += f"{idx + 1}. {chat_label} (`{tid}`)\n"
            
    return text

def get_pm_fwd_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    enabled = get_setting("pm_media_forwarding_enabled") == "1"
    toggle_label = "🔴 Disable System" if enabled else "🟢 Enable System"
    allow_dest = get_setting("pm_media_forwarding_allow_destructive") == "1"
    dest_btn_label = "❄️ Block Self-Destructing" if allow_dest else "🔥 Allow Self-Destructing"
    
    markup.add(
        InlineKeyboardButton(toggle_label, callback_data="pm_fwd_toggle"),
        InlineKeyboardButton(dest_btn_label, callback_data="pm_fwd_toggle_destructive"),
        InlineKeyboardButton("➕ Add Target Chat", callback_data="pm_fwd_add_target"),
        InlineKeyboardButton("🗑 Clear All Targets", callback_data="pm_fwd_clear_targets"),
        InlineKeyboardButton("💬 Private Chat Monitor Settings", callback_data="pm_mon_settings")
    )
    markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup

def get_pm_mon_text():
    enabled = get_setting("pm_chat_monitoring_enabled") == "1"
    status_emoji = "🟢 ENABLED" if enabled else "🔴 DISABLED"
    dest_id = get_setting("pm_chat_monitoring_destination")
    
    dest_label = "_Not Set_"
    if dest_id:
        title = None
        try:
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                c.execute(f"SELECT target_title FROM target_pairs WHERE target_id = {p} LIMIT 1", (int(dest_id),))
                row = c.fetchone()
                if row:
                    title = row[0]
        except Exception:
            pass
        dest_label = title if title else f"`{dest_id}`"
        
    text = f"💬 *PRIVATE CHAT MONITOR*\n\n"
    text += f"Status: `{status_emoji}`\n"
    text += f"Destination Chat: {dest_label}\n\n"
    text += "When active, any text messages or media received by the userbot account in private chats (from non-manager users) will be automatically forwarded to the destination chat.\n"
    return text

def get_pm_mon_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    enabled = get_setting("pm_chat_monitoring_enabled") == "1"
    toggle_label = "🔴 Disable Monitor" if enabled else "🟢 Enable Monitor"
    
    markup.add(
        InlineKeyboardButton(toggle_label, callback_data="pm_mon_toggle"),
        InlineKeyboardButton("🎯 Set Destination Chat", callback_data="pm_mon_set_dest")
    )
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="pm_fwd_main"))
    return markup

def pairs_list_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    pairs = get_target_pairs()
    for pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, c_filter in pairs:
        stats = get_pair_stats(pid)
        mon_status = "👁️" if is_mon else ""
        live_status = "⚡" if is_live else ""
        topic_status = "🧵" if (s_topic or t_topic) else ""
        
        btn_text = f"📁 {topic_status}{s_title} ➔ {t_title} {mon_status}{live_status} ({stats['pending']})"
        markup.add(InlineKeyboardButton(btn_text, callback_data=f"pair_view_{pid}"))
    
    markup.add(InlineKeyboardButton("➕ Add New Pair", callback_data="pair_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

async def show_pair_view(chat_id, message_id, pid):
    try:
        row = get_target_pair(pid)
        if not row:
            bot.send_message(chat_id, f"❌ Pair not found (ID: {pid}). It may have been deleted.")
            return
            
        pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, c_filter = row
        stats = get_pair_stats(pid)
        mon_status = "🟢 Running" if is_mon else "🔴 Stopped"
        live_status = "🟢 Running" if is_live else "🔴 Stopped"
        mir_status = "🟢 Enabled" if is_mir else "🔴 Disabled"
        
        src_text = f"`{s_title}`" + (f" • Topic: `{s_topic}`" if s_topic else "")
        tgt_text = f"`{t_title}`" + (f" • Topic: `{t_topic}`" if t_topic else "")

        text = (
            f"📁 *Pair Management*\n\n"
            f"Source: {src_text}\n"
            f"Target: {tgt_text}\n\n"
            f"📊 Collected: `{stats['total']}`\n"
            f"📥 Pending: `{stats['pending']}`\n\n"
            f"🤖 *Automation Status:*\n"
            f"Monitor: `{mon_status}`\n"
            f"Live: `{live_status}`\n"
            f"Mirror: `{mir_status}`"
        )
        
        # Resolve target chats asynchronously to check if both are topics/forums
        both_forums = False
        if userbot and userbot.is_connected():
            try:
                s_chat = await resolve_target_id(userbot, sid)
                t_chat = await resolve_target_id(userbot, tid)
                both_forums = getattr(s_chat, "forum", False) and getattr(t_chat, "forum", False)
            except Exception as e:
                logger.error(f"Forum check failed: {e}")
                
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=pair_view_markup(pid, both_forums), parse_mode="Markdown")
        except Exception as e:
            if "message is not modified" in str(e):
                pass
            else:
                raise e
    except Exception as e:
        logger.error(f"Pair View Error: {e}")
        bot.send_message(chat_id, f"❌ Error opening pair management: {e}")

def pair_view_markup(pair_id, show_mirror=False):
    pair = get_target_pair(pair_id)
    if not pair: return InlineKeyboardMarkup()
    
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, c_filter = pair
    markup = InlineKeyboardMarkup(row_width=2)
    
    mon_btn = "🛑 Stop Monitor" if is_mon else "👁️ Monitor"
    live_btn = "🛑 Stop Live" if is_live else "⚡ Live Forward"
    mir_btn = "🛑 Stop Mirror" if is_mir else "🔀 Mirror Mode"
    
    markup.add(
        InlineKeyboardButton(mon_btn, callback_data=f"pair_toggle_mon_{pair_id}"),
        InlineKeyboardButton(live_btn, callback_data=f"pair_toggle_live_{pair_id}")
    )
    
    # Unconditionally show Mirror Mode button so the admin can always toggle it!
    markup.add(InlineKeyboardButton(mir_btn, callback_data=f"pair_toggle_mir_{pair_id}"))
    
    # Content Filter Button
    cf = pair[10] or "everything"
    cf_map = {"everything": "🔄 All Content", "media": "🖼️ Media Only", "text": "📝 Text Only", "file": "📁 Files Only"}
    cf_text = cf_map.get(cf, "🔄 All Content")
    markup.add(InlineKeyboardButton(f"Filter: {cf_text}", callback_data=f"pair_toggle_filter_{pair_id}"))
    
    # Retrieve unreleased source counts
    mon, scr, col = get_pair_source_counts(pair_id)
    total_pending = mon + scr + col
    
    # Check if a manual task is running
    is_hist = is_task_running(f"hist_{pair_id}")
    is_coll = is_task_running(f"coll_{pair_id}")
    is_rel = is_task_running(f"rel_monitor_{pair_id}") or is_task_running(f"rel_scraper_{pair_id}") or is_task_running(f"rel_collection_{pair_id}")
    
    if is_hist: markup.add(InlineKeyboardButton("🛑 Stop History Scrape", callback_data=f"pair_stop_task_hist_{pair_id}"))
    else: markup.add(InlineKeyboardButton("📜 History Scraper", callback_data=f"pair_hist_menu_{pair_id}"))
    
    if is_coll: markup.add(InlineKeyboardButton("🛑 Stop Collection", callback_data=f"pair_stop_task_coll_{pair_id}"))
    else: markup.add(InlineKeyboardButton("📥 Collect Now", callback_data=f"pair_collect_{pair_id}"))
    
    # Single Release Button showing total unreleased count
    if is_rel: markup.add(InlineKeyboardButton("🛑 Stop Release", callback_data=f"pair_stop_task_rel_monitor_{pair_id}"))
    else: markup.add(InlineKeyboardButton(f"🚀 Release Now ({total_pending})", callback_data=f"pair_release_{pair_id}"))

    markup.add(InlineKeyboardButton("🗑 Delete Pair", callback_data=f"pair_delete_confirm_{pair_id}"))
    markup.add(InlineKeyboardButton("🔙 Back to Pairs", callback_data="pairs_main"))
    return markup
async def get_topic_selection_markup(chat_id, prefix):
    markup = InlineKeyboardMarkup(row_width=1)
    if not userbot or not userbot.is_connected():
        return None
    
    try:
        # Force entity resolution first to avoid ChannelInvalidError / PeerIdInvalidError
        entity = await resolve_target_id(userbot, chat_id)
        result = await userbot(functions.messages.GetForumTopicsRequest(
            peer=entity,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        topics = getattr(result, "topics", [])
        
        if not topics:
            markup.add(InlineKeyboardButton("⚠️ No Topics Found", callback_data="noop"))
            if prefix == "sel_src_topic":
                markup.add(InlineKeyboardButton("➕ Create Topic", callback_data=f"create_topic_prompt_{chat_id}"))
            # Even if no topics found, allow selecting the whole group
            markup.add(InlineKeyboardButton("🏢 Select Entire Group", callback_data=f"{prefix}_{chat_id}_0"))
        else:
            if prefix == "sel_src_topic":
                markup.add(InlineKeyboardButton("➕ Create Topic", callback_data=f"create_topic_prompt_{chat_id}"))
            # Add option to select the entire group as a source/target
            markup.add(InlineKeyboardButton("🏢 Select Entire Group", callback_data=f"{prefix}_{chat_id}_0"))
            for topic in topics:
                # Telethon Forum topics: 'id' is the permanent topic starter/anchor message ID
                topic_id = getattr(topic, "id", None)
                topic_title = getattr(topic, "title", f"Topic {topic_id}")
                if topic_id:
                    markup.add(
                        InlineKeyboardButton(
                            f"🧵 {topic_title}",
                            callback_data=f"{prefix}_{chat_id}_{topic_id}"
                        )
                    )
                    
    except Exception as e:
        logger.error(f"Telethon Topic Fetch Error: {e}")
        markup.add(InlineKeyboardButton("❌ Failed To Load Topics", callback_data="noop"))
        
    markup.add(InlineKeyboardButton("🔙 Back to Chats", callback_data="pair_add_start"))
    return markup

# -----------------------------
# Userbot Logic
# -----------------------------

async def get_chat_selection_markup(prefix, page=0):
    markup = InlineKeyboardMarkup(row_width=1)
    if not userbot or not userbot.is_connected():
        return None
    
    chats = []
    # Fetch enough dialogs to populate selection
    async for dialog in userbot.iter_dialogs(limit=100):
        entity = dialog.entity
        # Filter for relevant chat types
        if isinstance(entity, (types.Chat, types.Channel, types.User)):
            chats.append(dialog)
    
    # Pagination
    start = page * 10
    end = start + 10
    page_items = chats[start:end]
    
    for dialog in page_items:
        chat = dialog.entity
        is_forum = getattr(chat, "forum", False)
        
        # Better visual distinction
        if isinstance(chat, types.Channel):
            if is_forum:
                icon = "🏛️"
                title = f"『 TOPIC 』 {chat.title}"
            elif chat.broadcast:
                icon = "📢"
                title = chat.title or "Channel"
            else:
                icon = "👥"
                title = chat.title or "Group"
        elif isinstance(chat, types.Chat):
            icon = "👥"
            title = chat.title or "Group"
        elif isinstance(chat, types.User):
            if chat.bot: icon = "🤖"
            else: icon = "👤"
            title = f"{chat.first_name or ''} {chat.last_name or ''}".strip() or "Private Chat"
        else:
            icon = "💬"
            title = "Unknown"

        markup.add(
            InlineKeyboardButton(
                f"{icon} {title}",
                callback_data=f"{prefix}_{chat.id}"
            )
        )
    
    nav = []
    if page > 0: nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"{prefix}_page_{page-1}"))
    if end < len(chats): nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"{prefix}_page_{page+1}"))
    if nav: markup.add(*nav)
    
    # Dynamic Chat Search button
    markup.add(InlineKeyboardButton("🔍 Search Group", callback_data=f"sel_search|{prefix}"))
    
    markup.add(InlineKeyboardButton("🔙 Cancel", callback_data="pairs_main"))
    return markup


def user_account_markup(user_id=None):
    markup = InlineKeyboardMarkup(row_width=2)
    suffix = f"_{user_id}" if user_id else ""
    markup.add(
        InlineKeyboardButton("👥 Groups", callback_data=f"user_acc_list_groups_0{suffix}"),
        InlineKeyboardButton("📢 Channels", callback_data=f"user_acc_list_channels_0{suffix}")
    )
    markup.add(
        InlineKeyboardButton("👤 Private", callback_data=f"user_acc_list_private_0{suffix}"),
        InlineKeyboardButton("🤖 Bots", callback_data=f"user_acc_list_bots_0{suffix}")
    )
    if user_id:
        markup.add(InlineKeyboardButton("🔙 Back to Userbot details", callback_data=f"userbot_view_{user_id}"))
    else:
        markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
    return markup


def banlist_markup():
    markup = InlineKeyboardMarkup(row_width=1)
    banned = get_banned_users()
    for uid, uname in banned:
        identifier = uid if uid else uname
        label = f"🚫 {uname if uname else uid}"
        markup.add(InlineKeyboardButton(label, callback_data=f"unban_confirm_{identifier}"))
    
    markup.add(InlineKeyboardButton("➕ Add to Ban List", callback_data="ban_add_start"))
    markup.add(InlineKeyboardButton("🔙 Back", callback_data="dash_main"))
    return markup

async def get_or_create_target_topic(client, target_chat_id, topic_title, source_chat_id=None, source_topic_id=None, icon_emoji_id=None):
    """
    Search for a topic by title in target chat. If not found, create it.
    Uses database mapping first, then topic_cache.
    """
    if not topic_title: return None
    
    t_chat_id = int(target_chat_id)
    title_key = topic_title.lower().strip()
    logger.info(f"TOPIC_SEARCH: Looking for '{topic_title}' (Key: '{title_key}') in {t_chat_id}")
    
    # 1) Check Database Mapping (Most Reliable)
    if source_chat_id and source_topic_id:
        existing_mapping = get_topic_mapping(source_chat_id, source_topic_id, t_chat_id)
        if existing_mapping:
            return existing_mapping
    
    # 2) Check Cache
    if t_chat_id in topic_cache and title_key in topic_cache[t_chat_id]:
        res = topic_cache[t_chat_id][title_key]
        if source_chat_id and source_topic_id:
            save_topic_mapping(source_chat_id, source_topic_id, t_chat_id, res)
        return res
    
    # 3) Fetch Topics from Telegram
    try:
        # Force entity resolution first to avoid ChannelInvalidError / PeerIdInvalidError
        target_chat = await resolve_target_id(client, t_chat_id)
        result = await client(functions.messages.GetForumTopicsRequest(
            peer=target_chat,
            offset_date=0,
            offset_id=0,
            offset_topic=0,
            limit=100
        ))
        
        if t_chat_id not in topic_cache:
            topic_cache[t_chat_id] = {}
            
        for topic in result.topics:
            topic_cache[t_chat_id][topic.title.lower().strip()] = topic.id
            
        if title_key in topic_cache[t_chat_id]:
            res = topic_cache[t_chat_id][title_key]
            if source_chat_id and source_topic_id:
                save_topic_mapping(source_chat_id, source_topic_id, t_chat_id, res)
            return res
            
        # 4) Create if not found
        logger.info(f"MIRROR: Creating new topic '{topic_title}' in {t_chat_id} (Icon: {icon_emoji_id})")
        created = await client(functions.messages.CreateForumTopicRequest(
            peer=target_chat,
            title=topic_title,
            icon_emoji_id=int(icon_emoji_id) if icon_emoji_id else None
        ))
        
        await asyncio.sleep(1)
        res_after = await client(functions.messages.GetForumTopicsRequest(
            peer=target_chat,
            offset_date=0, offset_id=0, offset_topic=0, limit=20
        ))
        for t in res_after.topics:
            topic_cache[t_chat_id][t.title.lower().strip()] = t.id
            
        final_id = topic_cache[t_chat_id].get(title_key)
        if final_id and source_chat_id and source_topic_id:
            save_topic_mapping(source_chat_id, source_topic_id, t_chat_id, final_id)
            
        return final_id
    except Exception as e:
        logger.error(f"Mirroring Error (get_or_create): {e}")
        return None

async def _start_userbot_unlocked():
    await userbot_fleet_manager.start_all()
    if userbot_fleet_manager.get_all_clients():
        return True, "Userbot fleet started"
    return False, "No active userbots connected"

async def start_userbot():
    async with userbot_init_lock:
        return await _start_userbot_unlocked()

async def ensure_userbot():
    """Ensures at least one userbot is connected and ready."""
    async with userbot_init_lock:
        clients = userbot_fleet_manager.get_all_clients()
        if not clients:
            ok, msg = await _start_userbot_unlocked()
            if not ok: return False, msg
            clients = userbot_fleet_manager.get_all_clients()
            
        connected_any = False
        for client in clients:
            if not client.is_connected():
                try:
                    await client.connect()
                    setup_automation_handlers(client)
                except Exception:
                    pass
            if client.is_connected():
                connected_any = True
                
        if connected_any:
            return True, "Connected"
        return False, "No connected userbots"

async def forward_to_log_bots(client, messages, source_chat_id):
    """Sends collected content (single or album) to all registered log bots."""
    if not messages: return
    bots = get_log_bots()
    if not bots: return
    
    for token, username, bot_id in bots:
        await vault_media(client, messages, int(source_chat_id), int(bot_id), username)
        await asyncio.sleep(1.0)

async def vault_media(client, messages, source_chat_id, log_chat_id, t_name):
    """Helper to forward to vault and save the permanent File IDs (handles albums)"""
    try:
        if not messages: return
        first_msg = messages[0]
        
        # RESOLVE ENTITY
        target_peer = await resolve_target_id(client, log_chat_id)

        # Metadata for Log Bot extraction (only on the first message of album if multiple)
        metadata = f"SID: {source_chat_id} | MID: {first_msg.id}\n"
        caption_text = metadata + (first_msg.message or "")
        
        is_restricted = False
        auto_mirror = False
        try:
            source_chat = await resolve_target_id(client, source_chat_id)
            if getattr(source_chat, 'noforwards', False):
                is_restricted = True
            if getattr(source_chat, 'forum', False) and getattr(target_peer, 'forum', False):
                auto_mirror = True
        except Exception:
            source_chat = int(source_chat_id)

        dest_topic_id = None
        if auto_mirror:
            s_top = getattr(first_msg.reply_to, 'reply_to_top_id', None) or (first_msg.reply_to.reply_to_msg_id if first_msg.reply_to else None)
            if not s_top and getattr(first_msg, 'forum_topic', False):
                s_top = first_msg.id
            if not s_top and first_msg.reply_to_msg_id:
                s_top = first_msg.reply_to_msg_id
            
            if s_top:
                mapped = get_topic_mapping(source_chat_id, s_top, log_chat_id)
                if mapped:
                    dest_topic_id = mapped
                else:
                    forum = getattr(first_msg.reply_to, "forum_topic", None) if first_msg.reply_to else None
                    src_title = getattr(forum, "title", None)
                    src_icon = getattr(forum, "icon_emoji_id", None)
                    if not src_title:
                        try:
                            res = await client(functions.messages.GetForumTopicsRequest(
                                peer=source_chat, offset_date=0, offset_id=0, offset_topic=0, limit=100
                            ))
                            for t in res.topics:
                                if t.id == s_top:
                                    src_title = t.title
                                    src_icon = getattr(t, "icon_emoji_id", None)
                                    break
                        except Exception: pass
                    if src_title:
                        dest_topic_id = await get_or_create_target_topic(
                            client, log_chat_id, src_title, source_chat_id, s_top, icon_emoji_id=src_icon
                        )

        try:
            if not is_restricted:
                try:
                    vaulted_result = await client.forward_messages(
                        entity=target_peer,
                        messages=messages,
                        from_peer=source_chat_id,
                        reply_to=int(dest_topic_id) if dest_topic_id else None
                    )
                except Exception as fwd_err:
                    logger.warning(f"Native forward to vault failed: {fwd_err}. Falling back to send_message.")
                    vaulted_result = await client.send_message(
                        entity=target_peer,
                        file=[m.media for m in messages] if len(messages) > 1 else messages[0].media,
                        message=caption_text,
                        reply_to=int(dest_topic_id) if dest_topic_id else None
                    )
            else:
                vaulted_result = await client.send_message(
                    entity=target_peer,
                    file=[m.media for m in messages] if len(messages) > 1 else messages[0].media,
                    message=caption_text,
                    reply_to=int(dest_topic_id) if dest_topic_id else None
                )
            await asyncio.sleep(2)
        except errors.FloodWaitError as fwe:
            logger.warning(f"⏳ VAULT FLOOD: Waiting {fwe.seconds}s...")
            await notify_admin_flood_wait(client, "Vault Storage Upload", fwe.seconds)
            await notify_admin_flood_wait(client, "Vault Storage Upload", fwe.seconds)
            await asyncio.sleep(fwe.seconds)
            return await vault_media(client, messages, source_chat_id, log_chat_id, t_name) # Retry
        except Exception as e:
            if any(x in str(e).lower() for x in ["protected", "forward", "restricted", "noforwards", "forbidden", "reference"]):
                logger.warning(f"🛡️ VAULT: Protected media detected but could not forward to vault bot. Vaulting skipped.")
            else:
                raise e
            
        if vaulted_result:
            # vaulted_result is a list if it was an album, or a single Message object
            v_msgs = vaulted_result if isinstance(vaulted_result, list) else [vaulted_result]
            
            for i, v_m in enumerate(v_msgs):
                orig_m = messages[i]
                logger.info(f"✅ VAULT: Message {orig_m.id} logged to @{t_name} -> Log ID: {v_m.id}")
                save_logged_media(
                    bot_id=int(log_chat_id),
                    log_msg_id=int(v_m.id),
                    source_chat_id=int(source_chat_id),
                    source_msg_id=int(orig_m.id),
                    file_id=None,
                    media_type=type(orig_m.media).__name__ if orig_m.media else "text",
                    caption=orig_m.message or "",
                    grouped_id=orig_m.grouped_id
                )
    except Exception as e:
        logger.error(f"VAULT ERROR for @{t_name}: {e}")

def update_telethon_entity_cache(client, peer):
    """Safely and dynamically updates Telethon's internal entity caches."""
    if not peer:
        return
    try:
        if hasattr(client, '_entity_cache'):
            cache = client._entity_cache
            if hasattr(cache, 'add'):
                cache.add(peer)
            elif hasattr(cache, 'extend'):
                cache.extend([], [peer])
            elif isinstance(cache, dict):
                cache[peer.id] = peer
    except Exception as e:
        logger.error(f"Failed to update client._entity_cache: {e}")

    try:
        if hasattr(client, '_mb_entity_cache'):
            mb_cache = client._mb_entity_cache
            if hasattr(mb_cache, 'extend'):
                mb_cache.extend([], [peer])
            elif hasattr(mb_cache, 'add'):
                mb_cache.add(peer)
            elif isinstance(mb_cache, dict):
                mb_cache[peer.id] = peer
    except Exception as e:
        logger.error(f"Failed to update client._mb_entity_cache: {e}")

async def send_mirrored_content(client, tid, messages, default_t_topic, is_mir, sid, pre_downloaded=None):
    """Unified Hub for mirrored sending with native Forum Topic support and Client Failover."""
    downloaded_files = []
    try:
        if not messages: return
        first_msg = messages[0]
        
        # 1. Build Client Pool for Failover
        clients_pool = [client]
        fallback_str = get_setting("fallback_userbot_ids") or ""
        fallback_ids = [int(x) for x in fallback_str.split(",") if x.strip()]
        for c in userbot_fleet_manager.get_all_clients():
            if c.is_connected() and c != client:
                if hasattr(c, '_me') and c._me and c._me.id in fallback_ids:
                    clients_pool.append(c)

        # 2. Pre-download media if needed (so we don't have to download multiple times during failover)
        has_media = any(m.media for m in messages)
        downloaded_paths = {}
        if pre_downloaded:
            if isinstance(pre_downloaded, dict):
                downloaded_paths = pre_downloaded
            elif isinstance(pre_downloaded, list):
                media_msgs = [m for m in messages if m.media]
                for idx, m in enumerate(media_msgs):
                    if idx < len(pre_downloaded):
                        downloaded_paths[m.id] = pre_downloaded[idx]
        
        sent = None
        last_error = Exception("No clients available to send")
        
        for active_client in clients_pool:
            client_name = getattr(active_client._me, 'first_name', 'Unknown')
            logger.info(f"🔄 Mirror attempt using client: {client_name} (@{getattr(active_client._me, 'username', 'unknown')})")
            
            # Download media if not already done and we need to upload
            if has_media and not downloaded_paths:
                for m in messages:
                    if m.media:
                        try:
                            async with media_semaphore:
                                path = await active_client.download_media(m)
                            if path:
                                downloaded_paths[m.id] = path
                                downloaded_files.append(path)
                        except Exception as de:
                            logger.error(f"Mirror download failed for client {client_name}: {de}")
            
            dest_topic_id = default_t_topic
            try:
                # 0. Resolve Target Chat Entity
                try:
                    target_entity = await resolve_target_id(active_client, tid)
                except Exception as e:
                    logger.error(f"Failed to resolve target ID {tid}: {e}")
                    target_entity = int(tid)

                # 1. Resolve Topic Mapping
                if is_mir:
                    source_top = getattr(first_msg.reply_to, 'reply_to_top_id', None) or (first_msg.reply_to.reply_to_msg_id if first_msg.reply_to else None)
                    if not source_top and getattr(first_msg, 'forum_topic', False):
                        source_top = first_msg.id
                    if not source_top and first_msg.reply_to_msg_id:
                        source_top = first_msg.reply_to_msg_id
                    if source_top:
                        forum = getattr(first_msg.reply_to, "forum_topic", None)
                        src_title = getattr(forum, "title", None)
                        src_icon = None
                        if not src_title:
                            try:
                                resolved_sid = await resolve_target_id(active_client, sid)
                                res = await active_client(functions.messages.GetForumTopicsRequest(
                                    peer=resolved_sid,
                                    offset_date=0,
                                    offset_id=0,
                                    offset_topic=0,
                                    limit=100
                                ))
                                for t in res.topics:
                                    if t.id == source_top:
                                        src_title = t.title
                                        src_icon = getattr(t, "icon_emoji_id", None)
                                        break
                            except Exception: pass
                        
                        if src_title:
                            logger.info(f"MIRROR: Resolved source topic title: '{src_title}' (Icon: {src_icon})")
                            dest_topic_id = await get_or_create_target_topic(active_client, tid, src_title, sid, source_top, icon_emoji_id=src_icon)

                # 2. Check if Target is a Forum
                is_forum = getattr(target_entity, 'forum', False) if not isinstance(target_entity, int) else False

                # 3. Resolve Reply Header
                reply_header = None
                if is_forum:
                    reply_header = int(dest_topic_id) if dest_topic_id else None
                    top_msg_id = None
                    if first_msg.reply_to:
                        top_msg_id = getattr(first_msg.reply_to, 'reply_to_top_id', None)
                        if not top_msg_id and first_msg.reply_to.reply_to_msg_id:
                            top_msg_id = first_msg.reply_to.reply_to_msg_id
                    if top_msg_id:
                        mapped_top = get_message_mapping(sid, top_msg_id, tid)
                        if mapped_top:
                            reply_header = int(mapped_top)
                    if first_msg.reply_to_msg_id and (not top_msg_id or first_msg.reply_to_msg_id != top_msg_id):
                        mapped = get_message_mapping(sid, first_msg.reply_to_msg_id, tid)
                        if mapped:
                            reply_header = int(mapped)
                else:
                    if first_msg.reply_to_msg_id:
                        mapped = get_message_mapping(sid, first_msg.reply_to_msg_id, tid)
                        if mapped:
                            reply_header = int(mapped)

                # 4. Construct Album Text and Files List
                album_text = next((msg.message for msg in messages if msg.message), "")
                files_to_send = []
                for msg in messages:
                    if msg.media:
                        local_path = downloaded_paths.get(msg.id)
                        if local_path and os.path.exists(local_path):
                            files_to_send.append(local_path)
                        else:
                            files_to_send.append(msg.media)
                
                file_to_send = files_to_send if len(files_to_send) > 1 else (files_to_send[0] if files_to_send else None)
                
                if not album_text.strip() and not file_to_send:
                    logger.warning(f"⚠️ MIRROR: Skipping message {first_msg.id} because it has no text content and no media/file.")
                    return

                for attempt in range(3):
                    try:
                        # Attempt native forward first if not restricted/pre-downloaded to preserve the forward tag
                        if not pre_downloaded and not downloaded_files:
                            try:
                                import random
                                random_ids = [random.randint(-9223372036854775808, 9223372036854775807) for _ in messages]
                                top_msg_id_val = int(reply_header) if (is_forum and reply_header) else None
                                
                                sent_fwd = await active_client(functions.messages.ForwardMessagesRequest(
                                    from_peer=await active_client.get_input_entity(int(sid)),
                                    id=[msg.id for msg in messages],
                                    to_peer=target_entity,
                                    random_id=random_ids,
                                    top_msg_id=top_msg_id_val
                                ))
                                if sent_fwd:
                                    fwd_msgs = []
                                    if hasattr(sent_fwd, 'updates'):
                                        for u in sent_fwd.updates:
                                            if type(u).__name__ in ["UpdateNewMessage", "UpdateNewChannelMessage"]:
                                                fwd_msgs.append(u.message)
                                    if fwd_msgs:
                                        first_id = fwd_msgs[0].id
                                        sent = fwd_msgs if len(fwd_msgs) > 1 else fwd_msgs[0]
                                        logger.info(f"✅ MIRROR: Sent via native ForwardMessagesRequest to {tid} -> MSG ID: {first_id}")
                                        save_message_mapping(sid, first_msg.id, tid, first_id)
                                        break
                            except Exception as fwd_err:
                                logger.warning(f"Native forward in mirror failed: {fwd_err}. Trying send_message copy fallback...")

                        # Fallback to copy/re-upload system
                        if not sent:
                            if file_to_send:
                                async with media_semaphore:
                                    sent = await active_client.send_message(
                                        entity=target_entity, 
                                        message=album_text, 
                                        file=file_to_send,
                                        reply_to=reply_header
                                    )
                            else:
                                sent = await active_client.send_message(
                                    entity=target_entity, 
                                    message=album_text, 
                                    reply_to=reply_header
                                )
                            if sent:
                                first_id = sent[0].id if isinstance(sent, list) else sent.id
                                logger.info(f"✅ MIRROR: Sent via send_message to {tid} -> MSG ID: {first_id}")
                                save_message_mapping(sid, first_msg.id, tid, first_id)
                                break # Success!
                    except errors.FloodWaitError as fwe:
                        logger.warning(f"⏳ MIRROR FLOOD: Client {getattr(active_client._me, 'username', 'unknown')} hit rate limit. Waiting {fwe.seconds}s...")
                        last_error = fwe
                        # If we have other clients, try the next client instead of sleeping here
                        if active_client != clients_pool[-1]:
                            logger.info("🔄 FAILOVER: Trying next userbot in pool immediately.")
                            break
                        else:
                            await notify_admin_flood_wait(active_client, "Mirroring Content", fwe.seconds)
                            await asyncio.sleep(fwe.seconds)
                    except (errors.rpcerrorlist.WorkerBusyTooLongRetryError, errors.rpcerrorlist.TimedOutError):
                        await asyncio.sleep(2)
                    except Exception as e:
                        # Fallback download & upload
                        err_msg = str(e).lower()
                        is_protected_error = any(x in err_msg for x in ["protected", "forward", "restricted", "noforwards", "forbidden", "reference", "peer", "empty", "invalid or you can't do that operation"])
                        if is_protected_error and not pre_downloaded and not downloaded_files and any(m.media for m in messages):
                            logger.info(f"🛡️ MIRROR: Protected/empty media error detected ({e}). Attempting download & upload fallback...")
                            for m in messages:
                                if m.media:
                                    try:
                                        async with media_semaphore:
                                            path = await active_client.download_media(m)
                                        if path:
                                            downloaded_paths[m.id] = path
                                            downloaded_files.append(path)
                                    except Exception as de:
                                        logger.error(f"Mirror download fallback failed: {de}")
                            if downloaded_files:
                                file_to_send = downloaded_files if len(downloaded_files) > 1 else downloaded_files[0]
                                try:
                                    async with media_semaphore:
                                        sent = await active_client.send_message(
                                            entity=target_entity,
                                            message=album_text,
                                            file=file_to_send,
                                            reply_to=reply_header
                                        )
                                    if sent:
                                        first_id = sent[0].id if isinstance(sent, list) else sent.id
                                        logger.info(f"✅ MIRROR: Sent via fallback to {tid} -> MSG ID: {first_id}")
                                        save_message_mapping(sid, first_msg.id, tid, first_id)
                                        break
                                except Exception as fe:
                                    e = fe
                        
                        if not sent:
                            if reply_header is not None:
                                next_reply_header = None
                                if is_forum and dest_topic_id and reply_header != int(dest_topic_id):
                                    next_reply_header = int(dest_topic_id)
                                logger.warning(f"⚠️ MIRROR: Failed to send with reply_to={reply_header} ({e}). Retrying with reply_to={next_reply_header}...")
                                try:
                                    sent = await active_client.send_message(
                                        entity=target_entity, 
                                        message=album_text, 
                                        file=file_to_send,
                                        reply_to=next_reply_header
                                    )
                                    if sent:
                                        first_id = sent[0].id if isinstance(sent, list) else sent.id
                                        logger.info(f"✅ MIRROR: Sent after reply downgrade to {tid} -> MSG ID: {first_id}")
                                        save_message_mapping(sid, first_msg.id, tid, first_id)
                                        break
                                except Exception as e2:
                                    if next_reply_header is not None:
                                        logger.warning(f"⚠️ MIRROR: Failed to send with reply_to={next_reply_header} ({e2}). Retrying with reply_to=None...")
                                        try:
                                            sent = await active_client.send_message(
                                                entity=target_entity, 
                                                message=album_text, 
                                                file=file_to_send,
                                                reply_to=None
                                            )
                                            if sent:
                                                first_id = sent[0].id if isinstance(sent, list) else sent.id
                                                logger.info(f"✅ MIRROR: Sent after final reply clear to {tid} -> MSG ID: {first_id}")
                                                save_message_mapping(sid, first_msg.id, tid, first_id)
                                                break
                                        except Exception as e3:
                                            e = e3
                                    else:
                                        e = e2
                        logger.error(f"MIRROR SEND ATTEMPT {attempt+1} FAILED: {e}")
                        last_error = e
                        if attempt == 2:
                            raise e
                if sent:
                    break
            except Exception as outer_e:
                logger.error(f"Outer active_client error: {outer_e}")
                last_error = outer_e
        if not sent:
            raise last_error
    except Exception as e:
        logger.error(f"Global Mirror Error: {e}")
        raise e
    finally:
        for path in downloaded_files:
            if os.path.exists(path):
                try: os.remove(path)
                except Exception: pass


def contains_link(message_text, entities=None):
    if not message_text:
        return False
    import re
    url_pattern = re.compile(
        r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    )
    if url_pattern.search(message_text):
        return True
    if entities:
        for ent in entities:
            ent_type = type(ent).__name__
            if ent_type in ["MessageEntityUrl", "MessageEntityTextUrl"]:
                return True
    return False


def get_specific_media_type(media):
    if not media:
        return "text"
    name = type(media).__name__
    if name == "MessageMediaPhoto":
        return "photo"
    elif name == "MessageMediaDocument":
        doc = getattr(media, "document", None)
        if doc:
            mime = getattr(doc, "mime_type", "").lower()
            if "video" in mime:
                return "video"
            if "audio" in mime or "voice" in mime:
                return "file"
            # Check attributes
            for attr in getattr(doc, "attributes", []):
                attr_name = type(attr).__name__
                if attr_name == "DocumentAttributeVideo":
                    return "video"
                if attr_name == "DocumentAttributeAudio":
                    return "file"
        return "file"
    return "file"

def get_pair_source_counts(pair_id):
    with db_conn() as conn:
        c = conn.cursor()
        p = get_placeholder()
        # Monitor
        c.execute(f"SELECT COUNT(*) FROM collected_media WHERE pair_id = {p} AND COALESCE(added_by, 'monitor') = 'monitor' AND released = 0", (pair_id,))
        mon = c.fetchone()[0] or 0
        # Scraper
        c.execute(f"SELECT COUNT(*) FROM collected_media WHERE pair_id = {p} AND COALESCE(added_by, 'monitor') = 'scraper' AND released = 0", (pair_id,))
        scr = c.fetchone()[0] or 0
        # Collection (Collect Now)
        c.execute(f"SELECT COUNT(*) FROM collected_media WHERE pair_id = {p} AND COALESCE(added_by, 'monitor') = 'collection' AND released = 0", (pair_id,))
        col = c.fetchone()[0] or 0
        return mon, scr, col


async def queue_failed_messages(client, pair_id, sid, tid, t_topic, messages, is_restricted):
    logger.info(f"📥 QUEUE: Queueing {len(messages)} messages for pair {pair_id} due to rate limit/error...")
    
    # Pre-download all media files locally
    downloaded_paths = {}
    for msg in messages:
        if msg.media:
            try:
                logger.info(f"📥 QUEUE: Pre-downloading media for message {msg.id}...")
                async with media_semaphore:
                    path = await client.download_media(msg)
                if path:
                    downloaded_paths[msg.id] = path
                    logger.info(f"📥 QUEUE: Downloaded to {path}")
            except Exception as de:
                logger.error(f"📥 QUEUE: Failed to download media for msg {msg.id}: {de}")
                
    # Add to database queue
    with db_conn() as conn:
        c = conn.cursor()
        for msg in messages:
            m_path = downloaded_paths.get(msg.id)
            # If it has no media but it was part of the batch, we can still queue it (as text) or if it has media and download succeeded
            if msg.media and not m_path:
                logger.warning(f"📥 QUEUE: Skipping queueing of message {msg.id} because media download failed.")
                continue
                
            p = get_placeholder()
            if USING_POSTGRES:
                c.execute(
                    """INSERT INTO pending_media_queue 
                    (pair_id, source_chat_id, source_msg_id, media_file_path, caption, grouped_id, target_chat_id, target_topic_id, is_restricted) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (pair_id, sid, msg.id, m_path, msg.message or "", msg.grouped_id, tid, t_topic, 1 if is_restricted else 0)
                )
            else:
                c.execute(
                    """INSERT INTO pending_media_queue 
                    (pair_id, source_chat_id, source_msg_id, media_file_path, caption, grouped_id, target_chat_id, target_topic_id, is_restricted) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (pair_id, sid, msg.id, m_path, msg.message or "", msg.grouped_id, tid, t_topic, 1 if is_restricted else 0)
                )

async def media_queue_worker():
    """Background worker to process queued media when rate limits are cleared."""
    logger.info("⏰ Queue Worker: Started.")
    while True:
        try:
            # 1. Fetch all pending queue items
            items = []
            with db_conn() as conn:
                c = conn.cursor()
                c.execute("SELECT id, pair_id, source_chat_id, source_msg_id, media_file_path, caption, grouped_id, target_chat_id, target_topic_id, is_restricted FROM pending_media_queue ORDER BY id ASC")
                items = c.fetchall()
                
            if not items:
                await asyncio.sleep(15)
                continue
                
            # 2. Group items by grouped_id (if not NULL) or process individually
            grouped = {}
            ungrouped = []
            for item in items:
                gid = item[6]
                if gid:
                    grouped.setdefault(gid, []).append(item)
                else:
                    ungrouped.append(item)
                    
            batches = []
            for gid, group in grouped.items():
                batches.append((min(it[0] for it in group), group))
            for item in ungrouped:
                batches.append((item[0], [item]))
                
            batches.sort(key=lambda x: x[0])
            
            for min_id, batch in batches:
                primary_client = userbot_fleet_manager.get_any_client()
                if not primary_client or not primary_client.is_connected():
                    logger.warning("⏰ Queue Worker: No active userbot client. Skipping batch.")
                    await asyncio.sleep(15)
                    break
                
                clients_pool = [primary_client]
                fallback_str = get_setting("fallback_userbot_ids") or ""
                fallback_ids = [int(x) for x in fallback_str.split(",") if x.strip()]
                for c in userbot_fleet_manager.get_all_clients():
                    if c.is_connected() and c != primary_client:
                        if hasattr(c, '_me') and c._me and c._me.id in fallback_ids:
                            clients_pool.append(c)
                
                first_item = batch[0]
                queue_ids = [it[0] for it in batch]
                pair_id = first_item[1]
                sid = first_item[2]
                target_chat_id = first_item[7]
                target_topic_id = first_item[8]
                is_restricted = first_item[9]
                
                logger.info(f"⏰ Queue Worker: Sending batch of {len(batch)} items to target {target_chat_id}...")
                
                album_text = next((it[5] for it in batch if it[5]), "")
                files = [it[4] for it in batch if it[4]]
                reply_header = target_topic_id
                
                sent = None
                last_err = Exception("No clients available to send")
                
                resolved_files = []
                for active_client in clients_pool:
                    client_name = getattr(active_client._me, 'first_name', 'Unknown')
                    logger.info(f"⏰ Queue Worker: Attempting send using client {client_name}...")
                    try:
                        target_entity = await resolve_target_id(active_client, target_chat_id)
                    except Exception as te:
                        logger.error(f"⏰ Queue Worker: Failed to resolve target {target_chat_id} for client {client_name}: {te}")
                        last_err = te
                        continue
                    
                    # Self-healing download logic for missing local files
                    resolved_files = []
                    for f_path, msg_id in zip(files, [it[3] for it in batch]):
                        if f_path:
                            if os.path.exists(f_path):
                                resolved_files.append(f_path)
                            else:
                                logger.warning(f"⏰ Queue Worker: File {f_path} not found. Attempting to re-download message {msg_id} from {sid}...")
                                try:
                                    msg = await active_client.get_messages(int(sid), ids=int(msg_id))
                                    if msg and msg.media:
                                        async with media_semaphore:
                                            new_path = await active_client.download_media(msg)
                                        if new_path and os.path.exists(new_path):
                                            resolved_files.append(new_path)
                                            # Update database so we don't have to download again
                                            with db_conn() as conn:
                                                c = conn.cursor()
                                                if USING_POSTGRES:
                                                    c.execute("UPDATE pending_media_queue SET media_file_path = %s WHERE source_msg_id = %s AND source_chat_id = %s", (new_path, msg_id, sid))
                                                else:
                                                    c.execute("UPDATE pending_media_queue SET media_file_path = ? WHERE source_msg_id = ? AND source_chat_id = ?", (new_path, msg_id, sid))
                                except Exception as re_err:
                                    logger.error(f"⏰ Queue Worker: Failed to re-download media: {re_err}")
                        else:
                            resolved_files.append(None)
                    
                    try:
                        active_files = [f for f in resolved_files if f]
                        if active_files:
                            file_payload = active_files if len(active_files) > 1 else active_files[0]
                            async with media_semaphore:
                                sent = await active_client.send_message(
                                    entity=target_entity,
                                    message=album_text,
                                    file=file_payload,
                                    reply_to=reply_header
                                )
                        else:
                            if files:
                                logger.error(f"⏰ Queue Worker: Skipping send using client {client_name} because all files failed to resolve/re-download.")
                                last_err = Exception("All files failed to resolve/re-download")
                                continue
                                
                            sent = await active_client.send_message(
                                entity=target_entity,
                                message=album_text,
                                reply_to=reply_header
                            )
                        if sent:
                            logger.info(f"⏰ Queue Worker: Successfully sent batch via {client_name}! MSG ID: {sent[0].id if isinstance(sent, list) else sent.id}")
                            break
                    except errors.FloodWaitError as fwe:
                        logger.warning(f"⏰ Queue Worker: Client {client_name} hit rate limit. Must wait {fwe.seconds}s.")
                        last_err = fwe
                        if active_client != clients_pool[-1]:
                            logger.info("🔄 FAILOVER: Trying next userbot in pool for queue worker.")
                            continue
                        else:
                            await notify_admin_flood_wait(active_client, "Queue Worker", fwe.seconds)
                            await asyncio.sleep(fwe.seconds)
                            break
                    except Exception as se:
                        logger.error(f"⏰ Queue Worker: Client {client_name} failed to send batch: {se}")
                        last_err = se
                        if active_client != clients_pool[-1]:
                            continue
                        else:
                            await asyncio.sleep(5)
                            break
                
                if sent:
                    active_files = [f for f in resolved_files if f]
                    # Send to vault bot if configured using the client that successfully sent it
                    for token, username, bot_id in get_log_bots():
                        metadata = f"SID: {sid} | MID: {first_item[3]}\n"
                        caption_text = metadata + album_text
                        try:
                            await active_client.send_message(
                                entity=int(bot_id),
                                file=active_files if len(active_files) > 1 else (active_files[0] if active_files else None),
                                message=caption_text
                            )
                        except Exception as ve:
                            logger.error(f"⏰ Queue Worker: Failed to send to vault bot {bot_id}: {ve}")
                            
                    # Delete from database
                    with db_conn() as conn:
                        c = conn.cursor()
                        for q_id in queue_ids:
                            if USING_POSTGRES:
                                c.execute("DELETE FROM pending_media_queue WHERE id = %s", (q_id,))
                            else:
                                c.execute("DELETE FROM pending_media_queue WHERE id = ?", (q_id,))
                                
                    # Delete local files (both original and newly downloaded) to free space
                    all_paths_to_delete = set(files + active_files)
                    for f_path in all_paths_to_delete:
                        if f_path and os.path.exists(f_path):
                            try:
                                os.remove(f_path)
                                logger.info(f"⏰ Queue Worker: Cleaned up local file {f_path}")
                            except Exception as fe:
                                logger.error(f"⏰ Queue Worker: Failed to delete file {f_path}: {fe}")
                                
                    # Wait 5 to 10 seconds before sending the next one to avoid rate limits
                    import random
                    wait_time = random.uniform(5.0, 10.0)
                    logger.info(f"⏰ Queue Worker: Waiting {wait_time:.2f}s before next send...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"⏰ Queue Worker: Batch failed to send on all clients: {last_err}")
                    if not isinstance(last_err, errors.FloodWaitError):
                        await asyncio.sleep(15)
                    break
        except Exception as e:
            logger.error(f"⏰ Queue Worker error: {e}")
            await asyncio.sleep(15)


async def process_automation_pipeline(client, messages, source_chat_id):
    """
    Unified execution core. 
    Correctly updates network entity maps and routes mixed message batches securely.
    """
    pairs = get_target_pairs()
    if not messages: 
        return
    first_msg = messages[0]
    
    # 1. Topic Identification Routing
    msg_topic_anchor = None
    if first_msg.reply_to:
        msg_topic_anchor = getattr(first_msg.reply_to, 'reply_to_top_id', None) or first_msg.reply_to.reply_to_msg_id
    if not msg_topic_anchor and getattr(first_msg, 'forum_topic', False):
        msg_topic_anchor = first_msg.id
    if not msg_topic_anchor and first_msg.reply_to_msg_id:
        msg_topic_anchor = first_msg.reply_to_msg_id

    # 2. FIXED: Reliable Live Entity Fetching & Map Syncing
    is_protected_flow = False
    try:
        chat_peer = await resolve_target_id(client, source_chat_id)
        # Check initial flag status
        if getattr(chat_peer, 'noforwards', False):
            # Force structural updates over the network wire to purge stale attributes
            from telethon.tl.functions.channels import GetChannelsRequest
            from telethon.tl.types import InputChannel
            
            if hasattr(chat_peer, 'access_hash'):
                input_channel = InputChannel(chat_peer.id, chat_peer.access_hash)
                res = await client(GetChannelsRequest(id=[input_channel]))
                if res and res.chats:
                    fresh_peer = res.chats[0]
                    is_protected_flow = getattr(fresh_peer, 'noforwards', False)
                    # Correctly bind the structural entity metadata to Telethon's internal maps
                    update_telethon_entity_cache(client, fresh_peer)
            else:
                is_protected_flow = getattr(chat_peer, 'noforwards', False)
    except Exception as e:
        logger.error(f"Failed to refresh stale entity cache for chat {source_chat_id}: {e}")
        is_protected_flow = False

    # 3. Pre-download files safely if the chat is truly restricted
    media_to_file = {} # {msg_id: local_path}
    if is_protected_flow:
        logger.info(f"🛡️ PIPELINE: Protected source chat verified (-100{str(source_chat_id).replace('-100', '')}). Pre-downloading media...")
        for msg in messages:
            if msg.media:
                try:
                    async with media_semaphore:
                        path = await client.download_media(msg)
                    if path:
                        media_to_file[msg.id] = path
                except errors.FloodWaitError as fwe:
                    logger.warning(f"⏳ PIPELINE FLOOD: Download limit hit. Sleeping {fwe.seconds}s...")
                    await notify_admin_flood_wait(client, "Pipeline Download", fwe.seconds)
                    await notify_admin_flood_wait(client, "Pipeline Download", fwe.seconds)
                    await asyncio.sleep(fwe.seconds)
                    try:
                        async with media_semaphore:
                            path = await client.download_media(msg)
                        if path: media_to_file[msg.id] = path
                    except Exception: pass
                except Exception as e:
                    logger.error(f"Failed to copy media asset: {e}")

    try:
        already_vaulted = False
        msg_chat_str = str(source_chat_id).replace("-100", "")
        
        for pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf in pairs:
            source_id_str = str(sid).replace("-100", "")
            if source_id_str != msg_chat_str:
                continue

            # Topic Context Verification
            topic_filter_id = None
            if s_topic and str(s_topic).strip().lower() not in ["", "0", "none"]:
                try: topic_filter_id = int(s_topic)
                except Exception: pass
            if topic_filter_id is not None and str(msg_topic_anchor) != str(topic_filter_id):
                continue

            # Content Filter Validation Rules (Evaluated on per-message context)
            cf_val = cf or "everything"
            valid_messages = []
            for msg in messages:
                # If message contains link, bypass the media/text/file filter entirely
                if contains_link(msg.message, msg.entities):
                    valid_messages.append(msg)
                    continue
                    
                m_type = get_specific_media_type(msg.media)
                if cf_val == "media" and m_type not in ["photo", "video"]:
                    continue
                if cf_val == "text" and m_type != "text":
                    continue
                if cf_val == "file" and m_type != "file":
                    continue
                valid_messages.append(msg)
                
            if not valid_messages:
                continue

            # Execution Step A: Database Logging Operations
            if is_mon:
                with db_conn() as conn:
                    c = conn.cursor()
                    for msg in valid_messages:
                        m_type = get_specific_media_type(msg.media)
                        rel_val = 1 if is_live else 0
                        if USING_POSTGRES:
                            c.execute(
                                "INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption, added_by, released) VALUES (%s, %s, %s, %s, %s, 'monitor', %s) ON CONFLICT DO NOTHING",
                                (pid, sid, msg.id, m_type, msg.message or "", rel_val)
                            )
                        else:
                            c.execute(
                                "INSERT OR IGNORE INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption, added_by, released) VALUES (?, ?, ?, ?, ?, 'monitor', ?)",
                                (pid, sid, msg.id, m_type, msg.message or "", rel_val)
                            )

            # Execution Step B: Live Mirror/Forward Engine Routine
            if is_live:
                has_media = any(m.media for m in valid_messages)
                if is_protected_flow and has_media and not any(m.id in media_to_file for m in valid_messages):
                    logger.warning(f"🛡️ PIPELINE: Skipping live mirror for target {tid} (Download Failed). Queueing instead...")
                    await queue_failed_messages(client, pid, sid, tid, t_topic, valid_messages, is_protected_flow)
                else:
                    try:
                        await send_mirrored_content(
                            client, 
                            tid, 
                            valid_messages, 
                            t_topic, 
                            is_mir, 
                            sid, 
                            pre_downloaded=media_to_file if (is_protected_flow and has_media) else None
                        )
                    except Exception as sme:
                        logger.error(f"Live mirror/forward failed: {sme}. Queueing...")
                        await queue_failed_messages(client, pid, sid, tid, t_topic, valid_messages, is_protected_flow)

            # Execution Step C: Backup Storage Vault Allocation
            if is_mon and not already_vaulted:
                if is_protected_flow:
                    has_media = any(m.media for m in valid_messages)
                    if has_media:
                        files_to_vault = [media_to_file.get(m.id) or m.media for m in valid_messages if m.media]
                        if files_to_vault:
                            file_payload = files_to_vault if len(files_to_vault) > 1 else files_to_vault[0]
                            for token, username, bot_id in get_log_bots():
                                metadata = f"SID: {source_chat_id} | MID: {first_msg.id}\n"
                                caption_text = metadata + (first_msg.message or "")
                                try:
                                    vaulted_result = await client.send_message(
                                        entity=int(bot_id),
                                        file=file_payload,
                                        message=caption_text
                                    )
                                    if vaulted_result:
                                        v_msgs = vaulted_result if isinstance(vaulted_result, list) else [vaulted_result]
                                        for i, v_m in enumerate(v_msgs):
                                            if i < len(valid_messages):
                                                orig_m = valid_messages[i]
                                                save_logged_media(
                                                    bot_id=int(bot_id), log_msg_id=int(v_m.id),
                                                    source_chat_id=int(source_chat_id), source_msg_id=int(orig_m.id),
                                                    file_id=None, media_type=type(orig_m.media).__name__ if orig_m.media else "text",
                                                    caption=orig_m.message or "", grouped_id=orig_m.grouped_id
                                                )
                                except Exception as e:
                                    logger.error(f"Error executing backup synchronization pipeline: {e}")
                    else:
                        asyncio.create_task(forward_to_log_bots(client, valid_messages, sid))
                else:
                    asyncio.create_task(forward_to_log_bots(client, valid_messages, sid))
                already_vaulted = True

    finally:
        for temp_path in media_to_file.values():
            if os.path.exists(temp_path):
                try: os.remove(temp_path)
                except Exception: pass

def parse_telegram_message_link(text):
    import re
    text = text.strip()
    # Private message link: t.me/c/123456789/4567
    m = re.search(r'(?:t\.me|telegram\.me)/c/(\d+)/(\d+)', text)
    if m:
        chat_id = int(f"-100{m.group(1)}")
        msg_id = int(m.group(2))
        return {"type": "private", "chat_id": chat_id, "message_id": msg_id}
    # Public message link: t.me/username/4567
    m = re.search(r'(?:t\.me|telegram\.me)/([a-zA-Z0-9_]{5,})/(\d+)', text)
    if m:
        username = m.group(1)
        msg_id = int(m.group(2))
        return {"type": "public", "username": username, "message_id": msg_id}
    return None

async def process_and_send_message_link(client: TelegramClient, link_info, sender_id):
    try:
        if link_info["type"] == "private":
            chat_ref = link_info["chat_id"]
        else:
            chat_ref = link_info["username"]
            
        try:
            chat = await resolve_target_id(client, chat_ref)
        except Exception as e:
            if link_info["type"] == "public":
                try:
                    from telethon.tl.functions.channels import JoinChannelRequest
                    chat = await client.get_entity(chat_ref)
                    await client(JoinChannelRequest(chat))
                except Exception as join_err:
                    logger.error(f"Auto-join failed for {chat_ref}: {join_err}")
                    raise e
            else:
                raise e
                
        msg_id = link_info["message_id"]
        
        async with userbot_lock:
            msgs = await client.get_messages(chat, ids=msg_id)
            
        if not msgs:
            logger.error(f"Message ID {msg_id} not found in chat {chat_ref}")
            return False, "Message not found in the chat."
            
        me = await client.get_me()
        targets = [me.id]
        if sender_id and sender_id != me.id:
            targets.append(sender_id)
            
        is_restricted = getattr(chat, 'noforwards', False)
        
        if is_restricted:
            logger.info(f"Chat {chat_ref} is restricted. Downloading media locally.")
            temp_path = await client.download_media(msgs)
            
            for target in targets:
                try:
                    if temp_path:
                        await client.send_message(target, file=temp_path, message=msgs.message or "")
                    elif msgs.message:
                        await client.send_message(target, msgs.message)
                except Exception as e:
                    logger.error(f"Failed to send restricted media to target {target}: {e}")
                    
            if temp_path and os.path.exists(temp_path):
                try: os.remove(temp_path)
                except Exception: pass
        else:
            for target in targets:
                try:
                    await client.forward_messages(target, msgs)
                except Exception as e:
                    logger.error(f"Failed to forward message to target {target}: {e}")
                    
        return True, "Success"
    except Exception as e:
        logger.error(f"Error processing message link: {e}")
        return False, str(e)

def setup_automation_handlers(client: TelegramClient):
    if getattr(client, '_automation_handlers_registered', False):
        logger.info("Automation handlers already registered on this client. Skipping.")
        return
    client._automation_handlers_registered = True

    processed_reactions_cache = []
    processed_manager_reactions_cache = []

    async def process_manager_reaction(client, peer_id, msg_id, msg, manager_id):
        try:
            album_msgs = [msg]
            if msg.grouped_id is not None:
                try:
                    entity = await client.get_entity(peer_id)
                    msg_ids = list(range(max(1, msg_id - 10), msg_id + 11))
                    async with userbot_lock:
                        siblings = await client.get_messages(entity, ids=msg_ids)
                    album_msgs = [m for m in siblings if m and m.grouped_id == msg.grouped_id]
                    album_msgs.sort(key=lambda x: x.id)
                    if not album_msgs:
                        album_msgs = [msg]
                except Exception as e:
                    logger.error(f"Failed to fetch album siblings for manager reaction msg_id={msg_id}: {e}")
                    album_msgs = [msg]

            logger.warning(f"Downloading media for manager {manager_id} from message {msg_id} in {peer_id}...")
            temp_paths = []
            album_text = next((m.message for m in album_msgs if m.message), "")
            for m in album_msgs:
                if m.media:
                    try:
                        path = await client.download_media(m)
                        if path:
                            temp_paths.append(path)
                    except Exception as e:
                        logger.error(f"Failed to download media for manager reaction msg_id={m.id}: {e}")

            try:
                if temp_paths:
                    await client.send_file(manager_id, file=temp_paths, caption=album_text)
                elif album_text:
                    await client.send_message(manager_id, album_text)
                logger.warning(f"Successfully sent manager reacted message to manager {manager_id}")
            except Exception as e:
                logger.error(f"Failed to send manager reacted message to manager {manager_id}: {e}")

            for temp_path in temp_paths:
                if temp_path and os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except Exception: pass
        except Exception as ex:
            logger.error(f"Error in process_manager_reaction: {ex}")

    @client.on(events.Raw)
    async def reaction_handler(event):
        msg = None
        peer_id = None
        msg_id = None
        
        if isinstance(event, types.UpdateMessageReactions):
            try:
                peer_id = client.get_peer_id(event.peer)
                if asyncio.iscoroutine(peer_id):
                    peer_id = await peer_id
                msg_id = event.msg_id
            except Exception:
                return
        elif isinstance(event, (types.UpdateEditMessage, types.UpdateEditChannelMessage)):
            try:
                msg = event.message
                if not msg:
                    return
                peer_id = client.get_peer_id(msg.peer_id)
                if asyncio.iscoroutine(peer_id):
                    peer_id = await peer_id
                msg_id = msg.id
            except Exception:
                return
        else:
            return
            
        logger.warning(f"📢 Reaction raw event received: type={type(event).__name__}, msg_id={msg_id}, peer_id={peer_id}")
        
        if not hasattr(client, '_me') or not client._me:
            try:
                client._me = await client.get_me()
            except Exception as e:
                logger.error(f"Failed to get_me for reaction handler: {e}")
                return
                
        me = getattr(client, '_me', None)
        if not me:
            return

        is_group_or_channel = True
        if isinstance(event, types.UpdateMessageReactions):
            if isinstance(event.peer, types.PeerUser):
                is_group_or_channel = False
        elif msg and msg.peer_id:
            if isinstance(msg.peer_id, types.PeerUser):
                is_group_or_channel = False

        is_client_manager = me and is_authorized_manager(me.id)
        reacting_managers = []
        if is_group_or_channel:
            try:
                from telethon.tl.functions.messages import GetMessageReactionsListRequest
                entity = await client.get_entity(peer_id)
                res = await client(GetMessageReactionsListRequest(
                    peer=entity,
                    id=msg_id,
                    limit=100
                ))
                if res and res.reactions:
                    for r in res.reactions:
                        if isinstance(r.peer_id, types.PeerUser):
                            u_id = r.peer_id.user_id
                            if is_authorized_manager(u_id):
                                # Determine active userbots
                                try:
                                    active_uids = {s[5] for s in get_active_userbot_sessions() if s[5] is not None}
                                except Exception:
                                    active_uids = set()
                                
                                # 1. If this is the main userbot, ignore reactions from managers who have active userbots
                                if not is_client_manager and u_id in active_uids:
                                    continue
                                    
                                # 2. If this is the manager's userbot, only process its own reactions
                                if is_client_manager and u_id != me.id:
                                    continue
                                    
                                if u_id not in reacting_managers:
                                    reacting_managers.append(u_id)
            except Exception as e:
                logger.error(f"Failed to fetch reaction list via GetMessageReactionsListRequest: {e}")

        if not reacting_managers and is_group_or_channel:
            if hasattr(event, 'reactions') and event.reactions and getattr(event.reactions, 'recent_reactions', None):
                for rr in event.reactions.recent_reactions:
                    if isinstance(rr.peer_id, types.PeerUser):
                        u_id = rr.peer_id.user_id
                        if is_authorized_manager(u_id):
                            if u_id != me.id or is_client_manager:
                                if u_id not in reacting_managers:
                                    reacting_managers.append(u_id)

        if not msg:
            try:
                logger.warning(f"Fetching message details for reaction msg_id={msg_id} in peer_id={peer_id}")
                entity = await client.get_entity(peer_id)
                async with userbot_lock:
                    msgs = await client.get_messages(entity, ids=msg_id)
                if isinstance(msgs, list):
                    msg = msgs[0] if msgs else None
                else:
                    msg = msgs
            except Exception as e:
                logger.error(f"Failed to fetch message for reaction: {e}")
                return

        if not reacting_managers and is_group_or_channel and msg and msg.reactions and msg.reactions.recent_reactions:
            for rr in msg.reactions.recent_reactions:
                if isinstance(rr.peer_id, types.PeerUser):
                    u_id = rr.peer_id.user_id
                    if is_authorized_manager(u_id):
                        if u_id != me.id or is_client_manager:
                            if u_id not in reacting_managers:
                                reacting_managers.append(u_id)

        if reacting_managers:
            for manager_id in reacting_managers:
                manager_cache_key = (peer_id, msg_id, manager_id)
                if manager_cache_key not in processed_manager_reactions_cache:
                    processed_manager_reactions_cache.append(manager_cache_key)
                    if len(processed_manager_reactions_cache) > 1000:
                        processed_manager_reactions_cache.pop(0)
                    logger.warning(f"Manager {manager_id} reacted. Initiating download and send.")
                    asyncio.create_task(process_manager_reaction(client, peer_id, msg_id, msg, manager_id))

        if is_client_manager:
            return

        cache_key = (peer_id, msg_id)
        if cache_key in processed_reactions_cache:
            return
            
        if not msg or not msg.reactions:
            logger.warning("No reactions on fetched message.")
            return
            
        has_userbot_reacted = False
        if msg.reactions.results:
            for r in msg.reactions.results:
                if getattr(r, 'chosen_order', None) is not None:
                    has_userbot_reacted = True
                    break
                    
        if not has_userbot_reacted and msg.reactions.recent_reactions:
            for rr in msg.reactions.recent_reactions:
                if isinstance(rr.peer_id, types.PeerUser) and rr.peer_id.user_id == me.id:
                    has_userbot_reacted = True
                    break
                    
        if not has_userbot_reacted:
            logger.warning("Userbot reaction not found in message reactions.")
            return
            
        logger.warning(f"Userbot reaction detected on message {msg_id}! Forwarding/downloading...")
        
        processed_reactions_cache.append(cache_key)
        if len(processed_reactions_cache) > 1000:
            processed_reactions_cache.pop(0)
            
        targets = [me.id]
        try:
            with db_conn() as conn:
                c = conn.cursor()
                c.execute("SELECT user_id FROM managers")
                for row in c.fetchall():
                    m_id = row[0]
                    if m_id and m_id != me.id and m_id not in targets:
                        targets.append(m_id)
        except Exception as db_err:
            logger.error(f"Failed to fetch managers from DB: {db_err}")
            
        try:
            chat_peer = await resolve_target_id(client, peer_id)
            is_restricted = getattr(chat_peer, 'noforwards', False)
        except Exception:
            is_restricted = False

        album_msgs = [msg]
        if msg.grouped_id is not None:
            try:
                entity = await client.get_entity(peer_id)
                msg_ids = list(range(max(1, msg_id - 10), msg_id + 11))
                async with userbot_lock:
                    siblings = await client.get_messages(entity, ids=msg_ids)
                album_msgs = [m for m in siblings if m and m.grouped_id == msg.grouped_id]
                album_msgs.sort(key=lambda x: x.id)
                if not album_msgs:
                    album_msgs = [msg]
            except Exception as e:
                logger.error(f"Failed to fetch album siblings for msg_id={msg_id}: {e}")
                album_msgs = [msg]

        if is_restricted:
            logger.warning(f"Source chat {peer_id} is restricted. Downloading media locally...")
            temp_paths = []
            album_text = next((m.message for m in album_msgs if m.message), "")
            for m in album_msgs:
                if m.media:
                    try:
                        path = await client.download_media(m)
                        if path:
                            temp_paths.append(path)
                    except Exception as e:
                        logger.error(f"Failed to download media for msg_id={m.id}: {e}")
            for target in targets:
                try:
                    if temp_paths:
                        await client.send_file(target, file=temp_paths, caption=album_text)
                    elif album_text:
                        await client.send_message(target, album_text)
                except Exception as e:
                    logger.error(f"Failed to send reacted message to target {target}: {e}")
            for temp_path in temp_paths:
                if temp_path and os.path.exists(temp_path):
                    try: os.remove(temp_path)
                    except Exception: pass
        else:
            logger.warning(f"Source chat {peer_id} is unrestricted. Forwarding natively...")
            for target in targets:
                try:
                    await client.forward_messages(target, album_msgs)
                except Exception as e:
                    logger.error(f"Failed to forward reacted message to target {target}: {e}")

    @client.on(events.NewMessage)
    async def auto_handler(event):
        m = event.message
        if not m: return

        # Ensure client._me is cached
        if not hasattr(client, '_me') or not client._me:
            try:
                client._me = await client.get_me()
            except Exception as e:
                logger.error(f"Failed to get_me() for userbot: {e}")

        me = getattr(client, '_me', None)
        is_client_manager = me and is_authorized_manager(me.id)

        # Manager userbot only handles private message forwarding (PM monitor / PM media forwarder) and reaction fetching.
        # It bypasses all group/channel auto-forwarding, links, commands, and promotion systems.
        if is_client_manager:
            if event.is_private and m.sender_id != me.id and m.media:
                async def run_manager_private_save():
                    try:
                        # Extract sender info
                        sender = await event.get_sender()
                        sender_name = getattr(sender, 'first_name', '') or ''
                        if getattr(sender, 'last_name', None):
                            sender_name += f" {sender.last_name}"
                        if not sender_name:
                            sender_name = "User"
                        username = getattr(sender, 'username', '')
                        username_str = f" (@{username})" if username else ""
                        
                        header = f"💬 **Monitored PM Message**\n"
                        header += f"👤 **Sender:** [{sender_name}](tg://user?id={m.sender_id}) (`{m.sender_id}`){username_str}\n\n"
                        
                        try:
                            temp_path = await client.download_media(m)
                            if temp_path:
                                # Add self-destructing visual tag if applicable
                                ttl = getattr(m, 'ttl_seconds', None) or (getattr(m.media, 'ttl_seconds', None) if m.media else None)
                                tag = "🔥 **Self-Destructing Media Save**\n\n" if (ttl and ttl > 0) else ""
                                
                                await client.send_message(
                                    entity="me",
                                    file=temp_path,
                                    message=header + tag + (m.message or "")
                                )
                                if os.path.exists(temp_path):
                                    os.remove(temp_path)
                            else:
                                # Fallback to native forward
                                await client.send_message(entity="me", message=header)
                                await client.forward_messages(
                                    entity="me",
                                    messages=m,
                                    from_peer=event.chat_id
                                )
                        except Exception as e:
                            logger.error(f"Manager userbot failed to download/send private media: {e}")
                            try:
                                await client.send_message(entity="me", message=header)
                                await client.forward_messages(
                                    entity="me",
                                    messages=m,
                                    from_peer=event.chat_id
                                )
                            except Exception as e2:
                                logger.error(f"Manager userbot fallback forward failed: {e2}")
                    except Exception as e:
                        logger.error(f"Error in manager private save: {e}")
                        
                asyncio.create_task(run_manager_private_save())
            return

        # FAST DROP: Immediately ignore messages if the source chat isn't in configured pairs
        # This prevents unconfigured active channels from flooding your CPU loop
        configured_pairs = get_target_pairs()
        configured_sources = {str(p[1]).replace("-100", "") for p in configured_pairs}
        
        current_chat_str = str(event.chat_id).replace("-100", "")
        if current_chat_str not in configured_sources and not event.is_private:
            return # Drop execution immediately before hitting locks or networks

        # Message Link Auto-Downloader/Forwarder System
        is_primary_admin = (m.sender_id == ADMIN_ID) or (me and m.sender_id == me.id)
        is_manager = is_primary_admin or is_authorized_manager(m.sender_id)
        if event.is_private and is_manager and m.text:
            text_clean = m.text.strip().lower()
            parts = m.text.split()
            cmd_start = parts[0].lower() if parts else ""
            
            if cmd_start in [".login", "/login"] or m.sender_id in userbot_login_states:
                if cmd_start in [".login", "/login"]:
                    if len(parts) >= 4:
                        phone = parts[1].strip().replace(" ", "")
                        try:
                            api_id = int(parts[2].strip())
                        except ValueError:
                            await event.reply("❌ **Invalid API ID format.** Must be an integer.")
                            return
                        api_hash = parts[3].strip()
                        
                        userbot_login_states[m.sender_id] = {
                            "state": "awaiting_otp",
                            "phone": phone,
                            "api_id": api_id,
                            "api_hash": api_hash
                        }
                        
                        await event.reply("⏳ **Connecting & Sending OTP...**")
                        try:
                            temp_client = TelegramClient(
                                StringSession(), 
                                api_id, 
                                api_hash,
                                device_model="PC 64bit",
                                system_version="Windows 11",
                                app_version="4.11.2"
                            )
                            await temp_client.connect()
                            send_code = await temp_client.send_code_request(phone)
                            userbot_login_states[m.sender_id]["client"] = temp_client
                            userbot_login_states[m.sender_id]["phone_code_hash"] = send_code.phone_code_hash
                            await event.reply("📩 **OTP Sent!**\n\n⚠️ **IMPORTANT**: To prevent Telegram from automatically blocking/invalidating your login code, **DO NOT** send the code as a plain number.\n\nSend it with dashes or spaces between the digits (e.g. if your code is `53488`, send it as **`5-3-4-8-8`** or **`5 3 4 8 8`**):")
                        except Exception as e:
                            await event.reply(f"❌ **Failed to send OTP:** {e}")
                            userbot_login_states.pop(m.sender_id, None)
                        return
                    else:
                        userbot_login_states[m.sender_id] = {"state": "awaiting_phone"}
                        await event.reply("📲 **Manager Userbot Login Sequence Initiated**\n\nPlease send your **Phone Number** (with country code, e.g., `+1234567890`):\n\n*Tip*: You can also login in one go using:\n`.login <phone> <api_id> <api_hash>`")
                        return
                else:
                    state_data = userbot_login_states[m.sender_id]
                    current_state = state_data["state"]
                    
                    if current_state == "awaiting_phone":
                        phone = m.text.strip().replace(" ", "")
                        state_data["phone"] = phone
                        
                        # Fetch default API ID and Hash
                        default_api_id = int(os.getenv("API_ID", 0))
                        default_api_hash = os.getenv("API_HASH", "")
                        
                        if not default_api_id or not default_api_hash:
                            try:
                                default_api_id = int(get_setting("api_id", 0))
                                default_api_hash = get_setting("api_hash", "")
                            except Exception:
                                pass
                                
                        if not default_api_id or not default_api_hash:
                            sessions = get_userbot_sessions()
                            if sessions:
                                default_api_id = int(sessions[0][2])
                                default_api_hash = sessions[0][3]
                                
                        if not default_api_id or not default_api_hash:
                            await event.reply("❌ **API ID or API Hash not configured on the main server.**\n\nPlease connect using:\n`.login <phone> <api_id> <api_hash>`")
                            userbot_login_states.pop(m.sender_id, None)
                            return
                            
                        state_data["api_id"] = default_api_id
                        state_data["api_hash"] = default_api_hash
                        
                        await event.reply(f"⏳ **Sending OTP (Telethon)...**\n\n*Debug Info*:\n• **API ID**: `{default_api_id}`\n• **Phone**: `{phone}`")
                        
                        try:
                            temp_client = TelegramClient(
                                StringSession(), 
                                default_api_id, 
                                default_api_hash,
                                device_model="PC 64bit",
                                system_version="Windows 11",
                                app_version="4.11.2"
                            )
                            await temp_client.connect()
                            send_code = await temp_client.send_code_request(phone)
                            state_data["client"] = temp_client
                            state_data["phone_code_hash"] = send_code.phone_code_hash
                            state_data["state"] = "awaiting_otp"
                            await event.reply("📩 **OTP Sent!**\n\n⚠️ **IMPORTANT**: To prevent Telegram from automatically blocking/invalidating your login code, **DO NOT** send the code as a plain number.\n\nSend it with dashes or spaces between the digits (e.g. if your code is `53488`, send it as **`5-3-4-8-8`** or **`5 3 4 8 8`**):")
                        except Exception as e:
                            await event.reply(f"❌ **Failed to send OTP:** {e}")
                            userbot_login_states.pop(m.sender_id, None)
                        return
                        
                    elif current_state == "awaiting_otp":
                        otp = "".join(c for c in m.text if c.isdigit())
                        temp_client = state_data["client"]
                        phone = state_data["phone"]
                        phone_code_hash = state_data["phone_code_hash"]
                        
                        await event.reply("⏳ **Verifying OTP...**")
                        
                        try:
                            await temp_client.sign_in(phone=phone, code=otp, phone_code_hash=phone_code_hash)
                            await complete_userbot_login_flow(m.sender_id, temp_client, event)
                        except errors.SessionPasswordNeededError:
                            state_data["state"] = "awaiting_password"
                            await event.reply("🔐 **Cloud Password Required**\n\nYour account has Two-Step Verification enabled. Please send your **Cloud Password**:")
                        except Exception as e:
                            await event.reply(f"❌ **OTP verification failed:** {e}")
                            userbot_login_states.pop(m.sender_id, None)
                        return
                        
                    elif current_state == "awaiting_password":
                        password = m.text.strip()
                        temp_client = state_data["client"]
                        
                        await event.reply("⏳ **Verifying Password...**")
                        
                        try:
                            await temp_client.sign_in(password=password)
                            await complete_userbot_login_flow(m.sender_id, temp_client, event)
                        except Exception as e:
                            await event.reply(f"❌ **Password verification failed:** {e}")
                            userbot_login_states.pop(m.sender_id, None)
                        return

            link_info = parse_telegram_message_link(m.text)
            if link_info:
                async def handle_link():
                    await event.reply("⏳ **Processing message link...**")
                    success, err = await process_and_send_message_link(client, link_info, m.sender_id)
                    if success:
                        await event.reply("✅ **Message successfully processed and delivered!**")
                    else:
                        await event.reply(f"❌ **Failed to process link:** {err}")
                asyncio.create_task(handle_link())
                return

        # Private Chat Monitor System
        if event.is_private and not is_manager and me and m.sender_id != me.id:
            pm_mon_enabled = get_setting("pm_chat_monitoring_enabled") == "1"
            dest_id = get_setting("pm_chat_monitoring_destination")
            if pm_mon_enabled and dest_id:
                async def run_pm_monitor():
                    try:
                        target = await resolve_target_id(client, int(dest_id))
                        sender = await event.get_sender()
                        sender_name = getattr(sender, 'first_name', '') or ''
                        if getattr(sender, 'last_name', None):
                            sender_name += f" {sender.last_name}"
                        if not sender_name:
                            sender_name = "User"
                        username = getattr(sender, 'username', '')
                        username_str = f" (@{username})" if username else ""
                        
                        header = f"💬 **Monitored PM Message**\n"
                        header += f"👤 **Sender:** [{sender_name}](tg://user?id={m.sender_id}) (`{m.sender_id}`){username_str}"
                        
                        await client.send_message(target, header, parse_mode="Markdown")
                        
                        ttl = getattr(m, 'ttl_seconds', None) or getattr(m.media, 'ttl_seconds', None)
                        is_destructive = ttl and (ttl > 0)
                        if is_destructive:
                            temp_path = await client.download_media(m)
                            if temp_path:
                                await client.send_message(target, file=temp_path, message=m.message or "")
                                if os.path.exists(temp_path):
                                    os.remove(temp_path)
                        else:
                            try:
                                await client.forward_messages(target, m)
                            except Exception:
                                temp_path = await client.download_media(m)
                                if temp_path:
                                    await client.send_message(target, file=temp_path, message=m.message or "")
                                    if os.path.exists(temp_path):
                                        os.remove(temp_path)
                                elif m.message:
                                    await client.send_message(target, m.message)
                    except Exception as mon_err:
                        logger.error(f"Failed to execute PM monitoring for sender {m.sender_id}: {mon_err}")
                asyncio.create_task(run_pm_monitor())

        # Private Media Forwarding System
        if event.is_private and m.media:
            is_me = me and (m.sender_id == me.id)
            if not is_me:
                pm_enabled = get_setting("pm_media_forwarding_enabled") == "1"
                if pm_enabled:
                    # Detect self-destructing/view-once media via TTL
                    ttl = getattr(m, 'ttl_seconds', None) or getattr(m.media, 'ttl_seconds', None)
                    is_destructive = ttl and (ttl > 0)
                    
                    targets_str = get_setting("pm_media_forwarding_targets") or ""
                    target_ids = [t.strip() for t in targets_str.split(",") if t.strip()]
                    
                    if is_destructive:
                        # Only proceed if allowed in settings
                        allow_destructive = get_setting("pm_media_forwarding_allow_destructive") == "1"
                        if allow_destructive:
                            try:
                                # Standard forward is blocked by server, so we decrypt and download locally first
                                temp_path = await client.download_media(m)
                                if temp_path:
                                    for tid in target_ids:
                                        try:
                                            tgt_peer = await resolve_target_id(client, int(tid))
                                            await client.send_message(
                                                entity=tgt_peer,
                                                file=temp_path,
                                                message=m.message or ""
                                            )
                                            logger.info(f"🔥 PM_FORWARD: Saved and sent decrypted self-destructing media to target {tid}")
                                        except Exception as send_err:
                                            logger.error(f"Failed to send decrypted self-destructing media to target {tid}: {send_err}")
                                    
                                    # Strict local cleanup
                                    if os.path.exists(temp_path):
                                        os.remove(temp_path)
                            except Exception as dl_err:
                                logger.error(f"Failed to pre-download self-destructing media: {dl_err}")
                        else:
                            logger.info(f"📬 PM_FORWARD: Skipped self-destructing media because allow_destructive toggle is disabled.")
                    else:
                        # Normal media: standard forward directly
                        for tid in target_ids:
                            try:
                                tgt_peer = await resolve_target_id(client, int(tid))
                                await client.forward_messages(
                                    entity=tgt_peer,
                                    messages=m,
                                    from_peer=event.chat_id
                                )
                                logger.info(f"📬 PM_FORWARD: Auto-forwarded media message {m.id} from user {m.sender_id} to target {tid}")
                            except Exception as pm_err:
                                logger.error(f"Failed to auto-forward normal PM media to target {tid}: {pm_err}")

        # Promotion keyword check for unauthorized users (Private chats with userbot)
        is_primary_admin = (m.sender_id == ADMIN_ID) or (me and m.sender_id == me.id)
        is_manager = is_primary_admin or is_authorized_manager(m.sender_id)
        if not is_manager and event.is_private and m.text:
            sender_entity = await event.get_sender()
            sender_uname = getattr(sender_entity, 'username', None) or ""
            async def userbot_reply(reply_text):
                await event.reply(reply_text)
            promoted = await check_and_promote_user(client, m.sender_id, sender_uname, m.text, userbot_reply)
            if promoted:
                return

        # Userbot commands for target pairs configuration
        if m.text and m.text.strip().startswith('.'):
            if is_manager:
                text = m.text.strip()
                parts = text.split()
                cmd = parts[0].lower()
                
                if cmd in ['.addpair', '.pair', '.delpair', '.listpairs', '.pairs', '.setpair', '.addmanager', '.delmanager', '.managers', '.join', '.setpromo', '.promo', '.tasks', '.task']:
                    try:
                        if cmd in ['.tasks', '.task']:
                            report = get_active_tasks_report()
                            await event.reply(report)
                            return
                            
                        elif cmd in ['.addpair', '.pair']:
                            if len(parts) < 3:
                                await event.reply("❌ **Usage:** `.addpair <source> <target>`\n(Source/Target can be usernames, links, topic links, or numeric IDs)")
                                return
                            
                            source_raw = parts[1]
                            target_raw = parts[2]
                            await event.reply("⏳ **Resolving source and target entities...**")
                            
                            s_entity, s_topic = await resolve_chat_and_topic(client, source_raw)
                            t_entity, t_topic = await resolve_chat_and_topic(client, target_raw)
                            
                            s_title = getattr(s_entity, 'title', None) or getattr(s_entity, 'first_name', None) or str(s_entity.id)
                            t_title = getattr(t_entity, 'title', None) or getattr(t_entity, 'first_name', None) or str(t_entity.id)
                            
                            from telethon.utils import get_peer_id
                            sid = get_peer_id(s_entity)
                            tid = get_peer_id(t_entity)
                            
                            add_target_pair(sid, s_topic, tid, t_topic, s_title, t_title)
                            
                            # Update target pair to set is_live = 1 and is_mirror = 1 by default
                            with db_conn() as conn:
                                c = conn.cursor()
                                p = get_placeholder()
                                query = f"UPDATE target_pairs SET is_live = 1, is_mirror = 1 WHERE source_id = {p} AND target_id = {p}"
                                params = [sid, tid]
                                if s_topic is not None:
                                    query += f" AND source_topic_id = {p}"
                                    params.append(s_topic)
                                else:
                                    query += " AND source_topic_id IS NULL"
                                    
                                if t_topic is not None:
                                    query += f" AND target_topic_id = {p}"
                                    params.append(t_topic)
                                else:
                                    query += " AND target_topic_id IS NULL"
                                c.execute(query, tuple(params))
                                
                            # Fetch pair ID
                            pair_id = None
                            with db_conn() as conn:
                                c = conn.cursor()
                                p = get_placeholder()
                                query = f"SELECT id FROM target_pairs WHERE source_id = {p} AND target_id = {p}"
                                params = [sid, tid]
                                if s_topic is not None:
                                    query += f" AND source_topic_id = {p}"
                                    params.append(s_topic)
                                else:
                                    query += " AND source_topic_id IS NULL"
                                if t_topic is not None:
                                    query += f" AND target_topic_id = {p}"
                                    params.append(t_topic)
                                else:
                                    query += " AND target_topic_id IS NULL"
                                c.execute(query, tuple(params))
                                row = c.fetchone()
                                if row:
                                    pair_id = row[0]
                            
                            await event.reply(
                                f"✅ **Target Pair Added & Activated!**\n\n"
                                f"**ID:** `{pair_id}`\n"
                                f"**Source:** `{s_title}`" + (f" (Topic: `{s_topic}`)" if s_topic else "") + f" (ID: `{sid}`)\n"
                                f"**Target:** `{t_title}`" + (f" (Topic: `{t_topic}`)" if t_topic else "") + f" (ID: `{tid}`)\n\n"
                                f"⚡ *Live forwarding and mirroring enabled by default.*"
                            )
                            return
                            
                        elif cmd == '.delpair':
                            if len(parts) < 2:
                                await event.reply("❌ **Usage:** `.delpair <pair_id>` or `.delpair <source> <target>`")
                                return
                            
                            # If they provided a pair ID
                            if len(parts) == 2 and parts[1].isdigit():
                                pid = int(parts[1])
                                row = get_target_pair(pid)
                                if not row:
                                    await event.reply(f"❌ Pair ID `{pid}` not found.")
                                    return
                                with db_conn() as conn:
                                    c = conn.cursor()
                                    p = get_placeholder()
                                    c.execute(f"DELETE FROM target_pairs WHERE id = {p}", (pid,))
                                await event.reply(f"✅ Deleted pair ID `{pid}` (`{row[3]}` -> `{row[4]}`).")
                                return
                            
                            if len(parts) < 3:
                                await event.reply("❌ **Usage:** `.delpair <pair_id>` or `.delpair <source> <target>`")
                                return
                            
                            source_raw = parts[1]
                            target_raw = parts[2]
                            await event.reply("⏳ **Resolving source and target...**")
                            
                            s_entity, s_topic = await resolve_chat_and_topic(client, source_raw)
                            t_entity, t_topic = await resolve_chat_and_topic(client, target_raw)
                            
                            from telethon.utils import get_peer_id
                            sid = get_peer_id(s_entity)
                            tid = get_peer_id(t_entity)
                            
                            pair_id = None
                            with db_conn() as conn:
                                c = conn.cursor()
                                p = get_placeholder()
                                query = f"SELECT id FROM target_pairs WHERE source_id = {p} AND target_id = {p}"
                                params = [sid, tid]
                                if s_topic is not None:
                                    query += f" AND source_topic_id = {p}"
                                    params.append(s_topic)
                                else:
                                    query += " AND source_topic_id IS NULL"
                                if t_topic is not None:
                                    query += f" AND target_topic_id = {p}"
                                    params.append(t_topic)
                                else:
                                    query += " AND target_topic_id IS NULL"
                                c.execute(query, tuple(params))
                                row = c.fetchone()
                                if row:
                                    pair_id = row[0]
                            
                            if not pair_id:
                                await event.reply("❌ Pair not found matching those settings.")
                                return
                            
                            with db_conn() as conn:
                                c = conn.cursor()
                                p = get_placeholder()
                                c.execute(f"DELETE FROM target_pairs WHERE id = {p}", (pair_id,))
                            
                            await event.reply(f"✅ Deleted pair ID `{pair_id}` (`{s_entity.title}` -> `{t_entity.title}`).")
                            return
                            
                        elif cmd in ['.listpairs', '.pairs']:
                            pairs = []
                            with db_conn() as conn:
                                c = conn.cursor()
                                c.execute("SELECT id, source_title, source_topic_id, target_title, target_topic_id, is_live, is_mirror, is_monitoring FROM target_pairs ORDER BY id ASC")
                                pairs = c.fetchall()
                            
                            if not pairs:
                                await event.reply("📭 No active target pairs configured.")
                                return
                            
                            text_lines = ["📋 **Configured Target Pairs:**\n"]
                            for r in pairs:
                                pid, s_title, s_topic, t_title, t_topic, live, mir, mon = r
                                status = []
                                if live: status.append("⚡Live")
                                if mir: status.append("🔄Mirror")
                                if mon: status.append("👁️Mon")
                                status_str = f"[{', '.join(status)}]" if status else "[Disabled]"
                                
                                s_desc = f"{s_title}" + (f" (Topic: {s_topic})" if s_topic else "")
                                t_desc = f"{t_title}" + (f" (Topic: {t_topic})" if t_topic else "")
                                
                                text_lines.append(f"🔹 **ID {pid}**: `{s_desc}` ➡️ `{t_desc}` {status_str}")
                            
                            full_text = "\n".join(text_lines)
                            if len(full_text) > 4000:
                                chunk = []
                                for line in text_lines:
                                    if len("\n".join(chunk) + "\n" + line) > 4000:
                                        await event.reply("\n".join(chunk))
                                        chunk = [line]
                                    else:
                                        chunk.append(line)
                                if chunk:
                                    await event.reply("\n".join(chunk))
                            else:
                                await event.reply(full_text)
                            return
                            
                        elif cmd == '.setpair':
                            if len(parts) < 4:
                                await event.reply("❌ **Usage:** `.setpair <pair_id> <live/mon/mir> <1/0>`")
                                return
                            
                            pid_str = parts[1]
                            setting = parts[2].lower()
                            val_str = parts[3]
                            
                            if not pid_str.isdigit() or not val_str.isdigit():
                                await event.reply("❌ Pair ID and value must be integers.")
                                return
                            
                            pid = int(pid_str)
                            val = int(val_str)
                            
                            if val not in [0, 1]:
                                await event.reply("❌ Value must be `0` (off) or `1` (on).")
                                return
                            
                            if setting not in ['live', 'mon', 'monitoring', 'mir', 'mirror']:
                                await event.reply("❌ Setting must be one of: `live`, `mon`/`monitoring`, `mir`/`mirror`.")
                                return
                            
                            col = None
                            if setting == 'live':
                                col = 'is_live'
                            elif setting in ['mon', 'monitoring']:
                                col = 'is_monitoring'
                            elif setting in ['mir', 'mirror']:
                                col = 'is_mirror'
                            
                            row = get_target_pair(pid)
                            if not row:
                                await event.reply(f"❌ Pair ID `{pid}` not found.")
                                return
                            
                            with db_conn() as conn:
                                c = conn.cursor()
                                p = get_placeholder()
                                c.execute(f"UPDATE target_pairs SET {col} = {p} WHERE id = {p}", (val, pid))
                            
                            if col == 'is_monitoring' and val == 1:
                                asyncio.create_task(run_collection(ADMIN_ID, pid, limit=None))
                                await event.reply(f"✅ Updated pair ID `{pid}`: set `{col}` to `{val}`. Also started background history scan.")
                            else:
                                await event.reply(f"✅ Updated pair ID `{pid}`: set `{col}` to `{val}`.")
                            return

                        elif cmd == '.addmanager':
                            if not is_primary_admin:
                                await event.reply("❌ Only the primary admin can manage manager accounts.")
                                return
                            if len(parts) < 2:
                                await event.reply("❌ **Usage:** `.addmanager <username_or_id>`")
                                return
                            
                            target_user = parts[1]
                            await event.reply("⏳ **Resolving manager user...**")
                            try:
                                user_entity = await client.get_entity(target_user)
                                uid = user_entity.id
                                uname = getattr(user_entity, 'username', None) or ""
                                
                                with db_conn() as conn:
                                    c = conn.cursor()
                                    p = get_placeholder()
                                    if USING_POSTGRES:
                                        c.execute("INSERT INTO managers (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username", (uid, uname))
                                    else:
                                        c.execute("INSERT OR REPLACE INTO managers (user_id, username) VALUES (?, ?)", (uid, uname))
                                
                                await event.reply(f"✅ **Manager Authorized!**\n**User ID:** `{uid}`\n**Username:** `@{uname}`" if uname else f"✅ **Manager Authorized!**\n**User ID:** `{uid}`")
                                
                                # Send welcome DM to the new manager from the userbot!
                                welcome_msg = (
                                    "🎉 **Congratulations! You have been authorized as a Manager!**\n\n"
                                    "You can now configure target pairs and instruct the userbot to join groups directly through this chat!\n\n"
                                    "🛠️ **Available Commands:**\n"
                                    "• `.join <link_or_username>`: Request the userbot to join a group or channel.\n"
                                    "• `.pair <source> <target>` (or `.addpair`): Link a source chat to a target chat for live forwarding.\n"
                                    "• `.delpair <pair_id>`: Delete a target pair.\n"
                                    "• `.pairs` (or `.listpairs`): List all active target pairs.\n"
                                    "• `.setpair <pair_id> <live/mon/mir> <1/0>`: Turn settings on (1) or off (0).\n\n"
                                    "💬 **Group Joining Wizard:**\n"
                                    "Simply send any Telegram group link or username (e.g. `t.me/cctest` or `@cctest`) to this chat, and I will automatically guide you on how to join and configure it!"
                                )
                                try:
                                    await client.send_message(user_entity, welcome_msg)
                                except Exception as welcome_err:
                                    logger.error(f"Failed to send welcome message to new manager {uid}: {welcome_err}")
                            except Exception as e:
                                await event.reply(f"❌ Failed to authorize manager: {e}")
                            return

                        elif cmd == '.delmanager':
                            if not is_primary_admin:
                                await event.reply("❌ Only the primary admin can manage manager accounts.")
                                return
                            if len(parts) < 2:
                                await event.reply("❌ **Usage:** `.delmanager <username_or_id>`")
                                return
                            
                            target_user = parts[1]
                            await event.reply("⏳ **Resolving manager user...**")
                            try:
                                if target_user.lstrip("-").isdigit():
                                    uid = int(target_user)
                                else:
                                    user_entity = await client.get_entity(target_user)
                                    uid = user_entity.id
                                
                                with db_conn() as conn:
                                    c = conn.cursor()
                                    p = get_placeholder()
                                    c.execute(f"DELETE FROM managers WHERE user_id = {p}", (uid,))
                                
                                await event.reply(f"✅ **Manager Revoked!**\n**User ID:** `{uid}`")
                            except Exception as e:
                                await event.reply(f"❌ Failed to revoke manager: {e}")
                            return

                        elif cmd == '.managers':
                            if not is_primary_admin:
                                await event.reply("❌ Only the primary admin can view manager accounts.")
                                return
                            
                            try:
                                with db_conn() as conn:
                                    c = conn.cursor()
                                    c.execute("SELECT user_id, username FROM managers ORDER BY user_id ASC")
                                    rows = c.fetchall()
                                
                                if not rows:
                                    await event.reply("📭 No additional managers authorized.")
                                    return
                                
                                try:
                                    userbot_sessions = get_userbot_sessions()
                                    userbot_uids = {s[5] for s in userbot_sessions if s[5] is not None}
                                except Exception:
                                    userbot_uids = set()
                                
                                text_lines = ["📋 **Authorized Managers:**\n"]
                                for uid, uname in rows:
                                    escaped_uname = uname.replace("_", "\\_") if uname else ""
                                    suffix = " (Manager + Userbot)" if uid in userbot_uids else ""
                                    text_lines.append(f"👤 `{uid}`" + (f" (@{escaped_uname})" if uname else "") + suffix)
                                
                                await event.reply("\n".join(text_lines))
                            except Exception as e:
                                await event.reply(f"❌ Error fetching managers: {e}")
                            return

                        elif cmd == '.join':
                            if len(parts) < 2:
                                await event.reply("❌ **Usage:** `.join <link_or_username>`")
                                return
                            
                            target_link = parts[1]
                            await event.reply("⏳ **Joining chat...**")
                            
                            parsed = parse_telegram_link(target_link)
                            try:
                                chat_entity = None
                                if parsed and parsed["type"] == "invite":
                                    from telethon.tl.functions.messages import ImportChatInviteRequest
                                    try:
                                        result = await client(ImportChatInviteRequest(parsed["hash"]))
                                        if hasattr(result, "chats") and result.chats:
                                            chat_entity = result.chats[0]
                                    except errors.UserAlreadyParticipantError:
                                        from telethon.tl.functions.messages import CheckChatInviteRequest
                                        invite_info = await client(CheckChatInviteRequest(parsed["hash"]))
                                        chat_entity = invite_info.chat
                                else:
                                    from telethon.tl.functions.channels import JoinChannelRequest
                                    username = parsed["username"] if parsed else target_link.strip().replace("@", "")
                                    chat_entity = await client.get_entity(username)
                                    await client(JoinChannelRequest(chat_entity))
                                
                                if chat_entity:
                                    from telethon.utils import get_peer_id
                                    cid = get_peer_id(chat_entity)
                                    title = getattr(chat_entity, 'title', None) or getattr(chat_entity, 'first_name', None) or str(cid)
                                    
                                    await event.reply(
                                        f"✅ **Joined Group Successfully!**\n"
                                        f"**Name:** `{title}`\n"
                                        f"**ID:** `{cid}`\n\n"
                                        f"💡 **Quick Setup Templates:**\n"
                                        f"• Set as **Source** (forward FROM this group):\n"
                                        f"  `.pair {cid} <target_id>`\n"
                                        f"• Set as **Target** (forward TO this group):\n"
                                        f"  `.pair <source_id> {cid}`"
                                    )
                                else:
                                    await event.reply("❌ Failed to retrieve chat information.")
                            except Exception as e:
                                await event.reply(f"❌ Failed to join: {e}")
                            return

                        elif cmd == '.setpromo':
                            if not is_primary_admin:
                                await event.reply("❌ Only the primary admin can configure the promotion keyword.")
                                return
                            if len(parts) < 2:
                                await event.reply("❌ **Usage:** `.setpromo <keyword>` (or `.setpromo disable` to turn off)")
                                return
                            
                            keyword = parts[1]
                            if keyword.lower() in ['disable', 'none', 'off']:
                                set_setting("promotion_keyword", "")
                                await event.reply("✅ **Promotion Keyword Disabled.** Anyone sending the keyword will no longer be promoted.")
                            else:
                                set_setting("promotion_keyword", keyword)
                                await event.reply(f"✅ **Promotion Keyword Set!**\n\nKeyword: `{keyword}`\nUsers sending this exact code in DMs will be automatically promoted to Manager.")
                            return
                            
                        elif cmd == '.promo':
                            if not is_primary_admin:
                                await event.reply("❌ Only the primary admin can check the promotion keyword.")
                                return
                            keyword = get_setting("promotion_keyword")
                            if keyword:
                                await event.reply(f"🔑 **Active Promotion Keyword:** `{keyword}`\nUsers sending this exact code in DMs will be promoted.")
                            else:
                                await event.reply("🔑 **Active Promotion Keyword:** `None/Disabled`\nUse `.setpromo <word>` to set one.")
                            return

                    except Exception as err:
                        logger.error(f"Command execution error: {err}")
                        await event.reply(f"❌ **Command Error:** {err}")
                        return

        # Auto-prompt invite/chat link helper in DMs
        if m.text and not m.text.strip().startswith('.'):
            if event.is_private:
                is_admin_or_mgr = (m.sender_id == ADMIN_ID) or (me and m.sender_id == me.id) or is_authorized_manager(m.sender_id)
                if is_admin_or_mgr:
                    parsed_ref = parse_telegram_link(m.text.strip())
                    if parsed_ref:
                        await event.reply(
                            f"ℹ️ **Telegram Chat Link Detected!**\n"
                            f"To instruct the userbot to join this group, reply with:\n"
                            f"`.join {m.text.strip()}`"
                        )
                        return

        # Ignore all outgoing messages sent by the userbot itself to prevent loops and media leakage
        if m.out or (me and m.sender_id == me.id):
            return

        # Check if the message is too old (e.g., older than 10 minutes before bot startup)
        # to prevent massive replay of history on a fresh DB start/wipe
        msg_date = m.date
        if msg_date:
            # Ensure msg_date is timezone-aware
            if msg_date.tzinfo is None:
                msg_date = msg_date.replace(tzinfo=timezone.utc)
            if (BOT_START_TIME - msg_date).total_seconds() > 600:
                logger.info(f"⏳ Ignore old catchup message {m.id} in chat {m.chat_id} (Date: {msg_date})")
                return

        # Deduplicate message events to prevent duplicate processing (RAM + DB lookup)
        msg_key = (m.chat_id, m.id)
        if msg_key in processed_messages or is_message_processed(m.chat_id, m.id):
            if msg_key not in processed_messages:
                processed_messages.add(msg_key)
                processed_messages_queue.append(msg_key)
            logger.info(f"🔄 Ignore duplicate event for message {m.id} in chat {m.chat_id}")
            return
        
        # Mark immediately as processed in memory and DB
        processed_messages.add(msg_key)
        processed_messages_queue.append(msg_key)
        if len(processed_messages) > 2000:
            processed_messages.clear()
            processed_messages.update(processed_messages_queue)
        mark_message_processed(m.chat_id, m.id)

        # --- BAN LIST CHECK ---
        sender_id = m.sender_id
        sender_username = getattr(m.sender, 'username', None)
        if is_user_banned(sender_id, sender_username):
            logger.info(f"🚫 BLOCKED: Ignored message from banned user {sender_id} (@{sender_username})")
            return

        # --- ALBUM / SINGLE MESSAGE SPLIT ---
        if m.grouped_id:
            if m.grouped_id not in album_cache:
                album_cache[m.grouped_id] = [m]
                
                # Consolidated delayed task that processes all rules sequentially
                async def delayed_send_album(gid, s_id):
                    await asyncio.sleep(2.5)  # Time window optimization
                    messages = album_cache.pop(gid, [])
                    if not messages: return
                    
                    # Prevent identical racing handler updates from repeating pipeline execution
                    lock_key = f"{s_id}_{gid}"
                    if lock_key in album_processing_lock:
                        return
                    album_processing_lock.add(lock_key)
                    
                    try:
                        # Sort sequentially by message entry indexes
                        messages.sort(key=lambda x: x.id)
                        await process_automation_pipeline(client, messages, s_id)
                    finally:
                        # Safely clean garbage parameters and release loop execution tokens
                        album_processing_lock.discard(lock_key)
                
                asyncio.create_task(delayed_send_album(m.grouped_id, m.chat_id))
            else:
                # Add to existing collection bundle safely
                if m.id not in [msg.id for msg in album_cache[m.grouped_id]]:
                    album_cache[m.grouped_id].append(m)
        else:
            # Single Message Flow routed through the exact same processing core
            await process_automation_pipeline(client, [m], m.chat_id)
@bot.message_handler(commands=['start', 'dash'])
def cmd_start(message):
    if not is_authorized_manager(message.from_user.id):
        return
    bot.send_message(
        message.chat.id,
        get_dashboard_text(),
        reply_markup=get_dashboard_markup(),
        parse_mode="Markdown"
    )

@bot.message_handler(commands=["list"])
def cmd_list(message):
    if not is_authorized_manager(message.from_user.id):
        return
    
    if not userbot or not userbot.is_connected():
        bot.send_message(message.chat.id, "❌ Userbot is not connected. Use /start to connect first.")
        return

    status_msg = bot.send_message(message.chat.id, "🔍 Fetching your chats...")
    
    async def fetch_and_list():
        try:
            text = "📋 *Your Groups & Channels*\n\n"
            async for dialog in userbot.iter_dialogs(limit=50):
                entity = dialog.entity
                if isinstance(entity, (types.Chat, types.Channel)):
                    chat_type = "📢 Channel" if isinstance(entity, types.Channel) and entity.broadcast else "👥 Group"
                    title = entity.title or "Untitled"
                    text += f"{chat_type}: `{title}`\nID: `{entity.id}`\n\n"
            
            if len(text) > 4000:
                parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
                for p in parts: bot.send_message(message.chat.id, p, parse_mode="Markdown")
            else:
                bot.send_message(message.chat.id, text, parse_mode="Markdown")
            
            try: bot.delete_message(message.chat.id, status_msg.message_id)
            except Exception: pass
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Error fetching chats: {e}")

    asyncio.run_coroutine_threadsafe(fetch_and_list(), loop)
    
@bot.message_handler(commands=['extract'])
def cmd_extract_media(message):
    """Retrieves media from your Vault using source message ID"""
    if not is_authorized_manager(message.from_user.id): return
    try:
        args = message.text.split()
        if len(args) < 2: 
            return bot.reply_to(message, "💡 *Usage:* `/extract [message_id]`\n\nFind the ID in your collected logs.", parse_mode="Markdown")
        
        try:
            smid = int(args[1])
        except ValueError:
            return bot.reply_to(message, "❌ Message ID must be a valid number.")
            
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"SELECT file_id, media_type FROM media_logs WHERE source_message_id = {p} LIMIT 1", (smid,))
            res = c.fetchone()
        
        if res:
            file_id, m_type = res
            m_type = m_type.lower()
            
            bot.send_chat_action(message.chat.id, 'upload_document')
            caption = f"✅ *Extracted from Vault*\n\n🆔 Source ID: `{smid}`\n📂 Type: `{m_type}`"
            
            if "photo" in m_type:
                bot.send_photo(message.chat.id, file_id, caption=caption, parse_mode="Markdown")
            elif "video" in m_type:
                bot.send_video(message.chat.id, file_id, caption=caption, parse_mode="Markdown")
            else:
                bot.send_document(message.chat.id, file_id, caption=caption, parse_mode="Markdown")
        else:
            bot.reply_to(message, "❌ No record found in the vault for this ID.")
    except Exception as e:
        bot.reply_to(message, f"❌ Extraction Error: {e}")

@bot.message_handler(commands=['ping'])
def cmd_ping(message):
    if message.from_user.id != ADMIN_ID: return
    bot.reply_to(message, f"🏓 *Pong!*\n\nI am currently awake and running.\nTime: `{datetime.now().strftime('%H:%M:%S')}`", parse_mode="Markdown")

@bot.message_handler(commands=['tasks', 'task'])
def cmd_tasks(message):
    if not is_authorized_manager(message.from_user.id): return
    report = get_active_tasks_report()
    bot.send_message(message.chat.id, report, parse_mode="Markdown")

@bot.message_handler(commands=['ban', 'block'])
def cmd_ban_user(message):
    if message.from_user.id != ADMIN_ID: return
    try:
        args = message.text.split()
        if len(args) < 2:
            bot.reply_to(message, "💡 *Usage:* `/ban` or `/block [username_or_id]`", parse_mode="Markdown")
            return
        
        target = args[1].replace("@", "")
        uid, uname = None, None
        if target.isdigit(): uid = int(target)
        else: uname = target
        
        ban_user(user_id=uid, username=uname)
        bot.reply_to(message, f"✅ *User Banned:* `{target}`\nTheir messages will no longer be processed.", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Ban Error: {e}")

@bot.message_handler(commands=['unban', 'unblock'])
def cmd_unban_user(message):
    if message.from_user.id != ADMIN_ID: return
    try:
        args = message.text.split()
        if len(args) < 2:
            bot.reply_to(message, "💡 *Usage:* `/unban` or `/unblock [username_or_id]`", parse_mode="Markdown")
            return
        
        target = args[1].replace("@", "")
        uid, uname = None, None
        if target.isdigit(): uid = int(target)
        else: uname = target
        
        unban_user(user_id=uid, username=uname)
        bot.reply_to(message, f"✅ *User Unbanned:* `{target}`", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Unban Error: {e}")

@bot.message_handler(commands=['banlist', 'blocklist'])
def cmd_ban_list(message):
    if message.from_user.id != ADMIN_ID: return
    bot.send_message(message.chat.id, "🚫 *Banned Users*\n\nMessages from these users are ignored by all automated tasks:", reply_markup=banlist_markup(), parse_mode="Markdown")

@bot.message_handler(commands=["logout"])
def cmd_logout(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("✅ Yes, Logout", callback_data="user_logout_do"))
    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="dash_main"))
    bot.send_message(message.chat.id, "⚠️ *Logout Confirmation*\n\nThis will stop the userbot and delete the session from the database. Are you sure?", reply_markup=markup, parse_mode="Markdown")

@bot.message_handler(commands=['addmanager'])
def cmd_add_manager(message):
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        bot.reply_to(message, "💡 *Usage:* `/addmanager [username_or_id]`", parse_mode="Markdown")
        return
    
    target_user = args[1]
    status_msg = bot.send_message(message.chat.id, "⏳ Resolving manager user...")
    
    async def do_add():
        try:
            is_ok, msg = await ensure_userbot()
            if not is_ok:
                bot.edit_message_text(f"❌ Userbot error: {msg}", message.chat.id, status_msg.message_id)
                return
            
            user_entity = await resolve_target_id(userbot, target_user)
            uid = user_entity.id
            uname = getattr(user_entity, 'username', None) or ""
            
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                if USING_POSTGRES:
                    c.execute("INSERT INTO managers (user_id, username) VALUES (%s, %s) ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username", (uid, uname))
                else:
                    c.execute("INSERT OR REPLACE INTO managers (user_id, username) VALUES (?, ?)", (uid, uname))
            
            bot.edit_message_text(
                f"✅ *Manager Authorized!*\n\n**User ID:** `{uid}`\n**Username:** `@{uname}`" if uname else f"✅ *Manager Authorized!*\n\n**User ID:** `{uid}`",
                message.chat.id,
                status_msg.message_id,
                parse_mode="Markdown"
            )
            
            # Send welcome DM to the new manager from the userbot!
            welcome_msg = (
                "🎉 **Congratulations! You have been authorized as a Manager!**\n\n"
                "You can now configure target pairs and instruct the userbot to join groups directly through this chat!\n\n"
                "🛠️ **Available Commands:**\n"
                "• `.join <link_or_username>`: Request the userbot to join a group or channel.\n"
                "• `.pair <source> <target>` (or `.addpair`): Link a source chat to a target chat for live forwarding.\n"
                "• `.delpair <pair_id>`: Delete a target pair.\n"
                "• `.pairs` (or `.listpairs`): List all active target pairs.\n"
                "• `.setpair <pair_id> <live/mon/mir> <1/0>`: Turn settings on (1) or off (0).\n\n"
                "💬 **Group Joining Wizard:**\n"
                "Simply send any Telegram group link or username (e.g. `t.me/cctest` or `@cctest`) to this chat, and I will automatically guide you on how to join and configure it!"
            )
            try:
                await userbot.send_message(user_entity, welcome_msg)
            except Exception as welcome_err:
                logger.error(f"Failed to send welcome message to new manager {uid}: {welcome_err}")
                
        except Exception as e:
            bot.edit_message_text(f"❌ Failed to authorize manager: {e}", message.chat.id, status_msg.message_id)
    
    asyncio.run_coroutine_threadsafe(do_add(), loop)

@bot.message_handler(commands=['delmanager'])
def cmd_del_manager(message):
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        bot.reply_to(message, "💡 *Usage:* `/delmanager [username_or_id]`", parse_mode="Markdown")
        return
    
    target_user = args[1]
    status_msg = bot.send_message(message.chat.id, "⏳ Resolving manager user...")
    
    async def do_del():
        try:
            if target_user.lstrip("-").isdigit():
                uid = int(target_user)
            else:
                is_ok, msg = await ensure_userbot()
                if not is_ok:
                    bot.edit_message_text(f"❌ Userbot error: {msg}", message.chat.id, status_msg.message_id)
                    return
                user_entity = await resolve_target_id(userbot, target_user)
                uid = user_entity.id
            
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                c.execute(f"DELETE FROM managers WHERE user_id = {p}", (uid,))
            
            bot.edit_message_text(f"✅ *Manager Revoked!*\n\n**User ID:** `{uid}`", message.chat.id, status_msg.message_id, parse_mode="Markdown")
        except Exception as e:
            bot.edit_message_text(f"❌ Failed to revoke manager: {e}", message.chat.id, status_msg.message_id)
    
    asyncio.run_coroutine_threadsafe(do_del(), loop)

@bot.message_handler(commands=['managers'])
def cmd_list_managers(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        with db_conn() as conn:
            c = conn.cursor()
            c.execute("SELECT user_id, username FROM managers ORDER BY user_id ASC")
            rows = c.fetchall()
        
        if not rows:
            bot.send_message(message.chat.id, "📭 *No additional managers authorized.*", parse_mode="Markdown")
            return
        
        try:
            userbot_sessions = get_userbot_sessions()
            userbot_uids = {s[5] for s in userbot_sessions if s[5] is not None}
        except Exception:
            userbot_uids = set()
            
        text_lines = ["📋 *Authorized Managers:*\n"]
        for uid, uname in rows:
            escaped_uname = uname.replace("_", "\\_") if uname else ""
            suffix = " (Manager + Userbot)" if uid in userbot_uids else ""
            text_lines.append(f"👤 `{uid}`" + (f" (@{escaped_uname})" if uname else "") + suffix)
        
        bot.send_message(message.chat.id, "\n".join(text_lines), parse_mode="Markdown")
    except Exception as e:
        bot.send_message(message.chat.id, f"❌ Error fetching managers: {e}")

@bot.message_handler(commands=['setpromo'])
def cmd_set_promo(message):
    if message.from_user.id != ADMIN_ID:
        return
    args = message.text.split()
    if len(args) < 2:
        bot.reply_to(message, "💡 *Usage:* `/setpromo [keyword]` (or `/setpromo disable` to turn off)", parse_mode="Markdown")
        return
    
    keyword = args[1]
    if keyword.lower() in ['disable', 'none', 'off']:
        set_setting("promotion_keyword", "")
        bot.reply_to(message, "✅ *Promotion Keyword Disabled.* Anyone sending the keyword will no longer be promoted.", parse_mode="Markdown")
    else:
        set_setting("promotion_keyword", keyword)
        bot.reply_to(message, f"✅ *Promotion Keyword Set!*\n\nKeyword: `{keyword}`\nUsers sending this exact code in DMs will be automatically promoted to Manager.", parse_mode="Markdown")

@bot.message_handler(commands=['promo'])
def cmd_get_promo(message):
    if message.from_user.id != ADMIN_ID:
        return
    keyword = get_setting("promotion_keyword")
    if keyword:
        bot.reply_to(message, f"🔑 *Active Promotion Keyword:* `{keyword}`\nUsers sending this exact code in DMs will be promoted.", parse_mode="Markdown")
    else:
        bot.reply_to(message, "🔑 *Active Promotion Keyword:* `None/Disabled`\nUse `/setpromo [word]` to set one.", parse_mode="Markdown")

@bot.message_handler(func=lambda m: not is_authorized_manager(m.from_user.id))
def handle_unauthorized_direct_message(message):
    text = message.text.strip() if message.text else ""
    if not text: return
    
    # We need to run it in the event loop because check_and_promote_user is async and needs Telethon userbot
    async def run_promo_check():
        is_ok, msg = await ensure_userbot()
        if not is_ok:
            return
            
        async def bot_reply(reply_text):
            bot.reply_to(message, reply_text, parse_mode="Markdown")
            
        await check_and_promote_user(userbot, message.from_user.id, message.from_user.username, text, bot_reply)

    asyncio.run_coroutine_threadsafe(run_promo_check(), loop)

def parse_telegram_link(text):
    import re
    text = text.strip()
    # Private Invite links
    m = re.search(r'(?:t\.me|telegram\.me)/joinchat/([a-zA-Z0-9_\-]+)', text)
    if m: return {"type": "invite", "hash": m.group(1)}
    m = re.search(r'(?:t\.me|telegram\.me)/\+([a-zA-Z0-9_\-]+)', text)
    if m: return {"type": "invite", "hash": m.group(1)}
    # Public Usernames
    m = re.search(r'(?:t\.me|telegram\.me)/([a-zA-Z0-9_]{5,})', text)
    if m: return {"type": "username", "username": m.group(1)}
    m = re.search(r'^@([a-zA-Z0-9_]{5,})$', text)
    if m: return {"type": "username", "username": m.group(1)}
    return None

async def resolve_chat_and_topic(client, input_str):
    import re
    input_str = input_str.strip()
    topic_id = None
    
    # Check for explicit colon topic_id at the end, e.g. chat:123
    if ":" in input_str:
        parts = input_str.rsplit(":", 1)
        if parts[1].isdigit():
            input_str = parts[0]
            topic_id = int(parts[1])
            
    # Try parsing telegram URL for topic path:
    m_private = re.search(r'(?:t\.me|telegram\.me)/c/(\d+)/(\d+)', input_str)
    if m_private:
        chat_ref = int(f"-100{m_private.group(1)}")
        if topic_id is None:
            topic_id = int(m_private.group(2))
        entity = await resolve_target_id(client, chat_ref)
        return entity, topic_id
        
    m_public = re.search(r'(?:t\.me|telegram\.me)/([a-zA-Z0-9_]{5,})/(\d+)', input_str)
    if m_public:
        chat_ref = m_public.group(1)
        if topic_id is None:
            topic_id = int(m_public.group(2))
        entity = await resolve_target_id(client, chat_ref)
        return entity, topic_id

    # Fallback to parse_telegram_link logic
    parsed = parse_telegram_link(input_str)
    if parsed:
        if parsed["type"] == "invite":
            from telethon.tl.functions.messages import ImportChatInviteRequest
            try:
                result = await client(ImportChatInviteRequest(parsed["hash"]))
                if hasattr(result, "chats") and result.chats:
                    entity = result.chats[0]
                else:
                    entity = await resolve_target_id(client, input_str)
            except errors.UserAlreadyParticipantError:
                from telethon.tl.functions.messages import CheckChatInviteRequest
                try:
                    invite_info = await client(CheckChatInviteRequest(parsed["hash"]))
                    entity = invite_info.chat
                except Exception:
                    entity = await resolve_target_id(client, input_str)
            except Exception as e:
                raise Exception(f"Failed to join invite link: {e}")
        else: # type == username
            entity = await resolve_target_id(client, parsed["username"])
    else:
        # Numeric ID or raw string
        entity = await resolve_target_id(client, input_str)
        
    return entity, topic_id

async def join_chat_task(call, link_type, value):
    try:
        is_ok, msg = await ensure_userbot()
        if not is_ok:
            bot.edit_message_text(f"❌ Userbot connection failed: {msg}", call.message.chat.id, call.message.message_id)
            return
        
        bot.edit_message_text("⏳ *Userbot joining chat...*", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
        
        chat_entity = None
        if link_type == "invite":
            from telethon.tl.functions.messages import ImportChatInviteRequest
            try:
                result = await userbot(ImportChatInviteRequest(value))
                if hasattr(result, "chats") and result.chats:
                    chat_entity = result.chats[0]
            except errors.UserAlreadyParticipantError:
                from telethon.tl.functions.messages import CheckChatInviteRequest
                invite_info = await userbot(CheckChatInviteRequest(value))
                if hasattr(invite_info, "chat"):
                    chat_entity = invite_info.chat
            except Exception as e:
                bot.edit_message_text(f"❌ *Failed to join invite link:*\n`{e}`", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
                return
        else:
            from telethon.tl.functions.channels import JoinChannelRequest
            try:
                chat_entity = await resolve_target_id(userbot, value)
                await userbot(JoinChannelRequest(chat_entity))
            except Exception as e:
                bot.edit_message_text(f"❌ *Failed to join username:*\n`{e}`", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
                return
                
        if not chat_entity:
            bot.edit_message_text("❌ Could not resolve chat details after joining.", call.message.chat.id, call.message.message_id)
            return
            
        from telethon.utils import get_peer_id
        peer_id = get_peer_id(chat_entity)
        chat_title = getattr(chat_entity, "title", "Joined Chat")
        
        login_data[call.from_user.id] = {
            "joined_chat_id": peer_id,
            "joined_chat_title": chat_title
        }
        
        text = (f"✅ *Successfully Joined!*\n\n"
                f"Group: `{chat_title}`\n"
                f"ID: `{peer_id}`\n\n"
                f"What would you like to set this group as?")
                
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton("👁️ Set as Source", callback_data=f"join_set_source|{peer_id}"),
            InlineKeyboardButton("🎯 Set as Target", callback_data=f"join_set_target|{peer_id}"),
            InlineKeyboardButton("❌ Nothing (Just Join)", callback_data="join_set_nothing")
        )
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"Join Chat Task Error: {e}")
        bot.edit_message_text(f"❌ Join error: {e}", call.message.chat.id, call.message.message_id)

async def finalize_pair_task(call, uid):
    try:
        data = login_data.get(uid)
        if not data:
            bot.send_message(call.message.chat.id, "❌ Session expired. Please try again.")
            return

        sid = data["source_id"]
        stid = data["source_topic_id"]
        tid = data["target_id"]
        ttid = data["target_topic_id"]

        bot.edit_message_text("⏳ Resolving pair details...", call.message.chat.id, call.message.message_id)
        
        s_chat = await resolve_target_id(userbot, sid)
        t_chat = await resolve_target_id(userbot, tid)
        
        s_title = getattr(s_chat, 'title', None) or getattr(s_chat, 'first_name', None) or str(sid)
        t_title = getattr(t_chat, 'title', None) or getattr(t_chat, 'first_name', None) or str(tid)
        
        add_target_pair(sid, stid, tid, ttid, s_title, t_title)
        
        success_text = f"✅ *Pair Added!*\n\n"
        success_text += f"Source: `{s_title}`" + (f" (Topic: `{stid}`)" if stid else "") + "\n"
        success_text += f"Target: `{t_title}`" + (f" (Topic: `{ttid}`)" if ttid else "")
        
        bot.send_message(call.message.chat.id, success_text, parse_mode="Markdown")
        bot.send_message(call.message.chat.id, "🎯 *Target Pairs*", reply_markup=pairs_list_markup())
        
        # Cleanup
        login_data.pop(uid, None)
    except Exception as e:
        logger.error(f"Finalize Pair Error: {e}")
        bot.send_message(call.message.chat.id, f"❌ Pair error: {e}")

@bot.callback_query_handler(func=lambda call: True)
def handle_callbacks(call):
    global userbot
    uid = call.from_user.id
    if not is_authorized_manager(uid):
        return

    data = call.data
    
    if data == "progress_bar_click":
        bot.answer_callback_query(call.id)
        return
        
    if data.startswith("join_chat_yes|"):
        parts = data.split("|")
        link_type = parts[1]
        value = parts[2]
        asyncio.run_coroutine_threadsafe(join_chat_task(call, link_type, value), loop)
        return
        
    elif data == "join_chat_cancel":
        bot.answer_callback_query(call.id, "Cancelled")
        bot.edit_message_text("❌ Join request cancelled.", call.message.chat.id, call.message.message_id)
        return
        
    elif data == "join_set_nothing":
        bot.answer_callback_query(call.id)
        login_data.pop(uid, None)
        bot.edit_message_text("✅ Chat joined. No automation pairs were created.", call.message.chat.id, call.message.message_id)
        return

    elif data == "pm_fwd_main":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(get_pm_fwd_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_fwd_markup(), parse_mode="Markdown")

    elif data == "pm_fwd_toggle":
        enabled = get_setting("pm_media_forwarding_enabled") == "1"
        new_state = "0" if enabled else "1"
        set_setting("pm_media_forwarding_enabled", new_state)
        bot.answer_callback_query(call.id, "System Enabled" if new_state == "1" else "System Disabled")
        bot.edit_message_text(get_pm_fwd_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_fwd_markup(), parse_mode="Markdown")

    elif data == "pm_fwd_toggle_destructive":
        allow_dest = get_setting("pm_media_forwarding_allow_destructive") == "1"
        new_state = "0" if allow_dest else "1"
        set_setting("pm_media_forwarding_allow_destructive", new_state)
        bot.answer_callback_query(call.id, "Self-Destructing Allowed" if new_state == "1" else "Self-Destructing Blocked")
        bot.edit_message_text(get_pm_fwd_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_fwd_markup(), parse_mode="Markdown")

    elif data == "pm_fwd_clear_targets":
        set_setting("pm_media_forwarding_targets", "")
        bot.answer_callback_query(call.id, "Targets Cleared")
        bot.edit_message_text(get_pm_fwd_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_fwd_markup(), parse_mode="Markdown")

    elif data == "pm_fwd_add_target":
        bot.answer_callback_query(call.id)
        async def show_pm_tgt():
            markup = await get_chat_selection_markup("pm_tgt", 0)
            bot.edit_message_text("🎯 *Select Target Chat*\nChoose the group or channel to forward private media to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(show_pm_tgt(), loop)

    elif data.startswith("pm_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_pm_tgt():
                markup = await get_chat_selection_markup("pm_tgt", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_pm_tgt(), loop)
        else:
            tid = int(parts[2])
            targets_str = get_setting("pm_media_forwarding_targets") or ""
            target_ids = [t.strip() for t in targets_str.split(",") if t.strip()]
            if str(tid) not in target_ids:
                target_ids.append(str(tid))
                set_setting("pm_media_forwarding_targets", ",".join(target_ids))
                bot.answer_callback_query(call.id, "Target chat added!")
            else:
                bot.answer_callback_query(call.id, "Target already added.")
            
            bot.edit_message_text(get_pm_fwd_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_fwd_markup(), parse_mode="Markdown")

    elif data == "pm_mon_settings":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(get_pm_mon_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_mon_markup(), parse_mode="Markdown")

    elif data == "pm_mon_toggle":
        enabled = get_setting("pm_chat_monitoring_enabled") == "1"
        new_state = "0" if enabled else "1"
        set_setting("pm_chat_monitoring_enabled", new_state)
        bot.answer_callback_query(call.id, "Monitor Enabled" if new_state == "1" else "Monitor Disabled")
        bot.edit_message_text(get_pm_mon_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_mon_markup(), parse_mode="Markdown")

    elif data == "pm_mon_set_dest":
        bot.answer_callback_query(call.id)
        async def show_pm_mon_tgt():
            markup = await get_chat_selection_markup("pm_mon_tgt", 0)
            bot.edit_message_text("🎯 *Select Destination Chat*\nChoose the group or channel to send private chat logs to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(show_pm_mon_tgt(), loop)

    elif data.startswith("pm_mon_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[3] == "page":
            page = int(parts[4])
            async def update_pm_mon_tgt():
                markup = await get_chat_selection_markup("pm_mon_tgt", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_pm_mon_tgt(), loop)
        else:
            tid = int(parts[3])
            set_setting("pm_chat_monitoring_destination", str(tid))
            bot.answer_callback_query(call.id, "Destination set!")
            bot.edit_message_text(get_pm_mon_text(), call.message.chat.id, call.message.message_id, reply_markup=get_pm_mon_markup(), parse_mode="Markdown")

    elif data.startswith("sel_search|"):
        bot.answer_callback_query(call.id)
        prefix = data.split("|")[1]
        
        bot.edit_message_text(
            "🔍 *Search Group or Channel*\n\nPlease send me the exact name or keyword of the group/channel you want to search:",
            call.message.chat.id,
            call.message.message_id,
            parse_mode="Markdown"
        )
        
        def process_chat_search_query(message):
            if not is_authorized_manager(message.from_user.id):
                return
            query = message.text.strip() if message.text else ""
            if not query:
                bot.reply_to(message, "❌ Search cancelled or invalid input.")
                return
            
            async def search_dialogs_task():
                try:
                    search_msg = bot.send_message(message.chat.id, "🔍 Searching dialogues...")
                    
                    chats = []
                    async for dialog in userbot.iter_dialogs(limit=500):
                        entity = dialog.entity
                        if isinstance(entity, (types.Chat, types.Channel, types.User)):
                            title = ""
                            if isinstance(entity, (types.Chat, types.Channel)):
                                title = entity.title or ""
                            elif isinstance(entity, types.User):
                                title = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
                            
                            if query.lower() in title.lower():
                                chats.append(dialog)
                    
                    bot.delete_message(message.chat.id, search_msg.message_id)
                    
                    if not chats:
                        bot.send_message(message.chat.id, f"❌ No group or channel found matching `{query}`.", parse_mode="Markdown")
                        return
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    for dialog in chats[:15]:
                        chat = dialog.entity
                        is_forum = getattr(chat, "forum", False)
                        
                        if isinstance(chat, types.Channel):
                            if is_forum: icon = "🏛️"; t = f"『 TOPIC 』 {chat.title}"
                            elif chat.broadcast: icon = "📢"; t = chat.title
                            else: icon = "👥"; t = chat.title
                        elif isinstance(chat, types.Chat):
                            icon = "👥"; t = chat.title
                        elif isinstance(chat, types.User):
                            icon = "👤"; t = f"{chat.first_name or ''} {chat.last_name or ''}".strip()
                        else:
                            icon = "💬"; t = "Unknown"
                            
                        markup.add(
                            InlineKeyboardButton(
                                f"{icon} {t}",
                                callback_data=f"{prefix}_{chat.id}"
                            )
                        )
                        
                    markup.add(InlineKeyboardButton("🔙 Cancel", callback_data="dash_main"))
                    bot.send_message(message.chat.id, f"🔍 *Search Results for '{query}':*", reply_markup=markup, parse_mode="Markdown")
                    
                except Exception as err:
                    logger.error(f"Search task failed: {err}")
                    bot.send_message(message.chat.id, f"❌ Search error: {err}")
            
            asyncio.run_coroutine_threadsafe(search_dialogs_task(), loop)
            
        bot.register_next_step_handler(call.message, process_chat_search_query)
        return

    elif data.startswith("join_set_source|"):
        bot.answer_callback_query(call.id)
        parts = data.split("|")
        sid = int(parts[1])
        
        async def init_source_flow():
            try:
                full_chat = await resolve_target_id(userbot, sid)
                is_forum = getattr(full_chat, "forum", False)
                if is_forum:
                    markup = await get_topic_selection_markup(sid, "join_src_topic")
                    bot.edit_message_text(f"🧵 *『 {getattr(full_chat, 'title', 'Forum')} 』*\nSelect a source topic:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                else:
                    login_data[uid] = {
                        "source_id": sid,
                        "source_topic_id": None,
                        "preselected_flow": "source"
                    }
                    markup = await get_chat_selection_markup("sel_tgt", 0)
                    bot.edit_message_text("🎯 *Select Target Chat*\nChoose the group or channel to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            except Exception as e:
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")
        asyncio.run_coroutine_threadsafe(init_source_flow(), loop)
        return

    elif data.startswith("join_src_topic_"):
        bot.answer_callback_query(call.id)
        payload = data.replace("join_src_topic_", "", 1)
        sid_str, stid_str = payload.rsplit("_", 1)
        sid = int(sid_str)
        stid = int(stid_str)
        if stid == 0: stid = None
        
        login_data[uid] = {
            "source_id": sid,
            "source_topic_id": stid,
            "preselected_flow": "source"
        }
        async def show_tgt():
            markup = await get_chat_selection_markup("sel_tgt", 0)
            bot.edit_message_text("🎯 *Select Target Chat*\nChoose the group or channel to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(show_tgt(), loop)
        return

    elif data.startswith("join_set_target|"):
        bot.answer_callback_query(call.id)
        parts = data.split("|")
        tid = int(parts[1])
        
        login_data[uid] = {
            "target_id": tid,
            "target_topic_id": None,
            "preselected_flow": "target"
        }
        async def show_src():
            markup = await get_chat_selection_markup("sel_src", 0)
            bot.edit_message_text("🎯 *Select Source Chat*\nChoose the group or channel to collect from:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(show_src(), loop)
        return
    
    if data == "dash_main":
        bot.answer_callback_query(call.id)
        bot.edit_message_text(get_dashboard_text(), call.message.chat.id, call.message.message_id, reply_markup=get_dashboard_markup(), parse_mode="Markdown")

    elif data == "vault_main":
        bot.answer_callback_query(call.id)
        bots = get_log_bots()
        total_vaulted = get_total_vaulted_count()
        group_stats = get_all_vault_stats()
        
        text = "🔒 *VAULT CONSOLE*\n\n"
        text += "🤖 *Active Vault Bots:*\n"
        if bots:
            for token, username, bot_id in bots:
                stats = get_logged_media_stats(bot_id)
                text += f"• @{username} (`{stats}` items)\n"
        else:
            text += "• _No vault bots added._\n"
            
        text += f"\n📦 *Total Vaulted Content:* `{total_vaulted}` items\n\n"
        
        text += "📁 *Breakdown by Source Group:*\n"
        if group_stats:
            for sid, title, count in group_stats:
                if sid == 0 or not sid: continue
                text += f"• `{title}`: `{count}` items\n"
        else:
            text += "• _No vaulted content breakdown available._\n"
            
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=vault_console_markup(), parse_mode="Markdown")

    elif data == "vault_rel_main":
        bot.answer_callback_query(call.id)
        sources = get_vault_sources()
        if not sources:
            bot.edit_message_text("❌ No vaulted media found. Make sure you have collected media and set up a Log Target.", call.message.chat.id, call.message.message_id, reply_markup=vault_console_markup())
            return
            
        markup = InlineKeyboardMarkup(row_width=1)
        for sid, title in sources:
            markup.add(InlineKeyboardButton(f"📁 {title}", callback_data=f"vault_src_{sid}"))
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="vault_main"))
        
        bot.edit_message_text("🚀 *Vault Release Engine*\n\nSelect the source group you want to release media for:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("vault_src_"):
        bot.answer_callback_query(call.id)
        sid = int(data.split("_")[-1])
        login_data[uid] = {"vault_source_id": sid}
        
        async def show_tgt():
            markup = await get_chat_selection_markup("vault_tgt", 0)
            bot.edit_message_text("🎯 *Select Target Chat*\n\nChoose the group/channel where you want to release this media.\n⚠️ *IMPORTANT*: The Main Bot must be an admin in the target chat!", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(show_tgt(), loop)
        
    elif data.startswith("vault_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_tgt_list():
                markup = await get_chat_selection_markup("vault_tgt", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_tgt_list(), loop)
        else:
            tid = int(parts[2])
            sid = login_data.get(uid, {}).get("vault_source_id")
            if not sid:
                bot.send_message(call.message.chat.id, "❌ Session expired. Please start over.")
                return
            
            # Start background task
            login_data.pop(uid, None)
            bot.edit_message_text(f"🚀 *Starting Vault Release*\n\nDistributing media to target: `{tid}`\nThis may take some time due to Telegram rate limits.", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
            asyncio.run_coroutine_threadsafe(run_vault_release(bot, call.message.chat.id, sid, tid), loop)

    elif data == "log_bot_main":
        bot.answer_callback_query(call.id)
        bot.edit_message_text("📜 *Log Bot System*\nManage your backup bots and storage:", call.message.chat.id, call.message.message_id, reply_markup=log_bot_list_markup(), parse_mode="Markdown")

    elif data == "banlist_main":
        bot.answer_callback_query(call.id)
        bot.edit_message_text("🚫 *Banned Users List*\n\nSelect a user to unban or add a new one:", call.message.chat.id, call.message.message_id, reply_markup=banlist_markup(), parse_mode="Markdown")

    elif data == "ban_add_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_ban_target"
        bot.send_message(call.message.chat.id, "🚫 *Add to Ban List*\n\nPlease send the *Username* or *User ID* you want to block.")

    elif data.startswith("unban_confirm_"):
        target = data.replace("unban_confirm_", "")
        bot.answer_callback_query(call.id)
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Confirm Unban", callback_data=f"unban_do_{target}"))
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data="banlist_main"))
        bot.edit_message_text(f"❓ *Unban User:* `{target}`?", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("unban_do_"):
        target = data.replace("unban_do_", "")
        bot.answer_callback_query(call.id, "User Unbanned")
        uid, uname = None, None
        if target.isdigit(): uid = int(target)
        else: uname = target
        unban_user(user_id=uid, username=uname)
        bot.edit_message_text("🚫 *Banned Users List*\n\nSelect a user to unban or add a new one:", call.message.chat.id, call.message.message_id, reply_markup=banlist_markup(), parse_mode="Markdown")

    elif data == "log_bot_add_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_log_bot_token"
        bot.send_message(call.message.chat.id, "📜 *Add Log Bot*\nPlease send the *Bot Token* of your backup bot.\n\n_Note: You must create this bot via @BotFather._")

    elif data.startswith("log_bot_view_"):
        bot_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        stats = get_logged_media_stats(bot_id)
        
        # Find username
        bots = get_log_bots()
        username = next((b[1] for b in bots if b[2] == bot_id), "Unknown")
        
        text = f"🤖 *Log Bot:* @{username}\n\n"
        text += f"📊 *Stats:*\n"
        text += f"📦 Total Items: `{stats}`\n\n"
        text += "_Click Fetch to download a file containing all logged media IDs._"
        
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=log_bot_view_markup(bot_id), parse_mode="Markdown")

    elif data.startswith("log_bot_fetch_"):
        bot_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "📂 Generating log file...")
        
        media = fetch_logged_media(bot_id)
        if not media:
            bot.send_message(call.message.chat.id, "❌ No logs found for this bot.")
            return
            
        file_content = f"LOG MEDIA REPORT - BOT ID: {bot_id}\n"
        file_content += "="*40 + "\n\n"
        for sid, smid, fid, mtype, cap in media:
            file_content += f"SOURCE: {sid} | MSG: {smid} | TYPE: {mtype}\n"
            file_content += f"FILE_ID: {fid}\n"
            if cap: file_content += f"CAPTION: {cap[:50]}...\n"
            file_content += "-"*20 + "\n"
            
        filename = f"log_media_{bot_id}.txt"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(file_content)
            
        # Find username
        bots = get_log_bots()
        username = next((b[1] for b in bots if b[2] == bot_id), str(bot_id))
        
        with open(filename, "rb") as f:
            bot.send_document(call.message.chat.id, f, caption=f"📂 Media Logs for @{username}")
        
        try: os.remove(filename)
        except: pass

    elif data.startswith("log_bot_delete_confirm_"):
        bot_id = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Confirm Delete", callback_data=f"log_bot_delete_do_{bot_id}"))
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"log_bot_view_{bot_id}"))
        bot.edit_message_text(f"⚠️ *Delete Log Bot?*\n\nThis will stop the bot and delete all `{get_logged_media_stats(bot_id)}` logged media records from the database. This cannot be undone!", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("log_bot_delete_do_"):
        bot_id = int(data.split("_")[-1])
        delete_log_bot(bot_id)
        # Remove from fleet
        if bot_id in log_bot_manager.bots:
            try:
                log_bot_manager.bots[bot_id].stop_polling()
                del log_bot_manager.bots[bot_id]
            except: pass
            
        bot.answer_callback_query(call.id, "Log Bot Removed")
        bot.edit_message_text("📜 *Log Bot System*\nManage your backup bots and storage:", call.message.chat.id, call.message.message_id, reply_markup=log_bot_list_markup(), parse_mode="Markdown")

    elif data == "pairs_main":
        bot.answer_callback_query(call.id)
        try:
            markup = pairs_list_markup()
            bot.edit_message_text("🎯 *Target Pairs*\nSelect a pair to manage collection or release:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Pairs List Error: {e}")
            bot.send_message(call.message.chat.id, f"❌ Error loading pairs: {e}")

    elif data == "pair_add_start":
        bot.answer_callback_query(call.id, "🔍 Loading your chats...")
        async def show_src_list():
            try:
                is_ok, msg = await ensure_userbot()
                if not is_ok:
                    bot.send_message(call.message.chat.id, f"❌ Userbot connection failed: {msg}\n\nPlease go to *👤 User Account* and ensure your session is active.")
                    return
                
                markup = await get_chat_selection_markup("sel_src", 0)
                if markup:
                    bot.edit_message_text("🎯 *Select Source Chat*\nChoose the group or channel to collect from:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                else:
                    bot.edit_message_text("❌ No chats found. Make sure your userbot is in at least one group or channel.", call.message.chat.id, call.message.message_id, reply_markup=get_dashboard_markup())
            except Exception as e:
                logger.error(f"Add Pair Start Error: {e}")
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")
        asyncio.run_coroutine_threadsafe(show_src_list(), loop)

    elif data.startswith("sel_src_topic_"):
        bot.answer_callback_query(call.id)
        # Safer parsing for negative IDs with underscores
        payload = data.replace("sel_src_topic_", "", 1)
        sid_str, stid_str = payload.rsplit("_", 1)
        sid = int(sid_str)
        stid = int(stid_str)
        
        # 0 = entire topic group/forum
        if stid == 0:
            stid = None
            
        if uid not in login_data:
            login_data[uid] = {}
        login_data[uid]["source_id"] = sid
        login_data[uid]["source_topic_id"] = stid
        
        if login_data[uid].get("preselected_flow") == "target":
            asyncio.run_coroutine_threadsafe(finalize_pair_task(call, uid), loop)
        else:
            async def show_tgt():
                markup = await get_chat_selection_markup("sel_tgt", 0)
                bot.edit_message_text("🎯 *Select Target Chat*\nChoose the group or channel to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
            asyncio.run_coroutine_threadsafe(show_tgt(), loop)

    elif data.startswith("create_topic_prompt_"):
        bot.answer_callback_query(call.id)
        chat_id = int(data.replace("create_topic_prompt_", ""))
        admin_states[uid] = f"awaiting_new_topic_name_{chat_id}"
        bot.send_message(call.message.chat.id, "➕ *Create New Topic*\n\nPlease send the *Name* of the topic you want to create in the source group:")

    elif data.startswith("sel_src_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_src_list():
                markup = await get_chat_selection_markup("sel_src", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_src_list(), loop)
        else:
            sid = int(parts[2])
            async def handle_src():
                try:
                    full_chat = await resolve_target_id(userbot, sid)
                    is_forum = getattr(full_chat, "forum", False)
                    
                    if is_forum:
                        markup = await get_topic_selection_markup(sid, "sel_src_topic")
                        bot.edit_message_text(f"🧵 *『 {getattr(full_chat, 'title', 'Forum')} 』*\nSelect a source topic:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                    else:
                        if uid not in login_data:
                            login_data[uid] = {}
                        login_data[uid]["source_id"] = sid
                        login_data[uid]["source_topic_id"] = None
                        
                        if login_data[uid].get("preselected_flow") == "target":
                            await finalize_pair_task(call, uid)
                        else:
                            markup = await get_chat_selection_markup("sel_tgt", 0)
                            bot.edit_message_text("🎯 *Select Target Chat*\nChoose the group or channel to send to:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                except Exception as e:
                    bot.send_message(call.message.chat.id, f"❌ Error: {e}")
            asyncio.run_coroutine_threadsafe(handle_src(), loop)

    elif data.startswith("sel_tgt_topic_"):
        bot.answer_callback_query(call.id)
        # Safer parsing for negative IDs with underscores
        payload = data.replace("sel_tgt_topic_", "", 1)
        tid_str, ttid_str = payload.rsplit("_", 1)
        tid = int(tid_str)
        ttid = int(ttid_str)
        
        # 0 = entire topic group/forum
        if ttid == 0:
            ttid = None
            
        login_data[uid]["target_id"] = tid
        login_data[uid]["target_topic_id"] = ttid
        asyncio.run_coroutine_threadsafe(finalize_pair_task(call, uid), loop)

    elif data.startswith("sel_tgt_"):
        bot.answer_callback_query(call.id)
        parts = data.split("_")
        if parts[2] == "page":
            page = int(parts[3])
            async def update_tgt_list():
                markup = await get_chat_selection_markup("sel_tgt", page)
                if markup:
                    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
            asyncio.run_coroutine_threadsafe(update_tgt_list(), loop)
        else:
            tid = int(parts[2])
            async def handle_tgt():
                try:
                    full_chat = await resolve_target_id(userbot, tid)
                    is_forum = getattr(full_chat, "forum", False)
                    
                    if is_forum:
                        markup = await get_topic_selection_markup(tid, "sel_tgt_topic")
                        bot.edit_message_text(f"🧵 *『 {getattr(full_chat, 'title', 'Forum')} 』*\nSelect a target topic:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                    else:
                        login_data[uid]["target_id"] = tid
                        login_data[uid]["target_topic_id"] = None
                        await finalize_pair_task(call, uid)
                except Exception as e:
                    bot.send_message(call.message.chat.id, f"❌ Error: {e}")
            asyncio.run_coroutine_threadsafe(handle_tgt(), loop)

    elif data.startswith("pair_view_"):
        bot.answer_callback_query(call.id)
        pid = int(data.split("_")[-1])
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_toggle_mon_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        if not row:
            bot.answer_callback_query(call.id, "Pair not found")
            return
        new_val = 0 if row[5] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_monitoring = {p} WHERE id = {p}", (new_val, pid))
        
        if new_val == 1:
            bot.send_message(call.message.chat.id, "👁️ Monitoring Started! Initializing full history scan in background...")
            asyncio.run_coroutine_threadsafe(run_collection(call.message.chat.id, pid, limit=None), loop)
        
        bot.answer_callback_query(call.id, f"Monitor {'Started' if new_val else 'Stopped'}")
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_toggle_mir_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        if not row:
            bot.answer_callback_query(call.id, "Pair not found")
            return
        # row[7] is is_mirror
        new_val = 0 if row[7] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_mirror = {p} WHERE id = {p}", (new_val, pid))
        bot.answer_callback_query(call.id, f"Mirror Mode {'Enabled' if new_val else 'Disabled'}")
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_toggle_filter_"):
        pid = int(data.split("_")[-1])
        pair = get_target_pair(pid)
        if not pair: return
        current = pair[10] or "everything"
        next_filter = "media" if current == "everything" else "text" if current == "media" else "file" if current == "text" else "everything"
        
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET content_filter = {p} WHERE id = {p}", (next_filter, pid))
        
        bot.answer_callback_query(call.id, f"🎯 Filter: {next_filter.title()}")
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_toggle_live_"):
        pid = int(data.split("_")[-1])
        row = get_target_pair(pid)
        if not row:
            bot.answer_callback_query(call.id, "Pair not found")
            return
        new_val = 0 if row[6] else 1
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"UPDATE target_pairs SET is_live = {p} WHERE id = {p}", (new_val, pid))
        bot.answer_callback_query(call.id, f"Live Forward {'Started' if new_val else 'Stopped'}")
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_hist_menu_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🔢 Count Based", callback_data=f"pair_hist_type_count_{pid}"),
            InlineKeyboardButton("📅 Date Based", callback_data=f"pair_hist_type_date_{pid}")
        )
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"pair_view_{pid}"))
        bot.edit_message_text("📜 *History Scraper*\n\nChoose your scraping mode:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("pair_hist_type_count_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        admin_states[uid] = f"hist_setup_count_only_{pid}"
        bot.send_message(call.message.chat.id, "🔢 *Count Based Scrape*\n\nHow many messages would you like to scrape?")

    elif data.startswith("pair_hist_type_date_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        admin_states[uid] = f"hist_setup_date_start_{pid}"
        bot.send_message(call.message.chat.id, "📅 *Date Based Scrape*\n\nEnter *Start Date* (DD/MM/YYYY):")

    elif data.startswith("pair_stop_task_"):
        parts = data.split("_")
        pid = int(parts[-1])
        type_str = "_".join(parts[3:-1])
        task_key = f"{type_str}_{pid}"
        if stop_task(task_key):
            bot.answer_callback_query(call.id, f"Stopping {type_str}...")
        else:
            bot.answer_callback_query(call.id, "No active task found.")
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_collect_confirm_"):
        logger.warning(f"CALLBACK DATA = {data}")
        logger.warning("PAIR_COLLECT_CONFIRM")
        # Format: pair_collect_confirm_{pair_id}
        pid = int(data.split("_")[-1])
        logger.info(f"COLLECT_CONFIRM CLICKED: {pid}")
        bot.answer_callback_query(call.id, "🚀 Starting Collection...")
        
        async def transition_and_start(chat_id, msg_id, pair_id):
            logger.info(f"TRANSITION START: {pair_id}")
            try:
                bot.delete_message(chat_id, msg_id)
            except Exception as e:
                logger.error(f"DELETE ERROR: {e}")
            logger.info(f"CALLING run_collection({pair_id})")
            try:
                await run_collection(chat_id, pair_id)
                logger.info(f"run_collection RETURNED: {pair_id}")
            except Exception as e:
                logger.exception(f"COLLECTION CRASH: {e}")
                try:
                    bot.send_message(chat_id, f"❌ Collection crashed:\n<code>{e}</code>", parse_mode="HTML")
                except Exception:
                    pass
            
        asyncio.run_coroutine_threadsafe(transition_and_start(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_collect_cancel_"):
        logger.warning(f"CALLBACK DATA = {data}")
        logger.warning("PAIR_COLLECT_CANCEL")
        # Format: pair_collect_cancel_{pair_id}
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "❌ Collection Cancelled")
        # Reopen pair view directly
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_collect_"):
        logger.warning(f"CALLBACK DATA = {data}")
        logger.warning("PAIR_COLLECT")
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "🔍 Scanning group/channel...")
        asyncio.run_coroutine_threadsafe(run_collection_preview(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_coll_toggle_"):
        parts = data.split("_")
        # Structure: pair_coll_toggle_{pair_id}_{mode}
        pid = int(parts[3])
        mode = parts[4]
        task_key = f"coll_{pid}"
        
        # Check if task is running
        if task_key not in running_tasks or not running_tasks[task_key]:
            bot.answer_callback_query(call.id, "❌ Collection is not currently running.")
            return
            
        instant_rel = (mode == "instant")
        opts = collection_options.setdefault(task_key, {})
        opts["instant_release"] = instant_rel
        if "instant_filter" not in opts:
            opts["instant_filter"] = "everything"
            
        status_text = "⚡ Instant Release Enabled" if instant_rel else "📥 Hold Release Enabled"
        bot.answer_callback_query(call.id, status_text)
        
        # Reconstruct the message text using saved states if available
        if "s_title" in opts:
            try:
                bot.edit_message_text(
                    get_collection_status_text(task_key),
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=get_collection_markup(pid),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error editing message text in toggle callback: {e}")
        else:
            try:
                bot.edit_message_reply_markup(
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=get_collection_markup(pid)
                )
            except Exception as e:
                logger.error(f"Error editing reply markup in toggle callback: {e}")

    elif data.startswith("pair_coll_filter_"):
        pid = int(data.split("_")[-1])
        task_key = f"coll_{pid}"
        
        # Check if task is running
        if task_key not in running_tasks or not running_tasks[task_key]:
            bot.answer_callback_query(call.id, "❌ Collection is not currently running.")
            return
            
        opts = collection_options.setdefault(task_key, {})
        current = opts.get("instant_filter", "everything")
        next_filter = "media" if current == "everything" else "text" if current == "media" else "everything"
        opts["instant_filter"] = next_filter
        
        bot.answer_callback_query(call.id, f"🎯 Filter: {next_filter.title()}")
        
        # Reconstruct the message text using saved states if available
        if "s_title" in opts:
            try:
                bot.edit_message_text(
                    get_collection_status_text(task_key),
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=get_collection_markup(pid),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error editing message text in filter callback: {e}")
        else:
            try:
                bot.edit_message_reply_markup(
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=get_collection_markup(pid)
                )
            except Exception as e:
                logger.error(f"Error editing reply markup in filter callback: {e}")

    elif data.startswith("pair_coll_cfilter_"):
        pid = int(data.split("_")[-1])
        task_key = f"coll_{pid}"
        
        # Check if task is running
        if task_key not in running_tasks or not running_tasks[task_key]:
            bot.answer_callback_query(call.id, "❌ Collection is not currently running.")
            return
            
        opts = collection_options.setdefault(task_key, {})
        current = opts.get("collect_filter", "everything")
        # Cycle through: everything -> media -> text -> file -> everything
        next_filter = "media" if current == "everything" else "text" if current == "media" else "file" if current == "text" else "everything"
        opts["collect_filter"] = next_filter
        
        bot.answer_callback_query(call.id, f"📥 Collect Filter: {next_filter.title()}")
        
        # Reconstruct the message text using saved states if available
        if "s_title" in opts:
            try:
                bot.edit_message_text(
                    get_collection_status_text(task_key),
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=get_collection_markup(pid),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error editing message text in collect filter callback: {e}")
        else:
            try:
                bot.edit_message_reply_markup(
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=get_collection_markup(pid)
                )
            except Exception as e:
                logger.error(f"Error editing reply markup in collect filter callback: {e}")

    elif data.startswith("pair_release_"):
        pid = int(data.split("_")[-1])
        bot.answer_callback_query(call.id)
        
        # Retrieve counts
        mon, scr, col = get_pair_source_counts(pid)
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(
            InlineKeyboardButton(f"👁️ Release Monitor ({mon})", callback_data=f"pair_rel_src_monitor_{pid}"),
            InlineKeyboardButton(f"📜 Release Scraper ({scr})", callback_data=f"pair_rel_src_scraper_{pid}"),
            InlineKeyboardButton(f"📥 Release Collect Now ({col})", callback_data=f"pair_rel_src_collection_{pid}"),
            InlineKeyboardButton("🔙 Back to Pair", callback_data=f"pair_view_{pid}")
        )
        bot.edit_message_text("🚀 *Release Vault Items*\n\nChoose which collection source to release:", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("pair_rel_src_"):
        parts = data.split("_")
        # Structure: pair_rel_src_{source_type}_{pid}
        source_type = parts[3]
        pid = int(parts[4])
        bot.answer_callback_query(call.id)
        
        m_names = {"monitor": "Monitor 👁️", "scraper": "History Scraper 📜", "collection": "Collect Now 📥"}
        display_name = m_names.get(source_type, "Vault Items")
        bot.edit_message_text(f"🚀 *Release Engine: {display_name}*\n\nChoose release mode:", call.message.chat.id, call.message.message_id, reply_markup=get_release_markup(pid, source_type), parse_mode="Markdown")

    elif data.startswith("pair_rel_filter_"):
        parts = data.split("_")
        # Structure: pair_rel_filter_{source_type}_{pid}
        source_type = parts[3]
        pid = int(parts[4])
        
        key = f"{pid}_{source_type}"
        current = release_options.setdefault(key, "everything")
        next_filter = "media" if current == "everything" else "text" if current == "media" else "everything"
        release_options[key] = next_filter
        
        bot.answer_callback_query(call.id, f"🎯 Release Filter: {next_filter.title()}")
        
        try:
            bot.edit_message_reply_markup(
                call.message.chat.id,
                call.message.message_id,
                reply_markup=get_release_markup(pid, source_type)
            )
        except Exception as e:
            logger.error(f"Error updating release markup: {e}")

    elif data.startswith("pair_rel_now_"):
        parts = data.split("_")
        # Structure: pair_rel_now_{source_type}_{pid}
        source_type = parts[3]
        pid = int(parts[4])
        
        # Get chosen release filter
        key = f"{pid}_{source_type}"
        release_filter = release_options.get(key, "everything")
        
        bot.answer_callback_query(call.id, f"🚀 Starting Instant Release...")
        asyncio.run_coroutine_threadsafe(run_release(call.message.chat.id, pid, added_by=source_type, interval=1.5, release_filter=release_filter), loop)
        asyncio.run_coroutine_threadsafe(show_pair_view(call.message.chat.id, call.message.message_id, pid), loop)

    elif data.startswith("pair_rel_slow_"):
        parts = data.split("_")
        # Structure: pair_rel_slow_{source_type}_{pid}
        source_type = parts[3]
        pid = int(parts[4])
        
        # Get chosen release filter
        key = f"{pid}_{source_type}"
        release_filter = release_options.get(key, "everything")
        
        bot.answer_callback_query(call.id)
        admin_states[uid] = f"rel_src_setup_interval_{source_type}_{pid}_{release_filter}"
        bot.send_message(call.message.chat.id, "⏰ *Slow Release Setup*\n\nEnter the *interval* between items in seconds:\n(Example: `60` for 1 minute, `300` for 5 minutes)")
    elif data.startswith("pair_delete_confirm_"):
        pid = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Confirm Delete", callback_data=f"pair_delete_do_{pid}"))
        markup.add(InlineKeyboardButton("❌ Cancel", callback_data=f"pair_view_{pid}"))
        bot.edit_message_text("⚠️ Delete this pair and all its collected media history?", call.message.chat.id, call.message.message_id, reply_markup=markup)

    elif data.startswith("pair_delete_do_"):
        pid = int(data.split("_")[-1])
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"DELETE FROM target_pairs WHERE id = {p}", (pid,))
            c.execute(f"DELETE FROM collected_media WHERE pair_id = {p}", (pid,))
        bot.answer_callback_query(call.id, "Pair Deleted")
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "pairs_main", 'message': call.message, 'id': call.id}))

    elif data == "userbots_main":
        bot.answer_callback_query(call.id)
        sessions = get_userbot_sessions()
        text = "👤 *Userbots Fleet Dashboard*\n\n"
        text += f"Total Registered Userbots: `{len(sessions)}`\n\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        for s in sessions:
            db_id, phone, api_id, api_hash, session_string, u_id, username, first_name, is_active = s
            client = userbot_fleet_manager.get_client(u_id)
            is_connected = client.is_connected() if client else False
            status_emoji = "🟢 Online" if is_connected else "🔴 Inactive/Offline" if not is_active else "🟡 Connected but Stopped"
            markup.add(InlineKeyboardButton(f"👤 {first_name} (@{username or 'No Username'}) - {status_emoji}", callback_data=f"userbot_view_{u_id}"))
            
        markup.add(InlineKeyboardButton("⚙️ Select Fallback Userbots", callback_data="fallback_userbots_menu"))
        
        markup.add(InlineKeyboardButton("🔌 Connect New Userbot", callback_data="user_connect_start"))
        markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data == "fallback_userbots_menu":
        bot.answer_callback_query(call.id)
        sessions = get_userbot_sessions()
        fallback_str = get_setting("fallback_userbot_ids") or ""
        fallback_ids = [int(x) for x in fallback_str.split(",") if x.strip()]
        
        text = "⚙️ *Select Fallback Userbots*\n\n"
        text += "Choose which userbots you want to use as fallback/failover accounts when a rate limit is hit:\n\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        for s in sessions:
            db_id, phone, api_id, api_hash, session_string, u_id, username, first_name, is_active = s
            selected = u_id in fallback_ids
            indicator = "✅" if selected else "❌"
            markup.add(InlineKeyboardButton(f"{indicator} {first_name} (@{username or 'No Username'})", callback_data=f"fallback_userbot_toggle_{u_id}"))
            
        markup.add(InlineKeyboardButton("🔙 Back to Userbots", callback_data="userbots_main"))
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("fallback_userbot_toggle_"):
        target_uid = int(data.split("_")[-1])
        fallback_str = get_setting("fallback_userbot_ids") or ""
        fallback_ids = [int(x) for x in fallback_str.split(",") if x.strip()]
        
        if target_uid in fallback_ids:
            fallback_ids.remove(target_uid)
            msg_text = "Removed from fallback list."
        else:
            fallback_ids.append(target_uid)
            msg_text = "Added to fallback list."
            
        set_setting("fallback_userbot_ids", ",".join(str(x) for x in fallback_ids))
        bot.answer_callback_query(call.id, msg_text)
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': "fallback_userbots_menu", 'message': call.message, 'id': call.id}))

    elif data.startswith("userbot_view_"):
        bot.answer_callback_query(call.id)
        u_id = int(data.split("_")[-1])
        sessions = get_userbot_sessions()
        sess = [s for s in sessions if s[5] == u_id]
        if not sess:
            bot.edit_message_text("❌ Userbot session not found.", call.message.chat.id, call.message.message_id, reply_markup=get_dashboard_markup())
            return
        
        db_id, phone, api_id, api_hash, session_string, user_id, username, first_name, is_active = sess[0]
        client = userbot_fleet_manager.get_client(user_id)
        is_connected = client.is_connected() if client else False
        status_text = "🟢 Connected (Online)" if is_connected else "🔴 Inactive" if not is_active else "🔴 Stopped/Offline"
        
        text = f"👤 **Userbot Account Details**\n\n"
        text += f"🏷️ **First Name:** `{first_name}`\n"
        text += f"🏷️ **Username:** @{username or 'None'}\n"
        text += f"🆔 **User ID:** `{user_id}`\n"
        text += f"📱 **Phone:** `{phone}`\n"
        text += f"API ID: `{api_id}`\n"
        text += f"Status: {status_text}\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📂 Browse Chats", callback_data=f"user_acc_main_{user_id}"))
        
        toggle_label = "🔴 Disable Account" if is_active else "🟢 Enable Account"
        markup.add(InlineKeyboardButton(toggle_label, callback_data=f"userbot_toggle_{user_id}"))
        
        markup.add(InlineKeyboardButton("🗑️ Logout / Remove", callback_data=f"userbot_logout_confirm_{user_id}"))
        markup.add(InlineKeyboardButton("🔙 Back to List", callback_data="userbots_main"))
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("userbot_toggle_"):
        u_id = int(data.split("_")[-1])
        sessions = get_userbot_sessions()
        sess = [s for s in sessions if s[5] == u_id]
        if sess:
            db_id, phone, api_id, api_hash, session_string, user_id, username, first_name, is_active = sess[0]
            new_active = 0 if is_active else 1
            update_userbot_active_status(user_id, new_active)
            if new_active == 1:
                bot.answer_callback_query(call.id, "Starting client...")
                async def start_new():
                    try:
                        await userbot_fleet_manager.start_client(sess[0])
                        bot.send_message(call.message.chat.id, f"✅ Account `{first_name}` successfully enabled and started.")
                    except Exception as e:
                        bot.send_message(call.message.chat.id, f"❌ Failed to start `{first_name}`: {e}")
                asyncio.run_coroutine_threadsafe(start_new(), loop)
            else:
                bot.answer_callback_query(call.id, "Stopping client...")
                async def stop_new():
                    await userbot_fleet_manager.stop_client(user_id)
                    bot.send_message(call.message.chat.id, f"🛑 Account `{first_name}` successfully disabled.")
                asyncio.run_coroutine_threadsafe(stop_new(), loop)
                
        handle_callbacks(type('obj', (object,), {'from_user': call.from_user, 'data': f"userbot_view_{u_id}", 'message': call.message, 'id': call.id}))

    elif data.startswith("userbot_logout_confirm_"):
        bot.answer_callback_query(call.id)
        u_id = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🚨 Yes, Logout", callback_data=f"userbot_logout_do_{u_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"userbot_view_{u_id}")
        )
        bot.edit_message_text("⚠️ *Logout Confirmation*\n\nThis will stop this userbot account and delete its session from the database. Are you sure?", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("userbot_logout_do_"):
        u_id = int(data.split("_")[-1])
        bot.answer_callback_query(call.id, "Logging out...")
        async def do_logout():
            await userbot_fleet_manager.stop_client(u_id)
            delete_userbot_session(u_id)
            bot.send_message(call.message.chat.id, "✅ Userbot session successfully deleted.")
            bot.send_message(call.message.chat.id, get_dashboard_text(), reply_markup=get_dashboard_markup(), parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(do_logout(), loop)

    elif data == "user_logout_do":
        bot.answer_callback_query(call.id, "Logging out all userbots...")
        async def do_logout_all():
            sessions = get_active_userbot_sessions()
            if not sessions:
                sessions = get_userbot_sessions()
            for sess in sessions:
                u_id = sess[5]  # user_id
                if u_id:
                    await userbot_fleet_manager.stop_client(u_id)
                    delete_userbot_session(u_id)
            bot.send_message(call.message.chat.id, "✅ All active userbot sessions successfully deleted.")
            bot.send_message(call.message.chat.id, get_dashboard_text(), reply_markup=get_dashboard_markup(), parse_mode="Markdown")
        asyncio.run_coroutine_threadsafe(do_logout_all(), loop)

    elif data == "user_connect_start":
        bot.answer_callback_query(call.id)
        admin_states[uid] = "awaiting_api_id"
        bot.send_message(call.message.chat.id, "Step 1: Please send your *API ID*.\n(Get it from my.telegram.org)", parse_mode="Markdown")

    elif data.startswith("user_acc_main"):
        parts = data.split("_")
        user_id = int(parts[-1]) if len(parts) > 3 and parts[-1].isdigit() else None
        bot.edit_message_text(
            "👤 *User Account Dashboard*\n\nBrowse and inspect the chats in your account:",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=user_account_markup(user_id),
            parse_mode="Markdown"
        )

    elif data.startswith("user_acc_list_"):
        parts = data.split("_")
        category = parts[3]
        page = int(parts[4])
        user_id = int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else None
        bot.answer_callback_query(call.id, f"Loading {category}...")
        
        async def run_list():
            client = userbot_fleet_manager.get_client(user_id) if user_id else userbot_fleet_manager.get_any_client()
            if not client:
                bot.send_message(call.message.chat.id, "❌ Userbot not running.")
                return
            
            all_dialogs = []
            async for dialog in client.iter_dialogs():
                entity = dialog.entity
                if category == "groups" and isinstance(entity, (types.Chat, types.Channel)) and not getattr(entity, 'broadcast', False):
                    all_dialogs.append(entity)
                elif category == "channels" and isinstance(entity, types.Channel) and entity.broadcast:
                    all_dialogs.append(entity)
                elif category == "bots" and isinstance(entity, types.User) and entity.bot:
                    all_dialogs.append(entity)
                elif category == "private" and isinstance(entity, types.User) and not entity.bot:
                    all_dialogs.append(entity)
            
            page_size = 8
            start = page * page_size
            end = start + page_size
            page_items = all_dialogs[start:end]
            
            markup = InlineKeyboardMarkup(row_width=1)
            suffix = f"_{user_id}" if user_id else ""
            for chat in page_items:
                title = getattr(chat, 'title', None) or getattr(chat, 'first_name', None) or str(chat.id)
                markup.add(InlineKeyboardButton(f"👁 {title}", callback_data=f"user_acc_view_{chat.id}{suffix}"))
            
            nav = []
            if page > 0:
                nav.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"user_acc_list_{category}_{page-1}{suffix}"))
            if end < len(all_dialogs):
                nav.append(InlineKeyboardButton("Next ➡️", callback_data=f"user_acc_list_{category}_{page+1}{suffix}"))
            if nav:
                markup.add(*nav)
            
            markup.add(InlineKeyboardButton("🔙 Back to Categories", callback_data=f"user_acc_main{suffix}"))
            
            msg = f"👤 *Account Browser:* {category.capitalize()}\nPage {page + 1} | Total: {len(all_dialogs)}"
            bot.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        asyncio.run_coroutine_threadsafe(run_list(), loop)

    elif data.startswith("user_acc_view_"):
        parts = data.split("_")
        chat_id = int(parts[3])
        user_id = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else None
        bot.answer_callback_query(call.id, "Fetching details...")
        
        async def run_view():
            client = userbot_fleet_manager.get_client(user_id) if user_id else userbot_fleet_manager.get_any_client()
            if not client: return
            try:
                chat = await resolve_target_id(client, chat_id)
                history = await client.get_messages(chat, limit=0)
                msg_count = history.total
                title = getattr(chat, 'title', None) or getattr(chat, 'first_name', 'Unknown')
                
                info = f"📋 *Chat Details:*\n\n"
                info += f"🏷 *Title:* `{title}`\n"
                info += f"🆔 *ID:* `{chat.id}`\n"
                info += f"📂 *Type:* `{type(chat).__name__}`\n"
                info += f"💬 *Messages:* `{msg_count}`\n"
                
                if hasattr(chat, 'username') and chat.username:
                    info += f"🔗 *Username:* @{chat.username}\n"
                
                markup = InlineKeyboardMarkup(row_width=1)
                suffix = f"_{user_id}" if user_id else ""
                markup.add(InlineKeyboardButton("🔙 Back to List", callback_data=f"user_acc_main{suffix}"))
                
                bot.edit_message_text(info, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

            except Exception as e:
                bot.send_message(call.message.chat.id, f"❌ Error: {e}")

        asyncio.run_coroutine_threadsafe(run_view(), loop)

async def complete_login(uid, client: TelegramClient, chat_id):
    session_string = client.session.save()
    me = await client.get_me()
    phone = login_data[uid]["phone"]
    api_id = login_data[uid]["api_id"]
    api_hash = login_data[uid]["api_hash"]
    
    add_userbot_session(phone, api_id, api_hash, session_string, me.id, me.username, me.first_name)
    
    # Start it in the fleet manager
    sessions = get_userbot_sessions()
    sess = [s for s in sessions if s[5] == me.id]
    if sess:
        await userbot_fleet_manager.start_client(sess[0])
        
    admin_states.pop(uid, None)
    login_data.pop(uid, None)
    
    bot.send_message(chat_id, f"✅ *Userbot Connected (Telethon)!*\nAccount: `{me.first_name}` (@{me.username or 'No Username'})", parse_mode="Markdown")
    bot.send_message(chat_id, get_dashboard_text(), reply_markup=get_dashboard_markup(), parse_mode="Markdown")

@bot.message_handler(func=lambda m: is_authorized_manager(m.from_user.id) and admin_states.get(m.from_user.id))
def handle_state_inputs(message):
    uid = message.from_user.id
    state = admin_states.get(uid)
    text = message.text.strip()
    
    # --- Ban List System ---
    if state == "awaiting_ban_target":
        target = text.replace("@", "")
        b_uid, b_uname = None, None
        if target.isdigit(): b_uid = int(target)
        else: b_uname = target
        
        ban_user(user_id=b_uid, username=b_uname)
        admin_states.pop(uid, None)
        bot.reply_to(message, f"✅ *User Banned:* `{target}`\nTheir messages will no longer be processed.", parse_mode="Markdown")
        bot.send_message(message.chat.id, "🚫 *Banned Users*", reply_markup=banlist_markup(), parse_mode="Markdown")

    elif state.startswith("awaiting_new_topic_name_"):
        chat_id = int(state.replace("awaiting_new_topic_name_", ""))
        topic_title = text
        admin_states.pop(uid, None)
        
        bot.send_message(message.chat.id, f"⏳ **Creating topic '{topic_title}' in source group...**")
        
        async def create_and_select():
            try:
                from telethon.tl.functions.channels import CreateForumTopicRequest
                entity = await resolve_target_id(userbot, chat_id)
                result = await userbot(CreateForumTopicRequest(
                    channel=entity,
                    title=topic_title
                ))
                
                topic_id = None
                if hasattr(result, 'updates'):
                    for u in result.updates:
                        if hasattr(u, 'message') and u.message:
                            topic_id = u.message.id
                            break
                            
                if not topic_id and hasattr(result, 'updates'):
                    from telethon.tl.types import UpdateMessageID
                    for u in result.updates:
                        if isinstance(u, UpdateMessageID):
                            topic_id = u.id
                            break
                            
                if not topic_id:
                    bot.send_message(message.chat.id, "❌ **Failed to retrieve created topic ID.** Please select manually.")
                    return
                    
                if uid not in login_data:
                    login_data[uid] = {}
                login_data[uid]["source_id"] = chat_id
                login_data[uid]["source_topic_id"] = topic_id
                
                bot.send_message(message.chat.id, f"✅ **Topic '{topic_title}' Created and Selected!** (ID: `{topic_id}`)")
                
                if login_data[uid].get("preselected_flow") == "target":
                    tid = login_data[uid]["target_id"]
                    ttid = login_data[uid]["target_topic_id"]
                    t_chat = await resolve_target_id(userbot, tid)
                    s_title = getattr(entity, 'title', None) or getattr(entity, 'first_name', None) or str(chat_id)
                    t_title = getattr(t_chat, 'title', None) or getattr(t_chat, 'first_name', None) or str(tid)
                    
                    add_target_pair(chat_id, topic_id, tid, ttid, s_title, t_title)
                    
                    success_text = f"✅ *Pair Added!*\n\n"
                    success_text += f"Source: `{s_title}` (Topic: `{topic_id}`)\n"
                    success_text += f"Target: `{t_title}`" + (f" (Topic: `{ttid}`)" if ttid else "")
                    
                    bot.send_message(message.chat.id, success_text, parse_mode="Markdown")
                    bot.send_message(message.chat.id, "🎯 *Target Pairs*", reply_markup=pairs_list_markup())
                    login_data.pop(uid, None)
                else:
                    markup = await get_chat_selection_markup("sel_tgt", 0)
                    bot.send_message(message.chat.id, "🎯 *Select Target Chat*\nChoose the group or channel to send to:", reply_markup=markup, parse_mode="Markdown")
            except Exception as e:
                logger.error(f"Error creating topic: {e}")
                bot.send_message(message.chat.id, f"❌ **Failed to create topic:** {e}\n(Make sure the userbot is admin with 'Manage Topics' permission)")
                
        asyncio.run_coroutine_threadsafe(create_and_select(), loop)

    # --- Logging System ---
    # --- Log Bot System ---
    if state == "awaiting_log_bot_token":
        token = text
        admin_states.pop(uid, None)
        bot.send_message(message.chat.id, "⏳ Verifying Log Bot Token...")
        
        try:
            temp_bot = telebot.TeleBot(token, threaded=False)
            bot_info = temp_bot.get_me()
            
            # Save to DB
            add_log_bot(token, bot_info.username, bot_info.id)
            
            # Start in fleet
            log_bot_manager.add_bot(token)
            
            bot.send_message(message.chat.id, f"✅ *Log Bot Added!*\nUsername: @{bot_info.username}\nID: `{bot_info.id}`", parse_mode="Markdown")
            bot.send_message(message.chat.id, "📜 *Log Bot System*", reply_markup=log_bot_list_markup(), parse_mode="Markdown")
        except Exception as e:
            bot.send_message(message.chat.id, f"❌ Failed to verify Bot Token: {e}")
            
    # --- Login Flow ---
    elif state == "awaiting_api_id":
        if not text.isdigit():
            bot.reply_to(message, "Invalid API ID. Please send a numeric ID.")
            return
        api_id_val = int(text)
        if api_id_val > 2147483647 or api_id_val < 0:
            bot.reply_to(
                message, 
                "⚠️ **Invalid API ID!**\n\n"
                "The number you entered exceeds the 32-bit integer limit (2,147,483,647). This usually happens if you accidentally enter:\n"
                "• Your **Telegram User ID** (e.g., `8881447083`)\n"
                "• Your **Phone Number**\n"
                "• A **Bot Token** prefix\n\n"
                "Please get your actual **API ID** (which is a 7 to 8 digit number, e.g., `28956432`) from [my.telegram.org](https://my.telegram.org/apps) and send it again.",
                parse_mode="Markdown",
                disable_web_page_preview=True
            )
            return
        login_data[uid] = {"api_id": api_id_val}
        admin_states[uid] = "awaiting_api_hash"
        bot.send_message(message.chat.id, "Step 2: Please send your *API HASH*.", parse_mode="Markdown")

    elif state == "awaiting_api_hash":
        if len(text) < 10:
            bot.reply_to(message, "Invalid API HASH.")
            return
        login_data[uid]["api_hash"] = text
        admin_states[uid] = "awaiting_phone"
        bot.send_message(message.chat.id, "Step 3: Please send your *Phone Number* (with country code).\nExample: `+1234567890`", parse_mode="Markdown")

    elif state == "awaiting_phone":
        login_data[uid]["phone"] = text
        bot.send_message(message.chat.id, "⏳ Sending OTP (Telethon)...")
        async def send_otp_task():
            try:
                temp_client = TelegramClient(StringSession(), login_data[uid]["api_id"], login_data[uid]["api_hash"])
                await temp_client.connect()
                send_code = await temp_client.send_code_request(login_data[uid]["phone"])
                login_data[uid]["client"] = temp_client
                login_data[uid]["phone_code_hash"] = send_code.phone_code_hash
                admin_states[uid] = "awaiting_otp"
                bot.send_message(message.chat.id, "Step 4: Please send the *OTP* you received.", parse_mode="Markdown")
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Error: {e}")
                admin_states.pop(uid, None)
        asyncio.run_coroutine_threadsafe(send_otp_task(), loop)

    elif state == "awaiting_otp":
        otp = text.replace(" ", "")
        bot.send_message(message.chat.id, "⏳ Verifying OTP...")
        async def verify_otp_task():
            client = login_data[uid].get("client")
            try:
                await client.sign_in(phone=login_data[uid]["phone"], code=otp, phone_code_hash=login_data[uid]["phone_code_hash"])
                bot.send_message(message.chat.id, "✅ OTP Verified!")
                await complete_login(uid, client, message.chat.id)
            except errors.SessionPasswordNeededError:
                admin_states[uid] = "awaiting_password"
                bot.send_message(message.chat.id, "🔐 Step 5: Please send your *Cloud Password*.", parse_mode="Markdown")
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ OTP Error: {e}")
                admin_states.pop(uid, None)
        asyncio.run_coroutine_threadsafe(verify_otp_task(), loop)

    elif state == "awaiting_password":
        async def verify_password_task():
            client = login_data[uid].get("client")
            try:
                await client.sign_in(password=text)
                await complete_login(uid, client, message.chat.id)
            except Exception as e:
                bot.send_message(message.chat.id, f"❌ Password error: {e}")
                admin_states.pop(uid, None)
        asyncio.run_coroutine_threadsafe(verify_password_task(), loop)

    elif state.startswith("rel_src_setup_interval_"):
        # Format: rel_src_setup_interval_{source_type}_{pid}_{filter}
        parts = state.split("_")
        source_type = parts[4]
        pid = int(parts[5])
        release_filter = "everything"
        if len(parts) > 6:
            release_filter = parts[6]
            
        if not text.isdigit():
            bot.reply_to(message, "Please send a valid number.")
            return
        interval = int(text)
        admin_states.pop(uid, None)
        
        m_names = {"monitor": "Monitoring", "scraper": "History Scraper", "collection": "Collect Now"}
        display_name = m_names.get(source_type, "Vault Items")
        
        f_names = {"everything": "All Content 🔄", "media": "Media Only 🖼️", "text": "Text Only 📝"}
        display_filter = f_names.get(release_filter, "All Content 🔄")
        
        bot.send_message(
            message.chat.id, 
            f"⏳ **Slow Release Initiated**\n\n"
            f"🎯 **Target Pair ID:** `{pid}`\n"
            f"📥 **Collection Source:** `{display_name}`\n"
            f"🎯 **Release Filter:** `{display_filter}`\n"
            f"⏰ **Release Interval:** `{interval}s` between items\n\n"
            f"🚀 *Engine running in background...*",
            parse_mode="Markdown"
        )
        asyncio.run_coroutine_threadsafe(run_release(message.chat.id, pid, added_by=source_type, interval=interval, release_filter=release_filter), loop)
    elif state.startswith("hist_setup_count_only_"):
        pid = int(state.split("_")[-1])
        if not text.isdigit():
            bot.reply_to(message, "⚠️ Please send a valid number.")
            return
        count = int(text)
        admin_states.pop(uid)
        bot.send_message(message.chat.id, f"🔢 *Count-Based Scrape*\n\n🎯 Pair ID: `{pid}`\n📥 Limit: `{count}` messages\n\n🚀 *Initializing engine...*", parse_mode="Markdown")
async def run_history_scrape(admin_chat_id, pair_id, limit=None, start_date=None, end_date=None):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return

    task_key = f"hist_{pair_id}"
    running_tasks[task_key] = True
    history_options[task_key] = {
        "s_title": "Unknown Source",
        "scanned": 0,
        "collected": 0,
        "sent_count": 0,
        "limit": limit
    }
    
    pair = get_target_pair(pair_id)
    if not pair: return
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf = pair
    history_options[task_key]["s_title"] = s_title
    
    collected = 0
    scanned = 0
    sent_count = 0
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("🛑 Stop Scrape", callback_data=f"pair_stop_task_hist_{pair_id}"))
    status_msg = bot.send_message(admin_chat_id, f"📜 *History Scrape: `{s_title}`*\n\n🔍 Scanned: `0`\n📥 Collected: `0`\n📤 Sent: `0`", reply_markup=markup, parse_mode="Markdown")
    
    try:
        client, target_chat = await resolve_target_across_fleet(sid)
        if not client:
            client = userbot
            target_chat = await resolve_target_id(client, sid)
        userbot = client
        
        target_topic = None
        if s_topic and str(s_topic).strip().lower() not in ["", "0", "none"]:
            try:
                target_topic = int(s_topic)
            except Exception:
                pass
        
        collected_messages = []
        
        offset_id = 0
        chunk_size = 100
        
        while True:
            if not running_tasks.get(task_key):
                break
                
            async with userbot_lock:
                chunk = await userbot.get_messages(
                    target_chat,
                    limit=chunk_size,
                    min_id=offset_id,
                    reply_to=target_topic,
                    reverse=True
                )
            
            if not chunk:
                break
                
            for m in chunk:
                scanned += 1
                
                if isinstance(m, types.MessageService):
                    continue
                
                if start_date and m.date < start_date:
                    continue
                if end_date and m.date > end_date:
                    break
 
                sender_id = m.sender_id
                sender_username = getattr(m.sender, 'username', None)
                if is_user_banned(sender_id, sender_username):
                    continue

                cf_val = cf or "everything"
                if contains_link(m.message, m.entities):
                    pass
                else:
                    m_type = get_specific_media_type(m.media)
                    if cf_val == "media" and m_type not in ["photo", "video"]:
                        continue
                    if cf_val == "text" and m_type != "text":
                        continue
                    if cf_val == "file" and m_type != "file":
                        continue

                if task_key in history_options:
                    history_options[task_key].update({
                        "scanned": scanned,
                        "collected": collected,
                        "sent_count": sent_count
                    })

                collected_messages.append(m)
                collected += 1
                
                if limit and collected >= limit:
                    break
            
            if limit and collected >= limit:
                break
            if end_date and chunk[-1].date > end_date:
                break
                
            offset_id = chunk[-1].id
            
            if scanned % 50 == 0:
                l_text = f" / {limit}" if limit else ""
                try:
                    bot.edit_message_text(
                        f"📜 *History Scrape: `{s_title}`*\n\n🔍 Scanned: `{scanned}`\n📥 Collected: `{collected}{l_text}`\n📤 Sent: `{sent_count}`",
                        admin_chat_id,
                        status_msg.message_id,
                        reply_markup=markup,
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
            await asyncio.sleep(1.0)
            
        if not running_tasks.get(task_key):
            bot.send_message(admin_chat_id, f"🛑 History scrape for `{s_title}` stopped by user.")
            return
        
        grouped_batches = []
        temp_group = []
        for m in collected_messages:
            if m.grouped_id:
                if not temp_group:
                    temp_group.append(m)
                elif temp_group[0].grouped_id == m.grouped_id:
                    temp_group.append(m)
                else:
                    grouped_batches.append(temp_group)
                    temp_group = [m]
            else:
                if temp_group:
                    grouped_batches.append(temp_group)
                    temp_group = []
                grouped_batches.append([m])
        if temp_group:
            grouped_batches.append(temp_group)

        is_protected_flow = getattr(target_chat, 'noforwards', False)
        if is_protected_flow:
            try:
                from telethon.tl.functions.channels import GetChannelsRequest
                async with userbot_lock:
                    res = await userbot(GetChannelsRequest(id=[target_chat]))
                if res and res.chats:
                    target_chat = res.chats[0]
                    is_protected_flow = getattr(target_chat, 'noforwards', False)
                    update_telethon_entity_cache(userbot, target_chat)
            except Exception:
                pass

        for batch in grouped_batches:
            if not running_tasks.get(task_key):
                bot.send_message(admin_chat_id, f"🛑 History scrape forwarding stopped by user.")
                break

            media_to_file = {}
            if is_protected_flow:
                for msg in batch:
                    if msg.media:
                        try:
                            async with userbot_lock:
                                async with media_semaphore:
                                    path = await userbot.download_media(msg)
                            if path:
                                media_to_file[msg.id] = path
                        except errors.FloodWaitError as fwe:
                            logger.warning(f"⏳ SCRAPE FLOOD: Download media flood wait of {fwe.seconds}s required. Skipping media.")
                            await notify_admin_flood_wait(userbot, "Scrape Download", fwe.seconds)
                            await notify_admin_flood_wait(userbot, "Scrape Download", fwe.seconds)
                            if fwe.seconds <= 5:
                                await asyncio.sleep(fwe.seconds)
                                try:
                                    async with userbot_lock:
                                        async with media_semaphore:
                                            path = await userbot.download_media(msg)
                                    if path: media_to_file[msg.id] = path
                                except Exception as e2:
                                    logger.error(f"Failed to download media after short flood wait: {e2}")
                        except Exception as e:
                            logger.error(f"Failed to download media for message {msg.id}: {e}")

            try:
                has_media = any(msg.media for msg in batch)
                if is_protected_flow:
                    if has_media and not any(msg.id in media_to_file for msg in batch):
                        logger.warning("🛡️ SCRAPE: Skipping mirror because media download failed/skipped.")
                    else:
                        async with userbot_lock:
                            await send_mirrored_content(userbot, tid, batch, t_topic, is_mir, sid, pre_downloaded=media_to_file if (is_protected_flow and has_media) else None)
                else:
                    try:
                        async with userbot_lock:
                            src_peer = await userbot.get_input_entity(int(sid))
                            tgt_peer = await userbot.get_input_entity(int(tid))
                            dest_topic_id = t_topic
                            if is_mir:
                                first_msg = batch[0]
                                s_top = getattr(first_msg.reply_to, 'reply_to_top_id', None) or (first_msg.reply_to.reply_to_msg_id if first_msg.reply_to else None)
                                if not s_top and getattr(first_msg, 'forum_topic', False):
                                    s_top = first_msg.id
                                if not s_top and first_msg.reply_to_msg_id:
                                    s_top = first_msg.reply_to_msg_id
                                if s_top:
                                    mapped_topic = get_topic_mapping(sid, s_top, tid)
                                    if mapped_topic:
                                        dest_topic_id = mapped_topic
                                    else:
                                        forum = getattr(first_msg.reply_to, "forum_topic", None)
                                        src_title = getattr(forum, "title", None)
                                        src_icon = getattr(forum, "icon_emoji_id", None)
                                        if not src_title:
                                            try:
                                                res = await userbot(functions.messages.GetForumTopicsRequest(
                                                    peer=src_peer, offset_date=0, offset_id=0, offset_topic=0, limit=100
                                                ))
                                                for t in res.topics:
                                                    if t.id == s_top:
                                                        src_title = t.title
                                                        src_icon = getattr(t, "icon_emoji_id", None)
                                                        break
                                            except Exception: pass
                                        if src_title:
                                            dest_topic_id = await get_or_create_target_topic(userbot, tid, src_title, sid, s_top, icon_emoji_id=src_icon)

                            import random
                            random_ids = [random.randint(-9223372036854775808, 9223372036854775807) for _ in batch]
                            target_entity = await resolve_target_id(userbot, tid)
                            is_forum = getattr(target_entity, 'forum', False) if not isinstance(target_entity, int) else False
                            top_msg_id_val = int(dest_topic_id) if (is_forum and dest_topic_id) else None
                            
                            fwd_res = await userbot(functions.messages.ForwardMessagesRequest(
                                from_peer=src_peer,
                                id=[msg.id for msg in batch],
                                to_peer=target_entity,
                                random_id=random_ids,
                                top_msg_id=top_msg_id_val
                            ))
                            if fwd_res:
                                fwd_msgs = []
                                if hasattr(fwd_res, 'updates'):
                                    for u in fwd_res.updates:
                                        if type(u).__name__ in ["UpdateNewMessage", "UpdateNewChannelMessage"]:
                                            fwd_msgs.append(u.message)
                                if len(fwd_msgs) == len(batch):
                                    for orig_m, fwd_m in zip(batch, fwd_msgs):
                                        save_message_mapping(sid, orig_m.id, tid, fwd_m.id)
                    except Exception as fwd_err:
                        logger.error(f"Native Forward in history scrape failed ({fwd_err}). Falling back to mirror...")
                        async with userbot_lock:
                            await send_mirrored_content(userbot, tid, batch, t_topic, is_mir, sid)
                
                sent_count += len(batch)
                
                if is_mon:
                    with db_conn() as conn:
                        c = conn.cursor()
                        for m in batch:
                            if m.media:
                                m_type = type(m.media).__name__
                                if USING_POSTGRES:
                                    c.execute(
                                        """
                                        INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption, added_by, released) 
                                        VALUES (%s, %s, %s, %s, %s, 'scraper', 1) 
                                        ON CONFLICT (source_chat_id, source_message_id) 
                                        DO UPDATE SET 
                                            pair_id = EXCLUDED.pair_id,
                                            media_type = EXCLUDED.media_type,
                                            caption = EXCLUDED.caption,
                                            added_by = EXCLUDED.added_by,
                                            released = EXCLUDED.released,
                                            timestamp = CURRENT_TIMESTAMP
                                        """,
                                        (pair_id, sid, m.id, m_type, m.message or "")
                                    )
                                else:
                                    c.execute(
                                        """
                                        INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption, added_by, released) 
                                        VALUES (?, ?, ?, ?, ?, 'scraper', 1) 
                                        ON CONFLICT (source_chat_id, source_message_id) 
                                        DO UPDATE SET 
                                            pair_id = excluded.pair_id,
                                            media_type = excluded.media_type,
                                            caption = excluded.caption,
                                            added_by = excluded.added_by,
                                            released = excluded.released,
                                            timestamp = datetime('now')
                                        """,
                                        (pair_id, sid, m.id, m_type, m.message or "")
                                    )
                        if not USING_POSTGRES or not DATABASE_URL:
                            conn.commit()
                    
                    if is_protected_flow:
                        files_to_vault = [media_to_file.get(m.id) for m in batch if m.id in media_to_file]
                        if files_to_vault:
                            file_payload = files_to_vault if len(files_to_vault) > 1 else files_to_vault[0]
                            for token, username, bot_id in get_log_bots():
                                metadata = f"SID: {sid} | MID: {batch[0].id}\n"
                                caption_text = metadata + (batch[0].message or "")
                                try:
                                    async with userbot_lock:
                                        vaulted_result = await userbot.send_message(
                                            entity=int(bot_id),
                                            file=file_payload,
                                            message=caption_text
                                        )
                                    if vaulted_result:
                                        v_msgs = vaulted_result if isinstance(vaulted_result, list) else [vaulted_result]
                                        for i, v_m in enumerate(v_msgs):
                                            orig_m = batch[i]
                                            save_logged_media(
                                                bot_id=int(bot_id),
                                                log_msg_id=int(v_m.id),
                                                source_chat_id=int(sid),
                                                source_msg_id=int(orig_m.id),
                                                file_id=None,
                                                media_type=type(orig_m.media).__name__ if orig_m.media else "text",
                                                caption=orig_m.message or "",
                                                grouped_id=orig_m.grouped_id
                                            )
                                except Exception as e:
                                    logger.error(f"Error vaulting pre-downloaded media to bot {bot_id}: {e}")
                    else:
                        await forward_to_log_bots(userbot, batch, sid)
            finally:
                for temp_path in media_to_file.values():
                    if os.path.exists(temp_path):
                        try: os.remove(temp_path)
                        except Exception: pass
                
            if task_key in history_options:
                history_options[task_key].update({
                    "scanned": scanned,
                    "collected": collected,
                    "sent_count": sent_count
                })

            l_text = f" / {limit}" if limit else ""
            try:
                bot.edit_message_text(
                    f"📜 *History Scrape: `{s_title}`*\n\n🔍 Scanned: `{scanned}`\n📥 Collected: `{collected}{l_text}`\n📤 Sent: `{sent_count}`",
                    admin_chat_id,
                    status_msg.message_id,
                    reply_markup=markup,
                    parse_mode="Markdown"
                )
            except Exception:
                pass
                
            await asyncio.sleep(0.5)

        bot.send_message(admin_chat_id, f"✅ History Scrape Done: `{s_title}`\nScanned: `{scanned}`\nCollected: `{collected}`\nSent to Target: `{sent_count}`")
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Scrape Error: {e}")
    finally:
        running_tasks.pop(task_key, None)
        history_options.pop(task_key, None)

async def resolve_target_id(client: TelegramClient, target_ref):
    from telethon.tl.types import PeerChannel, PeerChat, PeerUser
    
    # Try resolving target_ref directly
    try:
        return await client.get_entity(target_ref)
    except Exception as e:
        logger.warning(f"Initial get_entity failed for {target_ref}: {e}")

    ref_str = str(target_ref).strip()
    
    # Check if target_ref is a username or invite link
    if not ref_str.replace("-", "").isdigit():
        # If it's a link, parse it
        parsed = parse_telegram_link(ref_str)
        if parsed:
            if parsed["type"] == "username":
                try:
                    return await client.get_entity(parsed["username"])
                except Exception:
                    pass
        else:
            # Try as username directly
            try:
                return await client.get_entity(ref_str)
            except Exception:
                pass

    # If it is numeric (or looks like an ID)
    clean_id = ref_str.replace("-100", "").replace("-", "")
    if clean_id.isdigit():
        clean_id_int = int(clean_id)
        
        # Candidates to try:
        # 1. PeerChannel(clean_id_int) -> standard channel
        # 2. PeerChat(clean_id_int) -> standard chat
        # 3. PeerUser(clean_id_int) -> standard user
        # 4. int(f"-100{clean_id}")
        # 5. -clean_id_int
        # 6. clean_id_int
        candidates = [
            PeerChannel(clean_id_int),
            PeerChat(clean_id_int),
            PeerUser(clean_id_int),
            int(f"-100{clean_id}"),
            -clean_id_int,
            clean_id_int
        ]
        
        # First attempt: check if we can resolve any candidate from existing cache
        for candidate in candidates:
            try:
                return await client.get_entity(candidate)
            except Exception:
                pass
                
        # Second attempt: fetch dialogs from network to refresh entity cache
        try:
            logger.info("Target entity not found in cache. Refreshing dialogs from Telegram network...")
            await client.get_dialogs()
            
            # Retry candidates after refreshing cache
            for candidate in candidates:
                try:
                    return await client.get_entity(candidate)
                except Exception:
                    pass
        except Exception as ex:
            logger.error(f"Failed to refresh dialogs: {ex}")
            
        # Third attempt: search dialogs list manually
        try:
            async for dialog in client.iter_dialogs(limit=200):
                d_id_str = str(dialog.id).replace("-100", "").replace("-", "")
                if d_id_str == clean_id:
                    return dialog.entity
        except Exception as ex:
            logger.error(f"iter_dialogs failed during resolution: {ex}")

    raise ValueError(f"Could not find or access chat: {target_ref}")

async def resolve_target_across_fleet(target_ref):
    for client in userbot_fleet_manager.get_all_clients():
        try:
            entity = await resolve_target_id(client, target_ref)
            if entity:
                return client, entity
        except Exception:
            continue
    return None, None

async def run_collection_preview(admin_chat_id, message_id, pair_id):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return
        
    row = get_target_pair(pair_id)
    if not row:
        bot.send_message(admin_chat_id, "❌ Target pair not found.")
        return
        
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf = row
    
    # Edit message to show Scanning status
    try:
        bot.edit_message_text(
            f"🔍 <b>Scanning Group/Channel</b>\n\n<b>Source:</b> {s_title}\n\nPlease wait while we analyze the message statistics...",
            admin_chat_id,
            message_id,
            parse_mode="HTML"
        )
    except Exception:
        pass

    try:
        client, source_chat = await resolve_target_across_fleet(sid)
        if not client:
            client = userbot
            source_chat = await resolve_target_id(client, sid)
        userbot = client
        
        target_topic = None
        if s_topic and str(s_topic).strip().lower() not in ["", "0", "none"]:
            try:
                target_topic = int(s_topic)
            except Exception:
                pass

        total_count = 0
        photo_count = 0
        video_count = 0
        file_count = 0
        text_count = 0
        
        # We fetch up to 1000 messages or use limit/get_messages to count types quickly
        async with userbot_lock:
            # Let's get total count first
            total_msg = await userbot.get_messages(source_chat, limit=0, reply_to=target_topic)
            total_count = total_msg.total
            
            # Fast scan of the last 1000 messages to estimate type distribution
            scan_limit = min(total_count, 1000)
            if scan_limit > 0:
                try:
                    messages = await userbot.get_messages(source_chat, limit=scan_limit, reply_to=target_topic)
                    for m in messages:
                        m_type = get_specific_media_type(m.media)
                        if m_type == "photo":
                            photo_count += 1
                        elif m_type == "video":
                            video_count += 1
                        elif m_type == "file":
                            file_count += 1
                        else:
                            text_count += 1
                except errors.FloodWaitError as fwe:
                    logger.warning(f"⏳ PREVIEW RATE LIMIT: Hit {fwe.seconds}s wait. Using partial metrics.")
                    if scan_limit <= 1: scan_limit = 100
                        
        # Scaling counts if total_count > 1000
        if total_count > 1000:
            scale = total_count / 1000.0
            est_photo = int(photo_count * scale)
            est_video = int(video_count * scale)
            est_file = int(file_count * scale)
            est_text = int(text_count * scale)
            desc_suffix = f"\n<i>(Distribution estimated based on a sample of 1,000 messages)</i>"
        else:
            est_photo = photo_count
            est_video = video_count
            est_file = file_count
            est_text = text_count
            desc_suffix = ""

        # Estimates on duration: Telethon with semaphores has ~1-2 messages per second for media, text is faster.
        # Let's assume an average speed of 3 messages/second for estimation.
        est_seconds = int(total_count / 3) + 5
        hours = est_seconds // 3600
        minutes = (est_seconds % 3600) // 60
        seconds = est_seconds % 60
        
        time_str = ""
        if hours > 0:
            time_str += f"{hours}h "
        if minutes > 0 or hours > 0:
            time_str += f"{minutes}m "
        time_str += f"{seconds}s"

        preview_text = (
            f"📋 <b>Collection Preview & Scan Result</b>\n\n"
            f"<b>Source Chat:</b> {s_title}\n"
            f"<b>Target Chat:</b> {t_title}\n"
            f"<b>Total Messages in Source:</b> {total_count}\n"
            f"<b>Filter Configured:</b> {cf or 'everything'}\n"
            f"<b>Estimated Duration:</b> ~{time_str}\n\n"
            f"📊 <b>Estimated Content Breakdown:</b>\n"
            f"🖼️ Photos: {est_photo}\n"
            f"🎥 Videos: {est_video}\n"
            f"📁 Files/Docs: {est_file}\n"
            f"📝 Text Only: {est_text}\n"
            f"{desc_suffix}\n\n"
            f"Would you like to start the collection process?"
        )
        
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("🚀 Start Collection", callback_data=f"pair_collect_confirm_{pair_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"pair_collect_cancel_{pair_id}")
        )
        
        try:
            bot.edit_message_text(
                preview_text,
                admin_chat_id,
                message_id,
                reply_markup=markup,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Failed to edit preview message: {e}")
            try:
                bot.delete_message(admin_chat_id, message_id)
            except Exception as del_err:
                logger.error(f"Failed to delete old message during preview edit fallback: {del_err}")
            bot.send_message(admin_chat_id, preview_text, reply_markup=markup, parse_mode="HTML")
            
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        # fallback to starting collection directly or showing error
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🔙 Back to Pair", callback_data=f"pair_view_{pair_id}"))
        try:
            bot.edit_message_text(
                f"❌ <b>Scan Failed</b>\n\nAn error occurred while scanning: {e}\n\nYou can try again or check userbot connection.",
                admin_chat_id,
                message_id,
                reply_markup=markup,
                parse_mode="HTML"
            )
        except Exception:
            pass

async def run_collection(admin_chat_id, pair_id, limit=None):
    logger.info(f"RUN_COLLECTION ENTERED: {pair_id}")
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        logger.error(f"Userbot error during run_collection ensure_userbot(): {msg}")
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return
        
    task_key = f"coll_{pair_id}"
    running_tasks[task_key] = True
        
    row = get_target_pair(pair_id)
    if not row:
        logger.error(f"Target pair {pair_id} not found in database.")
        return
    pid, sid, tid, s_title, t_title, is_mon, is_live, is_mir, s_topic, t_topic, cf = row
    collected = 0
    scanned = 0
    sent_count = 0
    
    default_cf = cf or "everything"
    if default_cf not in ["everything", "media", "text", "file"]:
        default_cf = "everything"
        
    # DIRECT ASSIGNMENT (Do not use .clear() to prevent reading empty dictionaries during async switches)
    existing_opts = collection_options.get(task_key, {})
    collection_options[task_key] = {
        "instant_release": existing_opts.get("instant_release", bool(is_live)),
        "instant_filter": existing_opts.get("instant_filter", "everything"),
        "collect_filter": existing_opts.get("collect_filter", default_cf),
        "s_title": s_title,
        "scanned": existing_opts.get("scanned", 0),
        "collected": existing_opts.get("collected", 0),
        "duplicates": existing_opts.get("duplicates", 0),
        "deleted": existing_opts.get("deleted", 0),
        "skipped": existing_opts.get("skipped", 0),
        "filtered": existing_opts.get("filtered", 0),
        "limit": limit,
        "sent_count": existing_opts.get("sent_count", 0),
        "progress": existing_opts.get("progress", 0),
        "status": "Fetching"
    }
    
    logger.info("Sending status message")
    status_msg = bot.send_message(
        admin_chat_id, 
        get_collection_status_text(task_key), 
        reply_markup=get_collection_markup(pair_id), 
        parse_mode="HTML"
    )
    
    try:
        logger.info("Resolving source chat")
        source_chat = await resolve_target_id(userbot, sid)
        logger.info("Resolving target chat")
        dest_chat = await resolve_target_id(userbot, tid)
        
        target_topic = None
        if s_topic and str(s_topic).strip().lower() not in ["", "0", "none"]:
            try:
                target_topic = int(s_topic)
            except Exception:
                pass
        
        total_count = 0
        try:
            async with userbot_lock:
                total_msg = await userbot.get_messages(source_chat, limit=0, reply_to=target_topic)
                total_count = total_msg.total
        except Exception as e:
            logger.warning(f"Could not get total message count: {e}")
            
        total_to_fetch = min(limit, total_count) if limit and total_count else (total_count or limit or 1)
        if total_to_fetch <= 0:
            total_to_fetch = 1

        opts = collection_options[task_key]
        opts["status"] = "Fetching"
        
        offset_id = 0
        chunk_size = 100
        to_fetch_remain = limit if limit else total_count
        
        auto_mirror = is_mir
        is_protected_flow = getattr(source_chat, 'noforwards', False)
        if is_protected_flow:
            try:
                from telethon.tl.functions.channels import GetChannelsRequest
                async with userbot_lock:
                    res = await userbot(GetChannelsRequest(id=[source_chat]))
                if res and res.chats:
                    source_chat = res.chats[0]
                    is_protected_flow = getattr(source_chat, 'noforwards', False)
                    update_telethon_entity_cache(userbot, source_chat)
            except Exception:
                pass

        while to_fetch_remain is None or to_fetch_remain > 0:
            if not running_tasks.get(task_key):
                break
                
            cur_limit = chunk_size if to_fetch_remain is None else min(chunk_size, to_fetch_remain)
            if cur_limit <= 0:
                break
                
            async with userbot_lock:
                chunk = await userbot.get_messages(
                    source_chat,
                    limit=cur_limit,
                    min_id=offset_id,
                    reply_to=target_topic,
                    reverse=True
                )
                
            if not chunk:
                break
                
            chunk_valid = []
            for m in chunk:
                scanned += 1
                progress = int((scanned / total_to_fetch) * 100)
                if progress > 100: progress = 100
                
                if isinstance(m, types.MessageService):
                    opts["filtered"] += 1
                    opts.update({"scanned": scanned, "progress": progress})
                    continue
                
                sender_id = m.sender_id
                sender_username = getattr(m.sender, 'username', None)
                if is_user_banned(sender_id, sender_username):
                    opts["filtered"] += 1
                    opts.update({"scanned": scanned, "progress": progress})
                    continue
                
                cf_val = opts.get("collect_filter", "everything")
                if contains_link(m.message, m.entities):
                    pass
                else:
                    m_type = get_specific_media_type(m.media)
                    if cf_val == "media" and m_type not in ["photo", "video"]:
                        opts["filtered"] += 1
                        opts.update({"scanned": scanned, "progress": progress})
                        continue
                    if cf_val == "text" and m_type != "text":
                        opts["filtered"] += 1
                        opts.update({"scanned": scanned, "progress": progress})
                        continue
                    if cf_val == "file" and m_type != "file":
                        opts["filtered"] += 1
                        opts.update({"scanned": scanned, "progress": progress})
                        continue
                
                is_dup = False
                try:
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"SELECT 1 FROM collected_media WHERE source_chat_id = {p} AND source_message_id = {p}", (sid, m.id))
                        is_dup = c.fetchone() is not None
                except Exception as dbe:
                    logger.error(f"Error checking duplicate: {dbe}")
                    
                if is_dup:
                    opts["duplicates"] += 1
                    opts.update({"scanned": scanned, "progress": progress})
                    continue
                    
                chunk_valid.append(m)
                collected += 1
                
                opts.update({
                    "scanned": scanned,
                    "collected": collected,
                    "progress": progress
                })

            if chunk_valid:
                chunk_batches = []
                temp_group = []
                for m in chunk_valid:
                    if m.grouped_id:
                        if not temp_group:
                            temp_group.append(m)
                        elif temp_group[0].grouped_id == m.grouped_id:
                            temp_group.append(m)
                        else:
                            chunk_batches.append(temp_group)
                            temp_group = [m]
                    else:
                        if temp_group:
                            chunk_batches.append(temp_group)
                            temp_group = []
                        chunk_batches.append([m])
                if temp_group:
                    chunk_batches.append(temp_group)

                for batch in chunk_batches:
                    is_task_active = running_tasks.get(task_key)
                    curr_instant = opts.get("instant_release", False) and is_task_active
                    instant_filter = opts.get("instant_filter", "everything")

                    matching_batch = []
                    for msg in batch:
                        matches = True
                        msg_type = get_specific_media_type(msg.media)
                        if instant_filter == "media" and msg_type not in ["photo", "video"]:
                            matches = False
                        elif instant_filter == "text" and msg_type != "text":
                            matches = False
                        elif instant_filter == "file" and msg_type != "file":
                            matches = False
                        if matches:
                            matching_batch.append(msg)

                    should_vault = not curr_instant
                    media_to_file = {}
                    download_targets = matching_batch if curr_instant else (batch if should_vault else [])
                    if is_protected_flow and download_targets:
                        for msg in download_targets:
                            if msg.media:
                                try:
                                    async with userbot_lock:
                                        async with media_semaphore:
                                            path = await userbot.download_media(msg)
                                    if path:
                                        media_to_file[msg.id] = path
                                except errors.FloodWaitError as fwe:
                                    logger.warning(f"⏳ COLLECTION FLOOD: Download media flood wait of {fwe.seconds}s required. Skipping media.")
                                    await notify_admin_flood_wait(userbot, "Collection Download", fwe.seconds)
                                    if fwe.seconds <= 5:
                                        await asyncio.sleep(fwe.seconds)
                                        try:
                                            async with userbot_lock:
                                                async with media_semaphore:
                                                    path = await userbot.download_media(msg)
                                            if path: media_to_file[msg.id] = path
                                        except Exception as e2:
                                            logger.error(f"Failed to download media after short flood wait: {e2}")
                                except Exception as e:
                                    logger.error(f"Failed to download media for message {msg.id}: {e}")

                    try:
                        if curr_instant and matching_batch:
                            try:
                                has_media = any(msg.media for msg in matching_batch)
                                is_reply = any(getattr(msg, 'reply_to_msg_id', None) for msg in matching_batch)
                                if is_protected_flow or is_reply:
                                    if is_protected_flow and has_media and not any(msg.id in media_to_file for msg in matching_batch):
                                        logger.warning("🛡️ COLLECTION: Skipping mirror because media download failed/skipped.")
                                        opts["skipped"] += len(matching_batch)
                                    else:
                                        async with userbot_lock:
                                            await send_mirrored_content(userbot, tid, matching_batch, t_topic, auto_mirror, sid, pre_downloaded=media_to_file if (is_protected_flow and has_media) else None)
                                        sent_count += len(matching_batch)
                                else:
                                    try:
                                        async with userbot_lock:
                                            src_peer = await userbot.get_input_entity(int(sid))
                                            tgt_peer = await userbot.get_input_entity(int(tid))
                                        
                                        dest_topic_id = t_topic
                                        if auto_mirror:
                                            first_msg = matching_batch[0]
                                            s_top = getattr(first_msg.reply_to, 'reply_to_top_id', None) or (first_msg.reply_to.reply_to_msg_id if first_msg.reply_to else None)
                                            if not s_top and getattr(first_msg, 'forum_topic', False):
                                                s_top = first_msg.id
                                            if not s_top and first_msg.reply_to_msg_id:
                                                s_top = first_msg.reply_to_msg_id
                                            if s_top:
                                                mapped_topic = get_topic_mapping(sid, s_top, tid)
                                                if mapped_topic:
                                                    dest_topic_id = mapped_topic
                                                else:
                                                    forum = getattr(first_msg.reply_to, "forum_topic", None)
                                                    src_title = getattr(forum, "title", None)
                                                    src_icon = getattr(forum, "icon_emoji_id", None)
                                                    if not src_title:
                                                        try:
                                                            async with userbot_lock:
                                                                res = await userbot(functions.messages.GetForumTopicsRequest(
                                                                    peer=src_peer, offset_date=0, offset_id=0, offset_topic=0, limit=100
                                                                ))
                                                            for t in res.topics:
                                                                if t.id == s_top:
                                                                    src_title = t.title
                                                                    src_icon = getattr(t, "icon_emoji_id", None)
                                                                    break
                                                        except Exception: pass
                                                    if src_title:
                                                        dest_topic_id = await get_or_create_target_topic(userbot, tid, src_title, sid, s_top, icon_emoji_id=src_icon)

                                        import random
                                        random_ids = [random.randint(-9223372036854775808, 9223372036854775807) for _ in matching_batch]
                                        target_entity = await resolve_target_id(userbot, tid)
                                        is_forum = getattr(target_entity, 'forum', False) if not isinstance(target_entity, int) else False
                                        top_msg_id_val = int(dest_topic_id) if (is_forum and dest_topic_id) else None
                                        
                                        async with userbot_lock:
                                            fwd_res = await userbot(functions.messages.ForwardMessagesRequest(
                                                from_peer=src_peer,
                                                id=[msg.id for msg in matching_batch],
                                                to_peer=target_entity,
                                                random_id=random_ids,
                                                top_msg_id=top_msg_id_val
                                            ))
                                        if fwd_res:
                                            fwd_msgs = []
                                            if hasattr(fwd_res, 'updates'):
                                                for u in fwd_res.updates:
                                                    if type(u).__name__ in ["UpdateNewMessage", "UpdateNewChannelMessage"]:
                                                        fwd_msgs.append(u.message)
                                            if len(fwd_msgs) == len(matching_batch):
                                                for orig_m, fwd_m in zip(matching_batch, fwd_msgs):
                                                    save_message_mapping(sid, orig_m.id, tid, fwd_m.id)
                                        sent_count += len(matching_batch)
                                    except Exception as fwd_err:
                                        logger.error(f"Native Forward in collection failed ({fwd_err}). Falling back to mirror...")
                                        async with userbot_lock:
                                            await send_mirrored_content(userbot, tid, matching_batch, t_topic, auto_mirror, sid)
                                        sent_count += len(matching_batch)
                            except Exception as fe:
                                logger.error(f"Failed to forward batch: {fe}")
                                opts["skipped"] += len(matching_batch)
                        
                        with db_conn() as conn:
                            c = conn.cursor()
                            for m in batch:
                                m_type = get_specific_media_type(m.media)
                                if curr_instant:
                                    matches = True
                                    if instant_filter == "media" and not m.media:
                                        matches = False
                                    elif instant_filter == "text" and m.media:
                                        matches = False
                                    rel_val = 1 if matches else 0
                                else:
                                    rel_val = 0

                                if USING_POSTGRES:
                                    c.execute(
                                        """
                                        INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption, added_by, released) 
                                        VALUES (%s, %s, %s, %s, %s, 'collection', %s) 
                                        ON CONFLICT (source_chat_id, source_message_id) 
                                        DO UPDATE SET 
                                            pair_id = EXCLUDED.pair_id,
                                            media_type = EXCLUDED.media_type,
                                            caption = EXCLUDED.caption,
                                            added_by = EXCLUDED.added_by,
                                            released = EXCLUDED.released,
                                            timestamp = CURRENT_TIMESTAMP
                                        """,
                                        (pair_id, sid, m.id, m_type, m.message or "", rel_val)
                                    )
                                else:
                                    c.execute(
                                        """
                                        INSERT INTO collected_media (pair_id, source_chat_id, source_message_id, media_type, caption, added_by, released) 
                                        VALUES (?, ?, ?, ?, ?, 'collection', ?) 
                                        ON CONFLICT (source_chat_id, source_message_id) 
                                        DO UPDATE SET 
                                            pair_id = excluded.pair_id,
                                            media_type = excluded.media_type,
                                            caption = excluded.caption,
                                            added_by = excluded.added_by,
                                            released = excluded.released,
                                            timestamp = datetime('now')
                                        """,
                                        (pair_id, sid, m.id, m_type, m.message or "", rel_val)
                                    )
                            conn.commit()

                        if should_vault and batch:
                            if is_protected_flow:
                                files_to_vault = [media_to_file.get(m.id) for m in batch if m.id in media_to_file]
                                if files_to_vault or not any(m.media for m in batch):
                                    file_payload = files_to_vault if len(files_to_vault) > 1 else (files_to_vault[0] if files_to_vault else None)
                                    for token, username, bot_id in get_log_bots():
                                        metadata = f"SID: {sid} | MID: {batch[0].id}\n"
                                        caption_text = metadata + (batch[0].message or "")
                                        
                                        vault_topic_id = None
                                        try:
                                            vault_entity = await resolve_target_id(userbot, int(bot_id))
                                            if getattr(source_chat, 'forum', False) and getattr(vault_entity, 'forum', False):
                                                s_top = getattr(batch[0].reply_to, 'reply_to_top_id', None) or (batch[0].reply_to.reply_to_msg_id if batch[0].reply_to else None)
                                                if not s_top and getattr(batch[0], 'forum_topic', False):
                                                    s_top = batch[0].id
                                                if not s_top and batch[0].reply_to_msg_id:
                                                    s_top = batch[0].reply_to_msg_id
                                                if s_top:
                                                    mapped = get_topic_mapping(sid, s_top, int(bot_id))
                                                    if mapped:
                                                        vault_topic_id = mapped
                                                    else:
                                                        forum = getattr(batch[0].reply_to, "forum_topic", None) if batch[0].reply_to else None
                                                        src_title = getattr(forum, "title", None)
                                                        src_icon = getattr(forum, "icon_emoji_id", None)
                                                        if not src_title:
                                                            try:
                                                                async with userbot_lock:
                                                                    res = await userbot(functions.messages.GetForumTopicsRequest(
                                                                        peer=source_chat, offset_date=0, offset_id=0, offset_topic=0, limit=100
                                                                    ))
                                                                for t in res.topics:
                                                                    if t.id == s_top:
                                                                        src_title = t.title
                                                                        src_icon = getattr(t, "icon_emoji_id", None)
                                                                        break
                                                            except Exception: pass
                                                        if src_title:
                                                            vault_topic_id = await get_or_create_target_topic(
                                                                userbot, int(bot_id), src_title, sid, s_top, icon_emoji_id=src_icon
                                                            )
                                        except Exception as topic_err:
                                            logger.error(f"Error resolving topic for vault group {bot_id}: {topic_err}")

                                        try:
                                            async with userbot_lock:
                                                vaulted_result = await userbot.send_message(
                                                    entity=int(bot_id),
                                                    file=file_payload,
                                                    message=caption_text,
                                                    reply_to=int(vault_topic_id) if vault_topic_id else None
                                                )
                                            if vaulted_result:
                                                v_msgs = vaulted_result if isinstance(vaulted_result, list) else [vaulted_result]
                                                for i, v_m in enumerate(v_msgs):
                                                    orig_m = batch[i]
                                                    save_logged_media(
                                                        bot_id=int(bot_id),
                                                        log_msg_id=int(v_m.id),
                                                        source_chat_id=int(sid),
                                                        source_msg_id=int(orig_m.id),
                                                        file_id=None,
                                                        media_type=type(orig_m.media).__name__ if orig_m.media else "text",
                                                        caption=orig_m.message or "",
                                                        grouped_id=orig_m.grouped_id
                                                    )
                                        except Exception as e:
                                            logger.error(f"Error vaulting pre-downloaded media to bot {bot_id}: {e}")
                            else:
                                await forward_to_log_bots(userbot, batch, sid)
                    finally:
                        for temp_path in media_to_file.values():
                            if os.path.exists(temp_path):
                                try: os.remove(temp_path)
                                except Exception: pass
                                
                        opts.update({
                            "sent_count": sent_count
                        })
                        
                    if (curr_instant and matching_batch) or (should_vault and batch):
                        # Gradual release sleep delay to avoid bulk flood
                        await asyncio.sleep(1.2)

            if to_fetch_remain is not None:
                to_fetch_remain -= len(chunk)
                
            offset_id = chunk[-1].id
            
            is_task_active = running_tasks.get(task_key)
            if is_task_active:
                try:
                    bot.edit_message_text(
                        get_collection_status_text(task_key),
                        admin_chat_id,
                        status_msg.message_id,
                        reply_markup=get_collection_markup(pair_id),
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
                await asyncio.sleep(2.5)

        if running_tasks.get(task_key):
            opts = collection_options.setdefault(task_key, {})
            opts["status"] = "Completed"
            opts["progress"] = 100
            try:
                bot.edit_message_text(
                    get_collection_status_text(task_key, is_done=True),
                    admin_chat_id,
                    status_msg.message_id,
                    reply_markup=get_collection_markup(pair_id),
                    parse_mode="HTML"
                )
            except Exception:
                pass
                
            curr_instant = opts.get("instant_release", False)
            instant_filter = opts.get("instant_filter", "everything")
            f_map = {"everything": "All Content", "media": "Media Only", "text": "Text Only", "file": "Files Only"}
            f_label = f" matching {f_map.get(instant_filter, 'All Content')} 🔄" if curr_instant else ""
            sent_label = f"Sent to Target: `{sent_count}`{f_label}" if curr_instant else f"Sent to Target: `{sent_count} (Hold Mode)`"
            bot.send_message(admin_chat_id, f"✅ Collection Done: `{s_title}`\nScanned: `{scanned}`\nCollected & Saved: `{collected}`\n{sent_label}")
        else:
            bot.send_message(admin_chat_id, f"🛑 Collection for `{s_title}` stopped by user.\nScanned: `{scanned}`\nCollected & Saved: `{collected}`")
            
    except Exception as e:
        bot.send_message(admin_chat_id, f"❌ Collection Error: {e}")
    finally:
        running_tasks.pop(task_key, None)
        collection_options.pop(task_key, None)

async def run_release(admin_chat_id, pair_id, added_by=None, interval=1.2, release_filter="everything"):
    is_ok, msg = await ensure_userbot()
    if not is_ok:
        bot.send_message(admin_chat_id, f"❌ Userbot error: {msg}")
        return

    task_key = f"rel_{added_by}_{pair_id}" if added_by else f"rel_{pair_id}"
    running_tasks[task_key] = True
    
    try:
        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"SELECT source_id, target_id, source_title, is_mirror, source_topic_id, target_topic_id, content_filter FROM target_pairs WHERE id = {p}", (pair_id,))
            row = c.fetchone()
        
        if not row: return
        sid_ref, tid_ref, s_title, is_mir, s_topic, t_topic, cf = row
    
        source_chat = None
        source_accessible = False
        client, source_chat = await resolve_target_across_fleet(sid_ref)
        if not client:
            client = userbot
            try:
                source_chat = await resolve_target_id(client, sid_ref)
            except Exception:
                pass
        userbot = client
        
        if source_chat:
            source_accessible = True
            
        try:
            target_chat = await resolve_target_id(userbot, tid_ref)
        except Exception as e:
            bot.send_message(admin_chat_id, f"❌ Connection Error: {e}\n\nMake sure the bot is a member of the target chat.")
            return

        # Map added_by filter for query backward compatibility
        media_filter = ""
        category_name = "Collected Items"
        if added_by == "monitor":
            media_filter = "AND COALESCE(added_by, 'monitor') = 'monitor'"
            category_name = "Monitor"
        elif added_by == "scraper":
            media_filter = "AND COALESCE(added_by, 'monitor') = 'scraper'"
            category_name = "History Scraper"
        elif added_by == "collection":
            media_filter = "AND COALESCE(added_by, 'monitor') = 'collection'"
            category_name = "Collect Now"

        with db_conn() as conn:
            c = conn.cursor()
            p = get_placeholder()
            c.execute(f"SELECT id, source_message_id FROM collected_media WHERE pair_id = {p} AND released = 0 {media_filter}", (pair_id,))
            items = c.fetchall()
        
        if not items:
            bot.send_message(admin_chat_id, f"No pending items from {category_name} to release.")
            return

        sent = 0
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🛑 Stop Release", callback_data=f"pair_stop_task_rel_{added_by}_{pair_id}" if added_by else f"pair_stop_task_rel_{pair_id}"))
        
        f_names = {"everything": "All Content 🔄", "media": "Media Only 🖼️", "text": "Text Only 📝"}
        display_filter = f_names.get(release_filter, "All Content 🔄")
        status_msg = bot.send_message(admin_chat_id, f"🚀 Releasing `{len(items)}` items from {category_name}...\n🎯 Filter: `{display_filter}`", reply_markup=markup)
        
        idx = 0
        while idx < len(items):
            if not running_tasks.get(task_key): break
            row_id, smid = items[idx]
            
            advance = True
            try:
                msg = None
                from_vault = False
                vault_bot_id = None
                
                if source_accessible and source_chat:
                    try:
                        msg = await userbot.get_messages(source_chat, ids=smid)
                    except Exception as ge:
                        logger.warning(f"Could not get message {smid} from source: {ge}. Trying vault.")
                
                if not msg:
                    # Try to fetch from vault bot mapping
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"SELECT bot_id, log_msg_id FROM log_media WHERE source_chat_id = {p} AND source_msg_id = {p}", (sid_ref, smid))
                        vault_row = c.fetchone()
                    
                    if vault_row:
                        vault_bot_id, vault_msg_id = vault_row
                        try:
                            vault_chat = await resolve_target_id(userbot, vault_bot_id)
                            msg = await userbot.get_messages(vault_chat, ids=vault_msg_id)
                            if msg:
                                from_vault = True
                        except Exception as ve:
                            logger.error(f"Failed to fetch message {smid} from vault bot {vault_bot_id}: {ve}")
                
                if not msg:
                    # Message is completely inaccessible, mark as skipped so we don't loop
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"UPDATE collected_media SET released = 2 WHERE id = {p}", (row_id,))
                        conn.commit()
                    continue

                # --- CONTENT FILTERING ---
                cf = cf or "everything"
                if cf == "media" and not msg.media:
                    # Mark as released so we don't try again
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"UPDATE collected_media SET released = 1 WHERE id = {p}", (row_id,))
                        conn.commit()
                    continue
                if cf == "text" and msg.media:
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"UPDATE collected_media SET released = 1 WHERE id = {p}", (row_id,))
                        conn.commit()
                    continue

                # --- DYNAMIC RELEASE FILTERING ---
                # Check message against dynamic release filter: if it doesn't match, we skip
                # sending but keep it in the database as unreleased (released = 0) for future release runs.
                if release_filter == "media" and not msg.media:
                    continue
                if release_filter == "text" and msg.media:
                    continue

                target_topic_anchor = t_topic
                
                # Determine if BOTH chats are forums to automatically mirror topics
                auto_mirror = False
                is_source_forum = getattr(source_chat, 'forum', False) if source_accessible else True # Try mapping if target is forum
                if is_source_forum and getattr(target_chat, 'forum', False):
                    auto_mirror = True

                # Handle Mirroring ID detection for release
                if auto_mirror:
                    s_top = None
                    if from_vault and vault_bot_id:
                        v_top = getattr(msg.reply_to, 'reply_to_top_id', None) or (msg.reply_to.reply_to_msg_id if msg.reply_to else None)
                        if v_top:
                            with db_conn() as conn:
                                c = conn.cursor()
                                p = get_placeholder()
                                c.execute(f"SELECT source_topic_id FROM topic_mappings WHERE source_chat_id = {p} AND target_topic_id = {p} AND target_chat_id = {p}", (sid_ref, v_top, vault_bot_id))
                                row_t = c.fetchone()
                                if row_t:
                                    s_top = row_t[0]
                    else:
                        s_top = getattr(msg.reply_to, 'reply_to_top_id', None) or (msg.reply_to.reply_to_msg_id if msg.reply_to else None)
                        if not s_top and getattr(msg, 'forum_topic', False):
                            s_top = msg.id
                        if not s_top and msg.reply_to_msg_id:
                            s_top = msg.reply_to_msg_id
                    
                    if s_top:
                        # Priority check database mapping
                        mapped = get_topic_mapping(sid_ref, s_top, tid_ref)
                        if mapped:
                            target_topic_anchor = mapped
                        elif source_accessible:
                            # Search for title/icon in source chat to mirror dynamically
                            forum = getattr(msg.reply_to, "forum_topic", None) if msg.reply_to else None
                            src_title = getattr(forum, "title", None)
                            src_icon = getattr(forum, "icon_emoji_id", None)
                            if not src_title:
                                try:
                                    async with userbot_lock:
                                        res = await userbot(functions.messages.GetForumTopicsRequest(
                                            peer=source_chat, offset_date=0, offset_id=0, offset_topic=0, limit=100
                                        ))
                                    for t in res.topics:
                                        if t.id == s_top:
                                            src_title = t.title
                                            src_icon = getattr(t, "icon_emoji_id", None)
                                            break
                                except Exception as e:
                                    logger.error(f"Failed to fetch source forum topics in release: {e}")
                            
                            if src_title:
                                target_topic_anchor = await get_or_create_target_topic(
                                    userbot, tid_ref, src_title, sid_ref, s_top, icon_emoji_id=src_icon
                                )

                # Resolve reply mapping
                reply_to_val = None
                src_reply_msg_id = None
                if from_vault and vault_bot_id and getattr(msg, "reply_to_msg_id", None):
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"SELECT source_msg_id FROM log_media WHERE bot_id = {p} AND log_msg_id = {p}", (vault_bot_id, msg.reply_to_msg_id))
                        row_r = c.fetchone()
                        if row_r:
                            src_reply_msg_id = row_r[0]
                elif not from_vault and getattr(msg, "reply_to_msg_id", None):
                    src_reply_msg_id = msg.reply_to_msg_id
                    
                if src_reply_msg_id:
                    reply_to_val = get_message_mapping(sid_ref, src_reply_msg_id, tid_ref)

                # Construct Topic Header
                # If it's a specific reply, use it. Otherwise, use the Topic Header ID.
                final_reply_target = reply_to_val if reply_to_val else target_topic_anchor

                sent_msg = None
                try:
                    sent_msg = await userbot.send_message(
                        entity=target_chat,
                        message=msg.message or "",
                        file=msg.media,
                        reply_to=int(final_reply_target) if final_reply_target else None
                    )
                except Exception as e:
                    # If we had a reply target, attempt to fallback/downgrade reply first
                    if final_reply_target is not None:
                        is_forum = getattr(target_chat, 'forum', False) if not isinstance(target_chat, int) else False
                        next_reply = None
                        if is_forum and target_topic_anchor and int(final_reply_target) != int(target_topic_anchor):
                            next_reply = int(target_topic_anchor)
                        
                        logger.warning(f"⚠️ RELEASE: Failed to send with reply_to={final_reply_target} ({e}). Retrying with reply_to={next_reply}...")
                        try:
                            sent_msg = await userbot.send_message(
                                entity=target_chat,
                                message=msg.message or "",
                                file=msg.media,
                                reply_to=next_reply
                            )
                        except Exception as e2:
                            if next_reply is not None:
                                logger.warning(f"⚠️ RELEASE: Failed to send with reply_to={next_reply} ({e2}). Retrying with reply_to=None...")
                                try:
                                    sent_msg = await userbot.send_message(
                                        entity=target_chat,
                                        message=msg.message or "",
                                        file=msg.media,
                                        reply_to=None
                                    )
                                except Exception as e3:
                                    e = e3
                            else:
                                e = e2
                    
                    # If still failed, check if we need to do fallback download & upload
                    if not sent_msg:
                        err_msg = str(e).lower()
                        if any(x in err_msg for x in ["protected", "forward", "restricted", "noforwards", "forbidden", "reference", "peer"]):
                            logger.info("🛡️ RELEASE: Protected or invalid peer media detected. Attempting download & upload fallback...")
                            local_file = None
                            try:
                                local_file = await userbot.download_media(msg)
                            except errors.FloodWaitError as fwe:
                                raise fwe
                            except Exception as de:
                                logger.error(f"Failed to download media in release fallback: {de}")
                            if local_file:
                                try:
                                    sent_msg = await userbot.send_message(
                                        entity=target_chat,
                                        message=msg.message or "",
                                        file=local_file,
                                        reply_to=int(final_reply_target) if final_reply_target else None
                                    )
                                except Exception as fe:
                                    # Downgrade in fallback as well
                                    if final_reply_target is not None:
                                        is_forum = getattr(target_chat, 'forum', False) if not isinstance(target_chat, int) else False
                                        next_reply = None
                                        if is_forum and target_topic_anchor and int(final_reply_target) != int(target_topic_anchor):
                                            next_reply = int(target_topic_anchor)
                                        
                                        logger.warning(f"⚠️ RELEASE FALLBACK: Failed to send with reply_to={final_reply_target} ({fe}). Retrying with reply_to={next_reply}...")
                                        try:
                                            sent_msg = await userbot.send_message(
                                                entity=target_chat,
                                                message=msg.message or "",
                                                file=local_file,
                                                reply_to=next_reply
                                            )
                                        except Exception as fe2:
                                            if next_reply is not None:
                                                logger.warning(f"⚠️ RELEASE FALLBACK: Failed to send with reply_to={next_reply} ({fe2}). Retrying with reply_to=None...")
                                                try:
                                                    sent_msg = await userbot.send_message(
                                                        entity=target_chat,
                                                        message=msg.message or "",
                                                        file=local_file,
                                                        reply_to=None
                                                    )
                                                except Exception as fe3:
                                                    raise fe3
                                            else:
                                                raise fe2
                                    else:
                                        raise fe
                                finally:
                                    if os.path.exists(local_file):
                                        os.remove(local_file)
                            else:
                                raise e
                        else:
                            raise e
                
                if sent_msg:
                    save_message_mapping(sid_ref, msg.id, tid_ref, sent_msg.id)
                    with db_conn() as conn:
                        c = conn.cursor()
                        p = get_placeholder()
                        c.execute(f"UPDATE collected_media SET released = 1 WHERE id = {p}", (row_id,))
                        conn.commit()
                sent += 1
                if sent % 5 == 0:
                    try: bot.edit_message_text(f"🚀 Releasing `{s_title}` ({category_name})...\n🎯 Filter: `{display_filter}`\nSent: `{sent}/{len(items)}`", admin_chat_id, status_msg.message_id, reply_markup=markup)
                    except Exception: pass
                await asyncio.sleep(interval)
            except errors.FloodWaitError as fwe:
                logger.warning(f"⏳ RELEASE FLOOD: A wait of {fwe.seconds} seconds is required. Sleeping...")
                try:
                    bot.edit_message_text(
                        f"⏳ *Release Rate-Limited*\n\nWaiting `{fwe.seconds}` seconds before retrying message ID `{smid}`...",
                        admin_chat_id,
                        status_msg.message_id,
                        reply_markup=markup
                    )
                except Exception:
                    pass
                await asyncio.sleep(fwe.seconds)
                advance = False
            except Exception as e:
                err_msg = str(e).lower()
                if any(x in err_msg for x in ["private", "permission", "ban", "forbidden", "access"]):
                    logger.warning(f"⚠️ RELEASE: Message ID {smid} is inaccessible ({e}). Marking as failed/skipped.")
                    try:
                        with db_conn() as conn:
                            c = conn.cursor()
                            p = get_placeholder()
                            c.execute(f"UPDATE collected_media SET released = 2 WHERE id = {p}", (row_id,))
                            conn.commit()
                    except Exception as db_err:
                        logger.error(f"Failed to update inaccessible status in DB: {db_err}")
                logger.error(f"Release error: {e}")
                await asyncio.sleep(0.05)
            finally:
                if advance:
                    idx += 1

        bot.send_message(admin_chat_id, f"✅ Release Complete: Sent `{sent}` items from {category_name} matching `{display_filter}`.")
    except Exception as e:
        logger.error(f"Global Release Error: {e}")
        bot.send_message(admin_chat_id, f"❌ Release Crashed: {e}")
    finally:
        running_tasks.pop(task_key, None)


# -----------------------------
# Watchdog
# -----------------------------
async def userbot_watchdog():
    """
    Periodically check if the userbot session is still valid.
    If banned or deactivated, clear session and notify admin.
    """
    while True:
        global userbot
        if userbot and userbot.is_connected():
            try:
                await userbot.get_me()
            except Exception as e:
                err_msg = str(e).lower()
                if isinstance(e, errors.UnauthorizedError) or any(x in err_msg for x in ["deactivated", "authorized", "revoked", "simultaneous", "ip address"]):
                    logger.warning(f"WATCHDOG: Userbot session invalid or conflict: {e}")
                    try: await userbot.disconnect()
                    except Exception: pass
                    userbot = None
                    
                    # Clear session from DB to force re-login
                    with db_conn() as conn:
                        c = conn.cursor()
                        c.execute("DELETE FROM settings WHERE key IN ('session_string', 'api_id', 'api_hash')")
                    logger.info("WATCHDOG: Session cleared from DB due to conflict/invalidation.")
                    
                    bot.send_message(ADMIN_ID, f"⚠️ *USERBOT SESSION EXPIRED/BANNED*\n\nThe account session has been deactivated, revoked, or unauthorized. Session has been cleared.\nError: `{e}`", parse_mode="Markdown")
                else:
                    logger.error(f"WATCHDOG: Unexpected error: {e}")
        
        await asyncio.sleep(1800) # Check every 30 minutes

# -----------------------------
# Health + keepalive
# -----------------------------
def keep_alive_worker():
    """
    Periodically ping this service URL to reduce idle/sleep risk on Render.
    Auto-detects URL from environment variables.
    """
    def detect_public_url() -> str:
        # 1) Render Env
        url = os.getenv("RENDER_EXTERNAL_URL", "").strip()
        if url: return url.rstrip("/")
        
        # 2) DB Saved (Auto-detected from first visit)
        url = get_setting("detected_url")
        if url: return url.rstrip("/")
        
        # 3) Railway Env
        url = os.getenv("RAILWAY_STATIC_URL", "").strip()
        if url:
            if url.startswith("http"): return url.rstrip("/")
            return f"https://{url}".rstrip("/")
        
        # 4) Manual Generic
        url = os.getenv("WEB_URL", "").strip()
        if url: return url.rstrip("/")
        
        return ""

    detected_url = ""
    while True:
        try:
            url = detect_public_url()
            if url:
                if url != detected_url:
                    detected_url = url
                    logger.info(f"KEEP_ALIVE: Monitoring URL: {detected_url}")
                
                resp = requests.get(f"{url}?t={int(time.time())}", timeout=15)
                logger.info(f"KEEP_ALIVE: Ping sent to {url}, Status: {resp.status_code}")
            else:
                # If no URL is set in ENV, we can't ping
                logger.warning("KEEP_ALIVE: No RENDER_EXTERNAL_URL or WEB_URL found. Bot will likely sleep on Render Free tier!")
        except Exception as e:
            logger.warning(f"KEEP_ALIVE: Ping failed: {e}")
        finally:
            time.sleep(240) # 4 minutes

from flask import Flask, request

app = Flask(__name__)
@app.route("/")
def health():
    # Auto-detect URL from the first request
    if not get_setting("detected_url"):
        # We assume https because Render/Railway use it
        protocol = "https" if request.is_secure or "https" in request.url_root else "http"
        detected = f"{protocol}://{request.host}"
        set_setting("detected_url", detected)
        logger.info(f"KEEP_ALIVE: Auto-detected and SAVED public URL: {detected}")
    
    return "Userbot v2 Running", 200

@app.route("/webhook/<token>", methods=["POST"])
def telegram_webhook(token):
    if token == BOT_TOKEN:
        if request.headers.get("content-type") == "application/json":
            json_string = request.get_data().decode("utf-8")
            update = telebot.types.Update.de_json(json_string)
            bot.process_new_updates([update])
            return "OK", 200
    return "Forbidden", 403

def run_web():
    app.run(host="0.0.0.0", port=PORT)

def shutdown_handler(*args):
    logger.warning("🛑 Shutting down cleanly...")
    try:
        bot.stop_polling()
    except:
        pass
    try:
        if userbot and userbot.is_connected():
            loop.run_until_complete(userbot.disconnect())
    except:
        pass
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown_handler)
signal.signal(signal.SIGINT, shutdown_handler)

# -----------------------------
# Log Bot System (Fleet Manager)
# -----------------------------
class LogBotManager:
    def __init__(self):
        self.bots = {} # { bot_id: telebot_instance }
        self.states = {} # { bot_id: { user_id: { state_data } } }

    def start_all(self):
        logger.info("📡 Initializing Log Bot Fleet...")
        bots = get_log_bots()
        for token, username, bot_id in bots:
            try:
                self.add_bot(token)
            except Exception as e:
                logger.error(f"Failed to start Log Bot {username}: {e}")

    def add_bot(self, token):
        new_bot = telebot.TeleBot(token, threaded=False)
        bot_info = new_bot.get_me()
        bot_id = bot_info.id
        
        if bot_id in self.bots:
            return bot_id
            
        self.bots[bot_id] = new_bot
        self._setup_handlers(new_bot, bot_id)
        
        # Start polling in a separate thread
        def run_polling():
            while True:
                try:
                    logger.info(f"🚀 Log Bot @{bot_info.username} started polling.")
                    new_bot.delete_webhook(drop_pending_updates=True)
                    # Use a shorter timeout and skip pending to reduce conflict duration
                    new_bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
                except Exception as e:
                    if "Conflict" in str(e):
                        logger.warning(f"⚠️ Log Bot @{bot_info.username} conflict. Retrying in 15s...")
                        time.sleep(15)
                    else:
                        logger.error(f"Log Bot @{bot_info.username} crashed: {e}")
                        time.sleep(10)
        
        threading.Thread(target=run_polling, daemon=True).start()
        return bot_id

    def _setup_handlers(self, bot_instance, bot_id):
        @bot_instance.message_handler(commands=['start'])
        def cmd_start(message):
            if message.from_user.id != ADMIN_ID: return
            count = get_logged_media_stats(bot_id)
            text = (f"🤖 *Vault Manager Online*\n\n"
                    f"📊 *Storage Stats:*\n"
                    f"📦 Total Vaulted: `{count}` items\n\n"
                    f"Use /grouplist to see categorized media.")
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("📤 Send Log", callback_data="lb_vault_main"))
            bot_instance.send_message(message.chat.id, text, reply_markup=markup, parse_mode="Markdown")

        @bot_instance.message_handler(commands=['get'])
        def fetch_from_vault(message):
            if message.from_user.id != ADMIN_ID: return
            try:
                args = message.text.split()
                if len(args) < 2:
                    bot_instance.reply_to(message, "❌ Usage: `/get [Fetch_ID]`")
                    return
                fetch_id = args[1]
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    try:
                        f_id_val = int(fetch_id)
                    except ValueError:
                        bot_instance.reply_to(message, "❌ Invalid ID format. Must be a number.")
                        return
                    c.execute(f"SELECT file_id, media_type, caption FROM log_media WHERE log_msg_id = {p} AND bot_id = {p}", (f_id_val, bot_id))
                    res = c.fetchone()
                if res:
                    file_id, m_type, caption = res
                    bot_instance.send_chat_action(message.chat.id, 'upload_document')
                    if m_type == "photo": bot_instance.send_photo(message.chat.id, file_id, caption=caption)
                    elif m_type == "video": bot_instance.send_video(message.chat.id, file_id, caption=caption)
                    else: bot_instance.send_document(message.chat.id, file_id, caption=caption)
                else:
                    bot_instance.reply_to(message, "🔍 ID not found in this bot's vault.")
            except Exception as e:
                bot_instance.reply_to(message, f"❌ Error: {e}")

        @bot_instance.message_handler(commands=['getcount'])
        def fetch_recent_batch(message):
            if message.from_user.id != ADMIN_ID: return
            try:
                args = message.text.split()
                count = int(args[1]) if len(args) > 1 and args[1].isdigit() else 5
                if count > 30: count = 30 
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    c.execute(f"SELECT file_id, media_type, caption, log_msg_id FROM log_media WHERE bot_id = {p} ORDER BY timestamp DESC LIMIT {p}", (bot_id, count))
                    results = c.fetchall()
                if not results:
                    bot_instance.reply_to(message, "🔍 Vault empty.")
                    return
                for f_id, m_t, cap, l_id in reversed(results):
                    full_cap = f"🆔 ID: `{l_id}`\n\n{cap or ''}"
                    if m_t == "photo": bot_instance.send_photo(message.chat.id, f_id, caption=full_cap, parse_mode="Markdown")
                    elif m_t == "video": bot_instance.send_video(message.chat.id, f_id, caption=full_cap, parse_mode="Markdown")
                    else: bot_instance.send_document(message.chat.id, f_id, caption=full_cap, parse_mode="Markdown")
                    time.sleep(0.5)
            except Exception as e:
                bot_instance.reply_to(message, f"❌ Error: {e}")

        @bot_instance.message_handler(commands=['grouplist'])
        def cmd_group_list(message):
            if message.from_user.id != ADMIN_ID: return
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                c.execute(f"""
                    SELECT m.source_chat_id, p.source_title, COUNT(m.id)
                    FROM log_media m
                    LEFT JOIN target_pairs p ON m.source_chat_id = p.source_id
                    WHERE m.bot_id = {p}
                    GROUP BY m.source_chat_id, p.source_title
                """, (bot_id,))
                groups = c.fetchall()
            if not groups:
                bot_instance.send_message(message.chat.id, "📭 No media found.")
                return
            markup = InlineKeyboardMarkup(row_width=1)
            for sid, title, cnt in groups:
                if sid is None or sid == 0: continue
                markup.add(InlineKeyboardButton(f"📁 {title or 'Direct'} — {cnt}", callback_data=f"v_group_stats_{sid}"))
            bot_instance.send_message(message.chat.id, "📂 *Vault Groups*", reply_markup=markup, parse_mode="Markdown")

        @bot_instance.message_handler(commands=['getbyid'])
        def fetch_by_group_id(message):
            if message.from_user.id != ADMIN_ID: return
            try:
                args = message.text.split()
                if len(args) < 2: return bot_instance.reply_to(message, "❌ `/getbyid [ID] [Count]`")
                group_id, count = int(args[1]), (int(args[2]) if len(args) > 2 else 5)
                with db_conn() as conn:
                    c = conn.cursor()
                    p = get_placeholder()
                    c.execute(f"SELECT file_id, media_type, caption, log_msg_id FROM log_media WHERE source_chat_id = {p} AND bot_id = {p} ORDER BY timestamp DESC LIMIT {p}", (group_id, bot_id, count))
                    results = c.fetchall()
                for f_id, m_t, cap, l_id in reversed(results):
                    bot_instance.send_photo(message.chat.id, f_id, caption=f"🆔 ID: `{l_id}`\n{cap or ''}") if m_t == "photo" else bot_instance.send_document(message.chat.id, f_id, caption=f"🆔 ID: `{l_id}`")
                    time.sleep(0.5)
            except Exception as e: bot_instance.reply_to(message, f"❌ Error: {e}")

        @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("v_group_stats_"))
        def handle_group_stats(call):
            sid = int(call.data.split("_")[-1])
            with db_conn() as conn:
                c = conn.cursor()
                p = get_placeholder()
                
                c.execute(f"SELECT source_title FROM target_pairs WHERE source_id = {p} LIMIT 1", (sid,))
                res = c.fetchone()
                title = res[0] if res else "Unknown Group"
                
                # Fetch count for this specific bot
                c.execute(f"SELECT COUNT(*) FROM log_media WHERE source_chat_id = {p} AND bot_id = {p}", (sid, bot_id))
                total = c.fetchone()[0]

            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🚀 Send batch to Group", callback_data=f"v_dump_start_{sid}"))
            markup.add(InlineKeyboardButton("🔙 Back to List", callback_data="lb_vault_main"))

            msg = (f"📊 *Group Statistics*\n\n"
                   f"🏷 *Title:* `{title}`\n"
                   f"🆔 *ID:* `{sid}`\n"
                   f"📦 *Total Media:* `{total}`\n\n"
                   f"💡 Click the button below to send this media into a different group via this Log Bot.")
            bot_instance.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

        @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("v_dump_start_"))
        def start_dump_flow(call):
            sid = int(call.data.split("_")[-1])
            login_data[call.from_user.id] = {"dump_sid": sid}
            
            async def get_list():
                markup = await get_chat_selection_markup("lb_vault_tgt", 0) # Reuse the tgt selection markup
                if not markup:
                    bot_instance.answer_callback_query(call.id, "❌ Main Userbot Offline", show_alert=True)
                    return
                bot_instance.edit_message_text(
                    "🎯 *Select Destination*\nWhere should the Log Bot send this media?",
                    call.message.chat.id, call.message.message_id, reply_markup=markup
                )
            asyncio.run_coroutine_threadsafe(get_list(), loop)

        @bot_instance.message_handler(content_types=['photo', 'video', 'document', 'audio', 'animation', 'sticker'])
        def handle_logging(message):
            try:
                m_type = "document"
                file_id = None
                caption = message.caption or ""
                if message.photo: m_type, file_id = "photo", message.photo[-1].file_id
                elif message.video: m_type, file_id = "video", message.video.file_id
                elif message.document: m_type, file_id = "document", message.document.file_id
                elif message.audio: m_type, file_id = "audio", message.audio.file_id
                elif message.animation: m_type, file_id = "animation", message.animation.file_id
                elif message.sticker: m_type, file_id = "sticker", message.sticker.file_id
                
                if file_id:
                    sid, mid = 0, message.message_id
                    if caption and "SID:" in caption and "MID:" in caption:
                        try:
                            parts = caption.split("|")
                            sid = int(parts[0].replace("SID:", "").strip())
                            mid = int(parts[1].split("\n")[0].replace("MID:", "").strip())
                            caption = caption.split("\n", 1)[1] if "\n" in caption else ""
                        except: pass
                    
                    # Log Bot API also has media_group_id
                    m_gid = message.media_group_id
                    save_logged_media(bot_id, message.message_id, sid, mid, file_id, m_type, caption, grouped_id=m_gid)
                    if sid == 0 and message.from_user.id == ADMIN_ID:
                        bot_instance.reply_to(message, f"✅ *Saved to Vault!*\n🆔 ID: `{message.message_id}`\nFetch: `/get {message.message_id}`")
            except Exception as e: logger.error(f"Logging Error: {e}")

        @bot_instance.callback_query_handler(func=lambda call: True)
        def handle_log_bot_callbacks(call):
            if call.from_user.id != ADMIN_ID: return
            data = call.data
            uid = call.from_user.id
            
            if data == "lb_vault_main":
                bot_instance.answer_callback_query(call.id)
                stats = get_log_bot_stats(bot_id)
                if not stats:
                    bot_instance.send_message(call.message.chat.id, "❌ No vaulted media found for this bot.")
                    return
                    
                markup = InlineKeyboardMarkup(row_width=1)
                for sid, title, count in stats:
                    markup.add(InlineKeyboardButton(f"📁 {title} ({count})", callback_data=f"lb_vault_src_{sid}"))
                markup.add(InlineKeyboardButton("🔙 Cancel", callback_data="lb_cancel"))
                
                bot_instance.edit_message_text("🚀 *Select Source Group*\n\nWhich group's vaulted content do you want to send?", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")

            elif data.startswith("lb_vault_src_"):
                bot_instance.answer_callback_query(call.id)
                sid = int(data.split("_")[-1])
                login_data[uid] = {"vault_source_id": sid}
                
                async def show_tgt():
                    markup = await get_chat_selection_markup("lb_vault_tgt", 0)
                    if not markup:
                        main_bot_username = bot.get_me().username
                        msg = "⚠️ *Userbot Offline*\n\nI cannot fetch your group list because the main userbot is not connected.\n\nPlease go to your *Main Admin Bot* and use the *'Connect Userbot'* button."
                        btn = InlineKeyboardMarkup().add(InlineKeyboardButton("🔌 Connect at Main Bot", url=f"https://t.me/{main_bot_username}"))
                        btn.add(InlineKeyboardButton("🔙 Back", callback_data="lb_vault_main"))
                        bot_instance.edit_message_text(msg, call.message.chat.id, call.message.message_id, reply_markup=btn, parse_mode="Markdown")
                        return
                        
                    bot_instance.edit_message_text("🎯 *Select Target Chat*\n\nChoose the group/channel where you want to release this media.\n⚠️ *IMPORTANT*: The Main Bot must be an admin in the target chat!", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="Markdown")
                asyncio.run_coroutine_threadsafe(show_tgt(), loop)
                
            elif data.startswith("lb_vault_tgt_"):
                bot_instance.answer_callback_query(call.id)
                parts = data.split("_")
                if parts[3] == "page":
                    page = int(parts[4])
                    async def update_tgt_list():
                        markup = await get_chat_selection_markup("lb_vault_tgt", page)
                        if markup:
                            bot_instance.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)
                    asyncio.run_coroutine_threadsafe(update_tgt_list(), loop)
                else:
                    tid = int(parts[3])
                    
                    async def handle_dest():
                        try:
                            entity = await resolve_target_id(userbot, tid)
                            if getattr(entity, 'forum', False):
                                markup = await get_topic_selection_markup(tid, "lb_vault_topic")
                                bot_instance.edit_message_text(f"🧵 *Forum Detected*\nSelect a topic in `{entity.title}`:", call.message.chat.id, call.message.message_id, reply_markup=markup)
                            else:
                                # Standard group
                                is_dump = "dump_sid" in login_data.get(uid, {})
                                if is_dump:
                                    login_data[uid]["dump_tid"] = tid
                                    login_data[uid]["dump_topic"] = None
                                    admin_states[f"lb_{bot_id}_{uid}"] = f"wait_dump_count_{tid}"
                                    bot_instance.edit_message_text(f"🔢 *How many items?*\nEnter count for group `{tid}`:", call.message.chat.id, call.message.message_id)
                                else:
                                    login_data[uid]["vault_target_id"] = tid
                                    login_data[uid]["vault_topic_id"] = None
                                    admin_states[f"lb_{bot_id}_{uid}"] = "awaiting_rel_interval"
                                    bot_instance.edit_message_text("⏳ *Release Interval*\nEnter time (seconds) between messages:", call.message.chat.id, call.message.message_id)
                        except Exception as e:
                            bot_instance.send_message(call.message.chat.id, f"❌ Error: {e}")
                    asyncio.run_coroutine_threadsafe(handle_dest(), loop)

            elif data.startswith("lb_vault_topic_"):
                bot_instance.answer_callback_query(call.id)
                payload = data.replace("lb_vault_topic_", "")
                tid_str, topic_id_str = payload.rsplit("_", 1)
                tid = int(tid_str)
                topic_id = int(topic_id_str)
                topic_val = topic_id if topic_id != 0 else None
                
                is_dump = "dump_sid" in login_data.get(uid, {})
                if is_dump:
                    login_data[uid]["dump_tid"] = tid
                    login_data[uid]["dump_topic"] = topic_val
                    admin_states[f"lb_{bot_id}_{uid}"] = f"wait_dump_count_{tid}"
                    bot_instance.edit_message_text(f"🔢 *Topic Set!*\nEnter count for topic `{topic_id}`:", call.message.chat.id, call.message.message_id)
                else:
                    login_data[uid]["vault_target_id"] = tid
                    login_data[uid]["vault_topic_id"] = topic_val
                    admin_states[f"lb_{bot_id}_{uid}"] = "awaiting_rel_interval"
                    bot_instance.edit_message_text(f"⏳ *Topic Set!*\nEnter release interval (seconds):", call.message.chat.id, call.message.message_id)
                    
            elif data == "lb_cancel":
                bot_instance.answer_callback_query(call.id)
                admin_states.pop(f"lb_{bot_id}_{uid}", None)
                cmd_start(call.message)

            elif data.startswith("lb_stop_rel_"):
                bot_instance.answer_callback_query(call.id, "🛑 Stopping...")
                task_key = data.replace("lb_stop_rel_", "")
                stop_task(task_key)

            elif data.startswith("lb_do_release_"):
                # lb_do_release_{sid}_{tid}_{interval}_{topic}
                bot_instance.answer_callback_query(call.id)
                parts = data.split("_")
                sid, tid = int(parts[3]), int(parts[4])
                interval = float(parts[5])
                topic_id = int(parts[6])
                
                # FIX: Convert 0 back to None for the engine
                topic_val = topic_id if topic_id != 0 else None
                
                bot_instance.edit_message_text(f"🚀 *Initializing Engine...*\nInterval: `{interval}s`", call.message.chat.id, call.message.message_id, parse_mode="Markdown")
                asyncio.run_coroutine_threadsafe(run_vault_release(bot_instance, call.message.chat.id, sid, tid, interval=interval, target_topic_id=topic_val, log_target_id=bot_id), loop)

        @bot_instance.message_handler(func=lambda m: m.from_user.id == ADMIN_ID and admin_states.get(f"lb_{bot_id}_{m.from_user.id}"))
        def handle_lb_messages(message):
            uid = message.from_user.id
            state = admin_states.get(f"lb_{bot_id}_{uid}")
            text = message.text.strip()
            
            if state == "awaiting_rel_interval":
                try:
                    interval = float(text)
                    if interval < 0.1: raise ValueError()
                    
                    admin_states.pop(f"lb_{bot_id}_{uid}", None)
                    sid = login_data.get(uid, {}).get("vault_source_id")
                    tid = login_data.get(uid, {}).get("vault_target_id")
                    t_topic = login_data.get(uid, {}).get("vault_topic_id")
                    
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("🚀 Start Release", callback_data=f"lb_do_release_{sid}_{tid}_{interval}_{t_topic or 0}"))
                    markup.add(InlineKeyboardButton("❌ Cancel", callback_data="lb_cancel"))
                    
                    bot_instance.send_message(message.chat.id, f"✅ *Interval Set: `{interval}s`*\nReady to release from `{sid}` to `{tid}`.", reply_markup=markup, parse_mode="Markdown")
                except:
                    bot_instance.reply_to(message, "⚠️ Invalid interval. Please send a number (e.g. `2.0`).")

            elif state.startswith("wait_dump_count_"):
                try:
                    target_cid = int(state.split("_")[-1])
                    count = int(text)
                    
                    user_session = login_data.get(uid)
                    if not user_session or "dump_sid" not in user_session:
                        bot_instance.send_message(message.chat.id, "❌ Session expired.")
                        return

                    source_cid = user_session["dump_sid"]
                    target_topic = user_session.get("dump_topic") 
                    
                    admin_states.pop(f"lb_{bot_id}_{uid}", None)
                    
                    # Start the background task
                    asyncio.run_coroutine_threadsafe(
                        run_vault_release(
                            sender_bot=bot_instance, 
                            admin_chat_id=message.chat.id, 
                            source_id=source_cid, 
                            target_id=target_cid, 
                            interval=2.0, 
                            log_target_id=bot_id,
                            limit=count,
                            target_topic_id=target_topic
                        ), 
                        loop
                    )
                except Exception as e:
                    bot_instance.reply_to(message, f"❌ Error: {e}. Please send a number.")

log_bot_manager = LogBotManager()

# -----------------------------
# Main Loop
# -----------------------------
async def main():
    try:
        bot_info = bot.get_me()
        logger.info(f"🤖 Main Admin Bot Token belongs to: @{bot_info.username} (ID: {bot_info.id})")
    except Exception as e:
        logger.error(f"❌ Failed to get main bot info: {e}")

    # Load external plugins dynamically from plugins directory
    plugins_dir = os.path.join(os.path.dirname(__file__), "plugins")
    if os.path.exists(plugins_dir):
        import importlib.util
        import glob
        import sys
        
        initial_cb_count = len(bot.callback_query_handlers)
        initial_msg_count = len(bot.message_handlers)
        
        plugin_files = glob.glob(os.path.join(plugins_dir, "*.py"))
        for fpath in plugin_files:
            if os.path.basename(fpath) == "__init__.py":
                continue
            module_name = f"plugins.{os.path.splitext(os.path.basename(fpath))[0]}"
            try:
                spec = importlib.util.spec_from_file_location(module_name, fpath)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                logger.info(f"🔌 Loaded plugin: {module_name}")
            except Exception as e:
                logger.error(f"❌ Error loading plugin {fpath}: {e}")
                
        # Re-order handlers so plugin handlers run before default catch-alls
        new_cb_handlers = bot.callback_query_handlers[initial_cb_count:]
        if new_cb_handlers:
            del bot.callback_query_handlers[initial_cb_count:]
            bot.callback_query_handlers = new_cb_handlers + bot.callback_query_handlers
            
        new_msg_handlers = bot.message_handlers[initial_msg_count:]
        if new_msg_handlers:
            del bot.message_handlers[initial_msg_count:]
            bot.message_handlers = new_msg_handlers + bot.message_handlers

    # Start web server IMMEDIATELY for Render health checks
    logger.info(f"Starting web server on port {PORT}...")
    threading.Thread(target=run_web, daemon=True).start()
    
    try:
        init_db()
    except Exception as e:
        logger.error(f"DB Init Error: {e}")
    if not DATABASE_URL:
        logger.warning("⚠️ WARNING: You are using SQLite. Your data (pairs, media) will be DELETED every time Render restarts!")
        logger.warning("Please set a DATABASE_URL (PostgreSQL) for permanent storage.")

    # Boot all saved Log Bots
    try:
        log_bot_manager.start_all()
    except Exception as e:
        logger.error(f"Error booting log bots: {e}")

    asyncio.create_task(userbot_watchdog())
    asyncio.create_task(instance_coordinator())
    asyncio.create_task(media_queue_worker())
    threading.Thread(target=keep_alive_worker, daemon=True).start()

    # Try to start existing session
    try:
        ok, msg = await start_userbot()
        if ok: 
            logger.info("✅ Userbot fleet started")
            # Cache warmer: fetch dialogs for all active clients
            for client in userbot_fleet_manager.get_all_clients():
                logger.info(f"📡 Warming up peer cache for {client._me.first_name}...")
                try:
                    async for _ in client.iter_dialogs(limit=50): pass
                except Exception:
                    pass
            logger.info("✅ Peer cache warmed")
    except Exception as e: 
        logger.error(f"Userbot startup error: {e}")

    # Configure Webhook if RENDER_EXTERNAL_URL is available, otherwise fallback to polling
    webhook_url = os.getenv("RENDER_EXTERNAL_URL", "").strip().rstrip("/")
    use_webhook = bool(webhook_url)
    
    if use_webhook:
        try:
            bot.remove_webhook()
            success = bot.set_webhook(url=f"{webhook_url}/webhook/{BOT_TOKEN}", drop_pending_updates=True)
            if success:
                logger.info(f"🚀 Admin Bot webhook set successfully: {webhook_url}/webhook/<token>")
            else:
                logger.error("❌ Failed to set Admin Bot webhook: Telegram returned False.")
                use_webhook = False
        except Exception as e:
            logger.error(f"❌ Failed to set Admin Bot webhook: {e}")
            use_webhook = False
            
    if not use_webhook:
        # Start telebot polling with AUTO-RESTART
        def run_polling():
            while True:
                try:
                    logger.info("🚀 Starting Admin Bot polling...")
                    bot.delete_webhook(drop_pending_updates=True)
                    # Reduced timeout and conflict handling
                    bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=20)
                except Exception as e:
                    if "Conflict" in str(e):
                        logger.warning("⚠️ Main Admin Bot conflict. Retrying in 20s...")
                        time.sleep(20)
                    else:
                        logger.error(f"❌ Polling crashed: {e}. Restarting in 30s...")
                        time.sleep(30)
        
        polling_thread = threading.Thread(target=run_polling, daemon=True)
        polling_thread.start()
        logger.info("✨ Admin bot monitor started (Polling Mode)")
    else:
        logger.info("✨ Admin bot monitor started (Webhook Mode)")
    
    if userbot:
        try:
            await userbot.run_until_disconnected()
        except Exception as e:
            logger.error(f"Userbot disconnected with error: {e}")
            if "AuthKeyDuplicatedError" in str(e):
                logger.critical("🚨 CRITICAL: Duplicate session detected. Stopping Userbot loop.")
            # Keep the main thread alive so background bots keep working
            while True:
                await asyncio.sleep(3600)
    else:
        while True:
            await asyncio.sleep(3600)

@bot.message_handler(func=lambda m: is_authorized_manager(m.from_user.id))
def handle_admin_direct_message(message):
    text = message.text.strip() if message.text else ""
    if not text: return
    
    parsed = parse_telegram_link(text)
    if parsed:
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("✅ Yes, Join", callback_data=f"join_chat_yes|{parsed['type']}|{parsed['hash'] if parsed['type'] == 'invite' else parsed['username']}"),
            InlineKeyboardButton("❌ Cancel", callback_data="join_chat_cancel")
        )
        bot.send_message(
            message.chat.id,
            f"❓ *Group Join Request*\n\nDetected Telegram reference: `{text}`\nDo you want the Userbot to join this chat?",
            reply_markup=markup,
            parse_mode="Markdown"
        )
    else:
        bot.reply_to(message, "❓ *Unrecognized Input*\n\nTo join a group, send its username (e.g. `@groupname`), public link (e.g. `t.me/groupname`), or private invite link (e.g. `t.me/+invitehash`).", parse_mode="Markdown")

if __name__ == "__main__":
    loop.run_until_complete(main())
