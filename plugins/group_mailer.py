import sys
import os
import json
import asyncio
import logging
import random
import time
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest

logger = logging.getLogger(__name__)

# Find the active running bot module from sys.modules
main_module = None
for name in ['__main__', 'userbot', 'testuserbot_v3']:
    mod = sys.modules.get(name)
    if mod and hasattr(mod, 'get_dashboard_markup'):
        main_module = mod
        break

if main_module is None:
    try:
        import userbot as main_module
    except ImportError:
        import testuserbot_v3 as main_module

bot = main_module.bot
get_dashboard_markup = main_module.get_dashboard_markup
is_authorized_manager = main_module.is_authorized_manager
set_setting = main_module.set_setting
get_setting = main_module.get_setting
admin_states = main_module.admin_states
userbot_fleet_manager = main_module.userbot_fleet_manager
loop = main_module.loop

# Media folder configuration
MEDIA_DIR = "mailer_media"
os.makedirs(MEDIA_DIR, exist_ok=True)

# In-memory cache for userbot groups
userbot_groups_cache = {}

# Initialize database schema for task-based mailer
def init_plugin_db():
    try:
        with main_module.db_conn() as conn:
            c = conn.cursor()
            is_pg = main_module.USING_POSTGRES
            auto_inc = "SERIAL PRIMARY KEY" if is_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"
            
            c.execute(f"""
                CREATE TABLE IF NOT EXISTS gm_tasks (
                    id {auto_inc},
                    name TEXT,
                    userbot_ids TEXT,
                    group_ids TEXT,
                    message TEXT,
                    repeat_interval INTEGER DEFAULT 0,
                    last_run REAL DEFAULT 0,
                    update_group_id TEXT
                )
            """)
            
            # Map join links schema inside plugin db
            c.execute("""
                CREATE TABLE IF NOT EXISTS gm_links_map (
                    group_id TEXT PRIMARY KEY,
                    link TEXT
                )
            """)
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize Group Mailer Task database: {e}")

# Run DB initialization
init_plugin_db()

# DB Helpers for Tasks
def db_create_task(name):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO gm_tasks (name, userbot_ids, group_ids, message, repeat_interval, last_run, update_group_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (name, "[]", "[]", "{}", 0, 0.0, "")
        ) if not main_module.USING_POSTGRES else c.execute(
            "INSERT INTO gm_tasks (name, userbot_ids, group_ids, message, repeat_interval, last_run, update_group_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (name, "[]", "[]", "{}", 0, 0.0, "")
        )
        conn.commit()
        return c.lastrowid

def db_get_tasks():
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, name, repeat_interval, last_run FROM gm_tasks ORDER BY id DESC")
        return c.fetchall()

def db_get_task(task_id):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT id, name, userbot_ids, group_ids, message, repeat_interval, last_run, update_group_id FROM gm_tasks WHERE id = ?", (task_id,)) if not main_module.USING_POSTGRES else c.execute("SELECT id, name, userbot_ids, group_ids, message, repeat_interval, last_run, update_group_id FROM gm_tasks WHERE id = %s", (task_id,))
        return c.fetchone()

def db_update_task(task_id, field, value):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        query = f"UPDATE gm_tasks SET {field} = ? WHERE id = ?" if not main_module.USING_POSTGRES else f"UPDATE gm_tasks SET {field} = %s WHERE id = %s"
        c.execute(query, (value, task_id))
        conn.commit()

def db_delete_task(task_id):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("DELETE FROM gm_tasks WHERE id = ?", (task_id,)) if not main_module.USING_POSTGRES else c.execute("DELETE FROM gm_tasks WHERE id = %s", (task_id,))
        conn.commit()

# DB Helpers for Join Links
def db_save_link(group_id, link):
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO gm_links_map (group_id, link) VALUES (?, ?)", (str(group_id), link)) if not main_module.USING_POSTGRES else c.execute("INSERT INTO gm_links_map (group_id, link) VALUES (%s, %s) ON CONFLICT (group_id) DO UPDATE SET link = EXCLUDED.link", (str(group_id), link))
        conn.commit()

def db_get_links_map():
    with main_module.db_conn() as conn:
        c = conn.cursor()
        c.execute("SELECT group_id, link FROM gm_links_map")
        return dict(c.fetchall())

# Translate Telethon exceptions to user-friendly reasons
def get_friendly_error(exception):
    err_str = str(exception).lower()
    if "write_forbidden" in err_str or "chatwriteforbidden" in err_str:
        return "Write forbidden (Account restricted, banned, or lacks permission to post)"
    elif "deactivated" in err_str or "authkeydeactivated" in err_str:
        return "Userbot account is deactivated or banned by Telegram"
    elif "flood" in err_str or "floodwait" in err_str:
        return "Flood wait limits hit (Temporarily restricted by Telegram due to spam rules)"
    elif "private" in err_str or "channelprivate" in err_str:
        return "Group is private or inaccessible (Not a member / invite expired)"
    elif "peer" in err_str or "invalid" in err_str:
        return "Group username or ID is invalid/dead"
    elif "paid" in err_str or "star" in err_str or "paywall" in err_str:
        return "Paywall enabled (Group requires Stars to send messages)"
    elif "banned" in err_str:
        return "Banned from the chat/group"
    elif "slow_mode" in err_str or "slowmode" in err_str:
        return "Slow mode is active in this chat"
    return f"Failed: {str(exception)[:60]}"

# Helper to join a group using Telethon
async def join_group_via_client(client, link):
    link = link.strip()
    if not link:
        return False
    try:
        if "+" in link or "joinchat/" in link:
            hash_val = link.split("+")[-1].strip() if "+" in link else link.split("joinchat/")[-1].strip()
            hash_val = hash_val.split("/")[0].split("?")[0]
            await client(ImportChatInviteRequest(hash_val))
            return True
        else:
            username = link
            if "t.me/" in link:
                username = link.split("t.me/")[-1].split("/")[0].split("?")[0]
            if not username.startswith("@") and not username.isdigit():
                username = "@" + username
            await client(JoinChannelRequest(username))
            return True
    except Exception as e:
        logger.error(f"Join failed for {link}: {e}")
        raise e

# Save the original get_dashboard_markup function
original_get_dashboard_markup = get_dashboard_markup

def new_get_dashboard_markup():
    markup = original_get_dashboard_markup()
    markup.add(InlineKeyboardButton("📬 Group Mailer Tasks", callback_data="gm_tasks_main"))
    return markup

# Monkeypatch the dashboard markup function
main_module.get_dashboard_markup = new_get_dashboard_markup

# Dynamic fetching dialogs function
async def fetch_dialogs_async(client, ub_id):
    groups = []
    async for dialog in client.iter_dialogs(limit=200):
        if dialog.is_group or dialog.is_channel:
            groups.append({
                "id": dialog.id,
                "title": dialog.name,
                "username": dialog.entity.username if hasattr(dialog.entity, 'username') and dialog.entity.username else None
            })
    userbot_groups_cache[ub_id] = groups

# Render task control status description
def get_task_status_text(task):
    t_id, name, userbot_ids_raw, group_ids_raw, message_raw, interval, last_run_timestamp, update_group = task
    selected_ubs = json.loads(userbot_ids_raw or "[]")
    selected_groups = json.loads(group_ids_raw or "[]")
    msg_data = json.loads(message_raw or "{}")
    
    ub_status = f"🔴 None"
    if selected_ubs:
        connected_count = 0
        for ub_id in selected_ubs:
            client = userbot_fleet_manager.get_client(int(ub_id))
            if client and client.is_connected():
                connected_count += 1
        ub_status = f"🟢 Configured ({connected_count}/{len(selected_ubs)} Connected)"
        
    msg_status = "🔴 None"
    if msg_data:
        msg_status = f"🟢 Configured ({msg_data.get('type').upper()})"
        
    rep_status = "🔴 Off (Manual Only)"
    if interval > 0:
        if interval < 60:
            rep_status = f"🟢 Every {interval} minutes"
        else:
            rep_status = f"🟢 Every {interval // 60} hour(s)"
            
    last_run_time = "Never"
    if last_run_timestamp > 0:
        last_run_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_run_timestamp))
        
    update_group_status = f"`{update_group}`" if update_group else "🔴 Muted (No log updates)"
        
    return (
        f"📋 *Task Name:* `{name}` (ID: `{t_id}`)\n"
        f"👤 *Selected Userbots:* {ub_status}\n"
        f"👥 *Groups Selected:* `{len(selected_groups)}` groups marked\n"
        f"📢 *Update Group:* {update_group_status}\n"
        f"💬 *Mailer Message:* {msg_status}\n"
        f"⏰ *Repeat Interval:* `{rep_status}`\n"
        f"📅 *Last Run:* `{last_run_time}`"
    )

# Helper to render the interactive groups checklist page for a specific task
def show_task_groups_page(chat_id, message_id, task_id, ub_id, page=0):
    groups = userbot_groups_cache.get(ub_id, [])
    task = db_get_task(task_id)
    selected_ids = set(json.loads(task[3] or "[]"))
    update_group_id = task[7]
    
    if not groups:
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("🔄 Refresh List", callback_data=f"gm_tref_{task_id}_{page}"),
            InlineKeyboardButton("🔙 Back", callback_data=f"gm_task_view_{task_id}")
        )
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="👥 *Groups:* Userbot is not in any groups yet. Tap **Refresh List** to fetch groups dynamically.",
            reply_markup=markup,
            parse_mode="Markdown"
        )
        return

    page_size = 8
    total_pages = (len(groups) + page_size - 1) // page_size
    page = max(0, min(page, total_pages - 1))
    
    start_idx = page * page_size
    end_idx = start_idx + page_size
    page_groups = groups[start_idx:end_idx]
    
    markup = InlineKeyboardMarkup()
    for g in page_groups:
        is_selected = g["id"] in selected_ids
        checkbox = "✅" if is_selected else "⬜"
        title = g["title"][:25]
        markup.add(InlineKeyboardButton(f"{checkbox} {title}", callback_data=f"gm_ttg_{task_id}_{g['id']}_{page}"))
        
    # Navigation row
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gm_tpage_{task_id}_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data="gm_noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"gm_tpage_{task_id}_{page+1}"))
    markup.row(*nav_row)
    
    # Bulk actions and Refresh row
    markup.row(
        InlineKeyboardButton("Select All", callback_data=f"gm_tselall_{task_id}_{page}"),
        InlineKeyboardButton("Clear All", callback_data=f"gm_tclrall_{task_id}_{page}"),
        InlineKeyboardButton("🔄 Refresh", callback_data=f"gm_tref_{task_id}_{page}")
    )

    # Log/Update group configuration row
    log_group_btn_text = f"📢 Group: {update_group_id}" if update_group_id else "📢 Set Update Group"
    markup.row(
        InlineKeyboardButton(log_group_btn_text, callback_data=f"gm_tsetgrp_{task_id}_{page}"),
        InlineKeyboardButton("❌ Remove Group", callback_data=f"gm_tdelgrp_{task_id}_{page}")
    )
    
    markup.add(InlineKeyboardButton("🔙 Back to Task Panel", callback_data=f"gm_task_view_{task_id}"))
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"👥 *SELECT TARGET GROUPS* (Selected: `{len(selected_ids)}`)\nToggle target checkboxes. Click **Refresh** to sync new groups:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# Register callback query handler
@bot.callback_query_handler(func=lambda call: call.data.startswith("gm_task"))
def handle_tasks_callbacks(call):
    uid = call.from_user.id
    if not is_authorized_manager(uid):
        return

    data = call.data
    chat_id = call.message.chat.id
    message_id = call.message.message_id

    if data == "gm_tasks_main":
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("📋 List Tasks", callback_data="gm_tasks_list"),
            InlineKeyboardButton("➕ Create Task", callback_data="gm_tasks_create")
        )
        markup.add(InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main"))
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="📬 *GROUP MAILER TASKS MANAGER*\n\nDefine separate message campaigns (tasks) with different userbots, target groups, and intervals.",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data == "gm_tasks_create":
        admin_states[uid] = "awaiting_gm_task_name"
        bot.send_message(chat_id, "📋 *CREATE CAMPAIGN TASK*\n\nPlease enter a name for your campaign task (e.g. `Promo Group A`):")
        bot.answer_callback_query(call.id)

    elif data == "gm_tasks_list":
        tasks = db_get_tasks()
        markup = InlineKeyboardMarkup()
        
        if not tasks:
            markup.add(InlineKeyboardButton("➕ Create Task", callback_data="gm_tasks_create"))
            markup.add(InlineKeyboardButton("🔙 Back", callback_data="gm_tasks_main"))
            bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="📋 *Group Mailer Tasks:* No tasks defined yet.", reply_markup=markup, parse_mode="Markdown")
            return

        for t_id, name, interval, last_run in tasks:
            rep_lbl = "Manual" if interval == 0 else (f"{interval}m" if interval < 60 else f"{interval//60}h")
            markup.add(InlineKeyboardButton(f"📋 {name} ({rep_lbl})", callback_data=f"gm_task_view_{t_id}"))

        markup.add(InlineKeyboardButton("🔙 Back", callback_data="gm_tasks_main"))
        bot.edit_message_text(chat_id=chat_id, message_id=message_id, text="📋 *Group Mailer Tasks:* Select a task to configure/run:", reply_markup=markup, parse_mode="Markdown")

    elif data.startswith("gm_task_view_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        if not task:
            bot.answer_callback_query(call.id, "❌ Task not found!")
            return

        # Save active chat ID for scheduler updates
        set_setting("gm_admin_chat_id", str(chat_id))

        markup = InlineKeyboardMarkup()
        
        # Row 1: Select Userbots, Select Msg
        markup.row(
            InlineKeyboardButton("👤 Select Userbots", callback_data=f"gm_taskubs_{t_id}"),
            InlineKeyboardButton("💬 Select Msg", callback_data=f"gm_taskmsg_{t_id}")
        )
        
        # Row 2: Groups Selector, Repeat Interval
        markup.row(
            InlineKeyboardButton("👥 Groups", callback_data=f"gm_taskgrps_{t_id}"),
            InlineKeyboardButton("⏰ Repeat Interval", callback_data=f"gm_taskrep_{t_id}")
        )
        
        # Row 3: Import Links, Start Operation
        markup.row(
            InlineKeyboardButton("🔗 Import Join Links", callback_data=f"gm_tasklinks_{t_id}"),
            InlineKeyboardButton("🚀 Start Operation", callback_data=f"gm_taskstart_{t_id}")
        )
        
        # Row 4: Delete Task, Back to list
        markup.row(
            InlineKeyboardButton("🗑 Delete Task", callback_data=f"gm_taskdel_{t_id}"),
            InlineKeyboardButton("🔙 Back to Tasks", callback_data="gm_tasks_list")
        )

        status_desc = get_task_status_text(task)
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"📬 *TASK CONTROL PANEL*\n\n{status_desc}",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_taskubs_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        clients = userbot_fleet_manager.get_all_clients()
        connected_clients = [c for c in clients if c.is_connected()]
        
        if not connected_clients:
            bot.answer_callback_query(call.id, "❌ No active connected userbots found!")
            return

        markup = InlineKeyboardMarkup(row_width=1)
        for client in connected_clients:
            c_id = client._me.id
            is_selected = c_id in selected_ubs
            checkbox = "✅" if is_selected else "⬜"
            
            first_name = client._me.first_name if hasattr(client, '_me') and client._me else "Userbot"
            username = f"@{client._me.username}" if hasattr(client, '_me') and client._me and client._me.username else ""
            
            markup.add(InlineKeyboardButton(f"{checkbox} {first_name} {username}", callback_data=f"gm_tasktglub_{t_id}_{c_id}"))
        
        markup.add(InlineKeyboardButton("🔙 Done / Back", callback_data=f"gm_task_view_{t_id}"))
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=f"👤 *Select Userbots for task `{task[1]}` (Multiple selection enabled):*",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_tasktglub_"):
        parts = data.split("_")
        t_id = int(parts[2])
        ub_id = int(parts[3])
        
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        if ub_id in selected_ubs:
            selected_ubs.remove(ub_id)
        else:
            selected_ubs.append(ub_id)
            
        db_update_task(t_id, "userbot_ids", json.dumps(selected_ubs))
        bot.answer_callback_query(call.id, "Preference updated!")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_taskubs_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_taskmsg_"):
        t_id = int(data.split("_")[-1])
        admin_states[uid] = f"awaiting_gm_taskmsg_{t_id}"
        bot.send_message(
            chat_id,
            "💬 *SET TASK MAILER MESSAGE*\n\n"
            "Please send or forward the message you want to broadcast for this campaign (can be text, photo, video, or document with captions)."
        )
        bot.answer_callback_query(call.id)

    elif data.startswith("gm_taskgrps_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        if not selected_ubs:
            bot.answer_callback_query(call.id, "⚠️ Please select at least one Userbot first!", show_alert=True)
            return

        primary_ub = str(selected_ubs[0])
        client = userbot_fleet_manager.get_client(int(primary_ub))
        if not client or not client.is_connected():
            bot.answer_callback_query(call.id, "❌ Primary userbot is offline or disconnected!", show_alert=True)
            return

        bot.answer_callback_query(call.id, "⏳ Loading groups...")
        if primary_ub in userbot_groups_cache:
            show_task_groups_page(chat_id, message_id, t_id, primary_ub, page=0)
        else:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏳ *Fetching groups list from userbot. Please wait...*",
                parse_mode="Markdown"
            )
            def on_fetch_done(fut):
                show_task_groups_page(chat_id, message_id, t_id, primary_ub, page=0)
            
            future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
            future.add_done_callback(on_fetch_done)

    elif data.startswith("gm_tasklinks_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        if not selected_ubs:
            bot.answer_callback_query(call.id, "❌ Please select at least one Userbot first!", show_alert=True)
            return
            
        admin_states[uid] = f"awaiting_gm_tasklinks_{t_id}"
        bot.send_message(
            chat_id,
            "🔗 *IMPORT TASK GROUP JOIN LINKS*\n\n"
            "Please send your group links (invite links or usernames, one per line).\n"
            "Example:\n`t.me/+invitehash`\n`@my_group`"
        )
        bot.answer_callback_query(call.id)

    elif data.startswith("gm_taskrep_"):
        t_id = int(data.split("_")[-1])
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("❌ Off (Manual)", callback_data=f"gm_tasksetrep_{t_id}_0"),
            InlineKeyboardButton("30 Min", callback_data=f"gm_tasksetrep_{t_id}_30")
        )
        markup.row(
            InlineKeyboardButton("1 Hour", callback_data=f"gm_tasksetrep_{t_id}_60"),
            InlineKeyboardButton("2 Hours", callback_data=f"gm_tasksetrep_{t_id}_120")
        )
        markup.row(
            InlineKeyboardButton("6 Hours", callback_data=f"gm_tasksetrep_{t_id}_360"),
            InlineKeyboardButton("12 Hours", callback_data=f"gm_tasksetrep_{t_id}_720")
        )
        markup.row(
            InlineKeyboardButton("24 Hours", callback_data=f"gm_tasksetrep_{t_id}_1440")
        )
        markup.add(InlineKeyboardButton("🔙 Back", callback_data=f"gm_task_view_{t_id}"))
        
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⏰ *SELECT REPEAT INTERVAL*\nConfigure how often this campaign task should automatically broadcast:",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_tasksetrep_"):
        parts = data.split("_")
        t_id = int(parts[2])
        minutes = int(parts[3])
        
        db_update_task(t_id, "repeat_interval", minutes)
        bot.answer_callback_query(call.id, "✅ Repeat interval updated!")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': f"gm_task_view_{t_id}", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_taskdel_"):
        t_id = int(data.split("_")[-1])
        db_delete_task(t_id)
        bot.answer_callback_query(call.id, "🗑 Task deleted successfully!")
        handle_tasks_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': "gm_tasks_list", 'message': call.message, 'id': call.id})())

    elif data.startswith("gm_taskstart_"):
        t_id = int(data.split("_")[-1])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        selected_groups = json.loads(task[3] or "[]")
        msg_data = json.loads(task[4] or "{}")

        if not selected_ubs:
            bot.answer_callback_query(call.id, "❌ Please select at least one Userbot first!", show_alert=True)
            return
        if not selected_groups:
            bot.answer_callback_query(call.id, "❌ Please select target groups first!", show_alert=True)
            return
        if not msg_data:
            bot.answer_callback_query(call.id, "❌ Please set the mailer message first!", show_alert=True)
            return

        bot.answer_callback_query(call.id, "🚀 Starting campaign operation...")
        asyncio.run_coroutine_threadsafe(
            run_task_broadcast(t_id),
            loop
        )

# Catch-all sub-handlers for page navigations and toggles on tasks
@bot.callback_query_handler(func=lambda call: call.data.startswith("gm_t"))
def handle_task_checklist_callbacks(call):
    uid = call.from_user.id
    if not is_authorized_manager(uid):
        return

    data = call.data
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    parts = data.split("_")

    # gm_ttg_{task_id}_{group_id}_{page}
    if data.startswith("gm_ttg_"):
        t_id = int(parts[2])
        g_id = int(parts[3])
        page = int(parts[4])
        
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        selected_ids = json.loads(task[3] or "[]")
        
        if g_id in selected_ids:
            selected_ids.remove(g_id)
        else:
            selected_ids.append(g_id)
            
        db_update_task(t_id, "group_ids", json.dumps(selected_ids))
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id)

    # gm_tpage_{task_id}_{page}
    elif data.startswith("gm_tpage_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id)

    # gm_tselall_{task_id}_{page}
    elif data.startswith("gm_tselall_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        primary_ub = str(selected_ubs[0])
        groups = userbot_groups_cache.get(primary_ub, [])
        
        selected_ids = set(json.loads(task[3] or "[]"))
        for g in groups:
            selected_ids.add(g["id"])
            
        db_update_task(t_id, "group_ids", json.dumps(list(selected_ids)))
        show_task_groups_page(chat_id, message_id, t_id, primary_ub, page)
        bot.answer_callback_query(call.id, "✅ Selected all groups!")

    # gm_tclrall_{task_id}_{page}
    elif data.startswith("gm_tclrall_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        db_update_task(t_id, "group_ids", "[]")
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)
        bot.answer_callback_query(call.id, "🗑 Cleared selections!")

    # gm_tref_{task_id}_{page}
    elif data.startswith("gm_tref_"):
        t_id = int(parts[2])
        page = int(parts[3])
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        primary_ub = str(selected_ubs[0])
        
        client = userbot_fleet_manager.get_client(int(primary_ub))
        if not client or not client.is_connected():
            bot.answer_callback_query(call.id, "❌ Selected userbot is offline!", show_alert=True)
            return
            
        bot.answer_callback_query(call.id, "🔄 Syncing new groups...")
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="🔄 *Syncing new groups from Telegram... Please wait...*",
            parse_mode="Markdown"
        )
        
        if primary_ub in userbot_groups_cache:
            del userbot_groups_cache[primary_ub]
            
        def on_sync_done(fut):
            show_task_groups_page(chat_id, message_id, t_id, primary_ub, page)
            
        future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
        future.add_done_callback(on_sync_done)

    # gm_tsetgrp_{task_id}_{page}
    elif data.startswith("gm_tsetgrp_"):
        t_id = int(parts[2])
        admin_states[uid] = f"awaiting_gm_tasklog_{t_id}"
        bot.send_message(
            chat_id,
            "📢 *SET UPDATE/LOG GROUP FOR THIS TASK*\n\n"
            "Please send the Group Chat ID (e.g. `-1001234567890`) where updates and failure logs for this campaign should go."
        )
        bot.answer_callback_query(call.id)

    # gm_tdelgrp_{task_id}_{page}
    elif data.startswith("gm_tdelgrp_"):
        t_id = int(parts[2])
        page = int(parts[3])
        db_update_task(t_id, "update_group_id", "")
        bot.answer_callback_query(call.id, "❌ Log group removed!")
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        show_task_groups_page(chat_id, message_id, t_id, str(selected_ubs[0]), page)


# Intercept message state inputs for Group Mailer Campaign Tasks
@bot.message_handler(func=lambda m: is_authorized_manager(m.from_user.id) and admin_states.get(m.from_user.id) and admin_states.get(m.from_user.id).startswith("awaiting_gm_"))
def handle_task_states(message):
    uid = message.from_user.id
    chat_id = message.chat.id
    state = admin_states.get(uid)
    text = message.text or ""

    if state == "awaiting_gm_task_name":
        cleaned_name = text.strip()
        if not cleaned_name:
            bot.reply_to(message, "❌ Name cannot be empty.")
            return
            
        task_id = db_create_task(cleaned_name)
        admin_states[uid] = None
        
        # Confirmation and direct redirect
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("⚙️ Configure Task", callback_data=f"gm_task_view_{task_id}"))
        bot.reply_to(
            message,
            f"✅ *Task `{cleaned_name}` Created!*",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif state.startswith("awaiting_gm_taskmsg_"):
        t_id = int(state.split("_")[-1])
        msg_type = "text"
        file_id = None
        caption = message.caption or ""
        local_path = None

        if message.photo:
            msg_type = "photo"
            file_id = message.photo[-1].file_id
        elif message.video:
            msg_type = "video"
            file_id = message.video.file_id
        elif message.document:
            msg_type = "document"
            file_id = message.document.file_id

        if file_id:
            try:
                msg_status = bot.reply_to(message, "⏳ Downloading media locally...")
                file_info = bot.get_file(file_id)
                downloaded_file = bot.download_file(file_info.file_path)
                
                ext = file_info.file_path.split(".")[-1]
                local_path = os.path.join(MEDIA_DIR, f"task_media_{t_id}_{uid}.{ext}")
                
                with open(local_path, "wb") as f:
                    f.write(downloaded_file)
                bot.delete_message(chat_id, msg_status.message_id)
            except Exception as e:
                bot.reply_to(message, f"❌ Media download failed: {e}")
                return

        msg_data = {
            "type": msg_type,
            "text": message.text or "",
            "caption": caption,
            "local_path": local_path
        }
        
        db_update_task(t_id, "message", json.dumps(msg_data))
        admin_states[uid] = None
        bot.reply_to(message, f"✅ *Mailer Message Saved for Task!* (Type: `{msg_type.upper()}`)", parse_mode="Markdown")

    elif state.startswith("awaiting_gm_tasklinks_"):
        t_id = int(state.split("_")[-1])
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        
        task = db_get_task(t_id)
        selected_ubs = json.loads(task[2] or "[]")
        
        client = userbot_fleet_manager.get_client(int(selected_ubs[0]))
        if not client or not client.is_connected():
            bot.reply_to(message, "❌ Primary userbot is offline. Cannot check invite links.")
            return

        bot.reply_to(message, "⏳ Processing and resolving join links...")
        
        links_map = db_get_links_map()
        selected_groups = json.loads(task[3] or "[]")
        
        success_count = 0
        
        async def resolve_links_task():
            nonlocal success_count
            for line in lines:
                try:
                    group_id = None
                    if "+" in line or "joinchat/" in line:
                        hash_val = line.split("+")[-1].strip() if "+" in line else line.split("joinchat/")[-1].strip()
                        hash_val = hash_val.split("/")[0].split("?")[0]
                        invite_info = await client(CheckChatInviteRequest(hash_val))
                        if hasattr(invite_info, 'chat'):
                            group_id = invite_info.chat.id
                    else:
                        username = line
                        if "t.me/" in line:
                            username = line.split("t.me/")[-1].split("/")[0].split("?")[0]
                        if not username.startswith("@") and not username.isdigit():
                            username = "@" + username
                        entity = await client.get_entity(username)
                        group_id = entity.id

                    if group_id:
                        final_id = int(group_id)
                        db_save_link(final_id, line)
                        if final_id not in selected_groups:
                            selected_groups.append(final_id)
                        success_count += 1
                except Exception as e:
                    logger.error(f"Error resolving line {line}: {e}")

            db_update_task(t_id, "group_ids", json.dumps(selected_groups))
            admin_states[uid] = None
            bot.send_message(
                chat_id,
                f"✅ *Links Processed!*\nSuccessfully resolved and added `{success_count}` groups to your selections.",
                parse_mode="Markdown"
            )

        asyncio.run_coroutine_threadsafe(resolve_links_task(), loop)

    elif state.startswith("awaiting_gm_tasklog_"):
        t_id = int(state.split("_")[-1])
        cleaned_id = text.strip()
        
        if not (cleaned_id.startswith("-") and cleaned_id.replace("-", "").isdigit()):
            bot.reply_to(message, "❌ *Invalid Group ID!*\nGroup IDs must start with a minus (e.g. `-1001234567890`).")
            return
            
        db_update_task(t_id, "update_group_id", cleaned_id)
        admin_states[uid] = None
        bot.reply_to(
            message,
            f"✅ *Task Log Group Configured!*\nLogs will go to: `{cleaned_id}`.",
            parse_mode="Markdown"
        )


# Asynchronous campaign execution running failover and joining logic
async def run_task_broadcast(task_id, is_auto=False):
    task = db_get_task(task_id)
    if not task:
        return
        
    _, name, userbot_ids_raw, group_ids_raw, message_raw, interval, _, update_group_id = task
    ub_ids = json.loads(userbot_ids_raw or "[]")
    msg_data = json.loads(message_raw or "{}")
    
    success = 0
    failed = 0
    label = f"⏰ Scheduled Campaign: {name}" if is_auto else f"📬 Task Mailer: {name}"
    
    dest_chat = int(update_group_id) if (update_group_id and update_group_id.strip()) else None

    progress_msg = None
    if dest_chat:
        try:
            progress_msg = bot.send_message(dest_chat, f"⏳ *{label} progress:* `0%`", parse_mode="Markdown")
        except Exception as err:
            logger.error(f"Failed to send task progress updates to {dest_chat}: {err}")

    # Update database last run timestamp
    db_update_task(task_id, "last_run", time.time())
    
    sent_group_ids = set()
    failed_details = []
    
    # Pre-load group titles from local cache
    group_titles = {}
    for ub_id in ub_ids:
        for g in userbot_groups_cache.get(str(ub_id), []):
            group_titles[g["id"]] = g["title"]

    while True:
        # Re-fetch task to load live group ids dynamically on every loop step
        live_task = db_get_task(task_id)
        if not live_task:
            break
            
        live_group_ids = json.loads(live_task[3] or "[]")
        links_map = db_get_links_map()
        
        remaining_groups = [g for g in live_group_ids if g not in sent_group_ids]
        if not remaining_groups:
            break
            
        group_id = remaining_groups[0]
        sent_group_ids.add(group_id)
        
        group_sent_successfully = False
        group_errors = []

        # Iterate over all selected userbots
        for ub_id in ub_ids:
            client = userbot_fleet_manager.get_client(int(ub_id))
            if not client or not client.is_connected():
                group_errors.append((ub_id, "Userbot offline"))
                continue

            try:
                entity = group_id
                try:
                    if isinstance(group_id, str) and group_id.startswith("@"):
                        entity = await client.get_entity(group_id)
                    elif isinstance(group_id, str) and group_id.isdigit():
                        entity = int(group_id)
                except Exception as ent_err:
                    join_link = links_map.get(str(group_id))
                    if join_link:
                        try:
                            await join_group_via_client(client, join_link)
                            join_wait = random.randint(5, 10)
                            await asyncio.sleep(join_wait)
                            entity = group_id
                            if isinstance(group_id, str) and group_id.startswith("@"):
                                entity = await client.get_entity(group_id)
                            elif isinstance(group_id, str) and group_id.isdigit():
                                entity = int(group_id)
                        except Exception as join_err:
                            raise Exception(f"Auto-join failed: {join_err}")
                    else:
                        raise ent_err

                # Send
                msg_type = msg_data.get("type")
                if msg_type == "text":
                    await client.send_message(entity, msg_data["text"])
                elif msg_type in ["photo", "video", "document"]:
                    await client.send_file(entity, msg_data["local_path"], caption=msg_data.get("caption", ""))
                
                group_sent_successfully = True
                break
            except Exception as e:
                try:
                    join_link = links_map.get(str(group_id))
                    if join_link and "auto-join failed" not in str(e).lower():
                        await join_group_via_client(client, join_link)
                        join_wait = random.randint(5, 10)
                        await asyncio.sleep(join_wait)
                        
                        msg_type = msg_data.get("type")
                        if msg_type == "text":
                            await client.send_message(entity, msg_data["text"])
                        elif msg_type in ["photo", "video", "document"]:
                            await client.send_file(entity, msg_data["local_path"], caption=msg_data.get("caption", ""))
                        
                        group_sent_successfully = True
                        break
                except Exception as retry_err:
                    e = retry_err
                
                group_errors.append((ub_id, e))
                logger.warning(f"Userbot {ub_id} failed to send to {group_id} under Task {task_id}: {e}")
        
        if group_sent_successfully:
            success += 1
        else:
            failed += 1
            g_title = group_titles.get(group_id, f"ID: {group_id}")
            report_lines = [f"❌ *Group:* `{g_title}`"]
            for ub_id, err in group_errors:
                report_lines.append(f"  ⚠️ *UB {ub_id}:* {get_friendly_error(err)}")
            failed_details.append("\n".join(report_lines))

        # Live progress update
        total_groups = len(live_group_ids)
        processed_count = len(sent_group_ids)
        
        if progress_msg and dest_chat and (processed_count % 3 == 0 or processed_count == total_groups):
            pct = int((processed_count / max(1, total_groups)) * 100)
            try:
                bot.edit_message_text(
                    chat_id=dest_chat,
                    message_id=progress_msg.message_id,
                    text=f"⏳ *{label} progress:* `{pct}%` (Success: `{success}`, Failed: `{failed}` | Total: `{total_groups}`)",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

        # Random delay between 5 to 10 seconds
        await asyncio.sleep(random.randint(5, 10))

    if dest_chat:
        try:
            bot.send_message(
                dest_chat,
                f"✅ *{label} Completed!*\n\n🟢 Success: `{success}`\n🔴 Failed: `{failed}`",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    # Send Detailed Failure Report
    if failed_details and dest_chat:
        try:
            header = f"🚨 *{label} Failure Report:*\nThe message could not be sent to these groups on all configured accounts:\n\n"
            current_message = header
            
            for report in failed_details:
                if len(current_message) + len(report) + 2 > 4000:
                    bot.send_message(dest_chat, current_message, parse_mode="Markdown")
                    current_message = ""
                current_message += report + "\n\n"
                
            if current_message.strip():
                bot.send_message(dest_chat, current_message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error sending failure report: {e}")


# Background Scheduled Supervisor Loop for Campaign Tasks
async def scheduler_loop():
    logger.info("⏰ Group Mailer Tasks scheduler supervisor loop running...")
    while True:
        try:
            # Query all tasks with repeat schedules
            with main_module.db_conn() as conn:
                c = conn.cursor()
                c.execute("SELECT id, name, repeat_interval, last_run, userbot_ids, group_ids, message FROM gm_tasks WHERE repeat_interval > 0")
                tasks = c.fetchall()
                
            for t_id, name, interval, last_run, userbot_ids_raw, group_ids_raw, message_raw in tasks:
                now = time.time()
                if now - last_run >= (interval * 60):
                    ub_ids = json.loads(userbot_ids_raw or "[]")
                    selected_groups = json.loads(group_ids_raw or "[]")
                    msg_data = json.loads(message_raw or "{}")
                    
                    if ub_ids and selected_groups and msg_data:
                        has_active_client = False
                        for ub_id in ub_ids:
                            client = userbot_fleet_manager.get_client(int(ub_id))
                            if client and client.is_connected():
                                has_active_client = True
                                break
                                
                        if has_active_client:
                            # Start campaign task asynchronously
                            await run_task_broadcast(t_id, is_auto=True)
        except Exception as e:
            logger.error(f"Error in Tasks scheduler loop: {e}")
            
        await asyncio.sleep(30)  # Check every 30 seconds

# Start the background schedule task safely
asyncio.run_coroutine_threadsafe(scheduler_loop(), loop)
