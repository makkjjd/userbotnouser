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

def get_mailer_status():
    selected_ubs = json.loads(get_setting("gm_selected_userbots") or "[]")
    selected_groups = json.loads(get_setting("gm_selected_group_ids") or "[]")
    msg_data = json.loads(get_setting("gm_message") or "{}")
    interval = int(get_setting("gm_repeat_interval") or "0")
    links_map = json.loads(get_setting("gm_group_links_map") or "{}")
    update_group = get_setting("gm_update_group_id")
    
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
    last_run_timestamp = float(get_setting("gm_last_run") or "0")
    if last_run_timestamp > 0:
        last_run_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last_run_timestamp))
        
    update_group_status = f"`{update_group}`" if update_group else "🔴 Muted (No log updates)"
        
    return (
        f"👤 *Selected Userbots:* {ub_status}\n"
        f"👥 *Groups Selected:* `{len(selected_groups)}` groups marked\n"
        f"🔗 *Mapped Join Links:* `{len(links_map)}` links stored\n"
        f"📢 *Update Group:* {update_group_status}\n"
        f"💬 *Mailer Message:* {msg_status}\n"
        f"⏰ *Repeat Interval:* `{rep_status}`\n"
        f"📅 *Last Run:* `{last_run_time}`"
    )

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
    markup.add(InlineKeyboardButton("📬 Group Mailer", callback_data="group_mailer_main"))
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

# Helper to render the interactive groups page
def show_groups_page(chat_id, message_id, ub_id, page=0):
    groups = userbot_groups_cache.get(ub_id, [])
    selected_ids = set(json.loads(get_setting("gm_selected_group_ids") or "[]"))
    update_group_id = get_setting("gm_update_group_id")
    
    if not groups:
        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("🔄 Refresh List", callback_data=f"gm_refresh_{page}"),
            InlineKeyboardButton("🔙 Back", callback_data="group_mailer_main")
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
        markup.add(InlineKeyboardButton(f"{checkbox} {title}", callback_data=f"gm_tgl_{g['id']}_{page}"))
        
    # Navigation row
    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("◀️ Prev", callback_data=f"gm_page_{page-1}"))
    nav_row.append(InlineKeyboardButton(f"Page {page+1}/{total_pages}", callback_data="gm_noop"))
    if page < total_pages - 1:
        nav_row.append(InlineKeyboardButton("Next ▶️", callback_data=f"gm_page_{page+1}"))
    markup.row(*nav_row)
    
    # Bulk actions and Refresh row
    markup.row(
        InlineKeyboardButton("Select All", callback_data=f"gm_all_sel_{page}"),
        InlineKeyboardButton("Clear All", callback_data=f"gm_all_clr_{page}"),
        InlineKeyboardButton("🔄 Refresh", callback_data=f"gm_refresh_{page}")
    )

    # Log/Update group configuration row
    log_group_btn_text = f"📢 Group: {update_group_id}" if update_group_id else "📢 Set Update Group"
    markup.row(
        InlineKeyboardButton(log_group_btn_text, callback_data=f"gm_set_loggrp_{page}"),
        InlineKeyboardButton("❌ Remove Group", callback_data=f"gm_clear_loggrp_{page}")
    )
    
    markup.add(InlineKeyboardButton("🔙 Back to Mailer Console", callback_data="group_mailer_main"))
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=message_id,
        text=f"👥 *SELECT TARGET GROUPS* (Selected: `{len(selected_ids)}`)\nToggle target checkboxes. Click **Refresh** to sync new groups:",
        reply_markup=markup,
        parse_mode="Markdown"
    )

# Register callback query handler
@bot.callback_query_handler(func=lambda call: call.data == "group_mailer_main" or call.data.startswith("gm_"))
def handle_group_mailer_callbacks(call):
    uid = call.from_user.id
    if not is_authorized_manager(uid):
        return

    data = call.data
    chat_id = call.message.chat.id
    message_id = call.message.message_id
    selected_ubs = json.loads(get_setting("gm_selected_userbots") or "[]")

    if data == "group_mailer_main":
        interval = int(get_setting("gm_repeat_interval") or "0")
        if interval == 0:
            rep_btn_text = "⏰ Repeat: Off"
        elif interval < 60:
            rep_btn_text = f"⏰ Repeat: {interval}m"
        else:
            rep_btn_text = f"⏰ Repeat: {interval // 60}h"

        markup = InlineKeyboardMarkup()
        
        # Row 1: Select Userbots, Select Msg
        markup.row(
            InlineKeyboardButton("👤 Select Userbots", callback_data="gm_select_userbot"),
            InlineKeyboardButton("💬 Select Msg", callback_data="gm_select_msg")
        )
        
        # Row 2: Groups, Repeat Interval
        markup.row(
            InlineKeyboardButton("👥 Groups", callback_data="gm_groups_list"),
            InlineKeyboardButton(rep_btn_text, callback_data="gm_repeat_menu")
        )
        
        # Row 3: Import Join Links, Start Operation
        markup.row(
            InlineKeyboardButton("🔗 Import Join Links", callback_data="gm_import_links"),
            InlineKeyboardButton("🚀 Start Operation", callback_data="gm_start_op")
        )
        
        # Row 4: Back to Dashboard
        markup.row(
            InlineKeyboardButton("🔙 Back to Dashboard", callback_data="dash_main")
        )

        status_text = get_mailer_status()
        text = (
            "📬 *GROUP MAILER CONSOLE*\n\n"
            f"{status_text}\n\n"
            "Use the options below to configure and execute your broadcast operation."
        )
        
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data == "gm_select_userbot":
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
            
            markup.add(InlineKeyboardButton(f"{checkbox} {first_name} {username}", callback_data=f"gm_tglub_{c_id}"))
        
        markup.add(InlineKeyboardButton("🔙 Done / Back", callback_data="group_mailer_main"))
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="👤 *Select Userbots to use for broadcast (Multiple selection enabled):*",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_tglub_"):
        ub_id = int(data.split("_")[-1])
        if ub_id in selected_ubs:
            selected_ubs.remove(ub_id)
        else:
            selected_ubs.append(ub_id)
            
        set_setting("gm_selected_userbots", json.dumps(selected_ubs))
        bot.answer_callback_query(call.id, "Preference updated!")
        handle_group_mailer_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': 'gm_select_userbot', 'message': call.message, 'id': call.id})())

    elif data == "gm_groups_list":
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
            show_groups_page(chat_id, message_id, primary_ub, page=0)
        else:
            bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text="⏳ *Fetching groups list from userbot. Please wait...*",
                parse_mode="Markdown"
            )
            def on_fetch_done(fut):
                show_groups_page(chat_id, message_id, primary_ub, page=0)
            
            future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
            future.add_done_callback(on_fetch_done)

    elif data.startswith("gm_refresh_"):
        page = int(data.split("_")[-1])
        if not selected_ubs:
            bot.answer_callback_query(call.id, "❌ Userbot is not selected!")
            return
            
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
            show_groups_page(chat_id, message_id, primary_ub, page)
            
        future = asyncio.run_coroutine_threadsafe(fetch_dialogs_async(client, primary_ub), loop)
        future.add_done_callback(on_sync_done)

    elif data.startswith("gm_page_"):
        page = int(data.split("_")[-1])
        primary_ub = str(selected_ubs[0])
        show_groups_page(chat_id, message_id, primary_ub, page)
        bot.answer_callback_query(call.id)

    elif data.startswith("gm_tgl_"):
        parts = data.split("_")
        g_id = int(parts[2])
        page = int(parts[3])
        
        selected_ids = json.loads(get_setting("gm_selected_group_ids") or "[]")
        if g_id in selected_ids:
            selected_ids.remove(g_id)
        else:
            selected_ids.append(g_id)
            
        set_setting("gm_selected_group_ids", json.dumps(selected_ids))
        primary_ub = str(selected_ubs[0])
        show_groups_page(chat_id, message_id, primary_ub, page)
        bot.answer_callback_query(call.id)

    elif data.startswith("gm_all_sel_"):
        page = int(data.split("_")[-1])
        primary_ub = str(selected_ubs[0])
        groups = userbot_groups_cache.get(primary_ub, [])
        selected_ids = set(json.loads(get_setting("gm_selected_group_ids") or "[]"))
        
        for g in groups:
            selected_ids.add(g["id"])
            
        set_setting("gm_selected_group_ids", json.dumps(list(selected_ids)))
        show_groups_page(chat_id, message_id, primary_ub, page)
        bot.answer_callback_query(call.id, "✅ Selected all groups!")

    elif data.startswith("gm_all_clr_"):
        page = int(data.split("_")[-1])
        primary_ub = str(selected_ubs[0])
        set_setting("gm_selected_group_ids", "[]")
        show_groups_page(chat_id, message_id, primary_ub, page)
        bot.answer_callback_query(call.id, "🗑 Cleared all selections!")

    elif data.startswith("gm_set_loggrp_"):
        admin_states[uid] = "awaiting_gm_update_group"
        bot.send_message(
            chat_id,
            "📢 *SET UPDATE/LOG GROUP*\n\n"
            "Please send the Group Chat ID (e.g. `-1001234567890`) where the status updates, progress report, and failure logs of the Group Mailer should be directed.\n\n"
            "⚠️ *Note:* If you do not configure this, you will not receive any progress updates or failure reports to avoid spamming the bot direct messages."
        )
        bot.answer_callback_query(call.id)

    elif data.startswith("gm_clear_loggrp_"):
        page = int(data.split("_")[-1])
        set_setting("gm_update_group_id", "")
        bot.answer_callback_query(call.id, "❌ Update/Log group removed!")
        primary_ub = str(selected_ubs[0])
        show_groups_page(chat_id, message_id, primary_ub, page)

    elif data == "gm_select_msg":
        admin_states[uid] = "awaiting_gm_message"
        bot.send_message(
            chat_id,
            "💬 *SET MAILER MESSAGE*\n\n"
            "Please send or forward the message you want to broadcast (can be text, photo, video, or document with captions)."
        )
        bot.answer_callback_query(call.id)

    elif data == "gm_import_links":
        if not selected_ubs:
            bot.answer_callback_query(call.id, "❌ Please select at least one Userbot first!", show_alert=True)
            return
            
        admin_states[uid] = "awaiting_gm_links"
        bot.send_message(
            chat_id,
            "🔗 *IMPORT GROUP JOIN LINKS*\n\n"
            "Please send your group links (invite links or usernames, one per line).\n"
            "Example:\n`t.me/+invitehash`\n`@my_group`"
        )
        bot.answer_callback_query(call.id)

    elif data == "gm_repeat_menu":
        markup = InlineKeyboardMarkup()
        markup.row(
            InlineKeyboardButton("❌ Off (Manual)", callback_data="gm_setrep_0"),
            InlineKeyboardButton("30 Min", callback_data="gm_setrep_30")
        )
        markup.row(
            InlineKeyboardButton("1 Hour", callback_data="gm_setrep_60"),
            InlineKeyboardButton("2 Hours", callback_data="gm_setrep_120")
        )
        markup.row(
            InlineKeyboardButton("6 Hours", callback_data="gm_setrep_360"),
            InlineKeyboardButton("12 Hours", callback_data="gm_setrep_720")
        )
        markup.row(
            InlineKeyboardButton("24 Hours", callback_data="gm_setrep_1440")
        )
        markup.add(InlineKeyboardButton("🔙 Back", callback_data="group_mailer_main"))
        
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text="⏰ *SELECT REPEAT INTERVAL*\nConfigure how often the userbot should automatically broadcast the message:",
            reply_markup=markup,
            parse_mode="Markdown"
        )

    elif data.startswith("gm_setrep_"):
        minutes = int(data.split("_")[-1])
        set_setting("gm_repeat_interval", str(minutes))
        bot.answer_callback_query(call.id, "✅ Repeat interval updated!")
        handle_group_mailer_callbacks(type('MockCall', (object,), {'from_user': call.from_user, 'data': 'group_mailer_main', 'message': call.message, 'id': call.id})())

    elif data == "gm_start_op":
        selected_groups = json.loads(get_setting("gm_selected_group_ids") or "[]")
        msg_data = json.loads(get_setting("gm_message") or "{}")

        if not selected_ubs:
            bot.answer_callback_query(call.id, "❌ Please select at least one Userbot first!", show_alert=True)
            return
        if not selected_groups:
            bot.answer_callback_query(call.id, "❌ Please select target groups first!", show_alert=True)
            return
        if not msg_data:
            bot.answer_callback_query(call.id, "❌ Please set the mailer message first!", show_alert=True)
            return

        bot.answer_callback_query(call.id, "🚀 Starting operation...")
        # Dispatch the task safely to the main event loop thread
        asyncio.run_coroutine_threadsafe(
            run_broadcast_failover(selected_ubs, selected_groups, msg_data),
            loop
        )

# Intercept message state inputs for Group Mailer
@bot.message_handler(func=lambda m: is_authorized_manager(m.from_user.id) and admin_states.get(m.from_user.id) in ["awaiting_gm_message", "awaiting_gm_links", "awaiting_gm_update_group"])
def handle_mailer_states(message):
    uid = message.from_user.id
    chat_id = message.chat.id
    state = admin_states.get(uid)
    text = message.text or ""

    if state == "awaiting_gm_message":
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
                local_path = os.path.join(MEDIA_DIR, f"mailer_media_{uid}.{ext}")
                
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
        
        set_setting("gm_message", json.dumps(msg_data))
        admin_states[uid] = None
        bot.reply_to(message, f"✅ *Mailer Message Saved!* (Type: `{msg_type.upper()}`)", parse_mode="Markdown")

    elif state == "awaiting_gm_links":
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        
        selected_ubs = json.loads(get_setting("gm_selected_userbots") or "[]")
        if not selected_ubs:
            bot.reply_to(message, "❌ Please select at least one userbot first to process links.")
            return
            
        client = userbot_fleet_manager.get_client(int(selected_ubs[0]))
        if not client or not client.is_connected():
            bot.reply_to(message, "❌ Selected primary userbot is offline. Cannot check invite links.")
            return

        bot.reply_to(message, "⏳ Processing and resolving join links...")
        
        links_map = json.loads(get_setting("gm_group_links_map") or "{}")
        selected_groups = json.loads(get_setting("gm_selected_group_ids") or "[]")
        
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
                        links_map[str(final_id)] = line
                        if final_id not in selected_groups:
                            selected_groups.append(final_id)
                        success_count += 1
                except Exception as e:
                    logger.error(f"Error resolving line {line}: {e}")

            set_setting("gm_group_links_map", json.dumps(links_map))
            set_setting("gm_selected_group_ids", json.dumps(selected_groups))
            admin_states[uid] = None
            bot.send_message(
                chat_id,
                f"✅ *Links Processed!*\nSuccessfully resolved and added `{success_count}` groups to your selections.",
                parse_mode="Markdown"
            )

        asyncio.run_coroutine_threadsafe(resolve_links_task(), loop)

    elif state == "awaiting_gm_update_group":
        cleaned_id = text.strip()
        
        # Verify the format looks like a Telegram group ID
        if not (cleaned_id.startswith("-") and cleaned_id.replace("-", "").isdigit()):
            bot.reply_to(message, "❌ *Invalid Group ID!*\nGroup IDs must start with a minus (e.g. `-1001234567890`). Please try again.")
            return
            
        set_setting("gm_update_group_id", cleaned_id)
        admin_states[uid] = None
        bot.reply_to(
            message,
            f"✅ *Update/Log Group Configured!*\nAll future mailer updates and failure reports will be sent to the group ID: `{cleaned_id}`.\n\n"
            "⚠️ *Notice:* You must ensure the Admin Bot is added as a member in that group to successfully send updates.",
            parse_mode="Markdown"
        )

# Asynchronous broadcast implementation with Multi-Userbot Failover, Auto-Join, and Log Group Redirection
async def run_broadcast_failover(ub_ids, initial_group_ids, msg_data, is_auto=False):
    success = 0
    failed = 0
    
    label = "⏰ Scheduled Mailer" if is_auto else "📬 Group Mailer"
    
    # Retrieve configuration for Log/Update Group
    update_group_id = get_setting("gm_update_group_id")
    dest_chat = int(update_group_id) if (update_group_id and update_group_id.strip()) else None

    progress_msg = None
    if dest_chat:
        try:
            progress_msg = bot.send_message(dest_chat, f"⏳ *{label} progress:* `0%`", parse_mode="Markdown")
        except Exception as err:
            logger.error(f"Failed to send initial progress update to log group {dest_chat}: {err}")

    # Save last run timestamp
    set_setting("gm_last_run", str(time.time()))
    
    # Store processed IDs in this run
    sent_group_ids = set()
    
    # Keep track of detailed failures for reporting
    failed_details = []

    # Map to resolve group title from cached groups
    group_titles = {}
    for ub_id in ub_ids:
        for g in userbot_groups_cache.get(str(ub_id), []):
            group_titles[g["id"]] = g["title"]

    while True:
        # Fetch live settings on EVERY iteration
        live_group_ids = json.loads(get_setting("gm_selected_group_ids") or "[]")
        links_map = json.loads(get_setting("gm_group_links_map") or "{}")
        
        # Determine which selected groups haven't been processed yet
        remaining_groups = [g for g in live_group_ids if g not in sent_group_ids]
        
        if not remaining_groups:
            break
            
        group_id = remaining_groups[0]
        sent_group_ids.add(group_id)
        
        group_sent_successfully = False
        group_errors = []

        # Failover send loop across all selected userbots in order
        for ub_id in ub_ids:
            client = userbot_fleet_manager.get_client(int(ub_id))
            if not client or not client.is_connected():
                group_errors.append((ub_id, "Userbot client is offline or disconnected"))
                continue

            try:
                entity = group_id
                
                # Resolve entity for target
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

                msg_type = msg_data.get("type")
                if msg_type == "text":
                    await client.send_message(entity, msg_data["text"])
                elif msg_type in ["photo", "video", "document"]:
                    await client.send_file(entity, msg_data["local_path"], caption=msg_data.get("caption", ""))
                
                group_sent_successfully = True
                break # Success! Exit failover loop for this group
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
                        break # Success! Exit failover loop for this group
                except Exception as retry_err:
                    e = retry_err
                
                group_errors.append((ub_id, e))
                logger.warning(f"Userbot {ub_id} failed to send to {group_id}: {e}")
        
        if group_sent_successfully:
            success += 1
        else:
            failed += 1
            g_title = group_titles.get(group_id, f"ID: {group_id}")
            report_lines = [f"❌ *Group:* `{g_title}`"]
            for ub_id, err in group_errors:
                friendly_desc = get_friendly_error(err)
                report_lines.append(f"  ⚠️ *UB {ub_id}:* {friendly_desc}")
            failed_details.append("\n".join(report_lines))

        # Live progress updates (Only if Log/Update Group is configured)
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

        # Random delay between 5 to 10 seconds to protect account
        delay = random.randint(5, 10)
        await asyncio.sleep(delay)

    if dest_chat:
        try:
            bot.send_message(
                dest_chat,
                f"✅ *{label} Completed!*\n\n🟢 Success: `{success}`\n🔴 Failed: `{failed}`",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    # Send Detailed Failure Report (Only if Log/Update Group is configured)
    if failed_details and dest_chat:
        try:
            header = f"🚨 *{label} Failure Report:*\nThe message could not be sent to the following groups on all selected accounts:\n\n"
            current_message = header
            
            for report in failed_details:
                if len(current_message) + len(report) + 2 > 4000:
                    bot.send_message(dest_chat, current_message, parse_mode="Markdown")
                    current_message = ""
                current_message += report + "\n\n"
                
            if current_message.strip():
                bot.send_message(dest_chat, current_message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"Error sending failure report to log group {dest_chat}: {e}")

# Background Scheduled Supervisor Loop
async def scheduler_loop():
    logger.info("⏰ Group Mailer background scheduler loop running...")
    while True:
        try:
            interval_minutes = int(get_setting("gm_repeat_interval") or "0")
            if interval_minutes > 0:
                last_run = float(get_setting("gm_last_run") or "0")
                now = time.time()
                
                if now - last_run >= (interval_minutes * 60):
                    selected_ubs = json.loads(get_setting("gm_selected_userbots") or "[]")
                    selected_groups = json.loads(get_setting("gm_selected_group_ids") or "[]")
                    msg_data = json.loads(get_setting("gm_message") or "{}")
                    
                    if selected_ubs and selected_groups and msg_data:
                        has_active_client = False
                        for ub_id in selected_ubs:
                            client = userbot_fleet_manager.get_client(int(ub_id))
                            if client and client.is_connected():
                                has_active_client = True
                                break
                                
                        if has_active_client:
                            # Execute scheduled broadcast
                            await run_broadcast_failover(selected_ubs, selected_groups, msg_data, is_auto=True)
        except Exception as e:
            logger.error(f"Error in Group Mailer scheduler loop: {e}")
            
        await asyncio.sleep(30)  # Check every 30 seconds

# Start the background schedule task safely
asyncio.run_coroutine_threadsafe(scheduler_loop(), loop)
