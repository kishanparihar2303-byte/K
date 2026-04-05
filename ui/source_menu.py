import time
import asyncio
from telethon import events, Button, errors, TelegramClient
from telethon.sessions import StringSession
from config import bot, get_default_forward_rules, API_ID, API_HASH, logger
from database import get_user_data, save_persistent_db, user_sessions
from utils import get_display_name


# ==========================================
# 🔧 AUTO-RECONNECT HELPER
# ==========================================

async def _get_or_reconnect_client(user_id: int, data: dict):
    """
    Bot restart ke baad user_sessions empty ho jaata hai.
    Ye function check karta hai:
      1. Agar user_sessions mein connected client hai → wahi return karo
      2. Agar DB mein session string hai → naya client banao, connect karo,
         user_sessions mein store karo, wapis do
      3. Kuch nahi mila → None return karo
    """
    # Already connected client
    existing = user_sessions.get(user_id)
    if existing:
        try:
            if existing.is_connected():
                return existing
        except Exception:
            pass

    # Session string DB mein hai?
    session_str = data.get("session")
    if not session_str:
        return None

    # Naya client banao session se
    try:
        from config import ENTITY_CACHE_LIMIT
    except ImportError:
        ENTITY_CACHE_LIMIT = 50

    try:
        client = TelegramClient(
            StringSession(session_str), API_ID, API_HASH,
            connection_retries=3,
            retry_delay=1,
            auto_reconnect=True,
            entity_cache_limit=ENTITY_CACHE_LIMIT,
            flood_sleep_threshold=20,
            request_retries=2,
        )
        await client.connect()
        if not await client.is_user_authorized():
            # Session expire ho gayi — clear karo
            data["session"] = None
            data.setdefault("settings", {})["running"] = False
            save_persistent_db(force_mongo=True)
            await client.disconnect()
            return None
        # Save to user_sessions so future calls use this client
        user_sessions[user_id] = client
        return client
    except Exception as e:
        try:
            from config import logger
            logger.warning(f"[PIN] Auto-reconnect failed for {user_id}: {e}")
        except Exception:
            pass
        return None

# ==========================================
# 🚨 HELPER FUNCTIONS
# ==========================================

def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_src_by_index(user_id, index):
    data = get_user_data(user_id)
    try:
        return data["sources"][int(index)]
    except (IndexError, ValueError):
        return None

def get_index_by_src(user_id, src_id):
    data = get_user_data(user_id)
    try:
        # Try finding exact match first
        return data["sources"].index(src_id)
    except ValueError:
        try:
            str_sources = [str(s) for s in data["sources"]]
            return str_sources.index(str(src_id))
        except ValueError:
            return None

# ==========================================
# PINNED CHATS LOGIC
# ==========================================

async def show_pinned_chats(event, mode):
    user_id = event.sender_id
    data = get_user_data(user_id)

    # ✅ FIX: Bot restart ke baad user_sessions empty ho jaata hai.
    # Pehle auto-reconnect try karo — sirf tab "login first" dikhao jab
    # sach mein session nahi hai ya session expire ho gayi ho.
    client = await _get_or_reconnect_client(user_id, data)

    if not client:
        from .main_menu import get_main_buttons
        return await event.respond(
            "❌ Please login first." + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""),
            buttons=get_main_buttons(user_id)
        )
    try:
        dialogs = await client.get_dialogs()
        pinned = [d for d in dialogs if d.pinned]
        if not pinned:
            return await event.respond("❌ No pinned chats found." + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "", buttons=[Button.inline("🏠 Menu", b"main_menu")])
        buttons = []
        # ✅ FIX: Use channel_already_exists() for smart "Already Added" detection
        # Works even when same channel was stored as invite link vs numeric ID
        from utils import channel_already_exists
        names_id = data.get("channel_names_id", {})
        target_list = data["sources"] if mode == 'src' else data["destinations"]
        for dialog in pinned:
            chat_id = str(dialog.id)
            # Smart duplicate check: numeric ID vs invite-link cross-match
            is_added = channel_already_exists(chat_id, target_list, names_id)
            status = " ✅" if is_added else ""
            name = dialog.name[:30]
            buttons.append([Button.inline(f"{name}{status}", f"addpin_{mode}_{chat_id}".encode())])
        buttons.append([Button.inline("🏠 Back to Menu", b"main_menu")])
        await event.respond(f"📌 Select a Pinned Chat as **{mode.upper()}**:" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""), buttons=buttons)
    except Exception as e:
        await event.respond(f"❌ Error: {str(e)[:80]}")

@bot.on(events.CallbackQuery(pattern=b"addpin_"))
async def handle_add_pin_callback(event):
    await event.answer()
    user_id   = event.sender_id
    data      = get_user_data(user_id)
    parts     = event.data.decode().split("_", 2)
    mode      = parts[1]
    chat_id   = parts[2]

    target_list = data["sources"] if mode == "src" else data["destinations"]

    # ✅ FIX: Comprehensive duplicate check using channel_already_exists()
    # Handles: same numeric ID, same invite link, AND invite-link ↔ numeric-ID cross-match
    from utils import sources_match, channel_already_exists
    names_id = data.get("channel_names_id", {})

    if channel_already_exists(chat_id, target_list, names_id):
        await event.answer("⚠️ Ye channel pehle se add hai!", alert=True)
        return

    # Loop prevention — also uses smart matching
    if mode == "src":
        if channel_already_exists(chat_id, data.get("destinations", []), names_id):
            await event.answer(
                "⚠️ Loop Warning! Ye channel already Destination hai.\n"
                "Pehle destination se remove karo.",
                alert=True
            )
            return
    else:
        if channel_already_exists(chat_id, data.get("sources", []), names_id):
            await event.answer(
                "⚠️ Loop Warning! Ye channel already Source hai.\n"
                "Pehle source se remove karo.",
                alert=True
            )
            return

    # ✅ FIX: Auto-replace any stale invite-link entry with the correct numeric ID.
    # Example: user added "+HashXXX" earlier (link failed to resolve) — now PIN add
    # gives us the real numeric ID. Replace the bad entry silently.
    for entry in list(target_list):
        entry_str = str(entry)
        # Invite-link entry whose resolved ID matches this chat_id?
        if entry_str in names_id:
            cached_resolved = str(names_id[entry_str])
            if sources_match(cached_resolved, chat_id) or sources_match(entry_str, chat_id):
                target_list.remove(entry)
                if mode == "src":
                    data.get("custom_forward_rules", {}).pop(entry_str, None)
                break

    # Title cache
    try:
        from utils import get_display_name
        client = await _get_or_reconnect_client(user_id, data)
        if client:
            title = await get_display_name(client, int(chat_id), user_id)
            data.setdefault("channel_names", {})[str(chat_id)] = title
    except Exception:
        pass

    target_list.append(chat_id)
    save_persistent_db(force_mongo=True)
    # ✅ FIX: Directly await MongoDB save — fire-and-forget se data loss hota tha restart par
    try:
        from database import save_to_mongo as _stm
        await _stm()
        logger.info(f"[DB] Source/Dest add — MongoDB await save done for user {user_id}")
    except Exception as _me:
        logger.warning(f"[DB] MongoDB direct save error (non-fatal): {_me}")
    await event.answer("✅ Added!")
    await event.delete()
    await show_pinned_chats(event, mode)

# ==========================================
# SOURCE MANAGEMENT MENUS
# ==========================================

@bot.on(events.CallbackQuery(data=b"add_src"))
async def add_src_cb(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    if not data["session"]:
        return await event.answer("⚠️ Pehle login karo!", alert=True)
    srcs  = data.get("sources", [])
    count = len(srcs)
    try:
        await event.edit(
            "📥 **SOURCE MANAGEMENT**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Active sources: **{count}**\n\n"
            "**Source kya hota hai?**\n"
            "_Jis channel/group se messages copy honge_\n\n"
            "Kaise add karein:",
            buttons=[
                [Button.inline("📌 Pinned Chats se Choose",  b"pin_src_flow")],
                [Button.inline("✏️ Manually ID/Link daalo",   b"man_src_flow")],
                [Button.inline(f"📋 Manage Sources ({count})", b"list_src_0")],
                [Button.inline("🔙 Main Menu",                 b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"pin_src_flow"))
async def pin_src_flow(event):
    await event.answer()
    await event.delete()
    await show_pinned_chats(event, 'src')

@bot.on(events.CallbackQuery(data=b"man_src_flow"))
async def man_src_flow(event):
    await event.answer()
    get_user_data(event.sender_id)["step"] = "wait_src_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **Source Add Karo**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Inme se koi bhi bhejo:\n\n"
            "• `@username` — public channel\n"
            "• `https://t.me/channelname` — channel link\n"
            "• `https://t.me/+XXXXX` — private invite link\n"
            "• `-1001234567890` — direct channel ID\n\n"
            "💡 **Multiple sources ek saath add karo!**\n"
            "Har ID/link alag line mein ya comma se alag karke bhejo.\n\n"
            "**Private channel ke liye:** pehle apne account se join karo.",
            buttons=[[Button.inline("🔙 Back", b"add_src")]]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"rem_src"))
async def rem_src_handler(event):
    await event.answer()
    await list_sources_handler(event, 0)

@bot.on(events.CallbackQuery(pattern=b"list_src_"))
async def list_sources_handler(event, page=None):
    await event.answer()
    data_str = event.data.decode()
    is_delete_mode = "del" in data_str
    
    if page is None: 
        try: page = int(data_str.split("_")[-1])
        except: page = 0
        
    user_id = event.sender_id
    data = get_user_data(user_id)
    sources = data["sources"]
    
    back_btn_data = b"main_menu" if is_delete_mode else b"add_src"
    
    if not sources:
        try:
            return await event.edit("❌ No sources added.", buttons=[[Button.inline("🔙 Back", back_btn_data)]])
        except errors.MessageNotModifiedError: return

    MAX = 5
    start = page * MAX
    end = start + MAX
    subset = sources[start:end]
    
    if not subset and page > 0:
        return await list_sources_handler(event, page=page-1)

    client = user_sessions.get(user_id)
    buttons = []
    
    mode_flag = "del" if is_delete_mode else "view"
    
    for i, s in enumerate(subset):
        idx = start + i
        name = str(s)
        if client:
            name = await get_display_name(client, s, user_id)
        else:
            # Cache se try karo even without client
            cached = data.get("channel_names", {}).get(str(s))
            if cached:
                name = cached
            elif "t.me/+" in str(s) or "t.me/joinchat" in str(s) or str(s).startswith("+"):
                # ✅ FIX: For invite-link entries, show resolved channel name via channel_names_id
                resolved_id = data.get("channel_names_id", {}).get(str(s))
                if resolved_id:
                    cached_by_id = data.get("channel_names", {}).get(str(resolved_id))
                    name = cached_by_id if cached_by_id else "🔒 Private Channel"
                else:
                    name = "🔒 Private Channel"
        buttons.append([Button.inline(f"{name}", f"view_src_{mode_flag}_{idx}".encode())])
        
    nav = []
    prefix = f"list_src_{mode_flag}"
    if page > 0: nav.append(Button.inline("⬅️ Prev", f"{prefix}_{page-1}".encode()))
    if end < len(sources): nav.append(Button.inline("Next ➡️", f"{prefix}_{page+1}".encode()))
    if nav: buttons.append(nav)
    
    if page > 0:
        buttons.append([Button.inline("🔙 Back", f"{prefix}_{page-1}".encode())])
    else:
        buttons.append([Button.inline("🔙 Back", back_btn_data)])
        
    try:
        await event.edit("📋 **Source List**\nSelect to View/Delete:", buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"view_src_"))
async def view_src_item(event):
    await event.answer()
    parts = event.data.decode().split("_")
    mode = parts[2]
    idx = int(parts[3])
    
    data = get_user_data(event.sender_id)
    if idx >= len(data["sources"]): return await event.answer("Not found")
    
    val = data["sources"][idx]
    
    page = idx // 5
    back_data = f"list_src_{mode}_{page}".encode()
    del_data = f"del_src_{mode}_{idx}".encode()
    
    uid    = event.sender_id
    client = user_sessions.get(uid)
    name   = data.get("channel_names", {}).get(str(val))
    if not name and client:
        try:
            name = await get_display_name(client, val, uid)
        except Exception:
            name = str(val)
    if not name:
        name = "🔒 Private Channel" if ("t.me/+" in str(val) or "t.me/joinchat" in str(val)) else str(val)

    try:
        await event.edit(
            f"📺 **Source Details**\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"📛 Name: **{name}**\n"
            f"🆔 ID: `{val}`",
            buttons=[
                [Button.inline("🗑 Delete", del_data)],
                [Button.inline("🔙 Back", back_data)]
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"del_src_"))
async def del_src_item(event):
    await event.answer()
    try:
        parts = event.data.decode().split("_")
        mode = parts[2]
        idx  = int(parts[3])
    except (IndexError, ValueError):
        return await event.answer("❌ Button outdated — please refresh.", alert=True)

    data = get_user_data(event.sender_id)

    if idx >= len(data["sources"]):
        await event.answer("❌ Item already deleted or list changed!", alert=True)
        return await list_sources_handler(event, page=0)

    # FIX 19: Get current page before deleting
    MAX_PER_PAGE = 5
    current_page = idx // MAX_PER_PAGE

    val = data["sources"][idx]
    del data["sources"][idx]

    str_val = str(val)
    if str_val in data.get("custom_forward_rules", {}):
        del data["custom_forward_rules"][str_val]

    save_persistent_db(force_mongo=True)
    # ✅ FIX: Directly await MongoDB save
    try:
        from database import save_to_mongo as _stm
        await _stm()
        logger.info(f"[DB] Source delete — MongoDB await save done for user {event.sender_id}")
    except Exception as _me:
        logger.warning(f"[DB] MongoDB direct save error: {_me}")
    # FIX 19: If current page now empty, go to previous page
    if current_page > 0 and current_page * MAX_PER_PAGE >= len(data["sources"]):
        current_page -= 1
    await event.answer("🗑 Source Removed!")
    
    target_page = idx // 5
    if target_page * 5 >= len(data["sources"]) and target_page > 0:
        target_page -= 1
        
    await list_sources_handler(event, page=target_page)

# ==========================================
# CUSTOM SOURCE CONFIG & PER-DEST RULES
# ==========================================

async def render_custom_src_menu(event, idx):
    user_id = event.sender_id
    src_id_raw = get_src_by_index(user_id, idx)
    if not src_id_raw:
        return await event.answer("❌ Source nahi mila (shayad delete ho gaya?)", alert=True)

    src_id = str(src_id_raw)
    data = get_user_data(user_id)
    rules_db = data.setdefault("custom_forward_rules", {})
    src_rules = rules_db.setdefault(src_id, {"default": get_default_forward_rules()})["default"]

    client = user_sessions.get(user_id)
    display_name = src_id
    if client:
        display_name = await get_display_name(client, src_id_raw)

    def on_off(val):
        return "🟢" if val else "🔴"

    prefix_st = f"`{src_rules['prefix'][:20]}`" if src_rules.get("prefix") else "❌ Set nahi"
    suffix_st = f"`{src_rules['suffix'][:20]}`" if src_rules.get("suffix") else "❌ Set nahi"
    link_mode = src_rules.get("link_mode", "keep").upper()
    link_emoji = {"KEEP": "✅", "REPLACE": "🔄", "REMOVE": "🚫"}.get(link_mode, "✅")

    txt = (
        f"📍 **Src Config: {display_name[:30]}**\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        "🔑 **Ye settings sirf IS source ke liye hain**\n"
        "Global Settings se ye override ho jaati hain\n\n"
        f"📨 **Kya bhejein:**\n"
        f"  Text {on_off(src_rules['forward_text'])}  "
        f"Photo {on_off(src_rules['forward_photos'])}  "
        f"Video {on_off(src_rules['forward_videos'])}\n"
        f"  File {on_off(src_rules['forward_files'])}  "
        f"Voice {on_off(src_rules.get('forward_voice', True))}\n\n"
        f"🔗 **Links:** {link_emoji} {link_mode}\n"
        f"✏️ **Upar ka text (Prefix):** {prefix_st}\n"
        f"✏️ **Neeche ka text (Suffix):** {suffix_st}\n\n"
        f"📤 **Per-Destination Rules:** Alag dest ke liye alag rules\n\n" + _get_owner_footer()
    )


    # Source enabled status
    _src_enabled = True
    try:
        _src_id_for_check = str(get_src_by_index(uid, idx))
        _src_rules_check  = data.get("custom_forward_rules", {}).get(
            _src_id_for_check, {}).get("default", {})
        _src_enabled = _src_rules_check.get("src_enabled", True)
    except Exception:
        pass

    buttons = [
        # Most used options first
        [Button.inline("🔗 Link Mode Badlo", f"cs_lnk_{idx}_def".encode())],

        [Button.inline("✏️ Upar ka Text (Prefix)", f"cs_pre_menu_{idx}".encode()),
         Button.inline("✏️ Neeche ka Text (Suffix)", f"cs_suf_menu_{idx}".encode())],
        [Button.inline(
            f"{'⏸️ Pause Source' if _src_enabled else '▶️ Resume Source'}",
            f"src_toggle_enabled_{idx}".encode()
        )],
        [Button.inline("─── Media Toggle ───", f"custom_src_idx_{idx}".encode())],
        [Button.inline(f"📝 Text {on_off(src_rules['forward_text'])}", f"cs_txt_{idx}_def".encode()),
         Button.inline(f"🖼 Photo {on_off(src_rules['forward_photos'])}", f"cs_pho_{idx}_def".encode())],
        [Button.inline(f"🎥 Video {on_off(src_rules['forward_videos'])}", f"cs_vid_{idx}_def".encode()),
         Button.inline(f"📁 File {on_off(src_rules['forward_files'])}", f"cs_fil_{idx}_def".encode())],
        [Button.inline(f"🎙 Voice {on_off(src_rules.get('forward_voice', True))}", f"cs_voi_{idx}_def".encode()),
         Button.inline(f"🎨 Media Mode: {src_rules['media_mode']}", f"cs_med_{idx}_def".encode())],
        [Button.inline("─── More Options ───", f"custom_src_idx_{idx}".encode())],
        [Button.inline("🏷 Username Replace/Edit", f"cs_usr_menu_{idx}".encode())],
        [Button.inline("#️⃣ Hashtags Add Karo", f"cs_hsh_add_menu_{idx}".encode())],
        [Button.inline("📤 Per-Destination Rules →", f"cs_dest_list_{idx}".encode())],
        [Button.inline("🗑 Sab Reset Karo", f"cs_rst_{idx}".encode())],
        [Button.inline("🔙 Sources List", b"ps_menu")]
    ]
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass
    except Exception:
        pass

@bot.on(events.CallbackQuery(pattern=b"custom_src_idx_"))
async def custom_src_menu(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    await render_custom_src_menu(event, idx)

@bot.on(events.CallbackQuery(data=b"ps_menu"))
async def ps_menu(event):
    await event.answer()
    from force_subscribe import check_force_subscribe_cb, get_fs_config
    if get_fs_config().get("enabled"):
        if not await check_force_subscribe_cb(event): return
    try:
        if not get_user_data(event.sender_id)["session"]:
            return await event.answer("⚠️ Please Login First!", alert=True)
        user_id = event.sender_id
        data = get_user_data(user_id)
        if not data["sources"]:
            return await event.edit(
                "❌ Koi source nahi hai abhi.\n\nPehle source add karo." + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "",
                buttons=[Button.inline("🏠 Menu", b"main_menu")]
            )

        buttons = []
        client = user_sessions.get(user_id)
        # BUG 19 FIX: Batch display name fetch with concurrency limit
        if client:
            import asyncio as _asyncio
            async def _fetch_name(src, idx):
                try:
                    return idx, await get_display_name(client, src)
                except Exception:
                    return idx, str(src)
            tasks = [_fetch_name(src, i) for i, src in enumerate(data["sources"])]
            results = await _asyncio.gather(*tasks, return_exceptions=True)
            names = {}
            for r in results:
                if isinstance(r, tuple):
                    names[r[0]] = r[1]
        else:
            names = {i: str(src) for i, src in enumerate(data["sources"])}

        for i, src in enumerate(data["sources"]):
            display = names.get(i, str(src))
            has_rules = str(src) in data.get("custom_forward_rules", {})
            mark = "⚙️" if has_rules else "📌"
            buttons.append([Button.inline(f"{mark} {display[:35]}", f"custom_src_idx_{i}".encode())])

        buttons.append([Button.inline("❓ Src Config Kya Hai?", b"help_srcconfig")])
        buttons.append([Button.inline("🏠 Main Menu", b"main_menu")])

        custom_count = sum(
            1 for s in data["sources"]
            if str(s) in data.get("custom_forward_rules", {})
        )
        txt = (
            "📍 **SOURCE CONFIG**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Total sources: **{len(data['sources'])}**  ·  Custom rules: **{custom_count}**\n\n"
            "Yahan har source ke liye **alag alag rules** set kar sakte ho\n"
            "_(prefix, suffix, link mode, per-destination rules)_\n\n"
            "**⚙️** = Custom rules set hain\n"
            "**📌** = Global settings follow ho rahi hain\n\n"
            "Source chuniye:"
        )
        try:
            await event.edit(txt, buttons=buttons)
        except errors.MessageNotModifiedError:
            pass
    except errors.MessageNotModifiedError:
        await event.answer("Refreshed!")

# ==========================================
# PER-DESTINATION RULES LOGIC
# ==========================================

@bot.on(events.CallbackQuery(pattern=b"cs_dest_list_"))
async def cs_dest_list(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    user_id = event.sender_id
    src_id_raw = get_src_by_index(user_id, idx)
    if not src_id_raw: return
    
    data = get_user_data(user_id)
    if not data["destinations"]:
        return await event.answer("❌ Pehle destination add karo!", alert=True)

    client = user_sessions.get(user_id)
    buttons = []
    src_id = str(src_id_raw)
    src_name = str(src_id_raw)
    if client:
        src_name = await get_display_name(client, src_id_raw)

    for i, dest in enumerate(data["destinations"]):
        d_name = str(dest)
        if client: d_name = await get_display_name(client, dest)
        has_custom = str(dest) in data.get("custom_forward_rules", {}).get(src_id, {})
        mark = "⚙️" if has_custom else "🌐"
        buttons.append([Button.inline(f"{mark} {d_name[:35]}", f"cs_dest_edit_{idx}_{i}".encode())])

    buttons.append([Button.inline("❓ Per-Dest Rules Kya Hai?", b"help_per_dest")])
    buttons.append([Button.inline("🔙 Source Settings", f"custom_src_idx_{idx}".encode())])

    try:
        await event.edit(
            f"📤 **Per-Destination Rules**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Source: **{src_name[:30]}**\n\n"
            f"Har destination ke liye **alag rules** set kar sakte ho.\n"
            f"Jaise Dest A ko sirf photo, Dest B ko sirf text.\n\n"
            f"⚙️ = Custom rules set hain\n"
            f"🌐 = Source ki default settings use ho rahi hain\n\n"
            f"Destination chunno:",
            buttons=buttons
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"cs_dest_edit_"))
async def cs_dest_edit(event, src_idx=None, dest_idx=None):
    await event.answer()
    if src_idx is None or dest_idx is None:
        parts = event.data.decode().split("_")
        src_idx = int(parts[3])
        dest_idx = int(parts[4])
    
    user_id = event.sender_id
    src_id_raw = get_src_by_index(user_id, src_idx)
    data = get_user_data(user_id)
    if dest_idx >= len(data["destinations"]): return
    dest_id_raw = data["destinations"][dest_idx]
    
    src_id = str(src_id_raw)
    dest_id = str(dest_id_raw)
    
    rules_db = data.setdefault("custom_forward_rules", {})
    src_entry = rules_db.setdefault(src_id, {"default": get_default_forward_rules()})
    
    if dest_id not in src_entry:
        src_entry[dest_id] = src_entry["default"].copy()
        src_entry[dest_id]["prefix"] = ""
        src_entry[dest_id]["suffix"] = ""
        save_persistent_db(force_mongo=True)
        
    rules = src_entry[dest_id]
    
    client = user_sessions.get(user_id)
    d_name = str(dest_id)
    if client: d_name = await get_display_name(client, dest_id)

    def on_off(val):
        return "🟢" if val else "🔴"

    prefix_st = f"`{rules.get('prefix')[:20]}`" if rules.get("prefix") else "❌ Set nahi"
    suffix_st = f"`{rules.get('suffix')[:20]}`" if rules.get("suffix") else "❌ Set nahi"
    media_mode = rules.get('media_mode', 'original').upper()

    # Extra rule fields with defaults
    link_mode_d  = rules.get("link_mode", "keep").upper()
    cap_custom   = rules.get("custom_caption") or None
    cap_st       = f"`{cap_custom[:25]}...`" if cap_custom and len(cap_custom) > 25 else (f"`{cap_custom}`" if cap_custom else "❌ Set nahi")
    rm_links_d   = rules.get("remove_links", False)
    rm_user_d    = rules.get("remove_usernames", False)
    rm_hash_d    = rules.get("remove_hashtags", False)
    repl_count   = len(rules.get("replace_map", {}))

    # New fields
    dest_enabled  = rules.get("dest_enabled", True)
    copy_mode_d   = rules.get("copy_mode", False)
    pin_fwd_d     = rules.get("pin_forwarded", False)
    fail_count    = rules.get("fail_count", 0)
    dis_reason    = rules.get("disabled_reason", "")
    health_warn   = ""
    if not dest_enabled:
        health_warn = f"\n⚠️ **DISABLED** — {dis_reason or 'manually'}"
    elif fail_count >= 3:
        health_warn = f"\n⚠️ Fail count: `{fail_count}` (auto-disable at 5)"

    txt = (
        f"⚙️ **Per-Dest Rules — {d_name[:28]}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: {'✅ Active' if dest_enabled else '⏸️ Disabled'}{health_warn}\n\n"
        f"**📨 Media Types:**\n"
        f"  {'🟢' if rules['forward_text'] else '🔴'} Text  "
        f"{'🟢' if rules['forward_photos'] else '🔴'} Photo  "
        f"{'🟢' if rules['forward_videos'] else '🔴'} Video\n"
        f"  {'🟢' if rules['forward_files'] else '🔴'} File  "
        f"{'🟢' if rules.get('forward_voice',True) else '🔴'} Voice\n\n"
        f"**🎨 Media Mode:** `{media_mode}`  **🔗 Link:** `{link_mode_d}`\n"
        f"**📋 Copy Mode (no fwd tag):** {'✅' if copy_mode_d else '❌'}  "
        f"**📌 Pin fwd:** {'✅' if pin_fwd_d else '❌'}\n\n"
        f"**✍️ Text:**\n"
        f"  {'🔴' if rm_links_d else '🟢'} Links  "
        f"{'🔴' if rm_user_d else '🟢'} @Username  "
        f"{'🔴' if rm_hash_d else '🟢'} #Hashtag\n"
        f"  🔄 Replacements: `{repl_count}` set\n\n"
        f"**✏️ Prefix:** {prefix_st}  **Suffix:** {suffix_st}\n"
        f"**📋 Custom Caption:** {cap_st}\n"
    )

    suffix = f"{src_idx}_{dest_idx}"

    buttons = [
        # Media type row
        [Button.inline(f"{'🟢' if rules['forward_text'] else '🔴'} Text",
                       f"cst_txt_{suffix}".encode()),
         Button.inline(f"{'🟢' if rules['forward_photos'] else '🔴'} Photo",
                       f"cst_pho_{suffix}".encode()),
         Button.inline(f"{'🟢' if rules['forward_videos'] else '🔴'} Video",
                       f"cst_vid_{suffix}".encode())],
        [Button.inline(f"{'🟢' if rules['forward_files'] else '🔴'} File",
                       f"cst_fil_{suffix}".encode()),
         Button.inline(f"{'🟢' if rules.get('forward_voice',True) else '🔴'} Voice",
                       f"cst_voi_{suffix}".encode())],
        # Media mode
        [Button.inline(f"🎨 Media: {media_mode}", f"cst_med_{suffix}".encode()),
         Button.inline(f"🔗 Link: {link_mode_d}", f"cst_lnk_{suffix}".encode())],
        # Remove toggles
        [Button.inline(f"{'🔴' if rm_links_d else '🟢'} Links",
                       f"cst_rml_{suffix}".encode()),
         Button.inline(f"{'🔴' if rm_user_d else '🟢'} @Username",
                       f"cst_rmu_{suffix}".encode()),
         Button.inline(f"{'🔴' if rm_hash_d else '🟢'} #Hashtag",
                       f"cst_rmh_{suffix}".encode())],
        # Text edits
        [Button.inline("✏️ Prefix", f"cst_pre_menu_{suffix}".encode()),
         Button.inline("✏️ Suffix", f"cst_suf_menu_{suffix}".encode()),
         Button.inline("📋 Caption", f"cst_cap_{suffix}".encode())],
        # New: Copy mode, Pin, Dest toggle
        [Button.inline(f"{'✅' if rules.get('copy_mode') else '❌'} Copy Mode",
                        f"cst_copy_toggle_{suffix}".encode()),
         Button.inline(f"{'✅' if rules.get('pin_forwarded') else '❌'} Pin Fwd",
                        f"cst_pin_toggle_{suffix}".encode())],
        [Button.inline(f"{'⏸️ Disable' if dest_enabled else '▶️ Enable'} Dest",
                        f"cst_dest_toggle_{suffix}".encode()),
         Button.inline("🔄 Reset Fail Count", f"cst_dest_health_reset_{suffix}".encode())],
        # Replacements
        [Button.inline(f"🔄 Replacements ({repl_count})", f"cst_repl_{suffix}".encode())],
        # Actions
        [Button.inline("🗑 Reset Default", f"cst_rst_{suffix}".encode())],
        [Button.inline("🔙 Back", f"cs_dest_list_{src_idx}".encode())]
    ]
    
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

# 🚨 FIX: STRICT REGEX PATTERN TO AVOID CONFLICT WITH NAV BUTTONS
@bot.on(events.CallbackQuery(pattern=b"cst_(txt|pho|vid|fil|voi|med|lnk|rml|rmu|rmh)_|^cst_rst_\\d+_\\d+$"))
async def toggle_dest_rule(event):
    await event.answer()
    data_str = event.data.decode()
    parts = data_str.split("_")
    action = parts[1] # txt, pho, vid, etc
    src_idx = int(parts[2])
    dest_idx = int(parts[3])
    
    user_id = event.sender_id
    data = get_user_data(user_id)
    
    src_id = str(get_src_by_index(user_id, src_idx))
    dest_id = str(data["destinations"][dest_idx])
    
    rules = data["custom_forward_rules"][src_id][dest_id]
    
    if action == "txt": rules["forward_text"] = not rules["forward_text"]
    elif action == "pho": rules["forward_photos"] = not rules["forward_photos"]
    elif action == "vid": rules["forward_videos"] = not rules["forward_videos"]
    elif action == "fil": rules["forward_files"] = not rules["forward_files"]
    elif action == "voi": rules["forward_voice"] = not rules.get("forward_voice", True)
    elif action == "med":
        modes = ["original", "as_document", "skip"]
        current = rules.get("media_mode", "original")
        idx_m = modes.index(current)
        rules["media_mode"] = modes[(idx_m + 1) % len(modes)]
    elif action == "lnk":
        modes = ["keep", "remove", "replace"]
        current = rules.get("link_mode", "keep")
        idx_m = modes.index(current) if current in modes else 0
        rules["link_mode"] = modes[(idx_m + 1) % len(modes)]
    elif action == "rml": rules["remove_links"] = not rules.get("remove_links", False)
    elif action == "rmu": rules["remove_usernames"] = not rules.get("remove_usernames", False)
    elif action == "rmh": rules["remove_hashtags"] = not rules.get("remove_hashtags", False)
    elif action == "rst":
        del data["custom_forward_rules"][src_id][dest_id]
        save_persistent_db(force_mongo=True)
        return await cs_dest_list(event)

    save_persistent_db(force_mongo=True)
    await cs_dest_edit(event, src_idx, dest_idx)


# ==========================================
# DESTINATION PREFIX/SUFFIX MENUS
# ==========================================

@bot.on(events.CallbackQuery(pattern=b"cst_pre_menu_"))
async def cst_pre_menu(event):
    await event.answer()
    parts = event.data.decode().split("_")
    src_idx = int(parts[3])
    dest_idx = int(parts[4])
    suffix = f"{src_idx}_{dest_idx}"
    
    user_id = event.sender_id
    data = get_user_data(user_id)
    src_id = str(get_src_by_index(user_id, src_idx))
    dest_id = str(data["destinations"][dest_idx])
    
    rules = data["custom_forward_rules"][src_id][dest_id]
    current = rules.get("prefix") if rules.get("prefix") else "None"
    
    try:
        await event.edit(f"✏️ **Dest Specific Prefix**\nCurrent: `{current}`", buttons=[
            [Button.inline("✏️ Set/Edit", f"cst_set_pre_{suffix}".encode())],
            [Button.inline("🗑 Remove", f"cst_rem_pre_{suffix}".encode())],
            [Button.inline("🔙 Back", f"cs_dest_edit_{src_idx}_{dest_idx}".encode())]
        ])
    except errors.MessageNotModifiedError:
        await event.answer("Already updated")

@bot.on(events.CallbackQuery(pattern=b"cst_suf_menu_"))
async def cst_suf_menu(event):
    await event.answer()
    parts = event.data.decode().split("_")
    src_idx = int(parts[3])
    dest_idx = int(parts[4])
    suffix = f"{src_idx}_{dest_idx}"
    
    user_id = event.sender_id
    data = get_user_data(user_id)
    src_id = str(get_src_by_index(user_id, src_idx))
    dest_id = str(data["destinations"][dest_idx])
    
    rules = data["custom_forward_rules"][src_id][dest_id]
    current = rules.get("suffix") if rules.get("suffix") else "None"
    
    try:
        await event.edit(f"✏️ **Dest Specific Suffix**\nCurrent: `{current}`", buttons=[
            [Button.inline("✏️ Set/Edit", f"cst_set_suf_{suffix}".encode())],
            [Button.inline("🗑 Remove", f"cst_rem_suf_{suffix}".encode())],
            [Button.inline("🔙 Back", f"cs_dest_edit_{src_idx}_{dest_idx}".encode())]
        ])
    except errors.MessageNotModifiedError:
        await event.answer("Already updated")

@bot.on(events.CallbackQuery(pattern=b"cst_set_pre_"))
async def cst_set_pre(event):
    await event.answer()
    parts = event.data.decode().split("_")
    src_idx = int(parts[3])
    dest_idx = int(parts[4])
    
    user_id = event.sender_id
    src_id = str(get_src_by_index(user_id, src_idx))
    data = get_user_data(user_id)
    dest_id = str(data["destinations"][dest_idx])
    
    data["step"] = f"wait_dest_prefix_{src_idx}_{dest_idx}"
    data["step_since"] = time.time()
    
    try:
        await event.edit(
            "✏️ Send the **Start Text (Prefix)** for this destination only:",
            buttons=[Button.inline("🔙 Back", f"cst_pre_menu_{src_idx}_{dest_idx}".encode())]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"cst_set_suf_"))
async def cst_set_suf(event):
    await event.answer()
    parts = event.data.decode().split("_")
    src_idx = int(parts[3])
    dest_idx = int(parts[4])
    
    user_id = event.sender_id
    data = get_user_data(user_id)
    
    data["step"] = f"wait_dest_suffix_{src_idx}_{dest_idx}"
    data["step_since"] = time.time()
    
    try:
        await event.edit(
            "✏️ Send the **End Text (Suffix)** for this destination only:",
            buttons=[Button.inline("🔙 Back", f"cst_suf_menu_{src_idx}_{dest_idx}".encode())]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"cst_rem_pre_"))
async def cst_rem_pre(event):
    await event.answer()
    parts = event.data.decode().split("_")
    src_idx = int(parts[3])
    dest_idx = int(parts[4])
    
    user_id = event.sender_id
    data = get_user_data(user_id)
    src_id = str(get_src_by_index(user_id, src_idx))
    dest_id = str(data["destinations"][dest_idx])
    
    data["custom_forward_rules"][src_id][dest_id]["prefix"] = ""
    save_persistent_db(force_mongo=True)
    await event.answer("Prefix Removed!")
    await cst_pre_menu(event)

@bot.on(events.CallbackQuery(pattern=b"cst_rem_suf_"))
async def cst_rem_suf(event):
    await event.answer()
    parts = event.data.decode().split("_")
    src_idx = int(parts[3])
    dest_idx = int(parts[4])
    
    user_id = event.sender_id
    data = get_user_data(user_id)
    src_id = str(get_src_by_index(user_id, src_idx))
    dest_id = str(data["destinations"][dest_idx])
    
    data["custom_forward_rules"][src_id][dest_id]["suffix"] = ""
    save_persistent_db(force_mongo=True)
    await event.answer("Suffix Removed!")
    await cst_suf_menu(event)


# ==========================================
# DEFAULT SOURCE TOGGLES
# ==========================================

def parse_src_idx(event):
    parts = event.data.decode().split("_")
    return int(parts[2])

@bot.on(events.CallbackQuery(pattern=b"cs_txt_"))
async def toggle_src_text_idx(event):
    await event.answer()
    idx = parse_src_idx(event)
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    rules["forward_text"] = not rules["forward_text"]
    save_persistent_db(force_mongo=True)
    await render_custom_src_menu(event, idx)

@bot.on(events.CallbackQuery(pattern=b"cs_pho_"))
async def toggle_src_pho_idx(event):
    await event.answer()
    idx = parse_src_idx(event)
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    rules["forward_photos"] = not rules["forward_photos"]
    save_persistent_db(force_mongo=True)
    await render_custom_src_menu(event, idx)

@bot.on(events.CallbackQuery(pattern=b"cs_vid_"))
async def toggle_src_vid_idx(event):
    await event.answer()
    idx = parse_src_idx(event)
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    rules["forward_videos"] = not rules["forward_videos"]
    save_persistent_db(force_mongo=True)
    await render_custom_src_menu(event, idx)

@bot.on(events.CallbackQuery(pattern=b"cs_fil_"))
async def toggle_src_fil_idx(event):
    await event.answer()
    idx = parse_src_idx(event)
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    rules["forward_files"] = not rules["forward_files"]
    save_persistent_db(force_mongo=True)
    await render_custom_src_menu(event, idx)

@bot.on(events.CallbackQuery(pattern=b"cs_voi_"))
async def toggle_src_voi_idx(event):
    await event.answer()
    idx = parse_src_idx(event)
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    rules["forward_voice"] = not rules.get("forward_voice", True)
    save_persistent_db(force_mongo=True)
    await render_custom_src_menu(event, idx)

@bot.on(events.CallbackQuery(pattern=b"cs_lnk_"))
async def src_link_mode_idx(event):
    await event.answer()
    idx = parse_src_idx(event)
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    modes = ["keep", "remove", "replace"]
    current = rules.get("link_mode", "keep")
    idx_m = modes.index(current)
    rules["link_mode"] = modes[(idx_m + 1) % len(modes)]
    save_persistent_db(force_mongo=True)
    await render_custom_src_menu(event, idx)

@bot.on(events.CallbackQuery(pattern=b"cs_med_"))
async def src_media_mode_idx(event):
    await event.answer()
    idx = parse_src_idx(event)
    src_id_raw = get_src_by_index(event.sender_id, idx) 
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    modes = ["original", "as_document", "skip"]
    current = rules.get("media_mode", "original")
    idx_m = modes.index(current)
    rules["media_mode"] = modes[(idx_m + 1) % len(modes)]
    save_persistent_db(force_mongo=True)
    await render_custom_src_menu(event, idx)

# Usernames / Hashtags / Prefix / Suffix Sub-handlers (Existing)
@bot.on(events.CallbackQuery(pattern=b"cs_usr_menu_"))
async def cs_usr_menu_handler(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    user_map = rules.get("username_map", {})

    txt = f"🏷 **Username Replacements** for {src_id}\n\nList of specific replacements:"
    buttons = []
    
    keys = list(user_map.keys())
    for i, k in enumerate(keys):
        v = user_map[k]
        display = f"{k} -> {v}"
        if len(display) > 30: display = display[:27] + "..."
        buttons.append([Button.inline(display, f"cs_usr_edit_{idx}_{i}".encode())])
    
    buttons.append([Button.inline("➕ Add Replacement", f"cs_usr_add_{idx}".encode())])
    buttons.append([Button.inline("🗑 Clear All Replacements", f"cs_usr_clear_{idx}".encode())])
    buttons.append([Button.inline("🔙 Back", f"custom_src_idx_{idx}".encode())])
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"cs_usr_add_"))
async def cs_usr_add_start(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    src_id = str(src_id_raw)
    get_user_data(event.sender_id)["step"] = f"wait_src_usr_repl_old_{src_id}"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("Send the **Old Username/Text** (e.g. `@oldchannel`):", buttons=[Button.inline("🔙 Cancel", f"cs_usr_menu_{idx}".encode())])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"cs_usr_edit_"))
async def cs_usr_edit_handler(event):
    await event.answer()
    parts = event.data.decode().split("_")
    idx = parts[3]
    key_idx = int(parts[4])
    
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)

    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    
    keys = list(rules.get("username_map", {}).keys())
    if key_idx < len(keys):
        key = keys[key_idx]
        val = rules["username_map"][key]
        
        try:
            await event.edit(f"Replacement:\n`{key}`\n⬇️\n`{val}`", buttons=[
                [Button.inline("🗑 Delete", f"cs_usr_del_{idx}_{key_idx}".encode())],
                [Button.inline("🔙 Back", f"cs_usr_menu_{idx}".encode())]
            ])
        except errors.MessageNotModifiedError:
            pass
    else:
        await event.answer("Replacement not found (modified?)", alert=True)
        await cs_usr_menu_handler(event)

@bot.on(events.CallbackQuery(pattern=b"cs_usr_del_"))
async def cs_usr_del_handler(event):
    await event.answer()
    parts = event.data.decode().split("_")
    idx = parts[3]
    key_idx = int(parts[4])
    
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    
    keys = list(rules.get("username_map", {}).keys())
    if key_idx < len(keys):
        key = keys[key_idx]
        del rules["username_map"][key]
        save_persistent_db(force_mongo=True)
        await event.answer("Deleted!")
    else:
        await event.answer("Already deleted or not found.")
        
    await cs_usr_menu_handler(event)

@bot.on(events.CallbackQuery(pattern=b"cs_hsh_add_menu_"))
async def cs_hsh_add_menu(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    tags = rules.get("added_hashtags", [])
    txt = f"#️⃣ **Add Hashtags** for {src_id}\n\nThese will be appended to messages:"
    buttons = []
    for i, t in enumerate(tags):
        buttons.append([Button.inline(f"{t}", f"cs_hsh_view_{idx}_{i}".encode())])
    buttons.append([Button.inline("➕ Add Hashtag", f"cs_hsh_add_start_{idx}".encode())])
    buttons.append([Button.inline("🗑 Clear All Hashtags", f"cs_hsh_clear_{idx}".encode())])
    buttons.append([Button.inline("🔙 Back", f"custom_src_idx_{idx}".encode())])
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"cs_hsh_add_start_"))
async def cs_hsh_add_start(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    src_id = str(src_id_raw)
    get_user_data(event.sender_id)["step"] = f"wait_src_hsh_add_{src_id}"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("Send the Hashtag (e.g. `#news`):", buttons=[Button.inline("🔙 Cancel", f"cs_hsh_add_menu_{idx}".encode())])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"cs_hsh_view_"))
async def cs_hsh_view(event):
    await event.answer()
    parts = event.data.decode().split("_")
    idx = parts[3]
    h_idx = int(parts[4])
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    tags = rules.get("added_hashtags", [])
    if h_idx < len(tags):
        try:
            await event.edit(f"Hashtag: `{tags[h_idx]}`", buttons=[
                [Button.inline("🗑 Delete", f"cs_hsh_del_{idx}_{h_idx}".encode())],
                [Button.inline("🔙 Back", f"cs_hsh_add_menu_{idx}".encode())]
            ])
        except errors.MessageNotModifiedError:
            pass

@bot.on(events.CallbackQuery(pattern=b"cs_hsh_del_"))
async def cs_hsh_del(event):
    await event.answer()
    parts = event.data.decode().split("_")
    idx = parts[3]
    h_idx = int(parts[4])
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    tags = rules.get("added_hashtags", [])
    if h_idx < len(tags):
        del tags[h_idx]
        save_persistent_db(force_mongo=True)
        await event.answer("Deleted!")
    await event.respond("✅ Deleted.", buttons=[Button.inline("🔙 Return to List", f"cs_hsh_add_menu_{idx}".encode())])

@bot.on(events.CallbackQuery(pattern=b"cs_pre_menu_"))
async def cs_pre_menu(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    prefix_val = rules.get("prefix")
    current = prefix_val if prefix_val else "None"
    
    try:
        await event.edit(f"✏️ **Start Text (Prefix)**\nCurrent: `{current}`", buttons=[
            [Button.inline("✏️ Set/Edit", f"cs_set_pre_{idx}".encode())],
            [Button.inline("🗑 Remove", f"cs_pre_rem_{idx}".encode())],
            [Button.inline("🔙 Back", f"custom_src_idx_{idx}".encode())]
        ])
    except errors.MessageNotModifiedError:
        await event.answer("Prefix is already empty or same.")

@bot.on(events.CallbackQuery(pattern=b"cs_pre_rem_"))
async def cs_pre_rem(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    rules["prefix"] = ""
    save_persistent_db(force_mongo=True)
    await event.answer("Removed!")
    await cs_pre_menu(event)

@bot.on(events.CallbackQuery(pattern=b"cs_suf_menu_"))
async def cs_suf_menu(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    suffix_val = rules.get("suffix")
    current = suffix_val if suffix_val else "None"
    
    try:
        await event.edit(f"✏️ **End Text (Suffix)**\nCurrent: `{current}`", buttons=[
            [Button.inline("✏️ Set/Edit", f"cs_set_suf_{idx}".encode())],
            [Button.inline("🗑 Remove", f"cs_suf_rem_{idx}".encode())],
            [Button.inline("🔙 Back", f"custom_src_idx_{idx}".encode())]
        ])
    except errors.MessageNotModifiedError:
        await event.answer("Suffix is already empty or same.")

@bot.on(events.CallbackQuery(pattern=b"cs_suf_rem_"))
async def cs_suf_rem(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": get_default_forward_rules()})["default"]
    rules["suffix"] = ""
    save_persistent_db(force_mongo=True)
    await event.answer("Removed!")
    await cs_suf_menu(event)

@bot.on(events.CallbackQuery(pattern=b"cs_rst_"))
async def src_reset_idx(event):
    await event.answer()
    idx = event.data.decode().split("_")[-1]
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw: return
    src_id = str(src_id_raw)
    
    data = get_user_data(event.sender_id)
    data.setdefault("custom_forward_rules", {})[src_id] = {"default": get_default_forward_rules()}
    save_persistent_db(force_mongo=True)
    await event.answer("♻️ Source rules reset!")
    await render_custom_src_menu(event, idx)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ✅ FIX: 4 Missing Handlers — pehle button daba ke kuch nahi hota tha
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ✅ cs_set_pre_{idx} — Source default Prefix set karne ka handler
@bot.on(events.CallbackQuery(pattern=b"cs_set_pre_"))
async def cs_set_pre_handler(event):
    await event.answer()
    raw = event.data.decode()
    idx = raw.replace("cs_set_pre_", "")
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw:
        return await event.answer("❌ Source not found!", alert=True)
    src_id = str(src_id_raw)
    get_user_data(event.sender_id)["step"] = f"wait_src_prefix_{src_id}"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **Set Start Text (Prefix)**"

            "Har message ke UPAR yeh text add hoga."
            "Jo chahein woh bhejein." + _get_owner_footer(),
            buttons=[Button.inline("🔙 Cancel", f"cs_pre_menu_{idx}".encode())]
        )
    except Exception:
        pass


# ✅ cs_set_suf_{idx} — Source default Suffix set karne ka handler
@bot.on(events.CallbackQuery(pattern=b"cs_set_suf_"))
async def cs_set_suf_handler(event):
    await event.answer()
    raw = event.data.decode()
    idx = raw.replace("cs_set_suf_", "")
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw:
        return await event.answer("❌ Source not found!", alert=True)
    src_id = str(src_id_raw)
    get_user_data(event.sender_id)["step"] = f"wait_src_suffix_{src_id}"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **Set End Text (Suffix)**"

            "Har message ke NEECHE yeh text add hoga."
            "Jo chahein woh bhejein." + _get_owner_footer(),
            buttons=[Button.inline("🔙 Cancel", f"cs_suf_menu_{idx}".encode())]
        )
    except Exception:
        pass


# ✅ cs_hsh_clear_{idx} — Sab hashtags clear karne ka handler
@bot.on(events.CallbackQuery(pattern=b"cs_hsh_clear_"))
async def cs_hsh_clear_handler(event):
    await event.answer()
    raw = event.data.decode()
    idx = raw.replace("cs_hsh_clear_", "")
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw:
        return await event.answer("❌ Source not found!", alert=True)
    src_id = str(src_id_raw)
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(
        src_id, {"default": get_default_forward_rules()}
    )["default"]
    rules["added_hashtags"] = []
    save_persistent_db(force_mongo=True)
    await event.answer("✅ Saare hashtags clear ho gaye!")
    try:
        await event.edit(
            "#️⃣ **Added Hashtags**"

"Saare hashtags successfully remove kiye." + _get_owner_footer(),
            buttons=[[Button.inline("🔙 Back", f"custom_src_idx_{idx}".encode())]]
        )
    except Exception:
        pass


# ✅ cs_usr_clear_{idx} — Sab username replacements clear karne ka handler
@bot.on(events.CallbackQuery(pattern=b"cs_usr_clear_"))
async def cs_usr_clear_handler(event):
    await event.answer()
    raw = event.data.decode()
    idx = raw.replace("cs_usr_clear_", "")
    src_id_raw = get_src_by_index(event.sender_id, idx)
    if not src_id_raw:
        return await event.answer("❌ Source not found!", alert=True)
    src_id = str(src_id_raw)
    data = get_user_data(event.sender_id)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(
        src_id, {"default": get_default_forward_rules()}
    )["default"]
    rules["username_map"] = {}
    save_persistent_db(force_mongo=True)
    await event.answer("✅ Saare username replacements clear ho gaye!")
    try:
        await event.edit(
            "👤 **Username Replacements**"

"Saare replacements successfully remove kiye." + _get_owner_footer(),
            buttons=[[Button.inline("🔙 Back", f"custom_src_idx_{idx}".encode())]]
        )
    except Exception:
        pass


@bot.on(events.CallbackQuery(pattern=b"cst_cap_"))
async def cst_cap_menu(event):
    await event.answer()
    parts = event.data.decode().split("_")
    src_idx  = int(parts[2])
    dest_idx = int(parts[3])
    suffix   = f"{src_idx}_{dest_idx}"
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    src_id   = str(get_src_by_index(user_id, src_idx))
    dest_id  = str(data["destinations"][dest_idx])
    rules    = data["custom_forward_rules"][src_id][dest_id]
    curr     = rules.get("custom_caption") or "❌ Set nahi"

    try:
        await event.edit(
            f"📋 **Custom Caption**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Abhi: `{curr[:60]}`\n\n"
            f"Media ke saath ye caption lagegi (original caption replace ho jaayegi).\n"
            f"Khaali rakhne ke liye Reset dabao.\n\n"
            f"Naya caption type karke bhejo:",
            buttons=[
                [Button.inline("🗑 Remove Caption", f"cst_cap_reset_{suffix}".encode())],
                [Button.inline("🔙 Back", f"cs_dest_edit_{src_idx}_{dest_idx}".encode())]
            ]
        )
    except errors.MessageNotModifiedError:
        pass
    data["step"] = f"cst_cap_set_{src_idx}_{dest_idx}"
    data["step_since"] = time.time()
    save_persistent_db(force_mongo=True)


@bot.on(events.CallbackQuery(pattern=b"cst_cap_reset_"))
async def cst_cap_reset(event):
    await event.answer()
    parts    = event.data.decode().split("_")
    src_idx  = int(parts[3])
    dest_idx = int(parts[4])
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    src_id   = str(get_src_by_index(user_id, src_idx))
    dest_id  = str(data["destinations"][dest_idx])
    data["custom_forward_rules"][src_id][dest_id]["custom_caption"] = None
    save_persistent_db(force_mongo=True)
    await event.answer("✅ Caption remove kar diya!", alert=True)
    await cs_dest_edit(event, src_idx, dest_idx)


@bot.on(events.CallbackQuery(pattern=b"cst_repl_"))
async def cst_repl_menu(event):
    """Per-dest replacements menu."""
    await event.answer()
    parts    = event.data.decode().split("_")
    src_idx  = int(parts[2])
    dest_idx = int(parts[3])
    suffix   = f"{src_idx}_{dest_idx}"
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    src_id   = str(get_src_by_index(user_id, src_idx))
    dest_id  = str(data["destinations"][dest_idx])
    rules    = data["custom_forward_rules"].get(src_id, {}).get(dest_id, {})
    repl_map = rules.get("replace_map", {})

    lines = []
    for old, new in list(repl_map.items())[:10]:
        o_short = old[:15] + "…" if len(old) > 15 else old
        n_short = new[:15] + "…" if len(new) > 15 else new
        lines.append(f"  `{o_short}` → `{n_short}`")
    repl_txt = "\n".join(lines) if lines else "  (koi nahi)"

    try:
        await event.edit(
            f"🔄 **Per-Dest Replacements**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Sirf is destination ke liye text replace hoga.\n\n"
            f"**Active ({len(repl_map)}):**\n{repl_txt}\n\n"
            f"Format: `purana text → naya text` bhejo",
            buttons=[
                [Button.inline("➕ Nayi Replacement Add", f"cst_repl_add_{suffix}".encode())],
                [Button.inline("🗑 Sab Clear", f"cst_repl_clear_{suffix}".encode())],
                [Button.inline("🔙 Back", f"cs_dest_edit_{src_idx}_{dest_idx}".encode())]
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"cst_repl_add_"))
async def cst_repl_add(event):
    await event.answer()
    parts    = event.data.decode().split("_")
    src_idx  = int(parts[3])
    dest_idx = int(parts[4])
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    data["step"] = f"cst_repl_set_{src_idx}_{dest_idx}"
    data["step_since"] = time.time()
    save_persistent_db(force_mongo=True)
    try:
        await event.edit(
            "🔄 **Replacement Add Karo**\n\n"
            "Format mein type karo:\n`purana text → naya text`\n\n"
            "Example:\n`Amazon` → `Flipkart`",
            buttons=[[Button.inline("🔙 Cancel", f"cst_repl_{src_idx}_{dest_idx}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"cst_repl_clear_"))
async def cst_repl_clear(event):
    await event.answer()
    parts    = event.data.decode().split("_")
    src_idx  = int(parts[3])
    dest_idx = int(parts[4])
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    src_id   = str(get_src_by_index(user_id, src_idx))
    dest_id  = str(data["destinations"][dest_idx])
    data["custom_forward_rules"][src_id][dest_id]["replace_map"] = {}
    save_persistent_db(force_mongo=True)
    await event.answer("✅ Sab replacements clear!", alert=True)
    await cst_repl_menu(event)


# ── Source Enable / Disable ───────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"src_toggle_enabled_(.+)"))
async def src_toggle_enabled(event):
    await event.answer()
    idx    = event.data.decode().replace("src_toggle_enabled_", "")
    uid    = event.sender_id
    data   = get_user_data(uid)
    src_id = str(get_src_by_index(uid, idx))
    if not src_id:
        return await event.answer("Source nahi mila!", alert=True)
    rules = data.setdefault("custom_forward_rules", {}).setdefault(
        src_id, {}).setdefault("default", {})
    cur    = rules.get("src_enabled", True)
    rules["src_enabled"] = not cur
    save_persistent_db(force_mongo=True)
    new_status = "▶️ Resumed" if not cur else "⏸️ Paused"
    await event.answer(f"Source {new_status}", alert=False)
    # Refresh the source menu
    event.data = f"custom_src_idx_{idx}".encode()
    try:
        await render_custom_src_menu(event, int(idx))
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════
# 🔧 PER-DEST NEW HANDLERS — Copy Mode, Pin, Enable/Disable, Health
# ══════════════════════════════════════════════════════════════

def _get_dest_rules(uid, src_idx, dest_idx):
    data    = get_user_data(uid)
    src_id  = str(get_src_by_index(uid, src_idx))
    dest_id = str(data["destinations"][dest_idx])
    return (data.setdefault("custom_forward_rules", {})
               .setdefault(src_id, {})
               .setdefault(dest_id, {}))


@bot.on(events.CallbackQuery(pattern=b"cst_copy_toggle_(.+)"))
async def cst_copy_toggle(event):
    await event.answer()
    raw      = event.data.decode()  # e.g. "cst_copy_toggle_0_1"
    idxs     = raw.split("_")
    src_idx, dest_idx = int(idxs[-2]), int(idxs[-1])
    rules = _get_dest_rules(event.sender_id, src_idx, dest_idx)
    rules["copy_mode"] = not rules.get("copy_mode", False)
    save_persistent_db(force_mongo=True)
    status = "✅ Copy Mode ON (no forward tag)" if rules["copy_mode"] else "❌ Copy Mode OFF"
    await event.answer(status, alert=False)
    await cs_dest_edit(event, src_idx, dest_idx)


@bot.on(events.CallbackQuery(pattern=b"cst_pin_toggle_(.+)"))
async def cst_pin_toggle(event):
    await event.answer()
    raw      = event.data.decode()
    idxs     = raw.split("_")
    src_idx, dest_idx = int(idxs[-2]), int(idxs[-1])
    rules = _get_dest_rules(event.sender_id, src_idx, dest_idx)
    rules["pin_forwarded"] = not rules.get("pin_forwarded", False)
    save_persistent_db(force_mongo=True)
    status = "📌 Pin ON" if rules["pin_forwarded"] else "📌 Pin OFF"
    await event.answer(status, alert=False)
    await cs_dest_edit(event, src_idx, dest_idx)


@bot.on(events.CallbackQuery(pattern=b"cst_dest_toggle_(.+)"))
async def cst_dest_toggle(event):
    await event.answer()
    raw      = event.data.decode()
    idxs     = raw.split("_")
    src_idx, dest_idx = int(idxs[-2]), int(idxs[-1])
    rules = _get_dest_rules(event.sender_id, src_idx, dest_idx)
    cur    = rules.get("dest_enabled", True)
    rules["dest_enabled"]    = not cur
    rules["disabled_reason"] = "" if not cur else "Manually disabled"
    save_persistent_db(force_mongo=True)
    status = "▶️ Destination Enabled" if not cur else "⏸️ Destination Disabled"
    await event.answer(status, alert=False)
    await cs_dest_edit(event, src_idx, dest_idx)


@bot.on(events.CallbackQuery(pattern=b"cst_dest_health_reset_(.+)"))
async def cst_dest_health_reset(event):
    await event.answer()
    raw      = event.data.decode()
    idxs     = raw.split("_")
    src_idx, dest_idx = int(idxs[-2]), int(idxs[-1])
    rules = _get_dest_rules(event.sender_id, src_idx, dest_idx)
    rules["fail_count"]      = 0
    rules["dest_enabled"]    = True
    rules["disabled_reason"] = ""
    save_persistent_db(force_mongo=True)
    await event.answer("✅ Dest re-enabled! Fail count reset.", alert=False)
    await cs_dest_edit(event, src_idx, dest_idx)
