from telethon import events, Button, errors
from config import bot
from database import get_user_data, save_persistent_db, user_sessions
from utils import get_display_name
from .source_menu import show_pinned_chats

def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

@bot.on(events.CallbackQuery(data=b"add_dest"))
async def add_dest_cb(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    if not data["session"]:
        return await event.answer("⚠️ Pehle login karo!", alert=True)
    dests = data.get("destinations", [])
    count = len(dests)
    try:
        await event.edit(
            "📤 **DESTINATION MANAGEMENT**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Active destinations: **{count}**\n\n"
            "**Destination kya hota hai?**\n"
            "_Jahan forward kiya jaayega — channel ya group_\n\n"
            "⚠️ Apna account us channel ka **admin** hona chahiye!\n\n"
            "Kaise add karein:",
            buttons=[
                [Button.inline("📌 Pinned Chats se Choose",       b"pin_dest_flow")],
                [Button.inline("✏️ Manually ID/Link daalo",        b"man_dest_flow")],
                [Button.inline(f"📋 Manage Destinations ({count})", b"list_dest_0")],
                [Button.inline("🔙 Main Menu",                      b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"pin_dest_flow"))
async def pin_dest_flow(event):
    await event.answer()
    await event.delete()
    await show_pinned_chats(event, 'dest')

@bot.on(events.CallbackQuery(data=b"man_dest_flow"))
async def man_dest_flow(event):
    await event.answer()
    get_user_data(event.sender_id)["step"] = "wait_dest_input"
    await event.edit(
        "✏️ **Destination Add Karo**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Inme se koi bhi bhejo:\n\n"
        "• `@username` — public channel\n"
        "• `https://t.me/channelname` — channel link\n"
        "• `-1001234567890` — direct channel ID\n\n"
        "💡 **Multiple destinations ek saath add karo!**\n"
        "Har ID/link alag line mein ya comma se alag karke bhejo.\n\n"
        "⚠️ Apna account us channel ka **admin** hona zaroori hai.\n"
        "_Bina admin ke messages nahi jaayenge!_",
        buttons=[[Button.inline("🔙 Back", b"add_dest")]]
    )

@bot.on(events.CallbackQuery(data=b"rem_dest"))
async def rem_dest_handler(event):
    await event.answer()
    await list_dest_handler(event, 0)

@bot.on(events.CallbackQuery(data=b"dest_menu"))
async def dest_menu_handler(event):
    """BUG 22 FIX: dest_menu callback - destination list dikhao"""
    await event.answer()
    await list_dest_handler(event, 0)

@bot.on(events.CallbackQuery(pattern=b"list_dest_"))
async def list_dest_handler(event, page=None):
    await event.answer()
    data_str = event.data.decode()
    is_delete_mode = "del" in data_str

    if page is None: 
        try: page = int(data_str.split("_")[-1])
        except: page = 0

    user_id = event.sender_id
    data = get_user_data(user_id)
    dests = data["destinations"]
    
    # Navigation Logic
    back_btn_data = b"main_menu" if is_delete_mode else b"add_dest"
    
    if not dests:
        try:
            return await event.edit("❌ No destinations added.", buttons=[[Button.inline("🔙 Back", back_btn_data)]])
        except errors.MessageNotModifiedError:
            return

    MAX = 5
    start = page * MAX
    end = start + MAX
    subset = dests[start:end]
    
    client = user_sessions.get(user_id)
    buttons = []
    
    mode_flag = "del" if is_delete_mode else "view"

    for i, s in enumerate(subset):
        idx = start + i
        name = str(s)
        if client:
            name = await get_display_name(client, s, user_id)
        else:
            cached = data.get("channel_names", {}).get(str(s))
            if cached:
                name = cached
            elif "t.me/+" in str(s) or "t.me/joinchat" in str(s) or str(s).startswith("+"):
                # ✅ FIX: For invite-link entries, try to show resolved channel name from channel_names_id
                resolved_id = data.get("channel_names_id", {}).get(str(s))
                if resolved_id:
                    cached_by_id = data.get("channel_names", {}).get(str(resolved_id))
                    name = cached_by_id if cached_by_id else "🔒 Private Channel"
                else:
                    name = "🔒 Private Channel"
        buttons.append([Button.inline(f"{name}", f"view_dest_{mode_flag}_{idx}".encode())])
        
    nav = []
    prefix = f"list_dest_{mode_flag}"
    if page > 0: nav.append(Button.inline("⬅️ Prev", f"{prefix}_{page-1}".encode()))
    if end < len(dests): nav.append(Button.inline("Next ➡️", f"{prefix}_{page+1}".encode()))
    if nav: buttons.append(nav)
    
    buttons.append([Button.inline("🔙 Back", back_btn_data)])
    try:
        await event.edit("📋 **Destination List**\nSelect to View/Delete:", buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"view_dest_"))
async def view_dest_item(event):
    await event.answer()
    parts = event.data.decode().split("_")
    mode = parts[2]
    idx = int(parts[3])
    
    data = get_user_data(event.sender_id)
    if idx >= len(data["destinations"]): return await event.answer("Not found")
    val = data["destinations"][idx]
    
    back_data = f"list_dest_{mode}_{idx//5}".encode()
    del_data = f"del_dest_{mode}_{idx}".encode()
    
    try:
        await event.edit(f"Destination Details:\nID: `{val}`", buttons=[
            [Button.inline("🗑 Delete", del_data)],
            [Button.inline("🔙 Back", back_data)]
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"del_dest_"))
async def del_dest_item(event):
    await event.answer()
    parts = event.data.decode().split("_")
    mode = parts[2]
    idx = int(parts[3])
    
    data = get_user_data(event.sender_id)
    if idx < len(data["destinations"]):
        del data["destinations"][idx]
        save_persistent_db(force_mongo=True)
        # ✅ FIX: Directly await MongoDB save
        try:
            from database import save_to_mongo as _stm
            await _stm()
        except Exception:
            pass
        
    await event.answer("🗑 Destination Removed!")
    # FIX: event.data setter removed to avoid AttributeError
    # Seedha handler call karein page 0 ke liye
    await list_dest_handler(event, page=0)
