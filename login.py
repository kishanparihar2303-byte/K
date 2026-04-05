import asyncio
import time
from telethon import events, Button, errors
from config import bot
from database import get_user_data, user_sessions, save_persistent_db
from lang import t, get_lang  # FIX: t aur get_lang import missing tha — NameError fix

# ==========================================
# LOGIN MENU HANDLER
# ==========================================
def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

@bot.on(events.CallbackQuery(data=b"login_menu"))
async def login_menu(event):
    await event.answer()
    user_id = event.sender_id
    data    = get_user_data(user_id)

    if data["session"]:
        # Already logged in — show status
        connected = user_id in user_sessions and user_sessions[user_id].is_connected()
        status = "📡 Live & Connected" if connected else "💾 Session Saved (will reconnect)"
        srcs  = len(data.get("sources", []))
        dests = len(data.get("destinations", []))
        await event.edit(
            "🔐 **ACCOUNT STATUS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ **Logged In**\n"
            f"📊 {status}\n\n"
            f"📥 Sources: `{srcs}`  📤 Destinations: `{dests}`\n\n"
            "_Logout karne se forwarding band ho jaayegi_",
            buttons=[
                [Button.inline("🔴 Logout",     b"logout_proc")],
                [Button.inline("🏠 Main Menu",  b"main_menu")],
            ]
        )
    else:
        # Start login — Safety info + step-by-step guide
        data["step"]       = "wait_phone"
        data["step_since"] = time.time()
        user_id = event.sender_id
        await event.edit(
            "📱 **TELEGRAM ACCOUNT LOGIN**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔒 **Teri Safety — Humare Vaade:**\n"
            "✅ Tera password **kabhi save nahi** hota\n"
            "✅ Sirf encrypted session string store hoti hai\n"
            "✅ Sirf forwarding ke liye use hota hai — kuch aur nahi\n"
            "✅ Logout karte hi session turant delete ho jaata hai\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📋 **Login Kaise Hoga — 3 Steps:**\n\n"
            "1️⃣ **Phone Number** — country code ke saath\n"
            "   _Example: +919876543210_\n\n"
            "2️⃣ **OTP Code** — Telegram se aayega\n"
            "   _5 digit code, Telegram app mein check karo_\n\n"
            "3️⃣ **2FA Password** _(sirf agar set hai)_\n"
            "   _Yeh optional hai — agar tumne set kiya ho_\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📞 **Abhi apna phone number bhejo:**\n"
            "_(Country code ke saath, e.g., +91XXXXXXXXXX)_",
            buttons=[[Button.inline("❌ Cancel", b"main_menu")]]
        )


# ==========================================
# LOGOUT PROCESS HANDLER (PROBLEM 47 FIXED)
# ==========================================
@bot.on(events.CallbackQuery(data=b"logout_proc"))
async def logout_proc(event):
    """Show confirmation first, then logout"""
    await event.answer()
    user_id = event.sender_id
    try:
        await event.edit(
            t(user_id, "logout_confirm"),
            buttons=[
                [Button.inline(t(user_id, "btn_logout_yes"), b"logout_confirmed")],
                [Button.inline(t(user_id, "btn_logout_no"),  b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"logout_confirmed"))
async def logout_confirmed(event):
    await event.answer()
    user_id = event.sender_id
    data = get_user_data(user_id)

    await event.edit("⏳ **Logging out...**\nStopping background tasks, please wait.")

    # 🚨 FIX PROBLEM 47: Graceful Shutdown Logic
    # 1. सबसे पहले रनिंग फ्लैग को False करें (ताकि forward_engine लूप रुक जाए)
    if "settings" in data:
        data["settings"]["running"] = False
    
    # 2. डेटाबेस सेव करें ताकि फ्लैग डिस्क पर अपडेट हो जाए
    save_persistent_db()

    # 3. थोड़ा इंतज़ार करें (2 सेकंड) ताकि चल रहे tasks (जैसे फाइल अपलोड) को पता चले कि रुकना है
    await asyncio.sleep(2)

    # 4. अब सुरक्षित रूप से क्लाइंट को डिस्कनेक्ट करें
    if user_id in user_sessions:
        client = user_sessions[user_id]
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception as e:
            print(f"Logout disconnect error for {user_id}: {e}")
        
        # मेमोरी से क्लाइंट हटाएं
        del user_sessions[user_id]

    # 5. Session data saaf karo
    data["session"] = None
    data["phone"] = None
    data["hash"] = None
    data["step"] = None
    data["settings"]["running"] = False
    # FIX 21: Clear PBKDF2 key cache on logout (security)
    try:
        from session_vault import clear_key_cache
        clear_key_cache(user_id)
    except Exception:
        pass

    # WORKER ARCHITECTURE: Assignment hatao — worker automatically session stop kar dega
    from worker_manager import unassign_worker
    unassign_worker(user_id)
    
    # फाइनल सेव
    save_persistent_db()

    # 6. UI update
    await event.edit(
        "✅ **LOGGED OUT SUCCESSFULLY**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Tumhara account disconnect ho gaya.\n"
        "Sab background tasks band kar diye.\n\n"
        "_Dobara login karne ke liye button dabao_ 👇",
        buttons=[
            [Button.inline("📱 Login Again", b"login_menu")],
            [Button.inline("🏠 Main Menu",   b"main_menu")],
        ]
    )
