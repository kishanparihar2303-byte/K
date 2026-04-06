import time
import asyncio
import datetime

# ✅ FIX: Telethon Python 3.12 GeneratorExit patch — MUST be first
# Disables libssl ctypes AES → falls back to pure Python (no performance hit on Railway)
import telethon_patch
telethon_patch.apply()

# ✅ QUEUED MESSAGES — COMPLETE ELIMINATION
# BOT_START_TIME se pehle ke SAARE events (NewMessage + CallbackQuery) block honge
BOT_START_TIME = time.time()

def _is_queued_message(event) -> bool:
    """
    True return karo agar event bot start se PEHLE ka hai.
    NewMessage aur CallbackQuery dono ke liye kaam karta hai.
    
    BUG FIX: Pehle (BOT_START_TIME - 300) tha — galat direction.
    Correct: koi bhi event jo BOT_START_TIME se pehle ka ho = queued = ignore.
    """
    try:
        # NewMessage — event.message.date
        date = getattr(event, 'date', None)
        if date is None:
            msg = getattr(event, 'message', None)
            date = getattr(msg, 'date', None) if msg else None
        if date:
            ts = date.timestamp() if hasattr(date, 'timestamp') else float(date)
            return ts < BOT_START_TIME  # Bot start se pehle = queued
    except Exception:
        pass
    return False

from scheduler import scheduler_queue_loop as _sched_queue_loop
from aiohttp import web
import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from telethon import TelegramClient, events, Button, errors
from telethon.sessions import StringSession
from telethon.errors import (
    ForbiddenError, UserIsBlockedError, FloodWaitError,
    ChannelPrivateError, InviteHashInvalidError, UserAlreadyParticipantError
)

from config import bot, BOT_TOKEN, API_ID, API_HASH, logger, ADMINS, OWNER_ID
from database import (
    db, active_clients, user_sessions, load_persistent_db,
    get_user_data, update_last_active, cleanup_inactive_users,
    CLEANUP_CONFIG, GLOBAL_STATE, save_persistent_db,
    is_blocked
)
from utils import resolve_id, resolve_id_and_name, normalize_url, normalize_channel_id, sources_match, channel_already_exists, get_display_name
from config import get_default_forward_rules
from forward_engine import start_user_forwarder
from ui.source_menu import get_index_by_src, get_src_by_index
from lang import t as _t_lang, get_lang; t = _t_lang  # backward compat alias — get_lang FIX #1
from admin import is_admin as _is_admin_fn; is_admin = _is_admin_fn  # alias
from premium import is_premium_user, get_premium_config, check_source_limit, check_dest_limit
from force_subscribe import check_force_subscribe, get_fs_config

# Import all UI handlers
import ui.main_menu
import ui.source_menu
import ui.dest_menu
import ui.settings_menu
try:
    import support   # Contact Admin / Support Ticket System — handlers register hote hain
except Exception as _sup_e:
    print(f"Support module load error: {_sup_e}")

# Background tasks ka set — prevent garbage collection
_background_tasks: set = set()

# OTP brute-force protection: {user_id: (attempt_count, first_attempt_ts)}
_otp_attempts: dict = {}
_OTP_MAX_ATTEMPTS = 5       # Max failed OTP attempts before lockout
_OTP_LOCKOUT_SECS = 300     # 5-minute lockout after max attempts

def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def _create_tracked_task(coro, name="task"):
    """Create asyncio task with error logging."""
    task = asyncio.create_task(coro, name=name)
    _background_tasks.add(task)
    def _on_done(t):
        _background_tasks.discard(t)
        if not t.cancelled() and t.exception():
            logger.error(f"Background task '{name}' crashed: {t.exception()}")
    task.add_done_callback(_on_done)
    return task

import ui.filters_menu
import ui.admin_menu
import login
import backup
import refer
import health_monitor
import premium
import force_subscribe
import analytics
import payment
import msg_limit
import source_tracker

# ── New Feature Modules ──────────────────────────────────
import feature_flags       # Admin master control
import notifications
from notification_center import register_nc_handlers as _reg_nc
import ui.translate_menu as _translate_menu_mod  # noqa — registers handlers
import ui.ads_menu as _ads_menu_mod  # noqa — registers handlers
import ui.promo_menu as _promo_menu_mod  # noqa
import ui.task_menu as _task_menu_mod  # noqa  # noqa — registers handlers       # Smart notifications
import ui.feature_flags_menu  # Admin feature flags UI
import ui.anti_spam_menu       # Anti-Spam admin panel
import ui.reseller_menu    # Reseller panel UI
# ── v3: New modules ───────────────────────────────────────────────────────────
try:
    from circuit_breaker import CircuitBreakerRegistry
    from rate_limiter import RateLimiterRegistry
    print("✅ v3 Circuit Breaker + Rate Limiter loaded")
except ImportError as _e:
    print(f"⚠️ v3 modules not found (optional): {_e}")

# v3 New UI handlers (registers callbacks automatically)
try:
    import ui.settings_menu   # has v3 menus appended
    print("✅ v3 Settings menus loaded")
except Exception as _e2:
    print(f"⚠️ Settings menu load error: {_e2}")
# ─────────────────────────────────────────────────────────────────────────────

from reseller import add_reseller, remove_reseller, reseller_give_premium

# Termux Wake Lock
if "ANDROID_ROOT" in os.environ:
    try:
        subprocess.run(["termux-wake-lock"])
        print("📱 Termux Wake-Lock Acquired!")
    except Exception as e:
        print(f"⚠️ Failed to acquire wake-lock: {e}")

# Async DB Save
# FIX: Separate thread pools — JSON disk write vs MongoDB (async)
# JSON write = sync disk I/O → needs thread pool
# MongoDB = async driver → never blocks thread pool
db_executor = ThreadPoolExecutor(max_workers=3)  # Was 1 — increased for parallel saves

def sync_save_db():
    from database import save_persistent_db
    save_persistent_db()

async def save_db_async():
    """Save to local JSON (thread pool) + MongoDB (awaited — data loss prevent karo)."""
    loop = asyncio.get_running_loop()
    # JSON: blocking disk I/O → thread pool
    await loop.run_in_executor(db_executor, sync_save_db)
    # MongoDB: directly await — create_task se restart par save miss ho jaata tha
    try:
        from database import save_to_mongo as _save_mongo, _mongo_enabled
        if _mongo_enabled:
            await _save_mongo()  # ✅ FIX: await — no longer fire-and-forget
    except Exception as _mongo_err:
        logger.debug(f"MongoDB sync error (non-critical): {_mongo_err}")


def save_db_bg():
    """
    ⚡ Fire-and-forget save — handlers ke liye jo save ka result nahi chahiye.
    Caller ko block nahi karta — background mein save hoti hai.
    Usage: save_db_bg()  # instead of: await save_db_async()
    """
    try:
        asyncio.create_task(save_db_async())
    except RuntimeError:
        # No event loop (startup) — sync fallback
        sync_save_db()


# FIX #2: Duplicate handler hataya — sirf yeh ek handler hai /rules /notice /info /contact ke liye
@bot.on(events.NewMessage(pattern=r'(?i)^/(rules|notice|info|contact)$'))
async def rules_contact_cmd(event):
    if _is_queued_message(event): return
    if not event.is_private:
        return
    uid = event.sender_id
    cmd = event.pattern_match.group(1).lower() if event.pattern_match and event.pattern_match.group(1) else "rules"
    if cmd == "contact":
        data = get_user_data(uid)
        data["step"] = "wait_contact_admin_msg"
        data["step_since"] = time.time()  # FIX #3: __import__ hataya
        await event.respond(
            "📞 **Admin ko Message Bhejo**\n\n"
            "Apna message type karo 👇\n"
            "_Ya /cancel karke wapas jao_",
            buttons=[
                [Button.inline("❌ Cancel", b"main_menu")]
            ]
        )
        return
    try:
        from ui.admin_menu import get_notice_text_for_user
        txt = get_notice_text_for_user()
    except Exception:
        txt = None
    if txt:
        await event.respond(txt, buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]])
    else:
        await event.respond(
            "ℹ️ Abhi koi notice ya rules set nahi hain।",
            buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]]
        )


@bot.on(events.NewMessage(pattern=r'/start(?: (.+))?'))
async def start_handler(event):
    if not event.is_private:
        return
    user_id = event.sender_id

    # ✅ Queued message check
    if _is_queued_message(event): return

    logger.debug(f"[START] user_id={user_id}")

    try:
        args = event.pattern_match.group(1)
    except Exception:
        args = None

    # ── Blocked user ──────────────────────────────────
    if is_blocked(user_id):  # FIX: top-level import use karo — local import hataya
        return await event.respond(t(user_id, "banned_msg"))

    # ── Maintenance mode — only non-admins blocked ───────────────────
    if GLOBAL_STATE.get("maintenance_mode") and user_id != OWNER_ID:
        from admin import is_admin as _ia
        if not _ia(user_id):
            return await event.respond(t(user_id, "maintenance_msg") + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""))

    # ── Block new registrations ───────────────────────
    if GLOBAL_STATE.get("block_new_reg") and user_id not in db and user_id != OWNER_ID:
        return await event.respond(t(user_id, "new_reg_closed"))

    # ── Force Subscribe check ─────────────────────────
    try:
        subscribed = await force_subscribe.check_force_subscribe(event)
        if not subscribed:
            logger.debug(f"[START] force_sub blocked user_id={user_id}")
            return
    except Exception as _fs_err:
        logger.warning(f"[START] force_sub error for {user_id}: {_fs_err}")
        # Error mein user ko block mat karo

    # ── /start rate limit — anti-spam ────────────────
    import time as _st
    _START_RLIMIT = getattr(start_handler, "_rl", {})
    start_handler._rl = _START_RLIMIT
    _now_rl = _st.time()
    if _now_rl - _START_RLIMIT.get(user_id, 0) < 3:  # FIX 7: 3s cooldown
        return
    _START_RLIMIT[user_id] = _now_rl
    if len(_START_RLIMIT) > 5000:
        _c = _now_rl - 60
        for _k in [k for k, v in list(_START_RLIMIT.items()) if v < _c]:
            del _START_RLIMIT[_k]

    # ── DB + profile ──────────────────────────────────
    try:
        data = get_user_data(user_id)
        update_last_active(user_id)
        if data.get("step"):
            data["step"] = None
            data.pop("step_since", None)  # FIX H: also clear timeout tracker
    except Exception as e:
        logger.error(f"[START] get_user_data error for {user_id}: {e}")
        data = {}

    try:
        sender = await event.get_sender()
        if sender and data:
            data["profile"] = {
                "first_name": getattr(sender, 'first_name', '') or "",
                "last_name":  getattr(sender, 'last_name', '') or "",
                "username":   getattr(sender, 'username', '') or "",
            }
    except Exception:
        pass

    # ── New user setup ─────────────────────────────────
    try:
        is_brand_new = not data.get("notified_admin", False)
        if is_brand_new and data:
            data["notified_admin"] = True
            try:
                from premium import setup_trial_if_new
                setup_trial_if_new(user_id)
            except Exception: pass
            # FIX 1: Save new user to DB immediately — prevent data loss on crash
            try:
                asyncio.create_task(save_db_async())
            except Exception: pass
            try: asyncio.create_task(_notify_admin_new_user(user_id))
            except Exception: pass
            try:
                _fn  = getattr(event.sender, "first_name", None) or ""
                _un  = getattr(event.sender, "username", None) or ""
                # ✅ FIX: New user notification dobara chalu — notification_center se
                from notification_center import notify_new_user as _nc_notify
                asyncio.create_task(_nc_notify(user_id, username=_un, first_name=_fn))
            except Exception: pass
    except Exception as e:
        logger.warning("[START] new user setup error: {e}")

    # ── Source tracking ────────────────────────────────
    try:
        source_tracker.record_user_source(user_id, args)
    except Exception: pass

    # ── Referral ───────────────────────────────────────
    if args and args.startswith("ref_"):
        try:
            referrer_id = int(args.replace("ref_", "").split("_")[0])
            asyncio.create_task(refer.process_referral(user_id, referrer_id))
        except Exception: pass

    # ── Build welcome message ──────────────────────────
    try:
        from ui.main_menu import get_main_buttons
        main_btns = get_main_buttons(user_id)
    except Exception as e:
        logger.debug(f"[START] get_main_buttons error: {e}")
        main_btns = [[Button.inline("🏠 Menu", b"main_menu")]]

    try:
        from ui.main_menu import _build_menu_text
        if data.get("session"):
            # Logged-in: smart live menu text
            wel_text = _build_menu_text(user_id)
        else:
            # New/logged-out: welcome from lang.py
            try:
                refer_settings = refer.get_refer_settings()
                group_link = refer_settings.get("group_link")
                if group_link:
                    main_btns = [[Button.url("📢 Join Our Group", group_link)]] + main_btns
            except Exception:
                pass
            _footer = _get_owner_footer()
            wel_text = t(user_id, "welcome_new") + ("\n\n" + _footer if _footer else "")

        logger.debug("[START] Sending welcome to {user_id}")
        await event.respond(wel_text, buttons=main_btns)
        logger.debug("[START] Welcome sent OK to {user_id}")
    except Exception as e:
        logger.warning("[START] respond error for {user_id}: {e}")
        try:
            await event.respond(t(user_id, "error_generic"))
        except Exception as e2:
            logger.warning("[START] fallback also failed for {user_id}: {e2}")


async def _notify_admin_new_user(user_id: int):
    """Admin ko new user ki notification bhejo — notification_center se."""
    # This is now handled by notification_center.notify_new_user()
    pass


# ==========================================
# QUICK ADD SOURCE (from no-match suggestion)
# ==========================================
@bot.on(events.CallbackQuery(pattern=b"quick_add_src_"))
async def quick_add_src_cb(event):
    await event.answer()
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    chat_id  = event.data.decode().replace("quick_add_src_", "")

    # Duplicate check
    from utils import channel_already_exists
    if channel_already_exists(chat_id, data.get("sources", []), data.get("channel_names_id", {})):
        return await event.answer("⚠️ Yeh source pehle se add hai!", alert=True)

    # Loop check — destination mein toh nahi?
    if channel_already_exists(chat_id, data.get("destinations", []), data.get("channel_names_id", {})):
        return await event.answer("⚠️ Yeh channel already Destination hai!", alert=True)

    data.setdefault("sources", []).append(chat_id)

    # Title cache karo
    try:
        from database import user_sessions
        from utils import get_display_name
        _c = user_sessions.get(user_id)
        if _c:
            title = await get_display_name(_c, int(chat_id), user_id)
            data.setdefault("channel_names", {})[chat_id] = title
    except Exception:
        pass

    save_persistent_db(force_mongo=True)
    # Direct MongoDB await — restart pe data loss nahi hoga
    try:
        from database import save_to_mongo as _stm
        await _stm()
    except Exception:
        pass

    await event.edit(
        f"✅ **Source Add Ho Gaya!**\n\n"
        f"🆔 ID: `{chat_id}`\n\n"
        f"Ab is channel ke messages forward honge.\n"
        f"Forwarding ON karke test karo! 🚀",
        buttons=[
            [Button.inline("▶️ Forwarding Start Karo", b"start_engine")],
            [Button.inline("🏠 Main Menu", b"main_menu")]
        ]
    )


# ==========================================
# CALLBACK HANDLER (manual_rem)
# ==========================================
@bot.on(events.CallbackQuery(pattern=b"man_rem_"))
async def handle_manual_rem_callback(event):
    await event.answer()
    user_id = event.sender_id

    # BUG 42 FIX: GLOBAL_STATE se check
    if is_blocked(user_id):
        return await event.answer("🚫 Banned.", alert=True)

    mode = event.data.decode().split("_")[-1]
    get_user_data(event.sender_id)["step"] = f"wait_man_rem_{mode}"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        f"Send the **{mode.upper()}** Link, ID, or Username you want to remove:" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""),
        buttons=[Button.inline("🔙 Cancel", b"main_menu")]
    )


# ==========================================
# INPUT HANDLER
# ==========================================

@bot.on(events.NewMessage(pattern=r'/cancel'))
async def cancel_handler(event):
    if _is_queued_message(event): return
    if not event.is_private:
        return
    user_id = event.sender_id
    data = get_user_data(user_id)
    step = data.get("step")
    if step:
        data["step"] = None
        data.pop("step_since", None)  # BUG 14 FIX: step_since clear karo warna timeout loop fire karega
        # Temp data bhi clear karo
        data.pop("temp_data", None)
        # Active login client bhi disconnect karo
        if step in ("wait_phone", "wait_otp", "wait_pass") and user_id in active_clients:
            try:
                await active_clients[user_id].disconnect()
            except Exception:
                pass
            active_clients.pop(user_id, None)
        _lang = get_lang(user_id)
        _cancel_msg = (
            "✅ **Cancelled!**\n\nJo bhi chal raha tha band kar diya.\nMain menu pe wapas aa gaye."
            if _lang == "hi" else
            "✅ **Cancelled!**\n\nAll steps cleared.\nYou're back at the main menu."
        )
        from ui.main_menu import get_main_buttons
        await event.respond(_cancel_msg, buttons=get_main_buttons(user_id))
    else:
        await event.respond(t(user_id, "nothing_to_cancel"))


@bot.on(events.NewMessage(pattern=r'/help'))
async def help_handler(event):
    if _is_queued_message(event): return
    if not event.is_private:
        return
    uid = event.sender_id
    from telethon import Button
    try:
        await event.respond(
            "📚 **Help Guide**\n\nKya jaanna chahte ho?",
            buttons=[
                [Button.inline("🚀 Getting Started",  b"help_start")],
                [Button.inline("⚙️ Global Settings",  b"help_settings"),
                 Button.inline("🔧 Src Config",        b"help_srcconfig")],
                [Button.inline("📺 Source/Dest Add",  b"help_add_channel"),
                 Button.inline("♻️ Duplicate Filter", b"help_dup")],
                [Button.inline("🚫 Link Blocker",      b"help_link_blocker"),
                 Button.inline("🔄 Replacements",      b"help_replacements")],
                [Button.inline("⏰ Scheduler",         b"help_scheduler"),
                 Button.inline("🛍️ Affiliate",         b"help_affiliate")],
                [Button.inline("💾 Backup/Restore",   b"help_backup"),
                 Button.inline("❓ Common Problems",  b"help_problems")],
                [Button.inline("🏠 Main Menu",         b"main_menu")],
            ]
        )
    except Exception:
        await event.respond(
            "📚 **Help Guide**\n\n"
            "Bot use karne ke liye Main Menu → ❓ Help button dabao.\n\n"
            "Quick commands:\n"
            "• /start — Bot shuru karo\n"
            "• /status — Bot ki status dekho\n"
            "• /cancel — Koi bhi step cancel karo\n"
            "• /stats — Apni stats dekho\n\n" + _get_owner_footer()
        )


@bot.on(events.NewMessage(pattern=r'/status'))
async def status_handler(event):
    if _is_queued_message(event): return
    if not event.is_private:
        return
    user_id = event.sender_id
    data = get_user_data(user_id)
    from database import user_sessions
    from premium import is_premium_user, get_remaining_days
    session_ok  = user_id in user_sessions and user_sessions[user_id].is_connected()
    running     = data.get("settings", {}).get("running", False)
    is_prem     = is_premium_user(user_id)
    src_count   = len(data.get("sources", []))
    dest_count  = len(data.get("destinations", []))
    status_icon = "🟢" if (session_ok and running) else "🔴"
    # BUG 17 FIX: rem_days safe format — get_remaining_days return type varies
    if is_prem:
        try:
            rem_days = get_remaining_days(user_id)
            prem_txt = f"✅ Premium ({rem_days})" if rem_days else "✅ Premium (Lifetime ♾️)"
        except Exception:
            prem_txt = "✅ Premium"
    else:
        prem_txt = "❌ Free"
    await event.respond(
        f"📊 **Bot Status**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🔌 Session: {'✅ Connected' if session_ok else '❌ Disconnected'}\n"
        f"▶️ Forwarding: {'✅ Running' if running else '❌ Stopped'}\n"
        f"{status_icon} Overall: {'Active' if (session_ok and running) else 'Inactive'}\n\n"
        f"💎 {prem_txt}\n"
        f"📺 Sources: `{src_count}` | 📤 Destinations: `{dest_count}`\n\n" + _get_owner_footer(),
        buttons=[
            [Button.inline("▶️ Start" if not running else "⏹ Stop",
                           b"start_engine" if not running else b"stop_engine")],
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ]
    )



@bot.on(events.NewMessage(pattern=r'/menu'))
async def menu_handler(event):
    if _is_queued_message(event): return
    if not event.is_private:
        return
    user_id = event.sender_id

    # Force Subscribe check
    try:
        if get_fs_config().get("enabled"):
            ok = await check_force_subscribe(event)
            if not ok:
                return
    except Exception as _fs_e:
        logger.warning(f"[MENU] force_sub error: {_fs_e}")

    try:
        from ui.main_menu import get_main_buttons
        data = get_user_data(user_id)
        main_btns = get_main_buttons(user_id)
        from ui.main_menu import _build_menu_text
        txt = _build_menu_text(user_id)
        await event.respond(txt, buttons=main_btns)
    except Exception as e:
        logger.warning("[MENU] Error: {e}")
        _fb = "🏠 Main Menu" if get_lang(user_id) == "en" else "🏠 Main Menu"
        await event.respond(_fb, buttons=[[Button.inline(t(user_id, "menu_label"), b"main_menu")]])

@bot.on(events.NewMessage())
async def input_handler(event):
    if not event.is_private:
        return

    user_id = event.sender_id

    # ✅ Queued message check — restart se pehle ke messages ignore
    if _is_queued_message(event): return

    # BUG 42 FIX: GLOBAL_STATE["blocked_users"] se check karo (db nahi)
    if is_blocked(user_id):
        return

    # Maintenance mode
    if GLOBAL_STATE.get("maintenance_mode") and user_id not in GLOBAL_STATE.get("admins", {}):
        return

    update_last_active(user_id)
    data = get_user_data(user_id)
    step = data.get("step")

    # BUG 20 FIX: step_since refresh karo - user active hai, timeout reset
    if step and not data.get("step_since"):
        data["step_since"] = time.time()  # FIX: import alias hataya — time already imported

    # --- /start aur commands skip karo (upar handle ho gaye) ---
    if event.text and event.text.startswith("/"):
        return

    if not step:
        # Force subscribe check — even without step
        if get_fs_config().get("enabled"):
            ok = await check_force_subscribe(event)
            if not ok:
                return
        # Koi step nahi — main menu dikhao
        if event.text and event.text.lower().strip() in ["menu", "start", "hi", "hello", "hey"]:
            from ui.main_menu import get_main_buttons
            await event.respond("🏠 Main Menu:", buttons=get_main_buttons(user_id))
        elif event.text and not event.text.startswith("/"):
            # User ne kuch type kiya par koi active step nahi — gentle guide
            _lang = get_lang(user_id)
            _msg = t(user_id, "random_text_reply")
            from ui.main_menu import get_main_buttons
            await event.respond(_msg, buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]])
        return

    # --- PAYMENT INPUTS ---
    if step in ("adm_upi_id_input", "adm_upi_name_input", "adm_edit_plans_input",
                "wait_payment_screenshot", "adm_set_default_curr_input", "adm_add_alt_curr_input"):
        handled = await payment.handle_payment_inputs(event, user_id, step)
        if handled:
            return

    # --- MESSAGE LIMIT INPUTS ---
    # ── Support ticket system steps ─────────────────────────────────────
    try:
        from support import get_support_step_handler
        support_handlers = get_support_step_handler()
        if step in support_handlers:
            return await support_handlers[step](event, user_id, data)
    except Exception as _sup_err:
        pass

    if step == "wait_contact_admin_msg":
        msg_text = event.text.strip() if event.text else ""
        has_media = bool(event.media or event.file or event.photo)
        if not msg_text and not has_media:
            await event.respond(t(user_id, "empty_msg"))
            return True
        data["step"] = None
        try:
            me = await event.get_sender()
            user_name = ((getattr(me, "first_name","") or "") + " " + (getattr(me, "last_name","") or "")).strip() or f"User {user_id}"
            username  = f"@{me.username}" if getattr(me,"username",None) else "no username"
        except Exception:
            user_name, username = f"User {user_id}", "unknown"
        from config import OWNER_ID
        admin_msg = (
            f"📩 **New User Message**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 **From:** {user_name} ({username})\n🆔 **ID:** `{user_id}`\n\n"
            f"💬 **Message:**\n{msg_text or '[Media attached]'}"
        )
        all_admins = list(set([OWNER_ID] + [int(k) for k in GLOBAL_STATE.get("admins", {}).keys()]))
        notified = False
        for aid in all_admins:
            try:
                await bot.send_message(int(aid), admin_msg,
                    buttons=[[Button.inline(f"↩️ Reply", f"reply_to_user_{user_id}".encode())]])
                if has_media:
                    await event.forward_to(int(aid))
                notified = True
            except Exception:
                pass
        if notified:
            await event.respond(t(user_id, "contact_sent"),
                buttons=[[Button.inline(t(user_id, "menu_label"), b"main_menu")]])
        else:
            await event.respond(t(user_id, "contact_failed"),
                buttons=[[Button.inline(t(user_id, "menu_label"), b"main_menu")]])
        return True

    elif step == "wait_admin_reply_msg":
        reply_text = event.text.strip() if event.text else ""
        if not reply_text:
            await event.respond(t(user_id, "empty_msg"))
            return True
        target_uid = data.get("temp_data", {}).get("reply_to_uid")
        if not target_uid:
            await event.respond(t(user_id, "error_not_found"))
            data["step"] = None
            return True
        data["step"] = None
        data["temp_data"] = {}
        try:
            await bot.send_message(int(target_uid),
                f"📨 **Admin ka Reply:**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{reply_text}",
                buttons=[[Button.inline("📞 Contact Again", b"contact_admin"),
                          Button.inline("🏠 Menu", b"main_menu")]])
            await event.respond(f"✅ Reply user `{target_uid}` ko bhej di!",
                buttons=[[Button.inline("🏠 Menu", b"main_menu")]])
        except Exception as e:
            await event.respond(f"❌ Reply nahi gayi: {str(e)[:80]}")
        return True

    # --- MESSAGE LIMIT INPUTS ---
    if step == "adm_notice_text_input":
        text = event.text.strip()
        if not text:
            await event.respond("❌ Khali text nahi chalega!")
            return True
        try:
            from ui.admin_menu import get_notice_config
            cfg = get_notice_config()
            cfg["text"] = text
            cfg["enabled"] = True
            cfg["updated_at"] = int(time.time())
            save_persistent_db()
            await event.respond(
                "✅ **Notice Set Ho Gayi!**\n\n"
                f"`{text[:200]}{'...' if len(text)>200 else ''}`\n\n"
                "Users `/rules` command se dekh sakte hain।",
                buttons=[[Button.inline("📌 Notice Panel", b"adm_notice_panel"),
                          Button.inline("🏠 Menu", b"main_menu")]]
            )
        except Exception as e:
            await event.respond(f"❌ Error: {str(e)[:80]}")
        get_user_data(user_id)["step"] = None
        return True

    elif step == "adm_welcome_text_input":
        text = event.text.strip()
        if not text:
            await event.respond("❌ Khali text nahi chalega!")
            return True
        cfg = GLOBAL_STATE.setdefault("welcome_msg_config", {})
        cfg["text"] = text
        cfg["enabled"] = True
        save_persistent_db()
        await event.respond(
            "✅ **Welcome Message Set Ho Gaya!**",
            buttons=[[Button.inline("👁 Welcome Panel", b"adm_welcome_msg"),
                      Button.inline("🏠 Menu", b"main_menu")]]
        )
        get_user_data(user_id)["step"] = None
        return True

    if step in ("adm_daily_limit_input", "adm_monthly_limit_input"):
        handled = await msg_limit.handle_limit_inputs(event, user_id, step)
        if handled:
            return

    # --- ANTI-SPAM STEPS ---
    if step and step.startswith("as_input_"):
        try:
            from ui.anti_spam_menu import handle_antispam_steps
            if await handle_antispam_steps(event, user_id, step):
                return
        except Exception as _ase:
            logger.warning("[AntiSpam] Step error: {_ase}")

    # --- ALERT DESTINATION INPUTS ---
    if step == "adm_alert_dest_input":
        from health_monitor import handle_alert_dest_inputs
        handled = await handle_alert_dest_inputs(event, user_id, step)
        if handled:
            return

    # --- FORCE SUBSCRIBE INPUTS ---
    if step in ("adm_fs_channel_input", "adm_fs_test_uid_input"):
        handled = await force_subscribe.handle_fs_inputs(event, user_id, step)
        if handled:
            return

    # --- PREMIUM INPUTS ---
    if step in ("adm_give_prem_input", "adm_rem_prem_input", "adm_prem_price_input",
                "adm_prem_paymsg_input", "adm_max_days_input",
                "adm_src_limit_input", "adm_dest_limit_input",
                "adm_give_prem_days_input"):  # BUG 43 FIX + uid-specific give prem
        if step == "adm_give_prem_days_input":
            # Direct give from user profile
            try:
                days = int(event.text.strip())
                # BUG 24 FIX: temp_data missing hone par safe access
                target_uid = data.get("temp_data", {}).get("give_prem_uid")
                if not target_uid:
                    await event.respond("❌ Error: User ID missing. Try again from user profile.")
                    data["step"] = None
                    return
                await premium.give_premium(target_uid, days, given_by=user_id)
                data["step"] = None
                data.get("temp_data", {}).pop("give_prem_uid", None)
                await save_db_async()
                duration = "Lifetime" if days == 0 else f"{days} din"
                await event.respond(
                    f"✅ User `{target_uid}` ko **{duration}** ka Premium diya gaya!",
                    buttons=[Button.inline("🔙 User Profile", f"adm_view_u_{target_uid}".encode())]
                )
            except ValueError:
                await event.respond("❌ Sirf number daalo.", buttons=[Button.inline("🔙 Back", b"adm_premium")])
            return
        handled = await premium.handle_premium_inputs(event, user_id, step)
        if handled:
            return

    # ── NEW FEATURE STEPS ────────────────────────────
    _handled = await _handle_new_feature_steps(event, step, user_id, data)
    if _handled:
        return

    # --- REFER INPUTS ---
    if step in ("adm_refer_days_input", "adm_refer_needed_input", "adm_refer_group_input"):
        handled = await refer.handle_refer_inputs(event, user_id, step)
        if handled:
            return

    # --- ADMIN SEARCH ---
    if step == "adm_search_user_input":
        try:
            target_id = int(event.text.strip())
            if target_id in db:
                data["step"] = None
                await event.respond(f"🔍 User `{target_id}` found.", buttons=[
                    [Button.inline("👤 View Profile", f"adm_view_u_{target_id}".encode())],
                    [Button.inline("🔙 Back", b"adm_user_mg")]
                ])
            else:
                await event.respond("❌ User not found in DB.", buttons=[Button.inline("🔙 Back", b"adm_user_mg")])
        except ValueError:
            await event.respond("❌ Valid User ID bhejo।")
        return

    # --- ADMIN ADD ---
    if step == "adm_add_admin_input":
        try:
            parts = event.text.strip().split()
            # BUG 35 FIX: Format instruction tha blank — ab properly handle
            target_id = int(parts[0])
            role = parts[1] if len(parts) > 1 else "Support"
            from admin import update_admin_role
            success, msg = update_admin_role(target_id, role, user_id)
            data["step"] = None
            save_persistent_db()
            await event.respond(
                f"{'✅' if success else '❌'} {msg}",
                buttons=[Button.inline("🔙 Back", b"adm_main")]
            )
        except (ValueError, IndexError):
            await event.respond("❌ Format: `USER_ID ROLE`\nRoles: Support, Moderator, Super Admin")
        return

    # --- LOGIN STEPS ---
    if step == "wait_phone":
        phone = event.text.strip()
        if not phone.startswith("+"):
            phone = "+" + phone

        if user_id in active_clients:
            try:
                await active_clients[user_id].disconnect()
                del active_clients[user_id]
            except Exception:
                pass

        data["phone"] = phone
        await event.respond("⏳ Sending OTP... Please wait." + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") )

        client = TelegramClient(StringSession(), API_ID, API_HASH)
        await client.connect()

        try:
            res = await client.send_code_request(phone)
            active_clients[user_id] = client
            data["hash"] = res.phone_code_hash
            # ✅ Problem 21: Auto-set timezone from phone country code
            try:
                from time_helper import auto_set_timezone
                auto_set_timezone(user_id, data.get("phone", ""))
            except Exception:
                pass
            data["step"] = "wait_otp"
            data["step_since"] = time.time()

            # ✅ Show detected timezone in OTP confirmation
            try:
                from time_helper import detect_tz_from_phone, PHONE_TZ_MAP
                _detected_tz = detect_tz_from_phone(phone)
                _tz_note = f"\n🕐 _Timezone auto-detect: **{_detected_tz}**_" if _detected_tz else ""
            except Exception:
                _tz_note = ""

            await event.respond(
                "✅ **OTP Bhej Diya Gaya!**\n\n"
                "📲 **OTP kahan milega:**\n"
                "• Telegram App → Settings → Devices → Active Sessions\n"
                "• Ya aapke registered number par SMS\n\n"
                "✏️ **Format mein bhejo:** `HELLO12345`\n"
                "_(Pehle HELLO likho, phir code)_" +
                _tz_note +
                (("\n\n" + _get_owner_footer()) if _get_owner_footer() else "") +
                "\n\n⏳ _OTP 2 minute mein expire hoga_",
                buttons=[[Button.inline("❌ Cancel", b"main_menu")]]
            )

            # BUG 9: OTP timeout — check step before clearing
            async def otp_timeout(uid):
                await asyncio.sleep(120)
                u_data = get_user_data(uid)
                if u_data.get("step") == "wait_otp":
                    u_client = active_clients.pop(uid, None)
                    if u_client:
                        try:
                            await u_client.disconnect()
                        except Exception:
                            pass
                    u_data["step"] = None
                    u_data.pop("step_since", None)
                    _tmsg = (
                        "⏰ **OTP Timeout!**\n\nOTP 2 minute mein enter nahi kiya.\nDobara login karne ke liye /start dabao."
                        if get_lang(uid) == "hi" else
                        "⏰ **OTP Timeout!**\n\nYou didn't enter OTP in 2 minutes.\nPress /start to try again."
                    )
                    try:
                        await bot.send_message(uid, _tmsg,
                            buttons=[[Button.inline("🔁 Try Again", b"login_menu")]])
                    except Exception:
                        pass

            asyncio.create_task(otp_timeout(user_id))

        except errors.PhoneNumberInvalidError:
            await event.respond(
                "❌ **Phone Number Galat Hai!**\n\n"
                "Sahi format: `+91XXXXXXXXXX` (country code ke saath)\n"
                "Example: `+91XXXXXXXXXX`"
            )
            await client.disconnect()
            active_clients.pop(user_id, None)
            data["step"] = "wait_phone"
            data["step_since"] = time.time()
        except errors.PhoneNumberBannedError:
            await event.respond(
                "🚫 **Ye Number Telegram ne Ban Kar Diya Hai!**\n\n"
                "Doosra number try karo."
            )
            data["step"] = None
            await client.disconnect()
            active_clients.pop(user_id, None)
        except errors.FloodWaitError as e:
            await event.respond(
                f"⏱ **Too Many Requests!**\n\n"
                f"Telegram se `{e.seconds}` seconds baad OTP maango."
            )
            data["step"] = None
            await client.disconnect()
            active_clients.pop(user_id, None)
        except Exception as e:
            err_str = str(e).lower()
            if "phone" in err_str and "invalid" in err_str:
                msg = "❌ Phone number invalid. Country code ke saath daalo: `+91XXXXXXXXXX`"
            elif "flood" in err_str:
                msg = "⏱ Too many requests. Please try again later." if get_lang(user_id) == "en" else "⏱ Too many requests. Kuch der baad try karo."
            else:
                msg = f"❌ Error: {str(e)[:100]}\nPhone number sahi format mein daalo: `+91XXXXXXXXXX`"
            await event.respond(msg)
            await client.disconnect()
            active_clients.pop(user_id, None)

    elif step == "wait_otp":
        # ── Brute-force protection ────────────────────────────────────────
        _now_bf = time.time()
        _bf = _otp_attempts.get(user_id, (0, _now_bf))
        _bf_count, _bf_first = _bf
        # Reset window if lockout period passed
        if _now_bf - _bf_first > _OTP_LOCKOUT_SECS:
            _bf_count = 0
            _bf_first = _now_bf
        if _bf_count >= _OTP_MAX_ATTEMPTS:
            _remaining = int(_OTP_LOCKOUT_SECS - (_now_bf - _bf_first))
            await event.respond(
                f"🔒 **Too many wrong attempts!**\n\n"
                f"Security lockout: {_remaining}s remaining.\n"
                f"Please wait before trying again."
            )
            return
        # ─────────────────────────────────────────────────────────────────
        if not event.text or not event.text.startswith("HELLO"):
            await event.respond("❌ **Wrong Format!** Example: `HELLO12345`")
            return

        otp = event.text.replace("HELLO", "").strip()
        client = active_clients.get(user_id)

        if not client:
            await event.respond("❌ Session expired. Please /start again." if get_lang(user_id) == "en" else "❌ Session expired. Dobara /start karo।")
            data["step"] = None
            return

        try:
            await client.sign_in(data["phone"], otp, phone_code_hash=data["hash"])
            # Clear brute-force counter on success
            _otp_attempts.pop(user_id, None)
            session_str = client.session.save()
            # FIX 6: Validate session string length before saving
            if not session_str or len(session_str) < 50:
                logger.error(f"Invalid session string for user {user_id} (len={len(session_str or '')})")
                await event.respond("❌ Session invalid — please try /start again.")
                return
            data["session"] = session_str
            data["step"] = None
            # BUG LOGIN FIX: running=True set karo taaki worker forward kare
            data["settings"]["running"] = True
            active_clients.pop(user_id, None)

            # RENDER FREE TIER: Single-process architecture
            # worker.py alag process nahi — sab kuch main.py mein asyncio tasks ke through
            from worker_manager import assign_worker
            worker_id = assign_worker(user_id)
            logger.info(f"User {user_id} assigned to Worker {worker_id} (in-process)")

            # FIX 31: start_user_forwarder directly in main event loop
            asyncio.create_task(start_user_forwarder(user_id, session_str))

            from ui.main_menu import get_main_buttons
            await event.respond(
                "✅ **Login Successful!**\n\n🔄 Forwarding automatically shuru ho gaya!" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "",
                buttons=get_main_buttons(user_id)
            )
            await save_db_async()

        except errors.PhoneCodeInvalidError:
            # Increment brute-force counter
            _bf_count += 1
            _otp_attempts[user_id] = (_bf_count, _bf_first)
            _remaining_tries = _OTP_MAX_ATTEMPTS - _bf_count
            _warn = f"\n⚠️ {_remaining_tries} attempts remaining." if _remaining_tries > 0 else "\n🔒 Next wrong attempt will lock you out for 5 minutes."
            await event.respond(
                "❌ **OTP Galat Hai!**\n\n"
                "Sahi code enter karo.\n"
                f"Format: `HELLO` + OTP (jaise `HELLO12345`){_warn}"
            )
            # Step clear mat karo — same session se retry ho sakta hai
        except errors.PhoneCodeExpiredError:
            await event.respond(
                "⏰ **OTP Expire Ho Gaya!**\n\n"
                "OTP 2 minute ka hota hai.\n"
                "To get a new OTP, /start and enter your phone number again." if get_lang(user_id) == "en" else "Naya OTP lene ke liye /start karo aur phone number phir se daalo."
            )
            data["step"] = None
            if client:
                await client.disconnect()
                active_clients.pop(user_id, None)
        except errors.PhoneCodeEmptyError:
            await event.respond("❌ OTP khali nahi ho sakta! Format: `HELLO12345`")
        except errors.SessionPasswordNeededError:
            await event.respond("🔐 2-Step Verification ON hai। Apna Telegram password bhejo.")
            data["step"] = "wait_pass"
            data["step_since"] = time.time()
            if client:
                active_clients[user_id] = client
        except errors.FloodWaitError as e:
            await event.respond(
                f"⏱ **Too Many Attempts!**\n\n"
                f"Telegram ne block kiya hai. `{e.seconds}` seconds baad try karo."
            )
            data["step"] = None
        except Exception as e:
            err_str = str(e).lower()
            if "invalid" in err_str:
                msg = "❌ OTP invalid hai. Sahi OTP enter karo."
            elif "expire" in err_str or "expired" in err_str:
                msg = "⏰ OTP expire ho gaya. /start karke dobara try karo."
            else:
                msg = f"❌ Login error: {str(e)[:100]}"
            await event.respond(msg)
            data["step"] = None
            if client:
                await client.disconnect()
                active_clients.pop(user_id, None)

    elif step == "wait_pass":
        password = event.text.strip()
        try:
            await event.delete()
        except Exception:
            pass

        client = active_clients.get(user_id)

        if not client:
            await event.respond("❌ Session expired. Please /start again." if get_lang(user_id) == "en" else "❌ Session expired. Dobara /start karo।")
            data["step"] = None
            return

        try:
            await client.sign_in(password=password)
            session_str = client.session.save()
            # FIX 6: Validate session string length before saving
            if not session_str or len(session_str) < 50:
                logger.error(f"Invalid session string for user {user_id} (len={len(session_str or '')})")
                await event.respond("❌ Session invalid — please try /start again.")
                return
            data["session"] = session_str
            data["step"] = None
            # BUG LOGIN FIX: running=True set karo
            data["settings"]["running"] = True
            active_clients.pop(user_id, None)

            # WORKER ARCHITECTURE: Worker ko assign karo
            from worker_manager import assign_worker
            assign_worker(user_id)

            # UI SESSION: Pinned chats + display names ke liye
            asyncio.create_task(start_user_forwarder(user_id, session_str))

            from ui.main_menu import get_main_buttons
            await event.respond(
                "✅ **Login Successful!**\n\n🔄 Forwarding automatically shuru ho gaya!" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "",
                buttons=get_main_buttons(user_id)
            )
            await save_db_async()

        except Exception as e:
            await event.respond("❌ Password galat hai ya session expire ho gaya।")
            data["step"] = None
            if client:
                await client.disconnect()
                active_clients.pop(user_id, None)

    # --- REPLACEMENT STEPS ---
    elif step == "wait_repl_old":
        data["temp_data"]["repl_old"] = event.text.strip()
        data["step"] = "wait_repl_new"
        data["step_since"] = time.time()  # BUG 20 FIX
        await event.respond("Step 2/2: Send the **New Value**.")

    elif step == "wait_repl_new":
        old_v = data["temp_data"].get("repl_old")
        new_v = event.text.strip()
        data["replacements"][old_v] = new_v
        data["step"] = None
        await save_db_async()
        await event.respond(
            f"✅ Replacement added: `{old_v}` ➔ `{new_v}`",
            buttons=[Button.inline("🔄 Replace Menu", b"replace_menu")]
        )

    # --- LINK BLOCKER ---
    elif step == "wait_link_block_input":
        if "|" in event.text:
            link_v, limit_v = event.text.split("|", 1)
            norm = normalize_url(link_v)
            try:
                l_val = int(limit_v.strip())
                data["link_limits"][norm] = l_val
                data["blocked_links"][norm] = 0
                data["step"] = None
                await event.respond(
                    f"✅ Blocker: `{norm}` (Limit: {l_val})",
                    buttons=[Button.inline("🚫 Blocker Menu", b"link_block_menu")]
                )
            except Exception:
                await event.respond("❌ Count must be a number.")
        else:
            norm = normalize_url(event.text)
            data["link_limits"][norm] = 0
            data["blocked_links"][norm] = 0
            data["step"] = None
            await event.respond(
                f"✅ Blocked `{norm}` forever.",
                buttons=[Button.inline("🚫 Blocker Menu", b"link_block_menu")]
            )

    # --- SOURCE ADD ---
    elif step == "wait_src_input":
        # ✅ FIX: Multiple IDs/links ek saath add — newline ya comma se alag karo
        raw_input = event.text.strip()
        entries = [e.strip() for e in re.split(r'[,\n\r]+', raw_input) if e.strip()]

        u_client = user_sessions.get(user_id)
        added = []
        skipped = []
        errors_list = []

        for val in entries:
            # Limit check pehle
            allowed, limit_msg = check_source_limit(user_id, len(data["sources"]))
            if not allowed:
                data["step"] = None
                await event.respond(limit_msg, buttons=[Button.inline("💎 Premium Info", b"premium_info")])
                break

            # Resolve ID + Name (ek hi API call mein dono)
            channel_name_resolved = None
            if u_client:
                try:
                    resolved, channel_name_resolved = await resolve_id_and_name(u_client, val)
                    if resolved:
                        val = resolved
                        # Name turant cache mein save karo — unknown channels ke liye bhi
                        if channel_name_resolved:
                            data.setdefault("channel_names", {})[str(val)] = channel_name_resolved
                except (ChannelPrivateError, InviteHashInvalidError):
                    errors_list.append(f"`{val}` — Channel private hai, pehle join karo")
                    continue
                except Exception:
                    channel_name_resolved = None
            else:
                if val.lstrip("-").isdigit():
                    pass
                elif val.startswith("@"):
                    pass
                elif "t.me/c/" in val:
                    parts = val.split("/")
                    try:
                        cid = parts[parts.index("c") + 1]
                        val = f"-100{cid}"
                    except Exception:
                        pass

            names_id = data.get("channel_names_id", {})

            # Loop prevention check
            if channel_already_exists(val, data.get("destinations", []), names_id):
                skipped.append(f"`{val}` — already Destination hai (loop ban jaata)")
                continue

            # Duplicate check
            if channel_already_exists(val, data.get("sources", []), names_id):
                skipped.append(f"`{val}` — already Source mein hai")
                continue

            # Premium limit check
            try:
                _cfg = get_premium_config()
                _limit = _cfg.get("free_source_limit", 2)
                if not is_premium_user(user_id) and len(data["sources"]) >= _limit:
                    data["step"] = None
                    await event.respond(
                        f"❌ **Source Limit Reached!**\n\nFree plan mein max `{_limit}` sources allowed hain।\n\n"
                        "💎 **Premium** lo unlimited sources ke liye — /premium",
                        buttons=[[Button.inline("💎 Premium Info", b"premium_info")],
                                 [Button.inline("🏠 Menu", b"main_menu")]]
                    )
                    break
            except Exception:
                pass

            data["sources"].append(val)

            # Channel title — resolve_id_and_name ne pehle se name de diya (extra API call nahi)
            channel_title = (channel_name_resolved
                             or data.get("channel_names", {}).get(str(val)))
            if not channel_title and u_client:
                try:
                    channel_title = await get_display_name(u_client, val, user_id)
                except Exception:
                    pass
            channel_title = channel_title or str(val)
            added.append(f"📺 {channel_title} — `{val}`")

        data["step"] = None
        await save_db_async()

        # Summary message banao
        lines = []
        if added:
            lines.append(f"✅ **{len(added)} Source(s) Add Hue:**")
            lines.extend(added)
        if skipped:
            lines.append(f"\n⚠️ **{len(skipped)} Skip Hue (already exist):**")
            lines.extend(skipped)
        if errors_list:
            lines.append(f"\n❌ **{len(errors_list)} Error(s):**")
            lines.extend(errors_list)

        summary = "\n".join(lines) if lines else "⚠️ Koi bhi source add nahi hua."
        await event.respond(
            summary,
            buttons=[
                [Button.inline("📋 Source List Dekho", b"ps_menu")],
                [Button.inline("🏠 Main Menu", b"main_menu")],
            ]
        )
    # --- DESTINATION ADD ---
    elif step == "wait_dest_input":
        # ✅ FIX: Multiple IDs/links ek saath add — newline ya comma se alag karo
        raw_input = event.text.strip()
        entries = [e.strip() for e in re.split(r'[,\n]+', raw_input) if e.strip()]

        u_client = user_sessions.get(user_id)
        added = []
        skipped = []
        errors_list = []

        for val in entries:
            # Limit check pehle
            allowed, limit_msg = check_dest_limit(user_id, len(data["destinations"]))
            if not allowed:
                data["step"] = None
                await event.respond(limit_msg, buttons=[Button.inline("💎 Premium Info", b"premium_info")])
                break

            # Resolve ID + Name (ek hi API call mein dono)
            channel_name_resolved = None
            if u_client:
                try:
                    resolved, channel_name_resolved = await resolve_id_and_name(u_client, val)
                    if resolved:
                        val = resolved
                        if channel_name_resolved:
                            data.setdefault("channel_names", {})[str(val)] = channel_name_resolved
                except (ChannelPrivateError, InviteHashInvalidError):
                    errors_list.append(f"`{val}` — Channel private hai, pehle join karo")
                    continue
                except Exception:
                    channel_name_resolved = None
            else:
                if val.lstrip("-").isdigit():
                    pass
                elif val.startswith("@"):
                    pass
                elif "t.me/c/" in val:
                    parts = val.split("/")
                    try:
                        cid = parts[parts.index("c") + 1]
                        val = f"-100{cid}"
                    except Exception:
                        pass

            names_id = data.get("channel_names_id", {})

            # Loop prevention check
            if channel_already_exists(val, data.get("sources", []), names_id):
                skipped.append(f"`{val}` — already Source hai (loop ban jaata)")
                continue

            # Duplicate check
            if channel_already_exists(val, data.get("destinations", []), names_id):
                skipped.append(f"`{val}` — already Destination mein hai")
                continue

            # Premium limit check
            try:
                _cfg = get_premium_config()
                _dlimit = _cfg.get("free_dest_limit", 2)
                if not is_premium_user(user_id) and len(data["destinations"]) >= _dlimit:
                    data["step"] = None
                    await event.respond(
                        f"❌ **Destination Limit Reached!**\n\nFree plan mein max `{_dlimit}` destinations allowed hain।\n\n"
                        "💎 **Premium** lo unlimited destinations ke liye — /premium",
                        buttons=[[Button.inline("💎 Premium Info", b"premium_info")],
                                 [Button.inline("🏠 Menu", b"main_menu")]]
                    )
                    break
            except Exception:
                pass

            data["destinations"].append(val)

            # Channel title — resolve_id_and_name ne pehle se name de diya
            dest_title = (channel_name_resolved
                          or data.get("channel_names", {}).get(str(val)))
            if not dest_title and u_client:
                try:
                    dest_title = await get_display_name(u_client, val, user_id)
                except Exception:
                    pass
            dest_title = dest_title or str(val)
            perm_warn = ""
            if u_client:
                # ── Issue #16: Auto permission check ─────────────────────────
                try:
                    from telethon.tl.functions.channels import GetParticipantRequest
                    from telethon.tl.types import ChannelParticipantAdmin, ChannelParticipantCreator
                    _entity = await u_client.get_entity(val)
                    _me = await u_client.get_me()
                    _part = await u_client(GetParticipantRequest(channel=_entity, participant=_me.id))
                    _p = _part.participant
                    if isinstance(_p, (ChannelParticipantAdmin, ChannelParticipantCreator)):
                        _rights = getattr(_p, "admin_rights", None)
                        if _rights and not getattr(_rights, "post_messages", True):
                            perm_warn = " ⚠️ _Post permission nahi — admin rights check karo_"
                    else:
                        perm_warn = " ⚠️ _Admin nahi ho — post nahi kar paoge_"
                except Exception:
                    pass  # Permission check silently skip if can't verify
            added.append(f"📺 {dest_title} — `{val}`{perm_warn}")

        data["step"] = None
        await save_db_async()

        # Summary message banao
        lines = []
        if added:
            lines.append(f"✅ **{len(added)} Destination(s) Add Hue:**")
            lines.extend(added)
        if skipped:
            lines.append(f"\n⚠️ **{len(skipped)} Skip Hue (already exist):**")
            lines.extend(skipped)
        if errors_list:
            lines.append(f"\n❌ **{len(errors_list)} Error(s):**")
            lines.extend(errors_list)

        summary = "\n".join(lines) if lines else "⚠️ Koi bhi destination add nahi hua."
        await event.respond(
            summary,
            buttons=[
                [Button.inline("📋 Dest List Dekho", b"dest_menu")],
                [Button.inline("🏠 Main Menu", b"main_menu")],
            ]
        )
    # --- SETTINGS ---
    elif step == "wait_start_msg":
        raw = event.raw_text.strip()
        if len(raw) > 500:
            await event.respond(
                f"❌ Start message bahut lamba hai ({len(raw)} chars).\n"
                "Maximum 500 characters allowed.",
                buttons=[Button.inline("✏️ Dobara Try Karo", b"se_start_edit")]
            )
            return True
        data["settings"]["start_msg"] = raw
        data["settings"].setdefault("start_msg_enabled", True)
        data["step"] = None
        await save_db_async()
        # Show live preview
        try:
            from forward_engine import _render_msg_template
            rendered = _render_msg_template(raw)
        except Exception:
            rendered = raw
        await event.respond(
            "✅ **Start Message Saved!**\n\n"
            "**Preview** (variables resolved):\n"
            "┌─────────────────────────\n"
            f"{rendered}\n"
            "└─────────────────────────",
            buttons=[
                [Button.inline("✏️ Start/End Msg", b"adv_msg_settings")],
                [Button.inline("🏠 Main Menu", b"main_menu")]
            ],
            parse_mode="html"
        )

    elif step == "wait_end_msg":
        raw = event.raw_text.strip()
        if len(raw) > 500:
            await event.respond(
                f"❌ End message bahut lamba hai ({len(raw)} chars).\n"
                "Maximum 500 characters allowed.",
                buttons=[Button.inline("✏️ Dobara Try Karo", b"se_end_edit")]
            )
            return True
        data["settings"]["end_msg"] = raw
        data["settings"].setdefault("end_msg_enabled", True)
        data["step"] = None
        await save_db_async()
        try:
            from forward_engine import _render_msg_template
            rendered = _render_msg_template(raw)
        except Exception:
            rendered = raw
        await event.respond(
            "✅ **End Message Saved!**\n\n"
            "**Preview** (variables resolved):\n"
            "└─────────────────────────\n"
            f"{rendered}\n"
            "─────────────────────────┘",
            buttons=[
                [Button.inline("✏️ Start/End Msg", b"adv_msg_settings")],
                [Button.inline("🏠 Main Menu", b"main_menu")]
            ],
            parse_mode="html"
        )

    elif step == "wait_kw_input":
        words = [w.strip() for w in event.text.split(",")]
        mode = data["settings"]["filter_mode"]
        target = data["settings"]["keywords_blacklist"] if mode == "Blacklist" else data["settings"]["keywords_whitelist"]
        for w in words:
            if w and w not in target:
                target.append(w)
        data["step"] = None
        await save_db_async()
        await event.respond(
            f"✅ Added {len(words)} keywords to {mode}!",
            buttons=[Button.inline("🔙 Back", b"kw_filter_menu")]
        )

    elif step == "wait_delay":
        # BUG FIX: "wait_delay" step ka handler missing tha — user manually number type
        # karta tha to koi handler nahi tha, value save nahi hoti thi, delay 0 rehta tha.
        # Ab direct seconds mein save hoga.
        try:
            val = int(event.text.strip())
            if val < 0:
                raise ValueError("Negative delay not allowed")
            data["settings"]["custom_delay"] = val
            data["step"] = None
            await save_db_async()
            await event.respond(
                f"✅ Delay set ho gaya: **{val} seconds**.",
                buttons=[Button.inline("🏠 Menu", b"main_menu")]
            )
        except Exception:
            await event.respond("❌ Invalid number. Sirf positive number bhejo (jaise: `5`)")

    elif step == "wait_delay_val":
        try:
            val = int(event.text.strip())
            unit = data["temp_data"].get("delay_unit", "Seconds")
            multiplier = 60 if unit == "Minutes" else 3600 if unit == "Hours" else 86400 if unit == "Days" else 1
            total = val * multiplier
            data["settings"]["custom_delay"] = total
            data["step"] = None
            await save_db_async()
            await event.respond(f"✅ Delay set to {val} {unit}.", buttons=[Button.inline("🏠 Menu", b"main_menu")])
        except Exception:
            await event.respond("❌ Invalid Number.")

    elif step == "wait_json_file" or step == "wait_backup_file":
        _is_backup = step == "wait_backup_file"
        if event.file and event.file.name and event.file.name.endswith(".json"):
            try:
                content = await event.download_media(file=bytes)
                loaded = json.loads(content)
                if not isinstance(loaded, dict):
                    await event.respond("❌ Invalid Backup File format. Sahi JSON file bhejo.")
                    return
                current_data = get_user_data(user_id)

                # ── Premium limit enforcement ────────────────────────────────
                # Free users cannot bypass source/dest limits via backup restore
                from premium import is_premium_user, get_premium_config as _gpc
                _is_prem = is_premium_user(user_id)
                _pc = _gpc()
                _src_limit  = None if _is_prem else _pc.get("free_source_limit", 2)
                _dest_limit = None if _is_prem else _pc.get("free_dest_limit", 2)

                for k in ["sources", "destinations", "replacements", "blocked_links",
                          "link_limits", "scheduler", "custom_forward_rules", "ui_mode",
                          "timezone", "watermark", "affiliate", "language", "forward_rules",
                          "channel_names"]:
                    if k not in loaded:
                        continue
                    val = loaded[k]
                    # Enforce limits for non-premium users
                    if k == "sources" and _src_limit is not None and isinstance(val, list):
                        val = val[:_src_limit]
                    elif k == "destinations" and _dest_limit is not None and isinstance(val, list):
                        val = val[:_dest_limit]
                    current_data[k] = val
                if "settings" in loaded:
                    current_data["settings"].update(loaded["settings"])
                    current_data["settings"]["running"] = False
                # Scheduler timezone sync
                if "timezone" in loaded and "scheduler" in current_data:
                    current_data["scheduler"]["timezone"] = loaded["timezone"]
                # Admin: force_sub global restore (with schema validation)
                from admin import is_admin as _is_admin
                from config import OWNER_ID as _OWNER_ID
                if user_id == _OWNER_ID or _is_admin(user_id):
                    from database import GLOBAL_STATE as _GS
                    if "_admin_force_sub" in loaded:
                        _fs_val = loaded["_admin_force_sub"]
                        # Validate it's a dict (not arbitrary data injection)
                        if isinstance(_fs_val, dict):
                            _GS["force_sub"] = _fs_val
                        else:
                            logger.warning(f"Backup restore: invalid _admin_force_sub type from user {user_id}, skipped")
                data["step"] = None
                data.pop("step_since", None)
                await save_db_async()
                _r_src = len(current_data.get("sources", []))
                _r_dst = len(current_data.get("destinations", []))
                _r_rep = len(current_data.get("replacements", {}))
                _r_bl  = len(current_data.get("blocked_links", {}))
                _lang  = get_lang(user_id)
                if _lang == "hi":
                    _msg = (
                        "✅ **Restore Ho Gaya!**\n\n"
                        "**Restore Kya Hua:**\n"
                        f"📥 Sources: `{_r_src}` | 📤 Destinations: `{_r_dst}`\n"
                        f"🔄 Replacements: `{_r_rep}` | 🚫 Blocked Links: `{_r_bl}`\n"
                        f"⚙️ Settings, Scheduler, Filters\n\n"
                        "⚠️ Forwarding band kar diya — safety ke liye.\n"
                        "**Start Forwarding** dabao wapas shuru karne ke liye."
                    )
                else:
                    _msg = (
                        "✅ **Restore Successful!**\n\n"
                        "**Restored:**\n"
                        f"📥 Sources: `{_r_src}` | 📤 Destinations: `{_r_dst}`\n"
                        f"🔄 Replacements: `{_r_rep}` | 🚫 Blocked Links: `{_r_bl}`\n"
                        f"⚙️ Settings, Scheduler, Filters\n\n"
                        "⚠️ Forwarding stopped for safety.\n"
                        "Press **Start Forwarding** to resume."
                    )
                await event.respond(
                    _msg,
                    buttons=[
                        [Button.inline(t(user_id, "btn_start_fwd"), b"start_engine")],
                        [Button.inline(t(user_id, "btn_main_menu"), b"main_menu")]
                    ]
                )
            except Exception as e:
                await event.respond(
                    f"❌ Restore Failed: {str(e)[:80]}\n\nSahi backup file bhejo.",
                    buttons=[[Button.inline("🔙 Back", b"backup_menu" if _is_backup else b"main_menu")]]
                )
        else:
            _lang = get_lang(user_id)
            await event.respond(
                "❌ Valid `.json` file bhejo!" if _lang == "hi" else "❌ Please send a valid `.json` backup file.",
                buttons=[[Button.inline("❌ Cancel", b"backup_menu" if _is_backup else b"main_menu")]]
            )

    elif step == "wait_dup_expiry":
        try:
            val = int(event.text.strip())
            unit = data["temp_data"].get("dup_unit", "Hours")
            hours = val / 3600 if unit == "Seconds" else val / 60 if unit == "Minutes" else val * 24 if unit == "Days" else val
            data["settings"]["dup_expiry_hours"] = hours
            data["step"] = None
            await save_db_async()
            await event.respond(
                f"✅ Duplicate expiry set to {val} {unit}.",
                buttons=[Button.inline("🔙 Back", b"dup_menu")]
            )
        except Exception:
            await event.respond("❌ Invalid Number.")

    elif step == "wait_dup_whitelist":
        words = [w.strip() for w in event.text.split(",")]
        for w in words:
            if w and w not in data["settings"]["dup_whitelist_words"]:
                data["settings"]["dup_whitelist_words"].append(w)
        data["step"] = None
        await save_db_async()
        await event.respond(f"✅ Added {len(words)} words to whitelist.", buttons=[Button.inline("🔙 Back", b"dup_menu")])

    elif step == "regex_add_input":
        # Handle regex pattern input
        _re_mod = re  # re already imported at module level
        pat = event.text.strip()
        if not pat:
            await event.respond("❌ Pattern khali nahi ho sakta!")
            return
        try:
            _re_mod.compile(pat)  # Validate regex
        except _re_mod.error as re_err:
            await event.respond(
                f"❌ Invalid regex pattern!\n\nError: `{re_err}`\n\nSahi pattern dobara bhejo.",
                buttons=[[Button.inline("❌ Cancel", b"regex_filter_menu")]]
            )
            return
        cfg = data.setdefault("regex_filters", {"enabled": False, "patterns": []})
        if pat not in cfg["patterns"]:
            cfg["patterns"].append(pat)
        data["step"] = None
        data.pop("step_since", None)
        await save_db_async()
        await event.respond(
            f"✅ **Regex Pattern Add Ho Gaya!**\n\n`{pat}`",
            buttons=[[Button.inline("🔡 Regex Filter", b"regex_filter_menu"),
                      Button.inline("🏠 Menu", b"main_menu")]]
        )

    elif step == "timef_tz_input":
        # Handle manual timezone input
        tz_val = event.text.strip()
        if not tz_val:
            await event.respond("❌ Timezone khali nahi ho sakta!")
            return
        try:
            import pytz
            pytz.timezone(tz_val)  # Validate
            valid = True
        except Exception:
            # pytz not available or invalid — accept common formats
            valid = "/" in tz_val and len(tz_val) > 3
        if not valid:
            await event.respond(
                f"❌ Invalid timezone: `{tz_val}`\n\n"
                "Format: `Asia/Kolkata`, `America/New_York`",
                buttons=[[Button.inline("🔙 Try Again", b"timef_set_tz")]]
            )
            return
        data.setdefault("time_filter", {})["timezone"] = tz_val
        data.get("scheduler") and data["scheduler"].update({"timezone": tz_val})
        data["step"] = None
        data.pop("step_since", None)
        await save_db_async()
        await event.respond(
            f"✅ **Timezone Set!**\n\n`{tz_val}`",
            buttons=[[Button.inline("⏱ Time Filter", b"timef_menu"),
                      Button.inline("🏠 Menu", b"main_menu")]]
        )

    # ── SCHEDULER ──────────────────────────────
    elif step == "wait_sched_start":
        raw = event.text.strip()
        val = _parse_time_flexible(raw)
        if not val:
            await event.respond(
                "❌ **Time samajh nahi aaya!**\n\n"
                "Ye formats valid hain:\n"
                "`09:00 AM`  `9:00 PM`\n"
                "`21:00`  `9AM`  `9:30PM`\n\n"
                "Dobara bhejo:"
            )
            return
        data.setdefault("scheduler", {})["start"] = val
        data["step"] = None
        await save_db_async()
        await event.respond(
            f"✅ **Start Time Save Ho Gaya!**\n\n"
            f"🕐 Start: **{val}**\n"
            f"🕐 End: **{data['scheduler'].get('end', 'Not Set')}**",
            buttons=[Button.inline("🔙 Scheduler", b"sched_menu")]
        )

    elif step == "wait_sched_end":
        raw = event.text.strip()
        val = _parse_time_flexible(raw)
        if not val:
            await event.respond(
                "❌ **Time samajh nahi aaya!**\n\n"
                "Ye formats valid hain:\n"
                "`09:00 AM`  `9:00 PM`\n"
                "`21:00`  `9AM`  `9:30PM`\n\n"
                "Dobara bhejo:"
            )
            return
        data.setdefault("scheduler", {})["end"] = val
        data["step"] = None
        await save_db_async()
        await event.respond(
            f"✅ **End Time Save Ho Gaya!**\n\n"
            f"🕐 Start: **{data['scheduler'].get('start', 'Not Set')}**\n"
            f"🕐 End: **{val}**",
            buttons=[Button.inline("🔙 Scheduler", b"sched_menu")]
        )

    # ── SRC CONFIG — PREFIX / SUFFIX ───────────
    elif step and step.startswith("wait_src_prefix_"):
        src_id = step.replace("wait_src_prefix_", "")
        val    = event.raw_text.strip()
        if len(val) > 500:
            src_idx = next((i for i, s in enumerate(data.get("sources", [])) if str(s) == src_id), 0)
            await event.respond(
                f"❌ Bahut lamba ({len(val)} chars). Max 500.",
                buttons=[Button.inline("🔙 Back", f"cs_pre_menu_{src_idx}".encode())]
            )
            return True
        rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {}).setdefault("default", {})
        rules["prefix"]         = val
        rules["prefix_enabled"] = True
        data["step"] = None
        await save_db_async()
        try:
            from forward_engine import _render_msg_template
            rendered = _render_msg_template(val)
        except Exception:
            rendered = val
        src_idx = next((i for i, s in enumerate(data.get("sources", [])) if str(s) == src_id), 0)
        await event.respond(
            f"✅ **Source Prefix Set!**\n\n**Preview:**\n{rendered}",
            buttons=[Button.inline("🔙 Back", f"cs_pre_menu_{src_idx}".encode())],
            parse_mode="html"
        )

    elif step and step.startswith("wait_src_suffix_"):
        src_id = step.replace("wait_src_suffix_", "")
        val    = event.raw_text.strip()
        if len(val) > 500:
            src_idx = next((i for i, s in enumerate(data.get("sources", [])) if str(s) == src_id), 0)
            await event.respond(
                f"❌ Bahut lamba ({len(val)} chars). Max 500.",
                buttons=[Button.inline("🔙 Back", f"cs_suf_menu_{src_idx}".encode())]
            )
            return True
        rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {}).setdefault("default", {})
        rules["suffix"]         = val
        rules["suffix_enabled"] = True
        data["step"] = None
        await save_db_async()
        try:
            from forward_engine import _render_msg_template
            rendered = _render_msg_template(val)
        except Exception:
            rendered = val
        src_idx = next((i for i, s in enumerate(data.get("sources", [])) if str(s) == src_id), 0)
        await event.respond(
            f"✅ **Source Suffix Set!**\n\n**Preview:**\n{rendered}",
            buttons=[Button.inline("🔙 Back", f"cs_suf_menu_{src_idx}".encode())],
            parse_mode="html"
        )

    # ── PER-DEST PREFIX / SUFFIX ────────────────
    elif step and step.startswith("wait_dest_prefix_"):
        parts    = step.replace("wait_dest_prefix_", "").split("_")
        src_idx, dest_idx = int(parts[0]), int(parts[1])
        val      = event.raw_text.strip()
        if len(val) > 500:
            await event.respond(
                f"❌ Bahut lamba ({len(val)} chars). Max 500.",
                buttons=[Button.inline("🔙 Back", f"cst_pre_menu_{src_idx}_{dest_idx}".encode())]
            )
            return True
        src_id  = str(get_src_by_index(user_id, src_idx))
        dest_id = str(data["destinations"][dest_idx])
        rules   = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {}).setdefault(dest_id, {})
        rules["prefix"]         = val
        rules["prefix_enabled"] = True
        data["step"] = None
        await save_db_async()
        try:
            from forward_engine import _render_msg_template
            rendered = _render_msg_template(val)
        except Exception:
            rendered = val
        await event.respond(
            f"✅ **Prefix Set!** (Src#{src_idx+1}→Dest#{dest_idx+1})\n\n"
            f"**Preview:**\n{rendered}",
            buttons=[Button.inline("🔙 Back", f"cst_pre_menu_{src_idx}_{dest_idx}".encode())],
            parse_mode="html"
        )

    elif step and step.startswith("wait_dest_suffix_"):
        parts    = step.replace("wait_dest_suffix_", "").split("_")
        src_idx, dest_idx = int(parts[0]), int(parts[1])
        val      = event.raw_text.strip()
        if len(val) > 500:
            await event.respond(
                f"❌ Bahut lamba ({len(val)} chars). Max 500.",
                buttons=[Button.inline("🔙 Back", f"cst_suf_menu_{src_idx}_{dest_idx}".encode())]
            )
            return True
        src_id  = str(get_src_by_index(user_id, src_idx))
        dest_id = str(data["destinations"][dest_idx])
        rules   = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {}).setdefault(dest_id, {})
        rules["suffix"]         = val
        rules["suffix_enabled"] = True
        data["step"] = None
        await save_db_async()
        try:
            from forward_engine import _render_msg_template
            rendered = _render_msg_template(val)
        except Exception:
            rendered = val
        await event.respond(
            f"✅ **Suffix Set!** (Src#{src_idx+1}→Dest#{dest_idx+1})\n\n"
            f"**Preview:**\n{rendered}",
            buttons=[Button.inline("🔙 Back", f"cst_suf_menu_{src_idx}_{dest_idx}".encode())],
            parse_mode="html"
        )

    # ── SRC CONFIG — USERNAME REPLACEMENT ──────
    elif step and step.startswith("wait_src_usr_repl_old_"):
        src_id = step.replace("wait_src_usr_repl_old_", "")
        val = event.text.strip()
        data["temp_data"]["usr_repl_old"] = val
        data["step"] = f"wait_src_usr_repl_new_{src_id}"
        data["step_since"] = time.time()
        await save_db_async()
        await event.respond(f"Old: `{val}`\n\nAb **naya username/text** bhejo jisse replace hoga:", buttons=[Button.inline("🔙 Cancel", b"ps_menu")])

    elif step and step.startswith("wait_src_usr_repl_new_"):
        src_id = step.replace("wait_src_usr_repl_new_", "")
        new_val = event.text.strip()
        old_val = data["temp_data"].get("usr_repl_old", "")
        rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": {}})["default"]
        rules.setdefault("username_map", {})[old_val] = new_val
        data["step"] = None
        data["temp_data"].pop("usr_repl_old", None)
        await save_db_async()
        src_list = data.get("sources", [])
        idx = 0
        for i, s in enumerate(src_list):
            if str(s) == src_id:
                idx = i
                break
        await event.respond(f"✅ `{old_val}` → `{new_val}` replacement added!", buttons=[Button.inline("🔙 Back", f"cs_usr_menu_{idx}".encode())])

    # ── SRC CONFIG — HASHTAGS ──────────────────
    elif step and step.startswith("wait_src_hsh_add_"):
        src_id = step.replace("wait_src_hsh_add_", "")
        tag = event.text.strip()
        if not tag.startswith("#"):
            tag = "#" + tag
        rules = data.setdefault("custom_forward_rules", {}).setdefault(src_id, {"default": {}})["default"]
        rules.setdefault("added_hashtags", [])
        if tag not in rules["added_hashtags"]:
            rules["added_hashtags"].append(tag)
        data["step"] = None
        await save_db_async()
        src_list = data.get("sources", [])
        idx = 0
        for i, s in enumerate(src_list):
            if str(s) == src_id:
                idx = i
                break
        await event.respond(f"✅ Hashtag `{tag}` added!", buttons=[Button.inline("🔙 Back", f"cs_hsh_add_menu_{idx}".encode())])

    # ── ADMIN — BROADCAST ──────────────────────
    elif step == "bc_msg_input":
        # FIX: bc_msg_input was set in admin_menu broadcast panel but never handled
        if not is_admin(user_id):
            data["step"] = None
            return
        msg_text = event.text.strip()
        bc_target = data.pop("bc_target", "all")
        data["step"] = None
        data.pop("step_since", None)
        await save_db_async()
        await event.respond("📢 **Broadcast shuru ho raha hai...**")
        # Route to correct target list
        if bc_target == "premium":
            targets = [int(uid) for uid, u in list(db.items()) if u.get("premium", {}).get("active")]
            asyncio.create_task(_targeted_broadcast(event, msg_text, targets, user_id, "💎 Premium Users"))
        elif bc_target == "free":
            targets = [int(uid) for uid, u in list(db.items()) if not u.get("premium", {}).get("active")]
            asyncio.create_task(_targeted_broadcast(event, msg_text, targets, user_id, "🆓 Free Users"))
        elif bc_target == "active":
            targets = [int(uid) for uid, u in list(db.items()) if u.get("settings", {}).get("running")]
            asyncio.create_task(_targeted_broadcast(event, msg_text, targets, user_id, "⚡ Active Users"))
        else:
            asyncio.create_task(run_broadcast_background(event, msg_text, user_id))

    # ── ADMIN NOTIFICATION/BROADCAST STEPS ─────────────────────────────────
    if step.startswith("adm_bc") or step.startswith("adm_notify") or \
            step in ("adm_analytics_uid_input", "adm_search_name_input"):
        _handled = await _handle_admin_notification_steps(event, step, user_id, data)
        if _handled:
            return

    # ── ADMIN — REMOVE ADMIN ───────────────────
    elif step == "adm_rem_admin_input":
        if not is_admin(user_id, "Super Admin"):
            data["step"] = None
            return
        try:
            rem_id = int(event.text.strip())
            admins = GLOBAL_STATE.get("admins", {})
            if str(rem_id) in admins:
                del admins[str(rem_id)]
                await save_db_async()
                await event.respond(f"✅ Admin `{rem_id}` remove ho gaya!", buttons=[Button.inline("🔙 Admin Mgmt", b"adm_mgmt")])
            else:
                await event.respond(f"❌ `{rem_id}` admin list mein nahi hai.", buttons=[Button.inline("🔙 Back", b"adm_mgmt")])
        except ValueError:
            await event.respond("❌ Valid User ID daalo (numbers only).", buttons=[Button.inline("🔙 Back", b"adm_mgmt")])
        data["step"] = None

    elif step and step.startswith("wait_man_rem_"):
        mode = step.replace("wait_man_rem_", "").split("_")[0]
        val = event.text.strip()
        target_list = data["sources"] if mode == "src" else data["destinations"]
        u_client = user_sessions.get(user_id)
        res_val = val
        if u_client:
            try:
                res_val = await resolve_id(u_client, val)
            except Exception:
                pass

        removed = False
        if res_val in target_list:
            target_list.remove(res_val)
            removed = True
        elif val in target_list:
            target_list.remove(val)
            removed = True

        # BUG 15 FIX: Custom rules bhi remove karo jab source delete ho
        if removed and mode == "src":
            str_id = str(res_val) if res_val in target_list else str(val)
            data.get("custom_forward_rules", {}).pop(str_id, None)

        data["step"] = None
        await save_db_async()
        await event.respond(
            f"{'✅ Removed' if removed else '⚠️ Not Found'}: `{val}`",
            buttons=[Button.inline("🏠 Menu", b"main_menu")]
        )


# ==========================================
# ADMIN RESTART
# ==========================================
# ── DIRECT ADMIN COMMANDS (no step system) ───────────────
@bot.on(events.NewMessage(pattern='/fixsrc'))
async def fixsrc_cmd(event):
    if _is_queued_message(event): return
    """Source ko actual chat_id se replace karo"""
    from admin import is_admin
    if not is_admin(event.sender_id):
        return
    from database import get_user_data, save_persistent_db, save_to_mongo
    data = get_user_data(event.sender_id)
    old_sources = data.get("sources", [])
    await event.respond(
        f"📋 **Current Sources:**\n"
        + "\n".join([f"  `{s}`" for s in old_sources]) +
        f"\n\n**Ab source wale group/channel mein jao aur koi bhi ek message forward karo — "
        f"bot automatically sahi ID detect karega.**\n\n"
        f"Ya `/delsrc <id>` bhejo source delete karne ke liye aur phir bot se dobara add karo."
    )


@bot.on(events.NewMessage(pattern=r'/delsrc (.+)'))
async def delsrc_cmd(event):
    if _is_queued_message(event): return
    from admin import is_admin
    if not is_admin(event.sender_id):
        return
    from database import get_user_data, save_persistent_db, save_to_mongo
    target = event.pattern_match.group(1).strip()
    data = get_user_data(event.sender_id)
    before = list(data["sources"])
    data["sources"] = [s for s in data["sources"] if str(s) != target]
    save_persistent_db()
    await save_to_mongo()
    await event.respond(
        f"✅ Source removed!\n"
        f"Before: {before}\n"
        f"After: {data['sources']}\n\n"
        f"Ab Main Menu → Add Source se sahi source add karo."
    )
    try:
        await event.delete()
    except Exception:
        pass


@bot.on(events.NewMessage(pattern=r'/addsrc (.+)'))
async def addsrc_cmd(event):
    if _is_queued_message(event): return
    """Seedha source add karo ID se"""
    from admin import is_admin
    if not is_admin(event.sender_id):
        return
    from database import get_user_data, save_persistent_db, save_to_mongo
    src_id = event.pattern_match.group(1).strip()
    data = get_user_data(event.sender_id)
    if src_id not in [str(s) for s in data["sources"]]:
        data["sources"].append(src_id)
        save_persistent_db()
        await save_to_mongo()
        await event.respond(f"✅ Source Added: `{src_id}`\nSources: {data['sources']}")
    else:
        await event.respond(f"⚠️ Already exists: `{src_id}`")
    try:
        await event.delete()
    except Exception:
        pass


@bot.on(events.NewMessage(pattern='/srccheck'))
async def srccheck_cmd(event):
    if _is_queued_message(event): return
    from admin import is_admin
    if not is_admin(event.sender_id):
        return
    from database import get_user_data
    data = get_user_data(event.sender_id)
    sources = data.get("sources", [])
    dests   = data.get("destinations", [])
    running = data["settings"].get("running", False)
    
    src_text = "\n".join([f"  `{s}`" for s in sources]) or "  ❌ Koi source nahi"
    dst_text = "\n".join([f"  `{d}`" for d in dests]) or "  ❌ Koi destination nahi"
    
    await event.respond(
        f"🔍 **Source/Dest Debug**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🟢 Forwarding: `{'ON' if running else 'OFF'}`\n\n"
        f"📥 **Sources:**\n{src_text}\n\n"
        f"📤 **Destinations:**\n{dst_text}"
    )




@bot.on(events.CallbackQuery(data=b"restart_confirmed"))
async def restart_confirmed_cb(event):
    """FIX 3: Confirmed restart — runs graceful shutdown."""
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    await event.edit("🔄 **Restarting...**\n⏳ Disconnecting sessions...")
    await _do_graceful_restart()


async def _do_graceful_restart():
    """Shared restart logic for both /restart and confirm button."""
    from database import user_sessions, db, save_persistent_db
    # ✅ FIX: DON'T set running=False — preserve state so sessions auto-restore on restart
    # Previously this cleared running=False causing users to manually re-press Start every restart
    disconnect_count = 0
    for uid, client in list(user_sessions.items()):
        try:
            if client and client.is_connected():
                await client.disconnect()
                disconnect_count += 1
        except Exception: pass
    user_sessions.clear()
    save_persistent_db()
    logger.info(f"Graceful restart: {disconnect_count} sessions disconnected, running states preserved")
    await asyncio.sleep(2)
    import sys; sys.exit(0)


@bot.on(events.NewMessage(pattern='/restart'))
async def restart_cmd(event):
    if _is_queued_message(event): return
    """FIX 3: /restart with confirmation dialog — accidental restart prevention."""
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.respond("❌ Admin only command.")

    await event.respond(
        "⚠️ **Bot Restart — Confirm Karo**\n\n"
        "Ye action:\n"
        "• Sab forwarding sessions ~30s ke liye band hogi\n"
        "• Render automatically restart karega\n\n"
        "**Pakka karna chahte ho?**",
        buttons=[
            [Button.inline("✅ Haan, Restart Karo", b"restart_confirmed")],
            [Button.inline("❌ Cancel", b"main_menu")],
        ]
    )
    # ✅ FIX: Removed erroneous direct call to _do_graceful_restart() here
    # Previously this restarted immediately without waiting for user confirmation button


# ==========================================
# BOT STARTUP
# ==========================================


async def premium_expiry_background():
    """Har 1 ghante mein premium status check karo."""
    import time
    await asyncio.sleep(300)
    while True:
        try:
            from premium import _notify_expiry
            from notification_center import alert_user_premium_expiring
            expired_count  = 0
            warned_3d      = 0
            now = time.time()

            for uid, udata in list(db.items()):
                try:
                    uid_int = int(uid)
                    prem = udata.get("premium", {})
                    if not prem.get("active"):
                        continue
                    exp = prem.get("expires_at")
                    if not exp:
                        continue  # Lifetime — skip

                    days_left = (exp - now) / 86400

                    # FIX 8a: Expired — deactivate
                    if now > exp:
                        prem["active"]     = False
                        prem["expires_at"] = None
                        prem["expired_at"] = int(now)
                        expired_count += 1
                        asyncio.create_task(_notify_expiry(uid_int))

                    # FIX 8b: 3-day warning — sirf ek baar bhejo
                    elif 0 < days_left <= 3 and not prem.get("_warned_3d"):
                        prem["_warned_3d"] = True  # Don't warn again
                        warned_3d += 1
                        asyncio.create_task(
                            alert_user_premium_expiring(uid_int, int(days_left))
                        )

                    # FIX 8c: Reset 3d warning flag when renewed
                    elif days_left > 3 and prem.get("_warned_3d"):
                        prem["_warned_3d"] = False

                except Exception:
                    pass

            if expired_count > 0 or warned_3d > 0:
                save_persistent_db()
                logger.info(
                    f"🕐 Premium check: {expired_count} expired, "
                    f"{warned_3d} warned (3-day)"
                )
        except Exception as e:
            logger.warning(f"Premium expiry cron error: {e}")
        await asyncio.sleep(3600)  # FIX 8d: 6h → 1h (faster expiry detection)




async def step_timeout_background():
    """Har 5 min mein check karo — koi user 15+ min se ek step mein phansa hai to clear karo."""
    await asyncio.sleep(60)
    while True:
        try:
            import time
            now = time.time()
            for uid, udata in list(db.items()):
                step = udata.get("step")
                if not step:
                    continue
                step_since = udata.get("step_since", 0)
                if step_since and (now - step_since) > 900:  # 15 minutes
                    # Admin input steps clear nahi karo — wo lamba le sakte hain
                    skip_steps = {"adm_fs_channel_input", "adm_fs_test_uid_input",
                                  "adm_give_prem_input",
                                  "wait_payment_screenshot", "adm_edit_plans_input"}
                    if step not in skip_steps:
                        udata["step"] = None
                        udata.pop("step_since", None)
                        try:
                            uid_int = int(uid)
                            # Active login client cleanup
                            if step in ("wait_phone", "wait_otp", "wait_pass"):
                                client = active_clients.pop(uid_int, None)
                                if client:
                                    try: await client.disconnect()
                                    except Exception: pass
                            _lang = get_lang(uid_int)
                            _tmsg = (
                                "⏰ **Timeout!**\n\n"
                                "15 minute se koi jawab nahi diya.\n"
                                "Jo chal raha tha automatically cancel ho gaya.\n\n"
                                "Dobara shuru karne ke liye /start dabao ya neeche button dabao."
                                if _lang == "hi" else
                                "⏰ **Timeout!**\n\n"
                                "No response for 15 minutes.\n"
                                "Current step was automatically cancelled.\n\n"
                                "Press /start or the button below to continue."
                            )
                            await bot.send_message(
                                uid_int, _tmsg,
                                buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]]
                            )
                        except Exception:
                            pass
        except Exception as e:
            logger.warning(f"Step timeout cron error: {e}")
        await asyncio.sleep(300)  # Har 5 minute

async def auto_cleanup_background():
    while True:
        await asyncio.sleep(86400)
        if CLEANUP_CONFIG.get("enabled", True):
            try:
                result = cleanup_inactive_users()
                if result["count"] > 0:
                    logger.info(f"🧹 Cleanup: {result['count']} users removed.")
            except Exception as e:
                logger.error(f"Auto cleanup error: {e}")


async def self_ping_background():
    import aiohttp
    await asyncio.sleep(60)
    while True:
        try:
            url = os.environ.get("RENDER_EXTERNAL_URL", "https://your-bot.onrender.com")  # ⚠️ Set RENDER_EXTERNAL_URL env var!
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url + "/health") as resp:
                    logger.info(f"🏓 Self-ping: {resp.status} — Bot alive!")
        except Exception as e:
            logger.warning(f"⚠️ Self-ping failed: {e}")
        await asyncio.sleep(600)


async def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        # Web Panel load karo
        _panel_enabled = False
        try:
            from web_panel import register_panel_routes, auth_middleware
            _panel_enabled = True
            print("\u2705 Web Panel loaded successfully")
        except Exception as e:
            print(f"\u274c web_panel import FAILED: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

        if _panel_enabled:
            app = web.Application(middlewares=[auth_middleware])
        else:
            app = web.Application()

        async def health(request):
            return web.Response(text="Bot is running!")
        app.router.add_get("/", health)
        app.router.add_get("/health", health)
        app.router.add_get("/ping", health)

        if _panel_enabled:
            register_panel_routes(app)
            print("\u2705 Panel routes registered -> /panel")

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        if _panel_enabled:
            print("\U0001f310 Admin Panel available -> /panel")
        print(f"✅ Web server started on port {port}")
    except Exception as e:
        # Fallback: blocking HTTP server in a thread
        print(f"aiohttp failed ({e}), using threading fallback on port {port}")
        from web import start_web_server
        start_web_server()



async def run_broadcast_background(trigger_event, msg_text: str, admin_id: int):
    """Admin broadcast — sabhi users ko message bhejo with flood control."""
    targets = [int(uid) for uid in list(db.keys())]
    await _targeted_broadcast(trigger_event, msg_text, targets, admin_id, "📢 All Users")


async def _targeted_broadcast(trigger_event, msg_text: str, targets: list, admin_id: int, label: str, pin: bool = False):
    """Background task — send message to list of targets with flood control."""
    sent = failed = 0
    batch = 0
    for target_id in targets:
        try:
            sent_msg = await bot.send_message(target_id, msg_text, parse_mode="html")
            if pin:
                try:
                    await bot.pin_message(target_id, sent_msg.id)
                except Exception:
                    pass
            sent += 1
        except errors.FloodWaitError as e:
            # FIX 15: Respect FloodWait — don't hammer Telegram or bot gets banned
            wait_sec = min(e.seconds, 120)
            logger.warning(f"Broadcast FloodWait {e.seconds}s — sleeping {wait_sec}s")
            await asyncio.sleep(wait_sec)
            try:
                await bot.send_message(target_id, msg_text, parse_mode="html")
                sent += 1
            except Exception:
                failed += 1
        except (errors.UserIsBlockedError, errors.UserDeactivatedError,
                errors.UserDeactivatedBanError, errors.InputUserDeactivatedError):
            # FIX 15: Dead user — mark inactive to prevent future spam
            failed += 1
            try:
                from database import get_user_data, save_persistent_db
                if target_id in db:
                    db[target_id].setdefault("_inactive", True)
            except Exception:
                pass
        except Exception:
            failed += 1
        batch += 1
        if batch % 20 == 0:
            await asyncio.sleep(1.0)  # FIX 15: 1s pause every 20 msgs (safer rate)
        else:
            await asyncio.sleep(0.1)  # FIX 15: 10 msgs/sec (was 50 — too aggressive)
    try:
        from admin import add_log
        add_log(admin_id, f"Broadcast {label}", details=f"sent={sent} failed={failed}")
        await bot.send_message(
            admin_id,
            f"✅ **{label} Broadcast Done!**\n\n"
            f"✅ Sent: `{sent}`\n"
            f"❌ Failed: `{failed}`\n"
            f"📊 Total: `{sent+failed}`\n\n"
            f"(Main panel pe bhi update ho gaya)"
        )
    except Exception:
        pass

async def _cleanup_stale_temp_files():
    """FIX 22: /tmp mein purane bot files delete karo (Render disk leak prevention)."""
    import glob
    patterns = ["/tmp/dl_*", "/tmp/wm_vid_*", "/tmp/wm_gif_*",
                "/tmp/watermark_*", "/tmp/*.mp4", "/tmp/*.jpg", "/tmp/*.png",
                "/tmp/*.webp", "/tmp/inp_*", "/tmp/out_*"]
    deleted = 0
    for pattern in patterns:
        for f in glob.glob(pattern):
            try:
                age = time.time() - os.path.getmtime(f)
                if age > 3600:  # 1 hour se purani files delete karo
                    os.remove(f)
                    deleted += 1
            except Exception:
                pass
    if deleted > 0:
        logger.info(f"🧹 Startup cleanup: {deleted} stale temp files deleted")


async def _handle_task_exception(loop, context):
    """FIX 2: Global async exception handler — log all unhandled task crashes."""
    exc = context.get("exception")
    msg = context.get("message", "unknown")
    if exc:
        logger.error(f"Unhandled async exception: {type(exc).__name__}: {exc}", exc_info=exc)
    else:
        logger.error(f"Unhandled async error: {msg}")
    # Don't crash the bot — just log it


async def main():
    # FIX 2: Set global exception handler for asyncio tasks
    try:
        asyncio.get_event_loop().set_exception_handler(_handle_task_exception)
    except Exception:
        pass
    # FIX 22: Startup temp file cleanup
    await _cleanup_stale_temp_files()

    await run_web_server()

    from database import init_mongodb, load_from_mongodb_if_available
    await init_mongodb()
    mongo_loaded = await load_from_mongodb_if_available()
    if not mongo_loaded:
        load_persistent_db()

    # FloodWait on startup handle karo — retry karo jab tak connect ho
    _flood_retries = 0
    while True:
        try:
            await bot.start(bot_token=BOT_TOKEN)

            # ══════════════════════════════════════════════════════════════
            # ✅ QUEUED UPDATES — COMPLETE DRAIN
            # Saare registered handlers temporarily hata do.
            # Telegram ke queued updates aate hain — koi handler nahi
            # toh process nahi hote, seedha discard.
            # 3 second baad handlers wapas — sirf fresh updates process honge.
            # ══════════════════════════════════════════════════════════════
            _saved_handlers = bot._event_builders[:]  # Saare handlers save karo
            bot._event_builders.clear()               # Temporarily sab hata do
            logger.info("[STARTUP] Handlers paused — draining queued updates...")
            await asyncio.sleep(3)                    # Queued updates drain hone do
            bot._event_builders[:] = _saved_handlers  # Handlers wapas lagao
            logger.info("[STARTUP] ✅ Queue drained — bot ready for fresh messages only")
            # ── v3: Startup banner ────────────────────────────────────────────────
            try:
                from config import print_startup_banner
                print_startup_banner()
            except Exception:
                pass
            # ─────────────────────────────────────────────────────────────────────

            logger.info("Bot started successfully!")
            break
        except errors.FloodWaitError as e:
            _flood_retries += 1
            wait_sec = e.seconds
            logger.warning(f"⏳ FloodWait on startup: {wait_sec}s wait required (attempt {_flood_retries})")
            if wait_sec > 7200:  # 2 ghante se zyada → exit karo (Render restart karega)
                logger.error(f"FloodWait too long ({wait_sec}s). Exiting — Render will restart.")
                import sys; sys.exit(1)
            # Render mein log dikhao
            print(f"⏳ Telegram FloodWait: {wait_sec} seconds rukna padega ({wait_sec//60} min)...")
            import asyncio as _aio
            await _aio.sleep(min(wait_sec + 5, 3600))
        except Exception as e:
            logger.error(f"Bot start failed: {e}")
            raise

    # ── Bot Commands Setup (blue menu button) ────────────────────
    try:
        from telethon.tl.functions.bots import SetBotCommandsRequest
        from telethon.tl.types import BotCommand, BotCommandScopeDefault, BotCommandScopeUsers

        user_commands = [
            BotCommand(command="start",    description="🏠 Bot shuru karo / Main Menu"),
            BotCommand(command="menu",     description="🏠 Main Menu open karo"),
            BotCommand(command="status",   description="⚡ Forwarding status check karo"),
            BotCommand(command="stats",    description="📈 Apni forwarding stats dekho"),
            BotCommand(command="premium",  description="💎 Premium status aur features"),
            BotCommand(command="buy",      description="💳 Premium plan kharido"),
            BotCommand(command="tasks",    description="🎯 Task board — coins earn karo"),
            BotCommand(command="promote",  description="📣 Advertise / sponsor inquiry"),
            BotCommand(command="help",     description="❓ Help guide aur tips"),
            BotCommand(command="commands", description="📋 Sab commands ki list"),
            BotCommand(command="cancel",   description="❌ Current step cancel karo"),
        ]
        await bot(SetBotCommandsRequest(scope=BotCommandScopeDefault(), lang_code="", commands=user_commands))
        logger.info("✅ Bot user commands set!")
    except Exception as _cmd_err:
        logger.warning(f"Bot commands set failed: {_cmd_err}")

    # ── Admin Commands (only visible to admin in private) ──────────
    try:
        from telethon.tl.functions.bots import SetBotCommandsRequest
        from telethon.tl.types import BotCommand, BotCommandScopeUsers
        from config import OWNER_ID

        admin_commands = [
            BotCommand(command="start",    description="🏠 Main Menu"),
            BotCommand(command="menu",     description="🏠 Main Menu open karo"),
            BotCommand(command="status",   description="⚡ Forwarding status"),
            BotCommand(command="stats",    description="📈 Stats dekho"),
            BotCommand(command="premium",  description="💎 Premium status"),
            BotCommand(command="buy",      description="💳 Premium kharido"),
            BotCommand(command="tasks",    description="🎯 Task board"),
            BotCommand(command="promote",  description="📣 Sponsor inquiry"),
            BotCommand(command="help",     description="❓ Help guide"),
            BotCommand(command="commands", description="📋 Commands list"),
            BotCommand(command="cancel",   description="❌ Step cancel karo"),
            BotCommand(command="admin",    description="🛠 Admin Panel"),
            BotCommand(command="health",   description="🩺 Bot Health check"),
            BotCommand(command="restart",  description="🔄 Bot Restart"),
            BotCommand(command="setmenu",  description="📋 Bot menu commands set karo"),
            BotCommand(command="fixsrc",   description="🔧 Fix Sources"),
            BotCommand(command="srccheck", description="🔍 Source Check"),
        ]
        from telethon.tl.types import InputPeerUser
        try:
            peer = await bot.get_input_entity(OWNER_ID)
        except Exception:
            peer = None
        if peer:
            from telethon.tl.types import BotCommandScopePeer
            await bot(SetBotCommandsRequest(
                scope=BotCommandScopePeer(peer=peer),
                lang_code="",
                commands=admin_commands
            ))
        logger.info("✅ Admin commands set!")
    except Exception as _cmd_err2:
        logger.warning(f"Admin commands set failed: {_cmd_err2}")

    # Message Queue Workers start karo — burst messages handle karenge
    # Queue removed — direct asyncio.create_task use hota hai

    # ✅ FIX: Suppress Telethon Python 3.12 GeneratorExit + Task destroyed noise
    # These are known Telethon issues with asyncio teardown on container shutdown
    def _suppress_generator_exit(loop, context):
        msg = context.get("message", "")
        exc = context.get("exception")
        if (isinstance(exc, RuntimeError) and "GeneratorExit" in str(exc)) or \
           ("coroutine" in msg and "GeneratorExit" in msg) or \
           ("was never awaited" in msg and "_handle_task_exception" in msg) or \
           ("Task was destroyed but it is pending" in msg) or \
           (isinstance(exc, RuntimeError) and "coroutine ignored" in str(exc)):
            return  # Silently suppress Telethon teardown noise — harmless on shutdown
        loop.default_exception_handler(context)

    try:
        asyncio.get_event_loop().set_exception_handler(_suppress_generator_exit)
    except Exception:
        pass

    asyncio.create_task(health_monitor.send_startup_alert())
    _reg_nc(bot)  # 🔔 Notification Center handlers
    asyncio.create_task(_sched_queue_loop())  # ✅ Real scheduler queue
    # Anti-sleep (Render free tier ping)
    try:
        from anti_sleep import start_anti_sleep
        start_anti_sleep()
        logger.info('✅ Anti-sleep started')
    except Exception as _ase:
        print(f'Anti-sleep error: {_ase}')

    # Task board maintenance loop
    try:
        from task_board import maintenance_loop as _task_loop
        asyncio.create_task(_task_loop(bot))
    except Exception as _te:
        print(f'Task loop error: {_te}')
    # Promo maintenance loop
    try:
        from promo_engine import promo_maintenance_loop as _promo_loop
        asyncio.create_task(_promo_loop(bot))
    except Exception as _pe:
        print(f'Promo loop error: {_pe}')
    # Ads blast loop
    try:
        from ads_engine import blast_loop as _blast_loop
        from config import bot as _bot_ref
        asyncio.create_task(_blast_loop(_bot_ref))
    except Exception as _be:
        print(f'Blast loop error: {_be}')
    # Live admin clock auto-refresh
    try:
        from ui.admin_menu import _live_clock_loop
        asyncio.create_task(_live_clock_loop())
    except Exception as _clk_e:
        print(f'Clock loop error: {_clk_e}')

    # ============================================================
    # DUAL ROLE ARCHITECTURE:
    # main.py → UI sessions (pinned chats, display names, resolve_id)
    # worker.py → Forwarding sessions (actual message forwarding)
    #
    # Dono alag alag TelegramClient instances chalate hain — ok hai.
    # Worker assignment bhi ensure karo taki worker.py kaam kare.
    # ============================================================
    logger.info("🔄 Restoring sessions on startup...")
    from worker_manager import assign_worker
    count = 0
    running_count = 0
    for uid, udata in list(db.items()):
        session_str = udata.get("session")
        if session_str:
            # Worker assignment ensure karo
            if udata.get("assigned_worker") is None:
                assign_worker(int(uid))
            is_running = udata.get("settings", {}).get("running", False)
            if is_running:
                # Only start forwarder task for running users
                await asyncio.sleep(0.5)
                asyncio.create_task(start_user_forwarder(int(uid), session_str))
                running_count += 1
                logger.info(f"▶️ Startup: session task started for user {uid}")
            count += 1
    logger.info(f"✅ {count} users with sessions found, {running_count} forwarding sessions started.")

    _create_tracked_task(auto_cleanup_background(), "auto_cleanup")
    _create_tracked_task(step_timeout_background(), "step_timeout")
    _create_tracked_task(idle_session_manager(), "idle_session_mgr")  # FIX 21
    _create_tracked_task(premium_expiry_background(), "premium_expiry")
    _create_tracked_task(self_ping_background(), "self_ping")
    _create_tracked_task(health_monitor.health_monitor_loop(), "health_monitor")

    # ── New Feature Background Tasks ─────────────────────
    _create_tracked_task(notifications.start_daily_summary_task(), "daily_summary")

    # Anti-Spam background cleanup
    try:
        from anti_spam import auto_cleanup_loop
        _create_tracked_task(auto_cleanup_loop(), "anti_spam_cleanup")
    except Exception:
        pass

    await asyncio.gather(
        bot.run_until_disconnected(),
        asyncio.sleep(float('inf')),
        return_exceptions=True,
    )


# ══════════════════════════════════════════════════════════
# NEW FEATURE STEP HANDLERS
# Admin steps: API key, affiliate, watermark, reseller
# User steps: watermark, affiliate, per-day scheduler
# ══════════════════════════════════════════════════════════

def _parse_time_flexible(raw: str) -> str | None:
    """
    Flexible time parser — multiple formats accept karta hai.
    Returns: "09:00 AM" format, ya None if invalid.
    """
    import re
    s = raw.strip().upper().replace(".", ":").replace("-", ":")
    s = re.sub(r"\s+", " ", s)

    # 24-hour: 21:00 or 09:00
    m = re.match(r"^(\d{1,2}):(\d{2})$", s)
    if m:
        h, mn = int(m.group(1)), int(m.group(2))
        if 0 <= h <= 23 and 0 <= mn <= 59:
            suffix = "AM" if h < 12 else "PM"
            h12 = h % 12 or 12
            return f"{h12:02d}:{mn:02d} {suffix}"

    # 12-hour with AM/PM: 9:00 AM, 09:30PM, 9AM
    m = re.match(r"^(\d{1,2})(?::(\d{2}))?\s*(AM|PM)$", s)
    if m:
        h  = int(m.group(1))
        mn = int(m.group(2)) if m.group(2) else 0
        ap = m.group(3)
        if 1 <= h <= 12 and 0 <= mn <= 59:
            return f"{h:02d}:{mn:02d} {ap}"

    return None




async def idle_session_manager():
    """
    FIX 21: Idle Session Manager — Render 512MB RAM ke liye critical.
    Ek TelegramClient ~10-15MB RAM leta hai.
    Agar 50 users login hain = 500MB = OOM crash.
    Sessions jo 30 min se idle hain unhe disconnect karo.
    Jab naya message aaye tab auto-reconnect hoga (forward_engine handles it).
    """
    await asyncio.sleep(120)  # Bot startup ke 2min baad start karo
    while True:
        try:
            now = time.time()
            IDLE_THRESHOLD = 1800  # 30 minutes
            disconnected = 0

            from database import user_sessions, get_user_data
            for uid in list(user_sessions.keys()):
                try:
                    udata = get_user_data(uid)
                    last_active = udata.get("last_active", 0)
                    running = udata.get("settings", {}).get("running", False)

                    # NEVER disconnect if user is actively running forwarding
                    # Running sessions must stay alive regardless of last_active
                    if running:
                        continue

                    # Only disconnect stopped/idle sessions to free RAM
                    if not running and (now - last_active) > IDLE_THRESHOLD:
                        client = user_sessions.pop(uid, None)
                        if client:
                            try:
                                if client.is_connected():
                                    await client.disconnect()
                            except Exception:
                                pass
                            disconnected += 1

                except Exception:
                    pass

            if disconnected > 0:
                logger.info(f"💾 Idle Session Manager: {disconnected} idle sessions disconnected (RAM freed)")

        except Exception as e:
            logger.warning(f"Idle session manager error: {e}")

        await asyncio.sleep(600)  # Check every 10 minutes



async def _handle_new_feature_steps(event, step, user_id, data):
    """
    Returns True agar step handle ho gaya, False agar nahi.
    main.py ke step handler se call karo.
    """
    # ── PER-DEST STEPS ───────────────────────────────────
    if step and step.startswith("cst_cap_set_"):
        parts    = step.split("_")
        src_idx  = int(parts[3])
        dest_idx = int(parts[4])
        src_id   = str(get_src_by_index(user_id, src_idx))
        dest_id  = str(data["destinations"][dest_idx])
        data.setdefault("custom_forward_rules", {}).setdefault(src_id, {}).setdefault(dest_id, {})["custom_caption"] = event.raw_text.strip()
        data["step"] = None
        save_persistent_db()
        await save_to_mongo()
        await event.respond(f"✅ Custom caption set!")
        return True

    if step and step.startswith("cst_repl_set_"):
        parts    = step.split("_")
        src_idx  = int(parts[3])
        dest_idx = int(parts[4])
        txt      = event.raw_text.strip()
        if "→" in txt:
            old_t, new_t = [x.strip() for x in txt.split("→", 1)]
            src_id = str(get_src_by_index(user_id, src_idx))
            dest_id = str(data["destinations"][dest_idx])
            data.setdefault("custom_forward_rules", {}).setdefault(src_id, {}).setdefault(dest_id, {}).setdefault("replace_map", {})[old_t] = new_t
            data["step"] = None
            save_persistent_db()
            await save_to_mongo()
            await event.respond(f"✅ Replacement added!\n`{old_t}` → `{new_t}`")
        else:
            await event.respond("❌ Format: `purana text → naya text`\nDobara try karo:")
        return True

    # ── ADMIN STEPS ──────────────────────────────────────
    # AI Rewrite steps removed — feature band kar di gayi

    if step == "admin_set_amazon_tag":
        tag = event.text.strip()
        if not tag:
            await event.respond("❌ Empty tag. Phir se bhejo:")
            return True
        from feature_flags import set_flag
        from database import save_to_mongo
        set_flag("owner_amazon_tag", tag)
        data["step"] = None
        await save_db_async()
        await save_to_mongo()
        await event.respond(
            f"✅ Amazon Affiliate Tag save ho gaya!\n`{tag}`",
            buttons=[Button.inline("🔙 Affiliate", b"flags_affiliate_menu")]
        )
        return True

    elif step == "admin_set_flipkart_id":
        fid = event.text.strip()
        if not fid:
            await event.respond("❌ Empty ID. Phir se bhejo:")
            return True
        from feature_flags import set_flag
        from database import save_to_mongo
        set_flag("owner_flipkart_id", fid)
        data["step"] = None
        await save_db_async()
        await save_to_mongo()
        await event.respond(
            f"✅ Flipkart Affiliate ID save ho gaya!\n`{fid}`",
            buttons=[Button.inline("🔙 Affiliate", b"flags_affiliate_menu")]
        )
        return True

    elif step == "admin_set_force_wm_text":
        txt = event.text.strip()
        if not txt:
            await event.respond("❌ Empty text. Phir se bhejo:")
            return True
        from feature_flags import set_flag
        from database import save_to_mongo
        set_flag("force_watermark_text", txt)
        data["step"] = None
        await save_db_async()
        await save_to_mongo()
        await event.respond(
            f"✅ Force Watermark text save ho gaya!\n`{txt}`",
            buttons=[Button.inline("🔙 Watermark", b"flags_watermark_menu")]
        )
        return True

    elif step == "admin_set_alert_channel":
        val = event.text.strip()
        from feature_flags import set_flag
        from database import save_to_mongo
        if val == "0":
            set_flag("alert_channel_id", None)
            data["step"] = None
            await save_db_async()
            await save_to_mongo()
            await event.respond(
                "✅ Alert channel hata diya.\nAb alerts owner ke DM mein jaayenge.",
                buttons=[Button.inline("⚙️ Feature Flags", b"adm_feature_flags")]
            )
            return True
        # Try resolve — ID, username, link sab accept karo
        u_client = user_sessions.get(user_id)
        channel_id = None
        if val.lstrip('-').isdigit():
            channel_id = int(val)
        elif u_client:
            try:
                resolved = await resolve_id(u_client, val)
                channel_id = int(resolved)
            except Exception as e:
                await event.respond(f"❌ Channel resolve nahi hua: {str(e)[:80]}\n\nID bhejo (e.g. -1001234567890):")
                return True
        else:
            await event.respond("❌ Sirf channel ID daalo (e.g. -1001234567890):\n0 bhejo alerts owner DM mein lene ke liye.")
            return True
        set_flag("alert_channel_id", channel_id)
        data["step"] = None
        await save_db_async()
        await save_to_mongo()
        await event.respond(
            f"✅ **Alert Channel Save Ho Gaya!**\n\nID: `{channel_id}`",
            buttons=[Button.inline("⚙️ Feature Flags", b"adm_feature_flags")]
        )
        return True

    # ── ADMIN — RESELLER STEPS ───────────────────────────
    # ── RESELLER MANAGEMENT STEPS ─────────────────────────────────────────────
    if step.startswith("adm_add_reseller") or step.startswith("adm_remove") or \
            step.startswith("res_give_prem"):
        _handled = await _handle_reseller_steps(event, step, user_id, data)
        if _handled:
            return

    # ── USER STEPS — WATERMARK ───────────────────────────
    elif step == "wait_watermark_text":
        txt = event.text.strip()
        if not txt:
            await event.respond("❌ Text empty hai. Phir se bhejo:")
            return True
        # FIX 8: Validate watermark text length
        if len(txt) > 100:
            await event.respond(
                f"❌ Text bahut lamba hai ({len(txt)} chars)।\n"
                "Maximum 100 characters allowed.\n"
                "Chhota text bhejo:"
            )
            return True
        data.setdefault("watermark", {})["text"] = txt
        data["watermark"]["enabled"] = True
        data["step"] = None
        await save_db_async()
        await event.respond(
            f"✅ Watermark text set: `{txt}`\n"
            "Ab photos pe ye text lagega!",
            buttons=[Button.inline("🖼️ Watermark Settings", b"settings_watermark")]
        )
        return True

    elif step == "wait_watermark_logo":
        # User ne image bheja — logo save karo
        if not event.photo and not event.file:
            await event.respond(
                "❌ Koi image nahi mili!\n\n"
                "Please ek image/photo bhejo (PNG ya JPG).",
                buttons=[Button.inline("❌ Cancel", b"settings_watermark")]
            )
            return True
        try:
            # BUG FIX 1: Upload progress indicator — user ko pata chale kaam ho raha hai
            progress_msg = await event.respond("⏳ **Logo upload ho raha hai...**\nPlease wait...")

            # Image download karo
            img_bytes = await event.download_media(file=bytes)
            if not img_bytes:
                await progress_msg.edit("❌ Image download nahi hua. Dobara try karo.")
                return True

            # Size check (5MB max)
            if len(img_bytes) > 5 * 1024 * 1024:
                await progress_msg.edit(
                    "❌ **Image bahut badi hai!**\n\nMax 5MB allowed hai.\nChhoti image bhejo."
                )
                return True

            # Extension decide karo
            ext = "png"
            if event.file:
                fname_attr = getattr(event.file, "name", "") or ""
                if fname_attr.lower().endswith(".jpg") or fname_attr.lower().endswith(".jpeg"):
                    ext = "jpg"
                elif fname_attr.lower().endswith(".webp"):
                    ext = "webp"

            from watermark import save_logo
            filename = save_logo(user_id, img_bytes, ext=ext)

            # BUG FIX 2: Logo ko base64 mein DB mein bhi save karo
            # Render free tier mein filesystem ephemeral hai — restart par logo delete ho jaata tha
            import base64
            data.setdefault("watermark", {})["logo_file"]   = filename
            data["watermark"]["logo_b64"]                   = base64.b64encode(img_bytes).decode()
            data["watermark"]["logo_ext"]                   = ext
            data["watermark"]["enabled"] = True

            # Auto mode set karo
            current_mode = data["watermark"].get("mode", "text")
            if current_mode == "text" and not data["watermark"].get("text"):
                data["watermark"]["mode"] = "image"
            elif current_mode == "text":
                data["watermark"]["mode"] = "both"
            data["step"] = None
            await save_db_async()

            # Progress message ko success mein update karo
            await progress_msg.edit(
                "✅ **Logo Upload Ho Gaya!**\n\n"
                f"📁 File: `{filename}`\n"
                f"📏 Size: `{len(img_bytes)//1024}KB`\n"
                "🎨 Watermark mode automatically set ho gaya.\n\n"
                "💡 **Preview** button se dekho kaisa lagega!",
                buttons=[Button.inline("🖼️ Watermark Settings", b"settings_watermark")]
            )
        except Exception as e:
            data["step"] = None
            await event.respond(
                f"❌ Logo save nahi hua: {str(e)[:80]}\n\nDobara try karo.",
                buttons=[Button.inline("🖼️ Watermark Settings", b"settings_watermark")]
            )
        return True

    # ── USER STEPS — AI PROMPT (feature removed, graceful clear) ────────────
    elif step in ("wait_personal_ai_prompt", "wait_personal_gemini_key"):
        data["step"] = None
        await save_db_async()
        await event.respond(
            "ℹ️ **AI Rewrite feature band kar di gayi hai.**\n\n"
            "Ye feature ab available nahi hai.",
            buttons=[Button.inline("🏠 Main Menu", b"main_menu")]
        )
        return True

    # ── USER STEPS — AFFILIATE ───────────────────────────
    elif step and step.startswith("wait_aff_"):
        # Generic affiliate platform tag handler
        platform = step.replace("wait_aff_", "")

        # Special: test URL
        if platform == "test_url":
            url = event.text.strip()
            data["step"] = None
            from affiliate import test_affiliate_url, registry
            results = test_affiliate_url(url, user_id)
            if not results:
                await event.respond(
                    "❌ Koi affiliate platform match nahi hua।\n\n"
                    "Ye URL Amazon, Flipkart, Meesho, Myntra, Ajio, Nykaa, ya Snapdeal ka hona chahiye।",
                    buttons=[[Button.inline("🔙 Back", b"settings_affiliate")]]
                )
            else:
                lines = ["🧪 **URL Test Result:**\n"]
                for pname, res in results.items():
                    if "error" in res:
                        lines.append(f"❌ {pname}: {res['error']}")
                    elif res.get("changed"):
                        lines.append(f"✅ **{pname.title()}:**")
                        lines.append(f"  Before: `{res['original'][:60]}`")
                        lines.append(f"  After:  `{res['modified'][:60]}`")
                    else:
                        lines.append(f"⚠️ {pname}: Tag set nahi hai")
                await event.respond(
                    "\n".join(lines),
                    buttons=[[Button.inline("🔙 Affiliate", b"settings_affiliate")]]
                )
            await save_db_async()
            return True

        # Normal platform tag input
        from affiliate import registry
        plugin = registry.get_plugin(platform)
        if not plugin:
            # Try old-style platform names
            _old_map = {"amazon": "amazon", "flipkart": "flipkart"}
            platform = _old_map.get(platform, platform)
            plugin   = registry.get_plugin(platform)

        tag = event.text.strip()
        data["step"] = None

        if tag.lower() == "remove":
            if plugin:
                data.setdefault("affiliate", {})[plugin.tag_key] = ""
            await event.respond(
                f"✅ {platform.title()} tag hata diya!",
                buttons=[[Button.inline("🔗 Affiliate", b"settings_affiliate")]]
            )
        elif plugin:
            data.setdefault("affiliate", {})[plugin.tag_key] = tag
            data["affiliate"]["enabled"] = True
            await event.respond(
                f"✅ **{plugin.icon} {platform.title()} Tag Set!**\n\n"
                f"Tag: `{tag}`\n"
                f"Ab forwarded {platform.title()} links mein tumhara tag lagega! 💰",
                buttons=[[Button.inline("🔗 Affiliate", b"settings_affiliate")]]
            )
        else:
            await event.respond("❌ Platform nahi mila!")

        await save_db_async()
        return True

    # Backward compat
    elif step == "wait_amazon_tag":
        tag = event.text.strip()
        data.setdefault("affiliate", {})["amazon_tag"] = tag
        data["affiliate"]["enabled"] = True
        data["step"] = None
        await save_db_async()
        await event.respond(
            f"✅ Amazon Tag set: `{tag}`",
            buttons=[Button.inline("🔗 Affiliate", b"settings_affiliate")]
        )
        return True

    elif step == "wait_flipkart_id":
        fid = event.text.strip()
        data.setdefault("affiliate", {})["flipkart_id"] = fid
        data["affiliate"]["enabled"] = True
        data["step"] = None
        await save_db_async()
        await event.respond(
            f"✅ Flipkart ID set: `{fid}`",
            buttons=[Button.inline("🔗 Affiliate", b"settings_affiliate")]
        )
        return True

    # ── USER STEPS — AI PERSONAL KEY removed (feature band) ───

    # ── PER-DAY SCHEDULER STEPS ──────────────────────────
    elif step and step.startswith("wait_sched_day_start_"):
        day_key = step.replace("wait_sched_day_start_", "")
        raw_val = event.text.strip()

        # BUG 18 FIX: _parse_time_flexible use karo - 24hr + 12hr dono support
        time_val = _parse_time_flexible(raw_val)
        if not time_val:
            await event.respond(
                f"❌ **Format galat hai!**\n\n"
                f"Ye formats valid hain:\n"
                f"`09:00 AM`  `9:00 PM`\n"
                f"`21:00`  `9AM`  `9:30PM`\n\n"
                f"Dobara bhejo:"
            )
            return True

        data.setdefault("scheduler", {}).setdefault("per_day", {})
        data["scheduler"]["per_day"].setdefault(day_key, {
            "enabled": True, "start": "09:00 AM", "end": "10:00 PM"
        })["start"] = time_val
        data["step"] = None
        await save_db_async()

        cfg = data["scheduler"]["per_day"].get(day_key, {})
        # Problem 8 Fix: Same day edit screen dikhao — not per_day main menu
        await event.respond(
            f"✅ **{day_key} — Start Time Set!**\n\n"
            f"📅 Start: `{time_val}`\n"
            f"📅 End:   `{cfg.get('end', '10:00 PM')}`\n\n"
            f"Kya karna hai?",
            buttons=[
                [Button.inline(f"⏰ {day_key} End Time Badlo",   f"schedet_{day_key}".encode())],
                [Button.inline("📅 Dusra Din Edit Karo",          b"sched_edit_days")],
                [Button.inline("🗓 Per-Day Main Menu",            b"sched_per_day_menu")],
            ]
        )
        return True

    elif step and step.startswith("wait_sched_day_end_"):
        day_key = step.replace("wait_sched_day_end_", "")
        raw_val = event.text.strip()

        # BUG 18 FIX: _parse_time_flexible use karo - 24hr + 12hr dono support
        time_val = _parse_time_flexible(raw_val)
        if not time_val:
            await event.respond(
                f"❌ **Format galat hai!**\n\n"
                f"Ye formats valid hain:\n"
                f"`09:00 AM`  `9:00 PM`\n"
                f"`21:00`  `9AM`  `9:30PM`\n\n"
                f"Dobara bhejo:"
            )
            return True

        data.setdefault("scheduler", {}).setdefault("per_day", {})
        data["scheduler"]["per_day"].setdefault(day_key, {
            "enabled": True, "start": "09:00 AM", "end": "10:00 PM"
        })["end"] = time_val
        data["step"] = None
        await save_db_async()

        cfg = data["scheduler"]["per_day"].get(day_key, {})
        # Problem 8 Fix: Same day edit screen dikhao
        await event.respond(
            f"✅ **{day_key} — End Time Set!**\n\n"
            f"📅 Start: `{cfg.get('start', '09:00 AM')}`\n"
            f"📅 End:   `{time_val}`\n\n"
            f"Kya karna hai?",
            buttons=[
                [Button.inline(f"⏰ {day_key} Start Time Badlo", f"schedst_{day_key}".encode())],
                [Button.inline("📅 Dusra Din Edit Karo",          b"sched_edit_days")],
                [Button.inline("🗓 Per-Day Main Menu",            b"sched_per_day_menu")],
            ]
        )
        return True

    elif step == "wait_holiday_add":
        dates = [d.strip() for d in event.text.strip().split(",")]
        valid, invalid = [], []
        import re
        for d in dates:
            if re.match(r"\d{4}-\d{2}-\d{2}", d):
                valid.append(d)
            else:
                invalid.append(d)
        data.setdefault("scheduler", {}).setdefault("holidays", [])
        for d in valid:
            if d not in data["scheduler"]["holidays"]:
                data["scheduler"]["holidays"].append(d)
        data["step"] = None
        await save_db_async()
        msg = f"✅ {len(valid)} holidays add ki gai."
        if invalid:
            msg += f"\n⚠️ Invalid format (YYYY-MM-DD chahiye): {', '.join(invalid)}"
        await event.respond(msg, buttons=[Button.inline("📅 Scheduler", b"sched_menu")])
        return True

    return False  # Step handle nahi hua

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped by user.")


async def _handle_admin_notification_steps(event, step, user_id, data) -> bool:
    """Handle broadcast, analytics, search and notify admin step inputs.
    Returns True if step was handled.
    """
    if step == "adm_bc_input":
        if not is_admin(user_id):
            data["step"] = None
            return
        msg_text = event.text.strip()
        data["step"] = None
        data.pop("step_since", None)  # BUG 17 FIX: step_since clear karo
        await save_db_async()
        await event.respond("📢 **Broadcast shuru ho raha hai...**")
        asyncio.create_task(run_broadcast_background(event, msg_text, user_id))

    elif step == "adm_analytics_uid_input":
        if not is_admin(user_id):
            data["step"] = None
            return
        raw = event.text.strip()
        try:
            target_uid = int(raw.lstrip('-'))
        except ValueError:
            await event.respond("❌ Valid User ID bhejo (sirf numbers)")
            return
        data["step"] = None
        # Analytics module se call karo
        from analytics import _show_user_analytics
        await _show_user_analytics(event, target_uid)

    # ── Notification Center text inputs ──────────────
    if step and step.startswith(("nc_dest", "nc_owner")):
        try:
            from notification_center import handle_nc_input as _nc_input
            if await _nc_input(event, user_id, step):
                return
        except Exception as e:
            print(f"nc_input error: {e}")
        return  # nc step tha — baaki elif chain evaluate mat karo

    elif step == "adm_bc_premium_input":
        if not is_admin(user_id): data["step"] = None; return
        msg_text = event.text.strip()
        data["step"] = None
        await save_db_async()
        targets = [int(uid) for uid, u in list(db.items()) if u.get("premium",{}).get("active")]
        await event.respond(f"💎 Premium broadcast — `{len(targets)}` users...")
        asyncio.create_task(_targeted_broadcast(event, msg_text, targets, user_id, "💎 Premium"))

    elif step == "adm_bc_free_input":
        if not is_admin(user_id): data["step"] = None; return
        msg_text = event.text.strip()
        data["step"] = None
        await save_db_async()
        targets = [int(uid) for uid, u in list(db.items()) if not u.get("premium",{}).get("active")]
        await event.respond(f"🆓 Free broadcast — `{len(targets)}` users...")
        asyncio.create_task(_targeted_broadcast(event, msg_text, targets, user_id, "🆓 Free"))

    elif step == "adm_bc_active_input":
        if not is_admin(user_id): data["step"] = None; return
        msg_text = event.text.strip()
        data["step"] = None
        await save_db_async()
        targets = [int(uid) for uid, u in list(db.items()) if u["settings"]["running"]]
        await event.respond(f"🟢 Active broadcast — `{len(targets)}` users...")
        asyncio.create_task(_targeted_broadcast(event, msg_text, targets, user_id, "🟢 Active"))

    elif step == "adm_bc_pin_input":
        if not is_admin(user_id, "Super Admin"): data["step"] = None; return
        msg_text = event.text.strip()
        data["step"] = None
        await save_db_async()
        targets = list(db.keys())
        await event.respond(f"📌 Pinned broadcast — `{len(targets)}` users...")
        asyncio.create_task(_targeted_broadcast(event, msg_text, [int(u) for u in targets], user_id, "📌 Pinned", pin=True))

    elif step == "adm_bc_one_uid":
        if not is_admin(user_id): data["step"] = None; return
        try:
            target_uid = int(event.text.strip())
            data["step"] = f"adm_bc_one_msg_{target_uid}"
            data["step_since"] = time.time()
            await event.respond(f"👤 User `{target_uid}` — Ab message bhejo:")
        except ValueError:
            await event.respond("❌ Valid User ID daalo.")

    elif step and step.startswith("adm_bc_one_msg_"):
        if not is_admin(user_id): data["step"] = None; return
        try:
            target_uid = int(step.replace("adm_bc_one_msg_",""))
        except Exception:
            data["step"] = None; return
        msg_text = event.text.strip()
        data["step"] = None
        await save_db_async()
        try:
            await bot.send_message(target_uid, msg_text, parse_mode="html")
            await event.respond(f"✅ User `{target_uid}` ko message bhej diya!")
            from admin import add_log
            add_log(user_id, "Direct Message", target=target_uid)
        except Exception as e:
            await event.respond(f"❌ Error: {str(e)[:80]}")

    elif step and step.startswith("adm_add_note_input_"):
        from admin import is_admin, add_user_note
        if not is_admin(user_id): data["step"] = None; return
        try:
            target_uid = int(step.replace("adm_add_note_input_", ""))
        except Exception:
            data["step"] = None; return
        note_text = event.text.strip()[:200]
        data["step"] = None
        await save_db_async()
        add_user_note(user_id, target_uid, note_text)
        await event.respond(
            f"✅ **Note add ho gayi!**",
            buttons=[Button.inline("📝 Notes Dekho", f"adm_user_notes_{target_uid}".encode())]
        )
        return True

    elif step and step.startswith("adm_msg_user_input_"):
        if not is_admin(user_id): data["step"] = None; return
        try:
            target_uid = int(step.replace("adm_msg_user_input_",""))
        except Exception:
            data["step"] = None; return
        msg_text = event.text.strip()
        data["step"] = None
        await save_db_async()
        try:
            await bot.send_message(target_uid, msg_text, parse_mode="html")
            await event.respond(f"✅ Bhej diya user `{target_uid}` ko!",
                                buttons=[Button.inline("🔙 Back", f"adm_view_u_{target_uid}".encode())])
            from admin import add_log
            add_log(user_id, "Direct Message", target=target_uid)
        except Exception as e:
            await event.respond(f"❌ Error: {str(e)[:80]}")

    # ── Admin Notes ─────────────────────────────────────────────
    elif step and step.startswith("adm_note_input_"):
        if not is_admin(user_id): data["step"] = None; return
        target_uid = int(step.replace("adm_note_input_", ""))
        note_text  = event.text.strip()[:500]  # Max 500 chars
        data["step"] = None
        if target_uid in db:
            db[target_uid].setdefault("_admin_notes", []).append({
                "time": datetime.datetime.now().strftime("%d/%m %H:%M"),
                "by":   str(user_id),
                "text": note_text,
            })
            # Keep only last 20 notes
            if len(db[target_uid]["_admin_notes"]) > 20:
                db[target_uid]["_admin_notes"] = db[target_uid]["_admin_notes"][-20:]
            await save_db_async()
            await event.respond(
                f"✅ Note add ho gaya User `{target_uid}` par!",
                buttons=[[Button.inline("📝 Notes Dekho", f"adm_notes_{target_uid}".encode()),
                          Button.inline("👤 Profile",    f"adm_view_u_{target_uid}".encode())]]
            )
        else:
            await event.respond("❌ User not found.")
        return

    # ── Admin Notify All Admins ──────────────────────────────────
    elif step == "adm_notify_admins_input":
        if not is_admin(user_id, "Super Admin"): data["step"] = None; return
        msg_text = event.text.strip()
        data["step"] = None
        await save_db_async()
        admins = list(GLOBAL_STATE.get("admins", {}).keys())
        sent, failed = 0, 0
        for aid in admins:
            try:
                aid_int = int(aid)
                if aid_int == user_id: continue
                await bot.send_message(
                    aid_int,
                    f"🔔 **Admin Notice from `{user_id}`:**\n\n{msg_text}",
                    parse_mode='html'
                )
                sent += 1
            except Exception:
                failed += 1
        await event.respond(
            f"✅ Message sent to {sent} admin(s). Failed: {failed}",
            buttons=[Button.inline("🔙 Admin Panel", b"adm_main")]
        )
        return

    elif step == "adm_search_name_input":
        if not is_admin(user_id): data["step"] = None; return
        query = event.text.strip().lower().lstrip("@")
        data["step"] = None
        results = []
        for uid, u in list(db.items()):
            p     = u.get("profile", {})
            first = p.get("first_name","").lower()
            last  = p.get("last_name","").lower()
            uname = p.get("username","").lower()
            if query in first or query in last or query in uname or query in str(uid):
                results.append(int(uid))
        if not results:
            await event.respond(f"❌ `{query}` se koi user nahi mila.",
                                buttons=[Button.inline("🔙 Back", b"adm_user_mg")])
        elif len(results) == 1:
            from database import get_user_data as _gud
            _gud(user_id)["step"] = None
            await event.respond(
                f"✅ 1 user mila:",
                buttons=[[Button.inline(f"👤 {results[0]}", f"adm_view_u_{results[0]}".encode())],
                         [Button.inline("🔙 Back", b"adm_user_mg")]]
            )
        else:
            btns = [[Button.inline(f"👤 {uid} — {(db.get(uid,{}).get('profile',{}).get('first_name','') or str(uid))[:20]}",
                                  f"adm_view_u_{uid}".encode())] for uid in results[:10]]
            btns.append([Button.inline("🔙 Back", b"adm_user_mg")])
            await event.respond(f"🔍 **{len(results)} results** for `{query}`:", buttons=btns)

    return False


async def _handle_reseller_steps(event, step, user_id, data) -> bool:
    """Handle reseller management step inputs.
    Returns True if step was handled.
    """
    if step == "adm_add_reseller_step1":
        try:
            reseller_id = int(event.text.strip())
            data["temp_data"]["new_reseller_id"] = reseller_id
            data["step"] = "adm_add_reseller_step2"
            data["step_since"] = time.time()
            await save_db_async()
            await event.respond(
                f"✅ User ID: `{reseller_id}`\n\n"
                "Step 2/3: Quota kitna dena hai? (kitne users ko premium de sakta hai)\n\n"
                "Number bhejo (e.g. `20`):",
                buttons=[Button.inline("❌ Cancel", b"adm_resellers")]
            )
        except ValueError:
            await event.respond("❌ Valid User ID daalo:")
        return True

    elif step == "adm_add_reseller_step2":
        try:
            quota = int(event.text.strip())
            data["temp_data"]["new_reseller_quota"] = quota
            data["step"] = "adm_add_reseller_step3"
            data["step_since"] = time.time()
            await save_db_async()
            await event.respond(
                f"✅ Quota: `{quota}` users\n\n"
                "Step 3/3: Commission % kitna? (0-100)\n\n"
                "Number bhejo (e.g. `20` for 20%):",
                buttons=[Button.inline("❌ Cancel", b"adm_resellers")]
            )
        except ValueError:
            await event.respond("❌ Valid number daalo:")
        return True

    elif step == "adm_add_reseller_step3":
        try:
            commission = float(event.text.strip())
            reseller_id = data["temp_data"].get("new_reseller_id")
            quota       = data["temp_data"].get("new_reseller_quota", 10)
            add_reseller(reseller_id, quota, commission, event.sender_id)
            data["step"] = None
            data["temp_data"] = {}
            await save_db_async()
            await event.respond(
                f"✅ **Reseller Added!**\n\n"
                f"User: `{reseller_id}`\n"
                f"Quota: `{quota}` users\n"
                f"Commission: `{commission}%`\n\n"
                f"User ko `/start` karne ke baad Reseller Panel milega.",
                buttons=[Button.inline("👥 Resellers", b"adm_resellers")]
            )
        except ValueError:
            await event.respond("❌ Valid number daalo (0-100):")
        return True

    elif step == "adm_remove_reseller":
        try:
            rid = int(event.text.strip())
            removed = remove_reseller(rid)
            data["step"] = None
            await save_db_async()
            msg = f"✅ Reseller `{rid}` removed!" if removed else f"❌ User `{rid}` reseller nahi hai."
            await event.respond(msg, buttons=[Button.inline("👥 Resellers", b"adm_resellers")])
        except ValueError:
            await event.respond("❌ Valid User ID daalo:")
        return True

    # ── RESELLER STEPS ───────────────────────────────────
    elif step == "res_give_prem_user_id":
        try:
            target_uid = int(event.text.strip())
            data["temp_data"]["res_target_uid"] = target_uid
            data["step"] = "res_give_prem_days"
            data["step_since"] = time.time()
            await save_db_async()
            await event.respond(
                f"✅ User ID: `{target_uid}`\n\n"
                "Step 2/2: Kitne din ka premium dena hai?\n"
                "(0 = Lifetime, 30 = 1 month, 365 = 1 year)\n\n"
                "Days bhejo:",
                buttons=[Button.inline("❌ Cancel", b"reseller_panel")]
            )
        except ValueError:
            await event.respond("❌ Valid User ID daalo:")
        return True

    elif step == "res_give_prem_days":
        try:
            days = int(event.text.strip())
            target_uid = data["temp_data"].get("res_target_uid")
            from reseller import reseller_give_premium
            success, msg = reseller_give_premium(event.sender_id, target_uid, days)
            data["step"] = None
            data["temp_data"] = {}
            await save_db_async()
            await event.respond(msg, buttons=[Button.inline("👥 My Panel", b"reseller_panel")])
        except ValueError:
            await event.respond("❌ Valid days daalo:")
        return True

    elif step == "res_remove_prem_user_id":
        try:
            target_uid = int(event.text.strip())
            from reseller import reseller_remove_premium
            success, msg = reseller_remove_premium(event.sender_id, target_uid)
            data["step"] = None
            await save_db_async()
            await event.respond(msg, buttons=[Button.inline("👥 My Panel", b"reseller_panel")])
        except ValueError:
            await event.respond("❌ Valid User ID daalo:")
        return True

    return False
