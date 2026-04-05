# bot/premium.py
import asyncio
import datetime
import time

from telethon import events, Button, errors
from config import bot, OWNER_ID
from database import db, GLOBAL_STATE, get_user_data, save_persistent_db
from admin import is_admin, add_log

ALL_FEATURES = {
    "scheduler":            ("⏰ Scheduler",              "Forwarding",  True),
    "duplicate_filter":     ("🔁 Duplicate Filter",       "Filters",     True),
    "auto_shorten":         ("🔗 Auto Link Shortener",    "Tools",       True),
    "smart_filter":         ("🧠 Smart Filter",           "Filters",     True),
    "remove_links":         ("🚫 Remove Links",           "Tools",       False),
    "remove_user":          ("👤 Remove Usernames",       "Tools",       False),
    "custom_delay":         ("⏱ Custom Delay",            "Forwarding",  True),
    "keywords_filter":      ("🔑 Keyword Filters",        "Filters",     True),
    "replace_text":         ("🔄 Text Replacement",       "Tools",       True),
    "link_blocker":         ("🛑 Link Blocker",           "Tools",       True),
    "start_end_msg":        ("✉️ Start/End Message",      "Customise",   True),
    "prefix_suffix":        ("📝 Prefix/Suffix",          "Customise",   True),
    "backup_restore":       ("💾 Backup & Restore",       "Tools",       True),
    "multiple_sources":     ("📦 Multiple Sources (3+)",  "Forwarding",  True),
    "multiple_dest":        ("📤 Multiple Destinations",  "Forwarding",  True),
}


def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_premium_config():
    GLOBAL_STATE.setdefault("premium_config", {
        "paid_features": {k: v[2] for k, v in ALL_FEATURES.items()},
        "plan_price": "₹99/month",
        "plan_name": "Premium Plan",
        "upi_id": "",
        "payment_msg": "💳 Premium lene ke liye admin se contact karo।",
        "trial_days": 7,
        "trial_enabled": True,
        "free_source_limit": 2,
        "free_dest_limit": 2,
        "free_mode": False,
        "max_plan_days": 365,
        # New fields
        "promo_codes": {},          # code → {discount_pct, uses_left, expires_at}
        "referral_enabled": False,  # Referral program on/off
        "referral_bonus_days": 7,   # Referrer ko kitne days milenge
        "renewal_reminder_days": 3, # Expiry se kitne din pehle reminder bhejo
        "gift_enabled": True,       # Users can gift premium to others
    })
    return GLOBAL_STATE["premium_config"]


def is_feature_paid(feature_key: str) -> bool:
    config = get_premium_config()
    return config["paid_features"].get(feature_key, False)


def get_user_premium(user_id: int) -> dict:
    data = get_user_data(user_id)
    data.setdefault("premium", {
        "active": False,
        "expires_at": None,
        "plan": None,
        "given_by": None,
        "given_at": None,
    })
    return data["premium"]


def get_premium_history(user_id: int) -> list:
    """User ka premium purchase/renewal history."""
    return get_user_data(user_id).get("premium_history", [])


def record_premium_history(user_id: int, days: int, given_by: int = None, note: str = ""):
    """Har premium grant ko history mein record karo."""
    data = get_user_data(user_id)
    hist = data.setdefault("premium_history", [])
    hist.insert(0, {
        "timestamp": int(time.time()),
        "days":      days,
        "given_by":  given_by,
        "note":      note,
    })
    data["premium_history"] = hist[:20]


def get_usage_summary(user_id: int) -> dict:
    """User ka current usage — sources, dests."""
    data   = get_user_data(user_id)
    config = get_premium_config()
    return {
        "sources":    len(data.get("sources", [])),
        "dests":      len(data.get("destinations", [])),
        "src_limit":  config.get("free_source_limit", 2),
        "dest_limit": config.get("free_dest_limit", 2),
        "is_premium": is_premium_user(user_id),
        "forwarding": data.get("settings", {}).get("running", False),
    }


def validate_promo_code(code: str) -> dict | None:
    """Promo code validate karo."""
    config = get_premium_config()
    entry  = config.get("promo_codes", {}).get(code.upper().strip())
    if not entry:
        return None
    if entry.get("uses_left", 0) <= 0:
        return None
    exp = entry.get("expires_at")
    if exp and int(time.time()) > exp:
        return None
    return entry


def use_promo_code(code: str) -> bool:
    """Promo code use karo — uses_left decrement."""
    config = get_premium_config()
    key    = code.upper().strip()
    codes  = config.get("promo_codes", {})
    if key in codes and codes[key].get("uses_left", 0) > 0:
        codes[key]["uses_left"] -= 1
        save_persistent_db()
        return True
    return False


def get_referral_code(user_id: int) -> str:
    """User ka unique referral code."""
    data = get_user_data(user_id)
    if "referral_code" not in data:
        import random, string
        data["referral_code"] = "REF" + "".join(
            random.choices(string.ascii_uppercase + string.digits, k=6)
        )
        save_persistent_db()
    return data["referral_code"]


async def give_premium(target_id: int, days: int, given_by: int = None):
    """
    Give or extend premium for a user.
    - days == 0  → Lifetime premium
    - days > 0, user already active  → EXTEND from current expiry (not reset)
    - days > 0, user not active      → Activate fresh
    """
    config      = get_premium_config()
    target_data = get_user_data(target_id)
    prem        = target_data.setdefault("premium", {})
    now         = int(time.time())
    max_days    = config.get("max_plan_days", 365)

    if days == 0:
        # Lifetime
        prem["active"]     = True
        prem["plan"]       = config.get("plan_name", "Premium")
        prem["given_by"]   = given_by
        prem["given_at"]   = now
        prem["expires_at"] = None
        exp_txt = "Lifetime ♾️"
    elif days > 0 and prem.get("active") and prem.get("expires_at"):
        # Auto-extend: add days on top of current expiry
        days = min(days, max_days)
        new_exp = max(prem["expires_at"], now) + (days * 86400)
        prem["expires_at"] = new_exp
        prem["active"]     = True
        prem["plan"]       = config.get("plan_name", "Premium")
        prem["given_by"]   = given_by
        exp_txt = datetime.datetime.fromtimestamp(new_exp).strftime("%d %b %Y")
        record_premium_history(target_id, days, given_by=given_by, note="Extended")
        save_persistent_db()
        try:
            from config import bot as _bot
            await _bot.send_message(
                target_id,
                f"🎉 **Premium Extended!**\n\n"
                f"💎 +{days} days added\n"
                f"📅 New expiry: `{exp_txt}`\n\n" + _get_owner_footer(),
                buttons=[[Button.inline("💎 Status", b"premium_info")]]
            )
        except Exception:
            pass
        return exp_txt
    else:
        # Fresh activation
        days = min(days, max_days)
        prem["active"]     = True
        prem["plan"]       = config.get("plan_name", "Premium")
        prem["given_by"]   = given_by
        prem["given_at"]   = now
        prem["expires_at"] = now + (days * 86400)
        exp_txt = f"{days} days"

    record_premium_history(target_id, days, given_by=given_by,
                           note=f"Given by {given_by} — {exp_txt}")
    save_persistent_db()
    try:
        from config import bot
        days_left_str = get_remaining_days(target_id)
        await bot.send_message(
            target_id,
            f"🎉 **Tumhe Premium Mil Gaya!**\n\n"
            f"💎 Plan: `{prem['plan']}`\n"
            f"📅 Duration: `{exp_txt}`\n"
            f"⏳ Valid till: `{days_left_str}`\n\n"
            f"Ab sare premium features unlock ho gaye!\n\n" + _get_owner_footer(),
            buttons=[[Button.inline("💎 Premium Status", b"premium_info")]]
        )
    except Exception:
        pass
    return exp_txt


def is_premium_user(user_id: int) -> bool:
    if user_id == OWNER_ID:
        return True
    if is_admin(user_id):
        return True

    prem = get_user_premium(user_id)
    if not prem.get("active"):
        return False

    exp = prem.get("expires_at")
    if exp is not None and time.time() > exp:
        prem["active"] = False
        prem["expires_at"] = None
        prem["expired_at"] = int(time.time())
        save_persistent_db()
        # BUG 10 FIX: asyncio.create_task safe call — event loop check karo
        try:
            loop = asyncio.get_running_loop()
            if loop and loop.is_running():
                loop.create_task(_notify_expiry(user_id))
        except RuntimeError:
            # No event loop — schedule for later (background task will catch it)
            pass
        return False

    return True

# BUG 39 FIX: Dead code block hataya gaya (pehle return True ke baad dobara expiry check tha)

async def _notify_expiry(user_id: int):
    """User ko expiry notification bhejo — upgraded with downgrade warning."""
    try:
        prem = get_user_premium(user_id)
        exp  = prem.get("expires_at")
        if not exp:
            return
        days_left = max(0, int((exp - time.time()) / 86400))

        # Build downgrade warning — kya band hoga
        paid_active = []
        data = get_user_data(user_id)
        s    = data.get("settings", {})
        for key, (name, cat, is_paid) in ALL_FEATURES.items():
            if is_paid and s.get(key):
                paid_active.append(name)

        downgrade_warn = ""
        if paid_active:
            items = ", ".join(paid_active[:4])
            more  = f" (+{len(paid_active)-4} more)" if len(paid_active) > 4 else ""
            downgrade_warn = (
                f"\n\n⚠️ **Ye features band ho jaayenge:**\n"
                f"  {items}{more}"
            )

        if days_left <= 0:
            msg = (
                "❌ **Tumhara Premium Expire Ho Gaya!**\n\n"
                "Ab free plan pe aa gaye ho।"
                + downgrade_warn
                + "\n\n💎 Renew karo:"
            )
            btns = [[Button.inline("💳 Renew Now", b"buy_premium")]]
        elif days_left <= 1:
            msg = (
                "🚨 **Premium KAL expire ho raha hai!**\n\n"
                f"⏳ Kal se premium band ho jaayega।"
                + downgrade_warn
                + "\n\n💎 Abhi renew karo:"
            )
            btns = [[Button.inline("💳 Renew Now — Last Chance!", b"buy_premium")]]
        elif days_left <= 3:
            msg = (
                f"⚠️ **Premium {days_left} din mein expire ho raha hai!**\n\n"
                "Renew karo taaki features band na ho।"
                + downgrade_warn
            )
            btns = [[Button.inline("💳 Renew Premium", b"buy_premium")]]
        else:
            return  # No notification needed

        await bot.send_message(user_id, msg + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""), buttons=btns)
    except Exception:
        pass



def get_remaining_days(user_id: int) -> str:
    """User ke liye remaining days string return karo."""
    prem = get_user_premium(user_id)
    if not prem.get("active"):
        return "Inactive"
    exp = prem.get("expires_at")
    if exp is None:
        return "Lifetime \u267e\ufe0f"
    remaining = int((exp - time.time()) / 86400)
    if remaining <= 0:
        return "Expiring today"
    if remaining == 1:
        return "1 day"
    return f"{remaining} days"


def setup_trial_if_new(user_id: int, require_login: bool = True):
    """New user ko free trial do — sirf login ke baad."""
    config = get_premium_config()
    if not config.get("trial_enabled", True):
        return
    if config.get("free_mode", False):
        return
    trial_days = config.get("trial_days", 7)
    if trial_days <= 0:
        return
    data = get_user_data(user_id)
    if require_login:
        sessions = data.get("sessions", {})
        if not sessions:
            return
    prem = data.setdefault("premium", {})
    if prem.get("active"):
        return
    if prem.get("plan") is not None:
        return
    prem["active"]     = True
    prem["plan"]       = "Trial"
    prem["is_trial"]   = True
    prem["expires_at"] = int(time.time()) + (trial_days * 86400)
    prem["given_by"]   = 0
    prem["given_at"]   = int(time.time())
    record_premium_history(user_id, trial_days, given_by=0, note="Free trial")
    save_persistent_db()
    try:
        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            loop.create_task(_send_trial_welcome(user_id, trial_days))
    except RuntimeError:
        pass


async def _send_trial_welcome(user_id: int, trial_days: int):
    """Trial start hone par welcome message bhejo."""
    try:
        exp_date   = datetime.datetime.fromtimestamp(
            int(time.time()) + trial_days * 86400
        ).strftime("%d %b %Y")
        paid_feats = [v[0] for k, v in ALL_FEATURES.items() if is_feature_paid(k)][:6]
        feat_txt   = "\n".join("  \u2705 " + f for f in paid_feats)
        msg = (
            f"\U0001f381 **{trial_days}-Day Free Trial Started!**\n\n"
            f"`{trial_days} days` ka free premium trial mil gaya.\n"
            f"\U0001f4c5 Trial ends: `{exp_date}`\n\n"
            f"**Ye features unlock hain:**\n{feat_txt}\n\n"
            "_Trial khatam hone se pehle premium lo!_\n\n"
            + _get_owner_footer()
        )
        await bot.send_message(
            user_id, msg,
            buttons=[
                [Button.inline("\U0001f48e Buy Premium", b"buy_premium")],
                [Button.inline("\U0001f3e0 Main Menu",   b"main_menu")],
            ]
        )
    except Exception:
        pass


def check_source_limit(user_id: int, current_count: int) -> tuple:
    """Return (allowed: bool, message: str)"""
    if is_premium_user(user_id):
        return True, ""
    config = get_premium_config()
    limit  = config.get("free_source_limit", 2)
    if current_count >= limit:
        return False, (
            f"\U0001f512 **Free Plan Limit!**\n\n"
            f"Free plan mein sirf **{limit} sources** add kar sakte ho.\n"
            "Premium lo unlimited sources ke liye!\n\n"
            "\U0001f48e /premium dekho"
        )
    return True, ""


def check_dest_limit(user_id: int, current_count: int) -> tuple:
    """Return (allowed: bool, message: str)"""
    if is_premium_user(user_id):
        return True, ""
    config = get_premium_config()
    limit  = config.get("free_dest_limit", 2)
    if current_count >= limit:
        return False, (
            f"\U0001f512 **Free Plan Limit!**\n\n"
            f"Free plan mein sirf **{limit} destinations** add kar sakte ho.\n"
            "Premium lo unlimited destinations ke liye!\n\n"
            "\U0001f48e /premium dekho"
        )
    return True, ""


def is_free_mode() -> bool:
    return get_premium_config().get("free_mode", False)


def can_use_feature(user_id: int, feature_key: str) -> bool:
    """User is feature ko use kar sakta hai?"""
    if is_premium_user(user_id):
        return True
    if is_free_mode():
        return True
    return not is_feature_paid(feature_key)


async def premium_gate(event, feature_key: str, feature_name: str) -> bool:
    user_id = event.sender_id
    if can_use_feature(user_id, feature_key):
        return True
    config = get_premium_config()
    msg = (
        f"🔒 **Premium Feature**\n\n"
        f"**{feature_name}** sirf Premium users ke liye available hai।\n\n"
        f"💎 Plan: `{config['plan_name']}`\n"
        f"💰 Price: `{config['plan_price']}`\n\n"
        f"{config['payment_msg']}\n\n" + _get_owner_footer()
    )
    try:
        await event.answer("🔒 Premium Feature!", alert=True)
    except Exception:
        pass
    await event.respond(msg, buttons=[
        [Button.inline("💎 Premium Info", b"premium_info")],
        [Button.inline("🏠 Main Menu", b"main_menu")]
    ])
    return False


@bot.on(events.CallbackQuery(data=b"premium_info"))
async def premium_info(event):
    await event.answer()
    user_id = event.sender_id
    config  = get_premium_config()
    is_prem = is_premium_user(user_id)
    usage   = get_usage_summary(user_id)

    if is_prem:
        prem          = get_user_premium(user_id)
        plan_name     = prem.get("plan", "Premium")
        exp           = prem.get("expires_at")
        days_left_str = get_remaining_days(user_id)

        if exp is None:
            days_left_num = 9999
            exp_date      = "Lifetime ♾️"
        else:
            days_left_num = max(0, int((exp - time.time()) / 86400))
            exp_date      = datetime.datetime.fromtimestamp(exp).strftime("%d %b %Y, %I:%M %p")

        # Progress bar (based on original duration)
        total_days = int(prem.get("total_days", 30) or 30)
        pct        = min(round(days_left_num / max(total_days, 1) * 10), 10)
        bar        = "█" * pct + "░" * (10 - pct)
        urgency    = ""
        if 0 < days_left_num <= 1:
            urgency = "\n  🚨 **EXPIRES TODAY!** Abhi renew karo!"
        elif days_left_num <= 3:
            urgency = f"\n  ⚠️ Sirf **{days_left_num} days** bache — Renew karo!"

        # Usage stats
        src_bar  = f"{usage['sources']}/{usage['src_limit'] if not is_prem else '∞'}"
        dst_bar  = f"{usage['dests']}/{usage['dest_limit'] if not is_prem else '∞'}"
        running  = "🟢 Running" if usage["forwarding"] else "🔴 Stopped"

        is_trial   = prem.get("is_trial") or prem.get("plan") == "Trial"
        trial_tag  = " 🎁 _(Trial)_" if is_trial else ""
        paused_tag = " ⏸️ _(Paused)_" if prem.get("paused") else ""
        status_block = (
            "✅ **PREMIUM ACTIVE**\n"
            f"  💎 Plan: **{plan_name}**{trial_tag}{paused_tag}\n"
            f"  📅 Expires: `{exp_date}`\n"
            f"  ⏳ [{bar}] **{days_left_str}** remaining{urgency}\n"
            f"  📡 Forwarding: {running}\n"
            f"  📥 Sources: `{src_bar}` · 📤 Dests: `{dst_bar}`\n"
        )
        btns = [
            [Button.inline("💳 Renew / Extend",    b"buy_premium"),
             Button.inline("🎁 Gift Premium",       b"prem_gift_start")],
            [Button.inline("📊 Feature Status",     b"prem_feature_status"),
             Button.inline("🕘 History",             b"prem_history")],
            [Button.inline("⏸️ Pause",              b"prem_pause"),
             Button.inline("🔁 Transfer",           b"prem_transfer")],
            [Button.inline("🔗 Referral",           b"prem_referral"),
             Button.inline("🔔 Reminder",           b"prem_reminder_pref")],
            [Button.inline("🧾 Payment History",    b"pay_history"),
             Button.inline("📊 Analytics",          b"prem_feat_analytics")],
            [Button.inline("🏠 Main Menu",           b"main_menu")],
        ]
    else:
        # Free user — show what they're missing
        locked_count = sum(1 for k in ALL_FEATURES if is_feature_paid(k))
        src_used     = usage["sources"]
        src_limit    = usage["src_limit"]
        dst_used     = usage["dests"]
        dst_limit    = usage["dest_limit"]

        limits_warn = ""
        if src_used >= src_limit:
            limits_warn += f"\n  ⚠️ Sources full ({src_used}/{src_limit})"
        if dst_used >= dst_limit:
            limits_warn += f"\n  ⚠️ Dests full ({dst_used}/{dst_limit})"

        status_block = (
            "🆓 **FREE PLAN**\n"
            f"  📥 Sources: `{src_used}/{src_limit}` · 📤 Dests: `{dst_used}/{dst_limit}`{limits_warn}\n"
            f"  🔒 **{locked_count} premium features** locked\n"
        )
        btns = [
            [Button.inline("💎 Upgrade to Premium",  b"buy_premium")],
            [Button.inline("🔍 What Will Unlock?",   b"prem_unlock_preview"),
             Button.inline("📊 Compare Plans",        b"prem_compare")],
            [Button.inline("🎁 Redeem Code",         b"prem_redeem_gift"),
             Button.inline("🔗 Referral",             b"prem_referral")],
            [Button.inline("🏠 Main Menu",            b"main_menu")],
        ]

    paid_list = [f"  ✅ {v[0]}" for k, v in ALL_FEATURES.items() if is_feature_paid(k)]
    free_list = [f"  🆓 {v[0]}" for k, v in ALL_FEATURES.items() if not is_feature_paid(k)]

    txt = (
        f"💎 **{config['plan_name']}**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + status_block
        + f"\n💰 Price: **{config['plan_price']}**\n\n"
        "**💎 Premium Features:**\n"
        + "\n".join(paid_list[:8])
        + ("\n  _...aur bhi_" if len(paid_list) > 8 else "")
        + "\n\n**🆓 Free Features:**\n"
        + "\n".join(free_list[:4])
        + (f"\n\n{config['payment_msg']}" if not is_prem and config.get("payment_msg") else "")
        + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "")
    )
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass
    except Exception:
        await event.respond(txt, buttons=btns)


@bot.on(events.NewMessage(pattern='/premium'))
async def premium_cmd(event):
    user_id = event.sender_id
    config  = get_premium_config()
    is_prem = is_premium_user(user_id)
    usage   = get_usage_summary(user_id)

    if is_prem:
        prem      = get_user_premium(user_id)
        days_left = get_remaining_days(user_id)
        exp       = prem.get("expires_at")
        exp_str   = datetime.datetime.fromtimestamp(exp).strftime("%d %b %Y") if exp else "Lifetime ♾️"
        txt = (
            f"💎 **Premium Active!**\n\n"
            f"📅 Expires: `{exp_str}`\n"
            f"⏳ Remaining: `{days_left}`\n"
            f"📥 Sources: `{usage['sources']}` (unlimited)\n"
            f"📤 Dests: `{usage['dests']}` (unlimited)\n\n"
            f"Tap below for full details।\n\n" + _get_owner_footer()
        )
        btns = [
            [Button.inline("💎 Full Status", b"premium_info"),
             Button.inline("💳 Renew",       b"buy_premium")],
            [Button.inline("🏠 Main Menu",   b"main_menu")],
        ]
    else:
        src_limit = usage["src_limit"]
        dst_limit = usage["dest_limit"]
        txt = (
            f"💎 **{config['plan_name']}**\n\n"
            f"❌ Premium active nahi hai\n\n"
            f"📥 Sources: `{usage['sources']}/{src_limit}` (limit)\n"
            f"📤 Dests: `{usage['dests']}/{dst_limit}` (limit)\n\n"
            f"💰 Price: `{config['plan_price']}`\n\n" + _get_owner_footer()
        )
        btns = [
            [Button.inline("💎 Buy Premium",       b"buy_premium"),
             Button.inline("🔍 What Unlocks?",     b"prem_unlock_preview")],
            [Button.inline("📊 Compare Plans",     b"prem_compare"),
             Button.inline("🏠 Main Menu",         b"main_menu")],
        ]
    await event.respond(txt, buttons=btns)


@bot.on(events.CallbackQuery(data=b"adm_premium"))
async def adm_premium_panel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    free_mode = config.get("free_mode", False)
    max_days = config.get("max_plan_days", 365)
    trial_days = config.get("trial_days", 7)
    trial_enabled = config.get("trial_enabled", True)
    src_limit = config.get("free_source_limit", 2)
    dest_limit = config.get("free_dest_limit", 2)
    total_premium = sum(
        1 for uid, udata in list(db.items())
        if isinstance(udata, dict) and is_premium_user(int(uid))
    )
    mode_status    = "🟢 FREE MODE — sab users free" if free_mode else "🔒 PAID MODE — Premium lock"
    mode_btn_label = "🔒 Switch to Paid Mode" if free_mode else "🆓 Switch to Free Mode"
    mode_btn_data  = b"adm_paid_mode_on" if free_mode else b"adm_free_mode_on"

    # Premium users expiry breakdown
    expiring_3  = sum(1 for uid, ud in list(db.items()) if isinstance(ud, dict) and
                      is_premium_user(int(uid)) and 0 < ud.get("premium",{}).get("days_remaining",99) <= 3)
    expiring_7  = sum(1 for uid, ud in list(db.items()) if isinstance(ud, dict) and
                      is_premium_user(int(uid)) and 3 < ud.get("premium",{}).get("days_remaining",99) <= 7)

    txt = (
        "💎 **PREMIUM MANAGEMENT**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚙️ Mode: **{mode_status}**\n\n"
        f"**👥 Users:**\n"
        f"  Active Premium: `{total_premium}`\n"
        + (f"  ⚠️ Expiring ≤3d: `{expiring_3}`\n" if expiring_3 else "")
        + (f"  ⏳ Expiring ≤7d: `{expiring_7}`\n" if expiring_7 else "")
        + f"\n**💰 Plan:**\n"
        f"  Name: `{config['plan_name']}`\n"
        f"  Price: `{config['plan_price']}`\n"
        f"  Max: `{max_days} days`\n\n"
        f"**🎁 Trial:** {'✅ ON' if trial_enabled else '❌ OFF'} ({trial_days} days)\n"
        f"**📦 Free Limits:** src `{src_limit}` · dest `{dest_limit}`\n"
    )
    btns = [
        [Button.inline(mode_btn_label, mode_btn_data)],
        [Button.inline("🔧 Feature Toggle",    b"adm_feat_toggle"),
         Button.inline("👥 Manage Users",      b"adm_prem_users")],
        [Button.inline("💰 Set Price/Name",    b"adm_prem_price"),
         Button.inline("💳 Payment Msg",       b"adm_prem_paymsg")],
        [Button.inline("➕ Give Premium",      b"adm_give_prem"),
         Button.inline("👥 Bulk Give",         b"adm_bulk_prem")],
        [Button.inline("➖ Remove Premium",    b"adm_rem_prem"),
         Button.inline("🏷️ Promo Codes",      b"adm_promo_menu")],
        [Button.inline("🎁 Trial Settings",    b"adm_trial_settings"),
         Button.inline("📊 Limits",            b"adm_set_limits")],
        [Button.inline("📅 Max Plan Days",     b"adm_max_plan_days"),
         Button.inline("🔗 Referral Program",  b"adm_referral_settings")],
        [Button.inline("📊 Premium Stats",     b"prem_leaderboard")],
        [Button.inline("🔙 Back",              b"adm_main")]
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_feat_toggle"))
async def adm_feat_toggle_menu(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    btns = []
    for key, (name, category, _) in ALL_FEATURES.items():
        is_paid = config["paid_features"].get(key, False)
        status = "🔒 Paid" if is_paid else "🆓 Free"
        btns.append([Button.inline(f"{name} — {status}", f"adm_ftgl_{key}".encode())])
    btns.append([
        Button.inline("🆓 Sab Free Karo", b"adm_all_free"),
        Button.inline("🔒 Sab Paid Karo", b"adm_all_paid")
    ])
    btns.append([Button.inline("🔄 Default Reset Karo", b"adm_feat_reset")])
    btns.append([Button.inline("🔙 Back", b"adm_premium")])
    try:
        await event.edit("🔧 **Feature Toggle**\n\nKisi bhi feature par click karo:", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_ftgl_"))
async def adm_feature_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    feature_key = event.data.decode().replace("adm_ftgl_", "")
    config = get_premium_config()
    if feature_key not in ALL_FEATURES:
        return await event.answer("Feature not found!", alert=True)
    current = config["paid_features"].get(feature_key, False)
    config["paid_features"][feature_key] = not current
    save_persistent_db()
    feature_name = ALL_FEATURES[feature_key][0]
    new_status = "🔒 Paid" if not current else "🆓 Free"
    add_log(event.sender_id, f"Feature Toggle", details=f"{feature_name} → {new_status}")
    await event.answer(f"{feature_name} ab {new_status} hai!", alert=False)
    await adm_feat_toggle_menu(event)


@bot.on(events.CallbackQuery(data=b"adm_all_free"))
async def adm_all_free(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    for key in ALL_FEATURES:
        config["paid_features"][key] = False
    save_persistent_db()
    add_log(event.sender_id, "All Features → FREE")
    await event.answer("✅ Sab features free ho gaye!", alert=True)
    await adm_feat_toggle_menu(event)


@bot.on(events.CallbackQuery(data=b"adm_all_paid"))
async def adm_all_paid(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    for key in ALL_FEATURES:
        config["paid_features"][key] = True
    save_persistent_db()
    add_log(event.sender_id, "All Features → PAID")
    await event.answer("✅ Sab features paid ho gaye!", alert=True)
    await adm_feat_toggle_menu(event)


@bot.on(events.CallbackQuery(data=b"adm_feat_reset"))
async def adm_feat_reset(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    config["paid_features"] = {k: v[2] for k, v in ALL_FEATURES.items()}
    save_persistent_db()
    add_log(event.sender_id, "Features → Default Reset")
    paid_count = sum(1 for v in config["paid_features"].values() if v)
    free_count = len(config["paid_features"]) - paid_count
    await event.answer(f"✅ Reset! {paid_count} paid, {free_count} free.", alert=True)
    await adm_feat_toggle_menu(event)


@bot.on(events.CallbackQuery(data=b"adm_give_prem"))
async def adm_give_prem_start(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_give_prem_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "➕ **Premium Do**\n\n"
        "Format bhejo:\n"
        "`USER_ID DAYS`\n\n"
        "Examples:\n"
        "`5768614596 30` — 30 din ke liye\n"
        "`5768614596 365` — 1 Saal\n"
        "`5768614596 0` — Lifetime (permanent)\n\n" + _get_owner_footer(),
        buttons=[Button.inline("🔙 Cancel", b"adm_premium")]
    )


@bot.on(events.CallbackQuery(pattern=b"adm_give_prem_"))
async def adm_give_prem_uid_handler(event):
    """User profile se seedha premium do — UID already known"""
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    try:
        target_uid = int(event.data.decode().replace("adm_give_prem_", ""))
    except ValueError:
        return await event.answer("❌ Invalid user ID", alert=True)
    data = get_user_data(event.sender_id)
    data["temp_data"]["give_prem_uid"] = target_uid
    data["step"] = "adm_give_prem_days_input"
    data["step_since"] = time.time()
    await event.edit(
        f"💎 **Premium Do — User `{target_uid}`**\n\n"
        "Kitne din ke liye? Sirf number bhejo:\n\n"
        "`30` = 30 din\n`365` = 1 Saal\n`0` = Lifetime\n\n" + _get_owner_footer(),
        buttons=[Button.inline("🔙 Cancel", f"adm_view_u_{target_uid}".encode())]
    )


@bot.on(events.CallbackQuery(data=b"adm_rem_prem"))
async def adm_rem_prem_start(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_rem_prem_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "➖ **Premium Wapas Lo**\n\nUser ID bhejo:\n`5768614596`" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "",
        buttons=[Button.inline("🔙 Cancel", b"adm_premium")]
    )


@bot.on(events.CallbackQuery(data=b"adm_prem_users"))
async def adm_prem_users(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    now = int(time.time())
    active_users  = []
    paused_users  = []
    expired_soon  = []

    for uid, udata in list(db.items()):
        if not isinstance(udata, dict): continue
        prem = udata.get("premium", {})
        if prem.get("paused"):
            saved = prem.get("saved_days", 0)
            paused_users.append((uid, saved))
        elif prem.get("active"):
            exp = prem.get("expires_at")
            if exp is None:
                active_users.append((uid, "♾️", 9999))
            else:
                days_left = max(0, int((exp - now) / 86400))
                exp_str   = f"{days_left}d"
                active_users.append((uid, exp_str, days_left))
                if days_left <= 7:
                    expired_soon.append((uid, days_left))

    active_users.sort(key=lambda x: x[2])  # Soonest expiry first
    total = len(active_users) + len(paused_users)

    if not total:
        txt = "💎 **Premium Users**\n\nKoi premium user nahi hai abhi।"
    else:
        txt = (
            f"💎 **Premium Users ({total})**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        if expired_soon:
            txt += f"⚠️ **Expiring ≤7 days ({len(expired_soon)}):**\n"
            for uid, d in expired_soon:
                txt += f"  `{uid}` — {d}d\n"
            txt += "\n"
        txt += f"**✅ Active ({len(active_users)}):**\n"
        for uid, exp_str, _ in active_users[:15]:
            txt += f"  `{uid}` — {exp_str}\n"
        if len(active_users) > 15:
            txt += f"  _...+{len(active_users)-15} more_\n"
        if paused_users:
            txt += f"\n**⏸️ Paused ({len(paused_users)}):**\n"
            for uid, saved in paused_users:
                txt += f"  `{uid}` — {saved}d saved\n"

    try:
        await event.edit(txt, buttons=[
            [Button.inline("✏️ Extend/Modify User", b"adm_extend_user"),
             Button.inline("📊 Stats",              b"prem_leaderboard")],
            [Button.inline("🔙 Back",               b"adm_premium")],
        ])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_prem_price"))
async def adm_prem_price(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    get_user_data(event.sender_id)["step"] = "adm_prem_price_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        f"💰 **Plan Price/Name Set Karo**\n\n"
        f"Current: `{config['plan_name']} — {config['plan_price']}`\n\n"
        "Format: `PLAN_NAME | PRICE`\n\nExample: `Premium Plan | ₹99/month`",
        buttons=[Button.inline("🔙 Cancel", b"adm_premium")]
    )


@bot.on(events.CallbackQuery(data=b"adm_prem_paymsg"))
async def adm_prem_paymsg(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_prem_paymsg_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "💳 **Payment Message Set Karo**\n\nYe message user ko dikhega:\n\nExample:\n`💳 @YourUsername ko contact karo।`",
        buttons=[Button.inline("🔙 Cancel", b"adm_premium")]
    )


async def handle_premium_inputs(event, user_id, step) -> bool:
    config = get_premium_config()

    if step == "adm_give_prem_input":
        try:
            parts = event.text.strip().split()
            if len(parts) != 2:
                await event.respond("❌ Format: `USER_ID DAYS`\nExample: `123456 30` ya `123456 0` lifetime ke liye")
                return True
            target_id = int(parts[0])
            days = int(parts[1])
            target_data = get_user_data(target_id)
            prem = target_data.setdefault("premium", {})
            prem["active"] = True
            prem["plan"] = config["plan_name"]
            prem["given_by"] = user_id
            prem["given_at"] = int(time.time())
            max_days = config.get("max_plan_days", 365)

            # BUG 37 FIX: days=0 = Lifetime (None), nahi ki current timestamp
            if days == 0:
                prem["expires_at"] = None  # Permanent/Lifetime
                exp_txt = "Lifetime ♾️"
            else:
                if days > max_days:
                    days = max_days
                    await event.respond(f"⚠️ Max {max_days} days allowed। {max_days} days set kar diya.")
                prem["expires_at"] = int(time.time()) + (days * 86400)
                exp_txt = f"{days} days"

            get_user_data(user_id)["step"] = None
            save_persistent_db()
            add_log(user_id, "Give Premium", target=target_id, details=exp_txt)
            try:
                await bot.send_message(
                    target_id,
                    f"🎉 **Tumhe Premium Mil Gaya!**\n\n"
                    f"💎 Plan: `{config['plan_name']}`\n"
                    f"📅 Duration: `{exp_txt}`\n\n"
                    f"Ab sare premium features unlock ho gaye hain!\n\n" + _get_owner_footer()
                )
            except Exception:
                pass
            await event.respond(
                f"✅ **Premium De Diya!**\n\n👤 User: `{target_id}`\n📅 Duration: `{exp_txt}`",
                buttons=[Button.inline("🔙 Premium Panel", b"adm_premium")]
            )
        except ValueError:
            await event.respond("❌ Valid User ID aur number bhejo।")
        return True

    elif step == "adm_rem_prem_input":
        try:
            target_id = int(event.text.strip())
            target_data = get_user_data(target_id)
            prem = target_data.get("premium", {})
            prem["active"] = False
            prem["expires_at"] = None
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            add_log(user_id, "Remove Premium", target=target_id)
            try:
                await bot.send_message(
                    target_id,
                    "❌ **Tumhara Premium Remove Ho Gaya।**\n\nPremium features ab available nahi hain।" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") 
                )
            except Exception:
                pass
            await event.respond(
                f"✅ User `{target_id}` ka premium remove ho gaya।",
                buttons=[Button.inline("🔙 Premium Panel", b"adm_premium")]
            )
        except ValueError:
            await event.respond("❌ Valid User ID bhejo।")
        return True

    elif step == "adm_prem_price_input":
        if "|" not in event.text:
            await event.respond("❌ Format: `PLAN_NAME | PRICE`")
            return True
        parts = event.text.split("|", 1)
        config["plan_name"] = parts[0].strip()
        config["plan_price"] = parts[1].strip()
        get_user_data(user_id)["step"] = None
        save_persistent_db()
        await event.respond(
            f"✅ Saved!\nPlan: `{config['plan_name']}`\nPrice: `{config['plan_price']}`",
            buttons=[Button.inline("🔙 Premium Panel", b"adm_premium")]
        )
        return True

    elif step == "adm_prem_paymsg_input":
        config["payment_msg"] = event.text.strip()
        get_user_data(user_id)["step"] = None
        save_persistent_db()
        await event.respond("✅ Payment message save ho gaya!", buttons=[Button.inline("🔙 Premium Panel", b"adm_premium")])
        return True

    # BUG 43 FIX: adm_src_limit_input aur adm_dest_limit_input handlers missing the
    elif step == "adm_src_limit_input":
        try:
            limit = int(event.text.strip())
            if limit < 1:
                limit = 1
            config["free_source_limit"] = limit
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(
                f"✅ Free source limit: `{limit}`",
                buttons=[Button.inline("🔙 Back", b"adm_set_limits")]
            )
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True

    elif step == "adm_dest_limit_input":
        try:
            limit = int(event.text.strip())
            if limit < 1:
                limit = 1
            config["free_dest_limit"] = limit
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(
                f"✅ Free destination limit: `{limit}`",
                buttons=[Button.inline("🔙 Back", b"adm_set_limits")]
            )
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True

    elif step == "adm_max_days_input":
        try:
            days = int(event.text.strip())
            config["max_plan_days"] = days
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(f"✅ Max plan duration: `{days} days`", buttons=[Button.inline("🔙 Back", b"adm_premium")])
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True

    elif step == "adm_promo_input_days":
        try:
            parts = event.raw_text.strip().split()
            if len(parts) != 3:
                await event.respond("❌ Format: `CODE DAYS USES`\nExample: `WELCOME30 30 100`")
                return True
            code, days_v, uses_v = parts[0].upper(), int(parts[1]), int(parts[2])
            config.setdefault("promo_codes", {})[code] = {
                "bonus_days": days_v,
                "discount_pct": 0,
                "uses_left": uses_v,
                "expires_at": None,
            }
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            add_log(user_id, "Promo Code", details=f"Created {code}: {days_v}d, {uses_v} uses")
            await event.respond(
                f"✅ Code `{code}` created!\n{days_v} bonus days | {uses_v} uses",
                buttons=[Button.inline("🏷️ Promo Manager", b"adm_promo_menu")]
            )
        except (ValueError, IndexError):
            await event.respond("❌ Format: `CODE DAYS USES`")
        return True

    elif step == "adm_promo_input_disc":
        try:
            parts = event.raw_text.strip().split()
            if len(parts) != 3:
                await event.respond("❌ Format: `CODE PERCENT USES`\nExample: `SAVE20 20 50`")
                return True
            code, pct_v, uses_v = parts[0].upper(), int(parts[1]), int(parts[2])
            if not 1 <= pct_v <= 99:
                await event.respond("❌ Percent 1-99 ke beech hona chahiye।")
                return True
            config.setdefault("promo_codes", {})[code] = {
                "bonus_days": 0,
                "discount_pct": pct_v,
                "uses_left": uses_v,
                "expires_at": None,
            }
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            add_log(user_id, "Promo Code", details=f"Created {code}: {pct_v}% off, {uses_v} uses")
            await event.respond(
                f"✅ Code `{code}` created!\n{pct_v}% discount | {uses_v} uses",
                buttons=[Button.inline("🏷️ Promo Manager", b"adm_promo_menu")]
            )
        except (ValueError, IndexError):
            await event.respond("❌ Format: `CODE PERCENT USES`")
        return True

    elif step == "adm_ref_bonus_input":
        try:
            days = int(event.raw_text.strip())
            if days < 1: days = 1
            config["referral_bonus_days"] = days
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(
                f"✅ Referral bonus: `{days} days`",
                buttons=[Button.inline("🔙 Referral Settings", b"adm_referral_settings")]
            )
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True

    elif step == "adm_extend_user_input":
        raw  = event.raw_text.strip()
        parts = raw.split()
        get_user_data(user_id)["step"] = None
        if len(parts) < 2:
            await event.respond("❌ Format: `USER_ID DAYS`")
            return True
        try:
            target_id = int(parts[0])
            action    = parts[1]
            if action.lower() == "reset":
                tdata = get_user_data(target_id)
                tdata.get("premium", {}).update({"active": False, "expires_at": None})
                save_persistent_db()
                await event.respond(
                    f"✅ User `{target_id}` premium removed.",
                    buttons=[Button.inline("🔙 Users", b"adm_prem_users")]
                )
            elif action == "0":
                await give_premium(target_id, 0, given_by=user_id)
                add_log(user_id, "Extend User", target=target_id, details="Lifetime")
                await event.respond(
                    f"✅ User `{target_id}` — Lifetime!",
                    buttons=[Button.inline("🔙 Users", b"adm_prem_users")]
                )
            else:
                delta = int(action)
                prem  = get_user_premium(target_id)
                exp   = prem.get("expires_at") or int(time.time())
                new_exp = max(int(time.time()), exp) + (delta * 86400)
                prem["expires_at"] = new_exp
                prem["active"]     = True
                save_persistent_db()
                direction = "added" if delta > 0 else "removed"
                new_date  = datetime.datetime.fromtimestamp(new_exp).strftime("%d %b %Y")
                add_log(user_id, "Extend User", target=target_id, details=f"{delta}d")
                await event.respond(
                    f"\u2705 `{abs(delta)}` days {direction}.\nNew expiry: `{new_date}`",
                    buttons=[Button.inline("🔙 Users", b"adm_prem_users")]
                )
        except (ValueError, IndexError):
            await event.respond("❌ Valid USER_ID aur DAYS/reset bhejo।")
        return True

    elif step == "adm_bulk_prem_input":
        lines = event.raw_text.strip().split("\n")
        if len(lines) < 2:
            await event.respond("❌ Format: first line = DAYS, phir User IDs")
            return True
        try:
            days    = int(lines[0].strip())
            uids    = [int(l.strip()) for l in lines[1:] if l.strip().isdigit()]
            success = 0
            failed  = 0
            for target in uids:
                try:
                    await give_premium(target, days, given_by=user_id)
                    success += 1
                except Exception:
                    failed += 1
            get_user_data(user_id)["step"] = None
            add_log(user_id, "Bulk Premium", details=f"{days}d to {success} users")
            await event.respond(
                f"✅ **Bulk Premium Done!**\n\n"
                f"✅ Success: {success}\n❌ Failed: {failed}\n📅 Duration: {days} days",
                buttons=[Button.inline("🔙 Premium Panel", b"adm_premium")]
            )
        except (ValueError, IndexError):
            await event.respond("❌ First line mein valid days number bhejo।")
        return True

    return False


# BUG 40 FIX: adm_trial_settings aur adm_trial_ conflict
# Solution: adm_trial_settings ko exact data match se handle karo
# adm_trial_days_ prefix use karo conflict se bachne ke liye
@bot.on(events.CallbackQuery(data=b"adm_trial_settings"))
async def adm_trial_settings(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    trial_days = config.get("trial_days", 7)
    trial_enabled = config.get("trial_enabled", True)
    txt = (
        "🎁 **Trial System Settings**\n\n"
        f"Status: {'✅ ON' if trial_enabled else '❌ OFF'}\n"
        f"Trial Duration: `{trial_days} days`\n\n"
        "Naya user pehli baar bot use kare to automatically\n"
        "itne din ke liye premium features milenge।"
    )
    # BUG 40 FIX: adm_trial_3 → adm_tdays_3 (conflict avoid)
    btns = [
        [Button.inline("🔴 Disable" if trial_enabled else "🟢 Enable", b"adm_tgl_trial")],
        [Button.inline("3 Days", b"adm_tdays_3"), Button.inline("7 Days", b"adm_tdays_7"),
         Button.inline("14 Days", b"adm_tdays_14"), Button.inline("30 Days", b"adm_tdays_30")],
        [Button.inline("🔙 Back", b"adm_premium")]
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_tgl_trial"))
async def adm_tgl_trial(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    config["trial_enabled"] = not config.get("trial_enabled", True)
    save_persistent_db()
    await event.answer(f"Trial {'ON' if config['trial_enabled'] else 'OFF'}!")
    await adm_trial_settings(event)


# BUG 40 FIX: adm_tdays_ prefix use kiya (was adm_trial_ jo settings ke saath conflict karta tha)
@bot.on(events.CallbackQuery(pattern=b"adm_tdays_"))
async def adm_trial_days_set(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    days = int(event.data.decode().split("_")[-1])
    get_premium_config()["trial_days"] = days
    save_persistent_db()
    await event.answer(f"Trial duration: {days} days!")
    await adm_trial_settings(event)


@bot.on(events.CallbackQuery(data=b"adm_set_limits"))
async def adm_set_limits(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    src_limit = config.get("free_source_limit", 2)
    dest_limit = config.get("free_dest_limit", 2)
    txt = (
        "📊 **Free Plan Limits**\n\n"
        f"📦 Max Sources (Free): `{src_limit}`\n"
        f"📤 Max Destinations (Free): `{dest_limit}`\n\n"
        "Premium users ke liye koi limit nahi।"
    )
    btns = [
        [Button.inline("📦 Source Limit", b"adm_src_limit"),
         Button.inline("📤 Dest Limit", b"adm_dest_limit")],
        [Button.inline("🔙 Back", b"adm_premium")]
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_src_limit"))
async def adm_src_limit(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_src_limit_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "📦 **Max Sources for Free Users**\n\nNumber bhejo (e.g., `2`):",
        buttons=[Button.inline("🔙 Cancel", b"adm_set_limits")]
    )


@bot.on(events.CallbackQuery(data=b"adm_dest_limit"))
async def adm_dest_limit(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_dest_limit_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "📤 **Max Destinations for Free Users**\n\nNumber bhejo (e.g., `2`):",
        buttons=[Button.inline("🔙 Cancel", b"adm_set_limits")]
    )


@bot.on(events.CallbackQuery(data=b"adm_free_mode_on"))
async def adm_free_mode_on(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    config["free_mode"] = True
    save_persistent_db()
    add_log(event.sender_id, "Bot Mode Changed", details="FREE MODE ON")
    await event.answer("✅ FREE MODE ON! Ab sab users ko sab features free hain।", alert=True)
    await adm_premium_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_paid_mode_on"))
async def adm_paid_mode_on(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_premium_config()
    config["free_mode"] = False
    save_persistent_db()
    add_log(event.sender_id, "Bot Mode Changed", details="PAID MODE ON")
    await event.answer("🔒 PAID MODE ON! Premium features ab lock hain।", alert=True)
    await adm_premium_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_max_plan_days"))
async def adm_max_plan_days(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_max_days_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    config = get_premium_config()
    await event.edit(
        f"📅 **Max Plan Duration**\n\nCurrent: `{config.get('max_plan_days', 365)} days`\n\n"
        "Number bhejo (days mein):\nExample: `30` ya `365`",
        buttons=[Button.inline("🔙 Cancel", b"adm_premium")]
    )


# ══════════════════════════════════════════════════════════════
# 💎 PREMIUM v2 — New Handlers
# ══════════════════════════════════════════════════════════════

# ── What will unlock (free user preview) ─────────────────────
@bot.on(events.CallbackQuery(data=b"prem_unlock_preview"))
async def prem_unlock_preview(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data.get("settings", {})

    lines = []
    for key, (name, cat, is_paid) in ALL_FEATURES.items():
        if not is_paid: continue
        currently_using = bool(s.get(key, False))
        if currently_using:
            lines.append(f"  🔓 **{name}** — already using (will keep)")
        else:
            lines.append(f"  ✨ **{name}** — will unlock!")

    # Limits
    config    = get_premium_config()
    src_limit = config.get("free_source_limit", 2)
    dst_limit = config.get("free_dest_limit", 2)
    lines.insert(0, f"  📥 Sources: `{src_limit}` → **Unlimited**")
    lines.insert(1, f"  📤 Destinations: `{dst_limit}` → **Unlimited**")
    lines.insert(2, "")

    body = "\n".join(lines)
    try:
        await event.edit(
            "✨ **WHAT PREMIUM UNLOCKS FOR YOU**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{body}\n\n"
            "_Premium lene ke baad ye sab turant mil jaayega!_",
            buttons=[
                [Button.inline("💎 Buy Premium", b"buy_premium")],
                [Button.inline("🔙 Back",         b"premium_info")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Plan comparison table ─────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_compare"))
async def prem_compare(event):
    await event.answer()
    config = get_premium_config()
    src_l  = config.get("free_source_limit", 2)
    dst_l  = config.get("free_dest_limit", 2)

    paid_feats = [v[0] for k, v in ALL_FEATURES.items() if is_feature_paid(k)]
    free_feats = [v[0] for k, v in ALL_FEATURES.items() if not is_feature_paid(k)]

    def yn(b): return "✅" if b else "❌"

    rows = [
        f"  📥 Sources       {yn(False)} {src_l}       {yn(True)} Unlimited",
        f"  📤 Destinations  {yn(False)} {dst_l}       {yn(True)} Unlimited",
    ]
    for feat in paid_feats[:8]:
        rows.append(f"  {feat:<22} ❌          ✅")
    for feat in free_feats[:3]:
        rows.append(f"  {feat:<22} ✅          ✅")

    col_header = "  Feature                 🆓 Free    💎 Premium"
    separator  = "  " + "─" * 44

    try:
        await event.edit(
            "📊 **FREE vs PREMIUM**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"`{col_header}`\n"
            f"`{separator}`\n"
            + "\n".join(f"`{r}`" for r in rows)
            + f"\n\n💰 Price: **{config['plan_price']}**",
            buttons=[
                [Button.inline("💎 Buy Premium", b"buy_premium")],
                [Button.inline("🔙 Back",         b"premium_info")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Feature status (premium user) ────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_feature_status"))
async def prem_feature_status(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data.get("settings", {})

    lines = []
    cats  = {}
    for key, (name, cat, is_paid) in ALL_FEATURES.items():
        cats.setdefault(cat, []).append((key, name, is_paid))

    for cat, feats in cats.items():
        lines.append(f"\n**{cat}:**")
        for key, name, is_paid in feats:
            active = bool(s.get(key, False))
            badge  = "🟢" if active else "⚪"
            lock   = "" if (is_premium_user(uid) or not is_paid) else " 🔒"
            lines.append(f"  {badge} {name}{lock}")

    try:
        await event.edit(
            "📊 **YOUR FEATURE STATUS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🟢 = Active  ⚪ = Inactive  🔒 = Locked\n"
            + "\n".join(lines),
            buttons=[[Button.inline("🔙 Back", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Renewal history ───────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_history"))
async def prem_history(event):
    await event.answer()
    uid  = event.sender_id
    hist = get_premium_history(uid)

    if not hist:
        try:
            await event.edit(
                "🕘 **PREMIUM HISTORY**\n\n_Abhi tak koi record nahi।_",
                buttons=[[Button.inline("🔙 Back", b"premium_info")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    lines = []
    for h in hist[:10]:
        ts   = datetime.datetime.fromtimestamp(h["timestamp"]).strftime("%d %b %Y")
        days = h.get("days", 0)
        dur  = "Lifetime" if days == 0 else f"{days}d"
        note = h.get("note", "")[:30]
        lines.append(f"  📅 {ts} — {dur}  _{note}_")

    try:
        await event.edit(
            "🕘 **PREMIUM HISTORY** (Last 10)\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n".join(lines),
            buttons=[[Button.inline("🔙 Back", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Gift premium — sender side ────────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_gift_start"))
async def prem_gift_start(event):
    await event.answer()
    config = get_premium_config()
    if not config.get("gift_enabled", True):
        return await event.answer("Gift feature disabled hai!", alert=True)
    if not is_premium_user(event.sender_id):
        return await event.answer("Gift karne ke liye premium chahiye!", alert=True)
    data = get_user_data(event.sender_id)
    data["step"]       = "prem_gift_input_id"
    data["step_since"] = int(time.time())
    try:
        await event.edit(
            "🎁 **GIFT PREMIUM**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Jis user ko gift karna hai uska **Telegram User ID** type karo:\n"
            "_(Admin se ya /id command se pata karo)_",
            buttons=[[Button.inline("❌ Cancel", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "prem_gift_input_id"))
async def prem_gift_id_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    try:
        target_id = int(event.raw_text.strip())
    except ValueError:
        await event.respond("❌ Valid User ID bhejo (numbers only)।",
                            buttons=[[Button.inline("🔙 Back", b"premium_info")]])
        return
    if target_id == uid:
        await event.respond("❌ Khud ko gift nahi kar sakte!",
                            buttons=[[Button.inline("🔙 Back", b"premium_info")]])
        return
    data["temp_data"]["gift_target"] = target_id
    data["step"] = "prem_gift_input_days"
    await event.respond(
        f"🎁 Gift to: `{target_id}`\n\nKitne din ka gift dena hai? (e.g. `30`)\n"
        "_(0 = lifetime — sirf admin kar sakta hai)_",
        buttons=[[Button.inline("❌ Cancel", b"premium_info")]]
    )


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "prem_gift_input_days"))
async def prem_gift_days_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        days = int(event.raw_text.strip())
    except ValueError:
        await event.respond("❌ Valid number bhejo।")
        return
    if days == 0 and not is_admin(uid):
        await event.respond("❌ Lifetime gift sirf admin de sakta hai।",
                            buttons=[[Button.inline("🔙 Back", b"premium_info")]])
        return
    target_id = data["temp_data"].pop("gift_target", None)
    if not target_id:
        await event.respond("❌ Session expire ho gaya। Phir try karo।")
        return
    await give_premium(target_id, days, given_by=uid)
    record_premium_history(uid, days, given_by=uid, note=f"Gifted to {target_id}")
    add_log(uid, "Gift Premium", target=target_id, details=f"{days}d")
    exp_txt = "Lifetime" if days == 0 else f"{days} days"
    await event.respond(
        f"🎁 **Premium Gift Bhej Diya!**\n\n"
        f"To: `{target_id}`\nDuration: `{exp_txt}`\n\n"
        "Unhe notification aa gayi hogi!",
        buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]]
    )


# ── Redeem gift code ──────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_redeem_gift"))
async def prem_redeem_gift(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"]       = "prem_redeem_input"
    data["step_since"] = int(time.time())
    try:
        await event.edit(
            "🎁 **REDEEM GIFT / PROMO CODE**\n\n"
            "Admin ya kisi ne bheja code type karo:",
            buttons=[[Button.inline("❌ Cancel", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "prem_redeem_input"))
async def prem_redeem_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    code  = event.raw_text.strip().upper()
    entry = validate_promo_code(code)
    if not entry:
        await event.respond(
            f"❌ Code `{code}` valid nahi hai ya expire ho gaya।",
            buttons=[[Button.inline("🔙 Back", b"premium_info")]]
        )
        return
    days    = entry.get("bonus_days", 0)
    disc    = entry.get("discount_pct", 0)
    use_promo_code(code)
    if days > 0:
        await give_premium(uid, days, given_by=0)
        record_premium_history(uid, days, note=f"Promo code {code}")
        await event.respond(
            f"✅ **Code Redeemed!**\n\n🎁 `{days}` days premium add ho gaya!",
            buttons=[[Button.inline("💎 Status", b"premium_info")]]
        )
    elif disc > 0:
        data["temp_data"]["promo_discount"] = disc
        data["temp_data"]["promo_code"]     = code
        await event.respond(
            f"✅ **Code Valid!**\n\n🏷️ `{disc}%` discount milega next purchase pe!\n"
            "Ab plan select karo:",
            buttons=[
                [Button.inline("💎 Buy Now (with discount)", b"buy_premium")],
                [Button.inline("🔙 Back",                    b"premium_info")],
            ]
        )
    else:
        await event.respond("❌ Code mein koi value nahi thi।")


# ══════════════════════════════════════════════════════════════
# 🏷️ ADMIN — PROMO CODE MANAGEMENT
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_promo_menu"))
async def adm_promo_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    config = get_premium_config()
    codes  = config.get("promo_codes", {})

    lines = []
    now   = int(time.time())
    for code, entry in codes.items():
        uses   = entry.get("uses_left", 0)
        disc   = entry.get("discount_pct", 0)
        days   = entry.get("bonus_days", 0)
        exp    = entry.get("expires_at")
        exp_s  = datetime.datetime.fromtimestamp(exp).strftime("%d/%m/%y") if exp else "No expiry"
        val    = f"{days}d free" if days else f"{disc}% off"
        lines.append(f"  `{code}` — {val} | {uses} uses left | Exp: {exp_s}")

    body = "\n".join(lines) if lines else "  _(koi promo code nahi)_"
    try:
        await event.edit(
            "🏷️ **PROMO CODE MANAGER**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**Active Codes ({len(codes)}):**\n{body}",
            buttons=[
                [Button.inline("➕ New Bonus Days Code",    b"adm_promo_new_days"),
                 Button.inline("➕ New Discount Code",      b"adm_promo_new_disc")],
                [Button.inline("🗑 Delete a Code",          b"adm_promo_del_menu")],
                [Button.inline("🔙 Premium Panel",          b"adm_premium")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_promo_new_days"))
async def adm_promo_new_days(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_promo_input_days"
    data["step_since"] = int(time.time())
    await event.edit(
        "🏷️ **NEW BONUS DAYS CODE**\n\n"
        "Format bhejo:\n`CODE DAYS USES`\n\n"
        "Example: `WELCOME30 30 100`\n"
        "_(CODE = 2-10 chars, DAYS = kitne din, USES = kitne log use kar sakte hain)_",
        buttons=[[Button.inline("❌ Cancel", b"adm_promo_menu")]]
    )


@bot.on(events.CallbackQuery(data=b"adm_promo_new_disc"))
async def adm_promo_new_disc(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_promo_input_disc"
    data["step_since"] = int(time.time())
    await event.edit(
        "🏷️ **NEW DISCOUNT CODE**\n\n"
        "Format bhejo:\n`CODE PERCENT USES`\n\n"
        "Example: `SAVE20 20 50`\n"
        "_(PERCENT = 1-99, USES = kitne log use kar sakte hain)_",
        buttons=[[Button.inline("❌ Cancel", b"adm_promo_menu")]]
    )


@bot.on(events.CallbackQuery(data=b"adm_promo_del_menu"))
async def adm_promo_del_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    config = get_premium_config()
    codes  = config.get("promo_codes", {})
    if not codes:
        return await event.answer("Koi code nahi hai!", alert=True)
    btns = []
    for code in codes:
        btns.append([Button.inline(f"🗑 {code}", f"adm_promo_del|{code}".encode())])
    btns.append([Button.inline("🔙 Back", b"adm_promo_menu")])
    try:
        await event.edit("🗑 Kaunsa code delete karna hai?", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_promo_del\\|(.+)"))
async def adm_promo_del(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    code   = event.data.decode().split("|")[1]
    config = get_premium_config()
    config.get("promo_codes", {}).pop(code, None)
    save_persistent_db()
    await event.answer(f"🗑 `{code}` deleted!", alert=False)
    await adm_promo_menu(event)


# ══════════════════════════════════════════════════════════════
# 👥 ADMIN — BULK PREMIUM
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_bulk_prem"))
async def adm_bulk_prem(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_bulk_prem_input"
    data["step_since"] = int(time.time())
    try:
        await event.edit(
            "👥 **BULK GIVE PREMIUM**\n\n"
            "Multiple user IDs ek saath premium do.\n\n"
            "Format:\n`DAYS\nUID1\nUID2\nUID3`\n\n"
            "Example:\n`30\n123456789\n987654321\n111222333`\n\n"
            "_(First line = days, phir har line mein ek User ID)_",
            buttons=[[Button.inline("❌ Cancel", b"adm_premium")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════════════════════════
# 🔗 REFERRAL PROGRAM UI
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"prem_referral"))
async def prem_referral_menu(event):
    await event.answer()
    uid    = event.sender_id
    config = get_premium_config()

    if not config.get("referral_enabled", False):
        try:
            await event.edit(
                "🔗 **REFERRAL PROGRAM**\n\n"
                "❌ Referral program abhi enabled nahi hai.\n"
                "Admin se contact karo.",
                buttons=[[Button.inline("🔙 Back", b"premium_info")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    ref_code  = get_referral_code(uid)
    data      = get_user_data(uid)
    ref_count = data.get("referral_count", 0)
    bonus_days = config.get("referral_bonus_days", 7)

    try:
        from config import bot as _bot
        bot_me = await _bot.get_me()
        bot_username = bot_me.username or "yourbot"
        ref_link = f"https://t.me/{bot_username}?start=ref_{ref_code}"
    except Exception:
        ref_link = f"Your code: `{ref_code}`"

    try:
        await event.edit(
            "🔗 **REFERRAL PROGRAM**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Doston ko refer karo — dono ko **{bonus_days} days** free!\n\n"
            f"**Tumhara Referral Code:** `{ref_code}`\n"
            f"**Referral Link:**\n`{ref_link}`\n\n"
            f"👥 **Successful Referrals:** `{ref_count}`\n"
            f"🎁 **Bonus Earned:** `{ref_count * bonus_days} days`\n\n"
            "_Jab koi tera code use karke premium le, dono ko bonus milega!_",
            buttons=[
                [Button.inline("📋 Copy Code", f"copy_ref_{ref_code}".encode())],
                [Button.inline("🔙 Back",       b"premium_info")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Admin: Referral Settings ─────────────────────────────────
@bot.on(events.CallbackQuery(data=b"adm_referral_settings"))
async def adm_referral_settings(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    config     = get_premium_config()
    ref_on     = config.get("referral_enabled", False)
    bonus_days = config.get("referral_bonus_days", 7)
    tog_lbl    = "🔴 Disable Referral" if ref_on else "🟢 Enable Referral"
    try:
        await event.edit(
            "🔗 **REFERRAL PROGRAM SETTINGS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Status: {'🟢 Enabled' if ref_on else '🔴 Disabled'}\n"
            f"Bonus per referral: **{bonus_days} days**\n\n"
            "Referrer + new user dono ko bonus milta hai\n"
            "jab new user premium purchase karta hai।",
            buttons=[
                [Button.inline(tog_lbl,                    b"adm_ref_toggle")],
                [Button.inline("📅 Change Bonus Days",     b"adm_ref_bonus_days")],
                [Button.inline("🔙 Premium Panel",         b"adm_premium")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_ref_toggle"))
async def adm_ref_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    config = get_premium_config()
    config["referral_enabled"] = not config.get("referral_enabled", False)
    save_persistent_db()
    status = "🟢 Enabled" if config["referral_enabled"] else "🔴 Disabled"
    await event.answer(f"Referral → {status}", alert=False)
    await adm_referral_settings(event)


@bot.on(events.CallbackQuery(data=b"adm_ref_bonus_days"))
async def adm_ref_bonus_days(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_ref_bonus_input"
    data["step_since"] = int(time.time())
    await event.edit(
        "📅 Referral bonus kitne days ka ho?\n_(e.g. `7`)_",
        buttons=[[Button.inline("❌ Cancel", b"adm_referral_settings")]]
    )


# ── Premium user referral UI ─────────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_referral"))
async def prem_referral_ui(event):
    await event.answer()
    # Handled above in new handlers section — this is a duplicate guard
    pass


# ══════════════════════════════════════════════════════════════
# 💎 PREMIUM v3 — Advanced Features
# ══════════════════════════════════════════════════════════════

# ── Subscription Pause / Freeze ───────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_pause"))
async def prem_pause(event):
    await event.answer()
    uid  = event.sender_id
    if not is_premium_user(uid):
        return await event.answer("Premium nahi hai!", alert=True)
    prem = get_user_premium(uid)
    if prem.get("paused"):
        # Already paused — show resume option
        paused_at  = prem.get("paused_at", 0)
        saved_days = prem.get("saved_days", 0)
        paused_str = datetime.datetime.fromtimestamp(paused_at).strftime("%d %b %Y")
        try:
            await event.edit(
                "⏸️ **SUBSCRIPTION PAUSED**\n\n"
                f"Paused on: `{paused_str}`\n"
                f"Days saved: `{saved_days}`\n\n"
                "Resume karne par remaining days wapas aayenge।",
                buttons=[
                    [Button.inline("▶️ Resume Subscription", b"prem_resume")],
                    [Button.inline("🔙 Back",                b"premium_info")],
                ]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    exp = prem.get("expires_at")
    if exp is None:
        return await event.answer("Lifetime premium pause nahi hota!", alert=True)

    days_left = max(0, int((exp - time.time()) / 86400))
    try:
        await event.edit(
            "⏸️ **PAUSE SUBSCRIPTION**\n\n"
            f"Remaining days: **{days_left}**\n\n"
            "Pause karne par:\n"
            "• Premium features band ho jaayengi\n"
            "• Remaining days save ho jaayenge\n"
            "• Resume karne par sab wapas milega\n\n"
            "Confirm?",
            buttons=[
                [Button.inline("⏸️ Haan, Pause Karo", b"prem_pause_confirm"),
                 Button.inline("❌ Cancel",            b"premium_info")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"prem_pause_confirm"))
async def prem_pause_confirm(event):
    await event.answer()
    uid  = event.sender_id
    prem = get_user_premium(uid)
    exp  = prem.get("expires_at")
    if not exp:
        return await event.answer("Lifetime premium pause nahi hota!", alert=True)
    days_left = max(0, int((exp - time.time()) / 86400))
    prem["paused"]     = True
    prem["paused_at"]  = int(time.time())
    prem["saved_days"] = days_left
    prem["active"]     = False
    prem["expires_at"] = None  # Temporarily unset
    save_persistent_db()
    add_log(0, "Pause", target=uid, details=f"{days_left}d saved")
    try:
        await event.edit(
            f"⏸️ **Subscription Paused!**\n\n"
            f"`{days_left} days` save ho gaye।\n"
            "Resume karne par premium wapas activate hoga।",
            buttons=[
                [Button.inline("▶️ Resume Later", b"prem_resume")],
                [Button.inline("🏠 Main Menu",    b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"prem_resume"))
async def prem_resume(event):
    await event.answer()
    uid  = event.sender_id
    prem = get_user_premium(uid)
    if not prem.get("paused"):
        return await event.answer("Subscription paused nahi hai!", alert=True)
    saved_days = prem.get("saved_days", 0)
    prem["paused"]     = False
    prem["active"]     = True
    prem["expires_at"] = int(time.time()) + (saved_days * 86400)
    prem.pop("paused_at", None)
    prem.pop("saved_days", None)
    save_persistent_db()
    add_log(0, "Resume", target=uid, details=f"{saved_days}d restored")
    await event.answer(f"▶️ Resumed! {saved_days} days restored!", alert=False)
    try:
        await event.edit(
            f"▶️ **Subscription Resumed!**\n\n"
            f"`{saved_days} days` restored ho gaye।\n"
            "Premium features ab active hain!",
            buttons=[[Button.inline("💎 Premium Status", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Premium Transfer A→B ──────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_transfer"))
async def prem_transfer(event):
    await event.answer()
    uid = event.sender_id
    if not is_premium_user(uid):
        return await event.answer("Premium nahi hai!", alert=True)
    prem      = get_user_premium(uid)
    exp       = prem.get("expires_at")
    days_left = "Lifetime" if not exp else str(max(0, int((exp - time.time()) / 86400))) + " days"
    data      = get_user_data(uid)
    data["step"]       = "prem_transfer_input"
    data["step_since"] = int(time.time())
    try:
        await event.edit(
            "🔁 **TRANSFER PREMIUM**\n\n"
            f"Your remaining: **{days_left}**\n\n"
            "⚠️ Transfer karne par:\n"
            "• Tumhara premium **remove** ho jaayega\n"
            "• Recipient ko **sab remaining days** milenge\n\n"
            "Recipient ka **User ID** type karo:",
            buttons=[[Button.inline("❌ Cancel", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "prem_transfer_input"))
async def prem_transfer_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        target_id = int(event.raw_text.strip())
    except ValueError:
        await event.respond("❌ Valid User ID bhejo।",
                            buttons=[[Button.inline("🔙 Back", b"premium_info")]])
        return
    if target_id == uid:
        await event.respond("❌ Khud ko transfer nahi kar sakte!",
                            buttons=[[Button.inline("🔙 Back", b"premium_info")]])
        return
    prem      = get_user_premium(uid)
    exp       = prem.get("expires_at")
    days_left = 0 if not exp else max(0, int((exp - time.time()) / 86400))
    is_life   = exp is None and prem.get("active")

    # Remove from sender
    prem["active"]     = False
    prem["expires_at"] = None
    save_persistent_db()

    # Give to recipient
    await give_premium(target_id, 0 if is_life else days_left, given_by=uid)
    record_premium_history(uid, 0, note=f"Transferred to {target_id}")
    record_premium_history(target_id, days_left, given_by=uid, note=f"Received from {uid}")
    add_log(uid, "Transfer Premium", target=target_id,
            details=f"{'Lifetime' if is_life else days_left}")

    dur = "Lifetime" if is_life else f"{days_left} days"
    await event.respond(
        f"✅ **Transfer Complete!**\n\n"
        f"**{dur}** transferred to `{target_id}`\n"
        "Tumhara premium remove ho gaya।",
        buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]]
    )


# ── Renewal reminder preference ───────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_reminder_pref"))
async def prem_reminder_pref(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    cur  = data.get("renewal_reminder_days", 3)
    try:
        await event.edit(
            "🔔 **RENEWAL REMINDER**\n\n"
            f"Current: **{cur} days** before expiry\n\n"
            "Kitne din pehle reminder chahiye?",
            buttons=[
                [Button.inline("1 day",  b"prem_remind_1"),
                 Button.inline("3 days", b"prem_remind_3"),
                 Button.inline("7 days", b"prem_remind_7")],
                [Button.inline("🔕 Disable", b"prem_remind_0")],
                [Button.inline("🔙 Back",    b"premium_info")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"prem_remind_(.+)"))
async def prem_remind_set(event):
    await event.answer()
    days = int(event.data.decode().replace("prem_remind_", ""))
    uid  = event.sender_id
    data = get_user_data(uid)
    data["renewal_reminder_days"] = days
    save_persistent_db()
    msg = "🔕 Reminders disabled" if days == 0 else f"🔔 Reminder set: {days} days before expiry"
    await event.answer(msg, alert=False)
    try:
        await event.edit(
            f"✅ **{msg}**",
            buttons=[[Button.inline("🔙 Back", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Feature usage analytics ───────────────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_feat_analytics"))
async def prem_feat_analytics(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data.get("settings", {})

    # Collect active premium features
    active_paid = []
    active_free = []
    inactive    = []
    for key, (name, cat, is_paid) in ALL_FEATURES.items():
        if s.get(key):
            if is_paid:
                active_paid.append(f"  💎 {name}")
            else:
                active_free.append(f"  🆓 {name}")
        else:
            inactive.append(name)

    lines = []
    if active_paid:
        lines.append("**💎 Active Premium Features:**")
        lines.extend(active_paid)
    if active_free:
        lines.append("\n**🆓 Active Free Features:**")
        lines.extend(active_free)
    if inactive:
        lines.append(f"\n**⚪ Inactive ({len(inactive)}):**")
        lines.append("  " + ", ".join(inactive[:6]))

    body = "\n".join(lines) if lines else "_Koi feature active nahi_"
    try:
        await event.edit(
            "📊 **YOUR FEATURE ANALYTICS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + body,
            buttons=[[Button.inline("🔙 Back", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Admin: Premium leaderboard/stats ─────────────────────────
@bot.on(events.CallbackQuery(data=b"prem_leaderboard"))
async def prem_leaderboard(event):
    await event.answer()
    if not is_admin(event.sender_id): return

    now    = int(time.time())
    active = []
    expiring_soon = []
    lifetime_count = 0

    for uid_key, udata in list(db.items()):
        if not isinstance(udata, dict): continue
        prem = udata.get("premium", {})
        if not prem.get("active"): continue
        exp = prem.get("expires_at")
        if exp is None:
            lifetime_count += 1
            active.append((uid_key, 9999999))
        else:
            days = max(0, int((exp - now) / 86400))
            active.append((uid_key, days))
            if days <= 7:
                expiring_soon.append((uid_key, days))

    active.sort(key=lambda x: x[1], reverse=True)
    expiring_soon.sort(key=lambda x: x[1])

    lines = [f"👥 **Total Premium:** {len(active)}"]
    lines.append(f"♾️ Lifetime: {lifetime_count}")
    lines.append(f"⚠️ Expiring ≤7 days: {len(expiring_soon)}")
    lines.append("")
    lines.append("**⚠️ Expiring Soon:**")
    for uid_key, days in expiring_soon[:5]:
        lines.append(f"  `{uid_key}` — {days}d left")

    try:
        await event.edit(
            "📊 **PREMIUM STATS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n".join(lines),
            buttons=[
                [Button.inline("🔄 Refresh",    b"prem_leaderboard")],
                [Button.inline("🔙 Admin Panel", b"adm_main")],
            ]
        )
    except errors.MessageNotModifiedError:
        await event.answer("Already up to date", alert=False)


# ── Auto-extend patch removed — logic merged directly into give_premium above ──


# ── Referral: track referred_by + apply bonus on purchase ─────
def apply_referral_on_purchase(buyer_uid: int, days_purchased: int):
    """Agar buyer ne referral code use kiya tha, referrer ko bonus do."""
    config      = get_premium_config()
    if not config.get("referral_enabled", False):
        return
    bonus_days  = config.get("referral_bonus_days", 7)
    buyer_data  = get_user_data(buyer_uid)
    referrer_id = buyer_data.get("referred_by")
    if not referrer_id:
        return
    # Referrer ko bonus
    import asyncio
    loop = None
    try:
        loop = asyncio.get_event_loop()
    except Exception:
        pass
    if loop and loop.is_running():
        asyncio.ensure_future(
            give_premium(referrer_id, bonus_days, given_by=0)
        )
    # Buyer ko bhi bonus (first purchase only)
    if not buyer_data.get("referral_bonus_given"):
        buyer_data["referral_bonus_given"] = True
        if loop and loop.is_running():
            asyncio.ensure_future(
                give_premium(buyer_uid, bonus_days, given_by=0)
            )
    # Track referral count
    ref_data = get_user_data(referrer_id)
    ref_data["referral_count"] = ref_data.get("referral_count", 0) + 1
    save_persistent_db()


# ══════════════════════════════════════════════════════════════
# 💎 PREMIUM v4 — Admin Extend + Background Tasks
# ══════════════════════════════════════════════════════════════

# ── Admin: extend/modify specific user expiry ─────────────────
@bot.on(events.CallbackQuery(data=b"adm_extend_user"))
async def adm_extend_user(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_extend_user_input"
    data["step_since"] = int(time.time())
    try:
        await event.edit(
            "✏️ **EXTEND / MODIFY USER EXPIRY**\n\n"
            "Format:\n`USER_ID DAYS`\n\n"
            "Examples:\n"
            "`123456 30` — 30 din add karo\n"
            "`123456 -7` — 7 din hatao\n"
            "`123456 0` — Lifetime karo\n"
            "`123456 reset` — Remove premium",
            buttons=[[Button.inline("❌ Cancel", b"adm_prem_users")]]
        )
    except errors.MessageNotModifiedError:
        pass


# Input handled in handle_premium_inputs via new step
# Wire it in handle_premium_inputs:
_old_handle_end = None  # placeholder — handled below in patched handle


# ── Background expiry check task ──────────────────────────────
async def _run_expiry_check():
    """
    Har 6 ghante: sabhi users ke premium expire check karo.
    Reminder notifications bhejo based on user preferences.
    """
    while True:
        try:
            await asyncio.sleep(6 * 3600)  # 6 ghante
            now = int(time.time())
            for uid_key, udata in list(db.items()):
                if not isinstance(udata, dict): continue
                prem = udata.get("premium", {})
                if not prem.get("active"): continue
                exp = prem.get("expires_at")
                if not exp: continue
                days_left = max(0, int((exp - now) / 86400))
                # Check user's reminder preference
                reminder_days = udata.get("renewal_reminder_days", 3)
                if reminder_days == 0: continue
                # Notify only once per threshold
                last_notif = prem.get("_last_expiry_notif", 0)
                if now - last_notif < 86400: continue  # Already notified today
                if days_left <= reminder_days:
                    prem["_last_expiry_notif"] = now
                    save_persistent_db()
                    try:
                        uid_int = int(uid_key)
                        await _notify_expiry(uid_int)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            break
        except Exception:
            pass


def start_expiry_check_task():
    """Call this from main.py on bot start."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop.create_task(_run_expiry_check())
    except Exception:
        pass