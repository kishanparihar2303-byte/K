# bot/msg_limit.py
import time
import datetime
import pytz

def _get_user_tz(user_id=None):
    """FIX 12: User timezone or fallback to IST."""
    if user_id:
        try:
            from database import get_user_data
            tz_name = get_user_data(user_id).get("timezone", "Asia/Kolkata")
            return pytz.timezone(tz_name)
        except Exception:
            pass
    return pytz.timezone("Asia/Kolkata")
from telethon import events, Button, errors
from config import bot, OWNER_ID
from database import db, GLOBAL_STATE, get_user_data, save_persistent_db
from admin import is_admin

IST = pytz.timezone("Asia/Kolkata")

def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_limit_config():
    GLOBAL_STATE.setdefault("msg_limit_config", {
        "enabled": True,
        "daily_limit": 500,
        "monthly_limit": 10000,
    })
    return GLOBAL_STATE["msg_limit_config"]

# BUG 46 FIX: Server local time ki jagah IST use karo
def get_today_key(user_id=None):
    """FIX 12: User-specific date key."""
    return datetime.datetime.now(_get_user_tz(user_id)).strftime("%Y-%m-%d")

def get_month_key():
    return datetime.datetime.now(_get_user_tz()).strftime("%Y-%m")

def get_msg_count(user_id: int) -> dict:
    data = get_user_data(user_id)
    data.setdefault("msg_counts", {
        "daily": {},
        "monthly": {},
    })
    return data["msg_counts"]

def increment_msg_count(user_id: int):
    counts = get_msg_count(user_id)
    today = get_today_key(user_id)   # BUG FIX: user_id pass karo correct timezone ke liye
    month = get_month_key()
    counts["daily"][today] = counts["daily"].get(today, 0) + 1
    counts["monthly"][month] = counts["monthly"].get(month, 0) + 1
    # FIX 9: dict insertion order (Python 3.7+) — no sorted() needed
    if len(counts["daily"]) > 7:
        to_del = list(counts["daily"].keys())[:-7]  # oldest first
        for k in to_del:
            del counts["daily"][k]
    if len(counts["monthly"]) > 3:
        to_del = list(counts["monthly"].keys())[:-3]
        for k in to_del:
            del counts["monthly"][k]

def can_forward(user_id: int) -> tuple:
    from premium import is_premium_user
    config = get_limit_config()
    if not config.get("enabled"):
        return True, ""
    if is_premium_user(user_id):
        return True, ""
    counts = get_msg_count(user_id)
    today = get_today_key(user_id)   # BUG FIX: user_id pass karo correct timezone ke liye
    month = get_month_key()
    daily_count = counts["daily"].get(today, 0)
    monthly_count = counts["monthly"].get(month, 0)
    daily_limit = config.get("daily_limit", 500)
    monthly_limit = config.get("monthly_limit", 10000)
    if daily_count >= daily_limit:
        return False, (
            f"📵 **Daily Limit Khatam Ho Gayi!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 Aaj: `{daily_count}/{daily_limit}` messages forward hue\n"
            f"⏰ Reset: Aaj **midnight IST** par\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "💎 **Premium mein kya milega?**\n\n"
            "✅ **Unlimited** daily forwarding\n"
            "✅ **Unlimited** monthly forwarding\n"
            "✅ Smart duplicate filter\n"
            "✅ Custom delay control\n"
            "✅ Smart AI message filter\n"
            "✅ Priority support\n\n"
            + _get_owner_footer()
        )
    if monthly_count >= monthly_limit:
        return False, (
            f"📵 **Monthly Limit Khatam Ho Gayi!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 Is mahine: `{monthly_count}/{monthly_limit}` messages forward hue\n"
            f"⏰ Reset: **Agli mahine** pehle din\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "💎 **Premium lo — Kabhi limit nahi aayegi!**\n\n"
            "✅ Unlimited daily + monthly forwarding\n"
            "✅ Duplicate filter, Smart filter, Auto delay\n"
            "✅ 10+ sources, 10+ destinations\n\n"
            + _get_owner_footer()
        )
    return True, ""

# ─── DAILY LIMIT WARNING TRACKER ────────────────────────────────────────────
# Issue #15: 80% limit pe warning bhejo — ek baar per day
_warned_80pct: set = set()

def check_limit_warning(user_id: int):
    """80% limit pe ek baar warning bhejo. Returns warning text or ''."""
    config = get_limit_config()
    if not config.get("enabled"):
        return ""
    try:
        from premium import is_premium_user
        if is_premium_user(user_id):
            return ""
    except Exception:
        pass

    counts  = get_msg_count(user_id)
    today   = get_today_key(user_id)
    daily   = counts["daily"].get(today, 0)
    limit   = config.get("daily_limit", 500)
    key     = (user_id, today)

    pct = (daily / limit * 100) if limit > 0 else 0
    if pct >= 80 and key not in _warned_80pct:
        _warned_80pct.add(key)
        remaining = limit - daily
        return (
            f"⚠️ **80% Daily Limit Use Ho Gayi!**\n\n"
            f"📊 Aaj: `{daily}/{limit}` messages forwarded\n"
            f"🔢 Bache hue: **{remaining} messages**\n\n"
            f"Limit khatam hone par forwarding **band ho jaayegi** aaj ke liye.\n\n"
            f"💎 **Premium lo — Unlimited forwarding!**\n"
        )
    return ""


# ─── BASIC DUPLICATE FILTER FOR FREE USERS ───────────────────────────────────
# Issue #19: Message ID based basic dup filter — free users ke liye bhi
_seen_msg_ids: dict = {}   # user_id → set of (chat_id, msg_id)
_SEEN_MAX = 500            # Per user max remembered IDs

def is_basic_duplicate(user_id: int, chat_id, msg_id) -> bool:
    """Basic free-user duplicate check by message ID. Returns True if duplicate."""
    key = (int(chat_id), int(msg_id))
    user_seen = _seen_msg_ids.setdefault(user_id, set())
    if key in user_seen:
        return True
    user_seen.add(key)
    # Memory limit — FIFO eviction (convert to list, trim, convert back)
    if len(user_seen) > _SEEN_MAX:
        items = list(user_seen)
        _seen_msg_ids[user_id] = set(items[-_SEEN_MAX:])
    return False

def clear_basic_dup_cache(user_id: int):
    """Clear dup cache on session restart."""
    _seen_msg_ids.pop(user_id, None)


async def adm_msg_limits(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    config = get_limit_config()
    txt = (
        "📨 **Message Limit Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"⚙️ Status: {'✅ ON' if config.get('enabled') else '❌ OFF'}\n"
        f"📆 Daily Limit: `{config['daily_limit']}` msgs\n"
        f"📅 Monthly Limit: `{config['monthly_limit']}` msgs\n\n"
        "Free users ke liye apply hota hai।\n"
        "Premium users unlimited hain।\n"
        "⏰ Reset: IST midnight par (Indian Standard Time)\n\n" + _get_owner_footer()
    )
    btns = [
        [Button.inline(
            "🔴 Disable" if config.get("enabled") else "🟢 Enable",
            b"adm_msglimit_toggle"
        )],
        [Button.inline("📆 Set Daily Limit", b"adm_set_daily_limit"),
         Button.inline("📅 Set Monthly Limit", b"adm_set_monthly_limit")],
        [Button.inline("🔙 Back", b"adm_main")]
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_msglimit_toggle"))
async def adm_msglimit_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    config = get_limit_config()
    config["enabled"] = not config.get("enabled", True)
    save_persistent_db()
    await event.answer(f"Message Limit {'ON' if config['enabled'] else 'OFF'}!")
    await adm_msg_limits(event)

@bot.on(events.CallbackQuery(data=b"adm_set_daily_limit"))
async def adm_set_daily_limit_cb(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_daily_limit_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "📆 **Daily Limit Set Karo**\n\nNumber bhejo (e.g., `500`):",
        buttons=[Button.inline("🔙 Cancel", b"adm_msg_limits")]
    )

@bot.on(events.CallbackQuery(data=b"adm_set_monthly_limit"))
async def adm_set_monthly_limit_cb(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_monthly_limit_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "📅 **Monthly Limit Set Karo**\n\nNumber bhejo (e.g., `10000`):",
        buttons=[Button.inline("🔙 Cancel", b"adm_msg_limits")]
    )

async def handle_limit_inputs(event, user_id: int, step: str) -> bool:
    config = get_limit_config()
    if step == "adm_daily_limit_input":
        try:
            limit = int(event.text.strip())
            config["daily_limit"] = limit
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(
                f"✅ Daily limit: `{limit}` messages",
                buttons=[Button.inline("🔙 Back", b"adm_msg_limits")]
            )
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True
    elif step == "adm_monthly_limit_input":
        try:
            limit = int(event.text.strip())
            config["monthly_limit"] = limit
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(
                f"✅ Monthly limit: `{limit}` messages",
                buttons=[Button.inline("🔙 Back", b"adm_msg_limits")]
            )
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True
    return False
