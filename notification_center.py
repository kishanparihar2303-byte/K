from time_helper import ab_fmt as _ab_fmt, ab_now as _ab_now
"""
notification_center.py — Unified Notification Control Center v2.0

ALL notifications ek jagah se:
  - Kahin bhi bhejo: Bot DM / Channel / Group
  - Per-notification alag destination
  - "Bot Wapas Online" fix — ab channel/group mein bhi jaata hai
  - New user notification fix — jo band ho gaya tha
  - Bot Owner naam: change / rename / hide / delete
  - Master on/off + per-notification on/off
  - Smart cooldown — duplicate alerts nahi
  - Live stats — kab gaya, kahan gaya
"""

import asyncio
import logging
import time
import datetime
from typing import Optional
from config import bot, OWNER_ID
from database import GLOBAL_STATE, db, save_persistent_db

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
# CATALOG — Har notification ki definition
# ═══════════════════════════════════════════════════════

NOTIFICATION_CATALOG = {
    # KEY: (label, description, category, default_on, cooldown_sec, who_receives)
    # who_receives: "admin" = sirf admins, "active_users" = forwarding-on users, "user" = specific user

    # ── SYSTEM ────────────────────────────────────────
    "bot_online":      ("✅ Bot Online Alert",         "Bot restart/wapas online",           "system",  True,  300,   "admin"),
    "bot_offline":     ("🔴 Bot Offline Alert",        "Bot band hone se pehle",              "system",  True,  300,   "admin"),
    "ram_high":        ("🔴 RAM High Alert",           "RAM 85%+ ho jaaye",                   "system",  True,  1800,  "admin"),
    "worker_dead":     ("💀 Worker Down",              "Background worker crash",              "system",  True,  600,   "admin"),
    "forward_errors":  ("⚠️ Forward Errors",          "Kisi user ke 5+ errors",              "system",  True,  3600,  "admin"),

    # ── USERS ─────────────────────────────────────────
    "new_user":        ("👤 Naya User",                "Koi naya /start kare",                "users",   True,  0,     "admin"),
    "new_premium":     ("💎 Naya Premium User",        "Kisi ko premium mile",                "users",   True,  0,     "admin"),

    # ── USER → USER ALERTS ────────────────────────────
    "user_bot_online": ("📣 Users: Bot Wapas Online",  "Active users ko bhi batao",           "users",   True,  300,   "active_users"),
    "user_session":    ("⚠️ Users: Session Expire",   "User ka session khatam ho",           "users",   True,  0,     "user"),
    "user_premium_exp":("⏰ Users: Premium Warning",   "Premium 3 din mein khatam",           "users",   True,  86400, "user"),
    "user_limit":      ("📊 Users: Daily Limit 80%",  "80% daily limit use ho",              "users",   True,  3600,  "user"),
    "user_not_admin":  ("🔐 Users: Admin Missing",    "Destination mein admin nahi",          "users",   True,  3600,  "user"),
    "user_paused":     ("🛑 Users: Auto-Pause",       "Too many errors — auto-pause",         "users",   True,  0,     "user"),

    # ── PAYMENT ───────────────────────────────────────
    "payment":         ("💰 Payment Screenshot",       "User ne payment bheja",               "payment", True,  0,     "admin"),
    "fraud":           ("🚨 Fraud Payment",            "Fake/duplicate payment pakdi",        "payment", True,  0,     "admin"),

    # ── REPORTS ───────────────────────────────────────
    "daily_summary":   ("📊 Daily Summary",            "Roz raat 12 baje stats",              "reports", True,  0,     "admin"),
}

CATEGORIES = {
    "system":  "🖥 System Alerts",
    "users":   "👥 User Events",
    "payment": "💰 Payment Alerts",
    "reports": "📊 Reports",
}


# ═══════════════════════════════════════════════════════
# CONFIG STORE
# ═══════════════════════════════════════════════════════

def _nc() -> dict:
    """FIX 20: Notification Center config with migration."""
    nc = GLOBAL_STATE.get("nc")
    if nc is None:
        GLOBAL_STATE["nc"] = {
            "master":       True,
            "owner_name":   "",
            "owner_show":   True,
            "global_dest":  None,
            "per_dest":     {},
            "enabled":      {k: v[3] for k, v in NOTIFICATION_CATALOG.items()},
            "cooldown":     {},
            "stats":        {},
        }
        nc = GLOBAL_STATE["nc"]
    # Migration: add any new keys
    for k, v in NOTIFICATION_CATALOG.items():
        nc["enabled"].setdefault(k, v[3])
    return nc


def is_on(key: str) -> bool:
    nc = _nc()
    return nc.get("master", True) and nc["enabled"].get(key, True)


def _cooldown_ok(key: str) -> bool:
    cooldown = NOTIFICATION_CATALOG.get(key, ("", "", "", True, 0, "admin"))[4]
    if cooldown == 0:
        return True
    last = _nc().get("cooldown", {}).get(key, 0)
    return (time.time() - last) >= cooldown


_NC_LAST_SAVE: float = 0.0

def _mark_sent(key: str, dest):
    global _NC_LAST_SAVE
    nc_data = _nc()
    now = time.time()
    nc_data.setdefault("cooldown", {})[key] = now
    nc_data.setdefault("stats", {}).setdefault(key, {"count": 0, "last_ts": 0, "last_dest": ""})
    nc_data["stats"][key]["count"]    += 1
    nc_data["stats"][key]["last_ts"]   = now
    nc_data["stats"][key]["last_dest"] = str(dest)
    # FIX: Debounced save — max once per 30s (cooldowns need persistence for restart safety)
    if now - _NC_LAST_SAVE > 30:
        _NC_LAST_SAVE = now
        try:
            save_persistent_db()
        except Exception:
            pass
    # FIX 7: Persist stats (not cooldowns — those can reset, it's fine)
    # Save debounced — don't spam disk on high-traffic bots
    try:
        from database import save_persistent_db
        save_persistent_db()
    except Exception:
        pass


def _dest(key: str) -> Optional[int]:
    """Destination priority: per-key > global > None(=admins)."""
    nc = _nc()
    per = nc.get("per_dest", {}).get(key)
    if per:
        return int(per)
    glb = nc.get("global_dest")
    if glb:
        return int(glb)
    return None


def _footer() -> str:
    nc = _nc()
    if not nc.get("owner_show", True):
        return ""
    name = nc.get("owner_name", "")
    return f"\n\n👤 Bot Owner: {name}" if name else ""


# ═══════════════════════════════════════════════════════
# CORE SEND
# ═══════════════════════════════════════════════════════

async def _send(key: str, text: str,
                force_uid: Optional[int] = None,
                who: str = "admin"):
    if not is_on(key):
        return
    if not _cooldown_ok(key):
        return

    msg = text + _footer()

    try:
        if who == "user" and force_uid:
            await bot.send_message(int(force_uid), msg, parse_mode="md")
            _mark_sent(key, force_uid)

        elif who == "active_users":
            dest = _dest(key)
            if dest:
                # Custom channel/group mein ek message
                await bot.send_message(dest, msg, parse_mode="md")
                _mark_sent(key, dest)
            else:
                # Har active user ko seedha
                count = 0
                for uid, udata in list(db.items()):
                    if isinstance(udata, dict) and udata.get("settings", {}).get("running"):
                        try:
                            await bot.send_message(int(uid), msg, parse_mode="md")
                            count += 1
                            await asyncio.sleep(0.3)
                        except Exception:
                            pass
                _mark_sent(key, f"active_users({count})")

        else:  # admin
            dest = _dest(key)
            if dest:
                await bot.send_message(dest, msg, parse_mode="md")
                _mark_sent(key, dest)
            else:
                admin_ids = list(GLOBAL_STATE.get("admins", {}).keys())
                if OWNER_ID not in admin_ids:
                    admin_ids.append(OWNER_ID)
                for aid in admin_ids:
                    try:
                        await bot.send_message(aid, msg, parse_mode="md")
                        await asyncio.sleep(0.2)
                    except Exception as e:
                        logger.debug(f"Alert to {aid} failed: {e}")
                _mark_sent(key, f"admins({len(admin_ids)})")

    except Exception as e:
        logger.error(f"NC send error [{key}]: {e}")

    save_persistent_db()


# ═══════════════════════════════════════════════════════
# PUBLIC NOTIFY FUNCTIONS
# ═══════════════════════════════════════════════════════

async def notify_bot_online():
    t = _ab_fmt(None, "%d/%m %H:%M")
    await _send("bot_online",
        "✅ **Bot Wapas Online Hai!**\n\n"
        "Bot thodi der ke liye offline tha, ab wapas chal raha hai.\n"
        "Tumhari forwarding automatically resume ho gayi hai.",
        who="admin")
    # Alag notification — active users ko
    await _send("user_bot_online",
        "✅ **Bot Wapas Online Hai!**\n\n"
        "Bot thodi der ke liye offline tha, ab wapas chal raha hai.\n"
        "Tumhari forwarding automatically resume ho gayi hai.",
        who="active_users")


async def notify_bot_offline():
    await _send("bot_offline",
        "🔴 **Bot Offline Ho Raha Hai!**\n\n"
        "Bot restart ho raha hai.\nWapas aane par forwarding auto-resume hogi.",
        who="admin")


async def notify_new_user(user_id: int, username: str = "", first_name: str = ""):
    """✅ New user notification — pura detail format."""
    from database import db, get_user_data
    import time as _time

    # Name display
    name_display = first_name or f"User {user_id}"

    # Username
    uname_display = f"@{username}" if username else "No username"

    # Referred by
    try:
        referred_by = get_user_data(user_id).get("refer", {}).get("referred_by")
        if referred_by:
            ref_data  = get_user_data(referred_by)
            ref_name  = ref_data.get("profile", {}).get("first_name") or f"User {referred_by}"
            ref_uname = ref_data.get("profile", {}).get("username", "")
            ref_str   = f"{ref_name}" + (f" (@{ref_uname})" if ref_uname else "") + f" `[{referred_by}]`"
        else:
            ref_str = "Direct"
    except Exception:
        ref_str = "Direct"

    # Total users
    try:
        total = len(db)
    except Exception:
        total = "?"

    t = _ab_fmt(None, "%d/%m/%Y %H:%M")

    await _send("new_user",
        f"🆕 **NAYA USER AAYA!**\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 Name: `{name_display}`\n"
        f"🆔 User ID: `{user_id}`\n"
        f"📱 Username: {uname_display}\n"
        f"👥 Referred By: {ref_str}\n"
        f"👥 Total Users Now: `{total}`\n"
        f"🕒 Time: `{t}`\n"
        f"━━━━━━━━━━━━━━━━━━━━",
        who="admin")


async def notify_new_premium(user_id: int, days: int, given_by: int = None):
    plan = "Lifetime ♾️" if days == 0 else f"{days} din"
    by   = f"\n✍️ By: `{given_by}`" if given_by else ""
    await _send("new_premium",
        f"💎 **Naya Premium!**\n━━━━━━━━━━━━━━━\n"
        f"🆔 `{user_id}`\n📦 {plan}{by}",
        who="admin")


async def notify_payment_received(user_id: int, amount: str = "", utr: str = ""):
    amt = f"\n💰 `{amount}`" if amount else ""
    utr_t = f"\n🔑 UTR: `{utr}`" if utr else ""
    await _send("payment",
        f"💰 **Payment Aaya!**\n━━━━━━━━━━━━━━━\n"
        f"🆔 `{user_id}`{amt}{utr_t}\n⚠️ Manual approval required!",
        who="admin")


async def notify_fraud_detected(user_id: int, reason: str):
    await _send("fraud",
        f"🚨 **Fraud Detected!**\n━━━━━━━━━━━━━━━\n"
        f"🆔 `{user_id}`\n❌ `{reason}`\n🛡 Auto-rejected.",
        who="admin")


async def notify_worker_dead(worker_id: int):
    await _send("worker_dead",
        f"💀 **Worker Down!**\n━━━━━━━━━━━━━━━\n"
        f"Worker: `{worker_id}`\n⚠️ Logs check karo!",
        who="admin")


async def notify_ram_high(ram_mb: float, ram_pct: float):
    await _send("ram_high",
        f"🔴 **HIGH RAM!**\n━━━━━━━━━━━━━━━\n"
        f"📊 `{ram_mb}MB` ({ram_pct}%)\n💡 Redeploy ya sessions band karo.",
        who="admin")


async def notify_forward_errors(user_id: int, error_msg: str, count: int):
    if count < 5: return
    await _send("forward_errors",
        f"⚠️ **Forward Errors x{count}**\n"
        f"User: `{user_id}`\n`{error_msg[:100]}`",
        who="admin")


async def notify_daily_summary():
    try:
        from analytics import get_global_summary
        s = get_global_summary()
        d = _ab_fmt(None, "%d %b %Y")
        await _send("daily_summary",
            f"📊 **Daily Summary — {d}**\n━━━━━━━━━━━━━━━━━━━━\n"
            f"👥 Total: `{s['total_users']}`\n"
            f"🟢 Active Today: `{s['active_today']}`\n"
            f"✅ Forwarded: `{s['today']}`\n"
            f"📅 This Week: `{s['week']}`\n"
            f"📈 All Time: `{s['total']}`",
            who="admin")
    except Exception as e:
        logger.debug(f"Daily summary: {e}")


# ── User alerts ──────────────────────────────────────

async def alert_user_session_expired(user_id: int):
    await _send("user_session",
        "⚠️ **Session Expire!**\n\nForwarding band ho gayi.\n✅ /start → Login dobara.",
        force_uid=user_id, who="user")


async def alert_user_not_admin(user_id: int, channel: str):
    await _send("user_not_admin",
        f"⚠️ **Admin Permission Missing!**\n\nChannel: `{channel}`\n✅ Admin bano ya remove karo.",
        force_uid=user_id, who="user")


async def alert_user_limit_warning(user_id: int, used: int, total: int):
    pct = int(used / total * 100) if total > 0 else 0
    if pct < 80: return
    await _send("user_limit",
        f"⚠️ **Daily Limit {pct}%!**\n`{used}/{total}` use ho gaye.\n💎 Premium lo unlimited ke liye!",
        force_uid=user_id, who="user")


async def alert_user_premium_expiring(user_id: int, days_left: int):
    """FIX J: Better premium expiry warning with CTA."""
    if days_left > 3:
        return
    if days_left <= 0:
        msg = "❌ **Tumhara Premium Expire Ho Gaya!**\n\nPremium features band ho gaye hain।\n💎 Renew karne ke liye /premium dabao।"
    elif days_left == 1:
        msg = "⚠️ **Premium Kal Expire Hoga!**\n\nSirf **1 din** bacha hai।\n💎 Renew karne ke liye /premium dabao।"
    else:
        msg = f"⏰ **Premium {days_left} Din Mein Expire!**\n\n{days_left} din baad premium features band ho jayenge।\n💎 Renew karne ke liye /premium dabao।"
    await _send("user_premium_exp", msg, force_uid=user_id, who="user")


async def alert_user_auto_paused(user_id: int, reason: str = ""):
    await _send("user_paused",
        f"🛑 **Forwarding Auto-Pause!**\n\nBahut zyada errors.\n"
        f"{f'Reason: `{reason}`' if reason else ''}\n\n✅ /start karo.",
        force_uid=user_id, who="user")


async def start_daily_summary_task():
    """FIX 6: Daily summary at midnight IST (not server time)."""
    while True:
        try:
            import pytz
            tz_ist = pytz.timezone("Asia/Kolkata")
            now_ist = datetime.datetime.now(tz_ist)
            nxt_ist = (now_ist + datetime.timedelta(days=1)).replace(
                hour=0, minute=5, second=0, microsecond=0
            )
            sleep_sec = (nxt_ist - now_ist).total_seconds()
            await asyncio.sleep(max(sleep_sec, 60))
            await notify_daily_summary()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Daily summary task: {e}")
            await asyncio.sleep(3600)


# ═══════════════════════════════════════════════════════
# ADMIN MENU — Notification Control Center UI
# ═══════════════════════════════════════════════════════

from telethon import errors,  Button, events


def _nc_main_text() -> str:
    nc    = _nc()
    total = len(NOTIFICATION_CATALOG)
    on_ct = sum(1 for k in NOTIFICATION_CATALOG if nc["enabled"].get(k, True))
    dest  = nc.get("global_dest")
    dest_txt = f"`{dest}`" if dest else "Admins (default)"
    name  = nc.get("owner_name") or "—"
    show  = "✅ Visible" if nc.get("owner_show", True) else "❌ Hidden"
    master = nc.get("master", True)

    return (
        "🔔 **Notification Control Center**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{'🟢' if master else '🔴'} **Master:** {'ON — Sab notifications active' if master else 'OFF — Koi notification nahi jayegi'}\n"
        f"📊 **Active:** `{on_ct}/{total}` notifications ON\n"
        f"📤 **Default Dest:** {dest_txt}\n"
        f"👤 **Bot Owner Name:** `{name}` ({show})\n\n"
        "Category select karo ya neeche se settings karo:"
    )


def _nc_main_buttons() -> list:
    nc     = _nc()
    master = nc.get("master", True)
    btns   = [
        [Button.inline(
            f"{'🔴 Master OFF karein' if master else '🟢 Master ON karein'}",
            b"nc_master"
        )],
    ]
    for cat_key, cat_label in CATEGORIES.items():
        keys_in = [k for k, v in NOTIFICATION_CATALOG.items() if v[2] == cat_key]
        on_ct   = sum(1 for k in keys_in if nc["enabled"].get(k, True))
        btns.append([
            Button.inline(f"{cat_label}  ·  {on_ct}/{len(keys_in)} ON",
                          f"nc_cat|{cat_key}".encode())
        ])
    btns += [
        [Button.inline("📤 Alert Destination Set",  b"nc_dest_global"),
         Button.inline("👤 Bot Owner Name",          b"nc_owner")],
        [Button.inline("🧪 Test Alert Bhejo",        b"nc_test"),
         Button.inline("📋 Notification History",    b"nc_history")],
        [Button.inline("🔙 Admin Menu",              b"adm_main")],
    ]
    return btns


def _cat_text(cat_key: str) -> str:
    nc  = _nc()
    lbl = CATEGORIES.get(cat_key, cat_key)
    lines = [f"🔔 **{lbl}**\n━━━━━━━━━━━━━━━━━━\n"]
    for k, v in NOTIFICATION_CATALOG.items():
        if v[2] != cat_key: continue
        on      = nc["enabled"].get(k, v[3])
        stats   = nc.get("stats", {}).get(k, {})
        last_ts = stats.get("last_ts", 0)
        last    = datetime.datetime.fromtimestamp(last_ts).strftime("%d/%m %H:%M") if last_ts else "Kabhi nahi"
        dest    = nc.get("per_dest", {}).get(k, "Default")
        lines.append(
            f"{'✅' if on else '❌'} **{v[0]}**\n"
            f"   📝 _{v[1]}_\n"
            f"   📤 Dest: `{dest}` · 🕒 Last: `{last}`\n"
        )
    return "\n".join(lines)


def _cat_buttons(cat_key: str) -> list:
    nc   = _nc()
    btns = []
    for k, v in NOTIFICATION_CATALOG.items():
        if v[2] != cat_key: continue
        on = nc["enabled"].get(k, v[3])
        btns.append([
            Button.inline(
                f"{'✅' if on else '❌'}  {v[0]}",
                f"nc_tog|{k}".encode()
            ),
            Button.inline("📤", f"nc_setdest|{k}".encode()),
        ])
    btns.append([Button.inline("🔙 Back", b"nc_menu")])
    return btns


def register_nc_handlers(b):
    """Call this from main.py on startup."""

    @b.on(events.CallbackQuery(data=b"nc_menu"))
    async def _nc_main(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        await event.edit(_nc_main_text(), buttons=_nc_main_buttons(), parse_mode="md")

    @b.on(events.CallbackQuery(data=b"nc_master"))
    async def _master(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        nc = _nc()
        nc["master"] = not nc.get("master", True)
        save_persistent_db()
        st = "🟢 ON" if nc["master"] else "🔴 OFF"
        await event.answer(f"Master: {st}", alert=True)
        await event.edit(_nc_main_text(), buttons=_nc_main_buttons(), parse_mode="md")

    @b.on(events.CallbackQuery(pattern=b"nc_cat\\|"))
    async def _cat(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        cat = event.data.decode().split("|")[1]
        await event.edit(_cat_text(cat), buttons=_cat_buttons(cat), parse_mode="md")

    @b.on(events.CallbackQuery(pattern=b"nc_tog\\|"))
    async def _tog(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        key = event.data.decode().split("|")[1]
        nc  = _nc()
        nc["enabled"][key] = not nc["enabled"].get(key, True)
        save_persistent_db()
        st  = "✅ ON" if nc["enabled"][key] else "❌ OFF"
        await event.answer(st, alert=False)
        cat = NOTIFICATION_CATALOG[key][2]
        await event.edit(_cat_text(cat), buttons=_cat_buttons(cat), parse_mode="md")

    @b.on(events.CallbackQuery(pattern=b"nc_setdest\\|"))
    async def _setdest_per(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        key   = event.data.decode().split("|")[1]
        label = NOTIFICATION_CATALOG[key][0]
        _set_step(event.sender_id, f"nc_dest|{key}")
        await event.edit(
            f"📤 **Destination — {label}**\n\n"
            "Kahan bhejna hai?\n\n"
            "• Channel ID: `-1001234567890`\n"
            "• Group ID: `-1009876543210`\n"
            "• Username: `@mychannel`\n"
            "• Bot me: apna user ID\n\n"
            "Default (admins) ke liye: `0` bhejo",
            buttons=[[Button.inline("🔙 Cancel", b"nc_menu")]]
        )

    @b.on(events.CallbackQuery(data=b"nc_dest_global"))
    async def _dest_global(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        _set_step(event.sender_id, "nc_dest_global")
        nc   = _nc()
        curr = nc.get("global_dest") or "Admins (default)"
        await event.edit(
            f"📤 **Global Alert Destination**\n\n"
            f"Abhi: `{curr}`\n\n"
            "Ye destination sab admin notifications ke liye default hoga.\n\n"
            "• Channel: `-1001234567890` ya `@channel`\n"
            "• Group: `-1001234567890`\n"
            "• Bot mein apne paas: apna user ID\n"
            "• Reset (admins): `0`",
            buttons=[[Button.inline("🔙 Cancel", b"nc_menu")]]
        )

    @b.on(events.CallbackQuery(data=b"nc_owner"))
    async def _owner_menu(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        nc   = _nc()
        name = nc.get("owner_name") or "—"
        show = nc.get("owner_show", True)
        await event.edit(
            f"👤 **Bot Owner Name Settings**\n\n"
            f"Current: `{name}`\n"
            f"Status: {'✅ Visible' if show else '❌ Hidden'}\n\n"
            "Kya karna hai?",
            buttons=[
                [Button.inline("✏️ Naam Change Karo",  b"nc_owner_rename")],
                [Button.inline(
                    "👁 Hide/Show Toggle",
                    b"nc_owner_hide"
                )],
                [Button.inline("🗑 Remove Completely",  b"nc_owner_del")],
                [Button.inline("🔙 Back",               b"nc_menu")],
            ]
        )

    @b.on(events.CallbackQuery(data=b"nc_owner_rename"))
    async def _owner_rename(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        _set_step(event.sender_id, "nc_owner_name")
        await event.edit(
            "✏️ **Naya Bot Owner Naam Bhejo**\n\n"
            "Example: `Rahul Singh` ya `@MyBotAdmin`\n\n"
            "Ye naam har notification ke neeche dikhega.",
            buttons=[[Button.inline("🔙 Cancel", b"nc_owner")]]
        )

    @b.on(events.CallbackQuery(data=b"nc_owner_hide"))
    async def _owner_toggle(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        nc = _nc()
        nc["owner_show"] = not nc.get("owner_show", True)
        save_persistent_db()
        st = "✅ Visible" if nc["owner_show"] else "❌ Hidden"
        await event.answer(f"Owner Line: {st}", alert=True)
        await _owner_menu(event)

    @b.on(events.CallbackQuery(data=b"nc_owner_del"))
    async def _owner_del(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        nc = _nc()
        nc["owner_name"] = ""
        nc["owner_show"] = False
        save_persistent_db()
        await event.answer("🗑 Removed!", alert=True)
        await event.edit(_nc_main_text(), buttons=_nc_main_buttons(), parse_mode="md")

    @b.on(events.CallbackQuery(data=b"nc_test"))
    async def _test(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        nc   = _nc()
        dest = nc.get("global_dest") or event.sender_id
        try:
            footer = _footer()
            t = _ab_fmt(None, "%d/%m/%Y %H:%M")
            await bot.send_message(int(dest),
                f"🧪 **Test Notification**\n━━━━━━━━━━━━━━━\n"
                f"✅ Notification Center kaam kar raha hai!\n"
                f"📤 Destination: `{dest}`\n"
                f"🕒 `{t}`{footer}",
                parse_mode="md")
            await event.answer("✅ Test bheja!", alert=True)
        except Exception as e:
            await event.answer(f"❌ {e}", alert=True)

    @b.on(events.CallbackQuery(data=b"nc_history"))
    async def _history(event):
        if not _adm(event.sender_id): return await event.answer("❌", alert=True)
        nc    = _nc()
        stats = nc.get("stats", {})
        if not stats:
            return await event.answer("Abhi koi notification nahi gayi.", alert=True)
        lines = ["📋 **Notification History**\n━━━━━━━━━━━━━━━\n"]
        for k, v in NOTIFICATION_CATALOG.items():
            s = stats.get(k, {})
            if not s.get("count"): continue
            ts   = s.get("last_ts", 0)
            last = datetime.datetime.fromtimestamp(ts).strftime("%d/%m %H:%M") if ts else "—"
            lines.append(
                f"**{v[0]}**\n"
                f"   Sent: `{s.get('count',0)}x` · Last: `{last}` · To: `{s.get('last_dest','?')}`\n"
            )
        await event.edit(
            "\n".join(lines) if len(lines) > 1 else "Koi history nahi.",
            buttons=[[Button.inline("🔙 Back", b"nc_menu")]],
            parse_mode="md"
        )


def _adm(uid: int) -> bool:
    try:
        return uid in GLOBAL_STATE.get("admins", {}) or uid == OWNER_ID
    except Exception:
        return False


def _set_step(uid: int, step: str):
    from database import get_user_data
    get_user_data(uid)["step"] = step


async def handle_nc_input(event, user_id: int, step: str) -> bool:
    """
    main.py ke text handler se call karo.
    Returns True agar step handle hua.
    """
    nc   = _nc()
    text = (event.raw_text or "").strip()

    if step == "nc_dest_global":
        if text == "0":
            nc["global_dest"] = None
        else:
            nc["global_dest"] = text
        save_persistent_db()
        disp = f"`{text}`" if text != "0" else "Admins (reset)"
        await event.respond(f"✅ Global destination: {disp}\n\nTest ke liye Admin → 🔔 Notifications → 🧪 Test.")
        _set_step(user_id, None)
        return True

    if step.startswith("nc_dest|"):
        key  = step.split("|")[1]
        if text == "0":
            nc.setdefault("per_dest", {}).pop(key, None)
        else:
            nc.setdefault("per_dest", {})[key] = text
        save_persistent_db()
        lbl = NOTIFICATION_CATALOG.get(key, ("?",))[0]
        await event.respond(f"✅ Destination set for **{lbl}**: `{text if text != '0' else 'Default'}`")
        _set_step(user_id, None)
        return True

    if step == "nc_owner_name":
        nc["owner_name"] = text
        nc["owner_show"] = True
        save_persistent_db()
        await event.respond(f"✅ Bot Owner naam update ho gaya: `{text}`\n\nSab notifications mein ab ye naam aayega.")
        _set_step(user_id, None)
        return True

    return False
