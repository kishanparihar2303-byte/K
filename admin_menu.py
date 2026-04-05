import time
# bot/ui/admin_menu.py  — UPGRADED v4.0
# ══════════════════════════════════════════════════════════════
# COMPLETE ADMIN PANEL — All features in one place
# New in v4.0:
#   • Revenue Dashboard — daily/weekly/monthly income
#   • New Users Today counter on main panel
#   • Pending Payments quick badge
#   • User Notes — internal admin notes per user
#   • Broadcast delivery report (sent/failed)
#   • Premium expiry warning list
#   • Quick Search from main panel
#   • Better user profile (notes, revenue, timeline)
#   • Admin activity log with filter
#   • Improved navigation — all back buttons work
# ══════════════════════════════════════════════════════════════

from telethon import events, Button, errors
from config import bot, OWNER_ID
from database import (
    db, GLOBAL_STATE, save_persistent_db, user_sessions,
    admin_logs, get_user_data, cleanup_inactive_users, CLEANUP_CONFIG
)
from admin import (
    is_admin, get_system_stats, add_log,
    get_revenue_stats, get_last_broadcast, record_broadcast_result,
    get_user_notes, add_user_note, delete_user_note,
)

def get_admin_role(user_id) -> str:
    """Inline fallback — works with any admin.py version."""
    from config import OWNER_ID as _oid
    if user_id == _oid:
        return "Owner"
    return GLOBAL_STATE.get("admins", {}).get(user_id, "")
import json, io, datetime, time, asyncio


# ══════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════

def _user_display(uid):
    p = db.get(int(uid), {}).get("profile", {})
    first = p.get("first_name", "")
    last  = p.get("last_name", "")
    uname = p.get("username", "")
    full  = (first + " " + last).strip()
    if uname:
        return f"{full} (@{uname})" if full else f"@{uname}"
    return full or str(uid)

def _prem_badge(uid):
    prem = db.get(int(uid), {}).get("premium", {})
    if not prem.get("active"): return "🆓"
    d = prem.get("days_remaining", 0)
    if d <= 3: return f"💎⚠️{d}d"
    return f"💎{d}d"

def _now_str():
    from time_helper import ab_fmt  # FIX: lazy import instead of __import__
    return ab_fmt(None, "%d/%m/%Y %I:%M %p")

def _age_str(ts):
    if not ts: return "—"
    d = int((time.time() - ts) / 86400)
    if d == 0: return "Aaj"
    if d == 1: return "Kal"
    return f"{d}d pehle"


# ══════════════════════════════════════════════════════════════
# MAIN PANEL
# ══════════════════════════════════════════════════════════════

def get_admin_main_buttons(user_id):
    """
    Cleaner 6-section admin layout v5.0:
      🚀 Quick  |  👤 Users  |  🖥 System  |  💰 Money  |  ⚙️ Config  |  🛠 Tools
    Har section clearly labeled. Menu Customizer + Ads Packages added.
    """
    # Live badges
    pending = len([p for p in GLOBAL_STATE.get("pending_payments", {}).values()
                   if p.get("status") == "pending"])
    prem_expiring = sum(
        1 for u in db.values()
        if u.get("premium", {}).get("active") and 0 < u.get("premium", {}).get("days_remaining", 99) <= 3
    )
    from database import GLOBAL_STATE as _gs
    proofs_pending = len([p for p in _gs.get("task_proofs", []) if p.get("status") == "pending"])
    inquiries_pending = 0
    try:
        import promo_engine as _pe
        inquiries_pending = len(_pe.get_pending_inquiries())
    except Exception:
        pass

    pend_badge   = f" ({pending})" if pending > 0 else ""
    expiry_badge = f" ⚠️{prem_expiring}" if prem_expiring > 0 else ""
    proof_badge  = f" ({proofs_pending})" if proofs_pending > 0 else ""
    inq_badge    = f" ({inquiries_pending})" if inquiries_pending > 0 else ""

    return [
        # ── Quick Actions ──────────────────────────────────────────────────
        [Button.inline("🔄 Refresh",       b"adm_main"),
         Button.inline("📊 Live Monitor",  b"adm_live")],

        # ── 👤 Users ───────────────────────────────────────────────────────
        [Button.inline("━━━━━ 👤 USERS ━━━━━", b"adm_main")],
        [Button.inline("👥 Users",              b"adm_user_mg"),
         Button.inline("🔐 Admins",             b"adm_mgmt")],
        [Button.inline(f"💎 Premium{expiry_badge}", b"adm_premium"),
         Button.inline(f"💳 Payments{pend_badge}",  b"adm_payment_settings")],
        [Button.inline("📢 Broadcast",          b"adm_broadcast_menu"),
         Button.inline("📝 User Notes",         b"adm_notes_list")],

        # ── 🖥 System ──────────────────────────────────────────────────────
        [Button.inline("━━━━━ 🖥️ SYSTEM ━━━━━", b"adm_main")],
        [Button.inline("🛠 Bot Control",        b"adm_bot_ctrl"),
         Button.inline("🤖 Workers",            b"adm_workers")],
        [Button.inline("📈 Analytics",          b"adm_analytics"),
         Button.inline("📁 Logs",               b"adm_logs")],
        [Button.inline("🧹 Cleanup",            b"adm_cleanup_panel"),
         Button.inline("🛡️ Anti-Spam",          b"as_main")],

        # ── 💰 Money ───────────────────────────────────────────────────────
        [Button.inline("━━━━━ 💰 MONEY ━━━━━", b"adm_main")],
        [Button.inline("📊 Revenue",            b"adm_revenue"),
         Button.inline("💰 Monetization",       b"adm_monetize_hub")],
        [Button.inline(f"⚠️ Expiry{expiry_badge}",  b"adm_expiry_warn"),
         Button.inline("🔗 Refer Settings",     b"adm_refer_panel")],
        [Button.inline(f"📣 Sponsor Inq{inq_badge}", b"promo_inquiries"),
         Button.inline(f"🎯 Task Proofs{proof_badge}", b"adm_task_proofs")],

        # ── ⚙️ Config ──────────────────────────────────────────────────────
        [Button.inline("━━━━━ ⚙️ CONFIG ━━━━━", b"adm_main")],
        [Button.inline("⚙️ Feature Flags",      b"adm_feature_flags"),
         Button.inline("🔒 Force Subscribe",    b"adm_force_sub")],
        [Button.inline("📢 Msg Limits",         b"adm_msg_limits"),
         Button.inline("🔔 Notifications",      b"nc_menu")],
        [Button.inline("📌 Notice / Rules",     b"adm_notice_panel"),
         Button.inline("👁 Bot Welcome Msg",    b"adm_welcome_msg")],
        [Button.inline("🎫 Support Tickets",    b"adm_support_panel")],
        [Button.inline("🖼️ Watermark",          b"flags_watermark_menu"),
         Button.inline("🔗 Affiliate Tags",     b"flags_affiliate_menu")],
        [Button.inline("✏️ Force Start/End Msg", b"adm_force_se_menu"),
         Button.inline("🌐 Global Templates",     b"adm_global_tpl_menu")],
        [Button.inline("👥 Resellers",          b"adm_resellers"),
         Button.inline("🆓/💎 Free/Paid Mode",  b"adm_premium")],

        # ── 🛠 Tools ───────────────────────────────────────────────────────
        [Button.inline("━━━━━ 🛠️ TOOLS ━━━━━", b"adm_main")],
        [Button.inline("🎨 Menu Customizer",    b"adm_menu_customizer"),
         Button.inline("📦 Ads Packages",       b"adm_ads_packages")],

        # ── Footer ────────────────────────────────────────────────────────
        [Button.inline("🏠 Main Menu",          b"main_menu")],
    ]

def _admin_header(stats, user_id=None):
    """Smart admin panel header with live clock, alerts, and KPIs."""
    from time_helper import ab_now, tz_name
    now      = ab_now(user_id)
    tz_label = tz_name(user_id)

    # Status flags
    m_mode   = GLOBAL_STATE.get("maintenance_mode", False)
    blk_reg  = GLOBAL_STATE.get("block_new_reg", False)
    pending  = len([p for p in GLOBAL_STATE.get("pending_payments", {}).values()
                    if p.get("status") == "pending"])
    rev      = get_revenue_stats()

    # Alert lines — only show what needs attention
    alerts = []
    if m_mode:
        alerts.append("🔧 **MAINTENANCE MODE ON** — users cannot use bot!")
    if blk_reg:
        alerts.append("🚫 New registrations are BLOCKED")
    if pending > 0:
        alerts.append(f"💳 **{pending} payment(s)** awaiting approval")
    prem_expiring = sum(
        1 for u in db.values()
        if u.get("premium", {}).get("active") and 0 < u.get("premium", {}).get("days_remaining", 99) <= 3
    )
    if prem_expiring > 0:
        alerts.append(f"⚠️ **{prem_expiring} premium** user(s) expiring in ≤3 days")
    try:
        import promo_engine as _pe
        inq = len(_pe.get_pending_inquiries())
        if inq > 0: alerts.append(f"📬 **{inq} sponsor inquiry** unanswered")
    except Exception:
        pass

    alert_block = ""
    if alerts:
        alert_block = "\n⚡ **ALERTS:**\n" + "\n".join(f"  • {a}" for a in alerts) + "\n"

    # Last broadcast line
    last_bc = get_last_broadcast()
    bc_line = ""
    if last_bc:
        bc_line = (f"\n📢 Last BC: ✅{last_bc.get('sent',0)} ❌{last_bc.get('failed',0)}"
                   f" → `{last_bc.get('target','?')}` `{last_bc.get('time','?')}`")

    # Status indicators
    sys_status = "🟢 Normal" if not m_mode else "🔧 Maintenance"
    reg_status = "🔴 Blocked" if blk_reg else "🟢 Open"

    # Forwarding activity bar
    ratio = stats["active_fwd"] / max(stats["total_users"], 1)
    bar_w = 10
    filled = round(ratio * bar_w)
    fwd_bar = "█" * filled + "░" * (bar_w - filled)

    return (
        "🛠 **ADMIN PANEL**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 `{now.strftime('%H:%M:%S')}` "
        f"📅 `{now.strftime('%d %b %Y')}` "
        f"🌍 `{tz_label}`\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{alert_block}"
        f"**👤 Users:** `{stats['total_users']}` total  "
        f"🆕 `+{stats['new_today']}` today  "
        f"📅 `+{stats['new_week']}` week\n"
        f"**⚡ Forwarding:** [{fwd_bar}] `{stats['active_fwd']}/{stats['total_users']}`  "
        f"💎 `{stats['prem_count']}` prem  "
        f"📡 `{len(user_sessions)}` sess\n"
        f"**💰 Revenue:** `₹{rev['today']}` today  "
        f"`₹{rev['month']}` month  "
        f"`₹{stats['revenue_month']}` total\n"
        f"**🔧 System:** {sys_status}  🚪 Reg: {reg_status}"
        f"{bc_line}"
    )


# ══════════════════════════════════════════════════════════════
# ADMIN SESSION MANAGER — v5.0
# ✅ FIX: Auto-redirect band kiya
# ✅ FIX: 10-minute idle auto-close
# ✅ FIX: Clock sirf main panel pe update hota hai (sub-menus pe nahi)
# ══════════════════════════════════════════════════════════════

_ADMIN_IDLE_TIMEOUT = 600   # 10 minutes in seconds
_CLOCK_INTERVAL     = 60    # Clock refresh every 60s (less aggressive)

# { user_id: (msg_id, chat_id) } — jab /admin open ho
_clock_sessions: dict = {}

# { user_id: last_activity_timestamp } — har click pe update hota hai
_admin_last_activity: dict = {}

# Set of admins currently on MAIN panel (not sub-menus)
_admin_at_main: set = set()


def admin_mark_at_main(uid: int):
    """Admin main panel pe aa gaya — clock allow karo."""
    _admin_at_main.add(uid)
    _admin_last_activity[uid] = time.time()


def admin_mark_left_main(uid: int):
    """Admin sub-menu pe gaya — clock band karo, session alive rakho."""
    _admin_at_main.discard(uid)
    _admin_last_activity[uid] = time.time()   # activity record karo


def admin_mark_activity(uid: int):
    """Koi bhi admin action — idle timer reset karo."""
    _admin_last_activity[uid] = time.time()


def admin_clock_cleanup(uid: int):
    """Admin panel poora band — session hata do."""
    _admin_at_main.discard(uid)
    _clock_sessions.pop(uid, None)
    _admin_last_activity.pop(uid, None)


async def _live_clock_loop():
    """
    Every 60s:
    1. Idle check — agar 10 min se koi activity nahi to admin panel auto-close
    2. Clock update — sirf main panel pe (sub-menus pe nahi)
    """
    while True:
        await asyncio.sleep(_CLOCK_INTERVAL)
        if not _clock_sessions:
            continue

        now   = time.time()
        stats = get_system_stats()

        for uid, (msg_id, chat_id) in list(_clock_sessions.items()):
            try:
                from config import bot as _bot
                from admin import is_admin as _is_admin
                if not _is_admin(uid):
                    admin_clock_cleanup(uid)
                    continue

                last_act = _admin_last_activity.get(uid, now)
                idle_sec = now - last_act

                # ── 10-minute idle → auto-close admin panel ────────────────
                if idle_sec >= _ADMIN_IDLE_TIMEOUT:
                    admin_clock_cleanup(uid)
                    try:
                        await _bot.edit_message(
                            chat_id, msg_id,
                            "⏰ **Admin Panel — Session Expire**\n\n"
                            "10 minute tak koi activity nahi thi.\n"
                            "Panel band kar diya gaya.\n\n"
                            "_Dobara use karne ke liye /admin bhejo._",
                            buttons=[[Button.inline("🔄 Wapas Kholein", b"adm_reopen")]],
                            parse_mode="md"
                        )
                    except Exception:
                        pass
                    continue

                # ── Clock update — SIRF main panel pe ─────────────────────
                if uid not in _admin_at_main:
                    continue   # Sub-menu pe hai — overwrite mat karo

                await _bot.edit_message(
                    chat_id, msg_id,
                    _admin_header(stats, uid),
                    buttons=get_admin_main_buttons(uid),
                    parse_mode="md"
                )

            except errors.MessageNotModifiedError:
                pass
            except (errors.MessageDeletedError, errors.MessageIdInvalidError):
                admin_clock_cleanup(uid)
            except Exception as _e:
                if any(x in str(_e).lower() for x in ["deleted", "invalid", "not found", "forbidden"]):
                    admin_clock_cleanup(uid)


@bot.on(events.CallbackQuery(data=b"adm_reopen"))
async def adm_reopen(event):
    """Admin panel dobara kholein after session expire."""
    uid = event.sender_id
    if not is_admin(uid):
        return await event.answer("❌ Admin nahi ho.", alert=True)
    stats = get_system_stats()
    try:
        await event.edit(
            _admin_header(stats, uid),
            buttons=get_admin_main_buttons(uid)
        )
        _clock_sessions[uid] = (event.message_id, event.chat_id)
        admin_mark_at_main(uid)
    except Exception as e:
        await event.answer(f"❌ Error: {str(e)[:80]}", alert=True)

@bot.on(events.NewMessage(pattern='/admin'))
async def admin_cmd(event):
    if not event.is_private:
        return
    uid = event.sender_id
    # FIX 2: Clear any pending step when admin opens panel
    from database import get_user_data
    _adata = get_user_data(uid)
    if _adata.get("step"):
        _adata["step"] = None
        _adata.pop("step_since", None)
    print(f"[ADMIN] /admin from uid={uid}")
    if not is_admin(uid):
        return await event.respond("⚠️ Tumhare paas admin permission nahi hai।")
    try:
        stats = get_system_stats()
        sent  = await event.respond(_admin_header(stats, uid), buttons=get_admin_main_buttons(uid))
        _clock_sessions[uid] = (sent.id, event.chat_id)   # Track for live clock
        admin_mark_at_main(uid)   # ✅ FIX: Mark admin as on main panel
        print(f"[ADMIN] Panel sent to {uid}")
    except Exception as e:
        print(f"[ADMIN] Error: {e}")
        await event.respond(f"❌ Admin panel load error: {str(e)[:80]}")


@bot.on(events.NewMessage(pattern=r'/admin (.+)'))
async def admin_shortcut_cmd(event):
    """Quick admin shortcuts: /admin uid:12345  /admin search:name  /admin bc"""
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    arg = event.pattern_match.group(1).strip().lower()

    if arg.startswith("uid:"):
        # Quick user lookup
        try:
            target_uid = int(arg.split(":")[1])
            event.data = f"adm_view_u_{target_uid}".encode()
            await adm_view_user(event)
        except Exception:
            await event.respond("❌ Format: `/admin uid:12345`")

    elif arg.startswith("bc") or arg == "broadcast":
        event.data = b"adm_broadcast_menu"
        await adm_broadcast_menu(event)

    elif arg in ("live", "monitor", "stats"):
        event.data = b"adm_live"
        await adm_live(event)

    elif arg in ("users", "user"):
        event.data = b"adm_user_mg"
        await adm_user_mg(event)

    elif arg in ("revenue", "rev", "money"):
        event.data = b"adm_revenue"
        await adm_revenue_panel(event)

    else:
        await event.respond(
            "⚡ **Admin Shortcuts:**\n\n"
            "`/admin uid:12345` — User profile\n"
            "`/admin bc` — Broadcast\n"
            "`/admin live` — Live monitor\n"
            "`/admin users` — User list\n"
            "`/admin revenue` — Revenue\n"
        )


@bot.on(events.CallbackQuery(data=b"adm_main"))
async def adm_main_cb(event):
    await event.answer()
    uid = event.sender_id
    if not is_admin(uid): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    # FIX: Clear pending step when returning to admin panel
    from database import get_user_data as _gud
    _d = _gud(uid)
    if _d.get("step") and not str(_d.get("step", "")).startswith("adm_"):
        _d["step"] = None
        _d.pop("step_since", None)
    _d.pop("_admin_subscreen", None)
    stats = get_system_stats()
    try:
        await event.edit(_admin_header(stats, uid), buttons=get_admin_main_buttons(uid))
        _clock_sessions[uid] = (event.message_id, event.chat_id)
        admin_mark_at_main(uid)
    except errors.MessageNotModifiedError:
        admin_mark_at_main(uid)
        await event.answer("✅ Refreshed!")


# ══════════════════════════════════════════════════════════════
# 💰 REVENUE DASHBOARD (NEW)
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_revenue"))
async def adm_revenue_panel(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    rev = get_revenue_stats()
    pending = GLOBAL_STATE.get("pending_payments", {})
    pending_list = [(pid, p) for pid, p in pending.items() if p.get("status") == "pending"]

    # Leaderboard — top paying users
    payment_history = GLOBAL_STATE.get("payment_history", [])
    user_totals = {}
    for p in payment_history:
        if p.get("status") == "approved":
            uid = str(p.get("user_id", "?"))
            user_totals[uid] = user_totals.get(uid, 0) + p.get("amount", 0)
    top_payers = sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:5]

    pending_txt = ""
    if pending_list:
        pending_txt = f"\n⏳ **Pending Payments ({len(pending_list)}):**\n"
        for pid, p in pending_list[:3]:
            uid_str = str(p.get("user_id", "?"))
            plan    = p.get("plan", "?")
            amt     = p.get("amount", "?")
            pending_txt += f"  • `{uid_str}` — {plan} ₹{amt}\n"
        if len(pending_list) > 3:
            pending_txt += f"  ...aur {len(pending_list)-3} aur\n"

    top_txt = ""
    if top_payers:
        top_txt = "\n🏆 **Top Payers (All Time):**\n"
        medals = ["🥇","🥈","🥉","4️⃣","5️⃣"]
        for i, (uid, amt) in enumerate(top_payers):
            name = _user_display(uid)[:20]
            top_txt += f"  {medals[i]} {name} — ₹{amt}\n"

    txt = (
        "💰 **Revenue Dashboard**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📆 Aaj:       **₹{rev['today']}**\n"
        f"📅 Is Hafte:  **₹{rev['week']}**\n"
        f"🗓 Is Mahine: **₹{rev['month']}**\n"
        f"📈 All Time:  **₹{rev['total']}**\n"
        f"💳 Total Transactions: `{rev['total_txns']}`\n"
        f"⏳ Pending: `{rev['pending_count']}`\n"
        f"{pending_txt}{top_txt}"
    )
    btns = []
    if pending_list:
        btns.append([Button.inline(f"⏳ Approve Pending ({len(pending_list)})", b"adm_payment_settings")])
    btns += [
        [Button.inline("📊 Full Analytics",  b"adm_analytics"),
         Button.inline("💳 Payment Config",  b"adm_payment_settings")],
        [Button.inline("🔄 Refresh",         b"adm_revenue"),
         Button.inline("🔙 Admin Panel",     b"adm_main")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        await event.answer("Up to date!")


# ══════════════════════════════════════════════════════════════
# 👥 USER MANAGEMENT
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_user_mg"))
async def adm_user_mg(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    total    = len(db)
    active   = sum(1 for u in db.values() if u["settings"]["running"])
    premium  = sum(1 for u in db.values() if u.get("premium", {}).get("active"))
    blocked  = len(GLOBAL_STATE.get("blocked_users", []))
    session  = len(user_sessions)
    today_ts = datetime.datetime.now().replace(hour=0, minute=0, second=0).timestamp()
    new_today = sum(1 for u in db.values() if u.get("joined_at", 0) >= today_ts)
    expiring = sum(
        1 for u in db.values()
        if u.get("premium", {}).get("active") and 0 < u.get("premium", {}).get("days_remaining", 99) <= 3
    )

    txt = (
        "👥 **USER MANAGEMENT**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Overview:**\n"
        f"  👤 Total: `{total}`   🆕 Today: `+{new_today}`\n"
        f"  ⚡ Active Fwd: `{active}`   🔴 Stopped: `{total - active}`\n"
        f"  💎 Premium: `{premium}`"
        + (f"  ⚠️ Expiring: `{expiring}`" if expiring else "")
        + f"\n"
        f"  📡 Sessions: `{session}`   🚫 Blocked: `{blocked}`\n\n"
        "**Filter & Browse:**"
    )

    try:
        await event.edit(txt, buttons=[
            [Button.inline(f"👤 All ({total})",           b"adm_ulist_all_0"),
             Button.inline(f"⚡ Active ({active})",        b"adm_ulist_active_0")],
            [Button.inline(f"💎 Premium ({premium})",      b"adm_ulist_premium_0"),
             Button.inline(f"🚫 Blocked ({blocked})",      b"adm_ulist_blocked_0")],
            [Button.inline(f"📡 Sessions ({session})",     b"adm_ulist_session_0"),
             Button.inline(f"🆕 New Today ({new_today})",  b"adm_ulist_new_0")],
            [Button.inline(f"⚠️ Expiring ({expiring})",    b"adm_ulist_expiring_0"),
             Button.inline("🔴 Stopped",                   b"adm_ulist_stopped_0")],
            [Button.inline("🔍 Search by ID",              b"adm_search_user"),
             Button.inline("🔍 Search by Name",            b"adm_search_name")],
            [Button.inline("📤 Export All Users",          b"adm_export_users"),
             Button.inline("⚡ Bulk Actions",              b"adm_bulk_actions")],
            [Button.inline("📋 Expiry Warnings",           b"adm_expiry_warn"),
             Button.inline("📝 User Notes",               b"adm_notes_list")],
            [Button.inline("🔙 Admin Panel",               b"adm_main")],
        ])
    except errors.MessageNotModifiedError:
        pass

def _get_filtered_users(filter_type):
    users = list(db.keys())
    now = time.time()
    today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0).timestamp()
    if filter_type == "active":    return [u for u in users if db[u]["settings"]["running"]]
    elif filter_type == "stopped": return [u for u in users if not db[u]["settings"]["running"]]
    elif filter_type == "premium": return [u for u in users if db[u].get("premium",{}).get("active")]
    elif filter_type == "blocked": return [u for u in users if int(u) in GLOBAL_STATE.get("blocked_users",[])]
    elif filter_type == "session": return [u for u in users if db[u].get("session")]
    elif filter_type == "expiring": return [u for u in users
                                            if db[u].get("premium",{}).get("active")
                                            and db[u].get("premium",{}).get("days_remaining", 99) <= 3]
    elif filter_type == "new":     return [u for u in users if db[u].get("joined_at", 0) >= today_start]
    return users

async def _show_user_list(event, filter_type, page):
    PER  = 8
    users = _get_filtered_users(filter_type)
    total = len(users)
    start_i = page * PER
    end_i   = start_i + PER
    sub     = users[start_i:end_i]

    filter_labels = {
        "all":"All","active":"⚡ Active","stopped":"🔴 Stopped",
        "premium":"💎 Premium","blocked":"🚫 Blocked",
        "session":"📡 Session","expiring":"⚠️ Expiring","new":"🆕 New"
    }
    label = filter_labels.get(filter_type, filter_type)

    if not sub:
        try:
            return await event.edit(
                f"👥 **{label} Users**\n\nKoi nahi mila!",
                buttons=[[Button.inline("🔙 Back", b"adm_user_mg")]]
            )
        except errors.MessageNotModifiedError: return

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    btns  = []
    for uid in sub:
        u     = db.get(uid, {})
        name  = _user_display(uid)[:20]
        prem  = _prem_badge(uid)
        run   = "🟢" if u.get("settings",{}).get("running") else "🔴"
        sess  = "📡" if uid in user_sessions else "  "
        fwd   = u.get("analytics",{}).get("daily",{}).get(today,{}).get("forwarded",0)
        fwd_s = f" ·{fwd}↑" if fwd > 0 else ""
        btns.append([Button.inline(
            f"{run}{sess} {name} {prem}{fwd_s}",
            f"adm_view_u_{uid}".encode()
        )])

    nav = []
    if page > 0:      nav.append(Button.inline("⬅️ Prev", f"adm_ulist_{filter_type}_{page-1}".encode()))
    if end_i < total: nav.append(Button.inline("Next ➡️", f"adm_ulist_{filter_type}_{page+1}".encode()))

    header = (
        f"👥 **{label} Users** ({start_i+1}–{min(end_i,total)} of {total})\n"
        "🟢=running  📡=session  💎=prem  ↑=msgs today\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    )
    footer = []
    if nav:     footer.append(nav)
    footer += [
        [Button.inline("🔍 Search",      b"adm_search_user"),
         Button.inline("🔙 User Mgmt",  b"adm_user_mg")],
    ]
    try:
        await event.edit(header, buttons=btns + footer)
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_all_"))
async def adm_ulist_all(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "all", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_active_"))
async def adm_ulist_active(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "active", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_stopped_"))
async def adm_ulist_stopped(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "stopped", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_premium_"))
async def adm_ulist_premium(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "premium", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_blocked_"))
async def adm_ulist_blocked(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "blocked", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_session_"))
async def adm_ulist_session(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "session", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_expiring_"))
async def adm_ulist_expiring(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "expiring", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_ulist_new_"))
async def adm_ulist_new(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "new", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(pattern=b"adm_list_users_"))
async def adm_list_users_legacy(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_user_list(event, "all", int(event.data.decode().split("_")[-1]))

@bot.on(events.CallbackQuery(data=b"adm_search_user"))
async def adm_search_user(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    get_user_data(event.sender_id)["step"] = "adm_search_user_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("🔍 **User ID se Search**\n\nUser ka Telegram ID bhejo:",
                         buttons=[Button.inline("🔙 Back", b"adm_user_mg")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_search_name"))
async def adm_search_name(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    get_user_data(event.sender_id)["step"] = "adm_search_name_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "🔍 **Name/Username se Search**\n\nKoi bhi hissa bhejo:\nExample: `kishan` ya `@parihar`",
            buttons=[Button.inline("🔙 Back", b"adm_user_mg")]
        )
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════════════════════════
# 👤 USER PROFILE (UPGRADED)
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(pattern=b"adm_view_u_"))
async def adm_view_user(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    raw   = event.data.decode()
    uid   = int(raw.split("adm_view_u_")[1])
    udata = db.get(uid, {})
    if not udata:
        return await event.answer("User nahi mila!", alert=True)

    profile = udata.get("profile", {})
    prem    = udata.get("premium", {})
    sett    = udata.get("settings", {})
    stats   = udata.get("stats", {})
    refer   = udata.get("refer", {})
    sched   = udata.get("scheduler", {})
    ana     = udata.get("analytics", {})
    today   = datetime.datetime.now().strftime("%Y-%m-%d")

    # Names
    fname  = profile.get("first_name", "")
    lname  = profile.get("last_name", "")
    uname  = profile.get("username", "")
    full   = (fname + " " + lname).strip() or "—"
    handle = f"@{uname}" if uname else "no username"

    # Premium
    if prem.get("active"):
        rem  = prem.get("days_remaining", 0)
        plan = prem.get("plan", "?")
        prem_line = f"💎 **Premium** — {plan}  ({rem}d left)"
        if rem <= 3: prem_line += " ⚠️ EXPIRING SOON"
    else:
        prem_line = "🆓 Free user"

    # Forwarding
    srcs  = len(udata.get("sources", []))
    dests = len(udata.get("destinations", []))
    running = sett.get("running", False)
    fwd_state = "🟢 Running" if running else "🔴 Stopped"

    # Today stats
    today_fwd = ana.get("daily", {}).get(today, {}).get("forwarded", 0)
    today_blk = ana.get("daily", {}).get(today, {}).get("blocked", 0)
    total_fwd = stats.get("total_forwarded", 0)

    # Referrals
    refs = len(refer.get("referred_users", []))

    # Session
    has_session = uid in user_sessions
    sess_icon   = "📡 Connected" if has_session else "❌ No session"

    # Join date
    joined = udata.get("joined_at", 0)
    joined_str = datetime.datetime.fromtimestamp(joined).strftime("%d %b %Y") if joined else "—"
    age_str = _age_str(joined)

    # Scheduler
    sched_line = ""
    if sched.get("enabled"):
        sched_line = f"\n  ⏰ Scheduler: {sched.get('start','?')}–{sched.get('end','?')}"

    # User notes
    notes = get_user_notes(uid)
    notes_line = ""
    if notes:
        notes_line = f"\n\n📝 **Admin Notes ({len(notes)}):**\n" + "\n".join(
            f"  • `{n.get('note','')[:60]}`" for n in notes[-2:]
        )

    role = get_admin_role(uid)
    role_badge = f"  🔐 **{role}**" if role else ""

    txt = (
        f"👤 **User Profile**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"**{full}** {role_badge}\n"
        f"🆔 `{uid}`  {handle}\n"
        f"📅 Joined: `{joined_str}` ({age_str})\n\n"
        f"{prem_line}\n"
        f"{sess_icon}\n\n"
        f"**⚡ Forwarding:** {fwd_state}\n"
        f"  📥 Sources: `{srcs}`  📤 Dests: `{dests}`\n"
        f"  Today: `{today_fwd}` fwd  `{today_blk}` blk\n"
        f"  All time: `{total_fwd}` forwarded"
        f"{sched_line}\n\n"
        f"👥 Referrals: `{refs}`"
        f"{notes_line}"
    )

    is_blocked = uid in (GLOBAL_STATE.get("blocked_users") or [])
    blk_lbl = "✅ Unblock" if is_blocked else "🚫 Block"

    try:
        await event.edit(txt, buttons=[
            [Button.inline(blk_lbl,                f"adm_tgl_blk_{uid}".encode()),
             Button.inline("⏹ Force Stop",          f"adm_fstop_{uid}".encode())],
            [Button.inline("💎 Add Premium",         f"adm_premium_user_{uid}".encode()),
             Button.inline("💎 Remove Premium",      f"adm_rem_prem_{uid}".encode())],
            [Button.inline("💬 Send Message",        f"adm_msg_user_{uid}".encode()),
             Button.inline("📤 Export Config",       f"adm_export_u_{uid}".encode())],
            [Button.inline("📝 Notes",               f"adm_notes_{uid}".encode()),
             Button.inline("🔍 View Config",         f"adm_view_cfg_{uid}".encode())],
            [Button.inline("🗑 Reset Data",           f"adm_reset_u_{uid}".encode()),
             Button.inline("❌ Delete User",          f"adm_del_u_{uid}".encode())],
            [Button.inline("🔙 Back",                 b"adm_user_mg")],
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"adm_view_cfg_"))
async def adm_view_cfg(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid    = int(event.data.decode().split("_")[-1])
    u_data = db.get(uid)
    if not u_data: return await event.answer("User not found!", alert=True)
    srcs   = u_data.get("sources", [])
    dests  = u_data.get("destinations", [])
    names  = u_data.get("channel_names_id", {})
    def _lbl(v): return names.get(str(v), str(v))[:35]
    src_txt  = "\n".join(f"  • {_lbl(s)}" for s in srcs[:15]) or "  (koi nahi)"
    dest_txt = "\n".join(f"  • {_lbl(d)}" for d in dests[:15]) or "  (koi nahi)"
    settings = u_data.get("settings", {})
    running  = "🟢 Chalu" if settings.get("running") else "🔴 Band"
    try:
        await event.edit(
            f"📋 **User `{uid}` — Config**\n\n"
            f"⚡ Status: {running}\n\n"
            f"📥 **Sources ({len(srcs)}):**\n{src_txt}\n\n"
            f"📤 **Destinations ({len(dests)}):**\n{dest_txt}",
            buttons=[[Button.inline("🔙 Profile", f"adm_view_u_{uid}".encode())]]
        )
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(pattern=b"adm_msg_user_"))
async def adm_msg_user(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid  = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    data["step"] = f"adm_msg_user_input_{uid}"
    data["step_since"] = time.time()
    try:
        await event.edit(
            f"📨 **User `{uid}` ko Message Bhejo**\n\n"
            "Message likhkar bhejo (HTML supported):",
            buttons=[Button.inline("❌ Cancel", f"adm_view_u_{uid}".encode())]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"adm_rem_prem_"))
async def adm_rem_prem(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().split("_")[-1])
    if uid not in db: return await event.answer("User not found!", alert=True)
    db[uid]["premium"] = {"active": False}
    save_persistent_db()
    add_log(event.sender_id, "Remove Premium", target=uid)
    await event.answer(f"✅ Premium removed from {uid}", alert=True)
    await adm_view_user(event)

@bot.on(events.CallbackQuery(pattern=b"adm_export_u_"))
async def adm_export_u(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid    = int(event.data.decode().split("_")[-1])
    u_data = db.get(uid)
    if not u_data: return await event.answer("User not found!", alert=True)
    export = {k: v for k, v in u_data.items() if k != "session"}
    buf    = io.BytesIO(json.dumps(export, indent=2, default=str).encode())
    buf.name = f"user_{uid}_data.json"
    await event.respond(f"📤 User `{uid}` ka data:", file=buf)
    await event.answer("Sent!")

@bot.on(events.CallbackQuery(data=b"adm_export_users"))
async def adm_export_users(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await event.answer("⏳ Generating...", alert=False)
    out = io.StringIO()
    out.write("UserID,Name,Username,Premium,Plan,DaysLeft,Running,Sources,Dests,TotalFwd,TotalSpend,JoinDate,LastActive\n")
    for uid, u in db.items():
        p     = u.get("profile", {})
        name  = ((p.get("first_name","") + " " + p.get("last_name","")).strip() or "—").replace(",","")
        un    = p.get("username","—")
        prem  = u.get("premium", {})
        la    = u.get("last_active",0)
        la_s  = datetime.datetime.fromtimestamp(la).strftime("%Y-%m-%d") if la else "—"
        jt    = u.get("joined_at",0)
        jt_s  = datetime.datetime.fromtimestamp(jt).strftime("%Y-%m-%d") if jt else "—"
        spend = sum(pp.get("amount",0) for pp in GLOBAL_STATE.get("payment_history",[])
                    if str(pp.get("user_id"))==str(uid) and pp.get("status")=="approved")
        out.write(
            f"{uid},{name},{un},"
            f"{'Yes' if prem.get('active') else 'No'},"
            f"{prem.get('plan','—')},{int(max(0,(prem.get('expires_at',0)-time.time())/86400)) if prem.get('expires_at') else ('♾️' if prem.get('active') else '0')},"
            f"{'Yes' if u['settings']['running'] else 'No'},"
            f"{len(u.get('sources',[]))},{len(u.get('destinations',[]))},"
            f"{u.get('stats',{}).get('processed',0)},{spend},{jt_s},{la_s}\n"
        )
    out.seek(0)
    buf = io.BytesIO(out.getvalue().encode())
    buf.name = f"all_users_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    await event.respond("📊 **All Users Export:**", file=buf)


# ══════════════════════════════════════════════════════════════
# 📝 USER NOTES (NEW)
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(pattern=b"adm_notes_"))
async def adm_user_notes(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid   = int(event.data.decode().replace("adm_notes_", ""))
    notes = get_user_notes(uid)
    txt = f"📝 **Notes for `{uid}` — {_user_display(uid)[:25]}**\n━━━━━━━━━━━━━━━━━━━━\n"
    if not notes:
        txt += "\nKoi note nahi hai abhi।"
    else:
        for i, n in enumerate(notes):
            txt += f"\n**{i+1}.** {n['text']}\n   _by {n['by']} · {n['time']}_\n"
    btns = []
    # Delete buttons for each note
    if notes:
        del_row = []
        for i in range(len(notes)):
            del_row.append(Button.inline(f"🗑{i+1}", f"adm_del_note_{uid}_{i}".encode()))
            if len(del_row) == 4:
                btns.append(del_row)
                del_row = []
        if del_row: btns.append(del_row)
    btns.append([Button.inline("➕ Note Add Karo", f"adm_add_note_{uid}".encode())])
    btns.append([Button.inline("🔙 Profile",       f"adm_view_u_{uid}".encode())])
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(pattern=b"adm_add_note_"))
async def adm_add_note_start(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().replace("adm_add_note_", ""))
    get_user_data(event.sender_id)["step"] = f"adm_add_note_input_{uid}"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            f"📝 **Note Add Karo — User `{uid}`**\n\n"
            "Note likhke bhejo (max 200 chars):",
            buttons=[Button.inline("❌ Cancel", f"adm_notes_{uid}".encode())]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"adm_del_note_"))
async def adm_del_note(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    parts = event.data.decode().replace("adm_del_note_", "").split("_")
    uid, idx = int(parts[0]), int(parts[1])
    delete_user_note(uid, idx)
    await event.answer("✅ Note deleted!")
    await adm_user_notes(event)

@bot.on(events.CallbackQuery(data=b"adm_notes_list"))
async def adm_notes_list(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    # Collect all users with notes
    noted = []
    for uid, udata in db.items():
        notes = get_user_notes(uid)
        if notes:
            noted.append((uid, notes))

    if not noted:
        return await event.edit(
            "📝 **User Notes**\n\nAbhi kisi user ka koi note nahi.",
            buttons=[[Button.inline("🔙 Admin Panel", b"adm_main")]]
        )

    noted.sort(key=lambda x: -x[1][-1].get("ts", 0))  # most recent first

    lines = [
        "📝 **USER NOTES**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Total users with notes: **{len(noted)}**",
        "",
    ]
    btns = []
    for uid, notes in noted[:10]:
        name   = _user_display(uid)[:22]
        latest = notes[-1].get("note","")[:40]
        ts_str = _age_str(notes[-1].get("ts",0))
        lines.append(f"👤 **{name}** — {len(notes)} note(s)")
        lines.append(f"  `{uid}`  ·  {ts_str}: _{latest}_")
        btns.append([Button.inline(
            f"📝 {name[:22]} ({len(notes)})",
            f"adm_notes_{uid}".encode()
        )])

    btns.append([Button.inline("🔙 Admin Panel", b"adm_main")])
    try:
        await event.edit("\n".join(lines), buttons=btns)
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(data=b"adm_expiry_warn"))
async def adm_expiry_warnings(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    now     = time.time()
    expiring = []
    for uid, u in db.items():
        prem = u.get("premium", {})
        if not prem.get("active"): continue
        days_rem = prem.get("days_remaining", 999)
        exp_ts   = prem.get("expires_at", 0)
        if 0 <= days_rem <= 7:
            expiring.append((uid, days_rem, exp_ts, prem.get("plan","?")))

    expiring.sort(key=lambda x: x[1])  # soonest first

    if not expiring:
        return await event.edit(
            "✅ **Expiry Warnings**\n\nKoi user expiring nahi hai (7 days mein).",
            buttons=[[Button.inline("🔙 Admin Panel", b"adm_main")]]
        )

    today_exp  = [x for x in expiring if x[1] == 0]
    warn3      = [x for x in expiring if 1 <= x[1] <= 3]
    warn7      = [x for x in expiring if 4 <= x[1] <= 7]

    lines = [
        "⚠️ **PREMIUM EXPIRY WARNINGS**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Total expiring (7d): **{len(expiring)}**",
        f"🔴 Today: **{len(today_exp)}**  "
        f"🟡 1–3 days: **{len(warn3)}**  "
        f"🟢 4–7 days: **{len(warn7)}**",
        "",
    ]
    for uid, days, exp_ts, plan in expiring[:12]:
        name = _user_display(uid)[:20]
        if days == 0:
            d_str = "🔴 **TODAY**"
        elif days <= 3:
            d_str = f"🟡 {days}d left"
        else:
            d_str = f"🟢 {days}d left"
        exp_str = datetime.datetime.fromtimestamp(exp_ts).strftime("%d %b") if exp_ts else "—"
        lines.append(f"{d_str} — **{name}**")
        lines.append(f"  `{uid}` · {plan} · exp {exp_str}")

    if len(expiring) > 12:
        lines.append(f"\n_...aur {len(expiring)-12} user(s)_")

    try:
        await event.edit("\n".join(lines), buttons=[
            [Button.inline(f"📬 Remind All ({len(expiring)})", b"adm_expiry_remind_all")],
            [Button.inline("🔙 Admin Panel",                   b"adm_main")],
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_expiry_remind_all"))
async def adm_expiry_remind_all(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    await event.answer("⏳ Sending reminders...", alert=False)
    sent = 0
    for uid, u in db.items():
        prem = u.get("premium", {})
        if prem.get("active") and prem.get("days_remaining", 99) <= 7:
            days = prem.get("days_remaining", 0)
            try:
                await bot.send_message(
                    int(uid),
                    f"⚠️ **Premium Expiry Reminder**\n\n"
                    f"Tumhara premium plan **{days} din** mein expire ho jaayega!\n\n"
                    f"Renew karne ke liye: /buy"
                )
                sent += 1
                await asyncio.sleep(0.3)
            except Exception:
                pass
    add_log(event.sender_id, "Expiry Reminders Sent", details=f"{sent} users")
    await event.answer(f"✅ {sent} reminders sent!", alert=True)


# ══════════════════════════════════════════════════════════════
# ⚡ BULK ACTIONS
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_bulk_actions"))
async def adm_bulk_actions(event):
    await event.answer()
    if not is_admin(event.sender_id, "Super Admin"):
        return await event.answer("❌ Super Admin only!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    total       = len(db)
    running_cnt = sum(1 for u in db.values() if u["settings"]["running"])
    stopped_cnt = total - running_cnt
    inactive_30 = sum(1 for u in db.values() if u.get("last_active",0) < time.time()-30*86400
                      and not u.get("premium",{}).get("active"))
    no_sess     = sum(1 for u in db.values() if not u.get("session"))
    prem_cnt    = sum(1 for u in db.values() if u.get("premium",{}).get("active"))
    blocked_cnt = len(GLOBAL_STATE.get("blocked_users",[]))

    try:
        await event.edit(
            "⚡ **BULK ACTIONS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 Total: `{total}`  🟢 Running: `{running_cnt}`  🔴 Stopped: `{stopped_cnt}`\n"
            f"📡 No session: `{no_sess}`  🚫 Blocked: `{blocked_cnt}`\n"
            f"⚠️ 30d inactive (non-prem): `{inactive_30}` — cleanup eligible\n\n"
            "⚠️ **Dhyan se use karo — irreversible actions!**",
            buttons=[
                [Button.inline(f"🛑 Stop All ({running_cnt})",   b"adm_bulk_stop_all"),
                 Button.inline(f"▶️ Start All ({stopped_cnt})",  b"adm_bulk_start_all")],
                [Button.inline(f"🧹 Del Inactive ({inactive_30})", b"adm_run_cleanup"),
                 Button.inline("📢 Broadcast All",               b"adm_broadcast_menu")],
                [Button.inline("📤 Export Users CSV",             b"adm_export_users")],
                [Button.inline("🔙 User Mgmt",                    b"adm_user_mg")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_bulk_stop_all"))
async def adm_bulk_stop_all(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"):
        return await event.answer("❌ Super Admin required", alert=True)
    count = sum(1 for u in db.values() if u["settings"]["running"])
    for u in db.values(): u["settings"]["running"] = False
    save_persistent_db()
    add_log(event.sender_id, "Bulk Stop All", details=f"{count} stopped")
    await event.answer(f"✅ {count} users stopped!", alert=True)
    await adm_bulk_actions(event)

@bot.on(events.CallbackQuery(data=b"adm_bulk_start_all"))
async def adm_bulk_start_all(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"):
        return await event.answer("❌ Super Admin required", alert=True)
    from forward_engine import start_user_forwarder
    count = 0
    for uid, u in db.items():
        sess = u.get("session")
        if sess and not u["settings"]["running"]:
            u["settings"]["running"] = True
            asyncio.create_task(start_user_forwarder(int(uid), sess))
            count += 1
    save_persistent_db()
    add_log(event.sender_id, "Bulk Start All", details=f"{count} started")
    await event.answer(f"✅ {count} users started!", alert=True)
    await adm_bulk_actions(event)


# ══════════════════════════════════════════════════════════════
# USER ACTIONS
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(pattern=b"adm_tgl_blk_"))
async def adm_tgl_blk(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().split("_")[-1])
    if uid == OWNER_ID: return await event.answer("❌ Owner ko block nahi!", alert=True)
    bl  = GLOBAL_STATE.setdefault("blocked_users", [])
    if uid in bl:
        bl.remove(uid); msg = "Unblocked ✅"
    else:
        bl.append(uid); msg = "Blocked 🚫"
    save_persistent_db()
    add_log(event.sender_id, f"User {msg}", target=uid)
    await event.answer(f"User {uid} — {msg}")
    await adm_view_user(event)

@bot.on(events.CallbackQuery(pattern=b"adm_fstop_"))
async def adm_fstop(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().split("_")[-1])
    if uid in db: db[uid]["settings"]["running"] = False
    save_persistent_db()
    client = user_sessions.pop(uid, None)
    if client:
        try:
            if client.is_connected(): await client.disconnect()
        except Exception: pass
    add_log(event.sender_id, "Force Stop", target=uid)
    await event.answer(f"✅ Stopped {uid}")
    await adm_view_user(event)

@bot.on(events.CallbackQuery(pattern=b"adm_fstart_"))
async def adm_fstart(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().split("_")[-1])
    if uid not in db: return await event.answer("User not found!", alert=True)
    sess = db[uid].get("session")
    if not sess: return await event.answer("❌ Session nahi — pehle login karo.", alert=True)
    from forward_engine import start_user_forwarder
    db[uid]["settings"]["running"] = True
    save_persistent_db()
    asyncio.create_task(start_user_forwarder(uid, sess))
    add_log(event.sender_id, "Force Start", target=uid)
    await event.answer(f"✅ Started {uid}")
    await adm_view_user(event)

@bot.on(events.CallbackQuery(pattern=b"adm_reset_u_"))
async def adm_reset_user_data(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().split("_")[-1])
    if uid not in db: return await event.answer("User not found!", alert=True)
    from config import DEFAULT_SETTINGS
    client = user_sessions.pop(uid, None)
    if client:
        try:
            if client.is_connected(): await client.disconnect()
        except Exception: pass
    db[uid].update({
        "sources":[], "destinations":[], "settings": DEFAULT_SETTINGS.copy(),
        "stats":{"processed":0,"blocked":0}, "replacements":{},
        "blocked_links":{}, "link_limits":{}, "custom_forward_rules":{},
        "scheduler":{"enabled":False,"start":"09:00 AM","end":"10:00 PM","timezone":"Asia/Kolkata"},
        "temp_data":{},
    })
    db[uid]["settings"]["running"] = False
    save_persistent_db()
    add_log(event.sender_id, "Reset User Data", target=uid)
    await event.answer(f"✅ {uid} reset!", alert=True)
    await adm_view_user(event)

@bot.on(events.CallbackQuery(pattern=b"adm_del_u_"))
async def adm_del_user_confirm(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().split("_")[-1])
    if uid == OWNER_ID: return await event.answer("❌ Owner delete nahi!", alert=True)
    if uid not in db:   return await event.answer("User not found!", alert=True)
    u = db[uid]
    try:
        await event.edit(
            f"⚠️ **DELETE CONFIRM**\n\n"
            f"🆔 `{uid}` — {_user_display(uid)}\n"
            f"📦 Src: {len(u.get('sources',[]))}  📤 Dst: {len(u.get('destinations',[]))}\n"
            f"💎 Premium: {'Yes' if u.get('premium',{}).get('active') else 'No'}\n"
            f"🔐 Session: {'Yes' if u.get('session') else 'No'}\n\n"
            f"⚠️ **PERMANENT ACTION — Wapas nahi hoga!**",
            buttons=[
                [Button.inline("✅ Haan Delete", f"adm_del_confirm_{uid}".encode()),
                 Button.inline("❌ Cancel",       f"adm_view_u_{uid}".encode())],
            ]
        )
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(pattern=b"adm_del_confirm_"))
async def adm_del_user_execute(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    uid = int(event.data.decode().split("_")[-1])
    if uid == OWNER_ID: return await event.answer("❌ Owner delete nahi!", alert=True)
    if uid not in db:   return await event.answer("User not found!", alert=True)
    client = user_sessions.pop(uid, None)
    if client:
        try:
            if client.is_connected(): await client.disconnect()
        except Exception: pass
    if uid in GLOBAL_STATE.get("blocked_users",[]): GLOBAL_STATE["blocked_users"].remove(uid)
    del db[uid]
    save_persistent_db()
    add_log(event.sender_id, "DELETE USER", target=uid)
    await event.answer(f"✅ User {uid} deleted!", alert=True)
    try:
        await event.edit(f"🗑️ User `{uid}` deleted.",
                         buttons=[[Button.inline("🔙 User List", b"adm_user_mg")]])
    except Exception: pass


# ══════════════════════════════════════════════════════════════
# 📊 LIVE MONITOR
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_live"))
async def adm_live(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    total_u    = len(db)
    active_fwd = sum(1 for u in db.values() if u["settings"]["running"])
    blocked    = len(GLOBAL_STATE.get("blocked_users", []))
    prem_cnt   = sum(1 for u in db.values() if u.get("premium", {}).get("active"))
    active_s   = len(user_sessions)
    m_mode     = "🔴 ON" if GLOBAL_STATE.get("maintenance_mode") else "🟢 OFF"
    new_reg    = "🔴 Blocked" if GLOBAL_STATE.get("block_new_reg") else "🟢 Open"

    today_start = datetime.datetime.now().replace(hour=0, minute=0, second=0).timestamp()
    new_today   = sum(1 for u in db.values() if u.get("joined_at", 0) >= today_start)
    rev         = get_revenue_stats()

    today = datetime.datetime.now().strftime("%Y-%m-%d")
    today_total = sum(
        u.get("analytics", {}).get("daily", {}).get(today, {}).get("forwarded", 0)
        for u in db.values()
    )

    # Forwarding activity bars
    ratio    = active_fwd / max(total_u, 1)
    bar_fill = round(ratio * 12)
    fwd_bar  = "█" * bar_fill + "░" * (12 - bar_fill)

    # Queue stats
    queue_txt = ""
    try:
        from msg_queue import get_queue_stats
        qs  = get_queue_stats()
        pct = round(qs["pending"] / max(qs["max_size"], 1) * 10)
        qbar = "█" * pct + "░" * (10 - pct)
        queue_txt = f"  📨 Queue: [{qbar}] `{qs['pending']}/{qs['max_size']}`  ✅`{qs['processed']}` ✗`{qs['dropped']}`\n"
    except Exception:
        pass

    # Top users today
    top_users = sorted(
        [(uid, u.get("analytics", {}).get("daily", {}).get(today, {}).get("forwarded", 0))
         for uid, u in db.items()
         if u.get("analytics", {}).get("daily", {}).get(today, {}).get("forwarded", 0) > 0],
        key=lambda x: x[1], reverse=True
    )[:5]
    top_txt = ""
    if top_users:
        medals = ["🥇", "🥈", "🥉", "4.", "5."]
        top_txt = "\n**🏆 Top Users Today:**\n" + "".join(
            f"  {medals[i]} `{uid}` — `{v}` msgs\n"
            for i, (uid, v) in enumerate(top_users)
        )

    # Recent logs
    recent  = admin_logs[-4:]
    log_txt = ""
    if recent:
        log_txt = "\n**📋 Recent Actions:**\n" + "".join(
            f"  • `{l.get('action','?')}` → `{str(l.get('target',''))[:20]}`\n"
            for l in recent
        )

    # Session health
    sess_ratio = active_s / max(total_u, 1) * 100
    sess_health = "🟢 Good" if sess_ratio > 30 else ("🟡 Low" if sess_ratio > 10 else "🔴 Very Low")

    # Pending alerts
    pending_pay = len([p for p in GLOBAL_STATE.get("pending_payments", {}).values()
                       if p.get("status") == "pending"])
    alert_txt = ""
    if pending_pay:
        alert_txt += f"  💳 **{pending_pay} payment(s)** need approval\n"

    from time_helper import ab_now
    now = ab_now(event.sender_id)

    txt = (
        "📊 **LIVE MONITOR**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 `{now.strftime('%H:%M:%S %d/%m/%Y')}`\n"
        + (f"\n⚡ **ALERTS:**\n{alert_txt}" if alert_txt else "")
        + "\n**👤 Users:**\n"
        f"  Total: `{total_u}`  🆕 Aaj: `+{new_today}`\n"
        f"  ⚡ Forwarding: [{fwd_bar}] `{active_fwd}/{total_u}`\n"
        f"  💎 Premium: `{prem_cnt}`  🚫 Blocked: `{blocked}`\n"
        f"  📡 Sessions: `{active_s}` ({sess_health})\n"
        f"\n**📨 Messages Aaj:** `{today_total}`\n"
        + (queue_txt)
        + f"\n**💰 Revenue:**  Aaj: `₹{rev['today']}`  Month: `₹{rev['month']}`\n"
        f"\n**🔧 System:**  Maintenance: {m_mode}  NewReg: {new_reg}\n"
        f"  RAM Est: `~{active_s * 1.5:.0f}MB`"
        + top_txt + log_txt
    )

    try:
        await event.edit(txt, buttons=[
            [Button.inline("🔄 Refresh",      b"adm_live"),
             Button.inline("📈 Analytics",    b"adm_analytics")],
            [Button.inline("💰 Revenue",      b"adm_revenue"),
             Button.inline("👥 Users",        b"adm_user_mg")],
            [Button.inline("🤖 Workers",      b"adm_workers"),
             Button.inline("📁 Logs",         b"adm_logs")],
            [Button.inline("🛠 Bot Control",  b"adm_bot_ctrl"),
             Button.inline("🔙 Admin Panel",  b"adm_main")],
        ])
    except errors.MessageNotModifiedError:
        pass

# ══════════════════════════════════════════════════════════════
# 🛠 BOT CONTROL
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_bot_ctrl"))
async def adm_bot_ctrl(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"):
        return await event.answer("❌ Super Admin required", alert=True)
    m_mode     = GLOBAL_STATE.get("maintenance_mode", False)
    no_new_reg = GLOBAL_STATE.get("block_new_reg", False)
    cleanup_en = CLEANUP_CONFIG.get("enabled", True)
    txt = (
        "🛠 **Bot Control**\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔧 Maintenance: {'🔴 ON' if m_mode else '🟢 OFF'}\n"
        f"📡 Sessions: `{len(user_sessions)}/{len(db)}`\n"
        f"🚪 New Reg: {'🔴 Blocked' if no_new_reg else '🟢 Open'}\n"
        f"🧹 Auto Cleanup: {'✅' if cleanup_en else '❌'} ({CLEANUP_CONFIG.get('inactive_days',30)}d)\n"
    )
    btns = [
        [Button.inline("🟢 Maint OFF" if m_mode else "🔴 Maint ON",    b"adm_tgl_maint"),
         Button.inline("🟢 Allow Reg" if no_new_reg else "🚪 Block Reg", b"adm_tgl_newreg")],
        [Button.inline("📢 Broadcast",        b"adm_broadcast_menu"),
         Button.inline("🔄 Sync Sessions",    b"adm_sync_sessions")],
        [Button.inline("🧹 Cleanup Settings", b"adm_cleanup_panel"),
         Button.inline("🔔 Alerts",           b"adm_alert_dest")],
        [Button.inline("🔙 Admin Panel",      b"adm_main")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(data=b"adm_tgl_maint"))
async def adm_tgl_maint(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    GLOBAL_STATE["maintenance_mode"] = not GLOBAL_STATE.get("maintenance_mode", False)
    status = "ON 🔴" if GLOBAL_STATE["maintenance_mode"] else "OFF 🟢"
    add_log(event.sender_id, f"Maintenance {status}")
    save_persistent_db()
    await event.answer(f"Maintenance {status}")
    await adm_bot_ctrl(event)

@bot.on(events.CallbackQuery(data=b"adm_maint"))
async def adm_maint_handler(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    await adm_tgl_maint(event)

@bot.on(events.CallbackQuery(data=b"adm_tgl_newreg"))
async def adm_tgl_newreg(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    GLOBAL_STATE["block_new_reg"] = not GLOBAL_STATE.get("block_new_reg", False)
    status = "Blocked 🔴" if GLOBAL_STATE["block_new_reg"] else "Open 🟢"
    add_log(event.sender_id, f"New Reg {status}")
    save_persistent_db()
    await event.answer(f"New Reg: {status}")
    await adm_bot_ctrl(event)

@bot.on(events.CallbackQuery(data=b"adm_sync_sessions"))
async def adm_sync_sessions(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    await event.answer("⏳ Syncing...", alert=False)
    from forward_engine import start_user_forwarder
    count = 0
    for uid, udata in db.items():
        sess = udata.get("session")
        if sess and udata.get("settings",{}).get("running") and int(uid) not in user_sessions:
            asyncio.create_task(start_user_forwarder(int(uid), sess))
            count += 1
    add_log(event.sender_id, "Sync Sessions", details=f"{count} restarted")
    try:
        await event.edit(
            f"✅ **Sync Done!**\n🔄 Restarted: `{count}`  📡 Live: `{len(user_sessions)}`",
            buttons=[[Button.inline("🔙 Bot Control", b"adm_bot_ctrl")]]
        )
    except errors.MessageNotModifiedError: pass


# ══════════════════════════════════════════════════════════════
# 📢 BROADCAST CENTER (UPGRADED — Delivery Reports)
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_broadcast_menu"))
async def adm_broadcast_menu(event):
    await event.answer()
    if not is_admin(event.sender_id, "Super Admin"):
        return await event.answer("❌ Super Admin required", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    total   = len(db)
    premium = sum(1 for u in db.values() if u.get("premium", {}).get("active"))
    free    = total - premium
    active  = sum(1 for u in db.values() if u["settings"]["running"])

    last_bc = get_last_broadcast()
    last_line = ""
    if last_bc:
        last_line = (
            f"\n📊 **Last Broadcast:**\n"
            f"  Target: `{last_bc.get('target','?')}` · "
            f"✅`{last_bc.get('sent',0)}` ❌`{last_bc.get('failed',0)}` · "
            f"`{last_bc.get('time','?')}`"
        )

    txt = (
        "📢 **BROADCAST**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Message likhne se pehle target chuniye:\n\n"
        f"👤 All Users: **{total}**\n"
        f"💎 Premium only: **{premium}**\n"
        f"🆓 Free only: **{free}**\n"
        f"⚡ Active (forwarding): **{active}**"
        f"{last_line}"
    )

    from database import get_user_data as _gud
    d = _gud(event.sender_id)
    d["step"] = "bc_msg_input"
    d["step_since"] = time.time()
    d.pop("bc_target", None)

    try:
        await event.edit(txt, buttons=[
            [Button.inline(f"👤 All ({total})",       b"adm_bc_all"),
             Button.inline(f"💎 Premium ({premium})",  b"adm_bc_premium")],
            [Button.inline(f"🆓 Free ({free})",        b"adm_bc_free"),
             Button.inline(f"⚡ Active ({active})",    b"adm_bc_active")],
            [Button.inline("👤 One User",              b"adm_bc_one"),
             Button.inline("📌 Pin to All",            b"adm_bc_pin")],
            [Button.inline("🔙 Admin Panel",           b"adm_main")],
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_bc_all"))
async def adm_bc_all(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_bc_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("📢 **Broadcast — Sab Users**\n\nMessage bhejo (HTML ok):",
                         buttons=[Button.inline("❌ Cancel", b"adm_broadcast_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_bc_premium"))
async def adm_bc_premium(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_bc_premium_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("💎 **Broadcast — Premium Only**\n\nMessage bhejo:",
                         buttons=[Button.inline("❌ Cancel", b"adm_broadcast_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_bc_free"))
async def adm_bc_free(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_bc_free_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("🆓 **Broadcast — Free Users Only**\n\nMessage bhejo:",
                         buttons=[Button.inline("❌ Cancel", b"adm_broadcast_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_bc_active"))
async def adm_bc_active(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_bc_active_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("🟢 **Broadcast — Active Users Only**\n\nMessage bhejo:",
                         buttons=[Button.inline("❌ Cancel", b"adm_broadcast_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_bc_one"))
async def adm_bc_one(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_bc_one_uid"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("👤 **Specific User ko Message**\n\nPehle User ID bhejo:",
                         buttons=[Button.inline("❌ Cancel", b"adm_broadcast_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_bc_pin"))
async def adm_bc_pin(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_bc_pin_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "📌 **Pinned Announcement**\n\nSab users ko jayega + pin hoga.\n\nMessage bhejo:",
            buttons=[Button.inline("❌ Cancel", b"adm_broadcast_menu")]
        )
    except errors.MessageNotModifiedError:
        pass

async def _do_broadcast(admin_id, message_html: str, target: str, filter_fn=None) -> tuple[int, int]:
    """Broadcast helper — returns (sent, failed). Records result in GLOBAL_STATE."""
    sent = failed = 0
    for uid, u in db.items():
        if filter_fn and not filter_fn(u):
            continue
        try:
            await bot.send_message(int(uid), message_html, parse_mode="html")
            sent += 1
        except Exception:
            failed += 1
        if (sent + failed) % 20 == 0:
            await asyncio.sleep(0.5)   # Rate limit — 40 msgs/sec max
    record_broadcast_result(sent, failed, target)
    add_log(admin_id, "Broadcast", target=target, details=f"✅{sent} ❌{failed}")
    return sent, failed


# ══════════════════════════════════════════════════════════════
# 🔐 ADMIN MANAGEMENT
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_mgmt"))
async def adm_mgmt_handler(event):
    await event.answer()
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    admins_dict  = GLOBAL_STATE.get("admins", {})
    role_icons   = {"Owner": "👑", "Super Admin": "🔴", "Moderator": "🟡", "Support": "🟢"}

    lines = [
        "🔐 **ADMIN MANAGEMENT**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Total admins: **{len(admins_dict) + 1}** (including Owner)",
        "",
    ]
    # Owner first
    try:
        op = db.get(int(OWNER_ID), {}).get("profile", {})
        oname = (op.get("first_name","") + " " + op.get("last_name","")).strip() or f"@{op.get('username','')}" or str(OWNER_ID)
    except Exception:
        oname = str(OWNER_ID)
    lines.append(f"👑 **{oname}** — Owner (You)")
    lines.append(f"  `{OWNER_ID}`")
    lines.append("")

    for aid, role in admins_dict.items():
        try:
            p     = db.get(int(aid), {}).get("profile", {})
            fname = p.get("first_name", "") or ""
            uname = p.get("username", "")
            name  = fname or (f"@{uname}" if uname else "—")
            handle = f"  @{uname}" if uname else ""
        except Exception:
            name   = "—"
            handle = ""
        icon = role_icons.get(role, "⚪")
        lines.append(f"{icon} **{name}** — {role}{handle}")
        lines.append(f"  `{aid}`")

    txt = "\n".join(lines)
    btns = [
        [Button.inline("➕ Add Admin",   b"adm_add_admin"),
         Button.inline("🗑 Remove",      b"adm_rem_admin")],
        [Button.inline("📋 Role Guide",  b"adm_role_guide"),
         Button.inline("🔙 Admin Panel", b"adm_main")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(data=b"adm_role_guide"))
async def adm_role_guide(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    try:
        await event.edit(
            "📋 **Admin Role Guide**\n━━━━━━━━━━━━━━━━━━━━\n\n"
            "🟢 **Support** — Users dekho, logs dekho\n"
            "🟡 **Moderator** — Block/unblock, premium de/lo\n"
            "🔴 **Super Admin** — Bot control, broadcast, delete users\n"
            "👑 **Owner** — Sab kuch (sirf ek ho sakta hai)\n",
            buttons=[[Button.inline("🔙 Back", b"adm_mgmt")]]
        )
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(data=b"adm_add_admin"))
async def adm_add_admin(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_add_admin_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "➕ **Add Admin**\n\n"
            "Format: `USER_ID ROLE`\n\n"
            "Roles: `Support` | `Moderator` | `Super Admin`\n\n"
            "Example: `5768614596 Support`",
            buttons=[Button.inline("🔙 Back", b"adm_mgmt")]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_rem_admin"))
async def adm_rem_admin(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admins = GLOBAL_STATE.get("admins", {})
    btns   = []
    for aid, role in admins.items():
        if int(str(aid)) == OWNER_ID: continue
        btns.append([Button.inline(f"🗑 {_user_display(int(aid))[:25]} ({role})",
                                   f"adm_rem_admin_id_{aid}".encode())])
    if not btns:
        return await event.answer("Koi removable admin nahi.", alert=True)
    btns.append([Button.inline("🔙 Back", b"adm_mgmt")])
    try:
        await event.edit("🗑 **Kise remove karna hai?**", buttons=btns)
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(pattern=b"adm_rem_admin_id_"))
async def adm_rem_admin_by_btn(event):
    await event.answer()
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    aid    = event.data.decode().replace("adm_rem_admin_id_", "")
    admins = GLOBAL_STATE.get("admins", {})
    if aid in admins:
        del admins[aid]
        save_persistent_db()
        add_log(event.sender_id, "Remove Admin", target=aid)
        await event.answer(f"✅ Admin `{aid}` removed!", alert=True)
    else:
        await event.answer("Already removed.", alert=True)
    await adm_mgmt_handler(event)


# ══════════════════════════════════════════════════════════════
# 📁 LOGS (UPGRADED — Filter by type)
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_logs"))
async def adm_logs_view(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    await _show_logs_page(event, 0, "all")

async def _show_logs_page(event, page, log_filter="all"):
    PER  = 8
    all_logs = admin_logs[::-1]  # Latest first

    if log_filter != "all":
        filtered = [l for l in all_logs if log_filter.lower() in l.get("action","").lower()]
    else:
        filtered = all_logs

    total   = len(filtered)
    s_idx   = page * PER
    e_idx   = s_idx + PER
    sub     = filtered[s_idx:e_idx]

    if not sub:
        try:
            return await event.edit(
                "📁 **Admin Logs**\n\nKoi log nahi.",
                buttons=[[Button.inline("🔙 Back", b"adm_main")]]
            )
        except errors.MessageNotModifiedError: return

    # Summary stats for header
    action_counts = {}
    for l in all_logs[-50:]:  # last 50
        a = l.get("action","?")
        action_counts[a] = action_counts.get(a, 0) + 1
    top_actions = sorted(action_counts.items(), key=lambda x: -x[1])[:3]
    top_str = "  ".join(f"`{a}:{c}`" for a, c in top_actions)

    filter_label = {
        "all":"All","premium":"💎 Premium","block":"🚫 Block",
        "broadcast":"📢 Broadcast","admin":"🔐 Admin"
    }.get(log_filter, log_filter)

    lines = [
        "📁 **ADMIN LOGS**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Filter: **{filter_label}**  ({s_idx+1}–{min(e_idx,total)} of {total})",
        f"Top actions: {top_str}",
        "",
    ]

    for l in sub:
        try:
            aname = _user_display(int(l["admin"]))[:14] if str(l["admin"]).isdigit() else str(l["admin"])
        except Exception:
            aname = str(l.get("admin","?"))
        detail = f"\n   └ `{l['details'][:50]}`" if l.get("details") else ""
        lines.append(
            f"🕒 `{l['time'][:16]}`\n"
            f"👤 {aname} → **{l['action']}**\n"
            f"🎯 `{str(l.get('target',''))[:30]}`{detail}"
        )

    nav = []
    if page > 0:      nav.append(Button.inline("⬅️", f"adm_logs_p_{page-1}_{log_filter}".encode()))
    if e_idx < total: nav.append(Button.inline("➡️", f"adm_logs_p_{page+1}_{log_filter}".encode()))

    btns = []
    if nav: btns.append(nav)
    btns.append([
        Button.inline("All",       b"adm_logs_f_all"),
        Button.inline("💎 Prem",   b"adm_logs_f_premium"),
        Button.inline("🚫 Block",  b"adm_logs_f_block"),
        Button.inline("📢 BC",     b"adm_logs_f_broadcast"),
        Button.inline("🔐 Admin",  b"adm_logs_f_admin"),
    ])
    btns.append([
        Button.inline("📥 Export CSV",  b"adm_dl_logs"),
        Button.inline("🗑 Clear Logs",  b"adm_clear_logs"),
        Button.inline("🔙 Back",        b"adm_main"),
    ])
    try:
        await event.edit("\n".join(lines), buttons=btns)
    except errors.MessageNotModifiedError: pass

@bot.on(events.CallbackQuery(pattern=b"adm_logs_p_"))
async def adm_logs_paged(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    parts = event.data.decode().replace("adm_logs_p_", "").split("_")
    page  = int(parts[0])
    filt  = parts[1] if len(parts) > 1 else "all"
    await _show_logs_page(event, page, filt)

@bot.on(events.CallbackQuery(pattern=b"adm_logs_f_"))
async def adm_logs_filter(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    filt = event.data.decode().replace("adm_logs_f_", "")
    await _show_logs_page(event, 0, filt)

@bot.on(events.CallbackQuery(data=b"adm_dl_logs"))
async def adm_dl_logs(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    out = io.StringIO()
    out.write("Time,Admin,Action,Target,Details\n")
    for l in admin_logs:
        out.write(f"{l['time']},{l['admin']},{l['action']},{l['target']},"
                  f"{str(l.get('details','')).replace(',',';')}\n")
    out.seek(0)
    buf = io.BytesIO(out.getvalue().encode())
    buf.name = f"admin_logs_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    await event.respond("📂 **Audit Log:**", file=buf)
    await event.answer("Sent!")

@bot.on(events.CallbackQuery(data=b"adm_clear_logs"))
async def adm_clear_logs(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    count = len(admin_logs)
    admin_logs.clear()
    save_persistent_db()
    await event.answer(f"✅ {count} logs cleared!", alert=True)
    await adm_logs_view(event)


# ══════════════════════════════════════════════════════════════
# 🤖 WORKERS
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_workers"))
async def adm_workers_panel(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    from worker_manager import get_worker_status, get_total_active_sessions
    from config import TOTAL_WORKERS, MAX_USERS_PER_WORKER
    workers    = get_worker_status()
    active     = get_total_active_sessions()
    cap        = TOTAL_WORKERS * MAX_USERS_PER_WORKER
    cap_pct    = round(active / max(cap, 1) * 100)
    cap_bar_f  = round(active / max(cap, 1) * 12)
    cap_bar    = "█" * cap_bar_f + "░" * (12 - cap_bar_f)
    health     = "🟢 Healthy" if cap_pct < 70 else ("🟡 Moderate" if cap_pct < 90 else "🔴 Near Capacity")

    lines = [
        "🤖 **WORKER MANAGEMENT**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Capacity: [{cap_bar}] `{active}/{cap}` ({cap_pct}%) — {health}",
        f"Workers: **{TOTAL_WORKERS}**  Sessions: **{active}**",
        "",
    ]
    for w in workers:
        pct   = round(w["users"] / max(w["capacity"], 1) * 10)
        bar   = "█" * pct + "░" * (10 - pct)
        icon  = "🟢" if w["alive"] else "🔴"
        load  = "🔥 HIGH" if w["users"] >= w["capacity"] * 0.9 else ""
        lines.append(
            f"{icon} **W{w['id']}** [{bar}] `{w['users']}/{w['capacity']}` {load}"
        )
        lines.append(f"   Last ping: `{w['last_seen']}`")

    lines += [
        "",
        "**Scale guide:** 1–100 → 1 worker · 100–300 → 3 · 300–600 → 6",
        "_Change `TOTAL_WORKERS` env var to scale_",
    ]

    try:
        await event.edit("\n".join(lines), buttons=[
            [Button.inline("🔄 Refresh",          b"adm_workers"),
             Button.inline("⚖️ Rebalance",        b"adm_worker_rebalance")],
            [Button.inline("📋 User→Worker Map",  b"adm_worker_map_0")],
            [Button.inline("🔙 Admin Panel",       b"adm_main")],
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"adm_worker_rebalance"))
async def adm_worker_rebalance(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    from worker_manager import rebalance_workers
    moved = rebalance_workers()
    add_log(event.sender_id, "Rebalance Workers", details=f"{moved} moved")
    await event.answer(f"✅ {moved} users rebalanced!", alert=True)
    await adm_workers_panel(event)

@bot.on(events.CallbackQuery(data=b"adm_rebalance_workers"))
async def adm_rebalance_cb(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    await adm_worker_rebalance(event)

@bot.on(events.CallbackQuery(pattern=b"adm_worker_map_"))
async def adm_worker_map(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    try: page = int(event.data.decode().replace("adm_worker_map_",""))
    except: page = 0
    MAX   = 15
    start = page * MAX
    ul    = sorted(
        [(int(uid), udata.get("assigned_worker","?"), udata.get("settings",{}).get("running",False))
         for uid, udata in db.items() if udata.get("session")],
        key=lambda x: (x[1] if x[1]!="?" else 99, x[0])
    )
    sub = ul[start:start+MAX]
    if not sub:
        try: return await event.edit("❌ Koi session user nahi.", buttons=[[Button.inline("🔙",b"adm_workers")]])
        except: return
    lines = ["📋 **User → Worker Map**","━━━━━━━━━━━━━━━━━━"]
    for uid, wid, run in sub:
        lines.append(f"{'🟢' if run else '🔴'} `{uid}` → Worker **{wid}**")
    nav = []
    if page > 0:            nav.append(Button.inline("⬅️", f"adm_worker_map_{page-1}".encode()))
    if start+MAX < len(ul): nav.append(Button.inline("➡️", f"adm_worker_map_{page+1}".encode()))
    btns = []
    if nav: btns.append(nav)
    btns.append([Button.inline("🔙 Workers", b"adm_workers")])
    try:
        await event.edit("\n".join(lines), buttons=btns)
    except errors.MessageNotModifiedError: pass


# ══════════════════════════════════════════════════════════════
# 🧹 CLEANUP
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_cleanup_panel"))
async def adm_cleanup_panel(event):
    await event.answer()
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    enabled = CLEANUP_CONFIG.get("enabled", True)
    days    = CLEANUP_CONFIG.get("inactive_days", 30)
    cutoff  = int(time.time()) - (days * 86400)
    total   = len(db)

    # Breakdown of what will be deleted
    eligible     = []
    no_sess      = 0
    never_active = 0
    for uid, u in db.items():
        if u.get("last_active", 0) < cutoff and not u.get("premium", {}).get("active"):
            eligible.append(uid)
            if uid not in user_sessions:
                no_sess += 1
            if not u.get("last_active"):
                never_active += 1

    n = len(eligible)
    pct = round(n / max(total, 1) * 100)
    bar_f = round(n / max(total, 1) * 12)
    bar = "█" * bar_f + "░" * (12 - bar_f)

    try:
        await event.edit(
            "🧹 **AUTO CLEANUP**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"👤 Total Users: `{total}`\n"
            f"⏰ Threshold: `{days}` days inactive\n"
            f"⚙️ Auto Cleanup: {'✅ ON' if enabled else '❌ OFF'}\n\n"
            f"**Will be deleted:** [{bar}] `{n}` ({pct}%)\n"
            f"  📡 No session: `{no_sess}`\n"
            f"  ❓ Never active: `{never_active}`\n"
            f"  💎 Premium = always protected\n\n"
            "Set threshold (days):",
            buttons=[
                [Button.inline("7d",  b"adm_setclean_7"),  Button.inline("14d", b"adm_setclean_14"),
                 Button.inline("30d", b"adm_setclean_30"),  Button.inline("60d", b"adm_setclean_60"),
                 Button.inline("90d", b"adm_setclean_90")],
                [Button.inline("🔴 Disable Auto" if enabled else "🟢 Enable Auto", b"adm_tgl_cleanup")],
                [Button.inline(f"⚡ Run Now — Delete {n} users", b"adm_run_cleanup")],
                [Button.inline("🗂 Storage Manager", b"adm_storage_manager")],
                [Button.inline("🔙 Admin Panel", b"adm_main")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"adm_setclean_"))
async def adm_setclean(event):
    await event.answer()
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    days = int(event.data.decode().split("_")[-1])
    CLEANUP_CONFIG["inactive_days"] = days
    add_log(event.sender_id, "Cleanup Threshold", details=f"{days}d")
    await event.answer(f"✅ {days} days")
    await adm_cleanup_panel(event)

@bot.on(events.CallbackQuery(data=b"adm_tgl_cleanup"))
async def adm_tgl_cleanup(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    CLEANUP_CONFIG["enabled"] = not CLEANUP_CONFIG.get("enabled", True)
    status = "ON ✅" if CLEANUP_CONFIG["enabled"] else "OFF ❌"
    add_log(event.sender_id, f"Auto Cleanup {status}")
    await event.answer(f"Cleanup {status}")
    await adm_cleanup_panel(event)

@bot.on(events.CallbackQuery(data=b"adm_run_cleanup"))
async def adm_run_cleanup(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id, "Super Admin"): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    await event.answer("⏳ Running...", alert=False)
    result = cleanup_inactive_users()
    count  = result["count"]
    add_log(event.sender_id, "Manual Cleanup", details=f"Deleted {count}")
    try:
        await event.edit(
            f"✅ **Cleanup Done!**\n🗑 Deleted: `{count}`  Remaining: `{len(db)}`",
            buttons=[[Button.inline("🔙 Back", b"adm_cleanup_panel")]]
        )
    except errors.MessageNotModifiedError: pass


# ══════════════════════════════════════════════════════════════
# 📍 SOURCE TRACKING
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_source_tracking"))
async def adm_source_tracking_handler(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    from source_tracker import get_source_stats, get_tracker_data
    tracker  = get_tracker_data()
    enabled  = tracker.get("enabled", True)
    stats_txt = get_source_stats()
    try:
        await event.edit(
            f"📍 **Source Tracking**\n━━━━━━━━━━━━━━━━━━━━\n"
            f"Status: {'🟢 ON' if enabled else '🔴 OFF'}\n\n{stats_txt}",
            buttons=[
                [Button.inline("🔴 Band Karo" if enabled else "🟢 Chalu Karo", b"adm_src_track_toggle")],
                [Button.inline("🗑 Stats Reset", b"adm_src_track_reset")],
                [Button.inline("🔙 Admin Panel", b"adm_main")],
            ]
        )
    except errors.MessageNotModifiedError:
        await event.answer("Up to date.")

@bot.on(events.CallbackQuery(data=b"adm_src_track_toggle"))
async def adm_src_track_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    from source_tracker import get_tracker_data
    tracker = get_tracker_data()
    tracker["enabled"] = not tracker.get("enabled", True)
    save_persistent_db()
    await event.answer(f"✅ Tracking {'ON' if tracker['enabled'] else 'OFF'}")
    await adm_source_tracking_handler(event)

@bot.on(events.CallbackQuery(data=b"adm_src_track_reset"))
async def adm_src_track_reset(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    from source_tracker import get_tracker_data
    get_tracker_data()["sources"] = {}
    save_persistent_db()
    await event.answer("✅ Reset!")
    await adm_source_tracking_handler(event)

@bot.on(events.CallbackQuery(data=b"adm_feature_flags_2"))
async def adm_feature_flags_alias(event):
    """Alias — Feature Flags panel open karo."""
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)  # ✅ FIX: Admin left main panel
    admin_mark_activity(event.sender_id)
    try:
        from ui.feature_flags_menu import flags_main
        await flags_main(event)
    except Exception as e:
        await event.answer(f"Error: {str(e)[:80]}", alert=True)


# ─────────────────────────────────────────────────────────────────────────────
# MONETIZATION HUB — clearly explains and routes Ads vs Sponsor Campaigns
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_monetize_hub"))
async def adm_monetize_hub(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    # Fetch quick stats from both engines
    try:
        import ads_engine as AE
        ae = AE.get_earnings()
        ads_line = (
            f"  ⚡ {'🟢 ON' if ae['enabled'] else '🔴 OFF'}  ·  "
            f"Ads: `{ae['active_ads']}/{ae['total_ads']}`  ·  "
            f"Earned: `₹{ae['total_earned']}`"
        )
    except Exception:
        ads_line = "  (Stats unavailable)"

    try:
        import promo_engine as PE
        ps = PE.get_promo_summary()
        promo_line = (
            f"  🟢 Active: `{ps['active_campaigns']}`  ·  "
            f"Pending: `{ps['pending_payment']}`  ·  "
            f"Earned: `₹{ps['total_revenue']}`"
        )
        alert = ""
        if ps["pending_approval"] > 0:
            alert += f"\n  ⚠️ {ps['pending_approval']} campaigns need approval"
        if ps["pending_inquiries"] > 0:
            alert += f"\n  📬 {ps['pending_inquiries']} sponsor inquiries unanswered"
    except Exception:
        promo_line = "  (Stats unavailable)"
        alert = ""

    text = (
        "💰 **MONETIZATION HUB**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        "**📢 Ads Engine** — _Bot ke andar automated ads_\n"
        "```\n"
        "Kya karta hai:\n"
        "  • Tum khud ads banate ho (banner/popup/blast)\n"
        "  • Ye ads automatically sabhi users ko dikhte hain\n"
        "  • Rotation, frequency, CPM — sab tum control karte ho\n"
        "  • Apne hi ads hain — koi external sponsor nahi\n"
        "```\n"
        f"{ads_line}\n\n"

        "**📣 Sponsor Campaigns** — _External sponsors ko manage karo_\n"
        "```\n"
        "Kya karta hai:\n"
        "  • Koi baahar se tumhare bot pe advertise karna chahta hai\n"
        "  • /promote se inquiry aati hai → tum deal karo\n"
        "  • Package set karo, payment lo, campaign approve karo\n"
        "  • Real external revenue — sponsors tumhe PAISA dete hain\n"
        "```\n"
        f"{promo_line}{alert}\n\n"

        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "💡 **Ek line mein:**\n"
        "📢 Ads Engine = Tum khud ads chalate ho apne bot mein\n"
        "📣 Sponsor = Koi aur tumhare bot pe apne ads chalata hai aur PAISA deta hai"
    )

    try:
        await event.edit(text, buttons=[
            [Button.inline("📢 Ads Engine",         b"ads_panel")],
            [Button.inline("📣 Sponsor Campaigns",   b"promo_panel")],
            [Button.inline("🔙 Admin Panel",          b"adm_main")],
        ])
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════════════════════════
# FIX 1: MENU CUSTOMIZER — Button On/Off + Naam Change
# ══════════════════════════════════════════════════════════════

# All customizable main menu buttons with default labels
_MENU_BUTTONS_LIST = [
    ("start_stop",    "🟢/🔴 Start/Stop"),
    ("dashboard",     "📊 Dashboard"),
    ("source",        "➕ Source"),
    ("dest",          "📤 Destination"),
    ("per_dest",      "📤 Per-Dest"),
    ("settings",      "⚙️ Settings"),
    ("src_config",    "📍 Src Config"),
    ("filters",       "🧠 Filters"),
    ("replacements",  "🔄 Replacements"),
    ("scheduler",     "⏰ Scheduler"),
    ("per_day",       "📅 Per-Day"),
    ("translation",   "🌐 Translation"),
    ("watermark",     "🖼️ Watermark"),
    ("affiliate",     "🔗 Affiliate"),
    ("backup",        "💾 Backup"),
    ("start_end_msg", "✏️ Start/End Msg"),
    ("templates",     "📋 Templates"),
    ("premium",       "💎 Premium"),
    ("earn",          "🎁 Earn & Rewards"),
    ("advertise",     "📣 Advertise"),
    ("help",          "❓ Help"),
    ("commands",      "📋 Commands"),
]


def _get_menu_cfg():
    return GLOBAL_STATE.setdefault("menu_config", {})


@bot.on(events.CallbackQuery(data=b"adm_menu_customizer"))
async def adm_menu_customizer(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    cfg = _get_menu_cfg()

    lines = []
    for key, default_label in _MENU_BUTTONS_LIST:
        btn_cfg = cfg.get(key, {})
        visible = btn_cfg.get("visible", True)
        label   = btn_cfg.get("label", default_label)
        status  = "🟢" if visible else "🔴"
        lines.append(f"{status} `{label}`")

    try:
        await event.edit(
            "🎨 **MENU CUSTOMIZER**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Main menu ke buttons ko ON/OFF karo ya naam badlo.\n\n"
            "**Current Status:**\n" + "\n".join(lines) + "\n\n"
            "💡 Neeche button select karo modify karne ke liye:",
            buttons=[
                [Button.inline("👁 Visibility Toggle",   b"adm_mc_visibility"),
                 Button.inline("✏️ Naam Badlo",          b"adm_mc_rename")],
                [Button.inline("♻️ Sab Reset Karo",      b"adm_mc_reset")],
                [Button.inline("🔙 Admin Panel",          b"adm_main")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_mc_visibility"))
async def adm_mc_visibility(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    cfg = _get_menu_cfg()
    btns = []
    for key, default_label in _MENU_BUTTONS_LIST:
        btn_cfg = cfg.get(key, {})
        visible = btn_cfg.get("visible", True)
        status  = "🟢 ON" if visible else "🔴 OFF"
        label   = btn_cfg.get("label", default_label)
        btns.append([Button.inline(
            f"{status}  |  {label[:22]}",
            f"adm_mc_toggle|{key}".encode()
        )])
    btns.append([Button.inline("🔙 Back", b"adm_menu_customizer")])
    try:
        await event.edit(
            "👁 **VISIBILITY TOGGLE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Button dabao ON/OFF karne ke liye:",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_mc_toggle\\|(.+)"))
async def adm_mc_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    key = event.data.decode().split("|")[1]
    cfg = _get_menu_cfg()
    btn_cfg = cfg.setdefault(key, {})
    btn_cfg["visible"] = not btn_cfg.get("visible", True)
    save_persistent_db()
    status = "🟢 ON" if btn_cfg["visible"] else "🔴 OFF"
    await event.answer(f"'{key}' → {status}", alert=False)
    await adm_mc_visibility(event)


@bot.on(events.CallbackQuery(data=b"adm_mc_rename"))
async def adm_mc_rename(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    cfg = _get_menu_cfg()
    btns = []
    for key, default_label in _MENU_BUTTONS_LIST:
        btn_cfg = cfg.get(key, {})
        current = btn_cfg.get("label", default_label)
        btns.append([Button.inline(
            f"✏️ {current[:30]}",
            f"adm_mc_rename_pick|{key}".encode()
        )])
    btns.append([Button.inline("🔙 Back", b"adm_menu_customizer")])
    try:
        await event.edit(
            "✏️ **NAAM BADLO**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kaun sa button rename karna hai?",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_mc_rename_pick\\|(.+)"))
async def adm_mc_rename_pick(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    key = event.data.decode().split("|")[1]
    data = get_user_data(event.sender_id)
    data["step"] = f"adm_mc_rename_input|{key}"
    data["step_since"] = time.time()
    cfg = _get_menu_cfg()
    cur = cfg.get(key, {}).get("label", key)
    try:
        await event.edit(
            f"✏️ **'{key}' ka naya naam type karo:**\n\n"
            f"Current: `{cur}`\n\n"
            "_(Max 30 characters, emoji allowed)_",
            buttons=[[Button.inline("❌ Cancel", b"adm_mc_rename")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        isinstance(get_user_data(e.sender_id).get("step"), str) and
        get_user_data(e.sender_id).get("step", "").startswith("adm_mc_rename_input|")))
async def adm_mc_rename_handler(event):
    uid = event.sender_id
    if not is_admin(uid): return
    data = get_user_data(uid)
    step = data.get("step", "")
    key  = step.split("|")[1] if "|" in step else ""
    data["step"] = None
    new_name = event.raw_text.strip()[:30]
    if key:
        cfg = _get_menu_cfg()
        cfg.setdefault(key, {})["label"] = new_name
        save_persistent_db()
        add_log(uid, "Menu Customizer", details=f"Renamed '{key}' → '{new_name}'")
    await event.respond(
        f"✅ **'{key}' ab '{new_name}' dikhega!**",
        buttons=[[Button.inline("🎨 Menu Customizer", b"adm_menu_customizer"),
                  Button.inline("🏠 Admin", b"adm_main")]]
    )


@bot.on(events.CallbackQuery(data=b"adm_mc_reset"))
async def adm_mc_reset(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    GLOBAL_STATE.pop("menu_config", None)
    save_persistent_db()
    await event.answer("✅ Menu config reset! Sab defaults par wapas.", alert=True)
    await adm_menu_customizer(event)


# ══════════════════════════════════════════════════════════════
# FIX 5: ADS PACKAGES — Pre-defined Modifiable Promo Packages
# ══════════════════════════════════════════════════════════════

_DEFAULT_ADS_PACKAGES = {
    "starter": {
        "name": "🌱 Starter",
        "desc": "Chhote business ke liye — 3 din ka banner",
        "type": "banner",
        "duration_days": 3,
        "price": 199,
        "impressions": 5000,
        "active": True,
    },
    "basic": {
        "name": "📦 Basic",
        "desc": "7 din ka banner — sabse popular",
        "type": "banner",
        "duration_days": 7,
        "price": 399,
        "impressions": 15000,
        "active": True,
    },
    "standard": {
        "name": "⭐ Standard",
        "desc": "15 din — banner + 1 popup blast",
        "type": "popup",
        "duration_days": 15,
        "price": 699,
        "impressions": 35000,
        "active": True,
    },
    "premium": {
        "name": "💎 Premium",
        "desc": "30 din — button + banner combo",
        "type": "button",
        "duration_days": 30,
        "price": 1299,
        "impressions": 80000,
        "active": True,
    },
    "vip": {
        "name": "👑 VIP",
        "desc": "60 din — sab channels, max visibility",
        "type": "blast",
        "duration_days": 60,
        "price": 2499,
        "impressions": 0,  # unlimited
        "active": False,
    },
}


def _get_ads_packages():
    pkgs = GLOBAL_STATE.get("ads_packages")
    if pkgs is None:
        GLOBAL_STATE["ads_packages"] = dict(_DEFAULT_ADS_PACKAGES)
        save_persistent_db()
    return GLOBAL_STATE["ads_packages"]


@bot.on(events.CallbackQuery(data=b"adm_ads_packages"))
async def adm_ads_packages(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    pkgs = _get_ads_packages()
    btns = []
    for pid, p in pkgs.items():
        status = "✅" if p.get("active") else "❌"
        btns.append([Button.inline(
            f"{status} {p['name']} — ₹{p['price']} · {p['duration_days']}d",
            f"adm_adspkg_detail|{pid}".encode()
        )])
    btns += [
        [Button.inline("➕ Naya Package Banao",  b"adm_adspkg_create"),
         Button.inline("🔄 Defaults Reset",      b"adm_adspkg_reset")],
        [Button.inline("🔙 Admin Panel",          b"adm_main")],
    ]
    try:
        await event.edit(
            "📦 **ADS PACKAGES**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Ye packages /promote command par sponsors ko dikhte hain.\n"
            "Koi bhi package edit ya on/off kar sakte ho.\n\n"
            "Package select karo manage karne ke liye:",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_adspkg_detail\\|(.+)"))
async def adm_adspkg_detail(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    pid = event.data.decode().split("|")[1]
    pkgs = _get_ads_packages()
    p = pkgs.get(pid)
    if not p:
        return await event.answer("Package nahi mila!", alert=True)
    impr = f"{p['impressions']:,}" if p.get("impressions") else "Unlimited"
    text = (
        f"📦 **{p['name']}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📝 {p.get('desc', '—')}\n\n"
        f"💰 Price: **₹{p['price']}**\n"
        f"📅 Duration: **{p['duration_days']} din**\n"
        f"📢 Type: `{p.get('type', 'banner')}`\n"
        f"👁 Max Impressions: `{impr}`\n"
        f"Status: {'✅ Active' if p.get('active') else '❌ Inactive'}"
    )
    tog_lbl = "❌ Deactivate" if p.get("active") else "✅ Activate"
    try:
        await event.edit(text, buttons=[
            [Button.inline(tog_lbl,                  f"adm_adspkg_toggle|{pid}".encode()),
             Button.inline("✏️ Edit",                f"adm_adspkg_edit|{pid}".encode())],
            [Button.inline("💰 Price Badlo",          f"adm_adspkg_price|{pid}".encode()),
             Button.inline("📅 Days Badlo",           f"adm_adspkg_days|{pid}".encode())],
            [Button.inline("📝 Naam/Desc Badlo",      f"adm_adspkg_rename|{pid}".encode())],
            [Button.inline("🗑 Delete",               f"adm_adspkg_del|{pid}".encode())],
            [Button.inline("🔙 Packages",             b"adm_ads_packages")],
        ])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_adspkg_toggle\\|(.+)"))
async def adm_adspkg_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    pid = event.data.decode().split("|")[1]
    pkgs = _get_ads_packages()
    if pid in pkgs:
        pkgs[pid]["active"] = not pkgs[pid].get("active", True)
        save_persistent_db()
        status = "✅ Active" if pkgs[pid]["active"] else "❌ Inactive"
        await event.answer(f"'{pkgs[pid]['name']}' → {status}", alert=False)
    await adm_adspkg_detail(event)


@bot.on(events.CallbackQuery(pattern=b"adm_adspkg_price\\|(.+)"))
async def adm_adspkg_price(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    pid = event.data.decode().split("|")[1]
    data = get_user_data(event.sender_id)
    data["step"] = f"adm_adspkg_price_input|{pid}"
    data["step_since"] = time.time()
    p = _get_ads_packages().get(pid, {})
    try:
        await event.edit(
            f"💰 **'{p.get('name', pid)}' ka naya price:**\n\n"
            f"Current: ₹{p.get('price', 0)}\n\n"
            "Naya price (sirf number) bhejo:\nExample: `499`",
            buttons=[[Button.inline("❌ Cancel", f"adm_adspkg_detail|{pid}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_adspkg_days\\|(.+)"))
async def adm_adspkg_days(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    pid = event.data.decode().split("|")[1]
    data = get_user_data(event.sender_id)
    data["step"] = f"adm_adspkg_days_input|{pid}"
    data["step_since"] = time.time()
    p = _get_ads_packages().get(pid, {})
    try:
        await event.edit(
            f"📅 **'{p.get('name', pid)}' ke kitne din:**\n\n"
            f"Current: {p.get('duration_days', 7)} din\n\n"
            "Naye din (sirf number) bhejo:\nExample: `30`",
            buttons=[[Button.inline("❌ Cancel", f"adm_adspkg_detail|{pid}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_adspkg_rename\\|(.+)"))
async def adm_adspkg_rename(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    pid = event.data.decode().split("|")[1]
    data = get_user_data(event.sender_id)
    data["step"] = f"adm_adspkg_rename_input|{pid}"
    data["step_since"] = time.time()
    p = _get_ads_packages().get(pid, {})
    try:
        await event.edit(
            f"📝 **Naam aur description bhejo (2 lines mein):**\n\n"
            f"Current naam: `{p.get('name', '—')}`\n"
            f"Current desc: `{p.get('desc', '—')}`\n\n"
            "Format:\n`Naya Naam`\n`Naya description`",
            buttons=[[Button.inline("❌ Cancel", f"adm_adspkg_detail|{pid}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_adspkg_del\\|(.+)"))
async def adm_adspkg_del(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    pid = event.data.decode().split("|")[1]
    pkgs = _get_ads_packages()
    name = pkgs.get(pid, {}).get("name", pid)
    pkgs.pop(pid, None)
    save_persistent_db()
    await event.answer(f"🗑 '{name}' delete ho gaya!", alert=True)
    await adm_ads_packages(event)


@bot.on(events.CallbackQuery(data=b"adm_adspkg_create"))
async def adm_adspkg_create(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    data = get_user_data(event.sender_id)
    data["step"] = "adm_adspkg_new_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "➕ **NAYA ADS PACKAGE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Niche format mein bhejo (4 lines):\n\n"
            "`Package Naam`\n"
            "`Description`\n"
            "`Price (₹)`\n"
            "`Duration (days)`\n\n"
            "Example:\n"
            "`Gold Package`\n"
            "`20 din ka banner, 50k impressions`\n"
            "`999`\n"
            "`20`",
            buttons=[[Button.inline("❌ Cancel", b"adm_ads_packages")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_adspkg_reset"))
async def adm_adspkg_reset(event):
    await event.answer()
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    GLOBAL_STATE["ads_packages"] = dict(_DEFAULT_ADS_PACKAGES)
    save_persistent_db()
    await event.answer("✅ Default packages restore ho gaye!", alert=True)
    await adm_ads_packages(event)


# ── Text input handler for ads packages ───────────────────────────────────────
@bot.on(events.NewMessage(func=lambda e: e.is_private and
        isinstance(get_user_data(e.sender_id).get("step"), str) and
        any(get_user_data(e.sender_id).get("step","").startswith(p) for p in [
            "adm_adspkg_price_input|",
            "adm_adspkg_days_input|",
            "adm_adspkg_rename_input|",
            "adm_adspkg_new_input",
        ])))
async def adm_adspkg_text_handler(event):
    uid = event.sender_id
    if not is_admin(uid): return
    data = get_user_data(uid)
    step = data.get("step", "")
    data["step"] = None
    pkgs = _get_ads_packages()
    txt = event.raw_text.strip()

    if step.startswith("adm_adspkg_price_input|"):
        pid = step.split("|")[1]
        try:
            price = int(txt)
            pkgs[pid]["price"] = price
            save_persistent_db()
            await event.respond(f"✅ Price updated → ₹{price}",
                buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]])
        except ValueError:
            await event.respond("❌ Sirf number bhejo! Jaise: `499`",
                buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]])

    elif step.startswith("adm_adspkg_days_input|"):
        pid = step.split("|")[1]
        try:
            days = int(txt)
            pkgs[pid]["duration_days"] = days
            save_persistent_db()
            await event.respond(f"✅ Duration updated → {days} din",
                buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]])
        except ValueError:
            await event.respond("❌ Sirf number bhejo! Jaise: `30`",
                buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]])

    elif step.startswith("adm_adspkg_rename_input|"):
        pid = step.split("|")[1]
        lines = txt.split("\n", 1)
        pkgs[pid]["name"] = lines[0].strip()[:40]
        if len(lines) > 1:
            pkgs[pid]["desc"] = lines[1].strip()[:100]
        save_persistent_db()
        await event.respond(f"✅ Package update ho gaya!",
            buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]])

    elif step == "adm_adspkg_new_input":
        lines = txt.split("\n")
        if len(lines) < 4:
            await event.respond(
                "❌ 4 lines chahiye!\n\nDobara try karo:",
                buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]]
            )
            return
        try:
            import random, string as _s
            pid = "".join(random.choices(_s.ascii_lowercase, k=6))
            pkgs[pid] = {
                "name": lines[0].strip()[:40],
                "desc": lines[1].strip()[:100],
                "price": int(lines[2].strip()),
                "duration_days": int(lines[3].strip()),
                "type": "banner",
                "impressions": 0,
                "active": True,
            }
            save_persistent_db()
            add_log(uid, "Ads Package Created", details=pkgs[pid]["name"])
            await event.respond(
                f"✅ **'{pkgs[pid]['name']}' package bana!**",
                buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]]
            )
        except (ValueError, IndexError):
            await event.respond(
                "❌ Format galat hai! Price aur Days sirf numbers hone chahiye.",
                buttons=[[Button.inline("📦 Packages", b"adm_ads_packages")]]
            )


# ══════════════════════════════════════════════════════════════
# 📌 NOTICE / RULES / PINNED MESSAGE FEATURE
# ══════════════════════════════════════════════════════════════

def get_notice_config() -> dict:
    """Bot-wide notice/rules config."""
    GLOBAL_STATE.setdefault("notice_config", {
        "enabled": False,
        "title": "📌 Important Notice",
        "text": "",
        "show_on_start": False,
        "show_on_menu": False,
        "pin_type": "notice",  # notice / rules / announcement / tips
        "created_at": 0,
        "updated_at": 0,
    })
    return GLOBAL_STATE["notice_config"]


@bot.on(events.CallbackQuery(data=b"adm_notice_panel"))
async def adm_notice_panel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    cfg = get_notice_config()
    status   = "🟢 ON" if cfg.get("enabled") else "🔴 OFF"
    on_start = "✅" if cfg.get("show_on_start") else "❌"
    on_menu  = "✅" if cfg.get("show_on_menu")  else "❌"
    text_preview = (cfg.get("text", "") or "")[:80]
    text_preview = text_preview + "..." if len(cfg.get("text","")) > 80 else text_preview
    pin_type = cfg.get("pin_type", "notice").title()

    txt = (
        "📌 **Notice / Rules Manager**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Status:** {status}\n"
        f"**Type:** {pin_type}\n"
        f"**Show on /start:** {on_start}\n"
        f"**Show on Menu open:** {on_menu}\n\n"
        f"**Current Text:**\n"
        f"`{text_preview or '(empty — set karo)'}`\n\n"
        "Users ko `/rules` ya `/notice` command se bhi dekh sakte hain।"
    )
    btns = [
        [Button.inline("✏️ Set Notice/Rules Text",  b"adm_notice_set_text")],
        [Button.inline(
            "🔴 Disable" if cfg.get("enabled") else "🟢 Enable",
            b"adm_notice_toggle"
        )],
        [Button.inline(
            f"📲 On /start: {'ON ✅' if cfg.get('show_on_start') else 'OFF ❌'}",
            b"adm_notice_toggle_start"
        )],
        [Button.inline(
            f"🏠 On Menu: {'ON ✅' if cfg.get('show_on_menu') else 'OFF ❌'}",
            b"adm_notice_toggle_menu"
        )],
        [Button.inline("📤 Sabko Bhejo (Broadcast)", b"adm_notice_broadcast")],
        [Button.inline("🗑 Clear Notice",             b"adm_notice_clear"),
         Button.inline("🔙 Admin Panel",              b"adm_main")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_notice_set_text"))
async def adm_notice_set_text(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return
    get_user_data(event.sender_id)["step"] = "adm_notice_text_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **Notice / Rules Text Set Karo**\n\n"
            "Ab apna notice, rules, ya koi bhi message bhejo।\n"
            "• Bold ke liye: `**text**`\n"
            "• Italic ke liye: `_text_`\n"
            "• Code ke liye: `` `text` ``\n\n"
            "Bhejo abhi 👇",
            buttons=[[Button.inline("❌ Cancel", b"adm_notice_panel")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_notice_toggle"))
async def adm_notice_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg = get_notice_config()
    cfg["enabled"] = not cfg.get("enabled", False)
    save_persistent_db()
    state = "Enable" if cfg["enabled"] else "Disable"
    await event.answer(f"✅ Notice {state} ho gayi!", alert=False)
    await adm_notice_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_notice_toggle_start"))
async def adm_notice_toggle_start(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg = get_notice_config()
    cfg["show_on_start"] = not cfg.get("show_on_start", False)
    save_persistent_db()
    await adm_notice_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_notice_toggle_menu"))
async def adm_notice_toggle_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg = get_notice_config()
    cfg["show_on_menu"] = not cfg.get("show_on_menu", False)
    save_persistent_db()
    await adm_notice_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_notice_clear"))
async def adm_notice_clear(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg = get_notice_config()
    cfg["text"]    = ""
    cfg["enabled"] = False
    save_persistent_db()
    await event.answer("🗑 Notice clear ho gayi!", alert=True)
    await adm_notice_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_notice_broadcast"))
async def adm_notice_broadcast(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg = get_notice_config()
    if not cfg.get("text"):
        return await event.answer("❌ Pehle notice text set karo!", alert=True)

    title = cfg.get("title", "📌 Important Notice")
    text  = cfg.get("text", "")
    msg   = f"{title}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{text}"

    sent = failed = 0
    from database import db as _db
    for uid in list(_db.keys()):
        try:
            await bot.send_message(int(uid), msg)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1

    save_persistent_db()
    await event.answer(f"✅ Bheja: {sent} | ❌ Failed: {failed}", alert=True)
    await adm_notice_panel(event)


def get_notice_text_for_user() -> str | None:
    """Notice text get karo — user ko dikhane ke liye."""
    cfg = get_notice_config()
    if not cfg.get("enabled") or not cfg.get("text"):
        return None
    title = cfg.get("title", "📌 Important Notice")
    return f"{title}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n{cfg['text']}"


# ── Admin welcome msg panel ───────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_welcome_msg"))
async def adm_welcome_msg(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg = GLOBAL_STATE.get("welcome_msg_config", {})
    enabled = cfg.get("enabled", False)
    text    = (cfg.get("text", "") or "")[:80]

    try:
        await event.edit(
            "👁 **Bot Welcome Message**\n\n"
            f"Status: {'🟢 ON' if enabled else '🔴 OFF'}\n"
            f"Text: `{text or '(default)'}`\n\n"
            "Ye message tab dikhai deta hai jab koi user pehli baar /start karta hai।",
            buttons=[
                [Button.inline("✏️ Welcome Msg Set Karo", b"adm_set_welcome_text")],
                [Button.inline(
                    "🔴 Disable" if enabled else "🟢 Enable",
                    b"adm_welcome_toggle"
                )],
                [Button.inline("🔙 Admin Panel", b"adm_main")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_welcome_toggle"))
async def adm_welcome_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg = GLOBAL_STATE.setdefault("welcome_msg_config", {})
    cfg["enabled"] = not cfg.get("enabled", False)
    save_persistent_db()
    await adm_welcome_msg(event)


@bot.on(events.CallbackQuery(data=b"adm_set_welcome_text"))
async def adm_set_welcome_text(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    get_user_data(event.sender_id)["step"] = "adm_welcome_text_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "👁 **Welcome Message Set Karo**\n\n"
            "Ye message new users ko /start karne par dikhega।\n"
            "Ab bhejo 👇",
            buttons=[[Button.inline("❌ Cancel", b"adm_welcome_msg")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════════════════════════
# 🗂 STORAGE MANAGER — Production Level v2
# ══════════════════════════════════════════════════════════════

def _get_storage_stats() -> dict:
    import os, psutil
    stats = {}

    # Users
    stats["total_users"]   = len(db)
    stats["active_users"]  = sum(1 for u in db.values() if u.get("settings",{}).get("running"))
    stats["premium_users"] = sum(1 for u in db.values() if u.get("premium",{}).get("active"))
    stats["no_session"]    = sum(1 for u in db.values() if not u.get("session"))
    stats["dead_sessions"] = sum(1 for u in db.values()
                                  if not u.get("session") and not u.get("last_active", 0))

    # Memory (RAM)
    try:
        proc = psutil.Process()
        mem  = proc.memory_info()
        stats["ram_mb"]      = round(mem.rss / 1024 / 1024, 1)
        stats["ram_pct"]     = round(psutil.virtual_memory().percent, 1)
        stats["ram_avail"]   = round(psutil.virtual_memory().available / 1024 / 1024, 1)
    except Exception:
        stats["ram_mb"] = stats["ram_pct"] = stats["ram_avail"] = 0

    # Cache sizes
    from database import duplicate_db, PRODUCT_HISTORY_STORE, REPLY_CACHE
    stats["dup_entries"]    = sum(len(v.get("history",{})) for v in duplicate_db.values())
    stats["prod_entries"]   = sum(len(v.get("links",{})) + len(v.get("images",{})) + len(v.get("texts",{}))
                                  for v in PRODUCT_HISTORY_STORE.values())
    stats["reply_entries"]  = sum(
        sum(len(msgs) for msgs in src.values())
        for src in REPLY_CACHE.values()
    )

    # Admin logs
    stats["log_count"] = len(admin_logs)

    # DLQ
    try:
        from msg_queue import DLQ_FILE
        if os.path.exists(DLQ_FILE):
            stats["dlq_count"]   = sum(1 for _ in open(DLQ_FILE))
            stats["dlq_size_kb"] = round(os.path.getsize(DLQ_FILE) / 1024, 1)
        else:
            stats["dlq_count"] = stats["dlq_size_kb"] = 0
    except Exception:
        stats["dlq_count"] = stats["dlq_size_kb"] = 0

    # Watermark logos
    try:
        from watermark import LOGO_DIR
        if os.path.exists(LOGO_DIR):
            logo_files         = os.listdir(LOGO_DIR)
            logo_size          = sum(os.path.getsize(os.path.join(LOGO_DIR,f))
                                     for f in logo_files if os.path.isfile(os.path.join(LOGO_DIR,f)))
            stats["logo_count"]   = len(logo_files)
            stats["logo_size_kb"] = round(logo_size / 1024, 1)
            db_logos = set(u.get("watermark",{}).get("logo_file","") for u in db.values()
                           if u.get("watermark",{}).get("logo_file"))
            stats["orphan_logos"] = len(set(logo_files) - db_logos)
        else:
            stats["logo_count"] = stats["logo_size_kb"] = stats["orphan_logos"] = 0
    except Exception:
        stats["logo_count"] = stats["logo_size_kb"] = stats["orphan_logos"] = 0

    # Temp files
    try:
        tmp_files          = [f for f in os.listdir("/tmp") if f.startswith(("dl_","wm_","ph_"))]
        tmp_size           = sum(os.path.getsize(os.path.join("/tmp",f)) for f in tmp_files
                                 if os.path.isfile(os.path.join("/tmp",f)))
        stats["tmp_count"]   = len(tmp_files)
        stats["tmp_size_kb"] = round(tmp_size / 1024, 1)
    except Exception:
        stats["tmp_count"] = stats["tmp_size_kb"] = 0

    # Analytics
    stats["analytics_entries"] = sum(len(u.get("analytics",{}).get("daily",{})) for u in db.values())

    # Last cleanup time
    stats["last_cleanup"] = GLOBAL_STATE.get("last_full_cleanup", 0)

    # Auto cleanup config
    stats["auto_clean_enabled"] = GLOBAL_STATE.get("auto_storage_clean", {}).get("enabled", False)
    stats["auto_clean_hours"]   = GLOBAL_STATE.get("auto_storage_clean", {}).get("interval_hours", 24)

    return stats


def _bar(used, total, w=8):
    if total <= 0: return "░" * w
    filled = min(round(used / total * w), w)
    return "█" * filled + "░" * (w - filled)


@bot.on(events.CallbackQuery(data=b"adm_storage_manager"))
async def adm_storage_manager(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    s       = _get_storage_stats()
    ram_bar = _bar(s["ram_mb"], 512)
    last_cl = f"<t:{int(s['last_cleanup'])}:R>" if s["last_cleanup"] else "kabhi nahi"
    auto_st = f"🟢 Har {s['auto_clean_hours']}h" if s["auto_clean_enabled"] else "🔴 OFF"

    txt = (
        "🗂 **Storage Manager**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🖥 **RAM:** [{ram_bar}] `{s['ram_mb']}MB / 512MB` ({s['ram_pct']}%)\n"
        f"📡 Available: `{s['ram_avail']}MB`\n\n"
        f"👥 **Users:** `{s['total_users']}` total  "
        f"`{s['active_users']}` active  "
        f"`{s['no_session']}` no-session\n"
        f"💎 Premium: `{s['premium_users']}`  "
        f"🔴 Dead: `{s['dead_sessions']}`\n\n"
        f"🧠 **Cache:**\n"
        f"  Dup Filter: `{s['dup_entries']}` entries\n"
        f"  Product Dup: `{s['prod_entries']}` entries\n"
        f"  Reply Map: `{s['reply_entries']}` entries\n"
        f"  Analytics: `{s['analytics_entries']}` entries\n"
        f"  Admin Logs: `{s['log_count']}` entries\n\n"
        f"📁 **Files:**\n"
        f"  DLQ: `{s['dlq_count']}` msgs (`{s['dlq_size_kb']}KB`)\n"
        f"  Logos: `{s['logo_count']}` files (`{s['logo_size_kb']}KB`)\n"
        f"  Orphan Logos: `{s['orphan_logos']}` (no owner)\n"
        f"  Temp Files: `{s['tmp_count']}` (`{s['tmp_size_kb']}KB`)\n\n"
        f"⏰ Auto Clean: {auto_st}\n"
        f"🕐 Last Cleanup: {last_cl}"
    )

    btns = [
        [Button.inline("🗑 Clear DLQ",           b"adm_clear_dlq"),
         Button.inline("🖼 Clean Logos",          b"adm_clean_logos")],
        [Button.inline("🧹 Clean Temp",           b"adm_clean_tmp"),
         Button.inline("♻️ Flush Dup Cache",       b"adm_flush_dup")],
        [Button.inline("📊 Clean Analytics",      b"adm_clean_analytics"),
         Button.inline("💬 Clear Reply Cache",    b"adm_clear_reply")],
        [Button.inline("📋 Clear Admin Logs",     b"adm_clear_logs"),
         Button.inline("🔴 Del Dead Sessions",    b"adm_del_dead")],
        [Button.inline("🧹 Del Inactive Users",   b"adm_run_cleanup")],
        [Button.inline(f"⏰ Auto: {auto_st}",     b"adm_auto_clean_toggle")],
        [Button.inline("⚡ CLEAN EVERYTHING",     b"adm_clean_all_confirm")],
        [Button.inline("🔄 Refresh",              b"adm_storage_manager"),
         Button.inline("🔙 Back",                 b"adm_cleanup_panel")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


# ── Confirmation before clean all ────────────────────────────
@bot.on(events.CallbackQuery(data=b"adm_clean_all_confirm"))
async def adm_clean_all_confirm(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        await event.edit(
            "⚠️ **Confirm: Clean Everything?**\n\n"
            "Ye sab delete ho jaayega:\n"
            "• DLQ messages\n"
            "• Orphan watermark logos\n"
            "• Temp files\n"
            "• Old analytics (31d+)\n"
            "• Dup filter cache\n"
            "• Reply map cache\n"
            "• Old admin logs (200+ wale)\n\n"
            "**Users ka data safe rahega।**\n"
            "Kya aap sure hain?",
            buttons=[
                [Button.inline("✅ Haan, Clean Karo", b"adm_clean_all"),
                 Button.inline("❌ Cancel",            b"adm_storage_manager")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_clear_dlq"))
async def adm_clear_dlq(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        from msg_queue import dlq
        count = dlq.count()
        dlq.clear()
        add_log(event.sender_id, "DLQ Cleared", details=f"{count} msgs")
        await event.answer(f"✅ DLQ cleared! {count} msgs deleted", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_clean_logos"))
async def adm_clean_logos(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        import os
        from watermark import LOGO_DIR
        if not os.path.exists(LOGO_DIR):
            return await event.answer("✅ No logos folder", alert=True)
        disk  = set(os.listdir(LOGO_DIR))
        owned = set(u.get("watermark",{}).get("logo_file","") for u in db.values()
                    if u.get("watermark",{}).get("logo_file"))
        deleted = 0
        for f in disk - owned:
            try: os.remove(os.path.join(LOGO_DIR, f)); deleted += 1
            except: pass
        add_log(event.sender_id, "Logos Cleaned", details=f"{deleted} orphans")
        await event.answer(f"✅ {deleted} orphan logos deleted!", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_clean_tmp"))
async def adm_clean_tmp(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        import os
        files   = [f for f in os.listdir("/tmp") if f.startswith(("dl_","wm_","ph_"))]
        deleted = 0
        for f in files:
            try: os.remove(os.path.join("/tmp", f)); deleted += 1
            except: pass
        add_log(event.sender_id, "Temp Cleaned", details=f"{deleted} files")
        await event.answer(f"✅ {deleted} temp files deleted!", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_flush_dup"))
async def adm_flush_dup(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        from database import duplicate_db, PRODUCT_HISTORY_STORE
        c1 = sum(len(v.get("history",{})) for v in duplicate_db.values())
        c2 = sum(len(v.get("links",{})) + len(v.get("images",{})) + len(v.get("texts",{}))
                 for v in PRODUCT_HISTORY_STORE.values())
        for v in duplicate_db.values(): v["history"] = {}
        for v in PRODUCT_HISTORY_STORE.values(): v["links"] = {}; v["images"] = {}; v["texts"] = {}
        save_persistent_db()
        add_log(event.sender_id, "Dup Cache Flushed", details=f"{c1+c2} entries")
        await event.answer(f"✅ {c1+c2} dup cache entries cleared!", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_clean_analytics"))
async def adm_clean_analytics(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        import time as _t
        cutoff  = _t.time() - (31 * 86400)
        cleaned = 0
        for udata in db.values():
            daily   = udata.get("analytics", {}).get("daily", {})
            old_k   = [k for k in daily
                        if _t.mktime(_t.strptime(k, "%Y-%m-%d")) < cutoff]
            for k in old_k: del daily[k]; cleaned += 1
        save_persistent_db()
        add_log(event.sender_id, "Analytics Cleaned", details=f"{cleaned} entries")
        await event.answer(f"✅ {cleaned} old analytics entries deleted!", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_clear_reply"))
async def adm_clear_reply(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        from database import REPLY_CACHE
        count = sum(sum(len(msgs) for msgs in src.values()) for src in REPLY_CACHE.values())
        REPLY_CACHE.clear()
        add_log(event.sender_id, "Reply Cache Cleared", details=f"{count} entries")
        await event.answer(f"✅ {count} reply mappings cleared!", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_clear_logs"))
async def adm_clear_logs_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        count = len(admin_logs)
        keep  = admin_logs[-50:]   # Last 50 rakho
        admin_logs.clear()
        admin_logs.extend(keep)
        save_persistent_db()
        add_log(event.sender_id, "Logs Cleared", details=f"Kept last 50 of {count}")
        await event.answer(f"✅ {count-50} old logs cleared! (last 50 kept)", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_del_dead"))
async def adm_del_dead_sessions(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    try:
        from database import user_sessions
        protected = set(str(k) for k in GLOBAL_STATE.get("admins", {}).keys())
        dead = [
            uid for uid, udata in list(db.items())
            if not udata.get("session")
            and str(uid) not in protected
            and not udata.get("premium", {}).get("active")
            and not udata.get("last_active", 0)
        ]
        for uid in dead:
            db.pop(uid, None)
        save_persistent_db()
        add_log(event.sender_id, "Dead Sessions Deleted", details=f"{len(dead)} users")
        await event.answer(f"✅ {len(dead)} dead session users deleted!", alert=True)
    except Exception as e:
        await event.answer(f"❌ {str(e)[:80]}", alert=True)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_auto_clean_toggle"))
async def adm_auto_clean_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    cfg     = GLOBAL_STATE.setdefault("auto_storage_clean", {"enabled": False, "interval_hours": 24})
    enabled = not cfg.get("enabled", False)
    cfg["enabled"] = enabled
    save_persistent_db()
    state = "ON ✅" if enabled else "OFF ❌"
    await event.answer(f"Auto Clean: {state}", alert=False)
    await adm_storage_manager(event)


@bot.on(events.CallbackQuery(data=b"adm_clean_all"))
async def adm_clean_all(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    import os, time as _t

    progress = await bot.send_message(
        event.sender_id,
        "⏳ **Full Cleanup Chal Raha Hai...**\n\n"
        "1️⃣ DLQ clear...\n"
        "2️⃣ Orphan logos...\n"
        "3️⃣ Temp files...\n"
        "4️⃣ Analytics trim...\n"
        "5️⃣ Dup cache...\n"
        "6️⃣ Reply cache...\n"
        "7️⃣ Admin logs trim..."
    )

    r = {}

    # 1. DLQ
    try:
        from msg_queue import dlq
        r["dlq"] = dlq.count(); dlq.clear()
    except: r["dlq"] = 0

    # 2. Orphan logos
    try:
        from watermark import LOGO_DIR
        disk  = set(os.listdir(LOGO_DIR)) if os.path.exists(LOGO_DIR) else set()
        owned = set(u.get("watermark",{}).get("logo_file","") for u in db.values()
                    if u.get("watermark",{}).get("logo_file"))
        deleted = 0
        for f in disk - owned:
            try: os.remove(os.path.join(LOGO_DIR, f)); deleted += 1
            except: pass
        r["logos"] = deleted
    except: r["logos"] = 0

    # 3. Temp files
    try:
        tmp = [f for f in os.listdir("/tmp") if f.startswith(("dl_","wm_","ph_"))]
        deleted = 0
        for f in tmp:
            try: os.remove(os.path.join("/tmp",f)); deleted += 1
            except: pass
        r["tmp"] = deleted
    except: r["tmp"] = 0

    # 4. Analytics
    try:
        cutoff = _t.time() - (31 * 86400)
        c = 0
        for udata in db.values():
            daily = udata.get("analytics", {}).get("daily", {})
            old_k = [k for k in daily if _t.mktime(_t.strptime(k, "%Y-%m-%d")) < cutoff]
            for k in old_k: del daily[k]; c += 1
        r["analytics"] = c
    except: r["analytics"] = 0

    # 5. Dup cache
    try:
        from database import duplicate_db, PRODUCT_HISTORY_STORE
        c1 = sum(len(v.get("history",{})) for v in duplicate_db.values())
        c2 = sum(len(v.get("links",{})) + len(v.get("images",{})) + len(v.get("texts",{}))
                 for v in PRODUCT_HISTORY_STORE.values())
        for v in duplicate_db.values(): v["history"] = {}
        for v in PRODUCT_HISTORY_STORE.values(): v["links"] = {}; v["images"] = {}; v["texts"] = {}
        r["dup"] = c1 + c2
    except: r["dup"] = 0

    # 6. Reply cache
    try:
        from database import REPLY_CACHE
        rc = sum(sum(len(msgs) for msgs in src.values()) for src in REPLY_CACHE.values())
        REPLY_CACHE.clear()
        r["reply"] = rc
    except: r["reply"] = 0

    # 7. Admin logs
    try:
        old_count = len(admin_logs)
        keep = admin_logs[-50:]
        admin_logs.clear(); admin_logs.extend(keep)
        r["logs"] = old_count - 50
    except: r["logs"] = 0

    GLOBAL_STATE["last_full_cleanup"] = int(_t.time())
    save_persistent_db()
    add_log(event.sender_id, "Full Cleanup", details=str(r))

    total_freed = r["dlq"] + r["logos"] + r["tmp"] + r["analytics"] + r["dup"] + r["reply"] + max(0, r["logs"])

    await progress.edit(
        "✅ **Full Cleanup Complete!**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🗑 DLQ: `{r['dlq']}` messages\n"
        f"🖼 Orphan Logos: `{r['logos']}` files\n"
        f"🧹 Temp Files: `{r['tmp']}` files\n"
        f"📊 Analytics: `{r['analytics']}` old entries\n"
        f"♻️ Dup Cache: `{r['dup']}` entries\n"
        f"💬 Reply Cache: `{r['reply']}` entries\n"
        f"📋 Admin Logs: `{max(0,r['logs'])}` old entries\n\n"
        f"📦 **Total freed: `{total_freed}` items**\n"
        f"💾 Database saved ✅",
        buttons=[
            [Button.inline("🗂 Storage Manager", b"adm_storage_manager"),
             Button.inline("🏠 Admin Panel",     b"adm_main")]
        ]
    )


# ══════════════════════════════════════════════════════════════
# 🔗 AFFILIATE MANAGER — Admin Panel
# Commission split, owner tags, platform control
# ══════════════════════════════════════════════════════════════

def _aff_dashboard_text() -> str:
    """Affiliate dashboard ka full status text banao."""
    from feature_flags import get_flag
    from affiliate import registry

    mode        = get_flag("affiliate_mode") or "user"
    c_enabled   = bool(get_flag("commission_enabled"))
    c_rate      = int(get_flag("commission_rate") or 30)
    user_pct    = 100 - c_rate

    # Owner tags status
    tags_set = []
    tags_missing = []
    for p in registry.list_platforms():
        t = get_flag(f"owner_{p.tag_key}") or ""
        if t:
            tags_set.append(f"  {p.icon} {p.name.title()}: `{t[:25]}`")
        else:
            tags_missing.append(f"  {p.icon} {p.name.title()}")

    mode_line = "👑 **Owner Mode** — Sabke links mein admin ka tag" if mode == "owner" \
                else "👤 **User Mode** — Har user ka apna tag (+ commission split)"

    commission_block = ""
    if mode == "user":
        if c_enabled:
            commission_block = (
                f"\n💰 **Commission Split:**\n"
                f"  • Free users: {user_pct}% unka tag / {c_rate}% admin ka tag\n"
                f"  • Premium users: 100% unka apna tag (no cut)\n"
                f"  • Status: 🟢 Active"
            )
        else:
            commission_block = "\n💰 **Commission Split:** 🔴 Disabled"

    tags_block = ""
    if tags_set:
        tags_block += "\n\n✅ **Set Tags:**\n" + "\n".join(tags_set)
    if tags_missing:
        tags_block += "\n\n❌ **Missing Tags** (commission kaam nahi karega):\n" + "\n".join(tags_missing)

    return (
        "🔗 **AFFILIATE MANAGER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚙️ **Mode:** {mode_line}"
        + commission_block
        + tags_block
        + "\n\n💡 Neeche options se configure karo:"
    )


@bot.on(events.CallbackQuery(data=b"flags_affiliate_menu"))
async def flags_affiliate_menu(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ Access Denied", alert=True)
    admin_mark_left_main(event.sender_id)
    admin_mark_activity(event.sender_id)

    from feature_flags import get_flag
    mode      = get_flag("affiliate_mode") or "user"
    c_enabled = bool(get_flag("commission_enabled"))
    c_rate    = int(get_flag("commission_rate") or 30)

    mode_btn_lbl = "🔄 Switch → Owner Mode" if mode == "user" else "🔄 Switch → User Mode"
    comm_btn_lbl = f"💰 Commission: {'🟢 ON' if c_enabled else '🔴 OFF'} ({c_rate}%)"

    btns = [
        [Button.inline(mode_btn_lbl,           b"aff_adm_toggle_mode")],
        [Button.inline(comm_btn_lbl,           b"aff_adm_commission_menu")],
        [Button.inline("🏷️ Owner Tags Setup",  b"aff_adm_tags_menu")],
        [Button.inline("📊 Live Stats",         b"aff_adm_stats")],
        [Button.inline("🔙 Admin Panel",        b"adm_main")],
    ]
    try:
        await event.edit(_aff_dashboard_text(), buttons=btns)
    except errors.MessageNotModifiedError:
        pass


# ── Mode Toggle ─────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"aff_adm_toggle_mode"))
async def aff_adm_toggle_mode(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from feature_flags import get_flag, set_flag
    current = get_flag("affiliate_mode") or "user"
    new_mode = "owner" if current == "user" else "user"
    set_flag("affiliate_mode", new_mode)
    add_log(event.sender_id, "Affiliate", details=f"Mode changed: {current} → {new_mode}")
    await event.answer(f"✅ Mode → {'Owner' if new_mode == 'owner' else 'User'}", alert=False)
    await flags_affiliate_menu(event)


# ── Commission Menu ─────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"aff_adm_commission_menu"))
async def aff_adm_commission_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from feature_flags import get_flag
    c_enabled = bool(get_flag("commission_enabled"))
    c_rate    = int(get_flag("commission_rate") or 30)
    user_pct  = 100 - c_rate

    txt = (
        "💰 **COMMISSION SPLIT SETTINGS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: {'🟢 Active' if c_enabled else '🔴 Disabled'}\n"
        f"Admin ka hissa: `{c_rate}%`\n"
        f"User ka hissa: `{user_pct}%`\n\n"
        "📌 **Rules:**\n"
        "  • Premium users se kabhi commission nahi\n"
        "  • Agar owner tag set nahi — user ka hi tag lagega\n"
        "  • 0% = commission band (sab user ka)\n\n"
        "Rate change karne ke liye neeche % select karo:"
    )

    # Quick rate buttons — common percentages
    rate_btns = []
    for pct in [10, 20, 30, 40, 50]:
        active = "✅ " if pct == c_rate else ""
        rate_btns.append(Button.inline(f"{active}{pct}%", f"aff_adm_set_rate|{pct}".encode()))

    toggle_lbl = "🔴 Disable Commission" if c_enabled else "🟢 Enable Commission"
    btns = [
        rate_btns,
        [Button.inline("✏️ Custom % Type Karo",    b"aff_adm_custom_rate")],
        [Button.inline(toggle_lbl,                  b"aff_adm_toggle_commission")],
        [Button.inline("🔙 Back",                   b"flags_affiliate_menu")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"aff_adm_set_rate\\|(.+)"))
async def aff_adm_set_rate(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from feature_flags import set_flag
    rate = int(event.data.decode().split("|")[1])
    rate = max(0, min(100, rate))
    set_flag("commission_rate", rate)
    add_log(event.sender_id, "Affiliate", details=f"Commission rate set to {rate}%")
    await event.answer(f"✅ Commission rate → {rate}%", alert=False)
    await aff_adm_commission_menu(event)


@bot.on(events.CallbackQuery(data=b"aff_adm_custom_rate"))
async def aff_adm_custom_rate(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    data = get_user_data(event.sender_id)
    data["step"] = "aff_adm_custom_rate_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **CUSTOM COMMISSION RATE**\n\n"
            "0 se 100 ke beech koi bhi number type karo:\n"
            "_(e.g. `25` = 25% admin ka, 75% user ka)_",
            buttons=[[Button.inline("❌ Cancel", b"aff_adm_commission_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "aff_adm_custom_rate_input"))
async def aff_adm_custom_rate_input(event):
    uid = event.sender_id
    if not is_admin(uid): return
    data = get_user_data(uid)
    data["step"] = None
    from feature_flags import set_flag
    try:
        rate = int(event.raw_text.strip())
        rate = max(0, min(100, rate))
        set_flag("commission_rate", rate)
        save_persistent_db()
        add_log(uid, "Affiliate", details=f"Custom commission rate → {rate}%")
        await event.respond(
            f"✅ **Commission rate set: `{rate}%`**\n"
            f"Free users ke `{100-rate}%` links mein unka apna tag lagega।",
            buttons=[[Button.inline("💰 Commission Menu", b"aff_adm_commission_menu"),
                      Button.inline("🔗 Affiliate Home",  b"flags_affiliate_menu")]]
        )
    except ValueError:
        await event.respond(
            "❌ Invalid input — sirf number type karo (0-100)",
            buttons=[[Button.inline("🔙 Back", b"aff_adm_commission_menu")]]
        )


@bot.on(events.CallbackQuery(data=b"aff_adm_toggle_commission"))
async def aff_adm_toggle_commission(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from feature_flags import get_flag, set_flag
    current = bool(get_flag("commission_enabled"))
    set_flag("commission_enabled", not current)
    status = "🟢 Enabled" if not current else "🔴 Disabled"
    add_log(event.sender_id, "Affiliate", details=f"Commission {status}")
    await event.answer(f"Commission → {status}", alert=False)
    await aff_adm_commission_menu(event)


# ── Owner Tags Setup ────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"aff_adm_tags_menu"))
async def aff_adm_tags_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from feature_flags import get_flag
    from affiliate import registry

    lines = []
    btns  = []
    for p in registry.list_platforms():
        tag = get_flag(f"owner_{p.tag_key}") or ""
        status = f"✅ `{tag[:20]}`" if tag else "❌ Not Set"
        lines.append(f"  {p.icon} **{p.name.title()}:** {status}")
        lbl = f"{p.icon} {'✅' if tag else '❌'} {p.name.title()}"
        btns.append([Button.inline(lbl, f"aff_adm_set_tag|{p.name}".encode())])

    btns.append([Button.inline("🗑️ Saare Tags Clear",  b"aff_adm_clear_all_tags")])
    btns.append([Button.inline("🔙 Back",               b"flags_affiliate_menu")])

    txt = (
        "🏷️ **OWNER AFFILIATE TAGS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Ye tags commission wale messages mein lagenge।\n\n"
        + "\n".join(lines)
        + "\n\n💡 Platform select karo tag set/edit/remove karne ke liye:"
    )
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"aff_adm_set_tag\\|(.+)"))
async def aff_adm_set_tag(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from affiliate import registry
    from feature_flags import get_flag
    platform_name = event.data.decode().split("|")[1]
    plugin = registry.get_plugin(platform_name)
    if not plugin:
        return await event.answer("❌ Platform nahi mila", alert=True)

    current_tag = get_flag(f"owner_{plugin.tag_key}") or ""
    data = get_user_data(event.sender_id)
    data["step"] = f"aff_adm_tag_input|{platform_name}"
    data["step_since"] = time.time()

    current_line = f"Current: `{current_tag}`\n\n" if current_tag else ""
    try:
        await event.edit(
            f"{plugin.icon} **{plugin.name.title()} Affiliate Tag**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{current_line}"
            f"Example format: `{plugin.example}`\n\n"
            "Apna affiliate tag type karo:\n"
            "_(Tag clear karne ke liye `-` type karo)_",
            buttons=[
                [Button.inline("🗑️ Remove Tag", f"aff_adm_remove_tag|{platform_name}".encode())],
                [Button.inline("❌ Cancel",      b"aff_adm_tags_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        isinstance(get_user_data(e.sender_id).get("step"), str) and
        get_user_data(e.sender_id).get("step", "").startswith("aff_adm_tag_input|")))
async def aff_adm_tag_input_handler(event):
    uid = event.sender_id
    if not is_admin(uid): return
    data = get_user_data(uid)
    step = data.get("step", "")
    platform_name = step.split("|")[1] if "|" in step else ""
    data["step"] = None

    from affiliate import registry
    from feature_flags import set_flag
    plugin = registry.get_plugin(platform_name)
    if not plugin:
        return await event.respond("❌ Platform nahi mila")

    raw = event.raw_text.strip()
    if raw == "-" or raw.lower() == "clear":
        set_flag(f"owner_{plugin.tag_key}", "")
        save_persistent_db()
        add_log(uid, "Affiliate", details=f"Owner tag removed: {plugin.name}")
        await event.respond(
            f"🗑️ **{plugin.icon} {plugin.name.title()} tag removed।**",
            buttons=[[Button.inline("🏷️ Tags Menu", b"aff_adm_tags_menu"),
                      Button.inline("🔗 Affiliate",  b"flags_affiliate_menu")]]
        )
    else:
        tag = raw[:60]
        set_flag(f"owner_{plugin.tag_key}", tag)
        save_persistent_db()
        add_log(uid, "Affiliate", details=f"Owner tag set: {plugin.name} = {tag}")
        await event.respond(
            f"✅ **{plugin.icon} {plugin.name.title()} tag set:**\n`{tag}`",
            buttons=[[Button.inline("🏷️ Tags Menu", b"aff_adm_tags_menu"),
                      Button.inline("🔗 Affiliate",  b"flags_affiliate_menu")]]
        )


@bot.on(events.CallbackQuery(pattern=b"aff_adm_remove_tag\\|(.+)"))
async def aff_adm_remove_tag(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    platform_name = event.data.decode().split("|")[1]
    from affiliate import registry
    from feature_flags import set_flag
    plugin = registry.get_plugin(platform_name)
    if not plugin:
        return await event.answer("❌ Platform nahi mila", alert=True)
    # Clear step if set
    data = get_user_data(event.sender_id)
    data["step"] = None
    set_flag(f"owner_{plugin.tag_key}", "")
    save_persistent_db()
    add_log(event.sender_id, "Affiliate", details=f"Owner tag removed: {plugin.name}")
    await event.answer(f"🗑️ {plugin.name.title()} tag removed", alert=False)
    await aff_adm_tags_menu(event)


@bot.on(events.CallbackQuery(data=b"aff_adm_clear_all_tags"))
async def aff_adm_clear_all_tags(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from affiliate import registry
    from feature_flags import set_flag
    for p in registry.list_platforms():
        set_flag(f"owner_{p.tag_key}", "")
    save_persistent_db()
    add_log(event.sender_id, "Affiliate", details="All owner tags cleared")
    await event.answer("🗑️ Saare owner tags cleared!", alert=True)
    await aff_adm_tags_menu(event)


# ── Live Stats ──────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"aff_adm_stats"))
async def aff_adm_stats(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    admin_mark_activity(event.sender_id)
    from feature_flags import get_flag
    from affiliate import registry

    c_rate   = int(get_flag("commission_rate") or 30)
    c_on     = bool(get_flag("commission_enabled"))
    mode     = get_flag("affiliate_mode") or "user"

    # Count users with affiliate enabled
    aff_users = 0
    aff_active = 0
    total_replaced = 0
    platform_totals = {p.name: 0 for p in registry.list_platforms()}

    try:
        for uid_key, udata in db.items():
            if not isinstance(udata, dict): continue
            aff = udata.get("affiliate", {})
            if aff.get("enabled"):
                aff_users += 1
                stats = udata.get("affiliate_stats", {})
                tr = stats.get("total_replaced", 0)
                if tr > 0:
                    aff_active += 1
                    total_replaced += tr
                    for p in registry.list_platforms():
                        platform_totals[p.name] += stats.get(f"{p.name}_replaced", 0)
    except Exception:
        pass

    platform_lines = []
    for p in registry.list_platforms():
        cnt = platform_totals[p.name]
        if cnt > 0:
            platform_lines.append(f"  {p.icon} {p.name.title()}: `{cnt}`")

    plat_txt = "\n".join(platform_lines) if platform_lines else "  (abhi koi data nahi)"

    today = time.strftime("%Y-%m-%d")

    txt = (
        "📊 **AFFILIATE LIVE STATS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚙️ Mode: `{mode.upper()}`\n"
        f"💰 Commission: {'🟢 ' + str(c_rate) + '%' if c_on else '🔴 Off'}\n\n"
        f"👥 Affiliate users: `{aff_users}`\n"
        f"🔗 Active (have replaced): `{aff_active}`\n"
        f"🔄 Total links replaced: `{total_replaced}`\n\n"
        f"**Per Platform (all time):**\n{plat_txt}"
    )

    try:
        await event.edit(txt, buttons=[
            [Button.inline("🔄 Refresh",   b"aff_adm_stats")],
            [Button.inline("🔙 Back",      b"flags_affiliate_menu")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer("Already up to date", alert=False)


# ══════════════════════════════════════════════════════════════
# ✏️ ADMIN FORCE START/END MESSAGE
# Sabke forwarded messages mein admin ka custom text lagao
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_force_se_menu"))
async def adm_force_se_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    admin_mark_activity(event.sender_id)
    from feature_flags import get_flag

    f_start = get_flag("force_start_msg") or ""
    f_end   = get_flag("force_end_msg")   or ""
    f_mode  = get_flag("force_msg_mode")  or "append"

    s_status = f"`{f_start[:50]}{'…' if len(f_start)>50 else ''}`" if f_start else "❌ Not Set"
    e_status = f"`{f_end[:50]}{'…' if len(f_end)>50 else ''}`"     if f_end   else "❌ Not Set"
    mode_lbl = "📎 Append (user ke saath)" if f_mode == "append" else "♻️ Replace (user ka hatao)"

    txt = (
        "✏️ **ADMIN FORCE START/END MSG**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Ye messages SABKE forwarded messages mein\n"
        "automatically lagenge — user override nahi kar sakta.\n\n"
        f"**▲ Force Start:** {s_status}\n\n"
        f"**▼ Force End:** {e_status}\n\n"
        f"**Mode:** {mode_lbl}\n\n"
        "💡 Formatting + variables supported.\n"
        "Blank karo to disable."
    )
    btns = [
        [Button.inline("✏️ Set Force Start",   b"adm_fse_set_start"),
         Button.inline("✏️ Set Force End",     b"adm_fse_set_end")],
        [Button.inline("🗑 Clear Force Start", b"adm_fse_clear_start"),
         Button.inline("🗑 Clear Force End",   b"adm_fse_clear_end")],
        [Button.inline(f"🔄 Mode: {f_mode.title()}", b"adm_fse_toggle_mode")],
        [Button.inline("🔙 Admin Panel",        b"adm_main")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_fse_set_start"))
async def adm_fse_set_start(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"] = "adm_fse_input_start"
    data["step_since"] = time.time()
    from feature_flags import get_flag
    cur = get_flag("force_start_msg") or ""
    cur_line = f"\nCurrent: `{cur[:60]}`" if cur else ""
    try:
        await event.edit(
            f"✏️ **FORCE START MSG**{cur_line}\n\n"
            "Sabke messages ke UPAR lagega.\n"
            "Variables + formatting supported.\n\n"
            "Text bhejo (max 500 chars):",
            buttons=[[Button.inline("❌ Cancel", b"adm_force_se_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_fse_set_end"))
async def adm_fse_set_end(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"] = "adm_fse_input_end"
    data["step_since"] = time.time()
    from feature_flags import get_flag
    cur = get_flag("force_end_msg") or ""
    cur_line = f"\nCurrent: `{cur[:60]}`" if cur else ""
    try:
        await event.edit(
            f"✏️ **FORCE END MSG**{cur_line}\n\n"
            "Sabke messages ke NEECHE lagega.\n"
            "Variables + formatting supported.\n\n"
            "Text bhejo (max 500 chars):",
            buttons=[[Button.inline("❌ Cancel", b"adm_force_se_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") in ("adm_fse_input_start", "adm_fse_input_end")))
async def adm_fse_input_handler(event):
    uid  = event.sender_id
    if not is_admin(uid): return
    data = get_user_data(uid)
    step = data.get("step", "")
    slot = "start" if "start" in step else "end"
    data["step"] = None
    raw = event.raw_text.strip()
    if len(raw) > 500:
        await event.respond(f"❌ Max 500 chars. Tumne bheja: {len(raw)}",
                            buttons=[[Button.inline("🔙 Back", b"adm_force_se_menu")]])
        return
    from feature_flags import set_flag
    from database import save_to_mongo as _stm
    set_flag(f"force_{slot}_msg", raw)
    save_persistent_db()
    await _stm()
    add_log(uid, "Force SE", details=f"Force {slot} set: {raw[:50]}")
    try:
        from forward_engine import _render_msg_template
        rendered = _render_msg_template(raw)
    except Exception:
        rendered = raw
    await event.respond(
        f"✅ **Force {slot.title()} msg set!**\n\n**Preview:**\n{rendered}",
        buttons=[[Button.inline("✏️ Force SE Menu", b"adm_force_se_menu"),
                  Button.inline("🏠 Admin",         b"adm_main")]],
        parse_mode="html"
    )


@bot.on(events.CallbackQuery(data=b"adm_fse_clear_start"))
async def adm_fse_clear_start(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from feature_flags import set_flag
    set_flag("force_start_msg", "")
    save_persistent_db()
    await event.answer("✅ Force start cleared!", alert=False)
    await adm_force_se_menu(event)


@bot.on(events.CallbackQuery(data=b"adm_fse_clear_end"))
async def adm_fse_clear_end(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from feature_flags import set_flag
    set_flag("force_end_msg", "")
    save_persistent_db()
    await event.answer("✅ Force end cleared!", alert=False)
    await adm_force_se_menu(event)


@bot.on(events.CallbackQuery(data=b"adm_fse_toggle_mode"))
async def adm_fse_toggle_mode(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from feature_flags import get_flag, set_flag
    cur  = get_flag("force_msg_mode") or "append"
    new  = "replace" if cur == "append" else "append"
    set_flag("force_msg_mode", new)
    save_persistent_db()
    await event.answer(f"Mode → {new.title()}", alert=False)
    await adm_force_se_menu(event)


# ══════════════════════════════════════════════════════════════
# 🌐 ADMIN GLOBAL TEMPLATES
# Admin kuch templates sabke liye push kare
# ══════════════════════════════════════════════════════════════

def _get_admin_fwd_templates() -> dict:
    from database import GLOBAL_STATE
    return GLOBAL_STATE.setdefault("admin_fwd_templates", {})

def _save_admin_fwd_templates(tpls: dict):
    from database import GLOBAL_STATE
    GLOBAL_STATE["admin_fwd_templates"] = tpls
    save_persistent_db()


@bot.on(events.CallbackQuery(data=b"adm_global_tpl_menu"))
async def adm_global_tpl_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    admin_mark_activity(event.sender_id)
    tpls  = _get_admin_fwd_templates()
    lines = []
    for k, t in tpls.items():
        locked = "🔒 " if t.get("locked") else ""
        lines.append(f"  🌐 {locked}**{t['name']}** — _{t.get('desc','')[:40]}_")
    body = "\n".join(lines) if lines else "  _(koi global template nahi)_"
    txt = (
        "🌐 **ADMIN GLOBAL TEMPLATES**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Ye templates sabhi users ke Templates menu mein\n"
        "'🌐 Admin' category mein dikhenge.\n\n"
        f"**Active ({len(tpls)}):**\n{body}\n\n"
        "💡 Users apply kar sakte hain, edit nahi."
    )
    btns = []
    for k, t in tpls.items():
        locked = "🔒" if t.get("locked") else "🔓"
        btns.append([
            Button.inline(f"🌐 {t['name'][:24]}", f"adm_tpl_edit|{k}".encode()),
            Button.inline(locked,                  f"adm_tpl_lock|{k}".encode()),
            Button.inline("🗑",                    f"adm_tpl_del|{k}".encode()),
        ])
    btns.append([Button.inline("➕ Push Current Settings as Template", b"adm_tpl_push_current")])
    btns.append([Button.inline("🔙 Admin Panel", b"adm_main")])
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_tpl_push_current"))
async def adm_tpl_push_current(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    admin_mark_activity(event.sender_id)
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_tpl_push_name"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "➕ **PUSH GLOBAL TEMPLATE**\n\n"
            "Admin ki current settings sabke liye template banengi.\n\n"
            "Template naam type karo (max 40 chars):",
            buttons=[[Button.inline("❌ Cancel", b"adm_global_tpl_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "adm_tpl_push_name"))
async def adm_tpl_push_name_handler(event):
    uid  = event.sender_id
    if not is_admin(uid): return
    data = get_user_data(uid)
    data["step"] = None
    name = event.raw_text.strip()[:40]
    import random, string, time, datetime

    # Saveable keys — same as user templates
    _KEYS = [
        "text", "image", "video", "voice", "files", "caption",
        "remove_links", "remove_user", "as_document", "auto_shorten",
        "preview_mode", "custom_delay", "smart_filter", "duplicate_filter",
        "product_duplicate_filter", "keyword_filter_enabled", "filter_mode",
        "global_filter", "dup_expiry_hours",
        "start_msg", "end_msg", "start_msg_enabled", "end_msg_enabled",
    ]
    s       = data.get("settings", {})
    snap    = {k: s[k] for k in _KEYS if k in s}
    tpls    = _get_admin_fwd_templates()
    new_key = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))

    tpls[new_key] = {
        "name":       name,
        "desc":       f"Admin template — {datetime.datetime.now().strftime('%d/%m/%y %H:%M')}",
        "settings":   snap,
        "locked":     False,
        "created_at": int(time.time()),
    }
    _save_admin_fwd_templates(tpls)
    add_log(uid, "Global Template", details=f"Pushed: {name}")
    await event.respond(
        f"✅ **Global Template '{name}' pushed!**\n\n"
        f"Sabke 'Admin' category mein dikhega.\n"
        f"{len(snap)} settings saved.",
        buttons=[[Button.inline("🌐 Global Templates", b"adm_global_tpl_menu"),
                  Button.inline("🏠 Admin",            b"adm_main")]]
    )


@bot.on(events.CallbackQuery(pattern=b"adm_tpl_lock\\|(.+)"))
async def adm_tpl_lock(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    key  = event.data.decode().split("|")[1]
    tpls = _get_admin_fwd_templates()
    if key not in tpls:
        return await event.answer("Template nahi mila!", alert=True)
    tpls[key]["locked"] = not tpls[key].get("locked", False)
    _save_admin_fwd_templates(tpls)
    status = "🔒 Locked" if tpls[key]["locked"] else "🔓 Unlocked"
    await event.answer(status, alert=False)
    await adm_global_tpl_menu(event)


@bot.on(events.CallbackQuery(pattern=b"adm_tpl_del\\|(.+)"))
async def adm_tpl_del(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    key  = event.data.decode().split("|")[1]
    tpls = _get_admin_fwd_templates()
    name = tpls.get(key, {}).get("name", "?")
    tpls.pop(key, None)
    _save_admin_fwd_templates(tpls)
    add_log(event.sender_id, "Global Template", details=f"Deleted: {name}")
    await event.answer(f"🗑 '{name}' deleted!", alert=False)
    await adm_global_tpl_menu(event)


@bot.on(events.CallbackQuery(pattern=b"adm_tpl_edit\\|(.+)"))
async def adm_tpl_edit(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    key  = event.data.decode().split("|")[1]
    tpls = _get_admin_fwd_templates()
    tpl  = tpls.get(key)
    if not tpl:
        return await event.answer("Template nahi mila!", alert=True)
    settings = tpl.get("settings", {})
    lines = []
    _LABELS = {
        "text": "Text", "image": "Images", "video": "Videos",
        "remove_links": "Remove links", "smart_filter": "Smart filter",
        "duplicate_filter": "Dup filter", "custom_delay": "Delay",
        "start_msg": "Start msg", "end_msg": "End msg",
    }
    for k, v in settings.items():
        lbl = _LABELS.get(k, k)
        lines.append(f"  {lbl}: `{str(v)[:30]}`")
    tname = tpl['name']
    body  = "\n".join(lines[:10]) if lines else "_(no settings)_"
    try:
        await event.edit(
            f"🌐 **{tname}**\n_{tpl.get('desc','')}_\n\n"
            f"**Settings ({len(settings)}):**\n{body}",
            buttons=[
                [Button.inline("🔙 Back", b"adm_global_tpl_menu")]
            ]
        )
    except errors.MessageNotModifiedError:
        pass
