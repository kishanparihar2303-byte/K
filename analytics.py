# bot/analytics.py
# ==========================================
# ANALYTICS & STATS SYSTEM
# Daily/Weekly/Monthly message stats
# Admin aur User dono ke liye
# ==========================================

import time
import datetime
from telethon import events, Button, errors
from config import bot, OWNER_ID
from database import db, GLOBAL_STATE, get_user_data, save_persistent_db
from admin import is_admin


def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_today_key():
    return datetime.datetime.now().strftime("%Y-%m-%d")


def get_analytics_data(user_id: int) -> dict:
    """User ka analytics data — daily stats store karta hai।"""
    data = get_user_data(user_id)
    data.setdefault("analytics", {
        "daily": {},       # {"2026-03-01": {"forwarded": 10, "blocked": 2}}
        "joined_at": int(time.time()),
    })
    return data["analytics"]


def record_message(user_id: int, stat_type: str = "forwarded"):
    """
    Har forward/block hone par call karo — daily stats mein add karta hai।
    stat_type: "forwarded" ya "blocked"
    """
    try:
        analytics = get_analytics_data(user_id)
        today = get_today_key()
        analytics["daily"].setdefault(today, {"forwarded": 0, "blocked": 0})
        analytics["daily"][today][stat_type] = analytics["daily"][today].get(stat_type, 0) + 1
        # ── v3: Hourly tracking ────────────────────────────────────────
        try:
            import datetime as _dth
            _hour = _dth.datetime.now().hour
            _hourly = analytics.setdefault("hourly", {})
            _hourly[str(_hour)] = _hourly.get(str(_hour), 0) + 1
        except Exception:
            pass
        # ──────────────────────────────────────────────────────────────

        # BUG 23 FIX: Last 31 days rakho (timezone safe) — sorted by actual date
        if len(analytics["daily"]) > 31:
            import datetime as _dt
            today = _dt.date.today()
            cutoff = (today - _dt.timedelta(days=31)).isoformat()
            stale = [k for k in analytics["daily"].keys() if k < cutoff]
            for stale_key in stale:
                del analytics["daily"][stale_key]
    except Exception:
        pass


def get_user_summary(user_id: int) -> dict:
    """User ka complete analytics summary।"""
    analytics = get_analytics_data(user_id)
    daily = analytics.get("daily", {})

    today = get_today_key()
    today_data = daily.get(today, {"forwarded": 0, "blocked": 0})

    # Last 7 days
    week_fwd = 0
    week_blk = 0
    for i in range(7):
        day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        d = daily.get(day, {})
        week_fwd += d.get("forwarded", 0)
        week_blk += d.get("blocked", 0)

    # BUG 13 FIX: All time stats from analytics daily sum (accurate, no double-count)
    data = get_user_data(user_id)
    # Use db stats as fallback but prefer analytics sum if available
    total_fwd_db = data.get("stats", {}).get("processed", 0)
    total_blk_db = data.get("stats", {}).get("blocked", 0)
    # Sum all analytics days for more accurate total
    total_fwd = sum(d.get("forwarded", 0) for d in daily.values()) or total_fwd_db
    total_blk = sum(d.get("blocked", 0) for d in daily.values()) or total_blk_db

    # Joined date
    joined_ts = analytics.get("joined_at", int(time.time()))
    joined = datetime.datetime.fromtimestamp(joined_ts).strftime("%d/%m/%Y")

    return {
        "today_forwarded": today_data.get("forwarded", 0),
        "today_blocked": today_data.get("blocked", 0),
        "week_forwarded": week_fwd,
        "week_blocked": week_blk,
        "total_forwarded": total_fwd,
        "total_blocked": total_blk,
        "joined": joined,
        "daily": daily,
    }


def get_global_summary() -> dict:
    """Poore bot ka analytics — admin ke liye।"""
    today = get_today_key()
    total_today = 0
    total_week = 0
    total_all = 0
    active_today = 0

    for uid, udata in list(db.items()):
        if not isinstance(udata, dict):
            continue
        analytics = udata.get("analytics", {})
        daily = analytics.get("daily", {})

        # Today
        t = daily.get(today, {})
        tf = t.get("forwarded", 0)
        total_today += tf
        if tf > 0:
            active_today += 1

        # Week
        for i in range(7):
            day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            d = daily.get(day, {})
            total_week += d.get("forwarded", 0)

        # All time
        total_all += udata.get("stats", {}).get("processed", 0)

    return {
        "today": total_today,
        "week": total_week,
        "total": total_all,
        "active_today": active_today,
        "total_users": len(db),
    }


def make_bar_chart(daily: dict, days: int = 7) -> str:
    """Simple text-based bar chart — last N days।"""
    bars = []
    max_val = 1
    data_points = []

    for i in range(days - 1, -1, -1):
        day = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        val = daily.get(day, {}).get("forwarded", 0)
        label = (datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%d/%m")
        data_points.append((label, val))
        if val > max_val:
            max_val = val

    chart = "📊 **Last 7 Days:**\n"
    for label, val in data_points:
        bar_len = int((val / max_val) * 10) if max_val > 0 else 0
        bar = "█" * bar_len + "░" * (10 - bar_len)
        chart += f"`{label}` {bar} {val}\n"

    return chart


# ==========================================
# USER — /stats COMMAND
# ==========================================

@bot.on(events.NewMessage(pattern='/stats'))
async def stats_cmd(event):
    """
    /stats command — full detailed stats (separate from dashboard).
    Dashboard is the main hub; /stats gives a text-only deep view.
    """
    user_id = event.sender_id
    summary = get_user_summary(user_id)
    chart   = make_bar_chart(get_analytics_data(user_id).get("daily", {}), days=14)
    data    = get_user_data(user_id)

    # Block rate this week
    week_total = summary["week_forwarded"] + summary["week_blocked"]
    blk_rate   = f"{int(summary['week_blocked']/week_total*100)}%" if week_total else "0%"

    # Top forwarding day this month
    daily   = get_analytics_data(user_id).get("daily", {})
    best_day = max(daily.items(), key=lambda x: x[1].get("forwarded", 0), default=(None, {}))
    best_txt = f"`{best_day[0]}` ({best_day[1].get('forwarded',0)} msgs)" if best_day[0] else "N/A"

    txt = (
        "📊 **Detailed Stats Report**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Bot se joined: `{summary['joined']}`\n\n"
        "**📆 Aaj:**\n"
        f"  ✅ Forwarded: `{summary['today_forwarded']}`  "
        f"❌ Blocked: `{summary['today_blocked']}`\n\n"
        "**📅 Is Hafte (7 din):**\n"
        f"  ✅ Forwarded: `{summary['week_forwarded']}`  "
        f"❌ Blocked: `{summary['week_blocked']}`  "
        f"📊 Block Rate: `{blk_rate}`\n\n"
        "**📈 All Time:**\n"
        f"  ✅ Total Forwarded: `{summary['total_forwarded']}`\n"
        f"  ❌ Total Blocked: `{summary['total_blocked']}`\n"
        f"  🏆 Best Day: {best_txt}\n\n"
        f"{chart}\n" + _get_owner_footer()
    )

    await event.respond(txt, buttons=[
        [Button.inline("📊 Open Dashboard", b"dashboard_view")],
        [Button.inline("🏠 Main Menu", b"main_menu")]
    ])


# Note: stats_refresh callback is now handled as alias in main_menu.py → dashboard_view


# ==========================================
# ADMIN — GLOBAL ANALYTICS
# ==========================================

# ══════════════════════════════════════════
# DEEP ANALYTICS — Full Admin UI
# ══════════════════════════════════════════

def _get_user_display(uid) -> str:
    """User ka naam/username return karo, fallback ID."""
    try:
        p = db.get(int(uid), {}).get("profile", {})
        first = p.get("first_name", "")
        last  = p.get("last_name",  "")
        uname = p.get("username",   "")
        full  = (first + " " + last).strip()
        if uname:
            return f"{full} (@{uname})" if full else f"@{uname}"
        return full or str(uid)
    except Exception:
        return str(uid)


def _build_leaderboard(period: str = "today") -> list:
    """
    period = "today" | "week" | "alltime"
    Returns sorted list of (uid, name, count)
    """
    today = get_today_key()
    result = []
    for uid, udata in list(db.items()):
        if not isinstance(udata, dict):
            continue
        if period == "today":
            val = udata.get("analytics", {}).get("daily", {}).get(today, {}).get("forwarded", 0)
        elif period == "week":
            val = sum(
                udata.get("analytics", {}).get("daily", {})
                    .get((datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d"), {})
                    .get("forwarded", 0)
                for i in range(7)
            )
        else:  # alltime
            val = udata.get("stats", {}).get("processed", 0)
        if val > 0:
            result.append((uid, _get_user_display(uid), val))
    result.sort(key=lambda x: x[2], reverse=True)
    return result


def _make_global_bar_chart(days: int = 7) -> str:
    """Bot-wide daily forwarded — last N days text bar chart."""
    today_dt = datetime.datetime.now()
    points   = []
    max_val  = 1
    for i in range(days - 1, -1, -1):
        day = (today_dt - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        lbl = (today_dt - datetime.timedelta(days=i)).strftime("%d/%m")
        val = sum(
            udata.get("analytics", {}).get("daily", {}).get(day, {}).get("forwarded", 0)
            for udata in list(db.values()) if isinstance(udata, dict)
        )
        points.append((lbl, val))
        if val > max_val:
            max_val = val
    chart = "📊 **Last 7 Days (Bot-wide):**\n"
    for lbl, val in points:
        bar_len = int((val / max_val) * 10) if max_val > 0 else 0
        bar = "█" * bar_len + "░" * (10 - bar_len)
        chart += f"`{lbl}` {bar} {val}\n"
    return chart


@bot.on(events.CallbackQuery(data=b"adm_analytics"))
async def adm_analytics(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    await _show_analytics_overview(event)


async def _show_analytics_overview(event):
    summary = get_global_summary()
    chart   = _make_global_bar_chart(7)
    board   = _build_leaderboard("today")
    top_txt = ""
    if board:
        top_txt = "\n🏆 **Top 5 Aaj:**\n"
        medals  = ["🥇","🥈","🥉","4️⃣","5️⃣"]
        for i, (uid, name, val) in enumerate(board[:5]):
            top_txt += f"  {medals[i]} {name[:22]} — `{val}`\n"

    now = datetime.datetime.now().strftime("%d/%m %I:%M %p")
    txt = (
        "📊 **Deep Analytics**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒 Updated: `{now}`\n\n"
        f"👥 Total Users: `{summary['total_users']}`  "
        f"🟢 Active Today: `{summary['active_today']}`\n\n"
        f"📆 **Aaj:** `{summary['today']}` msgs forwarded\n"
        f"📅 **Is Hafte:** `{summary['week']}` msgs\n"
        f"📈 **All Time:** `{summary['total']}` msgs\n\n"
        f"{chart}"
        f"{top_txt}"
    )
    btns = [
        [Button.inline("🏆 Leaderboard Aaj",    b"adm_lb_today"),
         Button.inline("📅 Leaderboard Week",   b"adm_lb_week")],
        [Button.inline("🏅 All-Time Top",        b"adm_lb_alltime"),
         Button.inline("📤 CSV Export",          b"adm_analytics_csv")],
        [Button.inline("🔄 Refresh",             b"adm_analytics"),
         Button.inline("🔙 Back",                b"adm_main")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        await event.answer("Already updated!")


# ── Leaderboard pages ──────────────────────────────────

async def _show_leaderboard(event, period: str, page: int = 0):
    board = _build_leaderboard(period)
    MAX   = 10
    start = page * MAX
    end   = start + MAX
    subset = board[start:end]
    total  = len(board)

    period_labels = {"today": "📆 Aaj", "week": "📅 Is Hafte", "alltime": "📈 All Time"}
    medals = ["🥇","🥈","🥉"] + ["🔹"] * 50

    txt = (
        f"🏆 **Leaderboard — {period_labels.get(period, period)}**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
    )
    if not subset:
        txt += "\nAbhi koi data nahi hai."
    else:
        for i, (uid, name, val) in enumerate(subset):
            rank = start + i
            txt += f"{medals[rank]} `{rank+1}.` {name[:25]} — **{val}** msgs\n"

    btns = []
    nav = []
    if page > 0:
        nav.append(Button.inline("⬅️ Prev", f"adm_lb_{period}_{page-1}".encode()))
    if end < total:
        nav.append(Button.inline("Next ➡️", f"adm_lb_{period}_{page+1}".encode()))
    if nav:
        btns.append(nav)

    # User drill-down buttons (top 5 on page 0)
    if page == 0 and subset:
        btns.append([Button.inline("🔍 User Detail Dekho", b"adm_analytics_user_pick")])

    btns.append([Button.inline("🔙 Overview", b"adm_analytics")])
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        await event.answer("Already updated!")


@bot.on(events.CallbackQuery(data=b"adm_lb_today"))
async def adm_lb_today(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    await _show_leaderboard(event, "today", 0)

@bot.on(events.CallbackQuery(data=b"adm_lb_week"))
async def adm_lb_week(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    await _show_leaderboard(event, "week", 0)

@bot.on(events.CallbackQuery(data=b"adm_lb_alltime"))
async def adm_lb_alltime(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    await _show_leaderboard(event, "alltime", 0)

@bot.on(events.CallbackQuery(pattern=b"adm_lb_today_"))
async def adm_lb_today_paged(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    page = int(event.data.decode().split("_")[-1])
    await _show_leaderboard(event, "today", page)

@bot.on(events.CallbackQuery(pattern=b"adm_lb_week_"))
async def adm_lb_week_paged(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    page = int(event.data.decode().split("_")[-1])
    await _show_leaderboard(event, "week", page)

@bot.on(events.CallbackQuery(pattern=b"adm_lb_alltime_"))
async def adm_lb_alltime_paged(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    page = int(event.data.decode().split("_")[-1])
    await _show_leaderboard(event, "alltime", page)


# ── User pick for drill-down ───────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_analytics_user_pick"))
async def adm_analytics_user_pick(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from database import get_user_data as _gud
    _gud(event.sender_id)["step"] = "adm_analytics_uid_input"
    _gud(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "🔍 **User Analytics**\n\n"
        "Jis user ka detail dekhna hai uska **User ID** bhejo:\n"
        "_(Ya phir Leaderboard se kisi user ka ID copy karke bhejo)_",
        buttons=[Button.inline("❌ Cancel", b"adm_analytics")]
    )


# ── Per-User Analytics Drill-down ─────────────────────

async def _show_user_analytics(event, target_uid: int):
    """Ek specific user ka full analytics dikhao."""
    from database import db as _db
    udata = _db.get(target_uid)
    if not udata:
        try:
            await event.edit(
                "❌ **User nahi mila!**\n\n"
                f"User ID `{target_uid}` database mein exist nahi karta.\n"
                "Sahi User ID check karo.",
                buttons=[[Button.inline("🔙 Analytics", b"adm_analytics")]]
            )
        except Exception:
            await event.answer("❌ User not found!", alert=True)
        return

    summary = get_user_summary(target_uid)
    daily   = get_analytics_data(target_uid).get("daily", {})
    chart   = make_bar_chart(daily, days=7) if daily else "📊 _Abhi koi forwarding data nahi hai_\n"
    name    = _get_user_display(target_uid)

    # 30-day total
    total_30 = sum(
        udata.get("analytics", {}).get("daily", {})
            .get((datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d"), {})
            .get("forwarded", 0)
        for i in range(30)
    )

    prem   = "💎 Premium" if udata.get("premium", {}).get("active") else "🆓 Free"
    status = "🟢 Active" if udata.get("settings", {}).get("running") else "🔴 Stopped"
    srcs   = len(udata.get("sources", []))
    dests  = len(udata.get("destinations", []))

    # Empty state — user joined but never forwarded
    has_activity = summary.get("total_forwarded", 0) > 0
    activity_note = "" if has_activity else "\n⚠️ _Is user ne abhi tak koi message forward nahi kiya._\n"

    txt = (
        f"📊 **User Analytics**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 {name}\n"
        f"🆔 `{target_uid}`\n"
        f"{status}  {prem}\n"
        f"📥 Sources: {srcs}  📤 Dests: {dests}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{activity_note}"
        f"📆 **Aaj:** `{summary['today_forwarded']}` fwd  `{summary['today_blocked']}` blocked\n"
        f"📅 **Is Hafte:** `{summary['week_forwarded']}` fwd\n"
        f"📅 **30 Din:** `{total_30}` fwd\n"
        f"📈 **All Time:** `{summary['total_forwarded']}` fwd\n\n"
        f"{chart}"
    )
    btns = [
        [Button.inline("👤 User Profile", f"adm_view_u_{target_uid}".encode())],
        [Button.inline("🔙 Analytics",    b"adm_analytics")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        await event.answer("Updated!")


# ── CSV Export ────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_analytics_csv"))
async def adm_analytics_csv(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    await event.answer("⏳ CSV generate ho raha hai...", alert=False)

    import io as _io
    buf = _io.StringIO()
    buf.write("user_id,name,today,week,alltime\n")
    today = get_today_key()
    for uid, udata in list(db.items()):
        if not isinstance(udata, dict): continue
        name  = _get_user_display(uid).replace(",", " ")
        t_fwd = udata.get("analytics", {}).get("daily", {}).get(today, {}).get("forwarded", 0)
        w_fwd = sum(
            udata.get("analytics", {}).get("daily", {})
                .get((datetime.datetime.now() - datetime.timedelta(days=i)).strftime("%Y-%m-%d"), {})
                .get("forwarded", 0)
            for i in range(7)
        )
        a_fwd = udata.get("stats", {}).get("processed", 0)
        buf.write(f"{uid},{name},{t_fwd},{w_fwd},{a_fwd}\n")

    buf.seek(0)
    out = _io.BytesIO(buf.getvalue().encode())
    out.name = f"analytics_{datetime.datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    await event.respond("📊 **Analytics CSV Export:**", file=out)
    await event.answer("Sent!")
