# anti_spam.py — ADVANCED ANTI-SPAM ENGINE v2.0
# ══════════════════════════════════════════════════════════════════
#
#  FEATURES:
#  1. Rate Limiter        — per-user sliding window (msgs/min + msgs/hour)
#  2. Burst Detector      — sudden spike detection in short window
#  3. Repeat Offender     — tracks violations, auto-block after N strikes
#  4. Shadow Mode         — silently drop messages (no user notification)
#  5. Warn System         — send warning message before block
#  6. Auto-Unblock        — temp block expires after configurable time
#  7. Whitelist           — premium or specific users bypass all checks
#  8. Stats               — per-user + global violation stats
#  9. Admin Alerts        — notify admin when user is auto-blocked
# 10. Keyword Spam Filter — block messages with repeated/banned keywords
#
# INTEGRATION:
#   Call check_spam(user_id, msg_text) in forward_engine.py BEFORE forwarding
#   Returns: (allowed: bool, action: str, reason: str)
#
# ══════════════════════════════════════════════════════════════════

import time
import asyncio
import logging
import re
from collections import defaultdict, deque
from database import GLOBAL_STATE, save_persistent_db, get_user_data
from config import OWNER_ID, logger
from health_monitor import alert_admins

# ── CONFIG KEY ────────────────────────────────────────────────────
_CFG_KEY = "anti_spam_config"

DEFAULT_CONFIG = {
    # Master switch
    "enabled":              True,

    # Rate Limiting — bot forwarding channel se aata hai, not user typing
    # Isliye limits bahut generous hain by default
    "rate_limit_enabled":   True,
    "max_per_min":          60,       # msgs per 60s — channel flood cover karta hai
    "max_per_hour":         1000,     # msgs per hour
    "burst_limit":          20,       # max msgs in burst window
    "burst_window_sec":     5,

    # Violation Handling — default: sirf warn, block mat karo
    "action":               "warn",   # "warn" / "pause" / "shadow" / "block"
    "warn_before_block":    True,
    "max_strikes":          5,        # 5 violations before auto-block (lenient)
    "strike_window_hours":  24,

    # Temp Block — default sirf 10 min
    "temp_block_enabled":   True,
    "temp_block_minutes":   10,

    # Shadow Mode — OFF by default
    "shadow_mode":          False,

    # Keyword Filter — OFF by default (opt-in)
    "keyword_filter":       False,
    "banned_keywords":      [],
    "keyword_action":       "warn",

    # Whitelist — premium bypass ON
    "whitelist_premium":    True,
    "whitelist_uids":       [],

    # Admin Alerts — use configured channel/group
    "alert_on_autoblock":   True,
    "alert_on_burst":       False,

    # Pause Duration
    "pause_minutes":        5,

    # Smart detection — consecutive same-source flood only
    "smart_flood_detect":   True,     # agar same source se ata hai tab hi count karo
    "ignore_small_msg":     True,     # short msgs (< 10 chars) rate limit se exempt
}


# ── IN-MEMORY STATE (resets on restart) ──────────────────────────
# {user_id: deque of timestamps}
# maxlen prevents unbounded growth if cleanup loop is delayed (memory safety)
_min_windows:   dict[int, deque] = defaultdict(lambda: deque(maxlen=2000))
_hour_windows:  dict[int, deque] = defaultdict(lambda: deque(maxlen=10000))
_burst_windows: dict[int, deque] = defaultdict(lambda: deque(maxlen=500))

# {user_id: {"count": int, "first_ts": float}}
_strikes:      dict[int, dict]  = defaultdict(lambda: {"count": 0, "first_ts": 0.0})

# {user_id: unblock_timestamp}  (0 = permanent)
_temp_blocked: dict[int, float] = {}

# {user_id: pause_until_timestamp}
_paused_until: dict[int, float] = {}

# {user_id: last_warning_ts}
_last_warned:  dict[int, float] = {}

# Global stats (persisted to GLOBAL_STATE)
_STATS_KEY = "anti_spam_stats"


# ── CONFIG HELPERS ────────────────────────────────────────────────
def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_config() -> dict:
    cfg = GLOBAL_STATE.setdefault(_CFG_KEY, {})
    for k, v in DEFAULT_CONFIG.items():
        if k not in cfg:
            cfg[k] = v
    return cfg

def set_config_key(key: str, value):
    cfg = get_config()
    cfg[key] = value
    save_persistent_db()

def get_stats() -> dict:
    return GLOBAL_STATE.setdefault(_STATS_KEY, {
        "total_violations": 0,
        "total_auto_blocks": 0,
        "total_warns": 0,
        "total_drops": 0,
        "by_user": {},           # {uid_str: {"violations": N, "blocks": N}}
    })

def _incr_stat(key: str, user_id: int = None, amount: int = 1):
    stats = get_stats()
    stats[key] = stats.get(key, 0) + amount
    if user_id:
        u = stats["by_user"].setdefault(str(user_id), {"violations": 0, "blocks": 0, "warns": 0})
        if key == "total_violations": u["violations"] += amount
        if key == "total_auto_blocks": u["blocks"] += amount
        if key == "total_warns": u["warns"] += amount


# ── WHITELIST CHECK ───────────────────────────────────────────────
def is_whitelisted(user_id: int) -> bool:
    cfg = get_config()
    if user_id == OWNER_ID:
        return True
    if user_id in cfg.get("whitelist_uids", []):
        return True
    if cfg.get("whitelist_premium"):
        try:
            from premium import is_premium_user
            if is_premium_user(user_id):
                return True
        except Exception:
            pass
    try:
        from admin import is_admin
        if is_admin(user_id):
            return True
    except Exception:
        pass
    return False


# ── TEMP BLOCK HELPERS ────────────────────────────────────────────
def is_temp_blocked(user_id: int) -> bool:
    if user_id not in _temp_blocked:
        return False
    unblock_at = _temp_blocked[user_id]
    if unblock_at == 0:
        return True  # permanent
    if time.time() >= unblock_at:
        del _temp_blocked[user_id]
        _incr_stat("total_auto_blocks", user_id, 0)
        logger.info(f"[AntiSpam] User {user_id} auto-unblocked (temp block expired)")
        return False
    return True

def temp_block_user(user_id: int, minutes: int = None):
    cfg = get_config()
    mins = minutes if minutes is not None else cfg.get("temp_block_minutes", 30)
    if mins == 0:
        _temp_blocked[user_id] = 0  # permanent
    else:
        _temp_blocked[user_id] = time.time() + (mins * 60)
    _incr_stat("total_auto_blocks", user_id)
    logger.warning(f"[AntiSpam] User {user_id} temp-blocked for {mins} min")

def unblock_user(user_id: int):
    _temp_blocked.pop(user_id, None)
    _strikes[user_id] = {"count": 0, "first_ts": 0.0}
    logger.info(f"[AntiSpam] User {user_id} manually unblocked")

def get_unblock_time(user_id: int) -> str:
    if user_id not in _temp_blocked:
        return "Not blocked"
    unblock_at = _temp_blocked[user_id]
    if unblock_at == 0:
        return "Permanent"
    remaining = int(unblock_at - time.time())
    if remaining <= 0:
        return "Expired"
    m, s = divmod(remaining, 60)
    return f"{m}m {s}s"


# ── PAUSE HELPERS ─────────────────────────────────────────────────
def is_paused(user_id: int) -> bool:
    if user_id not in _paused_until:
        return False
    if time.time() >= _paused_until[user_id]:
        del _paused_until[user_id]
        return False
    return True

def pause_user(user_id: int, minutes: int = None):
    cfg = get_config()
    mins = minutes if minutes is not None else cfg.get("pause_minutes", 5)
    _paused_until[user_id] = time.time() + (mins * 60)
    logger.info(f"[AntiSpam] User {user_id} paused for {mins} min")


# ── STRIKE SYSTEM ─────────────────────────────────────────────────
def add_strike(user_id: int) -> int:
    """Add a violation strike. Returns current strike count."""
    cfg = get_config()
    window_hrs = cfg.get("strike_window_hours", 24)
    now = time.time()
    s = _strikes[user_id]

    # Reset if window expired
    if s["first_ts"] > 0 and (now - s["first_ts"]) > (window_hrs * 3600):
        s["count"] = 0
        s["first_ts"] = now

    s["count"] += 1
    if s["first_ts"] == 0:
        s["first_ts"] = now

    _incr_stat("total_violations", user_id)
    return s["count"]

def get_strikes(user_id: int) -> int:
    return _strikes.get(user_id, {}).get("count", 0)


# ── KEYWORD FILTER ────────────────────────────────────────────────
def check_keywords(text: str) -> tuple[bool, str]:
    """Returns (violation: bool, matched_keyword: str)"""
    cfg = get_config()
    if not cfg.get("keyword_filter") or not text:
        return False, ""
    keywords = cfg.get("banned_keywords", [])
    text_lower = text.lower()
    for kw in keywords:
        if kw.lower() in text_lower:
            return True, kw
    return False, ""


# ── RATE LIMIT CHECK ──────────────────────────────────────────────
def check_rate_limits(user_id: int) -> tuple[bool, str]:
    """
    Check per-minute, per-hour, and burst windows.
    Returns (ok: bool, reason: str)
    """
    cfg = get_config()
    if not cfg.get("rate_limit_enabled"):
        return True, ""

    now = time.time()

    # ── Per-minute window ────────────────────────────────────────
    win_min = _min_windows[user_id]
    cutoff_min = now - 60
    while win_min and win_min[0] < cutoff_min:
        win_min.popleft()
    if len(win_min) >= cfg.get("max_per_min", 30):
        return False, f"rate_min:{len(win_min)}/{cfg['max_per_min']}"

    # ── Per-hour window ──────────────────────────────────────────
    win_hr = _hour_windows[user_id]
    cutoff_hr = now - 3600
    while win_hr and win_hr[0] < cutoff_hr:
        win_hr.popleft()
    if len(win_hr) >= cfg.get("max_per_hour", 500):
        return False, f"rate_hour:{len(win_hr)}/{cfg['max_per_hour']}"

    # ── Burst window ─────────────────────────────────────────────
    burst_sec = cfg.get("burst_window_sec", 5)
    win_burst = _burst_windows[user_id]
    cutoff_burst = now - burst_sec
    while win_burst and win_burst[0] < cutoff_burst:
        win_burst.popleft()
    if len(win_burst) >= cfg.get("burst_limit", 10):
        return False, f"burst:{len(win_burst)}/{cfg['burst_limit']}@{burst_sec}s"

    # ── Record this message ──────────────────────────────────────
    win_min.append(now)
    win_hr.append(now)
    win_burst.append(now)

    return True, ""


# ── MAIN CHECK (call from forward_engine) ────────────────────────
async def check_spam(user_id: int, msg_text: str = "") -> tuple[bool, str, str]:
    """
    Main entry point. Call BEFORE forwarding.

    Returns:
        (allowed: bool, action: str, reason: str)
        action: "ok" / "warn" / "pause" / "shadow" / "block" / "temp_block"
    """
    cfg = get_config()

    if not cfg.get("enabled"):
        return True, "ok", ""

    if is_whitelisted(user_id):
        # Still record to windows for stats, but always allow
        now = time.time()
        _min_windows[user_id].append(now)
        _hour_windows[user_id].append(now)
        _burst_windows[user_id].append(now)
        return True, "ok", "whitelisted"

    # ── Check if already temp-blocked ────────────────────────────
    if is_temp_blocked(user_id):
        _incr_stat("total_drops", user_id)
        remaining = get_unblock_time(user_id)
        return False, "block", f"temp_blocked (unblocks in {remaining})"

    # ── Check if paused ──────────────────────────────────────────
    if is_paused(user_id):
        _incr_stat("total_drops", user_id)
        return False, "pause", "paused"

    # ── Keyword filter ───────────────────────────────────────────
    kw_hit, matched_kw = check_keywords(msg_text)
    if kw_hit:
        strike_count = add_strike(user_id)
        kw_action = cfg.get("keyword_action", "warn")
        reason = f"keyword:{matched_kw}"
        logger.info(f"[AntiSpam] User {user_id} keyword hit '{matched_kw}' strike={strike_count}")
        return await _handle_violation(user_id, kw_action, reason, strike_count)

    # ── Rate limit check ─────────────────────────────────────────
    ok, rl_reason = check_rate_limits(user_id)
    if not ok:
        # Smart check: agar user ka forwarding chal raha hai normally
        # toh short spikes pe immediately react mat karo
        # Only count as strike if limit is exceeded significantly (>150%)
        cfg_max = cfg.get("max_per_min", 60)
        actual = len(_min_windows.get(user_id, []))
        if actual <= int(cfg_max * 1.5):
            # Marginal overage — silently skip, don't even warn
            return False, "skip", rl_reason
        strike_count = add_strike(user_id)
        action = cfg.get("action", "warn")
        logger.info(f"[AntiSpam] User {user_id} rate exceeded — {rl_reason} strike={strike_count}")
        return await _handle_violation(user_id, action, rl_reason, strike_count)

    return True, "ok", ""


# ── VIOLATION HANDLER ─────────────────────────────────────────────
async def _handle_violation(user_id: int, action: str, reason: str, strike_count: int):
    cfg = get_config()
    max_strikes = cfg.get("max_strikes", 3)

    # Shadow mode — silently drop (user not notified)
    if cfg.get("shadow_mode"):
        _incr_stat("total_drops", user_id)
        return False, "shadow", reason

    # If strikes hit max — auto-block regardless of action
    if strike_count >= max_strikes:
        temp_block_user(user_id)
        _incr_stat("total_drops", user_id)
        asyncio.create_task(_notify_user_blocked(user_id))
        asyncio.create_task(_notify_admin_blocked(user_id, reason, strike_count))
        logger.warning(f"[AntiSpam] User {user_id} auto-blocked after {strike_count} strikes")
        return False, "temp_block", reason

    # Action: warn
    if action == "warn":
        now = time.time()
        last_warn = _last_warned.get(user_id, 0)
        # Don't spam warnings — max 1 per 60s
        if now - last_warn > 60:
            _last_warned[user_id] = now
            _incr_stat("total_warns", user_id)
            asyncio.create_task(_send_warning(user_id, reason, strike_count, max_strikes))
        return True, "warn", reason  # Still allow this message

    # Action: pause
    elif action == "pause":
        pause_user(user_id)
        _incr_stat("total_drops", user_id)
        asyncio.create_task(_send_warning(user_id, reason, strike_count, max_strikes, paused=True))
        return False, "pause", reason

    # Action: block
    elif action == "block":
        temp_block_user(user_id)
        _incr_stat("total_drops", user_id)
        asyncio.create_task(_notify_user_blocked(user_id))
        asyncio.create_task(_notify_admin_blocked(user_id, reason, strike_count))
        return False, "temp_block", reason

    # Default: allow but warn
    _incr_stat("total_warns", user_id)
    return True, "warn", reason


# ── NOTIFICATIONS ─────────────────────────────────────────────────
async def _send_warning(user_id: int, reason: str, strikes: int, max_strikes: int, paused: bool = False):
    try:
        from config import bot
        cfg = get_config()
        max_s = cfg.get("max_strikes", 3)
        remaining_strikes = max(0, max_s - strikes)
        strike_bar = "🔴" * strikes + "⚪" * remaining_strikes

        if paused:
            mins = cfg.get("pause_minutes", 5)
            msg = (
                "⏸️ **Forwarding Thodi Der Ke Liye Ruki**\n\n"
                f"Tumhare source channel se messages bahut fast aa rahe hain.\n"
                f"Ye normal hai agar channel mein flood tha — bot ne automatically pause kiya.\n\n"
                f"⏰ **{mins} minutes** mein automatically resume ho jaayegi.\n"
                f"Strike: {strike_bar} ({strikes}/{max_s})\n\n"
                "Kuch karne ki zarurat nahi, wait karo! 😊\n" + _get_owner_footer()
            )
        else:
            msg = (
                "⚠️ **High Speed Alert**\n\n"
                "Tumhare source se messages bahut tezi se aa rahe hain.\n"
                "Ye usually tab hota hai jab source channel mein bahut posts hote hain ek saath.\n\n"
                f"Strike: {strike_bar} ({strikes}/{max_s})\n"
                f"⚠️ {remaining_strikes} aur warning ke baad forwarding temporarily ruk jaayegi.\n\n"
                "💡 **Tip:** Agar ye aksar aata hai:\n"
                "• Global Settings mein delay badhao\n"
                "• Ya Scheduler se time limit lagao\n\n" + _get_owner_footer()
            )
        await bot.send_message(user_id, msg)
    except Exception as e:
        logger.debug(f"[AntiSpam] Warning send failed for {user_id}: {e}")


async def _notify_user_blocked(user_id: int):
    try:
        from config import bot
        cfg = get_config()
        mins = cfg.get("temp_block_minutes", 10)
        msg = (
            "🔴 **Forwarding Temporarily Stopped**\n\n"
            "Tumhare source channel se bahut zyada messages ek saath aa rahe the.\n"
            "Bot ne automatically forwarding rok di — koi bhi message permanently delete nahi hua.\n\n"
            f"⏰ **{mins if mins > 0 else '∞'} minutes** mein automatically wapas shuru ho jaayegi.\n\n"
            "💡 Agar ye baar baar hota hai:\n"
            "• `/start` → Global Settings → Delay badhao\n"
            "• Ya admin se contact karo\n\n" + _get_owner_footer()
        )
        await bot.send_message(user_id, msg)
    except Exception as e:
        logger.debug(f"[AntiSpam] Block notif failed for {user_id}: {e}")


async def _notify_admin_blocked(user_id: int, reason: str, strikes: int):
    try:
        cfg = get_config()
        if not cfg.get("alert_on_autoblock"):
            return
        data = get_user_data(user_id)
        profile = data.get("profile", {})
        name = profile.get("first_name", str(user_id))
        username = "@" + profile.get("username") if profile.get("username") else "—"
        mins = cfg.get("temp_block_minutes", 30)
        msg = (
            f"🚨 **Anti-Spam Auto-Block**\n\n"
            f"👤 User: [{name}](tg://user?id={user_id}) ({username})\n"
            f"🆔 ID: `{user_id}`\n"
            f"⚡ Reason: `{reason}`\n"
            f"🔴 Strikes: `{strikes}`\n"
            f"⏰ Block: {mins if mins > 0 else '∞'} min\n\n"
            f"Unblock: `/admin` → Users → Profile → Unblock"
        )
        # Use configured alert destinations (channel/group/admin)
        from health_monitor import alert_admins
        await alert_admins(msg)
    except Exception as e:
        logger.debug(f"[AntiSpam] Admin alert failed: {e}")


# ── STATS HELPERS ─────────────────────────────────────────────────
def get_user_spam_info(user_id: int) -> dict:
    """For admin panel — full info about a user's spam status."""
    stats = get_stats()
    u_stats = stats["by_user"].get(str(user_id), {})
    return {
        "is_blocked":   is_temp_blocked(user_id),
        "is_paused":    is_paused(user_id),
        "strikes":      get_strikes(user_id),
        "violations":   u_stats.get("violations", 0),
        "warns":        u_stats.get("warns", 0),
        "blocks":       u_stats.get("blocks", 0),
        "unblock_time": get_unblock_time(user_id),
        "msgs_min":     len(_min_windows.get(user_id, [])),
        "msgs_hour":    len(_hour_windows.get(user_id, [])),
    }

def get_global_stats() -> dict:
    stats = get_stats()
    return {
        "total_violations": stats.get("total_violations", 0),
        "total_auto_blocks": stats.get("total_auto_blocks", 0),
        "total_warns":       stats.get("total_warns", 0),
        "total_drops":       stats.get("total_drops", 0),
        "active_blocks":     len(_temp_blocked),
        "active_pauses":     len(_paused_until),
        "top_offenders":     _get_top_offenders(5),
    }

def _get_top_offenders(n: int = 5) -> list:
    stats = get_stats()
    by_user = stats.get("by_user", {})
    sorted_users = sorted(
        by_user.items(),
        key=lambda x: x[1].get("violations", 0),
        reverse=True
    )
    return [(int(uid), info) for uid, info in sorted_users[:n]]


# ── BACKGROUND: Auto-cleanup expired blocks ───────────────────────
async def auto_cleanup_loop():
    """Run every 5 min — clean expired temp blocks and stale windows."""
    while True:
        try:
            await asyncio.sleep(300)
            now = time.time()
            # Clean expired temp blocks
            expired = [uid for uid, ts in _temp_blocked.items() if ts != 0 and ts <= now]
            for uid in expired:
                del _temp_blocked[uid]
                logger.info(f"[AntiSpam] Auto-unblocked {uid}")
            # Clean expired pauses
            exp_pauses = [uid for uid, ts in _paused_until.items() if ts <= now]
            for uid in exp_pauses:
                del _paused_until[uid]
            # Clean stale per-min windows (>2 min old)
            for uid in list(_min_windows.keys()):
                w = _min_windows[uid]
                while w and w[0] < now - 120:
                    w.popleft()
                if not w:
                    del _min_windows[uid]

            # FIX 11: Clean _hour_windows (>1 hour old) — was leaking memory!
            for uid in list(_hour_windows.keys()):
                w = _hour_windows[uid]
                while w and w[0] < now - 3600:
                    w.popleft()
                if not w:
                    del _hour_windows[uid]

            # FIX 11: Clean _burst_windows (>30s old)
            for uid in list(_burst_windows.keys()):
                w = _burst_windows[uid]
                while w and w[0] < now - 30:
                    w.popleft()
                if not w:
                    del _burst_windows[uid]

            # FIX 11: Clean stale strikes (>24h old)
            stale_strikes = [uid for uid, d in _strikes.items()
                             if d.get("first_ts", 0) < now - 86400]
            for uid in stale_strikes:
                del _strikes[uid]

            # FIX 11: Clean old warnings (>1h old)
            stale_warned = [uid for uid, ts in _last_warned.items() if ts < now - 3600]
            for uid in stale_warned:
                del _last_warned[uid]

        except Exception as e:
            logger.debug(f"[AntiSpam] cleanup_loop error: {e}")
