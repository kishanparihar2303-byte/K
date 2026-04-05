"""
task_board.py  v2.0 — Production Task Board + Growth Engine

NEW vs v1:
  ✅ Anti-cheat system  — duplicate/bot submissions detect karo
  ✅ User levels & tiers — Bronze → Silver → Gold → Platinum
  ✅ Daily streak        — consecutive day bonus multiplier
  ✅ Task categories     — bundle tasks by campaign/sponsor
  ✅ Wallet system       — coins + transaction history
  ✅ Sponsor dashboard   — sponsor khud stats dekhe
  ✅ Smart verification  — URL-based auto-verify (Telegram join)
  ✅ Growth analytics    — referral funnel, conversion tracking
  ✅ Cooldown per task-type — spam prevention
  ✅ Bonus events        — admin sets 2x/3x coin periods
  ✅ Withdrawal system   — coins → UPI/premium/custom rewards
"""

import asyncio, hashlib, logging, random, time, uuid
from collections import defaultdict

logger = logging.getLogger(__name__)

# FIX 18: Per-user coin operation locks (race condition prevention)
import threading as _threading
_coin_locks: dict = {}
_coin_locks_mutex = _threading.Lock()
_coin_locks_last_cleanup: float = 0.0

def _get_coin_lock(user_id: int) -> "_threading.Lock":
    global _coin_locks_last_cleanup
    with _coin_locks_mutex:
        if user_id not in _coin_locks:
            _coin_locks[user_id] = _threading.Lock()
        return _coin_locks[user_id]

def _cleanup_coin_locks(max_locks: int = 1000):
    """Remove coin locks for users beyond the max limit (LRU-style eviction)."""
    global _coin_locks_last_cleanup
    import time as _t
    now = _t.time()
    if now - _coin_locks_last_cleanup < 600:  # Only every 10 min
        return
    _coin_locks_last_cleanup = now
    with _coin_locks_mutex:
        if len(_coin_locks) > max_locks:
            # Remove oldest entries (first inserted)
            excess = len(_coin_locks) - max_locks
            for k in list(_coin_locks.keys())[:excess]:
                del _coin_locks[k]


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

TASK_TYPES = {
    "like_post":    ("❤️", "Like karo",          "Post like karo"),
    "comment_post": ("💬", "Comment karo",        "Post pe comment do"),
    "watch_video":  ("▶️", "Video dekho",         "Video watch karo"),
    "follow_page":  ("➕", "Follow/Subscribe",    "Page ya channel follow karo"),
    "join_channel": ("📣", "Channel join karo",   "Telegram channel join karo"),
    "visit_link":   ("🔗", "Link visit karo",     "Website ya link visit karo"),
    "share_post":   ("📤", "Share karo",          "Post share karo"),
    "review_app":   ("⭐", "Review/Rating do",    "App ya service ko rate karo"),
    "signup":       ("📝", "Register/Signup",     "Platform pe signup karo"),
    "purchase":     ("🛒", "Purchase karo",       "Product purchase karo"),
    "custom":       ("✨", "Custom task",         "Custom action karo"),
}

PLATFORMS = {
    "instagram": "📸 Instagram",
    "youtube":   "▶️  YouTube",
    "facebook":  "📘 Facebook",
    "telegram":  "✈️  Telegram",
    "twitter":   "🐦 Twitter/X",
    "website":   "🌐 Website",
    "playstore": "📱 Play Store",
    "appstore":  "🍎 App Store",
    "amazon":    "🛍 Amazon",
    "flipkart":  "🛒 Flipkart",
    "other":     "🔗 Other",
}

# Level thresholds (total lifetime coins)
LEVELS = [
    (0,    "🥉 Bronze",   1.0),
    (500,  "🥈 Silver",   1.1),
    (2000, "🥇 Gold",     1.25),
    (5000, "💎 Platinum", 1.5),
    (15000,"👑 Diamond",  2.0),
]

# Cooldown per task type (seconds) — same user, same type
TASK_TYPE_COOLDOWN = {
    "like_post":    300,    # 5 min
    "comment_post": 600,    # 10 min
    "watch_video":  120,    # 2 min
    "follow_page":  0,      # no cooldown (one-time)
    "join_channel": 0,
    "visit_link":   60,
    "share_post":   1800,   # 30 min
    "review_app":   0,
    "signup":       0,
    "purchase":     0,
    "custom":       60,
}

DEFAULT_CFG = {
    "enabled":           True,
    "tasks":             {},
    "categories":        {},      # {cat_id: {name, color, sponsor}}
    "coin_name":         "Coins",
    "coin_symbol":       "🪙",
    "coins_to_premium":  500,
    "max_daily_tasks":   30,
    "bonus_multiplier":  1.0,     # Admin sets 2x/3x events
    "bonus_until":       0.0,
    "total_coins_given": 0,
    "total_tasks_done":  0,
    "total_users":       0,
    "growth": {
        "enabled":           False,
        "referral_bonus":    75,
        "share_kit_text":    "",
        "promo_channels":    [],
        "auto_interval":     86400,
        "last_auto_post":    0,
        "referral_funnel":   {},  # {uid: {clicks, signups, actives}}
    },
    "withdrawal": {
        "enabled":       False,
        "min_coins":     1000,
        "methods":       [],   # [{type, label, details}]
        "pending":       [],   # withdrawal requests
    },
    "anti_cheat": {
        "enabled":       True,
        "flag_threshold": 3,   # 3 flags → auto-ban from tasks
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# CORE CONFIG
# ─────────────────────────────────────────────────────────────────────────────

def _cfg() -> dict:
    from database import GLOBAL_STATE
    # Migrate v1 key if exists
    if "task_board_cfg" in GLOBAL_STATE:
        GLOBAL_STATE.pop("task_board_cfg", None)
    cfg = GLOBAL_STATE.setdefault("task_cfg_v2", {})
    for k, v in DEFAULT_CFG.items():
        if isinstance(v, dict):
            cfg.setdefault(k, {})
            for kk, vv in v.items():
                cfg[k].setdefault(kk, vv)
        else:
            cfg.setdefault(k, v)
    return cfg

def _save():
    try:
        from database import save_persistent_db
        save_persistent_db()
    except Exception: pass

# ─────────────────────────────────────────────────────────────────────────────
# TASK CRUD
# ─────────────────────────────────────────────────────────────────────────────

def create_task(
    task_type: str,
    platform: str,
    title: str,
    description: str,
    link: str,
    reward_coins: int    = 10,
    sponsor_name: str    = "",
    sponsor_uid: int     = 0,
    expires_hours: int   = 72,
    max_completions: int = 0,
    proof_required: bool = False,
    proof_hint: str      = "",
    category_id: str     = "",
    bonus_coins: int     = 0,      # Extra one-time bonus for first N completions
    bonus_slots: int     = 0,
    priority: int        = 0,      # Higher = shown first
) -> str:
    tid  = f"t_{uuid.uuid4().hex[:8]}"
    icon, short, _ = TASK_TYPES.get(task_type, ("✨", task_type, ""))
    _cfg()["tasks"][tid] = {
        "id":             tid,
        "type":           task_type,
        "platform":       platform,
        "icon":           icon,
        "title":          title,
        "description":    description,
        "link":           link,
        "reward_coins":   reward_coins,
        "sponsor_name":   sponsor_name,
        "sponsor_uid":    sponsor_uid,
        "category_id":    category_id,
        "active":         True,
        "created":        time.time(),
        "expires_at":     time.time() + expires_hours * 3600,
        "max_completions": max_completions,
        "bonus_coins":    bonus_coins,
        "bonus_slots":    bonus_slots,
        "priority":       priority,
        "proof_required": proof_required,
        "proof_hint":     proof_hint,
        "completions":    0,
        "views":          0,
        "completed_by":   {},    # {uid: timestamp}
        "flags":          [],    # anti-cheat flags
    }
    _save()
    return tid

def get_task(tid: str) -> dict | None:
    return _cfg()["tasks"].get(tid)

def list_tasks(active_only=True, category_id=None, platform=None, sort="priority") -> list[dict]:
    now   = time.time()
    tasks = list(_cfg()["tasks"].values())
    if active_only:
        tasks = [t for t in tasks if (
            t.get("active") and
            t.get("expires_at", 0) > now and
            (t.get("max_completions", 0) == 0 or
             t.get("completions", 0) < t.get("max_completions", 0))
        )]
    if category_id:
        tasks = [t for t in tasks if t.get("category_id") == category_id]
    if platform:
        tasks = [t for t in tasks if t.get("platform") == platform]

    if sort == "priority":
        tasks.sort(key=lambda t: (-(t.get("priority",0)), -t.get("reward_coins",0)))
    elif sort == "reward":
        tasks.sort(key=lambda t: -t.get("reward_coins", 0))
    elif sort == "newest":
        tasks.sort(key=lambda t: -t.get("created", 0))
    elif sort == "ending":
        tasks.sort(key=lambda t: t.get("expires_at", 0))
    return tasks

def delete_task(tid: str) -> bool:
    if tid in _cfg()["tasks"]:
        del _cfg()["tasks"][tid]
        _save(); return True
    return False

def toggle_task(tid: str) -> bool:
    t = _cfg()["tasks"].get(tid)
    if t:
        t["active"] = not t.get("active", True)
        _save(); return t["active"]
    return False

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORIES
# ─────────────────────────────────────────────────────────────────────────────

def create_category(name: str, sponsor: str = "", color: str = "🟦") -> str:
    cid = f"cat_{uuid.uuid4().hex[:6]}"
    _cfg()["categories"][cid] = {"id": cid, "name": name, "sponsor": sponsor, "color": color}
    _save(); return cid

def list_categories() -> list[dict]:
    return list(_cfg()["categories"].values())

# ─────────────────────────────────────────────────────────────────────────────
# USER PROFILE — level, streak, wallet
# ─────────────────────────────────────────────────────────────────────────────

def get_user_profile(user_id: int) -> dict:
    from database import get_user_data
    d   = get_user_data(user_id)
    tp  = d.setdefault("task_profile", {
        "coins":          0,
        "lifetime_coins": 0,
        "streak":         0,
        "last_task_date": "",
        "total_done":     0,
        "flags":          0,
        "banned":         False,
        "coin_log":       [],
        "type_cooldowns": {},   # {task_type: last_done_ts}
    })
    return tp

def get_user_coins(user_id: int) -> int:
    return get_user_profile(user_id).get("coins", 0)

def get_user_level(user_id: int) -> tuple:
    """Returns (level_name, multiplier, next_threshold)"""
    lifetime = get_user_profile(user_id).get("lifetime_coins", 0)
    level    = LEVELS[0]
    for threshold, name, mult in LEVELS:
        if lifetime >= threshold:
            level = (threshold, name, mult)
    idx = LEVELS.index(level)
    nxt = LEVELS[idx+1][0] if idx < len(LEVELS)-1 else None
    return level[1], level[2], nxt

def get_streak_multiplier(user_id: int) -> float:
    p     = get_user_profile(user_id)
    today = time.strftime("%Y-%m-%d")
    last  = p.get("last_task_date","")
    yesterday = time.strftime("%Y-%m-%d", time.localtime(time.time()-86400))

    if last == today:
        streak = p.get("streak", 1)
    elif last == yesterday:
        streak = p.get("streak", 0) + 1
        p["streak"] = streak
    else:
        p["streak"] = 1; streak = 1
    p["last_task_date"] = today

    # Streak bonus: +5% per day, max +50%
    return min(1.0 + (streak - 1) * 0.05, 1.5)

def add_coins(user_id: int, base_amount: int, reason: str = "") -> dict:
    """
    Add coins with all multipliers applied.
    Returns {coins_added, total, level, streak, multipliers}
    """
    p  = get_user_profile(user_id)
    if p.get("banned"):
        return {"coins_added": 0, "total": p["coins"], "blocked": True}

    cfg = _cfg()
    # Global bonus event
    global_mult = 1.0
    if cfg.get("bonus_until", 0) > time.time():
        global_mult = cfg.get("bonus_multiplier", 1.0)

    # Level multiplier
    _, level_mult, _ = get_user_level(user_id)

    # Streak multiplier
    streak_mult = get_streak_multiplier(user_id)

    final = max(1, round(base_amount * global_mult * level_mult * streak_mult))

    # FIX 18: Atomic update with per-user lock (race condition prevention)
    with _get_coin_lock(user_id):
        # Re-read profile inside lock to get latest value
        p = get_user_profile(user_id)
        p["coins"]          = p.get("coins", 0) + final
        p["lifetime_coins"] = p.get("lifetime_coins", 0) + final
        p["total_done"]     = p.get("total_done", 0) + 1

        log = p.setdefault("coin_log", [])
        log.append({"amt": final, "base": base_amount, "reason": reason[:40], "t": time.time()})
        if len(log) > 200: p["coin_log"] = log[-100:]

        cfg["total_coins_given"] = cfg.get("total_coins_given", 0) + final

        from database import get_user_data
        get_user_data(user_id)["task_profile"] = p
        _save()

    level_name, _, _ = get_user_level(user_id)
    return {
        "coins_added":    final,
        "base_amount":    base_amount,
        "total":          p["coins"],
        "lifetime":       p["lifetime_coins"],
        "level":          level_name,
        "streak":         p.get("streak", 1),
        "global_mult":    global_mult,
        "level_mult":     level_mult,
        "streak_mult":    round(streak_mult, 2),
        "blocked":        False,
    }

def spend_coins(user_id: int, amount: int, reason: str = "") -> bool:
    """Spend coins atomically with per-user lock to prevent double-spend race conditions."""
    with _get_coin_lock(user_id):
        # Re-read inside lock for latest value (same pattern as add_coins)
        p = get_user_profile(user_id)
        if p.get("coins", 0) < amount:
            return False
        p["coins"] -= amount
        log = p.setdefault("coin_log", [])
        log.append({"amt": -amount, "reason": reason[:40], "t": time.time()})
        # Trim coin_log to prevent unbounded growth (add_coins already does this)
        if len(log) > 200:
            p["coin_log"] = log[-100:]
        from database import get_user_data
        get_user_data(user_id)["task_profile"] = p
        _save()
    return True

def get_leaderboard(top_n=10, by="coins") -> list[dict]:
    from database import db
    scores = []
    for uid, d in db.items():
        tp = d.get("task_profile", {})
        if tp.get("lifetime_coins", 0) > 0:
            scores.append({
                "uid":      uid,
                "coins":    tp.get("coins", 0),
                "lifetime": tp.get("lifetime_coins", 0),
                "done":     tp.get("total_done", 0),
                "streak":   tp.get("streak", 0),
                "level":    get_user_level(uid)[0],
            })
    key = "lifetime" if by == "lifetime" else "coins"
    return sorted(scores, key=lambda x: -x[key])[:top_n]

# ─────────────────────────────────────────────────────────────────────────────
# ANTI-CHEAT
# ─────────────────────────────────────────────────────────────────────────────

_submit_log: dict[int, list] = defaultdict(list)   # {uid: [ts, ts, ...]}

def _anti_cheat_check(user_id: int, task_id: str) -> tuple[bool, str]:
    cfg   = _cfg()
    if not cfg["anti_cheat"].get("enabled", True):
        return True, "ok"

    p = get_user_profile(user_id)
    if p.get("banned"):
        return False, "❌ Tumhara task board access suspended hai."

    flags = p.get("flags", 0)
    threshold = cfg["anti_cheat"].get("flag_threshold", 3)
    if flags >= threshold:
        p["banned"] = True
        from database import get_user_data; get_user_data(user_id)["task_profile"] = p; _save()
        return False, "❌ Suspicious activity detected. Account suspended."

    # Rate check: >10 submissions in 60 seconds = flag
    now = time.time()
    log = _submit_log[user_id]
    log.append(now)
    _submit_log[user_id] = [t for t in log if now - t < 60]
    # Cleanup inactive users from submit_log (>1h no activity)
    if len(_submit_log) > 1000:
        stale = [u for u, ts in _submit_log.items() if not ts or now - ts[-1] > 3600]
        for u in stale[:200]: del _submit_log[u]
    if len(_submit_log[user_id]) > 10:
        p["flags"] = p.get("flags", 0) + 1
        from database import get_user_data; get_user_data(user_id)["task_profile"] = p; _save()
        logger.warning(f"Anti-cheat flag: uid={user_id} rapid submissions")
        return False, "⚠️ Bahut fast! Thoda ruko."

    return True, "ok"

# ─────────────────────────────────────────────────────────────────────────────
# TASK COMPLETION ENGINE
# ─────────────────────────────────────────────────────────────────────────────

_daily_done: dict[str, dict] = {}   # {"YYYY-MM-DD": {uid: [tid,...]}}

def _today_key() -> str:
    return time.strftime("%Y-%m-%d")

def _user_done_today(user_id: int) -> list:
    return _daily_done.get(_today_key(), {}).get(user_id, [])

def can_do_task(user_id: int, task_id: str) -> tuple[bool, str]:
    t   = get_task(task_id)
    now = time.time()
    if not t:                            return False, "Task nahi mila!"
    if not t.get("active"):              return False, "Task inactive hai."
    if t.get("expires_at", 0) < now:    return False, "⌛ Task expire ho gaya!"
    if (t.get("max_completions", 0) > 0
            and t.get("completions", 0) >= t["max_completions"]):
        return False, "👥 Sabhi spots bhar gaye!"
    if user_id in t.get("completed_by", {}):
        return False, "✅ Ye task tum pehle kar chuke ho!"

    # Daily limit
    done_today = len(_user_done_today(user_id))
    max_d      = _cfg().get("max_daily_tasks", 30)
    if done_today >= max_d:
        return False, f"📋 Aaj ke {max_d} tasks ho gaye! Kal aao."

    # Task-type cooldown
    p        = get_user_profile(user_id)
    cooldown = TASK_TYPE_COOLDOWN.get(t.get("type",""), 60)
    if cooldown > 0:
        last_cd = p.get("type_cooldowns", {}).get(t.get("type",""), 0)
        if now - last_cd < cooldown:
            remaining = int(cooldown - (now - last_cd))
            return False, f"⏱ {remaining}s baad try karo!"

    return True, "ok"


def complete_task(user_id: int, task_id: str) -> dict:
    ok, reason = can_do_task(user_id, task_id)
    if not ok:
        return {"ok": False, "msg": reason}

    ok2, reason2 = _anti_cheat_check(user_id, task_id)
    if not ok2:
        return {"ok": False, "msg": reason2}

    t   = _cfg()["tasks"].get(task_id)
    if not t:
        return {"ok": False, "msg": "❌ Task nahi mila (delete ho gaya)."}
    now = time.time()

    # Mark complete
    t["completions"] = t.get("completions", 0) + 1
    t.setdefault("completed_by", {})[user_id] = now
    _cfg()["total_tasks_done"] = _cfg().get("total_tasks_done", 0) + 1

    # Daily tracking
    today = _today_key()
    _daily_done.setdefault(today, {}).setdefault(user_id, []).append(task_id)

    # Update type cooldown
    p = get_user_profile(user_id)
    p.setdefault("type_cooldowns", {})[t.get("type","")] = now
    from database import get_user_data
    get_user_data(user_id)["task_profile"] = p

    # Calculate reward
    base   = t.get("reward_coins", 10)
    bonus  = 0
    if t.get("bonus_coins", 0) > 0 and t.get("completions", 0) <= t.get("bonus_slots", 0):
        bonus = t["bonus_coins"]

    result = add_coins(user_id, base + bonus, f"Task: {t['title'][:30]}")

    cfg    = _cfg()
    sym    = cfg.get("coin_symbol", "🪙")
    name   = cfg.get("coin_name", "Coins")
    streak = result.get("streak", 1)

    parts = [f"✅ +{result['coins_added']} {sym}"]
    if bonus > 0:
        parts.append(f"(incl. {bonus} bonus!)")
    if streak > 1:
        parts.append(f"🔥 {streak}d streak!")
    if result.get("level_mult", 1.0) > 1.0:
        parts.append(f"🏆 Level bonus ×{result['level_mult']}")
    if result.get("global_mult", 1.0) > 1.0:
        parts.append(f"⚡ Event ×{result['global_mult']}!")
    parts.append(f"Total: {result['total']} {sym}")

    _save()
    return {
        "ok":     True,
        "msg":    " ".join(parts),
        "coins":  result["coins_added"],
        "total":  result["total"],
        "level":  result["level"],
        "streak": streak,
    }

# ─────────────────────────────────────────────────────────────────────────────
# TELEGRAM JOIN AUTO-VERIFY
# ─────────────────────────────────────────────────────────────────────────────

async def verify_telegram_join(user_id: int, channel: str, client) -> bool:
    """Auto-verify Telegram channel join — no proof needed."""
    try:
        entity    = await client.get_entity(channel)
        from telethon.tl.functions.channels import GetParticipantRequest
        await client(GetParticipantRequest(entity, user_id))
        return True
    except Exception:
        return False

# ─────────────────────────────────────────────────────────────────────────────
# REDEEM SYSTEM
# ─────────────────────────────────────────────────────────────────────────────

async def redeem_for_premium(user_id: int, days: int) -> dict:
    rate  = _cfg().get("coins_to_premium", 500)
    cost  = rate * days
    coins = get_user_coins(user_id)
    if coins < cost:
        return {"ok": False, "msg": f"❌ {cost - coins} aur chahiye! ({coins}/{cost})"}
    if not spend_coins(user_id, cost, f"{days}d premium"):
        return {"ok": False, "msg": "❌ Spend error!"}
    try:
        from premium import give_premium
        await give_premium(user_id, days, given_by=0)
        sym = _cfg().get("coin_symbol","🪙")
        return {"ok": True, "msg": f"🎉 {days} day Premium! {cost} {sym} use hue."}
    except Exception as e:
        add_coins(user_id, cost, "Refund: premium error")
        return {"ok": False, "msg": f"❌ Error: {e}"}

def request_withdrawal(user_id: int, coins: int, method: str, details: str) -> dict:
    wd = _cfg()["withdrawal"]
    if not wd.get("enabled"):
        return {"ok": False, "msg": "❌ Withdrawal abhi available nahi."}
    min_c = wd.get("min_coins", 1000)
    if coins < min_c:
        return {"ok": False, "msg": f"❌ Minimum {min_c} coins required!"}
    if not spend_coins(user_id, coins, f"Withdrawal: {method}"):
        return {"ok": False, "msg": "❌ Insufficient coins!"}
    req_id = f"wd_{uuid.uuid4().hex[:6]}"
    wd.setdefault("pending", []).append({
        "id": req_id, "uid": user_id, "coins": coins,
        "method": method, "details": details,
        "ts": time.time(), "status": "pending"
    })
    _save()
    return {"ok": True, "msg": f"✅ Withdrawal request `{req_id}` submitted!", "id": req_id}

# ─────────────────────────────────────────────────────────────────────────────
# GROWTH ENGINE
# ─────────────────────────────────────────────────────────────────────────────

_PROMO_TEXTS = [
    "🤖 **Auto Forwarding Bot** — Free!\n\n✅ Multiple sources → destinations\n✅ Filters, scheduler, watermark\n✅ Affiliate links auto-inject\n✅ 25-language translation\n\n👉 Try: @{bot}",
    "📡 **Telegram Channel Forwarding Bot**\n\nSetup in 5 minutes, runs 24/7.\nNo coding required!\n\n🔧 Sources, Destinations, Smart Filters\n💎 Premium features available\n\n👉 @{bot}",
    "⚡ **Forward Messages Automatically!**\n\nChannel se channel, group se group.\nAuto-forward with custom filters.\n\nFree to use! 👇\n@{bot}",
]

def get_share_kit(user_id: int) -> dict:
    from refer import get_bot_link
    bot  = get_bot_link()
    link = f"https://t.me/{bot}?start=ref_{user_id}"
    txt  = _cfg()["growth"].get("share_kit_text", "").replace("{ref_link}", link) or (
        f"🤖 Auto forwarding Telegram bot try karo!\nMessages automatically forward karo.\n👉 {link}"
    )
    return {
        "ref_link":  link,
        "full":      txt,
        "short":     f"⚡ Auto forwarding bot: {link}",
        "whatsapp":  f"Telegram auto forward bot: {link}",
    }

def get_promo_text() -> str:
    from refer import get_bot_link
    return random.choice(_PROMO_TEXTS).replace("{bot}", get_bot_link())

async def auto_post(bot_client) -> int:
    ge = _cfg()["growth"]
    if not ge.get("enabled"): return 0
    chs = ge.get("promo_channels", [])
    if not chs: return 0
    if time.time() - ge.get("last_auto_post", 0) < ge.get("auto_interval", 86400): return 0

    text = get_promo_text()
    sent = 0
    for ch in chs:
        try:
            await bot_client.send_message(ch, text, parse_mode="md")
            sent += 1; await asyncio.sleep(2)
        except Exception as e:
            logger.debug(f"Auto-post fail {ch}: {e}")
    if sent:
        ge["last_auto_post"] = time.time()
        _save()
    return sent

def referral_coin_bonus(referrer_id: int):
    bonus = _cfg()["growth"].get("referral_bonus", 75)
    if bonus > 0:
        add_coins(referrer_id, bonus, "Referral bonus")

# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────

def get_board_stats() -> dict:
    from database import db
    cfg    = _cfg()
    tasks  = cfg.get("tasks", {})
    active = [t for t in tasks.values()
              if t.get("active") and t.get("expires_at",0) > time.time()]
    users_active = sum(1 for d in db.values()
                       if d.get("task_profile",{}).get("total_done",0) > 0)
    top    = max(tasks.values(), key=lambda t: t.get("completions",0), default=None)
    month  = time.strftime("%Y-%m")
    return {
        "enabled":        cfg.get("enabled"),
        "active_tasks":   len(active),
        "total_tasks":    len(tasks),
        "total_done":     cfg.get("total_tasks_done", 0),
        "total_coins":    cfg.get("total_coins_given", 0),
        "users_active":   users_active,
        "coin_name":      cfg.get("coin_name","Coins"),
        "coin_symbol":    cfg.get("coin_symbol","🪙"),
        "coins_to_prem":  cfg.get("coins_to_premium", 500),
        "max_daily":      cfg.get("max_daily_tasks", 30),
        "bonus_on":       cfg.get("bonus_until",0) > time.time(),
        "bonus_mult":     cfg.get("bonus_multiplier",1.0),
        "top_task":       top.get("title","—") if top else "—",
        "top_completions":top.get("completions",0) if top else 0,
        "growth_enabled": cfg["growth"].get("enabled",False),
        "promo_channels": len(cfg["growth"].get("promo_channels",[])),
        "withdrawal_on":  cfg["withdrawal"].get("enabled",False),
    }

def get_task_stats(tid: str) -> dict:
    t   = get_task(tid)
    if not t: return {}
    imp = t.get("views",0)
    cmp = t.get("completions",0)
    cvr = round(cmp / max(1, imp) * 100, 1)
    remaining = max(0, t.get("expires_at",0) - time.time())
    return {
        "views":      imp,
        "completions": cmp,
        "cvr":        cvr,
        "remaining":  remaining,
        "coins_paid": cmp * t.get("reward_coins",0),
        "active":     t.get("active",False) and remaining > 0,
    }

# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND
# ─────────────────────────────────────────────────────────────────────────────

async def maintenance_loop(bot_client):
    while True:
        await asyncio.sleep(1800)
        try:
            now = time.time()
            exp = 0
            for t in _cfg()["tasks"].values():
                if t.get("active") and t.get("expires_at",0) < now:
                    t["active"] = False; exp += 1
            if exp: _save(); logger.info(f"Expired {exp} tasks")

            # Clean old daily_done entries (keep only today + yesterday)
            keep  = {time.strftime("%Y-%m-%d"), time.strftime("%Y-%m-%d", time.localtime(now-86400))}
            stale = [k for k in _daily_done if k not in keep]
            for k in stale: del _daily_done[k]

            await auto_post(bot_client)
        except Exception as e:
            logger.error(f"task maintenance: {e}")
