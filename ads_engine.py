"""
ads_engine.py — Production-Grade Bot Monetization Engine v2.0

INCOME STREAMS:
  1. 📢 Banner Ads      — Main menu text mein sponsor block
  2. 🔘 Button Ads      — Menu mein clickable sponsor button
  3. 📣 Popup Ads       — Alag sponsored message (frequency-controlled)
  4. ⏰ Scheduled Blast — Cron-style: sab users ko sponsor message
  5. 💎 Ad-Free Plans   — Users ko ad-free premium upsell
  6. 🎯 Targeted Ads    — Premium vs Free users ko alag ads

ADVANCED FEATURES:
  • Per-ad frequency caps (daily/lifetime impression limit)
  • A/B testing — 2 variants ka performance compare karo
  • Click tracking with UTM-style unique links
  • Real-time CTR, eCPM, estimated revenue
  • Ad scheduling — specific time window par hi dikho
  • Blacklist — specific users ko specific ads mat dikho
  • Rotation strategies: Round Robin / Weighted / Random
  • Cooldown: same user ko same ad N min mein dobara nahi
  • Blast rate limiting — Telegram flood se bachao
"""

import asyncio
import hashlib
import logging
import math
import random
import time
import uuid
from collections import defaultdict, deque
from typing import Optional
from telethon import Button
from time_helper import ab_now

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

AD_TYPES = {
    "banner":  "📢 Banner  (menu text ke saath)",
    "button":  "🔘 Button  (inline button menu mein)",
    "popup":   "📣 Pop-up  (alag sponsored message)",
    "blast":   "⏰ Blast   (sab users scheduled message)",
}

ROTATION = {
    "roundrobin": "🔄 Round Robin (order mein)",
    "weighted":   "⚖️ Weighted   (zyada CPM = zyada dikho)",
    "random":     "🎲 Random     (random pick)",
}

DEFAULT_CFG = {
    "enabled":           False,
    "ads":               {},
    "rotation":          "weighted",
    "banner_freq":       1,       # Har N menu opens mein 1 banner
    "popup_freq":        8,       # Har N menu opens mein 1 popup
    "popup_cooldown":    1800,    # Same user same ad — min 30 min gap (sec)
    "blast_interval":    21600,   # Har 6 ghante (0 = off)
    "blast_batch":       30,      # Ek baar mein max 30 users (flood safe)
    "blast_batch_delay": 0.08,    # Har message ke beech delay (sec)
    "skip_premium":      True,    # Premium users = ad-free by default
    "ad_free_price":     49,      # Ad-free monthly price ₹
    "total_earned":      0.0,
    "pending_payout":    0.0,
    "paid_out":          0.0,
    "monthly":           {},
    "payout_log":        [],
    "click_log":         [],
    "ab_tests":          {},      # {test_id: {variant_a, variant_b, stats}}
}

# ─────────────────────────────────────────────────────────────────────────────
# IN-MEMORY STATE (never persisted — reset on bot restart is fine)
# ─────────────────────────────────────────────────────────────────────────────

_menu_opens:     dict[int, int]   = defaultdict(int)    # user_id → open count
_popup_shown:    dict[str, float] = {}                   # "uid:ad_id" → last shown ts
_rr_index:       dict[str, int]   = defaultdict(int)    # per-type round-robin idx
_blast_sent:     dict[int, float] = {}                   # user_id → last blast ts
_click_tokens:   dict[str, tuple] = {}                   # token → (ad_id, url, ts)
_popup_last_clean: float = 0.0                           # last cleanup timestamp

def _clean_popup_cache():
    """FIX: _popup_shown unbounded memory — clean stale entries."""
    global _popup_last_clean
    import time as _t
    now = _t.time()
    if now - _popup_last_clean < 3600:
        return
    _popup_last_clean = now
    cfg = _cfg()
    cooldown = cfg.get("popup_cooldown", 1800)
    cutoff = now - cooldown * 2
    stale = [k for k, v in list(_popup_shown.items()) if v < cutoff]
    for k in stale:
        del _popup_shown[k]
    # Also clean click tokens older than 24h
    token_cutoff = now - 86400
    stale_tokens = [k for k, (aid, url, ts) in list(_click_tokens.items()) if ts < token_cutoff]
    for k in stale_tokens:
        del _click_tokens[k]

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG ACCESS
# ─────────────────────────────────────────────────────────────────────────────

def _cfg() -> dict:
    from database import GLOBAL_STATE
    cfg = GLOBAL_STATE.setdefault("ads_config", {})
    # Fill missing keys without overwriting existing
    for k, v in DEFAULT_CFG.items():
        cfg.setdefault(k, v)
    return cfg


def _save():
    try:
        from database import save_persistent_db
        save_persistent_db()
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# AD SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

def _new_ad(
    ad_type: str,
    title: str,
    text: str,
    url: str          = "",
    btn_label: str    = "",
    sponsor: str      = "",
    cpm: float        = 10.0,
    weight: int       = 100,
    daily_cap: int    = 0,         # 0 = unlimited
    lifetime_cap: int = 0,         # 0 = unlimited
    schedule_start: str = "",      # "09:00"
    schedule_end:   str = "",      # "22:00"
    variant_of: str   = "",        # A/B test parent id
    # ── Targeting ───────────────────────────────
    target_premium: bool = True,   # Show to premium users?
    target_free:    bool = True,   # Show to free users?
    target_active:  bool = False,  # Only users with running=True?
    min_sources:    int  = 0,      # Min sources user must have
    # ── Frequency ───────────────────────────────
    user_daily_cap: int  = 0,      # Max times per user per day (0=unlimited)
    user_total_cap: int  = 0,      # Max times per user ever (0=unlimited)
) -> dict:
    return {
        "id":             f"ad_{uuid.uuid4().hex[:8]}",
        "type":           ad_type,
        "title":          title,
        "text":           text,
        "url":            url,
        "btn_label":      btn_label or "🔗 Visit Now",
        "sponsor":        sponsor,
        "cpm":            float(cpm),
        "weight":         int(weight),
        "daily_cap":      int(daily_cap),
        "lifetime_cap":   int(lifetime_cap),
        "schedule_start": schedule_start,
        "schedule_end":   schedule_end,
        "variant_of":     variant_of,
        # Targeting
        "target_premium": target_premium,
        "target_free":    target_free,
        "target_active":  target_active,
        "min_sources":    int(min_sources),
        # Per-user frequency
        "user_daily_cap": int(user_daily_cap),
        "user_total_cap": int(user_total_cap),
        "user_impressions": {},    # {str(uid): {"total": N, "today": N, "date": "YYYY-MM-DD"}}
        "user_clicks":    {},      # {str(uid): count}  — dedup
        # Stats
        "active":         True,
        "impressions":    0,
        "clicks":         0,
        "unique_clicks":  0,
        "earned":         0.0,
        "daily_impressions": {},   # {"YYYY-MM-DD": count}
        "created":        time.time(),
        "last_shown":     0.0,
        "paused_reason":  "",      # auto-pause reason
    }

# ─────────────────────────────────────────────────────────────────────────────
# CRUD
# ─────────────────────────────────────────────────────────────────────────────

def create_ad(**kwargs) -> str:
    ad = _new_ad(**kwargs)
    _cfg()["ads"][ad["id"]] = ad
    _save()
    logger.info(f"Ad created: {ad['id']} type={ad['type']} sponsor={ad.get('sponsor')} "
                f"target=prem:{ad.get('target_premium')} free:{ad.get('target_free')}")
    return ad["id"]


def update_ad(ad_id: str, **fields) -> bool:
    ad = _cfg()["ads"].get(ad_id)
    if not ad:
        return False
    ad.update(fields)
    _save()
    return True


def delete_ad(ad_id: str) -> bool:
    if ad_id in _cfg()["ads"]:
        del _cfg()["ads"][ad_id]
        _save()
        return True
    return False


def toggle_ad(ad_id: str) -> bool:
    ad = _cfg()["ads"].get(ad_id)
    if ad:
        ad["active"] = not ad["active"]
        _save()
        return ad["active"]
    return False


def list_ads(ad_type: str = None, active_only: bool = False) -> list[dict]:
    ads = list(_cfg()["ads"].values())
    if ad_type:
        ads = [a for a in ads if a["type"] == ad_type]
    if active_only:
        ads = [a for a in ads if a.get("active")]
    return sorted(ads, key=lambda a: a.get("created", 0), reverse=True)


def get_ad(ad_id: str) -> dict | None:
    return _cfg()["ads"].get(ad_id)

# ─────────────────────────────────────────────────────────────────────────────
# ELIGIBILITY CHECKS
# ─────────────────────────────────────────────────────────────────────────────

def _is_ad_free(user_id: int) -> bool:
    if not _cfg().get("skip_premium", True):
        return False
    try:
        from premium import is_premium_user
        if is_premium_user(user_id):
            return True
    except Exception:
        pass
    try:
        from database import get_user_data
        return bool(get_user_data(user_id).get("ad_free"))
    except Exception:
        return False


def _ad_eligible(ad: dict, user_id: int) -> bool:
    """All eligibility checks for showing an ad to a user."""
    if not ad.get("active"):
        return False

    # ── Targeting checks ──────────────────────────────────────────────────────
    if user_id > 0:
        try:
            from premium import is_premium_user
            is_prem = is_premium_user(user_id)
        except Exception:
            is_prem = False

        # Premium targeting
        if is_prem and not ad.get("target_premium", True):
            return False
        if not is_prem and not ad.get("target_free", True):
            return False

        # Active-only targeting
        if ad.get("target_active"):
            try:
                from database import get_user_data
                if not get_user_data(user_id).get("settings", {}).get("running"):
                    return False
            except Exception:
                pass

        # Min sources targeting
        min_srcs = ad.get("min_sources", 0)
        if min_srcs > 0:
            try:
                from database import get_user_data
                if len(get_user_data(user_id).get("sources", [])) < min_srcs:
                    return False
            except Exception:
                pass

        # Per-user daily cap
        uid_str   = str(user_id)
        udata     = ad.get("user_impressions", {}).get(uid_str, {})
        today_str = time.strftime("%Y-%m-%d")
        udaily_cap = ad.get("user_daily_cap", 0)
        if udaily_cap > 0:
            if udata.get("date") == today_str and udata.get("today", 0) >= udaily_cap:
                return False

        # Per-user total cap
        utotal_cap = ad.get("user_total_cap", 0)
        if utotal_cap > 0 and udata.get("total", 0) >= utotal_cap:
            return False

    # ── Schedule window ───────────────────────────────────────────────────────
    start = ad.get("schedule_start", "")
    end   = ad.get("schedule_end",   "")
    if start and end:
        try:
            from time_helper import ab_now
            now_t = ab_now(user_id)
            sh, sm = map(int, start.split(":"))
            eh, em = map(int, end.split(":"))
            cur_min = now_t.hour * 60 + now_t.minute
            s_min   = sh * 60 + sm
            e_min   = eh * 60 + em
            if not (s_min <= cur_min <= e_min):
                return False
        except Exception:
            pass

    # ── Global daily cap ──────────────────────────────────────────────────────
    today = time.strftime("%Y-%m-%d")
    daily_cap = ad.get("daily_cap", 0)
    if daily_cap > 0:
        daily_count = ad.get("daily_impressions", {}).get(today, 0)
        if daily_count >= daily_cap:
            return False

    # ── Lifetime cap ──────────────────────────────────────────────────────────
    lt_cap = ad.get("lifetime_cap", 0)
    if lt_cap > 0 and ad.get("impressions", 0) >= lt_cap:
        return False

    return True


def _popup_cooldown_ok(user_id: int, ad_id: str) -> bool:
    cooldown = _cfg().get("popup_cooldown", 1800)
    key      = f"{user_id}:{ad_id}"
    last     = _popup_shown.get(key, 0)
    # Periodic cleanup of old popup entries
    if len(_popup_shown) > 5000:
        cutoff = time.time() - max(cooldown * 2, 7200)
        stale  = [k for k, v in _popup_shown.items() if v < cutoff]
        for k in stale[:1000]: del _popup_shown[k]
    return (time.time() - last) >= cooldown

# ─────────────────────────────────────────────────────────────────────────────
# AD SELECTION — rotation strategies
# ─────────────────────────────────────────────────────────────────────────────

def _pick(ad_type: str, user_id: int = 0, popup_check: bool = False) -> dict | None:
    candidates = [
        a for a in list_ads(ad_type, active_only=True)
        if _ad_eligible(a, user_id)
        and (not popup_check or _popup_cooldown_ok(user_id, a["id"]))
    ]
    if not candidates:
        return None

    strategy = _cfg().get("rotation", "weighted")

    if strategy == "roundrobin":
        idx = _rr_index[ad_type] % len(candidates)
        _rr_index[ad_type] += 1
        return candidates[idx]

    elif strategy == "weighted":
        # Higher CPM * weight = more chances
        total = sum(max(1, a.get("weight", 100)) for a in candidates)
        r     = random.uniform(0, total)
        cum   = 0
        for a in candidates:
            cum += max(1, a.get("weight", 100))
            if r <= cum:
                return a
        return candidates[-1]

    else:  # random
        return random.choice(candidates)

# ─────────────────────────────────────────────────────────────────────────────
# IMPRESSION & CLICK TRACKING
# ─────────────────────────────────────────────────────────────────────────────

def _record_impression(ad_id: str, user_id: int = 0):
    ad = _cfg()["ads"].get(ad_id)
    if not ad:
        return
    ad["impressions"] = ad.get("impressions", 0) + 1
    ad["last_shown"]  = time.time()

    today = time.strftime("%Y-%m-%d")
    ad.setdefault("daily_impressions", {})[today] =         ad["daily_impressions"].get(today, 0) + 1

    # Per-user impression tracking
    if user_id > 0:
        uid_str = str(user_id)
        udata   = ad.setdefault("user_impressions", {}).setdefault(uid_str, {"total": 0, "today": 0, "date": ""})
        if udata.get("date") != today:
            udata["today"] = 0
            udata["date"]  = today
        udata["total"] = udata.get("total", 0) + 1
        udata["today"] = udata.get("today", 0) + 1

        # Trim user_impressions if too large (memory guard)
        if len(ad.get("user_impressions", {})) > 10000:
            ui = ad["user_impressions"]
            # Keep top 5000 by total impressions
            sorted_uids = sorted(ui.items(), key=lambda x: -x[1].get("total", 0))
            ad["user_impressions"] = dict(sorted_uids[:5000])

    earn = ad.get("cpm", 10.0) / 1000.0
    ad["earned"] = ad.get("earned", 0.0) + earn
    _cfg()["total_earned"]   = _cfg().get("total_earned", 0.0) + earn
    _cfg()["pending_payout"] = _cfg().get("pending_payout", 0.0) + earn
    month = time.strftime("%Y-%m")
    _cfg().setdefault("monthly", {})[month] =         _cfg()["monthly"].get(month, 0.0) + earn

    # Auto-pause when lifetime cap hit
    lt_cap = ad.get("lifetime_cap", 0)
    if lt_cap > 0 and ad.get("impressions", 0) >= lt_cap and ad.get("active"):
        ad["active"]        = False
        ad["paused_reason"] = "lifetime_cap_reached"
        logger.info(f"Ad {ad_id} auto-paused: lifetime cap {lt_cap} reached")


def generate_click_token(ad_id: str, url: str) -> str:
    """Unique trackable token per click."""
    token = hashlib.md5(f"{ad_id}:{url}:{time.time()}:{random.random()}".encode()).hexdigest()[:12]
    _click_tokens[token] = (ad_id, url, time.time())
    # Cleanup old tokens (>24h)
    if len(_click_tokens) > 500:
        cutoff = time.time() - 86400
        for k in [k for k, v in _click_tokens.items() if v[2] < cutoff]:
            del _click_tokens[k]
    return token


def resolve_click(token: str, user_id: int = 0) -> str:
    """Token redeem karo — URL return karo + click record karo."""
    entry = _click_tokens.pop(token, None)
    if not entry:
        return ""
    ad_id, url, _ = entry
    ad = _cfg()["ads"].get(ad_id)
    if ad:
        ad["clicks"] = ad.get("clicks", 0) + 1
        # Unique click tracking — same user same ad = no double count
        uid_str = str(user_id) if user_id else "anon"
        user_clicks = ad.setdefault("user_clicks", {})
        is_new_user = uid_str not in user_clicks
        user_clicks[uid_str] = user_clicks.get(uid_str, 0) + 1
        if is_new_user:
            ad["unique_clicks"] = ad.get("unique_clicks", 0) + 1
        log = _cfg().setdefault("click_log", [])
        log.append({"ad": ad_id, "t": time.time(), "uid": user_id, "unique": is_new_user})
        if len(log) > 1000:
            _cfg()["click_log"] = log[-500:]
    return url

# ─────────────────────────────────────────────────────────────────────────────
# AD RENDER — each type
# ─────────────────────────────────────────────────────────────────────────────

def get_banner_text(user_id: int) -> str:
    """
    Returns banner block to append to main menu text.
    "" if no ad or user is ad-free.
    """
    if not _cfg().get("enabled") or _is_ad_free(user_id):
        return ""

    opens = _menu_opens[user_id]
    freq  = max(1, _cfg().get("banner_freq", 1))
    if opens % freq != 0:
        return ""

    ad = _pick("banner", user_id)
    if not ad:
        return ""

    _record_impression(ad["id"], user_id)

    sponsor_line = f"\n_— {ad['sponsor']}_" if ad.get("sponsor") else ""
    cta = ""
    if ad.get("url"):
        label = ad.get("btn_label") or "Dekhiye →"
        cta   = f"\n[{label}]({ad['url']})"

    return (
        f"\n\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"📣 _Sponsored_{sponsor_line}\n"
        f"{ad['text']}{cta}"
    )


def get_button_ad(user_id: int):
    """Returns Telethon Button or None."""
    if not _cfg().get("enabled") or _is_ad_free(user_id):
        return None
    ad = _pick("button", user_id)
    if not ad or not ad.get("url"):
        return None
    _record_impression(ad["id"], user_id)
    try:
        from telethon import Button
        token  = generate_click_token(ad["id"], ad["url"])
        label  = ad.get("btn_label") or "Sponsored"
        prefix = "📣 " if not any(ord(c) > 127 for c in label[:2]) else ""
        return Button.inline(f"{prefix}{label}", f"adclick|{token}|{user_id}".encode())
    except Exception:
        return None


async def maybe_send_popup(user_id: int, bot) -> bool:
    """Send popup ad if frequency + cooldown allow it."""
    if not _cfg().get("enabled") or _is_ad_free(user_id):
        return False

    opens = _menu_opens[user_id]
    freq  = max(1, _cfg().get("popup_freq", 8))
    if opens == 0 or opens % freq != 0:
        return False

    ad = _pick("popup", user_id, popup_check=True)
    if not ad:
        return False

    _popup_shown[f"{user_id}:{ad['id']}"] = time.time()
    # FIX 12: Cleanup old entries (>2x cooldown time) to prevent memory leak
    if len(_popup_shown) > 5000:
        _cutoff = time.time() - (cfg.get("popup_cooldown", 1800) * 2)
        stale = [k for k, v in list(_popup_shown.items()) if v < _cutoff]
        for k in stale:
            del _popup_shown[k]
    _record_impression(ad["id"])

    try:
        from telethon import Button
        sponsor  = f"\n\n📣 _Sponsor: {ad['sponsor']}_" if ad.get("sponsor") else ""
        text     = f"📢 **Sponsored**\n\n{ad['text']}{sponsor}"
        buttons  = None
        if ad.get("url") and ad.get("btn_label"):
            token   = generate_click_token(ad["id"], ad["url"])
            buttons = [[Button.inline(f"🔗 {ad['btn_label']}", f"adclick|{token}".encode())]]
        await bot.send_message(user_id, text, buttons=buttons)
        return True
    except Exception as e:
        logger.debug(f"Popup send failed uid={user_id}: {e}")
    return False


def tick_menu_open(user_id: int):
    """Call on every main_menu open."""
    _menu_opens[user_id] += 1

# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULED BLAST LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def blast_loop(bot):
    """
    Background task — intelligently blasts sponsor messages
    in rate-limited batches. Flood-safe.
    """
    await asyncio.sleep(60)   # Bot warmup
    while True:
        await asyncio.sleep(120)
        try:
            cfg      = _cfg()
            interval = cfg.get("blast_interval", 0)
            if not cfg.get("enabled") or not interval:
                continue

            ad = _pick("blast")
            if not ad:
                continue

            from database import db as user_db
            batch_size  = cfg.get("blast_batch", 30)
            batch_delay = cfg.get("blast_batch_delay", 0.08)
            now         = time.time()
            sent = failed = skipped = 0

            _clean_popup_cache()  # FIX: periodic memory cleanup
            user_ids = list(user_db.keys())
            random.shuffle(user_ids)   # Fair rotation

            for uid in user_ids:
                if _is_ad_free(uid):
                    skipped += 1
                    continue
                if (now - _blast_sent.get(uid, 0)) < interval:
                    skipped += 1
                    continue
                # FIX 1: Skip dead/inactive users
                if user_db.get(uid, {}).get("_blast_inactive"):
                    skipped += 1
                    continue

                try:
                    from telethon import Button
                    sponsor  = f"\n\n📣 _Sponsor: {ad['sponsor']}_" if ad.get("sponsor") else ""
                    text     = f"📢 **Sponsored Message**\n\n{ad['text']}{sponsor}"
                    buttons  = None
                    if ad.get("url") and ad.get("btn_label"):
                        token   = generate_click_token(ad["id"], ad["url"])
                        buttons = [[Button.inline(
                            f"🔗 {ad['btn_label']}", f"adclick|{token}".encode()
                        )]]
                    await bot.send_message(uid, text, buttons=buttons, parse_mode="md")
                    _blast_sent[uid] = now
                    _record_impression(ad["id"], uid)
                    sent += 1
                    await asyncio.sleep(batch_delay)
                    if sent % batch_size == 0:
                        await asyncio.sleep(2)
                except Exception as _e:
                    err_str = str(_e).lower()
                    # FIX 1: Dead users — mark inactive, skip future blasts
                    if any(x in err_str for x in ["blocked", "deactivated", "not found", "user_id_invalid", "input_user_deactivated"]):
                        try:
                            from database import db as _db
                            if uid in _db:
                                _db[uid].setdefault("_blast_inactive", True)
                        except Exception:
                            pass
                    # FIX 1: FloodWait — respect Telegram limits
                    elif "flood" in err_str and hasattr(_e, "seconds"):
                        wait = min(getattr(_e, "seconds", 30), 120)
                        logger.warning(f"Ads blast FloodWait {wait}s")
                        await asyncio.sleep(wait)
                    failed += 1

            if sent:
                logger.info(f"Blast '{ad['title']}': sent={sent} skipped={skipped} failed={failed}")

            # FIX 19: Cleanup stale blast entries (>7 days old)
            _stale_cutoff = time.time() - 7 * 86400
            _stale = [k for k, v in list(_blast_sent.items()) if v < _stale_cutoff]
            for k in _stale:
                del _blast_sent[k]
            # Also clear _blast_inactive flag periodically (user might reactivate)
            if sent == 0 and failed > 10:
                # All failed — clear inactive flags to retry next time
                try:
                    from database import db as _db
                    for _uid in list(_db.keys()):
                        _db[_uid].pop("_blast_inactive", None)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"blast_loop error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# A/B TESTING
# ─────────────────────────────────────────────────────────────────────────────

def create_ab_test(ad_id_a: str, ad_id_b: str, name: str = "") -> str:
    """Compare 2 ad variants. Returns test_id."""
    test_id = f"ab_{uuid.uuid4().hex[:6]}"
    _cfg()["ab_tests"][test_id] = {
        "name":    name or f"Test {test_id}",
        "ad_a":    ad_id_a,
        "ad_b":    ad_id_b,
        "started": time.time(),
        "active":  True,
    }
    _save()
    return test_id


def get_ab_results(test_id: str) -> dict:
    test = _cfg()["ab_tests"].get(test_id)
    if not test:
        return {}
    ads = _cfg()["ads"]
    a   = ads.get(test["ad_a"], {})
    b   = ads.get(test["ad_b"], {})

    def _ctr(ad):
        imp = ad.get("impressions", 0)
        clk = ad.get("clicks", 0)
        return round(clk / imp * 100, 2) if imp else 0.0

    return {
        "name": test["name"],
        "variant_a": {
            "id":          a.get("id", ""),
            "title":       a.get("title", ""),
            "impressions": a.get("impressions", 0),
            "clicks":      a.get("clicks", 0),
            "ctr":         _ctr(a),
            "earned":      round(a.get("earned", 0.0), 2),
        },
        "variant_b": {
            "id":          b.get("id", ""),
            "title":       b.get("title", ""),
            "impressions": b.get("impressions", 0),
            "clicks":      b.get("clicks", 0),
            "ctr":         _ctr(b),
            "earned":      round(b.get("earned", 0.0), 2),
        },
        "winner": _ab_winner(a, b),
    }


def _ab_winner(a: dict, b: dict) -> str:
    def ctr(ad): return ad.get("clicks",0) / max(1, ad.get("impressions",0))
    ca, cb = ctr(a), ctr(b)
    if abs(ca - cb) < 0.001:
        return "⚖️ Tie — aur data chahiye"
    w = a if ca > cb else b
    pct = abs(ca - cb) / max(ca, cb) * 100
    return f"🏆 '{w.get('title','')}' — {pct:.1f}% better CTR"

# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────

def get_earnings() -> dict:
    cfg   = _cfg()
    ads   = cfg.get("ads", {})
    month = time.strftime("%Y-%m")
    lm    = time.strftime("%Y-%m", time.localtime(time.time() - 30 * 86400))

    total_imp = sum(a.get("impressions", 0) for a in ads.values())
    total_clk = sum(a.get("clicks",      0) for a in ads.values())
    ctr       = round(total_clk / total_imp * 100, 2) if total_imp else 0.0
    ecpm      = round(cfg.get("total_earned", 0.0) / max(1, total_imp) * 1000, 2)

    # Top performer
    top = max(ads.values(), key=lambda a: a.get("earned", 0), default=None)

    return {
        "enabled":        cfg.get("enabled", False),
        "total_ads":      len(ads),
        "active_ads":     sum(1 for a in ads.values() if a.get("active")),
        "total_impressions": total_imp,
        "total_clicks":   total_clk,
        "ctr":            ctr,
        "ecpm":           ecpm,
        "total_earned":   round(cfg.get("total_earned",   0.0), 2),
        "pending_payout": round(cfg.get("pending_payout", 0.0), 2),
        "paid_out":       round(cfg.get("paid_out",       0.0), 2),
        "this_month":     round(cfg.get("monthly", {}).get(month, 0.0), 2),
        "last_month":     round(cfg.get("monthly", {}).get(lm,    0.0), 2),
        "top_ad":         top.get("title", "—") if top else "—",
        "top_earned":     round(top.get("earned", 0.0), 2) if top else 0.0,
        "rotation":       cfg.get("rotation", "weighted"),
    }


def get_ad_analytics(ad_id: str) -> dict:
    """Detailed analytics for one ad."""
    ad  = _cfg()["ads"].get(ad_id, {})
    imp = ad.get("impressions", 0)
    clk = ad.get("clicks",      0)
    ctr = round(clk / imp * 100, 2) if imp else 0.0
    rpm = round(ad.get("earned", 0.0) / max(1, imp) * 1000, 2)  # ₹ per 1000 imp

    # Last 7 days daily impressions
    daily = []
    for i in range(6, -1, -1):
        d   = time.strftime("%Y-%m-%d", time.localtime(time.time() - i * 86400))
        cnt = ad.get("daily_impressions", {}).get(d, 0)
        daily.append((d[-5:], cnt))   # "MM-DD": count

    unique_clk  = ad.get("unique_clicks", 0)
    unique_ctr  = round(unique_clk / imp * 100, 2) if imp else 0.0
    unique_users = len(ad.get("user_impressions", {}))

    # Reach estimate
    today = time.strftime("%Y-%m-%d")
    today_imp = ad.get("daily_impressions", {}).get(today, 0)

    return {
        "id": ad_id, "title": ad.get("title",""),
        "type": ad.get("type",""), "active": ad.get("active", False),
        "paused_reason": ad.get("paused_reason",""),
        "impressions": imp, "clicks": clk, "ctr": ctr,
        "unique_clicks": unique_clk, "unique_ctr": unique_ctr,
        "unique_users": unique_users,
        "today_imp": today_imp,
        "earned": round(ad.get("earned",0.0), 2), "rpm": rpm,
        "cpm": ad.get("cpm", 10.0), "weight": ad.get("weight", 100),
        "daily": daily,
        "last_shown": ad.get("last_shown", 0),
        "created": ad.get("created", 0),
        "daily_cap": ad.get("daily_cap", 0),
        "lifetime_cap": ad.get("lifetime_cap", 0),
        "user_daily_cap": ad.get("user_daily_cap", 0),
        "user_total_cap": ad.get("user_total_cap", 0),
        "schedule": f"{ad.get('schedule_start','')}–{ad.get('schedule_end','')}".strip("–"),
        # Targeting
        "target_premium": ad.get("target_premium", True),
        "target_free":    ad.get("target_free",    True),
        "target_active":  ad.get("target_active",  False),
        "min_sources":    ad.get("min_sources",    0),
    }


def mark_payout(amount: float, note: str = ""):
    cfg = _cfg()
    amount = min(amount, cfg.get("pending_payout", 0.0))
    cfg["pending_payout"] = max(0.0, cfg.get("pending_payout", 0.0) - amount)
    cfg["paid_out"]       = cfg.get("paid_out", 0.0) + amount
    cfg.setdefault("payout_log", []).append({
        "amount": round(amount, 2),
        "note":   note,
        "t":      time.time(),
    })
    _save()

# ─────────────────────────────────────────────────────────────────────────────
# AD-FREE SUBSCRIPTION
# ─────────────────────────────────────────────────────────────────────────────

def grant_ad_free(user_id: int, days: int = 30):
    from database import get_user_data, save_persistent_db
    d = get_user_data(user_id)
    expires = time.time() + days * 86400
    d["ad_free"]         = True
    d["ad_free_expires"] = expires
    save_persistent_db()


def check_ad_free_expiry():
    """Periodic cleanup — expired ad-free remove karo."""
    from database import db as user_db
    now = time.time()
    for uid, d in user_db.items():
        if d.get("ad_free") and d.get("ad_free_expires", 0) < now:
            d["ad_free"] = False
            d.pop("ad_free_expires", None)

# ─────────────────────────────────────────────────────────────────────────────
# BULK ACTIONS
# ─────────────────────────────────────────────────────────────────────────────

def pause_all_ads(ad_type: str = None) -> int:
    """Pause all (or specific type) ads. Returns count."""
    count = 0
    for ad in _cfg()["ads"].values():
        if ad.get("active") and (not ad_type or ad["type"] == ad_type):
            ad["active"] = False
            ad["paused_reason"] = "bulk_pause"
            count += 1
    if count:
        _save()
    return count


def activate_all_ads(ad_type: str = None) -> int:
    """Activate all paused ads. Returns count."""
    count = 0
    for ad in _cfg()["ads"].values():
        if not ad.get("active") and (not ad_type or ad["type"] == ad_type):
            ad["active"] = True
            ad["paused_reason"] = ""
            count += 1
    if count:
        _save()
    return count


def duplicate_ad(ad_id: str, new_title: str = "") -> str | None:
    """Clone an ad. Returns new ad_id."""
    src = _cfg()["ads"].get(ad_id)
    if not src:
        return None
    import copy
    new_ad = copy.deepcopy(src)
    new_ad["id"]          = f"ad_{uuid.uuid4().hex[:8]}"
    new_ad["title"]       = new_title or f"{src['title']} (Copy)"
    new_ad["active"]      = False   # Start paused
    new_ad["impressions"] = 0
    new_ad["clicks"]      = 0
    new_ad["unique_clicks"] = 0
    new_ad["earned"]      = 0.0
    new_ad["daily_impressions"] = {}
    new_ad["user_impressions"]  = {}
    new_ad["user_clicks"]       = {}
    new_ad["paused_reason"]     = "new_duplicate"
    new_ad["created"]     = time.time()
    new_ad["last_shown"]  = 0.0
    _cfg()["ads"][new_ad["id"]] = new_ad
    _save()
    return new_ad["id"]


def get_ad_health(ad_id: str) -> list[str]:
    """Return list of warning strings for an ad."""
    ad     = _cfg()["ads"].get(ad_id, {})
    issues = []
    imp    = ad.get("impressions", 0)
    clk    = ad.get("clicks",      0)
    ctr    = clk / imp if imp else 0

    if not ad.get("url"):
        issues.append("⚠️ No URL — clicks se koi benefit nahi")
    if not ad.get("sponsor"):
        issues.append("💡 Sponsor name missing")
    if imp > 100 and ctr < 0.005:
        issues.append(f"📉 Low CTR ({ctr*100:.2f}%) — ad text improve karo")
    if ad.get("paused_reason") == "lifetime_cap_reached":
        issues.append("⏹ Auto-paused: lifetime cap hit")
    if ad.get("paused_reason") == "bulk_pause":
        issues.append("⏹ Manually paused (bulk)")
    if ad.get("daily_cap", 0) > 0:
        today = time.strftime("%Y-%m-%d")
        used  = ad.get("daily_impressions", {}).get(today, 0)
        pct   = round(used / ad["daily_cap"] * 100)
        if pct >= 90:
            issues.append(f"📊 Daily cap almost full ({pct}% used today)")
    if not ad.get("active"):
        reason = ad.get("paused_reason", "")
        if reason:
            issues.append(f"🔴 Inactive — reason: {reason}")

    return issues


def get_ads_summary() -> dict:
    """Quick stats for dashboard."""
    ads    = _cfg()["ads"]
    active = [a for a in ads.values() if a.get("active")]
    paused = [a for a in ads.values() if not a.get("active")]

    by_type = {}
    for t in ("banner", "button", "popup", "blast"):
        by_type[t] = sum(1 for a in active if a.get("type") == t)

    today     = time.strftime("%Y-%m-%d")
    today_imp = sum(a.get("daily_impressions", {}).get(today, 0) for a in ads.values())
    today_clk = sum(
        len([l for l in _cfg().get("click_log", [])
             if l.get("ad") in ads and
             time.strftime("%Y-%m-%d", time.localtime(l.get("t",0))) == today])
    for _ in [1])  # one-shot

    return {
        "total":    len(ads),
        "active":   len(active),
        "paused":   len(paused),
        "by_type":  by_type,
        "today_imp": today_imp,
        "today_clk": today_clk,
    }
