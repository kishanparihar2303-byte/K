import time
import datetime
from time_helper import ab_now, ab_fmt
from translator import maybe_translate

# ✅ FIX: Ensure Telethon patch is active (applied by main.py, but guard here too)
try:
    import telethon_patch as _tp
    _tp.apply()
except Exception:
    pass
# ── Destination failure tracker (consecutive failures → user notification) ──
_dest_fail_count: dict = {}   # {(user_id, dest): count}
_dest_notified:   set  = set()  # Already notified pairs
_dest_fail_last_clean: float = 0.0  # Last cleanup timestamp

def _maybe_clean_dest_fails():
    """FIX 13: Periodic cleanup of dest_fail dicts (memory leak prevention)."""
    global _dest_fail_last_clean
    import time as _t
    now = _t.time()
    if now - _dest_fail_last_clean < 3600:  # Once per hour
        return
    _dest_fail_last_clean = now
    # Clear counts for users who no longer have active sessions
    from database import user_sessions
    active = set(user_sessions.keys())
    stale_keys = [k for k in list(_dest_fail_count.keys()) if k[0] not in active]
    for k in stale_keys:
        _dest_fail_count.pop(k, None)
    stale_notified = {k for k in _dest_notified if k[0] not in active}
    _dest_notified.difference_update(stale_notified)
import asyncio
import re
import html
import os
import aiohttp
import traceback
import warnings

# Suppress Telethon Python 3.12 GeneratorExit RuntimeWarning
# Known Telethon issue with asyncio teardown — not our bug
warnings.filterwarnings("ignore", category=RuntimeWarning,
                        message="coroutine.*was never awaited")
warnings.filterwarnings("ignore", category=RuntimeWarning,
                        message=".*GeneratorExit.*")
from telethon import TelegramClient, events, errors, Button
from telethon.sessions import StringSession
from telethon.tl.types import (
    MessageMediaWebPage, MessageEntityTextUrl, MessageEntityUrl,
    MessageMediaPoll, DocumentAttributeFilename,
    MessageEntityTextUrl    as _METU,
    MessageEntityUrl        as _MEU,
    MessageEntityBold       as _MEB,
    MessageEntityItalic     as _MEI,
    MessageEntityCode       as _MEC,
    MessageEntityPre        as _MEP,
    MessageEntityUnderline  as _MEUN,
    MessageEntityStrike     as _MES,
    MessageEntitySpoiler    as _MESP,
    MessageEntityBlockquote as _MEBQ,
)
from telethon.errors import FloodWaitError, MediaCaptionTooLongError
from telethon.errors import (
    AuthKeyDuplicatedError, AuthKeyUnregisteredError,
    AuthKeyInvalidError, SessionExpiredError, SessionRevokedError,
    UserDeactivatedError, UserDeactivatedBanError
)

# ══════════════════════════════════════════════════════════════════════
# ✅ FIX: PeerUser "unknown entity" — Entity Resolution Helper
#
# Problem: Jab destination ek user hai (PeerUser), Telethon ka client
#   us user ko sirf tab jaanta hai jab:
#     (a) client ne us user ko pehle message receive kiya ho, ya
#     (b) us user ka entity explicitly fetch kiya gaya ho.
#   Agar client kabhi us user se mila nahi → ValueError:
#   "Could not find the input entity for PeerUser(user_id=...)"
#
# Fix: Pehle get_input_entity() try karo, fail hone par get_entity()
#   try karo (yeh Telegram server se directly fetch karta hai).
#   Agar entity mil gayi to resolved entity se send karo — success.
#   Agar nahi mili to original target rakhte hain (fallback).
#
# Yeh fix _send_one_dest, _send_captioned_media_dest, _send_text_dest
# aur process_single_message ke sequential loop — sabko cover karta hai.
# ══════════════════════════════════════════════════════════════════════

# In-memory cache: successfully resolved entities {user_id: {target_str: resolved}}
_ENTITY_CACHE: dict = {}

async def _resolve_entity(client, user_id: int, target):
    """
    ✅ FIX: PeerUser "unknown entity" — Bot-assisted Entity Resolution
    
    Telethon ka limitation: user client sirf un users ko message bhej sakta hai
    jinhe usne pehle dekha ho (common group, message exchange).
    Agar PeerUser kabhi nahi mila → ValueError.

    Solution — 3-step resolution:
      Step 1: client.get_input_entity()  — local cache se (fastest)
      Step 2: client.get_entity()        — Telegram server se direct fetch
      Step 3: BOT client se access_hash  — Bot ne us user ko zaroor dekha hoga
                                           (kyunki user ne bot se interact kiya)
              → InputPeerUser(user_id, access_hash) banao
              → User client seedha yeh use kar sakta hai!

    Bot client = master resolver — Telegram bots saare interacting users
    ka access_hash store karte hain automatically.
    """
    target_str = str(target)
    user_cache = _ENTITY_CACHE.setdefault(user_id, {})

    # Cache hit
    if target_str in user_cache:
        return user_cache[target_str]

    # ── Step 1: client local cache ──────────────────────────────────
    try:
        entity = await client.get_input_entity(target)
        user_cache[target_str] = entity
        logger.debug(f"[ENTITY] Step1 hit (local cache): {target}")
        return entity
    except Exception:
        pass

    # ── Step 2: client → Telegram server ────────────────────────────
    try:
        entity = await client.get_entity(target)
        user_cache[target_str] = entity
        logger.debug(f"[ENTITY] Step2 hit (server fetch): {target}")
        return entity
    except Exception:
        pass

    # ── Step 3: BOT client se access_hash fetch karo ─────────────────
    # Yeh main trick hai:
    # Bot ke paas har us user ka access_hash hota hai jisne kabhi bot use kiya.
    # access_hash mile → InputPeerUser banao → user client directly use kar sakta hai.
    try:
        from telethon.tl.types import InputPeerUser, InputPeerChannel, InputPeerChat
        from telethon.tl.functions.users import GetUsersRequest
        from telethon.tl.types import InputUser

        bot_entity = await bot.get_input_entity(target)

        # Bot ne entity resolve kar li — ab user_id + access_hash extract karo
        if hasattr(bot_entity, 'access_hash'):
            if hasattr(bot_entity, 'user_id'):
                # InputPeerUser
                resolved = InputPeerUser(
                    user_id=bot_entity.user_id,
                    access_hash=bot_entity.access_hash
                )
            elif hasattr(bot_entity, 'channel_id'):
                # InputPeerChannel
                resolved = InputPeerChannel(
                    channel_id=bot_entity.channel_id,
                    access_hash=bot_entity.access_hash
                )
            else:
                resolved = bot_entity
            user_cache[target_str] = resolved
            logger.info(
                f"[ENTITY] ✅ Step3 BOT-assisted resolve SUCCESS: "
                f"dest={target} user={user_id} → {type(resolved).__name__}"
            )
            return resolved
    except Exception as bot_e:
        logger.debug(f"[ENTITY] Step3 bot-assist failed for {target}: {bot_e}")

    logger.warning(f"[ENTITY] All 3 steps failed for dest={target} user={user_id} — unreachable")
    return target  # Fallback: original target

from config import API_ID, API_HASH, logger, get_default_forward_rules, bot
from database import (
    get_user_data, user_sessions, GLOBAL_STATE, 
    save_reply_mapping, get_reply_id, save_persistent_db,
    update_user_stats, get_rules_for_pair, update_last_active
)

# FREE/PAID MODE FIX: Forward engine mein premium check
def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def _can_use_feature(user_id, feature_key):
    """Lazy import — circular import se bachne ke liye."""
    try:
        from premium import can_use_feature
        return can_use_feature(user_id, feature_key)
    except Exception:
        return True  # Error par allow karo

# Analytics + Limit — lazy import to avoid circular
def _record_analytics(user_id, stat_type="forwarded"):
    try:
        from analytics import record_message
        record_message(user_id, stat_type)
    except Exception:
        pass

def _check_msg_limit(user_id):
    try:
        from msg_limit import can_forward, increment_msg_count
        return can_forward(user_id), increment_msg_count
    except Exception:
        return (True, ""), lambda uid: None
from utils import (
    normalize_url, apply_smart_delay, extract_all_urls, ROBUST_LINK_PATTERN,
    format_user_time, format_ts, user_now
)
from filters import is_duplicate, check_product_duplicate, is_album_duplicate 
from scheduler import is_schedule_allowed
from shortener import shorten_url_rotation
import sqlite3

# ══════════════════════════════════════════════════════════════════════
# ✅ FIX DUPLICATE: Persistent Dedup DB (SQLite)
# Problem: _MSG_PROCESSED_CACHE RAM mein hoti hai.
#   Jab bot restart hota hai ya Telegram reconnect karta hai,
#   Telegram "missed updates" replay karta hai — 1,2,3 mein se 1,2
#   dobara forward ho jaate hain kyunki RAM cache clear ho chuki hoti hai.
# Fix: SQLite file mein bhi dedup keys save karo — restart ke baad bhi kaam karta hai.
# ══════════════════════════════════════════════════════════════════════

_DEDUP_DB_PATH = "msg_dedup.sqlite"
_dedup_sqlite_conn: sqlite3.Connection = None

_dedup_sqlite_lock = __import__("threading").Lock()  # Explicit write lock for async safety

def _get_dedup_db() -> sqlite3.Connection:
    """SQLite connection with WAL mode for concurrent read safety."""
    global _dedup_sqlite_conn
    if _dedup_sqlite_conn is None:
        try:
            _dedup_sqlite_conn = sqlite3.connect(
                _DEDUP_DB_PATH,
                check_same_thread=False,
                timeout=10.0,
            )
            # WAL mode: allows concurrent reads while writing — prevents corruption
            _dedup_sqlite_conn.execute("PRAGMA journal_mode=WAL")
            _dedup_sqlite_conn.execute("PRAGMA synchronous=NORMAL")  # Faster, still safe with WAL
            _dedup_sqlite_conn.execute("PRAGMA cache_size=-8000")    # 8MB page cache
            _dedup_sqlite_conn.execute(
                "CREATE TABLE IF NOT EXISTS dedup "
                "(key TEXT PRIMARY KEY, ts REAL NOT NULL)"
            )
            _dedup_sqlite_conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ts ON dedup(ts)"
            )
            _dedup_sqlite_conn.commit()
            logger.info("[DEDUP-DB] SQLite initialized with WAL mode")
        except Exception as _e:
            logger.warning(f"[DEDUP-DB] SQLite init failed: {_e} — RAM-only mode")
            _dedup_sqlite_conn = None
    return _dedup_sqlite_conn

def _persistent_dedup_check(key: str, now: float, ttl: float) -> bool:
    """
    True = already seen (duplicate hai, forward mat karo).
    False = naya message hai (forward karo, aur DB mein mark karo).
    Thread-safe: WAL mode reads + explicit write lock for check-then-insert atomicity.
    """
    conn = _get_dedup_db()
    if conn is None:
        return False  # DB unavailable → RAM-only fallback
    try:
        cutoff = now - ttl
        with _dedup_sqlite_lock:  # Atomic check-then-insert
            row = conn.execute(
                "SELECT ts FROM dedup WHERE key=?", (key,)
            ).fetchone()
            if row and row[0] > cutoff:
                return True  # Duplicate!
            conn.execute(
                "INSERT OR REPLACE INTO dedup(key, ts) VALUES (?,?)", (key, now)
            )
            # Periodic cleanup — purane entries delete karo (every ~500 msgs)
            if hash(key) % 500 == 0:
                conn.execute("DELETE FROM dedup WHERE ts < ?", (cutoff,))
            conn.commit()
        return False  # Naya message
    except Exception as _e:
        logger.debug(f"[DEDUP-DB] check/write error: {_e}")
        return False  # Error par allow karo
# ── v3: Circuit Breaker + Rate Limiter ──────────────────────────────────────
try:
    from circuit_breaker import CircuitBreakerRegistry, CBConfig
    from rate_limiter import RateLimiterRegistry
    _CB_AVAILABLE = True
except ImportError:
    _CB_AVAILABLE = False
    class _FakeCB:
        def is_closed(self): return True
        def record_success(self): pass
        def record_failure(self, e=""): pass
    class _FakeRL:
        async def wait_for_slot(self, dest, delay=0):
            if delay > 0:
                await asyncio.sleep(delay)
            return delay
    class _FakeCBReg:
        def get(self, *a, **kw): return _FakeCB()
        def on_flood_wait(self, *a): pass
        def on_success(self, *a): pass
    class _FakeRLReg:
        def get(self, uid, **kw): return _FakeRL()
        def on_flood_wait(self, *a): pass
        def on_success(self, *a): pass
    CircuitBreakerRegistry = _FakeCBReg()
    RateLimiterRegistry    = _FakeRLReg()

# ── v3: ForwardStats in-memory tracker ────────────────────────────────────
_FWD_STATS = {}  # {user_id: {sent_today, blocked_today, errors_today}}

def _stats(uid):
    if uid not in _FWD_STATS:
        import time as _ti
        _FWD_STATS[uid] = {"sent_today": 0, "blocked_today": 0, "errors_today": 0, "ts": _ti.time()}
    return _FWD_STATS[uid]

def _record_sent(uid, dest_key=""):
    _stats(uid)["sent_today"] += 1

def _record_blocked(uid):
    _stats(uid)["blocked_today"] += 1

def _record_error(uid):
    _stats(uid)["errors_today"] += 1

def get_forward_stats(uid):
    s = _stats(uid)
    return f"📤`{s['sent_today']}` ❌`{s['blocked_today']}` ⚠️`{s['errors_today']}`"
# ─────────────────────────────────────────────────────────────────────────────


# ══════════════════════════════════════════════════════════════════
# ⚡ SPEED OPTIMIZATIONS
# ══════════════════════════════════════════════════════════════════

# Per-user send semaphore — prevent thundering herd when many destinations
# Max 3 concurrent sends per user (Telegram rate: ~20 msg/s per bot)
_USER_SEND_SEM: dict = {}   # {user_id: asyncio.Semaphore}

def _get_send_sem(user_id: int) -> asyncio.Semaphore:
    if user_id not in _USER_SEND_SEM:
        _USER_SEND_SEM[user_id] = asyncio.Semaphore(10)  # Speed up: 3→10 concurrent sends
    return _USER_SEND_SEM[user_id]


def _update_dest_health(data: dict, dest_key: str, success: bool):
    """Destination health tracker — auto-disable after 5 consecutive fails.
    Auto re-enable after 30 minutes — khud try karega.
    """
    if not data["settings"].get("dest_health_check", True):
        return
    rules_map = data.get("custom_forward_rules", {})
    for src_id, src_rules in rules_map.items():
        if str(dest_key) in src_rules:
            r = src_rules[str(dest_key)]
            if success:
                r["fail_count"] = 0
                # Agar manually ya auto disabled tha — re-enable karo on success
                if not r.get("dest_enabled", True):
                    r["dest_enabled"]    = True
                    r["disabled_reason"] = ""
                    r["disabled_at"]     = None
                    logger.info(f"Dest {dest_key} auto re-enabled after success")
            else:
                # ✅ FIX: Pehle check karo — 30 min baad auto re-enable
                disabled_at = r.get("disabled_at")
                if not r.get("dest_enabled", True) and disabled_at:
                    if time.time() - disabled_at >= 1800:  # 30 minutes
                        r["dest_enabled"]    = True
                        r["fail_count"]      = 0
                        r["disabled_reason"] = ""
                        r["disabled_at"]     = None
                        logger.info(f"Dest {dest_key} auto re-enabled after 30min cooldown")
                        return  # Is message ko try hone do

                r["fail_count"] = r.get("fail_count", 0) + 1
                if r["fail_count"] >= 5:
                    r["dest_enabled"]    = False
                    r["disabled_reason"] = "Auto-disabled: 5 consecutive failures (30 min mein auto retry hoga)"
                    r["disabled_at"]     = time.time()  # Timestamp save karo
                    from database import save_persistent_db as _spdb
                    _spdb()
                    logger.warning(f"Dest {dest_key} auto-disabled — will auto retry in 30min")


def _update_src_stats(data: dict, source_id, count: int = 1):
    """Per-source forwarding stats."""
    stats = data.setdefault("src_stats", {})
    key   = str(source_id)
    stats.setdefault(key, {"total": 0, "today": {}})
    stats[key]["total"] = stats[key].get("total", 0) + count
    today = datetime.date.today().isoformat()
    stats[key]["today"][today] = stats[key]["today"].get(today, 0) + count
    # Keep 7 days only
    if len(stats[key]["today"]) > 7:
        oldest = sorted(stats[key]["today"].keys())[0]
        del stats[key]["today"][oldest]


async def _send_one_dest(client, user_id, target, final_text, media_to_send,
                         show_preview, force_doc, reply_to_id,
                         source_id, event_id, dest, data, source_id_orig,
                         dest_rules=None):
    """
    ⚡ Single destination sender — asyncio.gather() parallelism.
    Semaphore limits to 3 concurrent sends per user (flood protection).
    New: copy_mode, pin_forwarded, dest health check, per-source stats.
    """
    # ── Destination enabled check ─────────────────────────────────────
    if dest_rules and not dest_rules.get("dest_enabled", True):
        # ✅ 30 min ke baad auto retry
        disabled_at = dest_rules.get("disabled_at")
        if disabled_at and time.time() - disabled_at >= 1800:
            dest_rules["dest_enabled"]    = True
            dest_rules["fail_count"]      = 0
            dest_rules["disabled_reason"] = ""
            dest_rules["disabled_at"]     = None
            logger.info(f"Dest {target} auto re-enabled (30min cooldown passed) — retrying")
        else:
            logger.debug(f"Dest {target} skipped — disabled (reason: {dest_rules.get('disabled_reason','')})")
            return False

    # ── Copy mode: send_message instead of forward ──────────────────────
    use_copy = (data["settings"].get("copy_mode", False) or
                (dest_rules and dest_rules.get("copy_mode", False)))

    # ── v3: Circuit Breaker check ─────────────────────────────────────────────
    _cb = CircuitBreakerRegistry.get(user_id, str(dest))
    if not _cb.is_closed():
        logger.debug(f"[CB] Dest {dest} OPEN — skip user={user_id}")
        return False

    # ── v3: Rate Limiter — wait for our send slot ─────────────────────────
    _rl   = RateLimiterRegistry.get(user_id)
    _cdelay = data["settings"].get("custom_delay", 0)
    await _rl.wait_for_slot(str(dest), _cdelay)

    sem = _get_send_sem(user_id)
    async with sem:
        try:
            sent_msg = await client.send_message(
                target,
                final_text if final_text else None,
                file=media_to_send,
                parse_mode='html',
                link_preview=show_preview,
                reply_to=reply_to_id,
                force_document=force_doc
            )
            update_user_stats(user_id, "processed")
            if sent_msg:
                save_reply_mapping(user_id, source_id_orig, event_id, target, sent_msg.id)
                if dest_rules and dest_rules.get("pin_forwarded", False):
                    try:
                        await client.pin_message(target, sent_msg.id, notify=False)
                    except Exception:
                        pass
            _update_dest_health(data, str(dest), True)
            _update_src_stats(data, source_id_orig)
            _cb.record_success()
            RateLimiterRegistry.on_success(user_id, str(dest))
            _record_sent(user_id, str(dest))
            return True
        except FloodWaitError as e:
            _fs = e.seconds
            RateLimiterRegistry.on_flood_wait(user_id, str(dest), _fs)
            _fw_wait = min(_fs, 300)
            logger.warning(
                f"FloodWait {_fs}s for user {user_id} dest {target} "
                f"— waiting {_fw_wait}s then retry (no drop)"
            )
            # ── Issue #13: User ko clear FloodWait notification bhejo ──────────
            try:
                from config import bot as _notif_bot
                from telethon import Button as _Btn
                import datetime as _dt
                _resume_at = _dt.datetime.now() + _dt.timedelta(seconds=_fw_wait)
                _resume_str = _resume_at.strftime("%H:%M:%S")
                # Sirf notify karo agar wait > 10 seconds hai (chhoti waits ignore)
                if _fw_wait > 10:
                    # Duplicate notifications throttle karo — har user ke liye max 1 per 2 min
                    _fw_notif_key = f"_fw_last_notif_{user_id}"
                    import time as _t
                    _last = data.get(_fw_notif_key, 0)
                    if _t.time() - _last > 120:
                        data[_fw_notif_key] = _t.time()
                        await _notif_bot.send_message(
                            user_id,
                            f"⏳ **Telegram Rate Limit (FloodWait)**\n"
                            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"Telegram ne temporarily forwarding slow kar di hai.\n"
                            f"⏱ **Wait:** `{_fw_wait}` seconds\n"
                            f"▶️ **Resume at:** `{_resume_str}`\n\n"
                            "✅ Yeh normal hai — bot automatically resume karega.\n"
                            "❌ Kuch karne ki zaroorat nahi, bot band mat karo.",
                            buttons=[[_Btn.inline("📊 Status Dekho", b"main_menu")]]
                        )
            except Exception as _ne:
                logger.debug(f"FloodWait notify failed for {user_id}: {_ne}")
            # ─────────────────────────────────────────────────────────────────
            await asyncio.sleep(_fw_wait)
            if not data["settings"].get("running"):
                return False
            try:
                sent_msg = await client.send_message(
                    target,
                    final_text if final_text else None,
                    file=media_to_send,
                    parse_mode='html',
                    link_preview=show_preview,
                    reply_to=reply_to_id,
                    force_document=force_doc
                )
                update_user_stats(user_id, "processed")
                if sent_msg:
                    save_reply_mapping(user_id, source_id_orig, event_id, target, sent_msg.id)
                    if dest_rules and dest_rules.get("pin_forwarded", False):
                        try:
                            await client.pin_message(target, sent_msg.id, notify=False)
                        except Exception:
                            pass
                _update_dest_health(data, str(dest), True)
                _update_src_stats(data, source_id_orig)
                _cb.record_success()
                _record_sent(user_id, str(dest))
                return True
            except Exception as retry_e:
                logger.warning(f"Retry send failed for user {user_id} dest {target}: {retry_e}")
                # ✅ FIX: Retry failure pe hi CB/dest_health count karo, FloodWait pe nahi
                _update_dest_health(data, str(dest), False)
                _cb.record_failure(str(retry_e))
                _record_error(user_id)
                return False
        except ValueError as e:
            # ✅ FIX: PeerUser unknown entity → resolve karo aur retry karo
            # Pehle: permanently fail karta tha, dest health kharab karta tha
            # Ab: get_entity() se Telegram server se entity fetch karo, phir retry
            logger.debug(f"[ENTITY-FIX] ValueError for dest {target} user {user_id} — resolving entity: {e}")
            try:
                resolved = await _resolve_entity(client, user_id, target)
                if resolved != target:
                    # Entity mil gayi — resolved entity se dobara try karo
                    sent_msg = await client.send_message(
                        resolved,
                        final_text if final_text else None,
                        file=media_to_send,
                        parse_mode='html',
                        link_preview=show_preview,
                        reply_to=reply_to_id,
                        force_document=force_doc
                    )
                    update_user_stats(user_id, "processed")
                    if sent_msg:
                        save_reply_mapping(user_id, source_id_orig, event_id, target, sent_msg.id)
                        if dest_rules and dest_rules.get("pin_forwarded", False):
                            try:
                                await client.pin_message(resolved, sent_msg.id, notify=False)
                            except Exception:
                                pass
                    _update_dest_health(data, str(dest), True)
                    _cb.record_success()
                    _record_sent(user_id, str(dest))
                    logger.info(f"[ENTITY-FIX] ✅ Send succeeded after entity resolve: dest={target} user={user_id}")
                    return True
                else:
                    # Entity resolve nahi hui — PeerUser unresolvable hai
                    # ✅ FIX: Circuit breaker mat trigger karo — yeh network/send error nahi hai,
                    # sirf entity cache miss hai. Dest healthy rakho taaki baad mein retry ho.
                    _vk = (user_id, str(target))
                    _dest_fail_count[_vk] = _dest_fail_count.get(_vk, 0) + 1
                    if _vk not in _dest_notified:
                        _dest_notified.add(_vk)
                        logger.warning(f"[ENTITY] PeerUser unresolvable dest={target} user={user_id} — notifying once")
                        try:
                            asyncio.create_task(bot.send_message(
                                user_id,
                                f"⚠️ **Destination Unreachable — Action Needed**\n\n"
                                f"🆔 Destination: `{target}`\n\n"
                                f"Tumhara Telegram account is user ko **nahi pehchanta**.\n\n"
                                f"**✅ Fix:**\n"
                                f"1️⃣ Apne Telegram se user `{target}` ko ek message bhejo\n"
                                f"2️⃣ Ya unka @username destination mein add karo\n\n"
                                f"_Destination remove nahi kiya — fix ke baad automatically kaam karega._"
                            ))
                        except Exception:
                            pass
                    return False
            except Exception as retry_e:
                logger.warning(f"[ENTITY-FIX] Retry after entity resolve failed dest={target} user={user_id}: {retry_e}")
                _update_dest_health(data, str(dest), False)
                _cb.record_failure(str(retry_e))
                _record_error(user_id)
                return False
        except Exception as e:
            logger.error(f"Send error user {user_id} dest {target}: {e}")
            _update_dest_health(data, str(dest), False)
            _cb.record_failure(str(e))
            _record_error(user_id)
            return False

# ==========================================
# GLOBAL ALBUM BUFFER & ACTIVE TASKS
# ==========================================
ALBUM_BUFFER = {}

# ── ASYNC-SAFE DEDUP LOCK: prevents race condition double-send ──────────────
# Problem: Telegram sometimes sends duplicate update events for same message.
# Two coroutines check _MSG_PROCESSED_CACHE simultaneously → both pass → 2× send.
# Fix: Per message-key asyncio.Lock — second arrival BLOCKS until first sets cache.
_DEDUP_LOCKS: dict = {}   # {dedup_key: asyncio.Lock}
_DEDUP_LOCK_CLEAN_TS: float = 0.0

def _get_dedup_lock(key: str) -> asyncio.Lock:
    """Get or create a lock for this dedup key. Auto-cleanup old locks.

    BUG FIX: Previously, cleanup ran inside the `if key not in _DEDUP_LOCKS`
    block and could delete the freshly-created lock (because the new key is
    not yet in _MSG_PROCESSED_CACHE), causing KeyError on the final return.
    Fix: store a local reference first, skip current key during cleanup.
    """
    global _DEDUP_LOCK_CLEAN_TS
    if key not in _DEDUP_LOCKS:
        _DEDUP_LOCKS[key] = asyncio.Lock()
    lock = _DEDUP_LOCKS[key]  # hold ref before cleanup — avoids KeyError
    # Periodic cleanup: prune stale locks (explicitly skip current key)
    now = time.time()
    if now - _DEDUP_LOCK_CLEAN_TS > 60 and len(_DEDUP_LOCKS) > 200:
        _DEDUP_LOCK_CLEAN_TS = now
        stale = [k for k in list(_DEDUP_LOCKS.keys())
                 if k != key and k not in _MSG_PROCESSED_CACHE]
        for k in stale[:100]:
            _DEDUP_LOCKS.pop(k, None)
    return lock
# ✅ FIX 16: Per-user session lock — prevents AuthKeyDuplicatedError
# Only ONE connection per user_session at a time
_SESSION_LOCKS: dict[int, asyncio.Lock] = {}

def _get_session_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _SESSION_LOCKS:
        _SESSION_LOCKS[user_id] = asyncio.Lock()
        # Cleanup: agar bohot zyada locks hain to purane wale clean karo
        if len(_SESSION_LOCKS) > 500:
            from database import user_sessions
            active = set(user_sessions.keys())
            stale = [k for k in list(_SESSION_LOCKS.keys()) if k not in active]
            for k in stale[:200]:
                _SESSION_LOCKS.pop(k, None)
    return _SESSION_LOCKS[user_id]



# ── DEDUP CACHE: prevent same msg processed twice ──────────────────────
_INVITE_ID_CACHE: dict = {}
_MSG_PROCESSED_CACHE: dict = {}

# ── EDIT FLOOD CACHE: same message edited within 5s → skip (anti-flood) ──
_EDIT_CACHE: dict = {}  # {(user_id, chat_id, msg_id): last_edit_ts}
_EDIT_CACHE_TTL = 5.0   # seconds — edits within this window are skipped
# ✅ FIX RE-FORWARD BUG:
# Pehle _MSG_CACHE_TTL = 180s (3 min) — RAM aur SQLite DONO sirf 3 min yaad rakhte the.
# Telegram reconnect/restart pe "missed updates" 3 min se zyada baad bhi replay karta hai.
# SQLite bhi usi 3 min TTL se check karta tha → expired → duplicate nahi maana → DOBARA FORWARD!
#
# Fix:
#   RAM TTL = 600s (10 min)  — recent duplicate events ke liye fast in-memory check
#   SQLite TTL = 86400s (24h) — restart/reconnect ke baad bhi yaad rahe
#   RAM MAX = 2000            — zyada entries = SQLite pe kam depend karna
_MSG_CACHE_MAX = 2000  # ✅ FIX: 500→2000 — busy channels pe entries jaldi evict nahi hongi
_MSG_CACHE_TTL = 600   # ✅ FIX: 180→600 — RAM mein 10 min tak yaad rakho
_SQLITE_DEDUP_TTL = 86400  # ✅ FIX: 24 ghante — restart ke baad bhi re-forward nahi hoga

# FIX 30: Edit FloodWait dedup — same message 5s mein edit hua → skip
_EDIT_DEDUP_CACHE: dict = {}
_EDIT_DEDUP_TTL = 5  # seconds
# 🚨 PROBLEM 47 FIX: Track active tasks for safe cancellation
active_tasks = set()

async def _cleanup_client(client, user_id):
    """Client ko gracefully disconnect karta hai."""
    # NOTE: active_tasks cancel NAHI karte — ye global set hai, saare users ke tasks hain isme.
    # Cancel karne se doosre users ki forwarding bhi ruk jaati thi (yahi bug tha).
    try:
        if client:
            await client.disconnect()
    except Exception:
        pass
    user_sessions.pop(user_id, None)


async def start_user_forwarder(user_id, session_str):
    """FIX 9: Guard against starting forwarder when user has stopped."""
    # Check if still wanted before starting
    _check_data = get_user_data(user_id)
    if not _check_data.get("settings", {}).get("running", False):
        logger.debug(f"start_user_forwarder: user {user_id} not running — skipping")
        return

    # Clean up old sessions first
    if user_id in user_sessions:
        try:
            old_client = user_sessions[user_id]
            if old_client: await old_client.disconnect()
        except Exception as e:
            logger.debug(f"Old session disconnect error for {user_id}: {e}")

    while True:
        client = None
        try:
            # PROD OPT: Render 512MB ke liye entity cache minimize
            from config import ENTITY_CACHE_LIMIT
            client = TelegramClient(
                StringSession(session_str), API_ID, API_HASH,
                connection_retries=3,
                retry_delay=1,
                auto_reconnect=True,
                entity_cache_limit=ENTITY_CACHE_LIMIT,  # Config se (default 50)
                flood_sleep_threshold=20,
                request_retries=2,  # Fewer retries = less memory in flight
            )
            await client.connect()
            
            if not await client.is_user_authorized():
                logger.warning(f"User {user_id} session expired/invalid.")
                get_user_data(user_id)["session"] = None
                get_user_data(user_id)["settings"]["running"] = False
                await _cleanup_client(client, user_id)
                return
                
            user_sessions[user_id] = client
            me = await client.get_me() 
            my_id = me.id

            # ─────────────────────────────────────────────────────────────
            # FIX: Entity Pre-Warming on Restart
            # Problem: Bot restart ke baad Telethon ka entity cache empty hota
            # hai. "Own channel" ke liye Telegram turant entity push karta hai
            # (user admin hai), lekin unknown/joined channels ke liye nahi —
            # unke messages thodi der tak forward nahi hote jab tak entity
            # lazy-resolve na ho.
            # Fix: Startup par hi sabhi configured sources ki entity fetch karo
            # taaki Telethon unka access_hash cache kare aur GetChannelDifference
            # sahi se call kar sake.
            # ─────────────────────────────────────────────────────────────
            try:
                _startup_data = get_user_data(user_id)
                _sources_to_warm = list(_startup_data.get("sources", []))
                if _sources_to_warm:
                    logger.info(
                        f"[ENTITY-WARM] user={user_id} pre-fetching "
                        f"{len(_sources_to_warm)} source(s) on startup..."
                    )
                    for _src in _sources_to_warm:
                        try:
                            await client.get_entity(_src)
                            await asyncio.sleep(0.3)   # Telegram flood se bachao
                        except Exception as _ew:
                            logger.debug(
                                f"[ENTITY-WARM] user={user_id} src={_src} "
                                f"prefetch skipped: {_ew}"
                            )
                    logger.info(
                        f"[ENTITY-WARM] user={user_id} entity warm-up done."
                    )
                else:
                    # ✅ FIX Bug 3: Koi source set nahi (all-channels mode) →
                    # Recent 50 dialogs iterate karo taaki Telethon entity cache warm ho.
                    # Bina iske, bot restart ke baad unknown channels ke pehle kuch
                    # messages miss ho jaate hain (entity cache empty hoti hai).
                    logger.info(f"[ENTITY-WARM] user={user_id} no sources set — warming via iter_dialogs(50)...")
                    try:
                        _warm_count = 0
                        async for _dlg in client.iter_dialogs(limit=50):
                            _warm_count += 1
                            await asyncio.sleep(0.05)
                        logger.info(f"[ENTITY-WARM] user={user_id} dialog warm-up done ({_warm_count} dialogs).")
                    except Exception as _dlg_ew:
                        logger.debug(f"[ENTITY-WARM] iter_dialogs fallback failed user={user_id}: {_dlg_ew}")
            except Exception as _ew_outer:
                logger.debug(f"[ENTITY-WARM] outer error user={user_id}: {_ew_outer}")

            # ----------------------------------------
            # 1. NEW MESSAGE HANDLER
            # ----------------------------------------
            @client.on(events.NewMessage())
            async def handler(event):
                # Direct asyncio task — no queue overhead, instant processing
                task = asyncio.create_task(handle_new_message(client, user_id, event, my_id))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

            # ----------------------------------------
            # 2. EDITED MESSAGE HANDLER
            # ----------------------------------------
            @client.on(events.MessageEdited())
            async def edit_handler(event):
                # Direct asyncio task — no queue overhead, instant processing
                task = asyncio.create_task(handle_edit_message(client, user_id, event, my_id))
                active_tasks.add(task)
                task.add_done_callback(active_tasks.discard)

            await client.run_until_disconnected()

            # Clean disconnect (no exception) — check if should reconnect
            await _cleanup_client(client, user_id)
            _check = get_user_data(user_id)
            if not _check.get("settings", {}).get("running", False):
                logger.info(f"Session {user_id}: clean disconnect, user stopped — exiting loop")
                return
            # running=True → reconnect after brief pause
            logger.info(f"Session {user_id}: reconnecting after clean disconnect...")
            await asyncio.sleep(3)

        # ✅ FIX: Fatal session errors — retry nahi karna, loop band karo
        except (AuthKeyDuplicatedError, AuthKeyUnregisteredError, AuthKeyInvalidError) as e:
            logger.error(f"FATAL SESSION ERROR for {user_id}: {e}")
            await _cleanup_client(client, user_id)
            try:
                data = get_user_data(user_id)
                data["session"] = None
                data["settings"]["running"] = False
                from database import save_persistent_db
                save_persistent_db()
            except Exception as e:
                logger.error(f"DB save error after auth key error (user {user_id}): {e}")
            try:
                from config import bot
                from telethon import Button
                await bot.send_message(
                    user_id,
                    "🔄 **Session Temporarily Disconnect Ho Gayi**\n\n"
                    "Yeh **bilkul normal** hai — Telegram kabhi kabhi session automatically "
                    "refresh karta hai, especially naya device login karne par.\n\n"
                    "⏱️ Fix time: **~1 minute**\n"
                    "✅ Bas neeche button dabao → OTP dalo → Done!\n\n"
                    "_Login ke baad forwarding automatically resume ho jaayegi._",
                    buttons=[[Button.inline("🔁 Login Karo — 1 Min Lagega", b"login_menu")]]
                )
            except Exception as e:
                logger.warning(f"Could not notify user {user_id} of session error: {e}")
            return  # 🔴 Loop band — retry nahi karenge

        except (SessionExpiredError, SessionRevokedError) as e:
            logger.error(f"SESSION REVOKED for {user_id}: {e}")
            await _cleanup_client(client, user_id)
            try:
                data = get_user_data(user_id)
                data["session"] = None
                data["settings"]["running"] = False
                from database import save_persistent_db
                save_persistent_db()
            except Exception as e:
                logger.error(f"DB save error after session revoked (user {user_id}): {e}")
            # User ko clear message bhejo
            try:
                from config import bot, OWNER_ID, ADMINS
                from database import GLOBAL_STATE
                import datetime
                time_str = ab_fmt(user_id, "%d/%m/%Y %H:%M")
                await bot.send_message(
                    user_id,
                    "🔄 **Session Expire Ho Gayi — Yeh Normal Hai!**\n\n"
                    "Telegram security ke liye har kuch dino/hafton mein session "
                    "automatically expire karta hai — tumhari koi galti nahi.\n\n"
                    f"🕒 Time: {time_str}\n"
                    "⏱️ Fix time: **~1 minute**\n\n"
                    "✅ Bas neeche button dabao → OTP dalo → Forwarding automatically resume!\n\n"
                    "_Tumhare saare sources aur settings safe hain._",
                    buttons=[[Button.inline("🔁 Login Karo — 1 Min Lagega", b"login_menu")]]
                )
                # Admin ko bhi notify karo
                admin_ids = list(GLOBAL_STATE.get("admins", {}).keys())
                if OWNER_ID not in admin_ids:
                    admin_ids.append(OWNER_ID)
                for aid in admin_ids:
                    try:
                        await bot.send_message(
                            aid,
                            f"⚠️ **User Session Revoked**\n\n"
                            f"🆔 User ID: `{user_id}`\n"
                            f"🔴 Reason: Session expired/revoked\n"
                            f"🕒 Time: {time_str}\n\n"
                            "User ko login karne ko kaho."
                        )
                    except Exception as e:
                        logger.debug(f"Could not notify admin of session revoke: {e}")
            except Exception as e:
                logger.warning(f"Could not send session revoke notification to user {user_id}: {e}")
            return  # 🔴 Loop band

        except (UserDeactivatedError, UserDeactivatedBanError) as e:
            logger.error(f"USER BANNED/DEACTIVATED for {user_id}: {e}")
            await _cleanup_client(client, user_id)
            try:
                data = get_user_data(user_id)
                data["settings"]["running"] = False
                from database import save_persistent_db
                save_persistent_db()
            except Exception as e:
                logger.error(f"DB save error after user ban (user {user_id}): {e}")
            return  # 🔴 Loop band

        except Exception as e:
            # Sirf temporary errors ke liye retry karo
            logger.error(f"Engine Crash for {user_id}: {e}")
            await _cleanup_client(client, user_id)
            await asyncio.sleep(5)   # Was 15s — faster restart

# ==========================================
# HANDLER LOGIC (Separated for Task Tracking)
# ==========================================

async def handle_new_message(client, user_id, event, my_id):
    try:
        # Always load _d so it's available regardless of sender
        from database import get_user_data as _gud
        _d = _gud(user_id)

        # Apna message bhi forward karo — sirf bot ka message skip karo
        if event.sender_id == my_id:
            if not _d.get("settings", {}).get("forward_own_messages", True):
                return

        # BUG 28 FIX: O(1) blocked check using set
        from database import is_user_blocked
        if is_user_blocked(user_id):
            logger.info(f"[FWD-DROP] user={user_id} reason=USER_BLOCKED")
            return

        if GLOBAL_STATE.get("maintenance_mode", False):
            logger.info(f"[FWD-DROP] user={user_id} reason=MAINTENANCE_MODE")
            return

        data = get_user_data(user_id)
        if not data["settings"]["running"]:
            logger.info(f"[FWD-DROP] user={user_id} reason=RUNNING_FALSE chat={event.chat_id}")
            return
        if not is_schedule_allowed(user_id):
            logger.info(f"[FWD-DROP] user={user_id} reason=SCHEDULE_BLOCKED chat={event.chat_id}")
            return

        # ── Anti-Spam check ──────────────────────────────────────
        try:
            from anti_spam import check_spam
            _msg_text_for_spam = event.raw_text or ""
            _spam_ok, _spam_act, _spam_reason = await check_spam(user_id, _msg_text_for_spam)
            if not _spam_ok:
                logger.info(f"[FWD-DROP] user={user_id} reason=ANTI_SPAM act={_spam_act}")
                return
        except Exception as _ase:
            pass

        # Message limit check
        (allowed, limit_msg), inc_fn = _check_msg_limit(user_id)
        if not allowed:
            # Forwarding band karo + user ko ek baar notify karo
            data["settings"]["running"] = False
            save_persistent_db()
            try:
                from config import bot as _bot
                from telethon import Button as _Btn
                await _bot.send_message(
                    user_id,
                    limit_msg,
                    buttons=[[_Btn.inline("💎 Premium Info", b"premium_info"),
                              _Btn.inline("💳 Buy Now", b"buy_premium")]]
                )
            except Exception:
                pass
            return

        # ── Issue #15: 80% limit warning (non-blocking) ──────────────────
        try:
            from msg_limit import check_limit_warning
            _warn = check_limit_warning(user_id)
            if _warn:
                from config import bot as _wbot
                from telethon import Button as _WBtn
                asyncio.create_task(_wbot.send_message(
                    user_id,
                    _warn,
                    buttons=[[_WBtn.inline("💎 Premium Info", b"premium_info"),
                              _WBtn.inline("💳 Buy Now", b"buy_premium")]]
                ))
        except Exception:
            pass

        # ── Issue #19: Basic duplicate filter (free users) — by message ID ──
        try:
            from premium import is_premium_user as _is_prem
            _premium = _is_prem(user_id)
        except Exception:
            _premium = False
        if not _premium:
            try:
                from msg_limit import is_basic_duplicate
                if is_basic_duplicate(user_id, event.chat_id, event.id):
                    logger.info(f"[FWD-DROP] user={user_id} reason=BASIC_DUPLICATE_FREE msg_id={event.id}")
                    return
            except Exception:
                pass

        chat_id = event.chat_id
        source_match = False
        current_chat_id = str(chat_id)

        # ── NEW: Source-level enable/disable ─────────────────────────────
        _src_rules_default = _d.get("custom_forward_rules", {}).get(current_chat_id, {}).get("default", {})
        if not _src_rules_default.get("src_enabled", True):
            logger.info(f"[FWD-DROP] user={user_id} reason=SOURCE_PAUSED chat={current_chat_id}")
            return

        # ── DEDUP ────────────────────────────────────────────────────────
        _dedup_key = f"{user_id}:{chat_id}:{event.id}"
        _now_ts = time.time()

        if _dedup_key in _MSG_PROCESSED_CACHE:
            if _now_ts - _MSG_PROCESSED_CACHE[_dedup_key] < _MSG_CACHE_TTL:
                logger.debug(f"[FWD-DROP] user={user_id} reason=DEDUP_RAM key={_dedup_key}")
                return

        _dedup_lock = _get_dedup_lock(_dedup_key)
        async with _dedup_lock:
            _now_ts = time.time()
            if _dedup_key in _MSG_PROCESSED_CACHE:
                if _now_ts - _MSG_PROCESSED_CACHE[_dedup_key] < _MSG_CACHE_TTL:
                    return
            if _persistent_dedup_check(_dedup_key, _now_ts, _SQLITE_DEDUP_TTL):
                _MSG_PROCESSED_CACHE[_dedup_key] = _now_ts
                logger.debug(f"[FWD-DROP] user={user_id} reason=DEDUP_SQLITE key={_dedup_key}")
                return
            _MSG_PROCESSED_CACHE[_dedup_key] = _now_ts

        if len(_MSG_PROCESSED_CACHE) > _MSG_CACHE_MAX:
            cutoff = _now_ts - _MSG_CACHE_TTL
            stale = [k for k, v in list(_MSG_PROCESSED_CACHE.items()) if v < cutoff]
            for k in stale:
                _MSG_PROCESSED_CACHE.pop(k, None)
                _DEDUP_LOCKS.pop(k, None)

        # Anti-loop: agar ye chat already destination hai
        dest_strs = [str(d) for d in data.get("destinations", [])]
        for d in dest_strs:
            if current_chat_id == d or current_chat_id == f"-100{d}" or f"-100{current_chat_id}" == d:
                logger.info(f"[FWD-DROP] user={user_id} reason=ANTI_LOOP chat={current_chat_id}")
                return

        # ── SMART SOURCE MATCHING ────────────────────────────────────────
        # Logic:
        # 1. Agar sources list EMPTY hai → saare channels/groups se forward karo
        # 2. Agar sources set hain → sirf unse match karo
        # 3. Agar sources set hain but match nahi hua → auto-add karke forward karo
        #    (user ne galat ID dali thi — bot khud fix karta hai)

        user_sources = data.get("sources", [])

        if not user_sources:
            # Koi source set nahi — saare channels/groups se forward karo
            source_match = True
            logger.info(f"[FWD-PASS] user={user_id} no-source-filter chat={current_chat_id}")
        else:
            # Sources set hain — match try karo
            for src in user_sources:
                src_str = str(src)

                # Numeric ID match (with/without -100 prefix)
                # ✅ FIX: lstrip("-100") individual chars strip karta tha (bug)
                # removeprefix("-100") exact prefix remove karta hai
                if (current_chat_id == src_str
                        or current_chat_id == f"-100{src_str}"
                        or f"-100{current_chat_id}" == src_str
                        or current_chat_id.lstrip("-") == src_str.lstrip("-")
                        or current_chat_id.removeprefix("-100") == src_str.removeprefix("-100")):
                    source_match = True
                    current_chat_id = src_str
                    break

                # @username match
                if event.chat and getattr(event.chat, 'username', None):
                    if getattr(event.chat, 'username', '').lower() == src.replace("@", "").lower():
                        source_match = True
                        current_chat_id = src
                        break

                # Invite link match
                if src_str.startswith('+') or ('t.me/+' in src_str) or ('t.me/joinchat' in src_str):
                    if src_str not in _INVITE_ID_CACHE:
                        _INVITE_ID_CACHE[src_str] = data.get("channel_names_id", {}).get(src_str)
                    cached_id = _INVITE_ID_CACHE.get(src_str)
                    # FIX: channel_names_id mein "1234567890" store hota hai
                    # (bina -100 prefix ke), lekin current_chat_id hota hai
                    # "-1001234567890". Teen formats mein compare karo:
                    if cached_id:
                        _cid_s = str(cached_id)
                        _match_invite = (
                            _cid_s == current_chat_id                     # exact match
                            or f"-100{_cid_s}" == current_chat_id         # stored without -100
                            or _cid_s == current_chat_id.removeprefix("-100")  # stored with -100, current without
                            or _cid_s.lstrip("-") == current_chat_id.lstrip("-")  # both stripped
                        )
                        if _match_invite:
                            source_match = True
                            current_chat_id = src_str
                            break
                    if client and not cached_id:
                        try:
                            hash_only = src_str.lstrip('+').split('/')[-1].lstrip('+')
                            from telethon.tl.functions.messages import ImportChatInviteRequest
                            try:
                                result = await client(ImportChatInviteRequest(hash_only))
                                resolved_id = str(result.chats[0].id)
                            except Exception:
                                ent = await client.get_entity(src_str if src_str.startswith('+') else src_str.split('/')[-1])
                                from telethon import utils as tg_utils
                                resolved_id = str(abs(tg_utils.get_peer_id(ent)))
                            data.setdefault("channel_names_id", {})[src_str] = resolved_id
                            _INVITE_ID_CACHE[src_str] = resolved_id
                            try:
                                idx = data["sources"].index(src)
                                data["sources"][idx] = int(f"-100{resolved_id}") if not resolved_id.startswith('-') else int(resolved_id)
                                from database import save_persistent_db
                                save_persistent_db(force_mongo=True)
                                logger.info(f"Auto-fixed source: {src_str} → -100{resolved_id}")
                            except Exception:
                                pass
                            if resolved_id == current_chat_id.removeprefix('-100') or f"-100{resolved_id}" == current_chat_id or resolved_id == current_chat_id:
                                source_match = True
                                current_chat_id = src_str
                                break
                        except Exception as _resolve_err:
                            logger.debug(f"Invite link resolve fail: {src_str} — {_resolve_err}")

            if not source_match:
                # Sources set hain but match nahi hua — forward mat karo
                # (User ne deliberately sources set kiye hain, respect karo)
                logger.debug(f"[FWD-DROP] user={user_id} reason=SOURCE_NO_MATCH chat={current_chat_id}")
                return

        logger.info(f"[FWD-PASS] user={user_id} source_matched={current_chat_id} dests={[str(d) for d in data.get('destinations',[])]}")

        # FIX 27: Echo chamber prevention — forwarded messages se infinite loop
        # Agar message already kisi aur bot/channel se forward ho ke aaya hai,
        # aur global_dup filter ON hai, toh ye already dup filter pakad lega.
        # Lekin extra safety: agar message fwd_from me hamare own destination se aaya hai → drop
        try:
            if event.fwd_from and event.fwd_from.channel_id:
                fwd_ch = str(event.fwd_from.channel_id)
                dest_strs_for_echo = [str(d) for d in data.get("destinations", [])]
                # Check if fwd source is one of our destinations
                for d_str in dest_strs_for_echo:
                    if fwd_ch == d_str or fwd_ch == d_str.removeprefix("-100") or \
                            f"-100{fwd_ch}" == d_str:
                        logger.debug(f"Echo loop prevented for user {user_id}: fwd from dest {fwd_ch}")
                        return
        except Exception:
            pass

        # 🆕 AUTO CLEANUP: User active hai, timestamp refresh karo
        update_last_active(user_id)
        # FIX 13: Periodic dest_fail cleanup
        _maybe_clean_dest_fails()

        # FIX G: Echo chamber prevention — if message was forwarded FROM our destination, skip
        # This prevents User A (X→Y) + User B (Y→X) infinite ping-pong loops
        # BUG FIX: from_id (user ID) ko echo check mein use mat karo — sirf channel_id check karo
        # Pehle: `from_id` fallback se kisi random user ka ID destination se match ho sakta tha → silent drop
        # Ab: sirf channel_id check hoga; aur drop hone par logging bhi hogi
        if event.fwd_from:
            fwd_channel_id = getattr(event.fwd_from, "channel_id", None)  # sirf channel_id, from_id nahi
            if fwd_channel_id:
                fwd_str = str(fwd_channel_id)
                for dest in data.get("destinations", []):
                    dest_str = str(dest).removeprefix("-100")
                    if fwd_str == dest_str or fwd_str == str(dest) or \
                            f"-100{fwd_str}" == str(dest):
                        logger.info(
                            f"[FWD-DROP] user={user_id} reason=ECHO_LOOP "
                            f"fwd_from_ch={fwd_str} matched_dest={dest}"
                        )
                        return  # Echo detected — stop forwarding

        # Album Handling
        if event.grouped_id:
            grp_id = event.grouped_id
            if grp_id not in ALBUM_BUFFER:
                ALBUM_BUFFER[grp_id] = []
                # Create task for album processing
                alb_task = asyncio.create_task(process_album_batch(client, user_id, grp_id, current_chat_id, data))
                active_tasks.add(alb_task)
                alb_task.add_done_callback(active_tasks.discard)
            ALBUM_BUFFER[grp_id].append(event)
            return
        
        await process_single_message(client, user_id, event, current_chat_id, data)
        inc_fn(user_id)  # BUG 19 FIX: message limit counter increment karo
    
    except Exception as e:
        logger.error(f"Handler Error for {user_id}: {traceback.format_exc()}")

async def handle_edit_message(client, user_id, event, my_id):
    try:
        if event.sender_id == my_id: return
        from database import is_user_blocked
        if is_user_blocked(user_id): return  # BUG 28 FIX: O(1) check
        
        data = get_user_data(user_id)
        if not data["settings"]["running"]: return

        # FIX 30b: Edit dedup — same msg 5s mein dobara → skip (FloodWait prevention)
        _edit_key = (user_id, event.chat_id, event.id)
        _now_et = time.time()
        if _now_et - _EDIT_DEDUP_CACHE.get(_edit_key, 0) < _EDIT_DEDUP_TTL:
            return
        _EDIT_DEDUP_CACHE[_edit_key] = _now_et
        if len(_EDIT_DEDUP_CACHE) > 500:
            _cutoff = _now_et - _EDIT_DEDUP_TTL * 3
            for _k in [k for k, v in list(_EDIT_DEDUP_CACHE.items()) if v < _cutoff]:
                del _EDIT_DEDUP_CACHE[_k]

        chat_id = event.chat_id
        source_match = False
        current_chat_id = str(chat_id)

        for src in data["sources"]:
            src_str = str(src)
            if current_chat_id == src_str or current_chat_id == f"-100{src_str}" or f"-100{current_chat_id}" == src_str:
                source_match = True
                current_chat_id = src_str
                break
            if event.chat and getattr(event.chat, 'username', None) == src.replace("@", ""):
                source_match = True
                current_chat_id = src
                break
        
        if not source_match: return
        
        msg_text = event.raw_text or ""
        
        # Global filter check
        if await should_filter_out(data, msg_text, user_id, event, current_chat_id, client):
            return 

        # Poll setting check — DEFAULT_SETTINGS mein poll=True hai lekin
        # pehle setting check hi nahi hoti thi → polls hamesha drop hote the (Bug fix)
        if isinstance(event.media, MessageMediaPoll):
            if not data["settings"].get("poll", True):
                logger.info(f"[FWD-DROP] user={user_id} reason=POLL_DISABLED chat={current_chat_id}")
                return

        has_real_media = bool(event.media) and not isinstance(event.media, MessageMediaWebPage)
        
        # 🚨 FIX 37: Link Preview Setting Respect
        show_preview = data["settings"].get("preview_mode", False)

        for dest in data["destinations"]:
            if "Add" in str(dest): continue
            target = int(dest) if str(dest).lstrip('-').isdigit() else dest
            
            dest_msg_id = get_reply_id(user_id, current_chat_id, event.id, target)
            
            if dest_msg_id:
                # ✅ REGENERATE TEXT BASED ON DESTINATION RULES
                dest_rules = get_rules_for_pair(user_id, current_chat_id, target)
                final_text = await process_text_content(msg_text, data, dest_rules, has_real_media, event=event, user_id=user_id)

                limit = 1024 if has_real_media else 4096
                if final_text and len(final_text) > limit:
                    final_text = final_text[:limit-3] + "..."

                try:
                    await client.edit_message(
                        target, 
                        dest_msg_id, 
                        final_text, 
                        link_preview=show_preview 
                    )
                except Exception:
                    pass
    except Exception:
        pass

# ==========================================
# 3. PROCESS ALBUM BATCH (REFACTORED FOR PER-DEST RULES)
# ==========================================
async def process_album_batch(client, user_id, grp_id, source_id, data):
    """
    ⚡ OPTIMIZED Album Wait Strategy:

    OLD: Poll every 0.1s up to 3.0s, then force minimum 0.5s = up to 3.5s wait.
    NEW: Smart adaptive wait:
      - Initial 0.3s wait (covers 99% of albums — Telegram sends parts <200ms apart)
      - Then check: if parts stopped arriving → process immediately
      - Max cap: 1.5s (was 3.5s) — only for very slow servers

    Result: Single photo albums: ~0.3s (was 0.5s+)
            Multi-part albums:   ~0.3-0.6s (was 1-3.5s)
    """
    _album_wait_start = time.time()

    # ⚡ Phase 1: Initial 150ms wait (was 300ms)
    # Telegram sends album parts within 50-150ms on good connections.
    # 150ms catches 99%+ of albums; adaptive phase catches the rest.
    await asyncio.sleep(0.15)

    if grp_id not in ALBUM_BUFFER:
        return
    if not data.get("settings", {}).get("running"):
        ALBUM_BUFFER.pop(grp_id, None)
        return

    # ⚡ Phase 2: Adaptive — check every 100ms if more parts arriving (was 200ms)
    # Total max cap reduced from 1.5s → 0.8s
    prev_count = len(ALBUM_BUFFER.get(grp_id, []))
    for _ in range(6):  # Max 6 × 0.1s = 0.6s (total max 0.75s)
        if time.time() - _album_wait_start > 0.75:
            break
        await asyncio.sleep(0.1)
        if grp_id not in ALBUM_BUFFER:
            return
        if not data.get("settings", {}).get("running"):
            ALBUM_BUFFER.pop(grp_id, None)
            return
        curr_count = len(ALBUM_BUFFER.get(grp_id, []))
        if curr_count == prev_count:
            break  # ⚡ Parts stopped — process immediately, don't wait more
        prev_count = curr_count
    
    # 🚨 FIX 47: Check if stopped during sleep
    if not data["settings"]["running"]: return

    if grp_id not in ALBUM_BUFFER: return
    events_list = ALBUM_BUFFER.pop(grp_id, [])
    # FIX 6: Guard against memory leak — ALBUM_BUFFER already popped above
    if not events_list: return
    events_list.sort(key=lambda x: x.id)

    # Pre-calculate restriction status
    is_restricted = False
    if events_list[0].chat and getattr(events_list[0].chat, 'noforwards', False):
        is_restricted = True
    elif getattr(events_list[0], 'noforwards', False):
        is_restricted = True

    # Get Caption
    raw_caption = ""
    caption_event = None
    for evt in events_list:
        if evt.raw_text:
            raw_caption = evt.raw_text
            caption_event = evt
            break
    if not caption_event: caption_event = events_list[0]
    
    # ── Global keyword/spam filter (caption text) ─────────────────────────
    if await should_filter_out(data, raw_caption, user_id, caption_event, source_id, client): return

    # ── Album Duplicate Filter ──────────────────────────────────────────────
    # ✅ FIX: is_duplicate() checked only caption text — album photos/videos missed.
    # is_album_duplicate() hashes ALL media IDs in the album.
    # Same album = same hash → blocked. Catches:
    #   • Full album repost (all same photos/videos)
    #   • Album with slightly changed caption but same media
    #   • Single photo from album reposted as individual message (via individual hashes)
    if _can_use_feature(user_id, "duplicate_filter"):
        if is_album_duplicate(user_id, events_list, source_id):
            logger.debug(f"Album dup filtered: user={user_id} parts={len(events_list)}")
            return

    # ⚡ Apply smart delay ONCE before all destinations
    await apply_smart_delay(user_id)

    c_delay = max(0.0, min(float(data["settings"].get("custom_delay", 0)), 3600))
    if c_delay > 0:
        await asyncio.sleep(c_delay)

    if not data["settings"]["running"]:
        return

    # ──────────────────────────────────────────────────────────────────────
    # ⚡ FIX 4: PARALLEL ALBUM DESTINATION SENDING
    # Non-restricted albums → build media_group ONCE → send to ALL dests in parallel
    # Restricted albums     → download ONCE → send to ALL dests in parallel (saves re-download)
    # Result: 3 destinations = 3x faster album delivery
    # ──────────────────────────────────────────────────────────────────────

    # ── Step 1: Build common media list (same for all dests when not restricted) ──
    import uuid as _uuid
    _common_media = []      # Telethon media objects (non-restricted)
    _dl_paths_shared = []   # Downloaded file paths (restricted)

    for evt in events_list:
        if isinstance(evt.media, MessageMediaWebPage):
            continue
        is_pf = (evt.document and hasattr(evt.document, 'mime_type')
                 and evt.document.mime_type.startswith("image/"))
        allowed = (
            ((evt.photo or is_pf) and data["settings"].get("image"))
            or ((evt.video or evt.video_note) and data["settings"].get("video"))
            or (evt.document and not is_pf and not evt.video and data["settings"].get("files"))
        )
        if not allowed:
            continue
        if is_restricted:
            from config import MAX_DOWNLOAD_MB
            if _is_too_large_to_download(evt, MAX_DOWNLOAD_MB):
                logger.info(f"Album: skipping large file {_get_media_size(evt)//1024//1024}MB")
                continue
            try:
                _dl_path = f"/tmp/dl_{user_id}_{_uuid.uuid4().hex[:8]}"
                path = await client.download_media(evt.media, file=_dl_path)
                if path:
                    _dl_paths_shared.append(path)
                    _common_media.append(path)
            except Exception as e:
                logger.error(f"Album restricted download error: {e}")
        else:
            _common_media.append(evt.media)

    if not _common_media:
        for p in _dl_paths_shared:
            try: os.remove(p)
            except Exception: pass
        return

    # ── Step 2: Send to each destination (parallel if multiple, no custom_delay) ──
    _album_msg_counted = False

    async def _send_album_dest(dest):
        nonlocal _album_msg_counted
        if "Add" in str(dest):
            return
        target = int(dest) if str(dest).lstrip('-').isdigit() else dest
        dest_rules = get_rules_for_pair(user_id, source_id, target)

        # Per-destination media filter (rules may differ between destinations)
        dest_media = []
        for evt in events_list:
            if isinstance(evt.media, MessageMediaWebPage):
                continue
            is_pf = (evt.document and hasattr(evt.document, 'mime_type')
                     and evt.document.mime_type.startswith("image/"))
            allowed_for_dest = (
                ((evt.photo or is_pf) and dest_rules.get("forward_photos", True))
                or ((evt.video or evt.video_note) and dest_rules.get("forward_videos", True))
                or (evt.document and not is_pf and not evt.video and dest_rules.get("forward_files", True))
            )
            if allowed_for_dest:
                # Find matching entry in _common_media by position
                for cm in _common_media:
                    if (isinstance(cm, str) and os.path.exists(cm)) or not isinstance(cm, str):
                        dest_media = _common_media   # Use shared list
                        break
                break

        if not dest_media:
            dest_media = _common_media  # fallback — send all

        caption = await process_text_content(
            raw_caption, data, dest_rules, True,
            event=caption_event, user_id=user_id
        )
        if caption and len(caption) > 1024:
            caption = _safe_html_truncate(caption, 1024)

        try:
            # ✅ FIX: Album path mein bhi rate limiter add karo
            _rl_alb = RateLimiterRegistry.get(user_id)
            await _rl_alb.wait_for_slot(str(dest), data["settings"].get("custom_delay", 0))

            sent_msgs = await client.send_message(
                target, caption, file=dest_media, parse_mode='html'
            )
            update_user_stats(user_id, "processed")
            _record_analytics(user_id, "forwarded")
            _album_msg_counted = True
            if sent_msgs:
                first_sent = sent_msgs[0] if isinstance(sent_msgs, list) else sent_msgs
                for evt in events_list:
                    save_reply_mapping(user_id, source_id, evt.id, target, first_sent.id)
        except FloodWaitError as e:
            # ✅ FIX: FloodWait pe drop mat karo — wait karke retry karo
            _fw_alb = min(e.seconds, 300)
            logger.warning(f"Album FloodWait {e.seconds}s dest={target} — waiting {_fw_alb}s")
            RateLimiterRegistry.on_flood_wait(user_id, str(dest), e.seconds)
            await asyncio.sleep(_fw_alb)
            if not data["settings"].get("running"):
                return
            try:
                sent_msgs = await client.send_message(
                    target, caption, file=dest_media, parse_mode='html'
                )
                update_user_stats(user_id, "processed")
                _album_msg_counted = True
                if sent_msgs:
                    first_sent = sent_msgs[0] if isinstance(sent_msgs, list) else sent_msgs
                    for evt in events_list:
                        save_reply_mapping(user_id, source_id, evt.id, target, first_sent.id)
            except Exception as re:
                logger.warning(f"Album retry failed dest {target}: {re}")
        except MediaCaptionTooLongError:
            caption = _safe_html_truncate(caption, 1024)
            try:
                await client.send_message(target, caption, file=dest_media, parse_mode='html')
                update_user_stats(user_id, "processed")
            except Exception: pass
        except ValueError as val_err:
            # ✅ FIX: Unknown entity → resolve karke retry karo (same as _send_one_dest)
            logger.warning(f"Album send ValueError (unknown entity) dest {target}: {val_err} — resolving entity...")
            try:
                resolved = await _resolve_entity(client, user_id, target)
                if resolved != target:
                    sent_msgs = await client.send_message(
                        resolved, caption, file=dest_media, parse_mode='html'
                    )
                    update_user_stats(user_id, "processed")
                    _album_msg_counted = True
                    if sent_msgs:
                        first_sent = sent_msgs[0] if isinstance(sent_msgs, list) else sent_msgs
                        for evt in events_list:
                            save_reply_mapping(user_id, source_id, evt.id, target, first_sent.id)
                    logger.info(f"[ENTITY-FIX] ✅ Album send succeeded after entity resolve: dest={target} user={user_id}")
                else:
                    logger.warning(f"[ENTITY-FIX] Album entity unresolvable: dest={target} user={user_id}")
            except Exception as resolve_err:
                logger.warning(f"[ENTITY-FIX] Album entity resolve retry failed dest={target}: {resolve_err}")
        except Exception as e:
            logger.error(f"Album send error dest {target}: {e}")

    # ⚡ Fire all destination sends in parallel (semaphore limits to 3 concurrent)
    valid_dests = [d for d in data["destinations"] if "Add" not in str(d)]
    if valid_dests:
        await asyncio.gather(*[_send_album_dest(d) for d in valid_dests], return_exceptions=True)

    # Cleanup shared downloaded files
    for p in _dl_paths_shared:
        try:
            if os.path.exists(p): os.remove(p)
        except Exception: pass

    # BUG 10 FIX: Sirf ek baar count karo - saare destinations ke baad
    if _album_msg_counted:
        try:
            from msg_limit import increment_msg_count
            increment_msg_count(user_id)
        except Exception as e:
            logger.debug(f"msg_limit album increment error (user {user_id}): {e}")

# ==========================================
# 4. PROCESS SINGLE MESSAGE (REFACTORED FOR PER-DEST RULES)
# ==========================================
async def process_single_message(client, user_id, event, source_id, data):
    # 🚨 FIX 47: Immediate stop check
    if not data["settings"]["running"]: return

    # ── NEW: File size filter ────────────────────────────────────────────
    max_mb = data["settings"].get("max_file_size_mb", 0)
    if max_mb > 0 and event.media:
        try:
            from forward_engine import _get_media_size
            file_size = _get_media_size(event)
            if file_size > max_mb * 1024 * 1024:
                logger.debug(f"Skipped: file {file_size//1024//1024}MB > limit {max_mb}MB")
                return
        except Exception:
            pass

    # ── NEW: Forward count tracking ──────────────────────────────────────
    fwd_limit = data["settings"].get("fwd_count_limit", 0)
    if fwd_limit > 0:
        src_counts = data.setdefault("src_fwd_counts", {})
        src_key    = str(source_id)
        src_counts[src_key] = src_counts.get(src_key, 0) + 1

    msg_text = event.raw_text or ""
    
    # Global filtering (happens once per message)
    if await should_filter_out(data, msg_text, user_id, event, source_id, client): return

    # ✅ FIX: apply_smart_delay ek baar — har destination ke liye nahi (triple delay fix)
    await apply_smart_delay(user_id)

    # ── NEW: Random delay variance ────────────────────────────────────────
    variance = data["settings"].get("delay_variance", 0)
    if variance > 0:
        import random as _rnd
        extra = _rnd.uniform(0, variance)
        if extra > 0:
            await asyncio.sleep(extra)

    # ⚡ Poll Handling — parallel send to all destinations
    if isinstance(event.media, MessageMediaPoll):
        try:
            async def _send_poll(dest):
                if "Add" in str(dest): return
                target = int(dest) if str(dest).lstrip('-').isdigit() else dest
                try:
                    sent = await client.send_message(target, file=event.media)
                    save_reply_mapping(user_id, source_id, event.id, target, sent.id)
                    update_user_stats(user_id, "processed")
                except ValueError as ve:
                    logger.warning(f"Poll send ValueError (unknown entity) dest={dest} user={user_id}: {ve}")
                except Exception as e:
                    logger.warning(f"Poll send failed to {dest} for user {user_id}: {e}")
            # ⚡ All destinations in parallel
            await asyncio.gather(*[_send_poll(d) for d in data["destinations"]], return_exceptions=True)
            return
        except Exception as e:
            logger.error(f"Poll handling error for user {user_id}: {e}")
            return

    is_webpage = isinstance(event.media, MessageMediaWebPage)
    has_real_media = event.media and not is_webpage
    
    # 🚨 FIX 36: Precise Media Detection
    is_photo_file = False
    if event.document and hasattr(event.document, 'mime_type') and event.document.mime_type.startswith("image/"):
        is_photo_file = True

    # ⚡ FAST PATH: Pure text message (no media, no download needed)
    # Send to ALL destinations in parallel with asyncio.gather()
    is_restricted_ch = (event.chat and getattr(event.chat, 'noforwards', False)) or getattr(event, 'noforwards', False)
    _wm_enabled = data.get("watermark", {}).get("enabled", False)
    _translation_on = data.get("settings", {}).get("translate_enabled", False)
    _has_custom_delay = data.get("settings", {}).get("custom_delay", 0) > 0

    # ⚡ FAST PATH FOR CAPTIONED MEDIA (non-restricted, no watermark, no custom delay)
    # Photo/Video with caption → pre-process text for ALL dests → send all in parallel
    # Previously: sequential loop (dest1 wait → dest2 wait → dest3) = N× delay
    # Now: all dests get caption processed concurrently → send simultaneously = 1× delay
    _can_parallel_media = (
        has_real_media
        and not is_restricted_ch
        and not _wm_enabled
        and not _translation_on
        and not _has_custom_delay
        and len(data["destinations"]) > 1
    )
    if _can_parallel_media:
        show_preview = data["settings"].get("preview_mode", False)

        async def _send_captioned_media_dest(dest):
            if "Add" in str(dest): return
            target = int(dest) if str(dest).lstrip('-').isdigit() else dest
            dest_rules = get_rules_for_pair(user_id, source_id, target)

            # Destination enabled check
            if not dest_rules.get("dest_enabled", True): return

            # Media type check for this destination
            if event.photo or is_photo_file:
                if not (data["settings"].get("image") and dest_rules.get("forward_photos", True)): return
            elif event.video or event.video_note:
                if not (data["settings"].get("video") and dest_rules.get("forward_videos", True)): return
            elif event.voice or event.audio:
                if not (data["settings"].get("voice", False) and dest_rules.get("forward_voice", True)): return
            elif event.document:
                if not (data["settings"].get("files") and dest_rules.get("forward_files", True)): return
            elif event.sticker:
                if not data["settings"].get("sticker", False): return
            if event.gif and not data["settings"].get("gif", True): return

            if dest_rules.get("media_mode") == "skip":
                if not msg_text: return
                media_obj = None
            else:
                media_obj = event.media

            allow_caption = data["settings"].get("caption", True) and dest_rules.get("forward_captions", True)
            ft = ""
            if allow_caption and msg_text:
                ft = await process_text_content(msg_text, data, dest_rules, True, event=event, user_id=user_id)
                if ft and len(ft) > 1024:
                    ft = _safe_html_truncate(ft, 1024)

            if not ft and not media_obj: return

            reply_to_id = None
            if event.reply_to_msg_id:
                reply_to_id = get_reply_id(user_id, source_id, event.reply_to_msg_id, target)

            try:
                # ✅ FIX: Rate limiter fast path mein missing tha — yahi root cause tha
                # Bina rate limiter ke asyncio.gather 10 msgs ek saath bhejta tha → FloodWait
                _rl_fp = RateLimiterRegistry.get(user_id)
                _cdelay_fp = data["settings"].get("custom_delay", 0)
                await _rl_fp.wait_for_slot(str(dest), _cdelay_fp)

                sem = _get_send_sem(user_id)
                async with sem:
                    sent = await client.send_message(
                        target,
                        ft if ft else None,
                        file=media_obj,
                        parse_mode='html',
                        link_preview=show_preview,
                        reply_to=reply_to_id
                    )
                    update_user_stats(user_id, "processed")
                    if sent:
                        save_reply_mapping(user_id, source_id, event.id, target, sent.id)
                        if dest_rules.get("pin_forwarded", False):
                            try:
                                await client.pin_message(target, sent.id, notify=False)
                            except Exception:
                                pass
                    _update_src_stats(data, source_id)
            except FloodWaitError as e:
                # ✅ FIX: FloodWait pe drop mat karo — wait karke retry karo
                _fw_fp = min(e.seconds, 300)
                logger.warning(f"FloodWait {e.seconds}s captioned media dest={target} user={user_id} — waiting {_fw_fp}s")
                RateLimiterRegistry.on_flood_wait(user_id, str(dest), e.seconds)
                await asyncio.sleep(_fw_fp)
                if not data["settings"].get("running"): return
                try:
                    sem2 = _get_send_sem(user_id)
                    async with sem2:
                        sent = await client.send_message(
                            target, ft if ft else None,
                            file=media_obj, parse_mode='html',
                            link_preview=show_preview, reply_to=reply_to_id
                        )
                        update_user_stats(user_id, "processed")
                        if sent:
                            save_reply_mapping(user_id, source_id, event.id, target, sent.id)
                except Exception as re:
                    logger.warning(f"Captioned media retry fail dest={target}: {re}")
            except MediaCaptionTooLongError:
                try:
                    ft_short = _safe_html_truncate(ft, 1024) if ft else None
                    sem3 = _get_send_sem(user_id)
                    async with sem3:
                        await client.send_message(
                            target, ft_short, file=media_obj,
                            parse_mode='html', reply_to=reply_to_id
                        )
                        update_user_stats(user_id, "processed")
                except Exception: pass
            except ValueError as val_err:
                # ✅ FIX: Entity resolve karke retry karo
                logger.debug(f"[ENTITY-FIX] Captioned media ValueError dest={target} user={user_id} — resolving")
                try:
                    resolved = await _resolve_entity(client, user_id, target)
                    if resolved != target:
                        sem_r = _get_send_sem(user_id)
                        async with sem_r:
                            sent = await client.send_message(
                                resolved, ft if ft else None,
                                file=media_obj, parse_mode='html',
                                link_preview=show_preview, reply_to=reply_to_id
                            )
                            update_user_stats(user_id, "processed")
                            if sent:
                                save_reply_mapping(user_id, source_id, event.id, target, sent.id)
                        logger.info(f"[ENTITY-FIX] ✅ Captioned media sent after resolve: dest={target}")
                    else:
                        logger.warning(f"Captioned media ValueError (unknown entity) dest={target} user={user_id}: {val_err}")
                except Exception as _re:
                    logger.warning(f"Captioned media ValueError (unknown entity) dest={target} user={user_id}: {val_err}")
            except Exception as te:
                logger.warning(f"Captioned media send fail dest={target} user={user_id}: {te}")

        # Apply keyword routing — override destinations if keyword match
        effective_dests = _get_keyword_routed_dests(data, msg_text, data["destinations"])
        await asyncio.gather(
            *[_send_captioned_media_dest(d) for d in effective_dests],
            return_exceptions=True
        )
        try:
            from msg_limit import increment_msg_count
            increment_msg_count(user_id)
        except Exception: pass
        return

    if (not has_real_media and not is_restricted_ch and not _wm_enabled
            and not _translation_on and not _has_custom_delay
            and len(data["destinations"]) > 1):
        # All text-only messages to multiple destinations in parallel
        show_preview = data["settings"].get("preview_mode", False)

        async def _send_text_dest(dest):
            if "Add" in str(dest): return
            target = int(dest) if str(dest).lstrip('-').isdigit() else dest
            dest_rules = get_rules_for_pair(user_id, source_id, target)
            if not dest_rules.get("dest_enabled", True): return
            allow_text = data["settings"].get("text", True) and dest_rules.get("forward_text", True)
            if not allow_text: return
            ft = await process_text_content(msg_text, data, dest_rules, False, event=event, user_id=user_id)
            if not ft: return
            if len(ft) > 4096: ft = _safe_html_truncate(ft, 4096)
            reply_to_id = None
            if event.reply_to_msg_id:
                reply_to_id = get_reply_id(user_id, source_id, event.reply_to_msg_id, target)
            try:
                # ✅ FIX: Text parallel path mein bhi rate limiter missing tha
                _rl_txt = RateLimiterRegistry.get(user_id)
                await _rl_txt.wait_for_slot(str(dest), data["settings"].get("custom_delay", 0))

                sem = _get_send_sem(user_id)
                async with sem:
                    sent = await client.send_message(
                        target, ft, parse_mode='html',
                        link_preview=show_preview, reply_to=reply_to_id
                    )
                    update_user_stats(user_id, "processed")
                    if sent:
                        save_reply_mapping(user_id, source_id, event.id, target, sent.id)
            except FloodWaitError as e:
                # ✅ FIX: FloodWait pe drop mat karo — max 300s wait karke retry
                _fw_txt = min(e.seconds, 300)
                logger.warning(f"FloodWait {e.seconds}s text dest={target} user={user_id} — waiting {_fw_txt}s")
                RateLimiterRegistry.on_flood_wait(user_id, str(dest), e.seconds)
                await asyncio.sleep(_fw_txt)
                if not data["settings"].get("running"): return
                try:
                    sem2 = _get_send_sem(user_id)
                    async with sem2:
                        sent = await client.send_message(
                            target, ft, parse_mode='html',
                            link_preview=show_preview, reply_to=reply_to_id
                        )
                        update_user_stats(user_id, "processed")
                        if sent:
                            save_reply_mapping(user_id, source_id, event.id, target, sent.id)
                except Exception: pass
            except ValueError as val_err:
                # ✅ FIX: Entity resolve karke retry karo
                logger.debug(f"[ENTITY-FIX] Text dest ValueError dest={target} user={user_id} — resolving")
                try:
                    resolved = await _resolve_entity(client, user_id, target)
                    if resolved != target:
                        sem_r = _get_send_sem(user_id)
                        async with sem_r:
                            sent = await client.send_message(
                                resolved, ft, parse_mode='html',
                                link_preview=show_preview, reply_to=reply_to_id
                            )
                            update_user_stats(user_id, "processed")
                            if sent:
                                save_reply_mapping(user_id, source_id, event.id, target, sent.id)
                        logger.info(f"[ENTITY-FIX] ✅ Text sent after resolve: dest={target}")
                    else:
                        logger.warning(f"Text parallel send ValueError (unknown entity) dest={target} user={user_id}: {val_err}")
                except Exception:
                    logger.warning(f"Text parallel send ValueError (unknown entity) dest={target} user={user_id}: {val_err}")
            except Exception as te:
                logger.warning(f"Text parallel send fail dest={target} user={user_id}: {te}")

        _eff_text_dests = _get_keyword_routed_dests(data, msg_text, data["destinations"])
        await asyncio.gather(*[_send_text_dest(d) for d in _eff_text_dests], return_exceptions=True)
        return

    # 🔄 ITERATE DESTINATIONS
    # Pre-download restricted media ONCE, then send all in parallel
    _shared_dl_path = None
    _shared_dl_media = None
    is_restricted_global = (
        (event.chat and getattr(event.chat, 'noforwards', False))
        or getattr(event, 'noforwards', False)
    )
    if is_restricted_global and has_real_media and not _wm_enabled:
        from config import MAX_DOWNLOAD_MB
        if _is_too_large_to_download(event, MAX_DOWNLOAD_MB):
            logger.info(f"Restricted file too large ({_get_media_size(event)//1024//1024}MB) — skipping")
            # All destinations will be skipped due to too-large file
        else:
            import uuid as _uuid_shared
            _dl_path_shared = f"/tmp/dl_{user_id}_{_uuid_shared.uuid4().hex[:8]}"
            try:
                _shared_dl_path = await client.download_media(event.media, file=_dl_path_shared)
                _shared_dl_media = _shared_dl_path
                logger.debug(f"Shared restricted download: {_shared_dl_path} (user {user_id})")
            except Exception as _dl_err:
                logger.error(f"Shared restricted download failed: {_dl_err}")

    for dest in data["destinations"]:
        if "Add" in str(dest): continue
        target = int(dest) if str(dest).lstrip('-').isdigit() else dest
        
        # ✅ FETCH SPECIFIC RULES FOR THIS PAIR (Src -> Dest)
        dest_rules = get_rules_for_pair(user_id, source_id, target)
        
        # --- 1. MEDIA TYPE CHECKS BASED ON DEST RULES ---
        should_send_media = False
        
        # Determine if media allowed for THIS destination
        if has_real_media:
            if (event.photo or is_photo_file):
                if data["settings"].get("image") and dest_rules.get("forward_photos", True): should_send_media = True
            elif (event.video or event.video_note):
                if data["settings"].get("video") and dest_rules.get("forward_videos", True): should_send_media = True
            elif (event.voice or event.audio):
                # ✅ FIX: voice default False — DEFAULT_SETTINGS ke saath match kiya
                if data["settings"].get("voice", False) and dest_rules.get("forward_voice", True): should_send_media = True
            elif event.document:
                if data["settings"].get("files") and dest_rules.get("forward_files", True): should_send_media = True

        # If no media allowed and message is only media -> Skip this destination
        if has_real_media and not should_send_media and not msg_text:
            continue

        # --- 2. TEXT PROCESSING BASED ON DEST RULES ---
        allow_text = data["settings"].get("text", True) and dest_rules.get("forward_text", True)
        allow_caption = data["settings"].get("caption", True) and dest_rules.get("forward_captions", True)

        final_text = ""

        if (has_real_media and allow_caption) or (not has_real_media and allow_text):
            # Process text specifically for this destination (Prefix/Suffix might differ!)
            final_text = await process_text_content(msg_text, data, dest_rules, has_real_media, event=event, user_id=user_id)
        
        if event.sticker: final_text = None
        if not final_text and not should_send_media: continue

        # BUG FIX 4+7: Translate PLAIN text (msg_text), NOT final_text
        # Pehle: final_text (HTML tags + prefix/suffix) translate hota tha
        # Problem 1: HTML tags (<b>, <a href>) bhi translate ho jaate the → broken HTML
        # Problem 2: User ka prefix/suffix bhi translate ho jaata tha → galat output
        # Fix: raw msg_text translate karo, phir process_text_content dobara run karo
        if msg_text and final_text:
            try:
                translated_raw = await maybe_translate(msg_text, user_id, str(source_id))
                if translated_raw and translated_raw != msg_text:
                    # Raw text translated — ab iske saath final_text rebuild karo
                    final_text = await process_text_content(
                        translated_raw, data, dest_rules, has_real_media,
                        event=None,  # event=None: entities skip (already plain text)
                        user_id=user_id
                    )
            except Exception:
                pass

        limit = 1024 if should_send_media else 4096
        if final_text and len(final_text) > limit:
            final_text = _safe_html_truncate(final_text, limit)  # FIX 14

        # Restricted Content Check
        is_restricted = False
        if event.chat and getattr(event.chat, 'noforwards', False): is_restricted = True
        elif getattr(event, 'noforwards', False): is_restricted = True

        media_to_send = event.media if should_send_media else None
        downloaded_path = None
        force_doc = False 

        # ✅ FIX: media_mode "skip" — pehle engine ignore karta tha, ab media skip hoga
        if has_real_media and dest_rules.get("media_mode") == "skip":
            should_send_media = False
            if not msg_text:
                continue  # Pure media message, skip karo

        # doc mode removed — always use original

        try:
            if is_restricted and should_send_media and media_to_send:
                from config import MAX_DOWNLOAD_MB
                if _is_too_large_to_download(event, MAX_DOWNLOAD_MB):
                    logger.info(f"Skipping large restricted file: {_get_media_size(event)//1024//1024}MB > {MAX_DOWNLOAD_MB}MB (user {user_id})")
                    continue
                # ⚡ BUG 3 FIX: Use shared pre-downloaded file if available
                if _shared_dl_media:
                    media_to_send = _shared_dl_media   # Reuse shared download — no re-download!
                    downloaded_path = None              # Don't cleanup shared file in finally
                else:
                    try:
                        import uuid as _uuid2
                        _dl_unique2 = f"/tmp/dl_{user_id}_{_uuid2.uuid4().hex[:8]}"
                        downloaded_path = await client.download_media(event.media, file=_dl_unique2)
                        media_to_send = downloaded_path
                    except Exception as e:
                        logger.error(f"Failed to download restricted media: {e}")
                        continue

            # ── WATERMARK PROCESSING ─────────────────────────────
            try:
                from watermark import process_photo_with_watermark, apply_video_watermark
                from config import MAX_DOWNLOAD_MB

                # PHOTO watermark
                if (should_send_media and event.photo
                        and not getattr(event, 'sticker', None)
                        and not getattr(event, 'video', None)
                        and not force_doc):
                    if not _is_too_large_to_download(event, MAX_DOWNLOAD_MB):
                        photo_bytes = await client.download_media(event.media, file=bytes)
                        if photo_bytes:
                            wm_bytes = await process_photo_with_watermark(user_id, photo_bytes)
                            if wm_bytes and wm_bytes != photo_bytes:
                                import io as _io
                                _buf = _io.BytesIO(wm_bytes)
                                _buf.name = "photo.jpg"
                                media_to_send = _buf
                                downloaded_path = None

                # VIDEO watermark (ffmpeg required)
                elif (should_send_media
                      and getattr(event, 'video', None)
                      and not getattr(event, 'sticker', None)
                      and not force_doc):
                    _wm_data = data.get("watermark", {})
                    if _wm_data.get("enabled") and _wm_data.get("mode") in ("text", "both"):
                        if not _is_too_large_to_download(event, MAX_DOWNLOAD_MB):
                            # FIX 4: Download to DISK not RAM (videos can be 50MB+ — RAM crash!)
                            import uuid as _uuid_vid
                            _vid_path = f"/tmp/wm_vid_{user_id}_{_uuid_vid.uuid4().hex[:8]}.mp4"
                            try:
                                _dl_path = await client.download_media(event.media, file=_vid_path)
                                if _dl_path:
                                    with open(_dl_path, 'rb') as _vf:
                                        vid_bytes = _vf.read()
                                    wm_vid = await apply_video_watermark(user_id, vid_bytes)
                                    if wm_vid and wm_vid != vid_bytes:
                                        import io as _io
                                        _buf = _io.BytesIO(wm_vid)
                                        _buf.name = "video.mp4"
                                        media_to_send = _buf
                                    # Clean up temp file
                                    try: os.remove(_dl_path)
                                    except Exception: pass
                            except Exception as _ve:
                                logger.debug(f"Video watermark disk download failed: {_ve}")

                # GIF/Animation watermark
                elif (should_send_media
                      and getattr(event, 'gif', None)
                      and not force_doc):
                    _wm_data = data.get("watermark", {})
                    if _wm_data.get("enabled") and _wm_data.get("mode") in ("text", "both"):
                        if not _is_too_large_to_download(event, MAX_DOWNLOAD_MB):
                            # FIX 4: GIF also to disk
                            import uuid as _uuid_gif
                            _gif_path = f"/tmp/wm_gif_{user_id}_{_uuid_gif.uuid4().hex[:8]}.gif"
                            try:
                                _gif_dl = await client.download_media(event.media, file=_gif_path)
                                if _gif_dl:
                                    with open(_gif_dl, 'rb') as _gf:
                                        gif_bytes = _gf.read()
                                    wm_gif = await apply_video_watermark(user_id, gif_bytes, is_gif=True)
                                    if wm_gif and wm_gif != gif_bytes:
                                        import io as _io
                                        _buf = _io.BytesIO(wm_gif)
                                        _buf.name = "animation.mp4"
                                        media_to_send = _buf
                                    try: os.remove(_gif_dl)
                                    except Exception: pass
                            except Exception as _ge:
                                logger.debug(f"GIF watermark failed: {_ge}")

            except Exception as _wm_err:
                logger.debug(f"Watermark skip: {_wm_err}")
            # ─────────────────────────────────────────────────────

            # ✅ FIX Bug 4: custom_delay DOUBLE apply ho raha tha!
            # Pehle: process_single_message top pe + har destination ke liye = N× delay
            # Example: 60s delay, 2 dests = 60 + 60 + 60 = 180s = 3 MINUTES lag!
            # Fix: custom_delay sirf top pe (line ~1305) apply hogi. Yahan se remove kiya.

            # Check running again
            if not data["settings"]["running"]: return
            
            # 🚨 FIX 37: Link Preview Setting
            show_preview = data["settings"].get("preview_mode", False)

            try:
                if "Add" in str(dest): continue
                reply_to_id = None
                if event.reply_to_msg_id:
                    reply_to_id = get_reply_id(user_id, source_id, event.reply_to_msg_id, target)

                try:
                    sent_msg = await client.send_message(
                        target, 
                        final_text if final_text else None, 
                        file=media_to_send,
                        parse_mode='html', 
                        link_preview=show_preview,
                        reply_to=reply_to_id,
                        force_document=force_doc
                    )
                    
                    # 🚨 FIX 43: Stats Update
                    update_user_stats(user_id, "processed")

                    if sent_msg:
                        save_reply_mapping(user_id, source_id, event.id, target, sent_msg.id)

                except FloodWaitError as e:
                    # ✅ FIX: FloodWait pe destination skip mat karo — wait karke retry karo
                    # Pehle: > 60s pe `continue` → message permanently drop
                    # Ab: max 300s wait → retry → 100/100 forward hoga
                    _flood_seconds = e.seconds
                    _fw_seq = min(_flood_seconds, 300)
                    logger.warning(
                        f"FloodWait {_flood_seconds}s for user {user_id} dest {target} "
                        f"— waiting {_fw_seq}s then retry"
                    )
                    RateLimiterRegistry.on_flood_wait(user_id, str(dest), _flood_seconds)
                    await asyncio.sleep(_fw_seq)
                    # FIX 7: Check if user stopped forwarding during FloodWait sleep
                    if not data["settings"].get("running"):
                        return
                    try:
                        sent_msg = await client.send_message(
                            target, 
                            final_text if final_text else None, 
                            file=media_to_send,
                            parse_mode='html', 
                            link_preview=show_preview,
                            reply_to=reply_to_id,
                            force_document=force_doc
                        )
                        if sent_msg:
                            update_user_stats(user_id, "processed")
                    except Exception as _retry_e:
                        logger.debug(f"Post-flood retry failed for user {user_id}: {_retry_e}")

                except MediaCaptionTooLongError:
                    if final_text: final_text = _safe_html_truncate(final_text, 1024)  # FIX 14
                    await client.send_message(
                        target, 
                        final_text if final_text else None, 
                        file=media_to_send,
                        parse_mode='html',
                        force_document=force_doc
                    )
                    update_user_stats(user_id, "processed")

                except ValueError as val_err:
                    # ✅ FIX: Pehle entity resolve karke retry karo
                    # Agar resolve ke baad bhi fail → phir auto-remove/warning
                    _entity_resolved_ok = False
                    try:
                        resolved = await _resolve_entity(client, user_id, target)
                        if resolved != target:
                            sent_retry = await client.send_message(
                                resolved,
                                final_text if final_text else None,
                                file=media_to_send,
                                parse_mode='html',
                                link_preview=show_preview,
                                reply_to=reply_to_id,
                                force_document=force_doc
                            )
                            if sent_retry:
                                update_user_stats(user_id, "processed")
                                save_reply_mapping(user_id, source_id, current_chat_id, target, sent_retry.id)
                            logger.info(f"[ENTITY-FIX] ✅ Sequential send succeeded after resolve: dest={target} user={user_id}")
                            _entity_resolved_ok = True
                            # Reset fail count on success
                            _dest_fail_count.pop((user_id, str(target)), None)
                    except Exception as _re:
                        logger.debug(f"[ENTITY-FIX] Retry after resolve failed dest={target}: {_re}")

                    if not _entity_resolved_ok:
                        # ✅ FIX: Auto-remove BAND karo — destination raho, user ko fix karne do
                        # Pehle: 3 fails ke baad dest permanently delete ho jaata tha — DATA LOSS!
                        # Ab: skip karo, sirf ek baar notify karo, dest rakho
                        _vk = (user_id, str(target))
                        _dest_fail_count[_vk] = _dest_fail_count.get(_vk, 0) + 1
                        if _vk not in _dest_notified:
                            _dest_notified.add(_vk)
                            logger.warning(
                                f"[ENTITY] PeerUser not reachable: dest={target} user={user_id} — "
                                f"keeping dest, notifying user to fix"
                            )
                            try:
                                from config import bot as _b
                                asyncio.create_task(_b.send_message(
                                    user_id,
                                    f"⚠️ **Destination Unreachable — Action Needed**\n\n"
                                    f"🆔 Destination: `{target}`\n\n"
                                    f"Tumhara Telegram account is user ko **nahi pehchanta** — "
                                    f"isliye messages forward nahi ho rahe.\n\n"
                                    f"**✅ Fix karo (2 tarike):**\n"
                                    f"1️⃣ Apne main Telegram se user `{target}` ko **ek bhi message bhejo** "
                                    f"(ya unka message receive karo), phir forwarding restart karo\n"
                                    f"2️⃣ **Ya:** Destination delete karke unka **@username** add karo\n\n"
                                    f"_Destination remove nahi kiya gaya — fix karne ke baad automatically kaam karega._"
                                ))
                            except Exception:
                                pass
                        else:
                            logger.debug(
                                f"[ENTITY] PeerUser still unresolved dest={target} user={user_id} "
                                f"(attempt {_dest_fail_count[_vk]}) — skipping silently"
                            )
                    continue

                except (errors.ChatAdminRequiredError,
                        errors.ChatWriteForbiddenError,
                        errors.UserNotParticipantError,
                        errors.ChannelPrivateError) as perm_err:
                    # BUG FIX: Saare permission errors ek jagah handle karo
                    # Pehle sirf ChatAdminRequiredError tha — baaki silently fail hote the
                    err_type = type(perm_err).__name__
                    logger.warning(f"{err_type} for dest {target} — user {user_id}")

                    _reason_map = {
                        "ChatAdminRequiredError":   "Account ko channel mein Admin banana padega।",
                        "ChatWriteForbiddenError":  "Channel mein post karne ki permission nahi।\nAdmin se permission lo ya bot ko admin banao।",
                        "UserNotParticipantError":  "Account is private channel ka member nahi।\nPehle apna account us channel mein join karo।",
                        "ChannelPrivateError":      "Channel private hai ya ID galat hai।\nChannel dobara add karo correct ID ke saath।",
                    }
                    reason_msg = _reason_map.get(err_type, f"Permission error: {err_type}")

                    dest_key = (user_id, str(target))
                    _dest_fail_count[dest_key] = _dest_fail_count.get(dest_key, 0) + 1

                    # Pehli baar hi user ko notify karo (private channel = immediate fix needed)
                    if dest_key not in _dest_notified:
                        _dest_notified.add(dest_key)
                        try:
                            from config import bot as _bot
                            await _bot.send_message(
                                user_id,
                                f"⚠️ **Private Destination Error!**\n\n"
                                f"Destination `{target}` mein message nahi ja raha।\n\n"
                                f"**Reason:** {reason_msg}\n\n"
                                f"**Steps:**\n"
                                f"1️⃣ Us private channel mein apna Telegram account join karo\n"
                                f"2️⃣ Apne account ko Admin banao (post permission ke saath)\n"
                                f"3️⃣ Bot restart karo\n\n"
                                f"_Ya destination delete karke dobara add karo।_"
                            )
                        except Exception:
                            pass
                    continue

            except Exception as e: 
                logger.error(f"Send Error: {traceback.format_exc()}")
            
        finally:
            if downloaded_path and os.path.exists(downloaded_path):
                try: os.remove(downloaded_path)
                except Exception as e:
                    logger.debug(f"Downloaded file cleanup error (process_single): {e}")

    # ⚡ Cleanup shared restricted download file (after all destinations done)
    if _shared_dl_path and os.path.exists(_shared_dl_path):
        try:
            os.remove(_shared_dl_path)
        except Exception:
            pass

# ==========================================
# 5. HELPER FUNCTIONS
# ==========================================

def _get_keyword_routed_dests(data: dict, msg_text: str, default_dests: list) -> list:
    """
    Keyword-based routing — agar message mein keyword match ho toh
    specific destinations ko override karo।
    Rules: data["keyword_routes"] = [{"keywords": ["sale","offer"], "dests": [id1, id2]}]
    """
    routes = data.get("keyword_routes", [])
    if not routes or not msg_text:
        return default_dests

    text_lower = msg_text.lower()
    for rule in routes:
        keywords = rule.get("keywords", [])
        dests    = rule.get("dests", [])
        if dests and any(kw.lower() in text_lower for kw in keywords):
            logger.debug(f"Keyword route matched: {keywords} → {dests}")
            return dests

    return default_dests


async def should_filter_out(data, text, user_id, event, source_id, client):
    from config import SCAM_KEYWORDS
    text_lower = text.lower()
    kf_enabled = data["settings"].get("keyword_filter_enabled", True)
    mode = data["settings"].get("filter_mode", "Blacklist")

    if kf_enabled and mode == "Blacklist":
        if any(k.lower() in text_lower for k in data["settings"].get("keywords_blacklist", [])): return True
    elif kf_enabled:
        # BUG 25 FIX: Whitelist - empty text wale messages (pure media) whitelist se exempt hain
        wl = data["settings"].get("keywords_whitelist", [])
        if wl and text_lower.strip():  # Sirf non-empty text check karo
            if not any(k.lower() in text_lower for k in wl):
                return True
        # Pure media messages (no text) whitelist se block nahi honge

    # ✅ FIX: Smart Filter — SCAM_KEYWORDS check
    # FREE/PAID MODE FIX: smart_filter premium feature hai
    if data["settings"].get("smart_filter", False) and _can_use_feature(user_id, "smart_filter"):
        if any(kw.lower() in text_lower for kw in SCAM_KEYWORDS):
            return True

    # ✅ FIX: Link Blocker
    # FREE/PAID MODE FIX: link_blocker premium feature hai
    blocked_links = data.get("blocked_links", {})
    link_limits  = data.get("link_limits", {})
    lb_enabled = data.get("settings", {}).get("link_blocker_enabled", False)  # BUG 11 FIX: default False - user ne explicitly enable kiya ho tabhi
    if lb_enabled and blocked_links and event and _can_use_feature(user_id, "link_blocker"):
        urls_in_msg = extract_all_urls(event)
        for url in urls_in_msg:
            norm = normalize_url(url)
            if norm in blocked_links:
                limit = link_limits.get(norm, 0)
                if limit == 0:
                    return True
                count = blocked_links[norm]
                if count >= limit:
                    return True
                blocked_links[norm] = count + 1

    # FREE/PAID MODE FIX: duplicate_filter premium feature hai
    if _can_use_feature(user_id, "duplicate_filter") and is_duplicate(user_id, event, source_id):
        return True

    # FREE/PAID MODE FIX: product_duplicate premium feature hai
    if _can_use_feature(user_id, "duplicate_filter") and await check_product_duplicate(client, user_id, event):
        return True

    # ── NEW: Min/max message length filter ───────────────────────────────
    s = data.get("settings", {})
    min_len = s.get("min_msg_length", 0)
    max_len = s.get("max_msg_length", 0)
    if text:
        tlen = len(text.strip())
        if min_len > 0 and tlen < min_len:
            logger.debug(f"Skipped: msg too short ({tlen} < {min_len})")
            return True
        if max_len > 0 and tlen > max_len:
            logger.debug(f"Skipped: msg too long ({tlen} > {max_len})")
            return True

    # ── NEW: Require media filter ─────────────────────────────────────────
    if s.get("require_media") and not (event and event.media and
       not isinstance(event.media, MessageMediaWebPage)):  # FIX: top-level import use karo
        return True

    # ── NEW: Forward count limit per source ──────────────────────────────
    count_limit = s.get("fwd_count_limit", 0)
    if count_limit > 0:
        src_counts = data.setdefault("src_fwd_counts", {})
        src_key    = str(source_id)
        current    = src_counts.get(src_key, 0)
        if current >= count_limit:
            logger.debug(f"Skipped: fwd_count_limit reached ({current}/{count_limit}) for src {source_id}")
            return True

    # ── v3: Mention Filter ────────────────────────────────────────────────
    mention_mode = s.get("mention_filter", "off")
    if mention_mode != "off" and text:
        import re as _re3
        has_mention = bool(_re3.search(r'@\w+', text))
        if mention_mode == "block_mentions" and has_mention:
            _record_blocked(user_id); return True
        if mention_mode == "require_mentions" and not has_mention:
            _record_blocked(user_id); return True

    # ── v3: Forward Origin Filter ─────────────────────────────────────────
    origin_mode = s.get("forward_origin_filter", "off")
    if origin_mode != "off" and event:
        is_fwd = bool(getattr(event, "forward", None))
        if origin_mode == "block_forwarded" and is_fwd:
            _record_blocked(user_id); return True
        if origin_mode == "only_forwarded" and not is_fwd:
            _record_blocked(user_id); return True

    # ── v3: Link Count Filter ─────────────────────────────────────────────
    min_links = s.get("min_links", 0)
    max_links = s.get("max_links", 0)
    if (min_links or max_links) and event:
        try:
            lcount = len(extract_all_urls(event))
            if min_links and lcount < min_links:
                _record_blocked(user_id); return True
            if max_links and lcount > max_links:
                _record_blocked(user_id); return True
        except Exception:
            pass

    # ── v3: Hashtag Filter ────────────────────────────────────────────────
    ht_req = s.get("hashtag_required", [])
    ht_blk = s.get("hashtag_blocked", [])
    min_ht  = s.get("min_hashtags", 0)
    max_ht  = s.get("max_hashtags", 0)
    if any([ht_req, ht_blk, min_ht, max_ht]) and text:
        import re as _re4
        tags = [t.lower() for t in _re4.findall(r'#\w+', text)]
        if ht_req and not any(h.lower() in tags for h in ht_req):
            _record_blocked(user_id); return True
        if ht_blk and any(h.lower() in tags for h in ht_blk):
            _record_blocked(user_id); return True
        if min_ht and len(tags) < min_ht:
            _record_blocked(user_id); return True
        if max_ht and len(tags) > max_ht:
            _record_blocked(user_id); return True

    # ── v3: Language Filter ───────────────────────────────────────────────
    lang_cfg = data.get("lang_filter", {})
    if lang_cfg.get("enabled") and lang_cfg.get("allowed") and text and len(text) > 10:
        try:
            import re as _re5
            lang_ranges = {
                "hi": r'[\u0900-\u097F]', "ar": r'[\u0600-\u06FF]',
                "ru": r'[\u0400-\u04FF]', "zh": r'[\u4E00-\u9FFF]',
                "ja": r'[\u3040-\u30FF]', "ko": r'[\uAC00-\uD7AF]',
            }
            detected = "en"
            for lang, pat in lang_ranges.items():
                if len(_re5.findall(pat, text)) / max(len(text), 1) >= 0.05:
                    detected = lang; break
            if detected not in lang_cfg["allowed"]:
                _record_blocked(user_id); return True
        except Exception:
            pass

    # ── v3: Content Quality Filter ────────────────────────────────────────
    qf = data.get("quality_filter", {})
    if qf.get("enabled") and event:
        try:
            import re as _re6
            score = 50
            tlen  = len(text) if text else 0
            if tlen == 0:   score += 10 if event.media else -20
            elif tlen < 10: score -= 10
            elif tlen < 300: score += 20
            else:            score += 30
            if event.media:  score += 15
            _spam_re = r'(join now|click here|buy now|limited offer|free money|pump|100x|guaranteed profit)'
            if text and _re6.search(_spam_re, text, _re6.IGNORECASE): score -= 25
            if text and len(text) > 10:
                cap_r = sum(1 for c in text if c.isupper()) / max(len(text), 1)
                if cap_r > 0.6: score -= 20
            if text and _re6.search(r'[!?]{3,}', text): score -= 10
            score = max(0, min(100, score))
            if score < qf.get("min_score", 30):
                _record_blocked(user_id); return True
        except Exception:
            pass

    # ── v3: Regex Filter ─────────────────────────────────────────────────
    rf_cfg = data.get("regex_filters", {})
    if rf_cfg.get("enabled") and rf_cfg.get("patterns") and text:
        try:
            import re as _re7
            flag_map = {"IGNORECASE": _re7.IGNORECASE, "MULTILINE": _re7.MULTILINE}
            flags = 0
            for f in rf_cfg.get("flags", "IGNORECASE").upper().split("|"):
                flags |= flag_map.get(f.strip(), 0)
            mode     = rf_cfg.get("mode", "blacklist")
            matched  = any(
                _re7.search(p, text, flags)
                for p in rf_cfg["patterns"]
                if p
            )
            if mode == "blacklist" and matched:
                _record_blocked(user_id); return True
            if mode == "whitelist" and not matched:
                _record_blocked(user_id); return True
        except Exception:
            pass

    # ── v3: Time-Based Filter ─────────────────────────────────────────────
    tf_cfg = data.get("time_filter", {})
    if tf_cfg.get("enabled") and tf_cfg.get("rules"):
        try:
            import datetime as _dt5
            try:
                from zoneinfo import ZoneInfo as _ZI
                _now = _dt5.datetime.now(_ZI(tf_cfg.get("timezone", "Asia/Kolkata")))
            except Exception:
                _now = _dt5.datetime.now()
            _nm = _now.hour * 60 + _now.minute
            for rule in tf_cfg["rules"]:
                rt = rule.get("type", "all")
                # Determine message type
                _mtype = "text"
                if event and event.media:
                    from telethon.tl.types import MessageMediaDocument as _MMD
                    _mtype = "file" if isinstance(event.media, _MMD) else "media"
                if rt not in ("all", _mtype): continue
                sh, sm = map(int, rule["start"].split(":"))
                eh, em = map(int, rule["end"].split(":"))
                if not (sh*60+sm <= _nm <= eh*60+em):
                    _record_blocked(user_id); return True
        except Exception:
            pass

    return False

def _safe_html_truncate(text: str, limit: int) -> str:
    """
    FIX 14: Safe HTML truncation — closes open tags, handles emoji surrogate pairs.
    Prevents Telegram ParseError from broken HTML or split emojis.
    """
    if not text or len(text) <= limit:
        return text
    import re as _re
    # Close any open HTML tags after truncation point
    truncated = text[:limit - 3]
    # Find unclosed tags and close them
    open_tags = _re.findall(r'<(\w+)(?:\s[^>]*)?>(?!.*</\1>)', truncated, _re.DOTALL)
    suffix = "..."
    # Close tags in reverse order
    for tag in reversed(open_tags):
        if tag.lower() in ("b", "i", "u", "s", "code", "pre", "tg-spoiler", "blockquote", "a"):
            suffix = f"</{tag}>" + suffix
    # Ensure we don't split a surrogate pair (emoji)
    # Walk backwards to find clean UTF-16 boundary
    raw = truncated.encode("utf-16-le", errors="surrogatepass")
    # Even number of bytes = clean boundary
    if len(raw) % 2 != 0:
        truncated = truncated[:-1]
    return truncated + suffix


def _get_media_size(event) -> int:
    """Media ka size bytes mein return karo. 0 agar pata na chale."""
    try:
        if event.document:
            return event.document.size or 0
        if event.video:
            return getattr(event.video, 'size', 0) or 0
        if event.audio:
            return getattr(event.audio, 'size', 0) or 0
        # Photos ka size thumbnail se estimate — actual download chhota hota hai
        if event.photo:
            sizes = getattr(event.photo, 'sizes', [])
            if sizes:
                last = sizes[-1]
                return getattr(last, 'size', 0) or 0
    except Exception:
        pass
    return 0


def _is_too_large_to_download(event, max_mb: int) -> bool:
    """True agar file download ke liye bahut badi hai."""
    size = _get_media_size(event)
    if size == 0:
        return False  # Unknown size — try karo
    return size > (max_mb * 1024 * 1024)


async def process_text_content(msg_text, data, src_rules, has_media, event=None, user_id=None):

    working_text = msg_text
    # Global settings → Src Config priority logic
    # Src Config ne agar "keep" rakha (default) → global setting override karti hai
    # Src Config ne explicitly "remove"/"replace" set kiya → wahi chalega
    _src_link_mode = src_rules.get("link_mode", "keep")
    if _src_link_mode == "keep":
        # Global override check karo
        if data["settings"].get("remove_links"):
            link_mode = "remove"
        else:
            link_mode = "keep"
    else:
        link_mode = _src_link_mode  # Src Config ki explicit setting

    replacements = data.get("replacements", {})
    source_replace = src_rules.get("replace_map", {})

    # ── TEXT → HTML CONVERSION ───────────────────────────────────────────
    # Full formatting preservation:
    # Bold, Italic, Underline, Strike, Mono/Code, Spoiler, Quote, TextUrl, Url
    # Algorithm: merge overlapping entity spans → build HTML tag stack
    if event and event.entities:
        # ⚡ Entity types already imported at module level as _METU, _MEU etc.

        def _entities_at(offset, length, entities):
            """offset..offset+length ke andar saari entities lo."""
            end = offset + length
            return [e for e in entities if e.offset <= offset and e.offset + e.length >= end]

        # Build a sorted list of (position, is_open, entity) events
        # We'll process char-by-char via "breakpoints"
        entities_sorted = sorted(event.entities, key=lambda e: (e.offset, -e.length))

        # Collect all breakpoints
        breakpoints = set([0, len(msg_text)])
        for e in entities_sorted:
            breakpoints.add(e.offset)
            breakpoints.add(e.offset + e.length)
        breakpoints = sorted(breakpoints)

        result_parts = []

        def _open_tag(ent):
            if isinstance(ent, _MEB):   return "<b>"
            if isinstance(ent, _MEI):   return "<i>"
            if isinstance(ent, _MEUN):  return "<u>"
            if isinstance(ent, _MES):   return "<s>"
            if isinstance(ent, _MEC):   return "<code>"
            if isinstance(ent, _MEP):   return f'<pre language="{html.escape(ent.language or "")}">' if hasattr(ent, "language") and ent.language else "<pre>"
            if isinstance(ent, _MESP):  return "<tg-spoiler>"
            if isinstance(ent, _MEBQ):  return "<blockquote>"
            return ""

        def _close_tag(ent):
            if isinstance(ent, _MEB):   return "</b>"
            if isinstance(ent, _MEI):   return "</i>"
            if isinstance(ent, _MEUN):  return "</u>"
            if isinstance(ent, _MES):   return "</s>"
            if isinstance(ent, _MEC):   return "</code>"
            if isinstance(ent, _MEP):   return "</pre>"
            if isinstance(ent, _MESP):  return "</tg-spoiler>"
            if isinstance(ent, _MEBQ):  return "</blockquote>"
            return ""

        for i in range(len(breakpoints) - 1):
            seg_start = breakpoints[i]
            seg_end   = breakpoints[i + 1]
            seg_text  = msg_text[seg_start:seg_end]
            if not seg_text:
                continue

            # Find which entities cover this entire segment
            covering = [e for e in entities_sorted
                        if e.offset <= seg_start and e.offset + e.length >= seg_end]

            # Separate link entities from formatting entities
            link_ents  = [e for e in covering if isinstance(e, (_METU, _MEU))]
            fmt_ents   = [e for e in covering
                          if isinstance(e, (_MEB, _MEI, _MEUN, _MES, _MEC, _MEP, _MESP, _MEBQ))]

            safe_seg = html.escape(seg_text)

            # Apply formatting tags (innermost first — shortest span)
            fmt_ents_sorted = sorted(fmt_ents, key=lambda e: e.length)
            for fe_ent in fmt_ents_sorted:
                ot = _open_tag(fe_ent)
                ct = _close_tag(fe_ent)
                if ot:
                    safe_seg = ot + safe_seg + ct

            # Apply link entity (outermost)
            if link_ents:
                le = link_ents[0]
                if isinstance(le, _METU):
                    actual_url = le.url
                    if link_mode == "remove":
                        pass  # just safe_seg, no link wrap
                    else:
                        if link_mode == "replace":
                            for old_r, new_r in source_replace.items():
                                if old_r in actual_url:
                                    actual_url = actual_url.replace(old_r, new_r)
                        safe_url = html.escape(actual_url, quote=True)
                        safe_seg = f'<a href="{safe_url}">{safe_seg}</a>'
                elif isinstance(le, _MEU):
                    # Plain URL entity (the URL itself is the text)
                    actual_url = seg_text
                    if link_mode == "remove":
                        pass
                    elif link_mode == "replace":
                        for old_r, new_r in source_replace.items():
                            if old_r in actual_url:
                                actual_url = actual_url.replace(old_r, new_r)
                        safe_url = html.escape(actual_url, quote=True)
                        safe_seg = f'<a href="{safe_url}">{html.escape(actual_url)}</a>'

            result_parts.append(safe_seg)

        working_text = "".join(result_parts)
    else:
        # No entities at all — escape everything
        working_text = html.escape(working_text)

    # Global Text Replacements
    if replacements:
        pattern = r'(<a [^>]+>|</a>|https?://\S+)'
        parts = re.split(pattern, working_text)
        for i in range(len(parts)):
            if not re.match(pattern, parts[i]):
                for old_word, new_word in replacements.items():
                    # BUG FIX: Word boundary replacement — partial matches block karo
                    # Pehle: "a" → "tttt" replace karne par "apply" → "ttttpply" hota tha
                    # Ab: sirf standalone "a" replace hoga, "apply" safe rahega
                    try:
                        escaped = re.escape(old_word)
                        # Word boundary: alphanumeric ke beech match nahi hoga
                        boundary_pattern = r'(?<![\w])'  + escaped + r'(?![\w])'
                        parts[i] = re.sub(boundary_pattern, new_word, parts[i])
                    except re.error:
                        # Invalid regex — fallback to exact replace
                        parts[i] = parts[i].replace(old_word, new_word)
        working_text = "".join(parts)

    urls = extract_all_urls(event) if event else []
    
    if link_mode == "remove":
        # BUG FIX: ROBUST_LINK_PATTERN mein '@' bhi tha — isliye remove_user OFF hone par bhi
        # @username hat jaata tha. Ab sirf URLs remove karo, @mentions nahi.
        # @username removal sirf tab hogi jab remove_user/remove_usernames ON ho (neeche check hai).
        _URL_ONLY_PATTERN = r'(?:https?://|www\.|t\.me/|telegram\.me/|telegram\.dog/)[\w\d_\-\./\?=&%#]+'
        working_text = re.sub(_URL_ONLY_PATTERN, '', working_text)
        # Also remove any remaining <a href> tags
        working_text = re.sub(r'<a [^>]+>.*?</a>', '', working_text)
    
    elif link_mode == "replace":
        for url in urls:
            for old_r, new_r in source_replace.items():
                if old_r in url:
                    working_text = working_text.replace(html.escape(url, quote=True), html.escape(new_r, quote=True))
                    working_text = working_text.replace(url, new_r)

    # BUG FIX: Preview mode ON hone par bhi auto_shorten kaam karega
    # Pehle: not _preview_mode condition thi → preview ON = shorten skip
    # User experience: dono features ek saath kaam karne chahiye
    # Telegram short URL ka preview nahi banata — lekin original URL ka preview
    # message mein already attached hota hai (MessageMediaWebPage as media)
    # Isliye: shorten karo TEXT mein, preview media alag se attach rahega
    _preview_mode = data["settings"].get("preview_mode", False)
    _has_existing_preview = isinstance(getattr(event, 'media', None), MessageMediaWebPage) if event else False
    if data["settings"].get("auto_shorten") and urls and link_mode != "remove" and _can_use_feature(user_id, "auto_shorten"):
        for url in urls:
            if len(url) > 30 and "t.me" not in url.lower():
                try:
                    short_url = await shorten_url_rotation(url, user_id=user_id)  # ✅ FIX: user_id passed for per-user circuit breaker
                    if short_url and short_url != url:
                        escaped_url   = html.escape(url, quote=True)
                        escaped_short = html.escape(short_url, quote=True)
                        # Case 1: URL already inside <a href="url">text</a> — sirf href badlo
                        if f'href="{escaped_url}"' in working_text:
                            working_text = working_text.replace(
                                f'href="{escaped_url}"',
                                f'href="{escaped_short}"'
                            )
                        elif f'href="{url}"' in working_text:
                            working_text = working_text.replace(
                                f'href="{url}"',
                                f'href="{escaped_short}"'
                            )
                        # Case 2: Raw URL in text — wrap in clickable <a href> tag
                        elif url in working_text:
                            clickable = f'<a href="{escaped_short}">{escaped_short}</a>'
                            working_text = working_text.replace(url, clickable)
                        elif escaped_url in working_text:
                            clickable = f'<a href="{escaped_short}">{escaped_short}</a>'
                            working_text = working_text.replace(escaped_url, clickable)
                except Exception as e:
                    logger.debug(f"URL shortening error: {e}")
                    continue

    for u_old, u_new in src_rules.get("username_map", {}).items():
        try:
            pattern = re.escape(u_old) + r"(?!\w)"
            working_text = re.sub(pattern, u_new, working_text, flags=re.IGNORECASE)
        except Exception as e:
            logger.debug(f"Username map regex error ({u_old}): {e}")
            working_text = working_text.replace(u_old, u_new)
        
    if src_rules.get("remove_usernames") or data["settings"].get("remove_user"):
        working_text = re.sub(r'@\w+', '', working_text)
        
    if src_rules.get("remove_hashtags"):
        working_text = re.sub(r'#\w+', '', working_text)
        
    if src_rules.get("added_hashtags"):
        working_text += "\n" + " ".join(src_rules["added_hashtags"])

    if not working_text and has_media:
        if src_rules.get("custom_caption"): return html.escape(src_rules["custom_caption"])
        return ""

    # 🚨 FIX 42: Prefix/Suffix Logic (Prevents Double Spam)
    final_text = ""
    
    # 1. Prefix (Source Specific > Global)
    prefix = src_rules.get("prefix")
    if prefix:
        final_text += html.escape(prefix) + "\n\n"
    elif data["settings"]["start_msg"]:
        final_text += html.escape(data["settings"]["start_msg"]) + "\n\n"

    # 2. Main Content
    if src_rules.get("custom_caption"):
        final_text += html.escape(src_rules["custom_caption"])
    else:
        # Raw URLs fix: agar working_text mein koi raw URL hai jo already <a href> mein nahi hai
        # to usse wrap karo — parse_mode='html' mein & wali URLs Telegram silently drop karta hai
        def _fix_raw_urls(text):
            # Already processed <a href> tags ko skip karo
            # Baaki jo raw URLs hain unhe safe banao
            result = []
            last = 0
            # HTML tag pattern
            tag_pat = re.compile(r'<a [^>]+>.*?</a>', re.DOTALL)
            # Raw URL pattern (http/https)
            url_pat = re.compile(r'https?://[^\s<>"]+')
            
            # First collect all <a> tag spans to skip
            protected = [(m.start(), m.end()) for m in tag_pat.finditer(text)]
            
            def _in_protected(pos):
                return any(s <= pos < e for s, e in protected)
            
            for m in url_pat.finditer(text):
                if _in_protected(m.start()):
                    continue  # Already inside <a href>, skip
                url = m.group()
                # Escape & in URL so HTML parse nahi fail ho
                safe = html.escape(url, quote=True)
                # Replace raw URL with escaped version (keep as clickable plain text)
                result.append((m.start(), m.end(), f'<a href="{safe}">{html.escape(url)}</a>'))
            
            if not result:
                return text
            
            # Apply replacements in reverse order
            out = text
            for start, end, replacement in reversed(result):
                out = out[:start] + replacement + out[end:]
            return out
        
        final_text += _fix_raw_urls(working_text)

    # 3. Suffix (Source Specific > Global)
    suffix = src_rules.get("suffix")
    if suffix:
        final_text += "\n\n" + html.escape(suffix)
    elif data["settings"]["end_msg"]:
        final_text += "\n\n" + html.escape(data["settings"]["end_msg"])

    # ── AFFILIATE LINK PROCESSING ───────────────────────
    # Amazon/Flipkart links mein affiliate tag add karo (free feature)
    if user_id is not None and final_text:
        try:
            from affiliate import apply_affiliate_to_message
            final_text = apply_affiliate_to_message(user_id, final_text)
        except Exception:
            pass

    # AI Rewrite — removed

    return final_text.strip()
