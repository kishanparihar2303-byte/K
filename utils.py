import time
import asyncio
import re
import hashlib
import urllib.parse as urlparse
from io import BytesIO
from collections import deque
import aiohttp
import gc
from telethon import utils
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

try:
    from PIL import Image
except ImportError:
    Image = None

from database import safety_db
from config import logger

# ══════════════════════════════════════════
# TIMEZONE HELPER
# Har jagah datetime.now() ki jagah user_now(uid) use karo
# ══════════════════════════════════════════
import datetime as _datetime_module

# Common timezones with display names
TIMEZONE_LIST = [
    ("Asia/Kolkata",      "🇮🇳 India (IST +5:30)"),
    ("Asia/Karachi",      "🇵🇰 Pakistan (PKT +5:00)"),
    ("Asia/Dhaka",        "🇧🇩 Bangladesh (BST +6:00)"),
    ("Asia/Kathmandu",    "🇳🇵 Nepal (NPT +5:45)"),
    ("Asia/Colombo",      "🇱🇰 Sri Lanka (SLST +5:30)"),
    ("Asia/Dubai",        "🇦🇪 UAE/Gulf (GST +4:00)"),
    ("Asia/Singapore",    "🇸🇬 Singapore (SGT +8:00)"),
    ("Asia/Bangkok",      "🇹🇭 Thailand (ICT +7:00)"),
    ("Asia/Kuala_Lumpur", "🇲🇾 Malaysia (MYT +8:00)"),
    ("Asia/Jakarta",      "🇮🇩 Indonesia (WIB +7:00)"),
    ("Asia/Riyadh",       "🇸🇦 Saudi Arabia (AST +3:00)"),
    ("Europe/London",     "🇬🇧 UK (GMT/BST)"),
    ("Europe/Berlin",     "🇩🇪 Germany (CET +1:00)"),
    ("Europe/Moscow",     "🇷🇺 Russia (MSK +3:00)"),
    ("US/Eastern",        "🇺🇸 USA East (EST -5:00)"),
    ("US/Pacific",        "🇺🇸 USA West (PST -8:00)"),
    ("Australia/Sydney",  "🇦🇺 Australia (AEST +10:00)"),
    ("Africa/Nairobi",    "🇰🇪 East Africa (EAT +3:00)"),
    ("UTC",               "🌐 UTC (Universal)"),
]

def _get_user_tz(user_id):
    """User ki timezone object return karo (pytz). Default: Asia/Kolkata."""
    try:
        import pytz
        from database import get_user_data
        data = get_user_data(user_id)
        tz_name = (
            data.get("timezone")
            or data.get("scheduler", {}).get("timezone")
            or "Asia/Kolkata"
        )
        try:
            return pytz.timezone(tz_name)
        except Exception:
            return pytz.timezone("Asia/Kolkata")
    except Exception:
        return _datetime_module.timezone(_datetime_module.timedelta(hours=5, minutes=30))


def user_now(user_id=None) -> _datetime_module.datetime:
    """
    User ki timezone mein current datetime return karo.
    user_id=None ho to Asia/Kolkata (IST) use karo.
    """
    try:
        import pytz
        if user_id is None:
            tz = pytz.timezone("Asia/Kolkata")
        else:
            tz = _get_user_tz(user_id)
        return _datetime_module.datetime.now(tz)
    except Exception:
        # Fallback: IST manually (+5:30)
        utc = _datetime_module.datetime.now(_datetime_module.timezone.utc)
        ist = utc + _datetime_module.timedelta(hours=5, minutes=30)
        return ist


def user_today_key(user_id=None) -> str:
    """User ki timezone mein aaj ki date key: YYYY-MM-DD"""
    return user_now(user_id).strftime("%Y-%m-%d")


def format_user_time(user_id=None, fmt="%d/%m/%Y %I:%M %p") -> str:
    """User ki timezone mein formatted current time string."""
    return user_now(user_id).strftime(fmt)


def format_ts(timestamp: float, user_id=None, fmt="%d/%m/%Y %I:%M %p") -> str:
    """Unix timestamp ko user timezone mein convert karke format karo."""
    try:
        import pytz
        tz = _get_user_tz(user_id) if user_id else pytz.timezone("Asia/Kolkata")
        dt = _datetime_module.datetime.fromtimestamp(timestamp, tz)
        return dt.strftime(fmt)
    except Exception:
        utc = _datetime_module.datetime.utcfromtimestamp(timestamp)
        ist = utc + _datetime_module.timedelta(hours=5, minutes=30)
        return ist.strftime(fmt)


# ==========================================
# RAM SAVER: SAFE HASHING
# ==========================================

async def get_safe_image_hash(client, event):
    if not event.photo:
        return None
    thumb_data = BytesIO()
    try:
        downloaded = await client.download_media(event.photo, file=thumb_data, thumb=-1)
        if not downloaded:
            return None
        val = generate_perceptual_hash(thumb_data.getvalue())
        return val
    except Exception as e:
        logger.error(f"Hash Error: {e}")
        return None
    finally:
        thumb_data.close()
        del thumb_data
        gc.collect()

def generate_perceptual_hash(img_data):
    if not Image:
        return None
    try:
        img = Image.open(BytesIO(img_data)).convert('L').resize((9, 8), Image.Resampling.LANCZOS)
        pixels = list(img.getdata())
        diff = []
        for row in range(8):
            for col in range(8):
                diff.append(pixels[row * 9 + col] > pixels[row * 9 + col + 1])
        img.close()
        return hashlib.md5(str(diff).encode()).hexdigest()
    except Exception:
        return None

# ==========================================
# SAFETY & SMART DELAY
# ==========================================

def get_safety_data(user_id):
    if user_id not in safety_db:
        safety_db[user_id] = {
            "msg_timestamps": deque(maxlen=30),
            "flood_delay": 0,
            "last_flood_time": 0
        }
    return safety_db[user_id]

# ── Token bucket state for smart delay ────────────────────────────────────────
# Per-user token bucket: refill_rate tokens/sec, burst_cap max tokens
# When tokens run out → apply delay. Zero-overhead when tokens available.
_BUCKET: dict = {}   # {user_id: [tokens, last_refill_ts]}
_BUCKET_REFILL  = 3.0    # tokens per second (safer: 3 msgs/sec sustained)
_BUCKET_BURST   = 8      # max burst (8 rapid messages before throttle kicks in)
_BUCKET_MIN_TOK = 0.5    # when tokens fall below this → apply delay

# ── Adaptive channel volume tracking ─────────────────────────────────────────
# Track per-user message rate — if channel is "busy" (>5 msg/min), apply
# extra safety delay to avoid Telegram rate-limiting / account restrictions.
_RATE_WINDOW = 60        # seconds window for rate tracking
_MSG_TIMES: dict = {}    # {user_id: deque of timestamps}
_EXTRA_DELAY_THRESHOLD = 5   # msgs/min above which extra safety delay kicks in
_EXTRA_DELAY_SEC = 1.5       # extra delay when channel is very active

def _get_msg_rate(user_id: int) -> float:
    """Returns messages/min for this user in the last 60 seconds."""
    dq = _MSG_TIMES.get(user_id)
    if not dq:
        return 0.0
    now = time.time()
    cutoff = now - _RATE_WINDOW
    # Remove old timestamps
    while dq and dq[0] < cutoff:
        dq.popleft()
    return len(dq) * (60.0 / _RATE_WINDOW)

def _record_msg_time(user_id: int):
    """Record a message timestamp for rate tracking."""
    from collections import deque as _deque
    if user_id not in _MSG_TIMES:
        _MSG_TIMES[user_id] = _deque(maxlen=200)
    _MSG_TIMES[user_id].append(time.time())


async def apply_smart_delay(user_id):
    """
    ⚡ Adaptive Smart Delay System v2 — Telegram-safe, auto-adjusting.

    Algorithm:
      - Token bucket: burst 8 msgs, sustained 3 msg/sec
      - Adaptive layer: if channel is sending >5 msgs/min, extra 1.5s safety delay
      - FloodWait history: if recent FloodWait, apply extra backoff
      - Result: safe forwarding that adapts to channel volume automatically

    Why this matters:
      - Prevents Telegram account restrictions from bulk forwarding
      - Adapts automatically — busy channels slow down, quiet channels go fast
      - No user config needed — works automatically
    """
    _record_msg_time(user_id)
    now = time.time()
    bucket = _BUCKET.get(user_id)
    if bucket is None:
        _BUCKET[user_id] = [float(_BUCKET_BURST), now]
        return   # First message: full bucket, instant

    tokens, last_refill = bucket
    elapsed = now - last_refill
    tokens = min(_BUCKET_BURST, tokens + elapsed * _BUCKET_REFILL)
    tokens -= 1.0

    if tokens >= _BUCKET_MIN_TOK:
        _BUCKET[user_id] = [tokens, now]
        # ── Adaptive layer: busy channel check ───────────────────────────────
        rate = _get_msg_rate(user_id)
        if rate > _EXTRA_DELAY_THRESHOLD:
            # Channel is very active — add small safety buffer
            extra = min(_EXTRA_DELAY_SEC, (rate - _EXTRA_DELAY_THRESHOLD) * 0.2)
            if extra > 0.1:
                await asyncio.sleep(extra)
        return

    # Not enough tokens → sleep just until we have enough
    deficit = _BUCKET_MIN_TOK - tokens
    sleep_sec = min(deficit / _BUCKET_REFILL, 3.0)   # cap at 3s (was 2s)
    _BUCKET[user_id] = [_BUCKET_MIN_TOK, now]
    await asyncio.sleep(sleep_sec)

    # ── Legacy safety_db timestamp tracking (kept for backward compat) ──
    s_data = get_safety_data(user_id)
    s_data["msg_timestamps"].append(time.time())


# ==========================================
# SMART ENTITY RESOLVER
# ==========================================



def normalize_channel_id(val) -> str:
    """
    Channel ID ko consistent format mein convert karo for comparison.
    Handles: int, "-100xxx", "xxx", "@username", "t.me/username"
    Returns: "-100XXXXXXXX" for numeric IDs, "@username" for usernames
    """
    if val is None:
        return ""
    s = str(val).strip()
    # Remove t.me prefix
    if "t.me/" in s:
        s = s.split("t.me/")[-1].split("?")[0].split("/")[0]
    # If it's a pure number (possibly with -100)
    clean = s.lstrip("-")
    if clean.isdigit():
        n = int(clean)
        # Normalize to -100XXXXXXX format for supergroups/channels
        if s.startswith("-100"):
            return s  # already normalized
        elif s.startswith("-"):
            return f"-100{clean}"  # group id → -100 prefix
        else:
            return f"-100{clean}"  # raw id → -100 prefix
    # Username
    if s.startswith("@"):
        return s.lower()
    if not s.startswith("+") and not "/" in s:
        return f"@{s.lower()}"
    return s.lower()


def sources_match(a, b) -> bool:
    """
    Check if two channel references refer to the same channel.
    Handles: numeric IDs (with/without -100), @usernames, invite links.
    """
    if a is None or b is None:
        return False
    na, nb = normalize_channel_id(a), normalize_channel_id(b)
    if na == nb:
        return True
    # ✅ FIX: lstrip("-100") is WRONG — it strips individual chars, not prefix.
    # Correct approach: removeprefix / startswith check.
    def _get_num(s):
        if s.startswith("-100"):
            return s[4:]
        if s.startswith("-"):
            return s[1:]
        return s
    na_num = _get_num(na)
    nb_num = _get_num(nb)
    if na_num.isdigit() and nb_num.isdigit() and na_num == nb_num:
        return True
    return False


def channel_already_exists(val, target_list: list, channel_names_id: dict = None) -> bool:
    """
    Comprehensive duplicate check for a channel value against a list.

    Handles ALL storage formats:
      - Numeric ID  ↔  Numeric ID  (with/without -100 prefix)
      - Invite link ↔  Invite link (same hash)
      - Numeric ID  ↔  Invite link (via channel_names_id reverse-lookup cache)

    Args:
        val               : The new channel value to check (numeric ID, link, @username)
        target_list       : Existing sources or destinations list
        channel_names_id  : data.get("channel_names_id", {}) — maps invite-link → numeric-ID
    Returns:
        True if the channel is already in the list.
    """
    if not target_list:
        return False

    val_str = str(val).strip()

    # --- Pass 1: direct sources_match against every stored entry ---
    for entry in target_list:
        if sources_match(val_str, str(entry)):
            return True

    # --- Pass 2: reverse-lookup via channel_names_id cache ---
    # channel_names_id = { "+HashXXX": "-1001234567890", ... }
    if channel_names_id:
        # A) val is numeric → see if any invite-link in the list resolves to same ID
        val_norm = normalize_channel_id(val_str)

        def _num_part(s):
            s = str(s)
            if s.startswith("-100"):
                return s[4:]
            if s.startswith("-"):
                return s[1:]
            return s if s.isdigit() else ""

        val_num = _num_part(val_norm)

        for link, cached_id in channel_names_id.items():
            cached_num = _num_part(str(cached_id))
            # Does this cached mapping relate to our val?
            ids_match = (
                (val_num and cached_num and val_num == cached_num)
                or sources_match(val_str, str(cached_id))
            )
            if ids_match:
                # Check if this invite link is in target_list
                for entry in target_list:
                    if sources_match(link, str(entry)) or sources_match(str(cached_id), str(entry)):
                        return True

        # B) val is an invite link → see if its resolved ID matches any numeric entry
        if val_str in channel_names_id:
            resolved = str(channel_names_id[val_str])
            for entry in target_list:
                if sources_match(resolved, str(entry)):
                    return True

    return False

async def resolve_id(client, text):
    """
    Channel reference (link / @username / numeric ID) ko numeric ID mein resolve karo.

    ✅ FIX: Private invite links ke liye UserAlreadyParticipantError properly handle hoti hai.
         CheckChatInviteRequest se bina join kiye hi channel ID milti hai.
    """
    if not text:
        return text
    text = str(text).strip()
    if " " in text and "http" not in text:
        return text
    try:
        # --- t.me/c/XXXXXXX/MSG format (private channel link) ---
        if "t.me/c/" in text:
            parts = text.split('/')
            cid = parts[parts.index('c') + 1]
            return f"-100{cid}"

        # --- t.me/... link ---
        elif "t.me/" in text:
            slug = text.split('t.me/')[-1].split('/')[0].strip('/')

            if slug.startswith('+') or text.split('/')[-1].startswith('+'):
                # Invite link (private channel)
                hash_only = slug.lstrip('+')

                # Step 1: Try joining (works when not already a member)
                try:
                    from telethon.tl.functions.messages import ImportChatInviteRequest
                    result = await client(ImportChatInviteRequest(hash_only))
                    chat = result.chats[0]
                    return str(utils.get_peer_id(chat))
                except Exception as join_err:
                    err_str = str(join_err).lower()

                    # ✅ FIX: Already member — use CheckChatInviteRequest to get info without joining
                    if "already" in err_str or "participant" in err_str:
                        try:
                            from telethon.tl.functions.messages import CheckChatInviteRequest
                            invite_info = await client(CheckChatInviteRequest(hash_only))
                            # CheckChatInviteRequest returns ChatInvite or ChatInviteAlready
                            chat_obj = getattr(invite_info, 'chat', None)
                            if chat_obj:
                                peer_id = utils.get_peer_id(chat_obj)
                                return str(peer_id)
                        except Exception:
                            pass

                    # Step 2: get_entity fallback (works for some cases)
                    try:
                        ent = await client.get_entity(f"+{hash_only}")
                        return str(utils.get_peer_id(ent))
                    except Exception:
                        pass

                    # Step 3: Scan dialogs — already-joined channel dhundho
                    try:
                        async for dialog in client.iter_dialogs():
                            inv = getattr(dialog.entity, 'username', None)
                            # Match by checking if this dialog was joined via invite
                            # We can't match exactly, but numeric access via get_entity might work
                            pass
                    except Exception:
                        pass

                # ✅ If all resolution failed — return raw hash (forward_engine will cache-resolve later)
                return f"+{hash_only}"

            else:
                # Public channel @username or t.me/username
                target = f"@{slug}" if not slug.startswith('@') else slug
                ent = await client.get_entity(target)
                return str(utils.get_peer_id(ent))

        # --- Pure numeric ID ---
        if text.lstrip('-').isdigit():
            target = int(text)
            ent = await client.get_entity(target)
            return str(utils.get_peer_id(ent))

        # --- @username ---
        target = text if text.startswith('@') else f"@{text}"
        ent = await client.get_entity(target)
        return str(utils.get_peer_id(ent))

    except Exception as e:
        logger.debug(f"Resolve ID fallback for '{text}': {e}")
        # Safe fallbacks — don't lose the original value
        if str(text).lstrip('-').isdigit():
            return text
        if not text.startswith('@') and not text.startswith('-') \
                and 't.me' not in text and ' ' not in text \
                and not text.startswith('+'):
            return f"@{text}"
        return text

async def resolve_id_and_name(client, text):
    """
    ✅ SMART: Link/username/ID ko resolve karo — ID aur Channel Name dono ek saath return karo.
    
    Purana resolve_id() sirf ID deta tha — name discard ho jaata tha.
    Unknown/private channels mein baad mein get_entity() fail karta tha → link dikhti thi.
    
    Ab: entity ek baar fetch hoti hai → ID + name dono capture → cache mein store.
    User ko hamesha proper name dikhega, chahe channel unknown ho.
    
    Returns: (resolved_id_str, channel_name_or_None)
    """
    from telethon import utils as tl_utils
    text = str(text).strip()
    name = None

    def _extract_name(ent):
        return (getattr(ent, "title", None)
                or getattr(ent, "first_name", None)
                or getattr(ent, "username", None))

    try:
        # ── t.me/c/XXXXXXX/MSG — private channel direct link ──────────
        if "t.me/c/" in text:
            parts = text.split('/')
            cid = parts[parts.index('c') + 1]
            resolved = f"-100{cid}"
            try:
                ent = await client.get_entity(int(resolved))
                name = _extract_name(ent)
            except Exception:
                pass
            return resolved, name

        # ── t.me/... link ───────────────────────────────────────────────
        elif "t.me/" in text:
            slug = text.split('t.me/')[-1].split('/')[0].strip('/')

            if slug.startswith('+'):
                # Private invite link
                hash_only = slug.lstrip('+')
                try:
                    from telethon.tl.functions.messages import ImportChatInviteRequest
                    result = await client(ImportChatInviteRequest(hash_only))
                    chat = result.chats[0]
                    name = _extract_name(chat)
                    return str(tl_utils.get_peer_id(chat)), name
                except Exception as join_err:
                    err_str = str(join_err).lower()
                    if "already" in err_str or "participant" in err_str:
                        # Pehle se member — CheckChatInviteRequest se info lo
                        try:
                            from telethon.tl.functions.messages import CheckChatInviteRequest
                            inv = await client(CheckChatInviteRequest(hash_only))
                            chat = getattr(inv, 'chat', None)
                            if chat:
                                name = _extract_name(chat)
                                return str(tl_utils.get_peer_id(chat)), name
                        except Exception:
                            pass
                    # get_entity fallback
                    try:
                        ent = await client.get_entity(f"+{hash_only}")
                        name = _extract_name(ent)
                        return str(tl_utils.get_peer_id(ent)), name
                    except Exception:
                        pass
                # Sab fail — raw hash return karo, name nahi mila
                return f"+{hash_only}", None

            else:
                # Public channel: t.me/username
                target = f"@{slug}" if not slug.startswith('@') else slug
                ent = await client.get_entity(target)
                name = _extract_name(ent) or target
                return str(tl_utils.get_peer_id(ent)), name

        # ── Pure numeric ID ─────────────────────────────────────────────
        if text.lstrip('-').isdigit():
            try:
                ent = await client.get_entity(int(text))
                name = _extract_name(ent)
                return str(tl_utils.get_peer_id(ent)), name
            except Exception:
                return text, None

        # ── @username ───────────────────────────────────────────────────
        target = text if text.startswith('@') else f"@{text}"
        ent = await client.get_entity(target)
        name = _extract_name(ent) or target
        return str(tl_utils.get_peer_id(ent)), name

    except Exception as e:
        logger.debug(f"resolve_id_and_name fallback '{text}': {e}")
        # Fallback: public link se username extract karo — naam jaisa kuch to dikhao
        if "t.me/" in text and "+" not in text:
            slug = text.split("t.me/")[-1].split("/")[0].strip("/")
            name = f"@{slug}"
        return text, name


# ⚡ GLOBAL DISPLAY NAME CACHE (TTL=10 min, max 2000 entries)
# Telegram API entity lookup bohot slow hoti hai — ek baar fetch
# karo, 10 min tak RAM mein raho. Bot buttons fast dikhenge.
# ══════════════════════════════════════════════════════
import time as _time_mod
from telethon.tl.functions.messages import CheckChatInviteRequest
from telethon.tl.functions.messages import ImportChatInviteRequest

_DISPLAY_CACHE: dict = {}        # {chat_id_str: (name, expire_ts)}
_DISPLAY_CACHE_TTL  = 600        # 10 minutes
_DISPLAY_CACHE_MAX  = 2000       # Max entries before LRU trim

def _display_cache_get(key: str):
    """Cached name return karo. Miss ya expired → None."""
    entry = _DISPLAY_CACHE.get(key)
    if entry and _time_mod.time() < entry[1]:
        return entry[0]
    _DISPLAY_CACHE.pop(key, None)
    return None

def _display_cache_set(key: str, name: str):
    """Name cache mein daalo with expiry."""
    if len(_DISPLAY_CACHE) >= _DISPLAY_CACHE_MAX:
        # LRU-style: oldest 20% hata do
        trim = list(_DISPLAY_CACHE.keys())[:_DISPLAY_CACHE_MAX // 5]
        for k in trim:
            _DISPLAY_CACHE.pop(k, None)
    _DISPLAY_CACHE[key] = (name, _time_mod.time() + _DISPLAY_CACHE_TTL)


async def get_display_name(client, chat_id, user_id=None):
    """
    Channel/user ka display naam return karo.
    ⚡ 3-layer cache:
       Layer 1 — Global in-memory TTL cache (fastest, 10 min)
       Layer 2 — Per-user DB channel_names dict
       Layer 3 — Telegram API get_entity() (slowest, only on cache miss)
    """
    key = str(chat_id)

    # ── Layer 1: Global RAM cache ─────────────────────────────
    cached_global = _display_cache_get(key)
    if cached_global:
        return cached_global

    # ── Layer 2: Per-user DB cache ────────────────────────────
    if user_id is not None:
        try:
            from database import get_user_data
            cached_db = get_user_data(user_id).get("channel_names", {}).get(key)
            if cached_db:
                _display_cache_set(key, cached_db)   # Promote to global cache
                return cached_db
        except Exception:
            pass

    # ── Layer 3: Telegram API (only on full cache miss) ───────
    name = None
    try:
        s = str(chat_id)
        if s.startswith("-100") or s.lstrip("-").isdigit():
            entity = await client.get_entity(int(chat_id))
        else:
            entity = await client.get_entity(chat_id)

        name = (getattr(entity, "title", None)
                or (f"@{entity.username}" if getattr(entity, "username", None) else None)
                or getattr(entity, "first_name", None))
    except Exception:
        pass

    if name:
        # Write-through to both caches
        _display_cache_set(key, name)
        if user_id is not None:
            try:
                from database import get_user_data
                udata = get_user_data(user_id)
                names = udata.setdefault("channel_names", {})
                names[key] = name
                if len(names) > 300:
                    to_del = list(names.keys())[:-200]
                    for k in to_del:
                        del names[k]
            except Exception:
                pass
        return name

    # Fallback
    if "t.me/+" in key or "t.me/joinchat" in key or key.startswith("+"):
        return "🔒 Private Channel"

    return key

# ==========================================
# HELPERS
# ==========================================

ROBUST_LINK_PATTERN = r'(?:https?://|www\.|t\.me/|telegram\.me/|telegram\.dog/|@)[\w\d_\-\./\?=&%#]+'


def safe_split_data(data_bytes: bytes, sep: str = "_", index: int = -1, default=None):
    """
    Safe callback data split — prevents IndexError on stale/invalid buttons.
    Use instead of event.data.decode().split("_")[-1] everywhere.
    """
    try:
        parts = data_bytes.decode("utf-8", errors="replace").split(sep)
        return parts[index]
    except (IndexError, ValueError, AttributeError):
        return default


def safe_int(val, default: int = 0) -> int:
    """Safe int conversion — prevents ValueError on invalid callback data."""
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def extract_all_urls(event):
    urls = set()
    text = event.raw_text or ""
    found_plaintext = re.findall(ROBUST_LINK_PATTERN, text)
    for u in found_plaintext:
        clean_u = u.strip(".,!?) ")
        urls.add(clean_u)
    if event.entities:
        for ent in event.entities:
            if isinstance(ent, MessageEntityTextUrl):
                urls.add(ent.url)
            elif isinstance(ent, MessageEntityUrl):
                url_val = text[ent.offset:ent.offset+ent.length]
                urls.add(url_val)
    return list(urls)

# BUG 26 FIX: timeout=5 integer deprecated → ClientTimeout
async def resolve_url_robust(url):
    try:
        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.head(url, allow_redirects=True) as r:
                return str(r.url)
    except Exception:
        return url

def normalize_url(url):
    url = re.sub(r'https?://(www\.)?', '', url.strip().lower())
    return url.split('?')[0].split('#')[0].rstrip('/')

# Short link → real URL map (in-memory)
_UNSHORTEN_CACHE: dict = {}
_UNSHORTEN_CACHE_MAX = 500  # Max URLs to cache (memory limit)

async def _unshorten_async(url: str) -> str:
    """Follow redirects asynchronously. Returns original on failure."""
    if url in _UNSHORTEN_CACHE:
        return _UNSHORTEN_CACHE[url]
    try:
        timeout = aiohttp.ClientTimeout(total=4)
        async with aiohttp.ClientSession(timeout=timeout) as sess:
            async with sess.head(url, allow_redirects=True,
                                 headers={"User-Agent": "Mozilla/5.0"}) as resp:
                final = str(resp.url)
                _UNSHORTEN_CACHE[url] = final
                return final
    except Exception:
        try:
            # Fallback: GET request (some servers reject HEAD)
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4)) as sess:
                async with sess.get(url, allow_redirects=True,
                                    headers={"User-Agent": "Mozilla/5.0"}) as resp:
                    final = str(resp.url)
                    _UNSHORTEN_CACHE[url] = final
                    return final
        except Exception:
            _UNSHORTEN_CACHE[url] = url
            return url

def _unshorten_sync(url: str) -> str:
    """Sync fallback — tries urllib. Use _unshorten_async when possible."""
    if url in _UNSHORTEN_CACHE:
        return _UNSHORTEN_CACHE[url]
    try:
        import urllib.request
        req = urllib.request.Request(url, method="HEAD")
        req.add_header("User-Agent", "Mozilla/5.0")
        with urllib.request.urlopen(req, timeout=3) as r:
            final = r.url
            # Trim cache if too large
            if len(_UNSHORTEN_CACHE) > _UNSHORTEN_CACHE_MAX:
                # Remove 20% oldest (dict insertion order)
                to_del = list(_UNSHORTEN_CACHE.keys())[:_UNSHORTEN_CACHE_MAX // 5]
                for k in to_del:
                    del _UNSHORTEN_CACHE[k]
            _UNSHORTEN_CACHE[url] = final
            return final
    except Exception:
        _UNSHORTEN_CACHE[url] = url
        return url


def _normalize_amazon_url(url: str) -> str:
    """
    Amazon URL ko normalize karo — affiliate tags, session params hata do.
    Sirf ASIN/product path rakho.
    Example: amazon.in/dp/B08XYZ?tag=old-21&ref=... → amazon.in/dp/B08XYZ
    """
    try:
        import urllib.parse as _up
        parsed = _up.urlparse(url)
        # Only keep clean path
        clean_path = parsed.path.rstrip("/")
        return f"{parsed.scheme}://{parsed.netloc}{clean_path}"
    except Exception:
        return url


# ── Product-domain whitelist — only these domains are "products" ─────────────
# Non-product URLs (t.me, news sites, etc.) must NEVER trigger product filter.
# BUG FIX: Old code stored hashes for ALL URLs including t.me channel links,
# causing every subsequent message from same channel to be marked as duplicate.
_PRODUCT_DOMAINS = (
    "amazon.in", "amazon.com", "amazon.co", "amzn.in", "amzn.to",
    "flipkart.com", "fkrt.it", "fk.io",
    "meesho.com", "myntra.com", "myntr.in", "myntr.it",  # FIX: myntra short links added
    "ajio.com", "nykaa.com",
    "snapdeal.com", "tatacliq.com", "reliancedigital.com",
    "paytmmall.com", "shopsy.in", "jiomart.com",
    "indiamart.com", "industrybuying.com", "moglix.com",
)

# ── Short-link domains → always resolve before ID extraction ─────────────────
_SHORT_LINK_DOMAINS = (
    "amzn.to", "amzn.in", "fkrt.it", "bit.ly", "tinyurl", "t.co",
    "tiny.cc", "rb.gy", "cutt.ly", "shorturl", "ow.ly", "s.click",
    "clnk.in", "dl.flipkart", "shrsl.com", "myntr.in", "myntr.it",
    "linkin.bio", "go.flipkart", "fk.io", "nyka.in", "ajio.to",
    "meesho.page.link", "shp.ee",
)

def _is_short_link(url: str) -> bool:
    """
    Short link detect karo — 2 tarike se:
    1. Domain known short-link service hai
    2. Path bahut chhota hai (≤ 12 chars, sirf alphanumeric/dash) — generic short link pattern
    """
    u_lower = url.lower()
    if any(d in u_lower for d in _SHORT_LINK_DOMAINS):
        return True
    try:
        import urllib.parse as _sup
        path_parts = [p for p in _sup.urlparse(url).path.split("/") if p]
        if path_parts and len(path_parts) == 1 and len(path_parts[0]) <= 12:
            if re.match(r'^[A-Za-z0-9_\-]+$', path_parts[0]):
                return True
    except Exception:
        pass
    return False

def _is_product_url(url: str) -> bool:
    """Return True only if URL is from a known shopping/product domain."""
    u_lower = url.lower()
    return any(d in u_lower for d in _PRODUCT_DOMAINS)


def _strip_tracking_params(url: str) -> str:
    """
    URL se sab tracking/affiliate query params strip karo.
    Sirf domain + path rakho — product ID path mein hota hai, query mein nahi
    (Amazon ASIN bhi path mein hota hai, query mein nahi).
    """
    _TRACKING_PARAMS = {
        # Amazon
        "tag", "ref", "ref_", "linkCode", "linkId", "camp", "creative",
        "creativeASIN", "ascsubtag", "th", "psc", "smid", "sprefix",
        # Flipkart
        "affid", "affExtParam1", "affExtParam2", "otracker", "otracker1",
        "fm", "iid", "ppt", "ppn", "ssid", "qH",
        # Common
        "utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term",
        "fbclid", "gclid", "msclkid", "dclid", "zanpid", "clickref",
        "source", "s", "afftrack",
    }
    try:
        import urllib.parse as _tp
        parsed = _tp.urlparse(url)
        qs = _tp.parse_qs(parsed.query, keep_blank_values=False)
        # sirf non-tracking params rakho (product-specific like pid, itemId, etc.)
        clean_qs = {k: v for k, v in qs.items() if k.lower() not in _TRACKING_PARAMS}
        # Rebuild URL: path (tracking-param-free query agar bacha to rakho)
        new_query = _tp.urlencode(clean_qs, doseq=True)
        rebuilt = _tp.urlunparse((
            parsed.scheme, parsed.netloc, parsed.path,
            parsed.params, new_query, ""  # fragment strip
        ))
        return rebuilt
    except Exception:
        return url


def _extract_product_id_from_url(url: str):
    """
    Product URL se canonical ID extract karo.
    - Affiliate/tracking params ignore
    - Kisi bhi variation mein same product → same ID
    Returns None if not a known product domain.
    """
    if not _is_product_url(url):
        return None

    # Tracking params pehle strip karo
    url = _strip_tracking_params(url)
    u_lower = url.lower()
    import urllib.parse as _up

    # ── Amazon / amzn ────────────────────────────────────────────────────────
    if "amazon" in u_lower or "amzn" in u_lower:
        for pat in [
            r"/(?:dp|gp/product|product-reviews|exec/obidos/ASIN)/([A-Z0-9]{10})",
            r"[?&](?:ASIN|asin)=([A-Z0-9]{10})",
            r"/([A-Z0-9]{10})(?:[/?&]|$)",
        ]:
            m = re.search(pat, url, re.IGNORECASE)
            if m:
                return f"amz_{m.group(1).upper()}"
        # Fallback: path-based stable hash (short amzn.in link jis ko resolve nahi kar sake)
        try:
            parsed = _up.urlparse(url)
            stable = parsed.path.rstrip("/").lower()
            return f"amz_p_{hashlib.md5(stable.encode()).hexdigest()[:12]}"
        except Exception:
            pass

    # ── Flipkart ─────────────────────────────────────────────────────────────
    if "flipkart" in u_lower or "fkrt" in u_lower or "fk.io" in u_lower:
        try:
            parsed = _up.urlparse(url)
            p = _up.parse_qs(parsed.query)
            if "pid" in p:
                return f"fk_{p['pid'][0]}"
            m = re.search(r"/p/(itm[a-z0-9]+)", url, re.IGNORECASE)
            if m:
                return f"fk_{m.group(1)}"
            m2 = re.search(r"/([0-9A-Z]{16,20})(?:[/?]|$)", url, re.IGNORECASE)
            if m2:
                return f"fk_{m2.group(1)}"
            stable = parsed.path.rstrip("/").lower()
            return f"fk_p_{hashlib.md5(stable.encode()).hexdigest()[:12]}"
        except Exception:
            pass

    # ── Meesho ───────────────────────────────────────────────────────────────
    if "meesho" in u_lower:
        m = re.search(r"/product/(\d+)", url, re.IGNORECASE)
        if m:
            return f"ms_{m.group(1)}"
        m2 = re.search(r"/(\d{6,})(?:[/?]|$)", url, re.IGNORECASE)
        if m2:
            return f"ms_{m2.group(1)}"

    # ── Myntra (myntra.com + myntr.in / myntr.it short links) ────────────────
    if "myntra" in u_lower or "myntr." in u_lower:
        # Standard: /buy/brand/slug/PRODUCT_ID/buy
        m = re.search(r"/buy/[^/]+/(\d{6,})", url, re.IGNORECASE)
        if m:
            return f"myn_{m.group(1)}"
        # Any long numeric segment in path
        m = re.search(r"/(\d{6,})(?:[/?]|$)", url, re.IGNORECASE)
        if m:
            return f"myn_{m.group(1)}"
        # Short link fallback (if not resolved)
        try:
            stable = _up.urlparse(url).path.rstrip("/").lower()
            return f"myn_p_{hashlib.md5(stable.encode()).hexdigest()[:12]}"
        except Exception:
            pass

    # ── Ajio ─────────────────────────────────────────────────────────────────
    if "ajio" in u_lower:
        # ajio.com/p/RF2140-WHT-XL or /p/CODE
        m = re.search(r"/p/([A-Z0-9\-]+)(?:[/?]|$)", url, re.IGNORECASE)
        if m:
            return f"ajio_{m.group(1).upper()}"
        m2 = re.search(r"/(\d{6,})(?:[/?]|$)", url, re.IGNORECASE)
        if m2:
            return f"ajio_{m2.group(1)}"

    # ── Nykaa ────────────────────────────────────────────────────────────────
    if "nykaa" in u_lower:
        # nykaa.com/.../p/PRODUCT_ID
        m = re.search(r"/p/(\d+)", url, re.IGNORECASE)
        if m:
            return f"nyk_{m.group(1)}"
        m2 = re.search(r"/(\d{5,})(?:[/?]|$)", url, re.IGNORECASE)
        if m2:
            return f"nyk_{m2.group(1)}"

    # ── Snapdeal ──────────────────────────────────────────────────────────────
    if "snapdeal" in u_lower:
        m = re.search(r"/product/[^/]+/(\d+)", url, re.IGNORECASE)
        if m:
            return f"sd_{m.group(1)}"

    # ── TataCliq ──────────────────────────────────────────────────────────────
    if "tatacliq" in u_lower:
        m = re.search(r"/p-[^/]*-(\d+)", url, re.IGNORECASE)
        if not m:
            m = re.search(r"/(\d{8,})(?:[/?]|$)", url, re.IGNORECASE)
        if m:
            return f"tc_{m.group(1)}"

    # ── JioMart ───────────────────────────────────────────────────────────────
    if "jiomart" in u_lower:
        m = re.search(r"/p/[^/]+-(\d+)", url, re.IGNORECASE)
        if not m:
            m = re.search(r"/(\d{6,})(?:[/?]|$)", url, re.IGNORECASE)
        if m:
            return f"jio_{m.group(1)}"

    # ── Shopsy ───────────────────────────────────────────────────────────────
    if "shopsy" in u_lower:
        m = re.search(r"/p/[^/]+/(\d+)", url, re.IGNORECASE)
        if m:
            return f"shp_{m.group(1)}"

    # ── IndiaMart ────────────────────────────────────────────────────────────
    if "indiamart" in u_lower:
        m = re.search(r"/proddetail/[^/]+-(\d+)", url, re.IGNORECASE)
        if m:
            return f"im_{m.group(1)}"

    # ── Generic fallback — path-only hash (query params already stripped) ────
    # Path-only hash ensure karta hai ki same product different query params ke saath
    # bhi same ID produce kare (e.g. paytmmall, reliancedigital, etc.)
    try:
        parsed = _up.urlparse(url)
        # path se sirf numeric IDs ya product slugs nikalo — common patterns
        path_clean = parsed.path.rstrip("/").lower()
        # Remove version/size/color suffixes jo change ho sakte hain
        # e.g. /product/name-blue-xl/12345 → stable on numeric ID
        long_num = re.search(r"/(\d{5,})(?:[/?]|$)", parsed.path)
        if long_num:
            domain_key = next(
                (d.split(".")[0] for d in _PRODUCT_DOMAINS if d in u_lower), "prod"
            )
            return f"{domain_key}_{long_num.group(1)}"
        # No numeric ID → path hash (tracking params already stripped)
        domain_key = next(
            (d.split(".")[0] for d in _PRODUCT_DOMAINS if d in u_lower), "prod"
        )
        return f"{domain_key}_p_{hashlib.md5(path_clean.encode()).hexdigest()[:12]}"
    except Exception:
        pass

    return None


def get_canonical_product_id(url: str):
    """
    Sync version: URL se canonical product ID nikalo.
    Short links synchronously resolve karta hai — known aur auto-detected dono.
    For best results use get_canonical_product_id_async().
    """
    if _is_short_link(url):
        url = _unshorten_sync(url)
    return _extract_product_id_from_url(url)


async def get_canonical_product_id_async(url: str):
    """
    Async version — sabse accurate.
    1. Short links resolve karta hai (known domain ya auto-detected)
    2. Tracking/affiliate params strip karta hai
    3. Har site ke liye canonical product ID extract karta hai
    """
    # Step 1: Short link detect aur resolve karo
    if _is_short_link(url):
        resolved = await _unshorten_async(url)
        # Agar resolved URL product domain pe hai to use karo
        # Agar resolve fail hua (same URL wapas aaya) to original try karo
        if resolved != url:
            url = resolved

    # Step 2: Extract canonical ID (tracking params strip bhi iske andar hota hai)
    return _extract_product_id_from_url(url)

def clean_text_semantic(text):
    if not text:
        return ""
    text = re.sub(r'http\S+', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    return " ".join(text.lower().split())

def generate_content_hash(event, source_id):
    components = [str(source_id)]
    text = (event.raw_text or "").strip().lower()
    if text:
        norm_text = re.sub(r'[^a-z0-9]', '', text)
        components.append(f"txt:{norm_text}")
    if event.media:
        if hasattr(event.media, 'document') and event.media.document:
            doc = event.media.document
            components.append(f"doc:{doc.id}:{doc.size}")
        elif hasattr(event.media, 'photo') and event.media.photo:
            components.append(f"pho:{event.media.photo.id}")
        elif hasattr(event.media, 'web_page') and event.media.web_page:
            wp = event.media.web_page
            # BUG FIX: web_page ID unique nahi hoti always — URL better hai
            url_key = getattr(wp, 'url', None) or str(getattr(wp, 'id', 'wp'))
            components.append(f"web:{hashlib.md5(url_key.encode()).hexdigest()[:12]}")
        elif event.media:
            # BUG FIX: Unrecognized media type → use media class name + message id
            # Prevents false negatives where same media not blocked as dup
            media_type = type(event.media).__name__
            msg_id = getattr(event, 'id', 0)
            components.append(f"unk:{media_type}:{msg_id}")
    # BUG FIX: Pure source_id only → still return hash (not None)
    # Previously returned None → message skipped from dup check entirely
    fingerprint = "|".join(components)
    return hashlib.md5(fingerprint.encode()).hexdigest()
