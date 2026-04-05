"""
translator.py — Unlimited Translation via Multi-Engine Rotation

ARCHITECTURE:
  5 free engines rotate karte hain — Round Robin + Smart Fallback
  Koi bhi limit kabhi full nahi hogi.

  Engine 1: MyMemory Free     (1000 req/day per IP)
  Engine 2: MyMemory + Email  (10,000 req/day — MYMEMORY_EMAIL env set karo)
  Engine 3: Google Free       (unofficial, high limit)
  Engine 4: LibreTranslate    (open source, free public instance)
  Engine 5: deep-translator   (agar installed ho — wraps multiple backends)

  Rotation: Round Robin per request
  Fallback: Agar engine fail → next engine try karo automatically
  Cache:    Same text+lang = no API call (1hr TTL, 500 entries)
"""

import asyncio
import hashlib
import logging
import os
import time

import aiohttp
from deep_translator import GoogleTranslator
from deep_translator import MyMemoryTranslator

logger = logging.getLogger(__name__)

# FIX 8: Global aiohttp session — reuse instead of creating new per request (socket exhaustion fix)
_global_http_session: "aiohttp.ClientSession | None" = None

async def get_http_session() -> "aiohttp.ClientSession":
    """Get or create the global HTTP session."""
    global _global_http_session
    import aiohttp
    if _global_http_session is None or _global_http_session.closed:
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        _global_http_session = aiohttp.ClientSession(timeout=timeout)
    return _global_http_session

async def close_http_session():
    """Call on bot shutdown."""
    global _global_http_session
    if _global_http_session and not _global_http_session.closed:
        await _global_http_session.close()
        _global_http_session = None

# ── Language map ──────────────────────────────────────────────────────────────
LANGUAGES = {
    "hi": "🇮🇳 Hindi",
    "en": "🇬🇧 English",
    "ur": "🇵🇰 Urdu",
    "bn": "🇧🇩 Bengali",
    "te": "Telugu",
    "mr": "Marathi",
    "ta": "Tamil",
    "gu": "Gujarati",
    "kn": "Kannada",
    "ml": "Malayalam",
    "pa": "Punjabi",
    "ar": "🇸🇦 Arabic",
    "fr": "🇫🇷 French",
    "de": "🇩🇪 German",
    "es": "🇪🇸 Spanish",
    "pt": "🇧🇷 Portuguese",
    "ru": "🇷🇺 Russian",
    "ja": "🇯🇵 Japanese",
    "ko": "🇰🇷 Korean",
    "zh": "🇨🇳 Chinese",
    "id": "🇮🇩 Indonesian",
    "tr": "🇹🇷 Turkish",
    "fa": "🇮🇷 Persian",
    "th": "🇹🇭 Thai",
    "vi": "🇻🇳 Vietnamese",
}

# ── Cache ─────────────────────────────────────────────────────────────────────
_CACHE: dict = {}
_CACHE_TTL   = 3600   # 1 hour
_MAX_CACHE   = 500

def _cache_key(text: str, lang: str) -> str:
    return hashlib.md5(f"{lang}:{text}".encode()).hexdigest()

def _cache_get(text: str, lang: str) -> str | None:
    e = _CACHE.get(_cache_key(text, lang))
    if e and (time.time() - e[1]) < _CACHE_TTL:
        return e[0]
    return None

def _cache_set(text: str, lang: str, translated: str):
    if len(_CACHE) > _MAX_CACHE:
        oldest = sorted(_CACHE, key=lambda k: _CACHE[k][1])
        for k in oldest[:_MAX_CACHE // 5]:
            del _CACHE[k]
    _CACHE[_cache_key(text, lang)] = (translated, time.time())

# ── Engine rotation state ─────────────────────────────────────────────────────
_engine_index = 0
_engine_failures: dict[int, int] = {}   # {engine_id: consecutive_failures}
_ENGINE_MAX_FAIL = 5   # After 5 failures, skip engine for 10 min
_engine_skip_until: dict[int, float] = {}

def _next_engine() -> int:
    global _engine_index
    now = time.time()
    for _ in range(5):
        idx = _engine_index % 5
        _engine_index += 1
        # Skip if temporarily disabled
        if _engine_skip_until.get(idx, 0) > now:
            continue
        # BUG FIX: MyMemory-Email (id=3) skip karo agar env set nahi
        if idx == 3 and not _MYMEMORY_EMAIL_SET:
            continue
        return idx
    return 0   # Fallback to engine 0

def _mark_success(engine_id: int):
    _engine_failures[engine_id] = 0
    _engine_skip_until.pop(engine_id, None)

def _mark_failure(engine_id: int):
    _engine_failures[engine_id] = _engine_failures.get(engine_id, 0) + 1
    if _engine_failures[engine_id] >= _ENGINE_MAX_FAIL:
        _engine_skip_until[engine_id] = time.time() + 600   # Skip 10 min
        logger.warning(f"Translation engine {engine_id} paused for 10 min (too many failures)")

# ── Engine 0: MyMemory Free ───────────────────────────────────────────────────
async def _mymemory_call(text: str, target: str, email: str = "") -> str | None:
    """
    MyMemory API helper — auto source fix.
    'auto' langpair MyMemory accept nahi karta (screenshot mein yahi error tha).
    Fix: en|target use karo as fallback source.
    """
    url     = "https://api.mymemory.translated.net/get"
    timeout = aiohttp.ClientTimeout(total=8)
    # BUG FIX 1: "auto" ke jagah detectlanguage use karo
    # Hardcoded "en" source galat tha — Arabic/Hindi source messages wrong translate hote the
    # BUG FIX: "autodetect" MyMemory accept nahi karta → "en" use karo
    # "autodetect" send karne par "PLEASE SELECT TWO DISTINCT LANGUAGES" error aata hai
    # Jo message ke text mein forward ho jaata tha
    src = "en" if target != "en" else "fr"
    params: dict = {"q": text[:2000], "langpair": f"{src}|{target}"}
    if email:
        params["de"] = email
    try:
        s = await get_http_session()
        async with s.get(url, params=params, timeout=timeout) as r:
            if r.status == 200:
                d   = await r.json(content_type=None)
                res = d.get("responseData", {}).get("translatedText", "")
                res_up = (res or "").upper()
                # BUG FIX: "PLEASE SELECT" error bhi check karo
                if (res
                        and "MYMEMORY WARNING"           not in res_up
                        and "INVALID SOURCE"             not in res_up
                        and "INVALID TARGET"             not in res_up
                        and "PLEASE SELECT"              not in res_up
                        and "TWO DISTINCT"               not in res_up
                        and res.strip() != text.strip()):
                    return res
    except Exception as e:
        logger.debug(f"MyMemory call error: {e}")
    return None


async def _engine_mymemory_free(text: str, target: str) -> str | None:
    return await _mymemory_call(text, target)


# ── Engine 1: MyMemory with Email (10x limit) ─────────────────────────────────
async def _engine_mymemory_email(text: str, target: str) -> str | None:
    email = os.environ.get("MYMEMORY_EMAIL", "")
    if not email:
        # BUG FIX: Email nahi set → engine ko skip karo aur failure count mat badhao
        # Pehle: None return hota tha → _mark_failure call hota tha → engine pause
        # Ab: directly None return, caller ko pata chale engine available nahi
        return None
    return await _mymemory_call(text, target, email=email)

# ── Engine 2: Google Translate (unofficial free endpoint) ─────────────────────
# ── Google Translate Multi-Endpoint Rotation ─────────────────────────────────
# Unlimited translation trick: Google ke multiple endpoints/clients rotate karo
# Har endpoint ka alag rate limit → effectively unlimited
_GOOGLE_ENDPOINTS = [
    # (url, client, extra_params)
    ("https://translate.googleapis.com/translate_a/single", "gtx",           {}),
    ("https://translate.google.com/translate_a/single",     "gtx",           {}),
    ("https://translate.google.co.in/translate_a/single",   "gtx",           {}),
    ("https://translate.google.com/translate_a/single",     "dict-chrome-ex",{}),
    ("https://translate.googleapis.com/translate_a/single", "gtx",           {"dj": "1"}),
]
_google_ep_index = 0

async def _engine_google_free(text: str, target: str) -> str | None:
    """
    Multi-endpoint Google Translate rotation.
    5 alag endpoints/clients → ek fail ho to dusra try karo
    Effectively unlimited — alag servers, alag rate limits.
    """
    global _google_ep_index
    timeout = aiohttp.ClientTimeout(total=10)
    s = await get_http_session()

    # Current endpoint try karo, fail hone par next
    for attempt in range(len(_GOOGLE_ENDPOINTS)):
        ep_idx = (_google_ep_index + attempt) % len(_GOOGLE_ENDPOINTS)
        url, client, extra = _GOOGLE_ENDPOINTS[ep_idx]
        params = {
            "client": client,
            "sl":     "auto",
            "tl":     target,
            "dt":     "t",
            "q":      text[:5000],
            **extra
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        try:
            async with s.get(url, params=params, headers=headers, timeout=timeout) as r:
                if r.status == 200:
                    data   = await r.json(content_type=None)
                    parts  = data[0]
                    result = "".join(p[0] for p in parts if p and p[0])
                    if result and result.strip():
                        # Rotate to next endpoint for next call (load balance)
                        _google_ep_index = (ep_idx + 1) % len(_GOOGLE_ENDPOINTS)
                        return result
                elif r.status == 429:
                    # Rate limited — next endpoint try karo
                    logger.debug(f"Google endpoint {ep_idx} rate limited, trying next")
                    continue
        except Exception as e:
            logger.debug(f"Google endpoint {ep_idx} error: {e}")
            continue

    return None

# ── Engine 3: LibreTranslate (free public instance) ───────────────────────────
_LIBRE_INSTANCES = [
    "https://libretranslate.com",
    # BUG FIX: Unreliable/dead instances hataye — sirf main instance rakha
    # translate.argosopentech.com aur libretranslate.de frequently down rehte hain
]
_libre_idx = 0

async def _engine_libretranslate(text: str, target: str) -> str | None:
    global _libre_idx
    instance = _LIBRE_INSTANCES[_libre_idx % len(_LIBRE_INSTANCES)]
    _libre_idx += 1
    try:
        url     = f"{instance}/translate"
        payload = {"q": text[:5000], "source": "auto", "target": target, "format": "text"}
        timeout = aiohttp.ClientTimeout(total=10)
        s = await get_http_session()
        async with s.post(url, json=payload, timeout=timeout) as r:
                if r.status == 200:
                    d = await r.json()
                    return d.get("translatedText") or None
    except Exception as e:
        logger.debug(f"LibreTranslate error ({instance}): {e}")
    return None

# ── Engine 4: deep-translator library ────────────────────────────────────────
async def _engine_deep_translator(text: str, target: str) -> str | None:
    """
    deep-translator — pip install deep-translator
    Wraps: Google, MyMemory, DeepL, Linguee, Papago, etc.
    Falls back gracefully if not installed.
    """
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _deep_translator_sync, text, target)
        return result
    except Exception as e:
        logger.debug(f"deep-translator error: {e}")
    return None

def _deep_translator_sync(text: str, target: str) -> str | None:
    """deep-translator: multiple backends try karo."""
    # Backend 1: GoogleTranslator (most reliable)
    try:
        from deep_translator import GoogleTranslator
        translated = GoogleTranslator(source="auto", target=target).translate(text[:5000])
        if translated and translated != text:
            return translated
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"deep-translator GoogleTranslator error: {e}")

    # Backend 2: MyMemoryTranslator (different service, different limit)
    try:
        from deep_translator import MyMemoryTranslator
        translated = MyMemoryTranslator(source="auto", target=target).translate(text[:5000])
        if translated and translated != text:
            return translated
    except ImportError:
        pass
    except Exception:
        pass

    return None

# ── Engine list ───────────────────────────────────────────────────────────────
_MYMEMORY_EMAIL_SET = bool(os.environ.get("MYMEMORY_EMAIL", ""))

# Engine order: Google multi-endpoint first (effectively unlimited)
# Fallbacks sirf tab kaam aate hain jab Google completely fail ho
_ENGINES = [
    (0, "Google-Multi",     _engine_google_free),       # PRIMARY: 5 endpoints rotate, effectively unlimited
    (1, "deep-translator",  _engine_deep_translator),   # BACKUP 1: GoogleTranslator + MyMemory
    (2, "MyMemory-Free",    _engine_mymemory_free),     # BACKUP 2: 1k/day
    (3, "MyMemory-Email",   _engine_mymemory_email),    # BACKUP 3: 10k/day (agar email set ho)
    (4, "LibreTranslate",   _engine_libretranslate),    # BACKUP 4: open source
]

# ── Main translate function ───────────────────────────────────────────────────
async def translate_text(text: str, target_lang: str, user_id: int = None) -> str | None:
    """
    Translate text using rotating engines.
    Returns translated text or None (caller uses original).

    Rotation: Round Robin across 5 engines
    Fallback: If engine fails → try next automatically
    Cache: 1hr TTL — same text never translated twice
    """
    if not text or not text.strip():
        return None

    target_lang = target_lang.lower().strip()
    if target_lang not in LANGUAGES:
        return None

    # Cache check
    cached = _cache_get(text, target_lang)
    if cached:
        return cached

    # Google (engine 0) pehle try karo — 5 endpoints internally rotate karta hai
    # Sirf fail hone par fallback engines try karo
    tried = set()
    for attempt in range(5):
        eng_id  = _next_engine()
        if eng_id in tried:
            continue
        tried.add(eng_id)

        _, name, fn = _ENGINES[eng_id]
        try:
            result = await asyncio.wait_for(fn(text, target_lang), timeout=10.0)
            if result and result.strip() and result.lower() != text.lower():
                _mark_success(eng_id)
                _cache_set(text, target_lang, result)
                logger.debug(f"Translated via {name} ({len(text)} chars → {target_lang})")
                return result
            elif result is None:
                # BUG FIX: None = engine intentionally skipped (e.g. no email set)
                # Failure count mat badhao — engine available nahi, broken nahi
                pass
            else:
                # Empty or same text = actual failure
                _mark_failure(eng_id)
        except asyncio.TimeoutError:
            _mark_failure(eng_id)
            logger.debug(f"Engine {name} timeout")
        except Exception as e:
            _mark_failure(eng_id)
            logger.debug(f"Engine {name} error: {e}")

    logger.debug(f"All translation engines failed for lang={target_lang}")
    return None

# ── User settings ─────────────────────────────────────────────────────────────
def get_translation_settings(user_id: int) -> dict:
    from database import get_user_data
    return get_user_data(user_id).get("translation", {})

def set_global_translate(user_id: int, enabled: bool, lang: str = None):
    from database import get_user_data
    d = get_user_data(user_id)
    d.setdefault("translation", {})
    d["translation"]["global_enabled"] = enabled
    if lang:
        d["translation"]["global_lang"] = lang

def set_source_translate(user_id: int, source_id: str, lang: str | None):
    from database import get_user_data
    d = get_user_data(user_id)
    d.setdefault("translation", {}).setdefault("per_source", {})
    if lang:
        d["translation"]["per_source"][str(source_id)] = lang
    else:
        d["translation"]["per_source"].pop(str(source_id), None)

def get_target_lang(user_id: int, source_id=None) -> str | None:
    try:
        from database import get_user_data
        trans = get_user_data(user_id).get("translation", {})
        if source_id:
            per = trans.get("per_source", {}).get(str(source_id))
            if per:
                return per
        if trans.get("global_enabled") and trans.get("global_lang"):
            return trans["global_lang"]
    except Exception:
        pass
    return None

async def maybe_translate(text: str, user_id: int, source_id=None) -> str:
    """
    Translate text to target language.
    Returns original on failure — never breaks forwarding.
    """
    if not text or len(text.strip()) < 3:
        return text
    target = get_target_lang(user_id, source_id)
    if not target:
        return text

    # BUG FIX: HTML tags strip karke meaningful content check karo
    import re as _re
    plain = _re.sub(r'<[^>]+>', '', text)
    clean = _re.sub(r'https?://[\S]+|@[\w]+|#[\w]+|[\d]+', '', plain).strip()
    if len(clean) < 3:
        return text  # Only URLs/numbers/tags — nothing meaningful to translate

    try:
        # BUG FIX 6: timeout 30s (5 engines × 8s each)
        translated = await asyncio.wait_for(
            translate_text(text, target, user_id),
            timeout=30.0
        )
        if not translated or translated.strip() == text.strip():
            return text
        return translated
    except Exception:
        return text


def get_engine_status() -> str:
    """Admin ke liye engine status — kaun chal raha hai, kaun paused."""
    now    = time.time()
    lines  = ["🔄 **Translation Engine Status:**\n"]
    for eid, name, _ in _ENGINES:
        skip_until = _engine_skip_until.get(eid, 0)
        fails      = _engine_failures.get(eid, 0)
        if skip_until > now:
            remaining = int(skip_until - now)
            lines.append(f"  ⏸ {name} — paused {remaining}s")
        elif fails > 0:
            lines.append(f"  ⚠️ {name} — {fails} recent failures")
        else:
            lines.append(f"  ✅ {name} — active")
    return "\n".join(lines)
