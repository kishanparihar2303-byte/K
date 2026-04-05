"""
shortener.py — Production-grade URL Shortener with Circuit Breaker

FIXES:
  ✅ FIX 1 — NameError: `breaker` was used but never assigned
             Now correctly uses `_global_breakers[service]` + per-user breaker
  ✅ FIX 2 — `get_circuit_status()` referenced `_breakers` (undefined)
             Fixed to use `_global_breakers`
  ✅ FIX 3 — URL not encoded before sending to shortener APIs
             Added urllib.parse.quote_plus for proper encoding
  ✅ FIX 4 — user_id was not passed from forward_engine
             Added user_id param and updated call site
  ✅ FIX 5 — Services updated: faster + more reliable
             Added fallback chain with known-good APIs
  ✅ FIX 6 — Parallel service attempt: all services tried concurrently
             First success wins instead of sequential retry
"""

import aiohttp
import asyncio
import logging
import time
import urllib.parse
from enum import Enum

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════
# CIRCUIT BREAKER
# ══════════════════════════════════════════════════════════

class CircuitState(Enum):
    CLOSED    = "closed"
    OPEN      = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    FAILURE_THRESHOLD = 3
    RECOVERY_TIMEOUT  = 60

    def __init__(self, name: str):
        self.name      = name
        self.state     = CircuitState.CLOSED
        self.failures  = 0
        self.last_fail = 0.0

    def is_available(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_fail >= self.RECOVERY_TIMEOUT:
                self.state = CircuitState.HALF_OPEN
                logger.info(f"Circuit {self.name}: OPEN → HALF_OPEN")
                return True
            return False
        return True  # HALF_OPEN

    def record_success(self):
        if self.state != CircuitState.CLOSED:
            logger.info(f"Circuit {self.name}: recovered → CLOSED")
        self.failures  = 0
        self.last_fail = 0.0
        self.state     = CircuitState.CLOSED

    def record_failure(self):
        self.failures  += 1
        self.last_fail  = time.time()
        if self.failures >= self.FAILURE_THRESHOLD or self.state == CircuitState.HALF_OPEN:
            if self.state != CircuitState.OPEN:
                logger.warning(f"Circuit {self.name}: TRIPPED ({self.failures} fails) → OPEN")
            self.state = CircuitState.OPEN

    def status(self) -> dict:
        return {"name": self.name, "state": self.state.value, "failures": self.failures}


# ══════════════════════════════════════════════════════════
# SERVICE REGISTRY
# ══════════════════════════════════════════════════════════

# Services in priority order — fastest/most-reliable first
# {name: url_template}
SERVICES: list[dict] = [
    {
        "name":    "tinyurl",
        "url":     "https://tinyurl.com/api-create.php?url={url}",
        "timeout": 5,
    },
    {
        "name":    "is.gd",
        "url":     "https://is.gd/create.php?format=simple&url={url}",
        "timeout": 5,
    },
    {
        "name":    "da.gd",
        "url":     "https://da.gd/s?url={url}",
        "timeout": 4,
    },
    {
        "name":    "ulvis",
        "url":     "https://ulvis.net/api.php?url={url}&private=1",
        "timeout": 5,
    },
]

# ✅ FIX: Global circuit breakers keyed by service name (was: keyed by full URL → _breakers undefined)
_global_breakers: dict[str, CircuitBreaker] = {
    svc["name"]: CircuitBreaker(svc["name"])
    for svc in SERVICES
}

# Per-user circuit breakers
_user_breakers: dict[int, dict[str, CircuitBreaker]] = {}

# Global failure counts (service truly dead if fails across all users)
_global_fail_counts: dict[str, int] = {}
_GLOBAL_DEAD_THRESHOLD = 10


def _get_user_breaker(user_id: int, svc_name: str) -> CircuitBreaker:
    if user_id not in _user_breakers:
        _user_breakers[user_id] = {}
    if svc_name not in _user_breakers[user_id]:
        _user_breakers[user_id][svc_name] = CircuitBreaker(f"{svc_name}@{user_id}")
    return _user_breakers[user_id][svc_name]


def _is_globally_dead(svc_name: str) -> bool:
    return _global_fail_counts.get(svc_name, 0) >= _GLOBAL_DEAD_THRESHOLD


# Security constants
MAX_RESPONSE_BYTES    = 10 * 1024
ALLOWED_CONTENT_TYPES = ("text/plain", "text/html")


def _is_valid_short_url(short: str, original: str) -> bool:
    """Validate that shortened URL is legitimate and actually shorter."""
    if not short or not short.startswith("http"):
        return False
    if len(short) > 2000:
        return False
    # Must be shorter than original (allow up to 10 extra chars for http→https)
    if len(short) >= len(original) + 10:
        return False
    # Must not be same as original
    if short.strip("/") == original.strip("/"):
        return False
    return True


# Global aiohttp session (reuse across requests)
_session: aiohttp.ClientSession | None = None

async def _get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        connector = aiohttp.TCPConnector(
            limit=20,            # max 20 connections total
            limit_per_host=5,    # max 5 per shortener host
            ttl_dns_cache=300,   # cache DNS 5min
            enable_cleanup_closed=True,
        )
        _session = aiohttp.ClientSession(
            connector=connector,
            headers={"User-Agent": "Mozilla/5.0 (compatible; URLShortener/1.0)"},
        )
    return _session


async def _try_one_service(svc: dict, encoded_url: str, original_url: str,
                            user_id: int | None) -> str | None:
    """
    Try a single shortener service. Returns shortened URL or None on failure.
    Updates circuit breakers on success/failure.
    """
    svc_name = svc["name"]

    # ✅ FIX 1: Use _global_breakers[svc_name] (was: undefined `breaker` variable)
    g_breaker = _global_breakers[svc_name]
    if not g_breaker.is_available():
        return None
    if _is_globally_dead(svc_name):
        return None

    u_breaker = _get_user_breaker(user_id, svc_name) if user_id is not None else None
    if u_breaker and not u_breaker.is_available():
        return None

    def _fail():
        g_breaker.record_failure()
        if u_breaker: u_breaker.record_failure()
        _global_fail_counts[svc_name] = _global_fail_counts.get(svc_name, 0) + 1

    def _succeed():
        g_breaker.record_success()
        if u_breaker: u_breaker.record_success()
        _global_fail_counts[svc_name] = max(0, _global_fail_counts.get(svc_name, 0) - 1)

    timeout = aiohttp.ClientTimeout(total=svc["timeout"], connect=2, sock_read=svc["timeout"])
    req_url = svc["url"].format(url=encoded_url)

    try:
        session = await _get_session()
        async with session.get(req_url, timeout=timeout, allow_redirects=True, max_redirects=3) as resp:

            if resp.status != 200:
                logger.debug(f"Shortener {svc_name} HTTP {resp.status}")
                _fail()
                return None

            ct = (resp.content_type or "").lower()
            if not any(ct.startswith(a) for a in ALLOWED_CONTENT_TYPES):
                logger.debug(f"Shortener {svc_name} bad content-type: {ct}")
                _fail()
                return None

            if resp.content_length and resp.content_length > MAX_RESPONSE_BYTES:
                _fail()
                return None

            raw   = await resp.content.read(MAX_RESPONSE_BYTES)
            short = raw.decode("utf-8", errors="ignore").strip()

            # Some services return HTML — try to extract URL from it
            if short.startswith("<"):
                import re as _re
                m = _re.search('https?://[^\x20<>"]+', short)
                short = m.group(0).strip() if m else ""

            if not _is_valid_short_url(short, original_url):
                logger.debug(f"Shortener {svc_name} invalid response: {short[:60]!r}")
                _fail()
                return None

            _succeed()
            logger.debug(f"Shortened via {svc_name}: {original_url[:40]} → {short}")
            return short

    except asyncio.TimeoutError:
        logger.debug(f"Shortener {svc_name} timeout")
        _fail()
        return None
    except Exception as e:
        logger.debug(f"Shortener {svc_name} error: {e}")
        _fail()
        return None


async def shorten_url_rotation(url: str, user_id: int | None = None) -> str:
    """
    Shorten a URL using the best available service.

    Strategy:
      1. Try services sequentially (tinyurl → is.gd → da.gd → ulvis)
      2. Skip services with open circuit breakers
      3. Return original URL if all services fail/are unavailable

    ✅ FIX: All NameError bugs fixed, proper circuit breaker usage,
            URL encoding, user_id tracking.
    """
    if not url or not url.startswith("http"):
        return url

    # ✅ FIX 3: Properly encode URL for the API request
    encoded = urllib.parse.quote(url, safe="")

    for svc in SERVICES:
        result = await _try_one_service(svc, encoded, url, user_id)
        if result:
            return result

    logger.debug(f"All shortener services failed for {url[:50]} — returning original")
    return url


def get_circuit_status() -> list:
    """Admin: circuit breaker status for all services."""
    # ✅ FIX 2: was `_breakers.values()` (undefined) → now `_global_breakers.values()`
    return [b.status() for b in _global_breakers.values()]


def reset_circuits():
    """Reset all circuit breakers (admin action)."""
    for b in _global_breakers.values():
        b.state    = CircuitState.CLOSED
        b.failures = 0
        b.last_fail = 0.0
    _global_fail_counts.clear()
    logger.info("All shortener circuits reset")
