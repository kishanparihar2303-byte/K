"""
rate_limiter.py — Adaptive Token Bucket Rate Limiter v2.0
══════════════════════════════════════════════════════════

ALGORITHM: Token Bucket (superior to leaky bucket for burst handling)
  - Har destination ke liye alag "bucket" hoti hai
  - Bucket mein tokens hote hain (= allowed sends)
  - Har second kuch tokens add hote hain (refill rate)
  - Send karne ke liye 1 token chahiye
  - Bucket full hoti hai agar tokens consume nahi ho rahe

ADAPTIVE FEATURE:
  - FloodWait milne pe: automatically send rate kam karo (halve it)
  - No FloodWait 5 min ke baad: rate wapas badhao (slowly)
  - Min rate: 0.1 msg/sec | Max rate: 10 msg/sec per dest

PREMIUM vs FREE:
  - Free tier: 1 msg/2s per dest (0.5 tokens/sec)
  - Premium:   5 msg/s per dest  (5.0 tokens/sec)
  - Admin:     10 msg/s per dest (10.0 tokens/sec)

INTEGRATION:
  from rate_limiter import RateLimiterRegistry

  limiter = RateLimiterRegistry.get(user_id, dest_key)
  delay = limiter.acquire()   # Returns how long to wait (0 = immediate)
  if delay > 0:
      await asyncio.sleep(delay)

  # On FloodWait:
  limiter.on_flood_wait(seconds=30)

  # On success:
  limiter.on_success()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Tuple, Optional

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════

@dataclass
class RLConfig:
    # Tokens per second refill rate
    base_rate:   float = 1.0    # Default: 1 msg/sec
    max_rate:    float = 10.0   # Never exceed this
    min_rate:    float = 0.1    # Never go below this

    # Bucket capacity (burst size)
    burst_size:  float = 5.0    # Max burst: 5 messages at once

    # Adaptive settings
    adaptive:    bool  = True   # Enable adaptive rate adjustment
    backoff_factor:  float = 0.5   # On FloodWait: rate × 0.5
    recovery_factor: float = 1.1   # Every 5min no flood: rate × 1.1
    recovery_interval: float = 300.0  # 5 minutes

    # Custom delay (user-set)
    custom_delay_sec: float = 0.0


# ══════════════════════════════════════════
# TOKEN BUCKET RATE LIMITER
# ══════════════════════════════════════════

class TokenBucket:
    """
    Token bucket rate limiter — async safe.
    """
    def __init__(self, config: RLConfig):
        self.config    = config
        self.tokens    = config.burst_size   # Start full
        self.rate      = config.base_rate    # Current tokens/sec
        self.last_refill = time.monotonic()

        # Adaptive tracking
        self.last_flood_ts    = 0.0
        self.last_recovery_ts = time.monotonic()
        self.flood_wait_count = 0
        self.success_streak   = 0

        # Stats
        self.total_waits    = 0
        self.total_acquired = 0
        self.total_flood_waits = 0
        self._lock = asyncio.Lock()

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.last_refill = now

        # Add tokens based on rate
        new_tokens = elapsed * self.rate
        self.tokens = min(self.config.burst_size, self.tokens + new_tokens)

        # Adaptive recovery: no flood in 5 min → slowly increase rate
        if (self.config.adaptive and
                (now - self.last_recovery_ts) >= self.config.recovery_interval and
                (now - self.last_flood_ts) >= self.config.recovery_interval):
            old_rate = self.rate
            self.rate = min(self.config.max_rate, self.rate * self.config.recovery_factor)
            self.last_recovery_ts = now
            if self.rate != old_rate:
                logger.debug(f"[RL] Rate recovered: {old_rate:.2f} → {self.rate:.2f} tok/s")

    def acquire(self) -> float:
        """
        Try to acquire a token.
        Returns: seconds to wait (0 = can send immediately)
        """
        self._refill()

        # Custom delay (user-configured)
        custom_delay = self.config.custom_delay_sec

        if self.tokens >= 1.0:
            self.tokens -= 1.0
            self.total_acquired += 1
            return max(0.0, custom_delay)

        # Not enough tokens — calculate wait time
        tokens_needed = 1.0 - self.tokens
        wait_time = tokens_needed / self.rate
        self.total_waits += 1

        return max(wait_time, custom_delay)

    async def acquire_async(self) -> float:
        """Async-safe token acquisition with lock."""
        async with self._lock:
            return self.acquire()

    def on_flood_wait(self, seconds: float):
        """FloodWait mila — rate halve karo."""
        self.total_flood_waits += 1
        self.flood_wait_count += 1
        self.last_flood_ts = time.monotonic()

        if self.config.adaptive:
            old_rate = self.rate
            self.rate = max(self.config.min_rate, self.rate * self.config.backoff_factor)
            # Also drain tokens
            self.tokens = 0.0
            logger.warning(
                f"[RL] FloodWait {seconds}s → Rate: {old_rate:.2f} → {self.rate:.2f} tok/s"
            )

    def on_success(self):
        """Successful send — track streak."""
        self.success_streak += 1

    def get_stats(self) -> dict:
        return {
            "current_rate":    round(self.rate, 3),
            "tokens":          round(self.tokens, 2),
            "total_acquired":  self.total_acquired,
            "total_waits":     self.total_waits,
            "flood_waits":     self.total_flood_waits,
            "success_streak":  self.success_streak,
        }


# ══════════════════════════════════════════
# PER-DESTINATION RATE LIMITER
# ══════════════════════════════════════════

class UserDestRateLimiter:
    """
    Ek user ke liye multi-destination rate limiter.
    Har destination ke liye alag bucket.
    """
    def __init__(self, user_id: int, is_premium: bool = False, is_admin: bool = False):
        self.user_id    = user_id
        self.is_premium = is_premium
        self.is_admin   = is_admin
        self._buckets: Dict[str, TokenBucket] = {}
        # Global user-level bucket (Telegram account limit)
        self._global_bucket = TokenBucket(self._global_config())

    def _global_config(self) -> RLConfig:
        """User-level global rate limit."""
        if self.is_admin:
            return RLConfig(base_rate=10.0, burst_size=20.0, max_rate=20.0)
        elif self.is_premium:
            return RLConfig(base_rate=5.0, burst_size=15.0, max_rate=10.0)
        else:
            # ✅ FIX DELAY: burst_size 5→15 kiya — 15 msgs bina wait ke ja sakenge
            # Pehle 5 msgs ke baad rate limit lag jaata tha — jab source se 3 msgs
            # jaldi-jaldi aate the to 3rd msg ko delay hoti thi. Ab nahi hogi.
            return RLConfig(base_rate=1.0, burst_size=15.0, max_rate=3.0)

    def _dest_config(self, custom_delay: float = 0.0) -> RLConfig:
        """Per-destination config."""
        if self.is_admin:
            return RLConfig(base_rate=5.0, burst_size=10.0, custom_delay_sec=custom_delay)
        elif self.is_premium:
            return RLConfig(base_rate=2.0, burst_size=8.0, custom_delay_sec=custom_delay)
        else:
            # ✅ FIX DELAY: per-dest burst 3→8 kiya
            # Pehle har destination ke liye sirf 3 msgs burst mein ja sakte the.
            # 3 msgs ek saath aane pe 3rd msg delay hoti thi. Ab 8 burst hai.
            return RLConfig(base_rate=0.5, burst_size=8.0, custom_delay_sec=custom_delay)

    def get_bucket(self, dest_key: str, custom_delay: float = 0.0) -> TokenBucket:
        if dest_key not in self._buckets:
            self._buckets[dest_key] = TokenBucket(self._dest_config(custom_delay))
        return self._buckets[dest_key]

    async def wait_for_slot(self, dest_key: str, custom_delay: float = 0.0) -> float:
        """
        Wait until we can send to this destination.
        Returns actual wait time in seconds.
        """
        dest_bucket   = self.get_bucket(dest_key, custom_delay)
        dest_wait     = await dest_bucket.acquire_async()
        global_wait   = await self._global_bucket.acquire_async()

        total_wait = max(dest_wait, global_wait)
        if total_wait > 0:
            await asyncio.sleep(total_wait)
        return total_wait

    def on_flood_wait(self, dest_key: str, seconds: float):
        self.get_bucket(dest_key).on_flood_wait(seconds)
        self._global_bucket.on_flood_wait(seconds * 0.5)  # Global mein bhi thoda slow

    def on_success(self, dest_key: str):
        self.get_bucket(dest_key).on_success()
        self._global_bucket.on_success()

    def update_custom_delay(self, dest_key: str, delay: float):
        """User ne delay change kiya — update karo."""
        if dest_key in self._buckets:
            self._buckets[dest_key].config.custom_delay_sec = delay

    def get_stats(self) -> dict:
        return {
            "user_id":    self.user_id,
            "is_premium": self.is_premium,
            "global":     self._global_bucket.get_stats(),
            "dests": {
                dest: bucket.get_stats()
                for dest, bucket in self._buckets.items()
            }
        }


# ══════════════════════════════════════════
# GLOBAL REGISTRY
# ══════════════════════════════════════════

class _RateLimiterRegistry:
    def __init__(self):
        self._limiters: Dict[int, UserDestRateLimiter] = {}
        self._lock = asyncio.Lock()

    def get(self, user_id: int, force_refresh: bool = False) -> UserDestRateLimiter:
        """Get or create a rate limiter for this user."""
        if user_id not in self._limiters or force_refresh:
            try:
                from admin import is_admin
                from premium import is_premium_user
                _is_admin   = is_admin(user_id)
                _is_premium = is_premium_user(user_id)
            except Exception:
                _is_admin   = False
                _is_premium = False

            self._limiters[user_id] = UserDestRateLimiter(
                user_id    = user_id,
                is_premium = _is_premium,
                is_admin   = _is_admin,
            )
        return self._limiters[user_id]

    def on_flood_wait(self, user_id: int, dest_key: str, seconds: float):
        if user_id in self._limiters:
            self._limiters[user_id].on_flood_wait(dest_key, seconds)

    def on_success(self, user_id: int, dest_key: str):
        if user_id in self._limiters:
            self._limiters[user_id].on_success(dest_key)

    def cleanup(self):
        """Inactive users ke limiters clean karo."""
        try:
            from database import user_sessions
            active = set(user_sessions.keys())
            stale = [uid for uid in self._limiters if uid not in active]
            for uid in stale:
                del self._limiters[uid]
            if stale:
                logger.debug(f"[RL] Cleaned {len(stale)} stale rate limiters")
        except Exception:
            pass

    def get_global_stats(self) -> dict:
        """All limiters ka summary."""
        total_users = len(self._limiters)
        total_buckets = sum(len(l._buckets) for l in self._limiters.values())
        return {
            "tracked_users":  total_users,
            "tracked_buckets": total_buckets,
        }


# Singleton
RateLimiterRegistry = _RateLimiterRegistry()


# ══════════════════════════════════════════
# EASY INTEGRATION FUNCTION
# ══════════════════════════════════════════

async def smart_delay(user_id: int, dest_key: str, custom_delay: float = 0.0) -> float:
    """
    Drop-in replacement for asyncio.sleep() in forward_engine.py

    Usage:
        # Old code:
        if custom_delay > 0:
            await asyncio.sleep(custom_delay)

        # New code:
        waited = await smart_delay(user_id, dest_key, custom_delay)
    """
    limiter = RateLimiterRegistry.get(user_id)
    return await limiter.wait_for_slot(str(dest_key), custom_delay)
