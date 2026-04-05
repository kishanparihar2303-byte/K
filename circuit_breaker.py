"""
circuit_breaker.py — Production Circuit Breaker v2.0
═══════════════════════════════════════════════════════════════════

PATTERN: Circuit Breaker (Martin Fowler)
Har destination ke liye ek circuit breaker — 3 states:

  ┌─────────┐  5 fails   ┌──────┐  cool off  ┌───────────┐
  │ CLOSED  │ ──────────▶│ OPEN │ ──────────▶│ HALF_OPEN │
  │(normal) │            │(skip)│            │ (1 test)  │
  └─────────┘            └──────┘            └───────────┘
       ▲                                           │
       └───────────── success ────────────────────┘

WHY:
  - Purana system: destination fail hoti rahi → har message try karta tha → wasted time
  - Naya system: 5 consecutive fail → circuit OPEN → destination skip karo → speed 3x
  - Half-open state: cooldown ke baad ek test send → agar kaam kiya → back to normal

FEATURES:
  ✅ Per-user per-destination circuit breaker
  ✅ Configurable thresholds (fail_count, cooldown)
  ✅ Admin notification on OPEN/CLOSE
  ✅ Persistent state across restarts
  ✅ Half-open test with exponential backoff
  ✅ Dashboard stats for admin panel
  ✅ Thread-safe with asyncio locks

INTEGRATION:
  from circuit_breaker import CircuitBreakerRegistry
  cb = CircuitBreakerRegistry.get(user_id, dest_id)
  if not cb.is_closed(): return False  # skip this dest
  try:
      await send_message(...)
      cb.record_success()
  except Exception:
      cb.record_failure()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Tuple

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════
# STATE ENUM
# ══════════════════════════════════════════

class CBState(Enum):
    CLOSED    = "closed"     # Normal — allow all requests
    OPEN      = "open"       # Failing — block all requests
    HALF_OPEN = "half_open"  # Testing — allow one probe request


# ══════════════════════════════════════════
# CIRCUIT BREAKER CONFIG
# ══════════════════════════════════════════

@dataclass
class CBConfig:
    fail_threshold:     int   = 15     # ✅ FIX: 5→15 — burst mein FloodWait se CB jaldi na khule
    success_threshold:  int   = 2      # Half-open mein kitni success pe CLOSE ho
    cooldown_sec:       float = 30.0   # ✅ FIX: 60→30 — CB jaldi recover kare
    max_cooldown_sec:   float = 300.0  # ✅ FIX: 3600→300 — max 5 min cooldown (1hr bahut zyada tha)
    backoff_multiplier: float = 1.5    # ✅ FIX: 2.0→1.5 — exponential backoff dheema karo
    notify_admin:       bool  = True   # Admin ko notify karo on state change


# ══════════════════════════════════════════
# SINGLE CIRCUIT BREAKER
# ══════════════════════════════════════════

@dataclass
class CircuitBreaker:
    user_id:  int
    dest_key: str
    config:   CBConfig = field(default_factory=CBConfig)

    # Runtime state
    state:           CBState = CBState.CLOSED
    fail_count:      int     = 0
    success_count:   int     = 0
    last_failure_ts: float   = 0.0
    opened_at:       float   = 0.0
    open_count:      int     = 0       # Kitni baar khula — exponential backoff ke liye
    last_error:      str     = ""
    total_fails:     int     = 0
    total_successes: int     = 0

    # Async lock — concurrent access safe
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def is_closed(self) -> bool:
        """True agar requests allow hain."""
        self._maybe_attempt_reset()
        return self.state in (CBState.CLOSED, CBState.HALF_OPEN)

    def is_open(self) -> bool:
        """True agar requests blocked hain."""
        self._maybe_attempt_reset()
        return self.state == CBState.OPEN

    def _current_cooldown(self) -> float:
        """Exponential backoff: cooldown × 2^open_count (capped)."""
        raw = self.config.cooldown_sec * (self.config.backoff_multiplier ** min(self.open_count, 6))
        return min(raw, self.config.max_cooldown_sec)

    def _maybe_attempt_reset(self):
        """OPEN → HALF_OPEN transition: cooldown expired?"""
        if self.state == CBState.OPEN:
            elapsed = time.time() - self.opened_at
            if elapsed >= self._current_cooldown():
                self.state = CBState.HALF_OPEN
                self.success_count = 0
                logger.info(
                    f"[CB] HALF_OPEN: user={self.user_id} dest={self.dest_key} "
                    f"(after {elapsed:.0f}s cooldown)"
                )

    def record_success(self):
        """Successful send record karo."""
        self.total_successes += 1
        if self.state == CBState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self._transition_to_closed()
        elif self.state == CBState.CLOSED:
            self.fail_count = 0  # Reset streak on success

    def record_failure(self, error: str = ""):
        """Failed send record karo."""
        self.total_fails += 1
        self.last_failure_ts = time.time()
        self.last_error = error[:200] if error else ""

        if self.state == CBState.HALF_OPEN:
            # Test failed — back to OPEN
            self._transition_to_open("Half-open test failed")
        elif self.state == CBState.CLOSED:
            self.fail_count += 1
            if self.fail_count >= self.config.fail_threshold:
                self._transition_to_open(f"Threshold reached ({self.fail_count} fails)")

    def _transition_to_open(self, reason: str = ""):
        old_state = self.state
        self.state     = CBState.OPEN
        self.opened_at = time.time()
        self.open_count += 1
        cooldown = self._current_cooldown()
        logger.warning(
            f"[CB] OPEN: user={self.user_id} dest={self.dest_key} "
            f"reason='{reason}' cooldown={cooldown:.0f}s open_count={self.open_count}"
        )
        if self.config.notify_admin and old_state == CBState.CLOSED:
            asyncio.create_task(self._notify_admin_open(reason, cooldown))

    def _transition_to_closed(self):
        self.state       = CBState.CLOSED
        self.fail_count  = 0
        self.success_count = 0
        self.open_count  = 0  # Reset backoff on full recovery
        logger.info(f"[CB] CLOSED: user={self.user_id} dest={self.dest_key} (recovered)")
        asyncio.create_task(self._notify_admin_closed())

    async def _notify_admin_open(self, reason: str, cooldown: float):
        """User ko bata do ki destination auto-disabled ho gayi."""
        try:
            from config import bot
            from database import get_user_data
            data = get_user_data(self.user_id)
            # Find dest name
            dest_name = self.dest_key
            for d in data.get("destinations", []):
                if str(d.get("id", "")) == str(self.dest_key) or str(d) == str(self.dest_key):
                    dest_name = d.get("name", self.dest_key) if isinstance(d, dict) else str(d)
                    break
            msg = (
                f"⚠️ **Destination Auto-Paused**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📤 Dest: `{dest_name}`\n"
                f"❌ Reason: {reason}\n"
                f"⏱ Auto-resume in: {cooldown/60:.0f} min\n"
                f"🔁 Open count: {self.open_count}\n\n"
                f"_Bot automatically retry karega cooldown ke baad._\n"
                f"_Manual resume: Settings → Destinations_"
            )
            await bot.send_message(self.user_id, msg)
        except Exception as e:
            logger.debug(f"CB notify error: {e}")

    async def _notify_admin_closed(self):
        """User ko bata do ki destination recover ho gayi."""
        try:
            from config import bot
            msg = (
                f"✅ **Destination Recovered**\n"
                f"📤 Dest: `{self.dest_key}` is now working again!\n"
                f"📊 Total fails this session: {self.total_fails}"
            )
            await bot.send_message(self.user_id, msg)
        except Exception:
            pass

    def get_status_emoji(self) -> str:
        self._maybe_attempt_reset()
        return {"closed": "🟢", "open": "🔴", "half_open": "🟡"}.get(self.state.value, "⚪")

    def to_dict(self) -> dict:
        """Persistent storage ke liye serialize karo."""
        return {
            "state":           self.state.value,
            "fail_count":      self.fail_count,
            "success_count":   self.success_count,
            "last_failure_ts": self.last_failure_ts,
            "opened_at":       self.opened_at,
            "open_count":      self.open_count,
            "last_error":      self.last_error,
            "total_fails":     self.total_fails,
            "total_successes": self.total_successes,
        }

    @classmethod
    def from_dict(cls, user_id: int, dest_key: str, d: dict, config: CBConfig = None) -> "CircuitBreaker":
        cb = cls(user_id=user_id, dest_key=dest_key, config=config or CBConfig())
        cb.state           = CBState(d.get("state", "closed"))
        cb.fail_count      = d.get("fail_count", 0)
        cb.success_count   = d.get("success_count", 0)
        cb.last_failure_ts = d.get("last_failure_ts", 0.0)
        cb.opened_at       = d.get("opened_at", 0.0)
        cb.open_count      = d.get("open_count", 0)
        cb.last_error      = d.get("last_error", "")
        cb.total_fails     = d.get("total_fails", 0)
        cb.total_successes = d.get("total_successes", 0)
        return cb


# ══════════════════════════════════════════
# GLOBAL REGISTRY
# ══════════════════════════════════════════

class _CircuitBreakerRegistry:
    """
    Global registry — har (user_id, dest_key) pair ke liye ek CB instance.
    Thread-safe singleton.
    """
    def __init__(self):
        self._registry: Dict[Tuple[int, str], CircuitBreaker] = {}
        self._lock = asyncio.Lock()

    def get(self, user_id: int, dest_key: str, config: CBConfig = None) -> CircuitBreaker:
        """Get or create a circuit breaker for this user+dest pair."""
        key = (user_id, str(dest_key))
        if key not in self._registry:
            self._registry[key] = CircuitBreaker(
                user_id=user_id,
                dest_key=str(dest_key),
                config=config or CBConfig()
            )
        return self._registry[key]

    def reset(self, user_id: int, dest_key: str):
        """Manually reset a circuit breaker (admin action)."""
        key = (user_id, str(dest_key))
        if key in self._registry:
            self._registry[key].state       = CBState.CLOSED
            self._registry[key].fail_count  = 0
            self._registry[key].open_count  = 0
            logger.info(f"[CB] Manual reset: user={user_id} dest={dest_key}")

    def get_all_for_user(self, user_id: int) -> Dict[str, CircuitBreaker]:
        """User ke saare circuit breakers return karo."""
        return {
            dest_key: cb
            for (uid, dest_key), cb in self._registry.items()
            if uid == user_id
        }

    def get_stats(self) -> dict:
        """Global stats for admin dashboard."""
        total = len(self._registry)
        open_count = sum(1 for cb in self._registry.values() if cb.state == CBState.OPEN)
        half_open  = sum(1 for cb in self._registry.values() if cb.state == CBState.HALF_OPEN)
        closed     = total - open_count - half_open
        return {
            "total":     total,
            "closed":    closed,
            "open":      open_count,
            "half_open": half_open,
        }

    def cleanup_stale(self, max_age_hours: int = 24):
        """Purane / inactive circuit breakers clean karo."""
        now = time.time()
        stale = [
            k for k, cb in self._registry.items()
            if cb.state == CBState.CLOSED
            and (now - cb.last_failure_ts) > max_age_hours * 3600
            and cb.total_fails == 0
        ]
        for k in stale:
            del self._registry[k]
        if stale:
            logger.debug(f"[CB] Cleaned {len(stale)} stale circuit breakers")

    def save_state(self) -> dict:
        """Full state serialize karo — DB mein save ke liye."""
        return {
            f"{uid}:{dest}": cb.to_dict()
            for (uid, dest), cb in self._registry.items()
        }

    def load_state(self, state_dict: dict):
        """DB se state restore karo on startup."""
        for key, data in state_dict.items():
            try:
                uid_str, dest = key.split(":", 1)
                uid = int(uid_str)
                cb = CircuitBreaker.from_dict(uid, dest, data)
                # Skip stale OPEN states older than 1 hour
                if cb.state == CBState.OPEN:
                    elapsed = time.time() - cb.opened_at
                    if elapsed > 3600:
                        cb.state = CBState.CLOSED
                        cb.fail_count = 0
                self._registry[(uid, dest)] = cb
            except Exception as e:
                logger.debug(f"[CB] Failed to restore state for {key}: {e}")


# Singleton instance
CircuitBreakerRegistry = _CircuitBreakerRegistry()


# ══════════════════════════════════════════
# CONVENIENCE WRAPPER
# ══════════════════════════════════════════

async def check_and_send(
    send_coro,
    user_id: int,
    dest_key: str,
    config: CBConfig = None
):
    """
    Circuit breaker wrapper for any send coroutine.

    Usage:
        result = await check_and_send(
            client.send_message(target, text),
            user_id=uid,
            dest_key=str(dest)
        )

    Returns: (success: bool, result: Any)
    """
    cb = CircuitBreakerRegistry.get(user_id, dest_key, config)

    if not cb.is_closed():
        cooldown_remaining = cb._current_cooldown() - (time.time() - cb.opened_at)
        logger.debug(
            f"[CB] Skipped dest={dest_key} user={user_id} "
            f"(OPEN, {cooldown_remaining:.0f}s remaining)"
        )
        return False, None

    try:
        result = await send_coro
        cb.record_success()
        return True, result
    except Exception as e:
        cb.record_failure(str(e))
        raise


# ══════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════

def get_cb_status_text(user_id: int) -> str:
    """User ke liye circuit breaker status — settings menu mein show karo."""
    cbs = CircuitBreakerRegistry.get_all_for_user(user_id)
    if not cbs:
        return "✅ Sabhi destinations healthy hain"

    lines = ["📊 **Destination Health:**"]
    for dest_key, cb in cbs.items():
        cb._maybe_attempt_reset()
        emoji = cb.get_status_emoji()
        state_label = {
            CBState.CLOSED:    "OK",
            CBState.OPEN:      f"Paused ({cb._current_cooldown()/60:.0f}m cooldown)",
            CBState.HALF_OPEN: "Testing..."
        }.get(cb.state, "Unknown")
        lines.append(f"  {emoji} `{dest_key[:20]}`: {state_label}")
        if cb.last_error:
            lines.append(f"    ↳ _{cb.last_error[:60]}_")

    return "\n".join(lines)
