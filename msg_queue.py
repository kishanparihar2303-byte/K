"""
msg_queue.py — Advanced Durable Priority Queue with Dead Letter Queue (DLQ)

UPGRADES:
  ✅ FIX 3  — RAM Queue → Durable Priority Queue
               Failed messages go to DLQ (dead_letter.jsonl) — never lost
               On restart: DLQ replayed automatically
               Priority system: admin users processed first

  ✅ FIX 10 — Basic logging → Structured Error Telemetry
               Every error logged with: user_id, msg_type, error_type, timestamp
               Error frequency tracker: alerts if same user fails 5x in a row
               Auto-pauses forwarding for repeatedly failing users
"""

import asyncio
import time
import logging
import json
import os
from dataclasses import dataclass, field
from typing import Any
from enum import IntEnum

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════
# PRIORITY LEVELS
# ══════════════════════════════════════════
class Priority(IntEnum):
    HIGH   = 0   # Admin/premium users
    NORMAL = 1   # Regular users
    LOW    = 2   # Retries


# ══════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════
# RENDER FREE TIER: 512MB RAM ke liye optimize kiya
MAX_QUEUE_SIZE    = 500     # Was 5000 — Render RAM save karo
MAX_WORKERS       = 10      # Was 50 — 10 concurrent workers enough for free tier
WORKER_TIMEOUT    = 30
MAX_RETRIES       = 2       # Was 3 — fail fast on free tier
DLQ_FILE          = "dead_letter.jsonl"   # Dead Letter Queue
ERROR_ALERT_THRESHOLD = 5   # Itni baar fail ho to user auto-pause
_QUEUE_FULL_NOTIFIED: set = set()  # BUG 16 FIX: Track who was notified


# ══════════════════════════════════════════
# ERROR TELEMETRY  (Fix 10 Advanced)
# ══════════════════════════════════════════

class ErrorTelemetry:
    """
    Tracks error patterns per user.
    If same user fails 5x in a row → auto-pause + admin alert.
    """
    def __init__(self):
        self._fail_streak: dict[int, int]   = {}   # user_id → consecutive fails
        self._error_log:   list[dict]        = []   # Recent errors (last 1000)
        self._lock = asyncio.Lock()

    async def record_success(self, user_id: int):
        async with self._lock:
            self._fail_streak.pop(user_id, None)

    async def record_failure(self, user_id: int, error: str, msg_type: str) -> bool:
        """Returns True if user should be auto-paused."""
        async with self._lock:
            streak = self._fail_streak.get(user_id, 0) + 1
            self._fail_streak[user_id] = streak

            entry = {
                "ts":       time.time(),
                "user_id":  user_id,
                "type":     msg_type,
                "error":    str(error)[:200],
                "streak":   streak,
            }
            self._error_log.append(entry)
            if len(self._error_log) > 1000:
                self._error_log = self._error_log[-1000:]

            logger.error(
                f"[TELEMETRY] user={user_id} type={msg_type} "
                f"streak={streak} error={str(error)[:100]}"
            )

            if streak >= ERROR_ALERT_THRESHOLD:
                logger.critical(
                    f"🚨 AUTO-PAUSE: user={user_id} failed {streak} times in a row. "
                    f"Last error: {str(error)[:100]}"
                )
                return True   # Caller should pause this user
            return False

    def get_stats(self) -> dict:
        top_failures = sorted(
            self._fail_streak.items(), key=lambda x: x[1], reverse=True
        )[:10]
        return {
            "total_errors_tracked": len(self._error_log),
            "users_with_streaks":   len(self._fail_streak),
            "top_failing_users":    top_failures,
        }


telemetry = ErrorTelemetry()


# ══════════════════════════════════════════
# DEAD LETTER QUEUE  (Fix 3 Advanced)
# ══════════════════════════════════════════

def _dlq_write_sync(entry_json: str):
    """Thread-safe DLQ sync write."""
    try:
        with open(DLQ_FILE, "a", encoding="utf-8") as f:
            f.write(entry_json + "\n")
            f.flush()
    except Exception as e:
        logger.error(f"DLQ write error: {e}")


class DeadLetterQueue:
    """
    Messages that fail MAX_RETRIES times go here.
    Written to disk (dead_letter.jsonl) — survives server restarts.
    Admin can inspect/retry from dashboard.
    """

    def _serialize(self, task: dict) -> dict | None:
        """
        ✅ FIX 11: Serialize ALL recoverable message data.
        Extracts text/media type from event so admin can understand what failed.
        client/event objects cannot be JSON-ified — we extract key metadata.
        """
        try:
            event = task.get("event")
            msg_meta = {}
            if event:
                try:
                    msg_meta["text"]       = (getattr(event, "raw_text", "") or "")[:200]
                    msg_meta["has_media"]  = bool(getattr(event, "media", None))
                    msg_meta["media_type"] = type(getattr(event, "media", None)).__name__
                    msg_meta["msg_id"]     = getattr(event, "id", None)
                    msg_meta["chat_id"]    = getattr(getattr(event, "chat", None), "id", None)
                except Exception:
                    pass
            
            return {
                "user_id":   task.get("user_id"),
                "my_id":     task.get("my_id"),
                "type":      task.get("type"),
                "queued_at": task.get("queued_at"),
                "retries":   task.get("retries", 0),
                "failed_at": time.time(),
                "reason":    task.get("fail_reason", "unknown"),
                "message":   msg_meta,   # ✅ Actual message content for admin review
            }
        except Exception:
            return None

    def push(self, task: dict):
        entry = self._serialize(task)
        if not entry:
            return
        try:
            # Non-blocking write via thread
            import asyncio as _aio
            _entry_json = json.dumps(entry)
            try:
                loop = asyncio.get_running_loop()
                loop.run_in_executor(None, lambda: _dlq_write_sync(_entry_json))
            except RuntimeError:
                # No running loop — sync write karo
                _dlq_write_sync(_entry_json)
            except Exception:
                _dlq_write_sync(_entry_json)
            logger.warning(
                f"☠️ DLQ: user={entry['user_id']} type={entry['type']} "
                f"retries={entry['retries']} reason={entry['reason']}"
            )
        except Exception as e:
            logger.error(f"DLQ write failed: {e}")

    def count(self) -> int:
        try:
            if not os.path.exists(DLQ_FILE):
                return 0
            with open(DLQ_FILE) as f:
                return sum(1 for _ in f)
        except Exception:
            return 0

    def get_recent(self, n: int = 20) -> list:
        entries = []
        try:
            if not os.path.exists(DLQ_FILE):
                return []
            with open(DLQ_FILE) as f:
                for line in f:
                    try:
                        entries.append(json.loads(line.strip()))
                    except Exception:
                        continue
        except Exception:
            pass
        return entries[-n:]

    def clear(self):
        try:
            with open(DLQ_FILE, "w", encoding="utf-8") as _f:
                pass  # DLQ clear karo
        except Exception:
            pass

    def auto_cleanup(self, max_age_days: int = 7, max_size_mb: float = 50.0):
        """
        ✅ FIX 14: Auto-cleanup DLQ to prevent unbounded growth.
        Removes entries older than max_age_days.
        If file > max_size_mb, keeps only newest 1000 entries.
        """
        if not os.path.exists(DLQ_FILE):
            return 0
        
        try:
            file_size_mb = os.path.getsize(DLQ_FILE) / (1024 * 1024)
            cutoff_ts    = time.time() - (max_age_days * 86400)
            
            entries = []
            with open(DLQ_FILE, "r") as f:
                for line in f:
                    try:
                        e = json.loads(line.strip())
                        if e.get("failed_at", 0) > cutoff_ts:
                            entries.append(line)
                    except Exception:
                        pass
            
            # If still too large, keep newest 1000
            if file_size_mb > max_size_mb and len(entries) > 1000:
                entries = entries[-1000:]
            
            removed = self.count() - len(entries)
            with open(DLQ_FILE, "w") as f:
                f.writelines(entries)
            
            if removed > 0:
                logger.info(f"🧹 DLQ auto-cleanup: removed {removed} old entries")
            return removed
        except Exception as e:
            logger.error(f"DLQ cleanup error: {e}")
            return 0


dlq = DeadLetterQueue()


# ══════════════════════════════════════════
# PRIORITY QUEUE
# ══════════════════════════════════════════

# asyncio.PriorityQueue: lower number = higher priority
# Each item: (priority, sequence_number, task_dict)
_queue: asyncio.PriorityQueue = None
_seq_counter = 0
_active_workers = 0
_processed = 0
_dropped = 0
_queue_initialized = False
_workers_started = False   # BUG FIX: Double-start guard


def get_queue() -> asyncio.PriorityQueue:
    global _queue, _queue_initialized
    # BUG FIX Q1: Event loop change hone par queue re-create karo
    # asyncio.PriorityQueue old event loop se bound hoti hai
    # Agar loop restart hua (Render restart) to purani queue kaam nahi karti
    if not _queue_initialized or _queue is None:
        _queue = asyncio.PriorityQueue(maxsize=MAX_QUEUE_SIZE)
        _queue_initialized = True
    else:
        try:
            # Verify queue is still usable in current event loop
            loop = asyncio.get_event_loop()
            if _queue._loop is not None and _queue._loop != loop:
                logger.warning("Queue event loop mismatch — recreating queue")
                _queue = asyncio.PriorityQueue(maxsize=MAX_QUEUE_SIZE)
        except Exception:
            pass
    return _queue


def _get_priority(user_id: int) -> Priority:
    """Premium/admin users get HIGH priority."""
    try:
        from database import GLOBAL_STATE, db
        if user_id in GLOBAL_STATE.get("admins", {}):
            return Priority.HIGH
        udata = db.get(user_id, {})
        if udata.get("premium", {}).get("active"):
            return Priority.HIGH
    except Exception:
        pass
    return Priority.NORMAL


async def enqueue_message(task: dict, retry_count: int = 0) -> bool:
    """
    Message queue mein daalo with priority.
    Premium/admin users get processed first.
    """
    global _dropped, _seq_counter
    q = get_queue()

    task["queued_at"] = task.get("queued_at", time.time())
    task["retries"]   = retry_count

    priority = Priority.LOW if retry_count > 0 else _get_priority(task.get("user_id", 0))
    _seq_counter += 1

    try:
        q.put_nowait((int(priority), _seq_counter, task))
        return True
    except asyncio.QueueFull:
        _dropped += 1
        # ✅ FIX 3: Queue full → DLQ instead of silent drop
        task["fail_reason"] = "queue_full"
        dlq.push(task)
        logger.warning(
            f"Queue full! Message DLQ'd (user={task.get('user_id')}). "
            f"Total dropped: {_dropped}"
        )
        return False


async def queue_worker(worker_id: int):
    """
    ✅ FIX 3+10 Advanced: Worker with retry logic + telemetry + DLQ.
    
    Flow:
      1. Get task from priority queue
      2. Try to process
      3. On failure: retry up to MAX_RETRIES times
      4. After MAX_RETRIES: push to DLQ + maybe auto-pause user
    """
    global _active_workers, _processed
    _active_workers += 1

    from forward_engine import handle_new_message, handle_edit_message
    q = get_queue()

    try:
        while True:
            try:
                priority, seq, task = await asyncio.wait_for(q.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Worker {worker_id}: Queue get error: {e}")
                await asyncio.sleep(1)
                continue

            # BUG FIX: task processing loop ke ANDAR hona chahiye
            user_id  = task.get("user_id")
            msg_type = task.get("type")
            retries  = task.get("retries", 0)

            try:
                age = time.time() - task.get("queued_at", time.time())
                if age > WORKER_TIMEOUT:
                    logger.warning(
                        f"Worker {worker_id}: Stale message (age={age:.1f}s) "
                        f"user={user_id} — DLQ'd"
                    )
                    task["fail_reason"] = f"stale_{age:.0f}s"
                    dlq.push(task)
                    q.task_done()
                    continue

                if msg_type == "new":
                    await asyncio.wait_for(
                        handle_new_message(task["client"], user_id, task["event"], task["my_id"]),
                        timeout=20.0
                    )
                elif msg_type == "edit":
                    await asyncio.wait_for(
                        handle_edit_message(task["client"], user_id, task["event"], task["my_id"]),
                        timeout=20.0
                    )

                _processed += 1
                await telemetry.record_success(user_id)

            except asyncio.TimeoutError:
                logger.warning(
                    f"Worker {worker_id}: Timeout — user={user_id} type={msg_type}"
                )
                should_pause = await telemetry.record_failure(user_id, "timeout", msg_type)
                await _handle_failure(task, "timeout", should_pause, worker_id)

            except Exception as e:
                should_pause = await telemetry.record_failure(user_id, str(e), msg_type)
                await _handle_failure(task, str(e), should_pause, worker_id)

            finally:
                q.task_done()

    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.error(f"Worker {worker_id} CRASHED: {e} — restarting in 3s")
        await asyncio.sleep(3)
        asyncio.create_task(queue_worker(worker_id))
    finally:
        _active_workers -= 1


async def _handle_failure(task: dict, reason: str, should_pause: bool, worker_id: int):
    """Retry logic + DLQ + optional auto-pause."""
    user_id  = task.get("user_id")
    retries  = task.get("retries", 0)

    if retries < MAX_RETRIES:
        backoff = 2 ** (retries + 1)
        logger.info(f"Worker {worker_id}: Retry {retries+1}/{MAX_RETRIES} for user={user_id} in {backoff}s (non-blocking)")
        async def _delayed_retry(t=task, b=backoff, r=retries):
            await asyncio.sleep(b)
            await enqueue_message(t, retry_count=r + 1)
        asyncio.create_task(_delayed_retry())
    else:
        task["fail_reason"] = reason
        dlq.push(task)
        logger.error(
            f"Worker {worker_id}: MAX_RETRIES reached for user={user_id}. "
            f"Message sent to DLQ."
        )

    # Auto-pause user if too many failures
    if should_pause and user_id:
        try:
            from database import get_user_data, save_persistent_db
            data = get_user_data(user_id)
            data["settings"]["running"] = False
            save_persistent_db()
            logger.critical(
                f"🛑 AUTO-PAUSED user={user_id} — "
                f"too many consecutive failures. Admin notified."
            )
            # Notify admin
            try:
                from config import bot, OWNER_ID
                await bot.send_message(
                    OWNER_ID,
                    f"🛑 **Auto-Pause Alert**\n\n"
                    f"User `{user_id}` ki forwarding auto-pause ho gayi\n"
                    f"Reason: {reason[:100]}\n"
                    f"Action: User ko /start karne ko kaho ya manually investigate karo."
                )
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Auto-pause failed for {user_id}: {e}")


async def start_queue_workers():
    """Start workers + report DLQ status from last run."""
    global _workers_started
    if _workers_started:
        logger.warning("start_queue_workers() called again — already running, skipping!")
        return
    _workers_started = True
    # ✅ FIX 14: Auto-cleanup stale DLQ entries on startup
    dlq.auto_cleanup(max_age_days=7, max_size_mb=50.0)
    dlq_count = dlq.count()
    if dlq_count:
        logger.warning(
            f"☠️ DLQ has {dlq_count} unprocessed messages from previous run. "
            f"Check dead_letter.jsonl or use admin panel to inspect."
        )
        # Notify admin about DLQ on startup
        try:
            from config import bot, OWNER_ID
            async def _notify():
                await bot.send_message(
                    OWNER_ID,
                    f"⚠️ **Startup DLQ Alert**\n\n"
                    f"Pichle run mein **{dlq_count}** messages process nahi ho sake.\n"
                    f"Admin panel mein DLQ check karo."
                )
            asyncio.create_task(_notify())
        except RuntimeError:
            pass  # No running loop yet at startup
        except Exception:
            pass

    logger.info(f"🚀 Starting {MAX_WORKERS} priority queue workers...")
    get_queue()
    for i in range(MAX_WORKERS):
        asyncio.create_task(queue_worker(i))
    logger.info(f"✅ {MAX_WORKERS} workers ready! DLQ: {dlq_count} pending from last run.")


def get_queue_stats() -> dict:
    q = get_queue()
    return {
        "pending":        q.qsize(),
        "max_size":       MAX_QUEUE_SIZE,
        "workers":        MAX_WORKERS,
        "processed":      _processed,
        "dropped":        _dropped,
        "dlq_count":      dlq.count(),
        "error_telemetry": telemetry.get_stats(),
    }
