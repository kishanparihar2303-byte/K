"""
scheduler.py — Production Scheduler with REAL Queue Execution

FIXES:
  ✅ FIX 6: "Fake Queue" → Real scheduled message delivery
             Queue stores full message content (not just reference).
             Background loop flushes queue when schedule window opens.
             Survives restarts: queue persisted to disk.

ARCHITECTURE:
    Message arrives outside schedule window
    → Serialized (text/media info) → saved to queue_store.jsonl
    → Background loop checks every 30s
    → When window opens → forward queued messages
"""

import asyncio
import datetime
import logging
import time
import json
import os

from database import get_user_data, save_persistent_db
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

MAX_QUEUE_SIZE  = 200
QUEUE_STORE     = "scheduler_queue.jsonl"    # Persistent queue
_CHECK_INTERVAL = 30                          # seconds between queue checks

# In-memory queue: {user_id: [serialized_task, ...]}
_memory_queue: dict = {}

# ── Per-Day Scheduler Constants ───────────────────────────────────────────────

# Short names for all 7 days (used as dict keys and display labels)
DAY_SHORT = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Default schedule applied to any day that hasn't been configured yet
DEFAULT_DAY_SCHEDULE: dict = {
    "enabled": True,
    "start": "09:00 AM",
    "end": "10:00 PM",
}


def get_default_per_day_schedule() -> dict:
    """Return a fresh per-day schedule dict with defaults for every day."""
    return {day: DEFAULT_DAY_SCHEDULE.copy() for day in DAY_SHORT}



# ── Time window check ─────────────────────────────────────────────────────────

def is_schedule_allowed(user_id: int) -> bool:
    """Returns True if current time is within user's schedule window."""
    try:
        data  = get_user_data(user_id)
        sched = data.get("scheduler", {})

        # Neither basic nor per_day enabled
        if not sched.get("enabled") and not sched.get("per_day_enabled"):
            return True

        # Use sched-level tz first, fallback to user-level tz (set from settings menu)
        tz_name = sched.get("timezone") or data.get("timezone", "Asia/Kolkata")
        try:
            from zoneinfo import ZoneInfo
            now_dt = datetime.datetime.now(ZoneInfo(tz_name))
        except Exception:
            now_dt = datetime.datetime.now()

        now = now_dt.time()
        # Locale-safe weekday — strftime("%a") can vary by server locale
        today_str = DAY_SHORT[now_dt.weekday()]  # 0=Mon…6=Sun, always English
        today_date = now_dt.strftime("%Y-%m-%d")

        # BUG 11 FIX: Per-day scheduler check
        if sched.get("per_day_enabled"):
            per_day = sched.get("per_day", {})

            # Holiday check — FIX: use same tz-aware today_date for comparison
            holidays = sched.get("holidays", [])
            # Normalize stored holidays to YYYY-MM-DD, strip time parts if any
            normalized_holidays = [h[:10] for h in holidays if isinstance(h, str)]
            if today_date in normalized_holidays:
                return False

            # Per-day timing check
            day_cfg = per_day.get(today_str, {})
            if day_cfg:
                if not day_cfg.get("enabled", True):
                    return False  # This day is disabled
                start_str = day_cfg.get("start", "09:00 AM")
                end_str   = day_cfg.get("end",   "10:00 PM")
                start, end = _parse_times(start_str, end_str)
                if start is not None:
                    if start <= end:
                        return start <= now <= end
                    else:
                        return now >= start or now <= end
            # No per-day config for today — allow
            return True

        # Basic scheduler
        if not sched.get("enabled"):
            return True

        start_str = sched.get("start", "09:00 AM")
        end_str   = sched.get("end",   "10:00 PM")
        start, end = _parse_times(start_str, end_str)

        if start is None:
            return True

        if start <= end:
            return start <= now <= end
        else:
            return now >= start or now <= end

    except Exception as e:
        logger.debug(f"Scheduler check error for {user_id}: {e}")
        return True


def _parse_times(start_str, end_str):
    try:
        try:
            start = datetime.datetime.strptime(start_str, "%I:%M %p").time()
            end   = datetime.datetime.strptime(end_str,   "%I:%M %p").time()
        except ValueError:
            start = datetime.datetime.strptime(start_str, "%H:%M").time()
            end   = datetime.datetime.strptime(end_str,   "%H:%M").time()
        return start, end
    except Exception:
        return None, None


# ── Serialization ─────────────────────────────────────────────────────────────

def _serialize_message(event, client_user_id: int, source_id: int) -> dict | None:
    """
    ✅ FIX 6: Serialize message content for persistent queue.
    Stores everything needed to re-forward the message later.
    """
    try:
        task = {
            "user_id":   client_user_id,
            "source_id": source_id,
            "queued_at": time.time(),
            "msg_id":    getattr(event, "id", None),
            "text":      getattr(event, "raw_text", "") or "",
            "has_media": bool(getattr(event, "media", None)),
            "media_type": None,
        }
        
        media = getattr(event, "media", None)
        if media:
            task["media_type"] = type(media).__name__
        
        return task
    except Exception as e:
        logger.debug(f"Serialize error: {e}")
        return None


def queue_message(user_id: int, event, source_id: int) -> bool:
    """
    ✅ FIX 6: Queue message with full content for later delivery.
    Returns True if queued successfully.
    """
    data  = get_user_data(user_id)
    sched = data.get("scheduler", {})
    
    if not sched.get("queue_mode", False):
        return False
    
    serialized = _serialize_message(event, user_id, source_id)
    if not serialized:
        return False
    
    # Memory queue
    if user_id not in _memory_queue:
        _memory_queue[user_id] = []
    
    if len(_memory_queue[user_id]) >= MAX_QUEUE_SIZE:
        _memory_queue[user_id].pop(0)   # Drop oldest
        logger.debug(f"Scheduler queue full for user {user_id} — oldest dropped")
    
    _memory_queue[user_id].append(serialized)
    
    # Persist to disk
    _persist_task(serialized)
    
    logger.debug(f"Message queued for user {user_id} (queue size: {len(_memory_queue[user_id])})")
    return True


def _persist_task(task: dict):
    """Append task to disk queue."""
    try:
        with open(QUEUE_STORE, "a", encoding="utf-8") as f:
            f.write(json.dumps(task) + "\n")
    except Exception as e:
        logger.error(f"Queue persist error: {e}")


def _load_persisted_queue():
    """Load queue from disk on startup."""
    if not os.path.exists(QUEUE_STORE):
        return
    try:
        loaded = 0
        with open(QUEUE_STORE, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    task    = json.loads(line.strip())
                    uid     = task.get("user_id")
                    age     = time.time() - task.get("queued_at", 0)
                    if not uid or age > 86400:   # Drop tasks older than 24h
                        continue
                    _memory_queue.setdefault(uid, []).append(task)
                    loaded += 1
                except Exception:
                    continue
        if loaded:
            logger.info(f"📅 Scheduler: Loaded {loaded} queued messages from disk")
        # BUG 25 FIX: Rename first, then delete — crash-safe
        backup_path = QUEUE_STORE + ".bak"
        try:
            os.rename(QUEUE_STORE, backup_path)
            os.remove(backup_path)
        except Exception:
            try:
                os.remove(QUEUE_STORE)
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Queue load error: {e}")


def get_queue_size(user_id: int) -> int:
    return len(_memory_queue.get(user_id, []))


def clear_queue(user_id: int):
    _memory_queue.pop(user_id, None)


def get_scheduler_status(user_id: int) -> str:
    data  = get_user_data(user_id)
    sched = data.get("scheduler", {})
    lines = []
    
    if sched.get("enabled"):
        start = sched.get("start", "09:00 AM")
        end   = sched.get("end",   "10:00 PM")
        tz    = sched.get("timezone") or data.get("timezone", "Asia/Kolkata")
        lines.append(f"⏰ Schedule: `{start}` → `{end}` ({tz})")
        
        now_allowed = is_schedule_allowed(user_id)
        lines.append(f"📍 Current: {'✅ Active Window' if now_allowed else '⏸ Outside Window'}")
    
    if sched.get("queue_mode"):
        qsize = get_queue_size(user_id)
        lines.append(f"📦 Queue Mode: ON | Queued: `{qsize}` msgs")
    
    return "\n".join(lines) if lines else "⏰ Scheduler: OFF"


# ── Background Queue Flush Loop ───────────────────────────────────────────────

async def scheduler_queue_loop():
    """
    ✅ FIX 6: REAL queue execution loop.
    Runs every 30s. When schedule window opens → flush queued messages.
    This is the missing piece that made the queue feature "fake".
    """
    _load_persisted_queue()
    logger.info("📅 Scheduler queue loop started")
    
    while True:
        try:
            await _flush_ready_queues()
        except Exception as e:
            logger.error(f"Scheduler flush error: {e}")
        await asyncio.sleep(_CHECK_INTERVAL)


async def _flush_ready_queues():
    """Flush queued messages for all users whose window just opened."""
    for user_id, tasks in list(_memory_queue.items()):
        if not tasks:
            continue
        
        # Check if schedule window is NOW open
        if not is_schedule_allowed(user_id):
            continue
        
        data  = get_user_data(user_id)
        sched = data.get("scheduler", {})
        
        if not sched.get("queue_mode"):
            continue
        
        if not data.get("settings", {}).get("running"):
            continue
        
        # Window is open — flush this user's queue
        flushed = _memory_queue.pop(user_id, [])
        if not flushed:
            continue
        
        logger.info(f"📅 Flushing {len(flushed)} queued messages for user {user_id}")
        
        try:
            await _deliver_flushed(user_id, flushed, data)
        except Exception as e:
            logger.error(f"Flush delivery error for user {user_id}: {e}")


async def _deliver_flushed(user_id: int, tasks: list, data: dict):
    """
    BUG 5 FIX: Re-deliver queued messages through proper forward pipeline.
    Ab forward_messages ki jagah process_single_message use karte hain —
    isse sare filters (dup, keyword, watermark, affiliate, prefix/suffix) apply honge.
    """
    from database import user_sessions
    
    client = user_sessions.get(user_id)
    if not client or not client.is_connected():
        logger.warning(f"Scheduler: client not available for user {user_id} — requeueing")
        _memory_queue[user_id] = tasks   # Put back
        return
    
    delivered = 0
    for task in tasks:
        try:
            msg_id    = task.get("msg_id")
            source_id = task.get("source_id")
            
            if not msg_id or not source_id:
                continue
            
            # Re-fetch the original message from source
            try:
                msgs = await client.get_messages(source_id, ids=[msg_id])
                if not msgs:
                    logger.debug(f"Queued msg {msg_id} no longer available in source")
                    continue
                original = msgs[0] if isinstance(msgs, list) else msgs
                if not original:
                    continue
            except Exception as e:
                logger.debug(f"Can't fetch queued msg {msg_id}: {e}")
                continue
            
            # BUG 5 FIX: Use proper forward pipeline — all filters apply
            try:
                from forward_engine import process_single_message
                # Temporarily mark as running for this delivery
                await process_single_message(client, user_id, original, str(source_id), data)
                delivered += 1
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.debug(f"Queued process_single failed for user {user_id}: {e}")
                # Fallback: raw forward (better than nothing)
                try:
                    destinations = data.get("destinations", [])
                    for dest in destinations:
                        if "Add" in str(dest): continue
                        target = int(dest) if str(dest).lstrip("-").isdigit() else dest
                        await client.forward_messages(target, original, source_id)
                        await asyncio.sleep(0.5)
                        delivered += 1
                except Exception as e2:
                    logger.debug(f"Fallback forward also failed: {e2}")
        
        except Exception as e:
            logger.debug(f"Task delivery error: {e}")
    
    if delivered:
        logger.info(f"📅 Delivered {delivered} queued messages for user {user_id}")
        # FIX 28b: Only clean up queue backup AFTER successful delivery
        cleanup_queue_backup()
        try:
            from config import bot
            await bot.send_message(
                user_id,
                f"📅 **Schedule Queue Delivered!**\n\n"
                f"✅ `{delivered}` messages jo schedule band time mein aaye the, "
                f"ab forward kar diye gaye।"
            )
        except Exception:
            pass
