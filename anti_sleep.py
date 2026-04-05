"""
anti_sleep.py — Render Free Tier Sleep Prevention

Render free tier 10 min inactivity ke baad service sleep kar deta hai.
Ye module har 9 min mein ping karta hai — service alive rehti hai.

FIX: Replaced blocking sleep() in a daemon thread with a proper
asyncio coroutine that uses await asyncio.sleep() — no event loop blocking.
"""
import asyncio
import os
import logging

logger = logging.getLogger(__name__)

_PING_INTERVAL = 540   # 9 min (Render 10-min timeout se pehle)
_PING_TIMEOUT  = 10    # 10 second HTTP timeout
_MAX_FAILURES  = 5     # Consecutive failures ke baad warn karo


async def anti_sleep_loop():
    """
    Async coroutine — event loop block nahi karta.
    await asyncio.sleep() use karta hai blocking sleep() ki jagah.
    """
    url = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url:
        logger.info("RENDER_EXTERNAL_URL not set — anti-sleep disabled")
        return

    import aiohttp

    # Bot fully start hone ka wait (non-blocking)
    await asyncio.sleep(60)

    failures = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"{url}/health",
                    timeout=aiohttp.ClientTimeout(total=_PING_TIMEOUT),
                    headers={"User-Agent": "RenderAntiSleep/1.0"},
                ) as resp:
                    if resp.status == 200:
                        failures = 0
                        logger.debug("Anti-sleep ping OK")
                    else:
                        failures += 1
                        logger.warning(f"Anti-sleep ping: HTTP {resp.status}")
        except asyncio.CancelledError:
            break
        except Exception as e:
            failures += 1
            logger.debug(f"Anti-sleep ping failed ({failures}): {e}")

        if failures >= _MAX_FAILURES:
            logger.warning(
                f"Anti-sleep: {failures} consecutive failures — check RENDER_EXTERNAL_URL"
            )
            failures = 0

        await asyncio.sleep(_PING_INTERVAL)


def start_anti_sleep():
    """Schedule the async anti-sleep loop as a tracked asyncio task."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            task = asyncio.create_task(anti_sleep_loop(), name="anti_sleep")
            logger.info("✅ Anti-sleep async task started (9 min ping interval)")
            return task
    except Exception as e:
        logger.warning(f"Anti-sleep start failed: {e}")
    return None
