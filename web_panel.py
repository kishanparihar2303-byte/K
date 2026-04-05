"""
web_panel.py — Admin Web Panel Backend API
==========================================

Ek powerful REST API jo admin web panel ke liye
sab data provide karta hai.

SETUP:
1. web_panel.py ko bot folder mein rakhein
2. PANEL_SECRET_KEY env var set karein (password)
3. web.py mein web_panel import karein (instructions neeche)

SECURITY:
- Har request mein X-Panel-Key header required hai
- CORS sirf allowed origins ke liye
- Rate limiting built-in
"""

import time
import json
import hashlib
import asyncio
import logging
from aiohttp import web

logger = logging.getLogger(__name__)

# ── Panel Password (env se lo ya yahan set karo) ─────────────────────────────
import os
from web import _start_time
_raw_secret = os.environ.get("PANEL_SECRET_KEY", "")
if not _raw_secret or _raw_secret.strip() == "":
    import logging as _wlog
    _wlog.getLogger(__name__).critical(
        "PANEL_SECRET_KEY env var not set! Web panel will reject all requests. "
        "Set a strong secret: export PANEL_SECRET_KEY='your-strong-password'"
    )
    # Refuse panel access entirely rather than using a weak default
    PANEL_SECRET = None
else:
    PANEL_SECRET = _raw_secret.strip()
PANEL_ALLOWED_ORIGINS = os.environ.get("PANEL_ORIGIN", "*")

# ── Rate Limiter ──────────────────────────────────────────────────────────────
_rate_limits: dict = {}
_rate_limits_last_cleanup: float = 0.0

def _rate_check(ip: str, max_req: int = 60, window: int = 60) -> bool:
    """Rate limiter — IP per minute. Auto-cleans stale entries to prevent memory leak."""
    global _rate_limits_last_cleanup
    now = time.time()

    # Cleanup stale IPs every 5 minutes
    if now - _rate_limits_last_cleanup > 300:
        stale = [k for k, v in list(_rate_limits.items()) if not v or v[-1] < now - window * 2]
        for k in stale:
            del _rate_limits[k]
        _rate_limits_last_cleanup = now

    if ip not in _rate_limits:
        _rate_limits[ip] = []
    _rate_limits[ip] = [t for t in _rate_limits[ip] if now - t < window]
    if len(_rate_limits[ip]) >= max_req:
        return False
    _rate_limits[ip].append(now)
    return True

# ── Auth Middleware ───────────────────────────────────────────────────────────
def _check_auth(request) -> bool:
    if PANEL_SECRET is None:
        return False  # No secret configured — reject all
    key = request.headers.get("X-Panel-Key", "")
    if not key:
        return False
    # Constant-time comparison to prevent timing attacks
    import hmac
    return hmac.compare_digest(key.encode(), PANEL_SECRET.encode())

def _cors_headers():
    return {
        "Access-Control-Allow-Origin": PANEL_ALLOWED_ORIGINS,
        "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, X-Panel-Key",
    }

def json_resp(data, status=200):
    return web.Response(
        text=json.dumps(data, ensure_ascii=False),
        content_type="application/json",
        status=status,
        headers=_cors_headers()
    )

def err_resp(msg, status=400):
    return json_resp({"error": msg}, status)

async def options_handler(request):
    return web.Response(headers=_cors_headers())

# ── MIDDLEWARE: Auth + Rate Limit ─────────────────────────────────────────────
@web.middleware
async def auth_middleware(request, handler):
    # Allow OPTIONS (CORS preflight)
    if request.method == "OPTIONS":
        return await options_handler(request)

    # Allow static panel HTML
    if request.path in ("/", "/panel", "/health", "/ping"):
        return await handler(request)

    # API routes need auth
    if request.path.startswith("/api/"):
        ip = request.remote or "unknown"
        if not _rate_check(ip):
            return err_resp("Rate limit exceeded", 429)
        if not _check_auth(request):
            return err_resp("Unauthorized — wrong panel key", 401)

    return await handler(request)

# ── PANEL HTML HANDLER ────────────────────────────────────────────────────────
_panel_html_path = os.path.join(os.path.dirname(__file__), "web_panel_frontend.html")

async def panel_html_handler(request):
    """Serve the admin panel HTML."""
    try:
        with open(_panel_html_path, "r", encoding="utf-8") as f:
            html = f.read()
        return web.Response(text=html, content_type="text/html", headers=_cors_headers())
    except FileNotFoundError:
        return web.Response(text="Panel HTML not found. Place web_panel_frontend.html in bot directory.", status=404)

# ── API: STATS ────────────────────────────────────────────────────────────────
async def api_stats(request):
    """Bot-wide stats — dashboard ke liye."""
    try:
        from database import db, GLOBAL_STATE, user_sessions
        from admin import get_system_stats, get_revenue_stats
        from config import OWNER_ID

        stats = get_system_stats()
        rev   = get_revenue_stats()

        # Sessions
        active_sessions = sum(
            1 for uid, client in user_sessions.items()
            if client and client.is_connected()
        )

        # Uptime
        try:
            from web import _start_time
            uptime = int(time.time() - _start_time)
        except Exception:
            uptime = 0

        # Bot username
        try:
            from config import bot as _bot
            me = await _bot.get_me()
            bot_username = f"@{me.username}" if me.username else "Bot"
        except Exception:
            bot_username = "@YourBot"

        return json_resp({
            "total_users":    stats["total_users"],
            "active_fwd":     stats["active_fwd"],
            "stopped_users":  stats["stopped_users"],
            "prem_count":     stats["prem_count"],
            "blocked":        stats["blocked"],
            "total_src":      stats["sources"],
            "total_dest":     stats["dest"],
            "m_mode":         stats["m_mode"],
            "new_today":      stats["new_today"],
            "new_week":       stats["new_week"],
            "revenue_month":  stats["revenue_month"],
            "rev_today":      rev["today"],
            "rev_total":      rev["total"],
            "pending_payments": rev["pending_count"],
            "sessions":       active_sessions,
            "uptime":         uptime,
            "owner_id":       str(OWNER_ID),
            "bot_username":   bot_username,
        })
    except Exception as e:
        logger.error(f"[WebPanel] /api/stats error: {e}")
        return err_resp(str(e), 500)


# ── API: USERS ────────────────────────────────────────────────────────────────
async def api_users(request):
    """All users list."""
    try:
        from database import db
        from premium import is_premium_user

        users_out = []
        for uid, udata in list(db.items()):
            prof = udata.get("profile", {})
            name = " ".join(filter(None, [
                prof.get("first_name", ""),
                prof.get("last_name", "")
            ])).strip() or f"User {uid}"
            users_out.append({
                "id":       str(uid),
                "name":     name,
                "username": prof.get("username", ""),
                "running":  udata.get("settings", {}).get("running", False),
                "premium":  bool(udata.get("premium", {}).get("active")),
                "sources":  len(udata.get("sources", [])),
                "dests":    len(udata.get("destinations", [])),
                "joined":   _fmt_ts(udata.get("joined_at", 0)),
                "blocked":  False,  # check separately
                "last_active": _fmt_ts(udata.get("last_active", 0)),
            })

        # Mark blocked users
        from database import GLOBAL_STATE
        blocked_set = set(GLOBAL_STATE.get("blocked_users", []))
        for u in users_out:
            u["blocked"] = int(u["id"]) in blocked_set

        # Sort by join date (newest first)
        users_out.sort(key=lambda x: x["joined"], reverse=True)
        return json_resp({"users": users_out, "total": len(users_out)})
    except Exception as e:
        logger.error(f"[WebPanel] /api/users error: {e}")
        return err_resp(str(e), 500)


# ── API: PAYMENTS ─────────────────────────────────────────────────────────────
async def api_payments(request):
    """Payment history and pending list."""
    try:
        from database import GLOBAL_STATE, db

        payments = GLOBAL_STATE.get("payment_history", [])
        out = []
        for p in reversed(payments[-100:]):  # Last 100
            uid = p.get("user_id", "")
            # Get user name
            user_data = db.get(uid) or db.get(int(uid)) or {}
            prof = user_data.get("profile", {})
            name = prof.get("first_name", "") or f"User {uid}"
            out.append({
                "id":        p.get("payment_id", ""),
                "user_id":   str(uid),
                "user_name": name,
                "plan":      p.get("plan_name", "Unknown"),
                "amount":    p.get("amount", 0),
                "status":    p.get("status", "pending"),
                "ts":        _fmt_ts(p.get("ts", 0)),
                "utr":       p.get("utr", ""),
            })
        return json_resp({"payments": out})
    except Exception as e:
        logger.error(f"[WebPanel] /api/payments error: {e}")
        return err_resp(str(e), 500)


# ── API: PAYMENT ACTIONS ──────────────────────────────────────────────────────
async def api_payment_approve(request):
    try:
        data = await request.json()
        pay_id = data.get("payment_id")
        from database import GLOBAL_STATE, save_persistent_db
        from config import bot, OWNER_ID

        payments = GLOBAL_STATE.get("payment_history", [])
        pay = next((p for p in payments if p.get("payment_id") == pay_id), None)
        if not pay:
            return err_resp("Payment not found")

        pay["status"] = "approved"
        save_persistent_db()

        # Give premium
        uid = int(pay.get("user_id", 0))
        days = int(pay.get("days", 30))
        if uid:
            try:
                from premium import give_premium
                give_premium(uid, days, plan_name=pay.get("plan_name", "Panel"))
                await bot.send_message(uid,
                    f"🎉 **Payment Approved!**\n\n"
                    f"💎 Premium activated for {days} days!\n"
                    f"Amount: ₹{pay.get('amount', 0)}\n\n"
                    "Thank you! ✨"
                )
            except Exception as e:
                logger.warning(f"Premium grant error: {e}")

        return json_resp({"ok": True, "payment_id": pay_id})
    except Exception as e:
        return err_resp(str(e), 500)


async def api_payment_reject(request):
    try:
        data = await request.json()
        pay_id = data.get("payment_id")
        reason = data.get("reason", "Payment rejected by admin")
        from database import GLOBAL_STATE, save_persistent_db
        from config import bot

        payments = GLOBAL_STATE.get("payment_history", [])
        pay = next((p for p in payments if p.get("payment_id") == pay_id), None)
        if not pay:
            return err_resp("Payment not found")

        pay["status"] = "rejected"
        save_persistent_db()

        uid = int(pay.get("user_id", 0))
        if uid:
            try:
                await bot.send_message(uid,
                    f"❌ **Payment Rejected**\n\n"
                    f"Reason: {reason}\n\n"
                    "Please contact admin for more info."
                )
            except Exception:
                pass

        return json_resp({"ok": True})
    except Exception as e:
        return err_resp(str(e), 500)


# ── API: USER ACTIONS ─────────────────────────────────────────────────────────
async def api_user_block(request):
    try:
        data = await request.json()
        uid = int(data.get("user_id", 0))
        from database import block_user
        block_user(uid)
        return json_resp({"ok": True, "action": "blocked", "user_id": uid})
    except Exception as e:
        return err_resp(str(e), 500)


async def api_user_unblock(request):
    try:
        data = await request.json()
        uid = int(data.get("user_id", 0))
        from database import unblock_user
        unblock_user(uid)
        return json_resp({"ok": True, "action": "unblocked", "user_id": uid})
    except Exception as e:
        return err_resp(str(e), 500)


async def api_user_premium(request):
    try:
        data = await request.json()
        uid   = int(data.get("user_id", 0))
        days  = int(data.get("days", 30))
        from premium import give_premium
        from config import bot
        give_premium(uid, days, plan_name="Admin Panel")
        try:
            await bot.send_message(uid,
                f"🎉 **Premium Activated!**\n\n"
                f"💎 You got **{days} days** premium!\n"
                "Enjoy all premium features! ✨"
            )
        except Exception:
            pass
        return json_resp({"ok": True, "user_id": uid, "days": days})
    except Exception as e:
        return err_resp(str(e), 500)


# ── API: LOGS ─────────────────────────────────────────────────────────────────
async def api_logs(request):
    """Admin action logs."""
    try:
        from database import admin_logs
        logs = list(reversed(admin_logs[-50:]))
        return json_resp({"logs": [
            {
                "time":    l.get("time", ""),
                "level":   "OK" if "success" in str(l.get("action","")).lower() else "INFO",
                "admin":   str(l.get("admin", "")),
                "action":  l.get("action", ""),
                "target":  str(l.get("target", "")),
                "details": l.get("details", ""),
                "msg":     f"{l.get('action','')} → {l.get('target','')}",
            }
            for l in logs
        ]})
    except Exception as e:
        return err_resp(str(e), 500)


# ── API: BROADCAST ────────────────────────────────────────────────────────────
async def api_broadcast(request):
    try:
        data   = await request.json()
        msg    = data.get("message", "")
        target = data.get("target", "all")

        if not msg:
            return err_resp("Message required")

        from database import db, GLOBAL_STATE
        from config import bot
        from admin import add_log

        # Choose users
        all_uids = list(db.keys())
        if target == "premium":
            uids = [u for u in all_uids if db[u].get("premium", {}).get("active")]
        elif target == "active" or "tActive" in target:
            uids = [u for u in all_uids if db[u].get("settings", {}).get("running")]
        elif target == "free":
            uids = [u for u in all_uids if not db[u].get("premium", {}).get("active")]
        else:
            uids = all_uids

        sent = 0; failed = 0
        for uid in uids:
            try:
                await bot.send_message(uid, msg, parse_mode="md")
                sent += 1
                await asyncio.sleep(0.05)  # Rate limit
            except Exception:
                failed += 1

        add_log(0, "Broadcast", target=f"{target} ({sent} users)", details=msg[:60])
        return json_resp({"ok": True, "sent": sent, "failed": failed, "target": target})
    except Exception as e:
        return err_resp(str(e), 500)


# ── API: BOT ACTIONS ──────────────────────────────────────────────────────────
async def api_action(request):
    try:
        data   = await request.json()
        action = data.get("action")
        from database import GLOBAL_STATE, save_persistent_db
        from admin import add_log, toggle_maintenance

        if action == "maintenance_on":
            GLOBAL_STATE["maintenance_mode"] = True
            save_persistent_db()
            add_log(0, "Maintenance ON", target="Global", details="Via Web Panel")
            return json_resp({"ok": True, "mode": "maintenance"})

        elif action == "maintenance_off":
            GLOBAL_STATE["maintenance_mode"] = False
            save_persistent_db()
            add_log(0, "Maintenance OFF", target="Global", details="Via Web Panel")
            return json_resp({"ok": True, "mode": "online"})

        elif action == "reg_close":
            GLOBAL_STATE["block_new_reg"] = True
            save_persistent_db()
            return json_resp({"ok": True})

        elif action == "reg_open":
            GLOBAL_STATE["block_new_reg"] = False
            save_persistent_db()
            return json_resp({"ok": True})

        elif action == "cleanup":
            from database import cleanup_inactive_users
            removed = cleanup_inactive_users()
            return json_resp({"ok": True, "removed": removed})

        elif action == "restart":
            import sys, os
            add_log(0, "Bot Restart", target="System", details="Via Web Panel")
            save_persistent_db()
            asyncio.create_task(_delayed_restart())
            return json_resp({"ok": True, "msg": "Restarting in 2 seconds..."})

        else:
            return err_resp(f"Unknown action: {action}")

    except Exception as e:
        return err_resp(str(e), 500)


async def _delayed_restart():
    await asyncio.sleep(2)
    import os, sys
    os.execv(sys.executable, [sys.executable] + sys.argv)


# ── API: SETTINGS ─────────────────────────────────────────────────────────────
async def api_settings(request):
    try:
        data  = await request.json()
        key   = data.get("key")
        value = data.get("value")
        from database import GLOBAL_STATE, save_persistent_db
        from admin import add_log

        setting_map = {
            "maintenance":  ("maintenance_mode", bool),
            "force_sub":    ("force_sub_enabled", bool),
            "anti_spam":    ("anti_spam_enabled", bool),
            "notice":       ("bot_notice", str),
            "footer":       ("bot_footer", str),
            "reg_open":     ("block_new_reg", lambda v: not v),
            "free_limits":  ("free_limits", dict),
        }

        if key in setting_map:
            gs_key, cast = setting_map[key]
            GLOBAL_STATE[gs_key] = cast(value) if callable(cast) else value
            save_persistent_db()
            add_log(0, f"Setting: {key}", target="Global", details=str(value)[:40])
            return json_resp({"ok": True, "key": key})
        else:
            return err_resp(f"Unknown setting: {key}")
    except Exception as e:
        return err_resp(str(e), 500)


# ── HEALTH ────────────────────────────────────────────────────────────────────
async def health_handler(request):
    uptime = int(time.time() - (_start_time if '_start_time' in dir() else time.time()))
    return web.Response(text=f"OK | panel=active | uptime={uptime}s",
                        content_type="text/plain",
                        headers=_cors_headers())


_start_time = time.time()


# ── REGISTER ROUTES ───────────────────────────────────────────────────────────
def register_panel_routes(app: web.Application):
    """
    Main web.py mein call karo:

        from web_panel import register_panel_routes
        register_panel_routes(app)
    """
    # Panel UI
    app.router.add_get("/panel",  panel_html_handler)
    app.router.add_get("/admin",  panel_html_handler)

    # API endpoints
    app.router.add_get ("/api/stats",             api_stats)
    app.router.add_get ("/api/users",             api_users)
    app.router.add_get ("/api/payments",          api_payments)
    app.router.add_get ("/api/logs",              api_logs)

    app.router.add_post("/api/payment/approve",   api_payment_approve)
    app.router.add_post("/api/payment/reject",    api_payment_reject)
    app.router.add_post("/api/user/block",        api_user_block)
    app.router.add_post("/api/user/unblock",      api_user_unblock)
    app.router.add_post("/api/user/premium",      api_user_premium)
    app.router.add_post("/api/broadcast",         api_broadcast)
    app.router.add_post("/api/action",            api_action)
    app.router.add_post("/api/settings",          api_settings)

    # CORS preflight
    app.router.add_route("OPTIONS", "/api/{tail:.*}", options_handler)

    logger.info("✅ Web Panel routes registered → /panel")


# ── HELPER ────────────────────────────────────────────────────────────────────
def _fmt_ts(ts) -> str:
    if not ts:
        return "-"
    try:
        import datetime
        return datetime.datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(ts)
