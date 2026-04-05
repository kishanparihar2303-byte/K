"""
web.py — Production Web Server for Render Deployment
Lightweight aiohttp server — health check + webhook support.
"""
import os
import time
import logging
from aiohttp import web

logger = logging.getLogger(__name__)

_start_time = time.time()


async def health_handler(request):
    uptime = int(time.time() - _start_time)
    h = uptime // 3600
    m = (uptime % 3600) // 60
    s = uptime % 60
    return web.Response(
        text=f"OK | uptime={h}h{m}m{s}s",
        content_type="text/plain",
        status=200
    )


async def root_handler(request):
    return web.Response(
        text="🤖 Telegram Forward Bot — Running",
        content_type="text/plain",
        status=200
    )


async def run_web_server():
    port = int(os.environ.get("PORT", 8080))
    try:
        # ── Web Panel import ──────────────────────────────────────────
        _panel_enabled = False
        try:
            from web_panel import register_panel_routes, auth_middleware
            _panel_enabled = True
            logger.info("✅ Web Panel loaded successfully")
        except Exception as e:
            # Broad except — real error log mein dikhega
            logger.error(f"❌ web_panel import FAILED: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

        # ── App ───────────────────────────────────────────────────────
        if _panel_enabled:
            app = web.Application(middlewares=[auth_middleware])
        else:
            app = web.Application()

        # ── Basic routes ──────────────────────────────────────────────
        app.router.add_get("/",       root_handler)
        app.router.add_get("/health", health_handler)
        app.router.add_get("/ping",   health_handler)

        # ── Panel routes ──────────────────────────────────────────────
        if _panel_enabled:
            register_panel_routes(app)
            logger.info("✅ Panel routes registered → /panel")

        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info(f"✅ Web server started on port {port}")
        if _panel_enabled:
            logger.info(f"🌐 Admin Panel → /panel")

    except OSError as e:
        logger.error(f"Web server port bind failed {port}: {e}")
        raise
    except Exception as e:
        logger.error(f"Web server error: {e}")
        raise


def start_web_server():
    from http.server import HTTPServer, BaseHTTPRequestHandler
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        def log_message(self, *args):
            pass
    port = int(os.environ.get("PORT", 8080))
    import threading
    srv = HTTPServer(("0.0.0.0", port), Handler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    logger.info(f"✅ Fallback web server on port {port}")
 
