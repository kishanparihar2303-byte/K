from time_helper import ab_fmt as _ab_fmt
# bot/health_monitor.py
# ==========================================
# HEALTH MONITOR + ALERT SYSTEM
# Bot ON/OFF, RAM, Network, Server alerts
# Admin + Users dono ko notify karta hai
# ==========================================

import asyncio
import datetime
import os
import platform
import time
import traceback

import psutil
from telethon import errors

from config import bot, OWNER_ID, ADMINS, logger
from database import db, GLOBAL_STATE, get_user_data, save_persistent_db

# ==========================================
# ALERT SETTINGS
# ==========================================

HEALTH_CONFIG = {
    "ram_alert_percent": 75,   # RENDER 512MB: Alert at 75% = 384MB
    "check_interval_sec": 120,
    "notify_users_on_down": True,
    "notify_users_on_up": True,
    "last_alert_time": {},
    "alert_cooldown_sec": 1800,
}

def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_alert_config():
    """
    Alert destinations config — GLOBAL_STATE mein store hoti hai।
    Admin panel se channel/group/bot set kar sakte ho।
    """
    GLOBAL_STATE.setdefault("alert_config", {
        "destinations": [],   # List of {id, name, type}
        # type: "bot" = bot private message, "channel" = channel, "group" = group
    })
    return GLOBAL_STATE["alert_config"]

# Bot start time track karo
BOT_START_TIME = time.time()
_prev_net_bytes = None  # Network speed track karne ke liye


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_uptime_str():
    """Bot kitni der se chal raha hai — readable format mein."""
    elapsed = int(time.time() - BOT_START_TIME)
    hours, rem = divmod(elapsed, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _get_system_health_sync():
    """Synchronous health data collection — called via run_in_executor, never directly."""
    health = {}

    # RAM — Bot process ki actual RAM
    try:
        process = psutil.Process()
        proc_mem = process.memory_info()
        health["bot_ram_mb"] = round(proc_mem.rss / 1024 / 1024, 1)  # Bot ki actual RAM
        
        # Server total (info only)
        ram = psutil.virtual_memory()
        # FIX E: Use Render 512MB limit for accurate % calculation
        _limit_mb = int(os.environ.get("RENDER_RAM_LIMIT_MB", "512"))
        health["ram_used_percent"] = round((health["bot_ram_mb"] / _limit_mb) * 100, 1)
        health["ram_used_mb"] = health["bot_ram_mb"]
        health["ram_total_mb"] = _limit_mb
        health["ram_free_mb"] = round(max(0, _limit_mb - health["bot_ram_mb"]), 1)
    except Exception:
        health["ram_used_percent"] = 0
        health["ram_used_mb"] = 0
        health["bot_ram_mb"] = 0
        health["ram_total_mb"] = 0

    # CPU — sirf is process ka CPU (server ka nahi)
    try:
        process = psutil.Process()
        health["cpu_percent"] = round(process.cpu_percent(interval=0.5), 1)
        health["cpu_server"] = psutil.cpu_percent(interval=None)  # Server total (info only)
    except Exception:
        health["cpu_percent"] = 0
        health["cpu_server"] = 0

    # Disk
    try:
        disk = psutil.disk_usage("/")
        health["disk_used_percent"] = disk.percent
        health["disk_free_gb"] = round(disk.free / 1024 / 1024 / 1024, 2)
    except Exception:
        health["disk_used_percent"] = 0
        health["disk_free_gb"] = 0

    # Network (bytes sent/recv since last check)
    global _prev_net_bytes
    try:
        net = psutil.net_io_counters()
        if _prev_net_bytes:
            sent_kb = round((net.bytes_sent - _prev_net_bytes[0]) / 1024, 1)
            recv_kb = round((net.bytes_recv - _prev_net_bytes[1]) / 1024, 1)
            health["net_sent_kb"] = sent_kb
            health["net_recv_kb"] = recv_kb
        else:
            health["net_sent_kb"] = 0
            health["net_recv_kb"] = 0
        _prev_net_bytes = (net.bytes_sent, net.bytes_recv)
    except Exception:
        health["net_sent_kb"] = 0
        health["net_recv_kb"] = 0

    health["uptime"] = get_uptime_str()
    health["platform"] = platform.system()
    health["checked_at"] = _ab_fmt(None, "%d/%m/%Y %H:%M:%S")

    return health


async def get_system_health() -> dict:
    """
    Async wrapper for health data collection.
    Runs blocking psutil calls in a thread pool to avoid freezing the event loop.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _get_system_health_sync)


def can_send_alert(alert_key: str) -> bool:
    """Duplicate alert avoid karo — cooldown check."""
    now = time.time()
    last = HEALTH_CONFIG["last_alert_time"].get(alert_key, 0)
    if now - last >= HEALTH_CONFIG["alert_cooldown_sec"]:
        HEALTH_CONFIG["last_alert_time"][alert_key] = now
        return True
    return False


def get_status_emoji(percent: float) -> str:
    if percent >= 90:
        return "🔴"
    elif percent >= 75:
        return "🟡"
    return "🟢"


# ==========================================
# ALERT SENDERS
# ==========================================

async def alert_admins(message: str, parse_mode: str = None):
    """
    Alerts bhejo — pehle custom destinations check karo,
    agar koi nahi set to sabhi admins ko directly bhejo।
    """
    alert_cfg = get_alert_config()
    destinations = alert_cfg.get("destinations", [])

    if destinations:
        # Custom destinations set hain — unhe bhejo
        for dest in destinations:
            dest_id = dest.get("id")
            if not dest_id:
                continue
            try:
                await bot.send_message(int(dest_id), message)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.warning(f"Alert send failed to {dest_id}: {e}")
    else:
        # Default: sabhi admins ko directly bhejo
        admin_ids = list(GLOBAL_STATE.get("admins", {}).keys())
        if OWNER_ID not in admin_ids:
            admin_ids.append(OWNER_ID)
        for admin_id in admin_ids:
            try:
                await bot.send_message(admin_id, message)
                await asyncio.sleep(0.3)
            except Exception as e:
                logger.warning(f"Alert send failed to admin {admin_id}: {e}")


async def alert_active_users(message: str):
    """Sirf un users ko notify karo jinki forwarding chal rahi thi."""
    if not HEALTH_CONFIG.get("notify_users_on_down", True):
        return

    notified = 0
    for uid, udata in list(db.items()):
        if isinstance(udata, dict) and udata.get("settings", {}).get("running"):
            try:
                await bot.send_message(int(uid), message)
                notified += 1
                await asyncio.sleep(0.3)
            except Exception:
                pass

    return notified


# ==========================================
# BOT STARTUP ALERT
# ==========================================

async def send_startup_alert():
    """Bot start hone par ek hi alert bhejo — duplicate nahi."""
    await asyncio.sleep(3)  # Bot fully start hone do

    health = await get_system_health()
    ram_emoji = get_status_emoji(health["ram_used_percent"])

    active_count = sum(
        1 for u in list(db.values())
        if isinstance(u, dict) and u.get("settings", {}).get("running")
    )

    # Sirf notification_center se bhejo — ek hi message, system stats ke saath
    from notification_center import _send as _nc_send, _footer as _nc_footer
    msg = (
        "🟢 **BOT ONLINE ALERT**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ Bot successfully started!\n"
        f"🕒 Time: `{health['checked_at']}`\n\n"
        f"📊 **System Status:**\n"
        f"{ram_emoji} RAM: `{health['ram_used_mb']}MB / {health['ram_total_mb']}MB` ({health['ram_used_percent']}%)\n"
        f"💻 CPU: `{health['cpu_percent']}%`\n"
        f"💾 Disk Free: `{health['disk_free_gb']} GB`\n"
        f"👥 Active users: `{active_count}`\n"
        f"🖥 Platform: `{health['platform']}`"
    )
    # Admin ko ek hi baar bhejo
    await _nc_send("bot_online", msg, who="admin")

    # Active users ko alag message (sirf agar active users hain)
    if active_count > 0:
        await _nc_send("user_bot_online",
            "✅ **Bot Wapas Online Hai!**\n\n"
            "Bot thodi der ke liye offline tha, ab wapas chal raha hai.\n"
            "Tumhari forwarding automatically resume ho gayi hai.",
            who="active_users"
        )


# ==========================================
# RAM ALERT
# ==========================================

async def check_and_alert_ram(health: dict):
    """RAM zyada ho to admin ko alert karo."""
    ram_pct = health["ram_used_percent"]
    if ram_pct >= HEALTH_CONFIG["ram_alert_percent"]:
        if can_send_alert("ram_high"):
            msg = (
                "🔴 **HIGH RAM ALERT!**\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                f"⚠️ RAM usage bahut high hai!\n\n"
                f"📊 RAM Used: `{health['ram_used_mb']}MB / {health['ram_total_mb']}MB`\n"
                f"📈 Usage: **{ram_pct}%**\n"
                f"🕒 Time: `{health['checked_at']}`\n\n"
                f"⚡ Bot slow ho sakta hai ya crash ho sakta hai.\n"
                f"💡 Kuch inactive users ka session band karo ya Render redeploy karo.\n\n" + _get_owner_footer()
            )
            await alert_admins(msg)


# ==========================================
# NETWORK ALERT
# ==========================================

# FIX B: Global session for health checks (no new TCP per check)
_hm_session = None

async def _get_hm_session():
    global _hm_session
    import aiohttp
    if _hm_session is None or _hm_session.closed:
        _hm_session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=8)
        )
    return _hm_session


async def check_network():
    """Internet connectivity check karo — multiple endpoints try karo."""
    urls = [
        "https://api.telegram.org",
        "https://google.com",
        "https://cloudflare.com",
    ]
    for url in urls:
        try:
            session = await _get_hm_session()
            async with session.get(url, allow_redirects=True) as resp:
                if resp.status < 500:
                    return True
        except Exception:
            continue
    return False


# ==========================================
# MAIN HEALTH MONITOR LOOP
# ==========================================

async def health_monitor_loop():
    """
    Background task — har 2 min mein system check karta hai.
    Issues milne par admin + users ko alert karta hai.
    """
    await asyncio.sleep(15)  # Bot fully start hone do
    logger.info("💓 Health Monitor started!")

    consecutive_net_failures = 0

    while True:
        try:
            health = await get_system_health()

            # 1. RAM Check
            await check_and_alert_ram(health)

            # 2. Network Check
            net_ok = await check_network()
            if not net_ok:
                consecutive_net_failures += 1
                if consecutive_net_failures >= 10 and can_send_alert("net_down"):
                    msg = (
                        "🔴 **NETWORK ALERT!**\n"
                        "━━━━━━━━━━━━━━━━━━━━\n"
                        f"⚠️ Telegram servers se connection nahi!\n\n"
                        f"🌐 Network: ❌ Down\n"
                        f"🕒 Time: `{health['checked_at']}`\n"
                        f"⏱ Uptime: `{health['uptime']}`\n\n"
                        f"💡 Bot messages receive nahi kar sakta abhi.\n"
                        f"🔄 Telethon automatically reconnect karegi.\n\n" + _get_owner_footer()
                    )
                    await alert_admins(msg)
            else:
                if consecutive_net_failures >= 10 and can_send_alert("net_up"):
                    # Network wapas aai — notify karo
                    msg = (
                        "🟢 **NETWORK RESTORED!**\n"
                        "━━━━━━━━━━━━━━━━━━━━\n"
                        f"✅ Network connection wapas aa gayi!\n\n"
                        f"🌐 Network: ✅ Online\n"
                        f"🕒 Time: `{health['checked_at']}`\n\n"
                        f"Bot ab normally kaam karega.\n\n" + _get_owner_footer()
                    )
                    await alert_admins(msg)
                consecutive_net_failures = 0

            # 3. CPU Alert (90% se zyada)
            if health["cpu_percent"] >= 90 and can_send_alert("cpu_high"):
                msg = (
                    "🟡 **HIGH CPU ALERT!**\n"
                    "━━━━━━━━━━━━━━━━━━━━\n"
                    f"⚠️ CPU usage bahut high hai!\n\n"
                    f"💻 CPU: **{health['cpu_percent']}%**\n"
                    f"🕒 Time: `{health['checked_at']}`\n\n"
                    f"Bot slow respond kar sakta hai.\n\n" + _get_owner_footer()
                )
                await alert_admins(msg)

        except Exception as e:
            logger.error(f"Health monitor error: {e}")

        await asyncio.sleep(HEALTH_CONFIG["check_interval_sec"])


# ==========================================
# CRASH / EXCEPTION HANDLER
# ==========================================

async def send_crash_alert(error: Exception, context: str = "Unknown"):
    """
    Koi bhi unexpected crash hone par admin ko immediately alert karo.
    """
    error_text = "".join(traceback.format_exception(type(error), error, error.__traceback__))
    error_short = str(error)[:300]

    msg = (
        "🚨 **BOT CRASH / ERROR ALERT!**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"❌ Error Context: `{context}`\n"
        f"🔴 Error: `{error_short}`\n"
        f"🕒 Time: `{datetime.datetime.now().strftime('%d/%m/%Y %H:%M:%S')}`\n"
        f"⏱ Uptime Was: `{get_uptime_str()}`\n\n"
        f"💡 Bot ne automatically recover karne ki koshish ki.\n"
        f"Agar kaam nahi kar raha to Render dashboard se redeploy karo.\n\n" + _get_owner_footer()
    )
    await alert_admins(msg)


# ==========================================
# ADMIN: LIVE HEALTH COMMAND
# ==========================================

from telethon import events, Button

@bot.on(events.NewMessage(pattern='/health'))
async def health_cmd(event):
    """Admin ke liye /health command — live system stats."""
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.respond("❌ Admin only command.")

    health = await get_system_health()
    ram_emoji = get_status_emoji(health["ram_used_percent"])
    cpu_emoji = get_status_emoji(health["cpu_percent"])
    disk_emoji = get_status_emoji(health["disk_used_percent"])

    active_sessions = sum(
        1 for u in list(db.values())
        if isinstance(u, dict) and u.get("settings", {}).get("running")
    )

    msg = (
        "💓 **LIVE HEALTH REPORT**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒 Time: `{health['checked_at']}`\n"
        f"⏱ Uptime: `{health['uptime']}`\n\n"
        f"**💾 Memory:**\n"
        f"{ram_emoji} RAM: `{health['ram_used_mb']}MB / {health['ram_total_mb']}MB` ({health['ram_used_percent']}%)\n\n"
        f"**⚡ Processor:**\n"
        f"{cpu_emoji} Bot CPU: `{health['cpu_percent']}%` | Server: `{health.get('cpu_server',0)}%`\n\n"
        f"**💽 Storage:**\n"
        f"{disk_emoji} Disk Free: `{health['disk_free_gb']} GB`\n\n"
        f"**🌐 Network:**\n"
        f"📤 Sent: `{health['net_sent_kb']} KB`\n"
        f"📥 Recv: `{health['net_recv_kb']} KB`\n\n"
        f"**🤖 Bot Stats:**\n"
        f"👥 Total Users: `{len(db)}`\n"
        f"🟢 Active Forwarders: `{active_sessions}`\n\n" + _get_owner_footer()
    )

    await event.respond(msg, buttons=[
        [Button.inline("🔄 Refresh", b"health_refresh")]
    ])


@bot.on(events.CallbackQuery(data=b"health_refresh"))
async def health_refresh(event):
    await event.answer()
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)

    health = await get_system_health()
    ram_emoji = get_status_emoji(health["ram_used_percent"])
    cpu_emoji = get_status_emoji(health["cpu_percent"])
    disk_emoji = get_status_emoji(health["disk_used_percent"])

    active_sessions = sum(
        1 for u in list(db.values())
        if isinstance(u, dict) and u.get("settings", {}).get("running")
    )

    msg = (
        "💓 **LIVE HEALTH REPORT**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"🕒 Time: `{health['checked_at']}`\n"
        f"⏱ Uptime: `{health['uptime']}`\n\n"
        f"**💾 Memory:**\n"
        f"{ram_emoji} RAM: `{health['ram_used_mb']}MB / {health['ram_total_mb']}MB` ({health['ram_used_percent']}%)\n\n"
        f"**⚡ Processor:**\n"
        f"{cpu_emoji} Bot CPU: `{health['cpu_percent']}%` | Server: `{health.get('cpu_server',0)}%`\n\n"
        f"**💽 Storage:**\n"
        f"{disk_emoji} Disk Free: `{health['disk_free_gb']} GB`\n\n"
        f"**🌐 Network:**\n"
        f"📤 Sent: `{health['net_sent_kb']} KB`\n"
        f"📥 Recv: `{health['net_recv_kb']} KB`\n\n"
        f"**🤖 Bot Stats:**\n"
        f"👥 Total Users: `{len(db)}`\n"
        f"🟢 Active Forwarders: `{active_sessions}`\n\n" + _get_owner_footer()
    )

    try:
        await event.edit(msg, buttons=[
            [Button.inline("🔄 Refresh", b"health_refresh")]
        ])
    except errors.MessageNotModifiedError:
        await event.answer("Already up to date!")



# ==========================================
# ADMIN — ALERT DESTINATIONS PANEL
# ==========================================

@bot.on(events.CallbackQuery(data=b"adm_alert_dest"))
async def adm_alert_dest_panel(event):
    await event.answer()
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.answer("No permission", alert=True)

    alert_cfg = get_alert_config()
    destinations = alert_cfg.get("destinations", [])

    if destinations:
        dest_txt = ""
        for i, d in enumerate(destinations, 1):
            dtype = d.get("type", "bot")
            if dtype == "channel":
                em = "Channel"
            elif dtype == "group":
                em = "Group"
            else:
                em = "Bot/User"
            d_name = str(d.get("name", d.get("id", "")))
            d_id = str(d.get("id", ""))
            dest_txt += str(i) + ". [" + em + "] " + d_name + " | ID: " + d_id + "\n"
    else:
        dest_txt = "Koi destination set nahi\n(Default: Sabhi admins ko directly jaayega)"

    txt = (
        "ALERT DESTINATIONS\n"
        "--------------------\n"
        "Bot ON/OFF, RAM, Network alerts\n"
        "yahan bheje jaayenge.\n\n"
        "Current Destinations:\n" + dest_txt + "\n" +
        "Channel/Group mein bot ko admin banana zaroori hai.\n\n" +
        (_get_owner_footer() or "")
    )

    btns = [
        [Button.inline("+ Add Destination", b"adm_add_alert_dest")],
    ]

    for i, d in enumerate(destinations):
        d_name = str(d.get("name", d.get("id", str(i))))
        btns.append([Button.inline(
            "Remove: " + d_name[:25],
            ("adm_rem_dest_" + str(i)).encode()
        )])

    if destinations:
        btns.append([Button.inline("Remove All", b"adm_rem_all_dest")])

    btns.append([Button.inline("Test Alert Bhejo", b"adm_test_alert")])
    btns.append([Button.inline("Back", b"adm_main")])

    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_add_alert_dest"))
async def adm_add_alert_dest(event):
    await event.answer()
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.answer("No permission", alert=True)

    get_user_data(event.sender_id)["step"] = "adm_alert_dest_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    txt = (
        "Add Alert Destination\n\n"
        "Format: ID | NAME | TYPE\n\n"
        "TYPE options:\n"
        "bot = Admin private chat\n"
        "channel = Channel (bot must be admin)\n"
        "group = Group (bot must be admin)\n\n"
        "Examples:\n"
        "YOUR_ID | Your Name | bot\n"  # ⚠️ Change this!
        "-1001234567890 | Alert Channel | channel\n"
        "-1009876543210 | Admin Group | group\n\n"
        "Channel ID pane ke liye: @userinfobot ko forward karo"
    )
    btns = [Button.inline("Cancel", b"adm_alert_dest")]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass
    except Exception:
        await event.respond(txt, buttons=btns)


@bot.on(events.CallbackQuery(pattern=b"adm_rem_dest_"))
async def adm_rem_dest(event):
    await event.answer()
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.answer("No permission", alert=True)

    idx = int(event.data.decode().replace("adm_rem_dest_", ""))
    alert_cfg = get_alert_config()
    destinations = alert_cfg.get("destinations", [])

    if 0 <= idx < len(destinations):
        removed = destinations.pop(idx)
        from database import save_persistent_db
        save_persistent_db()
        await event.answer("Removed: " + str(removed.get("name", removed.get("id"))))
    else:
        await event.answer("Not found!", alert=True)

    await adm_alert_dest_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_rem_all_dest"))
async def adm_rem_all_dest(event):
    await event.answer()
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.answer("No permission", alert=True)

    get_alert_config()["destinations"] = []
    from database import save_persistent_db
    save_persistent_db()
    await event.answer("Sab destinations remove! Ab alerts admins ko directly jaayenge.")
    await adm_alert_dest_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_test_alert"))
async def adm_test_alert(event):
    await event.answer()
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.answer("No permission", alert=True)

    import datetime
    test_msg = (
        "TEST ALERT\n"
        "--------------------\n"
        "Alert destination kaam kar raha hai!\n"
        "Time: " + _ab_fmt(None, "%d/%m/%Y %H:%M:%S") + "\n\n" +
        "Ye ek test message tha.\n\n" +
        (_get_owner_footer() or "")
    )

    await alert_admins(test_msg)
    await event.answer("Test alert bhej diya! Check karo.", alert=True)


async def handle_alert_dest_inputs(event, user_id: int, step: str) -> bool:
    """main.py ke input handler se call hoga."""
    if step != "adm_alert_dest_input":
        return False

    text = event.text.strip()
    if "|" not in text:
        await event.respond(
            "Format galat hai.\n\n"
            "Sahi format: ID | NAME | TYPE\n"
            "Example: 123456789 | Your Name | bot",
            buttons=[Button.inline("Back", b"adm_alert_dest")]
        )
        return True

    parts = [p.strip() for p in text.split("|")]
    if len(parts) < 2:
        await event.respond("Kam se kam ID aur NAME chahiye.")
        return True

    try:
        dest_id = int(parts[0])
    except ValueError:
        await event.respond("ID valid number hona chahiye.")
        return True

    dest_name = parts[1] if len(parts) > 1 else str(dest_id)
    dest_type = parts[2].lower().strip() if len(parts) > 2 else "bot"

    if dest_type not in ("bot", "channel", "group"):
        dest_type = "bot"

    # Test message bhejo
    try:
        await bot.send_message(
            dest_id,
            "Alert destination connected!\n"
            "Ye channel/group ab alerts receive karega.\n\n" +
            (_get_owner_footer() or "")
        )
    except Exception as e:
        get_user_data(user_id)["step"] = None
        await event.respond(
            "Message send nahi hua!\n\n"
            "Error: " + str(e)[:100] + "\n\n"
            "Check karo:\n"
            "- ID sahi hai?\n"
            "- Channel/Group mein bot ko admin banaya?\n"
            "- Bot ko channel mein add kiya?",
            buttons=[Button.inline("Back", b"adm_alert_dest")]
        )
        return True

    # Save karo
    alert_cfg = get_alert_config()
    alert_cfg["destinations"].append({
        "id": dest_id,
        "name": dest_name,
        "type": dest_type
    })

    get_user_data(user_id)["step"] = None
    from database import save_persistent_db
    save_persistent_db()

    await event.respond(
        "Destination Add Ho Gaya!\n\n"
        "Name: " + dest_name + "\n"
        "ID: " + str(dest_id) + "\n"
        "Type: " + dest_type + "\n\n"
        "Ab sabhi alerts yahan bhi jaayenge.",
        buttons=[Button.inline("Alert Panel", b"adm_alert_dest")]
    )
    return True

# ════════════════════════════════════════
# v3 ADDITIONS — Health Score + Predictive OOM + Trends
# ════════════════════════════════════════

from collections import deque as _deque
from circuit_breaker import CircuitBreakerRegistry
_HEALTH_HISTORY = _deque(maxlen=96)   # ~2.4hr at 90s intervals
_ALERT_SENT: dict = {}                # alert_key → timestamp

def _can_alert(key: str, cooldown: int = 1800) -> bool:
    now = time.time()
    if now - _ALERT_SENT.get(key, 0) > cooldown:
        _ALERT_SENT[key] = now
        return True
    return False

def calculate_health_score(metrics: dict) -> int:
    score = 100
    ram = metrics.get("ram_percent", 0)
    if ram > 90:    score -= 40
    elif ram > 80:  score -= 20
    elif ram > 70:  score -= 10
    cpu = metrics.get("cpu_percent", 0)
    if cpu > 90:    score -= 20
    elif cpu > 70:  score -= 10
    disk = metrics.get("disk_percent", 0)
    if disk > 95:   score -= 30
    elif disk > 80: score -= 10
    return max(0, score)

def predict_oom(history) -> str:
    if len(history) < 10: return ""
    recent  = list(history)[-10:]
    oldest  = recent[0].get("ram_percent", 0)
    newest  = recent[-1].get("ram_percent", 0)
    growth  = newest - oldest
    if growth > 10 and newest > 65:
        import config as _cfg
        pts_to_100 = (100 - newest) / max(growth / len(recent), 0.01)
        eta_min    = int(pts_to_100 * _cfg.HEALTH_CONFIG["check_interval_sec"] / 60)
        return (f"📈 RAM growing: {oldest:.0f}% → {newest:.0f}% (+{growth:.1f}% in 15min)\n"
                f"⏱ Est. OOM in ~{eta_min} min if unchecked!")
    return ""

def get_cb_summary() -> str:
    try:
        from circuit_breaker import CircuitBreakerRegistry
        s = CircuitBreakerRegistry.get_stats()
        return f"CB: 🟢{s['closed']} 🟡{s['half_open']} 🔴{s['open']} (total:{s['total']})"
    except Exception:
        return "CB: N/A"

def get_health_emoji(score: int) -> str:
    if score >= 80: return "💚"
    if score >= 50: return "💛"
    return "❤️"

# ── /status command (admin) ──────────────────────────────────────────────────
@bot.on(events.NewMessage(pattern=r"(?i)^/status$"))
async def admin_status_cmd(event):
    if not event.is_private: return
    from admin import is_admin
    if not is_admin(event.sender_id): return
    h  = await get_system_health()
    sc = calculate_health_score(h)
    se = get_health_emoji(sc)
    oom = predict_oom(_HEALTH_HISTORY)
    cb_s = get_cb_summary()
    try:
        from database import user_sessions
        au = len(user_sessions)
    except Exception:
        au = "?"
    ram_bar = "█" * int(h.get("ram_percent",0)/5) + "░" * (20 - int(h.get("ram_percent",0)/5))
    txt = (
        f"🤖 **BOT STATUS — v3**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{se} **Health Score: {sc}/100**\n\n"
        f"💾 RAM: `{h.get('ram_percent',0):.1f}%`  `{ram_bar}`\n"
        f"  Process: `{h.get('proc_ram_mb',0):.0f}MB`\n"
        f"⚙️ CPU: `{h.get('cpu_percent',0):.1f}%`  "
        f"Threads: `{h.get('proc_threads',0)}`\n"
        f"💿 Disk: `{h.get('disk_percent',0):.0f}%`\n"
        f"👥 Active sessions: `{au}`\n"
        f"⏱ Uptime: `{get_uptime_str()}`\n"
        f"🔌 {cb_s}"
        + (f"\n\n⚠️ {oom}" if oom else "")
    )
    from telethon import Button
    await event.respond(txt, buttons=[[Button.inline("🔄 Refresh", b"health_status_refresh")]])

@bot.on(events.CallbackQuery(data=b"health_status_refresh"))
async def health_refresh_cb(event):
    await event.answer()
    from admin import is_admin
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin only", alert=True)
    await event.answer("Refreshing...")
    await admin_status_cmd(event)
