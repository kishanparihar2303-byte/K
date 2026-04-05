"""
support.py — Production-Level Support Ticket System

Features:
- Ticket IDs (#TKT-001)
- Status: open / in_progress / closed / resolved
- Priority: low / normal / high / urgent
- Full conversation thread
- Admin assignment
- Rate limiting (spam protection)
- User ticket history
- Admin ticket dashboard
- Auto-close old tickets
"""

import time
import asyncio
import logging
from database import GLOBAL_STATE, get_user_data, save_persistent_db
from config import bot, OWNER_ID
from telethon import events, Button, errors
from admin import is_admin

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════
# TICKET STORE
# ══════════════════════════════════════════════════════════════

def get_ticket_store() -> dict:
    GLOBAL_STATE.setdefault("support_tickets", {
        "tickets": {},       # {ticket_id: ticket_dict}
        "counter": 0,        # Auto-increment ID
        "user_index": {},    # {user_id: [ticket_id, ...]}
        "settings": {
            "enabled": True,
            "rate_limit_hours": 1,    # User ek ghante mein max 3 tickets
            "rate_limit_count": 3,
            "auto_close_days": 3,     # 3 din mein koi reply nahi = auto close
            "welcome_msg": "📞 Hum aapki help ke liye hain! Apni problem clearly describe karo।",
        }
    })
    return GLOBAL_STATE["support_tickets"]


def _new_ticket_id() -> str:
    store = get_ticket_store()
    store["counter"] += 1
    return f"TKT-{store['counter']:04d}"


def create_ticket(user_id: int, user_name: str, username: str,
                  subject: str, message: str, priority: str = "normal") -> dict:
    store  = get_ticket_store()
    tid    = _new_ticket_id()
    ticket = {
        "id":         tid,
        "user_id":    user_id,
        "user_name":  user_name,
        "username":   username,
        "subject":    subject,
        "priority":   priority,
        "status":     "open",
        "assigned_to": None,
        "messages": [
            {
                "from":    "user",
                "user_id": user_id,
                "text":    message,
                "ts":      time.time(),
            }
        ],
        "created_at":  time.time(),
        "updated_at":  time.time(),
        "closed_at":   None,
        "rating":      None,
    }
    store["tickets"][tid] = ticket
    store["user_index"].setdefault(str(user_id), []).append(tid)
    save_persistent_db()
    return ticket


def add_message(tid: str, from_type: str, user_id: int, text: str):
    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)
    if not ticket:
        return
    ticket["messages"].append({
        "from":    from_type,  # "user" or "admin"
        "user_id": user_id,
        "text":    text,
        "ts":      time.time(),
    })
    ticket["updated_at"] = time.time()
    if from_type == "admin" and ticket["status"] == "open":
        ticket["status"] = "in_progress"
    if not ticket["assigned_to"] and from_type == "admin":
        ticket["assigned_to"] = user_id
    save_persistent_db()


def close_ticket(tid: str, resolver_id: int = None):
    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)
    if ticket:
        ticket["status"]    = "resolved"
        ticket["closed_at"] = time.time()
        if resolver_id:
            ticket["assigned_to"] = resolver_id
        save_persistent_db()


def get_user_tickets(user_id: int, limit: int = 5) -> list:
    store  = get_ticket_store()
    tids   = store["user_index"].get(str(user_id), [])
    result = []
    for tid in reversed(tids[-limit:]):
        t = store["tickets"].get(tid)
        if t:
            result.append(t)
    return result


def get_open_tickets() -> list:
    store = get_ticket_store()
    return [t for t in store["tickets"].values()
            if t["status"] in ("open", "in_progress")]


def check_rate_limit(user_id: int) -> tuple[bool, int]:
    """Returns (allowed, seconds_to_wait)"""
    store    = get_ticket_store()
    settings = store["settings"]
    limit    = settings.get("rate_limit_count", 3)
    window   = settings.get("rate_limit_hours", 1) * 3600
    tids     = store["user_index"].get(str(user_id), [])
    now      = time.time()
    recent   = sum(
        1 for tid in tids
        if now - store["tickets"].get(tid, {}).get("created_at", 0) < window
    )
    if recent >= limit:
        oldest_recent = min(
            store["tickets"].get(tid, {}).get("created_at", 0)
            for tid in tids
            if now - store["tickets"].get(tid, {}).get("created_at", 0) < window
        )
        wait = int(window - (now - oldest_recent))
        return False, wait
    return True, 0


# ══════════════════════════════════════════════════════════════
# STATUS & PRIORITY HELPERS
# ══════════════════════════════════════════════════════════════

_STATUS_EMOJI = {
    "open":        "🔴",
    "in_progress": "🟡",
    "resolved":    "🟢",
    "closed":      "⚫",
}
_PRIORITY_EMOJI = {
    "low":    "🔵",
    "normal": "🟢",
    "high":   "🟠",
    "urgent": "🔴",
}

def _status_label(status: str) -> str:
    return f"{_STATUS_EMOJI.get(status, '⚪')} {status.replace('_', ' ').title()}"

def _priority_label(priority: str) -> str:
    return f"{_PRIORITY_EMOJI.get(priority, '⚪')} {priority.title()}"

def _time_ago(ts: float) -> str:
    diff = int(time.time() - ts)
    if diff < 60:       return f"{diff}s ago"
    if diff < 3600:     return f"{diff//60}m ago"
    if diff < 86400:    return f"{diff//3600}h ago"
    return f"{diff//86400}d ago"


# ══════════════════════════════════════════════════════════════
# USER SIDE HANDLERS
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"contact_admin"))
async def contact_admin_cb(event):
    await event.answer()
    store = get_ticket_store()
    if not store["settings"].get("enabled", True):
        try:
            await event.edit(
                "📞 **Support Unavailable**\n\n"
                "❌ Abhi support system offline hai। Baad mein try karo।",
                buttons=[[Button.inline("🔙 Back", b"help_guide")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    uid  = event.sender_id
    # Rate limit check
    allowed, wait = check_rate_limit(uid)
    if not allowed:
        mins = wait // 60 + 1
        try:
            await event.edit(
                f"⏳ **Too Many Tickets!**\n\n"
                f"Aapne bahut zyada tickets khole hain।\n"
                f"`{mins}` minute baad dobara try karo।",
                buttons=[[Button.inline("🔙 Back", b"help_guide")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    welcome = store["settings"].get("welcome_msg", "")

    try:
        await event.edit(
            "📞 **Support System**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + (f"_{welcome}_\n\n" if welcome else "")
            + "Aap kya karna chahte ho?",
            buttons=[
                [Button.inline("✉️ New Ticket Kholo",  b"support_new_ticket")],
                [Button.inline("📋 Mere Tickets",       b"support_my_tickets")],
                [Button.inline("🔙 Help Menu",          b"help_guide")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"support_new_ticket"))
async def support_new_ticket_cb(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "support_ticket_subject"
    data["step_since"] = time.time()
    data["temp_data"]  = {}

    try:
        await event.edit(
            "🎫 **New Support Ticket**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Step 1/2:** Problem ka short subject likho\n\n"
            "Examples:\n"
            "• `Forwarding kaam nahi kar raha`\n"
            "• `Premium payment issue`\n"
            "• `Translation galat ho rahi hai`\n\n"
            "Ab subject type karo 👇",
            buttons=[[Button.inline("❌ Cancel", b"contact_admin")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"support_my_tickets"))
async def support_my_tickets_cb(event):
    await event.answer()
    uid     = event.sender_id
    tickets = get_user_tickets(uid, limit=5)

    if not tickets:
        try:
            await event.edit(
                "📋 **Aapke Tickets**\n\n"
                "❌ Abhi tak koi ticket nahi khola।\n"
                "Koi problem ho to naya ticket kholo।",
                buttons=[
                    [Button.inline("✉️ New Ticket",  b"support_new_ticket")],
                    [Button.inline("🔙 Back",         b"contact_admin")],
                ]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    txt = "📋 **Aapke Recent Tickets:**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    btns = []
    for t in tickets:
        st  = _status_label(t["status"])
        ago = _time_ago(t["created_at"])
        txt += f"**#{t['id']}** — {st}\n"
        txt += f"  _{t['subject'][:40]}_  ·  {ago}\n\n"
        btns.append([Button.inline(
            f"#{t['id']} {_STATUS_EMOJI.get(t['status'],'')} {t['subject'][:25]}",
            f"support_view_{t['id']}".encode()
        )])
    btns.append([Button.inline("✉️ New Ticket", b"support_new_ticket"),
                 Button.inline("🔙 Back",        b"contact_admin")])

    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"support_view_"))
async def support_view_ticket_cb(event):
    await event.answer()
    tid    = event.data.decode().replace("support_view_", "")
    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)
    uid    = event.sender_id

    if not ticket or (ticket["user_id"] != uid and not is_admin(uid)):
        return await event.answer("❌ Ticket nahi mila!", alert=True)

    st   = _status_label(ticket["status"])
    pri  = _priority_label(ticket["priority"])
    msgs = ticket["messages"][-5:]  # Last 5 messages
    ago  = _time_ago(ticket["created_at"])

    txt = (
        f"🎫 **Ticket #{tid}**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 **Subject:** {ticket['subject']}\n"
        f"📊 **Status:** {st}\n"
        f"⚡ **Priority:** {pri}\n"
        f"🕐 **Opened:** {ago}\n\n"
        "**💬 Conversation:**\n"
        "─────────────────────\n"
    )
    for m in msgs:
        sender = "👤 You" if m["from"] == "user" else "👨‍💼 Admin"
        t_ago  = _time_ago(m["ts"])
        txt   += f"{sender} _{t_ago}_:\n{m['text'][:200]}\n\n"

    btns = []
    if ticket["status"] not in ("resolved", "closed"):
        btns.append([Button.inline("✏️ Reply / Add Info",
                                   f"support_reply_{tid}".encode())])
        btns.append([Button.inline("✅ Mark Resolved",
                                   f"support_close_{tid}".encode())])
    else:
        if not ticket.get("rating"):
            btns.append([
                Button.inline("⭐ 1", f"support_rate_{tid}_1".encode()),
                Button.inline("⭐⭐ 3", f"support_rate_{tid}_3".encode()),
                Button.inline("⭐⭐⭐ 5", f"support_rate_{tid}_5".encode()),
            ])
    btns.append([Button.inline("🔙 My Tickets", b"support_my_tickets")])

    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"support_reply_"))
async def support_user_reply_cb(event):
    await event.answer()
    tid  = event.data.decode().replace("support_reply_", "")
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "support_ticket_reply"
    data["step_since"] = time.time()
    data["temp_data"]  = {"reply_tid": tid}

    try:
        await event.edit(
            f"✏️ **Ticket #{tid} mein reply karo:**\n\n"
            "Additional information ya follow-up bhejo 👇",
            buttons=[[Button.inline("❌ Cancel",
                                    f"support_view_{tid}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"support_close_"))
async def support_close_by_user_cb(event):
    await event.answer()
    tid = event.data.decode().replace("support_close_", "")
    uid = event.sender_id
    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)

    if not ticket or ticket["user_id"] != uid:
        return await event.answer("❌ Permission denied!", alert=True)

    close_ticket(tid, uid)
    await event.answer("✅ Ticket resolved mark ho gaya!", alert=True)
    await support_my_tickets_cb(event)


@bot.on(events.CallbackQuery(pattern=b"support_rate_"))
async def support_rate_cb(event):
    await event.answer()
    parts  = event.data.decode().split("_")
    tid    = parts[2]
    rating = int(parts[3])
    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)
    if ticket:
        ticket["rating"] = rating
        save_persistent_db()
    stars  = "⭐" * rating
    await event.answer(f"✅ Rating: {stars} — Shukriya!", alert=True)
    await support_my_tickets_cb(event)


# ══════════════════════════════════════════════════════════════
# ADMIN SIDE HANDLERS
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_support_panel"))
async def adm_support_panel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    open_tix   = get_open_tickets()
    total      = len(get_ticket_store()["tickets"])
    urgent_c   = sum(1 for t in open_tix if t["priority"] == "urgent")
    high_c     = sum(1 for t in open_tix if t["priority"] == "high")

    txt = (
        "🎫 **Support Dashboard**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 **Stats:**\n"
        f"  Total Tickets: `{total}`\n"
        f"  🔴 Open: `{sum(1 for t in open_tix if t['status']=='open')}`\n"
        f"  🟡 In Progress: `{sum(1 for t in open_tix if t['status']=='in_progress')}`\n"
        f"  🔴 Urgent: `{urgent_c}`  🟠 High: `{high_c}`\n\n"
    )
    if open_tix:
        txt += "**📬 Open Tickets:**\n"
        for t in sorted(open_tix, key=lambda x: (
            {"urgent": 0, "high": 1, "normal": 2, "low": 3}.get(x["priority"], 2),
            -x["updated_at"]
        ))[:5]:
            ago = _time_ago(t["updated_at"])
            txt += f"  #{t['id']} {_PRIORITY_EMOJI.get(t['priority'],'')} `{t['subject'][:30]}` — {ago}\n"

    btns = [
        [Button.inline("📬 Open Tickets",    b"adm_support_open"),
         Button.inline("📦 All Tickets",     b"adm_support_all")],
        [Button.inline("⚙️ Support Settings", b"adm_support_settings")],
        [Button.inline("🔙 Admin Panel",      b"adm_main")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_support_open"))
async def adm_support_open_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return

    open_tix = sorted(get_open_tickets(), key=lambda x: (
        {"urgent": 0, "high": 1, "normal": 2, "low": 3}.get(x["priority"], 2),
        -x["updated_at"]
    ))

    if not open_tix:
        try:
            await event.edit(
                "📬 **Open Tickets**\n\n✅ Koi open ticket nahi!",
                buttons=[[Button.inline("🔙 Dashboard", b"adm_support_panel")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    btns = []
    for t in open_tix[:10]:
        pri = _PRIORITY_EMOJI.get(t["priority"], "")
        st  = _STATUS_EMOJI.get(t["status"], "")
        ago = _time_ago(t["updated_at"])
        btns.append([Button.inline(
            f"{pri}{st} #{t['id']} — {t['subject'][:28]} ({ago})",
            f"adm_ticket_{t['id']}".encode()
        )])
    btns.append([Button.inline("🔙 Dashboard", b"adm_support_panel")])

    try:
        await event.edit("📬 **Open Tickets** (priority order):\n", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_support_all"))
async def adm_support_all_cb(event):
    """Show ALL tickets (open + closed) — was missing handler (FIXED)."""
    await event.answer()
    if not is_admin(event.sender_id): return

    store = get_ticket_store()
    all_tix = sorted(store["tickets"].values(), key=lambda x: -x.get("updated_at", 0))

    if not all_tix:
        try:
            await event.edit(
                "📦 **All Tickets**\n\n✅ Koi ticket nahi!",
                buttons=[[Button.inline("🔙 Dashboard", b"adm_support_panel")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    btns = []
    for t in all_tix[:15]:
        pri = _PRIORITY_EMOJI.get(t.get("priority", "normal"), "")
        st  = _STATUS_EMOJI.get(t.get("status", "open"), "")
        ago = _time_ago(t.get("updated_at", 0))
        btns.append([Button.inline(
            f"{pri}{st} #{t['id']} — {t['subject'][:25]} ({ago})",
            f"adm_ticket_{t['id']}".encode()
        )])
    btns.append([Button.inline("🔙 Dashboard", b"adm_support_panel")])

    total = len(all_tix)
    open_c  = sum(1 for t in all_tix if t.get("status") != "closed")
    closed_c = total - open_c

    try:
        await event.edit(
            f"📦 **All Tickets** — Total: {total}\n"
            f"📬 Open: {open_c}  |  ✅ Closed: {closed_c}\n",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_ticket_"))
async def adm_view_ticket_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return

    tid    = event.data.decode().replace("adm_ticket_", "")
    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)

    if not ticket:
        return await event.answer("❌ Ticket nahi mila!", alert=True)

    msgs = ticket["messages"][-6:]
    ago  = _time_ago(ticket["created_at"])

    txt = (
        f"🎫 **Ticket #{tid}**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 **User:** {ticket['user_name']} ({ticket['username']})\n"
        f"🆔 **ID:** `{ticket['user_id']}`\n"
        f"📌 **Subject:** {ticket['subject']}\n"
        f"📊 **Status:** {_status_label(ticket['status'])}\n"
        f"⚡ **Priority:** {_priority_label(ticket['priority'])}\n"
        f"🕐 **Opened:** {ago}\n\n"
        "**💬 Messages:**\n─────────────────────\n"
    )
    for m in msgs:
        sender = f"👤 {ticket['user_name']}" if m["from"] == "user" else "👨‍💼 Admin"
        t_ago  = _time_ago(m["ts"])
        txt   += f"**{sender}** _{t_ago}_:\n{m['text'][:300]}\n\n"

    btns = [
        [Button.inline("↩️ Reply",
                       f"adm_reply_{tid}".encode()),
         Button.inline("✅ Close",
                       f"adm_close_{tid}".encode())],
        [Button.inline("🔴 Set Urgent",
                       f"adm_pri_{tid}_urgent".encode()),
         Button.inline("🟠 Set High",
                       f"adm_pri_{tid}_high".encode())],
        [Button.inline("🔙 Open Tickets", b"adm_support_open")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_reply_"))
async def adm_reply_ticket_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return

    tid  = event.data.decode().replace("adm_reply_", "")
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "adm_support_reply"
    data["step_since"] = time.time()
    data["temp_data"]  = {"reply_tid": tid}

    try:
        await event.edit(
            f"✏️ **Ticket #{tid} — Reply Likho:**\n\n"
            "User ko message type karo 👇",
            buttons=[[Button.inline("❌ Cancel",
                                    f"adm_ticket_{tid}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_close_"))
async def adm_close_ticket_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return

    tid = event.data.decode().replace("adm_close_", "")
    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)

    if not ticket: return

    close_ticket(tid, event.sender_id)

    # User ko notify karo
    try:
        await bot.send_message(
            int(ticket["user_id"]),
            f"✅ **Ticket #{tid} Close Ho Gaya**\n\n"
            f"Aapka ticket admin ne resolve kar diya।\n"
            f"Problem solve hui? Rating do 👇",
            buttons=[
                [Button.inline("⭐ 1 Star",   f"support_rate_{tid}_1".encode()),
                 Button.inline("⭐⭐⭐ 3",      f"support_rate_{tid}_3".encode()),
                 Button.inline("⭐⭐⭐⭐⭐ 5",  f"support_rate_{tid}_5".encode())],
                [Button.inline("📋 Mere Tickets", b"support_my_tickets")],
            ]
        )
    except Exception:
        pass

    await event.answer(f"✅ Ticket #{tid} closed!", alert=True)
    await adm_support_open_cb(event)


@bot.on(events.CallbackQuery(pattern=b"adm_pri_"))
async def adm_set_priority_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return

    parts    = event.data.decode().split("_")
    tid      = parts[2]
    priority = parts[3]
    store    = get_ticket_store()
    ticket   = store["tickets"].get(tid)

    if ticket:
        ticket["priority"]   = priority
        ticket["updated_at"] = time.time()
        save_persistent_db()
        await event.answer(f"Priority: {_priority_label(priority)}", alert=False)
        # Re-render
        fake = type("E", (), {"answer": event.answer, "edit": event.edit,
                               "data": f"adm_ticket_{tid}".encode(),
                               "sender_id": event.sender_id})()
        await adm_view_ticket_cb(fake)


@bot.on(events.CallbackQuery(data=b"adm_support_settings"))
async def adm_support_settings_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return

    store    = get_ticket_store()
    settings = store["settings"]
    enabled  = settings.get("enabled", True)

    try:
        await event.edit(
            "⚙️ **Support System Settings**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Status: {'🟢 ON' if enabled else '🔴 OFF'}\n"
            f"Rate Limit: `{settings.get('rate_limit_count',3)} tickets/{settings.get('rate_limit_hours',1)}hr`\n"
            f"Auto-close: `{settings.get('auto_close_days',3)} days`\n\n"
            f"Welcome Msg:\n`{settings.get('welcome_msg','')[:80]}`",
            buttons=[
                [Button.inline(
                    "🔴 Disable" if enabled else "🟢 Enable",
                    b"adm_support_toggle"
                )],
                [Button.inline("✏️ Set Welcome Msg", b"adm_support_set_welcome")],
                [Button.inline("🔙 Dashboard", b"adm_support_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_support_toggle"))
async def adm_support_toggle_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    store = get_ticket_store()
    store["settings"]["enabled"] = not store["settings"].get("enabled", True)
    save_persistent_db()
    await adm_support_settings_cb(event)


@bot.on(events.CallbackQuery(data=b"adm_support_set_welcome"))
async def adm_support_set_welcome_cb(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "adm_support_welcome_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **Support Welcome Message Set Karo:**\n\nBhejo 👇",
            buttons=[[Button.inline("❌ Cancel", b"adm_support_settings")]]
        )
    except errors.MessageNotModifiedError:
        pass


def get_support_step_handler():
    """Returns step handlers dict for main.py integration."""
    return {
        "support_ticket_subject": _handle_subject,
        "support_ticket_message": _handle_message,
        "support_ticket_reply":   _handle_user_reply,
        "adm_support_reply":      _handle_admin_reply,
        "adm_support_welcome_input": _handle_welcome_input,
    }


async def _handle_subject(event, user_id, data):
    subject = event.text.strip()
    if not subject:
        await event.respond("❌ Subject khali nahi ho sakta!")
        return True
    if len(subject) > 100:
        await event.respond("❌ Subject 100 characters se chhota hona chahiye!")
        return True
    data["temp_data"]["subject"] = subject
    data["step"] = "support_ticket_message"
    data["step_since"] = time.time()
    await event.respond(
        f"📌 **Subject:** `{subject}`\n\n"
        "**Step 2/2:** Ab apni problem detail mein describe karo।\n"
        "Jitna detail doge, utna jaldi solve hoga।\n\n"
        "Ab message bhejo 👇",
        buttons=[[Button.inline("❌ Cancel", b"contact_admin")]]
    )
    return True


async def _handle_message(event, user_id, data):
    message = event.text.strip() if event.text else ""
    has_media = bool(event.media or event.file or event.photo)

    if not message and not has_media:
        await event.respond("❌ Message khali nahi ho sakta!")
        return True

    subject = data.get("temp_data", {}).get("subject", "Support Request")
    data["step"] = None
    data["temp_data"] = {}

    # User info
    try:
        me = await event.get_sender()
        user_name = ((getattr(me,"first_name","") or "") + " " + (getattr(me,"last_name","") or "")).strip() or f"User {user_id}"
        username  = f"@{me.username}" if getattr(me,"username",None) else "no username"
    except Exception:
        user_name = f"User {user_id}"
        username  = "unknown"

    ticket = create_ticket(user_id, user_name, username, subject, message or "[Media]")
    tid    = ticket["id"]

    # User ko confirm
    await event.respond(
        f"✅ **Ticket #{tid} Create Ho Gaya!**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📌 Subject: `{subject}`\n"
        f"🔴 Status: Open\n\n"
        "Admin jald reply karenge। Patience rakho 🙏\n"
        f"Status check: `/ticket {tid}`",
        buttons=[
            [Button.inline("📋 Mere Tickets", b"support_my_tickets")],
            [Button.inline("🏠 Main Menu",    b"main_menu")],
        ]
    )

    # Admins ko notify
    from config import OWNER_ID
    all_admins = list(set([OWNER_ID] + [int(k) for k in GLOBAL_STATE.get("admins", {}).keys()]))
    notif_txt = (
        f"🎫 **New Support Ticket #{tid}**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 **From:** {user_name} ({username})\n"
        f"🆔 **ID:** `{user_id}`\n"
        f"📌 **Subject:** {subject}\n\n"
        f"💬 **Message:**\n{message[:400] if message else '[Media attached]'}"
    )
    for aid in all_admins:
        try:
            await bot.send_message(int(aid), notif_txt,
                buttons=[[Button.inline(f"🎫 View #{tid}", f"adm_ticket_{tid}".encode())]])
            if has_media:
                await event.forward_to(int(aid))
        except Exception:
            pass
    return True


async def _handle_user_reply(event, user_id, data):
    reply_text = event.text.strip() if event.text else ""
    if not reply_text:
        await event.respond("❌ Reply khali nahi ho sakti!")
        return True

    tid = data.get("temp_data", {}).get("reply_tid", "")
    data["step"] = None
    data["temp_data"] = {}

    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)
    if not ticket:
        await event.respond("❌ Ticket nahi mila!")
        return True

    add_message(tid, "user", user_id, reply_text)

    await event.respond(
        f"✅ Reply ticket #{tid} mein add ho gayi!",
        buttons=[[Button.inline(f"🎫 View #{tid}", f"support_view_{tid}".encode()),
                  Button.inline("🏠 Menu", b"main_menu")]]
    )

    # Admin ko notify
    from config import OWNER_ID
    all_admins = list(set([OWNER_ID] + [int(k) for k in GLOBAL_STATE.get("admins", {}).keys()]))
    for aid in all_admins:
        try:
            await bot.send_message(int(aid),
                f"📩 **Ticket #{tid} — New Reply**\n"
                f"👤 {ticket['user_name']}: {reply_text[:200]}",
                buttons=[[Button.inline(f"🎫 View #{tid}", f"adm_ticket_{tid}".encode())]])
        except Exception:
            pass
    return True


async def _handle_admin_reply(event, user_id, data):
    reply_text = event.text.strip() if event.text else ""
    if not reply_text:
        await event.respond("❌ Reply khali nahi ho sakti!")
        return True

    tid = data.get("temp_data", {}).get("reply_tid", "")
    data["step"] = None
    data["temp_data"] = {}

    store  = get_ticket_store()
    ticket = store["tickets"].get(tid)
    if not ticket:
        await event.respond("❌ Ticket nahi mila!")
        return True

    add_message(tid, "admin", user_id, reply_text)

    # User ko reply bhejo
    try:
        await bot.send_message(
            int(ticket["user_id"]),
            f"📨 **Admin Reply — Ticket #{tid}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{reply_text}",
            buttons=[
                [Button.inline(f"↩️ Reply",      f"support_reply_{tid}".encode()),
                 Button.inline("✅ Resolved",    f"support_close_{tid}".encode())],
                [Button.inline("📋 My Tickets",  b"support_my_tickets")],
            ]
        )
        await event.respond(
            f"✅ Reply user `{ticket['user_id']}` ko bhej di!",
            buttons=[[Button.inline(f"🎫 View #{tid}", f"adm_ticket_{tid}".encode()),
                      Button.inline("📬 Open Tickets", b"adm_support_open")]]
        )
    except Exception as e:
        await event.respond(f"❌ Reply nahi gayi: {str(e)[:80]}")
    return True


async def _handle_welcome_input(event, user_id, data):
    text = event.text.strip()
    data["step"] = None
    store = get_ticket_store()
    store["settings"]["welcome_msg"] = text
    save_persistent_db()
    await event.respond("✅ Welcome message updated!",
        buttons=[[Button.inline("⚙️ Settings", b"adm_support_settings")]])
    return True


# ── /ticket command ───────────────────────────────────────────

@bot.on(events.NewMessage(pattern=r'(?i)^/ticket(?: (.+))?$'))
async def ticket_cmd(event):
    uid = event.sender_id
    arg = event.pattern_match.group(1)
    if arg:
        tid    = arg.strip().upper()
        store  = get_ticket_store()
        ticket = store["tickets"].get(tid)
        if not ticket or (ticket["user_id"] != uid and not is_admin(uid)):
            await event.respond("❌ Ticket nahi mila ya access nahi!")
            return
        st  = _status_label(ticket["status"])
        ago = _time_ago(ticket["created_at"])
        await event.respond(
            f"🎫 **Ticket #{tid}**\n"
            f"📌 {ticket['subject']}\n"
            f"📊 Status: {st}\n"
            f"🕐 {ago}",
            buttons=[[Button.inline(f"🎫 View Details", f"support_view_{tid}".encode())]]
        )
    else:
        await event.respond(
            "📋 **Support Commands:**\n\n"
            "• `/ticket TKT-001` — ticket details\n"
            "• `/contact` — naya ticket kholo",
            buttons=[[Button.inline("📞 Contact Support", b"contact_admin")]]
        )
