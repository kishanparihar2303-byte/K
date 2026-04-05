# ui/feature_flags_menu.py
# ══════════════════════════════════════════════════════════
# ADMIN — FEATURE FLAGS PANEL
# Har feature ko free/premium/disabled set karo
# Gemini API key set karo
# Affiliate mode set karo
# ══════════════════════════════════════════════════════════

from telethon import events, Button, errors
from config import bot
from admin import is_admin
from database import get_user_data, save_persistent_db, save_to_mongo
import asyncio

async def _save_step(data):
    """Step save karo — JSON + MongoDB dono mein."""
    save_persistent_db()
    try:
        await save_to_mongo()
    except Exception:
        pass


# ── Helper ───────────────────────────────────────────────
def _flags_main_text() -> str:
    from feature_flags import get_flag

    def ae(val):
        if val == "free":     return "🟢 Free"
        if val == "premium":  return "💎 Prem"
        if val == "disabled": return "🔴 Off"
        if val is True:       return "🟢 ON"
        if val is False:      return "🔴 OFF"
        return str(val)

    def row(icon, name, flag_key):
        v = ae(get_flag(flag_key))
        return f"  {icon} {name:<22} **{v}**"

    notify_ch = get_flag("alert_channel_id") or "Owner"
    rl_en  = get_flag("user_rate_limit")
    rl_max = get_flag("max_msg_per_min") or 30
    rl_ab  = get_flag("spam_auto_block")

    return (
        "⚙️ **FEATURE FLAGS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "_🟢 Free = sabhi users  ·  💎 Prem = premium only  ·  🔴 Off = disabled_\n\n"
        "**📋 Feature Access:**\n"
        f"{row('📅','Per-Day Scheduler','per_day_scheduler')}\n"
        f"{row('⏰','Basic Scheduler','scheduler_basic')}\n"
        f"{row('🖼️','Auto Watermark','auto_watermark')}\n"
        f"{row('📊','Deep Analytics','deep_analytics')}\n"
        f"{row('🔔','Notifications','smart_notifications')}\n"
        f"{row('🔗','Affiliate Mgr','affiliate_manager')}\n"
        f"🔁 Duplicate Filter:   **{ae(get_flag('duplicate_filter'))}**\n"
        f"🚫 Link Blocker:       **{ae(get_flag('link_blocker'))}**\n"
        f"🔄 Replacements:       **{ae(get_flag('replacement_rules'))}**\n"
        f"📺 Per-Dest Rules:     **{ae(get_flag('per_dest_rules'))}**\n"
        f"✉️ Start/End Msg:      **{ae(get_flag('start_end_msg'))}**\n"
        f"👥 Reseller System:    **{ae(get_flag('reseller_system'))}**\n\n"
        "**🛡️ Anti-Spam / Rate Limit:**\n"
        f"  Rate Limit:  **{'🟢 ON' if rl_en else '🔴 OFF'}**  "
        f"Max: **{rl_max}/min**\n"
        f"  Auto-Block:  **{'🟢 ON' if rl_ab else '🔴 OFF'}**\n\n"
        f"**🔔 Alert Channel:** `{notify_ch}`\n"
        "_(Tap a button to cycle: Free → Premium → Disabled)_"
    )


def _flags_buttons():
    return [
        [Button.inline("📅 Per-Day Sched",     b"flags_toggle_scheduler"),
         Button.inline("⏰ Basic Sched",        b"flags_toggle_basic_sched")],
        [Button.inline("🖼️ Watermark",         b"flags_toggle_watermark"),
         Button.inline("📊 Analytics",         b"flags_toggle_analytics")],
        [Button.inline("🔔 Notifications",     b"flags_toggle_notifications"),
         Button.inline("🔗 Affiliate",         b"flags_toggle_affiliate")],
        [Button.inline("🔁 Dup Filter",        b"flags_toggle_dup_filter"),
         Button.inline("🚫 Link Blocker",      b"flags_toggle_link_blocker")],
        [Button.inline("🔄 Replacements",      b"flags_toggle_replacements"),
         Button.inline("📺 Per-Dest Rules",    b"flags_toggle_per_dest")],
        [Button.inline("✉️ Start/End Msg",     b"flags_toggle_start_end"),
         Button.inline("👥 Reseller",          b"flags_toggle_reseller")],
        [Button.inline("🛡️ Anti-Spam Engine",  b"flags_antispam_menu")],
        [Button.inline("💎 Affiliate Tags",    b"flags_affiliate_menu"),
         Button.inline("🖼️ Force Watermark",  b"flags_watermark_menu")],
        [Button.inline("🔔 Alert Channel",     b"flags_set_alert_channel")],
        [Button.inline("🔙 Admin Panel",       b"adm_main")],
    ]


# ── Main Panel ───────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"adm_feature_flags"))
async def flags_main(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    try:
        await event.edit(_flags_main_text(), buttons=_flags_buttons())
    except errors.MessageNotModifiedError:
        await event.answer("No changes!")


# ── Access Level Toggles ─────────────────────────────────
_ACCESS_CYCLE = ["free", "premium", "disabled"]

def _cycle_access(key: str):
    from feature_flags import get_flag, set_flag
    current = get_flag(key)
    if current not in _ACCESS_CYCLE:
        current = "free"
    next_val = _ACCESS_CYCLE[(_ACCESS_CYCLE.index(current) + 1) % len(_ACCESS_CYCLE)]
    set_flag(key, next_val)
    try:
        import asyncio as _aio
        from database import save_to_mongo as _sm
        try:
            asyncio.get_running_loop().create_task(_sm())
        except RuntimeError:
            pass
    except Exception:
        pass
    return next_val


@bot.on(events.CallbackQuery(pattern=b"flags_toggle_(.+)"))
async def flags_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)

    key_map = {
        b"flags_toggle_scheduler":     "per_day_scheduler",
        b"flags_toggle_basic_sched":   "scheduler_basic",
        b"flags_toggle_watermark":     "auto_watermark",
        b"flags_toggle_analytics":     "deep_analytics",
        b"flags_toggle_affiliate":     "affiliate_manager",
        b"flags_toggle_notifications": "smart_notifications",
        b"flags_toggle_dup_filter":    "duplicate_filter",
        b"flags_toggle_link_blocker":  "link_blocker",
        b"flags_toggle_replacements":  "replacement_rules",
        b"flags_toggle_per_dest":      "per_dest_rules",
        b"flags_toggle_start_end":     "start_end_msg",
    }

    data = event.data
    if data == b"flags_toggle_reseller":
        from feature_flags import get_flag, set_flag
        current = get_flag("reseller_system")
        set_flag("reseller_system", not current)
        from database import save_to_mongo as _smr; await _smr()
        await event.answer(f"Reseller: {'ON' if not current else 'OFF'}")
    elif data in key_map:
        feature_key = key_map[data]
        new_val = _cycle_access(feature_key)
        emoji = {"free": "🟢", "premium": "💎", "disabled": "🔴"}.get(new_val, "")
        await event.answer(f"{emoji} {feature_key}: {new_val}")
    else:
        await event.answer("Unknown toggle")
        return

    try:
        await event.edit(_flags_main_text(), buttons=_flags_buttons())
    except errors.MessageNotModifiedError:
        pass


# ── Affiliate Menu — Naye admin_menu.py handler pe redirect ──────────────
# Purana handler hata diya — naya complete handler admin_menu.py mein hai
# flags_toggle_aff_mode, flags_set_amazon_tag, flags_set_flipkart_id
# — ye sab bhi admin_menu.py ke naye handler se handle hote hain
# Yahan sirf legacy step handlers ke liye backward compatibility rakhhi hai

@bot.on(events.CallbackQuery(data=b"flags_toggle_aff_mode"))
async def flags_toggle_aff_mode_legacy(event):
    """Legacy redirect — naye aff_adm_toggle_mode pe bhejo."""
    await event.answer()
    if not is_admin(event.sender_id): return
    from feature_flags import get_flag, set_flag
    from database import save_to_mongo as _stm4
    current  = get_flag("affiliate_mode")
    new_mode = "user" if current == "owner" else "owner"
    set_flag("affiliate_mode", new_mode)
    await _stm4()
    # Naye menu pe redirect
    event.data = b"flags_affiliate_menu"


@bot.on(events.CallbackQuery(data=b"flags_set_amazon_tag"))
async def flags_set_amazon_tag_legacy(event):
    """Legacy — ab naye aff_adm_set_tag|amazon se handle hoga."""
    await event.answer()
    if not is_admin(event.sender_id): return
    event.data = b"aff_adm_set_tag|amazon"


@bot.on(events.CallbackQuery(data=b"flags_set_flipkart_id"))
async def flags_set_flipkart_id_legacy(event):
    """Legacy — ab naye aff_adm_set_tag|flipkart se handle hoga."""
    await event.answer()
    if not is_admin(event.sender_id): return
    event.data = b"aff_adm_set_tag|flipkart"


# ── Force Watermark ───────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"flags_watermark_menu"))
async def flags_watermark_menu(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from feature_flags import get_flag
    force   = get_flag("force_watermark_all")
    text_wm = get_flag("force_watermark_text") or "❌ Not Set"
    allow   = get_flag("allow_user_watermark")
    text = (
        "🖼️ **Force Watermark Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Force All:** `{'ON ✅' if force else 'OFF ❌'}`\n"
        f"**Forced Text:** `{text_wm}`\n"
        f"**Allow User Watermark:** `{'Yes' if allow else 'No'}`\n\n"
        "ℹ️ Force ON karne se sabke photos pe\n"
        "admin ka watermark lagega, chahe user ne\n"
        "apna watermark set kiya ho ya nahi."
    )
    btns = [
        [Button.inline(f"{'✅ Force ON' if not force else '❌ Force OFF'}", b"flags_toggle_force_wm")],
        [Button.inline("✏️ Set Force Text", b"flags_set_force_wm_text")],
        [Button.inline(f"{'🔒 Block User WM' if allow else '🔓 Allow User WM'}", b"flags_toggle_user_wm")],
        [Button.inline("🔙 Admin Panel", b"adm_main")],
    ]
    await event.edit(text, buttons=btns)


@bot.on(events.CallbackQuery(data=b"flags_toggle_force_wm"))
async def flags_toggle_force_wm(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from feature_flags import get_flag, set_flag
    set_flag("force_watermark_all", not get_flag("force_watermark_all"))
    from database import save_to_mongo as _stm5
    try:
        asyncio.get_running_loop().create_task(_stm5())
    except RuntimeError: pass
    await event.answer("Toggled!")
    await flags_watermark_menu(event)


@bot.on(events.CallbackQuery(data=b"flags_toggle_user_wm"))
async def flags_toggle_user_wm(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from feature_flags import get_flag, set_flag
    set_flag("allow_user_watermark", not get_flag("allow_user_watermark"))
    from database import save_to_mongo as _stm6
    try:
        asyncio.get_running_loop().create_task(_stm6())
    except RuntimeError: pass
    await event.answer("Toggled!")
    await flags_watermark_menu(event)


@bot.on(events.CallbackQuery(data=b"flags_set_force_wm_text"))
async def flags_set_force_wm_text(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from database import get_user_data
    data = get_user_data(event.sender_id)
    data["step"] = "admin_set_force_wm_text"
    await _save_step(data)
    await event.edit(
        "✏️ **Forced Watermark Text Set Karo**\n\n"
        "Example: `@YourChannel` ya `YourBrand.com`\n\n"
        "Text send karo:",
        buttons=[[Button.inline("❌ Cancel", b"flags_watermark_menu")]]
    )


@bot.on(events.CallbackQuery(data=b"flags_set_alert_channel"))
async def flags_set_alert_channel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from database import get_user_data
    data = get_user_data(event.sender_id)
    data["step"] = "admin_set_alert_channel"
    await _save_step(data)
    await event.edit(
        "🔔 **Alert Channel Set Karo**\n\n"
        "Saare admin alerts is channel mein jaayenge.\n\n"
        "Channel ID bhejo (e.g. `-1001234567890`)\n"
        "Ya `0` bhejo owner ke DM ke liye:",
        buttons=[[Button.inline("❌ Cancel", b"adm_feature_flags")]]
    )


# ══════════════════════════════════════════
# UNIFIED NOTIFICATIONS PANEL
# Sab notifications ek jagah — admin control
# ══════════════════════════════════════════

def _notif_panel_text():
    from feature_flags import get_flag

    def tick(key):
        return "🟢" if get_flag(key) else "🔴"

    ch = get_flag("alert_channel_id") or "Owner DM (default)"
    return (
        "🔔 **Notifications Control Panel**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Admin Notifications (owner ko jaata hai):**\n"
        f"  {tick('notify_new_user')} Naya user join hone par\n"
        f"  {tick('notify_new_premium')} Premium activate hone par\n"
        f"  {tick('notify_payment')} Payment receive hone par\n"
        f"  {tick('notify_worker_dead')} Worker crash hone par\n"
        f"  {tick('notify_db_warning')} DB full hone par (warning)\n"
        f"  {tick('notify_daily_summary')} Daily summary (midnight)\n\n"
        "**User Notifications (user ko jaata hai):**\n"
        f"  {tick('smart_notifications')} Smart alerts (session expire, limit, etc)\n\n"
        f"📢 **Alert Channel:** `{ch}`\n"
        "_(sab alerts is channel mein jaate hain agar set ho)_\n\n"
        "🟢 = ON  🔴 = OFF  — button dabao toggle karne ke liye"
    )


def _notif_panel_buttons():
    from feature_flags import get_flag
    def label(key, name):
        s = "🟢" if get_flag(key) else "🔴"
        return Button.inline(f"{s} {name}", f"notif_tog_{key}".encode())

    return [
        [label("notify_new_user",      "New User"),
         label("notify_new_premium",   "New Premium")],
        [label("notify_payment",       "Payment"),
         label("notify_worker_dead",   "Worker Dead")],
        [label("notify_db_warning",    "DB Warning"),
         label("notify_daily_summary", "Daily Summary")],
        [label("smart_notifications",  "Smart User Alerts")],
        [Button.inline("🔕 Sab Band Karo",  b"notif_all_off"),
         Button.inline("🔔 Sab Chalu Karo", b"notif_all_on")],
        [Button.inline("📢 Alert Channel Set", b"flags_set_alert_channel")],
        [Button.inline("🔙 Admin Panel", b"adm_main")],
    ]


@bot.on(events.CallbackQuery(data=b"adm_notifications"))
async def adm_notifications_panel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    try:
        await event.edit(_notif_panel_text(), buttons=_notif_panel_buttons())
    except errors.MessageNotModifiedError:
        await event.answer("Already up to date!")


@bot.on(events.CallbackQuery(pattern=b"notif_tog_"))
async def notif_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from feature_flags import get_flag, set_flag
    key = event.data.decode().replace("notif_tog_", "", 1)
    current = get_flag(key)
    if current is None:
        return await event.answer("Invalid key", alert=True)
    set_flag(key, not current)
    await _save_step(get_user_data(event.sender_id))
    await adm_notifications_panel(event)


@bot.on(events.CallbackQuery(data=b"notif_all_off"))
async def notif_all_off(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from feature_flags import set_flag
    for k in ["notify_new_user", "notify_new_premium", "notify_payment",
              "notify_worker_dead", "notify_db_warning", "notify_daily_summary",
              "smart_notifications"]:
        set_flag(k, False)
    await _save_step(get_user_data(event.sender_id))
    await event.answer("🔕 Sab notifications band!", alert=True)
    await adm_notifications_panel(event)


@bot.on(events.CallbackQuery(data=b"notif_all_on"))
async def notif_all_on(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from feature_flags import set_flag
    for k in ["notify_new_user", "notify_new_premium", "notify_payment",
              "notify_worker_dead", "notify_db_warning", "notify_daily_summary",
              "smart_notifications"]:
        set_flag(k, True)
    await _save_step(get_user_data(event.sender_id))
    await event.answer("🔔 Sab notifications chalu!", alert=True)
    await adm_notifications_panel(event)


# ── Anti-Spam redirects to new panel ────────────────────
@bot.on(events.CallbackQuery(data=b"flags_antispam_menu"))
async def flags_antispam_menu_redirect(event):
    """Redirect to new advanced anti-spam panel."""
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    # Import and show new panel
    from ui.anti_spam_menu import as_main
    await as_main(event)
