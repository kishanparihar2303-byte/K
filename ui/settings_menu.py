# ui/settings_menu.py
# ISSUE 5 & 7 FIX: Settings UI clear aur user-friendly banaya
import time
import datetime
from telethon import events, Button, errors
from config import bot
from database import get_user_data, save_persistent_db, save_to_mongo
import asyncio

def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

async def _save_step(data):
    """Step save karo — JSON + MongoDB dono mein."""
    save_persistent_db()
    try:
        asyncio.create_task(save_to_mongo())   # ⚡ Non-blocking
    except Exception:
        pass

def _save_bg():
    """⚡ Fire-and-forget save — settings toggles ke liye instant response."""
    save_persistent_db()
    try:
        asyncio.create_task(save_to_mongo())
    except Exception:
        pass

# Alias — _save() aur _save_bg() dono kaam karein
_save = _save_bg
from .main_menu import get_main_buttons

PREMIUM_FEATURES_MAP = {
    "smart_filter": "smart_filter",
    "auto_shorten": "auto_shorten",
    "duplicate_filter": "duplicate_filter",
    "custom_delay": "custom_delay",
}


# BUG FIX: Ye purana handler disable kiya — settings_menu callback ke liye
# do handlers the (line ~41 aur line ~1871), dono fire hote the jisse
# pehle purana menu flash hota tha phir naya aata tha.
# Naya tabbed handler (stab_ wala) ab sab kuch handle karta hai.
# toggle_ callbacks ab naye handler mein "settings_menu" ke saath combined hain.
@bot.on(events.CallbackQuery(pattern=b"_old_settings_handler_disabled_"))
async def settings_handler_old_disabled(event):
    await event.answer()
    uid = event.sender_id
    if not get_user_data(uid)["session"]:
        return await event.answer("⚠️ Please Login First!", alert=True)
    await event.answer()
    data = get_user_data(uid)
    # FIX J: Clear non-settings steps when entering settings menu
    current_step = data.get("step", "")
    if current_step and not str(current_step).startswith(("wait_delay", "wait_src", "wait_dest")):
        data["step"] = None
        data.pop("step_since", None)
    cmd = event.data.decode()

    if cmd.startswith("toggle_") and "src" not in cmd:
        key = cmd.replace("toggle_", "")
        if key in data["settings"]:
            from premium import can_use_feature, is_feature_paid
            prem_key = PREMIUM_FEATURES_MAP.get(key)
            if prem_key and is_feature_paid(prem_key) and not can_use_feature(event.sender_id, prem_key):
                await event.answer("🔒 Ye Premium feature hai! /premium dekhein.", alert=True)
                return
            data["settings"][key] = not data["settings"][key]
            save_persistent_db()

    s = data["settings"]

    def state_btn(label, key):
        state = "🟢" if s[key] else "🔴"
        return Button.inline(f"{state} {label}", f"toggle_{key}".encode())

    from premium import can_use_feature, is_feature_paid

    def prem_btn(label, key, feat_key=None):
        if feat_key and is_feature_paid(feat_key) and not can_use_feature(event.sender_id, feat_key):
            return Button.inline(f"🔒 {label}", f"toggle_{key}".encode())
        state = "🟢" if s[key] else "🔴"
        return Button.inline(f"{state} {label}", f"toggle_{key}".encode())

    # Build compact status summary
    media_on  = [k for k in ["text","image","video","caption","voice","files"] if s.get(k)]
    media_off = [k for k in ["text","image","video","caption","voice","files"] if not s.get(k)]
    mods_on   = []
    if s.get("remove_links"):   mods_on.append("🚫Links")
    if s.get("remove_user"):    mods_on.append("👤User")
    if s.get("smart_filter"):   mods_on.append("🧠Smart")
    if s.get("auto_shorten"):   mods_on.append("✂️Shorten")
    dup_on    = s.get("duplicate_filter") or s.get("product_duplicate_filter")
    active_features = len(mods_on) + (1 if dup_on else 0)

    txt = (
        "⚙️ **GLOBAL SETTINGS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ Ye settings **SABHI** sources par apply hoti hain\n"
        "_Ek source ke liye alag rules → Main Menu → 🔧 Src Config_\n\n"
        f"**📊 Quick Status:** {active_features} modifications active\n"
        + (f"  Media ON: `{'  '.join(media_on)}`\n" if media_on else "")
        + (f"  Media OFF: `{'  '.join(media_off)}`\n" if media_off else "")
        + (f"  Active mods: `{'  '.join(mods_on)}`\n" if mods_on else "")
        + "\n**🟢 = On   🔴 = Off   🔒 = Premium**\n"
    )

    buttons = [
        [Button.inline("── 📨 Media Types ──", b"settings_menu")],
        [state_btn("Text",    "text"),    state_btn("Image",   "image"),
         state_btn("Video",   "video"),   state_btn("Caption", "caption")],
        [state_btn("Voice",   "voice"),   state_btn("Files",   "files")],

        [Button.inline("── ✂️ Modifications ──", b"settings_menu")],
        [state_btn("🚫 Remove Links",     "remove_links"),
         state_btn("👤 Remove Username",  "remove_user")],
        [prem_btn("🧠 Smart Filter",      "smart_filter",  "smart_filter"),
         prem_btn("✂️ Auto Shorten",      "auto_shorten",  "auto_shorten")],
        [state_btn("🔗 Link Preview",     "preview_mode")],

        [Button.inline("── ♻️ Duplicate Filter ──", b"settings_menu")],
        [prem_btn("♻️ Dup Filter",         "duplicate_filter",         "duplicate_filter"),
         state_btn("🌐 Global Scope",      "global_filter")],
        [state_btn("🛒 Product Dup",       "product_duplicate_filter"),
         Button.inline("⚙️ Dup Settings",  b"dup_menu")],

        [Button.inline("── ⏱ Delay & Media ──", b"settings_menu")],
        [Button.inline(f"⏱ Delay: {s['custom_delay']}s", b"set_delay_flow"),
         Button.inline(f"±{s.get('delay_variance',0)}s variance", b"fwd_set_variance")],
        [Button.inline(f"{'🟢' if s.get('sticker',False) else '🔴'} Sticker",  b"fwd_toggle_sticker"),
         Button.inline(f"{'🟢' if s.get('gif',True) else '🔴'} GIF",           b"fwd_toggle_gif"),
         Button.inline(f"{'🟢' if s.get('copy_mode',False) else '🔴'} No Fwd Tag", b"fwd_toggle_copy_mode")],

        [Button.inline("── 🔧 Advanced ──", b"settings_menu")],
        [Button.inline("⚙️ Fwd Filters v2",  b"fwd_filters_menu"),
         Button.inline("🗺️ Keyword Routes",   b"fwd_keyword_routes")],
        [Button.inline("📊 Src Stats",        b"fwd_src_stats"),
         Button.inline("🔢 Fwd Limits",       b"fwd_set_count_limit")],

        [Button.inline("❓ Explain All",      b"help_settings"),
         Button.inline("🔄 Reset Defaults",   b"settings_reset_confirm")],
        [Button.inline("🔧 Src Config",       b"ps_menu"),
         Button.inline("🏠 Main Menu",        b"main_menu")],
    ]
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass




@bot.on(events.CallbackQuery(data=b"sched_list_holidays"))
async def sched_list_holidays(event):
    await event.answer()
    data     = get_user_data(event.sender_id)
    sched    = data.setdefault("scheduler", {})
    holidays = sched.get("holidays", [])
    if not holidays:
        await event.answer("Koi holiday set nahi hai.", alert=True)
        return await sched_per_day_menu(event)
    btns = []
    for i, h in enumerate(holidays):
        btns.append([Button.inline(f"🗑 {h}", f"sched_del_hol_{i}".encode())])
    btns.append([Button.inline("🗑 Sab Hatao", b"sched_clear_holidays")])
    btns.append([Button.inline("🔙 Back", b"sched_per_day_menu")])
    try:
        await event.edit(f"🗓️ **Holidays ({len(holidays)})**\nDelete karne ke liye tap karo:", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"sched_del_hol_"))
async def sched_del_holiday(event):
    await event.answer()
    idx   = int(event.data.decode().split("_")[-1])
    data  = get_user_data(event.sender_id)
    hols  = data.setdefault("scheduler", {}).setdefault("holidays", [])
    if idx < len(hols):
        removed = hols.pop(idx)
        save_persistent_db()
        await event.answer(f"Removed: {removed}")
    await sched_list_holidays(event)


@bot.on(events.CallbackQuery(data=b"sched_clear_holidays"))
async def sched_clear_holidays(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data.setdefault("scheduler", {})["holidays"] = []
    save_persistent_db()
    await event.answer("Sab holidays hata di!", alert=True)
    await sched_per_day_menu(event)


@bot.on(events.CallbackQuery(data=b"settings_reset_confirm"))
async def settings_reset_confirm(event):
    await event.answer()
    try:
        await event.edit(
            "🔄 **Global Settings Reset**\n\n"
            "Ye action **saari Global Settings** ko default pe wapas kar dega.\n\n"
            "⚠️ Tumhare sources, destinations, aur Src Config safe rahenge.\n"
            "Sirf Global Settings reset hongi.\n\n"
            "Confirm karo?",
            buttons=[
                [Button.inline("✅ Haan, Reset Karo", b"settings_do_reset"),
                 Button.inline("❌ Cancel", b"settings_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"settings_do_reset"))
async def settings_do_reset(event):
    await event.answer()
    from database import save_persistent_db, get_user_data
    from config import DEFAULT_SETTINGS
    uid  = event.sender_id
    data = get_user_data(uid)

    # Sirf settings reset — baaki sab safe
    data["settings"] = DEFAULT_SETTINGS.copy()
    save_persistent_db()

    # Problem 6 Fix: pehle confirm message edit karo SUCCESS message se
    # Phir settings_menu dikhao — warna confirm dialog reh jaata hai
    try:
        await event.edit(
            "✅ **Global Settings Reset Ho Gayi!**\n\n"
            "Saari settings default pe wapas aa gayi hain.\n"
            "Sources, Destinations, Src Config sab safe hain.\n\n"
            "↓ Settings menu mein wapas jao:",
            buttons=[
                [Button.inline("⚙️ Settings Menu", b"settings_menu")],
                [Button.inline("🏠 Main Menu",     b"main_menu")],
            ]
        )
    except Exception:
        await event.answer("✅ Settings reset ho gayi!", alert=True)
        try:
            await settings_menu(event)
        except Exception:
            pass


@bot.on(events.CallbackQuery(data=b"set_delay_flow"))
async def set_delay_flow(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    cur  = data["settings"].get("custom_delay", 0)

    txt = (
        "⏱ **MESSAGE DELAY**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Current delay: **{cur}s**\n\n"
        "**Kyun use karte hain?**\n"
        "• Telegram rate limits se bachne ke liye\n"
        "• Spam-like feel avoid karne ke liye\n"
        "• High-volume sources ke liye recommended: 3-5s\n\n"
        "Preset chuniye ya custom value type karo:"
    )
    data["step"] = "wait_delay"
    data["step_since"] = time.time()
    try:
        await event.edit(txt, buttons=[
            [Button.inline("⚡ 0s (instant)",  b"delay_preset_0"),
             Button.inline("1s",               b"delay_preset_1"),
             Button.inline("2s",               b"delay_preset_2")],
            [Button.inline("3s",               b"delay_preset_3"),
             Button.inline("5s",               b"delay_preset_5"),
             Button.inline("10s",              b"delay_preset_10")],
            [Button.inline("30s",              b"delay_preset_30"),
             Button.inline("60s (1 min)",      b"delay_preset_60")],
            [Button.inline("✏️ Custom value type karo", b"settings_menu")],
            [Button.inline("🔙 Settings",      b"settings_menu")],
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"delay_reset"))
async def delay_reset_cb(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["settings"]["custom_delay"] = 0
    save_persistent_db()
    await event.answer("⏱ Delay 0 ho gaya!")
    await settings_handler(event)



@bot.on(events.CallbackQuery(pattern=b"delay_preset_"))
async def delay_preset_cb(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    val  = int(event.data.decode().replace("delay_preset_", ""))
    data["settings"]["custom_delay"] = val
    data["step"] = None
    _save_bg()
    await event.answer(f"\u2705 Delay: {val}s set!", alert=False)
    await settings_handler(event)
@bot.on(events.CallbackQuery(pattern=b"delay_unit_"))
async def delay_unit_set(event):
    await event.answer()
    unit = event.data.decode().split("_")[-1]
    get_user_data(event.sender_id)["temp_data"]["delay_unit"] = unit
    get_user_data(event.sender_id)["step"] = "wait_delay_val"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            f"⏱ **{unit} mein number bhejo:**\n\nExample: `5`" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""),
            buttons=[Button.inline("🔙 Back", b"set_delay_flow")]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"set_start"))
async def set_start_cb(event):
    await event.answer()
    get_user_data(event.sender_id)["step"] = "wait_start_msg"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **Start Message Set Karo**\n\n"
            "Ye text har message ke UPAR lagega.\n"
            "Example: `🔥 Deal of the Day:`\n\n"
            "Text bhejo:",
            buttons=[Button.inline("🔙 Cancel", b"main_menu")]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"set_end"))
async def set_end_cb(event):
    await event.answer()
    get_user_data(event.sender_id)["step"] = "wait_end_msg"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "✏️ **End Message Set Karo**\n\n"
            "Ye text har message ke NEECHE lagega.\n"
            "Example: `📢 Join @mychannel`\n\n"
            "Text bhejo:",
            buttons=[Button.inline("🔙 Cancel", b"main_menu")]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"rem_start"))
async def rem_start_msg_cb(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    current = data["settings"].get("start_msg", "")
    if not current:
        return await event.answer("Start message already set nahi hai!", alert=True)
    try:
        preview = current[:50] + ("..." if len(current) > 50 else "")
        try:
            await event.edit(
                f"⚠️ **Confirm Karo**\n\n"
                f"Kya tum **Start Message** delete karna chahte ho?\n\n"
                f"Current: `{preview}`",
                buttons=[
                    [Button.inline("🗑 Haan, Delete Karo", b"rem_start_confirm"),
                     Button.inline("❌ Cancel", b"main_menu")],
                ]
            )
        except errors.MessageNotModifiedError:
            pass
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"rem_start_confirm"))
async def rem_start_confirm_cb(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["settings"]["start_msg"] = ""
    save_persistent_db()
    await event.answer("✅ Start Message delete ho gaya!", alert=False)
    try:
        await event.edit("✅ Start Message remove ho gaya.", buttons=get_main_buttons(event.sender_id))
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"rem_end"))
async def rem_end_msg_cb(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    current = data["settings"].get("end_msg", "")
    if not current:
        return await event.answer("End message already set nahi hai!", alert=True)
    try:
        preview = current[:50] + ("..." if len(current) > 50 else "")
        try:
            await event.edit(
                f"⚠️ **Confirm Karo**\n\n"
                f"Kya tum **End Message** delete karna chahte ho?\n\n"
                f"Current: `{preview}`",
                buttons=[
                    [Button.inline("🗑 Haan, Delete Karo", b"rem_end_confirm"),
                     Button.inline("❌ Cancel", b"main_menu")],
                ]
            )
        except errors.MessageNotModifiedError:
            pass
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"rem_end_confirm"))
async def rem_end_confirm_cb(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["settings"]["end_msg"] = ""
    save_persistent_db()
    await event.answer("✅ End Message delete ho gaya!", alert=False)
    try:
        await event.edit("✅ End Message remove ho gaya.", buttons=get_main_buttons(event.sender_id))
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════════════════════
# NEW FEATURES — USER SETTINGS
# Watermark, Affiliate, AI Rewrite, Per-Day Scheduler
# ══════════════════════════════════════════════════════════

# ── WATERMARK SETTINGS ─────────────────────────────────

def _wm_main_text(uid):
    from database import get_user_data
    from feature_flags import get_flag
    from watermark import get_logo_path
    data    = get_user_data(uid)
    wm      = data.get("watermark", {})
    enabled = wm.get("enabled", False)
    mode    = wm.get("mode", "text")
    text_wm = wm.get("text", "") or "❌ Set nahi kiya"
    pos     = wm.get("position", "bottom_right")
    opacity = wm.get("opacity", 60)
    size    = wm.get("size", "medium")
    color   = wm.get("color", "white")
    scale   = wm.get("logo_scale", 15)
    has_logo = bool(get_logo_path(uid))
    logo_status = "✅ Uploaded" if has_logo else "❌ Upload nahi kiya"
    force_text = get_flag("force_watermark_text") or ""
    force_note = f"\n\n⚠️ Admin override: `{force_text}`" if force_text else ""

    mode_icon = {"text": "✏️ Sirf Text", "image": "🖼️ Sirf Logo/Image", "both": "✏️+🖼️ Text + Logo"}.get(mode, mode)
    pos_clean = pos.replace("_", " ").title()

    # Smart readiness check
    ready_issues = []
    if mode in ("text", "both") and not wm.get("text"):
        ready_issues.append("• Text set karo")
    if mode in ("image", "both") and not has_logo:
        ready_issues.append("• Logo upload karo")
    ready_line = "\n⚠️ _Abhi kaam nahi karega:_\n" + "\n".join(ready_issues) if (enabled and ready_issues) else ""

    status_dot = "🟢 **Active**" if enabled else "🔴 **Off**"
    return (
        "🖼️ **AUTO WATERMARK**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: {status_dot}\n"
        f"Type: **{mode_icon}**\n"
        f"{ready_line}\n\n"
        "**Content:**\n"
        f"  ✏️ Text: `{text_wm}`\n"
        f"  🖼️ Logo: {logo_status}\n\n"
        "**Style:**\n"
        f"  📍 Position: `{pos_clean}`\n"
        f"  🌫️ Opacity: `{opacity}%`  •  📐 Size: `{str(size).title()}`\n"
        f"  🎨 Color: `{str(color).title()}`  •  📏 Logo Scale: `{scale}%`"
        f"{force_note}"
    )


@bot.on(events.CallbackQuery(data=b"settings_watermark"))
async def watermark_settings(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    wm   = data.get("watermark", {})

    from feature_flags import watermark_available
    if not watermark_available(uid):
        return await event.edit(
            "🖼️ **Auto Watermark — Premium Feature**\n\n"
            "🔒 Har forwarded image/video par apna brand/text lagao.\n\n"
            "Premium lo aur activate karo!",
            buttons=[[Button.inline("💎 Premium Info", b"premium_info"),
                      Button.inline("🏠 Menu", b"main_menu")]]
        )

    enabled = wm.get("enabled", False)
    mode    = wm.get("mode", "text")
    btns = [
        # Master toggle — most prominent
        [Button.inline(f"{'🔴 Watermark Band Karo' if enabled else '🟢 Watermark Chalu Karo'}", b"wm_toggle_enabled")],

        # Type selection
        [Button.inline("── Watermark Type ──", b"settings_watermark")],
        [Button.inline(f"{'▶ ' if mode=='text' else ''}✏️ Sirf Text",       b"wm_mode_text"),
         Button.inline(f"{'▶ ' if mode=='image' else ''}🖼️ Sirf Logo",      b"wm_mode_image"),
         Button.inline(f"{'▶ ' if mode=='both' else ''}✏️+🖼️ Dono",        b"wm_mode_both")],

        # Content
        [Button.inline("── Content Set Karo ──", b"settings_watermark")],
        [Button.inline("✏️ Text Likhao",         b"wm_set_text"),
         Button.inline("🗑 Text Hatao",           b"wm_remove_text")],
        [Button.inline("🖼️ Logo Upload Karo",    b"wm_upload_logo"),
         Button.inline("🗑 Logo Delete Karo",     b"wm_delete_logo")],

        # Positioning & Style
        [Button.inline("── Style ──", b"settings_watermark")],
        [Button.inline("📍 Position",             b"wm_pos_menu"),
         Button.inline("🎨 Color",                b"wm_color_menu")],
        [Button.inline("📐 Text Size",            b"wm_size_menu"),
         Button.inline("🌫️ Opacity (Transparency)", b"wm_opacity_menu")],
        [Button.inline("📏 Logo Size %",          b"wm_scale_menu"),
         Button.inline("👁️ Preview (Test karo)",  b"wm_preview")],

        [Button.inline("🔄 Sab Reset Karo",       b"wm_reset_all")],
        [Button.inline("🏠 Main Menu",             b"main_menu")],
    ]
    txt = _wm_main_text(uid)
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass
    except Exception:
        await event.respond(txt, buttons=btns)



@bot.on(events.CallbackQuery(data=b"wm_remove_text"))
async def wm_remove_text(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data.setdefault("watermark", {})["text"] = ""
    save_persistent_db()
    await event.answer("✅ Watermark text hata diya!", alert=True)
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_reset_all"))
async def wm_reset_all(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["watermark"] = {
        "enabled": False,
        "text": "",
        "mode": "text",
        "position": "bottom_right",
        "color": "white",
        "size": 24,
        "opacity": 128,
        "logo_path": None,
        "logo_scale": 0.15,
    }
    save_persistent_db()
    await event.answer("✅ Watermark settings reset ho gayi!", alert=True)
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_toggle_enabled"))
async def wm_toggle(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    wm   = data.setdefault("watermark", {})
    wm["enabled"] = not wm.get("enabled", False)
    status = "🟢 ON" if wm["enabled"] else "🔴 OFF"
    save_persistent_db()
    try:
        await save_to_mongo()
    except Exception:
        pass
    await event.answer(f"Watermark {status}")
    await watermark_settings(event)


# Mode buttons
@bot.on(events.CallbackQuery(pattern=b"wm_mode_"))
async def wm_mode_set(event):
    await event.answer()
    mode = event.data.decode().replace("wm_mode_", "")
    data = get_user_data(event.sender_id)
    data.setdefault("watermark", {})["mode"] = mode
    save_persistent_db()
    labels = {"text": "Text Only ✏️", "image": "Image Only 🖼️", "both": "Text + Image ✏️🖼️"}
    await event.answer(f"Mode: {labels.get(mode, mode)}")
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_set_text"))
async def wm_set_text(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"] = "wait_watermark_text"
    data["step_since"] = time.time()
    await _save_step(data)
    try:
        await event.edit(
            "✏️ **Watermark Text Set Karo**\n\n"
            "Example: `@YourChannel`\n"
            "Example: `YourBrand.com`\n"
            "Example: `© 2026 YourName`\n\n"
            "Text bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"settings_watermark")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"wm_upload_logo"))
async def wm_upload_logo(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"] = "wait_watermark_logo"
    data["step_since"] = time.time()
    await _save_step(data)
    try:
        await event.edit(
            "🖼️ **Apna Logo/Image Upload Karo**\n\n"
            "✅ PNG (best — transparency support)\n"
            "✅ JPG/JPEG\n"
            "✅ WebP\n\n"
            "**Tips:**\n"
            "• PNG with transparent background best lagta hai\n"
            "• Simple logo/icon best kaam karta hai\n"
            "• Max size: 5MB\n\n"
            "Ab apna logo image bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"settings_watermark")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"wm_delete_logo"))
async def wm_delete_logo(event):
    await event.answer()
    uid  = event.sender_id
    from watermark import delete_logo, get_logo_path
    if not get_logo_path(uid):
        return await event.answer("❌ Koi logo upload nahi hai.", alert=True)
    delete_logo(uid)
    save_persistent_db()
    await event.answer("🗑️ Logo delete ho gaya!")
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_preview"))
async def wm_preview(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    wm   = data.get("watermark", {})

    from watermark import get_logo_path, generate_preview, get_user_watermark_settings
    settings = get_user_watermark_settings(uid) or wm
    if not settings:
        return await event.answer("❌ Watermark settings pehle set karo!", alert=True)

    await event.answer("🖼️ Preview generate ho raha hai...")
    logo_path = get_logo_path(uid)
    preview   = generate_preview(settings, logo_path=logo_path)
    if preview:
        import io
        buf = io.BytesIO(preview)
        buf.name = "watermark_preview.jpg"
        await event.respond(
            "👁️ **Watermark Preview:**\n(Aisi dikhegi tumhare photos pe)",
            file=buf
        )
    else:
        await event.respond("❌ Preview generate nahi ho saka. Settings check karo.")


@bot.on(events.CallbackQuery(pattern=b"wm_pos_"))
async def wm_position_set(event):
    await event.answer()
    if event.data == b"wm_pos_menu":
        positions = [
            ("Bottom Right ↘️", "bottom_right"),
            ("Bottom Left ↙️",  "bottom_left"),
            ("Top Right ↗️",    "top_right"),
            ("Top Left ↖️",     "top_left"),
            ("Center ⊕",        "center"),
            ("Bottom Center ⬇️","bottom_center"),
            ("Top Center ⬆️",   "top_center"),
        ]
        btns = [[Button.inline(label, f"wm_pos_{val}".encode())] for label, val in positions]
        btns.append([Button.inline("🔙 Back", b"settings_watermark")])
        return await event.edit("📍 **Position Choose Karo:**", buttons=btns)
    pos  = event.data.decode().replace("wm_pos_", "")
    data = get_user_data(event.sender_id)
    data.setdefault("watermark", {})["position"] = pos
    save_persistent_db()
    await event.answer(f"📍 {pos}")
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_color_menu"))
async def wm_color_menu(event):
    await event.answer()
    colors = [
        ("⬜ White",  "white"),
        ("⬛ Black",  "black"),
        ("🟡 Yellow", "yellow"),
        ("🔴 Red",    "red"),
        ("🔵 Blue",   "blue"),
        ("🟢 Green",  "green"),
    ]
    btns = [[Button.inline(label, f"wm_clr_{val}".encode())] for label, val in colors]
    btns.append([Button.inline("🔙 Back", b"settings_watermark")])
    try:
        await event.edit("🎨 **Text Color Choose Karo:**", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"wm_clr_"))
async def wm_color_set(event):
    await event.answer()
    color = event.data.decode().replace("wm_clr_", "")
    data  = get_user_data(event.sender_id)
    data.setdefault("watermark", {})["color"] = color
    save_persistent_db()
    await event.answer(f"🎨 Color: {color}")
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_size_menu"))
async def wm_size_menu(event):
    await event.answer()
    sizes = [("🔤 Small", "small"), ("🔡 Medium", "medium"), ("🔠 Large", "large")]
    btns  = [[Button.inline(label, f"wm_sz_{val}".encode())] for label, val in sizes]
    btns.append([Button.inline("🔙 Back", b"settings_watermark")])
    try:
        await event.edit("📐 **Text Size Choose Karo:**", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"wm_sz_"))
async def wm_size_set(event):
    await event.answer()
    size = event.data.decode().replace("wm_sz_", "")
    data = get_user_data(event.sender_id)
    data.setdefault("watermark", {})["size"] = size
    save_persistent_db()
    await event.answer(f"📐 Size: {size}")
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_opacity_menu"))
async def wm_opacity_menu(event):
    await event.answer()
    levels = [
        ("👻 Ghost (20%)",   "20"),
        ("🌫️ Subtle (40%)",  "40"),
        ("☁️ Medium (60%)",  "60"),
        ("🔲 Strong (80%)",  "80"),
        ("⬛ Full (100%)",   "100"),
    ]
    btns = [[Button.inline(label, f"wm_op_{val}".encode())] for label, val in levels]
    btns.append([Button.inline("🔙 Back", b"settings_watermark")])
    try:
        await event.edit("🌫️ **Opacity (Transparency) Choose Karo:**\n\nKam = zyada transparent", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"wm_op_"))
async def wm_opacity_set(event):
    await event.answer()
    try:
        raw     = event.data.decode().replace("wm_op_", "")
        opacity = max(0, min(100, int(raw)))   # Clamp 0-100
    except (ValueError, TypeError):
        await event.answer("❌ Invalid opacity value", alert=True)
        return
    data = get_user_data(event.sender_id)
    data.setdefault("watermark", {})["opacity"] = opacity
    save_persistent_db()
    await event.answer(f"🌫️ Opacity: {opacity}%")
    await watermark_settings(event)


@bot.on(events.CallbackQuery(data=b"wm_scale_menu"))
async def wm_scale_menu(event):
    """Logo ka size — image width ka kitna % hoga."""
    await event.answer()
    scales = [
        ("🔹 Tiny (5%)",    "5"),
        ("🔸 Small (10%)",  "10"),
        ("🟠 Medium (15%)", "15"),
        ("🟡 Large (25%)",  "25"),
        ("🔴 XL (35%)",     "35"),
    ]
    btns = [[Button.inline(label, f"wm_sc_{val}".encode())] for label, val in scales]
    btns.append([Button.inline("🔙 Back", b"settings_watermark")])
    try:
        await event.edit(
            "📏 **Logo Size Set Karo**\n\nImage width ka kitna % hoga logo?",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"wm_sc_"))
async def wm_scale_set(event):
    await event.answer()
    try:
        raw   = event.data.decode().replace("wm_sc_", "")
        scale = max(1, min(50, int(raw)))   # Clamp 1-50 prevents ffmpeg memory abuse
    except (ValueError, TypeError):
        await event.answer("❌ Invalid scale value", alert=True)
        return
    data = get_user_data(event.sender_id)
    data.setdefault("watermark", {})["logo_scale"] = scale
    save_persistent_db()
    await event.answer(f"📏 Logo Scale: {scale}%")
    await watermark_settings(event)

# ── AFFILIATE SETTINGS ─────────────────────────────────
@bot.on(events.CallbackQuery(data=b"settings_affiliate"))
async def affiliate_settings(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)

    from feature_flags import affiliate_available, get_flag
    if not affiliate_available(uid):
        try:
            await event.edit(
                "🔗 **Affiliate Manager**\n\n🔒 Ye Premium feature hai!\n\n/premium se upgrade karo.",
                buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    aff_mode = get_flag("affiliate_mode") or "user"
    if aff_mode == "owner":
        from affiliate import registry
        owner_tags = []
        for p in registry.list_platforms():
            tag = get_flag(f"owner_{p.tag_key}") or ""
            if tag:
                owner_tags.append(f"  {p.icon} {p.name.title()}: `{tag}`")
        tags_txt = "\n".join(owner_tags) or "  ❌ Koi tag set nahi"
        try:
            await event.edit(
                "🔗 **Affiliate Manager**\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "ℹ️ **Owner Mode** — Admin ka tag sabke links mein lagega।\n\n"
                f"**Active Tags:**\n{tags_txt}",
                buttons=[[Button.inline("🏠 Main Menu", b"main_menu")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    aff     = data.get("affiliate", {})
    enabled = aff.get("enabled", False)

    from affiliate import registry, get_affiliate_stats_summary
    stats   = get_affiliate_stats_summary(uid)
    today   = time.strftime("%Y-%m-%d")
    today_c = stats.get("today", {}).get(today, 0)

    # Build platform status
    platform_lines = []
    for p in registry.list_platforms():
        tag = aff.get(p.tag_key, "")
        status = f"✅ `{tag[:20]}`" if tag else "❌ Set Nahi"
        platform_lines.append(f"  {p.icon} {p.name.title()}: {status}")

    status_dot = "🟢 **Active**" if enabled else "🔴 **Inactive**"
    txt = (
        "🔗 **Affiliate Manager**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: {status_dot}\n"
        f"📊 Aaj replace hue: `{today_c}` links  |  Total: `{stats.get('total_replaced',0)}`\n\n"
        "**🛒 Platforms:**\n"
        + "\n".join(platform_lines)
        + "\n\n💡 Link forward hone par automatically affiliate tag lagega।"
    )

    platform_btns = []
    for p in registry.list_platforms():
        tag = aff.get(p.tag_key, "")
        lbl = f"{p.icon} {'✅' if tag else '❌'} {p.name.title()}"
        platform_btns.append(Button.inline(lbl, f"aff_set_{p.name}".encode()))

    # Group 2 per row
    rows = []
    for i in range(0, len(platform_btns), 2):
        rows.append(platform_btns[i:i+2])

    btns = [
        [Button.inline(
            f"{'🔴 Band Karo' if enabled else '🟢 Chalu Karo'} Affiliate",
            b"aff_toggle"
        )],
        *rows,
        [Button.inline("🧪 URL Test Karo",     b"aff_test_url"),
         Button.inline("📊 Stats Dekho",        b"aff_stats")],
        [Button.inline("🏠 Main Menu",           b"main_menu")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"aff_toggle"))
async def aff_toggle(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    aff  = data.setdefault("affiliate", {})
    aff["enabled"] = not aff.get("enabled", False)
    await _save_step(data)
    await affiliate_settings(event)


@bot.on(events.CallbackQuery(pattern=b"aff_set_"))
async def aff_set_platform(event):
    await event.answer()
    platform = event.data.decode().replace("aff_set_", "")
    from affiliate import registry
    plugin = registry.get_plugin(platform)
    if not plugin:
        return await event.answer("❌ Platform nahi mila!", alert=True)

    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = f"wait_aff_{platform}"
    data["step_since"] = time.time()
    await _save_step(data)

    current = data.get("affiliate", {}).get(plugin.tag_key, "")
    try:
        await event.edit(
            f"{plugin.icon} **{platform.title()} Affiliate Tag Set Karo**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + (f"Current: `{current}`\n\n" if current else "")
            + f"Format: `{plugin.example}`\n\n"
            "Tag bhejo 👇\n"
            "_Ya 'remove' type karo tag hatane ke liye_",
            buttons=[
                [Button.inline("🗑 Remove Tag",  f"aff_remove_{platform}".encode())],
                [Button.inline("❌ Cancel",       b"settings_affiliate")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"aff_remove_"))
async def aff_remove_platform(event):
    await event.answer()
    platform = event.data.decode().replace("aff_remove_", "")
    from affiliate import registry
    plugin = registry.get_plugin(platform)
    if not plugin:
        return
    data = get_user_data(event.sender_id)
    data.setdefault("affiliate", {})[plugin.tag_key] = ""
    save_persistent_db()
    await event.answer(f"✅ {platform.title()} tag hata diya!", alert=True)
    await affiliate_settings(event)


@bot.on(events.CallbackQuery(data=b"aff_stats"))
async def aff_stats(event):
    await event.answer()
    uid   = event.sender_id
    from affiliate import get_affiliate_stats_summary, registry
    stats = get_affiliate_stats_summary(uid)
    aff   = get_user_data(uid).get("affiliate", {})

    lines = ["📊 **Affiliate Stats**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"]
    lines.append(f"🔢 Total Links Replaced: `{stats.get('total_replaced', 0)}`")

    # Per platform
    lines.append("\n**Platform Breakdown:**")
    for p in registry.list_platforms():
        count = stats.get(f"{p.name}_replaced", 0)
        tag   = aff.get(p.tag_key, "")
        if tag or count > 0:
            lines.append(f"  {p.icon} {p.name.title()}: `{count}` links  Tag: `{tag or '❌'}`")

    # Last 7 days
    today_data = stats.get("today", {})
    if today_data:
        lines.append("\n**Last 7 Days:**")
        for date in sorted(today_data.keys(), reverse=True):
            lines.append(f"  `{date}`: `{today_data[date]}` links")

    last_ts = stats.get("last_replaced_at", 0)
    if last_ts:
        import datetime
        last_str = datetime.datetime.fromtimestamp(last_ts).strftime("%d/%m %H:%M")
        lines.append(f"\n🕐 Last: `{last_str}`")

    try:
        await event.edit(
            "\n".join(lines),
            buttons=[[Button.inline("🔙 Back", b"settings_affiliate")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"aff_test_url"))
async def aff_test_url(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "wait_aff_test_url"
    data["step_since"] = time.time()
    await _save_step(data)
    try:
        await event.edit(
            "🧪 **URL Test Karo**\n\n"
            "Koi bhi product URL bhejo — main dikhaunga\n"
            "kaise affiliate tag lagega।\n\n"
            "Example:\n"
            "`https://www.amazon.in/dp/B08XYZ123`\n"
            "`https://www.flipkart.com/product/p/xyz`\n\n"
            "URL bhejo 👇",
            buttons=[[Button.inline("❌ Cancel", b"settings_affiliate")]]
        )
    except errors.MessageNotModifiedError:
        pass


# Old handlers for backward compatibility
@bot.on(events.CallbackQuery(data=b"aff_remove_amazon"))
async def _aff_remove_amazon_compat(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data.setdefault("affiliate", {})["amazon_tag"] = ""
    save_persistent_db()
    await event.answer("Amazon tag hata diya!", alert=True)
    await affiliate_settings(event)

@bot.on(events.CallbackQuery(data=b"aff_remove_flipkart"))
async def _aff_remove_flipkart_compat(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data.setdefault("affiliate", {})["flipkart_id"] = ""
    save_persistent_db()
    await event.answer("Flipkart ID hata diya!", alert=True)
    await affiliate_settings(event)

@bot.on(events.CallbackQuery(data=b"aff_set_amazon"))
async def _aff_set_amazon_compat(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"]       = "wait_aff_amazon"
    data["step_since"] = time.time()
    await _save_step(data)
    try:
        await event.edit(
            "🛒 **Amazon Affiliate Tag Set Karo**\n\nFormat: `yourtag-21`\n\nTag bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"settings_affiliate")]]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"aff_set_flipkart"))
async def _aff_set_flipkart_compat(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"]       = "wait_aff_flipkart"
    data["step_since"] = time.time()
    await _save_step(data)
    try:
        await event.edit(
            "🛍️ **Flipkart Affiliate ID Set Karo**\n\nID bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"settings_affiliate")]]
        )
    except errors.MessageNotModifiedError:
        pass



# ── AI REWRITE SETTINGS — REMOVED ────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"ai_set_personal_key"))
async def ai_set_personal_key(event):
    await event.answer()
    await event.answer("❌ AI Rewrite feature band kar di gayi hai.", alert=True)

@bot.on(events.CallbackQuery(data=b"ai_set_user_prompt"))
async def ai_set_user_prompt(event):
    await event.answer()
    await event.answer("❌ AI Rewrite feature band kar di gayi hai.", alert=True)

@bot.on(events.CallbackQuery(data=b"settings_ai"))
async def settings_ai_removed(event):
    await event.answer()
    try:
        await event.edit(
            "🤖 **AI Rewrite**\n\n"
            "❌ Ye feature band kar di gayi hai.\n\n"
            "Wapas jaane ke liye neeche button dabao.",
            buttons=[[Button.inline("🔙 Settings", b"settings_menu"),
                      Button.inline("🏠 Main Menu", b"main_menu")]]
        )
    except Exception:
        pass


# ── PER-DAY SCHEDULER ──────────────────────────────────
@bot.on(events.CallbackQuery(data=b"sched_per_day_menu"))
async def sched_per_day_menu(event):
    await event.answer()  # ⚡ instant ack — prevents button "loading" spinner
    uid  = event.sender_id
    data = get_user_data(uid)

    from feature_flags import scheduler_advanced_available
    _is_adv = scheduler_advanced_available(uid)

    sched    = data.get("scheduler", {})
    enabled  = sched.get("per_day_enabled", False)
    per_day  = sched.get("per_day", {})

    from scheduler import DAY_SHORT, DEFAULT_DAY_SCHEDULE
    queue    = sched.get("queue_mode", False)
    holidays = sched.get("holidays", [])
    curr_tz  = data.get("timezone", "Asia/Kolkata")
    from utils import user_now, TIMEZONE_LIST
    _tz_lbl  = curr_tz
    for _k, _l in TIMEZONE_LIST:
        if _k == curr_tz:
            _tz_lbl = _l; break
    now_in_tz = user_now(uid).strftime("%I:%M %p")
    status   = "🟢 **Active**" if enabled else "🔴 **Inactive**"

    # Build schedule table
    day_lines = []
    for day in DAY_SHORT:
        cfg = per_day.get(day, DEFAULT_DAY_SCHEDULE.copy())
        if cfg.get("enabled", True):
            day_lines.append(f"  ✅ **{day}**: `{cfg['start']}` → `{cfg['end']}`")
        else:
            day_lines.append(f"  ❌ **{day}**: Off")

    hol_txt = ""
    if holidays:
        hol_txt = f"\n🗓️ **Holidays** ({len(holidays)}): " + ", ".join(holidays[:3])
        if len(holidays) > 3:
            hol_txt += f" +{len(holidays)-3} more"

    lines = [
        f"📅 **Per-Day Scheduler**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: {status}  |  🕒 Abhi: {now_in_tz}\n"
        f"⏰ Timezone: {_tz_lbl}\n"
        f"📦 Queue Mode: {'🟢 On' if queue else '🔴 Off'}\n\n"
        f"**Weekly Schedule:**\n" + "\n".join(day_lines) + hol_txt
    ]

    btns = [
        [Button.inline(f"{'🔴 Per-Day OFF' if enabled else '🟢 Per-Day ON'}", b"sched_toggle_perday")],
        [Button.inline("📅 Edit Days", b"sched_edit_days")],
        [Button.inline("🗓️ Add Holiday", b"sched_add_holiday"),
         Button.inline(f"🗑 Holidays ({len(holidays)})", b"sched_list_holidays")],
        [Button.inline(f"{'📦 Queue OFF' if queue else '📦 Queue ON'}", b"sched_toggle_queue")],
        [Button.inline("🏠 Main Menu", b"main_menu")],
    ]
    try:
        await event.edit(lines[0], buttons=btns)
    except errors.MessageNotModifiedError:
        pass   # Same content — OK
    except Exception as _e:
        import logging as _lg
        _lg.getLogger(__name__).warning(f"sched_per_day_menu edit error: {_e}")
        # Fallback: respond as new message
        try:
            await event.respond(lines[0], buttons=btns)
        except Exception:
            pass


@bot.on(events.CallbackQuery(data=b"sched_toggle_perday"))
async def sched_toggle_perday(event):
    await event.answer()
    uid   = event.sender_id
    # FIX E2: Premium check with helpful message
    from feature_flags import scheduler_advanced_available
    if not scheduler_advanced_available(uid):
        return await event.answer(
            "💎 Per-Day Scheduler Premium feature hai! /premium se upgrade karo.",
            alert=True
        )
    data  = get_user_data(uid)
    sched = data.setdefault("scheduler", {})
    sched["per_day_enabled"] = not sched.get("per_day_enabled", False)
    # Initialize per_day agar nahi hai
    if sched["per_day_enabled"] and not sched.get("per_day"):
        from scheduler import get_default_per_day_schedule
        sched["per_day"] = get_default_per_day_schedule()
    save_persistent_db()
    try:
        from database import save_to_mongo
        await save_to_mongo()
    except Exception:
        pass
    await sched_per_day_menu(event)


@bot.on(events.CallbackQuery(data=b"sched_toggle_queue"))
async def sched_toggle_queue(event):
    await event.answer()
    data  = get_user_data(event.sender_id)
    sched = data.setdefault("scheduler", {})
    sched["queue_mode"] = not sched.get("queue_mode", False)
    save_persistent_db()
    await event.answer(f"Queue Mode: {'ON' if sched['queue_mode'] else 'OFF'}")
    await sched_per_day_menu(event)


@bot.on(events.CallbackQuery(data=b"sched_edit_days"))
async def sched_edit_days(event):
    await event.answer()
    from scheduler import DAY_SHORT
    data    = get_user_data(event.sender_id)
    sched   = data.get("scheduler", {})
    per_day = sched.get("per_day", {})
    btns = []
    for day in DAY_SHORT:
        cfg     = per_day.get(day, {})
        enabled = cfg.get("enabled", True)
        status  = "🟢 ON" if enabled else "🔴 OFF"
        start   = cfg.get("start", "09:00 AM")
        end     = cfg.get("end", "10:00 PM")
        label   = f"{status} {day}: {start}→{end}"
        toggle  = f"{'🔴 OFF Karo' if enabled else '🟢 ON Karo'} {day}"
        btns.append([
            Button.inline(label[:28], f"sched_edit_{day}".encode()),
            Button.inline(f"{'🔴' if enabled else '🟢'}", f"sched_toggle_{day}".encode()),
        ])
    btns.append([Button.inline("🔙 Back", b"sched_per_day_menu")])
    try:
        await event.edit("📅 **Din Chuno — Green=ON, Red=OFF**\n(Left = Edit, Right = Toggle)", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"sched_edit_"))
async def sched_edit_day(event):
    await event.answer()
    if event.data == b"sched_edit_days":
        return
    day = event.data.decode().replace("sched_edit_", "")
    data  = get_user_data(event.sender_id)
    sched = data.setdefault("scheduler", {})
    per_day = sched.setdefault("per_day", {})
    cfg = per_day.get(day, {"enabled": True, "start": "09:00 AM", "end": "10:00 PM"})
    enabled = cfg.get("enabled", True)
    status_icon = "🟢 ON" if enabled else "🔴 OFF"
    try:
        await event.edit(
            f"✏️ **{day} Schedule**\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Status: {status_icon}\n"
            f"⏰ Start: `{cfg['start']}`\n"
            f"⏰ End:   `{cfg['end']}`\n\n"
            "Kya badalna hai?",
            buttons=[
                [Button.inline("▶️ Start Time Set Karo", f"schedst_{day}".encode()),
                 Button.inline("⏹ End Time Set Karo",   f"schedet_{day}".encode())],
                [Button.inline(
                    f"{'🔴 Is Din OFF Karo' if enabled else '🟢 Is Din ON Karo'}",
                    f"sched_toggle_{day}".encode()
                )],
                [Button.inline("← Dusra Din Edit Karo", b"sched_edit_days")],
                [Button.inline("🗓 Per-Day Menu",        b"sched_per_day_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"schedst_"))
async def sched_set_start_day(event):
    await event.answer()
    day  = event.data.decode().replace("schedst_", "")
    data = get_user_data(event.sender_id)
    data["step"] = f"wait_sched_day_start_{day}"
    import time as _t; data["step_since"] = _t.time()  # step_since fix
    try:
        await event.edit(
            f"⏰ **{day} — Start Time Set Karo**\n\n"
            "Format: `09:00 AM` ya `21:00`\n\nTime bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"sched_edit_days")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"schedet_"))
async def sched_set_end_day(event):
    await event.answer()
    day  = event.data.decode().replace("schedet_", "")
    data = get_user_data(event.sender_id)
    data["step"] = f"wait_sched_day_end_{day}"
    import time as _t; data["step_since"] = _t.time()  # step_since fix
    try:
        await event.edit(
            f"⏰ **{day} — End Time Set Karo**\n\n"
            "Format: `10:00 PM` ya `22:00`\n\nTime bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"sched_edit_days")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"sched_toggle_"))
async def sched_toggle_day(event):
    await event.answer()
    if event.data in (b"sched_toggle_perday", b"sched_toggle_queue"):
        return  # Alag handlers hain
    day   = event.data.decode().replace("sched_toggle_", "")
    data  = get_user_data(event.sender_id)
    sched = data.setdefault("scheduler", {})
    per_day = sched.setdefault("per_day", {})
    per_day.setdefault(day, {"enabled": True, "start": "09:00 AM", "end": "10:00 PM"})
    per_day[day]["enabled"] = not per_day[day].get("enabled", True)
    save_persistent_db()
    await event.answer(f"{day}: {'ON' if per_day[day]['enabled'] else 'OFF'}")
    await sched_edit_days(event)


@bot.on(events.CallbackQuery(data=b"sched_add_holiday"))
async def sched_add_holiday(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"] = "wait_holiday_add"
    data["step_since"] = time.time()
    await _save_step(data)
    try:
        await event.edit(
            "🗓️ **Holiday Dates Add Karo**\n\n"
            "Format: `YYYY-MM-DD`\n"
            "Multiple: `2026-08-15, 2026-01-26, 2026-12-25`\n\n"
            "Dates bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"sched_per_day_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass



# ══════════════════════════════════════════
# TIMEZONE — auto-detect on login, manual change via tz_menu
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"tz_menu"))
async def tz_menu(event):
    """
    Timezone info screen — auto-detect hoti hai login pe phone se.
    Manual change ke liye simple list deta hai (for edge cases like US/Russia multi-tz).
    """
    await event.answer()
    from utils import TIMEZONE_LIST
    from time_helper import ab_fmt, detect_tz_from_phone
    uid  = event.sender_id
    data = get_user_data(uid)
    curr_tz   = data.get("timezone", "Asia/Kolkata")
    phone     = data.get("phone", "")

    # Detected timezone from phone
    auto_tz   = detect_tz_from_phone(phone) if phone else None

    # Display name for current TZ
    curr_name = curr_tz
    for tz_key, tz_lbl in TIMEZONE_LIST:
        if tz_key == curr_tz:
            curr_name = tz_lbl
            break

    # Current time in user's TZ
    try:
        now_str = ab_fmt(uid, "%d %b %Y, %I:%M %p")
    except Exception:
        now_str = "—"

    auto_note = ""
    if auto_tz and auto_tz != curr_tz:
        auto_name = auto_tz
        for tz_key, tz_lbl in TIMEZONE_LIST:
            if tz_key == auto_tz:
                auto_name = tz_lbl
                break
        auto_note = f"\n💡 _Phone se detect: **{auto_name}** — neeche se reset kar sakte ho_"

    txt = (
        "🕐 **Timezone**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Abhi set hai:** {curr_name}\n"
        f"**Abhi ka waqt:** `{now_str}`\n"
        f"{auto_note}\n\n"
        "✅ Timezone **login ke time phone number se automatic** set hoti hai.\n"
        "Neeche se manually badal bhi sakte ho (agar bot ne galat detect kiya ho):"
    )

    # Build selection buttons
    btns = []
    for tz_key, tz_lbl in TIMEZONE_LIST:
        tick = "✅ " if tz_key == curr_tz else ""
        btns.append([Button.inline(f"{tick}{tz_lbl}", f"tz_set_{tz_key}".encode())])

    # Reset to phone-detected
    if auto_tz:
        btns.insert(0, [Button.inline("🔄 Phone se detect ki TZ use karo", b"tz_reset_auto")])

    btns.append([Button.inline("🔙 Back", b"settings_menu")])

    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"tz_reset_auto"))
async def tz_reset_auto(event):
    """Reset timezone to phone-detected value."""
    await event.answer()
    from time_helper import detect_tz_from_phone
    from utils import TIMEZONE_LIST
    uid  = event.sender_id
    data = get_user_data(uid)
    phone = data.get("phone", "")
    auto_tz = detect_tz_from_phone(phone) if phone else None
    if not auto_tz:
        await event.answer("❌ Phone number se timezone detect nahi ho saki.", alert=True)
        return
    data["timezone"] = auto_tz
    data.setdefault("scheduler", {})["timezone"] = auto_tz
    data["_tz_manual_override"] = False   # ✅ Allow auto-detect to work again
    save_persistent_db()
    lbl = auto_tz
    for k, l in TIMEZONE_LIST:
        if k == auto_tz:
            lbl = l
            break
    await event.answer(f"✅ Timezone reset: {lbl}", alert=True)
    await tz_menu(event)


@bot.on(events.CallbackQuery(pattern=b"tz_set_"))
async def tz_set(event):
    await event.answer()
    from utils import TIMEZONE_LIST
    uid    = event.sender_id
    data   = get_user_data(uid)
    tz_key = event.data.decode().replace("tz_set_", "", 1)

    valid_keys = [t[0] for t in TIMEZONE_LIST]
    if tz_key not in valid_keys:
        await event.answer("Invalid timezone!", alert=True)
        return

    data["timezone"] = tz_key
    data.setdefault("scheduler", {})["timezone"] = tz_key
    data["_tz_manual_override"] = True   # ✅ Preserve on next auto-detect
    save_persistent_db()

    lbl = tz_key
    for k, l in TIMEZONE_LIST:
        if k == tz_key:
            lbl = l
            break

    await event.answer(f"✅ Timezone set: {lbl}", alert=True)
    await tz_menu(event)


# ══════════════════════════════════════════════════════════════
# ⚙️ FORWARDING FILTERS v2 — New Settings UI
# Size filter, length filter, require media, delay variance,
# sticker/GIF/Poll, copy mode, keyword routing, source stats
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"fwd_filters_menu"))
async def fwd_filters_menu(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]

    def yn(k, default=False): return "✅" if s.get(k, default) else "❌"
    def val(k, default=0):    return s.get(k, default)

    min_len  = val("min_msg_length")
    max_len  = val("max_msg_length")
    max_mb   = val("max_file_size_mb")
    variance = val("delay_variance")
    cnt_lim  = val("fwd_count_limit")

    txt = (
        "⚙️ **FORWARDING FILTERS v2**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📏 Min msg length: `{min_len}` chars {'_(off)_' if not min_len else ''}\n"
        f"📏 Max msg length: `{max_len}` chars {'_(off)_' if not max_len else ''}\n"
        f"📦 Max file size:  `{max_mb}` MB {'_(no limit)_' if not max_mb else ''}\n"
        f"📎 Require media:  {yn('require_media')}\n"
        f"⏱ Delay variance: `±{variance}s` {'_(off)_' if not variance else ''}\n"
        f"🔢 Fwd count limit:`{cnt_lim}` {'_(unlimited)_' if not cnt_lim else ''}\n\n"
        f"🎯 Sticker fwd:    {yn('sticker', False)}\n"
        f"🎬 GIF fwd:        {yn('gif', True)}\n"
        f"📊 Poll fwd:       {yn('poll', True)}\n"
        f"📋 Copy mode:      {yn('copy_mode', False)} _(removes forward tag)_\n"
        f"🏥 Dest health:    {yn('dest_health_check', True)}"
    )

    btns = [
        [Button.inline("📏 Set Length Filter",   b"fwd_set_length"),
         Button.inline("📦 Set File Size",        b"fwd_set_filesize")],
        [Button.inline("⏱ Set Delay Variance",   b"fwd_set_variance"),
         Button.inline("🔢 Set Count Limit",      b"fwd_set_count_limit")],
        [Button.inline(f"{'🟢' if s.get('require_media') else '🔴'} Require Media",
                        b"fwd_toggle_require_media"),
         Button.inline(f"{'🟢' if s.get('copy_mode') else '🔴'} Copy Mode",
                        b"fwd_toggle_copy_mode")],
        [Button.inline(f"{'🟢' if s.get('sticker', False) else '🔴'} Stickers",
                        b"fwd_toggle_sticker"),
         Button.inline(f"{'🟢' if s.get('gif', True) else '🔴'} GIFs",
                        b"fwd_toggle_gif"),
         Button.inline(f"{'🟢' if s.get('poll', True) else '🔴'} Polls",
                        b"fwd_toggle_poll")],
        [Button.inline(f"{'🟢' if s.get('dest_health_check', True) else '🔴'} Dest Health",
                        b"fwd_toggle_dest_health")],
        [Button.inline("🗺️ Keyword Routing",      b"fwd_keyword_routes"),
         Button.inline("📊 Source Stats",          b"fwd_src_stats")],
        [Button.inline("🔄 Reset Fwd Counts",     b"fwd_reset_counts"),
         Button.inline("🏥 Re-enable All Dests",  b"fwd_reenable_dests")],
        [Button.inline("🔙 Main Menu",             b"main_menu")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


# ── Toggle handlers ───────────────────────────────────────────
for _toggle_key, _toggle_cb, _default in [
    ("require_media",    b"fwd_toggle_require_media", False),
    ("copy_mode",        b"fwd_toggle_copy_mode",     False),
    ("sticker",          b"fwd_toggle_sticker",       False),
    ("gif",              b"fwd_toggle_gif",            True),
    ("poll",             b"fwd_toggle_poll",           True),
    ("dest_health_check",b"fwd_toggle_dest_health",   True),
]:
    def _make_toggle(key, default):
        async def _handler(event, _k=key, _d=default):
            await event.answer()
            data = get_user_data(event.sender_id)
            data["settings"][_k] = not data["settings"].get(_k, _d)
            save_persistent_db()
            status = "✅ ON" if data["settings"][_k] else "❌ OFF"
            await event.answer(f"{_k} → {status}", alert=False)
            await fwd_filters_menu(event)
        return _handler

    bot.on(events.CallbackQuery(data=_toggle_cb))(_make_toggle(_toggle_key, _default))


# ── Set length filter ─────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_set_length"))
async def fwd_set_length(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    s    = data["settings"]
    data["step"]       = "fwd_length_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "📏 **MESSAGE LENGTH FILTER**\n\n"
            f"Current: min=`{s.get('min_msg_length',0)}` max=`{s.get('max_msg_length',0)}`\n\n"
            "Format: `MIN MAX`\n"
            "Example: `10 500` (10 se 500 chars)\n"
            "Example: `0 200` (max 200, no min)\n"
            "Example: `50 0` (min 50, no max)\n"
            "_(0 = disabled)_",
            buttons=[[Button.inline("❌ Cancel", b"fwd_filters_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_length_input"))
async def fwd_length_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        parts = event.raw_text.strip().split()
        if len(parts) != 2:
            await event.respond("❌ Format: `MIN MAX` (e.g. `10 500`)")
            return
        mn, mx = int(parts[0]), int(parts[1])
        data["settings"]["min_msg_length"] = max(0, mn)
        data["settings"]["max_msg_length"] = max(0, mx)
        save_persistent_db()
        await event.respond(
            f"✅ Length filter: min=`{mn}` max=`{mx}`",
            buttons=[[Button.inline("⚙️ Filters", b"fwd_filters_menu")]]
        )
    except ValueError:
        await event.respond("❌ Numbers bhejo। Example: `10 500`")


# ── Set file size filter ──────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_set_filesize"))
async def fwd_set_filesize(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    cur  = data["settings"].get("max_file_size_mb", 0)
    data["step"]       = "fwd_filesize_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            f"📦 **FILE SIZE FILTER**\n\nCurrent: `{cur}` MB ({'no limit' if not cur else f'max {cur}MB'})\n\n"
            "Max file size MB mein bhejo:\n_(0 = no limit, e.g. `10` for 10MB max)_",
            buttons=[[Button.inline("❌ Cancel", b"fwd_filters_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_filesize_input"))
async def fwd_filesize_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        mb = int(event.raw_text.strip())
        data["settings"]["max_file_size_mb"] = max(0, mb)
        save_persistent_db()
        msg = f"✅ File size limit: `{mb}MB`" if mb else "✅ File size limit: disabled"
        await event.respond(msg, buttons=[[Button.inline("⚙️ Filters", b"fwd_filters_menu")]])
    except ValueError:
        await event.respond("❌ Number bhejo (e.g. `10`)")


# ── Set delay variance ────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_set_variance"))
async def fwd_set_variance(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    cur  = data["settings"].get("delay_variance", 0)
    data["step"]       = "fwd_variance_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            f"⏱ **DELAY VARIANCE**\n\nCurrent: `±{cur}s`\n\n"
            "Har message ke delay mein random ±N seconds add honge.\n"
            "Example: delay=5s, variance=3 → actual delay 5-8s\n\n"
            "Seconds mein bhejo (0 = off):",
            buttons=[[Button.inline("❌ Cancel", b"fwd_filters_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_variance_input"))
async def fwd_variance_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        v = int(event.raw_text.strip())
        data["settings"]["delay_variance"] = max(0, v)
        save_persistent_db()
        await event.respond(
            f"✅ Delay variance: `±{v}s`" if v else "✅ Delay variance: disabled",
            buttons=[[Button.inline("⚙️ Filters", b"fwd_filters_menu")]]
        )
    except ValueError:
        await event.respond("❌ Number bhejo")


# ── Set forward count limit ───────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_set_count_limit"))
async def fwd_set_count_limit(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    cur  = data["settings"].get("fwd_count_limit", 0)
    data["step"]       = "fwd_count_limit_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            f"🔢 **FORWARD COUNT LIMIT**\n\nCurrent: `{cur}` ({'unlimited' if not cur else f'stop after {cur} msgs'})\n\n"
            "Kitne messages forward karne ke baad stop karna hai?\n"
            "_(0 = unlimited)_\n\nNumber bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"fwd_filters_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_count_limit_input"))
async def fwd_count_limit_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        n = int(event.raw_text.strip())
        data["settings"]["fwd_count_limit"] = max(0, n)
        save_persistent_db()
        await event.respond(
            f"✅ Forward count limit: `{n}`" if n else "✅ Count limit: unlimited",
            buttons=[[Button.inline("⚙️ Filters", b"fwd_filters_menu")]]
        )
    except ValueError:
        await event.respond("❌ Number bhejo")


# ── Keyword-based routing ─────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_keyword_routes"))
async def fwd_keyword_routes(event):
    await event.answer()
    uid    = event.sender_id
    data   = get_user_data(uid)
    routes = data.get("keyword_routes", [])

    if not routes:
        body = "  _(koi route set nahi)_"
    else:
        lines = []
        for i, r in enumerate(routes):
            kws  = ", ".join(r.get("keywords", []))[:40]
            dsts = str(r.get("dests", []))[:40]
            lines.append(f"  {i+1}. `{kws}` → `{dsts}`")
        body = "\n".join(lines)

    btns = []
    for i in range(len(routes)):
        btns.append([Button.inline(f"🗑 Delete Route #{i+1}", f"fwd_route_del|{i}".encode())])
    btns.append([Button.inline("➕ Add Route",    b"fwd_route_add")])
    btns.append([Button.inline("🔙 Back",         b"fwd_filters_menu")])

    try:
        await event.edit(
            "🗺️ **KEYWORD ROUTING**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Agar message mein keyword ho → specific destinations pe bhejo।\n\n"
            f"**Active Routes ({len(routes)}):**\n{body}",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"fwd_route_add"))
async def fwd_route_add(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"]       = "fwd_route_add_kw"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "🗺️ **ADD KEYWORD ROUTE — Step 1/2**\n\n"
            "Keywords type karo (comma se alag karo):\n"
            "Example: `sale, discount, offer`",
            buttons=[[Button.inline("❌ Cancel", b"fwd_keyword_routes")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_route_add_kw"))
async def fwd_route_kw_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]                    = "fwd_route_add_dests"
    data["temp_data"]["route_kws"]  = [k.strip() for k in event.raw_text.split(",") if k.strip()]
    await event.respond(
        f"✅ Keywords: `{data['temp_data']['route_kws']}`\n\n"
        "**Step 2/2** — Destination IDs type karo (comma se alag karo):\n"
        "Example: `-100123456789, -100987654321`",
        buttons=[[Button.inline("❌ Cancel", b"fwd_keyword_routes")]]
    )


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_route_add_dests"))
async def fwd_route_dests_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        dests = [int(d.strip()) for d in event.raw_text.split(",") if d.strip()]
    except ValueError:
        await event.respond("❌ Valid channel IDs bhejo।")
        return
    kws   = data["temp_data"].pop("route_kws", [])
    route = {"keywords": kws, "dests": dests}
    data.setdefault("keyword_routes", []).append(route)
    save_persistent_db()
    await event.respond(
        f"✅ Route added!\n`{kws}` → `{dests}`",
        buttons=[[Button.inline("🗺️ Routes", b"fwd_keyword_routes")]]
    )


@bot.on(events.CallbackQuery(pattern=b"fwd_route_del\\|(.+)"))
async def fwd_route_del(event):
    await event.answer()
    idx    = int(event.data.decode().split("|")[1])
    data   = get_user_data(event.sender_id)
    routes = data.get("keyword_routes", [])
    if 0 <= idx < len(routes):
        routes.pop(idx)
        save_persistent_db()
        await event.answer("🗑 Route deleted!", alert=False)
    await fwd_keyword_routes(event)


# ── Per-source forward stats ──────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_src_stats"))
async def fwd_src_stats(event):
    await event.answer()
    uid    = event.sender_id
    data   = get_user_data(uid)
    stats  = data.get("src_stats", {})
    srcs   = data.get("sources", [])
    counts = data.get("src_fwd_counts", {})

    if not stats and not counts:
        try:
            await event.edit(
                "📊 **SOURCE STATS**\n\nAbhi koi stats nahi hain।",
                buttons=[[Button.inline("🔙 Back", b"fwd_filters_menu")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    today  = datetime.date.today().isoformat()
    lines  = []
    for i, src in enumerate(srcs[:10]):
        key         = str(src)
        total       = stats.get(key, {}).get("total", counts.get(key, 0))
        today_count = stats.get(key, {}).get("today", {}).get(today, 0)
        lines.append(f"  Src#{i+1} `{str(src)[:15]}`: {total} total | {today_count} today")

    body = "\n".join(lines) if lines else "  _(no data)_"
    try:
        await event.edit(
            "📊 **FORWARDING STATS PER SOURCE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + body,
            buttons=[
                [Button.inline("🔄 Refresh",    b"fwd_src_stats"),
                 Button.inline("🗑 Reset Stats", b"fwd_reset_stats")],
                [Button.inline("🔙 Back",        b"fwd_filters_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        await event.answer("Already up to date", alert=False)


@bot.on(events.CallbackQuery(data=b"fwd_reset_stats"))
async def fwd_reset_stats(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["src_stats"]      = {}
    data["src_fwd_counts"] = {}
    save_persistent_db()
    await event.answer("✅ Stats reset!", alert=False)
    await fwd_src_stats(event)


# ── Reset forward counts ──────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_reset_counts"))
async def fwd_reset_counts(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["src_fwd_counts"] = {}
    save_persistent_db()
    await event.answer("✅ Forward counts reset!", alert=False)
    await fwd_filters_menu(event)


# ── Re-enable all auto-disabled destinations ──────────────────
@bot.on(events.CallbackQuery(data=b"fwd_reenable_dests"))
async def fwd_reenable_dests(event):
    await event.answer()
    uid    = event.sender_id
    data   = get_user_data(uid)
    rules  = data.get("custom_forward_rules", {})
    count  = 0
    for src_rules in rules.values():
        for dest_key, dest_rules in src_rules.items():
            if isinstance(dest_rules, dict) and not dest_rules.get("dest_enabled", True):
                dest_rules["dest_enabled"]    = True
                dest_rules["fail_count"]      = 0
                dest_rules["disabled_reason"] = ""
                count += 1
    if count:
        save_persistent_db()
    await event.answer(f"✅ {count} destination(s) re-enabled!", alert=True if count else False)
    await fwd_filters_menu(event)


# ── Forward stats per-source info also in source info display ─
# (Displays in fwd_src_stats — already implemented above)




# ══════════════════════════════════════════════
# v3 NEW FILTER MENUS — Auto-appended by patch
# ══════════════════════════════════════════════



    try:
        from notification_center import _footer as __f
        return __f()
    except Exception:
        return ""

    save_persistent_db()
    try:
        from database import save_to_mongo
        asyncio.create_task(save_to_mongo())
    except Exception:
        pass


# ══════════════════════════════════════════
# MAIN SETTINGS PANEL (Tabbed)
# ══════════════════════════════════════════

SETTINGS_TABS = {
    "media":    "📨 Kya Forward Karo",
    "mods":     "✂️ Message Badlo",
    "dup":      "♻️ Duplicate Rokko",
    "advanced": "🔧 Advanced",
    "new":      "🆕 Naye Filters",
}


def _state(s: dict, key: str, default=False) -> str:
    return "🟢" if s.get(key, default) else "🔴"


def _build_settings_text(s: dict, tab: str = "media") -> str:
    active = []
    if s.get("duplicate_filter"):         active.append("♻️Dup")
    if s.get("smart_filter"):             active.append("🧠Smart")
    if s.get("remove_links"):             active.append("🚫Links")
    if s.get("auto_shorten"):             active.append("✂️Short")
    if s.get("keyword_filter_enabled"):   active.append("🔑KW")
    if s.get("regex_filter_enabled"):     active.append("🔡Regex")
    if s.get("quality_filter_enabled"):   active.append("⭐Quality")
    if s.get("mention_filter", "off") != "off": active.append("👤Mention")
    if s.get("forward_origin_filter", "off") != "off": active.append("📨Origin")
    if s.get("lang_filter_enabled"):      active.append("🌐Lang")
    if s.get("time_filter_enabled"):      active.append("⏰Time")

    active_str  = "  ".join(active) if active else "_Koi active nahi_"
    tab_display = SETTINGS_TABS.get(tab, tab)

    # Tab-specific hint so user knows what they're looking at
    tab_hints = {
        "media":    "👉 Kaunse type ke messages forward hone chahiye",
        "mods":     "👉 Forward se pehle message mein kya change karna hai",
        "dup":      "👉 Same message dobara forward hone se rokna",
        "advanced": "👉 Size limits, health check, aur expert options",
        "new":      "👉 Language, time, regex aur quality filters",
    }
    hint = tab_hints.get(tab, "")

    header = (
        f"⚙️ **SETTINGS  ›  {tab_display}**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"_{hint}_\n\n"
        f"✅ Active: {active_str}\n"
        "🟢 = ON   🔴 = OFF   🔒 = Premium Only\n"
    )
    return header


def _tab_buttons(active_tab: str) -> list:
    # BUG FIX: 2 rows mein split + clear active highlight
    # Active tab: ✅ prefix + naam uppercase — clearly pata chale user kahan hai
    TAB_NAMES = list(SETTINGS_TABS.items())
    row1 = []
    row2 = []
    for i, (key, label) in enumerate(TAB_NAMES):
        if key == active_tab:
            # Active tab — clearly highlighted with checkmark and caps hint
            btn_label = f"✅ {label} ◀"
        else:
            btn_label = label
        btn = Button.inline(btn_label, f"stab_{key}".encode())
        if i < 3:
            row1.append(btn)
        else:
            row2.append(btn)
    rows = []
    if row1: rows.append(row1)
    if row2: rows.append(row2)
    return rows


@bot.on(events.CallbackQuery(pattern=b"settings_menu|stab_|toggle_"))
async def settings_handler(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    if not data.get("session"):
        return await event.answer("⚠️ Login karo pehle!", alert=True)

    await event.answer()
    s   = data["settings"]
    cmd = event.data.decode()

    # toggle_ handled by generic_toggle below — yahan sirf display refresh karna hai
    # Determine active tab
    if cmd.startswith("stab_"):
        tab = cmd.replace("stab_", "")
        data["_settings_tab"] = tab
    else:
        tab = data.get("_settings_tab", "media")

    txt     = _build_settings_text(s, tab)
    tab_row = _tab_buttons(tab)

    try:
        from premium import can_use_feature, is_feature_paid
        def _prem(label, key, feat_key=None):
            if feat_key and is_feature_paid(feat_key) and not can_use_feature(uid, feat_key):
                return Button.inline(f"🔒 {label}", f"toggle_{key}".encode())
            st = "🟢" if s.get(key) else "🔴"
            return Button.inline(f"{st} {label}", f"toggle_{key}".encode())
    except Exception:
        def _prem(label, key, feat_key=None):
            st = "🟢" if s.get(key) else "🔴"
            return Button.inline(f"{st} {label}", f"toggle_{key}".encode())

    def _btn(label, key, default=False):
        st = "🟢" if s.get(key, default) else "🔴"
        return Button.inline(f"{st} {label}", f"toggle_{key}".encode())

    # ── MEDIA TAB ────────────────────────────────────────────
    if tab == "media":
        buttons = tab_row + [
            [Button.inline("── Kaunse messages forward hone chahiye? ──", b"stab_media")],
            [_btn("📝 Text",    "text",    True),   _btn("🖼 Image",   "image",   True),
             _btn("🎬 Video",   "video",   True),   _btn("💬 Caption", "caption", True)],
            [_btn("🎙 Voice",   "voice",   False),  _btn("📁 Files",   "files",   False)],
            [_btn("🎭 Sticker", "sticker", False),  _btn("🎞 GIF",     "gif",     True),
             _btn("📊 Poll",    "poll",    True)],
            [Button.inline("── Display Options ──", b"stab_media")],
            [_btn("🔗 Link Preview Dikhao",          "preview_mode"),
             _btn("👁 Copy Mode (No Fwd Tag)",        "copy_mode")],
            [_btn("📎 Sirf Media wale forward karo",  "require_media")],
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ]

    # ── MODIFICATIONS TAB ────────────────────────────────────
    elif tab == "mods":
        delay = s.get("custom_delay", 0)
        var   = s.get("delay_variance", 0)
        delay_label = f"⏱ Delay: {'Auto' if delay == 0 else str(delay) + 's'}"
        buttons = tab_row + [
            [Button.inline("── Forward se pehle message mein kya hatao? ──", b"stab_mods")],
            [_btn("🚫 Links Hatao",          "remove_links"),
             _btn("👤 Usernames Hatao",      "remove_user")],
            [_prem("🧠 Smart Filter (AI)",   "smart_filter",  "smart_filter"),
             _prem("✂️ Links Shorten Karo",  "auto_shorten",  "auto_shorten")],

            [Button.inline("── Forwarding Speed (Delay) ──", b"stab_mods")],
            [Button.inline(delay_label,              b"set_delay_flow"),
             Button.inline(f"+/-{var}s random vary", b"fwd_set_variance")],

            [Button.inline("── Start / End Message ──", b"stab_mods")],
            [Button.inline("✏️ Start Message Set Karo",  b"set_start"),
             Button.inline("✏️ End Message Set Karo",    b"set_end")],

            [Button.inline("🏠 Main Menu", b"main_menu")],
        ]

    # ── DEDUP TAB ────────────────────────────────────────────
    elif tab == "dup":
        expiry = s.get("dup_expiry_hours", 2)
        buttons = tab_row + [
            [Button.inline("── Same message dobara forward mat karo ──", b"stab_dup")],
            [_prem("♻️ Dup Filter (Premium)", "duplicate_filter", "duplicate_filter"),
             _btn("🌐 Sab sources check karo",  "global_filter")],
            [_btn("🛒 Product Dup Filter",       "product_duplicate_filter"),
             _btn("🧠 Smart Content Dup",        "smart_dup")],

            [Button.inline("── Settings ──", b"stab_dup")],
            [Button.inline(f"⏱ {expiry}h baad reset ho",   b"dup_set_expiry_flow"),
             Button.inline("🗒 Allowed Words List",         b"dup_list_white_0")],
            [Button.inline("📊 Dup Stats Dekho",            b"dup_explain"),
             Button.inline("🗑 History Clear Karo",         b"dup_clear_history")],
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ]

    # ── ADVANCED TAB ────────────────────────────────────────
    elif tab == "advanced":
        buttons = tab_row + [
            [Button.inline("── Message Size & Count Limits ──", b"stab_advanced")],
            [Button.inline("📏 Message Length Limit",   b"fwd_set_length"),
             Button.inline("📦 File Size Limit",        b"fwd_set_filesize")],
            [Button.inline("🔢 Max Forward Count",      b"fwd_set_count_limit"),
             _btn("🏥 Dest Health Check", "dest_health_check", True)],

            [Button.inline("── Routing & Stats ──", b"stab_advanced")],
            [Button.inline("🗺️ Keyword Routes",          b"fwd_keyword_routes"),
             Button.inline("📊 Source Stats",            b"fwd_src_stats")],
            [Button.inline("🏥 Circuit Breaker",         b"cb_status"),
             Button.inline("⚡ Rate Limiter",            b"rl_stats")],

            [Button.inline("── Maintenance ──", b"stab_advanced")],
            [Button.inline("🔄 Sab Dests Re-enable",    b"fwd_reenable_dests"),
             Button.inline("🗑 Forward Counts Reset",   b"fwd_reset_counts")],
            [Button.inline("🔄 Sab Settings Reset",     b"settings_reset_confirm"),
             Button.inline("❓ Help",                   b"help_settings")],
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ]

    # ── NEW FILTERS TAB (v3) ─────────────────────────────────
    elif tab == "new":
        mention_mode    = s.get("mention_filter", "off")
        origin_mode     = s.get("forward_origin_filter", "off")
        lang_enabled    = s.get("lang_filter_enabled", False)
        time_enabled    = s.get("time_filter_enabled", False)
        regex_enabled   = data.get("regex_filters", {}).get("enabled", False)
        quality_enabled = data.get("quality_filter", {}).get("enabled", False)
        min_lnk = s.get("min_links", 0)
        max_lnk = s.get("max_links", 0)

        mention_label = {
            "off":               "👤 @Mentions: Filter Off",
            "block_mentions":    "👤 @Mentions: Block karo",
            "require_mentions":  "👤 @Mentions: Sirf yahi forward karo",
        }.get(mention_mode, "👤 @Mentions")
        origin_label = {
            "off":               "📨 Forward Origin: Off",
            "block_forwarded":   "📨 Forwarded msgs: Block karo",
            "only_forwarded":    "📨 Forwarded msgs: Sirf yahi",
        }.get(origin_mode, "📨 Origin Filter")

        buttons = tab_row + [
            [Button.inline("── Content-based Filters ──", b"stab_new")],
            [Button.inline(f"{'🟢' if regex_enabled else '🔴'} 🔡 Regex (Pattern match)",  b"regex_filter_menu"),
             Button.inline(f"{'🟢' if quality_enabled else '🔴'} ⭐ Quality Score",         b"quality_filter_menu")],
            [Button.inline(f"{'🟢' if lang_enabled else '🔴'} 🌐 Language Filter",          b"lang_filter_menu"),
             Button.inline(f"{'🟢' if time_enabled else '🔴'} ⏰ Time-of-day Filter",       b"time_filter_menu")],
            [Button.inline(mention_label,  b"mention_filter_menu"),
             Button.inline(origin_label,   b"forward_origin_menu")],
            [Button.inline(f"🔗 Link Count: {min_lnk}-{max_lnk if max_lnk else 'infinity'}", b"link_count_menu"),
             Button.inline("# Hashtag Filter",  b"hashtag_filter_menu")],
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ]

    else:
        buttons = tab_row + [[Button.inline("🏠 Main Menu", b"main_menu")]]

    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════
# TOGGLE HANDLER (Generic)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(pattern=b"toggle_"))
async def generic_toggle(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    key  = event.data.decode().replace("toggle_", "")

    if key in data["settings"]:
        from premium import can_use_feature, is_feature_paid
        PREMIUM_MAP = {
            "smart_filter": "smart_filter",
            "auto_shorten": "auto_shorten",
            "duplicate_filter": "duplicate_filter",
        }
        prem_key = PREMIUM_MAP.get(key)
        if prem_key and is_feature_paid(prem_key) and not can_use_feature(uid, prem_key):
            return await event.answer("🔒 Premium feature! /premium dekhein.", alert=True)

        data["settings"][key] = not data["settings"][key]
        _save()
        status = "ON ✅" if data["settings"][key] else "OFF ❌"
        await event.answer(f"{key}: {status}", alert=False)

    await settings_handler(event)


# ══════════════════════════════════════════
# SMART DELAY CALCULATOR (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"smart_delay_calc"))
async def smart_delay_calc(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    srcs = len(data.get("sources", []))
    dsts = len(data.get("destinations", []))
    cur_delay = data["settings"].get("custom_delay", 0)

    # Calculate recommended delay based on volume + safety tiers
    if srcs == 0 or dsts == 0:
        rec = 0
        reason = "Pehle source aur destination add karo"
        risk_level = "⬜ N/A"
    elif srcs <= 2 and dsts <= 2:
        rec = 2
        reason = "Light setup — 2s safe hai"
        risk_level = "🟢 Low Risk"
    elif srcs <= 5 and dsts <= 5:
        rec = 4
        reason = "Medium setup — 4s recommended"
        risk_level = "🟡 Medium"
    elif srcs <= 10 or dsts <= 10:
        rec = 6
        reason = "High volume — 6s strongly recommended"
        risk_level = "🟠 High Volume"
    else:
        rec = 10
        reason = "Very heavy setup — 10s for account safety"
        risk_level = "🔴 Heavy Load"

    try:
        from premium import is_premium_user
        _is_prem = is_premium_user(uid)
    except Exception:
        _is_prem = False

    risk_emoji = "🔴" if cur_delay == 0 else ("🟡" if cur_delay < rec else "🟢")

    txt = (
        "⚡ **SMART DELAY CALCULATOR**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Tumhara Setup:**\n"
        f"  📥 Sources: `{srcs}`   📤 Destinations: `{dsts}`\n"
        f"  ⏱ Current Delay: `{cur_delay}s` {risk_emoji}\n"
        f"  Load Level: {risk_level}\n\n"
        f"**💡 Recommended: `{rec}s`**\n"
        f"  _{reason}_\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "🛡️ **Account Safety Guide:**\n\n"
        "  🟢 `1-3s` — Light use, safe\n"
        "  🟡 `3-6s` — Medium volume, recommended\n"
        "  🟠 `6-10s` — High volume, essential\n"
        "  🔴 `0s` — ⚠️ Risk of FloodWait + account restrict\n\n"
        "💡 **Smart Delay system active hai** — automatically\n"
        "   busy channels ko slow karta hai protect karne ke liye.\n"
        + ("   _Premium user: Extra optimization active ✅_\n" if _is_prem else "")
    )

    presets = [
        Button.inline(f"✅ Apply {rec}s (Recommended)", f"delay_preset_{rec}".encode()),
    ]
    btns = [
        presets,
        [Button.inline("2s", b"delay_preset_2"),
         Button.inline("4s", b"delay_preset_4"),
         Button.inline("6s", b"delay_preset_6"),
         Button.inline("10s", b"delay_preset_10")],
        [Button.inline("✏️ Custom Delay", b"set_delay_flow"),
         Button.inline("🔙 Back", b"stab_mods")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════
# CIRCUIT BREAKER STATUS (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"cb_status"))
async def cb_status_panel(event):
    await event.answer()
    uid = event.sender_id

    try:
        from circuit_breaker import CircuitBreakerRegistry, CBState
        cbs   = CircuitBreakerRegistry.get_all_for_user(uid)
        stats = CircuitBreakerRegistry.get_stats()

        if not cbs:
            body = "_Koi circuit breaker data nahi hai._\n_Sab destinations healthy hain._"
        else:
            lines = []
            for dest_key, cb in cbs.items():
                cb._maybe_attempt_reset()
                emoji  = cb.get_status_emoji()
                state  = cb.state.value.upper()
                fails  = cb.total_fails
                ok     = cb.total_successes
                lines.append(f"{emoji} `{str(dest_key)[:20]}` — {state}  ✓{ok} ✗{fails}")
                if cb.last_error:
                    lines.append(f"   ↳ _{cb.last_error[:50]}_")
            body = "\n".join(lines)
    except ImportError:
        body = "_Circuit Breaker module not loaded._"
        cbs  = {}

    txt = (
        "🏥 **CIRCUIT BREAKER STATUS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Agar koi destination repeatedly fail ho\n"
        "to circuit breaker use karke skip karta hai.\n\n"
        "🟢 CLOSED = Normal  🟡 HALF_OPEN = Testing  🔴 OPEN = Paused\n\n"
        f"**Your Destinations:**\n{body}\n"
    )

    btns = [
        [Button.inline("🔄 Reset All CBs",  b"cb_reset_all"),
         Button.inline("🔄 Refresh",        b"cb_status")],
        [Button.inline("🔙 Back",            b"stab_advanced")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"cb_reset_all"))
async def cb_reset_all(event):
    await event.answer()
    uid = event.sender_id
    try:
        from circuit_breaker import CircuitBreakerRegistry, CBState
        cbs = CircuitBreakerRegistry.get_all_for_user(uid)
        for dest_key in list(cbs.keys()):
            CircuitBreakerRegistry.reset(uid, dest_key)
        await event.answer(f"✅ {len(cbs)} circuit breakers reset!", alert=True)
    except Exception as e:
        await event.answer(f"Error: {e}", alert=True)
    await cb_status_panel(event)


# ══════════════════════════════════════════
# RATE LIMITER STATS (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"rl_stats"))
async def rl_stats_panel(event):
    await event.answer()
    uid = event.sender_id

    try:
        from rate_limiter import RateLimiterRegistry
        limiter = RateLimiterRegistry.get(uid)
        stats   = limiter.get_stats()
        global_s = stats["global"]
        dests_s  = stats.get("dests", {})

        lines = [
            f"  🌐 Global Rate: `{global_s['current_rate']} tok/s`",
            f"  📤 Total Sent: `{global_s['total_acquired']}`",
            f"  ⏱ Total Waits: `{global_s['total_waits']}`",
            f"  🌊 FloodWaits: `{global_s['flood_waits']}`",
        ]
        if dests_s:
            lines.append("\n**Per Destination:**")
            for dest, ds in list(dests_s.items())[:5]:
                lines.append(
                    f"  `{str(dest)[:15]}`: {ds['current_rate']}tok/s "
                    f"| sent:{ds['total_acquired']} | floods:{ds['flood_waits']}"
                )
        body = "\n".join(lines)
    except ImportError:
        body = "_Rate Limiter module not loaded._"

    txt = (
        "⚡ **RATE LIMITER STATS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Adaptive token bucket algorithm — automatically adjusts\n"
        "send rate based on Telegram's FloodWait responses.\n\n"
        f"{body}\n"
    )

    try:
        await event.edit(txt, buttons=[[Button.inline("🔙 Back", b"stab_advanced")]])
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════
# MENTION FILTER MENU (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"mention_filter_menu"))
async def mention_filter_menu(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    mode = data["settings"].get("mention_filter", "off")

    modes = {
        "off":              ("🔕 OFF — No mention filter", b"mention_set_off"),
        "block_mentions":   ("🚫 Block messages with @mentions", b"mention_set_block"),
        "require_mentions": ("✅ Only forward messages WITH @mentions", b"mention_set_require"),
    }

    lines = [f"{'▶ ' if k == mode else '   '}{label}" for k, (label, _) in modes.items()]
    txt = (
        "👤 **MENTION FILTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Filter messages based on @username mentions.\n\n"
        f"**Current:** `{mode}`\n\n"
        + "\n".join(lines)
    )
    btns = [
        [Button.inline(label, cb) for _, (label, cb) in modes.items()],
        [Button.inline("🔙 Back", b"stab_new")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


for _mode_key, _cb_data in [
    ("off",              b"mention_set_off"),
    ("block_mentions",   b"mention_set_block"),
    ("require_mentions", b"mention_set_require"),
]:
    def _make_mention_handler(mk):
        @bot.on(events.CallbackQuery(data=_cb_data if _cb_data else b"_unused"))
        async def _h(event, _mk=mk):
            uid = event.sender_id
            get_user_data(uid)["settings"]["mention_filter"] = _mk
            _save()
            await event.answer(f"Mention filter: {_mk}")
            await mention_filter_menu(event)
        return _h

# Manual registration for clarity
@bot.on(events.CallbackQuery(data=b"mention_set_off"))
async def mention_set_off(event):
    await event.answer()
    get_user_data(event.sender_id)["settings"]["mention_filter"] = "off"
    _save(); await event.answer("Mention filter: OFF")
    await mention_filter_menu(event)

@bot.on(events.CallbackQuery(data=b"mention_set_block"))
async def mention_set_block(event):
    await event.answer()
    get_user_data(event.sender_id)["settings"]["mention_filter"] = "block_mentions"
    _save(); await event.answer("Block @mentions: ON")
    await mention_filter_menu(event)

@bot.on(events.CallbackQuery(data=b"mention_set_require"))
async def mention_set_require(event):
    await event.answer()
    get_user_data(event.sender_id)["settings"]["mention_filter"] = "require_mentions"
    _save(); await event.answer("Require @mention: ON")
    await mention_filter_menu(event)


# ══════════════════════════════════════════
# FORWARD ORIGIN FILTER (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"forward_origin_menu"))
async def forward_origin_menu(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    mode = data["settings"].get("forward_origin_filter", "off")

    txt = (
        "📨 **FORWARD ORIGIN FILTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Kya message already forward hua tha?\n\n"
        f"**Current:** `{mode}`\n\n"
        "🔕 **OFF** — Koi check nahi\n"
        "🚫 **Block Forwarded** — Sirf original messages forward karo\n"
        "✅ **Only Forwarded** — Sirf already-forwarded msgs forward karo\n"
    )
    btns = [
        [Button.inline("🔕 OFF",              b"origin_set_off")],
        [Button.inline("🚫 Block Forwarded",  b"origin_set_block")],
        [Button.inline("✅ Only Forwarded",    b"origin_set_only")],
        [Button.inline("🔙 Back",             b"stab_new")],
    ]
    # Mark active
    active_labels = {
        "off": "🔕 OFF ✓",
        "block_forwarded": "🚫 Block Forwarded ✓",
        "only_forwarded": "✅ Only Forwarded ✓",
    }
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"origin_set_off"))
async def origin_set_off(event):
    await event.answer()
    get_user_data(event.sender_id)["settings"]["forward_origin_filter"] = "off"
    _save(); await event.answer("Origin filter: OFF")
    await forward_origin_menu(event)

@bot.on(events.CallbackQuery(data=b"origin_set_block"))
async def origin_set_block(event):
    await event.answer()
    get_user_data(event.sender_id)["settings"]["forward_origin_filter"] = "block_forwarded"
    _save(); await event.answer("Blocking forwarded messages")
    await forward_origin_menu(event)

@bot.on(events.CallbackQuery(data=b"origin_set_only"))
async def origin_set_only(event):
    await event.answer()
    get_user_data(event.sender_id)["settings"]["forward_origin_filter"] = "only_forwarded"
    _save(); await event.answer("Only forwarded messages allowed")
    await forward_origin_menu(event)


# ══════════════════════════════════════════
# LANGUAGE FILTER MENU (NEW)
# ══════════════════════════════════════════

LANG_OPTIONS = {
    "en": "🇬🇧 English",
    "hi": "🇮🇳 Hindi",
    "ar": "🇸🇦 Arabic",
    "ru": "🇷🇺 Russian",
    "zh": "🇨🇳 Chinese",
    "ja": "🇯🇵 Japanese",
    "ko": "🇰🇷 Korean",
}


@bot.on(events.CallbackQuery(data=b"lang_filter_menu"))
async def lang_filter_menu(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    cfg  = data.get("lang_filter", {"enabled": False, "allowed": []})
    enabled = cfg.get("enabled", False)
    allowed = cfg.get("allowed", [])

    allowed_labels = [LANG_OPTIONS.get(l, l) for l in allowed]
    txt = (
        "🌐 **LANGUAGE FILTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Sirf selected language ke messages forward karo.\n"
        "_Detection: Character set analysis (offline, no API)_\n\n"
        f"**Status:** {'🟢 ON' if enabled else '🔴 OFF'}\n"
        f"**Allowed:** {', '.join(allowed_labels) if allowed_labels else 'None selected'}\n\n"
        "_Note: Emoji-only or very short messages detected as English_"
    )

    lang_btns = []
    row = []
    for code, label in LANG_OPTIONS.items():
        marker = "✅" if code in allowed else "⬜"
        row.append(Button.inline(f"{marker} {label}", f"langf_toggle_{code}".encode()))
        if len(row) == 2:
            lang_btns.append(row)
            row = []
    if row:
        lang_btns.append(row)

    btns = [
        [Button.inline(f"{'🟢 ON' if enabled else '🔴 OFF'} Toggle", b"langf_toggle_enabled")],
        *lang_btns,
        [Button.inline("🗑 Clear All", b"langf_clear"),
         Button.inline("🔙 Back",     b"stab_new")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"langf_toggle_enabled"))
async def langf_toggle_enabled(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    cfg  = data.setdefault("lang_filter", {"enabled": False, "allowed": []})
    cfg["enabled"] = not cfg.get("enabled", False)
    _save(); await event.answer(f"Language filter: {'ON' if cfg['enabled'] else 'OFF'}")
    await lang_filter_menu(event)


@bot.on(events.CallbackQuery(pattern=b"langf_toggle_[a-z]{2}"))
async def langf_toggle_lang(event):
    await event.answer()
    uid  = event.sender_id
    code = event.data.decode().replace("langf_toggle_", "")
    data = get_user_data(uid)
    cfg  = data.setdefault("lang_filter", {"enabled": False, "allowed": []})
    allowed = cfg.setdefault("allowed", [])
    if code in allowed:
        allowed.remove(code)
        await event.answer(f"Removed: {LANG_OPTIONS.get(code, code)}")
    else:
        allowed.append(code)
        await event.answer(f"Added: {LANG_OPTIONS.get(code, code)}")
    _save()
    await lang_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"langf_clear"))
async def langf_clear(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data.setdefault("lang_filter", {})["allowed"] = []
    _save(); await event.answer("Language filter cleared")
    await lang_filter_menu(event)


# ══════════════════════════════════════════
# TIME-BASED FILTER MENU (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"time_filter_menu"))
async def time_filter_menu(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    cfg  = data.get("time_filter", {"enabled": False, "rules": [], "timezone": "Asia/Kolkata"})
    enabled = cfg.get("enabled", False)
    rules   = cfg.get("rules", [])
    tz      = cfg.get("timezone", "Asia/Kolkata")

    rules_text = ""
    for i, r in enumerate(rules):
        rtype = r.get("type", "all")
        start = r.get("start", "00:00")
        end   = r.get("end", "23:59")
        rules_text += f"\n  {i+1}. [{rtype}] `{start}` – `{end}`"

    txt = (
        "⏰ **TIME-BASED FILTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Sirf specific hours mein messages forward karo.\n"
        "Alag rules alag content types ke liye.\n\n"
        f"**Status:** {'🟢 ON' if enabled else '🔴 OFF'}\n"
        f"**Timezone:** `{tz}`\n"
        f"**Rules ({len(rules)}):**{rules_text if rules_text else chr(10) + '  _(none)_'}\n"
    )

    btns = [
        [Button.inline(f"{'🟢 ON' if enabled else '🔴 OFF'} Toggle", b"timef_toggle")],
        [Button.inline("➕ Add Rule",        b"timef_add"),
         Button.inline("🌍 Set Timezone",   b"timef_set_tz")],
        [Button.inline("🗑 Clear Rules",    b"timef_clear"),
         Button.inline("🔙 Back",           b"stab_new")],
    ]
    if rules:
        for i in range(len(rules)):
            btns.insert(-1, [Button.inline(f"🗑 Delete Rule #{i+1}", f"timef_del_{i}".encode())])
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"timef_toggle"))
async def timef_toggle(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    cfg  = data.setdefault("time_filter", {"enabled": False, "rules": []})
    cfg["enabled"] = not cfg.get("enabled", False)
    _save(); await event.answer(f"Time filter: {'ON' if cfg['enabled'] else 'OFF'}")
    await time_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"timef_add"))
async def timef_add(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "timef_add_input"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "⏰ **ADD TIME RULE**\n\n"
            "Format bhejo: `TYPE START END`\n\n"
            "**TYPE options:**\n"
            "  `all`   — sab messages\n"
            "  `text`  — sirf text\n"
            "  `media` — image/video\n"
            "  `file`  — documents\n\n"
            "**Example:** `all 09:00 22:00`\n"
            "_(24-hour format)_",
            buttons=[[Button.inline("❌ Cancel", b"time_filter_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "timef_add_input"))
async def timef_add_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    try:
        parts = event.raw_text.strip().split()
        if len(parts) != 3:
            raise ValueError("Need 3 parts")
        rtype, start, end = parts[0].lower(), parts[1], parts[2]
        if rtype not in ("all", "text", "media", "file"):
            raise ValueError("Invalid type")
        # Validate time format
        for t in [start, end]:
            h, m = map(int, t.split(":"))
            assert 0 <= h <= 23 and 0 <= m <= 59

        cfg = data.setdefault("time_filter", {"enabled": True, "rules": []})
        cfg["rules"].append({"type": rtype, "start": start, "end": end})
        _save()
        await event.respond(
            f"✅ Time rule added: [{rtype}] {start} – {end}",
            buttons=[[Button.inline("⏰ Time Filter", b"time_filter_menu")]]
        )
    except Exception:
        await event.respond(
            "❌ Format galat hai.\nExample: `all 09:00 22:00`",
            buttons=[[Button.inline("⏰ Time Filter", b"time_filter_menu")]]
        )


@bot.on(events.CallbackQuery(pattern=b"timef_del_\\d+"))
async def timef_del(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    uid = event.sender_id
    data = get_user_data(uid)
    rules = data.setdefault("time_filter", {}).setdefault("rules", [])
    if 0 <= idx < len(rules):
        rules.pop(idx)
        _save()
        await event.answer("Rule deleted!")
    await time_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"timef_clear"))
async def timef_clear(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data.setdefault("time_filter", {})["rules"] = []
    _save(); await event.answer("Time rules cleared")
    await time_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"timef_set_tz"))
async def timef_set_tz(event):
    """Set timezone for time-based filter — was missing handler (FIXED)."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "timef_tz_input"
    data["step_since"] = time.time()

    cur_tz = data.get("time_filter", {}).get("timezone", "Asia/Kolkata")

    common_tzs = [
        ("🇮🇳 India (IST)",        "Asia/Kolkata"),
        ("🇵🇰 Pakistan (PKT)",      "Asia/Karachi"),
        ("🇦🇪 Dubai (GST)",         "Asia/Dubai"),
        ("🇸🇦 Riyadh (AST)",        "Asia/Riyadh"),
        ("🇺🇸 New York (EST)",       "America/New_York"),
        ("🇬🇧 London (GMT)",         "Europe/London"),
        ("🇩🇪 Berlin (CET)",         "Europe/Berlin"),
        ("🇸🇬 Singapore (SGT)",      "Asia/Singapore"),
    ]

    btns = [[Button.inline(label, f"timef_tz_set|{tz}".encode())]
            for label, tz in common_tzs]
    btns.append([Button.inline("⌨️ Manual Enter", b"timef_tz_manual")])
    btns.append([Button.inline("🔙 Back", b"timef_menu")])

    try:
        await event.edit(
            f"🌍 **Timezone Set Karo**\n\n"
            f"Abhi: `{cur_tz}`\n\n"
            f"Apna timezone chuniye:",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"timef_tz_set|"))
async def timef_tz_set(event):
    """Apply selected timezone."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    raw  = event.data.decode()
    if "|" not in raw:
        return
    tz = raw.split("|", 1)[1]
    data.setdefault("time_filter", {})["timezone"] = tz
    data.get("scheduler", {}) and data["scheduler"].update({"timezone": tz})
    _save()
    await event.answer(f"✅ Timezone set: {tz}", alert=False)
    await time_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"timef_tz_manual"))
async def timef_tz_manual(event):
    """Manual timezone entry."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "timef_tz_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "⌨️ **Timezone manually daalo**\n\n"
            "Example: `Asia/Kolkata`, `America/New_York`, `Europe/London`\n\n"
            "Full list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones",
            buttons=[[Button.inline("❌ Cancel", b"timef_set_tz")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"regex_filter_menu"))
async def regex_filter_menu_cb(event):
    """Regex filter settings — was missing handler (FIXED)."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    cfg  = data.get("regex_filters", {"enabled": False, "patterns": []})
    enabled  = cfg.get("enabled", False)
    patterns = cfg.get("patterns", [])

    pat_text = "\n".join(f"  `{p}`" for p in patterns[:5]) if patterns else "  _(none)_"
    if len(patterns) > 5:
        pat_text += f"\n  _...aur {len(patterns)-5} patterns_"

    try:
        await event.edit(
            "🔡 **Regex Filter**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Regex patterns se messages filter karo.\n"
            "Match hone wale messages **forward honge** (whitelist mode).\n\n"
            f"**Status:** {'🟢 ON' if enabled else '🔴 OFF'}\n"
            f"**Patterns ({len(patterns)}):**\n{pat_text}\n\n"
            "💡 _Example: `\\d{{4,}}` sirf numbers wale messages_",
            buttons=[
                [Button.inline(
                    f"{'🟢 Disable' if enabled else '🔴 Enable'} Regex Filter",
                    b"regex_toggle"
                )],
                [Button.inline("➕ Add Pattern",   b"regex_add"),
                 Button.inline("🗑 Clear All",     b"regex_clear")],
                [Button.inline("🔙 Back",          b"stab_new")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"regex_toggle"))
async def regex_toggle(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    cfg  = data.setdefault("regex_filters", {"enabled": False, "patterns": []})
    cfg["enabled"] = not cfg.get("enabled", False)
    _save()
    await event.answer("✅ Regex filter " + ("enabled" if cfg["enabled"] else "disabled"))
    await regex_filter_menu_cb(event)


@bot.on(events.CallbackQuery(data=b"regex_add"))
async def regex_add(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "regex_add_input"
    data["step_since"] = time.time()
    await event.answer()
    try:
        await event.edit(
            "🔡 **Regex Pattern Add Karo**\n\n"
            "Apna regex pattern bhejo.\n\n"
            "Examples:\n"
            "  `\\d{4,}` — 4+ digit numbers\n"
            "  `#\\w+` — hashtags\n"
            "  `https?://\\S+` — URLs",
            buttons=[[Button.inline("❌ Cancel", b"regex_filter_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"regex_clear"))
async def regex_clear(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data.setdefault("regex_filters", {})["patterns"] = []
    _save()
    await event.answer("✅ All regex patterns cleared")
    await regex_filter_menu_cb(event)


@bot.on(events.CallbackQuery(data=b"quality_filter_menu"))
async def quality_filter_menu_cb(event):
    """Quality filter settings — was missing handler (FIXED)."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]
    enabled   = s.get("quality_filter_enabled", False)
    min_score = s.get("quality_min_score", 30)

    # Score legend
    score_desc = {
        10: "Very low — almost everything pass",
        30: "Low — filters obvious junk (recommended)",
        50: "Medium — balanced quality",
        70: "High — only good content",
        90: "Very high — strict",
    }
    desc = score_desc.get(min_score, f"Custom ({min_score})")

    try:
        await event.edit(
            "⭐ **Quality Filter**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Messages ko quality score ke basis par filter karo.\n"
            "Low quality spam ya junk messages block honge.\n\n"
            f"**Status:** {'🟢 ON' if enabled else '🔴 OFF'}\n"
            f"**Min Score:** `{min_score}/100` — _{desc}_\n\n"
            "Score kaise banta hai:\n"
            "• Message length ✅\n"
            "• Media presence ✅\n"
            "• Spam keywords ❌\n"
            "• All-caps text ❌",
            buttons=[
                [Button.inline(
                    f"{'🟢 Disable' if enabled else '🔴 Enable'} Quality Filter",
                    b"quality_toggle"
                )],
                [Button.inline("📊 Score: 10 (Low)",  b"quality_score|10"),
                 Button.inline("📊 Score: 30 ⭐",     b"quality_score|30")],
                [Button.inline("📊 Score: 50",        b"quality_score|50"),
                 Button.inline("📊 Score: 70 (High)", b"quality_score|70")],
                [Button.inline("🔙 Back",             b"stab_new")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"quality_toggle"))
async def quality_toggle(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["settings"]["quality_filter_enabled"] = not data["settings"].get("quality_filter_enabled", False)
    _save()
    await event.answer("✅ Quality filter " + ("enabled" if data["settings"]["quality_filter_enabled"] else "disabled"))
    await quality_filter_menu_cb(event)


@bot.on(events.CallbackQuery(pattern=b"quality_score|"))
async def quality_score_set(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    raw  = event.data.decode()
    if "|" not in raw:
        return
    try:
        score = int(raw.split("|", 1)[1])
        data["settings"]["quality_min_score"] = score
        _save()
        await event.answer(f"✅ Min quality score set to {score}")
        await quality_filter_menu_cb(event)
    except (ValueError, IndexError):
        await event.answer("❌ Invalid score", alert=True)


# ══════════════════════════════════════════
# LINK COUNT FILTER (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"link_count_menu"))
async def link_count_menu(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]
    mn   = s.get("min_links", 0)
    mx   = s.get("max_links", 0)

    txt = (
        "🔗 **LINK COUNT FILTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Message mein kitne links hone chahiye?\n\n"
        f"**Min Links:** `{mn}` ({'disabled' if not mn else f'{mn}+ links required'})\n"
        f"**Max Links:** `{mx}` ({'disabled' if not mx else f'max {mx} links allowed'})\n\n"
        "Example: `min=1, max=3` → 1 to 3 links wale messages only\n"
        "Example: `min=0, max=0` → Disabled"
    )
    btns = [
        [Button.inline(f"Min: {mn} (change)", b"linkf_set_min"),
         Button.inline(f"Max: {mx} (change)", b"linkf_set_max")],
        [Button.inline("🗑 Disable Filter",   b"linkf_disable"),
         Button.inline("🔙 Back",             b"stab_new")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"linkf_set_min"))
async def linkf_set_min(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data["step"] = "linkf_min_input"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "🔗 Min links ka number bhejo (0 = disabled):",
            buttons=[[Button.inline("❌ Cancel", b"link_count_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"linkf_set_max"))
async def linkf_set_max(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data["step"] = "linkf_max_input"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "🔗 Max links ka number bhejo (0 = disabled):",
            buttons=[[Button.inline("❌ Cancel", b"link_count_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") in ("linkf_min_input", "linkf_max_input")))
async def linkf_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    step = data.get("step")
    data["step"] = None
    try:
        n = max(0, int(event.raw_text.strip()))
        key = "min_links" if step == "linkf_min_input" else "max_links"
        data["settings"][key] = n
        _save()
        await event.respond(
            f"✅ {key.replace('_', ' ').title()}: {n}",
            buttons=[[Button.inline("🔗 Link Filter", b"link_count_menu")]]
        )
    except ValueError:
        await event.respond("❌ Number bhejo")


@bot.on(events.CallbackQuery(data=b"linkf_disable"))
async def linkf_disable(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data["settings"]["min_links"] = 0
    data["settings"]["max_links"] = 0
    _save(); await event.answer("Link count filter disabled")
    await link_count_menu(event)


# ══════════════════════════════════════════
# HASHTAG FILTER MENU (NEW)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"hashtag_filter_menu"))
async def hashtag_filter_menu(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]
    req  = s.get("hashtag_required", [])
    blk  = s.get("hashtag_blocked",  [])
    mn   = s.get("min_hashtags", 0)
    mx   = s.get("max_hashtags", 0)

    req_str = ", ".join(req) if req else "_none_"
    blk_str = ", ".join(blk) if blk else "_none_"

    txt = (
        "# **HASHTAG FILTER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**✅ Required (at least one):** {req_str}\n"
        f"**🚫 Blocked (any = rejected):** {blk_str}\n"
        f"**Count:** min={mn}, max={mx if mx else '∞'}\n\n"
        "_Example required: #deal, #sale_\n"
        "_Example blocked: #spam, #nsfw_"
    )
    btns = [
        [Button.inline("➕ Add Required #tag", b"hashtag_add_req"),
         Button.inline("➕ Add Blocked #tag",  b"hashtag_add_blk")],
        [Button.inline(f"Min count: {mn}",     b"hashtag_set_min"),
         Button.inline(f"Max count: {mx}",     b"hashtag_set_max")],
        [Button.inline("🗑 Clear All",          b"hashtag_clear"),
         Button.inline("🔙 Back",              b"stab_new")],
    ]
    # Show delete buttons for existing tags
    for tag in req[:3]:
        btns.insert(-1, [Button.inline(f"🗑 Required: {tag}", f"hashtag_del_req_{tag}".encode())])
    for tag in blk[:3]:
        btns.insert(-1, [Button.inline(f"🗑 Blocked: {tag}", f"hashtag_del_blk_{tag}".encode())])

    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"hashtag_add_req"))
async def hashtag_add_req(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data["step"] = "hashtag_input_req"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "# Required hashtag bhejo:\nExample: `#deal` or `deal`",
            buttons=[[Button.inline("❌ Cancel", b"hashtag_filter_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"hashtag_add_blk"))
async def hashtag_add_blk(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data["step"] = "hashtag_input_blk"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "# Blocked hashtag bhejo:\nExample: `#spam` or `spam`",
            buttons=[[Button.inline("❌ Cancel", b"hashtag_filter_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") in ("hashtag_input_req", "hashtag_input_blk")))
async def hashtag_input(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    step = data.get("step")
    data["step"] = None
    tag = event.raw_text.strip().lstrip("#").lower()
    if not tag:
        await event.respond("❌ Valid hashtag bhejo"); return
    tag = f"#{tag}"
    key = "hashtag_required" if step == "hashtag_input_req" else "hashtag_blocked"
    lst = data["settings"].setdefault(key, [])
    if tag not in lst:
        lst.append(tag)
    _save()
    await event.respond(
        f"✅ Added to {key.replace('_', ' ')}: `{tag}`",
        buttons=[[Button.inline("# Hashtag Filter", b"hashtag_filter_menu")]]
    )


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") in ("hashtag_set_min_input", "hashtag_set_max_input")))
async def hashtag_minmax_input(event):
    """Handle min/max hashtag count input."""
    uid  = event.sender_id
    data = get_user_data(uid)
    step = data.get("step")
    data["step"] = None
    data.pop("step_since", None)
    try:
        val = int(event.raw_text.strip())
        if not 0 <= val <= 50:
            raise ValueError
    except ValueError:
        await event.respond(
            "❌ 0 se 50 ke beech koi number daalo!",
            buttons=[[Button.inline("🔙 Wapas", b"hashtag_filter_menu")]]
        )
        return
    key = "min_hashtags" if step == "hashtag_set_min_input" else "max_hashtags"
    data["settings"][key] = val
    _save()
    label = "Min" if key == "min_hashtags" else "Max"
    await event.respond(
        f"✅ {label} hashtag count set: `{val}`",
        buttons=[[Button.inline("# Hashtag Filter", b"hashtag_filter_menu")]]
    )


@bot.on(events.CallbackQuery(data=b"hashtag_clear"))
async def hashtag_clear(event):
    await event.answer()
    uid = event.sender_id
    data = get_user_data(uid)
    data["settings"]["hashtag_required"] = []
    data["settings"]["hashtag_blocked"]  = []
    data["settings"]["min_hashtags"]     = 0
    data["settings"]["max_hashtags"]     = 0
    _save(); await event.answer("Hashtag filter cleared")
    await hashtag_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"hashtag_set_min"))
async def hashtag_set_min(event):
    """Set minimum hashtag count — step input."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "hashtag_set_min_input"
    data["step_since"] = time.time()
    try:
        cur = data["settings"].get("min_hashtags", 0)
        try:
            await event.edit(
                f"🔢 **Min Hashtag Count Set Karo**\n\n"
                f"Abhi: `{cur}`\n\n"
                f"0 = koi limit nahi\n"
                f"Koi number bhejo (0–50):",
                buttons=[[Button.inline("❌ Cancel", b"hashtag_filter_menu")]]
            )
        except errors.MessageNotModifiedError:
            pass
    except Exception:
        pass


@bot.on(events.CallbackQuery(data=b"hashtag_set_max"))
async def hashtag_set_max(event):
    """Set maximum hashtag count — step input."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "hashtag_set_max_input"
    data["step_since"] = time.time()
    try:
        cur = data["settings"].get("max_hashtags", 0)
        try:
            await event.edit(
                f"🔢 **Max Hashtag Count Set Karo**\n\n"
                f"Abhi: `{cur}`\n\n"
                f"0 = koi limit nahi\n"
                f"Koi number bhejo (0–50):",
                buttons=[[Button.inline("❌ Cancel", b"hashtag_filter_menu")]]
            )
        except errors.MessageNotModifiedError:
            pass
    except Exception:
        pass


# ══════════════════════════════════════════
# ANALYTICS MENU (Upgraded)
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"analytics_menu"))
async def analytics_menu(event):
    await event.answer()
    uid = event.sender_id

    try:
        from smart_analytics import AnalyticsEngine
        report = AnalyticsEngine.get_full_report(uid)
    except ImportError:
        from analytics import get_analytics_report
        report = get_analytics_report(uid) if hasattr(get_analytics_report, '__call__') else "Analytics unavailable"
    except Exception as e:
        report = f"⚠️ Analytics error: {e}"

    btns = [
        [Button.inline("🔄 Refresh",       b"analytics_menu"),
         Button.inline("📥 Export CSV",    b"analytics_export")],
        [Button.inline("📊 Per-Source",    b"fwd_src_stats"),
         Button.inline("🏥 Dest Health",  b"cb_status")],
        [Button.inline("🔙 Main Menu",     b"main_menu")],
    ]
    try:
        await event.edit(report, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"analytics_export"))
async def analytics_export(event):
    await event.answer()
    await event.answer("Generating CSV...")
    uid = event.sender_id
    try:
        from smart_analytics import export_analytics_csv
        csv_text = export_analytics_csv(uid)
        await event.respond(
            f"📥 **Analytics CSV Export**\n\n```\n{csv_text[:3000]}\n```",
            buttons=[[Button.inline("🔙 Analytics", b"analytics_menu")]]
        )
    except Exception as e:
        await event.answer(f"Export error: {e}", alert=True)
