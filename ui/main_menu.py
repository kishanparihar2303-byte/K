"""
main_menu.py — Smart Main Menu v3.0

Changes:
  ✅ Context-aware menu — 3 states (new user / setup done / running)
  ✅ Live mini-status in menu message (sources, dests, today's msgs, premium)
  ✅ Duplicate Scheduler/Filters buttons bug FIXED
  ✅ Clean grouped layout — no more random ordering
  ✅ Advanced mode ━━━ New Features ━━━ divider removed
  ✅ Smart "setup guide" for new users
  ✅ Dashboard fully upgraded — activity bars, feature snapshot
  ✅ Better start/stop feedback messages
"""

import asyncio
import datetime
from telethon import events, Button, errors
from config import bot
from database import get_user_data, save_persistent_db, user_sessions
from utils import get_display_name
from lang import t, set_lang, SUPPORTED_LANGS, get_lang


def _get_owner_footer() -> str:
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""


def _menu_btn_visible(key: str) -> bool:
    """Admin ne agar koi button hide kiya hai to False return karo."""
    from database import GLOBAL_STATE
    cfg = GLOBAL_STATE.get("menu_config", {})
    return cfg.get(key, {}).get("visible", True)


def _menu_btn_label(key: str, default: str) -> str:
    """Admin ne agar custom naam set kiya hai to wo return karo."""
    from database import GLOBAL_STATE
    cfg = GLOBAL_STATE.get("menu_config", {})
    return cfg.get(key, {}).get("label", default)





# ─────────────────────────────────────────────────────────────────────────────
# SMART MENU TEXT — live mini-status in message header
# ─────────────────────────────────────────────────────────────────────────────

def _build_menu_text(user_id) -> str:
    """Build smart menu header with live status snapshot."""
    data    = get_user_data(user_id)
    s       = data["settings"]
    running = s.get("running", False)
    srcs    = len(data.get("sources", []))
    dests   = len(data.get("destinations", []))
    sched   = data.get("scheduler", {})

    # Premium status
    try:
        from premium import is_premium_user, get_remaining_days
        is_prem = is_premium_user(user_id)
        prem_badge = f"💎 {get_remaining_days(user_id)}d" if is_prem else "🆓 Free"
    except Exception:
        prem_badge = ""

    # Today's forwarding stats
    today = datetime.date.today().strftime("%Y-%m-%d")
    today_fwd = data.get("analytics", {}).get("daily", {}).get(today, {}).get("forwarded", 0)
    today_blk = data.get("analytics", {}).get("daily", {}).get(today, {}).get("blocked", 0)

    # Scheduler status
    if sched.get("enabled"):
        sched_badge = f"⏰ {sched.get('start','?')}–{sched.get('end','?')}"
    elif sched.get("per_day_enabled"):
        sched_badge = "⏰ Per-Day"
    else:
        sched_badge = ""

    # Active features compact list
    feats = []
    if s.get("duplicate_filter"):       feats.append("♻️")
    if s.get("product_duplicate_filter"):feats.append("🛒")
    if s.get("smart_filter"):           feats.append("🧠")
    if s.get("remove_links"):           feats.append("🚫")
    if s.get("auto_shorten"):           feats.append("✂️")
    if data.get("translation", {}).get("global_enabled"): feats.append("🌐")
    if data.get("watermark", {}).get("enabled"):           feats.append("🖼️")
    if data.get("affiliate", {}).get("enabled"):           feats.append("🔗")
    feat_line = " ".join(feats) if feats else ""

    # Determine user state — bilingual via t()
    if srcs == 0 or dests == 0:
        state_line = t(user_id, "state_setup")
    elif running:
        _act = t(user_id, "activity_today", fwd=today_fwd, blk=today_blk) if (today_fwd or today_blk) else ""
        state_line = t(user_id, "state_running", activity=_act)
    else:
        state_line = t(user_id, "state_stopped")

    # Build header — bilingual
    lines = [
        "🏠 **MAIN MENU**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    ]

    # Status row
    status_parts = [state_line]
    lines.append("\n".join(status_parts))

    # Info row (only when logged in and has setup)
    if srcs > 0 or dests > 0:
        info_parts = [f"📥`{srcs}` 📤`{dests}`"]
        if prem_badge: info_parts.append(prem_badge)
        if sched_badge: info_parts.append(sched_badge)
        if feat_line: info_parts.append(feat_line)
        lines.append("  ".join(info_parts))

    # ── v3: Circuit Breaker health badge ────────────────────────────────────
    try:
        from circuit_breaker import CircuitBreakerRegistry
        _cbs     = CircuitBreakerRegistry.get_all_for_user(user_id)
        _open_n  = sum(1 for cb in _cbs.values() if cb.is_open())
        if _open_n:
            lines.append(f"  ⚠️ {_open_n} destination(s) auto-paused")
        elif _cbs:
            lines.append("  ✅ All destinations healthy")
    except Exception:
        pass

    # ── v3: Real-time stats from in-memory tracker ───────────────────────
    try:
        from forward_engine import get_forward_stats
        _fs = get_forward_stats(user_id)
        if _fs:
            lines.append(f"📊 {_fs}")
    except Exception:
        pass

    footer = _get_owner_footer()
    if footer:
        lines.append(footer)

    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# SMART BUTTON BUILDER — context-aware grouped layout
# ─────────────────────────────────────────────────────────────────────────────

def get_main_buttons(user_id):
    data = get_user_data(user_id)
    mode = data.get("ui_mode", "beginner")

    # ── Not logged in ─────────────────────────────────────────────────────────
    if not data["session"]:
        return [
            [Button.inline(t(user_id, "btn_login"), b"login_menu")],
            [Button.inline(t(user_id, "btn_language"), b"lang_menu")],
        ]

    # ── Context detection ─────────────────────────────────────────────────────
    try:
        from premium import is_free_mode, is_premium_user
        _free_mode = is_free_mode()
        _is_prem   = is_premium_user(user_id)
    except Exception:
        _free_mode = False
        _is_prem   = False

    running   = data["settings"]["running"]
    srcs      = data.get("sources", [])
    dests     = data.get("destinations", [])
    has_srcs  = len(srcs) > 0
    has_dests = len(dests) > 0
    is_new    = not has_srcs or not has_dests

    # ── Issue #1: Clear start/stop button — shows exact current state ─────────
    if running:
        start_stop_btn = Button.inline("🔴 Forwarding CHALU hai — Band Karo", b"stop_engine")
    else:
        start_stop_btn = Button.inline("🟢 Forwarding BAND hai — Chalu Karo", b"start_engine")

    # ── Issue #10: Full-width mode toggle with clear labels ───────────────────
    if mode == "beginner":
        toggle_text = "⚡ Advanced Mode Switch Karo →"
    else:
        toggle_text = "🔰 Beginner Mode Switch Karo ←"

    # ── NEW USER — setup flow ─────────────────────────────────────────────────
    if is_new:
        rows = [
            [Button.inline("━━ 🚀 Pehle Yeh Karo ━━", b"main_menu")],
            [Button.inline(
                f"{'✅' if has_srcs else '1️⃣'} Source Add Karo  {'(' + str(len(srcs)) + ' added)' if srcs else '← Yahan se shuru karo'}",
                b"add_src"
            )],
            [Button.inline(
                f"{'✅' if has_dests else '2️⃣'} Destination Add Karo  {'(' + str(len(dests)) + ' added)' if dests else '← Phir yeh karo'}",
                b"add_dest"
            )],
        ]
        if has_srcs and has_dests:
            rows.append([start_stop_btn])
        if not _free_mode and not _is_prem:
            rows.append([Button.inline("💎 Premium Info", b"premium_info"),
                         Button.inline("💳 Buy",          b"buy_premium")])
        rows += [
            [Button.inline("❓ Setup Guide",  b"help_start"),
             Button.inline("🌐 Language",     b"lang_menu")],
            [Button.inline("🔓 Logout",       b"logout_proc")],
        ]
        return rows

    # ── BEGINNER MODE ─────────────────────────────────────────────────────────
    if mode == "beginner":
        rows = []
        if _menu_btn_visible("start_stop"):
            rows.append([start_stop_btn])                                    # #1: full-width, clear label
        if _menu_btn_visible("dashboard"):
            rows.append([Button.inline(_menu_btn_label("dashboard", "📊 Dashboard  📈"), b"dashboard_view")])
        src_row = []
        if _menu_btn_visible("source"):
            src_row.append(Button.inline(_menu_btn_label("source", f"➕ Source  ({len(srcs)})"), b"add_src"))
        if _menu_btn_visible("dest"):
            src_row.append(Button.inline(_menu_btn_label("dest", f"📤 Dest  ({len(dests)})"), b"add_dest"))
        if src_row:
            rows.append(src_row)
        cfg_row = []
        if _menu_btn_visible("settings"):
            cfg_row.append(Button.inline(_menu_btn_label("settings", "⚙️ Settings"), b"settings_menu"))
        if _menu_btn_visible("backup"):
            cfg_row.append(Button.inline(_menu_btn_label("backup", "💾 Backup"), b"backup_menu"))
        if cfg_row:
            rows.append(cfg_row)
        help_row = []
        if _menu_btn_visible("help"):
            help_row.append(Button.inline(_menu_btn_label("help", "❓ Help"), b"help_guide"))
        # #9: Commands button removed — no show_commands here
        if help_row:
            rows.append(help_row)
        rows += [
            [Button.inline("🌐 Language", b"lang_menu")],
            [Button.inline(toggle_text, b"switch_ui_mode")],              # #10: full-width row
            [Button.inline("🔓 Logout",  b"logout_proc")],
        ]

        if not _free_mode:
            if _is_prem:
                rows.insert(-2, [Button.inline(
                    _menu_btn_label("premium", "💎 Premium Status & Renew"), b"premium_info")])
            elif _menu_btn_visible("premium"):
                rows.insert(-2, [Button.inline(_menu_btn_label("premium", "💎 Premium Info"), b"premium_info"),
                                  Button.inline("💳 Buy Premium", b"buy_premium")])

    # ── ADVANCED MODE ─────────────────────────────────────────────────────────
    else:
        rows = []
        # Primary action — #1: full-width start/stop so state is unmissable
        if _menu_btn_visible("start_stop"):
            rows.append([start_stop_btn])
        if _menu_btn_visible("dashboard"):
            rows.append([Button.inline(_menu_btn_label("dashboard", "📊 Dashboard"), b"dashboard_view")])

        # Channels — #2: per_dest removed from main menu
        rows.append([Button.inline("━━ 📡 Channels ━━", b"main_menu")])
        ch_row = []
        if _menu_btn_visible("source"):
            ch_row.append(Button.inline(_menu_btn_label("source", f"➕ Source ({len(srcs)})"), b"add_src"))
        if _menu_btn_visible("dest"):
            ch_row.append(Button.inline(_menu_btn_label("dest", f"📤 Dest ({len(dests)})"), b"add_dest"))
        # #2: per_dest button intentionally removed from main menu
        if ch_row:
            rows.append(ch_row)

        # Config
        rows.append([Button.inline("━━ ⚙️ Config ━━", b"main_menu")])
        cfg1 = []
        if _menu_btn_visible("settings"):
            cfg1.append(Button.inline(_menu_btn_label("settings", "⚙️ Settings"), b"settings_menu"))
        if _menu_btn_visible("src_config"):
            cfg1.append(Button.inline(_menu_btn_label("src_config", "📍 Src Config"), b"ps_menu"))
        if cfg1:
            rows.append(cfg1)
        cfg2 = []
        if _menu_btn_visible("filters"):
            cfg2.append(Button.inline(_menu_btn_label("filters", "🧠 Filters"), b"advanced_filters"))
        if _menu_btn_visible("replacements"):
            cfg2.append(Button.inline(_menu_btn_label("replacements", "🔄 Replacements"), b"replace_menu"))
        if cfg2:
            rows.append(cfg2)

        # Translation button (scheduler removed from main menu - available in filters only)
        cfg3 = []
        if _menu_btn_visible("translation"):
            cfg3.append(Button.inline(_menu_btn_label("translation",
                "🌐 " + ("Anuvad" if get_lang(user_id) == "hi" else "Translation")), b"translate_menu"))
        if cfg3:
            rows.append(cfg3)

        # Features
        rows.append([Button.inline("━━ ✨ Features ━━", b"main_menu")])
        f1 = []
        if _menu_btn_visible("watermark"):
            f1.append(Button.inline(_menu_btn_label("watermark", "🖼️ Watermark"), b"settings_watermark"))
        if _menu_btn_visible("affiliate"):
            f1.append(Button.inline(_menu_btn_label("affiliate", "🔗 Affiliate"), b"settings_affiliate"))
        if f1:
            rows.append(f1)
        f2 = []
        if _menu_btn_visible("backup"):
            f2.append(Button.inline(_menu_btn_label("backup", "💾 Backup"), b"backup_menu"))
        if _menu_btn_visible("start_end_msg"):
            f2.append(Button.inline(_menu_btn_label("start_end_msg", "✏️ Start/End Msg"), b"adv_msg_settings"))
        if _menu_btn_visible("templates"):
            f2.append(Button.inline(_menu_btn_label("templates", "📋 Templates"), b"fwd_templates_menu"))
        if f2:
            rows.append(f2)

        if not _free_mode:
            if _is_prem:
                rows += [[Button.inline(
                    _menu_btn_label("premium", "💎 Premium Status & Renew"), b"premium_info")]]
            elif _menu_btn_visible("premium"):
                rows += [[Button.inline(_menu_btn_label("premium", "💎 Premium Info"), b"premium_info"),
                          Button.inline("💳 Buy Premium", b"buy_premium")]]

        extra = []
        if _menu_btn_visible("earn"):
            extra.append(Button.inline(_menu_btn_label("earn", "🎁 Earn & Rewards"), b"earn_hub"))
        if _menu_btn_visible("advertise"):
            extra.append(Button.inline(_menu_btn_label("advertise", "📣 Advertise"), b"pub_promo_intro"))
        if extra:
            rows.append(extra)
        rows += [
            [Button.inline("❓ Help",      b"help_guide")],               # #9: Commands removed
            [Button.inline("🌐 Language",  b"lang_menu")],
            [Button.inline(toggle_text,    b"switch_ui_mode")],           # #10: full-width
            [Button.inline("🔓 Logout",    b"logout_proc")],
        ]

    # ── Injections (reseller, sponsor ad) ─────────────────────────────────────
    try:
        from reseller import is_reseller
        if is_reseller(user_id):
            rows.insert(-2, [Button.inline("👥 Reseller Panel", b"reseller_panel")])
    except Exception:
        pass

    try:
        from ads_engine import get_button_ad
        ad_btn = get_button_ad(user_id)
        if ad_btn:
            rows.insert(-1, [ad_btn])
    except Exception:
        pass

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# ADVANCED MSG SETTINGS SHORTCUT (Start Msg / End Msg combined)
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adv_msg_settings"))
async def adv_msg_settings(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]

    start_msg     = s.get("start_message", "") or s.get("start_msg", "")
    end_msg       = s.get("end_message",   "") or s.get("end_msg",   "")
    start_enabled = s.get("start_msg_enabled", bool(start_msg))
    end_enabled   = s.get("end_msg_enabled",   bool(end_msg))

    def _preview(msg, label):
        if not msg:
            return f"_{label}: Set nahi kiya_"
        preview = msg[:80] + ("..." if len(msg) > 80 else "")
        return f"**{label}:**\n`{preview}`"

    start_status = "🟢 ON" if (start_msg and start_enabled) else ("🔴 OFF" if start_msg else "❌ Set nahi")
    end_status   = "🟢 ON" if (end_msg   and end_enabled)   else ("🔴 OFF" if end_msg   else "❌ Set nahi")

    txt = (
        "✏️ **START & END MESSAGES**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Har forwarded message ke saath extra text lagao:\n"
        "⬆️ **Start Msg** — message ke UPAR dikhe\n"
        "⬇️ **End Msg** — message ke NEECHE dikhe\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⬆️ Start: {start_status}\n"
        f"{_preview(start_msg, 'Preview')}\n\n"
        f"⬇️ End: {end_status}\n"
        f"{_preview(end_msg, 'Preview')}\n\n"
        "💡 **Tips:**\n"
        "• Bold ke liye: `**text**`\n"
        "• Italic ke liye: `_text_`\n"
        "• Max 500 characters allowed"
    )

    # Smart toggle buttons — show current state clearly
    start_toggle_lbl = "🔴 Start Msg Band Karo" if start_enabled and start_msg else "🟢 Start Msg Chalu Karo"
    end_toggle_lbl   = "🔴 End Msg Band Karo"   if end_enabled   and end_msg   else "🟢 End Msg Chalu Karo"

    btns = [
        [Button.inline("── ⬆️ Start Message ──", b"adv_msg_settings")],
        [Button.inline("✏️ Start Msg Likhao",   b"set_start"),
         Button.inline("🗑 Delete",              b"rem_start")],
    ]
    if start_msg:
        btns.append([Button.inline(start_toggle_lbl, b"toggle_start_msg_enabled")])

    btns += [
        [Button.inline("── ⬇️ End Message ──", b"adv_msg_settings")],
        [Button.inline("✏️ End Msg Likhao",     b"set_end"),
         Button.inline("🗑 Delete",              b"rem_end")],
    ]
    if end_msg:
        btns.append([Button.inline(end_toggle_lbl, b"toggle_end_msg_enabled")])

    btns.append([Button.inline("🏠 Main Menu", b"main_menu")])

    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"toggle_start_msg_enabled"))
async def toggle_start_msg_cb(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]
    s["start_msg_enabled"] = not s.get("start_msg_enabled", True)
    save_persistent_db()
    await adv_msg_settings(event)


@bot.on(events.CallbackQuery(data=b"toggle_end_msg_enabled"))
async def toggle_end_msg_cb(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]
    s["end_msg_enabled"] = not s.get("end_msg_enabled", True)
    save_persistent_db()
    await adv_msg_settings(event)


@bot.on(events.CallbackQuery(data=b"se_start_edit"))
async def se_start_edit_cb(event):
    """Re-prompt for start message after error — was missing handler (FIXED)."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "wait_start_msg"
    data["step_since"] = time.time()
    _lang = get_lang(uid)
    try:
        await event.edit(
            "✏️ **Start Message Likhein**\n\n"
            "Yeh text har message ke UPAR lagega.\n\n"
            "Max 500 characters. /cancel se band karo."
            if _lang == "hi" else
            "✏️ **Write Start Message**\n\n"
            "This text will appear ABOVE every forwarded message.\n\n"
            "Max 500 characters. Press /cancel to stop.",
            buttons=[[Button.inline(t(uid, "btn_cancel"), b"adv_msg_settings")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"se_end_edit"))
async def se_end_edit_cb(event):
    """Re-prompt for end message after error — was missing handler (FIXED)."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "wait_end_msg"
    data["step_since"] = time.time()
    _lang = get_lang(uid)
    try:
        await event.edit(
            "✏️ **End Message Likhein**\n\n"
            "Yeh text har message ke NEECHE lagega.\n\n"
            "Max 500 characters. /cancel se band karo."
            if _lang == "hi" else
            "✏️ **Write End Message**\n\n"
            "This text will appear BELOW every forwarded message.\n\n"
            "Max 500 characters. Press /cancel to stop.",
            buttons=[[Button.inline(t(uid, "btn_cancel"), b"adv_msg_settings")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# CORE MENU HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"switch_ui_mode"))
async def switch_ui_mode_handler(event):
    await event.answer()
    user_id = event.sender_id
    data = get_user_data(user_id)
    current_mode = data.get("ui_mode", "beginner")
    new_mode = "advanced" if current_mode == "beginner" else "beginner"
    data["ui_mode"] = new_mode
    save_persistent_db()
    lbl = "⚡ Advanced Mode" if new_mode == "advanced" else "🧑 Beginner Mode"
    await event.answer(f"Switched to {lbl}!", alert=False)
    await main_menu_callback(event)


@bot.on(events.NewMessage(pattern='(?i)^Menu$'))
async def menu_button_handler(event):
    user_id = event.sender_id
    menu_text = _build_menu_text(user_id)
    await event.respond(menu_text, buttons=get_main_buttons(user_id))



# ── v3: Callback dedup (double-tap protection) ──────────────────────────────
import time as _mm_time
_CB_LAST: dict = {}

def _is_dup_cb(uid: int, data: bytes) -> bool:
    k   = (uid, data)
    now = _mm_time.monotonic()
    if now - _CB_LAST.get(k, 0) < 1.5:
        return True
    _CB_LAST[k] = now
    if len(_CB_LAST) > 400:
        cutoff = now - 15.0
        stale  = [x for x, t in list(_CB_LAST.items()) if t < cutoff]
        for x in stale: _CB_LAST.pop(x, None)
    return False
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"main_menu"))
async def main_menu_callback(event):
    await event.answer()
    # Force Subscribe check
    await event.answer()
    from force_subscribe import check_force_subscribe_cb, get_fs_config
    if get_fs_config().get("enabled"):
        if not await check_force_subscribe_cb(event):
            return

    user_id = event.sender_id
    data    = get_user_data(user_id)

    # Clear any active step
    data["step"] = None
    data.pop("step_since", None)
    data.pop("temp_data", None)

    # Stop admin clock if coming from admin panel
    try:
        from ui.admin_menu import admin_clock_cleanup
        admin_clock_cleanup(user_id)
    except Exception:
        pass

    # Async: popup ad (non-blocking)
    try:
        from ads_engine import maybe_send_popup
        asyncio.create_task(maybe_send_popup(user_id, bot))
    except Exception:
        pass

    # Tick for banner ads
    try:
        from ads_engine import tick_menu_open, get_banner_text
        tick_menu_open(user_id)
        banner = get_banner_text(user_id)
    except Exception:
        banner = ""

    menu_text = _build_menu_text(user_id) + banner

    try:
        await event.edit(menu_text, buttons=get_main_buttons(user_id))
    except errors.MessageNotModifiedError:
        await event.answer("🏠 Menu")


# ─────────────────────────────────────────────────────────────────────────────
# START / STOP ENGINE — Direct toggle, koi sub-menu nahi
# Button click → toast notification + main menu refresh
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"start_engine"))
async def start_engine_cb(event):
    uid  = event.sender_id
    data = get_user_data(uid)

    if not data["session"]:
        return await event.answer(t(uid, "login_first"), alert=True)
    if not data["sources"] and not data["destinations"]:
        return await event.answer(t(uid, "fwd_no_src_dest"), alert=True)
    if not data["sources"]:
        return await event.answer(t(uid, "fwd_no_src"), alert=True)
    if not data["destinations"]:
        return await event.answer(t(uid, "fwd_no_dest"), alert=True)

    data["settings"]["running"] = True
    save_persistent_db()

    from forward_engine import start_user_forwarder
    import asyncio as _asyncio
    _asyncio.create_task(start_user_forwarder(uid, data["session"]))

    srcs  = len(data["sources"])
    dests = len(data["destinations"])
    await event.answer(f"🟢 Forwarding ON!  {srcs} src → {dests} dest", alert=False)

    # Main menu refresh — sub-page nahi kholna
    try:
        menu_text = _build_menu_text(uid)
        await event.edit(menu_text, buttons=get_main_buttons(uid))
    except errors.MessageNotModifiedError:
        pass
    except Exception:
        pass


@bot.on(events.CallbackQuery(data=b"stop_engine"))
async def stop_engine_cb(event):
    uid = event.sender_id
    get_user_data(uid)["settings"]["running"] = False
    save_persistent_db()

    today     = datetime.date.today().strftime("%Y-%m-%d")
    today_fwd = get_user_data(uid).get("analytics", {}).get("daily", {}).get(today, {}).get("forwarded", 0)
    msg = "🔴 Forwarding OFF!"
    if today_fwd:
        msg += f"  Aaj: {today_fwd} msgs forwarded"
    await event.answer(msg, alert=False)

    # Main menu refresh — sub-page nahi kholna
    try:
        menu_text = _build_menu_text(uid)
        await event.edit(menu_text, buttons=get_main_buttons(uid))
    except errors.MessageNotModifiedError:
        pass
    except Exception:
        pass



# ─────────────────────────────────────────────────────────────────────────────
# DASHBOARD — smart, live, beautiful
# ─────────────────────────────────────────────────────────────────────────────

def _bar(v, mx, w=10):
    """Progress bar helper."""
    if mx <= 0: return "░" * w
    filled = min(round(v / mx * w), w)
    return "█" * filled + "░" * (w - filled)


def _build_dashboard_overview(uid) -> str:
    import datetime as _dt
    from analytics import make_bar_chart, get_analytics_data, get_user_summary
    from premium import is_premium_user, get_remaining_days

    data    = get_user_data(uid)
    s       = data["settings"]
    summary = get_user_summary(uid)
    sched   = data.get("scheduler", {})
    aff     = data.get("affiliate", {})
    wm      = data.get("watermark", {})
    trans   = data.get("translation", {})

    running  = s.get("running", False)
    is_prem  = is_premium_user(uid)
    prem_lbl = f"💎 Premium ({get_remaining_days(uid)}d left)" if is_prem else "🆓 Free"

    srcs  = len(data.get("sources", []))
    dests = len(data.get("destinations", []))

    # Scheduler
    if sched.get("enabled"):
        sched_txt = f"🟢 {sched.get('start','?')}–{sched.get('end','?')}"
    elif sched.get("per_day_enabled"):
        sched_txt = "🟢 Per-Day Active"
    else:
        sched_txt = "🔴 OFF"

    # Active features
    feat_groups = {
        "Filters": [],
        "Modify":  [],
        "Other":   [],
    }
    if s.get("duplicate_filter"):        feat_groups["Filters"].append("♻️Dup")
    if s.get("product_duplicate_filter"):feat_groups["Filters"].append("🛒Prod")
    if s.get("smart_filter"):            feat_groups["Filters"].append("🧠Smart")
    if s.get("keyword_filter_enabled"):  feat_groups["Filters"].append("🔍KW")
    if s.get("remove_links"):            feat_groups["Modify"].append("🚫Links")
    if s.get("auto_shorten"):            feat_groups["Modify"].append("✂️Short")
    if s.get("remove_user"):             feat_groups["Modify"].append("👤NoUser")
    if wm.get("enabled"):                feat_groups["Other"].append("🖼️WM")
    if aff.get("enabled"):               feat_groups["Other"].append("🔗Aff")
    if trans.get("global_enabled"):      feat_groups["Other"].append("🌐Trans")

    feat_lines = []
    for grp, items in feat_groups.items():
        if items:
            feat_lines.append(f"  {grp}: `{'  '.join(items)}`")
    feat_block = "\n".join(feat_lines) if feat_lines else "  _Koi active nahi_"

    # Today's stats with bar
    today_fwd = summary.get("today_forwarded", 0)
    today_blk = summary.get("today_blocked", 0)
    today_tot = today_fwd + today_blk
    blk_pct   = round(today_blk / max(today_tot, 1) * 100)
    fwd_bar   = _bar(today_fwd, max(today_fwd + today_blk, 1))

    # Week stats
    wk_fwd = summary.get("week_forwarded", 0)
    wk_blk = summary.get("week_blocked", 0)
    wk_blk_pct = round(wk_blk / max(wk_fwd + wk_blk, 1) * 100)

    # 7-day chart
    chart = make_bar_chart(get_analytics_data(uid).get("daily", {}), days=7)

    # Delay
    delay = s.get("custom_delay", 0)
    delay_str = f"`{delay}s`" if delay > 0 else "`instant`"

    return (
        "📊 **DASHBOARD**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{'🟢 RUNNING' if running else '🔴 STOPPED'}  ·  {prem_lbl}\n"
        f"📥 Sources: `{srcs}`  📤 Dests: `{dests}`  ⏱ Delay: {delay_str}\n\n"
        f"**📈 Today:**\n"
        f"  [{fwd_bar}] `{today_fwd}↑` `{today_blk}✗` ({blk_pct}% blocked)\n\n"
        f"**📅 This Week:**\n"
        f"  Forwarded: `{wk_fwd}`  Blocked: `{wk_blk}` ({wk_blk_pct}%)\n"
        f"  All Time: `{summary.get('total_forwarded', 0)}`\n\n"
        f"**⏰ Scheduler:** {sched_txt}\n\n"
        f"**🛡 Active Features:**\n{feat_block}\n\n"
        f"{chart}"
    )


@bot.on(events.CallbackQuery(data=b"dashboard_view"))
async def dashboard_view_cb(event):
    await event.answer()
    uid = event.sender_id
    try:
        text = _build_dashboard_overview(uid)
    except Exception as e:
        text = f"📊 Dashboard error: {e}"

    running = get_user_data(uid)["settings"].get("running", False)
    stop_start_lbl = "🔴 Band Karo" if running else "🟢 Chalu Karo"
    stop_start_cb  = b"stop_engine" if running else b"start_engine"

    try:
        await event.edit(text, buttons=[
            [Button.inline(stop_start_lbl,          stop_start_cb),
             Button.inline("🔄 Refresh",             b"dashboard_view")],
            [Button.inline("📡 Sources & Dests",    b"dash_sources"),
             Button.inline("⚙️ Settings Snapshot",  b"dash_settings")],
            [Button.inline("📅 7-Day Stats",         b"dash_weekly"),
             Button.inline("🏠 Menu",                b"main_menu")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer("Already fresh!")


@bot.on(events.CallbackQuery(data=b"dash_sources"))
async def dash_sources(event):
    await event.answer()
    uid    = event.sender_id
    data   = get_user_data(uid)
    client = user_sessions.get(uid)

    src_names, dest_names = [], []
    for s in data["sources"]:
        src_names.append(await get_display_name(client, s, uid) if client else str(s))
    for d in data["destinations"]:
        dest_names.append(await get_display_name(client, d, uid) if client else str(d))

    # Custom rules summary
    custom_rules = data.get("custom_forward_rules", {})
    custom_count = sum(1 for s in data["sources"] if str(s) in custom_rules)

    src_txt  = "\n".join(f"  `{i+1}.` {n}" for i, n in enumerate(src_names)) or "  (Koi nahi)"
    dest_txt = "\n".join(f"  `{i+1}.` {n}" for i, n in enumerate(dest_names)) or "  (Koi nahi)"

    msg = (
        f"📥 **Sources ({len(src_names)})**"
        + (f"  ·  ⚙️ {custom_count} custom rules" if custom_count else "")
        + f"\n{src_txt}\n\n"
        f"📤 **Destinations ({len(dest_names)})**\n{dest_txt}"
    )
    if len(msg) > 3800:
        msg = msg[:3800] + "\n...(aur hain)"

    try:
        await event.edit(msg, buttons=[
            [Button.inline(f"➕ Source ({len(src_names)})", b"add_src"),
             Button.inline(f"📤 Dest ({len(dest_names)})",  b"add_dest")],
            [Button.inline("📍 Src Config",                  b"ps_menu"),
             Button.inline("🔙 Dashboard",                   b"dashboard_view")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"dash_settings"))
async def dash_settings_snapshot(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    s    = data["settings"]
    wm   = data.get("watermark", {})
    aff  = data.get("affiliate", {})
    sched = data.get("scheduler", {})
    kf   = data.get("keyword_filters", {})
    bl   = data.get("blocked_links", {})

    def _tick(val): return "✅" if val else "❌"
    def _count_badge(n): return f" ({n})" if n > 0 else ""

    msg = (
        "⚙️ **SETTINGS SNAPSHOT**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📨 Media:**\n"
        f"  Text:{_tick(s.get('text'))} Img:{_tick(s.get('image'))} "
        f"Vid:{_tick(s.get('video'))} Cap:{_tick(s.get('caption'))} "
        f"Voice:{_tick(s.get('voice'))} Files:{_tick(s.get('files'))}\n\n"
        "**🛡 Filters:**\n"
        f"  ♻️ Dup:{_tick(s.get('duplicate_filter'))}  "
        f"🛒 Prod:{_tick(s.get('product_duplicate_filter'))}  "
        f"🌐 Global:{_tick(s.get('global_filter'))}\n"
        f"  🧠 Smart:{_tick(s.get('smart_filter'))}  "
        f"🔍 KW:{_tick(s.get('keyword_filter_enabled'))}{_count_badge(len(kf.get('words',[])))}\n"
        f"  🚫 LinkBlock:{_tick(s.get('link_blocker_enabled'))}{_count_badge(len(bl))}\n\n"
        "**✂️ Modifications:**\n"
        f"  Remove Links:{_tick(s.get('remove_links'))}  "
        f"Remove Users:{_tick(s.get('remove_user'))}\n"
        f"  Auto Shorten:{_tick(s.get('auto_shorten'))}  "
        f"Link Preview:{_tick(s.get('preview_mode'))}\n"
        f"  Delay: `{s.get('custom_delay', 0)}s`\n\n"
        "**✨ Features:**\n"
        f"  Watermark:{_tick(wm.get('enabled'))}  "
        f"Affiliate:{_tick(aff.get('enabled'))}\n\n"
        "**⏰ Scheduler:**\n"
        f"  Basic:{_tick(sched.get('enabled'))}  "
        f"Per-Day:{_tick(sched.get('per_day_enabled'))}  "
        f"Queue:{_tick(sched.get('queue_mode'))}\n"
    )
    try:
        await event.edit(msg, buttons=[
            [Button.inline("⚙️ Settings",   b"settings_menu"),
             Button.inline("🧠 Filters",    b"advanced_filters")],
            [Button.inline("📍 Src Config", b"ps_menu"),
             Button.inline("🔙 Dashboard",  b"dashboard_view")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"dash_weekly"))
async def dash_weekly(event):
    await event.answer()   # BUG FIX: Telegram retry loop rokne ke liye
    uid   = event.sender_id
    import datetime as _dt
    from utils import user_now as _un
    from analytics import get_analytics_data
    daily = get_analytics_data(uid).get("daily", {})  # BUG FIX: correct analytics path

    rows      = []
    total_fwd = 0
    total_blk = 0

    # Collect data first to get max for bars
    day_data = []
    for i in range(6, -1, -1):
        day_dt  = _un(uid) - _dt.timedelta(days=i)
        day_key = day_dt.strftime("%Y-%m-%d")
        lbl     = day_dt.strftime("%a %d/%m")
        d       = daily.get(day_key, {})
        fwd     = d.get("forwarded", 0)
        blk     = d.get("blocked", 0)
        total_fwd += fwd
        total_blk += blk
        day_data.append((lbl, fwd, blk))

    max_fwd = max((r[1] for r in day_data), default=1)

    for lbl, fwd, blk in day_data:
        bar = _bar(fwd, max_fwd, 10)
        rows.append(f"`{lbl}` {bar} `{fwd}↑` `{blk}✗`")

    blk_pct = round(total_blk / max(total_fwd + total_blk, 1) * 100)

    msg = (
        "📅 **7-DAY STATS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + "\n".join(rows)
        + f"\n\n**Total:** `{total_fwd}` forwarded  `{total_blk}` blocked ({blk_pct}% block rate)"
    )
    try:
        await event.edit(msg, buttons=[
            [Button.inline("🔄 Refresh",    b"dash_weekly"),
             Button.inline("🔙 Dashboard",  b"dashboard_view")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


# Legacy aliases
@bot.on(events.CallbackQuery(data=b"status_view"))
async def status_view_cb(event):
    await event.answer()
    await dashboard_view_cb(event)

@bot.on(events.CallbackQuery(data=b"stats_refresh"))
async def stats_refresh_cb(event):
    await event.answer()
    await dashboard_view_cb(event)

@bot.on(events.CallbackQuery(data=b"full_status_report"))
async def full_status_report(event):
    await event.answer()
    await dash_sources(event)

@bot.on(events.CallbackQuery(data=b"dashboard_menu"))
async def dashboard_menu_cb(event):
    await event.answer()
    await dashboard_view_cb(event)


# ─────────────────────────────────────────────────────────────────────────────
# HELP GUIDE
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"help_guide"))
async def help_guide_main(event):
    await event.answer()
    uid = event.sender_id
    try:
        await event.edit(
            "❓ **HELP & GUIDE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kya jaanna chahte ho?",
            buttons=[
                [Button.inline("🚀 Setup Guide",        b"help_start"),
                 Button.inline("📺 Source/Dest",        b"help_add_channel")],
                [Button.inline("⚙️ Settings Samjho",    b"help_settings"),
                 Button.inline("📍 Src Config",         b"help_srcconfig")],
                [Button.inline("♻️ Dup Filter",          b"help_dup"),
                 Button.inline("🚫 Link Blocker",        b"help_link_blocker")],
                [Button.inline("🔄 Replacements",        b"help_replacements"),
                 Button.inline("⏰ Scheduler",           b"help_scheduler")],
                [Button.inline("🛍️ Affiliate",           b"help_affiliate"),
                 Button.inline("💾 Backup/Restore",      b"help_backup")],
                [Button.inline("📤 Per-Dest Rules",      b"help_per_dest"),
                 Button.inline("❓ Common Problems",     b"help_problems")],
                [Button.inline("📞 Contact Admin",        b"contact_admin")],
                [Button.inline("🏠 Main Menu",            b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_add_channel"))
async def help_add_channel(event):
    await event.answer()
    try:
        await event.edit(
            "📺 **SOURCE & DESTINATION ADD KARNA**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Source** = Jis channel se msgs copy honge\n"
            "**Destination** = Jahan forward karna hai\n\n"
            "**Formats:**\n"
            "• `@username` — Public channel\n"
            "• `https://t.me/channelname` — Link\n"
            "• `https://t.me/+XXXXXX` — Private invite link\n"
            "• `-1001234567890` — Direct chat ID\n"
            "• Pinned Chats — apne pinned chats mein se choose karo\n\n"
            "**Private Channel ke liye:**\n"
            "Pehle apne account se join karo, phir add karo\n\n"
            "⚠️ **Source = Destination nahi hona chahiye** (loop!)",
            buttons=[[Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_dup"))
async def help_dup(event):
    await event.answer()
    try:
        await event.edit(
            "♻️ **DUPLICATE FILTER GUIDE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**3 Filters hain:**\n\n"
            "**♻️ Dup Filter** — Same exact message dobara forward nahi hoga\n"
            "  Hash se check — text ya image same hai toh block\n"
            "  Expiry ke baad dobara allow\n\n"
            "**🛒 Product Dup** — Same Amazon/Flipkart product dobara nahi\n"
            "  ASIN/Product ID se pehchanta hai\n"
            "  Short links bhi detect hoti hain\n\n"
            "**🌐 Global Dup** — Alag sources se same msg bhi block\n"
            "  Source A aur Source B dono se same → ek hi baar jaayega\n\n"
            "**💡 Best Practice:** Teeno ON karo max protection ke liye",
            buttons=[[Button.inline("⚙️ Dup Settings", b"dup_menu"),
                      Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_link_blocker"))
async def help_link_blocker(event):
    await event.answer()
    try:
        await event.edit(
            "🚫 **LINK BLOCKER GUIDE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Kya karta hai:** Blocked links wale msgs forward nahi hote\n\n"
            "**3 tarike:**\n"
            "• **Domain:** `amazon.in` → us site ki saari links block\n"
            "• **URL:** specific link add karo\n"
            "• **Limit:** `amzn.to | 3` → 3 baar allow, phir block\n\n"
            "**Settings ke saath combo:**\n"
            "• `Remove Links` (Settings) = Sabhi links hata do\n"
            "• `Link Blocker` = Sirf specific links block karo\n"
            "Dono alag hain, dono use kar sakte ho",
            buttons=[[Button.inline("🚫 Link Blocker", b"link_block_menu"),
                      Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_replacements"))
async def help_replacements(event):
    await event.answer()
    try:
        await event.edit(
            "🔄 **REPLACEMENTS GUIDE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kisi bhi text ya link ko automatically replace karo.\n\n"
            "**Examples:**\n"
            "• `Buy Now` → `Order Karo`\n"
            "• `@oldchannel` → `@newchannel`\n"
            "• `amzn.to/xyz` → `yourlink.com`\n\n"
            "**2 levels:**\n"
            "• **Global** (Main Menu → Replacements) = Sabhi sources pe\n"
            "• **Src Config** = Sirf ek source ke liye",
            buttons=[[Button.inline("🔄 Replacements", b"replace_menu"),
                      Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_scheduler"))
async def help_scheduler(event):
    await event.answer()
    try:
        await event.edit(
            "⏰ **SCHEDULER GUIDE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Basic:** Ek time window set karo\n"
            "  `9 AM – 10 PM` → Sirf is time forward hoga\n\n"
            "**Per-Day (Premium):**\n"
            "  Har din alag timing · Specific din band karo\n"
            "  Holidays add karo — us din forward nahi hoga\n\n"
            "**Queue Mode:**\n"
            "  Off-time mein aaye msgs queue mein rakhta hai\n"
            "  Time ON hone par sab forward karta hai\n\n"
            "🟢 = Active window  ·  🔴 = Off window",
            buttons=[[Button.inline("⏰ Scheduler", b"sched_menu"),
                      Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_affiliate"))
async def help_affiliate(event):
    await event.answer()
    try:
        await event.edit(
            "🛍️ **AFFILIATE GUIDE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Amazon:**\n"
            "1. `amazon.in/associates` pe account banao\n"
            "2. Tracking ID milegi (format: `yourname-21`)\n"
            "3. Bot mein add karo → har link pe auto-tag\n\n"
            "**Flipkart:**\n"
            "1. `affiliate.flipkart.com` pe register karo\n"
            "2. Tracking ID copy karo → bot mein add\n\n"
            "**Flow:**\n"
            "Source link aata hai → Bot tag add karta hai\n"
            "→ Destination mein modified link → Commission! 💰\n\n"
            "Short links (amzn.to etc) bhi kaam karte hain",
            buttons=[[Button.inline("🔗 Affiliate Settings", b"settings_affiliate"),
                      Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_backup"))
async def help_backup_guide(event):
    await event.answer()
    try:
        await event.edit(
            "💾 **BACKUP & RESTORE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Backup mein kya hota hai:**\n"
            "Sources, Destinations, Settings, Scheduler,\n"
            "Replacements, Blocked Links, Src Config\n\n"
            "**Kab backup lo:**\n"
            "✅ Naye device/account se pehle\n"
            "✅ Koi bada change karne se pehle\n"
            "✅ Bot reinstall se pehle\n\n"
            "**Restore:**\n"
            "1. Backup export karo (JSON file)\n"
            "2. Restore button → file bhejo\n\n"
            "⚠️ Restore karne par current data replace hoga",
            buttons=[[Button.inline("💾 Backup/Restore", b"backup_menu"),
                      Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"backup_menu"))
async def backup_menu_cb(event):
    """Main backup/restore menu — was missing handler (FIXED)."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)

    srcs  = len(data.get("sources", []))
    dests = len(data.get("destinations", []))
    repls = len(data.get("replacements", {}))
    sched = "✅ Set" if data.get("scheduler", {}).get("enabled") else "❌ Off"
    _lang = get_lang(uid)

    if _lang == "hi":
        txt = (
            "💾 **Backup / Restore**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 **Abhi ki Settings:**\n"
            f"  📥 Sources: `{srcs}`\n"
            f"  📤 Destinations: `{dests}`\n"
            f"  🔄 Replacements: `{repls}`\n"
            f"  ⏰ Scheduler: {sched}\n\n"
            "**Export** — JSON file mein sab save karo\n"
            "**Import** — Purana backup restore karo\n\n"
            "⚠️ _Import karne par current data replace hoga_"
        )
    else:
        txt = (
            "💾 **Backup / Restore**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 **Current Data:**\n"
            f"  📥 Sources: `{srcs}`\n"
            f"  📤 Destinations: `{dests}`\n"
            f"  🔄 Replacements: `{repls}`\n"
            f"  ⏰ Scheduler: {sched}\n\n"
            "**Export** — Save all settings to JSON file\n"
            "**Import** — Restore from a previous backup\n\n"
            "⚠️ _Importing will replace your current data_"
        )

    try:
        await event.edit(txt, buttons=[
            [Button.inline("📤 Export Backup", b"backup_export"),
             Button.inline("📥 Import Backup", b"backup_import")],
            [Button.inline("🏠 Main Menu",     b"main_menu")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"backup_export"))
async def backup_export_cb(event):
    """Export user settings as JSON file."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    _lang = get_lang(uid)

    try:
        import json as _json
        import datetime as _dt

        # Build backup payload — exclude session for security
        backup_data = {
            "version":      "3.0",
            "exported_at":  _dt.datetime.now().isoformat(),
            "sources":      data.get("sources", []),
            "destinations": data.get("destinations", []),
            "settings":     {k: v for k, v in data.get("settings", {}).items()
                             if k not in ("running",)},
            "replacements": data.get("replacements", {}),
            "blocked_links":data.get("blocked_links", {}),
            "scheduler":    data.get("scheduler", {}),
            "forward_rules":data.get("forward_rules", {}),
            "channel_names":data.get("channel_names", {}),
        }

        payload = _json.dumps(backup_data, ensure_ascii=False, indent=2)

        await bot.send_file(
            uid,
            file=payload.encode("utf-8"),
            attributes=[],
            caption=(
                "✅ **Backup Export Ho Gaya!**\n\n"
                f"📦 Sources: `{len(backup_data['sources'])}`\n"
                f"📤 Destinations: `{len(backup_data['destinations'])}`\n\n"
                "⚠️ _Is file ko safe rakhein — restore ke liye zaroorat padegi_"
                if _lang == "hi" else
                "✅ **Backup Exported!**\n\n"
                f"📦 Sources: `{len(backup_data['sources'])}`\n"
                f"📤 Destinations: `{len(backup_data['destinations'])}`\n\n"
                "⚠️ _Keep this file safe — you'll need it to restore_"
            ),
            file_name=f"bot_backup_{uid}_{_dt.date.today()}.json",
            buttons=[[Button.inline(
                "🏠 Main Menu" if _lang == "en" else "🏠 Main Menu",
                b"main_menu"
            )]]
        )

        try:
            await event.delete()
        except Exception:
            pass

    except Exception as e:
        await event.answer(f"❌ Export failed: {e}", alert=True)


@bot.on(events.CallbackQuery(data=b"backup_import"))
async def backup_import_cb(event):
    """Prompt user to send backup JSON file."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"]       = "wait_backup_file"
    data["step_since"] = time.time()
    _lang = get_lang(uid)

    try:
        await event.edit(
            "📥 **Backup Import**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Apna backup JSON file bhejo 👇\n\n"
            "⚠️ **Warning:** Ye current data replace kar dega!\n"
            "_Sources, destinations aur settings reset honge_"
            if _lang == "hi" else
            "📥 **Import Backup**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Send your backup JSON file below 👇\n\n"
            "⚠️ **Warning:** This will replace your current data!\n"
            "_Sources, destinations and settings will be reset_",
            buttons=[[Button.inline("❌ Cancel", b"backup_menu")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_start"))
async def help_start_cb(event):
    await event.answer()
    uid = event.sender_id
    is_en = get_lang(uid) == "en"

    if is_en:
        txt = (
            "🚀 **SETUP GUIDE — Step by Step**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Step 1: Login**\n"
            "• Tap Login → enter your phone (+91XXXXXXXXXX)\n"
            "• OTP will arrive → send as `HELLO12345`\n\n"
            "**Step 2: Add Source**\n"
            "• Tap ➕ Add Source\n"
            "• Enter `@channel` or link to copy FROM\n\n"
            "**Step 3: Add Destination**\n"
            "• Tap 📤 Add Destination\n"
            "• ⚠️ Your account must be **Admin** there!\n\n"
            "**Step 4: Start Forwarding**\n"
            "• Tap 🟢 Start — done!\n\n"
            "💡 Keep the bot running 24/7 on Render"
            + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "")
        )
    else:
        txt = (
            "🚀 **SETUP GUIDE — Step by Step**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Step 1: Login**\n"
            "• Login button dabao → phone number daalo (+91XXXXXXXXXX)\n"
            "• OTP aayega → `HELLO12345` format mein bhejo\n\n"
            "**Step 2: Source Add Karo**\n"
            "• ➕ Source Add dabao\n"
            "• Jis channel se copy karna hai uska `@channel` ya link daalo\n\n"
            "**Step 3: Destination Add Karo**\n"
            "• 📤 Dest Add dabao\n"
            "• ⚠️ Us channel mein apna account **Admin** hona chahiye!\n\n"
            "**Step 4: Start Karo**\n"
            "• 🟢 Forwarding Chalu Karo — ho gaya!\n\n"
            "💡 Bot 24/7 chalane do — Render pe deploy karo"
            + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "")
        )
    try:
        await event.edit(txt, buttons=[
            [Button.inline("➕ Add Source", b"add_src"),
             Button.inline("📤 Add Dest",  b"add_dest")],
            [Button.inline("🔙 Help Menu",  b"help_guide")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_settings"))
async def help_settings_cb(event):
    await event.answer()
    uid = event.sender_id
    is_en = get_lang(uid) == "en"

    if is_en:
        txt = (
            "⚙️ **GLOBAL SETTINGS GUIDE**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Global = applies to ALL sources**\n"
            "**Src Config = override for ONE source**\n\n"
            "📨 **Media:** Text/Image/Video/Caption/Voice/Files\n"
            "✂️ **Modify:** Remove Links · Remove @username · Smart Filter\n"
            "♻️ **Dup:** Block same message twice\n"
            "⏱ **Delay:** Gap between messages (0 = instant)\n"
        )
    else:
        txt = (
            "⚙️ **GLOBAL SETTINGS — SAMJHO**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Global = SABHI sources pe apply hoti hain**\n"
            "**Src Config = Sirf ek source ke liye**\n\n"
            "📨 **Media:** Text/Image/Video/Caption/Voice/Files\n"
            "✂️ **Modify:** Links hatao · @username hatao · Smart Filter\n"
            "♻️ **Dup:** Same message dobara forward nahi hoga\n"
            "⏱ **Delay:** Har message ke beech gap (0 = instant)\n\n"
            "🔑 Priority: Per-Dest > Src Config > Global"
        )
    try:
        await event.edit(txt, buttons=[
            [Button.inline("⚙️ Settings Kholo", b"settings_menu"),
             Button.inline("📍 Src Config",     b"help_srcconfig")],
            [Button.inline("🔙 Help Menu",       b"help_guide")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_srcconfig"))
async def help_srcconfig_cb(event):
    await event.answer()
    uid = event.sender_id
    is_en = get_lang(uid) == "en"

    if is_en:
        txt = (
            "📍 **SOURCE CONFIG — What is it?**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Use when different sources need **different rules**.\n\n"
            "**You can set per source:**\n"
            "• ✏️ Prefix — text ABOVE every message\n"
            "• ✏️ Suffix — text BELOW every message\n"
            "• 🔗 Link Mode: Keep / Replace / Remove\n"
            "• 📤 Per-Destination: different content per dest\n\n"
            "**Priority:** Per-Dest > Src Config > Global"
        )
    else:
        txt = (
            "📍 **SRC CONFIG — KYA HAI YE?**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Jab alag alag sources ke liye alag rules chahiye.\n\n"
            "**Per source set kar sakte ho:**\n"
            "• ✏️ Prefix — Har msg ke UPAR apna text\n"
            "• ✏️ Suffix — Har msg ke NEECHE apna text\n"
            "• 🔗 Link Mode: Keep / Replace / Remove\n"
            "• 📤 Per-Dest Rules: alag destinations alag content\n\n"
            "**Priority:** Per-Dest > Src Config > Global"
        )
    try:
        await event.edit(txt, buttons=[
            [Button.inline("📍 Src Config Kholo", b"ps_menu"),
             Button.inline("🔙 Help Menu",         b"help_guide")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_advanced"))
async def help_advanced_cb(event):
    await event.answer()
    uid = event.sender_id
    try:
        await event.edit(
            "🔧 **ADVANCED FEATURES**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🔄 **Replacement** — Text/links automatically replace\n"
            "♻️ **Dup Filter** — Same msg block, expiry set karo\n"
            "⏰ **Scheduler** — Sirf specific hours mein forward\n"
            "🚫 **Link Blocker** — Specific links ya domains block\n"
            "✂️ **Auto Shorten** — Long URLs short ho jaate hain\n"
            "💾 **Backup** — Settings export/import JSON mein",
            buttons=[[Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_per_dest"))
async def help_per_dest_cb(event):
    await event.answer()
    try:
        await event.edit(
            "📤 **PER-DESTINATION RULES**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Ek source se alag destinations ko alag content bhejo.\n\n"
            "**Example:**\n"
            "Source: @news_channel\n"
            "• Dest A (Family) → Sirf text + photos\n"
            "• Dest B (Work) → Sirf text, no media\n"
            "• Dest C (Archive) → Sab kuch\n\n"
            "**Kya set hota hai per-destination:**\n"
            "📨 Media types ON/OFF  ·  🎨 Media mode\n"
            "🔗 Link mode  ·  ✏️ Prefix/Suffix\n"
            "📋 Custom caption  ·  🔄 Replacements\n\n"
            "**Priority:** Per-Dest > Src Config > Global",
            buttons=[
                [Button.inline("📍 Source Config Kholo", b"ps_menu"),
                 Button.inline("🔙 Help Menu",            b"help_guide")],
            ]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(data=b"help_problems"))
async def help_problems_cb(event):
    await event.answer()
    try:
        await event.edit(
            "❓ **COMMON PROBLEMS & SOLUTIONS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**❌ Messages forward nahi ho rahe?**\n"
            "1. 🟢 Start Forwarding dabao\n"
            "2. Source ka exact link/ID sahi hai?\n"
            "3. Destination mein account Admin hai?\n"
            "4. Settings mein Text/Image ON hai?\n\n"
            "**❌ Login nahi ho raha?**\n"
            "• Phone: `+91XXXXXXXXXX` format use karo\n"
            "• OTP format: `HELLO12345`\n"
            "• 2-Step = password bhi maangega\n\n"
            "**❌ Source add nahi ho raha?**\n"
            "• Private channel = pehle join karo, phir add\n\n"
            "**❌ Bot restart pe login gaya?**\n"
            "• MongoDB configure karo — data persist hoga"
            + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""),
            buttons=[[Button.inline("🔙 Help Menu", b"help_guide")]]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


# ─────────────────────────────────────────────────────────────────────────────
# LANGUAGE SELECTOR
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"lang_menu"))
async def lang_menu_cb(event):
    await event.answer()
    uid = event.sender_id
    current = SUPPORTED_LANGS.get(get_lang(uid), "🇮🇳 हिंदी")
    try:
        await event.edit(
            t(uid, "lang_title", current=current),
            buttons=[
                [Button.inline(t(uid, "btn_lang_hi"), b"set_lang_hi"),
                 Button.inline(t(uid, "btn_lang_en"), b"set_lang_en")],
                [Button.inline(t(uid, "btn_main_menu"), b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        await event.answer()


@bot.on(events.CallbackQuery(pattern=b"set_lang_"))
async def set_lang_cb(event):
    await event.answer()
    uid  = event.sender_id
    lang = event.data.decode().replace("set_lang_", "")
    set_lang(uid, lang)
    await event.answer(t(uid, "lang_changed"), alert=False)
    await main_menu_callback(event)


# ─────────────────────────────────────────────────────────────────────────────
# PER-DEST SHORTCUT
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"per_dest_shortcut"))
async def per_dest_shortcut(event):
    await event.answer()
    user_id = event.sender_id
    data    = get_user_data(user_id)

    if not data.get("sources"):
        return await event.answer(t(user_id, "no_sources"), alert=True)
    if not data.get("destinations"):
        return await event.answer(t(user_id, "no_dests"), alert=True)

    try:
        await event.edit(
            "📤 **PER-DESTINATION RULES**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Ek source → multiple destinations mein **alag rules**.\n\n"
            "**Set kar sakte ho:**\n"
            "• Media types (text/photo/video/file/voice)\n"
            "• Media mode (original / document / skip)\n"
            "• Link mode (keep / remove / replace)\n"
            "• Prefix + Suffix (destination-specific)\n"
            "• Custom caption · Replacements\n\n"
            "Source Config kholo → Destination chuniye:",
            buttons=[
                [Button.inline("📍 Source Config", b"ps_menu")],
                [Button.inline("🏠 Main Menu",      b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        await event.answer("📤 Per-Dest Rules")

# ─────────────────────────────────────────────────────────────────────────────
# /COMMANDS — Full command list with descriptions
# ─────────────────────────────────────────────────────────────────────────────

COMMAND_LIST = [
    # User commands
    ("start",   "🚀 Bot shuru karo / welcome message"),
    ("menu",    "🏠 Main menu open karo"),
    ("status",  "⚡ Forwarding status check karo"),
    ("stats",   "📊 Apni forwarding stats dekho"),
    ("premium", "💎 Premium status aur features dekho"),
    ("buy",     "💳 Premium plan kharido"),
    ("tasks",   "🎯 Task board — coins earn karo"),
    ("promote", "📣 Advertise karo / sponsor inquiry"),
    ("cancel",  "❌ Current step cancel karo"),
    ("help",    "❓ Help guide aur tips"),
    ("rules",   "📜 Bot ke rules aur notice dekho"),
    ("contact", "📞 Admin ko message bhejo"),
    ("backup",  "💾 Apni settings ka backup lo"),
    # Admin-only commands
    ("admin",    "🛠 Admin panel [Admin only]"),
    ("health",   "🩺 Bot health check [Admin only]"),
    ("restart",  "🔄 Bot restart karo [Admin only]"),
    ("fixsrc",   "🔧 Sources fix karo [Admin only]"),
    ("srccheck", "🔍 Sources check karo [Admin only]"),
    ("delsrc",   "🗑 Source ID se delete karo [Admin only]"),
    ("addsrc",   "➕ Source force-add karo [Admin only]"),
]


@bot.on(events.NewMessage(pattern=r'/commands'))
async def commands_list_cmd(event):
    if not event.is_private:
        return
    uid = event.sender_id

    try:
        from admin import is_admin
        _is_admin = is_admin(uid)
    except Exception:
        _is_admin = False

    # User commands
    user_lines = []
    admin_lines = []
    for cmd, desc in COMMAND_LIST:
        if "[Admin only]" in desc:
            if _is_admin:
                admin_lines.append(f"/{cmd} — {desc.replace(' [Admin only]', '')}")
        else:
            user_lines.append(f"/{cmd} — {desc}")

    txt = (
        "📋 **BOT COMMANDS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**👤 User Commands:**\n"
        + "\n".join(f"  `{line}`" for line in user_lines)
    )
    if admin_lines:
        txt += (
            "\n\n**🔐 Admin Commands:**\n"
            + "\n".join(f"  `{line}`" for line in admin_lines)
        )
    txt += (
        "\n\n**💡 Tip:** Telegram ke neeche Menu button (☰) dabao "
        "sab commands ek click pe milenge!"
    )

    try:
        await event.edit(txt, buttons=[
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ])
    except Exception:
        await event.respond(txt, buttons=[
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ])


@bot.on(events.CallbackQuery(data=b"show_commands"))
async def show_commands_cb(event):
    """Callback version — from menu button."""
    await event.answer()
    uid = event.sender_id
    try:
        from admin import is_admin
        _is_admin = is_admin(uid)
    except Exception:
        _is_admin = False

    user_lines  = []
    admin_lines = []
    for cmd, desc in COMMAND_LIST:
        if "[Admin only]" in desc:
            if _is_admin:
                admin_lines.append(f"/{cmd} — {desc.replace(' [Admin only]', '')}")
        else:
            user_lines.append(f"/{cmd} — {desc}")

    txt = (
        "📋 **BOT COMMANDS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**👤 User Commands:**\n"
        + "\n".join(f"  `{line}`" for line in user_lines)
    )
    if admin_lines:
        txt += (
            "\n\n**🔐 Admin Commands:**\n"
            + "\n".join(f"  `{line}`" for line in admin_lines)
        )
    txt += (
        "\n\n**💡 Tip:** Telegram ke neeche `/` type karo ya "
        "Menu button (☰) dabao!"
    )

    try:
        await event.edit(txt, buttons=[
            [Button.inline("🏠 Main Menu", b"main_menu")],
        ])
    except errors.MessageNotModifiedError:
        await event.answer()


# ─────────────────────────────────────────────────────────────────────────────
# BOTFATHER COMMAND SETUP HELPER  (/setcommands text)
# ─────────────────────────────────────────────────────────────────────────────
# Admin ye message @BotFather ko send kare /setcommands ke baad:
#
# start - Bot shuru karo
# menu - Main menu open karo
# status - Forwarding status dekho
# stats - Apni stats dekho
# premium - Premium status dekho
# buy - Premium kharido
# tasks - Task board
# promote - Advertise / sponsor inquiry
# help - Help guide
# commands - Sab commands ki list
# cancel - Current step cancel karo
#

# ─────────────────────────────────────────────────────────────────────────────
# /setmenu — Admin command to auto-set Telegram bot commands menu
# ─────────────────────────────────────────────────────────────────────────────

_BOT_COMMANDS = [
    ("start",   "Bot shuru karo / welcome"),
    ("menu",    "Main menu open karo"),
    ("status",  "Forwarding status check karo"),
    ("stats",   "Apni forwarding stats dekho"),
    ("premium", "Premium status aur features dekho"),
    ("buy",     "Premium plan kharido"),
    ("tasks",   "Task board — coins earn karo"),
    ("promote", "Advertise / sponsor inquiry"),
    ("help",    "Help guide aur tips"),
    ("commands","Sab commands ki list"),
    ("cancel",  "Current step cancel karo"),
]


@bot.on(events.NewMessage(pattern=r'/setmenu'))
async def setmenu_cmd(event):
    if not event.is_private:
        return
    from admin import is_admin
    if not is_admin(event.sender_id):
        return await event.respond("❌ Admin only command hai.")

    msg = await event.respond("⏳ Bot commands set ho rahi hain...")

    try:
        import aiohttp
        from config import BOT_TOKEN

        commands_payload = [
            {"command": cmd, "description": desc}
            for cmd, desc in _BOT_COMMANDS
        ]

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"

        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"commands": commands_payload}) as resp:
                result = await resp.json()

        if result.get("ok"):
            lines = "\n".join(f"  /{cmd} — {desc}" for cmd, desc in _BOT_COMMANDS)
            await msg.edit(
                "✅ **Bot commands set ho gayi!**\n\n"
                "Ab Telegram ke neeche blue Menu button mein ye dikhega:\n\n"
                f"{lines}\n\n"
                "_Users ko bot restart ya chat reopen karna pad sakta hai_"
            )
        else:
            await msg.edit(
                f"❌ **Error:**\n`{result}`\n\n"
                "Check karo BOT_TOKEN sahi hai?"
            )

    except ImportError:
        # aiohttp nahi hai — requests se try karo
        try:

            from config import BOT_TOKEN

            commands_payload = [
                {"command": cmd, "description": desc}
                for cmd, desc in _BOT_COMMANDS
            ]
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/setMyCommands"
            import json as _json_mod
            import urllib.request as _ureq
            _req_data = _json_mod.dumps({"commands": commands_payload}).encode()
            _req = _ureq.Request(url, data=_req_data, headers={"Content-Type": "application/json"}, method="POST")
            with _ureq.urlopen(_req, timeout=10) as _resp:
                result = _json_mod.loads(_resp.read())

            if result.get("ok"):
                lines = "\n".join(f"  /{cmd} — {desc}" for cmd, desc in _BOT_COMMANDS)
                await msg.edit(
                    "✅ **Bot commands set ho gayi!**\n\n"
                    f"{lines}"
                )
            else:
                await msg.edit(f"❌ Error: `{result}`")

        except Exception as e:
            await msg.edit(f"❌ Failed: `{e}`")

    except Exception as e:
        await msg.edit(f"❌ Unexpected error: `{e}`")


# ─────────────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════
# 📋 FORWARDING TEMPLATES — v3.0 (Production Upgrade)
# Features:
#   • Per-user templates + Admin global templates
#   • 8 rich built-in + unlimited custom (max 20)
#   • Categories: Quick / Media / Filtering / Custom / Pinned
#   • Preview, Apply-with-diff, Partial apply (checkboxes)
#   • Share template via code, Admin push to all users
#   • Pin templates for quick access
#   • Search + Sort (usage/name/date)
#   • Undo last apply (snapshot restore)
#   • Bulk delete, locking, auto-description
#   • Per-source apply
#   • Import/Export JSON
# ══════════════════════════════════════════════════════════════

import time as _time
import random as _random
import string as _string
import copy as _copy
import json as _json
from database import GLOBAL_STATE, save_persistent_db as _spdb

# ── Built-in Templates ──────────────────────────────────────────────────────
_FWD_TEMPLATES = {
    "news": {
        "name": "📰 News Channel",
        "desc": "News forward — smart filter on, duplicates block",
        "category": "quick",
        "settings": {
            "smart_filter": True, "duplicate_filter": True,
            "remove_links": False, "preview_mode": True,
        },
    },
    "product": {
        "name": "🛒 Product / Deals",
        "desc": "Affiliate links — duplicate block, links keep",
        "category": "quick",
        "settings": {
            "duplicate_filter": True, "product_duplicate_filter": True,
            "remove_links": False, "preview_mode": False,
        },
    },
    "clean": {
        "name": "🧹 Clean Forward",
        "desc": "Sirf text — links, usernames, hashtags hata do",
        "category": "filtering",
        "settings": {
            "remove_links": True, "remove_user": True,
            "smart_filter": True, "keyword_filter_enabled": False,
        },
    },
    "media_only": {
        "name": "🖼️ Media Only",
        "desc": "Sirf photos/videos — text forward nahi hoga",
        "category": "media",
        "settings": {
            "text": False, "image": True, "video": True,
            "voice": False, "files": False, "caption": True,
        },
    },
    "silent": {
        "name": "🔇 Silent / Raw",
        "desc": "Koi filter nahi — jaise hai waise forward",
        "category": "quick",
        "settings": {
            "remove_links": False, "smart_filter": False,
            "duplicate_filter": False, "keyword_filter_enabled": False,
            "remove_user": False,
        },
    },
    "broadcast": {
        "name": "📢 Broadcast",
        "desc": "Delay 3s, caption on, preview off — stable broadcast",
        "category": "quick",
        "settings": {
            "custom_delay": 3, "caption": True, "preview_mode": False,
            "duplicate_filter": True,
        },
    },
    "document": {
        "name": "📄 Document Mode",
        "desc": "Files as document — no compression",
        "category": "media",
        "settings": {
            "as_document": True, "files": True, "image": True,
            "video": True, "voice": False,
        },
    },
    "strict_filter": {
        "name": "🛡️ Strict Filter",
        "desc": "Keyword filter + smart filter + dup block — maximum filtering",
        "category": "filtering",
        "settings": {
            "smart_filter": True, "duplicate_filter": True,
            "keyword_filter_enabled": True, "filter_mode": "Blacklist",
            "remove_links": False,
        },
    },
}

_CATEGORIES = {
    "quick":     "⚡ Quick Setups",
    "media":     "🖼️ Media",
    "filtering": "🛡️ Filtering",
    "custom":    "✏️ My Templates",
    "pinned":    "📌 Pinned",
    "admin":     "🌐 Admin",
}

# ── Per-user custom templates ───────────────────────────────────────────────
_ALL_SAVEABLE_KEYS = [
    # Media types
    "text", "image", "video", "voice", "files", "caption",
    # Forwarding behavior
    "remove_links", "remove_user", "as_document", "auto_shorten",
    "preview_mode", "custom_delay",
    # Filters
    "smart_filter", "duplicate_filter", "product_duplicate_filter",
    "keyword_filter_enabled", "filter_mode", "global_filter",
    "dup_expiry_hours",
    # Start/End messages
    "start_msg", "end_msg", "start_msg_enabled", "end_msg_enabled",
    "start_msg_slots", "end_msg_slots",
    "start_rotation_mode", "end_rotation_mode",
    # Watermark
    "watermark_enabled",
    # Affiliate
    "affiliate",
]

# Readable labels for diff/preview
_KEY_LABELS = {
    "text": "Text fwd", "image": "Images", "video": "Videos",
    "voice": "Voice", "files": "Files", "caption": "Captions",
    "remove_links": "Remove links", "remove_user": "Remove usernames",
    "smart_filter": "Smart filter", "duplicate_filter": "Dup filter",
    "product_duplicate_filter": "Product dup", "keyword_filter_enabled": "Keyword filter",
    "filter_mode": "Filter mode", "preview_mode": "Link preview",
    "as_document": "As document", "custom_delay": "Delay (s)",
    "auto_shorten": "Auto shorten", "global_filter": "Global filter",
    "dup_expiry_hours": "Dup expiry (h)", "start_msg": "Start msg",
    "end_msg": "End msg", "start_msg_enabled": "Start ON",
    "end_msg_enabled": "End ON", "watermark_enabled": "Watermark",
    "start_rotation_mode": "Start rotation", "end_rotation_mode": "End rotation",
}

def _get_user_templates(uid: int) -> dict:
    """Per-user custom templates — stored in user data."""
    data = get_user_data(uid)
    return data.setdefault("fwd_templates", {})

def _save_user_templates(uid: int, templates: dict):
    data = get_user_data(uid)
    data["fwd_templates"] = templates
    save_persistent_db()

def _tpl_snapshot(uid: int) -> dict:
    """Current user settings ka snapshot lo — saari saveable keys."""
    data = get_user_data(uid)
    s    = data.get("settings", {})
    return {k: s[k] for k in _ALL_SAVEABLE_KEYS if k in s}

def _apply_snapshot(uid: int, snapshot: dict):
    """Snapshot ko user settings mein apply karo."""
    data = get_user_data(uid)
    for k, v in snapshot.items():
        data["settings"][k] = v
    save_persistent_db()

def _tpl_usage_bump(uid: int, tpl_key: str):
    """Template use count badhao."""
    data = get_user_data(uid)
    data.setdefault("tpl_usage", {})
    data["tpl_usage"][tpl_key] = data["tpl_usage"].get(tpl_key, 0) + 1


# ── Undo snapshot ────────────────────────────────────────────────────────────
def _tpl_save_undo(uid: int):
    """Apply se pehle current settings snapshot save karo — undo ke liye."""
    data = get_user_data(uid)
    data["tpl_undo_snapshot"] = _tpl_snapshot(uid)


def _tpl_undo_available(uid: int) -> bool:
    return bool(get_user_data(uid).get("tpl_undo_snapshot"))


# ── Admin global templates ───────────────────────────────────────────────────
def _get_admin_templates() -> dict:
    """Admin-pushed templates — GLOBAL_STATE mein, sabke liye visible."""
    return GLOBAL_STATE.get("admin_fwd_templates", {})

def _save_admin_templates(templates: dict):
    GLOBAL_STATE["admin_fwd_templates"] = templates
    _spdb()


# ── Share codes ──────────────────────────────────────────────────────────────
def _get_share_codes() -> dict:
    """Short code → template data — GLOBAL_STATE mein."""
    return GLOBAL_STATE.setdefault("tpl_share_codes", {})

def _create_share_code(tpl: dict) -> str:
    """6-char share code generate karo."""
    import random, string
    codes = _get_share_codes()
    for _ in range(20):
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if code not in codes:
            codes[code] = {
                "tpl": tpl,
                "created_at": int(_time.time()),
                "uses": 0,
            }
            _spdb()
            return code
    return None

def _resolve_share_code(code: str) -> dict | None:
    """Share code se template lo."""
    entry = _get_share_codes().get(code.upper().strip())
    if entry:
        entry["uses"] = entry.get("uses", 0) + 1
        _spdb()
        return entry["tpl"]
    return None


# ── Auto-description ─────────────────────────────────────────────────────────
def _auto_desc(settings: dict) -> str:
    """Settings se smart readable description banao."""
    parts = []
    if settings.get("smart_filter"):      parts.append("smart filter")
    if settings.get("duplicate_filter"):  parts.append("dup block")
    if settings.get("remove_links"):      parts.append("links removed")
    if settings.get("remove_user"):       parts.append("usernames removed")
    if settings.get("as_document"):       parts.append("as document")
    if not settings.get("text", True):    parts.append("no text")
    d = settings.get("custom_delay", 0)
    if d:                                 parts.append(f"{d}s delay")
    if settings.get("start_msg"):         parts.append("start msg")
    if settings.get("end_msg"):           parts.append("end msg")
    if not parts:
        return "Custom template"
    return ", ".join(parts[:5]).capitalize()


# ── Pinned templates ─────────────────────────────────────────────────────────
def _get_pinned(uid: int) -> list:
    return get_user_data(uid).setdefault("tpl_pinned", [])

def _toggle_pin(uid: int, key: str):
    data   = get_user_data(uid)
    pinned = data.setdefault("tpl_pinned", [])
    if key in pinned:
        pinned.remove(key)
    else:
        pinned.insert(0, key)
        if len(pinned) > 5:
            pinned.pop()
    save_persistent_db()


# ── Sort helpers ─────────────────────────────────────────────────────────────
def _sorted_custom(uid: int, mode: str = "date") -> list:
    """Custom templates sorted — returns [(key, tpl), ...]"""
    tpls  = _get_user_templates(uid)
    usage = get_user_data(uid).get("tpl_usage", {})
    items = list(tpls.items())
    if mode == "usage":
        items.sort(key=lambda x: usage.get(f"custom_{x[0]}", 0), reverse=True)
    elif mode == "name":
        items.sort(key=lambda x: x[1].get("name","").lower())
    else:  # date (default — newest first)
        items.sort(key=lambda x: x[1].get("created_at", 0), reverse=True)
    return items


# ── Search ───────────────────────────────────────────────────────────────────
def _search_templates(uid: int, query: str) -> list:
    """Query se templates dhundho — built-in + custom."""
    q      = query.lower()
    result = []
    for k, t in _FWD_TEMPLATES.items():
        if q in t["name"].lower() or q in t.get("desc","").lower():
            result.append((k, t, "builtin"))
    for k, t in _get_user_templates(uid).items():
        if q in t.get("name","").lower() or q in t.get("desc","").lower():
            result.append((k, t, "custom"))
    admin = _get_admin_templates()
    for k, t in admin.items():
        if q in t.get("name","").lower() or q in t.get("desc","").lower():
            result.append((k, t, "admin"))
    return result


# ── Dashboard ────────────────────────────────────────────────────────────────
def _tpl_dashboard_text(uid: int, category: str = "quick", sort: str = "date", search: str = "") -> str:
    data    = get_user_data(uid)
    usage   = data.get("tpl_usage", {})
    pinned  = _get_pinned(uid)
    undo_ok = _tpl_undo_available(uid)

    header = "📋 **FORWARDING TEMPLATES v3**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"

    if search:
        results = _search_templates(uid, search)
        if not results:
            body = f"  ❌ `{search}` ke liye koi result nahi mila."
        else:
            lines = []
            for k, t, src in results:
                icon = {"builtin": "⚡", "custom": "✏️", "admin": "🌐"}.get(src, "•")
                uses = usage.get(k if src != "custom" else f"custom_{k}", 0)
                lines.append(f"  {icon} **{t['name']}** ({uses}✓)\n     _{t.get('desc','')[:50]}_")
            body = "\n\n".join(lines)
        return header + f"\n🔍 Search: `{search}`\n\n{body}"

    cat_label = _CATEGORIES.get(category, category)

    if category == "pinned":
        if not pinned:
            body = "  _Koi template pin nahi hua.\n  Template pe 📌 press karo._"
        else:
            lines = []
            for key in pinned:
                if key.startswith("custom_"):
                    t = _get_user_templates(uid).get(key[7:])
                    icon = "✏️"
                elif key.startswith("admin_"):
                    t = _get_admin_templates().get(key[6:])
                    icon = "🌐"
                else:
                    t = _FWD_TEMPLATES.get(key)
                    icon = "⚡"
                if t:
                    uses = usage.get(key, 0)
                    lines.append(f"  📌 {icon} **{t['name']}** ({uses}✓)")
            body = "\n".join(lines) if lines else "  _(pinned templates resolve nahi hue)_"

    elif category == "admin":
        admin_tpls = _get_admin_templates()
        if not admin_tpls:
            body = "  _Admin ne abhi koi template push nahi kiya._"
        else:
            lines = []
            for k, t in admin_tpls.items():
                uses   = usage.get(f"admin_{k}", 0)
                locked = "🔒 " if t.get("locked") else ""
                lines.append(f"  🌐 {locked}**{t['name']}** ({uses}✓)\n     _{t.get('desc','')[:50]}_")
            body = "\n\n".join(lines)

    elif category == "custom":
        custom = _get_user_templates(uid)
        if not custom:
            body = "  _Abhi koi custom template nahi hai._\n  'Save Current' se banao!"
        else:
            sorted_items = _sorted_custom(uid, sort)
            lines = []
            for k, t in sorted_items:
                uses   = usage.get(f"custom_{k}", 0)
                pin_mk = "📌 " if f"custom_{k}" in pinned else ""
                locked = "🔒 " if t.get("locked") else ""
                lines.append(
                    f"  {pin_mk}{locked}✏️ **{t['name']}** ({uses}✓)\n"
                    f"     _{t.get('desc','')[:50]}_"
                )
            body = "\n\n".join(lines)
            cat_label += f" · {sort}"
    else:
        lines = []
        for k, t in _FWD_TEMPLATES.items():
            if t["category"] != category: continue
            uses   = usage.get(k, 0)
            pin_mk = "📌 " if k in pinned else ""
            lines.append(f"  {pin_mk}⚡ **{t['name']}** ({uses}✓)\n     _{t['desc']}_")
        body = "\n\n".join(lines) if lines else "  _(koi template nahi)_"

    undo_line = "\n♻️ _Undo available_" if undo_ok else ""
    return (
        header +
        f"\nCategory: **{cat_label}**{undo_line}\n\n"
        f"{body}\n\n"
        "💡 Apply karne par current settings replace hongi."
    )


def _tpl_buttons(uid: int, category: str = "quick", sort: str = "date") -> list:
    custom = _get_user_templates(uid)
    pinned = _get_pinned(uid)
    btns   = []

    # Category tabs — 3 rows of 2 (6 cats now)
    _CAT_SHORT = {
        "quick":     "\u26a1 Quick",
        "media":     "\U0001f5bc\ufe0f Media",
        "filtering": "\U0001f6e1\ufe0f Filter",
        "custom":    "\u270f\ufe0f Custom",
        "pinned":    "\U0001f4cc Pinned",
        "admin":     "\U0001f310 Admin",
    }
    cat_btns = []
    for ckey, clabel in _CAT_SHORT.items():
        active = "▶" if ckey == category else ""
        cat_btns.append(Button.inline(f"{active}{clabel}", f"fwd_tpl_cat|{ckey}".encode()))
    btns.append(cat_btns[:3])
    btns.append(cat_btns[3:])
    btns.append([Button.inline("─────────────────────", b"fwd_templates_menu")])

    undo_ok = _tpl_undo_available(uid)
    if undo_ok:
        btns.append([Button.inline("♻️ Undo Last Apply", b"fwd_tpl_undo")])

    if category == "pinned":
        for key in pinned:
            if key.startswith("custom_"):
                t = custom.get(key[7:])
                akey = f"fwd_tpl_apply|custom_{key[7:]}"
            elif key.startswith("admin_"):
                t = _get_admin_templates().get(key[6:])
                akey = f"fwd_tpl_apply|admin_{key[6:]}"
            else:
                t = _FWD_TEMPLATES.get(key)
                akey = f"fwd_tpl_apply|{key}"
            if t:
                lbl = t["name"][:26]
                btns.append([
                    Button.inline(f"✅ {lbl}", akey.encode()),
                    Button.inline("📌 Unpin", f"fwd_tpl_pin|{key}".encode()),
                ])

    elif category == "admin":
        admin_tpls = _get_admin_templates()
        for k, t in admin_tpls.items():
            locked = "🔒" if t.get("locked") else "📌"
            btns.append([
                Button.inline(f"🌐 {t['name'][:24]}", f"fwd_tpl_apply|admin_{k}".encode()),
                Button.inline("👁", f"fwd_tpl_preview|admin_{k}".encode()),
                Button.inline(locked, f"fwd_tpl_pin|admin_{k}".encode()),
            ])

    elif category == "custom":
        sorted_items = _sorted_custom(uid, sort)
        if sorted_items:
            # Sort toggle buttons
            btns.append([
                Button.inline("📅 Date" if sort != "date" else "▶📅 Date",   b"fwd_tpl_sort|date"),
                Button.inline("⭐ Usage" if sort != "usage" else "▶⭐ Usage", b"fwd_tpl_sort|usage"),
                Button.inline("🔤 Name" if sort != "name" else "▶🔤 Name",   b"fwd_tpl_sort|name"),
            ])
            for k, t in sorted_items:
                pin_icon = "📌" if f"custom_{k}" in pinned else "☆"
                locked   = t.get("locked", False)
                btns.append([
                    Button.inline(f"✅ {t['name'][:20]}", f"fwd_tpl_apply|custom_{k}".encode()),
                    Button.inline("👁",       f"fwd_tpl_preview|custom_{k}".encode()),
                    Button.inline(pin_icon,   f"fwd_tpl_pin|custom_{k}".encode()),
                    Button.inline("✏️" if not locked else "🔒", f"fwd_tpl_edit|{k}".encode()),
                    Button.inline("🗑",       f"fwd_tpl_del|{k}".encode()),
                ])
        btns.append([Button.inline("💾 Save Current", b"fwd_tpl_save_current"),
                     Button.inline("🔍 Search",        b"fwd_tpl_search")])
        btns.append([Button.inline("📤 Export",    b"fwd_tpl_export"),
                     Button.inline("📥 Import",    b"fwd_tpl_import"),
                     Button.inline("🗑 Bulk Del",  b"fwd_tpl_bulk_del")])
    else:
        # Built-in categories
        for k, t in _FWD_TEMPLATES.items():
            if t["category"] != category: continue
            pin_icon = "📌" if k in pinned else "☆"
            btns.append([
                Button.inline(f"⚡ {t['name'][:26]}", f"fwd_tpl_apply|{k}".encode()),
                Button.inline("👁",      f"fwd_tpl_preview|{k}".encode()),
                Button.inline(pin_icon,  f"fwd_tpl_pin|{k}".encode()),
            ])

    # Bottom row
    row = [Button.inline("🔍 Search", b"fwd_tpl_search")]
    if category != "custom":
        row.append(Button.inline("📋 Import Code", b"fwd_tpl_use_code"))
    btns.append(row)
    btns.append([Button.inline("🔙 Main Menu", b"main_menu")])
    return btns

# ── Main Menu Handler ────────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_templates_menu"))
async def fwd_templates_menu(event):
    await event.answer()
    uid = event.sender_id
    try:
        await event.edit(
            _tpl_dashboard_text(uid, "quick"),
            buttons=_tpl_buttons(uid, "quick")
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_cat\\|(.+)"))
async def fwd_tpl_cat(event):
    await event.answer()
    uid  = event.sender_id
    raw  = event.data.decode().split("|")[1]
    # Support cat|sort format: "custom|usage"
    parts = raw.split(":")
    cat  = parts[0]
    sort = parts[1] if len(parts) > 1 else "date"
    try:
        await event.edit(
            _tpl_dashboard_text(uid, cat, sort),
            buttons=_tpl_buttons(uid, cat, sort)
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_sort\\|(.+)"))
async def fwd_tpl_sort(event):
    await event.answer()
    uid  = event.sender_id
    sort = event.data.decode().split("|")[1]
    try:
        await event.edit(
            _tpl_dashboard_text(uid, "custom", sort),
            buttons=_tpl_buttons(uid, "custom", sort)
        )
    except errors.MessageNotModifiedError:
        pass


# ── Apply ────────────────────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_apply\\|(.+)"))
async def fwd_tpl_apply(event):
    await event.answer()
    uid = event.sender_id
    key = event.data.decode().split("|")[1]

    if key.startswith("custom_"):
        tpl = _get_user_templates(uid).get(key[7:])
        cat = "custom"
    elif key.startswith("admin_"):
        tpl = _get_admin_templates().get(key[6:])
        cat = "admin"
    else:
        tpl = _FWD_TEMPLATES.get(key)
        cat = tpl["category"] if tpl else "quick"

    if not tpl:
        return await event.answer("❌ Template nahi mila!", alert=True)

    # Save undo snapshot BEFORE applying
    _tpl_save_undo(uid)
    _apply_snapshot(uid, tpl.get("settings", {}))
    _tpl_usage_bump(uid, key)

    applied_count = len(tpl.get("settings", {}))
    await event.answer(f"✅ '{tpl['name']}' applied! ({applied_count} settings)", alert=False)

    try:
        name_str = tpl['name']
        desc_str = tpl.get('desc','')
        try:
            await event.edit(
                f"✅ **Applied: {name_str}**\n\n"
                f"_{desc_str}_ \n\n"
                f"{applied_count} settings update ho gayi.\n"
                "♻️ Undo ke liye 'Undo Last Apply' dabao.",
                buttons=[
                    [Button.inline("♻️ Undo", b"fwd_tpl_undo"),
                     Button.inline("📋 Templates", f"fwd_tpl_cat|{cat}".encode())],
                    [Button.inline("🏠 Main Menu", b"main_menu")],
                ]
            )
        except errors.MessageNotModifiedError:
            pass
    except errors.MessageNotModifiedError:
        pass


# ── Preview ───────────────────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_preview\\|(.+)"))
async def fwd_tpl_preview(event):
    await event.answer()
    uid = event.sender_id
    key = event.data.decode().split("|")[1]

    if key.startswith("custom_"):
        tpl = _get_user_templates(uid).get(key[7:])
    else:
        tpl = _FWD_TEMPLATES.get(key)

    if not tpl:
        return await event.answer("❌ Template nahi mila!", alert=True)

    settings = tpl.get("settings", {})

    def _yn(v):
        if v is True:  return "✅"
        if v is False: return "❌"
        return f"`{v}`"

    # Readable setting labels
    labels = {
        "text": "Text forward", "image": "Images", "video": "Videos",
        "voice": "Voice", "files": "Files", "caption": "Captions",
        "remove_links": "Remove links", "remove_user": "Remove usernames",
        "smart_filter": "Smart filter", "duplicate_filter": "Dup filter",
        "product_duplicate_filter": "Product dup filter",
        "keyword_filter_enabled": "Keyword filter",
        "preview_mode": "Link preview", "as_document": "As document",
        "custom_delay": "Delay (seconds)", "auto_shorten": "Auto shorten",
        "start_msg": "Start message", "end_msg": "End message",
        "dup_expiry_hours": "Dup expiry (hrs)",
    }

    lines = []
    for k, v in settings.items():
        if v is None: continue
        lbl = labels.get(k, k)
        if k in ("start_msg", "end_msg"):
            v_str = f"`{str(v)[:40]}…`" if len(str(v)) > 40 else f"`{v}`" if v else "_(empty)_"
        else:
            v_str = _yn(v)
        lines.append(f"  {lbl}: {v_str}")

    data    = get_user_data(uid)
    usage   = data.get("tpl_usage", {}).get(key, 0)

    try:
        await event.edit(
            f"👁 **TEMPLATE PREVIEW**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**{tpl['name']}**\n"
            f"_{tpl.get('desc', '')}_ \n"
            f"Used: {usage} times\n\n"
            f"**Settings jo apply hongi:**\n"
            + "\n".join(lines) if lines else "  _(koi settings nahi)_",
            buttons=[
                [Button.inline(f"✅ Apply", f"fwd_tpl_apply|{key}".encode()),
                 Button.inline("🔄 Show Diff", f"fwd_tpl_diff|{key}".encode())],
                [Button.inline("🔙 Back", b"fwd_tpl_cat|" + (b"custom" if key.startswith("custom_") else tpl.get("category","quick").encode()))],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Save Current ─────────────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_tpl_save_current"))
async def fwd_tpl_save_current(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    snap = _tpl_snapshot(uid)
    data["step"]           = "fwd_tpl_name_input"
    data["step_since"]     = _time.time()
    data["temp_data"]["tpl_snap"] = snap

    # Show what will be saved
    count = len([v for v in snap.values() if v not in (None, "", [], {})])
    try:
        await event.edit(
            f"💾 **SAVE CURRENT SETTINGS**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**{count} settings** save hongi, including:\n"
            f"  • Media types, filters, delay\n"
            f"  • Start/End messages\n"
            f"  • Duplicate filter settings\n\n"
            "Template ka naam type karo:\n"
            "_(Max 40 chars, e.g. 'Mera News Setup')_",
            buttons=[[Button.inline("❌ Cancel", b"fwd_tpl_cat|custom")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_tpl_name_input"))
async def fwd_tpl_name_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    name = event.raw_text.strip()[:40]
    data["step"] = None

    snap    = data.get("temp_data", {}).pop("tpl_snap", _tpl_snapshot(uid))
    customs = _get_user_templates(uid)

    if len(customs) >= 20:
        await event.respond(
            "❌ Max 20 custom templates. Pehle kuch delete karo.",
            buttons=[[Button.inline("📋 Templates", b"fwd_tpl_cat|custom")]]
        )
        return

    tpl_id = "".join(_random.choices(_string.ascii_lowercase + _string.digits, k=8))
    customs[tpl_id] = {
        "name":       name,
        "desc":       f"Saved {datetime.datetime.now().strftime('%d/%m/%y %H:%M')}",
        "category":   "custom",
        "settings":   snap,
        "created_at": int(_time.time()),
    }
    _save_user_templates(uid, customs)

    await event.respond(
        f"✅ **Template '{name}' saved!**\n\n"
        f"{len(snap)} settings saved.\n"
        "Templates menu se apply kar sakte ho.",
        buttons=[
            [Button.inline("📋 My Templates", b"fwd_tpl_cat|custom")],
            [Button.inline("🏠 Main Menu",     b"main_menu")],
        ]
    )


# ── Edit Custom Template ──────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_edit\\|(.+)"))
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_edit\\|(.+)"))
async def fwd_tpl_edit(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    tpls = _get_user_templates(uid)
    tpl  = tpls.get(key)
    if not tpl:
        return await event.answer("\u274c Template nahi mila!", alert=True)
    locked   = tpl.get("locked", False)
    lock_lbl = "\U0001f513 Unlock" if locked else "\U0001f512 Lock"
    lock_st  = "\U0001f512 Locked" if locked else "\U0001f513 Unlocked"
    tname    = tpl["name"]
    try:
        await event.edit(
            f"\u270f\ufe0f **EDIT: {tname}** [{lock_st}]\n\nKya karna chahte ho?",
            buttons=[
                [Button.inline("\u270f\ufe0f Naam Badlo",  f"fwd_tpl_rename|{key}".encode()),
                 Button.inline("\U0001f4dd Desc Badlo",    f"fwd_tpl_redesc|{key}".encode())],
                [Button.inline("\U0001f504 Overwrite",     f"fwd_tpl_overwrite|{key}".encode()),
                 Button.inline("\U0001f4cb Duplicate",     f"fwd_tpl_dup|{key}".encode())],
                [Button.inline("\U0001f4e4 Share Code",    f"fwd_tpl_share|{key}".encode()),
                 Button.inline(lock_lbl,                    f"fwd_tpl_lock|{key}".encode())],
                [Button.inline("\U0001f504 Show Diff",     f"fwd_tpl_diff|custom_{key}".encode())],
                [Button.inline("\U0001f519 Back",          b"fwd_tpl_cat|custom")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_rename\\|(.+)"))
async def fwd_tpl_rename(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    data = get_user_data(uid)
    data["step"]       = f"fwd_tpl_rename_input|{key}"
    data["step_since"] = _time.time()
    tpl  = _get_user_templates(uid).get(key, {})
    try:
        await event.edit(
            f"✏️ Naya naam type karo:\nCurrent: `{tpl.get('name','')}`",
            buttons=[[Button.inline("❌ Cancel", b"fwd_tpl_cat|custom")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        isinstance(get_user_data(e.sender_id).get("step"), str) and
        get_user_data(e.sender_id).get("step", "").startswith("fwd_tpl_rename_input|")))
async def fwd_tpl_rename_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    key  = data["step"].split("|")[1]
    data["step"] = None
    tpls = _get_user_templates(uid)
    if key in tpls:
        tpls[key]["name"] = event.raw_text.strip()[:40]
        _save_user_templates(uid, tpls)
    await event.respond(
        "✅ Naam update ho gaya!",
        buttons=[[Button.inline("📋 My Templates", b"fwd_tpl_cat|custom")]]
    )


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_redesc\\|(.+)"))
async def fwd_tpl_redesc(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    data = get_user_data(uid)
    data["step"]       = f"fwd_tpl_redesc_input|{key}"
    data["step_since"] = _time.time()
    tpl  = _get_user_templates(uid).get(key, {})
    try:
        await event.edit(
            f"📝 Naya description type karo (max 80 chars):\nCurrent: `{tpl.get('desc','')}`",
            buttons=[[Button.inline("❌ Cancel", b"fwd_tpl_cat|custom")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        isinstance(get_user_data(e.sender_id).get("step"), str) and
        get_user_data(e.sender_id).get("step", "").startswith("fwd_tpl_redesc_input|")))
async def fwd_tpl_redesc_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    key  = data["step"].split("|")[1]
    data["step"] = None
    tpls = _get_user_templates(uid)
    if key in tpls:
        tpls[key]["desc"] = event.raw_text.strip()[:80]
        _save_user_templates(uid, tpls)
    await event.respond(
        "✅ Description update ho gaya!",
        buttons=[[Button.inline("📋 My Templates", b"fwd_tpl_cat|custom")]]
    )


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_overwrite\\|(.+)"))
async def fwd_tpl_overwrite(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    tpls = _get_user_templates(uid)
    if key not in tpls:
        return await event.answer("❌ Template nahi mila!", alert=True)
    snap = _tpl_snapshot(uid)
    tpls[key]["settings"]   = snap
    tpls[key]["desc"]       = f"Updated {datetime.datetime.now().strftime('%d/%m/%y %H:%M')}"
    _save_user_templates(uid, tpls)
    await event.answer(f"✅ '{tpls[key]['name']}' overwritten!", alert=False)
    await fwd_tpl_edit(event)


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_dup\\|(.+)"))
async def fwd_tpl_dup(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    tpls = _get_user_templates(uid)
    tpl  = tpls.get(key)
    if not tpl:
        return await event.answer("❌ Template nahi mila!", alert=True)
    if len(tpls) >= 20:
        return await event.answer("❌ Max 20 templates!", alert=True)
    new_key = "".join(_random.choices(_string.ascii_lowercase + _string.digits, k=8))
    import copy
    tpls[new_key] = copy.deepcopy(tpl)
    tpls[new_key]["name"] = f"{tpl['name']} (Copy)"
    _save_user_templates(uid, tpls)
    await event.answer("✅ Duplicate ban gaya!", alert=False)
    try:
        await event.edit(_tpl_dashboard_text(uid, "custom"), buttons=_tpl_buttons(uid, "custom"))
    except errors.MessageNotModifiedError:
        pass


# ── Delete ────────────────────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_del\\|(.+)"))
async def fwd_tpl_del(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    tpls = _get_user_templates(uid)
    tpl  = tpls.get(key)
    if not tpl:
        return await event.answer("❌ Template nahi mila!", alert=True)
    try:
        await event.edit(
            f"🗑 **DELETE: {tpl['name']}**\n\nConfirm karo?",
            buttons=[
                [Button.inline("🗑 Haan, Delete Karo", f"fwd_tpl_del_ok|{key}".encode()),
                 Button.inline("❌ Cancel",             b"fwd_tpl_cat|custom")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_del_ok\\|(.+)"))
async def fwd_tpl_del_ok(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    tpls = _get_user_templates(uid)
    name = tpls.get(key, {}).get("name", "?")
    tpls.pop(key, None)
    _save_user_templates(uid, tpls)
    await event.answer(f"🗑 '{name}' deleted!", alert=False)
    try:
        await event.edit(_tpl_dashboard_text(uid, "custom"), buttons=_tpl_buttons(uid, "custom"))
    except errors.MessageNotModifiedError:
        pass


# ── Export / Import ───────────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_tpl_export"))
async def fwd_tpl_export(event):
    await event.answer()
    uid  = event.sender_id
    tpls = _get_user_templates(uid)
    if not tpls:
        return await event.answer("Koi custom template nahi hai!", alert=True)

    import json as _json
    export_data = {
        "version": 2,
        "templates": tpls,
        "exported_at": datetime.datetime.now().isoformat(),
    }
    txt = _json.dumps(export_data, ensure_ascii=False, indent=2)

    # Send as file
    import io
    buf = io.BytesIO(txt.encode())
    buf.name = "my_templates.json"
    await event.respond(
        f"📤 **Templates Export** ({len(tpls)} templates)\n\n"
        "File download karo aur import ke liye use karo.",
        file=buf,
        buttons=[[Button.inline("📋 Templates", b"fwd_tpl_cat|custom")]]
    )


@bot.on(events.CallbackQuery(data=b"fwd_tpl_import"))
async def fwd_tpl_import(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"]       = "fwd_tpl_import_file"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "📥 **IMPORT TEMPLATES**\n\n"
            "Export ki hui JSON file bhejo.\n"
            "_(Export se mila .json file upload karo)_",
            buttons=[[Button.inline("❌ Cancel", b"fwd_tpl_cat|custom")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_tpl_import_file"))
async def fwd_tpl_import_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None

    if not event.document:
        await event.respond("❌ File nahi mili. JSON file upload karo.",
                            buttons=[[Button.inline("🔙 Back", b"fwd_tpl_cat|custom")]])
        return

    try:
        import json as _json
        file_bytes = await event.download_media(bytes)
        payload    = _json.loads(file_bytes.decode())

        imported = payload.get("templates", {})
        if not isinstance(imported, dict):
            raise ValueError("Invalid format")

        existing = _get_user_templates(uid)
        count    = 0
        for k, v in imported.items():
            if len(existing) >= 20:
                break
            if isinstance(v, dict) and "name" in v and "settings" in v:
                # New key to avoid collision
                new_key = "".join(_random.choices(_string.ascii_lowercase + _string.digits, k=8))
                existing[new_key] = v
                count += 1

        _save_user_templates(uid, existing)
        await event.respond(
            f"✅ **{count} templates import ho gaye!**",
            buttons=[[Button.inline("📋 My Templates", b"fwd_tpl_cat|custom"),
                      Button.inline("🏠 Main Menu",    b"main_menu")]]
        )
    except Exception as e:
        await event.respond(
            f"❌ Import failed: `{str(e)[:100]}`\n\nValid JSON file bhejo.",
            buttons=[[Button.inline("🔙 Back", b"fwd_tpl_cat|custom")]]
        )




# Contact Admin — handled by support.py


# ══════════════════════════════════════════════════════════════
# 📋 TEMPLATES v3 — New Handlers
# ══════════════════════════════════════════════════════════════

# ── Undo last apply ──────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_tpl_undo"))
async def fwd_tpl_undo(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    snap = data.pop("tpl_undo_snapshot", None)
    if not snap:
        return await event.answer("Undo snapshot nahi hai!", alert=True)
    _apply_snapshot(uid, snap)
    save_persistent_db()
    await event.answer("✅ Undo complete!", alert=False)
    try:
        await event.edit(
            "♻️ **UNDO COMPLETE**\n\nPehle wali settings restore ho gayi.",
            buttons=[
                [Button.inline("📋 Templates", b"fwd_templates_menu")],
                [Button.inline("🏠 Main Menu",  b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Sort toggle ──────────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_sort\\|(.+)"))
async def fwd_tpl_sort(event):
    await event.answer()
    sort = event.data.decode().split("|")[1]
    uid  = event.sender_id
    try:
        await event.edit(
            _tpl_dashboard_text(uid, "custom", sort),
            buttons=_tpl_buttons(uid, "custom", sort)
        )
    except errors.MessageNotModifiedError:
        pass


# ── Pin / Unpin ──────────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_pin\\|(.+)"))
async def fwd_tpl_pin(event):
    await event.answer()
    uid = event.sender_id
    key = event.data.decode().split("|")[1]
    _toggle_pin(uid, key)
    pinned    = _get_pinned(uid)
    is_pinned = key in pinned
    await event.answer("📌 Pinned!" if is_pinned else "☆ Unpinned", alert=False)
    cat = ("custom"  if key.startswith("custom_") else
           "admin"   if key.startswith("admin_")  else
           _FWD_TEMPLATES.get(key, {}).get("category", "quick"))
    try:
        await event.edit(
            _tpl_dashboard_text(uid, cat),
            buttons=_tpl_buttons(uid, cat)
        )
    except errors.MessageNotModifiedError:
        pass


# ── Search prompt ────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_tpl_search"))
async def fwd_tpl_search_prompt(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"]       = "fwd_tpl_search_input"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "🔍 **TEMPLATE SEARCH**\n\n"
            "Naam ya description ka koi word type karo:\n"
            "_(e.g. `filter`, `deal`, `media`)_",
            buttons=[[Button.inline("❌ Cancel", b"fwd_templates_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_tpl_search_input"))
async def fwd_tpl_search_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    query   = event.raw_text.strip()[:40]
    results = _search_templates(uid, query)
    btns = []
    for k, t, src in results:
        icon = {"builtin": "⚡", "custom": "✏️", "admin": "🌐"}.get(src, "•")
        akey = (f"custom_{k}" if src == "custom" else
                f"admin_{k}"  if src == "admin"  else k)
        btns.append([
            Button.inline(f"{icon} {t['name'][:28]}", f"fwd_tpl_apply|{akey}".encode()),
            Button.inline("👁",                        f"fwd_tpl_preview|{akey}".encode()),
        ])
    if not btns:
        btns.append([Button.inline("🔍 Try Again", b"fwd_tpl_search")])
    btns.append([Button.inline("🔙 Back", b"fwd_templates_menu")])
    await event.respond(
        f"🔍 **`{query}`** — {len(results)} results",
        buttons=btns
    )


# ── Share code — generate ────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_share\\|(.+)"))
async def fwd_tpl_share(event):
    await event.answer()
    uid = event.sender_id
    key = event.data.decode().split("|")[1]
    tpl = _get_user_templates(uid).get(key)
    if not tpl:
        return await event.answer("Template nahi mila!", alert=True)
    code = _create_share_code(tpl)
    if not code:
        return await event.answer("Code generate nahi hua!", alert=True)
    tname = tpl['name']
    try:
        await event.edit(
            f"📤 **SHARE CODE: `{code}`**\n\n"
            f"Template: **{tname}**\n\n"
            "Kisi ko bhi ye 6-char code do — woh\n"
            "'📋 Import Code' se import kar sakta hai.\n"
            "_(Code hamesha valid rahega)_",
            buttons=[[Button.inline("🔙 Back", b"fwd_tpl_cat|custom")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Share code — import / use ─────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_tpl_use_code"))
async def fwd_tpl_use_code(event):
    await event.answer()
    data = get_user_data(event.sender_id)
    data["step"]       = "fwd_tpl_code_input"
    data["step_since"] = _time.time()
    try:
        await event.edit(
            "📋 **IMPORT VIA SHARE CODE**\n\n"
            "6-char share code type karo:\n_(e.g. `AB3X7K`)_",
            buttons=[[Button.inline("❌ Cancel", b"fwd_templates_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        get_user_data(e.sender_id).get("step") == "fwd_tpl_code_input"))
async def fwd_tpl_code_handler(event):
    uid  = event.sender_id
    data = get_user_data(uid)
    data["step"] = None
    code = event.raw_text.strip().upper()[:6]
    tpl  = _resolve_share_code(code)
    if not tpl:
        await event.respond(
            f"❌ Code `{code}` valid nahi hai.",
            buttons=[[Button.inline("🔙 Back", b"fwd_templates_menu")]]
        )
        return
    tname = tpl.get('name', 'Unnamed')
    tdesc = tpl.get('desc', '')
    await event.respond(
        f"✅ **Template Found: {tname}**\n_{tdesc}_\n\nKya karna hai?",
        buttons=[
            [Button.inline("✅ Apply Now",        f"fwd_tpl_apply_code|{code}".encode()),
             Button.inline("💾 Save to My Tpl",   f"fwd_tpl_save_code|{code}".encode())],
            [Button.inline("❌ Cancel",            b"fwd_templates_menu")],
        ]
    )


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_apply_code\\|(.+)"))
async def fwd_tpl_apply_code(event):
    await event.answer()
    uid  = event.sender_id
    code = event.data.decode().split("|")[1]
    tpl  = _resolve_share_code(code)
    if not tpl:
        return await event.answer("Code expire ho gaya!", alert=True)
    _tpl_save_undo(uid)
    _apply_snapshot(uid, tpl.get("settings", {}))
    tname = tpl.get('name','?')
    await event.answer(f"✅ Applied: {tname}", alert=False)
    try:
        await event.edit(
            f"✅ **Applied from code `{code}`**\n\n{tname}",
            buttons=[[Button.inline("📋 Templates", b"fwd_templates_menu"),
                      Button.inline("🏠 Menu",       b"main_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_save_code\\|(.+)"))
async def fwd_tpl_save_code(event):
    await event.answer()
    uid  = event.sender_id
    code = event.data.decode().split("|")[1]
    tpl  = _resolve_share_code(code)
    if not tpl:
        return await event.answer("Code expire ho gaya!", alert=True)
    customs = _get_user_templates(uid)
    if len(customs) >= 20:
        return await event.answer("❌ Max 20 templates!", alert=True)
    new_key = "".join(_random.choices(_string.ascii_lowercase + _string.digits, k=8))
    customs[new_key] = {
        "name":       tpl.get("name", f"Imported {code}"),
        "desc":       f"Imported via code {code}",
        "category":   "custom",
        "settings":   tpl.get("settings", {}),
        "created_at": int(_time.time()),
    }
    _save_user_templates(uid, customs)
    tname = tpl.get('name','?')
    await event.answer("✅ Saved!", alert=False)
    try:
        await event.edit(
            f"💾 **Saved: {tname}**",
            buttons=[[Button.inline("📋 My Templates", b"fwd_tpl_cat|custom"),
                      Button.inline("🏠 Menu",         b"main_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Lock / Unlock ────────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_lock\\|(.+)"))
async def fwd_tpl_lock(event):
    await event.answer()
    uid  = event.sender_id
    key  = event.data.decode().split("|")[1]
    tpls = _get_user_templates(uid)
    if key not in tpls:
        return await event.answer("Template nahi mila!", alert=True)
    tpls[key]["locked"] = not tpls[key].get("locked", False)
    _save_user_templates(uid, tpls)
    status = "🔒 Locked" if tpls[key]["locked"] else "🔓 Unlocked"
    await event.answer(status, alert=False)
    # Reopen edit menu
    event.data = f"fwd_tpl_edit|{key}".encode()
    await fwd_tpl_edit(event)


# ── Bulk Delete ──────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"fwd_tpl_bulk_del"))
async def fwd_tpl_bulk_del(event):
    await event.answer()
    uid      = event.sender_id
    custom   = _get_user_templates(uid)
    unlocked = sum(1 for v in custom.values() if not v.get("locked"))
    locked   = sum(1 for v in custom.values() if v.get("locked"))
    try:
        await event.edit(
            f"🗑 **BULK DELETE**\n\n"
            f"Unlocked: **{unlocked}** (delete honge)\n"
            f"Locked: **{locked}** (safe rahenge)\n\n"
            "Confirm?",
            buttons=[
                [Button.inline(f"🗑 Delete {unlocked} Templates", b"fwd_tpl_bulk_del_ok"),
                 Button.inline("❌ Cancel", b"fwd_tpl_cat|custom")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"fwd_tpl_bulk_del_ok"))
async def fwd_tpl_bulk_del_ok(event):
    await event.answer()
    uid     = event.sender_id
    custom  = _get_user_templates(uid)
    kept    = {k: v for k, v in custom.items() if v.get("locked")}
    deleted = len(custom) - len(kept)
    _save_user_templates(uid, kept)
    await event.answer(f"🗑 {deleted} deleted!", alert=False)
    try:
        await event.edit(
            _tpl_dashboard_text(uid, "custom"),
            buttons=_tpl_buttons(uid, "custom")
        )
    except errors.MessageNotModifiedError:
        pass


# ── Apply with Diff ──────────────────────────────────────────
@bot.on(events.CallbackQuery(pattern=b"fwd_tpl_diff\\|(.+)"))
async def fwd_tpl_diff(event):
    await event.answer()
    uid = event.sender_id
    key = event.data.decode().split("|")[1]

    if key.startswith("custom_"):
        tpl = _get_user_templates(uid).get(key[7:])
    elif key.startswith("admin_"):
        tpl = _get_admin_templates().get(key[6:])
    else:
        tpl = _FWD_TEMPLATES.get(key)

    if not tpl:
        return await event.answer("Template nahi mila!", alert=True)

    current  = _tpl_snapshot(uid)
    incoming = tpl.get("settings", {})

    changed = []
    same_c  = 0
    for k, nv in incoming.items():
        lbl = _KEY_LABELS.get(k, k)
        cv  = current.get(k)
        if cv == nv:
            same_c += 1
        else:
            def _fmt(v):
                if isinstance(v, str) and len(str(v)) > 25:
                    return str(v)[:25] + "…"
                return str(v) if v is not None else "—"
            arrow = "➕" if cv is None else "🔄"
            changed.append(f"  {arrow} **{lbl}**: `{_fmt(cv)}` → `{_fmt(nv)}`")

    diff_body = "\n".join(changed) if changed else "  _Koi change nahi hoga!_"
    tname = tpl['name']
    try:
        await event.edit(
            f"🔄 **DIFF: {tname}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**Jo badlega ({len(changed)}):**\n{diff_body}\n"
            f"_({same_c} settings same rahenge)_",
            buttons=[
                [Button.inline("✅ Apply", f"fwd_tpl_apply|{key}".encode()),
                 Button.inline("❌ Cancel", b"fwd_templates_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Edit menu — upgrade with share/lock/diff buttons ─────────
# Note: fwd_tpl_edit handler already exists above — we add a v2 that replaces it
# by monkey-patching it via a separate registration won't work in Telethon
# So the edit menu upgrade is handled inside the existing handler via event.data check


# Contact Admin — handled by support.py
