# ui/anti_spam_menu.py — ADVANCED ANTI-SPAM ADMIN PANEL v2.0
# ══════════════════════════════════════════════════════════════════

from telethon import events, Button, errors
from config import bot
from admin import is_admin
from database import get_user_data, save_persistent_db, GLOBAL_STATE
import asyncio


def _cfg():
    from anti_spam import get_config
    return get_config()

def _stats():
    from anti_spam import get_global_stats
    return get_global_stats()


# ── Main Panel Text ───────────────────────────────────────────────
def _main_text() -> str:
    cfg  = _cfg()
    st   = _stats()

    enabled   = cfg.get("enabled", True)
    action    = cfg.get("action", "warn")
    shadow    = cfg.get("shadow_mode", False)
    rl_en     = cfg.get("rate_limit_enabled", True)
    max_min   = cfg.get("max_per_min", 30)
    max_hr    = cfg.get("max_per_hour", 500)
    burst     = cfg.get("burst_limit", 10)
    burst_win = cfg.get("burst_window_sec", 5)
    strikes   = cfg.get("max_strikes", 3)
    temp_bl   = cfg.get("temp_block_minutes", 30)
    kw_en     = cfg.get("keyword_filter", False)
    kw_count  = len(cfg.get("banned_keywords", []))
    pause_m   = cfg.get("pause_minutes", 5)
    wl_prem   = cfg.get("whitelist_premium", True)

    action_icon = {"warn": "⚠️", "pause": "⏸️", "shadow": "👻", "block": "🚫"}.get(action, "⚠️")
    status_icon = "🟢" if enabled else "🔴"

    total_checks = st.get('total_checks', 0)
    block_rate = round(st.get('total_blocks', 0) / max(total_checks, 1) * 100, 1)
    bar_f = round(block_rate / 100 * 10)
    rate_bar = "█" * bar_f + "░" * (10 - bar_f)

    return (
        f"🛡️ **ANTI-SPAM ENGINE**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Status:** {status_icon} {'ON' if enabled else 'OFF'}  "
        f"{'  👻 Shadow Mode' if shadow else ''}\n\n"

        "**📊 Live Stats:**\n"
        f"  🔴 Active Blocks: `{st['active_blocks']}`\n"
        f"  ⏸️ Paused Users:  `{st['active_pauses']}`\n"
        f"  ⚡ Total Violations: `{st['total_violations']}`\n"
        f"  🚫 Total Drops:      `{st['total_drops']}`\n"
        f"  ⚠️ Warnings Sent:    `{st['total_warns']}`\n\n"

        "**⚙️ Rate Limits:**\n"
        f"  {'✅' if rl_en else '❌'} Rate Limit\n"
        f"  📬 Max/min: `{max_min}`  Max/hr: `{max_hr}`\n"
        f"  ⚡ Burst: `{burst}` msgs in `{burst_win}s`\n\n"

        "**🎯 Violation Settings:**\n"
        f"  Action: {action_icon} **{action.upper()}**\n"
        f"  Strikes before block: `{strikes}`\n"
        f"  Temp block duration: `{temp_bl} min`\n"
        f"  Pause duration: `{pause_m} min`\n\n"

        "**🔑 Keyword Filter:**\n"
        f"  {'✅ ON' if kw_en else '❌ OFF'}  "
        f"Banned keywords: `{kw_count}`\n\n"

        "**👑 Whitelist:**\n"
        f"  Premium bypass: {'✅' if wl_prem else '❌'}\n"
        f"  Custom UIDs: `{len(cfg.get('whitelist_uids', []))}`\n"
    )


def _main_buttons(user_id):
    cfg     = _cfg()
    enabled = cfg.get("enabled", True)
    shadow  = cfg.get("shadow_mode", False)
    rl_en   = cfg.get("rate_limit_enabled", True)
    kw_en   = cfg.get("keyword_filter", False)
    action  = cfg.get("action", "warn")

    return [
        [Button.inline(
            f"{'🔴 Disable AntiSpam' if enabled else '🟢 Enable AntiSpam'}",
            b"as_toggle_master"
        )],
        [Button.inline("⚙️ Rate Limits",       b"as_rate_menu"),
         Button.inline("🎯 Violation Action",  b"as_action_menu")],
        [Button.inline("⚡ Strike Settings",   b"as_strike_menu"),
         Button.inline("⏰ Block/Pause Time",  b"as_time_menu")],
        [Button.inline(
            f"{'👻 Shadow Mode OFF' if shadow else '👻 Shadow Mode ON'}",
            b"as_toggle_shadow"
        )],
        [Button.inline(
            f"{'🔴 Rate Limit OFF' if rl_en else '🟢 Rate Limit ON'}",
            b"as_toggle_ratelimit"
        ),
         Button.inline(
            f"{'🔴 KW Filter OFF' if kw_en else '🟢 KW Filter ON'}",
            b"as_toggle_kw"
        )],
        [Button.inline("🔑 Keywords",          b"as_kw_menu"),
         Button.inline("👑 Whitelist",         b"as_whitelist_menu")],
        [Button.inline("📊 Stats & Offenders", b"as_stats_menu"),
         Button.inline("🔔 Alert Settings",    b"as_alert_menu")],
        [Button.inline("🔙 Admin Panel",        b"adm_main")],
    ]


# ── Main Panel ────────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_main"))
async def as_main(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    try:
        await event.edit(_main_text(), buttons=_main_buttons(event.sender_id))
    except errors.MessageNotModifiedError:
        await event.answer("✅ Up to date!")


# ── Master Toggle ─────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_toggle_master"))
async def as_toggle_master(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key, get_config
    cfg = get_config()
    new = not cfg.get("enabled", True)
    set_config_key("enabled", new)
    await event.answer(f"Anti-Spam {'🟢 ON' if new else '🔴 OFF'}!")
    await as_main(event)


# ── Shadow Mode Toggle ────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_toggle_shadow"))
async def as_toggle_shadow(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key, get_config
    cfg = get_config()
    new = not cfg.get("shadow_mode", False)
    set_config_key("shadow_mode", new)
    await event.answer(f"Shadow Mode {'👻 ON' if new else 'OFF'}!")
    await as_main(event)


# ── Rate Limit Toggle ─────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_toggle_ratelimit"))
async def as_toggle_rl(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key, get_config
    cfg = get_config()
    new = not cfg.get("rate_limit_enabled", True)
    set_config_key("rate_limit_enabled", new)
    await event.answer(f"Rate Limit {'🟢 ON' if new else '🔴 OFF'}!")
    await as_main(event)


# ── Rate Limits Menu ──────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_rate_menu"))
async def as_rate_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    cfg = _cfg()
    text = (
        "⚙️ **Rate Limit Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📬 **Max/Minute:** `{cfg['max_per_min']}`\n"
        f"📊 **Max/Hour:**   `{cfg['max_per_hour']}`\n"
        f"⚡ **Burst Limit:** `{cfg['burst_limit']}` msgs in `{cfg['burst_window_sec']}s`\n\n"
        "Matlab kya hai:\n"
        "• `Max/Min` = 1 minute mein allowed forwarded messages\n"
        "• `Max/Hour` = 1 ghante mein allowed forwarded messages\n"
        "• `Burst` = N seconds mein agar itne msgs aaye toh violation\n\n"
        "👇 Preset choose karo ya custom set karo:"
    )
    btns = [
        [Button.inline("🐢 Slow (10/min)",   b"as_rl_preset_slow"),
         Button.inline("🚶 Normal (30/min)", b"as_rl_preset_normal")],
        [Button.inline("🏃 Fast (60/min)",   b"as_rl_preset_fast"),
         Button.inline("🚀 Turbo (120/min)", b"as_rl_preset_turbo")],
        [Button.inline("✏️ Custom Max/Min",  b"as_set_maxmin"),
         Button.inline("✏️ Custom Max/Hr",   b"as_set_maxhr")],
        [Button.inline("⚡ Set Burst Limit", b"as_set_burst"),
         Button.inline("⏱️ Burst Window",    b"as_set_bwin")],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError: pass


@bot.on(events.CallbackQuery(pattern=b"as_rl_preset_"))
async def as_rl_preset(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    preset = event.data.decode().replace("as_rl_preset_", "")
    presets = {
        "slow":   (10,  200,  5, 5),
        "normal": (30,  500, 10, 5),
        "fast":   (60, 1000, 15, 5),
        "turbo":  (120,2000, 25, 5),
    }
    if preset in presets:
        pm, ph, bl, bw = presets[preset]
        set_config_key("max_per_min",    pm)
        set_config_key("max_per_hour",   ph)
        set_config_key("burst_limit",    bl)
        set_config_key("burst_window_sec", bw)
        await event.answer(f"✅ {preset.title()} preset applied!")
    await as_rate_menu(event)


@bot.on(events.CallbackQuery(data=b"as_set_maxmin"))
async def as_set_maxmin(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    get_user_data(event.sender_id)["step"] = "as_input_maxmin"
    save_persistent_db()
    try:
        await event.edit(
            "✏️ **Max Messages per Minute**\n\nNumber bhejo (e.g. `30`):",
            buttons=[[Button.inline("❌ Cancel", b"as_rate_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"as_set_maxhr"))
async def as_set_maxhr(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    get_user_data(event.sender_id)["step"] = "as_input_maxhr"
    save_persistent_db()
    try:
        await event.edit(
            "✏️ **Max Messages per Hour**\n\nNumber bhejo (e.g. `500`):",
            buttons=[[Button.inline("❌ Cancel", b"as_rate_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"as_set_burst"))
async def as_set_burst(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    get_user_data(event.sender_id)["step"] = "as_input_burst"
    save_persistent_db()
    try:
        await event.edit(
            "✏️ **Burst Limit**\n\nKitne msgs burst window mein allowed? (e.g. `10`):",
            buttons=[[Button.inline("❌ Cancel", b"as_rate_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"as_set_bwin"))
async def as_set_bwin(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    get_user_data(event.sender_id)["step"] = "as_input_bwin"
    save_persistent_db()
    try:
        await event.edit(
            "✏️ **Burst Window (seconds)**\n\nKitne seconds ka window? (e.g. `5`):",
            buttons=[[Button.inline("❌ Cancel", b"as_rate_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Action Menu ───────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_action_menu"))
async def as_action_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    cfg     = _cfg()
    current = cfg.get("action", "warn")
    text = (
        "🎯 **Violation Action**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Current: **{current.upper()}**\n\n"
        "**Actions:**\n"
        "⚠️ `WARN`   — Message allow karo, warning bhejo\n"
        "⏸️ `PAUSE`  — Forwarding N minutes ke liye pause\n"
        "👻 `SHADOW` — Silently drop (user ko pata nahi)\n"
        "🚫 `BLOCK`  — Turant temp-block kar do\n\n"
        "Max strikes ke baad ALWAYS temp-block hoga\n"
        "(chahe action kuch bhi ho)"
    )
    btns = [
        [Button.inline("⚠️ WARN"   + (" ✅" if current=="warn"   else ""), b"as_act_warn"),
         Button.inline("⏸️ PAUSE"  + (" ✅" if current=="pause"  else ""), b"as_act_pause")],
        [Button.inline("👻 SHADOW" + (" ✅" if current=="shadow" else ""), b"as_act_shadow"),
         Button.inline("🚫 BLOCK"  + (" ✅" if current=="block"  else ""), b"as_act_block")],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"as_act_"))
async def as_set_action(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    action = event.data.decode().replace("as_act_", "")
    if action in ("warn", "pause", "shadow", "block"):
        set_config_key("action", action)
        await event.answer(f"Action: {action.upper()} ✅")
    await as_action_menu(event)


# ── Strike Settings ───────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_strike_menu"))
async def as_strike_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    cfg = _cfg()
    text = (
        "⚡ **Strike Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔴 **Max Strikes:** `{cfg['max_strikes']}`\n"
        f"⏰ **Strike Window:** `{cfg['strike_window_hours']} hours`\n\n"
        "Matlab: Agar koi `max_strikes` times violate kare\n"
        "`strike_window` ghante ke andar → auto-block.\n\n"
        "Window expire hone ke baad strikes reset ho jaate hain."
    )
    btns = [
        [Button.inline("1 Strike",  b"as_sk_1"),  Button.inline("2 Strikes", b"as_sk_2"),
         Button.inline("3 Strikes", b"as_sk_3"),  Button.inline("5 Strikes", b"as_sk_5")],
        [Button.inline("Window: 1h",  b"as_skw_1"), Button.inline("Window: 6h",  b"as_skw_6"),
         Button.inline("Window: 12h", b"as_skw_12"),Button.inline("Window: 24h", b"as_skw_24")],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"as_sk_"))
async def as_set_strikes(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    val = int(event.data.decode().replace("as_sk_", ""))
    set_config_key("max_strikes", val)
    await event.answer(f"Max strikes: {val}")
    await as_strike_menu(event)


@bot.on(events.CallbackQuery(pattern=b"as_skw_"))
async def as_set_strike_window(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    val = int(event.data.decode().replace("as_skw_", ""))
    set_config_key("strike_window_hours", val)
    await event.answer(f"Strike window: {val}h")
    await as_strike_menu(event)


# ── Block/Pause Time Menu ─────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_time_menu"))
async def as_time_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    cfg = _cfg()
    text = (
        "⏰ **Block & Pause Duration**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🚫 **Temp Block:** `{cfg['temp_block_minutes']} min` "
        f"({'∞ Permanent' if cfg['temp_block_minutes']==0 else 'Auto-unblock'})\n"
        f"⏸️ **Pause Duration:** `{cfg['pause_minutes']} min`\n\n"
        "Block = 0 → Permanent ban (manual unblock needed)"
    )
    btns = [
        [Button.inline("🚫 Block: 5m",  b"as_bl_5"),   Button.inline("Block: 15m", b"as_bl_15"),
         Button.inline("Block: 30m",    b"as_bl_30"),  Button.inline("Block: 60m", b"as_bl_60")],
        [Button.inline("Block: 6h",     b"as_bl_360"), Button.inline("Block: 24h", b"as_bl_1440"),
         Button.inline("Block: ∞",      b"as_bl_0")],
        [Button.inline("⏸️ Pause: 2m",  b"as_pa_2"),   Button.inline("Pause: 5m",  b"as_pa_5"),
         Button.inline("Pause: 10m",    b"as_pa_10"),  Button.inline("Pause: 30m", b"as_pa_30")],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"as_bl_"))
async def as_set_blocktime(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    val = int(event.data.decode().replace("as_bl_", ""))
    set_config_key("temp_block_minutes", val)
    label = f"{val} min" if val > 0 else "Permanent"
    await event.answer(f"Block duration: {label}")
    await as_time_menu(event)


@bot.on(events.CallbackQuery(pattern=b"as_pa_"))
async def as_set_pausetime(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    val = int(event.data.decode().replace("as_pa_", ""))
    set_config_key("pause_minutes", val)
    await event.answer(f"Pause duration: {val} min")
    await as_time_menu(event)


# ── Keyword Filter ────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_toggle_kw"))
async def as_toggle_kw(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key, get_config
    cfg = get_config()
    new = not cfg.get("keyword_filter", False)
    set_config_key("keyword_filter", new)
    await event.answer(f"Keyword Filter {'🟢 ON' if new else '🔴 OFF'}!")
    await as_main(event)


@bot.on(events.CallbackQuery(data=b"as_kw_menu"))
async def as_kw_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    cfg = _cfg()
    kws = cfg.get("banned_keywords", [])
    kw_list = "\n".join(f"  {i+1}. `{k}`" for i, k in enumerate(kws)) or "  (koi nahi)"
    kw_action = cfg.get("keyword_action", "warn")
    text = (
        "🔑 **Keyword Filter**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: {'🟢 ON' if cfg.get('keyword_filter') else '🔴 OFF'}\n"
        f"Action on hit: **{kw_action.upper()}**\n\n"
        f"**Banned Keywords ({len(kws)}):**\n{kw_list}\n\n"
        "Ye keywords source messages mein detect hone par action liya jaayega.\n"
        "Case-insensitive matching."
    )
    btns = [
        [Button.inline("➕ Add Keyword",     b"as_kw_add"),
         Button.inline("🗑️ Clear All",       b"as_kw_clear")],
        [Button.inline("⚠️ KW Action: WARN"  + (" ✅" if kw_action=="warn" else ""),  b"as_kwa_warn"),
         Button.inline("🚫 KW Action: BLOCK" + (" ✅" if kw_action=="block" else ""), b"as_kwa_block")],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    if kws:
        btns.insert(1, [Button.inline(f"🗑 Remove #{i+1}: {k[:12]}", f"as_kw_del_{i}".encode())
                        for i, k in enumerate(kws[:3])])
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"as_kw_add"))
async def as_kw_add(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    get_user_data(event.sender_id)["step"] = "as_input_kw_add"
    save_persistent_db()
    try:
        await event.edit(
            "➕ **Add Banned Keyword**\n\nKeyword/phrase bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"as_kw_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"as_kw_clear"))
async def as_kw_clear(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    set_config_key("banned_keywords", [])
    await event.answer("🗑️ All keywords cleared!")
    await as_kw_menu(event)


@bot.on(events.CallbackQuery(pattern=b"as_kw_del_"))
async def as_kw_del(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import get_config, set_config_key
    idx = int(event.data.decode().replace("as_kw_del_", ""))
    kws = get_config().get("banned_keywords", [])
    if 0 <= idx < len(kws):
        removed = kws.pop(idx)
        set_config_key("banned_keywords", kws)
        await event.answer(f"Removed: {removed}")
    await as_kw_menu(event)


@bot.on(events.CallbackQuery(pattern=b"as_kwa_"))
async def as_kwa_set(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    action = event.data.decode().replace("as_kwa_", "")
    set_config_key("keyword_action", action)
    await event.answer(f"KW Action: {action.upper()}")
    await as_kw_menu(event)


# ── Whitelist Menu ────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_whitelist_menu"))
async def as_whitelist_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    cfg  = _cfg()
    uids = cfg.get("whitelist_uids", [])
    uid_list = "\n".join(f"  • `{uid}`" for uid in uids) or "  (koi nahi)"
    text = (
        "👑 **Whitelist Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Premium Bypass: {'✅ ON' if cfg.get('whitelist_premium', True) else '❌ OFF'}\n\n"
        f"**Custom Whitelisted UIDs ({len(uids)}):**\n{uid_list}\n\n"
        "Whitelisted users pe koi anti-spam check nahi lagta."
    )
    btns = [
        [Button.inline(
            f"{'🔴 Premium Bypass OFF' if cfg.get('whitelist_premium',True) else '🟢 Premium Bypass ON'}",
            b"as_wl_tgl_prem"
        )],
        [Button.inline("➕ Add UID",  b"as_wl_add"),
         Button.inline("🗑️ Clear UIDs", b"as_wl_clear")],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"as_wl_tgl_prem"))
async def as_wl_tgl_prem(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key, get_config
    new = not get_config().get("whitelist_premium", True)
    set_config_key("whitelist_premium", new)
    await event.answer(f"Premium bypass {'✅ ON' if new else '🔴 OFF'}!")
    await as_whitelist_menu(event)


@bot.on(events.CallbackQuery(data=b"as_wl_add"))
async def as_wl_add(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    get_user_data(event.sender_id)["step"] = "as_input_wl_uid"
    save_persistent_db()
    try:
        await event.edit(
            "➕ **Whitelist mein UID add karo**\n\nUser ID bhejo:",
            buttons=[[Button.inline("❌ Cancel", b"as_whitelist_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"as_wl_clear"))
async def as_wl_clear(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key
    set_config_key("whitelist_uids", [])
    await event.answer("🗑️ Whitelist cleared!")
    await as_whitelist_menu(event)


# ── Stats & Offenders ─────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_stats_menu"))
async def as_stats_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    st = _stats()
    top = st.get("top_offenders", [])

    top_txt = ""
    medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
    for i, (uid, info) in enumerate(top):
        from anti_spam import get_user_spam_info
        si = get_user_spam_info(uid)
        status = "🔴" if si["is_blocked"] else ("⏸️" if si["is_paused"] else "🟢")
        top_txt += (
            f"{medals[i]} `{uid}` {status}\n"
            f"   Violations: `{info.get('violations',0)}`  "
            f"Blocks: `{info.get('blocks',0)}`  "
            f"Warns: `{info.get('warns',0)}`\n"
        )

    text = (
        "📊 **Anti-Spam Stats**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ Total Violations: `{st['total_violations']}`\n"
        f"🚫 Auto Blocks:      `{st['total_auto_blocks']}`\n"
        f"⚠️ Warnings Sent:    `{st['total_warns']}`\n"
        f"💀 Messages Dropped: `{st['total_drops']}`\n\n"
        f"🔴 Currently Blocked: `{st['active_blocks']}`\n"
        f"⏸️ Currently Paused:  `{st['active_pauses']}`\n\n"
        f"**🏆 Top Offenders:**\n"
        f"{top_txt or '  (no data yet)'}"
    )
    btns = [
        [Button.inline("🗑️ Reset All Stats",   b"as_stats_reset"),
         Button.inline("🔓 Unblock All",        b"as_unblock_all")],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"as_stats_reset"))
async def as_stats_reset(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    GLOBAL_STATE["anti_spam_stats"] = {
        "total_violations": 0, "total_auto_blocks": 0,
        "total_warns": 0, "total_drops": 0, "by_user": {}
    }
    save_persistent_db()
    await event.answer("✅ Stats reset!")
    await as_stats_menu(event)


@bot.on(events.CallbackQuery(data=b"as_unblock_all"))
async def as_unblock_all(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import _temp_blocked, _paused_until, _strikes
    count = len(_temp_blocked)
    _temp_blocked.clear()
    _paused_until.clear()
    for uid in _strikes:
        _strikes[uid] = {"count": 0, "first_ts": 0.0}
    await event.answer(f"✅ {count} users unblocked!")
    await as_stats_menu(event)


# ── Alert Settings ────────────────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"as_alert_menu"))
async def as_alert_menu(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    cfg = _cfg()
    al_bl  = cfg.get("alert_on_autoblock", True)
    al_br  = cfg.get("alert_on_burst", False)
    text = (
        "🔔 **Alert Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Auto-Block Alert: {'✅ ON' if al_bl else '❌ OFF'}\n"
        f"Burst Alert:      {'✅ ON' if al_br else '❌ OFF'}\n\n"
        "Alerts aapke (owner) Telegram account pe aate hain\n"
        "jab koi user auto-block hota hai."
    )
    btns = [
        [Button.inline(
            f"{'🔴 Block Alert OFF' if al_bl else '🟢 Block Alert ON'}",
            b"as_tgl_alert_bl"
        ),
         Button.inline(
            f"{'🔴 Burst Alert OFF' if al_br else '🟢 Burst Alert ON'}",
            b"as_tgl_alert_br"
        )],
        [Button.inline("🔙 Back", b"as_main")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"as_tgl_alert_bl"))
async def as_tgl_alert_bl(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key, get_config
    new = not get_config().get("alert_on_autoblock", True)
    set_config_key("alert_on_autoblock", new)
    await event.answer(f"Block alerts {'ON' if new else 'OFF'}!")
    await as_alert_menu(event)


@bot.on(events.CallbackQuery(data=b"as_tgl_alert_br"))
async def as_tgl_alert_br(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("❌", alert=True)
    from anti_spam import set_config_key, get_config
    new = not get_config().get("alert_on_burst", False)
    set_config_key("alert_on_burst", new)
    await event.answer(f"Burst alerts {'ON' if new else 'OFF'}!")
    await as_alert_menu(event)


# ── Step Input Handler (called from main.py) ──────────────────────
async def handle_antispam_steps(event, user_id: int, step: str) -> bool:
    """Returns True if step was handled."""
    from anti_spam import get_config, set_config_key

    text = (event.text or "").strip()

    if step == "as_input_maxmin":
        try:
            v = int(text)
            if 1 <= v <= 1000:
                set_config_key("max_per_min", v)
                get_user_data(user_id)["step"] = None
                save_persistent_db()
                await event.respond(f"✅ Max/min: `{v}`", buttons=[[Button.inline("🔙 Rate Limits", b"as_rate_menu")]])
            else:
                await event.respond("❌ 1–1000 ke beech number do.")
        except ValueError:
            await event.respond("❌ Valid number bhejo.")
        return True

    if step == "as_input_maxhr":
        try:
            v = int(text)
            if 10 <= v <= 100000:
                set_config_key("max_per_hour", v)
                get_user_data(user_id)["step"] = None
                save_persistent_db()
                await event.respond(f"✅ Max/hr: `{v}`", buttons=[[Button.inline("🔙 Rate Limits", b"as_rate_menu")]])
            else:
                await event.respond("❌ 10–100000 ke beech number do.")
        except ValueError:
            await event.respond("❌ Valid number bhejo.")
        return True

    if step == "as_input_burst":
        try:
            v = int(text)
            if 1 <= v <= 500:
                set_config_key("burst_limit", v)
                get_user_data(user_id)["step"] = None
                save_persistent_db()
                await event.respond(f"✅ Burst limit: `{v}`", buttons=[[Button.inline("🔙 Rate Limits", b"as_rate_menu")]])
            else:
                await event.respond("❌ 1–500 ke beech number do.")
        except ValueError:
            await event.respond("❌ Valid number bhejo.")
        return True

    if step == "as_input_bwin":
        try:
            v = int(text)
            if 1 <= v <= 60:
                set_config_key("burst_window_sec", v)
                get_user_data(user_id)["step"] = None
                save_persistent_db()
                await event.respond(f"✅ Burst window: `{v}s`", buttons=[[Button.inline("🔙 Rate Limits", b"as_rate_menu")]])
            else:
                await event.respond("❌ 1–60 seconds ke beech number do.")
        except ValueError:
            await event.respond("❌ Valid number bhejo.")
        return True

    if step == "as_input_kw_add":
        if text:
            cfg = get_config()
            kws = cfg.get("banned_keywords", [])
            if text.lower() not in [k.lower() for k in kws]:
                kws.append(text.lower())
                set_config_key("banned_keywords", kws)
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(f"✅ Keyword added: `{text}`", buttons=[[Button.inline("🔙 Keywords", b"as_kw_menu")]])
        else:
            await event.respond("❌ Keyword bhejo.")
        return True

    if step == "as_input_wl_uid":
        try:
            uid = int(text)
            cfg = get_config()
            wl  = cfg.get("whitelist_uids", [])
            if uid not in wl:
                wl.append(uid)
                set_config_key("whitelist_uids", wl)
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(f"✅ UID `{uid}` whitelisted!", buttons=[[Button.inline("🔙 Whitelist", b"as_whitelist_menu")]])
        except ValueError:
            await event.respond("❌ Valid User ID bhejo (numbers only).")
        return True

    return False
