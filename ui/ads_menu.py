"""
ads_menu.py — Ads Admin Panel v5.0

New in v5.0:
  ✅ Ad targeting — target premium / free / active users, min sources
  ✅ Per-user frequency cap — user daily / total impression limit
  ✅ Unique click tracking — dedup clicks per user
  ✅ Ad health warnings — low CTR, missing URL, cap alerts
  ✅ Duplicate ad — clone any ad
  ✅ Bulk pause / bulk activate by type
  ✅ Auto-pause on lifetime cap hit
  ✅ Targeting step in creation wizard
  ✅ Better analytics — unique users, today's impressions, health
  ✅ Settings — preset buttons for all numeric fields
  ✅ Quick Templates — one-click starter ads
"""

import time
from telethon import events, Button, errors
from config import bot
from admin import is_admin
import ads_engine as AE


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _fmt(v):   return f"₹{v:,.2f}"
def _ts(ts):   return time.strftime("%d/%m %H:%M", time.localtime(ts)) if ts else "—"
def _bar(v, mx, w=10):
    filled = round(v / max(1, mx) * w)
    return "█" * filled + "░" * (w - filled)

def _adm(event):
    if not is_admin(event.sender_id):
        raise PermissionError

def _clear_wiz(d: dict):
    d["step"] = None
    for k in ("adwiz", "adedit_ad_id", "adscfg_key"):
        d.pop(k, None)

def _progress(idx: int, total: int) -> str:
    filled = round(idx / total * 8)
    return f"[{'▓'*filled}{'░'*(8-filled)}] {idx}/{total}"

def _type_emoji(t: str) -> str:
    return {"banner": "📢", "button": "🔘", "popup": "📣", "blast": "⏰"}.get(t, "📢")

def _target_badge(ad: dict) -> str:
    parts = []
    if not ad.get("target_premium", True): parts.append("no💎")
    elif not ad.get("target_free", True):  parts.append("💎only")
    if ad.get("target_active"):            parts.append("⚡only")
    if ad.get("min_sources", 0):           parts.append(f"src≥{ad['min_sources']}")
    return f" `[{' '.join(parts)}]`" if parts else ""

def _wiz_buttons(idx: int, ad_type: str, optional: bool = False) -> list:
    row = []
    if idx > 0:
        row.append(Button.inline("◀️ Back", f"adwiz_back|{ad_type}".encode()))
    if optional:
        row.append(Button.inline("⏭ Skip", f"adwiz_skip|{ad_type}".encode()))
    row.append(Button.inline("❌ Cancel", b"adwiz_cancel"))
    return [row]

def _adwiz_next(wiz: dict):
    wiz["step_idx"] = wiz.get("step_idx", 0) + 1


# ─────────────────────────────────────────────────────────────────────────────
# MAIN DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"ads_panel"))
async def ads_panel(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌ Access denied", alert=True)

    e   = AE.get_earnings()
    cfg = AE._cfg()
    s   = AE.get_ads_summary()

    master  = "🟢 ON" if e["enabled"] else "🔴 OFF"
    monthly = cfg.get("monthly", {})
    months  = sorted(monthly)[-6:]
    max_rev = max((monthly.get(m, 0) for m in months), default=1)
    spark   = " ".join(f"`{m[-5:]}` {_bar(monthly.get(m,0), max_rev, 5)}" for m in months) or "—"

    # Type breakdown
    type_line = "  ".join(
        f"{_type_emoji(t)}`{s['by_type'].get(t,0)}`"
        for t in ["banner","button","popup","blast"]
    )

    # Today activity
    today_line = f"  📅 Aaj: `{s['today_imp']}` imp  `{s['today_clk']}` clicks\n" if s["today_imp"] else ""

    # Alerts
    alerts = []
    for ad in AE.list_ads():
        issues = AE.get_ad_health(ad["id"])
        if issues:
            alerts.append(f"⚠️ `{ad['title'][:20]}`: {issues[0]}")
    alert_block = "\n".join(alerts[:3])
    if alert_block:
        alert_block = f"\n\n**⚡ Health Alerts:**\n{alert_block}"

    text = (
        "📊 **ADS & MONETIZATION**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ System: **{master}**  ·  Rotation: `{e['rotation']}`\n"
        f"Active: `{s['active']}/{s['total']}`  Types: {type_line}\n"
        f"{today_line}\n"
        "**📈 Performance:**\n"
        f"  👁 Total Imp: `{e['total_impressions']:,}`\n"
        f"  🖱 Clicks: `{e['total_clicks']:,}`  CTR: `{e['ctr']}%`\n"
        f"  📊 eCPM: `{_fmt(e['ecpm'])}`\n\n"
        "**💰 Revenue:**\n"
        f"  📅 Is Mahine:  **{_fmt(e['this_month'])}**\n"
        f"  💼 Total:       {_fmt(e['total_earned'])}\n"
        f"  ⏳ Pending:     **{_fmt(e['pending_payout'])}**\n\n"
        f"**📅 Monthly:**\n{spark}"
        f"{alert_block}"
    )

    toggle_lbl = "🔴 Ads Band Karo" if e["enabled"] else "🟢 Ads Chalu Karo"
    try:
        await event.edit(text, buttons=[
            [Button.inline(toggle_lbl, b"ads_toggle_master")],
            [Button.inline("➕ New Ad",        b"ads_create_type"),
             Button.inline("⚡ Templates",    b"ads_quick_templates")],
            [Button.inline("📋 All Ads",      b"ads_list_all"),
             Button.inline("📊 Analytics",    b"ads_analytics")],
            [Button.inline("⚙️ Settings",     b"ads_settings"),
             Button.inline("🔬 A/B Tests",    b"ads_ab_menu")],
            [Button.inline("⚡ Bulk Actions", b"ads_bulk_menu"),
             Button.inline("💸 Payout",       b"ads_payout_menu")],
            [Button.inline("🏠 Admin",         b"adm_main")],
        ])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"ads_toggle_master"))
async def ads_toggle_master(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = AE._cfg()
    cfg["enabled"] = not cfg.get("enabled", False)
    AE._save()
    await event.answer(f"{'🟢 Chalu' if cfg['enabled'] else '🔴 Band'}!", alert=False)
    await ads_panel(event)


# ─────────────────────────────────────────────────────────────────────────────
# BULK ACTIONS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"ads_bulk_menu"))
async def ads_bulk_menu(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    s = AE.get_ads_summary()
    try:
        await event.edit(
            "⚡ **BULK ACTIONS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Active: `{s['active']}`  Paused: `{s['paused']}`\n\n"
            "Kya karna hai?",
            buttons=[
                [Button.inline(f"⏸ Pause All ({s['active']})", b"ads_bulk_pause_all")],
                [Button.inline(f"▶️ Activate All ({s['paused']})", b"ads_bulk_activate_all")],
                [Button.inline("⏸ Pause All Banners", b"ads_bulk_pause|banner"),
                 Button.inline("⏸ Pause All Popups",  b"ads_bulk_pause|popup")],
                [Button.inline("▶️ Activate Banners",  b"ads_bulk_act|banner"),
                 Button.inline("▶️ Activate Popups",   b"ads_bulk_act|popup")],
                [Button.inline("🔙 Back",               b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"ads_bulk_pause_all"))
async def ads_bulk_pause_all(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    count = AE.pause_all_ads()
    await event.answer(f"⏸ {count} ads paused!", alert=False)
    await ads_panel(event)


@bot.on(events.CallbackQuery(data=b"ads_bulk_activate_all"))
async def ads_bulk_activate_all(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    count = AE.activate_all_ads()
    await event.answer(f"▶️ {count} ads activated!", alert=False)
    await ads_panel(event)


@bot.on(events.CallbackQuery(pattern=b"ads_bulk_pause\\|(.+)"))
async def ads_bulk_pause_type(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_type = event.data.decode().split("|")[1]
    count   = AE.pause_all_ads(ad_type)
    await event.answer(f"⏸ {count} {ad_type} ads paused!", alert=False)
    await ads_bulk_menu(event)


@bot.on(events.CallbackQuery(pattern=b"ads_bulk_act\\|(.+)"))
async def ads_bulk_act_type(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_type = event.data.decode().split("|")[1]
    count   = AE.activate_all_ads(ad_type)
    await event.answer(f"▶️ {count} {ad_type} ads activated!", alert=False)
    await ads_bulk_menu(event)


# ─────────────────────────────────────────────────────────────────────────────
# QUICK TEMPLATES
# ─────────────────────────────────────────────────────────────────────────────

_TEMPLATES = {
    "channel": dict(
        ad_type="banner", title="Channel Promo",
        text="📢 Hamara channel join karo — daily updates, tips & tricks!",
        btn_label="📢 Join Karo", cpm=15.0, weight=100,
        target_premium=True, target_free=True,
    ),
    "product": dict(
        ad_type="button", title="Product Sale",
        text="🛍 Limited time offer! Abhi dekho aur save karo 50%",
        btn_label="🛍 Deal Dekho", cpm=25.0, weight=150,
        target_free=True, target_premium=False,
    ),
    "app": dict(
        ad_type="popup", title="App Download",
        text="📱 Naya app download karo — FREE! 10,000+ users already using it.",
        btn_label="📱 Download Karo", cpm=30.0, weight=120,
        target_premium=True, target_free=True,
    ),
    "premium_only": dict(
        ad_type="banner", title="Premium Sponsor",
        text="✨ Exclusive offer for premium members only!",
        btn_label="✨ Dekhiye", cpm=50.0, weight=200,
        target_premium=True, target_free=False,
    ),
    "active_only": dict(
        ad_type="popup", title="Active Users Offer",
        text="⚡ Sirf active users ke liye — special deal!",
        btn_label="⚡ Claim Karo", cpm=40.0, weight=150,
        target_active=True, target_premium=True, target_free=True,
    ),
    "generic": dict(
        ad_type="banner", title="Sponsor Banner",
        text="✨ Sponsored Message — Click to know more!",
        btn_label="✨ Janiye", cpm=10.0, weight=100,
        target_premium=True, target_free=True,
    ),
}


@bot.on(events.CallbackQuery(data=b"ads_quick_templates"))
async def ads_quick_templates(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    try:
        await event.edit(
            "⚡ **QUICK TEMPLATES**\n\n"
            "Ek click mein ready-made ad — baad mein edit karo!\n\n"
            "📢 **Channel** — Banner, all users, ₹15 CPM\n"
            "🛍 **Product Sale** — Button, free users only, ₹25 CPM\n"
            "📱 **App Download** — Pop-up, all users, ₹30 CPM\n"
            "💎 **Premium Only** — Banner, premium users only, ₹50 CPM\n"
            "⚡ **Active Only** — Pop-up, forwarding users only, ₹40 CPM\n"
            "✨ **Generic Banner** — Banner, all users, ₹10 CPM",
            buttons=[
                [Button.inline("📢 Channel",     b"qtpl|channel"),
                 Button.inline("🛍 Product",     b"qtpl|product")],
                [Button.inline("📱 App",         b"qtpl|app"),
                 Button.inline("💎 Prem Only",   b"qtpl|premium_only")],
                [Button.inline("⚡ Active Only", b"qtpl|active_only"),
                 Button.inline("✨ Generic",     b"qtpl|generic")],
                [Button.inline("✏️ Custom Ad",   b"ads_create_type")],
                [Button.inline("🔙 Back",         b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"qtpl\\|(.+)"))
async def qtpl_create(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    tpl_key = event.data.decode().split("|")[1]
    tpl = _TEMPLATES.get(tpl_key)
    if not tpl:
        return await event.answer("❌ Template nahi mila!", alert=True)
    ad_id = AE.create_ad(**tpl)
    await event.answer("✅ Template se Ad bana!", alert=False)
    try:
        await event.edit(
            f"✅ **Template Ad Created!**\n\n"
            f"{_build_ad_preview(AE.get_ad(ad_id))}\n\n"
            "💡 URL, sponsor aur details edit karo 👇",
            buttons=[
                [Button.inline("✏️ Edit",        f"ads_edit_menu|{ad_id}".encode())],
                [Button.inline("📋 Detail",      f"ads_detail|{ad_id}".encode()),
                 Button.inline("⚡ More Templates", b"ads_quick_templates")],
                [Button.inline("📊 Dashboard",    b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# AD TYPE SELECTION
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"ads_create_type"))
async def ads_create_type(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    try:
        await event.edit(
            "➕ **NEW AD — TYPE CHUNIYE**\n\n"
            "📢 **Banner** — Menu ke neeche text block\n"
            "   _Best for: awareness, channel promo_\n\n"
            "🔘 **Button** — Menu mein sponsor button\n"
            "   _Best for: high CTR, app download_\n\n"
            "📣 **Pop-up** — Dedicated alag message\n"
            "   _Best for: rich content, max impact_\n\n"
            "⏰ **Blast** — Sab users ko ek baar\n"
            "   _Best for: launches, time-sensitive_",
            buttons=[
                [Button.inline("📢 Banner",  b"adwiz|banner"),
                 Button.inline("🔘 Button",  b"adwiz|button")],
                [Button.inline("📣 Pop-up",  b"adwiz|popup"),
                 Button.inline("⏰ Blast",   b"adwiz|blast")],
                [Button.inline("⚡ Templates", b"ads_quick_templates")],
                [Button.inline("🔙 Back",      b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# CREATION WIZARD — 11 steps with targeting
# ─────────────────────────────────────────────────────────────────────────────

_WIZARD_STEPS = [
    ("title",         "📝 **Title** — admin label\ne.g., `Amazon Diwali Sale`",    False),
    ("text",          "✍️ **Ad Text** — users ko dikhega\nMarkdown supported",       False),
    ("url",           "🔗 **Destination URL** _(optional)_\n`https://` ya `@channel`", True),
    ("btn_label",     "🔘 **Button Label** _(optional)_\ne.g., `🛍 Deal Dekho`",    True),
    ("sponsor",       "👤 **Sponsor Name** _(optional)_\ne.g., `Amazon India`",      True),
    ("cpm",           "💰 **CPM Rate (₹)**\nHar 1000 impressions ki earning\nSkip = ₹10", True),
    ("weight",        "⚖️ **Priority Weight** (1–1000)\nZyada = zyada baar dikhe\nSkip = 100", True),
    ("daily_cap",     "📊 **Daily Impression Cap**\n0 ya Skip = unlimited",          True),
    ("schedule",      "⏰ **Time Schedule** _(optional)_\nFormat: `09:00-22:00`",    True),
    ("targeting",     None, True),   # Special step — buttons
    ("frequency",     "🎯 **Per-User Frequency Cap** _(optional)_\nFormat: `daily_cap total_cap`\ne.g., `3 10` = max 3 per day, 10 ever\nSkip = unlimited", True),
]
_WIZ_TOTAL = len(_WIZARD_STEPS)


@bot.on(events.CallbackQuery(pattern=b"adwiz\\|(.+)"))
async def adwiz_start(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    uid     = event.sender_id
    ad_type = event.data.decode().split("|")[1]
    from database import get_user_data
    d = get_user_data(uid)
    d["step"]  = "adwiz"
    d["adwiz"] = {"type": ad_type, "step_idx": 0, "data": {}}
    prog = _progress(1, _WIZ_TOTAL)
    try:
        await event.edit(
            f"➕ **New Ad: {AE.AD_TYPES.get(ad_type, ad_type)}**  {prog}\n\n"
            f"{_WIZARD_STEPS[0][1]}",
            buttons=_wiz_buttons(0, ad_type, _WIZARD_STEPS[0][2])
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adwiz_cancel"))
async def adwiz_cancel(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data, save_persistent_db
    d = get_user_data(event.sender_id)
    _clear_wiz(d); save_persistent_db()
    await event.answer("❌ Cancelled")
    await ads_create_type(event)


@bot.on(events.CallbackQuery(pattern=b"adwiz_back\\|(.+)"))
async def adwiz_back(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    uid     = event.sender_id
    ad_type = event.data.decode().split("|")[1]
    from database import get_user_data
    d   = get_user_data(uid)
    wiz = d.get("adwiz", {})
    idx = wiz.get("step_idx", 0)
    if idx <= 0:
        _clear_wiz(d)
        return await ads_create_type(event)
    new_idx = idx - 1
    wiz["step_idx"] = new_idx
    d["adwiz"] = wiz
    key, txt, optional = _WIZARD_STEPS[new_idx]
    prog = _progress(new_idx + 1, _WIZ_TOTAL)
    if key == "targeting":
        await _show_targeting_step(event, wiz, ad_type, new_idx)
    else:
        try:
            await event.edit(
                f"➕ **{AE.AD_TYPES.get(ad_type, ad_type)}**  {prog}\n\n{txt}",
                buttons=_wiz_buttons(new_idx, ad_type, optional)
            )
        except errors.MessageNotModifiedError:
            pass


async def _show_targeting_step(event, wiz: dict, ad_type: str, idx: int):
    """Show targeting selection buttons."""
    data = wiz.get("data", {})
    prog = _progress(idx + 1, _WIZ_TOTAL)
    t_prem    = data.get("target_premium", True)
    t_free    = data.get("target_free",    True)
    t_active  = data.get("target_active",  False)
    min_src   = data.get("min_sources",    0)

    try:
        await event.edit(
            f"🎯 **Targeting**  {prog}\n\n"
            "Ye ad KAUN dekhe?\n\n"
            f"  💎 Premium users: **{'✅ Haan' if t_prem else '❌ Nahi'}**\n"
            f"  🆓 Free users: **{'✅ Haan' if t_free else '❌ Nahi'}**\n"
            f"  ⚡ Active only (forwarding): **{'✅ Haan' if t_active else '❌ Nahi'}**\n"
            f"  📥 Min sources: **{min_src if min_src else 'Any'}**\n\n"
            "_Toggle karo ya Done dabao:_",
            buttons=[
                [Button.inline(f"💎 Premium: {'✅' if t_prem else '❌'}", f"adwiz_tgt|prem|{ad_type}".encode()),
                 Button.inline(f"🆓 Free: {'✅' if t_free else '❌'}",     f"adwiz_tgt|free|{ad_type}".encode())],
                [Button.inline(f"⚡ Active Only: {'✅' if t_active else '❌'}", f"adwiz_tgt|active|{ad_type}".encode())],
                [Button.inline("📥 Min Src: Any", f"adwiz_tgt|minsrc0|{ad_type}".encode()),
                 Button.inline("📥 Min Src: 1+",  f"adwiz_tgt|minsrc1|{ad_type}".encode()),
                 Button.inline("📥 Min Src: 2+",  f"adwiz_tgt|minsrc2|{ad_type}".encode())],
                [Button.inline("✅ Done — Next Step", f"adwiz_tgt|done|{ad_type}".encode())],
                [Button.inline("⏭ Skip (all users)",  f"adwiz_skip|{ad_type}".encode()),
                 Button.inline("❌ Cancel",            b"adwiz_cancel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adwiz_tgt\\|(.+)\\|(.+)"))
async def adwiz_targeting(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    action, ad_type = parts[1], parts[2]
    from database import get_user_data
    d   = get_user_data(event.sender_id)
    wiz = d.get("adwiz", {})
    dat = wiz.setdefault("data", {})
    idx = wiz.get("step_idx", 0)

    if action == "prem":
        dat["target_premium"] = not dat.get("target_premium", True)
    elif action == "free":
        dat["target_free"] = not dat.get("target_free", True)
    elif action == "active":
        dat["target_active"] = not dat.get("target_active", False)
    elif action.startswith("minsrc"):
        dat["min_sources"] = int(action.replace("minsrc", ""))
    elif action == "done":
        # Advance to next step
        _adwiz_next(wiz)
        d["adwiz"] = wiz
        next_idx = wiz["step_idx"]
        if next_idx >= _WIZ_TOTAL:
            return await _finish_wizard(event, wiz, ad_type, d)
        key, txt, optional = _WIZARD_STEPS[next_idx]
        prog = _progress(next_idx + 1, _WIZ_TOTAL)
        try:
            await event.edit(
                f"➕ **{AE.AD_TYPES.get(ad_type, ad_type)}**  {prog}\n\n{txt}",
                buttons=_wiz_buttons(next_idx, ad_type, optional)
            )
        except errors.MessageNotModifiedError:
            pass
        return

    d["adwiz"] = wiz
    await _show_targeting_step(event, wiz, ad_type, idx)


@bot.on(events.CallbackQuery(pattern=b"adwiz_skip\\|(.+)"))
async def adwiz_skip(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    uid     = event.sender_id
    ad_type = event.data.decode().split("|")[1]
    from database import get_user_data, save_persistent_db
    d   = get_user_data(uid)
    wiz = d.get("adwiz", {})
    _adwiz_next(wiz)
    d["adwiz"] = wiz
    next_idx = wiz.get("step_idx", 0)
    if next_idx >= _WIZ_TOTAL:
        return await _finish_wizard(event, wiz, ad_type, d)
    key, txt, optional = _WIZARD_STEPS[next_idx]
    prog = _progress(next_idx + 1, _WIZ_TOTAL)
    if key == "targeting":
        await _show_targeting_step(event, wiz, ad_type, next_idx)
    else:
        try:
            await event.edit(
                f"➕ **{AE.AD_TYPES.get(ad_type, ad_type)}**  {prog}\n\n{txt}",
                buttons=_wiz_buttons(next_idx, ad_type, optional)
            )
        except errors.MessageNotModifiedError:
            pass


async def _finish_wizard(event, wiz: dict, ad_type: str, d: dict):
    from database import save_persistent_db
    data  = wiz.get("data", {})
    ad_id = AE.create_ad(
        ad_type         = ad_type,
        title           = data.get("title",    "Untitled"),
        text            = data.get("text",     ""),
        url             = data.get("url",      ""),
        btn_label       = data.get("btn_label",""),
        sponsor         = data.get("sponsor",  ""),
        cpm             = float(data.get("cpm", 10.0)),
        weight          = int(data.get("weight", 100)),
        daily_cap       = int(data.get("daily_cap", 0)),
        schedule_start  = data.get("schedule_start", ""),
        schedule_end    = data.get("schedule_end",   ""),
        target_premium  = data.get("target_premium", True),
        target_free     = data.get("target_free",    True),
        target_active   = data.get("target_active",  False),
        min_sources     = int(data.get("min_sources", 0)),
        user_daily_cap  = int(data.get("user_daily_cap", 0)),
        user_total_cap  = int(data.get("user_total_cap", 0)),
    )
    _clear_wiz(d); save_persistent_db()
    try:
        await event.edit(
            f"✅ **Ad Created!**\n\n{_build_ad_preview(AE.get_ad(ad_id))}",
            buttons=[
                [Button.inline("📋 Ad Detail",  f"ads_detail|{ad_id}".encode())],
                [Button.inline("✏️ Edit",       f"ads_edit_menu|{ad_id}".encode()),
                 Button.inline("➕ Aur Banao",  b"ads_create_type")],
                [Button.inline("📊 Dashboard",  b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage())
async def adwiz_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "adwiz": return

    wiz     = d.get("adwiz", {})
    idx     = wiz.get("step_idx", 0)
    text    = event.raw_text.strip()
    ad_type = wiz.get("type", "banner")

    if text.lower() in ("/cancel", "cancel"):
        _clear_wiz(d); save_persistent_db()
        return await event.respond("❌ Ad creation cancelled.", buttons=[
            [Button.inline("➕ New Ad",    b"ads_create_type"),
             Button.inline("📊 Dashboard", b"ads_panel")]])

    key, _, _ = _WIZARD_STEPS[idx]
    error_msg = None

    # Parse based on field
    if key == "cpm":
        try:
            val = float(text)
            if val < 0: raise ValueError
            wiz["data"][key] = val
        except ValueError:
            error_msg = "❌ CPM: positive number daalo (e.g., `25.5`)"
    elif key == "weight":
        try:
            wiz["data"][key] = max(1, min(1000, int(text)))
        except ValueError:
            error_msg = "❌ Weight: 1–1000 ke beech daalo"
    elif key == "daily_cap":
        try:
            val = int(text); assert val >= 0
            wiz["data"][key] = val
        except:
            error_msg = "❌ Cap: 0 ya positive integer"
    elif key == "schedule":
        try:
            s, e = text.split("-")
            sh, sm = map(int, s.strip().split(":")); eh, em = map(int, e.strip().split(":"))
            assert all(0 <= x <= 23 for x in [sh, eh]) and all(0 <= x <= 59 for x in [sm, em])
            wiz["data"]["schedule_start"] = s.strip()
            wiz["data"]["schedule_end"]   = e.strip()
        except:
            error_msg = "❌ Format: `09:00-22:00`"
    elif key == "frequency":
        try:
            parts = text.split()
            wiz["data"]["user_daily_cap"] = int(parts[0]) if len(parts) > 0 else 0
            wiz["data"]["user_total_cap"] = int(parts[1]) if len(parts) > 1 else 0
        except:
            error_msg = "❌ Format: `3 10` (daily total) ya `0 0` (unlimited)"
    elif key == "title" and len(text) < 2:
        error_msg = "❌ Title 2+ chars"
    elif key == "text" and len(text) < 5:
        error_msg = "❌ Ad text 5+ chars"
    else:
        wiz["data"][key] = text

    if error_msg:
        return await event.respond(error_msg, buttons=_wiz_buttons(idx, ad_type, True))

    _adwiz_next(wiz)
    d["adwiz"] = wiz
    next_idx   = wiz["step_idx"]

    if next_idx >= _WIZ_TOTAL:
        return await _finish_wizard(event, wiz, ad_type, d)

    nkey, ntxt, nopt = _WIZARD_STEPS[next_idx]
    prog = _progress(next_idx + 1, _WIZ_TOTAL)
    if nkey == "targeting":
        class _FakeEvent:
            def __init__(self, uid):
                self.sender_id = uid
            async def edit(self, *a, **kw):
                pass
            async def respond(self, txt, buttons=None):
                nonlocal _msg
                _msg = (txt, buttons)
        _msg = (None, None)

        await event.respond(
            f"🎯 **Targeting**  {prog}",
            buttons=[
                [Button.inline("💎 Premium ✅", f"adwiz_tgt|prem|{ad_type}".encode()),
                 Button.inline("🆓 Free ✅",    f"adwiz_tgt|free|{ad_type}".encode())],
                [Button.inline("⚡ Active Only ❌", f"adwiz_tgt|active|{ad_type}".encode())],
                [Button.inline("📥 Min Src: Any", f"adwiz_tgt|minsrc0|{ad_type}".encode()),
                 Button.inline("📥 Min Src: 1+",  f"adwiz_tgt|minsrc1|{ad_type}".encode()),
                 Button.inline("📥 Min Src: 2+",  f"adwiz_tgt|minsrc2|{ad_type}".encode())],
                [Button.inline("✅ Done", f"adwiz_tgt|done|{ad_type}".encode())],
                [Button.inline("⏭ Skip", f"adwiz_skip|{ad_type}".encode()),
                 Button.inline("❌ Cancel", b"adwiz_cancel")],
            ]
        )
    else:
        await event.respond(
            f"➕ **{AE.AD_TYPES.get(ad_type, ad_type)}**  {prog}\n\n{ntxt}",
            buttons=_wiz_buttons(next_idx, ad_type, nopt)
        )


# ─────────────────────────────────────────────────────────────────────────────
# AD LIST
# ─────────────────────────────────────────────────────────────────────────────

def _build_ad_preview(ad: dict) -> str:
    if not ad: return "—"
    ana    = AE.get_ad_analytics(ad["id"])
    sched  = f"`{ad.get('schedule_start')}–{ad.get('schedule_end')}`" if ad.get("schedule_start") else "Anytime"
    cap    = f"`{ad.get('daily_cap')}/day`" if ad.get("daily_cap") else "Unlimited"
    target = _target_badge(ad)
    health = AE.get_ad_health(ad["id"])
    warn   = f"\n⚠️ {health[0]}" if health else ""

    return (
        f"🆔 `{ad['id']}`  {_type_emoji(ad['type'])} {AE.AD_TYPES.get(ad['type'], ad['type'])}{target}\n"
        f"📝 **{ad['title']}**\n"
        f"👁 Imp:`{ana['impressions']}`  Unique Users:`{ana['unique_users']}`\n"
        f"🖱 Clicks:`{ana['clicks']}`  Unique:`{ana['unique_clicks']}`  CTR:`{ana['ctr']}%`\n"
        f"💰 CPM:`₹{ad['cpm']}`  Earned:`₹{ana['earned']}`  RPM:`₹{ana['rpm']}`\n"
        f"⚖️ Weight:`{ad.get('weight',100)}`  Cap:{cap}  Sched:{sched}\n"
        f"Status:{'✅ Active' if ad.get('active') else '⏸ Paused'}"
        f"{warn}"
    )


@bot.on(events.CallbackQuery(pattern=b"ads_list(_all|_type\\|.+)?"))
async def ads_list_all(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    data_str    = event.data.decode()
    filter_type = data_str.split("|")[1] if "|" in data_str else None
    ads = AE.list_ads(ad_type=filter_type)

    if not ads:
        return await event.edit(
            "📋 **Koi ads nahi hain.**\n\nPehle ad banao!",
            buttons=[[Button.inline("➕ New Ad", b"ads_create_type"),
                      Button.inline("⚡ Templates", b"ads_quick_templates")],
                     [Button.inline("🔙 Back", b"ads_panel")]]
        )

    type_btns = [Button.inline(
        f"{'✅' if filter_type==t else ''}{_type_emoji(t)}",
        f"ads_list_type|{t}".encode()
    ) for t, _ in AE.AD_TYPES.items()]
    type_btns.append(Button.inline("🔄 All", b"ads_list_all"))

    ad_btns = []
    for ad in ads[:12]:
        ana    = AE.get_ad_analytics(ad["id"])
        state  = "✅" if ad.get("active") else "⏸"
        health = "⚠️" if AE.get_ad_health(ad["id"]) else ""
        ad_btns.append([Button.inline(
            f"{state}{health} {ad['title'][:18]} · 👁{ana['impressions']} CTR{ana['ctr']}%",
            f"ads_detail|{ad['id']}".encode()
        )])

    try:
        await event.edit(
            f"📋 **All Ads** — {len(ads)} total\n_⚠️ = health issues_\n\nFilter:",
            buttons=[type_btns[:2], type_btns[2:]] + ad_btns + [
                [Button.inline("➕ New Ad",        b"ads_create_type"),
                 Button.inline("⚡ Bulk Actions", b"ads_bulk_menu")],
                [Button.inline("🔙 Back",           b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# AD DETAIL
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"ads_detail\\|(.+)"))
async def ads_detail(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ad    = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌ Ad nahi mila!", alert=True)

    ana    = AE.get_ad_analytics(ad_id)
    daily  = ana["daily"]
    max_d  = max((c for _, c in daily), default=1)
    chart  = "\n".join(f"`{d}` {_bar(c, max_d, 8)} `{c}`" for d, c in daily)
    health = AE.get_ad_health(ad_id)
    health_block = ""
    if health:
        health_block = "\n\n**⚠️ Health Issues:**\n" + "\n".join(f"  {h}" for h in health)

    toggle_lbl = "⏸ Pause" if ad.get("active") else "▶️ Activate"
    try:
        await event.edit(
            f"📢 **Ad Detail**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{_build_ad_preview(ad)}\n\n"
            f"**Today:** 👁`{ana['today_imp']}`\n"
            f"**📅 Last 7 Days:**\n{chart or '—'}\n\n"
            f"📝 `{ad.get('text','')[:120]}`\n"
            f"🔗 `{ad.get('url','—')}`  👤 `{ad.get('sponsor','—')}`"
            f"{health_block}",
            buttons=[
                [Button.inline(toggle_lbl,         f"adtog|{ad_id}".encode()),
                 Button.inline("✏️ Edit",           f"ads_edit_menu|{ad_id}".encode())],
                [Button.inline("👁 Preview/Test",  f"ad_preview|{ad_id}".encode()),
                 Button.inline("📊 Full Stats",    f"adstats|{ad_id}".encode())],
                [Button.inline("📋 Duplicate",     f"addupe|{ad_id}".encode()),
                 Button.inline("🔬 A/B Test",      f"adab_start|{ad_id}".encode())],
                [Button.inline("🗑 Delete",         f"addel|{ad_id}".encode()),
                 Button.inline("🔙 All Ads",        b"ads_list_all")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adtog\\|(.+)"))
async def ad_toggle_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    state = AE.toggle_ad(ad_id)
    await event.answer(f"{'✅ Activated' if state else '⏸ Paused'}")
    await ads_detail(event)


@bot.on(events.CallbackQuery(pattern=b"addupe\\|(.+)"))
async def ad_duplicate(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id  = event.data.decode().split("|")[1]
    new_id = AE.duplicate_ad(ad_id)
    if not new_id:
        return await event.answer("❌ Duplicate failed!", alert=True)
    await event.answer("✅ Ad duplicated (paused)!", alert=False)
    event.data = f"ads_detail|{new_id}".encode()
    await ads_detail(event)


# ─────────────────────────────────────────────────────────────────────────────
# EDIT MENU
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"ads_edit_menu\\|(.+)"))
async def ads_edit_menu(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ad    = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌ Ad nahi mila!", alert=True)

    # Show targeting summary
    t_info = (
        f"  💎{'✅' if ad.get('target_premium',True) else '❌'}  "
        f"🆓{'✅' if ad.get('target_free',True) else '❌'}  "
        f"⚡{'✅' if ad.get('target_active') else '❌'}  "
        f"Src≥{ad.get('min_sources',0)}"
    )

    try:
        await event.edit(
            f"✏️ **Edit: {ad['title']}**\n\n"
            f"Targeting: {t_info}\n\n"
            "Kya edit karna hai?",
            buttons=[
                [Button.inline("📝 Title",        f"adedit_f|title|{ad_id}".encode()),
                 Button.inline("✍️ Text",         f"adedit_f|text|{ad_id}".encode())],
                [Button.inline("🔗 URL",          f"adedit_f|url|{ad_id}".encode()),
                 Button.inline("🔘 Btn Label",    f"adedit_f|btn_label|{ad_id}".encode())],
                [Button.inline("👤 Sponsor",      f"adedit_f|sponsor|{ad_id}".encode()),
                 Button.inline("💰 CPM",          f"adedit_f|cpm|{ad_id}".encode())],
                [Button.inline("⚖️ Weight",       f"adedit_f|weight|{ad_id}".encode()),
                 Button.inline("📊 Daily Cap",    f"adedit_f|daily_cap|{ad_id}".encode())],
                [Button.inline("⏰ Schedule",     f"adedit_f|schedule|{ad_id}".encode())],
                [Button.inline("🎯 Targeting",    f"adedit_targeting|{ad_id}".encode())],
                [Button.inline("🔙 Ad Detail",    f"ads_detail|{ad_id}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adedit_targeting\\|(.+)"))
async def adedit_targeting(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ad    = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌", alert=True)

    t_prem   = ad.get("target_premium", True)
    t_free   = ad.get("target_free",    True)
    t_active = ad.get("target_active",  False)
    min_src  = ad.get("min_sources",    0)

    try:
        await event.edit(
            f"🎯 **Targeting: {ad['title']}**\n\n"
            f"💎 Premium: **{'✅' if t_prem else '❌'}**\n"
            f"🆓 Free: **{'✅' if t_free else '❌'}**\n"
            f"⚡ Active only: **{'✅' if t_active else '❌'}**\n"
            f"📥 Min sources: **{min_src}**",
            buttons=[
                [Button.inline(f"💎 Premium: {'✅→❌' if t_prem else '❌→✅'}", f"adtgt|prem|{ad_id}".encode()),
                 Button.inline(f"🆓 Free: {'✅→❌' if t_free else '❌→✅'}",    f"adtgt|free|{ad_id}".encode())],
                [Button.inline(f"⚡ Active Only: {'✅→❌' if t_active else '❌→✅'}", f"adtgt|active|{ad_id}".encode())],
                [Button.inline("📥 Min Src: 0", f"adtgt|minsrc0|{ad_id}".encode()),
                 Button.inline("📥 Min Src: 1", f"adtgt|minsrc1|{ad_id}".encode()),
                 Button.inline("📥 Min Src: 2", f"adtgt|minsrc2|{ad_id}".encode())],
                [Button.inline("🔙 Edit Menu",  f"ads_edit_menu|{ad_id}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adtgt\\|(.+)\\|(.+)"))
async def adtgt_toggle(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts  = event.data.decode().split("|")
    action, ad_id = parts[1], parts[2]
    ad = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌", alert=True)

    if action == "prem":
        AE.update_ad(ad_id, target_premium=not ad.get("target_premium", True))
    elif action == "free":
        AE.update_ad(ad_id, target_free=not ad.get("target_free", True))
    elif action == "active":
        AE.update_ad(ad_id, target_active=not ad.get("target_active", False))
    elif action.startswith("minsrc"):
        AE.update_ad(ad_id, min_sources=int(action.replace("minsrc","")))

    await event.answer("✅ Updated!", alert=False)
    event.data = f"adedit_targeting|{ad_id}".encode()
    await adedit_targeting(event)


@bot.on(events.CallbackQuery(pattern=b"adedit_f\\|(.+)\\|(.+)"))
async def adedit_field_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    field, ad_id = parts[1], parts[2]
    ad = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌ Ad nahi mila!", alert=True)
    labels = {"title":"Title","text":"Ad Text","url":"URL","btn_label":"Button Label",
              "sponsor":"Sponsor Name","cpm":"CPM (₹)","weight":"Weight (1–1000)",
              "daily_cap":"Daily Cap (0=unlimited)","schedule":"Schedule (HH:MM-HH:MM / off)"}
    cur_val = ad.get(field,"")
    if field == "schedule":
        cur_val = f"{ad.get('schedule_start','')}–{ad.get('schedule_end','')}".strip("–") or "—"
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = f"adedit_{field}"; d["adedit_ad_id"] = ad_id
    d["step_since"] = time.time()
    try:
        await event.edit(
            f"✏️ **Edit: {labels.get(field,field)}**\n\nCurrent: `{cur_val}`\n\nNaya value bhejo:",
            buttons=[[Button.inline("🔙 Edit Menu", f"ads_edit_menu|{ad_id}".encode()),
                      Button.inline("❌ Cancel",     f"ads_detail|{ad_id}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


# Legacy redirect
@bot.on(events.CallbackQuery(pattern=b"adedit_(cpm|wt)\\|(.+)"))
async def adedit_legacy(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    field = "weight" if parts[0].endswith("wt") else "cpm"
    ad_id = parts[1]
    event.data = f"adedit_f|{field}|{ad_id}".encode()
    await adedit_field_cb(event)


@bot.on(events.NewMessage())
async def adedit_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d    = get_user_data(uid)
    step = d.get("step") or ""
    if not step.startswith("adedit_"): return

    field = step.replace("adedit_","")
    ad_id = d.get("adedit_ad_id","")
    text  = event.raw_text.strip()

    if text.lower() in ("/cancel","cancel"):
        _clear_wiz(d); save_persistent_db()
        back = [[Button.inline("✏️ Edit Menu", f"ads_edit_menu|{ad_id}".encode()),
                 Button.inline("📋 Ad Detail", f"ads_detail|{ad_id}".encode())]] if ad_id else \
               [[Button.inline("📋 All Ads", b"ads_list_all")]]
        return await event.respond("❌ Edit cancelled.", buttons=back)

    if not ad_id or not AE.get_ad(ad_id):
        _clear_wiz(d)
        return await event.respond("❌ Ad nahi mila.")

    retry = [[Button.inline("🔁 Retry", f"adedit_f|{field}|{ad_id}".encode()),
              Button.inline("❌ Cancel",  f"ads_detail|{ad_id}".encode())]]
    try:
        if field == "cpm":
            AE.update_ad(ad_id, cpm=float(text))
        elif field == "weight":
            AE.update_ad(ad_id, weight=max(1,min(1000,int(text))))
        elif field == "daily_cap":
            AE.update_ad(ad_id, daily_cap=max(0,int(text)))
        elif field == "schedule":
            if text.lower() in ("off","0","none"):
                AE.update_ad(ad_id, schedule_start="", schedule_end="")
            else:
                s, e2 = text.split("-")
                sh,sm = map(int,s.strip().split(":")); eh,em = map(int,e2.strip().split(":"))
                assert all(0<=x<=23 for x in [sh,eh]) and all(0<=x<=59 for x in [sm,em])
                AE.update_ad(ad_id, schedule_start=s.strip(), schedule_end=e2.strip())
        elif field == "title" and len(text) < 2:
            return await event.respond("❌ Title 2+ chars chahiye:", buttons=retry)
        elif field in ("title","text","url","btn_label","sponsor"):
            AE.update_ad(ad_id, **{field: text})
        else:
            _clear_wiz(d); return await event.respond("❌ Unknown field.")

        _clear_wiz(d); save_persistent_db()
        await event.respond(
            f"✅ **{field.replace('_',' ').title()} updated!**",
            buttons=[[Button.inline("✏️ Aur Edit", f"ads_edit_menu|{ad_id}".encode()),
                      Button.inline("📋 Detail",    f"ads_detail|{ad_id}".encode())]]
        )
    except:
        hints = {"cpm":"❌ Positive number (e.g. `25.5`)",
                 "weight":"❌ 1–1000 ke beech",
                 "daily_cap":"❌ 0 ya positive integer",
                 "schedule":"❌ Format: `09:00-22:00` ya `off`"}
        await event.respond(hints.get(field, "❌ Invalid value"), buttons=retry)


# ─────────────────────────────────────────────────────────────────────────────
# FULL STATS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"adstats\\|(.+)"))
async def adstats_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ana   = AE.get_ad_analytics(ad_id)
    daily = ana["daily"]
    max_d = max((c for _, c in daily), default=1)
    chart = "\n".join(f"`{d}` {_bar(c, max_d, 12)} `{c}`" for d, c in daily)

    # Targeting summary
    tgt_parts = []
    if ana.get("target_premium") and ana.get("target_free"): tgt_parts.append("All users")
    elif ana.get("target_premium"): tgt_parts.append("💎 Premium only")
    elif ana.get("target_free"):    tgt_parts.append("🆓 Free only")
    if ana.get("target_active"):    tgt_parts.append("⚡ Active only")
    if ana.get("min_sources", 0):   tgt_parts.append(f"📥 src≥{ana['min_sources']}")
    target_line = "  ·  ".join(tgt_parts) if tgt_parts else "All users"

    health  = AE.get_ad_health(ad_id)
    h_block = "\n".join(f"  {h}" for h in health) if health else "  ✅ No issues"

    try:
        await event.edit(
            f"📊 **Full Stats: {ana['title']}**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👁 Impressions: `{ana['impressions']:,}`\n"
            f"👥 Unique Users: `{ana['unique_users']:,}`\n"
            f"🖱 Clicks: `{ana['clicks']:,}`  Unique: `{ana['unique_clicks']:,}`\n"
            f"📈 CTR: `{ana['ctr']}%`  Unique CTR: `{ana['unique_ctr']}%`\n"
            f"💰 Earned: `₹{ana['earned']}`  RPM: `₹{ana['rpm']}`/1K\n"
            f"📅 Today: `{ana['today_imp']}` impressions\n\n"
            f"🎯 Target: {target_line}\n"
            f"⚖️ Weight:`{ana['weight']}`  Cap:`{ana['daily_cap'] or 'Unlimited'}`\n"
            f"⏰ Schedule:`{ana['schedule'] or 'Anytime'}`\n\n"
            f"**🩺 Health:**\n{h_block}\n\n"
            f"**📅 Last 7 Days:**\n{chart}",
            buttons=[
                [Button.inline("✏️ Edit",      f"ads_edit_menu|{ad_id}".encode()),
                 Button.inline("🔙 Ad Detail", f"ads_detail|{ad_id}".encode())],
                [Button.inline("📋 All Ads",   b"ads_list_all")]
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# DELETE
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"addel\\|(.+)"))
async def ad_delete_confirm(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ad    = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌ Ad nahi mila!", alert=True)
    try:
        await event.edit(
            f"🗑 **Delete?**\n\n**{ad.get('title','?')}**  ·  {AE.AD_TYPES.get(ad.get('type',''),'')}\n\n"
            f"⚠️ Permanently delete ho jaayega!",
            buttons=[[Button.inline("✅ Haan Delete", f"addelok|{ad_id}".encode()),
                      Button.inline("🔙 Cancel",       f"ads_detail|{ad_id}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"addelok\\|(.+)"))
async def ad_delete_ok(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    AE.delete_ad(event.data.decode().split("|")[1])
    await event.answer("🗑 Deleted!")
    await ads_list_all(event)


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS — preset buttons for all numeric fields
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"ads_settings"))
async def ads_settings(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = AE._cfg()
    try:
        await event.edit(
            "⚙️ **ADS SETTINGS**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🔄 Rotation: **{cfg.get('rotation','weighted')}**\n"
            f"📢 Banner freq: har **{cfg.get('banner_freq',1)}** opens\n"
            f"📣 Popup freq:  har **{cfg.get('popup_freq',8)}** opens\n"
            f"⏱ Popup cooldown: **{cfg.get('popup_cooldown',1800)//60}** min\n"
            f"⏰ Blast interval: **{cfg.get('blast_interval',21600)//3600}** ghante\n"
            f"📦 Blast batch:   **{cfg.get('blast_batch',30)}** users\n"
            f"💎 Skip premium: **{'✅' if cfg.get('skip_premium') else '❌'}**",
            buttons=[
                [Button.inline("🔄 Rotation",       b"ads_set_rotation")],
                [Button.inline("📢 Banner Freq",     b"adscfg_q|banner_freq"),
                 Button.inline("📣 Popup Freq",      b"adscfg_q|popup_freq")],
                [Button.inline("⏱ Popup Cooldown",  b"adscfg|popup_cooldown_min"),
                 Button.inline("⏰ Blast Interval",  b"adscfg|blast_interval_hr")],
                [Button.inline("📦 Blast Batch",     b"adscfg_q|blast_batch"),
                 Button.inline(f"💎 Prem Skip: {'ON' if cfg.get('skip_premium') else 'OFF'}",
                               b"adscfg_toggle_prem")],
                [Button.inline("🔙 Back",             b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adscfg_q\\|(.+)"))
async def adscfg_quick(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    key = event.data.decode().split("|")[1]
    cfg = AE._cfg()
    presets = {
        "banner_freq": {"label":"📢 Banner Frequency","hint":"Har N menu opens mein 1 banner",
                        "cur": cfg.get("banner_freq",1),"vals":[1,2,3,5,8,10,15,20]},
        "popup_freq":  {"label":"📣 Popup Frequency","hint":"Har N opens mein 1 popup",
                        "cur": cfg.get("popup_freq",8),"vals":[3,5,8,10,15,20,30,50]},
        "blast_batch": {"label":"📦 Blast Batch Size","hint":"Ek batch mein kitne users",
                        "cur": cfg.get("blast_batch",30),"vals":[10,20,30,50,100,200]},
    }
    info = presets.get(key)
    if not info:
        event.data = f"adscfg|{key}".encode()
        return await adscfg_set(event)
    cur  = info["cur"]
    vals = info["vals"]
    btn_rows = []
    for i in range(0, len(vals), 4):
        row = [Button.inline(f"{'✅ ' if v==cur else ''}{v}", f"adscfg_sv|{key}|{v}".encode())
               for v in vals[i:i+4]]
        btn_rows.append(row)
    btn_rows.append([Button.inline("✏️ Custom", f"adscfg|{key}".encode()),
                     Button.inline("🔙 Settings", b"ads_settings")])
    try:
        await event.edit(
            f"⚙️ **{info['label']}**\n_{info['hint']}_\n\nCurrent: **{cur}**\n\nPreset chuniye:",
            buttons=btn_rows
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adscfg_sv\\|(.+)\\|(.+)"))
async def adscfg_set_val(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    key, val = parts[1], int(parts[2])
    AE._cfg()[key] = val; AE._save()
    await event.answer(f"✅ {key}={val} saved!")
    await ads_settings(event)


@bot.on(events.CallbackQuery(data=b"ads_set_rotation"))
async def ads_set_rotation(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cur  = AE._cfg().get("rotation","weighted")
    btns = [[Button.inline(f"{'✅ ' if cur==k else ''}{v}", f"adscfg_rot|{k}".encode())]
            for k,v in AE.ROTATION.items()]
    btns.append([Button.inline("🔙 Back", b"ads_settings")])
    try:
        await event.edit("🔄 **Rotation Strategy:**", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adscfg_rot\\|(.+)"))
async def adscfg_rot(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    AE._cfg()["rotation"] = event.data.decode().split("|")[1]; AE._save()
    await event.answer("✅ Set!")
    await ads_settings(event)


@bot.on(events.CallbackQuery(data=b"adscfg_toggle_prem"))
async def adscfg_toggle_prem(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = AE._cfg(); cfg["skip_premium"] = not cfg.get("skip_premium",True); AE._save()
    await ads_settings(event)


@bot.on(events.CallbackQuery(pattern=b"adscfg\\|(.+)"))
async def adscfg_set(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    key = event.data.decode().split("|")[1]
    cfg = AE._cfg()
    hints = {
        "banner_freq":       ("📢 Banner frequency","Har N opens mein 1",cfg.get("banner_freq",1)),
        "popup_freq":        ("📣 Popup frequency","Har N opens mein 1",cfg.get("popup_freq",8)),
        "popup_cooldown_min":(("⏱ Popup cooldown","Minutes mein",cfg.get("popup_cooldown",1800)//60)),
        "blast_interval_hr": ("⏰ Blast interval","Ghante mein (0=off)",cfg.get("blast_interval",21600)//3600),
        "blast_batch":       ("📦 Blast batch","Users per batch",cfg.get("blast_batch",30)),
    }
    lbl, hint, cur = hints.get(key,(key,"",0))
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "adscfg_val"; d["adscfg_key"] = key
    d["step_since"] = time.time()
    try:
        await event.edit(
            f"⚙️ **{lbl}**\n_{hint}_\n\nCurrent: `{cur}`\n\nNaya number bhejo:",
            buttons=[[Button.inline("🔙 Back",b"ads_settings"),Button.inline("❌ Cancel",b"ads_settings")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage())
async def adscfg_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "adscfg_val": return
    key = d.get("adscfg_key",""); text = event.raw_text.strip()
    if text.lower() in ("/cancel","cancel"):
        d["step"]=None; d.pop("adscfg_key",None); save_persistent_db()
        return await event.respond("❌ Cancelled.",buttons=[[Button.inline("⚙️ Settings",b"ads_settings")]])
    try:
        val = int(text); assert val >= 0
        cfg_key = key
        if key == "popup_cooldown_min": cfg_key = "popup_cooldown"; val *= 60
        elif key == "blast_interval_hr": cfg_key = "blast_interval"; val *= 3600
        AE._cfg()[cfg_key] = val; AE._save()
        d["step"]=None; d.pop("adscfg_key",None); save_persistent_db()
        await event.respond("✅ Saved!",buttons=[[Button.inline("⚙️ Settings",b"ads_settings"),Button.inline("📊 Dashboard",b"ads_panel")]])
    except:
        await event.respond("❌ Positive number daalo!",buttons=[[Button.inline("❌ Cancel",b"ads_settings")]])


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS LEADERBOARD
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"ads_analytics"))
async def ads_analytics(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ads = AE.list_ads()
    if not ads:
        return await event.edit("📊 Koi data nahi!",
            buttons=[[Button.inline("➕ New Ad",b"ads_create_type"),Button.inline("🔙 Back",b"ads_panel")]])

    sorted_ads = sorted(ads, key=lambda a: a.get("earned",0), reverse=True)
    max_earn   = max((a.get("earned",0) for a in sorted_ads), default=1)
    e          = AE.get_earnings()
    today      = time.strftime("%Y-%m-%d")
    today_imp  = sum(a.get("daily_impressions",{}).get(today,0) for a in ads)

    lines = [
        "📊 **ADS ANALYTICS LEADERBOARD**",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"Today: `{today_imp}` imp  Total: `{e['total_impressions']:,}` imp  eCPM: `₹{e['ecpm']}`",
        "",
    ]
    for i, ad in enumerate(sorted_ads[:8], 1):
        ana    = AE.get_ad_analytics(ad["id"])
        health = "⚠️" if AE.get_ad_health(ad["id"]) else "  "
        lines.append(
            f"`{i}.` {health}**{ad['title'][:18]}**\n"
            f"   {_bar(ad.get('earned',0), max_earn, 8)} `₹{ana['earned']}`  "
            f"CTR:`{ana['ctr']}%`  👥`{ana['unique_users']}`"
        )
    lines.append(f"\n**Total: ₹{e['total_earned']}  Pending: ₹{e['pending_payout']}**")

    try:
        await event.edit("\n".join(lines), buttons=[[Button.inline("🔙 Dashboard",b"ads_panel")]])
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# A/B TESTING
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"ads_ab_menu"))
async def ads_ab_menu(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    tests = AE._cfg().get("ab_tests", {})
    if not tests:
        return await event.edit(
            "🔬 **A/B Tests**\n\nKoi test nahi.\n\n"
            "**Kaise:** 2 same-type ads banao → Ad detail → 🔬 A/B Test",
            buttons=[[Button.inline("📋 All Ads",b"ads_list_all")],
                     [Button.inline("🔙 Back",   b"ads_panel")]]
        )
    btns = [[Button.inline(f"🔬 {t.get('name','?')[:30]}", f"ab_results|{tid}".encode())]
            for tid,t in tests.items()]
    btns.append([Button.inline("🔙 Back",b"ads_panel")])
    try:
        await event.edit("🔬 **A/B Tests:**", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adab_start\\|(.+)"))
async def adab_start(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id_a = event.data.decode().split("|")[1]
    ad_a    = AE.get_ad(ad_id_a)
    if not ad_a: return await event.answer("❌ Ad nahi mila!", alert=True)
    same_type_ads = [a for a in AE.list_ads() if a["id"] != ad_id_a and a.get("type") == ad_a.get("type")]
    if not same_type_ads:
        return await event.answer("❌ Same type ka koi aur ad nahi!", alert=True)
    btns = [[Button.inline(a["title"][:28], f"adab_pair|{ad_id_a}|{a['id']}".encode())]
            for a in same_type_ads[:8]]
    btns.append([Button.inline("🔙 Ad Detail", f"ads_detail|{ad_id_a}".encode())])
    try:
        await event.edit(f"🔬 **A/B Test**\nA: **{ad_a['title']}**\n\nVariant B chuniye:", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adab_pair\\|(.+)\\|(.+)"))
async def adab_pair(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    a, b  = parts[1], parts[2]
    ad_a  = AE.get_ad(a); ad_b = AE.get_ad(b)
    if not ad_a or not ad_b: return await event.answer("❌ Ad nahi mila!", alert=True)
    test_id = AE.create_ab_test(a, b, name=f"{ad_a['title'][:12]} vs {ad_b['title'][:12]}")
    await event.answer("✅ A/B Test started!")
    await _show_ab_results(event, test_id)


@bot.on(events.CallbackQuery(pattern=b"ab_results\\|(.+)"))
async def ab_results_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    await _show_ab_results(event, event.data.decode().split("|")[1])


async def _show_ab_results(event, test_id: str):
    r = AE.get_ab_results(test_id)
    if not r:
        return await event.edit("❌ Test nahi mila!",
            buttons=[[Button.inline("🔙",b"ads_ab_menu")]])
    a, b = r["variant_a"], r["variant_b"]
    try:
        await event.edit(
            f"🔬 **A/B Test: {r['name']}**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**A: {a['title']}**\n"
            f"  👁`{a['impressions']}` 🖱`{a['clicks']}` CTR:`{a['ctr']}%` ₹`{a['earned']}`\n\n"
            f"**B: {b['title']}**\n"
            f"  👁`{b['impressions']}` 🖱`{b['clicks']}` CTR:`{b['ctr']}%` ₹`{b['earned']}`\n\n"
            f"**🏆 Winner:** {r['winner']}",
            buttons=[
                [Button.inline("🔄 Refresh",   f"ab_results|{test_id}".encode())],
                [Button.inline("🔙 A/B Menu",  b"ads_ab_menu"),
                 Button.inline("📊 Dashboard", b"ads_panel")]
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# PAYOUT
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"ads_payout_menu"))
async def ads_payout_menu(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    e   = AE.get_earnings()
    log = AE._cfg().get("payout_log",[])[-5:]
    history = "\n".join(
        f"  `{time.strftime('%d/%m', time.localtime(p['t']))}` — ₹{p['amount']} {p.get('note','')}"
        for p in reversed(log)
    ) or "  (Koi record nahi)"
    try:
        await event.edit(
            f"💸 **PAYOUT MANAGEMENT**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💼 Total Kamaya: **₹{e['total_earned']}**\n"
            f"⏳ Pending:      **₹{e['pending_payout']}**\n"
            f"✅ Paid Out:      ₹{e['paid_out']}\n\n"
            f"**Recent Payouts:**\n{history}",
            buttons=[
                [Button.inline("💸 Payout Record Karo", b"ads_do_payout")],
                [Button.inline("🔙 Back",                b"ads_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"ads_do_payout"))
async def ads_do_payout(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    get_user_data(event.sender_id)["step"] = "ads_payout_amt"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "💸 **Payout Amount daalo:**\n\nFormat: `500` ya `500 UPI se`\n\n_/cancel = wapas_",
            buttons=[[Button.inline("🔙 Cancel",b"ads_payout_menu")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage())
async def ads_payout_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "ads_payout_amt": return
    text  = event.raw_text.strip()
    if text.lower() in ("/cancel","cancel"):
        d["step"] = None; save_persistent_db()
        return await event.respond("❌ Cancelled.",buttons=[[Button.inline("💸 Payout",b"ads_payout_menu")]])
    parts = text.split(None,1)
    try:
        amt  = float(parts[0]); assert amt > 0
        note = parts[1] if len(parts)>1 else ""
        AE.mark_payout(amt, note); d["step"]=None; save_persistent_db()
        await event.respond(f"✅ **₹{amt:.2f} payout recorded!**  {note or ''}",
            buttons=[[Button.inline("💸 Payout",b"ads_payout_menu"),Button.inline("📊 Dashboard",b"ads_panel")]])
    except:
        await event.respond("❌ Format: `500` ya `500 UPI se`",
            buttons=[[Button.inline("❌ Cancel",b"ads_payout_menu")]])



# ─────────────────────────────────────────────────────────────────────────────
# AD PREVIEW / TEST SEND
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"ad_preview\\|(.+)"))
async def ad_preview(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ad    = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌ Ad nahi mila!", alert=True)
    type_name = AE.AD_TYPES.get(ad.get("type","banner"), ad.get("type",""))
    try:
        await event.edit(
            "👁 **AD PREVIEW & TEST**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**Ad:** {ad['title']}\n"
            f"**Type:** {type_name}\n\n"
            "👁 **Preview** — is message mein exact render dekhoge\n"
            "📤 **Test Send** — tumhare chat pe real message jaayega\n"
            "📢 **Banner Preview** — main menu mein kaisa lagega\n\n"
            "_Preview se koi impression record nahi hoga_",
            buttons=[
                [Button.inline("👁 Inline Preview",        f"adprev_inline|{ad_id}".encode())],
                [Button.inline("📤 Test Send (mere chat)", f"adprev_send|{ad_id}".encode())],
                [Button.inline("📢 Banner Text Preview",   f"adprev_banner|{ad_id}".encode())],
                [Button.inline("🔙 Ad Detail",              f"ads_detail|{ad_id}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adprev_inline\\|(.+)"))
async def adprev_inline(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id   = event.data.decode().split("|")[1]
    ad      = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌", alert=True)
    ad_type = ad.get("type", "banner")
    sponsor = f"\n_Presented by: {ad['sponsor']}_" if ad.get("sponsor") else ""

    if ad_type == "banner":
        preview_txt = (
            "🏠 **[Tumhara Menu]**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ Forwarding chal raha hai\n\n"
            "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
            f"📣 _Sponsored_{sponsor}\n"
            f"{ad['text']}"
            + (f"\n[{ad.get('btn_label','Dekhiye →')}]({ad['url']})" if ad.get("url") else "")
        )
    else:
        preview_txt = (
            "📣 **Sponsored Message**\n"
            "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
            f"{ad['text']}{sponsor}"
        )

    btns = []
    if ad.get("url") and ad.get("btn_label"):
        btns.append([Button.inline(f"👉 {ad['btn_label']}  (preview — not trackable)", b"ad_prev_noop")])
    btns.append([Button.inline("🔙 Back", f"ad_preview|{ad_id}".encode())])

    try:
        await event.edit(
            "👁 **INLINE PREVIEW** _(koi impression nahi)_\n\n"
            "══════════════════════\n"
            f"{preview_txt}\n"
            "══════════════════════",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adprev_send\\|(.+)"))
async def adprev_send(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ad    = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌", alert=True)
    uid = event.sender_id
    sponsor = f"\n_Presented by: {ad['sponsor']}_" if ad.get("sponsor") else ""
    text = (
        "📣 **Sponsored Message**\n"
        "┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n\n"
        f"{ad['text']}{sponsor}"
    )
    btns = [[Button.inline("✕ Dismiss (test)", b"ad_dismiss")]]
    if ad.get("url") and ad.get("btn_label"):
        token = AE.generate_click_token(ad_id, ad["url"])
        btns  = [
            [Button.inline(f"👉 {ad['btn_label']}", f"adclick|{token}|{uid}".encode())],
            [Button.inline("✕ Dismiss (test)", b"ad_dismiss")],
        ]
    await bot.send_message(uid, text, buttons=btns)
    await event.answer("📤 Test ad bheja!", alert=False)
    try:
        await event.edit(
            "✅ **Test ad tumhare chat pe bhej diya!**\n\n"
            "Upar wala message dekho ☝️\n\n"
            "Click button bhi test kar sakte ho — real URL khulega\n"
            "_Note: Test se impression count nahi badha_",
            buttons=[
                [Button.inline("📤 Dobara Send",  f"adprev_send|{ad_id}".encode())],
                [Button.inline("🔙 Preview Menu", f"ad_preview|{ad_id}".encode())],
                [Button.inline("📊 Ad Detail",    f"ads_detail|{ad_id}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adprev_banner\\|(.+)"))
async def adprev_banner(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ad_id = event.data.decode().split("|")[1]
    ad    = AE.get_ad(ad_id)
    if not ad: return await event.answer("❌", alert=True)
    sponsor_line = f"\n_— {ad['sponsor']}_" if ad.get("sponsor") else ""
    cta = ""
    if ad.get("url"):
        cta = f"\n[{ad.get('btn_label') or 'Dekhiye →'}]({ad['url']})"
    banner = (
        "\n\n┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄\n"
        f"📣 _Sponsored_{sponsor_line}\n"
        f"{ad['text']}{cta}"
    )
    try:
        await event.edit(
            "📢 **BANNER PREVIEW**\n"
            "_Menu ke neeche exactly aisa dikhega:_\n\n"
            "──────────────────────\n"
            "🏠 **Main Menu**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚡ Forwarding chal raha hai  Aaj: `23↑` `2✗`\n"
            "📥`5` 📤`3`  💎 7d"
            f"{banner}\n"
            "──────────────────────",
            buttons=[
                [Button.inline("📤 Test Send", f"adprev_send|{ad_id}".encode())],
                [Button.inline("🔙 Back",       f"ad_preview|{ad_id}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"ad_prev_noop"))
async def ad_prev_noop(event):
    await event.answer()
    await event.answer("💡 Preview mode — real ad mein click trackable hoga!", alert=True)


# ─────────────────────────────────────────────────────────────────────────────
# CLICK TRACKING + DISMISS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"adclick\\|.+"))
async def adclick_cb(event):
    await event.answer()
    parts   = event.data.decode().split("|")
    token   = parts[1] if len(parts) > 1 else ""
    # SECURITY FIX: Always use the actual sender — never trust user_id from callback data
    # (callback data is client-controlled and can be spoofed to fake clicks for other users)
    user_id = event.sender_id
    url     = AE.resolve_click(token, user_id)
    if url:
        await event.answer(url, url=True)
    else:
        await event.answer("🔗 Link expired — menu refresh karo!", alert=False)


@bot.on(events.CallbackQuery(data=b"ad_dismiss"))
async def ad_dismiss(event):
    await event.answer()
    try:
        await event.message.delete()
    except Exception:
        await event.answer("✕ Dismissed", alert=False)
