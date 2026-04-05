"""
promo_menu.py — Promotion & Sponsorship Platform v3.0

PUBLIC (User-facing):
  /promote OR main menu "📣 Advertise" button
  → Professional sponsor onboarding flow
  → Package showcase, guided campaign builder, status tracking

ADMIN:
  /admin → 💰 Monetization Hub → 📣 Sponsor Campaigns

Changes v3.0:
  ✅ Complete redesign of public advertise flow
  ✅ Professional landing page with reach stats
  ✅ Step-by-step campaign builder (category → format → details → confirm)
  ✅ Package browser with full details
  ✅ FAQ section for sponsors
  ✅ Campaign status tracking for submitted inquiries
  ✅ Direct contact + rate card buttons
  ✅ Admin: cmp_edit fully implemented
  ✅ Admin: pkgwiz advance bug fixed
  ✅ Admin: skip buttons, duration presets, bulk mark all
  ✅ Admin: promo_ratecard_tpl, separate payout button
"""

import time
from telethon import events, Button, errors
from config import bot
from admin import is_admin
import promo_engine as PE

# ─── Helpers ─────────────────────────────────────────────────────────────────
def _fmt(v):    return f"₹{v:,.0f}" if v == int(v) else f"₹{v:,.2f}"
def _ts(ts):    return time.strftime("%d %b %Y", time.localtime(ts)) if ts else "—"
def _ts_short(ts): return time.strftime("%d/%m", time.localtime(ts)) if ts else "—"
def _bar(v, mx, w=10):
    f = round(v / max(1, mx) * w)
    return "█" * f + "░" * (w - f)
def _adm(e):
    if not is_admin(e.sender_id): raise PermissionError
def _status_icon(s):
    return {"active":"🟢","paused":"⏸","pending_payment":"⏳",
            "draft":"📝","expired":"⌛","rejected":"❌","completed":"✅"}.get(s,"❓")
def _status_text(s):
    return {"active":"🟢 Live chal raha hai","paused":"⏸ Paused hai","pending_payment":"⏳ Payment awaited",
            "draft":"📝 Approval pending","expired":"⌛ Expired","rejected":"❌ Rejected","completed":"✅ Completed"}.get(s,"❓ Unknown")

def _clear_wiz(d: dict):
    d["step"] = None
    for k in ("pkgwiz", "cmpwiz", "cmp_paid_cid", "pub_promo_cat", "pub_promo_pkg_id",
              "pub_promo_format", "pub_promo_step", "pub_promo_data",
              "cedit_field", "cedit_cid"):
        d.pop(k, None)


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC SECTION — User-facing Advertise Flow
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# LANDING PAGE  (pub_promo_intro / /promote command)
# ─────────────────────────────────────────────────────────────────────────────

_FORMAT_LABELS = {
    "banner":  ("📢", "Banner Ad",    "Menu ke neeche show hota hai"),
    "button":  ("🔘", "Button Ad",    "Menu mein clickable sponsor button"),
    "popup":   ("📣", "Pop-up Ad",    "Users ko dedicated message milta hai"),
    "blast":   ("⏰", "Blast",        "Ek baar SARE users ko message"),
}

async def _show_landing(event, edit: bool = True):
    """Professional landing page for sponsors."""
    cfg  = PE._cfg()
    if not cfg.get("enabled", True):
        txt = (
            "📣 **Advertise Here**\n\n"
            "Abhi promotional campaigns temporarily unavailable hain.\n\n"
            "Jald hi wapas aayenge! Baad mein try karo. 🙏"
        )
        btns = [[Button.inline("🏠 Main Menu", b"main_menu")]]
        if edit:
            return await event.edit(txt, buttons=btns)
        return await event.respond(txt, buttons=btns)

    # Reach stats
    s    = PE.get_promo_summary()
    pkgs = PE.list_packages()

    # Build stats line
    try:
        from database import GLOBAL_STATE
        user_count = len(GLOBAL_STATE.get("users", {}))
    except Exception:
        user_count = 0
    reach_line = f"👥 `{user_count:,}+` active users" if user_count > 0 else "👥 Growing active user base"

    # Build package preview (top 3)
    pkg_lines = ""
    if pkgs:
        pkg_lines = "\n**📦 Popular Packages:**\n"
        for p in pkgs[:3]:
            pop_star = " ⭐" if p.get("popular") else ""
            mode_short = {"banner":"Banner","button":"Button","popup":"Pop-up","blast":"Blast"}.get(p.get("delivery_mode",""), "Ad")
            pkg_lines += f"• **{p['name']}**{pop_star} — {_fmt(p.get('flat_price',0))} · {p.get('duration_days')}d · {mode_short}\n"
        pkg_lines += "\n"

    contact = cfg.get("contact_info", "")
    contact_line = f"\n📞 **Direct Contact:** {contact}" if contact else ""

    text = (
        "📣 **ADVERTISE ON OUR BOT**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{reach_line}  ·  📈 `{s['total_impressions']:,}+` total impressions served\n\n"
        "**✨ Kyun advertise karo yahan?**\n"
        "🎯 Highly engaged, niche audience — real users, no bots\n"
        "⚡ Campaigns 24 ghante mein live ho jaate hain\n"
        "📊 Real-time tracking — impressions, clicks, CTR\n"
        "💬 Dedicated support & campaign optimization\n"
        "🔄 Pause/resume/extend — full control\n"
        f"{pkg_lines}"
        "**💰 Kya promote kar sakte ho?**\n"
        "Telegram channels · YouTube · Products · Apps\n"
        "Brands · Events · Services · Kuch bhi!\n"
        f"{contact_line}"
    )

    btns = [
        [Button.inline("🚀 Campaign Shuru Karo", b"pub_promo_start")],
        [Button.inline("📦 Sare Packages Dekho", b"pub_promo_packages"),
         Button.inline("❓ FAQ",                 b"pub_promo_faq")],
        [Button.inline("📊 Meri Inquiries",       b"pub_promo_myinq")],
        [Button.inline("🏠 Main Menu",             b"main_menu")],
    ]
    if edit:
        try:
            await event.edit(text, buttons=btns)
        except errors.MessageNotModifiedError:
            pass
        except Exception: await event.respond(text, buttons=btns)
    else:
        await event.respond(text, buttons=btns)


@bot.on(events.NewMessage(pattern='/promote'))
async def promote_cmd(event):
    if not event.is_private: return
    await _show_landing(event, edit=False)


@bot.on(events.CallbackQuery(data=b"pub_promo_intro"))
async def pub_promo_intro(event):
    await event.answer()
    await _show_landing(event, edit=True)


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC — PACKAGE BROWSER
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"pub_promo_packages"))
async def pub_promo_packages(event):
    await event.answer()
    cfg  = PE._cfg()
    pkgs = PE.list_packages()
    if not pkgs:
        return await event.edit(
            "📦 **Packages**\n\nAbhi koi package available nahi hai.\n\n"
            "Admin se directly contact karo ya inquiry bhejo!",
            buttons=[
                [Button.inline("📩 Inquiry Bhejo", b"pub_promo_start")],
                [Button.inline("🔙 Back",           b"pub_promo_intro")],
            ]
        )

    lines = ["📦 **AVAILABLE PACKAGES**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"]
    for p in pkgs:
        mode  = PE.DELIVERY_MODES.get(p.get("delivery_mode",""), "Ad")
        price_model = PE.PRICING_MODELS.get(p.get("pricing_model","flat"), "")
        pop   = " ⭐ **POPULAR**" if p.get("popular") else ""
        max_imp = f"  👁 Max: {p['max_impressions']:,}" if p.get("max_impressions") else "  👁 Impressions: Unlimited"
        lines.append(
            f"{'━' * 20}\n"
            f"**{p['name']}**{pop}\n"
            f"💰 {_fmt(p.get('flat_price',0))}  ·  📅 {p.get('duration_days')} days\n"
            f"📺 Format: {mode}\n"
            f"{max_imp}\n"
            + (f"  📝 {p['description']}\n" if p.get("description") else "")
        )

    btns = [
        [Button.inline(f"{'⭐ ' if p.get('popular') else ''}{p['name'][:20]} — {_fmt(p.get('flat_price',0))}",
                        f"pub_promo_bookpkg|{p['id']}".encode())]
        for p in pkgs[:6]
    ]
    btns += [
        [Button.inline("🚀 Campaign Shuru Karo", b"pub_promo_start")],
        [Button.inline("🔙 Back",                 b"pub_promo_intro")],
    ]
    contact = cfg.get("contact_info","")
    if contact:
        lines.append(f"\n📞 **Direct Contact:** {contact}")

    try:
        await event.edit("\n".join(lines), buttons=btns)
    except errors.MessageTooLongError:
        try:
            await event.edit("\n".join(lines)[:3000] + "\n...", buttons=btns)
        except errors.MessageNotModifiedError:
            pass


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC — FAQ
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"pub_promo_faq"))
async def pub_promo_faq(event):
    await event.answer()
    try:
        await event.edit(
            "❓ **FREQUENTLY ASKED QUESTIONS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    
            "**1. Inquiry karne ke baad kya hota hai?**\n"
            "📩 Inquiry submit hone ke 24 ghante mein admin contact karta hai payment details ke saath.\n\n"
    
            "**2. Payment kab karni padegi?**\n"
            "💳 Admin confirm karne ke baad payment ka tarika bataya jaayega (UPI/bank/etc).\n\n"
    
            "**3. Campaign kab live hoga?**\n"
            "⚡ Payment confirm hote hi — usually 24 ghante mein. Rush processing bhi available hai.\n\n"
    
            "**4. Kya main apna ad text khud likh sakta hun?**\n"
            "✍️ Haan! Inquiry mein apna exact promo text, link, aur button label mention karo.\n\n"
    
            "**5. Kya performance reports milenge?**\n"
            "📊 Haan — impressions, clicks, CTR daily track hota hai. Admin share karta hai.\n\n"
    
            "**6. Campaign pause/extend kar sakte hain?**\n"
            "⏸ Haan — active campaign pause, resume, ya extend kar sakte ho. Admin se baat karo.\n\n"
    
            "**7. Kya guarantee hai?**\n"
            "✅ Minimum impressions guarantee hoti hai (package ke hisaab se). Agar nahi mili to extra time free.\n\n"
    
            "**8. Custom package chahiye?**\n"
            "🤝 Bilkul! Inquiry mein 'Custom Budget' choose karo aur apni requirements likho.",
    
            buttons=[
                [Button.inline("🚀 Campaign Shuru Karo", b"pub_promo_start")],
                [Button.inline("🔙 Back",                 b"pub_promo_intro")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC — CAMPAIGN BUILDER (4-step guided flow)
# Step 1: Category → Step 2: Format/Package → Step 3: Details → Step 4: Confirm
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"pub_promo_start"))
async def pub_promo_start(event):
    """Step 1: What to promote (category)."""
    await event.answer()
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "pub_promo_cat"
    d["step_since"] = time.time()
    d["pub_promo_data"] = {}

    btns = [
        [Button.inline("📣 Telegram Channel/Group", b"pub_cat|channel"),
         Button.inline("📱 App / Website",          b"pub_cat|app")],
        [Button.inline("🛍 Product / Deal",          b"pub_cat|product"),
         Button.inline("🏢 Brand / Service",         b"pub_cat|company")],
        [Button.inline("▶️ YouTube / Video",         b"pub_cat|youtube"),
         Button.inline("📸 Social Media",            b"pub_cat|social")],
        [Button.inline("📅 Event / Webinar",         b"pub_cat|event"),
         Button.inline("✨ Kuch Aur",                b"pub_cat|custom")],
        [Button.inline("🔙 Back",                     b"pub_promo_intro")],
    ]
    try:
        await event.edit(
            "🚀 **CAMPAIGN BUILDER** — Step 1/4\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "**Kya promote karna hai?**\n\n"
            "Apni category choose karo 👇",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pub_cat\\|(.+)"))
async def pub_cat_cb(event):
    """Step 2: Choose ad format or package."""
    await event.answer()
    uid = event.sender_id
    cat = event.data.decode().split("|")[1]
    from database import get_user_data
    d = get_user_data(uid)
    d["pub_promo_data"] = d.get("pub_promo_data") or {}
    d["pub_promo_data"]["category"] = cat
    cat_label = PE.PROMO_CATEGORIES.get(cat, cat)

    # Check if packages are available
    pkgs = PE.list_packages()

    text = (
        f"🚀 **CAMPAIGN BUILDER** — Step 2/4\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📂 Category: **{cat_label}**\n\n"
        f"**Ad Format chuniye:**\n\n"
        f"📢 **Banner** — Menu ke neeche subtle text block\n"
        f"   _Best for: awareness, low intrusion_\n\n"
        f"🔘 **Button** — Menu mein dedicated sponsor button\n"
        f"   _Best for: direct clicks, app downloads_\n\n"
        f"📣 **Pop-up** — Users ko alag dedicated message\n"
        f"   _Best for: rich content, max impact_\n\n"
        f"⏰ **Blast** — Ek baar SABHI active users ko message\n"
        f"   _Best for: launches, time-sensitive offers_\n\n"
        + ("📦 **Ya koi package choose karo:**" if pkgs else "")
    )

    btns = [
        [Button.inline("📢 Banner",  b"pub_fmt|banner"),
         Button.inline("🔘 Button",  b"pub_fmt|button")],
        [Button.inline("📣 Pop-up",  b"pub_fmt|popup"),
         Button.inline("⏰ Blast",   b"pub_fmt|blast")],
    ]
    if pkgs:
        for p in pkgs[:3]:
            pop = "⭐ " if p.get("popular") else ""
            btns.append([Button.inline(
                f"{pop}{p['name']} — {_fmt(p.get('flat_price',0))} · {p.get('duration_days')}d",
                f"pub_bookpkg|{p['id']}".encode()
            )])
    btns += [
        [Button.inline("💬 Custom Budget", b"pub_fmt|custom")],
        [Button.inline("🔙 Back",           b"pub_promo_start")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pub_fmt\\|(.+)"))
async def pub_fmt_cb(event):
    """Step 3: Fill campaign details."""
    await event.answer()
    uid = event.sender_id
    fmt = event.data.decode().split("|")[1]
    from database import get_user_data
    d = get_user_data(uid)
    d["pub_promo_data"] = d.get("pub_promo_data") or {}
    d["pub_promo_data"]["format"] = fmt
    d["step"] = "pub_promo_details"
    d["step_since"] = time.time()

    fmt_info = _FORMAT_LABELS.get(fmt, ("✨", fmt.title(), ""))
    cat_label = PE.PROMO_CATEGORIES.get(d["pub_promo_data"].get("category",""), "")

    try:
        await event.edit(
            f"🚀 **CAMPAIGN BUILDER** — Step 3/4\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📂 {cat_label}  ·  {fmt_info[0]} {fmt_info[1]}\n\n"
            f"**Ek message mein ye details likho:**\n\n"
            f"1️⃣ **Kya promote karna hai?** (naam, product, channel link)\n"
            f"2️⃣ **Promo text kya dikhana hai?** (users ko kya message milega)\n"
            f"3️⃣ **Click hone par kahan jaayega?** (URL, @channel, etc)\n"
            f"4️⃣ **Duration chahiye?** (days mein)\n"
            f"5️⃣ **Budget?** (ya 'package se' likh do)\n"
            f"6️⃣ **Contact info** (WhatsApp/Telegram — follow-up ke liye)\n\n"
            f"_Example: 'Mera channel @TechDeals hai. Text: Join karo, daily deals! Link: @TechDeals. 15 din. Budget ₹500. Contact: @myusername'_\n\n"
            f"👇 Ab message type karo:",
            buttons=[
                [Button.inline("🔙 Back", b"pub_promo_start"),
                 Button.inline("❌ Cancel", b"pub_promo_intro")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pub_bookpkg\\|(.+)"))
async def pub_bookpkg_cb(event):
    """User chose a specific package — go directly to details step."""
    await event.answer()
    uid    = event.sender_id
    pkg_id = event.data.decode().split("|")[1]
    p      = PE.get_package(pkg_id)
    if not p:
        return await event.answer("Package nahi mila!", alert=True)
    from database import get_user_data
    d = get_user_data(uid)
    d["pub_promo_data"] = d.get("pub_promo_data") or {}
    d["pub_promo_data"]["package_id"] = pkg_id
    d["pub_promo_data"]["format"]     = p.get("delivery_mode","banner")
    d["step"] = "pub_promo_details"
    d["step_since"] = time.time()

    try:
        await event.edit(
            f"🚀 **CAMPAIGN BUILDER** — Step 3/4\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 Package: **{p['name']}**\n"
            f"💰 Price: {_fmt(p.get('flat_price',0))}  ·  📅 {p.get('duration_days')} days\n\n"
            f"**Ek message mein ye details likho:**\n\n"
            f"1️⃣ **Kya promote karna hai?** (naam, product, channel link)\n"
            f"2️⃣ **Promo text** (users ko kya message milega)\n"
            f"3️⃣ **Destination link** (URL, @channel, etc)\n"
            f"4️⃣ **Contact info** (WhatsApp/Telegram)\n\n"
            f"👇 Message type karo:",
            buttons=[
                [Button.inline("🔙 Back", b"pub_promo_start"),
                 Button.inline("❌ Cancel", b"pub_promo_intro")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pub_promo_bookpkg\\|(.+)"))
async def pub_promo_bookpkg_alias(event):
    """Package browse se direct book karo."""
    await event.answer()
    event.data = b"pub_bookpkg|" + event.data.split(b"|")[1]
    await pub_bookpkg_cb(event)


@bot.on(events.CallbackQuery(data=b"pub_fmt|custom"))
async def pub_fmt_custom(event):
    """Custom budget option."""
    await event.answer()
    uid = event.sender_id
    from database import get_user_data
    d = get_user_data(uid)
    d["pub_promo_data"] = d.get("pub_promo_data") or {}
    d["pub_promo_data"]["format"] = "custom"
    d["step"] = "pub_promo_details"
    d["step_since"] = time.time()
    try:
        await event.edit(
            "🚀 **CAMPAIGN BUILDER** — Step 3/4\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "💬 **Custom Campaign**\n\n"
            "Apni full requirements ek message mein likho:\n\n"
            "1️⃣ Kya promote karna hai?\n"
            "2️⃣ Promo text / content\n"
            "3️⃣ Link / destination\n"
            "4️⃣ Duration\n"
            "5️⃣ Budget range (e.g., '₹500-₹1000')\n"
            "6️⃣ Contact (WhatsApp/Telegram)\n"
            "7️⃣ Koi special requirements?\n\n"
            "👇 Message bhejo:",
            buttons=[
                [Button.inline("🔙 Back", b"pub_promo_start"),
                 Button.inline("❌ Cancel", b"pub_promo_intro")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage())
async def pub_promo_details_handler(event):
    """Step 4: Receive details, show confirmation."""
    if not event.is_private: return
    uid = event.sender_id
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "pub_promo_details": return

    msg = event.raw_text.strip()
    if msg.lower() in ("/cancel", "cancel"):
        _clear_wiz(d); save_persistent_db()
        return await event.respond("❌ Campaign cancelled.",
            buttons=[[Button.inline("📣 Advertise", b"pub_promo_intro"),
                      Button.inline("🏠 Menu",       b"main_menu")]])
    if len(msg) < 15:
        return await event.respond(
            "❌ Thoda detail mein likho (15+ chars)!\n\n"
            "_Kya promote karna hai, link kya hai, duration kitna chahiye, contact number — sab likho._",
            buttons=[[Button.inline("🔙 Back", b"pub_promo_start"),
                      Button.inline("❌ Cancel", b"pub_promo_intro")]]
        )

    data = d.get("pub_promo_data", {})
    cat  = data.get("category", "custom")
    fmt  = data.get("format", "banner")
    pkg_id = data.get("package_id", "")

    cat_label = PE.PROMO_CATEGORIES.get(cat, cat)
    fmt_info  = _FORMAT_LABELS.get(fmt, ("✨", fmt.title(), ""))

    # Show confirmation
    d["step"] = "pub_promo_confirm"
    d["step_since"] = time.time()
    d["pub_promo_data"]["msg"] = msg
    save_persistent_db()

    pkg_line = ""
    if pkg_id:
        p = PE.get_package(pkg_id)
        if p:
            pkg_line = f"\n📦 Package: **{p['name']}** — {_fmt(p.get('flat_price',0))}"

    await event.respond(
        "🚀 **CAMPAIGN BUILDER** — Step 4/4\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**📋 Campaign Summary — Confirm karo:**\n\n"
        f"📂 Category: **{cat_label}**\n"
        f"{fmt_info[0]} Format: **{fmt_info[1]}**{pkg_line}\n\n"
        f"**Details:**\n{msg[:300]}{'...' if len(msg)>300 else ''}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "✅ **Submit karne ke baad:**\n"
        "1. Admin 24 ghante mein contact karega\n"
        "2. Payment details dega\n"
        "3. Payment ke baad campaign live ho jaayega\n\n"
        "_Sab sahi hai?_",
        buttons=[
            [Button.inline("✅ Haan, Submit Karo!",  b"pub_promo_confirm_yes")],
            [Button.inline("✏️ Edit Karo",            b"pub_promo_start"),
             Button.inline("❌ Cancel",               b"pub_promo_intro")],
        ]
    )


@bot.on(events.CallbackQuery(data=b"pub_promo_confirm_yes"))
async def pub_promo_confirm_yes(event):
    """Final submission."""
    await event.answer()
    uid = event.sender_id
    from database import get_user_data, save_persistent_db
    import time as _t
    d   = get_user_data(uid)

    # FIX: Stale session check — agar 2 ghante se zyada purana hai to reject karo
    step_since = d.get("step_since", 0)
    if step_since and (_t.time() - step_since) > 7200:
        d["step"] = None
        d.pop("pub_promo_data", None)
        save_persistent_db()
        return await event.answer(
            "⏰ Session expire ho gaya! Dobara /promote se shuru karo.",
            alert=True
        )

    data = d.get("pub_promo_data", {})
    cat  = data.get("category", "custom")
    msg  = data.get("msg", "")
    pkg_id = data.get("package_id", "")
    fmt  = data.get("format", "banner")

    if not msg:
        return await event.answer("❌ Koi details nahi mili!", alert=True)

    # Build full message for admin
    fmt_name = _FORMAT_LABELS.get(fmt, ("", fmt.title(), ""))[1]
    cat_label = PE.PROMO_CATEGORIES.get(cat, cat)
    pkg_info  = ""
    if pkg_id:
        p = PE.get_package(pkg_id)
        if p:
            pkg_info = f"\n📦 Package selected: {p['name']} ({_fmt(p.get('flat_price',0))} · {p.get('duration_days')}d)"
    full_msg = (
        f"[{cat_label}] [{fmt_name}]{pkg_info}\n\n{msg}"
    )
    iid = PE.log_inquiry(uid, cat, full_msg)
    _clear_wiz(d); save_persistent_db()

    # Notify admins
    try:
        from database import GLOBAL_STATE
        admins = list(GLOBAL_STATE.get("admins", {}).keys())
        for admin_id in admins[:3]:
            try:
                await bot.send_message(
                    admin_id,
                    f"🔔 **New Campaign Inquiry!**\n\n"
                    f"👤 User: `{uid}`\n"
                    f"📂 Category: {cat_label}\n"
                    f"{fmt_info[0]} Format: {fmt_name}"
                    f"{pkg_info}\n\n"
                    f"**Details:**\n{msg[:600]}\n\n"
                    f"🆔 Inquiry ID: `{iid}`\n"
                    f"_Admin Panel → Promotions → Inquiries_",
                    parse_mode="md"
                )
            except Exception:
                pass
    except Exception:
        pass

    cfg     = PE._cfg()
    contact = cfg.get("contact_info", "")

    try:
        await event.edit(
            "✅ **CAMPAIGN INQUIRY SUBMIT HO GAYI!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🆔 **Inquiry ID:** `{iid}`\n\n"
            "**⏭ Aage kya hoga:**\n\n"
            "1️⃣ **24 ghante mein** — Admin review karega\n"
            "2️⃣ **Payment details** — UPI / Bank milega\n"
            "3️⃣ **Payment ke baad** — Campaign draft mein jayega\n"
            "4️⃣ **Approval pe** — 🟢 **LIVE!** Campaign shuru\n"
            "5️⃣ **Tracking** — Impressions, clicks daily milte hain\n\n"
            + (f"📞 **Direct Contact:** {contact}\n\n" if contact else "")
            + "_'Meri Inquiries' se status track kar sakte ho 👇_",
            buttons=[
                [Button.inline("📊 Meri Inquiries Track Karo", b"pub_promo_myinq")],
                [Button.inline("➕ Aur Campaign Bhejo",         b"pub_promo_start")],
                [Button.inline("🏠 Main Menu",                  b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# PUBLIC — MY INQUIRIES (status tracking)
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"pub_promo_myinq"))
async def pub_promo_myinq(event):
    """User apni submitted inquiries dekhe."""
    await event.answer()
    uid     = event.sender_id
    all_inq = PE._cfg().get("inquiry_log", [])
    my_inq  = [i for i in all_inq if i.get("user_id") == uid]

    if not my_inq:
        return await event.edit(
            "📊 **Meri Inquiries**\n\n"
            "Abhi tak koi inquiry submit nahi ki hai.\n\n"
            "Campaign shuru karo! 👇",
            buttons=[
                [Button.inline("🚀 Campaign Shuru Karo", b"pub_promo_start")],
                [Button.inline("🔙 Back",                 b"pub_promo_intro")],
            ]
        )

    lines = ["📊 **MERI CAMPAIGN INQUIRIES**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"]
    for inq in reversed(my_inq[-5:]):
        status = "✅ Handled" if inq.get("handled") else "🔔 Pending review"
        ts     = _ts_short(inq.get("ts", 0))
        cat    = PE.PROMO_CATEGORIES.get(inq.get("category",""), "")
        preview = inq.get("msg","")[:60] + "..." if len(inq.get("msg","")) > 60 else inq.get("msg","")
        lines.append(
            f"📋 **ID:** `{inq['id']}`\n"
            f"📂 {cat}  ·  {ts}\n"
            f"Status: {status}\n"
            f"_\"{preview}\"_\n"
        )

    # Check if any are in active campaigns
    camps = PE._cfg().get("campaigns", {})
    my_camps = [c for c in camps.values() if c.get("requested_by") == uid]
    if my_camps:
        lines.append("\n**📣 Active Campaigns:**")
        for c in my_camps[-3:]:
            icon = _status_icon(c.get("status",""))
            lines.append(
                f"{icon} **{c['title'][:25]}**\n"
                f"   {_status_text(c.get('status',''))}\n"
                f"   📅 Expires: {_ts(c.get('expires_at',0))}\n"
            )

    cfg = PE._cfg()
    contact = cfg.get("contact_info","")
    if contact:
        lines.append(f"\n📞 **Koi sawaal?** Contact: {contact}")

    try:
        await event.edit("\n".join(lines), buttons=[
            [Button.inline("🚀 Naya Campaign", b"pub_promo_start")],
            [Button.inline("🔙 Back",           b"pub_promo_intro")],
        ])
    except errors.MessageNotModifiedError:
        pass


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN SECTION — Sponsor Campaign Management
# ═══════════════════════════════════════════════════════════════════════════════

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — MAIN PROMO DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"promo_panel"))
async def promo_panel(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)

    s = PE.get_promo_summary()
    monthly = PE._cfg().get("monthly_revenue", {})
    months  = sorted(monthly)[-4:]
    max_rev = max(monthly.values(), default=1)
    spark   = "  ".join(f"`{m[-5:]}` {_bar(monthly.get(m,0), max_rev, 5)}" for m in months) or "—"

    alert_line = ""
    if s["pending_approval"] > 0:
        alert_line += f"\n⚠️ **{s['pending_approval']} campaigns** approval pending!"
    if s["pending_inquiries"] > 0:
        alert_line += f"\n📬 **{s['pending_inquiries']} inquiries** unanswered!"

    text = (
        "📣 **SPONSOR CAMPAIGNS PANEL**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "_Ye panel external sponsors ke campaigns manage karta hai._\n"
        "_Sponsor /promote se inquiry bhejta hai → tum manage karo._\n\n"
        f"**📊 Campaigns:**\n"
        f"  🟢 Active: `{s['active_campaigns']}`  "
        f"⏳ Pay Pending: `{s['pending_payment']}`\n"
        f"  📝 Approval: `{s['pending_approval']}`  "
        f"✅ Total: `{s['total_campaigns']}`\n\n"
        f"**📈 Performance:**\n"
        f"  👁 Impressions: `{s['total_impressions']:,}`\n"
        f"  🖱 Clicks: `{s['total_clicks']:,}`  CTR: `{s['ctr']}%`\n\n"
        f"**💰 Revenue:**\n"
        f"  📅 Is Mahine: **{_fmt(s['this_month'])}**\n"
        f"  ⏳ Pending: **{_fmt(s['pending_revenue'])}**\n"
        f"  💼 Total: {_fmt(s['total_revenue'])}\n\n"
        f"**📅 Monthly:**\n{spark}"
        f"{alert_line}"
    )
    try:
        await event.edit(text, buttons=[
            [Button.inline("📋 Campaigns",       b"promo_campaigns"),
             Button.inline("📦 Packages",        b"promo_packages")],
            [Button.inline("📬 Inquiries",       b"promo_inquiries"),
             Button.inline("📊 Analytics",       b"promo_analytics")],
            [Button.inline("⚙️ Settings",        b"promo_settings"),
             Button.inline("💸 Payout",          b"promo_payout")],
            [Button.inline("🔙 Monetization Hub", b"adm_monetize_hub")],
        ])
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# PACKAGES MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"promo_packages"))
async def promo_packages(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    pkgs = PE.list_packages(active_only=False)
    if not pkgs:
        return await event.edit(
            "📦 **Packages** — Koi package nahi hai.\n\n"
            "Packages sponsor ke liye pricing tiers hain.\n"
            "Jaise: ₹299 - 7 day banner, ₹999 - 30 day blast\n\n"
            "💡 **Quick Start:** Auto-templates se ready-made packages ek click mein bana lo!",
            buttons=[
                [Button.inline("⚡ Auto-Templates Load Karo", b"pkg_load_templates")],
                [Button.inline("➕ Package Banao",            b"pkg_create")],
                [Button.inline("🔙 Back",                     b"promo_panel")],
            ]
        )
    btns = []
    for p in pkgs[:8]:
        state = "✅" if p.get("active") else "❌"
        price = _fmt(p.get("flat_price", 0))
        pop   = " ⭐" if p.get("popular") else ""
        btns.append([Button.inline(
            f"{state}{pop} {p['name'][:18]} · {price} · {p.get('duration_days')}d",
            f"pkg_detail|{p['id']}".encode()
        )])
    btns += [
        [Button.inline("⚡ Auto-Templates",   b"pkg_load_templates"),
         Button.inline("➕ Package Banao",   b"pkg_create")],
        [Button.inline("🔙 Back",             b"promo_panel")],
    ]
    try:
        await event.edit("📦 **Pricing Packages:**\n\nPackage select karo manage karne ke liye:", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pkg_detail\\|(.+)"))
async def pkg_detail(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    pkg_id = event.data.decode().split("|")[1]
    p = PE.get_package(pkg_id)
    if not p: return await event.answer("Package nahi mila!", alert=True)
    mode  = PE.DELIVERY_MODES.get(p.get("delivery_mode",""), p.get("delivery_mode",""))
    price = PE.PRICING_MODELS.get(p.get("pricing_model",""), "")
    text = (
        f"📦 **{p['name']}**{'  ⭐ Popular' if p.get('popular') else ''}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📺 Delivery: {mode}\n"
        f"💰 Pricing: {price}\n"
        f"💵 Flat Price: {_fmt(p.get('flat_price',0))}\n"
        f"📊 CPM Rate: ₹{p.get('cpm_rate',0)}/1000\n"
        f"📅 Duration: {p.get('duration_days',7)} days\n"
        f"👁 Max Impressions: {p.get('max_impressions') or 'Unlimited'}\n"
        f"📋 Bookings: {p.get('bookings',0)}\n"
        f"💰 Total Earned: {_fmt(p.get('total_earned',0))}\n\n"
        f"📝 {p.get('description','—')}\n"
        f"Status: {'✅ Active' if p.get('active') else '❌ Inactive'}"
    )
    tog = "❌ Deactivate" if p.get("active") else "✅ Activate"
    pop_lbl = "⭐ Unmark Popular" if p.get("popular") else "⭐ Mark Popular"
    try:
        await event.edit(text, buttons=[
            [Button.inline(tog,             f"pkg_toggle|{pkg_id}".encode()),
             Button.inline(pop_lbl,         f"pkg_popular|{pkg_id}".encode())],
            [Button.inline("🗑 Delete",     f"pkg_delete|{pkg_id}".encode())],
            [Button.inline("🔙 Packages",   b"promo_packages")],
        ])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pkg_popular\\|(.+)"))
async def pkg_popular_toggle(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    pkg_id = event.data.decode().split("|")[1]
    p = PE._cfg()["packages"].get(pkg_id)
    if p:
        p["popular"] = not p.get("popular", False)
        PE._save()
    await pkg_detail(event)


@bot.on(events.CallbackQuery(pattern=b"pkg_toggle\\|(.+)"))
async def pkg_toggle(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    pkg_id = event.data.decode().split("|")[1]
    p = PE._cfg()["packages"].get(pkg_id)
    if p:
        p["active"] = not p.get("active", True)
        PE._save()
    await pkg_detail(event)


@bot.on(events.CallbackQuery(pattern=b"pkg_delete\\|(.+)"))
async def pkg_delete_confirm(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    pkg_id = event.data.decode().split("|")[1]
    p = PE.get_package(pkg_id)
    if not p: return await event.answer("Package nahi mila!", alert=True)
    try:
        await event.edit(
            f"🗑 **Package delete karna chahte ho?**\n\n"
            f"**{p['name']}** — {_fmt(p.get('flat_price',0))} · {p.get('duration_days')}d\n\n"
            f"⚠️ Permanently delete ho jaayega!",
            buttons=[
                [Button.inline("✅ Haan, Delete", f"pkg_del_ok|{pkg_id}".encode()),
                 Button.inline("🔙 Cancel",       f"pkg_detail|{pkg_id}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pkg_del_ok\\|(.+)"))
async def pkg_del_ok(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    PE.delete_package(event.data.decode().split("|")[1])
    await event.answer("🗑 Deleted!", alert=False)
    await promo_packages(event)


# ─── Package Wizard ───────────────────────────────────────────────────────────
_PKG_STEPS = [
    ("name",          "📦 **Package Name:**\ne.g., `7-Day Banner`, `Premium Blast`"),
    ("delivery_mode", None),
    ("pricing_model", None),
    ("flat_price",    "💰 **Flat Price (₹):**\ne.g., `299`"),
    ("cpm_rate",      "📊 **CPM Rate (₹/1000 impressions):** _(optional)_"),
    ("duration_days", None),
    ("description",   "📝 **Short Description:** _(optional)_"),
]
_PKG_TOTAL    = len(_PKG_STEPS)
_PKG_OPTIONAL = {"flat_price", "cpm_rate", "description"}


async def _pkgwiz_advance(event, wiz: dict, from_idx: int):
    next_idx = from_idx + 1
    wiz["step_idx"] = next_idx
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["pkgwiz"] = wiz

    if next_idx >= _PKG_TOTAL:
        from database import save_persistent_db
        data = wiz.get("data", {})
        pkg_id = PE.create_package(
            name          = data.get("name", "Package"),
            delivery_mode = data.get("delivery_mode", "popup"),
            duration_days = int(data.get("duration_days", 7)),
            pricing_model = data.get("pricing_model", "flat"),
            flat_price    = float(data.get("flat_price", 0)),
            cpm_rate      = float(data.get("cpm_rate", 0)),
            description   = data.get("description", ""),
        )
        _clear_wiz(d); save_persistent_db()
        try:
            await event.edit(
                f"✅ **Package Created!** `{pkg_id}`\n\nAb is package ko sponsor campaigns mein use karo.",
                buttons=[[Button.inline("📦 Packages", b"promo_packages"),
                          Button.inline("📣 Dashboard", b"promo_panel")]]
            )
        except Exception:
            await event.respond(f"✅ **Package Created!** `{pkg_id}`",
                buttons=[[Button.inline("📦 Packages", b"promo_packages")]])
        return

    nkey, ntxt = _PKG_STEPS[next_idx]
    cancel_btn = Button.inline("❌ Cancel", b"promo_packages")

    if nkey == "delivery_mode":
        btns = [[Button.inline(lbl[:22], f"pkgwiz_dm|{k}".encode())] for k, lbl in PE.DELIVERY_MODES.items()]
        btns.append([cancel_btn])
        msg = f"**Step {next_idx+1}/{_PKG_TOTAL}**\n\n📺 **Delivery Mode chuniye:**"
    elif nkey == "pricing_model":
        btns = [[Button.inline(lbl[:22], f"pkgwiz_pm|{k}".encode())] for k, lbl in PE.PRICING_MODELS.items()]
        btns.append([cancel_btn])
        msg = f"**Step {next_idx+1}/{_PKG_TOTAL}**\n\n💰 **Pricing Model chuniye:**"
    elif nkey == "duration_days":
        btns = [
            [Button.inline("7 din", b"pkgwiz_dur|7"),   Button.inline("14 din", b"pkgwiz_dur|14"),
             Button.inline("30 din", b"pkgwiz_dur|30")],
            [Button.inline("60 din", b"pkgwiz_dur|60"),  Button.inline("90 din", b"pkgwiz_dur|90")],
            [Button.inline("✏️ Custom", b"pkgwiz_dur_custom"), cancel_btn],
        ]
        msg = f"**Step {next_idx+1}/{_PKG_TOTAL}**\n\n📅 **Duration kitne din?**"
    else:
        skip_row = [Button.inline("⏭ Skip", f"pkgwiz_skip|{nkey}".encode())] if nkey in _PKG_OPTIONAL else []
        btns = [skip_row + [cancel_btn]]
        msg = f"**Step {next_idx+1}/{_PKG_TOTAL}**\n\n{ntxt}"

    try:
        await event.edit(msg, buttons=btns)
    except errors.MessageNotModifiedError:
        pass
    except Exception: await event.respond(msg, buttons=btns)


@bot.on(events.CallbackQuery(data=b"pkg_create"))
async def pkg_create(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "pkgwiz"; d["pkgwiz"] = {"step_idx": 0, "data": {}}
    d["step_since"] = time.time()
    try:
        await event.edit(
            f"📦 **New Package — Step 1/{_PKG_TOTAL}**\n\n{_PKG_STEPS[0][1]}\n\n_/cancel to abort_",
            buttons=[[Button.inline("❌ Cancel", b"promo_packages")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pkgwiz_(dm|pm)\\|(.+)"))
async def pkgwiz_choice(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    parts = event.data.decode().split("|")
    ftype, val = parts[0].replace("pkgwiz_",""), parts[1]
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("pkgwiz", {})
    wiz["data"]["delivery_mode" if ftype=="dm" else "pricing_model"] = val
    await event.answer(f"✅ Selected!", alert=False)
    await _pkgwiz_advance(event, wiz, wiz.get("step_idx", 0))


@bot.on(events.CallbackQuery(pattern=b"pkgwiz_dur\\|(.+)"))
async def pkgwiz_dur(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    days = int(event.data.decode().split("|")[1])
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("pkgwiz", {})
    wiz["data"]["duration_days"] = days
    await event.answer(f"✅ {days} din selected!", alert=False)
    await _pkgwiz_advance(event, wiz, wiz.get("step_idx", 0))


@bot.on(events.CallbackQuery(data=b"pkgwiz_dur_custom"))
async def pkgwiz_dur_custom(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("pkgwiz", {})
    wiz["_awaiting_custom_dur"] = True
    d["pkgwiz"] = wiz
    try:
        await event.edit("📅 Custom duration daalo (days mein):\ne.g., `45`",
            buttons=[[Button.inline("❌ Cancel", b"promo_packages")]])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pkgwiz_skip\\|(.+)"))
async def pkgwiz_skip(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("pkgwiz", {})
    await event.answer("⏭ Skipped", alert=False)
    await _pkgwiz_advance(event, wiz, wiz.get("step_idx", 0))


@bot.on(events.NewMessage())
async def pkg_wiz_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "pkgwiz": return
    wiz  = d.get("pkgwiz", {})
    idx  = wiz.get("step_idx", 0)
    text = event.raw_text.strip()

    if text.lower() == "/cancel":
        _clear_wiz(d); save_persistent_db()
        await event.respond("❌ Cancelled.", buttons=[[Button.inline("📦 Packages", b"promo_packages")]])
        return

    # Custom duration awaited?
    if wiz.get("_awaiting_custom_dur"):
        try:
            wiz["data"]["duration_days"] = int(text)
            wiz.pop("_awaiting_custom_dur", None)
            await _pkgwiz_advance(event, wiz, idx)
        except:
            await event.respond("❌ Number daalo! (e.g., `45`)")
        return

    key = _PKG_STEPS[idx][0]
    if key in ("delivery_mode","pricing_model","duration_days"): return

    if key in ("flat_price","cpm_rate"):
        try: wiz["data"][key] = float(text)
        except: return await event.respond("❌ Number daalo! (e.g., `299`)")
    else:
        wiz["data"][key] = text
    await _pkgwiz_advance(event, wiz, idx)


# ─────────────────────────────────────────────────────────────────────────────
# CAMPAIGNS MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"promo_campaigns"))
async def promo_campaigns(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    s = PE.get_promo_summary()
    try:
        await event.edit(
            f"📋 **Campaigns**\n\n"
            f"🟢 Active: `{s['active_campaigns']}`  "
            f"⏳ Pending: `{s['pending_payment']}`  "
            f"📝 Approval: `{s['pending_approval']}`\n\n"
            "Filter chuniye:",
            buttons=[
                [Button.inline("🟢 Active",   b"promo_clist|active"),
                 Button.inline("⏳ Pay Pend", b"promo_clist|pending_payment")],
                [Button.inline("📝 Approval", b"promo_clist|draft"),
                 Button.inline("⌛ Expired",  b"promo_clist|expired")],
                [Button.inline("📋 All",      b"promo_clist|all"),
                 Button.inline("➕ Add",      b"promo_add_manual")],
                [Button.inline("🔙 Back",      b"promo_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"promo_clist\\|(.+)"))
async def promo_clist(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    status = event.data.decode().split("|")[1]
    camps  = PE.list_campaigns(None if status == "all" else status)
    if not camps:
        return await event.edit(
            f"📋 Koi campaign nahi hai status: `{status}`",
            buttons=[[Button.inline("🔙 Back", b"promo_campaigns")]]
        )
    btns = [[Button.inline(
        f"{_status_icon(c.get('status',''))} {c['title'][:22]} · {PE.PROMO_CATEGORIES.get(c.get('category',''),'')[:10]}",
        f"promo_cdetail|{c['id']}".encode()
    )] for c in camps[:10]]
    btns.append([Button.inline("🔙 Back", b"promo_campaigns")])
    try:
        await event.edit(f"📋 **Campaigns ({len(camps)}):**", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"promo_cdetail\\|(.+)"))
async def promo_cdetail(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cid = event.data.decode().split("|")[1]
    c   = PE.get_campaign(cid)
    if not c: return await event.answer("Campaign nahi mila!", alert=True)
    ana   = PE.get_campaign_analytics(cid)
    cat   = PE.PROMO_CATEGORIES.get(c.get("category",""), c.get("category",""))
    icon  = _status_icon(c.get("status",""))
    modes = ", ".join(PE.DELIVERY_MODES.get(m, m) for m in c.get("delivery_modes",[]))
    last7 = ana.get("last7", [])
    max_i = max((r[1] for r in last7), default=1)
    chart = "\n".join(f"`{d}` {_bar(i, max_i, 8)} `{i}`" for d,i,_ in last7)

    text = (
        f"{icon} **{c['title']}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📂 {cat}  ·  👤 `{c.get('sponsor_name','—')}`\n"
        f"📞 `{c.get('sponsor_contact','—')}`\n"
        f"📺 {modes}\n"
        f"💰 {_fmt(c.get('price',0))}  ·  `{c.get('pricing_model','flat')}`\n"
        f"📅 {c.get('duration_days')}d  {_ts(c.get('starts_at',0))} → {_ts(c.get('expires_at',0))}\n"
        f"⏳ Days Left: {ana['days_left']}\n\n"
        f"📊 Imp:`{ana['impressions']}` Clicks:`{ana['clicks']}` CTR:`{ana['ctr']}%`\n"
        f"💵 Earned: `{_fmt(ana['earned'])}`\n\n"
        f"📝 `{c.get('promo_text','')[:100]}`\n"
        f"🔗 {c.get('link','—')}\n\n"
        f"**Last 7 Days:**\n{chart}"
    )
    status = c.get("status","")
    action_btns = []
    if status == "draft":
        action_btns = [Button.inline("✅ Approve", f"cmp_approve|{cid}".encode()),
                       Button.inline("❌ Reject",  f"cmp_reject|{cid}".encode())]
    elif status == "active":
        action_btns = [Button.inline("⏸ Pause",   f"cmp_pause|{cid}".encode()),
                       Button.inline("✏️ Edit",    f"cmp_edit|{cid}".encode())]
    elif status == "paused":
        action_btns = [Button.inline("▶️ Resume",  f"cmp_resume|{cid}".encode()),
                       Button.inline("✏️ Edit",    f"cmp_edit|{cid}".encode())]
    elif status == "pending_payment":
        action_btns = [Button.inline("💰 Mark Paid", f"cmp_paid|{cid}".encode())]

    buttons = []
    if action_btns: buttons.append(action_btns)
    buttons += [
        [Button.inline("🗑 Delete",    f"cmp_delete|{cid}".encode()),
         Button.inline("📊 Full Stats", f"cmp_stats|{cid}".encode())],
        [Button.inline("🔙 Back",      b"promo_campaigns")],
    ]
    try:
        await event.edit(text, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass
    except errors.MessageTooLongError: await event.edit(text[:3000]+"...", buttons=buttons)


@bot.on(events.CallbackQuery(pattern=b"cmp_approve\\|(.+)"))
async def cmp_approve(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cid = event.data.decode().split("|")[1]
    ok  = PE.approve_campaign(cid)
    await event.answer("✅ Approved & Live!" if ok else "❌ Error", alert=True)
    c = PE.get_campaign(cid)
    if c and c.get("requested_by"):
        try:
            await bot.send_message(c["requested_by"],
                f"🎉 **Your campaign is now LIVE!**\n\n"
                f"📣 **{c['title']}**\n"
                f"📅 Duration: {c.get('duration_days')} days\n"
                f"🔗 {c.get('link','—')}\n\n"
                f"Performance reports ke liye humse contact karo!"
            )
        except Exception: pass
    await promo_cdetail(event)


@bot.on(events.CallbackQuery(pattern=b"cmp_reject\\|(.+)"))
async def cmp_reject(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    PE.reject_campaign(event.data.decode().split("|")[1])
    await event.answer("❌ Rejected", alert=False)
    await promo_cdetail(event)


@bot.on(events.CallbackQuery(pattern=b"cmp_pause\\|(.+)"))
async def cmp_pause_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    PE.pause_campaign(event.data.decode().split("|")[1])
    await event.answer("⏸ Paused", alert=False)
    await promo_cdetail(event)


@bot.on(events.CallbackQuery(pattern=b"cmp_resume\\|(.+)"))
async def cmp_resume_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    PE.resume_campaign(event.data.decode().split("|")[1])
    await event.answer("▶️ Resumed", alert=False)
    await promo_cdetail(event)


@bot.on(events.CallbackQuery(pattern=b"cmp_paid\\|(.+)"))
async def cmp_paid_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cid = event.data.decode().split("|")[1]
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "cmp_payment_ref"; d["cmp_paid_cid"] = cid
    d["step_since"] = time.time()
    try:
        await event.edit(
            "💰 **Payment Reference daalo:**\n\nUTR / Transaction ID:",
            buttons=[
                [Button.inline("⏭ Skip (no ref)", b"cmp_paid_skip")],
                [Button.inline("❌ Cancel", f"promo_cdetail|{cid}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"cmp_paid_skip"))
async def cmp_paid_skip(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data, save_persistent_db
    d   = get_user_data(event.sender_id)
    cid = d.get("cmp_paid_cid","")
    if not cid: return await event.answer("❌ CID lost!", alert=True)
    PE.mark_payment_received(cid, "")
    _clear_wiz(d); save_persistent_db()
    await event.answer("✅ Payment recorded!", alert=False)
    await promo_cdetail(event)


@bot.on(events.NewMessage())
async def cmp_payment_ref_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "cmp_payment_ref": return
    cid = d.get("cmp_paid_cid","")
    ref = "" if event.raw_text.strip().lower() == "/skip" else event.raw_text.strip()
    PE.mark_payment_received(cid, ref)
    _clear_wiz(d); save_persistent_db()
    await event.respond("✅ Payment recorded! Campaign sent for approval.",
        buttons=[[Button.inline("📋 Campaigns", b"promo_campaigns"),
                  Button.inline("📣 Dashboard", b"promo_panel")]])


# ─── Campaign Edit ────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"cmp_edit\\|(.+)"))
async def cmp_edit(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cid = event.data.decode().split("|")[1]
    c   = PE.get_campaign(cid)
    if not c: return await event.answer("Campaign nahi mila!", alert=True)
    try:
        await event.edit(
            f"✏️ **Edit Campaign: {c['title'][:30]}**\n\nKya edit karna hai?",
            buttons=[
                [Button.inline("📝 Title",         f"cedit_f|title|{cid}".encode()),
                 Button.inline("✍️ Promo Text",     f"cedit_f|promo_text|{cid}".encode())],
                [Button.inline("🔗 Link",           f"cedit_f|link|{cid}".encode()),
                 Button.inline("🔘 Button Label",   f"cedit_f|btn_label|{cid}".encode())],
                [Button.inline("👤 Sponsor Name",   f"cedit_f|sponsor_name|{cid}".encode()),
                 Button.inline("📞 Contact",        f"cedit_f|sponsor_contact|{cid}".encode())],
                [Button.inline("💰 Price",          f"cedit_f|price|{cid}".encode())],
                [Button.inline("🔙 Campaign Detail", f"promo_cdetail|{cid}".encode())],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"cedit_f\\|(.+)\\|(.+)"))
async def cedit_field_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    field, cid = parts[1], parts[2]
    c = PE.get_campaign(cid)
    if not c: return await event.answer("Campaign nahi mila!", alert=True)
    labels = {"title":"Title","promo_text":"Promo Text","link":"Link","btn_label":"Button Label",
              "sponsor_name":"Sponsor Name","sponsor_contact":"Contact","price":"Price (₹)"}
    optional = {"link","btn_label","sponsor_name","sponsor_contact"}
    cur_val  = c.get(field,"—") or "—"
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "cedit_handler"; d["cedit_field"] = field; d["cedit_cid"] = cid
    d["step_since"] = time.time()
    skip = [Button.inline("⏭ Skip/Clear", b"cedit_skip")] if field in optional else []
    try:
        await event.edit(
            f"✏️ **Edit: {labels.get(field,field)}**\n\nCurrent: `{str(cur_val)[:80]}`\n\nNaya value bhejo:",
            buttons=[skip + [Button.inline("🔙 Menu", f"cmp_edit|{cid}".encode())],
                     [Button.inline("❌ Cancel", f"promo_cdetail|{cid}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"cedit_skip"))
async def cedit_skip(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data, save_persistent_db
    d = get_user_data(event.sender_id)
    field, cid = d.get("cedit_field",""), d.get("cedit_cid","")
    if not cid: return await event.answer("❌ State lost!", alert=True)
    PE.update_campaign(cid, **{field: ""})
    _clear_wiz(d); save_persistent_db()
    await event.answer(f"✅ {field} cleared!", alert=False)
    event.data = f"promo_cdetail|{cid}".encode()
    await promo_cdetail(event)


@bot.on(events.NewMessage())
async def cedit_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "cedit_handler": return
    field, cid = d.get("cedit_field",""), d.get("cedit_cid","")
    text = event.raw_text.strip()
    if text.lower() in ("/cancel","cancel"):
        _clear_wiz(d); save_persistent_db()
        return await event.respond("❌ Edit cancelled.",
            buttons=[[Button.inline("✏️ Edit Menu", f"cmp_edit|{cid}".encode())]])
    if not cid or not PE.get_campaign(cid):
        _clear_wiz(d)
        return await event.respond("❌ Campaign nahi mila.")
    try:
        if field == "price":
            val = float(text)
            if val < 0: raise ValueError
            PE.update_campaign(cid, price=val)
        elif field == "title" and len(text) < 2:
            return await event.respond("❌ Title 2+ chars hona chahiye.")
        else:
            PE.update_campaign(cid, **{field: text})
        _clear_wiz(d); save_persistent_db()
        await event.respond(f"✅ **{field.replace('_',' ').title()} updated!**",
            buttons=[[Button.inline("✏️ Aur Edit", f"cmp_edit|{cid}".encode()),
                      Button.inline("📊 Detail",    f"promo_cdetail|{cid}".encode())]])
    except ValueError:
        await event.respond("❌ Price: number daalo (e.g., `999`)",
            buttons=[[Button.inline("🔁 Retry", f"cedit_f|{field}|{cid}".encode())]])


@bot.on(events.CallbackQuery(pattern=b"cmp_delete\\|(.+)"))
async def cmp_delete_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cid = event.data.decode().split("|")[1]
    c   = PE.get_campaign(cid)
    try:
        await event.edit(f"🗑 **Delete?**\n\n`{c.get('title','?')}`\n\nPermanently delete hoga!",
            buttons=[[Button.inline("✅ Haan Delete", f"cmp_del_ok|{cid}".encode()),
                      Button.inline("❌ Cancel",      f"promo_cdetail|{cid}".encode())]])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"cmp_del_ok\\|(.+)"))
async def cmp_del_ok(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cid = event.data.decode().split("|")[1]
    c   = PE._cfg()["campaigns"].pop(cid, None)
    if c:
        try:
            import ads_engine as AE
            for aid in c.get("ad_ids",[]): AE.delete_ad(aid)
        except Exception: pass
        PE._save()
    await event.answer("🗑 Deleted!", alert=False)
    await promo_campaigns(event)


@bot.on(events.CallbackQuery(pattern=b"cmp_stats\\|(.+)"))
async def cmp_stats_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cid  = event.data.decode().split("|")[1]
    ana  = PE.get_campaign_analytics(cid)
    last7 = ana.get("last7",[])
    max_i = max((r[1] for r in last7), default=1)
    chart = "\n".join(f"`{d}` {_bar(i, max_i, 10)} 👁`{i}` 🖱`{c}`" for d,i,c in last7)
    try:
        await event.edit(
            f"📊 **Stats: {ana['title']}**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👁 Impressions: `{ana['impressions']:,}`\n"
            f"🖱 Clicks: `{ana['clicks']:,}`\n"
            f"📈 CTR: `{ana['ctr']}%`\n"
            f"💵 Earned: `{_fmt(ana['earned'])}`\n"
            f"📅 Days Left: `{ana['days_left']}`\n\n"
            f"**Last 7 Days:**\n{chart}",
            buttons=[[Button.inline("🔙 Campaign", f"promo_cdetail|{cid}".encode())]]
        )
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# MANUAL CAMPAIGN ADD
# ─────────────────────────────────────────────────────────────────────────────

_ADD_STEPS = [
    ("category",     None),
    ("package_id",   None),
    ("title",        "📝 **Campaign Title:**"),
    ("promo_text",   "✍️ **Promotion Text:**\n_(Jo users ko dikhega)_"),
    ("link",         "🔗 **Link:** _(optional)_"),
    ("btn_label",    "🔘 **Button Label:** _(optional)_"),
    ("sponsor_name", "👤 **Sponsor Name:** _(optional)_"),
    ("custom_price", "💰 **Custom Price (₹):** _(optional, skip = package price)_"),
    ("delivery",     None),
]
_ADD_OPTIONAL = {"link","btn_label","sponsor_name","custom_price"}


@bot.on(events.CallbackQuery(data=b"promo_add_manual"))
async def promo_add_manual(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "cmpwiz"; d["cmpwiz"] = {"step_idx": 0, "data": {}}
    d["step_since"] = time.time()
    btns = [[Button.inline(lbl[:22], f"cmpwiz_cat|{k}".encode())] for k,lbl in PE.PROMO_CATEGORIES.items()]
    btns.append([Button.inline("❌ Cancel", b"promo_campaigns")])
    try:
        await event.edit("➕ **New Campaign**\n\n📂 **Category chuniye:**", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"cmpwiz_cat\\|(.+)"))
async def cmpwiz_cat(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("cmpwiz",{})
    wiz["data"]["category"] = event.data.decode().split("|")[1]
    wiz["step_idx"] = 1
    pkgs = PE.list_packages()
    if not pkgs:
        try:
            await event.edit("❌ Pehle packages banao!",
                buttons=[[Button.inline("📦 Packages", b"promo_packages")]])
        except errors.MessageNotModifiedError:
            pass
        return
    btns = [[Button.inline(
        f"{'⭐ ' if p.get('popular') else ''}{p['name'][:20]} · {_fmt(p.get('flat_price',0))} · {p.get('duration_days')}d",
        f"cmpwiz_pkg|{p['id']}".encode()
    )] for p in pkgs[:8]]
    await event.edit("📦 **Package chuniye:**", buttons=btns)
    d["cmpwiz"] = wiz


@bot.on(events.CallbackQuery(pattern=b"cmpwiz_pkg\\|(.+)"))
async def cmpwiz_pkg(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("cmpwiz",{})
    wiz["data"]["package_id"] = event.data.decode().split("|")[1]
    wiz["step_idx"] = 2
    _, ntxt = _ADD_STEPS[2]
    try:
        await event.edit(f"**Step 3/{len(_ADD_STEPS)}**\n\n{ntxt}",
            buttons=[[Button.inline("❌ Cancel", b"promo_campaigns")]])
    except errors.MessageNotModifiedError:
        pass
    d["cmpwiz"] = wiz


@bot.on(events.CallbackQuery(pattern=b"cmpwiz_dm\\|(.+)"))
async def cmpwiz_dm(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("cmpwiz",{})
    wiz["data"].setdefault("delivery",[]).append(event.data.decode().split("|")[1])
    selected = ", ".join(PE.DELIVERY_MODES.get(m,"") for m in wiz["data"]["delivery"])
    btns = [[Button.inline(lbl[:20], f"cmpwiz_dm|{k}".encode())]
            for k,lbl in PE.DELIVERY_MODES.items() if k not in wiz["data"]["delivery"]]
    btns.append([Button.inline(f"✅ Done ({selected[:30]})", b"cmpwiz_done")])
    try:
        await event.edit("📺 **Aur delivery mode?**\nYa Done:", buttons=btns)
    except errors.MessageNotModifiedError:
        pass
    d["cmpwiz"] = wiz


@bot.on(events.CallbackQuery(data=b"cmpwiz_done"))
async def cmpwiz_done(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data, save_persistent_db
    d   = get_user_data(event.sender_id)
    wiz = d.get("cmpwiz",{})
    data = wiz.get("data",{})
    cid = PE.create_campaign(
        category=data.get("category","custom"), package_id=data.get("package_id",""),
        title=data.get("title","Untitled"), promo_text=data.get("promo_text",""),
        link=data.get("link",""), btn_label=data.get("btn_label",""),
        sponsor_name=data.get("sponsor_name",""), custom_price=float(data.get("custom_price",0)),
        delivery_modes=data.get("delivery",[]), requested_by=event.sender_id,
    )
    _clear_wiz(d); save_persistent_db()
    try:
        await event.edit(f"✅ **Campaign Created!**\nID: `{cid}`\nStatus: ⏳ Payment Pending",
            buttons=[[Button.inline("📋 Campaigns", b"promo_campaigns"),
                      Button.inline("📣 Dashboard", b"promo_panel")]])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage())
async def cmpwiz_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "cmpwiz": return
    wiz = d.get("cmpwiz",{}); idx = wiz.get("step_idx",0)
    text = event.raw_text.strip()
    if text.lower() == "/cancel":
        _clear_wiz(d); save_persistent_db()
        await event.respond("❌ Cancelled.", buttons=[[Button.inline("📋 Campaigns", b"promo_campaigns")]])
        return
    if idx < len(_ADD_STEPS):
        key = _ADD_STEPS[idx][0]
        if key == "custom_price":
            try: wiz["data"][key] = float(text)
            except: return await event.respond("❌ Number daalo!")
        else:
            wiz["data"][key] = text
        next_idx = idx + 1
        wiz["step_idx"] = next_idx
        if next_idx < len(_ADD_STEPS):
            nkey, ntxt = _ADD_STEPS[next_idx]
            if nkey == "delivery":
                btns = [[Button.inline(lbl[:20], f"cmpwiz_dm|{k}".encode())] for k,lbl in PE.DELIVERY_MODES.items()]
                await event.respond(f"**Step {next_idx+1}/{len(_ADD_STEPS)}**\n\n📺 **Delivery Mode:**", buttons=btns)
            elif ntxt:
                skip_row = ([Button.inline("⏭ Skip", f"cmpwiz_skip_field|{nkey}".encode())] if nkey in _ADD_OPTIONAL else [])
                skip_row.append(Button.inline("❌ Cancel", b"promo_campaigns"))
                await event.respond(f"**Step {next_idx+1}/{len(_ADD_STEPS)}**\n\n{ntxt}", buttons=[skip_row])
        d["cmpwiz"] = wiz


@bot.on(events.CallbackQuery(pattern=b"cmpwiz_skip_field\\|(.+)"))
async def cmpwiz_skip_field(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d   = get_user_data(event.sender_id)
    wiz = d.get("cmpwiz",{})
    idx = wiz.get("step_idx",0)
    next_idx = idx + 1
    wiz["step_idx"] = next_idx
    d["cmpwiz"] = wiz
    await event.answer("⏭ Skipped", alert=False)
    if next_idx < len(_ADD_STEPS):
        nkey, ntxt = _ADD_STEPS[next_idx]
        if nkey == "delivery":
            btns = [[Button.inline(lbl[:20], f"cmpwiz_dm|{k}".encode())] for k,lbl in PE.DELIVERY_MODES.items()]
            try:
                await event.edit(f"**Step {next_idx+1}/{len(_ADD_STEPS)}**\n\n📺 **Delivery Mode:**", buttons=btns)
            except errors.MessageNotModifiedError:
                pass
        elif ntxt:
            skip_row = ([Button.inline("⏭ Skip", f"cmpwiz_skip_field|{nkey}".encode())] if nkey in _ADD_OPTIONAL else [])
            skip_row.append(Button.inline("❌ Cancel", b"promo_campaigns"))
            await event.edit(f"**Step {next_idx+1}/{len(_ADD_STEPS)}**\n\n{ntxt}", buttons=[skip_row])


# ─────────────────────────────────────────────────────────────────────────────
# INQUIRIES
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"promo_inquiries"))
async def promo_inquiries(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    pending = PE.get_pending_inquiries()
    all_inq = PE._cfg().get("inquiry_log", [])
    if not all_inq:
        return await event.edit(
            "📬 **Inquiries**\n\nKoi inquiry nahi aayi.\nSponsors /promote se inquiry bhejte hain.",
            buttons=[[Button.inline("🔙 Back", b"promo_panel")]])
    btns = [[Button.inline(
        f"{'🔔' if not i.get('handled') else '✅'} uid:{i['user_id']} · {PE.PROMO_CATEGORIES.get(i.get('category',''),'')[:10]} · {_ts_short(i.get('ts',0))}",
        f"inq_detail|{i['id']}".encode()
    )] for i in list(reversed(all_inq))[:8]]
    if pending:
        btns.append([Button.inline(f"✅ Mark All Handled ({len(pending)})", b"inq_mark_all")])
    btns.append([Button.inline("🔙 Back", b"promo_panel")])
    try:
        await event.edit(f"📬 **Inquiries** — 🔔 Pending: `{len(pending)}`  Total: `{len(all_inq)}`", buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"inq_mark_all"))
async def inq_mark_all(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    count = sum(1 for inq in PE.get_pending_inquiries() if PE.mark_inquiry_handled(inq["id"]) is not None or True)
    for inq in PE.get_pending_inquiries():
        PE.mark_inquiry_handled(inq["id"])
    await event.answer(f"✅ All marked handled!", alert=False)
    await promo_inquiries(event)


@bot.on(events.CallbackQuery(pattern=b"inq_detail\\|(.+)"))
async def inq_detail(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    iid = event.data.decode().split("|")[1]
    inq = next((i for i in PE._cfg().get("inquiry_log",[]) if i["id"]==iid), None)
    if not inq: return await event.answer("Nahi mila!", alert=True)
    cat = PE.PROMO_CATEGORIES.get(inq.get("category",""), inq.get("category",""))
    try:
        await event.edit(
            f"📬 **Inquiry**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"👤 User: `{inq['user_id']}`\n"
            f"📂 {cat}  ·  🕐 {_ts(inq.get('ts',0))}\n"
            f"Status: {'✅ Handled' if inq.get('handled') else '🔔 Pending'}\n\n"
            f"**Message:**\n{inq.get('msg','—')}",
            buttons=[
                [Button.inline("✅ Mark Handled", f"inq_done|{iid}".encode()),
                 Button.inline("💬 Reply",        f"inq_reply|{inq['user_id']}".encode())],
                [Button.inline("🔙 Inquiries",     b"promo_inquiries")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"inq_done\\|(.+)"))
async def inq_done(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    PE.mark_inquiry_handled(event.data.decode().split("|")[1])
    await event.answer("✅ Marked handled", alert=False)
    await promo_inquiries(event)


@bot.on(events.CallbackQuery(pattern=b"inq_reply\\|(.+)"))
async def inq_reply(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    uid = event.data.decode().split("|")[1]
    await event.answer(f"Telegram pe directly message karo: {uid}", alert=True)


# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"promo_analytics"))
async def promo_analytics(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    camps = PE.list_campaigns("active") + PE.list_campaigns("completed") + PE.list_campaigns("expired")
    if not camps:
        return await event.edit("📊 Koi data nahi!",
            buttons=[[Button.inline("🔙 Back", b"promo_panel")]])
    sorted_c = sorted(camps, key=lambda c: c.get("impressions",0), reverse=True)
    max_imp  = max((c.get("impressions",0) for c in sorted_c), default=1)
    lines    = ["📊 **Sponsor Campaigns Analytics**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"]
    for i, c in enumerate(sorted_c[:7], 1):
        ana = PE.get_campaign_analytics(c["id"])
        cat = PE.PROMO_CATEGORIES.get(c.get("category",""),"")[:8]
        lines.append(
            f"`{i}.` **{c['title'][:16]}** `[{cat}]`\n"
            f"   {_bar(c.get('impressions',0), max_imp, 8)} 👁`{ana['impressions']}` CTR:`{ana['ctr']}%` 💰`{_fmt(ana['price'])}`"
        )
    s = PE.get_promo_summary()
    lines.append(f"\n**Total: {_fmt(s['total_revenue'])}  Pending: {_fmt(s['pending_revenue'])}**")
    try:
        await event.edit("\n".join(lines), buttons=[[Button.inline("🔙 Back", b"promo_panel")]])
    except errors.MessageNotModifiedError:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"promo_settings"))
async def promo_settings(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = PE._cfg()
    try:
        await event.edit(
            "⚙️ **Promotion Settings**\n\n"
            f"🔌 System: **{'ON' if cfg.get('enabled') else 'OFF'}**\n"
            f"✅ Auto-approve: **{'ON (manual review nahi hoga)' if cfg.get('auto_approve') else 'OFF (manual review)'}**\n"
            f"📞 Contact: `{cfg.get('contact_info','—')}`\n\n"
            f"Rate Card (jo /promote pe dikhta hai):\n`{cfg.get('rate_card_msg','—')[:150]}`",
            buttons=[
                [Button.inline(f"{'🔴 Disable' if cfg.get('enabled') else '🟢 Enable'} System", b"promo_toggle")],
                [Button.inline(f"Auto-approve: {'ON → OFF' if cfg.get('auto_approve') else 'OFF → ON'}", b"promo_toggle_aa")],
                [Button.inline("📞 Set Contact",      b"promo_set_contact"),
                 Button.inline("📋 Set Rate Card",    b"promo_set_ratecard")],
                [Button.inline("⚡ Rate Card Template", b"promo_ratecard_tpl")],
                [Button.inline("🔙 Back",               b"promo_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"promo_toggle"))
async def promo_toggle(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = PE._cfg(); cfg["enabled"] = not cfg.get("enabled",True); PE._save()
    await promo_settings(event)


@bot.on(events.CallbackQuery(data=b"promo_toggle_aa"))
async def promo_toggle_aa(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = PE._cfg(); cfg["auto_approve"] = not cfg.get("auto_approve",False); PE._save()
    await promo_settings(event)


@bot.on(events.CallbackQuery(data=b"promo_set_contact"))
async def promo_set_contact(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    get_user_data(event.sender_id)["step"] = "promo_contact_val"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("📞 **Contact info daalo:**\n(e.g. `@YourUsername`)\n\n_/cancel = wapas_",
            buttons=[[Button.inline("❌ Cancel", b"promo_settings")]])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"promo_set_ratecard"))
async def promo_set_ratecard(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    get_user_data(event.sender_id)["step"] = "promo_ratecard_val"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "📋 **Rate Card message daalo:**\n\nYe message /promote pe dikhega.\n\n_/cancel = wapas_",
            buttons=[[Button.inline("⚡ Template Use Karo", b"promo_ratecard_tpl"),
                      Button.inline("❌ Cancel",             b"promo_settings")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"promo_ratecard_tpl"))
async def promo_ratecard_tpl(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg     = PE._cfg()
    contact = cfg.get("contact_info","@YourUsername")
    template = (
        "📢 **Banner Ad** — ₹299/7 days · ₹799/30 days\n"
        "🔘 **Button Ad** — ₹199/7 days · ₹499/30 days\n"
        "📣 **Pop-up Ad** — ₹399/7 days · ₹999/30 days\n"
        "⏰ **Blast Campaign** — ₹599/blast\n\n"
        f"💬 Contact: {contact}\n"
        "⚡ 24hr turnaround · Full performance reports"
    )
    cfg["rate_card_msg"] = template
    PE._save()
    await event.answer("✅ Template applied!", alert=False)
    await promo_settings(event)


@bot.on(events.NewMessage())
async def promo_settings_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    step = d.get("step") or ""
    if step == "promo_contact_val":
        if event.raw_text.strip().lower() == "/cancel":
            d["step"] = None; save_persistent_db()
            return await event.respond("❌ Cancelled.", buttons=[[Button.inline("⚙️ Settings", b"promo_settings")]])
        PE._cfg()["contact_info"] = event.raw_text.strip(); PE._save()
        d["step"] = None; save_persistent_db()
        await event.respond("✅ Contact set!", buttons=[[Button.inline("⚙️ Settings", b"promo_settings")]])
    elif step == "promo_ratecard_val":
        if event.raw_text.strip().lower() == "/cancel":
            d["step"] = None; save_persistent_db()
            return await event.respond("❌ Cancelled.", buttons=[[Button.inline("⚙️ Settings", b"promo_settings")]])
        PE._cfg()["rate_card_msg"] = event.raw_text.strip(); PE._save()
        d["step"] = None; save_persistent_db()
        await event.respond("✅ Rate card set!", buttons=[[Button.inline("⚙️ Settings", b"promo_settings")]])


# ─────────────────────────────────────────────────────────────────────────────
# PAYOUT
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"promo_payout"))
async def promo_payout(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    s   = PE.get_promo_summary()
    log = PE._cfg().get("payout_log", [])[-5:]
    history = "\n".join(f"  `{_ts_short(p['t'])}` — {_fmt(p['amount'])} {p.get('note','')}"
                        for p in reversed(log)) or "  (Koi record nahi)"
    try:
        await event.edit(
            f"💸 **Payout Management**\n\n"
            f"💼 Total Revenue: **{_fmt(s['total_revenue'])}**\n"
            f"⏳ Pending: **{_fmt(s['pending_revenue'])}**\n"
            f"✅ Paid Out: {_fmt(s['paid_revenue'])}\n\n"
            f"**Recent:**\n{history}",
            buttons=[
                [Button.inline("💸 Payout Record Karo", b"promo_do_payout")],
                [Button.inline("🔙 Back",                b"promo_panel")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"promo_do_payout"))
async def promo_do_payout(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    get_user_data(event.sender_id)["step"] = "promo_payout_amt"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(
            "💸 **Payout Amount daalo:**\n\nFormat: `2000` ya `2000 UPI se`\n\n_/cancel = wapas_",
            buttons=[[Button.inline("🔙 Cancel", b"promo_payout")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.NewMessage())
async def promo_payout_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "promo_payout_amt": return
    text = event.raw_text.strip()
    if text.lower() in ("/cancel","cancel"):
        d["step"] = None; save_persistent_db()
        return await event.respond("❌ Cancelled.", buttons=[[Button.inline("💸 Payout", b"promo_payout")]])
    parts = text.split(None, 1)
    try:
        amt = float(parts[0]); note = parts[1] if len(parts)>1 else ""
        PE.mark_payout(amt, note); d["step"] = None; save_persistent_db()
        await event.respond(f"✅ **{_fmt(amt)} payout recorded!**  Note: {note or '—'}",
            buttons=[[Button.inline("💸 Payout", b"promo_payout"),
                      Button.inline("📣 Panel",   b"promo_panel")]])
    except ValueError:
        await event.respond("❌ Format: `500` ya `500 UPI se`",
            buttons=[[Button.inline("❌ Cancel", b"promo_payout")]])


# ══════════════════════════════════════════════════════════════
# FIX 5: PROMO PACKAGE AUTO-TEMPLATES
# ══════════════════════════════════════════════════════════════

_PROMO_PACKAGE_TEMPLATES = [
    {
        "name": "🌱 Starter — ₹299",
        "description": "7 din ka banner ad — chhote business ke liye",
        "delivery_mode": "banner",
        "pricing_model": "flat",
        "flat_price": 299,
        "cpm_rate": 0,
        "duration_days": 7,
        "max_impressions": 10000,
        "popular": False,
        "active": True,
    },
    {
        "name": "⭐ Standard — ₹699",
        "description": "15 din — banner + weekly blast, medium reach",
        "delivery_mode": "banner",
        "pricing_model": "flat",
        "flat_price": 699,
        "cpm_rate": 0,
        "duration_days": 15,
        "max_impressions": 30000,
        "popular": True,
        "active": True,
    },
    {
        "name": "💎 Premium — ₹1499",
        "description": "30 din — button ad, max visibility, priority support",
        "delivery_mode": "button",
        "pricing_model": "flat",
        "flat_price": 1499,
        "cpm_rate": 0,
        "duration_days": 30,
        "max_impressions": 80000,
        "popular": False,
        "active": True,
    },
    {
        "name": "🚀 Blast Only — ₹499",
        "description": "Ek baar sab users ko message blast — launch offer",
        "delivery_mode": "blast",
        "pricing_model": "flat",
        "flat_price": 499,
        "cpm_rate": 0,
        "duration_days": 1,
        "max_impressions": 0,
        "popular": False,
        "active": True,
    },
    {
        "name": "👑 VIP — ₹2999",
        "description": "60 din — sab ad types, unlimited impressions",
        "delivery_mode": "popup",
        "pricing_model": "flat",
        "flat_price": 2999,
        "cpm_rate": 0,
        "duration_days": 60,
        "max_impressions": 0,
        "popular": False,
        "active": True,
    },
]


@bot.on(events.CallbackQuery(data=b"pkg_load_templates"))
async def pkg_load_templates(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)

    try:
        await event.edit(
            "⚡ **AUTO-TEMPLATES**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Ready-made sponsor packages — ek click mein load karo!\n"
            "Baad mein kisi bhi package ko edit kar sakte ho.\n\n"
            + "\n".join(
                f"  **{t['name']}** — {t['description']}"
                for t in _PROMO_PACKAGE_TEMPLATES
            ),
            buttons=[
                [Button.inline("✅ Sare Templates Load Karo",  b"pkg_tpl_load_all")],
                [Button.inline("🌱 Starter Only",              b"pkg_tpl_load|0"),
                 Button.inline("⭐ Standard Only",             b"pkg_tpl_load|1")],
                [Button.inline("💎 Premium Only",              b"pkg_tpl_load|2"),
                 Button.inline("🚀 Blast Only",                b"pkg_tpl_load|3")],
                [Button.inline("👑 VIP Only",                  b"pkg_tpl_load|4")],
                [Button.inline("🔙 Back",                      b"promo_packages")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"pkg_tpl_load_all"))
async def pkg_tpl_load_all(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    created = 0
    for tpl in _PROMO_PACKAGE_TEMPLATES:
        try:
            PE.create_package(**tpl)
            created += 1
        except Exception:
            pass
    await event.answer(f"✅ {created} packages bane!", alert=True)
    await promo_packages(event)


@bot.on(events.CallbackQuery(pattern=b"pkg_tpl_load\\|(.+)"))
async def pkg_tpl_load_single(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    idx = int(event.data.decode().split("|")[1])
    tpl = _PROMO_PACKAGE_TEMPLATES[idx] if 0 <= idx < len(_PROMO_PACKAGE_TEMPLATES) else None
    if not tpl:
        return await event.answer("Template nahi mila!", alert=True)
    try:
        PE.create_package(**tpl)
        await event.answer(f"✅ '{tpl['name']}' bana!", alert=True)
    except Exception as e:
        await event.answer(f"❌ Error: {e}", alert=True)
    await promo_packages(event)
