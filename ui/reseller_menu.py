# ui/reseller_menu.py
# ══════════════════════════════════════════════════════════
# RESELLER PANEL — Admin aur Reseller dono ke liye
# ══════════════════════════════════════════════════════════

from telethon import events, Button, errors
from config import bot
from admin import is_admin
from database import get_user_data, save_persistent_db, save_to_mongo
import asyncio
import time

async def _save_step(data):
    """Step save karo — JSON + MongoDB dono mein."""
    save_persistent_db()
    try:
        await save_to_mongo()
    except Exception:
        pass
from reseller import (
    is_reseller, get_reseller_stats, get_all_resellers,
    add_reseller, remove_reseller, suspend_reseller,
    reseller_give_premium, reseller_remove_premium,
    get_admin_reseller_summary
)


# ══════════════════════════════════════════════
# RESELLER — APNA PANEL
# ══════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"reseller_panel"))
async def reseller_panel(event):
    await event.answer()
    uid = event.sender_id
    if not is_reseller(uid):
        return await event.answer("❌ Tum reseller nahi ho.", alert=True)

    stats    = get_reseller_stats(uid)
    used     = stats.get("used", 0)
    quota    = stats.get("quota", 0)
    rem      = stats.get("remaining", 0)
    earnings = stats.get("earnings", 0)
    comm     = stats.get("commission", 0)
    users    = stats.get("users", [])

    # Quota bar
    pct     = round(used / max(quota, 1) * 10)
    bar     = "█" * pct + "░" * (10 - pct)
    health  = "🟢 Good" if rem > quota * 0.3 else ("🟡 Low" if rem > 0 else "🔴 Full")

    text = (
        "👥 **RESELLER PANEL**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**Quota:** [{bar}] `{used}/{quota}` — {health}\n"
        f"✅ Remaining: **{rem}** slots\n"
        f"👤 My Users: **{len(users)}**\n\n"
        f"**💰 Earnings:** `₹{earnings:.2f}`\n"
        f"📊 Commission: `{comm}%`\n\n"
        "Kya karna chahte ho?"
    )
    btns = [
        [Button.inline("💎 User ko Premium Do",      b"res_give_premium"),
         Button.inline("❌ Premium Hatao",            b"res_remove_premium_start")],
        [Button.inline("👥 Mere Users",               b"res_my_users")],
        [Button.inline("🏠 Main Menu",                b"main_menu")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except Exception:
        await event.respond(text, buttons=btns)


@bot.on(events.CallbackQuery(data=b"res_give_premium"))
async def res_give_premium_start(event):
    await event.answer()
    if not is_reseller(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    from database import get_user_data, save_persistent_db
    data = get_user_data(event.sender_id)
    data["step"] = "res_give_prem_user_id"
    await _save_step(data)
    await event.edit(
        "💎 **User ko Premium Do**\n\n"
        "Step 1/2: User ID bhejo jise premium dena hai\n"
        "(User ID @userinfobot se milega)\n\n"
        "User ID bhejo:",
        buttons=[[Button.inline("❌ Cancel", b"reseller_panel")]]
    )


@bot.on(events.CallbackQuery(data=b"res_remove_premium_start"))
async def res_remove_premium_start(event):
    await event.answer()
    if not is_reseller(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    stats = get_reseller_stats(event.sender_id)
    users = stats.get("users", [])
    if not users:
        return await event.answer("Tumhare paas koi user nahi hai.", alert=True)

    data = get_user_data(event.sender_id)
    data["step"] = "res_remove_prem_user_id"
    save_persistent_db()
    users_txt = "\n".join([f"• `{uid}`" for uid in users[:20]])
    await event.edit(
        f"❌ **Premium Hatao**\n\n"
        f"Tumhare users:\n{users_txt}\n\n"
        "User ID bhejo jiska premium hatana hai:",
        buttons=[[Button.inline("❌ Cancel", b"reseller_panel")]]
    )


@bot.on(events.CallbackQuery(data=b"res_my_users"))
async def res_my_users(event):
    await event.answer()
    if not is_reseller(event.sender_id):
        return await event.answer("❌ No permission", alert=True)

    stats = get_reseller_stats(event.sender_id)
    users = stats.get("users", [])

    if not users:
        return await event.edit(
            "👥 **Mere Users**\n\nAbhi koi user nahi hai.",
            buttons=[[Button.inline("🔙 Back", b"reseller_panel")]]
        )

    import time
    lines = []
    for uid in users[:30]:
        udata = get_user_data(uid)
        prem  = udata.get("premium", {})
        exp   = prem.get("expires_at")
        if exp is None and prem.get("active"):
            status = "♾️ Lifetime"
        elif exp and exp > time.time():
            import datetime
            days_left = int((exp - time.time()) / 86400)
            status = f"💎 {days_left}d left"
        else:
            status = "❌ Expired"
        lines.append(f"`{uid}` — {status}")

    text = "👥 **Mere Users:**\n\n" + "\n".join(lines)
    if len(users) > 30:
        text += f"\n\n...aur {len(users) - 30} users"

    await event.edit(text, buttons=[[Button.inline("🔙 Back", b"reseller_panel")]])


# ══════════════════════════════════════════════
# ADMIN — RESELLER MANAGEMENT
# ══════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_resellers"))
async def adm_resellers_panel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)

    from feature_flags import reseller_system_enabled
    if not reseller_system_enabled():
        return await event.edit(
            "👥 **Reseller System**\n\n🔴 Disabled hai.\n\nFeature Flags se enable karo.",
            buttons=[[Button.inline("⚙️ Feature Flags", b"adm_feature_flags"),
                      Button.inline("🔙 Back", b"adm_main")]]
        )

    summary = get_admin_reseller_summary()
    text = (
        "👥 **Reseller Management**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Total Resellers: `{summary['total']}`\n"
        f"🟢 Active: `{summary['active']}`\n"
        f"📦 Total Quota: `{summary['quota']}`\n"
        f"✅ Used: `{summary['used']}`\n"
        f"⬜ Remaining: `{summary['remaining']}`\n"
        f"💰 Total Earnings Tracked: `₹{summary['earnings']:.2f}`"
    )
    btns = [
        [Button.inline("➕ Add Reseller",      b"adm_add_reseller")],
        [Button.inline("📋 List Resellers",    b"adm_list_resellers")],
        [Button.inline("❌ Remove Reseller",   b"adm_remove_reseller_start")],
        [Button.inline("🔙 Admin Menu",        b"adm_main")],
    ]
    await event.edit(text, buttons=btns)


@bot.on(events.CallbackQuery(data=b"adm_add_reseller"))
async def adm_add_reseller_start(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    data = get_user_data(event.sender_id)
    data["step"] = "adm_add_reseller_step1"
    await _save_step(data)
    await event.edit(
        "➕ **Naya Reseller Add Karo**\n\n"
        "Step 1/3: Reseller ka Telegram User ID bhejo:\n\n"
        "(User ID @userinfobot se milega)",
        buttons=[[Button.inline("❌ Cancel", b"adm_resellers")]]
    )


@bot.on(events.CallbackQuery(data=b"adm_list_resellers"))
async def adm_list_resellers(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)

    resellers = get_all_resellers()
    if not resellers:
        return await event.edit(
            "📋 **Resellers:**\n\nKoi reseller nahi hai abhi.",
            buttons=[[Button.inline("➕ Add", b"adm_add_reseller"),
                      Button.inline("🔙 Back", b"adm_resellers")]]
        )

    lines = []
    for r in resellers[:20]:
        uid    = r["user_id"]
        status = "🟢" if r.get("active", True) else "🔴"
        lines.append(
            f"{status} `{uid}` | Q:{r['used']}/{r['quota']} "
            f"| {r['commission']}% comm | ₹{r.get('earnings', 0):.0f}"
        )

    text = "📋 **Resellers:**\n\n" + "\n".join(lines)
    btns = [
        [Button.inline("➕ Add", b"adm_add_reseller")],
        [Button.inline("🔙 Back", b"adm_resellers")],
    ]
    await event.edit(text, buttons=btns)


@bot.on(events.CallbackQuery(data=b"adm_remove_reseller_start"))
async def adm_remove_reseller_start(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    data = get_user_data(event.sender_id)
    data["step"] = "adm_remove_reseller"
    await _save_step(data)
    await event.edit(
        "❌ **Reseller Remove Karo**\n\n"
        "Reseller ka User ID bhejo:",
        buttons=[[Button.inline("❌ Cancel", b"adm_resellers")]]
    )
