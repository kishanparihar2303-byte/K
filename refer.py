import time
# bot/refer.py
import time  # BUG 36 FIX: time import missing tha — referral crash fix
import asyncio
from telethon import events, Button, errors
from config import bot, OWNER_ID
from database import db, GLOBAL_STATE, get_user_data, save_persistent_db
from admin import is_admin, add_log
from premium import is_premium_user, get_premium_config


def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_refer_settings():
    GLOBAL_STATE.setdefault("refer_settings", {
        "enabled": True,
        "reward_type": "premium",  # "premium" ya "none"
        "reward_days": 7,
        "referrals_needed": 3,
        "group_id": None,
        "group_link": None,
    })
    return GLOBAL_STATE["refer_settings"]


def get_bot_link():
    return GLOBAL_STATE.get("bot_username", "YourBot")


async def process_referral(new_user_id: int, referrer_id: int):
    """Referral process karo jab naya user join kare."""
    if new_user_id == referrer_id:
        return
    # BUG 15 FIX: int aur str dono keys check karo
    if referrer_id not in db and str(referrer_id) not in db:
        return

    new_user_data = get_user_data(new_user_id)
    referrer_data = get_user_data(referrer_id)

    # Pehle se referred? Skip
    if new_user_data["refer"].get("referred_by"):
        return

    new_user_data["refer"]["referred_by"] = referrer_id

    if new_user_id not in referrer_data["refer"]["referred_users"]:
        referrer_data["refer"]["referred_users"].append(new_user_id)

    settings = get_refer_settings()
    needed = settings.get("referrals_needed", 3)
    total_referred = len(referrer_data["refer"]["referred_users"])

    save_persistent_db()

    # ✅ Task Board: Referral coin bonus
    try:
        from task_board import referral_coin_bonus
        referral_coin_bonus(referrer_id)
    except Exception:
        pass

    # Auto group add
    # BUG 38 FIX: try_add_to_group ab call hota hai
    await try_add_to_group(new_user_id)

    # Referrer ko notify karo
    try:
        await bot.send_message(
            referrer_id,
            f"🎉 **Naya Referral!**\n\n"
            f"Ek naya user tumhare link se join kiya!\n"
            f"Total referrals: **{total_referred}/{needed}**\n\n"
            f"{'✅ Reward milne wala hai!' if total_referred >= needed else f'{needed - total_referred} aur chahiye reward ke liye।'}"
        )
    except Exception:
        pass

    # Reward check
    if total_referred >= needed and settings.get("reward_type") == "premium":
        already_rewarded = referrer_data["refer"].get("reward_claimed", 0)
        rewards_earned = total_referred // needed
        if rewards_earned > already_rewarded:
            referrer_data["refer"]["reward_claimed"] = rewards_earned
            reward_days = settings.get("reward_days", 7)
            prem = referrer_data.setdefault("premium", {})
            now = int(time.time())  # BUG 36 FIX: time.time() crash fix
            if prem.get("active") and prem.get("expires_at") and prem["expires_at"] > now:
                prem["expires_at"] += reward_days * 86400
            else:
                prem["active"] = True
                prem["expires_at"] = now + (reward_days * 86400)
                prem["plan"] = "Referral Reward"
                prem["given_by"] = 0
                prem["given_at"] = now
            save_persistent_db()
            try:
                await bot.send_message(
                    referrer_id,
                    f"🎁 **Referral Reward Mila!**\n\n"
                    f"✅ {reward_days} din ka Premium add ho gaya!\n"
                    f"Congratulations! 🎉\n\n" + _get_owner_footer()
                )
            except Exception:
                pass


async def try_add_to_group(user_id: int):
    """BUG 38 FIX: Auto group add — ab actually call hota hai process_referral mein."""
    settings = get_refer_settings()
    group_link = settings.get("group_link")
    if not group_link:
        return
    try:
        await bot.send_message(
            user_id,
            f"👋 **Welcome!**\n\n"
            f"Hamare group se judne ke liye ye link use karo:\n{group_link}"
        )
    except Exception:
        pass


async def handle_start_referral(event, start_param: str):
    """BUG 20 + BUG 38 FIX: /start ke saath referral process karo."""
    user_id = event.sender_id
    if start_param and start_param.startswith("ref_"):
        try:
            referrer_id = int(start_param.replace("ref_", ""))
            await process_referral(user_id, referrer_id)
        except Exception:
            pass


@bot.on(events.CallbackQuery(data=b"refer_menu"))
async def refer_menu(event):
    await event.answer()
    user_id = event.sender_id
    settings = get_refer_settings()
    if not settings.get("enabled"):
        return await event.answer("Referral system band hai abhi।", alert=True)
    data = get_user_data(user_id)
    total = len(data["refer"].get("referred_users", []))
    # BUG 7 FIX: referrals_needed use karo, reward_days nahi
    needed = settings.get("referrals_needed", 3)
    reward_days = settings.get("reward_days", 7)
    bot_username = get_bot_link()
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    txt = (
        "👥 **Referral Program**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Tumhara Referral Link:\n`{ref_link}`\n\n"
        f"📊 Total Referrals: **{total}**\n"
        f"🎁 Reward: **{reward_days} din Premium** (har {needed} referrals par)\n\n"
        "Apne dost ko link share karo!\n\n" + _get_owner_footer()
    )
    await event.edit(txt, buttons=[
        [Button.inline("🔗 Copy Link", b"ref_copy_link")],
        [Button.inline("📊 My Referrals", b"ref_stats")],
        [Button.inline("🏠 Main Menu", b"main_menu")]
    ])


@bot.on(events.CallbackQuery(data=b"ref_copy_link"))
async def ref_copy_link_cb(event):
    """Show referral link as copyable text — was missing handler (FIXED)."""
    await event.answer()
    user_id = event.sender_id
    try:
        from config import BOT_TOKEN
        bot_username = (await event.client.get_me()).username
    except Exception:
        bot_username = "your_bot"
    ref_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
    try:
        await event.answer(
            f"🔗 Apna link copy karo:\n{ref_link}",
            alert=True
        )
    except Exception:
        pass


@bot.on(events.CallbackQuery(data=b"ref_stats"))
async def ref_stats(event):
    await event.answer()
    user_id = event.sender_id
    data = get_user_data(user_id)
    referred = data["refer"].get("referred_users", [])
    claimed = data["refer"].get("reward_claimed", 0)
    settings = get_refer_settings()
    needed = settings.get("referrals_needed", 3)
    txt = (
        f"📊 **Tumhare Referrals**\n\n"
        f"Total: **{len(referred)}**\n"
        f"Rewards Claimed: **{claimed}**\n"
        f"Agle reward ke liye: **{needed - (len(referred) % needed)} aur chahiye**\n\n" + _get_owner_footer()
    )
    await event.edit(txt, buttons=[[Button.inline("🔙 Back", b"refer_menu")]])


@bot.on(events.CallbackQuery(data=b"adm_refer_panel"))
async def adm_refer_panel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    settings = get_refer_settings()
    txt = (
        "👥 **Referral System Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"Status: {'✅ ON' if settings.get('enabled') else '❌ OFF'}\n"
        f"Reward Type: `{settings.get('reward_type', 'premium')}`\n"
        f"Reward Days: `{settings.get('reward_days', 7)}`\n"
        f"Referrals Needed: `{settings.get('referrals_needed', 3)}`\n"
        f"Group ID: `{settings.get('group_id', 'Not Set')}`\n\n" + _get_owner_footer()
    )
    btns = [
        [Button.inline("🔄 Toggle ON/OFF", b"adm_refer_toggle")],
        [Button.inline("🎁 Reward Days", b"adm_refer_days"),
         Button.inline("👥 Needed Count", b"adm_refer_needed")],
        [Button.inline("🏢 Group ID Set", b"adm_refer_group")],
        [Button.inline("🏆 Reward Settings", b"adm_reward_settings")],  # BUG 41 FIX: button add
        [Button.inline("🔙 Back", b"adm_main")]
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


# BUG 41 FIX: adm_reward_settings panel ka button add kiya (pehle unreachable tha)
@bot.on(events.CallbackQuery(data=b"adm_reward_settings"))
async def adm_reward_settings(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    settings = get_refer_settings()
    txt = (
        "🏆 **Reward Settings**\n\n"
        f"Reward Type: `{settings.get('reward_type', 'premium')}`\n"
        f"Reward Days: `{settings.get('reward_days', 7)}`\n"
        f"Referrals Needed: `{settings.get('referrals_needed', 3)}`\n\n"
        "Reward type change karo:"
    )
    await event.edit(txt, buttons=[
        [Button.inline("💎 Premium Reward", b"adm_reward_premium"),
         Button.inline("❌ No Reward", b"adm_reward_none")],
        [Button.inline("🔙 Back", b"adm_refer_panel")]
    ])


@bot.on(events.CallbackQuery(data=b"adm_reward_premium"))
async def adm_reward_premium(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    get_refer_settings()["reward_type"] = "premium"
    save_persistent_db()
    await event.answer("✅ Reward type: Premium!")
    await adm_reward_settings(event)


@bot.on(events.CallbackQuery(data=b"adm_reward_none"))
async def adm_reward_none(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    get_refer_settings()["reward_type"] = "none"
    save_persistent_db()
    await event.answer("✅ Reward type: None!")
    await adm_reward_settings(event)


@bot.on(events.CallbackQuery(data=b"adm_refer_toggle"))
async def adm_refer_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    settings = get_refer_settings()
    settings["enabled"] = not settings.get("enabled", True)
    save_persistent_db()
    await event.answer(f"Referral system {'ON' if settings['enabled'] else 'OFF'}!")
    await adm_refer_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_refer_days"))
async def adm_refer_days(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_refer_days_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "🎁 **Reward Days**\n\nKitne din ka premium reward milega? (number):",
        buttons=[Button.inline("🔙 Cancel", b"adm_refer_panel")]
    )


@bot.on(events.CallbackQuery(data=b"adm_refer_needed"))
async def adm_refer_needed(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_refer_needed_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "👥 **Referrals Needed**\n\nKitne referrals par reward milega? (number):",
        buttons=[Button.inline("🔙 Cancel", b"adm_refer_panel")]
    )


@bot.on(events.CallbackQuery(data=b"adm_refer_group"))
async def adm_refer_group(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("❌ No permission", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_refer_group_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "🏢 **Group ID Set Karo**\n\nFormat:\n`GROUP_ID | INVITE_LINK`\n\nExample:\n`-1001234567890 | https://t.me/+xxxx`",
        buttons=[Button.inline("🔙 Cancel", b"adm_refer_panel")]
    )


async def handle_refer_inputs(event, user_id, step) -> bool:
    settings = get_refer_settings()
    if step == "adm_refer_days_input":
        try:
            days = int(event.text.strip())
            settings["reward_days"] = days
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(f"✅ Reward: {days} din ka premium!", buttons=[Button.inline("🔙 Back", b"adm_refer_panel")])
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True
    elif step == "adm_refer_needed_input":
        try:
            needed = int(event.text.strip())
            settings["referrals_needed"] = needed
            get_user_data(user_id)["step"] = None
            save_persistent_db()
            await event.respond(f"✅ {needed} referrals chahiye ab!", buttons=[Button.inline("🔙 Back", b"adm_refer_panel")])
        except ValueError:
            await event.respond("❌ Valid number bhejo।")
        return True
    elif step == "adm_refer_group_input":
        if "|" in event.text:
            parts = event.text.split("|", 1)
            settings["group_id"] = parts[0].strip()
            settings["group_link"] = parts[1].strip()
        else:
            settings["group_id"] = event.text.strip()
        get_user_data(user_id)["step"] = None
        save_persistent_db()
        await event.respond("✅ Group settings save ho gaya!", buttons=[Button.inline("🔙 Back", b"adm_refer_panel")])
        return True
    return False
