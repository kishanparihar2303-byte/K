import time
# force_subscribe.py — FIXED VERSION
# ══════════════════════════════════════════
# Fixes:
# 1. is_user_subscribed: proper error handling, public + private channel
# 2. check_force_subscribe: called from ALL entry points
# 3. fs_check_again: shows main menu on success
# 4. Admin panel: channel set with validation + bot-admin check
# 5. Multiple channels support
# ══════════════════════════════════════════

from telethon import events, Button, errors
from telethon.tl.functions.channels import GetParticipantRequest
from telethon.tl.types import (
    ChannelParticipantBanned, ChannelParticipantLeft,
    ChannelParticipantCreator, ChannelParticipant,
    ChannelParticipantAdmin
)
from config import bot, OWNER_ID
from database import GLOBAL_STATE, get_user_data, save_persistent_db
from admin import is_admin, add_log
import logging

logger = logging.getLogger(__name__)

# Auto-verify tasks tracker — user_id → asyncio.Task
# Jab user manually "Maine Join Kar Liya" dabata hai, task cancel hoti hai
_FS_VERIFY_TASKS: dict = {}


def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_fs_config():
    GLOBAL_STATE.setdefault("force_sub", {
        "enabled":       False,
        "channel_id":    None,   # numeric ID (int or str of int)
        "channel_link":  None,   # join link — shown to user
        "channel_name":  "Our Channel",
        "bypass_admins": True,
        "channels":      [],     # multiple channels list [{id, link, name}]
    })
    return GLOBAL_STATE["force_sub"]


# ══════════════════════════════════════════
# CORE: Check if user is in channel
# ══════════════════════════════════════════

def _get_channel_id_variants(ch_int: int) -> list:
    """
    Ek channel ID ke -100 format variants return karo.
    Telethon ke liye:
      - Supergroup/Channel: -100XXXXXXXXXX format mein hote hain
      - Positive ID diya → -100 prefix laga ke try karo
    NOTE: Positive raw IDs kabhi mat dalo — Telethon unhe USER ID samajhta hai!
    """
    if ch_int > 0:
        # User ne bina -100 ke diya — convert karo
        return [-(1000000000000 + ch_int)]
    return [ch_int]  # Already negative — direct use karo


def _get_pure_channel_id(ch_int: int) -> int:
    """
    Telethon InputPeerChannel/InputChannel ke liye pure channel_id extract karo.
    -1001234567890 → 1234567890 (without -100 prefix)
    """
    s = str(ch_int)
    if s.startswith("-100"):
        return int(s[4:])
    if ch_int < 0:
        return abs(ch_int)
    return ch_int


async def is_user_subscribed(user_id: int, force_check: bool = False, admin_test: bool = False) -> tuple:
    """
    Returns (subscribed: bool, missing_channels: list)
    missing_channels = list of {name, link} that user hasn't joined

    force_check=True → admin bypass ignore hoga (testing ke liye)
    admin_test=True  → safety fallback bhi disable — real errors return hogi
                       (Test button se specific user check karne ke liye use karo)
    """
    config = get_fs_config()

    # Bypass for admins (unless force_check=True — for admin testing)
    if not force_check and config.get("bypass_admins") and (user_id == OWNER_ID or is_admin(user_id)):
        return True, []

    # Collect all channels to check
    channels = []

    # Legacy single channel
    if config.get("channel_id"):
        channels.append({
            "id":   config["channel_id"],
            "link": config.get("channel_link", ""),
            "name": config.get("channel_name", "Our Channel"),
        })

    # Multiple channels
    for ch in config.get("channels", []):
        if ch.get("id") and ch not in channels:
            channels.append(ch)

    if not channels:
        return True, []  # No channel configured — allow all

    missing = []
    for ch in channels:
        ch_id = ch["id"]

        # ── Step 1: Channel ID normalize karo ──────────────────────────
        try:
            ch_int = int(str(ch_id).strip())
        except (ValueError, TypeError):
            logger.warning(f"Force sub: invalid channel_id {ch_id!r}")
            continue

        # ── Step 2: Telethon entity fetch karo (access hash ke liye) ───
        # Raw integer pass karna unreliable hai — Telethon ko access hash
        # chahiye hoti hai API call ke liye. get_entity() se ye cache hoti hai.
        # Fallback: InputPeerChannel aur GetChannelsRequest se force server lookup.
        ch_entity = None

        # Method 1: Standard get_entity (local cache + server)
        for _try_id in _get_channel_id_variants(ch_int):
            try:
                ch_entity = await bot.get_entity(_try_id)
                break
            except Exception:
                continue

        # Method 2: InputPeerChannel with access_hash=0 — cache bypass karta hai
        # Ye tab kaam karta hai jab bot recently join hua ho aur entity cache mein na ho
        if ch_entity is None:
            try:
                from telethon.tl.types import InputPeerChannel
                pure_id = _get_pure_channel_id(ch_int)
                ch_entity = await bot.get_entity(
                    InputPeerChannel(channel_id=pure_id, access_hash=0)
                )
            except Exception:
                pass

        # Method 3: GetChannelsRequest — direct API call, always works if bot is member
        if ch_entity is None:
            try:
                from telethon.tl.functions.channels import GetChannelsRequest
                from telethon.tl.types import InputChannel
                pure_id = _get_pure_channel_id(ch_int)
                result = await bot(GetChannelsRequest([InputChannel(pure_id, access_hash=0)]))
                if result.chats:
                    ch_entity = result.chats[0]
            except Exception:
                pass

        if ch_entity is None:
            logger.error(f"Force sub: Channel {ch_id} resolve nahi hua — bot is channel mein nahi hai ya ID galat hai.")
            missing.append({**ch, "invalid": True})
            continue

        # ── Step 3: Membership check ────────────────────────────────────
        try:
            result = await bot(GetParticipantRequest(
                channel=ch_entity,
                participant=user_id
            ))
            p = result.participant
            if isinstance(p, (ChannelParticipantBanned, ChannelParticipantLeft)):
                missing.append(ch)
            # Creator, Admin, Member — sab OK hain
        except errors.UserNotParticipantError:
            missing.append(ch)
        except errors.UserIdInvalidError:
            continue
        except errors.ChatAdminRequiredError:
            logger.error(
                f"Force sub: Bot channel {ch_id} mein admin nahi hai! "
                f"Bot ko admin banao."
            )
            missing.append({**ch, "bot_not_admin": True})
        except (errors.ChannelInvalidError, errors.ChannelPrivateError):
            logger.error(f"Force sub: Channel {ch_id} invalid/private — bot member nahi hai.")
            missing.append({**ch, "invalid": True})
        except Exception as e:
            logger.warning(f"Force sub check error for {user_id} in {ch_id}: {e}")
            missing.append(ch)

    # Safety: agar saare channels "bot_not_admin" ya "invalid" hain
    if missing and all(ch.get("bot_not_admin") or ch.get("invalid") for ch in missing):
        if admin_test:
            # Test mode mein real errors dikhao — safety bypass mat karo
            # Warna test button hamesha "user subscribed" dikhata hai
            return False, missing
        logger.error(
            "Force Sub SAFETY: Sab channels misconfigured hain (bot admin nahi hai ya invalid). "
            "Sab users allow kar rahe hain. Admin ko fix karna hoga!"
        )
        return True, []  # Allow all — don't silently block entire userbase

    return len(missing) == 0, missing


# ══════════════════════════════════════════
# MAIN CHECK — called from /start + all other messages
# ══════════════════════════════════════════

async def check_force_subscribe(event) -> bool:
    """
    Returns True = user can proceed, False = blocked (message already sent).
    Call this at the TOP of every handler.
    """
    config = get_fs_config()
    if not config.get("enabled"):
        return True

    user_id = event.sender_id
    subscribed, missing = await is_user_subscribed(user_id)

    if subscribed:
        return True

    # Build join buttons
    btns = []
    for ch in missing:
        link = ch.get("link", "")
        name = ch.get("name", "Channel")
        if ch.get("bot_not_admin"):
            btns.append([Button.inline(f"⚠️ {name} (Bot error)", b"fs_bot_not_admin")])
        elif link:
            btns.append([Button.url(f"✅ Join {name}", link)])
        else:
            btns.append([Button.inline(f"📢 {name} — Link nahi diya gaya", b"fs_no_link")])

    # "Maine Join Kar Liya" button — manual verify ke liye
    btns.append([Button.inline("🔄 Maine Join Kar Liya ✅", b"fs_check_again")])

    not_joined_names = ", ".join(ch["name"] for ch in missing)

    try:
        msg = (
            "⛔ **Pehle Join Karo!**\n"
            "━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Bot use karne ke liye ye channel(s) join karna zaroori hai:\n\n"
            f"📢 **{not_joined_names}**\n\n"
            "✅ Join karne ke baad **'Maine Join Kar Liya'** button dabao.\n\n" + _get_owner_footer()
        )
        # Try to edit if it's a callback, else respond
        if hasattr(event, "data"):
            try:
                await event.edit(msg, buttons=btns)
                return False
            except Exception:
                pass
        sent_msg = await event.respond(msg, buttons=btns)

        # ── Auto-verify: background mein check karte raho ─────────────────
        # Task ko track karo — agar user manually button dabae toh cancel karo
        import asyncio as _aio
        user_id_fs = event.sender_id

        # Pehle ka koi task ho toh cancel karo (double message situation)
        _prev_task = _FS_VERIFY_TASKS.pop(user_id_fs, None)
        if _prev_task and not _prev_task.done():
            _prev_task.cancel()

        async def _auto_verify():
            try:
                for _ in range(30):  # Max 90 seconds (30 x 3s)
                    await _aio.sleep(3)
                    # Agar task cancel ho gayi (manual verify) toh rok do
                    if _aio.current_task().cancelled():
                        return
                    try:
                        ok, _ = await is_user_subscribed(user_id_fs)
                        if ok:
                            # User ne join kar liya — message update karo
                            _FS_VERIFY_TASKS.pop(user_id_fs, None)
                            try:
                                await sent_msg.delete()
                            except Exception:
                                pass
                            try:
                                from ui.main_menu import get_main_buttons
                                await bot.send_message(
                                    user_id_fs,
                                    "✅ **Verified! Channel join ho gaya.**\n\n"
                                    "🎉 Ab tum bot use kar sakte ho!\n\n" + _get_owner_footer(),
                                    buttons=get_main_buttons(user_id_fs)
                                )
                            except Exception:
                                pass
                            return
                    except Exception:
                        pass  # Next iteration mein retry
            except _aio.CancelledError:
                pass  # Manual verify ne cancel kiya — normal hai
            finally:
                _FS_VERIFY_TASKS.pop(user_id_fs, None)

        task = _aio.create_task(_auto_verify())
        _FS_VERIFY_TASKS[user_id_fs] = task
        return False
    except Exception as e:
        logger.warning(f"Force sub message send error: {e}")
        try:
            plain = f"⛔ Pehle join karo: {not_joined_names}\nJoin karne ke baad /start bhejo."
            await event.respond(plain)
        except Exception:
            pass

    return False


async def check_force_subscribe_cb(event) -> bool:
    """
    Callback query version — same as above but uses edit.
    Returns True = allow, False = blocked.
    """
    config = get_fs_config()
    if not config.get("enabled"):
        return True

    user_id = event.sender_id
    if config.get("bypass_admins") and (user_id == OWNER_ID or is_admin(user_id)):
        return True

    subscribed, missing = await is_user_subscribed(user_id)
    if subscribed:
        return True

    # Blocked — show join buttons
    btns = []
    for ch in missing:
        link = ch.get("link", "")
        name = ch.get("name", "Channel")
        if link:
            btns.append([Button.url(f"✅ Join {name}", link)])
        else:
            btns.append([Button.inline(f"📢 {name}", b"fs_no_link")])

    btns.append([Button.inline("🔄 Maine Join Kar Liya", b"fs_check_again")])

    not_joined_names = ", ".join(ch["name"] for ch in missing)
    try:
        await event.edit(
            f"⛔ **Pehle Join Karo!**\n\n"
            f"📢 **{not_joined_names}** join karo, phir check karo.",
            buttons=btns
        )
    except Exception:
        try:
            await event.respond(
                f"⛔ Pehle join karo: **{not_joined_names}**",
                buttons=btns
            )
        except Exception:
            pass
    return False


# ══════════════════════════════════════════
# CALLBACK: Check Again button
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"fs_check_again"))
async def fs_check_again(event):
    await event.answer()
    user_id = event.sender_id

    # Auto-verify task cancel karo — manual check ho raha hai
    _prev = _FS_VERIFY_TASKS.pop(user_id, None)
    if _prev and not _prev.done():
        _prev.cancel()

    subscribed, missing = await is_user_subscribed(user_id)

    if subscribed:
        await event.answer("✅ Verified! Ab bot use kar sakte ho.", alert=True)
        try:
            await event.delete()
        except Exception:
            pass
        # Show main menu
        try:
            from ui.main_menu import get_main_buttons
            data = get_user_data(user_id)
            await bot.send_message(
                user_id,
                "✅ **Verification ho gayi!**\n\nMain Menu:",
                buttons=get_main_buttons(user_id)
            )
        except Exception:
            pass
    else:
        config = get_fs_config()
        missing_names = ", ".join(ch["name"] for ch in missing)
        btns = []
        for ch in missing:
            link = ch.get("link", "")
            name = ch.get("name", "Channel")
            if ch.get("bot_not_admin"):
                await event.answer(
                    "⚠️ Bot error: Admin ne channel sahi configure nahi kiya. Admin se contact karo.",
                    alert=True
                )
                return
            if link:
                btns.append([Button.url(f"✅ Join {name}", link)])
        btns.append([Button.inline("🔄 Phir Check Karo", b"fs_check_again")])
        try:
            await event.edit(
                f"❌ **Abhi join nahi kiya!**\n\n"
                f"📢 Ye channels join karo: **{missing_names}**\n\n"
                f"Join karne ke baad \'Phir Check Karo\' dabao.",
                buttons=btns
            )
        except Exception:
            await event.answer(
                f"❌ Pehle join karo: {missing_names}", alert=True
            )


@bot.on(events.CallbackQuery(data=b"fs_bot_not_admin"))
async def fs_bot_not_admin_cb(event):
    await event.answer()
    await event.answer(
        "⚠️ Configuration error: Bot channel mein admin nahi hai. "
        "Admin se contact karo.",
        alert=True
    )


@bot.on(events.CallbackQuery(data=b"fs_no_link"))
async def fs_no_link_cb(event):
    await event.answer()
    await event.answer(
        "❌ Is channel ka join link set nahi kiya gaya. Admin se link maango.",
        alert=True
    )


# ══════════════════════════════════════════
# ADMIN PANEL
# ══════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"adm_force_sub"))
async def adm_force_sub_panel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    config = get_fs_config()
    enabled = config.get("enabled", False)

    # Count channels
    channels = []
    if config.get("channel_id"):
        channels.append({
            "id":   config["channel_id"],
            "link": config.get("channel_link", ""),
            "name": config.get("channel_name", "Our Channel"),
        })
    channels += config.get("channels", [])

    ch_lines = []
    for i, ch in enumerate(channels):
        ch_lines.append(f"  {i+1}. **{ch.get('name','?')[:20]}** — `{ch.get('id','?')}`")
    ch_text = "\n".join(ch_lines) if ch_lines else "  ❌ Koi channel set nahi"

    txt = (
        "📢 **Force Subscribe / Join**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: **{'✅ ON' if enabled else '❌ OFF'}**\n"
        f"Admin Bypass: **{'✅ Yes' if config.get('bypass_admins') else '❌ No'}**\n\n"
        f"**Channels ({len(channels)}):**\n{ch_text}\n\n"
        "⚠️ Bot ko channel mein **Admin** banana zaroori hai\n"
        "(warna membership check nahi kar payega)\n\n" + _get_owner_footer()
    )

    btns = [
        [Button.inline(f"{'🔴 Band Karo' if enabled else '🟢 Chalu Karo'}", b"adm_fs_toggle")],
        [Button.inline("➕ Channel Add Karo", b"adm_fs_add_channel")],
        [Button.inline("🗑 Channels Hatao", b"adm_fs_remove_channels")],
        [Button.inline("✅ Test Karo (Khud Check)", b"adm_fs_test")],
        [Button.inline(f"{'❌ Admin Bypass Band' if config.get('bypass_admins') else '✅ Admin Bypass Chalu'}", b"adm_fs_bypass_toggle")],
        [Button.inline("🔙 Admin Panel", b"adm_main")],
    ]

    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        await event.answer("Refreshed!")


@bot.on(events.CallbackQuery(data=b"adm_fs_toggle"))
async def adm_fs_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_fs_config()
    config["enabled"] = not config.get("enabled", False)
    save_persistent_db()
    status = "ON ✅" if config["enabled"] else "OFF ❌"
    add_log(event.sender_id, "Force Subscribe Toggle", details=status)
    await event.answer(f"Force Subscribe {status}!", alert=True)
    await adm_force_sub_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_fs_test_as_user"))
async def adm_fs_test_as_user(event):
    """
    Admin ke liye full preview:
    - Admin bypass ignore karke check karta hai
    - Exactly wahi UI dikhata hai jo ek blocked user dekhega
    - Agar admin khud joined hai to 'all clear' dikhata hai
    """
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    await event.answer("Checking your membership...")

    config = get_fs_config()
    if not config.get("enabled"):
        return await event.edit(
            "ℹ️ **Force Subscribe abhi OFF hai.**\n\n"
            "Pehle ON karo, phir test karo.",
            buttons=[[Button.inline("🔙 Back", b"adm_force_sub")]]
        )

    # force_check=True → admin bypass ignore, real membership check
    # admin_test=True  → safety fallback disable — real error result mile
    subscribed, missing = await is_user_subscribed(event.sender_id, force_check=True, admin_test=True)

    if subscribed:
        await event.edit(
            "✅ **Preview Result: All Clear!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Aap sabhi configured channels mein already joined hain.\n\n"
            "👤 **Regular user experience:**\n"
            "Koi bhi user jo sabhi channels join kar chuka hai — use force subscribe prompt **nahi dikhega**. Woh seedha bot use kar sakta hai. ✅\n\n"
            "💡 Kisi specific user ko test karna ho to uska Telegram ID niche type karo:",
            buttons=[
                [Button.inline("🔍 Kisi User Ko Test Karo", b"adm_fs_test_user_id")],
                [Button.inline("🔙 Back", b"adm_fs_test")],
            ]
        )
    else:
        # Show exactly what a blocked user would see
        not_joined_names = ", ".join(ch["name"] for ch in missing)
        btns = []
        for ch in missing:
            link = ch.get("link", "")
            name = ch.get("name", "Channel")
            if ch.get("bot_not_admin"):
                btns.append([Button.inline(f"⚠️ {name} (Bot not admin!)", b"fs_bot_not_admin")])
            elif link:
                btns.append([Button.url(f"✅ Join {name}", link)])
            else:
                btns.append([Button.inline(f"📢 {name} — Link set nahi", b"fs_no_link")])
        btns.append([Button.inline("🔄 Maine Join Kar Liya ✅", b"adm_fs_reverify")])
        btns.append([Button.inline("🔙 Back to Test", b"adm_fs_test")])

        await event.edit(
            "👁 **[ADMIN PREVIEW] — Yahi dikhega ek blocked user ko:**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⛔ **Pehle Join Karo!**\n\n"
            f"Bot use karne ke liye ye channel(s) join karna zaroori hai:\n\n"
            f"📢 **{not_joined_names}**\n\n"
            "✅ Join karne ke baad **'Maine Join Kar Liya'** button dabao.\n\n"
            "─────────────────────\n"
            "ℹ️ **[Admin Note]** Aap khud in channels mein nahi hain (isliye yeh dikh raha hai). "
            "Channels join karo → phir 'Maine Join Kar Liya' dabao → verify ho jayega.",
            buttons=btns
        )


@bot.on(events.CallbackQuery(data=b"adm_fs_reverify"))
async def adm_fs_reverify(event):
    """Admin test ke baad re-verify — force_check=True se check karta hai."""
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    await event.answer("Checking...")
    subscribed, missing = await is_user_subscribed(event.sender_id, force_check=True, admin_test=True)

    if subscribed:
        await event.edit(
            "✅ **Verified! Sab channels join ho gaye.**\n\n"
            "Force Subscribe system sahi kaam kar raha hai.\n"
            "Ab regular users bhi verify ho payenge.",
            buttons=[[Button.inline("🔙 Force Sub Panel", b"adm_force_sub")]]
        )
    else:
        missing_names = ", ".join(ch["name"] for ch in missing)
        await event.answer(
            f"❌ Abhi bhi join nahi kiya: {missing_names}",
            alert=True
        )


@bot.on(events.CallbackQuery(data=b"adm_fs_test_user_id"))
async def adm_fs_test_user_id_prompt(event):
    """Admin se user ID maango specific user ko test karne ke liye."""
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_fs_test_uid_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    save_persistent_db()
    await event.edit(
        "🔍 **Kisi User Ka Test**\n\n"
        "Jis user ko test karna hai uska **Telegram User ID** bhejo.\n"
        "Example: `123456789`\n\n"
        "User ID kaise milega? @userinfobot se pata kar sakte ho.",
        buttons=[Button.inline("🔙 Cancel", b"adm_fs_test")]
    )


@bot.on(events.CallbackQuery(data=b"adm_fs_bypass_toggle"))
async def adm_fs_bypass_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_fs_config()
    config["bypass_admins"] = not config.get("bypass_admins", True)
    save_persistent_db()
    await event.answer(f"Admin bypass: {'ON' if config['bypass_admins'] else 'OFF'}")
    await adm_force_sub_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_fs_add_channel"))
async def adm_fs_add_channel(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_fs_channel_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    save_persistent_db()
    await event.edit(
        "📝 **Naya Channel Add Karo**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**3 cheezein bhejo — ek line mein, | se alag karke:**\n\n"
        "`Channel_ID | Join_Link | Channel_Name`\n\n"
        "**Examples:**\n"
        "`-1001234567890 | https://t.me/mychannel | My Channel`\n"
        "`-1009876543210 | https://t.me/+abcXYZ | Private Chan`\n\n"
        "**Channel ID kaise milega?**\n"
        "• @userinfobot se forward karo — ID milega\n"
        "• Ya `@username` likho — bot resolve karega\n\n"
        "⚠️ **Zaruri:** Bot channel mein Admin hona chahiye!",
        buttons=[Button.inline("🔙 Cancel", b"adm_force_sub")]
    )


@bot.on(events.CallbackQuery(data=b"adm_fs_remove_channels"))
async def adm_fs_remove_channels(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_fs_config()

    # Collect all channels
    channels = []
    if config.get("channel_id"):
        channels.append({
            "id":   config["channel_id"],
            "link": config.get("channel_link", ""),
            "name": config.get("channel_name", "Our Channel"),
            "_main": True,
        })
    channels += [dict(ch, _main=False) for ch in config.get("channels", [])]

    if not channels:
        return await event.answer("Koi channel set nahi hai!", alert=True)

    btns = []
    for i, ch in enumerate(channels):
        btns.append([Button.inline(
            f"🗑 {ch.get('name','?')[:30]} ({ch.get('id','?')})",
            f"adm_fs_del_ch_{i}".encode()
        )])
    btns.append([Button.inline("🗑 Sab Hatao", b"adm_fs_clear_all")])
    btns.append([Button.inline("🔙 Back", b"adm_force_sub")])

    await event.edit("🗑 Kaunsa channel hatana hai?", buttons=btns)


@bot.on(events.CallbackQuery(pattern=b"adm_fs_del_ch_"))
async def adm_fs_del_ch(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    idx = int(event.data.decode().replace("adm_fs_del_ch_", ""))
    config = get_fs_config()

    channels = []
    if config.get("channel_id"):
        channels.append({"_main": True})
    for ch in config.get("channels", []):
        channels.append(dict(ch, _main=False))

    if idx >= len(channels):
        return await event.answer("Invalid index", alert=True)

    ch = channels[idx]
    if ch.get("_main"):
        config["channel_id"]   = None
        config["channel_link"] = None
        config["channel_name"] = "Our Channel"
    else:
        extra_idx = idx - (1 if config.get("channel_id") else 0)
        if 0 <= extra_idx < len(config.get("channels", [])):
            config["channels"].pop(extra_idx)

    save_persistent_db()
    await event.answer("✅ Channel hata diya!", alert=True)
    await adm_force_sub_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_fs_clear_all"))
async def adm_fs_clear_all(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_fs_config()
    config["channel_id"]   = None
    config["channel_link"] = None
    config["channel_name"] = "Our Channel"
    config["channels"]     = []
    save_persistent_db()
    await event.answer("✅ Sab channels hata diye!", alert=True)
    await adm_force_sub_panel(event)


@bot.on(events.CallbackQuery(data=b"adm_fs_test"))
async def adm_fs_test(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_fs_config()

    channels = []
    if config.get("channel_id"):
        channels.append({
            "id":   config["channel_id"],
            "link": config.get("channel_link", ""),
            "name": config.get("channel_name", "Our Channel"),
        })
    channels += config.get("channels", [])

    if not channels:
        return await event.answer("Koi channel set nahi hai!", alert=True)

    # --- Bot admin role check ---
    bot_results = []
    for ch in channels:
        try:
            ch_int = int(str(ch["id"]).strip())
            from telethon.tl.functions.channels import GetParticipantRequest as GPR
            # Entity resolve karo — same multi-method approach
            ch_entity = None
            for _v in _get_channel_id_variants(ch_int):
                try:
                    ch_entity = await bot.get_entity(_v)
                    break
                except Exception:
                    continue
            if ch_entity is None:
                try:
                    from telethon.tl.types import InputPeerChannel
                    ch_entity = await bot.get_entity(
                        InputPeerChannel(channel_id=_get_pure_channel_id(ch_int), access_hash=0)
                    )
                except Exception:
                    pass
            if ch_entity is None:
                try:
                    from telethon.tl.functions.channels import GetChannelsRequest
                    from telethon.tl.types import InputChannel
                    res = await bot(GetChannelsRequest([InputChannel(_get_pure_channel_id(ch_int), access_hash=0)]))
                    if res.chats:
                        ch_entity = res.chats[0]
                except Exception:
                    pass
            if ch_entity is None:
                bot_results.append(f"• **{ch.get('name')}**: ❌ Channel resolve nahi hua — ID check karo")
                continue
            r = await bot(GPR(channel=ch_entity, participant="me"))
            p = r.participant
            if isinstance(p, (ChannelParticipantCreator, ChannelParticipantAdmin)):
                role = "✅ Admin — membership check kaam karega"
            elif isinstance(p, ChannelParticipant):
                role = "⚠️ Sirf Member — Bot ko Admin banao!"
            else:
                role = "❌ Banned/Left — Bot channel mein nahi hai"
            bot_results.append(f"• **{ch.get('name')}**: {role}")
        except errors.UserNotParticipantError:
            bot_results.append(f"• **{ch.get('name')}**: ❌ Bot channel mein nahi hai!")
        except Exception as e:
            bot_results.append(f"• **{ch.get('name')}**: ❌ Error: {str(e)[:50]}")

    # --- Admin's own membership check (force_check=True — bypass ignore) ---
    try:
        admin_subscribed, admin_missing = await is_user_subscribed(event.sender_id, force_check=True, admin_test=True)
        if admin_subscribed:
            my_status = "✅ Aap sabhi channels mein already joined hain"
        else:
            missing_names = ", ".join(ch["name"] for ch in admin_missing)
            my_status = f"❌ Aap in channels mein nahi hain: **{missing_names}**"
    except Exception as e:
        my_status = f"⚠️ Check nahi ho saka: {str(e)[:60]}"

    txt = (
        "🔍 **Force Subscribe — Test Results**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Bot Ki Channel Role:**\n" +
        "\n".join(bot_results) + "\n\n"
        "**Aapki Apni Membership (Admin bypass ignore):**\n"
        f"{my_status}\n\n"
        "💡 **'Preview as User'** dabao — exactly wahi dikhega jo ek blocked user ko dikhta hai."
    )

    await event.edit(
        txt,
        buttons=[
            [Button.inline("👤 Preview as User (Full Test)", b"adm_fs_test_as_user")],
            [Button.inline("🔄 Refresh", b"adm_fs_test")],
            [Button.inline("🔙 Back", b"adm_force_sub")],
        ]
    )


# ══════════════════════════════════════════
# INPUT HANDLER for setting channel
# ══════════════════════════════════════════

async def handle_fs_inputs(event, user_id, step) -> bool:
    # --- Test specific user ID ---
    if step == "adm_fs_test_uid_input":
        text = event.raw_text.strip() if event.raw_text else ""
        try:
            test_uid = int(text)
        except ValueError:
            await event.respond(
                "❌ Galat format! Sirf numeric User ID bhejo.\nExample: `123456789`"
            )
            return True

        subscribed, missing = await is_user_subscribed(test_uid, force_check=True, admin_test=True)
        if subscribed:
            result_txt = f"✅ User `{test_uid}` — **Subscribed hai**, bot use kar sakta hai."
        else:
            missing_names = ", ".join(ch["name"] for ch in missing)
            result_txt = f"❌ User `{test_uid}` — **Blocked hoga**, in channels mein nahi: **{missing_names}**"

        get_user_data(user_id)["step"] = None
        save_persistent_db()
        await event.respond(
            f"🔍 **Force Sub Check — User {test_uid}**\n\n{result_txt}",
            buttons=[Button.inline("📢 Force Sub Panel", b"adm_force_sub")]
        )
        return True

    if step != "adm_fs_channel_input":
        return False

    text = event.raw_text.strip() if event.raw_text else ""
    parts = [p.strip() for p in text.split("|")]

    if len(parts) < 1 or not parts[0]:
        await event.respond(
            "❌ Format galat hai!\n"
            "Sahi format: `Channel_ID | Join_Link | Channel_Name`"
        )
        return True

    raw_id   = parts[0]
    link     = parts[1] if len(parts) > 1 else ""
    name     = parts[2] if len(parts) > 2 else "Our Channel"

    # Resolve @username to numeric ID
    resolved_id = raw_id
    if raw_id.startswith("@") or (not raw_id.lstrip("-").isdigit()):
        try:
            entity = await bot.get_entity(raw_id)
            resolved_id = str(-1000000000000 - entity.id) if hasattr(entity, "id") else raw_id
            if not name or name == "Our Channel":
                name = getattr(entity, "title", name)
        except Exception as e:
            await event.respond(
                f"❌ Channel resolve nahi ho saka: `{raw_id}`\n"
                f"Error: `{str(e)[:80]}`\n\n"
                f"Numeric ID use karo (e.g. `-1001234567890`)"
            )
            return True

    # Validate numeric
    try:
        int_id = int(str(resolved_id).strip())
    except ValueError:
        await event.respond(
            f"❌ Channel ID galat format mein hai: `{raw_id}`\n"
            f"Numeric ID chahiye jaise `-1001234567890`"
        )
        return True

    config = get_fs_config()

    # Check if already exists → update, else add
    if config.get("channel_id") and str(config["channel_id"]) == str(int_id):
        config["channel_id"]   = int_id
        config["channel_link"] = link
        config["channel_name"] = name
    else:
        config.setdefault("channels", [])
        existing_ids = [str(c["id"]) for c in config["channels"]]
        if str(int_id) in existing_ids:
            # Update existing
            for c in config["channels"]:
                if str(c["id"]) == str(int_id):
                    c["link"] = link
                    c["name"] = name
        else:
            # First channel? Set as main, else add to list
            if not config.get("channel_id"):
                config["channel_id"]   = int_id
                config["channel_link"] = link
                config["channel_name"] = name
            else:
                config["channels"].append({"id": int_id, "link": link, "name": name})

    get_user_data(user_id)["step"] = None
    save_persistent_db()
    add_log(user_id, "Force Sub Channel Set", details=f"{name} ({int_id})")

    await event.respond(
        f"✅ **Channel Add Ho Gaya!**\n\n"
        f"📢 Name: **{name}**\n"
        f"🆔 ID: `{int_id}`\n"
        f"🔗 Link: {link or '(set nahi)'} \n\n"
        f"⚠️ Bot ko is channel mein **Admin** banao warna check kaam nahi karega.\n"
        f"Test karne ke liye: Admin Panel → Force Subscribe → ✅ Test Karo",
        buttons=[Button.inline("📢 Force Sub Panel", b"adm_force_sub")]
    )
    return True
