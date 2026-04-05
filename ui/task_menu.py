import asyncio
"""
task_menu.py  v2.0 — Complete Task Board + Growth Engine UI
"""

import time
from telethon import events, Button, errors
from config import bot
from admin import is_admin
import task_board as TB

# ─── Helpers ─────────────────────────────────────────────────────────────────
def _adm(e):
    if not is_admin(e.sender_id): raise PermissionError
def _ts(ts):   return time.strftime("%d/%m/%y %H:%M", time.localtime(ts)) if ts else "—"
def _rem(ts):
    r = ts - time.time()
    if r < 0:      return "⌛ Expired"
    if r < 3600:   return f"⏱ {int(r/60)}m"
    if r < 86400:  return f"⏰ {int(r/3600)}h"
    return f"📅 {int(r/86400)}d"
def _bar(v, mx, w=10):
    f = round(v / max(1, mx) * w)
    return "█" * f + "░" * (w - f)
def _safe_edit(event, text, buttons=None):
    try:
        if hasattr(event, "edit"):
            return event.edit(text, buttons=buttons)
        return event.respond(text, buttons=buttons)
    except errors.MessageNotModifiedError:
        return event.answer()  # returns coroutine — callers must await _safe_edit

# ─────────────────────────────────────────────────────────────────────────────
# USER — MAIN TASK BOARD
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.NewMessage(pattern='/tasks'))
async def tasks_cmd(event):
    if not event.is_private: return
    await _show_board(event.sender_id, event)

@bot.on(events.CallbackQuery(data=b"task_board"))
async def task_board_cb(event):
    await event.answer()
    await _show_board(event.sender_id, event)

# ─────────────────────────────────────────────────────────────────────────────
# EARN HUB — unified entry point replacing separate "Refer & Earn",
#             "Tasks & Earn", "Share & Earn" buttons
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"earn_hub"))
async def earn_hub(event):
    await event.answer()
    uid  = event.sender_id
    cfg  = TB._cfg()
    sym  = cfg.get("coin_symbol", "\U0001f9f8")
    name = cfg.get("coin_name", "Coins")
    prof  = TB.get_user_profile(uid)
    coins = prof.get("coins", 0)
    done  = prof.get("total_done", 0)
    lvl, _, _ = TB.get_user_level(uid)
    try:
        from database import get_user_data
        d    = get_user_data(uid)
        refs = len(d.get("refer", {}).get("referred_users", []))
    except Exception:
        refs = 0
    tasks_avail = len(TB.list_tasks(True))
    rate = cfg.get("coins_to_premium", 500)

    lines = [
        "\U0001f381 **EARN & REWARDS**",
        "\u2501" * 30,
        "",
        f"{lvl}  {sym} **{coins}** {name}  \u00b7  \u2705 {done} tasks done",
        "",
        f"**\U0001f3af Tasks** \u2014 Chhoti activities karo, coins kamao",
        f"  {tasks_avail} tasks available abhi",
        f"  {rate} {sym} = 1 din Premium",
        "",
        f"**\U0001f465 Refer & Share** \u2014 Dost laao, reward kamao",
        f"  Tumhare referrals: **{refs}**",
        "  Refer karo \u2192 coins + premium days dono milte hain",
        "",
        f"**\U0001f4b0 Redeem** \u2014 Coins ko Premium mein convert karo",
    ]
    text = "\n".join(lines)

    btns = [
        [Button.inline(f"\U0001f3af Tasks ({tasks_avail} available)", b"task_board")],
        [Button.inline("\U0001f465 Refer & Share \u2014 Dost Laao",   b"earn_refer_share")],
        [Button.inline("\U0001f4b0 Redeem Coins \u2192 Premium",       b"task_redeem")],
        [Button.inline("\U0001f3c6 Leaderboard",                       b"task_lb")],
        [Button.inline("\U0001f3e0 Main Menu",                         b"main_menu")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except Exception:
        await event.respond(text, buttons=btns)


@bot.on(events.CallbackQuery(data=b"earn_refer_share"))
async def earn_refer_share(event):
    """Merged Refer + Share page — dono ek jagah."""
    await event.answer()
    uid = event.sender_id
    cfg = TB._cfg()
    sym = cfg.get("coin_symbol", "\U0001f9f8")
    ge  = cfg.get("growth", {})
    ref_bonus = ge.get("referral_bonus", 75)
    try:
        from refer import get_refer_settings, get_bot_link
        from database import get_user_data
        ref_cfg     = get_refer_settings()
        d           = get_user_data(uid)
        total_refs  = len(d.get("refer", {}).get("referred_users", []))
        needed      = ref_cfg.get("referrals_needed", 3)
        reward_days = ref_cfg.get("reward_days", 7)
        bot_username = get_bot_link()
        ref_link    = f"https://t.me/{bot_username}?start=ref_{uid}"
        refer_on    = ref_cfg.get("enabled", True)
    except Exception:
        total_refs = 0; needed = 3; reward_days = 7
        ref_link   = "(link unavailable)"; refer_on = False
    try:
        kit = TB.get_share_kit(uid)
    except Exception:
        kit = {"ref_link": ref_link, "full": "", "short": "", "whatsapp": ""}

    next_reward = needed - (total_refs % needed) if refer_on and total_refs % needed != 0 else needed
    lines = [
        "\U0001f465 **REFER & SHARE**",
        "\u2501" * 30,
        "",
        "**Ek link \u2014 Do faayde:**",
        "",
        f"\U0001f9f8 **Coins Bonus** \u2014 Har referral pe `+{ref_bonus}{sym}`",
    ]
    if refer_on:
        lines += [
            f"\U0001f48e **Premium Reward** \u2014 Har `{needed}` referrals pe `{reward_days}` din free",
        ]
    lines += [
        "",
        f"**Tera Referral Link:**",
        f"`{ref_link}`",
        "",
        f"\U0001f4ca Total Referrals: **{total_refs}**"
        + (f"  \u00b7  Agle reward ke liye: **{next_reward} aur**" if refer_on else ""),
        "",
        "_Link share karo \U0001f447_",
    ]
    text = "\n".join(lines)

    btns = [
        [Button.inline("\U0001f4cb Full Message Copy",    b"share_full"),
         Button.inline("\U0001f4dd Short Message",        b"share_short")],
        [Button.inline("\U0001f4ac WhatsApp Message",     b"share_wa")],
        [Button.inline("\U0001f4ca Meri Referral Stats",  b"ref_stats")],
        [Button.inline("\U0001f519 Earn Hub",             b"earn_hub")],
    ]
    try:
        await event.edit(text, buttons=btns)
    except Exception:
        await event.respond(text, buttons=btns)


async def _show_board(uid: int, event, sort="priority", platform=None, category=None):
    cfg   = TB._cfg()
    if not cfg.get("enabled"):
        txt = "🎯 Task Board unavailable."
        return await _safe_edit(event, txt, [[Button.inline("🏠 Menu", b"main_menu")]])

    tasks  = TB.list_tasks(True, category_id=category, platform=platform, sort=sort)
    prof   = TB.get_user_profile(uid)
    coins  = prof.get("coins", 0)
    done_t = prof.get("total_done", 0)
    streak = prof.get("streak", 0)
    sym    = cfg.get("coin_symbol","🪙")
    name   = cfg.get("coin_name","Coins")
    rate   = cfg.get("coins_to_premium", 500)
    lvl, lm, nxt = TB.get_user_level(uid)
    done_today = len(TB._user_done_today(uid))
    max_d  = cfg.get("max_daily_tasks", 30)

    # Bonus event banner
    bonus_line = ""
    if cfg.get("bonus_until", 0) > time.time():
        bonus_line = f"\n⚡ **{cfg.get('bonus_multiplier',2)}x BONUS EVENT ACTIVE!**"

    header = (
        f"🎯 **Task Board**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{lvl}  {sym} **{coins}** {name}"
        + (f"  🔥 {streak}d streak" if streak > 1 else "") + "\n"
        f"Aaj: `{done_today}/{max_d}`  Total: `{done_t}`"
        + (f"  Next level: `{nxt} lifetime`" if nxt else "") + "\n"
        f"1 day Premium = {rate} {sym}"
        + bonus_line + "\n\n"
    )

    if not tasks:
        header += "⏳ Koi task nahi. Baad mein aao!"
        btns = [
            [Button.inline("💰 Redeem",    b"task_redeem"),
             Button.inline("🏆 Leaderboard", b"task_lb")],
            [Button.inline("📤 Share&Earn", b"task_share"),
             Button.inline("🏠 Menu",      b"main_menu")],
        ]
    else:
        # Platform filter buttons
        header += f"**{len(tasks)} tasks available** _(optional — aapki marzi)_"
        plat_btns = []
        for p, plbl in list(TB.PLATFORMS.items())[:5]:
            active_mark = "✅ " if platform == p else ""
            plat_btns.append(Button.inline(f"{active_mark}{plbl[:10]}", f"tb_filter|{p}".encode()))
        plat_btns.append(Button.inline("🔄 All", b"task_board"))

        task_btns = []
        for t in tasks[:9]:
            done  = uid in t.get("completed_by", {})
            mk    = "✅ " if done else ""
            rew   = t.get("reward_coins", 0)
            rem   = _rem(t.get("expires_at", 0))
            p_ico = TB.PLATFORMS.get(t.get("platform",""), "")[:4]
            task_btns.append([Button.inline(
                f"{mk}{t.get('icon','')} {t['title'][:22]} {sym}{rew} {rem}",
                f"task_view|{t['id']}".encode()
            )])
        btns = [plat_btns] + task_btns + [
            [Button.inline("💰 Redeem",     b"task_redeem"),
             Button.inline("🏆 Top",        b"task_lb"),
             Button.inline("📤 Share",      b"task_share")],
            [Button.inline("🔃 Sort: Reward", b"tb_sort|reward"),
             Button.inline("⏰ Ending Soon",  b"tb_sort|ending")],
            [Button.inline("🏠 Menu",         b"main_menu")],
        ]

    await _safe_edit(event, header, btns)


@bot.on(events.CallbackQuery(pattern=b"tb_filter\\|(.+)"))
async def tb_filter(event):
    await event.answer()
    plat = event.data.decode().split("|")[1]
    await _show_board(event.sender_id, event, platform=plat)

@bot.on(events.CallbackQuery(pattern=b"tb_sort\\|(.+)"))
async def tb_sort(event):
    await event.answer()
    sort = event.data.decode().split("|")[1]
    await _show_board(event.sender_id, event, sort=sort)

# ─────────────────────────────────────────────────────────────────────────────
# TASK DETAIL + COMPLETE
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(pattern=b"task_view\\|(.+)"))
async def task_view(event):
    await event.answer()
    uid = event.sender_id
    tid = event.data.decode().split("|")[1]
    t   = TB.get_task(tid)
    if not t: return await event.answer("Task nahi mila!", alert=True)

    t["views"] = t.get("views", 0) + 1

    cfg  = TB._cfg()
    sym  = cfg.get("coin_symbol","🪙")
    done = uid in t.get("completed_by", {})
    stats = TB.get_task_stats(tid)

    # Bonus calculation preview
    prof  = TB.get_user_profile(uid)
    _, lm, _ = TB.get_user_level(uid)
    sm    = TB.get_streak_multiplier(uid)
    gm    = cfg.get("bonus_multiplier", 1.0) if cfg.get("bonus_until",0) > time.time() else 1.0
    total_m = round(gm * lm * sm, 2)
    base  = t.get("reward_coins", 0)
    final = max(1, round(base * total_m))

    mult_parts = []
    if gm > 1:   mult_parts.append(f"⚡ Event ×{gm}")
    if lm > 1:   mult_parts.append(f"🏆 Level ×{lm}")
    if sm > 1:   mult_parts.append(f"🔥 Streak ×{round(sm,2)}")
    mult_line = "  ".join(mult_parts) if mult_parts else ""

    bonus_first = ""
    if t.get("bonus_coins",0) > 0 and t.get("completions",0) < t.get("bonus_slots",0):
        slots_left = t["bonus_slots"] - t["completions"]
        bonus_first = f"\n🎁 First {t['bonus_slots']} bonus: +{t['bonus_coins']} {sym} ({slots_left} left!)"

    text = (
        f"{t.get('icon','')} **{t['title']}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{TB.PLATFORMS.get(t.get('platform',''),'')}"
        + (f"  ·  {TB.TASK_TYPES.get(t.get('type',''),('','',''))[1]}" if t.get('type') else "") + "\n"
        f"📝 {t.get('description','')}\n\n"
        f"💰 Reward: **{sym}{final}**"
        + (f" (base {base} × {total_m})" if total_m != 1 else "") + "\n"
        + (mult_line + "\n" if mult_line else "")
        + bonus_first + "\n"
        f"👥 {stats.get('completions',0)} completed"
        + (f"/{t.get('max_completions')}" if t.get('max_completions') else "") + "\n"
        f"👁 {stats.get('views',0)} views  CVR: {stats.get('cvr',0)}%\n"
        f"{_rem(t.get('expires_at',0))}"
        + (f"\n📣 Sponsor: {t['sponsor_name']}" if t.get("sponsor_name") else "")
        + ("\n\n✅ **Tumne ye task kar liya!**" if done else "")
    )

    btns = []
    if not done:
        can, reason = TB.can_do_task(uid, tid)
        if t.get("link"):
            btns.append([Button.url(
                f"{t.get('icon','')} Task karo ↗",
                t["link"]
            )])
        if t.get("type") == "join_channel" and t.get("link","").startswith("@"):
            btns.append([Button.inline(
                f"✅ Join verify + {sym}{final} lo",
                f"task_verify_join|{tid}".encode()
            )])
        elif t.get("proof_required"):
            btns.append([Button.inline(
                f"📸 Proof bhejo → {sym}{final}",
                f"task_proof|{tid}".encode()
            )])
        else:
            lbl = f"✅ Done! → {sym}{final}" if can else f"⏳ {reason[:25]}"
            btns.append([Button.inline(lbl, f"task_done|{tid}".encode())])
    btns.append([Button.inline("◀️ Back", b"task_board")])
    await _safe_edit(event, text, btns)


@bot.on(events.CallbackQuery(pattern=b"task_done\\|(.+)"))
async def task_done_cb(event):
    await event.answer()
    uid    = event.sender_id
    tid    = event.data.decode().split("|")[1]
    result = TB.complete_task(uid, tid)
    if result["ok"]:
        await event.answer(result["msg"][:200], alert=False)
        await task_view(event)
    else:
        await event.answer(result["msg"], alert=True)


@bot.on(events.CallbackQuery(pattern=b"task_verify_join\\|(.+)"))
async def task_verify_join(event):
    await event.answer()
    uid = event.sender_id
    tid = event.data.decode().split("|")[1]
    t   = TB.get_task(tid)
    if not t: return await event.answer("Task nahi mila!", alert=True)

    await event.answer("Verifying...", alert=False)
    from database import user_sessions, GLOBAL_STATE
    client = user_sessions.get(uid)
    if not client:
        await event.answer("Pehle login karo!", alert=True)
        return

    channel = t.get("link","").strip()
    joined  = await TB.verify_telegram_join(uid, channel, client)
    if joined:
        result = TB.complete_task(uid, tid)
        await event.answer(result.get("msg","✅ Verified!")[:200], alert=False)
        await task_view(event)
    else:
        await event.answer(f"❌ {channel} join nahi kiya! Join karke dobara try karo.", alert=True)


@bot.on(events.CallbackQuery(pattern=b"task_proof\\|(.+)"))
async def task_proof_cb(event):
    await event.answer()
    uid = event.sender_id
    tid = event.data.decode().split("|")[1]
    t   = TB.get_task(tid)
    from database import get_user_data
    d = get_user_data(uid)
    d["step"] = "task_proof"; d["task_proof_tid"] = tid
    d["step_since"] = time.time()
    hint = t.get("proof_hint","Screenshot ya proof bhejo") if t else "Proof bhejo"
    await _safe_edit(event,
        f"📸 **Proof Submit Karo**\n\n{hint}\n\n_Admin verify karega._",
        [[Button.inline("❌ Cancel", b"task_board")]]
    )


@bot.on(events.NewMessage())
async def task_proof_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    from database import get_user_data, save_persistent_db, GLOBAL_STATE
    d = get_user_data(uid)
    if d.get("step") != "task_proof": return
    tid  = d.get("task_proof_tid","")
    task = TB.get_task(tid)
    if not task: d["step"] = None; save_persistent_db(); return await event.respond("❌ Task nahi mila!")
    GLOBAL_STATE.setdefault("task_proofs",[]).append({
        "uid": uid, "tid": tid, "task": task.get("title",""),
        "reward": task.get("reward_coins",0),
        "proof":  event.raw_text or "(media)", "ts": time.time(), "status": "pending"
    })
    TB._save(); d["step"] = None; save_persistent_db()
    sym = TB._cfg().get("coin_symbol","🪙")
    try:
        admins = list(GLOBAL_STATE.get("admins",{}).keys())
        for adm in admins[:1]:
            await bot.send_message(adm,
                f"📸 **New Task Proof!**\n👤 `{uid}`\n🎯 {task.get('title')}\n💰 {task.get('reward_coins')}{sym}\n\nAdmin → Task Board → Proofs",
                parse_mode="md"
            )
    except Exception: pass
    await event.respond("✅ Proof submit ho gaya! Admin verify karega.",
        buttons=[[Button.inline("🎯 Task Board", b"task_board")]])

# ─────────────────────────────────────────────────────────────────────────────
# REDEEM
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"task_redeem"))
async def task_redeem(event):
    await event.answer()
    uid   = event.sender_id
    cfg   = TB._cfg()
    coins = TB.get_user_coins(uid)
    sym   = cfg.get("coin_symbol","🪙")
    rate  = cfg.get("coins_to_premium",500)
    wd    = cfg["withdrawal"]
    lvl, _, _ = TB.get_user_level(uid)

    text = (
        f"💰 **Redeem {sym}**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{lvl}  Balance: **{coins} {sym}**\n"
        f"💎 {rate} {sym} = 1 day Premium\n\n"
        "**Premium Packages:**"
    )
    btns = []
    for d, lbl in [(1,"1 Day"),(7,"7 Days"),(30,"30 Days")]:
        cost = rate * d
        if coins >= cost:
            btns.append([Button.inline(f"💎 {lbl} Premium — {cost}{sym}", f"redeem_prem|{d}".encode())])

    if not any(coins >= rate*d for d in [1,7,30]):
        text += f"\n\n❌ {rate}{sym} chahiye 1 day ke liye. Aur tasks karo!"

    if wd.get("enabled") and wd.get("methods"):
        text += "\n\n**Withdraw karo:**"
        btns.append([Button.inline(f"💸 Withdraw ({wd.get('min_coins',1000)}{sym} min)", b"task_withdraw")])

    btns.append([Button.inline("◀️ Back", b"task_board")])
    await _safe_edit(event, text, btns)


@bot.on(events.CallbackQuery(pattern=b"redeem_prem\\|(.+)"))
async def redeem_prem(event):
    await event.answer()
    uid    = event.sender_id
    days   = int(event.data.decode().split("|")[1])
    result = await TB.redeem_for_premium(uid, days)
    await event.answer(result["msg"], alert=True)
    if result["ok"]:
        await task_redeem(event)


@bot.on(events.CallbackQuery(data=b"task_withdraw"))
async def task_withdraw(event):
    await event.answer()
    uid = event.sender_id
    wd  = TB._cfg()["withdrawal"]
    if not wd.get("methods"):
        return await event.answer("Withdrawal methods nahi hain!", alert=True)
    btns = [[Button.inline(m.get("label","Method"), f"wd_method|{m.get('type','')}".encode())]
            for m in wd["methods"][:5]]
    btns.append([Button.inline("◀️ Back", b"task_redeem")])
    coins = TB.get_user_coins(uid)
    sym   = TB._cfg().get("coin_symbol","🪙")
    await _safe_edit(event,
        f"💸 **Withdraw**\n\nBalance: {coins}{sym}\nMin: {wd.get('min_coins',1000)}{sym}\n\nMethod chuniye:",
        btns
    )


@bot.on(events.CallbackQuery(pattern=b"wd_method\\|(.+)"))
async def wd_method(event):
    await event.answer()
    uid    = event.sender_id
    method = event.data.decode().split("|")[1]
    from database import get_user_data
    d = get_user_data(uid)
    d["step"] = "wd_amount"; d["wd_method"] = method
    d["step_since"] = time.time()
    await _safe_edit(event,
        "💸 **Kitne coins withdraw?**\n\nAmount type karo:",
        [[Button.inline("❌ Cancel", b"task_redeem")]]
    )


@bot.on(events.NewMessage())
async def wd_amount_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    from database import get_user_data
    d = get_user_data(uid)
    if d.get("step") != "wd_amount": return
    try:
        coins  = int(event.raw_text.strip())
        if coins <= 0:
            return await event.respond("❌ Amount 0 se zyada hona chahiye!")
        method = d.get("wd_method","")
        d["step"] = "wd_details"; d["wd_coins"] = coins
        d["step_since"] = time.time()
        await event.respond(f"📋 **{method} details daalo:**\n(UPI ID / Account / etc.):")
    except ValueError:
        await event.respond("❌ Number daalo!")


@bot.on(events.NewMessage())
async def wd_details_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "wd_details": return
    result = TB.request_withdrawal(uid, d.get("wd_coins",0), d.get("wd_method",""), event.raw_text.strip())
    d["step"] = None; save_persistent_db()
    await event.respond(result["msg"],
        buttons=[[Button.inline("💰 Redeem Menu", b"task_redeem")]])
    if result["ok"]:
        try:
            from database import GLOBAL_STATE
            for adm in list(GLOBAL_STATE.get("admins",{}).keys())[:1]:
                await bot.send_message(adm,
                    f"💸 **Withdrawal Request!**\n👤 `{uid}`\n💰 {d.get('wd_coins')} coins\n📋 {d.get('wd_method')}: {event.raw_text[:50]}")
        except Exception: pass

# ─────────────────────────────────────────────────────────────────────────────
# LEADERBOARD
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"task_lb"))
async def task_lb(event):
    await event.answer()
    uid   = event.sender_id
    sym   = TB._cfg().get("coin_symbol","🪙")
    board = TB.get_leaderboard(10)
    medals = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4,11)]
    my_rank = None
    lines = ["🏆 **Leaderboard (Lifetime Coins)**\n"]
    for i, e in enumerate(board):
        me   = " ← You" if e["uid"] == uid else ""
        anon = f"User...{str(e['uid'])[-4:]}"
        lines.append(f"{medals[i]} {anon} · {e['level']} · **{e['lifetime']}{sym}**{me}")
        if e["uid"] == uid: my_rank = i+1

    my_c = TB.get_user_coins(uid)
    my_l, _, _ = TB.get_user_level(uid)
    if my_rank:
        lines.append(f"\n🎯 Tera rank: **#{my_rank}**")
    else:
        lines.append(f"\n😔 Top 10 mein nahi ho. {my_c}{sym} | {my_l}")

    btns = [
        [Button.inline("🔃 By Today", b"task_lb_today"),
         Button.inline("🔥 Streaks",  b"task_lb_streak")],
        [Button.inline("◀️ Back",      b"task_board")],
    ]
    await _safe_edit(event, "\n".join(lines), btns)


@bot.on(events.CallbackQuery(data=b"task_lb_today"))
async def task_lb_today(event):
    await event.answer()
    uid    = event.sender_id
    today  = TB._today_key()
    done   = TB._daily_done.get(today, {})
    sorted_u = sorted(done.items(), key=lambda x: -len(x[1]))[:10]
    sym = TB._cfg().get("coin_symbol","🪙")
    medals = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4,11)]
    lines = [f"📅 **Today's Active Users**\n"]
    for i, (u, tids) in enumerate(sorted_u):
        me = " ← You" if u == uid else ""
        lines.append(f"{medals[i]} User...{str(u)[-4:]} · {len(tids)} tasks{me}")
    if not sorted_u:
        lines.append("Aaj koi data nahi!")
    await _safe_edit(event, "\n".join(lines),
        [[Button.inline("🏆 All-time", b"task_lb"), Button.inline("◀️", b"task_board")]])


@bot.on(events.CallbackQuery(data=b"task_lb_streak"))
async def task_lb_streak(event):
    await event.answer()
    from database import db
    uid    = event.sender_id
    scores = [(u, d.get("task_profile",{}).get("streak",0))
              for u, d in db.items() if d.get("task_profile",{}).get("streak",0) > 0]
    scores.sort(key=lambda x: -x[1])
    medals = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4,11)]
    lines = ["🔥 **Streak Leaderboard**\n"]
    for i, (u, s) in enumerate(scores[:10]):
        me = " ← You" if u == uid else ""
        lines.append(f"{medals[i]} User...{str(u)[-4:]} · **{s}d streak**{me}")
    await _safe_edit(event, "\n".join(lines),
        [[Button.inline("🏆 All-time", b"task_lb"), Button.inline("◀️", b"task_board")]])

# ─────────────────────────────────────────────────────────────────────────────
# SHARE & EARN
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"task_share"))
async def task_share(event):
    await event.answer()
    uid  = event.sender_id
    kit  = TB.get_share_kit(uid)
    cfg  = TB._cfg()
    sym  = cfg.get("coin_symbol","🪙")
    ge   = cfg["growth"]
    bonus= ge.get("referral_bonus", 75)
    from database import get_user_data
    refs = len(get_user_data(uid).get("refer",{}).get("referred_users",[]))
    coins= TB.get_user_coins(uid)
    text = (
        f"📤 **Share & Earn**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Har referral pe **+{bonus}{sym}** bonus!\n"
        f"Tumhare referrals: `{refs}`  Balance: `{coins}{sym}`\n\n"
        f"**Tera referral link:**\n`{kit['ref_link']}`\n\n"
        "Message copy karo aur share karo 👇"
    )
    await _safe_edit(event, text, [
        [Button.inline("📋 Full Message", b"share_full"),
         Button.inline("📝 Short",       b"share_short")],
        [Button.inline("💬 WhatsApp",    b"share_wa")],
        [Button.inline("◀️ Back",         b"task_board")],
    ])

@bot.on(events.CallbackQuery(data=b"share_full"))
async def share_full(event):
    await event.answer()
    kit = TB.get_share_kit(event.sender_id)
    await event.respond(f"📋 Copy karo:\n\n{kit['full']}",
        buttons=[[Button.inline("◀️ Back", b"task_share")]])

@bot.on(events.CallbackQuery(data=b"share_short"))
async def share_short(event):
    await event.answer()
    kit = TB.get_share_kit(event.sender_id)
    await event.respond(f"📝 Short:\n\n{kit['short']}",
        buttons=[[Button.inline("◀️ Back", b"task_share")]])

@bot.on(events.CallbackQuery(data=b"share_wa"))
async def share_wa(event):
    await event.answer()
    kit = TB.get_share_kit(event.sender_id)
    await event.respond(f"💬 WhatsApp:\n\n{kit['whatsapp']}",
        buttons=[[Button.inline("◀️ Back", b"task_share")]])

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — TASK BOARD DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_task_board"))
async def adm_task_board(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    s   = TB.get_board_stats()
    sym = s["coin_symbol"]
    bonus_line = f"\n⚡ **Bonus Event: {s['bonus_mult']}x ACTIVE!**" if s["bonus_on"] else ""
    text = (
        "🎯 **Task Board Dashboard**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"System: **{'🟢 ON' if s['enabled'] else '🔴 OFF'}**"
        f"{bonus_line}\n\n"
        f"**Tasks:**\n"
        f"  🎯 Active: `{s['active_tasks']}`  Total: `{s['total_tasks']}`\n"
        f"  ✅ Total Completed: `{s['total_done']}`\n"
        f"  👥 Active Users: `{s['users_active']}`\n"
        f"  📈 Top Task: `{s['top_task']}` ({s['top_completions']} done)\n\n"
        f"**Economy:**\n"
        f"  {sym} Total Distributed: `{s['total_coins']}`\n"
        f"  💎 Premium Rate: `{s['coins_to_prem']}` per day\n"
        f"  📋 Daily Limit: `{s['max_daily']}`\n\n"
        f"**Growth Engine:** {'🟢' if s['growth_enabled'] else '🔴'}\n"
        f"  📣 Channels: `{s['promo_channels']}`\n"
        f"**Withdrawal:** {'🟢' if s['withdrawal_on'] else '🔴'}"
    )
    await _safe_edit(event, text, [
        [Button.inline("➕ New Task",       b"adm_task_create"),
         Button.inline("📋 Tasks",         b"adm_task_list")],
        [Button.inline("📸 Proofs",        b"adm_task_proofs"),
         Button.inline("🏆 Leaderboard",  b"adm_task_lb")],
        [Button.inline("🚀 Growth",        b"adm_growth"),
         Button.inline("💸 Withdrawals",  b"adm_wd_panel")],
        [Button.inline("⚡ Bonus Event",   b"adm_bonus_event"),
         Button.inline("⚙️ Settings",     b"adm_task_cfg")],
        [Button.inline("🔙 Admin",          b"adm_main")],
    ])

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — TASK CREATE WIZARD
# ─────────────────────────────────────────────────────────────────────────────

_TWIZ = [
    ("type",     None),
    ("platform", None),
    ("title",    "📝 **Task Title:**\ne.g., `Like our Instagram post`"),
    ("desc",     "✍️ **Description (short):**\nKya karna hai:"),
    ("link",     "🔗 **Direct Link:**\nPost/video/profile ka direct URL:"),
    ("coins",    "💰 **Reward Coins:**\ne.g., `15`, `25`, `50`"),
    ("sponsor",  "👤 **Sponsor Name:** _(optional)_\n`/skip` = anonymous"),
    ("hours",    "⏰ **Expires in (hours):**\ne.g., `24`, `72`\n`/skip` = 72h"),
    ("max_comp", "👥 **Max completions:**\n`/skip` = unlimited"),
    ("bonus",    "🎁 **First-N Bonus?** _(optional)_\nFormat: `coins slots` e.g. `5 10`\n`/skip` = no bonus"),
    ("priority", "⬆️ **Priority (0-100):**\nHigher = shown first\n`/skip` = 0"),
    ("proof",    None),
]


@bot.on(events.CallbackQuery(data=b"adm_task_create"))
async def adm_task_create(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "twiz"; d["twiz"] = {"idx": 0, "data": {}}
    d["step_since"] = time.time()
    btns = [[Button.inline(f"{ico} {lbl[:18]}", f"twiz_t|{k}".encode())]
            for k, (ico, lbl, _) in TB.TASK_TYPES.items()]
    await _safe_edit(event, "🎯 **New Task — Type:**", btns)


@bot.on(events.CallbackQuery(pattern=b"twiz_t\\|(.+)"))
async def twiz_type(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("twiz",{}); wiz["data"]["type"] = event.data.decode().split("|")[1]
    wiz["idx"] = 1
    btns = [[Button.inline(lbl[:22], f"twiz_p|{k}".encode())]
            for k, lbl in TB.PLATFORMS.items()]
    await _safe_edit(event, "📱 **Platform:**", btns)
    d["twiz"] = wiz


@bot.on(events.CallbackQuery(pattern=b"twiz_p\\|(.+)"))
async def twiz_plat(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data
    d = get_user_data(event.sender_id)
    wiz = d.get("twiz",{}); wiz["data"]["platform"] = event.data.decode().split("|")[1]
    wiz["idx"] = 2
    await _safe_edit(event, f"**Step 3/{len(_TWIZ)}**\n\n{_TWIZ[2][1]}",
                     [[Button.inline("❌ Cancel", b"adm_task_board")]])
    d["twiz"] = wiz


@bot.on(events.CallbackQuery(pattern=b"twiz_proof\\|(.+)"))
async def twiz_proof(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    from database import get_user_data, save_persistent_db, GLOBAL_STATE
    d   = get_user_data(event.sender_id)
    wiz = d.get("twiz",{}); data = wiz.get("data",{})
    req = event.data.decode().split("|")[1] == "yes"

    bonus_raw = data.get("bonus","")
    bonus_c = bonus_s = 0
    if bonus_raw:
        try:
            parts  = bonus_raw.split()
            bonus_c = int(parts[0]); bonus_s = int(parts[1])
        except Exception: pass

    tid = TB.create_task(
        task_type       = data.get("type","custom"),
        platform        = data.get("platform","other"),
        title           = data.get("title","Task"),
        description     = data.get("desc",""),
        link            = data.get("link",""),
        reward_coins    = int(data.get("coins",10)),
        sponsor_name    = data.get("sponsor",""),
        expires_hours   = int(data.get("hours",72)),
        max_completions = int(data.get("max_comp",0)),
        proof_required  = req,
        bonus_coins     = bonus_c,
        bonus_slots     = bonus_s,
        priority        = int(data.get("priority",0)),
    )
    d["step"] = None; d.pop("twiz",None); save_persistent_db()
    t   = TB.get_task(tid)
    sym = TB._cfg().get("coin_symbol","🪙")
    await _safe_edit(event,
        f"✅ **Task Created!**\n`{tid}`\n\n"
        f"{t.get('icon','')} **{t.get('title','')}**\n"
        f"Platform: {TB.PLATFORMS.get(t.get('platform',''),'')}\n"
        f"Reward: {sym}{t.get('reward_coins',0)}\n"
        f"Expires: {_ts(t.get('expires_at',0))}",
        [[Button.inline("➕ Another", b"adm_task_create"),
          Button.inline("📋 All Tasks", b"adm_task_list")],
         [Button.inline("🎯 Dashboard", b"adm_task_board")]]
    )


@bot.on(events.NewMessage())
async def twiz_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data, save_persistent_db
    d = get_user_data(uid)
    if d.get("step") != "twiz": return
    wiz = d.get("twiz",{}); idx = wiz.get("idx",0)
    txt = event.raw_text.strip()
    if txt.lower() == "/cancel":
        d["step"] = None; d.pop("twiz",None); save_persistent_db()
        return await event.respond("❌ Cancelled.", buttons=[[Button.inline("🎯 Board", b"adm_task_board")]])
    key = _TWIZ[idx][0]
    skip = txt.lower() == "/skip"
    if key in ("coins","hours","max_comp","priority") and not skip:
        try: wiz["data"][key] = int(txt)
        except: return await event.respond("❌ Number daalo!")
    elif not skip:
        wiz["data"][key] = txt
    nxt = idx + 1
    wiz["idx"] = nxt
    if nxt < len(_TWIZ):
        nkey, ntxt = _TWIZ[nxt]
        if nkey == "proof":
            await event.respond("📸 **Proof required?**",
                buttons=[[Button.inline("✅ Haan", b"twiz_proof|yes"),
                          Button.inline("❌ Nahi",  b"twiz_proof|no")]])
        elif ntxt:
            await event.respond(f"**Step {nxt+1}/{len(_TWIZ)}**\n\n{ntxt}")
    d["twiz"] = wiz

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — TASK LIST + DETAIL
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_task_list"))
async def adm_task_list(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    tasks = TB.list_tasks(False, sort="newest")
    sym   = TB._cfg().get("coin_symbol","🪙")
    if not tasks:
        return await _safe_edit(event, "📋 Koi task nahi!",
            [[Button.inline("➕ Create", b"adm_task_create"),
              Button.inline("◀️", b"adm_task_board")]])
    btns = []
    for t in tasks[:12]:
        s  = TB.get_task_stats(t["id"])
        ok = "🟢" if s.get("active") else "🔴"
        btns.append([Button.inline(
            f"{ok} {t.get('icon','')} {t['title'][:20]} · {sym}{t.get('reward_coins',0)} · {s.get('completions',0)}✅",
            f"adm_td|{t['id']}".encode()
        )])
    btns += [[Button.inline("➕ New", b"adm_task_create"),
              Button.inline("◀️ Back", b"adm_task_board")]]
    await _safe_edit(event, f"📋 **All Tasks ({len(tasks)}):**", btns)


@bot.on(events.CallbackQuery(pattern=b"adm_td\\|(.+)"))
async def adm_td(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    tid  = event.data.decode().split("|")[1]
    t    = TB.get_task(tid)
    if not t: return await event.answer("Nahi mila!", alert=True)
    s    = TB.get_task_stats(tid)
    sym  = TB._cfg().get("coin_symbol","🪙")
    text = (
        f"🎯 **Task Detail**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 `{tid}`\n"
        f"{t.get('icon','')} **{t['title']}**\n"
        f"{TB.PLATFORMS.get(t.get('platform',''),'')}\n"
        f"📝 {t.get('description','')}\n"
        f"🔗 `{t.get('link','—')[:60]}`\n\n"
        f"💰 Reward: {sym}{t.get('reward_coins',0)}\n"
        f"⬆️ Priority: {t.get('priority',0)}\n"
        f"📸 Proof: {'Required' if t.get('proof_required') else 'Not required'}\n"
        f"🎁 Bonus: {t.get('bonus_coins',0)}{sym} for first {t.get('bonus_slots',0)}\n\n"
        f"**Stats:**\n"
        f"  👁 Views: {s.get('views',0)}  ✅ Done: {s.get('completions',0)}\n"
        f"  📈 CVR: {s.get('cvr',0)}%  💸 Paid: {s.get('coins_paid',0)}{sym}\n"
        f"  ⌛ {_rem(t.get('expires_at',0))}\n"
        f"  👤 Sponsor: {t.get('sponsor_name','—')}\n"
        f"Status: {'🟢 Active' if s.get('active') else '🔴 Inactive'}"
    )
    tog = "⏸ Pause" if t.get("active") else "▶️ Activate"
    await _safe_edit(event, text, [
        [Button.inline(tog, f"adm_tt|{tid}".encode()),
         Button.inline("🗑 Delete", f"adm_tdel|{tid}".encode())],
        [Button.inline("◀️ Back", b"adm_task_list")],
    ])

@bot.on(events.CallbackQuery(pattern=b"adm_tt\\|(.+)"))
async def adm_tt(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    TB.toggle_task(event.data.decode().split("|")[1])
    await adm_td(event)

@bot.on(events.CallbackQuery(pattern=b"adm_tdel\\|(.+)"))
async def adm_tdel(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    TB.delete_task(event.data.decode().split("|")[1])
    await event.answer("🗑 Deleted!", alert=False)
    await adm_task_list(event)

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — PROOFS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_task_proofs"))
async def adm_task_proofs(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import GLOBAL_STATE
    pend = [p for p in GLOBAL_STATE.get("task_proofs",[]) if p.get("status")=="pending"]
    if not pend:
        return await _safe_edit(event, "📸 Koi pending proof nahi!",
            [[Button.inline("◀️ Back", b"adm_task_board")]])
    btns = []
    for p in pend[:8]:
        ts = time.strftime("%d/%m", time.localtime(p.get("ts",0)))
        btns.append([Button.inline(
            f"👤{p['uid']} · {p.get('task','')[:16]} · {ts}",
            f"proof_rv|{p['uid']}|{p.get('tid','')}".encode()
        )])
    btns.append([Button.inline("◀️ Back", b"adm_task_board")])
    await _safe_edit(event, f"📸 **Pending Proofs ({len(pend)}):**", btns)


@bot.on(events.CallbackQuery(pattern=b"proof_rv\\|(.+)\\|(.+)"))
async def proof_rv(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    uid, tid = int(parts[1]), parts[2]
    t   = TB.get_task(tid)
    sym = TB._cfg().get("coin_symbol","🪙")
    from database import GLOBAL_STATE
    proof_entry = next((p for p in GLOBAL_STATE.get("task_proofs",[])
                        if p["uid"]==uid and p.get("tid")==tid and p.get("status")=="pending"), {})
    await _safe_edit(event,
        f"📸 **Review Proof**\n\n"
        f"👤 User: `{uid}`\n🎯 Task: `{t.get('title','') if t else tid}`\n"
        f"💰 Reward: {t.get('reward_coins',0)}{sym}\n\n"
        f"Proof: `{proof_entry.get('proof','—')[:100]}`",
        [[Button.inline("✅ Approve + Coins", f"proof_ok|{uid}|{tid}".encode()),
          Button.inline("❌ Reject",          f"proof_no|{uid}|{tid}".encode())],
         [Button.inline("◀️ Back", b"adm_task_proofs")]]
    )


@bot.on(events.CallbackQuery(pattern=b"proof_ok\\|(.+)\\|(.+)"))
async def proof_ok(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    uid, tid = int(parts[1]), parts[2]
    from database import GLOBAL_STATE
    for p in GLOBAL_STATE.get("task_proofs",[]): 
        if p["uid"]==uid and p.get("tid")==tid: p["status"]="approved"
    result = TB.complete_task(uid, tid)
    TB._save()
    try: await bot.send_message(uid, f"✅ **Proof approved!**\n{result.get('msg','')}", parse_mode="md")
    except Exception: pass
    await event.answer("✅ Approved!", alert=False)
    await adm_task_proofs(event)


@bot.on(events.CallbackQuery(pattern=b"proof_no\\|(.+)\\|(.+)"))
async def proof_no(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts = event.data.decode().split("|")
    uid, tid = int(parts[1]), parts[2]
    from database import GLOBAL_STATE
    for p in GLOBAL_STATE.get("task_proofs",[]): 
        if p["uid"]==uid and p.get("tid")==tid: p["status"]="rejected"
    TB._save()
    try: await bot.send_message(uid, "❌ Task proof reject ho gaya. Dobara try karo!", parse_mode="md")
    except Exception: pass
    await event.answer("❌ Rejected", alert=False)
    await adm_task_proofs(event)

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — LEADERBOARD
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_task_lb"))
async def adm_task_lb_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    board = TB.get_leaderboard(15)
    sym   = TB._cfg().get("coin_symbol","🪙")
    lines = ["🏆 **Admin Leaderboard (Top 15)**\n"]
    for i, e in enumerate(board, 1):
        lines.append(f"`{i:2}.` `uid:{e['uid']}` {e['level']} · **{e['lifetime']}{sym}** · {e['done']}tasks · {e['streak']}🔥")
    await _safe_edit(event, "\n".join(lines) or "No data!", [[Button.inline("◀️ Back", b"adm_task_board")]])

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — BONUS EVENT
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_bonus_event"))
async def adm_bonus_event(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = TB._cfg()
    on  = cfg.get("bonus_until",0) > time.time()
    if on:
        rem = int((cfg["bonus_until"] - time.time()) / 3600)
        status = f"⚡ **{cfg.get('bonus_multiplier',2)}x EVENT ACTIVE** — {rem}h remaining"
    else:
        status = "🔴 No event running"
    await _safe_edit(event,
        f"⚡ **Bonus Event**\n\n{status}\n\n"
        "Ek limited time event start karo — sab coins multiply honge!",
        [[Button.inline("2x — 1 Hour",   b"bonus|2|1"),
          Button.inline("2x — 24 Hours", b"bonus|2|24")],
         [Button.inline("3x — 1 Hour",   b"bonus|3|1"),
          Button.inline("3x — 6 Hours",  b"bonus|3|6")],
         [Button.inline("🔴 Stop Event", b"bonus|1|0"),
          Button.inline("◀️ Back",       b"adm_task_board")]]
    )

@bot.on(events.CallbackQuery(pattern=b"bonus\\|(.+)\\|(.+)"))
async def bonus_cb(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts  = event.data.decode().split("|")
    mult, hours = float(parts[1]), int(parts[2])
    cfg = TB._cfg()
    if hours == 0:
        cfg["bonus_until"] = 0; cfg["bonus_multiplier"] = 1.0
        await event.answer("🔴 Event stopped!", alert=False)
    else:
        cfg["bonus_multiplier"] = mult
        cfg["bonus_until"]      = time.time() + hours * 3600
        # Notify all users
        try:
            from database import db
            count = 0
            for uid in list(db.keys()):
                try:
                    await bot.send_message(uid,
                        f"⚡ **{mult}x COIN BONUS EVENT!**\n\n"
                        f"Agli {hours} ghante tak task karo — {mult}x coins!\n\n"
                        f"👉 /tasks",
                        parse_mode="md"
                    )
                    count += 1
                    await asyncio.sleep(0.05)
                    if count >= 200: break
                except Exception: pass
        except Exception: pass
        await event.answer(f"⚡ {mult}x event started for {hours}h!", alert=False)
    TB._save()
    await adm_bonus_event(event)

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — GROWTH ENGINE
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_growth"))
async def adm_growth(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ge  = TB._cfg()["growth"]
    on  = ge.get("enabled", False)
    chs = ge.get("promo_channels", [])
    last = _ts(ge.get("last_auto_post",0))
    await _safe_edit(event,
        f"🚀 **Growth Engine**\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Status: **{'🟢 ON' if on else '🔴 OFF'}**\n\n"
        f"📣 Your channels: `{len(chs)}`\n"
        f"⏰ Auto interval: `{ge.get('auto_interval',86400)//3600}h`\n"
        f"🕐 Last post: `{last}`\n\n"
        f"💰 Referral bonus: `{ge.get('referral_bonus',75)}` coins\n\n"
        f"**Share Kit:**\n`{ge.get('share_kit_text','')[:60] or 'Default'}`",
        [[Button.inline("🟢 Enable" if not on else "🔴 Disable", b"growth_tog"),
          Button.inline("📤 Post Now", b"growth_post_now")],
         [Button.inline("📣 Channels", b"growth_chs"),
          Button.inline("✏️ Share Kit", b"growth_skit")],
         [Button.inline("💰 Ref Bonus", b"growth_rb"),
          Button.inline("⏰ Interval",  b"growth_int")],
         [Button.inline("◀️ Back", b"adm_task_board")]]
    )

@bot.on(events.CallbackQuery(data=b"growth_tog"))
async def growth_tog(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ge = TB._cfg()["growth"]; ge["enabled"] = not ge.get("enabled",False); TB._save()
    await adm_growth(event)

@bot.on(events.CallbackQuery(data=b"growth_post_now"))
async def growth_post_now(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    ge  = TB._cfg()["growth"]
    chs = ge.get("promo_channels",[])
    if not chs: return await event.answer("Pehle channels add karo!", alert=True)
    txt = TB.get_promo_text()
    sent = 0
    for ch in chs:
        try:
            await bot.send_message(ch, txt, parse_mode="md")
            sent += 1; await asyncio.sleep(2)
        except Exception: pass
    ge["last_auto_post"] = time.time(); TB._save()
    await event.answer(f"✅ {sent}/{len(chs)} channels posted!", alert=True)
    await adm_growth(event)


async def _growth_input(event, step, prompt):
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    get_user_data(event.sender_id)["step"] = step
    get_user_data(event.sender_id)["step_since"] = time.time()
    await _safe_edit(event, prompt, [[Button.inline("❌ Cancel", b"adm_growth")]])

@bot.on(events.CallbackQuery(data=b"growth_chs"))
async def growth_chs(event):
    await event.answer()
    ge = TB._cfg()["growth"]
    ch_txt = "\n".join(f"• `{c}`" for c in ge.get("promo_channels",[])) or "None"
    await _growth_input(event, "growth_ch_input",
        f"📣 **Channels:**\n{ch_txt}\n\nAdd: `@channel`\nRemove: `-@channel`")

@bot.on(events.CallbackQuery(data=b"growth_skit"))
async def growth_skit(event):
    await event.answer()
    await _growth_input(event, "growth_skit_input",
        "✏️ Custom share text:\n(Use `{ref_link}` for referral link)")

@bot.on(events.CallbackQuery(data=b"growth_rb"))
async def growth_rb(event):
    await event.answer()
    await _growth_input(event, "growth_rb_input", "💰 Referral bonus coins:")

@bot.on(events.CallbackQuery(data=b"growth_int"))
async def growth_int(event):
    await event.answer()
    await _growth_input(event, "growth_int_input", "⏰ Auto post interval (hours):")


@bot.on(events.NewMessage())
async def growth_input_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data
    d = get_user_data(uid); step = d.get("step") or ""
    ge = TB._cfg()["growth"]; txt = event.raw_text.strip()

    if step == "growth_ch_input":
        if txt.startswith("-"):
            ch = txt[1:].strip()
            if ch in ge.get("promo_channels",[]): ge["promo_channels"].remove(ch)
            await event.respond(f"🗑 `{ch}` removed!")
        else:
            ge.setdefault("promo_channels",[]).append(txt)
            await event.respond(f"✅ `{txt}` added!")
        TB._save(); d["step"] = None
    elif step == "growth_skit_input":
        ge["share_kit_text"] = txt; TB._save(); d["step"] = None
        await event.respond("✅ Share kit updated!", buttons=[[Button.inline("◀️ Growth", b"adm_growth")]])
    elif step == "growth_rb_input":
        try:
            ge["referral_bonus"] = int(txt); TB._save(); d["step"] = None
            await event.respond("✅ Referral bonus set!", buttons=[[Button.inline("◀️", b"adm_growth")]])
        except: await event.respond("❌ Number!")
    elif step == "growth_int_input":
        try:
            ge["auto_interval"] = int(txt)*3600; TB._save(); d["step"] = None
            await event.respond("✅ Interval set!", buttons=[[Button.inline("◀️", b"adm_growth")]])
        except: await event.respond("❌ Ghante mein number!")

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — WITHDRAWALS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_wd_panel"))
async def adm_wd_panel(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    wd  = TB._cfg()["withdrawal"]
    pnd = [r for r in wd.get("pending",[]) if r.get("status")=="pending"]
    on  = wd.get("enabled",False)
    txt = (
        f"💸 **Withdrawals**\n"
        f"Status: **{'🟢 ON' if on else '🔴 OFF'}**\n"
        f"Min coins: `{wd.get('min_coins',1000)}`\n"
        f"Methods: `{len(wd.get('methods',[]))}`\n\n"
        f"⏳ Pending: **{len(pnd)}**"
    )
    btns = [
        [Button.inline("🟢 Enable" if not on else "🔴 Disable", b"wd_toggle")],
        [Button.inline("➕ Add Method", b"wd_add_method"),
         Button.inline("📋 Pending",   b"wd_pending")],
        [Button.inline("◀️ Back", b"adm_task_board")],
    ]
    await _safe_edit(event, txt, btns)

@bot.on(events.CallbackQuery(data=b"wd_toggle"))
async def wd_toggle(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    wd = TB._cfg()["withdrawal"]; wd["enabled"] = not wd.get("enabled",False); TB._save()
    await adm_wd_panel(event)

@bot.on(events.CallbackQuery(data=b"wd_add_method"))
async def wd_add_method(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    from database import get_user_data
    get_user_data(event.sender_id)["step"] = "wd_method_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await _safe_edit(event,
        "➕ **Add Withdrawal Method:**\nFormat: `type|label`\ne.g. `upi|UPI Transfer` ya `paytm|Paytm Wallet`",
        [[Button.inline("❌ Cancel", b"adm_wd_panel")]]
    )

@bot.on(events.NewMessage())
async def wd_method_input_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data
    d = get_user_data(uid)
    if d.get("step") != "wd_method_input": return
    try:
        mtype, mlabel = event.raw_text.strip().split("|",1)
        TB._cfg()["withdrawal"].setdefault("methods",[]).append({"type":mtype.strip(),"label":mlabel.strip()})
        TB._save(); d["step"] = None
        await event.respond("✅ Method added!", buttons=[[Button.inline("💸 Withdrawals", b"adm_wd_panel")]])
    except: await event.respond("❌ Format: `type|label`")

@bot.on(events.CallbackQuery(data=b"wd_pending"))
async def wd_pending(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    pnd = [r for r in TB._cfg()["withdrawal"].get("pending",[]) if r.get("status")=="pending"]
    if not pnd:
        return await _safe_edit(event, "💸 Koi pending request nahi!",
            [[Button.inline("◀️", b"adm_wd_panel")]])
    btns = []
    for r in pnd[:8]:
        sym = TB._cfg().get("coin_symbol","🪙")
        btns.append([Button.inline(
            f"👤{r['uid']} · {r['coins']}{sym} · {r['method']}",
            f"wd_rv|{r['id']}".encode()
        )])
    btns.append([Button.inline("◀️ Back", b"adm_wd_panel")])
    await _safe_edit(event, f"⏳ **Pending ({len(pnd)}):**", btns)

@bot.on(events.CallbackQuery(pattern=b"wd_rv\\|(.+)"))
async def wd_rv(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    rid = event.data.decode().split("|")[1]
    req = next((r for r in TB._cfg()["withdrawal"].get("pending",[]) if r.get("id")==rid), None)
    if not req: return await event.answer("Nahi mila!", alert=True)
    sym = TB._cfg().get("coin_symbol","🪙")
    await _safe_edit(event,
        f"💸 **Withdrawal Request**\n\n"
        f"👤 `{req['uid']}`\n💰 {req['coins']}{sym}\n📋 {req['method']}: {req.get('details','—')}\n🕐 {_ts(req.get('ts',0))}",
        [[Button.inline("✅ Mark Done", f"wd_done|{rid}".encode()),
          Button.inline("❌ Reject",   f"wd_rej|{rid}".encode())],
         [Button.inline("◀️ Back", b"wd_pending")]]
    )

@bot.on(events.CallbackQuery(pattern=b"wd_(done|rej)\\|(.+)"))
async def wd_action(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    parts  = event.data.decode().split("|")
    action, rid = parts[0].replace("wd_",""), parts[1]
    pnd = TB._cfg()["withdrawal"].get("pending",[])
    req = next((r for r in pnd if r.get("id")==rid), None)
    if not req: return await event.answer("Nahi mila!", alert=True)
    req["status"] = "done" if action=="done" else "rejected"
    if action == "rej":
        TB.add_coins(req["uid"], req["coins"], "Withdrawal refund")
    TB._save()
    sym = TB._cfg().get("coin_symbol","🪙")
    try:
        if action=="done":
            await bot.send_message(req["uid"], f"✅ Withdrawal processed! {req['coins']}{sym}")
        else:
            await bot.send_message(req["uid"], f"❌ Withdrawal rejected. {req['coins']}{sym} refunded!")
    except Exception: pass
    await event.answer(f"{'✅ Done' if action=='done' else '❌ Rejected'}!", alert=False)
    await wd_pending(event)

# ─────────────────────────────────────────────────────────────────────────────
# ADMIN — SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_task_cfg"))
async def adm_task_cfg(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    cfg = TB._cfg()
    sym = cfg.get("coin_symbol","🪙")
    await _safe_edit(event,
        f"⚙️ **Task Board Settings**\n\n"
        f"🪙 Name: `{cfg.get('coin_name','Coins')}`\n"
        f"🪙 Symbol: `{sym}`\n"
        f"💎 Coins per 1d Premium: `{cfg.get('coins_to_premium',500)}`\n"
        f"📋 Max daily tasks: `{cfg.get('max_daily_tasks',30)}`\n"
        f"🚨 Anti-cheat: `{'ON' if cfg['anti_cheat'].get('enabled') else 'OFF'}`",
        [[Button.inline("🟢/🔴 Toggle Board", b"task_sys_toggle")],
         [Button.inline("✏️ Coin Name",      b"tcfg|coin_name"),
          Button.inline("✏️ Symbol",         b"tcfg|coin_symbol")],
         [Button.inline("💎 Prem Rate",      b"tcfg|coins_to_premium"),
          Button.inline("📋 Daily Limit",    b"tcfg|max_daily_tasks")],
         [Button.inline("◀️ Back",            b"adm_task_board")]]
    )

@bot.on(events.CallbackQuery(data=b"task_sys_toggle"))
async def task_sys_toggle(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    TB._cfg()["enabled"] = not TB._cfg().get("enabled",True); TB._save()
    await adm_task_board(event)

@bot.on(events.CallbackQuery(pattern=b"tcfg\\|(.+)"))
async def tcfg(event):
    await event.answer()
    try: _adm(event)
    except: return await event.answer("❌", alert=True)
    key = event.data.decode().split("|")[1]
    from database import get_user_data
    d = get_user_data(event.sender_id)
    d["step"] = "tcfg_val"; d["tcfg_key"] = key
    d["step_since"] = time.time()
    await _safe_edit(event, f"⚙️ New value for `{key}`:", [[Button.inline("❌", b"adm_task_cfg")]])

@bot.on(events.NewMessage())
async def tcfg_handler(event):
    if not event.is_private: return
    uid = event.sender_id
    if not is_admin(uid): return
    from database import get_user_data
    d = get_user_data(uid)
    if d.get("step") != "tcfg_val": return
    key = d.get("tcfg_key",""); txt = event.raw_text.strip()
    try:
        val = int(txt) if key in ("coins_to_premium","max_daily_tasks") else txt
        TB._cfg()[key] = val; TB._save(); d["step"] = None
        await event.respond("✅ Updated!", buttons=[[Button.inline("⚙️ Settings", b"adm_task_cfg")]])
    except: await event.respond("❌ Invalid value!")
