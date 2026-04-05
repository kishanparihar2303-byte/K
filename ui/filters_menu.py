import time
from telethon import events, Button, errors
from utils import safe_split_data, safe_int
from config import bot
from database import get_user_data, save_persistent_db

def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

@bot.on(events.CallbackQuery(data=b"advanced_filters"))
async def advanced_filters_menu(event):
    await event.answer()
    uid      = event.sender_id
    data     = get_user_data(uid)
    settings = data.get("settings", {})

    dup_on    = settings.get("duplicate_filter", False)
    prod_on   = settings.get("product_duplicate_filter", False)
    glo_on    = settings.get("global_filter", False)
    smart_on  = settings.get("smart_dup", False)
    kw_count  = len(data.get("keyword_filters", {}).get("words", []))
    kw_on     = data.get("keyword_filters", {}).get("enabled", False)
    bl_count  = len(data.get("blocked_links", {}))
    repl_count = len(data.get("replacements", {}))
    lb_on     = data.get("link_blocker_enabled", False)

    # Active features count
    active = sum([dup_on, prod_on, kw_on, lb_on, bool(repl_count)])

    def status(on): return "🟢" if on else "🔴"

    txt = (
        "🧠 **ADVANCED FILTERS & TOOLS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"**{active} filters active right now**\n\n"
        f"{status(dup_on)} **Dup Filter** — same msg dobara forward nahi hoga\n"
        + (f"  └ Product: {status(prod_on)}  Global: {status(glo_on)}  Smart: {status(smart_on)}\n" if dup_on else "")
        + f"{status(kw_on)} **Keyword Filter** — `{kw_count}` words tracked\n"
        f"{status(lb_on)} **Link Blocker** — `{bl_count}` links/domains blocked\n"
        f"{'🟢' if repl_count else '⚪'} **Replacements** — `{repl_count}` rules active\n"
        "\n_Kisi bhi option ko tap karo manage karne ke liye:_"
    )
    try:
        await event.edit(txt, buttons=[
            [Button.inline(f"♻️ Dup Filter {status(dup_on)}",     b"dup_menu"),
             Button.inline(f"🔍 Keywords {status(kw_on)} ({kw_count})", b"kw_filter_menu")],
            [Button.inline(f"🚫 Link Blocker {status(lb_on)} ({bl_count})", b"link_block_menu"),
             Button.inline(f"🔄 Replace ({repl_count})",           b"replace_menu")],
            [Button.inline("📅 Scheduler",                          b"sched_menu"),
             Button.inline("⚙️ Global Settings",                    b"settings_menu")],
            [Button.inline("🏠 Main Menu",                          b"main_menu")],
        ])
    except errors.MessageNotModifiedError:
        pass

# REPLACEMENTS
@bot.on(events.CallbackQuery(data=b"replace_menu"))
async def replace_menu(event):
    await event.answer()
    uid    = event.sender_id
    repls  = get_user_data(uid).get("replacements", {})
    count  = len(repls)
    preview = ""
    if repls:
        sample = list(repls.items())[:3]
        preview = "\n**Active rules:**\n" + "".join(
            f"  `{k[:20]}` → `{v[:20]}`\n" for k, v in sample
        )
        if count > 3:
            preview += f"  _...aur {count-3} aur_\n"
    try:
        await event.edit(
            "🔄 **REPLACEMENTS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "Kisi bhi text ya link ko automatically replace karo.\n"
            "_Example: `@oldchan` → `@newchan`  ·  `amzn.to` → `yourlink.com`_\n\n"
            f"Total rules: **{count}**"
            + preview,
            buttons=[
                [Button.inline("➕ Add Rule",          b"add_repl"),
                 Button.inline(f"📋 View All ({count})", b"list_repl_0")],
                [Button.inline("🗑 Clear All",           b"clear_repl")],
                [Button.inline("🔙 Filters",             b"advanced_filters"),
                 Button.inline("🏠 Menu",                b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"list_repl_"))
async def list_repl_handler(event, page=None):
    if page is None:
        try: page = int(event.data.decode().split("_")[-1])
        except: page = 0
        
    data = get_user_data(event.sender_id)
    repls = list(data["replacements"].items())
    
    if not repls:
        await event.answer("No replacements found. Returning...", alert=True)
        try:
            return await replace_menu(event)
        except errors.MessageNotModifiedError:
            return

    MAX_PER_PAGE = 5
    start = page * MAX_PER_PAGE
    end = start + MAX_PER_PAGE
    subset = repls[start:end]
    
    txt = "📋 **Replacement List**\nSelect to Delete/Edit:"
    buttons = []
    for i, (k, v) in enumerate(subset):
        idx = start + i
        label = f"{k} -> {v}"[:30]
        buttons.append([Button.inline(label, f"view_repl_{idx}".encode())])
    
    nav = []
    if page > 0: nav.append(Button.inline("⬅️ Prev", f"list_repl_{page-1}".encode()))
    if end < len(repls): nav.append(Button.inline("Next ➡️", f"list_repl_{page+1}".encode()))
    if nav: buttons.append(nav)
    buttons.append([Button.inline("🔙 Back", b"replace_menu")])
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"view_repl_"))
async def view_repl_item(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    repls = list(data["replacements"].items())
    if idx >= len(repls): return await event.answer("Item not found")
    k, v = repls[idx]
    txt = f"🔄 **Replacement Detail**\n\nOriginal: `{k}`\nNew: `{v}`"
    try:
        await event.edit(txt, buttons=[
            [Button.inline("🗑 Delete", f"del_repl_{idx}".encode())],
            [Button.inline("🔙 Back", b"list_repl_0")]
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"del_repl_"))
async def del_repl_item(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    repls = list(data["replacements"].items())
    if idx < len(repls):
        key_to_del = repls[idx][0]
        del data["replacements"][key_to_del]
        save_persistent_db()
    await event.answer("🗑 Deleted!")
    # FIX: Direct call instead of event.data modification
    await list_repl_handler(event, page=0)

@bot.on(events.CallbackQuery(data=b"add_repl"))
async def add_repl(event):
    await event.answer()  # ⚡ instant ack
    get_user_data(event.sender_id)["step"] = "wait_repl_old"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("Step 1/2: Send the **Old Word/Link** (What you want to remove/change).", buttons=[Button.inline("🔙 Back", b"replace_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"clear_repl"))
async def clear_repl(event):
    await event.answer()  # ⚡ instant ack
    get_user_data(event.sender_id)["replacements"] = {}
    save_persistent_db()
    await event.answer("🗑 All replacements cleared!", alert=True)
    try:
        await replace_menu(event)
    except errors.MessageNotModifiedError:
        pass

# LINK BLOCKER
@bot.on(events.CallbackQuery(data=b"link_block_menu"))
async def link_block_menu(event):
    await event.answer()
    uid    = event.sender_id
    data   = get_user_data(uid)
    blocks = data.get("blocked_links", {})
    on     = data.get("link_blocker_enabled", False)
    count  = len(blocks)

    preview = ""
    if blocks:
        sample = list(blocks.items())[:4]
        preview = "\n**Blocked:**\n" + "".join(
            f"  🚫 `{k[:30]}`" + (f" (max {v}x)" if isinstance(v, int) and v > 0 else "") + "\n"
            for k, v in sample
        )
        if count > 4:
            preview += f"  _...aur {count-4} aur_\n"

    try:
        await event.edit(
            "🚫 **LINK BLOCKER**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Status: **{'🟢 ON' if on else '🔴 OFF'}**  ·  Blocked: **{count}**\n\n"
            "**Kya block kar sakte ho:**\n"
            "  • Domain: `amazon.in` — us site ki saari links\n"
            "  • Full URL: kisi specific link\n"
            "  • Limit: `amzn.to | 3` — sirf 3 baar allow, phir block\n"
            + preview,
            buttons=[
                [Button.inline(f"{'🔴 Blocker OFF karo' if on else '🟢 Blocker ON karo'}", b"lb_toggle_enable")],
                [Button.inline("➕ Add Block",           b"add_block_link"),
                 Button.inline(f"📋 View All ({count})", b"list_block_0")],
                [Button.inline("🗑 Clear All",            b"clear_block")],
                [Button.inline("🔙 Filters",              b"advanced_filters"),
                 Button.inline("🏠 Menu",                 b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"lb_toggle_enable"))
async def lb_toggle_enable(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    current = data.get("settings", {}).get("link_blocker_enabled", True)
    data.setdefault("settings", {})["link_blocker_enabled"] = not current
    save_persistent_db()
    await event.answer(f"Link Blocker: {'Paused' if current else 'Active'}")
    await link_block_menu(event)

@bot.on(events.CallbackQuery(pattern=b"list_block_"))
async def list_block_handler(event, page=None):
    if page is None:
        try: page = int(event.data.decode().split("_")[-1])
        except: page = 0
        
    data = get_user_data(event.sender_id)
    blocked = list(data["blocked_links"].keys())
    
    if not blocked: 
        await event.answer("List empty. Returning...", alert=True)
        try:
            return await link_block_menu(event)
        except errors.MessageNotModifiedError:
            return

    MAX = 5
    start = page * MAX
    end = start + MAX
    subset = blocked[start:end]
    txt = "📋 **Blocked Links List**"
    buttons = []
    for i, link in enumerate(subset):
        idx = start + i
        limit = data["link_limits"].get(link, 0)
        lim_txt = "∞" if limit == 0 else str(limit)
        buttons.append([Button.inline(f"{link} (Limit: {lim_txt})", f"view_block_{idx}".encode())])
    nav = []
    if page > 0: nav.append(Button.inline("⬅️ Prev", f"list_block_{page-1}".encode()))
    if end < len(blocked): nav.append(Button.inline("Next ➡️", f"list_block_{page+1}".encode()))
    if nav: buttons.append(nav)
    buttons.append([Button.inline("🔙 Back", b"link_block_menu")])
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"view_block_"))
async def view_block_item(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    blocked = list(data["blocked_links"].keys())
    if idx >= len(blocked): return await event.answer("Item not found")
    link = blocked[idx]
    limit = data["link_limits"].get(link, 0)
    txt = f"🚫 **Blocked Link Detail**\n\nLink: `{link}`\nLimit: {limit} (0 = Forever)"
    try:
        await event.edit(txt, buttons=[
            [Button.inline("🗑 Delete", f"del_block_{idx}".encode())],
            [Button.inline("🔙 Back", b"list_block_0")]
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"del_block_"))
async def del_block_item(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    blocked = list(data["blocked_links"].keys())
    if idx < len(blocked):
        link = blocked[idx]
        del data["blocked_links"][link]
        if link in data["link_limits"]: del data["link_limits"][link]
        save_persistent_db()
    await event.answer("🗑 Unblocked!")
    # FIX: Direct call instead of event.data modification
    await list_block_handler(event, page=0)

@bot.on(events.CallbackQuery(data=b"add_block_link"))
async def add_block_link_cb(event):
    await event.answer()  # ⚡ instant ack
    get_user_data(event.sender_id)["step"] = "wait_link_block_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("Send the link to block.\n(Optionally add `| count` for limit, e.g., `amzn.to/abc | 2` to block only after 2 posts).", buttons=[Button.inline("🔙 Back", b"link_block_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"clear_block"))
async def clear_block_cb(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    data["blocked_links"] = {}
    data["link_limits"] = {}
    save_persistent_db()
    await event.answer("🗑 Link blocker database reset!", alert=True)
    try:
        await link_block_menu(event)
    except errors.MessageNotModifiedError:
        pass

# KEYWORD FILTER
@bot.on(events.CallbackQuery(data=b"kw_filter_menu"))
async def kw_filter_menu(event):
    await event.answer()
    uid   = event.sender_id
    data  = get_user_data(uid)
    kf    = data.get("keyword_filters", {})
    on    = kf.get("enabled", False)
    mode  = kf.get("mode", "block")  # block = block matching, allow = only allow matching
    bl    = kf.get("words", [])
    wl    = kf.get("whitelist", [])
    bl_c  = len(bl)
    wl_c  = len(wl)

    mode_lbl  = "🚫 Block mode — matching msgs BLOCKED" if mode == "block" else "✅ Allow mode — ONLY matching msgs forwarded"
    mode_flip = "Switch to ✅ Allow Mode" if mode == "block" else "Switch to 🚫 Block Mode"

    preview = ""
    if bl:
        sample = bl[:5]
        preview = "\n**Keywords:**\n" + "  ".join(f"`{w}`" for w in sample)
        if bl_c > 5: preview += f"  _{bl_c-5} more_"
        preview += "\n"

    try:
        await event.edit(
            "🔍 **KEYWORD FILTER**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Status: **{'🟢 ON' if on else '🔴 OFF'}**\n"
            f"Mode: **{mode_lbl}**\n\n"
            f"📋 Keywords: **{bl_c}**  ·  ✅ Whitelist: **{wl_c}**"
            + preview,
            buttons=[
                [Button.inline(f"{'🔴 OFF karo' if on else '🟢 ON karo'}", b"kw_tgl_enable")],
                [Button.inline(mode_flip, b"kw_toggle_mode")],
                [Button.inline(f"➕ Add Keyword",        b"kw_add"),
                 Button.inline(f"📋 Keywords ({bl_c})",  b"kw_list_bl_0")],
                [Button.inline(f"✅ Whitelist ({wl_c})", b"kw_list_wl_0"),
                 Button.inline("🗑 Clear All",            b"kw_clear")],
                [Button.inline("🔙 Filters",              b"advanced_filters"),
                 Button.inline("🏠 Menu",                 b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"kw_tgl_enable"))
async def kw_tgl_enable(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    cur  = data["settings"].get("keyword_filter_enabled", True)
    data["settings"]["keyword_filter_enabled"] = not cur
    save_persistent_db()
    await event.answer(f"Keyword Filter: {'Paused' if cur else 'Active'}")
    await kw_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"kw_open_bl"))
async def kw_open_bl(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    data["settings"]["filter_mode"] = "Blacklist"
    save_persistent_db()
    await kw_filter_menu(event)


@bot.on(events.CallbackQuery(data=b"kw_open_wl"))
async def kw_open_wl(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    data["settings"]["filter_mode"] = "Whitelist"
    save_persistent_db()
    await kw_filter_menu(event)


@bot.on(events.CallbackQuery(pattern=b"kw_list_"))
async def kw_list_handler(event, page=None):
    if page is None:
        try: page = int(event.data.decode().split("_")[-1])
        except: page = 0
        
    data = get_user_data(event.sender_id)
    mode = data["settings"]["filter_mode"]
    kws = data["settings"]["keywords_blacklist"] if mode == "Blacklist" else data["settings"]["keywords_whitelist"]

    if not kws: 
        await event.answer(f"No keywords in {mode}. Returning...", alert=True)
        try:
            return await kw_filter_menu(event)
        except errors.MessageNotModifiedError:
            return

    MAX = 5
    start = page * MAX
    end = start + MAX
    subset = kws[start:end]
    txt = f"📋 **{mode} Keywords**"
    buttons = []
    for i, kw in enumerate(subset):
        idx = start + i
        buttons.append([Button.inline(kw, f"view_kw_{idx}".encode())])
    nav = []
    if page > 0: nav.append(Button.inline("⬅️ Prev", f"kw_list_{page-1}".encode()))
    if end < len(kws): nav.append(Button.inline("Next ➡️", f"kw_list_{page+1}".encode()))
    if nav: buttons.append(nav)
    buttons.append([Button.inline("🔙 Back", b"kw_filter_menu")])
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"view_kw_"))
async def view_kw_item(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    mode = data["settings"]["filter_mode"]
    kws = data["settings"]["keywords_blacklist"] if mode == "Blacklist" else data["settings"]["keywords_whitelist"]
    if idx >= len(kws): return await event.answer("Item not found")
    txt = f"🔍 **Keyword Detail**\n\nWord: `{kws[idx]}`\nList: {mode}"
    try:
        await event.edit(txt, buttons=[
            [Button.inline("🗑 Delete", f"del_kw_{idx}".encode())],
            [Button.inline("🔙 Back", b"kw_list_0")]
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"del_kw_"))
async def del_kw_item(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    mode = data["settings"]["filter_mode"]
    target_list = data["settings"]["keywords_blacklist"] if mode == "Blacklist" else data["settings"]["keywords_whitelist"]
    if idx < len(target_list):
        del target_list[idx]
        save_persistent_db()
    await event.answer("🗑 Deleted!")
    # FIX: Direct call instead of event.data modification
    await kw_list_handler(event, page=0)

@bot.on(events.CallbackQuery(data=b"kw_toggle_mode"))
async def kw_toggle_mode(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    data["settings"]["filter_mode"] = "Whitelist" if data["settings"]["filter_mode"] == "Blacklist" else "Blacklist"
    save_persistent_db()
    await kw_filter_menu(event)

@bot.on(events.CallbackQuery(data=b"kw_add"))
async def kw_add_cb(event):
    await event.answer()  # ⚡ instant ack
    get_user_data(event.sender_id)["step"] = "wait_kw_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("Send the Keyword(s) you want to add. (Separate with comma for multiple)", buttons=[Button.inline("🔙 Back", b"kw_filter_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"kw_clear"))
async def kw_clear_cb(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    mode = data["settings"]["filter_mode"]
    if mode == "Blacklist":
        data["settings"]["keywords_blacklist"] = []
    else:
        data["settings"]["keywords_whitelist"] = []
    save_persistent_db()
    await event.answer(f"🗑 {mode} Keywords cleared!")
    try:
        await kw_filter_menu(event)
    except errors.MessageNotModifiedError:
        pass

# DUPLICATE FILTER
@bot.on(events.CallbackQuery(data=b"dup_menu"))
async def dup_menu(event):
    await event.answer()  # ⚡ instant ack
    from filters import get_dup_stats
    uid   = event.sender_id
    data  = get_user_data(uid)
    s     = data["settings"]
    on    = s.get("duplicate_filter", False)
    prod  = s.get("product_duplicate_filter", False)
    glo   = s.get("global_filter", False)
    smart = s.get("smart_dup", False)
    exp   = s.get("dup_expiry_hours", 2)
    wl    = len(s.get("dup_whitelist_words", []))
    stats = get_dup_stats(uid)

    # Expiry label
    if exp < 1:
        exp_label = f"{int(exp * 60)} minute"
    elif exp == 1:
        exp_label = "1 ghanta"
    elif exp < 24:
        exp_label = f"{int(exp)} ghante"
    elif exp == 24:
        exp_label = "1 din"
    else:
        exp_label = f"{int(exp // 24)} din"

    # Scope description
    if not on:
        scope_info = "\u26a0\ufe0f Filter band hai — abhi koi message block nahi hoga"
    elif glo:
        scope_info = "\U0001f30d Sabhi sources milake check karo"
    else:
        scope_info = "\U0001f4cc Har source alag alag check karo"

    # Button labels
    main_lbl  = "\u2705 Duplicate Filter: ON  (Tap = OFF)" if on else "\u274c Duplicate Filter: OFF  (Tap = ON)"
    scope_lbl = "\U0001f30d Scope: Sab Milake (Tap = Alag)" if glo else "\U0001f4cc Scope: Source Alag (Tap = Sab Milake)"
    prod_lbl  = "\U0001f7e2 Product Filter: ON" if prod else "\U0001f534 Product Filter: OFF"
    smart_lbl = "\U0001f7e2 Smart Match: ON" if smart else "\U0001f534 Smart Match: OFF"

    txt = (
        "\u267b\ufe0f **Duplicate (Copy) Filter**\n"
        "\u2501" * 30 + "\n\n"

        f"**Status:** {'\U0001f7e2 Chal raha hai' if on else '\U0001f534 Band hai'}\n"
        f"**Scope:** {scope_info}\n\n"

        "**Aaj tak ka hisaab:**\n"
        f"  \U0001f6ab Block hue: `{stats['today_blocked']}` messages\n"
        f"  \U0001f5c4 Yaaddasht mein: `{stats['active_entries']}` entries\n"
        f"  \u23f1 Yaad rehne ki muddat: **{exp_label}**\n\n"

        "**Baaki features:**\n"
        f"  {'\U0001f7e2' if prod else '\U0001f534'} **Product Filter** — "
        "Same saman (Amazon/Flipkart link) dobara na aye\n"
        f"  {'\U0001f7e2' if smart else '\U0001f534'} **Smart Match** — "
        "Thoda alag text bhi pakde (price change ignore)\n"
        f"  \U0001f4dd Whitelist: `{wl}` words\n\n"

        "**Samajhne ke liye:**\n"
        "  \u2022 **Source Alag** = Source A ka dup sirf Source A se check hoga\n"
        "  \u2022 **Sab Milake** = Ek baar kisi bhi source se aya = har jagah block\n"
        "  \u2022 **Product Filter** = Link se ASIN/product ID nikaal ke check karta hai\n"
        "  \u2022 **Smart Match** = \"Buy at \u20b9500\" aur \"Buy at \u20b9600\" = same (price ignore)"
    )
    try:
        await event.edit(txt, buttons=[
            [Button.inline(main_lbl, b"dup_toggle")],
            [Button.inline(scope_lbl, b"dup_toggle_global")],
            [Button.inline(prod_lbl, b"dup_toggle_prod"),
             Button.inline(smart_lbl, b"dup_toggle_smart")],
            [Button.inline(f"\u23f1 Muddat: {exp_label}", b"dup_set_expiry_flow"),
             Button.inline("\U0001f4dc Block Log", b"dup_view_log")],
            [Button.inline("\u2795 Whitelist Add", b"dup_add_whitelist"),
             Button.inline(f"\U0001f4cb Whitelist ({wl})", b"dup_list_white_0")],
            [Button.inline("\U0001f5d1 Yaaddasht Clear", b"dup_clear_history"),
             Button.inline("\U0001f5d1 Whitelist Clear", b"dup_clear_whitelist")],
            [Button.inline("\U0001f519 Wapas", b"advanced_filters")],
        ])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"dup_explain"))
async def dup_explain(event):
    await event.answer()
    lines = [
        "♻️ **Duplicate Filter — Puri Jankari**",
        "━" * 28,
        "",
        "💡 **Ye Filter Kya Karta Hai?**",
        "Source se koi message aane par bot check karta hai:",
        "kya ye pehle aaya tha? Agar haan → block.",
        "",
        "📌 **Source Alag (Default mode):**",
        "Har source ka record alag rakha jaata hai.",
        "Channel A ne photo bheji + Channel B ne wahi bheji",
        "→ Channel B ki photo forward hogi (alag source hai).",
        "",
        "🌍 **Sab Milake (Global Dup):**",
        "Kisi bhi source ne content bheja → sab jagah block.",
        "Channel A ne photo bheji + Channel B ne wahi bheji",
        "→ Channel B ki photo bhi block hogi.",
        "",
        "🛍 **Product Filter:**",
        "Amazon/Flipkart links se product ID nikaal ke check.",
        "amzn.to/abc aur amazon.in/dp/B08XYZ → same → block.",
        "Caption alag ho tab bhi kaam karta hai.",
        "",
        "🧠 **Smart Match:**",
        "Numbers/prices ignore karta hai.",
        '"Sale! Rs 499" aur "Sale! Rs 599" → same → block.',
        "",
        "⏱ **Muddat (Expiry):**",
        "Kitni der tak yaad rakhe. 2 ghante baad",
        "same message dobara aaye → naya maana jaayega.",
        "",
        "📝 **Whitelist:**",
        "Kuch words add karo jo kabhi block na hon.",
        "E.g. 'BREAKING' → 'BREAKING NEWS' hamesha forward hoga.",
    ]
    txt = "\n".join(lines)
    try:
        await event.edit(txt, buttons=[[Button.inline("🔙 Wapas", b"dup_menu")]])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"dup_list_white_"))
async def dup_list_white(event, page=None):
    if page is None:
        try: page = int(event.data.decode().split("_")[-1])
        except: page = 0
        
    data = get_user_data(event.sender_id)
    w_list = data["settings"]["dup_whitelist_words"]
    
    if not w_list: 
        await event.answer("Whitelist is empty. Returning...", alert=True)
        try:
            return await dup_menu(event)
        except errors.MessageNotModifiedError:
            return

    MAX = 5
    start = page * MAX
    end = start + MAX
    subset = w_list[start:end]
    txt = "📋 **Duplicate Whitelist**"
    buttons = []
    for i, w in enumerate(subset):
        idx = start + i
        buttons.append([Button.inline(w, f"view_dup_w_{idx}".encode())])
    nav = []
    if page > 0: nav.append(Button.inline("⬅️ Prev", f"dup_list_white_{page-1}".encode()))
    if end < len(w_list): nav.append(Button.inline("Next ➡️", f"dup_list_white_{page+1}".encode()))
    if nav: buttons.append(nav)
    buttons.append([Button.inline("🔙 Back", b"dup_menu")])
    try:
        await event.edit(txt, buttons=buttons)
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"view_dup_w_"))
async def view_dup_w(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    w_list = data["settings"]["dup_whitelist_words"]
    if idx >= len(w_list): return await event.answer("Not found")
    try:
        await event.edit(f"Word: `{w_list[idx]}`", buttons=[
            [Button.inline("🗑 Delete", f"del_dup_w_{idx}".encode())],
            [Button.inline("🔙 Back", b"dup_list_white_0")]
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"del_dup_w_"))
async def del_dup_w(event):
    await event.answer()
    idx = int(event.data.decode().split("_")[-1])
    data = get_user_data(event.sender_id)
    w_list = data["settings"]["dup_whitelist_words"]
    if idx < len(w_list):
        del w_list[idx]
        save_persistent_db()
    await event.answer("Deleted!")
    # FIX: Direct call instead of event.data modification
    await dup_list_white(event, page=0)

@bot.on(events.CallbackQuery(data=b"dup_toggle"))
async def dup_toggle(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    new_val = not data["settings"].get("duplicate_filter", False)
    data["settings"]["duplicate_filter"] = new_val
    save_persistent_db()
    await event.answer(
        "🟢 Duplicate Filter ON kar diya!" if new_val
        else "🔴 Duplicate Filter OFF kar diya!",
        alert=True
    )
    await dup_menu(event)


@bot.on(events.CallbackQuery(data=b"dup_toggle_prod"))
async def dup_toggle_prod(event):
    await event.answer()  # ⚡ instant ack
    data    = get_user_data(event.sender_id)
    cur     = data["settings"].get("product_duplicate_filter", False)
    new_val = not cur
    data["settings"]["product_duplicate_filter"] = new_val
    save_persistent_db()
    await event.answer(
        "🟢 Product Filter ON — Amazon/Flipkart links se check hoga" if new_val
        else "🔴 Product Filter OFF",
        alert=True
    )
    await dup_menu(event)


@bot.on(events.CallbackQuery(data=b"dup_toggle_global"))
async def dup_toggle_global(event):
    await event.answer()  # ⚡ instant ack
    data    = get_user_data(event.sender_id)
    cur     = data["settings"].get("global_filter", False)
    new_val = not cur
    data["settings"]["global_filter"] = new_val
    save_persistent_db()
    await event.answer(
        "🌍 Scope: Sab Milake — Sabhi sources milake check hoga" if new_val
        else "📌 Scope: Source Alag — Har source alag check hoga",
        alert=True
    )
    await dup_menu(event)


@bot.on(events.CallbackQuery(data=b"dup_toggle_smart"))
async def dup_toggle_smart(event):
    await event.answer()  # ⚡ instant ack
    data    = get_user_data(event.sender_id)
    cur     = data["settings"].get("smart_dup", False)
    new_val = not cur
    data["settings"]["smart_dup"] = new_val
    save_persistent_db()
    await event.answer(
        "🟢 Smart Match ON — price/number change ignore hoga" if new_val
        else "🔴 Smart Match OFF",
        alert=True
    )
    await dup_menu(event)


@bot.on(events.CallbackQuery(data=b"dup_view_log"))
async def dup_view_log(event):
    await event.answer()
    from filters import get_dup_stats
    uid   = event.sender_id
    stats = get_dup_stats(uid)
    log   = stats["recent_log"]
    if not log:
        await event.answer("Abhi tak koi message block nahi hua.", alert=True)
        return
    import datetime as _dt
    lines = []
    for entry in log:
        ts  = entry.get("ts", 0)
        tm  = _dt.datetime.fromtimestamp(ts).strftime("%d/%m %H:%M")
        txt = entry.get("text_preview", "[media]")[:40]
        lines.append(f"`{tm}` — {txt}")
    msg = (
        "\U0001f4dc **Block Log — Haal ke Blocked Messages**\n"
        "\u2501" * 28 + "\n\n"
        "_Ye messages duplicate hone ki wajah se forward nahi hue:_\n\n" +
        "\n".join(lines) +
        "\n\n_Sirf aakhri 10 dikhaye ja rahe hain_"
    )
    try:
        await event.edit(msg, buttons=[[Button.inline("🔙 Back", b"dup_menu")]])
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"dup_clear_history"))
async def dup_clear_history(event):
    await event.answer()
    from database import get_dup_data
    uid  = event.sender_id
    dup  = get_dup_data(uid)
    cnt  = len(dup.get("history", {}))
    dup["history"] = {}
    dup["blocked_log"] = []
    save_persistent_db()
    await event.answer(
        f"\u2705 {cnt} entries delete ho gayi! Ab koi bhi message duplicate nahi maana jaayega.",
        alert=True
    )
    await dup_menu(event)


@bot.on(events.CallbackQuery(data=b"dup_set_expiry_flow"))
async def dup_set_expiry_flow(event):
    await event.answer()
    txt = (
        "\u23f1 **Yaad Rehne Ki Muddat Badlo**\n"
        "\u2501" * 25 + "\n\n"
        "Kitne time tak same message dobara ane par block karein?\n\n"
        "**Ready-made options:**"
    )
    try:
        await event.edit(txt, buttons=[
            [Button.inline("30 min", b"dup_preset_0.5"),
             Button.inline("1 ghanta", b"dup_preset_1"),
             Button.inline("2 ghante", b"dup_preset_2")],
            [Button.inline("6 ghante", b"dup_preset_6"),
             Button.inline("12 ghante", b"dup_preset_12"),
             Button.inline("1 din", b"dup_preset_24")],
            [Button.inline("3 din", b"dup_preset_72"),
             Button.inline("7 din", b"dup_preset_168"),
             Button.inline("Hamesha", b"dup_preset_8760")],
            [Button.inline("\u270f\ufe0f Khud likhein (Custom)", b"dup_unit_Hours")],
            [Button.inline("\U0001f519 Wapas", b"dup_menu")]
        ])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(pattern=b"dup_preset_"))
async def dup_preset(event):
    await event.answer()  # ⚡ instant ack
    raw   = event.data.decode().replace("dup_preset_", "")
    hours = float(raw)
    data  = get_user_data(event.sender_id)
    data["settings"]["dup_expiry_hours"] = hours
    save_persistent_db()
    # Label for confirmation
    if hours < 1:
        lbl = f"{int(hours * 60)} minute"
    elif hours == 1:
        lbl = "1 ghanta"
    elif hours < 24:
        lbl = f"{int(hours)} ghante"
    elif hours == 24:
        lbl = "1 din"
    elif hours == 8760:
        lbl = "Hamesha"
    else:
        lbl = f"{int(hours // 24)} din"
    await event.answer(f"✅ Muddat set: {lbl}", alert=True)
    await dup_menu(event)

@bot.on(events.CallbackQuery(data=b"dup_expiry_reset"))
async def dup_expiry_reset(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    data["settings"]["dup_expiry_hours"] = 6
    save_persistent_db()
    await event.answer("⏱ Expiry reset to 6 hours!")
    await dup_menu(event)

@bot.on(events.CallbackQuery(pattern=b"dup_unit_"))
async def dup_unit_set(event):
    await event.answer()
    unit = event.data.decode().split("_")[-1]
    get_user_data(event.sender_id)["temp_data"]["dup_unit"] = unit
    get_user_data(event.sender_id)["step"] = "wait_dup_expiry"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit(f"⏱ Enter Expiry in **{unit}**:" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else ""), buttons=[Button.inline("🔙 Back", b"dup_set_expiry_flow")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"dup_add_whitelist"))
async def dup_add_whitelist(event):
    await event.answer()  # ⚡ instant ack
    get_user_data(event.sender_id)["step"] = "wait_dup_whitelist"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit("➕ Send words to whitelist (separate by comma)." + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "", buttons=[Button.inline("🔙 Back", b"dup_menu")])

@bot.on(events.CallbackQuery(data=b"dup_clear_whitelist"))
async def dup_clear_whitelist(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    data["settings"]["dup_whitelist_words"] = []
    save_persistent_db()
    await event.answer("🗑 Whitelist cleared!")
    try:
        await dup_menu(event)
    except errors.MessageNotModifiedError:
        pass

# SCHEDULER
@bot.on(events.CallbackQuery(data=b"sched_hub"))
async def sched_hub(event):
    """Issue #4: Combined Scheduler Hub — Time Window + Per-Day in one place."""
    await event.answer()
    uid   = event.sender_id
    data  = get_user_data(uid)
    sched = data.setdefault("scheduler", {})
    on        = sched.get("enabled", False)
    perday_on = sched.get("per_day_enabled", False)
    s_t  = sched.get("start", "09:00 AM")
    e_t  = sched.get("end",   "10:00 PM")
    import datetime as _dt
    try:
        from time_helper import ab_now
        now_t = ab_now(uid).strftime("%I:%M %p")
    except Exception:
        now_t = _dt.datetime.now().strftime("%I:%M %p")

    global_badge = "🟢 ON" if on else "🔴 OFF"
    perday_badge = "🟢 ON" if perday_on else "🔴 OFF"
    active_mode  = "Time Window" if on else ("Per-Day" if perday_on else "Off — Hamesha Forward")

    txt = (
        "⏰ **SCHEDULER**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🕐 **Abhi:** `{now_t}`\n"
        f"📌 **Active Mode:** {active_mode}\n\n"
        "─── Time Window ───\n"
        f"Status: **{global_badge}**\n"
        f"Timing: **{s_t}** → **{e_t}**\n"
        "_Sirf is time pe forwarding chalegi_\n\n"
        "─── Per-Day Schedule ───\n"
        f"Status: **{perday_badge}**\n"
        "_Har day ke liye alag timing set karo_\n"
    )
    btns = [
        [Button.inline(f"{'🔴 Time Window Band Karo' if on else '🟢 Time Window Chalu Karo'}", b"sched_toggle")],
        [Button.inline(f"⏱ Start: {s_t}", b"sched_set_start"),
         Button.inline(f"⏱ End: {e_t}",   b"sched_set_end")],
        [Button.inline(f"{'🔴 Per-Day Band Karo' if perday_on else '📅 Per-Day Chalu Karo'}", b"sched_toggle_perday")],
        [Button.inline("📅 Per-Day Timing Edit Karo", b"sched_per_day_menu")],
        [Button.inline("🏠 Main Menu", b"main_menu")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"sched_toggle_perday"))
async def sched_toggle_perday(event):
    """Toggle per-day scheduling from hub."""
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    sched = data.setdefault("scheduler", {})
    sched["per_day_enabled"] = not sched.get("per_day_enabled", False)
    if sched["per_day_enabled"] and not sched.get("per_day"):
        try:
            from scheduler import get_default_per_day_schedule
            sched["per_day"] = get_default_per_day_schedule()
        except Exception:
            pass
    save_persistent_db()
    await sched_hub(event)


@bot.on(events.CallbackQuery(data=b"sched_menu"))
async def sched_menu(event):
    await event.answer()
    uid   = event.sender_id
    data  = get_user_data(uid)
    sched = data["scheduler"]
    on    = sched.get("enabled", False)
    s_t   = sched.get("start", "09:00 AM")
    e_t   = sched.get("end",   "10:00 PM")

    # Check if currently in window
    import datetime as _dt
    try:
        from time_helper import ab_now
        now_t = ab_now(uid).strftime("%I:%M %p")
    except Exception:
        now_t = _dt.datetime.now().strftime("%I:%M %p")

    in_window_note = ""
    if on:
        in_window_note = "\n_Sirf is window ke andar messages forward honge_"

    try:
        await event.edit(
            "⏰ **SCHEDULER**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Status: **{'🟢 ON' if on else '🔴 OFF'}**\n"
            f"Window: **{s_t}** → **{e_t}**\n"
            f"Now: `{now_t}`"
            + in_window_note + "\n\n"
            "**Advanced:**\n"
            "  📅 Per-Day — alag days alag timing\n"
            "  📦 Queue Mode — off-time msgs queue mein rakhta hai\n",
            buttons=[
                [Button.inline(f"{'🔴 Band Karo' if on else '🟢 Chalu Karo'}", b"sched_toggle")],
                [Button.inline(f"⏱ Start: {s_t}", b"sched_set_start"),
                 Button.inline(f"⏱ End: {e_t}",   b"sched_set_end")],
                [Button.inline("📅 Per-Day Settings",  b"sched_per_day_menu")],
                [Button.inline("🔙 Filters",            b"advanced_filters"),
                 Button.inline("🏠 Menu",               b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"sched_toggle"))
async def sched_toggle(event):
    await event.answer()  # ⚡ instant ack
    data = get_user_data(event.sender_id)
    data["scheduler"]["enabled"] = not data["scheduler"]["enabled"]
    save_persistent_db()
    await sched_menu(event)

@bot.on(events.CallbackQuery(data=b"sched_set_start"))
async def sched_set_start(event):
    await event.answer()  # ⚡ instant ack
    get_user_data(event.sender_id)["step"] = "wait_sched_start"
    get_user_data(event.sender_id)["step_since"] = time.time()
    try:
        await event.edit("⏱ Send **Start Time** in 12-hour format.\nExample: `09:00 AM` or `02:30 PM`" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "", buttons=[Button.inline("🔙 Back", b"sched_menu")])
    except errors.MessageNotModifiedError:
        pass

@bot.on(events.CallbackQuery(data=b"sched_set_end"))
async def sched_set_end(event):
    await event.answer()  # ⚡ instant ack
    get_user_data(event.sender_id)["step"] = "wait_sched_end"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit("⏱ Send **End Time** in 12-hour format.\nExample: `10:30 PM`" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") + "", buttons=[Button.inline("🔙 Back", b"sched_menu")])
