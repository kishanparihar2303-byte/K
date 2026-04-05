"""
translate_menu.py — Message Translation Settings UI v2.0

Features:
  ✅ Global translation toggle + language picker
  ✅ Per-source language override
  ✅ Language search / filter (bahut zyada languages — scrollable)
  ✅ Engine status page (admin ke liye debug)
  ✅ Har screen pe Back button
  ✅ Clear + Save properly
  ✅ Translation test feature
  ✅ Proper error handling

Navigation:
  settings_menu → translate_menu
    ├── trans_global_toggle (toggle on/off)
    ├── trans_global_lang → trans_lang_page|global|PAGE
    │     └── trans_set_global|LANG
    ├── trans_per_source → trans_src_pick|SRC_ID
    │     └── trans_lang_page|src:SRC_ID|PAGE
    │           └── trans_set_src|SRC_ID|LANG
    ├── trans_test (test translation)
    └── trans_engine_status (debug)
"""

import time
from telethon import events, Button, errors
from config import bot
from database import get_user_data, save_persistent_db, user_sessions
from translator import (
    LANGUAGES, get_translation_settings,
    set_global_translate, set_source_translate,
    get_target_lang, get_engine_status
)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

_LANGS_PER_PAGE = 12   # 12 languages per page (6 rows x 2)

def _get_owner_footer() -> str:
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""


def _lang_page_buttons(callback_prefix: str, current: str = None, page: int = 0) -> list:
    """
    Paginated language buttons — 12 per page, 2 per row.
    callback_prefix: e.g. 'trans_set_global' or 'trans_set_src|src_id'
    """
    lang_list = list(LANGUAGES.items())
    total     = len(lang_list)
    pages     = max(1, (total + _LANGS_PER_PAGE - 1) // _LANGS_PER_PAGE)
    page      = max(0, min(page, pages - 1))

    start = page * _LANGS_PER_PAGE
    chunk = lang_list[start: start + _LANGS_PER_PAGE]

    buttons = []
    row = []
    for code, name in chunk:
        mark = "✅ " if code == current else ""
        row.append(Button.inline(
            f"{mark}{name}",
            f"{callback_prefix}|{code}".encode()
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    # Disable / Off button
    buttons.append([Button.inline(
        "❌ Translation Band Karo",
        f"{callback_prefix}|off".encode()
    )])

    # Pagination
    nav = []
    if page > 0:
        nav.append(Button.inline("◀️ Prev", f"trans_lang_page|{callback_prefix}|{page-1}".encode()))
    if page < pages - 1:
        nav.append(Button.inline("▶️ Next", f"trans_lang_page|{callback_prefix}|{page+1}".encode()))
    if nav:
        buttons.append(nav)

    # Back button — detect where to go
    if "|" in callback_prefix and "src" in callback_prefix:
        # Per-source context: back to source list
        buttons.append([Button.inline("🔙 Back", b"trans_per_source")])
    else:
        buttons.append([Button.inline("🔙 Back", b"translate_menu")])

    return buttons


# ─────────────────────────────────────────────────────────────────────────────
# MAIN TRANSLATION MENU
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"translate_menu"))
async def translate_menu(event):
    await event.answer()
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    trans    = data.get("translation", {})

    global_on   = trans.get("global_enabled", False)
    global_lang = trans.get("global_lang", "")
    per_source  = trans.get("per_source", {})

    # Status
    if global_on and global_lang:
        lang_name   = LANGUAGES.get(global_lang, global_lang)
        status_line = f"🟢 **ON** → **{lang_name}**"
    elif global_on:
        status_line = "🟡 **ON** — _(language select nahi ki)_"
    else:
        status_line = "🔴 **OFF**"

    # Per-source summary
    per_src_lines = ""
    if per_source:
        sources = data.get("sources", [])
        per_src_lines = f"\n**📌 Per-Source ({len(per_source)} configured):**\n"
        shown = 0
        for src_id, lang in per_source.items():
            if shown >= 3: break
            lang_disp = LANGUAGES.get(lang, lang)
            per_src_lines += f"  • `{src_id}` → **{lang_disp}**\n"
            shown += 1
        if len(per_source) > 3:
            per_src_lines += f"  _...aur {len(per_source)-3} more_\n"

    toggle_lbl = "🔴 Translation Band Karo" if global_on else "🟢 Translation Chalu Karo"

    await event.edit(
        "🌐 **TRANSLATION SETTINGS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Global: {status_line}\n"
        f"{per_src_lines}\n"
        "_Messages automatically translate honge destination language mein_",
        buttons=[
            [Button.inline(toggle_lbl,                   b"trans_global_toggle")],
            [Button.inline("🌐 Language Choose Karo",    b"trans_global_lang"),
             Button.inline("📌 Per-Source",              b"trans_per_source")],
            [Button.inline("🧪 Test Translation",        b"trans_test_menu")],
            [Button.inline("🔙 Settings",                b"settings_menu"),
             Button.inline("🏠 Menu",                    b"main_menu")],
        ]
    )

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL TOGGLE
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"trans_global_toggle"))
async def trans_global_toggle(event):
    await event.answer()
    user_id = event.sender_id
    data    = get_user_data(user_id)
    trans   = data.setdefault("translation", {})
    new_val = not trans.get("global_enabled", False)
    trans["global_enabled"] = new_val
    save_persistent_db()
    msg = "🟢 Translation chalu ho gaya!" if new_val else "🔴 Translation band ho gaya."
    await event.answer(msg, alert=False)
    await translate_menu(event)


# ─────────────────────────────────────────────────────────────────────────────
# GLOBAL LANGUAGE PICKER — paginated
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"trans_global_lang"))
async def trans_global_lang(event):
    await event.answer()
    user_id = event.sender_id
    current = get_user_data(user_id).get("translation", {}).get("global_lang")
    lang_list = list(LANGUAGES.items())
    pages     = max(1, (len(lang_list) + _LANGS_PER_PAGE - 1) // _LANGS_PER_PAGE)

    await event.edit(
        "🌐 **Global Language Chuniye**\n\n"
        "Saare sources ke messages is language mein translate honge.\n"
        f"_(Page 1/{pages})_",
        buttons=_lang_page_buttons("trans_set_global", current, page=0)
    )


@bot.on(events.CallbackQuery(pattern=b"trans_lang_page\\|(.+)"))
async def trans_lang_page(event):
    """Pagination handler — generic for all language pickers."""
    await event.answer()
    raw   = event.data.decode()
    parts = raw.split("|")
    # Format: trans_lang_page|PREFIX|PAGE
    # But PREFIX itself might contain |
    # Last part is always page number
    page  = int(parts[-1])
    # Reconstruct prefix (everything between first | and last |)
    prefix = "|".join(parts[1:-1])

    # Determine context text
    if "src:" in prefix:
        src_id   = prefix.split("src:")[1].split("|")[0]
        ctx_text = f"📌 **Source ke liye Language:**\n`{src_id}`\n\n"
        current  = get_user_data(event.sender_id).get("translation", {}).get("per_source", {}).get(src_id)
    else:
        ctx_text = "🌐 **Global Language Chuniye**\n\n"
        current  = get_user_data(event.sender_id).get("translation", {}).get("global_lang")

    lang_list = list(LANGUAGES.items())
    pages     = max(1, (len(lang_list) + _LANGS_PER_PAGE - 1) // _LANGS_PER_PAGE)

    await event.edit(
        ctx_text + f"_(Page {page+1}/{pages})_",
        buttons=_lang_page_buttons(prefix, current, page=page)
    )


@bot.on(events.CallbackQuery(pattern=b"trans_set_global\\|(.+)"))
async def trans_set_global(event):
    await event.answer()
    user_id = event.sender_id
    lang    = event.data.decode().split("|")[1]
    if lang == "off":
        set_global_translate(user_id, False)
        save_persistent_db()
        await event.answer("❌ Global translation disabled", alert=False)
    else:
        set_global_translate(user_id, True, lang)
        save_persistent_db()
        await event.answer(f"✅ {LANGUAGES.get(lang, lang)} set!", alert=False)
    await translate_menu(event)


# ─────────────────────────────────────────────────────────────────────────────
# PER-SOURCE LANGUAGE
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"trans_per_source"))
async def trans_per_source(event):
    await event.answer()
    user_id = event.sender_id
    data    = get_user_data(user_id)
    sources = data.get("sources", [])

    if not sources:
        return await event.edit(
            "📌 **Per-Source Translation**\n\n"
            "❌ Koi sources add nahi hain!\n\n"
            "Pehle Sources menu se channels/groups add karo.",
            buttons=[[Button.inline("🔙 Back", b"translate_menu")]]
        )

    trans   = data.get("translation", {})
    per_src = trans.get("per_source", {})

    # ROOT CAUSE FIX: sources is list of plain IDs (str/int), not dicts
    # Fetch display names using channel_names cache
    channel_names = data.get("channel_names", {})
    buttons = []
    for src in sources[:15]:
        # src is int/str/channel_id directly
        src_id   = str(src)
        # Try to get display name from cache
        src_name = channel_names.get(src_id) or channel_names.get(src) or src_id
        if len(str(src_name)) > 22:
            src_name = str(src_name)[:22] + "…"
        cur_lang = per_src.get(src_id)
        if cur_lang:
            lang_label = LANGUAGES.get(cur_lang, cur_lang)
            label = f"✅ {src_name} → {lang_label}"
        else:
            label = f"📌 {src_name}"
        # Callback data max 64 bytes — use src_id (numeric ID)
        cb = f"trans_src_pick|{src_id}".encode()
        if len(cb) > 60:
            cb = f"trans_src_pick|{src_id[-50:]}".encode()
        buttons.append([Button.inline(label, cb)])

    if per_src:
        buttons.append([Button.inline("🗑 Sab Per-Source Clear Karo", b"trans_clear_all_src")])
    buttons.append([Button.inline("🔙 Back", b"translate_menu")])

    configured = len([s for s in sources if str(s) in per_src])  # sources = plain IDs
    await event.edit(
        f"📌 **Per-Source Translation**\n\n"
        f"Sources: {len(sources)} total  ·  {configured} configured\n\n"
        f"Kis source ke liye language set karni hai?",
        buttons=buttons
    )


@bot.on(events.CallbackQuery(pattern=b"trans_src_pick\\|(.+)"))
async def trans_src_pick(event):
    await event.answer()
    user_id  = event.sender_id
    src_id   = event.data.decode().split("|", 1)[1]
    current  = get_translation_settings(user_id).get("per_source", {}).get(src_id)
    lang_list = list(LANGUAGES.items())
    pages     = max(1, (len(lang_list) + _LANGS_PER_PAGE - 1) // _LANGS_PER_PAGE)

    # Find source display name from channel_names cache
    data_tmp  = get_user_data(user_id)
    ch_names  = data_tmp.get("channel_names", {})
    src_name  = ch_names.get(src_id) or ch_names.get(src_id.lstrip("-100")) or src_id
    src_name  = str(src_name)[:25]

    await event.edit(
        f"📌 **Source Language: {src_name}**\n\n"
        f"Is source ke messages kis language mein translate hon?\n"
        f"_(Page 1/{pages})_",
        buttons=_lang_page_buttons(f"trans_set_src|{src_id}", current, page=0)
    )


@bot.on(events.CallbackQuery(pattern=b"trans_set_src\\|(.+)\\|(.+)"))
async def trans_set_src(event):
    await event.answer()
    user_id = event.sender_id
    parts   = event.data.decode().split("|")
    src_id  = parts[1]
    lang    = parts[2]

    if lang == "off":
        set_source_translate(user_id, src_id, None)
        save_persistent_db()
        await event.answer("✅ Translation disabled for this source")
    else:
        set_source_translate(user_id, src_id, lang)
        save_persistent_db()
        await event.answer(f"✅ {LANGUAGES.get(lang, lang)} set!")
    await trans_per_source(event)


@bot.on(events.CallbackQuery(data=b"trans_clear_all_src"))
async def trans_clear_all_src(event):
    await event.answer()
    user_id = event.sender_id
    data    = get_user_data(user_id)
    data.setdefault("translation", {})["per_source"] = {}
    save_persistent_db()
    await event.answer("🗑 Sab per-source settings clear!", alert=False)
    await trans_per_source(event)


# ─────────────────────────────────────────────────────────────────────────────
# TEST TRANSLATION
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"trans_test_menu"))
async def trans_test_menu(event):
    await event.answer()
    user_id  = event.sender_id
    data     = get_user_data(user_id)
    trans    = data.get("translation", {})
    global_on   = trans.get("global_enabled", False)
    global_lang = trans.get("global_lang", "")

    if not global_on or not global_lang:
        return await event.edit(
            "🧪 **Test Translation**\n\n"
            "❌ Pehle global translation ON karo aur language set karo!\n\n"
            "_Settings → Translation → Global Language_",
            buttons=[[Button.inline("🌐 Language Set Karo", b"trans_global_lang")],
                     [Button.inline("🔙 Back",              b"translate_menu")]]
        )

    from database import get_user_data as gud
    d = gud(user_id)
    d["step"] = "trans_test_input"

    await event.edit(
        f"🧪 **Test Translation**\n\n"
        f"Current language: **{LANGUAGES.get(global_lang, global_lang)}**\n\n"
        f"Koi bhi text bhejo — main translate karke dikhaunga:\n\n"
        f"_/cancel = wapas jao_",
        buttons=[[Button.inline("🔙 Back", b"translate_menu"),
                  Button.inline("❌ Cancel", b"translate_menu")]]
    )


@bot.on(events.NewMessage())
async def trans_test_handler(event):
    if not event.is_private: return
    from database import get_user_data, save_persistent_db
    uid  = event.sender_id
    d    = get_user_data(uid)
    if d.get("step") != "trans_test_input": return
    text = event.raw_text.strip()

    if text.lower() in ("/cancel", "cancel"):
        d["step"] = None; save_persistent_db()
        return await event.respond("❌ Cancelled.",
            buttons=[[Button.inline("🌐 Translation", b"translate_menu")]])

    trans = d.get("translation", {})
    lang  = trans.get("global_lang", "")
    if not lang:
        d["step"] = None; save_persistent_db()
        return await event.respond("❌ Language set nahi hai.",
            buttons=[[Button.inline("🌐 Settings", b"translate_menu")]])

    await event.respond("⏳ Translating...")
    try:
        from translator import translate_text
        result = await translate_text(text, lang, uid)
        d["step"] = None; save_persistent_db()
        if result:
            await event.respond(
                f"🧪 **Translation Test**\n\n"
                f"**Original:**\n{text}\n\n"
                f"**{LANGUAGES.get(lang, lang)}:**\n{result}",
                buttons=[
                    [Button.inline("🧪 Aur Test Karo", b"trans_test_menu")],
                    [Button.inline("🌐 Back",           b"translate_menu")]
                ]
            )
        else:
            await event.respond(
                "❌ **Translation fail ho gaya.**\n\nEngines temporarily unavailable hain — thodi der baad try karo.",
                buttons=[[Button.inline("🔄 Retry",  b"trans_test_menu"),
                          Button.inline("🌐 Back",   b"translate_menu")]]
            )
    except Exception as e:
        d["step"] = None; save_persistent_db()
        await event.respond(
            f"❌ **Error:** `{str(e)[:100]}`",
            buttons=[[Button.inline("🔄 Retry",  b"trans_test_menu"),
                      Button.inline("🌐 Back",   b"translate_menu")]]
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENGINE STATUS
# ─────────────────────────────────────────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"trans_engine_status"))
async def trans_engine_status(event):
    await event.answer()
    status = get_engine_status()
    try:
        await event.edit(
            f"📡 **Translation Engine Status**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{status}\n\n"
            f"**Free Engines rotate karte hain:**\n"
            f"• Google Free (unlimited, multi-endpoint)\n"
            f"• deep-translator (Google + MyMemory)\n"
            f"• MyMemory Free (1k req/day)\n"
            f"• LibreTranslate (open source)\n\n"
            f"_Ek engine fail ho toh automatically next try hota hai._",
            buttons=[
                [Button.inline("🔄 Refresh",  b"trans_engine_status")],
                [Button.inline("🔙 Back",     b"translate_menu")]
            ]
        )
    except errors.MessageNotModifiedError:
        pass   # Refresh kiya lekin status same hai — silently ignore
