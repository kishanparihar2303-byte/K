# lang.py — Bilingual Support: Hinglish (hi) + English (en)
# Usage:
#   from lang import t, get_lang, set_lang
#   t(user_id, "key")           → string in user's language
#   t(user_id, "key", name="X") → with format args

import time
from database import get_user_data, save_persistent_db

SUPPORTED_LANGS = {"hi": "🇮🇳 हिंदी", "en": "🇬🇧 English"}
DEFAULT_LANG = "hi"


def _get_owner_footer() -> str:
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""


def get_lang(user_id: int) -> str:
    try:
        return get_user_data(user_id).get("language", DEFAULT_LANG)
    except Exception:
        return DEFAULT_LANG


def set_lang(user_id: int, lang: str):
    if lang in SUPPORTED_LANGS:
        get_user_data(user_id)["language"] = lang
        save_persistent_db()


def t(user_id: int, key: str, **kwargs) -> str:
    lang = get_lang(user_id)
    strings = STRINGS.get(lang, STRINGS["en"])
    text = strings.get(key) or STRINGS["en"].get(key, key)
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text


# ══════════════════════════════════════════
# ALL STRING TRANSLATIONS
# ══════════════════════════════════════════

STRINGS = {

# ──────────────────────────────────────────
# HINGLISH — Hindi words, Roman script
# ──────────────────────────────────────────
"hi": {

    # ── Core Buttons ──────────────────────────────────────────────────────────
    "btn_login":           "📱 Login — Telegram Account Connect Karo",
    "btn_logout":          "🔓 Logout Karo",
    "btn_add_src":         "➕ Source Add Karo",
    "btn_add_dest":        "📤 Destination Add Karo",
    "btn_del_src":         "🗑 Source Hatao",
    "btn_del_dest":        "🗑 Destination Hatao",
    "btn_start_fwd":       "🟢 Forwarding Chalu Karo",
    "btn_stop_fwd":        "🔴 Forwarding Band Karo",
    "btn_dashboard":       "📊 Dashboard",
    "btn_settings":        "⚙️ Global Settings",
    "btn_src_config":      "📍 Src Config",
    "btn_replacement":     "🔄 Replacement",
    "btn_filters":         "🧠 Filters",
    "btn_scheduler":       "⏰ Scheduler",
    "btn_link_blocker":    "🚫 Link Blocker",
    "btn_start_msg":       "✏️ Start Message",
    "btn_end_msg":         "✏️ End Message",
    "btn_del_start_msg":   "🗑 Start Msg Hatao",
    "btn_del_end_msg":     "🗑 End Msg Hatao",
    "btn_backup":          "💾 Backup/Restore",
    "btn_refer":           "🎁 Refer & Earn",
    "btn_premium":         "💎 Premium Info",
    "btn_buy_premium":     "💳 Premium Kharido",
    "btn_help":            "❓ Help & Guide",
    "btn_adv_mode":        "⚡ Advanced Mode",
    "btn_beg_mode":        "🧑 Beginner Mode",
    "btn_main_menu":       "🏠 Main Menu",
    "btn_back":            "🔙 Wapas Jao",
    "btn_cancel":          "❌ Cancel",
    "btn_confirm":         "✅ Confirm Karo",
    "btn_language":        "🌐 Bhasha / Language",
    "btn_lang_hi":         "🇮🇳 हिंदी (Hinglish)",
    "btn_lang_en":         "🇬🇧 English",
    "btn_try_again":       "🔁 Dobara Try Karo",
    "btn_contact_admin":   "📞 Admin Se Baat Karo",
    "btn_view_list":       "📋 List Dekho",
    "btn_pinned":          "📌 Pinned Chats Se Chuno",
    "btn_manual":          "⌨️ ID / Link / Username Daalo",
    "btn_refresh":         "🔄 Refresh Karo",
    "btn_save":            "💾 Save Karo",
    "btn_delete":          "🗑 Delete Karo",
    "btn_enable":          "✅ Enable",
    "btn_disable":         "❌ Disable",

    # ── Welcome / Start ────────────────────────────────────────────────────────
    "welcome_new":
        "👋 **Auto Forwarder Bot mein Aapka Swagat Hai!**\n\n"
        "✅ Aasan  |  🔒 Safe  |  ⚡ Bina Admin Rights ke\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "**Shuru Karo — Sirf 3 Steps:**\n\n"
        "1️⃣  **Login** — Apna Telegram account connect karo\n"
        "2️⃣  **Source** — Jis channel se copy karna hai\n"
        "3️⃣  **Destination** — Jahan forward karna hai\n\n"
        "▶️  Neeche **Login** button dabao!",

    "welcome_back":
        "🏠 **MAIN MENU**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "{state_line}\n"
        "{info_line}",

    "state_setup":    "🆕 Setup abhi baaki hai — neeche se shuru karo",
    "state_running":  "⚡ **Forwarding chal rahi hai**{activity}",
    "state_stopped":  "⏹ Forwarding band hai — Start karo!",
    "activity_today": "  •  Aaj: `{fwd}↑` `{blk}✗`",

    # ── Main Menu Title ────────────────────────────────────────────────────────
    "main_menu_title":
        "🏠 **MAIN MENU**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Forwarding: {status}  |  Mode: {mode}",
    "fwd_active":     "🟢 Chalu",
    "fwd_stopped":    "🔴 Band",
    "mode_beginner":  "Beginner",
    "mode_advanced":  "Advanced",

    # ── Login Flow ────────────────────────────────────────────────────────────
    "login_required":   "⚠️ Pehle Login Karo!",
    "login_step1":
        "📱 **LOGIN — Step 1 of 3**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Apna phone number bhejo:**\n\n"
        "✅ Format: `+91XXXXXXXXXX`\n"
        "📌 Example: `+91XXXXXXXXXX`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ **OTP kahan milega?**\n"
        "Telegram App → Settings → Devices → Active Sessions\n"
        "Ya aapke number par SMS\n\n"
        "🔒 _OTP kisi ko mat batao!_\n"
        "⏳ _15 min timeout — /cancel se band karo_",

    "login_otp_sent":
        "✅ **OTP Bhej Diya Gaya!**\n\n"
        "📲 **OTP kahan dekho:**\n"
        "• Telegram App → Settings → Devices\n"
        "• Ya registered number par SMS\n\n"
        "✏️ **Format mein bhejo:** `HELLO12345`\n"
        "_(Pehle HELLO likho, phir code)_\n\n"
        "⏳ _OTP 2 minute mein expire hoga_",

    "login_step_pass":
        "🔑 **2FA Password Daalo**\n\n"
        "Tumhare account mein Two-Factor Authentication hai.\n"
        "Apna Telegram password daalo.\n\n"
        "💡 _Settings → Privacy & Security → Two-Step Verification_",

    "login_success":
        "🎉 **Login Ho Gaya!**\n\n"
        "✅ Tumhara Telegram account connect ho gaya.\n\n"
        "**Agle Steps:**\n"
        "1️⃣ Source add karo (jis channel se copy karna hai)\n"
        "2️⃣ Destination add karo (jahan bhejana hai)\n"
        "3️⃣ Forwarding Start karo ✅",

    "login_wrong_phone":
        "❌ **Phone Number Galat Hai!**\n\n"
        "Sahi format: `+91XXXXXXXXXX`\n"
        "Example: `+91XXXXXXXXXX`\n\n"
        "Country code zaroor daalo.",

    "login_wrong_otp":
        "❌ **OTP Galat Hai!**\n\n"
        "Sahi code daalo.\n"
        "Naya OTP lene ke liye /start karo.",

    "login_otp_timeout":
        "⏰ **OTP Timeout!**\n\n"
        "OTP 2 minute mein enter nahi kiya.\n"
        "Dobara login karne ke liye button dabao.",

    "login_2fa_wrong":
        "❌ **Password Galat Hai!**\n\n"
        "Apna Telegram 2FA password check karo.\n"
        "Bhool gaye? Telegram app → Settings → Privacy → Two-Step Verification",

    # ── Logout ────────────────────────────────────────────────────────────────
    "logout_confirm":
        "🔓 **Logout Karna Chahte Ho?**\n\n"
        "⚠️ Logout karne par:\n"
        "• Forwarding turant band ho jaayegi\n"
        "• Sources aur destinations saved rahenge\n\n"
        "Kya aap pakka logout karna chahte ho?",
    "btn_logout_yes":  "✅ Haan, Logout Karo",
    "btn_logout_no":   "❌ Nahi, Wapas Jao",
    "logged_out":
        "✅ **Logout Ho Gaye!**\n\n"
        "Forwarding band ho gayi.\n"
        "Wapas shuru karne ke liye Login karo.",

    # ── Forwarding Start/Stop ──────────────────────────────────────────────────
    "fwd_started":
        "🟢 **FORWARDING SHURU HO GAYI!**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📥 Sources: `{srcs}`  →  📤 Destinations: `{dests}`\n\n"
        "✅ Ab source se aane wale messages automatically\n"
        "destination mein forward honge.\n\n"
        "💡 _Bot band mat karo — 24/7 chalane do!_",

    "fwd_stopped_msg":
        "⏹ **FORWARDING BAND HO GAYI**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "{today_line}"
        "_Dobara start karne ke liye neeche dabao_",

    "fwd_today_line":  "📊 Aaj forwarded: `{count}` messages\n\n",
    "fwd_no_src":      "❌ Pehle Source add karo!\n\nSource = Jis channel se copy karna hai.",
    "fwd_no_dest":     "❌ Pehle Destination add karo!\n\nDestination = Jahan message bhejana hai.",
    "fwd_no_src_dest": "❌ Pehle Source aur Destination dono add karo!\n\nSirf 2 steps mein setup ready.",
    "fwd_no_session":  "⚠️ Login required!\n\nPehle apna Telegram account connect karo.",

    # ── Dashboard ─────────────────────────────────────────────────────────────
    "dashboard_title":     "📊 **DASHBOARD**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    "dashboard_status":    "⚡ **Status:** {status}",
    "dashboard_account":   "👤 **Account:** {plan}",
    "dashboard_forwarded":  "📨 **Forward Hue:** `{count}` messages",
    "dashboard_dup":       "♻️ **Duplicate Blocked:** `{count}`",
    "dashboard_sources":   "📥 **Sources:** `{count}`",
    "dashboard_dests":     "📤 **Destinations:** `{count}`",
    "dashboard_filters":   "🛡 **Active Filters:**\n  • {filters}",
    "dashboard_scheduler": "⏰ **Scheduler:** {status} ({start} → {end})",
    "dashboard_no_filter": "Koi nahi",
    "btn_full_report":     "📜 Full Report Dekho",
    "plan_premium":        "💎 Premium ({days}d)",
    "plan_trial":          "🎁 Trial ({days}d)",
    "plan_free":           "🆓 Free User",
    "plan_lifetime":       "💎 Premium (Lifetime ♾️)",

    # ── Source / Destination ───────────────────────────────────────────────────
    "add_src_title":
        "➕ **Source Add Karo**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Source = Jis channel ya group se message copy hoga.\n\n"
        "📌 **Kaise add karein?**",

    "add_dest_title":
        "📤 **Destination Add Karo**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Destination = Jahan message bheja jaayega.\n"
        "⚠️ Us channel mein tumhara account **Admin** hona chahiye!\n\n"
        "📌 **Kaise add karein?**",

    "send_src_prompt":
        "📥 **Source ka Link, ID ya Username bhejo:**\n\n"
        "✅ Examples:\n"
        "  • `@mychannel`\n"
        "  • `https://t.me/mychannel`\n"
        "  • `-100xxxxxxxxx` (channel ID)\n\n"
        "💡 _Private channel ke liye invite link bhi kaam karta hai_",

    "send_dest_prompt":
        "📤 **Destination ka Link, ID ya Username bhejo:**\n\n"
        "✅ Examples:\n"
        "  • `@mychannnel`\n"
        "  • `-100xxxxxxxxx` (channel ID)\n\n"
        "⚠️ _Tumhara account us channel ka Admin hona chahiye!_",

    "src_added":     "✅ **Source Add Ho Gaya!**\n\n📥 `{name}` ab forward karega.",
    "dest_added":    "✅ **Destination Add Ho Gayi!**\n\n📤 `{name}` mein messages jaayenge.",
    "src_deleted":   "🗑 Source hat gaya: `{name}`",
    "dest_deleted":  "🗑 Destination hat gayi: `{name}`",
    "src_exists":    "⚠️ Yeh source pehle se add hai!",
    "dest_exists":   "⚠️ Yeh destination pehle se add hai!",
    "src_limit":     "❌ Source limit poori ho gayi!\n\n💎 Premium mein zyada sources milte hain.",
    "dest_limit":    "❌ Destination limit poori ho gayi!\n\n💎 Premium mein zyada destinations milte hain.",
    "src_not_found": "❌ Channel nahi mila!\n\nCheck karo:\n• Link sahi hai?\n• Channel public hai?\n• Bot se joined hai?",
    "no_sources":    "📭 **Koi Source Nahi Hai**\n\nPehle ek source add karo:\n➕ Source Add Karo button dabao.",
    "no_dests":      "📭 **Koi Destination Nahi Hai**\n\nPehle ek destination add karo:\n📤 Destination Add Karo button dabao.",
    "src_list_title": "📋 **Source List**\nDekhne ya delete karne ke liye chuno:",
    "dest_list_title": "📋 **Destination List**\nDekhne ya delete karne ke liye chuno:",

    # ── Settings ──────────────────────────────────────────────────────────────
    "settings_title":
        "⚙️ **Global Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ Ye settings **SABHI** sources par laagu hoti hain\n\n"
        "📌 Sirf ek source ke liye alag rules → **Src Config**\n\n"
        "📨 **Kya Forward Karna Hai:**\n"
        "🟢 = Forward karega  🔴 = Skip karega",
    "settings_mods":    "─── ✏️ Modifications ───",
    "settings_dup":     "─── ♻️ Duplicate Filter ───",
    "settings_delay":   "⏱ Delay: {delay}s — Tap to Change",
    "btn_help_settings": "❓ Settings Samjho",

    # ── Source Config ──────────────────────────────────────────────────────────
    "src_config_title":
        "📍 **Source Config**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Har source ke liye **alag rules** set karo.\n\n"
        "⚙️ = Custom rules hain  |  📌 = Global settings\n\n"
        "Source chuno:",
    "no_src_config":    "❌ Koi source nahi hai.\n\nPehle source add karo.",
    "btn_link_mode":    "🔗 Link Mode Badlo",
    "btn_prefix":       "✏️ Upar ka Text (Prefix)",
    "btn_suffix":       "✏️ Neeche ka Text (Suffix)",
    "btn_per_dest":     "📤 Per-Destination Rules →",
    "btn_reset_src":    "🗑 Sab Reset Karo",
    "btn_src_list":     "🔙 Sources List",
    "has_rules_mark":   "⚙️",
    "no_rules_mark":    "📌",

    # ── Per-Dest Rules ─────────────────────────────────────────────────────────
    "per_dest_title":
        "📤 **Per-Destination Rules**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Source: **{src_name}**\n\n"
        "Har destination ke liye **alag rules** set kar sakte ho.\n"
        "Jaise Dest A ko sirf photo, Dest B ko sirf text.\n\n"
        "⚙️ = Custom rules  |  🌐 = Source default\n\n"
        "Destination chuno:",
    "no_dests_config":  "❌ Pehle destination add karo!",
    "btn_help_per_dest":"❓ Per-Dest Rules Kya Hain?",
    "custom_mark":      "⚙️",
    "default_mark":     "🌐",

    # ── Filters ───────────────────────────────────────────────────────────────
    "filters_title":
        "🧠 **Advanced Tools**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "♻️ **Duplicate Filter** — Same message dobara nahi jaata\n"
        "🔍 **Keyword Filter** — Specific words filter karo\n"
        "🚫 **Link Blocker** — Links block/limit karo",
    "btn_dup_filter":   "♻️ Duplicate Filter",
    "btn_kw_filter":    "🔍 Keyword Filter",

    # ── Replacement ───────────────────────────────────────────────────────────
    "replace_title":
        "🔄 **Replacement Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Kisi bhi text ya link ko automatically replace karo.\n"
        "Example: `@purana` → `@naya`",
    "btn_add_repl":     "➕ Replacement Add Karo",
    "btn_list_repl":    "📋 List Dekho",
    "btn_clear_repl":   "🗑 Sab Clear Karo",
    "no_replacements":  "❌ Koi replacement nahi hai.\n\nNaya add karne ke liye button dabao.",

    # ── Scheduler ─────────────────────────────────────────────────────────────
    "scheduler_title":
        "⏰ **Scheduler**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Forwarding ko ek specific time par chalu/band karo.\n"
        "Jaise raat 10pm se subah 8am tak hi forward ho.",
    "sched_enabled":    "✅ Chalu Hai ({start} → {end})",
    "sched_disabled":   "❌ Band Hai",

    # ── Backup ────────────────────────────────────────────────────────────────
    "backup_title":
        "💾 **Backup / Restore**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Apni sabhi settings aur sources/destinations ka backup lo\n"
        "ya purani backup se restore karo.",
    "btn_export":       "📤 Backup Export Karo",
    "btn_import":       "📥 Backup Import Karo",

    # ── Status ────────────────────────────────────────────────────────────────
    "status_title":     "📊 **Bot Status**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    "status_session":   "🔌 Session: {val}",
    "status_fwd":       "▶️ Forwarding: {val}",
    "status_plan":      "💎 Plan: {val}",
    "status_src_dest":  "📺 Sources: `{src}` | 📤 Destinations: `{dest}`",
    "status_connected": "✅ Connected",
    "status_disconnected": "❌ Disconnected",
    "status_running":   "✅ Chal Raha Hai",
    "status_stopped":   "❌ Band Hai",

    # ── Help ──────────────────────────────────────────────────────────────────
    "help_title":       "❓ **Help & Guide**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nKya jaanna chahte ho?",
    "btn_help_start":   "🚀 Shuru Kaise Karein",
    "btn_help_settings_h": "⚙️ Settings Samjho",
    "btn_help_src":     "📍 Src Config Samjho",
    "btn_help_perdest": "📤 Per-Dest Rules Samjho",
    "btn_help_advanced":"🔧 Advanced Features",
    "btn_help_problems":"❓ Common Problems",
    "help_commands":
        "📋 **Commands:**\n\n"
        "• /start — Bot shuru karo\n"
        "• /status — Status dekho\n"
        "• /cancel — Koi bhi step cancel karo\n"
        "• /help — Help guide\n"
        "• /menu — Main menu\n"
        "• /rules — Bot ke rules",

    # ── Language Menu ─────────────────────────────────────────────────────────
    "lang_title":
        "🌐 **Bhasha Chuniye**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Abhi: **{current}**\n\n"
        "Apni pasand ki bhasha chuniye:",
    "lang_changed":     "✅ Bhasha Hindi (Hinglish) ho gayi!",

    # ── Premium ───────────────────────────────────────────────────────────────
    "premium_required":
        "💎 **Yeh Feature Premium Mein Hai!**\n\n"
        "Is feature ko use karne ke liye Premium plan kharido.\n\n"
        "Premium Benefits:\n"
        "• Zyada Sources & Destinations\n"
        "• Advanced Filters\n"
        "• Priority Support\n"
        "• Aur bahut kuch!",
    "btn_buy_now":      "💳 Abhi Kharido",

    # ── Errors & Generic ──────────────────────────────────────────────────────
    "error_generic":    "❌ Kuch galat ho gaya!\n\nDobara try karo ya /cancel karo.",
    "error_not_found":  "❌ Nahi mila! Check karo aur dobara try karo.",
    "error_permission": "🚫 Permission nahi hai!",
    "login_first":      "⚠️ Pehle Login Karo!\n\nBot use karne ke liye account connect karo.",
    "banned_msg":       "🚫 Tumhe bot se ban kar diya gaya hai.",
    "maintenance_msg":
        "🔧 **Bot Maintenance Par Hai**\n\n"
        "Abhi bot maintenance chal rahi hai.\n"
        "Thodi der mein wapas aao!",
    "new_reg_closed":   "🚫 Abhi naye users ke liye registration band hai.\n\nBaad mein try karo!",

    # ── Contact / Support ─────────────────────────────────────────────────────
    "contact_prompt":
        "📞 **Admin ko Message Bhejo**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Apna sawal ya problem type karo 👇\n\n"
        "_Ya /cancel se wapas jao_",
    "contact_sent":
        "✅ **Message Admin Tak Pahunch Gaya!**\n\n"
        "Admin jald se jald reply karenge 🙏",
    "contact_failed":   "❌ Message deliver nahi hua.\nAdmin se directly contact karo.",
    "empty_msg":        "❌ Khali message nahi bhej sakte!",

    # ── Timeout / Cancel ──────────────────────────────────────────────────────
    "timeout_msg":
        "⏰ **Timeout!**\n\n"
        "15 minute se koi jawab nahi diya.\n"
        "Jo chal raha tha automatically cancel ho gaya.\n\n"
        "Wapas shuru karne ke liye button dabao.",
    "cancelled_msg":    "✅ **Cancel Ho Gaya!**\n\nMain menu par wapas aa gaye.",
    "nothing_to_cancel": "ℹ️ Koi active step nahi tha cancel karne ke liye.",

    # ── Session Errors ────────────────────────────────────────────────────────
    "session_error":
        "⚠️ **Session Error!**\n\n"
        "Tumhara Telegram session invalid ho gaya.\n"
        "Reason: Session ek saath 2 jagah use hua.\n\n"
        "✅ Solution: Dobara login karo.",
    "session_revoked":
        "⚠️ **Session Expire Ho Gaya!**\n\n"
        "Tumhari forwarding BAND ho gayi.\n"
        "Reason: Telegram ne session revoke kar diya.\n\n"
        "✅ Solution: Dobara login karo.",
    "btn_relogin":      "🔁 Dobara Login Karo",

    # ── Misc ─────────────────────────────────────────────────────────────────
    "not_found":        "❌ Nahi mila!",
    "confirm_yes":      "✅ Haan",
    "confirm_no":       "❌ Nahi",
    "prev":             "⬅️ Pehle",
    "next":             "Aage ➡️",
    "owner_tag":        "",
    "random_text_reply":
        "🤔 Main samajh nahi paya!\n\n"
        "📋 Menu ke liye neeche button dabao:",
    "menu_label":       "🏠 Main Menu",
    "stop_btn":         "🔴 Band Karo",
    "start_btn":        "🟢 Chalu Karo",
    "dashboard_btn":    "📊 Dashboard",
},


# ──────────────────────────────────────────
# ENGLISH
# ──────────────────────────────────────────
"en": {

    # ── Core Buttons ──────────────────────────────────────────────────────────
    "btn_login":           "📱 Login — Connect Your Telegram Account",
    "btn_logout":          "🔓 Logout",
    "btn_add_src":         "➕ Add Source",
    "btn_add_dest":        "📤 Add Destination",
    "btn_del_src":         "🗑 Remove Source",
    "btn_del_dest":        "🗑 Remove Destination",
    "btn_start_fwd":       "🟢 Start Forwarding",
    "btn_stop_fwd":        "🔴 Stop Forwarding",
    "btn_dashboard":       "📊 Dashboard",
    "btn_settings":        "⚙️ Global Settings",
    "btn_src_config":      "📍 Source Config",
    "btn_replacement":     "🔄 Replacement",
    "btn_filters":         "🧠 Filters",
    "btn_scheduler":       "⏰ Scheduler",
    "btn_link_blocker":    "🚫 Link Blocker",
    "btn_start_msg":       "✏️ Start Message",
    "btn_end_msg":         "✏️ End Message",
    "btn_del_start_msg":   "🗑 Remove Start Msg",
    "btn_del_end_msg":     "🗑 Remove End Msg",
    "btn_backup":          "💾 Backup/Restore",
    "btn_refer":           "🎁 Refer & Earn",
    "btn_premium":         "💎 Premium Info",
    "btn_buy_premium":     "💳 Buy Premium",
    "btn_help":            "❓ Help & Guide",
    "btn_adv_mode":        "⚡ Advanced Mode",
    "btn_beg_mode":        "🧑 Beginner Mode",
    "btn_main_menu":       "🏠 Main Menu",
    "btn_back":            "🔙 Go Back",
    "btn_cancel":          "❌ Cancel",
    "btn_confirm":         "✅ Confirm",
    "btn_language":        "🌐 भाषा / Language",
    "btn_lang_hi":         "🇮🇳 हिंदी (Hinglish)",
    "btn_lang_en":         "🇬🇧 English",
    "btn_try_again":       "🔁 Try Again",
    "btn_contact_admin":   "📞 Contact Admin",
    "btn_view_list":       "📋 View List",
    "btn_pinned":          "📌 Choose from Pinned Chats",
    "btn_manual":          "⌨️ Enter ID / Link / Username",
    "btn_refresh":         "🔄 Refresh",
    "btn_save":            "💾 Save",
    "btn_delete":          "🗑 Delete",
    "btn_enable":          "✅ Enable",
    "btn_disable":         "❌ Disable",

    # ── Welcome / Start ────────────────────────────────────────────────────────
    "welcome_new":
        "👋 **Welcome to Auto Forwarder Bot!**\n\n"
        "✅ Easy to Use  |  🔒 Safe  |  ⚡ No Admin Rights Required\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "**Get Started — Only 3 Steps:**\n\n"
        "1️⃣  **Login** — Connect your Telegram account\n"
        "2️⃣  **Source** — The channel to copy from\n"
        "3️⃣  **Destination** — Where to forward messages\n\n"
        "▶️  Tap the **Login** button below!",

    "welcome_back":
        "🏠 **MAIN MENU**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "{state_line}\n"
        "{info_line}",

    "state_setup":    "🆕 Setup incomplete — start from below",
    "state_running":  "⚡ **Forwarding is active**{activity}",
    "state_stopped":  "⏹ Forwarding stopped — press Start!",
    "activity_today": "  •  Today: `{fwd}↑` `{blk}✗`",

    # ── Main Menu Title ────────────────────────────────────────────────────────
    "main_menu_title":
        "🏠 **MAIN MENU**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚡ Forwarding: {status}  |  Mode: {mode}",
    "fwd_active":     "🟢 Active",
    "fwd_stopped":    "🔴 Stopped",
    "mode_beginner":  "Beginner",
    "mode_advanced":  "Advanced",

    # ── Login Flow ────────────────────────────────────────────────────────────
    "login_required":   "⚠️ Please Login First!",
    "login_step1":
        "📱 **LOGIN — Step 1 of 3**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "**Send your phone number:**\n\n"
        "✅ Format: `+CountryCodeNumber`\n"
        "📌 Example: `+91XXXXXXXXXX`\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ **Where to get OTP?**\n"
        "Telegram App → Settings → Devices → Active Sessions\n"
        "Or via SMS on your registered number\n\n"
        "🔒 _Never share your OTP with anyone!_\n"
        "⏳ _15 min timeout — /cancel to stop_",

    "login_otp_sent":
        "✅ **OTP Sent!**\n\n"
        "📲 **Where to find it:**\n"
        "• Telegram App → Settings → Devices\n"
        "• Or SMS on your registered number\n\n"
        "✏️ **Send in this format:** `HELLO12345`\n"
        "_(Type HELLO before the code)_\n\n"
        "⏳ _OTP expires in 2 minutes_",

    "login_step_pass":
        "🔑 **Enter 2FA Password**\n\n"
        "Your account has Two-Factor Authentication enabled.\n"
        "Please enter your Telegram password.\n\n"
        "💡 _Settings → Privacy & Security → Two-Step Verification_",

    "login_success":
        "🎉 **Login Successful!**\n\n"
        "✅ Your Telegram account is now connected.\n\n"
        "**Next Steps:**\n"
        "1️⃣ Add a Source (channel to copy from)\n"
        "2️⃣ Add a Destination (where to send)\n"
        "3️⃣ Start Forwarding ✅",

    "login_wrong_phone":
        "❌ **Invalid Phone Number!**\n\n"
        "Correct format: `+CountryCodeNumber`\n"
        "Example: `+91XXXXXXXXXX`\n\n"
        "Make sure to include the country code.",

    "login_wrong_otp":
        "❌ **Wrong OTP!**\n\n"
        "Please enter the correct code.\n"
        "For a new OTP, press /start.",

    "login_otp_timeout":
        "⏰ **OTP Timeout!**\n\n"
        "You didn't enter the OTP within 2 minutes.\n"
        "Please try logging in again.",

    "login_2fa_wrong":
        "❌ **Wrong Password!**\n\n"
        "Check your Telegram 2FA password.\n"
        "Forgot it? Telegram → Settings → Privacy → Two-Step Verification",

    # ── Logout ────────────────────────────────────────────────────────────────
    "logout_confirm":
        "🔓 **Confirm Logout?**\n\n"
        "⚠️ After logging out:\n"
        "• Forwarding will stop immediately\n"
        "• Sources & destinations will be saved\n\n"
        "Are you sure you want to logout?",
    "btn_logout_yes":  "✅ Yes, Logout",
    "btn_logout_no":   "❌ No, Go Back",
    "logged_out":
        "✅ **Logged Out Successfully!**\n\n"
        "Forwarding has stopped.\n"
        "Login again to resume.",

    # ── Forwarding Start/Stop ──────────────────────────────────────────────────
    "fwd_started":
        "🟢 **FORWARDING STARTED!**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📥 Sources: `{srcs}`  →  📤 Destinations: `{dests}`\n\n"
        "✅ Messages from your sources will now be\n"
        "automatically forwarded to destinations.\n\n"
        "💡 _Keep the bot running 24/7 for best results!_",

    "fwd_stopped_msg":
        "⏹ **FORWARDING STOPPED**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "{today_line}"
        "_Press Start to resume forwarding_",

    "fwd_today_line":  "📊 Forwarded today: `{count}` messages\n\n",
    "fwd_no_src":      "❌ Please add a Source first!\n\nSource = The channel to copy messages from.",
    "fwd_no_dest":     "❌ Please add a Destination first!\n\nDestination = Where messages will be sent.",
    "fwd_no_src_dest": "❌ Please add both a Source and Destination!\n\nOnly 2 steps to get started.",
    "fwd_no_session":  "⚠️ Login required!\n\nPlease connect your Telegram account first.",

    # ── Dashboard ─────────────────────────────────────────────────────────────
    "dashboard_title":     "📊 **DASHBOARD**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    "dashboard_status":    "⚡ **Status:** {status}",
    "dashboard_account":   "👤 **Account:** {plan}",
    "dashboard_forwarded":  "📨 **Forwarded:** `{count}` messages",
    "dashboard_dup":       "♻️ **Duplicates Blocked:** `{count}`",
    "dashboard_sources":   "📥 **Sources:** `{count}`",
    "dashboard_dests":     "📤 **Destinations:** `{count}`",
    "dashboard_filters":   "🛡 **Active Filters:**\n  • {filters}",
    "dashboard_scheduler": "⏰ **Scheduler:** {status} ({start} → {end})",
    "dashboard_no_filter": "None",
    "btn_full_report":     "📜 View Full Report",
    "plan_premium":        "💎 Premium ({days}d)",
    "plan_trial":          "🎁 Trial ({days}d)",
    "plan_free":           "🆓 Free User",
    "plan_lifetime":       "💎 Premium (Lifetime ♾️)",

    # ── Source / Destination ───────────────────────────────────────────────────
    "add_src_title":
        "➕ **Add Source**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Source = The channel or group to copy messages from.\n\n"
        "📌 **How would you like to add?**",

    "add_dest_title":
        "📤 **Add Destination**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Destination = Where messages will be forwarded.\n"
        "⚠️ Your account must be an **Admin** in that channel!\n\n"
        "📌 **How would you like to add?**",

    "send_src_prompt":
        "📥 **Send Source Link, ID or Username:**\n\n"
        "✅ Examples:\n"
        "  • `@mychannel`\n"
        "  • `https://t.me/mychannel`\n"
        "  • `-100xxxxxxxxx` (channel ID)\n\n"
        "💡 _Private channel invite links also work_",

    "send_dest_prompt":
        "📤 **Send Destination Link, ID or Username:**\n\n"
        "✅ Examples:\n"
        "  • `@mychannel`\n"
        "  • `-100xxxxxxxxx` (channel ID)\n\n"
        "⚠️ _Your account must be Admin in that channel!_",

    "src_added":     "✅ **Source Added!**\n\n📥 `{name}` will now be forwarded.",
    "dest_added":    "✅ **Destination Added!**\n\n📤 Messages will be sent to `{name}`.",
    "src_deleted":   "🗑 Source removed: `{name}`",
    "dest_deleted":  "🗑 Destination removed: `{name}`",
    "src_exists":    "⚠️ This source is already added!",
    "dest_exists":   "⚠️ This destination is already added!",
    "src_limit":     "❌ Source limit reached!\n\n💎 Upgrade to Premium for more sources.",
    "dest_limit":    "❌ Destination limit reached!\n\n💎 Upgrade to Premium for more destinations.",
    "src_not_found": "❌ Channel not found!\n\nCheck:\n• Is the link correct?\n• Is the channel public?\n• Is the bot a member?",
    "no_sources":    "📭 **No Sources Added**\n\nAdd a source to get started:\nTap ➕ Add Source below.",
    "no_dests":      "📭 **No Destinations Added**\n\nAdd a destination to get started:\nTap 📤 Add Destination below.",
    "src_list_title": "📋 **Source List**\nSelect to view or delete:",
    "dest_list_title": "📋 **Destination List**\nSelect to view or delete:",

    # ── Settings ──────────────────────────────────────────────────────────────
    "settings_title":
        "⚙️ **Global Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⚠️ These settings apply to **ALL** sources\n\n"
        "📌 Per-source rules → **Source Config**\n\n"
        "📨 **What to Forward:**\n"
        "🟢 = Will forward  🔴 = Will skip",
    "settings_mods":    "─── ✏️ Modifications ───",
    "settings_dup":     "─── ♻️ Duplicate Filter ───",
    "settings_delay":   "⏱ Delay: {delay}s — Tap to Change",
    "btn_help_settings": "❓ Understand Settings",

    # ── Source Config ──────────────────────────────────────────────────────────
    "src_config_title":
        "📍 **Source Config**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Set **custom rules** per source.\n\n"
        "⚙️ = Custom rules  |  📌 = Global settings\n\n"
        "Select a source:",
    "no_src_config":    "❌ No sources found.\n\nPlease add a source first.",
    "btn_link_mode":    "🔗 Change Link Mode",
    "btn_prefix":       "✏️ Prefix (Top Text)",
    "btn_suffix":       "✏️ Suffix (Bottom Text)",
    "btn_per_dest":     "📤 Per-Destination Rules →",
    "btn_reset_src":    "🗑 Reset All Rules",
    "btn_src_list":     "🔙 Source List",
    "has_rules_mark":   "⚙️",
    "no_rules_mark":    "📌",

    # ── Per-Dest Rules ─────────────────────────────────────────────────────────
    "per_dest_title":
        "📤 **Per-Destination Rules**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Source: **{src_name}**\n\n"
        "Set **different rules** per destination.\n"
        "E.g. Dest A gets photos only, Dest B gets text only.\n\n"
        "⚙️ = Custom rules  |  🌐 = Source defaults\n\n"
        "Select a destination:",
    "no_dests_config":  "❌ Please add a destination first!",
    "btn_help_per_dest":"❓ What are Per-Dest Rules?",
    "custom_mark":      "⚙️",
    "default_mark":     "🌐",

    # ── Filters ───────────────────────────────────────────────────────────────
    "filters_title":
        "🧠 **Advanced Tools**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "♻️ **Duplicate Filter** — Block repeated messages\n"
        "🔍 **Keyword Filter** — Filter by specific words\n"
        "🚫 **Link Blocker** — Block or limit links",
    "btn_dup_filter":   "♻️ Duplicate Filter",
    "btn_kw_filter":    "🔍 Keyword Filter",

    # ── Replacement ───────────────────────────────────────────────────────────
    "replace_title":
        "🔄 **Replacement Settings**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Automatically replace any text or link.\n"
        "Example: `@old` → `@new`",
    "btn_add_repl":     "➕ Add Replacement",
    "btn_list_repl":    "📋 View List",
    "btn_clear_repl":   "🗑 Clear All",
    "no_replacements":  "❌ No replacements added.\n\nTap below to add one.",

    # ── Scheduler ─────────────────────────────────────────────────────────────
    "scheduler_title":
        "⏰ **Scheduler**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Set forwarding to run only during specific hours.\n"
        "E.g. only forward from 10pm to 8am.",
    "sched_enabled":    "✅ Active ({start} → {end})",
    "sched_disabled":   "❌ Disabled",

    # ── Backup ────────────────────────────────────────────────────────────────
    "backup_title":
        "💾 **Backup / Restore**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Export a backup of all your settings, sources,\n"
        "and destinations — or restore from a backup.",
    "btn_export":       "📤 Export Backup",
    "btn_import":       "📥 Import Backup",

    # ── Status ────────────────────────────────────────────────────────────────
    "status_title":     "📊 **Bot Status**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
    "status_session":   "🔌 Session: {val}",
    "status_fwd":       "▶️ Forwarding: {val}",
    "status_plan":      "💎 Plan: {val}",
    "status_src_dest":  "📺 Sources: `{src}` | 📤 Destinations: `{dest}`",
    "status_connected": "✅ Connected",
    "status_disconnected": "❌ Disconnected",
    "status_running":   "✅ Running",
    "status_stopped":   "❌ Stopped",

    # ── Help ──────────────────────────────────────────────────────────────────
    "help_title":       "❓ **Help & Guide**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\nWhat would you like to know?",
    "btn_help_start":   "🚀 Getting Started",
    "btn_help_settings_h": "⚙️ Understand Settings",
    "btn_help_src":     "📍 Source Config Guide",
    "btn_help_perdest": "📤 Per-Dest Rules Guide",
    "btn_help_advanced":"🔧 Advanced Features",
    "btn_help_problems":"❓ Common Problems",
    "help_commands":
        "📋 **Commands:**\n\n"
        "• /start — Open main menu\n"
        "• /status — Check bot status\n"
        "• /cancel — Cancel current step\n"
        "• /help — Help guide\n"
        "• /menu — Main menu\n"
        "• /rules — Bot rules",

    # ── Language Menu ─────────────────────────────────────────────────────────
    "lang_title":
        "🌐 **Choose Language / भाषा चुनें**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Current: **{current}**\n\n"
        "Select your preferred language:",
    "lang_changed":     "✅ Language set to English!",

    # ── Premium ───────────────────────────────────────────────────────────────
    "premium_required":
        "💎 **This is a Premium Feature!**\n\n"
        "Upgrade to Premium to unlock this feature.\n\n"
        "Premium Benefits:\n"
        "• More Sources & Destinations\n"
        "• Advanced Filters\n"
        "• Priority Support\n"
        "• And much more!",
    "btn_buy_now":      "💳 Buy Now",

    # ── Errors & Generic ──────────────────────────────────────────────────────
    "error_generic":    "❌ Something went wrong!\n\nPlease try again or press /cancel.",
    "error_not_found":  "❌ Not found! Check and try again.",
    "error_permission": "🚫 You don't have permission!",
    "login_first":      "⚠️ Please Login First!\n\nConnect your account to use the bot.",
    "banned_msg":       "🚫 You have been banned from this bot.",
    "maintenance_msg":
        "🔧 **Bot is Under Maintenance**\n\n"
        "The bot is currently under maintenance.\n"
        "Please come back later!",
    "new_reg_closed":   "🚫 New registrations are currently closed.\n\nPlease try later!",

    # ── Contact / Support ─────────────────────────────────────────────────────
    "contact_prompt":
        "📞 **Message the Admin**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Type your question or problem below 👇\n\n"
        "_Or press /cancel to go back_",
    "contact_sent":
        "✅ **Message Sent to Admin!**\n\n"
        "The admin will reply as soon as possible 🙏",
    "contact_failed":   "❌ Message could not be delivered.\nContact the admin directly.",
    "empty_msg":        "❌ Cannot send an empty message!",

    # ── Timeout / Cancel ──────────────────────────────────────────────────────
    "timeout_msg":
        "⏰ **Timeout!**\n\n"
        "No response received for 15 minutes.\n"
        "Current step was automatically cancelled.\n\n"
        "Press the button below to continue.",
    "cancelled_msg":    "✅ **Cancelled!**\n\nYou're back at the main menu.",
    "nothing_to_cancel": "ℹ️ There was no active step to cancel.",

    # ── Session Errors ────────────────────────────────────────────────────────
    "session_error":
        "⚠️ **Session Error!**\n\n"
        "Your Telegram session has become invalid.\n"
        "Reason: Session was used in two places.\n\n"
        "✅ Solution: Please login again.",
    "session_revoked":
        "⚠️ **Session Expired!**\n\n"
        "Your forwarding has STOPPED.\n"
        "Reason: Telegram revoked your session.\n\n"
        "✅ Solution: Please login again.",
    "btn_relogin":      "🔁 Login Again",

    # ── Misc ──────────────────────────────────────────────────────────────────
    "not_found":        "❌ Not found!",
    "confirm_yes":      "✅ Yes",
    "confirm_no":       "❌ No",
    "prev":             "⬅️ Previous",
    "next":             "Next ➡️",
    "owner_tag":        "",
    "random_text_reply":
        "🤔 I didn't understand that!\n\n"
        "📋 Tap the button below for the menu:",
    "menu_label":       "🏠 Main Menu",
    "stop_btn":         "🔴 Stop",
    "start_btn":        "🟢 Start",
    "dashboard_btn":    "📊 Dashboard",
},

}
