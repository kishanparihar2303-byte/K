# feature_flags.py
# ══════════════════════════════════════════════════════════
# ADMIN MASTER CONTROL — Har feature ka switch
# Admin bot se in flags ko change kar sakta hai
# API key bhi yahan se manage hoti hai
# ══════════════════════════════════════════════════════════

from database import GLOBAL_STATE, save_persistent_db

# ── Default Values ──────────────────────────────────────
# "free"     = sabke liye available
# "premium"  = sirf premium users ke liye
# "disabled" = feature band hai
DEFAULT_FLAGS = {
    # Feature access levels
    # AI Rewrite feature removed
    "per_day_scheduler":    "free",   # Was premium — ab sab use kar sakte hain
    "auto_watermark":       "free",
    "deep_analytics":       "premium",   # basic=free, full=premium
    "smart_notifications":  "free",
    "affiliate_manager":    "free",
    "duplicate_filter":     "free",    # Duplicate message filter
    "link_blocker":         "free",    # Link/username/hashtag blocker
    "replacement_rules":    "free",    # Text replacement rules
    "per_dest_rules":       "free",    # Per-destination rules
    "scheduler_basic":      "free",    # Basic time scheduler
    "start_end_msg":        "free",    # Start/End message feature
    "user_rate_limit":      True,      # Anti-spam rate limiting
    "max_msg_per_min":      30,        # Max msgs/min per user (anti-spam)
    "spam_auto_block":      False,     # Auto-block spammers
    "reseller_system":      True,        # enabled/disabled (bool)

    # AI Rewrite removed — ye feature band kar diya gaya hai

    # Watermark Settings
    "force_watermark_all":  False,       # Sabke photos pe force watermark
    "force_watermark_text": "",          # Admin ka forced text (e.g. "@MyBot")
    "allow_user_watermark": True,        # User apna watermark set kar sake

    # Affiliate Settings
    "affiliate_mode":       "user",      # "user" = user ka apna tag
                                         # "owner" = sabke links mein owner ka tag
    "owner_amazon_tag":     "",          # Owner ka Amazon affiliate tag
    "owner_flipkart_id":    "",          # Owner ka Flipkart ID
    "owner_meesho_ref":     "",          # Owner ka Meesho referral code
    "owner_myntra_id":      "",          # Owner ka Myntra campaign ID
    "owner_ajio_id":        "",          # Owner ka Ajio affiliate ID
    "owner_nykaa_id":       "",          # Owner ka Nykaa affiliate ID
    "owner_snapdeal_id":    "",          # Owner ka Snapdeal affiliate ID

    # Start/End Message Admin Override
    "force_start_msg":         "",     # Admin ka forced start msg (sabke liye)
    "force_end_msg":           "",     # Admin ka forced end msg (sabke liye)
    "force_msg_mode":          "append",  # "append" = user ke upar/neeche | "replace" = user ka hatao

    # Commission Split (Free users ke liye)
    # Premium users se KABHI commission nahi li jaayegi
    "commission_enabled":   True,        # Commission system on/off
    "commission_rate":      30,          # Admin ka hissa % mein (0-100)
                                         # e.g. 30 = 30% messages mein admin tag lagega
                                         # Free users ko onboarding pe clearly bataya jaayega

    # Notifications Settings
    "notify_new_user":      True,
    "notify_new_premium":   True,
    "notify_payment":       True,
    "notify_worker_dead":   True,
    "notify_db_warning":    True,
    "notify_daily_summary": True,
    "alert_channel_id":     None,        # Agar set ho toh is channel mein alerts jaayein
}


def _get_flags() -> dict:
    """GLOBAL_STATE se feature flags lo — initialize karo agar nahi hain."""
    if "feature_flags" not in GLOBAL_STATE:
        GLOBAL_STATE["feature_flags"] = DEFAULT_FLAGS.copy()
    else:
        # Naye flags jo DB mein nahi hain unhe add karo (migration)
        for k, v in DEFAULT_FLAGS.items():
            if k not in GLOBAL_STATE["feature_flags"]:
                GLOBAL_STATE["feature_flags"][k] = v
    return GLOBAL_STATE["feature_flags"]


def get_flag(key: str):
    """Ek specific flag ki value lo."""
    return _get_flags().get(key, DEFAULT_FLAGS.get(key))


def set_flag(key: str, value):
    """Ek flag set karo aur DB mein save karo."""
    flags = _get_flags()
    flags[key] = value
    save_persistent_db()


def get_all_flags() -> dict:
    """Saare flags ek saath lo."""
    return _get_flags().copy()


# ── Feature Access Check ────────────────────────────────
def is_feature_available(user_id: int, feature_key: str) -> bool:
    """
    Check karo ki user ye feature use kar sakta hai ya nahi.
    Priority: disabled > free_mode > flag_val > premium check
    """
    flag_val = get_flag(feature_key)

    # Agar feature globally disabled hai
    if flag_val == "disabled":
        return False

    # Bool flag (e.g. reseller_system = True/False)
    if isinstance(flag_val, bool):
        return flag_val

    # Free flag — everyone gets it
    if flag_val == "free":
        return True

    # Premium flag — check free_mode first
    if flag_val == "premium":
        # FIX D: Free mode override — admin ne free kar diya hai
        try:
            from premium import is_free_mode
            if is_free_mode():
                return True  # Bot is in free mode — all premium features unlocked
        except Exception:
            pass
        # Normal premium check
        try:
            from premium import is_premium_user as is_user_premium
            return is_user_premium(user_id)
        except Exception:
            return False

    return True


def get_gemini_api_key(user_id: int = None) -> str:
    """AI Rewrite removed — hamesha empty string."""
    return ""



# ── Shortcut Functions ──────────────────────────────────
def ai_rewrite_available(user_id: int = None) -> bool:
    """AI Rewrite feature permanently removed."""
    return False

def scheduler_advanced_available(user_id: int) -> bool:
    return is_feature_available(user_id, "per_day_scheduler")

def watermark_available(user_id: int) -> bool:
    return is_feature_available(user_id, "auto_watermark")


def dup_filter_available(user_id: int) -> bool:
    return is_feature_available(user_id, "duplicate_filter")

def link_blocker_available(user_id: int) -> bool:
    return is_feature_available(user_id, "link_blocker")

def replacement_available(user_id: int) -> bool:
    return is_feature_available(user_id, "replacement_rules")

def per_dest_available(user_id: int) -> bool:
    return is_feature_available(user_id, "per_dest_rules")

def start_end_msg_available(user_id: int) -> bool:
    return is_feature_available(user_id, "start_end_msg")

def get_rate_limit_config() -> dict:
    return {
        "enabled":        bool(get_flag("user_rate_limit")),
        "max_per_min":    int(get_flag("max_msg_per_min") or 30),
        "auto_block":     bool(get_flag("spam_auto_block")),
    }
def analytics_full_available(user_id: int) -> bool:
    return is_feature_available(user_id, "deep_analytics")

def affiliate_available(user_id: int) -> bool:
    return is_feature_available(user_id, "affiliate_manager")

def reseller_system_enabled() -> bool:
    return bool(get_flag("reseller_system"))
