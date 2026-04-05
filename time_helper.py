"""
time_helper.py — Unified Timezone + Time Display System

FIXES:
  ✅ Problem 21: Har jagah sahi timezone mein time dikhao
  ✅ Phone number se auto timezone detect karo (login pe)
  ✅ datetime.now() → ab_now(user_id) se replace

COUNTRY CODE → TIMEZONE MAP (login pe auto-set)
"""

import datetime
import logging

logger = logging.getLogger(__name__)

# ── Phone country code → timezone ─────────────────────────────────────────────
# Key = country calling code (string), Value = IANA timezone
PHONE_TZ_MAP = {
    "91":  "Asia/Kolkata",       # India
    "92":  "Asia/Karachi",       # Pakistan
    "880": "Asia/Dhaka",         # Bangladesh
    "977": "Asia/Kathmandu",     # Nepal
    "94":  "Asia/Colombo",       # Sri Lanka
    "971": "Asia/Dubai",         # UAE
    "966": "Asia/Riyadh",        # Saudi Arabia
    "968": "Asia/Muscat",        # Oman
    "974": "Asia/Qatar",         # Qatar
    "965": "Asia/Kuwait",        # Kuwait
    "973": "Asia/Bahrain",       # Bahrain
    "65":  "Asia/Singapore",     # Singapore
    "60":  "Asia/Kuala_Lumpur",  # Malaysia
    "66":  "Asia/Bangkok",       # Thailand
    "62":  "Asia/Jakarta",       # Indonesia
    "63":  "Asia/Manila",        # Philippines
    "84":  "Asia/Ho_Chi_Minh",   # Vietnam
    "86":  "Asia/Shanghai",      # China
    "81":  "Asia/Tokyo",         # Japan
    "82":  "Asia/Seoul",         # South Korea
    "44":  "Europe/London",      # UK
    "49":  "Europe/Berlin",      # Germany
    "33":  "Europe/Paris",       # France
    "7":   "Europe/Moscow",      # Russia
    "1":   "America/New_York",   # USA/Canada — Eastern default. West Coast: America/Los_Angeles
    "61":  "Australia/Sydney",   # Australia
    "64":  "Pacific/Auckland",   # New Zealand
    "27":  "Africa/Johannesburg",# South Africa
    "234": "Africa/Lagos",       # Nigeria
    "254": "Africa/Nairobi",     # Kenya
    "20":  "Africa/Cairo",       # Egypt
    "55":  "America/Sao_Paulo",  # Brazil
    "52":  "America/Mexico_City",# Mexico
    "54":  "America/Argentina/Buenos_Aires",  # Argentina
}


def detect_tz_from_phone(phone: str) -> str | None:
    """
    Phone number se timezone detect karo.
    Returns IANA timezone string ya None.
    
    '+91XXXXXXXXXX' → 'Asia/Kolkata'
    '+14155552671'  → 'America/New_York'
    """
    if not phone:
        return None
    # Strip + and spaces
    digits = phone.lstrip("+").replace(" ", "").replace("-", "")
    
    # Try longest prefix first (3 digits), then 2, then 1
    for length in (3, 2, 1):
        prefix = digits[:length]
        if prefix in PHONE_TZ_MAP:
            return PHONE_TZ_MAP[prefix]
    return None


def auto_set_timezone(user_id: int, phone: str) -> str:
    """
    Login ke time phone number se timezone auto-set karo.

    ✅ IMPROVED:
    - Har login pe re-detect karo (pehle sirf first-time set hoti thi)
    - Manual override preserve karo (user ne khud badla tha to respect karo)
    - Scheduler timezone bhi sync karo
    - Returns: (timezone_str, was_changed: bool, is_auto: bool)
    
    Returns: timezone name (str)
    """
    detected = detect_tz_from_phone(phone)
    fallback  = "Asia/Kolkata"

    try:
        from database import get_user_data, save_persistent_db
        data = get_user_data(user_id)

        current_tz    = data.get("timezone", "")
        manual_set    = data.get("_tz_manual_override", False)   # User ne khud set kiya?

        if detected:
            if not manual_set:
                # Auto-detect: always update (catches SIM swap / travel)
                if current_tz != detected:
                    data["timezone"] = detected
                    data.setdefault("scheduler", {})["timezone"] = detected
                    save_persistent_db()
                    logger.info(
                        f"Auto-TZ updated: user={user_id} "
                        f"{current_tz or '(none)'} → {detected} "
                        f"(phone: {phone[:4]}***)"
                    )
                else:
                    logger.debug(f"Auto-TZ unchanged: user={user_id} tz={detected}")
                return detected
            else:
                # Manual override — don't change, just log
                logger.debug(
                    f"Auto-TZ skipped (manual override): user={user_id} "
                    f"keeping={current_tz}"
                )
                return current_tz or detected
        else:
            # Country code not in map — set fallback only if nothing set
            if not current_tz:
                data["timezone"] = fallback
                data.setdefault("scheduler", {})["timezone"] = fallback
                save_persistent_db()
                logger.info(f"Auto-TZ fallback set: user={user_id} tz={fallback}")
            return data.get("timezone", fallback)

    except Exception as e:
        logger.debug(f"Auto-timezone set failed: {e}")
        return detected or fallback


# ── Core time functions ───────────────────────────────────────────────────────

def _get_tz(user_id=None, fallback="Asia/Kolkata"):
    """Get pytz timezone for user. Thread-safe."""
    tz_name = fallback
    if user_id is not None:
        try:
            from database import get_user_data
            d = get_user_data(user_id)
            tz_name = (
                d.get("timezone")
                or d.get("scheduler", {}).get("timezone")
                or fallback
            )
        except Exception:
            pass
    try:
        import pytz
        return pytz.timezone(tz_name)
    except Exception:
        import pytz
        return pytz.timezone(fallback)


def ab_now(user_id=None) -> datetime.datetime:
    """
    Current datetime in user's timezone.
    Use this EVERYWHERE instead of datetime.datetime.now()
    
    Usage:
        from time_helper import ab_now, ab_fmt
        now = ab_now(user_id)           # datetime object
        txt = ab_fmt(user_id)           # "13/03/2026 08:30 PM"
        txt = ab_fmt(user_id, "%H:%M")  # "20:30"
    """
    try:
        import pytz
        tz = _get_tz(user_id)
        return datetime.datetime.now(tz)
    except Exception:
        utc = datetime.datetime.now(datetime.timezone.utc)
        return utc + datetime.timedelta(hours=5, minutes=30)


def ab_fmt(user_id=None, fmt="%d/%m/%Y %I:%M %p") -> str:
    """Formatted current time in user's timezone."""
    return ab_now(user_id).strftime(fmt)


def ab_ts(timestamp: float, user_id=None, fmt="%d/%m/%Y %I:%M %p") -> str:
    """Convert Unix timestamp to user's timezone formatted string."""
    try:
        tz = _get_tz(user_id)
        dt = datetime.datetime.fromtimestamp(timestamp, tz)
        return dt.strftime(fmt)
    except Exception:
        utc = datetime.datetime.utcfromtimestamp(timestamp)
        ist = utc + datetime.timedelta(hours=5, minutes=30)
        return ist.strftime(fmt)


def ab_today_key(user_id=None) -> str:
    """Date key YYYY-MM-DD in user's timezone."""
    return ab_now(user_id).strftime("%Y-%m-%d")


def tz_name(user_id=None) -> str:
    """Get timezone display name for user."""
    try:
        from database import get_user_data
        d = get_user_data(user_id)
        return d.get("timezone") or "Asia/Kolkata"
    except Exception:
        return "Asia/Kolkata"
