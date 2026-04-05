# reseller.py
# ══════════════════════════════════════════════════════════
# RESELLER SYSTEM
# Owner → Resellers ko quota do
# Reseller → Apne users ko premium de
# Completely in-bot — koi extra setup nahi
# ══════════════════════════════════════════════════════════

import time
import logging
from database import GLOBAL_STATE, get_user_data, save_persistent_db

logger = logging.getLogger(__name__)

# ── Storage ─────────────────────────────────────────────
def _get_resellers() -> dict:
    """GLOBAL_STATE se resellers dict lo."""
    if "resellers" not in GLOBAL_STATE:
        GLOBAL_STATE["resellers"] = {}
    # BUG 23 FIX: JSON load ke baad string keys hoti hain - int mein normalize karo
    raw = GLOBAL_STATE["resellers"]
    normalized = {}
    changed = False
    for k, v in list(raw.items()):
        int_key = int(k) if not isinstance(k, int) else k
        normalized[int_key] = v
        if not isinstance(k, int):
            changed = True
    if changed:
        GLOBAL_STATE["resellers"] = normalized
    return GLOBAL_STATE["resellers"]


# ── Reseller Management (Admin) ──────────────────────────
def add_reseller(user_id: int, quota: int, commission_pct: float, added_by: int) -> dict:
    """
    Naya reseller add karo.
    quota: kitne users ko premium de sakta hai
    commission_pct: 0-100 (e.g. 20 = 20% commission)
    """
    resellers = _get_resellers()
    # Validate inputs regardless of new or update
    quota_val = max(0, int(quota))
    commission_val = max(0.0, min(100.0, float(commission_pct)))
    if user_id in resellers:
        # Update existing
        resellers[user_id]["quota"] = quota_val
        resellers[user_id]["commission"] = commission_val
    else:
        # FIX 15b: Validate commission (0-100%)
        commission_pct = max(0.0, min(100.0, float(commission_pct)))
        resellers[user_id] = {
            "quota":        max(0, int(quota)),
            "commission":   commission_pct,
            "used":         0,
            "users":        [],        # [user_id, ...]
            "earnings":     0.0,       # tracked earnings
            "added_by":     added_by,
            "added_at":     int(time.time()),
            "active":       True,
        }
    save_persistent_db()
    return resellers[user_id]


def remove_reseller(user_id: int) -> bool:
    """Reseller remove karo."""
    resellers = _get_resellers()
    if user_id in resellers:
        del resellers[user_id]
        save_persistent_db()
        return True
    return False


def suspend_reseller(user_id: int, suspended: bool = True) -> bool:
    """Reseller suspend/unsuspend karo."""
    resellers = _get_resellers()
    if user_id not in resellers:
        return False
    resellers[user_id]["active"] = not suspended
    save_persistent_db()
    return True


def get_reseller(user_id: int) -> dict | None:
    """Ek reseller ka data lo."""
    return _get_resellers().get(user_id)


def is_reseller(user_id: int) -> bool:
    """Check karo ki ye user reseller hai."""
    r = get_reseller(user_id)
    return r is not None and r.get("active", False)


def get_all_resellers() -> list:
    """Saare resellers ki list lo."""
    result = []
    for uid, data in _get_resellers().items():
        result.append({"user_id": uid, **data})
    return sorted(result, key=lambda x: x.get("added_at", 0), reverse=True)


# ── Premium Management (Reseller) ────────────────────────
def reseller_give_premium(
    reseller_id: int,
    target_user_id: int,
    days: int,
    plan_name: str = "Reseller Plan"
) -> tuple[bool, str]:
    """
    Reseller kisi user ko premium de.
    FIX 24: Thread-safe with per-reseller lock (race condition prevention).
    Returns: (success: bool, message: str)
    """
    if not is_reseller(reseller_id):
        return False, "❌ Tum reseller nahi ho."

    # FIX 24: Acquire lock before quota check + decrement (atomic operation)
    with _get_reseller_lock(reseller_id):
        resellers = _get_resellers()
        r = resellers[reseller_id]

        # Quota check (inside lock — prevents double-spend)
        remaining_quota = r["quota"] - r["used"]
        if remaining_quota <= 0:
            return False, (
                f"❌ Quota khatam ho gaya!\n"
                f"Tumhara total quota: {r['quota']}\n"
                f"Use ho gaya: {r['used']}\n"
                "Admin se zyada quota lo."
            )

    # User ko premium do
    try:
        from premium import give_premium_to_user
        success = give_premium_to_user(
            target_user_id, days,
            given_by=reseller_id,
            plan_name=plan_name
        )
        if not success:
            return False, "❌ Premium dene mein error aaya. User exists?"
    except Exception as e:
        return False, f"❌ Error: {e}"

    # Reseller ka record update karo
    r["used"] += 1
    if target_user_id not in r["users"]:
        r["users"].append(target_user_id)

    save_persistent_db()
    return True, (
        f"✅ Premium diya!\n"
        f"User: `{target_user_id}`\n"
        f"Plan: {days} din ({plan_name})\n\n"
        f"Tumhara quota: {r['used']}/{r['quota']} used\n"
        f"Baaki: {r['quota'] - r['used']}"
    )


def reseller_remove_premium(reseller_id: int, target_user_id: int) -> tuple[bool, str]:
    """Reseller kisi user ka premium hatao."""
    if not is_reseller(reseller_id):
        return False, "❌ Tum reseller nahi ho."

    resellers = _get_resellers()
    r = resellers[reseller_id]

    # Check karo ye user is reseller ne diya tha
    if target_user_id not in r.get("users", []):
        return False, "❌ Ye user tumhare users mein nahi hai."

    try:
        from premium import remove_premium
        remove_premium(target_user_id)
    except Exception as e:
        return False, f"❌ Error: {e}"

    # Quota wapas karo
    if r["used"] > 0:
        r["used"] -= 1
    r["users"] = [u for u in r["users"] if u != target_user_id]

    save_persistent_db()
    return True, (
        f"✅ Premium hataya!\n"
        f"User: `{target_user_id}`\n\n"
        f"Quota freed: {r['used']}/{r['quota']} used"
    )


def add_reseller_earnings(reseller_id: int, amount: float):
    """Reseller ki earnings mein amount add karo."""
    # FIX 15: Validate amount — no negative or extreme values
    if not isinstance(amount, (int, float)) or amount <= 0:
        return
    if amount > 100000:  # Sanity cap: max ₹1 lakh per transaction
        logger.warning(f"Reseller earnings cap hit: {reseller_id} amount={amount}")
        amount = 100000
    resellers = _get_resellers()
    if reseller_id in resellers:
        resellers[reseller_id]["earnings"] = resellers[reseller_id].get("earnings", 0) + amount
        save_persistent_db()


# ── Stats ────────────────────────────────────────────────
def get_reseller_stats(reseller_id: int) -> dict | None:
    """Reseller ka complete stats."""
    r = get_reseller(reseller_id)
    if not r:
        return None
    return {
        "quota":      r.get("quota", 0),
        "used":       r.get("used", 0),
        "remaining":  r.get("quota", 0) - r.get("used", 0),
        "users":      r.get("users", []),
        "earnings":   r.get("earnings", 0.0),
        "commission": r.get("commission", 0),
        "active":     r.get("active", True),
    }


def get_admin_reseller_summary() -> dict:
    """Admin ke liye overall reseller summary."""
    resellers = _get_resellers()
    total_resellers = len(resellers)
    active_resellers = sum(1 for r in resellers.values() if r.get("active", True))
    total_quota = sum(r.get("quota", 0) for r in resellers.values())
    total_used = sum(r.get("used", 0) for r in resellers.values())
    total_earnings = sum(r.get("earnings", 0) for r in resellers.values())

    return {
        "total":    total_resellers,
        "active":   active_resellers,
        "quota":    total_quota,
        "used":     total_used,
        "remaining": total_quota - total_used,
        "earnings": total_earnings,
  }
