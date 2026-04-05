# bot/admin.py  — UPGRADED v4.0
import datetime
import time
from time_helper import ab_fmt  # FIX: __import__ hataya, proper import
from database import db, GLOBAL_STATE, admin_logs, save_persistent_db
from config import OWNER_ID

PRIMARY_ADMIN_ID = OWNER_ID
ADMIN_ROLES = ["Support", "Moderator", "Super Admin", "Owner"]

# ── Role check ─────────────────────────────────────────────────────────────
def is_admin(user_id, min_role="Support"):
    if user_id == PRIMARY_ADMIN_ID:
        return True
    # BUG 19 FIX: JSON se load hone par keys strings ban jaati hain
    # int aur str dono try karo
    admins = GLOBAL_STATE.get("admins", {})
    user_role = admins.get(user_id) or admins.get(str(user_id))
    if not user_role:
        return False
    try:
        return ADMIN_ROLES.index(user_role) >= ADMIN_ROLES.index(min_role)
    except ValueError:
        return False

def get_admin_role(user_id) -> str:
    if user_id == PRIMARY_ADMIN_ID:
        return "Owner"
    return GLOBAL_STATE["admins"].get(user_id, "")

# ── Logging ─────────────────────────────────────────────────────────────────
_log_save_counter = 0

def add_log(admin_id, action, target="System", details=""):
    global _log_save_counter
    log_entry = {
        "time": ab_fmt(None, "%Y-%m-%d %H:%M:%S"),
        "admin": admin_id,
        "action": action,
        "target": str(target),
        "details": str(details),
    }
    admin_logs.append(log_entry)
    if len(admin_logs) > 1000:
        admin_logs[:] = admin_logs[-800:]
    _log_save_counter += 1
    if _log_save_counter >= 10:
        _log_save_counter = 0
        save_persistent_db()

# ── User Notes ───────────────────────────────────────────────────────────────
def get_user_notes(user_id: int) -> list:
    GLOBAL_STATE.setdefault("user_notes", {})
    return GLOBAL_STATE["user_notes"].get(str(user_id), [])

def add_user_note(admin_id: int, user_id: int, note: str):
    GLOBAL_STATE.setdefault("user_notes", {})
    key = str(user_id)
    GLOBAL_STATE["user_notes"].setdefault(key, [])
    entry = {
        "text": note,
        "by": admin_id,
        "time": ab_fmt(None, "%d/%m %H:%M"),
    }
    GLOBAL_STATE["user_notes"][key].append(entry)
    if len(GLOBAL_STATE["user_notes"][key]) > 10:
        GLOBAL_STATE["user_notes"][key] = GLOBAL_STATE["user_notes"][key][-10:]
    add_log(admin_id, "Add Note", target=user_id, details=note[:40])
    save_persistent_db()  # FIX #6: Sirf ek baar save — double save hataya

def delete_user_note(user_id: int, index: int):
    GLOBAL_STATE.setdefault("user_notes", {})
    notes = GLOBAL_STATE["user_notes"].get(str(user_id), [])
    if 0 <= index < len(notes):
        notes.pop(index)
        save_persistent_db()

# ── System Stats ─────────────────────────────────────────────────────────────
def get_system_stats():
    total_users  = len(db)
    active_fwd   = sum(1 for u in list(db.values()) if u.get("settings", {}).get("running"))
    total_src    = sum(len(u.get("sources", [])) for u in db.values())
    total_dest   = sum(len(u.get("destinations", [])) for u in db.values())
    blocked      = len(GLOBAL_STATE.get("blocked_users", []))
    prem_count   = sum(1 for u in list(db.values()) if u.get("premium", {}).get("active"))
    today_start  = datetime.datetime.now().replace(hour=0, minute=0, second=0).timestamp()
    week_start   = (datetime.datetime.now() - datetime.timedelta(days=7)).timestamp()
    new_today    = sum(1 for u in list(db.values()) if u.get("joined_at", 0) >= today_start)
    new_week     = sum(1 for u in list(db.values()) if u.get("joined_at", 0) >= week_start)
    revenue_month = _get_monthly_revenue()
    return {
        "total_users":   total_users,
        "active_fwd":    active_fwd,
        "stopped_users": total_users - active_fwd,
        "blocked":       blocked,
        "sources":       total_src,
        "dest":          total_dest,
        "m_mode":        "Online" if not GLOBAL_STATE.get("maintenance_mode") else "Maintenance",
        "prem_count":    prem_count,
        "new_today":     new_today,
        "new_week":      new_week,
        "revenue_month": revenue_month,
    }

def _get_monthly_revenue() -> int:
    try:
        payments   = GLOBAL_STATE.get("payment_history", [])
        month_ts   = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0).timestamp()
        return int(sum(p.get("amount", 0) for p in payments
                       if p.get("status") == "approved" and p.get("ts", 0) >= month_ts))
    except Exception:
        return 0

def get_revenue_stats() -> dict:
    try:
        payments  = GLOBAL_STATE.get("payment_history", [])
        now       = time.time()
        today_ts  = datetime.datetime.now().replace(hour=0, minute=0, second=0).timestamp()
        week_ts   = now - 7 * 86400
        month_ts  = datetime.datetime.now().replace(day=1, hour=0, minute=0, second=0).timestamp()
        approved  = [p for p in payments if p.get("status") == "approved"]
        return {
            "today":         int(sum(p.get("amount", 0) for p in approved if p.get("ts", 0) >= today_ts)),
            "week":          int(sum(p.get("amount", 0) for p in approved if p.get("ts", 0) >= week_ts)),
            "month":         int(sum(p.get("amount", 0) for p in approved if p.get("ts", 0) >= month_ts)),
            "total":         int(sum(p.get("amount", 0) for p in approved)),
            "pending_count": len([p for p in payments if p.get("status") == "pending"]),
            "total_txns":    len(approved),
        }
    except Exception:
        return {"today": 0, "week": 0, "month": 0, "total": 0, "pending_count": 0, "total_txns": 0}

def record_broadcast_result(sent: int, failed: int, target: str):
    GLOBAL_STATE["last_broadcast"] = {
        "sent":   sent,
        "failed": failed,
        "target": target,
        "time":   ab_fmt(None, "%d/%m/%Y %I:%M %p"),
    }
    save_persistent_db()

def get_last_broadcast() -> dict:
    return GLOBAL_STATE.get("last_broadcast", {})

def update_admin_role(admin_id, new_role, changer_id):
    if changer_id != PRIMARY_ADMIN_ID and not is_admin(changer_id, "Super Admin"):
        return False, "Permission Denied"
    if new_role not in ADMIN_ROLES:
        return False, "Invalid Role"
    GLOBAL_STATE["admins"][admin_id] = new_role
    add_log(changer_id, "Update Admin Role", target=admin_id, details=new_role)
    save_persistent_db()  # FIX #6: Sirf ek baar — double save hataya
    return True, f"Admin {admin_id} updated to {new_role}"

def toggle_maintenance(admin_id):
    if not is_admin(admin_id, "Super Admin"):
        return False
    GLOBAL_STATE["maintenance_mode"] = not GLOBAL_STATE.get("maintenance_mode", False)
    status = "ON" if GLOBAL_STATE["maintenance_mode"] else "OFF"
    add_log(admin_id, "Maintenance Toggle", target="Global", details=status)
    save_persistent_db()  # FIX #6: Sirf ek baar — double save hataya
    return True
