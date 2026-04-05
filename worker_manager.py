# worker_manager.py
# Worker Assignment + Load Balancing
# Main bot yahan se workers ko users assign karta hai
# Workers MongoDB poll karke apna kaam karte hain

import time
import logging
from database import db, GLOBAL_STATE, save_persistent_db, get_user_data
from config import MAX_USERS_PER_WORKER, TOTAL_WORKERS

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════
# WORKER STATE — GLOBAL_STATE mein store hoga
# ══════════════════════════════════════════
# GLOBAL_STATE["worker_heartbeats"] = {
#     0: 1710000000,   # worker_id: last_heartbeat_timestamp
#     1: 1710000010,
# }
# user_data["assigned_worker"] = 0  (ya None agar koi nahi)


def get_worker_loads() -> dict:
    """
    Har worker ke paas kitne users hain — count karo.
    Returns: {0: 12, 1: 34, 2: 5}
    """
    loads = {i: 0 for i in range(TOTAL_WORKERS)}
    for uid, udata in db.items():
        w = udata.get("assigned_worker")
        if w is not None and w in loads:
            loads[w] += 1
    return loads


def assign_worker(user_id: int) -> int:
    """
    Naye user ko sabse kam busy worker assign karo.
    Agar sab full hain — phir bhi sabse kam busy ko do (overflow allow).
    Returns: assigned worker_id (int)
    """
    # Already assigned hai?
    existing = get_user_data(user_id).get("assigned_worker")
    if existing is not None:
        return existing

    loads = get_worker_loads()

    # Sabse kam load wala worker dhundo
    best_worker = min(loads, key=loads.get)

    if loads[best_worker] >= MAX_USERS_PER_WORKER:
        logger.warning(
            f"⚠️ Worker {best_worker} full ({loads[best_worker]}/{MAX_USERS_PER_WORKER}). "
            f"User {user_id} ko overflow de rahe hain."
        )

    get_user_data(user_id)["assigned_worker"] = best_worker
    save_persistent_db()
    logger.info(f"✅ User {user_id} → Worker {best_worker} (load: {loads[best_worker]+1}/{MAX_USERS_PER_WORKER})")
    return best_worker


def unassign_worker(user_id: int):
    """User logout ya delete par — assignment hatao."""
    data = get_user_data(user_id)
    data.pop("assigned_worker", None)
    save_persistent_db()


def get_worker_status() -> list:
    """
    Admin panel ke liye — har worker ka status.
    Returns: list of dicts
    [
      {"id": 0, "users": 23, "capacity": 50, "alive": True, "last_seen": "2m ago"},
      ...
    ]
    """
    loads = get_worker_loads()
    heartbeats = GLOBAL_STATE.get("worker_heartbeats", {})
    now = time.time()
    result = []

    for wid in range(TOTAL_WORKERS):
        last_beat = heartbeats.get(str(wid)) or heartbeats.get(wid)
        if last_beat:
            elapsed = int(now - last_beat)
            alive = elapsed < 120  # 2 minute mein heartbeat na aaye = dead
            if elapsed < 60:
                seen = f"{elapsed}s ago"
            elif elapsed < 3600:
                seen = f"{elapsed // 60}m ago"
            else:
                seen = f"{elapsed // 3600}h ago"
        else:
            alive = False
            seen = "Never"

        result.append({
            "id": wid,
            "users": loads.get(wid, 0),
            "capacity": MAX_USERS_PER_WORKER,
            "alive": alive,
            "last_seen": seen,
        })

    return result


def rebalance_workers() -> int:
    """
    Overloaded workers se users ko underloaded workers mein shift karo.
    Uses list() snapshot so dict mutation during iteration is safe.
    """
    loads = get_worker_loads()
    changed = 0

    avg = sum(loads.values()) / max(len(loads), 1)
    overloaded  = [w for w, l in loads.items() if l > avg * 1.3]
    underloaded = [w for w, l in loads.items() if l < avg * 0.7]

    if not overloaded or not underloaded:
        return 0

    # list() snapshot — prevents RuntimeError if db changes during iteration
    for uid, udata in list(db.items()):
        w = udata.get("assigned_worker")
        if w in overloaded and underloaded:
            # Running users ko mat touch karo — sirf inactive
            if not udata.get("settings", {}).get("running", False):
                new_w = underloaded[0]
                udata["assigned_worker"] = new_w
                loads[w]     -= 1
                loads[new_w] += 1
                changed      += 1

                if loads[new_w] >= avg * 1.0:
                    underloaded.pop(0)
                    if not underloaded:
                        break

    if changed > 0:
        save_persistent_db()
        logger.info(f"🔄 Rebalanced: {changed} users moved.")

    return changed


def record_worker_heartbeat(worker_id: int):
    """Worker apna heartbeat record karta hai — alive sign."""
    hb = GLOBAL_STATE.setdefault("worker_heartbeats", {})
    hb[str(worker_id)] = time.time()
    # BUG 9 FIX: Debounced save — heartbeat persist karo (restart-safe)
    # Par har 30s mein sirf ek baar save karo (debounce)
    try:
        last_save_key = f"_hb_last_save_{worker_id}"
        last = hb.get(last_save_key, 0)
        if time.time() - last > 60:   # Max once per 60s
            hb[last_save_key] = time.time()
            save_persistent_db()
    except Exception:
        pass


def get_total_active_sessions() -> int:
    """Kitne users abhi running=True hain."""
    count = 0
    for udata in db.values():
        if udata.get("settings", {}).get("running") and udata.get("session"):
            count += 1
    return count
