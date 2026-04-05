"""
backup.py — Thin re-export of database.py

Previously this was an accidental exact duplicate of database.py (48 identical
functions, ~1046 lines of duplicated code). It is now a single re-export so any
legacy "from backup import X" or "import backup" still works without error.

All database logic lives exclusively in database.py.
"""
# noqa: F401, F403
from database import (
    db, user_sessions, active_clients, duplicate_db, safety_db,
    GLOBAL_STATE, CLEANUP_CONFIG, REPLY_CACHE, PRODUCT_HISTORY_STORE,
    admin_logs, change_notifier, db_lock, PerUserRWLock, ChangeNotifier,
    get_user_data, set_user_data_and_notify, update_last_active,
    update_user_stats, get_dup_data, save_dup_data, get_prod_history,
    get_reply_id, save_reply_mapping, get_rules_for_pair,
    block_user, unblock_user, is_blocked, is_user_blocked,
    cleanup_inactive_users, load_persistent_db, save_persistent_db,
    load_from_mongodb_if_available, init_mongodb, save_to_mongo,
    save_user_to_mongo, migrate_database,
)
