"""
notifications.py — notification_center.py ka wrapper.
Purana code jo yahan se import karta tha wo sab kaam karta rahe.
"""

from notification_center import (
    notify_bot_online,
    notify_bot_offline,
    notify_new_user,
    notify_new_premium,
    notify_payment_received,
    notify_fraud_detected,
    notify_worker_dead,
    notify_ram_high,
    notify_forward_errors,
    notify_daily_summary,
    alert_user_session_expired,
    alert_user_not_admin,
    alert_user_limit_warning,
    alert_user_premium_expiring,
    alert_user_auto_paused,
    start_daily_summary_task,
    handle_nc_input,
    register_nc_handlers,
)

# Legacy: purana code bot argument ke sath call karta tha
async def notify_new_user_legacy(bot, user_id: int, username: str = None):
    await notify_new_user(user_id, username=username or "")
