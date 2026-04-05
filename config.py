import os
import sys
import logging
import time
from typing import Any, Optional

# Load .env file if present (local development)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv optional — env vars already set in production


logging.basicConfig(
    format='[%(levelname)5s/%(asctime)s] %(name)s: %(message)s',
    level=logging.INFO,
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class EnvValidationError(Exception):
    pass

ENV_HINTS = {
    "API_ID":    "Get from https://my.telegram.org → App api_id",
    "API_HASH":  "Get from https://my.telegram.org → App api_hash",
    "BOT_TOKEN": "Create via @BotFather on Telegram → /newbot",
    "OWNER_ID":  "Your Telegram numeric user ID (get via @userinfobot)",
    "MONGO_URI": "Free cluster at https://cloud.mongodb.com",
}

def _env(name, required=True, default=None, cast=str, choices=None):
    raw = os.environ.get(name, "")
    if not raw or raw.strip() == "":
        if required and default is None:
            sep = "=" * 55
            print(f"\n{sep}")
            print(f"❌ MISSING REQUIRED ENV VAR: {name}")
            print(f"{sep}")
            if name in ENV_HINTS:
                print(f"  💡 {ENV_HINTS[name]}")
            sys.exit(1)
        return default
    try:
        if cast == bool:
            return raw.strip().lower() in ("1", "true", "yes", "on")
        return cast(raw.strip())
    except (ValueError, TypeError):
        logger.error(f"❌ {name} must be {cast.__name__}, got: '{raw}'")
        sys.exit(1)

API_ID    = _env("API_ID",    cast=int)
API_HASH  = _env("API_HASH")
BOT_TOKEN = _env("BOT_TOKEN")
OWNER_ID  = _env("OWNER_ID",  cast=int, required=True)
ADMINS    = [OWNER_ID]

MAX_USERS_PER_WORKER = _env("MAX_USERS_PER_WORKER", cast=int, required=False, default=100)
TOTAL_WORKERS        = _env("TOTAL_WORKERS",        cast=int, required=False, default=3)
WORKER_ID: Optional[int] = _env("WORKER_ID",       cast=int, required=False, default=None)

MAX_ACTIVE_SESSIONS  = _env("MAX_ACTIVE_SESSIONS",  cast=int,   required=False, default=50)
ENTITY_CACHE_LIMIT   = _env("ENTITY_CACHE_LIMIT",   cast=int,   required=False, default=50)
REPLY_CACHE_MAX      = _env("REPLY_CACHE_MAX",       cast=int,   required=False, default=100)
ALBUM_BUFFER_MAX     = _env("ALBUM_BUFFER_MAX",      cast=int,   required=False, default=50)
MAX_DOWNLOAD_MB      = _env("MAX_DOWNLOAD_MB",       cast=int,   required=False, default=20)

CB_FAIL_THRESHOLD    = _env("CB_FAIL_THRESHOLD",    cast=int,   required=False, default=5)
CB_COOLDOWN_SEC      = _env("CB_COOLDOWN_SEC",      cast=float, required=False, default=60.0)
CB_MAX_COOLDOWN_SEC  = _env("CB_MAX_COOLDOWN_SEC",  cast=float, required=False, default=3600.0)

RL_FREE_RATE         = _env("RL_FREE_RATE",         cast=float, required=False, default=0.5)
RL_PREMIUM_RATE      = _env("RL_PREMIUM_RATE",      cast=float, required=False, default=2.0)
RL_ADMIN_RATE        = _env("RL_ADMIN_RATE",        cast=float, required=False, default=5.0)

MONGO_URI            = _env("MONGO_URI",            required=False, default="")
MONGO_SAVE_INTERVAL  = _env("MONGO_SAVE_INTERVAL",  cast=int,   required=False, default=120)
INACTIVE_CLEANUP_DAYS= _env("INACTIVE_CLEANUP_DAYS",cast=int,   required=False, default=15)
MAX_USERS_SOFT_LIMIT = _env("MAX_USERS_SOFT_LIMIT", cast=int,   required=False, default=500)

RENDER_EXTERNAL_URL  = _env("RENDER_EXTERNAL_URL",  required=False, default="")
PORT                 = _env("PORT",                 cast=int,   required=False, default=7860)

SCAM_KEYWORDS = [
    'crypto pump', 'investment', 'adult', 'sex', 'nude',
    'free money', 'paisa kamao', 'doubling', 'carding', 'binance',
    'guaranteed profit', '100x', 'moon shot', 'get rich',
]

DEFAULT_SETTINGS = {
    "text": True, "image": True, "video": True, "caption": True,
    "voice": True, "files": True, "sticker": True, "gif": True, "poll": True,
    "remove_links": False, "remove_user": False, "smart_filter": False,
    "auto_shorten": False, "as_document": False, "copy_mode": False,
    "preview_mode": False, "start_msg": "", "end_msg": "",
    "duplicate_filter": False, "global_filter": False,
    "product_duplicate_filter": False, "smart_dup": False,
    "dup_expiry_hours": 2, "dup_whitelist_words": [],
    "custom_delay": 0, "delay_variance": 0, "running": False,
    "filter_mode": "Blacklist", "keywords": [],
    "keywords_blacklist": [], "keywords_whitelist": [],
    "keyword_filter_enabled": False, "link_blocker_enabled": False,
    "min_msg_length": 0, "max_msg_length": 0, "max_file_size_mb": 0,
    "require_media": False, "fwd_count_limit": 0,
    "dest_health_check": True, "language": "en",
    # ── v3 New Settings ────────────────────────────────
    "regex_filter_enabled": False,
    "quality_filter_enabled": False, "quality_min_score": 30,
    "min_links": 0, "max_links": 0,
    "mention_filter": "off",
    "forward_origin_filter": "off",
    "hashtag_required": [], "hashtag_blocked": [],
    "min_hashtags": 0, "max_hashtags": 0,
}

def get_default_forward_rules():
    return {
        "forward_text": True, "forward_photos": True, "forward_videos": True,
        "forward_files": True, "forward_voice": True, "forward_links": True,
        "forward_captions": True, "remove_usernames": False, "remove_hashtags": False,
        "replace_map": {}, "username_map": {}, "added_hashtags": [],
        "prefix": "", "suffix": "", "prefix_enabled": True, "suffix_enabled": True,
        "media_mode": "original", "link_mode": "keep", "custom_caption": None,
        "copy_mode": False, "pin_forwarded": False, "keyword_routes": [],
        "dest_enabled": True, "fail_count": 0, "disabled_reason": "",
        "delay_override": 0, "priority": 1, "max_retries": 3,
    }

try:
    from telethon import TelegramClient
    bot = TelegramClient('bot_ui_session', API_ID, API_HASH)
except Exception as e:
    logger.error(f"Failed to initialize Bot Client: {e}")
    sys.exit(1)

def print_startup_banner():
    mongo_status = "✅ Enabled" if MONGO_URI else "❌ Disabled (JSON only)"
    print(f"""
╔══════════════════════════════════════════════╗
║     🤖 AUTO FORWARD BOT v3.0  STARTING       ║
╠══════════════════════════════════════════════╣
║  API_ID    : {str(API_ID):<30} ║
║  OWNER_ID  : {str(OWNER_ID):<30} ║
║  Sessions  : {str(MAX_ACTIVE_SESSIONS):<30} ║
║  MongoDB   : {mongo_status:<30} ║
║  Workers   : {str(TOTAL_WORKERS):<30} ║
╚══════════════════════════════════════════════╝
""")
