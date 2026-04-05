# bot/source_tracker.py
# BUG 20 FIX: parse_start_args + record_user_source ab main.py ke /start handler mein call hoti hai
import time


def get_tracker_data():
    from database import GLOBAL_STATE
    GLOBAL_STATE.setdefault("source_tracker", {
        "enabled": True,
        "sources": {}
    })
    return GLOBAL_STATE["source_tracker"]


def parse_start_args(args: str) -> dict:
    """
    BUG 20 FIX: /start args parse karo.
    Formats:
      ref_123              → referral only
      src_ig               → source only
      ref_123_src_yt       → referral + source
      cmp_summer2026       → campaign
    """
    result = {"referrer_id": None, "source": None, "campaign": None}
    if not args:
        return result
    parts = args.split("_")
    i = 0
    while i < len(parts):
        if parts[i] == "ref" and i + 1 < len(parts):
            try:
                result["referrer_id"] = int(parts[i + 1])
            except ValueError:
                pass
            i += 2
        elif parts[i] == "src" and i + 1 < len(parts):
            result["source"] = parts[i + 1]
            i += 2
        elif parts[i] == "cmp" and i + 1 < len(parts):
            result["campaign"] = parts[i + 1]
            i += 2
        else:
            i += 1
    return result


def record_user_source(user_id: int, args: str):
    """BUG 20 FIX: User ka source record karo — main.py /start handler mein call hota hai."""
    from database import get_user_data, save_persistent_db
    tracker = get_tracker_data()
    if not tracker.get("enabled"):
        return
    parsed = parse_start_args(args)
    source = parsed.get("source") or "direct"
    sources = tracker.setdefault("sources", {})
    if source not in sources:
        sources[source] = {"count": 0, "label": source.title(), "first_seen": int(time.time())}
    sources[source]["count"] += 1
    sources[source]["last_seen"] = int(time.time())
    # User data mein bhi save karo (no immediate DB save — periodic save karega)
    data = get_user_data(user_id)
    data.setdefault("source_info", {})["origin"] = source
    # FIX 14: save_persistent_db() removed — saves 100 DB writes on 100 new users
    # database auto-saves periodically (debounced every 5s)


def get_source_stats() -> str:
    """Admin ke liye source tracking stats."""
    tracker = get_tracker_data()
    sources = tracker.get("sources", {})
    if not sources:
        return "📊 **Source Tracking**\n\nAbhi tak koi data nahi।"
    txt = "📊 **Source Tracking Stats**\n━━━━━━━━━━━━━━━━━━━━\n"
    total = sum(s["count"] for s in sources.values())
    for key, info in sorted(sources.items(), key=lambda x: x[1]["count"], reverse=True):
        pct = (info["count"] / total * 100) if total > 0 else 0
        txt += f"• **{info['label']}**: {info['count']} ({pct:.1f}%)\n"
    txt += f"\n**Total**: {total} users"
    return txt
