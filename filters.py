import time
import asyncio
import re
import hashlib
import logging
from database import get_user_data, get_dup_data, get_prod_history, update_user_stats
from utils import generate_content_hash, get_canonical_product_id, get_canonical_product_id_async, extract_all_urls
from utils import _is_product_url, _is_short_link

logger = logging.getLogger(__name__)

# Per-user lock — concurrent messages ko queue mein daalo
_dup_locks: dict = {}

def _get_dup_lock(user_id):
    if user_id not in _dup_locks:
        _dup_locks[user_id] = asyncio.Lock()
    return _dup_locks[user_id]


def _text_similarity_hash(text: str) -> str:
    """
    Smart text hash — minor changes ignore karta hai:
    - Lowercase, strip spaces
    - Numbers/prices remove (500 → 600 same product)
    - Short words remove (less than 3 chars)
    Returns a "semantic" hash
    """
    if not text:
        return ""
    t = text.lower()
    t = re.sub(r"\d+", "", t)          # numbers hata do
    t = re.sub(r"http\S+", "", t)       # URLs hata do
    t = re.sub(r"[^\w\s]", " ", t)      # punctuation → space
    words = [w for w in t.split() if len(w) > 3]  # chhote words hatao
    canonical = " ".join(sorted(set(words[:20])))  # first 20 unique words, sorted
    return hashlib.md5(canonical.encode()).hexdigest() if canonical else ""


# ══════════════════════════════════════════
# 1. NORMAL MESSAGE DUPLICATE FILTER
# ══════════════════════════════════════════
def is_duplicate(user_id, event, source_id):
    """
    Single message duplicate check.
    FIX: smart_dup ab duplicate_filter ke bina bhi kaam karta hai -- dono
    independent settings hain. smart_dup ON hai to sirf text-based near-dup
    check hoga chahe duplicate_filter OFF bhi ho.
    """
    data     = get_user_data(user_id)
    settings = data["settings"]

    dup_filter_on = settings.get("duplicate_filter", False)
    smart_dup_on  = settings.get("smart_dup", False)

    # Dono OFF hain to kuch karna nahi
    if not dup_filter_on and not smart_dup_on:
        return False

    # Whitelist check (dono filters ke liye)
    text = (event.raw_text or "").lower()
    if text and settings.get("dup_whitelist_words"):
        if any(w.lower() in text for w in settings["dup_whitelist_words"]):
            return False

    # Global dup scope
    hash_source_id = "global_scope" if settings.get("global_filter") else source_id

    # Primary: exact content hash (sirf duplicate_filter ON hone par)
    msg_hash = generate_content_hash(event, hash_source_id) if dup_filter_on else None

    # Secondary: semantic text hash (smart_dup ke liye -- duplicate_filter OFF bhi ho to chalega)
    smart_hash = None
    raw = event.raw_text or ""
    if smart_dup_on and raw and len(raw) > 30:
        sh = _text_similarity_hash(raw)
        if sh:
            smart_hash = f"smart_{hash_source_id}_{sh}"

    dup_store = get_dup_data(user_id)
    now       = time.time()
    expiry    = settings.get("dup_expiry_hours", 2) * 3600

    def _is_dup_in_store(h):
        if not h or h not in dup_store["history"]:
            return False
        return now - dup_store["history"][h] < expiry

    if _is_dup_in_store(msg_hash) or _is_dup_in_store(smart_hash):
        dup_store.setdefault("blocked_log", [])
        dup_store["blocked_log"].append({
            "ts": int(now),
            "hash": msg_hash[:8] if msg_hash else "?",
            "text_preview": raw[:50] if raw else "[media]"
        })
        if len(dup_store["blocked_log"]) > 50:
            dup_store["blocked_log"] = dup_store["blocked_log"][-50:]
        update_user_stats(user_id, "blocked")
        try:
            from database import save_dup_data
            save_dup_data(user_id)
        except Exception:
            pass
        return True

    # Save hashes
    if msg_hash:
        dup_store["history"][msg_hash] = now
    if smart_hash:
        dup_store["history"][smart_hash] = now

    # RAM cleanup -- sirf expired entries hatao
    cutoff  = now - expiry
    expired = [k for k, v in list(dup_store["history"].items()) if v < cutoff]
    for k in expired:
        del dup_store["history"][k]

    # BUG FIX: Cap 5000 tak
    if len(dup_store["history"]) > 5000:
        history = dup_store["history"]
        stale = [k for k, v in list(history.items()) if now - v >= expiry]
        for k in stale:
            del history[k]
        if len(history) > 4000:
            remove_count = len(history) - 3000
            to_remove = list(history.keys())[:remove_count]
            for k in to_remove:
                del history[k]

    return False


# ══════════════════════════════════════════
# 2. PRODUCT DUPLICATE FILTER
# ══════════════════════════════════════════
async def check_product_duplicate(client, user_id, event):
    """
    Product-specific duplicate filter.

    ✅ FIX 1 — Non-product URLs ignored:
       Old code stored hashes for t.me, news sites, etc.
       t.me/channelname was same in every message → every 2nd message blocked.
       Now ONLY product domain URLs (amazon, flipkart, etc.) are checked.

    ✅ FIX 2 — Short link resolution handled:
       amzn.in short links that can't unshorten now get stable path-hash.
       Different short links = different hashes. Same link = same hash. 

    ✅ FIX 3 — Two-pass save (atomic):
       Pass 1: check if product is dup → don't save yet
       Pass 2: save new IDs only if NOT dup
       Prevents partial saves causing false positives on re-processing.
    """
    data = get_user_data(user_id)
    if not data["settings"].get("product_duplicate_filter"):
        return False
    # BUG FIX: premium gate — duplicate_filter premium feature hai
    # product_duplicate_filter bhi same premium feature ke under aata hai
    try:
        from forward_engine import _can_use_feature
        if not _can_use_feature(user_id, "duplicate_filter"):
            return False
    except Exception:
        pass

    now      = time.time()
    expiry   = data["settings"].get("dup_expiry_hours", 2) * 3600
    msg_text = (event.raw_text or "").strip()
    urls     = extract_all_urls(event)

    # _is_product_url already imported from utils at top of file
    product_urls = [u for u in urls if _is_product_url(u)]

    # FIX P2: concurrent URL resolution (LOCK ke BAHAR — no blocking)
    if product_urls:
        resolved = await asyncio.gather(
            *[get_canonical_product_id_async(u) for u in product_urls],
            return_exceptions=True
        )
        import urllib.parse as _up2
        final_ids = []
        for url, p_id in zip(product_urls, resolved):
            if isinstance(p_id, Exception) or not p_id:
                # Fallback: path-ONLY hash (query/tracking params strip)
                # Same product different affiliate tag → same path → same hash
                try:
                    _prs = _up2.urlparse(url)
                    _path_only = _prs.path.rstrip("/").lower()
                    p_id = f"prod_p_{hashlib.md5(_path_only.encode()).hexdigest()[:12]}"
                except Exception:
                    p_id = f"prod_{hashlib.md5(url.encode()).hexdigest()[:12]}"
            final_ids.append(p_id)
    else:
        final_ids = []

    # FIX P3: precise photo detection (not video/sticker thumbnails)
    photo_id = None
    if event.photo and not event.video and not event.video_note and not event.sticker:
        try:
            photo_id = event.photo.id
        except Exception:
            pass

    # Lock sirf DB read/write ke liye (fast, no network calls inside)
    async with _get_dup_lock(user_id):
        prod_db = get_prod_history(user_id)
        is_dup  = False

        # Pass 1: check
        new_product_ids = []
        for p_id in final_ids:
            if p_id in prod_db["links"]:
                if now - prod_db["links"][p_id] < expiry:
                    is_dup = True
                    break
                else:
                    new_product_ids.append(p_id)
            else:
                new_product_ids.append(p_id)

        # Pass 2: save only if not dup
        if not is_dup:
            for p_id in new_product_ids:
                prod_db["links"][p_id] = now

        # Photo check (FIX P3: only real photos)
        if not is_dup and photo_id:
            img_id = f"ph_{photo_id}"
            if img_id in prod_db["images"]:
                if now - prod_db["images"][img_id] < expiry:
                    is_dup = True
                else:
                    prod_db["images"][img_id] = now
            else:
                prod_db["images"][img_id] = now

        # Text check (only if no product URLs)
        if not is_dup and not product_urls and msg_text:
            text_key = f"txt_{hashlib.md5(msg_text[:200].strip().lower().encode()).hexdigest()[:12]}"
            if text_key in prod_db["texts"]:
                if now - prod_db["texts"][text_key] < expiry:
                    is_dup = True
                else:
                    prod_db["texts"][text_key] = now
            else:
                prod_db["texts"][text_key] = now

        if is_dup:
            update_user_stats(user_id, "blocked")
            logger.debug(f"Product dup blocked: user={user_id}")

        # FIX P4: sirf expired entries hatao, active kabhi nahi
        for cat in ["links", "images", "texts"]:
            d = prod_db.get(cat, {})
            if len(d) > 300:
                cutoff = now - expiry
                stale = [k for k, v in list(d.items()) if v < cutoff]
                for k in stale:
                    del d[k]
                if len(d) > 500:  # hard cap last resort
                    to_remove = list(d.keys())[:len(d) - 400]
                    for k in to_remove:
                        del d[k]

        return is_dup



# ══════════════════════════════════════════
# HELPER: Dup stats for UI
# ══════════════════════════════════════════
def get_dup_stats(user_id) -> dict:
    """Dup filter ka stats return karo — UI ke liye."""
    dup_store = get_dup_data(user_id)
    now       = time.time()
    data      = get_user_data(user_id)
    expiry    = data["settings"].get("dup_expiry_hours", 2) * 3600

    active    = sum(1 for v in dup_store["history"].values() if now - v < expiry)
    expired   = sum(1 for v in dup_store["history"].values() if now - v >= expiry)
    log       = dup_store.get("blocked_log", [])
    today_ts  = now - 86400
    today_blk = sum(1 for e in log if e.get("ts", 0) > today_ts)

    return {
        "active_entries": active,
        "expired_entries": expired,
        "total_entries": len(dup_store["history"]),
        "today_blocked": today_blk,
        "recent_log": log[-10:][::-1],   # last 10, newest first
    }


# ══════════════════════════════════════════════════════════════
# 3. ALBUM DUPLICATE FILTER  ← NEW
# ══════════════════════════════════════════════════════════════

def _album_media_hash(events_list: list, source_id) -> str | None:
    """
    Album ke saare media parts ka COMBINED fingerprint banao.

    Why: is_duplicate() sirf caption_event check karta tha — jisme only TEXT hota
    hai. Album ke actual photos/videos check hi nahi hote the. Isiliye same album
    dobara bheja jaata tha bina rok ke.

    Algorithm:
      1. Har event se media ID extract karo (photo.id / document.id + size)
      2. Sab IDs ko sorted order mein combine karo (order-independent)
      3. Caption text bhi include karo (partial — numbers remove karke)
      4. MD5 hash return karo

    Result: Same photos = same hash, chahe caption thoda alag ho.
    """
    import hashlib, re as _re
    components = [str(source_id), "album"]

    media_ids = []
    caption_text = ""

    for evt in events_list:
        if evt.media:
            if hasattr(evt.media, "photo") and evt.media.photo:
                media_ids.append(f"ph:{evt.media.photo.id}")
            elif hasattr(evt.media, "document") and evt.media.document:
                doc = evt.media.document
                media_ids.append(f"dc:{doc.id}:{doc.size}")
            elif hasattr(evt.media, "video") and evt.media.video:
                vid = evt.media.video
                media_ids.append(f"vi:{vid.id}")
        if not caption_text and evt.raw_text:
            t = evt.raw_text.lower()
            t = _re.sub(r"\d+", "", t)        # numbers hata do (price changes)
            t = _re.sub(r"[^\w\s]", " ", t)   # punctuation → space
            caption_text = " ".join(t.split()[:15])  # first 15 words

    if not media_ids:
        return None  # No media → use regular is_duplicate

    # Sort media_ids so order of album parts doesn't matter
    media_ids.sort()
    components.extend(media_ids)
    if caption_text:
        components.append(f"cap:{caption_text}")

    fingerprint = "|".join(components)
    return hashlib.md5(fingerprint.encode()).hexdigest()


def is_album_duplicate(user_id: int, events_list: list, source_id) -> bool:
    """
    Album ke liye duplicate check — media IDs pe based, caption pe nahi.

    Called from process_album_batch() BEFORE sending.
    Returns True  → album is duplicate, block karo
    Returns False → album is new, forward karo
    """
    data     = get_user_data(user_id)
    settings = data["settings"]

    if not settings.get("duplicate_filter"):
        return False

    # Whitelist: agar caption mein whitelist word hai → never block
    caption = ""
    for evt in events_list:
        if evt.raw_text:
            caption = evt.raw_text.lower()
            break
    if caption and settings.get("dup_whitelist_words"):
        if any(w.lower() in caption for w in settings["dup_whitelist_words"]):
            return False

    # Global vs per-source scope
    hash_source_id = "global_scope" if settings.get("global_filter") else source_id

    album_hash = _album_media_hash(events_list, hash_source_id)
    if not album_hash:
        # No media found → fallback to caption_event's normal is_duplicate
        caption_evt = next((e for e in events_list if e.raw_text), events_list[0])
        return is_duplicate(user_id, caption_evt, source_id)

    dup_store = get_dup_data(user_id)
    now       = time.time()
    expiry    = settings.get("dup_expiry_hours", 2) * 3600

    # ── Check ──────────────────────────────────────────────────────────
    if album_hash in dup_store["history"]:
        stored_ts = dup_store["history"][album_hash]
        if now - stored_ts < expiry:
            # ✅ DUPLICATE DETECTED
            dup_store.setdefault("blocked_log", []).append({
                "ts": int(now),
                "hash": album_hash[:8],
                "text_preview": f"[album {len(events_list)} parts]"
            })
            if len(dup_store["blocked_log"]) > 50:
                dup_store["blocked_log"] = dup_store["blocked_log"][-50:]
            update_user_stats(user_id, "blocked")
            try:
                from database import save_dup_data
                save_dup_data(user_id)
            except Exception:
                pass
            logger.debug(
                f"Album dup blocked: user={user_id} hash={album_hash[:8]} "
                f"parts={len(events_list)}"
            )
            return True

    # ── New album → save hash ──────────────────────────────────────────
    dup_store["history"][album_hash] = now

    # Also save individual media hashes (catches single-photo repost of album photo)
    for evt in events_list:
        single_hash = None
        if evt.media:
            if hasattr(evt.media, "photo") and evt.media.photo:
                single_hash = hashlib.md5(
                    f"{hash_source_id}|pho:{evt.media.photo.id}".encode()
                ).hexdigest()
            elif hasattr(evt.media, "document") and evt.media.document:
                doc = evt.media.document
                single_hash = hashlib.md5(
                    f"{hash_source_id}|doc:{doc.id}:{doc.size}".encode()
                ).hexdigest()
        if single_hash:
            dup_store["history"][single_hash] = now

    # Periodic expiry cleanup
    cutoff  = now - expiry
    expired = [k for k, v in list(dup_store["history"].items()) if v < cutoff]
    for k in expired:
        del dup_store["history"][k]

    if len(dup_store["history"]) > 2000:
        history = dup_store["history"]
        to_remove = list(history.keys())[:len(history) // 3]
        for k in to_remove:
            del history[k]

    return False
