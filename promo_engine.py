"""
promo_engine.py — Unified Promotion & Sponsorship Platform v1.0

Koi bhi cheez promote kar sakte ho:
  Telegram channels / groups / bots
  YouTube channels / videos
  Products (physical / digital)
  Companies / brands / services
  Apps / websites
  Audio / podcasts
  Facebook pages / Instagram
  A to Z kuch bhi

CAMPAIGN TYPES (ye ads_engine ke types se alag hain):
  "channel"   — Telegram channel/group/bot promo
  "youtube"   — YouTube channel ya video
  "product"   — Physical ya digital product
  "company"   — Brand / company / service
  "app"       — Mobile app / website
  "social"    — Facebook / Instagram / Twitter
  "audio"     — Podcast / music / audio content
  "video"     — Generic video content
  "event"     — Event / webinar / launch
  "custom"    — Kuch bhi aur

DELIVERY MODES (ads_engine se inherit + extend):
  inline  — Bot menu mein seamlessly dikhe (banner/button)
  popup   — Dedicated message (ad-free users ko nahi)
  blast   — Scheduled mass send
  pinned  — Bot start message mein dikhao

PRICING MODELS:
  flat    — Ek baar fixed price (e.g. ₹500 for 7 days)
  cpm     — Per 1000 impressions
  cpc     — Per click
  hybrid  — Flat + CPM combo

CAMPAIGN LIFECYCLE:
  draft → pending_payment → active → paused → completed / expired
"""

import asyncio
import logging
import time
import uuid
import hashlib
import random
from collections import defaultdict
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

PROMO_CATEGORIES = {
    "channel":  "📣 Telegram Channel/Group/Bot",
    "youtube":  "▶️ YouTube Channel/Video",
    "product":  "🛍 Product (physical/digital)",
    "company":  "🏢 Brand/Company/Service",
    "app":      "📱 App/Website",
    "social":   "📸 Social Media (FB/IG/Twitter)",
    "audio":    "🎙 Podcast/Music/Audio",
    "video":    "🎬 Video Content",
    "event":    "📅 Event/Webinar/Launch",
    "custom":   "✨ Custom/Other",
}

DELIVERY_MODES = {
    "banner":  "📢 Banner (menu mein text)",
    "button":  "🔘 Button (menu mein button)",
    "popup":   "📣 Pop-up (alag message)",
    "blast":   "⏰ Blast (sab users ko)",
    "pinned":  "📌 Pinned (start message mein)",
}

PRICING_MODELS = {
    "flat":   "💰 Flat Rate (fixed price)",
    "cpm":    "📊 CPM (per 1000 views)",
    "cpc":    "🖱 CPC (per click)",
    "hybrid": "🔀 Hybrid (flat + CPM)",
}

STATUSES = {
    "draft":           "📝 Draft",
    "pending_payment": "⏳ Payment Pending",
    "active":          "🟢 Active",
    "paused":          "⏸ Paused",
    "completed":       "✅ Completed",
    "expired":         "⌛ Expired",
    "rejected":        "❌ Rejected",
}

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG STORE
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_PROMO_CFG = {
    "enabled":          True,
    "campaigns":        {},          # {cid: campaign_dict}
    "packages":         {},          # {pkg_id: package_dict}
    "inquiry_log":      [],          # Sponsor inquiries
    "total_revenue":    0.0,
    "pending_revenue":  0.0,
    "paid_revenue":     0.0,
    "monthly_revenue":  {},
    "payout_log":       [],
    "auto_approve":     False,       # Admin approval required by default
    "contact_info":     "",          # Admin ka contact (telegram/email)
    "rate_card_msg":    "",          # Rate card message for inquiries
}

def _cfg() -> dict:
    from database import GLOBAL_STATE
    cfg = GLOBAL_STATE.setdefault("promo_config", {})
    for k, v in DEFAULT_PROMO_CFG.items():
        cfg.setdefault(k, v)
    return cfg

def _save():
    try:
        from database import save_persistent_db
        save_persistent_db()
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
# PRICING PACKAGES (admin sets these — sponsor books from these)
# ─────────────────────────────────────────────────────────────────────────────

def create_package(
    name: str,
    delivery_mode: str,
    duration_days: int,
    pricing_model: str,
    flat_price: float    = 0.0,
    cpm_rate: float      = 0.0,
    cpc_rate: float      = 0.0,
    max_impressions: int = 0,
    description: str     = "",
    popular: bool        = False,
) -> str:
    pkg_id = f"pkg_{uuid.uuid4().hex[:6]}"
    _cfg()["packages"][pkg_id] = {
        "id":            pkg_id,
        "name":          name,
        "delivery_mode": delivery_mode,
        "duration_days": duration_days,
        "pricing_model": pricing_model,
        "flat_price":    flat_price,
        "cpm_rate":      cpm_rate,
        "cpc_rate":      cpc_rate,
        "max_impressions": max_impressions,
        "description":   description,
        "popular":       popular,
        "active":        True,
        "created":       time.time(),
        "bookings":      0,
        "total_earned":  0.0,
    }
    _save()
    return pkg_id

def list_packages(active_only: bool = True) -> list[dict]:
    pkgs = list(_cfg()["packages"].values())
    if active_only:
        pkgs = [p for p in pkgs if p.get("active")]
    return sorted(pkgs, key=lambda p: p.get("flat_price", 0))

def get_package(pkg_id: str) -> dict | None:
    return _cfg()["packages"].get(pkg_id)

def delete_package(pkg_id: str) -> bool:
    if pkg_id in _cfg()["packages"]:
        del _cfg()["packages"][pkg_id]
        _save()
        return True
    return False

# ─────────────────────────────────────────────────────────────────────────────
# CAMPAIGN (one sponsored promotion)
# ─────────────────────────────────────────────────────────────────────────────

def create_campaign(
    category: str,
    package_id: str,
    title: str,
    promo_text: str,
    link: str            = "",
    btn_label: str       = "",
    sponsor_name: str    = "",
    sponsor_contact: str = "",
    image_url: str       = "",
    custom_price: float  = 0.0,   # 0 = use package price
    delivery_modes: list = None,  # override package delivery
    requested_by: int    = 0,     # user_id who requested
) -> str:
    pkg = get_package(package_id) or {}
    cid = f"cmp_{uuid.uuid4().hex[:8]}"
    price = custom_price if custom_price > 0 else pkg.get("flat_price", 0.0)
    dur   = pkg.get("duration_days", 7)

    _cfg()["campaigns"][cid] = {
        "id":              cid,
        "category":        category,
        "package_id":      package_id,
        "title":           title,
        "promo_text":      promo_text,
        "link":            link,
        "btn_label":       btn_label or "🔗 Visit",
        "sponsor_name":    sponsor_name,
        "sponsor_contact": sponsor_contact,
        "image_url":       image_url,
        "price":           price,
        "pricing_model":   pkg.get("pricing_model", "flat"),
        "cpm_rate":        pkg.get("cpm_rate", 0.0),
        "cpc_rate":        pkg.get("cpc_rate", 0.0),
        "delivery_modes":  delivery_modes or [pkg.get("delivery_mode", "popup")],
        "duration_days":   dur,
        "status":          "pending_payment",
        "requested_by":    requested_by,
        "created":         time.time(),
        "approved_at":     0.0,
        "starts_at":       0.0,
        "expires_at":      0.0,
        "impressions":     0,
        "clicks":          0,
        "earned":          0.0,
        "daily_stats":     {},   # {"YYYY-MM-DD": {"imp": N, "clk": N}}
        "ad_ids":          [],   # linked ads_engine ad IDs
        "payment_ref":     "",
        "notes":           "",
    }
    _save()
    logger.info(f"Campaign created: {cid} cat={category} sponsor={sponsor_name}")
    return cid

def get_campaign(cid: str) -> dict | None:
    return _cfg()["campaigns"].get(cid)

def list_campaigns(status: str = None) -> list[dict]:
    camps = list(_cfg()["campaigns"].values())
    if status:
        camps = [c for c in camps if c.get("status") == status]
    return sorted(camps, key=lambda c: c.get("created", 0), reverse=True)

def update_campaign(cid: str, **fields) -> bool:
    c = _cfg()["campaigns"].get(cid)
    if not c:
        return False
    c.update(fields)
    _save()
    return True

# ─────────────────────────────────────────────────────────────────────────────
# CAMPAIGN LIFECYCLE
# ─────────────────────────────────────────────────────────────────────────────

def approve_campaign(cid: str) -> bool:
    """Admin approve karo — campaign active ho jaata hai."""
    c = _cfg()["campaigns"].get(cid)
    if not c:
        return False
    now = time.time()
    c["status"]      = "active"
    c["approved_at"] = now
    c["starts_at"]   = now
    c["expires_at"]  = now + c.get("duration_days", 7) * 86400

    # Auto-create ads_engine entries for each delivery mode
    try:
        import ads_engine as AE
        for mode in c.get("delivery_modes", ["popup"]):
            ad_id = AE.create_ad(
                ad_type   = mode if mode in AE.AD_TYPES else "popup",
                title     = c["title"],
                text      = c["promo_text"],
                url       = c["link"],
                btn_label = c["btn_label"],
                sponsor   = c["sponsor_name"],
                cpm       = c.get("cpm_rate") or 0.0,
                weight    = 200,   # Paid campaigns get higher weight
            )
            c.setdefault("ad_ids", []).append(ad_id)
    except Exception as e:
        logger.error(f"approve_campaign ads_engine error: {e}")

    _save()

    # Revenue tracking
    cfg = _cfg()
    price = c.get("price", 0.0)
    cfg["total_revenue"]   = cfg.get("total_revenue", 0.0) + price
    cfg["pending_revenue"] = cfg.get("pending_revenue", 0.0) + price
    month = time.strftime("%Y-%m")
    cfg.setdefault("monthly_revenue", {})[month] = \
        cfg["monthly_revenue"].get(month, 0.0) + price

    logger.info(f"Campaign approved: {cid} ₹{price}")
    return True


def reject_campaign(cid: str, reason: str = "") -> bool:
    c = _cfg()["campaigns"].get(cid)
    if not c:
        return False
    c["status"] = "rejected"
    c["notes"]  = reason
    _save()
    return True


def pause_campaign(cid: str) -> bool:
    c = _cfg()["campaigns"].get(cid)
    if not c or c["status"] != "active":
        return False
    c["status"] = "paused"
    # Pause linked ads
    try:
        import ads_engine as AE
        for aid in c.get("ad_ids", []):
            ad = AE.get_ad(aid)
            if ad and ad.get("active"):
                AE.toggle_ad(aid)
    except Exception:
        pass
    _save()
    return True


def resume_campaign(cid: str) -> bool:
    c = _cfg()["campaigns"].get(cid)
    if not c or c["status"] != "paused":
        return False
    if time.time() > c.get("expires_at", 0):
        c["status"] = "expired"
        _save()
        return False
    c["status"] = "active"
    try:
        import ads_engine as AE
        for aid in c.get("ad_ids", []):
            ad = AE.get_ad(aid)
            if ad and not ad.get("active"):
                AE.toggle_ad(aid)
    except Exception:
        pass
    _save()
    return True


async def check_expired_campaigns():
    """Background: expire campaigns past their end date + prune old expired."""
    now = time.time()
    for cid, c in list(_cfg()["campaigns"].items()):
        if c["status"] == "active" and c.get("expires_at", 0) < now:
            c["status"] = "expired"
            try:
                import ads_engine as AE
                for aid in c.get("ad_ids", []):
                    AE.delete_ad(aid)
            except Exception:
                pass
            logger.info(f"Campaign expired: {cid}")

    # Prune expired campaigns older than 30 days to prevent unbounded growth
    cutoff = now - 30 * 86400
    stale  = [cid for cid, c in _cfg()["campaigns"].items()
              if c.get("status") in ("expired", "rejected")
              and c.get("expires_at", now) < cutoff]
    for cid in stale:
        del _cfg()["campaigns"][cid]
    if stale:
        logger.info(f"Pruned {len(stale)} old campaigns")
    _save()

# ─────────────────────────────────────────────────────────────────────────────
# IMPRESSION / CLICK TRACKING
# ─────────────────────────────────────────────────────────────────────────────

def record_promo_impression(cid: str):
    c = _cfg()["campaigns"].get(cid)
    if not c:
        return
    c["impressions"] = c.get("impressions", 0) + 1
    today = time.strftime("%Y-%m-%d")
    day = c.setdefault("daily_stats", {}).setdefault(today, {"imp": 0, "clk": 0})
    day["imp"] += 1

    # CPC/CPM earnings
    pm = c.get("pricing_model", "flat")
    if pm in ("cpm", "hybrid") and c.get("cpm_rate"):
        earn = c["cpm_rate"] / 1000.0
        c["earned"] = c.get("earned", 0.0) + earn

def record_promo_click(cid: str):
    c = _cfg()["campaigns"].get(cid)
    if not c:
        return
    c["clicks"] = c.get("clicks", 0) + 1
    today = time.strftime("%Y-%m-%d")
    c.setdefault("daily_stats", {}).setdefault(today, {"imp": 0, "clk": 0})["clk"] += 1

    if c.get("pricing_model") == "cpc" and c.get("cpc_rate"):
        earn = c["cpc_rate"]
        c["earned"] = c.get("earned", 0.0) + earn
        _cfg()["pending_revenue"] = _cfg().get("pending_revenue", 0.0) + earn

# ─────────────────────────────────────────────────────────────────────────────
# INQUIRY SYSTEM (sponsors bot se contact karte hain)
# ─────────────────────────────────────────────────────────────────────────────

def log_inquiry(user_id: int, category: str, msg: str) -> str:
    iid = f"inq_{uuid.uuid4().hex[:6]}"
    _cfg().setdefault("inquiry_log", []).append({
        "id":       iid,
        "user_id":  user_id,
        "category": category,
        "msg":      msg,
        "ts":       time.time(),
        "handled":  False,
    })
    if len(_cfg()["inquiry_log"]) > 500:
        _cfg()["inquiry_log"] = _cfg()["inquiry_log"][-300:]
    _save()
    return iid

def get_pending_inquiries() -> list[dict]:
    return [i for i in _cfg().get("inquiry_log", []) if not i.get("handled")]

def mark_inquiry_handled(iid: str):
    for i in _cfg().get("inquiry_log", []):
        if i["id"] == iid:
            i["handled"] = True
    _save()

# ─────────────────────────────────────────────────────────────────────────────
# ANALYTICS
# ─────────────────────────────────────────────────────────────────────────────

def get_promo_summary() -> dict:
    cfg   = _cfg()
    camps = cfg.get("campaigns", {})
    pkgs  = cfg.get("packages",  {})
    month = time.strftime("%Y-%m")

    active = [c for c in camps.values() if c.get("status") == "active"]
    pending_pay = [c for c in camps.values() if c.get("status") == "pending_payment"]

    total_imp = sum(c.get("impressions", 0) for c in camps.values())
    total_clk = sum(c.get("clicks",      0) for c in camps.values())
    ctr = round(total_clk / max(1, total_imp) * 100, 2)

    return {
        "total_campaigns":   len(camps),
        "active_campaigns":  len(active),
        "pending_approval":  len([c for c in camps.values() if c.get("status") == "draft"]),
        "pending_payment":   len(pending_pay),
        "expired":           len([c for c in camps.values() if c.get("status") == "expired"]),
        "total_packages":    len(pkgs),
        "total_impressions": total_imp,
        "total_clicks":      total_clk,
        "ctr":               ctr,
        "total_revenue":     round(cfg.get("total_revenue",   0.0), 2),
        "pending_revenue":   round(cfg.get("pending_revenue", 0.0), 2),
        "paid_revenue":      round(cfg.get("paid_revenue",    0.0), 2),
        "this_month":        round(cfg.get("monthly_revenue", {}).get(month, 0.0), 2),
        "pending_inquiries": len(get_pending_inquiries()),
        "enabled":           cfg.get("enabled", True),
    }

def get_campaign_analytics(cid: str) -> dict:
    c   = _cfg()["campaigns"].get(cid, {})
    imp = c.get("impressions", 0)
    clk = c.get("clicks",      0)
    ctr = round(clk / max(1, imp) * 100, 2)
    remaining = max(0, c.get("expires_at", 0) - time.time())
    days_left  = int(remaining / 86400)

    daily = c.get("daily_stats", {})
    last7 = []
    for i in range(6, -1, -1):
        d   = time.strftime("%Y-%m-%d", time.localtime(time.time() - i * 86400))
        day = daily.get(d, {"imp": 0, "clk": 0})
        last7.append((d[-5:], day["imp"], day["clk"]))

    return {
        "id":           cid,
        "title":        c.get("title", ""),
        "category":     c.get("category", ""),
        "status":       c.get("status", ""),
        "impressions":  imp,
        "clicks":       clk,
        "ctr":          ctr,
        "earned":       round(c.get("earned", 0.0), 2),
        "price":        c.get("price", 0.0),
        "days_left":    days_left,
        "starts_at":    c.get("starts_at", 0),
        "expires_at":   c.get("expires_at", 0),
        "last7":        last7,
        "delivery_modes": c.get("delivery_modes", []),
    }

def mark_payment_received(cid: str, ref: str = "") -> bool:
    c = _cfg()["campaigns"].get(cid)
    if not c:
        return False
    c["payment_ref"] = ref
    # Auto-approve if enabled
    if _cfg().get("auto_approve"):
        return approve_campaign(cid)
    else:
        c["status"] = "draft"  # Waiting admin approval
        _save()
        return True

def mark_payout(amount: float, note: str = ""):
    cfg = _cfg()
    cfg["paid_revenue"]    = cfg.get("paid_revenue", 0.0) + amount
    cfg["pending_revenue"] = max(0.0, cfg.get("pending_revenue", 0.0) - amount)
    cfg.setdefault("payout_log", []).append({"amount": amount, "note": note, "t": time.time()})
    _save()

# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND LOOP
# ─────────────────────────────────────────────────────────────────────────────

async def promo_maintenance_loop(bot):
    """Check expired campaigns, notify admins of pending items."""
    while True:
        await asyncio.sleep(300)   # FIX 17: 30m → 5m check interval (faster inquiry response)
        try:
            await check_expired_campaigns()
            # Notify admin of pending inquiries
            pending = get_pending_inquiries()
            if pending:
                try:
                    from config import OWNER_ID
                    from database import GLOBAL_STATE
                    admins = list(GLOBAL_STATE.get("admins", {}).keys())
                    for admin_id in admins[:2]:
                        await bot.send_message(
                            admin_id,
                            f"📣 **{len(pending)} new promo inquiries** pending!\n\n"
                            f"Admin panel → Promotions → Inquiries",
                            parse_mode="md"
                        )
                        break
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"promo_maintenance_loop error: {e}")
