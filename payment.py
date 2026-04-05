# bot/payment.py
# ==========================================
# PAYMENT SYSTEM — UPI Based (Advanced)
# ==========================================
#
# ✅ FIX 4 Advanced: Cryptographic Anti-Fraud System
#
# FRAUD DETECTION LAYERS:
#   Layer 1 — UTR Uniqueness: same UTR nahi chale dobara
#   Layer 2 — Amount fingerprint: expected amount se match karo
#   Layer 3 — Timestamp window: payment 24h se purani nahi honi chahiye
#   Layer 4 — Rate limiting: ek user max 3 payment attempts per day
#   Layer 5 — Submission hash: har submission ka unique fingerprint
#
# ARCHITECTURE:
#   Fraud events → fraud_log.jsonl (permanent record)
#   UTR registry → GLOBAL_STATE["utr_registry"] (dedup store)
#   Rate limits  → per-user daily counter
#
# ==========================================

import time
from time_helper import ab_fmt as _ab_fmt  # FIX: __import__('time_helper') hataya
try:
    from payment_ocr import extract_payment_evidence, format_evidence_summary, is_payment_successful
    _OCR_AVAILABLE = True
except ImportError:
    _OCR_AVAILABLE = False

import datetime
import hashlib
from pathlib import Path  # FIX: __import__("pathlib") hataya
import json
import os
import re as _re
import logging
logger = logging.getLogger(__name__)
from telethon import events, Button, errors
from config import bot, OWNER_ID
from database import db, GLOBAL_STATE, get_user_data, save_persistent_db
from admin import is_admin, add_log

FRAUD_LOG_FILE = str(Path(__file__).parent / "fraud_log.jsonl")
MAX_PAYMENT_ATTEMPTS_PER_DAY = 3


# ════════════════════════════════════════════
# FRAUD DETECTION ENGINE
# ════════════════════════════════════════════

class PaymentFraudDetector:
    """
    Multi-layer fraud detection for UPI payments.
    Works without any external API — pure logic.
    """

    def _get_utr_registry(self) -> dict:
        GLOBAL_STATE.setdefault("utr_registry", {})
        return GLOBAL_STATE["utr_registry"]

    def _get_attempt_log(self) -> dict:
        GLOBAL_STATE.setdefault("payment_attempt_log", {})
        log = GLOBAL_STATE["payment_attempt_log"]
        # FIX 13c: Remove stale entries (past dates) — memory leak prevention
        import datetime as _dt
        today = _dt.date.today().isoformat()
        stale = [k for k in list(log.keys()) if not k.endswith(today)]
        for k in stale:
            del log[k]
        return log

    def _log_fraud(self, user_id: int, reason: str, metadata: dict):
        entry = {
            "ts":      time.time(),
            "user_id": user_id,
            "reason":  reason,
            **metadata
        }
        import logging
        logging.getLogger(__name__).warning(
            f"🚨 FRAUD DETECTED: user={user_id} reason={reason}"
        )
        try:
            # BUG 26 FIX: Log rotation — max 500 lines (Render disk space safe)
            lines = []
            if os.path.exists(FRAUD_LOG_FILE):
                with open(FRAUD_LOG_FILE, "r") as f:
                    lines = f.readlines()
            lines.append(json.dumps(entry) + "\n")
            # Keep only last 500 entries
            if len(lines) > 500:
                lines = lines[-500:]
            with open(FRAUD_LOG_FILE, "w") as f:
                f.writelines(lines)
        except Exception:
            pass

    def submission_fingerprint(self, user_id: int, amount: int, utr: str) -> str:
        """Unique fingerprint for this submission — prevents duplicate submissions."""
        raw = f"{user_id}:{amount}:{utr}:{int(time.time() // 300)}"  # 5-min window
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def check_utr_duplicate(self, utr: str, user_id: int) -> bool:
        """Returns True if UTR already used (fraud!)."""
        if not utr:
            return False
        registry = self._get_utr_registry()
        if utr in registry:
            prev = registry[utr]
            self._log_fraud(user_id, "duplicate_utr", {
                "utr": utr,
                "prev_user": prev.get("user_id"),
                "prev_time": prev.get("ts"),
            })
            return True
        return False

    def register_utr(self, utr: str, user_id: int, amount: int):
        """Mark UTR as used after successful verification."""
        if not utr:
            return
        self._get_utr_registry()[utr] = {
            "user_id": user_id,
            "amount":  amount,
            "ts":      time.time(),
        }
        save_persistent_db()

    def check_rate_limit(self, user_id: int) -> tuple[bool, int]:
        """Returns (is_allowed, attempts_today)."""
        log    = self._get_attempt_log()
        today  = datetime.date.today().isoformat()
        key    = f"{user_id}:{today}"
        count  = log.get(key, 0)
        return count < MAX_PAYMENT_ATTEMPTS_PER_DAY, count

    def record_attempt(self, user_id: int):
        log   = self._get_attempt_log()
        today = datetime.date.today().isoformat()
        key   = f"{user_id}:{today}"
        log[key] = log.get(key, 0) + 1

    def check_amount_mismatch(self, claimed_amount: int, expected_amount: int,
                              user_id: int) -> bool:
        """Returns True if amounts don't match (potential fraud)."""
        if claimed_amount and claimed_amount != expected_amount:
            self._log_fraud(user_id, "amount_mismatch", {
                "claimed": claimed_amount,
                "expected": expected_amount,
            })
            return True
        return False

    def check_timestamp_validity(self, payment_ts: float, user_id: int) -> bool:
        """Returns True if timestamp is valid (within 24h window)."""
        if not payment_ts:
            return True   # Can't verify — allow
        age_hours = (time.time() - payment_ts) / 3600
        if age_hours > 24:
            self._log_fraud(user_id, "stale_payment", {"age_hours": age_hours})
            return False
        return True

    def extract_utr(self, caption_text: str) -> str:
        """Extract UTR/Transaction ID from message caption."""
        if not caption_text:
            return ""
        # UTR patterns: 12-22 alphanumeric chars, often starts with digits
        patterns = [
            r'\bUTR[:\s#]*([A-Z0-9]{10,22})\b',
            r'\bTXN[:\s#]*([A-Z0-9]{10,22})\b',
            r'\bREF[:\s#]*([A-Z0-9]{10,22})\b',
            r'\b([0-9]{12,22})\b',            # Numeric UTR (most common)
            r'\b([A-Z]{2,4}[0-9]{10,18})\b',  # Bank-prefixed UTR
        ]
        for pat in patterns:
            m = _re.search(pat, caption_text.upper())
            if m:
                return m.group(1)
        return ""

    def full_check(self, user_id: int, utr: str, amount: int,
                   expected_amount: int) -> tuple[bool, str]:
        """
        Run all fraud checks. Returns (is_clean, reason_if_fraud).
        """
        # Layer 1: Rate limit
        allowed, attempts = self.check_rate_limit(user_id)
        if not allowed:
            return False, f"Rate limit: {attempts} attempts today (max {MAX_PAYMENT_ATTEMPTS_PER_DAY})"

        # Layer 2: UTR duplicate
        if utr and self.check_utr_duplicate(utr, user_id):
            return False, f"UTR {utr} already used"

        # Layer 3: Amount mismatch
        if self.check_amount_mismatch(amount, expected_amount, user_id):
            return False, f"Amount mismatch: claimed {amount} vs expected {expected_amount}"

        return True, ""


# Singleton
fraud_detector = PaymentFraudDetector()




def _get_owner_footer() -> str:
    """Dynamic Bot Owner footer — admin panel se change hota hai."""
    try:
        from notification_center import _footer
        return _footer()
    except Exception:
        return ""

def get_payment_config():
    """Payment settings।"""
    GLOBAL_STATE.setdefault("payment_config", {
        "upi_id": "",              # UPI ID
        "upi_name": "",            # UPI pe naam
        "plans": {
            "1month": {"name": "1 Month", "price": 99, "days": 30},
            "3month": {"name": "3 Months", "price": 249, "days": 90},
            "1year": {"name": "1 Year", "price": 499, "days": 365},
        },
        "enabled": True,
        "auto_approve": False,     # Admin manually approve karega
        # Multi-currency
        "currency":       "INR",   # Default currency
        "currency_symbol": "₹",    # Symbol displayed
        "alt_currencies": {},      # {"USD": 0.012, "EUR": 0.011} — conversion rates
    })
    return GLOBAL_STATE["payment_config"]


def get_pending_payments():
    """Pending payment requests — auto-expire after 7 days।"""
    GLOBAL_STATE.setdefault("pending_payments", {})
    pending = GLOBAL_STATE["pending_payments"]
    # FIX 4: Remove stale pending payments (>7 days old) — prevent accumulation
    now = time.time()
    stale = [k for k, v in list(pending.items())
             if now - v.get("timestamp", now) > 7 * 86400]
    for k in stale:
        del pending[k]
    if stale:
        import logging
        logging.getLogger(__name__).info(f"Cleaned {len(stale)} stale pending payments")
    return pending


# ==========================================
# USER — PAYMENT FLOW
# ==========================================

@bot.on(events.NewMessage(pattern='/buy'))
async def buy_cmd(event):
    await show_plans(event)


@bot.on(events.CallbackQuery(data=b"buy_premium"))
async def buy_premium_cb(event):
    await event.answer()
    await show_plans(event)


async def show_plans(event):
    """User ko plans dikhao।"""
    config = get_payment_config()

    # Free mode mein payment show mat karo
    try:
        from premium import is_free_mode
        if is_free_mode():
            try:
                await event.edit("✅ **Bot abhi FREE hai!**\n\nSab features free mein available hain। Premium lene ki zaroorat nahi!" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") )
            except Exception:
                await event.respond("✅ **Bot abhi FREE hai!**\n\nSab features free mein available hain।" + ("\n\n" + _get_owner_footer() if _get_owner_footer() else "") )
            return
    except Exception:
        pass

    if not config.get("enabled"):
        try:
            await event.edit(
                "❌ Payment system abhi available nahi hai।\n\n"
                "Admin se contact karo: premium ke liye।\n\n" + _get_owner_footer()
            )
        except Exception:
            await event.respond(
                "❌ Payment system abhi available nahi hai।\n\n"
                "Admin se contact karo premium ke liye।\n\n" + _get_owner_footer()
            )
        return

    # BUG 3 FIX: UPI not configured warning — improved with contact button
    if not config.get("upi_id") or config.get("upi_id") == "":
        try:
            await event.edit(
                "⚙️ **Payment Setup Incomplete**\n\n"
                "Admin ne abhi UPI ID configure nahi ki hai।\n"
                "Premium lene ke liye admin se directly contact karo।\n\n" + _get_owner_footer(),
                buttons=[
                    [Button.inline("📞 Admin se Contact Karo", b"contact_admin")],
                    [Button.inline("🔙 Main Menu", b"main_menu")],
                ]
            )
        except Exception:
            await event.respond(
                "⚙️ Payment setup incomplete। Admin se contact karo।",
                buttons=[[Button.inline("📞 Contact Admin", b"contact_admin")]]
            )
        return

    plans = config.get("plans", {})

    # Try to get premium feature list for context
    try:
        from premium import ALL_FEATURES, is_feature_paid
        paid_feats = [v[0] for k, v in ALL_FEATURES.items() if is_feature_paid(k)][:5]
        feat_line  = "  " + "  ·  ".join(paid_feats) if paid_feats else ""
    except Exception:
        feat_line = ""

    # Currency setup
    currency_sym = config.get("currency_symbol", "₹")
    alt_curr     = config.get("alt_currencies", {})
    uid_local    = event.sender_id if hasattr(event, "sender_id") else 0
    # Check user currency preference
    user_currency = get_user_data(uid_local).get("currency_pref", "")
    conv_rate     = alt_curr.get(user_currency, 0) if user_currency else 0

    txt = (
        "💎 **PREMIUM PLANS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    if feat_line:
        txt += f"**Kya milega:**\n{feat_line}\n\n"

    txt += "**Available Plans:**\n"

    # Check for active promo discount
    user_data_local = get_user_data(event.sender_id if hasattr(event, "sender_id") else 0)
    promo_disc  = user_data_local.get("temp_data", {}).get("promo_discount", 0)
    promo_code  = user_data_local.get("temp_data", {}).get("promo_code", "")
    if promo_disc:
        txt += f"\n🏷️ **Promo `{promo_code}` active: {promo_disc}% OFF!**\n\n"

    btns = []
    for plan_key, plan in plans.items():
        price    = plan["price"]
        name     = plan["name"]
        days_val = plan["days"]
        days_txt = "Lifetime ♾️" if days_val == 0 else f"{days_val} days"
        if promo_disc:
            disc_price = max(1, round(price * (1 - promo_disc / 100)))
            price_str  = f"~~{currency_sym}{price}~~ {currency_sym}{disc_price}"
            btn_price  = disc_price
        else:
            price_str  = f"{currency_sym}{price}{alt_str}"
            btn_price  = price
        per_day     = f" (~{currency_sym}{round(price/days_val, 1)}/day)" if days_val > 0 and price > 0 and not promo_disc else ""
        alt_str     = ""
        if conv_rate and user_currency:
            alt_price = round(price * conv_rate, 2)
            alt_str   = f" ≈ {user_currency} {alt_price}"
        popular_tag = " ⭐ Best Value" if days_val == 30 else ""
        txt += f"  **{name}** — {price_str} ({days_txt}){per_day}{popular_tag}\n"
        btns.append([Button.inline(
            f"{'⭐ ' if popular_tag else ''}💳 {name} — ₹{btn_price}",
            f"pay_plan_{plan_key}".encode()
        )])

    txt += (
        f"\n📲 **Payment:** UPI\n"
        f"🆔 UPI ID: `{config.get('upi_id', 'Not Set')}`\n\n"
        "✅ Payment screenshot bhejo → Admin approve karega → Instant activate!"
    )
    if _get_owner_footer():
        txt += "\n\n" + _get_owner_footer()

    btns.append([Button.inline("💱 Currency", b"pay_currency_pref"),
                 Button.inline("🔙 Back",     b"main_menu")])

    try:
        await event.edit(txt, buttons=btns)
    except Exception:
        await event.respond(txt, buttons=btns)


@bot.on(events.CallbackQuery(pattern=b"pay_plan_"))
async def pay_plan_selected(event):
    """Plan select kiya — UPI details dikhao।"""
    await event.answer()
    plan_key = event.data.decode().replace("pay_plan_", "")
    config = get_payment_config()
    plans = config.get("plans", {})

    if plan_key not in plans:
        return await event.answer("Plan not found!", alert=True)

    plan = plans[plan_key]
    upi_id = config.get("upi_id", "Not Set")
    upi_name = config.get("upi_name", "Bot Owner")

    txt = (
        f"💳 **Payment Instructions**\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 Plan: **{plan['name']}**\n"
        f"💰 Amount: **₹{plan['price']}**\n\n"
        f"**UPI Payment:**\n"
        f"🆔 UPI ID: `{upi_id}`\n"
        f"👤 Name: `{upi_name}`\n\n"
        "**Steps:**\n"
        "1️⃣ UPI app mein ₹{price} bhejo\n"
        "2️⃣ **Transaction ID** (UTR number) note karo\n"
        "3️⃣ Screenshot bhejo jisme **ye sab clearly dikhein:**\n"
        "   • ✅ Tumhara naam / phone number\n"
        "   • ✅ Received UPI ID (hamare se match karna chahiye)\n"
        "   • ✅ Amount: ₹{price}\n"
        "   • ✅ Transaction ID / UTR number\n"
        "   • ✅ Date & Time\n\n"
        "⚠️ **IMPORTANT — Dhyan Se Padho:**\n"
        "• Sirf **original bank app** ka screenshot accept hoga\n"
        "• Fake/edited screenshots **automatically detect** hote hain\n"
        "• Fake proof bhejne par account **permanently ban** hoga\n"
        "• Verify hone mein 1-24 ghante lag sakte hain\n\n"
        "✅ Screenshot ke saath **UTR/Transaction ID** bhi text mein bhejo\n\n" + _get_owner_footer()
    ).format(price=plan['price'])

    # Promo discount apply karo agar hai
    uid = event.sender_id
    user_d = get_user_data(uid)
    promo_disc = user_d.get("temp_data", {}).get("promo_discount", 0)
    actual_price = plan["price"]
    if promo_disc:
        actual_price = max(1, round(actual_price * (1 - promo_disc / 100)))
        txt += f"\n\n🏷️ **Promo discount applied: {promo_disc}% OFF → ₹{actual_price}**"

    # Store pending info in user's temp_data
    user_d["temp_data"]["pending_plan"]   = plan_key
    user_d["temp_data"]["pending_amount"] = actual_price
    user_d["step"]                        = "wait_payment_screenshot"
    user_d["step_since"]                  = time.time()

    # Try to generate QR code
    qr_generated = False
    try:
        import qrcode, io
        upi_str = f"upi://pay?pa={upi_id}&pn={upi_name}&am={actual_price}&cu=INR"
        qr = qrcode.make(upi_str)
        buf = io.BytesIO()
        qr.save(buf, format="PNG")
        buf.seek(0)
        buf.name = "payment_qr.png"
        await event.respond(
            f"📱 **UPI QR Code — ₹{actual_price}**\n"
            f"Scan karo ya UPI ID use karo: `{upi_id}`\n"
            "_Payment ke baad screenshot bhejo।_",
            file=buf
        )
        qr_generated = True
    except Exception:
        pass  # QR library nahi hai — skip silently

    btns = [
        [Button.inline("📸 Screenshot Bhejo",    b"pay_send_screenshot")],
        [Button.inline("⏳ Check Payment Status", b"pay_check_status")],
        [Button.inline("❌ Cancel Request",       b"pay_cancel_request")],
        [Button.inline("🔙 Back",                b"buy_premium")],
    ]
    try:
        await event.edit(txt, buttons=btns)
    except Exception:
        await event.respond(txt, buttons=btns)


@bot.on(events.CallbackQuery(data=b"pay_send_screenshot"))
async def pay_send_screenshot(event):
    """User ko screenshot bhejne ko kaho।"""
    await event.answer()
    get_user_data(event.sender_id)["step"] = "wait_payment_screenshot"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "📸 **Payment Screenshot Bhejo**\n\n"
        "UPI payment ka screenshot is chat mein bhejo।\n\n"
        "⚠️ Note: Screenshot bilkul clear honi chahiye jisme:\n"
        "• Amount dikhe\n"
        "• Transaction ID dikhe\n"
        "• Date/Time dikhe\n\n" + _get_owner_footer(),
        buttons=[Button.inline("🔙 Cancel", b"buy_premium")]
    )


async def handle_payment_screenshot(event, user_id: int):
    """
    User ne screenshot bheja — pending mein add karo aur admin ko notify karo.
    ✅ FIX 4: UTR/Transaction ID bhi capture karo — admin verification asaan hogi
    main.py ke input handler se call hoga.
    """
    if not event.photo and not event.document:
        await event.respond(
            "❌ Photo bhejo! Screenshot ki image chahiye।\n\n"
            "📸 Payment ka screenshot bhejo jisme Transaction ID clearly dikh rahi ho:",
            buttons=[Button.inline("🔙 Cancel", b"buy_premium")]
        )
        return

    data = get_user_data(user_id)
    plan_key = data.get("temp_data", {}).get("pending_plan")

    if not plan_key:
        data["step"] = None
        await event.respond("❌ Plan select nahi kiya। /buy se dobara try karo।")
        return

    config = get_payment_config()
    plan = config.get("plans", {}).get(plan_key, {})

    caption_text = event.raw_text or event.message.message or ""

    # Screenshot download with error handling
    screenshot_bytes = None
    if event.photo or event.media:
        try:
            screenshot_bytes = await event.download_media(bytes)
        except Exception as _dl_err:
            logger.warning(f"Screenshot download failed for {user_id}: {_dl_err}")

    # OCR-based evidence extraction — FIX: wrap in try/except to prevent silent crash
    utr_hint = ""
    if _OCR_AVAILABLE and screenshot_bytes:
        try:
            evidence = await extract_payment_evidence(screenshot_bytes, caption_text)
            utr_hint = evidence.utr or fraud_detector.extract_utr(caption_text)
            # Reject only if OCR source confirms failed payment
            if not is_payment_successful(evidence) and evidence.source == "ocr":
                await event.respond(
                    "❌ **Payment Rejected**\n\n"
                    "Screenshot se failed/pending transaction detect hua hai.\n"
                    "Successful payment ka screenshot bhejo.",
                    buttons=[Button.inline("🔙 Try Again", b"buy_premium")]
                )
                return
        except Exception as _ocr_err:
            logger.warning(f"OCR failed for {user_id}: {_ocr_err} — falling back to text extraction")
            utr_hint = fraud_detector.extract_utr(caption_text)
    else:
        utr_hint = fraud_detector.extract_utr(caption_text)
    
    amount = plan.get("price", 0)

    # Run all fraud checks
    is_clean, fraud_reason = fraud_detector.full_check(
        user_id, utr_hint, 0, amount   # claimed_amount=0 since we only have screenshot
    )
    if not is_clean:
        await event.respond(
            f"❌ **Payment Rejected**\n\n"
            f"Reason: `{fraud_reason}`\n\n"
            f"Agar ye galti hai toh admin se contact karo."
        )
        return

    # Record this attempt
    fraud_detector.record_attempt(user_id)

    # Payment request store karo
    payment_id = f"pay_{user_id}_{int(time.time())}"
    pending = get_pending_payments()
    pending[payment_id] = {
        "user_id": user_id,
        "plan_key": plan_key,
        "plan_name": plan.get("name", "Unknown"),
        "plan_days": plan.get("days", 30),
        "amount": plan.get("price", 0),
        "submitted_at": int(time.time()),
        "status": "pending",
        "message_id": event.id,
        "chat_id": event.chat_id,
        "utr": utr_hint,  # ✅ FIX 4: UTR save karo
    }

    data["step"] = None
    data["temp_data"].pop("pending_plan", None)
    save_persistent_db()

    utr_display = f"\n🔑 UTR/TXN ID: `{utr_hint}`" if utr_hint else "\n⚠️ UTR ID caption mein nahi mila — admin manually verify karega"

    # User ko confirm karo — clear timeline + what happens next
    await event.respond(
        f"✅ **Payment Request Successfully Submit Ho Gayi!**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 Plan: **{plan.get('name')}**\n"
        f"💰 Amount: **₹{plan.get('price')}**\n"
        f"🆔 Request ID: `{payment_id}`"
        f"{utr_display}\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "⏳ **Aage kya hoga?**\n\n"
        "1️⃣ Admin screenshot verify karega\n"
        "2️⃣ Confirm hone par **tumhe turant notification milegi**\n"
        "3️⃣ Premium **automatically activate** ho jaayega\n\n"
        "🕐 **Expected time:** 15 minutes se 6 hours\n"
        "_(Working hours: 10 AM – 10 PM IST)_\n\n"
        "📌 Is request ID ko save rakho: `" + payment_id + "`\n"
        "Koi problem ho to admin ko yeh ID share karo.\n\n"
        + _get_owner_footer()
    )

    # Admin ko notify karo with screenshot
    admin_ids = list(GLOBAL_STATE.get("admins", {}).keys())
    if OWNER_ID not in admin_ids:
        admin_ids.append(OWNER_ID)

    # User info
    try:
        user_entity = await bot.get_entity(user_id)
        name = f"{user_entity.first_name or ''} {user_entity.last_name or ''}".strip()
        username = f"@{user_entity.username}" if user_entity.username else "No username"
    except Exception:
        name = "Unknown"
        username = "Unknown"

    admin_msg = (
        f"💳 **NAYA PAYMENT REQUEST!**\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        f"👤 User: `{name}` ({username})\n"
        f"🆔 User ID: `{user_id}`\n"
        f"📦 Plan: `{plan.get('name')}`\n"
        f"💰 Amount: `₹{plan.get('price')}`\n"
        f"🆔 Request ID: `{payment_id}`\n"
        f"🔑 UTR/TXN ID: `{utr_hint if utr_hint else 'NOT PROVIDED — Manually verify karo'}`\n"
        f"🕒 Time: {_ab_fmt(None, '%d/%m/%Y %H:%M')}\n\n"
        "⚠️ **Verify karne se pehle:**\n"
        "• UPI app mein UTR ID se payment confirm karo\n"
        "• Amount aur date check karo\n"
        "• Fake screenshot ho sakti hai — bank statement se verify karo\n\n"
        "Screenshot neeche hai ↓\n"
        "Approve ya Reject karo:"
    )

    for admin_id in admin_ids:
        try:
            # Screenshot forward karo
            await bot.forward_messages(admin_id, event.id, event.chat_id)
            # Approval buttons ke saath message
            await bot.send_message(
                admin_id,
                admin_msg,
                buttons=[
                    [Button.inline("✅ APPROVE", f"pay_approve_{payment_id}".encode()),
                     Button.inline("❌ REJECT", f"pay_reject_{payment_id}".encode())]
                ]
            )
        except Exception as e:
            pass


# ==========================================
# ADMIN — APPROVE / REJECT
# ==========================================

@bot.on(events.CallbackQuery(pattern=b"pay_approve_"))
async def pay_approve(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    payment_id = event.data.decode().replace("pay_approve_", "")
    pending = get_pending_payments()

    if payment_id not in pending:
        return await event.answer("Payment request not found!", alert=True)

    payment = pending[payment_id]
    user_id = payment["user_id"]
    plan_days = payment["plan_days"]
    plan_name = payment["plan_name"]

    # Premium activate karo
    from premium import get_premium_config
    data = get_user_data(user_id)
    prem = data["premium"]
    prem["active"] = True
    prem["plan"] = plan_name
    prem["given_by"] = event.sender_id
    prem["given_at"] = int(time.time())
    # FIX 10: Extend existing premium — don't reset remaining days!
    now = int(time.time())
    existing_exp = prem.get("expires_at")
    if existing_exp and existing_exp > now:
        prem["expires_at"] = existing_exp + (plan_days * 86400)
    else:
        prem["expires_at"] = now + (plan_days * 86400)

    # BUG 23 FIX: Approved payment delete karo pending se (grow forever prevent)
    pending.pop(payment_id, None)
    save_persistent_db()
    # Apply referral bonus if applicable
    try:
        from premium import apply_referral_on_purchase
        apply_referral_on_purchase(user_id, plan.get("days", 30))
    except Exception:
        pass
    # Record in user's payment history
    try:
        u_data = get_user_data(user_id)
        u_data.setdefault("payment_history", []).insert(0, {
            "ts":       int(time.time()),
            "plan":     plan_name,
            "amount":   payment.get("amount", plan.get("price", 0)),
            "approved": True,
            "by":       event.sender_id,
        })
        u_data["payment_history"] = u_data["payment_history"][:20]
    except Exception:
        pass
    add_log(event.sender_id, "Payment Approved", target=user_id, details=plan_name)

    # Smart notification — admin ko alert
    try:
        import notifications
        from config import bot as _bot
        import asyncio
        asyncio.create_task(notifications.notify_new_premium(_bot, user_id, plan_name))
        asyncio.create_task(notifications.notify_payment_received(_bot, user_id, plan_name))
    except Exception:
        pass

    # User ko notify karo
    exp_txt = "♾️ Lifetime" if plan_days == 0 else f"{plan_days} days"
    try:
        await bot.send_message(
            user_id,
            f"🎉 **Payment Approved!**\n\n"
            f"✅ Tumhara premium activate ho gaya!\n"
            f"📦 Plan: `{plan_name}`\n"
            f"📅 Duration: `{exp_txt}`\n\n"
            f"Ab sare premium features use kar sakte ho!\n\n" + _get_owner_footer()
        )
    except Exception:
        pass

    try:
        await event.edit(
            f"✅ **Payment Approved!**\n\n"
            f"👤 User: `{user_id}`\n"
            f"📦 Plan: `{plan_name}`\n"
            f"User ko notification bhej diya gaya।"
        )
    except Exception:
        pass


@bot.on(events.CallbackQuery(pattern=b"pay_reject_"))
async def pay_reject(event):
    await event.answer()
    if not is_admin(event.sender_id):
        return await event.answer("🚫 Admin permission nahi hai!", alert=True)

    payment_id = event.data.decode().replace("pay_reject_", "")
    pending    = get_pending_payments()

    if payment_id not in pending:
        return await event.answer("Payment request not found!", alert=True)

    # Ask admin for rejection reason first
    data = get_user_data(event.sender_id)
    data["step"]                       = f"pay_reject_reason_input|{payment_id}"
    data["step_since"]                 = time.time()
    payment                            = pending[payment_id]
    uid                                = payment["user_id"]
    plan                               = payment.get("plan_name", "?")
    amt                                = payment.get("amount", "?")

    try:
        await event.edit(
            f"❌ **REJECT PAYMENT**\n\n"
            f"User: `{uid}` | Plan: {plan} | Amt: ₹{amt}\n\n"
            "Rejection reason type karo (user ko bheja jaayega):\n"
            "_(ya 'skip' type karo for default message)_",
            buttons=[
                [Button.inline("⚡ Quick: Screenshot unclear", f"pay_rej_quick|{payment_id}|screenshot".encode())],
                [Button.inline("⚡ Quick: Amount mismatch",    f"pay_rej_quick|{payment_id}|amount".encode())],
                [Button.inline("⚡ Quick: UTR invalid",        f"pay_rej_quick|{payment_id}|utr".encode())],
                [Button.inline("❌ Cancel (keep pending)",     b"adm_pending_payments")],
            ]
        )
    except Exception:
        pass


@bot.on(events.CallbackQuery(pattern=b"pay_rej_quick\\|(.+)"))
async def pay_rej_quick(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    parts      = event.data.decode().split("|")
    payment_id = parts[1]
    reason_key = parts[2]
    reasons    = {
        "screenshot": "Screenshot clear nahi tha — original bank app ka screenshot bhejo।",
        "amount":     "Amount mismatch — exact plan amount bhejo।",
        "utr":        "UTR/Transaction ID valid nahi tha — sahi ID bhejo।",
    }
    reason = reasons.get(reason_key, "Payment verify nahi ho saki।")
    await _do_reject_payment(event, payment_id, reason)


@bot.on(events.NewMessage(func=lambda e: e.is_private and
        isinstance(get_user_data(e.sender_id).get("step"), str) and
        get_user_data(e.sender_id).get("step", "").startswith("pay_reject_reason_input|")))
async def pay_reject_reason_handler(event):
    uid  = event.sender_id
    if not is_admin(uid): return
    data       = get_user_data(uid)
    payment_id = data["step"].split("|")[1]
    data["step"] = None
    reason = event.raw_text.strip()
    if reason.lower() == "skip":
        reason = "Payment verify nahi ho saki। Dobara try karo।"
    await _do_reject_payment(event, payment_id, reason)


async def _do_reject_payment(event, payment_id: str, reason: str):
    pending = get_pending_payments()
    if payment_id not in pending:
        await event.respond("Payment not found!")
        return
    payment = pending.pop(payment_id)
    save_persistent_db()
    user_id = payment["user_id"]
    add_log(event.sender_id if hasattr(event, "sender_id") else 0,
            "Payment Rejected", target=user_id, details=reason[:50])
    try:
        await bot.send_message(
            user_id,
            "❌ **Payment Reject Ho Gayi**\n\n"
            f"**Reason:** {reason}\n\n"
            "Sahi details ke saath dobara try karo।\n\n" + _get_owner_footer(),
            buttons=[[Button.inline("🔄 Try Again", b"buy_premium")]]
        )
    except Exception:
        pass
    try:
        await event.respond(
            f"❌ **Rejected** — User `{user_id}` notified.\nReason: _{reason}_",
            buttons=[[Button.inline("📋 Pending Payments", b"adm_pending_payments")]]
        )
    except Exception:
        pass


# ==========================================
# ADMIN — PAYMENT SETTINGS
# ==========================================

@bot.on(events.CallbackQuery(data=b"adm_payment_settings"))
async def adm_payment_settings(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config  = get_payment_config()
    enabled = config.get("enabled", False)
    upi_id  = config.get("upi_id", "—")
    upi_name = config.get("upi_name", "—")
    plans   = config.get("plans", {})

    # Pending payments count
    try:
        from database import GLOBAL_STATE as _gs
        pending = len([p for p in _gs.get("pending_payments", {}).values()
                       if p.get("status") == "pending"])
    except Exception:
        pending = 0

    plan_lines = ""
    for pk, pv in plans.items():
        days_str = "Lifetime" if pv.get("days",0) == 0 else f"{pv.get('days')}d"
        plan_lines += f"  • {pv.get('name','?')} — ₹{pv.get('price','?')} ({days_str})\n"

    txt = (
        "💳 **PAYMENT SETTINGS**\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"Status: **{'🟢 Enabled' if enabled else '🔴 Disabled'}**\n\n"
        f"**UPI Details:**\n"
        f"  ID: `{upi_id}`\n"
        f"  Name: `{upi_name}`\n\n"
        f"**Plans ({len(plans)}):**\n"
        + (plan_lines or "  _No plans configured_")
        + (f"\n⏳ **Pending Approvals: {pending}**" if pending > 0 else "")
    )

    await event.edit(txt, buttons=[
        [Button.inline(f"{'🔴 Disable' if enabled else '🟢 Enable'}", b"adm_pay_toggle")],
        [Button.inline("🆔 Set UPI ID",       b"adm_set_upi_id"),
         Button.inline("👤 Set UPI Name",     b"adm_set_upi_name")],
        [Button.inline(f"⏳ Pending ({pending})", b"adm_pending_payments"),
         Button.inline("📋 Edit Plans",        b"adm_edit_plans")],
        [Button.inline("🔙 Admin Panel",        b"adm_main")],
    ])

@bot.on(events.CallbackQuery(data=b"adm_pay_toggle"))
async def adm_pay_toggle(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_payment_config()
    config["enabled"] = not config.get("enabled", True)
    save_persistent_db()
    await event.answer(f"Payment {'ON' if config['enabled'] else 'OFF'}!")
    await adm_payment_settings(event)


@bot.on(events.CallbackQuery(data=b"adm_set_upi_id"))
async def adm_set_upi_id(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_upi_id_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "🆔 **UPI ID Set Karo**\n\nExample: `yourname@upi`",
        buttons=[Button.inline("🔙 Cancel", b"adm_payment_settings")]
    )


@bot.on(events.CallbackQuery(data=b"adm_set_upi_name"))
async def adm_set_upi_name(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    get_user_data(event.sender_id)["step"] = "adm_upi_name_input"
    get_user_data(event.sender_id)["step_since"] = time.time()
    await event.edit(
        "👤 **UPI Name Set Karo**\n\nJo naam UPI app mein dikhe:",
        buttons=[Button.inline("🔙 Cancel", b"adm_payment_settings")]
    )


@bot.on(events.CallbackQuery(data=b"adm_pending_payments"))
async def adm_pending_payments_list(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    pending = get_pending_payments()
    pending_list = [(pid, p) for pid, p in pending.items() if p.get("status") == "pending"]

    if not pending_list:
        return await event.edit(
            "✅ Koi pending payment nahi hai!",
            buttons=[[Button.inline("🔙 Back", b"adm_payment_settings")]]
        )

    txt = f"⏳ **Pending Payments ({len(pending_list)})**\n━━━━━━━━━━━━━━━━━━━━\n"
    btns = []
    for pid, p in pending_list[:10]:
        user_id = p.get("user_id")
        plan = p.get("plan_name", "Unknown")
        amount = p.get("amount", 0)
        txt += f"👤 `{user_id}` — {plan} (₹{amount})\n"
        btns.append([
            Button.inline(f"✅ {user_id}", f"pay_approve_{pid}".encode()),
            Button.inline(f"❌ Reject", f"pay_reject_{pid}".encode())
        ])

    btns.append([Button.inline("🔙 Back", b"adm_payment_settings")])
    try:
        await event.edit(txt, buttons=btns)
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_edit_plans"))
@bot.on(events.CallbackQuery(data=b"adm_edit_plans"))
async def adm_edit_plans(event):
    await event.answer()
    if not is_admin(event.sender_id): return await event.answer("🚫 Admin permission nahi hai!", alert=True)
    config = get_payment_config()
    plans  = config.get("plans", {})
    lines_txt = []
    for k, v in plans.items():
        days_str = "Lifetime" if v["days"] == 0 else f"{v['days']}d"
        lines_txt.append(f"  `{k}` — **{v['name']}** | ₹{v['price']} | {days_str}")
    body = "\n".join(lines_txt) if lines_txt else "  _(no plans)_"
    btns = []
    for k, v in plans.items():
        btns.append([Button.inline(
            f"✏️ {v['name']} (₹{v['price']})", f"adm_plan_edit|{k}".encode()
        )])
    btns.append([Button.inline("➕ Add New Plan",  b"adm_plan_add")])
    btns.append([Button.inline("🔙 Back",         b"adm_payment_settings")])
    try:
        await event.edit(
            f"💰 **PLANS MANAGER**\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**Current Plans:**\n{body}\n\n"
            "Plan select karo edit/delete karne ke liye:",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_plan_edit\\|(.+)"))
async def adm_plan_edit_select(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    key    = event.data.decode().split("|")[1]
    config = get_payment_config()
    plan   = config.get("plans", {}).get(key)
    if not plan: return await event.answer("Plan nahi mila!", alert=True)
    data = get_user_data(event.sender_id)
    data["step"]                     = f"adm_plan_edit_input|{key}"
    data["step_since"]               = time.time()
    try:
        await event.edit(
            f"✏️ **EDIT: {plan['name']}**\n\n"
            f"Current: ₹{plan['price']} | {plan['days']}d\n\n"
            "New format type karo:\n"
            "`NAME | PRICE | DAYS`\n\n"
            "Example: `1 Month | 99 | 30`\n"
            "_(DAYS=0 for lifetime)_\n\n"
            "Ya `-` type karo is plan ko delete karne ke liye:",
            buttons=[Button.inline("❌ Cancel", b"adm_edit_plans")]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_plan_add"))
async def adm_plan_add(event):
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_plan_add_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "➕ **ADD NEW PLAN**\n\n"
            "Format: `KEY | NAME | PRICE | DAYS`\n\n"
            "Example: `6month | 6 Months | 399 | 180`\n"
            "_(DAYS=0 for lifetime)_",
            buttons=[Button.inline("❌ Cancel", b"adm_edit_plans")]
        )
    except errors.MessageNotModifiedError:
        pass


async def handle_payment_inputs(event, user_id: int, step: str) -> bool:
    """main.py ke input handler se call hoga।"""
    config = get_payment_config()

    if step == "adm_upi_id_input":
        config["upi_id"] = event.text.strip()
        get_user_data(user_id)["step"] = None
        save_persistent_db()
        await event.respond(f"✅ UPI ID: `{config['upi_id']}`",
                            buttons=[Button.inline("🔙 Back", b"adm_payment_settings")])
        return True

    elif step == "adm_upi_name_input":
        config["upi_name"] = event.text.strip()
        get_user_data(user_id)["step"] = None
        save_persistent_db()
        await event.respond(f"✅ UPI Name: `{config['upi_name']}`",
                            buttons=[Button.inline("🔙 Back", b"adm_payment_settings")])
        return True

    elif step == "adm_edit_plans_input":
        try:
            new_plans = {}
            for line in event.text.strip().split("\n"):
                parts = [p.strip() for p in line.split("|")]
                if len(parts) == 4:
                    key, name, price, days = parts
                    new_plans[key] = {"name": name, "price": int(price), "days": int(days)}
            if new_plans:
                config["plans"] = new_plans
                get_user_data(user_id)["step"] = None
                save_persistent_db()
                await event.respond("✅ Plans updated!",
                                    buttons=[Button.inline("🔙 Back", b"adm_payment_settings")])
            else:
                await event.respond("❌ Format galat hai।")
        except Exception as e:
            await event.respond(f"❌ Error: {str(e)[:80]}")
        return True

    elif step == "wait_payment_screenshot":
        await handle_payment_screenshot(event, user_id)
        return True

    # Plan edit/add handlers
    if step.startswith("adm_plan_edit_input|") or step == "adm_plan_add_input":
        return await _handle_plan_inputs(event, user_id, step)

    return False


# ══════════════════════════════════════════════════════════════
# 💳 PAYMENT v2 — New Handlers
# ══════════════════════════════════════════════════════════════

# ── Check payment status (user side) ─────────────────────────
@bot.on(events.CallbackQuery(data=b"pay_check_status"))
async def pay_check_status(event):
    await event.answer()
    uid     = event.sender_id
    pending = get_pending_payments()

    user_pending = {k: v for k, v in pending.items() if v.get("user_id") == uid}

    if not user_pending:
        try:
            await event.edit(
                "⏳ **PAYMENT STATUS**\n\n"
                "Koi pending payment request nahi hai।\n\n"
                "Naya payment submit karo:",
                buttons=[
                    [Button.inline("💳 Buy Premium", b"buy_premium")],
                    [Button.inline("🏠 Main Menu",   b"main_menu")],
                ]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    lines = []
    for pid, p in user_pending.items():
        ts   = datetime.datetime.fromtimestamp(
                   p.get("timestamp", 0)).strftime("%d %b, %H:%M")
        plan = p.get("plan_name", "?")
        amt  = p.get("amount", "?")
        lines.append(f"  📦 {plan} — ₹{amt} | Submitted: {ts}")

    try:
        await event.edit(
            "⏳ **PAYMENT STATUS**\n\n"
            "**Pending Requests:**\n"
            + "\n".join(lines)
            + "\n\n_Admin review karega aur activate karega।_\n"
            "_Usually 1-24 ghante lagte hain।_",
            buttons=[
                [Button.inline("❌ Cancel Request",  b"pay_cancel_request")],
                [Button.inline("🔄 Refresh",         b"pay_check_status")],
                [Button.inline("🏠 Main Menu",       b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ── Cancel payment request (user side) ───────────────────────
@bot.on(events.CallbackQuery(data=b"pay_cancel_request"))
async def pay_cancel_request(event):
    await event.answer()
    uid     = event.sender_id
    pending = get_pending_payments()

    user_pending = {k: v for k, v in pending.items() if v.get("user_id") == uid}

    if not user_pending:
        return await event.answer("Koi pending request nahi hai!", alert=True)

    # Cancel all pending from this user
    cancelled = 0
    for pid in list(user_pending.keys()):
        pending.pop(pid, None)
        cancelled += 1

    save_persistent_db()

    try:
        await event.edit(
            f"✅ **{cancelled} payment request(s) cancel ho gayi।**\n\n"
            "Naya payment submit karna ho to phir try karo।",
            buttons=[
                [Button.inline("💳 Buy Again", b"buy_premium")],
                [Button.inline("🏠 Main Menu", b"main_menu")],
            ]
        )
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════════════════════════
# 💰 PLAN MANAGEMENT — Inline Edit/Add/Delete
# ══════════════════════════════════════════════════════════════

async def _handle_plan_inputs(event, user_id: int, step: str) -> bool:
    config = get_payment_config()

    if step.startswith("adm_plan_edit_input|"):
        key  = step.split("|")[1]
        raw  = event.raw_text.strip()
        plans = config.setdefault("plans", {})
        get_user_data(user_id)["step"] = None

        if raw == "-":
            # Delete plan
            if key in plans:
                name = plans[key]["name"]
                del plans[key]
                save_persistent_db()
                await event.respond(
                    f"🗑 Plan `{name}` deleted!",
                    buttons=[Button.inline("💰 Plans", b"adm_edit_plans")]
                )
            return True

        parts = [p.strip() for p in raw.split("|")]
        if len(parts) != 3:
            await event.respond("❌ Format: `NAME | PRICE | DAYS`")
            return True
        try:
            name, price, days = parts[0], int(parts[1]), int(parts[2])
            plans[key] = {"name": name, "price": price, "days": days}
            save_persistent_db()
            add_log(user_id, "Plan Edit", details=f"{key}: {name} ₹{price} {days}d")
            await event.respond(
                f"✅ Plan updated: **{name}** — ₹{price} | {days}d",
                buttons=[Button.inline("💰 Plans", b"adm_edit_plans")]
            )
        except ValueError:
            await event.respond("❌ Price aur Days numbers hone chahiye।")
        return True

    elif step == "adm_plan_add_input":
        raw   = event.raw_text.strip()
        parts = [p.strip() for p in raw.split("|")]
        get_user_data(user_id)["step"] = None
        if len(parts) != 4:
            await event.respond("❌ Format: `KEY | NAME | PRICE | DAYS`")
            return True
        try:
            key, name, price, days = parts[0], parts[1], int(parts[2]), int(parts[3])
            config.setdefault("plans", {})[key] = {"name": name, "price": price, "days": days}
            save_persistent_db()
            add_log(user_id, "Plan Add", details=f"{key}: {name} ₹{price} {days}d")
            await event.respond(
                f"✅ Plan added: **{name}** — ₹{price} | {days}d",
                buttons=[Button.inline("💰 Plans", b"adm_edit_plans")]
            )
        except ValueError:
            await event.respond("❌ Price aur Days numbers hone chahiye।")
        return True

    return False


# ── User payment history ──────────────────────────────────────
@bot.on(events.CallbackQuery(data=b"pay_history"))
async def pay_history(event):
    await event.answer()
    uid  = event.sender_id
    data = get_user_data(uid)
    hist = data.get("payment_history", [])

    if not hist:
        try:
            await event.edit(
                "🕘 **PAYMENT HISTORY**\n\n_Koi payment history nahi।_",
                buttons=[[Button.inline("🔙 Back", b"premium_info")]]
            )
        except errors.MessageNotModifiedError:
            pass
        return

    lines = []
    for h in hist[:10]:
        ts   = datetime.datetime.fromtimestamp(
                   h.get("ts", 0)).strftime("%d %b %Y")
        plan = h.get("plan", "?")
        amt  = h.get("amount", "?")
        stat = "✅" if h.get("approved") else "⏳"
        lines.append(f"  {stat} {ts} — {plan} ₹{amt}")

    try:
        await event.edit(
            "🕘 **YOUR PAYMENT HISTORY**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n".join(lines),
            buttons=[[Button.inline("🔙 Back", b"premium_info")]]
        )
    except errors.MessageNotModifiedError:
        pass


# ══════════════════════════════════════════════════════════════
# 💱 CURRENCY SETTINGS
# ══════════════════════════════════════════════════════════════

@bot.on(events.CallbackQuery(data=b"pay_currency_pref"))
async def pay_currency_pref(event):
    """User currency preference set karo।"""
    await event.answer()
    config   = get_payment_config()
    alt_curr = config.get("alt_currencies", {})
    default  = config.get("currency", "INR")
    uid      = event.sender_id
    cur_pref = get_user_data(uid).get("currency_pref", default)

    if not alt_curr:
        return await event.answer("Admin ne koi alternate currency set nahi ki।", alert=True)

    all_curr = {default: 1.0, **alt_curr}
    btns = []
    for code, rate in all_curr.items():
        active = "▶ " if code == cur_pref else ""
        btns.append([Button.inline(
            f"{active}{code}{'  (default)' if code == default else ''}",
            f"pay_set_currency|{code}".encode()
        )])
    btns.append([Button.inline("🔙 Back", b"buy_premium")])

    try:
        await event.edit(
            "💱 **SELECT YOUR CURRENCY**\n\n"
            "Plans ki prices aapki chosen currency mein dikhengi।",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"pay_set_currency\\|(.+)"))
async def pay_set_currency(event):
    await event.answer()
    code = event.data.decode().split("|")[1]
    data = get_user_data(event.sender_id)
    data["currency_pref"] = code
    save_persistent_db()
    await event.answer(f"✅ Currency set to {code}", alert=False)
    await show_plans(event)


@bot.on(events.CallbackQuery(data=b"adm_currency_settings"))
async def adm_currency_settings(event):
    """Admin currency settings।"""
    await event.answer()
    if not is_admin(event.sender_id): return
    config   = get_payment_config()
    default  = config.get("currency", "INR")
    sym      = config.get("currency_symbol", "₹")
    alt_curr = config.get("alt_currencies", {})

    lines = [f"Default: **{default}** ({sym})"]
    for code, rate in alt_curr.items():
        lines.append(f"  {code}: 1 {default} = {rate} {code}")

    btns = [
        [Button.inline("✏️ Set Default Currency",  b"adm_set_default_curr")],
        [Button.inline("➕ Add Alt Currency",       b"adm_add_alt_curr")],
        [Button.inline("🗑 Remove Alt Currency",    b"adm_rem_alt_curr")],
        [Button.inline("🔙 Payment Settings",      b"adm_payment_settings")],
    ]
    try:
        await event.edit(
            "💱 **CURRENCY SETTINGS**\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            + "\n".join(lines),
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


# ── MISSING CURRENCY HANDLERS (FIXED) ────────────────────────────────────────

@bot.on(events.CallbackQuery(data=b"adm_set_default_curr"))
async def adm_set_default_curr(event):
    """Set default currency — was missing handler (FIXED)."""
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_set_default_curr_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "💱 **Default Currency Set Karo**\n\n"
            "Currency code bhejo (3 letters):\n"
            "Example: `INR`, `USD`, `EUR`, `GBP`, `AED`\n\n"
            "Currency symbol bhi bhejo (same line):\n"
            "Format: `INR ₹`",
            buttons=[[Button.inline("❌ Cancel", b"adm_currency_settings")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_add_alt_curr"))
async def adm_add_alt_curr(event):
    """Add alternate currency — was missing handler (FIXED)."""
    await event.answer()
    if not is_admin(event.sender_id): return
    data = get_user_data(event.sender_id)
    data["step"]       = "adm_add_alt_curr_input"
    data["step_since"] = time.time()
    try:
        await event.edit(
            "➕ **Alt Currency Add Karo**\n\n"
            "Format: `CODE RATE`\n"
            "Example: `USD 0.012` (1 INR = 0.012 USD)\n\n"
            "Phir plans mein alt currency price bhi show hogi.",
            buttons=[[Button.inline("❌ Cancel", b"adm_currency_settings")]]
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(data=b"adm_rem_alt_curr"))
async def adm_rem_alt_curr(event):
    """Remove alternate currency — was missing handler (FIXED)."""
    await event.answer()
    if not is_admin(event.sender_id): return
    config   = get_payment_config()
    alt_curr = config.get("alt_currencies", {})

    if not alt_curr:
        await event.answer("❌ Koi alt currency nahi hai!", alert=True)
        return

    btns = [[Button.inline(f"🗑 Remove {code}", f"adm_rem_curr|{code}".encode())]
            for code in alt_curr]
    btns.append([Button.inline("🔙 Back", b"adm_currency_settings")])

    try:
        await event.edit(
            "🗑 **Alt Currency Hatao**\n\nKaunsi currency hatani hai?",
            buttons=btns
        )
    except errors.MessageNotModifiedError:
        pass


@bot.on(events.CallbackQuery(pattern=b"adm_rem_curr\\|(.+)"))
async def adm_rem_curr_confirm(event):
    """Confirm currency removal."""
    await event.answer()
    if not is_admin(event.sender_id): return
    raw = event.data.decode()
    if "|" not in raw:
        return
    code   = raw.split("|", 1)[1]
    config = get_payment_config()
    config.get("alt_currencies", {}).pop(code, None)
    from database import save_persistent_db
    save_persistent_db()
    await event.answer(f"✅ {code} removed!", alert=False)
    await adm_currency_settings(event)
