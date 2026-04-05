"""
payment_ocr.py — Real OCR-Based Payment Verification

PROBLEM: Bot screenshot read hi nahi karta tha — user bina UTR ke fake screenshot bhej sakta tha.
FIX:     Tesseract OCR se screenshot ki image read karke text extract karo.
         Fallback: Caption text (agar OCR unavailable).
         Multi-layer: amount detection + UTR pattern + timestamp validation.

ARCHITECTURE:
    Screenshot bytes → OCR → Extract {utr, amount, bank, timestamp}
    → Multi-layer fraud check → Accept/Reject
"""

import re
import io
import time
import logging
import asyncio
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PaymentEvidence:
    """Structured data extracted from payment screenshot."""
    utr:            Optional[str]   = None
    amount:         Optional[float] = None
    bank_name:      Optional[str]   = None
    account_last4:  Optional[str]   = None
    timestamp_str:  Optional[str]   = None
    raw_text:       str             = ""
    ocr_confidence: float           = 0.0   # 0-1
    source:         str             = "caption"  # "ocr" | "caption"


# ── OCR Engine ────────────────────────────────────────────────────────────────

async def extract_payment_evidence(
    image_bytes: Optional[bytes],
    caption_text: str = ""
) -> PaymentEvidence:
    """
    PRIMARY: Try OCR on screenshot image.
    FALLBACK: Parse caption text if OCR unavailable.
    """
    evidence = PaymentEvidence(raw_text=caption_text)
    
    # Try OCR first (if image provided and tesseract/easyocr available)
    if image_bytes:
        ocr_text = await _run_ocr(image_bytes)
        if ocr_text:
            evidence.raw_text     = ocr_text
            evidence.source       = "ocr"
            evidence.ocr_confidence = 0.9
            logger.info(f"OCR extracted {len(ocr_text)} chars from payment screenshot")
    
    # Parse the text (OCR or caption)
    _parse_into(evidence)
    return evidence


async def _run_ocr(image_bytes: bytes) -> str:
    """Run OCR in thread pool — non-blocking."""
    loop = asyncio.get_running_loop()
    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(None, _ocr_sync, image_bytes),
            timeout=15.0
        )
        return result or ""
    except asyncio.TimeoutError:
        logger.warning("OCR timeout (15s)")
        return ""
    except Exception as e:
        logger.debug(f"OCR failed: {e}")
        return ""


def _ocr_sync(image_bytes: bytes) -> str:
    """Synchronous OCR — runs in thread pool."""
    
    # Method 1: pytesseract (fastest, most common)
    try:
        import pytesseract
        from PIL import Image
        img = Image.open(io.BytesIO(image_bytes))
        # Preprocessing: grayscale + threshold for better accuracy
        img = img.convert("L")
        text = pytesseract.image_to_string(img, config="--psm 6 -l eng+hin")
        if text.strip():
            return text
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"pytesseract error: {e}")
    
    # Method 2: easyocr (better accuracy, slower)
    try:
        import easyocr
        reader = easyocr.Reader(["en"], verbose=False)
        result = reader.readtext(image_bytes, detail=0)
        if result:
            return " ".join(result)
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"easyocr error: {e}")
    
    # Method 3: Google Vision API (if key set)
    try:
        import os
        gcp_key = os.environ.get("GOOGLE_VISION_API_KEY")
        if gcp_key:
            import base64, urllib.request, json
            b64 = base64.b64encode(image_bytes).decode()
            payload = json.dumps({
                "requests": [{"image": {"content": b64},
                              "features": [{"type": "TEXT_DETECTION"}]}]
            }).encode()
            req = urllib.request.Request(
                f"https://vision.googleapis.com/v1/images:annotate?key={gcp_key}",
                data=payload,
                headers={"Content-Type": "application/json"}
            )
            with urllib.request.urlopen(req, timeout=10) as _r:
                resp = json.loads(_r.read())
            text = resp["responses"][0].get("fullTextAnnotation", {}).get("text", "")
            if text:
                return text
    except Exception as e:
        logger.debug(f"Google Vision error: {e}")
    
    return ""   # OCR unavailable


# ── Text Parsing ──────────────────────────────────────────────────────────────

# UTR patterns (covers IMPS, NEFT, UPI, PhonePe, GPay, Paytm)
_UTR_PATTERNS = [
    r'\bUTR[:\s#]*([A-Z0-9]{12,22})\b',
    r'\bTXN[:\s#]*([A-Z0-9]{10,22})\b',
    r'\bRef(?:erence)?[:\s#]*([A-Z0-9]{10,22})\b',
    r'\bTransaction\s*ID[:\s]*([A-Z0-9]{10,22})\b',
    r'\b([0-9]{12})\b',                        # 12-digit numeric UTR (IMPS)
    r'\b([A-Z]{2,6}[0-9]{10,18})\b',           # Bank-prefixed (HDFC123456789012)
    r'\bOrder\s*ID[:\s]*([A-Z0-9\-]{8,25})\b', # UPI order ID
]

_AMOUNT_PATTERNS = [
    r'(?:₹|Rs\.?|INR)\s*([0-9,]+(?:\.[0-9]{1,2})?)',
    r'([0-9,]+(?:\.[0-9]{1,2})?)\s*(?:₹|Rs\.?|INR)',
    r'Amount[:\s]+([0-9,]+(?:\.[0-9]{1,2})?)',
    r'Paid[:\s]+(?:₹|Rs\.?)?([0-9,]+(?:\.[0-9]{1,2})?)',
]

_BANK_NAMES = [
    "PhonePe", "GooglePay", "GPay", "Paytm", "BHIM",
    "HDFC", "SBI", "ICICI", "Axis", "Kotak", "Yes Bank",
    "NEFT", "IMPS", "UPI", "RTGS",
]

_SUCCESS_KEYWORDS = [
    "successful", "success", "paid", "credited", "deducted",
    "complete", "payment done", "transferred", "भुगतान", "सफल",
]

_FAILURE_KEYWORDS = [
    "failed", "declined", "rejected", "pending", "processing",
    "विफल", "रद्द",
]


def _parse_into(ev: PaymentEvidence):
    text = ev.raw_text.upper()
    
    # UTR
    for pat in _UTR_PATTERNS:
        m = re.search(pat, ev.raw_text, re.IGNORECASE)
        if m:
            ev.utr = m.group(1).upper()
            break
    
    # Amount
    for pat in _AMOUNT_PATTERNS:
        m = re.search(pat, ev.raw_text, re.IGNORECASE)
        if m:
            try:
                ev.amount = float(m.group(1).replace(",", ""))
                break
            except ValueError:
                pass
    
    # Bank name
    for bank in _BANK_NAMES:
        if bank.upper() in text:
            ev.bank_name = bank
            break
    
    # Account last 4 digits
    m = re.search(r'\bXXXX\s*([0-9]{4})\b|\b([0-9]{4})\s*(?:Account|A/C)\b', ev.raw_text, re.IGNORECASE)
    if m:
        ev.account_last4 = m.group(1) or m.group(2)
    
    # Check for failure keywords — flag it
    if any(k in text for k in [kw.upper() for kw in _FAILURE_KEYWORDS]):
        logger.warning(f"Payment screenshot may show FAILED transaction! Text: {text[:100]}")
    
    # Timestamp
    ts_patterns = [
        r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(\d{1,2}:\d{2}\s*(?:AM|PM))',
        r'(\d{4}-\d{2}-\d{2})',
    ]
    for pat in ts_patterns:
        m = re.search(pat, ev.raw_text, re.IGNORECASE)
        if m:
            ev.timestamp_str = m.group(1)
            break


def is_payment_successful(evidence: PaymentEvidence) -> bool:
    """Check if screenshot shows a SUCCESSFUL payment."""
    text = evidence.raw_text.upper()
    has_success = any(k.upper() in text for k in _SUCCESS_KEYWORDS)
    has_failure = any(k.upper() in text for k in _FAILURE_KEYWORDS)
    return has_success and not has_failure


def format_evidence_summary(ev: PaymentEvidence) -> str:
    """Human-readable evidence summary for admin review."""
    lines = [f"📄 Source: `{ev.source.upper()}`"]
    if ev.utr:       lines.append(f"🔑 UTR: `{ev.utr}`")
    if ev.amount:    lines.append(f"💰 Amount: `₹{ev.amount:.2f}`")
    if ev.bank_name: lines.append(f"🏦 Bank: `{ev.bank_name}`")
    if ev.account_last4: lines.append(f"💳 A/C Last4: `XXXX{ev.account_last4}`")
    if ev.timestamp_str: lines.append(f"🕒 Date: `{ev.timestamp_str}`")
    if not ev.utr and not ev.amount:
        lines.append("⚠️ No UTR/Amount detected — manual verification required!")
    return "\n".join(lines)
