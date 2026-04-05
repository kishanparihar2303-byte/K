"""
affiliate.py — Production-Level Affiliate Engine v2

Features:
- Amazon, Flipkart, Meesho, Myntra, Snapdeal, Ajio, Nykaa
- Real-time stats tracking (links replaced, estimated earnings)
- URL expansion for short links (amzn.to, fkrt.it)
- Test mode — URL test karo bina forward kiye
- Per-platform enable/disable
- Smart HTML-safe URL injection
- Async short link resolution
"""

import re
import logging
import time
import hashlib
from abc import ABC, abstractmethod
from urllib.parse import urlparse, urlunparse, urlencode, parse_qs
from feature_flags import affiliate_available
from feature_flags import get_flag

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════
# STATS TRACKING
# ═══════════════════════════════════════════════════════

def _get_affiliate_stats(user_id: int) -> dict:
    from database import get_user_data
    data = get_user_data(user_id)
    data.setdefault("affiliate_stats", {
        "total_replaced":  0,
        "amazon_replaced": 0,
        "flipkart_replaced": 0,
        "meesho_replaced": 0,
        "myntra_replaced": 0,
        "snapdeal_replaced": 0,
        "ajio_replaced": 0,
        "nykaa_replaced": 0,
        "last_replaced_at": 0,
        "today": {},   # {date: count}
    })
    return data["affiliate_stats"]


def _track_replacement(user_id: int, platform: str, count: int = 1):
    try:
        stats = _get_affiliate_stats(user_id)
        stats["total_replaced"]           = stats.get("total_replaced", 0) + count
        stats[f"{platform}_replaced"]     = stats.get(f"{platform}_replaced", 0) + count
        stats["last_replaced_at"]         = int(time.time())
        today = time.strftime("%Y-%m-%d")
        stats.setdefault("today", {})[today] = stats["today"].get(today, 0) + count
        # Keep only last 7 days
        if len(stats["today"]) > 7:
            oldest = sorted(stats["today"].keys())[0]
            del stats["today"][oldest]
    except Exception:
        pass


# ═══════════════════════════════════════════════════════
# URL EXPANSION (short links)
# ═══════════════════════════════════════════════════════

_expand_cache: dict = {}

def _expand_amazon_short_link(url: str) -> str:
    """Sync expand — uses cache."""
    if url in _expand_cache:
        return _expand_cache[url]
    try:
        import urllib.request
        req = urllib.request.Request(url, method="HEAD")
        req.add_header("User-Agent", "Mozilla/5.0")
        with urllib.request.urlopen(req, timeout=3) as r:
            final = r.url
            _expand_cache[url] = final
            return final
    except Exception:
        return url


async def _expand_url_async(url: str) -> str:
    """Async expand for better performance."""
    if url in _expand_cache:
        return _expand_cache[url]
    try:
        import aiohttp
        timeout = aiohttp.ClientTimeout(total=4)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            async with s.head(url, allow_redirects=True,
                              headers={"User-Agent": "Mozilla/5.0"}) as r:
                final = str(r.url)
                _expand_cache[url] = final
                return final
    except Exception:
        return url


# ═══════════════════════════════════════════════════════
# PLUGIN BASE
# ═══════════════════════════════════════════════════════

class AffiliatePlugin(ABC):
    # Class attributes - subclasses define these as class vars
    name:    str = ""
    pattern = None
    tag_key: str = ""
    icon:    str = "🔗"
    example: str = ""

    def matches(self, url: str) -> bool:
        return bool(self.pattern.search(url))

    @abstractmethod
    def inject(self, url: str, tag: str) -> str: ...

    def get_tag_key(self) -> str:
        return self.tag_key

    def process(self, text: str, tag: str, user_id: int = None) -> tuple[str, int]:
        """Returns (modified_text, count_replaced)."""
        if not tag:
            return text, 0
        count = [0]
        def _replace(m):
            try:
                result = self.inject(m.group(0), tag)
                if result != m.group(0):
                    count[0] += 1
                return result
            except Exception as e:
                logger.debug(f"{self.name}: inject error: {e}")
                return m.group(0)
        modified = self.pattern.sub(_replace, text)
        if count[0] > 0 and user_id:
            _track_replacement(user_id, self.name, count[0])
        return modified, count[0]


# ═══════════════════════════════════════════════════════
# PLATFORM IMPLEMENTATIONS
# ═══════════════════════════════════════════════════════

class AmazonAffiliate(AffiliatePlugin):
    name    = "amazon"
    tag_key = "amazon_tag"
    icon    = "🛒"
    example = "kishandeals-21"
    pattern = re.compile(
        r'https?://(?:www\.)?(?:amazon\.in|amzn\.to|amzn\.in)(?:/[^\s<>"]*[^\s<>".,!?;:\'])?',
        re.IGNORECASE
    )

    def inject(self, url: str, tag: str) -> str:
        import html as _html
        # Step 1: HTML-encoded &amp; ko proper & mein convert karo (source channels se aata hai)
        url = _html.unescape(url)
        if "amzn.to" in url or ("amzn.in" in url and "/dp/" not in url):
            expanded = _expand_amazon_short_link(url)
            if expanded and "amazon." in expanded:
                url = _html.unescape(expanded)
            else:
                sep = "&" if "?" in url else "?"
                return re.sub(r'tag=[^&]*', f'tag={tag}', url) if "tag=" in url \
                       else f"{url}{sep}tag={tag}"
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        # Step 2: Apna tag set karo — purana tag (source ka) replace ho jayega
        params["tag"] = [tag]
        # Step 3: Sab junk/tracking params hatao — ck bhi hatao jo pehle ka tag hold karta tha
        for k in ["ref", "ref_", "pf_rd_r", "pf_rd_p", "pd_rd_wg", "linkCode", "linkId", "ck"]:
            params.pop(k, None)
        return urlunparse(parsed._replace(
            query=urlencode({k: v[0] for k, v in params.items()})
        ))


class FlipkartAffiliate(AffiliatePlugin):
    name    = "flipkart"
    tag_key = "flipkart_id"
    icon    = "🛍️"
    example = "your_affiliate_id"
    pattern = re.compile(
        r'https?://(?:www\.)?(?:flipkart\.com|fkrt\.it|dl\.flipkart\.com)(?:/[^\s<>"]*)?',
        re.IGNORECASE
    )

    def inject(self, url: str, tag: str) -> str:
        if "fkrt.it" in url:
            expanded = _expand_amazon_short_link(url)
            if "flipkart.com" in expanded:
                url = expanded
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["affid"] = [tag]
        params["affExtParam1"] = ["ktbot"]
        return urlunparse(parsed._replace(
            query=urlencode({k: v[0] for k, v in params.items()})
        ))


class MeeshoAffiliate(AffiliatePlugin):
    name    = "meesho"
    tag_key = "meesho_ref"
    icon    = "👗"
    example = "your_referral_code"
    pattern = re.compile(
        r'https?://(?:www\.)?meesho\.com(?:/[^\s<>"]*)?',
        re.IGNORECASE
    )

    def inject(self, url: str, tag: str) -> str:
        sep = "&" if "?" in url else "?"
        if "referral_code=" in url:
            return re.sub(r'referral_code=[^&]*', f'referral_code={tag}', url)
        return f"{url}{sep}referral_code={tag}"


class MyntraAffiliate(AffiliatePlugin):
    name    = "myntra"
    tag_key = "myntra_id"
    icon    = "👠"
    example = "your_campaign_id"
    pattern = re.compile(
        r'https?://(?:www\.)?myntra\.com(?:/[^\s<>"]*)?',
        re.IGNORECASE
    )

    def inject(self, url: str, tag: str) -> str:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["at_medium"]   = ["affiliate"]
        params["at_campaign"] = [tag]
        params["at_custom1"]  = ["ktbot"]
        return urlunparse(parsed._replace(
            query=urlencode({k: v[0] for k, v in params.items()})
        ))


class AjioAffiliate(AffiliatePlugin):
    name    = "ajio"
    tag_key = "ajio_id"
    icon    = "👔"
    example = "your_ajio_id"
    pattern = re.compile(
        r'https?://(?:www\.)?ajio\.com(?:/[^\s<>"]*)?',
        re.IGNORECASE
    )

    def inject(self, url: str, tag: str) -> str:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["utm_source"]   = ["affiliate"]
        params["utm_medium"]   = [tag]
        params["utm_campaign"] = ["ktbot"]
        return urlunparse(parsed._replace(
            query=urlencode({k: v[0] for k, v in params.items()})
        ))


class NykaaAffiliate(AffiliatePlugin):
    name    = "nykaa"
    tag_key = "nykaa_id"
    icon    = "💄"
    example = "your_nykaa_id"
    pattern = re.compile(
        r'https?://(?:www\.)?(?:nykaa\.com|nykaafashion\.com)(?:/[^\s<>"]*)?',
        re.IGNORECASE
    )

    def inject(self, url: str, tag: str) -> str:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["utm_source"]   = ["affiliate"]
        params["utm_medium"]   = [tag]
        params["utm_campaign"] = ["ktbot"]
        return urlunparse(parsed._replace(
            query=urlencode({k: v[0] for k, v in params.items()})
        ))


class SnapdealAffiliate(AffiliatePlugin):
    name    = "snapdeal"
    tag_key = "snapdeal_id"
    icon    = "📦"
    example = "your_snapdeal_id"
    pattern = re.compile(
        r'https?://(?:www\.)?snapdeal\.com(?:/[^\s<>"]*)?',
        re.IGNORECASE
    )

    def inject(self, url: str, tag: str) -> str:
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["utm_source"]   = ["affiliate"]
        params["utm_medium"]   = [tag]
        return urlunparse(parsed._replace(
            query=urlencode({k: v[0] for k, v in params.items()})
        ))


# ═══════════════════════════════════════════════════════
# REGISTRY
# ═══════════════════════════════════════════════════════

class AffiliateRegistry:
    def __init__(self):
        self._plugins: list[AffiliatePlugin] = []

    def register(self, plugin: AffiliatePlugin):
        self._plugins.append(plugin)

    def get_plugin(self, name: str) -> AffiliatePlugin | None:
        return next((p for p in self._plugins if p.name == name), None)

    def process_text(self, text: str, settings: dict, user_id: int = None) -> tuple[str, int]:
        """Returns (modified_text, total_replacements)."""
        if not text or not settings:
            return text, 0
        result        = text
        total_replaced = 0
        disabled = settings.get("disabled_platforms", [])
        for plugin in self._plugins:
            if plugin.name in disabled:
                continue
            tag = settings.get(plugin.get_tag_key(), "")
            if tag:
                result, count = plugin.process(result, tag, user_id)
                total_replaced += count
        return result, total_replaced

    def test_url(self, url: str, settings: dict) -> dict:
        """Test URL se affiliate result dikhao."""
        results = {}
        for plugin in self._plugins:
            if plugin.matches(url):
                tag = settings.get(plugin.get_tag_key(), "")
                if tag:
                    try:
                        modified = plugin.inject(url, tag)
                        results[plugin.name] = {
                            "original": url,
                            "modified": modified,
                            "changed":  modified != url,
                            "icon":     plugin.icon,
                        }
                    except Exception as e:
                        results[plugin.name] = {"error": str(e)}
        return results

    def count_links(self, text: str) -> dict:
        return {p.name: len(p.pattern.findall(text or "")) for p in self._plugins}

    def list_platforms(self) -> list[AffiliatePlugin]:
        return self._plugins


# Build global registry
registry = AffiliateRegistry()
registry.register(AmazonAffiliate())
registry.register(FlipkartAffiliate())
registry.register(MeeshoAffiliate())
registry.register(MyntraAffiliate())
registry.register(AjioAffiliate())
registry.register(NykaaAffiliate())
registry.register(SnapdealAffiliate())


# ═══════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════

def process_affiliate_links(text: str, affiliate_settings: dict, user_id: int = None) -> str:
    result, _ = registry.process_text(text, affiliate_settings, user_id)
    return result


def get_user_affiliate_settings(user_id: int) -> dict | None:
    """
    User ke affiliate settings lo.

    Commission logic (free users ke liye):
    - Admin ek % set karta hai (default 30%)
    - Har message pe random roll hota hai
    - Roll < commission_rate  → admin ka tag use hoga
    - Roll >= commission_rate → user ka apna tag use hoga
    - Premium users: hamesha unka apna tag, koi commission nahi
    """
    try:
        from feature_flags import get_flag, affiliate_available
        if not affiliate_available(user_id):
            return None

        mode = get_flag("affiliate_mode") or "user"

        if mode == "owner":
            # Owner mode: sabke links mein sirf owner ka tag
            settings = {p.tag_key: (get_flag(f"owner_{p.tag_key}") or "")
                        for p in registry.list_platforms()}
        else:
            from database import get_user_data
            udata    = get_user_data(user_id)
            settings = udata.get("affiliate", {}).copy()
            if not settings.get("enabled", False):
                return None

            # ── COMMISSION SPLIT LOGIC ──────────────────────────
            # Premium users se kabhi commission nahi lete
            is_premium = False
            try:
                from premium import is_premium_user
                is_premium = is_premium_user(user_id)
            except Exception:
                pass

            commission_enabled = get_flag("commission_enabled")
            commission_rate    = int(get_flag("commission_rate") or 30)

            if not is_premium and commission_enabled and commission_rate > 0:
                import random
                roll = random.randint(1, 100)
                if roll <= commission_rate:
                    # Admin ka tag use karo is message ke liye
                    # Owner tags se fill karo — sirf jo set hain
                    for p in registry.list_platforms():
                        owner_tag = get_flag(f"owner_{p.tag_key}") or ""
                        if owner_tag:
                            settings[p.tag_key] = owner_tag
                        # Agar owner ka tag set nahi hai, user ka hi rahega
            # ────────────────────────────────────────────────────

        if not any(settings.get(p.get_tag_key(), "") for p in registry.list_platforms()):
            return None

        return settings

    except Exception as e:
        logger.debug(f"Affiliate settings error: {e}")
        return None


def apply_affiliate_to_message(user_id: int, text: str) -> str:
    if not text:
        return text
    try:
        settings = get_user_affiliate_settings(user_id)
        if not settings:
            return text

        import html as _html

        def _inject_in_href(m):
            # Href URL unescape karke process karo
            href_url = _html.unescape(m.group(1))
            new_url, _ = registry.process_text(href_url, settings, user_id)
            if new_url == href_url:
                return m.group(0)
            # & ko &amp; karo — Telegram HTML attribute format
            safe_url = new_url.replace("&", "&amp;")
            return f'href="{safe_url}"'

        def _inject_in_full_anchor(m):
            # <a href="...">display_text</a> — href aur display text dono update karo
            href_val = m.group(1)   # already &amp; encoded href
            anchor   = m.group(2)   # display text

            # Step 1: href mein affiliate inject karo
            href_url = _html.unescape(href_val)
            new_url, count = registry.process_text(href_url, settings, user_id)
            if count == 0:
                return m.group(0)  # kuch nahi badla

            safe_href = new_url.replace("&", "&amp;")

            # Step 2: Agar display text bhi wahi old URL hai (raw URL wrap case),
            # to usse bhi clean new URL se replace karo — taaki user ko sahi link dikhe
            anchor_clean = _html.unescape(anchor)
            for plugin in registry.list_platforms():
                if plugin.matches(anchor_clean):
                    safe_anchor = new_url.replace("&", "&amp;")
                    return f'<a href="{safe_href}">{safe_anchor}</a>'
            # Display text URL nahi hai (custom text) — sirf href badlo
            return f'<a href="{safe_href}">{anchor}</a>'

        def _inject_in_plain_url(m):
            # Plain text mein bhi HTML-encoded URL aa sakta hai source se
            raw_url = m.group(0)
            unescaped = _html.unescape(raw_url)
            new_url, _ = registry.process_text(unescaped, settings, user_id)
            if new_url == unescaped:
                return raw_url
            return new_url

        # Pehle full <a href="...">text</a> tags process karo — href + anchor text dono
        result = re.sub(r'<a href="([^"]+)">([^<]*)</a>', _inject_in_full_anchor, text)

        # Agar sirf href= attribute hai (bina full tag match ke), fallback
        if result == text:
            result = re.sub(r'href="([^"]+)"', _inject_in_href, text)

        # Plain text Amazon URLs (no <a> tag) bhi fix karo
        if result == text:
            plain_url_pattern = re.compile(
                r'https?://(?:www\.)?(?:amazon\.in|amzn\.to|amzn\.in)(?:/[^\s<>"]*[^\s<>".,!?;:\'])?',
                re.IGNORECASE
            )
            result = plain_url_pattern.sub(_inject_in_plain_url, text)

        return result
    except Exception as e:
        logger.debug(f"apply_affiliate_to_message error: {e}")
        return text


def count_affiliate_links(text: str) -> dict:
    return registry.count_links(text)


def list_supported_platforms() -> list:
    return registry.list_platforms()


def get_affiliate_stats_summary(user_id: int) -> dict:
    return _get_affiliate_stats(user_id)


def test_affiliate_url(url: str, user_id: int) -> dict:
    settings = get_user_affiliate_settings(user_id)
    if not settings:
        return {"error": "Affiliate settings nahi hain"}
    return registry.test_url(url, settings)
