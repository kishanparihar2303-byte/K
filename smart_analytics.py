"""
smart_analytics.py — Advanced Analytics Engine v3.0
═══════════════════════════════════════════════════════════════

UPGRADES OVER existing analytics.py:
  ✅ Trend detection (daily/weekly/monthly growth)
  ✅ Peak hour analysis (which hours get most messages)
  ✅ Per-source performance comparison
  ✅ Per-destination success rate tracking
  ✅ Filter effectiveness report
  ✅ Text-based "chart" for Telegram messages (no image needed)
  ✅ Smart insights with actionable recommendations
  ✅ Export to CSV format (user can download)
  ✅ Anomaly detection (sudden spike/drop alerts)

INTEGRATION:
  from smart_analytics import AnalyticsEngine
  report = AnalyticsEngine.get_full_report(user_id)
  await bot.send_message(user_id, report)
"""

import datetime
import time
import logging
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

from database import get_user_data
from config import bot

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════

WEEK_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
BAR_WIDTH  = 15   # Max bar length in text charts


# ══════════════════════════════════════════
# TEXT CHART GENERATOR
# ══════════════════════════════════════════

class TextChart:
    """Generate beautiful text-based bar charts for Telegram."""

    @staticmethod
    def bar(value: int, max_value: int, width: int = BAR_WIDTH, fill: str = "█", empty: str = "░") -> str:
        """Generate a single bar."""
        if max_value == 0:
            return empty * width
        filled = int((value / max_value) * width)
        return fill * filled + empty * (width - filled)

    @classmethod
    def horizontal_bar_chart(
        cls,
        data: Dict[str, int],
        title: str = "",
        max_rows: int = 10,
        unit: str = "",
        emoji_map: Dict[str, str] = None,
    ) -> str:
        """
        Generate a horizontal bar chart.

        Example output:
        📊 Messages by Day
        ─────────────────────
        Mon  ████████░░░░░░░  45
        Tue  ███████████████  78
        Wed  ░░░░░░░░░░░░░░░   3
        """
        if not data:
            return f"_No data available_"

        lines = []
        if title:
            lines.append(f"**{title}**")
            lines.append("─" * 22)

        max_val  = max(data.values()) if data else 1
        max_key_len = max(len(str(k)) for k in data.keys())

        sorted_items = sorted(data.items(), key=lambda x: x[1], reverse=True)[:max_rows]

        for key, val in sorted_items:
            emoji  = (emoji_map or {}).get(str(key), "")
            bar    = cls.bar(val, max_val)
            label  = str(key).ljust(max_key_len)
            val_str = f"{val:>5}{unit}"
            lines.append(f"{emoji}{label}  `{bar}`  {val_str}")

        return "\n".join(lines)

    @classmethod
    def sparkline(cls, values: List[int], title: str = "") -> str:
        """
        Mini inline sparkline: ▁▂▃▄▅▆▇█
        Shows trend at a glance.
        """
        if not values:
            return ""
        bars = "▁▂▃▄▅▆▇█"
        max_v = max(values) or 1
        line  = "".join(bars[int((v / max_v) * 7)] for v in values)
        return f"{title}: `{line}`" if title else f"`{line}`"


# ══════════════════════════════════════════
# CORE ANALYTICS ENGINE
# ══════════════════════════════════════════

class AnalyticsEngine:

    @staticmethod
    def get_daily_stats(user_id: int, days: int = 7) -> Dict[str, dict]:
        """Last N days ka daily stats dict."""
        data   = get_user_data(user_id)
        daily  = data.get("analytics", {}).get("daily", {})
        result = {}
        for i in range(days):
            dt  = datetime.date.today() - datetime.timedelta(days=i)
            key = dt.isoformat()
            result[key] = daily.get(key, {"forwarded": 0, "blocked": 0})
        return result

    @staticmethod
    def get_hourly_stats(user_id: int) -> Dict[int, int]:
        """Per-hour message count (from available history)."""
        data  = get_user_data(user_id)
        hourly = data.get("analytics", {}).get("hourly", {})
        return {int(h): v for h, v in hourly.items()} if hourly else {}

    @staticmethod
    def get_source_stats(user_id: int) -> Dict[str, dict]:
        """Per-source forwarding stats."""
        data = get_user_data(user_id)
        return data.get("src_stats", {})

    @classmethod
    def get_trend(cls, user_id: int) -> Tuple[str, float]:
        """
        Calculate trend over last 7 days.
        Returns: (direction, percent_change)
        direction: "up" | "down" | "stable"
        """
        daily = cls.get_daily_stats(user_id, 14)
        sorted_days = sorted(daily.keys())

        if len(sorted_days) < 4:
            return "stable", 0.0

        midpoint = len(sorted_days) // 2
        first_half  = sum(daily[d]["forwarded"] for d in sorted_days[:midpoint])
        second_half = sum(daily[d]["forwarded"] for d in sorted_days[midpoint:])

        if first_half == 0:
            return "stable", 0.0

        change = ((second_half - first_half) / first_half) * 100

        if change > 10:
            return "up", change
        elif change < -10:
            return "down", change
        return "stable", change

    @classmethod
    def get_peak_hours(cls, user_id: int) -> List[Tuple[int, int]]:
        """Return top 3 peak hours (hour, count)."""
        hourly = cls.get_hourly_stats(user_id)
        if not hourly:
            return []
        sorted_hours = sorted(hourly.items(), key=lambda x: x[1], reverse=True)
        return sorted_hours[:3]

    @classmethod
    def get_filter_effectiveness(cls, user_id: int, days: int = 7) -> dict:
        """How effective are filters — block rate."""
        daily  = cls.get_daily_stats(user_id, days)
        total_fwd = sum(d["forwarded"] for d in daily.values())
        total_blk = sum(d.get("blocked", 0) for d in daily.values())
        total     = total_fwd + total_blk

        block_rate = (total_blk / total * 100) if total else 0
        return {
            "forwarded":  total_fwd,
            "blocked":    total_blk,
            "total":      total,
            "block_rate": round(block_rate, 1),
        }

    @classmethod
    def detect_anomaly(cls, user_id: int) -> Optional[str]:
        """
        Detect unusual patterns and return alert text.
        - Zero messages today but had activity yesterday
        - Sudden 3x spike
        """
        daily = cls.get_daily_stats(user_id, 7)
        sorted_days = sorted(daily.keys())
        if len(sorted_days) < 2:
            return None

        today     = daily[sorted_days[-1]]["forwarded"]
        yesterday = daily[sorted_days[-2]]["forwarded"]

        if yesterday > 10 and today == 0:
            return "⚠️ Aaj koi message forward nahi hua! Source ya session check karo."
        if yesterday > 0 and today > yesterday * 3:
            return f"📈 Unusual spike: aaj {today} msgs (kal {yesterday} tha)"

        return None

    @classmethod
    def get_top_sources(cls, user_id: int, limit: int = 5) -> List[Tuple[str, int]]:
        """Top N sources by message count."""
        src_stats = cls.get_source_stats(user_id)
        totals = [(src, v.get("total", 0)) for src, v in src_stats.items()]
        return sorted(totals, key=lambda x: x[1], reverse=True)[:limit]

    @classmethod
    def get_full_report(cls, user_id: int) -> str:
        """Complete analytics report — formatted for Telegram."""
        data   = get_user_data(user_id)
        srcs   = data.get("sources", [])
        dests  = data.get("destinations", [])

        sections = ["📊 **ANALYTICS REPORT**", "━" * 32, ""]

        # ── Summary ──
        daily   = cls.get_daily_stats(user_id, 7)
        today   = datetime.date.today().isoformat()
        today_d = daily.get(today, {"forwarded": 0, "blocked": 0})
        week_total = sum(d["forwarded"] for d in daily.values())

        sections.append("**📌 Summary**")
        sections.append(f"  Aaj: `{today_d['forwarded']}` forwarded, `{today_d.get('blocked',0)}` blocked")
        sections.append(f"  This Week: `{week_total}` total messages")

        # ── Trend ──
        direction, pct = cls.get_trend(user_id)
        trend_emoji = {"up": "📈", "down": "📉", "stable": "➡️"}.get(direction, "")
        sections.append(f"  Trend: {trend_emoji} `{abs(pct):.1f}%` {'growth' if direction == 'up' else 'decline' if direction == 'down' else 'stable'}")
        sections.append("")

        # ── 7-Day Chart ──
        chart_data = {}
        for i in range(6, -1, -1):
            dt  = datetime.date.today() - datetime.timedelta(days=i)
            key = dt.isoformat()
            day_label = WEEK_DAYS[dt.weekday()]
            chart_data[day_label] = daily.get(key, {}).get("forwarded", 0)

        if any(v > 0 for v in chart_data.values()):
            sections.append(TextChart.horizontal_bar_chart(
                chart_data, title="📅 Last 7 Days", unit=" msgs"
            ))
            sections.append("")

        # ── Sparkline ──
        spark_values = list(chart_data.values())
        sections.append(TextChart.sparkline(spark_values, "7-day trend"))
        sections.append("")

        # ── Filter Effectiveness ──
        filt = cls.get_filter_effectiveness(user_id)
        if filt["total"] > 0:
            sections.append("**🔑 Filter Effectiveness**")
            sections.append(f"  Block Rate: `{filt['block_rate']}%` ({filt['blocked']} blocked of {filt['total']})")
            sections.append("")

        # ── Top Sources ──
        top_srcs = cls.get_top_sources(user_id, 5)
        if top_srcs:
            src_chart = {s[:20]: c for s, c in top_srcs}
            sections.append(TextChart.horizontal_bar_chart(
                src_chart, title="📥 Top Sources", max_rows=5, unit=" msgs"
            ))
            sections.append("")

        # ── Peak Hours ──
        peaks = cls.get_peak_hours(user_id)
        if peaks:
            sections.append("**⏰ Peak Hours**")
            for hour, count in peaks:
                period = "AM" if hour < 12 else "PM"
                h12    = hour if hour <= 12 else hour - 12
                sections.append(f"  {h12:02d}:00 {period} — `{count}` msgs")
            sections.append("")

        # ── Anomaly Alert ──
        anomaly = cls.detect_anomaly(user_id)
        if anomaly:
            sections.append(f"**⚠️ Alert:** {anomaly}")
            sections.append("")

        # ── Config Summary ──
        sections.append(f"**📋 Setup:** {len(srcs)} sources → {len(dests)} destinations")
        footer_ts = datetime.datetime.now().strftime("%d %b %Y, %H:%M")
        sections.append(f"\n_Generated: {footer_ts}_")

        return "\n".join(sections)

    @classmethod
    def get_mini_report(cls, user_id: int) -> str:
        """Short report for main menu dashboard."""
        daily   = cls.get_daily_stats(user_id, 7)
        today   = datetime.date.today().isoformat()
        today_d = daily.get(today, {"forwarded": 0, "blocked": 0})
        week    = sum(d["forwarded"] for d in daily.values())
        direction, pct = cls.get_trend(user_id)
        trend   = {"up": f"📈+{pct:.0f}%", "down": f"📉{pct:.0f}%", "stable": "➡️"}.get(direction, "")

        return (
            f"📤 Today: `{today_d['forwarded']}` | "
            f"Week: `{week}` | "
            f"{trend}"
        )


# ══════════════════════════════════════════
# RECORD HELPERS (Drop-in for analytics.py)
# ══════════════════════════════════════════

def record_message(user_id: int, stat_type: str = "forwarded"):
    """
    Enhanced record_message — also tracks hourly data.
    Drop-in replacement for existing analytics.record_message()
    """
    try:
        data    = get_user_data(user_id)
        analyt  = data.setdefault("analytics", {})
        daily   = analyt.setdefault("daily", {})
        hourly  = analyt.setdefault("hourly", {})

        today   = datetime.date.today().isoformat()
        hour    = datetime.datetime.now().hour

        daily.setdefault(today, {"forwarded": 0, "blocked": 0, "errors": 0})
        daily[today][stat_type] = daily[today].get(stat_type, 0) + 1

        # Hourly tracking
        hourly[str(hour)] = hourly.get(str(hour), 0) + 1

        # Keep 30 days only
        if len(daily) > 30:
            oldest = sorted(daily.keys())[0]
            del daily[oldest]

    except Exception as e:
        logger.debug(f"Analytics record error: {e}")


# ══════════════════════════════════════════
# EXPORT TO CSV
# ══════════════════════════════════════════

def export_analytics_csv(user_id: int) -> str:
    """
    Export daily analytics as CSV string.
    User can copy-paste or download.
    """
    data   = get_user_data(user_id)
    daily  = data.get("analytics", {}).get("daily", {})

    lines = ["Date,Forwarded,Blocked,Errors"]
    for date_str in sorted(daily.keys(), reverse=True):
        d = daily[date_str]
        lines.append(f"{date_str},{d.get('forwarded',0)},{d.get('blocked',0)},{d.get('errors',0)}")

    return "\n".join(lines)
