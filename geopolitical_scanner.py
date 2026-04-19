"""
geopolitical_scanner.py — Geopolitical Dragnet (Instant Black Swan Protection)
Fusion Score Bot V7.0

Ultra-low-latency background thread for unscheduled, market-breaking events.
Catches shocks that completely bypass the standard economic calendar.

Design:
    - Runs as a daemon thread inside the Render engine (volatility_atr.py)
    - Polls institutional RSS feeds every 30 seconds
    - Zero API cost — RSS is unlimited and free
    - Scans strictly for emergency keywords (attack, ceasefire, intervention, etc.)
    - On detection: instantly fires Telegram breaking alert to ALL channels
    - Does NOT modify Fusion Score or system_state
    - Deduplication: 4-hour TTL prevents same headline from re-alerting

RSS sources:
    Reuters Business News  — institutional, highly reliable
    BBC Business News      — broad global coverage
    AP Business News       — fast-moving breaking events

Alert format: HTML (not Markdown — consistent with all V7 messages)
"""

import time
import hashlib
import logging
import threading
import feedparser
import telebot
from datetime import datetime, timezone, timedelta
from typing import List

from config import (
    DRAGNET_POLL_SECONDS,
    DRAGNET_DEDUP_TTL_HOURS,
    DRAGNET_EMERGENCY_KEYWORDS,
    RSS_FEEDS_DRAGNET,
    TELEGRAM_TOKEN,
)
from shared_functions import send_error_notification

logger = logging.getLogger(__name__)

# IST timezone
IST_TZ = timezone(timedelta(hours=5, minutes=30))


class GeopoliticalScanner:
    """
    Background thread that monitors RSS feeds for black swan events.

    Usage (from volatility_atr.py __main__):
        from geopolitical_scanner import GeopoliticalScanner
        scanner = GeopoliticalScanner(bot_token, channel_ids)
        threading.Thread(target=scanner.run_loop, daemon=True).start()
    """

    def __init__(self, bot_token: str, channel_ids: List[str]):
        """
        Args:
            bot_token:   Telegram bot token for sending alerts
            channel_ids: List of Telegram channel/chat IDs to broadcast to
        """
        self.bot_token   = bot_token
        self.channel_ids = [c for c in channel_ids if c]  # Filter out None/empty
        self._seen_hashes: dict = {}  # hash → expiry_timestamp
        self._lock = threading.Lock()

        if not self.channel_ids:
            logger.warning("[Dragnet] No channel IDs configured. Alerts will not fire.")
        else:
            logger.info(f"[Dragnet] Initialized. Monitoring {len(RSS_FEEDS_DRAGNET)} feeds → {len(self.channel_ids)} channels.")

    def _is_emergency(self, text: str) -> bool:
        """Returns True if the text contains any emergency keyword."""
        text_lower = text.lower()
        return any(kw in text_lower for kw in DRAGNET_EMERGENCY_KEYWORDS)

    def _make_hash(self, title: str) -> str:
        """Produces a stable dedup hash for a headline."""
        return hashlib.md5(title.lower().strip().encode()).hexdigest()

    def _is_seen(self, h: str) -> bool:
        """Returns True if this hash was seen within the dedup TTL."""
        with self._lock:
            expiry = self._seen_hashes.get(h)
            if expiry is None:
                return False
            if time.time() > expiry:
                del self._seen_hashes[h]
                return False
            return True

    def _mark_seen(self, h: str):
        """Marks a hash as seen, expiring after DRAGNET_DEDUP_TTL_HOURS."""
        with self._lock:
            self._seen_hashes[h] = time.time() + (DRAGNET_DEDUP_TTL_HOURS * 3600)

    def _cleanup_expired(self):
        """Removes expired hashes to prevent unbounded memory growth."""
        now = time.time()
        with self._lock:
            expired = [h for h, expiry in self._seen_hashes.items() if now > expiry]
            for h in expired:
                del self._seen_hashes[h]

    def _extract_source_name(self, feed_url: str) -> str:
        """Extracts a readable source name from a feed URL."""
        try:
            domain = feed_url.split('/')[2]
            return domain.replace('feeds.', '').replace('www.', '')
        except Exception:
            return "Unknown Source"

    def _fire_alert(self, headline: str, source_url: str, entry_link: str = ""):
        """
        Sends the breaking event alert to all configured channels.
        Uses HTML parse mode (consistent with all V7 Telegram messages).
        """
        source_name = self._extract_source_name(source_url)
        now_ist     = datetime.now(IST_TZ).strftime('%H:%M IST')
        now_utc     = datetime.now(timezone.utc).strftime('%H:%M UTC')

        # Sanitize headline for HTML (escape special chars)
        safe_headline = (
            headline
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
        )

        msg = (
            f"🚨 <b>BREAKING MARKET EVENT</b> 🚨\n\n"
            f"<b>{safe_headline}</b>\n\n"
            f"📡 Source: {source_name}\n"
            f"🕐 {now_ist} ({now_utc})\n\n"
            f"<i>⚠️ Advisory only — review open positions before acting</i>"
        )

        try:
            bot = telebot.TeleBot(self.bot_token, threaded=False)
            for channel_id in self.channel_ids:
                try:
                    bot.send_message(channel_id, msg, parse_mode="HTML", timeout=10)
                    logger.info(f"[Dragnet] Alert sent to {channel_id}: {headline[:60]}")
                except Exception as e:
                    logger.error(f"[Dragnet] Failed to send to {channel_id}: {e}")
        except Exception as e:
            logger.error(f"[Dragnet] Bot init failed: {e}")

    def scan_once(self):
        """
        Runs a single scan across all RSS feeds.
        Called by run_loop() every DRAGNET_POLL_SECONDS.
        """
        self._cleanup_expired()

        for feed_url in RSS_FEEDS_DRAGNET:
            try:
                feed = feedparser.parse(feed_url)

                if not feed.entries:
                    logger.debug(f"[Dragnet] Empty feed: {feed_url}")
                    continue

                # Only check latest 10 entries per feed (newest first in most RSS)
                for entry in feed.entries[:10]:
                    title = entry.get('title', '').strip()
                    if not title:
                        continue

                    if not self._is_emergency(title):
                        continue

                    h = self._make_hash(title)
                    if self._is_seen(h):
                        continue

                    # New emergency headline — mark seen and fire alert
                    self._mark_seen(h)
                    link = entry.get('link', '')
                    self._fire_alert(title, feed_url, link)

            except Exception as e:
                logger.warning(f"[Dragnet] Feed error ({feed_url}): {e}")
                # Don't send error notification for individual feed failures
                # (feeds can be temporarily unreachable; this is not critical)

    def run_loop(self):
        """
        Main blocking loop. Run this in a daemon thread.
        Polls every DRAGNET_POLL_SECONDS (default: 30s).
        Never exits — daemon flag ensures cleanup on process death.
        """
        logger.info(f"[Dragnet] Geopolitical scanner active. Poll interval: {DRAGNET_POLL_SECONDS}s")
        error_count = 0

        while True:
            try:
                self.scan_once()
                error_count = 0  # Reset on success
            except Exception as e:
                error_count += 1
                logger.error(f"[Dragnet] scan_once() failed (#{error_count}): {e}")
                if error_count >= 5:
                    send_error_notification(
                        f"Geopolitical Dragnet failing repeatedly ({error_count} errors). Last: {e}",
                        "dragnet"
                    )
                    error_count = 0  # Reset after alerting

            time.sleep(DRAGNET_POLL_SECONDS)
