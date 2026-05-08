"""
bot.py — Smart News Scheduler & Event-Driven Sentiment Trigger
Fusion Score Bot V7.0

Fixes in this version:
    - maxOutputTokens raised from 100 to 300 (fixes "The" truncation bug)
    - Same-time event dedup: when two events fire at identical time (e.g.
      Main Refinancing Rate + Monetary Policy Statement both at 17:45),
      only one pipeline run fires. Second event gets the same result.
    - Gemini RPM guard: minimum 13s gap between macro summary calls
"""

import time
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

import telebot

from config import (
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
    SCAN_DELAY_MINUTES,
    validate_config
)
from shared_functions import send_error_notification
from sentiment_scanner import SentimentScannerPipeline

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
CALENDAR_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
    )
}

TARGET_CURRENCIES = {'USD', 'EUR', 'GBP'}
TARGET_IMPACTS    = {'High', 'Medium'}

CURRENCY_FLAGS = {
    'USD': '🇺🇸',
    'EUR': '🇪🇺',
    'GBP': '🇬🇧',
    'JPY': '🇯🇵',
    'AUD': '🇦🇺',
    'CAD': '🇨🇦',
    'CHF': '🇨🇭',
    'NZD': '🇳🇿',
}

IMPACT_CONFIG = {
    'High':   {'emoji': '🔴', 'label': 'HIGH'},
    'Medium': {'emoji': '🟡', 'label': 'MED'},
    'Low':    {'emoji': '⚪', 'label': 'LOW'},
}


# =============================================================================
# CALENDAR FETCHING
# =============================================================================

def fetch_todays_schedule() -> list:
    """
    Fetches ForexFactory calendar and returns today's upcoming events.
    Groups events at the same minute into a single trigger to avoid
    running the sentiment pipeline twice for simultaneous releases.
    """
    try:
        resp = requests.get(
            CALENDAR_URL,
            headers=CALENDAR_HEADERS,
            timeout=15
        )
        resp.raise_for_status()
        all_events = resp.json()
    except Exception as e:
        logger.error(f"[Calendar] Fetch failed: {e}")
        send_error_notification(f"ForexFactory Calendar Fetch Failed: {e}", "bot")
        return []

    now_ist   = datetime.now(IST)
    today_str = now_ist.date().isoformat()
    events    = []

    for event in all_events:
        try:
            currency = event.get('country', '').upper()
            impact   = event.get('impact', '')
            title    = event.get('title', 'Unknown Event')
            date_str = event.get('date', '')

            if currency not in TARGET_CURRENCIES:
                continue
            if impact not in TARGET_IMPACTS:
                continue

            event_utc  = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            event_ist  = event_utc.astimezone(IST)
            event_date = event_ist.date().isoformat()

            if event_date != today_str:
                continue

            effective_trigger = event_ist + timedelta(minutes=SCAN_DELAY_MINUTES)
            if effective_trigger <= now_ist:
                logger.info(f"[Calendar] Skipping past event: {title} at {event_ist.strftime('%H:%M IST')}")
                continue

            events.append({
                'time':     event_ist,
                'currency': currency,
                'impact':   impact,
                'title':    title,
                'utc_time': event_utc,
            })

        except Exception as e:
            logger.warning(f"[Calendar] Failed to parse event: {e}")
            continue

    events.sort(key=lambda e: e['time'])
    logger.info(f"[Calendar] Found {len(events)} upcoming events today.")
    return events


def group_simultaneous_events(events: list) -> list:
    """
    Groups events that fire at the exact same minute into clusters.
    Each cluster shares one pipeline run — prevents double Gemini calls
    and duplicate article processing.

    Returns list of clusters: each cluster is a list of events.
    """
    if not events:
        return []

    clusters = []
    current_cluster = [events[0]]

    for event in events[1:]:
        prev_time = current_cluster[-1]['time']
        curr_time = event['time']
        # Same minute = same cluster
        if (curr_time - prev_time).total_seconds() < 60:
            current_cluster.append(event)
        else:
            clusters.append(current_cluster)
            current_cluster = [event]

    clusters.append(current_cluster)
    return clusters


# =============================================================================
# TELEGRAM ALERTS
# =============================================================================

def send_daily_briefing(events: list):
    """Sends morning briefing sorted High → Medium with currency flags."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    now_ist     = datetime.now(IST)
    date_str    = now_ist.strftime('%d %b %Y')
    high_events   = [e for e in events if e['impact'] == 'High']
    medium_events = [e for e in events if e['impact'] == 'Medium']

    lines = [
        f"📅 <b>MARKET BRIEFING — {date_str}</b>\n",
        f"Monitoring: EUR/USD | GBP/USD\n",
        f"Events today: <b>{len(events)}</b> "
        f"({len(high_events)} high | {len(medium_events)} medium)\n"
    ]

    if high_events:
        lines.append(f"\n🔴 <b>HIGH IMPACT</b>")
        for e in high_events:
            flag     = CURRENCY_FLAGS.get(e['currency'], '🌐')
            time_str = e['time'].strftime('%H:%M IST')
            lines.append(f"{flag} <b>{time_str}</b> — {e['title']}")

    if medium_events:
        lines.append(f"\n🟡 <b>MEDIUM IMPACT</b>")
        for e in medium_events:
            flag     = CURRENCY_FLAGS.get(e['currency'], '🌐')
            time_str = e['time'].strftime('%H:%M IST')
            lines.append(f"{flag} {time_str} — {e['title']}")

    if not events:
        lines.append("\n<i>No high/medium impact events scheduled today.</i>")

    lines.append(f"\n⏰ Scan delay: {SCAN_DELAY_MINUTES} min post-release")
    lines.append("🤖 Fusion Bot V7.0 active")

    msg = "\n".join(lines)
    try:
        bot = telebot.TeleBot(TELEGRAM_TOKEN)
        bot.send_message(TELEGRAM_CHAT_ID, msg, parse_mode="HTML", timeout=10)
        logger.info("[Briefing] Daily briefing sent.")
    except Exception as e:
        logger.error(f"[Briefing] Failed: {e}")
        send_error_notification(f"Daily briefing send failed: {e}", "bot")


def send_cluster_event_alert(cluster: list):
    """
    Sends pre-scan alert for a cluster of simultaneous events.
    Shows all event names if multiple fire at same time.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    # Use highest impact event as the lead
    lead   = sorted(cluster, key=lambda e: 0 if e['impact'] == 'High' else 1)[0]
    impact_cfg = IMPACT_CONFIG.get(lead['impact'], IMPACT_CONFIG['Medium'])
    time_str   = lead['time'].strftime('%H:%M IST')

    if len(cluster) == 1:
        flag  = CURRENCY_FLAGS.get(lead['currency'], '🌐')
        title_line = f"{flag} <b>{lead['currency']}</b> | {impact_cfg['label']} IMPACT\n📌 {lead['title']}"
    else:
        # Multiple simultaneous events
        event_lines = []
        for e in cluster:
            flag = CURRENCY_FLAGS.get(e['currency'], '🌐')
            ic   = IMPACT_CONFIG.get(e['impact'], IMPACT_CONFIG['Medium'])
            event_lines.append(f"{flag} {ic['label']} — {e['title']}")
        title_line = "\n".join(event_lines)

    msg = (
        f"{impact_cfg['emoji']} <b>NEWS EVENT FIRING</b>\n\n"
        f"{title_line}\n"
        f"🕐 Released: {time_str}\n\n"
        f"🔍 Scanning sentiment in {SCAN_DELAY_MINUTES}m..."
    )

    try:
        bot = telebot.TeleBot(TELEGRAM_TOKEN)
        bot.send_message(TELEGRAM_CHAT_ID, msg, parse_mode="HTML", timeout=10)
    except Exception as e:
        logger.error(f"[EventAlert] Failed: {e}")


def send_scan_complete_alert(cluster: list, result: dict, macro_summary: Optional[str] = None):
    """
    Sends post-scan summary after pipeline completes.
    Shows combined title if multiple simultaneous events.
    """
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    lead      = sorted(cluster, key=lambda e: 0 if e['impact'] == 'High' else 1)[0]
    processed = result.get('processed', 0)
    errors    = result.get('errors', 0)
    flag      = CURRENCY_FLAGS.get(lead['currency'], '🌐')

    if len(cluster) == 1:
        title_display = lead['title']
    else:
        title_display = " + ".join(e['title'] for e in cluster)

    lines = [
        f"✅ <b>SCAN COMPLETE: {title_display}</b>\n",
        f"{flag} {lead['currency']} | {processed} articles processed | {errors} errors\n",
    ]

    if macro_summary:
        lines.append(f"\n🧠 <b>AI MACRO CONTEXT</b>\n{macro_summary}")

    msg = "\n".join(lines)
    try:
        bot = telebot.TeleBot(TELEGRAM_TOKEN)
        bot.send_message(TELEGRAM_CHAT_ID, msg, parse_mode="HTML", timeout=10)
    except Exception as e:
        logger.error(f"[ScanAlert] Failed: {e}")


# =============================================================================
# AI MACRO SUMMARY
# FIX: maxOutputTokens raised from 100 → 300 (prevents "The" truncation)
# FIX: 13s RPM guard prevents hitting Gemini 5 RPM limit on simultaneous events
# =============================================================================

_last_gemini_call_time = 0  # Module-level RPM guard


def generate_macro_summary(pipeline_result: dict, event_title: str) -> Optional[str]:
    """
    Generates a 2-sentence macro context using Gemini after an event scan.

    Fixes vs previous version:
        - maxOutputTokens: 300 (was 100 — caused "The" truncation)
        - RPM guard: enforces 13s minimum between calls
    """
    global _last_gemini_call_time
    import requests as req_lib
    from config import GEMINI_API_KEY

    if not GEMINI_API_KEY:
        return None

    processed = pipeline_result.get('processed', 0)
    if processed == 0:
        return None

    # RPM guard — Gemini allows 5 RPM = min 12s between calls
    elapsed = time.time() - _last_gemini_call_time
    if elapsed < 13:
        wait = 13 - elapsed
        logger.info(f"[MacroSummary] RPM guard — waiting {wait:.1f}s")
        time.sleep(wait)

    eur_sent = pipeline_result.get('eur_net_sentiment', 0)
    gbp_sent = pipeline_result.get('gbp_net_sentiment', 0)

    def _label(net: int) -> str:
        if net > 2:   return "BULLISH"
        if net < -2:  return "BEARISH"
        return "NEUTRAL"

    prompt = (
        f"Economic data just released: {event_title}. "
        f"News analysis: EUR/USD {_label(eur_sent)}, GBP/USD {_label(gbp_sent)}. "
        f"Write exactly 2 sentences of macro context for a Forex day trader. "
        f"Be specific and directional. No fluff. No markdown. "
        f"Complete both sentences fully."
    )

    try:
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
        )
        payload = {
            "system_instruction": {
                "parts": [{"text": "You are a concise Forex macro analyst. Always write complete sentences."}]
            },
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {
                "temperature":    0.2,
                "maxOutputTokens": 300   # FIX: was 100, caused truncation
            }
        }
        resp = req_lib.post(url, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json()

        _last_gemini_call_time = time.time()

        text = (
            data['candidates'][0]['content']['parts'][0]['text']
            .strip()
        )

        # Sanitize for HTML
        text = (
            text
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
        )

        logger.info(f"[MacroSummary] Generated: {text[:80]}...")
        return f"<i>{text}</i>"

    except Exception as e:
        logger.warning(f"[MacroSummary] Gemini failed: {e}")
        return None


# =============================================================================
# MAIN SCHEDULER
# =============================================================================

def run_scheduler():
    """
    Main event-driven scheduler.
    Groups simultaneous events into single pipeline runs.
    """
    validate_config('bot')

    logger.info("[Bot] Fusion Score Bot V7.0 starting...")

    events = fetch_todays_schedule()

    if not events:
        logger.info("[Bot] No events today. Sending briefing and exiting.")
        send_daily_briefing([])
        return

    send_daily_briefing(events)

    # Group simultaneous events — prevents double pipeline runs
    clusters = group_simultaneous_events(events)
    logger.info(f"[Bot] {len(events)} events grouped into {len(clusters)} clusters.")

    for cluster in clusters:
        lead         = sorted(cluster, key=lambda e: 0 if e['impact'] == 'High' else 1)[0]
        trigger_time = lead['time'] + timedelta(minutes=SCAN_DELAY_MINUTES)
        now_ist      = datetime.now(IST)
        wait_seconds = (trigger_time - now_ist).total_seconds()

        if wait_seconds > 0:
            names = " + ".join(e['title'] for e in cluster)
            logger.info(
                f"[Bot] Waiting {wait_seconds:.0f}s for cluster: "
                f"{names} at {trigger_time.strftime('%H:%M IST')}"
            )
            time.sleep(wait_seconds)

        # Fire alert for this cluster
        logger.info(f"[Bot] Triggering scan for cluster of {len(cluster)} event(s).")
        send_cluster_event_alert(cluster)

        # One pipeline run for the whole cluster
        try:
            pipeline = SentimentScannerPipeline()
            result   = pipeline.run_pipeline()

            macro_summary = generate_macro_summary(result, lead['title'])
            send_scan_complete_alert(cluster, result, macro_summary)

            logger.info(
                f"[Bot] Scan complete: {lead['title']} — "
                f"{result.get('processed', 0)} processed"
            )

        except Exception as e:
            logger.error(f"[Bot] Pipeline error for {lead['title']}: {e}")
            send_error_notification(
                f"Sentiment pipeline failed for '{lead['title']}': {e}",
                "bot"
            )

    logger.info("[Bot] All clusters processed. Job complete.")


if __name__ == "__main__":
    run_scheduler()
