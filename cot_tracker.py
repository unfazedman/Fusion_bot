"""
cot_tracker.py — COT Smart Money Tracker
Fusion Score Bot V7.0

Fetches weekly CFTC Commitment of Traders data.
Classifies institutional positioning using 52-week index normalization.
5-state classification: STRONGLY_BULLISH | BULLISH | NEUTRAL | BEARISH | STRONGLY_BEARISH

V7 improvements:
    - Exponential backoff retry (3 attempts) on CFTC API failure
    - COT momentum delta tracking (week-over-week change)
    - BEARISH_FADING / BULLISH_FADING detection (empirically validated)
    - Column ownership enforced: only updates COT-owned columns
    - max_52w == min_52w anomaly sends error notification (not just logger)
    - Commercial positions also tracked (non-commercial remains primary)

Run schedule: Saturday 10:00 AM IST (after CFTC Friday ~3:30 PM EST release)
GitHub Actions workflow: cot.yml
"""

import time as time_module
import logging
import requests
from datetime import datetime, timezone

from config import (
    SUPABASE_URL, SUPABASE_KEY,
    COT_LOOKBACK_WEEKS,
    COT_NEUTRAL_BAND,
    COT_STRONG_THRESHOLD,
    PAIRS,
    validate_config
)
from shared_functions import get_supabase_client, send_error_notification

logger = logging.getLogger(__name__)

# CFTC Socrata Open Data API
CFTC_API_URL = "https://publicreporting.cftc.gov/resource/jun7-fc8e.json"

# CFTC market names for our pairs
CFTC_MARKETS = {
    'EUR/USD': 'EURO FX - CHICAGO MERCANTILE EXCHANGE',
    'GBP/USD': 'BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE',
}


class COTTracker:

    def __init__(self):
        self.supabase = get_supabase_client()

    # -------------------------------------------------------------------------
    # DATA FETCHING WITH RETRY
    # -------------------------------------------------------------------------

    def fetch_cot_history(self, market_name: str) -> list:
        """
        Fetches COT history from CFTC Socrata API.
        Returns list of weekly records sorted newest first.

        V7 FIX: 3 attempts with exponential backoff (5s, 15s, 45s).
        V6 had no retry — one transient CFTC 503 would leave COT stale for a week.
        """
        params = {
            '$where':  f"market_and_exchange_names='{market_name}'",
            '$order':  'report_date_as_yyyy_mm_dd DESC',
            '$limit':  str(COT_LOOKBACK_WEEKS + 5),  # +5 buffer for missing weeks
            '$select': (
                'report_date_as_yyyy_mm_dd,'
                'noncomm_positions_long_all,'
                'noncomm_positions_short_all,'
                'comm_positions_long_all,'
                'comm_positions_short_all'
            )
        }

        delays = [5, 15, 45]  # Exponential backoff

        for attempt, delay in enumerate(delays, 1):
            try:
                resp = requests.get(CFTC_API_URL, params=params, timeout=20)
                resp.raise_for_status()
                data = resp.json()

                if not data:
                    logger.warning(f"[COT] Empty response for {market_name} (attempt {attempt}/3)")
                    if attempt < len(delays):
                        logger.info(f"[COT] Retrying in {delay}s...")
                        time_module.sleep(delay)
                    continue

                history = []
                for record in data:
                    try:
                        long_nc  = int(record.get('noncomm_positions_long_all', 0))
                        short_nc = int(record.get('noncomm_positions_short_all', 0))
                        long_c   = int(record.get('comm_positions_long_all', 0))
                        short_c  = int(record.get('comm_positions_short_all', 0))
                        net_nc   = long_nc - short_nc
                        net_c    = long_c - short_c

                        history.append({
                            'date':    record.get('report_date_as_yyyy_mm_dd', '')[:10],
                            'net':     net_nc,       # Non-commercial (primary)
                            'net_c':   net_c,        # Commercial (tracked, not primary)
                            'long':    long_nc,
                            'short':   short_nc,
                        })
                    except (ValueError, TypeError) as e:
                        logger.warning(f"[COT] Record parse error: {e}")
                        continue

                logger.info(f"[COT] Fetched {len(history)} weeks for {market_name}")
                return history

            except requests.exceptions.RequestException as e:
                logger.warning(f"[COT] CFTC API attempt {attempt}/3 failed: {e}")
                if attempt < len(delays):
                    logger.info(f"[COT] Retrying in {delay}s...")
                    time_module.sleep(delay)
                else:
                    logger.error(f"[COT] All retries exhausted for {market_name}")
                    send_error_notification(
                        f"COT API Failed after 3 attempts ({market_name}): {e}",
                        "cot_tracker"
                    )

        return []

    # -------------------------------------------------------------------------
    # INDEX NORMALIZATION & CLASSIFICATION
    # -------------------------------------------------------------------------

    def calculate_cot_index(self, history: list) -> tuple:
        """
        Calculates the 52-week COT index for the most recent data point.

        COT Index = (current - min_52w) / (max_52w - min_52w)
        Range: 0.0 (all-time bearish in window) to 1.0 (all-time bullish in window)

        Returns:
            (index: float, current_net: int, prev_net: int, max_52w: int, min_52w: int)
            or (0.5, 0, 0, 0, 0) on error
        """
        if not history:
            return 0.5, 0, 0, 0, 0

        # Take up to 52 weeks
        window = history[:COT_LOOKBACK_WEEKS]

        nets    = [w['net'] for w in window]
        max_52w = max(nets)
        min_52w = min(nets)

        current_net = window[0]['net']
        prev_net    = window[1]['net'] if len(window) > 1 else current_net

        if max_52w == min_52w:
            # Flat market — data anomaly (impossible in real Forex)
            logger.warning(f"[COT] max_52w == min_52w ({max_52w}). Possible data error.")
            send_error_notification(
                f"COT anomaly: max_52w == min_52w ({max_52w}). Index defaulting to 0.5 (NEUTRAL). "
                f"Verify CFTC data integrity.",
                "cot_tracker"
            )
            return 0.5, current_net, prev_net, max_52w, min_52w

        index = (current_net - min_52w) / (max_52w - min_52w)
        return round(index, 4), current_net, prev_net, max_52w, min_52w

    def classify_bias(self, index: float, current_net: int, prev_net: int) -> str:
        """
        Classifies COT bias from index value.

        Base classification (from index):
            >= 0.75: STRONGLY_BULLISH
            >= 0.60: BULLISH
            0.40-0.60: NEUTRAL
            <= 0.40: BEARISH
            <= 0.25: STRONGLY_BEARISH

        V7 ENHANCEMENT — Momentum delta override:
            BULLISH + institutions reducing longs → BULLISH_FADING
            BEARISH + institutions reducing shorts → BEARISH_FADING
            BULLISH + institutions adding longs → BULLISH_ACCELERATING
            BEARISH + institutions adding shorts → BEARISH_ACCELERATING

        The BULLISH_FADING state was empirically validated in the 76-trade audit
        as a counter-signal: when bias shows BULLISH but net is declining,
        SHORT setups historically perform better than LONG setups.
        """
        delta = current_net - prev_net

        # Base classification from 52-week index
        if index >= COT_STRONG_THRESHOLD:
            base_bias = "STRONGLY_BULLISH"
        elif index >= (1 - COT_NEUTRAL_BAND):
            base_bias = "BULLISH"
        elif index <= (1 - COT_STRONG_THRESHOLD):
            base_bias = "STRONGLY_BEARISH"
        elif index <= COT_NEUTRAL_BAND:
            base_bias = "BEARISH"
        else:
            base_bias = "NEUTRAL"

        # Delta enhancement (significant threshold: ±3000 net contracts)
        DELTA_THRESHOLD = 3000

        if base_bias == "BULLISH" and delta < -DELTA_THRESHOLD:
            return "BULLISH_FADING"           # Institutions cutting longs — reversal risk
        elif base_bias == "BULLISH" and delta > DELTA_THRESHOLD:
            return "BULLISH_ACCELERATING"     # Institutions adding longs
        elif base_bias == "BEARISH" and delta > DELTA_THRESHOLD:
            return "BEARISH_FADING"           # Institutions cutting shorts — reversal risk
        elif base_bias == "BEARISH" and delta < -DELTA_THRESHOLD:
            return "BEARISH_ACCELERATING"     # Institutions adding shorts

        return base_bias

    # -------------------------------------------------------------------------
    # SUPABASE UPDATE — COLUMN OWNERSHIP ENFORCED
    # -------------------------------------------------------------------------

    def update_system_state(
        self, pair: str, bias: str, index: float,
        current_net: int, cot_date: str
    ):
        """
        Updates COT-owned columns in system_state.

        V7 FIX: Uses targeted UPDATE instead of upsert.
        Columns owned by COT tracker: cot_bias, cot_index, cot_net, cot_date, cot_updated_at
        Does NOT touch: macro_sentiment, last_alerted_candle, market_regime, long_wr_20, etc.
        """
        try:
            now_iso = datetime.now(timezone.utc).isoformat()

            result = (
                self.supabase.table("system_state")
                .update({
                    "cot_bias":       bias,
                    "cot_index":      index,
                    "cot_net":        current_net,
                    "cot_date":       cot_date,
                    "cot_updated_at": now_iso,
                })
                .eq("pair", pair)
                .execute()
            )

            if not result.data:
                # Row doesn't exist — insert with all defaults
                self.supabase.table("system_state").insert({
                    "pair":             pair,
                    "cot_bias":         bias,
                    "cot_index":        index,
                    "cot_net":          current_net,
                    "cot_date":         cot_date,
                    "cot_updated_at":   now_iso,
                    "macro_sentiment":  0,
                    "market_regime":    "RANGING",
                }).execute()
                logger.info(f"[COT] Inserted new system_state row for {pair}.")
            else:
                logger.info(f"[COT] system_state updated: {pair} → {bias} (index={index:.2f})")

        except Exception as e:
            logger.error(f"[COT] DB update failed for {pair}: {e}")
            send_error_notification(f"COT DB Update Failed ({pair}): {e}", "cot_tracker")
            raise

    # -------------------------------------------------------------------------
    # TELEGRAM REPORT
    # -------------------------------------------------------------------------

    def send_cot_report(self, pair: str, bias: str, index: float, current_net: int,
                        prev_net: int, cot_date: str):
        """Sends a formatted COT update to the operator Telegram channel."""
        import telebot
        from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return

        delta      = current_net - prev_net
        delta_str  = f"{delta:+,}"
        filled     = int(round(index * 10))
        bar        = "█" * filled + "░" * (10 - filled)
        bias_clean = bias.replace('_', ' ')

        # Bias emoji
        if "STRONGLY_BULLISH" in bias:
            bias_emoji = "🟢🟢"
        elif "BULLISH_FADING" in bias:
            bias_emoji = "🟡⬇️"
        elif "BULLISH_ACCELERATING" in bias:
            bias_emoji = "🟢⬆️"
        elif "BULLISH" in bias:
            bias_emoji = "🟢"
        elif "STRONGLY_BEARISH" in bias:
            bias_emoji = "🔴🔴"
        elif "BEARISH_FADING" in bias:
            bias_emoji = "🟡⬆️"
        elif "BEARISH_ACCELERATING" in bias:
            bias_emoji = "🔴⬇️"
        elif "BEARISH" in bias:
            bias_emoji = "🔴"
        else:
            bias_emoji = "⚪"

        msg = (
            f"🏦 <b>COT UPDATE: {pair}</b>\n\n"
            f"{bias_emoji} Bias: <b>{bias_clean}</b>\n"
            f"📊 Index: [{bar}] {index:.2f}\n"
            f"📈 Net Positions: {current_net:+,}\n"
            f"🔄 Week Delta: {delta_str}\n"
            f"📅 Report Date: {cot_date}"
        )

        try:
            bot = telebot.TeleBot(TELEGRAM_TOKEN)
            bot.send_message(TELEGRAM_CHAT_ID, msg, parse_mode="HTML", timeout=10)
        except Exception as e:
            logger.error(f"[COT] Telegram report failed: {e}")

    # -------------------------------------------------------------------------
    # MAIN RUNNER
    # -------------------------------------------------------------------------

    def run(self):
        """
        Main COT update job. Called by cot.yml GitHub Actions workflow.
        """
        logger.info("[COT] Starting COT update job...")
        errors = 0

        for pair in PAIRS:
            try:
                market_name = CFTC_MARKETS.get(pair)
                if not market_name:
                    logger.error(f"[COT] No CFTC market name for {pair}")
                    continue

                history = self.fetch_cot_history(market_name)
                if not history:
                    logger.error(f"[COT] No COT data for {pair}")
                    errors += 1
                    continue

                index, current_net, prev_net, max_52w, min_52w = self.calculate_cot_index(history)
                bias     = self.classify_bias(index, current_net, prev_net)
                cot_date = history[0]['date']

                logger.info(
                    f"[COT] {pair}: bias={bias} index={index:.3f} "
                    f"net={current_net:+,} delta={current_net-prev_net:+,} "
                    f"range=[{min_52w:,} → {max_52w:,}]"
                )

                self.update_system_state(pair, bias, index, current_net, cot_date)
                self.send_cot_report(pair, bias, index, current_net, prev_net, cot_date)

            except Exception as e:
                logger.error(f"[COT] Failed for {pair}: {e}")
                errors += 1

        if errors == 0:
            logger.info("[COT] COT update complete. All pairs updated.")
        else:
            logger.warning(f"[COT] COT update complete with {errors} errors.")
            send_error_notification(
                f"COT update completed with {errors}/{len(PAIRS)} failures.",
                "cot_tracker"
            )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    validate_config('cot_tracker')
    tracker = COTTracker()
    tracker.run()
