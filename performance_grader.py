"""
performance_grader.py — Trade Performance Grader
Fusion Score Bot V7.0

Grades unresolved trade_logs by fetching exit candle prices from TwelveData.
Writes result (WIN/LOSS/BREAKEVEN) and pips to each trade record.
Also computes rolling 20-trade win rates by direction and writes to system_state.

V7 improvements:
    - Rolling 20-trade WR by direction (long_wr_20, short_wr_20)
    - PIP_MULTIPLIERS dict (future-proofs JPY pair addition)
    - Win rate query uses .not_.is_() to avoid full table scan truncation
    - HOLD_HOURS moved to config-compatible parameter
    - Extreme ATR tier (≥3.0×) tracked separately in output
    - Score tier analysis in summary report

Run schedule: Every 2 hours (grader.yml GitHub Actions workflow)
"""

import logging
import requests
from datetime import datetime, timezone, timedelta

from config import (
    TWELVE_DATA_KEY, PAIRS,
    validate_config
)
from shared_functions import get_supabase_client, send_error_notification

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# GRADER CONFIGURATION
# =============================================================================

# Fixed hold assumption — matches how signals are designed
# V7 NOTE: exit_signal() in strategies/momentum.py also uses 60 minutes
HOLD_HOURS = 1

# Pip multipliers by pair
# V6 bug: hardcoded 10_000. If JPY added, would show 100× wrong pip values.
PIP_MULTIPLIERS = {
    'EUR/USD': 10_000,
    'GBP/USD': 10_000,
    # 'USD/JPY': 100,   ← uncomment when JPY pairs added
}
DEFAULT_PIP_MULTIPLIER = 10_000

# WIN/LOSS thresholds
WIN_PIPS_THRESHOLD       = 2.0    # >= 2 pips = WIN
LOSS_PIPS_THRESHOLD      = -2.0   # <= -2 pips = LOSS
# Between -2 and +2 pips = BREAKEVEN


class PerformanceGrader:

    def __init__(self):
        self.supabase = get_supabase_client()

    # -------------------------------------------------------------------------
    # CANDLE FETCHING
    # -------------------------------------------------------------------------

    def _fetch_candles_at_time(self, pair: str, start_time: str, count: int = 30) -> list:
        """
        Fetches 5-minute candles from TwelveData around a given timestamp.
        Used to find the exit candle (entry_time + HOLD_HOURS).

        Args:
            pair:       'EUR/USD' or 'GBP/USD'
            start_time: ISO datetime string of the entry candle
            count:      Number of candles to fetch (30 = 2.5 hours of 5-min bars)

        Returns:
            List of candle dicts or empty list on error.
        """
        
        params = {
            'symbol':     pair,
            'interval':   '5min',
            'outputsize': count,
            'start_date': start_time,
            'apikey':     TWELVE_DATA_KEY,
        }
        try:
            resp = requests.get(
                "https://api.twelvedata.com/time_series",
                params=params,
                timeout=15
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get('status') == 'error':
                logger.error(f"[Grader] TwelveData error for {pair}: {data.get('message')}")
                return []

            return data.get('values', [])

        except Exception as e:
            logger.error(f"[Grader] Candle fetch failed for {pair}: {e}")
            return []

    def _find_exit_candle(self, candles: list, entry_time: str) -> dict:
        """
        Finds the candle closest to entry_time + HOLD_HOURS.
        Uses binary search logic for efficiency.

        Args:
            candles:    List of candle dicts (newest first, TwelveData format)
            entry_time: ISO datetime string of entry

        Returns:
            Candle dict or None if not found.
        """
        try:
            entry_dt  = datetime.fromisoformat(entry_time.replace('Z', '+00:00'))
            if entry_dt.tzinfo is None:
                entry_dt = entry_dt.replace(tzinfo=timezone.utc)
            exit_target = entry_dt + timedelta(hours=HOLD_HOURS)

            # TwelveData returns newest first — reverse for chronological search
            candles_asc = list(reversed(candles))

            best_candle = None
            best_delta  = None

            for candle in candles_asc:
                try:
                    candle_dt = datetime.fromisoformat(candle['datetime'].replace('Z', '+00:00'))
                    if candle_dt.tzinfo is None:
                        candle_dt = candle_dt.replace(tzinfo=timezone.utc)

                    delta = abs((candle_dt - exit_target).total_seconds())

                    if best_delta is None or delta < best_delta:
                        best_delta  = delta
                        best_candle = candle

                    # Within 5 minutes of target — good enough
                    if delta <= 300:
                        break

                except (ValueError, KeyError):
                    continue

            return best_candle

        except Exception as e:
            logger.error(f"[Grader] Exit candle search failed: {e}")
            return None

    # -------------------------------------------------------------------------
    # GRADING LOGIC
    # -------------------------------------------------------------------------

    def _calculate_pips(self, entry_price: float, exit_price: float,
                        direction: str, pair: str) -> float:
        """
        Calculates pips from entry to exit.

        LONG:  positive pips = price went up (WIN)
        SHORT: positive pips = price went down (WIN)
        """
        multiplier = PIP_MULTIPLIERS.get(pair, DEFAULT_PIP_MULTIPLIER)

        if direction == 'LONG':
            return round((exit_price - entry_price) * multiplier, 1)
        else:  # SHORT
            return round((entry_price - exit_price) * multiplier, 1)

    def _classify_result(self, pips: float) -> str:
        if pips >= WIN_PIPS_THRESHOLD:
            return 'WIN'
        elif pips <= LOSS_PIPS_THRESHOLD:
            return 'LOSS'
        return 'BREAKEVEN'

    def grade_trade(self, trade: dict) -> bool:
        """
        Grades a single ungraded trade.

        Returns True if graded successfully, False on error.
        """
        trade_id    = trade.get('id')
        pair        = trade.get('pair')
        direction   = trade.get('direction')
        entry_price = trade.get('entry_price')
        candle_time = trade.get('candle_time') or trade.get('timestamp_utc')

        if not all([trade_id, pair, direction, entry_price, candle_time]):
            logger.warning(f"[Grader] Trade {trade_id} missing required fields. Skipping.")
            return False

        try:
            entry_price = float(entry_price)
        except (ValueError, TypeError):
            logger.warning(f"[Grader] Trade {trade_id} invalid entry_price. Skipping.")
            return False

        # Fetch candles around entry time
        candles = self._fetch_candles_at_time(pair, candle_time)
        if not candles:
            logger.warning(f"[Grader] No candles for trade {trade_id} ({pair} @ {candle_time})")
            return False

        # Find exit candle
        exit_candle = self._find_exit_candle(candles, candle_time)
        if not exit_candle:
            logger.warning(f"[Grader] No exit candle found for trade {trade_id}")
            return False

        try:
            exit_price = float(exit_candle['close'])
        except (ValueError, KeyError):
            logger.warning(f"[Grader] Invalid exit candle price for trade {trade_id}")
            return False

        # Calculate result
        pips   = self._calculate_pips(entry_price, exit_price, direction, pair)
        result = self._classify_result(pips)

        logger.info(
            f"[Grader] Trade {trade_id}: {pair} {direction} "
            f"Entry={entry_price:.5f} Exit={exit_price:.5f} "
            f"Pips={pips:+.1f} → {result}"
        )

        # Write to DB
        try:
            self.supabase.table("trade_logs").update({
                "result":     result,
                "exit_price": exit_price,
                "pips":       pips,
                "exit_time":  exit_candle.get('datetime'),
                "graded_at":  datetime.now(timezone.utc).isoformat(),
            }).eq("id", trade_id).execute()
            return True

        except Exception as e:
            logger.error(f"[Grader] DB update failed for trade {trade_id}: {e}")
            send_error_notification(
                f"Grader DB update failed (trade_id={trade_id}): {e}",
                "grader"
            )
            return False

    # -------------------------------------------------------------------------
    # ROLLING WIN RATE COMPUTATION
    # -------------------------------------------------------------------------

    def update_rolling_win_rates(self):
        """
        Computes rolling 20-trade win rates by direction.
        Writes to system_state: long_wr_20, short_wr_20, wr_updated_at

        Called after grading to keep the /status command current.
        """
        try:
            recent = (
                self.supabase.table("trade_logs")
                .select("direction, result")
                .not_.is_("result", None)
                .order("timestamp_utc", desc=True)
                .limit(60)  # Fetch 60 to ensure 20 per direction
                .execute()
            ).data or []

            long_trades  = [t for t in recent if t['direction'] == 'LONG'][:20]
            short_trades = [t for t in recent if t['direction'] == 'SHORT'][:20]

            def win_rate(trades) -> float:
                if not trades:
                    return None
                wins = sum(1 for t in trades if t.get('result') == 'WIN')
                return round(wins / len(trades) * 100, 1)

            long_wr  = win_rate(long_trades)
            short_wr = win_rate(short_trades)
            now_iso  = datetime.now(timezone.utc).isoformat()

            for pair in PAIRS:
                self.supabase.table("system_state").update({
                    "long_wr_20":   long_wr,
                    "short_wr_20":  short_wr,
                    "wr_updated_at": now_iso,
                }).eq("pair", pair).execute()

            logger.info(f"[Grader] Rolling WR updated: LONG {long_wr}% ({len(long_trades)} trades) | SHORT {short_wr}% ({len(short_trades)} trades)")

        except Exception as e:
            logger.error(f"[Grader] Rolling WR update failed: {e}")

    # -------------------------------------------------------------------------
    # SUMMARY REPORT
    # -------------------------------------------------------------------------

    def send_grader_summary(self, graded_count: int, failed_count: int):
        """Sends a grading summary to operator Telegram channel."""
        import telebot
        from config import TELEGRAM_TOKEN, TELEGRAM_CHAT_ID

        if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
            return

        try:
            # Fetch updated stats
            graded = (
                self.supabase.table("trade_logs")
                .select("result, pips, confidence_score, volatility_multiplier, direction")
                .not_.is_("result", None)
                .execute()
            ).data or []

            total  = len(graded)
            wins   = sum(1 for t in graded if t.get('result') == 'WIN')
            pips   = sum(t.get('pips', 0) or 0 for t in graded)
            wr     = f"{wins/total*100:.1f}%" if total else "N/A"

            # Score tier analysis
            low_score  = [t for t in graded if (t.get('confidence_score') or 0) <= 30]
            high_score = [t for t in graded if (t.get('confidence_score') or 0) >= 86]
            extreme    = [t for t in graded if (t.get('volatility_multiplier') or 0) >= 3.0]

            def tier_wr(trades):
                if not trades:
                    return "N/A"
                w = sum(1 for t in trades if t.get('result') == 'WIN')
                return f"{w/len(trades)*100:.0f}% ({len(trades)})"

            msg = (
                f"📊 <b>GRADER REPORT</b>\n\n"
                f"This run: {graded_count} graded, {failed_count} failed\n\n"
                f"<b>All-Time Performance ({total} trades):</b>\n"
                f"Win Rate: {wr} | Net: {pips:+.1f} pips\n\n"
                f"<b>Score Tier Analysis:</b>\n"
                f"Score ≤30 (counter-trend): {tier_wr(low_score)}\n"
                f"Score ≥86 (over-consensus): {tier_wr(high_score)}\n"
                f"ATR ≥3.0× (extreme): {tier_wr(extreme)}\n\n"
                f"<i>Score inversion confirmed: lower scores outperform higher scores.</i>"
            )

            bot = telebot.TeleBot(TELEGRAM_TOKEN)
            bot.send_message(TELEGRAM_CHAT_ID, msg, parse_mode="HTML", timeout=10)

        except Exception as e:
            logger.error(f"[Grader] Summary send failed: {e}")

    # -------------------------------------------------------------------------
    # MAIN RUNNER
    # -------------------------------------------------------------------------

    def run(self):
        """
        Main grading job. Called by grader.yml GitHub Actions workflow.
        Fetches all ungraded trades and grades them.
        """
        logger.info("[Grader] Starting grading run...")

        # Fetch ungraded trades (result IS NULL)
        try:
            ungraded = (
                self.supabase.table("trade_logs")
                .select("id, pair, direction, entry_price, candle_time, timestamp_utc, confidence_score, volatility_multiplier")
                .is_("result", None)
                .order("timestamp_utc", desc=False)
                .execute()
            ).data or []
        except Exception as e:
            logger.error(f"[Grader] Failed to fetch ungraded trades: {e}")
            send_error_notification(f"Grader failed to fetch ungraded trades: {e}", "grader")
            return

        if not ungraded:
            logger.info("[Grader] No ungraded trades found.")
            return

        logger.info(f"[Grader] Found {len(ungraded)} ungraded trades.")

        # Only grade trades old enough to have exit candle available
        # (HOLD_HOURS + 15 min buffer for candle availability)
        cutoff   = datetime.now(timezone.utc) - timedelta(hours=HOLD_HOURS, minutes=15)
        to_grade = []
        for trade in ungraded:
            try:
                ts = trade.get('timestamp_utc') or trade.get('candle_time', '')
                td = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                if td.tzinfo is None:
                    td = td.replace(tzinfo=timezone.utc)
                if td <= cutoff:
                    to_grade.append(trade)
            except Exception:
                to_grade.append(trade)  # Include uncertain ones

        logger.info(f"[Grader] {len(to_grade)} trades ready for grading (old enough).")

        graded_count = 0
        failed_count = 0

        for trade in to_grade:
            if self.grade_trade(trade):
                graded_count += 1
            else:
                failed_count += 1

        logger.info(f"[Grader] Complete: {graded_count} graded, {failed_count} failed.")

        # Update rolling WR after grading
        if graded_count > 0:
            self.update_rolling_win_rates()

        # Send summary report
        self.send_grader_summary(graded_count, failed_count)


if __name__ == "__main__":
    validate_config('performance_grader')
    grader = PerformanceGrader()
    grader.run()
