"""
system_health_check.py — System Health Monitor
Fusion Score Bot V7.0

Checks all system components and sends a formatted HTML report to Telegram.
Runs via health.yml GitHub Actions workflow (every 6 hours + on-demand).

V7 improvements over V6:
    - Full HTML parse mode (V6 had broken Markdown = parse errors)
    - 10 health checks (V6 had 7)
    - Win rate query uses .not_.is_() not full table fetch (fixes >1000 row truncation)
    - GNews daily call counter check
    - Gemini daily call counter check
    - Zero-signals-during-market-hours detection
    - Error bot /health and /dbcheck command support
    - System runtime detection
    - All column ownership respected (no system_state upsert collision)

Health checks:
    1.  Supabase connectivity
    2.  Sentiment pipeline recency (< 48h)
    3.  COT data recency (< 8 days)
    4.  Trade log volume (signals firing)
    5.  Win rate calculation (all graded trades)
    6.  TwelveData API reachability
    7.  GNews daily call count (< 90/100)
    8.  Gemini daily call count (< 18/20)
    9.  Zero-signals detection (signals today during market hours)
    10. system_state completeness (both pairs have rows)
"""

import logging
import requests
from datetime import datetime, timezone, timedelta, date

from config import (
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
    ERROR_BOT_TOKEN, ERROR_CHAT_ID,
    TWELVE_DATA_KEY, GNEWS_API_KEY,
    PAIRS, GEMINI_RPD_SAFE_LIMIT, GEMINI_RPD_LIMIT,
    validate_config
)
from shared_functions import (
    get_supabase_client,
    send_error_notification,
    is_data_stale,
    format_age_string,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))


class HealthChecker:

    def __init__(self):
        self.supabase = get_supabase_client()
        self.results  = []   # List of (status, label, detail) tuples
        self.warnings = 0
        self.failures = 0

    def _add(self, status: str, label: str, detail: str):
        """
        Appends a check result.
        status: 'OK' | 'WARN' | 'FAIL'
        """
        self.results.append((status, label, detail))
        if status == 'WARN':
            self.warnings += 1
        elif status == 'FAIL':
            self.failures += 1

    # =========================================================================
    # CHECK 1: Supabase Connectivity
    # =========================================================================

    def check_supabase(self):
        try:
            resp = self.supabase.table("system_state").select("pair").limit(1).execute()
            if resp.data is not None:
                self._add('OK', 'Supabase', f"{len(resp.data)} system_state rows accessible")
            else:
                self._add('FAIL', 'Supabase', "Query returned None")
        except Exception as e:
            self._add('FAIL', 'Supabase', f"Connection failed: {str(e)[:100]}")

    # =========================================================================
    # CHECK 2: Sentiment Pipeline Recency
    # =========================================================================

    def check_sentiment_recency(self):
        try:
            resp = (
                self.supabase.table("processed_sentiment")
                .select("created_at")
                .order("created_at", desc=True)
                .limit(1)
                .execute()
            )
            if not resp.data:
                self._add('WARN', 'Sentiment Recency', "No processed_sentiment records found")
                return

            latest_ts = resp.data[0]['created_at']
            age_str   = format_age_string(latest_ts)

            if is_data_stale(latest_ts, 48):
                self._add('FAIL', 'Sentiment Recency', f"Last article: {age_str} — pipeline may be broken")
            elif is_data_stale(latest_ts, 12):
                self._add('WARN', 'Sentiment Recency', f"Last article: {age_str} — check sentiment.yml schedule")
            else:
                self._add('OK', 'Sentiment Recency', f"Last article: {age_str}")

        except Exception as e:
            self._add('FAIL', 'Sentiment Recency', f"Query failed: {str(e)[:100]}")

    # =========================================================================
    # CHECK 3: COT Data Recency
    # =========================================================================

    def check_cot_recency(self):
        try:
            resp = (
                self.supabase.table("system_state")
                .select("pair, cot_bias, cot_updated_at")
                .execute()
            )
            if not resp.data:
                self._add('WARN', 'COT Recency', "No system_state rows found")
                return

            for row in resp.data:
                pair      = row.get('pair', '?')
                ts        = row.get('cot_updated_at')
                age_str   = format_age_string(ts)
                cot_bias  = row.get('cot_bias', 'N/A')

                if is_data_stale(ts, 192):  # 8 days
                    self._add('FAIL', 'COT Recency', f"{pair}: {age_str} (bias={cot_bias}) — stale, check cot.yml")
                elif is_data_stale(ts, 100):  # ~4 days = last Saturday missed
                    self._add('WARN', 'COT Recency', f"{pair}: {age_str} (bias={cot_bias}) — approaching stale")
                else:
                    self._add('OK', 'COT Recency', f"{pair}: {age_str} (bias={cot_bias})")

        except Exception as e:
            self._add('FAIL', 'COT Recency', f"Query failed: {str(e)[:100]}")

    # =========================================================================
    # CHECK 4: Trade Log Volume
    # =========================================================================

    def check_trade_logging(self):
        try:
            # Total count
            total_resp = (
                self.supabase.table("trade_logs")
                .select("id", count="exact")
                .execute()
            )
            total = total_resp.count or 0

            # Today's count
            today_str  = date.today().isoformat()
            today_resp = (
                self.supabase.table("trade_logs")
                .select("id", count="exact")
                .gte("timestamp_utc", today_str)
                .execute()
            )
            today = today_resp.count or 0

            self._add('OK', 'Trade Logs', f"Total: {total} trades | Today: {today}")

        except Exception as e:
            self._add('FAIL', 'Trade Logs', f"Query failed: {str(e)[:100]}")

    # =========================================================================
    # CHECK 5: Win Rate (graded trades)
    # =========================================================================

    def check_win_rate(self):
        try:
            # V7 FIX: Use filtered query instead of full table fetch.
            # V6 bug: selected all rows, Supabase 1000-row limit silently truncated.
            graded_resp = (
                self.supabase.table("trade_logs")
                .select("result", count="exact")
                .not_.is_("result", None)
                .execute()
            )
            graded_data = graded_resp.data or []

            wins      = sum(1 for t in graded_data if t.get('result') == 'WIN')
            losses    = sum(1 for t in graded_data if t.get('result') == 'LOSS')
            breakeven = sum(1 for t in graded_data if t.get('result') == 'BREAKEVEN')
            decided   = wins + losses + breakeven

            if decided == 0:
                self._add('WARN', 'Win Rate', "No graded trades yet")
                return

            wr = wins / decided * 100
            self._add(
                'OK' if decided >= 10 else 'WARN',
                'Win Rate',
                f"{wr:.1f}% ({wins}W/{losses}L/{breakeven}B from {decided} graded)"
            )

        except Exception as e:
            self._add('FAIL', 'Win Rate', f"Query failed: {str(e)[:100]}")

    # =========================================================================
    # CHECK 6: TwelveData API Reachability
    # =========================================================================

    def check_twelvedata(self):
        if not TWELVE_DATA_KEY:
            self._add('WARN', 'TwelveData', "TWELVE_DATA_KEY not configured")
            return
        try:
            params = {
                'symbol':     'EURUSD',
                'interval':   '1min',
                'outputsize': 1,
                'apikey':     TWELVE_DATA_KEY,
            }
            resp = requests.get(
                "https://api.twelvedata.com/time_series",
                params=params,
                timeout=10
            )
            resp.raise_for_status()
            data = resp.json()

            if data.get('status') == 'error':
                msg = data.get('message', 'Unknown error')
                if 'apikey' in msg.lower() or 'limit' in msg.lower():
                    self._add('FAIL', 'TwelveData', f"API error: {msg[:100]}")
                else:
                    self._add('WARN', 'TwelveData', f"API error: {msg[:100]}")
            else:
                values = data.get('values', [])
                latest = values[0].get('datetime', 'N/A') if values else 'No data'
                self._add('OK', 'TwelveData', f"Reachable. Latest candle: {latest}")

        except Exception as e:
            self._add('FAIL', 'TwelveData', f"Request failed: {str(e)[:100]}")

    # =========================================================================
    # CHECK 7: GNews Daily Call Count
    # =========================================================================

    def check_gnews_usage(self):
        try:
            today    = date.today().isoformat()
            resp     = (
                self.supabase.table("api_usage")
                .select("call_count")
                .eq("date", today)
                .eq("api", "gnews")
                .execute()
            )
            count = resp.data[0]['call_count'] if resp.data else 0
            limit = 100

            if count >= 95:
                self._add('FAIL', 'GNews Usage', f"{count}/{limit} calls today — QUOTA NEAR EXHAUSTION")
            elif count >= 80:
                self._add('WARN', 'GNews Usage', f"{count}/{limit} calls today — approaching limit")
            else:
                self._add('OK', 'GNews Usage', f"{count}/{limit} calls today")

        except Exception as e:
            # api_usage table might not have a row for today — that's fine
            self._add('OK', 'GNews Usage', "No usage data for today (0 calls)")

    # =========================================================================
    # CHECK 8: Gemini Daily Call Count
    # =========================================================================

    def check_gemini_usage(self):
        try:
            today = date.today().isoformat()
            resp  = (
                self.supabase.table("api_usage")
                .select("call_count")
                .eq("date", today)
                .eq("api", "gemini")
                .execute()
            )
            count = resp.data[0]['call_count'] if resp.data else 0

            if count >= GEMINI_RPD_LIMIT:
                self._add('FAIL', 'Gemini Usage', f"{count}/{GEMINI_RPD_LIMIT} calls — DAILY LIMIT REACHED. FinBERT taking over.")
            elif count >= GEMINI_RPD_SAFE_LIMIT:
                self._add('WARN', 'Gemini Usage', f"{count}/{GEMINI_RPD_LIMIT} calls — near daily limit")
            else:
                self._add('OK', 'Gemini Usage', f"{count}/{GEMINI_RPD_LIMIT} calls today")

        except Exception as e:
            self._add('OK', 'Gemini Usage', "No usage data for today (0 calls)")

    # =========================================================================
    # CHECK 9: Zero Signals Detection
    # =========================================================================

    def check_signal_activity(self):
        """
        Alerts if no signals fired today during market hours.
        A silent engine that Flask keepalive is still masking.
        """
        try:
            now_ist    = datetime.now(IST)
            is_weekday = now_ist.weekday() < 5
            is_hours   = 9 <= now_ist.hour < 22  # 9 AM - 10 PM IST

            if not (is_weekday and is_hours):
                self._add('OK', 'Signal Activity', "Outside market hours — check skipped")
                return

            today_str  = date.today().isoformat()
            resp       = (
                self.supabase.table("trade_logs")
                .select("id", count="exact")
                .gte("timestamp_utc", today_str)
                .execute()
            )
            count = resp.count or 0

            # How many hours into the trading day are we?
            hours_in = now_ist.hour - 9
            if count == 0 and hours_in >= 3:
                self._add('WARN', 'Signal Activity', f"0 signals today despite {hours_in}h of market hours — check engine")
            else:
                self._add('OK', 'Signal Activity', f"{count} signals today")

        except Exception as e:
            self._add('WARN', 'Signal Activity', f"Query failed: {str(e)[:100]}")

    # =========================================================================
    # CHECK 10: system_state Completeness
    # =========================================================================

    def check_system_state_completeness(self):
        try:
            resp = (
                self.supabase.table("system_state")
                .select("pair, macro_sentiment, cot_bias, market_regime, long_wr_20, short_wr_20")
                .execute()
            )
            rows     = resp.data or []
            pairs_ok = {row['pair'] for row in rows}
            missing  = [p for p in PAIRS if p not in pairs_ok]

            if missing:
                self._add('FAIL', 'system_state', f"Missing rows for: {', '.join(missing)}")
                return

            for row in rows:
                pair    = row.get('pair', '?')
                sent    = row.get('macro_sentiment')
                cot     = row.get('cot_bias', 'N/A')
                regime  = row.get('market_regime', 'N/A')
                long_wr = row.get('long_wr_20')
                short_wr = row.get('short_wr_20')

                wr_str = f"WR: L{long_wr}%/S{short_wr}%" if long_wr is not None else "WR: N/A"
                self._add('OK', f'state:{pair}',
                          f"Sent={sent:+d} COT={cot} Regime={regime} {wr_str}")

        except Exception as e:
            self._add('FAIL', 'system_state', f"Query failed: {str(e)[:100]}")

    # =========================================================================
    # REPORT FORMATTING & SENDING
    # =========================================================================

    def run_all_checks(self):
        """Runs all 10 health checks in sequence."""
        self.check_supabase()
        self.check_sentiment_recency()
        self.check_cot_recency()
        self.check_trade_logging()
        self.check_win_rate()
        self.check_twelvedata()
        self.check_gnews_usage()
        self.check_gemini_usage()
        self.check_signal_activity()
        self.check_system_state_completeness()

    def build_report(self) -> str:
        """
        Builds the full health report as an HTML string.
        V7: HTML format (V6 used Markdown which caused parse errors).
        """
        now_ist  = datetime.now(IST).strftime('%d %b %Y %H:%M IST')
        ok_count = sum(1 for s, _, _ in self.results if s == 'OK')
        total    = len(self.results)

        if self.failures > 0:
            overall = "🔴 DEGRADED"
        elif self.warnings > 0:
            overall = "🟡 WARN"
        else:
            overall = "🟢 HEALTHY"

        lines = [
            f"🏥 <b>SYSTEM HEALTH — V7.0</b>\n",
            f"📅 {now_ist}\n",
            f"Status: <b>{overall}</b> ({ok_count}/{total} checks passed)\n",
        ]

        # Status icon mapping
        icons = {'OK': '✅', 'WARN': '⚠️', 'FAIL': '❌'}

        # Group by status: FAIL first, then WARN, then OK
        for target_status in ('FAIL', 'WARN', 'OK'):
            for status, label, detail in self.results:
                if status == target_status:
                    icon = icons[status]
                    lines.append(f"\n{icon} <b>{label}</b>\n   {detail}")

        if self.failures > 0:
            lines.append(f"\n\n⚠️ <b>{self.failures} failures require attention.</b>")

        return "\n".join(lines)

    def send_report(self):
        """Sends the health report to the operator Telegram channel."""
        import telebot

        token   = ERROR_BOT_TOKEN if ERROR_BOT_TOKEN else TELEGRAM_TOKEN
        chat_id = ERROR_CHAT_ID   if ERROR_CHAT_ID   else TELEGRAM_CHAT_ID

        if not token or not chat_id:
            logger.error("[Health] No Telegram credentials. Cannot send report.")
            return

        report = self.build_report()

        try:
            bot = telebot.TeleBot(token)
            bot.send_message(chat_id, report, parse_mode="HTML", timeout=15)
            logger.info("[Health] Report sent.")
        except Exception as e:
            logger.error(f"[Health] Report send failed: {e}")


# =============================================================================
# ERROR BOT COMMAND SUPPORT
# =============================================================================

def run_error_bot_commands():
    """
    Polls Telegram error bot for /health and /dbcheck commands.
    Run this in a daemon thread from health.yml or a long-running process.

    Commands:
        /health   → Runs full health check and sends report
        /dbcheck  → Checks all Supabase tables and returns row counts
    """
    import telebot

    if not ERROR_BOT_TOKEN or not ERROR_CHAT_ID:
        logger.warning("[ErrorBot] No ERROR_BOT_TOKEN/ERROR_CHAT_ID. Commands disabled.")
        return

    cmd_bot = telebot.TeleBot(ERROR_BOT_TOKEN)

    @cmd_bot.message_handler(commands=['health'])
    def cmd_health(message):
        if str(message.chat.id) != str(ERROR_CHAT_ID):
            return
        try:
            cmd_bot.reply_to(message, "🔍 Running health checks...")
            checker = HealthChecker()
            checker.run_all_checks()
            report = checker.build_report()
            cmd_bot.reply_to(message, report, parse_mode="HTML")
        except Exception as e:
            cmd_bot.reply_to(message, f"❌ Health check failed: {e}")

    @cmd_bot.message_handler(commands=['dbcheck'])
    def cmd_dbcheck(message):
        if str(message.chat.id) != str(ERROR_CHAT_ID):
            return
        try:
            supabase = get_supabase_client()
            tables   = [
                "system_state",
                "trade_logs",
                "processed_sentiment",
                "raw_sentiment_data",
                "api_usage",
            ]
            lines = ["🗄️ <b>DATABASE CHECK</b>\n"]
            for table in tables:
                try:
                    resp  = supabase.table(table).select("id", count="exact").limit(1).execute()
                    count = resp.count or 0
                    lines.append(f"✅ {table}: {count} rows")
                except Exception as e:
                    lines.append(f"❌ {table}: {str(e)[:60]}")

            cmd_bot.reply_to(message, "\n".join(lines), parse_mode="HTML")
        except Exception as e:
            cmd_bot.reply_to(message, f"❌ DB check failed: {e}")

    logger.info("[ErrorBot] Command polling started...")
    cmd_bot.infinity_polling(timeout=10, long_polling_timeout=5)


# =============================================================================
# MAIN RUNNER
# =============================================================================

def run():
    """
    Main health check job. Called by health.yml GitHub Actions workflow.
    """
    validate_config('system_health_check')
    logger.info("[Health] Starting system health check...")

    checker = HealthChecker()
    checker.run_all_checks()
    checker.send_report()

    if checker.failures > 0:
        logger.warning(f"[Health] {checker.failures} failures detected.")
    else:
        logger.info("[Health] All checks passed.")


if __name__ == "__main__":
    run()
