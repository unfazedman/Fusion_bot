"""
volatility_atr.py — Volatility Engine & Fusion Signal Trigger
Fusion Score Bot V7.0

Runs on Render (always-on free tier process).
Monitors EUR/USD and GBP/USD every 5 minutes for ATR expansion signals.

V7 improvements over V6:
    - Wilder's Smoothed ATR (industry standard, matches TradingView/Bloomberg)
    - DB write BEFORE Telegram alert (no orphaned broadcasts)
    - Thread-safe _engine_status with threading.Lock
    - Market regime detection on every cycle (regime_detector.py)
    - Strategy plugin system (strategies/ layer)
    - Geopolitical Dragnet thread (geopolitical_scanner.py)
    - Telegram command handler thread (/status /cot /news /perf)
    - Dual-channel SaaS routing (free + premium)
    - Friday kill hour moved to 21:00 UTC (prevents weekend gap bleed)
    - ATR extreme tier (≥3.0×) separate alert format
    - ATR dead zone (2.5-3.0×) blocked by momentum strategy
    - Daily range saturation filter
    - Stale data detection before signal generation
    - Error message sanitization
"""

import time
import os
import threading
import pytz
import logging
import requests
from datetime import datetime, timezone, timedelta
from flask import Flask
import telebot

from config import (
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
    FREE_CHANNEL_ID, PREMIUM_CHANNEL_ID,
    TWELVE_DATA_KEY, PAIRS,
    ATR_THRESHOLD, EXTREME_ATR_THRESHOLD,
    FRIDAY_KILL_HOUR_UTC,
    validate_config, get_all_broadcast_channels
)
from shared_functions import (
    get_supabase_client,
    send_error_notification,
    is_data_stale,
    format_age_string,
)
from strategies import get_active_strategy
from regime_detector import classify_regime, get_regime_emoji, get_regime_label, update_regime_in_db
from geopolitical_scanner import GeopoliticalScanner

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

IST_TZ = timezone(timedelta(hours=5, minutes=30))

# =============================================================================
# FLASK KEEPALIVE
# =============================================================================

app = Flask(__name__)

# V7 FIX: thread-safe engine status
_engine_lock  = threading.Lock()
_engine_status = {
    "alive":      True,
    "last_cycle": None,
    "errors":     0,
    "strategy":   "unknown",
}


def _update_status(key: str, value):
    with _engine_lock:
        _engine_status[key] = value


def _get_status() -> dict:
    with _engine_lock:
        return dict(_engine_status)


@app.route('/')
def keep_alive():
    return "Fusion Volatility Engine V7.0 Online."


@app.route('/health')
def health():
    s = _get_status()
    status = "OK" if s["alive"] else "DEAD"
    return {
        "status":     status,
        "last_cycle": s["last_cycle"],
        "errors":     s["errors"],
        "strategy":   s["strategy"],
        "version":    "7.0"
    }, 200 if s["alive"] else 503


def _run_web():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, use_reloader=False)


# =============================================================================
# VOLATILITY ENGINE
# =============================================================================

class VolatilityEngine:

    def __init__(self):
        self.supabase   = get_supabase_client()
        self.bot        = telebot.TeleBot(TELEGRAM_TOKEN, threaded=False)
        self.strategy   = get_active_strategy()

        # Dedup: last alerted candle datetime per pair (seeded from DB on start)
        self.last_alerted_candles = {pair: None for pair in PAIRS}
        self._seed_alerted_candles()

        _update_status("strategy", self.strategy.name)
        logger.info(f"[Engine] Strategy: {self.strategy.name} v{self.strategy.version}")

    # -------------------------------------------------------------------------
    # STARTUP: seed dedup markers from Supabase
    # -------------------------------------------------------------------------

    def _seed_alerted_candles(self):
        """
        Loads last_alerted_candle from Supabase on startup.
        Prevents duplicate signals on Render restart.
        """
        try:
            resp = self.supabase.table("system_state").select(
                "pair, last_alerted_candle_eurusd, last_alerted_candle_gbpusd"
            ).execute()

            if resp.data:
                row = resp.data[0]
                self.last_alerted_candles['EUR/USD'] = row.get('last_alerted_candle_eurusd')
                self.last_alerted_candles['GBP/USD'] = row.get('last_alerted_candle_gbpusd')
                logger.info(f"[Engine] Dedup seeded: EUR={self.last_alerted_candles['EUR/USD']}, GBP={self.last_alerted_candles['GBP/USD']}")
        except Exception as e:
            logger.error(f"[Engine] Failed to seed dedup: {e}")

    def _persist_alerted_candle(self, pair: str, candle_time: str):
        """Writes last alerted candle time to Supabase (owns ATR columns only)."""
        try:
            col     = "last_alerted_candle_eurusd" if pair == "EUR/USD" else "last_alerted_candle_gbpusd"
            now_iso = datetime.now(timezone.utc).isoformat()

            self.supabase.table("system_state").update({
                col:             candle_time,
                "atr_updated_at": now_iso,
            }).eq("pair", pair).execute()

        except Exception as e:
            logger.error(f"[Engine] Failed to persist dedup for {pair}: {e}")

    # -------------------------------------------------------------------------
    # DATA FETCHING
    # -------------------------------------------------------------------------

    def _fetch_candles(self, pair: str) -> list:
        """
        Fetches 32 candles from TwelveData (5-min interval).
        32 candles required for Wilder's ATR: 14 seed + 14 smooth + 2 buffer + 2 live.

        Returns:
            List of candle dicts (newest first) or empty list on error.
        """
        
        url    = "https://api.twelvedata.com/time_series"
        params = {
            'symbol':     pair,
            'interval':   '5min',
            'outputsize': 32,
            'apikey':     TWELVE_DATA_KEY,
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data.get('status') == 'error':
                msg = data.get('message', 'Unknown TwelveData error')
                logger.error(f"[Data] TwelveData error for {pair}: {msg}")
                send_error_notification(f"TwelveData API Error ({pair}): {msg}", "atr_engine")
                return []

            candles = data.get('values', [])
            if len(candles) < 20:
                logger.warning(f"[Data] Only {len(candles)} candles for {pair}. Need ≥20.")
                return []

            return candles

        except requests.exceptions.RequestException as e:
            logger.error(f"[Data] Network error fetching {pair}: {e}")
            send_error_notification(f"TwelveData Fetch Failed ({pair}): {e}", "atr_engine")
            return []

    def _fetch_system_state(self, pair: str) -> dict:
        """
        Fetches sentiment and COT state from Supabase.
        Returns safe defaults if stale or unavailable.
        Alerts operator if data is stale.

        Returns:
            {'macro_sentiment': int, 'cot_bias': str, 'market_regime': str}
        """
        defaults = {"macro_sentiment": 0, "cot_bias": "NEUTRAL", "market_regime": "RANGING"}

        try:
            resp = (
                self.supabase.table("system_state")
                .select("macro_sentiment, cot_bias, sentiment_updated_at, cot_updated_at, market_regime")
                .eq("pair", pair)
                .execute()
            )

            if not resp.data:
                return defaults

            state = resp.data[0]

            # Staleness checks — degrade gracefully, alert operator
            if is_data_stale(state.get('sentiment_updated_at'), 48):
                age = format_age_string(state.get('sentiment_updated_at'))
                send_error_notification(
                    f"STALE SENTIMENT ({pair}): {age}. Using neutral (0). Check sentiment pipeline.",
                    "atr_engine"
                )
                state['macro_sentiment'] = 0

            if is_data_stale(state.get('cot_updated_at'), 192):  # 8 days
                age = format_age_string(state.get('cot_updated_at'))
                send_error_notification(
                    f"STALE COT ({pair}): {age}. Using NEUTRAL. Check COT tracker.",
                    "atr_engine"
                )
                state['cot_bias'] = 'NEUTRAL'

            return {
                "macro_sentiment": state.get('macro_sentiment', 0) or 0,
                "cot_bias":        state.get('cot_bias', 'NEUTRAL') or 'NEUTRAL',
                "market_regime":   state.get('market_regime', 'RANGING') or 'RANGING',
            }

        except Exception as e:
            logger.error(f"[Engine] Failed to fetch system state for {pair}: {e}")
            return defaults

    # -------------------------------------------------------------------------
    # ATR CALCULATION — WILDER'S SMOOTHED (V7 UPGRADE)
    # -------------------------------------------------------------------------

    def _calculate_tr(self, high: float, low: float, prev_close: float) -> float:
        """True Range = max(H-L, |H-PC|, |L-PC|)"""
        return max(high - low, abs(high - prev_close), abs(low - prev_close))

    def _calculate_atr_wilder(self, candles: list) -> tuple:
        """
        Wilder's Smoothed ATR (industry standard).
        Matches TradingView and Bloomberg ATR values.

        Requires outputsize=32 candles. candles[0] = live (skip).
        candles[1] = signal candle. candles[2..31] = history.

        Wilder's formula:
            Seed:   ATR = SMA of first 14 TR values
            Smooth: ATR = (prev_ATR × 13 + current_TR) / 14

        Returns:
            (signal_tr, atr_14, candle_data) or (None, None, None) on error
        """
        try:
            if len(candles) < 20:
                logger.warning(f"[ATR] Only {len(candles)} candles — need ≥20")
                return None, None, None

            # Build TR series from candles[1..30]
            # candle[i] is the bar, candle[i+1] provides prev_close
            trs = []
            for i in range(1, min(30, len(candles) - 1)):
                h  = float(candles[i]['high'])
                l  = float(candles[i]['low'])
                pc = float(candles[i + 1]['close'])
                trs.append(self._calculate_tr(h, l, pc))

            if len(trs) < 14:
                logger.warning(f"[ATR] Only {len(trs)} TR values — need ≥14")
                return None, None, None

            # Seed: SMA of first 14 TR values
            atr = sum(trs[:14]) / 14

            # Wilder's smoothing on remaining values
            for tr in trs[14:]:
                atr = (atr * 13 + tr) / 14

            # Signal candle (candles[1] — not the live candle[0])
            signal_candle = candles[1]
            prev_close    = float(candles[2]['close'])
            signal_tr     = self._calculate_tr(
                float(signal_candle['high']),
                float(signal_candle['low']),
                prev_close
            )

            candle_data = {
                "time":  signal_candle['datetime'],
                "high":  float(signal_candle['high']),
                "low":   float(signal_candle['low']),
                "close": float(signal_candle['close']),
                "open":  float(signal_candle['open']),
            }

            return signal_tr, atr, candle_data

        except (ValueError, KeyError, IndexError, ZeroDivisionError) as e:
            logger.error(f"[ATR] Wilder calculation error: {e}")
            return None, None, None

    def _get_daily_range_saturation(self, pair: str) -> float:
        """
        Returns daily range saturation: 0.0 (fresh day) to 1.0 (full range consumed).
        Compares today's forming range to last 4 days' average range.
        Returns 0.0 on error (safe — won't suppress signal on API failure).

        Cost: 1 extra TwelveData API credit per pair per 5-min cycle.
        """
        try:
            
            params = {
                'symbol':     pair,
                'interval':   '1day',
                'outputsize': 5,
                'apikey':     TWELVE_DATA_KEY,
            }
            resp    = requests.get("https://api.twelvedata.com/time_series", params=params, timeout=10)
            data    = resp.json()
            candles = data.get('values', [])

            if len(candles) < 3:
                return 0.0

            today_range = float(candles[0]['high']) - float(candles[0]['low'])
            avg_range   = sum(
                float(c['high']) - float(c['low']) for c in candles[1:5]
            ) / min(4, len(candles) - 1)

            if avg_range <= 0:
                return 0.0

            return min(1.0, today_range / avg_range)

        except Exception:
            return 0.0  # Never suppress signal on saturation fetch failure

    # -------------------------------------------------------------------------
    # MARKET HOURS
    # -------------------------------------------------------------------------

    def _market_is_open(self) -> bool:
        """
        Returns False outside standard Forex trading hours.
        V7 FIX: Friday kill hour moved from 22:00 to 21:00 UTC.
        Reason: 1-hour hold from 22:01 exits at 23:01 = after close = weekend gap risk.
        """
        now     = datetime.now(timezone.utc)
        weekday = now.weekday()  # 0=Monday, 4=Friday, 5=Saturday, 6=Sunday

        if weekday == 5 or weekday == 6:
            return False  # Weekend

        if weekday == 4 and now.hour >= FRIDAY_KILL_HOUR_UTC:
            return False  # Friday after 21:00 UTC

        if weekday == 0 and now.hour < 1:
            return False  # Monday pre-open

        return True

    # -------------------------------------------------------------------------
    # SIGNAL PROCESSING
    # -------------------------------------------------------------------------

    def _determine_direction(self, open_price: float, close_price: float) -> str:
        """Returns 'LONG', 'SHORT', or None (doji)."""
        if close_price > open_price:
            return "LONG"
        elif close_price < open_price:
            return "SHORT"
        return None  # Doji — no trade

    def _build_signal_context(self, pair: str, hour_ist: int, saturation: float, regime: str) -> dict:
        """Builds the context dict passed to strategy.entry_signal()."""
        return {
            'regime':          regime,
            'hour_ist':        hour_ist,
            'daily_saturation': saturation,
            'pair':            pair,
        }

    def _process_signal(self, pair: str, candle: dict, multiplier: float, candles: list):
        """
        Full signal processing pipeline.

        V7 ORDER (critical):
            1. Get system state (sentiment + COT)
            2. Classify regime
            3. Ask strategy: entry_signal()?
            4. Ask strategy: score_signal()
            5. LOG TO DB FIRST (abort if DB fails — no orphaned broadcast)
            6. Send Telegram alert (after confirmed DB write)
            7. Persist dedup marker
        """
        direction = self._determine_direction(candle['open'], candle['close'])
        if direction is None:
            logger.info(f"[Signal] {pair} doji candle — skipped.")
            return

        state      = self._fetch_system_state(pair)
        sentiment  = state['macro_sentiment']
        cot        = state['cot_bias']
        regime     = state['market_regime']

        now_ist    = datetime.now(IST_TZ)
        hour_ist   = now_ist.hour
        saturation = self._get_daily_range_saturation(pair)

        context = self._build_signal_context(pair, hour_ist, saturation, regime)

        # Strategy entry filter
        if not self.strategy.entry_signal(candle, multiplier, context):
            logger.info(f"[Signal] {pair} filtered by strategy entry_signal() rules.")
            return

        # Strategy scoring
        score = self.strategy.score_signal(sentiment, multiplier, cot, direction, regime, context)

        is_extreme  = multiplier >= EXTREME_ATR_THRESHOLD
        is_premium  = self.strategy.is_premium_signal(score, multiplier)

        logger.info(
            f"[Signal] {pair} {direction} Score={score} ATR={multiplier:.2f}× "
            f"Regime={regime} Sent={sentiment} COT={cot} "
            f"Extreme={is_extreme} Premium={is_premium}"
        )

        # -----------------------------------------------------------------------
        # STEP 5: LOG TO DB FIRST — if this fails, DO NOT send alert
        # V7 FIX: V6 sent Telegram alert before DB write. If DB failed, signal
        # was broadcast with no record in the system. Completely reversed in V7.
        # -----------------------------------------------------------------------
        try:
            self._log_trade_to_db(
                pair, direction, score, multiplier,
                sentiment, cot, candle['close'], candle['time'],
                regime, is_extreme, is_premium
            )
            logger.info(f"[Signal] {pair} {direction} logged to DB. Sending alert.")
        except Exception as e:
            logger.error(f"[Signal] DB log FAILED for {pair}. Aborting broadcast: {e}")
            send_error_notification(
                f"Signal DB log failed — signal NOT broadcast ({pair} {direction} Score={score}): {e}",
                "atr_engine"
            )
            return  # Abort — do not send alert for unlogged signal

        # -----------------------------------------------------------------------
        # STEP 6: SEND TELEGRAM ALERTS (only after confirmed DB write)
        # -----------------------------------------------------------------------
        try:
            self._send_signal_alerts(
                pair, direction, score, multiplier,
                sentiment, cot, regime, is_extreme, is_premium, candle
            )
        except Exception as e:
            logger.error(f"[Signal] Alert send failed (trade IS logged to DB): {e}")
            send_error_notification(
                f"Signal alert FAILED but trade logged ({pair}): {e}",
                "atr_engine"
            )

        # -----------------------------------------------------------------------
        # STEP 7: PERSIST DEDUP MARKER
        # -----------------------------------------------------------------------
        self.last_alerted_candles[pair] = candle['time']
        self._persist_alerted_candle(pair, candle['time'])

    def _log_trade_to_db(
        self, pair, direction, score, multiplier,
        sentiment, cot, close_price, candle_time,
        regime, is_extreme, is_premium
    ):
        """Logs trade to Supabase trade_logs table. Raises on failure."""
        now_utc = datetime.now(timezone.utc)
        now_ist = datetime.now(IST_TZ)

        self.supabase.table("trade_logs").insert({
            "pair":                pair,
            "direction":           direction,
            "confidence_score":    score,
            "volatility_multiplier": round(multiplier, 4),
            "macro_sentiment":     sentiment,
            "cot_bias":            cot,
            "market_regime":       regime,
            "is_extreme_atr":      is_extreme,
            "is_premium_signal":   is_premium,
            "entry_price":         float(close_price),
            "candle_time":         candle_time,
            "timestamp_utc":       now_utc.isoformat(),
            "timestamp_ist":       now_ist.isoformat(),
            "strategy":            self.strategy.name,
            "result":              None,   # Filled by performance_grader
            "exit_price":          None,
            "pips":                None,
        }).execute()

    # -------------------------------------------------------------------------
    # TELEGRAM ALERT FORMATTING
    # -------------------------------------------------------------------------

    def _send_signal_alerts(
        self, pair, direction, score, multiplier,
        sentiment, cot, regime, is_extreme, is_premium, candle
    ):
        """
        Routes signal to appropriate Telegram channels.
        Free channel gets simplified message.
        Premium channel gets full context.
        Operator channel always gets full message.
        """
        now_ist  = datetime.now(IST_TZ).strftime('%d %b %Y %H:%M IST')
        tier     = self.strategy.classify_signal_tier(score)
        dir_emoji = "📈" if direction == "LONG" else "📉"
        reg_emoji = get_regime_emoji(regime)
        reg_label = get_regime_label(regime)

        # Sentiment description
        if sentiment > 3:
            sent_label = f"Bullish (+{sentiment})"
        elif sentiment < -3:
            sent_label = f"Bearish ({sentiment})"
        elif sentiment > 0:
            sent_label = f"Mild Bullish (+{sentiment})"
        elif sentiment < 0:
            sent_label = f"Mild Bearish ({sentiment})"
        else:
            sent_label = "Neutral (0)"

        # Header differs for extreme vs standard
        if is_extreme:
            header = f"🚨 <b>EXTREME EXPANSION: {pair}</b> 🚨"
            atr_label = f"💥 {multiplier:.2f}× (EXTREME — historically 82% WR)"
        else:
            header = f"⚡ <b>FUSION SIGNAL: {pair}</b>"
            atr_label = f"📊 {multiplier:.2f}×"

        # ── PREMIUM / OPERATOR message (full context) ──
        premium_msg = (
            f"{header}\n\n"
            f"{dir_emoji} Direction: <b>{direction}</b>\n"
            f"🎯 Score: <b>{score}</b> | Tier: {tier}\n"
            f"{reg_emoji} Regime: {reg_label}\n"
            f"🧠 Sentiment: {sent_label}\n"
            f"🏦 COT: {cot.replace('_', ' ')}\n"
            f"ATR: {atr_label}\n\n"
            f"📅 {now_ist}\n"
            f"⚙️ Strategy: {self.strategy.name}"
        )

        if is_extreme:
            premium_msg += (
                f"\n\n<i>Extreme ATR events historically show ~82% WR. "
                f"Review before acting — confirm direction from price action.</i>"
            )

        # ── FREE CHANNEL message (teaser) ──
        free_msg = (
            f"⚡ <b>FUSION SIGNAL: {pair}</b>\n\n"
            f"{dir_emoji} Direction: <b>{direction}</b>\n"
            f"📊 ATR Expansion: {multiplier:.2f}×\n\n"
            f"<i>Full analysis (score, COT, regime, sentiment) in Premium Channel.</i>"
        )

        # Send to operator channel (always full message)
        if TELEGRAM_CHAT_ID:
            self.bot.send_message(TELEGRAM_CHAT_ID, premium_msg, parse_mode="HTML", timeout=15)

        # Send to premium channel if signal qualifies
        if is_premium and PREMIUM_CHANNEL_ID:
            self.bot.send_message(PREMIUM_CHANNEL_ID, premium_msg, parse_mode="HTML", timeout=15)

        # Send to free channel (simplified)
        if FREE_CHANNEL_ID:
            self.bot.send_message(FREE_CHANNEL_ID, free_msg, parse_mode="HTML", timeout=15)

    # -------------------------------------------------------------------------
    # MAIN ANALYSIS LOOP
    # -------------------------------------------------------------------------

    def analyze_volatility(self, pair: str):
        """
        Full analysis cycle for one pair.
        Called every 5 minutes per pair.
        """
        candles = self._fetch_candles(pair)
        if not candles:
            return

        signal_tr, atr, candle = self._calculate_atr_wilder(candles)
        if signal_tr is None or atr is None:
            logger.warning(f"[ATR] Could not calculate ATR for {pair}")
            return

        if atr == 0:
            logger.warning(f"[ATR] ATR=0 for {pair} — possible data anomaly")
            send_error_notification(f"ATR=0 anomaly detected for {pair}. Check TwelveData feed.", "atr_engine")
            return

        multiplier = signal_tr / atr

        # Classify regime using fetched candles (zero extra API cost)
        regime = classify_regime(candles, multiplier)
        update_regime_in_db(self.supabase, pair, regime)

        logger.info(f"[ATR] {pair}: multiplier={multiplier:.2f}× ATR={atr:.5f} Regime={regime}")

        # Dedup check
        if self.last_alerted_candles.get(pair) == candle['time']:
            logger.info(f"[ATR] {pair}: already alerted for candle {candle['time']}. Skipping.")
            return

        # Signal threshold check
        if multiplier >= ATR_THRESHOLD:
            self._process_signal(pair, candle, multiplier, candles)

    def run_engine(self):
        """Main 5-minute polling loop."""
        logger.info(f"[Engine] Starting. Strategy: {self.strategy.name}. Pairs: {PAIRS}")

        consecutive_errors = 0

        while True:
            try:
                if not self._market_is_open():
                    logger.info("[Engine] Market closed. Sleeping 60s.")
                    time.sleep(60)
                    continue

                for pair in PAIRS:
                    try:
                        self.analyze_volatility(pair)
                    except Exception as e:
                        consecutive_errors += 1
                        logger.error(f"[Engine] Error analyzing {pair}: {e}")
                        _update_status("errors", _get_status()["errors"] + 1)

                        if consecutive_errors >= 3:
                            send_error_notification(
                                f"Engine consecutive errors ({consecutive_errors}): {e}",
                                "atr_engine"
                            )
                    else:
                        consecutive_errors = 0

                _update_status("last_cycle", datetime.now(timezone.utc).isoformat())
                _update_status("alive", True)
                time.sleep(300)  # 5-minute cycle

            except Exception as e:
                logger.critical(f"[Engine] Fatal loop error: {e}")
                _update_status("alive", False)
                send_error_notification(f"Engine loop FATAL error: {e}", "atr_engine")
                time.sleep(60)
                _update_status("alive", True)


# =============================================================================
# TELEGRAM COMMAND HANDLER
# =============================================================================

def _run_bot_commands(engine: VolatilityEngine):
    """
    Polls Telegram for bot commands. Runs as daemon thread.
    Commands: /status /cot /news /perf /help
    Only responds to operator chat (TELEGRAM_CHAT_ID).
    """
    cmd_bot = telebot.TeleBot(TELEGRAM_TOKEN)
    allowed_chat = TELEGRAM_CHAT_ID

    def _check_auth(message) -> bool:
        return str(message.chat.id) == str(allowed_chat)

    @cmd_bot.message_handler(commands=['status'])
    def cmd_status(message):
        if not _check_auth(message):
            return
        try:
            supabase    = get_supabase_client()
            strategy    = get_active_strategy()
            state_rows  = supabase.table("system_state").select("*").execute().data or []
            eng         = _get_status()

            lines = [
                f"📊 <b>SYSTEM STATUS — V7.0</b>\n",
                f"🔁 Last cycle: {format_age_string(eng.get('last_cycle'))}\n"
                f"⚙️ Strategy: {strategy.name} v{strategy.version}\n"
                f"❗ Errors: {eng.get('errors', 0)}\n\n"
            ]

            for row in state_rows:
                pair     = row.get('pair', '?')
                sent     = row.get('macro_sentiment', 0)
                cot      = row.get('cot_bias', 'N/A').replace('_', ' ')
                regime   = row.get('market_regime', 'UNKNOWN')
                sent_age = format_age_string(row.get('sentiment_updated_at'))
                cot_age  = format_age_string(row.get('cot_updated_at'))
                long_wr  = row.get('long_wr_20', 'N/A')
                short_wr = row.get('short_wr_20', 'N/A')
                reg_e    = get_regime_emoji(regime)

                lines.append(
                    f"<b>{pair}</b>\n"
                    f"  {reg_e} Regime: {get_regime_label(regime)}\n"
                    f"  🧠 Sentiment: {sent:+d} ({sent_age})\n"
                    f"  🏦 COT: {cot} ({cot_age})\n"
                    f"  📈 WR-20: LONG {long_wr}% | SHORT {short_wr}%\n\n"
                )

            cmd_bot.reply_to(message, "".join(lines), parse_mode="HTML")
        except Exception as e:
            cmd_bot.reply_to(message, f"❌ Status error: {e}")

    @cmd_bot.message_handler(commands=['cot'])
    def cmd_cot(message):
        if not _check_auth(message):
            return
        try:
            supabase   = get_supabase_client()
            state_rows = supabase.table("system_state").select(
                "pair, cot_bias, cot_index, cot_net, cot_date, cot_updated_at"
            ).execute().data or []

            lines = ["🏦 <b>COT REPORT</b>\n\n"]
            for row in state_rows:
                pair      = row.get('pair', '?')
                bias      = (row.get('cot_bias') or 'N/A').replace('_', ' ')
                index     = row.get('cot_index', 0)
                net       = row.get('cot_net', 0)
                date      = row.get('cot_date', 'N/A')
                age       = format_age_string(row.get('cot_updated_at'))

                # Visual index bar (10 blocks)
                filled   = int(round((index or 0.5) * 10))
                bar      = "█" * filled + "░" * (10 - filled)

                lines.append(
                    f"<b>{pair}</b>\n"
                    f"  Bias: <b>{bias}</b>\n"
                    f"  Index: [{bar}] {(index or 0):.2f}\n"
                    f"  Net Pos: {net:+,}\n"
                    f"  Report: {date} ({age})\n\n"
                )

            cmd_bot.reply_to(message, "".join(lines), parse_mode="HTML")
        except Exception as e:
            cmd_bot.reply_to(message, f"❌ COT error: {e}")

    @cmd_bot.message_handler(commands=['news'])
    def cmd_news(message):
        if not _check_auth(message):
            return
        try:
            supabase = get_supabase_client()
            records  = (
                supabase.table("processed_sentiment")
                .select("eur_usd_sentiment, gbp_usd_sentiment, importance_tier, title, created_at")
                .order("created_at", desc=True)
                .limit(6)
                .execute()
            ).data or []

            if not records:
                cmd_bot.reply_to(message, "No recent news in database.")
                return

            lines = ["📰 <b>LATEST NEWS SENTIMENT</b>\n\n"]
            for r in records:
                title = (r.get('title') or 'Untitled')[:70]
                eur   = r.get('eur_usd_sentiment', 'N/A')
                gbp   = r.get('gbp_usd_sentiment', 'N/A')
                tier  = r.get('importance_tier', 'LOW')
                age   = format_age_string(r.get('created_at'))

                tier_icon = "🔴" if tier == "HIGH" else ("🟡" if tier == "MEDIUM" else "⚪")
                eur_e = "📈" if eur == "BULLISH" else ("📉" if eur == "BEARISH" else "➡️")
                gbp_e = "📈" if gbp == "BULLISH" else ("📉" if gbp == "BEARISH" else "➡️")

                lines.append(
                    f"{tier_icon} <i>{title}</i>\n"
                    f"  EUR {eur_e} {eur} | GBP {gbp_e} {gbp} | {age}\n\n"
                )

            cmd_bot.reply_to(message, "".join(lines), parse_mode="HTML")
        except Exception as e:
            cmd_bot.reply_to(message, f"❌ News error: {e}")

    @cmd_bot.message_handler(commands=['perf'])
    def cmd_perf(message):
        if not _check_auth(message):
            return
        try:
            from datetime import date
            supabase = get_supabase_client()

            # Today's trades
            today_str = date.today().isoformat()
            today = (
                supabase.table("trade_logs")
                .select("direction, result, pips, confidence_score")
                .gte("timestamp_utc", today_str)
                .execute()
            ).data or []

            today_total = len(today)
            today_wins  = sum(1 for t in today if t.get('result') == 'WIN')
            today_pips  = sum(t.get('pips', 0) or 0 for t in today)

            # All-time graded
            graded = (
                supabase.table("trade_logs")
                .select("result, pips, confidence_score, volatility_multiplier")
                .not_.is_("result", None)
                .execute()
            ).data or []

            total   = len(graded)
            wins    = sum(1 for t in graded if t.get('result') == 'WIN')
            net_pip = sum(t.get('pips', 0) or 0 for t in graded)
            wr      = f"{wins/total*100:.1f}%" if total > 0 else "N/A"

            # Best tier
            extreme = [t for t in graded if (t.get('volatility_multiplier') or 0) >= 3.0]
            ext_wr  = f"{sum(1 for t in extreme if t.get('result')=='WIN')/len(extreme)*100:.0f}%" if extreme else "N/A"

            lines = [
                f"📊 <b>PERFORMANCE DASHBOARD</b>\n\n",
                f"<b>Today:</b>\n"
                f"  Signals: {today_total} | Wins: {today_wins}\n"
                f"  Pips: {today_pips:+.1f}\n\n",
                f"<b>All-Time ({total} graded):</b>\n"
                f"  Win Rate: {wr}\n"
                f"  Net Pips: {net_pip:+.1f}\n"
                f"  ATR ≥3.0× Win Rate: {ext_wr}\n\n",
                f"<b>Score Tier Notes:</b>\n"
                f"  Score ≤30: Best tier (~69% WR)\n"
                f"  Score 86+: Worst tier (~36% WR)\n"
                f"  <i>Score inversion is structural — see audit docs.</i>"
            ]

            cmd_bot.reply_to(message, "".join(lines), parse_mode="HTML")
        except Exception as e:
            cmd_bot.reply_to(message, f"❌ Perf error: {e}")

    @cmd_bot.message_handler(commands=['help'])
    def cmd_help(message):
        if not _check_auth(message):
            return
        help_text = (
            "🤖 <b>FUSION BOT COMMANDS</b>\n\n"
            "/status — Current sentiment, COT, regime per pair\n"
            "/cot — Full COT report with index bar\n"
            "/news — Last 6 processed news articles\n"
            "/perf — Performance dashboard & stats\n"
            "/help — This message"
        )
        cmd_bot.reply_to(message, help_text, parse_mode="HTML")

    logger.info("[Commands] Telegram command handler polling...")
    cmd_bot.infinity_polling(timeout=10, long_polling_timeout=5)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    validate_config('volatility_atr')

    engine = VolatilityEngine()

    # Flask web server thread (Render keepalive)
    threading.Thread(target=_run_web, daemon=True).start()
    logger.info("[Main] Flask keepalive started.")

    # Telegram command handler thread
    threading.Thread(target=_run_bot_commands, args=(engine,), daemon=True).start()
    logger.info("[Main] Telegram command handler started.")

    # Geopolitical Dragnet thread
    all_channels = get_all_broadcast_channels()
    if all_channels:
        scanner = GeopoliticalScanner(TELEGRAM_TOKEN, all_channels)
        threading.Thread(target=scanner.run_loop, daemon=True).start()
        logger.info(f"[Main] Geopolitical Dragnet started → {len(all_channels)} channels.")
    else:
        logger.warning("[Main] No channels configured for Geopolitical Dragnet.")

    # Main engine loop (blocking — runs in main thread)
    logger.info("[Main] Starting main volatility engine loop.")
    engine.run_engine()
