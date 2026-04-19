"""
shared_functions.py — Core Shared Utilities
Fusion Score Bot V7.0

V7 Fixes from V6 audit:
    - Thread-safe Supabase singleton (threading.Lock)
    - Error messages sanitized before Telegram send (no key leakage)
    - aggregate_and_push_sentiment uses targeted UPDATE not upsert
      (prevents COT/ATR columns being NULLed by sentiment writes)
    - Column ownership strictly documented and enforced
    - Fusion Score calculation unchanged (empirically validated)
"""

import re
import logging
import threading
from datetime import datetime, timezone, timedelta

import telebot
from supabase import create_client, Client

from config import (
    WEIGHT_ATR, WEIGHT_SENTIMENT, WEIGHT_COT,
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
    ERROR_BOT_TOKEN, ERROR_CHAT_ID,
    SUPABASE_URL, SUPABASE_KEY
)

logger = logging.getLogger(__name__)

# =============================================================================
# SECTION 1: SUPABASE CLIENT (Thread-Safe Singleton)
# V7 FIX: Added threading.Lock() — V6 had a classic double-checked locking
# failure. Flask daemon thread and engine main thread both call this during
# startup. Without a lock, two clients could be created simultaneously.
# =============================================================================

_supabase_lock   = threading.Lock()
_supabase_client: Client = None


def get_supabase_client() -> Client:
    """
    Returns a shared Supabase client (thread-safe singleton).
    Creates it on first call, reuses on all subsequent calls.

    Raises:
        Exception: If credentials are missing.
    """
    global _supabase_client
    with _supabase_lock:
        if _supabase_client is None:
            if not SUPABASE_URL or not SUPABASE_KEY:
                raise Exception(
                    "CRITICAL: SUPABASE_URL or SUPABASE_KEY not set. "
                    "Cannot initialize database client."
                )
            _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
            logger.info("[DB] Supabase client initialized (thread-safe singleton).")
    return _supabase_client


# =============================================================================
# SECTION 2: ERROR NOTIFICATIONS
# V7 FIX: Error messages are sanitized before sending to Telegram.
# API keys frequently appear in exception messages as URL query parameters.
# Sending them to Telegram in plaintext is a security vulnerability.
# =============================================================================

def _sanitize_error_message(msg: str) -> str:
    """
    Removes API keys and tokens from error messages before Telegram send.
    Matches URL query parameters and common key patterns.
    """
    if not msg:
        return "Unknown error"
    # Redact URL query parameters that look like keys
    msg = re.sub(
        r'(apikey|api_key|token|key|password|secret|auth)[=:][^\s&"\',]+',
        r'\1=***REDACTED***',
        msg,
        flags=re.IGNORECASE
    )
    # Truncate very long messages (avoid Telegram 4096 char limit issues)
    return msg[:800]


def send_error_notification(error_message: str, component: str = ""):
    """
    Sends a critical error alert via Telegram error bot.
    Uses dedicated error bot if configured, falls back to main bot.
    Never raises — error notifications must not crash the caller.

    Args:
        error_message: Human-readable error description.
        component:     Optional component name for context (e.g. "sentiment_scanner").
    """
    try:
        token   = ERROR_BOT_TOKEN if ERROR_BOT_TOKEN else TELEGRAM_TOKEN
        chat_id = ERROR_CHAT_ID   if ERROR_CHAT_ID   else TELEGRAM_CHAT_ID

        if not token or not chat_id:
            logger.error(f"[Alert] No Telegram credentials. Error lost: {error_message}")
            return

        safe_msg  = _sanitize_error_message(error_message)
        comp_tag  = f"[{component.upper()}] " if component else ""
        timestamp = datetime.now(timezone.utc).strftime('%H:%M UTC')

        text = (
            f"🚨 <b>FUSION BOT ERROR</b> 🚨\n\n"
            f"<b>{comp_tag}{timestamp}</b>\n"
            f"<code>{safe_msg}</code>"
        )

        bot = telebot.TeleBot(token)
        bot.send_message(chat_id, text, parse_mode="HTML", timeout=10)

    except Exception as e:
        # Last resort: just log. Never let this raise.
        logger.error(f"[Alert] Failed to send error notification: {e}")


# =============================================================================
# SECTION 3: FUSION SCORE CALCULATION
# Unchanged from V6 — empirically validated across 2 independent datasets.
# See config.py Section 7 for full empirical notes.
# =============================================================================

def calculate_fusion_score(
    sentiment: int,
    atr_multiplier: float,
    cot_bias: str,
    pair_direction: str
) -> int:
    """
    Master algorithm for trade viability scoring.

    Args:
        sentiment:       Integer -10 to +10. Positive = Bullish for the pair.
        atr_multiplier:  Current candle TR / 14-period ATR (Wilder's in V7).
        cot_bias:        STRONGLY_BULLISH | BULLISH | NEUTRAL | BEARISH | STRONGLY_BEARISH
        pair_direction:  "LONG" or "SHORT"

    Returns:
        Integer score clamped to 0–100.

    Score interpretation (empirically validated, 2 independent datasets):
        Score 86-100: 35.5% WR — worst tier (structural over-consensus)
        Score 56-70:  50.0% WR — mid tier
        Score 31-55:  63.0% WR — good tier
        Score ≤30:    68.6% WR — best tier (counter-trend setups)
    """
    score = 50  # Neutral baseline

    # --- Weight 1: Volatility (ATR Expansion) ---
    if atr_multiplier >= 1.5:
        score += WEIGHT_ATR

    # --- Weight 2: Macro Sentiment (Directional Alignment) ---
    if pair_direction == "LONG":
        if sentiment > 0:
            score += WEIGHT_SENTIMENT
        elif sentiment < 0:
            score -= WEIGHT_SENTIMENT
    else:  # SHORT
        if sentiment < 0:
            score += WEIGHT_SENTIMENT
        elif sentiment > 0:
            score -= WEIGHT_SENTIMENT

    # --- Weight 3: COT Bias (5-State) ---
    # Strong alignment: max boost. Strong opposition: max penalty.
    # Validated: BULLISH_FADING thesis — when institutions are reducing
    # longs but COT still reads BULLISH, it's actually a SHORT setup.
    if pair_direction == "LONG":
        if cot_bias == "STRONGLY_BULLISH":
            score += WEIGHT_COT + 5
        elif cot_bias == "BULLISH":
            score += WEIGHT_COT
        elif cot_bias == "BEARISH":
            score -= WEIGHT_COT
        elif cot_bias == "STRONGLY_BEARISH":
            score -= WEIGHT_COT + 5
        # NEUTRAL: no change
    else:  # SHORT
        if cot_bias == "STRONGLY_BEARISH":
            score += WEIGHT_COT + 5
        elif cot_bias == "BEARISH":
            score += WEIGHT_COT
        elif cot_bias == "BULLISH":
            score -= WEIGHT_COT
        elif cot_bias == "STRONGLY_BULLISH":
            score -= WEIGHT_COT + 5
        # NEUTRAL: no change

    return max(0, min(100, score))


# =============================================================================
# SECTION 4: SENTIMENT AGGREGATION
# V7 FIX: Uses targeted UPDATE instead of upsert to prevent column collision.
#
# V6 bug: aggregate_and_push_sentiment() upserted {pair, macro_sentiment,
# last_updated}. Supabase upsert on conflict can reset omitted columns to NULL.
# This means every sentiment run could NULL out cot_bias, cot_index, etc.
#
# V7 fix: Each component owns specific columns and ONLY updates those.
# Column ownership:
#   Sentiment aggregator: macro_sentiment, sentiment_updated_at
#   COT tracker:          cot_bias, cot_index, cot_net, cot_date, cot_updated_at
#   ATR engine:           last_alerted_candle_eurusd, last_alerted_candle_gbpusd,
#                         atr_updated_at
#   Regime detector:      market_regime, regime_updated_at
#   Grader:               long_wr_20, short_wr_20, wr_updated_at
# =============================================================================

def aggregate_and_push_sentiment(pair: str, lookback_hours: int = 6):
    """
    Reads recent processed_sentiment records for a pair, computes a net
    sentiment integer (-10 to +10), and writes ONLY sentiment columns
    to system_state.

    Scoring:
        HIGH importance   BULLISH → +2
        MEDIUM importance BULLISH → +1
        HIGH importance   BEARISH → -2
        MEDIUM importance BEARISH → -1
        NEUTRAL or LOW           →  0
        Final clamped to [-10, +10]

    Args:
        pair:           'EUR/USD' or 'GBP/USD'
        lookback_hours: How many hours of sentiment to aggregate
    """
    try:
        supabase = get_supabase_client()

        if pair == "EUR/USD":
            sentiment_col = "eur_usd_sentiment"
        elif pair == "GBP/USD":
            sentiment_col = "gbp_usd_sentiment"
        else:
            logger.error(f"[Aggregator] Unknown pair: {pair}")
            return

        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
        ).isoformat()

        response = (
            supabase.table("processed_sentiment")
            .select(f"{sentiment_col}, importance_tier")
            .gte("created_at", cutoff)
            .execute()
        )

        records = response.data or []

        if not records:
            logger.info(f"[Aggregator] No recent sentiment for {pair}. State unchanged.")
            return

        net = 0
        for record in records:
            sentiment = record.get(sentiment_col, "NEUTRAL")
            tier      = record.get("importance_tier", "LOW")
            weight    = 2 if tier == "HIGH" else (1 if tier == "MEDIUM" else 0)

            if sentiment == "BULLISH":
                net += weight
            elif sentiment == "BEARISH":
                net -= weight

        net = max(-10, min(10, net))

        logger.info(f"[Aggregator] {pair} net={net} from {len(records)} records")

        now_iso = datetime.now(timezone.utc).isoformat()

        # V7 FIX: UPDATE only owned columns. Never touch COT or ATR columns.
        result = (
            supabase.table("system_state")
            .update({
                "macro_sentiment":      net,
                "sentiment_updated_at": now_iso,
            })
            .eq("pair", pair)
            .execute()
        )

        # If no row exists yet (first run), insert it with all defaults
        if not result.data:
            supabase.table("system_state").insert({
                "pair":                 pair,
                "macro_sentiment":      net,
                "sentiment_updated_at": now_iso,
                "cot_bias":             "NEUTRAL",
                "cot_index":            0.5,
                "cot_net":              0,
                "market_regime":        "RANGING",
            }).execute()
            logger.info(f"[Aggregator] Inserted new system_state row for {pair}.")
        else:
            logger.info(f"[Aggregator] system_state updated: {pair} macro_sentiment={net}")

    except Exception as e:
        logger.error(f"[Aggregator] Failed for {pair}: {e}")
        send_error_notification(f"Sentiment Aggregation Failed ({pair}): {e}", "aggregator")


# =============================================================================
# SECTION 5: STALE DATA DETECTION UTILITIES
# Shared by volatility_atr.py and system_health_check.py
# =============================================================================

def is_data_stale(iso_timestamp: str, max_age_hours: float) -> bool:
    """
    Returns True if the given ISO timestamp is older than max_age_hours.
    Returns True (treat as stale) if timestamp is None or unparseable.
    """
    if not iso_timestamp:
        return True
    try:
        updated = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - updated).total_seconds() / 3600
        return age_hours > max_age_hours
    except (ValueError, TypeError):
        return True


def format_age_string(iso_timestamp: str) -> str:
    """
    Returns a human-readable age string like '2h 15m ago' or 'Unknown'.
    Used in health check and /status command output.
    """
    if not iso_timestamp:
        return "Unknown"
    try:
        updated = datetime.fromisoformat(iso_timestamp.replace('Z', '+00:00'))
        if updated.tzinfo is None:
            updated = updated.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - updated
        total_minutes = int(delta.total_seconds() / 60)
        hours, minutes = divmod(total_minutes, 60)
        if hours > 0:
            return f"{hours}h {minutes}m ago"
        return f"{minutes}m ago"
    except (ValueError, TypeError):
        return "Unknown"
