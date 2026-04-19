"""
regime_detector.py — Market Regime Classification
Fusion Score Bot V7.0

Detects current market regime from price action using already-fetched candles.
Zero additional API cost — reuses data from volatility_atr.py's TwelveData call.

Three regimes:
    TRENDING:        ADX proxy > 0.65 (directional bias confirmed)
    RANGING:         ADX proxy <= 0.65 (mean-reversion environment)
    HIGH_VOL_SHOCK:  ATR multiplier >= 2.5 (black swan / unscheduled shock)

How regime affects signals (see strategies/momentum.py for scoring impact):
    TRENDING:     Trend-following bias confirmed. COT alignment is rewarded.
    RANGING:      Counter-trend setups historically better. COT alignment penalized.
    HIGH_VOL:     Advisory regime — signal still fires, alert format changes.
"""

import logging
from typing import Optional
from config import (
    REGIME_DIRECTIONAL_THRESHOLD,
    REGIME_CANDLE_LOOKBACK,
    REGIME_HIGH_VOL_MULTIPLIER
)

logger = logging.getLogger(__name__)


def classify_regime(candles: list, current_multiplier: float) -> str:
    """
    Classifies the current market regime from recent price action.

    Args:
        candles:            List of OHLCV candle dicts (newest first, TwelveData format).
                            Needs at least REGIME_CANDLE_LOOKBACK (20) candles.
                            candles[0] is the live/current candle — skipped.
                            candles[1..20] are the analysis window.
        current_multiplier: Current candle's TR/ATR ratio (from volatility engine).

    Returns:
        'TRENDING' | 'RANGING' | 'HIGH_VOL_SHOCK'
    """
    try:
        # HIGH_VOL_SHOCK: current candle is >= 2.5× ATR (unscheduled shock event)
        if current_multiplier >= REGIME_HIGH_VOL_MULTIPLIER:
            logger.info(f"[Regime] HIGH_VOL_SHOCK (multiplier={current_multiplier:.2f})")
            return "HIGH_VOL_SHOCK"

        # Use candles[1..21] — skip live candle[0]
        analysis_candles = candles[1:REGIME_CANDLE_LOOKBACK + 1]

        if len(analysis_candles) < 10:
            logger.warning(f"[Regime] Insufficient candles ({len(analysis_candles)}). Defaulting RANGING.")
            return "RANGING"

        closes = []
        for c in analysis_candles:
            try:
                closes.append(float(c['close']))
            except (ValueError, KeyError, TypeError):
                continue

        if len(closes) < 5:
            return "RANGING"

        # Directional consistency metric (ADX proxy)
        # Counts consecutive closes in the same direction
        # > REGIME_DIRECTIONAL_THRESHOLD (0.65) = persistent directional movement = TRENDING
        moves = [(closes[i] - closes[i - 1]) for i in range(1, len(closes))]
        if not moves:
            return "RANGING"

        up_count    = sum(1 for m in moves if m > 0)
        total_moves = len(moves)
        directional_ratio = max(up_count, total_moves - up_count) / total_moves

        if directional_ratio >= REGIME_DIRECTIONAL_THRESHOLD:
            logger.info(f"[Regime] TRENDING (directional_ratio={directional_ratio:.2f})")
            return "TRENDING"

        logger.info(f"[Regime] RANGING (directional_ratio={directional_ratio:.2f})")
        return "RANGING"

    except Exception as e:
        logger.error(f"[Regime] Classification error: {e}. Defaulting RANGING.")
        return "RANGING"  # Conservative fallback — never crash the engine


def get_regime_emoji(regime: str) -> str:
    """Returns a Telegram-safe emoji for a regime string."""
    return {
        "TRENDING":       "📈",
        "RANGING":        "↔️",
        "HIGH_VOL_SHOCK": "⚡",
    }.get(regime, "❓")


def get_regime_label(regime: str) -> str:
    """Returns a human-readable regime label for Telegram messages."""
    return {
        "TRENDING":       "Trending",
        "RANGING":        "Ranging",
        "HIGH_VOL_SHOCK": "HIGH VOL SHOCK",
    }.get(regime, regime)


def update_regime_in_db(supabase, pair: str, regime: str):
    """
    Writes the detected regime to system_state.
    Owns only: market_regime, regime_updated_at

    Args:
        supabase: Supabase client instance
        pair:     'EUR/USD' or 'GBP/USD'
        regime:   Regime string from classify_regime()
    """
    try:
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()

        result = (
            supabase.table("system_state")
            .update({
                "market_regime":    regime,
                "regime_updated_at": now_iso,
            })
            .eq("pair", pair)
            .execute()
        )

        if result.data:
            logger.info(f"[Regime] DB updated: {pair} → {regime}")
        else:
            logger.warning(f"[Regime] system_state row not found for {pair}. Row may not exist yet.")

    except Exception as e:
        logger.error(f"[Regime] DB update failed for {pair}: {e}")
