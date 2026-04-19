"""
strategies/momentum.py — Momentum Strategy Plugin
Fusion Score Bot V7.0

This is the V6 logic extracted and encapsulated as a strategy plugin.
Momentum strategy: trade in the direction of ATR expansion + COT alignment.

Core thesis (empirically validated, 130 trades):
    - ATR expansion ≥ 1.5× signals a breakout attempt
    - COT alignment (institutional smart money confirmation) improves WR
    - Score INVERSION is structural: score ≤30 (counter-trend) outperforms
      score ≥86 (over-consensus) — this strategy respects that inversion

Best edges from historical analysis:
    LONG + COT Aligned + ATR <2.5×  → ~74.5% WR
    ATR ≥3.0× (extreme expansion)   → ~81.8% WR (any direction)
    Score ≤30 counter-trend setups  → ~68.6% WR

Worst edges:
    ATR 2.5-3.0× zone               → negative expectancy
    Hours 15:00 and 21:00 IST       → losing windows
    Score 86-100                    → ~35.5% WR
"""

from typing import Optional
from strategies.base import BaseStrategy
from shared_functions import calculate_fusion_score
from config import (
    WEIGHT_ATR, WEIGHT_SENTIMENT, WEIGHT_COT,
    ATR_THRESHOLD, EXTREME_ATR_THRESHOLD,
    PREMIUM_SCORE_THRESHOLD,
    REGIME_HIGH_VOL_MULTIPLIER
)


class MomentumStrategy(BaseStrategy):

    @property
    def name(self) -> str:
        return "momentum"

    @property
    def version(self) -> str:
        return "1.0"

    def entry_signal(
        self,
        candle: dict,
        atr_multiplier: float,
        context: dict
    ) -> bool:
        """
        Momentum entry conditions:
            1. ATR expansion >= 1.5× (volatility breakout)
            2. Not a doji (close != open — no directionless candle)
            3. Not in a losing hour window (15:00, 21:00 IST empirically bad)
            4. Not in the ATR dead zone (2.5-3.0×) — negative expectancy
            5. Daily range not > 90% consumed

        Note: ATR ≥3.0× (extreme tier) bypasses the dead zone filter.
        """
        # Condition 1: ATR expansion
        if atr_multiplier < ATR_THRESHOLD:
            return False

        # Condition 2: No doji
        try:
            if float(candle.get('close', 0)) == float(candle.get('open', 0)):
                return False
        except (ValueError, TypeError):
            return False

        # Condition 3: Skip dead zone (2.5-3.0×) — unless it's extreme (≥3.0×)
        if ATR_THRESHOLD <= atr_multiplier < EXTREME_ATR_THRESHOLD:
            if 2.5 <= atr_multiplier < 3.0:
                return False  # Dead zone — negative expectancy

        # Condition 4: Daily range saturation
        saturation = context.get('daily_saturation', 0.0)
        if saturation > 0.90:
            return False  # Move already exhausted

        # Condition 5: Losing hour windows (IST 15:00 and 21:00 = UTC 09:30 and 15:30)
        # These are empirically confirmed losing windows from the 130-trade dataset
        hour_ist = context.get('hour_ist')
        if hour_ist in (15, 21):
            return False

        return True

    def score_signal(
        self,
        sentiment: int,
        atr_multiplier: float,
        cot_bias: str,
        pair_direction: str,
        regime: str,
        context: Optional[dict] = None
    ) -> int:
        """
        Momentum scoring = base Fusion Score + regime adjustment.

        Base score from calculate_fusion_score() (V6-validated formula).
        Regime adjustment:
            TRENDING:  COT alignment gets +10 bonus (trend-following works)
            RANGING:   COT alignment gets -8 penalty (counter-trend works better)
                       SHORT in range without ATR ≥2.5× gets additional -15
            HIGH_VOL:  No score adjustment (advisory event — fire as-is)
        """
        base_score = calculate_fusion_score(
            sentiment, atr_multiplier, cot_bias, pair_direction
        )

        # Regime adjustment
        adjustment = 0
        if regime == "TRENDING":
            if pair_direction == "LONG" and cot_bias in ("BULLISH", "STRONGLY_BULLISH"):
                adjustment += 10
            elif pair_direction == "SHORT" and cot_bias in ("BEARISH", "STRONGLY_BEARISH"):
                adjustment += 10

        elif regime == "RANGING":
            if pair_direction == "LONG" and cot_bias in ("BULLISH", "STRONGLY_BULLISH"):
                adjustment -= 8
            elif pair_direction == "SHORT" and cot_bias in ("BEARISH", "STRONGLY_BEARISH"):
                if atr_multiplier < 2.5:
                    adjustment -= 15

        # HIGH_VOL_SHOCK: no adjustment, engine flags it separately

        # Downgrade if daily range highly consumed
        if context:
            saturation = context.get('daily_saturation', 0.0)
            if saturation > 0.75:
                adjustment -= 10

        return max(0, min(100, base_score + adjustment))

    def exit_signal(
        self,
        entry_candle: dict,
        current_candle: dict,
        entry_score: int,
        context: Optional[dict] = None
    ) -> bool:
        """
        Momentum exit: fixed 1-hour hold.
        The performance_grader uses this same HOLD_HOURS = 1 assumption.

        Future enhancements: ATR-based trailing stop, opposite signal exit.
        """
        try:
            from datetime import datetime
            entry_time   = datetime.fromisoformat(entry_candle.get('datetime', ''))
            current_time = datetime.fromisoformat(current_candle.get('datetime', ''))
            elapsed_minutes = (current_time - entry_time).total_seconds() / 60
            return elapsed_minutes >= 60
        except (ValueError, TypeError):
            return False

    def get_filters(self) -> dict:
        """
        Momentum strategy filter configuration.
        """
        return {
            'atr_min':              ATR_THRESHOLD,       # 1.5×
            'atr_max':              None,                # No upper limit (extreme tier handled separately)
            'extreme_atr':          EXTREME_ATR_THRESHOLD,  # 3.0×
            'atr_dead_zone_low':    2.5,                 # Dead zone start (negative expectancy)
            'atr_dead_zone_high':   3.0,                 # Dead zone end
            'daily_saturation_max': 0.90,                # Stop signals if 90%+ of daily range consumed
            'bad_hours_ist':        [15, 21],            # Losing windows from 130-trade analysis
            'regime_allowed':       None,                # All regimes allowed (regime affects score, not entry)
            'regime_blocked':       [],
            'hold_hours':           1,                   # Performance grader assumption
        }

    def describe(self) -> str:
        return (
            "Momentum Strategy v1.0: Trades in the direction of ATR expansion "
            "(breakout entries). COT institutional alignment improves score. "
            "Regime-aware scoring: TRENDING rewards COT alignment, RANGING penalizes it. "
            "ATR dead zone (2.5-3.0×) blocked — negative expectancy confirmed empirically. "
            "Extreme expansion (≥3.0×) triggers separate high-alpha alert format. "
            "Score inversion respected: lower scores (counter-trend) historically outperform."
        )
