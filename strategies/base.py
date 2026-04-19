"""
strategies/base.py — Abstract Strategy Interface
Fusion Score Bot V7.0

Architecture: Option B plugin layer with Option C interface shape.
    - base.py defines the contract every strategy must implement
    - Each strategy file (momentum.py, mean_reversion.py, etc.) subclasses BaseStrategy
    - The engine imports the active strategy via get_active_strategy() in __init__.py
    - Switching strategy = change ACTIVE_STRATEGY env var, zero engine code change

Interface methods:
    entry_signal(candle, atr, context) → bool        Should this candle trigger analysis?
    score_signal(sentiment, atr_mult, cot, direction) → int   Score the signal
    exit_signal(entry_candle, current_candle) → bool  Should position be closed?
    get_filters() → dict                              Strategy-specific filter thresholds
    describe() → str                                  Human-readable strategy description
"""

from abc import ABC, abstractmethod
from typing import Optional


class BaseStrategy(ABC):
    """
    Abstract base class for all Fusion Score Bot trading strategies.

    Every strategy must implement all abstract methods.
    Strategies are stateless — all context is passed as arguments.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Short strategy identifier. Must match the module filename."""
        pass

    @property
    @abstractmethod
    def version(self) -> str:
        """Strategy version string (e.g. '1.0')."""
        pass

    @abstractmethod
    def entry_signal(
        self,
        candle: dict,
        atr_multiplier: float,
        context: dict
    ) -> bool:
        """
        Determines whether a candle qualifies as a potential entry signal.
        This is the FIRST filter — called before scoring.

        Args:
            candle:         OHLCV candle dict with keys: open, high, low, close, datetime
            atr_multiplier: Current candle's TR / 14-period ATR (Wilder's)
            context:        Additional market context:
                            {
                                'regime':           str,  # TRENDING|RANGING|HIGH_VOL_SHOCK
                                'macro_sentiment':  int,  # -10 to +10
                                'cot_bias':         str,  # 5-state
                                'daily_saturation': float, # 0.0-1.0 (% of daily range consumed)
                                'hour_utc':         int,
                                'weekday':          int,  # 0=Mon, 4=Fri
                            }

        Returns:
            True if the signal qualifies for scoring. False to skip.
        """
        pass

    @abstractmethod
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
        Scores a qualified signal. Called only if entry_signal() returns True.

        Args:
            sentiment:      Macro sentiment integer -10 to +10
            atr_multiplier: Current candle's TR/ATR ratio
            cot_bias:       5-state COT classification string
            pair_direction: "LONG" or "SHORT"
            regime:         Market regime from regime_detector
            context:        Optional additional context

        Returns:
            Integer score 0–100.
        """
        pass

    @abstractmethod
    def exit_signal(
        self,
        entry_candle: dict,
        current_candle: dict,
        entry_score: int,
        context: Optional[dict] = None
    ) -> bool:
        """
        Determines whether an open position should be closed.
        NOTE: V7 initial implementation — performance_grader uses 1H fixed hold.
        This method is provided for future live exit logic.

        Args:
            entry_candle:   The candle at trade entry
            current_candle: The candle being evaluated for exit
            entry_score:    The Fusion Score at entry
            context:        Optional market context

        Returns:
            True if position should be closed.
        """
        pass

    @abstractmethod
    def get_filters(self) -> dict:
        """
        Returns strategy-specific filter thresholds used by the engine.

        Expected keys (all optional, engine uses defaults if absent):
            atr_min:             float  Minimum ATR multiplier for entry
            atr_max:             float  Maximum ATR multiplier (None = no max)
            extreme_atr:         float  Threshold for extreme expansion tier
            daily_saturation_max: float Maximum daily range saturation (0.0-1.0)
            bad_hours_utc:       list   Hours to skip (e.g. [14, 20] for thin markets)
            regime_allowed:      list   Allowed regimes (None = all)
            regime_blocked:      list   Blocked regimes

        Returns:
            dict of filter parameters
        """
        pass

    @abstractmethod
    def describe(self) -> str:
        """
        Returns a one-paragraph human-readable description of the strategy.
        Shown in /status Telegram command and health check output.
        """
        pass

    def classify_signal_tier(self, score: int) -> str:
        """
        Maps a numeric score to a human-readable tier label.
        Based on empirical win rate analysis (2 independent datasets).

        This is defined on BaseStrategy so all strategies benefit from it,
        but each strategy can override if their scoring range differs.
        """
        if score >= 86:
            return "⚠️ EXTREME CONSENSUS (historically weak)"
        elif score >= 71:
            return "HIGH"
        elif score >= 56:
            return "MODERATE"
        elif score >= 31:
            return "COUNTER-TREND"
        else:
            return "🎯 STRONG COUNTER-TREND (historically strongest)"

    def is_premium_signal(self, score: int, atr_multiplier: float) -> bool:
        """
        Default routing logic: premium if score >= 65 OR extreme ATR expansion.
        Strategies can override this for custom routing.
        """
        from config import PREMIUM_SCORE_THRESHOLD, EXTREME_ATR_THRESHOLD
        return score >= PREMIUM_SCORE_THRESHOLD or atr_multiplier >= EXTREME_ATR_THRESHOLD
