"""
strategies/__init__.py — Strategy Plugin Loader
Fusion Score Bot V7.0

Dynamically loads the active strategy based on ACTIVE_STRATEGY env var.
Usage:
    from strategies import get_active_strategy
    strategy = get_active_strategy()
    if strategy.entry_signal(candle, atr_mult, context):
        score = strategy.score_signal(...)
"""

import importlib
import logging
from strategies.base import BaseStrategy

logger = logging.getLogger(__name__)

_strategy_cache = None


def get_active_strategy() -> BaseStrategy:
    """
    Returns the active strategy instance (cached singleton).
    Reads ACTIVE_STRATEGY from config (which reads from env var).

    Raises:
        ImportError:   If the strategy module cannot be found.
        AttributeError: If the module doesn't have the expected class.
    """
    global _strategy_cache

    if _strategy_cache is not None:
        return _strategy_cache

    from config import ACTIVE_STRATEGY
    strategy_name = ACTIVE_STRATEGY.lower().strip()

    try:
        module = importlib.import_module(f"strategies.{strategy_name}")

        # Find the strategy class (subclass of BaseStrategy, not BaseStrategy itself)
        strategy_class = None
        for attr_name in dir(module):
            obj = getattr(module, attr_name)
            if (
                isinstance(obj, type)
                and issubclass(obj, BaseStrategy)
                and obj is not BaseStrategy
            ):
                strategy_class = obj
                break

        if strategy_class is None:
            raise AttributeError(
                f"No BaseStrategy subclass found in strategies/{strategy_name}.py"
            )

        _strategy_cache = strategy_class()
        logger.info(
            f"[Strategy] Loaded: {_strategy_cache.name} v{_strategy_cache.version}"
        )
        return _strategy_cache

    except ModuleNotFoundError:
        logger.error(
            f"[Strategy] Strategy module 'strategies/{strategy_name}.py' not found. "
            f"Available: momentum. Falling back to momentum."
        )
        from strategies.momentum import MomentumStrategy
        _strategy_cache = MomentumStrategy()
        return _strategy_cache

    except Exception as e:
        logger.error(f"[Strategy] Failed to load strategy '{strategy_name}': {e}")
        from strategies.momentum import MomentumStrategy
        _strategy_cache = MomentumStrategy()
        return _strategy_cache


def list_available_strategies() -> list:
    """
    Returns names of all available strategy modules in the strategies/ folder.
    Used by /status command and health check.
    """
    import os
    strategy_dir = os.path.dirname(__file__)
    strategies = []
    for filename in os.listdir(strategy_dir):
        if filename.endswith('.py') and filename not in ('__init__.py', 'base.py'):
            strategies.append(filename[:-3])  # Strip .py
    return sorted(strategies)
