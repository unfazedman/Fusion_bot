"""
config.py — Central Configuration & Validation
Fusion Score Bot V7.0

V7 Fixes from V6 audit:
    - ENV vars read inside validate_config() directly (not cached at import)
    - Secrets never stored in module-level dicts (prevents introspection leak)
    - Component registry uses string keys only (no live values stored)
    - New V7 parameters: strategy plugin, dual channels, extreme ATR tier,
      regime detection, Gemini daily budget tracking
"""

import os

# =============================================================================
# SECTION 1: TELEGRAM
# =============================================================================
TELEGRAM_TOKEN   = os.environ.get('TELEGRAM_TOKEN')    # Main signal bot
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')  # Operator/private channel

# Dedicated error bot (operator only — never subscriber-facing)
ERROR_BOT_TOKEN = os.environ.get('ERROR_BOT_TOKEN')
ERROR_CHAT_ID   = os.environ.get('ERROR_CHAT_ID')

# SaaS channels (V7 new)
FREE_CHANNEL_ID     = os.environ.get('FREE_CHANNEL_ID')     # Public free subscribers
PREMIUM_CHANNEL_ID  = os.environ.get('PREMIUM_CHANNEL_ID')  # Paid subscribers

# =============================================================================
# SECTION 2: TRADING DATA
# =============================================================================
TWELVE_DATA_KEY = os.environ.get('TWELVE_DATA_KEY')

# =============================================================================
# SECTION 3: AI & SENTIMENT
# =============================================================================
GEMINI_API_KEY      = os.environ.get('GEMINI_API_KEY')
HUGGINGFACE_API_KEY = os.environ.get('HUGGINGFACE_API_KEY')

# Gemini 2.5 Flash — VERIFIED limits from AI Studio, April 2026
# Public docs claim 250 RPD. Reality: 20 RPD, 5 RPM.
GEMINI_RPM_LIMIT        = 5     # Requests per minute
GEMINI_RPD_LIMIT        = 20    # Requests per day (hard ceiling)
GEMINI_RPD_SAFE_LIMIT   = 18    # Alert threshold (2-call buffer before hard limit)
GEMINI_THROTTLE_DELAY   = 13    # Seconds between calls (60/5 + 1s buffer)
GEMINI_CALLS_PER_CYCLE  = 2     # 1 highest-importance article × 2 pairs per run

# =============================================================================
# SECTION 4: NEWS APIS
# =============================================================================
GNEWS_API_KEY  = os.environ.get('GNEWS_API_KEY')
NEWS_API_KEY   = os.environ.get('NEWS_API_KEY')   # Optional second source

# GNews rotation — 1 keyword per run to stay under 100/day free limit
# 3 keywords × 32 runs/day = 96 calls (safe under 100 limit)
GNEWS_KEYWORDS = ["inflation Fed", "ECB rate", "GDP employment"]

# RSS feeds — zero API cost, no rate limits
# Used for both sentiment collection and Geopolitical Dragnet
RSS_FEEDS_SENTIMENT = [
    "https://www.forexlive.com/feed/news",
    "https://feeds.reuters.com/reuters/businessNews",
]
RSS_FEEDS_DRAGNET = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://feeds.ap.org/ap/businessnews",
]

# =============================================================================
# SECTION 5: DATABASE
# =============================================================================
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# =============================================================================
# SECTION 6: TRADING PARAMETERS
# =============================================================================
PAIRS = ['EUR/USD', 'GBP/USD']

# ATR thresholds — two tiers based on 130-trade empirical analysis
# Standard: 1.5× → base signal trigger
# Extreme:  3.0× → 81.8% WR, +220.6 pips (11 trades) — rare high-alpha tier
ATR_THRESHOLD         = 1.5   # Standard signal threshold
EXTREME_ATR_THRESHOLD = 3.0   # Extreme expansion (separate alert format)

# Market hours kill time — Friday 21:00 UTC
# Moved from 22:00 → 21:00 to prevent 1-hour holds bleeding into weekend gap
FRIDAY_KILL_HOUR_UTC = 21

# Scan delay after news release (minutes)
# Gives headlines time to hit wires before collection
SCAN_DELAY_MINUTES = 3

# =============================================================================
# SECTION 7: FUSION SCORE WEIGHTS
# Base score 50 + ATR(20) + Sentiment(25) + COT(15) = max 110, clamped to 100.
#
# Empirical finding (confirmed across 2 independent datasets):
#   Score 86-100: 35.5% WR (-194 pips) — WORST tier
#   Score 56-70:  50.0% WR             — Mid tier
#   Score 31-55:  63.0% WR             — Good tier
#   Score ≤30:    68.6% WR             — Best tier (low-score counter-trend)
#
# Score inversion is structural — do NOT present high scores as "best signals."
# =============================================================================
WEIGHT_ATR       = 20
WEIGHT_SENTIMENT = 25
WEIGHT_COT       = 15

# SaaS routing thresholds
PREMIUM_SCORE_THRESHOLD = 65   # Score >= 65 → premium channel
MIN_BROADCAST_SCORE     = 0    # Score < this → log only, no broadcast (0 = send all)

# =============================================================================
# SECTION 8: SENTIMENT PIPELINE PARAMETERS
# =============================================================================
SIMILARITY_THRESHOLD    = 0.85   # Fuzzy dedup threshold
MAX_ITEMS_PER_CYCLE     = 100    # Max articles collected per run
IMPORTANCE_DECAY_HOURS  = 6      # Reduce importance after this many hours
IMPORTANCE_CUTOFF_HOURS = 24     # Ignore articles older than this

# =============================================================================
# SECTION 9: COT TRACKER PARAMETERS
# Full 5-state momentum classification
# =============================================================================
COT_LOOKBACK_WEEKS   = 52    # 52-week window for index normalization
COT_NEUTRAL_BAND     = 0.40  # Index 0.40-0.60 = NEUTRAL
COT_STRONG_THRESHOLD = 0.75  # Index >= 0.75 = STRONGLY_BULLISH, <= 0.25 = STRONGLY_BEARISH

# =============================================================================
# SECTION 10: STRATEGY PLUGIN SYSTEM (V7 NEW)
# =============================================================================
# Active strategy module name. Must match a file in strategies/
# Available: "momentum" (V6 logic), more to be added in future versions
ACTIVE_STRATEGY = os.environ.get('ACTIVE_STRATEGY', 'momentum')

# =============================================================================
# SECTION 11: REGIME DETECTOR PARAMETERS (V7 NEW)
# =============================================================================
REGIME_DIRECTIONAL_THRESHOLD = 0.65   # ADX proxy: fraction of moves in same direction
REGIME_CANDLE_LOOKBACK       = 20     # Candles to analyze for regime classification
REGIME_HIGH_VOL_MULTIPLIER   = 2.5    # ATR × this = HIGH_VOL_SHOCK regime

# =============================================================================
# SECTION 12: GEOPOLITICAL DRAGNET PARAMETERS (V7 NEW)
# =============================================================================
DRAGNET_POLL_SECONDS    = 30    # RSS poll interval
DRAGNET_DEDUP_TTL_HOURS = 4     # Time before a seen headline can re-trigger
DRAGNET_EMERGENCY_KEYWORDS = {
    'attack', 'ceasefire', 'intervention', 'invasion', 'default',
    'sanction', 'crash', 'emergency', 'collapse', 'explosion',
    'nuclear', 'strike', 'conflict', 'seizure', 'blockade',
    'assassination', 'coup', 'war', 'missile', 'bomb'
}

# =============================================================================
# SECTION 13: VALIDATION
# V7 FIX: Values read from os.environ directly inside validate_config()
# so they are never cached at import time and never stored in module dicts.
# =============================================================================

# Registry: component name → list of required env var NAMES (not values)
_ALWAYS_REQUIRED_KEYS = [
    'TELEGRAM_TOKEN',
    'TELEGRAM_CHAT_ID',
    'SUPABASE_URL',
    'SUPABASE_KEY',
]

_COMPONENT_REQUIRED_KEYS = {
    'volatility_atr':     ['TWELVE_DATA_KEY'],
    'sentiment_scanner':  ['GEMINI_API_KEY', 'GNEWS_API_KEY'],
    'cot_tracker':        [],
    'performance_grader': [],
    'bot':                [],
    'system_health_check': [],
}


def validate_config(component: str = None) -> bool:
    """
    Validates environment variables are present.
    Reads from os.environ directly — never from cached module-level values.

    Args:
        component: Optional component name to also check component-specific vars.

    Returns:
        True if all required vars are set.

    Raises:
        EnvironmentError: If any required variable is missing.
    """
    keys_to_check = list(_ALWAYS_REQUIRED_KEYS)

    if component and component in _COMPONENT_REQUIRED_KEYS:
        keys_to_check.extend(_COMPONENT_REQUIRED_KEYS[component])

    missing = [key for key in keys_to_check if not os.environ.get(key)]

    if missing:
        raise EnvironmentError(
            f"CRITICAL: Missing required environment variables: {', '.join(missing)}"
        )

    return True


def get_all_broadcast_channels() -> list:
    """
    Returns all configured Telegram channel IDs for broadcast signals.
    Used by the Geopolitical Dragnet and emergency alerts.
    """
    channels = []
    for env_key in ['TELEGRAM_CHAT_ID', 'FREE_CHANNEL_ID', 'PREMIUM_CHANNEL_ID']:
        val = os.environ.get(env_key)
        if val:
            channels.append(val)
    # Deduplicate while preserving order
    seen = set()
    return [c for c in channels if not (c in seen or seen.add(c))]
