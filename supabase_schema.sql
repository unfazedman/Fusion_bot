-- =============================================================================
-- Fusion Score Bot V7.0 — Supabase Schema
-- =============================================================================
-- V6 CRITICAL BUG: supabase_schema.sql contained Python code, not SQL.
-- This is the actual schema. Run in Supabase SQL Editor to initialise V7 DB.
--
-- Column ownership (enforced in application code):
--   system_state.macro_sentiment, sentiment_updated_at  → shared_functions.py (aggregator)
--   system_state.cot_*                                  → cot_tracker.py
--   system_state.last_alerted_candle_*,  atr_updated_at → volatility_atr.py
--   system_state.market_regime, regime_updated_at       → regime_detector.py
--   system_state.long_wr_20, short_wr_20, wr_updated_at → performance_grader.py
-- =============================================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- =============================================================================
-- TABLE: system_state
-- One row per trading pair. All components write to their own columns only.
-- =============================================================================
CREATE TABLE IF NOT EXISTS system_state (
    id                          UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    pair                        VARCHAR(10)  NOT NULL UNIQUE,   -- 'EUR/USD' or 'GBP/USD'

    -- Owned by: shared_functions.py (sentiment aggregator)
    macro_sentiment             INTEGER      DEFAULT 0,          -- -10 to +10
    sentiment_updated_at        TIMESTAMPTZ,

    -- Owned by: cot_tracker.py
    cot_bias                    VARCHAR(30)  DEFAULT 'NEUTRAL',  -- 5-state + FADING/ACCELERATING
    cot_index                   NUMERIC(6,4) DEFAULT 0.5,        -- 0.0 to 1.0
    cot_net                     INTEGER      DEFAULT 0,          -- Non-commercial net positions
    cot_date                    DATE,                            -- CFTC report date
    cot_updated_at              TIMESTAMPTZ,

    -- Owned by: volatility_atr.py (dedup markers — one column per pair for clean indexing)
    last_alerted_candle_eurusd  VARCHAR(30),                     -- datetime string of last alerted candle
    last_alerted_candle_gbpusd  VARCHAR(30),
    atr_updated_at              TIMESTAMPTZ,

    -- Owned by: regime_detector.py
    market_regime               VARCHAR(20)  DEFAULT 'RANGING',  -- TRENDING | RANGING | HIGH_VOL_SHOCK
    regime_updated_at           TIMESTAMPTZ,

    -- Owned by: performance_grader.py
    long_wr_20                  NUMERIC(5,1),                    -- Rolling 20-trade LONG win rate %
    short_wr_20                 NUMERIC(5,1),                    -- Rolling 20-trade SHORT win rate %
    wr_updated_at               TIMESTAMPTZ,

    created_at                  TIMESTAMPTZ  DEFAULT now(),
    updated_at                  TIMESTAMPTZ  DEFAULT now()
);

-- Seed initial rows for both pairs
INSERT INTO system_state (pair, cot_bias, macro_sentiment, market_regime)
VALUES
    ('EUR/USD', 'NEUTRAL', 0, 'RANGING'),
    ('GBP/USD', 'NEUTRAL', 0, 'RANGING')
ON CONFLICT (pair) DO NOTHING;

-- Auto-update updated_at
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER system_state_updated_at
    BEFORE UPDATE ON system_state
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();


-- =============================================================================
-- TABLE: trade_logs
-- One row per generated signal. Grader fills result/exit_price/pips.
-- =============================================================================
CREATE TABLE IF NOT EXISTS trade_logs (
    id                    UUID         DEFAULT uuid_generate_v4() PRIMARY KEY,
    pair                  VARCHAR(10)  NOT NULL,
    direction             VARCHAR(5)   NOT NULL CHECK (direction IN ('LONG', 'SHORT')),
    confidence_score      INTEGER      NOT NULL CHECK (confidence_score BETWEEN 0 AND 100),
    volatility_multiplier NUMERIC(8,4) NOT NULL,
    macro_sentiment       INTEGER,
    cot_bias              VARCHAR(30),
    market_regime         VARCHAR(20),
    is_extreme_atr        BOOLEAN      DEFAULT FALSE,
    is_premium_signal     BOOLEAN      DEFAULT FALSE,
    entry_price           NUMERIC(10,5) NOT NULL,
    candle_time           VARCHAR(30),                -- TwelveData datetime string
    timestamp_utc         TIMESTAMPTZ  DEFAULT now(),
    timestamp_ist         TIMESTAMPTZ,
    strategy              VARCHAR(50)  DEFAULT 'momentum',

    -- Filled by performance_grader.py
    result                VARCHAR(12)  CHECK (result IN ('WIN', 'LOSS', 'BREAKEVEN')),
    exit_price            NUMERIC(10,5),
    pips                  NUMERIC(8,1),
    exit_time             VARCHAR(30),
    graded_at             TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_trade_logs_pair       ON trade_logs(pair);
CREATE INDEX IF NOT EXISTS idx_trade_logs_timestamp  ON trade_logs(timestamp_utc DESC);
CREATE INDEX IF NOT EXISTS idx_trade_logs_result     ON trade_logs(result);
CREATE INDEX IF NOT EXISTS idx_trade_logs_ungraded   ON trade_logs(result) WHERE result IS NULL;


-- =============================================================================
-- TABLE: raw_sentiment_data
-- Raw articles before cleaning/dedup/relevance filtering.
-- Append-only. Never updated.
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw_sentiment_data (
    id           UUID         DEFAULT uuid_generate_v4() PRIMARY KEY,
    title        TEXT,
    description  TEXT,
    source       VARCHAR(100),
    published_at TIMESTAMPTZ,
    url          TEXT,
    created_at   TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_sentiment_created ON raw_sentiment_data(created_at DESC);


-- =============================================================================
-- TABLE: processed_sentiment
-- Cleaned, deduped, relevance-filtered articles with AI sentiment scores.
-- =============================================================================
CREATE TABLE IF NOT EXISTS processed_sentiment (
    id                  UUID         DEFAULT uuid_generate_v4() PRIMARY KEY,
    title               TEXT,
    text_cleaned        TEXT,
    content_hash        VARCHAR(32)  UNIQUE NOT NULL,   -- MD5 for dedup
    source              VARCHAR(100),
    published_at        TIMESTAMPTZ,
    importance_tier     VARCHAR(10)  NOT NULL CHECK (importance_tier IN ('HIGH', 'MEDIUM', 'LOW')),
    eur_usd_sentiment   VARCHAR(10)  CHECK (eur_usd_sentiment IN ('BULLISH', 'BEARISH', 'NEUTRAL')),
    eur_usd_confidence  NUMERIC(4,3),                   -- 0.000 to 1.000
    gbp_usd_sentiment   VARCHAR(10)  CHECK (gbp_usd_sentiment IN ('BULLISH', 'BEARISH', 'NEUTRAL')),
    gbp_usd_confidence  NUMERIC(4,3),
    model_used          VARCHAR(20),                    -- 'Gemini' or 'HuggingFace'
    created_at          TIMESTAMPTZ  DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_processed_sentiment_created  ON processed_sentiment(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_processed_sentiment_hash     ON processed_sentiment(content_hash);
CREATE INDEX IF NOT EXISTS idx_processed_sentiment_tier     ON processed_sentiment(importance_tier);


-- =============================================================================
-- TABLE: api_usage
-- Cross-session daily call counters for rate-limited APIs.
-- Gemini: 20 RPD verified hard limit. GNews: 100/day free tier.
-- =============================================================================
CREATE TABLE IF NOT EXISTS api_usage (
    date        DATE         NOT NULL,
    api         VARCHAR(50)  NOT NULL,
    call_count  INTEGER      DEFAULT 0,
    PRIMARY KEY (date, api)
);


-- =============================================================================
-- ROW LEVEL SECURITY
-- All tables locked to service_role key only.
-- anon key has NO access. Public repo does not expose data.
-- =============================================================================

ALTER TABLE system_state        ENABLE ROW LEVEL SECURITY;
ALTER TABLE trade_logs          ENABLE ROW LEVEL SECURITY;
ALTER TABLE raw_sentiment_data  ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_sentiment ENABLE ROW LEVEL SECURITY;
ALTER TABLE api_usage           ENABLE ROW LEVEL SECURITY;

-- Service role bypass (application uses service_role key)
-- anon role has zero policies = zero access
CREATE POLICY "service_role_only" ON system_state
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "service_role_only" ON trade_logs
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "service_role_only" ON raw_sentiment_data
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "service_role_only" ON processed_sentiment
    FOR ALL TO service_role USING (true) WITH CHECK (true);

CREATE POLICY "service_role_only" ON api_usage
    FOR ALL TO service_role USING (true) WITH CHECK (true);
