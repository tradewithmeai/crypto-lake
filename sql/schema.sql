-- Crypto Lake Relational Schema
--
-- Portable SQL schema compatible with DuckDB, SQLite, and PostgreSQL
-- Supports time-series partitioning for efficient querying
--
-- Usage:
--   DuckDB/SQLite: CREATE TABLE IF NOT EXISTS ... (as-is)
--   PostgreSQL: Uncomment PARTITION BY RANGE for time-series optimization
--
-- Naming Convention: lowercase_snake_case

-- ========================================
-- 1) bars_1s - Base 1-second OHLCV bars
-- ========================================
-- High-frequency trading data from WebSocket collector
-- Includes bid/ask/spread for market microstructure analysis
--
-- PostgreSQL Partitioning Strategy:
--   PARTITION BY RANGE (ts) with daily partitions
--   Example: CREATE TABLE bars_1s_2025_01_15 PARTITION OF bars_1s
--            FOR VALUES FROM ('2025-01-15') TO ('2025-01-16');

CREATE TABLE IF NOT EXISTS bars_1s (
    symbol TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume_base DOUBLE PRECISION,
    volume_quote DOUBLE PRECISION,
    trade_count INTEGER,
    vwap DOUBLE PRECISION,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
); -- PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS idx_bars_1s_symbol_ts ON bars_1s(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_bars_1s_ts ON bars_1s(ts);

-- ========================================
-- 2) bars_1m - Aggregated 1-minute bars
-- ========================================
-- Rollup of bars_1s for reduced storage and faster queries
-- Standard OHLCV format compatible with most charting libraries
--
-- PostgreSQL Partitioning Strategy:
--   PARTITION BY RANGE (ts) with monthly partitions
--   Example: CREATE TABLE bars_1m_2025_01 PARTITION OF bars_1m
--            FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');

CREATE TABLE IF NOT EXISTS bars_1m (
    symbol TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume_base DOUBLE PRECISION,
    volume_quote DOUBLE PRECISION,
    trade_count INTEGER,
    vwap DOUBLE PRECISION,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
); -- PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS idx_bars_1m_symbol_ts ON bars_1m(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_bars_1m_ts ON bars_1m(ts);

-- ========================================
-- 3) klines_1m - Binance Klines API data
-- ========================================
-- Official 1-minute candles from Binance REST API
-- Used for validation and gap detection
--
-- PostgreSQL Partitioning Strategy:
--   PARTITION BY RANGE (ts) with monthly partitions

CREATE TABLE IF NOT EXISTS klines_1m (
    symbol TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume DOUBLE PRECISION,
    trades INTEGER,
    PRIMARY KEY (symbol, ts)
); -- PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS idx_klines_1m_symbol_ts ON klines_1m(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_klines_1m_ts ON klines_1m(ts);

-- ========================================
-- 4) compare_our_vs_kline_1m - Validation delta table
-- ========================================
-- Differences between our bars_1m and official klines_1m
-- Tracks OHLC discrepancies and absolute error for QA
--
-- No partitioning needed (delta table is typically smaller)

CREATE TABLE IF NOT EXISTS compare_our_vs_kline_1m (
    symbol TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    diff_open DOUBLE PRECISION,
    diff_high DOUBLE PRECISION,
    diff_low DOUBLE PRECISION,
    diff_close DOUBLE PRECISION,
    abs_error DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
);

CREATE INDEX IF NOT EXISTS idx_compare_symbol_ts ON compare_our_vs_kline_1m(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_compare_abs_error ON compare_our_vs_kline_1m(abs_error) WHERE abs_error > 0;

-- ========================================
-- 5) funding_oi_hourly - Derivatives data
-- ========================================
-- Funding rate, open interest, and turnover for perpetual futures
-- Sampled at hourly intervals from Binance Futures
--
-- PostgreSQL Partitioning Strategy:
--   PARTITION BY RANGE (ts) with monthly partitions

CREATE TABLE IF NOT EXISTS funding_oi_hourly (
    symbol TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    funding_rate DOUBLE PRECISION,
    open_interest DOUBLE PRECISION,
    turnover DOUBLE PRECISION,
    PRIMARY KEY (symbol, ts)
); -- PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS idx_funding_oi_symbol_ts ON funding_oi_hourly(symbol, ts);
CREATE INDEX IF NOT EXISTS idx_funding_oi_ts ON funding_oi_hourly(ts);

-- ========================================
-- 6) macro_minute - Macro indicators
-- ========================================
-- Time-series data for macro/FX tickers from yfinance
-- Examples: SPY, ES=F, EURUSD=X, UUP, etc.
--
-- Key format: ticker symbol (e.g., "SPY", "EURUSD=X")
-- PostgreSQL Partitioning Strategy:
--   PARTITION BY RANGE (ts) with monthly partitions

CREATE TABLE IF NOT EXISTS macro_minute (
    macro_key TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    value DOUBLE PRECISION,
    PRIMARY KEY (macro_key, ts)
); -- PARTITION BY RANGE (ts);

CREATE INDEX IF NOT EXISTS idx_macro_minute_key_ts ON macro_minute(macro_key, ts);
CREATE INDEX IF NOT EXISTS idx_macro_minute_ts ON macro_minute(ts);

-- ========================================
-- Entity Relationships Summary
-- ========================================
--
-- Data Flow:
--   bars_1s (raw WebSocket data)
--      ↓ (aggregate by minute)
--   bars_1m (1-minute candles)
--      ↓ (compare with)
--   klines_1m (Binance API candles)
--      ↓ (generate)
--   compare_our_vs_kline_1m (validation deltas)
--
-- Analysis Joins:
--   bars_1m + funding_oi_hourly (derivatives overlay)
--   bars_1m + macro_minute (correlation analysis)
--
-- Indexes optimized for:
--   - Time-range queries: WHERE ts BETWEEN ? AND ?
--   - Symbol filtering: WHERE symbol = ?
--   - Validation scans: WHERE abs_error > 0
