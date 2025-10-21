-- Convenience DuckDB views for multi-timeframe analysis
-- Usage: Run in DuckDB CLI or via Python connector
-- Note: Adjust base path wildcard if needed (/** works across drives)

PRAGMA threads=4;

-- ========================================
-- Base 1-second bars (all symbols, all dates)
-- ========================================
CREATE OR REPLACE VIEW bars_all_1s AS
SELECT *
FROM read_parquet('D:/CryptoDataLake/parquet/**/*.parquet')
WHERE window_start IS NOT NULL
ORDER BY symbol, window_start;

-- ========================================
-- 1-minute bars (rollup from 1s)
-- ========================================
CREATE OR REPLACE VIEW bars_1m AS
SELECT
    symbol,
    date_trunc('minute', window_start) AS window_start,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    -- VWAP recalculated from aggregated volumes
    CASE
        WHEN sum(volume_base) > 0 THEN sum(volume_quote) / sum(volume_base)
        ELSE last(close)
    END AS vwap,
    last(bid) AS bid,
    last(ask) AS ask,
    last(spread) AS spread
FROM bars_all_1s
GROUP BY symbol, date_trunc('minute', window_start)
ORDER BY symbol, window_start;

-- ========================================
-- 5-minute bars (rollup from 1s)
-- ========================================
CREATE OR REPLACE VIEW bars_5m AS
SELECT
    symbol,
    date_trunc('minute', window_start) - INTERVAL ((extract('minute' FROM window_start)::INTEGER % 5) * 60) second AS window_start,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    CASE
        WHEN sum(volume_base) > 0 THEN sum(volume_quote) / sum(volume_base)
        ELSE last(close)
    END AS vwap,
    last(bid) AS bid,
    last(ask) AS ask,
    last(spread) AS spread
FROM bars_all_1s
GROUP BY symbol, date_trunc('minute', window_start) - INTERVAL ((extract('minute' FROM window_start)::INTEGER % 5) * 60) second
ORDER BY symbol, window_start;

-- ========================================
-- 1-hour bars (rollup from 1s)
-- ========================================
CREATE OR REPLACE VIEW bars_1h AS
SELECT
    symbol,
    date_trunc('hour', window_start) AS window_start,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    CASE
        WHEN sum(volume_base) > 0 THEN sum(volume_quote) / sum(volume_base)
        ELSE last(close)
    END AS vwap,
    last(bid) AS bid,
    last(ask) AS ask,
    last(spread) AS spread
FROM bars_all_1s
GROUP BY symbol, date_trunc('hour', window_start)
ORDER BY symbol, window_start;

-- ========================================
-- Latest price (most recent bar per symbol)
-- ========================================
CREATE OR REPLACE VIEW latest_price AS
SELECT DISTINCT ON (symbol)
    symbol,
    window_start,
    close,
    bid,
    ask,
    spread
FROM bars_all_1s
ORDER BY symbol, window_start DESC;

-- ========================================
-- Daily summary stats
-- ========================================
CREATE OR REPLACE VIEW daily_summary AS
SELECT
    symbol,
    CAST(window_start AS DATE) AS date,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    CASE
        WHEN sum(volume_base) > 0 THEN sum(volume_quote) / sum(volume_base)
        ELSE last(close)
    END AS vwap
FROM bars_all_1s
GROUP BY symbol, CAST(window_start AS DATE)
ORDER BY symbol, date;
