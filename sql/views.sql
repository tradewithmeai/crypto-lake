-- Analysis-Ready DuckDB Views for Crypto Data Lake
--
-- Usage: Replace @@BASE@@ with actual base path before execution
-- Example: SELECT * FROM bars_1s WHERE symbol = 'SOLUSDT' LIMIT 10;
--
-- Note: These views are designed to make analysis "one SELECT away"

PRAGMA threads=4;

-- ========================================
-- 1) bars_1s - Base 1-second bars from our collector
-- ========================================
CREATE OR REPLACE VIEW bars_1s AS
SELECT
    'binance' AS exchange,
    symbol,
    window_start AS ts,
    open,
    high,
    low,
    close,
    volume_base,
    volume_quote,
    trade_count,
    vwap,
    bid,
    ask,
    spread
FROM read_parquet('@@BASE@@/parquet/binance/**/*.parquet')
WHERE window_start IS NOT NULL
ORDER BY symbol, ts;

-- Coinbase 1-second bars (if data exists)
CREATE OR REPLACE VIEW bars_1s_coinbase AS
SELECT
    'coinbase' AS exchange,
    symbol,
    window_start AS ts,
    open, high, low, close,
    volume_base, volume_quote, trade_count,
    vwap, bid, ask, spread
FROM read_parquet('@@BASE@@/parquet/coinbase/**/*.parquet')
WHERE window_start IS NOT NULL
ORDER BY symbol, ts;

-- Kraken 1-second bars (if data exists)
CREATE OR REPLACE VIEW bars_1s_kraken AS
SELECT
    'kraken' AS exchange,
    symbol,
    window_start AS ts,
    open, high, low, close,
    volume_base, volume_quote, trade_count,
    vwap, bid, ask, spread
FROM read_parquet('@@BASE@@/parquet/kraken/**/*.parquet')
WHERE window_start IS NOT NULL
ORDER BY symbol, ts;

-- ========================================
-- 2) bars_1m - 1-minute rollup from our 1s bars
-- ========================================
CREATE OR REPLACE VIEW bars_1m AS
SELECT
    exchange,
    symbol,
    date_trunc('minute', ts) AS ts,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    last(vwap) AS vwap,
    last(bid) AS bid,
    last(ask) AS ask,
    last(spread) AS spread,
    count(*) AS bar_count
FROM bars_1s
GROUP BY exchange, symbol, date_trunc('minute', ts)
ORDER BY exchange, symbol, ts;

-- ========================================
-- 3) klines_1m - Binance official 1m klines (if available)
-- ========================================
CREATE OR REPLACE VIEW klines_1m AS
SELECT
    'binance' AS exchange,
    symbol,
    window_start AS ts,
    open,
    high,
    low,
    close,
    volume_base,
    volume_quote,
    trade_count,
    taker_buy_base,
    taker_buy_quote
FROM read_parquet('@@BASE@@/klines/binance/**/*.parquet', hive_partitioning=true)
WHERE window_start IS NOT NULL
ORDER BY symbol, ts;

-- ========================================
-- 4) compare_our_vs_kline_1m - Comparison of our bars vs official klines
-- ========================================
CREATE OR REPLACE VIEW compare_our_vs_kline_1m AS
SELECT
    b.exchange,
    b.symbol,
    b.ts,
    b.open AS our_open,
    k.open AS kline_open,
    (b.open - k.open) AS open_diff,
    b.high AS our_high,
    k.high AS kline_high,
    (b.high - k.high) AS high_diff,
    b.low AS our_low,
    k.low AS kline_low,
    (b.low - k.low) AS low_diff,
    b.close AS our_close,
    k.close AS kline_close,
    (b.close - k.close) AS close_diff,
    b.volume_base AS our_volume,
    k.volume_base AS kline_volume,
    (b.volume_base - k.volume_base) AS volume_diff_abs,
    CASE
        WHEN k.volume_base > 0 THEN (ABS(b.volume_base - k.volume_base) / k.volume_base) * 10000
        ELSE NULL
    END AS volume_diff_bps
FROM bars_1m b
INNER JOIN klines_1m k
    ON b.symbol = k.symbol
    AND b.ts = k.ts
ORDER BY b.symbol, b.ts;

-- ========================================
-- 5) funding_oi_hourly - Funding rates and open interest (if available)
-- ========================================
CREATE OR REPLACE VIEW funding_oi_hourly AS
SELECT
    'binance_futures' AS exchange,
    symbol,
    ts,
    funding_rate,
    open_interest,
    oi_usd
FROM read_parquet('@@BASE@@/derivs/binance_futures/**/*.parquet', hive_partitioning=true)
WHERE ts IS NOT NULL
ORDER BY symbol, ts;

-- ========================================
-- 6) macro_minute - 1-minute macro data (SPY, UUP, ES=F, etc.)
-- ========================================
CREATE OR REPLACE VIEW macro_minute AS
SELECT
    ticker,
    ts,
    open,
    high,
    low,
    close,
    volume
FROM read_parquet('@@BASE@@/macro/minute/**/*.parquet', hive_partitioning=true)
WHERE ts IS NOT NULL
ORDER BY ticker, ts;

-- ========================================
-- 7) dxy_synthetic_minute - DXY synthetic index (if available)
-- ========================================
CREATE OR REPLACE VIEW dxy_synthetic_minute AS
SELECT
    'DXY_SYN' AS ticker,
    ts,
    open,
    high,
    low,
    close,
    volume
FROM read_parquet('@@BASE@@/macro/minute/DXY_SYN/**.parquet', hive_partitioning=true)
WHERE ts IS NOT NULL
ORDER BY ts;

-- ========================================
-- 5-minute bars (rollup from bars_1s)
-- ========================================
CREATE OR REPLACE VIEW bars_5m AS
SELECT
    exchange,
    symbol,
    date_trunc('minute', ts) - INTERVAL ((extract('minute' FROM ts)::INTEGER % 5) * 60) second AS ts,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    last(vwap) AS vwap,
    last(bid) AS bid,
    last(ask) AS ask,
    last(spread) AS spread,
    count(*) AS bar_count
FROM bars_1s
GROUP BY exchange, symbol, date_trunc('minute', ts) - INTERVAL ((extract('minute' FROM ts)::INTEGER % 5) * 60) second
ORDER BY exchange, symbol, ts;

-- ========================================
-- 15-minute bars (rollup from bars_1s)
-- ========================================
CREATE OR REPLACE VIEW bars_15m AS
SELECT
    exchange,
    symbol,
    date_trunc('minute', ts) - INTERVAL ((extract('minute' FROM ts)::INTEGER % 15) * 60) second AS ts,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    last(vwap) AS vwap,
    last(bid) AS bid,
    last(ask) AS ask,
    last(spread) AS spread,
    count(*) AS bar_count
FROM bars_1s
GROUP BY exchange, symbol, date_trunc('minute', ts) - INTERVAL ((extract('minute' FROM ts)::INTEGER % 15) * 60) second
ORDER BY exchange, symbol, ts;

-- ========================================
-- 1-hour bars (rollup from bars_1s)
-- ========================================
CREATE OR REPLACE VIEW bars_1h AS
SELECT
    exchange,
    symbol,
    date_trunc('hour', ts) AS ts,
    first(open) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close) AS close,
    sum(volume_base) AS volume_base,
    sum(volume_quote) AS volume_quote,
    sum(trade_count) AS trade_count,
    last(vwap) AS vwap,
    last(bid) AS bid,
    last(ask) AS ask,
    last(spread) AS spread,
    count(*) AS bar_count
FROM bars_1s
GROUP BY exchange, symbol, date_trunc('hour', ts)
ORDER BY exchange, symbol, ts;

-- Latest price (most recent bar per symbol)
CREATE OR REPLACE VIEW latest_price AS
SELECT DISTINCT ON (exchange, symbol)
    exchange,
    symbol,
    ts,
    close,
    bid,
    ask,
    spread
FROM bars_1s
ORDER BY exchange, symbol, ts DESC;

-- Daily summary stats
CREATE OR REPLACE VIEW daily_summary AS
SELECT
    exchange,
    symbol,
    CAST(ts AS DATE) AS date,
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
    count(*) AS bar_count
FROM bars_1s
GROUP BY exchange, symbol, CAST(ts AS DATE)
ORDER BY exchange, symbol, date;
