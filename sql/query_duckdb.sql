-- Summary: Example DuckDB views to query Parquet 1-second bars and rollup to 1-minute windows.
PRAGMA threads=4;

-- Root of Parquet dataset (adjust if base_path changed):
-- Reads all exchange/symbol partitions under default config base path.
CREATE OR REPLACE VIEW bars_all_1s AS
SELECT *
FROM read_parquet('D:/CryptoDataLake/parquet/**');

CREATE OR REPLACE VIEW bars_1m AS
WITH src AS (
    SELECT
        symbol,
        window_start,
        open,
        high,
        low,
        close,
        volume_base,
        volume_quote
    FROM bars_all_1s
)
SELECT
    symbol,
    date_trunc('minute', window_start) AS ts,
    first(open)     AS open,
    max(high)       AS high,
    min(low)        AS low,
    last(close)     AS close,
    sum(volume_base)  AS volume_base,
    sum(volume_quote) AS volume_quote
FROM src
GROUP BY symbol, ts
ORDER BY symbol, ts;

CREATE OR REPLACE VIEW latest_price AS
SELECT DISTINCT ON (symbol)
    symbol,
    window_start AS ts,
    close
FROM bars_all_1s
ORDER BY symbol, ts DESC;
