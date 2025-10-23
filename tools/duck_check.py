import argparse, os, duckdb

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True)
    ap.add_argument("--exchange", default="binance")
    ap.add_argument("--symbol", required=True)
    args = ap.parse_args()

    glob_path = os.path.join(args.base, "parquet", args.exchange, args.symbol, "**", "*.parquet")
    con = duckdb.connect()

    print("Files glob:", glob_path)

    # Row count
    rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{glob_path}')").fetchone()[0]
    print("Rows:", rows)
    if rows == 0:
        print("No rows found. Check transformer output or path/symbol casing.")
        return

    # Use correct timestamp column: window_start
    tmin, tmax = con.execute(
        f"SELECT min(window_start), max(window_start) FROM read_parquet('{glob_path}')"
    ).fetchone()
    print(f"Time span: {tmin} -> {tmax}")

    # Missing seconds (safe version — no nested window inside SUM)
    query = f"""
        WITH d AS (
            SELECT window_start AS ts,
                   LEAD(window_start) OVER (ORDER BY window_start) AS next_ts
            FROM read_parquet('{glob_path}')
        ),
        g AS (
            SELECT EXTRACT(EPOCH FROM (next_ts - ts)) AS gap
            FROM d
            WHERE next_ts IS NOT NULL
        )
        SELECT COUNT(*) FROM g WHERE gap > 1;
    """
    miss = con.execute(query).fetchone()[0]
    print("Missing seconds:", miss)

if __name__ == "__main__":
    main()
