"""
Data quality validation rulepack.

Runs a comprehensive set of data quality checks and generates a Markdown report
with violations, statistics, and recommendations.

Usage:
    python -m tools.validate_rules --config config.yml \
        --symbols SOLUSDT,SUIUSDT,ADAUSDT \
        --start 2025-10-21T00:00:00Z \
        --end   2025-10-21T14:40:00Z \
        --tf 1s \
        --source bars \
        --report reports/sanity_2025-10-21.md
"""

import argparse
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import pandas as pd
from loguru import logger

from tools.common import ensure_dir, load_config, setup_logging
from tools.db import connect_and_register_views

# Validation thresholds (configurable)
PRICE_TOLERANCE_OHLC = 0.0001  # For kline comparison (optional)
SPREAD_TO_MID_MAX_BPS = 500  # 5% spread to mid threshold
SPREAD_OUTLIER_MAX_PCT = 0.1  # Allow 0.1% of rows to exceed spread threshold
MAX_OFFENDING_ROWS = 25  # Max rows to show per rule in report


def fetch_data(
    conn: duckdb.DuckDBPyConnection,
    symbols: List[str],
    start: str,
    end: str,
    tf: str,
    source: str,
) -> pd.DataFrame:
    """Fetch data for validation."""
    # Determine source view
    if tf == "1s":
        if source != "bars":
            raise ValueError("1s timeframe only available for bars source")
        view = "bars_1s"
    elif tf == "1m":
        view = "bars_1m" if source == "bars" else "klines_1m"
    else:
        raise ValueError(f"Unsupported timeframe: {tf}")

    symbol_list = ", ".join([f"'{s}'" for s in symbols])

    query = f"""
    SELECT *
    FROM {view}
    WHERE symbol IN ({symbol_list})
      AND ts >= TIMESTAMP '{start}'
      AND ts < TIMESTAMP '{end}'
    ORDER BY symbol, ts
    """

    result = conn.execute(query)
    df = result.fetchdf()

    # Ensure timestamp is UTC
    if "ts" in df.columns and not df.empty:
        if df["ts"].dt.tz is None:
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
        else:
            df["ts"] = df["ts"].dt.tz_convert("UTC")

    return df


def rule_r1_ohlc_ordering(df: pd.DataFrame) -> Tuple[int, pd.DataFrame]:
    """R1: OHLC ordering - low <= open,close <= high"""
    violations = df[
        ~((df["low"] <= df["open"]) &
          (df["low"] <= df["close"]) &
          (df["open"] <= df["high"]) &
          (df["close"] <= df["high"]))
    ].copy()

    return len(violations), violations.head(MAX_OFFENDING_ROWS)


def rule_r2_positive_prices(df: pd.DataFrame) -> Tuple[int, pd.DataFrame]:
    """R2: Non-negative prices - open,high,low,close > 0"""
    violations = df[
        ~((df["open"] > 0) &
          (df["high"] > 0) &
          (df["low"] > 0) &
          (df["close"] > 0))
    ].copy()

    return len(violations), violations.head(MAX_OFFENDING_ROWS)


def rule_r3_ask_gte_bid(df: pd.DataFrame) -> Tuple[int, pd.DataFrame]:
    """R3: Ask >= Bid (bars only; ignore if NaN)"""
    if "bid" not in df.columns or "ask" not in df.columns:
        return 0, pd.DataFrame()

    # Only check rows where both bid and ask are not NaN
    valid_quotes = df[df["bid"].notna() & df["ask"].notna()].copy()

    if valid_quotes.empty:
        return 0, pd.DataFrame()

    violations = valid_quotes[~(valid_quotes["ask"] >= valid_quotes["bid"])].copy()

    return len(violations), violations.head(MAX_OFFENDING_ROWS)


def rule_r4_no_nans_ohlc(df: pd.DataFrame) -> Tuple[int, pd.DataFrame]:
    """R4: No NaNs in OHLC (allow NaN in bid/ask/spread if unavailable)"""
    violations = df[
        df["open"].isna() |
        df["high"].isna() |
        df["low"].isna() |
        df["close"].isna()
    ].copy()

    return len(violations), violations.head(MAX_OFFENDING_ROWS)


def rule_r5_timestamp_continuity(df: pd.DataFrame, tf: str) -> Tuple[int, pd.DataFrame]:
    """R5: Timestamp continuity"""
    violations_list = []

    if tf == "1m":
        # Check UTC minute alignment (ts % 60 == 0)
        df_check = df.copy()
        df_check["ts_second"] = df_check["ts"].dt.second

        misaligned = df_check[df_check["ts_second"] != 0].copy()
        violations_list.append(misaligned)

    elif tf == "1s":
        # Check for gaps > 1 second between consecutive timestamps per symbol
        for symbol in df["symbol"].unique():
            symbol_df = df[df["symbol"] == symbol].sort_values("ts").copy()

            if len(symbol_df) < 2:
                continue

            symbol_df["ts_diff"] = symbol_df["ts"].diff()
            # Allow up to 1 second gap (gaps > 1s are violations)
            gaps = symbol_df[symbol_df["ts_diff"] > pd.Timedelta(seconds=1)].copy()

            if not gaps.empty:
                violations_list.append(gaps)

    if violations_list:
        violations = pd.concat(violations_list, ignore_index=True)
        return len(violations), violations.head(MAX_OFFENDING_ROWS)

    return 0, pd.DataFrame()


def rule_r6_spread_sanity(df: pd.DataFrame) -> Tuple[int, pd.DataFrame, Dict[str, Any]]:
    """R6: Spread sanity (bars only)"""
    if "spread" not in df.columns or "bid" not in df.columns or "ask" not in df.columns:
        return 0, pd.DataFrame(), {}

    # Filter to rows with valid spread data
    valid_spread = df[df["spread"].notna() & df["bid"].notna() & df["ask"].notna()].copy()

    if valid_spread.empty:
        return 0, pd.DataFrame(), {}

    # Check spread >= 0
    negative_spread = valid_spread[valid_spread["spread"] < 0].copy()

    # Check spread / mid < 5% for >99.9% of rows
    valid_spread["mid"] = (valid_spread["bid"] + valid_spread["ask"]) / 2
    valid_spread["spread_to_mid_bps"] = (valid_spread["spread"] / valid_spread["mid"]) * 10000

    excessive_spread = valid_spread[valid_spread["spread_to_mid_bps"] >= SPREAD_TO_MID_MAX_BPS].copy()

    total_violations = len(negative_spread) + len(excessive_spread)
    pct_excessive = (len(excessive_spread) / len(valid_spread)) * 100 if len(valid_spread) > 0 else 0

    # Combine violations
    all_violations = pd.concat([negative_spread, excessive_spread], ignore_index=True)

    stats = {
        "total_checked": len(valid_spread),
        "negative_spread_count": len(negative_spread),
        "excessive_spread_count": len(excessive_spread),
        "excessive_spread_pct": pct_excessive,
    }

    return total_violations, all_violations.head(MAX_OFFENDING_ROWS), stats


def rule_r7_kline_parity(
    conn: duckdb.DuckDBPyConnection,
    symbols: List[str],
    start: str,
    end: str,
    tf: str,
) -> Tuple[int, pd.DataFrame, Dict[str, Any]]:
    """R7: Optional parity check with klines (if available & tf=1m)"""
    if tf != "1m":
        return 0, pd.DataFrame(), {"note": "Only applicable for 1m timeframe"}

    try:
        symbol_list = ", ".join([f"'{s}'" for s in symbols])

        query = f"""
        SELECT
            symbol,
            ts,
            our_open, kline_open, open_diff,
            our_high, kline_high, high_diff,
            our_low, kline_low, low_diff,
            our_close, kline_close, close_diff,
            volume_diff_bps
        FROM compare_our_vs_kline_1m
        WHERE symbol IN ({symbol_list})
          AND ts >= TIMESTAMP '{start}'
          AND ts < TIMESTAMP '{end}'
        ORDER BY symbol, ts
        """

        result = conn.execute(query)
        comparison = result.fetchdf()

        if comparison.empty:
            return 0, pd.DataFrame(), {"note": "No kline data available for comparison"}

        # Check for OHLC differences exceeding tolerance
        violations = comparison[
            (comparison["open_diff"].abs() > PRICE_TOLERANCE_OHLC) |
            (comparison["high_diff"].abs() > PRICE_TOLERANCE_OHLC) |
            (comparison["low_diff"].abs() > PRICE_TOLERANCE_OHLC) |
            (comparison["close_diff"].abs() > PRICE_TOLERANCE_OHLC)
        ].copy()

        # Volume diff stats (informational)
        avg_vol_diff_bps = comparison["volume_diff_bps"].mean()
        max_vol_diff_bps = comparison["volume_diff_bps"].max()

        stats = {
            "total_compared": len(comparison),
            "price_violations": len(violations),
            "avg_volume_diff_bps": avg_vol_diff_bps,
            "max_volume_diff_bps": max_vol_diff_bps,
        }

        return len(violations), violations.head(MAX_OFFENDING_ROWS), stats

    except Exception as e:
        logger.warning(f"R7 kline parity check failed: {e}")
        return 0, pd.DataFrame(), {"note": f"Check failed: {e}"}


def generate_report(
    symbols: List[str],
    start: str,
    end: str,
    tf: str,
    source: str,
    results: Dict[str, Any],
    report_path: str,
) -> None:
    """Generate Markdown validation report."""
    ensure_dir(os.path.dirname(report_path))

    with open(report_path, "w", encoding="utf-8") as f:
        # Header
        f.write("# Data Quality Validation Report\n\n")
        f.write(f"**Generated**: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC\n\n")

        # Metadata
        f.write("## Run Metadata\n\n")
        f.write(f"- **Symbols**: {', '.join(symbols)}\n")
        f.write(f"- **Time Range**: {start} to {end}\n")
        f.write(f"- **Timeframe**: {tf}\n")
        f.write(f"- **Source**: {source}\n")
        f.write(f"- **Total Rows Checked**: {results.get('total_rows', 0):,}\n\n")

        # Summary table
        f.write("## Validation Summary\n\n")
        f.write("| Rule | Description | Total Checked | Violations | % Violated |\n")
        f.write("|------|-------------|---------------|------------|------------|\n")

        total_rows = results.get("total_rows", 0)

        for rule_name, rule_data in results.get("rules", {}).items():
            checked = rule_data.get("checked", total_rows)
            violations = rule_data.get("violations", 0)
            pct = (violations / checked * 100) if checked > 0 else 0

            f.write(f"| {rule_name} | {rule_data.get('description', '')} | "
                   f"{checked:,} | {violations:,} | {pct:.2f}% |\n")

        f.write("\n")

        # Detailed violations per rule
        f.write("## Detailed Violations\n\n")

        for rule_name, rule_data in results.get("rules", {}).items():
            f.write(f"### {rule_name}: {rule_data.get('description', '')}\n\n")

            violations = rule_data.get("violations", 0)
            if violations == 0:
                f.write("✅ **PASS** - No violations detected\n\n")
                continue

            f.write(f"❌ **{violations:,} violations detected**\n\n")

            # Additional stats if available
            if "stats" in rule_data:
                f.write("**Additional Statistics:**\n\n")
                for key, value in rule_data["stats"].items():
                    f.write(f"- {key}: {value}\n")
                f.write("\n")

            # Top offending rows
            offending_df = rule_data.get("offending_rows")
            if offending_df is not None and not offending_df.empty:
                f.write(f"**Top {min(len(offending_df), MAX_OFFENDING_ROWS)} Offending Rows:**\n\n")
                f.write("```\n")
                f.write(offending_df.to_string(index=False))
                f.write("\n```\n\n")

        # Overall recommendation
        f.write("## Overall Assessment\n\n")

        # Count critical violations (R1-R4)
        critical_violations = sum([
            results.get("rules", {}).get(f"R{i}", {}).get("violations", 0)
            for i in range(1, 5)
        ])

        # Count spread violations
        r6_violations = results.get("rules", {}).get("R6", {}).get("violations", 0)
        r6_pct = results.get("rules", {}).get("R6", {}).get("stats", {}).get("excessive_spread_pct", 0)

        if critical_violations == 0 and r6_violations == 0:
            f.write("✅ **PASS** - All validation rules passed successfully.\n\n")
            f.write("Data quality is excellent with no structural issues detected.\n")
        elif critical_violations == 0 and r6_pct < SPREAD_OUTLIER_MAX_PCT:
            f.write("⚠️ **INVESTIGATE** - Minor issues detected.\n\n")
            f.write("Critical rules passed, but some spread outliers were found. "
                   "Review the detailed violations above.\n")
        else:
            f.write("🚫 **FAIL** - Significant data quality issues detected.\n\n")
            f.write(f"Critical violations: {critical_violations:,}\n")
            f.write("Immediate investigation and remediation required.\n")

    logger.info(f"✓ Report written to {report_path}")


def run_validation(
    config: Dict[str, Any],
    symbols: List[str],
    start: str,
    end: str,
    tf: str,
    source: str,
    report: str,
) -> None:
    """Run validation rulepack and generate report."""
    setup_logging("validate_rules", config)

    base_path = config["general"]["base_path"]

    logger.info(f"Running validation: symbols={symbols}, tf={tf}, source={source}")
    logger.info(f"Time range: {start} to {end}")

    # Connect to DuckDB and register views
    conn = connect_and_register_views(base_path)
    logger.info("Views loaded successfully")

    try:

        # Fetch data
        df = fetch_data(conn, symbols, start, end, tf, source)

        if df.empty:
            logger.warning("No data returned for validation")
            logger.warning(f"Verify data exists for symbols {symbols} in range {start} to {end}")
            return

        logger.info(f"Loaded {len(df):,} rows for validation")

        # Run validation rules
        results = {"total_rows": len(df), "rules": {}}

        # R1: OHLC ordering
        logger.info("Running R1: OHLC ordering...")
        r1_count, r1_rows = rule_r1_ohlc_ordering(df)
        results["rules"]["R1"] = {
            "description": "OHLC ordering (low <= open,close <= high)",
            "checked": len(df),
            "violations": r1_count,
            "offending_rows": r1_rows,
        }

        # R2: Positive prices
        logger.info("Running R2: Positive prices...")
        r2_count, r2_rows = rule_r2_positive_prices(df)
        results["rules"]["R2"] = {
            "description": "Non-negative prices (OHLC > 0)",
            "checked": len(df),
            "violations": r2_count,
            "offending_rows": r2_rows,
        }

        # R3: Ask >= Bid
        logger.info("Running R3: Ask >= Bid...")
        r3_count, r3_rows = rule_r3_ask_gte_bid(df)
        results["rules"]["R3"] = {
            "description": "Ask >= Bid (bars only)",
            "checked": len(df[df["bid"].notna() & df["ask"].notna()]) if "bid" in df.columns else 0,
            "violations": r3_count,
            "offending_rows": r3_rows,
        }

        # R4: No NaNs in OHLC
        logger.info("Running R4: No NaNs in OHLC...")
        r4_count, r4_rows = rule_r4_no_nans_ohlc(df)
        results["rules"]["R4"] = {
            "description": "No NaNs in OHLC",
            "checked": len(df),
            "violations": r4_count,
            "offending_rows": r4_rows,
        }

        # R5: Timestamp continuity
        logger.info("Running R5: Timestamp continuity...")
        r5_count, r5_rows = rule_r5_timestamp_continuity(df, tf)
        results["rules"]["R5"] = {
            "description": "Timestamp continuity and alignment",
            "checked": len(df),
            "violations": r5_count,
            "offending_rows": r5_rows,
        }

        # R6: Spread sanity
        logger.info("Running R6: Spread sanity...")
        r6_count, r6_rows, r6_stats = rule_r6_spread_sanity(df)
        results["rules"]["R6"] = {
            "description": "Spread sanity (spread >= 0, < 5% mid)",
            "checked": r6_stats.get("total_checked", 0),
            "violations": r6_count,
            "offending_rows": r6_rows,
            "stats": r6_stats,
        }

        # R7: Kline parity (optional)
        if tf == "1m" and source == "bars":
            logger.info("Running R7: Kline parity check...")
            r7_count, r7_rows, r7_stats = rule_r7_kline_parity(conn, symbols, start, end, tf)
            results["rules"]["R7"] = {
                "description": "Parity with official klines (optional)",
                "checked": r7_stats.get("total_compared", 0),
                "violations": r7_count,
                "offending_rows": r7_rows,
                "stats": r7_stats,
            }

        # Generate report
        logger.info("Generating report...")
        generate_report(symbols, start, end, tf, source, results, report)

        # Log summary
        total_violations = sum([r.get("violations", 0) for r in results["rules"].values()])
        logger.info(f"Validation complete: {total_violations:,} total violations across all rules")

    except Exception as e:
        logger.exception(f"Validation failed: {e}")
        raise
    finally:
        conn.close()


def main():
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="Run data quality validation rulepack")
    parser.add_argument("--config", type=str, default="config.yml", help="Path to config.yml")
    parser.add_argument("--symbols", type=str, required=True,
                       help="Comma-separated list of symbols")
    parser.add_argument("--start", type=str, required=True,
                       help="Start timestamp (ISO format, e.g., 2025-10-21T00:00:00Z)")
    parser.add_argument("--end", type=str, required=True,
                       help="End timestamp (ISO format, e.g., 2025-10-21T14:40:00Z)")
    parser.add_argument("--tf", type=str, default="1s", choices=["1s", "1m"],
                       help="Timeframe (1s or 1m)")
    parser.add_argument("--source", type=str, default="bars", choices=["bars", "klines"],
                       help="Data source (bars or klines)")
    parser.add_argument("--report", type=str, required=True,
                       help="Output report path (e.g., reports/sanity_2025-10-21.md)")

    args = parser.parse_args()

    # Load config
    config = load_config(args.config)

    # Parse symbols
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    if not symbols:
        print("Error: No symbols provided")
        return

    # Run validation
    run_validation(
        config=config,
        symbols=symbols,
        start=args.start,
        end=args.end,
        tf=args.tf,
        source=args.source,
        report=args.report,
    )


if __name__ == "__main__":
    main()
