"""
Schema Validator

Adaptor that reuses existing R1-R7 validation rules from tools.validate_rules.py
and transforms outputs to JSONL contract format.

IMPORTANT: This module does NOT duplicate rule logic - it imports and calls
existing rule functions.
"""

import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import duckdb
import pandas as pd
from loguru import logger

# Import existing rule functions (NO duplication)
from tools.validate_rules import (
    rule_r1_ohlc_ordering,
    rule_r2_positive_prices,
    rule_r3_ask_gte_bid,
    rule_r4_no_nans_ohlc,
    rule_r5_timestamp_continuity,
    rule_r6_spread_sanity,
    rule_r7_kline_parity,
)

# Severity mapping - single source of truth for both schema validation and reporting
SEVERITY_MAP = {
    "R1_OHLC_ORDER": "critical",
    "R2_POSITIVE_PRICES": "critical",
    "R4_NO_NANS_OHLC": "critical",
    "R5_TIMESTAMP_CONTINUITY": "critical",
    "R3_ASK_GTE_BID": "major",
    "R6_SPREAD_SANITY": "major",
    "NEGATIVE_VOLUME": "major",
    "R7_KLINE_PARITY": "minor",
}


def to_iso8601_utc(ts: pd.Timestamp) -> str:
    """Convert pandas Timestamp to ISO8601 UTC string."""
    if ts.tzinfo is None:
        ts = ts.tz_localize('UTC')
    else:
        ts = ts.tz_convert('UTC')
    return ts.isoformat()


def validate_schema(
    df: pd.DataFrame,
    conn: duckdb.DuckDBPyConnection,
    symbols: List[str],
    start: str,
    end: str,
    tf: str,
) -> List[Dict[str, Any]]:
    """
    Run all schema validation rules and return violations in JSONL format.

    Args:
        df: DataFrame to validate (from bars_1s or bars_1m view)
        conn: DuckDB connection (for R7 kline parity check)
        symbols: List of symbols being validated
        start: Start timestamp (ISO8601)
        end: End timestamp (ISO8601)
        tf: Timeframe ("1s" or "1m")

    Returns:
        List of violation dictionaries matching JSONL contract:
        {
            "symbol": str,
            "ts": str,  # ISO8601 UTC
            "rule": str,
            "severity": str,
            "detail": str,
            "row_sample": dict
        }
    """
    violations = []

    if df.empty:
        logger.warning("Empty DataFrame provided for validation")
        return violations

    # R1: OHLC Ordering
    count_r1, viol_r1 = rule_r1_ohlc_ordering(df)
    violations.extend(_transform_violations(
        viol_r1, "R1_OHLC_ORDER", "OHLC ordering violated: low <= open,close <= high"
    ))
    logger.info(f"R1: {count_r1} OHLC ordering violations")

    # R2: Positive Prices
    count_r2, viol_r2 = rule_r2_positive_prices(df)
    violations.extend(_transform_violations(
        viol_r2, "R2_POSITIVE_PRICES", "Negative or zero prices detected"
    ))
    logger.info(f"R2: {count_r2} positive price violations")

    # R3: Ask >= Bid
    count_r3, viol_r3 = rule_r3_ask_gte_bid(df)
    violations.extend(_transform_violations(
        viol_r3, "R3_ASK_GTE_BID", "Ask < Bid detected"
    ))
    logger.info(f"R3: {count_r3} ask/bid violations")

    # R4: No NaNs in OHLC
    count_r4, viol_r4 = rule_r4_no_nans_ohlc(df)
    violations.extend(_transform_violations(
        viol_r4, "R4_NO_NANS_OHLC", "NaN values in OHLC columns"
    ))
    logger.info(f"R4: {count_r4} NaN violations")

    # R5: Timestamp Continuity
    count_r5, viol_r5 = rule_r5_timestamp_continuity(df, tf)
    violations.extend(_transform_violations(
        viol_r5, "R5_TIMESTAMP_CONTINUITY", "Timestamp gaps or misalignment"
    ))
    logger.info(f"R5: {count_r5} continuity violations")

    # R6: Spread Sanity
    count_r6, viol_r6, stats_r6 = rule_r6_spread_sanity(df)
    violations.extend(_transform_violations(
        viol_r6, "R6_SPREAD_SANITY", "Spread sanity check failed"
    ))
    logger.info(f"R6: {count_r6} spread violations")

    # R7: Kline Parity (only for 1m timeframe)
    count_r7, viol_r7, stats_r7 = rule_r7_kline_parity(conn, symbols, start, end, tf)
    violations.extend(_transform_violations(
        viol_r7, "R7_KLINE_PARITY", "Kline parity check failed"
    ))
    logger.info(f"R7: {count_r7} kline parity violations")

    # Check for negative volumes (not in original rules, but part of QA spec)
    negative_vol = df[
        (df.get("volume_base", pd.Series([0])) < 0) |
        (df.get("volume_quote", pd.Series([0])) < 0)
    ].copy()

    if not negative_vol.empty:
        violations.extend(_transform_violations(
            negative_vol.head(25), "NEGATIVE_VOLUME", "Negative volume detected"
        ))
        logger.info(f"NEGATIVE_VOLUME: {len(negative_vol)} violations")

    logger.info(f"Total violations: {len(violations)}")
    return violations


def _transform_violations(
    viol_df: pd.DataFrame,
    rule_name: str,
    detail: str
) -> List[Dict[str, Any]]:
    """
    Transform violation DataFrame to JSONL contract format.

    Args:
        viol_df: DataFrame of violating rows
        rule_name: Rule identifier (e.g., "R1_OHLC_ORDER")
        detail: Human-readable detail string

    Returns:
        List of violation dictionaries
    """
    violations = []

    for _, row in viol_df.iterrows():
        # Extract symbol and timestamp
        symbol = row.get("symbol", "UNKNOWN")
        ts = row.get("ts", pd.NaT)

        # Convert timestamp to ISO8601 UTC
        if pd.isna(ts):
            ts_str = datetime.now(timezone.utc).isoformat()
        else:
            ts_str = to_iso8601_utc(ts)

        # Get severity from mapping
        severity = SEVERITY_MAP.get(rule_name, "major")

        # Convert row to dict, handling NaN/NaT values
        row_sample = row.to_dict()
        # Clean up row_sample (remove NaT, convert floats)
        row_sample = {
            k: (None if pd.isna(v) else (v.isoformat() if isinstance(v, pd.Timestamp) else float(v) if isinstance(v, (int, float)) else str(v)))
            for k, v in row_sample.items()
        }

        violation = {
            "symbol": str(symbol),
            "ts": ts_str,
            "rule": rule_name,
            "severity": severity,
            "detail": detail,
            "row_sample": row_sample
        }

        violations.append(violation)

    return violations


def get_critical_violations(violations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter violations to only critical severity.

    Args:
        violations: List of violation dictionaries

    Returns:
        List of critical violations
    """
    return [v for v in violations if v["severity"] == "critical"]


def get_violation_summary(violations: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Get summary counts by rule.

    Args:
        violations: List of violation dictionaries

    Returns:
        Dictionary mapping rule names to counts
    """
    summary = {}
    for v in violations:
        rule = v["rule"]
        summary[rule] = summary.get(rule, 0) + 1
    return summary
