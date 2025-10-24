"""
AI Anomaly Detection Runner

CLI tool to run AI/statistical anomaly detection and output results in JSONL format.

Usage:
    python -m qa.run_ai --day 2025-10-23
    python -m qa.run_ai --from 2025-10-21 --to 2025-10-23
"""

import argparse
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

import pandas as pd
from loguru import logger

from qa.ai.detectors import IsolationForestDetector, JumpDetector, ZScoreDetector
from qa.ai.labeler import create_labeler
from qa.config import load_qa_config
from qa.utils import (
    atomic_write_jsonl,
    ensure_qa_directories,
    format_duration,
    get_qa_ai_path,
    get_qa_schema_path,
    parse_date_args,
    parse_instant,
    setup_qa_logging,
)
from tools.common import load_config, setup_logging
from tools.db import connect_and_register_views
from tools.validate_rules import fetch_data

# Verify scikit-learn is installed (required for AI detectors)
try:
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
except ImportError:
    logger.error("scikit-learn not installed. Run: pip install -r requirements-qa.txt")
    sys.exit(1)


def resolve_day(token: str) -> str:
    """
    Resolve TODAY/YESTERDAY tokens to YYYY-MM-DD format.

    Args:
        token: Date string (YYYY-MM-DD, TODAY, or YESTERDAY)

    Returns:
        Date in YYYY-MM-DD format
    """
    if not token:
        return datetime.now(timezone.utc).date().isoformat()
    t = token.strip().upper()
    if t == "TODAY":
        return datetime.now(timezone.utc).date().isoformat()
    if t == "YESTERDAY":
        return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    return token  # Pass through YYYY-MM-DD


def cluster_anomalies(records: list, cooldown_seconds: int = 5) -> list:
    """
    Cluster anomalies within a cooldown window per (symbol, detector).

    Reduces bursty duplicates by merging anomalies that occur within
    cooldown_seconds of each other for the same symbol+detector pair.

    Args:
        records: List of anomaly dictionaries (must have symbol, ts, detector keys)
        cooldown_seconds: Window in seconds to cluster anomalies

    Returns:
        List of clustered anomaly dictionaries with optional "cluster" field
    """
    if not records or cooldown_seconds <= 0:
        return records

    # Sort by symbol, detector, then timestamp
    sorted_records = sorted(records, key=lambda r: (r.get("symbol", ""), r.get("detector", ""), r.get("ts", "")))

    clustered = []
    bucket = []
    last_ts = None
    last_key = None

    for rec in sorted_records:
        symbol = rec.get("symbol", "")
        detector = rec.get("detector", "")
        ts_str = rec.get("ts", "")
        key = (symbol, detector)

        # Parse timestamp
        try:
            ts = pd.Timestamp(ts_str, tz="UTC")
        except:
            # Invalid timestamp, emit as-is
            clustered.append(rec)
            continue

        # Check if same group and within cooldown
        if bucket and key == last_key and last_ts and (ts - last_ts).total_seconds() <= cooldown_seconds:
            bucket.append(rec)
        else:
            # Flush previous bucket
            if bucket:
                clustered.append(_flush_bucket(bucket))
            bucket = [rec]
            last_key = key

        last_ts = ts

    # Flush final bucket
    if bucket:
        clustered.append(_flush_bucket(bucket))

    return clustered


def _flush_bucket(bucket: list) -> dict:
    """
    Flush a bucket of anomalies into a single clustered record.

    Keeps the first record's structure and adds a "cluster" field
    with metadata about the cluster.

    Args:
        bucket: List of anomaly records to merge

    Returns:
        Clustered anomaly dictionary
    """
    if len(bucket) == 1:
        return bucket[0]

    # Use first record as base
    rec = dict(bucket[0])

    # Compute cluster metadata
    z_scores = []
    for b in bucket:
        features = b.get("features", {})
        z_score = features.get("z_score")
        if z_score is not None:
            z_scores.append(abs(z_score))

    rec["cluster"] = {
        "count": len(bucket),
        "start_ts": bucket[0].get("ts"),
        "end_ts": bucket[-1].get("ts"),
        "max_abs_z": max(z_scores) if z_scores else None
    }

    return rec


def load_schema_violations(base_path: str, date_str: str) -> set:
    """
    Load schema violations to filter clean data.

    Returns set of (symbol, ts) tuples with violations.
    """
    violations_path = get_qa_schema_path(base_path, date_str)

    if not os.path.exists(violations_path):
        logger.warning(f"Schema violations file not found: {violations_path}")
        return set()

    violation_keys = set()

    try:
        with open(violations_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    viol = json.loads(line)
                    violation_keys.add((viol["symbol"], viol["ts"]))
    except Exception as e:
        logger.error(f"Failed to load schema violations: {e}")
        return set()

    logger.debug(f"Loaded {len(violation_keys)} schema violations")
    return violation_keys


def create_clean_mask(df: pd.DataFrame, violation_keys: set) -> pd.Series:
    """
    Create boolean mask for clean rows (no schema violations).

    Args:
        df: DataFrame to mask
        violation_keys: Set of (symbol, ts) tuples with violations

    Returns:
        Boolean series (True = clean, False = has violation)
    """
    if not violation_keys:
        return pd.Series(True, index=df.index)

    # Create mask
    mask = pd.Series(True, index=df.index)

    for idx, row in df.iterrows():
        ts_str = row["ts"].isoformat()
        if (row["symbol"], ts_str) in violation_keys:
            mask.loc[idx] = False

    return mask


def main():
    parser = argparse.ArgumentParser(description="Run AI anomaly detection")
    parser.add_argument("--config", default="config.yml", help="Config file path")
    parser.add_argument("--from", dest="from_date",
                        help="Start timestamp (ISO-8601: 2025-10-24T00:00:00Z, or TODAY/YESTERDAY)")
    parser.add_argument("--to", dest="to_date",
                        help="End timestamp (ISO-8601, defaults to --from + 90m if omitted)")
    parser.add_argument("--day", help="Single day (YYYY-MM-DD, TODAY, or YESTERDAY)")
    parser.add_argument("--tf", default="1s", choices=["1s", "1m"], help="Timeframe")
    parser.add_argument("--symbols", help="Comma-separated symbols (default: from config)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                        help="Logging level (default: INFO)")
    parser.add_argument("--print-anomalies", type=int, default=0, metavar="N",
                        help="Print up to N example anomaly records to stdout (default: 0 = quiet)")
    parser.add_argument("--print-detector-breakdown", action="store_true",
                        help="Print detector and label breakdown to stdout")

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Set up logging
    base_path = config["general"]["base_path"]
    setup_qa_logging(base_path, "qa_ai", args.log_level)

    # Load QA config with defaults
    qa_config = load_qa_config(config)

    # Check if AI is enabled
    if not qa_config.get("enable_ai", True):
        logger.warning("AI detection is disabled in config")
        sys.exit(0)

    # Ensure QA directories exist
    ensure_qa_directories(base_path)

    # Parse date/time arguments with ISO-8601 support
    try:
        if args.day and not (args.from_date or args.to_date):
            # Day mode: process full day 00:00:00Z to 23:59:59Z
            day_str = resolve_day(args.day)
            start_ts = parse_instant(day_str + "T00:00:00Z")
            end_ts = parse_instant(day_str + "T23:59:59Z")
            start_date = day_str
            end_date = day_str
        else:
            # Timestamp mode: support ISO-8601 timestamps
            if not args.from_date:
                logger.error("--from is required when --day is not used")
                sys.exit(2)

            start_ts = parse_instant(args.from_date)
            if args.to_date:
                end_ts = parse_instant(args.to_date)
            else:
                # Default: from + 90 minutes
                end_ts = start_ts + timedelta(minutes=90)
                logger.info(f"--to not specified, defaulting to --from + 90 minutes")

            start_date = start_ts.date().isoformat()
            end_date = end_ts.date().isoformat()

        logger.info(f"Resolved time window: {start_ts.isoformat()} to {end_ts.isoformat()}")
    except ValueError as e:
        logger.error(f"Invalid date/time arguments: {e}")
        sys.exit(1)

    # Get symbols
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    else:
        exchanges = config.get("exchanges", [])
        if not exchanges:
            logger.error("No exchanges configured")
            sys.exit(1)
        symbols = exchanges[0].get("symbols", [])

    if not symbols:
        logger.error("No symbols to process")
        sys.exit(1)

    logger.info(f"AI anomaly detection starting")
    logger.debug(f"Date range: {start_date} to {end_date}")
    logger.debug(f"Timeframe: {args.tf}")
    logger.debug(f"Symbols: {', '.join(symbols)}")

    start_time = time.time()

    # Connect to DuckDB
    try:
        conn = connect_and_register_views(base_path)
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)

    # Initialize detectors
    zscore_detector = ZScoreDetector(
        window=qa_config["zscore"]["window"],
        k=qa_config["zscore"]["k"]
    )
    jump_detector = JumpDetector(
        k_sigma=qa_config["jump"]["k_sigma"],
        spread_stable_bps=qa_config["jump"]["spread_stable_bps"],
        min_trade_count=qa_config["jump"].get("min_trade_count", 0)
    )
    iforest_detector = IsolationForestDetector(
        n_estimators=qa_config["iforest"]["n_estimators"],
        contamination=qa_config["iforest"]["contamination"],
        random_state=qa_config["iforest"]["random_state"]
    )

    # Get clustering config
    cooldown_seconds = qa_config["jump"].get("cooldown_seconds", 0)

    # Initialize labeler
    labeler = create_labeler(qa_config["ai_labeler"])

    total_anomalies = 0

    # Process each day
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_dt:
        date_str = current_date.strftime("%Y-%m-%d")
        logger.debug(f"Processing {date_str}...")

        # Fetch data
        start_ts = f"{date_str}T00:00:00Z"
        end_ts = f"{date_str}T23:59:59Z"

        try:
            df = fetch_data(conn, symbols, start_ts, end_ts, args.tf, "bars")
        except Exception as e:
            logger.error(f"Failed to fetch data for {date_str}: {e}")
            current_date = current_date.replace(day=current_date.day + 1)
            continue

        if df.empty:
            logger.warning(f"No data found for {date_str}")
            output_path = get_qa_ai_path(base_path, date_str)
            atomic_write_jsonl([], output_path)
            logger.info(f"Wrote empty anomalies file: {output_path}")
            current_date = current_date.replace(day=current_date.day + 1)
            continue

        logger.info(f"Loaded {len(df)} rows for {date_str}")

        # Load schema violations to create clean mask
        violation_keys = load_schema_violations(base_path, date_str)
        clean_mask = create_clean_mask(df, violation_keys)
        logger.debug(f"Clean rows: {clean_mask.sum()} / {len(df)}")

        # Run detectors
        all_anomalies = []

        try:
            zscore_anomalies = zscore_detector.detect(df, clean_mask)
            all_anomalies.extend(zscore_anomalies)
            logger.debug(f"ZScore detector found {len(zscore_anomalies)} anomalies")
        except Exception as e:
            logger.error(f"ZScore detector failed: {e}")

        try:
            jump_anomalies = jump_detector.detect(df, clean_mask)
            all_anomalies.extend(jump_anomalies)
            logger.debug(f"Jump detector found {len(jump_anomalies)} anomalies")
        except Exception as e:
            logger.error(f"Jump detector failed: {e}")

        try:
            iforest_anomalies = iforest_detector.detect(df, clean_mask)
            all_anomalies.extend(iforest_anomalies)
            logger.debug(f"IsolationForest detector found {len(iforest_anomalies)} anomalies")
        except Exception as e:
            logger.error(f"IsolationForest detector failed: {e}")

        # Label anomalies
        if all_anomalies:
            all_anomalies = labeler.label(all_anomalies)

        # Track raw count before clustering
        raw_count = len(all_anomalies)

        # Apply clustering to JUMP anomalies
        if all_anomalies and cooldown_seconds > 0:
            # Separate JUMP from other detectors
            jump_anomalies = [a for a in all_anomalies if a.get("detector") == "JUMP"]
            other_anomalies = [a for a in all_anomalies if a.get("detector") != "JUMP"]

            # Cluster JUMP anomalies
            if jump_anomalies:
                clustered_jump = cluster_anomalies(jump_anomalies, cooldown_seconds)
                logger.debug(f"Clustered {len(jump_anomalies)} JUMP anomalies into {len(clustered_jump)} clusters")
                all_anomalies = clustered_jump + other_anomalies
            else:
                all_anomalies = other_anomalies

        # Write clustered anomalies to JSONL
        output_path = get_qa_ai_path(base_path, date_str)
        try:
            atomic_write_jsonl(all_anomalies, output_path)
            if raw_count != len(all_anomalies):
                logger.info(f"Wrote {len(all_anomalies)} clustered anomalies ({raw_count} raw) to {output_path}")
            else:
                logger.info(f"Wrote {len(all_anomalies)} anomalies to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write anomalies: {e}")
            sys.exit(1)

        # Build summary statistics
        if all_anomalies:
            by_detector = Counter(a.get("detector", "unknown") for a in all_anomalies)
            by_label = Counter(a.get("label", "unknown") for a in all_anomalies)

            # Print detector breakdown if requested
            if args.print_detector_breakdown:
                detector_str = ", ".join(f"{k}={v}" for k, v in sorted(by_detector.items()))
                label_str = ", ".join(f"{k}={v}" for k, v in sorted(by_label.items()))
                print(f"Detectors: {detector_str} | Labels: {label_str}")

            # Print preview anomalies if requested
            if args.print_anomalies > 0:
                preview_count = min(args.print_anomalies, len(all_anomalies))
                print(f"\n--- Preview: first {preview_count} anomalies ---")

                # Apply burst de-dup for preview (within 2s window per symbol+detector)
                seen_keys = {}  # (symbol, detector) -> last_ts
                printed = 0

                for anomaly in all_anomalies:
                    if printed >= preview_count:
                        break

                    symbol = anomaly.get("symbol", "")
                    detector = anomaly.get("detector", "")
                    ts_str = anomaly.get("ts", "")
                    key = (symbol, detector)

                    # Parse timestamp for de-dup check
                    try:
                        ts = parse_instant(ts_str) if ts_str else None
                    except:
                        ts = None

                    # Check if within 2s of last printed for same symbol+detector
                    if key in seen_keys and ts:
                        last_ts = seen_keys[key]
                        if (ts - last_ts).total_seconds() < 2.0:
                            continue  # Skip duplicate burst

                    # Print this anomaly
                    print(json.dumps(anomaly, ensure_ascii=False))
                    printed += 1

                    if ts:
                        seen_keys[key] = ts

                print(f"--- End preview ({printed} shown) ---\n")

        total_anomalies += len(all_anomalies)

        # Move to next day
        current_date = current_date.replace(day=current_date.day + 1)

    # Close connection
    conn.close()

    duration = time.time() - start_time
    logger.info(f"AI anomaly detection completed in {format_duration(duration)}")
    logger.info(f"Total anomalies: {total_anomalies}")

    sys.exit(0)


if __name__ == "__main__":
    main()
