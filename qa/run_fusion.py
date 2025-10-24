"""
Fusion Scoring Runner

CLI tool to compute fusion scores from schema violations and AI anomalies.

Usage:
    python -m qa.run_fusion --day 2025-10-23
    python -m qa.run_fusion --from 2025-10-21 --to 2025-10-23
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

from loguru import logger

from qa.config import load_qa_config
from qa.fusion import compute_fusion_scores, get_summary_stats, load_anomalies, load_violations
from qa.utils import (
    atomic_write_parquet,
    ensure_qa_directories,
    format_duration,
    get_qa_ai_path,
    get_qa_fusion_path,
    get_qa_schema_path,
    parse_date_args,
    parse_instant,
)
from tools.common import load_config, setup_logging


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


def main():
    parser = argparse.ArgumentParser(description="Run fusion scoring")
    parser.add_argument("--config", default="config.yml", help="Config file path")
    parser.add_argument("--from", dest="from_date",
                        help="Start timestamp (ISO-8601: 2025-10-24T00:00:00Z, or TODAY/YESTERDAY)")
    parser.add_argument("--to", dest="to_date",
                        help="End timestamp (ISO-8601, defaults to --from + 90m if omitted)")
    parser.add_argument("--day", help="Single day (YYYY-MM-DD, TODAY, or YESTERDAY)")
    parser.add_argument("--symbols", help="Comma-separated symbols (default: from config)")

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Set up logging
    base_path = config["general"]["base_path"]
    setup_logging("qa_fusion", config, test_mode=False)

    # Load QA config
    qa_config = load_qa_config(config)

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

    logger.info(f"Fusion scoring starting")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Symbols: {', '.join(symbols)}")

    start_time = time.time()

    # Process each day
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    total_symbols_processed = 0

    while current_date <= end_dt:
        date_str = current_date.strftime("%Y-%m-%d")
        logger.info(f"Processing {date_str}...")

        # Load violations and anomalies
        violations_path = get_qa_schema_path(base_path, date_str)
        anomalies_path = get_qa_ai_path(base_path, date_str)

        violations = load_violations(violations_path)
        anomalies = load_anomalies(anomalies_path)

        logger.info(f"Loaded {len(violations)} violations, {len(anomalies)} anomalies")

        # Check for empty inputs
        if len(violations) == 0 and len(anomalies) == 0:
            logger.info(f"No inputs for {date_str}; emitting empty fusion results with PASS by definition")

        # Compute fusion scores
        try:
            fusion_df = compute_fusion_scores(violations, anomalies, symbols, date_str)
        except Exception as e:
            logger.error(f"Fusion scoring failed for {date_str}: {e}")
            current_date = current_date.replace(day=current_date.day + 1)
            continue

        # Write fusion scores to Parquet
        output_path = get_qa_fusion_path(base_path, date_str)
        try:
            atomic_write_parquet(fusion_df, output_path)
            logger.info(f"Wrote fusion scores to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write fusion scores: {e}")
            sys.exit(1)

        # Log summary
        stats = get_summary_stats(fusion_df)
        logger.info(f"  PASS: {stats['pass_count']}, REVIEW: {stats['review_count']}, FAIL: {stats['fail_count']}")
        logger.info(f"  Avg score: {stats['avg_score']:.4f}")

        total_symbols_processed += len(fusion_df)

        # Move to next day
        current_date = current_date.replace(day=current_date.day + 1)

    duration = time.time() - start_time
    logger.info(f"Fusion scoring completed in {format_duration(duration)}")
    logger.info(f"Total symbols processed: {total_symbols_processed}")

    sys.exit(0)


if __name__ == "__main__":
    main()
