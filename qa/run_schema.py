"""
Schema Validation Runner

CLI tool to run schema validation and output violations in JSONL format.

Usage:
    python -m qa.run_schema --day 2025-10-23
    python -m qa.run_schema --from 2025-10-21 --to 2025-10-23
"""

import argparse
import sys
import time
from datetime import datetime, timedelta, timezone

from loguru import logger

from qa.config import load_qa_config
from qa.schema_validator import get_violation_summary, validate_schema
from qa.utils import (
    atomic_write_jsonl,
    ensure_qa_directories,
    format_duration,
    get_qa_schema_path,
    parse_date_args,
    parse_instant,
)
from tools.common import load_config, setup_logging
from tools.db import connect_and_register_views
from tools.validate_rules import fetch_data


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
    parser = argparse.ArgumentParser(description="Run schema validation")
    parser.add_argument("--config", default="config.yml", help="Config file path")
    parser.add_argument("--from", dest="from_date",
                        help="Start timestamp (ISO-8601: 2025-10-24T00:00:00Z, or TODAY/YESTERDAY)")
    parser.add_argument("--to", dest="to_date",
                        help="End timestamp (ISO-8601, defaults to --from + 90m if omitted)")
    parser.add_argument("--day", help="Single day (YYYY-MM-DD, TODAY, or YESTERDAY)")
    parser.add_argument("--tf", default="1s", choices=["1s", "1m"], help="Timeframe")
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
    setup_logging("qa_schema", config, test_mode=False)

    # Load QA config with defaults
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

    # Get symbols to validate
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",")]
    else:
        # Default to all symbols from first exchange in config
        exchanges = config.get("exchanges", [])
        if not exchanges:
            logger.error("No exchanges configured")
            sys.exit(1)
        symbols = exchanges[0].get("symbols", [])

    if not symbols:
        logger.error("No symbols to validate")
        sys.exit(1)

    logger.info(f"Schema validation starting")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Timeframe: {args.tf}")
    logger.info(f"Symbols: {', '.join(symbols)}")

    start_time = time.time()

    # Connect to DuckDB and register views
    try:
        conn = connect_and_register_views(base_path)
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        sys.exit(1)

    total_violations = 0

    # Process each day in range
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    while current_date <= end_dt:
        date_str = current_date.strftime("%Y-%m-%d")
        logger.info(f"Processing {date_str}...")

        # Fetch data for this day
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
            # Write empty JSONL file
            output_path = get_qa_schema_path(base_path, date_str)
            atomic_write_jsonl([], output_path)
            logger.info(f"Wrote empty violations file: {output_path}")
            current_date = current_date.replace(day=current_date.day + 1)
            continue

        logger.info(f"Loaded {len(df)} rows for {date_str}")

        # Run schema validation
        try:
            violations = validate_schema(df, conn, symbols, start_ts, end_ts, args.tf)
        except Exception as e:
            logger.error(f"Schema validation failed for {date_str}: {e}")
            current_date = current_date.replace(day=current_date.day + 1)
            continue

        # Write violations to JSONL
        output_path = get_qa_schema_path(base_path, date_str)
        try:
            atomic_write_jsonl(violations, output_path)
            logger.info(f"Wrote {len(violations)} violations to {output_path}")
        except Exception as e:
            logger.error(f"Failed to write violations: {e}")
            sys.exit(1)

        # Log summary
        if violations:
            summary = get_violation_summary(violations)
            for rule, count in summary.items():
                logger.info(f"  {rule}: {count} violations")

        total_violations += len(violations)

        # Move to next day
        current_date = current_date.replace(day=current_date.day + 1)

    # Close connection
    conn.close()

    duration = time.time() - start_time
    logger.info(f"Schema validation completed in {format_duration(duration)}")
    logger.info(f"Total violations: {total_violations}")

    # Exit 0 even if violations found (violations are data issues, not runner errors)
    sys.exit(0)


if __name__ == "__main__":
    main()
