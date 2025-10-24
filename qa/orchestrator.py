"""
QA Orchestrator

Separate orchestrator for QA pipeline with hourly and daily modes.

IMPORTANT: This is a SEPARATE orchestrator - it does NOT modify tools/orchestrator.py.

Modes:
    - hourly: Run QA on last N minutes of data (configured window)
    - daily: Run full-day QA at specified UTC time
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

from loguru import logger

from qa.config import load_qa_config
from qa.utils import ensure_qa_directories, format_duration
from tools.common import load_config, setup_logging


def run_qa_pipeline(config_path: str, date_str: str, tf: str = "1s") -> bool:
    """
    Run complete QA pipeline for a given date.

    Args:
        config_path: Path to config.yml
        date_str: Date to process (YYYY-MM-DD)
        tf: Timeframe (default: 1s)

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Running QA pipeline for {date_str}")

    # Step 1: Schema validation
    logger.info("Step 1/4: Schema validation")
    cmd_schema = [
        sys.executable, "-m", "qa.run_schema",
        "--config", config_path,
        "--day", date_str,
        "--tf", tf
    ]

    try:
        result = subprocess.run(cmd_schema, check=True, capture_output=True, text=True)
        logger.info("Schema validation completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Schema validation failed: {e.stderr}")
        return False

    # Step 2: AI detection
    logger.info("Step 2/4: AI detection")
    cmd_ai = [
        sys.executable, "-m", "qa.run_ai",
        "--config", config_path,
        "--day", date_str,
        "--tf", tf
    ]

    try:
        result = subprocess.run(cmd_ai, check=True, capture_output=True, text=True)
        logger.info("AI detection completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"AI detection failed: {e.stderr}")
        return False

    # Step 3: Fusion scoring
    logger.info("Step 3/4: Fusion scoring")
    cmd_fusion = [
        sys.executable, "-m", "qa.run_fusion",
        "--config", config_path,
        "--day", date_str
    ]

    try:
        result = subprocess.run(cmd_fusion, check=True, capture_output=True, text=True)
        logger.info("Fusion scoring completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Fusion scoring failed: {e.stderr}")
        return False

    # Step 4: Report generation
    logger.info("Step 4/4: Report generation")
    cmd_report = [
        sys.executable, "-m", "qa.run_report",
        "--config", config_path,
        "--day", date_str
    ]

    try:
        result = subprocess.run(cmd_report, check=True, capture_output=True, text=True)
        logger.info("Report generation completed")
    except subprocess.CalledProcessError as e:
        logger.error(f"Report generation failed: {e.stderr}")
        return False

    logger.info(f"QA pipeline completed successfully for {date_str}")
    return True


def mode_hourly(config_path: str, config: dict) -> int:
    """
    Run hourly QA on recent data.

    Processes data from last N minutes (configured window).

    Args:
        config_path: Path to config.yml
        config: Configuration dictionary

    Returns:
        Exit code (0 = success)
    """
    qa_config = load_qa_config(config)
    window_min = qa_config.get("hourly_window_min", 90)

    logger.info(f"Hourly QA mode: window = {window_min} minutes")

    # Determine date to process
    # For hourly mode, process "today" (current UTC date)
    now_utc = datetime.now(timezone.utc)
    date_str = now_utc.strftime("%Y-%m-%d")

    logger.info(f"Processing date: {date_str}")

    # Run pipeline
    success = run_qa_pipeline(config_path, date_str, tf="1s")

    return 0 if success else 1


def mode_daily(config_path: str, config: dict) -> int:
    """
    Run daily QA on full day.

    Processes complete day's data (typically yesterday).

    Args:
        config_path: Path to config.yml
        config: Configuration dictionary

    Returns:
        Exit code (0 = success)
    """
    logger.info("Daily QA mode")

    # Process yesterday (full day)
    now_utc = datetime.now(timezone.utc)
    yesterday = now_utc - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")

    logger.info(f"Processing date: {date_str}")

    # Run pipeline
    success = run_qa_pipeline(config_path, date_str, tf="1s")

    return 0 if success else 1


def mode_continuous(config_path: str, config: dict) -> int:
    """
    Run QA continuously (for testing/development).

    Runs hourly QA in a loop.

    Args:
        config_path: Path to config.yml
        config: Configuration dictionary

    Returns:
        Exit code (never returns in normal operation)
    """
    qa_config = load_qa_config(config)
    window_min = qa_config.get("hourly_window_min", 90)

    logger.info(f"Continuous QA mode: window = {window_min} minutes")
    logger.warning("Continuous mode is for testing only - use Windows Task Scheduler for production")

    while True:
        logger.info("=" * 60)
        logger.info("Starting QA cycle")

        start_time = time.time()

        # Run hourly QA
        mode_hourly(config_path, config)

        duration = time.time() - start_time
        logger.info(f"QA cycle completed in {format_duration(duration)}")

        # Sleep for configured interval (default: 1 hour)
        sleep_sec = window_min * 60
        logger.info(f"Sleeping for {window_min} minutes...")
        time.sleep(sleep_sec)


def main():
    parser = argparse.ArgumentParser(description="QA Orchestrator")
    parser.add_argument("--config", default="config.yml", help="Config file path")
    parser.add_argument(
        "--mode",
        choices=["hourly", "daily", "continuous"],
        required=True,
        help="Orchestration mode"
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Set up logging
    base_path = config["general"]["base_path"]
    setup_logging("qa_orchestrator", config, test_mode=False)

    # Ensure QA directories exist
    ensure_qa_directories(base_path)

    logger.info(f"QA Orchestrator starting in {args.mode} mode")

    # Run selected mode
    if args.mode == "hourly":
        exit_code = mode_hourly(args.config, config)
    elif args.mode == "daily":
        exit_code = mode_daily(args.config, config)
    elif args.mode == "continuous":
        exit_code = mode_continuous(args.config, config)
    else:
        logger.error(f"Invalid mode: {args.mode}")
        exit_code = 1

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
