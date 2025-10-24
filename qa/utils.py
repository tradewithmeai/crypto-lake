"""
QA Utilities

Provides atomic file writing, path helpers, and common utilities.
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

# ISO-8601 timestamp formats supported
ISO_FORMATS = [
    "%Y-%m-%dT%H:%M:%S%z",     # 2025-10-24T00:00:00+00:00
    "%Y-%m-%d %H:%M:%S%z",     # 2025-10-24 00:00:00+00:00
    "%Y-%m-%dT%H:%M:%S",       # 2025-10-24T00:00:00 (naive -> assume UTC)
    "%Y-%m-%d %H:%M:%S",       # 2025-10-24 00:00:00 (naive -> assume UTC)
    "%Y-%m-%d",                # Date only (YYYY-MM-DD)
]


def parse_instant(s: str) -> Optional[datetime]:
    """
    Parse ISO-8601 timestamp or TODAY/YESTERDAY tokens.

    Supports:
    - ISO-8601: 2025-10-24T00:00:00Z, 2025-10-24T00:00:00+00:00
    - Naive timestamps (assumes UTC): 2025-10-24T00:00:00
    - Date only: 2025-10-24 (returns midnight UTC)
    - Tokens: TODAY, YESTERDAY (returns midnight UTC)

    Args:
        s: Timestamp string

    Returns:
        datetime object in UTC, or None if input is None/empty

    Raises:
        ValueError: If format is not recognised
    """
    if s is None or s == "":
        return None

    s = s.strip()

    # Handle TODAY/YESTERDAY tokens
    if s.upper() in ("TODAY", "YESTERDAY"):
        base = datetime.now(timezone.utc).date()
        day = base if s.upper() == "TODAY" else (base - timedelta(days=1))
        return datetime(day.year, day.month, day.day, tzinfo=timezone.utc)

    # Normalise 'Z' suffix to +00:00 for %z parsing
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    # Try each format
    for fmt in ISO_FORMATS:
        try:
            dt = datetime.strptime(s, fmt)
            # If naive, assume UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    raise ValueError(f"Unrecognised datetime format: {s}")


def atomic_write_jsonl(records: List[Dict[str, Any]], path: str) -> None:
    """
    Atomically write JSONL file using temp file + os.replace().

    Args:
        records: List of dictionaries to write (one per line)
        path: Final output path
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + '.tmp'

    try:
        with open(tmp_path, 'w', encoding='utf-8', newline='\n') as f:
            for record in records:
                json.dump(record, f, ensure_ascii=False)
                f.write('\n')
        os.replace(tmp_path, path)
        logger.debug(f"Atomically wrote {len(records)} records to {path}")
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e


def atomic_write_parquet(df: pd.DataFrame, path: str, compression: str = 'snappy') -> None:
    """
    Atomically write Parquet file using temp file + os.replace().

    Args:
        df: DataFrame to write
        path: Final output path
        compression: Compression codec (default: snappy)
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + '.tmp'

    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, tmp_path, compression=compression)
        os.replace(tmp_path, path)
        logger.debug(f"Atomically wrote {len(df)} rows to {path}")
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e


def atomic_write_text(content: str, path: str) -> None:
    """
    Atomically write text file using temp file + os.replace().

    Args:
        content: Text content to write
        path: Final output path
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = path + '.tmp'

    try:
        with open(tmp_path, 'w', encoding='utf-8', newline='\n') as f:
            f.write(content)
        os.replace(tmp_path, path)
        logger.debug(f"Atomically wrote text to {path}")
    except Exception as e:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise e


def ensure_qa_directories(base_path: str) -> None:
    """
    Ensure all QA output directories exist.

    Args:
        base_path: Base data lake path (e.g., D:/CryptoDataLake)
    """
    dirs = [
        os.path.join(base_path, "qa", "schema"),
        os.path.join(base_path, "qa", "ai"),
        os.path.join(base_path, "qa", "fusion"),
        os.path.join(base_path, "reports", "qa"),
        os.path.join(base_path, "logs", "qa"),
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
    logger.debug(f"Ensured QA directories exist under {base_path}")


def get_qa_schema_path(base_path: str, date_str: str) -> str:
    """Get path for schema violations JSONL file."""
    return os.path.join(base_path, "qa", "schema", f"{date_str}_violations.jsonl")


def get_qa_ai_path(base_path: str, date_str: str) -> str:
    """Get path for AI anomalies JSONL file."""
    return os.path.join(base_path, "qa", "ai", f"{date_str}_anomalies.jsonl")


def get_qa_fusion_path(base_path: str, date_str: str) -> str:
    """Get path for fusion scores Parquet file."""
    return os.path.join(base_path, "qa", "fusion", f"{date_str}_fusion.parquet")


def get_qa_report_path(base_path: str, date_str: str) -> str:
    """Get path for daily QA report."""
    return os.path.join(base_path, "reports", "qa", f"{date_str}_qa_report.md")


def to_iso8601_utc(dt: datetime) -> str:
    """
    Convert datetime to ISO8601 UTC string.

    Args:
        dt: Datetime object (will be converted to UTC if naive)

    Returns:
        ISO8601 string with UTC timezone
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def parse_date_args(from_date: str = None, to_date: str = None, day: str = None) -> tuple:
    """
    Parse CLI date arguments (--from, --to, --day).

    Args:
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD)
        day: Single day (YYYY-MM-DD)

    Returns:
        (start_date_str, end_date_str) tuple

    Raises:
        ValueError: If date arguments are invalid
    """
    if day:
        if from_date or to_date:
            raise ValueError("Cannot use --day with --from or --to")
        return (day, day)

    if not from_date:
        from_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    if not to_date:
        to_date = from_date

    return (from_date, to_date)


def format_duration(seconds: float) -> str:
    """
    Format duration in seconds to human-readable string.

    Args:
        seconds: Duration in seconds

    Returns:
        Formatted string (e.g., "2m 34s", "45s")
    """
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    return f"{minutes}m {secs:.1f}s"


def setup_qa_logging(base_path: str, log_name: str, log_level: str = "INFO") -> None:
    """
    Set up QA logging with rotating file handler and minimal stdout.

    Args:
        base_path: Base data lake path
        log_name: Log file name (e.g., "qa_ai")
        log_level: Logging level (INFO, DEBUG, WARNING, ERROR)
    """
    # Ensure logs/qa directory exists
    logs_dir = os.path.join(base_path, "logs", "qa")
    os.makedirs(logs_dir, exist_ok=True)

    log_path = os.path.join(logs_dir, f"{log_name}.log")
    level = log_level.upper()

    # Remove all existing handlers
    logger.remove()

    # Add rotating file handler (5MB x 3 backups) with DEBUG+
    logger.add(
        log_path,
        rotation="5 MB",
        retention=3,
        level="DEBUG",
        backtrace=True,
        diagnose=False,
        enqueue=False,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    # Add stdout handler with minimal format (respects log_level)
    logger.add(
        sys.stdout,
        level=level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    )
