"""
Health monitoring and metrics collection for Crypto Lake orchestrator.

Provides functions to:
- Summarize file counts and data volumes
- Write health metrics to JSON and Markdown
"""

import glob
import json
import os
from datetime import datetime
from typing import Any, Dict

import duckdb
from loguru import logger

from tools.common import ensure_dir


def summarize_files(base_path: str, day_str: str) -> Dict[str, int]:
    """
    Summarize file counts and data volumes for a given day.

    Args:
        base_path: Base path to data lake
        day_str: Date string in YYYY-MM-DD format

    Returns:
        Dictionary with:
            - raw_count_today: Number of raw JSONL files for today
            - parquet_1s_rows_today: Number of 1s bars in Parquet for today
            - macro_min_rows_today: Number of macro minute bars for today
    """
    try:
        # Normalize path for DuckDB (forward slashes)
        base_path_normalized = base_path.replace("\\", "/")

        # Parse date components
        year, month, day = day_str.split("-")

        # Count raw JSONL files for today
        raw_pattern = os.path.join(base_path, "raw", "binance", "*", day_str, "part_*.jsonl")
        raw_files = glob.glob(raw_pattern)
        raw_count = len(raw_files)

        # Count Parquet 1s rows for today using DuckDB
        parquet_1s_rows = 0
        try:
            parquet_pattern = f"{base_path_normalized}/parquet/binance/*/**.parquet"
            conn = duckdb.connect(":memory:")

            # Query with date filter
            query = f"""
            SELECT COUNT(*) as count
            FROM read_parquet('{parquet_pattern}')
            WHERE window_start >= TIMESTAMP '{day_str} 00:00:00'
              AND window_start < TIMESTAMP '{day_str} 23:59:59'
            """
            result = conn.execute(query).fetchone()
            if result:
                parquet_1s_rows = result[0]
            conn.close()
        except Exception as e:
            logger.warning(f"Failed to count Parquet 1s rows: {e}")

        # Count macro minute rows for today using DuckDB
        macro_min_rows = 0
        try:
            # Check if macro directory exists
            macro_dir = os.path.join(base_path, "macro", "minute")
            if os.path.exists(macro_dir):
                macro_pattern = f"{base_path_normalized}/macro/minute/*/**.parquet"
                conn = duckdb.connect(":memory:")

                # Query with date filter
                query = f"""
                SELECT COUNT(*) as count
                FROM read_parquet('{macro_pattern}')
                WHERE ts >= TIMESTAMP '{day_str} 00:00:00'
                  AND ts < TIMESTAMP '{day_str} 23:59:59'
                """
                result = conn.execute(query).fetchone()
                if result:
                    macro_min_rows = result[0]
                conn.close()
        except Exception as e:
            logger.warning(f"Failed to count macro minute rows: {e}")

        return {
            "raw_count_today": raw_count,
            "parquet_1s_rows_today": parquet_1s_rows,
            "macro_min_rows_today": macro_min_rows,
        }

    except Exception as e:
        logger.exception(f"Failed to summarize files: {e}")
        return {
            "raw_count_today": 0,
            "parquet_1s_rows_today": 0,
            "macro_min_rows_today": 0,
        }


def write_heartbeat(json_path: str, md_path: str, payload: Dict[str, Any], test_mode: bool = False):
    """
    Write health metrics to JSON and Markdown files.

    Args:
        json_path: Path to JSON heartbeat file
        md_path: Path to Markdown health report file
        payload: Health metrics payload dictionary
        test_mode: If True, label reports as TEST mode
    """
    try:
        # Add mode to payload
        payload["mode"] = "TEST" if test_mode else "PRODUCTION"

        # Ensure directories exist
        ensure_dir(os.path.dirname(json_path))
        ensure_dir(os.path.dirname(md_path))

        # Write JSON
        with open(json_path, "w") as f:
            json.dump(payload, f, indent=2)

        # Write Markdown
        _write_markdown_report(md_path, payload)

        logger.debug(f"Wrote heartbeat to {json_path} and {md_path}")

    except Exception as e:
        logger.exception(f"Failed to write heartbeat: {e}")


def _write_markdown_report(md_path: str, payload: Dict[str, Any]):
    """
    Write health metrics to a Markdown file.

    Args:
        md_path: Path to Markdown file
        payload: Health metrics payload
    """
    collector = payload.get("collector", {})
    macro = payload.get("macro_minute", {})
    files = payload.get("files", {})
    ts_utc = payload.get("ts_utc", "Unknown")

    # Determine overall status
    collector_status = collector.get("status", "unknown")
    macro_status = macro.get("status", "unknown")

    if collector_status == "running" and (macro_status in ["idle", "running"]):
        overall_status = "HEALTHY"
        status_icon = "[OK]"
    elif collector_status == "error" or macro_status == "error":
        overall_status = "ERROR"
        status_icon = "[ERROR]"
    elif collector_status == "stopped":
        overall_status = "STOPPED"
        status_icon = "[STOPPED]"
    else:
        overall_status = "UNKNOWN"
        status_icon = "[UNKNOWN]"

    # Get mode
    mode = payload.get("mode", "PRODUCTION")

    # Build Markdown content
    md_content = f"""# Crypto Lake Health Report

**MODE:** {mode}

**Generated:** {ts_utc}

**Overall Status:** {status_icon} {overall_status}

---

## Real-Time Crypto Collector

| Metric | Value |
|--------|-------|
| Status | {collector_status.upper()} |
| Last Seen | {collector.get('last_seen_ts', 'N/A')} |
| Latency P50 | {collector.get('last_latency_p50_ms', 0):.1f} ms |
| Latency P95 | {collector.get('last_latency_p95_ms', 0):.1f} ms |

{_format_collector_status_text(collector_status)}

---

## Macro/FX Data Fetcher

| Metric | Value |
|--------|-------|
| Status | {macro_status.upper()} |
| Last Run Start | {macro.get('last_run_start', 'N/A')} |
| Last Run End | {macro.get('last_run_end', 'N/A')} |
| Last Run Rows | {macro.get('last_run_rows_written', 0):,} |
| Last Error | {macro.get('last_error') or 'None'} |

{_format_macro_status_text(macro_status, macro)}

---

## Data Volume (Today)

| Metric | Count |
|--------|-------|
| Raw JSONL Files | {files.get('raw_count_today', 0):,} |
| Parquet 1s Bars | {files.get('parquet_1s_rows_today', 0):,} |
| Macro Minute Bars | {files.get('macro_min_rows_today', 0):,} |

---

## Notes

- Health metrics are updated every 60 seconds
- Raw JSONL files rotate every 60 seconds
- Macro data is fetched on a schedule (typically every 15 minutes)
- Press Ctrl+C to stop the orchestrator gracefully

---

*Generated by Crypto Lake Orchestrator*
"""

    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md_content)


def _format_collector_status_text(status: str) -> str:
    """Format status text for collector."""
    if status == "running":
        return "The WebSocket collector is actively streaming data from Binance."
    elif status == "stopped":
        return "The WebSocket collector has been stopped."
    elif status == "error":
        return "WARNING: The WebSocket collector encountered an error. Check logs for details."
    else:
        return "Status unknown."


def _format_macro_status_text(status: str, macro_data: Dict[str, Any]) -> str:
    """Format status text for macro fetcher."""
    if status == "running":
        return "Currently fetching macro/FX data from yfinance..."
    elif status == "idle":
        last_run_end = macro_data.get("last_run_end")
        if last_run_end:
            return f"Macro fetcher is idle. Last fetch completed at {last_run_end}."
        else:
            return "Macro fetcher is idle, waiting for first scheduled run."
    elif status == "stopped":
        return "The macro fetcher has been stopped."
    elif status == "error":
        return f"WARNING: The macro fetcher encountered an error: {macro_data.get('last_error', 'Unknown')}"
    else:
        return "Status unknown."
