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

from tools.common import ensure_dir, wait_for_parquet_files


def summarize_connection_events(base_path: str, day_str: str) -> Dict[str, Any]:
    """
    Read connection event logs and summarize gaps for each exchange.

    Args:
        base_path: Base path to data lake
        day_str: Date string in YYYY-MM-DD format

    Returns:
        Dictionary with per-exchange gap statistics
    """
    result = {}
    events_dir = os.path.join(base_path, "raw")

    # Scan all exchange directories for _events folders
    if not os.path.isdir(events_dir):
        return result

    for exchange_dir in os.listdir(events_dir):
        events_path = os.path.join(events_dir, exchange_dir, "_events", f"connections_{day_str}.jsonl")
        if not os.path.isfile(events_path):
            continue

        events = []
        try:
            with open(events_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
        except Exception:
            continue

        disconnects = [e for e in events if e.get("event") == "disconnected"]
        reconnects = [e for e in events if e.get("event") == "reconnecting"]
        total_gap_seconds = sum(e.get("gap_seconds", 0) for e in reconnects)

        result[exchange_dir] = {
            "disconnect_count": len(disconnects),
            "reconnect_count": len(reconnects),
            "total_gap_seconds": round(total_gap_seconds, 1),
            "last_event": events[-1].get("event", "") if events else "",
            "last_event_ts": events[-1].get("ts", "") if events else "",
        }

    return result


def check_disk_space(base_path: str) -> Dict[str, Any]:
    """
    Check disk usage for the data directory and root filesystem.

    Args:
        base_path: Base path to data lake

    Returns:
        Dictionary with disk usage statistics for both /data and / (root)
    """
    try:
        # Check /data disk usage
        data_stat = os.statvfs(base_path)
        data_total_bytes = data_stat.f_blocks * data_stat.f_frsize
        data_free_bytes = data_stat.f_bfree * data_stat.f_frsize
        data_used_bytes = data_total_bytes - data_free_bytes
        data_usage_percent = (data_used_bytes / data_total_bytes) * 100 if data_total_bytes > 0 else 0

        # Check root filesystem usage (for comparison)
        root_stat = os.statvfs("/")
        root_total_bytes = root_stat.f_blocks * root_stat.f_frsize
        root_free_bytes = root_stat.f_bfree * root_stat.f_frsize
        root_used_bytes = root_total_bytes - root_free_bytes
        root_usage_percent = (root_used_bytes / root_total_bytes) * 100 if root_total_bytes > 0 else 0

        # Determine alert level
        alert_level = "ok"
        alert_message = None

        if data_usage_percent >= 95:
            alert_level = "critical"
            alert_message = f"/data disk critically full ({data_usage_percent:.1f}%) - immediate cleanup required!"
        elif data_usage_percent >= 90:
            alert_level = "warning"
            alert_message = f"/data disk usage high ({data_usage_percent:.1f}%) - cleanup recommended"
        elif data_usage_percent >= 80:
            alert_level = "caution"
            alert_message = f"/data disk usage elevated ({data_usage_percent:.1f}%)"

        # Check if /data is on same filesystem as root (potential issue)
        data_is_separate = data_stat.f_fsid != root_stat.f_fsid

        return {
            "data_total_gb": data_total_bytes / 1e9,
            "data_used_gb": data_used_bytes / 1e9,
            "data_free_gb": data_free_bytes / 1e9,
            "data_usage_percent": data_usage_percent,
            "root_total_gb": root_total_bytes / 1e9,
            "root_used_gb": root_used_bytes / 1e9,
            "root_free_gb": root_free_bytes / 1e9,
            "root_usage_percent": root_usage_percent,
            "data_is_separate_disk": data_is_separate,
            "alert_level": alert_level,
            "alert_message": alert_message
        }

    except Exception as e:
        logger.warning(f"Failed to check disk space: {e}")
        return {
            "data_total_gb": 0,
            "data_used_gb": 0,
            "data_free_gb": 0,
            "data_usage_percent": 0,
            "root_total_gb": 0,
            "root_used_gb": 0,
            "root_free_gb": 0,
            "root_usage_percent": 0,
            "data_is_separate_disk": False,
            "alert_level": "unknown",
            "alert_message": "Failed to check disk space"
        }


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

            # Check if files exist before attempting to read
            if not wait_for_parquet_files(parquet_pattern, timeout=10, check_interval=2):
                logger.debug(f"Health check: No Parquet 1s files yet. System may be starting up.")
            else:
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
        except duckdb.IOException:
            logger.debug(f"No Parquet 1s files found, returning 0.")
        except Exception as e:
            logger.warning(f"Failed to count Parquet 1s rows: {e}")

        # Count macro minute rows for today using DuckDB
        macro_min_rows = 0
        try:
            # Check if macro directory exists
            macro_dir = os.path.join(base_path, "macro", "minute")
            if os.path.exists(macro_dir):
                macro_pattern = f"{base_path_normalized}/macro/minute/*/**.parquet"

                # Check if files exist before attempting to read
                if not wait_for_parquet_files(macro_pattern, timeout=10, check_interval=2):
                    logger.debug(f"Health check: No macro minute files yet. System may be starting up.")
                else:
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
        except duckdb.IOException:
            logger.debug(f"No macro minute files found, returning 0.")
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

## Connection Gaps (Today)

{_format_connection_gaps(payload.get('connection_events', {}))}

---

## Disk Space

{_format_disk_space_section(payload.get('disk', {}))}

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


def _format_connection_gaps(events_data: Dict[str, Any]) -> str:
    """Format connection gap data for markdown report."""
    if not events_data:
        return "No connection events recorded today (or events logging not yet active)."

    output = "| Exchange | Disconnects | Total Gap | Last Event |\n"
    output += "|----------|-------------|-----------|------------|\n"

    for exchange, stats in sorted(events_data.items()):
        gap_secs = stats.get("total_gap_seconds", 0)
        if gap_secs >= 3600:
            gap_str = f"{gap_secs/3600:.1f}h"
        elif gap_secs >= 60:
            gap_str = f"{gap_secs/60:.1f}m"
        else:
            gap_str = f"{gap_secs:.0f}s"

        last_event = stats.get("last_event", "")
        last_ts = stats.get("last_event_ts", "")
        if last_ts:
            last_ts = last_ts.split("T")[1][:8] if "T" in last_ts else last_ts

        output += f"| {exchange} | {stats.get('disconnect_count', 0)} | {gap_str} | {last_event} @ {last_ts} |\n"

    return output


def _format_disk_space_section(disk_data: Dict[str, Any]) -> str:
    """Format disk space section for markdown report."""
    if not disk_data:
        return "Disk space information not available."

    data_usage = disk_data.get('data_usage_percent', 0)
    data_used = disk_data.get('data_used_gb', 0)
    data_total = disk_data.get('data_total_gb', 0)
    data_free = disk_data.get('data_free_gb', 0)

    root_usage = disk_data.get('root_usage_percent', 0)
    root_used = disk_data.get('root_used_gb', 0)
    root_total = disk_data.get('root_total_gb', 0)

    is_separate = disk_data.get('data_is_separate_disk', False)
    alert_level = disk_data.get('alert_level', 'ok')
    alert_message = disk_data.get('alert_message')

    # Choose status icon based on alert level
    if alert_level == "critical":
        status_icon = "🔴 CRITICAL"
    elif alert_level == "warning":
        status_icon = "⚠️ WARNING"
    elif alert_level == "caution":
        status_icon = "⚡ CAUTION"
    else:
        status_icon = "✅ OK"

    disk_type = "Separate persistent disk" if is_separate else "⚠️ Same as root filesystem"

    output = f"""| Metric | Value |
|--------|-------|
| **Status** | {status_icon} |
| **/data Mount** | {disk_type} |
| **/data Usage** | {data_usage:.1f}% ({data_used:.1f} GB / {data_total:.1f} GB) |
| **/data Free** | {data_free:.1f} GB |
| **Root (/) Usage** | {root_usage:.1f}% ({root_used:.1f} GB / {root_total:.1f} GB) |
"""

    if alert_message:
        output += f"\n**Alert:** {alert_message}\n"

    if not is_separate:
        output += "\n⚠️ **WARNING:** /data is not on a separate persistent disk! Run `tools/setup_persistent_disk.sh` to fix.\n"

    return output
