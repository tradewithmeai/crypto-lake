"""
Disk Cleanup Script

Automatically removes old raw JSONL files to prevent disk space exhaustion.
Designed for production use with cron scheduling.

Usage:
    python -m tools.disk_cleanup [--retention-days 7] [--dry-run]

Cron Schedule:
    0 2 * * * cd /home/Eschaton/crypto-lake && /home/Eschaton/crypto-lake/venv/bin/python -m tools.disk_cleanup >> /data/logs/qa/cleanup.log 2>&1
"""

import argparse
import glob
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from loguru import logger

from tools.common import load_config
from tools.logging_setup import setup_logging

setup_logging()


def cleanup_old_raw_files(base_path: str, retention_days: int = 7, dry_run: bool = False) -> dict:
    """
    Delete raw JSONL files older than retention_days.

    Args:
        base_path: Base data directory path
        retention_days: Number of days to retain files (default: 7)
        dry_run: If True, log what would be deleted without actually deleting

    Returns:
        dict: Statistics about cleanup operation
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    logger.info(f"Cleanup starting: retention={retention_days} days, cutoff={cutoff.isoformat()}, dry_run={dry_run}")

    # Find all raw JSONL files
    pattern = os.path.join(base_path, "raw", "**", "*.jsonl")
    files = glob.glob(pattern, recursive=True)

    logger.info(f"Found {len(files)} raw JSONL files total")

    deleted_count = 0
    deleted_size = 0
    kept_count = 0
    error_count = 0

    for file_path in files:
        try:
            # Get file modification time
            mtime = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)

            if mtime < cutoff:
                # File is older than retention period - delete it
                size = os.path.getsize(file_path)

                if dry_run:
                    logger.debug(f"[DRY RUN] Would delete: {file_path} ({size / 1e6:.2f} MB, age: {(datetime.now(timezone.utc) - mtime).days} days)")
                else:
                    os.remove(file_path)
                    logger.debug(f"Deleted: {file_path} ({size / 1e6:.2f} MB)")

                deleted_count += 1
                deleted_size += size
            else:
                kept_count += 1

        except FileNotFoundError:
            # File was deleted between glob and processing - not an error
            logger.debug(f"File already deleted: {file_path}")
        except PermissionError as e:
            logger.error(f"Permission denied: {file_path} - {e}")
            error_count += 1
        except Exception as e:
            logger.error(f"Failed to process {file_path}: {e}")
            error_count += 1

    # Remove empty directories
    if not dry_run:
        _cleanup_empty_directories(os.path.join(base_path, "raw"))

    # Log summary
    action = "Would delete" if dry_run else "Deleted"
    logger.info(
        f"Cleanup complete: {action} {deleted_count} files ({deleted_size / 1e9:.2f} GB), "
        f"kept {kept_count} files, {error_count} errors"
    )

    return {
        "deleted_count": deleted_count,
        "deleted_size_gb": deleted_size / 1e9,
        "kept_count": kept_count,
        "error_count": error_count,
        "retention_days": retention_days,
        "cutoff_date": cutoff.isoformat(),
        "dry_run": dry_run
    }


def _cleanup_empty_directories(root_path: str):
    """
    Remove empty directories under root_path.

    Args:
        root_path: Root directory to scan
    """
    removed_count = 0

    # Walk bottom-up so we can remove empty leaf directories first
    for dirpath, dirnames, filenames in os.walk(root_path, topdown=False):
        # Skip the root directory itself
        if dirpath == root_path:
            continue

        try:
            # Check if directory is empty
            if not os.listdir(dirpath):
                os.rmdir(dirpath)
                logger.debug(f"Removed empty directory: {dirpath}")
                removed_count += 1
        except OSError as e:
            logger.debug(f"Could not remove directory {dirpath}: {e}")

    if removed_count > 0:
        logger.info(f"Removed {removed_count} empty directories")


def check_disk_usage(base_path: str) -> dict:
    """
    Check disk usage for the data directory.

    Args:
        base_path: Base data directory path

    Returns:
        dict: Disk usage statistics
    """
    stat = os.statvfs(base_path)

    total_bytes = stat.f_blocks * stat.f_frsize
    free_bytes = stat.f_bfree * stat.f_frsize
    used_bytes = total_bytes - free_bytes
    usage_percent = (used_bytes / total_bytes) * 100

    stats = {
        "total_gb": total_bytes / 1e9,
        "used_gb": used_bytes / 1e9,
        "free_gb": free_bytes / 1e9,
        "usage_percent": usage_percent
    }

    logger.info(
        f"Disk usage: {stats['used_gb']:.1f} GB used / {stats['total_gb']:.1f} GB total "
        f"({stats['usage_percent']:.1f}%), {stats['free_gb']:.1f} GB free"
    )

    # Warn if disk usage is high
    if usage_percent > 80:
        logger.warning(f"Disk usage above 80%: {usage_percent:.1f}%")
    if usage_percent > 90:
        logger.error(f"Disk usage critical above 90%: {usage_percent:.1f}%")

    return stats


def main():
    parser = argparse.ArgumentParser(description="Disk cleanup for crypto-lake data")
    parser.add_argument(
        "--config",
        default="config.yml",
        help="Path to config.yml (default: config.yml)"
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=7,
        help="Number of days to retain files (default: 7)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting"
    )

    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config)
        base_path = config["general"]["base_path"]

        logger.info(f"Starting disk cleanup: base_path={base_path}")

        # Check disk usage before cleanup
        logger.info("=== Disk Usage Before Cleanup ===")
        disk_before = check_disk_usage(base_path)

        # Run cleanup
        logger.info("=== Running Cleanup ===")
        stats = cleanup_old_raw_files(base_path, args.retention_days, args.dry_run)

        # Check disk usage after cleanup
        if not args.dry_run:
            logger.info("=== Disk Usage After Cleanup ===")
            disk_after = check_disk_usage(base_path)
            freed_gb = disk_after["free_gb"] - disk_before["free_gb"]
            logger.info(f"Freed {freed_gb:.2f} GB of disk space")

        logger.info("Disk cleanup completed successfully")

    except Exception as e:
        logger.exception(f"Disk cleanup failed: {e}")
        raise


if __name__ == "__main__":
    main()
