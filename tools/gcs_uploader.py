"""
GCS Uploader Script

Uploads Parquet files to Google Cloud Storage for backup and durability.
Designed for production use with cron scheduling.

Usage:
    python tools/gcs_uploader.py [--dry-run] [--force]

Prerequisites:
    - google-cloud-storage package installed
    - Service account credentials configured (GOOGLE_APPLICATION_CREDENTIALS env var)
    - GCS bucket name set in config.yml

Cron Schedule:
    0 3 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/gcs_uploader.py >> /data/logs/qa/gcs-upload.log 2>&1
"""

import argparse
import glob
import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

try:
    from google.cloud import storage
    from google.cloud.exceptions import GoogleCloudError
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False
    logger.warning("google-cloud-storage not installed. Install with: pip install google-cloud-storage")

from tools.common import load_config
from tools.logging_setup import setup_logging

setup_logging()


def compute_md5(file_path: str) -> str:
    """
    Compute MD5 hash of a file.

    Args:
        file_path: Path to file

    Returns:
        str: MD5 hash as hex string
    """
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            md5.update(chunk)
    return md5.hexdigest()


def upload_parquet_to_gcs(
    base_path: str,
    bucket_name: str,
    exclude_current_day: bool = True,
    dry_run: bool = False,
    force: bool = False
) -> dict:
    """
    Upload Parquet files to GCS bucket.

    Args:
        base_path: Base data directory path
        bucket_name: GCS bucket name
        exclude_current_day: If True, skip files from current UTC date (still being written)
        dry_run: If True, show what would be uploaded without uploading
        force: If True, re-upload files even if they exist with same size

    Returns:
        dict: Statistics about upload operation
    """
    if not GCS_AVAILABLE:
        logger.error("google-cloud-storage package not installed")
        return {"error": "google-cloud-storage not installed"}

    logger.info(f"GCS upload starting: bucket={bucket_name}, exclude_current_day={exclude_current_day}, dry_run={dry_run}, force={force}")

    try:
        # Initialize GCS client
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # Verify bucket exists
        if not dry_run:
            if not bucket.exists():
                logger.error(f"GCS bucket does not exist: {bucket_name}")
                return {"error": f"Bucket {bucket_name} not found"}
            logger.info(f"Connected to GCS bucket: {bucket_name}")

    except GoogleCloudError as e:
        error_msg = str(e)

        # Check for scope-related errors
        if "403" in error_msg or "Forbidden" in error_msg or "scope" in error_msg.lower():
            logger.error("=" * 80)
            logger.error("GCS AUTHORIZATION ERROR: Insufficient OAuth Scopes")
            logger.error("=" * 80)
            logger.error(f"Error: {error_msg}")
            logger.error("")
            logger.error("ROOT CAUSE: The VM's service account has correct IAM permissions")
            logger.error("but lacks the required OAuth scopes for GCS write operations.")
            logger.error("")
            logger.error("SOLUTION: Update VM scopes to include 'storage-rw' or 'cloud-platform'")
            logger.error("")
            logger.error("To fix this issue, run the following commands:")
            logger.error("  1. gcloud compute instances stop crypto-lake-vm --zone=europe-west1-b")
            logger.error("  2. gcloud compute instances set-service-account crypto-lake-vm \\")
            logger.error("       --zone=europe-west1-b \\")
            logger.error("       --scopes=storage-rw,logging-write,monitoring-write")
            logger.error("  3. gcloud compute instances start crypto-lake-vm --zone=europe-west1-b")
            logger.error("")
            logger.error("Or use the convenience script: bash tools/fix_vm_scopes.sh")
            logger.error("=" * 80)
            return {"error": "Insufficient OAuth scopes - see logs for fix instructions"}
        else:
            logger.error(f"Failed to connect to GCS: {e}")
            return {"error": str(e)}
    except Exception as e:
        logger.error(f"Failed to initialize GCS client: {e}")
        return {"error": str(e)}

    # Get current date for exclusion
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"Current UTC date: {today} (will be excluded from upload)")

    # Find all Parquet files
    pattern = os.path.join(base_path, "parquet", "**", "*.parquet")
    files = glob.glob(pattern, recursive=True)
    logger.info(f"Found {len(files)} total Parquet files")

    uploaded_count = 0
    skipped_count = 0
    excluded_count = 0
    error_count = 0
    uploaded_size = 0

    for local_path in files:
        try:
            # Check if file is from current day
            if exclude_current_day and today in local_path:
                logger.debug(f"Excluded (current day): {local_path}")
                excluded_count += 1
                continue

            # Generate GCS path relative to base_path
            rel_path = os.path.relpath(local_path, base_path)
            # Normalize path separators for GCS (always use forward slash)
            gcs_path = rel_path.replace("\\", "/")

            # Get file size
            local_size = os.path.getsize(local_path)

            if dry_run:
                logger.debug(f"[DRY RUN] Would upload: {gcs_path} ({local_size / 1e6:.2f} MB)")
                uploaded_count += 1
                uploaded_size += local_size
                continue

            # Check if blob already exists
            blob = bucket.blob(gcs_path)
            if blob.exists() and not force:
                # Compare sizes
                if blob.size == local_size:
                    logger.debug(f"Skipped (already uploaded): {gcs_path}")
                    skipped_count += 1
                    continue
                else:
                    logger.info(f"Size mismatch - re-uploading: {gcs_path} (local: {local_size}, remote: {blob.size})")

            # Upload file
            logger.info(f"Uploading: {gcs_path} ({local_size / 1e6:.2f} MB)")
            blob.upload_from_filename(local_path)

            # Verify upload
            blob.reload()
            if blob.size == local_size:
                logger.info(f"Upload verified: {gcs_path}")
                uploaded_count += 1
                uploaded_size += local_size
            else:
                logger.error(f"Upload verification failed: {gcs_path} (local: {local_size}, remote: {blob.size})")
                error_count += 1

        except GoogleCloudError as e:
            error_msg = str(e)

            # Check for scope-related errors during upload
            if "403" in error_msg or "Forbidden" in error_msg or "scope" in error_msg.lower():
                logger.error(f"GCS SCOPE ERROR uploading {local_path}: {error_msg}")
                logger.error("VM lacks 'storage-rw' OAuth scope. Run: bash tools/fix_vm_scopes.sh")
                error_count += 1
                # Stop trying if we hit scope errors
                if error_count >= 3:
                    logger.error("Multiple scope errors detected. Stopping upload.")
                    logger.error("Fix VM scopes before retrying: see logs above for instructions")
                    break
            else:
                logger.error(f"GCS error uploading {local_path}: {e}")
                error_count += 1
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            error_count += 1

    # Log summary
    action = "Would upload" if dry_run else "Uploaded"
    logger.info(
        f"GCS upload complete: {action} {uploaded_count} files ({uploaded_size / 1e9:.2f} GB), "
        f"skipped {skipped_count}, excluded {excluded_count}, {error_count} errors"
    )

    return {
        "uploaded_count": uploaded_count,
        "uploaded_size_gb": uploaded_size / 1e9,
        "skipped_count": skipped_count,
        "excluded_count": excluded_count,
        "error_count": error_count,
        "bucket_name": bucket_name,
        "dry_run": dry_run,
        "today_excluded": today if exclude_current_day else None
    }


def list_gcs_files(bucket_name: str, prefix: str = "parquet/") -> dict:
    """
    List files in GCS bucket for verification.

    Args:
        bucket_name: GCS bucket name
        prefix: Prefix to filter files (default: "parquet/")

    Returns:
        dict: Statistics about GCS bucket contents
    """
    if not GCS_AVAILABLE:
        logger.error("google-cloud-storage package not installed")
        return {"error": "google-cloud-storage not installed"}

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        blobs = list(bucket.list_blobs(prefix=prefix))
        total_size = sum(blob.size for blob in blobs)

        logger.info(f"GCS bucket contains {len(blobs)} files ({total_size / 1e9:.2f} GB) under prefix '{prefix}'")

        return {
            "file_count": len(blobs),
            "total_size_gb": total_size / 1e9,
            "bucket_name": bucket_name,
            "prefix": prefix
        }

    except GoogleCloudError as e:
        logger.error(f"Failed to list GCS bucket: {e}")
        return {"error": str(e)}
    except Exception as e:
        logger.error(f"Failed to query GCS: {e}")
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="GCS uploader for crypto-lake Parquet files")
    parser.add_argument(
        "--config",
        default="config.yml",
        help="Path to config.yml (default: config.yml)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be uploaded without actually uploading"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-upload files even if they exist in GCS"
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only list files in GCS bucket, don't upload"
    )
    parser.add_argument(
        "--include-today",
        action="store_true",
        help="Include current day files (normally excluded as they're still being written)"
    )

    args = parser.parse_args()

    try:
        # Load configuration
        config = load_config(args.config)
        base_path = config["general"]["base_path"]
        bucket_name = config.get("gcs", {}).get("bucket_name", "")

        if not bucket_name:
            logger.error("GCS bucket_name not configured in config.yml under 'gcs.bucket_name'")
            logger.error("Please set: gcs.bucket_name: 'your-bucket-name'")
            return 1

        logger.info(f"Starting GCS operations: base_path={base_path}, bucket={bucket_name}")

        # List-only mode
        if args.list_only:
            logger.info("=== GCS Bucket Contents ===")
            stats = list_gcs_files(bucket_name)
            if "error" not in stats:
                logger.info(f"Success: {stats['file_count']} files, {stats['total_size_gb']:.2f} GB")
            return 0

        # Upload mode
        logger.info("=== Running GCS Upload ===")
        stats = upload_parquet_to_gcs(
            base_path,
            bucket_name,
            exclude_current_day=not args.include_today,
            dry_run=args.dry_run,
            force=args.force
        )

        if "error" in stats:
            logger.error(f"Upload failed: {stats['error']}")
            return 1

        logger.info("GCS upload completed successfully")
        return 0

    except Exception as e:
        logger.exception(f"GCS upload failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
