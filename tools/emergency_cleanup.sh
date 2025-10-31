#!/bin/bash
# Emergency Disk Cleanup
#
# This script performs aggressive cleanup when disk space is critically low.
# Use this as a temporary measure before setting up persistent disk.
#
# Usage:
#   bash tools/emergency_cleanup.sh [--retention-days N] [--dry-run]

set -e

# Default settings
RETENTION_DAYS=3
DRY_RUN=false
BASE_PATH="/data"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --retention-days)
            RETENTION_DAYS="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--retention-days N] [--dry-run]"
            exit 1
            ;;
    esac
done

echo "=========================================="
echo "Emergency Disk Cleanup"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Base Path:      $BASE_PATH"
echo "  Retention:      $RETENTION_DAYS days"
echo "  Dry Run:        $DRY_RUN"
echo ""

# Show current disk usage
echo "Current Disk Usage:"
df -h / | grep -E "Filesystem|/$"
df -h $BASE_PATH 2>/dev/null | grep -E "Filesystem|$BASE_PATH" || echo "$BASE_PATH not separately mounted"
echo ""

# Calculate cutoff date
CUTOFF_DATE=$(date -u -d "$RETENTION_DAYS days ago" '+%Y-%m-%d')
echo "Deleting raw JSONL files older than: $CUTOFF_DATE"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo "DRY RUN MODE - No files will be deleted"
    echo ""
fi

# Find and delete old raw JSONL files
TOTAL_SIZE=0
FILE_COUNT=0

echo "Scanning for old raw JSONL files..."
while IFS= read -r file; do
    SIZE=$(stat -f%z "$file" 2>/dev/null || stat -c%s "$file" 2>/dev/null || echo 0)
    TOTAL_SIZE=$((TOTAL_SIZE + SIZE))
    FILE_COUNT=$((FILE_COUNT + 1))

    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] Would delete: $file ($(numfmt --to=iec $SIZE 2>/dev/null || echo $SIZE bytes))"
    else
        echo "Deleting: $file"
        rm -f "$file"
    fi
done < <(find $BASE_PATH/raw -type f -name "*.jsonl" ! -newermt "$CUTOFF_DATE" 2>/dev/null || true)

echo ""
echo "Summary:"
echo "  Files: $FILE_COUNT"
echo "  Total Size: $(numfmt --to=iec $TOTAL_SIZE 2>/dev/null || echo $TOTAL_SIZE bytes)"

if [ "$DRY_RUN" = true ]; then
    echo "  Action: NONE (dry run)"
else
    echo "  Action: DELETED"
fi

# Clean up empty directories
if [ "$DRY_RUN" = false ]; then
    echo ""
    echo "Cleaning up empty directories..."
    find $BASE_PATH/raw -type d -empty -delete 2>/dev/null || true
fi

# Show disk usage after cleanup
echo ""
echo "Disk Usage After Cleanup:"
df -h / | grep -E "Filesystem|/$"
df -h $BASE_PATH 2>/dev/null | grep -E "Filesystem|$BASE_PATH" || echo "$BASE_PATH not separately mounted"

echo ""
echo "=========================================="
echo "Cleanup Complete"
echo "=========================================="

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "This was a DRY RUN. To actually delete files, run without --dry-run flag."
fi
