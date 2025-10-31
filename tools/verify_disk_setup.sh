#!/bin/bash
# Verify Persistent Disk Setup
#
# This script verifies that /data is properly mounted on a persistent disk
# and checks for common configuration issues.
#
# Usage:
#   bash tools/verify_disk_setup.sh

set -e

MOUNT_POINT="/data"
MIN_SIZE_GB=50

echo "=========================================="
echo "Crypto Lake - Disk Setup Verification"
echo "=========================================="
echo ""

# Check 1: Is /data a mount point?
echo "[1/6] Checking if $MOUNT_POINT is a mount point..."
if mountpoint -q $MOUNT_POINT; then
    echo "✅ PASS: $MOUNT_POINT is a mount point"
else
    echo "❌ FAIL: $MOUNT_POINT is NOT a mount point"
    echo "         /data is part of the root filesystem!"
    echo "         Run tools/setup_persistent_disk.sh to fix this."
    exit 1
fi

echo ""

# Check 2: What device is mounted?
echo "[2/6] Checking mounted device..."
DEVICE=$(df $MOUNT_POINT | tail -1 | awk '{print $1}')
echo "Device: $DEVICE"

if [[ $DEVICE == *"sda"* ]]; then
    echo "⚠️  WARNING: $MOUNT_POINT is mounted from boot disk ($DEVICE)"
    echo "             This is likely incorrect. Expected separate persistent disk."
    echo "             Run tools/setup_persistent_disk.sh to fix this."
    exit 1
elif [[ $DEVICE == *"sdb"* ]] || [[ $DEVICE == *"crypto-data"* ]]; then
    echo "✅ PASS: $MOUNT_POINT is on separate disk ($DEVICE)"
else
    echo "⚠️  UNKNOWN: Device $DEVICE - please verify manually"
fi

echo ""

# Check 3: Disk size
echo "[3/6] Checking disk size..."
TOTAL_GB=$(df -BG $MOUNT_POINT | tail -1 | awk '{print $2}' | sed 's/G//')
echo "Total size: ${TOTAL_GB}GB"

if [ $TOTAL_GB -lt $MIN_SIZE_GB ]; then
    echo "⚠️  WARNING: Disk size (${TOTAL_GB}GB) is less than recommended minimum (${MIN_SIZE_GB}GB)"
else
    echo "✅ PASS: Disk size is adequate"
fi

echo ""

# Check 4: Available space
echo "[4/6] Checking available space..."
AVAILABLE_GB=$(df -BG $MOUNT_POINT | tail -1 | awk '{print $4}' | sed 's/G//')
USED_PERCENT=$(df $MOUNT_POINT | tail -1 | awk '{print $5}' | sed 's/%//')
echo "Available: ${AVAILABLE_GB}GB (${USED_PERCENT}% used)"

if [ $USED_PERCENT -gt 90 ]; then
    echo "❌ CRITICAL: Disk is ${USED_PERCENT}% full!"
    echo "             Run tools/disk_cleanup.py or tools/emergency_cleanup.sh"
elif [ $USED_PERCENT -gt 80 ]; then
    echo "⚠️  WARNING: Disk is ${USED_PERCENT}% full"
    echo "             Consider running cleanup soon"
else
    echo "✅ PASS: Sufficient space available"
fi

echo ""

# Check 5: /etc/fstab entry
echo "[5/6] Checking /etc/fstab for permanent mount..."
if grep -q "$MOUNT_POINT" /etc/fstab; then
    echo "✅ PASS: /etc/fstab entry exists"
    echo "Entry:"
    grep "$MOUNT_POINT" /etc/fstab | sed 's/^/  /'
else
    echo "❌ FAIL: No /etc/fstab entry for $MOUNT_POINT"
    echo "         Disk will NOT auto-mount on reboot!"
    echo "         Add entry to /etc/fstab"
fi

echo ""

# Check 6: Ownership and permissions
echo "[6/6] Checking ownership and permissions..."
OWNER=$(stat -c '%U:%G' $MOUNT_POINT 2>/dev/null || stat -f '%Su:%Sg' $MOUNT_POINT 2>/dev/null)
PERMS=$(stat -c '%a' $MOUNT_POINT 2>/dev/null || stat -f '%Lp' $MOUNT_POINT 2>/dev/null)
echo "Owner: $OWNER"
echo "Permissions: $PERMS"

if [[ $OWNER == "Eschaton:Eschaton" ]] || [[ $OWNER == "root:root" ]]; then
    echo "✅ PASS: Ownership looks correct"
else
    echo "⚠️  WARNING: Unexpected ownership: $OWNER"
    echo "             Expected: Eschaton:Eschaton or root:root"
fi

echo ""
echo "=========================================="
echo "Verification Summary"
echo "=========================================="
echo ""
echo "Disk Information:"
df -h $MOUNT_POINT | grep -E "Filesystem|$MOUNT_POINT"
echo ""
echo "Mount Options:"
mount | grep "$MOUNT_POINT"
echo ""
echo "Directory Structure:"
ls -lah $MOUNT_POINT | head -10
echo ""
echo "=========================================="
echo "✅ Verification Complete"
echo "=========================================="
