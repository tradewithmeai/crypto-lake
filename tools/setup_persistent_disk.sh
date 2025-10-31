#!/bin/bash
# Setup Persistent Disk for /data
#
# This script creates, attaches, formats, and mounts a persistent disk
# for the crypto-lake data directory. It preserves existing data during migration.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Permissions to create/attach disks and stop/start VMs
#
# Usage:
#   bash tools/setup_persistent_disk.sh

set -e  # Exit on error

# Configuration
VM_NAME="crypto-lake-vm"
ZONE="europe-west1-b"
PROJECT_ID="serious-conduit-476419-q7"
DISK_NAME="crypto-lake-data-disk"
DISK_SIZE="100GB"
DISK_TYPE="pd-standard"  # or pd-ssd for better performance
MOUNT_POINT="/data"
DEVICE_NAME="crypto-data"

echo "=========================================="
echo "Crypto Lake - Persistent Disk Setup"
echo "=========================================="
echo ""
echo "This script will set up a dedicated persistent disk for /data"
echo ""
echo "Configuration:"
echo "  VM:          $VM_NAME"
echo "  Zone:        $ZONE"
echo "  Project:     $PROJECT_ID"
echo "  Disk Name:   $DISK_NAME"
echo "  Disk Size:   $DISK_SIZE"
echo "  Disk Type:   $DISK_TYPE"
echo "  Mount Point: $MOUNT_POINT"
echo ""
echo "WARNING: This will:"
echo "  1. Stop the VM (collector will stop temporarily)"
echo "  2. Create and attach a new $DISK_SIZE persistent disk"
echo "  3. Format the disk as ext4"
echo "  4. Migrate existing /data contents to the new disk"
echo "  5. Update /etc/fstab for permanent mounting"
echo "  6. Restart the VM"
echo ""
echo "Estimated downtime: 3-5 minutes"
echo ""

# Prompt for confirmation
read -p "Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted by user."
    exit 0
fi

echo ""
echo "Step 1/8: Checking if disk already exists..."
DISK_EXISTS=$(gcloud compute disks list \
    --filter="name=$DISK_NAME AND zone:$ZONE" \
    --project=$PROJECT_ID \
    --format="value(name)" | wc -l)

if [ "$DISK_EXISTS" -gt 0 ]; then
    echo "Disk $DISK_NAME already exists in zone $ZONE"
    read -p "Use existing disk? (yes/no): " USE_EXISTING
    if [ "$USE_EXISTING" != "yes" ]; then
        echo "Aborted. Please delete the existing disk or choose a different name."
        exit 1
    fi
    SKIP_CREATE=true
else
    SKIP_CREATE=false
fi

echo ""
echo "Step 2/8: Stopping VM..."
gcloud compute instances stop $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID

echo "Waiting for VM to stop..."
sleep 10

if [ "$SKIP_CREATE" = false ]; then
    echo ""
    echo "Step 3/8: Creating persistent disk..."
    gcloud compute disks create $DISK_NAME \
        --size=$DISK_SIZE \
        --type=$DISK_TYPE \
        --zone=$ZONE \
        --project=$PROJECT_ID

    echo "Disk created successfully."
else
    echo ""
    echo "Step 3/8: Skipping disk creation (using existing disk)"
fi

echo ""
echo "Step 4/8: Attaching disk to VM..."
gcloud compute instances attach-disk $VM_NAME \
    --disk=$DISK_NAME \
    --device-name=$DEVICE_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID

echo "Disk attached successfully."

echo ""
echo "Step 5/8: Starting VM..."
gcloud compute instances start $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID

echo "Waiting for VM to start..."
sleep 20

# Wait for VM to be fully running and SSH-ready
MAX_WAIT=120
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    VM_STATUS=$(gcloud compute instances describe $VM_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --format="value(status)")

    if [ "$VM_STATUS" == "RUNNING" ]; then
        # Give SSH a few more seconds to be ready
        sleep 10
        break
    fi

    echo "Waiting for VM to start... ($ELAPSED/$MAX_WAIT seconds)"
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ "$VM_STATUS" != "RUNNING" ]; then
    echo "ERROR: VM did not start within $MAX_WAIT seconds. Current status: $VM_STATUS"
    exit 1
fi

echo ""
echo "Step 6/8: Formatting and mounting disk on VM..."
echo "This step will:"
echo "  - Format the new disk as ext4"
echo "  - Create temporary mount point"
echo "  - Copy existing /data contents to new disk"
echo "  - Update /etc/fstab"
echo "  - Remount /data to new disk"
echo ""

# Create setup commands to run on the VM
cat > /tmp/setup_disk_commands.sh <<'EOFVM'
#!/bin/bash
set -e

DEVICE="/dev/disk/by-id/google-crypto-data"
MOUNT_POINT="/data"
TEMP_MOUNT="/mnt/crypto-data-temp"

echo "Finding device..."
sudo ls -la /dev/disk/by-id/ | grep crypto-data || echo "Device not found yet, waiting..."
sleep 5

echo "Formatting disk as ext4..."
# Check if already formatted
if sudo blkid $DEVICE | grep -q ext4; then
    echo "Disk already formatted as ext4"
else
    sudo mkfs.ext4 -F $DEVICE
    echo "Disk formatted successfully"
fi

echo "Creating temporary mount point..."
sudo mkdir -p $TEMP_MOUNT

echo "Mounting new disk temporarily..."
sudo mount $DEVICE $TEMP_MOUNT

echo "Checking existing /data contents..."
EXISTING_SIZE=$(du -sh $MOUNT_POINT 2>/dev/null | cut -f1 || echo "0")
echo "Existing /data size: $EXISTING_SIZE"

if [ -d "$MOUNT_POINT" ] && [ "$(ls -A $MOUNT_POINT 2>/dev/null)" ]; then
    echo "Copying existing /data to new disk..."
    sudo rsync -av $MOUNT_POINT/ $TEMP_MOUNT/
    echo "Data copied successfully"

    echo "Backing up old /data to /data.old..."
    sudo mv $MOUNT_POINT $MOUNT_POINT.old
else
    echo "No existing /data or empty, skipping copy"
fi

echo "Creating new /data mount point..."
sudo mkdir -p $MOUNT_POINT

echo "Unmounting temporary mount..."
sudo umount $TEMP_MOUNT
sudo rmdir $TEMP_MOUNT

echo "Getting UUID of new disk..."
UUID=$(sudo blkid -s UUID -o value $DEVICE)
echo "Disk UUID: $UUID"

echo "Updating /etc/fstab..."
# Remove any existing /data entries
sudo sed -i.bak '/\/data/d' /etc/fstab

# Add new entry
echo "UUID=$UUID $MOUNT_POINT ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab

echo "Mounting /data from new disk..."
sudo mount -a

echo "Verifying mount..."
df -h $MOUNT_POINT

echo "Setting ownership..."
sudo chown -R Eschaton:Eschaton $MOUNT_POINT

echo "Disk setup complete!"
EOFVM

chmod +x /tmp/setup_disk_commands.sh

# Copy script to VM and execute
echo "Uploading setup script to VM..."
gcloud compute scp /tmp/setup_disk_commands.sh $VM_NAME:/tmp/ \
    --zone=$ZONE \
    --project=$PROJECT_ID

echo "Executing setup script on VM..."
gcloud compute ssh $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --command="bash /tmp/setup_disk_commands.sh"

# Clean up local temp file
rm /tmp/setup_disk_commands.sh

echo ""
echo "Step 7/8: Verifying disk setup..."
gcloud compute ssh $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --command="df -h /data && echo '' && ls -lah /data && echo '' && cat /etc/fstab | grep /data"

echo ""
echo "Step 8/8: Starting crypto-lake service..."
gcloud compute ssh $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --command="sudo systemctl start crypto-lake && sleep 5 && sudo systemctl status crypto-lake"

echo ""
echo "=========================================="
echo "Persistent Disk Setup Completed!"
echo "=========================================="
echo ""
echo "Summary:"
echo "  ✅ $DISK_SIZE persistent disk created and attached"
echo "  ✅ Disk formatted as ext4"
echo "  ✅ Existing /data contents migrated"
echo "  ✅ /etc/fstab updated for auto-mount on boot"
echo "  ✅ /data now mounted on dedicated disk"
echo "  ✅ crypto-lake service restarted"
echo ""
echo "Next steps:"
echo "  1. Verify collector is running:"
echo "     gcloud compute ssh $VM_NAME --zone=$ZONE --command='sudo systemctl status crypto-lake'"
echo ""
echo "  2. Check disk usage:"
echo "     gcloud compute ssh $VM_NAME --zone=$ZONE --command='df -h /data'"
echo ""
echo "  3. Monitor logs:"
echo "     gcloud compute ssh $VM_NAME --zone=$ZONE --command='tail -f /data/logs/health/heartbeat.json'"
echo ""
echo "  4. If everything looks good, delete old backup:"
echo "     gcloud compute ssh $VM_NAME --zone=$ZONE --command='sudo rm -rf /data.old'"
echo ""
echo "=========================================="
