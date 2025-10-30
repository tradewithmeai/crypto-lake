#!/bin/bash
# Fix VM OAuth Scopes for GCS Access
#
# This script updates the crypto-lake-vm instance to include the required
# OAuth scopes for Google Cloud Storage write operations.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - Permissions to stop/start VMs in the project
#
# Usage:
#   bash tools/fix_vm_scopes.sh

set -e  # Exit on error

# Configuration
VM_NAME="crypto-lake-vm"
ZONE="europe-west1-b"
PROJECT_ID="serious-conduit-476419-q7"
REQUIRED_SCOPES="storage-rw,logging-write,monitoring-write"

echo "=========================================="
echo "Crypto Lake - VM OAuth Scope Fix"
echo "=========================================="
echo ""
echo "This script will update the VM's OAuth scopes to enable GCS write access."
echo ""
echo "VM:      $VM_NAME"
echo "Zone:    $ZONE"
echo "Project: $PROJECT_ID"
echo "Scopes:  $REQUIRED_SCOPES"
echo ""
echo "WARNING: This will stop and restart the VM, causing ~30-60 seconds of downtime."
echo ""

# Prompt for confirmation
read -p "Continue? (yes/no): " CONFIRM
if [ "$CONFIRM" != "yes" ]; then
    echo "Aborted by user."
    exit 0
fi

echo ""
echo "Step 1/4: Checking VM status..."
VM_STATUS=$(gcloud compute instances describe $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --format="value(status)")

echo "Current VM status: $VM_STATUS"

if [ "$VM_STATUS" != "RUNNING" ]; then
    echo "WARNING: VM is not running. Current status: $VM_STATUS"
    read -p "Continue anyway? (yes/no): " CONTINUE
    if [ "$CONTINUE" != "yes" ]; then
        echo "Aborted by user."
        exit 0
    fi
fi

echo ""
echo "Step 2/4: Stopping VM..."
gcloud compute instances stop $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID

echo "Waiting for VM to stop..."
sleep 5

echo ""
echo "Step 3/4: Updating OAuth scopes..."
gcloud compute instances set-service-account $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --scopes=$REQUIRED_SCOPES

echo ""
echo "Step 4/4: Starting VM..."
gcloud compute instances start $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID

echo "Waiting for VM to start..."
sleep 10

# Wait for VM to be fully running
MAX_WAIT=60
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    VM_STATUS=$(gcloud compute instances describe $VM_NAME \
        --zone=$ZONE \
        --project=$PROJECT_ID \
        --format="value(status)")

    if [ "$VM_STATUS" == "RUNNING" ]; then
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
echo "=========================================="
echo "Verification"
echo "=========================================="
echo ""

# Verify scopes
echo "Verifying OAuth scopes..."
CURRENT_SCOPES=$(gcloud compute instances describe $VM_NAME \
    --zone=$ZONE \
    --project=$PROJECT_ID \
    --format="value(serviceAccounts[0].scopes)")

echo "Current scopes:"
echo "$CURRENT_SCOPES" | tr ',' '\n' | sed 's/^/  - /'

# Check if storage-rw scope is present
if echo "$CURRENT_SCOPES" | grep -q "storage-rw\|cloud-platform"; then
    echo ""
    echo "✅ SUCCESS: VM now has GCS write access via OAuth scopes."
    echo ""
    echo "Next steps:"
    echo "  1. SSH to the VM:"
    echo "     gcloud compute ssh $VM_NAME --zone=$ZONE"
    echo ""
    echo "  2. Test GCS upload:"
    echo "     cd ~/crypto-lake"
    echo "     source venv/bin/activate"
    echo "     python tools/gcs_uploader.py --dry-run"
    echo ""
    echo "  3. Run actual upload:"
    echo "     python tools/gcs_uploader.py"
    echo ""
else
    echo ""
    echo "❌ WARNING: storage-rw or cloud-platform scope not found."
    echo "This may indicate the scope update failed."
    echo ""
    echo "To manually verify scopes, run:"
    echo "  gcloud compute instances describe $VM_NAME --zone=$ZONE --format='value(serviceAccounts[0].scopes)'"
    exit 1
fi

echo "=========================================="
echo "Scope Fix Completed Successfully"
echo "=========================================="
