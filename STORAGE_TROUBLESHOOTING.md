# Storage Troubleshooting Guide - Crypto Lake

This guide covers common storage issues and their solutions for the crypto-lake data pipeline.

---

## Table of Contents

1. [Disk Full Errors](#disk-full-errors)
2. [/data Not on Persistent Disk](#data-not-on-persistent-disk)
3. [Disk Usage Growing Too Fast](#disk-usage-growing-too-fast)
4. [Failed to Write Logs](#failed-to-write-logs)
5. [GCS Upload Failures](#gcs-upload-failures)
6. [Data Recovery](#data-recovery)

---

## Disk Full Errors

### Symptoms

```
OSError: [Errno 28] No space left on device
```

- Logs stop mid-execution
- Collector fails to write JSONL files
- Transformer fails to create Parquet files
- Cron jobs fail

### Diagnosis

```bash
# Check disk usage
df -h /
df -h /data

# Check which directories are consuming space
du -sh /data/*

# Check if /data is mounted separately
mountpoint /data
```

### Solutions

#### Immediate Emergency Cleanup

```bash
# Run emergency cleanup (deletes files older than 3 days)
cd ~/crypto-lake
bash tools/emergency_cleanup.sh --retention-days 3

# Or run with dry-run first to see what would be deleted
bash tools/emergency_cleanup.sh --retention-days 3 --dry-run
```

#### Long-term Fix: Setup Persistent Disk

```bash
# From your local machine
cd crypto-lake
bash tools/setup_persistent_disk.sh
```

See [PHASE1_DEPLOYMENT_GUIDE.md Step -1](PHASE1_DEPLOYMENT_GUIDE.md#step--1-setup-persistent-disk-for-data-30-minutes) for detailed instructions.

---

## /data Not on Persistent Disk

### Symptoms

- Disk fills up quickly (< 20GB total)
- `/data` shows same device as `/` (root)
- `mountpoint /data` returns "is not a mountpoint"

### Diagnosis

```bash
# Check if /data is a mount point
mountpoint /data

# Check what device /data is on
df /data

# If you see /dev/sda1, /data is on the root disk (INCORRECT)
# If you see /dev/sdb or similar, /data is on separate disk (CORRECT)
```

### Solution

This is a **critical infrastructure issue**. The system is writing all data to the 20GB boot disk instead of a dedicated persistent disk.

**Action Required:** Run the persistent disk setup immediately:

```bash
# From local machine
bash tools/setup_persistent_disk.sh
```

This will:
1. Create a 100GB persistent disk
2. Migrate existing `/data` contents
3. Mount the new disk to `/data`
4. Configure auto-mount on reboot

**Estimated Time:** 5 minutes (including 1-2 min VM downtime)

---

## Disk Usage Growing Too Fast

### Expected Growth Rates

| Component | Size | Retention |
|-----------|------|-----------|
| Raw JSONL | ~1-2 GB/day/symbol | 7 days (auto-cleanup) |
| Parquet 1s | ~100-200 MB/day/symbol | Permanent (upload to GCS) |
| Logs | ~10-50 MB/day | 14 days (auto-rotation) |

**For 13 symbols:**
- Raw: ~15-25 GB/day
- Parquet: ~1.5-2.5 GB/day
- Total: ~17-28 GB/day

### Diagnosis

```bash
# Check disk usage by directory
du -sh /data/*

# Check number of raw files
find /data/raw -name "*.jsonl" | wc -l

# Check oldest raw files
find /data/raw -name "*.jsonl" -type f -printf '%T+ %p\n' | sort | head -20
```

### Solutions

#### Enable Automated Cleanup

The disk cleanup script should run daily via cron:

```bash
# Check if cron job exists
crontab -l | grep disk_cleanup

# If missing, add it
crontab -e
# Add this line:
0 2 * * * cd /home/Eschaton/crypto-lake && LOG_LEVEL=INFO /home/Eschaton/crypto-lake/venv/bin/python -m tools.disk_cleanup >> /data/logs/qa/cleanup.log 2>&1
```

#### Manual Cleanup

```bash
# Run cleanup manually (dry-run first)
cd ~/crypto-lake
source venv/bin/activate
python -m tools.disk_cleanup --dry-run

# Actually delete old files
python -m tools.disk_cleanup

# Or use emergency cleanup for more aggressive deletion
bash tools/emergency_cleanup.sh --retention-days 2
```

#### Adaptive Retention

The disk cleanup script automatically adjusts retention based on disk usage:

- **< 80% full:** 7-day retention (default)
- **80-90% full:** 3-day retention
- **90-95% full:** 2-day retention
- **> 95% full:** 1-day retention

To disable adaptive behavior:
```bash
python -m tools.disk_cleanup --no-adaptive --retention-days 7
```

---

## Failed to Write Logs

### Symptoms

```
Failed to write heartbeat: [Errno 28] No space left on device
Permission denied: /data/logs/qa/...
```

### Diagnosis

```bash
# Check disk space
df -h /data

# Check log directory permissions
ls -la /data/logs

# Check log directory ownership
stat /data/logs/qa
```

### Solutions

#### Disk Full

See [Disk Full Errors](#disk-full-errors) above.

#### Permission Issues

```bash
# Fix ownership
sudo chown -R Eschaton:Eschaton /data

# Fix permissions
sudo chmod -R 755 /data/logs
```

#### Log Rotation Not Working

```bash
# Check loguru rotation settings in tools/logging_setup.py
# Default: 10 MB rotation, 5-file retention

# Manually clean up old logs if needed
find /data/logs -name "*.log.*" -mtime +14 -delete
```

---

## GCS Upload Failures

### Symptoms

```
403 Provided scope(s) are not authorized
Failed to upload to GCS
```

### Diagnosis

```bash
# Check OAuth scopes
gcloud compute instances describe crypto-lake-vm \
  --zone=europe-west1-b \
  --format="value(serviceAccounts[0].scopes)"

# Should include: storage-rw or cloud-platform
```

### Solution

See [PHASE1_DEPLOYMENT_GUIDE.md Step 0](PHASE1_DEPLOYMENT_GUIDE.md#step-0-verify-and-fix-vm-oauth-scopes-10-minutes) for OAuth scope setup.

```bash
# Quick fix from local machine
bash tools/fix_vm_scopes.sh
```

---

## Data Recovery

### Recover from Disk Full Situation

If the system filled up and stopped writing data:

1. **Free up space immediately:**
   ```bash
   bash tools/emergency_cleanup.sh --retention-days 1
   ```

2. **Check for partial/corrupt files:**
   ```bash
   # Find files modified in last hour (likely corrupt)
   find /data -type f -mmin -60

   # Check Parquet files for corruption
   python -c "import pandas as pd; pd.read_parquet('/path/to/file.parquet')"
   ```

3. **Restart collector:**
   ```bash
   sudo systemctl restart crypto-lake
   sudo systemctl status crypto-lake
   ```

4. **Backfill missing data** (if needed):
   ```bash
   # Use Binance REST API to backfill gaps
   python main.py --mode backfill_binance --symbols SOLUSDT --date 2025-10-30 --interval 1m
   ```

### Recover from /data on Wrong Disk

If you've been running with `/data` on the root disk:

1. **Stop the service:**
   ```bash
   sudo systemctl stop crypto-lake
   ```

2. **Setup persistent disk:**
   ```bash
   # From local machine
   bash tools/setup_persistent_disk.sh
   ```

   This will automatically migrate existing `/data` contents.

3. **Verify migration:**
   ```bash
   # SSH to VM
   df -h /data  # Should show /dev/sdb or similar
   ls -lah /data  # Should show all existing directories
   ```

4. **Remove old backup** (after verification):
   ```bash
   sudo rm -rf /data.old
   ```

---

## Monitoring & Alerts

### Check Disk Space in Health Report

```bash
# View health report
cat /data/reports/health.md

# Should include Disk Space section with:
# - /data usage percentage
# - /data free space
# - Whether /data is on separate disk
# - Alert level (OK / CAUTION / WARNING / CRITICAL)
```

### Setup Alerts

The health monitoring system now includes disk space alerts:

- **OK:** < 80% used
- **CAUTION:** 80-90% used
- **WARNING:** 90-95% used
- **CRITICAL:** > 95% used

Alerts appear in:
- `/data/reports/health.md`
- `/data/logs/health/heartbeat.json`
- Console logs (if LOG_LEVEL=INFO or DEBUG)

---

## Prevention

### Checklist for Production Deployment

- [ ] `/data` is on persistent disk (100GB minimum)
- [ ] Verify: `mountpoint /data` returns "is a mountpoint"
- [ ] Verify: `df /data` shows `/dev/sdb` or similar (NOT `/dev/sda`)
- [ ] Disk cleanup cron job configured and running
- [ ] GCS upload cron job configured for backup
- [ ] Health monitoring shows "Separate persistent disk"
- [ ] Alert level is "OK" in health report

### Regular Maintenance

**Daily:**
- Check health report: `cat /data/reports/health.md`
- Monitor disk usage alerts

**Weekly:**
- Verify cleanup ran successfully: `cat /data/logs/qa/cleanup.log`
- Verify GCS uploads: `gsutil du -sh gs://crypto-lake-data/`
- Check for any errors: `grep -i error /data/logs/qa/*.log`

**Monthly:**
- Review total data volume growth
- Verify Parquet files are being compacted daily
- Test data recovery procedure

---

## Quick Reference

### Emergency Commands

```bash
# Free up space NOW
bash tools/emergency_cleanup.sh --retention-days 1

# Check disk usage
df -h /data

# Verify disk setup
bash tools/verify_disk_setup.sh

# Restart collector
sudo systemctl restart crypto-lake

# Check logs
tail -f /data/logs/qa/*.log
```

### Useful Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `tools/setup_persistent_disk.sh` | Setup persistent disk | `bash tools/setup_persistent_disk.sh` |
| `tools/emergency_cleanup.sh` | Emergency disk cleanup | `bash tools/emergency_cleanup.sh --retention-days N` |
| `tools/verify_disk_setup.sh` | Verify disk configuration | `bash tools/verify_disk_setup.sh` |
| `tools/disk_cleanup.py` | Regular automated cleanup | `python -m tools.disk_cleanup` |

---

## Contact & Support

For issues not covered in this guide:

1. Check system logs: `sudo journalctl -u crypto-lake -n 100`
2. Check health report: `cat /data/reports/health.md`
3. Review deployment guide: [PHASE1_DEPLOYMENT_GUIDE.md](PHASE1_DEPLOYMENT_GUIDE.md)
4. Check GitHub issues: https://github.com/Eschaton31/crypto-lake/issues

---

**Last Updated:** 2025-10-31
**Version:** 1.0.0
