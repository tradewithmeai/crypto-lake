# Phase 1 Deployment Guide - GCP VM

**Target:** crypto-lake-vm (GCP, Debian 12, europe-west1-b)
**Objective:** Achieve full feature parity with local deployment
**Estimated Time:** 7-9 hours

---

## Prerequisites Checklist

Before starting Phase 1 deployment:

- [ ] All Phase 1 PRs merged to main branch
  - PR #3: systemd-service
  - PR #4: gcs-dependency (google-cloud-storage)
  - PR #5: dockerfile-entrypoint
  - PR #6: qa-nonblocking-optimization
- [ ] GCP VM accessible via SSH
- [ ] Service account credentials available for GCS access
- [ ] GCS bucket created (e.g., `crypto-lake-data`)

---

## Step 1: Connect to GCP VM (5 minutes)

```bash
# SSH to VM
gcloud compute ssh crypto-lake-vm \
  --zone=europe-west1-b \
  --project=serious-conduit-476419-q7

# Verify you're on the VM
hostname  # Should show: crypto-lake-vm
whoami    # Should show: Eschaton
pwd       # Should show: /home/Eschaton
```

---

## Step 2: Pull Latest Code (10 minutes)

```bash
# Navigate to project directory
cd ~/crypto-lake

# Check current status
git status
git branch  # Should be on 'main'

# Pull latest changes
git pull origin main

# Verify new scripts exist
ls -lh tools/disk_cleanup.py tools/gcs_uploader.py
# Should show both files

# Check requirements
cat requirements.txt | grep google-cloud-storage
# Should show: google-cloud-storage>=2.10.0
```

---

## Step 3: Install Dependencies (5 minutes)

```bash
# Activate virtual environment
source venv/bin/activate

# Update pip
python -m pip install --upgrade pip

# Install/update requirements
pip install -r requirements.txt

# Verify google-cloud-storage installed
python -c "from google.cloud import storage; print('✓ google-cloud-storage OK')"
```

---

## Step 4: Configure GCS Bucket (15 minutes)

### 4A. Create GCS Bucket (if not exists)

```bash
# Check if bucket exists
gsutil ls gs://crypto-lake-data/ 2>/dev/null && echo "Bucket exists" || echo "Bucket not found"

# If bucket doesn't exist, create it
gsutil mb -l europe-west1 gs://crypto-lake-data/
gsutil ls gs://crypto-lake-data/  # Verify
```

### 4B. Set Up Service Account

```bash
# Option 1: Use Application Default Credentials (if available)
gcloud auth application-default login

# Option 2: Use Service Account Key (recommended for production)
# Download key from GCP Console → IAM → Service Accounts
# Upload to VM:
# gcloud compute scp /local/path/to/service-account-key.json crypto-lake-vm:~/crypto-lake/ --zone=europe-west1-b

# Set environment variable
export GOOGLE_APPLICATION_CREDENTIALS="/home/Eschaton/crypto-lake/service-account-key.json"

# Add to /etc/default/crypto-lake for systemd
echo 'GOOGLE_APPLICATION_CREDENTIALS="/home/Eschaton/crypto-lake/service-account-key.json"' | sudo tee -a /etc/default/crypto-lake
```

### 4C. Update config.yml

```bash
# Edit config
nano config.yml

# Set GCS bucket name (around line 119)
# FROM:
#   gcs:
#     bucket_name: ""
# TO:
#   gcs:
#     bucket_name: "crypto-lake-data"

# Save and exit (Ctrl+X, Y, Enter)

# Verify change
cat config.yml | grep -A 1 "^gcs:"
# Should show:
#   gcs:
#     bucket_name: "crypto-lake-data"
```

---

## Step 5: Test Scripts Manually (30 minutes)

### 5A. Test Disk Cleanup (Dry Run)

```bash
# Run in dry-run mode to see what would be deleted
python tools/disk_cleanup.py --dry-run

# Expected output:
# INFO | Cleanup starting: retention=7 days, cutoff=...
# INFO | Found X raw JSONL files total
# INFO | [DRY RUN] Would delete: ...
# INFO | Cleanup complete: Would delete X files (Y GB), kept Z files

# Check no errors
echo $?  # Should be 0
```

### 5B. Test GCS Uploader (Dry Run)

```bash
# Run in dry-run mode to see what would be uploaded
python tools/gcs_uploader.py --dry-run

# Expected output:
# INFO | GCS upload starting: bucket=crypto-lake-data, exclude_current_day=True, dry_run=True
# INFO | Connected to GCS bucket: crypto-lake-data
# INFO | Found X total Parquet files
# INFO | [DRY RUN] Would upload: parquet/binance/SOLUSDT/...
# INFO | GCS upload complete: Would upload X files (Y GB), skipped Z, excluded A

# Check no errors
echo $?  # Should be 0
```

### 5C. Test QA Orchestrator (Manual Run)

```bash
# Run QA orchestrator in hourly mode
python -m qa.orchestrator --mode hourly --config config.yml

# Expected output:
# INFO | QA Orchestrator starting in hourly mode
# INFO | Hourly QA mode: window = 30 minutes
# INFO | Running QA pipeline for 2025-10-29 (timeframe: 1s)
# INFO | Step 1/4: Schema validation
# INFO | [Schema validation] Starting (timeout: 600s)...
# INFO | [Schema validation] Completed successfully in X.Xs
# INFO | Step 2/4: AI detection
# ...
# INFO | QA pipeline completed for 2025-10-29 in X.Xs

# Check outputs
ls -lh /data/logs/qa/violations_*.jsonl
ls -lh /data/logs/qa/anomalies_*.jsonl
ls -lh /data/logs/qa/fusion_scores_*.parquet
ls -lh /data/reports/qa_*.md

# Read latest report
cat /data/reports/qa_$(date -u +%Y-%m-%d).md | head -50
```

---

## Step 6: Configure Cron Jobs (20 minutes)

```bash
# Open crontab editor
crontab -e

# Add the following entries (copy-paste entire block):
# ========================================
# Crypto Lake - QA and Maintenance Jobs
# ========================================

# QA Orchestrator - Daily (at 00:15 UTC)
15 0 * * * /home/Eschaton/crypto-lake/venv/bin/python -m qa.orchestrator --mode daily --config /home/Eschaton/crypto-lake/config.yml >> /data/logs/qa/cron-daily.log 2>&1

# QA Orchestrator - Hourly (at :00 every hour)
0 * * * * /home/Eschaton/crypto-lake/venv/bin/python -m qa.orchestrator --mode hourly --config /home/Eschaton/crypto-lake/config.yml >> /data/logs/qa/cron-hourly.log 2>&1

# Disk Cleanup - Daily (at 02:00 UTC)
0 2 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/disk_cleanup.py >> /data/logs/qa/cleanup.log 2>&1

# GCS Upload - Daily (at 03:00 UTC)
0 3 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/gcs_uploader.py >> /data/logs/qa/gcs-upload.log 2>&1

# Compactor - Daily (at 04:00 UTC, process yesterday's data)
0 4 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/main.py --mode compact --date $(date -u -d 'yesterday' '+\%Y-\%m-\%d') >> /data/logs/qa/compact.log 2>&1

# Save and exit (Ctrl+X, Y, Enter in nano)

# Verify cron jobs are installed
crontab -l

# Expected output should show all 5 jobs above
```

---

## Step 7: Monitor First Runs (60 minutes)

### 7A. Monitor Hourly QA (Wait for next :00)

```bash
# Check current time
date -u

# Wait for next hour (or trigger manually if needed)
# At next :00, monitor log
tail -f /data/logs/qa/cron-hourly.log

# Should see:
# INFO | QA Orchestrator starting in hourly mode
# INFO | Running QA pipeline for...
# ...
# INFO | QA pipeline completed for ... in X.Xs

# Verify outputs created
ls -lh /data/logs/qa/violations_$(date -u +%Y-%m-%d).jsonl
ls -lh /data/logs/qa/anomalies_$(date -u +%Y-%m-%d).jsonl
ls -lh /data/reports/qa_$(date -u +%Y-%m-%d).md
```

### 7B. Monitor Disk Cleanup (Trigger Manually)

```bash
# Run cleanup manually to test
python tools/disk_cleanup.py

# Check log output
cat /data/logs/qa/cleanup.log

# Verify old files deleted (if any existed)
# Check disk space freed
df -h /data
```

### 7C. Monitor GCS Upload (Trigger Manually)

```bash
# Run GCS upload manually to test
python tools/gcs_uploader.py

# Check log output
cat /data/logs/qa/gcs-upload.log

# Verify files uploaded to GCS
gsutil ls -lh gs://crypto-lake-data/parquet/binance/SOLUSDT/ | head -20

# Check upload count
gsutil ls -r gs://crypto-lake-data/parquet/ | wc -l
```

---

## Step 8: Verify System Health (15 minutes)

```bash
# Check orchestrator status
sudo systemctl status crypto-lake

# Should show:
#   Active: active (running)
#   Main PID: XXXX

# Check recent logs
sudo journalctl -u crypto-lake -n 50

# Check health heartbeat
cat /data/logs/health/heartbeat.json | python -m json.tool

# Check disk usage
df -h /data

# Check cron jobs are scheduled
crontab -l | grep -c "crypto-lake"
# Should show: 5

# Check log files exist
ls -lh /data/logs/qa/*.log

# Check collector still writing data
ls -lt /data/raw/binance/SOLUSDT/$(date -u +%Y-%m-%d)/ | head -5
```

---

## Step 9: Validation Checklist

### Phase 1A: QA Orchestrator ✅
- [ ] Cron jobs configured (hourly + daily)
- [ ] First hourly run completed successfully
- [ ] QA outputs created (violations, anomalies, fusion scores, reports)
- [ ] No timeout warnings in logs
- [ ] Logs clean (no errors)

### Phase 1B: Disk Cleanup ✅
- [ ] disk_cleanup.py tested in dry-run mode
- [ ] Manual cleanup run successful
- [ ] Cron job scheduled (02:00 UTC)
- [ ] Old files deleted (if any existed)
- [ ] Disk space freed
- [ ] cleanup.log created and clean

### Phase 1C: GCS Upload ✅
- [ ] google-cloud-storage installed
- [ ] Service account configured
- [ ] config.yml updated with bucket name
- [ ] gcs_uploader.py tested in dry-run mode
- [ ] Manual upload run successful
- [ ] Files visible in GCS bucket
- [ ] Cron job scheduled (03:00 UTC)
- [ ] gcs-upload.log created and clean

### System Health ✅
- [ ] Orchestrator running (systemd status active)
- [ ] Collector still active and writing data
- [ ] Transformer running on schedule (60-min)
- [ ] Health heartbeat updating every 60s
- [ ] No disk space warnings (<80% used)
- [ ] All 5 cron jobs scheduled
- [ ] All log files clean (no errors)

---

## Step 10: Monitor for 24 Hours

### Monitoring Schedule

| Time (UTC) | Job | What to Check |
|------------|-----|---------------|
| 00:15 | Daily QA | /data/logs/qa/cron-daily.log |
| 02:00 | Disk Cleanup | /data/logs/qa/cleanup.log |
| 03:00 | GCS Upload | /data/logs/qa/gcs-upload.log |
| 04:00 | Compactor | /data/logs/qa/compact.log |
| Every :00 | Hourly QA | /data/logs/qa/cron-hourly.log |

### Monitoring Commands

```bash
# Monitor all cron logs in real-time
tail -f /data/logs/qa/*.log

# Check for errors in any log
grep -i error /data/logs/qa/*.log

# Check disk usage trending
watch -n 300 df -h /data  # Every 5 minutes

# Check GCS bucket size
gsutil du -sh gs://crypto-lake-data/

# Check health heartbeat
watch -n 60 "cat /data/logs/health/heartbeat.json | python -m json.tool"
```

---

## Troubleshooting

### Issue: QA Orchestrator Times Out

**Symptoms:** Log shows "Timeout exceeded (600s)"

**Solution:**
```bash
# Check if data exists for the date
ls /data/parquet/binance/SOLUSDT/year=$(date -u +%Y)/month=$(date -u +%-m)/day=$(date -u +%-d)/

# Check DuckDB can read files
python -c "import duckdb; con = duckdb.connect(); print(con.execute(\"SELECT COUNT(*) FROM read_parquet('/data/parquet/**/*.parquet')\").fetchone())"

# Increase timeout in qa/orchestrator.py if needed (current: 600s)
```

### Issue: Disk Cleanup Deletes Nothing

**Symptoms:** cleanup.log shows "Deleted 0 files"

**Solution:**
```bash
# Check if any files are old enough (>7 days)
find /data/raw -name "*.jsonl" -mtime +7

# Reduce retention days for testing
python tools/disk_cleanup.py --retention-days 1 --dry-run

# Check file modification times
ls -lt /data/raw/binance/SOLUSDT/*/part*.jsonl | tail -20
```

### Issue: GCS Upload Fails with Auth Error

**Symptoms:** gcs-upload.log shows "Failed to connect to GCS: Unauthorized"

**Solution:**
```bash
# Check GOOGLE_APPLICATION_CREDENTIALS is set
echo $GOOGLE_APPLICATION_CREDENTIALS

# Verify service account key file exists
ls -lh ~/crypto-lake/service-account-key.json

# Test auth manually
gcloud auth activate-service-account --key-file=~/crypto-lake/service-account-key.json

# Verify bucket access
gsutil ls gs://crypto-lake-data/

# Check service account has Storage Object Admin role
```

### Issue: Cron Jobs Not Running

**Symptoms:** Log files not created at scheduled times

**Solution:**
```bash
# Verify cron service is running
sudo systemctl status cron

# Check crontab syntax
crontab -l

# Check cron logs
sudo grep CRON /var/log/syslog | tail -20

# Verify paths are absolute
crontab -l | grep crypto-lake

# Test command directly
/home/Eschaton/crypto-lake/venv/bin/python -m qa.orchestrator --mode hourly --config /home/Eschaton/crypto-lake/config.yml
```

---

## Rollback Plan

If issues occur after deployment:

### Rollback Cron Jobs
```bash
# Edit crontab
crontab -e

# Comment out problematic jobs with #
# Or remove them entirely

# Verify
crontab -l
```

### Rollback Code Changes
```bash
# Revert to previous commit
git log --oneline -5
git checkout <previous-commit-hash>

# Or revert specific files
git checkout HEAD~1 -- tools/disk_cleanup.py tools/gcs_uploader.py

# Restart orchestrator
sudo systemctl restart crypto-lake
```

---

## Success Criteria

Phase 1 is complete when:

✅ **All Components Operational:**
- QA orchestrator running hourly and daily
- Disk cleanup running daily
- GCS upload running daily
- Compactor running daily
- All cron jobs executing without errors

✅ **Data Quality Monitoring Active:**
- Schema violations detected and logged
- AI anomaly detection running
- Fusion scoring computing PASS/REVIEW/FAIL verdicts
- Daily QA reports generated

✅ **System Safeguards Working:**
- Old raw files deleted after 7 days
- Parquet files backed up to GCS
- Daily file consolidation with SHA256 verification

✅ **Full Feature Parity Achieved:**
- Cloud deployment has same capabilities as local
- All validation systems operational
- No gaps in monitoring or QA

---

## Next Steps After Phase 1

Once Phase 1 is validated and stable:

### Phase 2: Monitoring & Alerting (Week 2)
- Implement `tools/alerting.py` for webhook notifications
- Add alert conditions: disk >80%, collector stopped, data gaps
- Test alerting to Slack/email/webhook endpoint

### Phase 3: Analysis Layer (Week 3)
- Deploy Streamlit dashboard to Cloud Run or VM
- Set up Jupyter notebook environment
- Create pre-built DuckDB analysis views

---

**Deployment Guide Completed**
**Next Review:** After 24-hour monitoring period
