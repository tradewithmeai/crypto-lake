# CLOUD FUNCTIONAL PARITY AUDIT — crypto-lake

**Audit Date:** 2025-10-29
**Environment:** GCP VM (crypto-lake-vm, Debian 12, europe-west1-b)
**Service:** crypto-lake.service (systemd)
**Objective:** Compare local vs cloud deployment feature parity

---

## Executive Summary

The crypto-lake system is **partially operational** on GCP. Core data collection and transformation pipelines are working, but **critical validation, QA, and safeguard systems are NOT running**. The comprehensive QA pipeline (anomaly detection, fusion scoring, reporting) exists in the codebase but is **not integrated into the cloud orchestrator**.

**Overall Status:** 🟡 **PARTIAL MIGRATION** — Data collection works, validation/monitoring does not

**Critical Gap:** QA orchestrator (`qa/orchestrator.py`) is a separate process that is **not scheduled or called** by the main orchestrator running under systemd.

---

## Migration Completeness Matrix

| Component | Local Status | Cloud Status | Notes |
|-----------|-------------|-------------|-------|
| **CORE PIPELINE** |
| Collector (WebSocket) | ✅ Working | ✅ Working | Binance WebSocket, 13 symbols, 60s rotation |
| Transformer (OHLCV) | ✅ Working | ✅ Working | 1-second bars, Parquet output, 60-min schedule |
| Macro Data Fetcher | ✅ Working | ✅ Working | yfinance 1m data, 15-min schedule |
| Macro Transformer | ✅ Working | ✅ Working | Validates macro Parquet files |
| Health Monitoring | ✅ Working | ✅ Working | 60s heartbeat JSON/MD, file counts |
| Orchestrator | ✅ Working | ✅ Working | Unified process via systemd |
| **VALIDATION SYSTEMS** |
| Basic Validator | ✅ Working | ⚠️ Present, not scheduled | tools/validator.py (schema, gaps, dupes) |
| Rule-based Validator | ✅ Working | ⚠️ Present, not scheduled | tools/validate_rules.py (R1-R7 rules) |
| Schema Validator (QA) | ✅ Working | 🔴 Not running | qa/schema_validator.py (wraps validate_rules) |
| AI Anomaly Detection | ✅ Working | 🔴 Not running | qa/ai/detectors.py (IsolationForest, ZScore, Jump) |
| Fusion Scoring | ✅ Working | 🔴 Not running | qa/fusion.py (schema + AI scoring) |
| QA Reporting | ✅ Working | 🔴 Not running | qa/reporting.py (PASS/REVIEW/FAIL verdicts) |
| QA Orchestrator | ✅ Working | 🔴 Not running | qa/orchestrator.py (hourly/daily modes) |
| **STORAGE & RETENTION** |
| Raw JSONL Storage | ✅ Working | ✅ Working | /data/raw/binance/[SYMBOL]/[DATE]/ |
| Parquet Storage | ✅ Working | ✅ Working | /data/parquet/binance/[SYMBOL]/year=.../month=.../day=.../ |
| Compactor | ✅ Working | ⚠️ Present, not scheduled | storage/compactor.py (daily files + SHA256) |
| Disk Cleanup | ✅ Working | 🔴 Not implemented | No cleanup script exists |
| GCS Upload | ✅ Working | 🔴 Not implemented | No uploader exists (dependency added in PR #4) |
| **MONITORING & ALERTING** |
| Health Heartbeat | ✅ Working | ✅ Working | /data/logs/health/heartbeat.json |
| Log Rotation | ✅ Working | ✅ Working | 10 MB rotation, 5-file retention |
| Alerting System | ✅ Working | 🔴 Not implemented | No tools/alerting.py exists |
| **INFRASTRUCTURE** |
| Systemd Service | ⚠️ N/A (Windows) | ✅ Working | crypto-lake.service with auto-restart |
| Cron Jobs | ⚠️ Task Scheduler | 🔴 Not configured | No QA/cleanup cron jobs |
| Graceful Shutdown | ✅ Working | ✅ Working | SIGTERM handling in orchestrator |
| **ANALYSIS LAYER** |
| Streamlit Dashboard | ✅ Working | ⚠️ Present, not running | gui/app.py (not deployed to cloud) |
| DuckDB Views | ✅ Working | ✅ Working | tools/db.py (bars_1s, bars_1m, klines_1m) |
| Backfill Tool | ✅ Working | ⚠️ Present, not tested | tools/backfill_binance.py |
| Slice Export | ✅ Working | ⚠️ Present, not tested | tools/slice.py |

**Legend:**
- ✅ **Working** - Fully operational on cloud
- ⚠️ **Present, not verified** - Code exists but not actively running/scheduled
- 🔴 **Not running** - Code exists but not integrated OR code does not exist

---

## Detailed Component Analysis

### 1. Core Data Pipeline ✅ **OPERATIONAL**

#### Collector (collector/collector.py)
**Status:** ✅ **WORKING**
- WebSocket connection to Binance
- 13 symbols: SOLUSDT, SUIUSDT, ADAUSDT, BTCUSDT, ETHUSDT, BNBUSDT, XRPUSDT, DOGEUSDT, AVAXUSDT, LINKUSDT, LTCUSDT, DOTUSDT, EURUSDT
- 60-second file rotation
- Exponential backoff + jitter for reconnections
- Latency tracking (p50/p95/max)
- Thread-safe signal handling (fixed in commit 6f52ae1)

**Verified Evidence:**
- Previous test: 103 raw JSONL files collected
- 1.1GB data written to /data/raw/
- No crashes or connection drops

---

#### Transformer (transformer/transformer.py)
**Status:** ✅ **WORKING**
- Scheduled every 60 minutes by orchestrator
- Generates 1-second OHLCV bars from raw JSONL
- Trade deduplication via trade_id
- Deterministic aggregation (sorted by timestamp)
- Parquet output with Snappy compression
- Partitioned storage (year/month/day)

**Verified Evidence:**
- Previous test: 26 Parquet files created
- Data written to /data/parquet/binance/[SYMBOL]/
- Transform triggered automatically after warmup

---

#### Orchestrator (tools/orchestrator.py)
**Status:** ✅ **WORKING**
- Running as systemd service (crypto-lake.service)
- Manages 5 threads:
  1. WebSocket collector (continuous)
  2. Transformer (60-min schedule)
  3. Macro fetcher (15-min schedule)
  4. Macro transformer (15-min schedule)
  5. Health monitor (60s heartbeat)
- Graceful shutdown handling
- Test mode support (accelerated intervals)

**Configuration:**
- Location: /etc/systemd/system/crypto-lake.service
- Auto-restart: RestartSec=10s
- Start on boot: WantedBy=multi-user.target
- Environment: /etc/default/crypto-lake

---

#### Health Monitoring (tools/health.py)
**Status:** ✅ **WORKING**
- 60-second heartbeat loop
- Outputs:
  - JSON: /data/logs/health/heartbeat.json
  - Markdown: /data/reports/health.md
- Metrics:
  - Collector status + latency stats
  - Macro fetcher status + last run
  - File counts (raw JSONL, Parquet rows)
  - Disk usage

**Missing:** No alerting on health failures (see Section 3)

---

### 2. Validation Systems 🔴 **NOT RUNNING**

#### Basic Validator (tools/validator.py)
**Status:** ⚠️ **PRESENT, NOT SCHEDULED**
- Can be run manually: `python main.py --mode validate --date 2025-10-29`
- Checks:
  - Schema completeness (expected columns)
  - Missing seconds (gaps in 1s data)
  - Duplicate timestamps
- Output: /data/logs/validation/[EXCHANGE]_[SYMBOL]_[DATE].txt

**Issue:** Not scheduled by orchestrator, not in cron jobs

---

#### Rule-based Validator (tools/validate_rules.py)
**Status:** ⚠️ **PRESENT, NOT SCHEDULED**
- Can be run manually: `python main.py --mode validate_rules --symbols SOLUSDT --start ... --end ...`
- Comprehensive rule checks:
  - **R1:** OHLC ordering (low ≤ open,close ≤ high)
  - **R2:** Positive prices (OHLC > 0)
  - **R3:** Ask ≥ Bid
  - **R4:** No NaN values in OHLC
  - **R5:** Timestamp continuity
  - **R6:** Spread sanity (< 500 bps)
  - **R7:** Kline parity (bars vs klines comparison)
- Output: Markdown report with violations

**Issue:** Not scheduled by orchestrator, not in cron jobs

---

#### QA Pipeline 🔴 **NOT INTEGRATED**

##### Schema Validator (qa/schema_validator.py)
**Status:** 🔴 **NOT RUNNING**
- Wraps tools/validate_rules.py
- Transforms output to JSONL contract format
- Severity mapping: critical/major/minor
- Output: /data/logs/qa/violations_YYYY-MM-DD.jsonl

**Issue:** Only called by QA orchestrator, which is not running

---

##### AI Anomaly Detection (qa/ai/detectors.py)
**Status:** 🔴 **NOT RUNNING**
- **IsolationForest Detector:**
  - ML-based outlier detection
  - Config: n_estimators=200, contamination=0.005
  - Features: spread_bp, log_volume, vwap_drift_bp
- **ZScore Detector:**
  - Rolling window Z-score (1-hour window, k=5.0)
  - Detects outliers in spread, volume, VWAP
- **Jump Detector:**
  - Sudden price movements (k_sigma=6.8)
  - Stable spread condition required (< 50 bps)
  - Cooldown clustering (5-second window)
- Output: /data/logs/qa/anomalies_YYYY-MM-DD.jsonl

**Issue:** Only called by QA orchestrator, which is not running

**Configuration in config.yml (lines 53-77):**
```yaml
qa:
  enable_ai: true
  hourly_window_min: 90
  daily_run_utc: "00:15"
  ai_labeler: "rules"

  iforest:
    n_estimators: 200
    contamination: 0.005
    random_state: 42

  zscore:
    window: 3600
    k: 5.0

  jump:
    k_sigma: 6.8
    spread_stable_bps: 50
    cooldown_seconds: 5
    min_trade_count: 5
```

---

##### Fusion Scoring (qa/fusion.py)
**Status:** 🔴 **NOT RUNNING**
- Combines schema violations + AI anomalies
- Scoring formula:
  ```
  fusion_score = 0.7 * schema_score + 0.2 * detector_score + 0.1 * ai_confidence
  ```
- Verdict logic:
  - **PASS:** score ≥ 0.85 AND no critical violations
  - **REVIEW:** 0.65 ≤ score < 0.85 OR has major violations
  - **FAIL:** score < 0.65 OR has critical violations
- Output: /data/logs/qa/fusion_scores_YYYY-MM-DD.parquet

**Issue:** Only called by QA orchestrator, which is not running

---

##### QA Reporting (qa/reporting.py)
**Status:** 🔴 **NOT RUNNING**
- Generates daily QA reports
- Includes:
  - Top-N symbols by anomaly count
  - Fusion verdict summary (PASS/REVIEW/FAIL)
  - Schema violation breakdown by severity
  - AI detector statistics
- Output: /data/reports/qa_YYYY-MM-DD.md

**Issue:** Only called by QA orchestrator, which is not running

---

##### QA Orchestrator (qa/orchestrator.py)
**Status:** 🔴 **NOT RUNNING**
- **CRITICAL FINDING:** This is a **separate orchestrator** from tools/orchestrator.py
- Modes:
  - **hourly:** Run QA on last N minutes (default: 90 minutes)
  - **daily:** Run full-day QA at 00:15 UTC
- Pipeline steps:
  1. Schema validation
  2. AI detection
  3. Fusion scoring
  4. Report generation

**Issue:** **NOT INTEGRATED INTO main.py**
- main.py --mode choices: collector, transformer, validate, compact, macro_minute, slice, validate_rules, orchestrate, backfill_binance, test
- **NO "qa" MODE EXISTS**
- QA orchestrator must be run separately: `python -m qa.orchestrator --mode hourly --config config.yml`

**Root Cause:** The main orchestrator (tools/orchestrator.py) does NOT schedule QA runs. The QA system was designed as a separate sidecar process.

---

### 3. Storage & Retention 🟡 **PARTIAL**

#### Compactor (storage/compactor.py)
**Status:** ⚠️ **PRESENT, NOT SCHEDULED**
- Reads partitioned Parquet files
- Consolidates to daily files (YYYY-MM-DD.parquet)
- Validates continuity (gap detection)
- Computes SHA256 hash for integrity
- Writes metadata sidecar (YYYY-MM-DD.meta.json)

**Usage:** `python main.py --mode compact --date 2025-10-29`

**Issue:** Not scheduled by orchestrator, not in cron jobs

---

#### Disk Cleanup 🔴 **NOT IMPLEMENTED**
**Status:** 🔴 **MISSING**
- No cleanup script exists in tools/
- Raw JSONL files accumulate indefinitely
- Risk: Disk full in ~12 days (20GB total, 1.1GB/day raw data)

**Required Action:** Implement tools/disk_cleanup.py with:
- Delete raw JSONL files older than 7 days
- Keep Parquet files indefinitely (or sync to GCS first)
- Alert when disk usage > 80%

---

#### GCS Upload 🔴 **NOT IMPLEMENTED**
**Status:** 🔴 **MISSING**
- No uploader script exists in tools/
- google-cloud-storage dependency added (PR #4) but not yet merged
- All data remains local to VM only
- Risk: Data loss if VM fails

**Required Action:** Implement tools/gcs_uploader.py with:
- Daily sync of Parquet files to GCS bucket
- Only upload complete daily files (not current day)
- Verify uploads with MD5 checksums
- Optionally delete local files after successful upload

**Configuration in config.yml (lines 118-119):**
```yaml
gcs:
  bucket_name: ""  # Empty - needs to be set
```

---

### 4. Monitoring & Alerting 🔴 **PARTIAL**

#### Health Heartbeat ✅ **WORKING**
**Status:** ✅ **OPERATIONAL**
- Writes metrics every 60 seconds
- Tracks:
  - Collector status (running/stopped/error)
  - Macro fetcher status + last run times
  - Transformer status + last run times
  - File counts and disk usage

**Missing:** No automated response to failures

---

#### Alerting System 🔴 **NOT IMPLEMENTED**
**Status:** 🔴 **MISSING**
- No tools/alerting.py exists
- No webhook/email/Slack integration
- Health failures go unnoticed

**Required Action:** Implement tools/alerting.py with:
- Webhook alerting (HTTP POST to monitoring service)
- Alert conditions:
  - Collector stopped
  - Disk usage > 80%
  - Missing data gaps > 5 minutes
  - Transform failure
- Rate limiting (max 1 alert per hour per condition)

**Configuration needed in config.yml:**
```yaml
alerting:
  webhook_url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
  conditions:
    - type: disk_usage
      threshold: 80
    - type: data_gap
      threshold_minutes: 5
```

---

### 5. Infrastructure 🟡 **PARTIAL**

#### Systemd Service ✅ **WORKING**
**Status:** ✅ **DEPLOYED** (PR #3 merged)
- Service file: /etc/systemd/system/crypto-lake.service
- Environment: /etc/default/crypto-lake
- Auto-restart on failure (RestartSec=10s)
- Start on boot (WantedBy=multi-user.target)

**Commands:**
```bash
sudo systemctl status crypto-lake
sudo systemctl restart crypto-lake
sudo journalctl -u crypto-lake -f
```

---

#### Cron Jobs 🔴 **NOT CONFIGURED**
**Status:** 🔴 **MISSING**

**Required Cron Jobs:**

1. **Daily QA (at 00:15 UTC):**
   ```cron
   15 0 * * * /home/Eschaton/crypto-lake/venv/bin/python -m qa.orchestrator --mode daily --config /home/Eschaton/crypto-lake/config.yml
   ```

2. **Hourly QA (at :00):**
   ```cron
   0 * * * * /home/Eschaton/crypto-lake/venv/bin/python -m qa.orchestrator --mode hourly --config /home/Eschaton/crypto-lake/config.yml
   ```

3. **Disk Cleanup (daily at 02:00 UTC):**
   ```cron
   0 2 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/disk_cleanup.py
   ```

4. **GCS Upload (daily at 03:00 UTC):**
   ```cron
   0 3 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/gcs_uploader.py
   ```

5. **Compactor (daily at 04:00 UTC):**
   ```cron
   0 4 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/main.py --mode compact --date $(date -u -d 'yesterday' '+%Y-%m-%d')
   ```

**Installation:**
```bash
crontab -e
# Add the above lines
```

---

### 6. Analysis Layer ⚠️ **PRESENT, NOT DEPLOYED**

#### Streamlit Dashboard (gui/app.py)
**Status:** ⚠️ **NOT RUNNING ON CLOUD**
- Exists locally, tested successfully
- Features:
  - Multi-timeframe candlestick charts
  - Volume bars, spread visualization
  - Data quality metrics
  - Auto-refresh (15s - 5m intervals)
  - CSV export

**Deployment Options:**
1. Run on VM with port forwarding: `venv/bin/python -m streamlit run gui/app.py --server.port 8501`
2. Deploy to Cloud Run as separate service
3. Use Cloud Storage + BigQuery for data access

---

#### Backfill Tool (tools/backfill_binance.py)
**Status:** ⚠️ **PRESENT, NOT TESTED ON CLOUD**
- Fetches historical klines via Binance REST API
- Usage: `python main.py --mode backfill_binance --symbols SOLUSDT --date 2025-10-01 --interval 1m`
- Supports 1m, 5m, 15m, 1h, 4h, 1d intervals

---

## Critical Gaps Summary

### 🔴 **CRITICAL** - Blocks Production Use

1. **QA Pipeline Not Running**
   - Entire qa/ directory exists but not integrated
   - No scheduled schema validation
   - No AI anomaly detection
   - No fusion scoring or reporting
   - Impact: Data quality issues go undetected

2. **Disk Cleanup Missing**
   - Raw JSONL files accumulate indefinitely
   - Disk full in ~12 days without cleanup
   - Impact: System failure when disk fills

3. **GCS Backup Missing**
   - No cloud backup of Parquet files
   - All data local to VM only
   - Impact: Data loss risk if VM fails

### 🟡 **HIGH** - Reduces Operational Visibility

4. **No Alerting System**
   - Health failures silent
   - No notification of collector disconnects
   - Impact: Delayed response to failures

5. **Compactor Not Scheduled**
   - Daily consolidation not automated
   - SHA256 verification not performed
   - Impact: No integrity checks on historical data

### 🟢 **MEDIUM** - Functional but Suboptimal

6. **Dashboard Not Deployed**
   - Streamlit GUI exists but not accessible
   - No real-time monitoring UI
   - Impact: Reduced operational convenience

7. **No Cron Jobs Configured**
   - All maintenance tasks manual
   - QA, cleanup, compaction require manual trigger
   - Impact: Operational burden, inconsistent execution

---

## Recommended Actions (Prioritized)

### Phase 1: Restore QA Systems (Week 1) 🔴

#### 1A. Integrate QA Orchestrator into Cron
**Priority:** 🔴 **CRITICAL**
**Effort:** 1 hour

**Steps:**
```bash
# SSH to GCP VM
gcloud compute ssh crypto-lake-vm --zone=europe-west1-b

# Edit crontab
crontab -e

# Add QA jobs
15 0 * * * /home/Eschaton/crypto-lake/venv/bin/python -m qa.orchestrator --mode daily --config /home/Eschaton/crypto-lake/config.yml >> /data/logs/qa/cron-daily.log 2>&1
0 * * * * /home/Eschaton/crypto-lake/venv/bin/python -m qa.orchestrator --mode hourly --config /home/Eschaton/crypto-lake/config.yml >> /data/logs/qa/cron-hourly.log 2>&1

# Verify
crontab -l
```

**Validation:**
- Check logs after next scheduled run: `tail -f /data/logs/qa/cron-daily.log`
- Verify JSONL outputs: `ls -lh /data/logs/qa/*.jsonl`
- Check reports: `ls -lh /data/reports/qa_*.md`

---

#### 1B. Implement Disk Cleanup Script
**Priority:** 🔴 **CRITICAL**
**Effort:** 2-3 hours

**Create tools/disk_cleanup.py:**
```python
import os
import glob
from datetime import datetime, timedelta, timezone
from loguru import logger
from tools.logging_setup import setup_logging
from tools.common import load_config

setup_logging()

def cleanup_old_raw_files(base_path: str, retention_days: int = 7):
    """Delete raw JSONL files older than retention_days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    pattern = os.path.join(base_path, "raw", "**", "*.jsonl")
    files = glob.glob(pattern, recursive=True)

    deleted_count = 0
    deleted_size = 0

    for file_path in files:
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(file_path), tz=timezone.utc)
            if mtime < cutoff:
                size = os.path.getsize(file_path)
                os.remove(file_path)
                deleted_count += 1
                deleted_size += size
        except Exception as e:
            logger.error(f"Failed to delete {file_path}: {e}")

    logger.info(f"Deleted {deleted_count} files ({deleted_size / 1e9:.2f} GB)")

if __name__ == "__main__":
    config = load_config("config.yml")
    base_path = config["general"]["base_path"]
    cleanup_old_raw_files(base_path, retention_days=7)
```

**Add to crontab:**
```cron
0 2 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/disk_cleanup.py >> /data/logs/qa/cleanup.log 2>&1
```

---

#### 1C. Implement GCS Uploader
**Priority:** 🔴 **CRITICAL**
**Effort:** 3-4 hours

**Prerequisites:**
- Merge PR #4 (google-cloud-storage dependency)
- Install on VM: `venv/bin/pip install google-cloud-storage`
- Set GCS bucket in config.yml: `gcs.bucket_name: "crypto-lake-data"`
- Configure service account credentials

**Create tools/gcs_uploader.py:**
```python
import os
import glob
from datetime import datetime, timedelta, timezone
from google.cloud import storage
from loguru import logger
from tools.logging_setup import setup_logging
from tools.common import load_config

setup_logging()

def upload_parquet_to_gcs(base_path: str, bucket_name: str, exclude_current_day: bool = True):
    """Upload Parquet files to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    pattern = os.path.join(base_path, "parquet", "**", "*.parquet")
    files = glob.glob(pattern, recursive=True)

    uploaded_count = 0

    for local_path in files:
        # Skip current day files (still being written)
        if exclude_current_day and today in local_path:
            continue

        # Generate GCS path relative to base_path
        rel_path = os.path.relpath(local_path, base_path)
        gcs_path = rel_path.replace("\\", "/")

        blob = bucket.blob(gcs_path)

        # Skip if already exists and same size
        if blob.exists():
            if blob.size == os.path.getsize(local_path):
                logger.debug(f"Skipping {gcs_path} (already uploaded)")
                continue

        # Upload
        blob.upload_from_filename(local_path)
        uploaded_count += 1
        logger.info(f"Uploaded {gcs_path}")

    logger.info(f"Uploaded {uploaded_count} files to gs://{bucket_name}")

if __name__ == "__main__":
    config = load_config("config.yml")
    base_path = config["general"]["base_path"]
    bucket_name = config["gcs"]["bucket_name"]

    if not bucket_name:
        logger.error("GCS bucket_name not configured in config.yml")
        exit(1)

    upload_parquet_to_gcs(base_path, bucket_name)
```

**Add to crontab:**
```cron
0 3 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/tools/gcs_uploader.py >> /data/logs/qa/gcs-upload.log 2>&1
```

---

### Phase 2: Monitoring & Alerting (Week 2) 🟡

#### 2A. Implement Alerting System
**Priority:** 🟡 **HIGH**
**Effort:** 2-3 hours

**Create tools/alerting.py with webhook support**

**Add alert checks to tools/health.py**

**Configure webhook URL in config.yml**

---

#### 2B. Schedule Compactor
**Priority:** 🟡 **HIGH**
**Effort:** 10 minutes

**Add to crontab:**
```cron
0 4 * * * /home/Eschaton/crypto-lake/venv/bin/python /home/Eschaton/crypto-lake/main.py --mode compact --date $(date -u -d 'yesterday' '+%Y-%m-%d') >> /data/logs/qa/compact.log 2>&1
```

---

### Phase 3: Analysis Layer (Week 3) 🟢

#### 3A. Deploy Streamlit Dashboard
**Priority:** 🟢 **MEDIUM**
**Effort:** 1-2 hours

**Option 1: Run on VM**
```bash
nohup venv/bin/python -m streamlit run gui/app.py --server.port 8501 > /data/logs/streamlit.log 2>&1 &
```

**Option 2: Deploy to Cloud Run**
- Build Docker image from Dockerfile
- Deploy to Cloud Run with read access to GCS bucket
- Use BigQuery for multi-day queries

---

## Operational Safety Assessment

### ✅ **Currently Safe**
- Data collection: Continuous, stable
- Storage: Writes succeeding
- Restarts: Auto-restart on failure
- Logging: Comprehensive, rotated

### ⚠️ **At Risk**
- **Disk space:** Will fill in ~12 days without cleanup
- **Data quality:** No automated validation running
- **Data durability:** No cloud backup
- **Failure detection:** No alerting on errors

### 🔴 **Critical Deficiencies**
- **QA pipeline disabled:** Anomalies, violations, bad data go unnoticed
- **No cleanup automation:** Manual intervention required
- **No backup strategy:** Single point of failure (VM)

---

## Summary of Required Changes

| Action | Priority | Effort | Impact |
|--------|----------|--------|--------|
| Add QA cron jobs | 🔴 Critical | 10 min | Restore full validation |
| Implement disk cleanup | 🔴 Critical | 2-3 hrs | Prevent disk full |
| Implement GCS uploader | 🔴 Critical | 3-4 hrs | Data durability |
| Implement alerting | 🟡 High | 2-3 hrs | Operational visibility |
| Schedule compactor | 🟡 High | 10 min | Daily consolidation |
| Deploy dashboard | 🟢 Medium | 1-2 hrs | User interface |

**Total Effort (Phase 1 only):** 6-8 hours
**Total Effort (All phases):** 10-15 hours

---

## Conclusion

The crypto-lake system on GCP is **functionally collecting and transforming data**, but **critical validation and safeguard systems are not running**. The QA pipeline exists in the codebase but was designed as a separate orchestrator that is **not integrated into the main systemd service**.

**Immediate Action Required:**
1. Configure cron jobs to run QA orchestrator (hourly + daily)
2. Implement disk cleanup script (prevent disk full in 12 days)
3. Implement GCS uploader (data durability)

**Timeline:**
- **Week 1:** Restore QA, cleanup, GCS backup (6-8 hours)
- **Week 2:** Alerting + compactor scheduling (3-4 hours)
- **Week 3:** Dashboard deployment (1-2 hours)

After Phase 1 completion, the system will achieve **full feature parity** with local deployment and be ready for production use.

---

**Audit Completed:** 2025-10-29
**Next Review:** After Phase 1 implementation
