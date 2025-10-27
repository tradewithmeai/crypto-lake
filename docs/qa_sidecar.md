# Crypto Lake QA Sidecar

Comprehensive quality assurance system for the Crypto Lake data pipeline.

## Overview

The QA Sidecar is a **non-blocking** quality assurance system that validates data quality through:

1. **Schema Validation** - Validates OHLCV data against R1-R7 rules
2. **AI/Statistical Detection** - Detects anomalies using Z-score, Jump, and IsolationForest detectors
3. **Fusion Scoring** - Combines results into actionable scores and verdicts
4. **Daily Reporting** - Generates Markdown reports with quality metrics

**Key Principles:**
- Non-blocking: Reads existing Parquet files, never modifies ingestion
- Separate orchestration: Independent from main data pipeline
- Machine-readable outputs: JSONL for violations/anomalies, Parquet for scores
- Atomic writes: All outputs use temp file → os.replace() pattern

---

## Architecture

```
┌─────────────────┐
│  Parquet Data   │  (Read-only input)
└────────┬────────┘
         │
    ┌────▼────────────────────────────────┐
    │                                     │
┌───▼──────────┐              ┌──────────▼────┐
│   Schema     │              │   AI/Stat     │
│  Validator   │              │  Detectors    │
│  (R1-R7)     │              │  (ZScore,     │
│              │              │   Jump,       │
│              │              │   IForest)    │
└───┬──────────┘              └──────────┬────┘
    │                                    │
    │ violations.jsonl      anomalies.jsonl
    │                                    │
    └────────┬───────────────────────────┘
             │
        ┌────▼────────┐
        │   Fusion    │
        │   Scoring   │
        └────┬────────┘
             │
             │ fusion.parquet
             │
        ┌────▼────────┐
        │  Reporting  │
        └─────────────┘
             │
             │ qa_report.md
             ▼
```

---

## Installation

### 1. Install QA Dependencies

```bash
# From project root
pip install -r requirements-qa.txt
```

This installs:
- `scikit-learn` - For IsolationForest and feature scaling
- `pandera` - For advanced schema validation (optional)
- `pytz` - For timezone support

### 2. Configure QA Settings

Add `qa:` section to `config.yml` (already included):

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
    window: "1h"
    k: 5.0
  jump:
    k_sigma: 6.8                 # Jump threshold (stricter than 6.0)
    spread_stable_bps: 50        # Maximum spread for stable condition
    cooldown_seconds: 5          # Cluster window: merge anomalies within 5s
    min_trade_count: 5           # Skip ultra-thin seconds (< 5 trades)
  reporting:
    anomalies_top_n: 10          # Show top-N symbols in reports
```

### 3. Verify Installation

```bash
python -m pytest tests/qa/ -v
```

All tests should pass in <5 seconds.

---

## Usage

### Manual Runs

#### Run Complete QA Pipeline (Recommended)

```bash
# Process today's data
python -m qa.orchestrator --mode hourly

# Process yesterday's full day
python -m qa.orchestrator --mode daily
```

#### Run Individual Steps

```bash
# 1. Schema validation only
python -m qa.run_schema --day 2025-10-23

# 2. AI detection only
python -m qa.run_ai --day 2025-10-23

# 3. Fusion scoring only
python -m qa.run_fusion --day 2025-10-23

# 4. Report generation only
python -m qa.run_report --day 2025-10-23
```

#### Date Range Processing

```bash
# Process multiple days
python -m qa.run_schema --from 2025-10-21 --to 2025-10-23
python -m qa.run_ai --from 2025-10-21 --to 2025-10-23
python -m qa.run_fusion --from 2025-10-21 --to 2025-10-23
python -m qa.run_report --from 2025-10-21 --to 2025-10-23
```

#### Using TODAY/YESTERDAY Tokens

```bash
# Process yesterday's data (all steps)
python -m qa.run_schema --day YESTERDAY
python -m qa.run_ai --day YESTERDAY
python -m qa.run_fusion --day YESTERDAY
python -m qa.run_report --day YESTERDAY

# Process today's data
python -m qa.run_schema --day TODAY
python -m qa.run_ai --day TODAY
python -m qa.run_fusion --day TODAY
python -m qa.run_report --day TODAY
```

---

### Anomaly Clustering and Signal Quality

The QA sidecar uses intelligent clustering to reduce bursty duplicates while preserving true signals:

**Configuration:**
```yaml
qa:
  jump:
    k_sigma: 6.8              # Stricter threshold (was 6.0)
    cooldown_seconds: 5       # Cluster window: merge anomalies within 5s
    min_trade_count: 5        # Skip ultra-thin seconds (< 5 trades)
```

**How Clustering Works:**

1. **Burst De-duplication:** Anomalies from the same detector occurring within `cooldown_seconds` for the same symbol are merged into a single cluster
2. **Metadata Preservation:** Clustered records include a `cluster` field with:
   - `count`: Number of raw anomalies merged
   - `start_ts` / `end_ts`: Time range of the cluster
   - `max_abs_z`: Peak absolute Z-score within the cluster

3. **Signal Quality Improvements:**
   - Stricter threshold (6.8 vs 6.0) reduces false positives
   - Trade count gating (`min_trade_count`) skips ultra-thin seconds
   - Clustering reduces bursty duplicates by ~30-50%

**Example Output:**

Raw anomalies (before clustering):
- `2025-10-24T12:00:54` - Z-score: 7.2
- `2025-10-24T12:00:55` - Z-score: 6.9
- `2025-10-24T12:00:57` - Z-score: 8.0

Clustered output (single record):
```json
{
  "symbol": "BTCUSDT",
  "ts": "2025-10-24T12:00:54+00:00",
  "detector": "JUMP",
  "features": {"z_score": 7.2, ...},
  "cluster": {
    "count": 3,
    "start_ts": "2025-10-24T12:00:54+00:00",
    "end_ts": "2025-10-24T12:00:57+00:00",
    "max_abs_z": 8.0
  }
}
```

**Benefits:**
- Cleaner JSONL outputs for downstream analysis
- Reports show expanded counts (e.g., "36 anomalies" from 29 clustered records)
- Preserves raw Z-scores in `features.z_score` for forensics
- Backward compatible: non-clustered records have no `cluster` field

**Disabling Clustering:**
Set `cooldown_seconds: 0` to disable clustering entirely.

---

### Windows CMD Usage (Not PowerShell)

For Windows CMD users (not PowerShell), use the same commands:

```cmd
REM Install dependencies
pip install -r requirements-qa.txt

REM Run QA for yesterday
python -m qa.run_schema --day YESTERDAY
python -m qa.run_ai --day YESTERDAY
python -m qa.run_fusion --day YESTERDAY
python -m qa.run_report --day YESTERDAY

REM Run QA for specific date
python -m qa.run_schema --day 2025-10-23
python -m qa.run_ai --day 2025-10-23
python -m qa.run_fusion --day 2025-10-23
python -m qa.run_report --day 2025-10-23
```

### Automated Scheduling (Windows Task Scheduler)

Use provided XML files to set up automated QA runs.

#### Hourly QA (Every 90 minutes)

```powershell
schtasks /create /xml "tasks\qa_hourly.xml" /tn "CryptoLake\QA_Hourly"
```

#### Daily QA (00:15 UTC)

```powershell
schtasks /create /xml "tasks\qa_daily.xml" /tn "CryptoLake\QA_Daily"
```

---

## Data Contracts

### Violations JSONL

**Path:** `D:/CryptoDataLake/qa/schema/{date}_violations.jsonl`

**Format:**
```json
{
  "symbol": "SOLUSDT",
  "ts": "2025-10-23T12:34:56.789000+00:00",
  "rule": "R1_OHLC_ORDER",
  "severity": "critical",
  "detail": "OHLC ordering violated: low <= open,close <= high",
  "row_sample": {"open": 100.5, "high": 100.3, "low": 100.0, "close": 100.4}
}
```

**Severity Levels:**
- `critical`: R1, R2, R4, R5 (OHLC ordering, positive prices, NaN, continuity)
- `major`: R3, R6, negative volumes (ask/bid, spread sanity)
- `minor`: R7 (kline parity)

### Anomalies JSONL

**Path:** `D:/CryptoDataLake/qa/ai/{date}_anomalies.jsonl`

**Format:**
```json
{
  "symbol": "SOLUSDT",
  "ts": "2025-10-23T12:34:56.789000+00:00",
  "features": {"spread_bp": 120.5, "z_score": 8.3},
  "detector": "ZSCORE_SPREAD_BP",
  "label": "anomaly",
  "rationale": "spread_bp Z-score 8.30 exceeds threshold 5.0",
  "confidence": 0.85
}
```

**Detectors:**
- `ZSCORE_*`: Statistical outliers (spread, volume, VWAP drift)
- `JUMP`: Sudden price movements
- `IFOREST`: Multivariate anomalies

### Fusion Scores Parquet

**Path:** `D:/CryptoDataLake/qa/fusion/{date}_fusion.parquet`

**Schema:**
- `ts` (timestamp): Date timestamp
- `symbol` (string): Symbol name
- `verdict` (string): PASS / REVIEW / FAIL
- `score` (float): Fusion score [0, 1]
- `metadata` (string): JSON with counts and component scores

**Verdict Logic:**
- `PASS`: score ≥ 0.85 AND no critical violations
- `REVIEW`: 0.65 ≤ score < 0.85 OR has major violations
- `FAIL`: score < 0.65 OR has critical violations

**Score Formula:**
```
fusion_score = 0.7 * schema_score + 0.2 * detector_score + 0.1 * ai_confidence
```

### Daily Reports

**Path:** `D:/CryptoDataLake/reports/qa/{date}_qa_report.md`

Markdown format with:
- Summary statistics (violations, anomalies, verdicts)
- Fusion scores table by symbol
- Violations by rule
- Anomalies by detector
- Timestamps in both UTC and Europe/London

---

## Configuration Reference

### Core Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enable_ai` | bool | `true` | Enable AI/statistical detection |
| `hourly_window_min` | int | `90` | Window for hourly QA runs (minutes) |
| `daily_run_utc` | string | `"00:15"` | Daily run time (UTC, HH:MM) |
| `ai_labeler` | string | `"rules"` | Labeler type: rules/llm/hybrid |

### IsolationForest Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `n_estimators` | int | `200` | Number of trees |
| `contamination` | float | `0.005` | Expected anomaly proportion (0.5%) |
| `random_state` | int | `42` | Random seed for reproducibility |

### Z-Score Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `window` | string | `"1h"` | Rolling window for statistics |
| `k` | float | `5.0` | Z-score threshold |

### Jump Detector Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `k_sigma` | float | `6.0` | Jump threshold (std deviations) |
| `spread_stable_bps` | float | `50` | Max spread for "stable" (bps) |

---

## Troubleshooting

### No Data in Reports

**Symptom:** QA report shows "No data available"

**Causes & Fixes:**
1. **Schema violations file missing**: Run `python -m qa.run_schema --day {date}` first
2. **Parquet data missing**: Ensure transformer has processed the date
3. **Wrong date specified**: Check date format (YYYY-MM-DD) and timezone (UTC)

### High Anomaly Counts

**Symptom:** Hundreds of anomalies detected

**Causes & Fixes:**
1. **Contamination too high**: Lower `qa.iforest.contamination` in config (default: 0.005)
2. **Z-score threshold too low**: Increase `qa.zscore.k` (default: 5.0)
3. **Data quality issue**: Review violations first - anomalies on bad data are expected

### Tests Failing

**Symptom:** `pytest tests/qa/` fails

**Causes & Fixes:**
1. **Missing dependencies**: Run `pip install -r requirements-qa.txt`
2. **Version conflicts**: Check pandas, scikit-learn versions
3. **Windows path issues**: Use raw strings or forward slashes

### Permission Errors

**Symptom:** "Access denied" when writing outputs

**Causes & Fixes:**
1. **Directory permissions**: Ensure write access to `D:/CryptoDataLake/qa/`
2. **File locked**: Close any open reports/files before running QA
3. **Antivirus blocking**: Whitelist QA output directories

---

## Development

### Running Tests

```bash
# All QA tests (should complete in <5s)
python -m pytest tests/qa/ -v

# Specific test file
python -m pytest tests/qa/test_fusion.py -v

# With coverage
python -m pytest tests/qa/ --cov=qa --cov-report=html
```

### Adding New Detectors

1. Create detector class in `qa/ai/detectors.py`
2. Implement `detect(df, clean_mask)` method
3. Return list of anomaly dictionaries (JSONL contract)
4. Add tests in `tests/qa/test_detectors.py`
5. Update `qa/run_ai.py` to instantiate detector

### Adding New Validation Rules

1. Add rule function to `tools/validate_rules.py` (NOT qa/)
2. Update `qa/schema_validator.py` to call new rule
3. Add rule to `SEVERITY_MAP` in `qa/schema_validator.py`
4. Add tests in `tests/qa/test_schema.py`

---

## File Structure

```
crypto-lake/
├── qa/
│   ├── __init__.py
│   ├── config.py              # Config loader with defaults
│   ├── utils.py               # Atomic write helpers
│   ├── schema_validator.py   # R1-R7 adaptor
│   ├── run_schema.py          # Schema validation CLI
│   ├── fusion.py              # Fusion scoring logic
│   ├── run_fusion.py          # Fusion CLI
│   ├── reporting.py           # Report generator
│   ├── run_report.py          # Reporting CLI
│   ├── orchestrator.py        # QA orchestrator
│   └── ai/
│       ├── __init__.py
│       ├── detectors.py       # ZScore, Jump, IForest
│       ├── labeler.py         # RuleBased + LLM stub
│       └── run_ai.py          # AI detection CLI
├── tests/qa/
│   ├── test_config.py         # Config tests
│   ├── test_fusion.py         # Fusion logic tests
│   └── test_utils.py          # Utility tests
├── tasks/
│   ├── qa_hourly.xml          # Task Scheduler (hourly)
│   └── qa_daily.xml           # Task Scheduler (daily)
├── config.yml                 # Main config (includes qa section)
├── requirements-qa.txt        # QA dependencies
└── docs/
    └── qa_sidecar.md          # This file
```

---

## FAQ

**Q: Does QA block data ingestion?**
A: No. QA only reads existing Parquet files and writes to separate `qa/` directories.

**Q: How much data does QA process?**
A: Hourly mode processes last 90 minutes (configurable). Daily mode processes full previous day.

**Q: Can I run QA on historical data?**
A: Yes. Use `--from` and `--to` flags to process any date range with existing Parquet data.

**Q: What if QA detects critical violations?**
A: Review daily reports. Critical violations indicate data corruption and should be investigated immediately.

**Q: How do I disable AI detection?**
A: Set `qa.enable_ai: false` in `config.yml`. Schema validation will still run.

**Q: Can I customize detector thresholds?**
A: Yes. Edit `qa.zscore.k`, `qa.jump.k_sigma`, or `qa.iforest.contamination` in `config.yml`.

**Q: Where are logs stored?**
A: `D:/CryptoDataLake/logs/qa/qa_{module}.log` with 14-day rotation.

---

## SQL Database Integration

The QA sidecar supports optional SQL database backends for centralized storage and analytics.

### Overview

By default, Crypto Lake uses **DuckDB in-memory** for QA operations, querying Parquet files directly. For production deployments, you can optionally configure a **persistent SQL database** (PostgreSQL, SQLite, or DuckDB on-disk) to:

- Centralize data across multiple collectors
- Enable shared analytics dashboards
- Support remote queries (e.g., Grafana, Metabase)
- Scale to cloud deployments (Google Cloud SQL, AWS RDS)

**Key Features:**
- Backward compatible: Falls back to DuckDB if database not configured
- Portable schema: Works with SQLite, PostgreSQL, and DuckDB
- Automated migrations: Export DuckDB views → SQL tables
- Integrity verification: Built-in row count checks

---

### Database Configuration

Add a `database:` section to `config.yml`:

#### Option 1: Full Connection String (Recommended)

```yaml
database:
  # DuckDB (on-disk)
  url: "duckdb:///D:/CryptoDataLake/crypto_lake.duckdb"

  # SQLite (local file)
  url: "sqlite:///D:/CryptoDataLake/crypto_lake.db"

  # PostgreSQL (local)
  url: "postgresql://crypto_user:password@localhost:5432/crypto_lake"

  # Google Cloud SQL (Unix socket)
  url: "postgresql://user:pass@/crypto_lake?host=/cloudsql/project:region:instance"
```

#### Option 2: Separate Credential Fields

```yaml
database:
  type: "postgres"                # Database type: postgres, sqlite, duckdb
  host: "localhost"               # Database host
  port: 5432                      # Database port
  user: "crypto_user"             # Database username
  password: "secure_password"     # Database password
  database: "crypto_lake"         # Database name

  # Connection pool settings (optional)
  pool_size: 5                    # Number of connections in pool
  max_overflow: 10                # Maximum overflow connections
  pool_timeout: 30                # Pool timeout in seconds
```

**Precedence:** If both formats are provided, `url` takes precedence. Credentials in `url` override separate fields.

---

### Schema Initialization

#### 1. Apply Schema to SQL Database

```bash
# Using Python API
python -c "from tools.sql_manager import init_database, apply_schema; \
  engine = init_database('auto', 'config.yml'); \
  apply_schema(engine)"
```

**Tables Created:**
- `bars_1s` - 1-second OHLCV bars (symbol, ts, open, high, low, close, volume_base, volume_quote, trade_count, vwap, bid, ask, spread)
- `bars_1m` - 1-minute OHLCV bars (aggregated from bars_1s)
- `klines_1m` - 1-minute klines from REST API (for comparison)
- `compare_our_vs_kline_1m` - Comparison table (our bars vs klines)
- `funding_oi_hourly` - Funding rates and open interest (hourly)
- `macro_minute` - Macro/FX data (SPY, UUP, ES=F, EURUSD=X, etc.)

**Primary Keys:** All tables use composite primary keys on `(symbol, ts)` or `(macro_key, ts)` for efficient time-series queries.

**Indexes:** Automatically created for:
- `idx_{table}_symbol_ts` - Symbol + timestamp composite index
- `idx_{table}_ts` - Timestamp-only index for time-range queries

#### 2. Verify Schema Integrity

```bash
# Using Python API
python -c "from tools.sql_manager import init_database, verify_integrity; \
  engine = init_database('auto', 'config.yml'); \
  all_ok, missing = verify_integrity(engine); \
  print('✓ All tables present' if all_ok else f'✗ Missing: {missing}')"
```

---

### Migrating Data from DuckDB to SQL

Use `tools/migrate_sql.py` to export DuckDB views to SQL database.

#### Full Migration (DuckDB → SQL)

```bash
# Migrate all tables from DuckDB to PostgreSQL
python -m tools.migrate_sql --mode migrate --config config.yml

# Dry-run mode (preview operations without executing)
python -m tools.migrate_sql --mode migrate --config config.yml --dry-run
```

**How it works:**
1. Connects to DuckDB and registers Parquet-backed views
2. Exports each view to temporary Parquet files
3. Imports Parquet files to SQL database using bulk insert
4. Uses PostgreSQL COPY for optimal performance (100K+ rows/sec)

#### Step-by-Step Migration

```bash
# Step 1: Export DuckDB views to Parquet
python -m tools.migrate_sql \
  --mode export \
  --config config.yml \
  --output D:/CryptoDataLake/export/

# Step 2: Import Parquet files to SQL database
python -m tools.migrate_sql \
  --mode import \
  --config config.yml \
  --input D:/CryptoDataLake/export/
```

#### Verify Migration Integrity

```bash
# Compare row counts between DuckDB and SQL
python -m tools.migrate_sql --mode verify --config config.yml
```

**Output:**
```
============================================================
MIGRATION VERIFICATION SUMMARY
============================================================
Table                          Source          Target          Status
------------------------------------------------------------
bars_1s                        3,456,789       3,456,789       ✓ MATCH
bars_1m                        57,612          57,612          ✓ MATCH
klines_1m                      57,612          57,612          ✓ MATCH
compare_our_vs_kline_1m        57,612          57,612          ✓ MATCH
funding_oi_hourly              240             240             ✓ MATCH
macro_minute                   10,080          10,080          ✓ MATCH
============================================================
Overall: PASSED
============================================================
```

---

### Supported Databases

| Database | Support Level | Notes |
|----------|--------------|-------|
| **DuckDB** | ✅ Full | In-memory or on-disk, Parquet-backed views |
| **SQLite** | ✅ Full | Local file, good for small deployments |
| **PostgreSQL** | ✅ Full | Recommended for production, supports partitioning |
| **Google Cloud SQL** | ✅ Full | PostgreSQL-compatible, use Unix socket connection |
| **AWS RDS** | ✅ Full | PostgreSQL-compatible |
| **MySQL** | ⚠️ Not tested | May work via SQLAlchemy, not officially supported |

---

### PostgreSQL Partitioning (Optional)

For high-volume production deployments, enable table partitioning by time.

**Schema Hints:** `sql/schema.sql` includes commented partitioning hints:

```sql
CREATE TABLE IF NOT EXISTS bars_1s (
    symbol TEXT NOT NULL,
    ts TIMESTAMP NOT NULL,
    ...
    PRIMARY KEY (symbol, ts)
); -- PARTITION BY RANGE (ts);

-- Uncomment to enable monthly partitioning:
-- CREATE TABLE bars_1s_2025_01 PARTITION OF bars_1s
--   FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
```

**Benefits:**
- Faster time-range queries (prune irrelevant partitions)
- Easier data retention (drop old partitions instead of DELETE)
- Parallel query execution (PostgreSQL 11+)

**Setup:**
1. Uncomment partitioning hints in `sql/schema.sql`
2. Create partitions for each month/day
3. Set up automated partition management (e.g., `pg_partman`)

---

### Use Cases

#### 1. Centralized Analytics Dashboard

**Scenario:** Multiple collectors writing to shared PostgreSQL database

**Setup:**
```yaml
# On all collector nodes
database:
  url: "postgresql://user:pass@analytics.example.com:5432/crypto_lake"
```

**Benefits:**
- Single source of truth for all symbols
- Grafana/Metabase can query directly
- No need to consolidate Parquet files

#### 2. Cloud-Native Deployment

**Scenario:** Running on Google Cloud Platform with Cloud SQL

**Setup:**
```yaml
database:
  url: "postgresql://user:pass@/crypto_lake?host=/cloudsql/project-id:us-central1:crypto-db"
```

**Benefits:**
- Managed backups and high availability
- Automatic failover and read replicas
- Integrates with BigQuery for analytics

#### 3. Local SQLite for Testing

**Scenario:** Testing QA pipelines without PostgreSQL

**Setup:**
```yaml
database:
  url: "sqlite:///D:/CryptoDataLake/test.db"
```

**Benefits:**
- Zero configuration
- Self-contained database file
- Perfect for CI/CD tests

---

### Testing SQL Integration

Run the SQL integration test suite:

```bash
# All SQL tests (schema, integrity, migration)
python -m pytest tests/sql/ -v

# Expected output (all tests should pass):
# tests/sql/test_schema.py::test_apply_schema_creates_tables PASSED
# tests/sql/test_schema.py::test_apply_schema_idempotent PASSED
# tests/sql/test_schema.py::test_table_columns_correct PASSED
# tests/sql/test_schema.py::test_primary_keys_created PASSED
# tests/sql/test_schema.py::test_indexes_created PASSED
# tests/sql/test_schema.py::test_verify_integrity_pass PASSED
# ... (13 tests total)
```

---

### Troubleshooting SQL Integration

#### Database Connection Fails

**Symptom:** `Failed to connect to postgres: could not connect to server`

**Fixes:**
1. Verify connection string in `config.yml`
2. Check database is running: `psql -h localhost -U crypto_user -d crypto_lake`
3. Check firewall allows connections on port 5432
4. Verify credentials are correct

#### Schema Creation Fails

**Symptom:** `Failed to apply schema: relation "bars_1s" already exists`

**Fixes:**
1. This is normal - schema creation is idempotent
2. Run `verify_integrity()` to confirm tables exist
3. If tables are corrupted, drop and recreate: `DROP TABLE bars_1s CASCADE; apply_schema()`

#### Migration Slow on Large Datasets

**Symptom:** Migration takes hours for millions of rows

**Fixes:**
1. Use PostgreSQL COPY instead of INSERT (automatic for large datasets)
2. Increase batch size: `--batch-size 500000`
3. Disable indexes during import, rebuild after
4. Use `--dry-run` to estimate time before running

#### Row Count Mismatch After Migration

**Symptom:** Verification shows `bars_1s: 1,000,000 → 999,500 rows (MISMATCH)`

**Fixes:**
1. Check for duplicate primary keys (migration skips duplicates)
2. Check database constraints (may reject invalid rows)
3. Re-run migration with clean target database
4. Review migration logs for errors

---

## Support

For issues or questions:
1. Check this documentation
2. Review logs in `D:/CryptoDataLake/logs/qa/`
3. Run tests: `python -m pytest tests/qa/ -v`
4. For SQL issues: `python -m pytest tests/sql/ -v`
5. Check recent QA reports for patterns

---

*QA Sidecar v1.0.0*
