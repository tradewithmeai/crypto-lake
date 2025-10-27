# Cloud SQL Migration Rehearsal Guide

This document provides a step-by-step rehearsal plan for migrating the Crypto Lake data warehouse from local DuckDB to Google Cloud SQL (PostgreSQL). The rehearsal uses local SQLite for safe testing before committing to production.

## Migration Overview

**Source:** DuckDB (local analytical database with Parquet views)
**Target:** PostgreSQL on Google Cloud SQL
**Method:** Export → Parquet intermediate → Import with verification
**Safety:** Dry-run mode, local rehearsal, row-count verification

---

## Prerequisites

1. **Local Environment**
   - Python 3.11+ with virtual environment activated
   - All dependencies installed: `pip install -r requirements.txt`
   - DuckDB database populated with views: `tools/db.py` registered
   - Local Parquet data available in configured `base_path`

2. **GCP Environment** (for production migration)
   - PostgreSQL instance provisioned in Cloud SQL
   - Cloud SQL Proxy configured (for secure connections)
   - Connection credentials stored in environment variables
   - Network access configured (allow Cloud Run IPs)

---

## Phase 1: Dry-Run Rehearsal (Preview Only)

Test the migration process without writing any data. This phase validates configuration, connectivity, and table schemas.

### Step 1.1: Dry-Run Export from DuckDB

Preview what would be exported from DuckDB to Parquet intermediate files:

```bash
python -m tools.migrate_sql \
  --mode export \
  --config config.yml \
  --output ./migration-test \
  --dry-run
```

**Expected Output:**
```
INFO | Starting export to ./migration-test
INFO | Found 1,234,567 rows in bars_1s
INFO | [DRY-RUN] Would export bars_1s → ./migration-test/bars_1s.parquet
INFO | Found 20,576 rows in bars_1m
INFO | [DRY-RUN] Would export bars_1m → ./migration-test/bars_1m.parquet
...
INFO | Export complete: 6/6 tables
```

**Validation:**
- All 6 tables should be found: `bars_1s`, `bars_1m`, `klines_1m`, `compare_our_vs_kline_1m`, `funding_oi_hourly`, `macro_minute`
- Row counts should match expected data volumes
- No errors or missing views

---

### Step 1.2: Dry-Run Import to SQLite

Preview what would be imported from Parquet files to SQLite (production target will be PostgreSQL):

```bash
# Configure SQLite as target database
export CRYPTO_DB_URL="sqlite:///./migration-rehearsal.db"

python -m tools.migrate_sql \
  --mode import \
  --config config.yml \
  --input ./migration-test \
  --dry-run
```

**Expected Output:**
```
INFO | Starting import from ./migration-test
INFO | Using database URL from CRYPTO_DB_URL environment variable
INFO | Initializing SQLITE database: sqlite:///./migration-rehearsal.db
INFO | Found 1,234,567 rows in bars_1s.parquet
INFO | [DRY-RUN] Would import bars_1s.parquet → bars_1s (1,234,567 rows)
...
INFO | Import complete: 6/6 tables
```

**Validation:**
- Environment variable `CRYPTO_DB_URL` is correctly recognised
- Schema application would succeed (check for no schema errors)
- All Parquet files found and readable

---

### Step 1.3: Dry-Run Full Migration

Preview the entire end-to-end migration process:

```bash
export CRYPTO_DB_URL="sqlite:///./migration-rehearsal.db"

python -m tools.migrate_sql \
  --mode migrate \
  --config config.yml \
  --dry-run
```

**Expected Output:**
```
INFO | Starting direct migration from DuckDB to SQL
INFO | Step 1/2: Exporting from DuckDB...
INFO | [DRY-RUN] Would export bars_1s → ...
...
INFO | Step 2/2: Importing to SQL database...
INFO | [DRY-RUN] Would import bars_1s.parquet → bars_1s (1,234,567 rows)
...
INFO | Migration complete: 6/6 tables
```

**Validation:**
- Both export and import phases complete successfully
- Total row counts match between source and target
- No errors or warnings (except for missing tables, which is expected in dry-run)

---

## Phase 2: Local Rehearsal (SQLite)

Execute a real migration to local SQLite to validate the complete process with data integrity verification.

### Step 2.1: Real Export from DuckDB

Export DuckDB views to Parquet intermediate files:

```bash
# Clean up previous test
rm -rf ./migration-test

python -m tools.migrate_sql \
  --mode export \
  --config config.yml \
  --output ./migration-test
```

**Expected Output:**
```
INFO | Starting export to ./migration-test
INFO | Found 1,234,567 rows in bars_1s
INFO | Exported 1,234,567 rows to ./migration-test/bars_1s.parquet
...
INFO | Export complete: 6/6 tables
```

**Validation:**
- Check Parquet files exist: `ls -lh ./migration-test/*.parquet`
- Verify file sizes are non-zero
- Verify row counts in logs match expectations

---

### Step 2.2: Real Import to SQLite

Import Parquet files to local SQLite database:

```bash
# Clean up previous database
rm -f ./migration-rehearsal.db

# Configure SQLite as target
export CRYPTO_DB_URL="sqlite:///./migration-rehearsal.db"

python -m tools.migrate_sql \
  --mode import \
  --config config.yml \
  --input ./migration-test
```

**Expected Output:**
```
INFO | Starting import from ./migration-test
INFO | Applying schema to target database...
INFO | Schema applied successfully to sqlite
INFO | Using pandas to_sql for bars_1s
INFO | Imported 1,234,567 rows to bars_1s
...
INFO | Import complete: 6/6 tables
```

**Validation:**
- Check database file exists: `ls -lh ./migration-rehearsal.db`
- Verify database size is reasonable (should be similar to DuckDB size)
- Check for import errors in logs

---

### Step 2.3: Verify Migration Integrity

Run verification to compare row counts between source (DuckDB) and target (SQLite):

```bash
export CRYPTO_DB_URL="sqlite:///./migration-rehearsal.db"

python -m tools.migrate_sql \
  --mode verify \
  --config config.yml
```

**Expected Output:**
```
INFO | Starting migration verification
INFO | Verifying migration integrity...
INFO |   ✓ bars_1s: 1,234,567 rows (match)
INFO |   ✓ bars_1m: 20,576 rows (match)
INFO |   ✓ klines_1m: 20,576 rows (match)
INFO |   ✓ compare_our_vs_kline_1m: 20,576 rows (match)
INFO |   ✓ funding_oi_hourly: 720 rows (match)
INFO |   ✓ macro_minute: 1,440 rows (match)
INFO | Migration verification: PASSED

============================================================
MIGRATION VERIFICATION SUMMARY
============================================================
Table                          Source          Target          Status
------------------------------------------------------------
bars_1s                        1,234,567       1,234,567       ✓ MATCH
bars_1m                        20,576          20,576          ✓ MATCH
klines_1m                      20,576          20,576          ✓ MATCH
compare_our_vs_kline_1m        20,576          20,576          ✓ MATCH
funding_oi_hourly              720             720             ✓ MATCH
macro_minute                   1,440           1,440           ✓ MATCH
============================================================
Overall: PASSED
============================================================
```

**Validation:**
- All tables show "✓ MATCH" status
- Source and target row counts are identical
- Overall verification status is "PASSED"
- **If verification fails:** Check logs for errors, re-run import, or investigate data corruption

---

### Step 2.4: Spot-Check Data Quality

Manually verify a sample of migrated data:

```bash
# Use DuckDB to query SQLite (yes, DuckDB can query SQLite!)
python -c "
import duckdb
conn = duckdb.connect()
conn.execute(\"ATTACH 'migration-rehearsal.db' AS sqlite_db (TYPE SQLITE)\")

# Check bars_1s sample
print('=== bars_1s Sample ===')
result = conn.execute(\"\"\"
    SELECT symbol, ts, open, high, low, close, volume
    FROM sqlite_db.bars_1s
    ORDER BY ts DESC
    LIMIT 5
\"\"\").fetchdf()
print(result)

# Check for NULL values
print('\n=== NULL Check ===')
null_check = conn.execute(\"\"\"
    SELECT
        COUNT(*) as total_rows,
        SUM(CASE WHEN open IS NULL THEN 1 ELSE 0 END) as null_open,
        SUM(CASE WHEN close IS NULL THEN 1 ELSE 0 END) as null_close
    FROM sqlite_db.bars_1s
\"\"\").fetchdf()
print(null_check)
"
```

**Validation:**
- Sample data looks correct (prices, volumes, timestamps)
- No unexpected NULL values
- Timestamps are in correct format (Unix epoch ms)

---

## Phase 3: Production Migration (Cloud SQL PostgreSQL)

Once local rehearsal is validated, execute production migration to Google Cloud SQL.

### Step 3.1: Configure Cloud SQL Connection

Set up environment variables for secure Cloud SQL connection:

```bash
# Option 1: Direct connection string (with Cloud SQL Proxy)
export CRYPTO_DB_URL="postgresql+pg8000://username:password@/crypto_lake?host=/cloudsql/project-id:region:instance-name"

# Option 2: Individual credentials (for cloud-native auth)
export CRYPTO_DB_USER="crypto_user"
export CRYPTO_DB_PASS="$(gcloud secrets versions access latest --secret=crypto-db-password)"
export CRYPTO_DB_HOST="/cloudsql/project-id:region:instance-name"
export CRYPTO_DB_NAME="crypto_lake"

# Verify connection (optional, requires psql)
psql "$CRYPTO_DB_URL" -c "SELECT version();"
```

**Validation:**
- Connection string format is correct (pg8000 driver for Cloud SQL)
- Credentials are valid and not hard-coded
- Test connection succeeds

---

### Step 3.2: Production Dry-Run

Test production configuration with dry-run mode:

```bash
python -m tools.migrate_sql \
  --mode migrate \
  --config config.yml \
  --dry-run
```

**Expected Output:**
```
INFO | Built database URL from CRYPTO_DB_* environment variables
INFO | Initializing POSTGRESQL database: postgresql+pg8000://crypto_user:****@...
INFO | Connected to POSTGRESQL database successfully
INFO | [DRY-RUN] Would export bars_1s → ...
INFO | [DRY-RUN] Would import bars_1s.parquet → bars_1s (1,234,567 rows)
...
INFO | Migration complete: 6/6 tables
```

**Validation:**
- PostgreSQL connection succeeds (not SQLite)
- Credentials are masked in logs (`****`)
- No schema or permission errors

---

### Step 3.3: Production Migration

Execute the real production migration:

```bash
# Set connection string (ensure it's PostgreSQL, not SQLite!)
export CRYPTO_DB_URL="postgresql+pg8000://username:password@/crypto_lake?host=/cloudsql/project-id:region:instance-name"

# Run migration
python -m tools.migrate_sql \
  --mode migrate \
  --config config.yml \
  --batch-size 100000
```

**Expected Output:**
```
INFO | Starting direct migration from DuckDB to SQL
INFO | Connected to POSTGRESQL database successfully
INFO | Step 1/2: Exporting from DuckDB...
INFO | Exported 1,234,567 rows to /tmp/.../bars_1s.parquet
...
INFO | Step 2/2: Importing to SQL database...
INFO | Using PostgreSQL COPY for bars_1s
INFO | Imported 1,234,567 rows to bars_1s via COPY
...
INFO | Migration complete: 6/6 tables
```

**Validation:**
- PostgreSQL COPY method is used (fastest bulk insert)
- All 6 tables imported successfully
- No transaction rollbacks or errors

---

### Step 3.4: Production Verification

Verify production migration integrity:

```bash
export CRYPTO_DB_URL="postgresql+pg8000://username:password@/crypto_lake?host=/cloudsql/project-id:region:instance-name"

python -m tools.migrate_sql \
  --mode verify \
  --config config.yml
```

**Expected Output:**
```
INFO | Migration verification: PASSED
============================================================
MIGRATION VERIFICATION SUMMARY
============================================================
Table                          Source          Target          Status
------------------------------------------------------------
bars_1s                        1,234,567       1,234,567       ✓ MATCH
bars_1m                        20,576          20,576          ✓ MATCH
...
============================================================
Overall: PASSED
============================================================
```

**Validation:**
- All tables show "✓ MATCH"
- Row counts are identical
- Overall verification status is "PASSED"
- **If verification fails:** Rollback migration, investigate, and retry

---

### Step 3.5: Production Smoke Test

Run smoke tests against the production database:

```bash
export CRYPTO_DB_URL="postgresql+pg8000://username:password@/crypto_lake?host=/cloudsql/project-id:region:instance-name"

# Run SQL integration tests
pytest tests/sql/ -v

# Test sample queries
python -c "
from tools.sql_manager import init_database
from sqlalchemy import text

engine = init_database('auto', config_path='config.yml')
with engine.connect() as conn:
    # Test query performance
    result = conn.execute(text(\"\"\"
        SELECT symbol, AVG(close) as avg_price
        FROM bars_1m
        WHERE ts >= EXTRACT(EPOCH FROM NOW() - INTERVAL '1 day') * 1000
        GROUP BY symbol
    \"\"\")).fetchall()
    print('Average prices (last 24h):', result)
"
```

**Validation:**
- SQL tests pass against PostgreSQL
- Query performance is acceptable
- Indexes are working (check with `EXPLAIN ANALYZE`)

---

## Phase 4: Post-Migration Cleanup

### Step 4.1: Document Production Configuration

Record production database details for operational use:

```bash
# Add to Cloud Run environment variables (via gcloud or Console)
gcloud run services update crypto-lake-orchestrator \
  --set-env-vars="CRYPTO_DB_URL=postgresql+pg8000://username:password@/crypto_lake?host=/cloudsql/project-id:region:instance-name" \
  --region=europe-west2
```

---

### Step 4.2: Clean Up Rehearsal Artifacts

Remove local rehearsal databases and temporary files:

```bash
# Remove SQLite rehearsal database
rm -f ./migration-rehearsal.db

# Remove Parquet intermediate files
rm -rf ./migration-test

# Remove test databases
rm -f ./ci-test.db ./test_env.db
```

---

### Step 4.3: Monitor Production Database

After migration, monitor database health:

```bash
# Check table sizes
psql "$CRYPTO_DB_URL" -c "\dt+"

# Check index usage
psql "$CRYPTO_DB_URL" -c "\di+"

# Check slow queries (Cloud SQL Console)
# → Performance Insights → Query Insights → Slow queries

# Check connection pool health (Cloud SQL Console)
# → Monitoring → Connections → Active connections
```

---

## Rollback Plan

If production migration fails or causes issues:

1. **Immediate Rollback:**
   ```bash
   # Revert config.yml to use DuckDB
   # (Comment out database.url, use default DuckDB)

   # Restart services with DuckDB config
   gcloud run services update crypto-lake-orchestrator \
     --clear-env-vars="CRYPTO_DB_URL" \
     --region=europe-west2
   ```

2. **Investigate and Retry:**
   - Review migration logs for errors
   - Check Cloud SQL permissions and quotas
   - Verify network connectivity from Cloud Run
   - Re-run local rehearsal with updated data

3. **Data Recovery:**
   - DuckDB remains authoritative source (Parquet files)
   - Re-export from DuckDB if needed
   - PostgreSQL is a replica, not the primary data store

---

## Troubleshooting

### Issue: "No database.url configured in config.yml"

**Cause:** Environment variable `CRYPTO_DB_URL` is not set, and config.yml has no database section.

**Fix:**
```bash
export CRYPTO_DB_URL="sqlite:///./migration-rehearsal.db"
# or
export CRYPTO_DB_URL="postgresql+pg8000://username:password@/crypto_lake?host=/cloudsql/..."
```

---

### Issue: "Failed to connect to POSTGRESQL database"

**Cause:** Cloud SQL Proxy not running, or incorrect connection string.

**Fix:**
```bash
# Start Cloud SQL Proxy
cloud_sql_proxy -instances=project-id:region:instance-name=tcp:5432 &

# Update connection string to use TCP
export CRYPTO_DB_URL="postgresql+psycopg2://username:password@localhost:5432/crypto_lake"
```

---

### Issue: "Verification failed: Row count mismatch"

**Cause:** Data loss during import, or incomplete transaction.

**Fix:**
```bash
# Drop and re-import affected table
psql "$CRYPTO_DB_URL" -c "DROP TABLE bars_1s;"
python -m tools.migrate_sql --mode import --input ./migration-test

# Re-run verification
python -m tools.migrate_sql --mode verify
```

---

### Issue: "Import too slow (hours for large tables)"

**Cause:** SQLite uses pandas to_sql (slow), PostgreSQL should use COPY.

**Fix:**
- Verify PostgreSQL connection string (not SQLite)
- Check logs for "Using PostgreSQL COPY" message
- Increase `--batch-size` for larger batches (e.g., 500000)
- Use Cloud SQL high-performance tier for migration

---

## Summary Checklist

- [ ] Phase 1: Dry-run rehearsal completed successfully
- [ ] Phase 2: Local SQLite migration verified with row counts
- [ ] Phase 3.1: Cloud SQL connection configured and tested
- [ ] Phase 3.2: Production dry-run completed
- [ ] Phase 3.3: Production migration executed
- [ ] Phase 3.4: Production verification PASSED
- [ ] Phase 3.5: Smoke tests passed
- [ ] Phase 4: Post-migration cleanup completed
- [ ] Rollback plan documented and tested
- [ ] Monitoring configured for production database

---

**Next Steps After Migration:**
1. Update README.md with production database configuration
2. Configure Cloud Run services to use `CRYPTO_DB_URL` env var
3. Set up Cloud SQL backups and maintenance windows
4. Implement database connection pooling (pgBouncer or Cloud SQL Proxy)
5. Migrate CI/CD pipelines to use PostgreSQL for integration tests
6. Archive DuckDB as historical backup (optional)

**End of Migration Rehearsal Guide**
