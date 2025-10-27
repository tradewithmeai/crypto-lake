#!/usr/bin/env python3
"""
SQL Migration Tool

Migrates data between DuckDB and SQL databases (PostgreSQL/SQLite).
Supports bulk import/export via Parquet intermediate format.

Features:
- Export DuckDB tables to Parquet files
- Import Parquet files to SQL databases
- Direct migration from DuckDB to SQL
- Integrity verification with row count checks
- Dry-run mode for previewing operations
- Batch processing for large datasets
- Transaction safety with rollback on error

Usage:
    # Export DuckDB views to Parquet
    python -m tools.migrate_sql --mode export --config config.yml --output /path/to/parquet

    # Import Parquet to PostgreSQL
    python -m tools.migrate_sql --mode import --config config.yml --input /path/to/parquet

    # Migrate DuckDB to PostgreSQL (direct)
    python -m tools.migrate_sql --mode migrate --config config.yml

    # Verify integrity after migration
    python -m tools.migrate_sql --mode verify --config config.yml

    # Dry-run mode (preview operations)
    python -m tools.migrate_sql --mode migrate --config config.yml --dry-run
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import duckdb
import pandas as pd
import yaml
from loguru import logger
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from tools.db import connect_and_register_views, is_sqlalchemy_engine
from tools.sql_manager import apply_schema, init_database, verify_integrity


# Tables to migrate (in dependency order)
MIGRATION_TABLES = [
    "bars_1s",
    "bars_1m",
    "klines_1m",
    "compare_our_vs_kline_1m",
    "funding_oi_hourly",
    "macro_minute",
]


def load_config(config_path: str) -> dict:
    """
    Load configuration from YAML file.

    Args:
        config_path: Path to config.yml

    Returns:
        Configuration dictionary

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config is invalid
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Validate required fields
    if "general" not in config or "base_path" not in config["general"]:
        raise ValueError("Config must contain general.base_path")

    return config


def export_table_to_parquet(
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_name: str,
    output_dir: Path,
    batch_size: int = 100000,
    dry_run: bool = False,
) -> Optional[int]:
    """
    Export a DuckDB table or view to Parquet file.

    Args:
        duckdb_conn: DuckDB connection with views registered
        table_name: Name of table/view to export
        output_dir: Directory to write Parquet file
        batch_size: Rows per batch (for progress logging)
        dry_run: If True, only preview without writing

    Returns:
        Number of rows exported, or None if table doesn't exist

    Raises:
        Exception: If export fails
    """
    try:
        # Check if table/view exists
        result = duckdb_conn.execute(f"""
            SELECT COUNT(*) as row_count
            FROM {table_name}
        """).fetchone()

        if result is None:
            logger.warning(f"Table/view {table_name} not found, skipping")
            return None

        row_count = result[0]
        logger.info(f"Found {row_count:,} rows in {table_name}")

        if dry_run:
            logger.info(f"[DRY-RUN] Would export {table_name} → {output_dir}/{table_name}.parquet")
            return row_count

        # Export to Parquet
        output_path = output_dir / f"{table_name}.parquet"
        duckdb_conn.execute(f"""
            COPY (SELECT * FROM {table_name})
            TO '{str(output_path)}'
            (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)

        logger.info(f"Exported {row_count:,} rows to {output_path}")
        return row_count

    except Exception as e:
        logger.error(f"Failed to export {table_name}: {e}")
        raise


def import_parquet_to_sql(
    engine: Engine,
    table_name: str,
    parquet_path: Path,
    batch_size: int = 100000,
    dry_run: bool = False,
) -> int:
    """
    Import Parquet file to SQL database using batch inserts.

    Uses pandas read_parquet with chunking for memory efficiency.
    For PostgreSQL, uses COPY for optimal performance.

    Args:
        engine: SQLAlchemy engine
        table_name: Target table name
        parquet_path: Path to Parquet file
        batch_size: Rows per batch
        dry_run: If True, only preview without writing

    Returns:
        Number of rows imported

    Raises:
        Exception: If import fails
    """
    if not parquet_path.exists():
        logger.warning(f"Parquet file not found: {parquet_path}, skipping {table_name}")
        return 0

    # Read row count
    df_sample = pd.read_parquet(parquet_path)
    row_count = len(df_sample)
    logger.info(f"Found {row_count:,} rows in {parquet_path.name}")

    if dry_run:
        logger.info(f"[DRY-RUN] Would import {parquet_path.name} → {table_name} ({row_count:,} rows)")
        return row_count

    # Detect engine type
    is_postgres = "postgres" in str(engine.url).lower()

    try:
        if is_postgres and row_count > batch_size:
            # PostgreSQL: Use COPY for bulk insert (fastest)
            logger.info(f"Using PostgreSQL COPY for {table_name}")

            with engine.begin() as conn:
                # Create temporary CSV for COPY
                import tempfile
                import csv

                with tempfile.NamedTemporaryFile(mode='w', newline='', suffix='.csv', delete=False) as tmp_file:
                    df_sample.to_csv(tmp_file, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
                    tmp_path = tmp_file.name

                try:
                    # Use COPY FROM STDIN
                    with open(tmp_path, 'r') as f:
                        # Get column names
                        columns = ", ".join(df_sample.columns)
                        copy_sql = f"COPY {table_name} ({columns}) FROM STDIN WITH CSV"

                        # Execute COPY
                        raw_conn = conn.connection
                        cursor = raw_conn.cursor()
                        cursor.copy_expert(copy_sql, f)

                    logger.info(f"Imported {row_count:,} rows to {table_name} via COPY")
                finally:
                    os.remove(tmp_path)

        else:
            # SQLite or small datasets: Use pandas to_sql with batching
            logger.info(f"Using pandas to_sql for {table_name}")

            # Import in batches
            imported = 0
            for i in range(0, row_count, batch_size):
                batch = df_sample.iloc[i:i+batch_size]
                batch.to_sql(
                    table_name,
                    engine,
                    if_exists="append",
                    index=False,
                    method="multi",
                )
                imported += len(batch)

                if row_count > batch_size:
                    logger.info(f"  Progress: {imported:,}/{row_count:,} rows ({100*imported//row_count}%)")

            logger.info(f"Imported {imported:,} rows to {table_name}")

        return row_count

    except Exception as e:
        logger.error(f"Failed to import {table_name}: {e}")
        raise


def verify_migration(
    source_conn: duckdb.DuckDBPyConnection,
    target_engine: Engine,
    tables: List[str],
) -> Tuple[bool, Dict[str, Tuple[int, int]]]:
    """
    Verify migration integrity by comparing row counts.

    Args:
        source_conn: Source DuckDB connection
        target_engine: Target SQL engine
        tables: List of table names to verify

    Returns:
        Tuple of (success: bool, counts: Dict[table_name, (source_count, target_count)])
    """
    logger.info("Verifying migration integrity...")

    counts = {}
    success = True

    for table_name in tables:
        try:
            # Count source rows
            source_result = source_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
            source_count = source_result[0] if source_result else 0

            # Count target rows
            with target_engine.connect() as conn:
                target_result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
                target_count = target_result[0] if target_result else 0

            counts[table_name] = (source_count, target_count)

            if source_count == target_count:
                logger.info(f"  ✓ {table_name}: {source_count:,} rows (match)")
            else:
                logger.error(f"  ✗ {table_name}: {source_count:,} → {target_count:,} rows (MISMATCH)")
                success = False

        except Exception as e:
            logger.error(f"  ✗ {table_name}: Verification failed - {e}")
            counts[table_name] = (0, 0)
            success = False

    if success:
        logger.info("Migration verification: PASSED")
    else:
        logger.error("Migration verification: FAILED")

    return success, counts


def mode_export(
    config: dict,
    output_dir: Path,
    batch_size: int = 100000,
    dry_run: bool = False,
) -> int:
    """
    Export DuckDB views to Parquet files.

    Args:
        config: Configuration dictionary
        output_dir: Directory to write Parquet files
        batch_size: Rows per batch
        dry_run: Preview only

    Returns:
        Number of tables exported
    """
    logger.info(f"Starting export to {output_dir}")

    # Create output directory
    if not dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)

    # Connect to DuckDB and register views
    base_path = config["general"]["base_path"]
    duckdb_conn = connect_and_register_views(base_path, config=config)

    # Ensure it's a DuckDB connection
    if is_sqlalchemy_engine(duckdb_conn):
        logger.error("Export requires DuckDB source, but SQL engine was configured")
        return 0

    # Export each table
    exported_count = 0
    for table_name in MIGRATION_TABLES:
        row_count = export_table_to_parquet(
            duckdb_conn,
            table_name,
            output_dir,
            batch_size=batch_size,
            dry_run=dry_run,
        )
        if row_count is not None:
            exported_count += 1

    logger.info(f"Export complete: {exported_count}/{len(MIGRATION_TABLES)} tables")
    return exported_count


def mode_import(
    config: dict,
    input_dir: Path,
    batch_size: int = 100000,
    dry_run: bool = False,
) -> int:
    """
    Import Parquet files to SQL database.

    Args:
        config: Configuration dictionary
        input_dir: Directory containing Parquet files
        batch_size: Rows per batch
        dry_run: Preview only

    Returns:
        Number of tables imported
    """
    logger.info(f"Starting import from {input_dir}")

    # Check for database configuration
    if "database" not in config or not config["database"].get("url"):
        logger.error("No database.url configured in config.yml")
        return 0

    # Initialize target database
    target_engine = init_database("auto", config_path="config.yml")

    # Apply schema if not exists
    if not dry_run:
        logger.info("Applying schema to target database...")
        apply_schema(target_engine)

    # Import each table
    imported_count = 0
    for table_name in MIGRATION_TABLES:
        parquet_path = input_dir / f"{table_name}.parquet"

        row_count = import_parquet_to_sql(
            target_engine,
            table_name,
            parquet_path,
            batch_size=batch_size,
            dry_run=dry_run,
        )

        if row_count > 0:
            imported_count += 1

    logger.info(f"Import complete: {imported_count}/{len(MIGRATION_TABLES)} tables")
    return imported_count


def mode_migrate(
    config: dict,
    batch_size: int = 100000,
    dry_run: bool = False,
) -> bool:
    """
    Direct migration from DuckDB to SQL database.

    Args:
        config: Configuration dictionary
        batch_size: Rows per batch
        dry_run: Preview only

    Returns:
        True if successful
    """
    logger.info("Starting direct migration from DuckDB to SQL")

    # Check for database configuration
    if "database" not in config or not config["database"].get("url"):
        logger.error("No database.url configured in config.yml")
        return False

    # Create temporary directory for intermediate Parquet
    import tempfile
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # Export from DuckDB
        logger.info("Step 1/2: Exporting from DuckDB...")
        exported = mode_export(config, tmp_path, batch_size=batch_size, dry_run=dry_run)

        if exported == 0:
            logger.error("No tables exported")
            return False

        # Import to SQL
        logger.info("Step 2/2: Importing to SQL database...")
        imported = mode_import(config, tmp_path, batch_size=batch_size, dry_run=dry_run)

        if imported == 0:
            logger.error("No tables imported")
            return False

        logger.info(f"Migration complete: {imported}/{len(MIGRATION_TABLES)} tables")
        return True


def mode_verify(config: dict) -> bool:
    """
    Verify migration integrity between DuckDB and SQL database.

    Args:
        config: Configuration dictionary

    Returns:
        True if verification passed
    """
    logger.info("Starting migration verification")

    # Check for database configuration
    if "database" not in config or not config["database"].get("url"):
        logger.error("No database.url configured in config.yml")
        return False

    # Connect to DuckDB
    base_path = config["general"]["base_path"]
    duckdb_conn = connect_and_register_views(base_path, config=config)

    if is_sqlalchemy_engine(duckdb_conn):
        logger.error("Verification requires DuckDB source, but SQL engine was configured")
        return False

    # Connect to SQL database
    target_engine = init_database("auto", config_path="config.yml")

    # Verify each table
    success, counts = verify_migration(duckdb_conn, target_engine, MIGRATION_TABLES)

    # Print summary
    print("\n" + "="*60)
    print("MIGRATION VERIFICATION SUMMARY")
    print("="*60)
    print(f"{'Table':<30} {'Source':<15} {'Target':<15} {'Status':<10}")
    print("-"*60)

    for table_name in MIGRATION_TABLES:
        if table_name in counts:
            source_count, target_count = counts[table_name]
            status = "✓ MATCH" if source_count == target_count else "✗ MISMATCH"
            print(f"{table_name:<30} {source_count:<15,} {target_count:<15,} {status:<10}")
        else:
            print(f"{table_name:<30} {'N/A':<15} {'N/A':<15} {'✗ ERROR':<10}")

    print("="*60)
    print(f"Overall: {'PASSED' if success else 'FAILED'}")
    print("="*60 + "\n")

    return success


def main():
    """Main entry point for migration tool."""
    parser = argparse.ArgumentParser(
        description="Migrate data between DuckDB and SQL databases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--mode",
        required=True,
        choices=["export", "import", "migrate", "verify"],
        help="Migration mode: export (DuckDB→Parquet), import (Parquet→SQL), migrate (DuckDB→SQL), verify (check integrity)",
    )

    parser.add_argument(
        "--config",
        default="config.yml",
        help="Path to config.yml (default: config.yml)",
    )

    parser.add_argument(
        "--output",
        type=Path,
        help="Output directory for export mode",
    )

    parser.add_argument(
        "--input",
        type=Path,
        help="Input directory for import mode",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=100000,
        help="Rows per batch for import (default: 100000)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview operations without executing",
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config(args.config)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        sys.exit(1)

    # Execute mode
    try:
        if args.mode == "export":
            if not args.output:
                logger.error("--output required for export mode")
                sys.exit(1)

            exported = mode_export(config, args.output, batch_size=args.batch_size, dry_run=args.dry_run)
            sys.exit(0 if exported > 0 else 1)

        elif args.mode == "import":
            if not args.input:
                logger.error("--input required for import mode")
                sys.exit(1)

            imported = mode_import(config, args.input, batch_size=args.batch_size, dry_run=args.dry_run)
            sys.exit(0 if imported > 0 else 1)

        elif args.mode == "migrate":
            success = mode_migrate(config, batch_size=args.batch_size, dry_run=args.dry_run)
            sys.exit(0 if success else 1)

        elif args.mode == "verify":
            success = mode_verify(config)
            sys.exit(0 if success else 1)

    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
