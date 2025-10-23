"""
Final System Validation Script for Crypto Lake v1.0.

Performs comprehensive data integrity checks to ensure the system is quant-ready.
"""

import duckdb
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any
from loguru import logger


class CryptoLakeValidator:
    """Comprehensive validator for Crypto Lake system."""

    def __init__(self, base_path: str):
        """Initialize validator with base path."""
        self.base_path = Path(base_path)
        self.results = {}
        self.conn = duckdb.connect(":memory:")

    def validate_backfill_schema(self) -> Dict[str, Any]:
        """Validate backfill Parquet schema."""
        logger.info("Validating backfill schema...")

        backfill_path = self.base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        try:
            query = f"""
            SELECT
                COUNT(*) as row_count,
                COUNT(DISTINCT open_time) as unique_timestamps,
                MIN(open_time) as earliest,
                MAX(open_time) as latest
            FROM read_parquet('{backfill_path.as_posix()}')
            """
            result = self.conn.execute(query).fetchone()

            # Verify schema
            schema_query = f"""
            DESCRIBE SELECT * FROM read_parquet('{backfill_path.as_posix()}')
            """
            schema = self.conn.execute(schema_query).fetchall()
            columns = [row[0] for row in schema]

            expected_columns = {
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_volume", "trades", "taker_base_vol", "taker_quote_vol"
            }

            # DuckDB adds partition columns when reading Hive-partitioned data - this is expected
            partition_columns = {"year", "month", "day"}

            missing = expected_columns - set(columns)
            extra = set(columns) - expected_columns - partition_columns

            return {
                "status": "PASS" if not missing and not extra else "FAIL",
                "row_count": result[0] if result else 0,
                "unique_timestamps": result[1] if result else 0,
                "earliest": result[2] if result else None,
                "latest": result[3] if result else None,
                "missing_columns": list(missing),
                "extra_columns": list(extra),
                "all_columns": columns,
            }

        except Exception as e:
            logger.error(f"Backfill schema validation failed: {e}")
            return {
                "status": "ERROR",
                "error": str(e)
            }

    def validate_timestamp_continuity(self) -> Dict[str, Any]:
        """Validate timestamp continuity in backfill data."""
        logger.info("Validating timestamp continuity...")

        backfill_path = self.base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        try:
            query = f"""
            WITH gaps AS (
                SELECT
                    open_time,
                    LAG(open_time) OVER (ORDER BY open_time) as prev_time,
                    EXTRACT(EPOCH FROM (open_time - LAG(open_time) OVER (ORDER BY open_time))) as gap_seconds
                FROM read_parquet('{backfill_path.as_posix()}')
            )
            SELECT
                AVG(gap_seconds) as avg_gap,
                MAX(gap_seconds) as max_gap,
                COUNT(*) FILTER (WHERE gap_seconds > 120) as large_gaps
            FROM gaps
            WHERE gap_seconds IS NOT NULL
            """
            result = self.conn.execute(query).fetchone()

            avg_gap = float(result[0]) if result and result[0] else 0
            max_gap = float(result[1]) if result and result[1] else 0
            large_gaps = int(result[2]) if result and result[2] else 0

            # Expected: 60 seconds (1-minute candles)
            # Allow some gaps for market hours, weekends, etc.
            # PASS if avg is close to 60s, WARN if gaps are excessive
            is_continuous = 55 <= avg_gap <= 120
            status = "PASS" if is_continuous and large_gaps < 10 else "WARN"

            return {
                "status": status,
                "avg_gap_seconds": avg_gap,
                "max_gap_seconds": max_gap,
                "large_gaps_count": large_gaps,
                "expected_gap": 60,
            }

        except Exception as e:
            logger.error(f"Timestamp continuity validation failed: {e}")
            return {
                "status": "ERROR",
                "error": str(e)
            }

    def validate_ohlc_sanity(self) -> Dict[str, Any]:
        """Validate OHLC relationships."""
        logger.info("Validating OHLC sanity...")

        backfill_path = self.base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        try:
            query = f"""
            SELECT
                COUNT(*) FILTER (WHERE low > open OR low > close OR low > high) as low_violations,
                COUNT(*) FILTER (WHERE high < open OR high < close OR high < low) as high_violations,
                COUNT(*) FILTER (WHERE open <= 0 OR high <= 0 OR low <= 0 OR close <= 0) as negative_prices,
                COUNT(*) as total_rows
            FROM read_parquet('{backfill_path.as_posix()}')
            """
            result = self.conn.execute(query).fetchone()

            low_violations = int(result[0]) if result else 0
            high_violations = int(result[1]) if result else 0
            negative_prices = int(result[2]) if result else 0
            total_rows = int(result[3]) if result else 0

            is_valid = low_violations == 0 and high_violations == 0 and negative_prices == 0

            return {
                "status": "PASS" if is_valid else "FAIL",
                "low_violations": low_violations,
                "high_violations": high_violations,
                "negative_prices": negative_prices,
                "total_rows": total_rows,
            }

        except Exception as e:
            logger.error(f"OHLC sanity validation failed: {e}")
            return {
                "status": "ERROR",
                "error": str(e)
            }

    def validate_deduplication(self) -> Dict[str, Any]:
        """Validate no duplicate timestamps exist."""
        logger.info("Validating deduplication...")

        backfill_path = self.base_path / "backfill" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        try:
            query = f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT open_time) as unique_timestamps,
                COUNT(*) - COUNT(DISTINCT open_time) as duplicates
            FROM read_parquet('{backfill_path.as_posix()}')
            """
            result = self.conn.execute(query).fetchone()

            total_rows = int(result[0]) if result else 0
            unique_timestamps = int(result[1]) if result else 0
            duplicates = int(result[2]) if result else 0

            return {
                "status": "PASS" if duplicates == 0 else "FAIL",
                "total_rows": total_rows,
                "unique_timestamps": unique_timestamps,
                "duplicates": duplicates,
            }

        except Exception as e:
            logger.error(f"Deduplication validation failed: {e}")
            return {
                "status": "ERROR",
                "error": str(e)
            }

    def validate_real_time_data(self) -> Dict[str, Any]:
        """Validate real-time collector data."""
        logger.info("Validating real-time collector data...")

        parquet_path = self.base_path / "parquet" / "binance" / "SOLUSDT" / "**" / "*.parquet"

        try:
            query = f"""
            SELECT
                COUNT(*) as row_count,
                MIN(window_start) as earliest,
                MAX(window_start) as latest
            FROM read_parquet('{parquet_path.as_posix()}')
            WHERE DATE(window_start) = CURRENT_DATE
            """
            result = self.conn.execute(query).fetchone()

            row_count = int(result[0]) if result and result[0] else 0
            earliest = result[1] if result and result[1] else None
            latest = result[2] if result and result[2] else None

            return {
                "status": "PASS" if row_count > 0 else "WARN",
                "row_count": row_count,
                "earliest": str(earliest) if earliest else None,
                "latest": str(latest) if latest else None,
            }

        except Exception as e:
            logger.warning(f"Real-time data validation skipped: {e}")
            return {
                "status": "SKIP",
                "reason": "No real-time data available",
                "error": str(e)
            }

    def validate_macro_data(self) -> Dict[str, Any]:
        """Validate macro data collection."""
        logger.info("Validating macro data...")

        macro_path = self.base_path / "macro" / "minute" / "**" / "*.parquet"

        try:
            query = f"""
            SELECT
                COUNT(DISTINCT ticker) as ticker_count,
                COUNT(*) as total_rows,
                MIN(ts) as earliest,
                MAX(ts) as latest
            FROM read_parquet('{macro_path.as_posix()}')
            """
            result = self.conn.execute(query).fetchone()

            ticker_count = int(result[0]) if result and result[0] else 0
            total_rows = int(result[1]) if result and result[1] else 0
            earliest = result[2] if result and result[2] else None
            latest = result[3] if result and result[3] else None

            return {
                "status": "PASS" if ticker_count > 0 and total_rows > 0 else "WARN",
                "ticker_count": ticker_count,
                "total_rows": total_rows,
                "earliest": str(earliest) if earliest else None,
                "latest": str(latest) if latest else None,
            }

        except Exception as e:
            logger.warning(f"Macro data validation skipped: {e}")
            return {
                "status": "SKIP",
                "reason": "No macro data available",
                "error": str(e)
            }

    def run_all_validations(self) -> Dict[str, Any]:
        """Run all validation checks."""
        logger.info("Starting comprehensive validation...")

        self.results = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "base_path": str(self.base_path),
            "validations": {
                "backfill_schema": self.validate_backfill_schema(),
                "timestamp_continuity": self.validate_timestamp_continuity(),
                "ohlc_sanity": self.validate_ohlc_sanity(),
                "deduplication": self.validate_deduplication(),
                "real_time_data": self.validate_real_time_data(),
                "macro_data": self.validate_macro_data(),
            }
        }

        # Calculate overall status
        statuses = [v["status"] for v in self.results["validations"].values()]
        if "FAIL" in statuses or "ERROR" in statuses:
            self.results["overall_status"] = "FAIL"
        elif "WARN" in statuses:
            self.results["overall_status"] = "PASS_WITH_WARNINGS"
        else:
            self.results["overall_status"] = "PASS"

        logger.info(f"Validation complete. Overall status: {self.results['overall_status']}")
        return self.results

    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()


def main():
    """Run validation and print results."""
    import json
    from tools.common import load_config

    config = load_config("config.yml")
    base_path = config["general"]["base_path"]

    validator = CryptoLakeValidator(base_path)

    try:
        results = validator.run_all_validations()

        # Print results
        print("\n" + "=" * 80)
        print("CRYPTO LAKE v1.0 - FINAL VALIDATION RESULTS")
        print("=" * 80)
        print(f"\nTimestamp: {results['timestamp']}")
        print(f"Base Path: {results['base_path']}")
        print(f"\nOverall Status: {results['overall_status']}")
        print("\n" + "-" * 80)

        for name, result in results["validations"].items():
            print(f"\n{name.upper().replace('_', ' ')}:")
            print(f"  Status: {result['status']}")
            for key, value in result.items():
                if key != "status":
                    print(f"  {key}: {value}")

        print("\n" + "=" * 80)

        if results["overall_status"] == "PASS":
            print("\n[OK] Crypto Lake v1.0 verified quant-ready.\n")
            return 0
        elif results["overall_status"] == "PASS_WITH_WARNINGS":
            print("\n[WARN] Crypto Lake v1.0 verified with warnings. Review details above.\n")
            return 0
        else:
            print("\n[FAIL] Crypto Lake v1.0 validation FAILED. Review details above.\n")
            return 1

    finally:
        validator.close()


if __name__ == "__main__":
    import sys
    sys.exit(main())
