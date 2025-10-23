import glob
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import duckdb
from loguru import logger
import yaml

def load_config(path: str = "config.yml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def is_test_mode(config: Dict[str, Any], args: Optional[Any] = None) -> bool:
    """
    Determine if system is running in test mode.

    Args:
        config: Configuration dictionary
        args: Parsed command-line arguments (optional)

    Returns:
        True if test mode is active
    """
    if args:
        if hasattr(args, 'mode') and args.mode == "test":
            return True
        if hasattr(args, 'testing') and args.testing:
            return True

    return config.get("testing", {}).get("enabled", False)

def setup_logging(app_name: str, config: Dict[str, Any], test_mode: bool = False) -> None:
    """
    Set up logging with rotation.

    Args:
        app_name: Name of the application/module
        config: Configuration dictionary
        test_mode: If True, logs go to test subdirectory
    """
    base = config["general"]["base_path"]

    # Use test logs subdirectory in test mode
    if test_mode:
        logs_dir = os.path.join(base, "logs", "test")
    else:
        logs_dir = os.path.join(base, "logs")

    os.makedirs(logs_dir, exist_ok=True)
    level = str(config["general"].get("log_level", "INFO")).upper()
    log_path = os.path.join(logs_dir, f"{app_name}.log")
    logger.remove()
    logger.add(
        log_path,
        rotation="00:00",
        retention="14 days",
        level=level,
        backtrace=True,
        diagnose=False,
        enqueue=False,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )
    logger.add(
        lambda msg: print(msg, end=""),
        level=level,
        colorize=True,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    )

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

def get_exchange_config(config: Dict[str, Any], name: str) -> Dict[str, Any]:
    for ex in config.get("exchanges", []):
        if ex.get("name", "").lower() == name.lower():
            return ex
    raise ValueError(f"Exchange config not found: {name}")

def get_raw_base_dir(config: Dict[str, Any], exchange_name: str) -> str:
    return os.path.join(config["general"]["base_path"], "raw", exchange_name)

def get_raw_symbol_day_dir(config: Dict[str, Any], exchange_name: str, symbol: str, date: str) -> str:
    return os.path.join(get_raw_base_dir(config, exchange_name), symbol, date)

def get_parquet_exchange_root(config: Dict[str, Any], exchange_name: str) -> str:
    return os.path.join(config["general"]["base_path"], "parquet", exchange_name)

def get_parquet_symbol_root(config: Dict[str, Any], exchange_name: str, symbol: str) -> str:
    return os.path.join(get_parquet_exchange_root(config, exchange_name), symbol)

def get_backfill_symbol_root(config: Dict[str, Any], exchange_name: str, symbol: str) -> str:
    return os.path.join(config["general"]["base_path"], "backfill", exchange_name, symbol)

def to_utc_dt(dt: datetime | None = None) -> datetime:
    return (dt or datetime.utcnow()).replace(tzinfo=timezone.utc)

def get_local_date_str_utc(epoch: Optional[float] = None) -> str:
    if epoch is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")

def wait_for_parquet_files(path_pattern: str, timeout: int = 90, check_interval: int = 5) -> bool:
    """
    Wait until at least one Parquet file matching pattern exists.

    Args:
        path_pattern: Glob pattern for Parquet files (e.g., "D:/CryptoDataLake/**/*.parquet")
        timeout: Maximum seconds to wait (default: 90)
        check_interval: Seconds between checks (default: 5)

    Returns:
        True if files found, False if timeout reached
    """
    start = time.time()
    while time.time() - start < timeout:
        files = glob.glob(path_pattern, recursive=True)
        if any(os.path.isfile(f) for f in files):
            return True
        time.sleep(check_interval)
    return False

def safe_count_parquet(pattern: str) -> int:
    """
    Safely count rows in Parquet files matching pattern.

    Args:
        pattern: Glob pattern for Parquet files

    Returns:
        Row count, or 0 if no files found or error occurred
    """
    try:
        conn = duckdb.connect(":memory:")
        result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{pattern}')").fetchone()
        conn.close()
        return result[0] if result else 0
    except duckdb.IOException:
        logger.debug(f"No Parquet files found at {pattern}, returning 0.")
        return 0
    except Exception as e:
        logger.warning(f"Unexpected error counting parquet at {pattern}: {e}")
        return 0
