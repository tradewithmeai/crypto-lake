import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from loguru import logger
import yaml

def load_config(path: str = "config.yml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logging(app_name: str, config: Dict[str, Any]) -> None:
    base = config["general"]["base_path"]
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
