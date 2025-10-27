"""
Quiet-by-default logging setup for production deployments.

This module provides a minimal logging configuration suitable for containerised
deployments (e.g., Cloud Run) where console output should be quiet by default.
Console logs default to WARNING level, whilst detailed DEBUG logs are written
to logs/qa/crypto-lake.log for forensic analysis.

Environment variables:
- LOG_LEVEL: Console log level (default: WARNING)

Usage:
    from tools.logging_setup import setup_logging
    setup_logging()
"""

from loguru import logger
import os
import sys
from pathlib import Path


def setup_logging():
    """
    Configure quiet-by-default logging with file output.

    Console output:
    - Controlled by LOG_LEVEL environment variable (default: WARNING)
    - Suitable for production/containerised environments
    - Minimal noise in logs

    File output:
    - Always at DEBUG level for detailed forensics
    - Written to logs/qa/crypto-lake.log
    - 10 MB rotation with 5-file retention
    """
    # Remove default logger handler
    logger.remove()

    # Console logging: quiet by default (WARNING level)
    level = os.getenv("LOG_LEVEL", "WARNING")
    logger.add(
        sys.stdout,
        level=level,
        enqueue=True,
        backtrace=False,
        diagnose=False,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
    )

    # File logging: detailed for QA (DEBUG level)
    Path("logs/qa").mkdir(parents=True, exist_ok=True)
    logger.add(
        "logs/qa/crypto-lake.log",
        level="DEBUG",
        rotation="10 MB",
        retention=5,
        enqueue=True,
        backtrace=True,
        diagnose=False,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )
