import argparse
import asyncio
import os
import sys
from loguru import logger

# Initialise quiet-by-default logging for production/containerised environments
from tools.logging_setup import setup_logging
setup_logging()

# Local modules
from tools.common import load_config, is_test_mode
from collector.collector import run_collector
from transformer.transformer import run_transformer
from tools.validator import run_validator
from storage.compactor import run_compactor
from tools.macro_minute import run_macro_minute
from tools.slice import export_slice
from tools.validate_rules import run_validation
from tools.orchestrator import Orchestrator
from tools.backfill_binance import backfill_binance
from pathlib import Path

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crypto Data Lake")
    parser.add_argument(
        "--mode",
        choices=["collector", "transformer", "validate", "compact", "macro_minute", "slice", "validate_rules", "orchestrate", "serve", "backfill_binance", "test"],
        required=True,
        help="Pipeline mode to run.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Optional date (YYYY-MM-DD) to process for transformer/validate/compact.",
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default=None,
        help="Optional comma-separated symbol list to override config.",
    )
    parser.add_argument(
        "--exchange",
        type=str,
        default="binance",
        help="Exchange name from config to use (default: binance).",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config.yml",
        help="Path to config.yml (default: ./config.yml).",
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Comma-separated list of macro tickers for macro_minute mode (e.g., SPY,UUP,ES=F).",
    )
    parser.add_argument(
        "--lookback_days",
        type=int,
        default=7,
        help="Number of days to look back for macro_minute mode (max 7).",
    )
    # Slice mode arguments
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start timestamp for slice/validate_rules mode (ISO format, e.g., 2025-10-21T00:00:00Z).",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End timestamp for slice/validate_rules mode (ISO format, e.g., 2025-10-21T23:59:00Z).",
    )
    parser.add_argument(
        "--tf",
        type=str,
        default="1m",
        help="Timeframe for slice/validate_rules mode (1s or 1m).",
    )
    parser.add_argument(
        "--source",
        type=str,
        default="bars",
        help="Data source for slice/validate_rules mode (bars or klines).",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output file path for slice mode.",
    )
    parser.add_argument(
        "--format",
        type=str,
        default="parquet",
        help="Output format for slice mode (parquet or csv).",
    )
    parser.add_argument(
        "--report",
        type=str,
        default=None,
        help="Report output path for validate_rules mode.",
    )
    # Orchestrate mode arguments
    parser.add_argument(
        "--macro_tickers",
        type=str,
        default=None,
        help="Comma-separated list of macro tickers for orchestrate mode (e.g., SPY,UUP,ES=F,EURUSD=X).",
    )
    parser.add_argument(
        "--macro_interval_min",
        type=int,
        default=15,
        help="Minutes between macro data fetches in orchestrate mode (default: 15).",
    )
    parser.add_argument(
        "--macro_lookback_startup_days",
        type=int,
        default=7,
        help="Days to backfill on startup in orchestrate mode (default: 7).",
    )
    parser.add_argument(
        "--macro_runtime_lookback_days",
        type=int,
        default=1,
        help="Days to fetch on each scheduled run in orchestrate mode (default: 1).",
    )
    # Backfill mode arguments
    parser.add_argument(
        "--interval",
        type=str,
        default="1m",
        help="Kline interval for backfill_binance mode (default: 1m).",
    )
    # Orchestrator transformer scheduling
    parser.add_argument(
        "--transform_interval_min",
        type=int,
        default=60,
        help="Minutes between transformer runs in orchestrate mode (default: 60, 0 to disable).",
    )
    # API server arguments
    parser.add_argument(
        "--api_port",
        type=int,
        default=8000,
        help="Port for API server in serve mode (default: 8000).",
    )
    # Testing mode flag
    parser.add_argument(
        "--testing",
        action="store_true",
        help="Enable testing mode with accelerated intervals and isolated directory.",
    )
    return parser.parse_args()

def get_symbols_override(arg_symbols: str | None) -> list[str] | None:
    if not arg_symbols:
        return None
    syms = [s.strip().upper() for s in arg_symbols.split(",") if s.strip()]
    return syms or None

def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    # Detect test mode and apply overrides
    test_mode = is_test_mode(config, args)
    if test_mode or args.mode == "test":
        test_mode = True
        logger.info("TEST MODE ENABLED")

        # Apply testing overrides from config
        test_config = config.get("testing", {})

        # Override base path for isolation
        if "base_path" in test_config:
            config["general"]["base_path"] = test_config["base_path"]
            logger.info(f"Test mode: Using isolated directory {test_config['base_path']}")

        # Override intervals for accelerated testing
        if "transform_interval_min" in test_config:
            args.transform_interval_min = test_config["transform_interval_min"]
        if "macro_interval_min" in test_config:
            args.macro_interval_min = test_config["macro_interval_min"]
        if "macro_lookback_startup_days" in test_config:
            args.macro_lookback_startup_days = test_config["macro_lookback_startup_days"]
        if "macro_runtime_lookback_days" in test_config:
            args.macro_runtime_lookback_days = test_config["macro_runtime_lookback_days"]

        # For backfill mode, override lookback days
        if args.mode == "backfill_binance" and "backfill_days" in test_config:
            args.lookback_days = test_config["backfill_days"]

        # If mode is "test", treat it as "orchestrate" with test_mode=True
        if args.mode == "test":
            args.mode = "orchestrate"

    # Logging already initialized at module level (line 8-9)

    symbols_override = get_symbols_override(args.symbols)

    if args.mode == "collector":
        # Collector is async
        asyncio.run(run_collector(config, exchange_name=args.exchange, symbols=symbols_override))
        return

    if args.mode == "transformer":
        run_transformer(config, exchange_name=args.exchange, date=args.date, symbols=symbols_override)
        return

    if args.mode == "validate":
        run_validator(config, exchange_name=args.exchange, date=args.date, symbols=symbols_override)
        return

    if args.mode == "compact":
        run_compactor(config, exchange_name=args.exchange, date=args.date, symbols=symbols_override)
        return

    if args.mode == "macro_minute":
        if not args.tickers:
            logger.error("--tickers is required for macro_minute mode")
            sys.exit(1)
        tickers = [t.strip() for t in args.tickers.split(",") if t.strip()]
        run_macro_minute(config, tickers=tickers, lookback_days=args.lookback_days)
        return

    if args.mode == "slice":
        if not args.symbols:
            logger.error("--symbols is required for slice mode")
            sys.exit(1)
        if not args.start or not args.end:
            logger.error("--start and --end are required for slice mode")
            sys.exit(1)
        if not args.out:
            logger.error("--out is required for slice mode")
            sys.exit(1)
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        export_slice(
            config=config,
            symbols=symbols,
            start=args.start,
            end=args.end,
            tf=args.tf,
            source=args.source,
            out=args.out,
            format=args.format,
        )
        return

    if args.mode == "validate_rules":
        if not args.symbols:
            logger.error("--symbols is required for validate_rules mode")
            sys.exit(1)
        if not args.start or not args.end:
            logger.error("--start and --end are required for validate_rules mode")
            sys.exit(1)
        if not args.report:
            logger.error("--report is required for validate_rules mode")
            sys.exit(1)
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        run_validation(
            config=config,
            symbols=symbols,
            start=args.start,
            end=args.end,
            tf=args.tf,
            source=args.source,
            report=args.report,
        )
        return

    if args.mode == "backfill_binance":
        if not args.symbols:
            logger.error("--symbols is required for backfill_binance mode")
            sys.exit(1)
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

        # Get base directory from config
        base_path = config.get("base_path", "D:/CryptoDataLake")
        backfill_dir = Path(base_path) / "backfill" / "binance"

        logger.info(
            f"Starting Binance backfill: symbols={symbols}, lookback_days={args.lookback_days}, "
            f"interval={args.interval}"
        )

        results = backfill_binance(
            symbols=symbols,
            lookback_days=args.lookback_days,
            base_dir=backfill_dir,
            interval=args.interval,
        )

        logger.info(f"Backfill complete. Results: {results}")
        return

    if args.mode == "serve":
        # Serve mode: orchestrate + API server force-enabled
        config.setdefault("api", {})["enabled"] = True
        if args.api_port:
            config.setdefault("api", {})["port"] = args.api_port
        args.mode = "orchestrate"

    if args.mode == "orchestrate":
        # Get macro tickers from args or config
        macro_tickers = None
        if args.macro_tickers:
            macro_tickers = [t.strip() for t in args.macro_tickers.split(",") if t.strip()]
        elif "macro_minute" in config and "tickers" in config["macro_minute"]:
            macro_tickers = config["macro_minute"]["tickers"]

        if not macro_tickers:
            logger.warning("No macro tickers configured, orchestrator will run crypto collector only")
            macro_tickers = []

        # Get schedule parameters from args or config
        macro_interval_min = args.macro_interval_min
        macro_lookback_startup_days = args.macro_lookback_startup_days
        macro_runtime_lookback_days = args.macro_runtime_lookback_days
        transform_interval_min = args.transform_interval_min

        if "macro_minute" in config:
            macro_config = config["macro_minute"]
            if "schedule_minutes" in macro_config:
                macro_interval_min = macro_config["schedule_minutes"]
            if "startup_backfill_days" in macro_config:
                macro_lookback_startup_days = macro_config["startup_backfill_days"]
            if "runtime_lookback_days" in macro_config:
                macro_runtime_lookback_days = macro_config["runtime_lookback_days"]

        # Get transformer interval from config if available
        if "transformer" in config and "schedule_minutes" in config["transformer"]:
            transform_interval_min = config["transformer"]["schedule_minutes"]

        # Get macro transform interval (defaults to macro_interval_min)
        macro_transform_interval_min = macro_interval_min

        if test_mode:
            # In test mode, check for macro_transform_interval_min override
            test_config = config.get("testing", {})
            if "macro_transform_interval_min" in test_config:
                macro_transform_interval_min = test_config["macro_transform_interval_min"]
        elif "macro_minute" in config and "macro_transform_interval_min" in config["macro_minute"]:
            # In production, check config for macro_transform_interval_min
            macro_transform_interval_min = config["macro_minute"]["macro_transform_interval_min"]

        # Create and start orchestrator
        orchestrator = Orchestrator(
            config=config,
            exchange_name=args.exchange,
            symbols=symbols_override,
            macro_tickers=macro_tickers,
            macro_interval_min=macro_interval_min,
            macro_lookback_startup_days=macro_lookback_startup_days,
            macro_runtime_lookback_days=macro_runtime_lookback_days,
            transform_interval_min=transform_interval_min,
            macro_transform_interval_min=macro_transform_interval_min,
            test_mode=test_mode,
        )

        try:
            orchestrator.start()

            # Block main thread until Ctrl+C
            logger.info("Orchestrator running. Press Ctrl+C to stop.")
            while True:
                import time
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("Ctrl+C received, stopping orchestrator...")
            orchestrator.stop()

        return

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C). Exiting...")
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
