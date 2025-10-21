import argparse
import asyncio
import os
import sys
from loguru import logger

# Local modules
from tools.common import load_config, setup_logging
from collector.collector import run_collector
from transformer.transformer import run_transformer
from tools.validator import run_validator
from storage.compactor import run_compactor
from tools.macro_minute import run_macro_minute
from tools.slice import export_slice
from tools.validate_rules import run_validation

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crypto Data Lake")
    parser.add_argument(
        "--mode",
        choices=["collector", "transformer", "validate", "compact", "macro_minute", "slice", "validate_rules"],
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
    return parser.parse_args()

def get_symbols_override(arg_symbols: str | None) -> list[str] | None:
    if not arg_symbols:
        return None
    syms = [s.strip().upper() for s in arg_symbols.split(",") if s.strip()]
    return syms or None

def main() -> None:
    args = parse_args()
    config = load_config(args.config)
    setup_logging("main", config)

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

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C). Exiting...")
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
