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

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crypto Data Lake")
    parser.add_argument(
        "--mode",
        choices=["collector", "transformer", "validate", "compact"],
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

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C). Exiting...")
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
