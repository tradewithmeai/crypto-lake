import argparse
import os
import subprocess
from typing import Any, Dict, Optional

from tools.common import load_config

def _python_exe() -> str:
    return os.path.join(os.path.dirname(os.sys.executable), "python.exe")

def _schtasks(args: list[str]) -> None:
    subprocess.run(["schtasks"] + args, check=True)

def schedule_collector_on_startup(config_path: str = "config.yml") -> None:
    py = _python_exe()
    cmd = f'"{py}" "{os.path.join(os.getcwd(), "main.py")}" --mode collector --config "{os.path.abspath(config_path)}"'
    _schtasks(["/Create", "/SC", "ONSTART", "/RL", "HIGHEST", "/TN", "CryptoDataLake_Collector", "/TR", cmd])

def schedule_transformer_every_5min(config_path: str = "config.yml") -> None:
    py = _python_exe()
    cmd = f'"{py}" "{os.path.join(os.getcwd(), "main.py")}" --mode transformer --config "{os.path.abspath(config_path)}"'
    _schtasks(["/Create", "/SC", "MINUTE", "/MO", "5", "/TN", "CryptoDataLake_Transformer", "/TR", cmd])

def schedule_compactor_nightly(config_path: str = "config.yml") -> None:
    py = _python_exe()
    cmd = f'"{py}" "{os.path.join(os.getcwd(), "main.py")}" --mode compact --config "{os.path.abspath(config_path)}"'
    _schtasks(["/Create", "/SC", "DAILY", "/ST", "01:30", "/TN", "CryptoDataLake_Compactor", "/TR", cmd])

def remove_task(task_name: str) -> None:
    _schtasks(["/Delete", "/TN", task_name, "/F"])

def main() -> None:
    parser = argparse.ArgumentParser(description="Scheduler helper for Crypto Data Lake (Windows).")
    parser.add_argument("--action", choices=["setup_all", "remove_all"], required=True)
    parser.add_argument("--config", default="config.yml")
    args = parser.parse_args()

    if args.action == "setup_all":
        schedule_collector_on_startup(args.config)
        schedule_transformer_every_5min(args.config)
        schedule_compactor_nightly(args.config)
    elif args.action == "remove_all":
        for name in ["CryptoDataLake_Collector", "CryptoDataLake_Transformer", "CryptoDataLake_Compactor"]:
            try:
                remove_task(name)
            except Exception:
                pass

if __name__ == "__main__":
    main()
