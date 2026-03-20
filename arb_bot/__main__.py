from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timezone

import yaml

from arb_bot.db import ArbBotDB
from arb_bot.engine import ArbBotEngine


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


def load_config(path: str | None = None) -> dict:
    config_path = path or CONFIG_PATH
    with open(config_path) as fh:
        return yaml.safe_load(fh) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Isolated Polymarket arbitrage paper bot")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to arb bot config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_p = subparsers.add_parser("scan", help="Find paper arbitrage positions")
    scan_p.add_argument("--dry", action="store_true", help="Show candidates without saving")

    subparsers.add_parser("resolve", help="Resolve closed paper positions")
    subparsers.add_parser("status", help="Print portfolio status")

    run_p = subparsers.add_parser("run", help="Continuous paper trading loop")
    run_p.add_argument("--interval", type=float, default=1.0, help="Interval in minutes")
    subparsers.add_parser("ws", help="Live websocket-driven paper trading")

    args = parser.parse_args()

    config = load_config(args.config)
    os.makedirs("data", exist_ok=True)
    db = ArbBotDB(config["db"]["path"])
    engine = ArbBotEngine(config, db)

    if args.command == "scan":
        engine.run_scan(dry_run=args.dry)
        return

    if args.command == "resolve":
        engine.check_resolutions()
        return

    if args.command == "status":
        engine.print_status()
        return

    if args.command == "ws":
        engine.run_websocket()
        return

    interval_sec = args.interval * 60
    cycle = 0
    print(f"[Run] Старт paper arbitrage bot. Интервал: {args.interval:.2f} мин.")
    while True:
        cycle += 1
        print(f"\n[Run] ══ Цикл #{cycle} [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] ══")
        engine.check_resolutions()
        engine.run_scan(dry_run=False)
        try:
            time.sleep(interval_sec)
        except KeyboardInterrupt:
            print("\n[Run] Остановлено.")
            break


if __name__ == "__main__":
    main()
