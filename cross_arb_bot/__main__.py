from __future__ import annotations

import argparse
import os
import time

import yaml

from cross_arb_bot.db import CrossArbDB
from cross_arb_bot.engine import CrossArbEngine


CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")


def load_config(path: str | None = None) -> dict:
    with open(path or CONFIG_PATH) as fh:
        return yaml.safe_load(fh) or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Cross-platform paper arbitrage monitor")
    parser.add_argument("--config", default=CONFIG_PATH, help="Path to cross-arb config")
    subparsers = parser.add_subparsers(dest="command", required=True)

    scan_p = subparsers.add_parser("scan", help="Run one live scan")
    scan_p.add_argument("--dry", action="store_true", help="Do not open paper positions")
    subparsers.add_parser("sim", help="Run one execution simulation cycle")
    subparsers.add_parser("status", help="Show current balances and last snapshot")
    subparsers.add_parser("resolve", help="Resolve expired paper positions")
    run_p = subparsers.add_parser("run", help="Continuous live monitoring loop")
    run_p.add_argument("--interval", type=int, default=None, help="Polling interval in seconds")
    subparsers.add_parser("live", help="Alias for continuous live monitoring")

    args = parser.parse_args()
    config = load_config(args.config)
    os.makedirs("data", exist_ok=True)
    db = CrossArbDB(config["db"]["path"])
    engine = CrossArbEngine(config, db)

    if args.command == "scan":
        opportunities = engine.scan(open_positions=not args.dry)
        engine.print_status()
        if not opportunities:
            print("No cross-platform lock opportunities in this snapshot.")
        return

    if args.command == "status":
        engine.print_status()
        return

    if args.command == "sim":
        engine.resolve()
        decisions = engine.simulate_execution_cycle()
        engine.print_status()
        if not decisions:
            print("No executable opportunities in this simulation cycle.")
        return

    if args.command == "resolve":
        engine.resolve()
        engine.print_status()
        return

    interval = config["runtime"]["poll_interval_seconds"]
    if getattr(args, "interval", None) is not None:
        interval = args.interval
    cycle = 0
    print(f"[Cross] Start live scanner. Interval: {interval}s")
    while True:
        cycle += 1
        print(f"\n[Cross] === cycle #{cycle} ===")
        engine.resolve()
        if engine.should_stop_for_balance():
            print("[Cross] Stopped: both balances are at or below the rebalance threshold.")
            engine.print_status()
            break
        engine.simulate_execution_cycle()
        engine.print_status()
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[Cross] Stopped.")
            break


if __name__ == "__main__":
    main()
