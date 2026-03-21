from __future__ import annotations

import argparse
import os
import statistics
import time

import yaml

from cross_arb_bot.db import CrossArbDB
from cross_arb_bot.engine import CrossArbEngine
from cross_arb_bot.watch_runner import CrossArbWatchRunner


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
    subparsers.add_parser("liquidity-report", help="Analyze stored orderbook depth from opened positions")
    subparsers.add_parser("watch", help="Hybrid live monitoring: HTTP discovery + Polymarket WS + executable pricing")
    rebalance_p = subparsers.add_parser("rebalance", help="Paper transfer balance between venues")
    rebalance_p.add_argument("--from", dest="from_venue", required=True, choices=["polymarket", "kalshi"])
    rebalance_p.add_argument("--to", dest="to_venue", required=True, choices=["polymarket", "kalshi"])
    rebalance_p.add_argument("--amount", type=float, required=True, help="Transfer amount in USD")
    rebalance_p.add_argument("--note", default="", help="Optional note for the paper transfer")
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

    if args.command == "liquidity-report":
        positions = db.get_positions_with_liquidity()
        print(f"Positions with stored liquidity snapshots: {len(positions)}")
        if not positions:
            print("No stored liquidity data yet. New positions will start populating it.")
            return

        def pct(values, q):
            if not values:
                return 0.0
            values = sorted(values)
            idx = min(len(values) - 1, max(0, int(round((len(values) - 1) * q))))
            return values[idx]

        yes_available = [float(p.yes_available_shares or 0.0) for p in positions]
        no_available = [float(p.no_available_shares or 0.0) for p in positions]
        min_available = [min(float(p.yes_available_shares or 0.0), float(p.no_available_shares or 0.0)) for p in positions]
        yes_slippage = [float((p.yes_avg_price or 0.0) - (p.yes_best_ask or 0.0)) for p in positions if p.yes_avg_price is not None and p.yes_best_ask is not None]
        no_slippage = [float((p.no_avg_price or 0.0) - (p.no_best_ask or 0.0)) for p in positions if p.no_avg_price is not None and p.no_best_ask is not None]

        print(
            f"Min-side available shares: avg={statistics.mean(min_available):.2f} "
            f"| p25={pct(min_available, 0.25):.2f} | p50={pct(min_available, 0.50):.2f} "
            f"| p75={pct(min_available, 0.75):.2f} | p90={pct(min_available, 0.90):.2f}"
        )
        print(
            f"YES-side book shares: avg={statistics.mean(yes_available):.2f} "
            f"| median={pct(yes_available, 0.50):.2f} | p90={pct(yes_available, 0.90):.2f}"
        )
        print(
            f"NO-side book shares: avg={statistics.mean(no_available):.2f} "
            f"| median={pct(no_available, 0.50):.2f} | p90={pct(no_available, 0.90):.2f}"
        )
        if yes_slippage:
            print(
                f"YES slippage: avg={statistics.mean(yes_slippage):+.4f} "
                f"| median={pct(yes_slippage, 0.50):+.4f} | p90={pct(yes_slippage, 0.90):+.4f}"
            )
        if no_slippage:
            print(
                f"NO slippage: avg={statistics.mean(no_slippage):+.4f} "
                f"| median={pct(no_slippage, 0.50):+.4f} | p90={pct(no_slippage, 0.90):+.4f}"
            )

        print("Recent positions:")
        for p in positions[:10]:
            print(
                f"  {p.symbol} | opened={p.opened_at.isoformat()} | shares={p.shares:.2f} | "
                f"yes_avail={float(p.yes_available_shares or 0.0):.2f} | no_avail={float(p.no_available_shares or 0.0):.2f} | "
                f"yes_slip={float((p.yes_avg_price or 0.0) - (p.yes_best_ask or 0.0)):+.4f} | "
                f"no_slip={float((p.no_avg_price or 0.0) - (p.no_best_ask or 0.0)):+.4f}"
            )
        return

    if args.command == "rebalance":
        transfer_id = engine.rebalance(
            from_venue=args.from_venue,
            to_venue=args.to_venue,
            amount=args.amount,
            note=args.note,
        )
        print(
            f"[Cross][REBALANCE] {args.from_venue} -> {args.to_venue} | amount=${args.amount:.2f} "
            f"| id={transfer_id}"
        )
        engine.print_status()
        return

    if args.command == "watch":
        runner = CrossArbWatchRunner(engine)
        runner.run()
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
