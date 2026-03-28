from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


def _load_config(path: str, dry_run: bool) -> dict:
    config = yaml.safe_load(Path(path).read_text())
    if dry_run:
        config["runtime"]["dry_run"] = True
    return config


def cmd_run(config: dict) -> None:
    from volatility_bot.bot import VolatilityBot
    from volatility_bot.db import VolatilityDB

    db = VolatilityDB(config["db"]["path"])
    bot = VolatilityBot(config, db)
    bot.run()


def cmd_scan(config: dict) -> None:
    from volatility_bot.bot import VolatilityBot
    from volatility_bot.db import VolatilityDB

    config["runtime"]["dry_run"] = True
    db = VolatilityDB(config["db"]["path"])
    bot = VolatilityBot(config, db)
    bot.scan_once()


def cmd_bets(config: dict, limit: int) -> None:
    from volatility_bot.db import VolatilityDB

    db = VolatilityDB(config["db"]["path"])
    bets = db.get_recent_bets(limit)
    stats = db.stats()
    print(f"Stats: total={stats['total']} resolved={stats['resolved']} "
          f"open={stats['open']} legacy={stats['legacy']} "
          f"realized_pnl=${stats['realized_pnl']:+.4f}\n")
    header = f"{'time':16s} {'venue':12s} {'sym':4s} {'int':3s} {'side':4s} {'bucket':10s} " \
             f"{'ask':6s} {'Q':2s} {'min':3s} {'pct':5s} {'pnl':8s} {'status':10s}"
    print(header)
    print("-" * len(header))
    for b in bets:
        pnl_str = f"${b.pnl:+.2f}" if b.pnl is not None else "     -"
        t = b.opened_at.strftime("%m-%d %H:%M:%S") if b.opened_at else "?"
        print(
            f"{t:16s} {b.venue:12s} {b.symbol:4s} {b.interval_minutes:3d} "
            f"{b.side:4s} {b.trigger_bucket:10s} "
            f"{b.entry_price:6.3f} {b.market_quarter:2d} {b.market_minute:3d} "
            f"{b.position_pct:5.3f} {pnl_str:8s} {b.status:10s}"
        )


def cmd_resolve(config: dict) -> None:
    from volatility_bot.bot import VolatilityBot
    from volatility_bot.db import VolatilityDB

    db = VolatilityDB(config["db"]["path"])
    bot = VolatilityBot(config, db)
    bot._resolve_expired()
    print("[resolve] done")


def main() -> None:
    parser = argparse.ArgumentParser(prog="volatility_bot")
    parser.add_argument("--config", default="volatility_bot/config.yaml")
    parser.add_argument("--dry-run", action="store_true")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="Run the bot (continuous)")
    sub.add_parser("scan", help="Single scan cycle, print markets found")
    p_bets = sub.add_parser("bets", help="Show recent bets")
    p_bets.add_argument("--limit", type=int, default=50)
    sub.add_parser("resolve", help="Check and resolve expired open bets")

    args = parser.parse_args()
    config = _load_config(args.config, args.dry_run)

    if args.cmd == "run":
        cmd_run(config)
    elif args.cmd == "scan":
        cmd_scan(config)
    elif args.cmd == "bets":
        cmd_bets(config, args.limit)
    elif args.cmd == "resolve":
        cmd_resolve(config)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
