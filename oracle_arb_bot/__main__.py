from __future__ import annotations

import argparse
import calendar
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


def _load_config(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text())


def cmd_run(config: dict) -> None:
    from oracle_arb_bot.bot import OracleArbBot
    from oracle_arb_bot.db import OracleDB

    db = OracleDB(config["db"]["path"])
    bot = OracleArbBot(config, db)
    bot.run()


def cmd_scan(config: dict) -> None:
    from oracle_arb_bot.bot import OracleArbBot
    from oracle_arb_bot.db import OracleDB

    db = OracleDB(config["db"]["path"])
    bot = OracleArbBot(config, db)
    bot.scan_once()


def cmd_bets(config: dict, limit: int) -> None:
    from oracle_arb_bot.db import OracleDB

    db = OracleDB(config["db"]["path"])
    bets = db.get_recent_bets(limit)
    stats = db.stats()
    print(
        f"Stats: total={stats['total']} resolved={stats['resolved']} "
        f"open={stats['open']} realized_pnl=${stats['realized_pnl']:+.4f}\n"
    )
    header = (
        f"{'time':16s} {'venue':6s} {'sym':4s} {'int':3s} {'side':4s} "
        f"{'ask':6s} {'delta%':7s} {'ref':9s} {'sec':4s} {'opp':5s} {'depth':6s} "
        f"{'pnl':8s} {'status':10s}"
    )
    print(header)
    print("-" * len(header))
    losses = []
    for b in bets:
        pnl_str = f"${b.pnl:+.2f}" if b.pnl is not None else "      -"
        t = b.opened_at.strftime("%m-%d %H:%M:%S") if b.opened_at else "?"
        ref_str = f"{b.pm_open_price:.2f}" if b.pm_open_price else "       -"
        venue = getattr(b, "venue", "pm")[:6]
        sec_str = f"{b.seconds_to_close:4d}" if b.seconds_to_close is not None else "   -"
        opp_str = f"{b.opposite_ask:.2f}" if b.opposite_ask is not None else "    -"
        dep_str = f"${b.depth_usd:.0f}" if b.depth_usd is not None else "     -"
        print(
            f"{t:16s} {venue:6s} {b.symbol:4s} {b.interval_minutes:3d} {b.side:4s} "
            f"{b.entry_price:6.3f} {b.delta_pct:+7.3f} {ref_str:9s} "
            f"{sec_str:4s} {opp_str:5s} {dep_str:6s} "
            f"{pnl_str:8s} {b.status:10s}"
        )
        if b.status == "resolved" and b.winning_side and b.winning_side != b.side and b.market_start:
            v = getattr(b, "venue", "polymarket")
            if v == "kalshi":
                losses.append(f"  LOSS {b.symbol} {b.interval_minutes}m {b.side.upper()} [{v}] — {b.market_id}")
            else:
                ts = calendar.timegm(b.market_start.timetuple())
                slug = f"{b.symbol.lower()}-updown-{b.interval_minutes}m-{ts}"
                losses.append(f"  LOSS {b.symbol} {b.interval_minutes}m {b.side.upper()} — https://polymarket.com/event/{slug}")

    if losses:
        print("\nПроигранные рынки:")
        for line in losses:
            print(line)


def cmd_resolve(config: dict) -> None:
    from oracle_arb_bot.bot import OracleArbBot
    from oracle_arb_bot.db import OracleDB

    db = OracleDB(config["db"]["path"])
    bot = OracleArbBot(config, db)
    bot._resolve_expired()
    print("[resolve] done")


def main() -> None:
    parser = argparse.ArgumentParser(prog="oracle_arb_bot")
    parser.add_argument("--config", default="oracle_arb_bot/config.yaml")

    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("run", help="Run the bot (continuous)")
    sub.add_parser("scan", help="Single scan cycle, print markets")
    p_bets = sub.add_parser("bets", help="Show recent bets")
    p_bets.add_argument("--limit", type=int, default=50)
    sub.add_parser("resolve", help="Resolve expired open bets")

    args = parser.parse_args()
    config = _load_config(args.config)

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
