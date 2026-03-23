from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


def _load_config(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text())


def _make_engine(config: dict):
    from real_arb_bot.db import RealArbDB
    from real_arb_bot.engine import RealArbEngine
    db = RealArbDB(config["db"]["path"])
    return RealArbEngine(config, db)


# ── Команды ─────────────────────────────────────────────────────────────


def cmd_status(engine) -> None:
    balances = engine.get_real_balances()
    print(f"[balance] Polymarket: ${balances['polymarket']:.2f} | Kalshi: ${balances['kalshi']:.2f}")
    engine.print_status(balances)


def cmd_scan(engine, dry: bool) -> None:
    if dry:
        engine.safety.dry_run = True
        print("[scan --dry] Discovery without order placement")
    else:
        print("[scan] Scanning for opportunities...")

    opps = engine.scan(execute=not dry)
    pm_markets, kalshi_markets, matches, _ = engine.last_snapshot
    print(
        f"[scan] pm={len(pm_markets)} kalshi={len(kalshi_markets)} "
        f"matches={len(matches)} opportunities={len(opps)}"
    )
    for opp in opps[:5]:
        print(
            f"  {opp.symbol} | {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO "
            f"| ask_sum={opp.ask_sum:.4f} | edge={opp.edge_per_share:.4f} "
            f"| cost=${opp.total_cost:.2f} | exp_profit=${opp.expected_profit:.2f}"
        )


def cmd_resolve(engine) -> None:
    print("[resolve] Checking expired positions...")
    engine.resolve()


def cmd_watch(engine) -> None:
    from real_arb_bot.watch_runner import RealArbWatchRunner
    print("[watch] Starting watch mode...")
    RealArbWatchRunner(engine).run()


def cmd_run(engine, interval: int, dry: bool = False) -> None:
    if dry:
        engine.safety.dry_run = True
    print(f"[run] Continuous loop | interval={interval}s | dry={dry} | Ctrl+C to stop")
    cycle = 0
    while True:
        cycle += 1
        print(f"\n[run] Цикл {cycle}")
        try:
            engine.resolve()
            opps = engine.scan(execute=not dry)
            if dry:
                pm_markets, kalshi_markets, matches, _ = engine.last_snapshot
                print(
                    f"[scan] pm={len(pm_markets)} kalshi={len(kalshi_markets)} "
                    f"matches={len(matches)} opportunities={len(opps)}"
                )
                for opp in opps:
                    print(
                        f"\n  [{opp.symbol}] {opp.buy_yes_venue}:YES + {opp.buy_no_venue}:NO"
                        f" | ask_sum={opp.ask_sum:.4f} | edge={opp.edge_per_share:.4f}"
                        f" | cost=${opp.total_cost:.2f} | exp_profit=${opp.expected_profit:.2f}"
                        f"\n    PM:    {opp.polymarket_title}"
                        f"\n    Kalshi: {opp.kalshi_title}"
                        f"\n    Expiry: {opp.expiry.strftime('%Y-%m-%d %H:%M')} UTC"
                        f" | expiry_delta={opp.expiry_delta_seconds:.0f}s"
                        f"\n    YES ask: {opp.yes_ask:.4f} ({opp.buy_yes_venue})"
                        f" | NO ask: {opp.no_ask:.4f} ({opp.buy_no_venue})"
                    )
                if not opps:
                    print("  (нет возможностей)")
        except Exception as e:
            print(f"[run] Error in cycle {cycle}: {e}")
            engine.db.audit("cycle_error", None, {"cycle": cycle, "error": str(e)})
        engine.print_status()
        time.sleep(interval)


def cmd_audit(engine, last: int) -> None:
    entries = engine.db.get_audit_log(last_n=last)
    print(f"Audit log (last {last}):")
    for entry in reversed(entries):
        print(f"  [{entry['timestamp'][:19]}] {entry['event_type']:25s} | pos={entry.get('position_id', '')[:8]} | {entry.get('details', '')[:80]}")


def cmd_orphans(engine) -> None:
    orphans = engine.db.get_orphaned_positions()
    if not orphans:
        print("Orphaned positions: none")
        return
    print(f"Orphaned positions ({len(orphans)}):")
    for o in orphans:
        print(
            f"  [{o['opened_at'][:19]}] {o['symbol']} | {o['execution_status']} | "
            f"pair={o['pair_key']} | kalshi_order={o.get('kalshi_order_id', '')[:16]}"
        )


# ── Main ─────────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Реальный кросс-маркет арбитражный бот (Polymarket ↔ Kalshi)"
    )
    parser.add_argument("--config", default="real_arb_bot/config.yaml")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Реальные балансы + позиции + P&L")

    p_scan = sub.add_parser("scan", help="Найти возможности (и исполнить)")
    p_scan.add_argument("--dry", action="store_true", help="Только поиск, без ордеров")

    sub.add_parser("resolve", help="Резолюция истёкших позиций")

    p_run = sub.add_parser("run", help="Непрерывный цикл")
    p_run.add_argument("--interval", type=int, default=20, help="Пауза между циклами (сек)")
    p_run.add_argument("--dry", action="store_true", help="Только поиск, без ордеров")

    p_audit = sub.add_parser("audit", help="Аудит-лог")
    p_audit.add_argument("--last", type=int, default=30, help="Последние N записей")

    sub.add_parser("orphans", help="Одноногие позиции требующие разбора")

    sub.add_parser("watch", help="WS-мониторинг (Polymarket + Kalshi live feed)")

    args = parser.parse_args()
    config = _load_config(args.config)

    print(f"[start] real_arb_bot | config={args.config}")
    engine = _make_engine(config)

    if args.cmd == "status":
        cmd_status(engine)
    elif args.cmd == "scan":
        cmd_scan(engine, dry=args.dry)
    elif args.cmd == "resolve":
        cmd_resolve(engine)
    elif args.cmd == "run":
        cmd_run(engine, interval=args.interval, dry=args.dry)
    elif args.cmd == "audit":
        cmd_audit(engine, last=args.last)
    elif args.cmd == "orphans":
        cmd_orphans(engine)
    elif args.cmd == "watch":
        cmd_watch(engine)


if __name__ == "__main__":
    main()
