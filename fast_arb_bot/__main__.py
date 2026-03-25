from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from dotenv import load_dotenv

load_dotenv()


def _load_config(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text())


def _make_engine(config: dict):
    try:
        from real_arb_bot.db import RealArbDB
        from real_arb_bot.engine import RealArbEngine
    except ModuleNotFoundError as exc:
        missing = exc.name or "unknown"
        raise SystemExit(
            f"[deps] Не хватает Python-пакета: {missing}\n"
            "  pip install -e \".[dev]\""
        ) from exc
    db = RealArbDB(config["db"]["path"])
    return RealArbEngine(config, db)


def cmd_watch(engine, dry: bool = False) -> None:
    from fast_arb_bot.watch_runner import FastArbWatchRunner
    print(f"[fast-arb] Starting watch mode | dry={dry} | Ctrl+C to stop")
    FastArbWatchRunner(engine, dry_run=dry).run()


def cmd_status(engine) -> None:
    balances = engine.get_real_balances()
    pm = f"${balances['polymarket']:.2f}" if balances.get("polymarket") is not None else "N/A"
    ka = f"${balances['kalshi']:.2f}" if balances.get("kalshi") is not None else "N/A"
    print(f"[balance] Polymarket: {pm} | Kalshi: {ka}")
    engine.print_status(balances)

    # Показываем одноногие позиции
    try:
        cursor = engine.db.conn.execute(
            "SELECT symbol, execution_status, total_cost, opened_at "
            "FROM positions WHERE execution_status IN "
            "('one_legged_kalshi','one_legged_polymarket') AND status='open'"
        )
        rows = cursor.fetchall()
        if rows:
            print(f"\nОдноногие открытые позиции ({len(rows)}):")
            for r in rows:
                print(f"  {r[3][:19]} | {r[0]} | {r[1]} | ${r[2]:.2f}")
    except Exception:
        pass


def cmd_resolve(engine) -> None:
    print("[fast-arb][resolve] Checking expired positions...")
    engine.resolve()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fast Arb Bot — параллельный арбитраж Polymarket ↔ Kalshi ($20 ставка)"
    )
    parser.add_argument("--config", default="fast_arb_bot/config.yaml")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_watch = sub.add_parser("watch", help="WS-мониторинг с параллельным исполнением")
    p_watch.add_argument("--dry", action="store_true", help="Только поиск, без ордеров")

    sub.add_parser("status", help="Балансы + позиции + P&L + одноногие")
    sub.add_parser("resolve", help="Резолюция истёкших позиций")

    args = parser.parse_args()
    config = _load_config(args.config)

    print(f"[start] fast_arb_bot | config={args.config}")
    engine = _make_engine(config)

    if args.cmd == "watch":
        cmd_watch(engine, dry=getattr(args, "dry", False))
    elif args.cmd == "status":
        cmd_status(engine)
    elif args.cmd == "resolve":
        cmd_resolve(engine)


if __name__ == "__main__":
    main()
