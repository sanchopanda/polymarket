from __future__ import annotations

import argparse
import time
from datetime import datetime

from src.api.bybit import BybitClient
from src.config import load_config
from src.db.store import Store
from src.engine.martingale import MartingaleEngine
from src.reports.dashboard import cmd_dashboard, show_series, show_open_positions


def build_engine(config_path: str = "config.yaml") -> tuple[MartingaleEngine, Store]:
    cfg = load_config(config_path)
    store = Store(cfg.db.path)
    client = BybitClient(cfg.bybit.api_key, cfg.bybit.api_secret, cfg.bybit.mode)
    engine = MartingaleEngine(cfg, store, client)
    return engine, store, cfg


def cmd_run(args: argparse.Namespace) -> None:
    engine, store, cfg = build_engine(args.config)
    interval = args.interval

    print(f"[Bot] Bybit Martingale Bot запущен | режим={cfg.bybit.mode} | интервал={interval}с")
    print(f"[Bot] Символы: {cfg.bybit.symbols}")
    print(f"[Bot] Параметры: depth={cfg.martingale.max_series_depth} "
          f"series={cfg.martingale.max_active_series} "
          f"margin=${cfg.martingale.initial_margin_usdt} "
          f"TP={cfg.martingale.take_profit_pct}% SL={cfg.martingale.stop_loss_pct}%")

    # Закрываем все оставшиеся позиции с прошлого запуска
    if not store.get_open_trades():
        print("[Bot] Закрываем оставшиеся позиции на Bybit...")
        engine.close_all_positions()

    while True:
        try:
            engine.run_cycle()
        except KeyboardInterrupt:
            print("\n[Bot] Остановлен пользователем.")
            break
        except Exception as e:
            print(f"[Bot] Ошибка в цикле: {e}")

        next_time = datetime.utcnow().strftime("%H:%M:%S")
        print(f"[Bot] Следующий цикл через {interval} сек. (UTC {next_time}+{interval}s)")
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[Bot] Остановлен пользователем.")
            break


def cmd_check(args: argparse.Namespace) -> None:
    engine, store, cfg = build_engine(args.config)
    engine.check_positions()


def cmd_scan(args: argparse.Namespace) -> None:
    engine, store, cfg = build_engine(args.config)
    engine.open_new_series()


def cmd_dashboard_cmd(args: argparse.Namespace) -> None:
    _, store, cfg = build_engine(args.config)
    cmd_dashboard(store, cfg.reports)


def cmd_series_cmd(args: argparse.Namespace) -> None:
    _, store, _ = build_engine(args.config)
    show_series(store)


def cmd_positions_cmd(args: argparse.Namespace) -> None:
    _, store, _ = build_engine(args.config)
    show_open_positions(store)


def cmd_balance(args: argparse.Namespace) -> None:
    cfg = load_config(args.config)
    client = BybitClient(cfg.bybit.api_key, cfg.bybit.api_secret, cfg.bybit.mode)
    data = client.get_wallet_balance()
    coins = data.get("result", {}).get("list", [{}])[0].get("coin", [])
    for c in coins:
        if float(c.get("walletBalance", 0)) > 0:
            print(f"{c['coin']}: {c['walletBalance']} (equity: {c.get('equity', '?')})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Bybit Martingale Bot")
    parser.add_argument("--config", default="config.yaml", help="Путь к конфигу")
    sub = parser.add_subparsers(dest="cmd")

    p_run = sub.add_parser("run", help="Запустить бота в цикле")
    p_run.add_argument("--interval", type=int, default=10, help="Интервал проверки (сек)")
    p_run.set_defaults(func=cmd_run)

    p_check = sub.add_parser("check", help="Проверить открытые позиции")
    p_check.set_defaults(func=cmd_check)

    p_scan = sub.add_parser("open", help="Открыть новые серии")
    p_scan.set_defaults(func=cmd_scan)

    p_dash = sub.add_parser("dashboard", help="Показать дашборд")
    p_dash.set_defaults(func=cmd_dashboard_cmd)

    p_series = sub.add_parser("series", help="Показать серии")
    p_series.set_defaults(func=cmd_series_cmd)

    p_pos = sub.add_parser("positions", help="Показать открытые позиции")
    p_pos.set_defaults(func=cmd_positions_cmd)

    p_bal = sub.add_parser("balance", help="Показать баланс аккаунта")
    p_bal.set_defaults(func=cmd_balance)

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
