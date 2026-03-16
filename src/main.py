#!/usr/bin/env python3
"""
Polymarket Martingale Bot — CLI
Использование:
  python -m src.main scan              # Один скан (paper trading)
  python -m src.main scan --dry        # Скан без сохранения ставок
  python -m src.main resolve           # Проверить резолюции + эскалация (paper)
  python -m src.main series            # Показать серии Мартингейла
  python -m src.main dashboard         # P&L сводка
  python -m src.main positions         # Открытые позиции
  python -m src.main history           # История ставок
  python -m src.main run               # Непрерывный режим paper trading
  python -m src.main real scan         # Один скан (real trading)
  python -m src.main real scan --dry   # Скан без размещения ордеров
  python -m src.main real resolve      # Резолюция + эскалация (real)
  python -m src.main real run          # Непрерывный режим real trading
  python -m src.main real balance      # Баланс кошелька
  python -m src.main backtest          # Бэктест на исторических данных
  python -m src.main backtest --limit 100 --no-price-history  # Быстрый тест
"""

import argparse
import sys
import time

from src.api.clob import ClobClient
from src.api.gamma import GammaClient
from src.config import load_config
from src.db.store import Store
from src.paper.engine import PaperTradingEngine
from src.real.engine import RealTradingEngine
from src.reports.dashboard import Dashboard


def build_engine(config) -> tuple:
    gamma = GammaClient(
        base_url=config.api.gamma_base_url,
        page_size=config.api.page_size,
        delay_ms=config.api.request_delay_ms,
    )
    clob = ClobClient(
        base_url=config.api.clob_base_url,
        delay_ms=config.api.request_delay_ms,
    )
    store = Store(config.db.path)
    engine = PaperTradingEngine(config, store, gamma, clob)
    return engine, store, gamma, clob


def cmd_scan(args, config):
    engine, *_ = build_engine(config)
    engine.run_scan(dry_run=args.dry)


def cmd_resolve(args, config):
    engine, *_ = build_engine(config)
    engine.check_resolutions()


def _dash(config, real: bool = False):
    db_path = config.db.real_path if real else config.db.path
    store = Store(db_path)
    if real:
        from src.api.gamma import GammaClient
        from src.real.engine import RealTradingEngine
        gamma = GammaClient(base_url=config.api.gamma_base_url, page_size=config.api.page_size, delay_ms=config.api.request_delay_ms)
        engine = RealTradingEngine(config, store, gamma)
        starting_balance = config.real_martingale.starting_balance
        try:
            current_wallet_balance = engine.check_balance()
        except Exception:
            current_wallet_balance = None
        max_depth = config.real_martingale.max_series_depth
        dash = Dashboard(store, config.reports.max_rows, starting_balance, max_depth,
                         real=True, current_wallet_balance=current_wallet_balance)
    else:
        starting_balance = config.paper_trading.starting_balance
        max_depth = config.martingale.max_series_depth
        dash = Dashboard(store, config.reports.max_rows, starting_balance, max_depth)
    return store, dash


def cmd_series(args, config):
    _, dash = _dash(config, real=getattr(args, "real", False))
    dash.show_series()


def cmd_dashboard(args, config):
    real = getattr(args, "real", False)
    _, dash = _dash(config, real=real)
    dash.show_summary()
    dash.show_depth_stats()
    dash.show_series()
    dash.show_open_positions()
    dash.show_best_worst()
    if real:
        dash.show_real_cash_flow()


def cmd_positions(args, config):
    _, dash = _dash(config, real=getattr(args, "real", False))
    dash.show_open_positions()


def cmd_history(args, config):
    _, dash = _dash(config, real=getattr(args, "real", False))
    dash.show_history()


def _next_sleep_seconds(store, max_interval_sec: float, max_active_series: int) -> float:
    """Возвращает сколько секунд спать до следующего цикла.

    Если есть свободные слоты — спим не дольше max_interval_sec (чтобы заполнить серии).
    Если все слоты заняты — спим до ближайшей экспирации.
    """
    from datetime import datetime, timezone
    open_bets = store.get_open_bets()
    stats = store.get_portfolio_stats()
    has_free_slots = stats.active_series_count < max_active_series

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Если есть ставки у которых срок истёк, но ещё не разрешились — повторим через минуту
    has_expired_unresolved = any(
        b.market_end_date and b.market_end_date <= now for b in open_bets
    )
    if has_expired_unresolved:
        return 60.0

    future_dates = [
        b.market_end_date for b in open_bets
        if b.market_end_date and b.market_end_date > now
    ]
    nearest = min(future_dates, default=None)

    if nearest is None:
        return max_interval_sec

    secs_until_expiry = (nearest - now).total_seconds() + 5.0

    if has_free_slots:
        return min(secs_until_expiry, 120.0)  # свободные слоты — проверяем каждые 2 мин
    else:
        return min(secs_until_expiry, 600.0)  # все слоты заняты — не более 10 минут


def build_real_engine(config) -> tuple:
    gamma = GammaClient(
        base_url=config.api.gamma_base_url,
        page_size=config.api.page_size,
        delay_ms=config.api.request_delay_ms,
    )
    store = Store(config.db.real_path)
    engine = RealTradingEngine(config, store, gamma)
    return engine, store


def cmd_real(args, config):
    """Диспетчер подкоманд real trading."""
    engine, store = build_real_engine(config)
    starting_balance = config.real_martingale.starting_balance
    try:
        current_wallet_balance = engine.check_balance()
    except Exception:
        current_wallet_balance = None
    dash = Dashboard(store, config.reports.max_rows, starting_balance,
                     config.real_martingale.max_series_depth, real=True,
                     current_wallet_balance=current_wallet_balance)

    if args.real_command == "balance":
        engine.check_balance()

    elif args.real_command == "scan":
        engine.run_scan(dry_run=getattr(args, "dry", False))

    elif args.real_command == "resolve":
        engine.check_resolutions()

    elif args.real_command == "redeem":
        engine.redeem_all_winning_positions()

    elif args.real_command == "run":
        from datetime import datetime, timezone
        max_interval_sec = args.interval * 3600
        print(f"[Real] Запускаем реальный торговый бот. Макс. интервал: {args.interval*60:.0f} мин.")

        tg_bot = None
        if config.telegram.token:
            from src.telegram_bot import TelegramBot
            paper_store = Store(config.db.path)
            tg_bot = TelegramBot(config.telegram.token, paper_store, config, real_store=store)
            tg_bot.start()
            engine.alert_fn = tg_bot.send_alert

        cycle = 0
        while True:
            cycle += 1
            print(f"\n[Real] ═══ Цикл #{cycle} ═══")
            engine.check_resolutions()
            engine.run_scan(dry_run=False)
            dash.current_wallet_balance = engine.check_balance()
            dash.show_summary()

            # Проверка: средств нет и серии стоят в ожидании
            min_bet = config.real_martingale.initial_bet_size
            if dash.current_wallet_balance is not None and dash.current_wallet_balance < min_bet:
                waiting_series = store.get_waiting_series()
                if waiting_series:
                    msg = (
                        f"⚠️ Недостаточно средств для продолжения.\n"
                        f"Баланс: ${dash.current_wallet_balance:.2f} (минимум ${min_bet:.2f})\n"
                        f"Серий в ожидании: {len(waiting_series)}\n"
                        f"Пополните счёт. Бот остановлен."
                    )
                    print(f"\n[Real] {msg}")
                    if tg_bot:
                        tg_bot.send_alert(msg)
                    break

            sleep_sec = _next_sleep_seconds(store, max_interval_sec, config.real_martingale.max_active_series)
            next_time = datetime.now(timezone.utc).replace(tzinfo=None).replace(microsecond=0)
            next_time = next_time.fromtimestamp(next_time.timestamp() + sleep_sec)
            if sleep_sec < 60:
                print(f"\n[Real] Следующий цикл в {next_time.strftime('%H:%M:%S')} (через {sleep_sec:.0f} сек.)")
            else:
                print(f"\n[Real] Следующий цикл в {next_time.strftime('%H:%M:%S')} (через {sleep_sec/60:.1f} мин.)")
            try:
                time.sleep(sleep_sec)
            except KeyboardInterrupt:
                print("\n[Real] Остановлено.")
                break


DEFAULT_CACHE_PATH = "data/backtest_markets.json"


def cmd_fetch(args, config):
    """Загрузить исторические рынки и сохранить в файл."""
    from src.api.gamma import GammaClient
    from src.api.clob import ClobClient
    from src.backtest.fetcher import fetch_historical_markets, save_markets

    gamma = GammaClient(
        base_url=config.api.gamma_base_url,
        page_size=config.api.page_size,
        delay_ms=config.api.request_delay_ms,
    )
    clob = ClobClient(
        base_url=config.api.clob_base_url,
        delay_ms=0,
    )

    use_price_history = not args.no_price_history
    markets = fetch_historical_markets(
        gamma=gamma,
        clob=clob,
        strategy=config.strategy,
        limit=args.limit,
        use_price_history=use_price_history,
        workers=args.workers,
    )

    if not markets:
        print("[Fetch] Нет подходящих рынков.")
        return

    save_markets(markets, args.output)


def cmd_backtest(args, config):
    """Бэктест стратегии Мартингейла на исторических данных Polymarket."""
    from src.backtest.fetcher import fetch_historical_markets, load_markets, save_markets
    from src.backtest.simulator import simulate
    from src.backtest.report import show_backtest_report
    from src.config import MartingaleConfig

    cache_path = args.cache
    use_price_history = not args.no_price_history

    if cache_path:
        # Загружаем из кэша
        markets = load_markets(cache_path)
    else:
        from src.api.gamma import GammaClient
        from src.api.clob import ClobClient

        gamma = GammaClient(
            base_url=config.api.gamma_base_url,
            page_size=config.api.page_size,
            delay_ms=config.api.request_delay_ms,
        )
        clob = ClobClient(
            base_url=config.api.clob_base_url,
            delay_ms=0,
        )

        markets = fetch_historical_markets(
            gamma=gamma,
            clob=clob,
            strategy=config.strategy,
            limit=args.limit,
            use_price_history=use_price_history,
            workers=args.workers,
        )

    if not markets:
        print("[Backtest] Нет подходящих рынков для симуляции.")
        return

    # Переопределяем параметры если заданы через CLI
    mg = MartingaleConfig(
        initial_bet_size=args.initial_bet if args.initial_bet is not None else config.martingale.initial_bet_size,
        max_series_depth=args.depth if args.depth is not None else config.martingale.max_series_depth,
        max_active_series=config.martingale.max_active_series,
        escalation_multiplier=config.martingale.escalation_multiplier,
    )

    balance = args.balance if args.balance is not None else config.paper_trading.starting_balance

    result = simulate(
        markets=markets,
        cfg=mg,
        taker_fee=config.paper_trading.taker_fee,
        starting_balance=balance,
    )

    show_backtest_report(result, len(markets), use_price_history, starting_balance=balance)


def cmd_run(args, config):
    """Непрерывный режим: resolve + scan, умный интервал между циклами."""
    from datetime import datetime, timezone
    max_interval_sec = args.interval * 3600
    print(f"[Run] Запускаем Мартингейл-бот. Макс. интервал: {args.interval*60:.0f} мин. Ctrl+C для остановки.")

    engine, store, *_ = build_engine(config)
    dash = Dashboard(store, config.reports.max_rows, config.paper_trading.starting_balance,
                     config.martingale.max_series_depth)

    if config.telegram.token:
        from src.telegram_bot import TelegramBot
        real_store = Store(config.db.real_path)
        tg = TelegramBot(config.telegram.token, store, config, real_store=real_store)
        tg.start()
    else:
        tg = None

    cycle = 0
    while True:
        cycle += 1
        print(f"\n[Run] ═══ Цикл #{cycle} ═══")
        # Сначала resolve (с эскалацией), потом scan (новые серии)
        engine.check_resolutions()
        engine.run_scan(dry_run=False)
        dash.show_summary()

        sleep_sec = _next_sleep_seconds(store, max_interval_sec, config.martingale.max_active_series)
        next_time = datetime.now(timezone.utc).replace(tzinfo=None).replace(microsecond=0)
        next_time = next_time.fromtimestamp(next_time.timestamp() + sleep_sec)
        if sleep_sec < 60:
            print(f"\n[Run] Следующий цикл в {next_time.strftime('%H:%M:%S')} (через {sleep_sec:.0f} сек.)")
        else:
            print(f"\n[Run] Следующий цикл в {next_time.strftime('%H:%M:%S')} (через {sleep_sec/60:.1f} мин.)")
        try:
            time.sleep(sleep_sec)
        except KeyboardInterrupt:
            print("\n[Run] Остановлено.")
            break


def main():
    parser = argparse.ArgumentParser(
        description="Polymarket Martingale Bot — paper trading",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config", default="config.yaml", help="Путь к config.yaml")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # scan
    scan_p = subparsers.add_parser("scan", help="Сканировать рынки и создать новые серии Мартингейла")
    scan_p.add_argument("--dry", action="store_true", help="Только показать кандидатов, не сохранять")

    # resolve
    subparsers.add_parser("resolve", help="Проверить резолюции и эскалировать проигравшие серии")

    # series
    series_p = subparsers.add_parser("series", help="Показать серии Мартингейла")
    series_p.add_argument("--real", action="store_true", help="Показать real trading данные")

    # dashboard
    dash_p = subparsers.add_parser("dashboard", help="Показать P&L сводку")
    dash_p.add_argument("--real", action="store_true", help="Показать real trading данные")

    # positions
    pos_p = subparsers.add_parser("positions", help="Показать открытые позиции")
    pos_p.add_argument("--real", action="store_true", help="Показать real trading данные")

    # history
    hist_p = subparsers.add_parser("history", help="Показать историю ставок")
    hist_p.add_argument("--real", action="store_true", help="Показать real trading данные")

    # run
    run_p = subparsers.add_parser("run", help="Непрерывный режим paper trading")
    run_p.add_argument("--interval", type=float, default=0.033, help="Интервал между циклами в часах (default: 0.033 = 2 мин)")

    # fetch — загрузить рынки в файл
    fetch_p = subparsers.add_parser("fetch", help="Загрузить исторические рынки и сохранить в файл")
    fetch_p.add_argument("--limit", type=int, default=10000, help="Кол-во закрытых рынков (default: 10000)")
    fetch_p.add_argument("--no-price-history", action="store_true", help="Не запрашивать историю цен (entry=0.50)")
    fetch_p.add_argument("--workers", type=int, default=20, help="Параллельных CLOB-запросов (default: 20)")
    fetch_p.add_argument("--output", type=str, default=DEFAULT_CACHE_PATH, help=f"Файл для сохранения (default: {DEFAULT_CACHE_PATH})")

    # backtest
    bt_p = subparsers.add_parser("backtest", help="Бэктест стратегии на исторических данных Polymarket")
    bt_p.add_argument("--cache", type=str, default=None, help=f"Загрузить рынки из файла (напр. {DEFAULT_CACHE_PATH})")
    bt_p.add_argument("--limit", type=int, default=300, help="Кол-во закрытых рынков для загрузки (default: 300)")
    bt_p.add_argument("--no-price-history", action="store_true", help="Не запрашивать историю цен (entry=0.50)")
    bt_p.add_argument("--initial-bet", type=float, default=None, help="Начальная ставка в $ (переопределяет config)")
    bt_p.add_argument("--depth", type=int, default=None, help="Макс. глубина серии (переопределяет config)")
    bt_p.add_argument("--balance", type=float, default=None, help="Стартовый баланс в $ (переопределяет config)")
    bt_p.add_argument("--workers", type=int, default=20, help="Параллельных CLOB-запросов (default: 20)")

    # real — реальная торговля
    real_p = subparsers.add_parser("real", help="Реальная торговля через Polymarket CLOB API")
    real_sub = real_p.add_subparsers(dest="real_command", required=True)
    real_sub.add_parser("balance", help="Баланс кошелька")
    real_scan_p = real_sub.add_parser("scan", help="Скан и размещение реальных ордеров")
    real_scan_p.add_argument("--dry", action="store_true", help="Только показать кандидатов, не размещать ордера")
    real_sub.add_parser("resolve", help="Проверить резолюции + эскалация")
    real_sub.add_parser("redeem", help="Выкупить выигранные токены → USDC")
    real_run_p = real_sub.add_parser("run", help="Непрерывный режим реальной торговли")
    real_run_p.add_argument("--interval", type=float, default=0.167, help="Интервал между циклами в часах (default: 0.167 = 10 мин)")

    args = parser.parse_args()
    config = load_config(args.config)

    commands = {
        "scan": cmd_scan,
        "resolve": cmd_resolve,
        "series": cmd_series,
        "fetch": cmd_fetch,
        "dashboard": cmd_dashboard,
        "positions": cmd_positions,
        "history": cmd_history,
        "run": cmd_run,
        "real": cmd_real,
        "backtest": cmd_backtest,
    }
    commands[args.command](args, config)


if __name__ == "__main__":
    main()
