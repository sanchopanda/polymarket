import sys

cmd = sys.argv[1] if len(sys.argv) > 1 else ""

if cmd == "compare":
    from research_bot.chainlink_compare import main
elif cmd == "prices":
    from research_bot.market_prices import main
elif cmd == "correlate":
    from research_bot.pm_correlate import main
elif cmd == "analyze":
    from research_bot.analyze_correlate import main as _main
    _main(sys.argv[2:])
    sys.exit(0)
elif cmd == "price-range":
    from research_bot.price_range_history import main as _main
    _main(sys.argv[2:])
    sys.exit(0)
elif cmd == "orderbook-monitor":
    from research_bot.orderbook_monitor import main as _main
    _main(sys.argv[2:])
    sys.exit(0)
elif cmd == "danger-zone":
    from research_bot.backtest_danger_zone import main as _main
    _main(sys.argv[2:])
    sys.exit(0)
else:
    from research_bot.chainlink_monitor import main

main()
