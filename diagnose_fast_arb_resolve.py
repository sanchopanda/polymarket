from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv

from real_arb_bot.db import RealArbDB
from real_arb_bot.engine import RealArbEngine


def _load_config(path: str) -> dict:
    return yaml.safe_load(Path(path).read_text())


def _expired_open_rows(db: RealArbDB, position_id: str | None) -> list:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    if position_id:
        row = db.conn.execute(
            """
            SELECT * FROM positions
            WHERE id=? AND status='open' AND expiry<=?
            ORDER BY expiry ASC
            """,
            (position_id, now),
        ).fetchone()
        return [row] if row is not None else []

    return db.conn.execute(
        """
        SELECT * FROM positions
        WHERE status='open' AND expiry<=?
        ORDER BY expiry ASC
        """,
        (now,),
    ).fetchall()


def _short_json(raw: str | None, max_len: int = 180) -> str:
    if not raw:
        return "-"
    text = raw.replace("\n", " ")
    return text if len(text) <= max_len else text[:max_len] + "..."


def _print_position_diagnostics(engine: RealArbEngine, row) -> None:
    position = engine.db.get_position(str(row["id"]))
    if position is None:
        return

    pm_market_id = position.market_yes if position.venue_yes == "polymarket" else position.market_no
    kalshi_ticker = position.market_yes if position.venue_yes == "kalshi" else position.market_no

    pm_market = engine.pm_feed.client.fetch_market(pm_market_id)
    kalshi_market = engine.kalshi_trader.get_market(kalshi_ticker)
    pm_result, pm_snapshot = engine.resolver._check_polymarket(position)
    kalshi_result, kalshi_snapshot = engine.resolver._check_kalshi(position)
    pm_state = "pm_unavailable"
    if pm_market is not None:
        try:
            up_idx = next(i for i, o in enumerate(pm_market.outcomes) if o.lower() == "up")
            down_idx = next(i for i, o in enumerate(pm_market.outcomes) if o.lower() == "down")
            up_price = float(pm_market.outcome_prices[up_idx])
            down_price = float(pm_market.outcome_prices[down_idx])
            if not pm_market.closed:
                pm_state = "waiting_pm_closed"
            elif up_price >= 0.95 and down_price <= 0.05:
                pm_state = "ready_pm_yes"
            elif down_price >= 0.95 and up_price <= 0.05:
                pm_state = "ready_pm_no"
            else:
                pm_state = "waiting_pm_final_prices"
        except StopIteration:
            pm_state = "pm_unknown_outcomes"

    kalshi_state = "kalshi_unavailable"
    if kalshi_market:
        kalshi_status = str(kalshi_market.get("status") or "").lower()
        kalshi_result_raw = str(kalshi_market.get("result") or "").lower()
        if kalshi_result_raw in {"yes", "no"}:
            kalshi_state = f"ready_kalshi_{kalshi_result_raw}"
        elif kalshi_status in {"closed", "determined", "finalized"}:
            kalshi_state = "waiting_kalshi_result"
        else:
            kalshi_state = "waiting_kalshi_finalized"

    if pm_result is not None and kalshi_result is not None:
        resolve_state = "ready_to_resolve"
    elif pm_result is None and kalshi_result is not None:
        resolve_state = pm_state
    elif pm_result is not None and kalshi_result is None:
        resolve_state = kalshi_state
    else:
        resolve_state = f"{pm_state} + {kalshi_state}"

    print(
        f"[diag] {position.symbol} | id={position.id[:8]} | pair={position.pair_key}\n"
        f"       expiry={position.expiry.isoformat()} | status={row['status']} | exec={row['execution_status']}\n"
        f"       resolve_state={resolve_state}\n"
        f"       PM {position.venue_yes if position.venue_yes == 'polymarket' else position.venue_no}"
        f" fill={float(row['polymarket_fill_shares'] or 0):.4f}@{float(row['polymarket_fill_price'] or 0):.4f}"
        f" | Kalshi fill={float(row['kalshi_fill_shares'] or 0):.4f}@{float(row['kalshi_fill_price'] or 0):.4f}"
    )

    if pm_market is None:
        print("       PM raw: unavailable")
    else:
        print(
            f"       PM raw: active={pm_market.active} closed={pm_market.closed} "
            f"prices={pm_market.outcome_prices}"
        )
    print(
        f"       PM resolved: result={pm_result} | snapshot={_short_json(pm_snapshot)}"
    )

    if not kalshi_market:
        print("       Kalshi raw: unavailable")
    else:
        print(
            f"       Kalshi raw: status={kalshi_market.get('status')} "
            f"result={kalshi_market.get('result')} yes_bid={kalshi_market.get('yes_bid')} "
            f"yes_ask={kalshi_market.get('yes_ask')}"
        )
    print(
        f"       Kalshi resolved: result={kalshi_result} | snapshot={_short_json(kalshi_snapshot)}"
    )


def _print_open_summary(db: RealArbDB) -> None:
    rows = db.conn.execute(
        """
        SELECT id, symbol, expiry, execution_status
        FROM positions
        WHERE status='open'
        ORDER BY expiry ASC
        """
    ).fetchall()
    if not rows:
        print("[diag] Открытых позиций нет.")
        return
    print(f"[diag] Открытых позиций: {len(rows)}")
    for row in rows:
        print(
            f"       {row['id'][:8]} | {row['symbol']} | expiry={row['expiry']} | exec={row['execution_status']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Диагностический цикл resolve для fast_arb_bot"
    )
    parser.add_argument("--config", default="fast_arb_bot/config.yaml")
    parser.add_argument("--position-id", help="Смотреть только одну позицию")
    parser.add_argument("--interval", type=float, default=5.0, help="Пауза между попытками resolve")
    parser.add_argument("--max-rounds", type=int, default=0, help="0 = бесконечно")
    parser.add_argument("--once", action="store_true", help="Один проход диагностики + resolve")
    args = parser.parse_args()

    load_dotenv()

    config = _load_config(args.config)
    db = RealArbDB(config["db"]["path"])
    engine = RealArbEngine(config, db)

    rounds = 0
    while True:
        rounds += 1
        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")
        print(f"\n=== Resolve round {rounds} | {now} UTC ===")

        expired_rows = _expired_open_rows(db, args.position_id)
        if not expired_rows:
            print("[diag] Нет просроченных open-позиций.")
            _print_open_summary(db)
            if args.once:
                break
            if args.max_rounds and rounds >= args.max_rounds:
                break
            time.sleep(args.interval)
            continue

        print(f"[diag] Просроченных open-позиций: {len(expired_rows)}")
        for row in expired_rows:
            _print_position_diagnostics(engine, row)

        print("[diag] Запускаю engine.resolve()")
        engine.resolve()

        print("[diag] После resolve:")
        for row in expired_rows:
            fresh = db.conn.execute(
                """
                SELECT status, execution_status, resolved_at, polymarket_result, kalshi_result,
                       polymarket_redeem_tx, actual_pnl
                FROM positions WHERE id=?
                """,
                (row["id"],),
            ).fetchone()
            if fresh is None:
                continue
            print(
                f"       {row['id'][:8]} | status={fresh['status']} | exec={fresh['execution_status']} "
                f"| pm={fresh['polymarket_result']} | kalshi={fresh['kalshi_result']} "
                f"| redeem_tx={fresh['polymarket_redeem_tx'] or '-'} | pnl={fresh['actual_pnl']}"
            )

        if args.once:
            break
        if args.max_rounds and rounds >= args.max_rounds:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
