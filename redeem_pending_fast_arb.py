from __future__ import annotations

import argparse
from pathlib import Path

from dotenv import load_dotenv

from real_arb_bot.clients import PolymarketTrader
from real_arb_bot.db import RealArbDB


def _pending_redeem_rows(db: RealArbDB, position_id: str | None) -> list:
    if position_id:
        row = db.conn.execute(
            """
            SELECT * FROM positions
            WHERE id=?
              AND status='resolved'
              AND polymarket_redeem_tx IS NULL
              AND (
                    (venue_yes='polymarket' AND polymarket_result='yes')
                 OR (venue_no='polymarket' AND polymarket_result='no')
              )
            """,
            (position_id,),
        ).fetchone()
        return [row] if row is not None else []

    return db.conn.execute(
        """
        SELECT * FROM positions
        WHERE status='resolved'
          AND is_paper=0
          AND polymarket_redeem_tx IS NULL
          AND (
                (venue_yes='polymarket' AND polymarket_result='yes')
             OR (venue_no='polymarket' AND polymarket_result='no')
          )
        ORDER BY resolved_at ASC
        """
    ).fetchall()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ручной redeem pending Polymarket-позиций из fast_arb_bot.db"
    )
    parser.add_argument(
        "--db",
        default="data/fast_arb_bot.db",
        help="Путь к SQLite базе fast_arb_bot",
    )
    parser.add_argument(
        "--position-id",
        help="Переисполнить redeem только для одной позиции",
    )
    args = parser.parse_args()

    load_dotenv()

    db_path = Path(args.db)
    if not db_path.exists():
        raise SystemExit(f"DB not found: {db_path}")

    db = RealArbDB(str(db_path))
    pm = PolymarketTrader()

    rows = _pending_redeem_rows(db, args.position_id)
    if not rows:
        print("Нет pending redeem позиций.")
        return

    print(f"Найдено pending redeem позиций: {len(rows)}")

    for row in rows:
        position_id = str(row["id"])
        symbol = str(row["symbol"])
        market_id = str(row["market_yes"] if row["venue_yes"] == "polymarket" else row["market_no"])
        pm_result = str(row["polymarket_result"] or "")
        print(f"[redeem] {symbol} | pos={position_id[:8]} | market={market_id} | pm_result={pm_result}")

        try:
            redeem = pm.redeem(market_id)
        except Exception as exc:
            print(f"[redeem] EXCEPTION | {exc}")
            db.audit(
                "redeem_manual_error",
                position_id,
                {"market_id": market_id, "error": str(exc)},
            )
            continue

        if redeem.success:
            db.update_polymarket_redeem(
                position_id=position_id,
                redeem_tx=redeem.tx_hash,
                redeem_gas_cost=redeem.gas_cost_pol,
                redeem_ms=redeem.total_ms,
            )
            print(
                f"[redeem] OK | tx={redeem.tx_hash[:12]}... | "
                f"payout=${redeem.payout_usdc:.2f} | ms={redeem.total_ms:.0f}"
            )
            continue

        status = "pending" if redeem.pending else "failed"
        print(f"[redeem] {status.upper()} | {redeem.error or 'unknown_error'}")
        db.audit(
            "redeem_manual_pending" if redeem.pending else "redeem_manual_failed",
            position_id,
            {
                "market_id": market_id,
                "error": redeem.error or "unknown_error",
                "pending": redeem.pending,
            },
        )


if __name__ == "__main__":
    main()
