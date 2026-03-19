"""SQLite хранилище для simple_bot."""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


@dataclass
class Bet:
    id: str
    market_id: str
    question: str
    outcome: str
    entry_price: float
    amount: float
    fee: float
    shares: float
    placed_at: datetime
    end_date: datetime
    status: str = "open"          # open | won | lost
    exit_price: Optional[float] = None
    pnl: Optional[float] = None
    resolved_at: Optional[datetime] = None


class BotDB:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS bets (
                id          TEXT PRIMARY KEY,
                market_id   TEXT NOT NULL,
                question    TEXT NOT NULL,
                outcome     TEXT NOT NULL,
                entry_price REAL NOT NULL,
                amount      REAL NOT NULL,
                fee         REAL NOT NULL,
                shares      REAL NOT NULL,
                placed_at   TEXT NOT NULL,
                end_date    TEXT NOT NULL,
                status      TEXT NOT NULL DEFAULT 'open',
                exit_price  REAL,
                pnl         REAL,
                resolved_at TEXT
            )
        """)
        self._conn.commit()

    def already_bet(self, market_id: str, outcome: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM bets WHERE market_id=? AND outcome=? AND status='open'",
            (market_id, outcome),
        ).fetchone()
        return row is not None

    def save_bet(self, bet: Bet) -> None:
        self._conn.execute("""
            INSERT INTO bets
              (id, market_id, question, outcome, entry_price, amount, fee, shares,
               placed_at, end_date, status)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            bet.id, bet.market_id, bet.question, bet.outcome,
            bet.entry_price, bet.amount, bet.fee, bet.shares,
            bet.placed_at.isoformat(), bet.end_date.isoformat(),
            bet.status,
        ))
        self._conn.commit()

    def get_open_bets(self) -> list[Bet]:
        rows = self._conn.execute(
            "SELECT * FROM bets WHERE status='open' ORDER BY end_date ASC"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def get_all_bets(self) -> list[Bet]:
        rows = self._conn.execute(
            "SELECT * FROM bets ORDER BY placed_at DESC"
        ).fetchall()
        return [self._row_to_bet(r) for r in rows]

    def resolve_bet(self, bet_id: str, exit_price: float) -> None:
        status = "won" if exit_price >= 0.9 else "lost"
        bet = self._get_bet(bet_id)
        if bet is None:
            return
        pnl = (bet.shares * 1.0 - bet.amount - bet.fee) if exit_price >= 0.9 else -(bet.amount + bet.fee)
        self._conn.execute("""
            UPDATE bets SET status=?, exit_price=?, pnl=?, resolved_at=?
            WHERE id=?
        """, (status, exit_price, pnl, datetime.now(timezone.utc).isoformat(), bet_id))
        self._conn.commit()

    def _get_bet(self, bet_id: str) -> Optional[Bet]:
        row = self._conn.execute("SELECT * FROM bets WHERE id=?", (bet_id,)).fetchone()
        return self._row_to_bet(row) if row else None

    def _row_to_bet(self, row) -> Bet:
        return Bet(
            id=row["id"],
            market_id=row["market_id"],
            question=row["question"],
            outcome=row["outcome"],
            entry_price=row["entry_price"],
            amount=row["amount"],
            fee=row["fee"],
            shares=row["shares"],
            placed_at=datetime.fromisoformat(row["placed_at"]),
            end_date=datetime.fromisoformat(row["end_date"]),
            status=row["status"],
            exit_price=row["exit_price"],
            pnl=row["pnl"],
            resolved_at=datetime.fromisoformat(row["resolved_at"]) if row["resolved_at"] else None,
        )

    def stats(self) -> dict:
        all_bets = self.get_all_bets()
        open_bets = [b for b in all_bets if b.status == "open"]
        won = [b for b in all_bets if b.status == "won"]
        lost = [b for b in all_bets if b.status == "lost"]
        realized_pnl = sum(b.pnl for b in all_bets if b.pnl is not None)
        open_invested = sum(b.amount + b.fee for b in open_bets)
        return {
            "total": len(all_bets),
            "open": len(open_bets),
            "won": len(won),
            "lost": len(lost),
            "realized_pnl": realized_pnl,
            "open_invested": open_invested,
        }
