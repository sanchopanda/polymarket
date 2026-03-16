from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from src.db.models import Series, Trade


class Store:
    def __init__(self, db_path: str) -> None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS series (
                id TEXT PRIMARY KEY,
                status TEXT NOT NULL DEFAULT 'active',
                symbol TEXT NOT NULL,
                current_depth INTEGER NOT NULL DEFAULT 0,
                initial_margin REAL NOT NULL,
                total_invested REAL NOT NULL DEFAULT 0,
                total_pnl REAL NOT NULL DEFAULT 0,
                started_at TEXT NOT NULL,
                finished_at TEXT
            );

            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                series_id TEXT NOT NULL,
                series_depth INTEGER NOT NULL DEFAULT 0,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_id TEXT NOT NULL DEFAULT '',
                margin_usdt REAL NOT NULL,
                qty REAL NOT NULL,
                entry_price REAL NOT NULL,
                take_profit REAL NOT NULL,
                stop_loss REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                exit_price REAL NOT NULL DEFAULT 0,
                pnl REAL NOT NULL DEFAULT 0,
                opened_at TEXT NOT NULL,
                closed_at TEXT
            );
        """)
        self.conn.commit()

    # --- Серии ---

    def create_series(self, s: Series) -> None:
        self.conn.execute("""
            INSERT INTO series (id, status, symbol, current_depth, initial_margin,
                                total_invested, total_pnl, started_at, finished_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (s.id, s.status, s.symbol, s.current_depth, s.initial_margin,
              s.total_invested, s.total_pnl, s.started_at.isoformat(),
              s.finished_at.isoformat() if s.finished_at else None))
        self.conn.commit()

    def get_active_series(self) -> List[Series]:
        rows = self.conn.execute(
            "SELECT * FROM series WHERE status = 'active' ORDER BY started_at"
        ).fetchall()
        return [self._row_to_series(r) for r in rows]

    def get_all_series(self) -> List[Series]:
        rows = self.conn.execute(
            "SELECT * FROM series ORDER BY started_at DESC"
        ).fetchall()
        return [self._row_to_series(r) for r in rows]

    def get_series_by_id(self, series_id: str) -> Optional[Series]:
        row = self.conn.execute("SELECT * FROM series WHERE id = ?", (series_id,)).fetchone()
        return self._row_to_series(row) if row else None

    def finish_series(self, series_id: str, status: str, total_pnl: float) -> None:
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE series SET status=?, total_pnl=?, finished_at=? WHERE id=?",
            (status, total_pnl, now, series_id)
        )
        self.conn.commit()

    def update_series_depth(self, series_id: str, depth: int, added: float) -> None:
        self.conn.execute(
            "UPDATE series SET current_depth=?, total_invested=total_invested+? WHERE id=?",
            (depth, added, series_id)
        )
        self.conn.commit()

    def get_active_symbols(self) -> List[str]:
        """Символы у которых уже есть активная серия."""
        rows = self.conn.execute(
            "SELECT DISTINCT symbol FROM series WHERE status='active'"
        ).fetchall()
        return [r["symbol"] for r in rows]

    def get_series_pending_escalation(self) -> List[Series]:
        """Активные серии без открытой сделки."""
        rows = self.conn.execute("""
            SELECT s.* FROM series s
            WHERE s.status = 'active'
              AND NOT EXISTS (
                SELECT 1 FROM trades t WHERE t.series_id = s.id AND t.status = 'open'
              )
            ORDER BY s.started_at
        """).fetchall()
        return [self._row_to_series(r) for r in rows]

    # --- Сделки ---

    def save_trade(self, t: Trade) -> None:
        self.conn.execute("""
            INSERT OR REPLACE INTO trades
            (id, series_id, series_depth, symbol, side, order_id, margin_usdt, qty,
             entry_price, take_profit, stop_loss, status, exit_price, pnl, opened_at, closed_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (t.id, t.series_id, t.series_depth, t.symbol, t.side, t.order_id,
              t.margin_usdt, t.qty, t.entry_price, t.take_profit, t.stop_loss,
              t.status, t.exit_price, t.pnl, t.opened_at.isoformat(),
              t.closed_at.isoformat() if t.closed_at else None))
        self.conn.commit()

    def get_open_trades(self) -> List[Trade]:
        rows = self.conn.execute(
            "SELECT * FROM trades WHERE status='open' ORDER BY opened_at"
        ).fetchall()
        return [self._row_to_trade(r) for r in rows]

    def get_all_trades(self) -> List[Trade]:
        rows = self.conn.execute(
            "SELECT * FROM trades ORDER BY opened_at DESC"
        ).fetchall()
        return [self._row_to_trade(r) for r in rows]

    def get_series_trades(self, series_id: str) -> List[Trade]:
        rows = self.conn.execute(
            "SELECT * FROM trades WHERE series_id=? ORDER BY series_depth",
            (series_id,)
        ).fetchall()
        return [self._row_to_trade(r) for r in rows]

    def close_trade(self, trade_id: str, exit_price: float, pnl: float, status: str) -> None:
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE trades SET status=?, exit_price=?, pnl=?, closed_at=? WHERE id=?",
            (status, exit_price, pnl, now, trade_id)
        )
        self.conn.commit()

    def update_trade_order_id(self, trade_id: str, order_id: str) -> None:
        self.conn.execute("UPDATE trades SET order_id=? WHERE id=?", (order_id, trade_id))
        self.conn.commit()

    # --- Helpers ---

    def _row_to_series(self, r: sqlite3.Row) -> Series:
        return Series(
            id=r["id"], status=r["status"], symbol=r["symbol"],
            current_depth=r["current_depth"], initial_margin=r["initial_margin"],
            total_invested=r["total_invested"], total_pnl=r["total_pnl"],
            started_at=datetime.fromisoformat(r["started_at"]),
            finished_at=datetime.fromisoformat(r["finished_at"]) if r["finished_at"] else None,
        )

    def _row_to_trade(self, r: sqlite3.Row) -> Trade:
        return Trade(
            id=r["id"], series_id=r["series_id"], series_depth=r["series_depth"],
            symbol=r["symbol"], side=r["side"], order_id=r["order_id"],
            margin_usdt=r["margin_usdt"], qty=r["qty"], entry_price=r["entry_price"],
            take_profit=r["take_profit"], stop_loss=r["stop_loss"],
            status=r["status"], exit_price=r["exit_price"], pnl=r["pnl"],
            opened_at=datetime.fromisoformat(r["opened_at"]),
            closed_at=datetime.fromisoformat(r["closed_at"]) if r["closed_at"] else None,
        )
