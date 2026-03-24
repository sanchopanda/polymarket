from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime
from typing import Optional


class RealMomentumDB:
    def __init__(self, path: str) -> None:
        self._path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                pair_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                title TEXT NOT NULL,
                expiry TEXT NOT NULL,
                side TEXT NOT NULL,
                bet_venue TEXT NOT NULL,
                leader_venue TEXT NOT NULL,
                entry_price REAL NOT NULL,
                leader_price_at_entry REAL NOT NULL,
                shares REAL NOT NULL,
                total_cost REAL NOT NULL,
                spike_magnitude REAL NOT NULL,
                opened_at TEXT NOT NULL,
                -- real order fields
                order_id TEXT,
                fill_price REAL,
                fill_shares REAL,
                order_fee REAL,
                -- resolution
                status TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                outcome TEXT,
                pnl REAL,
                -- market ids for resolution
                pm_market_id TEXT,
                kalshi_ticker TEXT
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event TEXT NOT NULL,
                position_id TEXT,
                details TEXT
            )
        """)
        self.conn.commit()

    def open_position(
        self,
        pair_key: str,
        symbol: str,
        title: str,
        expiry: datetime,
        side: str,
        bet_venue: str,
        leader_venue: str,
        entry_price: float,
        leader_price: float,
        shares: float,
        total_cost: float,
        spike_magnitude: float,
        order_id: str,
        fill_price: float,
        fill_shares: float,
        order_fee: float,
        pm_market_id: str,
        kalshi_ticker: str,
    ) -> str:
        pos_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """
            INSERT INTO positions
              (id, pair_key, symbol, title, expiry, side, bet_venue, leader_venue,
               entry_price, leader_price_at_entry, shares, total_cost, spike_magnitude,
               opened_at, order_id, fill_price, fill_shares, order_fee, status,
               pm_market_id, kalshi_ticker)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
            """,
            (
                pos_id, pair_key, symbol, title, expiry.isoformat(), side,
                bet_venue, leader_venue, entry_price, leader_price, shares,
                total_cost, spike_magnitude, now, order_id, fill_price,
                fill_shares, order_fee, pm_market_id, kalshi_ticker,
            ),
        )
        self.conn.commit()
        return pos_id

    def get_open_positions(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM positions WHERE status='open' ORDER BY opened_at DESC"
        ).fetchall()

    def has_open_position(self, pair_key: str, side: str, bet_venue: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM positions WHERE pair_key=? AND side=? AND bet_venue=? AND status='open'",
            (pair_key, side, bet_venue),
        ).fetchone()
        return row is not None

    def has_open_opposite_side(self, pair_key: str, side: str) -> bool:
        opposite = "no" if side == "yes" else "yes"
        row = self.conn.execute(
            "SELECT 1 FROM positions WHERE pair_key=? AND side=? AND status='open'",
            (pair_key, opposite),
        ).fetchone()
        return row is not None

    def last_trade_time(self, pair_key: str, side: str) -> Optional[float]:
        row = self.conn.execute(
            "SELECT opened_at FROM positions WHERE pair_key=? AND side=? ORDER BY opened_at DESC LIMIT 1",
            (pair_key, side),
        ).fetchone()
        if row is None:
            return None
        try:
            return datetime.fromisoformat(row["opened_at"]).timestamp()
        except Exception:
            return None

    def resolve_position(self, position_id: str, outcome: str, pnl: float) -> None:
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE positions SET status='resolved', resolved_at=?, outcome=?, pnl=? WHERE id=?",
            (now, outcome, pnl, position_id),
        )
        self.conn.commit()

    def mark_redeemed(self, position_id: str) -> None:
        self.conn.execute(
            "UPDATE positions SET redeem_done=1 WHERE id=?", (position_id,)
        )
        self.conn.commit()

    def get_pending_redeems(self) -> list[sqlite3.Row]:
        """Polymarket позиции выигравшие но ещё не зарезолвленные."""
        self._ensure_redeem_column()
        return self.conn.execute(
            "SELECT * FROM positions WHERE status='resolved' AND bet_venue='polymarket'"
            " AND outcome=side AND (redeem_done IS NULL OR redeem_done=0)"
        ).fetchall()

    def _ensure_redeem_column(self) -> None:
        cols = [r[1] for r in self.conn.execute("PRAGMA table_info(positions)").fetchall()]
        if "redeem_done" not in cols:
            self.conn.execute("ALTER TABLE positions ADD COLUMN redeem_done INTEGER DEFAULT 0")
            self.conn.commit()

    def cumulative_pnl(self) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(pnl), 0) as total FROM positions WHERE status='resolved'"
        ).fetchone()
        return float(row["total"])

    def stats(self) -> dict:
        open_rows = self.get_open_positions()
        locked = sum(float(r["total_cost"]) for r in open_rows)
        cum_pnl = self.cumulative_pnl()
        resolved = self.conn.execute(
            "SELECT COUNT(*) as c FROM positions WHERE status='resolved'"
        ).fetchone()["c"]
        won = self.conn.execute(
            "SELECT COUNT(*) as c FROM positions WHERE status='resolved' AND pnl > 0"
        ).fetchone()["c"]
        lost = self.conn.execute(
            "SELECT COUNT(*) as c FROM positions WHERE status='resolved' AND pnl <= 0"
        ).fetchone()["c"]
        return {
            "cumulative_pnl": cum_pnl,
            "locked": locked,
            "open_count": len(open_rows),
            "resolved": resolved,
            "won": won,
            "lost": lost,
        }

    def audit(self, event: str, position_id: Optional[str], details: dict) -> None:
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, event, position_id, details) VALUES (?, ?, ?, ?)",
            (datetime.utcnow().isoformat(), event, position_id, json.dumps(details)),
        )
        self.conn.commit()
