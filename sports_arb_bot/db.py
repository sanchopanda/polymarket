from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional


class SportsArbDB:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                sport TEXT NOT NULL,
                pm_slug TEXT NOT NULL,
                pm_title TEXT NOT NULL,
                pm_market_id TEXT NOT NULL,
                ka_event_ticker TEXT NOT NULL,
                ka_title TEXT NOT NULL,
                match_confidence REAL,
                player_a TEXT NOT NULL,
                player_b TEXT NOT NULL,
                leg_pm_player TEXT NOT NULL,
                leg_pm_token_id TEXT NOT NULL,
                leg_pm_price REAL NOT NULL,
                leg_ka_player TEXT NOT NULL,
                leg_ka_ticker TEXT NOT NULL,
                leg_ka_price REAL NOT NULL,
                cost REAL NOT NULL,
                edge REAL NOT NULL,
                shares INTEGER NOT NULL,
                total_cost REAL NOT NULL,
                expected_profit REAL NOT NULL,
                game_date TEXT NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                winner TEXT,
                pm_result TEXT,
                ka_result TEXT,
                pnl REAL,
                lock_valid INTEGER NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                captured_at TEXT NOT NULL,
                sport TEXT NOT NULL,
                pm_slug TEXT NOT NULL,
                pm_token_id TEXT NOT NULL,
                pm_player TEXT NOT NULL,
                pm_best_ask REAL,
                pm_ask_depth_usd REAL,
                ka_ticker TEXT NOT NULL,
                ka_player TEXT NOT NULL,
                ka_yes_ask REAL,
                ka_ask_depth_usd REAL
            );

            CREATE TABLE IF NOT EXISTS virtual_balance (
                id INTEGER PRIMARY KEY,
                initial_balance REAL NOT NULL,
                current_balance REAL NOT NULL,
                total_wagered REAL NOT NULL DEFAULT 0,
                total_won REAL NOT NULL DEFAULT 0,
                total_lost REAL NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
        """)
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            "INSERT OR IGNORE INTO virtual_balance "
            "(id, initial_balance, current_balance, total_wagered, total_won, total_lost, updated_at) "
            "VALUES (1, 10000.0, 10000.0, 0, 0, 0, ?)",
            (now,),
        )
        self.conn.commit()

    def open_position(
        self,
        sport: str,
        pm_slug: str,
        pm_title: str,
        pm_market_id: str,
        ka_event_ticker: str,
        ka_title: str,
        match_confidence: float,
        player_a: str,
        player_b: str,
        leg_pm_player: str,
        leg_pm_token_id: str,
        leg_pm_price: float,
        leg_ka_player: str,
        leg_ka_ticker: str,
        leg_ka_price: float,
        cost: float,
        edge: float,
        shares: int,
        game_date: datetime,
        lock_valid: bool = True,
    ) -> str:
        pos_id = str(uuid.uuid4())[:8]
        total_cost = shares * cost
        expected_profit = shares * edge
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO positions (
                id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date, opened_at, status, lock_valid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
            (
                pos_id, sport, pm_slug, pm_title, pm_market_id,
                ka_event_ticker, ka_title, match_confidence,
                player_a, player_b,
                leg_pm_player, leg_pm_token_id, leg_pm_price,
                leg_ka_player, leg_ka_ticker, leg_ka_price,
                cost, edge, shares, total_cost, expected_profit,
                game_date.isoformat(), now, int(lock_valid),
            ),
        )
        # Deduct stake from balance
        self.conn.execute(
            "UPDATE virtual_balance SET "
            "current_balance = current_balance - ?, "
            "total_wagered = total_wagered + ?, "
            "updated_at = ? WHERE id = 1",
            (total_cost, total_cost, now),
        )
        self.conn.commit()
        return pos_id

    def resolve_position(
        self,
        pos_id: str,
        winner: Optional[str],
        pm_result: Optional[str],
        ka_result: Optional[str],
    ) -> float:
        """Resolve position. Fetches shares/total_cost from DB. Returns pnl."""
        pos = self.conn.execute(
            "SELECT shares, total_cost, lock_valid FROM positions WHERE id = ?", (pos_id,)
        ).fetchone()
        if pos is None:
            return 0.0

        shares = int(pos["shares"])
        total_cost = float(pos["total_cost"])
        lock_valid = bool(pos["lock_valid"])

        # Lock arb: exactly one of the two YES legs wins → payout = shares * $1
        payout = float(shares) if lock_valid else 0.0
        pnl = payout - total_cost

        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE positions SET
                status = 'resolved', resolved_at = ?, winner = ?,
                pm_result = ?, ka_result = ?, pnl = ?
            WHERE id = ?""",
            (now, winner, pm_result, ka_result, pnl, pos_id),
        )
        # Add payout back to balance
        self.conn.execute(
            "UPDATE virtual_balance SET "
            "current_balance = current_balance + ?, "
            "total_won = total_won + ?, "
            "updated_at = ? WHERE id = 1",
            (payout, payout, now),
        )
        if not lock_valid:
            self.conn.execute(
                "UPDATE virtual_balance SET total_lost = total_lost + ?, updated_at = ? WHERE id = 1",
                (total_cost, now),
            )
        self.conn.commit()
        return pnl

    def get_open_positions(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at"
        ).fetchall()

    def get_all_positions(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM positions ORDER BY opened_at DESC"
        ).fetchall()

    def save_orderbook_snapshot(
        self,
        sport: str,
        pm_slug: str,
        pm_token_id: str,
        pm_player: str,
        pm_best_ask: Optional[float],
        pm_ask_depth_usd: Optional[float],
        ka_ticker: str,
        ka_player: str,
        ka_yes_ask: Optional[float],
        ka_ask_depth_usd: Optional[float],
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        self.conn.execute(
            """INSERT INTO orderbook_snapshots (
                captured_at, sport,
                pm_slug, pm_token_id, pm_player, pm_best_ask, pm_ask_depth_usd,
                ka_ticker, ka_player, ka_yes_ask, ka_ask_depth_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                now, sport, pm_slug, pm_token_id, pm_player, pm_best_ask, pm_ask_depth_usd,
                ka_ticker, ka_player, ka_yes_ask, ka_ask_depth_usd,
            ),
        )
        self.conn.commit()

    def get_balance(self) -> sqlite3.Row:
        return self.conn.execute("SELECT * FROM virtual_balance WHERE id = 1").fetchone()
