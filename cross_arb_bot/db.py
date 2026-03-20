from __future__ import annotations

import sqlite3
import uuid
from dataclasses import asdict
from datetime import datetime

from cross_arb_bot.models import CrossPosition, CrossVenueOpportunity


class CrossArbDB:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions (
                id TEXT PRIMARY KEY,
                pair_key TEXT NOT NULL,
                symbol TEXT NOT NULL,
                title TEXT NOT NULL,
                expiry TEXT NOT NULL,
                venue_yes TEXT NOT NULL,
                market_yes TEXT NOT NULL,
                venue_no TEXT NOT NULL,
                market_no TEXT NOT NULL,
                polymarket_title TEXT,
                kalshi_title TEXT,
                match_score REAL,
                expiry_delta_seconds REAL,
                polymarket_reference_price REAL,
                kalshi_reference_price REAL,
                polymarket_rules TEXT,
                kalshi_rules TEXT,
                shares REAL NOT NULL,
                yes_ask REAL NOT NULL,
                no_ask REAL NOT NULL,
                ask_sum REAL NOT NULL,
                total_cost REAL NOT NULL,
                expected_profit REAL NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL,
                resolved_at TEXT,
                winning_side TEXT,
                pnl REAL,
                polymarket_result TEXT,
                kalshi_result TEXT,
                lock_valid INTEGER
            )
            """
        )
        self._ensure_column("positions", "polymarket_result", "TEXT")
        self._ensure_column("positions", "kalshi_result", "TEXT")
        self._ensure_column("positions", "lock_valid", "INTEGER")
        self._ensure_column("positions", "polymarket_title", "TEXT")
        self._ensure_column("positions", "kalshi_title", "TEXT")
        self._ensure_column("positions", "match_score", "REAL")
        self._ensure_column("positions", "expiry_delta_seconds", "REAL")
        self._ensure_column("positions", "polymarket_reference_price", "REAL")
        self._ensure_column("positions", "kalshi_reference_price", "REAL")
        self._ensure_column("positions", "polymarket_rules", "TEXT")
        self._ensure_column("positions", "kalshi_rules", "TEXT")
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, column_type: str) -> None:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        existing = {row["name"] for row in rows}
        if column in existing:
            return
        self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")

    def has_open_position(self, pair_key: str, yes_venue: str, no_venue: str) -> bool:
        row = self.conn.execute(
            """
            SELECT 1 FROM positions
            WHERE pair_key=? AND venue_yes=? AND venue_no=? AND status='open'
            """,
            (pair_key, yes_venue, no_venue),
        ).fetchone()
        return row is not None

    def count_positions_for_pair(self, pair_key: str, yes_venue: str, no_venue: str) -> int:
        row = self.conn.execute(
            """
            SELECT COUNT(*) AS cnt FROM positions
            WHERE pair_key=? AND venue_yes=? AND venue_no=?
            """,
            (pair_key, yes_venue, no_venue),
        ).fetchone()
        return int(row["cnt"] or 0)

    def open_position(self, opportunity: CrossVenueOpportunity) -> CrossPosition:
        position = CrossPosition(
            id=str(uuid.uuid4()),
            pair_key=opportunity.pair_key,
            symbol=opportunity.symbol,
            title=opportunity.title,
            expiry=opportunity.expiry,
            venue_yes=opportunity.buy_yes_venue,
            market_yes=(opportunity.polymarket_market_id if opportunity.buy_yes_venue == "polymarket" else opportunity.kalshi_market_id),
            venue_no=opportunity.buy_no_venue,
            market_no=(opportunity.polymarket_market_id if opportunity.buy_no_venue == "polymarket" else opportunity.kalshi_market_id),
            shares=opportunity.shares,
            yes_ask=opportunity.yes_ask,
            no_ask=opportunity.no_ask,
            ask_sum=opportunity.ask_sum,
            total_cost=opportunity.total_cost,
            expected_profit=opportunity.expected_profit,
            opened_at=datetime.utcnow(),
        )
        self.conn.execute(
            """
            INSERT INTO positions (
                id, pair_key, symbol, title, expiry, venue_yes, market_yes, venue_no, market_no,
                polymarket_title, kalshi_title, match_score, expiry_delta_seconds,
                polymarket_reference_price, kalshi_reference_price, polymarket_rules, kalshi_rules,
                shares, yes_ask, no_ask, ask_sum, total_cost, expected_profit, opened_at, status
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                position.id,
                position.pair_key,
                position.symbol,
                position.title,
                position.expiry.isoformat(),
                position.venue_yes,
                position.market_yes,
                position.venue_no,
                position.market_no,
                opportunity.polymarket_title,
                opportunity.kalshi_title,
                opportunity.match_score,
                opportunity.expiry_delta_seconds,
                opportunity.polymarket_reference_price,
                opportunity.kalshi_reference_price,
                opportunity.polymarket_rules,
                opportunity.kalshi_rules,
                position.shares,
                position.yes_ask,
                position.no_ask,
                position.ask_sum,
                position.total_cost,
                position.expected_profit,
                position.opened_at.isoformat(),
                position.status,
            ),
        )
        self.conn.commit()
        return position

    def get_open_positions(self) -> list[CrossPosition]:
        rows = self.conn.execute("SELECT * FROM positions WHERE status='open' ORDER BY expiry ASC").fetchall()
        return [self._row_to_position(row) for row in rows]

    def resolve_position(
        self,
        position_id: str,
        winning_side: str,
        pnl: float,
        polymarket_result: str | None,
        kalshi_result: str | None,
        lock_valid: bool,
    ) -> None:
        position = self.get_position(position_id)
        if position is None:
            return
        self.conn.execute(
            """
            UPDATE positions
            SET status='resolved', resolved_at=?, winning_side=?, pnl=?, polymarket_result=?, kalshi_result=?, lock_valid=?
            WHERE id=?
            """,
            (
                datetime.utcnow().isoformat(),
                winning_side,
                pnl,
                polymarket_result,
                kalshi_result,
                1 if lock_valid else 0,
                position_id,
            ),
        )
        self.conn.commit()

    def get_position(self, position_id: str) -> CrossPosition | None:
        row = self.conn.execute("SELECT * FROM positions WHERE id=?", (position_id,)).fetchone()
        return self._row_to_position(row) if row else None

    def stats(self) -> dict:
        positions = self.get_all_positions()
        open_positions = [p for p in positions if p.status == "open"]
        realized = sum(p.pnl or 0.0 for p in positions if p.pnl is not None)
        locked_pm = sum(
            (p.yes_ask * p.shares if p.venue_yes == "polymarket" else 0.0) +
            (p.no_ask * p.shares if p.venue_no == "polymarket" else 0.0)
            for p in open_positions
        )
        locked_kalshi = sum(
            (p.yes_ask * p.shares if p.venue_yes == "kalshi" else 0.0) +
            (p.no_ask * p.shares if p.venue_no == "kalshi" else 0.0)
            for p in open_positions
        )
        return {
            "total": len(positions),
            "open": len(open_positions),
            "resolved": len([p for p in positions if p.status == "resolved"]),
            "realized_pnl": realized,
            "locked_polymarket": locked_pm,
            "locked_kalshi": locked_kalshi,
        }

    def get_all_positions(self) -> list[CrossPosition]:
        rows = self.conn.execute("SELECT * FROM positions ORDER BY opened_at DESC").fetchall()
        return [self._row_to_position(row) for row in rows]

    def _row_to_position(self, row) -> CrossPosition:
        return CrossPosition(
            id=row["id"],
            pair_key=row["pair_key"],
            symbol=row["symbol"],
            title=row["title"],
            expiry=datetime.fromisoformat(row["expiry"]),
            venue_yes=row["venue_yes"],
            market_yes=row["market_yes"],
            venue_no=row["venue_no"],
            market_no=row["market_no"],
            polymarket_title=row["polymarket_title"] if "polymarket_title" in row.keys() else None,
            kalshi_title=row["kalshi_title"] if "kalshi_title" in row.keys() else None,
            match_score=row["match_score"] if "match_score" in row.keys() else None,
            expiry_delta_seconds=row["expiry_delta_seconds"] if "expiry_delta_seconds" in row.keys() else None,
            polymarket_reference_price=row["polymarket_reference_price"] if "polymarket_reference_price" in row.keys() else None,
            kalshi_reference_price=row["kalshi_reference_price"] if "kalshi_reference_price" in row.keys() else None,
            polymarket_rules=row["polymarket_rules"] if "polymarket_rules" in row.keys() else None,
            kalshi_rules=row["kalshi_rules"] if "kalshi_rules" in row.keys() else None,
            shares=row["shares"],
            yes_ask=row["yes_ask"],
            no_ask=row["no_ask"],
            ask_sum=row["ask_sum"],
            total_cost=row["total_cost"],
            expected_profit=row["expected_profit"],
            opened_at=datetime.fromisoformat(row["opened_at"]),
            status=row["status"],
            resolved_at=datetime.fromisoformat(row["resolved_at"]) if row["resolved_at"] else None,
            winning_side=row["winning_side"],
            pnl=row["pnl"],
            polymarket_result=row["polymarket_result"],
            kalshi_result=row["kalshi_result"],
            lock_valid=bool(row["lock_valid"]) if row["lock_valid"] is not None else None,
        )
