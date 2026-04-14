from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import asdict
from datetime import datetime

from cross_arb_bot.models import CrossPosition, CrossVenueOpportunity

from real_arb_bot.clients import OrderResult


class RealArbDB:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._migrate()

    def _migrate(self) -> None:
        self.conn.executescript(
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
                polymarket_snapshot_open TEXT,
                kalshi_snapshot_open TEXT,
                polymarket_snapshot_resolved TEXT,
                kalshi_snapshot_resolved TEXT,
                shares REAL NOT NULL,
                yes_ask REAL NOT NULL,
                no_ask REAL NOT NULL,
                yes_requested_shares REAL,
                yes_filled_shares REAL,
                yes_available_shares REAL,
                yes_avg_price REAL,
                yes_best_ask REAL,
                yes_remaining_shares_after_fill REAL,
                no_requested_shares REAL,
                no_filled_shares REAL,
                no_available_shares REAL,
                no_avg_price REAL,
                no_best_ask REAL,
                no_remaining_shares_after_fill REAL,
                ask_sum REAL NOT NULL,
                total_cost REAL NOT NULL,
                expected_profit REAL NOT NULL,
                opened_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                resolved_at TEXT,
                winning_side TEXT,
                pnl REAL,
                actual_pnl REAL,
                polymarket_result TEXT,
                kalshi_result TEXT,
                lock_valid INTEGER,
                -- Реальные ордера
                kalshi_order_id TEXT,
                kalshi_client_order_id TEXT,
                kalshi_fill_price REAL,
                kalshi_fill_shares REAL,
                kalshi_order_fee REAL,
                kalshi_order_latency_ms REAL,
                kalshi_order_status TEXT,
                polymarket_order_id TEXT,
                polymarket_fill_price REAL,
                polymarket_fill_shares REAL,
                polymarket_order_fee REAL,
                polymarket_order_latency_ms REAL,
                polymarket_order_status TEXT,
                execution_status TEXT,
                execution_started_at TEXT,
                execution_completed_at TEXT,
                -- Settlement/redeem
                polymarket_redeem_tx TEXT,
                polymarket_redeem_gas_cost REAL,
                polymarket_redeem_ms REAL
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                event_type TEXT NOT NULL,
                position_id TEXT,
                details TEXT
            );

            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                trades_count INTEGER NOT NULL DEFAULT 0,
                realized_pnl REAL NOT NULL DEFAULT 0,
                orphaned_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        # Таблица для снапшотов баланса (reconciliation)
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS balance_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                pm_balance REAL NOT NULL,
                k_balance REAL NOT NULL,
                open_cost REAL NOT NULL DEFAULT 0,
                resolved_pnl REAL NOT NULL DEFAULT 0,
                resolved_count INTEGER NOT NULL DEFAULT 0,
                source TEXT NOT NULL DEFAULT 'periodic'
            );
            """
        )
        # Таблица для хранения траектории edge по тикам
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS edge_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                position_id TEXT NOT NULL,
                ts TEXT NOT NULL,
                seconds_to_expiry REAL,
                pm_yes_ask REAL,
                pm_no_ask REAL,
                kalshi_yes_ask REAL,
                kalshi_no_ask REAL,
                yes_ask REAL,
                no_ask REAL,
                edge REAL,
                binance_price REAL,
                binance_distance_pct REAL,
                pm_yes_bid REAL,
                pm_no_bid REAL,
                kalshi_yes_bid REAL,
                kalshi_no_bid REAL
            );
            CREATE INDEX IF NOT EXISTS idx_edge_ticks_pos ON edge_ticks(position_id);
            """
        )
        # Миграция: добавить bid-колонки если таблица уже существует
        for col in ("pm_yes_bid", "pm_no_bid", "kalshi_yes_bid", "kalshi_no_bid"):
            try:
                self.conn.execute(f"ALTER TABLE edge_ticks ADD COLUMN {col} REAL")
                self.conn.commit()
            except Exception:
                pass

        # Миграции для новых колонок
        for col, definition in [
            ("is_paper", "INTEGER NOT NULL DEFAULT 0"),
            ("exclude_from_stats", "INTEGER NOT NULL DEFAULT 0"),
            ("yes_usable_shares", "REAL"),
            ("no_usable_shares", "REAL"),
            ("pm_price_to_beat", "REAL"),
            ("kalshi_close_price", "REAL"),
            ("pm_close_price", "REAL"),
            # Edge divergence monitoring columns
            ("max_edge", "REAL"),
            ("edge_signal_at", "TEXT"),
            ("edge_at_signal", "REAL"),
            ("edge_yes_sell", "REAL"),
            ("edge_no_sell", "REAL"),
            ("edge_exit_pnl", "REAL"),
            # Edge trajectory milestones
            ("edge_first_10pct", "TEXT"),
            ("edge_first_15pct", "TEXT"),
            ("edge_first_20pct", "TEXT"),
            # Entry-time Binance distance
            ("entry_binance_distance_pct", "REAL"),
            # Which leg filled first when position opened one-legged
            ("initially_one_legged", "TEXT"),
            # Usable book depth at max_edge
            ("max_edge_yes_usable", "REAL"),
            ("max_edge_no_usable", "REAL"),
        ]:
            try:
                self.conn.execute(f"ALTER TABLE positions ADD COLUMN {col} {definition}")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # колонка уже есть

    # ── Позиции ─────────────────────────────────────────────────────────

    def open_position(
        self,
        opportunity: CrossVenueOpportunity,
        kalshi_result: OrderResult,
        polymarket_result: OrderResult,
        execution_status: str | None = None,
        route: str | None = None,
        polymarket_snapshot_open: str | None = None,
        kalshi_snapshot_open: str | None = None,
        yes_leg=None,
        no_leg=None,
        pm_price_to_beat: float | None = None,
        entry_binance_distance_pct: float | None = None,
    ) -> CrossPosition:
        pos_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        if execution_status is None:
            execution_status = "both_filled"
            if kalshi_result.status.startswith("error") or kalshi_result.shares_matched <= 0:
                execution_status = "failed"
            elif polymarket_result.status.startswith("error") or polymarket_result.shares_matched <= 0:
                execution_status = "orphaned_kalshi"

        self.conn.execute(
            """
            INSERT INTO positions (
                id, pair_key, symbol, title, expiry, venue_yes, market_yes, venue_no, market_no,
                polymarket_title, kalshi_title, match_score, expiry_delta_seconds,
                polymarket_reference_price, kalshi_reference_price, polymarket_rules, kalshi_rules,
                polymarket_snapshot_open, kalshi_snapshot_open,
                shares, yes_ask, no_ask,
                yes_requested_shares, yes_filled_shares, yes_available_shares, yes_usable_shares, yes_avg_price, yes_best_ask, yes_remaining_shares_after_fill,
                no_requested_shares, no_filled_shares, no_available_shares, no_usable_shares, no_avg_price, no_best_ask, no_remaining_shares_after_fill,
                ask_sum, total_cost, expected_profit, opened_at, status,
                kalshi_order_id, kalshi_fill_price, kalshi_fill_shares, kalshi_order_fee, kalshi_order_latency_ms, kalshi_order_status,
                polymarket_order_id, polymarket_fill_price, polymarket_fill_shares, polymarket_order_fee, polymarket_order_latency_ms, polymarket_order_status,
                execution_status, execution_started_at, execution_completed_at,
                pm_price_to_beat, entry_binance_distance_pct, initially_one_legged
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            """,
            (
                pos_id,
                opportunity.pair_key,
                opportunity.symbol,
                opportunity.title,
                opportunity.expiry.isoformat(),
                opportunity.buy_yes_venue,
                opportunity.polymarket_market_id if opportunity.buy_yes_venue == "polymarket" else opportunity.kalshi_market_id,
                opportunity.buy_no_venue,
                opportunity.polymarket_market_id if opportunity.buy_no_venue == "polymarket" else opportunity.kalshi_market_id,
                opportunity.polymarket_title,
                opportunity.kalshi_title,
                opportunity.match_score,
                opportunity.expiry_delta_seconds,
                opportunity.polymarket_reference_price,
                opportunity.kalshi_reference_price,
                opportunity.polymarket_rules,
                opportunity.kalshi_rules,
                polymarket_snapshot_open,
                kalshi_snapshot_open,
                opportunity.shares,
                opportunity.yes_ask,
                opportunity.no_ask,
                getattr(yes_leg, "requested_shares", None),
                getattr(yes_leg, "filled_shares", None),
                getattr(yes_leg, "available_shares", None),
                getattr(yes_leg, "usable_shares", None),
                getattr(yes_leg, "avg_price", None),
                getattr(yes_leg, "best_ask", None),
                getattr(yes_leg, "remaining_shares_after_fill", None),
                getattr(no_leg, "requested_shares", None),
                getattr(no_leg, "filled_shares", None),
                getattr(no_leg, "available_shares", None),
                getattr(no_leg, "usable_shares", None),
                getattr(no_leg, "avg_price", None),
                getattr(no_leg, "best_ask", None),
                getattr(no_leg, "remaining_shares_after_fill", None),
                opportunity.ask_sum,
                opportunity.total_cost,
                opportunity.expected_profit,
                now,
                "open",
                kalshi_result.order_id,
                kalshi_result.fill_price,
                kalshi_result.shares_matched,
                kalshi_result.fee,
                kalshi_result.latency_ms,
                kalshi_result.status,
                polymarket_result.order_id,
                polymarket_result.fill_price,
                polymarket_result.shares_matched,
                polymarket_result.fee,
                polymarket_result.latency_ms,
                polymarket_result.status,
                execution_status,
                now,
                now,
                pm_price_to_beat,
                entry_binance_distance_pct,
                "kalshi" if execution_status == "one_legged_kalshi" else
                "polymarket" if execution_status == "one_legged_polymarket" else None,
            ),
        )
        self.conn.commit()
        self.audit("position_opened", pos_id, {
            "pair_key": opportunity.pair_key,
            "symbol": opportunity.symbol,
            "execution_status": execution_status,
            "route": route,
            "kalshi_fill": kalshi_result.shares_matched,
            "polymarket_fill": polymarket_result.shares_matched,
        })
        return self.get_position(pos_id)

    def open_paper_position(
        self,
        opportunity: CrossVenueOpportunity,
        pm_price_to_beat: float | None = None,
        entry_binance_distance_pct: float | None = None,
        yes_leg=None,
        no_leg=None,
    ) -> CrossPosition:
        pos_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """
            INSERT INTO positions (
                id, pair_key, symbol, title, expiry, venue_yes, market_yes, venue_no, market_no,
                polymarket_title, kalshi_title, match_score, expiry_delta_seconds,
                polymarket_reference_price, kalshi_reference_price, polymarket_rules, kalshi_rules,
                shares, yes_ask, no_ask,
                yes_available_shares, yes_usable_shares, yes_best_ask,
                no_available_shares, no_usable_shares, no_best_ask,
                ask_sum, total_cost, expected_profit,
                opened_at, status, execution_status, is_paper, pm_price_to_beat,
                entry_binance_distance_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pos_id,
                opportunity.pair_key,
                opportunity.symbol,
                opportunity.title,
                opportunity.expiry.isoformat(),
                opportunity.buy_yes_venue,
                opportunity.polymarket_market_id if opportunity.buy_yes_venue == "polymarket" else opportunity.kalshi_market_id,
                opportunity.buy_no_venue,
                opportunity.polymarket_market_id if opportunity.buy_no_venue == "polymarket" else opportunity.kalshi_market_id,
                opportunity.polymarket_title,
                opportunity.kalshi_title,
                opportunity.match_score,
                opportunity.expiry_delta_seconds,
                opportunity.polymarket_reference_price,
                opportunity.kalshi_reference_price,
                opportunity.polymarket_rules,
                opportunity.kalshi_rules,
                opportunity.shares,
                opportunity.yes_ask,
                opportunity.no_ask,
                getattr(yes_leg, "available_shares", None),
                getattr(yes_leg, "usable_shares", None),
                getattr(yes_leg, "best_ask", None),
                getattr(no_leg, "available_shares", None),
                getattr(no_leg, "usable_shares", None),
                getattr(no_leg, "best_ask", None),
                opportunity.ask_sum,
                opportunity.total_cost,
                opportunity.expected_profit,
                now, "open", "paper", 1, pm_price_to_beat,
                entry_binance_distance_pct,
            ),
        )
        self.conn.commit()
        return self.get_position(pos_id)

    def has_open_paper_position(self, pair_key: str, yes_venue: str, no_venue: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM positions WHERE pair_key=? AND venue_yes=? AND venue_no=? AND status='open' AND is_paper=1",
            (pair_key, yes_venue, no_venue),
        ).fetchone() is not None

    def open_paper_one_legged_position(self, opportunity, side: str, leg) -> str:
        """Открывает одноногую paper позицию (только YES или только NO)."""
        import uuid as _uuid
        pos_id = str(_uuid.uuid4())
        now = datetime.utcnow().isoformat()
        exec_status = f"paper_one_legged_{side}"
        leg_cost = leg.filled_shares * leg.avg_price
        yes_avg = leg.avg_price if side == "yes" else None
        no_avg = leg.avg_price if side == "no" else None
        yes_shares = leg.filled_shares if side == "yes" else None
        no_shares = leg.filled_shares if side == "no" else None
        self.conn.execute(
            """
            INSERT INTO positions (
                id, pair_key, symbol, title, expiry, venue_yes, market_yes, venue_no, market_no,
                polymarket_title, kalshi_title, match_score, expiry_delta_seconds,
                polymarket_reference_price, kalshi_reference_price, polymarket_rules, kalshi_rules,
                shares, yes_ask, no_ask, ask_sum, total_cost, expected_profit,
                yes_avg_price, no_avg_price, yes_filled_shares, no_filled_shares,
                opened_at, status, execution_status, is_paper, initially_one_legged
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                pos_id,
                opportunity.pair_key, opportunity.symbol, opportunity.title,
                opportunity.expiry.isoformat(),
                opportunity.buy_yes_venue,
                opportunity.polymarket_market_id if opportunity.buy_yes_venue == "polymarket" else opportunity.kalshi_market_id,
                opportunity.buy_no_venue,
                opportunity.polymarket_market_id if opportunity.buy_no_venue == "polymarket" else opportunity.kalshi_market_id,
                opportunity.polymarket_title, opportunity.kalshi_title,
                opportunity.match_score, opportunity.expiry_delta_seconds,
                opportunity.polymarket_reference_price, opportunity.kalshi_reference_price,
                opportunity.polymarket_rules, opportunity.kalshi_rules,
                opportunity.shares, opportunity.yes_ask, opportunity.no_ask, opportunity.ask_sum,
                leg_cost, -leg_cost,
                yes_avg, no_avg, yes_shares, no_shares,
                now, "open", exec_status, 1, side,
            ),
        )
        self.conn.commit()
        return pos_id

    def complete_paper_one_legged(
        self, position_id: str, missing_side: str, price: float, shares: float
    ) -> None:
        """Симулирует докупку второй ноги одноногой paper позиции."""
        if missing_side == "yes":
            self.conn.execute(
                "UPDATE positions SET "
                "yes_avg_price=?, yes_filled_shares=?, "
                "total_cost = COALESCE(no_avg_price,0)*COALESCE(no_filled_shares,0) + ?*?, "
                "execution_status='paper' WHERE id=?",
                (price, shares, price, shares, position_id),
            )
        else:
            self.conn.execute(
                "UPDATE positions SET "
                "no_avg_price=?, no_filled_shares=?, "
                "total_cost = COALESCE(yes_avg_price,0)*COALESCE(yes_filled_shares,0) + ?*?, "
                "execution_status='paper' WHERE id=?",
                (price, shares, price, shares, position_id),
            )
        self.conn.commit()

    def resolve_position(
        self,
        position_id: str,
        winning_side: str,
        pnl: float,
        actual_pnl: float | None,
        polymarket_result: str | None,
        kalshi_result: str | None,
        lock_valid: bool,
        polymarket_snapshot_resolved: str | None = None,
        kalshi_snapshot_resolved: str | None = None,
        polymarket_redeem_tx: str | None = None,
        polymarket_redeem_gas_cost: float | None = None,
        polymarket_redeem_ms: float | None = None,
        kalshi_close_price: float | None = None,
        pm_close_price: float | None = None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE positions SET
                status='resolved', resolved_at=?, winning_side=?, pnl=?, actual_pnl=?,
                polymarket_result=?, kalshi_result=?, lock_valid=?,
                polymarket_snapshot_resolved=?, kalshi_snapshot_resolved=?,
                polymarket_redeem_tx=?, polymarket_redeem_gas_cost=?, polymarket_redeem_ms=?,
                kalshi_close_price=?, pm_close_price=?
            WHERE id=?
            """,
            (
                datetime.utcnow().isoformat(),
                winning_side, pnl, actual_pnl,
                polymarket_result, kalshi_result, 1 if lock_valid else 0,
                polymarket_snapshot_resolved, kalshi_snapshot_resolved,
                polymarket_redeem_tx, polymarket_redeem_gas_cost, polymarket_redeem_ms,
                kalshi_close_price, pm_close_price,
                position_id,
            ),
        )
        self.conn.commit()
        # Paper позиции не считаем в реальный P&L
        is_paper = self.conn.execute(
            "SELECT is_paper FROM positions WHERE id=?", (position_id,)
        ).fetchone()
        if not (is_paper and is_paper["is_paper"]):
            self._update_daily_stats(pnl or 0.0)
        self.audit("position_resolved", position_id, {
            "winning_side": winning_side, "pnl": pnl, "lock_valid": lock_valid,
        })

    def get_position(self, position_id: str) -> CrossPosition | None:
        row = self.conn.execute("SELECT * FROM positions WHERE id=?", (position_id,)).fetchone()
        return self._row_to_position(row) if row else None

    def get_open_positions(self) -> list[CrossPosition]:
        rows = self.conn.execute(
            "SELECT * FROM positions WHERE status='open' ORDER BY expiry ASC"
        ).fetchall()
        return [self._row_to_position(r) for r in rows]

    def get_positions_pending_redeem_retry(self) -> list[CrossPosition]:
        rows = self.conn.execute(
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
        return [self._row_to_position(r) for r in rows]

    def update_polymarket_redeem(
        self,
        position_id: str,
        redeem_tx: str,
        redeem_gas_cost: float | None,
        redeem_ms: float | None,
    ) -> None:
        self.conn.execute(
            """
            UPDATE positions
            SET polymarket_redeem_tx=?,
                polymarket_redeem_gas_cost=?,
                polymarket_redeem_ms=?
            WHERE id=?
            """,
            (redeem_tx, redeem_gas_cost, redeem_ms, position_id),
        )
        self.conn.commit()
        self.audit(
            "polymarket_redeem_recorded",
            position_id,
            {
                "tx": redeem_tx,
                "gas_cost": redeem_gas_cost,
                "redeem_ms": redeem_ms,
            },
        )

    def get_orphaned_positions(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM positions WHERE execution_status IN ('orphaned_kalshi','orphaned_polymarket') ORDER BY opened_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def has_open_position(self, pair_key: str, yes_venue: str, no_venue: str) -> bool:
        return self.conn.execute(
            "SELECT 1 FROM positions WHERE pair_key=? AND venue_yes=? AND venue_no=? AND status='open'",
            (pair_key, yes_venue, no_venue),
        ).fetchone() is not None

    def count_positions_for_pair(self, pair_key: str, yes_venue: str, no_venue: str) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS cnt FROM positions WHERE pair_key=? AND venue_yes=? AND venue_no=?",
            (pair_key, yes_venue, no_venue),
        ).fetchone()
        return int(row["cnt"] or 0)

    def _row_to_position(self, row) -> CrossPosition:
        d = dict(row)
        return CrossPosition(
            id=d["id"],
            pair_key=d["pair_key"],
            symbol=d["symbol"],
            title=d["title"],
            expiry=datetime.fromisoformat(d["expiry"]),
            venue_yes=d["venue_yes"],
            market_yes=d["market_yes"],
            venue_no=d["venue_no"],
            market_no=d["market_no"],
            shares=d["shares"],
            yes_ask=d["yes_ask"],
            no_ask=d["no_ask"],
            ask_sum=d["ask_sum"],
            total_cost=d["total_cost"],
            expected_profit=d["expected_profit"],
            opened_at=datetime.fromisoformat(d["opened_at"]),
            status=d.get("status", "open"),
            resolved_at=datetime.fromisoformat(d["resolved_at"]) if d.get("resolved_at") else None,
            pnl=d.get("pnl"),
            polymarket_result=d.get("polymarket_result"),
            kalshi_result=d.get("kalshi_result"),
            lock_valid=bool(d["lock_valid"]) if d.get("lock_valid") is not None else None,
            winning_side=d.get("winning_side"),
            is_paper=bool(d.get("is_paper", 0)),
            yes_filled_shares=d.get("yes_filled_shares"),
            yes_avg_price=d.get("yes_avg_price"),
            yes_best_ask=d.get("yes_best_ask"),
            no_filled_shares=d.get("no_filled_shares"),
            no_avg_price=d.get("no_avg_price"),
            no_best_ask=d.get("no_best_ask"),
            polymarket_title=d.get("polymarket_title"),
            kalshi_title=d.get("kalshi_title"),
            match_score=d.get("match_score"),
            expiry_delta_seconds=d.get("expiry_delta_seconds"),
            polymarket_snapshot_open=d.get("polymarket_snapshot_open"),
            kalshi_snapshot_open=d.get("kalshi_snapshot_open"),
            polymarket_snapshot_resolved=d.get("polymarket_snapshot_resolved"),
            kalshi_snapshot_resolved=d.get("kalshi_snapshot_resolved"),
        )

    # ── Edge trajectory ticks ──────────────────────────────────────────

    def insert_edge_tick(
        self,
        position_id: str,
        ts: str,
        seconds_to_expiry: float,
        pm_yes_ask: float | None,
        pm_no_ask: float | None,
        kalshi_yes_ask: float | None,
        kalshi_no_ask: float | None,
        yes_ask: float,
        no_ask: float,
        edge: float,
        binance_price: float | None = None,
        binance_distance_pct: float | None = None,
        pm_yes_bid: float | None = None,
        pm_no_bid: float | None = None,
        kalshi_yes_bid: float | None = None,
        kalshi_no_bid: float | None = None,
    ) -> None:
        self.conn.execute(
            """
            INSERT INTO edge_ticks
              (position_id, ts, seconds_to_expiry,
               pm_yes_ask, pm_no_ask, kalshi_yes_ask, kalshi_no_ask,
               yes_ask, no_ask, edge,
               binance_price, binance_distance_pct,
               pm_yes_bid, pm_no_bid, kalshi_yes_bid, kalshi_no_bid)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                position_id, ts, round(seconds_to_expiry, 2),
                pm_yes_ask, pm_no_ask, kalshi_yes_ask, kalshi_no_ask,
                round(yes_ask, 6), round(no_ask, 6), round(edge, 6),
                binance_price, binance_distance_pct,
                pm_yes_bid, pm_no_bid, kalshi_yes_bid, kalshi_no_bid,
            ),
        )
        self.conn.commit()

    # ── Balance reconciliation ──────────────────────────────────────────

    def save_balance_snapshot(
        self,
        pm_balance: float,
        k_balance: float,
        source: str = "periodic",
    ) -> int:
        """Сохраняет текущие балансы + состояние позиций. Возвращает id снапшота."""
        now = datetime.utcnow().isoformat()
        r = self.conn.execute(
            "SELECT COALESCE(SUM(CASE "
            "WHEN execution_status='one_legged_kalshi' "
            "    THEN kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0) "
            "WHEN execution_status='one_legged_polymarket' "
            "    THEN polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0) "
            "ELSE total_cost END), 0) FROM positions WHERE status='open' AND is_paper=0"
        ).fetchone()
        open_cost = float(r[0])
        r2 = self.conn.execute(
            "SELECT COALESCE(SUM(actual_pnl), 0), COUNT(*) FROM positions "
            "WHERE status='resolved' AND is_paper=0"
        ).fetchone()
        resolved_pnl = float(r2[0])
        resolved_count = int(r2[1])

        cursor = self.conn.execute(
            "INSERT INTO balance_snapshots "
            "(ts, pm_balance, k_balance, open_cost, resolved_pnl, resolved_count, source) "
            "VALUES (?,?,?,?,?,?,?)",
            (now, pm_balance, k_balance, open_cost, resolved_pnl, resolved_count, source),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_last_balance_snapshot(self) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM balance_snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        return dict(row) if row else None

    def check_balance_reconciliation(
        self,
        pm_balance: float,
        k_balance: float,
        threshold: float = 5.0,
    ) -> tuple[bool, float, str]:
        """Сравнивает текущие балансы с последним снапшотом + изменения в БД.

        Возвращает (ok, delta, details).
        ok=True если расхождение в пределах порога.
        """
        snap = self.get_last_balance_snapshot()
        if snap is None:
            return True, 0.0, "no_previous_snapshot"

        # Текущее состояние
        r = self.conn.execute(
            "SELECT COALESCE(SUM(CASE "
            "WHEN execution_status='one_legged_kalshi' "
            "    THEN kalshi_fill_shares * kalshi_fill_price + COALESCE(kalshi_order_fee, 0) "
            "WHEN execution_status='one_legged_polymarket' "
            "    THEN polymarket_fill_shares * polymarket_fill_price + COALESCE(polymarket_order_fee, 0) "
            "ELSE total_cost END), 0) FROM positions WHERE status='open' AND is_paper=0"
        ).fetchone()
        current_open_cost = float(r[0])
        r2 = self.conn.execute(
            "SELECT COALESCE(SUM(actual_pnl), 0), COUNT(*) FROM positions "
            "WHERE status='resolved' AND is_paper=0"
        ).fetchone()
        current_resolved_pnl = float(r2[0])
        current_resolved_count = int(r2[1])

        # Дельта resolved PnL с момента снапшота
        pnl_change = current_resolved_pnl - snap["resolved_pnl"]
        # Дельта open cost
        open_cost_change = current_open_cost - snap["open_cost"]

        # Ожидаемое изменение баланса = pnl_change - open_cost_change
        # (resolved PnL добавляет к балансу, новые open позиции уменьшают)
        prev_total = snap["pm_balance"] + snap["k_balance"]
        current_total = pm_balance + k_balance
        expected_total = prev_total + pnl_change - open_cost_change

        delta = current_total - expected_total

        new_resolved = current_resolved_count - snap["resolved_count"]
        details = (
            f"prev={prev_total:.2f} curr={current_total:.2f} "
            f"expected={expected_total:.2f} delta={delta:+.2f} | "
            f"pnl_change={pnl_change:+.2f} open_cost_change={open_cost_change:+.2f} "
            f"new_resolved={new_resolved}"
        )

        return abs(delta) <= threshold, delta, details

    # ── Аудит ──────────────────────────────────────────────────────────

    def audit(self, event_type: str, position_id: str | None, details: dict) -> None:
        self.conn.execute(
            "INSERT INTO audit_log (timestamp, event_type, position_id, details) VALUES (?,?,?,?)",
            (datetime.utcnow().isoformat(), event_type, position_id, json.dumps(details)),
        )
        self.conn.commit()

    def get_audit_log(self, last_n: int = 50) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (last_n,)
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Дневная статистика ─────────────────────────────────────────────

    def _update_daily_stats(self, pnl: float) -> None:
        today = datetime.utcnow().date().isoformat()
        self.conn.execute(
            """
            INSERT INTO daily_stats (date, trades_count, realized_pnl)
            VALUES (?, 1, ?)
            ON CONFLICT(date) DO UPDATE SET
                trades_count = trades_count + 1,
                realized_pnl = realized_pnl + excluded.realized_pnl
            """,
            (today, pnl),
        )
        self.conn.commit()

    def daily_realized_pnl(self) -> float:
        today = datetime.utcnow().date().isoformat()
        row = self.conn.execute(
            "SELECT SUM(actual_pnl) AS pnl FROM positions "
            "WHERE status='resolved' AND is_paper=0 AND exclude_from_stats=0 "
            "AND date(resolved_at)=?", (today,)
        ).fetchone()
        return float(row["pnl"] or 0.0)

    def last_trade_time(self) -> float | None:
        row = self.conn.execute(
            "SELECT opened_at FROM positions WHERE execution_status='both_filled' ORDER BY opened_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        try:
            return datetime.fromisoformat(row["opened_at"]).timestamp()
        except Exception:
            return None

    # ── Сводная статистика ─────────────────────────────────────────────

    def stats(self) -> dict:
        positions = [self._row_to_position(r) for r in self.conn.execute("SELECT * FROM positions").fetchall()]
        open_pos = [p for p in positions if p.status == "open"]
        resolved = [p for p in positions if p.status == "resolved"]
        rows_for_pnl = self.conn.execute(
            "SELECT actual_pnl FROM positions WHERE status='resolved' AND is_paper=0 AND exclude_from_stats=0"
        ).fetchall()
        realized_pnl = sum(float(r["actual_pnl"] or 0.0) for r in rows_for_pnl)

        orphans = self.conn.execute(
            "SELECT COUNT(*) AS c FROM positions WHERE execution_status IN ('orphaned_kalshi','orphaned_polymarket')"
        ).fetchone()["c"]

        return {
            "open": len(open_pos),
            "resolved": len(resolved),
            "realized_pnl": realized_pnl,
            "daily_pnl": self.daily_realized_pnl(),
            "orphaned": orphans,
        }
