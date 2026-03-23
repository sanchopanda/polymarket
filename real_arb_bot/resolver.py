from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from real_arb_bot.clients import PolymarketTrader, KalshiTrader
from real_arb_bot.db import RealArbDB


class PositionResolver:
    KALSHI_POLL_SECONDS = 5
    PM_POLL_SECONDS = 15

    def __init__(
        self,
        pm_trader: PolymarketTrader,
        kalshi_trader: KalshiTrader,
        pm_feed,
        kalshi_feed,
        db: RealArbDB,
        notifier=None,
    ) -> None:
        self.pm = pm_trader
        self.kalshi = kalshi_trader
        self.pm_feed = pm_feed
        self.kalshi_feed = kalshi_feed
        self.db = db
        self.notifier = notifier

    def resolve_all(self) -> None:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for position in self.db.get_open_positions():
            if position.expiry > now:
                continue
            self._resolve_one(position)

    def _resolve_one(self, position) -> None:
        # Проверяем paper позиции
        row_meta = self.db.conn.execute(
            "SELECT is_paper, execution_status FROM positions WHERE id=?", (position.id,)
        ).fetchone()
        if row_meta and row_meta["is_paper"]:
            self._resolve_paper(position)
            return

        # Пропускаем failed позиции — ничего не торговалось
        exec_status = getattr(position, "execution_status", None)
        if exec_status is None:
            row = self.db.conn.execute(
                "SELECT execution_status FROM positions WHERE id=?", (position.id,)
            ).fetchone()
            exec_status = row["execution_status"] if row else None
        if exec_status == "failed":
            self.db.resolve_position(
                position_id=position.id, winning_side="n/a", pnl=0.0, actual_pnl=0.0,
                polymarket_result=None, kalshi_result=None, lock_valid=False,
            )
            print(f"[resolve] {position.symbol} | execution_status=failed → pnl=0")
            return

        pm_result, pm_snapshot = self._check_polymarket(position)
        kalshi_result, kalshi_snapshot = self._check_kalshi(position)

        if pm_result is None or kalshi_result is None:
            return

        yes_wins = self._leg_wins(position.venue_yes, "yes", pm_result, kalshi_result)
        no_wins = self._leg_wins(position.venue_no, "no", pm_result, kalshi_result)
        lock_valid = (yes_wins + no_wins) == 1

        # PnL по реальным fills, не planned shares
        row = self.db.conn.execute(
            "SELECT kalshi_fill_price, kalshi_fill_shares, kalshi_order_fee, "
            "polymarket_fill_price, polymarket_fill_shares, polymarket_order_fee "
            "FROM positions WHERE id=?", (position.id,)
        ).fetchone()

        k_shares = float(row["kalshi_fill_shares"] or 0)
        k_price = float(row["kalshi_fill_price"] or 0)
        k_fee = float(row["kalshi_order_fee"] or 0)
        pm_shares = float(row["polymarket_fill_shares"] or 0)
        pm_price = float(row["polymarket_fill_price"] or 0)
        pm_fee = float(row["polymarket_order_fee"] or 0)

        # Kalshi: payout = shares * $1 если выигрывает, иначе 0
        kalshi_side = "yes" if position.venue_yes == "kalshi" else "no"
        kalshi_won = kalshi_result == kalshi_side
        kalshi_payout = k_shares * 1.0 if kalshi_won else 0.0
        kalshi_cost = k_shares * k_price + k_fee

        # Polymarket: payout = shares * $1 если выигрывает, иначе 0
        pm_side = "yes" if position.venue_yes == "polymarket" else "no"
        pm_won = pm_result == pm_side
        pm_payout = pm_shares * 1.0 if pm_won else 0.0
        pm_cost = pm_shares * pm_price + pm_fee

        pnl = (kalshi_payout + pm_payout) - (kalshi_cost + pm_cost)
        winning_side = "yes" if yes_wins and not no_wins else ("no" if no_wins and not yes_wins else "mismatch")

        # Polymarket: нужен redeem
        redeem_tx = None
        redeem_gas = None
        redeem_ms = None
        pm_payout_real = 0.0
        if position.venue_yes == "polymarket" or position.venue_no == "polymarket":
            pm_market_id = position.market_yes if position.venue_yes == "polymarket" else position.market_no
            print(f"[resolve] Polymarket redeem: {pm_market_id}")
            redeem = self.pm.redeem(pm_market_id)
            if redeem.success:
                redeem_tx = redeem.tx_hash
                redeem_gas = redeem.gas_cost_pol
                redeem_ms = redeem.total_ms
                pm_payout_real = redeem.payout_usdc
                print(f"[resolve] PM payout: ${pm_payout_real:.2f}")
            else:
                print(f"[resolve] redeem failed: {redeem.error}")

        # actual_pnl из redeem ненадёжен: Polymarket часто авто-зачисляет до нашего redeem
        # Используем расчётный pnl как actual — он основан на реальных fills
        actual_pnl = pnl
        print(
            f"[resolve] pnl=${pnl:+.2f} | kalshi: {'WIN' if kalshi_won else 'LOSE'} {k_shares}x@{k_price} "
            f"| pm: {'WIN' if pm_won else 'LOSE'} {pm_shares:.2f}x@{pm_price}"
            + (f" | pm_redeem_payout=${pm_payout_real:.2f}" if pm_payout_real > 0 else "")
        )

        self.db.resolve_position(
            position_id=position.id,
            winning_side=winning_side,
            pnl=pnl,
            actual_pnl=actual_pnl,
            polymarket_result=pm_result,
            kalshi_result=kalshi_result,
            lock_valid=lock_valid,
            polymarket_snapshot_resolved=pm_snapshot,
            kalshi_snapshot_resolved=kalshi_snapshot,
            polymarket_redeem_tx=redeem_tx,
            polymarket_redeem_gas_cost=redeem_gas,
            polymarket_redeem_ms=redeem_ms,
        )
        print(
            f"[resolve] {position.symbol} | pm={pm_result} kalshi={kalshi_result} "
            f"| lock_valid={lock_valid} | pnl=${pnl:+.2f}"
            + (f" | NOT a true arb!" if not lock_valid else "")
        )
        if self.notifier:
            self.notifier.notify_resolve(
                symbol=position.symbol,
                pm_result=pm_result,
                kalshi_result=kalshi_result,
                pnl=pnl,
                lock_valid=lock_valid,
            )

    def _resolve_paper(self, position) -> None:
        pm_result, _ = self._check_polymarket(position)
        kalshi_result, _ = self._check_kalshi(position)

        if pm_result is None or kalshi_result is None:
            return

        yes_wins = self._leg_wins(position.venue_yes, "yes", pm_result, kalshi_result)
        no_wins = self._leg_wins(position.venue_no, "no", pm_result, kalshi_result)
        lock_valid = (yes_wins + no_wins) == 1

        # PnL по плановым shares (реальных ордеров не было)
        pnl = position.shares - position.total_cost if lock_valid else -position.total_cost
        winning_side = "yes" if yes_wins and not no_wins else ("no" if no_wins and not yes_wins else "mismatch")

        self.db.resolve_position(
            position_id=position.id,
            winning_side=winning_side,
            pnl=pnl,
            actual_pnl=pnl,
            polymarket_result=pm_result,
            kalshi_result=kalshi_result,
            lock_valid=lock_valid,
        )
        tag = "✓ profit" if pnl > 0 else "✗ loss"
        print(
            f"[resolve][PAPER] {position.symbol} | pm={pm_result} kalshi={kalshi_result} "
            f"| lock_valid={lock_valid} | pnl=${pnl:+.2f} ({tag})"
        )
        if self.notifier:
            self.notifier.notify_resolve(
                symbol=position.symbol,
                pm_result=pm_result,
                kalshi_result=kalshi_result,
                pnl=pnl,
                lock_valid=lock_valid,
                is_paper=True,
            )

    def _check_polymarket(self, position) -> tuple[str | None, str | None]:
        market_id = position.market_yes if position.venue_yes == "polymarket" else position.market_no
        market = self.pm_feed.client.fetch_market(market_id)
        if market is None:
            return None, None

        snapshot = json.dumps({
            "stage": "resolve", "venue": "polymarket",
            "market_id": market.id,
            "outcomes": market.outcomes,
            "outcome_prices": market.outcome_prices,
        })
        try:
            up_idx = next(i for i, o in enumerate(market.outcomes) if o.lower() == "up")
            down_idx = next(i for i, o in enumerate(market.outcomes) if o.lower() == "down")
        except StopIteration:
            return None, snapshot

        if market.outcome_prices[up_idx] >= 0.9:
            return "yes", snapshot
        if market.outcome_prices[down_idx] >= 0.9:
            return "no", snapshot
        return None, snapshot

    def _check_kalshi(self, position) -> tuple[str | None, str | None]:
        market_id = position.market_yes if position.venue_yes == "kalshi" else position.market_no
        payload = self.kalshi.get_market(market_id)
        if payload is None:
            return None, None

        snapshot = json.dumps({
            "stage": "resolve", "venue": "kalshi",
            "ticker": payload.get("ticker"),
            "status": payload.get("status"),
            "result": payload.get("result"),
        })
        result = str(payload.get("result") or "").lower()
        if result in {"yes", "no"}:
            return result, snapshot

        status = str(payload.get("status") or "").lower()
        if status in {"closed", "determined", "finalized"}:
            return None, snapshot
        return None, snapshot

    def _leg_wins(self, venue: str, side: str, pm_result: str, kalshi_result: str) -> bool:
        if venue == "polymarket":
            return pm_result == side
        return kalshi_result == side
