from __future__ import annotations

import threading
import uuid
from datetime import datetime

from typing import Optional

from typing import TYPE_CHECKING

from py_clob_client.clob_types import MarketOrderArgs, OrderArgs, OrderType, BalanceAllowanceParams, AssetType, RoundConfig
from py_clob_client.order_builder.builder import ROUNDING_CONFIG

# SDK баг: amount=4 для tick "0.01", но CLOB API требует ≤2 знака для maker BUY
ROUNDING_CONFIG["0.01"] = RoundConfig(price=2, size=2, amount=2)

from real_arb_bot.clients import PolymarketTrader

if TYPE_CHECKING:
    from oracle_arb_bot.telegram_notify import OracleTelegramNotifier

from oracle_arb_bot.db import OracleDB
from oracle_arb_bot.models import OracleMarket, RealBet
from oracle_arb_bot.strategy import SignalResult

class OracleRealTrader:
    """
    Управляет реальным депозитом и размещает ордера на Polymarket CLOB.

    Депозит:
      - Стартует с initial_deposit (по умолчанию $8)
      - delta = max(initial_deposit, peak * floor_pct)
      - floor = max(0, peak - delta)  — trailing floor
      - Ставка $1; можно ставить пока (balance - stake) >= floor
      - При WIN: balance += shares_filled (stake уже был вычтен при ставке)
      - При LOSS: ничего (stake уже вычтен)
    """

    def __init__(
        self,
        db: OracleDB,
        stake_usd: float = 1.0,
        initial_deposit: float = 6.0,
        floor_pct: float = 0.20,
        tg: "Optional[OracleTelegramNotifier]" = None,
        price_10s_fn=None,
        max_price: float = 0.48,
    ) -> None:
        self._db = db
        self._stake = stake_usd
        self._initial_deposit = initial_deposit
        self._floor_pct = floor_pct
        self._tg = tg
        self._price_10s_fn = price_10s_fn
        self._max_price = max_price
        self._pm = PolymarketTrader()
        # Деdup: не повторяем попытку для одной market+side в рамках сессии
        self._attempted: set[tuple[str, str]] = set()
        self._attempted_lock = threading.Lock()
        self._refresh_clob_balance()
        db.init_real_deposit(initial_deposit)
        bal, peak = db.get_real_deposit()
        floor = self._calc_floor(peak)
        print(
            f"[real] депозит ${bal:.2f} | peak ${peak:.2f} | floor ${floor:.2f} | "
            f"доступно ${max(0.0, bal - floor):.2f}"
        )

    # ── Checks ────────────────────────────────────────────────────────────

    def _calc_floor(self, peak: float) -> float:
        """Флор = peak - delta, где delta = max(initial_deposit, peak * floor_pct).
        При депозите 6 и пике 6: delta=6, floor=0 (можем потерять всё).
        При пике 12: delta=6, floor=6. При пике 100: delta=20, floor=80."""
        delta = max(self._initial_deposit, peak * self._floor_pct)
        return max(0.0, peak - delta)

    def can_bet(self) -> bool:
        bal, peak = self._db.get_real_deposit()
        floor = self._calc_floor(peak)
        return bal >= self._stake and (bal - self._stake) >= floor

    def deposit_info(self) -> str:
        bal, peak = self._db.get_real_deposit()
        floor = self._calc_floor(peak)
        avail = max(0.0, bal - floor)
        return f"депозит ${bal:.2f} (peak ${peak:.2f} | floor ${floor:.2f} | доступно ${avail:.2f})"

    def sync_balance(self) -> Optional[float]:
        """Запрашивает реальный CLOB баланс для логирования (не перезаписывает виртуальный депозит).
        Возвращает реальный баланс или None при ошибке."""
        try:
            resp = self._pm._client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            real_bal = float(resp.get("balance", 0)) / 1e6
            db_bal, peak = self._db.get_real_deposit()
            floor = self._calc_floor(peak)
            print(
                f"[real] CLOB ${real_bal:.2f} | депозит ${db_bal:.2f} | peak ${peak:.2f} | "
                f"floor ${floor:.2f} | доступно ${max(0.0, db_bal - floor):.2f}"
            )
            return real_bal
        except Exception as e:
            print(f"[real] sync_balance failed: {e}")
            return None

    # ── Place ─────────────────────────────────────────────────────────────

    def _refresh_clob_balance(self) -> None:
        """Обновляет кэш CLOB-сервера по текущему on-chain балансу кошелька."""
        try:
            self._pm._client.update_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            r = self._pm._client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            clob_bal = float(r.get("balance", 0)) / 1e6
            print(f"[real] CLOB balance refreshed: ${clob_bal:.4f}")
        except Exception as e:
            print(f"[real] CLOB balance refresh failed: {e}")

    def get_balance_info(self) -> dict:
        """Возвращает wallet USDC и CLOB deposit balance."""
        result = {}
        try:
            result["wallet_usdc"] = self._pm.get_balance()
        except Exception as e:
            result["wallet_usdc_err"] = str(e)
        try:
            resp = self._pm._client.get_balance_allowance(
                BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            )
            result["clob_balance"] = float(resp.get("balance", 0)) / 1e6
            result["clob_allowance"] = float(resp.get("allowance", 0)) / 1e6
        except Exception as e:
            result["clob_err"] = str(e)
        return result

    def try_place(
        self,
        market: OracleMarket,
        signal: SignalResult,
        current_price: float,
        now: datetime,
        delta_pct: float = 0.0,
        cheap_delta: float = 0.10,
    ) -> None:
        key = (market.market_id, signal.side)
        with self._attempted_lock:
            if key in self._attempted:
                return
            self._attempted.add(key)

        if self._db.has_real_bet(market.market_id, signal.side):
            return

        if not self.can_bet():
            bal, peak = self._db.get_real_deposit()
            floor = self._calc_floor(peak)
            reason = f"депозит ${bal:.2f} floor ${floor:.2f}"
            print(f"[real] skip {market.symbol} {signal.side}: {reason}")
            if self._tg:
                self._tg.send_bet_failed(market.symbol, signal.side, reason)
            return

        token_id = market.yes_token_id if signal.side == "yes" else market.no_token_id
        if not token_id:
            reason = "нет token_id"
            print(f"[real] skip {market.symbol} {signal.side}: {reason}")
            if self._tg:
                self._tg.send_bet_failed(market.symbol, signal.side, reason)
            return

        requested_price = market.yes_ask if signal.side == "yes" else market.no_ask

        import time as _time
        from decimal import Decimal, ROUND_DOWN, ROUND_UP
        try:
            # Лимитный ордер: ставим по WS-цене + до 3 центов слиппейджа
            limit_price = float(Decimal(str(min(requested_price + 0.03, self._max_price)))
                                .quantize(Decimal("0.01"), rounding=ROUND_UP))

            if limit_price < 0.50 and abs(delta_pct) < cheap_delta:
                print(f"[real] skip cheap {market.symbol} {signal.side}: "
                      f"price {limit_price:.3f} < 0.50, delta {abs(delta_pct):.4f}% < {cheap_delta}%")
                return

            size = float(Decimal(str(self._stake / limit_price))
                         .quantize(Decimal("0.01"), rounding=ROUND_UP))
            if size <= 0:
                return

            args = OrderArgs(
                token_id=token_id,
                price=limit_price,
                size=size,
                side="BUY",
            )
            signed = self._pm._client.create_order(args)
            resp = self._pm._client.post_order(signed, orderType=OrderType.FOK)

            order_id = resp.get("orderID", "")
            status = resp.get("status", "")

            # Проверяем реальный статус и цену через get_order
            real_fill_price = None
            real_size_matched = None
            if order_id:
                _time.sleep(0.2)
                try:
                    info = self._pm._client.get_order(order_id)
                    status = info.get("status", status)
                    if info.get("associate_trades"):
                        trades = info["associate_trades"]
                        total_cost = sum(float(t.get("price", 0)) * float(t.get("size", 0)) for t in trades)
                        total_size = sum(float(t.get("size", 0)) for t in trades)
                        if total_size > 0:
                            real_fill_price = round(total_cost / total_size, 6)
                            real_size_matched = round(total_size, 6)
                    if real_fill_price is None and info.get("price"):
                        real_fill_price = float(info["price"])
                    if real_size_matched is None and info.get("size_matched"):
                        real_size_matched = float(info["size_matched"])
                except Exception as poll_err:
                    print(f"[real] order poll error: {poll_err}")
        except Exception as exc:
            reason = f"ошибка API: {exc}"
            print(f"[real] ордер ОШИБКА {market.symbol} {signal.side}: {exc}")
            if self._tg:
                self._tg.send_bet_failed(market.symbol, signal.side, reason)
            return

        matched = status.upper() in ("MATCHED", "FILLED")
        if not matched:
            reason = f"FOK не исполнен (status={status}, limit={limit_price:.3f})"
            print(f"[real] ордер НЕ ИСПОЛНЕН {market.symbol} {signal.side}: {reason}")
            if self._tg:
                self._tg.send_bet_failed(market.symbol, signal.side, reason)
            return

        price = real_fill_price if real_fill_price else limit_price
        size = real_size_matched if real_size_matched else size
        actual_stake = round(price * size, 6)

        bet = RealBet(
            id=str(uuid.uuid4()),
            market_id=market.market_id,
            symbol=market.symbol,
            interval_minutes=market.interval_minutes,
            market_start=market.market_start,
            market_end=market.expiry,
            placed_at=now,
            market_minute=signal.market_minute,
            side=signal.side,
            requested_price=requested_price,
            fill_price=price,
            shares_requested=size,
            shares_filled=size,
            stake_usd=actual_stake,
            order_id=order_id,
            order_status=status,
            delta_pct=round(signal.delta_pct, 4),
            pm_open_price=market.pm_open_price,
            binance_price_at_bet=current_price,
        )

        self._db.record_real_bet(bet)
        self._db.deduct_real_deposit(actual_stake)
        self._db.audit("real_bet_placed", bet.id, {
            "symbol": market.symbol,
            "side": signal.side,
            "fill_price": price,
            "shares_filled": size,
            "stake_usd": actual_stake,
            "order_id": order_id,
            "delta_pct": round(signal.delta_pct, 4),
        })

        bal, _ = self._db.get_real_deposit()
        print(
            f"[real] СТАВКА {market.symbol} {market.interval_minutes}m "
            f"{signal.side.upper()} price={price:.3f} "
            f"shares={size:.2f} stake=${actual_stake:.2f} | депозит ${bal:.2f}"
        )
        if self._tg:
            self._tg.send_bet(
                market.symbol, signal.side, price,
                signal.delta_pct, actual_stake, label="real",
                market_slug=market.pm_event_slug,
            )

        # Фиксируем цену через 10 секунд
        if self._price_10s_fn:
            import threading as _threading
            _threading.Thread(
                target=self._price_10s_fn,
                args=(bet.id, token_id, 10, "real_bets"),
                daemon=True,
            ).start()

    # ── Resolve ───────────────────────────────────────────────────────────

    def resolve(self, bet: RealBet, winning_side: str, pm_close_price: Optional[float]) -> None:
        won = winning_side == bet.side

        if won:
            # Пытаемся redeem (может быть уже auto-settled или redeemed вручную)
            try:
                result = self._pm.redeem(bet.market_id)
                if result.success and result.payout_usdc > 0:
                    print(
                        f"[real][redeem] {bet.symbol} OK | payout=${result.payout_usdc:.4f} "
                        f"| gas={result.gas_used} ({result.gas_cost_pol:.6f} POL)"
                    )
                elif result.pending:
                    print(f"[real][redeem] {bet.symbol} TX pending — резолв продолжаем")
                else:
                    print(f"[real][redeem] {bet.symbol} payout=0 (уже auto-settled/redeemed)")
                self._refresh_clob_balance()
            except Exception as e:
                print(f"[real][redeem] {bet.symbol} ошибка: {e} — продолжаем резолв")

            # Каждая выигрышная шера = $1 USDC, не зависим от redeem().payout_usdc
            payout = bet.shares_filled
            self._db.add_real_deposit(payout)
            pnl = payout - bet.stake_usd
        else:
            pnl = -bet.stake_usd  # уже вычтено при ставке

        self._db.resolve_real_bet(bet.id, winning_side, pm_close_price, round(pnl, 6))
        self._db.audit("real_bet_resolved", bet.id, {
            "symbol": bet.symbol,
            "side": bet.side,
            "winning_side": winning_side,
            "won": won,
            "pnl": round(pnl, 4),
            "payout": bet.shares_filled if won else 0.0,
        })

        bal, _ = self._db.get_real_deposit()
        tag = "WIN" if won else "LOSS"
        print(
            f"[real][resolve] {bet.symbol} {bet.side} → {winning_side} "
            f"| {tag} | pnl=${pnl:+.2f} | депозит ${bal:.2f}"
        )
        self._refresh_clob_balance()
