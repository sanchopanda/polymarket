from __future__ import annotations

import math
import random
import time
from datetime import datetime, timezone
from typing import List, Optional

from src.api.bybit import BybitClient
from src.config import Config
from src.db.models import Series, Trade
from src.db.store import Store


# Задержка перед первой проверкой (секунд) — ждём пока позиция точно откроется
OPEN_GRACE_SEC = 5


def _floor_to_step(qty: float, step: float) -> float:
    """Округляет qty вниз до кратного step."""
    return math.floor(qty / step + 1e-9) * step


def _format_qty(qty: float, step: float) -> str:
    """Форматирует qty в строку с точностью шага (без лишних float-артефактов)."""
    if step >= 1.0:
        return str(int(round(qty)))
    decimals = max(0, round(-math.log10(step)))
    return f"{qty:.{decimals}f}"


class MartingaleEngine:
    def __init__(self, config: Config, store: Store, client: BybitClient) -> None:
        self.config = config
        self.store = store
        self.client = client
        self._leverage_set: set[str] = set()
        self._instrument_info: dict[str, dict] = {}  # symbol -> {qty_step, min_qty}

    # ------------------------------------------------------------------
    # Публичный интерфейс
    # ------------------------------------------------------------------

    def close_all_positions(self) -> None:
        """Закрывает все открытые позиции на Bybit (вызывать при старте с чистой БД)."""
        for symbol in self.config.bybit.symbols:
            try:
                positions = self.client.get_positions(symbol)
                for p in positions:
                    size = float(p.get("size", 0) or 0)
                    if size > 0:
                        pos_side = p.get("side", "")
                        close_side = "Sell" if pos_side == "Buy" else "Buy"
                        instrument = self._get_instrument(symbol)
                        qty_str = _format_qty(size, instrument["qty_step"])
                        self.client.close_position(symbol, close_side, qty_str)
                        print(f"[Startup] Закрыта позиция {pos_side} {qty_str} {symbol}")
            except Exception as e:
                print(f"[Startup] Ошибка закрытия {symbol}: {e}")

    def run_cycle(self) -> None:
        print("[Cycle] ═══════════════ Начало цикла ═══════════════")
        self.check_positions()
        self.open_new_series()
        self._print_status()
        print("[Cycle] ═══════════════ Цикл завершён ═══════════════")

    def check_positions(self) -> None:
        """Проверяет открытые трейды: закрыт ли TP/SL."""
        open_trades = self.store.get_open_trades()
        if not open_trades:
            print("[Check] Открытых позиций нет.")
            return

        print(f"[Check] Проверяем {len(open_trades)} открытых позиций...")
        for trade in open_trades:
            # Пропускаем только что открытые позиции
            age_sec = (datetime.utcnow() - trade.opened_at).total_seconds()
            if age_sec < OPEN_GRACE_SEC:
                print(f"[Check] {trade.symbol} depth={trade.series_depth}: только что открыта, пропускаем.")
                continue
            self._check_trade(trade)

    def open_new_series(self) -> None:
        """Открывает новые серии в свободные слоты."""
        active = self.store.get_active_series()
        max_s = self.config.martingale.max_active_series
        free_slots = max_s - len(active)
        if free_slots <= 0:
            print(f"[Open] Все слоты заняты ({len(active)}/{max_s}).")
            return

        occupied = self._occupied_symbols()
        available = [s for s in self.config.bybit.symbols if s not in occupied]

        opened = 0
        for symbol in available:
            if opened >= free_slots:
                break
            self._start_series(symbol)
            opened += 1

        if opened == 0 and free_slots > 0:
            print("[Open] Нет свободных символов для новой серии.")

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _occupied_symbols(self) -> set[str]:
        """Символы, на которых сейчас есть открытый трейд."""
        return {t.symbol for t in self.store.get_open_trades()}

    def _start_series(self, symbol: str) -> None:
        margin = self.config.martingale.initial_margin_usdt
        series = Series(symbol=symbol, initial_margin=margin)
        self.store.create_series(series)
        print(f"[Open] Новая серия {series.id[:8]} | Символ: {symbol} | Маржа: ${margin:.4f}")
        self._open_trade(series.id, depth=0, symbol=symbol, margin_usdt=margin)

    def _get_instrument(self, symbol: str) -> dict:
        if symbol not in self._instrument_info:
            try:
                info = self.client.get_instrument_info(symbol)
                self._instrument_info[symbol] = info
                print(f"[Info] {symbol}: qtyStep={info['qty_step']} minQty={info['min_qty']}")
            except Exception as e:
                print(f"[Info] Не удалось получить параметры {symbol}: {e}")
                self._instrument_info[symbol] = {"qty_step": 0.001, "min_qty": 0.001}
        return self._instrument_info[symbol]

    def _ensure_leverage(self, symbol: str) -> None:
        if symbol in self._leverage_set:
            return
        try:
            self.client.set_leverage(symbol, self.config.bybit.leverage)
            self._leverage_set.add(symbol)
        except Exception as e:
            print(f"[Trade] Предупреждение: не удалось установить плечо {symbol}: {e}")

    def _open_trade(self, series_id: str, depth: int, symbol: str, margin_usdt: float) -> None:
        self._ensure_leverage(symbol)

        price = self.client.get_ticker(symbol)
        leverage = self.config.bybit.leverage
        tp_pct = self.config.martingale.take_profit_pct / 100.0
        sl_pct = self.config.martingale.stop_loss_pct / 100.0

        instrument = self._get_instrument(symbol)
        qty_step = instrument["qty_step"]
        min_qty = instrument["min_qty"]

        position_usdt = margin_usdt * leverage
        qty = _floor_to_step(position_usdt / price, qty_step)

        if qty < min_qty:
            print(
                f"[Trade] Объём {qty} < min {min_qty} для {symbol} "
                f"(margin=${margin_usdt:.4f} × {leverage}x = ${position_usdt:.2f}) — серия брошена"
            )
            self.store.finish_series(series_id, "abandoned", 0.0)
            return

        side = random.choice(["Buy", "Sell"])

        qty_str = _format_qty(qty, qty_step)

        try:
            result = self.client.place_order(symbol, side, qty_str)
        except Exception as e:
            print(f"[Trade] Ошибка открытия позиции {symbol} depth={depth}: {e}")
            return

        order_id = (result.get("result") or {}).get("orderId", "")
        if not order_id:
            ret_code = result.get("retCode")
            ret_msg = result.get("retMsg", "")
            print(f"[Trade] Ордер не создан (retCode={ret_code}: {ret_msg}) — пропускаем")
            return

        # Берём реальную цену заполнения из ордера
        time.sleep(0.5)
        avg_price = self._get_fill_price(symbol, order_id)
        if avg_price <= 0:
            # Фолбэк: пробуем позицию
            avg_price, real_side = self._get_position_info(symbol)
            if real_side and real_side != side:
                print(f"[Trade] Реальная сторона {real_side} (вместо {side}) для {symbol}")
                side = real_side
        if avg_price <= 0:
            avg_price = price
            print(f"[Trade] Не удалось получить fill price для {symbol}, используем тикер ${price:.4f}")
        else:
            print(f"[Trade] Fill price {symbol}: ${avg_price:.6f}")

        if side == "Buy":
            take_profit = round(avg_price * (1.0 + tp_pct), 6)
            stop_loss   = round(avg_price * (1.0 - sl_pct), 6)
        else:
            take_profit = round(avg_price * (1.0 - tp_pct), 6)
            stop_loss   = round(avg_price * (1.0 + sl_pct), 6)

        try:
            resp = self.client.set_trading_stop(symbol, take_profit, stop_loss)
            rc = resp.get("retCode")
            if rc != 0:
                print(f"[Trade] TP/SL не установлен для {symbol}: retCode={rc} {resp.get('retMsg')}")
            else:
                print(f"[Trade] TP/SL установлен: {symbol} TP={take_profit} SL={stop_loss}")
        except Exception as e:
            print(f"[Trade] Ошибка set_trading_stop для {symbol}: {e}")

        trade = Trade(
            series_id=series_id,
            series_depth=depth,
            symbol=symbol,
            side=side,
            order_id=order_id,
            margin_usdt=margin_usdt,
            qty=qty,
            entry_price=avg_price,
            take_profit=take_profit,
            stop_loss=stop_loss,
        )
        self.store.save_trade(trade)
        self.store.update_series_depth(series_id, depth, margin_usdt)
        print(
            f"[Trade] {side} {qty_str} {symbol} @ ${avg_price:.4f} | "
            f"Маржа: ${margin_usdt:.4f} | TP: ${take_profit:.6f} | SL: ${stop_loss:.6f} | "
            f"depth={depth} | orderId={order_id}"
        )

    def _get_fill_price(self, symbol: str, order_id: str) -> float:
        """Получает реальную цену заполнения ордера."""
        try:
            order = self.client.get_order(symbol, order_id)
            if order:
                avg = float(order.get("avgPrice", 0) or 0)
                if avg > 0:
                    return avg
        except Exception as e:
            print(f"[Trade] Ошибка получения fill price {symbol}: {e}")
        return 0.0

    def _get_position_info(self, symbol: str) -> tuple[float, str]:
        """Возвращает (avgPrice, side) открытой позиции."""
        try:
            positions = self.client.get_positions(symbol)
            for p in positions:
                size = float(p.get("size", 0) or 0)
                if size > 0:
                    avg_price = float(p.get("avgPrice", 0) or 0)
                    pos_side = p.get("side", "")
                    return avg_price, pos_side
        except Exception as e:
            print(f"[Trade] Ошибка получения позиции {symbol}: {e}")
        return 0.0, ""

    def _check_trade(self, trade: Trade) -> None:
        """Проверяет закрытость позиции и обрабатывает результат."""
        positions = self.client.get_positions(trade.symbol)
        open_size = 0.0
        for p in positions:
            try:
                open_size += abs(float(p.get("size", 0) or 0))
            except (ValueError, TypeError):
                pass

        if open_size > 0:
            print(
                f"[Check] {trade.symbol} depth={trade.series_depth}: "
                f"позиция открыта (size={open_size})"
            )
            return

        # Позиция закрыта — ищем PnL в истории
        pnl, exit_price = self._get_closed_pnl(trade)

        if exit_price == 0.0:
            # Не нашли запись в closed_pnl — позиция, возможно, ещё не появилась в истории
            print(
                f"[Check] {trade.symbol} depth={trade.series_depth}: "
                f"size=0 но closed_pnl не найден — ждём следующего цикла"
            )
            return

        status = "won" if pnl > 0 else "lost"
        self.store.close_trade(trade.id, exit_price, pnl, status)

        arrow = "✓ TP" if status == "won" else "✗ SL"
        print(
            f"[Check] {arrow}: {trade.symbol} depth={trade.series_depth} | "
            f"Выход: ${exit_price:.4f} | P&L: ${pnl:+.4f}"
        )

        if status == "won":
            self._finish_series_won(trade.series_id)
        else:
            self._escalate_or_abandon(trade.series_id, trade.series_depth)

    def _get_closed_pnl(self, trade: Trade) -> tuple[float, float]:
        """Ищет P&L закрытой позиции в истории Bybit."""
        try:
            closed_list = self.client.get_closed_pnl(trade.symbol, limit=10)
        except Exception as e:
            print(f"[Check] Не удалось получить closed_pnl {trade.symbol}: {e}")
            return 0.0, 0.0

        opened_ts_ms = int(trade.opened_at.timestamp() * 1000)
        for c in closed_list:
            try:
                updated_ms = int(c.get("updatedTime", 0))
                if updated_ms >= opened_ts_ms - 5000:  # с допуском 5 сек
                    pnl = float(c.get("closedPnl", 0))
                    exit_price = float(c.get("avgExitPrice", 0))
                    return pnl, exit_price
            except (ValueError, TypeError):
                continue

        return 0.0, 0.0

    def _finish_series_won(self, series_id: str) -> None:
        trades = self.store.get_series_trades(series_id)
        total_pnl = sum(t.pnl for t in trades)
        series = self.store.get_series_by_id(series_id)
        self.store.finish_series(series_id, "won", total_pnl)
        print(
            f"[Series] ✓ Серия ПОБЕДА | "
            f"Глубина: {series.current_depth if series else '?'} | "
            f"Вложено: ${(series.total_invested if series else 0):.4f} | "
            f"P&L: ${total_pnl:+.4f}"
        )

    def _escalate_or_abandon(self, series_id: str, current_depth: int) -> None:
        series = self.store.get_series_by_id(series_id)
        if not series:
            return

        next_depth = current_depth + 1
        max_depth = self.config.martingale.max_series_depth

        if next_depth >= max_depth:
            trades = self.store.get_series_trades(series_id)
            total_pnl = sum(t.pnl for t in trades)
            self.store.finish_series(series_id, "abandoned", total_pnl)
            print(
                f"[Series] ✗ Серия БРОШЕНА (лимит глубины {max_depth}) | "
                f"Вложено: ${series.total_invested:.4f} | P&L: ${total_pnl:+.4f}"
            )
            return

        new_margin = self.config.martingale.initial_margin_usdt * (2 ** next_depth)

        # Выбираем символ — желательно не занятый другими трейдами
        occupied = self._occupied_symbols()  # символы с ОТКРЫТЫМИ трейдами
        # Кроме текущего символа серии — пробуем найти свежий
        candidates = [
            s for s in self.config.bybit.symbols
            if s != series.symbol and s not in occupied
        ]
        if not candidates:
            # Все свободны, берём любой другой символ
            candidates = [s for s in self.config.bybit.symbols if s != series.symbol]
        if not candidates:
            candidates = self.config.bybit.symbols  # фолбэк

        next_symbol = candidates[0]
        print(
            f"[Series] ↑ Эскалация depth={next_depth} | "
            f"{series.symbol} → {next_symbol} | Маржа: ${new_margin:.4f}"
        )
        self._open_trade(series_id, next_depth, next_symbol, new_margin)

    def _print_status(self) -> None:
        active = self.store.get_active_series()
        open_trades = self.store.get_open_trades()
        print(
            f"[Status] Активных серий: {len(active)} | "
            f"Открытых позиций: {len(open_trades)}"
        )
