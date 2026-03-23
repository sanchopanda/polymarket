from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import httpx


@dataclass
class OrderLevel:
    price: float
    size: float   # в USDC


@dataclass
class OrderBook:
    bids: List[OrderLevel]
    asks: List[OrderLevel]


@dataclass
class LiquidityCheck:
    available: bool          # Можно ли купить на нужную сумму
    avg_fill_price: float    # Средняя цена с учётом стакана
    available_usd: float     # Сколько реально можно купить ($)


class ClobClient:
    def __init__(self, base_url: str, delay_ms: int = 300) -> None:
        self.base_url = base_url.rstrip("/")
        self.delay_s = delay_ms / 1000.0
        self._http = httpx.Client(timeout=15.0)
        self._dead_tokens: set[str] = set()  # 404 токены — не повторяем

    def get_orderbook(self, token_id: str) -> Optional[OrderBook]:
        """Получить orderbook для конкретного токена (исхода)."""
        if token_id in self._dead_tokens:
            return None
        try:
            resp = self._http.get(f"{self.base_url}/book", params={"token_id": token_id})
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self._dead_tokens.add(token_id)
                print(f"[ClobClient] Token не найден (404), пропускаю: {token_id[:20]}...")
            else:
                print(f"[ClobClient] Ошибка get_orderbook {token_id[:20]}...: {e}")
            return None
        except httpx.HTTPError as e:
            print(f"[ClobClient] Ошибка get_orderbook {token_id[:20]}...: {e}")
            return None

        bids = [OrderLevel(float(b["price"]), float(b["size"])) for b in data.get("bids", [])]
        asks = [OrderLevel(float(a["price"]), float(a["size"])) for a in data.get("asks", [])]
        # bids убывают по цене, asks возрастают
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)
        return OrderBook(bids=bids, asks=asks)

    def get_midpoint(self, token_id: str) -> Optional[float]:
        """Получить mid-price токена."""
        try:
            resp = self._http.get(f"{self.base_url}/midpoint", params={"token_id": token_id})
            resp.raise_for_status()
            return float(resp.json().get("mid", 0))
        except (httpx.HTTPError, ValueError, KeyError):
            return None

    def check_liquidity(self, token_id: str, amount_usd: float) -> LiquidityCheck:
        """
        Проверяет, можно ли купить контракты на указанную сумму.
        Проходит по ask-стороне стакана и считает среднюю цену входа.
        """
        book = self.get_orderbook(token_id)
        if not book or not book.asks:
            return LiquidityCheck(available=False, avg_fill_price=0.0, available_usd=0.0)

        remaining_usd = amount_usd
        total_cost = 0.0
        total_shares = 0.0

        for level in book.asks:
            level_cost = level.price * level.size  # стоимость всего уровня в $
            if level_cost <= remaining_usd:
                total_cost += level_cost
                total_shares += level.size
                remaining_usd -= level_cost
            else:
                shares_can_buy = remaining_usd / level.price
                total_cost += remaining_usd
                total_shares += shares_can_buy
                remaining_usd = 0.0
                break

        filled_usd = amount_usd - remaining_usd
        avg_price = (total_cost / total_shares) if total_shares > 0 else 0.0

        # Считаем ставку доступной если можно купить хотя бы 80% нужной суммы
        available = filled_usd >= amount_usd * 0.8

        return LiquidityCheck(
            available=available,
            avg_fill_price=avg_price,
            available_usd=filled_usd,
        )

    def get_price_history(self, token_id: str, fidelity: int = 10) -> list[tuple[int, float]]:
        """Получить историю цен токена.

        Returns список (timestamp_sec, price), отсортированный по времени.
        """
        try:
            resp = self._http.get(
                f"{self.base_url}/prices-history",
                params={"market": token_id, "interval": "max", "fidelity": fidelity},
            )
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError as e:
            print(f"[ClobClient] Ошибка get_price_history {token_id}: {e}")
            return []

        history = data.get("history", [])
        result: list[tuple[int, float]] = []
        for point in history:
            try:
                ts = int(point["t"])
                price = float(point["p"])
                result.append((ts, price))
            except (KeyError, ValueError, TypeError):
                continue
        result.sort(key=lambda x: x[0])
        return result

    def close(self) -> None:
        self._http.close()
