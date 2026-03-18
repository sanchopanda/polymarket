from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

import httpx


@dataclass
class Market:
    id: str
    question: str
    outcomes: List[str]
    outcome_prices: List[float]   # [0.03, 0.97] — цены исходов
    clob_token_ids: List[str]     # token_id для каждого исхода
    volume_num: float
    liquidity_num: float
    end_date: Optional[datetime]
    active: bool
    closed: bool
    neg_risk: bool                # True для multi-outcome рынков
    category: str = ""
    fee_type: str = ""            # "crypto_fees" для крипто, "" для остальных


def _parse_json_field(raw) -> list:
    """Gamma возвращает некоторые поля как JSON-строку внутри JSON."""
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return []


def _parse_end_date(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    # Нормализуем: "+00" -> "+00:00", "Z" -> "+00:00"
    s = raw.replace("Z", "+00:00")
    if s.endswith("+00"):
        s = s + ":00"
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s).replace(tzinfo=None)
    except ValueError:
        return None


class GammaClient:
    def __init__(self, base_url: str, page_size: int = 100, delay_ms: int = 300) -> None:
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.delay_s = delay_ms / 1000.0
        self._http = httpx.Client(timeout=30.0)

    def fetch_all_active_markets(self) -> List[Market]:
        """Загружает все активные незакрытые рынки постранично."""
        markets: List[Market] = []
        offset = 0

        while True:
            params = {
                "active": "true",
                "closed": "false",
                "limit": self.page_size,
                "offset": offset,
            }
            try:
                resp = self._http.get(f"{self.base_url}/markets", params=params)
                resp.raise_for_status()
                batch = resp.json()
            except httpx.HTTPError as e:
                print(f"[GammaClient] Ошибка запроса: {e}")
                break

            if not batch:
                break

            for raw in batch:
                m = self._parse_market(raw)
                if m:
                    markets.append(m)

            if len(batch) < self.page_size:
                break

            offset += self.page_size
            time.sleep(self.delay_s)

        return markets

    def fetch_closed_markets(
        self,
        limit: int = 500,
        min_volume: float | None = None,
        min_liquidity: float | None = None,
        fee_type: str | None = None,
        closed_after: datetime | None = None,
        end_date_max: datetime | None = None,
        ascending: bool = False,
        order_by: str = "endDate",
    ) -> List[Market]:
        """Загружает закрытые рынки постранично.

        limit — максимальное количество рынков после парсинга (не сырых страниц).
        min_volume / min_liquidity — серверная фильтрация через Gamma API.
        closed_after — клиентская фильтрация: остановка когда closedTime < этой даты
                       (API игнорирует endDate_min, поэтому фильтруем сами).
        end_date_max — клиентская фильтрация по макс. end_date.
        ascending — сортировка asc/desc.
        order_by — поле сортировки (endDate, closedTime, volume24hr и т.д.).
        """
        markets: List[Market] = []
        offset = 0
        stop = False

        while len(markets) < limit and not stop:
            params: dict = {
                "closed": "true",
                "resolved": "true",
                "order": order_by,
                "ascending": "true" if ascending else "false",
                "limit": self.page_size,
                "offset": offset,
            }
            if min_volume is not None:
                params["volumeNum_min"] = min_volume
            if min_liquidity is not None:
                params["liquidityNum_min"] = min_liquidity
            if fee_type is not None:
                params["feeType"] = fee_type

            try:
                resp = self._http.get(f"{self.base_url}/markets", params=params)
                resp.raise_for_status()
                batch = resp.json()
            except httpx.HTTPError as e:
                print(f"[GammaClient] Ошибка запроса закрытых рынков: {e}")
                break

            if not batch:
                break

            for raw in batch:
                # Ранняя остановка по closedTime (desc): если вышли за период — стоп
                if closed_after is not None and not ascending:
                    closed_time = _parse_end_date(raw.get("closedTime"))
                    if closed_time is not None and closed_time < closed_after:
                        stop = True
                        break

                m = self._parse_market(raw)
                if m:
                    # Клиентская фильтрация по end_date_max
                    if end_date_max is not None and m.end_date and m.end_date > end_date_max:
                        continue
                    markets.append(m)
                    if len(markets) >= limit:
                        break

            if len(batch) < self.page_size:
                break

            offset += self.page_size
            last_ct = _parse_end_date(batch[-1].get("closedTime")) if batch else None
            last_ct_str = last_ct.strftime("%Y-%m-%d %H:%M") if last_ct else "?"
            print(f"  [Gamma] {len(markets)} рынков | offset={offset} | последний: {last_ct_str}")
            time.sleep(self.delay_s)

        return markets

    def fetch_market(self, market_id: str) -> Optional[Market]:
        """Получить один рынок по ID (для проверки резолюции)."""
        try:
            resp = self._http.get(f"{self.base_url}/markets/{market_id}")
            resp.raise_for_status()
            return self._parse_market(resp.json())
        except httpx.HTTPError as e:
            print(f"[GammaClient] Ошибка fetch_market {market_id}: {e}")
            return None

    def _parse_market(self, raw: dict) -> Optional[Market]:
        try:
            outcomes = _parse_json_field(raw.get("outcomes"))
            outcome_prices_raw = _parse_json_field(raw.get("outcomePrices"))
            clob_token_ids = _parse_json_field(raw.get("clobTokenIds"))

            if not outcomes or not outcome_prices_raw or not clob_token_ids:
                return None

            outcome_prices = []
            for p in outcome_prices_raw:
                try:
                    outcome_prices.append(float(p))
                except (ValueError, TypeError):
                    outcome_prices.append(0.0)

            # Выравниваем длины списков
            min_len = min(len(outcomes), len(outcome_prices), len(clob_token_ids))
            if min_len == 0:
                return None

            return Market(
                id=str(raw.get("id", "")),
                question=raw.get("question", ""),
                outcomes=outcomes[:min_len],
                outcome_prices=outcome_prices[:min_len],
                clob_token_ids=clob_token_ids[:min_len],
                volume_num=float(raw.get("volumeNum", 0) or 0),
                liquidity_num=float(raw.get("liquidityNum", 0) or 0),
                end_date=_parse_end_date(raw.get("endDate")),
                active=bool(raw.get("active", False)),
                closed=bool(raw.get("closed", False)),
                neg_risk=bool(raw.get("negRisk", False)),
                category=str(raw.get("category", "") or ""),
                fee_type=str(raw.get("feeType", "") or ""),
            )
        except Exception as e:
            print(f"[GammaClient] Ошибка парсинга рынка: {e}")
            return None

    def close(self) -> None:
        self._http.close()
