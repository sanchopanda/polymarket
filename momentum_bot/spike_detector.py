from __future__ import annotations

import time
from collections import deque

from momentum_bot.models import PriceTick


class SpikeDetector:
    """Rolling window price tracker and upward spike detector.

    Key: (venue, identifier, side)
      venue      = "polymarket" | "kalshi"
      identifier = asset_id (Polymarket token) | ticker (Kalshi)
      side       = "yes" | "no"
    """

    def __init__(self, window_seconds: float, threshold_cents: float) -> None:
        self.window_seconds = window_seconds
        self.threshold_cents = threshold_cents
        self._windows: dict[tuple[str, str, str], deque[PriceTick]] = {}

    def record(self, venue: str, identifier: str, side: str, price: float, timestamp: float | None = None) -> None:
        if timestamp is None:
            timestamp = time.time()
        key = (venue, identifier, side)
        if key not in self._windows:
            self._windows[key] = deque()
        window = self._windows[key]
        window.append(PriceTick(timestamp=timestamp, price=price))
        cutoff = timestamp - self.window_seconds
        while window and window[0].timestamp < cutoff:
            window.popleft()

    def detect_spike(self, venue: str, identifier: str, side: str) -> float | None:
        """Return spike magnitude in cents if upward spike detected, else None.

        Spike conditions:
          - At least 2 data points
          - Current price > previous (trending up)
          - current - min(window) >= threshold_cents
        """
        key = (venue, identifier, side)
        window = self._windows.get(key)
        if not window or len(window) < 2:
            return None

        current = window[-1].price
        previous = window[-2].price
        if current <= previous:
            return None

        min_price = min(tick.price for tick in window)
        magnitude_cents = (current - min_price) * 100
        if magnitude_cents >= self.threshold_cents:
            return round(magnitude_cents, 2)
        return None

    def baseline_price(self, venue: str, identifier: str, side: str, lookback_seconds: float = 10.0) -> float | None:
        """Return the oldest price within the last lookback_seconds window, or None."""
        key = (venue, identifier, side)
        window = self._windows.get(key)
        if not window or len(window) < 2:
            return None
        cutoff = window[-1].timestamp - lookback_seconds
        return next((tick.price for tick in window if tick.timestamp >= cutoff), None)

    def is_rising(self, venue: str, identifier: str, side: str, lookback_seconds: float = 10.0) -> bool:
        """True if current price is higher than the earliest price in the last lookback_seconds.

        More robust than comparing last two ticks — ignores noise from frequent ticks.
        Returns False if there's only one data point in the window.
        """
        key = (venue, identifier, side)
        window = self._windows.get(key)
        if not window or len(window) < 2:
            return False
        current = window[-1].price
        baseline = self.baseline_price(venue, identifier, side, lookback_seconds)
        if baseline is None:
            return False
        return current >= baseline

    def current_price(self, venue: str, identifier: str, side: str) -> float | None:
        key = (venue, identifier, side)
        window = self._windows.get(key)
        if not window:
            return None
        return window[-1].price

    def clear_market(self, identifier: str) -> None:
        """Remove all windows for a given market identifier."""
        to_remove = [k for k in self._windows if k[1] == identifier]
        for k in to_remove:
            del self._windows[k]
