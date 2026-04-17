from __future__ import annotations

import time
from datetime import datetime, timezone

from recovery_bot.engine import RecoveryEngine

# Новые рынки появляются строго на границах 5 минут.
# Сканируем через SCAN_DELAY_SECONDS секунд после каждой такой границы.
SCAN_DELAY_SECONDS = 8
SCAN_GRID_MINUTES  = 5


def _next_scan_unix() -> float:
    """Возвращает unix-время следующего сканирования.

    Сканируем через SCAN_DELAY_SECONDS после ближайшей будущей 5-минутной
    границы (xx:00, xx:05, xx:10 ...).
    """
    now_dt   = datetime.now(timezone.utc)
    now_unix = now_dt.timestamp()

    total_seconds = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
    grid_seconds  = SCAN_GRID_MINUTES * 60

    # Сколько секунд до следующей границы
    secs_into_grid = total_seconds % grid_seconds
    secs_to_next   = grid_seconds - secs_into_grid

    return now_unix + secs_to_next + SCAN_DELAY_SECONDS


class RecoveryWatchRunner:
    def __init__(self, engine: RecoveryEngine) -> None:
        self.engine = engine

    def run(self) -> None:
        status_seconds = int(self.engine.runtime["status_interval_seconds"])
        last_status    = 0.0

        # Первое сканирование — сразу при старте
        self._do_scan()
        next_scan = _next_scan_unix()

        try:
            while True:
                now = time.time()

                if now >= next_scan:
                    self._do_scan()
                    next_scan = _next_scan_unix()

                if now - last_status >= status_seconds:
                    self.engine.resolve()
                    last_status = now

                time.sleep(1)

        except KeyboardInterrupt:
            print("\n[recovery] stopped")
        finally:
            self.engine.stop()

    def _do_scan(self) -> None:
        new_markets = self.engine.scan_markets()
        if new_markets:
            print(f"[recovery] scanned {len(new_markets)} new markets")
