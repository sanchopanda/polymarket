from __future__ import annotations

import time

from recovery_bot.engine import RecoveryEngine


class RecoveryWatchRunner:
    def __init__(self, engine: RecoveryEngine) -> None:
        self.engine = engine

    def run(self) -> None:
        refresh_seconds = int(self.engine.runtime["universe_refresh_seconds"])
        status_seconds = int(self.engine.runtime["status_interval_seconds"])
        last_refresh = 0.0
        last_status = 0.0
        try:
            while True:
                now = time.time()
                if now - last_refresh >= refresh_seconds:
                    new_markets = self.engine.scan_markets()
                    if new_markets:
                        print(f"[recovery] scanned {len(new_markets)} new markets")
                    last_refresh = now
                if now - last_status >= status_seconds:
                    self.engine.resolve()
                    self.engine.print_status()
                    last_status = now
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[recovery] stopped")
        finally:
            self.engine.stop()

