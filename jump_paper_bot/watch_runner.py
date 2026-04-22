from __future__ import annotations

import time

from jump_paper_bot.engine import JumpPaperEngine


class JumpPaperWatchRunner:
    def __init__(self, engine: JumpPaperEngine) -> None:
        self.engine = engine

    def run(self) -> None:
        refresh_seconds = int(self.engine.runtime["universe_refresh_seconds"])
        status_seconds = int(self.engine.runtime["status_interval_seconds"])
        last_scan = 0.0
        last_status = 0.0

        try:
            while True:
                now = time.time()
                if now - last_scan >= refresh_seconds:
                    new_markets = self.engine.scan_markets()
                    if new_markets:
                        print(f"[jump] scanned {len(new_markets)} new markets")
                    last_scan = now
                if now - last_status >= status_seconds:
                    self.engine.resolve()
                    print(self.engine.get_status_text())
                    last_status = now
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n[jump] stopped")
        finally:
            self.engine.stop()

