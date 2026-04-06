from __future__ import annotations

import os
import threading
from typing import Callable, Optional

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup

CHAT_ID_FILE = "data/.telegram_chat_id_sports_arb"
# Fallback — общий кеш, который пишут другие боты
CHAT_ID_FALLBACKS = [
    "data/.telegram_chat_id_real_momentum",
    "data/.telegram_chat_id",
]


def _status_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📊 Статус", callback_data="status"))
    return kb


class SportsTelegramNotifier:
    """
    Telegram-бот для sports_arb_bot.
    Опционален: если токен не задан — молча отключается.
    Поддерживает /start и /status команды.
    """

    def __init__(self, get_status_fn: Callable[[], str], token_env: str = "TELEGRAM_TOKEN") -> None:
        token = os.environ.get(token_env, "")
        if not token:
            raise RuntimeError(f"{token_env} не задан")

        self._get_status = get_status_fn
        self.bot = telebot.TeleBot(token, parse_mode="HTML")
        self.chat_id: Optional[int] = self._load_chat_id()
        self._setup_handlers()

    def _load_chat_id(self) -> Optional[int]:
        for path in [CHAT_ID_FILE] + CHAT_ID_FALLBACKS:
            try:
                with open(path) as f:
                    return int(f.read().strip())
            except (FileNotFoundError, ValueError):
                continue
        return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            with open(CHAT_ID_FILE, "w") as f:
                f.write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        t = threading.Thread(target=self._poll, daemon=True, name="sports-tg")
        t.start()
        if self.chat_id:
            print(f"[Telegram] chat_id={self.chat_id}")
            self._send("🎾 <b>sports_arb_bot запущен</b>")
        else:
            print("[Telegram] chat_id не найден, жду /start...")

    def _poll(self) -> None:
        self.bot.infinity_polling(timeout=10, long_polling_timeout=5, logger_level=None)

    def _setup_handlers(self) -> None:
        bot = self.bot

        @bot.message_handler(commands=["start"])
        def on_start(msg):
            self.chat_id = msg.chat.id
            self._save_chat_id(self.chat_id)
            print(f"[Telegram] /start от chat_id={self.chat_id}")
            self._send("🎾 <b>sports_arb_bot подключён</b>\nПришлю уведомления о матчах и ставках.")

        @bot.message_handler(commands=["status"])
        def on_status(msg):
            self.chat_id = msg.chat.id
            try:
                self.bot.send_message(msg.chat.id, self._get_status(), reply_markup=_status_kb())
            except Exception as e:
                print(f"[Telegram] Ошибка /status: {e}")

        @bot.callback_query_handler(func=lambda c: c.data == "status")
        def on_callback(call):
            bot.answer_callback_query(call.id, text="Обновляю...")
            try:
                self.bot.send_message(call.message.chat.id, self._get_status(), reply_markup=_status_kb())
            except Exception as e:
                print(f"[Telegram] Ошибка callback status: {e}")

    # ── Уведомления ─────────────────────────────────────────────────────

    def notify_scan(self, new_pairs: list[str], total_pairs: int) -> None:
        if not new_pairs:
            return
        lines = "\n".join(f"  • {p}" for p in new_pairs)
        text = (
            f"🔍 <b>Новые пары ({len(new_pairs)})</b>\n"
            f"{lines}\n"
            f"Всего отслеживается: {total_pairs}"
        )
        self._send(text)

    def notify_bet(
        self,
        pos_id: str,
        pm_slug: str,
        leg_pm_player: str,
        leg_pm_price: float,
        leg_ka_player: str,
        leg_ka_ticker: str,
        leg_ka_price: float,
        cost: float,
        edge: float,
        shares: int,
        total_cost: float,
        expected_profit: float,
        pm_depth: Optional[float],
        ka_depth: Optional[float],
        lock_valid: bool,
    ) -> None:
        depth_str = ""
        if pm_depth is not None and ka_depth is not None:
            depth_str = f"\n📦 Depth: PM ${pm_depth:.0f} | Kalshi ${ka_depth:.0f}"
        lock_str = "✅ lock valid" if lock_valid else "⚠️ depth insufficient"
        pm_url = f"https://polymarket.com/event/{pm_slug}"
        ka_event_ticker = leg_ka_ticker.rsplit("-", 1)[0]
        ka_url = f"https://kalshi.com/markets/{ka_event_ticker}"
        text = (
            f"📝 <b>PAPER BET {pos_id}</b>\n"
            f"PM: {leg_pm_player} @ {leg_pm_price:.3f}\n"
            f"Kalshi: {leg_ka_player} ({leg_ka_ticker}) @ {leg_ka_price:.3f}\n"
            f"edge={edge:.4f} cost={cost:.4f} shares={shares}\n"
            f"stake=${total_cost:.2f} → прибыль≈${expected_profit:.2f}"
            f"{depth_str}\n{lock_str}\n"
            f'<a href="{pm_url}">PM</a> | <a href="{ka_url}">Kalshi</a>'
        )
        self._send(text)

    def notify_real_bet(
        self,
        pos_id: str,
        pm_slug: str,
        leg_pm_player: str,
        leg_ka_player: str,
        leg_ka_ticker: str,
        execution_status: str,
        ka_fill_price: float,
        ka_fill_shares: float,
        pm_fill_price: float,
        pm_fill_shares: float,
        edge: float,
        total_cost: float,
        pm_depth: Optional[float] = None,
        ka_depth: Optional[float] = None,
        pm_balance: Optional[float] = None,
        ka_balance: Optional[float] = None,
    ) -> None:
        status_icons = {
            "both_filled": "✅",
            "one_legged_kalshi": "⚡",
            "one_legged_polymarket": "⚡",
        }
        icon = status_icons.get(execution_status, "⚠️")
        status_label = {
            "both_filled": "ОБЕ НОГИ",
            "one_legged_kalshi": "ТОЛЬКО KALSHI (ждём PM)",
            "one_legged_polymarket": "ТОЛЬКО PM (ждём Kalshi)",
        }.get(execution_status, execution_status.upper())

        pm_url = f"https://polymarket.com/event/{pm_slug}"
        ka_event_ticker = leg_ka_ticker.rsplit("-", 1)[0]
        ka_url = f"https://kalshi.com/markets/{ka_event_ticker}"

        balance_str = ""
        if pm_balance is not None and ka_balance is not None:
            balance_str = f"\n💼 PM: ${pm_balance:.2f} | Kalshi: ${ka_balance:.2f}"
        depth_str = ""
        if pm_depth is not None and ka_depth is not None:
            depth_str = f"\n📦 Depth: PM ${pm_depth:.0f} | Kalshi ${ka_depth:.0f}"

        text = (
            f"{icon} <b>REAL BET {pos_id}</b> — {status_label}\n"
            f"PM: {leg_pm_player} @ {pm_fill_price:.3f} × {pm_fill_shares:.1f}\n"
            f"Kalshi: {leg_ka_player} ({leg_ka_ticker}) @ {ka_fill_price:.3f} × {ka_fill_shares:.1f}\n"
            f"edge={edge:.4f} stake≈${total_cost:.2f}"
            f"{depth_str}"
            f"{balance_str}\n"
            f'<a href="{pm_url}">PM</a> | <a href="{ka_url}">Kalshi</a>'
        )
        self._send(text)

    def notify_real_filled(self, pos_id: str, leg: str, fill_price: float, fill_shares: float) -> None:
        """Уведомление о заполнении зависшей ноги."""
        label = "PM" if leg == "pm" else "Kalshi"
        self._send(
            f"✅ <b>REAL {pos_id}</b> — {label} нога заполнена\n"
            f"{fill_shares:.1f} @ {fill_price:.4f} → обе ноги закрыты"
        )

    def notify_resolve(
        self,
        pos_id: str,
        pm_slug: str,
        winner: Optional[str],
        pnl: float,
        one_legged: str | None = None,
    ) -> None:
        icon = "💰" if pnl > 0 else ("⚠️" if pnl == 0 else "📉")
        sign = "+" if pnl >= 0 else ""
        leg_tag = f" [ОДНОНОГАЯ — только {one_legged}]" if one_legged else ""
        text = (
            f"{icon} <b>RESOLVED{leg_tag} {pos_id}</b>\n"
            f"{pm_slug}\n"
            f"Победитель: {winner or '?'}\n"
            f"P&amp;L: <b>{sign}${pnl:.2f}</b>"
        )
        self._send(text)

    def _send(self, text: str) -> None:
        if self.chat_id is None:
            return
        try:
            self.bot.send_message(self.chat_id, text, reply_markup=_status_kb())
        except Exception as e:
            print(f"[Telegram] Ошибка отправки: {e}")
