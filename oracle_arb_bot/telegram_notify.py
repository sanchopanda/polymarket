from __future__ import annotations

import os
import threading
from typing import Callable, Optional

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


def _kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Статус", callback_data="oracle_status"))
    return kb


class OracleTelegramNotifier:
    """
    Telegram-уведомления для oracle_arb_bot.
    Читает SIMPLE_BOT_TOKEN из env и data/.telegram_chat_id.
    Запускает собственный polling поток — не запускать вместе с volatility_bot.
    """

    CHAT_ID_FILE = "data/.telegram_chat_id"

    def __init__(self, get_status_fn: Callable[[], str]) -> None:
        self._get_status = get_status_fn
        token = os.environ.get("SIMPLE_BOT_TOKEN", "")
        if not token:
            print("[oracle_tg] SIMPLE_BOT_TOKEN not set — Telegram disabled")
            self._bot: Optional[telebot.TeleBot] = None
            self._chat_id: Optional[int] = None
            return

        self._chat_id = self._load_chat_id()
        self._bot = telebot.TeleBot(token, parse_mode="HTML")
        self._setup_handlers()

    def _load_chat_id(self) -> Optional[int]:
        try:
            return int(open(self.CHAT_ID_FILE).read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            open(self.CHAT_ID_FILE, "w").write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        if not self._bot:
            return
        threading.Thread(
            target=self._bot.infinity_polling,
            kwargs={"timeout": 10, "long_polling_timeout": 5, "logger_level": None},
            daemon=True,
            name="tg-oracle",
        ).start()
        if self._chat_id:
            print(f"[oracle_tg] chat_id={self._chat_id}")
            self.send("🤖 <b>oracle_arb_bot запущен</b>")
        else:
            print("[oracle_tg] chat_id не найден — отправь /start боту")

    def _setup_handlers(self) -> None:
        bot = self._bot

        @bot.message_handler(commands=["start"])
        def handle_start(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            print(f"[oracle_tg] подключён chat_id={self._chat_id}")
            self.send("🤖 <b>oracle_arb_bot подключён</b>\n/status — текущая статистика")

        @bot.message_handler(commands=["status"])
        def handle_status(msg):
            self._chat_id = msg.chat.id
            self._send_status()

        @bot.callback_query_handler(func=lambda c: c.data == "oracle_status")
        def handle_cb(call):
            bot.answer_callback_query(call.id, "Обновляю...")
            self._send_status()

    def _send_status(self) -> None:
        try:
            self.send(self._get_status())
        except Exception as e:
            print(f"[oracle_tg] ошибка статуса: {e}")

    def send(self, text: str) -> None:
        if not self._bot or not self._chat_id:
            return
        try:
            self._bot.send_message(self._chat_id, text, reply_markup=_kb())
        except Exception as exc:
            print(f"[oracle_tg] send failed: {exc}")

    def send_bet(
        self,
        symbol: str,
        side: str,
        price: float,
        delta_pct: float,
        stake: float,
        label: str = "paper",
        market_slug: str | None = None,
        venue: str | None = None,
        market_id: str | None = None,
    ) -> None:
        direction = "YES" if side == "yes" else "NO"
        sign = "+" if delta_pct >= 0 else ""
        tag = "📝 paper" if label == "paper" else "💵 real"
        if market_slug:
            link = f'\n<a href="https://polymarket.com/event/{market_slug}">market</a>'
        elif venue == "kalshi" and market_id:
            link = f'\n<a href="https://kalshi.com/markets/{market_id}">kalshi</a>'
        else:
            link = ""
        v = "PM" if venue != "kalshi" else "Kalshi"
        text = (
            f"[OracleArb] {tag} [{v}] <b>{symbol} {direction}</b> @ {price:.3f} "
            f"| Δ{sign}{delta_pct:.3f}% | ${stake:.2f}{link}"
        )
        self.send(text)

    def send_signal(
        self,
        symbol: str,
        side: str,
        delta_pct: float,
        pm_price: float,
        interval_minutes: int,
    ) -> None:
        direction = "YES" if side == "yes" else "NO"
        sign = "+" if delta_pct >= 0 else ""
        text = (
            f"[OracleArb] 📊 PRICED_IN {symbol} {interval_minutes}m {direction} "
            f"| Δ{sign}{delta_pct:.3f}% | pm_{side}={pm_price:.3f}"
        )
        self.send(text)

    def send_bet_failed(self, symbol: str, side: str, reason: str) -> None:
        direction = "YES" if side == "yes" else "NO"
        text = f"[OracleArb] ⚠️ real <b>{symbol} {direction}</b> не исполнено: {reason}"
        self.send(text)

    def send_resolve(self, symbol: str, side: str, won: bool, pnl: float, label: str = "paper", venue: str | None = None) -> None:
        tag = "WIN" if won else "LOSS"
        emoji = "✅" if won else "❌"
        mode = "📝 paper" if label == "paper" else "💵 real"
        v = "PM" if venue != "kalshi" else "Kalshi"
        text = (
            f"[OracleArb] {emoji} {mode} [{v}] <b>{tag}</b> | {symbol} {side.upper()} "
            f"| pnl <b>${pnl:+.2f}</b>"
        )
        self.send(text)
