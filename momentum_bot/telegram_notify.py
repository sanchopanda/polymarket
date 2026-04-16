from __future__ import annotations

import os
import threading
from typing import Callable, Optional

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


def _kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Статус", callback_data="momentum_status"))
    return kb


class MomentumTelegramNotifier:
    """Telegram-уведомления для momentum_bot."""

    def __init__(
        self,
        get_status_fn: Callable[[], str],
        token_env: str = "FAST_ARB_BOT_TOKEN",
        chat_id_file: str = "data/.telegram_chat_id",
    ) -> None:
        self._get_status = get_status_fn
        self._chat_id_file = chat_id_file
        token = os.environ.get(token_env, "")
        if not token:
            print(f"[momentum_tg] {token_env} не задан — Telegram отключён")
            self._bot: Optional[telebot.TeleBot] = None
            self._chat_id: Optional[int] = None
            return

        self._chat_id = self._load_chat_id()
        self._bot = telebot.TeleBot(token, parse_mode="HTML")
        self._setup_handlers()

    def _load_chat_id(self) -> Optional[int]:
        try:
            with open(self._chat_id_file) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            with open(self._chat_id_file, "w") as f:
                f.write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        if not self._bot:
            return
        threading.Thread(
            target=self._bot.infinity_polling,
            kwargs={"timeout": 10, "long_polling_timeout": 5, "logger_level": None},
            daemon=True,
            name="tg-momentum",
        ).start()
        if self._chat_id:
            print(f"[momentum_tg] chat_id={self._chat_id}")
            self.send("🤖 <b>momentum_bot запущен</b>")
        else:
            print("[momentum_tg] chat_id не найден — отправь /start боту")

    def _setup_handlers(self) -> None:
        bot = self._bot

        @bot.message_handler(commands=["start"])
        def handle_start(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            print(f"[momentum_tg] подключён chat_id={self._chat_id}")
            self.send("🤖 <b>momentum_bot подключён</b>\n/momentum_status — статистика")

        @bot.message_handler(commands=["momentum_status"])
        def handle_status(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            self._send_status()

        @bot.callback_query_handler(func=lambda c: c.data == "momentum_status")
        def handle_cb(call):
            bot.answer_callback_query(call.id, "Обновляю...")
            self._send_status()

    def _send_status(self) -> None:
        try:
            self.send(self._get_status())
        except Exception as exc:
            print(f"[momentum_tg] ошибка статуса: {exc}")

    def send(self, text: str) -> None:
        if not self._bot or not self._chat_id:
            return
        try:
            self._bot.send_message(self._chat_id, text, reply_markup=_kb())
        except Exception as exc:
            print(f"[momentum_tg] send failed: {exc}")

    def notify_open(
        self,
        symbol: str,
        side: str,
        signal_type: str,
        leader_venue: str,
        follower_venue: str,
        leader_price: float,
        leader_baseline_price: float | None,
        follower_price: float,
        gap_cents: float,
        spike_cents: float,
        total_cost: float,
    ) -> None:
        detail = (
            f"spike={spike_cents:.1f}¢"
            + (
                f" ({leader_baseline_price:.3f} -> {leader_price:.3f})"
                if leader_baseline_price is not None
                else ""
            )
            if signal_type == "spike"
            else f"gap={gap_cents:.1f}¢"
        )
        self.send(
            f"📥 <b>OPEN: {symbol} {side.upper()}</b>\n"
            f"leader={leader_venue} {leader_price:.3f}\n"
            f"follower={follower_venue} {follower_price:.3f}\n"
            f"{detail}\n"
            f"cost=${total_cost:.2f}"
        )

    def notify_resolve(self, symbol: str, side: str, venue: str, outcome: str, pnl: float) -> None:
        emoji = "💰" if pnl >= 0 else "❌"
        self.send(
            f"{emoji} <b>RESOLVE: {symbol} {side.upper()}</b>\n"
            f"venue={venue} | outcome={outcome}\n"
            f"PnL: <b>${pnl:+.2f}</b>"
        )
