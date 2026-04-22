from __future__ import annotations

import os
import threading
from typing import Callable, Optional

import telebot
from telebot.types import InlineKeyboardButton, InlineKeyboardMarkup


def _kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Jump Status", callback_data="jump_status"))
    return kb


class JumpTelegramNotifier:
    def __init__(
        self,
        get_status_fn: Callable[[], str],
        token_env: str = "SIMPLE_BOT_TOKEN",
        chat_id_file: str = "data/.telegram_chat_id",
    ) -> None:
        self._get_status = get_status_fn
        self._chat_id_file = chat_id_file
        token = os.environ.get(token_env, "")
        if not token:
            print(f"[jump_tg] {token_env} not set — Telegram disabled")
            self._bot: Optional[telebot.TeleBot] = None
            self._chat_id: Optional[int] = None
            return
        self._chat_id = self._load_chat_id()
        self._bot = telebot.TeleBot(token, parse_mode="HTML")
        self._setup_handlers()

    def _load_chat_id(self) -> Optional[int]:
        try:
            with open(self._chat_id_file) as fh:
                return int(fh.read().strip())
        except Exception:
            return None

    def _save_chat_id(self, chat_id: int) -> None:
        try:
            with open(self._chat_id_file, "w") as fh:
                fh.write(str(chat_id))
        except Exception:
            pass

    def start(self) -> None:
        if not self._bot:
            return
        threading.Thread(
            target=self._bot.infinity_polling,
            kwargs={"timeout": 10, "long_polling_timeout": 5, "logger_level": None},
            daemon=True,
            name="tg-jump-paper",
        ).start()
        if self._chat_id:
            self.send("🤖 <b>jump_paper_bot запущен</b>\n/jump_status — текущая статистика")
        else:
            print("[jump_tg] chat_id not found — send /start to bot")

    def _setup_handlers(self) -> None:
        bot = self._bot

        @bot.message_handler(commands=["start"])
        def handle_start(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            self.send("🤖 <b>jump_paper_bot подключён</b>\n/jump_status — текущая статистика")

        @bot.message_handler(commands=["jump_status"])
        def handle_status(msg):
            self._chat_id = msg.chat.id
            self._save_chat_id(self._chat_id)
            self._send_status()

        @bot.callback_query_handler(func=lambda c: c.data == "jump_status")
        def handle_cb(call):
            bot.answer_callback_query(call.id, "Обновляю...")
            self._send_status()

    def _send_status(self) -> None:
        try:
            self.send(self._get_status())
        except Exception as exc:
            print(f"[jump_tg] status failed: {exc}")

    def send(self, text: str, reply_to_message_id: int | None = None) -> int | None:
        if not self._bot or not self._chat_id:
            return None
        try:
            msg = self._bot.send_message(
                self._chat_id,
                text,
                reply_markup=_kb(),
                reply_to_message_id=reply_to_message_id,
            )
            return getattr(msg, "message_id", None)
        except Exception as exc:
            print(f"[jump_tg] send failed: {exc}")
            return None

    def notify_open(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        side: str,
        signal_bucket_seconds: int,
        signal_level: float,
        signal_price: float,
        avg_prev_10s: float,
        fill_avg: float,
        stake_usd: float,
        shares: float,
        depth_usd: float,
        market_url: str | None = None,
    ) -> int | None:
        link = f'\n<a href="{market_url}">market</a>' if market_url else ""
        return self.send(
            f"📝 <b>OPEN {symbol} {interval_minutes}m {side.upper()}</b>\n"
            f"bucket={signal_bucket_seconds}s | level={signal_level:.2f}\n"
            f"signal={signal_price:.3f} | avg10s={avg_prev_10s:.3f}\n"
            f"fill={fill_avg:.3f} | stake=${stake_usd:.2f}\n"
            f"shares={shares:.2f} | depth=${depth_usd:.2f}{link}"
        )

    def notify_resolve(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        side: str,
        winning_side: str,
        pnl: float,
        reply_to_message_id: int | None = None,
    ) -> None:
        emoji = "💰" if pnl >= 0 else "❌"
        self.send(
            f"{emoji} <b>RESOLVE {symbol} {interval_minutes}m {side.upper()}</b>\n"
            f"winner={winning_side}\n"
            f"PnL: <b>${pnl:+.2f}</b>",
            reply_to_message_id=reply_to_message_id,
        )

