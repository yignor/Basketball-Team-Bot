#!/usr/bin/env python3
"""
Постоянно работающий демон бота.
Обрабатывает голоса в опросах в реальном времени (вместо hourly GitHub Actions).
Запускается как systemd-сервис и работает непрерывно.
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime

from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, PollAnswerHandler, ContextTypes

load_dotenv()

BOT_TOKEN         = os.getenv("BOT_TOKEN", "")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")
SPREADSHEET_ID    = os.getenv("SPREADSHEET_ID", "")
ADMIN_USER_ID     = os.getenv("ADMIN_USER_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/var/log/basketball-bot/daemon.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# httpx/httpcore логируют полный URL запроса на уровне INFO, а URL Telegram API
# содержит BOT_TOKEN — поднимаем порог, чтобы токен не попадал в логи/журнал.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


# Импортируем логику из collect_votes (переиспользуем без изменений)
from collect_votes import (
    _init_sheets,
    load_training_polls,
    classify_vote,
    upsert_vote,
    _get_service_ws,
    _get_or_create_sheet,
    _ensure_attend_header,
    ATTEND_SHEET,
    ATTEND_HEADER,
)
import admin_panel

# Кэш зарегистрированных опросов (обновляем раз в 5 минут)
_poll_cache: dict = {}
_poll_cache_time: float = 0.0
_spreadsheet = None
_attend_ws   = None
_service_ws  = None


def _get_spreadsheet():
    global _spreadsheet
    if _spreadsheet is None:
        _spreadsheet = _init_sheets()
    return _spreadsheet


def _get_worksheets():
    global _attend_ws, _service_ws
    sp = _get_spreadsheet()
    if _service_ws is None:
        _service_ws = _get_service_ws(sp)
    if _attend_ws is None:
        _attend_ws = _get_or_create_sheet(sp, ATTEND_SHEET, rows=2000, cols=len(ATTEND_HEADER))
        _ensure_attend_header(_attend_ws)
    return _service_ws, _attend_ws


def _refresh_poll_cache() -> None:
    global _poll_cache, _poll_cache_time
    import time
    now = time.time()
    if now - _poll_cache_time < 300:  # 5 минут
        return
    try:
        svc_ws, _ = _get_worksheets()
        _poll_cache = load_training_polls(svc_ws)
        _poll_cache_time = now
        log.info(f"Кэш опросов обновлён: {len(_poll_cache)} тренировочных опросов")
    except Exception as e:
        log.warning(f"Не удалось обновить кэш опросов: {e}")


async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    poll_answer = update.poll_answer
    if not poll_answer:
        return

    _refresh_poll_cache()

    tg_poll_id = str(poll_answer.poll_id)
    if tg_poll_id not in _poll_cache:
        return  # не тренировочный опрос

    poll_info      = _poll_cache[tg_poll_id]
    options_list   = poll_info["options"]
    training_date  = poll_info["training_date"]
    config_poll_id = poll_info["config_poll_id"]

    user       = poll_answer.user
    user_id    = str(user.id)
    username   = (user.username or "").lstrip("@")
    first_name = user.first_name or ""
    last_name  = user.last_name or ""

    if not poll_answer.option_ids:
        vote_text = ""
        vote_type = "REMOVED"
    else:
        chosen = [options_list[i] for i in poll_answer.option_ids if i < len(options_list)]
        vote_text = " + ".join(chosen)
        vote_type = classify_vote(chosen[0] if chosen else "")

    try:
        _, attend_ws = _get_worksheets()
        upsert_vote(
            attend_ws,
            tg_poll_id, user_id, username, first_name, last_name,
            vote_text, vote_type, training_date, config_poll_id,
        )
    except Exception as e:
        log.error(f"Ошибка при сохранении голоса: {e}")


async def handle_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    # Только личка с админом. Если ADMIN_USER_ID не настроен — команда не работает нигде.
    if not user or not chat or chat.type != "private":
        return
    if not ADMIN_USER_ID or str(user.id) != ADMIN_USER_ID:
        return

    try:
        text = admin_panel.build_dashboard(_get_spreadsheet())
    except Exception as e:
        log.error(f"Ошибка при формировании админ-панели: {e}")
        text = "⚠️ Не удалось получить статистику, подробности в логах демона."

    await update.message.reply_text(text)


async def on_startup(app: Application) -> None:
    log.info("=" * 50)
    log.info("Бот запущен (long-polling режим)")
    log.info(f"Время старта: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    log.info("=" * 50)
    _refresh_poll_cache()


async def on_shutdown(app: Application) -> None:
    log.info("Бот остановлен.")


def main() -> None:
    if not BOT_TOKEN:
        log.error("BOT_TOKEN не задан в .env")
        sys.exit(1)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(PollAnswerHandler(handle_poll_answer))
    app.add_handler(CommandHandler("admin", handle_admin))

    log.info("Запуск polling...")
    app.run_polling(
        allowed_updates=["poll_answer", "message"],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
