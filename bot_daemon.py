#!/usr/bin/env python3
"""
Постоянно работающий демон бота.
Обрабатывает голоса в опросах в реальном времени (вместо hourly GitHub Actions)
и интерактивное админ-меню (/admin) с inline-кнопками.
Запускается как systemd-сервис и работает непрерывно.
"""

import asyncio
import io
import json
import logging
import os
import re
import signal
import sys
import time
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telegram import (
    BotCommand,
    BotCommandScopeChat,
    BotCommandScopeDefault,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    MenuButtonCommands,
    MenuButtonWebApp,
    ReplyKeyboardMarkup,
    Update,
    WebAppInfo,
)
from telegram.ext import (
    Application,
    ApplicationHandlerStop,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PollAnswerHandler,
    filters,
)

load_dotenv()

BOT_TOKEN         = os.getenv("BOT_TOKEN", "")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")
SPREADSHEET_ID    = os.getenv("SPREADSHEET_ID", "")
ADMIN_USER_IDS    = {x.strip() for x in os.getenv("ADMIN_USER_IDS", os.getenv("ADMIN_USER_ID", "")).split(",") if x.strip()}

DAEMON_LOG_PATH = os.getenv("DAEMON_LOG_PATH", "/var/log/basketball-bot/daemon.log")

# Mini App фэнтези: фронт — статикой на GitHub Pages (FANTASY_WEBAPP_URL),
# данные и сохранение состава — через живой API (Cloudflare-туннель на
# api.one4two.ru, запасной путь — Tailscale Funnel).
FANTASY_WEBAPP_URL = os.getenv("FANTASY_WEBAPP_URL", "").strip()

# ЗАПАСНОЙ ВХОД (по умолчанию выключен, 31.07.2026). Пока туннеля не было, у
# части игроков живой API не открывался, и приложение работало «чёрным ходом»:
# reply-кнопка с данными прямо в URL (#d=payload) и сохранение состава через
# Telegram sendData, минуя сервер. С поднятым туннелем это лишняя дверь —
# лишний вход, лишние килобайты в клавиатуре и лишний способ разойтись с базой.
# Включается обратно одной переменной окружения, если туннель когда-нибудь ляжет:
#   FANTASY_FALLBACK_BUTTON=1
FANTASY_FALLBACK_BUTTON = os.getenv("FANTASY_FALLBACK_BUTTON", "").strip().lower() \
    in ("1", "true", "yes", "on")


_WEBAPP_VERSION = str(int(time.time()))


def _webapp_url() -> str:
    """URL Mini App с версией и текущим адресом живого API. Версия (?v=) меняется
    при каждом перезапуске демона — после деплоя пользователь получает свежий
    фронт, а не старый JS. Адрес API (?api=) добавляется, только если он задан
    в env: обычно его нет, и фронт идёт на зашитый Funnel."""
    if not FANTASY_WEBAPP_URL:
        return ""
    sep = "&" if "?" in FANTASY_WEBAPP_URL else "?"
    url = f"{FANTASY_WEBAPP_URL}{sep}v={_WEBAPP_VERSION}"
    api = fantasy_api.public_api_url()
    if api:
        from urllib.parse import quote
        url += "&api=" + quote(api, safe="")
    return url


async def _setup_menu_button(app) -> None:
    """Кнопка «Открыть» слева от поля ввода. Ставим её только вместе с живым
    API: из кнопки меню Telegram не даёт `sendData`, и без бэкенда приложение
    смогло бы лишь показывать данные, но не сохранять состав."""
    try:
        if FANTASY_WEBAPP_URL and FANTASY_API_ENABLED:
            await app.bot.set_chat_menu_button(
                menu_button=MenuButtonWebApp(text="Фэнтези",
                                             web_app=WebAppInfo(url=_webapp_url())))
            log.info("Кнопка меню: Mini App фэнтези")
        else:
            await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())
            log.info("Кнопка меню: список команд (живой API выключен)")
    except Exception as e:
        log.warning(f"Не удалось настроить кнопку меню: {e}")


def _scrub_token_from_old_log() -> None:
    """Одноразовая зачистка: до фикса httpx-логирования в daemon.log попадали
    URL Telegram API с токеном. Файл могут читать другие пользователи
    сервера, поэтому вычищаем токен из уже накопленных строк. Выполняется
    ДО открытия FileHandler, пока файл никто не дописывает."""
    if not BOT_TOKEN:
        return
    try:
        with open(DAEMON_LOG_PATH, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
        if BOT_TOKEN in content:
            with open(DAEMON_LOG_PATH, "w", encoding="utf-8") as f:
                f.write(content.replace(BOT_TOKEN, "***TOKEN-REDACTED***"))
    except OSError:
        pass  # локальный запуск без этого файла — не критично


class _RedactTokenFilter(logging.Filter):
    """Страховка: если токен каким-то путём снова окажется в сообщении
    лога (новая библиотека, DEBUG-режим), замазываем его до записи."""
    def filter(self, record: logging.LogRecord) -> bool:
        if BOT_TOKEN:
            msg = record.getMessage()
            if BOT_TOKEN in msg:
                record.msg = msg.replace(BOT_TOKEN, "***TOKEN-REDACTED***")
                record.args = ()
        return True


_scrub_token_from_old_log()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(DAEMON_LOG_PATH, encoding="utf-8"),
    ],
)
for _handler in logging.getLogger().handlers:
    _handler.addFilter(_RedactTokenFilter())
log = logging.getLogger(__name__)

# httpx/httpcore логируют полный URL запроса на уровне INFO, а URL Telegram API
# содержит BOT_TOKEN — поднимаем порог, чтобы токен не попадал в логи/журнал.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# Логи не должны быть читаемы посторонними пользователями сервера (в
# daemon.log исторически попадал BOT_TOKEN через httpx). chmod при каждом
# старте, а не разово руками: cron-ротация пересоздаёт файлы через mv и
# может вернуть широкие права.
for _log_path, _log_mode in (("/var/log/basketball-bot", 0o750),
                             ("/var/log/basketball-bot/daemon.log", 0o640)):
    try:
        os.chmod(_log_path, _log_mode)
    except OSError:
        pass  # локальный запуск без этого каталога / нет прав — не критично


# Импортируем логику из collect_votes (переиспользуем без изменений).
# upsert_vote (прямая запись в Sheets) больше не используется здесь — голоса
# локально-первичные, см. sheets_cache.upsert_vote_local/upsert_game_vote_local.
from collect_votes import (
    _init_sheets,
    classify_vote,
    classify_game_vote,
)
import admin_panel
import sheets_cache
import script_runner
import game_watcher
import fantasy_api
from enhanced_duplicate_protection import duplicate_protection

# Фэнтези-API (aiohttp) — включается флагом; наружу через Cloudflare Tunnel.
FANTASY_API_ENABLED = os.getenv("FANTASY_API_ENABLED", "false").lower() == "true"
FANTASY_API_PORT = int(os.getenv("FANTASY_API_PORT", "8081"))
_fantasy_runner = None

# Кэш зарегистрированных опросов (обновляем раз в 5 минут) — тренировки и игры
_poll_cache: dict = {}
_game_poll_cache: dict = {}
_poll_cache_time: float = 0.0
_spreadsheet = None

# Локальный SQLite-кэш листов Sheets для /admin (обновляем раз в 5 минут)
_db_sync_time: float = 0.0


def _get_spreadsheet():
    global _spreadsheet
    if _spreadsheet is None:
        _spreadsheet = _init_sheets()
    return _spreadsheet


def _refresh_poll_cache() -> None:
    """Читает реестр опросов (TRAINING_POLL_REG/GAME_POLL_REG) напрямую из
    локальной service_records — то же место, куда пишет add_record(), без
    задержки push'а в Sheets (раньше тренировочный реестр читался из живого
    Sheets, что могло на несколько часов "терять из виду" только что
    зарегистрированный опрос)."""
    global _poll_cache, _game_poll_cache, _poll_cache_time
    now = time.time()
    if now - _poll_cache_time < 300:  # 5 минут
        return
    try:
        _poll_cache = sheets_cache.load_poll_registrations_local("TRAINING_POLL_REG")
        _game_poll_cache = sheets_cache.load_poll_registrations_local("GAME_POLL_REG")
        _poll_cache_time = now
        log.info(f"Кэш опросов обновлён: {len(_poll_cache)} тренировочных, {len(_game_poll_cache)} игровых")
    except Exception as e:
        log.warning(f"Не удалось обновить кэш опросов: {e}")


def _refresh_db_cache() -> None:
    global _db_sync_time
    now = time.time()
    if now - _db_sync_time < 300:  # 5 минут, тот же интервал что и poll cache
        return
    try:
        sheets_cache.sync_all(_get_spreadsheet())
        _db_sync_time = now
    except Exception as e:
        log.warning(f"Не удалось обновить SQLite-кэш: {e}")


PUSH_INTERVAL_SECONDS = 6 * 60 * 60  # 6 часов — периодическая выгрузка в Sheets
_last_push_time: float = 0.0


def _push_local_changes() -> dict:
    """Выгружает накопленные локальные изменения (service_records,
    attendance, game_votes — все dirty=1) в Sheets. Используется и
    периодическим циклом демона, и кнопкой '🔄 Синхронизация' в /admin."""
    sp = _get_spreadsheet()
    result = {}
    try:
        result["service_records"] = sheets_cache.push_service_records(sp)
    except Exception as e:
        log.warning(f"Не удалось выгрузить service_records: {e}")
        result["service_records"] = {"error": str(e)}
    try:
        result["attendance"] = sheets_cache.push_attendance(sp)
    except Exception as e:
        log.warning(f"Не удалось выгрузить attendance: {e}")
        result["attendance"] = {"error": str(e)}
    try:
        result["game_votes"] = sheets_cache.push_game_votes(sp)
    except Exception as e:
        log.warning(f"Не удалось выгрузить game_votes: {e}")
        result["game_votes"] = {"error": str(e)}
    return result


def _periodic_push_local_changes() -> None:
    global _last_push_time
    now = time.time()
    if now - _last_push_time < PUSH_INTERVAL_SECONDS:
        return
    _push_local_changes()
    _last_push_time = now


async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    poll_answer = update.poll_answer
    if not poll_answer:
        return

    await asyncio.to_thread(_refresh_poll_cache)
    await asyncio.to_thread(_refresh_db_cache)
    await asyncio.to_thread(_periodic_push_local_changes)

    tg_poll_id = str(poll_answer.poll_id)
    is_training = tg_poll_id in _poll_cache
    is_game = tg_poll_id in _game_poll_cache
    if not is_training and not is_game:
        return  # ни тренировочный, ни игровой опрос

    poll_info = _poll_cache[tg_poll_id] if is_training else _game_poll_cache[tg_poll_id]
    options_list = poll_info["options"]

    user       = poll_answer.user
    user_id    = str(user.id)
    username   = (user.username or "").lstrip("@")
    first_name = user.first_name or ""
    last_name  = user.last_name or ""

    if not poll_answer.option_ids:
        vote_text = ""
        chosen_first = ""
    else:
        chosen = [options_list[i] for i in poll_answer.option_ids if i < len(options_list)]
        vote_text = " + ".join(chosen)
        chosen_first = chosen[0] if chosen else ""

    try:
        if is_training:
            vote_type = classify_vote(chosen_first) if vote_text else "REMOVED"
            # Голоса — локально-первичные (пишем в SQLite сразу, выгрузка в
            # Sheets отдельно, периодически/по кнопке — см. push_attendance).
            sheets_cache.upsert_vote_local(
                tg_poll_id, user_id, username, first_name, last_name,
                vote_text, vote_type, poll_info["training_date"], poll_info["config_poll_id"],
            )
        else:
            vote_type = classify_game_vote(chosen_first) if vote_text else "REMOVED"
            sheets_cache.upsert_game_vote_local(
                tg_poll_id, user_id, username, first_name, last_name,
                vote_text, vote_type, poll_info["game_id"], poll_info["training_date"],
            )
    except Exception as e:
        log.error(f"Ошибка при сохранении голоса: {e}")
        sheets_cache.report_error("handle_poll_answer", str(e), _get_spreadsheet())


# ─────────────────────────── Админ-меню ───────────────────────────────────

def _is_admin(user) -> bool:
    return bool(user) and bool(ADMIN_USER_IDS) and str(user.id) in ADMIN_USER_IDS


def _has_access(kind: str, user) -> bool:
    """Открыт ли человеку закрытый раздел. Админу — всё, остальным по выдаче.

    Доступ даётся по @нику, но живёт на числовом id: ник меняется и
    переуступается (см. sheets_cache.has_access)."""
    if not user:
        return False
    if _is_admin(user):
        return True
    try:
        return sheets_cache.has_access(kind, str(user.id), user.username or "")
    except Exception as e:
        log.warning(f"Проверка доступа «{kind}» не прошла: {e}")
        return False


def _can_see_reports(user) -> bool:
    return _has_access(sheets_cache.ACCESS_TEAM, user)


def _can_see_personal(user) -> bool:
    return _has_access(sheets_cache.ACCESS_PERSONAL, user)


# Подписи кнопок нижней клавиатуры. Она постоянная (is_persistent) и висит под
# полем ввода независимо от того, куда пролистан чат, — команд стало много, и
# держать их под рукой удобнее, чем искать сообщение с меню.
ADMIN_KEYBOARD_LABEL = "📊 Админ-панель"
PROGRESS_KEYBOARD_LABEL = "📈 Прогресс команды"
MYSTATS_KEYBOARD_LABEL = "📊 Моя статистика"
COACH_KEYBOARD_LABEL = "🧑‍🏫 Тренер"
FANTASY_KEYBOARD_LABEL = "🏀 Фэнтези"
FEEDBACK_KEYBOARD_LABEL = "💬 Написать админам"
MENU_KEYBOARD_LABEL = "☰ Меню"


def _bottom_keyboard(payload: str = "", is_admin: bool = False,
                     with_fantasy: bool = True, with_reports: bool = False,
                     with_personal: bool = False,
                     with_menu: bool = False) -> ReplyKeyboardMarkup:
    """Нижняя клавиатура: обратная связь, закрытые разделы и — админу — панель.

    Кнопки фэнтези тут по умолчанию НЕТ: приложение открывается кнопкой меню
    слева от поля ввода. Reply-кнопка была запасным входом на время, пока у
    части игроков не работал живой API (см. FANTASY_FALLBACK_BUTTON)."""
    rows: List[List[KeyboardButton]] = []
    if with_fantasy and FANTASY_FALLBACK_BUTTON and _webapp_url():
        url = _webapp_url() + ("#d=" + payload if payload else "")
        rows.append([KeyboardButton(FANTASY_KEYBOARD_LABEL, web_app=WebAppInfo(url=url))])
    # «Меню» — одна кнопка вместо россыпи: под ней шутки, обратная связь и
    # подписки. Видна только своим: посторонним там нечего делать, а игрокам
    # не приходится каждый раз спрашивать, где что писать.
    if with_menu:
        rows.append([KeyboardButton(MENU_KEYBOARD_LABEL)])
    else:
        rows.append([KeyboardButton(FEEDBACK_KEYBOARD_LABEL)])
    # Закрытые разделы: у кого есть доступ, тот и видит кнопку. Нет ни одного —
    # ни одной лишней кнопки под чатом.
    closed = []
    if with_personal:
        closed.append(KeyboardButton(MYSTATS_KEYBOARD_LABEL))
    if with_reports:
        # Раздел тренера вместо отдельной кнопки прогресса: разбор игр теперь
        # внутри него, рядом с оплатами.
        closed.append(KeyboardButton(COACH_KEYBOARD_LABEL))
    if closed:
        rows.append(closed)
    if is_admin:
        rows.append([KeyboardButton(ADMIN_KEYBOARD_LABEL)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True)


# Сколько данных пробуем запечь в кнопку фэнтези. Telegram отвергает слишком
# длинную клавиатуру, а какой именно потолок — не документировано, поэтому
# спускаемся по ступеням: полнее -> короче -> вообще без данных (лишь бы
# клавиатура появилась).
PAYLOAD_BUDGETS = (8000, 4000, None)      # None — кнопка вообще без данных

# Фоновые задачи, запущенные из обработчиков (прогрев пула). Держим ссылки:
# задачу без владельца сборщик мусора вправе выкинуть на полпути.
_side_tasks: set = set()

# Сколько ждать прогрева пула, отвечая человеку. Живая лига укладывается в
# секунду; всё, что дольше, — уже она недоступна, и ждать её незачем.
POOL_WAIT_SECONDS = 3.0


async def send_bottom_keyboard(message, user, text: str) -> None:
    """Показывает нижнюю клавиатуру.

    Пока был включён запасной вход, сюда упаковывались данные фэнтези, и
    ступени бюджета нужны были потому, что Telegram отвергает слишком длинную
    клавиатуру. С выключенным FANTASY_FALLBACK_BUTTON ничего этого не
    происходит: кнопки фэнтези в клавиатуре нет, payload не собирается, и
    /start отвечает мгновенно."""
    is_admin = _is_admin(user)
    uid = str(user.id)
    # Закрытые разделы — по выданному доступу, у админа оба.
    with_reports = _can_see_reports(user)
    with_personal = _can_see_personal(user)
    # Свой ли это человек: от этого зависят и «Меню», и запасная кнопка.
    try:
        is_member = is_admin or fantasy_api._is_team_member(uid, user.username or "")
    except Exception as e:
        log.warning(f"проверка состава для клавиатуры: {e}")
        is_member = is_admin
    with_fantasy = bool(FANTASY_FALLBACK_BUTTON and FANTASY_WEBAPP_URL
                        and _webapp_url() and is_member)

    # Запасные данные в кнопке собираются из пула. Пул греет фоновый цикл; если
    # он холодный (демон только поднялся) — подождём немного и уйдём без них.
    with_fantasy_payload = with_fantasy
    if with_fantasy and not fantasy_api.pool_is_warm():
        # Ссылку держим: задачу без владельца сборщик мусора вправе выкинуть
        # на полпути, и прогрев молча не случится.
        task = asyncio.create_task(_warm_fantasy_pool(force=True))
        _side_tasks.add(task)
        task.add_done_callback(_side_tasks.discard)
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=POOL_WAIT_SECONDS)
        except asyncio.TimeoutError:
            pass
        with_fantasy_payload = fantasy_api.pool_is_warm()
        if not with_fantasy_payload:
            log.info("клавиатура: пул фэнтези не успел прогреться — отдаю без запасных данных")

    payload: Optional[str] = None
    for budget in PAYLOAD_BUDGETS:
        if not with_fantasy_payload or budget is None:
            payload = ""
        elif payload is None:
            try:
                payload = await fantasy_api.build_webapp_payload(uid, max_len=budget)
            except Exception as e:
                log.warning(f"payload кнопки (бюджет {budget}) не собрался: {e}")
                continue
        try:
            await message.reply_text(text, reply_markup=_bottom_keyboard(
                payload or "", is_admin=is_admin, with_fantasy=with_fantasy,
                with_reports=with_reports, with_personal=with_personal,
                with_menu=True))
            return
        except Exception as e:
            log.warning(f"нижняя клавиатура (бюджет {budget}) не ушла: {e}")
            payload = None
    log.warning("нижняя клавиатура не отправлена")


async def _send_main_menu(update: Update) -> None:
    for attempt in range(3):
        try:
            await update.message.reply_text("📊 Админ-панель", reply_markup=_main_menu_markup())
            return
        except Exception as e:
            log.warning(f"Не удалось отправить админ-панель (попытка {attempt + 1}/3): {e}")
            await asyncio.sleep(2)
    log.error("Не удалось отправить админ-панель после 3 попыток")


PLAYER_MENU_TEXT = ("🏀 Привет!\n\n"
                    "• 🏆 Фэнтези — кнопка «Фэнтези» слева от поля ввода: "
                    "собрать состав, таблица, топ игроков\n"
                    "• ☰ Меню — внизу экрана: шутки к фамилиям, подписки, "
                    "написать админам\n\n"
                    "Опросы на игры и тренировки я присылаю сам в общий чат.")


async def handle_feedback_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Нажатие кнопки «💬 Написать админам» на нижней клавиатуре."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    _awaiting_feedback.add(user.id)
    await msg.reply_text(FEEDBACK_ASK)
    raise ApplicationHandlerStop


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return

    # /start обещан как выход из любого незаконченного ввода — в том числе из
    # недовведённого платежа.
    _awaiting_payment.discard(user.id)
    _pay_draft.pop(user.id, None)
    _awaiting_video.pop(user.id, None)
    _awaiting_field.pop(user.id, None)
    _awaiting_money.pop(user.id, None)

    # Фиксируем ЛЮБОГО пользователя, который запустил бота — не только
    # админа. Нужно для "Список пользователей → В боте".
    try:
        # В поток: запись идёт в Google Sheets, а синхронный поход в сеть прямо
        # из обработчика держит весь демон, пока Google отвечает.
        await asyncio.to_thread(
            sheets_cache.record_bot_user, _get_spreadsheet(), str(user.id),
            user.username or "", user.first_name or "")
    except Exception as e:
        log.warning(f"Не удалось записать пользователя бота: {e}")

    # Опознаём игрока ПРЯМО ЗДЕСЬ: здесь бот впервые видит числовой id рядом с
    # @ником, а больше их вместе взять негде. Раньше привязка жила только в
    # живом API, и опознание требовало пройти всю цепочку «/start → открыл
    # Mini App → достучался до сервера»: споткнулся на любом шаге — остался
    # чужим навсегда, хотя ник в листе стоял. Идемпотентно, лишний раз не пишет.
    try:
        import fantasy_api
        if fantasy_api.ensure_player_link(str(user.id), user.username or ""):
            log.info(f"/start: {user.id} (@{user.username or '—'}) опознан как игрок команды")
    except Exception as e:
        log.warning(f"/start: привязка игрока не прошла: {e}")

    if _is_admin(user):
        # Синхронизация с таблицей — приятный побочный эффект /start, но если
        # Google недоступен, клавиатура всё равно должна прийти: иначе молчок.
        try:
            await asyncio.to_thread(_refresh_db_cache)
            await asyncio.to_thread(_periodic_push_local_changes)
        except Exception as e:
            log.warning(f"/start: синхронизация не прошла: {e}")
    await send_bottom_keyboard(update.message, user, PLAYER_MENU_TEXT)


def _format_progress(source: str, player_id: str) -> str:
    """Личный прогресс по локальной копии протоколов. Пусто — если игр этого
    человека у нас ещё нет."""
    import fantasy_stats
    import player_identity
    s = fantasy_stats.career_summary(source, player_id)
    title = player_identity.SOURCE_TITLES.get(source, source)
    if not s.get("games"):
        return (f"• {title}: игр пока не нашёл. Если ты играешь в турнире, который "
                f"бот ещё не зеркалит, статистика подтянется после ближайшего обновления.")
    a, last, form = s["avg"], s["last"], s["form"]
    lines = [
        f"• {title}: {s['games']} игр ({s['first_date']} … {s['last_date']})",
        f"   в среднем за игру: {a['pts']} очк · {a['reb']} подб · {a['ast']} пас · "
        f"{a['stl']} перехв · {a['blk']} блок · {a['tur']} потерь",
        f"   последняя игра {last['date']}: {last['pts']} очк · {last['reb']} подб · "
        f"{last['ast']} пас",
    ]
    # Форма: сравниваем только когда есть с чем сравнивать, иначе цифра врёт.
    if form["prev_n"]:
        delta = round(form["recent"] - form["earlier"], 1)
        arrow = "📈" if delta > 0 else ("📉" if delta < 0 else "➖")
        lines.append(f"   форма {arrow} за {form['n']} игр {form['recent']} против "
                     f"{form['earlier']} за предыдущие {form['prev_n']}")
    return "\n".join(lines)


# Кто сейчас пишет обращение (нажал /feedback без текста). Только в памяти:
# после рестарта человек просто нажмёт ещё раз, терять тут нечего.
_awaiting_feedback: set = set()

FEEDBACK_ASK = ("💬 Напиши одним сообщением, что предложить или что сломалось — "
                "передам админам.\n\nПередумал — /start.")


async def _deliver_feedback(context: ContextTypes.DEFAULT_TYPE, user, text: str) -> int:
    """Сохраняет обращение и шлёт его админам в личку. Возвращает номер.

    Сначала пишем в базу, потом отправляем: если у админа закрыта личка или
    Telegram отвалился, обращение всё равно не потеряется — оно видно в
    админке «Лог действий → Обратная связь»."""
    fid = sheets_cache.add_feedback(user.id, user.username or "", user.first_name or "", text)
    who = f"@{user.username}" if user.username else (user.first_name or f"id {user.id}")
    note = f"💬 Обратная связь №{fid} от {who}:\n\n{text[:3500]}"
    for admin_id in ADMIN_USER_IDS:
        try:
            await context.bot.send_message(chat_id=int(admin_id), text=note)
        except Exception as e:
            log.warning(f"обратная связь №{fid}: не доставлено админу {admin_id}: {e}")
    return fid


async def handle_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/feedback [текст] — обращение к админам (бэклог п.12)."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    text = (msg.text or "").partition(" ")[2].strip()
    if not text:
        _awaiting_feedback.add(user.id)
        await msg.reply_text(FEEDBACK_ASK)
        return
    fid = await _deliver_feedback(context, user, text)
    await msg.reply_text(f"Спасибо, передал (обращение №{fid}).")


async def handle_feedback_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Следующее сообщение после /feedback. Висит ПЕРЕД разбором ссылок на
    профиль, поэтому текст обращения не уедет в привязку id."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if user.id not in _awaiting_feedback:
        return
    _awaiting_feedback.discard(user.id)
    text = (msg.text or "").strip()
    if not text:
        return
    fid = await _deliver_feedback(context, user, text)
    await msg.reply_text(f"Спасибо, передал (обращение №{fid}).")
    raise ApplicationHandlerStop


# ─── шутки к фамилиям ───────────────────────────────────────────────────────
#
# Диалог короткий и живёт только в памяти: фамилия -> случай -> фраза. После
# перезапуска человек просто начнёт заново, терять тут нечего.
# {tg_id: {"stage": "name"|"text", "row": int, "target": str, "occasion": str}}
_joke_draft: Dict[int, Dict[str, Any]] = {}

JOKE_HELP = ("😄 Шутки к фамилиям\n\n"
             "Оставь фразу игроку — своему или чужому. Когда он попадёт в "
             "лучшие (или в антилидеры) в сообщении о результате, бот допишет "
             "её к строке и подпишет твоим ником.\n\n"
             "Фраза срабатывает ОДИН РАЗ — в той игре, которую ты выберешь.")


def _joke_games_markup() -> InlineKeyboardMarkup:
    """Кнопки выбора игры: ближайшие матчи и «на ближайшую, где будет играть»."""
    import player_jokes
    rows = []
    for i, g in enumerate(player_jokes.upcoming_games()):
        rows.append([InlineKeyboardButton(g["label"], callback_data=f"joke:game:{i}")])
    rows.append([InlineKeyboardButton("⏭ На ближайшую, где будет играть",
                                      callback_data="joke:game:next")])
    return InlineKeyboardMarkup(rows)


def _joke_menu(uid: int, is_admin: bool = False) -> Tuple[str, InlineKeyboardMarkup]:
    import player_jokes
    mine = player_jokes.listing(uid)
    lines = [JOKE_HELP, ""]
    if mine:
        lines.append(f"Твоих фраз: {len(mine)} из {player_jokes.MAX_PER_AUTHOR}.")
    rows = [[InlineKeyboardButton("➕ Добавить фразу", callback_data="joke:add")]]
    if mine:
        rows.append([InlineKeyboardButton("📋 Мои фразы", callback_data="joke:mine")])
    if is_admin:
        rows.append([InlineKeyboardButton("👀 Все фразы команды", callback_data="joke:all")])
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _joke_list_screen(uid: Optional[int], title: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Список фраз с кнопками удаления. uid=None — админский вид (все)."""
    import player_jokes
    items = player_jokes.listing(uid)
    if not items:
        return "Пока пусто.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ Назад", callback_data="joke:menu")]])
    lines = [title, ""]
    rows = []
    for i, j in enumerate(items[:20], 1):
        when = j["game_label"] or "ближайшая игра"
        who = f" · @{j['author_nick']}" if (uid is None and j["author_nick"]) else ""
        lines.append(f"{i}. {j['target']} → {when}{who}\n   «{j['text']}»")
        rows.append([InlineKeyboardButton(f"🗑 Удалить {i}",
                                          callback_data=f"joke:del:{j['id']}")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="joke:menu")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


MENU_TEXT = ("☰ Меню\n\n"
             "Здесь всё, что можно сделать в боте руками. Опросы, анонсы и "
             "результаты я присылаю сам.")


def _menu_markup(is_member: bool = False) -> InlineKeyboardMarkup:
    """Меню открыто всем — посторонним тоже есть что тут делать (написать
    админам). А вот шутки к фамилиям только своим: они уходят в общий чат
    команды, и подписывать чужих людей человек со стороны не должен."""
    rows = []
    if is_member:
        rows.append([InlineKeyboardButton("😄 Шутки к фамилиям",
                                          callback_data="menu:jokes")])
    rows.append([InlineKeyboardButton("🔔 Мои подписки", callback_data="menu:subs")])
    # Проверка связи — всем и всегда: жалоба «фэнтези не открывается» почти
    # всегда про канал до сервера, а увидеть это можно только с устройства
    # самого человека. Ссылка ведёт в приложение сразу на экран проверки.
    url = _webapp_url()
    if url:
        rows.append([InlineKeyboardButton(
            "🔌 Проверить связь", web_app=WebAppInfo(url=url + "#diag"))])
    rows.append([InlineKeyboardButton("💬 Написать админам",
                                      callback_data="menu:feedback")])
    return InlineKeyboardMarkup(rows)


def _is_member(user) -> bool:
    try:
        return bool(_is_admin(user)
                    or fantasy_api._is_team_member(str(user.id), user.username or ""))
    except Exception as e:
        log.warning(f"проверка состава: {e}")
        return bool(_is_admin(user))


def _subs_markup(uid: Any) -> InlineKeyboardMarkup:
    import subscriptions
    state = subscriptions.all_of(uid)
    rows = [[InlineKeyboardButton(f"{'✅' if state[k] else '🚫'} {title}",
                                  callback_data=f"menu:sub:{k}")]
            for k, title in subscriptions.KINDS.items()]
    mine = len(subscriptions.my_players(uid))
    rows.append([InlineKeyboardButton(
        f"🏀 Слежу за игроками: {mine}" if mine else "🏀 Следить за игроком",
        callback_data="menu:players")])
    rows.append([InlineKeyboardButton("⬅️ В меню", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


# Кого показываем на одном экране выбора игрока.
PLAYERS_SUB_PAGE = 8


async def _player_subs_screen(uid: Any, page: int = 0, query: str = ""
                              ) -> Tuple[str, InlineKeyboardMarkup]:
    """Кто из игроков «под наблюдением» и кого можно добавить.

    Подписка на игрока — это «покажи мне матч, когда играет он», а не «когда
    играет его команда»: рассылка сверяется с протоколом
    (subscriptions.watchers_of_game)."""
    import player_names
    import subscriptions
    pool = await fantasy_api.build_pool()
    names = {p["ref"]: (p.get("name") or p["ref"]) for p in pool}
    mine = subscriptions.my_players(uid)

    lines = ["🏀 Слежу за игроками", "",
             "Придёт в личку, когда этот человек сыграл: счёт, его строка "
             "и ссылка на протокол.", ""]
    rows: List[List[InlineKeyboardButton]] = []

    # Реестр имён живёт в памяти и после рестарта пуст: в пуле тогда номера
    # вместо фамилий. Выбирать, за кем следить, по «ID 170068» невозможно —
    # честнее попросить подождать, чем показать список цифр.
    if player_names.is_cold():
        lines += ["⏳ Имена игроков ещё подгружаются — загляни через минуту."]
        if mine:
            lines += ["", "Сейчас слежу за: " + str(len(mine))]
        rows.append([InlineKeyboardButton("⬅️ К подпискам", callback_data="menu:subs")])
        return "\n".join(lines), InlineKeyboardMarkup(rows)
    if mine:
        lines.append("Уже слежу:")
        for ref in mine:
            lines.append(f"• {names.get(ref, ref)}")
            rows.append([InlineKeyboardButton(
                f"➖ {names.get(ref, ref)}"[:60],
                callback_data=f"menu:punsub:{ref}")])
        lines.append("")
    else:
        lines.append("Пока ни за кем. Выбери из списка ниже.")
        lines.append("")

    # Кого ещё можно добавить: весь пул минус уже выбранные.
    rest = [p for p in pool if p["ref"] not in set(mine)]
    if query:
        import player_search
        needle = player_search.norm(query)
        rest = [p for p in rest if needle in player_search.norm(p.get("name") or "")]
    rest.sort(key=lambda p: (p.get("name") or ""))
    pages = max(1, (len(rest) + PLAYERS_SUB_PAGE - 1) // PLAYERS_SUB_PAGE)
    page = max(0, min(page, pages - 1))
    for p in rest[page * PLAYERS_SUB_PAGE:(page + 1) * PLAYERS_SUB_PAGE]:
        rows.append([InlineKeyboardButton(f"➕ {p.get('name') or p['ref']}"[:60],
                                          callback_data=f"menu:psub:{p['ref']}")])
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"menu:ppage:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="menu:pnoop"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"menu:ppage:{page + 1}"))
        rows.append(nav)
    if not rest:
        lines.append("Больше добавить некого.")
    rows.append([InlineKeyboardButton("⬅️ К подпискам", callback_data="menu:subs")])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(rows)


def _subs_text(uid: Any) -> str:
    import subscriptions
    state = subscriptions.all_of(uid)
    lines = ["🔔 Мои подписки", "",
             "Нажми, чтобы включить или выключить. В общий чат всё приходит "
             "как обычно — это только про личку.", ""]
    for kind, title in subscriptions.KINDS.items():
        lines.append(f"{'✅' if state[kind] else '🚫'} {title}")
        lines.append(f"    {subscriptions.HINTS[kind]}")
        lines.append("")
    return "\n".join(lines).rstrip()


async def handle_menu_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопка «☰ Меню» на нижней клавиатуре."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    await msg.reply_text(MENU_TEXT, reply_markup=_menu_markup(_is_member(user)))
    raise ApplicationHandlerStop


async def handle_menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query, user = update.callback_query, update.effective_user
    if not query or not user:
        return
    await query.answer()
    parts = (query.data or "").split(":")
    what = parts[1] if len(parts) > 1 else ""

    if what == "main":
        await query.edit_message_text(MENU_TEXT,
                                      reply_markup=_menu_markup(_is_member(user)))
    elif what == "jokes":
        if not _is_member(user):
            await query.answer("Это для игроков команды", show_alert=True)
            return
        text, markup = _joke_menu(user.id, _is_admin(user))
        await query.edit_message_text(text, reply_markup=markup)
    elif what == "subs":
        await query.edit_message_text(_subs_text(user.id),
                                      reply_markup=_subs_markup(user.id))
    elif what == "sub" and len(parts) > 2:
        import subscriptions
        now_on = subscriptions.toggle(user.id, parts[2])
        await query.answer("Включено" if now_on else "Выключено")
        await query.edit_message_text(_subs_text(user.id),
                                      reply_markup=_subs_markup(user.id))
    elif what in ("players", "ppage"):
        page = int(parts[2]) if what == "ppage" and len(parts) > 2 else 0
        text, markup = await _player_subs_screen(user.id, page)
        await query.edit_message_text(text, reply_markup=markup)
    elif what in ("psub", "punsub") and len(parts) > 2:
        import subscriptions
        # ref вида «slpro:707:12996» сам содержит двоеточия — склеиваем обратно.
        ref = ":".join(parts[2:])
        now_on = await asyncio.to_thread(subscriptions.player_toggle, user.id, ref)
        await query.answer("Слежу" if now_on else "Больше не слежу")
        text, markup = await _player_subs_screen(user.id)
        await query.edit_message_text(text, reply_markup=markup)
    elif what == "pnoop":
        return
    elif what == "feedback":
        _awaiting_feedback.add(user.id)
        await query.edit_message_text(FEEDBACK_ASK)


async def handle_joke_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/joke — экран шуток. Только игрокам команды: чужие шутки про нашу
    команду в общий чат не летят."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if not (_is_admin(user) or fantasy_api._is_team_member(str(user.id), user.username or "")):
        await msg.reply_text("Эта штука для игроков команды. Нажми /start — "
                             "если ты в списке, я тебя узнаю.")
        return
    text, markup = _joke_menu(user.id, _is_admin(user))
    await msg.reply_text(text, reply_markup=markup)


async def handle_joke_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    import player_jokes
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return
    await query.answer()
    parts = (query.data or "").split(":")
    action = parts[1] if len(parts) > 1 else ""

    if action == "menu":
        text, markup = _joke_menu(user.id, _is_admin(user))
        await query.edit_message_text(text, reply_markup=markup)
    elif action == "add":
        _joke_draft[user.id] = {"stage": "name"}
        await query.edit_message_text(
            "Чью фамилию подписываем? Напиши одну фамилию — например, «Дроздов».\n\n"
            "Передумал — /start.")
    elif action == "pick" and len(parts) > 2:
        draft = _joke_draft.get(user.id) or {}
        row = int(parts[2])
        found = [p for p in player_jokes.find_player(draft.get("target", ""))
                 if p["row_index"] == row]
        if found:
            draft["target"] = f"{found[0]['surname']} {found[0]['name']}".strip()
        draft.update(row=row, stage="game")
        _joke_draft[user.id] = draft
        await query.edit_message_text(
            f"На какую игру фраза для «{draft.get('target', '')}»?",
            reply_markup=_joke_games_markup())
    elif action == "game" and len(parts) > 2:
        draft = _joke_draft.get(user.id) or {}
        if not draft.get("row"):
            await query.edit_message_text("Начни заново: /joke")
            return
        if parts[2] == "next":
            draft.update(game_source="", game_id="", game_label="", game_date="",
                         stage="occasion")
        else:
            games = player_jokes.upcoming_games()
            idx = int(parts[2])
            if idx >= len(games):
                await query.edit_message_text("Эта игра уже не в списке. Начни заново: /joke")
                return
            g = games[idx]
            draft.update(game_source=g["source"], game_id=g["game_id"],
                         game_label=g["label"], game_date=g["date"], stage="occasion")
        _joke_draft[user.id] = draft
        await query.edit_message_text(
            f"При каком исходе показывать фразу для «{draft.get('target', '')}»?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ После победы", callback_data="joke:when:win"),
                 InlineKeyboardButton("❌ После поражения", callback_data="joke:when:loss")],
                [InlineKeyboardButton("🤷 Любой исход", callback_data="joke:when:any")]]))
    elif action == "when" and len(parts) > 2:
        draft = _joke_draft.get(user.id) or {}
        if not draft.get("row"):
            await query.edit_message_text("Начни заново: /joke")
            return
        draft.update(occasion=parts[2], stage="text")
        _joke_draft[user.id] = draft
        where = (f"начиная с игры {draft['game_label']}" if draft.get("game_label")
                 else "в ближайшей игре, где он выйдет на площадку")
        when = {"win": ", если выиграем", "loss": ", если проиграем"}.get(parts[2], "")
        await query.edit_message_text(
            f"Пиши фразу для «{draft.get('target', '')}» — прозвучит {where}{when}, "
            f"один раз. Не попадёт в строку — подождёт следующей игры.\n\n"
            f"Одной строкой, до {player_jokes.MAX_LEN} символов. Её увидит весь чат "
            f"вместе с твоим ником.")
    elif action == "mine":
        text, markup = _joke_list_screen(user.id, "📋 Твои фразы")
        await query.edit_message_text(text, reply_markup=markup)
    elif action == "all" and _is_admin(user):
        text, markup = _joke_list_screen(None, "👀 Все фразы команды")
        await query.edit_message_text(text, reply_markup=markup)
    elif action == "del" and len(parts) > 2:
        # Свою — любой автор, чужую — только админ.
        ok = player_jokes.remove(int(parts[2]),
                                 None if _is_admin(user) else user.id)
        text, markup = _joke_list_screen(
            None if _is_admin(user) else user.id,
            "👀 Все фразы команды" if _is_admin(user) else "📋 Твои фразы")
        await query.edit_message_text(("Удалил.\n\n" if ok else "Не нашёл.\n\n") + text,
                                      reply_markup=markup)


async def handle_joke_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Фамилия и сама фраза. Висит на любом тексте в личке, поэтому первым
    делом проверяет, ждём ли мы что-то от этого человека."""
    import player_jokes
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    draft = _joke_draft.get(user.id)
    if not draft:
        return
    text = (msg.text or "").strip()

    if draft.get("stage") == "name":
        found = player_jokes.find_player(text)
        if not found:
            await msg.reply_text("Не нашёл такого в списке игроков. Проверь "
                                 "фамилию или напиши её полностью.")
            raise ApplicationHandlerStop
        if len(found) > 1:
            draft["target"] = text
            _joke_draft[user.id] = draft
            rows = [[InlineKeyboardButton(f"{p['surname']} {p['name']}".strip(),
                                          callback_data=f"joke:pick:{p['row_index']}")]
                    for p in found[:8]]
            await msg.reply_text("Кого именно?", reply_markup=InlineKeyboardMarkup(rows))
            raise ApplicationHandlerStop
        p = found[0]
        draft.update(row=p["row_index"], stage="game",
                     target=f"{p['surname']} {p['name']}".strip())
        _joke_draft[user.id] = draft
        await msg.reply_text(f"На какую игру фраза для «{draft['target']}»?",
                             reply_markup=_joke_games_markup())
        raise ApplicationHandlerStop

    if draft.get("stage") == "text":
        ok, said = player_jokes.add(
            draft["row"], draft.get("occasion", "any"), text,
            user.id, user.username or "",
            game_source=draft.get("game_source", ""),
            game_id=draft.get("game_id", ""),
            game_label=draft.get("game_label", ""),
            game_date=draft.get("game_date", ""))
        if ok:
            _joke_draft.pop(user.id, None)
            await msg.reply_text(
                f"😄 {said}\n\n«{text}» — для «{draft.get('target', '')}», "
                f"подпись: @{user.username or 'без ника'}.",
                reply_markup=_joke_menu(user.id, _is_admin(user))[1])
        else:
            await msg.reply_text(f"{said}\n\nПопробуй ещё раз или /start.")
        raise ApplicationHandlerStop


async def handle_profile_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Личное сообщение со ссылкой на профиль в лиге -> привязка id к человеку.

    Молчим, если ссылки в сообщении нет: обработчик висит на всех текстах в
    личке и не должен отвечать на обычную переписку."""
    msg = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if not _is_admin(user):
        return          # привязка профиля — часть скрытой личной статистики
    import player_identity
    parsed = None
    for word in (msg.text or "").split():
        parsed = player_identity.parse_profile_link(word)
        if parsed:
            break
    if not parsed:
        return

    # SLPRO умеет сказать, существует ли такой игрок — спрашиваем ДО привязки,
    # иначе опечатка в ссылке молча запомнится как «твой» несуществующий id.
    career = None
    if parsed["source"] == player_identity.SOURCE_SLPRO:
        try:
            from slpro_client import SlproClient
            info = await SlproClient().get_player_info(parsed["player_id"])
        except Exception as e:
            log.warning(f"SLPRO: проверка игрока {parsed['player_id']} не удалась: {e}")
            info = None          # лига недоступна — не мешаем привязке
        else:
            if not info:
                await msg.reply_text(
                    f"❌ В SLPRO нет игрока с id {parsed['player_id']}.\n\n"
                    "Проверь ссылку: нужна страница вида "
                    "https://slpro.basketstat.ru/player/XXXX")
                return
            career = info.get("career") or []

    res = player_identity.link_identity(user.id, parsed)
    title = player_identity.SOURCE_TITLES.get(parsed["source"], parsed["source"])
    if res.get("same"):
        head = f"✅ {title}: этот профиль уже привязан (id {parsed['player_id']})."
    elif res.get("changed"):
        head = (f"🔄 {title}: привязка изменена — id {res['previous']} → "
                f"{parsed['player_id']}.")
    else:
        head = f"✅ {title}: профиль привязан, id {parsed['player_id']}."

    # Инфобаскет отдаёт всю личную историю за пару запросов — качаем сразу, иначе
    # человек увидит только те игры, что попали в наше зеркало турнира команды.
    # SLPRO зеркалим целиком, там докачивать нечего.
    if parsed["source"] == player_identity.SOURCE_INFOBASKET:
        await msg.reply_text(head + "\n\n⏳ Собираю твою историю игр…")
        try:
            import stats_backfill
            got = await stats_backfill.fetch_person_games_infobasket(
                parsed["player_id"], parsed.get("api_url") or stats_backfill.IB_API)
            log.info(f"личная история {parsed['player_id']}: сезоны {got['seasons']}, "
                     f"игр {got['games']}, добавлено {got['added']}")
        except Exception as e:
            log.warning(f"личная история {parsed['player_id']} не скачалась: {e}")
        await msg.reply_text(_format_progress(parsed["source"], parsed["player_id"]))
        return

    text = head + "\n\n" + _format_progress(parsed["source"], parsed["player_id"])
    if career:
        seasons = ", ".join(sorted({str(c.get("season")) for c in career if c.get("season")},
                                   reverse=True))
        text += f"\n   сезоны в лиге: {seasons}"
        text += "\n" + _coverage_note(parsed["player_id"], career)
    await msg.reply_text(text)


def _coverage_note(player_id: str, career: List[Dict[str, Any]]) -> str:
    """Честно говорим, всю ли карьеру видно. Копия лиги неполная, и молча
    показывать «столько игр, сколько нашлось» — значит выдавать пробел за факт."""
    import player_identity
    league = 0
    for c in career:
        for st in (c.get("stats") or []):
            league += int(st.get("games") or 0)
    local = player_identity.have_games(player_identity.SOURCE_SLPRO, player_id)
    if not league:
        return ""
    if local >= league:
        return f"   охват: все {league} игр лиги уже в базе ✅"
    return (f"   охват: {local} из {league} игр лиги — остальные подтянутся "
            f"ночным обновлением копии")


async def handle_season(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/season <название> — создать сезон фэнтези.

    Через команду, а не кнопкой: в попапе Telegram нет поля ввода, а название
    сезону нужно осмысленное — их может идти несколько параллельно."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private" or not _is_admin(user):
        return
    name = " ".join(context.args or []).strip()
    if not name:
        await update.message.reply_text(
            "Укажи название: /season Осень 2026\n\n"
            "Дальше турниры и команды настраиваются в приложении (вкладка ⚙️).")
        return
    import fantasy
    season = fantasy.start_season(name)
    await update.message.reply_text(
        f"✅ Сезон «{season['name']}» создан.\n\n"
        "Открой приложение → ⚙️ и выбери турниры в зачёте: пока их нет, "
        "очки не считаются.")


async def handle_my_profile(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/profile — какие профили привязаны и что по ним видно.

    Пока функция скрытая: личная статистика открыта только админу, пока не
    решён вопрос с оплатой и согласием игроков (бэклог п.4)."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private" or not _can_see_personal(user):
        return
    await _send_profile(update.message, user)


async def _send_profile(message, user) -> None:
    """Личный прогресс: и по команде /profile, и по кнопке в админке."""
    import player_identity
    ids = player_identity.get_identities(user.id)
    if not ids:
        await message.reply_text(
            "У тебя пока не привязан ни один профиль.\n\n"
            "Пришли мне ссылку на свою страницу в лиге — например:\n"
            "• https://slpro.basketstat.ru/player/XXXX\n"
            "• https://www.fbp.ru/player.html?personId=XXXXXX&apiUrl=https://reg.infobasket.su\n\n"
            "Я запомню твой номер в лиге и буду показывать личный прогресс.")
        return
    import personal_report
    prefs = personal_report.get_prefs(user.id)
    parts = ["📊 Твой прогресс", ""]
    for rec in ids:
        title = player_identity.SOURCE_TITLES.get(rec["source"], rec["source"])
        data = personal_report.compare(rec["source"], rec["player_id"],
                                       mode=prefs["compare_mode"],
                                       since=prefs["compare_since"],
                                       metrics=personal_report.metrics_of(prefs))
        parts.append(personal_report.format_report(title, data, prefs["compare_mode"]))
        parts.append("")
    await message.reply_text("\n".join(parts).strip(),
                             reply_markup=_report_prefs_markup(prefs))


def _report_prefs_markup(prefs: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Настройки личного отчёта: с чем сравнивать и как часто присылать."""
    import personal_report
    rows = [[InlineKeyboardButton("⚙️ Сравнивать с периодом:", callback_data="rep:noop")]]
    row = []
    for key, title in personal_report.COMPARE_MODES.items():
        if key == "since":
            continue          # произвольная дата — отдельным вводом, не кнопкой
        mark = "✅ " if prefs["compare_mode"] == key else ""
        row.append(InlineKeyboardButton(f"{mark}{title}", callback_data=f"rep:cmp:{key}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("🔍 Подробный разбор", callback_data="rep:deep"),
                 InlineKeyboardButton("📌 Показатели", callback_data="rep:mets")])
    rows.append([InlineKeyboardButton("🎬 Я в записи", callback_data="rep:vid"),
                 InlineKeyboardButton("📄 Файл за месяц", callback_data="rep:file")])
    rows.append([InlineKeyboardButton("🔔 Присылать отчёт:", callback_data="rep:noop")])
    row = []
    for key, title in personal_report.NOTIFY_MODES.items():
        mark = "✅ " if prefs["notify_mode"] == key else ""
        row.append(InlineKeyboardButton(f"{mark}{title}", callback_data=f"rep:ntf:{key}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    return InlineKeyboardMarkup(rows)


async def _send_month_file(query, uid: Any) -> None:
    """Файл по кнопке — не дожидаясь расписания. Один файл на ВСЕ лиги игрока."""
    import player_identity
    import monthly_report
    profiles = [(r["source"], r["player_id"]) for r in player_identity.get_identities(uid)]
    if not profiles:
        await query.answer("Сначала пришли ссылку на свой профиль в лиге", show_alert=True)
        return
    await query.answer("Собираю файл…")
    today = datetime.now()
    year, month = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    try:
        html_doc = await asyncio.to_thread(
            monthly_report.build_combined, profiles, year, month, None, uid)
    except Exception as e:
        log.warning(f"месячный файл для {uid}: {e}")
        html_doc = None
    if not html_doc:
        # Прошлый месяц пуст — пробуем текущий, иначе человек получит пустоту.
        try:
            html_doc = await asyncio.to_thread(
                monthly_report.build_combined, profiles, today.year, today.month,
                None, uid)
            year, month = today.year, today.month
        except Exception:
            html_doc = None
    if not html_doc:
        await query.message.reply_text("За последние месяцы игр не нашлось — "
                                       "файл будет, когда появятся протоколы.")
        return
    bio = io.BytesIO(html_doc.encode("utf-8"))
    bio.name = f"otchet_{year}-{month:02d}.html"
    await query.message.reply_document(
        document=bio, filename=bio.name,
        caption="📊 Отчёт за месяц по всем твоим лигам. Внизу файла — готовый "
                "запрос для ИИ, если захочешь разбор поглубже.")


def _metrics_markup(prefs: Dict[str, Any]) -> InlineKeyboardMarkup:
    """Какие показатели отслеживать. Одному важны подборы, другому фолы —
    общий набор для всех превращает отчёт в простыню."""
    import personal_report
    chosen = {k for k, _, _ in personal_report.metrics_of(prefs)}
    rows, row = [], []
    for key, title, _ in personal_report.ALL_METRICS:
        row.append(InlineKeyboardButton(("✅ " if key in chosen else "⬜️ ") + title,
                                        callback_data=f"rep:met:{key}"))
        if len(row) == 2:
            rows.append(row); row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("✅ Выбрать все", callback_data="rep:allmet:on"),
                 InlineKeyboardButton("⬜️ Снять все", callback_data="rep:allmet:off")])
    rows.append([InlineKeyboardButton("⬅️ К отчёту", callback_data="rep:back")])
    return InlineKeyboardMarkup(rows)


def _deep_text(user_id: Any) -> str:
    """Подробный разбор: соперники, роль в команде, броски."""
    import personal_report
    import player_identity
    out: List[str] = ["🔍 Подробный разбор", ""]
    for rec in player_identity.get_identities(user_id):
        src, pid = rec["source"], rec["player_id"]
        title = player_identity.SOURCE_TITLES.get(src, src)
        out.append(f"— {title} —")

        role = personal_report.team_role(src, pid)
        if role:
            out.append(f"Твоя доля в команде за {role['games']} игр: "
                       f"{role['pts_share']}% очков, {role['reb_share']}% подборов")

        sh = personal_report.shooting(src, pid)
        if sh:
            out.append("Броски (форма против остального):")
            for label, v in sh.items():
                was = f"{v['was']}%" if v["was"] is not None else "—"
                out.append(f"   • {label}: {v['now']}% против {was}, "
                           f"{v['per_game']} попыток за игру")

        vs = personal_report.vs_opponents(src, pid)
        if vs:
            out.append("Против тех, с кем играл не раз:")
            for v in vs:
                out.append(f"   • соперник {v['opponent']}: {v['meetings']} встреч, "
                           f"побед {v['wins']}")
                out.append(f"     было {v['prev']['pts']} очк → стало {v['last']['pts']} очк; "
                           f"состав команды совпал на {v['roster_overlap']}%")
        elif not role and not sh:
            out.append("Данных пока мало — разбор появится после нескольких игр.")
        out.append("")
    return "\n".join(out).strip() or "Профиль не привязан."


async def handle_report_prefs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки настроек личного отчёта. Настройки персональные, поэтому никакой
    проверки на админа — но и чужие не тронуть: пишем по id нажавшего."""
    query = update.callback_query
    if not query or not query.from_user:
        return
    await query.answer()
    import personal_report
    import player_identity
    parts = (query.data or "").split(":")
    uid = query.from_user.id
    if len(parts) >= 2 and parts[1] == "vid":
        text, markup = await asyncio.to_thread(_my_games_video, uid)
        await query.edit_message_text(text, reply_markup=markup)
        return
    if len(parts) >= 5 and parts[1] == "vidt":
        _awaiting_video[uid] = f"rep:{parts[2]}:{parts[3]}:{parts[4]}"
        await query.edit_message_text(VIDTIME_ASK)
        return
    if len(parts) >= 5 and parts[1] == "vidg":
        text, markup = await asyncio.to_thread(_my_video_game, parts[2], parts[3], parts[4])
        await query.edit_message_text(text, reply_markup=markup, parse_mode="HTML",
                                      disable_web_page_preview=True)
        return
    if len(parts) >= 2 and parts[1] == "deep":
        await query.edit_message_text(
            _deep_text(uid),
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ К отчёту", callback_data="rep:back")]]))
        return
    if len(parts) >= 3 and parts[1] == "allmet":
        # «Снять все» оставляет один показатель: пустой отчёт бессмыслен.
        keys = ([k for k, _, _ in personal_report.ALL_METRICS] if parts[2] == "on"
                else [personal_report.DEFAULT_METRICS[0]])
        prefs = personal_report.set_pref(uid, "metrics", ",".join(keys))
        await query.edit_message_reply_markup(reply_markup=_metrics_markup(prefs))
        return
    if len(parts) >= 2 and parts[1] == "file":
        await _send_month_file(query, uid)
        return
    if len(parts) >= 2 and parts[1] == "mets":
        await query.edit_message_reply_markup(
            reply_markup=_metrics_markup(personal_report.get_prefs(uid)))
        return
    if len(parts) >= 3 and parts[1] == "met":
        prefs = personal_report.get_prefs(uid)
        chosen = [k for k, _, _ in personal_report.metrics_of(prefs)]
        key = parts[2]
        # Последний показатель снять нельзя: пустой отчёт бессмыслен.
        if key in chosen and len(chosen) > 1:
            chosen.remove(key)
        elif key not in chosen:
            chosen.append(key)
        prefs = personal_report.set_pref(uid, "metrics", ",".join(chosen))
        await query.edit_message_reply_markup(reply_markup=_metrics_markup(prefs))
        return
    if len(parts) >= 2 and parts[1] == "back":
        prefs = personal_report.get_prefs(uid)
    else:
        if len(parts) < 3:
            return
        field = {"cmp": "compare_mode", "ntf": "notify_mode"}.get(parts[1])
        if not field:
            return
        prefs = personal_report.set_pref(uid, field, parts[2])

    ids = player_identity.get_identities(query.from_user.id)
    out = ["📊 Твой прогресс", ""]
    for rec in ids:
        title = player_identity.SOURCE_TITLES.get(rec["source"], rec["source"])
        data = personal_report.compare(rec["source"], rec["player_id"],
                                       mode=prefs["compare_mode"],
                                       since=prefs["compare_since"],
                                       metrics=personal_report.metrics_of(prefs))
        out.append(personal_report.format_report(title, data, prefs["compare_mode"]))
        out.append("")
    try:
        await query.edit_message_text("\n".join(out).strip(),
                                      reply_markup=_report_prefs_markup(prefs))
    except Exception:
        pass          # текст не изменился — Telegram ругается, это не ошибка


async def handle_fantasy_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Приём состава из Mini App (Telegram sendData). Валидируем на сервере и
    сохраняем — клиенту не доверяем."""
    msg = update.effective_message
    user = update.effective_user
    if not msg or not msg.web_app_data or not user:
        return
    import fantasy
    import fantasy_api
    import fantasy_modes
    try:
        payload = json.loads(msg.web_app_data.data)
        refs = payload.get("refs") or []
        raw_mode = payload.get("mode")
        cats = payload.get("cats") or []
    except (json.JSONDecodeError, TypeError, AttributeError):
        await msg.reply_text("⚠️ Не удалось прочитать состав.")
        return

    uid = str(user.id)

    season = fantasy.get_active_season()
    if not season:
        await msg.reply_text("Сейчас нет активного сезона.")
        return
    if not fantasy_api._can_view({"id": user.id, "username": user.username or ""}, season):
        await msg.reply_text("Эта фэнтези-лига пока только для игроков команды.")
        return
    week_start, sched_locked = fantasy.active_selection(season)
    if sched_locked:
        det = fantasy.lock_details()
        since = f" (с {det['started_hhmm']})" if det.get("started_hhmm") else ""
        await msg.reply_text(
            f"🔒 Сейчас идёт игра{since} — состав заморожен.\n\n"
            "Менять его можно будет сразу после того, как бот пришлёт результат.")
        return
    # Режим приходит из приложения — и его НЕЛЬЗЯ терять. Раньше запасной вход
    # сохранял состав без режима, то есть молча переводил человека в
    # «свободный»: его очки уезжали в чужую таблицу, а он об этом не знал.
    mode = fantasy_modes.normalize(season, raw_mode)
    meta = {"cats": list(cats)} if mode == fantasy_modes.CATEGORY else {}
    prices = None
    try:
        all_pool = await fantasy_api.build_pool(season=season)
        pool_refs = {p["ref"] for p in all_pool}
        prices = {p["ref"]: p.get("price", 0)
                  for p in fantasy_api._pool_with_stats(all_pool, season)}
    except Exception:
        pool_refs = None  # пул недоступен — не заваливаем сохранение из-за этого
    err = fantasy.validate_roster(season, refs, pool_refs or None,
                                  mode=mode, meta=meta, prices=prices)
    if err:
        size = fantasy_modes.roster_size(season, mode)
        problems = {
            "invalid_roster": f"Нужно выбрать ровно {size} игроков.",
            "unknown_player": "В составе есть игрок не из пула. Открой заново.",
            "too_many_copies": f"Одного игрока можно взять не больше "
                               f"{fantasy.max_per_player(season)} раз(а).",
            "over_budget": f"Состав дороже бюджета "
                           f"({fantasy_modes.cost(refs, prices)} из "
                           f"{fantasy_modes.settings(season)['budget']}).",
            "no_price": "У кого-то из выбранных нет цены — напиши админу.",
            "bad_categories": "Нужно закрыть каждую категорию.",
            "duplicate_player": "Один игрок может занимать только одну категорию.",
        }
        await msg.reply_text(problems.get(err, "Состав не прошёл проверку."))
        return

    res = fantasy.save_roster(uid, season["id"], week_start, refs, mode=mode, meta=meta)
    if not res.get("ok"):
        reason = "набор на этот тур уже закрыт" if res.get("error") == "locked" else "не удалось сохранить"
        await msg.reply_text(f"⚠️ {reason.capitalize()}.")
        return
    title = fantasy_modes.MODE_TITLES.get(mode, mode)
    await msg.reply_text(f"✅ Состав сохранён (режим «{title}»)! Удачи в туре 🏀")


# Конфигурация кнопок "Запуск оповещений". "daily" (Оповещения на сегодня)
# обрабатывается отдельно ниже — это последовательный запуск первых трёх.
LAUNCH_ACTIONS = {
    "birthday": {
        "label": "🎂 ДР",
        "script": "run_birthday_notifications.py",
        "args": [],
        "data_types": ["ДЕНЬ_РОЖДЕНИЯ"],
    },
    "training_polls": {
        "label": "📋 Опросы тренировок",
        "script": "training_polls_enhanced.py",
        "args": [],
        "data_types": ["ОПРОС_ГОЛОСОВАНИЕ"],
    },
    "game_polls": {
        "label": "🏀 Опросы игры",
        "script": "run_game_system.py",
        "args": ["--only", "polls"],
        "data_types": ["ОПРОС_ИГРА"],
    },
    "game_announce": {
        "label": "📢 Анонс игры",
        "script": "run_game_system.py",
        "args": ["--only", "announcements"],
        "data_types": ["АНОНС_ИГРА"],
    },
    "slpro": {
        "label": "🏀 SLPRO (Farm)",
        "script": "run_slpro_monitor.py",
        "args": [],
        "data_types": ["ОПРОС_ИГРА_SLPRO", "АНОНС_ИГРА_SLPRO", "РЕЗУЛЬТАТ_ИГРА_SLPRO"],
    },
}
DAILY_DATA_TYPES = [
    "ДЕНЬ_РОЖДЕНИЯ", "ОПРОС_ГОЛОСОВАНИЕ", "ОПРОС_ИГРА", "АНОНС_ИГРА",
    "ОПРОС_ИГРА_SLPRO", "АНОНС_ИГРА_SLPRO", "РЕЗУЛЬТАТ_ИГРА_SLPRO",
]
DAILY_SCRIPTS = [
    ("run_birthday_notifications.py", []),
    ("training_polls_enhanced.py", []),
    ("run_game_system.py", []),
    ("run_slpro_monitor.py", []),
]


def _main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Запуск оповещений", callback_data="admin:menu:launch")],
        [InlineKeyboardButton("👥 Список пользователей", callback_data="admin:menu:users")],
        [InlineKeyboardButton("🔗 Опознание игроков", callback_data="admin:link:list")],
        [InlineKeyboardButton("📈 Прогресс команды", callback_data="prog:list")],
        [InlineKeyboardButton("🔑 Доступы", callback_data="admin:acc:list")],
        [InlineKeyboardButton("📋 Лог действий", callback_data="admin:menu:log")],
        [InlineKeyboardButton("📊 Отчёты", callback_data="admin:menu:reports")],
        [InlineKeyboardButton("🏆 Фэнтези лига", callback_data="admin:menu:fantasy")],
        [InlineKeyboardButton("🧾 Что бот прочитал в Конфиге", callback_data="admin:menu:config")],
        [InlineKeyboardButton("🗄 Статистика лиг", callback_data="admin:menu:stats")],
        [InlineKeyboardButton("⏱ Записи игр", callback_data="admin:video:list")],
        [InlineKeyboardButton("🎂 Дни рождения и ники", callback_data="admin:field:list:0")],
        [InlineKeyboardButton("🔌 Каналы связи", callback_data="admin:doors:list")],
        [InlineKeyboardButton("📈 Моя статистика (скрытое)", callback_data="admin:menu:profile")],
        [InlineKeyboardButton("🔄 Синхронизация", callback_data="admin:sync")],
    ])


def _plural(n: int, one: str, few: str, many: str) -> str:
    """Русское склонение числительных: 1 игра, 2 игры, 5 игр."""
    a, b = abs(n) % 100, abs(n) % 10
    if 10 < a < 20:
        return many
    if 1 < b < 5:
        return few
    return one if b == 1 else many


def _stats_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Что у нас есть из протоколов лиг и чего не хватает.

    Плюс-минус и время на площадке появились позже самого бэкфилла, поэтому у
    старых игр их нет: они нужны и карточкам игроков, и «объёмной» аналитике."""
    import stats_backfill
    summary = stats_backfill.local_summary()
    lines = ["🗄 Копия протоколов лиг", ""]
    for src, info in sorted(summary.items()):
        lines.append(f"• {src}: игр {info.get('games', 0)}, строк {info.get('rows', 0)}"
                     + (f", {info.get('first', '')[:7]}–{info.get('last', '')[:7]}"
                        if info.get("first") else ""))
    with sheets_cache.get_connection() as conn:
        stale = conn.execute(
            """SELECT COUNT(*) FROM (SELECT game_id FROM game_player_stats
               WHERE source = 'slpro' GROUP BY game_id
               HAVING MAX(secs) = 0 AND MAX(ABS(plus_minus)) = 0)""").fetchone()[0]
        # Игра без стадии не попадает в зачёт турнира — она хуже, чем просто
        # неполная: её как будто и нет.
        no_stage = conn.execute(
            """SELECT COUNT(DISTINCT game_id) FROM game_player_stats
               WHERE source = 'slpro' AND (stage_id IS NULL OR stage_id = '')"""
        ).fetchone()[0]
    lines += ["", f"Без плюс-минуса и минут: {stale} "
                  f"{_plural(stale, 'игра', 'игры', 'игр')} SLPRO."]
    if no_stage:
        lines.append(f"Без стадии (не считаются в зачёт): {no_stage} "
                     f"{_plural(no_stage, 'игра', 'игры', 'игр')}.")
    rows = []
    if no_stage:
        # Игры без стадии не считаются вообще, поэтому их надо чинить не ночью,
        # а сейчас: их мало, и перекачка занимает минуту.
        rows.append([InlineKeyboardButton(
            f"⬇️ Перекачать без стадии сейчас ({no_stage})",
            callback_data="admin:stats:now")])
    rows.append([InlineKeyboardButton("🔄 Перекачать наши игры (свежим парсером)",
                                      callback_data="admin:stats:ours")])
    if stale or no_stage:
        lines.append("Пометить их — и ночной бэкфилл перекачает протоколы "
                     "порциями по 200 за ночь. Уже скачанное не трогается.")
        rows.append([InlineKeyboardButton(
            f"♻️ Пометить к перекачке ({stale + no_stage})",
            callback_data="admin:stats:refetch")])
    rows.append(_back_button())
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _config_screen_text() -> str:
    """Что бот вычитал из «Конфига» — чтобы админ видел результат разметки, не
    лазая в логи. Лист бот не правит, поэтому единственная обратная связь по
    неудачно поставленному маркеру — этот экран."""
    import config_sheet
    from enhanced_duplicate_protection import duplicate_protection

    rows = sheets_cache.get_config_rows() or []
    if not rows:
        return "🧾 Конфиг\n\nЗеркало листа пустое — нажми «Синхронизация»."
    blocks = config_sheet.split(rows, strict=True)
    cfg = duplicate_protection.get_config_ids()
    lines = [f"🧾 {config_sheet.describe(rows)}", ""]

    lines.append("🎮 GAME — турниры")
    for row in blocks[config_sheet.GAME]:
        cells = [str(c or "").strip() for c in list(row) + [""] * 4]
        lines.append(f"• {cells[0] or '—'} · {cells[1] or '—'} · {cells[2] or '—'}"
                     + (f" · {cells[3]}" if cells[3] else ""))
    if not blocks[config_sheet.GAME]:
        lines.append("• пусто (проверь --- START GAME --- / --- END GAME ---)")
    lines.append(f"  → соревнования: {cfg.get('comp_ids') or '—'}, "
                 f"команды: {cfg.get('team_ids') or '—'}")

    lines.append("")
    lines.append("🗳 VOTING — опросы")
    for poll in duplicate_protection.get_full_config().get("voting_polls") or []:
        opts = ", ".join(o["text"] for o in poll.get("options") or [])
        lines.append(f"• «{poll.get('topic_template') or poll.get('poll_id')}»: {opts or '—'}")
        lines.append(f"  дни: {poll.get('weekdays') or '—'}, топик: {poll.get('topic_id') or '—'}")
    if not blocks[config_sheet.VOTING]:
        lines.append("• пусто (проверь --- START VOTING --- / --- END VOTING ---)")

    lines.append("")
    lines.append("⚙️ AUTOMATIONS — автосообщения")
    for key, entry in sorted((duplicate_protection.get_config_ids()
                              .get("automation_topics") or {}).items()):
        lines.append(f"• {entry.get('name') or key}: топик {entry.get('topic_id') or '—'}, "
                     f"время {entry.get('notify_time') or '—'}")
    if not blocks[config_sheet.AUTOMATIONS]:
        lines.append("• маркеры стоят не вокруг строк автосообщений — "
                     "бот прочитал секцию по заголовку")
    return "\n".join(lines)[:4000]


def _fantasy_menu_markup() -> InlineKeyboardMarkup:
    import fantasy
    seasons = fantasy.active_seasons()
    season = seasons[0] if seasons else None
    rows: List[List[InlineKeyboardButton]] = []
    # «Старт» доступен всегда — можно вести несколько параллельных лиг.
    rows.append([InlineKeyboardButton("▶️ Старт сезона (+лига)", callback_data="admin:fantasy:start")])
    if season:
        fmt = season.get("format", "3x3")
        other = "5x5" if str(fmt).startswith("3") else "3x3"
        rows.append([InlineKeyboardButton(f"🔀 Формат: {fmt} → {other}", callback_data="admin:fantasy:format")])
        rows.append([InlineKeyboardButton("👥 Составы в пуле", callback_data="admin:fantasy:pool")])
        rows.append([InlineKeyboardButton("🎯 Турнир подсчёта", callback_data="admin:fantasy:scope")])
        rows.append([InlineKeyboardButton("📥 Пересчитать статистику", callback_data="admin:fantasy:ingest")])
        # Цены двигаются сами после каждой игры. Эти две кнопки — на случай,
        # когда нужно посмотреть или применить прямо сейчас: в чат-панели их
        # не было, и тренер искал их именно здесь, а не в приложении.
        rows.append([InlineKeyboardButton("👀 Показать пересчёт цен",
                                          callback_data="admin:fantasy:pricesdry")])
        rows.append([InlineKeyboardButton("💰 Пересчитать цены",
                                          callback_data="admin:fantasy:prices")])
        end_label = "🏁 Завершить лигу…" if len(seasons) > 1 else "🏁 Завершить сезон"
        rows.append([InlineKeyboardButton(end_label, callback_data="admin:fantasy:end")])
    rows.append(_back_button())
    return InlineKeyboardMarkup(rows)


def _fantasy_menu_text() -> str:
    import fantasy
    seasons = fantasy.active_seasons()
    if not seasons:
        return "🏆 Фэнтези лига\n\nАктивной лиги нет."
    head = "🏆 Фэнтези лига\n"
    if len(seasons) > 1:
        head += f"\nАктивных лиг: {len(seasons)}\n"
    lines = []
    for s in seasons:
        lines.append(f"• «{s['name']}» · {s.get('format', '3x3')} · "
                     f"{fantasy.scopes_title(fantasy.season_scopes(s))}")
    tail = ("\n\nКнопки формата/пула/турнира действуют на последнюю лигу."
            if len(seasons) > 1 else "")
    return head + "\n".join(lines) + tail


async def _fantasy_scope_markup() -> InlineKeyboardMarkup:
    """Мультивыбор турниров подсчёта: стадии SLPRO + сезоны Инфобаскета (comp_id
    из Конфига). Выбранные помечены ✅. Первой — авто-настройка по поиску игр."""
    import fantasy
    season = fantasy.get_active_season()
    scopes = fantasy.season_scopes(season) if season else []
    rows: List[List[InlineKeyboardButton]] = [
        [InlineKeyboardButton("🎯 По настройкам поиска игр", callback_data="admin:fscope:auto")],
    ]
    try:
        from slpro_client import SlproClient
        stages = await SlproClient().iter_stages()
    except Exception as e:
        log.warning(f"Не удалось получить стадии SLPRO: {e}")
        stages = []
    for s in stages[:12]:   # активные первыми; ограничим, чтобы меню не разрослось
        division = s.get("division_name") or s.get("division") or "?"
        sc = {"source": "slpro", "season_id": str(s["season_id"]), "stage_id": str(s["stage_id"])}
        mark = "✅" if fantasy.scope_in(sc, scopes) else ("🟢" if s.get("active") else "⚪")
        label = f"{mark} SLPRO {s.get('season')} · {division}"
        rows.append([InlineKeyboardButton(
            label[:64], callback_data=f"admin:fscope:slpro:{s['season_id']}:{s['stage_id']}")])
    for comp in _config_comp_ids():
        sc = {"source": "infobasket", "season_id": str(comp)}
        mark = "✅" if fantasy.scope_in(sc, scopes) else "⚪"
        rows.append([InlineKeyboardButton(f"{mark} Инфобаскет · comp {comp}",
                                          callback_data=f"admin:fscope:ib:{comp}")])
    rows.append([InlineKeyboardButton("🧹 Очистить (все турниры)", callback_data="admin:fscope:clear")])
    rows.append(_back_button("admin:menu:fantasy"))
    return InlineKeyboardMarkup(rows)


def _config_comp_ids() -> List[int]:
    """comp_id Инфобаскета из Конфига — те же лиги, что использует поиск игр."""
    try:
        from enhanced_duplicate_protection import duplicate_protection
        return [int(c) for c in (duplicate_protection.get_config_ids().get("comp_ids") or [])
                if str(c).isdigit()]
    except Exception as e:
        log.warning(f"Не удалось прочитать comp_ids из Конфига: {e}")
        return []


async def _derive_scopes() -> List[Dict[str, Any]]:
    """Собирает турниры подсчёта из настроек поиска игр: активная стадия SLPRO
    нашей команды + comp_id Инфобаскета из Конфига. Названия — транзитно."""
    scopes: List[Dict[str, Any]] = []
    try:
        import slpro_client
        for ctx in await slpro_client.team_contexts():
            if ctx.get("stage_id") is not None:
                scopes.append(slpro_client.scope_of(ctx))
    except Exception as e:
        log.warning(f"derive SLPRO scope: {e}")
    for comp in _config_comp_ids():
        name = f"Инфобаскет comp {comp}"
        try:
            import stats_backfill
            async with stats_backfill._ib_session() as sess:
                cal = await stats_backfill._ib_calendar(sess, comp)
            comp_name = (cal[0].get("CompNameRu") if cal else "") or ""
            if comp_name:
                name = f"Инфобаскет · {comp_name}"
        except Exception as e:
            log.warning(f"derive comp {comp} name: {e}")
        scopes.append({"source": "infobasket", "season_id": str(comp), "name": name})
    return scopes


async def _fantasy_pool_markup() -> InlineKeyboardMarkup:
    """Тумблеры команд, чьи ростеры входят в пул. Пусто в настройках = все
    кандидаты включены (дефолт)."""
    import fantasy
    import fantasy_api
    season = fantasy.get_active_season()
    explicit = fantasy.pool_teams(season) if season else []
    candidates = await fantasy_api.derive_pool_teams()
    rows: List[List[InlineKeyboardButton]] = []
    for t in candidates:
        on = (not explicit) or fantasy.team_in_pool(t, explicit)
        mark = "✅" if on else "⬜"
        src = "SLPRO" if t.get("source") == "slpro" else "Инфобаскет"
        label = f"{mark} {src}: {t.get('name', '')} (id {t.get('team_id')})"
        rows.append([InlineKeyboardButton(
            label[:64], callback_data=f"admin:fpool:{t.get('source')}:{t.get('team_id')}")])
    if not candidates:
        rows.append([InlineKeyboardButton("⚠️ Команды не найдены в поиске игр", callback_data="admin:menu:fantasy")])
    rows.append(_back_button("admin:menu:fantasy"))
    return InlineKeyboardMarkup(rows)


async def _handle_fantasy_pool(query, parts: List[str]) -> None:
    """Тумблер команды в пуле. Из пустого (дефолт=все) при первом выключении
    материализуем полный список кандидатов, затем убираем выбранную."""
    import fantasy
    import fantasy_api
    if not fantasy.get_active_season():
        await query.edit_message_text("Активного сезона нет.", reply_markup=_fantasy_menu_markup())
        return
    src = parts[2] if len(parts) > 2 else ""
    team_id = parts[3] if len(parts) > 3 else ""
    candidates = await fantasy_api.derive_pool_teams()
    target = next((t for t in candidates
                   if str(t.get("source")) == src and str(t.get("team_id")) == team_id), None)
    if target:
        if not fantasy.pool_teams(fantasy.get_active_season()):
            fantasy.set_pool_teams(candidates)  # материализуем дефолт «все»
        fantasy.toggle_pool_team(target)
        fantasy_api._pool_cache.clear()   # пул считается на сезон — сбрасываем весь кеш
    await query.edit_message_text(
        "👥 Чьи ростеры в пуле фэнтези?\n\nОтмечай команды — их игроков можно "
        "будет ставить в состав. ✅ — в пуле.",
        reply_markup=await _fantasy_pool_markup())


async def _handle_fantasy_scope(query, parts: List[str]) -> None:
    """Мультивыбор турниров подсчёта. Тумблеры ✅/⬜; auto — по поиску игр;
    clear — считать всё. Имя турнира восстанавливаем по id (в callback_data
    только 64 байта)."""
    import fantasy
    kind = parts[2] if len(parts) > 2 else ""
    if not fantasy.get_active_season():
        await query.edit_message_text("Активного сезона нет.", reply_markup=_fantasy_menu_markup())
        return

    if kind == "clear":
        fantasy.set_season_scopes([])
    elif kind == "auto":
        await query.edit_message_text("⏳ Собираю турниры по настройкам поиска игр…")
        fantasy.set_season_scopes(await _derive_scopes())
    elif kind == "slpro" and len(parts) > 4:
        season_id, stage_id = parts[3], parts[4]
        name = f"SLPRO сезон {season_id}"
        try:
            from slpro_client import SlproClient
            for s in await SlproClient().iter_stages():
                if str(s["season_id"]) == season_id and str(s["stage_id"]) == stage_id:
                    name = f"SLPRO {s.get('season')} · {s.get('division_name') or s.get('division')}"
                    break
        except Exception:
            pass
        fantasy.toggle_season_scope({"source": "slpro", "season_id": season_id,
                                     "stage_id": stage_id, "name": name})
    elif kind == "ib" and len(parts) > 3:
        comp = parts[3]
        fantasy.toggle_season_scope({"source": "infobasket", "season_id": comp,
                                     "name": f"Инфобаскет comp {comp}"})

    current = fantasy.scopes_title(fantasy.season_scopes(fantasy.get_active_season()))
    await query.edit_message_text(
        "🎯 По каким турнирам считать очки?\n\nМожно выбрать несколько — команда играет "
        "в нескольких лигах. ✅ — выбрано, 🟢 — идёт сейчас.\n\n"
        f"Сейчас: {current}",
        reply_markup=await _fantasy_scope_markup())


def _back_button(target: str = "admin:menu:main") -> List[InlineKeyboardButton]:
    return [InlineKeyboardButton("⬅️ Назад", callback_data=target)]


def _launch_menu_markup() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton("📅 Оповещения на сегодня", callback_data="admin:run:daily")]]
    for key, cfg in LAUNCH_ACTIONS.items():
        rows.append([InlineKeyboardButton(cfg["label"], callback_data=f"admin:run:{key}")])
    rows.append(_back_button())
    return InlineKeyboardMarkup(rows)


def _log_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🤖 Лог бота", callback_data="admin:log:bot")],
        [InlineKeyboardButton("👤 Лог пользователей", callback_data="admin:log:users:0")],
        [InlineKeyboardButton("⚠️ Ошибки", callback_data="admin:log:errors:0")],
        [InlineKeyboardButton("💬 Обратная связь", callback_data="admin:log:feedback:0")],
        _back_button(),
    ])


def _users_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 По таблице", callback_data="admin:users:table:0")],
        [InlineKeyboardButton("🤖 В боте", callback_data="admin:users:bot:0")],
        _back_button(),
    ])


PAGE_SIZE = 8


def _pagination_row(base: str, offset: int, limit: int, total: int) -> List[InlineKeyboardButton]:
    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"{base}:{max(0, offset - limit)}"))
    if offset + limit < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"{base}:{offset + limit}"))
    return nav


def _render_players_page(offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    data = sheets_cache.get_players_page(offset=offset, limit=PAGE_SIZE)
    shown_to = min(data["offset"] + len(data["rows"]), data["total"])
    lines = [f"👥 Игроки по таблице ({data['offset'] + 1}-{shown_to} из {data['total']})", ""]
    for r in data["rows"]:
        name = f"{r['surname']} {r['name']}".strip()
        nick = f" (@{r['nickname']})" if r["nickname"] else ""
        tg = "✅ TG" if r["telegram_id"] else "— без TG"
        lines.append(f"• {name}{nick} — {tg}")
    if not data["rows"]:
        lines.append("Пусто")
    rows = [_pagination_row("admin:users:table", offset, PAGE_SIZE, data["total"])]
    rows.append(_back_button("admin:menu:users"))
    return "\n".join(lines), InlineKeyboardMarkup([r for r in rows if r])


def _render_bot_users_page(offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    data = sheets_cache.get_bot_users_page(offset=offset, limit=PAGE_SIZE)
    shown_to = min(data["offset"] + len(data["rows"]), data["total"])
    lines = [f"🤖 Пользователи в боте ({data['offset'] + 1}-{shown_to} из {data['total']})", ""]
    for r in data["rows"]:
        uname = f"@{r['username']}" if r["username"] else "(без username)"
        try:
            when = datetime.fromisoformat(r["first_seen_at"]).astimezone().strftime("%d.%m.%Y %H:%M")
        except ValueError:
            when = r["first_seen_at"]
        lines.append(f"• {r['first_name']} {uname} — первый /start {when}")
    if not data["rows"]:
        lines.append("Пока никто не запускал бота через /start")
    rows = [_pagination_row("admin:users:bot", offset, PAGE_SIZE, data["total"])]
    rows.append(_back_button("admin:menu:users"))
    return "\n".join(lines), InlineKeyboardMarkup([r for r in rows if r])


def game_timeline_drop(source: str, game_id: str) -> None:
    """Снять ручную привязку. Обёртка, чтобы не тащить импорт в роутер."""
    import game_timeline
    game_timeline.drop_offset(source, game_id)


# ─── каналы связи (двери к API) ─────────────────────────────────────────────
#
# У приложения две двери к одному серверу: Cloudflare и Tailscale Funnel.
# У части игроков провайдер режет одну из них, и приложение честно перебирает
# обе — но перебор стоит секунд. Выключенную дверь фронт не пробует вовсе.
#
# ВАЖНО: проверка отсюда — СЕРВЕРНАЯ. Она говорит «дверь открыта наружу», но
# ничего не знает про провайдера конкретного игрока: у него может резаться
# ровно та дверь, которая с сервера отвечает мгновенно. Поэтому у людей есть
# своя кнопка «🔌 Проверить связь», а тут — состояние самой инфраструктуры.

async def _probe_door(url: str, timeout: float = 8.0) -> Dict[str, Any]:
    """Стучимся в дверь снаружи: код ответа и время. 401 — здоровый ответ."""
    import time as _t
    import aiohttp
    started = _t.perf_counter()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{url}/fantasy/ping",
                                   timeout=aiohttp.ClientTimeout(total=timeout)) as r:
                await r.read()
                code = r.status
    except Exception as e:
        return {"ok": False, "code": 0, "ms": int((_t.perf_counter() - started) * 1000),
                "why": type(e).__name__}
    return {"ok": code in (200, 401), "code": code,
            "ms": int((_t.perf_counter() - started) * 1000)}


async def _doors_screen() -> Tuple[str, InlineKeyboardMarkup]:
    import fantasy_api
    import sheets_cache
    lines = ["🔌 Каналы связи", "",
             "Двери к одному и тому же серверу. Проверка отсюда, с сервера:", ""]
    rows = []
    for door in await asyncio.to_thread(fantasy_api.doors_state):
        probe = await _probe_door(door["url"])
        state = "включена" if door["enabled"] else "ВЫКЛЮЧЕНА"
        verdict = (f"отвечает ({probe['code']}) за {probe['ms']} мс" if probe["ok"]
                   else f"молчит ({probe.get('why') or probe['code']})")
        lines.append(f"• {door['title']} — {state}, {verdict}")
        lines.append(f"   {door['url']}")
        rows.append([InlineKeyboardButton(
            f"{'🔴 Выключить' if door['enabled'] else '🟢 Включить'} {door['title']}"[:60],
            callback_data=f"admin:doors:toggle:{door['id']}")])
    port = await asyncio.to_thread(_api_port_alive)
    lines += ["", f"Сам API на сервере: {'слушает порт' if port else 'НЕ СЛУШАЕТ'}"]
    lines += ["", "Выключенную дверь приложение не пробует — это экономит "
              "игрокам секунды ожидания. Но помни: с сервера обе двери почти "
              "всегда «в порядке», а режет их провайдер у игрока."]
    rows.append([InlineKeyboardButton("🔄 Проверить снова", callback_data="admin:doors:list")])
    rows.append(_back_button())
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _api_port_alive() -> bool:
    """Слушает ли фэнтези-API свой порт. Самый незаметный вид отказа: бот
    отвечает на кнопки, а приложение у всех мёртво."""
    import socket
    with socket.socket() as sock:
        sock.settimeout(1.5)
        return sock.connect_ex(("127.0.0.1", FANTASY_API_PORT)) == 0


# ─── дни рождения и ники ────────────────────────────────────────────────────
#
# Обе колонки живут в листе «Игроки» и правились только там. День рождения бот
# использует для поздравлений, ник — в шутках и разборах, и чаще всего их
# приходится дописывать по одному: открывать ради этого таблицу с телефона —
# то ещё удовольствие.

# Кто из админов что сейчас правит: id → "строка:поле".
_awaiting_field: Dict[int, str] = {}


def _fields_screen(offset: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    import coach_payments
    people = coach_payments.players()
    page = people[offset:offset + PLAYERS_PER_PAGE]
    shown_to = min(offset + len(page), len(people))
    no_bd = sum(1 for p in people if not p.get("birthday"))
    no_nick = sum(1 for p in people if not p.get("nickname"))
    lines = [f"🎂 Дни рождения и ники ({offset + 1}-{shown_to} из {len(people)})", "",
             f"Без даты рождения: {no_bd} · без ника: {no_nick}", ""]
    rows = []
    for p in page:
        bd = p.get("birthday") or "—"
        nick = p.get("nickname") or "—"
        lines.append(f"• {p['title']}: {bd} · {nick}")
        rows.append([InlineKeyboardButton(f"{p['title']}"[:60],
                                          callback_data=f"admin:field:pick:{p['row']}")])
    nav = _pagination_row("admin:field:list", offset, PLAYERS_PER_PAGE, len(people))
    if nav:
        rows.append(nav)
    rows.append(_back_button())
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _field_card(row: int) -> Tuple[str, InlineKeyboardMarkup]:
    import coach_payments
    p = coach_payments.player_by_row(int(row))
    if not p:
        return "Не нашёл этого игрока в листе.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К списку", callback_data="admin:field:list:0")]])
    lines = [f"👤 {p['title']}", "",
             f"🎂 Дата рождения: {p.get('birthday') or 'не указана'}",
             f"✏️ Ник: {p.get('nickname') or 'не указан'}", "",
             "Что поправить?"]
    return "\n".join(lines), InlineKeyboardMarkup([
        [InlineKeyboardButton("🎂 Дата рождения", callback_data=f"admin:field:set:{row}:bd"),
         InlineKeyboardButton("✏️ Ник", callback_data=f"admin:field:set:{row}:nick")],
        [InlineKeyboardButton("⬅️ К списку", callback_data="admin:field:list:0")]])


def _norm_birthday(text: str) -> Optional[str]:
    """«22.09.2001», «2001-09-22» или «22.09» → ISO для листа.

    Год не обязателен: поздравлять можно и без него, а выдумывать за человека
    возраст — хуже, чем не знать его. Тогда пишем 1900-й, как в остальных
    записях без года."""
    raw = str(text or "").strip().replace("/", ".").replace("-", ".")
    parts = [x for x in raw.split(".") if x]
    if len(parts) not in (2, 3):
        return None
    try:
        nums = [int(x) for x in parts]
    except ValueError:
        return None
    if len(nums) == 3 and nums[0] > 31:          # прислали 2001.09.22
        year, month, day = nums
    else:
        day, month = nums[0], nums[1]
        year = nums[2] if len(nums) == 3 else 1900
    if not (1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


async def handle_field_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Новое значение дня рождения или ника, присланное админом."""
    import sheets_cache
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_field:
        return
    if not _is_admin(user):
        _awaiting_field.pop(user.id, None)
        return
    row, field = _awaiting_field[user.id].split(":", 1)
    value = (msg.text or "").strip()
    if field == "bd":
        value = _norm_birthday(value)
        if not value:
            await msg.reply_text("Не понял дату. Как в паспорте: «22.09.2001» "
                                 "или без года «22.09». Передумал — /start.")
            raise ApplicationHandlerStop
    elif not value:
        await msg.reply_text("Пустой ник не записываю. Передумал — /start.")
        raise ApplicationHandlerStop

    _awaiting_field.pop(user.id, None)
    import coach_payments
    person = await asyncio.to_thread(coach_payments.player_by_row, int(row))
    try:
        import report_common
        book = await asyncio.to_thread(report_common.init_sheets)
        ok = await asyncio.to_thread(sheets_cache.write_player_field, book,
                                     int(row), field, value,
                                     (person or {}).get("title", ""))
    except Exception as e:
        log.warning(f"Правка поля игрока: {e}")
        ok = False
    if not ok:
        await msg.reply_text("Таблица не приняла запись — проверь доступ бота "
                             "к листу «Игроки».")
        raise ApplicationHandlerStop
    text, markup = await asyncio.to_thread(_field_card, int(row))
    await msg.reply_text(f"Записал.\n\n{text}", reply_markup=markup)
    raise ApplicationHandlerStop


# ─── привязка записей ВК ────────────────────────────────────────────────────
#
# Тайм-коды бот считает от начала эфира (ВК говорит, когда включили) и времени
# спорного из протокола. Это работает, но проверить его может только человек с
# записью перед глазами — поэтому здесь ручная поправка. Выставленное руками
# автоматика больше не трогает.

# Кто из админов сейчас присылает время спорного: id → "источник:игра".
_awaiting_video: Dict[int, str] = {}

VIDEO_ASK = ("⏱ Открой запись и найди спорный мяч.\n\n"
             "Пришли его время с плеера: «5:33» или «1:02:15».\n\n"
             "После этого все тайм-коды этой игры отсчитываются от него, и "
             "автоматика их больше не трогает.\n\nПередумал — /start.")

_VIDEO_WAY = {"vk": "по эфиру", "auto": "по расписанию", "hand": "вручную"}


def _video_screen() -> Tuple[str, InlineKeyboardMarkup]:
    import coach_payments
    import game_timeline
    import sheets_cache
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        games = game_timeline.our_games(conn, limit=8)
    back = [[InlineKeyboardButton("⬅️ В админку", callback_data="admin:menu:main")]]
    if not games:
        return ("⏱ Записи игр\n\nНи у одной нашей игры пока нет ссылки на запись.",
                InlineKeyboardMarkup(back))
    lines = ["⏱ Записи игр", "",
             "Где в записи спорный мяч — от него считаются все выходы:", ""]
    rows = []
    for g in games:
        day = coach_payments._human_date(str(g["game_date"]))
        title = f"{g['home_name'] or '—'} — {g['guest_name'] or '—'}"
        if not g["shifts"]:
            state = "разметки нет"
        else:
            off = game_timeline.offset(g["source"], g["game_id"])
            way = _VIDEO_WAY.get(game_timeline.offset_kind(g["source"], g["game_id"]), "?")
            state = f"{game_timeline.hhmmss(off)} ({way})"
        lines.append(f"• {day} · {title} — {state}")
        if g["shifts"]:
            row = [InlineKeyboardButton(
                f"{day} · {title}"[:56],
                callback_data=f"admin:video:set:{g['source']}:{g['game_id']}")]
            if game_timeline.offset_kind(g["source"], g["game_id"]) == "hand":
                row.append(InlineKeyboardButton(
                    "↩︎", callback_data=f"admin:video:auto:{g['source']}:{g['game_id']}"))
            rows.append(row)
    lines += ["", "Нажми на игру, если время не сходится с записью. "
              "↩︎ рядом с игрой — снять ручную привязку и вернуть автоматику."]
    rows += back
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def _rebuild_payments_sheet() -> None:
    """Пересобрать лист «Оплаты» после отмены платежа.

    Сводка считается из базы, но лежит в таблице: без пересборки там осталась
    бы сумма, которой в базе уже нет, и тренер видел бы призрак."""
    import coach_payments
    try:
        import report_common
        book = await asyncio.to_thread(report_common.init_sheets)
        await asyncio.to_thread(coach_payments.build_summary_sheet, book)
    except Exception as e:
        log.warning(f"Лист «Оплаты» не пересобран: {e}")


async def handle_money_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Суммы, которые тренер вводит руками: новый долг или размер взноса."""
    import coach_payments
    import sheets_cache
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_money:
        return
    if not _can_see_reports(user):
        _awaiting_money.pop(user.id, None)
        return
    pending = _awaiting_money[user.id]
    text = (msg.text or "").strip()
    m = re.match(r"^\s*(\d{1,7})\s*(.*)$", text)
    if not m:
        await msg.reply_text("Нужна сумма числом: «500» или «500 мяч». "
                             "Передумал — /start.")
        raise ApplicationHandlerStop
    amount, note = int(m.group(1)), m.group(2).strip()
    _awaiting_money.pop(user.id, None)

    if pending.startswith("sched:"):
        key = pending.split(":", 1)[1]
        import training_dues
        low, high = (training_dues.SCHEDULE[key][2:4] if key in training_dues.SCHEDULE
                     else SCHED_LIMITS.get(key, (0, 31)))
        if not low <= amount <= high:
            _awaiting_money[user.id] = pending
            await msg.reply_text(f"Нужно число от {low} до {high}. "
                                 "Передумал — /start.")
            raise ApplicationHandlerStop
        await asyncio.to_thread(sheets_cache.set_setting, key, amount)
        screen, markup = await asyncio.to_thread(_sched_screen)
        await msg.reply_text(f"Записал.\n\n{screen}", reply_markup=markup,
                             parse_mode="HTML")
        raise ApplicationHandlerStop

    if pending.startswith("debt:"):
        row = int(pending.split(":", 1)[1])
        await asyncio.to_thread(coach_payments.add_debt, row, amount, note, str(user.id))
        who = await asyncio.to_thread(coach_payments.player_by_row, row)
        head = (f"Добавил долг: {(who or {}).get('title', '')} — {amount} ₽"
                + (f" ({note})" if note else ""))
        screen, markup = await asyncio.to_thread(_debts_screen)
        await msg.reply_text(f"{head}\n\n{screen}", reply_markup=markup,
                             parse_mode="HTML")
        raise ApplicationHandlerStop

    _, row_s, field = pending.split(":", 2)
    person = await asyncio.to_thread(coach_payments.player_by_row, int(row_s))
    try:
        import report_common
        book = await asyncio.to_thread(report_common.init_sheets)
        ok = await asyncio.to_thread(sheets_cache.write_player_field, book,
                                     int(row_s), field, amount,
                                     (person or {}).get("title", ""))
    except Exception as e:
        log.warning(f"Правка суммы игрока: {e}")
        ok = False
    if not ok:
        await msg.reply_text("Таблица не приняла запись — проверь доступ бота "
                             "к листу «Игроки».")
        raise ApplicationHandlerStop
    screen, markup = await asyncio.to_thread(_sums_screen, int(row_s))
    await msg.reply_text(f"Записал {amount} ₽.\n\n{screen}", reply_markup=markup)
    raise ApplicationHandlerStop


async def handle_video_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Время спорного мяча: и из админки, и из «🎬 Я в записи» у игрока."""
    import game_timeline
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_video:
        return
    pending = _awaiting_video[user.id]
    from_player = pending.startswith("rep:")
    if not from_player and not _is_admin(user):
        _awaiting_video.pop(user.id, None)
        return
    seconds = game_timeline.parse_offset(msg.text or "")
    if seconds is None:
        await msg.reply_text("Не понял время. Как на плеере: «5:33» или "
                             "«1:02:15». Передумал — /start.")
        raise ApplicationHandlerStop

    _awaiting_video.pop(user.id, None)
    if from_player:
        _, source, game_id, player_id = pending.split(":", 3)
    else:
        source, game_id = pending.split(":", 1)
        player_id = ""
    await asyncio.to_thread(game_timeline.set_offset, source, game_id, seconds,
                            f"hand:{user.id}")
    if from_player:
        # Сразу показываем пересчитанные выходы: правку видно на своих же
        # тайм-кодах, а не «где-то потом».
        text, markup = await asyncio.to_thread(_my_video_game, source, game_id, player_id)
        await msg.reply_text(f"Готово, пересчитал от {game_timeline.hhmmss(seconds)}.\n\n{text}",
                             reply_markup=markup, parse_mode="HTML",
                             disable_web_page_preview=True)
    else:
        text, markup = await asyncio.to_thread(_video_screen)
        await msg.reply_text(
            f"Готово: спорный на {game_timeline.hhmmss(seconds)}.\n\n{text}",
            reply_markup=markup)
    raise ApplicationHandlerStop


# ─── опознание игроков ──────────────────────────────────────────────────────
#
# Доступ к фэнтези и личной статистике держится на связке «числовой Telegram id
# ↔ строка листа». Сама она возникает при /start по совпадению @ника, но ник в
# листе бывает старым, чужим или его нет вовсе — и человек навсегда остаётся
# «не игроком команды». Разобрать такое может только тот, кто знает людей в
# лицо, поэтому здесь список и ручная привязка.

def _name_of(row: Dict[str, Any]) -> str:
    return " ".join(x for x in (row.get("surname"), row.get("name")) if x).strip()


# Цифры в никах стоят вместо букв («an1m3_just» = «anime_just»), и без этой
# замены очевидное родство с «@anime_bratishka» не видно.
_LEET = str.maketrans({"0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t"})


def _norm(text: str) -> str:
    return "".join(ch for ch in (text or "").lower().translate(_LEET) if ch.isalnum())


def _same_start(a: str, b: str, least: int = 4) -> bool:
    """Совпадает ли начало — так ловятся и «@an1m3_just» против
    «@anime_bratishka», и «максон» против «Максима»."""
    a, b = _norm(a), _norm(b)
    if len(a) < least or len(b) < least:
        return False
    common = os.path.commonprefix([a, b])
    return len(common) >= least


def _link_candidates(person: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Свободные строки листа, самые похожие — сверху.

    Похожесть считаем по тому, что вообще известно про человека в Telegram:
    его @ник и имя. Это подсказка, а не решение — выбирает всё равно админ."""
    uname = (person.get("username") or "").lstrip("@").lower()
    first = (person.get("first_name") or "").strip().lower()

    def score(r: Dict[str, Any]) -> int:
        nick = (r.get("telegram_id") or "").lstrip("@").lower()
        name = (r.get("name") or "").lower()
        surname = (r.get("surname") or "").lower()
        if uname and nick == uname:
            return 0                      # ник совпал точно — почти наверняка он
        if first and first in (name, surname):
            return 1
        if uname and nick and (uname in nick or nick in uname):
            return 2
        if uname and nick and _same_start(uname, nick):
            return 3                      # ник сменился, но начало то же
        if first and (_same_start(first, name) or _same_start(first, surname)):
            return 4                      # «максон» -> «Максим»
        if first and (first in name or name in first):
            return 5
        return 6

    rows = sheets_cache.free_player_rows()
    return sorted(rows, key=lambda r: (score(r), _name_of(r)))


def _render_link_list() -> Tuple[str, InlineKeyboardMarkup]:
    # Заодно сверяем: строки листа могли сдвинуться с прошлого раза, и связка
    # уже указывает на соседа по алфавиту.
    drifted = sheets_cache.reconcile_player_links()
    people = sheets_cache.unlinked_bot_users()
    free = sheets_cache.free_player_rows()
    lines = ["🔗 Опознание игроков", ""]
    if drifted:
        lines.append(f"♻️ Строки листа сдвинулись — поправил {len(drifted)} "
                     f"{_plural(len(drifted), 'привязку', 'привязки', 'привязок')}:")
        for d in drifted:
            lines.append(f"   @{d['username'] or d['tg_user_id']}: было «{d['was']}» → "
                         f"стало «{d['now']}»")
        lines.append("")
    if people:
        lines += [f"В боте, но не сопоставлены с листом: {len(people)}.",
                  "Нажми на человека — предложу, кому он может быть.", ""]
    else:
        lines.append("Все, кто нажимал /start, сопоставлены с игроками. ✅")
    rows: List[List[InlineKeyboardButton]] = []
    for p in people[:12]:
        nick = f"@{p['username']}" if p["username"] else "без ника"
        title = f"{p['first_name'] or 'без имени'} · {nick}"
        rows.append([InlineKeyboardButton(title[:64],
                                          callback_data=f"admin:link:pick:{p['telegram_id']}:0")])
    if free:
        lines += ["", f"Строк листа без привязки: {len(free)} — эти игроки в бот "
                      f"ещё не заходили или их не опознали."]
        rows.append([InlineKeyboardButton(f"📋 Кого нет в боте ({len(free)})",
                                          callback_data="admin:link:free:0")])
    linked = sheets_cache.linked_players()
    if linked:
        rows.append([InlineKeyboardButton(f"✅ Привязанные ({len(linked)})",
                                          callback_data="admin:link:linked:0")])
    rows.append(_back_button())
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _render_link_pick(uid: str, offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    person = next((p for p in sheets_cache.unlinked_bot_users()
                   if str(p["telegram_id"]) == str(uid)), None)
    if not person:
        return "Этот человек уже привязан или пропал из списка.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К списку", callback_data="admin:link:list")]])
    nick = f"@{person['username']}" if person["username"] else "без ника"
    cand = _link_candidates(person)
    page = cand[offset:offset + PAGE_SIZE]
    lines = [f"🔗 Кто это: {person['first_name'] or '—'} · {nick}",
             f"id {person['telegram_id']}", "",
             "Выбери строку листа «Игроки». Сверху — самые похожие.",
             f"Свободных строк: {len(cand)}", ""]
    rows = [[InlineKeyboardButton(
        (f"{_name_of(r)}" + (f" · {r['telegram_id']}" if r["telegram_id"] else ""))[:64],
        callback_data=f"admin:link:do:{uid}:{r['row_index']}")] for r in page]
    nav = _pagination_row(f"admin:link:pick:{uid}", offset, PAGE_SIZE, len(cand))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="admin:link:list")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _render_link_free(offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    free = sheets_cache.free_player_rows()
    page = free[offset:offset + PAGE_SIZE]
    shown_to = min(offset + len(page), len(free))
    lines = [f"📋 В листе без привязки ({offset + 1}-{shown_to} из {len(free)})", "",
             "Этим людям достаточно нажать /start в боте: если ник в листе совпадёт "
             "с их @, бот опознает их сам. Если ника нет или он другой — вернись "
             "сюда и привяжи руками.", ""]
    for r in page:
        nick = r["telegram_id"] or "— ника нет —"
        lines.append(f"• {_name_of(r)} — {nick}")
    rows = [_pagination_row("admin:link:free", offset, PAGE_SIZE, len(free))]
    rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="admin:link:list")])
    return "\n".join(lines), InlineKeyboardMarkup([r for r in rows if r])


def _render_link_linked(offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    links = sheets_cache.linked_players()
    page = links[offset:offset + PAGE_SIZE]
    shown_to = min(offset + len(page), len(links))
    lines = [f"✅ Привязанные ({offset + 1}-{shown_to} из {len(links)})", "",
             "Нажми, чтобы снять привязку — например если опознали не того.", ""]
    rows = []
    for l in page:
        who = _name_of(l) or f"строка {l['player_row']}"
        nick = f"@{l['username']}" if l["username"] else f"id {l['tg_user_id']}"
        lines.append(f"• {who} — {nick}")
        rows.append([InlineKeyboardButton(f"✂️ {who} · {nick}"[:64],
                                          callback_data=f"admin:link:un:{l['tg_user_id']}")])
    if not page:
        lines.append("Пока никто не привязан.")
    nav = _pagination_row("admin:link:linked", offset, PAGE_SIZE, len(links))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="admin:link:list")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _render_unlink_confirm(uid: str) -> Tuple[str, InlineKeyboardMarkup]:
    link = next((l for l in sheets_cache.linked_players()
                 if str(l["tg_user_id"]) == str(uid)), None)
    if not link:
        return "Привязки уже нет.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К списку", callback_data="admin:link:list")]])
    who = _name_of(link) or f"строка {link['player_row']}"
    nick = f"@{link['username']}" if link["username"] else f"id {uid}"
    text = (f"✂️ Отвязать {who} от {nick}?\n\n"
            "У человека сразу пропадёт доступ к фэнтези и личной статистике. "
            "Уже набранные очки останутся — они привязаны к его id, а не к строке. "
            "Привязать заново можно тут же.")
    return text, InlineKeyboardMarkup([
        [InlineKeyboardButton("✂️ Да, отвязать", callback_data=f"admin:link:un2:{uid}")],
        [InlineKeyboardButton("Отмена", callback_data="admin:link:linked:0")]])


# ─── прогресс команды (для тренера) ─────────────────────────────────────────

async def _prog_teams() -> List[Dict[str, Any]]:
    """Наши команды с названиями — из локального справочника лиг."""
    import fantasy_api
    try:
        teams = await asyncio.to_thread(fantasy_api._resolve_pool_teams_local)
    except Exception as e:
        log.warning(f"Прогресс: не удалось получить команды: {e}")
        teams = []
    out = []
    for t in teams:
        tid, src = str(t.get("team_id") or ""), str(t.get("source") or "")
        if tid and src:
            out.append({"team_id": tid, "source": src,
                        "name": t.get("name") or t.get("team_name") or f"{src}:{tid}"})
    return out


async def _prog_names() -> Dict[str, str]:
    """id игрока -> ФИО из реестра в памяти. ФИО на диск не пишем, в сеть за
    ними тут не ходим: наполняет реестр качалка (league_sync), фоном."""
    import player_names
    names = player_names.by_player_id()
    if not names:
        log.info("Прогресс: реестр имён пуст (качалка ещё не отработала) — "
                 "в отчёте будут номера")
    return names


async def _prog_body() -> Tuple[List[str], List[List[InlineKeyboardButton]]]:
    teams = await _prog_teams()
    lines = ["📈 Прогресс команды", "",
             "Разбор последней игры: что пошло не как обычно и кто в этом "
             "участвовал. Сравнение — с собственными последними играми, "
             "а не с абстрактной нормой.", ""]
    rows = []
    if not teams:
        lines.append("Не нашёл наших команд. Проверь блок SLPRO/Инфобаскет в «Конфиге».")
    for t in teams:
        rows.append([InlineKeyboardButton(
            f"🏀 {t['name']}"[:64],
            callback_data=f"prog:team:{t['source']}:{t['team_id']}")])
    return lines, rows


async def _render_prog_list(is_admin: bool = False,
                            back: str = "admin:menu:main") -> Tuple[str, InlineKeyboardMarkup]:
    lines, rows = await _prog_body()
    if is_admin:
        lines += ["", "Кому открыт этот раздел — в «🔑 Доступы» главного меню."]
    rows.append(_back_button(back))
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def _prog_standings(source: str) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Турнирная таблица лиги и имена команд.

    Имена берём отсюда, а не из своей базы: у нас они появляются только у
    перекачанных игр, а лига знает их всегда — и соперники в отчёте перестают
    быть номерами."""
    if source != "slpro":
        return [], {}
    try:
        import slpro_client
        teams = slpro_client.config_team_names() or slpro_client.env_team_names()
        ctxs = await slpro_client.team_contexts(teams)
        if not ctxs:
            return [], {}
        rows = await slpro_client.SlproClient().get_standings(ctxs[0])
        return rows, {str(r.get("team_id")): r.get("name") or "" for r in rows}
    except Exception as e:
        log.warning(f"Таблица лиги недоступна: {e}")
        return [], {}


async def _prog_opp_names(source: str, opp_id: str, comp_id: str = "") -> Dict[str, str]:
    """ФИО игроков СОПЕРНИКА — из реестра в памяти, если качалка их принесла.

    Сначала смотрим память. Пусто — один раз сходим в заявку лиги: соперник
    меняется от игры к игре, держать заявки всех команд лиги незачем. Запрос
    прикрыт предохранителем клиента, поэтому недоступная лига стоит отчёту
    миллисекунды, а не минуту."""
    if not opp_id:
        return {}
    import player_names
    src_db = "slpro" if source == "slpro" else "infobasket"
    try:
        if source == "slpro":
            import slpro_client
            roster = await slpro_client.SlproClient().get_roster(int(opp_id))
            names = {str(p.get("player_id")):
                     f"{p.get('surname', '')} {p.get('name', '')}".strip()
                     for p in roster if p.get("player_id") is not None}
        else:
            import stats_backfill
            import run_backfill
            # У Инфобаскета заявка живёт внутри соревнования; comp_id их игр
            # лежит у нас в season_id, остальные берём из Конфига — заявка
            # бывает и в соседнем турнире.
            comps = ([comp_id] if comp_id else []) + [
                str(c) for c in run_backfill._ib_comps() if str(c) != comp_id]
            names = {}
            for c in comps:
                roster = await stats_backfill.fetch_infobasket_roster(opp_id, c)
                if roster:
                    names = {str(p["player_id"]): p["name"]
                             for p in roster if p.get("name")}
                    break
        if names:
            player_names.put_many(src_db, names.items())
            return names
    except Exception as e:
        log.warning(f"Заявка соперника {source}:{opp_id} недоступна: {e}")
    # Лига молчит — отдаём то, что уже знаем про этих игроков.
    known = player_names.get_all()
    return {k.split(":", 1)[1]: v for k, v in known.items() if k.startswith(f"{src_db}:")}


REPORT_FILE_CAPTION = ("Открой файл и листай вниз: сезон, последняя игра, "
                       "динамика, состав с тренировками, лига, лидеры "
                       "соперника. В самом конце — готовый промт для ИИ.")


async def _prog_build(source: str, team_id: str) -> Tuple[str, Dict[str, Any]]:
    """Сводка текстом и данные подробного разбора.

    Отдельно от отправки: один и тот же разбор нужен и по кнопке, и сам собой
    в личку тренеру после игры."""
    import team_progress

    teams = await _prog_teams()
    title = next((t["name"] for t in teams
                  if t["team_id"] == team_id and t["source"] == source), "")
    names = await _prog_names()
    standings, team_names = await _prog_standings(source)
    # Кто соперник, узнаём заранее: в лигу за их заявкой надо сходить ДО сборки
    # отчёта — сам team_progress в сеть не ходит.
    last_opp = await asyncio.to_thread(team_progress.last_opponent, team_id, source)
    opp_names = await _prog_opp_names(source, last_opp.get("team_id", ""),
                                      last_opp.get("season_id", ""))
    rep = await asyncio.to_thread(team_progress.game_report, team_id, source, names)
    detail = await asyncio.to_thread(team_progress.detailed_report,
                                     team_id, source, title, names,
                                     standings, team_names, opp_names)
    return team_progress.short_summary(rep, detail), detail


def _prog_file(detail: Dict[str, Any], team_id: str) -> io.BytesIO:
    import team_report_html
    page = team_report_html.build(detail)
    buf = io.BytesIO(page.encode("utf-8"))
    buf.name = f"razbor-{team_id}-{detail['series'][-1]['game_date']}.html"
    return buf


async def _prog_send(message, source: str, team_id: str) -> None:
    """Короткая сводка в чат + подробный разбор файлом.

    В сообщение помещается «что случилось в игре» на пять строк; сезон,
    тренды и сравнения — это таблицы и графики, в тексте они нечитаемы."""
    summary, detail = await _prog_build(source, team_id)
    await message.reply_text(summary,
                             reply_markup=InlineKeyboardMarkup(
                                 [[InlineKeyboardButton("⬅️ К командам",
                                                        callback_data="prog:list")]]))
    if not detail.get("ok"):
        return
    try:
        buf = await asyncio.to_thread(_prog_file, detail, team_id)
        await message.reply_document(buf, caption=REPORT_FILE_CAPTION)
    except Exception as e:
        log.error(f"HTML-разбор не собрался: {e}")
        await message.reply_text("⚠️ Подробный файл собрать не вышло, "
                                 "но сводка выше верная.")


async def _refetch_our_games_now(query) -> None:
    """Перекачка своих игр — после того, как парсер научился новому полю."""
    import stats_backfill
    import slpro_client
    try:
        teams = slpro_client.config_team_names() or slpro_client.env_team_names()
        st = await stats_backfill.refetch_our_games(slpro_client.SlproClient(), teams)
        # Инфобаскет качаем тем же заходом: иначе основа осталась бы без
        # заработанных фолов, а Farm с ними — и цены поехали бы у одних.
        import run_backfill
        ib = await stats_backfill.refetch_our_games_ib(
            run_backfill._ib_comps(), team_ids=[t for t in ("36502",)])
        with sheets_cache.get_connection() as conn:
            rows = conn.execute(
                """SELECT source, COUNT(*) n FROM game_player_stats
                   WHERE foul_on > 0 GROUP BY source""").fetchall()
        got = ", ".join(f"{r['source']}: {r['n']}" for r in rows) or "нет"
        text = (f"✅ Наши игры перекачаны.\n\n"
                f"SLPRO: {st.fetched}, Инфобаскет: {ib.fetched}, "
                f"ошибок: {st.failed + ib.failed}.\n"
                f"Строк с заработанными фолами — {got}.")
    except Exception as e:
        log.error(f"Перекачка наших игр не прошла: {e}")
        text = f"⚠️ Перекачка не прошла: {e}"
    try:
        await query.message.reply_text(text)
    except Exception:
        pass


async def _refetch_no_stage_now(query) -> None:
    """Перекачивает игры без стадии прямо сейчас, не дожидаясь ночного крона.

    Игра без стадии не попадает в зачёт турнира: её как будто нет ни в топе,
    ни в агрегатах. Ждать до 01:30 ради десятка игр — плохой размен, тем более
    что чинить приходится сразу после матча."""
    import stats_backfill
    import slpro_client
    try:
        teams = slpro_client.config_team_names() or slpro_client.env_team_names()
        st = await stats_backfill.refetch_missing_stage(slpro_client.SlproClient(), teams)
        with sheets_cache.get_connection() as conn:
            left = conn.execute(
                """SELECT COUNT(DISTINCT game_id) FROM game_player_stats
                   WHERE source = 'slpro' AND (stage_id IS NULL OR stage_id = '')"""
            ).fetchone()[0]
        text = (f"✅ Перекачка закончена.\n\n"
                f"Скачано: {st.fetched}, ошибок: {st.failed}.\n"
                f"Осталось игр без стадии: {left}.")
        if not left:
            text += "\n\nТоп за последнюю игру и неделю теперь считает все игры."
        elif st.remaining:
            text += (f"\n\n{st.remaining} игр нет в расписании наших турниров — "
                     "видимо, из другого дивизиона.")
    except Exception as e:
        log.error(f"Перекачка без стадии не прошла: {e}")
        text = f"⚠️ Перекачка не прошла: {e}"
    try:
        await query.message.reply_text(text)
    except Exception:
        pass


def _link_row_name(player_row: int) -> str:
    with sheets_cache.get_connection() as conn:
        r = conn.execute("SELECT surname, name FROM players WHERE row_index = ?",
                         (player_row,)).fetchone()
    return f"{r['surname']} {r['name']}".strip() if r else ""


def _do_unlink(uid: str) -> str:
    row = sheets_cache.unlink_player(str(uid))
    if not row:
        return "Привязки уже не было."
    try:
        sheets_cache.write_player_tg_id(_get_spreadsheet(), int(row), "",
                                        _link_row_name(int(row)))
    except Exception as e:
        log.warning(f"Отвязка: не удалось стереть id в листе: {e}")
    log.info(f"Админ снял привязку id {uid} со строки {row}")
    return f"✂️ Привязка снята, строка {row} снова свободна."


def _do_link(uid: str, row_index: int) -> str:
    """Привязывает и возвращает текст ответа админу."""
    people = {str(p["telegram_id"]): p for p in sheets_cache.unlinked_bot_users()}
    person = people.get(str(uid))
    target = next((r for r in sheets_cache.free_player_rows()
                   if int(r["row_index"]) == int(row_index)), None)
    if not person or not target:
        return "Не получилось: человек или строка уже заняты. Открой список заново."
    if not sheets_cache.link_player(str(uid), (person.get("username") or "").lower(), int(row_index)):
        return "Не получилось привязать — строка уже за кем-то закреплена."
    # Числовой id и актуальный ник — в лист: связка должна быть видна и в
    # таблице, а не только у бота. Ник в листе к этому моменту почти всегда
    # старый (из-за него человек и не опознался сам).
    try:
        ss = _get_spreadsheet()
        expect = _name_of(target)
        sheets_cache.write_player_tg_id(ss, int(row_index), str(uid), expect)
        sheets_cache.write_player_nickname(ss, int(row_index),
                                           person.get("username") or "", expect)
    except Exception as e:
        log.warning(f"Привязка: связка не записана в лист: {e}")
    log.info(f"Админ привязал id {uid} к строке {row_index} ({_name_of(target)})")
    return (f"✅ {_name_of(target)} — это {person.get('first_name') or ''} "
            f"(@{person.get('username') or '—'}, id {uid}).\n\n"
            "Теперь у него работает живая статистика и фэнтези.")


def _render_user_log_page(offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    data = sheets_cache.get_user_action_log(offset=offset, limit=10)
    shown_to = min(data["offset"] + len(data["rows"]), data["total"])
    lines = [f"👤 Лог пользователей ({data['offset'] + 1}-{shown_to} из {data['total']})", ""]
    for r in data["rows"]:
        who = f"@{r['username']}" if r["username"] else (r["first_name"] or r["user_id"])
        detail = f" — {r['detail']}" if r["detail"] else ""
        lines.append(f"• [{r['kind']}] {who}{detail} ({r['ts']})")
    if not data["rows"]:
        lines.append("Событий пока нет")
    rows = [_pagination_row("admin:log:users", offset, 10, data["total"])]
    rows.append(_back_button("admin:menu:log"))
    return "\n".join(lines), InlineKeyboardMarkup([r for r in rows if r])


def _render_feedback_page(offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    data = sheets_cache.get_feedback_page(offset=offset, limit=5)
    shown_to = min(data["offset"] + len(data["rows"]), data["total"])
    lines = [f"💬 Обратная связь ({data['offset'] + 1}-{shown_to} из {data['total']})", ""]
    for r in data["rows"]:
        who = f"@{r['username']}" if r["username"] else (r["name"] or f"id {r['user_id']}")
        lines.append(f"№{r['id']} · {who} · {r['logged_at'][:16].replace('T', ' ')}")
        lines.append(r["message"][:500])
        lines.append("")
    if not data["rows"]:
        lines.append("Обращений пока нет. Игроки шлют их командой /feedback.")
    rows = [_pagination_row("admin:log:feedback", offset, 5, data["total"])]
    rows.append(_back_button("admin:menu:log"))
    return "\n".join(lines), InlineKeyboardMarkup([r for r in rows if r])


def _render_errors_page(offset: int) -> Tuple[str, InlineKeyboardMarkup]:
    data = sheets_cache.get_errors_page(offset=offset, limit=PAGE_SIZE)
    shown_to = min(data["offset"] + len(data["rows"]), data["total"])
    lines = [f"⚠️ Ошибки ({data['offset'] + 1}-{shown_to} из {data['total']})", ""]
    for r in data["rows"]:
        lines.append(f"• [{r['source']}] {r['message'][:200]} ({r['logged_at']})")
    if not data["rows"]:
        lines.append("Ошибок не зафиксировано")
    rows = [_pagination_row("admin:log:errors", offset, PAGE_SIZE, data["total"])]
    rows.append(_back_button("admin:menu:log"))
    return "\n".join(lines), InlineKeyboardMarkup([r for r in rows if r])


def _reports_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏋️ Тренировки", callback_data="admin:menu:reports:training")],
        [InlineKeyboardButton("🏀 Игры", callback_data="admin:menu:reports:games")],
        _back_button(),
    ])


def _reports_training_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("За неделю", callback_data="admin:report:training:week")],
        [InlineKeyboardButton("За месяц", callback_data="admin:report:training:month")],
        _back_button("admin:menu:reports"),
    ])


def _reports_games_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("За неделю", callback_data="admin:report:games:week")],
        [InlineKeyboardButton("За месяц", callback_data="admin:report:games:month")],
        _back_button("admin:menu:reports"),
    ])


def _check_already_run_today(data_types: List[str]) -> Optional[str]:
    """Прямая проверка по Сервисному листу (не через 5-минутный кэш —
    сразу после реального запуска кэш ещё не мог обновиться)."""
    today_str = datetime.now().strftime("%d.%m.%Y")
    for dt_ in data_types:
        for record in duplicate_protection.get_records_by_type(dt_):
            if record.get("date", "").startswith(today_str):
                return record["date"]
    return None


def _prices_text(res: Dict[str, Any], dry: bool) -> str:
    """Что сделал (или сделал бы) пересчёт цен."""
    changes = res.get("changes") or []
    head = ("👀 Пересчёт цен — предварительно, в таблицу ничего не записано."
            if dry else f"💰 Цены пересчитаны: строк в листе обновлено {res.get('updated', 0)}.")
    lines = [head, "", f"Проверено игроков: {res.get('checked', 0)}"]
    if res.get("skipped"):
        lines.append(f"Пропущено: {res['skipped']}")
    if not changes:
        lines.append("")
        lines.append("Двигать некого — все цены уже соответствуют форме.")
        return "\n".join(lines)
    lines.append(f"Движений: {len(changes)}")
    lines.append("")
    for c in changes[:25]:
        arrow = "▲" if c["new"] > c["old"] else "▼"
        lines.append(f"{arrow} {c.get('name') or 'Игрок'}: {c['old']} → {c['new']}")
        lines.append(f"    {c.get('reason', '')}")
    if len(changes) > 25:
        lines.append(f"… и ещё {len(changes) - 25}")
    if dry:
        lines += ["", "Применить — кнопка «💰 Пересчитать цены»."]
    return "\n".join(lines)


async def _handle_fantasy_action(query, action: str, arg: Optional[str] = None) -> None:
    import fantasy
    if action in ("prices", "pricesdry"):
        import fantasy_prices
        dry = action == "pricesdry"
        await query.edit_message_text("⏳ Считаю цены…")
        res = await asyncio.to_thread(
            fantasy_prices.recalc, fantasy.get_active_season(),
            None if dry else _get_spreadsheet(), dry)
        await query.edit_message_text(
            _prices_text(res, dry),
            reply_markup=InlineKeyboardMarkup([_back_button("admin:menu:fantasy")]))
        return
    if action == "end":
        seasons = fantasy.active_seasons()
        if arg is None and len(seasons) > 1:
            # Несколько активных лиг — спрашиваем, какую закрыть.
            rows = [[InlineKeyboardButton(f"🏁 {s['name']} ({s.get('format','')})",
                                          callback_data=f"admin:fantasy:end:{s['id']}")]
                    for s in seasons]
            rows.append(_back_button("admin:menu:fantasy"))
            await query.edit_message_text("Какую лигу завершить?", reply_markup=InlineKeyboardMarkup(rows))
            return
        result = fantasy.end_season(int(arg) if arg else None)
        if not result:
            await query.edit_message_text("Активной лиги нет.", reply_markup=_fantasy_menu_markup())
            return
        final = fantasy.format_season_final(result["season"]["id"])
        await query.edit_message_text(
            f"{final}\n\n(Показано только тебе. Разошли в чат вручную, если нужно.)",
            reply_markup=_fantasy_menu_markup(),
        )
        return
    if action == "start":
        now = datetime.now()
        months = ["", "января", "февраля", "марта", "апреля", "мая", "июня",
                  "июля", "августа", "сентября", "октября", "ноября", "декабря"]
        name = f"Фэнтези {months[now.month]} {now.year}"
        season = fantasy.start_season(name, "3x3")
        await query.edit_message_text(
            f"✅ Сезон запущен: «{season['name']}» (формат {season['format']}).",
            reply_markup=_fantasy_menu_markup(),
        )
    elif action == "format":
        season = fantasy.get_active_season()
        if not season:
            await query.edit_message_text(_fantasy_menu_text(), reply_markup=_fantasy_menu_markup())
            return
        new_fmt = "5x5" if str(season.get("format", "3x3")).startswith("3") else "3x3"
        fantasy.set_format(new_fmt)
        await query.edit_message_text(
            f"🔀 Формат изменён на {new_fmt}.", reply_markup=_fantasy_menu_markup())
    elif action == "pool":
        await query.edit_message_text(
            "👥 Чьи ростеры в пуле фэнтези?\n\nОтмечай команды — их игроков можно "
            "будет ставить в состав. ✅ — в пуле.",
            reply_markup=await _fantasy_pool_markup())
    elif action == "scope":
        current = fantasy.scopes_title(fantasy.season_scopes(fantasy.get_active_season()))
        await query.edit_message_text(
            "🎯 По каким турнирам считать очки?\n\n"
            "Можно выбрать несколько — команда играет в нескольких лигах. "
            "✅ — выбрано, 🟢 — идёт сейчас.\n"
            "«По настройкам поиска игр» подставит те же лиги, что бот ищет для расписания.\n\n"
            f"Сейчас: {current}",
            reply_markup=await _fantasy_scope_markup())
    elif action == "ingest":
        await query.edit_message_text("⏳ Пересчёт статистики фэнтези...")
        try:
            code, out, err = await script_runner.run_script("run_fantasy.py", ["--only", "ingest"])
            summary = script_runner.summarize_output(out) if code == 0 else (err.strip().splitlines()[-1:] or ["ошибка"])[0]
        except Exception as e:
            summary = str(e)
        await query.edit_message_text(f"📥 Статистика фэнтези\n\n{summary}", reply_markup=_fantasy_menu_markup())
    else:
        await query.edit_message_text(_fantasy_menu_text(), reply_markup=_fantasy_menu_markup())


async def _handle_launch_action(query, action: str, force: bool) -> None:
    if action == "daily":
        data_types = DAILY_DATA_TYPES
        scripts = DAILY_SCRIPTS
        label = "Оповещения на сегодня"
    else:
        cfg = LAUNCH_ACTIONS.get(action)
        if not cfg:
            return
        data_types = cfg["data_types"]
        scripts = [(cfg["script"], cfg["args"])]
        label = cfg["label"]

    if not force:
        already_at = _check_already_run_today(data_types)
        if already_at:
            await query.edit_message_text(
                f"⚠️ {label}: уже запускалось сегодня ({already_at})\n\nЗапустить повторно?",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Всё равно запустить", callback_data=f"admin:run:{action}:force")],
                    [InlineKeyboardButton("Отмена", callback_data="admin:menu:launch")],
                ]),
            )
            return
    else:
        for dt_ in data_types:
            duplicate_protection.delete_todays_records(dt_)

    await query.edit_message_text(f"⏳ Запускаю: {label}...")

    ok = True
    result_lines = []
    for script, args in scripts:
        try:
            code, out, stderr = await script_runner.run_script(script, args)
        except Exception as e:
            code, out, stderr = 1, "", str(e)
        if code == 0:
            result_lines.append(f"✅ {script}\n{script_runner.summarize_output(out)}")
        else:
            ok = False
            result_lines.append(f"❌ {script}: {stderr.strip().splitlines()[-1] if stderr.strip() else 'ошибка, см. логи демона'}")
            log.error(f"Скрипт {script} завершился с ошибкой (код {code}): {stderr[-2000:]}")
            sheets_cache.report_error(script, stderr[-2000:] or f"exit code {code}", _get_spreadsheet())

    header = "✅" if ok else "⚠️"
    text = f"{header} {label} — готово\n\n" + "\n\n".join(result_lines)
    if len(text) > 3800:  # запас от лимита Telegram в 4096 символов
        text = text[:3800] + "\n…(обрезано)"
    await query.edit_message_text(text, reply_markup=_launch_menu_markup())


REPORT_SCRIPTS = {
    "training": ("training_report.py", "Тренировки", _reports_training_menu_markup),
    "games": ("game_report.py", "Игры", _reports_games_menu_markup),
}


async def _handle_report_action(query, kind: str, period: str) -> None:
    cfg = REPORT_SCRIPTS.get(kind)
    if not cfg:
        return
    script, sheet_label, menu_fn = cfg
    await query.edit_message_text(f"⏳ Формирую отчёт ({sheet_label.lower()}, {period})...")
    args = ["--week"] if period == "week" else ["--month", datetime.now().strftime("%Y-%m")]
    try:
        code, _stdout, stderr = await script_runner.run_script(script, args)
    except Exception as e:
        code, stderr = 1, str(e)
    if code == 0:
        text = f"✅ Отчёт обновлён в таблице (лист «{sheet_label}»)."
    else:
        text = f"❌ Не удалось сформировать отчёт: {stderr.strip().splitlines()[-1] if stderr.strip() else 'см. логи демона'}"
        log.error(f"{script} завершился с ошибкой (код {code}): {stderr[-2000:]}")
        sheets_cache.report_error(script, stderr[-2000:] or f"exit code {code}", _get_spreadsheet())
    await query.edit_message_text(text, reply_markup=menu_fn())


# Кто сейчас вводит ник для выдачи доступа: {user_id: вид доступа}. Только в
# памяти, как и обратная связь: после рестарта админ просто нажмёт ещё раз.
_awaiting_coach: Dict[int, str] = {}

COACH_ASK = ("Пришли @ник того, кому открыть раздел «{title}».\n\n"
             "Доступ выдаётся по нику, но закрепится за числовым id при первом "
             "его входе — сменит ник, доступ останется.\n\nПередумал — /start.")


def _render_access_list() -> Tuple[str, InlineKeyboardMarkup]:
    """Кому открыты закрытые разделы. Оба вида в одном месте — иначе админу
    пришлось бы помнить, в каком экране что выдаётся."""
    lines = ["🔑 Доступы к закрытым разделам", "",
             "Админам открыто всё и без списка. Остальным — по выдаче: "
             "по @нику, с закреплением за числовым id при первом входе.", ""]
    rows: List[List[InlineKeyboardButton]] = []
    for kind, title in sheets_cache.ACCESS_TITLES.items():
        people = sheets_cache.access_list(kind)
        lines.append(f"{title}: {len(people) or 'никому'}")
        for a in people:
            state = "вошёл" if a["tg_user_id"] else "ещё не заходил"
            lines.append(f"   @{a['username']} — {state}")
        lines.append("")
        rows.append([InlineKeyboardButton(f"➕ Открыть «{title}»",
                                          callback_data=f"admin:acc:add:{kind}")])
        for a in people:
            rows.append([InlineKeyboardButton(
                f"✂️ Забрать «{title}» у @{a['username']}"[:64],
                callback_data=f"admin:acc:del:{kind}:{a['username']}")])
    rows.append(_back_button())
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def handle_prog_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки «Прогресс команды». Отдельно от админских: сюда пускаем и
    тренеров, которым админ выдал доступ."""
    query = update.callback_query
    user = query.from_user if query else None
    if not query or not _can_see_reports(user):
        if query:
            await query.answer("Нет доступа", show_alert=True)
        return
    await query.answer()
    parts = (query.data or "").split(":")
    what = parts[1] if len(parts) > 1 else "list"
    admin = _is_admin(user)
    try:
        if what == "team":
            await query.edit_message_text("⏳ Считаю разбор…")
            await _prog_send(query.message, parts[2], parts[3])
            return
        else:
            text, markup = await _render_prog_list(is_admin=admin)
        await query.edit_message_text(text, reply_markup=markup)
    except Exception as e:
        log.error(f"Экран прогресса: {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}")


async def handle_coach_nick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Следующее сообщение после «Открыть раздел» — @ник получателя."""
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_coach:
        return
    kind = _awaiting_coach.pop(user.id)
    title = sheets_cache.ACCESS_TITLES.get(kind, kind)
    nick = (msg.text or "").strip().split()[0] if (msg.text or "").strip() else ""
    if not nick.lstrip("@").replace("_", "").isalnum():
        await msg.reply_text("Это не похоже на @ник. Открой экран и попробуй ещё раз.")
        raise ApplicationHandlerStop
    sheets_cache.grant_access(kind, nick, str(user.id))
    await msg.reply_text(
        f"✅ Раздел «{title}» открыт для {nick}.\n\n"
        "Кнопка появится у него после /start. Если он уже нажимал /start — "
        "пусть нажмёт ещё раз, клавиатура обновится.")
    raise ApplicationHandlerStop


async def handle_mystats_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопка «📊 Моя статистика» — тем, кому раздел открыт."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if not _can_see_personal(user):
        return
    await _send_profile(msg, user)
    raise ApplicationHandlerStop


async def handle_progress_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопка «📈 Прогресс команды» на нижней клавиатуре — для тренеров.

    Кнопки в клавиатуре больше нет (её место занял раздел тренера), но у тех,
    кто не нажимал /start после обновления, она ещё висит — обработчик оставлен
    ради них."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if not _can_see_reports(user):
        return
    text, markup = await _render_prog_list(is_admin=_is_admin(user), back="coach:main")
    await msg.reply_text(text, reply_markup=markup)
    raise ApplicationHandlerStop


# ─────────────────────────── Раздел тренера ────────────────────────────────
#
# Отдельная кнопка под чатом для тренера и тех, кому открыт «team»-доступ.
# Внутри — разбор игр (он же приходит в личку сам после матча) и учёт оплат:
# тренеру на телефон падает СМС о поступлении, он вставляет её сюда, а бот
# разбирает сумму с отправителем и пишет платёж в базу и лист «Оплаты».

COACH_TEXT = ("🧑‍🏫 Раздел тренера\n\n"
              "Разбор игр приходит сюда сам после каждого матча — здесь его "
              "можно открыть в любой момент.\n"
              "Оплаты: вставь СМС от банка, остальное сделаю я.")

# Черновики платежей: {tg id тренера: разобранная СМС + выбор}. В памяти —
# незаконченный ввод переживать рестарт не должен, а ФИО отправителя из СМС
# на диск не попадает.
_pay_draft: Dict[int, Dict[str, Any]] = {}
_awaiting_payment: set = set()

PAY_ASK = ("💳 Вставь СМС от банка целиком — я вытащу сумму и отправителя.\n\n"
           "Можно и руками: «Фамилия 900».\n\n"
           "Передумал — /start.")

PLAYERS_PER_PAGE = 8


def _coach_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📈 Разбор игр", callback_data="coach:prog")],
        [InlineKeyboardButton("💳 Внести оплату", callback_data="coach:pay")],
        [InlineKeyboardButton("👥 Состав на игру", callback_data="coach:games")],
        [InlineKeyboardButton("🏋️ Взносы за тренировки", callback_data="coach:train")],
        [InlineKeyboardButton("💸 Долги", callback_data="coach:debts"),
         InlineKeyboardButton("➕ Добавить долг", callback_data="coach:adddebt")],
        [InlineKeyboardButton("📒 Кто сколько внёс", callback_data="coach:owe"),
         InlineKeyboardButton("🧾 Последние платежи", callback_data="coach:last")],
        [InlineKeyboardButton("✏️ Изменить суммы", callback_data="coach:sums"),
         InlineKeyboardButton("🗑 Удалить оплату", callback_data="coach:delpay")],
        [InlineKeyboardButton("🗓 Даты оповещений", callback_data="coach:sched")],
    ])


# Даты оповещений об оплатах: ключ настройки → (подпись, что это значит).
SCHED_FIELDS = [
    ("dues_ahead_day", "Заранее за следующий месяц", "число месяца"),
    ("dues_first_day", "За начавшийся месяц", "число месяца"),
    ("dues_mid_day", "Повтор должникам", "число месяца"),
    ("dues_coach_warn", "Тренеру перед повтором", "за сколько дней"),
    ("dues_coach_end", "Тренеру перед концом месяца", "за сколько дней"),
    ("game_pay_hour", "Оплата игры: утро", "час"),
    ("game_pay_evening_hour", "Оплата игры: вечер следующего дня", "час"),
    ("roster_collect_days", "Собрать состав на игру", "за сколько дней"),
]

SCHED_LIMITS = {"game_pay_hour": (0, 23), "game_pay_evening_hour": (0, 23),
                "roster_collect_days": (0, 14)}


def _sched_value(key: str) -> int:
    import game_roster
    import sheets_cache
    import training_dues
    if key in training_dues.SCHEDULE:
        return training_dues.day(key)
    defaults = {"game_pay_hour": 9, "game_pay_evening_hour": 19,
                "roster_collect_days": game_roster.COLLECT_BEFORE_DAYS}
    return sheets_cache.get_int_setting(key, defaults[key])


def _sched_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Когда бот напоминает про деньги. Всё правится кнопками."""
    lines = ["🗓 Даты оповещений", "",
             "🏋️ <b>Взносы за тренировки</b> — по календарю месяца:"]
    rows = []
    for key, title, unit in SCHED_FIELDS:
        if key == "game_pay_hour":
            lines += ["", "🏀 <b>Оплата игр</b> — от даты матча:"]
        value = _sched_value(key)
        shown = (f"{value}-го" if unit == "число месяца"
                 else f"{value}:00" if unit == "час"
                 else f"за {value} дн.")
        lines.append(f"   • {title}: {shown}")
        rows.append([InlineKeyboardButton(f"{title}: {shown}"[:60],
                                          callback_data=f"coach:setsched:{key}")])
    lines += ["", "<i>Взносы за тренировки начинаются с сентября 2026 — "
              "до этого месяца бот про них молчит.</i>"]
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# Что тренер сейчас вводит: id → «долг:строка» или «сумма:строка:вид».
_awaiting_money: Dict[int, str] = {}


def _debts_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Кто и сколько должен — тремя блоками: тренировки, игры, добавленное.

    Показываем только тех, с кого действительно ждём: без проставленной суммы
    взноса человек не должник, а игры считаются лишь по объявленным составам
    начиная с даты, с которой действует порядок."""
    import coach_payments
    import game_roster
    import training_dues
    from datetime import date as _date

    lines = ["💸 Долги", ""]
    # Тренировки: все месяцы, которые уже считаются, от первого до текущего.
    train: List[Tuple[str, List[Dict[str, Any]]]] = []
    period = training_dues.FIRST_PERIOD
    cur = training_dues.period_of(_date.today())
    while period <= cur:
        if training_dues.counts(period):
            debtors = training_dues.debtors(period)
            if debtors:
                train.append((period, debtors))
        period = training_dues.next_period(period)

    lines.append("🏋️ <b>За тренировки</b>")
    if train:
        for per, people in train:
            lines.append(f"   {training_dues.month_title(per)}:")
            for r in people:
                lines.append(f"   • {r['title']} — {r['debt']} ₽")
    else:
        lines.append("   Никто не должен.")

    games = game_roster.game_debts()
    lines += ["", "🏀 <b>За игры</b>"]
    if games:
        for g in games:
            lines.append(f"   • {g['title']} — {g['games']} "
                         f"{_plural(g['games'], 'игра', 'игры', 'игр')}, {g['amount']} ₽")
    else:
        lines.append("   Никто не должен.")

    extra = coach_payments.extra_debts()
    if extra:
        lines += ["", "📌 <b>Добавлено вручную</b>"]
        for d in extra:
            who = (coach_payments.player_by_row(d["player_row"]) or {}).get("title", "?")
            note = f" — {d['note']}" if d["note"] else ""
            lines.append(f"   • {who}: {d['amount']} ₽{note}")

    total = (sum(r["debt"] for _, people in train for r in people)
             + sum(g["amount"] for g in games)
             + sum(d["amount"] for d in extra))
    lines += ["", f"Итого: {total} ₽" if total else "Долгов нет."]

    rows = [[InlineKeyboardButton(f"✅ Погасить: {(coach_payments.player_by_row(d['player_row']) or {}).get('title', '?')}"[:60],
                                  callback_data=f"coach:closedebt:{d['id']}")]
            for d in extra[:5]]
    rows.append([InlineKeyboardButton("➕ Добавить долг", callback_data="coach:adddebt")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _sums_screen(row: Optional[int] = None) -> Tuple[str, InlineKeyboardMarkup]:
    """Сколько ждём с человека: взнос за тренировки и цена игры."""
    import coach_payments
    if row is None:
        people = coach_payments.players()
        lines = ["✏️ Изменить суммы", "",
                 "Что бот ждёт с человека. Правится здесь же, в таблицу лезть "
                 "не надо.", ""]
        rows = []
        for p in people[:PLAYERS_PER_PAGE]:
            lines.append(f"• {p['title']}: тренировки {p['pay_season'] or '—'} ₽ · "
                         f"игра {p['pay_game'] or '—'} ₽")
            rows.append([InlineKeyboardButton(p["title"][:60],
                                              callback_data=f"coach:sums:{p['row']}")])
        if len(people) > PLAYERS_PER_PAGE:
            lines.append(f"…и ещё {len(people) - PLAYERS_PER_PAGE} — "
                         f"жми «👤 Другой игрок»")
            rows.append([InlineKeyboardButton("👤 Другой игрок", callback_data="coach:sumswho")])
        rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
        return "\n".join(lines), InlineKeyboardMarkup(rows)

    p = coach_payments.player_by_row(int(row))
    if not p:
        return "Не нашёл игрока.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К списку", callback_data="coach:sums")]])
    lines = [f"👤 {p['title']}", "",
             f"🏋️ Взнос за тренировки: {p['pay_season'] or 'не задан'} ₽",
             f"🏀 Оплата игры: {p['pay_game'] or 'не задана'} ₽", "",
             "Что поменять?"]
    return "\n".join(lines), InlineKeyboardMarkup([
        [InlineKeyboardButton("🏋️ Тренировки", callback_data=f"coach:setsum:{row}:season"),
         InlineKeyboardButton("🏀 Игра", callback_data=f"coach:setsum:{row}:game")],
        [InlineKeyboardButton("⬅️ К списку", callback_data="coach:sums")]])


def _delpay_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Последние платежи с возможностью отменить ошибочный."""
    import coach_payments
    items = coach_payments.recent_payments(limit=8)
    if not items:
        return ("🗑 Удалить оплату\n\nПлатежей пока нет.",
                InlineKeyboardMarkup([[InlineKeyboardButton(
                    "⬅️ В раздел", callback_data="coach:main")]]))
    lines = ["🗑 Удалить оплату", "",
             "Отменяем ошибочные: тест, ложное срабатывание, возврат части "
             "суммы. Запись уходит из расчётов, в листе «Логи оплаты» строка "
             "остаётся историей.", ""]
    rows = []
    for it in items:
        what = ("игра" if it["kind"] == coach_payments.KIND_GAME else
                "тренировки" if it["kind"] == coach_payments.KIND_SEASON else "?")
        extra = f" ×{it['games']}" if it["games"] else ""
        lines.append(f"• {coach_payments._human_date(it['paid_at'])} — {it['title']}: "
                     f"{it['amount']} ₽ ({what}{extra})")
        rows.append([InlineKeyboardButton(
            f"🗑 {it['title']} · {it['amount']} ₽"[:60],
            callback_data=f"coach:delpay:{it['id']}")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _my_games_video(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Свои игры с записью — список для «🎬 Я в записи»."""
    import coach_payments
    import game_timeline
    import player_identity
    ids = [(r["source"], r["player_id"]) for r in player_identity.get_identities(uid)]
    games = game_timeline.player_games(ids, limit=8) if ids else []
    back = [[InlineKeyboardButton("⬅️ К отчёту", callback_data="rep:back")]]
    if not games:
        return ("🎬 Я в записи\n\nПока нечего показать: разметка появляется "
                "после игры, когда бот найдёт запись в группе ВК.",
                InlineKeyboardMarkup(back))

    lines = ["🎬 Я в записи", "", "Выбери игру — покажу твои выходы на площадку "
             "со ссылками прямо на них.", ""]
    rows = []
    for g in games:
        day = coach_payments._human_date(str(g["game_date"]))
        title = f"{g['home_name'] or '—'} — {g['guest_name'] or '—'}"
        lines.append(f"• {day} · {title}")
        rows.append([InlineKeyboardButton(
            f"{day} · {title}"[:60],
            callback_data=f"rep:vidg:{g['source']}:{g['game_id']}:{g['player_id']}")])
    rows += back
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _my_video_game(source: str, game_id: str, player_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Тайм-коды своих выходов в одной игре (HTML)."""
    import coach_payments
    import game_timeline
    import sheets_cache
    import vk_video
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        meta = conn.execute(
            """SELECT game_date, home_name, guest_name, home_score, guest_score
                 FROM game_meta WHERE source = ? AND game_id = ?""",
            (source, str(game_id))).fetchone()
    link = vk_video.link_of(source, game_id)
    block = game_timeline.format_block(source, game_id, player_id, link,
                                       max_items=12)
    head = "🎬 Я в записи"
    if meta:
        head += (f"\n{coach_payments._human_date(str(meta['game_date']))} · "
                 f"{meta['home_name'] or '—'} — {meta['guest_name'] or '—'}")
        if meta["home_score"] or meta["guest_score"]:
            head += f" {meta['home_score']}:{meta['guest_score']}"
    text = f"{head}\n\n{block}" if block else (
        f"{head}\n\nВ этой игре разметки нет — протокол лиги не размечен.")
    rows = []
    if block:
        # Поправить может любой, кто смотрит запись: сверять время с табло
        # умеет только человек. Введённое действует для всех и переживает
        # автоматические пересчёты.
        rows.append([InlineKeyboardButton(
            "⏱ Время не сходится", callback_data=f"rep:vidt:{source}:{game_id}:{player_id}")])
    rows.append([InlineKeyboardButton("⬅️ К списку игр", callback_data="rep:vid")])
    rows.append([InlineKeyboardButton("⬅️ К отчёту", callback_data="rep:back")])
    return text, InlineKeyboardMarkup(rows)


VIDTIME_ASK = ("⏱ Открой запись и найди спорный мяч — момент, с которого "
               "пошла игра.\n\nПришли его время с плеера: «5:33» или "
               "«1:02:15».\n\nВсе выходы этой игры пересчитаю от него — и "
               "у тебя, и у остальных.\n\nПередумал — /start.")


def _games_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Ближайшие игры — чтобы собрать состав, не дожидаясь напоминания.
    Игру в лиге открывают когда угодно, а состав тренеру бывает нужен раньше."""
    import game_roster
    today = date.today()
    # Только предстоящие: по сыгранной игре состав уже не собирают, а в списке
    # она путается с ближайшей.
    upcoming = game_roster.games(from_day=today,
                                 until_day=today + timedelta(days=21))
    if not upcoming:
        return ("👥 Ближайших игр не вижу.\n\nОпрос на игру бот заводит, когда "
                "она появляется в лиге — тогда же можно собрать состав.",
                InlineKeyboardMarkup([[InlineKeyboardButton(
                    "⬅️ В раздел", callback_data="coach:main")]]))
    lines = ["👥 Состав на игру", "", "Выбери игру:"]
    rows = []
    for g in upcoming:
        picked = len(game_roster.roster(g["source"], g["game_id"]))
        mark = f" · в составе {picked}" if picked else ""
        rows.append([InlineKeyboardButton(
            f"{game_roster.game_label(g)}{mark}"[:60],
            callback_data=f"rost:show:{g['source']}:{g['game_id']}")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _roster_screen(source: str, game_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Сбор состава на игру: кто вызвался, кто уже в составе, кого дописали."""
    import game_roster
    game = next((g for g in game_roster.games()
                 if g["source"] == source and g["game_id"] == str(game_id)), None)
    if not game:
        return ("Игру не нашёл — возможно, опрос по ней уже удалён.",
                InlineKeyboardMarkup([[InlineKeyboardButton(
                    "⬅️ В раздел", callback_data="coach:main")]]))
    game_roster.ensure_state(game)
    picked = game_roster.roster(source, game_id)
    picked_rows = {p["row"] for p in picked}
    ready = game_roster.voters(str(game_id))

    lines = [f"🏀 Состав на игру: {game_roster.game_label(game)}", ""]
    if picked:
        lines.append(f"В составе ({len(picked)}):")
        lines += [f"• {p['title']}" for p in picked]
        lines.append("")
    waiting = [v for v in ready if v["row"] not in picked_rows]
    if waiting:
        lines.append("Отметились «Готов», но пока не в составе:")
        for v in waiting:
            lines.append(f"• {v['title']}" + ("" if v["linked"] else " (нет в листе)"))
        lines.append("")
    if not ready and not picked:
        lines.append("По опросу пока никто не отметился.")
        lines.append("")
    lines.append("Добавить любого — просто напиши фамилию или её часть.")
    posted = game_roster.is_posted(source, game_id)
    stale = posted and game_roster.is_stale(source, game_id)
    if stale:
        lines.append("⚠️ В чате висит прежний состав — обнови сообщение.")
    elif posted:
        lines.append("✅ Состав в чате актуален.")

    rows: List[List[InlineKeyboardButton]] = []
    for v in waiting[:10]:
        if v["linked"]:
            rows.append([InlineKeyboardButton(
                f"➕ {v['title']}"[:60],
                callback_data=f"rost:add:{source}:{game_id}:{v['row']}")])
    for p in picked[:16]:
        rows.append([InlineKeyboardButton(
            f"➖ {p['title']}"[:60],
            callback_data=f"rost:del:{source}:{game_id}:{p['row']}")])
    if picked and stale:
        rows.append([InlineKeyboardButton(
            "✏️ Обновить сообщение в чате",
            callback_data=f"rost:edit:{source}:{game_id}")])
    elif picked and not posted:
        rows.append([InlineKeyboardButton(
            "📣 Отправить состав в чат",
            callback_data=f"rost:post:{source}:{game_id}")])
    if picked and posted:
        rows.append([InlineKeyboardButton(
            "📣 Отправить заново (новое сообщение)",
            callback_data=f"rost:post:{source}:{game_id}")])
        # Долги по игре открывались только из рассылки после матча. Тренеру
        # они нужны и раньше — например, когда деньги собирают в зале.
        rows.append([InlineKeyboardButton(
            "💰 Кто не оплатил", callback_data=f"rost:debt:{source}:{game_id}")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(rows)


def _game_debt_screen(source: str, game_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Кто из состава не заплатил за игру."""
    import game_roster
    game = next((g for g in game_roster.games()
                 if g["source"] == source and g["game_id"] == str(game_id)), None)
    if not game:
        return ("Игру не нашёл.", InlineKeyboardMarkup([[InlineKeyboardButton(
            "⬅️ В раздел", callback_data="coach:main")]]))
    rows = game_roster.debtors(source, game_id)
    text = game_roster.coach_debt_text(game, rows)
    buttons = [[InlineKeyboardButton(f"✔ {p['title']}"[:60],
                                     callback_data=f"rost:paid:{source}:{game_id}:{p['row']}")]
               for p in rows[:20]]
    buttons.append([InlineKeyboardButton("👥 Состав",
                                         callback_data=f"rost:show:{source}:{game_id}")])
    buttons.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return text, InlineKeyboardMarkup(buttons)


def _train_screen(period: str = "") -> Tuple[str, InlineKeyboardMarkup]:
    """Взносы за месяц: кто не заплатил + кнопки «отметить оплату».

    Отметка нужна, когда деньги пришли мимо бота (наличными, чек не прислали):
    без неё человек до конца месяца висел бы в должниках."""
    import training_dues
    period = period or training_dues.period_of(date.today())
    rows = training_dues.status(period)
    title = training_dues.month_title(period)
    if not training_dues.counts(period):
        text = (f"🏋️ {title}\n\nВзносы считаем с "
                f"{training_dues.month_title_gen(training_dues.FIRST_PERIOD)} — "
                "более ранние месяцы тренер считает закрытыми.")
        return text, InlineKeyboardMarkup([[InlineKeyboardButton(
            "⬅️ Назад", callback_data="coach:main")]])

    debt = [r for r in rows if r["need"] and not r["ok"]]
    ok = [r for r in rows if r["ok"]]
    no_sum = [r for r in rows if not r["need"]]
    lines = [f"🏋️ Взносы за тренировки — {title}", ""]
    if debt:
        lines.append(f"Не оплатили ({len(debt)}):")
        for r in debt:
            got = f", внёс {r['paid']}" if r["paid"] else ""
            lines.append(f"• {r['title']} — {r['debt']} ₽{got}")
        lines.append("")
    if ok:
        lines.append(f"Оплатили ({len(ok)}): " + ", ".join(r["title"] for r in ok))
        lines.append("")
    if no_sum:
        lines.append("Не проставлена сумма в «Оплате сезона»: "
                     + ", ".join(r["title"] for r in no_sum))
        lines.append("")
    if not (debt or ok or no_sum):
        lines.append("Никого не ждём: ни у кого нет отметки в «Активности».")
    else:
        lines.append("Кнопка = «деньги были, чек не присылали».")

    buttons = [[InlineKeyboardButton(f"✔ {r['title']}"[:60],
                                     callback_data=f"coach:trmark:{r['row']}:{period}")]
               for r in debt[:20]]
    prev = training_dues.period_of(date(int(period[:4]), int(period[5:7]), 1)
                                   - timedelta(days=1))
    nav = []
    # За месяцы до старта учёта листать некуда — там всё закрыто по договорённости.
    if training_dues.counts(prev):
        nav.append(InlineKeyboardButton(f"⬅️ {training_dues.month_title(prev)}",
                                        callback_data=f"coach:train:{prev}"))
    nxt = training_dues.next_period(period)
    if nxt <= training_dues.period_of(date.today()):
        nav.append(InlineKeyboardButton(f"{training_dues.month_title(nxt)} ➡️",
                                        callback_data=f"coach:train:{nxt}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(buttons)


async def handle_coach_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопка «🧑‍🏫 Тренер» на нижней клавиатуре."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if not _can_see_reports(user):
        return
    await msg.reply_text(COACH_TEXT, reply_markup=_coach_markup())
    raise ApplicationHandlerStop


def _pay_players_markup(page: int = 0, query: str = "",
                        pick: str = "coach:pick") -> InlineKeyboardMarkup:
    """Выбор игрока списком. Страницами: тридцать кнопок разом Telegram
    покажет, но попасть в нужную пальцем уже нельзя.

    pick — куда ведёт нажатие: тот же список нужен и для оплаты, и для долга,
    и для правки сумм, менялся только адрес."""
    import coach_payments
    # Все из листа: за игру может заплатить и тот, кто сейчас не тренируется.
    people = coach_payments.players()
    if query:
        found = coach_payments.match_player(query) or coach_payments.search_players(query)
        if found:
            people = found + [p for p in people if p not in found]
    pages = max(1, (len(people) + PLAYERS_PER_PAGE - 1) // PLAYERS_PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = people[page * PLAYERS_PER_PAGE:(page + 1) * PLAYERS_PER_PAGE]
    rows = [[InlineKeyboardButton(p["title"][:60], callback_data=f"{pick}:{p['row']}")]
            for p in chunk]
    if pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"coach:page:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="coach:noop"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"coach:page:{page + 1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="coach:main")])
    return InlineKeyboardMarkup(rows)


def _pay_confirm(draft: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    """Экран подтверждения. Тип платежа не угадался — вместо «Записать»
    спрашиваем, за что деньги: молча записать не туда хуже, чем спросить."""
    import coach_payments
    player = coach_payments.player_by_row(draft["row"])
    title = player["title"] if player else f"строка {draft['row']}"
    kind, games = draft["kind"], draft["games"]
    gprice = coach_payments.game_price(player)
    sprice = coach_payments.season_price(player)
    unknown = kind == coach_payments.KIND_UNKNOWN

    lines = ["💳 Проверь и подтверди", "",
             f"Кто: {title}",
             f"Сумма: {draft['amount']} ₽"]
    if kind == coach_payments.KIND_GAME:
        rest = draft["amount"] - games * gprice
        lines.append(f"За что: {coach_payments.games_word(games)} (по {gprice} ₽)"
                     + (f" и ещё {rest} ₽ сверх" if rest > 0 else ""))
    elif kind == coach_payments.KIND_SEASON:
        lines.append("За что: взнос за сезон (тренировки)")
    lines.append(f"Дата: {coach_payments._human_date(draft['paid_at'])}")
    if draft.get("bank"):
        lines.append(f"Банк: {draft['bank']}")
    if draft.get("recognized"):
        lines += ["", "👤 Узнал по прошлым платежам — спрашивать, кто это, "
                      "больше не буду."]
    if draft.get("outgoing"):
        lines += ["", "⚠️ Похоже на списание, а не на поступление. "
                      "Если это всё-таки оплата — записывай."]

    if unknown:
        lines += ["", f"За что этот платёж? Не подошло ни под игры (по {gprice} ₽, "
                      f"до {coach_payments.MAX_GAMES_PER_PAYMENT}-х за раз), "
                      f"ни под взнос за сезон ({sprice or '—'} ₽)."]
        by_games = (draft["amount"] // gprice) if gprice else 0
        # Сумма не делится нацело — числа в подписи не обещаем: «за игры (5)»
        # на 5000 ₽ при цене 900 ₽ было бы неправдой.
        exact = bool(gprice) and draft["amount"] % gprice == 0
        label = (f"🏀 За игры ({coach_payments.games_word(by_games)})"
                 if exact and by_games else "🏀 За игры")
        rows = [[InlineKeyboardButton(label, callback_data="coach:kind:game")]]
        rows.append([InlineKeyboardButton("🏋️ Взнос за сезон",
                                          callback_data="coach:kind:season")])
    else:
        other = ("взнос за сезон" if kind == coach_payments.KIND_GAME else "оплату игр")
        rows = [[InlineKeyboardButton("✅ Записать", callback_data="coach:save")],
                [InlineKeyboardButton(f"🔀 Это {other}", callback_data="coach:kind")]]
    rows.append([InlineKeyboardButton("👤 Другой игрок", callback_data="coach:who")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="coach:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _pay_owe_text() -> str:
    """Кто сколько внёс. Взнос за сезон спрашиваем только с тех, у кого стоит
    отметка в «Активности»; оплата игр приходит от всех, кто был в составе."""
    import coach_payments
    rows = coach_payments.balances()
    if not rows:
        return ("📒 В листе «Игроки» пока никого нет.\n\n"
                "Проверь синхронизацию таблицы.")
    lines = ["📒 Кто сколько внёс", ""]
    debtors = [r for r in rows if r["need_season"] and r["debt"] > 0]
    closed = [r for r in rows if r["season_done"]]
    # Не тренируется — взноса с него не ждём, но платежи за игры показываем.
    resting = [r for r in rows if not r["pays_season"]]
    # Тренируется, а сумма взноса в листе не проставлена — тут нечего считать.
    no_plan = [r for r in rows if r["pays_season"] and not r["pay_season"]]

    if debtors:
        lines.append("Не хватает за сезон:")
        for r in debtors:
            lines.append(f"• {r['title']} — {r['debt']} ₽ "
                         f"(внёс {r['paid_season']} из {r['need_season']})")
        lines.append("")
    if closed:
        lines.append("Сезон закрыт:")
        for r in closed:
            games = f", {coach_payments.games_word(r['paid_games'])}" if r["paid_games"] else ""
            lines.append(f"• {r['title']} — {r['paid_season']} ₽{games}")
        lines.append("")
    if resting:
        lines.append(f"Взнос не ждём — не тренируются "
                     f"({coach_payments.plural(len(resting), 'игрок', 'игрока', 'игроков')}):")
        for r in resting:
            paid_sum = r["paid_season"] + r["paid_game_amount"]
            games = (f", {coach_payments.games_word(r['paid_games'])}"
                     if r["paid_games"] else "")
            lines.append(f"• {r['title']}" + (f" — за игры {paid_sum} ₽{games}"
                                              if paid_sum else ""))
        lines.append("")
    if no_plan:
        lines.append(f"Тренируются, но сумма взноса не проставлена "
                     f"({coach_payments.plural(len(no_plan), 'игрок', 'игрока', 'игроков')}): "
                     + ", ".join(r["title"] for r in no_plan))
        lines.append("")
        lines.append("Долг считаю по столбцу «Оплата сезона» в листе «Игроки» — "
                     "проставь там суммы, и я начну следить.")
    return "\n".join(lines).rstrip()


def _pay_last_text() -> str:
    import coach_payments
    rows = coach_payments.recent(12)
    if not rows:
        return "🧾 Платежей ещё не было."
    lines = ["🧾 Последние платежи", ""]
    for r in rows:
        what = (coach_payments.games_word(r["games"])
                if r["kind"] == coach_payments.KIND_GAME else "сезон")
        bank = f" · {r['bank']}" if r["bank"] else ""
        lines.append(f"• {coach_payments._human_date(r['paid_at'])} — {r['title']}: "
                     f"{r['amount']} ₽ ({what}){bank}")
    return "\n".join(lines)


# Кого тренер сейчас набирает: {tg id: (source, game_id)}. В памяти —
# незаконченный набор состава переживать рестарт не обязан, сам состав уже
# в базе.
_roster_focus: Dict[int, Tuple[str, str]] = {}


async def handle_roster_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки состава на игру. Только для тренерского доступа."""
    import game_roster
    query = update.callback_query
    user = query.from_user if query else None
    if not query or not _can_see_reports(user):
        if query:
            await query.answer("Нет доступа", show_alert=True)
        return
    await query.answer()
    parts = (query.data or "").split(":")
    what = parts[1] if len(parts) > 1 else ""
    source = parts[2] if len(parts) > 2 else ""
    game_id = parts[3] if len(parts) > 3 else ""
    row = int(parts[4]) if len(parts) > 4 else 0
    _roster_focus[user.id] = (source, game_id)

    try:
        if what == "add":
            await asyncio.to_thread(game_roster.add, source, game_id, row, str(user.id))
            _drop_pending(user.id)
        elif what == "skip":
            _drop_pending(user.id)
        elif what == "del":
            await asyncio.to_thread(game_roster.remove, source, game_id, row)
        elif what == "paid":
            await asyncio.to_thread(game_roster.mark_paid, row, source, game_id,
                                    str(user.id))
            text, markup = await asyncio.to_thread(_game_debt_screen, source, game_id)
            await query.edit_message_text(text, reply_markup=markup)
            return
        elif what == "debt":
            text, markup = await asyncio.to_thread(_game_debt_screen, source, game_id)
            await query.edit_message_text(text, reply_markup=markup)
            return
        elif what == "post":
            await _post_roster(query, source, game_id, user)
            return
        elif what == "edit":
            await _update_roster_post(query, source, game_id)
            return

        # Спорные фамилии из списка разбираем подряд, не возвращая тренера
        # каждый раз к общему экрану.
        question = _next_roster_question(user.id, source, game_id)
        if question and what in ("add", "skip"):
            text, markup = question
            await query.edit_message_text(text, reply_markup=markup)
            return
        text, markup = await asyncio.to_thread(_roster_screen, source, game_id)
        await query.edit_message_text(text, reply_markup=markup)
    except Exception as e:
        log.error(f"Состав ({what}): {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}")


def _drop_pending(user_id: int) -> None:
    queue = _roster_pending.get(user_id)
    if queue:
        queue.pop(0)
        if not queue:
            _roster_pending.pop(user_id, None)


async def _post_roster(query, source: str, game_id: str, user) -> None:
    """Отправка состава в общий чат — единственное тренерское сообщение,
    которое туда уходит, и только по кнопке."""
    import game_roster
    game = next((g for g in await asyncio.to_thread(game_roster.games)
                 if g["source"] == source and g["game_id"] == str(game_id)), None)
    people = await asyncio.to_thread(game_roster.roster, source, game_id)
    if not game or not people:
        await query.answer("Состав пуст", show_alert=True)
        return
    text = game_roster.post_text(game, people)
    gsm = await asyncio.to_thread(_game_manager)
    topic = getattr(gsm, "game_poll_topic_id", None)
    posts = []
    for chat_id in _result_chat_ids(gsm):
        kwargs = {"chat_id": chat_id, "text": text}
        if topic is not None:
            kwargs["message_thread_id"] = topic
        try:
            m = await query.get_bot().send_message(**kwargs)
            # Запоминаем адрес сообщения: состав правят после отправки, и
            # тогда мы отредактируем это же, а не пришлём в чат второй список.
            posts.append({"chat_id": chat_id, "message_id": m.message_id})
        except Exception as e:
            log.warning(f"Состав не ушёл в чат {chat_id}: {e}")
    if posts:
        await asyncio.to_thread(game_roster.mark_posted, source, game_id, posts)
    screen, markup = await asyncio.to_thread(_roster_screen, source, game_id)
    note = (f"📣 Состав отправлен ({len(people)} чел.)." if posts
            else "⚠️ Не смог отправить состав в чат.")
    await query.edit_message_text(note + "\n\n" + screen, reply_markup=markup)


async def _update_roster_post(query, source: str, game_id: str) -> None:
    """Правит уже отправленное сообщение — без нового уведомления в чате."""
    import game_roster
    game = next((g for g in await asyncio.to_thread(game_roster.games)
                 if g["source"] == source and g["game_id"] == str(game_id)), None)
    people = await asyncio.to_thread(game_roster.roster, source, game_id)
    posts = await asyncio.to_thread(game_roster.posted_messages, source, game_id)
    if not game or not posts:
        await query.answer("Нечего править — состав в чат не отправляли",
                           show_alert=True)
        return
    text = game_roster.post_text(game, people)
    done, gone = 0, 0
    for post in posts:
        try:
            await query.get_bot().edit_message_text(
                chat_id=post["chat_id"], message_id=post["message_id"], text=text)
            done += 1
        except Exception as e:
            # «Message is not modified» — не ошибка: в чате уже то, что нужно.
            if "not modified" in str(e).lower():
                done += 1
            else:
                log.warning(f"Состав не обновился в {post['chat_id']}: {e}")
                gone += 1
    if done:
        await asyncio.to_thread(game_roster.mark_posted, source, game_id, posts)
    screen, markup = await asyncio.to_thread(_roster_screen, source, game_id)
    note = ("✏️ Сообщение в чате обновлено — новых уведомлений никому не ушло."
            if done else "⚠️ Не смог поправить сообщение (возможно, его удалили). "
                         "Отправь состав заново.")
    await query.edit_message_text(note + "\n\n" + screen, reply_markup=markup)


# Спорные фамилии из списка: {tg id: [{query, options}]}. Разбираем их ПОСЛЕ
# того, как прошли весь список, — тренер пишет состав одной строкой и не
# должен останавливаться на каждом однофамильце.
_roster_pending: Dict[int, List[Dict[str, Any]]] = {}


def _split_names(text: str) -> List[str]:
    """«Дроздов, Романов; Катюргин» -> три фамилии. Опрос бывает пустым (лига
    ещё не открыла игру), и тогда состав приходит списком в одном сообщении."""
    return [p.strip() for p in re.split(r"[,;\n]+", text or "") if p.strip()]


def _next_roster_question(user_id: int, source: str, game_id: str
                          ) -> Optional[Tuple[str, InlineKeyboardMarkup]]:
    """Экран для следующей неоднозначной фамилии, если такие остались."""
    queue = _roster_pending.get(user_id) or []
    if not queue:
        _roster_pending.pop(user_id, None)
        return None
    item = queue[0]
    rows = [[InlineKeyboardButton(p["title"][:60],
                                  callback_data=f"rost:add:{source}:{game_id}:{p['row']}")]
            for p in item["options"]]
    rows.append([InlineKeyboardButton("⏭ Пропустить",
                                      callback_data=f"rost:skip:{source}:{game_id}")])
    left = f" (осталось уточнить: {len(queue)})" if len(queue) > 1 else ""
    return (f"Кто из них «{item['query']}»?{left}", InlineKeyboardMarkup(rows))


async def handle_roster_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Фамилии от тренера: одна или списком через запятую.

    Однозначных добавляем сразу, спорных и ненайденных копим и разбираем в
    конце — иначе длинный список рвётся на первом же однофамильце."""
    import game_roster
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _roster_focus:
        return
    if not _can_see_reports(user):
        _roster_focus.pop(user.id, None)
        return
    source, game_id = _roster_focus[user.id]
    names = _split_names(msg.text or "")
    if not names:
        return

    added: List[str] = []
    missing: List[str] = []
    pending: List[Dict[str, Any]] = []
    for name in names:
        found = await asyncio.to_thread(game_roster.search, name)
        if len(found) == 1:
            await asyncio.to_thread(game_roster.add, source, game_id,
                                    found[0]["row"], str(user.id))
            added.append(found[0]["title"])
        elif found:
            pending.append({"query": name, "options": found})
        else:
            missing.append(name)
    _roster_pending[user.id] = pending

    head = []
    if added:
        head.append("➕ Добавил: " + ", ".join(added))
    if missing:
        head.append("Не нашёл в листе «Игроки»: " + ", ".join(missing))
    question = _next_roster_question(user.id, source, game_id)
    if question:
        text, markup = question
        await msg.reply_text(("\n".join(head) + "\n\n" if head else "") + text,
                             reply_markup=markup)
        raise ApplicationHandlerStop
    screen, markup = await asyncio.to_thread(_roster_screen, source, game_id)
    await msg.reply_text(("\n".join(head) + "\n\n" if head else "") + screen,
                         reply_markup=markup)
    raise ApplicationHandlerStop


async def handle_coach_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    import coach_payments
    query = update.callback_query
    user = query.from_user if query else None
    if not query or not _can_see_reports(user):
        if query:
            await query.answer("Нет доступа", show_alert=True)
        return
    await query.answer()
    parts = (query.data or "").split(":")
    what = parts[1] if len(parts) > 1 else "main"
    back = [InlineKeyboardButton("⬅️ Назад", callback_data="coach:main")]

    try:
        if what == "main":
            _awaiting_payment.discard(user.id)
            _pay_draft.pop(user.id, None)
            await query.edit_message_text(COACH_TEXT, reply_markup=_coach_markup())

        elif what == "prog":
            text, markup = await _render_prog_list(is_admin=_is_admin(user),
                                                   back="coach:main")
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "pay":
            _awaiting_payment.add(user.id)
            _pay_draft.pop(user.id, None)
            await query.edit_message_text(PAY_ASK)

        elif what == "games":
            text, markup = await asyncio.to_thread(_games_screen)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "sched":
            text, markup = await asyncio.to_thread(_sched_screen)
            await query.edit_message_text(text, reply_markup=markup,
                                          parse_mode="HTML")

        elif what == "setsched" and len(parts) > 2:
            key = parts[2]
            title = next((t for k, t, _ in SCHED_FIELDS if k == key), key)
            unit = next((u for k, _, u in SCHED_FIELDS if k == key), "")
            _awaiting_money[user.id] = f"sched:{key}"
            hint = ("Пришли число месяца (1–28)." if unit == "число месяца"
                    else "Пришли час (0–23)." if unit == "час"
                    else "Пришли количество дней.")
            await query.edit_message_text(
                f"🗓 {title}. Сейчас: {_sched_value(key)}.\n\n{hint}\n\n"
                "Передумал — /start.")

        elif what == "debts":
            text, markup = await asyncio.to_thread(_debts_screen)
            await query.edit_message_text(text, reply_markup=markup,
                                          parse_mode="HTML")

        elif what == "closedebt" and len(parts) > 2:
            import coach_payments
            await asyncio.to_thread(coach_payments.close_debt, int(parts[2]))
            await query.answer("Погашен")
            text, markup = await asyncio.to_thread(_debts_screen)
            await query.edit_message_text(text, reply_markup=markup,
                                          parse_mode="HTML")

        elif what == "adddebt":
            # Сначала кому, потом сколько: список тот же, что при оплате.
            markup = await asyncio.to_thread(_pay_players_markup, 0, "", "coach:debtwho")
            await query.edit_message_text("➕ Кому добавить долг?", reply_markup=markup)

        elif what == "debtwho" and len(parts) > 2:
            _awaiting_money[user.id] = f"debt:{parts[2]}"
            import coach_payments
            who = await asyncio.to_thread(coach_payments.player_by_row, int(parts[2]))
            await query.edit_message_text(
                f"➕ Долг для {(who or {}).get('title', '')}.\n\n"
                "Пришли сумму и за что: «500 мяч» или просто «500».\n\n"
                "Передумал — /start.")

        elif what == "sums":
            row = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
            text, markup = await asyncio.to_thread(_sums_screen, row)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "sumswho":
            markup = await asyncio.to_thread(_pay_players_markup, 0, "", "coach:sums")
            await query.edit_message_text("👤 Чьи суммы меняем?", reply_markup=markup)

        elif what == "setsum" and len(parts) > 3:
            _awaiting_money[user.id] = f"sum:{parts[2]}:{parts[3]}"
            what_title = ("взнос за тренировки" if parts[3] == "season"
                          else "оплату одной игры")
            await query.edit_message_text(
                f"✏️ Пришли новую сумму — {what_title}, только число.\n\n"
                "Передумал — /start.")

        elif what == "delpay":
            if len(parts) > 2 and parts[2].isdigit():
                import coach_payments
                ok = await asyncio.to_thread(coach_payments.delete, int(parts[2]))
                await query.answer("Удалил" if ok else "Уже удалён")
                # Лист «Оплаты» пересобирается из базы — иначе в сводке
                # осталась бы сумма, которой больше нет.
                asyncio.create_task(_rebuild_payments_sheet())
            text, markup = await asyncio.to_thread(_delpay_screen)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "train":
            period = parts[2] if len(parts) > 2 else ""
            text, markup = await asyncio.to_thread(_train_screen, period)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "trmark" and len(parts) > 3:
            import training_dues
            row, period = int(parts[2]), parts[3]
            rec = await asyncio.to_thread(training_dues.mark_paid, row, period,
                                          str(user.id))
            player = await asyncio.to_thread(coach_payments.player_by_row, row)
            await query.answer(f"Отметил: {(player or {}).get('title', '')}"
                               if not rec.get("duplicate") else "Уже отмечено")
            text, markup = await asyncio.to_thread(_train_screen, period)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "owe":
            text = await asyncio.to_thread(_pay_owe_text)
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([back]))

        elif what == "last":
            text = await asyncio.to_thread(_pay_last_text)
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([back]))

        elif what in ("who", "page"):
            draft = _pay_draft.get(user.id)
            if not draft:
                await query.edit_message_text(COACH_TEXT, reply_markup=_coach_markup())
                return
            page = int(parts[2]) if what == "page" and len(parts) > 2 else 0
            draft["stage"] = "who"
            _pay_draft[user.id] = draft
            _awaiting_payment.add(user.id)
            markup = await asyncio.to_thread(_pay_players_markup, page,
                                             draft.get("sender", ""))
            await query.edit_message_text(
                f"👤 Кому засчитать {draft['amount']} ₽?\n\n"
                "Выбери из списка или просто напиши фамилию.", reply_markup=markup)

        elif what == "pick" and len(parts) > 2:
            draft = _pay_draft.get(user.id)
            if not draft:
                await query.edit_message_text(COACH_TEXT, reply_markup=_coach_markup())
                return
            player = await asyncio.to_thread(coach_payments.player_by_row, int(parts[2]))
            if not player:
                await query.edit_message_text("Такого игрока в листе больше нет — "
                                              "открой список заново.",
                                              reply_markup=_coach_markup())
                return
            draft["row"] = player["row"]
            draft.pop("stage", None)
            _awaiting_payment.discard(user.id)
            # Цена игры бывает личной, поэтому тип платежа пересчитываем уже
            # под конкретного человека: 1350 у одного — сезон, у другого — игра.
            draft["kind"], draft["games"] = await asyncio.to_thread(
                coach_payments.classify, draft["amount"], player)
            _pay_draft[user.id] = draft
            text, markup = _pay_confirm(draft)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "kind":
            draft = _pay_draft.get(user.id)
            if not draft:
                await query.edit_message_text(COACH_TEXT, reply_markup=_coach_markup())
                return
            player = await asyncio.to_thread(coach_payments.player_by_row, draft["row"])
            price = coach_payments.game_price(player)
            # С явным типом (coach:kind:game) — из экрана «за что этот платёж»;
            # без него — переключатель на обычном экране подтверждения.
            want = parts[2] if len(parts) > 2 else (
                "season" if draft["kind"] == coach_payments.KIND_GAME else "game")
            if want == "season":
                draft.update(kind=coach_payments.KIND_SEASON, games=0)
            else:
                draft.update(kind=coach_payments.KIND_GAME,
                             games=max(1, draft["amount"] // price) if price else 1)
            _pay_draft[user.id] = draft
            text, markup = _pay_confirm(draft)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "save":
            draft = _pay_draft.pop(user.id, None)
            if not draft:
                await query.edit_message_text(COACH_TEXT, reply_markup=_coach_markup())
                return
            if draft["kind"] == coach_payments.KIND_UNKNOWN:
                await query.answer("Сначала выбери, за что платёж", show_alert=True)
                _pay_draft[user.id] = draft
                return
            await query.edit_message_text("⏳ Записываю…")
            rec = await asyncio.to_thread(
                coach_payments.record, draft["row"], draft["amount"], draft["kind"],
                draft["games"], draft["paid_at"], draft.get("bank", ""), "",
                str(user.id), draft.get("fp", ""))
            # Связку «подпись в СМС -> игрок» запоминаем ПОСЛЕ записи: тренер
            # мог переиграть выбор, и запомнить надо итоговое решение.
            if draft.get("sender") and not rec.get("duplicate"):
                await asyncio.to_thread(coach_payments.remember_sender,
                                        draft["sender"], draft["row"])
            text = await asyncio.to_thread(_pay_saved_text, rec)
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("💳 Внести ещё", callback_data="coach:pay")],
                back]))

        elif what == "noop":
            return

    except Exception as e:
        log.error(f"Раздел тренера ({what}): {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}",
                                      reply_markup=InlineKeyboardMarkup([back]))


def _pay_saved_text(rec: Dict[str, Any]) -> str:
    """Итог записи + выгрузка в лист. Лист — не хранилище: в базе платёж уже
    лежит, поэтому недоступный Google меняет только приписку внизу."""
    import coach_payments
    player = coach_payments.player_by_row(rec["player_row"])
    title = player["title"] if player else f"строка {rec['player_row']}"
    if rec.get("duplicate"):
        return (f"🔁 Такой платёж уже записан: {title}, {rec['amount']} ₽ "
                f"от {coach_payments._human_date(rec['paid_at'])}.\n\n"
                "Дважды одно и то же не провожу. Если это правда другой "
                "перевод — подожди минуту и введи руками: «Фамилия сумма».")
    what = (coach_payments.games_word(rec["games"])
            if rec["kind"] == coach_payments.KIND_GAME else "взнос за сезон")
    lines = [f"✅ Записал: {title} — {rec['amount']} ₽ ({what}).", ""]
    if player:
        bal = next((b for b in coach_payments.balances()
                    if b["row"] == player["row"]), None)
        if bal and bal["pay_season"]:
            lines.append(f"Сезон: {bal['paid_season']} из {bal['pay_season']} ₽"
                         + (f", не хватает {bal['debt']} ₽" if bal["debt"] else " — закрыт"))
        if bal and bal["paid_games"]:
            lines.append(f"Оплачено: {coach_payments.games_word(bal['paid_games'])}")
    try:
        sheet = _get_spreadsheet()
        pushed = coach_payments.push_pending(sheet)
        # Сводку пересобираем сразу: тренер идёт смотреть её именно после
        # того, как внёс платёж.
        coach_payments.build_summary_sheet(sheet)
        lines.append("")
        lines.append(f"В «Логи оплаты» ушло строк: {pushed}. Лист «Оплаты» пересобран."
                     if pushed else "Листы обновлю следующим заходом.")
    except Exception as e:
        log.warning(f"Листы оплат не обновились: {e}")
        lines += ["", "Платёж записан, но в таблицу пока не попал — "
                      "допишу его при следующей записи."]
    return "\n".join(lines)


_MANUAL_RE = re.compile(r"^\s*([А-ЯЁа-яё\-]{3,})\s+(\d{2,7})\s*$|"
                        r"^\s*(\d{2,7})\s+([А-ЯЁа-яё\-]{3,})\s*$")


async def _pay_ask_who(msg, user_id: int, draft: Dict[str, Any], head: str) -> None:
    """Просит выбрать игрока — кнопками или фамилией с клавиатуры.

    Держим ввод открытым: тренеру с телефона набрать «Дроздов» быстрее, чем
    листать список, а спецификация раздела допускает оба пути."""
    draft["stage"] = "who"
    _pay_draft[user_id] = draft
    _awaiting_payment.add(user_id)
    markup = await asyncio.to_thread(_pay_players_markup, 0, draft.get("sender", ""))
    await msg.reply_text(f"💳 {draft['amount']} ₽. {head}\n\n"
                         "Можно и просто написать фамилию.", reply_markup=markup)


async def _pay_show_confirm(msg, user_id: int, draft: Dict[str, Any],
                            player: Dict[str, Any]) -> None:
    import coach_payments
    draft["row"] = player["row"]
    draft.pop("stage", None)
    # Цена игры бывает личной, поэтому тип платежа уточняем под конкретного
    # человека: 1350 у одного — сезон, у другого — игра.
    draft["kind"], draft["games"] = await asyncio.to_thread(
        coach_payments.classify, draft["amount"], player)
    _pay_draft[user_id] = draft
    _awaiting_payment.discard(user_id)
    text_out, markup = _pay_confirm(draft)
    await msg.reply_text(text_out, reply_markup=markup)


async def handle_payment_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Текст после «Внести оплату»: СМС от банка, «Фамилия 900» или фамилия."""
    import coach_payments
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_payment:
        return
    if not _can_see_reports(user):
        _awaiting_payment.discard(user.id)
        return
    _awaiting_payment.discard(user.id)
    text = (msg.text or "").strip()

    # Ждём фамилию к уже разобранной сумме.
    draft = _pay_draft.get(user.id)
    if draft and draft.get("stage") == "who":
        # Тренер печатает фамилию (часто часть) — ищем как везде в боте, и
        # только если ничего, пробуем разобрать это как подпись из СМС.
        found = await asyncio.to_thread(coach_payments.search_players, text)
        if not found:
            found = await asyncio.to_thread(coach_payments.match_player, text)
        if len(found) == 1:
            await _pay_show_confirm(msg, user.id, draft, found[0])
        else:
            draft["sender"] = text
            head = (f"Под «{text}» подходят {len(found)} — выбери, кто это."
                    if found else f"Не нашёл «{text}» в составе. Выбери из списка.")
            await _pay_ask_who(msg, user.id, draft, head)
        raise ApplicationHandlerStop

    manual = _MANUAL_RE.match(text)
    if manual:
        surname = manual.group(1) or manual.group(4)
        amount = int(manual.group(2) or manual.group(3))
        parsed = {"amount": amount, "sender": surname, "paid_at": "",
                  "bank": "", "outgoing": False, "fingerprint": ""}
    else:
        parsed = await asyncio.to_thread(coach_payments.parse_sms, text)

    if not parsed["amount"]:
        _awaiting_payment.add(user.id)
        await msg.reply_text(
            "Не нашёл в этом тексте сумму. Пришли СМС целиком или напиши "
            "коротко: «Фамилия 900».")
        raise ApplicationHandlerStop

    dup = await asyncio.to_thread(coach_payments.already_recorded,
                                  parsed.get("fingerprint", ""))
    if dup:
        await msg.reply_text(await asyncio.to_thread(
            _pay_saved_text, {**dup, "duplicate": True, "games": 0}),
            reply_markup=_coach_markup())
        raise ApplicationHandlerStop

    kind, games = await asyncio.to_thread(coach_payments.classify, parsed["amount"])
    draft = {"amount": parsed["amount"], "sender": parsed["sender"],
             "paid_at": parsed["paid_at"] or date.today().isoformat(),
             "bank": parsed["bank"], "outgoing": parsed.get("outgoing", False),
             "fp": parsed.get("fingerprint", ""), "kind": kind, "games": games}
    _pay_draft[user.id] = draft

    # Кого уже размечали раньше — не спрашиваем повторно.
    known = await asyncio.to_thread(coach_payments.known_sender, parsed["sender"])
    if known:
        draft["recognized"] = True
        await _pay_show_confirm(msg, user.id, draft, known)
        raise ApplicationHandlerStop

    found = await asyncio.to_thread(coach_payments.match_player, parsed["sender"])
    if len(found) == 1:
        await _pay_show_confirm(msg, user.id, draft, found[0])
        raise ApplicationHandlerStop

    who = (f"«{parsed['sender']}»" if parsed["sender"] else "отправителя")
    head = (f"Нашёл {coach_payments.plural(len(found), 'подходящего', 'подходящих', 'подходящих')}"
            " — выбери, кто это."
            if found else f"Не понял, кто платил ({who}). Выбери из списка.")
    await _pay_ask_who(msg, user.id, draft, head)
    raise ApplicationHandlerStop


async def handle_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    user = update.effective_user
    if not query or not _is_admin(user):
        if query:
            await query.answer()
        return

    await query.answer()
    data = query.data or ""
    parts = data.split(":")
    if len(parts) < 2 or parts[0] != "admin":
        return

    try:
        if parts[1] == "doors":
            what = parts[2] if len(parts) > 2 else "list"
            if what == "toggle" and len(parts) > 3:
                import fantasy_api
                import sheets_cache
                key = next((setting for did, _, _, setting in fantasy_api.DOORS
                            if did == parts[3]), "")
                if key:
                    now = sheets_cache.get_int_setting(key, 1)
                    await asyncio.to_thread(sheets_cache.set_setting, key,
                                            0 if now else 1)
            text, markup = await _doors_screen()
            await query.edit_message_text(text, reply_markup=markup)
        elif parts[1] == "field":
            what = parts[2] if len(parts) > 2 else "list"
            if what == "pick" and len(parts) > 3:
                text, markup = await asyncio.to_thread(_field_card, int(parts[3]))
                await query.edit_message_text(text, reply_markup=markup)
            elif what == "set" and len(parts) > 4:
                _awaiting_field[user.id] = f"{parts[3]}:{parts[4]}"
                ask = ("🎂 Пришли дату рождения: «22.09.2001» или без года «22.09»."
                       if parts[4] == "bd" else
                       "✏️ Пришли ник — как к человеку обращаются в команде.")
                await query.edit_message_text(f"{ask}\n\nПередумал — /start.")
            else:
                _awaiting_field.pop(user.id, None)
                off = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
                text, markup = await asyncio.to_thread(_fields_screen, off)
                await query.edit_message_text(text, reply_markup=markup)
        elif parts[1] == "video":
            what = parts[2] if len(parts) > 2 else "list"
            if what == "auto" and len(parts) > 4:
                await asyncio.to_thread(game_timeline_drop, parts[3], parts[4])
                text, markup = await asyncio.to_thread(_video_screen)
                await query.edit_message_text(text, reply_markup=markup)
            elif what == "set" and len(parts) > 4:
                _awaiting_video[user.id] = f"{parts[3]}:{parts[4]}"
                await query.edit_message_text(VIDEO_ASK)
            else:
                _awaiting_video.pop(user.id, None)
                text, markup = await asyncio.to_thread(_video_screen)
                await query.edit_message_text(text, reply_markup=markup)
        elif parts[1] == "menu":
            screen = parts[2] if len(parts) > 2 else "main"
            if screen == "main":
                await query.edit_message_text("📊 Админ-панель", reply_markup=_main_menu_markup())
            elif screen == "launch":
                await query.edit_message_text("🚀 Запуск оповещений\nВыберите действие:", reply_markup=_launch_menu_markup())
            elif screen == "users":
                await query.edit_message_text("👥 Список пользователей", reply_markup=_users_menu_markup())
            elif screen == "log":
                await query.edit_message_text("📋 Лог действий", reply_markup=_log_menu_markup())
            elif screen == "reports":
                sub = parts[3] if len(parts) > 3 else None
                if sub == "training":
                    await query.edit_message_text("📊 Отчёты → Тренировки", reply_markup=_reports_training_menu_markup())
                elif sub == "games":
                    await query.edit_message_text("📊 Отчёты → Игры", reply_markup=_reports_games_menu_markup())
                else:
                    await query.edit_message_text("📊 Отчёты", reply_markup=_reports_menu_markup())
            elif screen == "fantasy":
                await query.edit_message_text(_fantasy_menu_text(), reply_markup=_fantasy_menu_markup())
            elif screen == "profile":
                await _send_profile(query.message, update.effective_user)
            elif screen == "stats":
                text, markup = _stats_screen()
                await query.edit_message_text(text, reply_markup=markup)
            elif screen == "config":
                await query.edit_message_text(
                    _config_screen_text(),
                    reply_markup=InlineKeyboardMarkup([_back_button("admin:menu:main")]))

        elif parts[1] == "fantasy":
            await _handle_fantasy_action(query, parts[2] if len(parts) > 2 else "",
                                         parts[3] if len(parts) > 3 else None)

        elif parts[1] == "fscope":
            await _handle_fantasy_scope(query, parts)

        elif parts[1] == "fpool":
            await _handle_fantasy_pool(query, parts)

        elif parts[1] == "run":
            action = parts[2]
            force = len(parts) > 3 and parts[3] == "force"
            await _handle_launch_action(query, action, force)

        elif parts[1] == "users":
            mode = parts[2]
            offset = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            if mode == "table":
                _refresh_db_cache()
                text, markup = _render_players_page(offset)
                await query.edit_message_text(text, reply_markup=markup)
            elif mode == "bot":
                text, markup = _render_bot_users_page(offset)
                await query.edit_message_text(text, reply_markup=markup)

        elif parts[1] == "log":
            mode = parts[2]
            offset = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            if mode == "bot":
                _refresh_db_cache()
                await query.edit_message_text(admin_panel.render_bot_log(since_days=1), reply_markup=_log_menu_markup())
            elif mode == "users":
                text, markup = _render_user_log_page(offset)
                await query.edit_message_text(text, reply_markup=markup)
            elif mode == "errors":
                text, markup = _render_errors_page(offset)
                await query.edit_message_text(text, reply_markup=markup)
            elif mode == "feedback":
                text, markup = _render_feedback_page(offset)
                await query.edit_message_text(text, reply_markup=markup)

        elif parts[1] == "report":
            kind, period = parts[2], parts[3]
            await _handle_report_action(query, kind, period)

        elif parts[1] == "acc":
            what = parts[2] if len(parts) > 2 else "list"
            if what == "add":
                kind = parts[3]
                _awaiting_coach[user.id] = kind
                await query.edit_message_text(
                    COACH_ASK.format(title=sheets_cache.ACCESS_TITLES.get(kind, kind)))
                return
            if what == "del":
                sheets_cache.revoke_access(parts[3], parts[4])
            text, markup = _render_access_list()
            await query.edit_message_text(text, reply_markup=markup)

        elif parts[1] == "link":
            what = parts[2] if len(parts) > 2 else "list"
            if what == "list":
                text, markup = _render_link_list()
            elif what == "free":
                text, markup = _render_link_free(int(parts[3]) if len(parts) > 3 else 0)
            elif what == "pick":
                text, markup = _render_link_pick(parts[3], int(parts[4]) if len(parts) > 4 else 0)
            elif what == "linked":
                text, markup = _render_link_linked(int(parts[3]) if len(parts) > 3 else 0)
            elif what == "un":
                text, markup = _render_unlink_confirm(parts[3])
            elif what == "un2":
                answer = await asyncio.to_thread(_do_unlink, parts[3])
                text, markup = _render_link_list()
                text = answer + "\n\n" + text
            elif what == "do":
                # Привязка ходит в Sheets — в фоновый поток, чтобы не морозить
                # обработчик кнопок на время сетевого запроса.
                answer = await asyncio.to_thread(_do_link, parts[3], int(parts[4]))
                text, markup = _render_link_list()
                text = answer + "\n\n" + text
            else:
                text, markup = _render_link_list()
            await query.edit_message_text(text, reply_markup=markup)

        elif parts[1] == "stats" and len(parts) > 2 and parts[2] == "ours":
            await query.edit_message_text(
                "⏳ Перекачиваю наши игры свежим парсером — пара минут.\n"
                "Пришлю результат отдельным сообщением.")
            asyncio.create_task(_refetch_our_games_now(query))

        elif parts[1] == "stats" and len(parts) > 2 and parts[2] == "now":
            await query.edit_message_text(
                "⏳ Перекачиваю игры без стадии — это займёт около минуты.\n"
                "Пришлю результат отдельным сообщением.")
            asyncio.create_task(_refetch_no_stage_now(query))

        elif parts[1] == "stats" and len(parts) > 2 and parts[2] == "refetch":
            import stats_backfill
            marked = await asyncio.to_thread(stats_backfill.forget_games_missing_fields, "slpro")
            marked += await asyncio.to_thread(stats_backfill.forget_games_without_stage, "slpro")
            await query.edit_message_text(
                f"♻️ Помечено к перекачке: {marked} "
                f"{_plural(marked, 'игра', 'игры', 'игр')}.\n\n"
                "Ночной бэкфилл (01:30 МСК) заберёт их порциями по 200 — "
                "примерно по игровой неделе за ночь.",
                reply_markup=InlineKeyboardMarkup([_back_button()]))

        elif parts[1] == "sync":
            await query.edit_message_text("⏳ Синхронизация...")
            push_result = _push_local_changes()
            try:
                pull_result = sheets_cache.sync_all(_get_spreadsheet())
            except Exception as e:
                pull_result = {"error": str(e)}
            sr = push_result.get("service_records", {})
            at = push_result.get("attendance", {})
            gv = push_result.get("game_votes", {})
            lines = [
                "✅ Синхронизация завершена",
                "",
                f"Выгружено в Sheets: события {sr.get('pushed', 0)} "
                f"(добавлено {sr.get('inserted', 0)}, обновлено {sr.get('updated', 0)}, "
                f"удалено {sr.get('deleted', 0)}), голоса тренировок {at.get('pushed', 0)} "
                f"(добавлено {at.get('inserted', 0)}, обновлено {at.get('updated', 0)}), "
                f"голоса игр {gv.get('pushed', 0)} "
                f"(добавлено {gv.get('inserted', 0)}, обновлено {gv.get('updated', 0)})",
                f"Забрано из Sheets: {pull_result}",
            ]
            await query.edit_message_text("\n".join(lines), reply_markup=_main_menu_markup())

    except Exception as e:
        log.error(f"Ошибка в админ-меню (callback_data={data!r}): {e}")
        sheets_cache.report_error("admin_menu", f"{data!r}: {e}", _get_spreadsheet())
        try:
            await query.edit_message_text("⚠️ Произошла ошибка, подробности в логах демона.", reply_markup=_main_menu_markup())
        except Exception:
            pass


async def handle_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    # Только личка с админом. Если ADMIN_USER_IDS не настроен — команда не работает нигде.
    if not user or not chat or chat.type != "private":
        return
    if not _is_admin(user):
        return
    _refresh_db_cache()
    _periodic_push_local_changes()
    await _send_main_menu(update)


async def handle_admin_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Нажатие постоянной кнопки '📊 Админ-панель' — то же самое, что /admin."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _is_admin(user):
        return
    _refresh_db_cache()
    _periodic_push_local_changes()
    await _send_main_menu(update)


BACKGROUND_TICK_SECONDS = 30
_background_task = None

# Пул фэнтези кешируется в памяти на час, но собирается походом в API двух лиг —
# секунды. Греем его сами, чтобы за это ждал фоновый цикл, а не игрок, открывший
# приложение. Чуть чаще, чем истекает кеш, — иначе окно, когда он уже протух.
_POOL_WARM_INTERVAL = 2400.0
_pool_warm_at: float = 0.0


_funnel_warm_at = 0.0
_FUNNEL_WARM_INTERVAL = 600      # 10 минут: Funnel остывает за минуты, не за часы


async def _keep_funnel_warm() -> None:
    """Не даёт публичному входу остыть. Холодный Funnel отвечает ~15с — столько
    же, сколько фронт готов ждать, и игрок сваливается в запасной режим, хотя
    канал рабочий."""
    global _funnel_warm_at
    if not (FANTASY_API_ENABLED and BOT_TOKEN):
        return
    now = time.time()
    if now - _funnel_warm_at < _FUNNEL_WARM_INTERVAL:
        return
    _funnel_warm_at = now
    await fantasy_api.keep_funnel_warm()


_league_sync_at: float = 0.0
_LEAGUE_SYNC_INTERVAL = 3600.0     # раз в час: заявки меняются реже некуда
# Сводку оплат пересобираем и по времени: суммы «сколько должен» тренер правит
# прямо в листе «Игроки», и долг в сводке должен идти за ними, а не ждать
# следующего платежа.
_PAY_SHEET_INTERVAL = 1800.0
_pay_sheet_at = 0.0


def _game_manager():
    """Экземпляр GameSystemManager — только ради разрешённого конфига чатов.
    Создаём по требованию: он лезет в Sheets, и держать его постоянно незачем."""
    from game_system_manager import GameSystemManager
    return GameSystemManager()


def _bot_of(gsm) -> Any:
    return getattr(gsm, "bot", None)


def _result_chat_ids(gsm) -> List[Any]:
    """Чаты, куда идут результаты — туда же и запись игры."""
    try:
        from game_system_manager import (get_chat_ids_for_automation,
                                         AUTOMATION_KEY_GAME_ANNOUNCEMENTS)
        entry = gsm._get_automation_entry(AUTOMATION_KEY_GAME_ANNOUNCEMENTS)
        ids = get_chat_ids_for_automation(AUTOMATION_KEY_GAME_ANNOUNCEMENTS, entry)
        return [gsm._to_int(c) or c for c in ids]
    except Exception as e:
        log.warning(f"Чаты для оповещения о записи не определились: {e}")
        return []


async def _sync_leagues() -> None:
    """Справочники лиг — единственное место демона, которое ходит в чужие API
    ради состава команд и имён. Всё остальное читает результат из базы и
    памяти, поэтому недоступная лига больше не задевает игрока."""
    global _league_sync_at
    now = time.time()
    if now - _league_sync_at < _LEAGUE_SYNC_INTERVAL:
        return
    _league_sync_at = now
    try:
        import league_sync
        import player_names
        res = await league_sync.refresh()
        extra = await league_sync.fill_missing_names()
        log.info(f"Справочники лиг: команд {res['teams']}, в заявках {res['rosters']}, "
                 f"имён {player_names.stats()['count']} (+{extra} из протоколов), "
                 f"склеек {res.get('merged', 0)}, ошибок {res['failed']}")
        # Записи игр из VK — тем же фоновым заходом. Не настроено (нет токена
        # или групп) — тихо ничего не делает. Нашли новую — сразу оповещаем:
        # запись ценна тем, что её можно посмотреть, а не узнать о ней потом.
        import vk_video
        gsm = _game_manager()
        vk = await vk_video.sync(bot=_bot_of(gsm),
                                 chat_ids=_result_chat_ids(gsm),
                                 topic_id=getattr(gsm, "game_announcement_topic_id", None))
        if vk.get("skipped"):
            log.info(f"VK: {vk['skipped']} — записи игр не ищу")
        elif vk["found"]:
            log.info(f"VK: найдено записей игр — {vk['found']} из {vk['looked']}, "
                     f"оповещений {vk['notified']}")
        else:
            log.info(f"VK: групп {len(__import__('vk_video').groups())}, "
                     f"просмотрено игр {vk['looked']}, записей не нашлось")
        # Ссылка на запись найдена — можно спросить у ВК, когда начался эфир, и
        # пересчитать тайм-коды по нему вместо оценки по расписанию.
        import game_timeline
        upgraded = await game_timeline.resync_offsets()
        if upgraded:
            log.info(f"Тайм-коды: сдвиг уточнён по эфиру у {upgraded} игр")
    except Exception as e:
        log.warning(f"Справочники лиг не обновились: {e}")


# Разбор старше этого срока в личку не шлём. Первый прогон иначе вывалит
# тренеру отчёты по всем играм, которые уже лежат в базе, — так уже было с
# записями матчей из VK.
COACH_REPORT_MAX_AGE_DAYS = 3


def _coach_recipients() -> List[str]:
    """Кому уходит разбор: админы и все, кому открыт раздел тренера.

    Доступ выдаётся по @нику, а числовой id проставляется при первом входе —
    у кого его ещё нет, тому и слать некуда."""
    ids = [str(a) for a in ADMIN_USER_IDS]
    try:
        for row in sheets_cache.access_list(sheets_cache.ACCESS_TEAM):
            uid = str(row.get("tg_user_id") or "").strip()
            if uid:
                ids.append(uid)
    except Exception as e:
        log.warning(f"Список тренеров не собрался: {e}")
    return list(dict.fromkeys(ids))


async def _coach_reports(app: Application) -> None:
    """Разбор последней игры — сам в личку тренеру, без нажатия кнопки.

    Раньше отчёт существовал только по кнопке «Прогресс команды»: тренер
    узнавал о разборе, только если сам за ним пришёл. Отправляем, когда
    статистика игры уже лежит в базе (иначе разбирать нечего), один раз на
    игру."""
    import team_progress
    recipients = await asyncio.to_thread(_coach_recipients)
    if not recipients:
        return
    today = date.today()
    for team in await _prog_teams():
        source, team_id = team["source"], team["team_id"]
        try:
            games = await asyncio.to_thread(team_progress.team_games, team_id, source, 3)
            last = next((g for g in games if team_progress.is_real_game(g)), None)
            if not last:
                continue
            game_id = str(last["game_id"])
            if await asyncio.to_thread(sheets_cache.coach_report_sent,
                                       source, team_id, game_id):
                continue
            try:
                age = (today - date.fromisoformat(str(last["date"])[:10])).days
            except ValueError:
                age = 0
            if age > COACH_REPORT_MAX_AGE_DAYS:
                # Старое помечаем отправленным молча: это не новость, но и
                # спотыкаться о неё каждый тик незачем.
                await asyncio.to_thread(sheets_cache.mark_coach_report,
                                        source, team_id, game_id)
                continue

            summary, detail = await _prog_build(source, team_id)
            if not detail.get("ok"):
                continue
            buf = await asyncio.to_thread(_prog_file, detail, team_id)
            page = buf.getvalue()
            sent = 0
            for uid in recipients:
                try:
                    await app.bot.send_message(chat_id=int(uid), text=summary)
                    doc = io.BytesIO(page)
                    doc.name = buf.name
                    await app.bot.send_document(chat_id=int(uid), document=doc,
                                                caption=REPORT_FILE_CAPTION)
                    sent += 1
                except Exception as e:
                    log.warning(f"Разбор не доставлен {uid}: {e}")
            # Помечаем в любом случае: не дошло — значит человек закрыл личку
            # или заблокировал бота, и повторять это каждые пять минут незачем.
            await asyncio.to_thread(sheets_cache.mark_coach_report,
                                    source, team_id, game_id)
            log.info(f"Разбор игры {source}:{game_id} ушёл тренерам: {sent} из "
                     f"{len(recipients)}")
        except Exception as e:
            log.error(f"Разбор для {source}:{team_id} не отправлен: {e}")


async def _pay_schedule(app: Application) -> None:
    """Напоминания об оплате тренировок по календарю.

    Что и когда — в training_dues.due_events(); здесь только отправка. Каждое
    событие помечается в pay_events, поэтому фоновый цикл, который тикает раз
    в полминуты, не превращает напоминание в спам."""
    import training_dues
    for key, period, kind in training_dues.due_events():
        if await asyncio.to_thread(training_dues.event_done, key):
            continue
        try:
            if kind in ("coach_end", "coach_warn"):
                when = "end" if kind == "coach_end" else "warn"
                text = await asyncio.to_thread(training_dues.coach_report, period, when)
                markup = InlineKeyboardMarkup([[InlineKeyboardButton(
                    "🏋️ Отметить оплату", callback_data=f"coach:train:{period}")]])
                sent = await _tell_coaches(app, text, markup)
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"тренерам: {sent}")
                log.info(f"Взносы {period}: {kind} ушло тренерам ({sent})")
            else:
                # «Заранее» шлём ВСЕМ активным, а не должникам: за следующий
                # месяц ещё никто не должен, и списка должников там просто нет.
                stat = await _remind_players(app, period,
                                             ahead=(kind == "player_ahead"))
                report = await asyncio.to_thread(
                    training_dues.delivery_report, period, stat["sent"],
                    stat["failed"], stat["unknown"])
                await _tell_coaches(app, report)
                await asyncio.to_thread(
                    training_dues.mark_event, key,
                    f"дошло {len(stat['sent'])}, не дошло "
                    f"{len(stat['failed']) + len(stat['unknown'])}")
                log.info(f"Взносы {period}: напоминание игрокам — дошло "
                         f"{len(stat['sent'])}, не дошло "
                         f"{len(stat['failed']) + len(stat['unknown'])}")
        except Exception as e:
            log.error(f"Напоминание об оплате ({key}) не ушло: {e}")


async def _game_schedule(app: Application) -> None:
    """Состав на игру и напоминания об оплате игр — по календарю матчей.

    За три дня тренер собирает состав, в день игры и на следующий получает
    список должников, вечером следующего дня их дёргает уже бот."""
    import game_roster
    import training_dues
    for key, game, kind in await asyncio.to_thread(game_roster.due_events):
        if await asyncio.to_thread(training_dues.event_done, key):
            continue
        source, gid = game["source"], game["game_id"]
        try:
            if kind == "collect":
                await asyncio.to_thread(game_roster.ensure_state, game)
                text, markup = await asyncio.to_thread(_roster_screen, source, gid)
                sent = await _tell_coaches(
                    app, f"🗓 Через {game_roster.COLLECT_BEFORE_DAYS} дня игра — "
                         f"собери состав.\n\n{text}", markup)
                await asyncio.to_thread(training_dues.mark_event, key, f"тренерам: {sent}")
                log.info(f"Состав на {source}:{gid} запрошен у тренеров ({sent})")
            elif kind in ("coach_day", "coach_next"):
                rows = await asyncio.to_thread(game_roster.debtors, source, gid)
                if not rows:
                    await asyncio.to_thread(training_dues.mark_event, key, "должников нет")
                    continue
                text, markup = await asyncio.to_thread(_game_debt_screen, source, gid)
                sent = await _tell_coaches(app, text, markup)
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"должников {len(rows)}, тренерам {sent}")
                log.info(f"Долги за игру {source}:{gid}: {len(rows)}, тренерам {sent}")
            elif kind == "player_next":
                stat = await _remind_game_debtors(app, game)
                report = await asyncio.to_thread(
                    training_dues.delivery_report,
                    game["date"].strftime("%Y-%m"), stat["sent"], stat["failed"],
                    stat["unknown"])
                await _tell_coaches(
                    app, f"💰 Напомнил про оплату игры {game_roster.game_label(game)}.\n\n"
                         + report.split("\n", 2)[-1])
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"дошло {len(stat['sent'])}")
        except Exception as e:
            log.error(f"Событие по игре ({key}) не отработало: {e}")


async def _remind_game_debtors(app: Application, game: Dict[str, Any]) -> Dict[str, List[str]]:
    import game_roster
    import training_dues
    stat: Dict[str, List[str]] = {"sent": [], "failed": [], "unknown": []}
    rows = await asyncio.to_thread(game_roster.debtors, game["source"], game["game_id"])
    for p in rows:
        uid = await asyncio.to_thread(training_dues.chat_id_of, p["row"])
        if not uid:
            stat["unknown"].append(p["title"])
            continue
        try:
            await app.bot.send_message(chat_id=int(uid),
                                       text=game_roster.player_debt_text(game, p))
            stat["sent"].append(p["title"])
        except Exception as e:
            log.info(f"Напоминание об игре {p['title']} не доставлено: {e}")
            stat["failed"].append(p["title"])
    return stat


async def _tell_coaches(app: Application, text: str,
                        markup: Optional[InlineKeyboardMarkup] = None) -> int:
    """Сообщение тренерскому штабу — ТОЛЬКО в личку, никогда в общий чат."""
    sent = 0
    for uid in await asyncio.to_thread(_coach_recipients):
        try:
            await app.bot.send_message(chat_id=int(uid), text=text,
                                       reply_markup=markup)
            sent += 1
        except Exception as e:
            log.warning(f"Тренеру {uid} не доставлено: {e}")
    return sent


async def _remind_players(app: Application, period: str,
                          ahead: bool = False) -> Dict[str, List[str]]:
    """Напоминание об оплате тренировок. Кому дошло, а кому нет и почему.

    ahead=True — рассылка 25-го числа про следующий месяц: адресаты не
    должники (их ещё нет), а все, с кого взнос ждём вообще."""
    import training_dues
    stat: Dict[str, List[str]] = {"sent": [], "failed": [], "unknown": []}
    people = (await asyncio.to_thread(training_dues.status, period) if ahead
              else await asyncio.to_thread(training_dues.debtors, period))
    for row in people:
        uid = await asyncio.to_thread(training_dues.chat_id_of, row["row"])
        if not uid:
            stat["unknown"].append(row["title"])
            continue
        try:
            await app.bot.send_message(
                chat_id=int(uid), text=training_dues.player_reminder(row, ahead))
            stat["sent"].append(row["title"])
        except Exception as e:
            log.info(f"Напоминание {row['title']} не доставлено: {e}")
            stat["failed"].append(row["title"])
    return stat


async def _personal_digests(app: Application) -> None:
    """«Присылать после каждой игры» — настройка была, отправки не было.

    Ждём, пока протокол игры окажется в базе (иначе разбирать нечего), и шлём
    короткий разбор в личку. Одна игра — одно сообщение, повторов нет."""
    import personal_game
    try:
        todo = await asyncio.to_thread(personal_game.pending)
    except Exception as e:
        log.warning(f"Личные разборы: список не собрался: {e}")
        return
    for item in todo:
        try:
            text = await asyncio.to_thread(
                personal_game.digest, item["source"], item["title"],
                item["player_id"], item["game"])
            if not text:
                await asyncio.to_thread(personal_game.mark_sent, item["key"], "пусто")
                continue
            await app.bot.send_message(chat_id=int(item["uid"]), text=text,
                                       parse_mode="HTML")
            await asyncio.to_thread(personal_game.mark_sent, item["key"], "отправлено")
            log.info(f"Личный разбор игры {item['source']}:{item['game']['game_id']} "
                     f"ушёл {item['uid']}")
        except Exception as e:
            # Заблокировал бота или закрыл личку — помечаем, чтобы не долбиться
            # в закрытую дверь каждые полминуты.
            log.info(f"Личный разбор не доставлен {item['uid']}: {e}")
            await asyncio.to_thread(personal_game.mark_sent, item["key"], f"ошибка: {e}")


async def _watch_broadcasts(app: Application) -> None:
    """Сторожит трансляции идущих матчей — тикает часто, работает редко.

    Сначала спрашиваем локальное расписание: вне окна матча (за 15 минут до
    начала и три часа после) не делаем ни одного запроса и не трогаем
    GameSystemManager — он лезет в Google, и строить его каждые полминуты
    было бы дороже самой задачи."""
    import vk_video
    try:
        candidates = await asyncio.to_thread(vk_video.live_candidates)
        if not candidates:
            return
        gsm = await asyncio.to_thread(_game_manager)
        res = await vk_video.watch_live(
            bot=_bot_of(gsm), chat_ids=_result_chat_ids(gsm),
            topic_id=getattr(gsm, "game_announcement_topic_id", None))
        if res["found"]:
            log.info(f"VK: трансляций найдено {res['found']}, "
                     f"оповещений {res['notified']}")
        elif res["watching"]:
            log.info(f"VK: смотрю трансляции по {res['watching']} игре(ам)")
    except Exception as e:
        log.warning(f"Сторож трансляций: {e}")


async def _refresh_pay_summary() -> None:
    global _pay_sheet_at
    now = time.time()
    if now - _pay_sheet_at < _PAY_SHEET_INTERVAL:
        return
    _pay_sheet_at = now
    try:
        import coach_payments
        sheet = _get_spreadsheet()
        await asyncio.to_thread(coach_payments.push_pending, sheet)
        await asyncio.to_thread(coach_payments.build_summary_sheet, sheet)
    except Exception as e:
        log.warning(f"Сводка оплат не пересобралась: {e}")


async def _warm_fantasy_pool(force: bool = False) -> None:
    global _pool_warm_at
    if not (FANTASY_API_ENABLED and BOT_TOKEN):
        return
    now = time.time()
    # force — прогрев по требованию (клавиатура застала пул холодным). Интервал
    # всё равно уважаем: если только что грелись и не вышло, лига лежит, и
    # долбить её на каждый /start незачем.
    if now - _pool_warm_at < (60.0 if force else _POOL_WARM_INTERVAL):
        return
    _pool_warm_at = now
    import fantasy
    try:
        pool = await fantasy_api.build_pool(force=True)
        log.info(f"Пул фэнтези прогрет: {len(pool)} игроков")
        # Связки «карточка -> строка листа» умеет проставить только тот, у кого
        # тёплый реестр имён, то есть демон. Пересчёт цен после игры идёт в
        # кроне, где имён нет, и без связок он молча не двигает ничего.
        import player_names as _pn
        if not _pn.is_cold():
            linked = await asyncio.to_thread(fantasy_api.remember_price_refs, pool)
            log.info(f"Связок карточка→строка листа: {linked} из {len(pool)}")
        # Ссылка игрока меняется, когда бот склеил его из двух лиг. Прогрев —
        # единственное место, где точно известен свежий пул, поэтому здесь же
        # приводим сохранённые составы к новому виду: иначе у человека внезапно
        # «состав не из пула», хотя он ничего не трогал.
        # Переезд составов на новые ссылки — только когда пул собран с именами.
        # На холодном реестре человек в пуле раздваивается (склейка по ФИО не
        # срабатывает), и такой «переезд» переписал бы всем сохранённые составы
        # на половинчатые ссылки.
        import player_names
        if player_names.is_cold():
            log.info("Пул собран без имён — составы не трогаю до прогрева реестра")
        else:
            moved = fantasy.migrate_refs({p["ref"] for p in pool})
            if moved:
                log.info(f"Составы фэнтези переведены на новые ссылки: {moved}")
    except Exception as e:
        log.warning(f"Не удалось прогреть пул фэнтези: {e}")


async def _background_loop(app: Application) -> None:
    """Единственный независимый таймер демона. Раньше _refresh_poll_cache/
    _refresh_db_cache/_periodic_push_local_changes срабатывали только
    попутно с входящим трафиком Telegram — во время матча без активности
    в чате это могло надолго задерживать и их, и (что важнее) вотчер
    результатов игр, которому нужно тикать независимо от чата."""
    log.info(f"Фоновый цикл запущен (тик каждые {BACKGROUND_TICK_SECONDS}с)")
    last_api = fantasy_api.public_api_url()
    while True:
        try:
            await asyncio.sleep(BACKGROUND_TICK_SECONDS)
            # В отдельном потоке: это синхронные походы в Google Sheets, а
            # прямо в цикле событий они замораживают ВЕСЬ демон — и приём
            # сообщений, и фэнтези-API. Раз в пять минут по несколько секунд
            # — ровно те паузы, которые человек в чате принимает за «бот завис».
            await asyncio.to_thread(_refresh_poll_cache)
            await asyncio.to_thread(_refresh_db_cache)
            await asyncio.to_thread(_periodic_push_local_changes)
            await _sync_leagues()
            await _coach_reports(app)
            await _pay_schedule(app)
            await _game_schedule(app)
            await _personal_digests(app)
            await _watch_broadcasts(app)
            await _refresh_pay_summary()
            await _warm_fantasy_pool()
            await _keep_funnel_warm()
            # Адрес Cloudflare-туннеля меняется при его рестарте (независимо от
            # демона). Заметив смену, пере-ставим кнопку меню на свежий адрес —
            # иначе «Открыть» вело бы на мёртвый туннель.
            api_now = fantasy_api.public_api_url()
            if api_now != last_api:
                last_api = api_now
                await _setup_menu_button(app)
                log.info(f"Кнопка меню обновлена под новый адрес API: {api_now or '(пусто -> Funnel)'}")
            await game_watcher.tick()
        except asyncio.CancelledError:
            log.info("Фоновый цикл остановлен")
            raise
        except Exception as e:
            # Один плохой тик не должен убивать демон и останавливать вотчер навсегда.
            log.error(f"Ошибка в фоновом цикле: {e}")
            sheets_cache.report_error("background_loop", str(e), _get_spreadsheet())


async def _startup_sheets_work() -> None:
    """Подготовка листов при старте: переименования, столбцы, отложенные
    платежи, сводка «Оплаты», гашение старых игровых оповещений."""
    import coach_payments
    try:
        sheet = _get_spreadsheet()
        # Переименования листов идут ПЕРВЫМИ: «Оплаты» переезжает в «Логи
        # оплаты», и только после этого имя «Оплаты» можно занять сводкой.
        renamed = await asyncio.to_thread(sheets_cache.rename_legacy_sheets, sheet)
        if renamed:
            log.info(f"Листы переименованы: {'; '.join(renamed)}")
        added = await asyncio.to_thread(coach_payments.ensure_player_columns, sheet)
        if added:
            log.info(f"Лист «Игроки»: добавлены столбцы {', '.join(added)}")
        moved = await asyncio.to_thread(coach_payments.migrate_active_marks, sheet)
        if moved:
            log.info(f"«Активность»: отметок переписано на «1» — {moved}")
        # Платежи, не дошедшие до листа в прошлый раз (Google был недоступен).
        left = await asyncio.to_thread(coach_payments.push_pending, sheet)
        if left:
            log.info(f"Лист «Логи оплаты»: дописано отложенных строк — {left}")
        people = await asyncio.to_thread(coach_payments.build_summary_sheet, sheet)
        if people:
            log.info(f"Лист «Оплаты» пересобран: игроков {people}")
        # Игры до появления порядка оплат — молчим по ним навсегда.
        import game_roster
        import training_dues
        muted = await asyncio.to_thread(game_roster.silence_old,
                                        training_dues.mark_event)
        if muted:
            log.info(f"Старые игры: платёжные оповещения погашены ({muted})")
        global _pay_sheet_at
        _pay_sheet_at = time.time()      # только что собрали — фону ждать свой срок
    except Exception as e:
        log.warning(f"Листы оплат не подготовлены: {e}")


async def on_startup(app: Application) -> None:
    log.info("=" * 50)
    log.info("Бот запущен (long-polling режим)")
    log.info(f"Время старта: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    log.info("=" * 50)
    sheets_cache.init_db()
    _refresh_poll_cache()
    _refresh_db_cache()
    _periodic_push_local_changes()

    async def _startup_sheets() -> None:
        """Разовая работа с таблицей при старте — ОТДЕЛЬНОЙ задачей.

        После перезагрузки машины сеть встаёт не мгновенно, а у gspread нет
        таймаута: запрос повисает, и вместе с ним повисал весь старт — бот
        отвечал в Telegram, но фэнтези-API и фоновый цикл не поднимались
        вовсе, молча. Ровно это случилось 06.08.2026. Теперь всё, что нужно
        людям, стартует сразу, а таблица догоняет когда сможет."""
        await _startup_sheets_work()

    _sheets_task = asyncio.create_task(_startup_sheets())
    _side_tasks.add(_sheets_task)
    _sheets_task.add_done_callback(_side_tasks.discard)

    # Список команд у игрока и у админа разный: личная статистика и админка —
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Меню бота"),
            BotCommand("joke", "Шутка к фамилии игрока"),
            BotCommand("feedback", "Написать админам: идея или проблема"),
        ], scope=BotCommandScopeDefault())
        for admin_id in ADMIN_USER_IDS:
            try:
                await app.bot.set_my_commands([
                    BotCommand("start", "Меню бота"),
                    BotCommand("admin", "Админ-панель"),
                    BotCommand("profile", "Мой прогресс (скрытое)"),
                    BotCommand("season", "Создать сезон фэнтези"),
                    BotCommand("joke", "Шутка к фамилии игрока"),
                    BotCommand("feedback", "Написать админам: идея или проблема"),
                ], scope=BotCommandScopeChat(chat_id=int(admin_id)))
            except Exception as e:
                log.warning(f"Команды для админа {admin_id}: {e}")
    except Exception as e:
        log.warning(f"Не удалось зарегистрировать список команд: {e}")

    await _setup_menu_button(app)

    global _background_task
    _background_task = asyncio.create_task(_background_loop(app))

    # Справочники и пул греем сразу, а не через полминуты первого тика:
    # перезапуск демона — обычное дело, и ровно в это окно приходят первые
    # /start. Реестр имён живёт в памяти и после рестарта пуст, поэтому
    # качалку зовём первой — иначе пул соберётся на номерах.
    async def _boot_warm() -> None:
        await _sync_leagues()
        await _warm_fantasy_pool(force=True)

    _warm = asyncio.create_task(_boot_warm())
    _side_tasks.add(_warm)
    _warm.add_done_callback(_side_tasks.discard)

    # Фэнтези-API в том же event loop (localhost; наружу — Tailscale Funnel).
    global _fantasy_runner
    if FANTASY_API_ENABLED and BOT_TOKEN:
        try:
            from aiohttp import web
            fapp = fantasy_api.create_app(BOT_TOKEN)
            # access_log=None: адрес публичный, сканеры найдут его за часы, а в
            # строке запроса может оказаться подпись — незачем это писать в лог.
            _fantasy_runner = web.AppRunner(fapp, access_log=None)
            await _fantasy_runner.setup()
            site = web.TCPSite(_fantasy_runner, "127.0.0.1", FANTASY_API_PORT)
            await site.start()
            log.info(f"Фэнтези-API поднят на 127.0.0.1:{FANTASY_API_PORT}")
        except Exception as e:
            log.error(f"Не удалось поднять фэнтези-API: {e}")
            _fantasy_runner = None


async def on_shutdown(app: Application) -> None:
    global _background_task, _fantasy_runner
    if _background_task:
        _background_task.cancel()
        try:
            await _background_task
        except asyncio.CancelledError:
            pass
    if _fantasy_runner:
        try:
            await _fantasy_runner.cleanup()
        except Exception:
            pass
    log.info("Бот остановлен.")


def main() -> None:
    if not BOT_TOKEN:
        log.error("BOT_TOKEN не задан в .env")
        sys.exit(1)
    if not ADMIN_USER_IDS:
        log.warning("ADMIN_USER_IDS не задан — команда /admin будет недоступна никому")

    # Трафик бота идёт через VPN-туннель с обфускацией (обход блокировки Telegram
    # провайдером), что добавляет джиттер задержки — дефолтные таймауты httpx
    # (5 сек) иногда не успевают, поднимаем их с запасом.
    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(20)
        .read_timeout(20)
        .write_timeout(20)
        .pool_timeout(20)
        # Обновления обрабатываем параллельно. По умолчанию PTB берёт их строго
        # по одному: один медленный обработчик (сходил в API лиги, а тот молчит)
        # держал очередь, и ждали ВСЕ, кто написал боту после него.
        .concurrent_updates(True)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(PollAnswerHandler(handle_poll_answer))
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("admin", handle_admin))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_fantasy_webapp_data))
    app.add_handler(MessageHandler(filters.Text([ADMIN_KEYBOARD_LABEL]), handle_admin_button))
    app.add_handler(MessageHandler(filters.Text([PROGRESS_KEYBOARD_LABEL]),
                                   handle_progress_button))
    app.add_handler(MessageHandler(filters.Text([COACH_KEYBOARD_LABEL]),
                                   handle_coach_button))
    app.add_handler(MessageHandler(filters.Text([MYSTATS_KEYBOARD_LABEL]),
                                   handle_mystats_button))
    app.add_handler(MessageHandler(filters.Text([FEEDBACK_KEYBOARD_LABEL]), handle_feedback_button))
    app.add_handler(MessageHandler(filters.Text([MENU_KEYBOARD_LABEL]), handle_menu_button))
    app.add_handler(CommandHandler("profile", handle_my_profile))
    app.add_handler(CommandHandler("season", handle_season))
    app.add_handler(CommandHandler("feedback", handle_feedback))
    app.add_handler(CommandHandler("joke", handle_joke_command))
    app.add_handler(CallbackQueryHandler(handle_report_prefs_callback, pattern=r"^rep:(cmp|ntf|met|mets|allmet|deep|back|file|vid|vidg)"))

    # Обработчики, которые смотрят ЛЮБОЙ текст в личке, — каждый в своей группе.
    # Из одной группы python-telegram-bot выполняет ровно один подошедший
    # обработчик («Only a max of 1 handler per group is handled»), поэтому в
    # общей группе они затирали друг друга: приём ссылки на профиль перестал
    # работать, как только рядом появилась обратная связь. Каждый сам решает,
    # его ли это сообщение, а тот, кто его забрал, поднимает
    # ApplicationHandlerStop — и до остальных дело не доходит.
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_coach_nick), group=1)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_joke_text), group=4)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_feedback_text), group=2)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_profile_link), group=3)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_payment_text), group=5)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_roster_text), group=6)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_video_text), group=7)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_field_text), group=8)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_money_text), group=9)
    app.add_handler(CallbackQueryHandler(handle_admin_callback, pattern=r"^admin:"))
    app.add_handler(CallbackQueryHandler(handle_prog_callback, pattern=r"^prog:"))
    app.add_handler(CallbackQueryHandler(handle_joke_callback, pattern=r"^joke:"))
    app.add_handler(CallbackQueryHandler(handle_menu_callback, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(handle_coach_callback, pattern=r"^coach:"))
    app.add_handler(CallbackQueryHandler(handle_roster_callback, pattern=r"^rost:"))

    log.info("Запуск polling...")
    app.run_polling(
        allowed_updates=["poll_answer", "message", "callback_query"],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
