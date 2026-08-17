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
            # Опоздавший «Готов» по уже отправленному составу — сказать тренеру.
            # Иначе человек считает, что заявился, а его в списке нет: состав
            # в чате был собран до его голоса.
            if vote_type == "PRESENT":
                await _late_ready(context, poll_info["game_id"],
                                  user_id, first_name, last_name, username)
    except Exception as e:
        log.error(f"Ошибка при сохранении голоса: {e}")
        sheets_cache.report_error("handle_poll_answer", str(e), _get_spreadsheet())


async def _late_ready(context, game_id: str, user_id: str, first_name: str,
                      last_name: str, username: str) -> None:
    """«Готов» пришёл после того, как состав уже ушёл в чат.

    Экран состава — снимок: тренер его открыл, а голос прилетел позже. 11.08.2026
    так вышло с Морозовым — он нажал «Готов» в ту же минуту, когда уходил
    состав, и в список не попал. Молчать тут нельзя: человек уверен, что
    заявился."""
    import game_roster
    try:
        source = game_roster.source_of(game_id)
        if not await asyncio.to_thread(game_roster.is_posted, source, str(game_id)):
            return
        picked = {p["row"] for p in
                  await asyncio.to_thread(game_roster.roster, source, str(game_id))}
        link = await asyncio.to_thread(sheets_cache.get_player_link, str(user_id))
        row = int((link or {}).get("player_row") or 0)
        if row and row in picked:
            return                      # он и так в составе — всё в порядке
        who = " ".join(x for x in (last_name, first_name) if x) or f"@{username}"
        game = next((g for g in await asyncio.to_thread(game_roster.games)
                     if g["source"] == source and g["game_id"] == str(game_id)), None)
        label = game_roster.game_label(game) if game else str(game_id)
        await _tell_coaches(
            context.application,
            f"🔔 {who} отметился «Готов» на игру {label} уже ПОСЛЕ того, как "
            f"состав ушёл в чат.\n\nЕсли берём — добавь в состав и обнови "
            f"сообщение, иначе он ждёт игру, а его там нет.",
            InlineKeyboardMarkup([[InlineKeyboardButton(
                "👥 Открыть состав",
                callback_data=f"rost:open:{source}:{game_id}")]]))
    except Exception as e:
        log.warning(f"Опоздавший «Готов» ({game_id}): {e}")


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
    """Личная статистика — платный закрытый раздел, только по выдаче.

    Человек оплачивает, получает чек, и тренер (или админ) открывает ему доступ
    до конкретного числа. Принадлежность к команде сама по себе ничего не
    открывает: играть и покупать разбор своих игр — разные вещи.

    Доступ снимается сам по сроку (purge_expired_access и проверка на каждом
    входе), поэтому продление — обычное действие, а не исключение."""
    return _has_access(sheets_cache.ACCESS_PERSONAL, user)


# Подписи кнопок нижней клавиатуры. Она постоянная (is_persistent) и висит под
# полем ввода независимо от того, куда пролистан чат, — команд стало много, и
# держать их под рукой удобнее, чем искать сообщение с меню.
# Сроки доступа: на игру, на турнир, на месяц — и бессрочно.
ACCESS_PERIODS = [("На 1 день", 1), ("На неделю", 7), ("На месяц", 30),
                  ("Бессрочно", 0)]

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
    # Свой ли это человек: от этого зависят и «Меню», и запасная кнопка.
    try:
        is_member = is_admin or fantasy_api._is_team_member(uid, user.username or "")
    except Exception as e:
        log.warning(f"проверка состава для клавиатуры: {e}")
        is_member = is_admin
    # Кнопку личной статистики видят все свои, а не только оплатившие: за ней
    # для непривязанного лежит приглашение привязаться, а сам разбор закрыт
    # отдельно. Иначе про раздел узнавал бы только тот, кому его уже открыли, —
    # так и вышло: из 46 человек команды привязались двое.
    with_personal = _can_see_personal(user) or is_member
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
    _newgame.pop(user.id, None)

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


# Приглашение привязаться. Один текст на все входы: его показывает и /profile,
# и экран без привязки, и разойтись эти формулировки не должны — человек
# сверяет присланную ссылку с образцом буквально.
_LINK_INVITE = (
    "У тебя пока не привязан ни один профиль.\n\n"
    "Пришли мне ссылку на свою страницу в лиге — например:\n"
    "• https://slpro.basketstat.ru/player/XXXX\n"
    "• https://www.fbp.ru/player.html?personId=XXXXXX&apiUrl=https://reg.infobasket.su\n\n"
    "Найти её просто: открой себя на сайте лиги и скопируй адрес из строки "
    "браузера.\n\n"
    "Привязка бесплатная — я запомню твой номер в лиге и посчитаю, сколько "
    "твоих игр у меня уже есть."
)


def _progress_or_teaser(user, source: str, player_id: str) -> str:
    """Что показать сразу после привязки — разбор или предложение его купить.

    Привязаться может любой, но разбор платный, поэтому здесь развилка. Тому,
    у кого доступа нет, показываем ровно один факт: сколько его игр уже лежит
    в базе и за какой срок. Это не часть продукта — это охват, и он же лучшая
    причина купить: «мои игры у него есть, я хочу их увидеть». Средние, форма,
    броски и таймкоды остаются за замком."""
    if _can_see_personal(user):
        return _format_progress(source, player_id)
    import fantasy_stats
    import player_identity
    s = fantasy_stats.career_summary(source, player_id)
    title = player_identity.SOURCE_TITLES.get(source, source)
    if not s.get("games"):
        return (f"• {title}: игр пока не нашёл. Если ты играешь в турнире, который "
                f"бот ещё не зеркалит, они появятся после ближайшего обновления.")
    return "\n".join([
        f"• {title}: твоих игр в базе — {s['games']} "
        f"({s['first_date']} … {s['last_date']}).",
        "",
        "🔒 Разбор пока закрыт. В нём: средние и форма, броски, как ты играешь "
        "против конкретных соперников, отчёт после каждой игры, файл за месяц "
        "и таймкоды — где именно смотреть себя в записи.",
        "Открыть доступ — через «💬 Написать админам».",
    ])


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
                f"➖ {names.get(ref, ref)}"[:BTN_TEXT],
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
        rows.append([InlineKeyboardButton(f"➕ {p.get('name') or p['ref']}"[:BTN_TEXT],
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
    личке и не должен отвечать на обычную переписку.

    Привязка открыта всем и бесплатна — раньше её мог сделать только админ, и
    из 46 человек команды привязались двое: остальным бот на присланную ссылку
    просто молчал. Сама привязка ничего не раскрывает, бот лишь запоминает
    публичный номер человека в лиге; платный разбор закрыт отдельно
    (_can_see_personal) и на каждом входе, включая рассылки."""
    msg = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    import player_identity
    parsed = None
    for word in (msg.text or "").split():
        parsed = player_identity.parse_profile_link(word)
        if parsed:
            break
    if not parsed:
        return

    # Тренер нажал «Привязать по ссылке» на чьей-то карточке — ссылка идёт
    # тому человеку, а не тому, кто её прислал. Развилка здесь, а не отдельным
    # обработчиком: тот пришлось бы ставить в группу раньше этой, и стоило бы
    # ошибиться с номером — админ привязал бы игрока к себе.
    row = _awaiting_identity.pop(user.id, None)
    if row is not None and _is_admin(user):
        done = await asyncio.to_thread(_identity_set, int(row), parsed["source"],
                                       parsed["player_id"], str(user.id))
        if not done:
            await msg.reply_text("❌ Не к кому привязывать: у человека нет "
                                 "числового id — он ещё не писал боту.")
            return
        text, markup = await asyncio.to_thread(_identity_who, int(row))
        await msg.reply_text(f"✅ Привязал.\n\n{text}", reply_markup=markup,
                             disable_web_page_preview=True)
        return

    # Первую привязку человек делает сам, смену — нет. Иначе один оплативший
    # перецепляется на товарища по команде и пересказывает ему платный разбор,
    # и подписка расходится по кругу.
    #
    # Смену держит АДМИН, а не тренер: тренер рядом с командой каждый день, и
    # просьбу «перецепи на минутку» ему проще выполнить, чем отказать. Экран
    # «🎯 Профили в лигах» живёт в админ-панели и тренеру недоступен.
    if not _is_admin(user):
        have = {r["source"]: str(r["player_id"])
                for r in player_identity.get_identities(user.id)}
        old = have.get(parsed["source"])
        if old and old != str(parsed["player_id"]):
            await msg.reply_text(
                "🔒 Профиль уже привязан, и сменить его может только админ.\n\n"
                "Так сделано, чтобы платный разбор нельзя было передавать по "
                "цепочке. Если привязка ошибочная — нажми «💬 Написать "
                "админам», поправят.")
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
        await msg.reply_text(
            _progress_or_teaser(user, parsed["source"], parsed["player_id"]))
        return

    text = head + "\n\n" + _progress_or_teaser(user, parsed["source"],
                                               parsed["player_id"])
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

    Разбор платный, но молчать в ответ нельзя: раньше человек без доступа не
    получал ничего и уходил, не узнав, что привязка вообще существует. Теперь
    без доступа он видит, как привязаться и что за этим стоит, — а сам разбор
    по-прежнему закрыт."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    if not _can_see_personal(user):
        await _send_profile_locked(update.message, user)
        return
    await _send_profile(update.message, user)


async def _send_profile_locked(message, user) -> None:
    """То же место для того, кому разбор ещё не открыт: привязка и охват."""
    import player_identity
    ids = player_identity.get_identities(user.id)
    if not ids:
        await message.reply_text(_LINK_INVITE)
        return
    parts = ["📊 Твой профиль", ""]
    for rec in ids:
        parts.append(_progress_or_teaser(user, rec["source"], rec["player_id"]))
    await message.reply_text("\n".join(parts).strip())


async def _send_profile(message, user) -> None:
    """Личный прогресс: и по команде /profile, и по кнопке в админке."""
    import player_identity
    ids = player_identity.get_identities(user.id)
    if not ids:
        await message.reply_text(_LINK_INVITE)
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
    проверки на админа — но и чужие не тронуть: пишем по id нажавшего.

    Доступ к разделу проверяем всё равно: кнопки под старым сообщением живут
    вечно, и без проверки закрывшийся срок ничего бы не закрывал — человек
    доставал бы таймкоды и разбор из переписки."""
    query = update.callback_query
    if not query or not query.from_user:
        return
    if not _can_see_personal(query.from_user):
        await query.answer("Доступ к личной статистике закончился. "
                           "Продлить — у тренера.", show_alert=True)
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


# Разделы админки. Было восемнадцать кнопок одним столбцом — столько в память
# не помещается, и нужное искали перечитыванием всего списка. Сгруппировано по
# тому, ЗАЧЕМ туда идут: разослать, разобрать игру, поправить человека,
# посмотреть отчёт, починить бота. Один экран вглубь — плата за то, что список
# читается целиком.
def _main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Запуск оповещений", callback_data="admin:menu:launch")],
        [InlineKeyboardButton("🏀 Игры и записи", callback_data="admin:menu:games")],
        [InlineKeyboardButton("👥 Люди и доступы", callback_data="admin:menu:people")],
        [InlineKeyboardButton("📊 Отчёты", callback_data="admin:menu:reports")],
        [InlineKeyboardButton("🏆 Фэнтези лига", callback_data="admin:menu:fantasy")],
        [InlineKeyboardButton("⚙️ Обслуживание", callback_data="admin:menu:service")],
    ])


def _games_menu_markup() -> InlineKeyboardMarkup:
    """Всё, что про сыгранное: записи, тайм-коды, полнота протоколов."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ Записи игр", callback_data="admin:video:list")],
        [InlineKeyboardButton("🎬 Тайм-коды за любого", callback_data="admin:tc:games")],
        [InlineKeyboardButton("🗄 Статистика лиг", callback_data="admin:menu:stats")],
        _back_button(),
    ])


def _people_menu_markup() -> InlineKeyboardMarkup:
    """Всё, что про конкретного человека: кто он для бота и что ему открыто."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Список пользователей", callback_data="admin:menu:users")],
        [InlineKeyboardButton("🔗 Опознание игроков", callback_data="admin:link:list")],
        [InlineKeyboardButton("🎯 Профили в лигах", callback_data="admin:idn:list:0")],
        [InlineKeyboardButton("🎂 Дни рождения и ники", callback_data="admin:field:list:0")],
        [InlineKeyboardButton("🔑 Доступы", callback_data="admin:acc:list")],
        _back_button(),
    ])


def _service_menu_markup() -> InlineKeyboardMarkup:
    """Обслуживание бота: сюда идут, когда что-то пошло не так."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 Синхронизация", callback_data="admin:sync")],
        [InlineKeyboardButton("💾 Резервная копия", callback_data="admin:backup")],
        [InlineKeyboardButton("📋 Лог действий", callback_data="admin:menu:log")],
        [InlineKeyboardButton("🧾 Что бот прочитал в Конфиге", callback_data="admin:menu:config")],
        [InlineKeyboardButton("🔌 Каналы связи", callback_data="admin:doors:list")],
        _back_button(),
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
    rows.append([InlineKeyboardButton("🔄 Перекачать наши игры",
                                      callback_data="admin:stats:ours")])
    if stale or no_stage:
        lines.append("Пометить их — и ночной бэкфилл перекачает протоколы "
                     "порциями по 200 за ночь. Уже скачанное не трогается.")
        rows.append([InlineKeyboardButton(
            f"♻️ Пометить к перекачке ({stale + no_stage})",
            callback_data="admin:stats:refetch")])
    rows.append(_back_button("admin:menu:games"))
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
        _back_button("admin:menu:service"),
    ])


def _users_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 По таблице", callback_data="admin:users:table:0")],
        [InlineKeyboardButton("🤖 В боте", callback_data="admin:users:bot:0")],
        _back_button("admin:menu:people"),
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
            f"{'🔴 Выключить' if door['enabled'] else '🟢 Включить'} {door['title']}"[:BTN_TEXT],
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


async def _remind_debtors(query, kind: str) -> Tuple[int, int]:
    """Ручная рассылка должникам: тренировки или игры. (дошло, не дошло).

    Автоматика присылает такое по календарю, но деньги собирают когда удобно —
    тренеру нужна кнопка «прямо сейчас», а не ожидание следующего срока."""
    import coach_payments
    import game_roster
    import training_dues
    targets: List[Tuple[int, str]] = []
    if kind == "season":
        period = training_dues.period_of(date.today())
        for row in await asyncio.to_thread(training_dues.debtors, period):
            targets.append((row["row"], training_dues.player_reminder(row)))
    else:
        for d in await asyncio.to_thread(game_roster.game_debts):
            games_word = _plural(d["games"], "игру", "игры", "игр")
            targets.append((d["row"],
                            f"🏀 За {d['games']} {games_word} не закрыта оплата — "
                            f"{d['amount']} ₽.\n\nРеквизиты у тренера."))
    sent = skipped = 0
    for row, text in targets:
        uid = await asyncio.to_thread(training_dues.chat_id_of, row)
        if not uid:
            skipped += 1
            continue
        try:
            await query.get_bot().send_message(chat_id=int(uid), text=text)
            sent += 1
        except Exception as e:
            log.info(f"Напоминание строке {row} не доставлено: {e}")
            skipped += 1
    return sent, skipped


def _clear_pending(uid: int) -> None:
    """Сбрасывает все незаконченные диалоги пользователя.

    Обработчики текста разложены по группам и срабатывают по очереди: у кого
    в его словаре есть этот id, тот текст и забирает. Незакрытый диалог оплаты
    перехватывал название команды при создании игры — бот искал «Тосно» среди
    игроков и отвечал, что такого нет."""
    _awaiting_payment.discard(uid)
    _pay_draft.pop(uid, None)
    _awaiting_video.pop(uid, None)
    _awaiting_field.pop(uid, None)
    _awaiting_money.pop(uid, None)
    _newgame.pop(uid, None)
    _roster_focus.pop(uid, None)
    _awaiting_access.pop(uid, None)
    _debt_draft.pop(uid, None)
    _awaiting_priv.pop(uid, None)
    _awaiting_guest.pop(uid, None)
    _awaiting_identity.pop(uid, None)


def _start_games_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Игры, по которым можно смотреть стартовый состав — с уже собранным."""
    import coach_payments
    import game_roster
    from datetime_utils import get_moscow_time
    now = get_moscow_time()
    # Только предстоящие: стартовая пятёрка — решение ПЕРЕД игрой. По сыгранной
    # выбирать нечего, а в списке она путается с ближайшей.
    games = []
    for g in game_roster.games(from_day=now.date()):
        start = game_roster._game_start(g)
        if start and start < now:
            continue
        if not game_roster.roster(g["source"], g["game_id"]):
            continue
        games.append(g)
    rows = [[InlineKeyboardButton(
        f"{coach_payments._human_date(g['date'].isoformat())} · {g['opponent']}"[:BTN_TEXT],
        callback_data=f"coach:start:{g['source']}:{g['game_id']}:name")]
        for g in games[:8]]
    rows.append([InlineKeyboardButton("⬅️ К играм", callback_data="coach:play")])
    head = ("🏁 Стартовый состав\n\nВыбери игру:" if games else
            "🏁 Стартовый состав\n\nБлижайших игр с собранным составом нет. "
            "По сыгранным пятёрку не выбирают.")
    return head, InlineKeyboardMarkup(rows)


def _start_screen(source: str, game_id: str, sort: str,
                  roles: bool = False) -> Tuple[str, InlineKeyboardMarkup]:
    """Выбор стартовой пятёрки. Нажатие на фамилию ставит в старт и снимает.

    Раньше этот экран умел ровно одно — править амплуа, а самой пятёрки не
    было: тренер жал фамилию, у него спрашивали позицию, и на этом всё
    заканчивалось. Название обещало другое.

    Амплуа никуда не делось, но ушло за отдельную кнопку: в обычном режиме
    тренер собирает пятёрку, а позиции правит редко и отдельно."""
    import coach_lineup
    data = coach_lineup.lineup(source, game_id, sort)
    picked = data.get("start") or []
    rows: List[List[InlineKeyboardButton]] = []

    if roles:
        for p in data["rows"][:12]:
            title = coach_lineup.role_title(p["role"])
            rows.append([InlineKeyboardButton(
                f"🎽 {p['title']} — {title or 'без амплуа'}"[:BTN_TEXT],
                callback_data=f"coach:role:{p['row']}:{source}:{game_id}:{sort}")])
        rows.append([InlineKeyboardButton(
            "⬅️ К пятёрке", callback_data=f"coach:start:{source}:{game_id}:{sort}")])
        return ("🎽 Амплуа\n\nНажми на игрока, чтобы поменять позицию.",
                InlineKeyboardMarkup(rows))

    for p in data["rows"][:14]:
        mark = "✅" if p["row"] in picked else "⬜"
        num = coach_lineup.role_number(p["role"])
        tail = f" · №{num}" if num else ""
        rows.append([InlineKeyboardButton(
            f"{mark} {p['title']}{tail}"[:BTN_TEXT],
            callback_data=f"coach:sf:{source}:{game_id}:{p['row']}:{sort}")])
    # Порядок списка — одной строкой: подписи короткие, чтобы три кнопки в
    # ряду не обрезались на телефоне.
    rows.append([InlineKeyboardButton(
        ("✅ " if data["sort"] == key else "") + title,
        callback_data=f"coach:start:{source}:{game_id}:{key}")
        for key, title in coach_lineup.SORTS.items()])
    rows.append([InlineKeyboardButton(
        "🎽 Амплуа", callback_data=f"coach:roles:{source}:{game_id}:{sort}")])
    if picked:
        rows.append([InlineKeyboardButton(
            "📨 Прислать тренерам",
            callback_data=f"coach:startsend:{source}:{game_id}:{sort}")])
    rows.append([InlineKeyboardButton("⬅️ К играм", callback_data="coach:start")])
    return coach_lineup.text(data), InlineKeyboardMarkup(rows)


def _role_screen(row: int, source: str, game_id: str, sort: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Позиция игрока. В кнопке — номер и название разом.

    Номер кладём индексом, а не текстом: Telegram ограничивает callback_data
    64 байтами, и «Атакующий защитник» кириллицей вместе с адресом игры туда
    не влезает."""
    import coach_lineup
    import coach_payments
    person = coach_payments.player_by_row(int(row)) or {}
    now = str(person.get("role") or "")
    now_num = coach_lineup.role_number(now)
    keys = []
    for idx, (num, name) in enumerate(coach_lineup.ROLES):
        label = (f"№{num} {name}" if num else name)
        keys.append([InlineKeyboardButton(
            ("✅ " if num and num == now_num else "") + label,
            callback_data=f"coach:setrole:{row}:{idx}:{source}:{game_id}:{sort}")])
    keys.append([InlineKeyboardButton(
        "Снять амплуа",
        callback_data=f"coach:setrole:{row}:-1:{source}:{game_id}:{sort}")])
    keys.append([InlineKeyboardButton(
        "⬅️ К амплуа", callback_data=f"coach:roles:{source}:{game_id}:{sort}")])
    return (f"🎽 {person.get('title', '')}\n\n"
            f"Сейчас: {coach_lineup.role_title(now) or 'не задано'}.\n\n"
            "На какой позиции обычно играет?", InlineKeyboardMarkup(keys))


# ─── игра, заведённая тренером ──────────────────────────────────────────────
#
# Организатор объявляет матч раньше, чем тот появляется в расписании лиги.
# Мастер ведёт тренера по шагам и в конце отправляет обычный опрос: дальше
# игра живёт как лиговая — состав, оплата, долги.

# Черновик игры по тренеру: id → {шаг, лига, соперник, дата, время, ...}
_newgame: Dict[int, Dict[str, Any]] = {}

NG_CANCEL = "\n\nПередумал — /start."


def _ng_leagues_screen() -> Tuple[str, InlineKeyboardMarkup]:
    import coach_newgame
    rows = [[InlineKeyboardButton(lg["title"][:BTN_TEXT], callback_data=f"coach:ng:lg:{i}")]
            for i, lg in enumerate(coach_newgame.leagues())]
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="coach:main")])
    return ("➕ Создать игру\n\nВ какой лиге играем?", InlineKeyboardMarkup(rows))


def _ng_arena_screen(draft: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    import coach_newgame
    known = coach_newgame.arenas()
    rows = [[InlineKeyboardButton(a[:BTN_TEXT], callback_data=f"coach:ng:ar:{i}")]
            for i, a in enumerate(known)]
    draft["arena_list"] = known
    rows.append([InlineKeyboardButton("✍️ Другое место", callback_data="coach:ng:arown")])
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="coach:main")])
    return ("📍 Где играем?\n\nВыбери зал или впиши свой." , InlineKeyboardMarkup(rows))


def _ng_form_screen() -> Tuple[str, InlineKeyboardMarkup]:
    return ("👕 В какой форме играем?", InlineKeyboardMarkup([
        [InlineKeyboardButton("👕 Тёмная", callback_data="coach:ng:form:dark"),
         InlineKeyboardButton("👕 Светлая", callback_data="coach:ng:form:light")],
        [InlineKeyboardButton("Пропустить", callback_data="coach:ng:form:none")],
        [InlineKeyboardButton("❌ Отмена", callback_data="coach:main")]]))


def _ng_preview_screen(draft: Dict[str, Any]) -> Tuple[str, InlineKeyboardMarkup]:
    import coach_newgame
    return (coach_newgame.summary(draft), InlineKeyboardMarkup([
        [InlineKeyboardButton("📣 Отправить голосование", callback_data="coach:ng:send")],
        [InlineKeyboardButton("❌ Отмена", callback_data="coach:main")]]))


async def _ng_send(query, user) -> None:
    """Отправляет опрос и регистрирует игру — дальше она обычная."""
    import coach_newgame
    import game_roster
    draft = _newgame.get(user.id)
    if not draft:
        await query.edit_message_text("Черновик потерялся — начни заново.",
                                      reply_markup=_coach_markup())
        return
    gsm = _game_manager()
    chat_ids = _result_chat_ids(gsm)
    topic = getattr(gsm, "game_poll_topic_id", None)
    question = coach_newgame.poll_text(draft)
    sent: List[Dict[str, Any]] = []
    for chat_id in chat_ids:
        kwargs: Dict[str, Any] = {
            "chat_id": int(chat_id), "question": question,
            "options": coach_newgame.POLL_OPTIONS,
            "is_anonymous": getattr(gsm, "game_poll_is_anonymous", False),
            "allows_multiple_answers": getattr(gsm, "game_poll_allows_multiple", False),
        }
        if topic is not None:
            kwargs["message_thread_id"] = topic
        try:
            pm = await query.get_bot().send_poll(**kwargs)
        except Exception as e:
            # Топик мог быть удалён — шлём в общий чат, а не молчим.
            if topic is not None and "thread not found" in str(e).lower():
                kwargs.pop("message_thread_id", None)
                pm = await query.get_bot().send_poll(**kwargs)
            else:
                log.warning(f"Опрос новой игры в чат {chat_id}: {e}")
                continue
        sent.append({"poll_id": pm.poll.id if pm.poll else None,
                     "chat_id": pm.chat.id, "message_id": pm.message_id})
    if not sent:
        await query.edit_message_text(
            "Не смог отправить опрос — проверь, что бот в чате команды.",
            reply_markup=_coach_markup())
        return
    gid = await asyncio.to_thread(coach_newgame.register, draft, sent)
    # Форма ложится сразу в состояние игры: в опросе она уже написана, и
    # заставлять тренера выбирать её второй раз незачем.
    if draft.get("form") in game_roster.FORMS:
        await asyncio.to_thread(game_roster.set_form, draft["source"], gid,
                                draft["form"])
    _refresh_poll_cache()
    _newgame.pop(user.id, None)
    await query.edit_message_text(
        f"✅ Игра создана, опрос отправлен ({len(sent)} чат(а)).\n\n"
        f"{question}\n\nКак проголосуют — собери состав в «👥 Состав на игру».",
        reply_markup=_coach_markup())


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
        rows.append([InlineKeyboardButton(f"{p['title']}"[:BTN_TEXT],
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
        [InlineKeyboardButton("🎂 Дата рожд.", callback_data=f"admin:field:set:{row}:bd"),
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
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        games = game_timeline.our_games(conn, limit=8)
    back = [[InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu:games")]]
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
                f"{day} · {title}"[:BTN_TEXT],
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


def _our_team_title(league: Dict[str, str]) -> str:
    """Как называется НАША команда в этой лиге — для текста опроса."""
    try:
        import league_sync
        for t in league_sync.our_teams(league.get("source") or None):
            if not league.get("team_id") or str(t.get("team_id")) == league["team_id"]:
                return str(t.get("name") or "Мы")
    except Exception as e:
        log.warning(f"название нашей команды: {e}")
    return "Мы"


async def handle_newgame_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Шаги мастера создания игры, которые вводятся текстом."""
    import coach_newgame
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _newgame:
        return
    if not _can_see_reports(user):
        _newgame.pop(user.id, None)
        return
    draft = _newgame[user.id]
    stage = draft.get("stage", "")
    text = (msg.text or "").strip()

    if stage == "opponent":
        # Товарищеский матч не сверяем ни с чем: соперника может не быть ни в
        # одной лиге, и требовать «выбери из списка» тут не из чего.
        if draft.get("key") == coach_newgame.FRIENDLY:
            draft["opponent"], draft["stage"] = text, "date"
            await msg.reply_text(f"Соперник: {text}.\n\n📅 Дата игры? "
                                 "Например «09.08»." + NG_CANCEL)
            raise ApplicationHandlerStop
        found = await asyncio.to_thread(coach_newgame.find_teams,
                                        draft["source"], text)
        draft["found"] = found
        rows = [[InlineKeyboardButton(n[:BTN_TEXT], callback_data=f"coach:ng:opp:{i}")]
                for i, n in enumerate(found)]
        rows.append([InlineKeyboardButton(f"✍️ Оставить «{text}»"[:BTN_TEXT],
                                          callback_data=f"coach:ng:opp:{len(found)}")])
        rows.append([InlineKeyboardButton("❌ Отмена", callback_data="coach:main")])
        draft["found"] = found + [text]
        head = (f"Нашёл по «{text}»:" if found
                else f"В прошлых играх «{text}» не встречался — "
                     f"можно оставить как есть.")
        await msg.reply_text(head, reply_markup=InlineKeyboardMarkup(rows))
        raise ApplicationHandlerStop

    if stage == "date":
        day = coach_newgame.parse_day(text)
        if not day:
            await msg.reply_text("Не понял дату. Как «09.08» или «09.08.2026»."
                                 + NG_CANCEL)
            raise ApplicationHandlerStop
        draft["date"], draft["stage"] = day, "time"
        await msg.reply_text(f"Дата: {day.strftime('%d.%m.%Y')}.\n\n"
                             "🕒 Во сколько начало? Например «18:30»." + NG_CANCEL)
        raise ApplicationHandlerStop

    if stage == "time":
        when = coach_newgame.parse_time(text)
        if not when:
            await msg.reply_text("Не понял время. Как «18:30»." + NG_CANCEL)
            raise ApplicationHandlerStop
        draft["time"], draft["stage"] = when, "arena"
        screen, markup = await asyncio.to_thread(_ng_arena_screen, draft)
        await msg.reply_text(screen, reply_markup=markup)
        raise ApplicationHandlerStop

    if stage == "arena_text":
        draft["arena"], draft["stage"] = text, "form"
        screen, markup = _ng_form_screen()
        await msg.reply_text(screen, reply_markup=markup)
        raise ApplicationHandlerStop


async def handle_money_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Суммы, которые тренер вводит руками: новый долг или размер взноса."""
    import coach_payments
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_money:
        return
    if not _can_see_reports(user):
        _awaiting_money.pop(user.id, None)
        return
    pending = _awaiting_money[user.id]
    text = (msg.text or "").strip()

    # Выбор человека для долга: тренер пишет фамилию, а не сумму. Раньше на
    # этом экране текст не ловился вовсе — бот молчал, и это выглядело как
    # «кнопка есть, а не работает».
    if pending == "debtwho":
        found = await asyncio.to_thread(_find_people, text)
        if not found:
            # Никого не нашли — предлагаем завести долг на введённое имя, как
            # это уже сделано с соперником в мастере игры. Долг бывает и за
            # тем, кого в листе нет: гость на одну игру, родитель. Заводить
            # его в «Игроки» ради этого нельзя — он попадёт и в опросы, и в
            # состав, и в статистику.
            _debt_draft[user.id] = {"row": 0, "title": text, "who": text}
            _awaiting_money[user.id] = "debtwho"      # ждём другую фамилию
            await msg.reply_text(
                f"В листе «Игроки» никого похожего на «{text}» нет.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"✍️ Записать долг на «{text}»"[:BTN_TEXT],
                                          callback_data="coach:debtfree")],
                    [InlineKeyboardButton("🔍 Искать заново",
                                          callback_data="coach:adddebt")],
                    [InlineKeyboardButton("❌ Отмена", callback_data="coach:main")]]))
            raise ApplicationHandlerStop
        if len(found) == 1:
            p = found[0]
            _debt_draft[user.id] = {"row": p["row"], "title": p["title"], "who": ""}
            _awaiting_money.pop(user.id, None)
            why, markup = _debt_why(user.id)
            await msg.reply_text(why, reply_markup=markup)
            raise ApplicationHandlerStop
        markup = await asyncio.to_thread(_pay_players_markup, 0, text, "coach:debtwho")
        await msg.reply_text(f"Нашёл несколько по «{text}». Кому долг?",
                             reply_markup=markup)
        raise ApplicationHandlerStop

    # Своё пояснение: за что долг. Сумму спросим следующим шагом.
    if pending == "debtnote":
        draft = _debt_draft.get(user.id)
        if not draft:
            await msg.reply_text("Начни заново.")
            raise ApplicationHandlerStop
        draft["note"] = text[:60]
        _awaiting_money[user.id] = "debtsum"
        await msg.reply_text(
            f"➕ Долг для {draft['title']} — {draft['note']}.\n\n"
            "Пришли сумму: «500».\n\nПередумал — /start.")
        raise ApplicationHandlerStop

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

    if pending.startswith("editdebt:"):
        debt_id = int(pending.split(":", 1)[1])
        await asyncio.to_thread(coach_payments.edit_debt, debt_id, amount,
                                note if note else None)
        screen, markup = await asyncio.to_thread(_debts_screen)
        await msg.reply_text(f"✏️ Поправил: {amount} ₽"
                             + (f" ({note})" if note else "")
                             + f"\n\n{screen}", reply_markup=markup,
                             parse_mode="HTML")
        raise ApplicationHandlerStop

    if pending == "debtsum":
        draft = _debt_draft.pop(user.id, None)
        if not draft:
            await msg.reply_text("Начни заново.")
            raise ApplicationHandlerStop
        # Пояснение уже выбрано шагом раньше; если тренер дописал что-то к
        # сумме («500 за март»), это уточнение, а не замена.
        why = " · ".join(x for x in (draft.get("note", ""), note) if x)
        await asyncio.to_thread(coach_payments.add_debt, int(draft["row"]),
                                amount, why, str(user.id), draft.get("who", ""))
        head = f"Добавил долг: {draft['title']} — {amount} ₽"
        if why:
            head += f" ({why})"
        if not draft["row"]:
            head += ("\n\nЭтого человека нет в листе «Игроки» — напоминание "
                     "ему бот не пошлёт, телеграма его он не знает.")
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


async def handle_access_date(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Дата, до которой админ открывает раздел: и игроку из списка, и по нику."""
    import coach_newgame
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_access:
        return
    if not _is_admin(user):
        _awaiting_access.pop(user.id, None)
        return
    kind, how, who = str(_awaiting_access[user.id]).split(":", 2)
    raw = (msg.text or "").strip()
    forever = raw.lower().replace("ё", "е") in ("бессрочно", "навсегда", "без срока")
    until = ""
    if not forever:
        day = coach_newgame.parse_day(raw)
        if not day:
            await msg.reply_text("Не понял дату. Напиши «10.09» или «10.09.2026», "
                                 "либо «бессрочно». Передумал — /start.")
            raise ApplicationHandlerStop
        if day < date.today():
            await msg.reply_text("Эта дата уже прошла. Напиши будущую. "
                                 "Передумал — /start.")
            raise ApplicationHandlerStop
        until = day.isoformat()
    _awaiting_access.pop(user.id, None)
    when = f"до {date.fromisoformat(until):%d.%m.%Y}" if until else "бессрочно"
    title = sheets_cache.ACCESS_TITLES.get(kind, kind)

    if how == "nick":
        await asyncio.to_thread(sheets_cache.grant_access, kind, who,
                                str(user.id), 0, until)
        text, markup = _render_access_list()
        await msg.reply_text(
            f"✅ «{title}» открыт для @{who} {when}.\n\n"
            "Кнопка появится у него после /start.\n\n" + text, reply_markup=markup)
        raise ApplicationHandlerStop

    row = int(who)
    p = await asyncio.to_thread(_access_open, kind, row, until, str(user.id))
    if not p:
        await msg.reply_text("Не нашёл, кому открывать.")
        raise ApplicationHandlerStop
    told = await _access_tell(msg.get_bot(), kind, p, until)
    text, markup = await asyncio.to_thread(_access_who, kind, row)
    head = (f"✅ {p['title']}: открыл {when}."
            + ("" if told else " Сказать ему не смог — не запускал бота."))
    await msg.reply_text(f"{head}\n\n{text}", reply_markup=markup)
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
    # Голосовавшие в общем чате опознаются сами: заходить в личку ради этого
    # человек не обязан.
    by_votes = sheets_cache.link_from_votes()
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
    if by_votes:
        lines.append(f"🗳 Опознал по голосам в опросах: {len(by_votes)} — "
                     + ", ".join(f["title"] for f in by_votes[:5]))
        lines.append("")
    if people:
        lines += [f"Не сопоставлены с листом: {len(people)}.",
                  "🗳 — знаем по голосам в чате, в личку бота не заходили. "
                  "Обычно у них просто сменился ник.",
                  "Нажми на человека — предложу, кому он может быть.", ""]
    else:
        lines.append("Все, кто нажимал /start, сопоставлены с игроками. ✅")
    rows: List[List[InlineKeyboardButton]] = []
    for p in people[:12]:
        nick = f"@{p['username']}" if p["username"] else "без ника"
        # Откуда знаем человека: из личного /start или из голосов в чате.
        # Для голосующих это единственный способ попасть в этот список.
        mark = "🗳 " if p.get("source") == "опрос" else ""
        title = f"{mark}{p['first_name'] or 'без имени'} · {nick}"
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
        rows.append([InlineKeyboardButton(f"✂️ {who} · {nick}"[:BTN_TEXT],
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
        # Прогресс команды и личный разбор — тоже отчёты, просто по людям, а не
        # по событиям. В общем списке они лежали отдельно и терялись.
        [InlineKeyboardButton("📈 Прогресс команды", callback_data="prog:list")],
        [InlineKeyboardButton("📊 Моя статистика", callback_data="admin:menu:profile")],
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
    # Заодно подчищаем истёкшие: список должен показывать то, что есть сейчас.
    gone = sheets_cache.purge_expired_access()
    lines = ["🔑 Доступы к закрытым разделам", "",
             "Админам открыто всё и без списка. Остальным — по выдаче.", "",
             "«👥 Из списка игроков» — для своих: по фамилии, до конкретной "
             "даты, доступ работает сразу. «➕ Открыть по @нику» — для тех, "
             "кого в листе нет (помощник, родитель); он закрепится за числовым "
             "id при первом входе.", ""]
    if gone:
        lines.append(f"⌛ Снял по истечении срока: {gone}.")
        lines.append("")
    rows: List[List[InlineKeyboardButton]] = []
    for kind, title in sheets_cache.ACCESS_TITLES.items():
        people = sheets_cache.access_list(kind)
        lines.append(f"{title}: {len(people) or 'никому'}")
        for a in people:
            state = "вошёл" if a["tg_user_id"] else "ещё не заходил"
            until = str(a.get("until") or "")
            if until:
                try:
                    left = (date.fromisoformat(until) - date.today()).days
                    state += f", до {date.fromisoformat(until):%d.%m} ({left} дн.)"
                except ValueError:
                    pass
            else:
                state += ", бессрочно"
            lines.append(f"   @{a['username']} — {state}")
        lines.append("")
        rows.append([InlineKeyboardButton(f"👥 {title} — по списку",
                                          callback_data=f"admin:acc:who:{kind}:0")])
        rows.append([InlineKeyboardButton(f"➕ {title} — по @нику",
                                          callback_data=f"admin:acc:add:{kind}")])
        for a in people:
            rows.append([InlineKeyboardButton(
                f"✂️ Забрать у @{a['username']}"[:BTN_TEXT],
                callback_data=f"admin:acc:del:{kind}:{a['username']}")])
    rows.append(_back_button())
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# ─────────────── Выдача доступа из списка игроков ──────────────────────────
#
# Выдаёт только админ: это он получает деньги за личную статистику и решает,
# кому открыть раздел тренера. Тренеру такой кнопки нет.
#
# Второй способ рядом с выдачей по @нику, а не вместо неё. По нику — для тех,
# кого нет в листе (помощник, родитель). Из списка — для своих: админ знает их
# по фамилиям, а половина команды ник не показывает вовсе, и тогда выдавать
# просто не на что. Привязка (player_links) даёт числовой id — по нему доступ
# работает сразу, без «первого входа».

# Кто вводит дату окончания доступа руками: {user_id: "вид:row:N" | "вид:nick:X"}.
_awaiting_access: Dict[int, str] = {}

# Тренер жмёт «Привязать по ссылке» и присылает адрес страницы игрока. Здесь
# лежит строка листа, к которой эту ссылку прицепить.
_awaiting_identity: Dict[int, int] = {}

ACCESS_ASK = ("📅 До какого числа открыть доступ?\n\n"
              "Напиши дату: «10.09» или «10.09.2026». Без года возьму "
              "ближайшую будущую.\n\n"
              "«бессрочно» — открыть насовсем.\n\nПередумал — /start.")

# Быстрые сроки: сколько месяцев прибавить. Год отдельной кнопкой не нужен —
# для «насовсем» есть ручной ввод.
ACCESS_MONTHS = [(1, "месяц"), (3, "3 месяца"), (6, "полгода")]


def _add_months(day: date, months: int) -> date:
    """Та же дата через N месяцев. 31-е в коротком месяце — последнее число."""
    import calendar
    total = day.year * 12 + (day.month - 1) + int(months)
    year, month = divmod(total, 12)
    month += 1
    return date(year, month, min(day.day, calendar.monthrange(year, month)[1]))


IDENTITY_PAGE = 8


def _identity_people() -> List[Dict[str, Any]]:
    """Игроки листа с их привязками к профилям лиг.

    Сначала непривязанные: экран нужен ровно для того, чтобы этот список
    закончился. Внутри групп — по алфавиту."""
    import coach_payments
    import player_identity
    people = coach_payments.players()
    with sheets_cache.get_connection() as conn:
        links = {int(r["player_row"]): str(r["tg_user_id"])
                 for r in conn.execute(
                     "SELECT player_row, tg_user_id FROM player_links")}
    out = []
    for p in people:
        uid = links.get(int(p["row"]), "")
        ids = player_identity.get_identities(uid) if uid else []
        out.append({
            "row": int(p["row"]), "title": p["title"], "uid": uid,
            "nick": str(p.get("nickname") or "").lstrip("@").strip(),
            "ids": [{"source": r["source"], "player_id": str(r["player_id"]),
                     "games": player_identity.have_games(r["source"], r["player_id"])}
                    for r in ids],
            # Без Telegram-id привязывать не к кому: профиль лиги цепляется к
            # человеку, а человек для бота — это числовой id, а не строка листа.
            "reachable": bool(uid),
        })
    out.sort(key=lambda x: (bool(x["ids"]), x["title"].lower()))
    return out


def _identity_screen(page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    """Кто из команды привязан к профилю лиги, а кто ещё нет."""
    people = _identity_people()
    linked = [p for p in people if p["ids"]]
    mute = [p for p in people if not p["reachable"]]
    lines = ["🔗 Привязки профилей лиг", "",
             "Привязка связывает человека с его страницей в лиге — без неё "
             "личная статистика не знает, чьи игры показывать.", "",
             f"Привязано: {len(linked)} из {len(people)}."]
    if mute:
        lines.append(f"⚠️ Не нажимали «Старт» ({len(mute)}): "
                     + ", ".join(p["title"] for p in mute[:6])
                     + ("…" if len(mute) > 6 else ""))
        lines.append("   Пока не напишут боту, привязывать их не к чему.")
    lines.append("")
    lines.append("Смена привязки — только отсюда: сам игрок её поменять не может.")

    start = max(0, page) * IDENTITY_PAGE
    chunk = people[start:start + IDENTITY_PAGE]
    rows: List[List[InlineKeyboardButton]] = []
    for p in chunk:
        if p["ids"]:
            mark = "✅"
        elif p["reachable"]:
            mark = "▫️"
        else:
            mark = "🚫"
        rows.append([InlineKeyboardButton(f"{mark} {p['title']}",
                                          callback_data=f"admin:idn:w:{p['row']}")])
    nav = []
    if start:
        nav.append(InlineKeyboardButton("⬅️ Назад", callback_data=f"admin:idn:list:{page - 1}"))
    if start + IDENTITY_PAGE < len(people):
        nav.append(InlineKeyboardButton("Ещё ➡️", callback_data=f"admin:idn:list:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu:people")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _identity_who(row: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Карточка одного игрока: что привязано и что можно привязать.

    Кандидатов ищем по ФИО среди имён, которые лиги уже прислали в протоколах,
    — те самые имена, по которым люди видят себя в фэнтези. Наружу за ними не
    ходим."""
    import player_identity
    people = _identity_people()
    p = next((x for x in people if x["row"] == int(row)), None)
    if not p:
        return "Игрок не найден.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К списку", callback_data="admin:idn:list:0")]])

    lines = [f"🔗 {p['title']}", ""]
    rows: List[List[InlineKeyboardButton]] = []
    if not p["reachable"]:
        lines.append("🚫 Человек ещё не писал боту, числового id у нас нет — "
                     "привязать не к кому. Попроси его нажать «Старт».")
    else:
        if p["ids"]:
            lines.append("Привязано:")
            for r in p["ids"]:
                title = player_identity.SOURCE_TITLES.get(r["source"], r["source"])
                lines.append(f"   • {title}: id {r['player_id']} — игр в базе "
                             f"{r['games']}")
                lines.append(f"     {player_identity.profile_url(r['source'], r['player_id'])}")
                rows.append([InlineKeyboardButton(
                    f"✂️ Отвязать {title}",
                    callback_data=f"admin:idn:off:{p['row']}:{r['source']}")])
        else:
            lines.append("Пока ничего не привязано.")
        lines.append("")

        taken = {(r["source"], r["player_id"]) for r in p["ids"]}
        found = [c for c in player_identity.suggest_for_name(p["title"])
                 if (c["source"], c["player_id"]) not in taken]
        if found:
            lines.append("Похожие профили в лигах:")
            for c in found:
                title = player_identity.SOURCE_TITLES.get(c["source"], c["source"])
                lines.append(f"   • {title}: {c['name']} — id {c['player_id']}, "
                             f"игр {c['games']}")
                rows.append([InlineKeyboardButton(
                    f"🔗 {title}: {c['name']} ({c['games']} игр)",
                    callback_data=f"admin:idn:set:{p['row']}:{c['source']}:{c['player_id']}")])
        else:
            lines.append("Похожих по ФИО не нашёл — лиги пишут имена по-своему. "
                         "Открой его страницу на сайте лиги и пришли ссылку "
                         "кнопкой ниже.")
        rows.append([InlineKeyboardButton("✍️ Привязать по ссылке",
                                          callback_data=f"admin:idn:man:{p['row']}")])

    rows.append([InlineKeyboardButton("⬅️ К списку", callback_data="admin:idn:list:0")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


TC_PAGE = 10


def _tc_games() -> Tuple[str, InlineKeyboardMarkup]:
    """Наши игры с записью — выбор для показа тайм-кодов за любого игрока.

    Экран нужен для демонстрации: показать человеку, что он получит, ещё до
    того, как он привязал профиль и оплатил. Своими руками через «🎬 Я в
    записи» этого не сделать — там показывается только сам админ."""
    import coach_payments
    import game_timeline
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        games = game_timeline.our_games(conn, limit=TC_PAGE)
    back = [[InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu:games")]]
    if not games:
        return ("🎬 Тайм-коды за любого игрока\n\nИгр с записью пока нет.",
                InlineKeyboardMarkup(back))
    lines = ["🎬 Тайм-коды за любого игрока", "",
             "Выбери игру, потом человека — покажу его выходы на площадку и "
             "моменты со ссылками прямо в запись.", "",
             "Это тот самый экран, который получает игрок. Годится, чтобы "
             "показать вживую, за что просим деньги.", ""]
    rows = []
    for g in games:
        day = coach_payments._human_date(str(g["game_date"]))
        title = f"{g['home_name'] or '—'} — {g['guest_name'] or '—'}"
        mark = "" if g.get("shifts") else " (не размечена)"
        lines.append(f"• {day} · {title}{mark}")
        rows.append([InlineKeyboardButton(
            f"{day} · {title}"[:BTN_TEXT],
            callback_data=f"admin:tc:who:{g['source']}:{g['game_id']}")])
    rows += back
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _tc_players(source: str, game_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Кто играл в этом матче — по протоколу, обе команды.

    Имена берём из памяти (`player_names`) — те же, что в фэнтези. Кого лига
    не назвала, показываем номером: пустая кнопка хуже некрасивой."""
    import game_timeline
    import player_names
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        # dict, а не sqlite3.Row: у Row нет .get(), и на игре без строки в
        # game_meta экран падал бы вместо того, чтобы показать список.
        row = conn.execute(
            "SELECT home_name, guest_name, home_team_id, guest_team_id "
            "FROM game_meta WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
        meta = dict(row) if row else {}
        rows_db = [dict(r) for r in conn.execute(
            """SELECT player_id, team_id, number, pts, reb, ast
                 FROM game_player_stats WHERE source = ? AND game_id = ?
                ORDER BY pts DESC""", (source, str(game_id)))]
    back = [[InlineKeyboardButton("⬅️ К играм", callback_data="admin:tc:games")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="admin:menu:games")]]
    if not rows_db:
        return ("🎬 В этой игре нет протокола — показывать некого.",
                InlineKeyboardMarkup(back))

    marked = {str(m["player_id"]) for m in game_timeline.moments(source, game_id)}
    teams = {str(meta.get("home_team_id") or ""): meta.get("home_name") or "",
             str(meta.get("guest_team_id") or ""): meta.get("guest_name") or ""}
    lines = [f"🎬 {meta.get('home_name') or '—'} — {meta.get('guest_name') or '—'}", "",
             "Игроки по протоколу. ✨ — у кого разобраны моменты.", ""]
    rows: List[List[InlineKeyboardButton]] = []
    for r in rows_db:
        pid = str(r["player_id"])
        name = player_names.get(source, pid) or f"№{r['number'] or '?'}"
        team = teams.get(str(r["team_id"]), "")
        mark = "✨" if pid in marked else "▫️"
        label = (f"{mark} {name} · {r['pts']}+{r['reb']}+{r['ast']}"
                 + (f" · {team}" if team else ""))
        rows.append([InlineKeyboardButton(
            label[:BTN_TEXT],
            callback_data=f"admin:tc:show:{source}:{game_id}:{pid}")])
    rows += back
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _identity_set(row: int, source: str, player_id: str, by: str) -> bool:
    """Привязывает профиль лиги к человеку из строки листа. От имени тренера."""
    import player_identity
    people = _identity_people()
    p = next((x for x in people if x["row"] == int(row)), None)
    if not p or not p["uid"]:
        return False
    player_identity.link_identity(p["uid"], {
        "source": source, "player_id": str(player_id),
        "comp_id": "", "api_url": ""})
    log.info("привязка профиля: строка %s -> %s:%s (кем: %s)",
             row, source, player_id, by)
    return True


def _identity_off(row: int, source: str) -> bool:
    import player_identity
    people = _identity_people()
    p = next((x for x in people if x["row"] == int(row)), None)
    if not p or not p["uid"]:
        return False
    return bool(player_identity.unlink(p["uid"], source))


def _access_people(kind: str) -> List[Dict[str, Any]]:
    """Игроки с состоянием доступа к разделу.

    Сначала те, кому открыто (у кого срок ближе — выше): важно видеть, у кого
    вот-вот кончится оплаченное. Дальше остальные по алфавиту."""
    import coach_payments
    people = coach_payments.players()
    with sheets_cache.get_connection() as conn:
        links = {int(r["player_row"]): str(r["tg_user_id"])
                 for r in conn.execute(
                     "SELECT player_row, tg_user_id FROM player_links")}
    grants = sheets_cache.access_list(kind)
    by_id = {str(g["tg_user_id"]): g for g in grants if g["tg_user_id"]}
    by_nick = {str(g["username"]).lstrip("@").lower(): g for g in grants}
    out = []
    for p in people:
        uid = links.get(int(p["row"]), "")
        nick = str(p.get("nickname") or "").lstrip("@").strip().lower()
        grant = by_id.get(uid) if uid else None
        if grant is None and nick:
            grant = by_nick.get(nick)
        until = str((grant or {}).get("until") or "")
        left = None
        if grant and until:
            try:
                left = (date.fromisoformat(until) - date.today()).days
            except ValueError:
                until = ""
        out.append({"row": int(p["row"]), "title": p["title"], "uid": uid,
                    "nick": nick, "open": grant is not None,
                    "until": until, "left": left,
                    # Ни привязки, ни ника — выдавать некому: доступ не к кому
                    # прикрепить, человек его просто не увидит.
                    "reachable": bool(uid or nick)})
    out.sort(key=lambda r: (not r["open"],
                            r["left"] if r["left"] is not None else 10 ** 6,
                            r["title"]))
    return out


def _access_mark(p: Dict[str, Any]) -> str:
    """Одной строкой: открыто ли и до какого числа."""
    if not p["open"]:
        return "закрыто" if p["reachable"] else "закрыто, нет ни ника, ни привязки"
    if not p["until"]:
        return "открыто бессрочно"
    when = date.fromisoformat(p["until"]).strftime("%d.%m.%Y")
    return f"до {when} ({p['left']} дн.)"


def _access_screen(kind: str, page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    """Кому открыт раздел и до какого числа — списком по фамилиям."""
    title = sheets_cache.ACCESS_TITLES.get(kind, kind)
    people = _access_people(kind)
    opened = [p for p in people if p["open"]]
    soon = [p for p in opened if p["left"] is not None and p["left"] <= 7]
    lines = [f"🔑 {title}: кому открыто", "",
             "Раздел закрытый: открываешь до нужного числа, срок кончится сам — "
             "помнить о нём не надо.", "",
             f"Открыто: {len(opened)} из {len(people)}."]
    if soon:
        lines.append("⏳ Заканчивается на днях: "
                     + ", ".join(f"{p['title']} ({p['left']} дн.)" for p in soon))
    lines.append("")
    start = max(0, int(page)) * PLAYERS_PER_PAGE
    chunk = people[start:start + PLAYERS_PER_PAGE]
    rows: List[List[InlineKeyboardButton]] = []
    for p in chunk:
        lines.append(f"{'✅' if p['open'] else '🔒'} {p['title']} — {_access_mark(p)}")
        rows.append([InlineKeyboardButton(
            f"{'✅' if p['open'] else '🔒'} {p['title']}"[:BTN_TEXT],
            callback_data=f"admin:acc:w:{kind}:{p['row']}")])
    nav = []
    if start:
        nav.append(InlineKeyboardButton(
            "⬅️ Назад", callback_data=f"admin:acc:who:{kind}:{page - 1}"))
    if start + PLAYERS_PER_PAGE < len(people):
        nav.append(InlineKeyboardButton(
            "Ещё ➡️", callback_data=f"admin:acc:who:{kind}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("⬅️ К доступам", callback_data="admin:acc:list")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _access_who(kind: str, row: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Экран одного игрока: сроки одной кнопкой или своя дата."""
    title = sheets_cache.ACCESS_TITLES.get(kind, kind)
    back = [InlineKeyboardButton("⬅️ К списку",
                                 callback_data=f"admin:acc:who:{kind}:0")]
    people = _access_people(kind)
    p = next((x for x in people if x["row"] == int(row)), None)
    if not p:
        return "Не нашёл игрока.", InlineKeyboardMarkup([back])
    lines = [f"👤 {p['title']}", "", f"🔑 {title}: {_access_mark(p)}", ""]
    if not p["reachable"]:
        lines.append("У человека не привязан телеграм и не заполнен ник в листе "
                     "«Игроки» — открывать некому. Пусть нажмёт /start и "
                     "проголосует, бот его опознает.")
        return "\n".join(lines), InlineKeyboardMarkup([back])

    # Продлеваем от конца оплаченного, а не от сегодня: человек платит второй
    # месяц подряд — он должен получить его целиком, а не потерять остаток.
    base = date.today()
    if p["open"] and p["until"]:
        base = max(base, date.fromisoformat(p["until"]))
    forever = p["open"] and not p["until"]
    lines.append("Доступ бессрочный — продлевать нечего." if forever
                 else "Открыть " + ("ещё на:" if p["open"] else "на:"))
    rows: List[List[InlineKeyboardButton]] = []
    if not forever:
        for months, label in ACCESS_MONTHS:
            end = _add_months(base, months)
            rows.append([InlineKeyboardButton(
                f"📅 {label} — до {end:%d.%m.%Y}",
                callback_data=f"admin:acc:set:{kind}:{row}:{months}")])
    rows.append([InlineKeyboardButton(
        "✍️ Своя дата", callback_data=f"admin:acc:day:{kind}:{row}")])
    if p["open"]:
        rows.append([InlineKeyboardButton(
            "✂️ Закрыть доступ", callback_data=f"admin:acc:off:{kind}:{row}")])
    rows.append(back)
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _access_open(kind: str, row: int, until: str, by: str) -> Dict[str, Any]:
    """Открывает доступ игроку. Возвращает его карточку для сообщения."""
    people = _access_people(kind)
    p = next((x for x in people if x["row"] == int(row)), None)
    if not p or not p["reachable"]:
        return {}
    sheets_cache.grant_access_id(kind, p["uid"], p["nick"], by, until)
    return p


# Что человек получает — своими словами, а не названием раздела в базе.
ACCESS_INTRO = {
    sheets_cache.ACCESS_PERSONAL:
        ("📊 Личная статистика открыта{when}.\n\n"
         "Нажми /start — внизу появится кнопка «📊 Моя статистика». Там твои "
         "игры, разбор после каждого матча и таймкоды выходов на площадку "
         "в записи."),
    sheets_cache.ACCESS_TEAM:
        ("🧑‍🏫 Раздел тренера открыт{when}.\n\n"
         "Нажми /start — внизу появится кнопка «🧑‍🏫 Тренер». Там разбор игр, "
         "составы и учёт оплат."),
}


async def _access_tell(bot, kind: str, p: Dict[str, Any], until: str) -> bool:
    """Сообщает человеку, что раздел открыт. Молча выдавать бессмысленно —
    кнопка появится, а он о ней не узнает."""
    if not p.get("uid"):
        return False
    when = f" до {date.fromisoformat(until):%d.%m.%Y}" if until else " бессрочно"
    body = ACCESS_INTRO.get(kind, "Доступ открыт{when}.").format(when=when)
    try:
        await bot.send_message(chat_id=int(p["uid"]), text=body)
        return True
    except Exception as e:
        log.info(f"Не сказал про доступ {p['uid']}: {e}")
        return False


async def handle_gamelink_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ответ тренера на вопрос «одна это игра или две».

    Кнопки живут под сообщением, которое присылает менеджер игр — он работает
    отдельным процессом по расписанию, а нажатие приходит сюда, в демон."""
    import game_link
    query = update.callback_query
    user = query.from_user if query else None
    if not query or not _can_see_reports(user):
        if query:
            await query.answer("Нет доступа", show_alert=True)
        return
    await query.answer()
    parts = (query.data or "").split(":")
    what = parts[1] if len(parts) > 1 else ""
    source, league_id = (parts[2], parts[3]) if len(parts) > 3 else ("", "")
    try:
        if what == "keep":
            await asyncio.to_thread(game_link.link, source, league_id,
                                    (await asyncio.to_thread(game_link.alias_of, source,
                                                             league_id) or {}).get("coach_game_id", ""),
                                    game_link.CONFIRMED, "тренер подтвердил")
            await query.edit_message_text(
                "✅ Понял, игра одна. Новый опрос не отправляю — состав и "
                "оплата остаются на той, что ты завёл.")
            return
        # «Разные игры» и «нужен новый опрос» ведут к одному: развязать, чтобы
        # ближайший прогон менеджера завёл опрос как по обычной находке.
        await asyncio.to_thread(game_link.split, source, league_id)
        await query.edit_message_text(
            "🆕 Развязал. Опрос по игре из расписания уйдёт ближайшим "
            "прогоном — он ходит раз в час.")
    except Exception as e:
        log.error(f"Развязка игр: {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}")


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
            text, markup = await _render_prog_list(is_admin=admin,
                                                   back="admin:menu:reports")
        await query.edit_message_text(text, reply_markup=markup)
    except Exception as e:
        log.error(f"Экран прогресса: {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}")


async def handle_coach_nick(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Следующее сообщение после «Открыть раздел» — @ник получателя."""
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_coach:
        return
    pending = _awaiting_coach.pop(user.id)
    kind, _, days_raw = str(pending).partition(":")
    days = int(days_raw) if days_raw.isdigit() else 0
    title = sheets_cache.ACCESS_TITLES.get(kind, kind)
    nick = (msg.text or "").strip().split()[0] if (msg.text or "").strip() else ""
    if not nick.lstrip("@").replace("_", "").isalnum():
        await msg.reply_text("Это не похоже на @ник. Открой экран и попробуй ещё раз.")
        raise ApplicationHandlerStop
    # «До определённой даты»: ник взяли, теперь спрашиваем число. Двумя шагами,
    # а не одной строкой «@ник до 10.09» — разбирать вольный ввод в вещи,
    # которую потом никто не перепроверит, себе дороже.
    if days_raw == "date":
        _awaiting_access[user.id] = f"{kind}:nick:{nick.lstrip('@')}"
        await msg.reply_text(f"Кому: {nick}. Раздел: «{title}».\n\n" + ACCESS_ASK)
        raise ApplicationHandlerStop
    sheets_cache.grant_access(kind, nick, str(user.id), days)
    from datetime import timedelta as _td
    until = ((date.today() + _td(days=days)).strftime("%d.%m.%Y") if days else "")
    await msg.reply_text(
        f"✅ Раздел «{title}» открыт для {nick}"
        + (f" до {until}." if until else " бессрочно.") + "\n\n"
        "Кнопка появится у него после /start. Если он уже нажимал /start — "
        "пусть нажмёт ещё раз, клавиатура обновится.")
    raise ApplicationHandlerStop


async def handle_mystats_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопка «📊 Моя статистика».

    Кнопку видят все свои, поэтому здесь развилка, а не отказ: у кого разбор
    открыт — получает разбор, остальные приглашение привязаться и охват своих
    игр. Молчаливый отказ здесь и был причиной, по которой раздел не рос."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if _can_see_personal(user):
        await _send_profile(msg, user)
    else:
        await _send_profile_locked(msg, user)
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

# Сколько знаков влезает в подпись кнопки в один столбец. Длиннее Telegram
# обрежет сам, многоточием и не там, где надо: «Балтика (Летний Кубок Диви…».
# Режем сами — так хотя бы видно, что подпись урезана осмысленно.
BTN_TEXT = 38


def _coach_markup() -> InlineKeyboardMarkup:
    """Корень раздела: два больших входа и разбор игр.

    Плоский список из одиннадцати кнопок читался как свалка — деньги и игры
    перемешивались, и нужное приходилось искать глазами каждый раз."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Оплата", callback_data="coach:money")],
        [InlineKeyboardButton("🏀 Игры", callback_data="coach:play")],
        [InlineKeyboardButton("📈 Разбор игр", callback_data="coach:prog")],
        [InlineKeyboardButton("🗓 Даты оповещений", callback_data="coach:sched")],
        # Частные занятия к команде отношения не имеют, но живут там же, где
        # тренер: заводить ради них отдельную кнопку под чатом — засорять
        # клавиатуру всем ради одного человека.
        [InlineKeyboardButton("🎾 Частные занятия", callback_data="pl:main")],
    ])


def _money_markup() -> InlineKeyboardMarkup:
    """Всё про деньги. Ежедневное — на виду, редкое — этажом ниже.

    Раньше здесь было шесть двухколоночных рядов, и половина подписей на
    телефоне обрезалась: «Напомнить: тр…», «Кто сколько вн…», «Последние
    пла…». В двух колонках помещается около четырнадцати знаков — длиннее
    ставить нельзя, поэтому длинное едет в одну колонку, а редкое ушло в
    подменю (см. тест ширины подписей в tests/test_buttons.py)."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💳 Внести оплату", callback_data="coach:pay")],
        [InlineKeyboardButton("💸 Долги", callback_data="coach:debts"),
         InlineKeyboardButton("➕ Долг", callback_data="coach:adddebt")],
        [InlineKeyboardButton("📨 Напомнить про тренировки",
                              callback_data="coach:remind:season")],
        [InlineKeyboardButton("📨 Напомнить про игры",
                              callback_data="coach:remind:game")],
        [InlineKeyboardButton("📊 Сводки и правки", callback_data="coach:money2")],
        [InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")],
    ])


def _money2_markup() -> InlineKeyboardMarkup:
    """Второй этаж оплат: то, к чему тренер возвращается редко."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📒 Кто сколько внёс", callback_data="coach:owe")],
        [InlineKeyboardButton("🧾 Последние платежи", callback_data="coach:last")],
        [InlineKeyboardButton("🏋️ Взносы за тренировки", callback_data="coach:train")],
        [InlineKeyboardButton("✏️ Изменить суммы", callback_data="coach:sums")],
        [InlineKeyboardButton("🗑 Удалить оплату", callback_data="coach:delpay")],
        [InlineKeyboardButton("⬅️ К оплате", callback_data="coach:money")],
    ])


def _play_markup() -> InlineKeyboardMarkup:
    """Всё про игры."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👥 Состав на игру", callback_data="coach:games")],
        [InlineKeyboardButton("🏁 Стартовый состав", callback_data="coach:start")],
        [InlineKeyboardButton("➕ Создать игру", callback_data="coach:ng")],
        [InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")],
    ])


# Даты оповещений об оплатах: ключ настройки → (подпись, что это значит).
SCHED_FIELDS = [
    ("dues_ahead_day", "Заранее за следующий месяц", "число месяца"),
    ("dues_first_day", "За начавшийся месяц", "число месяца"),
    ("dues_mid_day", "Повтор должникам", "число месяца"),
    ("dues_coach_warn", "Тренеру перед повтором", "за сколько дней"),
    ("dues_coach_end", "Тренеру перед концом месяца", "за сколько дней"),
    ("game_pay_before_hour", "Оплата игры: накануне", "час"),
    ("game_pay_hour", "Оплата игры: утро", "час"),
    ("game_pay_evening_hour", "Оплата игры: вечер следующего дня", "час"),
    ("roster_collect_days", "Собрать состав на игру", "за сколько дней"),
]

SCHED_LIMITS = {"game_pay_hour": (0, 23), "game_pay_evening_hour": (0, 23),
                "game_pay_before_hour": (0, 23),
                "roster_collect_days": (0, 14)}


def _sched_value(key: str) -> int:
    import game_roster
    import training_dues
    if key in training_dues.SCHEDULE:
        return training_dues.day(key)
    defaults = {"game_pay_hour": 9, "game_pay_evening_hour": 19,
                "game_pay_before_hour": 12,
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
        rows.append([InlineKeyboardButton(f"{title}: {shown}"[:BTN_TEXT],
                                          callback_data=f"coach:setsched:{key}")])
    lines += ["", "<i>Взносы за тренировки начинаются с сентября 2026 — "
              "до этого месяца бот про них молчит.</i>"]
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:main")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


# Что тренер сейчас вводит: id → «долг:строка» или «сумма:строка:вид».
_awaiting_money: Dict[int, str] = {}

# Черновик добавляемого долга: {row, who, note}. Отдельно от callback_data —
# и имя, и пояснение кириллицей легко перебирают лимит Телеграма в 64 байта.
_debt_draft: Dict[int, Dict[str, Any]] = {}

# За что долг. Два частых повода кнопкой, остальное словами: половина долгов
# раньше оставалась без пояснения (его писали вместе с суммой и забывали), и
# через месяц было не вспомнить, за что человек должен.
DEBT_KINDS = {"train": "тренировка", "game": "игра"}


def _debt_why(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Экран «за что долг». Показывается сразу после выбора человека."""
    draft = _debt_draft.get(uid) or {}
    who = draft.get("title", "")
    tail = " (не из состава)" if not draft.get("row") else ""
    return (f"➕ Долг для {who}{tail}.\n\nЗа что?",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🏋️ Тренировка", callback_data="coach:debtwhy:train"),
                 InlineKeyboardButton("🏀 Игра", callback_data="coach:debtwhy:game")],
                [InlineKeyboardButton("✍️ Своё", callback_data="coach:debtwhy:own")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="coach:adddebt")]]))


def _debts_screen() -> Tuple[str, InlineKeyboardMarkup]:
    """Кто и сколько должен — тремя блоками: тренировки, игры, добавленное.

    Показываем только тех, с кого действительно ждём: без проставленной суммы
    взноса человек не должник, а игры считаются лишь по объявленным составам
    начиная с даты, с которой действует порядок."""
    import coach_payments
    import game_roster
    import training_dues
    from datetime import date as _date

    lines = ["💸 Долги"]

    # Игры — по играм, а не общим списком: тренер собирает деньги на игре и
    # ему нужен список, с которым можно пройтись по залу. Ближайшая сверху.
    by_game = game_roster.debts_by_game()
    lines += ["", "🏀 <b>За игры</b>"]
    if not by_game:
        lines.append("   Никто не должен.")
    for one in by_game:
        g = one["game"]
        when = g["date"].strftime("%d.%m")
        lines += ["", f"<b>{g.get('opponent') or 'соперник'} · {when}</b>"]
        for r in one["rows"]:
            lines.append(f"   • {r['title']} — {_rub(r['amount'])}")
        if len(one["rows"]) > 1:
            lines.append(f"   <i>{len(one['rows'])} чел. · {_rub(one['total'])}</i>")

    # Тренировки — по месяцам. Непогашенный месяц остаётся своим блоком рядом
    # с новым, а не сливается с ним в одну сумму.
    by_month = training_dues.debts_by_month()
    lines += ["", "🏋️ <b>За тренировки</b>"]
    if not by_month:
        lines.append("   Никто не должен.")
    for one in by_month:
        lines += ["", f"<b>{training_dues.month_title(one['period']).capitalize()}</b>"]
        for r in one["rows"]:
            lines.append(f"   • {r['title']} — {_rub(r['debt'])}")
        if len(one["rows"]) > 1:
            lines.append(f"   <i>{len(one['rows'])} чел. · {_rub(one['total'])}</i>")

    extra = coach_payments.extra_debts()
    if extra:
        lines += ["", "📌 <b>Добавлено вручную</b>", ""]
        for d in extra:
            who = coach_payments.debt_title(d)
            note = f" — {d['note']}" if d["note"] else ""
            # Кого нет в листе, помечаем: тренер должен видеть, что напоминание
            # такому человеку бот не отправит — телеграма его он не знает.
            mark = "" if int(d.get("player_row") or 0) > 0 else " (не из состава)"
            lines.append(f"   • {who}{mark}: {_rub(d['amount'])}{note}")

    total = (sum(one["total"] for one in by_month)
             + sum(one["total"] for one in by_game)
             + sum(d["amount"] for d in extra))
    lines += ["", f"<b>Всего: {_rub(total)}</b>" if total else "Долгов нет."]

    # Разовый долг заводят руками — и суммой, и пояснением ошибаются так же.
    # Гасить и заводить заново неправильно: в истории останется закрытый долг,
    # которого никто не платил.
    rows = [[InlineKeyboardButton(
        f"✅ Погасить: {coach_payments.debt_title(d)}"[:BTN_TEXT - 3],
        callback_data=f"coach:closedebt:{d['id']}"),
        InlineKeyboardButton("✏️", callback_data=f"coach:editdebt:{d['id']}")]
        for d in extra[:5]]
    rows.append([InlineKeyboardButton("➕ Добавить долг", callback_data="coach:adddebt")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:money")])
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
            rows.append([InlineKeyboardButton(p["title"][:BTN_TEXT],
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
                    "⬅️ В раздел", callback_data="coach:money2")]]))
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
            f"🗑 {it['title']} · {it['amount']} ₽"[:BTN_TEXT],
            callback_data=f"coach:delpay:{it['id']}")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:money2")])
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
            f"{day} · {title}"[:BTN_TEXT],
            callback_data=f"rep:vidg:{g['source']}:{g['game_id']}:{g['player_id']}")])
    rows += back
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _my_video_game(source: str, game_id: str, player_id: str,
                   back: Optional[List[List[InlineKeyboardButton]]] = None
                   ) -> Tuple[str, InlineKeyboardMarkup]:
    """Тайм-коды выходов и моментов в одной игре (HTML).

    Игрок задаётся снаружи, поэтому тот же экран показывает и «себя» игроку, и
    любого — админу: показывать разное было бы двумя разными правдами.
    Отличаются только кнопки возврата, их и передаём."""
    import coach_payments
    import game_timeline
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
    # Моменты — под выходами: сначала «где я был», потом «что я сделал».
    # Отрезки без действий бывают (секретарь отмечал замены и молчал), действия
    # без отрезков — тоже, поэтому показываем то, что есть.
    spots = game_timeline.format_moments(source, game_id, player_id, link,
                                         max_items=12, with_note=not block)
    if spots:
        block = f"{block}\n\n{spots}" if block else spots
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
    rows += back or [
        [InlineKeyboardButton("⬅️ К списку игр", callback_data="rep:vid")],
        [InlineKeyboardButton("⬅️ К отчёту", callback_data="rep:back")]]
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
                    "⬅️ В раздел", callback_data="coach:play")]]))
    lines = ["👥 Состав на игру", "", "Выбери игру:"]
    rows = []
    for g in upcoming:
        picked = len(game_roster.roster(g["source"], g["game_id"]))
        mark = f" · в составе {picked}" if picked else ""
        rows.append([InlineKeyboardButton(
            f"{game_roster.game_label(g)}{mark}"[:BTN_TEXT],
            callback_data=f"rost:show:{g['source']}:{g['game_id']}")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:play")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _roster_screen(source: str, game_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Сбор состава на игру: кто вызвался, кто уже в составе, кого дописали."""
    import game_roster
    game = next((g for g in game_roster.games()
                 if g["source"] == source and g["game_id"] == str(game_id)), None)
    if not game:
        return ("Игру не нашёл — возможно, опрос по ней уже удалён.",
                InlineKeyboardMarkup([[InlineKeyboardButton(
                    "⬅️ В раздел", callback_data="coach:games")]]))
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

    # Форма — рядом с составом: тренер решает её тогда же, когда собирает
    # людей, и отдельный экран ради двух вариантов был бы лишним шагом.
    form = game_roster.form_of(source, game_id, game)
    from_poll = bool(game.get("poll_form")) and form == game.get("poll_form")
    lines.append("")
    lines.append(f"👕 Форма: {game_roster.FORMS.get(form, 'не выбрана')}"
                 + (" (из опроса)" if from_poll else ""))
    if game.get("arena"):
        lines.append(f"📍 {game['arena']}")

    rows: List[List[InlineKeyboardButton]] = []
    rows.append([
        InlineKeyboardButton(
            f"{'✅ ' if form == 'dark' else ''}👕 Тёмная",
            callback_data=f"rost:form:{source}:{game_id}:dark"),
        InlineKeyboardButton(
            f"{'✅ ' if form == 'light' else ''}👕 Светлая",
            callback_data=f"rost:form:{source}:{game_id}:light"),
    ])
    for v in waiting[:10]:
        if v["linked"]:
            rows.append([InlineKeyboardButton(
                f"➕ {v['title']}"[:BTN_TEXT],
                callback_data=f"rost:add:{source}:{game_id}:{v['row']}")])
    for p in picked[:16]:
        # Гостя вписывают руками — значит и опечатываются руками. Рядом с
        # «убрать» даём «поправить имя», чтобы не сносить и не заводить заново.
        line = [InlineKeyboardButton(
            f"➖ {p['title']}"[:BTN_TEXT],
            callback_data=f"rost:del:{source}:{game_id}:{p['row']}")]
        if p.get("guest"):
            line.append(InlineKeyboardButton(
                "✏️", callback_data=f"rost:gname:{source}:{game_id}:{p['row']}"))
        rows.append(line)
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
        # Сразу после отправки состава деньги и собирают — кнопка тут, а не
        # только в разделе оплат.
        rows.append([InlineKeyboardButton(
            "📨 Напомнить об оплате", callback_data="coach:remind:game")])
    rows.append([InlineKeyboardButton("⬅️ В раздел", callback_data="coach:games")])
    return "\n".join(lines).rstrip(), InlineKeyboardMarkup(rows)


def _game_debt_screen(source: str, game_id: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Кто из состава не заплатил за игру."""
    import game_roster
    game = next((g for g in game_roster.games()
                 if g["source"] == source and g["game_id"] == str(game_id)), None)
    if not game:
        return ("Игру не нашёл.", InlineKeyboardMarkup([[InlineKeyboardButton(
            "⬅️ В раздел", callback_data="coach:games")]]))
    rows = game_roster.debtors(source, game_id)
    text = game_roster.coach_debt_text(game, rows)
    buttons = [[InlineKeyboardButton(f"✔ {p['title']}"[:BTN_TEXT],
                                     callback_data=f"rost:paid:{source}:{game_id}:{p['row']}")]
               for p in rows[:20]]
    # Долги открывают из карточки игры — туда и возвращаемся. «Состав» отдельной
    # кнопкой не нужен: это тот же экран, куда ведёт возврат.
    buttons.append([InlineKeyboardButton(
        "⬅️ К составу", callback_data=f"rost:show:{source}:{game_id}")])
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
            "⬅️ Назад", callback_data="coach:money2")]])

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

    buttons = [[InlineKeyboardButton(f"✔ {r['title']}"[:BTN_TEXT],
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
    и для правки сумм, менялся только адрес.

    Стрелки листания раньше вели в жёстко зашитый `coach:page`, то есть в поток
    ОПЛАТЫ, откуда бы список ни открыли. В «Добавить долг» это выбрасывало в
    корень раздела: обработчик искал черновик платежа, не находил и показывал
    главный экран. Теперь адрес листания строится из того же `pick`, и список
    остаётся в своём потоке."""
    import coach_payments
    # Все из листа: за игру может заплатить и тот, кто сейчас не тренируется.
    people = coach_payments.players()
    if query:
        found = people_search(query, people)
        if found:
            people = found + [p for p in people if p not in found]
    pages = max(1, (len(people) + PLAYERS_PER_PAGE - 1) // PLAYERS_PER_PAGE)
    page = max(0, min(page, pages - 1))
    chunk = people[page * PLAYERS_PER_PAGE:(page + 1) * PLAYERS_PER_PAGE]
    rows = [[InlineKeyboardButton(p["title"][:BTN_TEXT], callback_data=f"{pick}:{p['row']}")]
            for p in chunk]
    if pages > 1:
        # «coach:pick» -> «coach:page», «coach:debtwho» -> «coach:debtpage»:
        # у каждого потока своё листание, чужое его больше не перехватывает.
        flip = PICK_PAGES.get(pick, "coach:page")
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️", callback_data=f"{flip}:{page - 1}"))
        nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="coach:noop"))
        if page < pages - 1:
            nav.append(InlineKeyboardButton("▶️", callback_data=f"{flip}:{page + 1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton("❌ Отмена", callback_data="coach:main")])
    return InlineKeyboardMarkup(rows)


# Куда листать в каждом потоке выбора игрока.
PICK_PAGES = {
    "coach:pick": "coach:page",
    "coach:debtwho": "coach:debtpage",
    "coach:sums": "coach:sumspage",
}


def _find_people(query: str) -> List[Dict[str, Any]]:
    """Игроки по набранному тексту — карточками из листа «Игроки»."""
    import coach_payments
    return people_search(query, coach_payments.players())


def people_search(query: str, people: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Поиск по уже собранному списку игроков — тем же правилом, что везде.

    Общий на весь бот (player_search.rank): им же ищутся соперники при
    создании игры и игроки в составе. Раньше в каждом экране правило было
    своё, и человеку приходилось помнить, где как искать."""
    import player_search
    return player_search.rank(query, people, player_search.person_fields, limit=0)


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

# Фамилии, которых не нашлось в листе: держим, чтобы предложить дописать их
# гостями. В callback_data не влезают — кириллица по два байта на знак.
_roster_guests: Dict[int, List[str]] = {}

# Кого из гостей сейчас переименовывают: {tg id: (источник, игра, строка)}.
_awaiting_guest: Dict[int, Tuple[str, str, int]] = {}


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
    # Пятый кусок — не всегда строка листа: у кнопок формы там «dark»/«light».
    # Безусловный int() ронял обработчик раньше, чем дело доходило до ветки, и
    # кнопки формы в составе не работали НИ РАЗУ с момента появления.
    tail = parts[4] if len(parts) > 4 else ""
    row = int(tail) if tail.lstrip("-").isdigit() else 0
    _roster_focus[user.id] = (source, game_id)

    try:
        if what == "add":
            await asyncio.to_thread(game_roster.add, source, game_id, row, str(user.id))
            _drop_pending(user.id)
        elif what == "gname":
            _clear_pending(user.id)
            _awaiting_guest[user.id] = (source, game_id, row)
            one = await asyncio.to_thread(game_roster.guest_card, source,
                                          game_id, row)
            await query.edit_message_text(
                f"✏️ Новое имя вместо «{(one or {}).get('title', '?')}».\n\n"
                "Пришли как надо.\n\nПередумал — /start.")
            return

        elif what == "guest":
            # Имя в callback_data не кладём: кириллица это два байта на знак,
            # и лимит в 64 байта кончается на середине фамилии. Держим список
            # ненайденных в памяти и ссылаемся номером.
            names = _roster_guests.get(user.id) or []
            name = names[row] if 0 <= row < len(names) else ""
            made = await asyncio.to_thread(game_roster.add_guest, source,
                                           game_id, name, str(user.id))
            if made.get("error"):
                await query.answer(made["error"], show_alert=True)
            _roster_guests.pop(user.id, None)
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
            # Перед отправкой сверяемся с опросом ещё раз. Экран — снимок: его
            # отрисовали, а через минуту человек нажал «Готов», и тренер шлёт
            # состав без него, ничего не подозревая. Ровно так 11.08.2026
            # Морозов проголосовал в ту же минуту, когда уходил состав.
            missing = await asyncio.to_thread(_ready_but_out, source, game_id)
            if missing:
                await query.edit_message_text(
                    f"⚠️ Отметились «Готов», но в составе их нет ({len(missing)}):\n"
                    + "\n".join(f"• {m['title']}" for m in missing)
                    + "\n\nДобавить их или отправить как есть?",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("➕ Добавить и отправить",
                                              callback_data=f"rost:postall:{source}:{game_id}")],
                        [InlineKeyboardButton("📣 Отправить как есть",
                                              callback_data=f"rost:post2:{source}:{game_id}")],
                        [InlineKeyboardButton("⬅️ К составу",
                                              callback_data=f"rost:open:{source}:{game_id}")]]))
                return
            await _post_roster(query, source, game_id, user)
            return
        elif what == "post2":
            await _post_roster(query, source, game_id, user)
            return
        elif what == "postall":
            for m in await asyncio.to_thread(_ready_but_out, source, game_id):
                await asyncio.to_thread(game_roster.add, source, game_id,
                                        m["row"], str(user.id))
            await _post_roster(query, source, game_id, user)
            return
        elif what == "edit":
            await _update_roster_post(query, source, game_id)
            return
        elif what == "form" and len(parts) > 4:
            import game_roster
            value = parts[4] if parts[4] in game_roster.FORMS else ""
            # Повторное нажатие снимает выбор: передумал — не надо искать
            # отдельную кнопку «сбросить». Снятое помечаем особо, иначе на его
            # место молча вернётся форма из опроса.
            if game_roster.form_of(source, game_id) == value:
                value = game_roster.NO_FORM
            await asyncio.to_thread(game_roster.set_form, source, game_id, value)
            # Если состав уже в чате — правим то же сообщение, а не шлём второе:
            # форма меняется чаще состава, и каждое такое изменение новым
            # сообщением превратило бы чат в ленту уточнений.
            if await asyncio.to_thread(game_roster.is_posted, source, game_id):
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
        if "not modified" in str(e).lower():
            await query.answer("Уже открыто")
            return
        log.error(f"Состав ({what}): {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}")


def _drop_pending(user_id: int) -> None:
    queue = _roster_pending.get(user_id)
    if queue:
        queue.pop(0)
        if not queue:
            _roster_pending.pop(user_id, None)


def _ready_but_out(source: str, game_id: str) -> List[Dict[str, Any]]:
    """Кто отметился «Готов», но в составе его нет.

    Только опознанные: кого нет в листе «Игроки», добавить в состав всё равно
    некуда — их тренер видит отдельной строкой на экране состава."""
    import game_roster
    picked = {p["row"] for p in game_roster.roster(source, game_id)}
    return [v for v in game_roster.voters(str(game_id))
            if v["linked"] and v["row"] not in picked]


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
    who = f"@{user.username}" if getattr(user, "username", "") else str(user.id)
    if posts:
        await asyncio.to_thread(game_roster.mark_posted, source, game_id, posts)
    # В журнал — с именем: это единственное сообщение бота, которое видит вся
    # команда, и на вопрос «бот сам или человек?» должны отвечать данные.
    log.info(f"Состав {source}:{game_id} отправлен в чат ({len(people)} чел.) "
             f"кнопкой, нажал {who} (id {user.id})")
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
    rows = [[InlineKeyboardButton(p["title"][:BTN_TEXT],
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
        _awaiting_guest.pop(user.id, None)
        return

    # Правка имени гостя перехватывает ввод: тренер сейчас отвечает на вопрос
    # «как его зовут», а не дописывает людей в состав.
    if user.id in _awaiting_guest:
        src, gid, row = _awaiting_guest.pop(user.id)
        ok = await asyncio.to_thread(game_roster.rename_guest, src, gid, row,
                                     msg.text or "")
        screen, markup = await asyncio.to_thread(_roster_screen, src, gid)
        head = "✏️ Поправил." if ok else "⚠️ Не вышло — гостя уже убрали."
        await msg.reply_text(f"{head}\n\n{screen}", reply_markup=markup)
        raise ApplicationHandlerStop

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
    _roster_guests[user.id] = missing
    question = _next_roster_question(user.id, source, game_id)
    if question:
        if missing:
            head.append("Не нашёл в листе «Игроки»: " + ", ".join(missing))
        text, markup = question
        await msg.reply_text(("\n".join(head) + "\n\n" if head else "") + text,
                             reply_markup=markup)
        raise ApplicationHandlerStop

    # Никого похожего нет — предлагаем дописать гостем, а не оставляем тупик.
    # Играют не только те, кто в листе: подмена, легионер, отец игрока. В лист
    # их заводить нельзя — там опросы, взносы и статистика.
    if missing:
        rows = [[InlineKeyboardButton(f"✍️ Гость: {name}"[:BTN_TEXT],
                                      callback_data=f"rost:guest:{source}:{game_id}:{i}")]
                for i, name in enumerate(missing[:5])]
        rows.append([InlineKeyboardButton(
            "⬅️ К составу", callback_data=f"rost:open:{source}:{game_id}")])
        await msg.reply_text(
            ("\n".join(head) + "\n\n" if head else "")
            + "В листе «Игроки» никого похожего нет: " + ", ".join(missing)
            + "\n\nЕсли он всё-таки играет — допиши гостем. Он попадёт в состав "
              "и в пятёрку, но не в опросы, взносы и статистику.",
            reply_markup=InlineKeyboardMarkup(rows))
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
    # «Назад» обязано вернуть на шаг назад, а не на главную: экраны второго
    # этажа оплат открывают из «📊 Сводки и правки», туда же и возвращаемся.
    # Иначе человек теряет место, где был, и идёт заново через два меню.
    back_money2 = [InlineKeyboardButton("⬅️ Назад", callback_data="coach:money2")]

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
            _clear_pending(user.id)
            _awaiting_payment.add(user.id)
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
            _clear_pending(user.id)
            _awaiting_money[user.id] = f"sched:{key}"
            hint = ("Пришли число месяца (1–28)." if unit == "число месяца"
                    else "Пришли час (0–23)." if unit == "час"
                    else "Пришли количество дней.")
            await query.edit_message_text(
                f"🗓 {title}. Сейчас: {_sched_value(key)}.\n\n{hint}\n\n"
                "Передумал — /start.")

        elif what == "ng":
            import coach_newgame
            step = parts[2] if len(parts) > 2 else ""
            draft = _newgame.get(user.id) or {}
            if not step:
                _clear_pending(user.id)
                _newgame[user.id] = {"stage": "league"}
                text, markup = _ng_leagues_screen()
                await query.edit_message_text(text, reply_markup=markup)
            elif step == "lg" and len(parts) > 3:
                lg = coach_newgame.leagues()[int(parts[3])]
                _newgame[user.id] = {"stage": "opponent", "key": lg["key"],
                                     "source": lg["source"],
                                     "league_title": lg["title"],
                                     "our": _our_team_title(lg)}
                await query.edit_message_text(
                    "🏀 С кем играем?\n\nНапиши название команды или его часть."
                    + NG_CANCEL)
            elif step == "opp" and len(parts) > 3:
                draft["opponent"] = draft.get("found", [])[int(parts[3])]
                draft["stage"] = "date"
                await query.edit_message_text(
                    f"Соперник: {draft['opponent']}.\n\n📅 Дата игры? "
                    "Например «09.08» или «09.08.2026»." + NG_CANCEL)
            elif step == "ar" and len(parts) > 3:
                draft["arena"] = draft.get("arena_list", [])[int(parts[3])]
                draft["stage"] = "form"
                text, markup = _ng_form_screen()
                await query.edit_message_text(text, reply_markup=markup)
            elif step == "arown":
                draft["stage"] = "arena_text"
                await query.edit_message_text("📍 Напиши место игры." + NG_CANCEL)
            elif step == "form" and len(parts) > 3:
                draft["form"] = parts[3] if parts[3] in coach_newgame.FORMS else ""
                draft["stage"] = "preview"
                text, markup = _ng_preview_screen(draft)
                await query.edit_message_text(text, reply_markup=markup)
            elif step == "send":
                await _ng_send(query, user)

        elif what == "start":
            if len(parts) > 4:
                text, markup = await asyncio.to_thread(
                    _start_screen, parts[2], parts[3], parts[4])
                await query.edit_message_text(text, reply_markup=markup,
                                              parse_mode="HTML")
                return
            text, markup = await asyncio.to_thread(_start_games_screen)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "startsend" and len(parts) > 4:
            import coach_lineup
            data = await asyncio.to_thread(coach_lineup.lineup, parts[2], parts[3], parts[4])
            body = coach_lineup.text(data)
            sent = 0
            for uid in await asyncio.to_thread(_coach_recipients):
                try:
                    await query.get_bot().send_message(chat_id=int(uid), text=body,
                                                       parse_mode="HTML")
                    sent += 1
                except Exception as e:
                    log.warning(f"Стартовый состав тренеру {uid}: {e}")
            await query.answer(f"Отправил тренерам: {sent}")

        elif what == "role" and len(parts) > 5:
            text, markup = await asyncio.to_thread(
                _role_screen, int(parts[2]), parts[3], parts[4], parts[5])
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "setrole" and len(parts) > 6:
            import coach_lineup
            try:
                idx = int(parts[3])
            except ValueError:
                idx = -1
            role = (coach_lineup.ROLES[idx][1]
                    if 0 <= idx < len(coach_lineup.ROLES) else "")
            ok = await asyncio.to_thread(coach_lineup.set_role, int(parts[2]), role)
            await query.answer("Записал" if ok else "Таблица не приняла запись")
            text, markup = await asyncio.to_thread(
                _start_screen, parts[4], parts[5], parts[6], True)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "roles" and len(parts) > 4:
            text, markup = await asyncio.to_thread(
                _start_screen, parts[2], parts[3], parts[4], True)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "sf" and len(parts) > 5:
            # Нажатие на фамилию: поставить в старт или снять.
            import coach_lineup
            _, note = await asyncio.to_thread(coach_lineup.toggle_start,
                                              parts[2], parts[3], int(parts[4]))
            await query.answer(note)
            text, markup = await asyncio.to_thread(
                _start_screen, parts[2], parts[3], parts[5])
            await query.edit_message_text(text, reply_markup=markup,
                                          parse_mode="HTML")

        elif what == "money":
            await query.edit_message_text(
                "💰 Оплата\n\nВзносы, долги и напоминания.",
                reply_markup=_money_markup())

        elif what == "money2":
            await query.edit_message_text(
                "📊 Сводки и правки\n\nКто сколько внёс, последние платежи, "
                "размеры взносов и отмена ошибочных оплат.",
                reply_markup=_money2_markup())

        elif what == "play":
            await query.edit_message_text(
                "🏀 Игры\n\nСостав, создание игры.",
                reply_markup=_play_markup())

        elif what == "remind" and len(parts) > 2:
            kind = parts[2]
            sent, skipped = await _remind_debtors(query, kind)
            what_title = "за тренировки" if kind == "season" else "за игры"
            if not sent and not skipped:
                await query.answer("Должников нет", show_alert=True)
            else:
                await query.edit_message_text(
                    f"📨 Напоминание {what_title}: отправлено {sent}."
                    + (f"\nНе дошло до {skipped} — не запускали бота."
                       if skipped else ""),
                    reply_markup=_money_markup())

        elif what == "debts":
            text, markup = await asyncio.to_thread(_debts_screen)
            await query.edit_message_text(text, reply_markup=markup,
                                          parse_mode="HTML")

        elif what == "editdebt" and len(parts) > 2:
            _clear_pending(user.id)
            _awaiting_money[user.id] = f"editdebt:{parts[2]}"
            one = next((d for d in await asyncio.to_thread(
                coach_payments.extra_debts) if d["id"] == int(parts[2])), None)
            who = coach_payments.debt_title(one) if one else "?"
            note = (one or {}).get("note") or "без пояснения"
            await query.edit_message_text(
                f"✏️ Долг: {who} — {(one or {}).get('amount', 0)} ₽ ({note}).\n\n"
                "Пришли новую сумму: «500». Можно сразу с пояснением: "
                "«500 мяч».\n\nПередумал — /start.")

        elif what == "closedebt" and len(parts) > 2:
            import coach_payments
            await asyncio.to_thread(coach_payments.close_debt, int(parts[2]))
            await query.answer("Погашен")
            text, markup = await asyncio.to_thread(_debts_screen)
            await query.edit_message_text(text, reply_markup=markup,
                                          parse_mode="HTML")

        elif what in ("adddebt", "debtpage"):
            # Сначала кому, потом сколько: список тот же, что при оплате.
            # Листание своё (coach:debtpage), иначе стрелки уводили в поток
            # оплаты и выбрасывали в корень раздела.
            page = int(parts[2]) if what == "debtpage" and len(parts) > 2 else 0
            _clear_pending(user.id)
            _awaiting_money[user.id] = "debtwho"     # ждём фамилию текстом
            markup = await asyncio.to_thread(_pay_players_markup, page, "",
                                             "coach:debtwho")
            await query.edit_message_text(
                "➕ Кому добавить долг?\n\nВыбери из списка или напиши фамилию.",
                reply_markup=markup)

        elif what == "debtfree":
            name = (_debt_draft.get(user.id) or {}).get("who", "")
            if not name:
                text, markup = await asyncio.to_thread(_debts_screen)
                await query.edit_message_text("Имя потерялось — начни заново.\n\n" + text,
                                              reply_markup=markup, parse_mode="HTML")
                return
            _debt_draft[user.id] = {"row": 0, "title": name, "who": name}
            text, markup = _debt_why(user.id)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "debtwhy" and len(parts) > 2:
            kind = parts[2]
            draft = _debt_draft.get(user.id)
            if not draft:
                text, markup = await asyncio.to_thread(_debts_screen)
                await query.edit_message_text("Начни заново.\n\n" + text,
                                              reply_markup=markup, parse_mode="HTML")
                return
            if kind == "own":
                _awaiting_money[user.id] = "debtnote"
                await query.edit_message_text(
                    f"➕ Долг для {draft['title']}.\n\n✍️ За что? Коротко: «мяч», "
                    "«форма», «взнос за турнир».\n\nПередумал — /start.")
                return
            draft["note"] = DEBT_KINDS.get(kind, "")
            _awaiting_money[user.id] = "debtsum"
            await query.edit_message_text(
                f"➕ Долг для {draft['title']} — {draft['note']}.\n\n"
                "Пришли сумму: «500».\n\nПередумал — /start.")

        elif what == "debtwho" and len(parts) > 2:
            _clear_pending(user.id)
            who = await asyncio.to_thread(coach_payments.player_by_row, int(parts[2]))
            _debt_draft[user.id] = {"row": int(parts[2]),
                                    "title": (who or {}).get("title", ""), "who": ""}
            text, markup = _debt_why(user.id)
            await query.edit_message_text(text, reply_markup=markup)

        elif what == "sums":
            row = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
            text, markup = await asyncio.to_thread(_sums_screen, row)
            await query.edit_message_text(text, reply_markup=markup)

        elif what in ("sumswho", "sumspage"):
            page = int(parts[2]) if what == "sumspage" and len(parts) > 2 else 0
            markup = await asyncio.to_thread(_pay_players_markup, page, "", "coach:sums")
            await query.edit_message_text("👤 Чьи суммы меняем?", reply_markup=markup)

        elif what == "setsum" and len(parts) > 3:
            _clear_pending(user.id)
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
            await query.edit_message_text(text,
                                          reply_markup=InlineKeyboardMarkup([back_money2]))

        elif what == "last":
            text = await asyncio.to_thread(_pay_last_text)
            await query.edit_message_text(text,
                                          reply_markup=InlineKeyboardMarkup([back_money2]))

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
        # «Message is not modified» — не ошибка: экран уже показывает то, что
        # нужно (тренер нажал ту же кнопку дважды). Показывать это как сбой —
        # значит пугать человека на ровном месте.
        if "not modified" in str(e).lower():
            await query.answer("Уже открыто")
            return
        log.error(f"Раздел тренера ({what}): {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}",
                                      reply_markup=InlineKeyboardMarkup([back]))


# ─────────────────────── Частные занятия тренера ───────────────────────────
#
# Отдельное дело тренера: свои люди, свои занятия, свои деньги. К команде
# отношения не имеет и не должно иметь — здесь не появляются игроки из листа,
# отсюда ничего не уходит в общий чат, и напоминаний эти люди не получают
# (телеграма их бот не знает). Всё, что происходит внутри, принадлежит тому,
# кто это завёл: у каждого тренера свой список, соседу он не виден.
#
# Хранение и правила — в private_lessons.py, здесь только экраны.

# Что тренер сейчас вводит в разделе частных занятий: id → «add», «new»,
# «pay:<id>», «price», «sprice:<id>», «pprice:<id>».
_awaiting_priv: Dict[int, str] = {}

PRIV_INTRO = ("🎾 Частные занятия\n\n"
              "Отдельно от команды: свой список людей, свои цены, свои деньги. "
              "В общий чат отсюда ничего не уходит.")

PRIV_ASK_WHO = ("👤 Кого добавить?\n\n"
                "Напиши коротко: «Иванов И.» или «Петя».\n\n"
                "Полные имена я намеренно укорачиваю до фамилии с инициалом — "
                "эти люди нигде больше в боте не заведены, и хранить их "
                "паспортные данные незачем.\n\n"
                "Передумал — /start.")

PRIV_ASK_WHEN = ("📅 Когда занятие?\n\n"
                 "Одной строкой, как удобно: «12.08 19:00 Зал на Ленина», "
                 "«завтра 19:00», «сегодня».\n\n"
                 "Передумал — /start.")

PRIV_PER_PAGE = 8

# Сколько знаков имени влезает в подпись кнопки рядом с ценой и галочкой.
PRIV_NAME = 18


def _rub(amount: int) -> str:
    """1500 → «1 500 ₽». Неразрывный пробел: сумма не рвётся переносом строки."""
    return f"{int(amount):,}".replace(",", "\u00a0") + "\u00a0₽"


def _priv_main(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    # Раскладываем расписание при входе в раздел: фоновой задачи здесь нет
    # намеренно, а к моменту, когда тренер смотрит на занятия, они уже нужны.
    pl.ensure_ahead(uid)
    folk = pl.people(uid)
    debt = sum(p["balance"] for p in folk if p["balance"] > 0)
    now = pl.month(uid)
    price = pl.general_price(uid)

    lines = [PRIV_INTRO, ""]
    lines.append(f"Занятие стоит {_rub(price)}." if price
                 else "⚠️ Цена занятия не задана — начислять пока нечего.")
    lines.append(f"Людей: {len(folk)}"
                 + (f" · должны {_rub(debt)}" if debt else ""))
    plans = pl.series_list(uid)
    if plans:
        lines += ["", "Расписание: " + "; ".join(pl.series_title(s) for s in plans)]
    if now["sessions"] or now["paid"]:
        # Раньше стояло одно число — «получено», — и оно ничего не объясняло:
        # тренер видел 8 100 при шести занимавшихся по 900 и не понимал, откуда
        # оно. Рядом с ним начислено, и любое расхождение сразу видно.
        lines += ["", f"{pl.month_title(now['ym']).capitalize()}: "
                      f"занятий {now['sessions']}",
                  f"Начислено {_rub(now['charged'])} · "
                  f"получено {_rub(now['paid'])}"]
        if now.get("ahead"):
            lines.append(f"Из них авансом: {_rub(now['ahead'])}")
    return "\n".join(lines), InlineKeyboardMarkup([
        [InlineKeyboardButton("📅 Занятия", callback_data="pl:days")],
        [InlineKeyboardButton("👥 Люди", callback_data="pl:who")],
        [InlineKeyboardButton("💰 Деньги", callback_data="pl:cash")],
        [InlineKeyboardButton("💵 Цена занятия", callback_data="pl:price")],
        [InlineKeyboardButton("⬅️ В раздел тренера", callback_data="coach:main")],
    ])


def _priv_days(uid: int, page: int = 0) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    pl.ensure_ahead(uid)
    got = pl.sessions(uid, limit=200)
    total = len(got)
    chunk = got[page * PRIV_PER_PAGE:(page + 1) * PRIV_PER_PAGE]

    rows = []
    for s in chunk:
        mark = "✅" if s["status"] == pl.DONE else "🕒"
        loop = "🔁" if int(s.get("series_id") or 0) else ""
        when = pl.human_date(s["day"]) + (f" {s['at_time']}" if s["at_time"] else "")
        rows.append([InlineKeyboardButton(
            f"{mark}{loop} {when} · {s['going']} чел."[:BTN_TEXT],
            callback_data=f"pl:s:{s['id']}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"pl:days:{page - 1}"))
    if (page + 1) * PRIV_PER_PAGE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"pl:days:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([InlineKeyboardButton("➕ Новое занятие", callback_data="pl:new")])
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="pl:main")])

    head = "📅 Занятия\n\nБлижайшие сверху, дальше прошедшие."
    if not got:
        head = "📅 Занятий пока нет.\n\nЗаведи первое — дальше отметишь, кто пришёл."
    return head, InlineKeyboardMarkup(rows)


def _priv_session(uid: int, sid: int) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    s = pl.session(uid, sid)
    if not s:
        return "Занятие не найдено — возможно, оно отменено.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К занятиям", callback_data="pl:days")]])

    done = s["status"] == pl.DONE
    when = pl.human_date(s["day"]) + (f", {s['at_time']}" if s["at_time"] else "")
    lines = [("✅ " if done else "🕒 ") + when + (f" · {s['place']}" if s["place"] else "")]
    price = s["price"] or pl.general_price(uid)
    lines.append(f"Цена занятия: {_rub(price)}" if price
                 else "⚠️ Цена не задана — начислять будет нечего.")
    row = pl.series(uid, s["series_id"]) if int(s.get("series_id") or 0) else None
    if row:
        lines.append(f"🔁 Повторяется {pl.series_title(row)}")
    lines.append("")

    if not s["members"]:
        lines.append("Пока никто не записан.")
    else:
        lines.append(("Были" if done else "Идут") +
                     f" ({len(s['members'])}) — {_rub(s['total'])}:")
        for m in s["members"]:
            tail = ""
            if done:
                tail = " ✅ оплатил" if m["paid"] else " ❗ должен"
            own = " (своя цена)" if int(m.get("price_own") or 0) else ""
            lines.append(f"• {m['label']} — {_rub(m['price'])}{own}{tail}")

    rows = []
    if done:
        # После занятия главное действие — отметить, кто рассчитался.
        # Нажатие по человеку и есть отметка: отдельный экран ради галочки
        # заставлял бы ходить туда-обратно за каждым.
        for m in s["members"]:
            rows.append([InlineKeyboardButton(
                f"{'✅' if m['paid'] else '▫️'} {m['label'][:PRIV_NAME]} · "
                f"{m['price']} ₽", callback_data=f"pl:paid:{sid}:{m['id']}")])
    rows.append([InlineKeyboardButton("✏️ Кто идёт", callback_data=f"pl:pick:{sid}"),
                 InlineKeyboardButton("🕒 Когда", callback_data=f"pl:when:{sid}")])
    if not done and s["members"]:
        rows.append([InlineKeyboardButton("✅ Занятие прошло",
                                          callback_data=f"pl:done:{sid}")])
    rows.append([InlineKeyboardButton("💵 Цена этого занятия",
                                      callback_data=f"pl:sprice:{sid}")])
    if s["members"]:
        rows.append([InlineKeyboardButton("💵 Цена по людям",
                                          callback_data=f"pl:pp:{sid}")])
    rows.append([InlineKeyboardButton(
        "🔁 Повторение" if row else "🔁 Повторять каждую неделю",
        callback_data=f"pl:rep:{sid}")])
    rows.append([InlineKeyboardButton("🗑 Отменить занятие",
                                      callback_data=f"pl:off:{sid}")])
    rows.append([InlineKeyboardButton("⬅️ К занятиям", callback_data="pl:days")])
    if done:
        lines += ["", "Нажми на человека — отметится оплата."]
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _priv_spot_prices(uid: int, sid: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Цена каждому на этом занятии. Разовая, не трогает постоянную.

    Бывает, что с одного сегодня берут меньше: пришёл на полчаса, привёл
    друга, отрабатывает пропуск. Менять ради этого его постоянную цену нельзя —
    она вернётся не сразу и не вспомнится."""
    import private_lessons as pl
    s = pl.session(uid, sid)
    if not s:
        return "Занятие не найдено.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К занятиям", callback_data="pl:days")]])
    when = pl.human_date(s["day"]) + (f", {s['at_time']}" if s["at_time"] else "")
    lines = [f"💵 Цена на {when}", ""]
    if not s["members"]:
        lines.append("На это занятие пока никто не записан.")
    for m in s["members"]:
        why = ("разовая" if m.get("price_once") else
               "своя" if m.get("price_own") else "обычная")
        tail = " · уже начислено" if m["charged"] else ""
        lines.append(f"• {m['label']} — {_rub(m['price'])} ({why}){tail}")
    if any(m["charged"] for m in s["members"]):
        lines += ["", "Начисленное правка цены не изменит: занятие прошло по "
                      "той цене, что была."]
    rows = [[InlineKeyboardButton(
        f"{m['label'][:PRIV_NAME]} · {m['price']} ₽"[:BTN_TEXT],
        callback_data=f"pl:sp:{sid}:{m['id']}")] for m in s["members"]]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:s:{sid}")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _priv_repeat(uid: int, sid: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Повторять это занятие или уже повторяется.

    Экран один на оба случая: тренер приходит сюда с одним вопросом — «что с
    повторением этого занятия», а не «включить» или «выключить» по отдельности."""
    import private_lessons as pl
    s = pl.session(uid, sid)
    if not s:
        return "Занятие не найдено.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К занятиям", callback_data="pl:days")]])
    back = [InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:s:{sid}")]
    row = pl.series(uid, s["series_id"]) if int(s.get("series_id") or 0) else None

    if row:
        ahead = [x for x in pl.sessions(uid, limit=200)
                 if int(x.get("series_id") or 0) == int(row["id"])
                 and x["day"] >= date.today().isoformat()]
        lines = [f"🔁 Повторяется {pl.series_title(row)}", "",
                 f"Впереди заведено: {len(ahead)}."]
        if s["members"]:
            lines.append("Люди подставляются те же — на каждой дате их можно "
                         "менять, это не тронет остальные.")
        lines += ["",
                  "Отменить одну дату — «🗑 Отменить занятие» на самой дате: "
                  "расписание от этого не ломается, и обратно она не появится.",
                  "",
                  "Перенести время — останови повторение и включи заново с "
                  "занятия в новое время."]
        return "\n".join(lines), InlineKeyboardMarkup([
            [InlineKeyboardButton("⏹ Остановить повторение",
                                  callback_data=f"pl:repoff:{row['id']}:{sid}")],
            back])

    when = pl.human_date(s["day"]) + (f", {s['at_time']}" if s["at_time"] else "")
    day = pl.DAY_EVERY[datetime.strptime(s["day"], "%Y-%m-%d").weekday()]
    lines = [f"🔁 Повторять занятие {when}?", "",
             f"Заведу его {day}" + (f" в {s['at_time']}" if s["at_time"] else "")
             + f" на {pl.AHEAD_WEEKS} недели вперёд.",
             "",
             f"Место, цена и записанные ({len(s['members'])}) перенесутся. "
             "Каждая дата остаётся отдельной: состав и цену на ней можно "
             "поменять, не трогая остальные."]
    return "\n".join(lines), InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 Каждую неделю",
                              callback_data=f"pl:repon:{sid}:1")],
        [InlineKeyboardButton("🔁 Раз в две недели",
                              callback_data=f"pl:repon:{sid}:2")],
        back])


def _priv_pick(uid: int, sid: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Кто идёт на занятие. Нажатие переключает — состав меняется до последнего."""
    import private_lessons as pl
    s = pl.session(uid, sid)
    if not s:
        return "Занятие не найдено.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К занятиям", callback_data="pl:days")]])
    folk = pl.people(uid)
    rows = [[InlineKeyboardButton(
        f"{'✅' if int(p['id']) in s['going'] else '▫️'} {p['label'][:BTN_TEXT - 3]}",
        callback_data=f"pl:t:{sid}:{p['id']}")] for p in folk]
    rows.append([InlineKeyboardButton("➕ Новый человек",
                                      callback_data=f"pl:add:{sid}")])
    rows.append([InlineKeyboardButton("⬅️ Готово", callback_data=f"pl:s:{sid}")])
    when = pl.human_date(s["day"]) + (f", {s['at_time']}" if s["at_time"] else "")
    head = f"✏️ Кто идёт {when}\n\nНажми, чтобы записать или снять."
    if not folk:
        head = ("👥 В списке пока никого.\n\n"
                "Заведи людей — потом будешь отмечать их на занятия.")
    return head, InlineKeyboardMarkup(rows)


def _priv_who(uid: int, archived: bool = False) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    folk = pl.people(uid, archived=archived)
    rows = []
    for p in folk:
        mark = "❗" if p["balance"] > 0 else ("💚" if p["balance"] < 0 else "▫️")
        tail = f" · {p['balance']} ₽" if p["balance"] > 0 else ""
        rows.append([InlineKeyboardButton(
            f"{mark} {p['label'][:PRIV_NAME]}{tail}"[:BTN_TEXT],
            callback_data=f"pl:p:{p['id']}")])
    if not archived:
        rows.append([InlineKeyboardButton("➕ Добавить", callback_data="pl:add")])
        rows.append([InlineKeyboardButton("🗄 Архив", callback_data="pl:arc")])
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="pl:main")])
        head = ("👥 Кто ходит\n\n❗ — за человеком долг, 💚 — аванс."
                if folk else
                "👥 Пока никого.\n\nДобавь первого — дальше он появится в "
                "списке на каждое занятие.")
    else:
        rows.append([InlineKeyboardButton("⬅️ К людям", callback_data="pl:who")])
        head = ("🗄 Архив\n\nЭти в списке на занятие не показываются, но их "
                "долги и история никуда не делись."
                if folk else "🗄 Архив пуст.")
    return head, InlineKeyboardMarkup(rows)


def _priv_person(uid: int, pid: int) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    p = pl.person(uid, pid)
    if not p:
        return "Такого человека в списке нет.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К людям", callback_data="pl:who")]])
    own = int(p["price"] or 0)
    lines = [f"👤 {p['label']}", "",
             pl.money_word(p["balance"]).capitalize(),
             f"Цена: {_rub(own)} (своя)" if own
             else f"Цена: {_rub(pl.general_price(uid))} (общая)"]
    hist = pl.history(uid, pid, limit=3)
    if hist:
        lines += ["", "Последнее:"]
        for h in hist:
            sign = "+" if h["kind"] == pl.PAY else "−"
            lines.append(f"• {pl.human_date(h['at'])} {sign}{_rub(h['amount'])}"
                         + (f" · {h['note']}" if h["note"] else ""))
    rows = [[InlineKeyboardButton("💰 Внести оплату", callback_data=f"pl:pay:{pid}")],
            [InlineKeyboardButton("💵 Своя цена", callback_data=f"pl:pprice:{pid}"),
             InlineKeyboardButton("✏️ Имя", callback_data=f"pl:rename:{pid}")],
            [InlineKeyboardButton("📜 История", callback_data=f"pl:hist:{pid}")]]
    if p["active"]:
        rows.append([InlineKeyboardButton("🗄 В архив", callback_data=f"pl:arch:{pid}"),
                     InlineKeyboardButton("🗑 Удалить", callback_data=f"pl:del:{pid}")])
    else:
        rows.append([InlineKeyboardButton("↩️ Вернуть в список",
                                          callback_data=f"pl:back:{pid}")])
        rows.append([InlineKeyboardButton("🗑 Удалить", callback_data=f"pl:del:{pid}")])
    rows.append([InlineKeyboardButton("⬅️ К людям", callback_data="pl:who")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _priv_delete_ask(uid: int, pid: int) -> Tuple[str, InlineKeyboardMarkup]:
    """Подтверждение удаления — со счётом того, что пропадёт.

    Человека, заведённого по ошибке, надо сносить одним нажатием без нотаций.
    А вот вместе с тем, кто год ходил и платил, из итогов месяца молча уйдут
    его деньги — про это обязаны сказать числами, и рядом предложить архив: в
    девяти случаях из десяти тренер имел в виду именно его."""
    import private_lessons as pl
    p = pl.person(uid, pid)
    if not p:
        return "Такого человека в списке нет.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К людям", callback_data="pl:who")]])
    was = pl.person_stats(uid, pid)
    back = [InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:p:{pid}")]

    if not was["records"] and not was["visits"]:
        return (f"🗑 Удалить «{p['label']}»?\n\nЗа ним ничего не числится — "
                "уйдёт без следа.",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑 Да, удалить",
                                          callback_data=f"pl:del2:{pid}")], back]))

    lines = [f"🗑 Удалить «{p['label']}»?", "", "Вместе с ним пропадут:"]
    if was["visits"]:
        lines.append(f"• записи на занятия — {was['visits']}")
    if was["charged"]:
        lines.append(f"• начислено — {_rub(was['charged'])}")
    if was["paid"]:
        lines.append(f"• получено от него — {_rub(was['paid'])}")
    if was["paid"]:
        lines += ["", "Это изменит итоги месяца: полученные деньги перестанут "
                      "в них считаться."]
    if was["balance"] > 0:
        lines += ["", f"Долг {_rub(was['balance'])} тоже исчезнет — "
                      "спросить будет не с кого."]
    lines += ["", "Если нужно просто убрать из списка на занятия — это архив, "
                  "там всё сохранится."]
    return "\n".join(lines), InlineKeyboardMarkup([
        [InlineKeyboardButton("🗄 Лучше в архив", callback_data=f"pl:arch:{pid}")],
        [InlineKeyboardButton("🗑 Всё равно удалить", callback_data=f"pl:del2:{pid}")],
        back])


def _priv_history(uid: int, pid: int) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    p = pl.person(uid, pid)
    if not p:
        return "Такого человека в списке нет.", InlineKeyboardMarkup(
            [[InlineKeyboardButton("⬅️ К людям", callback_data="pl:who")]])
    hist = pl.history(uid, pid, limit=30)
    lines = [f"📜 {p['label']} — {pl.money_word(p['balance'])}", ""]
    if not hist:
        lines.append("Движения денег пока не было.")
    for h in hist:
        sign = "+" if h["kind"] == pl.PAY else "−"
        lines.append(f"{pl.human_date(h['at'])}  {sign}{_rub(h['amount'])}"
                     + (f"  · {h['note']}" if h["note"] else ""))
    # Удалять даём только последние: чаще всего ошибаются в только что
    # введённом, а длинный список кнопок «удалить» — приглашение промахнуться.
    rows = [[InlineKeyboardButton(
        f"🗑 {pl.human_date(h['at'])} · {h['amount']} ₽"[:BTN_TEXT],
        callback_data=f"pl:rm:{pid}:{h['id']}")] for h in hist[:3]]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:p:{pid}")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _priv_cash(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    now = pl.month(uid)
    lines = [f"💰 Деньги · {pl.month_title(now['ym'])}", "",
             f"Занятий провёл: {now['sessions']}",
             f"Начислено: {_rub(now['charged'])}",
             f"Получено: {_rub(now['paid'])}"]
    if now.get("ahead"):
        # Иначе «получено больше, чем начислено» читается как ошибка счёта.
        lines.append(f"Из них авансом: {_rub(now['ahead'])}")
    if now["debt"]:
        lines += ["", f"Всего долгов на сегодня: {_rub(now['debt'])}"]
    return "\n".join(lines), InlineKeyboardMarkup([
        [InlineKeyboardButton("💸 Кто должен", callback_data="pl:debts")],
        [InlineKeyboardButton("🧾 Последние оплаты", callback_data="pl:last")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="pl:main")],
    ])


def _priv_debts(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    owe = pl.debtors(uid)
    lines = ["💸 Кто должен", ""]
    if not owe:
        lines.append("Никто. Все рассчитались.")
    for p in owe:
        lines.append(f"• {p['label']} — {_rub(p['balance'])}")
    if owe:
        lines += ["", f"Итого: {_rub(sum(p['balance'] for p in owe))}",
                  "", "Нажми на человека, чтобы внести оплату."]
    rows = [[InlineKeyboardButton(
        f"{p['label'][:PRIV_NAME]} · {p['balance']} ₽"[:BTN_TEXT],
        callback_data=f"pl:p:{p['id']}")] for p in owe[:10]]
    rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="pl:cash")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _priv_last(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    import private_lessons as pl
    got = pl.payments(uid, limit=15)
    lines = ["🧾 Последние оплаты", ""]
    if not got:
        lines.append("Оплат пока не было.")
    for m in got:
        lines.append(f"{pl.human_date(m['at'])}  {m['label'] or '?'} — "
                     f"{_rub(m['amount'])}"
                     + (f"  · {m['note']}" if m["note"] else ""))
    return "\n".join(lines), InlineKeyboardMarkup(
        [[InlineKeyboardButton("⬅️ Назад", callback_data="pl:cash")]])


async def handle_private_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Кнопки частных занятий. Владелец всего — тот, кто нажимает."""
    import private_lessons as pl
    query = update.callback_query
    user = query.from_user if query else None
    if not query or not _can_see_reports(user):
        if query:
            await query.answer("Нет доступа", show_alert=True)
        return
    await query.answer()
    parts = (query.data or "").split(":")
    what = parts[1] if len(parts) > 1 else "main"
    arg = parts[2] if len(parts) > 2 else ""
    uid = user.id
    back_main = [InlineKeyboardButton("⬅️ Назад", callback_data="pl:main")]

    try:
        if what == "main":
            _clear_pending(uid)
            text, markup = await asyncio.to_thread(_priv_main, uid)

        elif what == "days":
            page = int(arg) if arg.isdigit() else 0
            text, markup = await asyncio.to_thread(_priv_days, uid, page)

        elif what == "s":
            text, markup = await asyncio.to_thread(_priv_session, uid, int(arg))

        elif what == "pick":
            text, markup = await asyncio.to_thread(_priv_pick, uid, int(arg))

        elif what == "t" and len(parts) > 3:
            await asyncio.to_thread(pl.toggle_visit, uid, int(arg), int(parts[3]))
            text, markup = await asyncio.to_thread(_priv_pick, uid, int(arg))

        elif what == "done":
            res = await asyncio.to_thread(pl.close_session, uid, int(arg))
            if res.get("free"):
                # Цену не задали — начислять нечего, и молчать об этом нельзя:
                # тренер решит, что долг записан, а его нет.
                await query.answer("Цена не задана — начислений нет", show_alert=True)
            text, markup = await asyncio.to_thread(_priv_session, uid, int(arg))

        elif what == "paid" and len(parts) > 3:
            await asyncio.to_thread(pl.toggle_paid, uid, int(arg), int(parts[3]))
            text, markup = await asyncio.to_thread(_priv_session, uid, int(arg))

        elif what == "off":
            # Отмена занятия снимает начисления, поэтому спрашиваем.
            text = ("🗑 Отменить занятие?\n\nНачисления за него снимутся. "
                    "Внесённые деньги останутся — станут авансом и закроют "
                    "следующее занятие.")
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Да, отменить",
                                      callback_data=f"pl:offok:{arg}")],
                [InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:s:{arg}")]])

        elif what == "offok":
            await asyncio.to_thread(pl.cancel_session, uid, int(arg))
            text, markup = await asyncio.to_thread(_priv_days, uid, 0)
            text = "🗑 Отменил.\n\n" + text

        elif what == "new":
            _clear_pending(uid)
            _awaiting_priv[uid] = "new"
            text, markup = PRIV_ASK_WHEN, InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Назад", callback_data="pl:days")]])

        elif what == "who":
            text, markup = await asyncio.to_thread(_priv_who, uid, False)

        elif what == "arc":
            text, markup = await asyncio.to_thread(_priv_who, uid, True)

        elif what == "add":
            _clear_pending(uid)
            # Из экрана «кто идёт» человек заводится под конкретное занятие —
            # туда и вернёмся, сразу записав его.
            _awaiting_priv[uid] = f"add:{arg}" if arg.isdigit() else "add"
            where = f"pl:pick:{arg}" if arg.isdigit() else "pl:who"
            text, markup = PRIV_ASK_WHO, InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data=where)]])

        elif what == "rep":
            text, markup = await asyncio.to_thread(_priv_repeat, uid, int(arg))

        elif what == "repon" and len(parts) > 3:
            res = await asyncio.to_thread(pl.repeat_session, uid, int(arg),
                                          int(parts[3]))
            if res.get("error"):
                await query.answer(res["error"], show_alert=True)
                text, markup = await asyncio.to_thread(_priv_session, uid, int(arg))
            else:
                text, markup = await asyncio.to_thread(_priv_session, uid, int(arg))
                text = (f"🔁 Готово: {res['title']}. Завёл дат: {res['made']}."
                        f"\n\n{text}")

        elif what == "repoff" and len(parts) > 3:
            # Останов сносит заведённые вперёд даты — спрашиваем, сколько.
            ahead = await asyncio.to_thread(
                lambda: len([x for x in pl.sessions(uid, limit=200)
                             if int(x.get("series_id") or 0) == int(arg)
                             and x["status"] == pl.PLAN
                             and x["day"] >= date.today().isoformat()]))
            text = ("⏹ Остановить повторение?\n\n"
                    f"Уберу {ahead} заведённых вперёд дат — их создало "
                    "расписание, а не ты.\nВсё, что уже прошло, останется: там "
                    "деньги и история.")
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("⏹ Да, остановить",
                                      callback_data=f"pl:repoff2:{arg}:{parts[3]}")],
                [InlineKeyboardButton("⬅️ Назад",
                                      callback_data=f"pl:rep:{parts[3]}")]])

        elif what == "repoff2" and len(parts) > 3:
            res = await asyncio.to_thread(pl.stop_series, uid, int(arg))
            text, markup = await asyncio.to_thread(_priv_days, uid, 0)
            head = ("⏹ Больше не повторяю."
                    + (f" Убрал дат: {res['dropped']}." if res.get("dropped") else ""))
            text = f"{head}\n\n{text}"

        elif what == "when":
            _clear_pending(uid)
            _awaiting_priv[uid] = f"when:{arg}"
            s = await asyncio.to_thread(pl.session, uid, int(arg))
            now = (pl.human_date((s or {})["day"])
                   + (f", {s['at_time']}" if (s or {}).get("at_time") else "")
                   + (f" · {s['place']}" if (s or {}).get("place") else ""))
            text = (f"🕒 Когда занятие?\n\nСейчас: {now}.\n\n"
                    "Пришли новое одной строкой: «12.08 19:00 Зал на Ленина», "
                    "«завтра 19:00».\n\nЛюди и начисления останутся при нём."
                    "\n\nПередумал — /start.")
            markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:s:{arg}")]])

        elif what == "pp":
            text, markup = await asyncio.to_thread(_priv_spot_prices, uid, int(arg))

        elif what == "sp" and len(parts) > 3:
            _clear_pending(uid)
            _awaiting_priv[uid] = f"sp:{arg}:{parts[3]}"
            s = await asyncio.to_thread(pl.session, uid, int(arg))
            who = next((m for m in (s or {}).get("members", [])
                        if int(m["id"]) == int(parts[3])), None)
            now = int((who or {}).get("price_once") or 0)
            text = (f"💵 Разовая цена для {(who or {}).get('label', '?')} на "
                    f"{pl.human_date((s or {})['day'])}.\n\n"
                    f"Сейчас: {_rub(now) if now else 'обычная'}.\n"
                    "Пришли число, «0» — вернуть обычную.\n\n"
                    "Постоянную цену человека это не меняет — только эту дату."
                    "\n\nПередумал — /start.")
            markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:pp:{arg}")]])

        elif what == "cash":
            text, markup = await asyncio.to_thread(_priv_cash, uid)

        elif what == "debts":
            text, markup = await asyncio.to_thread(_priv_debts, uid)

        elif what == "last":
            text, markup = await asyncio.to_thread(_priv_last, uid)

        elif what == "p":
            text, markup = await asyncio.to_thread(_priv_person, uid, int(arg))

        elif what == "hist":
            text, markup = await asyncio.to_thread(_priv_history, uid, int(arg))

        elif what == "rm" and len(parts) > 3:
            await asyncio.to_thread(pl.drop_money, uid, int(parts[3]))
            text, markup = await asyncio.to_thread(_priv_history, uid, int(arg))

        elif what in ("arch", "back"):
            await asyncio.to_thread(pl.archive, uid, int(arg), what == "back")
            text, markup = await asyncio.to_thread(_priv_person, uid, int(arg))

        elif what == "rename":
            _clear_pending(uid)
            _awaiting_priv[uid] = f"rename:{arg}"
            p = await asyncio.to_thread(pl.person, uid, int(arg))
            text = (f"✏️ Новое имя вместо «{(p or {}).get('label', '?')}».\n\n"
                    "Занятия, оплаты и долг останутся за ним — меняется только "
                    "подпись.\n\nПередумал — /start.")
            markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data=f"pl:p:{arg}")]])

        elif what == "del":
            text, markup = await asyncio.to_thread(_priv_delete_ask, uid, int(arg))

        elif what == "del2":
            gone = await asyncio.to_thread(pl.forget_person, uid, int(arg))
            text, markup = await asyncio.to_thread(_priv_who, uid, False)
            if gone.get("error"):
                text = f"{gone['error']}\n\n{text}"
            else:
                text = ("🗑 Удалил без следа.\n\n" + text) if not gone["records"] \
                    else (f"🗑 Удалил. Вместе с ним ушли {gone['records']} записей "
                          f"о деньгах.\n\n{text}")

        elif what in ("pay", "pprice", "sprice", "price"):
            _clear_pending(uid)
            _awaiting_priv[uid] = f"{what}:{arg}" if arg else what
            text, markup = await asyncio.to_thread(_priv_ask_sum, uid, what, arg)

        else:
            text, markup = await asyncio.to_thread(_priv_main, uid)

        await query.edit_message_text(text, reply_markup=markup)

    except Exception as e:
        if "not modified" in str(e).lower():
            await query.answer("Уже открыто")
            return
        log.error(f"Частные занятия ({what}): {e}")
        await query.edit_message_text(f"⚠️ Не получилось: {e}",
                                      reply_markup=InlineKeyboardMarkup([back_main]))


def _priv_ask_sum(uid: int, what: str, arg: str) -> Tuple[str, InlineKeyboardMarkup]:
    """Экран «пришли сумму». Один на четыре случая — вопросы разные, ввод один."""
    import private_lessons as pl
    tail = "\n\nПередумал — /start."
    if what == "pay":
        p = pl.person(uid, int(arg))
        who = p["label"] if p else "?"
        state = pl.money_word(p["balance"]) if p else ""
        return (f"💰 Оплата от {who} ({state}).\n\nПришли сумму: «1500». "
                f"Можно с пояснением: «3000 за две тренировки».{tail}",
                InlineKeyboardMarkup([[InlineKeyboardButton(
                    "⬅️ Назад", callback_data=f"pl:p:{arg}")]]))
    if what == "pprice":
        p = pl.person(uid, int(arg))
        now = int((p or {}).get("price") or 0)
        return (f"💵 Своя цена для {(p or {}).get('label', '?')}.\n\n"
                f"Сейчас: {_rub(now) if now else 'как у всех'}.\n"
                f"Пришли число, «0» — вернуть общую цену.{tail}",
                InlineKeyboardMarkup([[InlineKeyboardButton(
                    "⬅️ Назад", callback_data=f"pl:p:{arg}")]]))
    if what == "sprice":
        s = pl.session(uid, int(arg))
        now = int((s or {}).get("price") or 0)
        return (f"💵 Цена этого занятия.\n\n"
                f"Сейчас: {_rub(now) if now else 'общая'}.\n"
                f"Пришли число, «0» — вернуть общую цену.\n\n"
                f"Уже начисленное не изменится: занятие прошло по своей цене."
                f"{tail}",
                InlineKeyboardMarkup([[InlineKeyboardButton(
                    "⬅️ Назад", callback_data=f"pl:s:{arg}")]]))
    return (f"💵 Сколько стоит одно занятие?\n\n"
            f"Сейчас: {_rub(pl.general_price(uid)) if pl.general_price(uid) else 'не задана'}."
            f"\nПришли число: «1500».{tail}",
            InlineKeyboardMarkup([[InlineKeyboardButton(
                "⬅️ Назад", callback_data="pl:main")]]))


async def handle_private_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ввод в разделе частных занятий: имя, дата занятия, суммы."""
    import private_lessons as pl
    msg, user = update.effective_message, update.effective_user
    if not msg or not user or user.id not in _awaiting_priv:
        return
    if not _can_see_reports(user):
        _awaiting_priv.pop(user.id, None)
        return
    pending = _awaiting_priv.pop(user.id)
    uid = user.id
    text = (msg.text or "").strip()
    kind, _, arg = pending.partition(":")

    if kind == "add":
        made = await asyncio.to_thread(pl.add_person, uid, text)
        if made.get("error"):
            _awaiting_priv[uid] = pending          # даём ввести другое имя
            await msg.reply_text(made["error"] + "\n\nПришли другое имя.")
            raise ApplicationHandlerStop
        if arg.isdigit():
            # Заводили под занятие — сразу записываем на него.
            await asyncio.to_thread(pl.toggle_visit, uid, int(arg), made["id"])
            screen, markup = await asyncio.to_thread(_priv_pick, uid, int(arg))
            await msg.reply_text(f"Добавил: {made['label']}.\n\n{screen}",
                                 reply_markup=markup)
            raise ApplicationHandlerStop
        screen, markup = await asyncio.to_thread(_priv_who, uid, False)
        await msg.reply_text(f"Добавил: {made['label']}.\n\n{screen}",
                             reply_markup=markup)
        raise ApplicationHandlerStop

    if kind == "rename":
        res = await asyncio.to_thread(pl.rename_person, uid, int(arg), text)
        if res.get("error"):
            _awaiting_priv[uid] = pending          # даём ввести другое имя
            await msg.reply_text(res["error"] + "\n\nПришли другое имя.")
            raise ApplicationHandlerStop
        screen, markup = await asyncio.to_thread(_priv_person, uid, int(arg))
        await msg.reply_text(f"Теперь это {res['label']}.\n\n{screen}",
                             reply_markup=markup)
        raise ApplicationHandlerStop

    if kind == "when":
        got = await asyncio.to_thread(pl.parse_when, text)
        if not got:
            _awaiting_priv[uid] = pending
            await msg.reply_text("Не понял дату. Напиши «12.08 19:00» или "
                                 "«завтра 19:00».\n\nПередумал — /start.")
            raise ApplicationHandlerStop
        await asyncio.to_thread(pl.set_session_when, uid, int(arg), got["day"],
                                got["at_time"], got["place"])
        screen, markup = await asyncio.to_thread(_priv_session, uid, int(arg))
        await msg.reply_text(f"🕒 Перенёс.\n\n{screen}", reply_markup=markup)
        raise ApplicationHandlerStop

    if kind == "new":
        when = await asyncio.to_thread(pl.parse_when, text)
        if not when:
            _awaiting_priv[uid] = pending
            await msg.reply_text("Не понял дату. Напиши «12.08 19:00» или "
                                 "«завтра 19:00».\n\nПередумал — /start.")
            raise ApplicationHandlerStop
        sid = await asyncio.to_thread(pl.add_session, uid, when["day"],
                                      when["at_time"], when["place"])
        # Сразу спрашиваем, кто идёт: занятие без людей ни для чего не нужно,
        # а отдельная кнопка «а теперь выбери состав» — лишний шаг.
        screen, markup = await asyncio.to_thread(_priv_pick, uid, sid)
        await msg.reply_text(f"📅 Завёл: {pl.human_date(when['day'])}"
                             + (f", {when['at_time']}" if when["at_time"] else "")
                             + (f" · {when['place']}" if when["place"] else "")
                             + f"\n\n{screen}", reply_markup=markup)
        raise ApplicationHandlerStop

    m = re.match(r"^\s*(\d{1,7})\s*(.*)$", text)
    if not m:
        _awaiting_priv[uid] = pending
        await msg.reply_text("Нужна сумма числом: «1500».\n\nПередумал — /start.")
        raise ApplicationHandlerStop
    amount, note = int(m.group(1)), m.group(2).strip()

    if kind == "price":
        await asyncio.to_thread(pl.set_general_price, uid, amount)
        screen, markup = await asyncio.to_thread(_priv_main, uid)
        await msg.reply_text(f"Записал: {_rub(amount)} за занятие.\n\n{screen}",
                             reply_markup=markup)
    elif kind == "sprice":
        await asyncio.to_thread(pl.set_session_price, uid, int(arg), amount)
        screen, markup = await asyncio.to_thread(_priv_session, uid, int(arg))
        await msg.reply_text(screen, reply_markup=markup)
    elif kind == "sp":
        sid, _, pid = arg.partition(":")
        await asyncio.to_thread(pl.set_visit_price, uid, int(sid), int(pid), amount)
        screen, markup = await asyncio.to_thread(_priv_spot_prices, uid, int(sid))
        await msg.reply_text(screen, reply_markup=markup)
    elif kind == "pprice":
        await asyncio.to_thread(pl.set_person_price, uid, int(arg), amount)
        screen, markup = await asyncio.to_thread(_priv_person, uid, int(arg))
        await msg.reply_text(screen, reply_markup=markup)
    elif kind == "pay":
        await asyncio.to_thread(pl.add_payment, uid, int(arg), amount, note)
        screen, markup = await asyncio.to_thread(_priv_person, uid, int(arg))
        await msg.reply_text(f"Записал оплату {_rub(amount)}.\n\n{screen}",
                             reply_markup=markup)
    raise ApplicationHandlerStop


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
                # Здесь стояли `import fantasy_api` и `import sheets_cache`. Оба
                # модуля импортированы на уровне файла, а локальный импорт делал
                # имя ЛОКАЛЬНЫМ на всю функцию — и любое обращение к нему из
                # других веток падало с UnboundLocalError. Ломались выдача
                # доступа по нику, «Каналы связи», пересчёт цен и синхронизация,
                # а заодно и сам обработчик ошибок внизу.
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
                _clear_pending(user.id)
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
                _clear_pending(user.id)
                _awaiting_video[user.id] = f"{parts[3]}:{parts[4]}"
                await query.edit_message_text(VIDEO_ASK)
            else:
                _awaiting_video.pop(user.id, None)
                text, markup = await asyncio.to_thread(_video_screen)
                await query.edit_message_text(text, reply_markup=markup)
        elif parts[1] == "backup":
            # Снимаем свежую и присылаем сюда же: проверить, что копии живы,
            # человек должен с телефона, а не заходя на сервер по ssh.
            import backup_db
            await query.edit_message_text("⏳ Снимаю копию…")
            info = await asyncio.to_thread(backup_db.make_local)
            await asyncio.to_thread(backup_db.rotate_local)
            got = await _backup_to_telegram(context.application, info["path"])
            have = sorted(backup_db.LOCAL_DIR.glob(
                f"{backup_db.PREFIX}*{backup_db.SUFFIX}"), reverse=True)
            lines = [f"💾 Копия снята: {info['path'].name}",
                     f"{info['bytes'] / 1024 / 1024:.1f} МБ (из "
                     f"{info['raw_bytes'] / 1024 / 1024:.1f} МБ), "
                     f"таблиц {info['tables']}", ""]
            lines.append("Файл отправлен сюда же." if got
                         else "⚠️ Файл отправить не вышло — см. лог.")
            lines += ["", f"Всего копий на сервере: {len(have)}"]
            lines += [f"   {p.name}" for p in have[:8]]
            await query.edit_message_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup([_back_button("admin:menu:service")]))

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
            elif screen == "games":
                await query.edit_message_text(
                    "🏀 Игры и записи\n\nЧто уже сыграно: записи в ВК, "
                    "тайм-коды по игрокам и полнота скачанных протоколов.",
                    reply_markup=_games_menu_markup())
            elif screen == "people":
                await query.edit_message_text(
                    "👥 Люди и доступы\n\nКто есть кто для бота: связка с "
                    "Telegram, профиль в лиге, дни рождения и ники, "
                    "открытые платные разделы.",
                    reply_markup=_people_menu_markup())
            elif screen == "service":
                await query.edit_message_text(
                    "⚙️ Обслуживание\n\nСюда идут, когда что-то пошло не так: "
                    "синхронизация с таблицей, копия базы, лог, настройки, "
                    "каналы связи.",
                    reply_markup=_service_menu_markup())
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
                    reply_markup=InlineKeyboardMarkup([_back_button("admin:menu:service")]))

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
                # Сначала срок, потом ник: доступ «на разок» — обычное дело
                # (посмотреть отчёт перед турниром), и он должен заканчиваться
                # сам, а не жить, пока о нём не вспомнят.
                title = sheets_cache.ACCESS_TITLES.get(kind, kind)
                rows = [[InlineKeyboardButton(label, callback_data=f"admin:acc:days:{kind}:{days}")]
                        for label, days in ACCESS_PERIODS]
                rows.append([InlineKeyboardButton(
                    "✍️ До определённой даты", callback_data=f"admin:acc:days:{kind}:date")])
                rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="admin:acc:list")])
                await query.edit_message_text(
                    f"🔑 «{title}» — на какой срок открыть?",
                    reply_markup=InlineKeyboardMarkup(rows))
                return
            if what == "days" and len(parts) > 4:
                _clear_pending(user.id)
                _awaiting_coach[user.id] = f"{parts[3]}:{parts[4]}"
                title = sheets_cache.ACCESS_TITLES.get(parts[3], parts[3])
                await query.edit_message_text(
                    COACH_ASK.format(title=title))
                return
            # Выдача из списка игроков — второй способ рядом с выдачей по нику.
            if what == "who" and len(parts) > 3:
                page = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
                text, markup = await asyncio.to_thread(_access_screen, parts[3], page)
                await query.edit_message_text(text, reply_markup=markup)
                return
            if what == "w" and len(parts) > 4:
                text, markup = await asyncio.to_thread(_access_who, parts[3], int(parts[4]))
                await query.edit_message_text(text, reply_markup=markup)
                return
            if what == "set" and len(parts) > 5:
                kind, row, months = parts[3], int(parts[4]), int(parts[5])
                people = await asyncio.to_thread(_access_people, kind)
                p = next((x for x in people if x["row"] == row), None)
                base = date.today()
                if p and p["open"] and p["until"]:
                    base = max(base, date.fromisoformat(p["until"]))
                until = _add_months(base, months).isoformat()
                p = await asyncio.to_thread(_access_open, kind, row, until, str(user.id))
                if not p:
                    await query.answer("Некому открывать", show_alert=True)
                    return
                told = await _access_tell(query.get_bot(), kind, p, until)
                text, markup = await asyncio.to_thread(_access_who, kind, row)
                head = (f"✅ Открыл до {date.fromisoformat(until):%d.%m.%Y}."
                        + ("" if told else " Сказать ему не смог — не запускал бота."))
                await query.edit_message_text(f"{head}\n\n{text}", reply_markup=markup)
                return
            if what == "day" and len(parts) > 4:
                _clear_pending(user.id)
                _awaiting_access[user.id] = f"{parts[3]}:row:{parts[4]}"
                await query.edit_message_text(ACCESS_ASK)
                return
            if what == "off" and len(parts) > 4:
                kind, row = parts[3], int(parts[4])
                people = await asyncio.to_thread(_access_people, kind)
                p = next((x for x in people if x["row"] == row), None)
                if p:
                    await asyncio.to_thread(sheets_cache.revoke_access_id,
                                            kind, p["uid"], p["nick"])
                text, markup = await asyncio.to_thread(_access_who, kind, row)
                await query.edit_message_text(f"✂️ Закрыл.\n\n{text}",
                                              reply_markup=markup)
                return
            if what == "del":
                sheets_cache.revoke_access(parts[3], parts[4])
            text, markup = _render_access_list()
            await query.edit_message_text(text, reply_markup=markup)

        elif parts[1] == "tc":
            what = parts[2] if len(parts) > 2 else "games"
            if what == "who" and len(parts) > 4:
                text, markup = await asyncio.to_thread(_tc_players, parts[3], parts[4])
                await query.edit_message_text(text, reply_markup=markup,
                                              parse_mode="HTML",
                                              disable_web_page_preview=True)
                return
            if what == "show" and len(parts) > 5:
                source, game_id, pid = parts[3], parts[4], parts[5]
                back = [[InlineKeyboardButton(
                    "⬅️ К игрокам",
                    callback_data=f"admin:tc:who:{source}:{game_id}")],
                    [InlineKeyboardButton("⬅️ К играм",
                                          callback_data="admin:tc:games")]]
                text, markup = await asyncio.to_thread(
                    _my_video_game, source, game_id, pid, back)
                await query.edit_message_text(text, reply_markup=markup,
                                              parse_mode="HTML",
                                              disable_web_page_preview=True)
                return
            text, markup = await asyncio.to_thread(_tc_games)
            await query.edit_message_text(text, reply_markup=markup,
                                          disable_web_page_preview=True)
            return

        elif parts[1] == "idn":
            what = parts[2] if len(parts) > 2 else "list"
            if what == "w" and len(parts) > 3:
                text, markup = await asyncio.to_thread(_identity_who, int(parts[3]))
                await query.edit_message_text(text, reply_markup=markup,
                                              disable_web_page_preview=True)
                return
            if what == "set" and len(parts) > 5:
                row, source, pid = int(parts[3]), parts[4], parts[5]
                done = await asyncio.to_thread(_identity_set, row, source, pid,
                                               str(user.id))
                if not done:
                    await query.answer("Не к кому привязывать: нет id",
                                       show_alert=True)
                    return
                text, markup = await asyncio.to_thread(_identity_who, row)
                await query.edit_message_text(f"✅ Привязал.\n\n{text}",
                                              reply_markup=markup,
                                              disable_web_page_preview=True)
                return
            if what == "off" and len(parts) > 4:
                row, source = int(parts[3]), parts[4]
                await asyncio.to_thread(_identity_off, row, source)
                text, markup = await asyncio.to_thread(_identity_who, row)
                await query.edit_message_text(f"✂️ Отвязал.\n\n{text}",
                                              reply_markup=markup,
                                              disable_web_page_preview=True)
                return
            if what == "man" and len(parts) > 3:
                _clear_pending(user.id)
                _awaiting_identity[user.id] = int(parts[3])
                people = await asyncio.to_thread(_identity_people)
                p = next((x for x in people if x["row"] == int(parts[3])), None)
                await query.edit_message_text(
                    f"✍️ Пришли ссылку на страницу игрока в лиге — привяжу её "
                    f"к «{(p or {}).get('title', '')}».\n\n"
                    "• https://slpro.basketstat.ru/player/XXXX\n"
                    "• https://www.fbp.ru/player.html?personId=XXXXXX"
                    "&apiUrl=https://reg.infobasket.su")
                return
            page = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            text, markup = await asyncio.to_thread(_identity_screen, page)
            await query.edit_message_text(text, reply_markup=markup)
            return

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
            if kind == "coach_plan":
                text = await asyncio.to_thread(training_dues.plan_text, period)
                sent = await _tell_coaches(app, text)
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"тренерам: {sent}")

            elif kind == "player_ask":
                stat = await _ask_next_month(app, period)
                await _tell_coaches(app, await asyncio.to_thread(
                    training_dues.delivery_report, period, stat["sent"],
                    stat["failed"], stat["unknown"]))
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"спросили {len(stat['sent'])}")

            elif kind in ("coach_end", "coach_debt"):
                text = await asyncio.to_thread(training_dues.coach_report, period, "end")
                markup = InlineKeyboardMarkup([[InlineKeyboardButton(
                    "🏋️ Отметить оплату", callback_data=f"coach:train:{period}")]])
                sent = await _tell_coaches(app, text, markup)
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"тренерам: {sent}")

            else:
                # player_last и player_debt — должникам за текущий месяц.
                stat = await _remind_players(app, period)
                if stat["sent"] or stat["failed"] or stat["unknown"]:
                    report = await asyncio.to_thread(
                        training_dues.delivery_report, period, stat["sent"],
                        stat["failed"], stat["unknown"])
                    await _tell_coaches(app, report)
                await asyncio.to_thread(
                    training_dues.mark_event, key,
                    f"дошло {len(stat['sent'])}, не дошло "
                    f"{len(stat['failed']) + len(stat['unknown'])}")
                log.info(f"Взносы {period} ({kind}): дошло {len(stat['sent'])}")
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

            elif kind == "coach_pay":
                # Тренеру — ДО того, как бот напишет людям: он должен успеть
                # поправить состав и суммы. За два дня это ещё предупреждение
                # («вот кому уйдёт»), после игры — уже список должников.
                rows = await asyncio.to_thread(game_roster.debtors, source, gid)
                if not rows:
                    await asyncio.to_thread(training_dues.mark_event, key, "должников нет")
                    continue
                step = int(key.rsplit(":", 1)[-1])
                head = (f"📋 Через {abs(step)} дня игра {game_roster.game_label(game)}.\n"
                        f"Вот кому уйдёт напоминание об оплате — проверь состав "
                        f"и суммы." if step < 0 else
                        f"💸 Долги за игру {game_roster.game_label(game)} "
                        f"({step}-й день).")
                text, markup = await asyncio.to_thread(_game_debt_screen, source, gid)
                sent = await _tell_coaches(app, f"{head}\n\n{text}", markup)
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"должников {len(rows)}, тренерам {sent}")
                log.info(f"Оплата игры {source}:{gid}, шаг {step}: должников "
                         f"{len(rows)}, тренерам {sent}")

            elif kind in ("player_before", "player_pay"):
                ahead = kind == "player_before"
                stat = await _remind_game_debtors(app, game, ahead=ahead)
                if _reminder_worth_telling(stat):
                    await _tell_coaches(app, game_roster.delivery_report(
                        game, stat["sent"], stat["failed"], stat["unknown"],
                        ahead=ahead))
                else:
                    log.info(f"Оплата игры {game_roster.game_label(game)}: "
                             "должников нет, тренера не беспокою")
                await asyncio.to_thread(training_dues.mark_event, key,
                                        f"дошло {len(stat['sent'])}")

        except Exception as e:
            log.error(f"Событие по игре ({key}) не отработало: {e}")


def _reminder_worth_telling(stat: Dict[str, List[str]]) -> bool:
    """Стоит ли докладывать тренеру о рассылке напоминаний.

    Нечего сказать — молчим. «Напомнил про оплату. Дошло: 0» при закрытых
    долгах человек читает как поломку, а на деле это просто отчёт ни о чём.
    А вот «дошло 0, зато у пятерых нет телеграма» — уже дело: тренеру надо
    знать, до кого бот достучаться не может."""
    return bool(stat["sent"] or stat["failed"] or stat["unknown"])


async def _remind_game_debtors(app: Application, game: Dict[str, Any],
                               ahead: bool = False) -> Dict[str, List[str]]:
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
            await app.bot.send_message(
                chat_id=int(uid),
                text=game_roster.player_debt_text(game, p, ahead=ahead))
            stat["sent"].append(p["title"])
        except Exception as e:
            # В журнал — строка листа, не фамилия. Журнал ложится на диск и
            # уезжает в бэкапы, а имена по инварианту проекта на диске не
            # живут: их берут из листа в момент показа. Для разбора «кому не
            # дошло» строка и точнее — по ней всё и адресуется.
            log.info(f"Напоминание об игре в строку {p['row']} не доставлено: {e}")
            stat["failed"].append(p["title"])
    return stat


async def _tell_coaches(app: Application, text: str,
                        markup: Optional[InlineKeyboardMarkup] = None,
                        html: bool = False) -> int:
    """Сообщение тренерскому штабу — ТОЛЬКО в личку, никогда в общий чат."""
    sent = 0
    for uid in await asyncio.to_thread(_coach_recipients):
        try:
            await app.bot.send_message(chat_id=int(uid), text=text,
                                       reply_markup=markup,
                                       parse_mode="HTML" if html else None)
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
            log.info(f"Напоминание в строку {row['row']} не доставлено: {e}")
            stat["failed"].append(row["title"])
    return stat


async def _ask_next_month(app: Application, period: str) -> Dict[str, List[str]]:
    """25-го: «будешь заниматься в следующем месяце?» — двумя кнопками.

    Вопрос, а не требование: за зал платят вперёд, и тренеру надо знать, на
    сколько человек его брать. Ответ «нет» снимает отметку активности — бот
    перестаёт считать взнос и требовать долг, пока тренер не вернёт её сам."""
    import training_dues
    stat: Dict[str, List[str]] = {"sent": [], "failed": [], "unknown": []}
    people = await asyncio.to_thread(training_dues.status, period)
    # Долг за ТЕКУЩИЙ месяц дописываем в тот же вопрос, вторым сообщением не шлём.
    cur = training_dues.period_of(date.today())
    debts = {r["row"]: int(r["debt"] or 0)
             for r in await asyncio.to_thread(training_dues.debtors, cur)}
    for row in people:
        uid = await asyncio.to_thread(training_dues.chat_id_of, row["row"])
        if not uid:
            stat["unknown"].append(row["title"])
            continue
        markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Буду заниматься",
                                  callback_data=f"nm:yes:{period}:{row['row']}")],
            [InlineKeyboardButton("🙅 Не буду",
                                  callback_data=f"nm:no:{period}:{row['row']}")]])
        try:
            await app.bot.send_message(
                chat_id=int(uid),
                text=training_dues.ask_text(period, row, debts.get(row["row"], 0)),
                reply_markup=markup)
            stat["sent"].append(row["title"])
        except Exception as e:
            log.info(f"Вопрос про {period} не дошёл до строки {row['row']}: {e}")
            stat["failed"].append(row["title"])
    return stat


async def handle_next_month(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ответ игрока на «будешь заниматься в следующем месяце».

    «Не буду» — снимаем отметку активности: бот перестаёт ждать с человека
    взнос и требовать долг за тренировки. Вернуть активность может только
    тренер, руками в листе «Игроки»: это его решение, а не бота."""
    import coach_payments
    import training_dues
    query = update.callback_query
    user = query.from_user if query else None
    if not query or not user:
        return
    parts = (query.data or "").split(":")
    if len(parts) < 4:
        await query.answer()
        return
    answer, period, row = parts[1], parts[2], int(parts[3])
    person = await asyncio.to_thread(coach_payments.player_by_row, row)
    title = (person or {}).get("title", "")
    await query.answer()
    if answer == "yes":
        text = await asyncio.to_thread(training_dues.confirmed_text, period, row)
        await query.edit_message_text(text)
        await _tell_coaches(context.application,
                            f"✅ {title} подтвердил тренировки в "
                            f"{training_dues.month_title(period)}.")
        return
    ok = await asyncio.to_thread(_drop_active, row)
    await query.edit_message_text(
        f"Жаль. Снял тебя с {training_dues.month_title(period)} — про взносы "
        "больше писать не буду.\n\nПередумаешь — скажи тренеру, он вернёт.")
    await _tell_coaches(
        context.application,
        f"🙅 {title} отказался тренироваться в "
        f"{training_dues.month_title(period)}."
        + ("\n\nОтметку активности снял — взносы с него больше не жду."
           if ok else "\n\n⚠️ Отметку активности снять не вышло, сними руками "
                      "в листе «Игроки»."))


def _drop_active(row: int) -> bool:
    """Снимает отметку активности в листе «Игроки»."""
    import coach_payments
    try:
        import report_common
        book = report_common.init_sheets()
    except Exception as exc:
        log.warning(f"Снятие активности: таблица недоступна: {exc}")
        return False
    person = coach_payments.player_by_row(int(row)) or {}
    return sheets_cache.write_player_field(book, int(row), "active_mark", "",
                                           person.get("title", ""))


async def _send_starting_lineups(app: Application) -> None:
    """За час до игры присылает тренерам стартовый состав.

    Решение принимают перед игрой, и лезть в бота за списком в этот момент
    неудобно — пусть придёт сам. Одна игра — одно сообщение."""
    import coach_lineup
    import training_dues
    try:
        games = await asyncio.to_thread(coach_lineup.upcoming, 1)
    except Exception as e:
        log.warning(f"Стартовый состав: список игр не собрался: {e}")
        return
    for g in games:
        key = f"lineup:{g['source']}:{g['game_id']}"
        if await asyncio.to_thread(training_dues.event_done, key):
            continue
        data = await asyncio.to_thread(coach_lineup.lineup, g["source"],
                                       g["game_id"], "trainings")
        if not data.get("rows"):
            continue
        sent = await _tell_coaches(app, coach_lineup.text(
            data, "🏁 Стартовый состав — через час игра"), html=True)
        await asyncio.to_thread(training_dues.mark_event, key, f"тренерам: {sent}")
        log.info(f"Стартовый состав по игре {g['game_id']} ушёл тренерам ({sent})")


async def _catch_up_prices() -> None:
    """Двигает цены по играм, которые сыграны, но в движении цен не учтены.

    Цены пересчитывает кроновый обработчик результатов, и если он в тот момент
    споткнулся (Google недоступен, лига молчала), движение не происходило уже
    никогда: второго шанса не было. 09.08 из-за этого история цен осталась
    пустой при 13 назревших движениях. Теперь демон догоняет сам."""
    import fantasy
    import fantasy_prices
    import training_dues
    season = await asyncio.to_thread(fantasy.get_active_season)
    if not season:
        return
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        games = [dict(r) for r in conn.execute(
            """SELECT DISTINCT s.source, s.game_id, m.game_date
                 FROM fantasy_game_scores s
                 JOIN game_meta m ON m.source = s.source AND m.game_id = s.game_id
                WHERE s.season_id = ? AND m.game_date >= ?
                ORDER BY m.game_date""",
            (season["id"], fantasy_prices.settings(season).get("since") or "0000"))]
    for g in games:
        key = f"price:{g['source']}:{g['game_id']}"
        if await asyncio.to_thread(training_dues.event_done, key):
            continue
        try:
            res = await asyncio.to_thread(fantasy_prices.recalc, season, None,
                                          False, g["source"], g["game_id"])
        except Exception as e:
            log.warning(f"Догон цен по игре {g['source']}/{g['game_id']}: {e}")
            continue
        moved = len(res.get("changes") or [])
        await asyncio.to_thread(
            training_dues.mark_event, key,
            f"движений {moved}" if res.get("checked") else "наших игроков не было")
        if res.get("changes"):
            log.info(f"Цены после игры {g['source']}/{g['game_id']}: "
                     f"двинулось {len(res['changes'])}, записано {res.get('updated')}")


async def _personal_digests(app: Application) -> None:
    """«Присылать после каждой игры» — настройка была, отправки не было.

    Ждём, пока протокол игры окажется в базе (иначе разбирать нечего), и шлём
    короткий разбор в личку. Одна игра — одно сообщение, повторов нет."""
    import personal_game
    from datetime_utils import get_moscow_time
    # Протокол игры приходит когда угодно — 10.08 он лёг в 04:31 по Москве, и
    # люди получили разбор ночью. Личные сообщения ждут утра: раньше девяти
    # никто их не ждёт, а телефон звенит.
    hour = get_moscow_time().hour
    quiet_from = sheets_cache.get_int_setting("quiet_hour_to", 9)
    quiet_to = sheets_cache.get_int_setting("quiet_hour_from", 22)
    if not (quiet_from <= hour < quiet_to):
        return
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


async def _nightly_backup(app: Application) -> None:
    """Ночная резервная копия базы — раз в сутки, из самого демона.

    Не через cron: файл /etc/cron.d принадлежит root, а демон крутится под
    botuser и о своей базе знает всё сам. Заодно копия переживает переустановку
    сервиса — забыть «поставить ещё и крон» уже нельзя.

    Почему это вообще нужно: составы фэнтези и зафиксированные по играм очки
    не восстановимы ничем. В листы они не уезжают, а из API лиг не выводятся —
    очки считаются один раз, в момент игры, и задним числом не пересчитываются
    (см. deploy/BACKUP.md)."""
    from datetime_utils import get_moscow_time
    now = get_moscow_time()
    hour = sheets_cache.get_int_setting("backup_hour", 3)
    # Не «ровно в три», а «в три или при первой возможности после». Демон могли
    # перезапустить ровно в этот момент, сервер — простоять всё утро; правило
    # «одна копия в сутки» важнее круглого часа. До трёх ночи ждём: там тихо.
    if now.hour < hour:
        return
    if sheets_cache.get_setting("backup_last_day") == now.date().isoformat():
        return
    try:
        import backup_db
        if backup_db.today_copy(now.date()):
            await asyncio.to_thread(sheets_cache.set_setting, "backup_last_day",
                                    now.date().isoformat())
            return
        info = await asyncio.to_thread(backup_db.make_local)
        dropped = await asyncio.to_thread(backup_db.rotate_local)
        await asyncio.to_thread(sheets_cache.set_setting, "backup_last_day",
                                now.date().isoformat())
        log.info(f"Резервная копия: {info['path'].name}, "
                 f"{info['bytes'] / 1024 / 1024:.1f} МБ, таблиц {info['tables']}"
                 + (f", убрано старых: {len(dropped)}" if dropped else ""))
    except Exception as e:
        # День без копии — это то, о чём надо знать сразу, а не когда
        # понадобится восстановление.
        log.error(f"Резервная копия не снялась: {e}")
        sheets_cache.report_error("backup", str(e), _get_spreadsheet())
        return
    # Копию надо унести С СЕРВЕРА, иначе от «умер диск» она не спасает.
    # Раз в неделю, а не каждый день: воскресную мы и так держим восемь недель,
    # а семь файлов в неделю превращают переписку в свалку.
    day = sheets_cache.get_int_setting("backup_send_weekday", 6)   # 6 — воскресенье
    if now.weekday() == day:
        await _backup_to_telegram(app, info["path"])


async def _backup_to_telegram(app: Application, path) -> int:
    """Отправляет копию админам в личку — это и есть её вторая площадка.

    Google Диск не подошёл принципиально: служебные аккаунты не имеют своего
    места, и файл, созданный от их имени, отвергается («Service Accounts do not
    have storage quota») — хоть в своей папке, хоть в чужой. Обходятся это либо
    общим диском (только для Workspace, у нас обычная почта), либо отдельным
    OAuth от живого человека с его согласием.

    Телеграм закрывает ту же задачу без единой новой учётки: файл уходит
    владельцу в личку, лежит в облаке Телеграма, качается на любое устройство.
    3 МБ при потолке в 50 МБ для ботов — запас на годы вперёд."""
    import html
    sent = 0
    size = path.stat().st_size / 1024 / 1024
    caption = (f"💾 Резервная копия базы\n{path.name} · {size:.1f} МБ\n\n"
               "Разворачивается так:\n"
               "<code>gunzip -c файл > data/bot.db</code>\n\n"
               "Подробнее — deploy/BACKUP.md")
    for uid in ADMIN_USER_IDS:
        try:
            with open(path, "rb") as f:
                await app.bot.send_document(chat_id=int(uid), document=f,
                                            filename=path.name, caption=caption,
                                            parse_mode="HTML")
            sent += 1
        except Exception as e:
            log.warning(f"Копию не отправил {uid}: {e}")
    if sent:
        log.info(f"Резервная копия отправлена админам: {sent}")
    return sent


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
            try:
                dropped = await asyncio.to_thread(sheets_cache.purge_expired_access)
                if dropped:
                    log.info(f"Доступы с истёкшим сроком сняты: {dropped}")
            except Exception as e:
                log.warning(f"Уборка доступов: {e}")
            await _catch_up_prices()
            # Новые голосующие появляются каждую неделю, а ники меняются ещё
            # чаще: опознаём по ходу, а не только при перезапуске демона.
            try:
                found = await asyncio.to_thread(sheets_cache.link_from_votes)
                if found:
                    log.info("Опознаны по голосам: "
                             + ", ".join(f"строка {f['player_row']} (@{f['username']})"
                                         for f in found))
            except Exception as e:
                log.warning(f"Опознание по голосам: {e}")
            await _send_starting_lineups(app)
            await _nightly_backup(app)
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
            # С трассировкой: тик делает полтора десятка дел, и по одной строке
            # «can't subtract offset-naive and offset-aware datetimes» непонятно
            # даже, в каком файле искать. Тик при этом обрывается на первой же
            # ошибке — всё, что стояло после неё, в этот раз не выполняется, так
            # что молчать об этом нельзя.
            log.exception(f"Ошибка в фоновом цикле: {e}")
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
        # Игры без даты не считаются в долгах вовсе — чиним при каждом старте.
        try:
            import game_roster
            fixed = await asyncio.to_thread(game_roster.repair_dates)
            if fixed:
                log.info("Дозаполнил дату у игр: %d", fixed)
        except Exception as e:
            log.warning(f"Дозаполнение дат игр: {e}")
        try:
            found = await asyncio.to_thread(sheets_cache.link_from_votes)
            if found:
                log.info("Опознаны по голосам в опросах: "
                         + ", ".join(f"строка {f['player_row']} (@{f['username']})"
                                     for f in found))
        except Exception as e:
            log.warning(f"Опознание по голосам: {e}")
        # Свёрнутые суммы: собираем один раз, если их ещё нет. Дальше их
        # правит только приход новой игры — пересчитывать всё подряд незачем,
        # сыгранное не меняется.
        try:
            import fantasy_stats
            if not await asyncio.to_thread(fantasy_stats.totals_ready):
                rows = await asyncio.to_thread(fantasy_stats.refresh_totals, "", "", True)
                log.info(f"Свёрнутая статистика собрана: строк {rows}")
        except Exception as e:
            log.warning(f"Свёртка статистики не собралась: {e}")
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
    import bot_factory
    builder = (
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
    )
    # Ограничитель скорости. Лимиты Телеграм считает НА ТОКЕН, а токеном
    # пользуются ещё и десяток крон-задач (см. bot_factory) — каждая до сих пор
    # считала себя единственной. При упоре в потолок прилетает 429, цикл
    # рассылки обрывается, и часть людей сообщение не получает вовсе.
    limiter = bot_factory.rate_limiter()
    if limiter is not None:
        builder = builder.rate_limiter(limiter)
    app = builder.build()
    log.info("Ограничитель скорости: "
             + (f"{bot_factory.OVERALL_RATE:g}/с, {bot_factory.GROUP_RATE:g}/мин "
                f"в группу, повторов при 429: {bot_factory.MAX_RETRIES}"
                if limiter else "НЕ подключён (нет aiolimiter)"))

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
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_newgame_text), group=10)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_access_date), group=11)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_private_text), group=12)
    app.add_handler(CallbackQueryHandler(handle_admin_callback, pattern=r"^admin:"))
    app.add_handler(CallbackQueryHandler(handle_prog_callback, pattern=r"^prog:"))
    app.add_handler(CallbackQueryHandler(handle_gamelink_callback, pattern=r"^gl:"))
    app.add_handler(CallbackQueryHandler(handle_next_month, pattern=r"^nm:"))
    app.add_handler(CallbackQueryHandler(handle_joke_callback, pattern=r"^joke:"))
    app.add_handler(CallbackQueryHandler(handle_menu_callback, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(handle_coach_callback, pattern=r"^coach:"))
    app.add_handler(CallbackQueryHandler(handle_roster_callback, pattern=r"^rost:"))
    app.add_handler(CallbackQueryHandler(handle_private_callback, pattern=r"^pl:"))

    log.info("Запуск polling...")
    app.run_polling(
        allowed_updates=["poll_answer", "message", "callback_query"],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
