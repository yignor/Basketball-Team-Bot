#!/usr/bin/env python3
"""
Постоянно работающий демон бота.
Обрабатывает голоса в опросах в реальном времени (вместо hourly GitHub Actions)
и интерактивное админ-меню (/admin) с inline-кнопками.
Запускается как systemd-сервис и работает непрерывно.
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
from telegram import (
    BotCommand,
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

# Mini App фэнтези без туннеля: фронт — статикой на GitHub Pages
# (FANTASY_WEBAPP_URL), данные (пул/состав/таблица) бот кладёт прямо в URL при
# открытии, выбранный состав приходит обратно через Telegram sendData (сервер
# наружу не светим). Открывается reply-кнопкой (sendData работает только так).
FANTASY_WEBAPP_URL = os.getenv("FANTASY_WEBAPP_URL", "").strip()


_WEBAPP_VERSION = str(int(time.time()))


def _webapp_url() -> str:
    """URL Mini App с версией. GitHub Pages и Telegram кешируют страницу; версия
    меняется при каждом перезапуске демона, поэтому после деплоя пользователь
    гарантированно получает свежий фронт, а не старый JS."""
    if not FANTASY_WEBAPP_URL:
        return ""
    sep = "&" if "?" in FANTASY_WEBAPP_URL else "?"
    return f"{FANTASY_WEBAPP_URL}{sep}v={_WEBAPP_VERSION}"


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

    _refresh_poll_cache()
    _refresh_db_cache()
    _periodic_push_local_changes()

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


ADMIN_KEYBOARD_LABEL = "📊 Админ-панель"


def _admin_reply_keyboard() -> ReplyKeyboardMarkup:
    """Постоянная кнопка внизу экрана — открывает то же меню, что и /admin,
    без необходимости печатать команду каждый раз. Видна только админу,
    т.к. отправляется только в его личном чате с ботом."""
    return ReplyKeyboardMarkup(
        [[KeyboardButton(ADMIN_KEYBOARD_LABEL)]],
        resize_keyboard=True,
        is_persistent=True,
    )


async def _send_main_menu(update: Update, with_keyboard: bool = False) -> None:
    for attempt in range(3):
        try:
            if with_keyboard:
                await update.message.reply_text(ADMIN_KEYBOARD_LABEL + " активна ⬇️", reply_markup=_admin_reply_keyboard())
            await update.message.reply_text("📊 Админ-панель", reply_markup=_main_menu_markup())
            return
        except Exception as e:
            log.warning(f"Не удалось отправить главное меню (попытка {attempt + 1}/3): {e}")
            await asyncio.sleep(2)
    log.error("Не удалось отправить главное меню после 3 попыток")


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return

    # Фиксируем ЛЮБОГО пользователя, который запустил бота — не только
    # админа. Нужно для "Список пользователей → В боте".
    try:
        sheets_cache.record_bot_user(_get_spreadsheet(), str(user.id), user.username or "", user.first_name or "")
    except Exception as e:
        log.warning(f"Не удалось записать пользователя бота: {e}")

    if not _is_admin(user):
        return
    _refresh_db_cache()
    _periodic_push_local_changes()
    await _send_main_menu(update, with_keyboard=True)


async def handle_fantasy_notify(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/fantasy_notify — личные тумблеры уведомлений о наборе состава."""
    user = update.effective_user
    chat = update.effective_chat
    if not user or not chat or chat.type != "private":
        return
    import fantasy
    prefs = fantasy.get_notify_prefs(str(user.id))
    await update.message.reply_text(
        "🔔 Уведомления фэнтези о наборе состава.\nНажми, чтобы включить/выключить:",
        reply_markup=_fantasy_notify_markup(prefs))


async def handle_fantasy_notify_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.from_user:
        return
    await query.answer()
    import fantasy
    parts = (query.data or "").split(":")
    if len(parts) < 3:
        return
    kind, value = parts[1], parts[2] == "1"
    prefs = fantasy.set_notify_pref(str(query.from_user.id), kind, value)
    await query.edit_message_text(
        "🔔 Уведомления фэнтези о наборе состава.\nНажми, чтобы включить/выключить:",
        reply_markup=_fantasy_notify_markup(prefs))


async def handle_fantasy_webapp_data(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Приём состава из Mini App (Telegram sendData). Валидируем на сервере и
    сохраняем — клиенту не доверяем."""
    msg = update.effective_message
    user = update.effective_user
    if not msg or not msg.web_app_data or not user:
        return
    import fantasy
    import fantasy_api
    try:
        payload = json.loads(msg.web_app_data.data)
        refs = payload.get("refs") or []
    except (json.JSONDecodeError, TypeError, AttributeError):
        await msg.reply_text("⚠️ Не удалось прочитать состав.")
        return

    uid = str(user.id)
    if not fantasy_api._is_team_member(uid, user.username or ""):
        await msg.reply_text("Фэнтези доступна только игрокам команды.")
        return
    season = fantasy.get_active_season()
    if not season:
        await msg.reply_text("Сейчас нет активного сезона.")
        return
    week_start, sched_locked = fantasy.active_selection(season)
    if sched_locked:
        await msg.reply_text("🔒 Набор на этот тур уже закрыт — игры начались.")
        return
    try:
        pool_refs = {p["ref"] for p in await fantasy_api.build_pool()}
    except Exception:
        pool_refs = None  # пул недоступен — не заваливаем сохранение из-за этого
    err = fantasy.validate_roster(season, refs, pool_refs or None)
    if err:
        size = fantasy.roster_size(season)
        problems = {
            "invalid_roster": f"Нужно выбрать ровно {size} игроков.",
            "unknown_player": "В составе есть игрок не из пула. Открой заново.",
            "too_many_copies": f"Одного игрока можно взять не больше "
                               f"{fantasy.max_per_player(season)} раз(а).",
        }
        await msg.reply_text(problems.get(err, "Состав не прошёл проверку."))
        return

    res = fantasy.save_roster(uid, season["id"], week_start, refs)
    if not res.get("ok"):
        reason = "набор на этот тур уже закрыт" if res.get("error") == "locked" else "не удалось сохранить"
        await msg.reply_text(f"⚠️ {reason.capitalize()}.")
        return
    await msg.reply_text("✅ Состав сохранён! Удачи в туре 🏀")


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
        [InlineKeyboardButton("📋 Лог действий", callback_data="admin:menu:log")],
        [InlineKeyboardButton("📊 Отчёты", callback_data="admin:menu:reports")],
        [InlineKeyboardButton("🏆 Фэнтези лига", callback_data="admin:menu:fantasy")],
        [InlineKeyboardButton("🔄 Синхронизация", callback_data="admin:sync")],
    ])


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
        from slpro_client import SlproClient
        names = [n.strip() for n in os.getenv("SLPRO_TEAM_NAMES", "PullUp Farm,Pull Up Farm").split(",")
                 if n.strip()]
        ctx = await SlproClient().discover_context(names)
        if ctx and ctx.get("stage_id") is not None:
            scopes.append({"source": "slpro", "season_id": str(ctx["season_id"]),
                           "stage_id": str(ctx["stage_id"]),
                           "name": f"SLPRO {ctx.get('season')} · "
                                   f"{ctx.get('division_name') or ctx.get('division')}"})
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
        fantasy_api._pool_cache["data"] = None  # сбросить кеш пула
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


async def _handle_fantasy_action(query, action: str, arg: Optional[str] = None) -> None:
    import fantasy
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
        if parts[1] == "menu":
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

        elif parts[1] == "report":
            kind, period = parts[2], parts[3]
            await _handle_report_action(query, kind, period)

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


async def _background_loop() -> None:
    """Единственный независимый таймер демона. Раньше _refresh_poll_cache/
    _refresh_db_cache/_periodic_push_local_changes срабатывали только
    попутно с входящим трафиком Telegram — во время матча без активности
    в чате это могло надолго задерживать и их, и (что важнее) вотчер
    результатов игр, которому нужно тикать независимо от чата."""
    log.info(f"Фоновый цикл запущен (тик каждые {BACKGROUND_TICK_SECONDS}с)")
    while True:
        try:
            await asyncio.sleep(BACKGROUND_TICK_SECONDS)
            _refresh_poll_cache()
            _refresh_db_cache()
            _periodic_push_local_changes()
            await game_watcher.tick()
        except asyncio.CancelledError:
            log.info("Фоновый цикл остановлен")
            raise
        except Exception as e:
            # Один плохой тик не должен убивать демон и останавливать вотчер навсегда.
            log.error(f"Ошибка в фоновом цикле: {e}")
            sheets_cache.report_error("background_loop", str(e), _get_spreadsheet())


async def on_startup(app: Application) -> None:
    log.info("=" * 50)
    log.info("Бот запущен (long-polling режим)")
    log.info(f"Время старта: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    log.info("=" * 50)
    sheets_cache.init_db()
    _refresh_poll_cache()
    _refresh_db_cache()
    _periodic_push_local_changes()
    try:
        await app.bot.set_my_commands([
            BotCommand("admin", "Админ-панель"),
            BotCommand("start", "Показать кнопку админ-панели"),
            BotCommand("fantasy_notify", "Уведомления фэнтези вкл/выкл"),
        ])
    except Exception as e:
        log.warning(f"Не удалось зарегистрировать список команд: {e}")

    await _setup_menu_button(app)

    global _background_task
    _background_task = asyncio.create_task(_background_loop())

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
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(PollAnswerHandler(handle_poll_answer))
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("admin", handle_admin))
    app.add_handler(CommandHandler("fantasy_notify", handle_fantasy_notify))
    app.add_handler(CallbackQueryHandler(handle_fantasy_notify_callback, pattern=r"^fnotify:"))
    app.add_handler(MessageHandler(filters.StatusUpdate.WEB_APP_DATA, handle_fantasy_webapp_data))
    app.add_handler(MessageHandler(filters.Text([ADMIN_KEYBOARD_LABEL]), handle_admin_button))
    app.add_handler(CallbackQueryHandler(handle_admin_callback, pattern=r"^admin:"))

    log.info("Запуск polling...")
    app.run_polling(
        allowed_updates=["poll_answer", "message", "callback_query"],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
