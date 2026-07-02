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
from pathlib import Path
from typing import List, Optional, Tuple

from dotenv import load_dotenv
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
    Update,
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


# Импортируем логику из collect_votes (переиспользуем без изменений).
# upsert_vote (прямая запись в Sheets) больше не используется здесь — голоса
# локально-первичные, см. sheets_cache.upsert_vote_local.
from collect_votes import (
    _init_sheets,
    load_training_polls,
    classify_vote,
    _get_service_ws,
    _get_or_create_sheet,
    _ensure_attend_header,
    ATTEND_SHEET,
    ATTEND_HEADER,
)
import admin_panel
import sheets_cache
from enhanced_duplicate_protection import duplicate_protection

REPO_DIR = Path(__file__).parent

# Кэш зарегистрированных опросов (обновляем раз в 5 минут)
_poll_cache: dict = {}
_poll_cache_time: float = 0.0
_spreadsheet = None
_attend_ws   = None
_service_ws  = None

# Локальный SQLite-кэш листов Sheets для /admin (обновляем раз в 5 минут)
_db_sync_time: float = 0.0


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
    """Выгружает накопленные локальные изменения (service_records +
    attendance, оба dirty=1) в Sheets. Используется и периодическим
    циклом демона, и кнопкой '🔄 Синхронизация' в /admin."""
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
        # Голоса — локально-первичные (пишем в SQLite сразу, выгрузка в
        # Sheets отдельно, периодически/по кнопке — см. push_attendance).
        sheets_cache.upsert_vote_local(
            tg_poll_id, user_id, username, first_name, last_name,
            vote_text, vote_type, training_date, config_poll_id,
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
}
DAILY_DATA_TYPES = ["ДЕНЬ_РОЖДЕНИЯ", "ОПРОС_ГОЛОСОВАНИЕ", "ОПРОС_ИГРА", "АНОНС_ИГРА"]
DAILY_SCRIPTS = [
    ("run_birthday_notifications.py", []),
    ("training_polls_enhanced.py", []),
    ("run_game_system.py", []),
]


def _main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🚀 Запуск оповещений", callback_data="admin:menu:launch")],
        [InlineKeyboardButton("👥 Список пользователей", callback_data="admin:menu:users")],
        [InlineKeyboardButton("📋 Лог действий", callback_data="admin:menu:log")],
        [InlineKeyboardButton("📊 Отчёты", callback_data="admin:menu:reports")],
        [InlineKeyboardButton("🔄 Синхронизация", callback_data="admin:sync")],
    ])


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
        _back_button(),
    ])


def _reports_training_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("За неделю", callback_data="admin:report:training:week")],
        [InlineKeyboardButton("За месяц", callback_data="admin:report:training:month")],
        _back_button("admin:menu:reports"),
    ])


async def _run_script(script_name: str, args: List[str]) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(REPO_DIR / script_name), *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        cwd=str(REPO_DIR),
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode or 0, stdout.decode(errors="replace"), stderr.decode(errors="replace")


def _summarize_output(stdout: str, max_lines: int = 12) -> str:
    """Скрипты печатают много построчной отладки (например, каждого
    проверяемого игрока) — оставляем только содержательные строки
    (со статус-эмодзи), чтобы в Telegram было видно, что реально
    произошло, а не просто 'готово'."""
    noisy_prefixes = ("🔍", "   ", "--", "=")
    meaningful = [
        line.strip() for line in stdout.splitlines()
        if line.strip() and not line.strip().startswith(noisy_prefixes)
    ]
    if not meaningful:
        return "(скрипт не вывел статусных строк, см. полный лог в journalctl)"
    return "\n".join(meaningful[-max_lines:])


def _check_already_run_today(data_types: List[str]) -> Optional[str]:
    """Прямая проверка по Сервисному листу (не через 5-минутный кэш —
    сразу после реального запуска кэш ещё не мог обновиться)."""
    today_str = datetime.now().strftime("%d.%m.%Y")
    for dt_ in data_types:
        for record in duplicate_protection.get_records_by_type(dt_):
            if record.get("date", "").startswith(today_str):
                return record["date"]
    return None


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
            code, out, stderr = await _run_script(script, args)
        except Exception as e:
            code, out, stderr = 1, "", str(e)
        if code == 0:
            result_lines.append(f"✅ {script}\n{_summarize_output(out)}")
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


async def _handle_report_action(query, kind: str, period: str) -> None:
    if kind != "training":
        return
    await query.edit_message_text(f"⏳ Формирую отчёт (тренировки, {period})...")
    args = ["--week"] if period == "week" else ["--month", datetime.now().strftime("%Y-%m")]
    try:
        code, _stdout, stderr = await _run_script("training_report.py", args)
    except Exception as e:
        code, stderr = 1, str(e)
    if code == 0:
        text = "✅ Отчёт обновлён в таблице (лист «Тренировки»)."
    else:
        text = f"❌ Не удалось сформировать отчёт: {stderr.strip().splitlines()[-1] if stderr.strip() else 'см. логи демона'}"
        log.error(f"training_report.py завершился с ошибкой (код {code}): {stderr[-2000:]}")
        sheets_cache.report_error("training_report.py", stderr[-2000:] or f"exit code {code}", _get_spreadsheet())
    await query.edit_message_text(text, reply_markup=_reports_training_menu_markup())


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
                if len(parts) > 3 and parts[3] == "training":
                    await query.edit_message_text("📊 Отчёты → Тренировки", reply_markup=_reports_training_menu_markup())
                else:
                    await query.edit_message_text("📊 Отчёты", reply_markup=_reports_menu_markup())

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
            lines = [
                "✅ Синхронизация завершена",
                "",
                f"Выгружено в Sheets: события {sr.get('pushed', 0)} "
                f"(добавлено {sr.get('inserted', 0)}, обновлено {sr.get('updated', 0)}, "
                f"удалено {sr.get('deleted', 0)}), голоса {at.get('pushed', 0)} "
                f"(добавлено {at.get('inserted', 0)}, обновлено {at.get('updated', 0)})",
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
        ])
    except Exception as e:
        log.warning(f"Не удалось зарегистрировать список команд: {e}")


async def on_shutdown(app: Application) -> None:
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
    app.add_handler(MessageHandler(filters.Text([ADMIN_KEYBOARD_LABEL]), handle_admin_button))
    app.add_handler(CallbackQueryHandler(handle_admin_callback, pattern=r"^admin:"))

    log.info("Запуск polling...")
    app.run_polling(
        allowed_updates=["poll_answer", "message", "callback_query"],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
