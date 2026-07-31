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
import signal
import sys
import time
from datetime import datetime
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

# Mini App фэнтези без туннеля: фронт — статикой на GitHub Pages
# (FANTASY_WEBAPP_URL), данные (пул/состав/таблица) бот кладёт прямо в URL при
# открытии, выбранный состав приходит обратно через Telegram sendData (сервер
# наружу не светим). Открывается reply-кнопкой (sendData работает только так).
FANTASY_WEBAPP_URL = os.getenv("FANTASY_WEBAPP_URL", "").strip()


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
FANTASY_KEYBOARD_LABEL = "🏀 Фэнтези"
FEEDBACK_KEYBOARD_LABEL = "💬 Написать админам"


def _bottom_keyboard(payload: str = "", is_admin: bool = False,
                     with_fantasy: bool = True, with_reports: bool = False,
                     with_personal: bool = False) -> ReplyKeyboardMarkup:
    """Нижняя клавиатура: фэнтези, обратная связь и — админу — панель.

    Фэнтези открывается как web_app: только reply-кнопка даёт sendData, поэтому
    состав сохраняется через Telegram и не зависит от живого API. Данные едут в
    самом URL (#d=payload), приватно, в личном чате игрока."""
    rows: List[List[KeyboardButton]] = []
    if with_fantasy and _webapp_url():
        url = _webapp_url() + ("#d=" + payload if payload else "")
        rows.append([KeyboardButton(FANTASY_KEYBOARD_LABEL, web_app=WebAppInfo(url=url))])
    rows.append([KeyboardButton(FEEDBACK_KEYBOARD_LABEL)])
    # Закрытые разделы: у кого есть доступ, тот и видит кнопку. Нет ни одного —
    # ни одной лишней кнопки под чатом.
    closed = []
    if with_personal:
        closed.append(KeyboardButton(MYSTATS_KEYBOARD_LABEL))
    if with_reports:
        closed.append(KeyboardButton(PROGRESS_KEYBOARD_LABEL))
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
    """Показывает нижнюю клавиатуру, ужимая данные фэнтези, пока Telegram не
    примет. Последняя ступень — без данных: живой вход у большинства работает,
    и это лучше, чем клавиатура, которая не отправилась вовсе (так и было
    с 26.07, пока payload не дорос до 26 КБ)."""
    is_admin = _is_admin(user)
    uid = str(user.id)
    # Закрытые разделы — по выданному доступу, у админа оба.
    with_reports = _can_see_reports(user)
    with_personal = _can_see_personal(user)
    # Кнопка фэнтези — только игрокам команды: остальным она бесполезна.
    with_fantasy = False
    if FANTASY_WEBAPP_URL and _webapp_url():
        try:
            with_fantasy = fantasy_api._is_team_member(uid, user.username or "")
        except Exception as e:
            log.warning(f"проверка состава для клавиатуры: {e}")

    # Запасные данные в кнопке собираются из пула, а пул — это поход в API двух
    # лиг. Человек ждать этого не должен: пул греет фоновый цикл, и если он
    # тёплый — берём готовое, если нет — отдаём клавиатуру без данных и греем
    # в фоне. Иначе одна недоступная лига превращала /start в пятиминутное
    # ожидание, потому что клиент честно вырабатывал все таймауты.
    with_fantasy_payload = with_fantasy
    if with_fantasy and not fantasy_api.pool_is_warm():
        # Ссылку держим: задачу без владельца сборщик мусора вправе выкинуть
        # на полпути, и прогрев молча не случится.
        task = asyncio.create_task(_warm_fantasy_pool(force=True))
        _side_tasks.add(task)
        task.add_done_callback(_side_tasks.discard)
        # Немного подождать всё же стоит: с живой лигой пул собирается за
        # секунду, и уходить без запасных данных только потому, что демон
        # недавно перезапустился, — значит оставить человека с пустой кнопкой
        # до следующего /start. Не успели за POOL_WAIT — уходим без них,
        # прогрев доработает в фоне.
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
                with_reports=with_reports, with_personal=with_personal))
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


PLAYER_MENU_TEXT = ("🏀 Привет! Кнопки внизу экрана — всегда под рукой:\n\n"
                    "• Фэнтези — собрать состав, посмотреть таблицу и топ игроков\n"
                    "• Написать админам — идея, баг, пожелание\n\n"
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
    rows.append([InlineKeyboardButton("📄 Получить файл за месяц", callback_data="rep:file")])
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
            monthly_report.build_combined, profiles, year, month)
    except Exception as e:
        log.warning(f"месячный файл для {uid}: {e}")
        html_doc = None
    if not html_doc:
        # Прошлый месяц пуст — пробуем текущий, иначе человек получит пустоту.
        try:
            html_doc = await asyncio.to_thread(
                monthly_report.build_combined, profiles, today.year, today.month)
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
    if not fantasy_api._is_team_member(uid, user.username or ""):
        await msg.reply_text("Фэнтези доступна только игрокам команды.")
        return
    season = fantasy.get_active_season()
    if not season:
        await msg.reply_text("Сейчас нет активного сезона.")
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


async def _render_prog_list(is_admin: bool = False) -> Tuple[str, InlineKeyboardMarkup]:
    lines, rows = await _prog_body()
    if is_admin:
        lines += ["", "Кому открыт этот раздел — в «🔑 Доступы» главного меню."]
    rows.append(_back_button())
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


async def _prog_send(message, source: str, team_id: str) -> None:
    """Короткая сводка в чат + подробный разбор файлом.

    В сообщение помещается «что случилось в игре» на пять строк; сезон,
    тренды и сравнения — это таблицы и графики, в тексте они нечитаемы."""
    import team_progress
    import team_report_html

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
    await message.reply_text(team_progress.short_summary(rep, detail),
                             reply_markup=InlineKeyboardMarkup(
                                 [[InlineKeyboardButton("⬅️ К командам",
                                                        callback_data="prog:list")]]))
    if not detail.get("ok"):
        return
    try:
        page = await asyncio.to_thread(team_report_html.build, detail)
        buf = io.BytesIO(page.encode("utf-8"))
        buf.name = f"razbor-{team_id}-{detail['series'][-1]['game_date']}.html"
        await message.reply_document(
            buf, caption="Открой файл и листай вниз: сезон, последняя игра, "
                         "динамика, состав с тренировками, лига, лидеры соперника. "
                         "В самом конце — готовый промт для ИИ.")
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
    """Кнопка «📈 Прогресс команды» на нижней клавиатуре — для тренеров."""
    msg, user, chat = update.effective_message, update.effective_user, update.effective_chat
    if not msg or not user or not chat or chat.type != "private":
        return
    if not _can_see_reports(user):
        return
    text, markup = await _render_prog_list(is_admin=_is_admin(user))
    await msg.reply_text(text, reply_markup=markup)
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
                 f"ошибок {res['failed']}")
    except Exception as e:
        log.warning(f"Справочники лиг не обновились: {e}")


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
        # Ссылка игрока меняется, когда бот склеил его из двух лиг. Прогрев —
        # единственное место, где точно известен свежий пул, поэтому здесь же
        # приводим сохранённые составы к новому виду: иначе у человека внезапно
        # «состав не из пула», хотя он ничего не трогал.
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


async def on_startup(app: Application) -> None:
    log.info("=" * 50)
    log.info("Бот запущен (long-polling режим)")
    log.info(f"Время старта: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")
    log.info("=" * 50)
    sheets_cache.init_db()
    _refresh_poll_cache()
    _refresh_db_cache()
    _periodic_push_local_changes()
    # Список команд у игрока и у админа разный: личная статистика и админка —
    # скрытые функции, и в меню обычного игрока их быть не должно.
    try:
        await app.bot.set_my_commands([
            BotCommand("start", "Меню бота"),
            BotCommand("feedback", "Написать админам: идея или проблема"),
        ], scope=BotCommandScopeDefault())
        for admin_id in ADMIN_USER_IDS:
            try:
                await app.bot.set_my_commands([
                    BotCommand("start", "Меню бота"),
                    BotCommand("admin", "Админ-панель"),
                    BotCommand("profile", "Мой прогресс (скрытое)"),
                    BotCommand("season", "Создать сезон фэнтези"),
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
    app.add_handler(MessageHandler(filters.Text([MYSTATS_KEYBOARD_LABEL]),
                                   handle_mystats_button))
    app.add_handler(MessageHandler(filters.Text([FEEDBACK_KEYBOARD_LABEL]), handle_feedback_button))
    app.add_handler(CommandHandler("profile", handle_my_profile))
    app.add_handler(CommandHandler("season", handle_season))
    app.add_handler(CommandHandler("feedback", handle_feedback))
    app.add_handler(CallbackQueryHandler(handle_report_prefs_callback, pattern=r"^rep:(cmp|ntf|met|mets|allmet|deep|back|file)"))

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
        handle_feedback_text), group=2)
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND,
        handle_profile_link), group=3)
    app.add_handler(CallbackQueryHandler(handle_admin_callback, pattern=r"^admin:"))
    app.add_handler(CallbackQueryHandler(handle_prog_callback, pattern=r"^prog:"))

    log.info("Запуск polling...")
    app.run_polling(
        allowed_updates=["poll_answer", "message", "callback_query"],
        drop_pending_updates=False,
    )


if __name__ == "__main__":
    main()
