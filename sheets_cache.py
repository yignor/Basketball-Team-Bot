#!/usr/bin/env python3
"""
Локальный SQLite-кэш/зеркало данных из Google Sheets.

Это НЕ замена Google Sheets — Sheets остаётся источником истины и местом,
которое видят и редактируют люди. Этот модуль только читает Sheets и
складывает копию в SQLite для быстрого чтения (см. admin_panel.py).

Инвариант: только sync_all() (вызываемый из bot_daemon.py) пишет в bot.db.
Все остальные читатели должны только делать SELECT.

Кэш полностью одноразовый и восстанавливаемый: удаление data/bot.db ничего
не портит, следующая же синхронизация заново наполнит его из Sheets. Поэтому
при переезде на другой сервер файл базы можно не переносить вообще.
"""

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

DB_PATH = Path(__file__).parent / "data" / "bot.db"

PLAYERS_SHEET_NAME = "Игроки"
ATTEND_SHEET_NAME = "Посещаемость"
SERVICE_SHEET_NAME = "Сервисный"
BOT_USERS_SHEET_NAME = "Пользователи бота"
ERRORS_SHEET_NAME = "Ошибки"

ACTIVITY_TYPES = [
    "ОПРОС_ГОЛОСОВАНИЕ",
    "ОПРОС_ИГРА",
    "АНОНС_ИГРА",
    "РЕЗУЛЬТАТ_ИГРА",
    "ДЕНЬ_РОЖДЕНИЯ",
    "КАЛЕНДАРЬ_ИГРА",
]

PENDING_STATUSES = {"АКТИВЕН", "ОТПРАВЛЯЕТСЯ"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS players (
    row_index     INTEGER PRIMARY KEY,
    surname       TEXT NOT NULL DEFAULT '',
    name          TEXT NOT NULL DEFAULT '',
    nickname      TEXT NOT NULL DEFAULT '',
    telegram_id   TEXT NOT NULL DEFAULT '',
    birthday      TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT '',
    team          TEXT NOT NULL DEFAULT '',
    added_date    TEXT NOT NULL DEFAULT '',
    notes         TEXT NOT NULL DEFAULT '',
    synced_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_players_telegram_id ON players(telegram_id);

CREATE TABLE IF NOT EXISTS attendance (
    tg_poll_id       TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    username         TEXT NOT NULL DEFAULT '',
    first_name       TEXT NOT NULL DEFAULT '',
    last_name        TEXT NOT NULL DEFAULT '',
    vote_text        TEXT NOT NULL DEFAULT '',
    vote_type        TEXT NOT NULL DEFAULT '',
    training_date    TEXT NOT NULL DEFAULT '',
    config_poll_id   TEXT NOT NULL DEFAULT '',
    updated_at       TEXT NOT NULL DEFAULT '',
    revote_count     INTEGER NOT NULL DEFAULT 0,
    row_index        INTEGER NOT NULL,
    synced_at        TEXT NOT NULL,
    dirty            INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tg_poll_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_attendance_training_date ON attendance(training_date);

CREATE TABLE IF NOT EXISTS service_log (
    row_index         INTEGER PRIMARY KEY,
    data_type         TEXT NOT NULL,
    logged_at         TEXT NOT NULL DEFAULT '',
    unique_key        TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL DEFAULT '',
    additional_data   TEXT NOT NULL DEFAULT '',
    link              TEXT NOT NULL DEFAULT '',
    comp_id           TEXT NOT NULL DEFAULT '',
    team_id           TEXT NOT NULL DEFAULT '',
    alt_name          TEXT NOT NULL DEFAULT '',
    settings          TEXT NOT NULL DEFAULT '',
    game_id           TEXT NOT NULL DEFAULT '',
    game_date         TEXT NOT NULL DEFAULT '',
    game_time         TEXT NOT NULL DEFAULT '',
    arena             TEXT NOT NULL DEFAULT '',
    team_a_id         TEXT NOT NULL DEFAULT '',
    team_b_id         TEXT NOT NULL DEFAULT '',
    synced_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_service_log_type ON service_log(data_type);

CREATE TABLE IF NOT EXISTS sync_meta (
    table_name        TEXT PRIMARY KEY,
    last_success_at   TEXT,
    last_attempt_at   TEXT NOT NULL,
    last_error        TEXT,
    row_count         INTEGER NOT NULL DEFAULT 0
);

-- Эти две таблицы, в отличие от остальных, НЕ зеркала Sheets — бот сам
-- пишет сюда (и дублирует в соответствующие листы, чтобы было видно в
-- таблице). SQLite здесь для мгновенного показа в /admin.

CREATE TABLE IF NOT EXISTS bot_users (
    telegram_id   TEXT PRIMARY KEY,
    username      TEXT NOT NULL DEFAULT '',
    first_name    TEXT NOT NULL DEFAULT '',
    first_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS errors (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    message       TEXT NOT NULL,
    logged_at     TEXT NOT NULL
);

-- ── Локально-первичные данные (SERVICE_RECORDS_LOCAL_PRIMARY) ──────────────
-- В отличие от players/attendance/service_log (чистые зеркала для чтения),
-- эта таблица — основной рабочий слой для EnhancedDuplicateProtection,
-- когда флаг включён: пишут ~10 cron-скриптов и демон, читают тоже они.
-- Google Sheets становится периодическим экспортом (push_service_records),
-- не источником истины на каждый вызов. См. дизайн в плане:
-- уникальные индексы + INSERT ... ON CONFLICT DO NOTHING/DO UPDATE делают
-- проверку-и-запись одной атомарной операцией — гонка между процессами
-- невозможна структурно, а не "потому что мы аккуратно написали код".

CREATE TABLE IF NOT EXISTS service_records (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    data_type         TEXT NOT NULL,
    unique_key        TEXT NOT NULL,
    logged_at         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'АКТИВЕН',
    additional_data   TEXT NOT NULL DEFAULT '',
    link              TEXT NOT NULL DEFAULT '',
    comp_id           TEXT NOT NULL DEFAULT '',
    team_id           TEXT NOT NULL DEFAULT '',
    alt_name          TEXT NOT NULL DEFAULT '',
    settings          TEXT NOT NULL DEFAULT '',
    game_id           TEXT NOT NULL DEFAULT '',
    game_date         TEXT NOT NULL DEFAULT '',
    game_time         TEXT NOT NULL DEFAULT '',
    arena             TEXT NOT NULL DEFAULT '',
    team_a_id         TEXT NOT NULL DEFAULT '',
    team_b_id         TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    sheet_row_hint    INTEGER,
    dirty             INTEGER NOT NULL DEFAULT 1,
    deleted           INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_service_records_type_key
    ON service_records(data_type, unique_key) WHERE deleted = 0;
CREATE UNIQUE INDEX IF NOT EXISTS uq_service_records_type_gameid
    ON service_records(data_type, game_id) WHERE deleted = 0 AND game_id != '';
CREATE INDEX IF NOT EXISTS idx_service_records_dirty ON service_records(dirty) WHERE dirty = 1;
CREATE INDEX IF NOT EXISTS idx_service_records_type ON service_records(data_type) WHERE deleted = 0;

-- "Конфиг" — люди правят руками, поэтому только pull (та же схема, что
-- players/attendance): сырые колонки, парсинг остаётся в
-- enhanced_duplicate_protection.py как есть.
CREATE TABLE IF NOT EXISTS config_rows (
    row_index   INTEGER PRIMARY KEY,
    col_a       TEXT NOT NULL DEFAULT '',
    col_b       TEXT NOT NULL DEFAULT '',
    col_c       TEXT NOT NULL DEFAULT '',
    col_d       TEXT NOT NULL DEFAULT '',
    col_e       TEXT NOT NULL DEFAULT '',
    col_f       TEXT NOT NULL DEFAULT '',
    col_g       TEXT NOT NULL DEFAULT '',
    col_h       TEXT NOT NULL DEFAULT '',
    synced_at   TEXT NOT NULL
);
"""

# Порядок колонок в листе "Сервисный" — должен совпадать с SERVICE_HEADER
# в enhanced_duplicate_protection.py (индексы TYPE_COL..TEAM_B_ID_COL).
SERVICE_SHEET_COLUMNS = [
    "data_type", "logged_at", "unique_key", "status", "additional_data", "link",
    "comp_id", "team_id", "alt_name", "settings", "game_id", "game_date",
    "game_time", "arena", "team_a_id", "team_b_id",
]
CONFIG_SHEET_NAME = "Конфиг"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


now_iso = _now_iso  # публичный алиас для enhanced_duplicate_protection.py


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=8.0)
    conn.execute("PRAGMA journal_mode = WAL")
    # 8с (было 5с) — теперь пишут не только периодический sync демона, но и
    # ~10 cron-скриптов при включённом SERVICE_RECORDS_LOCAL_PRIMARY.
    conn.execute("PRAGMA busy_timeout = 8000")
    # NORMAL безопасен в WAL-режиме (не теряет закоммиченные транзакции при
    # обычном падении процесса, только при потере питания) и не платит за
    # fsync на каждый commit — сервер не эфемерный контейнер.
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def _connection() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


# Публичный алиас — используется enhanced_duplicate_protection.py для
# атомарных операций над service_records (ON CONFLICT ...), которым нужен
# прямой доступ к соединению, а не готовая обёртка-функция.
get_connection = _connection


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, coltype: str, default: str) -> None:
    """CREATE TABLE IF NOT EXISTS не добавляет колонки в уже существующую
    таблицу — нужна ручная миграция для баз, созданных до появления
    колонки (например data/bot.db на сервере с Phase 1)."""
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype} DEFAULT {default}")


def init_db() -> None:
    """Идемпотентно создаёт схему. Безопасно вызывать при каждом старте
    демона и в любом месте, откуда читают кэш."""
    with _connection() as conn:
        conn.executescript(SCHEMA)
        _ensure_column(conn, "attendance", "dirty", "INTEGER NOT NULL", "0")
        conn.commit()


def _mark_sync_result(conn: sqlite3.Connection, table_name: str, row_count: int, error: Optional[str]) -> None:
    now = _now_iso()
    if error is None:
        conn.execute(
            """
            INSERT INTO sync_meta (table_name, last_success_at, last_attempt_at, last_error, row_count)
            VALUES (?, ?, ?, NULL, ?)
            ON CONFLICT(table_name) DO UPDATE SET
                last_success_at = excluded.last_success_at,
                last_attempt_at = excluded.last_attempt_at,
                last_error = NULL,
                row_count = excluded.row_count
            """,
            (table_name, now, now, row_count),
        )
    else:
        conn.execute(
            """
            INSERT INTO sync_meta (table_name, last_success_at, last_attempt_at, last_error, row_count)
            VALUES (?, NULL, ?, ?, 0)
            ON CONFLICT(table_name) DO UPDATE SET
                last_attempt_at = excluded.last_attempt_at,
                last_error = excluded.last_error
            """,
            (table_name, now, error),
        )
    conn.commit()


# ── Sync (WRITE side — вызывается только из bot_daemon.py) ────────────────

def sync_players(spreadsheet) -> None:
    init_db()
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
            records = ws.get_all_records()
            now = _now_iso()
            rows = []
            for idx, r in enumerate(records, start=2):
                if not r.get("Имя"):
                    continue
                rows.append((
                    idx,
                    str(r.get("Фамилия", "")),
                    str(r.get("Имя", "")),
                    str(r.get("Ник", "")),
                    str(r.get("Telegram ID", "")),
                    str(r.get("Дата рождения", "")),
                    str(r.get("Статус", "")),
                    str(r.get("Команда", "")),
                    str(r.get("Дата добавления", "")),
                    str(r.get("Примечания", "")),
                    now,
                ))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM players")
            conn.executemany(
                """
                INSERT INTO players
                (row_index, surname, name, nickname, telegram_id, birthday, status, team, added_date, notes, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "players", len(rows), None)
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "players", 0, str(e))
            raise


def sync_attendance(spreadsheet) -> None:
    init_db()
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(ATTEND_SHEET_NAME)
            all_rows = ws.get_all_values()[1:]  # skip header
            now = _now_iso()
            rows = []
            for idx, row in enumerate(all_rows, start=2):
                if len(row) < 2 or not row[1]:
                    continue
                row = row + [""] * (11 - len(row))
                revote_count = int(row[10]) if row[10].isdigit() else 0
                rows.append((
                    row[0], row[1], row[2], row[3], row[4],
                    row[5], row[6], row[7], row[8], row[9],
                    revote_count, idx, now,
                ))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM attendance")
            conn.executemany(
                """
                INSERT INTO attendance
                (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
                 training_date, config_poll_id, updated_at, revote_count, row_index, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "attendance", len(rows), None)
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "attendance", 0, str(e))
            raise


def sync_service_log(spreadsheet) -> None:
    init_db()
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(SERVICE_SHEET_NAME)
            all_rows = ws.get_all_values()[1:]  # skip header
            now = _now_iso()
            rows = []
            for idx, row in enumerate(all_rows, start=2):
                if not row or not row[0] or row[0] not in ACTIVITY_TYPES:
                    continue
                row = row + [""] * (16 - len(row))
                rows.append((idx, *row[:16], now))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM service_log")
            conn.executemany(
                """
                INSERT INTO service_log
                (row_index, data_type, logged_at, unique_key, status, additional_data, link,
                 comp_id, team_id, alt_name, settings, game_id, game_date, game_time, arena,
                 team_a_id, team_b_id, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "service_log", len(rows), None)
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "service_log", 0, str(e))
            raise


def sync_config(spreadsheet) -> None:
    """Зеркалит лист 'Конфиг' целиком, сырыми колонками — сама разбор
    логика (несколько секций с маркерами) остаётся в
    enhanced_duplicate_protection.py и не меняется."""
    init_db()
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(CONFIG_SHEET_NAME)
            all_rows = ws.get_all_values()[1:]  # skip header
            now = _now_iso()
            rows = []
            for idx, row in enumerate(all_rows, start=2):
                row = row + [""] * (8 - len(row))
                rows.append((idx, *row[:8], now))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM config_rows")
            conn.executemany(
                """
                INSERT INTO config_rows
                (row_index, col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "config_rows", len(rows), None)
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "config_rows", 0, str(e))
            raise


def get_config_rows() -> List[List[str]]:
    """Возвращает сырые строки листа 'Конфиг' из локального зеркала —
    пустой список, если синхронизация ещё ни разу не проходила (вызывающий
    код должен в этом случае сам сделать fallback на живой Sheets-запрос)."""
    init_db()
    with _connection() as conn:
        rows = conn.execute(
            "SELECT col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h FROM config_rows ORDER BY row_index"
        ).fetchall()
    return [list(r) for r in rows]


def bootstrap_service_records(spreadsheet) -> Dict[str, Any]:
    """Разовая (но идемпотентная — безопасно перезапускать) заливка ВСЕХ
    строк/колонок листа 'Сервисный' (не только ACTIVITY_TYPES, в отличие
    от sync_service_log) в service_records, перед включением
    SERVICE_RECORDS_LOCAL_PRIMARY. INSERT ... ON CONFLICT DO NOTHING —
    повторный запуск не затирает то, что уже успело появиться локально."""
    init_db()
    ws = spreadsheet.worksheet(SERVICE_SHEET_NAME)
    all_rows = ws.get_all_values()[1:]  # skip header
    now = _now_iso()
    inserted = 0
    with _connection() as conn:
        conn.execute("BEGIN")
        for idx, row in enumerate(all_rows, start=2):
            if not row or not row[0]:
                continue
            data_type = row[0].strip()
            row = row + [""] * (16 - len(row))
            unique_key = row[2].strip()
            if not unique_key:
                continue
            cur = conn.execute(
                """
                INSERT INTO service_records
                (data_type, unique_key, logged_at, status, additional_data, link,
                 comp_id, team_id, alt_name, settings, game_id, game_date, game_time, arena,
                 team_a_id, team_b_id, created_at, updated_at, sheet_row_hint, dirty)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(data_type, unique_key) WHERE deleted = 0 DO NOTHING
                """,
                (data_type, unique_key, row[1], row[3], row[4], row[5], row[6], row[7],
                 row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],
                 now, now, idx),
            )
            inserted += cur.rowcount
        conn.commit()
    return {"sheet_rows": len(all_rows), "inserted": inserted}


def sync_all(spreadsheet) -> Dict[str, Any]:
    """Синхронизирует таблицы независимо — ошибка в одной не должна мешать
    остальным.

    "attendance" сюда намеренно не входит: голоса теперь локально-первичные
    (пишет upsert_vote_local в реальном времени), периодический pull из
    Sheets затирал бы ещё не выгруженные (dirty=1) локальные изменения.
    sync_attendance() остаётся доступной отдельно — для разового
    bootstrap существующих голосов при первом включении."""
    summary: Dict[str, Any] = {}
    for name, fn in (
        ("players", sync_players),
        ("service_log", sync_service_log),
        ("config_rows", sync_config),
    ):
        try:
            fn(spreadsheet)
            summary[name] = "ok"
        except Exception as e:
            summary[name] = f"error: {e}"
    return summary


# ── Read side (admin_panel.py и другие потребители) ────────────────────────

def get_players_stats() -> Dict[str, int]:
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        linked = conn.execute(
            "SELECT COUNT(*) FROM players WHERE telegram_id != ''"
        ).fetchone()[0]
    return {"total": total, "linked": linked}


def get_players_page(offset: int = 0, limit: int = 8) -> Dict[str, Any]:
    """Постраничный список игроков (для показа в /admin по 5-10 за раз)."""
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        rows = conn.execute(
            """
            SELECT surname, name, nickname, telegram_id, status
            FROM players ORDER BY row_index LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "limit": limit}


def get_attendance_stats() -> Dict[str, int]:
    init_db()
    with _connection() as conn:
        unique_users = conn.execute(
            "SELECT COUNT(DISTINCT user_id) FROM attendance"
        ).fetchone()[0]
        total_votes = conn.execute("SELECT COUNT(*) FROM attendance").fetchone()[0]
        unique_30d = conn.execute(
            """
            SELECT COUNT(DISTINCT user_id) FROM attendance
            WHERE updated_at != '' AND
                  substr(updated_at, 7, 4) || '-' || substr(updated_at, 4, 2) || '-' || substr(updated_at, 1, 2)
                  >= date('now', '-30 days')
            """
        ).fetchone()[0]
    return {
        "unique_users": unique_users,
        "total_votes": total_votes,
        "unique_30d": unique_30d,
    }


def get_service_activity_stats() -> Dict[str, Dict[str, int]]:
    init_db()
    stats: Dict[str, Dict[str, int]] = {}
    with _connection() as conn:
        rows = conn.execute(
            "SELECT data_type, status, COUNT(*) as cnt FROM service_log GROUP BY data_type, status"
        ).fetchall()
    for row in rows:
        bucket = stats.setdefault(row["data_type"], {"total": 0, "active": 0, "done": 0})
        bucket["total"] += row["cnt"]
        if row["status"] in PENDING_STATUSES:
            bucket["active"] += row["cnt"]
        else:
            bucket["done"] += row["cnt"]
    return stats


def get_recent_service_events(limit: int = 8, since_days: Optional[int] = None) -> List[sqlite3.Row]:
    """limit — обычный лимит по количеству; since_days — вместо/вместе с
    limit можно попросить все события за последние N дней (для "Лог бота",
    например since_days=1 — сегодня и вчера)."""
    init_db()
    where = ""
    params: tuple = ()
    if since_days is not None:
        where = """
            WHERE substr(logged_at, 7, 4) || '-' || substr(logged_at, 4, 2) || '-' || substr(logged_at, 1, 2)
                  >= date('now', ?)
        """
        params = (f"-{since_days} days",)
    with _connection() as conn:
        rows = conn.execute(
            f"""
            SELECT data_type, status, logged_at FROM service_log
            {where}
            ORDER BY
                substr(logged_at, 7, 4) || substr(logged_at, 4, 2) || substr(logged_at, 1, 2) ||
                substr(logged_at, 12, 2) || substr(logged_at, 15, 2) DESC
            LIMIT ?
            """,
            (*params, limit if since_days is None else 200),
        ).fetchall()
    return rows


def get_sync_status() -> Dict[str, Dict[str, Any]]:
    init_db()
    with _connection() as conn:
        rows = conn.execute("SELECT * FROM sync_meta").fetchall()
    return {row["table_name"]: dict(row) for row in rows}


# ── Пользователи бота ("В боте") и лог ошибок ───────────────────────────────
# В отличие от остального модуля, эти функции сами являются источником
# истины для SQLite (не просто кэш) и при наличии spreadsheet дублируют
# запись в соответствующий лист Google Sheets — по желанию пользователя
# видеть всё в таблицах. Ошибка записи в Sheets никогда не должна ронять
# вызывающий код (это часто сам обработчик ошибок).

def _get_or_create_ws(spreadsheet, title: str, header: List[str]):
    import gspread
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(header))
        ws.update("A1", [header])
    return ws


def record_bot_user(spreadsheet, telegram_id: str, username: str, first_name: str) -> bool:
    """Возвращает True, если пользователь новый (первый /start)."""
    init_db()
    with _connection() as conn:
        existing = conn.execute(
            "SELECT 1 FROM bot_users WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
        if existing:
            return False
        now = _now_iso()
        conn.execute(
            "INSERT INTO bot_users (telegram_id, username, first_name, first_seen_at) VALUES (?, ?, ?, ?)",
            (telegram_id, username, first_name, now),
        )
        conn.commit()
    if spreadsheet is not None:
        try:
            ws = _get_or_create_ws(spreadsheet, BOT_USERS_SHEET_NAME, ["Telegram ID", "Username", "Имя", "Первый /start"])
            ws.append_row([telegram_id, username, first_name, datetime.now().strftime("%d.%m.%Y %H:%M")])
        except Exception:
            pass
    return True


def get_bot_users_page(offset: int = 0, limit: int = 8) -> Dict[str, Any]:
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM bot_users").fetchone()[0]
        rows = conn.execute(
            "SELECT telegram_id, username, first_name, first_seen_at FROM bot_users ORDER BY first_seen_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "limit": limit}


def report_error(source: str, message: str, spreadsheet=None) -> None:
    """Логирует ошибку в SQLite (для быстрого показа в /admin) и, если
    передан spreadsheet, дублирует в лист "Ошибки". Сама никогда не
    бросает исключение — безопасно вызывать из любого except-блока."""
    now = _now_iso()
    message = message[:2000]
    try:
        init_db()
        with _connection() as conn:
            conn.execute(
                "INSERT INTO errors (source, message, logged_at) VALUES (?, ?, ?)",
                (source, message, now),
            )
            conn.commit()
    except Exception:
        pass
    if spreadsheet is not None:
        try:
            ws = _get_or_create_ws(spreadsheet, ERRORS_SHEET_NAME, ["Источник", "Сообщение", "Когда"])
            ws.append_row([source, message, datetime.now().strftime("%d.%m.%Y %H:%M")])
        except Exception:
            pass


def get_errors_page(offset: int = 0, limit: int = 8) -> Dict[str, Any]:
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM errors").fetchone()[0]
        rows = conn.execute(
            "SELECT source, message, logged_at FROM errors ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "limit": limit}


def get_user_action_log(offset: int = 0, limit: int = 10) -> Dict[str, Any]:
    """Объединённый лог действий пользователей: /start + голоса за
    тренировки, отсортированные по времени (новые сверху)."""
    init_db()
    with _connection() as conn:
        starts = conn.execute(
            "SELECT 'СТАРТ' as kind, telegram_id as user_id, username, first_name, '' as detail, first_seen_at as ts FROM bot_users"
        ).fetchall()
        votes = conn.execute(
            "SELECT 'ГОЛОС' as kind, user_id, username, first_name, vote_text as detail, updated_at as ts FROM attendance"
        ).fetchall()

    def _parse_ts(ts: str):
        # Оба формата приводим к наивному datetime — сервер работает в UTC,
        # так что оба варианта фактически в одной шкале, а сравнивать
        # tz-aware и tz-naive datetime напрямую нельзя.
        try:
            return datetime.fromisoformat(ts).replace(tzinfo=None)
        except ValueError:
            pass
        try:
            return datetime.strptime(ts, "%d.%m.%Y %H:%M")
        except ValueError:
            return datetime.min

    combined = [dict(r) for r in starts] + [dict(r) for r in votes]
    combined.sort(key=lambda item: _parse_ts(item["ts"]), reverse=True)
    total = len(combined)
    return {"rows": combined[offset:offset + limit], "total": total, "offset": offset, "limit": limit}


# ── Push: локальные изменения → Google Sheets ───────────────────────────────
# Обратное направление относительно всего остального модуля. Политика при
# конфликте: локальные данные всегда побеждают — это бот-генерируемые
# записи (не редактируются людьми), в отличие от players/config_rows,
# которые остаются pull-only в обратную сторону.

_SERVICE_END_COL = chr(ord('A') + len(SERVICE_SHEET_COLUMNS) - 1)


def push_service_records(spreadsheet, batch_size: int = 200) -> Dict[str, Any]:
    """Выгружает накопленные dirty=1 записи в лист 'Сервисный'.

    Ищет существующую строку по unique_key через ws.find() (а не по
    запомненному номеру строки) — надёжнее при параллельных
    удалениях/сдвигах строк, ценой одного API-вызова на запись. Push не на
    горячем пути (раз в 6 часов или по кнопке), так что это приемлемо."""
    init_db()
    ws = spreadsheet.worksheet(SERVICE_SHEET_NAME)

    with _connection() as conn:
        dirty_rows = conn.execute(
            "SELECT * FROM service_records WHERE dirty = 1 ORDER BY id LIMIT ?",
            (batch_size,),
        ).fetchall()

    if not dirty_rows:
        return {"pushed": 0, "inserted": 0, "updated": 0, "deleted": 0}

    inserted = updated = deleted = 0
    to_append: List[List[str]] = []
    to_append_ids: List[int] = []
    pushed_ids: List[int] = []

    for r in dirty_rows:
        try:
            cell = ws.find(r["unique_key"], in_column=3)
        except Exception:
            cell = None

        if r["deleted"]:
            if cell:
                try:
                    ws.delete_rows(cell.row)
                    deleted += 1
                except Exception:
                    continue  # не помечаем dirty=0 — попробуем в следующий push
            pushed_ids.append(r["id"])
            continue

        values = [str(r[col]) for col in SERVICE_SHEET_COLUMNS]
        if cell:
            try:
                ws.update(f"A{cell.row}:{_SERVICE_END_COL}{cell.row}", [values])
                updated += 1
                pushed_ids.append(r["id"])
            except Exception:
                continue
        else:
            to_append.append(values)
            to_append_ids.append(r["id"])

    if to_append:
        try:
            ws.append_rows(to_append, value_input_option="USER_ENTERED")
            inserted += len(to_append)
            pushed_ids.extend(to_append_ids)
        except Exception:
            pass  # не помечаем dirty=0 — попробуем весь append в следующий push

    if pushed_ids:
        with _connection() as conn:
            conn.executemany("UPDATE service_records SET dirty = 0 WHERE id = ?", [(i,) for i in pushed_ids])
            conn.commit()

    return {"pushed": len(pushed_ids), "inserted": inserted, "updated": updated, "deleted": deleted}


# ── Голоса за тренировки — локально-первичные (пишет только демон) ─────────

def upsert_vote_local(
    tg_poll_id: str, user_id: str, username: str, first_name: str, last_name: str,
    vote_text: str, vote_type: str, training_date: str, config_poll_id: str,
) -> str:
    """Атомарный upsert по (tg_poll_id, user_id) — заменяет прямую запись в
    Sheets на горячем пути bot_daemon.py:handle_poll_answer. Возвращает
    'new'/'updated'/'skipped' (тот же контракт, что и старый upsert_vote в
    collect_votes.py, для единообразия логов)."""
    init_db()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with _connection() as conn:
        existing = conn.execute(
            "SELECT revote_count FROM attendance WHERE tg_poll_id = ? AND user_id = ?",
            (tg_poll_id, user_id),
        ).fetchone()

        if vote_type == "REMOVED" and not existing:
            return "skipped"  # ретракт голоса, которого мы ещё не видели

        revotes = (existing["revote_count"] + 1) if existing else 0
        conn.execute(
            """
            INSERT INTO attendance
            (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
             training_date, config_poll_id, updated_at, revote_count, row_index, synced_at, dirty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 1)
            ON CONFLICT(tg_poll_id, user_id) DO UPDATE SET
                username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name,
                vote_text=excluded.vote_text, vote_type=excluded.vote_type,
                training_date=excluded.training_date, config_poll_id=excluded.config_poll_id,
                updated_at=excluded.updated_at, revote_count=excluded.revote_count, dirty=1
            """,
            (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
             training_date, config_poll_id, now, revotes, _now_iso()),
        )
        conn.commit()
    return "updated" if existing else "new"


def push_attendance(spreadsheet, batch_size: int = 500) -> Dict[str, Any]:
    """Выгружает накопленные dirty=1 голоса в лист 'Посещаемость'. Один
    объёмный get_all_values() строит индекс существующих строк по
    (tg_poll_id, user_id) вместо ws.find() на каждую запись — голосов за
    сезон может быть заметно больше, чем строк в 'Сервисный'."""
    init_db()
    with _connection() as conn:
        dirty_rows = conn.execute(
            "SELECT * FROM attendance WHERE dirty = 1 LIMIT ?", (batch_size,)
        ).fetchall()
    if not dirty_rows:
        return {"pushed": 0, "inserted": 0, "updated": 0}

    ws = spreadsheet.worksheet(ATTEND_SHEET_NAME)
    all_values = ws.get_all_values()
    index: Dict[Tuple[str, str], int] = {}
    for i, row in enumerate(all_values[1:], start=2):
        if len(row) >= 2:
            index[(row[0], row[1])] = i

    updates: List[Tuple[int, List[str]]] = []
    to_append: List[List[str]] = []
    pushed_keys: List[Tuple[str, str]] = []

    for r in dirty_rows:
        key = (r["tg_poll_id"], r["user_id"])
        values = [r["tg_poll_id"], r["user_id"], r["username"], r["first_name"], r["last_name"],
                  r["vote_text"], r["vote_type"], r["training_date"], r["config_poll_id"],
                  r["updated_at"], str(r["revote_count"])]
        if key in index:
            updates.append((index[key], values))
        else:
            to_append.append(values)
        pushed_keys.append(key)

    updated = inserted = 0
    for row_num, values in updates:
        try:
            ws.update(f"A{row_num}:K{row_num}", [values])
            updated += 1
        except Exception:
            pushed_keys = [k for k in pushed_keys if k != (values[0], values[1])]
    if to_append:
        try:
            ws.append_rows(to_append, value_input_option="USER_ENTERED")
            inserted += len(to_append)
        except Exception:
            append_keys = {(v[0], v[1]) for v in to_append}
            pushed_keys = [k for k in pushed_keys if k not in append_keys]

    if pushed_keys:
        with _connection() as conn:
            conn.executemany(
                "UPDATE attendance SET dirty = 0 WHERE tg_poll_id = ? AND user_id = ?",
                pushed_keys,
            )
            conn.commit()

    return {"pushed": len(pushed_keys), "inserted": inserted, "updated": updated}
