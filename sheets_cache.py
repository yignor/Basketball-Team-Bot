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
from typing import Any, Dict, Iterator, List, Optional

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
"""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5.0)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def _connection() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    """Идемпотентно создаёт схему. Безопасно вызывать при каждом старте
    демона и в любом месте, откуда читают кэш."""
    with _connection() as conn:
        conn.executescript(SCHEMA)
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


def sync_all(spreadsheet) -> Dict[str, Any]:
    """Синхронизирует все три таблицы независимо — ошибка в одной не
    должна мешать остальным."""
    summary: Dict[str, Any] = {}
    for name, fn in (
        ("players", sync_players),
        ("attendance", sync_attendance),
        ("service_log", sync_service_log),
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
