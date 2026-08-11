"""База одной команды: что в ней лежит с первого дня.

Только то, что нужно ядру без лиг — опросы, посещаемость, состав, деньги.
Статистика, фэнтези и таймкоды придут отдельными таблицами, когда дойдёт
очередь; заводить их пустыми заранее незачем.

Настройки живут парами ключ-значение, а не столбцами: у нового бота настройки
меняются каждую неделю, и миграция ради «а давайте ещё один час напоминания»
— не та цена, которую стоит платить. Всё, что ведёт человек (состав, суммы), —
нормальные таблицы: их читают выборками и сортируют.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import db

SCHEMA = """
-- Настройки команды: название, расписание тренировок, суммы взносов, часы
-- напоминаний. Всё, что в старом боте лежало в листе «Конфиг».
CREATE TABLE IF NOT EXISTS settings (
    key    TEXT PRIMARY KEY,
    value  TEXT NOT NULL DEFAULT ''
);

-- Состав команды. Строка листа больше не ключ: ключ — id, он не меняется от
-- сортировок. Это прямая заплата на грабли старого бота, где номер строки
-- листа считался ключом и связки уезжали при сортировке.
CREATE TABLE IF NOT EXISTS players (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    surname     TEXT NOT NULL DEFAULT '',
    name        TEXT NOT NULL DEFAULT '',
    username    TEXT NOT NULL DEFAULT '',   -- без @
    tg_user_id  TEXT NOT NULL DEFAULT '',   -- заполнится, когда нажмёт «Старт»
    number      TEXT NOT NULL DEFAULT '',
    role        TEXT NOT NULL DEFAULT '',   -- амплуа
    birthday    TEXT NOT NULL DEFAULT '',
    active      INTEGER NOT NULL DEFAULT 1, -- ждём ли взнос за месяц
    pay_season  INTEGER NOT NULL DEFAULT 0, -- своя сумма, если отличается
    pay_game    INTEGER NOT NULL DEFAULT 0,
    added_at    TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_players_tg
    ON players(tg_user_id) WHERE tg_user_id != '';

-- Опросы: и тренировочные, и игровые. Один вид записи вместо двух — они
-- отличаются только поводом, а обрабатываются одинаково.
CREATE TABLE IF NOT EXISTS polls (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,              -- training | game
    tg_poll_id  TEXT NOT NULL DEFAULT '',
    chat_id     TEXT NOT NULL DEFAULT '',
    message_id  TEXT NOT NULL DEFAULT '',
    event_date  TEXT NOT NULL DEFAULT '',   -- ISO: когда тренировка или игра
    title       TEXT NOT NULL DEFAULT '',
    closed      INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_polls_tg
    ON polls(tg_poll_id) WHERE tg_poll_id != '';

-- Голоса. Отдельной строкой на человека и опрос; переголосование правит её же.
CREATE TABLE IF NOT EXISTS votes (
    poll_id     INTEGER NOT NULL,
    tg_user_id  TEXT NOT NULL,
    player_id   INTEGER NOT NULL DEFAULT 0, -- 0 — ещё не опознан
    answer      TEXT NOT NULL DEFAULT '',   -- как ответил, словами
    kind        TEXT NOT NULL DEFAULT '',   -- yes | no | maybe
    updated_at  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (poll_id, tg_user_id)
);

-- Игры: и найденные в лиге, и заведённые тренером руками.
CREATE TABLE IF NOT EXISTS games (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    source      TEXT NOT NULL DEFAULT 'manual',
    league_id   TEXT NOT NULL DEFAULT '',   -- id в лиге, если она есть
    opponent    TEXT NOT NULL DEFAULT '',
    game_date   TEXT NOT NULL DEFAULT '',
    game_time   TEXT NOT NULL DEFAULT '',
    arena       TEXT NOT NULL DEFAULT '',
    form        TEXT NOT NULL DEFAULT '',
    posted_at   TEXT NOT NULL DEFAULT '',   -- когда состав ушёл в чат
    created_at  TEXT NOT NULL DEFAULT ''
);

-- Заявка на игру: кого тренер берёт. С неё же считается оплата игры.
CREATE TABLE IF NOT EXISTS game_roster (
    game_id    INTEGER NOT NULL,
    player_id  INTEGER NOT NULL,
    started    INTEGER NOT NULL DEFAULT 0,  -- в стартовой пятёрке
    PRIMARY KEY (game_id, player_id)
);

-- Деньги: и приход, и разовые долги одной таблицей. Знак различает.
CREATE TABLE IF NOT EXISTS money (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id  INTEGER NOT NULL DEFAULT 0,
    who        TEXT NOT NULL DEFAULT '',    -- имя, если человека нет в составе
    amount     INTEGER NOT NULL,            -- + приход, − начисленный долг
    kind       TEXT NOT NULL DEFAULT '',    -- season | game | other
    period     TEXT NOT NULL DEFAULT '',    -- «2026-09» для взносов
    game_id    INTEGER NOT NULL DEFAULT 0,
    note       TEXT NOT NULL DEFAULT '',
    added_at   TEXT NOT NULL DEFAULT '',
    added_by   TEXT NOT NULL DEFAULT ''
);

-- Что бот уже отправлял: по ключу события. Защита от повторов при частом тике.
CREATE TABLE IF NOT EXISTS sent (
    event_key  TEXT PRIMARY KEY,
    sent_at    TEXT NOT NULL,
    details    TEXT NOT NULL DEFAULT ''
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def create() -> None:
    """Разворачивает схему в базе текущей команды."""
    with db.connection() as conn:
        conn.executescript(SCHEMA)
        conn.commit()


def set_setting(key: str, value: Any) -> None:
    with db.connection() as conn:
        conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(key), str(value)))
        conn.commit()


def setting(key: str, default: str = "") -> str:
    with db.connection() as conn:
        row = conn.execute("SELECT value FROM settings WHERE key = ?",
                           (str(key),)).fetchone()
    return str(row["value"]) if row else default


def settings() -> Dict[str, str]:
    with db.connection() as conn:
        return {r["key"]: r["value"] for r in conn.execute("SELECT * FROM settings")}


def add_players(people: List[Dict[str, str]]) -> int:
    """Добавляет состав из мастера. Возвращает, сколько добавилось."""
    if not people:
        return 0
    with db.connection() as conn:
        for p in people:
            conn.execute(
                """INSERT INTO players (surname, name, username, number, added_at)
                   VALUES (?, ?, ?, ?, ?)""",
                (str(p.get("surname", ""))[:40], str(p.get("name", ""))[:40],
                 str(p.get("username", "")).lstrip("@")[:40],
                 str(p.get("number", ""))[:3], _now()))
        conn.commit()
    return len(people)


def players(active_only: bool = False) -> List[Dict[str, Any]]:
    sql = "SELECT * FROM players"
    if active_only:
        sql += " WHERE active = 1"
    with db.connection() as conn:
        rows = [dict(r) for r in conn.execute(sql)]
    for r in rows:
        r["title"] = f"{r['surname']} {r['name']}".strip()
    # Сортируем в Python: sqlite-функция lower() кириллицу не трогает, и
    # «ёлкин» с «Ёлкиным» разъезжаются (эта грабля в проекте уже была).
    rows.sort(key=lambda r: (r["surname"].lower().replace("ё", "е"),
                             r["name"].lower().replace("ё", "е")))
    return rows


def link_player(tg_user_id: Any, username: str = "",
                player_id: int = 0) -> Optional[Dict[str, Any]]:
    """Связывает телеграм-аккаунт с человеком из состава.

    Сначала по нику — его тренер вписал при подключении. Не нашли — вернём
    None, и бот предложит выбрать себя из списка: угадывать по имени нельзя,
    в команде бывают два Ивана."""
    uid, nick = str(tg_user_id), str(username or "").lstrip("@").lower()
    with db.connection() as conn:
        if player_id:
            row = conn.execute("SELECT * FROM players WHERE id = ?",
                               (int(player_id),)).fetchone()
        elif nick:
            row = conn.execute(
                "SELECT * FROM players WHERE lower(username) = ? AND tg_user_id = ''",
                (nick,)).fetchone()
        else:
            row = None
        if not row:
            return None
        conn.execute("UPDATE players SET tg_user_id = ? WHERE id = ?",
                     (uid, row["id"]))
        conn.commit()
        got = dict(row)
    got["tg_user_id"] = uid
    got["title"] = f"{got['surname']} {got['name']}".strip()
    return got
