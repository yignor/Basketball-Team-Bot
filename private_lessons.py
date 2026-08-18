"""Частные занятия тренера: свои люди, свои занятия, свои деньги.

Это **не команда**. Тренер ведёт частные тренировки, которые к клубу не имеют
отношения: другие люди, другие деньги, другая ответственность. Поэтому здесь
нет ни одного обращения к листу «Игроки», к Google-таблице, к опросам и
составам, и отсюда никогда ничего не уходит в общий чат. Единственное общее с
остальным ботом — файл базы (значит, и ночной бэкап), больше ничего.

**Всё принадлежит владельцу.** У каждой записи есть `owner_id` — telegram id
того тренера, который её завёл. Тренеров в боте может быть несколько, и чужой
частный заработок — не то, что показывают соседу «заодно». Даже админ видит
здесь только своё: раздел не про команду, а про личное дело человека.

**Имена — короткие, не ФИО.** Люди на частных занятиях нигде больше в боте не
встречаются: неоткуда взять имя, кроме как записать его на диск. Пишем
«Иванов И.», а не «Иванов Иван Иванович» — этого хватает, чтобы тренер узнал
человека в списке, и это ровно тот минимум, который допускает юр-инвариант
проекта для введённых руками людей.

**Деньги — две записи, начисление и оплата.** Начисление появляется, когда
занятие проведено, и после этого **не пересчитывается**: цена может смениться
завтра, но занятие было по вчерашней. Долг человека — разница между тем, что
начислено, и тем, что внесено. Минус означает аванс, и это нормально: за
частные занятия часто платят пакетом вперёд.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

# Занятие ещё не прошло; прошло; отменено.
PLAN, DONE, OFF = "plan", "done", "off"

CHARGE, PAY = "charge", "pay"

# Настройка «сколько стоит занятие по умолчанию» — у каждого тренера своя.
PRICE_KEY = "priv_price"

DAYS_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS priv_people (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id   TEXT NOT NULL,
    label      TEXT NOT NULL,              -- «Иванов И.», не ФИО
    price      INTEGER NOT NULL DEFAULT 0, -- своя цена; 0 = как у всех
    note       TEXT NOT NULL DEFAULT '',
    active     INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS priv_people_label
    ON priv_people (owner_id, label);

CREATE TABLE IF NOT EXISTS priv_sessions (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id   TEXT NOT NULL,
    day        TEXT NOT NULL,              -- YYYY-MM-DD
    at_time    TEXT NOT NULL DEFAULT '',
    place      TEXT NOT NULL DEFAULT '',
    price      INTEGER NOT NULL DEFAULT 0, -- цена этого занятия; 0 = общая
    status     TEXT NOT NULL DEFAULT 'plan',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS priv_sessions_day
    ON priv_sessions (owner_id, day);

CREATE TABLE IF NOT EXISTS priv_visits (
    session_id INTEGER NOT NULL,
    person_id  INTEGER NOT NULL,
    price      INTEGER NOT NULL DEFAULT 0, -- разовая цена; 0 = как обычно
    PRIMARY KEY (session_id, person_id)
);

CREATE TABLE IF NOT EXISTS priv_money (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id   TEXT NOT NULL,
    person_id  INTEGER NOT NULL,
    kind       TEXT NOT NULL,              -- charge | pay
    amount     INTEGER NOT NULL,
    session_id INTEGER NOT NULL DEFAULT 0,
    note       TEXT NOT NULL DEFAULT '',
    at         TEXT NOT NULL,              -- YYYY-MM-DD, когда это случилось
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS priv_money_person
    ON priv_money (owner_id, person_id);
-- Начисление за занятие ровно одно, и отметка «оплатил это занятие» тоже.
-- Двойное нажатие не должно удваивать долг.
CREATE UNIQUE INDEX IF NOT EXISTS priv_money_once
    ON priv_money (session_id, person_id, kind) WHERE session_id > 0;

CREATE TABLE IF NOT EXISTS priv_series (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id   TEXT NOT NULL,
    weekday    INTEGER NOT NULL,           -- 0 = понедельник
    at_time    TEXT NOT NULL DEFAULT '',
    place      TEXT NOT NULL DEFAULT '',
    price      INTEGER NOT NULL DEFAULT 0,
    every      INTEGER NOT NULL DEFAULT 1, -- раз в N недель
    start_day  TEXT NOT NULL,              -- якорь: от него считаем интервал
    roster     TEXT NOT NULL DEFAULT '[]', -- кто ходит обычно, id людей
    active     INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);
"""

_ready = False


def _ensure_column(conn: sqlite3.Connection, table: str, column: str,
                   decl: str) -> None:
    """CREATE TABLE IF NOT EXISTS не добавляет колонку в уже существующую
    таблицу. У кого раздел успел открыться до появления повторов, база уже
    создана — нужна ручная миграция."""
    have = [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]
    if column not in have:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init() -> None:
    """Идемпотентно создаёт свои таблицы. Схему команды не трогает."""
    global _ready
    if _ready:
        return
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.executescript(SCHEMA)
        _ensure_column(conn, "priv_sessions", "series_id",
                       "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "priv_visits", "price",
                       "INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    _ready = True


def _own(owner_id: Any) -> str:
    return str(owner_id)


# ─────────────────────────── имена и даты ──────────────────────────────────


def short_label(text: str) -> str:
    """«Иванов Иван Иванович» → «Иванов И.». Первое слово целиком, остальные —
    инициалами.

    Полные ФИО частных клиентов на диск не кладём: они нигде в боте больше не
    появляются, согласия не давали, а для списка из десяти человек фамилии с
    инициалом хватает с запасом. Коротышки вроде «мл» оставляем как есть — это
    пометка тренера, а не имя («Иванов И. мл»)."""
    words = [w for w in re.split(r"[\s,]+", str(text or "").strip()) if w]
    if not words:
        return ""
    head = words[0][:24]
    out = [head[:1].upper() + head[1:]]
    for w in words[1:3]:
        bare = w.strip(".")
        if not bare:
            continue
        if len(bare) <= 2 and not w.endswith("."):
            out.append(bare)                     # пометка тренера: «мл», «ст»
        else:
            out.append(bare[0].upper() + ".")
    return " ".join(out)[:32]


def human_date(day: str) -> str:
    """«2026-08-12» → «12.08 (ср)»."""
    try:
        d = datetime.strptime(str(day), "%Y-%m-%d").date()
    except ValueError:
        return str(day)
    return f"{d:%d.%m} ({DAYS_SHORT[d.weekday()]})"


def parse_when(text: str, today: Optional[date] = None) -> Optional[Dict[str, str]]:
    """«12.08 19:00 зал на Ленина», «завтра 19:00», «19:00» → дата, время, место.

    Тренер пишет занятие одной строкой, как сказал бы вслух. Требовать формат
    здесь нельзя: это первый экран, на котором заводят занятие, и «не понял,
    попробуйте ещё раз» — то место, где бросают.

    Про дату не написали, но написали время — значит сегодня: чаще всего
    занятие заводят в день занятия. А вот когда во всей строке нет ни даты, ни
    времени, лучше переспросить: молча поставить сегодняшнее число хуже, чем
    задать один вопрос — занятие не на тот день тренер заметит не сразу."""
    today = today or date.today()
    raw = str(text or "").strip()
    if not raw:
        return None
    # Ищем по приведённому к нижнему регистру, а вырезаем из исходного:
    # «Зал на Ленина» должен остаться «Зал на Ленина», а не «зал ленина».
    # Длина при .lower() и замене «ё» сохраняется, поэтому позиции совпадают.
    low = raw.lower().replace("ё", "е")
    taken: List[Tuple[int, int]] = []
    day: Optional[date] = None
    said = False                     # дату назвали явно, а не подставили сами

    for word, shift in (("послезавтра", 2), ("сегодня", 0), ("завтра", 1)):
        at = low.find(word)
        if at >= 0:
            day, said = today + timedelta(days=shift), True
            taken.append((at, at + len(word)))
            break

    if day is None:
        # Дата вида 12.08 или 12.08.2026. Месяц 00 или больше 12 — это не
        # дата, а время, записанное через точку («19.00»).
        m = re.search(r"\b(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\b", low)
        if m and 1 <= int(m.group(2)) <= 12:
            dd, mm = int(m.group(1)), int(m.group(2))
            yy = int(m.group(3) or 0)
            if yy and yy < 100:
                yy += 2000
            try:
                day = date(yy or today.year, mm, dd)
            except ValueError:
                return None
            # Год не назвали, а дата уже далеко позади — значит следующий год.
            if not yy and (today - day).days > 180:
                day = date(today.year + 1, mm, dd)
            taken.append((m.start(), m.end()))
            said = True
        else:
            day = today

    at_time = ""
    for m in re.finditer(r"\b(\d{1,2})[:.](\d{2})\b", low):
        if any(a <= m.start() < b for a, b in taken):
            continue
        if int(m.group(1)) < 24 and int(m.group(2)) < 60:
            at_time = f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
            taken.append((m.start(), m.end()))
        break

    # Ни даты, ни времени во всей строке — это не занятие, а что-то другое.
    if not said and not at_time:
        return None

    rest = raw
    for a, b in sorted(taken, reverse=True):
        rest = rest[:a] + " " + rest[b:]
    words = rest.split()
    if words and words[0].lower() in ("в", "на", "во"):
        words = words[1:]                      # «в 19:00 в зале» → «зале»
    return {"day": day.isoformat(), "at_time": at_time,
            "place": " ".join(words)[:40]}


# ─────────────────────────── цена занятия ──────────────────────────────────


def general_price(owner_id: Any) -> int:
    """Общая цена занятия у этого тренера."""
    return sheets_cache.get_int_setting(f"{PRICE_KEY}:{_own(owner_id)}", 0)


def set_general_price(owner_id: Any, value: int) -> None:
    sheets_cache.set_setting(f"{PRICE_KEY}:{_own(owner_id)}", int(value))


def price_for(owner_id: Any, person: Dict[str, Any],
              session: Optional[Dict[str, Any]] = None,
              once: int = 0) -> int:
    """Сколько платит этот человек за это занятие.

    От частного к общему: разовая цена на этой дате → своя цена человека →
    цена занятия → общая цена.

    Разовая бьёт всё остальное: «сегодня с него 1000, хотя обычно 1500» — это
    решение про конкретное занятие, и оно не должно менять ни его постоянную
    цену, ни цену занятия для остальных."""
    if int(once or 0) > 0:
        return int(once)
    if int(person.get("price") or 0) > 0:
        return int(person["price"])
    if session and int(session.get("price") or 0) > 0:
        return int(session["price"])
    return general_price(owner_id)


def set_visit_price(owner_id: Any, session_id: Any, person_id: Any,
                    price: int) -> bool:
    """Разовая цена человеку на одном занятии. 0 — вернуть обычную.

    Уже начисленное не трогаем: занятие прошло по той цене, которая была. Это
    тот же инвариант, что и везде в разделе, — задним числом деньги не
    переписываем."""
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE priv_visits SET price = ? WHERE session_id = ? AND "
            "person_id = ? AND session_id IN (SELECT id FROM priv_sessions "
            "WHERE owner_id = ?)",
            (int(price), int(session_id), int(person_id), _own(owner_id)))
        conn.commit()
        return cur.rowcount > 0


# ─────────────────────────── люди ──────────────────────────────────────────


def add_person(owner_id: Any, text: str, price: int = 0) -> Dict[str, Any]:
    """Заводит человека. Возвращает карточку или {'error': ...}."""
    init()
    label = short_label(text)
    if not label:
        return {"error": "Пустое имя."}
    with sheets_cache.get_connection() as conn:
        try:
            cur = conn.execute(
                "INSERT INTO priv_people (owner_id, label, price, created_at) "
                "VALUES (?, ?, ?, ?)",
                (_own(owner_id), label, int(price or 0),
                 datetime.now().isoformat(timespec="seconds")))
            conn.commit()
        except sqlite3.IntegrityError:
            return {"error": f"«{label}» уже есть в списке. Добавь пометку, "
                             f"чтобы различать: «{label} мл»."}
        return {"id": int(cur.lastrowid), "label": label, "price": int(price or 0)}


def person(owner_id: Any, person_id: Any) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM priv_people WHERE id = ? AND owner_id = ?",
            (int(person_id), _own(owner_id))).fetchone()
    if not row:
        return None
    got = dict(row)
    got["balance"] = balance(owner_id, got["id"])
    return got


def people(owner_id: Any, archived: bool = False) -> List[Dict[str, Any]]:
    """Люди тренера с долгом каждого. Должники — сверху."""
    init()
    owner = _own(owner_id)
    sql = ("SELECT * FROM priv_people WHERE owner_id = ? AND active = ? "
           "ORDER BY label")
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(sql, (owner, 0 if archived else 1))]
        sums = {}
        for r in conn.execute(
                "SELECT person_id, kind, SUM(amount) AS s FROM priv_money "
                "WHERE owner_id = ? GROUP BY person_id, kind", (owner,)):
            sums.setdefault(int(r["person_id"]), {})[r["kind"]] = int(r["s"] or 0)
    for r in rows:
        got = sums.get(int(r["id"]), {})
        r["balance"] = got.get(CHARGE, 0) - got.get(PAY, 0)
    rows.sort(key=lambda r: (-r["balance"], r["label"]))
    return rows


def set_person_price(owner_id: Any, person_id: Any, price: int) -> bool:
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE priv_people SET price = ? WHERE id = ? AND owner_id = ?",
            (int(price), int(person_id), _own(owner_id)))
        conn.commit()
        return cur.rowcount > 0


def rename_person(owner_id: Any, person_id: Any, text: str) -> Dict[str, Any]:
    """Переименовать. Имя укорачивается тем же правилом, что при заведении.

    Меняется только подпись: всё остальное держится на id, поэтому занятия,
    начисления и оплаты остаются за тем же человеком."""
    init()
    label = short_label(text)
    if not label:
        return {"error": "Пустое имя."}
    with sheets_cache.get_connection() as conn:
        if not conn.execute("SELECT 1 FROM priv_people WHERE id = ? AND "
                            "owner_id = ?",
                            (int(person_id), _own(owner_id))).fetchone():
            return {"error": "Такого человека в списке нет."}
        try:
            conn.execute(
                "UPDATE priv_people SET label = ? WHERE id = ? AND owner_id = ?",
                (label, int(person_id), _own(owner_id)))
            conn.commit()
        except sqlite3.IntegrityError:
            return {"error": f"«{label}» уже занято. Добавь пометку, чтобы "
                             f"различать: «{label} мл»."}
    return {"label": label}


def person_stats(owner_id: Any, person_id: Any) -> Dict[str, Any]:
    """Что за человеком числится: занятия и деньги.

    Нужно перед удалением. Сказать «удалю» и не сказать, что вместе с ним из
    итогов месяца уйдут проведённые деньги, — это подстава."""
    init()
    owner = _own(owner_id)
    with sheets_cache.get_connection() as conn:
        visits = int(conn.execute(
            "SELECT COUNT(*) FROM priv_visits v JOIN priv_sessions s "
            "ON s.id = v.session_id WHERE v.person_id = ? AND s.owner_id = ?",
            (int(person_id), owner)).fetchone()[0])
        rows = conn.execute(
            "SELECT kind, COUNT(*) AS n, SUM(amount) AS s FROM priv_money "
            "WHERE owner_id = ? AND person_id = ? GROUP BY kind",
            (owner, int(person_id))).fetchall()
    got = {r["kind"]: (int(r["n"]), int(r["s"] or 0)) for r in rows}
    charged, paid = got.get(CHARGE, (0, 0)), got.get(PAY, (0, 0))
    return {"visits": visits, "records": charged[0] + paid[0],
            "charged": charged[1], "paid": paid[1],
            "balance": charged[1] - paid[1]}


def forget_person(owner_id: Any, person_id: Any) -> Dict[str, Any]:
    """Удалить человека совсем: его самого, его записи на занятия и деньги.

    Именно удалить, а не спрятать: для «спрятать» есть архив, и если «удалить»
    делает то же самое, кнопка врёт. Заодно вычищаем его из расписаний —
    иначе повторяющиеся занятия продолжали бы записывать на них призрака."""
    init()
    owner = _own(owner_id)
    was = person_stats(owner_id, person_id)
    with sheets_cache.get_connection() as conn:
        if not conn.execute("SELECT 1 FROM priv_people WHERE id = ? AND "
                            "owner_id = ?", (int(person_id), owner)).fetchone():
            return {"error": "Такого человека в списке нет."}
        conn.execute(
            "DELETE FROM priv_visits WHERE person_id = ? AND session_id IN "
            "(SELECT id FROM priv_sessions WHERE owner_id = ?)",
            (int(person_id), owner))
        conn.execute("DELETE FROM priv_money WHERE owner_id = ? AND person_id = ?",
                     (owner, int(person_id)))
        for s in conn.execute("SELECT id, roster FROM priv_series WHERE "
                              "owner_id = ?", (owner,)).fetchall():
            roster = [int(x) for x in json.loads(s["roster"] or "[]")
                      if int(x) != int(person_id)]
            conn.execute("UPDATE priv_series SET roster = ? WHERE id = ?",
                         (json.dumps(roster), int(s["id"])))
        conn.execute("DELETE FROM priv_people WHERE id = ? AND owner_id = ?",
                     (int(person_id), owner))
        conn.commit()
    return was


def archive(owner_id: Any, person_id: Any, active: bool = False) -> bool:
    """В архив и обратно. Не удаляем: за человеком может остаться долг и
    история, а список выбора всё равно чистится."""
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE priv_people SET active = ? WHERE id = ? AND owner_id = ?",
            (1 if active else 0, int(person_id), _own(owner_id)))
        conn.commit()
        return cur.rowcount > 0


# ─────────────────────────── занятия ───────────────────────────────────────


def add_session(owner_id: Any, day: str, at_time: str = "", place: str = "",
                price: int = 0) -> int:
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO priv_sessions (owner_id, day, at_time, place, price, "
            "created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (_own(owner_id), str(day), str(at_time), str(place)[:40],
             int(price or 0), datetime.now().isoformat(timespec="seconds")))
        conn.commit()
        return int(cur.lastrowid)


def session(owner_id: Any, session_id: Any) -> Optional[Dict[str, Any]]:
    """Занятие вместе со списком записанных и деньгами по каждому."""
    init()
    owner = _own(owner_id)
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM priv_sessions WHERE id = ? AND owner_id = ?",
            (int(session_id), owner)).fetchone()
        if not row:
            return None
        got = dict(row)
        rows = conn.execute(
            "SELECT person_id, COALESCE(price, 0) AS price FROM priv_visits "
            "WHERE session_id = ?", (int(session_id),)).fetchall()
        going = {int(r["person_id"]) for r in rows}
        once = {int(r["person_id"]): int(r["price"]) for r in rows}
        money = {}
        for r in conn.execute(
                "SELECT person_id, kind, amount FROM priv_money "
                "WHERE session_id = ? AND owner_id = ?", (int(session_id), owner)):
            money.setdefault(int(r["person_id"]), {})[r["kind"]] = int(r["amount"])
    got["going"] = going
    members = []
    # Архивные тоже: человек мог уйти после занятия, но на нём он был и долг
    # за ним числится. Пропустить его значило бы потерять деньги из виду.
    for p in people(owner_id) + people(owner_id, archived=True):
        if int(p["id"]) not in going:
            continue
        m = money.get(int(p["id"]), {})
        # price — что человек платит именно за это занятие: уже начисленное,
        # если занятие проведено, иначе расчётное. price_own — его личная
        # цена, чтобы экран мог показать «(своя цена)».
        spot = once.get(int(p["id"]), 0)
        members.append({**p, "price_own": int(p["price"] or 0),
                        "price_once": spot,
                        "price": m.get(CHARGE, price_for(owner_id, p, got, spot)),
                        "charged": CHARGE in m, "paid": PAY in m})
    members.sort(key=lambda p: p["label"])
    got["members"] = members
    got["total"] = sum(m["price"] for m in members)
    return got


def sessions(owner_id: Any, limit: int = 10) -> List[Dict[str, Any]]:
    """Ближайшие и последние занятия: будущие сверху, потом свежие прошедшие."""
    init()
    owner = _own(owner_id)
    today = date.today().isoformat()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT s.*, (SELECT COUNT(*) FROM priv_visits v "
            "             WHERE v.session_id = s.id) AS going "
            "FROM priv_sessions s WHERE s.owner_id = ? AND s.status != ? "
            "ORDER BY s.day DESC, s.at_time DESC", (owner, OFF))]
    ahead = [r for r in rows if r["day"] >= today]
    past = [r for r in rows if r["day"] < today]
    ahead.reverse()
    return (ahead + past)[:limit]


def set_session_when(owner_id: Any, session_id: Any, day: str,
                     at_time: str = "", place: str = "") -> bool:
    """Перенести занятие: дата, время, место.

    Заводится оно руками и руками же ошибается — перепутанный день недели или
    не тот зал вылезают уже после того, как всё записано. Заводить занятие
    заново нельзя: к нему привязаны люди, а если оно прошло — ещё и деньги.

    Начисления не трогаем: они сделаны по факту занятия, а не по его дате."""
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE priv_sessions SET day = ?, at_time = ?, place = ? "
            "WHERE id = ? AND owner_id = ?",
            (str(day), str(at_time), str(place)[:40], int(session_id),
             _own(owner_id)))
        conn.commit()
        return cur.rowcount > 0


def set_session_price(owner_id: Any, session_id: Any, price: int) -> bool:
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE priv_sessions SET price = ? WHERE id = ? AND owner_id = ?",
            (int(price), int(session_id), _own(owner_id)))
        conn.commit()
        return cur.rowcount > 0


def toggle_visit(owner_id: Any, session_id: Any, person_id: Any) -> bool:
    """Записать человека на занятие или снять. Возвращает «идёт ли теперь».

    Состав занятия меняется до последнего: кто-то отпишется утром, кто-то
    добавится за час. Если занятие уже проведено, вместе с человеком уходит и
    его начисление — иначе долг остаётся за того, кого не было."""
    init()
    owner = _own(owner_id)
    with sheets_cache.get_connection() as conn:
        if not conn.execute("SELECT 1 FROM priv_sessions WHERE id = ? AND "
                            "owner_id = ?", (int(session_id), owner)).fetchone():
            return False
        there = conn.execute(
            "SELECT 1 FROM priv_visits WHERE session_id = ? AND person_id = ?",
            (int(session_id), int(person_id))).fetchone()
        if there:
            conn.execute("DELETE FROM priv_visits WHERE session_id = ? AND "
                         "person_id = ?", (int(session_id), int(person_id)))
            conn.execute("DELETE FROM priv_money WHERE session_id = ? AND "
                         "person_id = ? AND kind = ?",
                         (int(session_id), int(person_id), CHARGE))
        else:
            conn.execute("INSERT INTO priv_visits (session_id, person_id) "
                         "VALUES (?, ?)", (int(session_id), int(person_id)))
        conn.commit()
    if not there:
        # Занятие уже провели, а человека дописали задним числом — начисление
        # должно появиться сразу, иначе он «был бесплатно».
        got = session(owner_id, session_id)
        if got and got["status"] == DONE:
            close_session(owner_id, session_id)
    return not there


def close_session(owner_id: Any, session_id: Any) -> Dict[str, Any]:
    """Занятие проведено: каждому записанному — начисление по его цене.

    Начисление ставится один раз и потом не пересчитывается. Цена может
    вырасти через месяц, но занятие было по старой — задним числом долги не
    меняются. Повторный вызов только дописывает недостающих."""
    init()
    owner = _own(owner_id)
    got = session(owner_id, session_id)
    if not got:
        return {"error": "Занятие не найдено."}
    now = datetime.now().isoformat(timespec="seconds")
    added = 0
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE priv_sessions SET status = ? WHERE id = ? AND "
                     "owner_id = ?", (DONE, int(session_id), owner))
        for m in got["members"]:
            if m["charged"]:
                continue
            price = int(m["price"])
            if price <= 0:
                continue
            try:
                conn.execute(
                    "INSERT INTO priv_money (owner_id, person_id, kind, amount, "
                    "session_id, note, at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (owner, int(m["id"]), CHARGE, price, int(session_id),
                     "занятие", got["day"], now))
                added += 1
            except sqlite3.IntegrityError:
                pass
        conn.commit()
    free = [m["label"] for m in got["members"] if not m["charged"] and m["price"] <= 0]
    return {"charged": added, "people": len(got["members"]), "free": free}


def cancel_session(owner_id: Any, session_id: Any) -> bool:
    """Занятие отменено: начисления снимаем, а внесённые деньги — нет.

    Деньги человек отдал, они никуда не делись: без начисления они просто
    станут авансом и закроют следующее занятие. Стереть их вместе с занятием
    значило бы потерять чужие деньги."""
    init()
    owner = _own(owner_id)
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE priv_sessions SET status = ? WHERE id = ? AND owner_id = ?",
            (OFF, int(session_id), owner))
        conn.execute("DELETE FROM priv_money WHERE session_id = ? AND "
                     "owner_id = ? AND kind = ?", (int(session_id), owner, CHARGE))
        conn.execute("UPDATE priv_money SET session_id = 0, note = ? "
                     "WHERE session_id = ? AND owner_id = ? AND kind = ?",
                     ("за отменённое занятие", int(session_id), owner, PAY))
        conn.commit()
        return cur.rowcount > 0


# ─────────────────────────── повторение ────────────────────────────────────
#
# Частные занятия почти всегда идут по расписанию: «каждую среду в 19:00, те же
# люди». Заводить их по одному — самая частая работа тренера в этом разделе, и
# самая бессмысленная.
#
# Занятия заводим НАСТОЯЩИМИ строками на несколько недель вперёд, а не считаем
# расписание на лету. Причина простая: к конкретной дате привязано всё
# остальное — кто идёт (а состав меняется), цена, начисления, оплаты. К
# вычисляемой дате ничего не привяжешь, и первая же отмена одного занятия
# сломала бы всю затею.
#
# Раскладываем лениво, при открытии раздела. Фоновой задачи здесь нет намеренно:
# раздел ничего не шлёт и никуда не спешит, а лишний планировщик — лишнее место,
# где что-то тихо ломается.

# На сколько недель вперёд держим заведённые занятия. Четыре — чтобы месяц было
# видно и можно было планировать, но список не тонул в датах.
AHEAD_WEEKS = 4

# «каждую среду» и «по средам» — падежи разные, и оба нужны.
DAY_EVERY = ["каждый понедельник", "каждый вторник", "каждую среду",
             "каждый четверг", "каждую пятницу", "каждую субботу",
             "каждое воскресенье"]
DAY_ON = ["по понедельникам", "по вторникам", "по средам", "по четвергам",
          "по пятницам", "по субботам", "по воскресеньям"]


def series_title(s: Dict[str, Any]) -> str:
    """«каждую среду в 19:00» / «раз в две недели по средам в 19:00»."""
    if not s:
        return ""
    day = int(s["weekday"]) % 7
    every = int(s.get("every") or 1)
    head = DAY_EVERY[day] if every == 1 else f"раз в две недели {DAY_ON[day]}"
    if every > 2:
        head = f"раз в {every} недели {DAY_ON[day]}"
    return head + (f" в {s['at_time']}" if s.get("at_time") else "")


def series(owner_id: Any, series_id: Any) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM priv_series WHERE id = ? AND owner_id = ?",
            (int(series_id), _own(owner_id))).fetchone()
    if not row:
        return None
    got = dict(row)
    got["people"] = json.loads(got.get("roster") or "[]")
    return got


def series_list(owner_id: Any) -> List[Dict[str, Any]]:
    """Действующие расписания тренера."""
    init()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM priv_series WHERE owner_id = ? AND active = 1 "
            "ORDER BY weekday, at_time", (_own(owner_id),))]


def repeat_session(owner_id: Any, session_id: Any, every: int = 1) -> Dict[str, Any]:
    """Сделать занятие образцом: тот же день недели, время, место, цена и люди.

    Состав берём тот, что записан сейчас: если человек ходит по средам, он
    ходит по средам. Поправить на конкретную дату всё равно можно — каждое
    занятие остаётся отдельным."""
    init()
    owner = _own(owner_id)
    got = session(owner_id, session_id)
    if not got:
        return {"error": "Занятие не найдено."}
    if int(got.get("series_id") or 0):
        return {"error": "Это занятие уже повторяется."}
    day = datetime.strptime(got["day"], "%Y-%m-%d").date()
    roster = sorted(int(x) for x in got["going"])
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO priv_series (owner_id, weekday, at_time, place, price, "
            "every, start_day, roster, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (owner, day.weekday(), got["at_time"], got["place"],
             int(got["price"] or 0), max(1, int(every)), got["day"],
             json.dumps(roster), datetime.now().isoformat(timespec="seconds")))
        sid = int(cur.lastrowid)
        conn.execute("UPDATE priv_sessions SET series_id = ? WHERE id = ? AND "
                     "owner_id = ?", (sid, int(session_id), owner))
        conn.commit()
    made = ensure_ahead(owner_id)
    return {"series_id": sid, "made": made,
            "title": series_title(series(owner_id, sid) or {})}


def stop_series(owner_id: Any, series_id: Any) -> Dict[str, Any]:
    """Больше не повторять.

    Заведённые вперёд, но ещё не проведённые занятия убираем: их создал не
    человек, а расписание, и оставлять их висеть в списке — мусор. Всё, что уже
    прошло, остаётся нетронутым: там деньги и история."""
    init()
    owner = _own(owner_id)
    today = date.today().isoformat()
    with sheets_cache.get_connection() as conn:
        if not conn.execute("SELECT 1 FROM priv_series WHERE id = ? AND "
                            "owner_id = ?", (int(series_id), owner)).fetchone():
            return {"error": "Повторение не найдено."}
        doomed = [int(r["id"]) for r in conn.execute(
            "SELECT id FROM priv_sessions WHERE owner_id = ? AND series_id = ? "
            "AND status = ? AND day >= ?", (owner, int(series_id), PLAN, today))]
        for one in doomed:
            conn.execute("DELETE FROM priv_visits WHERE session_id = ?", (one,))
            conn.execute("DELETE FROM priv_sessions WHERE id = ?", (one,))
        conn.execute("UPDATE priv_series SET active = 0 WHERE id = ? AND "
                     "owner_id = ?", (int(series_id), owner))
        conn.commit()
    return {"dropped": len(doomed)}


def ensure_ahead(owner_id: Any, weeks: int = AHEAD_WEEKS) -> int:
    """Раскладывает занятия по расписанию на несколько недель вперёд.

    Возвращает, сколько завёл. Вызывать можно сколько угодно: дата, которая уже
    есть, второй раз не создаётся — в том числе отменённая. Отменил тренер одно
    занятие в серии — оно не должно воскреснуть при следующем открытии
    раздела."""
    init()
    owner = _own(owner_id)
    today = date.today()
    horizon = today + timedelta(weeks=max(1, int(weeks)))
    made = 0
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM priv_series WHERE owner_id = ? AND active = 1", (owner,))]
        for s in rows:
            every = max(1, int(s["every"] or 1))
            try:
                anchor = datetime.strptime(s["start_day"], "%Y-%m-%d").date()
            except ValueError:
                continue
            # Первая дата не раньше сегодняшнего дня и не раньше самой серии.
            day = max(anchor, today)
            day += timedelta(days=(int(s["weekday"]) - day.weekday()) % 7)
            while day <= horizon:
                if (day - anchor).days % (7 * every) == 0:
                    have = conn.execute(
                        "SELECT 1 FROM priv_sessions WHERE owner_id = ? AND "
                        "series_id = ? AND day = ?",
                        (owner, int(s["id"]), day.isoformat())).fetchone()
                    if not have:
                        cur = conn.execute(
                            "INSERT INTO priv_sessions (owner_id, day, at_time, "
                            "place, price, series_id, created_at) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (owner, day.isoformat(), s["at_time"], s["place"],
                             int(s["price"] or 0), int(s["id"]),
                             datetime.now().isoformat(timespec="seconds")))
                        new_id = int(cur.lastrowid)
                        for pid in json.loads(s["roster"] or "[]"):
                            conn.execute(
                                "INSERT OR IGNORE INTO priv_visits (session_id, "
                                "person_id) VALUES (?, ?)", (new_id, int(pid)))
                        made += 1
                day += timedelta(days=7)
        if made:
            conn.commit()
    return made


def series_roster(owner_id: Any, series_id: Any, people_ids: List[int]) -> bool:
    """Обновить «кто ходит обычно». Меняет только будущие пустые даты."""
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE priv_series SET roster = ? WHERE id = ? AND owner_id = ?",
            (json.dumps(sorted(int(x) for x in people_ids)), int(series_id),
             _own(owner_id)))
        conn.commit()
        return cur.rowcount > 0


# ─────────────────────────── деньги ────────────────────────────────────────


def toggle_paid(owner_id: Any, session_id: Any, person_id: Any) -> bool:
    """Отметка «заплатил за это занятие». Возвращает «оплачено ли теперь».

    Сначала засчитываем то, что человек уже внёс без привязки к занятию, и
    только если такого нет — заводим новую оплату.

    Без этого две дороги записывали одно и то же. Тренер вносил деньги на
    карточке человека («💰 Внести оплату»), а потом отмечал его же галочкой на
    проведённом занятии — и получалось две оплаты за одно занятие. У троих так
    и вышло 14.08.2026: начислено 5 400, «получено» 8 100, у каждого лишний
    аванс в 900 ₽.

    Снятие отметки: оплату, заведённую самой галочкой, удаляем; засчитанную —
    возвращаем в неразнесённые. Деньги человек отдал, и стирать их из-за
    снятой галочки нельзя."""
    init()
    owner = _own(owner_id)
    got = session(owner_id, session_id)
    if not got:
        return False
    who = next((m for m in got["members"] if int(m["id"]) == int(person_id)), None)
    if not who:
        return False
    with sheets_cache.get_connection() as conn:
        if who["paid"]:
            row = conn.execute(
                "SELECT id, note FROM priv_money WHERE session_id = ? AND "
                "person_id = ? AND kind = ?",
                (int(session_id), int(person_id), PAY)).fetchone()
            if row and str(row["note"] or "") == "за занятие":
                conn.execute("DELETE FROM priv_money WHERE id = ?", (row["id"],))
            elif row:
                conn.execute("UPDATE priv_money SET session_id = 0 WHERE id = ?",
                             (row["id"],))
            conn.commit()
            return False

        # Неразнесённая оплата этого человека, которой хватает на занятие.
        free = conn.execute(
            "SELECT id FROM priv_money WHERE owner_id = ? AND person_id = ? "
            "AND kind = ? AND session_id = 0 AND amount >= ? "
            "ORDER BY at, id LIMIT 1",
            (owner, int(person_id), PAY, int(who["price"]))).fetchone()
        if free:
            conn.execute("UPDATE priv_money SET session_id = ? WHERE id = ?",
                         (int(session_id), int(free["id"])))
        else:
            conn.execute(
                "INSERT INTO priv_money (owner_id, person_id, kind, amount, "
                "session_id, note, at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (owner, int(person_id), PAY, int(who["price"]), int(session_id),
                 "за занятие", got["day"],
                 datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    return True


def add_payment(owner_id: Any, person_id: Any, amount: int, note: str = "",
                at: str = "") -> int:
    """Оплата без привязки к занятию: наличными, переводом, пакетом вперёд."""
    init()
    now = datetime.now()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO priv_money (owner_id, person_id, kind, amount, "
            "session_id, note, at, created_at) VALUES (?, ?, ?, ?, 0, ?, ?, ?)",
            (_own(owner_id), int(person_id), PAY, int(amount), str(note)[:60],
             at or now.date().isoformat(), now.isoformat(timespec="seconds")))
        conn.commit()
        return int(cur.lastrowid)


def add_charge(owner_id: Any, person_id: Any, amount: int, note: str = "") -> int:
    """Начисление руками: пропущенное занятие, аренда, что угодно."""
    init()
    now = datetime.now()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO priv_money (owner_id, person_id, kind, amount, "
            "session_id, note, at, created_at) VALUES (?, ?, ?, ?, 0, ?, ?, ?)",
            (_own(owner_id), int(person_id), CHARGE, int(amount), str(note)[:60],
             now.date().isoformat(), now.isoformat(timespec="seconds")))
        conn.commit()
        return int(cur.lastrowid)


def balance(owner_id: Any, person_id: Any) -> int:
    """Начислено минус внесено. Больше нуля — должен, меньше — аванс."""
    init()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT kind, SUM(amount) AS s FROM priv_money "
            "WHERE owner_id = ? AND person_id = ? GROUP BY kind",
            (_own(owner_id), int(person_id))).fetchall()
    got = {r["kind"]: int(r["s"] or 0) for r in rows}
    return got.get(CHARGE, 0) - got.get(PAY, 0)


def debtors(owner_id: Any) -> List[Dict[str, Any]]:
    """Кто должен. Архивные тоже: ушёл — долг не растворился."""
    init()
    both = people(owner_id) + people(owner_id, archived=True)
    return sorted([p for p in both if p["balance"] > 0],
                  key=lambda p: -p["balance"])


def history(owner_id: Any, person_id: Any, limit: int = 20) -> List[Dict[str, Any]]:
    """Движение денег по человеку, свежее сверху."""
    init()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM priv_money WHERE owner_id = ? AND person_id = ? "
            "ORDER BY at DESC, id DESC LIMIT ?",
            (_own(owner_id), int(person_id), int(limit)))]


def payments(owner_id: Any, limit: int = 10) -> List[Dict[str, Any]]:
    """Последние оплаты — с именами, чтобы экран не собирал их сам."""
    init()
    owner = _own(owner_id)
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT m.*, p.label FROM priv_money m "
            "LEFT JOIN priv_people p ON p.id = m.person_id "
            "WHERE m.owner_id = ? AND m.kind = ? "
            "ORDER BY m.at DESC, m.id DESC LIMIT ?", (owner, PAY, int(limit)))]


def drop_money(owner_id: Any, money_id: Any) -> bool:
    """Удалить запись о деньгах — ошибся при вводе."""
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute("DELETE FROM priv_money WHERE id = ? AND owner_id = ?",
                           (int(money_id), _own(owner_id)))
        conn.commit()
        return cur.rowcount > 0


def month(owner_id: Any, ym: str = "") -> Dict[str, Any]:
    """Итог месяца: сколько занятий, начислено, получено, сколько висит."""
    init()
    owner = _own(owner_id)
    ym = ym or date.today().strftime("%Y-%m")
    with sheets_cache.get_connection() as conn:
        held = int(conn.execute(
            "SELECT COUNT(*) FROM priv_sessions WHERE owner_id = ? AND "
            "status = ? AND day LIKE ?", (owner, DONE, ym + "-%")).fetchone()[0])
        rows = conn.execute(
            "SELECT kind, SUM(amount) AS s FROM priv_money WHERE owner_id = ? "
            "AND at LIKE ? GROUP BY kind", (owner, ym + "-%")).fetchall()
    got = {r["kind"]: int(r["s"] or 0) for r in rows}
    # Авансы считаем отдельно: без них «получено больше, чем начислено»
    # выглядит ошибкой, а это чаще всего плата вперёд.
    ahead = sum(-p["balance"] for p in people(owner_id) if p["balance"] < 0)
    ahead += sum(-p["balance"] for p in people(owner_id, archived=True)
                 if p["balance"] < 0)
    return {"ym": ym, "sessions": held,
            "charged": got.get(CHARGE, 0), "paid": got.get(PAY, 0),
            "ahead": ahead,
            "debt": sum(p["balance"] for p in debtors(owner_id))}


def month_title(ym: str) -> str:
    names = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль",
             "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
    try:
        y, m = ym.split("-")
        return f"{names[int(m) - 1]} {y}"
    except (ValueError, IndexError):
        return ym


def rub(amount: int) -> str:
    """1500 → «1 500 ₽». Внутри неразрывный пробел: сумма не должна рваться
    переносом строки на «1» и «500 ₽»."""
    return f"{int(amount):,}".replace(",", " ") + " ₽"


def money_word(amount: int) -> str:
    """Долг или аванс — человеку понятнее словом, чем знаком минуса."""
    if amount > 0:
        return f"должен {rub(amount)}"
    if amount < 0:
        return f"аванс {rub(-amount)}"
    return "рассчитались"
