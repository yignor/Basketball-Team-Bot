#!/usr/bin/env python3
"""Группы игроков, рассылка по группе и повторяющиеся письма.

Зачем. Команда давно перестала быть одним списком: есть основа, есть второй
состав, есть те, кто ездит только на кубок. Тренер держал это в голове и писал
каждому руками — а бот умел обращаться либо ко всем, либо к одному.

Три вещи, и они разные:

* **Группа** — свободное имя и список игроков. Никакой заранее заданной
  номенклатуры: «Основа», «Второй состав», «Кто на выезд» — как тренеру удобно.
* **Привязка к лиге** — необязательная. Состав играет в конкретном турнире, и
  когда это записано, видно, кого касается расписание именно этой лиги.
* **Рассылка** — письмо всем из группы **в личку**. В общий чат отсюда не
  уходит ничего и никогда: это правило раздела тренера, а не настройка.

Повторяющееся письмо — то же самое по расписанию: день недели и время. Нужно
для «в четверг напомни второму составу про зал»: текст один и тот же каждую
неделю, и перенабирать его — работа, которую и просили убрать.

Хранение — свои таблицы в общей базе. Состав группы держим по строке листа
«Игроки», как и всё остальное в боте: строка — это и есть человек, и по ней же
находится его личный чат. ФИО здесь не хранится.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# Длина имени. Ограничение не выдумано: имя группы едет в подпись кнопки, а в
# callback_data живёт только номер — но подпись длиннее полусотни знаков
# Telegram обрежет, и тренер не узнает свою группу в списке.
NAME_LIMIT = 40

DAYS = ["понедельник", "вторник", "среда", "четверг", "пятница",
        "суббота", "воскресенье"]
DAYS_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS pg_groups (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL,
    league_source TEXT NOT NULL DEFAULT '',   -- 'infobasket' | 'slpro' | ''
    league_team   TEXT NOT NULL DEFAULT '',   -- team_id внутри лиги
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pg_members (
    group_id   INTEGER NOT NULL,
    player_row INTEGER NOT NULL,
    added_at   TEXT NOT NULL,
    PRIMARY KEY (group_id, player_row)
);

CREATE TABLE IF NOT EXISTS pg_templates (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    body       TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pg_repeats (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id    INTEGER NOT NULL,
    template_id INTEGER NOT NULL DEFAULT 0,
    body        TEXT NOT NULL DEFAULT '',
    weekday     INTEGER NOT NULL,
    at_time     TEXT NOT NULL,
    active      INTEGER NOT NULL DEFAULT 1,
    last_sent   TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL
);
"""

_ready = False


def init() -> None:
    """Идемпотентно создаёт свои таблицы. Чужих не трогает."""
    global _ready
    if _ready:
        return
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.executescript(SCHEMA)
        # Оплата в лиге появилась позже самих групп: у кого группы уже есть,
        # колонок нет, и CREATE TABLE IF NOT EXISTS их не добавит.
        have = [r[1] for r in conn.execute("PRAGMA table_info(pg_groups)")]
        if "pay_mode" not in have:
            conn.execute("ALTER TABLE pg_groups ADD COLUMN pay_mode "
                         "TEXT NOT NULL DEFAULT ''")
        if "pay_amount" not in have:
            conn.execute("ALTER TABLE pg_groups ADD COLUMN pay_amount "
                         "INTEGER NOT NULL DEFAULT 0")
        mem = [r[1] for r in conn.execute("PRAGMA table_info(pg_members)")]
        if "pay_amount" not in mem:
            conn.execute("ALTER TABLE pg_members ADD COLUMN pay_amount "
                         "INTEGER NOT NULL DEFAULT 0")
        # Взнос за тренировки — отдельно от оплаты в лиге: тренируются не по
        # лигам, и группа может задавать его, ни к какой лиге не привязываясь.
        if "train_amount" not in have:
            conn.execute("ALTER TABLE pg_groups ADD COLUMN train_amount "
                         "INTEGER NOT NULL DEFAULT 0")
        if "train_amount" not in mem:
            conn.execute("ALTER TABLE pg_members ADD COLUMN train_amount "
                         "INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    _ready = True


# ─────────────────────────── оплата в лиге ───────────────────────────
#
# Группа, привязанная к лиге, решает, как в этой лиге платят её люди:
#
# * «league» — взносом за лигу целиком, один раз. Живёт как сбор за турнир
#   (season_fees), а состав сбора — это и есть группа: отдельный список вести
#   незачем, он разъехался бы с группой при первом же изменении;
# * «game» — за каждую игру этой лиги.
#
# Сумма задаётся на всю группу разом, а кому-то можно поставить свою —
# скидка, договорённость. Своя сумма живёт у человека в группе и не съезжает,
# когда меняется общая.

PAY_LEAGUE, PAY_GAME = "league", "game"
PAY_TITLES = {PAY_LEAGUE: "за лигу целиком", PAY_GAME: "за игру", "": "не задано"}


def set_payment(gid: int, mode: str, amount: Optional[int] = None) -> None:
    """Как платит группа в своей лиге. amount None — оставить прежнюю сумму."""
    init()
    if mode not in PAY_TITLES:
        raise ValueError(f"неизвестный способ оплаты: {mode}")
    with sheets_cache.get_connection() as conn:
        if amount is None:
            conn.execute("UPDATE pg_groups SET pay_mode = ? WHERE id = ?",
                         (mode, int(gid)))
        else:
            conn.execute("UPDATE pg_groups SET pay_mode = ?, pay_amount = ? "
                         "WHERE id = ?", (mode, max(0, int(amount)), int(gid)))
        conn.commit()


def set_member_amount(gid: int, player_row: int, amount: int) -> None:
    """Своя сумма человека в группе. 0 — вернуть общую."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE pg_members SET pay_amount = ? "
                     "WHERE group_id = ? AND player_row = ?",
                     (max(0, int(amount)), int(gid), int(player_row)))
        conn.commit()


def member_amounts(gid: int) -> Dict[int, int]:
    """{строка: своя сумма} — только у тех, кому она задана."""
    init()
    with sheets_cache.get_connection() as conn:
        return {int(r["player_row"]): int(r["pay_amount"]) for r in conn.execute(
            "SELECT player_row, pay_amount FROM pg_members "
            "WHERE group_id = ? AND pay_amount > 0", (int(gid),))}


def amount_for(gid: int, player_row: int) -> int:
    """Сколько платит этот человек в этой группе: своя сумма или общая."""
    g = group(gid) or {}
    own = member_amounts(gid).get(int(player_row), 0)
    return own or int(g.get("pay_amount") or 0)


def game_price_for(player_row: int, source: str) -> Optional[int]:
    """Цена игры в лиге `source` по правилам группы. None — правил нет.

    None и ноль — разное. None: группа за эту лигу ничего не решала, и цена
    берётся у самого человека, как раньше. Ноль: группа платит иначе (за лигу
    целиком) или сумма не задана — с игры не берём.

    Если человек в двух группах одной лиги, решает первая по порядку заведения.
    На практике так не бывает, но и падать из-за этого нельзя."""
    init()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT g.id, g.pay_mode, g.pay_amount, m.pay_amount AS own
                 FROM pg_groups g JOIN pg_members m ON m.group_id = g.id
                WHERE g.league_source = ? AND m.player_row = ?
                  AND g.pay_mode != ''
                ORDER BY g.id""", (str(source), int(player_row))).fetchall()
    for r in rows:
        if r["pay_mode"] == PAY_GAME:
            return int(r["own"] or 0) or int(r["pay_amount"] or 0)
        if r["pay_mode"] == PAY_LEAGUE:
            return 0          # платит за лигу целиком — с каждой игры не берём
    return None


def _now() -> str:
    return sheets_cache.now_iso()


def clean_name(raw: str) -> str:
    """Имя группы как его ввёл человек, без краёв и переносов."""
    return " ".join(str(raw or "").split())[:NAME_LIMIT]


def _same_name(a: str, b: str) -> bool:
    """Сравнение имён без учёта регистра.

    Именно в питоне: SQLite приводит регистр только для латиницы, и «Основа»
    с «основа» для неё разные строки — проверка уникальности на COLLATE
    NOCASE молча пропустила бы дубль."""
    return a.strip().lower() == b.strip().lower()


# ─────────────────────────── группы ───────────────────────────


def groups() -> List[Dict[str, Any]]:
    """Все группы: имя, лига, сколько человек внутри."""
    init()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM pg_groups ORDER BY name")]
        sizes = {int(r["group_id"]): int(r["n"]) for r in conn.execute(
            "SELECT group_id, COUNT(*) AS n FROM pg_members GROUP BY group_id")}
    for g in rows:
        g["size"] = sizes.get(int(g["id"]), 0)
        g["league_title"] = league_title(g["league_source"], g["league_team"])
    return rows


def group(gid: int) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM pg_groups WHERE id = ?",
                           (int(gid),)).fetchone()
        if not row:
            return None
        got = dict(row)
        got["size"] = int(conn.execute(
            "SELECT COUNT(*) FROM pg_members WHERE group_id = ?",
            (int(gid),)).fetchone()[0])
    got["league_title"] = league_title(got["league_source"], got["league_team"])
    return got


def create(name: str) -> Tuple[Optional[int], str]:
    """Заводит группу. (id, объяснение). id пуст — не завелась."""
    init()
    title = clean_name(name)
    if not title:
        return None, "Пустое имя. Напишите, как назвать группу."
    for g in groups():
        if _same_name(g["name"], title):
            return None, f"Группа «{g['name']}» уже есть."
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO pg_groups (name, created_at) VALUES (?, ?)",
            (title, _now()))
        conn.commit()
        return int(cur.lastrowid), f"Группа «{title}» создана."


def rename(gid: int, name: str) -> Tuple[bool, str]:
    init()
    title = clean_name(name)
    if not title:
        return False, "Пустое имя."
    for g in groups():
        if int(g["id"]) != int(gid) and _same_name(g["name"], title):
            return False, f"Группа «{g['name']}» уже есть."
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE pg_groups SET name = ? WHERE id = ?",
                     (title, int(gid)))
        conn.commit()
    return True, f"Теперь это «{title}»."


def delete(gid: int) -> None:
    """Удаляет группу вместе с составом и её повторяющимися письмами.

    Оставлять их сиротами нельзя: письмо продолжило бы уходить в никуда, а
    тренер уже не нашёл бы, где это выключить."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM pg_members WHERE group_id = ?", (int(gid),))
        conn.execute("DELETE FROM pg_repeats WHERE group_id = ?", (int(gid),))
        conn.execute("DELETE FROM pg_groups WHERE id = ?", (int(gid),))
        conn.commit()


# ─────────────────────────── лига ───────────────────────────


def leagues(include_closed: bool = False) -> List[Dict[str, str]]:
    """Лиги, в которых играют наши команды. Больше выбирать не из чего.

    Закрытую лигу (сезон доигран) не предлагаем — привязывать к ней группу
    незачем. Но в подписи уже привязанной она обязана находиться, иначе
    привязка выглядит потерянной: за этим include_closed."""
    init()
    sql = ["SELECT source, team_id, name, league, closed_at FROM league_teams",
           "WHERE ours = 1"]
    if not include_closed:
        sql.append("AND COALESCE(closed_at, '') = ''")
    sql.append("ORDER BY league")
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(" ".join(sql))]
    out = []
    for r in rows:
        label = (r.get("league") or "").strip() or (r.get("name") or "").strip()
        if str(r.get("closed_at") or ""):
            label += " · закрыта"
        out.append({"source": r["source"], "team_id": str(r["team_id"]),
                    "title": label, "team": (r.get("name") or "").strip()})
    return out


def league_title(source: str, team_id: str) -> str:
    """Подпись привязки. Пусто — группа ни к какой лиге не привязана."""
    if not source or not team_id:
        return ""
    for item in leagues(include_closed=True):
        if item["source"] == source and item["team_id"] == str(team_id):
            return item["title"]
    # Лигу могли перезалить: показываем хоть что-то, а не пустоту.
    return f"{source} · {team_id}"


def bind(gid: int, source: str, team_id: str) -> None:
    """Привязывает группу к лиге. Пустой source — отвязывает."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "UPDATE pg_groups SET league_source = ?, league_team = ? WHERE id = ?",
            (str(source or ""), str(team_id or ""), int(gid)))
        conn.commit()


# ─────────────────────────── состав ───────────────────────────


def member_rows(gid: int) -> List[int]:
    init()
    with sheets_cache.get_connection() as conn:
        return [int(r["player_row"]) for r in conn.execute(
            "SELECT player_row FROM pg_members WHERE group_id = ?", (int(gid),))]


def members(gid: int) -> List[Dict[str, Any]]:
    """Состав группы с подписями. Порядок — как в списке игроков.

    Ушедших из команды здесь нет: им не нужны ни рассылки, ни заявка, ни
    взнос. Членство при этом НЕ стираем — вернётся человек в команду, и
    вернётся во все свои группы сам, без повторной расстановки."""
    import coach_payments
    inside = set(member_rows(gid))
    return [p for p in coach_payments.players()
            if int(p["row"]) in inside and not p.get("gone")]


def add(gid: int, player_row: int) -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO pg_members (group_id, player_row, added_at) "
            "VALUES (?, ?, ?)", (int(gid), int(player_row), _now()))
        conn.commit()


def remove(gid: int, player_row: int) -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "DELETE FROM pg_members WHERE group_id = ? AND player_row = ?",
            (int(gid), int(player_row)))
        conn.commit()


def toggle(gid: int, player_row: int) -> bool:
    """Переключает участие. True — теперь в группе."""
    if int(player_row) in set(member_rows(gid)):
        remove(gid, player_row)
        return False
    add(gid, player_row)
    return True


def groups_of(player_row: int) -> List[str]:
    """В каких группах человек. Нужно, чтобы показывать это в его карточке."""
    init()
    with sheets_cache.get_connection() as conn:
        return [str(r["name"]) for r in conn.execute(
            """SELECT g.name FROM pg_members m JOIN pg_groups g ON g.id = m.group_id
                WHERE m.player_row = ? ORDER BY g.name""", (int(player_row),))]


def targets(gid: int) -> Tuple[List[Tuple[int, str]], List[str]]:
    """Кому реально уйдёт письмо: [(chat_id, подпись)] и список молчунов.

    Молчуны — те, кто бота не запускал. Их возвращаем отдельно, чтобы тренер
    видел, до кого рассылка не дошла, а не считал, что написали всем."""
    import training_dues
    ready: List[Tuple[int, str]] = []
    silent: List[str] = []
    for p in members(gid):
        uid = training_dues.chat_id_of(int(p["row"]))
        if uid:
            try:
                ready.append((int(uid), p["title"]))
                continue
            except (TypeError, ValueError):
                pass
        silent.append(p["title"])
    return ready, silent


# ─────────────────────────── шаблоны писем ───────────────────────────


def templates() -> List[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM pg_templates ORDER BY name")]


def template(tid: int) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM pg_templates WHERE id = ?",
                           (int(tid),)).fetchone()
    return dict(row) if row else None


def template_save(name: str, body: str, tid: int = 0) -> Tuple[Optional[int], str]:
    """Сохраняет шаблон. tid > 0 — переписывает существующий."""
    init()
    title = clean_name(name)
    text = str(body or "").strip()
    if not title:
        return None, "У шаблона должно быть имя."
    if not text:
        return None, "Пустой текст сохранять незачем."
    for t in templates():
        if int(t["id"]) != int(tid) and _same_name(t["name"], title):
            return None, f"Шаблон «{t['name']}» уже есть."
    with sheets_cache.get_connection() as conn:
        if tid:
            conn.execute("UPDATE pg_templates SET name = ?, body = ? WHERE id = ?",
                         (title, text, int(tid)))
            conn.commit()
            return int(tid), f"Шаблон «{title}» переписан."
        cur = conn.execute(
            "INSERT INTO pg_templates (name, body, created_at) VALUES (?, ?, ?)",
            (title, text, _now()))
        conn.commit()
        return int(cur.lastrowid), f"Шаблон «{title}» сохранён."


def template_delete(tid: int) -> None:
    """Удаляет шаблон. Повторы на нём переводим на их же текст, чтобы
    расписание не осталось с пустым письмом."""
    init()
    got = template(tid)
    body = (got or {}).get("body", "")
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "UPDATE pg_repeats SET template_id = 0, body = ? "
            "WHERE template_id = ? AND body = ''", (body, int(tid)))
        conn.execute("DELETE FROM pg_templates WHERE id = ?", (int(tid),))
        conn.commit()


# ─────────────────────────── повторы ───────────────────────────


def repeats(gid: int = 0) -> List[Dict[str, Any]]:
    init()
    sql = ("SELECT r.*, g.name AS group_name FROM pg_repeats r "
           "JOIN pg_groups g ON g.id = r.group_id")
    args: Tuple[Any, ...] = ()
    if gid:
        sql += " WHERE r.group_id = ?"
        args = (int(gid),)
    sql += " ORDER BY r.weekday, r.at_time"
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(sql, args)]
    for r in rows:
        r["body_text"] = body_of(r)
        r["when"] = f"{DAYS[int(r['weekday']) % 7]}, {r['at_time']}"
    return rows


def body_of(rep: Dict[str, Any]) -> str:
    """Текст повтора: свой либо из шаблона."""
    if rep.get("template_id"):
        got = template(int(rep["template_id"]))
        if got:
            return str(got["body"])
    return str(rep.get("body") or "")


def repeat_add(gid: int, weekday: int, at_time: str,
               template_id: int = 0, body: str = "") -> int:
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO pg_repeats (group_id, template_id, body, weekday, "
            "at_time, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (int(gid), int(template_id), str(body or ""), int(weekday) % 7,
             str(at_time), _now()))
        conn.commit()
        return int(cur.lastrowid)


def repeat_delete(rid: int) -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM pg_repeats WHERE id = ?", (int(rid),))
        conn.commit()


def repeat_switch(rid: int) -> bool:
    """Включает/выключает повтор. True — теперь включён."""
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT active FROM pg_repeats WHERE id = ?",
                           (int(rid),)).fetchone()
        if not row:
            return False
        now = 0 if int(row["active"]) else 1
        conn.execute("UPDATE pg_repeats SET active = ? WHERE id = ?",
                     (now, int(rid)))
        conn.commit()
    return bool(now)


def parse_time(raw: str) -> Optional[str]:
    """«19:30», «19.30», «1930» → «19:30». Не разобрали — None."""
    got = str(raw or "").strip().replace(".", ":").replace(" ", "")
    if got.isdigit() and len(got) == 4:
        got = got[:2] + ":" + got[2:]
    parts = got.split(":")
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        return None
    hour, minute = int(parts[0]), int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return f"{hour:02d}:{minute:02d}"


def due(now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Повторы, которым пора уйти прямо сейчас.

    Время московское — как и всё остальное расписание бота. Отправленное
    сегодня повторно не уходит: отметка стоит по дате, а не по часу, поэтому
    перезапуск демона в тот же день письмо не задвоит.

    Опоздание допускаем в пределах часа: демон мог не работать в нужную
    минуту, и пропускать из-за этого недельное письмо было бы обидно."""
    from datetime_utils import get_moscow_time
    init()
    moment = now or get_moscow_time()
    today = moment.date().isoformat()
    out = []
    for rep in repeats():
        if not int(rep.get("active") or 0):
            continue
        if int(rep["weekday"]) % 7 != moment.weekday():
            continue
        if str(rep.get("last_sent") or "") == today:
            continue
        planned = parse_time(rep["at_time"])
        if not planned:
            continue
        hour, minute = (int(x) for x in planned.split(":"))
        minutes_now = moment.hour * 60 + moment.minute
        minutes_plan = hour * 60 + minute
        if 0 <= minutes_now - minutes_plan <= 60:
            out.append(rep)
    return out


def mark_sent(rid: int, when: Optional[date] = None) -> None:
    init()
    day = (when or date.today()).isoformat()
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE pg_repeats SET last_sent = ? WHERE id = ?",
                     (day, int(rid)))
        conn.commit()


# ─────────────────────────── взнос за тренировки ───────────────────────────
#
# Второй состав тренируется реже, у кубковой группы свой зал — и платят они
# за тренировки иначе, чем основа. Группа задаёт свою сумму на всех, а кому-то
# можно поставить личную. Ноль у группы — «группа за тренировки не решает»,
# и сумма берётся из карточки игрока, как всегда.


def set_train_amount(gid: int, amount: int) -> None:
    """Взнос за тренировки для всей группы. 0 — группа его не задаёт."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE pg_groups SET train_amount = ? WHERE id = ?",
                     (max(0, int(amount)), int(gid)))
        conn.commit()


def set_member_train(gid: int, player_row: int, amount: int) -> None:
    """Личный взнос за тренировки в группе. 0 — вернуть сумму группы."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE pg_members SET train_amount = ? "
                     "WHERE group_id = ? AND player_row = ?",
                     (max(0, int(amount)), int(gid), int(player_row)))
        conn.commit()


def member_trains(gid: int) -> Dict[int, int]:
    """{строка: личный взнос за тренировки} — только у тех, кому он задан."""
    init()
    with sheets_cache.get_connection() as conn:
        return {int(r["player_row"]): int(r["train_amount"]) for r in conn.execute(
            "SELECT player_row, train_amount FROM pg_members "
            "WHERE group_id = ? AND train_amount > 0", (int(gid),))}


def train_price_for(player_row: int) -> Optional[int]:
    """Взнос за тренировки по правилам группы. None — ни одна группа не решает.

    Личная сумма главнее групповой. Если человек в двух группах со своими
    взносами, решает первая по порядку заведения — падать из-за этого нельзя.
    Ноль вернуть отсюда нельзя по построению: группа с нулём не решает."""
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            """SELECT g.train_amount AS base, m.train_amount AS own
                 FROM pg_groups g JOIN pg_members m ON m.group_id = g.id
                WHERE m.player_row = ? AND (g.train_amount > 0 OR m.train_amount > 0)
                ORDER BY g.id LIMIT 1""", (int(player_row),)).fetchone()
    if not row:
        return None
    return int(row["own"] or 0) or int(row["base"] or 0) or None
