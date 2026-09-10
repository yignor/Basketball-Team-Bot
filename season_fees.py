#!/usr/bin/env python3
"""Взнос за турнир целиком: одна сумма вперёд, а не по играм.

Зачем. В одной лиге команда платит за сезон сразу, в другой — за каждую игру.
Прежние два вида оплаты этого не покрывали: «сезон» в боте всегда означал
ЕЖЕМЕСЯЧНЫЙ взнос за тренировки, а «игра» — разовую сумму за матч. Турнирный
взнос — третий вид, и он живёт рядом с ними, ничего не отменяя: человек может
одновременно платить за тренировки помесячно, за игры второй лиги поштучно и
за первую лигу — одним платежом вперёд.

**Цена одна на турнир, но с исключениями.** У сбора есть базовая сумма; кому-то
можно поставить свою — скидка, доплата, договорённость. Исключение хранится у
человека, а не подменяет базу: поменяется цена турнира — остальных это
подвинет, а договорённость останется.

**Состав собирает тренер руками.** Автоматики тут быть не может: кто едет на
турнир, знает только он, и списком «все активные» это не описывается — часть
команды играет лишь во второй лиге.

Наружу отсюда ничего не уходит: тексты собираются здесь, а рассылает bot_daemon
и только в личку ([[coach-messages-private-only]]).
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# Вид платежа в общей таблице. «season» уже занят ежемесячными взносами за
# тренировки — брать его же значило бы смешать месячные деньги с турнирными.
KIND = "tourney"

SCHEMA = """
CREATE TABLE IF NOT EXISTS season_fees (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    title      TEXT NOT NULL,
    amount     INTEGER NOT NULL DEFAULT 0,
    source     TEXT NOT NULL DEFAULT '',
    team_id    TEXT NOT NULL DEFAULT '',
    due_date   TEXT NOT NULL DEFAULT '',
    active     INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS season_fee_members (
    fee_id     INTEGER NOT NULL,
    player_row INTEGER NOT NULL,
    amount     INTEGER NOT NULL DEFAULT 0,   -- 0 = базовая цена турнира
    added_at   TEXT NOT NULL,
    PRIMARY KEY (fee_id, player_row)
);
"""

_ready = False


def init() -> None:
    global _ready
    if _ready:
        return
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.executescript(SCHEMA)
        have = [r[1] for r in conn.execute("PRAGMA table_info(season_fees)")]
        if "group_id" not in have:
            # Сбор, заведённый из группы, берёт состав у группы: отдельный
            # список разъехался бы с ней при первом же изменении.
            conn.execute("ALTER TABLE season_fees ADD COLUMN group_id "
                         "INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    _ready = True


def _now() -> str:
    return sheets_cache.now_iso()


def clean(text: Any) -> str:
    return " ".join(str(text or "").split())


# ─────────────────────────── сборы ───────────────────────────


def all_fees(active_only: bool = False) -> List[Dict[str, Any]]:
    init()
    where = " WHERE active = 1" if active_only else ""
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM season_fees" + where + " ORDER BY id DESC")]
        sizes = {int(r["fee_id"]): int(r["n"]) for r in conn.execute(
            "SELECT fee_id, COUNT(*) AS n FROM season_fee_members GROUP BY fee_id")}
    for f in rows:
        f["size"] = sizes.get(int(f["id"]), 0)
        f["league"] = league_title(f["source"], f["team_id"])
    return rows


def fee(fee_id: int) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM season_fees WHERE id = ?",
                           (int(fee_id),)).fetchone()
    if not row:
        return None
    got = dict(row)
    got["size"] = len(member_rows(fee_id))
    got["league"] = league_title(got["source"], got["team_id"])
    return got


def league_title(source: str, team_id: str) -> str:
    """Подпись лиги. Пусто — сбор ни к какой лиге не привязан."""
    if not source or not team_id:
        return ""
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT league, name FROM league_teams WHERE source = ? AND team_id = ?",
            (str(source), str(team_id))).fetchone()
    if not row:
        return f"{source} · {team_id}"
    return str(row["league"] or row["name"] or "").strip()


def leagues() -> List[Dict[str, str]]:
    """Лиги наших команд — к ним можно привязать сбор."""
    init()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT source, team_id, name, league FROM league_teams "
            "WHERE ours = 1 ORDER BY league")]
    return [{"source": r["source"], "team_id": str(r["team_id"]),
             "title": (r.get("league") or r.get("name") or "").strip()}
            for r in rows]


def create(title: str, amount: int = 0) -> Tuple[Optional[int], str]:
    init()
    name = clean(title)
    if not name:
        return None, "У сбора должно быть название."
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO season_fees (title, amount, created_at) VALUES (?, ?, ?)",
            (name, max(0, int(amount or 0)), _now()))
        conn.commit()
        return int(cur.lastrowid), f"Сбор «{name}» заведён."


def update(fee_id: int, **fields: Any) -> None:
    init()
    allowed = {"title", "amount", "source", "team_id", "due_date", "active"}
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets:
        return
    names = ", ".join(f"{k} = ?" for k in sets)
    with sheets_cache.get_connection() as conn:
        conn.execute(f"UPDATE season_fees SET {names} WHERE id = ?",
                     (*sets.values(), int(fee_id)))
        conn.commit()


def delete(fee_id: int) -> None:
    """Удаляет сбор и его состав. Внесённые платежи НЕ трогаем: деньги были,
    и стирать их вместе с настройкой нельзя."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM season_fee_members WHERE fee_id = ?", (int(fee_id),))
        conn.execute("DELETE FROM season_fees WHERE id = ?", (int(fee_id),))
        conn.commit()


# ─────────────────────────── состав ───────────────────────────


def _group_of(fee_id: int) -> int:
    """Из какой группы сбор берёт состав. 0 — ведёт свой список."""
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT group_id FROM season_fees WHERE id = ?",
                           (int(fee_id),)).fetchone()
    return int(row["group_id"] or 0) if row else 0


def member_rows(fee_id: int) -> List[int]:
    gid = _group_of(fee_id)
    if gid:
        import player_groups
        return player_groups.member_rows(gid)
    init()
    with sheets_cache.get_connection() as conn:
        return [int(r["player_row"]) for r in conn.execute(
            "SELECT player_row FROM season_fee_members WHERE fee_id = ?",
            (int(fee_id),))]


def fee_for_group(gid: int) -> int:
    """Сбор за лигу этой группы: находит или заводит. Возвращает id.

    Сумма и название следуют за группой: поменял общую сумму в группе — она
    же и в сборе. Отдельно их не ведём, иначе было бы два места, где правят
    одно и то же."""
    import player_groups
    init()
    g = player_groups.group(gid) or {}
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT id FROM season_fees WHERE group_id = ?",
                           (int(gid),)).fetchone()
        title = f"{g.get('name', 'Группа')} · {g.get('league_title') or 'лига'}"
        if row:
            conn.execute(
                "UPDATE season_fees SET title = ?, amount = ?, source = ?, "
                "team_id = ? WHERE id = ?",
                (title, int(g.get("pay_amount") or 0), g.get("league_source", ""),
                 g.get("league_team", ""), int(row["id"])))
            conn.commit()
            return int(row["id"])
        cur = conn.execute(
            "INSERT INTO season_fees (title, amount, source, team_id, group_id, "
            "created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (title, int(g.get("pay_amount") or 0), g.get("league_source", ""),
             g.get("league_team", ""), int(gid), _now()))
        conn.commit()
        return int(cur.lastrowid)


def toggle(fee_id: int, player_row: int) -> bool:
    """Переключает участие. True — теперь платит.

    Сбор, взятый из группы, правится через группу: иначе правка легла бы в
    список, который для такого сбора не читается, и пропала бы молча."""
    gid = _group_of(fee_id)
    if gid:
        import player_groups
        return player_groups.toggle(gid, player_row)
    init()
    inside = int(player_row) in set(member_rows(fee_id))
    with sheets_cache.get_connection() as conn:
        if inside:
            conn.execute(
                "DELETE FROM season_fee_members WHERE fee_id = ? AND player_row = ?",
                (int(fee_id), int(player_row)))
        else:
            conn.execute(
                "INSERT OR IGNORE INTO season_fee_members (fee_id, player_row, "
                "added_at) VALUES (?, ?, ?)",
                (int(fee_id), int(player_row), _now()))
        conn.commit()
    return not inside


def set_personal(fee_id: int, player_row: int, amount: int) -> None:
    """Своя сумма для человека. 0 — вернуть базовую цену турнира."""
    gid = _group_of(fee_id)
    if gid:
        import player_groups
        player_groups.set_member_amount(gid, player_row, amount)
        return
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "UPDATE season_fee_members SET amount = ? "
            "WHERE fee_id = ? AND player_row = ?",
            (max(0, int(amount or 0)), int(fee_id), int(player_row)))
        conn.commit()


def _paid_map(fee_id: int) -> Dict[int, int]:
    """{строка игрока: сколько внесено по этому сбору}."""
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT player_row, SUM(amount) AS amount FROM payments "
            "WHERE kind = ? AND period = ? GROUP BY player_row",
            (KIND, ref(fee_id))).fetchall()
    return {int(r["player_row"]): int(r["amount"] or 0) for r in rows}


def ref(fee_id: int) -> str:
    """Как сбор помечен в платеже. Отдельным полем не заводим: у платежа уже
    есть «период», и для турнира это он и есть."""
    return f"fee:{int(fee_id)}"


def status(fee_id: int) -> List[Dict[str, Any]]:
    """Состав сбора: сколько с кого ждём, сколько внесено, сколько осталось."""
    import coach_payments
    init()
    base = int((fee(fee_id) or {}).get("amount") or 0)
    gid = _group_of(fee_id)
    if gid:
        # Состав и свои суммы — у группы. Базовую берём оттуда же: сбор мог
        # не успеть подтянуть новую, если её поменяли минуту назад.
        import player_groups
        g = player_groups.group(gid) or {}
        base = int(g.get("pay_amount") or 0)
        mine = player_groups.member_amounts(gid)
        own = {row: mine.get(row, 0) for row in player_groups.member_rows(gid)}
    else:
        with sheets_cache.get_connection() as conn:
            own = {int(r["player_row"]): int(r["amount"] or 0) for r in conn.execute(
                "SELECT player_row, amount FROM season_fee_members WHERE fee_id = ?",
                (int(fee_id),))}
    paid = _paid_map(fee_id)
    out = []
    for p in coach_payments.players():
        row = int(p["row"])
        # Ушедший из команды в сборах не участвует: турнир он играть не будет.
        if row not in own or p.get("gone"):
            continue
        need = own[row] or base
        got = paid.get(row, 0)
        out.append({**p, "need": need, "paid": got,
                    "debt": max(0, need - got), "own": bool(own[row]),
                    "ok": bool(need) and got >= need})
    out.sort(key=lambda x: (x["ok"], x["title"]))
    return out


def debtors(fee_id: int) -> List[Dict[str, Any]]:
    """Кто ещё не закрыл турнирный взнос. Без суммы — не должник: считать
    нечего, и дёргать человека не за что."""
    return [r for r in status(fee_id) if r["need"] and not r["ok"]]


def totals(fee_id: int) -> Dict[str, int]:
    rows = status(fee_id)
    return {"people": len(rows),
            "need": sum(r["need"] for r in rows),
            "paid": sum(r["paid"] for r in rows),
            "debt": sum(r["debt"] for r in rows)}


def mark_paid(fee_id: int, player_row: int, amount: Optional[int] = None,
              by: str = "") -> Dict[str, Any]:
    """Тренер отметил взнос за турнир. Пишем обычным платежом.

    Помечаем by_coach — как и другие отметки без чека: в сводке видно, что
    платёж появился не из СМС, и его можно найти и отменить."""
    import coach_payments
    init()
    rows = {int(r["row"]): r for r in status(fee_id)}
    person = rows.get(int(player_row))
    need = amount if amount is not None else int((person or {}).get("debt") or 0)
    if need <= 0:
        return {"error": "Платить нечего: сумма не задана или взнос уже закрыт."}
    today = date.today().isoformat()
    rec = coach_payments.record(
        int(player_row), int(need), KIND, 0,
        paid_at=today, bank="", note="взнос за турнир",
        added_by=str(by), by_coach=True, period=ref(fee_id),
        # Отпечаток по смыслу, а не по текущей минуте: тычок через минуту
        # иначе заводил бы второй взнос за тот же турнир. Оплату частями это
        # не мешает — другая сумма или другой день дают другой отпечаток.
        fp=coach_payments.fingerprint(
            f"fee|{fee_id}|{player_row}|{need}|{today}"))
    return rec or {}


def reminder(fee_id: int, row: Dict[str, Any]) -> str:
    """Письмо должнику. Сумма и срок — то, ради чего его читают."""
    got = fee(fee_id) or {}
    lines = [f"🏆 {got.get('title', 'Турнир')}", "",
             f"Взнос за турнир: {row['need']} ₽."]
    if row["paid"]:
        lines.append(f"Внесено: {row['paid']} ₽, осталось {row['debt']} ₽.")
    if got.get("due_date"):
        lines.append(f"Срок: до {human_day(got['due_date'])}.")
    lines += ["", "Реквизиты у тренера."]
    return "\n".join(lines)


def human_day(iso: str) -> str:
    try:
        return datetime.strptime(str(iso), "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return str(iso or "")


def parse_day(text: str) -> str:
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
        try:
            return datetime.strptime(clean(text), fmt).date().isoformat()
        except ValueError:
            continue
    return ""
