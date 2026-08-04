"""Состав на игру: кого тренер заявил и кто из них не заплатил за игру.

За три дня до матча бот приносит тренеру в личку тех, кто отметился в опросе
«✅ Готов», даёт дописать остальных по фамилии и отправить состав в чат.
Заявленный состав — это и есть список тех, с кого ждём оплату игры: не
проголосовавшие и не вся команда, а именно те, кто поехал играть.

Оплату разносим по играм по порядку: перевод «за 2 игры» закрывает две
ближайшие неоплаченные игры этого человека. Иначе пришлось бы требовать от
тренера указывать, за какую именно игру пришли деньги, — а в СМС этого нет.

Тексты и расчёты здесь; отправляет всё bot_daemon. Тренерское — только в
личку ([[coach-messages-private-only]]), в общий чат уходит ровно одно
сообщение: сам состав, по кнопке тренера.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import coach_payments
import sheets_cache

logger = logging.getLogger(__name__)

# За сколько дней до игры просим тренера собрать состав.
COLLECT_BEFORE_DAYS = 3

# С какой игры действует цикл оплаты игр. Всё, что раньше, — до появления
# порядка: команда о нём не знала, состав в чат не объявлялся, и требовать
# деньги задним числом нельзя. 03.08.2026 из-за этого семи игрокам ушло
# «оплати игру» за матч 02.08 — состав тренер собрал, пробуя новый экран.
PAY_SINCE = "2026-08-04"

POLL_TYPES = ("ОПРОС_ИГРА", "ОПРОС_ИГРА_SLPRO")
VOTE_READY = "PRESENT"


def source_of(game_id: str) -> str:
    return "slpro" if str(game_id).startswith("slpro-") else "infobasket"


def _parse_date(value: str) -> Optional[date]:
    """Даты в записях лежат и как 09.08.2026, и как 2026-08-09."""
    raw = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw[:10], fmt).date()
        except ValueError:
            continue
    return None


def games(from_day: Optional[date] = None,
          until_day: Optional[date] = None) -> List[Dict[str, Any]]:
    """Игры, на которые бот заводил опрос, в окне дат. Свежие — первыми."""
    sheets_cache.init_db()
    marks = ",".join("?" * len(POLL_TYPES))
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            f"""SELECT game_id, game_date, game_time, alt_name, additional_data
                FROM service_records WHERE data_type IN ({marks})
                  AND game_id != '' AND deleted = 0""", POLL_TYPES).fetchall()
    out = []
    for r in rows:
        day = _parse_date(r["game_date"])
        if not day:
            continue
        if from_day and day < from_day:
            continue
        if until_day and day > until_day:
            continue
        out.append({
            "game_id": str(r["game_id"]),
            "source": source_of(r["game_id"]),
            "date": day,
            "time": str(r["game_time"] or ""),
            "opponent": _opponent_from(str(r["additional_data"] or "")),
            "title": str(r["alt_name"] or ""),
        })
    out.sort(key=lambda g: g["date"])
    return out


def _opponent_from(text: str) -> str:
    """Соперника достаём из текста опроса: «🏀 Мы против Соперник»."""
    for line in text.splitlines():
        if " против " in line:
            return line.split(" против ", 1)[1].strip()
    return ""


def game_label(game: Dict[str, Any]) -> str:
    opp = game.get("opponent") or "соперник"
    when = game["date"].strftime("%d.%m")
    time = f", {game['time']}" if game.get("time") else ""
    return f"{opp} · {when}{time}"


def voters(game_id: str, vote_type: str = VOTE_READY) -> List[Dict[str, Any]]:
    """Кто отметился в опросе. Заодно ищем, чья это строка в листе «Игроки».

    Не опознали (человек не привязан к строке) — всё равно показываем: тренеру
    важно видеть, кто вызвался, а привязать можно потом."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT user_id, username, first_name, last_name FROM game_votes
               WHERE game_id = ? AND vote_type = ?""",
            (str(game_id), vote_type)).fetchall()
    out = []
    for r in rows:
        link = sheets_cache.get_player_link(str(r["user_id"]))
        row_index = int((link or {}).get("player_row") or 0)
        player = coach_payments.player_by_row(row_index) if row_index else None
        name = (player or {}).get("title") or " ".join(
            x for x in (str(r["last_name"] or ""), str(r["first_name"] or "")) if x)
        out.append({"user_id": str(r["user_id"]), "row": row_index,
                    "title": name or f"@{r['username']}" or str(r["user_id"]),
                    "linked": bool(row_index)})
    out.sort(key=lambda x: x["title"])
    return out


# ─────────────────────────── Состав ────────────────────────────────────────

def roster(source: str, game_id: str) -> List[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT player_row FROM game_rosters WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchall()
    out = []
    for r in rows:
        player = coach_payments.player_by_row(int(r["player_row"]))
        if player:
            out.append(player)
    out.sort(key=lambda p: p["title"])
    return out


def add(source: str, game_id: str, player_row: int, by: str = "") -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO game_rosters
               (source, game_id, player_row, added_by, added_at)
               VALUES (?, ?, ?, ?, ?)""",
            (source, str(game_id), int(player_row), str(by),
             datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    return True


def remove(source: str, game_id: str, player_row: int) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "DELETE FROM game_rosters WHERE source = ? AND game_id = ? AND player_row = ?",
            (source, str(game_id), int(player_row)))
        conn.commit()
        return cur.rowcount > 0


def ensure_state(game: Dict[str, Any]) -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO game_roster_state
               (source, game_id, game_date, opponent, posted_at)
               VALUES (?, ?, ?, ?, '')""",
            (game["source"], str(game["game_id"]),
             game["date"].isoformat(), game.get("opponent", "")))
        conn.commit()


def is_posted(source: str, game_id: str) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT posted_at FROM game_roster_state WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return bool(row and str(row["posted_at"] or ""))


def mark_posted(source: str, game_id: str) -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_roster_state (source, game_id, posted_at)
               VALUES (?, ?, ?)
               ON CONFLICT(source, game_id) DO UPDATE SET posted_at = excluded.posted_at""",
            (source, str(game_id), datetime.now().isoformat(timespec="seconds")))
        conn.commit()


def search(query: str, limit: int = 8) -> List[Dict[str, Any]]:
    """Игроки по части фамилии или имени — общим поиском бота.

    Отдаём полные карточки (с суммами оплат), а не голые строки: состав потом
    идёт в расчёт долгов, и цена игры у людей разная."""
    import player_search
    by_row = {p["row"]: p for p in coach_payments.players()}
    out = []
    for hit in player_search.find(query, limit=limit):
        card = by_row.get(hit["row"])
        if card:
            out.append(card)
    return out


def post_text(game: Dict[str, Any], people: List[Dict[str, Any]]) -> str:
    """Сообщение в общий чат — единственное, что уходит не в личку."""
    head = f"🏀 Состав на игру: {game_label(game)}"
    if not people:
        return head + "\n\nСостав пока не собран."
    lines = [head, ""]
    for i, p in enumerate(people, start=1):
        lines.append(f"{i}. {p['title']}")
    lines += ["", f"Всего: {len(people)}."]
    return "\n".join(lines)


# ─────────────────────── Оплата игр по составу ─────────────────────────────

def _paid_games(player_row: int) -> int:
    """Сколько игр человек оплатил всего (переводы «за N игр» + отметки)."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT COALESCE(SUM(games), 0) FROM payments "
            "WHERE player_row = ? AND kind = ?",
            (int(player_row), coach_payments.KIND_GAME)).fetchone()
    return int(row[0] or 0)


def _played_games(player_row: int) -> List[Tuple[str, str, str]]:
    """Игры, где человек был в составе, от старых к новым."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT r.source, r.game_id, COALESCE(s.game_date, '') AS game_date
               FROM game_rosters r
               LEFT JOIN game_roster_state s
                      ON s.source = r.source AND s.game_id = r.game_id
               WHERE r.player_row = ?""", (int(player_row),)).fetchall()
    out = [(str(r["source"]), str(r["game_id"]), str(r["game_date"])) for r in rows]
    out.sort(key=lambda x: x[2] or "9999")
    return out


def owes_for(source: str, game_id: str, player_row: int) -> bool:
    """Не закрыта ли ЭТА игра. Оплаты ложатся на игры по порядку: заплатил за
    две — закрыты две самые ранние из тех, где он играл."""
    played = _played_games(player_row)
    covered = _paid_games(player_row)
    for i, (src, gid, _) in enumerate(played):
        if src == source and str(gid) == str(game_id):
            return i >= covered
    return False


def debtors(source: str, game_id: str) -> List[Dict[str, Any]]:
    """Кто из состава не оплатил эту игру."""
    out = []
    for p in roster(source, game_id):
        if owes_for(source, game_id, p["row"]):
            out.append(p)
    return out


def mark_paid(player_row: int, source: str, game_id: str, by: str = "") -> Dict[str, Any]:
    """Тренер отметил оплату игры без СМС."""
    player = coach_payments.player_by_row(player_row)
    price = coach_payments.game_price(player)
    return coach_payments.record(
        player_row, price, coach_payments.KIND_GAME, 1,
        paid_at=date.today().isoformat(), bank="", note="отметил тренер",
        added_by=str(by), fp="", game_ref=f"{source}:{game_id}", by_coach=True)


def coach_debt_text(game: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
    label = game_label(game)
    if not rows:
        return f"✅ Игра {label}: за игру рассчитались все."
    price = coach_payments.game_price()
    lines = [f"💰 Игра {label}. Не оплатили ({len(rows)}):", ""]
    for p in rows:
        lines.append(f"• {p['title']} — {coach_payments.game_price(p) or price} ₽")
    lines += ["", "Кнопкой ниже отметь тех, кто отдал деньги без чека."]
    return "\n".join(lines)


def player_debt_text(game: Dict[str, Any], player: Dict[str, Any]) -> str:
    return (f"💰 Оплата игры: {game_label(game)} — "
            f"{coach_payments.game_price(player)} ₽.\n\n"
            "Переведи, пожалуйста, тренеру и скинь ему чек — он отметит. "
            "Если уже оплатил, ничего делать не надо.")


# ─────────────────────── Что пора сделать сейчас ───────────────────────────

def due_events(now: Optional[datetime] = None) -> List[Tuple[str, Dict[str, Any], str]]:
    """[(ключ, игра, вид)] — что должно сработать в этот момент по Москве.

    Виды: collect (собрать состав за 3 дня), coach_day (в день игры утром),
    coach_next и player_next (на следующий день). Ключ уникален на игру и
    вид — по нему bot_daemon помнит, что уже отправлял."""
    from datetime_utils import get_moscow_time
    now = now or get_moscow_time()
    today = now.date()
    out: List[Tuple[str, Dict[str, Any], str]] = []

    for game in games(from_day=today - timedelta(days=2),
                      until_day=today + timedelta(days=COLLECT_BEFORE_DAYS)):
        ref = f"{game['source']}:{game['game_id']}"
        left = (game["date"] - today).days
        # Не «ровно за три дня», а «как только до игры осталось три дня или
        # меньше»: игру в лиге могут открыть и за два дня до неё, и тогда
        # точное совпадение просто не сработало бы. Событие помечается
        # выполненным, поэтому запрос уходит один раз.
        if 0 <= left <= COLLECT_BEFORE_DAYS:
            out.append((f"game:{ref}:collect", game, "collect"))

        # Дальше — только деньги, и тут два условия. Игра не старше порядка
        # оплат, и состав РАЗОСЛАН в чат: пока команда его не видела, никто
        # ничего не должен. Собранный, но не отправленный состав — это
        # черновик тренера, а не основание для требования.
        if game["date"].isoformat() < PAY_SINCE:
            continue
        if not is_posted(game["source"], game["game_id"]):
            continue
        if left == 0 and now.hour >= 9:
            out.append((f"game:{ref}:coach_day", game, "coach_day"))
        elif left == -1 and now.hour >= 9:
            out.append((f"game:{ref}:coach_next", game, "coach_next"))
            if now.hour >= 19:
                out.append((f"game:{ref}:player_next", game, "player_next"))
    return out


def silence_old(mark) -> int:
    """Гасит платёжные события по играм, которые были до PAY_SINCE.

    Правило выше их и так не выдаст, но отметка в базе нужна: если окно дат
    когда-нибудь сдвинется, старая игра не должна ожить и снова разослать
    людям требование оплаты. mark — функция (ключ, пояснение)."""
    done = 0
    for game in games(until_day=date.fromisoformat(PAY_SINCE) - timedelta(days=1)):
        ref = f"{game['source']}:{game['game_id']}"
        for kind in ("coach_day", "coach_next", "player_next"):
            mark(f"game:{ref}:{kind}", "старая игра, цикл оплат не применялся")
            done += 1
    return done
