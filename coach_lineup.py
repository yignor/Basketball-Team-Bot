"""Стартовый состав: кого тренер ставит и на что смотрит, выбирая.

Список фамилий сам по себе решения не подсказывает. Тренеру нужны две вещи
рядом с именем: сколько человек ходил на тренировки в последний месяц (по нему
видно форму и отношение) и на какой позиции он обычно играет. Отсюда три вида
одного списка — по алфавиту, по тренировкам и по амплуа.

Тренировки считаем ОТ ДАТЫ ИГРЫ назад, а не от сегодня: состав на прошлую игру
должен показывать ту картину, что была тогда, иначе разбор задним числом врёт.

Амплуа живёт в листе «Игроки» (столбец «Амплуа»), правится из бота. Своей
таблицы не заводим: тренер и так работает с этим листом, а два места хранения
одного факта неизбежно разъезжаются.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# Амплуа: номера и две общие роли. Порядок — как на площадке, от первого
# номера к пятому; «Разыгрывающий» и «Большой» для тех, кто мыслит словами.
ROLES = ["1", "2", "3", "4", "5", "Разыгрывающий", "Большой"]

# За какой срок до игры считаем тренировки.
WINDOW_DAYS = 30

SORTS = {"name": "по алфавиту", "trainings": "по тренировкам", "role": "по амплуа"}


def _sessions_in(vote_text: str) -> int:
    """Сколько тренировок отмечено в одном голосе.

    В опросе можно выбрать несколько дней сразу, и Telegram склеивает их через
    «+» («Среда, 20:30 + Пятница, 20:30»). Считать такой голос за одну
    тренировку — занижать вдвое тем, кто ходит регулярно."""
    text = str(vote_text or "").strip()
    if not text:
        return 0
    return len([p for p in text.split("+") if p.strip()])


def trainings_count(until: date, days: int = WINDOW_DAYS) -> Dict[int, int]:
    """{строка листа: сколько тренировок} за окно до указанной даты.

    Ключ — строка в листе «Игроки»: голоса приходят по telegram id, а состав
    живёт строками, и связывает их player_links."""
    since = (until - timedelta(days=days)).isoformat()
    sheets_cache.init_db()
    out: Dict[int, int] = {}
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT l.player_row AS row, a.vote_text AS vote
                 FROM attendance a
                 JOIN player_links l ON l.tg_user_id = a.user_id
                WHERE a.vote_type = 'PRESENT'
                  AND a.training_date >= ? AND a.training_date <= ?""",
            (since, until.isoformat())).fetchall()
    for r in rows:
        out[int(r["row"])] = out.get(int(r["row"]), 0) + _sessions_in(r["vote"])
    return out


def lineup(source: str, game_id: str, sort: str = "name") -> Dict[str, Any]:
    """Состав на игру с тренировками и амплуа, в нужном порядке."""
    import coach_payments
    import game_roster

    game = next((g for g in game_roster.games()
                 if g["source"] == source and g["game_id"] == str(game_id)), None)
    day = game["date"] if game else date.today()
    people = game_roster.roster(source, game_id)
    counts = trainings_count(day)
    rows = []
    for p in people:
        rows.append({**p, "trainings": counts.get(int(p["row"]), 0),
                     "role": str(p.get("role") or "")})
    if sort == "trainings":
        # Больше тренировок — выше; при равенстве по алфавиту, иначе порядок
        # прыгает от запроса к запросу и список нельзя сравнить с прошлым.
        rows.sort(key=lambda r: (-r["trainings"], game_roster._by_surname(r)))
    elif sort == "role":
        order = {role: i for i, role in enumerate(ROLES)}
        rows.sort(key=lambda r: (order.get(r["role"], len(ROLES)),
                                 game_roster._by_surname(r)))
    else:
        rows.sort(key=game_roster._by_surname)
    return {"game": game, "rows": rows, "sort": sort, "day": day}


def text(data: Dict[str, Any], title: str = "🏁 Стартовый состав") -> str:
    """Сообщение тренеру. Тренировки — за месяц до игры, а не до сегодня."""
    import game_roster
    game, rows = data.get("game"), data.get("rows") or []
    head = title
    if game:
        head += f"\n{game_roster.game_label(game)}"
    if not rows:
        return head + "\n\nСостав пока не собран."
    lines = [head, f"Тренировки — за {WINDOW_DAYS} дней до игры.", ""]
    last_role = None
    for r in rows:
        if data.get("sort") == "role":
            role = r["role"] or "без амплуа"
            if role != last_role:
                lines.append(f"— {role} —")
                last_role = role
        mark = f" · {r['role']}" if r["role"] and data.get("sort") != "role" else ""
        lines.append(f"• {r['title']} — {r['trainings']} трен.{mark}")
    return "\n".join(lines)


def set_role(player_row: int, role: str, spreadsheet: Any = None) -> bool:
    """Ставит амплуа игроку: в лист «Игроки» и в зеркало.

    Пустое значение снимает амплуа — тренер мог поставить по ошибке."""
    value = role if role in ROLES else ""
    if spreadsheet is None:
        try:
            import report_common
            spreadsheet = report_common.init_sheets()
        except Exception as exc:
            logger.warning("Амплуа: таблица недоступна: %s", exc)
            return False
    import coach_payments
    person = coach_payments.player_by_row(int(player_row)) or {}
    return sheets_cache.write_player_field(spreadsheet, int(player_row), "role",
                                           value, person.get("title", ""))


def upcoming(hours: int = 2) -> List[Dict[str, Any]]:
    """Игры, до начала которых осталось меньше указанного срока.

    По ним бот сам присылает тренеру стартовый состав: за час до игры решение
    уже принимают, и открывать бота ради списка неудобно."""
    import game_roster
    from datetime_utils import get_moscow_time
    now = get_moscow_time()
    out = []
    for g in game_roster.games(from_day=now.date(), until_day=now.date()):
        start = game_roster._game_time(g) if hasattr(game_roster, "_game_time") else None
        if start is None:
            try:
                hh, mm = str(g.get("time") or "").split(":")[:2]
                start = now.replace(hour=int(hh), minute=int(mm), second=0, microsecond=0)
            except (ValueError, TypeError):
                continue
        left = (start - now).total_seconds() / 3600.0
        if 0 <= left <= hours:
            out.append(g)
    return out
