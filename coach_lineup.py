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

# Амплуа: номер и название одной позиции — это одно и то же, и держать их
# порознь нельзя. Мои кнопки раньше предлагали голые «1…5», а в листе у команды
# уже стояли настоящие позиции («Атакующий защитник», «Центровой») — выбор
# кнопкой ЗАТИРАЛ их цифрой, и на экране получалась мешанина из цифр и слов.
#
# Храним название (оно уже в листе, его читает человек), показываем с номером.
ROLES = [("1", "Разыгрывающий"),
         ("2", "Атакующий защитник"),
         ("3", "Легкий форвард"),
         ("4", "Тяжелый форвард"),
         ("5", "Центровой"),
         ("", "Универсал")]

ROLE_NAMES = [name for _, name in ROLES]


def _norm_role(role: str) -> str:
    return str(role or "").strip().lower().replace("ё", "е")


def role_number(role: str) -> str:
    """Номер позиции по её названию. Пусто — если название нестандартное."""
    key = _norm_role(role)
    for num, name in ROLES:
        if _norm_role(name) == key:
            return num
    return key if key.isdigit() else ""

# За какой срок до игры считаем тренировки.
WINDOW_DAYS = 30

# Подписи короткие: три кнопки в ряду, длиннее девяти знаков телефон обрежет.
SORTS = {"name": "А–Я", "trainings": "Трен.", "role": "Амплуа"}

# Сколько человек начинают игру.
START_SIZE = 5


def role_title(role: str) -> str:
    """«Атакующий защитник» -> «№2 · атакующий защитник».

    Номер и название вместе: тренер думает номерами, а в листе записаны слова,
    и показывать что-то одно значит заставлять его переводить в уме."""
    role = str(role or "").strip()
    if not role:
        return ""
    num = role_number(role)
    if role.isdigit():
        name = next((n for k, n in ROLES if k == role), "")
        return f"№{role}" + (f" · {name.lower()}" if name else "")
    return (f"№{num} · " if num else "") + role.lower()


def start_five(source: str, game_id: str) -> List[int]:
    """Строки листа тех, кто выходит в старте. Порядок — как выбирал тренер."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT start_rows FROM game_roster_state WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    raw = str((row or {"start_rows": ""})["start_rows"] or "")
    try:
        import json
        return [int(x) for x in json.loads(raw)] if raw else []
    except (ValueError, TypeError):
        return []


def toggle_start(source: str, game_id: str, player_row: int) -> Tuple[bool, str]:
    """Ставит игрока в старт или снимает. (в старте ли, что сказать тренеру).

    Больше пяти не берём: пятёрка — это пятёрка, и молча растянуть её значит
    прислать в чат список, который не соответствует названию."""
    import json
    current = start_five(source, game_id)
    row = int(player_row)
    if row in current:
        current.remove(row)
        note = "снял из старта"
    elif len(current) >= START_SIZE:
        return False, f"В старте уже {START_SIZE} — сними кого-то"
    else:
        current.append(row)
        note = "в старте"
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_roster_state (source, game_id, start_rows)
               VALUES (?, ?, ?)
               ON CONFLICT(source, game_id) DO UPDATE SET
                   start_rows = excluded.start_rows""",
            (source, str(game_id), json.dumps(current)))
        conn.commit()
    return row in current, note


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


def averages(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, float]]:
    """{строка листа: средние за игру} — очки, подборы, потери.

    Мост от строки листа к статистике лиг — price_refs: там уже сведено, кто
    из листа кем играет в лиге (эту связку ведёт фэнтези-пул). Через
    player_identities нельзя: ссылку на свой профиль прислал один человек.

    Считаем по ВСЕЙ доступной истории, а не по текущему турниру: за один
    короткий турнир у половины команды две игры, и среднее по ним ничего не
    говорит."""
    import fantasy_stats
    sheets_cache.init_db()
    want = {int(r["row"]) for r in rows}
    pairs: Dict[int, List[Tuple[str, str]]] = {}
    with sheets_cache.get_connection() as conn:
        for r in conn.execute("SELECT ref, player_row FROM price_refs"):
            row = int(r["player_row"] or 0)
            if row not in want:
                continue
            for one in fantasy_stats.expand_refs([str(r["ref"])]):
                src, pid = fantasy_stats.parse_ref(one)[:2]
                if src and pid:
                    pairs.setdefault(row, []).append((src, pid))
    out: Dict[int, Dict[str, float]] = {}
    if not pairs:
        return out
    with sheets_cache.get_connection() as conn:
        for row, ids in pairs.items():
            games = pts = reb = tur = 0
            for src, pid in set(ids):
                got = conn.execute(
                    """SELECT SUM(games) g, SUM(pts) p, SUM(reb) r, SUM(tur) t
                         FROM player_totals WHERE source = ? AND player_id = ?""",
                    (src, str(pid))).fetchone()
                if got and got["g"]:
                    games += int(got["g"] or 0)
                    pts += int(got["p"] or 0)
                    reb += int(got["r"] or 0)
                    tur += int(got["t"] or 0)
            if games:
                out[row] = {"games": games,
                            "pts": round(pts / games, 1),
                            "reb": round(reb / games, 1),
                            "tur": round(tur / games, 1)}
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
    stats = averages(rows)
    for r in rows:
        r["avg"] = stats.get(r["row"], {})
    picked = start_five(source, str(game_id))
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
    return {"game": game, "rows": rows, "sort": sort, "day": day,
            # Порядок пятёрки — как выбирал тренер, а не как отсортирован список.
            "start": [r for r in picked if any(x["row"] == r for x in rows)]}


def player_card(p: Dict[str, Any], number: str = "") -> List[str]:
    """Игрок тремя строками: кто, на какой позиции, чем полезен.

    Имя жирным и пустая строка между игроками — иначе десять человек по три
    строки сливаются в стену, и глаз не находит, где кончается один и
    начинается другой. Отступ пробелами тут не спасает: шрифт в Телеграме
    пропорциональный, и два пробела почти не видны."""
    import html
    import coach_payments
    head = f"{number}<b>{html.escape(p['title'])}</b>"
    role = role_title(p.get("role", ""))
    second = [role or "амплуа не задано", f"{p['trainings']} трен."]
    lines = [head, "     " + " · ".join(second)]
    avg = p.get("avg") or {}
    if avg:
        lines.append(f"     {avg['pts']:g} очк · {avg['reb']:g} подб · "
                     f"{avg['tur']:g} пот   "
                     f"({coach_payments.plural(avg['games'], 'игра', 'игры', 'игр')})")
    return lines + [""]


def text(data: Dict[str, Any], title: str = "🏁 Стартовый состав") -> str:
    """Сообщение тренеру: два раздела — старт и скамейка.

    Разделы, а не один список: это разные решения. В старте важен порядок и
    позиции, на скамейке — кто готов выйти."""
    import game_roster
    game, rows = data.get("game"), data.get("rows") or []
    picked = data.get("start") or []
    head = title
    if game:
        head += f"\n{game_roster.game_label(game)}"
    if not rows:
        return head + "\n\nСостав пока не собран."

    by_row = {r["row"]: r for r in rows}
    lines = [head, ""]
    lines.append(f"━━━  СТАРТ · {len(picked)} из {START_SIZE}  ━━━")
    lines.append("")
    if picked:
        for i, row in enumerate(picked, start=1):
            p = by_row.get(row)
            if p:
                lines += player_card(p, f"{i}.  ")
    else:
        lines += ["Пусто. Нажми на фамилию — поставлю в старт.", ""]

    rest = [r for r in rows if r["row"] not in picked]
    lines.append(f"━━━  СКАМЕЙКА · {len(rest)}  ━━━")
    lines.append("")
    if rest:
        for p in rest:
            lines += player_card(p)
    else:
        lines += ["Пусто.", ""]
    lines.append(f"<i>Тренировки — за {WINDOW_DAYS} дней до игры, "
                 "средние — по всем сыгранным.</i>")
    return "\n".join(lines)


def set_role(player_row: int, role: str, spreadsheet: Any = None) -> bool:
    """Ставит амплуа игроку: в лист «Игроки» и в зеркало.

    Пустое значение снимает амплуа — тренер мог поставить по ошибке."""
    value = role if role in ROLE_NAMES else ""
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
