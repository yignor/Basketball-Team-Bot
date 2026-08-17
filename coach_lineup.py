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
    import game_roster
    current = start_five(source, game_id)
    # Считаем только тех, кто ещё в составе. Состав меняется до последнего, и
    # снятый игрок оставался в пятёрке: тренер видел четверых, а бот отказывал
    # добавить пятого — «в старте уже 5».
    live = {p["row"] for p in game_roster.roster(source, str(game_id))}
    current = [r for r in current if r in live]
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


def current_scopes() -> List[Dict[str, str]]:
    """Турниры, которые команда играет СЕЙЧАС — из листа «Конфиг».

    Тот же список, по которому бот ищет игры и считает очки фэнтези: держать
    для тренерского экрана свой было бы верным способом разойтись."""
    out: List[Dict[str, str]] = []
    try:
        import league_sync
        for team in league_sync.our_teams():
            out.append({"source": str(team.get("source") or ""),
                        "season_id": str(team.get("season_id") or ""),
                        "stage_id": str(team.get("stage_id") or "")})
    except Exception as exc:
        logger.warning("Текущие турниры не прочитались: %s", exc)
    # У Инфобаскета турнир — это comp_id, и их в «Конфиге» может быть
    # несколько на одну команду (у нас 140825 и 142849). Справочник команд
    # держит по одной строке на команду и второй турнир теряет, поэтому
    # дочитываем прямо из зеркала «Конфига».
    try:
        import config_sheet
        rows = _config_rows()
        for row in config_sheet.split(rows).get(config_sheet.GAME, []):
            kind = str(row[0] if row else "").strip().lower()
            comp = str(row[1] if len(row) > 1 else "").strip()
            if kind.startswith("инфобаскет") and comp.isdigit():
                out.append({"source": "infobasket", "season_id": comp, "stage_id": ""})
    except Exception as exc:
        logger.warning("Турниры из «Конфига» не прочитались: %s", exc)
    seen, uniq = set(), []
    for sc in out:
        key = (sc["source"], sc["season_id"], sc["stage_id"])
        if sc["source"] and sc["season_id"] and key not in seen:
            seen.add(key)
            uniq.append(sc)
    return uniq


def _config_rows() -> List[List[str]]:
    """Лист «Конфиг» из зеркала — списком строк, как его видит парсер."""
    sheets_cache.init_db()
    cols = [f"col_{c}" for c in "abcdefghij"]
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            f"SELECT {', '.join(cols)} FROM config_rows "
            "ORDER BY CAST(row_index AS INTEGER)").fetchall()
    return [[str(r[c] or "") for c in cols] for r in rows]


def averages(rows: List[Dict[str, Any]],
             scopes: Optional[List[Dict[str, str]]] = None) -> Dict[int, Dict[str, float]]:
    """{строка листа: средние за игру} — очки, подборы, потери.

    Считаем ТОЛЬКО по турнирам, которые команда играет сейчас (лист «Конфиг»).
    По всей истории выходило нечестно: у одного 137 игр за четыре года, у
    другого 7 за этот месяц, и рядом эти средние сравнивать нельзя — состав
    соперников и роль игрока за годы меняются полностью.

    Мост от строки листа к статистике лиг — price_refs: там уже сведено, кто
    из листа кем играет в лиге (связку ведёт фэнтези-пул). Через
    player_identities нельзя: ссылку на свой профиль прислал один человек."""
    import fantasy_stats
    scopes = current_scopes() if scopes is None else scopes
    sheets_cache.init_db()
    want = {int(r["row"]) for r in rows}
    pairs: Dict[int, set] = {}
    with sheets_cache.get_connection() as conn:
        for r in conn.execute("SELECT ref, player_row FROM price_refs"):
            row = int(r["player_row"] or 0)
            if row not in want:
                continue
            for one in fantasy_stats.expand_refs([str(r["ref"])]):
                src, pid = fantasy_stats.parse_ref(one)[:2]
                if src and pid:
                    pairs.setdefault(row, set()).add((src, str(pid)))
    out: Dict[int, Dict[str, float]] = {}
    if not pairs or not scopes:
        return out
    with sheets_cache.get_connection() as conn:
        for row, ids in pairs.items():
            games = pts = reb = tur = 0
            for src, pid in ids:
                for sc in scopes:
                    if sc["source"] != src:
                        continue
                    sql = ("SELECT SUM(games) g, SUM(pts) p, SUM(reb) r, SUM(tur) t "
                           "FROM player_totals WHERE source = ? AND player_id = ? "
                           "AND season_id = ?")
                    args = [src, pid, sc["season_id"]]
                    # Стадия есть только у SLPRO; у Инфобаскета сезон и есть
                    # турнир, и требовать пустую стадию значит ничего не найти.
                    if sc["stage_id"]:
                        sql += " AND stage_id = ?"
                        args.append(sc["stage_id"])
                    got = conn.execute(sql, args).fetchone()
                    if got and got["g"]:
                        games += int(got["g"] or 0)
                        pts += int(got["p"] or 0)
                        reb += int(got["r"] or 0)
                        tur += int(got["t"] or 0)
            out[row] = {"games": games,
                        "pts": round(pts / games, 1) if games else 0,
                        "reb": round(reb / games, 1) if games else 0,
                        "tur": round(tur / games, 1) if games else 0}
    # Кого нет в price_refs — того лига не знает вовсе: в заявке его нет.
    # Отличать это от «заявлен, но не играл» важно: первое чинит тренер
    # (дозаявить), второе не чинится ничем и пройдёт само.
    for row in want - set(pairs):
        out[row] = {"games": 0, "unlisted": True}
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
    avg = p.get("avg") or {}
    # Почему нет цифр — говорим прямо. «Нет в заявке» чинит тренер (дозаявить),
    # «не играл» не чинится ничем и пройдёт само; молчание же выглядит как
    # поломка бота.
    if p.get("guest"):
        # У гостя цифр нет и быть не может: он не в листе и не в заявке лиги.
        # «Нет в заявке» тут читалось бы как недоработка тренера, а чинить
        # нечего — человек просто не наш.
        second = ["гость на эту игру"]
    elif avg.get("unlisted"):
        second.append("нет в заявке лиги")
    elif not avg.get("games"):
        second.append("в этом турнире не играл")
    lines = [head, "     " + " · ".join(second)]
    if avg.get("games"):
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
                 "средние — по текущим турнирам.</i>")
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
        # Время начала берём одной общей функцией. Здесь стояла проверка
        # hasattr(game_roster, "_game_time") — функции с таким именем в
        # game_roster нет и не было, так что ветка не выполнялась ни разу, а
        # запасной разбор дублировал `_game_start` и делал это хуже: собирал
        # момент от СЕГОДНЯШНЕЙ даты, а не от даты игры.
        start = game_roster._game_start(g)
        if start is None:
            continue          # в расписании нет времени — сторожить нечего
        left = (start - now).total_seconds() / 3600.0
        if 0 <= left <= hours:
            out.append(g)
    return out
