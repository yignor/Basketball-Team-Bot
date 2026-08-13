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
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import coach_payments
import sheets_cache

logger = logging.getLogger(__name__)

# За сколько дней до игры просим тренера собрать состав.
COLLECT_BEFORE_DAYS = 3

# Цикл оплаты игры (решение пользователя 11.08.2026). Тренера предупреждаем
# раньше игрока — чтобы он успел поправить состав и суммы ДО того, как бот
# начнёт требовать деньги с людей. Дальше пара «сначала тренеру, через день
# игроку» повторяется с растущим шагом: чем дольше долг висит, тем реже
# дёргаем, иначе напоминание превращается в фон и его перестают читать.
#
# Отрицательное — до игры, положительное — после. Ноль был бы днём игры, но
# в день игры про деньги не пишем: людям не до того.
GAME_PAY_COACH_DAYS = (-2, 2, 5)      # тренеру: список, кому уйдёт, и долги
GAME_PAY_PLAYER_DAYS = (-1, 3, 8)     # игроку: сумма к оплате

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
            f"""SELECT game_id, game_date, game_time, alt_name, additional_data,
                       arena
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
            # Место и форму лига объявляет в самом опросе — берём оттуда, а не
            # заставляем тренера вводить то, что уже написано.
            "arena": str(r["arena"] or "") or _arena_from(str(r["additional_data"] or "")),
            "poll_form": _form_from(str(r["additional_data"] or "")),
        })
    out.sort(key=lambda g: g["date"])
    return out


def _form_from(text: str) -> str:
    """Форма из текста опроса: «👕 тёмная форма». Пусто — если не указана."""
    low = text.lower()
    if "тёмн" in low or "темн" in low:
        return "dark"
    if "светл" in low or "белая" in low:
        return "light"
    return ""


def _arena_from(text: str) -> str:
    """Место из текста опроса: строка с «📍»."""
    for line in text.splitlines():
        if "📍" in line:
            return line.replace("📍", "").strip()
    return ""


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
    out.sort(key=_by_surname)
    return out


# Форма на игру: тёмная или светлая. Хранится у игры, потому что зависит от
# соперника, а не от команды вообще.
FORMS = {"dark": "тёмная", "light": "светлая"}
# Тренер снял форму руками: не пусто (иначе снова подставится из опроса).
NO_FORM = "none"


def _by_surname(player: Dict[str, Any]) -> Tuple[str, str]:
    """Сортировка по-русски: без учёта регистра и с «ё» на своём месте.

    Сортировка по строке целиком ставила «Ёлкина» перед «Абрамовым»: в юникоде
    «Ё» стоит до «А», и алфавит рассыпался на ровном месте."""
    def norm(text: Any) -> str:
        return str(text or "").strip().lower().replace("ё", "е")
    return norm(player.get("surname")), norm(player.get("name"))


def form_of(source: str, game_id: str, game: Optional[Dict[str, Any]] = None) -> str:
    """Какая форма на игру. Выбор тренера сильнее, иначе — из опроса лиги.

    Отдельная отметка «снято руками» нужна, чтобы снятый тренером выбор не
    подменялся снова тем, что написано в опросе: иначе кнопка «сбросить» не
    работала бы вовсе."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT form FROM game_roster_state WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    chosen = str((row["form"] if row else "") or "")
    if chosen == NO_FORM:
        return ""
    if chosen:
        return chosen
    if game and game.get("poll_form"):
        return str(game["poll_form"])
    for g in games():
        if g["source"] == source and g["game_id"] == str(game_id):
            return str(g.get("poll_form") or "")
    return ""


def set_form(source: str, game_id: str, form: str) -> None:
    """Ставит форму. Пустая строка — снять выбор."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_roster_state (source, game_id, form, posted_at)
               VALUES (?, ?, ?, '')
               ON CONFLICT(source, game_id) DO UPDATE SET form = excluded.form""",
            (source, str(game_id), str(form or "")))
        conn.commit()


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
    """Заводит строку состояния игры и следит, чтобы дата в ней была.

    Здесь стоял `INSERT OR IGNORE`, и это был тихий баг. Строку состояния
    создаёт кто первый успел: выбор формы, публикация состава, стартовая
    пятёрка — и ни один из этих путей даты не знает. Если такой путь
    срабатывал раньше, дата оставалась пустой навсегда, потому что `IGNORE`
    её уже не заполнял. А пустая дата выкидывает игру из подсчёта долгов
    целиком: сравнение с датой начала порядка её просто не пропускает.

    Поэтому дозаполняем: строка есть — допишем то, чего в ней нет, не трогая
    уже известное."""
    sheets_cache.init_db()
    day = game["date"]
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_roster_state
               (source, game_id, game_date, opponent, posted_at)
               VALUES (?, ?, ?, ?, '')
               ON CONFLICT(source, game_id) DO UPDATE SET
                   game_date = CASE
                       WHEN COALESCE(game_roster_state.game_date, '') = ''
                       THEN excluded.game_date ELSE game_roster_state.game_date END,
                   opponent = CASE
                       WHEN COALESCE(game_roster_state.opponent, '') = ''
                       THEN excluded.opponent ELSE game_roster_state.opponent END""",
            (game["source"], str(game["game_id"]),
             day.isoformat() if hasattr(day, "isoformat") else str(day),
             game.get("opponent", "")))
        conn.commit()


def repair_dates() -> int:
    """Дозаполняет пустые даты игр из служебных записей. Возвращает, скольким.

    Чиним не разово, а при каждом старте демона: перекос выше мог случиться и
    на путях, которые мы ещё не перебрали, а цена ему — пропавшие долги целого
    состава. Запрос дешёвый: строк состояния десятки."""
    sheets_cache.init_db()
    fixed = 0
    with sheets_cache.get_connection() as conn:
        empty = [dict(r) for r in conn.execute(
            "SELECT source, game_id FROM game_roster_state "
            "WHERE COALESCE(game_date, '') = ''")]
        for one in empty:
            row = conn.execute(
                "SELECT game_date FROM service_records WHERE game_id = ? "
                "AND COALESCE(game_date, '') != '' LIMIT 1",
                (str(one["game_id"]),)).fetchone()
            if not row:
                continue
            conn.execute(
                "UPDATE game_roster_state SET game_date = ? "
                "WHERE source = ? AND game_id = ?",
                (str(row["game_date"])[:10], one["source"], str(one["game_id"])))
            fixed += 1
        if fixed:
            conn.commit()
        # Дата игры известна всегда: её либо вводит тренер, либо приносит
        # расписание лиги. Если она всё-таки пустая и восстановить неоткуда —
        # это не мелочь, а сломанный путь заведения игры, и он должен быть
        # виден. Подсчёт долгов такую игру не потеряет (возьмёт день
        # публикации состава), но чинить надо причину.
        left = [f"{r['source']}:{r['game_id']}" for r in conn.execute(
            "SELECT source, game_id FROM game_roster_state "
            "WHERE COALESCE(game_date, '') = ''")]
    if left:
        logger.warning("Игры без даты, восстановить неоткуда: %s",
                       ", ".join(left[:5]))
    return fixed


def is_posted(source: str, game_id: str) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT posted_at FROM game_roster_state WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return bool(row and str(row["posted_at"] or ""))


def mark_posted(source: str, game_id: str,
                posts: Optional[List[Dict[str, Any]]] = None) -> None:
    """Запоминает факт отправки: когда, куда и каким был состав.

    Адреса сообщений нужны, чтобы потом ПРАВИТЬ их, а не слать в чат второй
    список; снимок состава — чтобы видеть, что он с тех пор изменился.

    Кто отправил, в записи не держим: тренер и так знает, что это он, а на
    редкий вопрос «бот или человек» отвечает пометка в журнале."""
    rows = sorted(p["row"] for p in roster(source, game_id))
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_roster_state (source, game_id, posted_at,
                                              posted_json, posted_rows)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(source, game_id) DO UPDATE SET
                   posted_at = excluded.posted_at,
                   posted_json = excluded.posted_json,
                   posted_rows = excluded.posted_rows""",
            (source, str(game_id), datetime.now().isoformat(timespec="seconds"),
             json.dumps(posts or [], ensure_ascii=False), json.dumps(rows)))
        conn.commit()


def posted_messages(source: str, game_id: str) -> List[Dict[str, Any]]:
    """Куда ушёл состав: [{chat_id, message_id}] — их и правим."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT posted_json FROM game_roster_state WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    try:
        return json.loads((row["posted_json"] if row else "") or "[]")
    except (json.JSONDecodeError, TypeError):
        return []


def is_stale(source: str, game_id: str) -> bool:
    """Состав изменился после отправки — в чате висит устаревший список."""
    if not is_posted(source, game_id):
        return False
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT posted_rows FROM game_roster_state WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    try:
        was = json.loads((row["posted_rows"] if row else "") or "[]")
    except (json.JSONDecodeError, TypeError):
        was = []
    return sorted(was) != sorted(p["row"] for p in roster(source, game_id))


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
    lines = [head]
    # Форму пишем сразу под шапкой: это первое, что человек ищет перед выездом,
    # и искать её в конце списка из одиннадцати фамилий неудобно.
    form = form_of(game["source"], str(game["game_id"]), game)
    if form in FORMS:
        lines.append(f"👕 Форма: {FORMS[form]}")
    if game.get("arena"):
        lines.append(f"📍 {game['arena']}")
    lines.append("")
    for i, p in enumerate(people, start=1):
        lines.append(f"{i}. {p['title']}")
    lines += ["", f"Всего: {len(people)}."]
    return "\n".join(lines)


# ─────────────────────── Оплата игр по составу ─────────────────────────────

def _payments_of(player_row: int) -> Tuple[Set[str], int]:
    """Оплаты игрока: ссылки на закрытые игры и число оплат без ссылки.

    Считаем РАЗНЫЕ игры, а не количество платежей. Это важнее, чем кажется:
    оплата, привязанная к конкретной игре, закрывает ровно её и не может
    погасить чужую. Вторая оплата той же игры не добавляет ничего.

    Без ссылки платёж приходит из СМС («перевёл за две игры») — такие ложатся
    на самые ранние незакрытые игры по порядку."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT COALESCE(game_ref, '') AS ref, COALESCE(games, 1) AS games "
            "FROM payments WHERE player_row = ? AND kind = ?",
            (int(player_row), coach_payments.KIND_GAME)).fetchall()
    refs: Set[str] = set()
    free = 0
    for r in rows:
        ref = str(r["ref"]).strip()
        if ref:
            refs.add(ref)
        else:
            free += int(r["games"] or 1)
    return refs, free


def _countable_games(player_row: int) -> List[Tuple[str, str, str]]:
    """Игры, за которые с человека ждём денег: объявленные и с даты порядка."""
    return [g for g in _played_games(player_row)
            if g[2] >= PAY_SINCE and is_posted(g[0], g[1])]


def unpaid_games(player_row: int) -> List[Tuple[str, str, str]]:
    """Какие именно игры человек не закрыл, от старых к новым.

    Раньше долг считался как «сыграно минус оплачено», и это давало тихий
    перекос: игры фильтровались (только объявленные и с 04.08), а платежи
    брались ВСЕ подряд. Человек, заплативший за игру 02.08 — не объявленную и
    до начала порядка, — получал лишнюю оплату в зачёт, и она съедала долг за
    будущую игру. Так пропали долги за 15.08 и 16.08 у половины состава
    (12.08.2026)."""
    games = _countable_games(player_row)
    refs, free = _payments_of(player_row)
    rest = [g for g in games if f"{g[0]}:{g[1]}" not in refs]
    return rest[free:] if free else rest


def _played_games(player_row: int) -> List[Tuple[str, str, str]]:
    """Игры, где человек был в составе, от старых к новым.

    Даты нет — берём день публикации состава. Состав объявляют за день-два до
    игры, так что для порога «считаем с такого-то числа» это верно, а главное —
    состав, который тренер объявил, не пропадает из долгов молча. Именно так
    потерялись одиннадцать человек 11.08.2026."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT r.source, r.game_id,
                      COALESCE(NULLIF(s.game_date, ''),
                               substr(COALESCE(s.posted_at, ''), 1, 10),
                               '') AS game_date
               FROM game_rosters r
               LEFT JOIN game_roster_state s
                      ON s.source = r.source AND s.game_id = r.game_id
               WHERE r.player_row = ?""", (int(player_row),)).fetchall()
    out = [(str(r["source"]), str(r["game_id"]), str(r["game_date"])) for r in rows]
    out.sort(key=lambda x: x[2] or "9999")
    return out


def owes_for(source: str, game_id: str, player_row: int) -> bool:
    """Не закрыта ли ЭТА игра. Оплата с ссылкой закрывает ровно свою игру,
    оплата без ссылки — самые ранние незакрытые."""
    return any(src == source and str(gid) == str(game_id)
               for src, gid, _ in unpaid_games(player_row))


def debtors(source: str, game_id: str) -> List[Dict[str, Any]]:
    """Кто из состава не оплатил эту игру."""
    out = []
    for p in roster(source, game_id):
        if owes_for(source, game_id, p["row"]):
            out.append(p)
    return out


def game_debts() -> List[Dict[str, Any]]:
    """Кто и за сколько игр должен — по всем объявленным составам.

    Считаем по человеку целиком, а не по играм: оплаты ложатся на игры по
    порядку, и «должен за две игры» понятнее, чем список матчей."""
    sheets_cache.init_db()
    out: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        rows = [int(r["player_row"]) for r in conn.execute(
            "SELECT DISTINCT player_row FROM game_rosters")]
    for row in rows:
        owed_games = unpaid_games(row)
        if not owed_games:
            continue
        owed = len(owed_games)
        player = coach_payments.player_by_row(row) or {}
        price = coach_payments.game_price(player)
        out.append({"row": row, "title": player.get("title") or f"строка {row}",
                    "games": owed, "amount": owed * price,
                    "last": owed_games[-1][2]})
    out.sort(key=lambda x: -x["amount"])
    return out


def debts_by_game() -> List[Dict[str, Any]]:
    """Долги в разрезе игр: [{game, rows, total}], от ближайшей к дальней.

    Тренер собирает деньги на игре, а не «вообще»: ему нужен список, с которым
    можно прийти в зал и пройтись по людям. Общий список по людям на это не
    годится — по нему не понять, кто нужен сегодня, а кто в воскресенье.

    Долги каждого считаем один раз и раскладываем по играм, а не спрашиваем
    базу на каждого в каждой игре."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = [int(r["player_row"]) for r in conn.execute(
            "SELECT DISTINCT player_row FROM game_rosters")]
    by_ref: Dict[str, List[int]] = {}
    for row in rows:
        for src, gid, _ in unpaid_games(row):
            by_ref.setdefault(f"{src}:{gid}", []).append(row)
    if not by_ref:
        return []

    # Идём от объявленных составов, а не от записей об опросах. Опрос и состав
    # лежат в разных таблицах, и игра, заведённая без опроса, из списка бы
    # выпала целиком — ровно так же, как выпадала игра без даты. Подпись
    # (соперник, время) берём из опроса, если он есть, иначе из состояния игры.
    known = {f"{g['source']}:{g['game_id']}": g
             for g in games(from_day=date.fromisoformat(PAY_SINCE))}
    out: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        states = [dict(r) for r in conn.execute(
            "SELECT source, game_id, game_date, opponent FROM game_roster_state "
            "WHERE COALESCE(posted_at, '') != ''")]
    for st in states:
        ref = f"{st['source']}:{st['game_id']}"
        who = by_ref.get(ref)
        if not who:
            continue
        game = known.get(ref) or {
            "source": st["source"], "game_id": str(st["game_id"]),
            "date": _parse_date(st["game_date"]) or date.today(),
            "time": "", "opponent": str(st["opponent"] or ""), "arena": "",
        }
        people = []
        for player_row in who:
            player = coach_payments.player_by_row(player_row) or {}
            people.append({"row": player_row,
                           "title": player.get("title") or f"строка {player_row}",
                           "amount": coach_payments.game_price(player)})
        people.sort(key=lambda p: p["title"])
        out.append({"game": game, "rows": people,
                    "total": sum(p["amount"] for p in people)})
    out.sort(key=lambda x: x["game"]["date"])
    return out


def mark_paid(player_row: int, source: str, game_id: str, by: str = "") -> Dict[str, Any]:
    """Тренер отметил оплату игры без СМС.

    Отпечаток — по человеку и игре, без времени. За одну игру человек платит
    один раз, и отметить это дважды нельзя ни при каких обстоятельствах.

    Раньше отпечаток собирался из суммы и текущей МИНУТЫ, поэтому защищал
    только от двойного нажатия подряд. Нажатие через минуту заводило второй
    платёж за ту же игру — так пятеро оказались «оплатившими» игру 09.08
    дважды, а их долги за 15.08 и 16.08 бесследно исчезли (12.08.2026)."""
    player = coach_payments.player_by_row(player_row)
    price = coach_payments.game_price(player)
    ref = f"{source}:{game_id}"
    return coach_payments.record(
        player_row, price, coach_payments.KIND_GAME, 1,
        paid_at=date.today().isoformat(), bank="", note="отметил тренер",
        added_by=str(by),
        fp=coach_payments.fingerprint(f"gamemark|{player_row}|{ref}"),
        game_ref=ref, by_coach=True)




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


def player_debt_text(game: Dict[str, Any], player: Dict[str, Any],
                    ahead: bool = False) -> str:
    """Напоминание об оплате игры. ahead — накануне, а не вдогонку.

    Накануне человек ещё ничего не нарушил, и разговор другой: деньги за игру
    везут с собой, поэтому и напоминаем заранее."""
    price = coach_payments.game_price(player)
    if ahead:
        return (f"🏀 Завтра игра: {game_label(game)}.\n\n"
                f"Не забудь оплату — {price} ₽. Возьми с собой или переведи "
                "тренеру заранее.")
    return (f"💰 Оплата игры: {game_label(game)} — {price} ₽.\n\n"
            "Переведи, пожалуйста, тренеру и скинь ему чек — он отметит. "
            "Если уже оплатил, ничего делать не надо.")


# ─────────────────────── Что пора сделать сейчас ───────────────────────────

def _game_start(game: Dict[str, Any]) -> Optional[datetime]:
    """Начало игры как момент времени по Москве. Без времени в расписании — None.

    Со временнóй зоной, а не «голое» время: сравнивать это приходится с
    get_moscow_time(), а naive и aware в Python не вычитаются — сложение таких
    двух молча ломало весь фоновый цикл на первом же тике."""
    from datetime_utils import get_moscow_time
    try:
        hh, mm = str(game.get("time") or "").split(":")[:2]
        return datetime.combine(game["date"], time(int(hh), int(mm)),
                                tzinfo=get_moscow_time().tzinfo)
    except (ValueError, TypeError):
        return None


def due_events(now: Optional[datetime] = None) -> List[Tuple[str, Dict[str, Any], str]]:
    """[(ключ, игра, вид)] — что должно сработать в этот момент по Москве.

    Виды:
      collect        — за 3 дня: тренеру собрать состав;
      coach_pay      — за 2 дня, +2 и +5 дней: тренеру, кому уйдёт и кто должен;
      player_before  — ровно за сутки до начала: должникам, сумма к оплате;
      player_pay     — +3 и +8 дней: должникам повторно.

    Тренер узнаёт раньше игрока на каждом шаге — он должен успеть поправить
    состав и суммы до того, как бот начнёт требовать деньги с людей.

    Ключ уникален на игру и вид (с номером шага), по нему bot_daemon помнит,
    что уже отправлял."""
    from datetime_utils import get_moscow_time
    now = now or get_moscow_time()
    today = now.date()
    out: List[Tuple[str, Dict[str, Any], str]] = []
    hour = sheets_cache.get_int_setting("game_pay_hour", 9)
    after = max(GAME_PAY_COACH_DAYS + GAME_PAY_PLAYER_DAYS)

    for game in games(from_day=today - timedelta(days=after),
                      until_day=today + timedelta(days=COLLECT_BEFORE_DAYS)):
        ref = f"{game['source']}:{game['game_id']}"
        left = (game["date"] - today).days
        # Не «ровно за три дня», а «как только до игры осталось три дня или
        # меньше»: игру в лиге могут открыть и за два дня до неё, и тогда
        # точное совпадение просто не сработало бы. Событие помечается
        # выполненным, поэтому запрос уходит один раз.
        if 0 <= left <= sheets_cache.get_int_setting("roster_collect_days",
                                                     COLLECT_BEFORE_DAYS):
            out.append((f"game:{ref}:collect", game, "collect"))

        # Дальше — только деньги, и тут два условия. Игра не старше порядка
        # оплат, и состав РАЗОСЛАН в чат: пока команда его не видела, никто
        # ничего не должен. Собранный, но не отправленный состав — это
        # черновик тренера, а не основание для требования.
        if game["date"].isoformat() < PAY_SINCE:
            continue
        if not is_posted(game["source"], game["game_id"]):
            continue

        # Сколько дней прошло с игры: отрицательное — она ещё впереди.
        past = -left
        for step in GAME_PAY_COACH_DAYS:
            if past == step and now.hour >= hour:
                out.append((f"game:{ref}:coach_pay:{step}", game, "coach_pay"))

        # Накануне — напоминание СОСТАВУ: деньги обычно везут с собой, и
        # узнавать о долге уже в зале поздно. Отсчёт РОВНО от начала игры, а не
        # в условный час: игра в 18:30 — напоминание в 18:30 накануне.
        start = _game_start(game)
        if start and timedelta(hours=23, minutes=45) <= (start - now) <= timedelta(hours=24, minutes=30):
            out.append((f"game:{ref}:player_before", game, "player_before"))
        for step in GAME_PAY_PLAYER_DAYS:
            if step > 0 and past == step and now.hour >= hour:
                out.append((f"game:{ref}:player_pay:{step}", game, "player_pay"))
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
