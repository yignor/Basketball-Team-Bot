"""Тайм-коды присутствия игрока на площадке — чтобы смотреть в записи себя.

Обе лиги ведут живой протокол и помечают в нём выход/уход каждого игрока. У
Infobasket это события типов 8/9 (`Widget/GetOnline`), у SLPRO — записи
`action="status"` со значением 1/0. Из них складываются отрезки «вышел —
ушёл».

Главное, ради чего всё затевалось: перевести отрезки в РЕАЛЬНОЕ время, а не
игровое. На записи 10 игровых минут занимают 18–20 реальных, и по «минуте
игрового времени» видео не отмотать. Реальное время берём из самого
протокола:

* SLPRO — поле `date_add` (секретарь ведёт протокол вживую, это часы на стене);
* Infobasket — поле `VideoFrom` у части событий: абсолютный unix-таймстемп
  куска видео. Он есть примерно у четверти событий, остальные разносим
  линейной интерполяцией между соседними якорями.

Ноль отсчёта в записи — момент, с которого включили трансляцию. Считаем, что
её включают ко времени из расписания: тогда спорный мяч приходится на
«начало игры минус время по расписанию» — обычно это 1–12 минут (команды
опаздывают, предыдущая игра затягивается). Началом игры считаем не первую
запись в протоколе, а момент, когда пошли игровые часы: состав секретарь
заводит заранее, иногда сильно. Точнее из данных не узнать, поэтому под
тайм-кодами честно висит приписка про возможное смещение.

Имён здесь нет и не будет — только id игрока ([[legal-data-invariant]]).
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

IB_ONLINE_URL = "https://reg.infobasket.su/Widget/GetOnline/{game_id}?format=json&lang=ru"
IB_PLAY_IN = 8       # выход на площадку
IB_PLAY_OUT = 9      # уход с площадки
IB_PERIOD_START = 21  # старт периода — у него всегда есть VideoFrom
HTTP_TIMEOUT = 25

# Длина периода по умолчанию, если лига не прислала свою (десятые доли секунды
# в Infobasket, секунды в SLPRO).
DEFAULT_PERIOD_SECONDS = 600

# Отрезки короче — это «вышел и сразу ушёл» на замене-формальности; в списке
# тайм-кодов они только мешают.
MIN_SHIFT_SECONDS = 10

# Сколько дней назад ищем игры без разметки при фоновой дозагрузке.
BACKFILL_DAYS = 30

# Обе лиги играют по московскому времени, и расписание тоже московское.
MSK = timezone(timedelta(hours=3))

# Насколько поздно матч может начаться относительно расписания, чтобы в это
# ещё верилось. Больше часа — значит расписание в кеше не про эту игру
# (перенос, спаренный тур), и сдвиг лучше не выдумывать.
MAX_LATE_START = 3600

# Разметка, сделанная раньше этой отметки, пересчитывается заново: до неё
# нулём отсчёта у SLPRO была первая запись протокола, а не начало игры.
# Дешевле перекачать 13 игр, чем держать в базе две несовместимые эпохи.
# Время — UTC, как и fetched_at (сервер живёт в UTC): с московской отметкой
# условие оставалось бы верным ещё три часа после правки, и каждый прогон
# ingest перекачивал бы все протоколы заново.
REDO_BEFORE = "2026-08-04T09:00:00"

SHIFT_NOTE = ("<i>Время в записи считаем от начала трансляции по расписанию — "
              "возможно смещение на минуту-другую.</i>")


# ───────────────────────────── Infobasket ──────────────────────────────────

def _ib_fetch(game_id: str) -> Dict[str, Any]:
    req = urllib.request.Request(
        IB_ONLINE_URL.format(game_id=game_id),
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.load(resp)


def _ib_period_offsets(data: Dict[str, Any]) -> Dict[int, float]:
    """Сколько игровых секунд прошло к началу каждого периода."""
    offsets, total = {}, 0.0
    periods = data.get("OnlinePeriods") or []
    for item in sorted(periods, key=lambda p: int(p.get("Period") or 0)):
        num = int(item.get("Period") or 0)
        offsets[num] = total
        total += float(item.get("Seconds") or DEFAULT_PERIOD_SECONDS)
    return offsets


def _elapsed(period: int, play_second: Any, offsets: Dict[int, float]) -> float:
    """Игровое время события от начала матча, в секундах.

    PlaySecond у Infobasket — десятые доли секунды ВНУТРИ периода и растёт от
    нуля (в конце периода 6000 при десятиминутке)."""
    base = offsets.get(int(period))
    if base is None:  # овертайм, которого не было в OnlinePeriods
        base = max(offsets.values(), default=0.0) + DEFAULT_PERIOD_SECONDS
    return base + float(play_second or 0) / 10.0


def _interpolate(anchors: List[Tuple[float, float]], clock: float) -> float:
    """Реальное время по игровому — линейно между соседними якорями.

    Якорь — событие, у которого лига проставила настоящий таймстемп. За
    пределами крайних якорей продолжаем наклон ближайшего отрезка: замена в
    первые секунды матча иначе съезжала бы на начало записи."""
    if not anchors:
        return clock
    if len(anchors) == 1:
        return anchors[0][1] + (clock - anchors[0][0])
    if clock <= anchors[0][0]:
        (c1, r1), (c2, r2) = anchors[0], anchors[1]
    elif clock >= anchors[-1][0]:
        (c1, r1), (c2, r2) = anchors[-2], anchors[-1]
    else:
        lo, hi = 0, len(anchors) - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if anchors[mid][0] <= clock:
                lo = mid
            else:
                hi = mid
        (c1, r1), (c2, r2) = anchors[lo], anchors[hi]
    if c2 == c1:
        return r1
    return r1 + (r2 - r1) * (clock - c1) / (c2 - c1)


def _ib_shifts(game_id: str) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    data = _ib_fetch(str(game_id))
    plays = data.get("OnlinePlays") or []
    starts = data.get("OnlineStarts") or []
    if not plays:
        return [], None

    # StartID — номер заявки в этой игре; настоящий id игрока лежит в PersonID.
    person = {int(s.get("StartID") or 0): str(s.get("PersonID") or "")
              for s in starts if s.get("PersonID")}
    offsets = _ib_period_offsets(data)

    # Якоря разложены ПО ПЕРИОДАМ, и это принципиально. Конец третьего и
    # начало четвёртого приходятся на одну и ту же отметку игровых часов, а в
    # записи между ними перерыв в пару минут. Сквозная интерполяция этот
    # перерыв съедала: игрок, отыгравший без замены, получал два стыкующихся
    # отрезка, и второй начинался на две минуты раньше, чем на видео.
    per_anchors: Dict[int, List[Tuple[float, float]]] = {}
    for p in plays:
        video = p.get("VideoFrom")
        if not video:
            continue
        period = int(p.get("PlayPeriod") or 0)
        per_anchors.setdefault(period, []).append(
            (_elapsed(period, p.get("PlaySecond"), offsets), float(video)))
    for items in per_anchors.values():
        items.sort()
    if not per_anchors:
        logger.info("Тайм-коды infobasket/%s: в протоколе нет привязки к видео", game_id)
        return [], None
    every = sorted(a for items in per_anchors.values() for a in items)
    # Ноль — старт первого периода (у него якорь есть всегда, событие типа 21).
    zero = per_anchors.get(1, every)[0][1]

    events: List[Dict[str, Any]] = []
    for p in plays:
        kind = int(p.get("PlayTypeID") or 0)
        if kind not in (IB_PLAY_IN, IB_PLAY_OUT):
            continue
        pid = person.get(int(p.get("StartID") or 0))
        if not pid:
            continue
        period = int(p.get("PlayPeriod") or 0)
        clock = _elapsed(period, p.get("PlaySecond"), offsets)
        events.append({
            "player_id": pid,
            "period": period,
            "clock": clock,
            "real": _interpolate(per_anchors.get(period) or every, clock) - zero,
            "in": kind == IB_PLAY_IN,
            "order": int(p.get("PlaySortOrder") or 0),
        })
    return _pair(events), _seconds_of_day(datetime.fromtimestamp(zero, MSK))


# ─────────────────────────────── SLPRO ─────────────────────────────────────

def _seconds_of_day(moment: datetime) -> int:
    return moment.hour * 3600 + moment.minute * 60 + moment.second


def _scheduled_seconds(game_time: Any) -> Optional[int]:
    """Время из расписания в секундах от полуночи. Форматы у лиг разные:
    SLPRO пишет «21:10:00», Infobasket — «20.00»."""
    raw = str(game_time or "").strip().replace(".", ":")
    if not raw:
        return None
    parts = raw.split(":")
    try:
        nums = [int(p) for p in parts[:3]]
    except ValueError:
        return None
    if not nums or not (0 <= nums[0] < 24):
        return None
    while len(nums) < 3:
        nums.append(0)
    return nums[0] * 3600 + nums[1] * 60 + nums[2]


def _slpro_time(value: Any) -> Optional[float]:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S").timestamp()
    except (ValueError, TypeError):
        return None


async def _slpro_shifts(client, game_id: str) -> Tuple[List[Dict[str, Any]], Optional[int]]:
    game = await client.get_game(game_id)
    log = (game or {}).get("log") or []
    if not log:
        return [], None

    stamps = [t for t in (_slpro_time(e.get("date_add")) for e in log) if t]
    if not stamps:
        logger.info("Тайм-коды slpro/%s: в протоколе нет времени событий", game_id)
        return [], None

    # game_time у SLPRO идёт на УБЫВАНИЕ (600 → 0) и считается внутри периода.
    per_len = max((float(e.get("game_time") or 0) for e in log),
                  default=DEFAULT_PERIOD_SECONDS) or DEFAULT_PERIOD_SECONDS

    # Ноль — не первая запись в протоколе, а момент, когда ПОШЛИ ИГРОВЫЕ ЧАСЫ.
    # Секретарь заводит стартовые пятёрки заранее и по-разному: обычно за
    # полминуты, а 19.07 — за двенадцать минут до спорного. По первой записи
    # весь матч уезжал на эти минуты вперёд.
    running = [t for t in (_slpro_time(e.get("date_add")) for e in log
                           if int(e.get("period") or 0) == 1
                           and float(e.get("game_time") or per_len) < per_len) if t]
    zero = min(running) if running else min(stamps)

    events: List[Dict[str, Any]] = []
    for idx, e in enumerate(log):
        if str(e.get("action") or "") != "status":
            continue
        pid = str(e.get("player_id") or "")
        stamp = _slpro_time(e.get("date_add"))
        if not pid or stamp is None:
            continue
        period = int(e.get("period") or 0)
        elapsed = (period - 1) * per_len + (per_len - float(e.get("game_time") or 0))
        events.append({
            "player_id": pid,
            "period": period,
            "clock": elapsed,
            "real": stamp - zero,
            "in": int(e.get("value") or 0) == 1,
            "order": idx,
        })
    # date_add секретаря — местное московское время, как и расписание.
    return _pair(events), _seconds_of_day(datetime.fromtimestamp(zero))


# ──────────────────────────── Общая сборка ─────────────────────────────────

def _pair(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Складывает «вышел»/«ушёл» в отрезки. Незакрытые отбрасываем.

    Незакрытый отрезок означает дыру в протоколе (секретарь не отметил уход) —
    и лучше показать меньше, чем нарисовать человеку отрезок до конца матча,
    которого не было."""
    events.sort(key=lambda e: (e["period"], e["clock"], e["order"]))
    open_at: Dict[str, Dict[str, Any]] = {}
    shifts: List[Dict[str, Any]] = []
    for e in events:
        pid = e["player_id"]
        if e["in"]:
            open_at[pid] = e
            continue
        start = open_at.pop(pid, None)
        if not start:
            continue
        # Стартовую пятёрку заводят до спорного, поэтому её «выход» приходится
        # на отрицательное время. Для записи это ноль: игра начинается тогда,
        # когда начинается.
        start_real, end_real = max(0.0, start["real"]), e["real"]
        if end_real < start_real:
            continue
        shifts.append({
            "player_id": pid,
            "period": start["period"],
            "start_sec": int(round(start_real)),
            "end_sec": int(round(end_real)),
            "clock_sec": int(round(max(0.0, e["clock"] - start["clock"]))),
        })
    if open_at:
        logger.info("Тайм-коды: %d незакрытых отрезков в протоколе", len(open_at))
    shifts.sort(key=lambda s: (s["player_id"], s["start_sec"]))
    return shifts


def store(source: str, game_id: Any, shifts: List[Dict[str, Any]]) -> int:
    """Переписывает разметку игры целиком (протокол могли поправить задним
    числом — дописывать к старому нельзя)."""
    if not shifts:
        return 0
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_shifts WHERE source = ? AND game_id = ?",
                     (source, str(game_id)))
        conn.executemany(
            """INSERT OR REPLACE INTO game_shifts
                   (source, game_id, player_id, period, start_sec, end_sec,
                    clock_sec, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            [(source, str(game_id), s["player_id"], s["period"], s["start_sec"],
              s["end_sec"], s["clock_sec"], now) for s in shifts])
        conn.commit()
    return len(shifts)


def has_shifts(source: str, game_id: Any) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM game_shifts WHERE source = ? AND game_id = ? LIMIT 1",
            (source, str(game_id))).fetchone()
    return bool(row)


def shifts(source: str, game_id: Any, player_id: Any = None) -> List[Dict[str, Any]]:
    """Разметка из кеша. Живых запросов отсюда не делаем — это путь ответа
    человеку, наружу ходит только фоновая дозагрузка."""
    sheets_cache.init_db()
    sql = ("SELECT player_id, period, start_sec, end_sec, clock_sec "
           "FROM game_shifts WHERE source = ? AND game_id = ?")
    args: List[Any] = [source, str(game_id)]
    if player_id is not None:
        sql += " AND player_id = ?"
        args.append(str(player_id))
    sql += " ORDER BY start_sec"
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(sql, args)]


async def refresh(source: str, game_id: Any, client=None) -> int:
    """Тянет протокол игры и кладёт отрезки в кеш. Возвращает число отрезков."""
    source = str(source)
    try:
        if source == "infobasket":
            found, tipoff = await asyncio.to_thread(_ib_shifts, str(game_id))
        elif source == "slpro":
            if client is None:
                from slpro_client import SlproClient
                client = SlproClient()
            found, tipoff = await _slpro_shifts(client, str(game_id))
        else:
            return 0
    except Exception as exc:  # сеть/формат — не роняем ingest из-за тайм-кодов
        logger.warning("Тайм-коды %s/%s: %s", source, game_id, exc)
        return 0
    n = store(source, game_id, found)
    if n:
        sync_to_schedule(source, game_id, tipoff)
    return n


def sync_to_schedule(source: str, game_id: Any, tipoff_sec: Optional[int]) -> int:
    """Считает, на какой секунде записи спорный мяч, и запоминает.

    Трансляцию включают ко времени из расписания, а свисток дают позже:
    команды опаздывают, предыдущая игра затягивается. Разница «протокольный
    старт минус расписание» и есть искомый сдвиг — по нашим играм выходит от
    минуты до двенадцати.

    Отрицательная разница (в протоколе старт раньше расписания) означает, что
    трансляцию включили позже назначенного, и тогда спорный — в самом начале
    записи. Считаем сдвиг нулевым, а не отматываем в минус."""
    if tipoff_sec is None:
        return 0
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT game_time FROM game_meta WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    planned = _scheduled_seconds(row["game_time"]) if row else None
    if planned is None:
        return 0
    shift = tipoff_sec - planned
    if shift < 0 or shift > MAX_LATE_START:
        shift = 0
    set_offset(source, game_id, shift, "auto")
    return shift


def our_games(conn, extra_where: str = "", args: Optional[List[Any]] = None,
              limit: int = 20) -> List[Dict[str, Any]]:
    """Игры НАШИХ команд с записью ВК, свежие сначала.

    Кеш статистики держит всю лигу (по ней считаются рейтинги), и без этого
    фильтра в разметку лезли чужие матчи: протоколы качались зря, а тренеру в
    списке записей показывали «КУРАЖ — Кудрово»."""
    ours = [str(r["team_id"]) for r in conn.execute(
        "SELECT team_id FROM league_teams WHERE ours = 1")]
    if not ours:
        return []
    marks = ",".join("?" * len(ours))
    sql = (f"""SELECT m.source, m.game_id, m.game_date, m.home_name, m.guest_name,
                      (SELECT COUNT(*) FROM game_shifts s
                        WHERE s.source = m.source AND s.game_id = m.game_id) AS shifts
                 FROM game_meta m
                WHERE m.video_vk != '' AND m.game_date != ''
                  AND (m.home_team_id IN ({marks}) OR m.guest_team_id IN ({marks}))
                  {extra_where}
                ORDER BY m.game_date DESC LIMIT ?""")
    return [dict(r) for r in conn.execute(sql, ours + ours + list(args or []) + [int(limit)])]


def player_games(identities: List[Tuple[str, Any]], limit: int = 8) -> List[Dict[str, Any]]:
    """Игры с разметкой, где играл этот человек, свежие сначала.

    identities — пары (источник, id игрока в лиге): у большинства их две,
    основа и Farm."""
    sheets_cache.init_db()
    found: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        for source, player_id in identities:
            found += [dict(r, player_id=str(player_id)) for r in conn.execute(
                """SELECT m.source, m.game_id, m.game_date, m.home_name,
                          m.guest_name, m.video_vk, COUNT(*) AS shifts
                     FROM game_shifts s
                     JOIN game_meta m ON m.source = s.source AND m.game_id = s.game_id
                    WHERE s.source = ? AND s.player_id = ?
                    GROUP BY s.source, s.game_id
                    ORDER BY m.game_date DESC LIMIT ?""",
                (str(source), str(player_id), int(limit)))]
    found.sort(key=lambda g: str(g["game_date"]), reverse=True)
    return found[:limit]


def games_without_shifts(limit: int = 20, days: int = BACKFILL_DAYS) -> List[Dict[str, Any]]:
    """Наши сыгранные игры, которым разметки не хватает.

    Ограничиваемся играми с записью ВК: тайм-коды без видео некуда
    прикладывать. Берём и те, что размечены, но без привязки к записи, — так
    игры, размеченные до появления авторасчёта сдвига, дотянутся сами."""
    sheets_cache.init_db()
    since_iso = datetime.fromordinal(
        datetime.now().date().toordinal() - days).date().isoformat()
    with sheets_cache.get_connection() as conn:
        return our_games(
            conn,
            extra_where=("AND m.game_date >= ? "
                         "AND (NOT EXISTS (SELECT 1 FROM game_shifts s "
                         "                  WHERE s.source = m.source AND s.game_id = m.game_id) "
                         "  OR NOT EXISTS (SELECT 1 FROM game_video_sync v "
                         "                  WHERE v.source = m.source AND v.game_id = m.game_id) "
                         "  OR EXISTS (SELECT 1 FROM game_shifts s "
                         "              WHERE s.source = m.source AND s.game_id = m.game_id "
                         "                AND s.fetched_at < ?)) "
                         "AND EXISTS (SELECT 1 FROM game_player_stats p "
                         "             WHERE p.source = m.source AND p.game_id = m.game_id)"),
            args=[since_iso, REDO_BEFORE], limit=limit)


async def backfill(limit: int = 10, client=None) -> int:
    """Фоновая дозагрузка разметки. Зовётся из ingest, не из ответа человеку."""
    total = 0
    for game in games_without_shifts(limit=limit):
        n = await refresh(game["source"], game["game_id"], client=client)
        total += bool(n)
        if n:
            logger.info("Тайм-коды %s/%s: отрезков %d",
                        game["source"], game["game_id"], n)
    return total


# ───────────────────────── Привязка к записи ВК ────────────────────────────

def offset(source: str, game_id: Any) -> int:
    """На какой секунде записи спорный мяч. 0 — сдвиг не задан."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT offset_sec FROM game_video_sync WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return int(row["offset_sec"]) if row else 0


def set_offset(source: str, game_id: Any, seconds: int, who: Any = "") -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_video_sync (source, game_id, offset_sec, set_by, set_at)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(source, game_id) DO UPDATE SET
                   offset_sec = excluded.offset_sec,
                   set_by = excluded.set_by, set_at = excluded.set_at""",
            (source, str(game_id), int(seconds), str(who), sheets_cache.now_iso()))
        conn.commit()


def hhmmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def vk_link(video_url: str, seconds: int) -> str:
    """Ссылка на запись с перемоткой. ВК понимает `t=` в секундах."""
    if not video_url:
        return ""
    base = str(video_url).split("#")[0]
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}t={max(0, int(seconds))}"


def timecodes(source: str, game_id: Any, player_id: Any,
              video_url: str = "") -> List[Dict[str, Any]]:
    """Отрезки игрока, готовые к показу: время в записи + ссылка."""
    shift = offset(source, game_id)
    out = []
    for s in shifts(source, game_id, player_id):
        start = s["start_sec"] + shift
        end = s["end_sec"] + shift
        out.append({
            "period": s["period"],
            "start": start,
            "end": end,
            "label": f"{hhmmss(start)}–{hhmmss(end)}",
            "clock_sec": s["clock_sec"],
            "link": vk_link(video_url, start),
        })
    return out


def format_block(source: str, game_id: Any, player_id: Any,
                 video_url: str = "", max_items: int = 8) -> str:
    """Блок «где ты на записи» для личного разбора (HTML). Пусто — если нечего.

    Короткие выходы-однодневки прячем, но в счётчике их не теряем: человек
    сверяет число отрезков со своей памятью об игре."""
    items = [t for t in timecodes(source, game_id, player_id, video_url)]
    if not items:
        return ""
    shown = [t for t in items if (t["end"] - t["start"]) >= MIN_SHIFT_SECONDS] or items
    total = sum(s["clock_sec"] for s in shifts(source, game_id, player_id))
    # Считаем по показанным: заголовок «8 выходов» над списком из семи строк
    # выглядит как потерянная строка, а не как отброшенная секундная замена.
    count = len(shown)
    head = (f"⏱ <b>Ты на площадке</b> · {count} "
            f"{'выход' if count % 10 == 1 and count != 11 else 'выхода' if count % 10 in (2, 3, 4) and count not in (12, 13, 14) else 'выходов'}")
    if video_url:
        head += f" · <a href=\"{video_url}\">запись</a>"
    lines = [head]
    for t in shown[:max_items]:
        mark = f"{t['period']}-й период"
        # Ссылка спрятана под время: в сообщении остаётся «0:00–8:02», а не
        # простыня из vk.com/video-...?t=. Иначе блок из восьми выходов
        # раздувает разбор так, что своей статистики уже не видно.
        if video_url:
            lines.append(f"• <a href=\"{t['link']}\">{t['label']}</a> · {mark}")
        else:
            lines.append(f"• {t['label']} · {mark}")
    if len(shown) > max_items:
        lines.append(f"…и ещё {len(shown) - max_items}")
    if total:
        lines.append(f"Итого {total // 60} мин игрового времени")
    lines.append(SHIFT_NOTE)
    return "\n".join(lines)
