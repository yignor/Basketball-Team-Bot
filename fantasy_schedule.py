#!/usr/bin/env python3
"""
Окно набора состава фэнтези, привязанное к расписанию игр.

Набор открыт не по календарю, а по реальным играм недели:
  • закрывается на первом анонсе «игра сегодня» недели (в день первой игры,
    во время оповещений) — составы фиксируются;
  • открывается на СЛЕДУЮЩУЮ неделю после статистики по последней игре недели
    (последняя игра + окно отслеживания), с сообщением участникам.

Даты игр бот знает заранее — он сам создаёт записи опросов/анонсов. Отсюда и
берём расписание: game_date/game_time из service_records (обе лиги), без сети.
Состояние (первая/последняя игра недели, активная неделя, что уже разослано)
живёт в settings_json сезона; здесь — единственный, кто его пишет.
"""

import datetime as _dt
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache
import fantasy
from datetime_utils import get_moscow_time, GAME_TRACKING_WINDOW_HOURS, MOSCOW_TZ

# Записи, из которых видно расписание нашей команды (обе лиги).
SCHEDULE_TYPES = (
    "ОПРОС_ИГРА", "АНОНС_ИГРА", "РЕЗУЛЬТАТ_ИГРА",
    "ОПРОС_ИГРА_SLPRO", "АНОНС_ИГРА_SLPRO", "РЕЗУЛЬТАТ_ИГРА_SLPRO",
)

# Во сколько выходит анонс «игра сегодня» (тогда же закрываем набор).
DEFAULT_ANNOUNCE_HHMM = "09:00"

LOCK_MSG = (
    "🔒 Фэнтези: набор на этот тур закрыт — сегодня первая игра недели. "
    "Составы зафиксированы. Удачи всем!"
)
OPEN_MSG = (
    "🟢 Фэнтези: открыт набор команды на следующий тур!\n"
    "Игры недели сыграны, очки подведены. Собери состав в приложении — "
    "он закрепится с первой игрой следующего тура."
)


def _parse_game_dt(game_date: str, game_time: str) -> Optional[_dt.datetime]:
    """Дата игры в МСК. Дата — DD.MM.YYYY (Infobasket) или YYYY-MM-DD (SLPRO),
    время — HH:MM или HH:MM:SS."""
    game_date = (game_date or "").strip()
    game_time = (game_time or "").strip() or "00:00"
    if not game_date:
        return None
    try:
        if "-" in game_date:
            d = _dt.date.fromisoformat(game_date)
        else:
            d = _dt.datetime.strptime(game_date, "%d.%m.%Y").date()
        parts = game_time.split(":")
        hh, mm = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
        return _dt.datetime(d.year, d.month, d.day, hh, mm, tzinfo=MOSCOW_TZ)
    except (ValueError, IndexError, TypeError):
        return None


def collect_game_datetimes() -> List[_dt.datetime]:
    """Даты-время игр нашей команды из записей опросов/анонсов/результатов.
    Дедуп по game_id (у одной игры бывают и опрос, и результат)."""
    sheets_cache.init_db()
    placeholders = ",".join("?" for _ in SCHEDULE_TYPES)
    by_game: Dict[str, _dt.datetime] = {}
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            f"""SELECT game_id, game_date, game_time FROM service_records
                WHERE deleted = 0 AND data_type IN ({placeholders})""",
            SCHEDULE_TYPES,
        ).fetchall()
    for r in rows:
        dt = _parse_game_dt(r["game_date"], r["game_time"])
        if dt is None:
            continue
        key = str(r["game_id"] or dt.isoformat())
        # если у игры несколько записей — оставляем любую (дата одна)
        by_game[key] = dt
    return list(by_game.values())


def _announce_dt(day: _dt.date, hhmm: str) -> _dt.datetime:
    try:
        hh, mm = (int(x) for x in hhmm.split(":")[:2])
    except (ValueError, IndexError):
        hh, mm = 9, 0
    return _dt.datetime(day.year, day.month, day.day, hh, mm, tzinfo=MOSCOW_TZ)


def tick(now: Optional[_dt.datetime] = None,
         announce_hhmm: str = DEFAULT_ANNOUNCE_HHMM) -> Tuple[Dict[str, Any], List[Tuple[str, str]]]:
    """Пересчитывает окно набора и возвращает (состояние, события-к-рассылке).

    События — список (тип, текст) для отправки участникам. На ПЕРВОМ прогоне
    (пустой sched) ничего не рассылается — только инициализируем состояние,
    чтобы деплой в середине недели не выстрелил рассылкой задним числом."""
    season = fantasy.get_active_season()
    if not season:
        return {"active_week": None, "locked": False}, []

    now = now or get_moscow_time()
    sched = fantasy.get_sched(season)
    first_run = not sched
    weeks: Dict[str, Dict[str, str]] = dict(sched.get("weeks") or {})

    # Фиксируем first/last по каждой неделе; раз записанное не сдвигаем назад,
    # даже когда игра прошла и выпала из выборки (важно после рестарта).
    for dt in collect_game_datetimes():
        wk = fantasy.week_start_of(dt.date()).isoformat()
        w = weeks.setdefault(wk, {})
        iso = dt.isoformat()
        if not w.get("first") or iso < w["first"]:
            w["first"] = iso
        if not w.get("last") or iso > w["last"]:
            w["last"] = iso

    w0_start = fantasy.week_start_of(now.date())
    w0 = w0_start.isoformat()
    w1 = (w0_start + _dt.timedelta(days=7)).isoformat()
    wk0 = weeks.get(w0)

    if not wk0:
        # На этой неделе игр нет — набор открыт на текущую неделю.
        active, locked = w0, False
    else:
        first_dt = _dt.datetime.fromisoformat(wk0["first"])
        last_dt = _dt.datetime.fromisoformat(wk0["last"])
        lock_at = _announce_dt(first_dt.date(), announce_hhmm)
        open_at = last_dt + _dt.timedelta(hours=GAME_TRACKING_WINDOW_HOURS)
        if now < lock_at:
            active, locked = w0, False        # набор на эту неделю открыт
        elif now < open_at:
            active, locked = w0, True         # игры идут — состав заморожен
        else:
            active, locked = w1, False        # неделя сыграна — открыт след. тур

    lock_notified = sched.get("lock_notified", "")
    open_notified = sched.get("open_notified", "")
    events: List[Tuple[str, str]] = []

    if locked and lock_notified != w0:
        fantasy.lock_week(season["id"], w0)
        if not first_run:
            events.append(("lock", LOCK_MSG))
        lock_notified = w0

    if active == w1 and open_notified != w1:
        if not first_run:
            events.append(("open", OPEN_MSG))
        open_notified = w1

    sched.update(weeks=weeks, active_week=active, locked=locked,
                 lock_notified=lock_notified, open_notified=open_notified,
                 updated_at=now.isoformat())
    fantasy.set_sched(sched, season["id"])

    return ({"active_week": active, "locked": locked,
             "first_run": first_run}, events)
