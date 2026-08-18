#!/usr/bin/env python3
"""
Блокировка состава фэнтези, привязанная к конкретной игре.

Правило простое: состав замораживается с началом игры и оттаивает, когда бот
отправил в чат её результат. Между играми — даже внутри одной недели — состав
можно менять. Поэтому очки за каждую игру фиксируются в момент результата
(fantasy.record_game_scores), иначе смена состава уводила бы уже начисленное.

Даты игр бот знает заранее — он сам создаёт записи опросов/анонсов, а на
результат пишет запись РЕЗУЛЬТАТ. Отсюда и берём и время начала, и факт
завершения: service_records, обе лиги, без сети.
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

# Сколько держать состав закрытым, если результат так и не пришёл (бот лежал,
# игру отменили). Без этого потолка одна потерянная запись заморозила бы
# составы навсегда.
MAX_LOCK_HOURS = GAME_TRACKING_WINDOW_HOURS

# Во сколько выходит анонс «игра сегодня». Оставлено ради совместимости вызова
# из run_fantasy: на блокировку больше не влияет.
DEFAULT_ANNOUNCE_HHMM = "09:00"


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


def collect_games() -> List[Dict[str, Any]]:
    """Игры нашей команды: когда начало и отправлен ли результат.

    У одной игры несколько записей (опрос, анонс, результат) — схлопываем по
    game_id: время берём самое раннее непустое, а запись РЕЗУЛЬТАТ означает, что
    итог уже ушёл в чат и держать состав закрытым больше незачем."""
    sheets_cache.init_db()
    placeholders = ",".join("?" for _ in SCHEDULE_TYPES)
    by_game: Dict[str, Dict[str, Any]] = {}
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            f"""SELECT game_id, game_date, game_time, data_type FROM service_records
                WHERE deleted = 0 AND data_type IN ({placeholders})""",
            SCHEDULE_TYPES,
        ).fetchall()
    for r in rows:
        dt = _parse_game_dt(r["game_date"], r["game_time"])
        key = str(r["game_id"] or "") or (dt.isoformat() if dt else "")
        if not key:
            continue
        g = by_game.setdefault(key, {"game_id": key, "dt": None, "has_result": False})
        if dt and (g["dt"] is None or dt < g["dt"]):
            g["dt"] = dt
        if str(r["data_type"]).startswith("РЕЗУЛЬТАТ"):
            g["has_result"] = True
    return [g for g in by_game.values() if g["dt"] is not None]


def lock_state(now: Optional[_dt.datetime] = None) -> Dict[str, Any]:
    """Идёт ли сейчас игра, на время которой состав заморожен.

    Считается вживую, а не берётся из кеша расписания: cron тикает раз в 20
    минут, а закрыться состав должен ровно на стартовом свистке."""
    now = now or get_moscow_time()
    for g in sorted(collect_games(), key=lambda x: x["dt"]):
        if g["has_result"]:
            continue
        start = g["dt"]
        if start <= now < start + _dt.timedelta(hours=MAX_LOCK_HOURS):
            return {"locked": True, "game_id": g["game_id"],
                    "started_at": start.isoformat(),
                    "started_hhmm": start.strftime("%H:%M")}
    return {"locked": False, "game_id": None, "started_at": None, "started_hhmm": ""}


def tick(now: Optional[_dt.datetime] = None,
         announce_hhmm: str = DEFAULT_ANNOUNCE_HHMM,
         season: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], List[Tuple[str, str]]]:
    """Обновляет кешированное состояние сезона (для админки и логов).

    Рассылок больше нет: о блокировке игрок узнаёт в момент попытки сменить
    состав, об открытии — из сообщения с результатом игры. Второй элемент
    кортежа (события) всегда пуст — вызов сохранён ради совместимости."""
    if season is None:
        season = fantasy.get_active_season()
    if not season:
        return {"active_week": None, "locked": False}, []

    now = now or get_moscow_time()
    state = lock_state(now)
    week = fantasy.week_start_of(now.date()).isoformat()

    sched = fantasy.get_sched(season)
    sched.update(active_week=week, locked=state["locked"],
                 locked_game=state["game_id"], updated_at=now.isoformat())
    fantasy.set_sched(sched, season["id"])

    return ({"active_week": week, "locked": state["locked"],
             "locked_game": state["game_id"]}, [])
