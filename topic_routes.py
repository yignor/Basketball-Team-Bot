"""Куда что писать: топик для каждого вида сообщений, отдельно по лигам.

Зачем. В «Конфиге» у каждого автосообщения один топик на всё: опросы на игру
идут в один топик, чья бы это ни была лига. Пока лига одна, так и надо. Но
команда играет в нескольких сразу, и тренер хочет развести: НБЛ — в свой
топик, летняя — в свой, результаты — в третий.

Здесь эта настройка живёт для каждой пары «вид сообщения + лига». Порядок
поиска такой: правило для этой лиги → общее правило из бота → то, что стоит в
«Конфиге». Ничего не задал — всё работает ровно как раньше: пустая таблица
здесь не меняет в боте ничего.

Ноль значит «в общий чат, без топика» — это осознанный выбор тренера, и он
отличается от «не задано»: строки просто нет.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS topic_routes (
    kind       TEXT NOT NULL,          -- вид сообщения (GAME_POLLS и т.д.)
    scope      TEXT NOT NULL DEFAULT '', -- лига; пусто — общее правило
    topic_id   INTEGER NOT NULL,       -- 0 = общий чат
    set_by     TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (kind, scope)
);
"""

# Виды сообщений, которые можно развести по лигам. Дни рождения и опросы
# тренировок сюда не входят намеренно: они не про лигу.
KINDS: List[Tuple[str, str]] = [
    ("GAME_POLLS", "Опросы на игру"),
    ("GAME_ANNOUNCEMENTS", "Анонсы игр"),
    ("GAME_UPDATES", "Изменения в расписании"),
    ("GAME_RESULTS", "Результаты игр"),
    ("GAME_VIDEO", "Трансляции и записи"),
    ("ROSTER", "Состав на игру"),
    ("CALENDAR_EVENTS", "Календарь игры (.ics)"),
    ("FANTASY", "Фэнтези"),
    ("VOTING_POLLS", "Опросы тренировок"),
    ("BIRTHDAY_NOTIFICATIONS", "Дни рождения"),
]
KIND_NAMES = dict(KINDS)

# Что имеет смысл разводить по лигам, а что нет: тренировки и дни рождения к
# турниру отношения не имеют, у них только общее правило.
LEAGUE_KINDS = ("GAME_POLLS", "GAME_ANNOUNCEMENTS", "GAME_UPDATES",
                "GAME_RESULTS", "GAME_VIDEO", "ROSTER", "CALENDAR_EVENTS")


def kinds_for(scope: str) -> List[Tuple[str, str]]:
    """Виды сообщений, которые можно настроить в этой области."""
    if not scope:
        return list(KINDS)
    return [(k, n) for k, n in KINDS if k in LEAGUE_KINDS]

GENERAL = ""          # правило без лиги — «для всех»
TO_CHAT = 0           # «в общий чат, без топика»

_ready = False


def init() -> None:
    global _ready
    if _ready:
        return
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.executescript(SCHEMA)
        conn.commit()
    _ready = True


def scope_of(source: str, comp: Any) -> str:
    """Ключ лиги: «infobasket:142849», «slpro:SUMC». Пусто — лига неизвестна.

    Ключ строим из того, что известно в момент отправки: у Инфобаскета это
    номер турнира у самой игры, у SLPRO — код дивизиона."""
    code = str(comp or "").strip().upper()
    return f"{str(source).strip().lower()}:{code}" if code and source else ""


def scope_of_game(source: str, game_id: Any) -> str:
    """Лига конкретной игры — по справочнику матчей.

    У Инфобаскета турнир записан у самой игры, у SLPRO турнир задаётся
    дивизионом, и он лежит в контексте нашей команды."""
    import sheets_cache as sc
    src = str(source or "").lower()
    try:
        sc.init_db()
        if src == "slpro":
            import json
            with sc.get_connection() as conn:
                row = conn.execute(
                    "SELECT ctx_json FROM league_teams WHERE source = 'slpro' "
                    "AND ours = 1 AND ctx_json != '' LIMIT 1").fetchone()
            if row:
                ctx = json.loads(row["ctx_json"])
                return scope_of("slpro", ctx.get("division"))
            return ""
        gid = str(game_id)
        gid = gid.split("-", 1)[1] if gid.startswith("slpro-") else gid
        with sc.get_connection() as conn:
            row = conn.execute(
                "SELECT season_id FROM game_meta WHERE source = ? AND game_id = ?",
                (src, gid)).fetchone()
        return scope_of(src, (row or {"season_id": ""})["season_id"]) if row else ""
    except Exception as exc:
        logger.warning("Лига игры %s:%s не определилась: %s", source, game_id, exc)
        return ""


# ─────────────────────────── правила ───────────────────────────


def set_route(kind: str, scope: str, topic_id: Optional[int],
              set_by: Any = "") -> None:
    """Ставит правило. topic_id None — убрать правило (вернуться к «Конфигу»)."""
    init()
    with sheets_cache.get_connection() as conn:
        if topic_id is None:
            conn.execute("DELETE FROM topic_routes WHERE kind = ? AND scope = ?",
                         (str(kind), str(scope or "")))
        else:
            conn.execute(
                """INSERT INTO topic_routes (kind, scope, topic_id, set_by, updated_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(kind, scope) DO UPDATE SET
                       topic_id = excluded.topic_id, set_by = excluded.set_by,
                       updated_at = excluded.updated_at""",
                (str(kind), str(scope or ""), int(topic_id), str(set_by),
                 sheets_cache.now_iso()))
        conn.commit()


def route(kind: str, scope: str = GENERAL) -> Optional[int]:
    """Правило ровно для этой пары. None — правила нет."""
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT topic_id FROM topic_routes WHERE kind = ? AND scope = ?",
            (str(kind), str(scope or ""))).fetchone()
    return int(row["topic_id"]) if row else None


def routes() -> List[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM topic_routes ORDER BY scope, kind")]


def topic_for(kind: str, scope: str = GENERAL,
              default: Optional[int] = None) -> Optional[int]:
    """Топик для этого сообщения. None — общий чат.

    Лига → общее правило → «Конфиг». Ноль в правиле означает «общий чат»:
    тренер мог намеренно вывести сообщения лиги из топика."""
    try:
        for key in ([scope] if scope else []) + [GENERAL]:
            found = route(kind, key)
            if found is not None:
                return None if int(found) == TO_CHAT else int(found)
    except Exception as exc:
        logger.warning("Маршруты топиков не прочитались: %s", exc)
    return default


def source_of(kind: str, scope: str = GENERAL) -> str:
    """Откуда взялся топик — чтобы экран не врал тренеру."""
    if route(kind, scope) is not None and scope:
        return "правило лиги"
    if route(kind, GENERAL) is not None:
        return "общее правило"
    return "из «Конфига»"


def describe(topic_id: Optional[int]) -> str:
    return "общий чат" if topic_id is None else f"топик {topic_id}"
