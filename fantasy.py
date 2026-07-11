#!/usr/bin/env python3
"""
Логика фэнтези-лиги: сезоны, составы (по игровым неделям, с блокировкой),
недельная таблица, финал сезона.

Хранение — локальный SQLite (таблицы fantasy_seasons/fantasy_rosters/
fantasy_weekly_scores из sheets_cache.SCHEMA). Очки считает fantasy_stats.

Юр-инвариант: составы хранят только ссылки на игроков вида
source:team:player_id (без ФИО).
"""

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache
import fantasy_stats


# ─────────────────────────── Недели ──────────────────────────────────────────

def week_start_of(d: date) -> date:
    """Понедельник недели, к которой относится дата."""
    return d - timedelta(days=d.weekday())


def week_bounds(week_start: str) -> Tuple[str, str]:
    """(понедельник, воскресенье) ISO для строки week_start (ISO понедельник)."""
    start = date.fromisoformat(week_start)
    return start.isoformat(), (start + timedelta(days=6)).isoformat()


# ─────────────── Окно набора состава (привязка к расписанию) ─────────────────
# Набор состава открыт не по календарю, а по играм: закрывается на первом анонсе
# «игра сегодня» недели и открывается на следующую неделю после статистики по
# последней игре. Состояние ведёт fantasy_schedule.py, здесь — только чтение.

def get_sched(season: Dict[str, Any]) -> Dict[str, Any]:
    s = season_settings(season).get("sched")
    return s if isinstance(s, dict) else {}


def set_sched(sched: Dict[str, Any], season_id: Optional[int] = None) -> bool:
    season = get_active_season() if season_id is None else _get_season(season_id)
    return _update_settings(season, sched=sched) if season else False


def active_selection(season: Optional[Dict[str, Any]]) -> Tuple[str, bool]:
    """(неделя_для_набора ISO, заблокирован ли). Пока расписание не посчитано —
    календарная неделя, открыта (обратная совместимость)."""
    if not season:
        return week_start_of(date.today()).isoformat(), False
    sched = get_sched(season)
    week = sched.get("active_week") or week_start_of(date.today()).isoformat()
    return week, bool(sched.get("locked"))


# ─────────────────────────── Сезоны ──────────────────────────────────────────

def get_active_season() -> Optional[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM fantasy_seasons WHERE status = 'active' ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def start_season(name: str, fmt: str = "3x3",
                 weights: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """Создаёт активный сезон. Если уже есть активный — возвращает его
    (нельзя два активных одновременно)."""
    existing = get_active_season()
    if existing:
        return existing
    sheets_cache.init_db()
    scoring_json = json.dumps(weights or fantasy_stats.DEFAULT_WEIGHTS, ensure_ascii=False)
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            """INSERT INTO fantasy_seasons (name, format, status, scoring_json, started_at)
               VALUES (?, ?, 'active', ?, ?)""",
            (name, fmt, scoring_json, sheets_cache.now_iso()),
        )
        conn.commit()
        sid = cur.lastrowid
        row = conn.execute("SELECT * FROM fantasy_seasons WHERE id = ?", (sid,)).fetchone()
    return dict(row)


def end_season(season_id: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """Завершает активный (или указанный) сезон, возвращает итоговую тройку."""
    sheets_cache.init_db()
    season = None
    with sheets_cache.get_connection() as conn:
        if season_id is None:
            row = conn.execute(
                "SELECT * FROM fantasy_seasons WHERE status = 'active' ORDER BY id DESC LIMIT 1"
            ).fetchone()
        else:
            row = conn.execute("SELECT * FROM fantasy_seasons WHERE id = ?", (season_id,)).fetchone()
        if not row:
            return None
        season = dict(row)
        conn.execute(
            "UPDATE fantasy_seasons SET status='ended', ended_at=? WHERE id=?",
            (sheets_cache.now_iso(), season["id"]),
        )
        conn.commit()
    return {"season": season, "standings": season_standings(season["id"])}


def set_format(fmt: str, season_id: Optional[int] = None) -> bool:
    season = get_active_season() if season_id is None else {"id": season_id}
    if not season:
        return False
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE fantasy_seasons SET format=? WHERE id=?", (fmt, season["id"]))
        conn.commit()
    return True


def roster_size(season: Dict[str, Any]) -> int:
    return 5 if str(season.get("format", "3x3")).lower().startswith("5") else 3


def season_weights(season: Dict[str, Any]) -> Dict[str, float]:
    try:
        w = json.loads(season.get("scoring_json") or "")
        return {k: float(v) for k, v in w.items()} if w else fantasy_stats.DEFAULT_WEIGHTS
    except (json.JSONDecodeError, TypeError, ValueError):
        return fantasy_stats.DEFAULT_WEIGHTS


def season_settings(season: Dict[str, Any]) -> Dict[str, Any]:
    try:
        s = json.loads(season.get("settings_json") or "")
        return s if isinstance(s, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def max_per_player(season: Dict[str, Any]) -> int:
    """Сколько раз один игрок может входить в состав. По умолчанию — сколько
    угодно (до размера состава): участник вправе поставить всё на одного, и
    тогда очки этого игрока умножаются на число занятых слотов."""
    size = roster_size(season)
    raw = season_settings(season).get("max_per_player")
    try:
        return max(1, min(int(raw), size))
    except (TypeError, ValueError):
        return size


def _update_settings(season: Dict[str, Any], **changes: Any) -> bool:
    settings = season_settings(season)
    settings.update(changes)
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE fantasy_seasons SET settings_json=? WHERE id=?",
                     (json.dumps(settings, ensure_ascii=False), season["id"]))
        conn.commit()
    return True


def set_max_per_player(n: int, season_id: Optional[int] = None) -> bool:
    season = get_active_season() if season_id is None else _get_season(season_id)
    return _update_settings(season, max_per_player=int(n)) if season else False


def season_scopes(season: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Список турниров подсчёта очков. Команда играет сразу в нескольких лигах
    (напр. SLPRO Летний Кубок + Инфобаскет квалификация), поэтому scope —
    список, а не один турнир. [] — считать по всем (опасно после бэкфилла).

    Обратная совместимость: старый одиночный `scope` оборачиваем в список."""
    s = season_settings(season)
    scopes = s.get("scopes")
    if isinstance(scopes, list):
        return [x for x in scopes if isinstance(x, dict) and x]
    legacy = s.get("scope")
    return [legacy] if isinstance(legacy, dict) and legacy else []


def set_season_scopes(scopes: List[Dict[str, Any]], season_id: Optional[int] = None) -> bool:
    season = get_active_season() if season_id is None else _get_season(season_id)
    if not season:
        return False
    # Чистим legacy-ключ, чтобы не мешал.
    return _update_settings(season, scopes=list(scopes or []), scope={})


def set_auto_scopes(scopes: List[Dict[str, Any]], season_id: Optional[int] = None) -> bool:
    """Кеш лиг «в которых команда играет сейчас» — выводится из настроек поиска
    игр периодической задачей. Используется, когда админ не выбрал турниры явно."""
    season = get_active_season() if season_id is None else _get_season(season_id)
    return _update_settings(season, auto_scopes=list(scopes or [])) if season else False


def effective_scopes(season: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Турниры, по которым реально считаем очки: явный выбор админа, а если его
    нет — авто-лиги (в которых команда участвует сейчас). Пусто → считаем всё
    (историческое поведение)."""
    explicit = season_scopes(season)
    if explicit:
        return explicit
    auto = season_settings(season).get("auto_scopes")
    return [x for x in auto if isinstance(x, dict) and x] if isinstance(auto, list) else []


def _scope_key(sc: Dict[str, Any]) -> Tuple[str, str, str]:
    return (str(sc.get("source", "")), str(sc.get("season_id", "")), str(sc.get("stage_id", "")))


def toggle_season_scope(scope: Dict[str, Any],
                        season_id: Optional[int] = None) -> Tuple[bool, List[Dict[str, Any]]]:
    """Добавляет турнир в scope или убирает, если уже есть.
    Возвращает (добавлен?, новый список)."""
    season = get_active_season() if season_id is None else _get_season(season_id)
    if not season:
        return False, []
    scopes = season_scopes(season)
    key = _scope_key(scope)
    present = any(_scope_key(s) == key for s in scopes)
    if present:
        scopes = [s for s in scopes if _scope_key(s) != key]
    else:
        scopes = scopes + [scope]
    set_season_scopes(scopes, season["id"])
    return (not present), scopes


def scope_in(scope: Dict[str, Any], scopes: List[Dict[str, Any]]) -> bool:
    key = _scope_key(scope)
    return any(_scope_key(s) == key for s in scopes)


def scopes_title(scopes: List[Dict[str, Any]]) -> str:
    if not scopes:
        return "⚠️ не задано — считаются все турниры"
    return " + ".join(
        s.get("name") or f"{s.get('source', '?')}/{s.get('season_id', '?')}" for s in scopes)


# ─────────────────────────── Составы ─────────────────────────────────────────

def validate_roster(season: Dict[str, Any], refs: Any,
                    pool_refs: Optional[Any] = None) -> Optional[str]:
    """Проверяет состав по правилам сезона. Возвращает код ошибки или None.
    Одна точка правды: зовут и HTTP-API, и приём состава из Mini App."""
    size = roster_size(season)
    if not isinstance(refs, list) or len(refs) != size:
        return "invalid_roster"
    if pool_refs is not None and any(r not in pool_refs for r in refs):
        return "unknown_player"
    limit = max_per_player(season)
    if limit < size:
        counts: Dict[str, int] = {}
        for r in refs:
            counts[r] = counts.get(r, 0) + 1
        if max(counts.values()) > limit:
            return "too_many_copies"
    return None

def save_roster(user_id: str, season_id: int, week_start: str,
                refs: List[str], lock: bool = False) -> Dict[str, Any]:
    """Сохраняет/обновляет состав участника на неделю. Возвращает статус.
    Если состав уже заблокирован — не даём менять."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        existing = conn.execute(
            "SELECT locked FROM fantasy_rosters WHERE user_id=? AND season_id=? AND week_start=?",
            (str(user_id), season_id, week_start),
        ).fetchone()
        if existing and existing["locked"]:
            return {"ok": False, "error": "locked"}
        conn.execute(
            """INSERT INTO fantasy_rosters (user_id, season_id, week_start, player_refs_json, locked, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(user_id, season_id, week_start) DO UPDATE SET
                   player_refs_json=excluded.player_refs_json, locked=excluded.locked,
                   updated_at=excluded.updated_at""",
            (str(user_id), season_id, week_start, json.dumps(refs, ensure_ascii=False),
             1 if lock else 0, sheets_cache.now_iso()),
        )
        conn.commit()
    return {"ok": True}


def get_roster(user_id: str, season_id: int, week_start: str) -> Optional[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM fantasy_rosters WHERE user_id=? AND season_id=? AND week_start=?",
            (str(user_id), season_id, week_start),
        ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["refs"] = json.loads(d.get("player_refs_json") or "[]")
    return d


def get_week_rosters(season_id: int, week_start: str) -> List[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM fantasy_rosters WHERE season_id=? AND week_start=?",
            (season_id, week_start),
        ).fetchall()
    out = []
    for row in rows:
        d = dict(row)
        d["refs"] = json.loads(d.get("player_refs_json") or "[]")
        out.append(d)
    return out


def lock_week(season_id: int, week_start: str) -> int:
    """Блокирует все составы недели (перед началом игровой недели)."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "UPDATE fantasy_rosters SET locked=1 WHERE season_id=? AND week_start=?",
            (season_id, week_start),
        )
        conn.commit()
    return cur.rowcount


# ─────────────────────────── Таблицы ─────────────────────────────────────────

def weekly_standings(season_id: int, week_start: str) -> List[Dict[str, Any]]:
    """Таблица участников за неделю: [{user_id, points, refs}], по убыванию."""
    season = _get_season(season_id)
    weights = season_weights(season) if season else fantasy_stats.DEFAULT_WEIGHTS
    scopes = effective_scopes(season) if season else []
    d_from, d_to = week_bounds(week_start)
    rosters = get_week_rosters(season_id, week_start)
    table = []
    for r in rosters:
        pts = fantasy_stats.player_points(r["refs"], weights, date_from=d_from, date_to=d_to,
                                          scope=scopes)
        table.append({"user_id": r["user_id"], "points": pts, "refs": r["refs"]})
    table.sort(key=lambda x: x["points"], reverse=True)
    return table


def save_weekly_scores(season_id: int, week_start: str) -> List[Dict[str, Any]]:
    """Считает и кеширует недельные очки (fantasy_weekly_scores)."""
    table = weekly_standings(season_id, week_start)
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        for row in table:
            conn.execute(
                """INSERT INTO fantasy_weekly_scores (user_id, season_id, week_start, points, computed_at)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(user_id, season_id, week_start) DO UPDATE SET
                       points=excluded.points, computed_at=excluded.computed_at""",
                (row["user_id"], season_id, week_start, row["points"], sheets_cache.now_iso()),
            )
        conn.commit()
    return table


def season_standings(season_id: int) -> List[Dict[str, Any]]:
    """Итоговая таблица сезона: сумма недельных очков по участникам."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT user_id, SUM(points) total, COUNT(*) weeks
               FROM fantasy_weekly_scores WHERE season_id=?
               GROUP BY user_id ORDER BY total DESC""",
            (season_id,),
        ).fetchall()
    return [{"user_id": r["user_id"], "points": round(r["total"], 2), "weeks": r["weeks"]} for r in rows]


def _get_season(season_id: int) -> Optional[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM fantasy_seasons WHERE id=?", (season_id,)).fetchone()
    return dict(row) if row else None


# ─────────────────────────── Имена и форматирование ──────────────────────────

def display_names(user_ids: List[str]) -> Dict[str, str]:
    """Числовой Telegram ID -> отображаемое имя (транзитно; в таблицах фэнтези
    ФИО не храним, только показываем).

    В листе «Игроки» колонка «Telegram ID» заполнена @юзернеймами, а составы
    хранятся по числовому id. Поэтому связываем через bot_users (там есть
    numeric -> username), с запасными вариантами."""
    ids = [str(u) for u in user_ids]
    if not ids:
        return {}
    sheets_cache.init_db()
    placeholders = ",".join("?" * len(ids))
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT b.telegram_id AS tid,
                   COALESCE(p.surname, '')   AS surname,
                   COALESCE(p.name, '')      AS name,
                   COALESCE(p.nickname, '')  AS nickname,
                   COALESCE(b.first_name, '') AS first_name,
                   COALESCE(b.username, '')   AS username
            FROM bot_users b
            LEFT JOIN players p
                   ON p.telegram_id = b.telegram_id
                   OR (b.username != '' AND lower(ltrim(p.telegram_id, '@')) = lower(b.username))
            WHERE b.telegram_id IN ({placeholders})
            """,
            ids,
        ).fetchall()
    out: Dict[str, str] = {}
    for r in rows:
        out[str(r["tid"])] = (f"{r['surname']} {r['name']}".strip()
                              or r["nickname"] or r["first_name"]
                              or (f"@{r['username']}" if r["username"] else "")
                              or str(r["tid"]))
    # Кого не нашли в bot_users — прямая попытка по players (если там числовой id)
    missing = [i for i in ids if i not in out]
    if missing:
        ph = ",".join("?" * len(missing))
        with sheets_cache.get_connection() as conn:
            for r in conn.execute(
                f"SELECT telegram_id, surname, name, nickname FROM players WHERE telegram_id IN ({ph})",
                missing,
            ).fetchall():
                out[str(r["telegram_id"])] = (f"{r['surname']} {r['name']}".strip()
                                              or r["nickname"] or str(r["telegram_id"]))
    return out


_MEDALS = ["🥇", "🥈", "🥉"]


def format_weekly_table(season_id: int, week_start: str) -> str:
    """Текст недельной таблицы для чата/лички."""
    season = _get_season(season_id)
    table = weekly_standings(season_id, week_start)
    d_from, d_to = week_bounds(week_start)
    names = display_names([r["user_id"] for r in table])
    header = f"🏆 Фэнтези — итоги недели {d_from} – {d_to}"
    if season:
        header = f"🏆 Фэнтези «{season['name']}» — неделя {d_from} – {d_to}"
    if not table:
        return header + "\n\nНа этой неделе никто не набрал состав."
    lines = [header, ""]
    for i, r in enumerate(table):
        place = _MEDALS[i] if i < 3 else f"{i + 1}."
        name = names.get(str(r["user_id"]), f"Участник {r['user_id']}")
        lines.append(f"{place} {name} — {r['points']:g}")
    return "\n".join(lines)


def format_season_final(season_id: int) -> str:
    """Текст итогов сезона с тройкой победителей."""
    season = _get_season(season_id)
    table = season_standings(season_id)
    names = display_names([r["user_id"] for r in table])
    title = season["name"] if season else "Фэнтези"
    lines = [f"🏁 Сезон «{title}» завершён! Итоги:", ""]
    if not table:
        return f"🏁 Сезон «{title}» завершён. Участников нет."
    for i, r in enumerate(table):
        place = _MEDALS[i] if i < 3 else f"{i + 1}."
        name = names.get(str(r["user_id"]), f"Участник {r['user_id']}")
        lines.append(f"{place} {name} — {r['points']:g} очк за {r['weeks']} нед")
    lines += ["", "Поздравляем призёров! 🎉"]
    return "\n".join(lines)
