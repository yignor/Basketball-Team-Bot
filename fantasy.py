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
    """(неделя_для_набора ISO, заблокирован ли сейчас).

    Блокировку считаем ВЖИВУЮ по расписанию игр: она должна включаться ровно на
    стартовом свистке, а cron тикает раз в 20 минут. Состав хранится по
    календарным неделям — между играми недели его можно менять, очки за уже
    сыгранное зафиксированы (см. record_game_scores)."""
    week = week_start_of(date.today()).isoformat()
    if not season:
        return week, False
    try:
        import fantasy_schedule           # локально: fantasy_schedule импортирует нас
        return week, bool(fantasy_schedule.lock_state()["locked"])
    except Exception:
        # Расписание недоступно — не запираем игроков на ровном месте.
        return week, False


def lock_details() -> Dict[str, Any]:
    """Подробности текущей блокировки для объяснения игроку (какая игра, с
    какого времени). Пусто — если сейчас не заблокировано."""
    try:
        import fantasy_schedule
        return fantasy_schedule.lock_state()
    except Exception:
        return {"locked": False, "game_id": None, "started_at": None, "started_hhmm": ""}


RESULT_HINT = ("🏆 Фэнтези: состав снова открыт. Он замораживается только на время "
               "игры — собрать новый можно прямо сейчас, до следующего стартового свистка.")


def result_hint() -> str:
    """Напоминание про блокировку состава для сообщения с результатом игры.

    Показываем не чаще раза в неделю: в каждом результате это быстро стало бы
    шумом, который перестают читать. Пусто — если фэнтези сейчас не идёт."""
    season = get_active_season()
    if not season:
        return ""
    sched = get_sched(season)
    today = date.today()
    last = sched.get("hint_shown")
    try:
        if last and (today - date.fromisoformat(last)).days < 7:
            return ""
    except (ValueError, TypeError):
        pass
    sched["hint_shown"] = today.isoformat()
    set_sched(sched, season["id"])
    return RESULT_HINT


def get_roster_effective(user_id: str, season_id: int, week_start: str) -> Optional[Dict[str, Any]]:
    """Состав, который сейчас в игре у участника: за текущую неделю, а если её
    ещё не собирали — унаследованный с прошлой. Состав держится, пока игрок его
    не поменял, иначе новая неделя молча обнуляла бы человека."""
    row = get_roster(user_id, season_id, week_start)
    if row and row.get("refs"):
        return row
    inherited = effective_rosters(season_id, week_start).get(str(user_id))
    if not inherited:
        return row
    return {"user_id": str(user_id), "season_id": season_id, "week_start": week_start,
            "refs": inherited, "locked": 0, "inherited": True}


# ─────────────────────────── Сезоны ──────────────────────────────────────────

def active_seasons() -> List[Dict[str, Any]]:
    """Все активные сезоны. Их может быть несколько одновременно (летний турнир
    и основной сезон идут параллельно) — у каждого свой пул/таблица/окно."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM fantasy_seasons WHERE status = 'active' ORDER BY id DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def get_active_season() -> Optional[Dict[str, Any]]:
    """Последний активный сезон (для одиночного контекста/совместимости).
    Новый код, поддерживающий несколько лиг, ходит через active_seasons()."""
    seasons = active_seasons()
    return seasons[0] if seasons else None


def get_active_by_id(season_id: Any) -> Optional[Dict[str, Any]]:
    """Активный сезон по id (для выбора лиги в Mini App). None, если такого
    активного сезона нет — тогда вызывающий откатывается на последний активный."""
    try:
        sid = int(season_id)
    except (TypeError, ValueError):
        return None
    return next((s for s in active_seasons() if s["id"] == sid), None)


def start_season(name: str, fmt: str = "3x3",
                 weights: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """Создаёт активный сезон. Несколько активных допускаются (параллельные
    лиги) — админ управляет закрытием старых вручную. Идемпотентно по имени:
    если активный сезон с таким названием уже есть, возвращаем его (защита от
    двойного клика «Старт»)."""
    for s in active_seasons():
        if s.get("name") == name:
            return s
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


def norm_player_name(name: str) -> str:
    """Нормализованное ФИО — ключ и склейки карточек, и исключения из пула."""
    return " ".join((name or "").lower().replace("ё", "е").split())


def pool_excluded_names(season: Dict[str, Any]) -> List[str]:
    """Нормализованные ФИО игроков, убранных админом из пула фэнтези.
    Исключаем по имени, а не по id: устойчиво к склейке лиг и смене id."""
    ex = season_settings(season).get("pool_exclude_names")
    return [str(x) for x in ex] if isinstance(ex, list) else []


def is_excluded(season: Dict[str, Any], name: str) -> bool:
    return norm_player_name(name) in set(pool_excluded_names(season))


def toggle_pool_exclude_name(name: str, season_id: Optional[int] = None) -> Tuple[bool, List[str]]:
    """Убирает игрока (по ФИО) из пула или возвращает. (исключён_теперь?, список)."""
    season = get_active_season() if season_id is None else _get_season(season_id)
    if not season:
        return False, []
    key = norm_player_name(name)
    ex = pool_excluded_names(season)
    if key in ex:
        ex = [x for x in ex if x != key]
        now_excluded = False
    else:
        ex = ex + [key]
        now_excluded = True
    _update_settings(season, pool_exclude_names=ex)
    return now_excluded, ex


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


def pool_teams(season: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Команды, чьи ростеры образуют пул фэнтези: [{source, team_id, comp_id?,
    name}]. Пусто → build_pool соберёт дефолт (команды из настроек поиска игр:
    SLPRO-команда + команда Инфобаскета)."""
    tp = season_settings(season).get("pool_teams")
    return [x for x in tp if isinstance(x, dict) and x] if isinstance(tp, list) else []


def set_pool_teams(teams: List[Dict[str, Any]], season_id: Optional[int] = None) -> bool:
    season = get_active_season() if season_id is None else _get_season(season_id)
    return _update_settings(season, pool_teams=list(teams or [])) if season else False


def _team_key(t: Dict[str, Any]) -> Tuple[str, str]:
    return (str(t.get("source", "")), str(t.get("team_id", "")))


def toggle_pool_team(team: Dict[str, Any],
                     season_id: Optional[int] = None) -> Tuple[bool, List[Dict[str, Any]]]:
    season = get_active_season() if season_id is None else _get_season(season_id)
    if not season:
        return False, []
    teams = pool_teams(season)
    key = _team_key(team)
    present = any(_team_key(t) == key for t in teams)
    teams = [t for t in teams if _team_key(t) != key] if present else teams + [team]
    set_pool_teams(teams, season["id"])
    return (not present), teams


def team_in_pool(team: Dict[str, Any], teams: List[Dict[str, Any]]) -> bool:
    key = _team_key(team)
    return any(_team_key(t) == key for t in teams)


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


def season_participants(season_id: int) -> List[str]:
    """Все, кто хоть раз собирал состав в этом сезоне — аудитория личных
    уведомлений о наборе."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT user_id FROM fantasy_rosters WHERE season_id=?", (season_id,)
        ).fetchall()
    return [r["user_id"] for r in rows]


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


def apply_game_result(source: str, game_id: Any, game_date_iso: str) -> Dict[str, Any]:
    """Вызывается ПОСЛЕ сохранения box-score сыгранной игры (по результату в
    чат). Сверяет игроков матча с составами фэнтези и пересчитывает очки той
    недели по всем активным лигам (обновляет кеш недельных очков). Таблица и
    так живая, но так кеш и понедельничная рассылка сразу консистентны.
    Возвращает сводку: кого из участников затронула игра."""
    sheets_cache.init_db()
    try:
        wk = week_start_of(date.fromisoformat(game_date_iso)).isoformat()
    except (ValueError, TypeError):
        wk = week_start_of(date.today()).isoformat()
    with sheets_cache.get_connection() as conn:
        played = {str(r["player_id"]) for r in conn.execute(
            "SELECT player_id FROM game_player_stats WHERE source=? AND game_id=?",
            (source, str(game_id)))}
    out: List[Dict[str, Any]] = []
    for season in active_seasons():
        # Фиксируем очки ИМЕННО за эту игру, пока состав ещё заблокирован ею:
        # дальше он разморозится, и пересчитать «как было» будет уже нельзя.
        record_game_scores(season, source, game_id, game_date_iso)
        save_weekly_scores(season["id"], wk)          # обновляем кеш недели
        affected = []
        for r in get_week_rosters(season["id"], wk):
            for ref in fantasy_stats.expand_refs(r["refs"]):
                src, pid = fantasy_stats.parse_ref(ref)
                if src == source and pid in played:
                    affected.append(str(r["user_id"]))
                    break
        out.append({"season_id": season["id"], "affected": affected})
    return {"week": wk, "played": len(played), "seasons": out}


def effective_rosters(season_id: int, at_date_iso: str) -> Dict[str, List[str]]:
    """Составы участников, действовавшие на указанную дату: для каждого — его
    последний состав, собранный НЕ ПОЗЖЕ этой даты. Состав держится, пока игрок
    его не поменял, поэтому неделя без пересборки не обнуляет участника."""
    sheets_cache.init_db()
    try:
        week = week_start_of(date.fromisoformat(at_date_iso)).isoformat()
    except (ValueError, TypeError):
        # Бэкфилл идёт по датам из базы — одна битая строка не должна валить всё.
        week = week_start_of(date.today()).isoformat()
    out: Dict[str, List[str]] = {}
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT user_id, player_refs_json, week_start FROM fantasy_rosters
               WHERE season_id = ? AND week_start <= ?
               ORDER BY week_start""", (season_id, week)).fetchall()
    for r in rows:
        try:
            refs = json.loads(r["player_refs_json"]) or []
        except (json.JSONDecodeError, TypeError):
            refs = []
        if refs:
            out[str(r["user_id"])] = refs      # порядок ASC -> остаётся свежайший
    return out


def record_game_scores(season: Dict[str, Any], source: str, game_id: Any,
                       game_date_iso: str, inherit: bool = True) -> List[Dict[str, Any]]:
    """Фиксирует очки участников за КОНКРЕТНУЮ игру — навсегда.

    Вызывается в момент результата, когда состав ещё заблокирован этой игрой,
    поэтому в снимок попадает именно тот состав, что играл. Пересчитывать потом
    нельзя: состав размораживается после каждой игры, и «текущим» составом очки
    уехали бы задним числом. Повторный вызов по той же игре ничего не меняет.

    inherit=False — считать строго по составу той недели, без переноса с
    предыдущей. Так достраивается история: до перехода на поигровую модель
    несобранная неделя означала ноль, и задним числом менять это нельзя."""
    sheets_cache.init_db()
    weights = season_weights(season)
    if inherit:
        rosters = effective_rosters(season["id"], game_date_iso)
    else:
        try:
            wk = week_start_of(date.fromisoformat(game_date_iso)).isoformat()
        except (ValueError, TypeError):
            wk = week_start_of(date.today()).isoformat()
        rosters = {str(r["user_id"]): r["refs"] for r in get_week_rosters(season["id"], wk)}
    out: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        for uid, refs in rosters.items():
            pts = fantasy_stats.game_points(refs, source, game_id, weights)
            conn.execute(
                """INSERT INTO fantasy_game_scores
                   (user_id, season_id, source, game_id, game_date, points, refs_json, computed_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(user_id, season_id, source, game_id) DO NOTHING""",
                (uid, season["id"], source, str(game_id), game_date_iso, pts,
                 json.dumps(refs, ensure_ascii=False), sheets_cache.now_iso()))
            out.append({"user_id": uid, "points": pts})
        conn.commit()
    return out


def backfill_game_scores(season: Dict[str, Any]) -> int:
    """Достраивает снимки по играм, которых ещё нет в fantasy_game_scores:
    игры до перехода на поигровую фиксацию и те, чей результат бот пропустил
    (статистика приехала ночным бэкфиллом). Уже зафиксированное не трогает.

    Считает строго по составу той недели (inherit=False), чтобы сложившийся
    зачёт не поехал: раньше несобранная неделя давала ноль, и переносить в неё
    состав задним числом нельзя."""
    sheets_cache.init_db()
    scope_sql, scope_params = fantasy_stats.scope_where(effective_scopes(season))
    with sheets_cache.get_connection() as conn:
        games = conn.execute(
            f"""SELECT DISTINCT source, game_id, game_date FROM game_player_stats
                WHERE game_date != ''{scope_sql}""", scope_params).fetchall()
        done = {(r["source"], r["game_id"]) for r in conn.execute(
            "SELECT DISTINCT source, game_id FROM fantasy_game_scores WHERE season_id = ?",
            (season["id"],))}
    added = 0
    for g in games:
        if (g["source"], g["game_id"]) in done:
            continue
        record_game_scores(season, g["source"], g["game_id"], g["game_date"], inherit=False)
        added += 1
    return added


def season_standings_live(season_id: int) -> List[Dict[str, Any]]:
    """Живая таблица лиги: сумма зафиксированных очков участника за все игры +
    разбивка по играм (для истории при тапе). Считается из снимков, поэтому
    прошлые игры уже не пересчитываются — только прибавляются новые."""
    sheets_cache.init_db()
    season = _get_season(season_id)
    if season:
        backfill_game_scores(season)      # самолечение: подхватываем пропущенное
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT user_id, game_date, points FROM fantasy_game_scores
               WHERE season_id = ? ORDER BY game_date""", (season_id,)).fetchall()
    totals: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        uid = str(r["user_id"])
        e = totals.setdefault(uid, {"user_id": uid, "points": 0.0, "history": []})
        e["points"] = round(e["points"] + float(r["points"] or 0), 2)
        e["history"].append({"label": _game_label(r["game_date"]),
                             "week": r["game_date"], "points": r["points"]})
    return sorted(totals.values(), key=lambda x: x["points"], reverse=True)


def _game_label(game_date: str) -> str:
    """«2026-07-12» -> «Игра 12.07» — подпись строки истории."""
    try:
        d = date.fromisoformat(game_date)
        return f"Игра {d.day:02d}.{d.month:02d}"
    except (ValueError, TypeError):
        return "Игра"


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
