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


# ─────────────────────────── Составы ─────────────────────────────────────────

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
    d_from, d_to = week_bounds(week_start)
    rosters = get_week_rosters(season_id, week_start)
    table = []
    for r in rosters:
        pts = fantasy_stats.player_points(r["refs"], weights, date_from=d_from, date_to=d_to)
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
