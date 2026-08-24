#!/usr/bin/env python3
"""Статистика из протокола PDF — в базу, не дожидаясь лиги.

Сверено: разобранный протокол игры 22.08.2026 совпал с тем, что позже отдал
API, по всем 323 значениям. Значит цифрам можно верить.

Чего здесь боимся. Эта статистика питает фэнтези, и приписать её не тому
игроку — значит начислить ему чужие очки, а потом не понять, откуда они. Поэтому:

* игрока опознаём по ФИО и ТОЛЬКО при однозначном совпадении. Номер в помощь не
  идёт: в заявках лиг он у половины пуст, а у двоих бывает одинаков;
* игру, по которой статистика уже есть, не трогаем вовсе — данные лиги
  главнее наших разобранных;
* неопознанных не выдумываем, а показываем тренеру поимённо.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# Что кладём в базу: ключ протокола -> колонка статистики.
FIELDS = {
    "pts": "pts", "fgm": "fgm", "fga": "fga", "fg3m": "tpm", "fg3a": "tpa",
    "ftm": "ftm", "fta": "fta", "ast": "ast", "stl": "stl", "blk": "blk",
    "reb_off": "reb_off", "reb_def": "reb_def", "reb": "reb", "tur": "tur",
    "pf": "pf", "foul_on": "foul_on", "secs": "secs", "plus_minus": "plus_minus",
}


def _norm(name: str) -> str:
    return " ".join(str(name or "").lower().replace("ё", "е").split())


def _same(a: str, b: str) -> bool:
    """Одно ФИО с точностью до порядка слов. Опечатки НЕ прощаем.

    В фэнтези расхождение в букву прощается — там ошибка стоит неверной
    подписи. Здесь она стоит чужих очков, и цена ошибки другая."""
    pa, pb = sorted(_norm(a).split()), sorted(_norm(b).split())
    return bool(pa) and pa == pb


def find_game(parsed: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Игра, к которой относится протокол: по дате и названиям команд."""
    when = _date(parsed.get("date"))
    if not when:
        return None
    teams = [_norm(t) for t in (parsed.get("teams") or [])]
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT source, game_id, game_date, home_name, guest_name "
            "FROM game_meta WHERE game_date = ?", (when.isoformat(),))]
    for g in rows:
        names = {_norm(g["home_name"]), _norm(g["guest_name"])}
        if not teams or any(t in names for t in teams):
            return g
    # Протокол может приехать раньше, чем лига заведёт игру у нас: тогда
    # искать нечего, и это не ошибка — просто ещё рано.
    return rows[0] if len(rows) == 1 else None


def _date(text: Any) -> Optional[date]:
    for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(text or ""), fmt).date()
        except ValueError:
            continue
    return None


def already_has_stats(source: str, game_id: str) -> int:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM game_player_stats "
            "WHERE source = ? AND game_id = ?", (source, str(game_id))).fetchone()
    return int(row["n"] or 0)


def match(rows: List[Dict[str, Any]], source: str) -> Tuple[List[Dict[str, Any]],
                                                            List[str]]:
    """(опознанные, имена неопознанных). Двойников не берём — там гадание."""
    import player_names
    known: Dict[str, List[str]] = {}
    for key, name in player_names.get_all().items():
        src, _, pid = str(key).partition(":")
        if src == source and pid:
            known.setdefault(_norm(name), []).append(pid)

    with sheets_cache.get_connection() as conn:
        teams = {str(r["player_id"]): str(r["team_id"]) for r in conn.execute(
            "SELECT player_id, team_id FROM league_rosters WHERE source = ?",
            (source,))}

    ok: List[Dict[str, Any]] = []
    lost: List[str] = []
    for row in rows:
        hits = [pid for name, ids in known.items() if _same(name, row["name"])
                for pid in ids]
        if len(set(hits)) != 1:
            lost.append(row["name"] + ("" if hits else " (нет в лиге)"))
            continue
        pid = hits[0]
        ok.append({**row, "player_id": pid, "team_id": teams.get(pid, "")})
    return ok, lost


def store(source: str, game_id: str, game_date: str,
          rows: List[Dict[str, Any]]) -> int:
    """Кладёт статистику. Уже имеющиеся строки НЕ трогает.

    INSERT OR IGNORE намеренно: если лига успела прислать своё, её данные
    главнее — они первичны, а наши разобраны из бумаги."""
    if not rows:
        return 0
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    cols = ["source", "game_id", "game_date", "player_id", "team_id", "number",
            "fetched_at"] + list(FIELDS.values())
    marks = ",".join("?" * len(cols))
    payload = []
    for r in rows:
        line = [source, str(game_id), game_date, str(r["player_id"]),
                str(r.get("team_id") or ""), str(r.get("number") or ""), now]
        line += [int(r.get(key) or 0) for key in FIELDS]
        payload.append(line)
    with sheets_cache.get_connection() as conn:
        cur = conn.executemany(
            f"INSERT OR IGNORE INTO game_player_stats ({','.join(cols)}) "
            f"VALUES ({marks})", payload)
        conn.commit()
    logger.info("Из протокола PDF записано строк статистики: %s (игра %s:%s)",
                cur.rowcount, source, game_id)
    return int(cur.rowcount or 0)
