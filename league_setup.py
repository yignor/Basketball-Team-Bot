"""Команды в лигах: за кем бот следит, кроме того, что записано в «Конфиге».

Зачем. Турниры бот берёт из листа «Конфиг», а писать в этот лист ему нельзя:
лист ведёт человек, и бот, который правит его сам, однажды затрёт строку,
которую не понял. Но добавить команду в бота должно быть можно из бота — не
каждый тренер полезет в таблицу, да и id турнира там надо выковыривать из
адресной строки.

Поэтому здесь второй, свой список: id команды в лиге и турниры, в которых она
играет. Бот подмешивает его к «Конфигу» в одном месте — get_full_config, —
и дальше эту команду видят все: опросы, анонсы, результаты, статистика,
фэнтези и зал славы.

**Турнир бот находит сам.** У Инфобаскета игры команды лежат по её id, и в
каждой игре записан турнир. Поднимаемся от игры вверх до уровня, где есть
таблица с тремя и более командами: «Финал за 3 место» — это пара, а не
турнир, следить надо за «4 Лигой», чей календарь включает и плей-офф.

SLPRO так не заводится: там команда задаётся названием и кодом дивизиона, и
это по-прежнему «Конфиг».
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

API = "https://reg.infobasket.su"

SCHEMA = """
CREATE TABLE IF NOT EXISTS extra_league_teams (
    source     TEXT NOT NULL DEFAULT 'infobasket',
    team_id    TEXT NOT NULL,
    name       TEXT NOT NULL DEFAULT '',
    comps_json TEXT NOT NULL DEFAULT '[]',   -- турниры, за которыми следим
    note       TEXT NOT NULL DEFAULT '',
    added_by   TEXT NOT NULL DEFAULT '',
    added_at   TEXT NOT NULL,
    PRIMARY KEY (source, team_id)
);
"""

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


# ─────────────────────────── хранение ───────────────────────────


def saved(source: str = "infobasket") -> List[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM extra_league_teams WHERE source = ? ORDER BY added_at",
            (str(source),))]
    for r in rows:
        try:
            r["comps"] = [int(c) for c in json.loads(r["comps_json"] or "[]")]
        except (ValueError, TypeError):
            r["comps"] = []
    return rows


def add(team_id: Any, name: str = "", comps: Optional[List[Any]] = None,
        added_by: Any = "", source: str = "infobasket") -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO extra_league_teams (source, team_id, name, comps_json,
                                               added_by, added_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(source, team_id) DO UPDATE SET
                   name = excluded.name, comps_json = excluded.comps_json""",
            (str(source), str(team_id), str(name or ""),
             json.dumps(sorted({int(c) for c in (comps or []) if str(c).isdigit()})),
             str(added_by), sheets_cache.now_iso()))
        conn.commit()


def drop(team_id: Any, source: str = "infobasket") -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM extra_league_teams WHERE source = ? AND team_id = ?",
                     (str(source), str(team_id)))
        conn.commit()


def team_ids(source: str = "infobasket") -> List[str]:
    return [r["team_id"] for r in saved(source)]


def comp_ids(source: str = "infobasket") -> List[int]:
    out: Set[int] = set()
    for r in saved(source):
        out.update(r["comps"])
    return sorted(out)


def merge_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Подмешивает добавленные из бота команды к конфигурации из «Конфига».

    Одно место на весь проект: всё, что смотрит на турниры и команды, ходит
    через get_full_config, и подмешивать надо там, иначе половина бота знает
    про команду, а половина нет."""
    try:
        extra = saved()
        closed = {code for (src, code) in closed_comps("infobasket")}
    except Exception as exc:
        logger.warning("Команды из бота не прочитались: %s", exc)
        return payload
    if not extra and not closed:
        return payload
    comps = set(int(c) for c in (payload.get("comp_ids") or []))
    teams_ids = set(int(t) for t in (payload.get("team_ids") or []))
    teams = dict(payload.get("teams") or {})
    for row in extra:
        if not str(row["team_id"]).isdigit():
            continue
        tid = int(row["team_id"])
        teams_ids.add(tid)
        comps.update(row["comps"])
        entry = dict(teams.get(tid) or {})
        merged = sorted(set(entry.get("comp_ids") or []) | set(row["comps"]))
        entry["comp_ids"] = merged
        if row["name"] and not entry.get("alt_name"):
            entry["alt_name"] = row["name"]
        teams[tid] = entry
    # Закрытые соревнования выкидываем последними: бот перестаёт искать в них
    # игры, ставить опросы и считать статистику. Лист «Конфиг» при этом цел —
    # вернуть турнир можно той же кнопкой.
    comps = {c for c in comps if str(c).upper() not in closed}
    for tid, entry in teams.items():
        entry["comp_ids"] = [c for c in (entry.get("comp_ids") or [])
                             if str(c).upper() not in closed]
    payload = dict(payload)
    payload["comp_ids"] = sorted(comps)
    payload["team_ids"] = sorted(teams_ids)
    payload["teams"] = teams
    return payload


# ─────────────────────────── закрытые соревнования ───────────────────────────

# Турниры кончаются, а строки в «Конфиге» остаются: тренер не полезет править
# лист ради прошлогодней квалификации. Закрытое соревнование бот перестаёт
# считать своим — и в списках оно больше не мозолит глаза.

COMPS_SCHEMA = """
CREATE TABLE IF NOT EXISTS closed_comps (
    source    TEXT NOT NULL,
    code      TEXT NOT NULL,          -- comp_id у Инфобаскета, дивизион у SLPRO
    closed_at TEXT NOT NULL,
    set_by    TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (source, code)
);
"""


def _init_comps() -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.executescript(COMPS_SCHEMA)
        conn.commit()


def close_comp(source: str, code: Any, closed: bool = True,
               set_by: Any = "") -> None:
    """Закрывает соревнование или возвращает его. Данные остаются на месте."""
    _init_comps()
    key = (str(source).lower(), str(code).upper())
    with sheets_cache.get_connection() as conn:
        if closed:
            conn.execute(
                """INSERT INTO closed_comps (source, code, closed_at, set_by)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(source, code) DO UPDATE SET
                       closed_at = excluded.closed_at, set_by = excluded.set_by""",
                (*key, sheets_cache.now_iso(), str(set_by)))
        else:
            conn.execute("DELETE FROM closed_comps WHERE source = ? AND code = ?", key)
        conn.commit()


def closed_comps(source: Optional[str] = None) -> Dict[Tuple[str, str], str]:
    """{(источник, код): когда закрыли}."""
    _init_comps()
    sql = "SELECT * FROM closed_comps"
    args: List[Any] = []
    if source:
        sql += " WHERE source = ?"
        args.append(str(source).lower())
    with sheets_cache.get_connection() as conn:
        return {(r["source"], r["code"]): r["closed_at"]
                for r in conn.execute(sql, args)}


def is_comp_closed(source: str, code: Any) -> bool:
    return (str(source).lower(), str(code).upper()) in closed_comps()


# ─────────────────────────── SLPRO ───────────────────────────

# У SLPRO числового id команды на сайте не видно, поэтому турнир задаётся
# кодом дивизиона из адреса и названием команды — как в «Конфиге». Код
# дивизиона кладём в team_id: пара «дивизион + название» и есть здесь ключ.


def slpro_rows() -> List[Dict[str, str]]:
    """Команды SLPRO, заведённые через бота, — в том же виде, что «Конфиг»."""
    out = []
    for row in saved("slpro"):
        out.append({"source": "slpro", "division": str(row["team_id"]).upper(),
                    "team_name": row["name"],
                    "name": row["note"] or f"SLPRO {str(row['team_id']).upper()}"})
    return out


def add_slpro(division: str, team_name: str, title: str = "",
              added_by: Any = "") -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO extra_league_teams (source, team_id, name, comps_json,
                                               note, added_by, added_at)
               VALUES ('slpro', ?, ?, '[]', ?, ?, ?)
               ON CONFLICT(source, team_id) DO UPDATE SET
                   name = excluded.name, note = excluded.note""",
            (str(division).upper().strip(), str(team_name).strip(),
             str(title or "").strip(), str(added_by), sheets_cache.now_iso()))
        conn.commit()


async def discover_slpro(division: str, team_name: str) -> Dict[str, Any]:
    """Есть ли такая команда в таком дивизионе.

    {found: bool, division_name, season, stage_id, team_id, team_name,
     near: [похожие названия]}. Название должно совпадать с тем, что на сайте
     лиги, — поэтому при промахе показываем, кто там вообще есть: вписать
     «PullUp Farm» вместо «Pull Up Farm» проще, чем гадать."""
    import slpro_client
    code = str(division).upper().strip()
    want = slpro_client._normalize_name(team_name)
    out: Dict[str, Any] = {"found": False, "near": [], "division_name": "",
                           "season": "", "team_id": "", "team_name": ""}
    client = slpro_client.SlproClient()
    stages = await client.iter_stages()
    if not stages:
        out["error"] = "лига не ответила — попробуй позже"
        return out
    cands = [st for st in stages if str(st.get("division", "")).upper() == code]
    if not cands:
        out["error"] = (f"дивизиона {code} у лиги нет — код берётся из адреса "
                        "страницы турнира")
        return out
    near: List[str] = []
    for stage in cands:
        for team in await client.get_standings(stage):
            name = str(team.get("name") or "")
            if slpro_client._normalize_name(name) == want:
                out.update(found=True, team_id=str(team.get("team_id") or ""),
                           team_name=name, season=str(stage.get("season") or ""),
                           stage_id=str(stage.get("stage_id") or ""),
                           division_name=str(stage.get("division_name")
                                             or stage.get("division") or ""))
                return out
            if name not in near:
                near.append(name)
    # Отдаём весь дивизион, а не «похожие»: команд там дюжина, своё название
    # тренер узнает глазами. Похожесть тут не работает — «Пул Ап» и «PullUp
    # Farm» для сравнения строк далеки друг от друга, это разные алфавиты.
    out["near"] = sorted(near)[:16]
    out["division_name"] = str(cands[0].get("division_name") or code)
    return out


# ─────────────────────────── что знает лига ───────────────────────────


async def _jget(session, url: str) -> Any:
    try:
        import aiohttp
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
            return await r.json(content_type=None) if r.status == 200 else None
    except Exception as exc:
        logger.warning("Лига не ответила (%s): %s", url[-60:], exc)
        return None


async def _standings_level(session, comp_id: Any, team_id: str) -> Optional[Dict[str, Any]]:
    """От турнира игры поднимаемся до уровня, где есть таблица.

    Игра лежит в «Финале за 3 место» — это пара команд, и следить за ней
    бессмысленно: её календарь кончится завтра. Нужен тот уровень, где команд
    много и идёт турнир, — его календарь включает и плей-офф."""
    cur, seen = str(comp_id), set()
    for _ in range(5):
        if not cur or cur in seen:
            break
        seen.add(cur)
        table = await _jget(session, f"{API}/Widget/CompTeamResults/{cur}?format=json&lang=ru")
        rows = table if isinstance(table, list) else []
        mine = [r for r in rows if str(r.get("TeamID")) == str(team_id)]
        if len(rows) >= 3 and mine:
            issue = await _jget(session, f"{API}/Widget/CompIssue/{cur}?format=json&lang=ru")
            parent = (issue or {}).get("ParentComp") or {}
            up = str(parent.get("CompIDparent") or "")
            # Следим этажом выше: календарь этапа включает и группы, и плей-офф,
            # а значит новые игры найдутся сами, когда команду переставят в
            # другую группу. Именно эти номера тренер и вписывал в «Конфиг»
            # руками (142849, 140825).
            return {"comp_id": int(cur), "teams": len(rows),
                    "place": int(mine[0].get("Place") or 0),
                    "comp": str(parent.get("CompShortNameRu") or ""),
                    "track": int(up) if up.isdigit() else int(cur)}
        issue = await _jget(session, f"{API}/Widget/CompIssue/{cur}?format=json&lang=ru")
        parent = (issue or {}).get("ParentComp") or {}
        cur = str(parent.get("CompIDparent") or "")
    return None


async def discover(team_id: Any) -> Dict[str, Any]:
    """Что лига знает про команду: имя и турниры, в которых она сейчас играет.

    {name, comps: [{comp_id, league, comp, games, last_day, teams, place}]}.
    Берём последний сезон: добавляют команду, чтобы следить за ней дальше, а
    не чтобы перечитать позапрошлый год (для истории есть зал славы)."""
    import aiohttp
    out: Dict[str, Any] = {"name": "", "comps": []}
    async with aiohttp.ClientSession() as session:
        page = await _jget(session, f"{API}/Widget/TeamPage/{team_id}?format=json&lang=ru")
        out["name"] = str((page or {}).get("TeamNameRu") or "")
        seasons = await _jget(session, f"{API}/Widget/GetTeamSeasons/{team_id}?format=json&lang=ru")
        seasons = [s for s in (seasons or []) if s.get("CompID")]
        if not seasons:
            return out
        newest = max(seasons, key=lambda s: int(s.get("Year") or 0))
        games = await _jget(
            session,
            f"{API}/Widget/TeamGames/{team_id}?compId={newest['CompID']}&format=json&lang=ru")
        groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
        for game in games or []:
            key = (str(game.get("LeagueNameRu") or ""), str(game.get("CompNameRu") or ""))
            groups.setdefault(key, []).append(game)
        seen: Set[int] = set()
        # По одной игре из каждой группы: турнир у них общий, а запросов к лиге
        # тем меньше, чем меньше игр мы трогаем.
        for (league, comp_name), items in groups.items():
            newest_game = max(items, key=lambda g: int(g.get("GameDateInt") or 0))
            online = await _jget(
                session,
                f"{API}/Widget/GetOnline/{newest_game.get('GameID')}?format=json&lang=ru")
            comp_id = (online or {}).get("CompID")
            if not comp_id:
                continue
            level = await _standings_level(session, comp_id, str(team_id))
            if not level or level["comp_id"] in seen:
                continue
            seen.add(level["comp_id"])
            days = sorted(str(g.get("GameDate") or "") for g in items)
            out["comps"].append({
                "comp_id": level["comp_id"], "track": level.get("track") or level["comp_id"],
                "league": league,
                "comp": level["comp"] or comp_name, "teams": level["teams"],
                "place": level["place"], "games": len(items),
                "last_day": days[-1] if days else "",
            })
    out["comps"].sort(key=lambda c: c["last_day"][-4:] + c["last_day"][3:5] + c["last_day"][:2],
                      reverse=True)
    return out
