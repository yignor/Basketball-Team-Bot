"""Зал славы: где команда играла и какие места занимала.

Зачем. Сезон заканчивается, лига закрывается, и место в турнире остаётся
только в чьей-то памяти и в переписке. Здесь оно записано: лига, сезон, место
из скольких, фото с награждения. Тренер закрывает лигу — бот тут же спрашивает
про место и предлагает то, что нашёл в таблице.

**Место подтверждает человек.** У Инфобаскета место лежит в таблице готовым, и
ему можно верить. У SLPRO в ответе только победы и очки, а порядок при
равенстве решает личная встреча — бот считает по очкам и честно говорит, что
это предположение. Ставить в зал славы догадку молча нельзя: «второе вместо
первого» тренер запомнит надолго.

**Команда — по корню имени.** За годы она звалась «PULL UP», «PullUp Farm»,
«Pull-Up»; в старых турнирах будет ещё как-нибудь. Сверяем по корню, выбросив
регистр, пробелы и дефисы, — id команды в лигах тоже менялись.

Фото с награждения ни SLPRO, ни Инфобаскет наружу не отдают (проверено
11.09.2026: у SLPRO нет ни одного роута с новостями и галереями, у Инфобаскета
из картинок только логотипы и портреты). Поэтому фото присылает тренер, а бот
хранит его file_id — картинка уже лежит у Телеграма, второй раз её хранить
незачем.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS league_results (
    source      TEXT NOT NULL,
    season_id   TEXT NOT NULL,
    stage_id    TEXT NOT NULL DEFAULT '',
    team_id     TEXT NOT NULL,
    league      TEXT NOT NULL DEFAULT '',   -- как турнир назван в лиге
    season      TEXT NOT NULL DEFAULT '',   -- «2025-2026», «Сезон 2025/2026»
    team_name   TEXT NOT NULL DEFAULT '',   -- как команда называлась тогда
    place       INTEGER NOT NULL DEFAULT 0, -- 0 — место ещё не записано
    teams       INTEGER NOT NULL DEFAULT 0, -- из скольких
    wins        INTEGER NOT NULL DEFAULT 0,
    losses      INTEGER NOT NULL DEFAULT 0,
    last_day    TEXT NOT NULL DEFAULT '',   -- дата последней игры, для порядка
    photo_id    TEXT NOT NULL DEFAULT '',   -- file_id фото с награждения
    note        TEXT NOT NULL DEFAULT '',
    guess       INTEGER NOT NULL DEFAULT 0, -- место посчитано ботом, не подтверждено
    set_by      TEXT NOT NULL DEFAULT '',
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (source, season_id, stage_id, team_id)
);
"""

# Корни названия команды. «Фарм» — вторая команда, она тоже наша.
ROOTS = ("pullup", "пулап", "пуллап")

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


def _key(name: str) -> str:
    """«Pull-Up Farm» → «pullupfarm»: сравнивать имена можно только так."""
    low = str(name or "").lower().replace("ё", "е")
    return re.sub(r"[^a-zа-я0-9]+", "", low)


def is_ours(name: str) -> bool:
    """Наша ли это команда — по корню имени, а не по точному совпадению."""
    key = _key(name)
    return any(root in key for root in ROOTS)


# ─────────────────────────── хранение ───────────────────────────


def save(source: str, season_id: Any, stage_id: Any, team_id: Any,
         **fields: Any) -> None:
    """Заводит запись турнира или дополняет её. Пустое поле не затирает."""
    init()
    keys = ("league", "season", "team_name", "place", "teams", "wins", "losses",
            "last_day", "photo_id", "note", "guess", "set_by")
    vals = {k: fields[k] for k in keys if k in fields}
    ident = (str(source), str(season_id), str(stage_id or ""), str(team_id))
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO league_results (source, season_id, stage_id, "
            "team_id, updated_at) VALUES (?, ?, ?, ?, ?)",
            (*ident, sheets_cache.now_iso()))
        if vals:
            sets = ", ".join(f"{k} = ?" for k in vals)
            conn.execute(
                f"UPDATE league_results SET {sets}, updated_at = ? WHERE source = ? "
                "AND season_id = ? AND stage_id = ? AND team_id = ?",
                (*vals.values(), sheets_cache.now_iso(), *ident))
        conn.commit()


def get(source: str, season_id: Any, stage_id: Any,
        team_id: Any) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM league_results WHERE source = ? AND season_id = ? "
            "AND stage_id = ? AND team_id = ?",
            (str(source), str(season_id), str(stage_id or ""), str(team_id))).fetchone()
    return dict(row) if row else None


def forget(source: str, season_id: Any, stage_id: Any, team_id: Any) -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "DELETE FROM league_results WHERE source = ? AND season_id = ? "
            "AND stage_id = ? AND team_id = ?",
            (str(source), str(season_id), str(stage_id or ""), str(team_id)))
        conn.commit()


def results() -> List[Dict[str, Any]]:
    """Всё записанное, свежее сверху. Турниры без места — тоже: их видно, что
    они ждут, и тренер допишет место одной кнопкой."""
    init()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute("SELECT * FROM league_results")]
    rows.sort(key=lambda r: (str(r["last_day"] or ""), str(r["season"] or "")),
              reverse=True)
    return rows


MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


def title(row: Dict[str, Any]) -> str:
    """Строка турнира в зале славы."""
    place = int(row.get("place") or 0)
    mark = MEDALS.get(place, "🏀" if place else "▫️")
    where = str(row.get("league") or "").strip() or "турнир"
    season = str(row.get("season") or "").strip()
    tail = f" · {season}" if season else ""
    if not place:
        return f"{mark} {where}{tail} — место не записано"
    out = f"{mark} {where}{tail} — {place} место"
    if int(row.get("teams") or 0):
        out += f" из {int(row['teams'])}"
    if int(row.get("guess") or 0):
        out += " (бот посчитал, не подтверждено)"
    return out


# ─────────────────────────── что говорит лига ───────────────────────────


async def infobasket_table(comp_id: Any) -> List[Dict[str, Any]]:
    """Таблица турнира Инфобаскета: [{team_id, name, place, wins, losses}].

    Место лига считает сама (поле Place) — это тот же номер, что человек
    видит на сайте, и выдумывать его заново незачем."""
    import aiohttp
    url = (f"https://reg.infobasket.su/Widget/CompTeamResults/{comp_id}"
           "?format=json&lang=ru")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
                if r.status != 200:
                    return []
                data = await r.json(content_type=None)
    except Exception as exc:
        logger.warning("Зал славы: таблица Инфобаскета %s — %s", comp_id, exc)
        return []
    out = []
    for row in data if isinstance(data, list) else []:
        names = row.get("CompTeamName") or {}
        out.append({
            "team_id": str(row.get("TeamID") or ""),
            "name": str(names.get("CompTeamNameRu") or names.get("CompTeamShortNameRu") or ""),
            "place": int(row.get("Place") or 0),
            "wins": int(row.get("Win") or row.get("Wins") or 0),
            "losses": int(row.get("Loss") or row.get("Losses") or 0),
        })
    return out


def _slpro_places(teams: List[Dict[str, Any]]) -> Dict[str, Tuple[int, bool]]:
    """{team_id: (место, делят ли его)} по очкам и победам.

    Порядка SLPRO не присылает: в ответе просто список с победами и очками.
    Считаем как в таблице — больше очков выше, при равенстве больше побед, — а
    при полном равенстве честно помечаем, что место делится: решает личная
    встреча, и бот про неё не знает."""
    order = sorted(teams, key=lambda t: (-int(t.get("points") or 0),
                                         -int(t.get("wins") or 0)))
    out: Dict[str, Tuple[int, bool]] = {}
    for i, t in enumerate(order):
        same = [x for x in order
                if int(x.get("points") or 0) == int(t.get("points") or 0)
                and int(x.get("wins") or 0) == int(t.get("wins") or 0)]
        out[str(t.get("team_id"))] = (i + 1, len(same) > 1)
    return out


async def slpro_table(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Таблица стадии SLPRO с посчитанными местами."""
    import slpro_client
    try:
        teams = await slpro_client.SlproClient(
            tournament=str(ctx.get("division") or "")).get_standings(ctx)
    except Exception as exc:
        logger.warning("Зал славы: таблица SLPRO %s — %s", ctx.get("stage_id"), exc)
        return []
    places = _slpro_places(teams)
    out = []
    for t in teams:
        place, shared = places.get(str(t.get("team_id")), (0, False))
        out.append({"team_id": str(t.get("team_id")), "name": str(t.get("name") or ""),
                    "place": place, "shared": shared,
                    "wins": int(t.get("wins") or 0),
                    "losses": int(t.get("losses") or 0)})
    out.sort(key=lambda r: r["place"])
    return out


async def look_up(source: str, season_id: Any, stage_id: Any, team_id: Any,
                  ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Что лига говорит про наше место в этом турнире.

    {place, teams, wins, losses, sure, shared}. sure — можно ли верить без
    вопросов (Инфобаскет присылает место сам, SLPRO — нет)."""
    if str(source) == "infobasket":
        table = await infobasket_table(season_id)
        mine = next((r for r in table if r["team_id"] == str(team_id)), None)
        if not mine:
            mine = next((r for r in table if is_ours(r["name"])), None)
        if not mine:
            return {}
        return {"place": mine["place"], "teams": len(table), "wins": mine["wins"],
                "losses": mine["losses"], "name": mine["name"], "sure": True,
                "shared": False}
    table = await slpro_table(ctx or {"season_id": season_id, "stage_id": stage_id})
    mine = next((r for r in table if r["team_id"] == str(team_id)), None)
    if not mine:
        mine = next((r for r in table if is_ours(r["name"])), None)
    if not mine:
        return {}
    return {"place": mine["place"], "teams": len(table), "wins": mine["wins"],
            "losses": mine["losses"], "name": mine["name"], "sure": False,
            "shared": bool(mine.get("shared"))}


# ─────────────────────────── поиск прошлых турниров ───────────────────────────


async def scan_slpro() -> List[Dict[str, Any]]:
    """Все стадии SLPRO, где играла наша команда. Имя ищем по корню."""
    import slpro_client
    found: List[Dict[str, Any]] = []
    try:
        client = slpro_client.SlproClient()
        stages = await client.iter_stages()
    except Exception as exc:
        logger.warning("Зал славы: стадии SLPRO — %s", exc)
        return []
    for stage in stages:
        table = await slpro_table(stage)
        mine = next((r for r in table if is_ours(r["name"])), None)
        if not mine:
            continue
        wins, losses, last = _our_record(
            "slpro", stage.get("season_id"), stage.get("stage_id"), mine["team_id"])
        found.append({
            "source": "slpro", "season_id": str(stage.get("season_id") or ""),
            "stage_id": str(stage.get("stage_id") or ""),
            "team_id": mine["team_id"], "team_name": mine["name"],
            "league": str(stage.get("division_name") or stage.get("division") or ""),
            "season": str(stage.get("season") or ""),
            "place": mine["place"], "teams": len(table),
            "wins": wins or mine["wins"], "losses": losses or mine["losses"],
            "last_day": last, "guess": 1, "shared": mine.get("shared"),
        })
    return found


async def _infobasket_title(comp_id: Any) -> Tuple[str, str]:
    """Как называется турнир Инфобаскета и в каком он сезоне.

    Имя у них разложено по уровням: «Санкт-Петербург» → «Летняя лига» →
    «Мужчины» → «Летняя Лига» → «2 Этап» → «4 Лига». Человеку нужны последние
    содержательные — «Летняя Лига · 2 Этап · 4 Лига», — а сезон («25/26»)
    лежит в коротком имени самого верхнего уровня."""
    import aiohttp
    names: List[str] = []
    season = ""
    cur, seen = str(comp_id), set()
    try:
        async with aiohttp.ClientSession() as session:
            for _ in range(6):
                if not cur or cur in seen:
                    break
                seen.add(cur)
                url = (f"https://reg.infobasket.su/Widget/CompIssue/{cur}"
                       "?format=json&lang=ru")
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
                    data = await r.json(content_type=None) if r.status == 200 else None
                parent = (data or {}).get("ParentComp") or {}
                if not parent:
                    break
                names.append(str(parent.get("CompShortNameRu") or "").strip())
                season = str(parent.get("CompAbcNameRu") or "").strip() or season
                cur = str(parent.get("CompIDparent") or "")
    except Exception as exc:
        logger.warning("Зал славы: имя турнира %s — %s", comp_id, exc)
    # Города и «Мужчины» в зале славы не нужны: турнир узнают по лиге, этапу и
    # группе. Берём три ближайших уровня — дальше начинается адрес федерации.
    skip = {"мужчины", "женщины", "санкт-петербург"}
    good = [n for n in names if n and n.lower() not in skip][:3]
    return " · ".join(reversed(good)), (season if "/" in season else "")


def _our_record(source: str, season_id: Any, stage_id: Any,
                team_id: Any) -> Tuple[int, int, str]:
    """(побед, поражений, дата последней игры) по нашему же справочнику матчей.

    Лига победы в таблице отдаёт не всегда (у Инфобаскета там нули), а у нас
    результаты своих игр лежат целиком — считаем по ним."""
    init()
    sql = ("SELECT game_date, home_team_id, guest_team_id, home_score, guest_score "
           "FROM game_meta WHERE source = ? AND season_id = ? "
           "AND (home_team_id = ? OR guest_team_id = ?)")
    args: List[Any] = [str(source), str(season_id), str(team_id), str(team_id)]
    if str(stage_id or ""):
        sql += " AND stage_id = ?"
        args.append(str(stage_id))
    wins = losses = 0
    last = ""
    with sheets_cache.get_connection() as conn:
        for r in conn.execute(sql, args):
            ours_home = str(r["home_team_id"]) == str(team_id)
            mine = int(r["home_score"] if ours_home else r["guest_score"] or 0)
            other = int(r["guest_score"] if ours_home else r["home_score"] or 0)
            if mine == other == 0:
                continue                      # игра ещё не сыграна
            wins += int(mine > other)
            losses += int(mine < other)
            last = max(last, str(r["game_date"] or "")[:10])
    return wins, losses, last


def tracked() -> List[Dict[str, str]]:
    """Турниры, которые бот вёл: по своему же справочнику матчей.

    «Все лиги, которые добавляли и отслеживали» — это именно они: что попадало
    в «Конфиг», то и качалось. Команду узнаём и по id, и по имени — id за годы
    менялись, а корень имени нет."""
    init()
    import league_sync
    ids = {str(t["team_id"]) for t in league_sync.our_teams(include_closed=True)}
    out: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT source, season_id, stage_id, home_team_id, guest_team_id, "
            "home_name, guest_name FROM game_meta WHERE season_id != ''")
        for r in rows:
            for side in ("home", "guest"):
                tid = str(r[f"{side}_team_id"] or "")
                name = str(r[f"{side}_name"] or "")
                if tid not in ids and not is_ours(name):
                    continue
                key = (r["source"], str(r["season_id"]), str(r["stage_id"] or ""))
                out.setdefault(key, {"source": r["source"],
                                     "season_id": str(r["season_id"]),
                                     "stage_id": str(r["stage_id"] or ""),
                                     "team_id": tid, "team_name": name})
    return list(out.values())


async def scan_infobasket(comps: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    """Турниры Инфобаскета с местами из таблицы лиги."""
    found: List[Dict[str, Any]] = []
    for comp in comps:
        table = await infobasket_table(comp["season_id"])
        if not table:
            continue
        mine = next((r for r in table if r["team_id"] == str(comp.get("team_id"))), None)
        if not mine:
            mine = next((r for r in table if is_ours(r["name"])), None)
        if not mine:
            continue
        league, season = await _infobasket_title(comp["season_id"])
        wins, losses, last = _our_record("infobasket", comp["season_id"], "",
                                         mine["team_id"])
        found.append({
            "source": "infobasket", "season_id": comp["season_id"], "stage_id": "",
            "team_id": mine["team_id"], "team_name": mine["name"],
            "league": league, "season": season, "place": mine["place"],
            "teams": len(table), "wins": wins, "losses": losses,
            "last_day": last, "guess": 0,
        })
    return found


async def _slpro_stage_ctx() -> Dict[Tuple[str, str], Dict[str, Any]]:
    """{(сезон, стадия): контекст} — чтобы назвать турнир, который в таблице
    лиги под нашим именем не нашёлся."""
    import slpro_client
    try:
        stages = await slpro_client.SlproClient().iter_stages()
    except Exception as exc:
        logger.warning("Зал славы: стадии SLPRO — %s", exc)
        return {}
    return {(str(s.get("season_id")), str(s.get("stage_id"))): s for s in stages}


async def scan() -> Tuple[int, int]:
    """Ищет все турниры наших команд и заводит их в зал славы.

    (сколько нашли, сколько новых). Место, подтверждённое тренером, не трогаем:
    он видел награждение, а бот — таблицу."""
    init()
    known = tracked()
    ib = [t for t in known if t["source"] == "infobasket"]
    both = await asyncio.gather(scan_slpro(), scan_infobasket(ib))
    found = [row for part in both for row in part]
    added = 0
    for row in found:
        was = get(row["source"], row["season_id"], row["stage_id"], row["team_id"])
        if was and int(was.get("place") or 0) and not int(was.get("guess") or 0):
            # Место подтверждено человеком — оставляем как есть, дополняем
            # только справочное.
            save(row["source"], row["season_id"], row["stage_id"], row["team_id"],
                 league=row["league"], season=row["season"],
                 team_name=row["team_name"], teams=row["teams"],
                 wins=row["wins"], losses=row["losses"],
                 last_day=row.get("last_day", ""))
            continue
        if not was:
            added += 1
        save(row["source"], row["season_id"], row["stage_id"], row["team_id"],
             league=row["league"], season=row["season"], team_name=row["team_name"],
             place=row["place"], teams=row["teams"], wins=row["wins"],
             losses=row["losses"], last_day=row.get("last_day", ""),
             guess=int(row.get("guess") or 0))

    # Турниры, которые бот вёл, но в таблице лиги нас под нашим именем нет:
    # команда играла под другим названием, стадию переигрывали, лига правила
    # состав. Молчать о них нельзя — тренер помнит эти сезоны; заводим пустыми,
    # место он поставит руками.
    seen = {(r["source"], r["season_id"], r["stage_id"], r["team_id"]) for r in found}
    ctxs = await _slpro_stage_ctx()
    for t in known:
        key = (t["source"], t["season_id"], t["stage_id"], t["team_id"])
        if key in seen or get(*key):
            continue
        if t["source"] == "slpro":
            ctx = ctxs.get((t["season_id"], t["stage_id"])) or {}
            league = str(ctx.get("division_name") or ctx.get("division") or "")
            season = str(ctx.get("season") or "")
        else:
            league, season = await _infobasket_title(t["season_id"])
        wins, losses, last = _our_record(t["source"], t["season_id"], t["stage_id"],
                                         t["team_id"])
        save(*key, league=league, season=season, team_name=t["team_name"],
             wins=wins, losses=losses, last_day=last)
        added += 1
    return len(found), added
