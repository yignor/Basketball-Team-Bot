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
    org         TEXT NOT NULL DEFAULT '',   -- ЛИГА: SLPRO, Летняя лига, ВСЕСМАРТ, НБЛ
    league      TEXT NOT NULL DEFAULT '',   -- стадия внутри лиги: «Группа 4», «Первая лига»
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
    hidden      INTEGER NOT NULL DEFAULT 0, -- тренер убрал: поиск не возвращает обратно
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
        # Лига и «убрано» появились позже самой таблицы.
        sheets_cache._ensure_column(conn, "league_results", "org", "TEXT NOT NULL", "''")
        sheets_cache._ensure_column(conn, "league_results", "hidden",
                                    "INTEGER NOT NULL", "0")
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
    keys = ("org", "league", "season", "team_name", "place", "teams", "wins",
            "losses", "last_day", "photo_id", "note", "guess", "hidden", "set_by")
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
    """Убирает турнир из зала славы.

    Не удаляем, а прячем: иначе следующий поиск по лигам принёс бы его назад,
    и тренер убирал бы одно и то же каждый раз."""
    save(source, season_id, stage_id, team_id, hidden=1)


def results() -> List[Dict[str, Any]]:
    """Всё записанное, свежее сверху. Турниры без места — тоже: их видно, что
    они ждут, и тренер допишет место одной кнопкой."""
    init()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT * FROM league_results WHERE COALESCE(hidden, 0) = 0")]
    rows.sort(key=lambda r: (str(r["last_day"] or ""), str(r["season"] or "")),
              reverse=True)
    return rows


MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}


def season_key(row: Dict[str, Any]) -> Tuple[int, int]:
    """Сезон числами, чтобы сортировать и сводить вместе.

    У лиг он записан по-разному: «2025-2026» у SLPRO, «25/26» у Инфобаскета,
    «Сезон 2023/2024» в справочниках. Для тренера это один и тот же сезон."""
    digits = re.findall(r"\d{2,4}", str(row.get("season") or ""))
    years = []
    for d in digits[:2]:
        year = int(d)
        years.append(year if year > 100 else 2000 + year)
    if not years:
        day = str(row.get("last_day") or "")[:4]
        return (int(day), int(day)) if day.isdigit() else (0, 0)
    return (years[0], years[1] if len(years) > 1 else years[0])


def season_label(row: Dict[str, Any]) -> str:
    """«2025/26» — одинаково для всех лиг."""
    first, second = season_key(row)
    if not first:
        return "без сезона"
    return f"{first}/{str(second)[2:]}" if second != first else str(first)


def org_of(row: Dict[str, Any]) -> str:
    """Лига одним словом: SLPRO, ВСЕСМАРТ, Летняя лига."""
    org = str(row.get("org") or "").strip()
    if org:
        return org
    return "SLPRO" if row.get("source") == "slpro" else "Инфобаскет"


def group_key(org: str, season: str) -> str:
    """Короткий ключ лиги-сезона для кнопки: в callback 64 байта, а «ЛИГА
    Квалификация Высшая/Первая Лига» туда не влезет."""
    import hashlib
    return hashlib.md5(f"{org.casefold()}|{season}".encode()).hexdigest()[:8]


def best_of(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Какой результат показывать за лигу в сезоне.

    Сперва то, что подтвердил тренер: он был на награждении и знает, что
    третье место в лиге — это выигранный матч за третье место, а не строчка в
    таблице группы. Если не подтверждал — берём лучшее место."""
    live = [r for r in rows if int(r.get("place") or 0)]
    if not live:
        return None
    confirmed = [r for r in live if not int(r.get("guess") or 0) and r.get("set_by")]
    pool = confirmed or live
    return min(pool, key=lambda r: int(r["place"]))


def groups() -> List[Dict[str, Any]]:
    """Зал славы по сезонам: [{season, org, rows, best}], свежие сперва."""
    rows = results()
    # Лига в разные годы записана по-разному («Летняя лига» и «Летняя Лига»).
    # Берём то написание, что у самого свежего сезона: список не должен
    # выглядеть как две разные лиги.
    spelling: Dict[str, str] = {}
    for row in sorted(rows, key=season_key):
        spelling[org_of(row).casefold()] = org_of(row)
    out: Dict[Tuple[Tuple[int, int], str], Dict[str, Any]] = {}
    for row in rows:
        low = org_of(row).casefold()
        key = (season_key(row), low)
        box = out.setdefault(key, {"season": season_label(row),
                                   "org": spelling.get(low, org_of(row)),
                                   "sort": season_key(row), "rows": []})
        box["rows"].append(row)
    ordered = sorted(out.values(), key=lambda b: (b["sort"], b["org"].casefold()),
                     reverse=True)
    for box in ordered:
        box["rows"].sort(key=lambda r: (int(r["place"] or 0) or 99,
                                        str(r["league"] or "")))
        box["best"] = best_of(box["rows"])
        box["key"] = group_key(box["org"], box["season"])
    return ordered


def league_of(row: Dict[str, Any]) -> str:
    """Лига и сезон одной строкой: «Летняя лига · 25/26», «SLPRO · 2025-2026».

    Без лиги зал славы читается как список загадок: «Группа 4», «Первая лига»,
    «B» — а тренер держит в голове именно лиги, их у команды несколько сразу."""
    org = str(row.get("org") or "").strip()
    if not org:
        org = "SLPRO" if row.get("source") == "slpro" else "Инфобаскет"
    season = str(row.get("season") or "").strip()
    return f"{org} · {season}" if season else org


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
            "org": "SLPRO",
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


async def _team_games(team_id: Any, season_comp: str = "") -> List[Dict[str, Any]]:
    """Игры команды по справочнику лиги. season_comp — сезон (compId).

    Здесь у каждой игры написано, какая это ЛИГА («Летняя Лига», «ВСЕСМАРТ»)
    и какая внутри неё стадия («Группа 4», «Первая лига»). Своего справочника
    турниров у Инфобаскета нет: тот, что есть, отдаёт один текущий сезон."""
    import aiohttp
    tail = f"compId={season_comp}&" if season_comp else ""
    url = (f"https://reg.infobasket.su/Widget/TeamGames/{team_id}?{tail}"
           "format=json&lang=ru")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
                data = await r.json(content_type=None) if r.status == 200 else None
    except Exception as exc:
        logger.warning("Зал славы: игры команды %s — %s", team_id, exc)
        return []
    return data if isinstance(data, list) else []


async def _team_seasons(team_id: Any) -> List[Dict[str, str]]:
    """Сезоны, в которых команда играла: [{comp_id, name}]."""
    import aiohttp
    url = (f"https://reg.infobasket.su/Widget/GetTeamSeasons/{team_id}"
           "?format=json&lang=ru")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
                data = await r.json(content_type=None) if r.status == 200 else None
    except Exception as exc:
        logger.warning("Зал славы: сезоны команды %s — %s", team_id, exc)
        return []
    out = []
    for row in data if isinstance(data, list) else []:
        comp = row.get("CompID") or row.get("CompId")
        if comp:
            out.append({"comp_id": str(comp),
                        "name": str(row.get("SeasonName") or "")})
    return out


async def _comp_of_game(game_id: Any) -> str:
    """Турнир одной игры. В списке игр его id нет — спрашиваем у онлайна."""
    import aiohttp
    url = (f"https://reg.infobasket.su/Widget/GetOnline/{game_id}"
           "?format=json&lang=ru")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=25)) as r:
                data = await r.json(content_type=None) if r.status == 200 else None
    except Exception as exc:
        logger.warning("Зал славы: турнир игры %s — %s", game_id, exc)
        return ""
    return str((data or {}).get("CompID") or "")


# Уровни, которые лигой не являются: пол и город. У Инфобаскета имя турнира
# разложено по дереву, и эти два уровня попадаются всегда.
_NOT_A_LEAGUE = {"мужчины", "женщины", "санкт-петербург", "спб"}


def _clean_org(name: str) -> str:
    """«ЛИГА "Все Смарт". Мужчины» → «Все Смарт».

    Лига в справочнике записана как придётся: с приставкой «ЛИГА», в кавычках,
    с полом через точку. Тренеру нужно короткое имя, по которому он её узнаёт."""
    text = str(name or "").strip()
    quoted = re.search(r"[«\"']([^«»\"']{2,})[»\"']", text)
    if quoted:
        text = quoted.group(1)
    text = re.sub(r"^\s*лига\s+", "", text, flags=re.IGNORECASE)
    text = re.split(r"[.,]\s*(?:мужчины|женщины)\b", text, flags=re.IGNORECASE)[0]
    return text.strip(" .\"'«»")


async def _org_of_comp(comp_id: Any) -> str:
    """Какой лиге принадлежит турнир.

    У Инфобаскета имя разложено по дереву: «Санкт-Петербург» → «ВСЕСМАРТ» →
    «Мужчины» → «Квалификация…» → «Первая лига». Лига — это уровень сразу под
    городом: именно его человек называет, говоря «мы играем во ВСЕСМАРТе»."""
    import aiohttp
    names: List[str] = []
    cur, seen = str(comp_id), set()
    try:
        async with aiohttp.ClientSession() as session:
            for _ in range(7):
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
                cur = str(parent.get("CompIDparent") or "")
    except Exception as exc:
        logger.warning("Зал славы: лига турнира %s — %s", comp_id, exc)
        return ""
    # Снизу вверх: город последний, лига — перед ним.
    good = [n for n in names if n and n.lower() not in _NOT_A_LEAGUE]
    return _clean_org(good[-1]) if good else ""


def _season_label(name: str) -> str:
    """«Сезон 2024/2025» → «24/25». В строке зала славы важен год, не слово."""
    digits = re.findall(r"\d{4}", str(name or ""))
    if len(digits) >= 2:
        return f"{digits[0][2:]}/{digits[1][2:]}"
    return str(name or "").strip()


async def _infobasket_row(team_id: str, comp_id: str, org: str, stage: str,
                          season: str, days: List[str]) -> Optional[Dict[str, Any]]:
    """Одна строка зала славы по турниру Инфобаскета. None — если нас там нет."""
    table = await infobasket_table(comp_id)
    # Пары в таблице — это отдельная игра плей-офф («1/4 финала»), а не турнир:
    # «1 место из 2» в зале славы только сбивает.
    if len(table) < 3:
        return None
    mine = next((r for r in table if r["team_id"] == str(team_id)), None)
    if not mine:
        mine = next((r for r in table if is_ours(r["name"])), None)
    if not mine:
        return None
    wins, losses, last = _our_record("infobasket", comp_id, "", str(team_id))
    if not last:
        good = sorted(d for d in days if len(d) == 10)
        if good:
            d = good[-1]
            last = f"{d[6:]}-{d[3:5]}-{d[:2]}"
    return {"source": "infobasket", "season_id": str(comp_id), "stage_id": "",
            "team_id": str(team_id), "team_name": mine["name"], "org": org,
            "league": stage or mine["name"], "season": season,
            "place": mine["place"], "teams": len(table), "wins": wins,
            "losses": losses, "last_day": last, "guess": 0}


async def scan_infobasket(team_ids: List[str],
                          extra: Optional[List[Dict[str, str]]] = None) -> List[Dict[str, Any]]:
    """Турниры Инфобаскета: все лиги и стадии, где команда играла.

    Идём от игр команды, а не от «Конфига»: в «Конфиге» стоят только те лиги,
    за которыми бот следит сейчас, а зал славы должен помнить и НБЛ, и
    ВСЕСМАРТ, и летнюю — за все сезоны."""
    found: List[Dict[str, Any]] = []
    for team_id in team_ids:
        seasons = await _team_seasons(team_id)
        # Пустой сезон — текущий: у него свой ответ без compId.
        for season in [{"comp_id": "", "name": ""}] + seasons:
            games = await _team_games(team_id, season["comp_id"])
            groups: Dict[Tuple[str, str], Dict[str, Any]] = {}
            for game in games:
                org = str(game.get("LeagueNameRu") or "").strip()
                stage = str(game.get("CompNameRu") or "").strip()
                key = (org, stage)
                box = groups.setdefault(key, {"game_id": game.get("GameID"),
                                              "days": []})
                box["days"].append(str(game.get("GameDate") or ""))
            # Лигу узнаём по одному турниру на группу: у всех стадий одной
            # лиги она общая, а дерево имён стоит шести запросов.
            org_by_name: Dict[str, str] = {}
            for (org, stage), box in groups.items():
                comp_id = await _comp_of_game(box["game_id"])
                if not comp_id:
                    continue
                if org not in org_by_name:
                    org_by_name[org] = await _org_of_comp(comp_id) or _clean_org(org)
                row = await _infobasket_row(
                    team_id, comp_id, org_by_name[org], stage,
                    _season_label(season["name"]), box["days"])
                if row:
                    found.append(row)

    # Турниры, которые бот вёл сам: в списке игр лиги их может не быть (старый
    # сезон, переигровка), а места в них мы знаем — они в таблице лиги.
    seen = {(r["team_id"], r["season_id"]) for r in found}
    for one in extra or []:
        key = (str(one.get("team_id") or ""), str(one.get("season_id") or ""))
        if not key[1] or key in seen:
            continue
        stage, season = await _infobasket_title(key[1])
        stage = stage.rpartition(" · ")[2] or stage
        row = await _infobasket_row(key[0], key[1], await _org_of_comp(key[1]),
                                    stage, _season_label(season) or season, [])
        if row:
            found.append(row)
            seen.add(key)
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
    import league_sync
    ib_teams = sorted({str(t["team_id"]) for t in
                       league_sync.our_teams("infobasket", include_closed=True)}
                      | {t["team_id"] for t in known
                         if t["source"] == "infobasket" and t["team_id"]})
    ib_tracked = [t for t in known if t["source"] == "infobasket"]
    both = await asyncio.gather(scan_slpro(),
                                scan_infobasket(ib_teams, ib_tracked))
    found = [row for part in both for row in part]
    added = 0
    for row in found:
        was = get(row["source"], row["season_id"], row["stage_id"], row["team_id"])
        if was and int(was.get("place") or 0) and not int(was.get("guess") or 0):
            # Место подтверждено человеком — оставляем как есть, дополняем
            # только справочное.
            save(row["source"], row["season_id"], row["stage_id"], row["team_id"],
                 org=row.get("org", ""), league=row["league"], season=row["season"],
                 team_name=row["team_name"], teams=row["teams"],
                 wins=row["wins"], losses=row["losses"],
                 last_day=row.get("last_day", ""))
            continue
        if not was:
            added += 1
        save(row["source"], row["season_id"], row["stage_id"], row["team_id"],
             org=row.get("org", ""), league=row["league"], season=row["season"],
             team_name=row["team_name"], place=row["place"], teams=row["teams"],
             wins=row["wins"], losses=row["losses"],
             last_day=row.get("last_day", ""), guess=int(row.get("guess") or 0))

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
            org = "SLPRO"
            league = str(ctx.get("division_name") or ctx.get("division") or "")
            season = str(ctx.get("season") or "")
        else:
            league, season = await _infobasket_title(t["season_id"])
            org, _, league = league.rpartition(" · ")
            org = org.split(" · ")[0] or "Инфобаскет"
        wins, losses, last = _our_record(t["source"], t["season_id"], t["stage_id"],
                                         t["team_id"])
        save(*key, org=org, league=league, season=season,
             team_name=t["team_name"], wins=wins, losses=losses, last_day=last)
        added += 1
    return len(found), added
