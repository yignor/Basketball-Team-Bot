#!/usr/bin/env python3
"""
Копирование статистики чужих лиг в локальную базу.

Зачем: каждая завершённая игра неизменна, поэтому её box-score достаточно
скачать один раз. Дальше бот отвечает из своей SQLite — мгновенно и не
дёргая чужие серверы. Свежие игры добавляются попутно: их всё равно парсит
монитор результатов (см. store_slpro_box / store_infobasket_game).

Вежливость к чужим API — не опция, а требование:
  * запросы строго последовательные, с паузой между ними (DEFAULT_DELAY);
  * за один прогон качаем не больше `limit` игр, остальное — в следующую ночь;
  * уже скачанное не перекачиваем никогда (реестр game_stats_fetched);
  * незавершённые игры не помечаем скачанными — вернёмся к ним позже.

Юр-инвариант: в базу ложатся только идентификаторы игроков и команд плюс
сухие цифры. ФИО не сохраняются (см. [[legal-data-invariant]]).
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

import fantasy_stats
import sheets_cache

log = logging.getLogger(__name__)

DEFAULT_DELAY = 1.5          # секунд между запросами к чужому API
DEFAULT_LIMIT = 200          # игр за один прогон
IB_API = "https://reg.infobasket.su"

# Соревнования Infobasket по сезонам: comp_id -> подпись.
# Соревнования Infobasket команды. Источник правды — лист «Конфиг»
# (get_config_ids), здесь лишь читабельные подписи и запасной вариант.
# Старый хардкод [73582, 88649, 108009] был ОШИБОЧНЫМ — это чужие юношеские/
# ветеранские лиги (73582 = «Юноши 2010»), не путать с нашей 140825.
IB_COMPS: Dict[int, str] = {140825: "Летняя Лига · Группа 4 (2026)"}

# Статус завершённой игры отличается у источников.
SLPRO_FINISHED = 2
IB_FINISHED = 1


class BackfillStats:
    """Итог прогона — что скачали, что пропустили и почему."""

    def __init__(self) -> None:
        self.fetched = 0        # игр скачано и сохранено
        self.skipped_cached = 0  # уже были в базе
        self.skipped_live = 0    # ещё не доиграны
        self.failed = 0          # ошибка запроса/разбора
        self.remaining = 0       # сколько ещё осталось после потолка

    def __str__(self) -> str:
        parts = [f"скачано {self.fetched}", f"из кеша {self.skipped_cached}"]
        if self.skipped_live:
            parts.append(f"не доиграны {self.skipped_live}")
        if self.failed:
            parts.append(f"ошибок {self.failed}")
        if self.remaining:
            parts.append(f"осталось на следующий раз {self.remaining}")
        return ", ".join(parts)


# ─────────────────────────── SLPRO ───────────────────────────────────────────

async def _slpro_stages(client, scope: str, team_names: List[str]) -> List[Dict[str, Any]]:
    """Стадии для обхода. team — только те, где играем; league — все стадии
    тех же сезонов; all — вообще все.

    Наши турниры берутся из листа «Конфиг» (ТИП=SLPRO), а не из одного
    автоопределения: команда может играть в двух турнирах сразу."""
    import slpro_client
    if scope == "all":
        return await client.iter_stages()

    ours = await slpro_client.team_contexts(team_names)
    if scope == "team":
        return ours

    # league: все стадии сезонов, в которых играет наша команда
    stages = await client.iter_stages()
    seasons = {c.get("season_id") for c in ours if c.get("season_id") is not None}
    if not seasons:
        return stages
    return [s for s in stages if s.get("season_id") in seasons]


async def backfill_slpro(client, scope: str = "league", team_names: Optional[List[str]] = None,
                         limit: int = DEFAULT_LIMIT, delay: float = DEFAULT_DELAY,
                         dry_run: bool = False) -> BackfillStats:
    """Качает box-score завершённых игр SLPRO. Резюмируемо: повторный запуск
    продолжает с того места, где остановился прошлый."""
    import slpro_game

    st = BackfillStats()
    stages = await _slpro_stages(client, scope, team_names or [])
    if not stages:
        log.warning("бэкфилл slpro: не нашёл ни одной стадии (scope=%s)", scope)
        return st

    # Сперва собираем всё, что предстоит скачать, — так виден масштаб и
    # корректно считается остаток при упоре в потолок.
    todo: List[Tuple[str, Dict[str, Any]]] = []
    for ctx in stages:
        team_id = ctx.get("team_id") if scope == "team" else None
        for g in await client.get_schedule(ctx):
            if team_id is not None and team_id not in (g.get("home_id"), g.get("guest_id")):
                continue
            if g.get("status") != SLPRO_FINISHED:
                st.skipped_live += 1
                continue
            gid = str(g.get("game_id"))
            if fantasy_stats.is_game_fetched(fantasy_stats.SOURCE_SLPRO, gid):
                st.skipped_cached += 1
                continue
            todo.append((gid, ctx))
        await asyncio.sleep(delay)

    if limit and len(todo) > limit:
        st.remaining = len(todo) - limit
        todo = todo[:limit]

    log.info("бэкфилл slpro: стадий %d, к скачиванию %d игр%s",
             len(stages), len(todo), " (пробный прогон)" if dry_run else "")
    if dry_run:
        st.fetched = len(todo)
        return st

    for gid, ctx in todo:
        try:
            resp = await client.get_game(gid, ctx)
            box = slpro_game.parse_box_score(resp) if resp else None
            if not box:
                st.failed += 1
            else:
                fantasy_stats.store_slpro_box(box, str(ctx.get("season_id") or ""),
                                              ctx.get("stage_id") or "")
                st.fetched += 1
        except Exception as e:                    # сеть/разбор — не роняем прогон
            log.warning("бэкфилл slpro: игра %s — %s", gid, e)
            st.failed += 1
        await asyncio.sleep(delay)

    return st


# ─────────────────────────── Infobasket ──────────────────────────────────────

IB_CALENDAR_TIMEOUT = 90       # календарь сезона — до 2 МБ
IB_CALENDAR_RETRIES = 3


def _ib_session() -> aiohttp.ClientSession:
    """Соединение не переиспользуем: reg.infobasket.su не держит keep-alive так,
    как ожидает aiohttp — второй запрос в сессии виснет до таймаута."""
    return aiohttp.ClientSession(connector=aiohttp.TCPConnector(force_close=True))


async def _ib_calendar(session: aiohttp.ClientSession, comp_id: int) -> List[Dict[str, Any]]:
    url = f"{IB_API}/Comp/GetCalendar/?comps={comp_id}&format=json"
    for attempt in range(1, IB_CALENDAR_RETRIES + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=IB_CALENDAR_TIMEOUT)) as r:
                if r.status != 200:
                    log.warning("бэкфилл infobasket: календарь %s -> HTTP %s", comp_id, r.status)
                    return []
                data = await r.json(content_type=None)
            return data if isinstance(data, list) else []
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("бэкфилл infobasket: календарь %s, попытка %d/%d — %s",
                        comp_id, attempt, IB_CALENDAR_RETRIES, type(e).__name__)
            if attempt == IB_CALENDAR_RETRIES:
                return []
            await asyncio.sleep(2.0 * attempt)
    return []


async def fetch_infobasket_roster(team_id: Any, comp_id: Any) -> List[Dict[str, Any]]:
    """Ростер команды Инфобаскета: [{player_id, number, name, active}]. Имена —
    транзитно (в наших таблицах не храним). Widget/TeamRoster/<team>?compId=<comp>."""
    url = f"{IB_API}/Widget/TeamRoster/{team_id}?compId={comp_id}&format=json&lang=ru"
    out: List[Dict[str, Any]] = []
    try:
        async with _ib_session() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                if r.status != 200:
                    log.warning("ростер infobasket %s/%s -> HTTP %s", team_id, comp_id, r.status)
                    return []
                data = await r.json(content_type=None)
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("ростер infobasket %s/%s — %s", team_id, comp_id, type(e).__name__)
        return []
    for p in (data or {}).get("Players", []):
        pid = p.get("PersonID")
        if not pid:
            continue
        pi = p.get("PersonInfo") or {}
        name = f"{pi.get('PersonLastNameRu', '')} {pi.get('PersonFirstNameRu', '')}".strip()
        out.append({
            "player_id": pid,
            "number": str(p.get("DisplayNumber", "") or ""),
            "name": name,                    # транзитно
            "active": bool(p.get("IsActive")),
        })
    return out


async def backfill_infobasket(comp_ids: List[int], limit: int = DEFAULT_LIMIT,
                              delay: float = DEFAULT_DELAY, dry_run: bool = False) -> BackfillStats:
    """Качает статистику игроков завершённых игр Infobasket по календарям
    соревнований (comp_id). Разбор — тем же парсером, что и уведомления."""
    from enhanced_game_parser import EnhancedGameParser

    st = BackfillStats()
    todo: List[Tuple[str, int]] = []

    async with _ib_session() as session:
        for comp_id in comp_ids:
            for g in await _ib_calendar(session, comp_id):
                if g.get("GameStatus") != IB_FINISHED:
                    st.skipped_live += 1
                    continue
                gid = str(g.get("GameID") or "")
                if not gid:
                    continue
                if fantasy_stats.is_game_fetched(fantasy_stats.SOURCE_INFOBASKET, gid):
                    st.skipped_cached += 1
                    continue
                todo.append((gid, comp_id))
            await asyncio.sleep(delay)

    if limit and len(todo) > limit:
        st.remaining = len(todo) - limit
        todo = todo[:limit]

    log.info("бэкфилл infobasket: соревнований %d, к скачиванию %d игр%s",
             len(comp_ids), len(todo), " (пробный прогон)" if dry_run else "")
    if dry_run:
        st.fetched = len(todo)
        return st

    async def _game_data(parser, gid: str) -> Optional[Dict[str, Any]]:
        """Одна игра, с повторами: сеть до лиги капризная, а ронять из-за
        одного таймаута весь ночной прогон незачем."""
        for attempt in range(1, 3):
            data = await parser.get_game_data_from_api(gid, IB_API)
            if data:
                return data
            if attempt == 2:
                return None
            await asyncio.sleep(2.0)
        return None

    async with EnhancedGameParser() as parser:
        for gid, comp_id in todo:
            try:
                api_data = await _game_data(parser, gid)
                info = await parser.parse_game_info(api_data) if api_data else None
                if info:
                    info["player_stats"] = parser.extract_player_statistics(api_data)
                if not info or not (info.get("player_stats") or {}).get("players"):
                    st.failed += 1
                else:
                    # parse_game_info кладёт game_id=None, если его нет в ответе,
                    # поэтому setdefault тут не годится — присваиваем явно.
                    info["game_id"] = info.get("game_id") or gid
                    fantasy_stats.store_infobasket_game(info, str(comp_id))
                    st.fetched += 1
            except Exception as e:
                log.warning("бэкфилл infobasket: игра %s — %s", gid, e)
                st.failed += 1
            await asyncio.sleep(delay)

    return st


# ─────────────────────────── Сводка ──────────────────────────────────────────

def purge_source_season(source: str, season_id: str) -> Dict[str, int]:
    """Удаляет из локальной копии все игры источника за конкретный season_id.
    Нужно, чтобы вычистить ошибочно скачанную чужую лигу (напр. Infobasket
    73582 = «Юноши 2010», затянутую старым хардкодом comp). game_stats_fetched
    не имеет season_id — чистим по game_id."""
    sheets_cache.init_db()
    sid = str(season_id)
    with sheets_cache.get_connection() as conn:
        gids = {r[0] for r in conn.execute(
            "SELECT game_id FROM game_player_stats WHERE source=? AND season_id=?", (source, sid))}
        gids |= {r[0] for r in conn.execute(
            "SELECT game_id FROM game_meta WHERE source=? AND season_id=?", (source, sid))}
        n_stats = conn.execute(
            "DELETE FROM game_player_stats WHERE source=? AND season_id=?", (source, sid)).rowcount
        n_meta = conn.execute(
            "DELETE FROM game_meta WHERE source=? AND season_id=?", (source, sid)).rowcount
        n_fetched = 0
        for gid in gids:
            n_fetched += conn.execute(
                "DELETE FROM game_stats_fetched WHERE source=? AND game_id=?", (source, gid)).rowcount
        conn.commit()
    return {"games": len(gids), "stats": n_stats, "meta": n_meta, "fetched": n_fetched}


def forget_games_missing_fields(source: str = "slpro") -> int:
    """Помечает к перекачке игры, у которых пусто время на площадке и плюс-минус.

    Эти поля появились позже самого бэкфилла, и у старых игр их нет. Плюс-минус
    SLPRO в API не отдаёт — он считается из play-by-play (в событии заброшенного
    мяча приходит состав на площадке), поэтому нужна именно перекачка протокола,
    а не досчёт по имеющимся строкам."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT DISTINCT game_id FROM game_player_stats
               WHERE source = ? GROUP BY game_id
               HAVING MAX(secs) = 0 AND MAX(ABS(plus_minus)) = 0""", (source,)).fetchall()
        ids = [r["game_id"] for r in rows]
        for gid in ids:
            conn.execute("DELETE FROM game_stats_fetched WHERE source = ? AND game_id = ?",
                         (source, gid))
        conn.commit()
    log.info("к перекачке (нет времени/плюс-минуса): %d игр", len(ids))
    return len(ids)


def forget_games_without_stage(source: str = "slpro") -> int:
    """Снимает отметку «скачано» с игр, у которых не заполнена стадия.

    Такие строки остались от версии до появления stage_id: при подсчёте по
    конкретному турниру они молча выпадали бы из фэнтези. Бэкфилл перекачает
    их и заполнит стадию. Сами данные не трогаем — их перезапишет upsert."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT DISTINCT game_id FROM game_player_stats
               WHERE source = ? AND (stage_id IS NULL OR stage_id = '')""", (source,)
        ).fetchall()
        ids = [r["game_id"] for r in rows]
        for gid in ids:
            conn.execute("DELETE FROM game_stats_fetched WHERE source = ? AND game_id = ?",
                         (source, gid))
        conn.commit()
    return len(ids)


def local_summary() -> Dict[str, Any]:
    """Что уже лежит в локальной копии — для отчёта в админке.

    Источник правды по играм — game_stats_fetched: он вёлся и до появления
    game_meta, поэтому по нему видно всё скачанное, а не только новое."""
    sheets_cache.init_db()
    out: Dict[str, Any] = {}
    with sheets_cache.get_connection() as conn:
        for row in conn.execute(
            """SELECT source, COUNT(*) games, MIN(game_date) first, MAX(game_date) last
               FROM game_stats_fetched GROUP BY source"""
        ):
            out[row["source"]] = dict(row)
        for row in conn.execute(
            "SELECT source, COUNT(*) rows, COUNT(DISTINCT player_id) players FROM game_player_stats GROUP BY source"
        ):
            out.setdefault(row["source"], {}).update(rows=row["rows"], players=row["players"])
        for row in conn.execute("SELECT source, COUNT(*) with_meta FROM game_meta GROUP BY source"):
            out.setdefault(row["source"], {}).update(with_meta=row["with_meta"])
    return out


# ─── личная история игрока (Инфобаскет) ──────────────────────────────────────
#
# Открыто 27.07.2026: у Инфобаскета ЕСТЬ помачевая история по человеку, и берётся
# она за пару запросов, без зеркалирования целых соревнований.
#   Widget/PlayerSeasonStats/<personId>  -> SeasonStats[].Season.CompID — сезоны,
#                                           в которых человек играл;
#   Widget/PlayerStats/<personId>?compId=<сезон> -> GameStats[] — строка на игру
#                                           (GameID, дата, полный бокс-скор).
# `compId` здесь — СЕЗОН-контейнер (2024/2025/2026), а не турнир: compId=0 даёт
# только текущий сезон, поэтому за прошлые надо спрашивать отдельно.

def _ib_person_row(g: Dict[str, Any], person_id: str, season_comp: Any) -> Optional[tuple]:
    """Запись игры из PlayerStats -> кортеж под game_player_stats."""
    game = g.get("Game") or {}
    game_id = game.get("GameID")
    if not game_id:
        return None
    d = (g.get("GameDate") or "").strip()          # DD.MM.YYYY
    try:
        day, month, year = d.split(".")
        game_date = f"{year}-{month}-{day}"
    except ValueError:
        return None
    side = "TeamNameA" if g.get("TeamNumber") == 1 else "TeamNameB"
    team_id = str((g.get(side) or {}).get("TeamID") or "")

    def n(key: str) -> int:
        return int(g.get(key) or 0)

    return (fantasy_stats.SOURCE_INFOBASKET, str(game_id), str(person_id), team_id,
            str(g.get("DisplayNumber") or ""), game_date, str(season_comp), "",
            n("Points"), n("Rebound"), n("OffRebound"), n("DefRebound"), n("Assist"),
            n("Steal"), n("Blocks"), n("Turnover"), n("Foul"),
            n("Goal23"), n("Shot23"), n("Goal3"), n("Shot3"), n("Goal1"), n("Shot1"),
            n("Seconds"), n("PlusMinus"),
            sheets_cache.now_iso())


async def fetch_person_games_infobasket(person_id: Any,
                                        api_url: str = IB_API) -> Dict[str, Any]:
    """Скачивает всю личную историю человека и кладёт в локальную копию.

    Пишем через INSERT OR IGNORE — намеренно. У строк, скачанных по турниру,
    в `season_id` лежит comp_id (по нему фильтруется зачёт фэнтези), а здесь —
    сезон-контейнер. Перезапись сломала бы фильтр подсчёта очков, поэтому уже
    имеющиеся строки не трогаем: личная история только дополняет.
    """
    sheets_cache.init_db()
    api = (api_url or IB_API).rstrip("/")
    pid = str(person_id)
    seasons: List[Any] = []
    rows: List[tuple] = []

    async with _ib_session() as session:
        try:
            async with session.get(f"{api}/Widget/PlayerSeasonStats/{pid}?format=json",
                                   timeout=aiohttp.ClientTimeout(total=30)) as r:
                data = await r.json(content_type=None) if r.status == 200 else {}
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            log.warning("личная история %s: сезоны — %s", pid, type(e).__name__)
            data = {}
        for s in (data.get("SeasonStats") or []):
            comp = (s.get("Season") or {}).get("CompID")
            if comp is not None and comp not in seasons:
                seasons.append(comp)

        for comp in seasons or [0]:
            url = f"{api}/Widget/PlayerStats/{pid}?compId={comp}&filter=0&team=0&format=json"
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
                    payload = await r.json(content_type=None) if r.status == 200 else {}
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning("личная история %s: сезон %s — %s", pid, comp, type(e).__name__)
                continue
            for g in (payload.get("GameStats") or []):
                row = _ib_person_row(g, pid, comp)
                if row:
                    rows.append(row)
            await asyncio.sleep(DEFAULT_DELAY)

    added = 0
    if rows:
        with sheets_cache.get_connection() as conn:
            before = conn.total_changes
            conn.executemany(
                """INSERT OR IGNORE INTO game_player_stats
                   (source, game_id, player_id, team_id, number, game_date, season_id,
                    stage_id, pts, reb, reb_off, reb_def, ast, stl, blk, tur, pf,
                    fgm, fga, tpm, tpa, ftm, fta, secs, plus_minus, fetched_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)
            added = conn.total_changes - before
            conn.commit()
    return {"seasons": seasons, "games": len(rows), "added": added}
