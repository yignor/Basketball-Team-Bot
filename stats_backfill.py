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
    """Стадии для обхода. team — только та, где играем; league — все стадии
    текущего сезона; all — вообще все."""
    if scope == "team":
        ctx = await client.discover_context(team_names)
        return [ctx] if ctx else []

    stages = await client.iter_stages()
    if scope == "all":
        return stages

    # league: сезон, в котором сейчас играет наша команда
    ctx = await client.discover_context(team_names)
    if not ctx:
        return stages
    return [s for s in stages if s.get("season_id") == ctx.get("season_id")]


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

async def _ib_calendar(session: aiohttp.ClientSession, comp_id: int) -> List[Dict[str, Any]]:
    url = f"{IB_API}/Comp/GetCalendar/?comps={comp_id}&format=json"
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as r:
        if r.status != 200:
            log.warning("бэкфилл infobasket: календарь %s -> HTTP %s", comp_id, r.status)
            return []
        data = await r.json(content_type=None)
    return data if isinstance(data, list) else []


async def backfill_infobasket(comp_ids: List[int], limit: int = DEFAULT_LIMIT,
                              delay: float = DEFAULT_DELAY, dry_run: bool = False) -> BackfillStats:
    """Качает статистику игроков завершённых игр Infobasket по календарям
    соревнований (comp_id). Разбор — тем же парсером, что и уведомления."""
    from enhanced_game_parser import EnhancedGameParser

    st = BackfillStats()
    todo: List[Tuple[str, int]] = []

    async with aiohttp.ClientSession() as session:
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

    async with EnhancedGameParser() as parser:
        for gid, comp_id in todo:
            try:
                api_data = await parser.get_game_data_from_api(gid, IB_API)
                info = await parser.parse_game_info(api_data) if api_data else None
                if info:
                    info["player_stats"] = parser.extract_player_statistics(api_data)
                if not info or not (info.get("player_stats") or {}).get("players"):
                    st.failed += 1
                else:
                    info.setdefault("game_id", gid)
                    fantasy_stats.store_infobasket_game(info, str(comp_id))
                    st.fetched += 1
            except Exception as e:
                log.warning("бэкфилл infobasket: игра %s — %s", gid, e)
                st.failed += 1
            await asyncio.sleep(delay)

    return st


# ─────────────────────────── Сводка ──────────────────────────────────────────

def local_summary() -> Dict[str, Any]:
    """Что уже лежит в локальной копии — для отчёта в админке."""
    sheets_cache.init_db()
    out: Dict[str, Any] = {}
    with sheets_cache.get_connection() as conn:
        for row in conn.execute(
            """SELECT source, COUNT(*) games, MIN(game_date) first, MAX(game_date) last
               FROM game_meta GROUP BY source"""
        ):
            out[row["source"]] = dict(row)
        for row in conn.execute(
            "SELECT source, COUNT(*) rows, COUNT(DISTINCT player_id) players FROM game_player_stats GROUP BY source"
        ):
            out.setdefault(row["source"], {}).update(rows=row["rows"], players=row["players"])
    return out
