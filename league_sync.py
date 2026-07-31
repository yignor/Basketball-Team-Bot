#!/usr/bin/env python3
"""
Качалка справочников лиг: команды, заявки, имена.

Единственное место, которое ходит в чужие API ради СПРАВОЧНИКОВ (протоколы
качают stats_backfill/run_backfill). Всё остальное — пул фэнтези, разбор
команды, клавиатура — читает результат из локальной базы и памяти и в сеть
не ходит вовсе.

Так решено после 31.07.2026, когда весь трафик демона ушёл в VPN, лига через
него перестала отвечать, и каждое действие игрока начало стоить минуту
таймаутов. Правило простое: **в ответе человеку живых запросов быть не
должно**. Лига недоступна — работаем на том, что скачано вчера, и пишем об
этом в журнал, а не заставляем ждать.

Что куда ложится:
  • команды и стадии  -> таблица league_teams (id и названия команд — не ПДн);
  • состав заявки     -> таблица league_rosters (ТОЛЬКО id и номер);
  • ФИО               -> player_names, оперативная память, на диск не идёт
                         ([[legal-data-invariant]]).

Запускается: фоновым циклом демона (раз в час) и `run_league_sync.py` по cron.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional

import player_names
import sheets_cache

log = logging.getLogger(__name__)


# ── Команды: кого вообще считаем «нашими» ───────────────────────────────────

async def _slpro_teams() -> List[Dict[str, Any]]:
    """Наши команды SLPRO с полным контекстом стадии."""
    out: List[Dict[str, Any]] = []
    try:
        import slpro_client
        for ctx in await slpro_client.team_contexts():
            tid = ctx.get("team_id")
            if tid is None:
                continue
            out.append({
                "source": "slpro", "team_id": str(tid),
                "name": ctx.get("team_name") or "SLPRO",
                "league": slpro_client.scope_of(ctx).get("name", ""),
                "comp_id": "", "season_id": str(ctx.get("season_id") or ""),
                "stage_id": str(ctx.get("stage_id") or ""),
                "ctx": ctx,
            })
    except Exception as e:
        log.warning(f"качалка: команды SLPRO — {e}")
    return out


async def _infobasket_teams() -> List[Dict[str, Any]]:
    """Наши команды Инфобаскета: id и comp_id берутся из «Конфига» (локально),
    название спрашиваем у лиги."""
    out: List[Dict[str, Any]] = []
    try:
        from enhanced_duplicate_protection import duplicate_protection
        import stats_backfill
        cfg = duplicate_protection.get_config_ids()
        comps = cfg.get("comp_ids") or []
        comp = str(comps[0]) if comps else ""
        for tid in (cfg.get("team_ids") or []):
            entry = (cfg.get("teams") or {}).get(tid) or {}
            name = ""
            try:
                info = await stats_backfill.fetch_infobasket_team(tid, comp)
                name = info.get("name") or ""
            except Exception as e:
                log.warning(f"качалка: название команды Инфобаскета {tid} — {e}")
            out.append({
                "source": "infobasket", "team_id": str(tid),
                "name": name or f"Команда {tid}",
                "league": entry.get("alt_name") or "Инфобаскет",
                "comp_id": comp, "season_id": comp, "stage_id": "", "ctx": None,
            })
    except Exception as e:
        log.warning(f"качалка: команды Инфобаскета — {e}")
    return out


def _store_teams(teams: List[Dict[str, Any]]) -> int:
    """Пишем ТОЛЬКО то, что реально приехало: пустой ответ лиги не должен
    стирать вчерашний справочник — иначе один сбой сети оставляет бота без
    команд, а игрока без пула."""
    if not teams:
        return 0
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        for t in teams:
            conn.execute(
                """INSERT INTO league_teams
                   (source, team_id, name, league, comp_id, season_id, stage_id,
                    ours, ctx_json, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                   ON CONFLICT(source, team_id) DO UPDATE SET
                     name = excluded.name, league = excluded.league,
                     comp_id = excluded.comp_id, season_id = excluded.season_id,
                     stage_id = excluded.stage_id, ours = 1,
                     ctx_json = excluded.ctx_json, fetched_at = excluded.fetched_at""",
                (t["source"], t["team_id"], t["name"], t["league"], t["comp_id"],
                 t["season_id"], t["stage_id"],
                 json.dumps(t["ctx"], ensure_ascii=False) if t.get("ctx") else "", now))
        conn.commit()
    return len(teams)


def our_teams(source: Optional[str] = None) -> List[Dict[str, Any]]:
    """Наши команды из локального справочника. Без сети."""
    sheets_cache.init_db()
    sql = "SELECT * FROM league_teams WHERE ours = 1"
    args: List[Any] = []
    if source:
        sql += " AND source = ?"
        args.append(source)
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(sql + " ORDER BY source, team_id", args)]
    for r in rows:
        r["ctx"] = json.loads(r["ctx_json"]) if r["ctx_json"] else None
    return rows


# ── Заявки: id и номера на диск, ФИО в память ───────────────────────────────

async def _fetch_roster(team: Dict[str, Any]) -> List[Dict[str, Any]]:
    """[{player_id, number, name, active}] из заявки лиги."""
    src, tid = team["source"], team["team_id"]
    if src == "slpro":
        from slpro_client import SlproClient
        rows = await SlproClient().get_roster(int(tid))
        return [{"player_id": str(p.get("player_id")), "number": str(p.get("number") or ""),
                 "name": f"{p.get('surname', '')} {p.get('name', '')}".strip(),
                 "active": True}
                for p in rows if p.get("player_id") is not None]
    import stats_backfill
    rows = await stats_backfill.fetch_infobasket_roster(tid, team.get("comp_id"))
    return [{"player_id": str(p["player_id"]), "number": str(p.get("number") or ""),
             "name": p.get("name") or "", "active": bool(p.get("active", True))}
            for p in rows if p.get("player_id") is not None]


def _store_roster(team: Dict[str, Any], players: List[Dict[str, Any]]) -> int:
    """Заявка на диск — БЕЗ ФИО. Пустую заявку не применяем по той же причине,
    что и пустой справочник команд."""
    if not players:
        return 0
    now = sheets_cache.now_iso()
    src, tid = team["source"], team["team_id"]
    with sheets_cache.get_connection() as conn:
        # Выбывших помечаем неактивными, а не удаляем: они остаются в наших
        # протоколах, и пулу нужно знать их номер.
        conn.execute("UPDATE league_rosters SET active = 0 WHERE source = ? AND team_id = ?",
                     (src, tid))
        for p in players:
            conn.execute(
                """INSERT INTO league_rosters
                   (source, team_id, player_id, number, comp_id, active, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(source, team_id, player_id) DO UPDATE SET
                     number = CASE WHEN excluded.number != '' THEN excluded.number
                                   ELSE league_rosters.number END,
                     comp_id = excluded.comp_id, active = excluded.active,
                     fetched_at = excluded.fetched_at""",
                (src, tid, p["player_id"], p["number"], str(team.get("comp_id") or ""),
                 1 if p["active"] else 0, now))
        conn.commit()
    return len(players)


def roster_of(source: str, team_id: Any) -> List[Dict[str, Any]]:
    """Заявка команды из локальной базы: [{player_id, number, active}]."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            """SELECT player_id, number, active FROM league_rosters
               WHERE source = ? AND team_id = ?""", (source, str(team_id)))]


# ── Полный проход ───────────────────────────────────────────────────────────

async def refresh(teams_only: bool = False) -> Dict[str, Any]:
    """Обновить справочники. Возвращает сводку для журнала/админки.

    Ошибки не поднимаем наверх: качалка обязана пережить недоступную лигу
    молча — с точки зрения бота это просто «сегодня свежих данных нет»."""
    sheets_cache.init_db()
    teams = await _slpro_teams() + await _infobasket_teams()
    stored_teams = _store_teams(teams)
    out = {"teams": stored_teams, "rosters": 0, "names": 0, "failed": 0}
    if teams_only:
        return out

    # Заявки берём по локальному справочнику: если лига сейчас молчит, работаем
    # по вчерашнему списку команд, а не остаёмся вовсе ни с чем.
    for team in (teams or our_teams()):
        try:
            players = await _fetch_roster(team)
        except Exception as e:
            log.warning(f"качалка: заявка {team['source']}:{team['team_id']} — {e}")
            out["failed"] += 1
            continue
        out["rosters"] += _store_roster(team, players)
        out["names"] += player_names.put_many(
            team["source"], ((p["player_id"], p["name"]) for p in players))
    return out


async def fill_missing_names(limit: int = 40) -> int:
    """Имена тех, кто есть в протоколах, но выпал из заявки.

    Такие в пуле висят как «№10», и не понять ни кто это, ни не тот ли это
    человек, что уже есть в списке под другим написанием. Ходим по одному
    игроку и только фоном; `limit` — чтобы один прогон не растянулся на
    полтысячи запросов."""
    known = set(player_names.get_all())
    teams = our_teams()
    todo: List[tuple] = []
    with sheets_cache.get_connection() as conn:
        for t in teams:
            for r in conn.execute(
                    """SELECT DISTINCT player_id FROM game_player_stats
                       WHERE source = ? AND team_id = ?""", (t["source"], t["team_id"])):
                pid = str(r["player_id"])
                if f"{t['source']}:{pid}" not in known:
                    todo.append((t["source"], pid))
    got = 0
    for source, pid in todo[:limit]:
        name = ""
        try:
            if source == "slpro":
                from slpro_client import SlproClient
                info = await SlproClient().get_player_info(pid)
                if info:
                    name = f"{info.get('surname', '')} {info.get('name', '')}".strip()
            else:
                import stats_backfill
                name = await stats_backfill.fetch_infobasket_person(pid)
        except Exception as e:
            log.warning(f"качалка: имя {source}:{pid} — {e}")
        if name:
            player_names.put(source, pid, name)
            got += 1
    return got


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    res = await refresh()
    extra = await fill_missing_names()
    print(f"Справочники лиг обновлены: команд {res['teams']}, в заявках {res['rosters']}, "
          f"имён {res['names']} (+{extra} из протоколов), ошибок {res['failed']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))
