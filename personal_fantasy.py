#!/usr/bin/env python3
"""
Фэнтези-срез личного отчёта: как игрока оценивали, кого выбирал он сам.

Два взгляда, и их важно не путать:

  • **Тебя выбирали.** Игрок команды — это «актив» в фэнтези: его берут в
    составы, он приносит менеджерам очки, у него есть цена и ранг. Это самая
    интересная часть личного отчёта — единственное место, где видно, как тебя
    оценивают со стороны, причём не словами, а ставками.
  • **Ты выбирал.** Если человек ещё и играет сам, показываем его результат
    среди участников.

Всё считается по снимкам очков (`fantasy_game_scores`), где для каждой игры
записано, кто у кого стоял в составе. Это те же цифры, что видит менеджер, —
задним числом они не пересчитываются ([[fantasy-scoring-invariant]]).

ФИО тут не хранятся и не появляются: работаем на ссылках вида
`slpro:707:123`, имена подставляет вызывающий из реестра в памяти.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import fantasy
import fantasy_stats
import sheets_cache


def _month_range(year: int, month: int) -> Tuple[str, str]:
    last = [31, 29 if year % 4 == 0 and (year % 100 or year % 400 == 0) else 28,
            31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last:02d}"


def _my_refs(season_id: int, source: str, player_id: str) -> set:
    """Ссылки пула, за которыми стоит ЭТОТ человек. Составная ссылка склеивает
    две лиги, поэтому ищем вхождение, а не равенство."""
    out = set()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT DISTINCT refs_json FROM fantasy_game_scores WHERE season_id = ?",
            (season_id,)).fetchall()
    for r in rows:
        try:
            refs = json.loads(r["refs_json"] or "[]")
        except (TypeError, ValueError):
            continue
        for ref in refs:
            for one in fantasy_stats.expand_refs([ref]):
                src, pid = fantasy_stats.parse_ref(one)
                if src == source and str(pid) == str(player_id):
                    out.add(ref)
    return out


def as_asset(season: Dict[str, Any], source: str, player_id: str,
             year: int, month: int, name: str = "") -> Dict[str, Any]:
    """«Тебя выбирали»: сколько раз брали в состав, сколько ты принёс, цена и ранг.

    `name` нужен ровно для одного — найти цену в листе «Игроки», где строки
    заведены по ФИО. Приходит транзитно от вызывающего, здесь не сохраняется."""
    import fantasy_prices
    sid = season["id"]
    d_from, d_to = _month_range(year, month)
    mine = _my_refs(sid, source, player_id)
    # Ни разу не выбрали — это тоже факт, и молчать о нём неправильно: цену,
    # ранг и «сколько ты стоил бы менеджеру» показать всё равно есть из чего.
    # Ссылку в этом случае собираем сами из протоколов.
    if not mine:
        with sheets_cache.get_connection() as conn:
            row = conn.execute(
                """SELECT team_id FROM game_player_stats
                   WHERE source = ? AND player_id = ? AND team_id != ''
                   ORDER BY game_date DESC LIMIT 1""",
                (source, str(player_id))).fetchone()
        if not row:
            return {}
        pref = "slpro" if source == "slpro" else "ib"
        mine = {f"{pref}:{row['team_id']}:{player_id}"}

    # Сколько фэнтези-очков я набрал в играх этого месяца — по своим протоколам.
    weights = fantasy.season_weights(season)
    with sheets_cache.get_connection() as conn:
        my_games = [dict(r) for r in conn.execute(
            """SELECT * FROM game_player_stats
               WHERE source = ? AND player_id = ? AND game_date >= ? AND game_date <= ?
               ORDER BY game_date""", (source, str(player_id), d_from, d_to))]
    fps = [(g["game_date"], round(fantasy_stats.fantasy_points(g, weights), 1))
           for g in my_games]
    total_fp = round(sum(f for _, f in fps), 1)

    picks_by_game: Dict[str, int] = {}
    managers = set()
    total_pickers = 0
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT user_id, game_id, game_date, refs_json FROM fantasy_game_scores
               WHERE season_id = ? AND game_date >= ? AND game_date <= ?""",
            (sid, d_from, d_to))]
    for r in rows:
        try:
            refs = json.loads(r["refs_json"] or "[]")
        except (TypeError, ValueError):
            continue
        n = sum(1 for x in refs if x in mine)      # мог стоять и дважды
        if n:
            picks_by_game[r["game_id"]] = picks_by_game.get(r["game_id"], 0) + n
            managers.add(str(r["user_id"]))
            total_pickers += n

    # Насколько я популярен среди всех, кого вообще ставили в этом месяце.
    counts: Dict[str, int] = {}
    for r in rows:
        try:
            refs = json.loads(r["refs_json"] or "[]")
        except (TypeError, ValueError):
            continue
        for x in refs:
            counts[x] = counts.get(x, 0) + 1
    order = sorted(counts.items(), key=lambda kv: -kv[1])
    place = next((i + 1 for i, (ref, _) in enumerate(order) if ref in mine), None)

    price: Dict[str, Any] = {}
    if name:
        import fantasy_api
        pr = fantasy_api._lookup_price(name, sheets_cache.get_player_prices())
        if pr.get("price"):
            price = fantasy_prices.progress(pr["price"], list(mine), season)
    return {
        "picked_times": total_pickers,
        "managers": len(managers),
        "games_picked": len(picks_by_game),
        # Знаменатель — ТВОИ игры месяца, а не все игры лиги: «в 0 из 70»
        # читалось как «меня игнорируют», хотя в 68 из них я и не выходил.
        "games_total": len(my_games),
        "popularity_place": place,
        "pool_size": len(order),
        "brought": round(total_fp, 1),
        "per_game": round(total_fp / len(fps), 1) if fps else 0.0,
        "by_game": fps,
        "best": max(fps, key=lambda x: x[1], default=None),
        "worst": min(fps, key=lambda x: x[1], default=None),
        "price": price,
    }


def as_manager(season: Dict[str, Any], tg_user_id: Any,
               year: int, month: int) -> Dict[str, Any]:
    """«Ты выбирал»: результат человека как участника лиги за месяц."""
    sid = season["id"]
    d_from, d_to = _month_range(year, month)
    uid = str(tg_user_id)
    with sheets_cache.get_connection() as conn:
        mine = [dict(r) for r in conn.execute(
            """SELECT game_id, game_date, points, mode, refs_json
               FROM fantasy_game_scores
               WHERE season_id = ? AND user_id = ? AND game_date >= ? AND game_date <= ?
               ORDER BY game_date""", (sid, uid, d_from, d_to))]
    if not mine:
        return {}
    board = fantasy.top_participants(sid, d_from, d_to, limit=500)
    place = next((i + 1 for i, r in enumerate(board) if r["user_id"] == uid), None)
    total = round(sum(float(r["points"] or 0) for r in mine), 1)
    best = max(mine, key=lambda r: float(r["points"] or 0))
    return {
        "games": len(mine), "points": total,
        "per_game": round(total / len(mine), 1),
        "place": place, "of": len(board),
        "leader": round(float(board[0]["points"]), 1) if board else None,
        "best_game": {"date": best["game_date"], "points": round(float(best["points"] or 0), 1)},
        "by_game": [(r["game_date"], round(float(r["points"] or 0), 1)) for r in mine],
        "mode": next((r["mode"] for r in reversed(mine) if r["mode"]), ""),
    }


def month(tg_user_id: Optional[Any], source: str, player_id: str,
          year: int, month_no: int, name: str = "") -> Dict[str, Any]:
    """Всё фэнтези за месяц одним куском. Пусто — если сезона нет."""
    sheets_cache.init_db()
    season = fantasy.get_active_season()
    if not season:
        return {}
    out: Dict[str, Any] = {"season": season.get("name", "Фэнтези")}
    asset = as_asset(season, source, player_id, year, month_no, name)
    if asset:
        out["asset"] = asset
    if tg_user_id:
        mgr = as_manager(season, tg_user_id, year, month_no)
        if mgr:
            out["manager"] = mgr
    return out if len(out) > 1 else {}
