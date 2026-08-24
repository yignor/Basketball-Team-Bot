#!/usr/bin/env python3
"""Догон результатов: опубликовать итог игры, если в своё окно не успели.

За результатом следит монитор — но только 7 часов от начала игры. Лига иногда
публикует счёт позже: 22.08.2026 матч с овертаймом попал в базу лишь ночным
добором через двое суток, окно к тому времени закрылось, и в чат не ушло
ничего. Со стороны это выглядит как пропавшая игра.

Здесь — второй заход: если счёт у нас уже есть, а результат не публиковался,
публикуем с честной пометкой, что задним числом. Молча делать вид, что всё
вовремя, нельзя: люди помнят, что сообщения не было.

Границу ставим по дате игры, а не «по любым непубликованным»: результат
недельной давности в чате — не новость, а шум.
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import sheets_cache

logger = logging.getLogger(__name__)

# Насколько старую игру ещё имеет смысл объявлять. Три дня: за это время люди
# ещё помнят матч и ждут итог, дальше сообщение только удивит.
MAX_AGE_DAYS = 3

RESULT_TYPES = {"infobasket": "РЕЗУЛЬТАТ_ИГРА", "slpro": "РЕЗУЛЬТАТ_ИГРА_SLPRO"}


def _ours() -> set:
    with sheets_cache.get_connection() as conn:
        return {str(r["team_id"]) for r in conn.execute(
            "SELECT team_id FROM league_teams WHERE ours = 1")}


def pending(today: Optional[date] = None) -> List[Dict[str, Any]]:
    """Наши сыгранные игры со счётом, по которым результат не публиковался."""
    sheets_cache.init_db()
    today = today or date.today()
    since = (today - timedelta(days=MAX_AGE_DAYS)).isoformat()
    ours = _ours()
    if not ours:
        return []
    out = []
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT * FROM game_meta
                WHERE game_date >= ? AND game_date <= ?
                  AND (home_score > 0 OR guest_score > 0)""",
            (since, today.isoformat()))]
        done = {(r["data_type"], str(r["game_id"])) for r in conn.execute(
            "SELECT data_type, game_id FROM service_records WHERE deleted = 0")}
    for g in rows:
        if str(g["home_team_id"]) not in ours and str(g["guest_team_id"]) not in ours:
            continue
        kind = RESULT_TYPES.get(g["source"])
        # Идентификатор в записях у SLPRO с префиксом, в game_meta — голый.
        ids = {str(g["game_id"]), f"slpro-{g['game_id']}"}
        if kind and any((kind, gid) in done for gid in ids):
            continue
        out.append(g)
    return out


def text(game: Dict[str, Any]) -> str:
    """Сообщение об итоге. Собирается из зеркала — в лигу не ходим."""
    ours = _ours()
    home_is_ours = str(game["home_team_id"]) in ours
    our = game["home_name"] if home_is_ours else game["guest_name"]
    opp = game["guest_name"] if home_is_ours else game["home_name"]
    us = int(game["home_score"] if home_is_ours else game["guest_score"] or 0)
    them = int(game["guest_score"] if home_is_ours else game["home_score"] or 0)
    if us > them:
        head = "✅ ПОБЕДА"
    elif us < them:
        head = "❌ ПОРАЖЕНИЕ"
    else:
        head = "🤝 НИЧЬЯ"

    lines = [f"{head}: {our} против {opp}", f"🏀 {our} {us}:{them} {opp}"]
    quarters = _quarters(game, home_is_ours)
    if quarters:
        lines.append("📊 По четвертям: " + ", ".join(quarters))
        if len(quarters) > 4:
            lines.append("⏱ С овертаймом.")
    lines += ["", "⏳ Итог пришёл с опозданием: лига опубликовала счёт позже, "
                  "чем бот следит за игрой."]
    return "\n".join(lines)


def _quarters(game: Dict[str, Any], home_is_ours: bool) -> List[str]:
    """Счёт по четвертям, всегда «мы:они». Пусто — если лига не прислала."""
    raw = game.get("quarters_json") or ""
    try:
        data = json.loads(raw) if raw else []
    except (json.JSONDecodeError, TypeError):
        return []
    out = []
    for item in data if isinstance(data, list) else []:
        if isinstance(item, dict):
            a, b = item.get("score1"), item.get("score2")
            if a is None or b is None:
                got = str(item.get("total") or "").strip()
                if got:
                    out.append(got)
                continue
            out.append(f"{a}:{b}" if home_is_ours else f"{b}:{a}")
        elif item is not None and str(item).strip():
            out.append(str(item).strip())
    return out


def mark_done(game: Dict[str, Any], note: str = "догон") -> None:
    """Помечает результат отправленным — тем же типом, что и обычный путь."""
    from enhanced_duplicate_protection import duplicate_protection
    kind = RESULT_TYPES.get(game["source"])
    if not kind:
        return
    gid = str(game["game_id"])
    if game["source"] == "slpro":
        gid = f"slpro-{gid}"
    duplicate_protection.add_record(
        kind, gid, status="РЕЗУЛЬТАТ ОТПРАВЛЕН",
        additional_data=note, game_id=gid, game_date=str(game["game_date"]))
    logger.info("Результат игры %s:%s опубликован догоном", game["source"], gid)
