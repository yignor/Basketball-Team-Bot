"""Личный разбор одной игры — то самое «присылать после каждой игры».

Настройка «после каждой игры» в личном кабинете существовала давно, но её
никто не читал: сообщение уходило только раз в месяц файлом. Здесь — короткий
текст в личку сразу, как только протокол игры оказался в базе.

Считаем всё по локальной копии бокс-скоров: в сеть модуль не ходит вовсе.
Сравниваем игру с тем, как человек играл ДО неё, а не со всей историей вместе
с ней — иначе игрок сравнивается сам с собой и разница всегда меньше
настоящей.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import fantasy_stats
import personal_report
import sheets_cache

logger = logging.getLogger(__name__)

# Игру старше этого срока в личку не шлём: после простоя демона незачем
# заваливать человека разборами матчей, о которых он давно забыл.
MAX_AGE_DAYS = 3

# Сколько предыдущих игр берём за «обычно».
BASE_GAMES = 5

# Показатели строки игрока: ключ, подпись, «больше — лучше».
LINE_METRICS = (
    ("pts", "очки", True),
    ("reb", "подборы", True),
    ("ast", "передачи", True),
    ("stl", "перехваты", True),
    ("blk", "блок-шоты", True),
    ("tur", "потери", False),
)


def _rows(source: str, player_id: str) -> List[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            """SELECT * FROM game_player_stats
               WHERE source = ? AND player_id = ?
               ORDER BY game_date, game_id""", (source, str(player_id)))]


def latest_game(source: str, player_id: str,
                max_age_days: int = MAX_AGE_DAYS) -> Optional[Dict[str, Any]]:
    """Последняя игра человека, если она свежая."""
    rows = _rows(source, player_id)
    if not rows:
        return None
    last = rows[-1]
    try:
        age = (date.today() - date.fromisoformat(str(last["game_date"])[:10])).days
    except ValueError:
        return None
    return last if 0 <= age <= max_age_days else None


def _opponent_name(source: str, game_id: str, my_team: str, opp_id: str) -> str:
    """Как называется соперник.

    Сначала протокол игры (лига пишет туда названия обеих команд), потом
    справочник лиг. В сеть не идём: имя соперника не стоит того, чтобы разбор
    ждал чужой сервер."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        meta = conn.execute(
            """SELECT home_team_id, home_name, guest_name FROM game_meta
               WHERE source = ? AND game_id = ?""",
            (source, str(game_id))).fetchone()
        if meta:
            at_home = str(meta["home_team_id"] or "") == str(my_team)
            name = str((meta["guest_name"] if at_home else meta["home_name"]) or "")
            if name.strip():
                return name.strip()
        if opp_id:
            row = conn.execute(
                "SELECT name FROM league_teams WHERE source = ? AND team_id = ?",
                (source, str(opp_id))).fetchone()
            if row and str(row["name"] or "").strip():
                return str(row["name"]).strip()
    return ""


def digest(source: str, source_title: str, player_id: str,
           game: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Текст разбора последней игры. None — если разбирать нечего."""
    rows = _rows(source, player_id)
    if not rows:
        return None
    game = game or rows[-1]
    key = (str(game["game_id"]), str(game["game_date"]))
    before = [r for r in rows
              if (str(r["game_id"]), str(r["game_date"])) != key
              and str(r["game_date"]) <= str(game["game_date"])]
    base = before[-BASE_GAMES:]

    import monthly_report
    ctx = monthly_report._game_context(source, str(game["game_id"]),
                                       str(game["team_id"]), str(player_id))
    opp = _opponent_name(source, str(game["game_id"]), str(game["team_id"]),
                         str(ctx.get("opponent", ""))) or "соперник"
    when = personal_report._d(str(game["game_date"]))

    head = f"🏀 Твоя игра · {when} · {opp}"
    if ctx.get("ours") is not None and ctx.get("theirs") is not None:
        outcome = ("победа" if ctx["ours"] > ctx["theirs"]
                   else "поражение" if ctx["ours"] < ctx["theirs"] else "ничья")
        head += f" · {ctx['ours']}:{ctx['theirs']} ({outcome})"
    lines = [head, ""]

    stat_bits = [f"{title} {int(game.get(k) or 0)}" for k, title, _ in LINE_METRICS
                 if int(game.get(k) or 0)]
    lines.append(" · ".join(stat_bits) if stat_bits else "В протоколе — нули.")

    mins = int(game.get("secs") or 0) // 60
    if mins:
        lines.append(f"На площадке {mins} мин.")

    shots = []
    if int(game.get("fga") or 0):
        shots.append(f"с игры {int(game['fgm'])}/{int(game['fga'])}")
    if int(game.get("tpa") or 0):
        shots.append(f"трёхочковые {int(game['tpm'])}/{int(game['tpa'])}")
    if int(game.get("fta") or 0):
        shots.append(f"штрафные {int(game['ftm'])}/{int(game['fta'])}")
    if shots:
        lines.append("Броски: " + ", ".join(shots) + ".")

    fp = fantasy_stats.fantasy_points(game)
    lines.append("")
    if base:
        base_fp = sum(fantasy_stats.fantasy_points(r) for r in base) / len(base)
        diff = round(fp - base_fp, 1)
        mark = "выше" if diff > 0 else "ниже" if diff < 0 else "как"
        lines.append(f"Фэнтези-очки за игру: {fp:g} — это {mark} твоего среднего "
                     f"за последние {len(base)} игр ({base_fp:.1f}).")
    else:
        lines.append(f"Фэнтези-очки за игру: {fp:g}. Это первая игра в базе — "
                     "сравнивать пока не с чем.")

    if base:
        deltas = []
        for k, title, higher_better in LINE_METRICS:
            now = float(game.get(k) or 0)
            was = sum(float(r.get(k) or 0) for r in base) / len(base)
            delta = round(now - was, 1)
            if abs(delta) < 1:
                continue
            good = (delta > 0) == higher_better
            deltas.append((abs(delta), f"{'📈' if good else '📉'} {title} "
                                       f"{'+' if delta > 0 else ''}{delta:g}"))
        if deltas:
            deltas.sort(key=lambda x: -x[0])
            lines += ["", "Против своего обычного: "
                          + ", ".join(t for _, t in deltas[:4]) + "."]

    lines += ["", f"Лига: {source_title}. Настройки — «📊 Моя статистика»."]
    return "\n".join(lines)


# ─────────────────── Кому и что уже отправляли ─────────────────────────────

def sent_key(source: str, game_id: str, tg_user_id: Any) -> str:
    return f"pers:{source}:{game_id}:{tg_user_id}"


def already_sent(key: str) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return bool(conn.execute("SELECT 1 FROM pay_events WHERE event_key = ?",
                                 (key,)).fetchone())


def mark_sent(key: str, details: str = "") -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO pay_events (event_key, sent_at, details) "
            "VALUES (?, ?, ?)",
            (key, datetime.now().isoformat(timespec="seconds"), details))
        conn.commit()


def pending() -> List[Dict[str, Any]]:
    """Кому пора отправить разбор: [{uid, source, title, player_id, game, key}].

    Только те, у кого стоит «после каждой игры» и включена личная подписка."""
    import player_identity
    import subscriptions

    titles = {"slpro": "SLPRO", "infobasket": "Инфобаскет"}
    out: List[Dict[str, Any]] = []
    for uid in player_identity.linked_users():
        prefs = personal_report.get_prefs(uid)
        if (prefs.get("notify_mode") or "game") != "game":
            continue
        if not subscriptions.enabled(uid, "personal"):
            continue
        for ident in player_identity.get_identities(uid):
            source, pid = str(ident["source"]), str(ident["player_id"])
            game = latest_game(source, pid)
            if not game:
                continue
            key = sent_key(source, str(game["game_id"]), uid)
            if already_sent(key):
                continue
            out.append({"uid": str(uid), "source": source,
                        "title": titles.get(source, source),
                        "player_id": pid, "game": game, "key": key})
    return out
