#!/usr/bin/env python3
"""
Статистика игрока по personId Infobasket: все игры, все сезоны, прогресс/регресс.

Запуск:
  python player_stats.py --person-id 400566
  python player_stats.py --person-id 400566 --chat-id 123456789
  python player_stats.py --person-id 400566 --no-telegram --output report.html
"""

import argparse
import asyncio
import io
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
API = "https://reg.infobasket.su/Widget"


# ─────────────────────────── HTTP helper ─────────────────────────────────────

async def _get(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            print(f"   HTTP {r.status}: {url}")
    except Exception as exc:
        print(f"   ⚠️  {url[:80]}: {exc}")
    return None


# ─────────────────────────── Value helpers ───────────────────────────────────

def _i(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(float(str(v).replace(",", ".")))
    except Exception:
        return default


def _f(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return round(float(str(v).replace(",", ".")), 1)
    except Exception:
        return default


def _pct(m: int, a: int) -> str:
    return f"{round(m / a * 100)}%" if a else "—"


def _trend(now: float, prev: float, higher_is_better: bool = True) -> Tuple[str, str]:
    """Returns (arrow_html, css_color)."""
    delta = now - prev
    if abs(delta) < 0.05:
        return "→", "#94a3b8"
    up = delta > 0
    good = up == higher_is_better
    arrow = "↑" if up else "↓"
    color = "#22c55e" if good else "#ef4444"
    return arrow, color


def _ms_to_date(ms_str: str) -> str:
    """Convert /Date(1760648400000)/ to DD.MM.YYYY."""
    m = re.search(r"\d+", ms_str or "")
    if not m:
        return ""
    try:
        return datetime.fromtimestamp(int(m.group()) / 1000).strftime("%d.%m.%Y")
    except Exception:
        return ""


# ─────────────────────────── Data loading ────────────────────────────────────

async def fetch_player_profile(session: aiohttp.ClientSession, person_id: int) -> Dict:
    data = await _get(session, f"{API}/PlayerPage/{person_id}?format=json&lang=ru")
    if not data:
        return {}
    pos = ""
    players = data.get("Players") or []
    if players:
        pos_obj = (players[0].get("Position") or {})
        pos = pos_obj.get("PosNameRu") or ""

    return {
        "name": data.get("PersonFullNameRu") or f"ID {person_id}",
        "name_en": data.get("PersonFullNameEn") or "",
        "birth": data.get("PersonBirth") or "",
        "age": data.get("Age") or "",
        "height": data.get("PersonHeight") or "",
        "weight": data.get("PersonWeight") or "",
        "position": pos,
    }


async def fetch_season_aggregates(session: aiohttp.ClientSession, person_id: int) -> List[Dict]:
    data = await _get(session, f"{API}/PlayerSeasonStats/{person_id}?format=json&lang=ru")
    if not data:
        return []

    seasons: List[Dict] = []
    for s in data.get("SeasonStats") or []:
        comp = s.get("Season") or {}
        team_name_obj = s.get("TeamName") or {}
        seasons.append({
            "season": comp.get("CompShortNameRu") or "",
            "comp_id": comp.get("CompID"),
            "team": team_name_obj.get("CompTeamShortNameRu") or "",
            "team_id": s.get("TeamID"),
            "games": _i(s.get("GameCount")),
            "in_games": _i(s.get("InGameCount")),
            "starts": _i(s.get("StartCount")),
            "points": _i(s.get("Points")),
            "fg2_made": _i(s.get("Goal2")),
            "fg2_att": _i(s.get("Shot2")),
            "fg3_made": _i(s.get("Goal3")),
            "fg3_att": _i(s.get("Shot3")),
            "ft_made": _i(s.get("Goal1")),
            "ft_att": _i(s.get("Shot1")),
            "assists": _i(s.get("Assist")),
            "blocks": _i(s.get("Blocks")),
            "reb_def": _i(s.get("DefRebound")),
            "reb_off": _i(s.get("OffRebound")),
            "rebounds": _i(s.get("Rebound")),
            "steals": _i(s.get("Steal")),
            "turnovers": _i(s.get("Turnover")),
            "fouls": _i(s.get("Foul")),
            "plus_minus": _i(s.get("PlusMinus")),
            "played_time": s.get("PlayedTime") or "",
            "avg_time": s.get("AvgPlayedTime") or "",
            # Per-game averages (pre-calculated by API)
            "avg_points": _f(s.get("AvgPoints")),
            "avg_rebounds": _f(s.get("AvgRebound")),
            "avg_assists": _f(s.get("AvgAssist")),
            "avg_steals": _f(s.get("AvgSteal")),
            "avg_blocks": _f(s.get("AvgBlocks")),
            "avg_turnovers": _f(s.get("AvgTurnover")),
        })
    return seasons


async def fetch_game_log(
    session: aiohttp.ClientSession, person_id: int, comp_id: int, season_label: str
) -> List[Dict]:
    data = await _get(
        session,
        f"{API}/PlayerStats/{person_id}?compId={comp_id}&format=json&lang=ru"
    )
    if not data:
        return []

    log: List[Dict] = []
    for g in data.get("GameStats") or []:
        seconds = g.get("Seconds")
        # Skip games the player didn't participate in
        if not seconds:
            continue

        game = g.get("Game") or {}
        game_teams = game.get("GameTeams") or []
        team_num = _i(g.get("TeamNumber"), 1)  # 1=A, 2=B

        # Determine player's team and opponent score
        score_a = score_b = None
        if len(game_teams) >= 2:
            score_a = game_teams[0].get("GameTeamScore")
            score_b = game_teams[1].get("GameTeamScore")

        our_score = (_i(score_a) if team_num == 1 else _i(score_b)) if score_a is not None else None
        opp_score = (_i(score_b) if team_num == 1 else _i(score_a)) if score_b is not None else None

        team_a_name = (g.get("TeamNameA") or {}).get("CompTeamShortNameRu") or ""
        team_b_name = (g.get("TeamNameB") or {}).get("CompTeamShortNameRu") or ""
        our_team = team_a_name if team_num == 1 else team_b_name
        opp_team = team_b_name if team_num == 1 else team_a_name

        result = ""
        if our_score is not None and opp_score is not None:
            result = "W" if our_score > opp_score else ("L" if our_score < opp_score else "T")

        log.append({
            "game_id": game.get("GameID"),
            "date": g.get("GameDate") or _ms_to_date(str(game.get("GameDate") or "")),
            "season": season_label,
            "comp_id": comp_id,
            "our_team": our_team,
            "opp_team": opp_team,
            "our_score": our_score,
            "opp_score": opp_score,
            "result": result,
            "number": g.get("DisplayNumber") or g.get("PlayerNumber") or "—",
            "is_start": bool(g.get("IsStart")),
            "minutes": g.get("PlayedTime") or "",
            "seconds": _i(seconds),
            "points": _i(g.get("Points")),
            "fg2_made": _i(g.get("Goal2")),
            "fg2_att": _i(g.get("Shot2")),
            "fg3_made": _i(g.get("Goal3")),
            "fg3_att": _i(g.get("Shot3")),
            "ft_made": _i(g.get("Goal1")),
            "ft_att": _i(g.get("Shot1")),
            "rebounds": _i(g.get("Rebound")),
            "reb_off": _i(g.get("OffRebound")),
            "reb_def": _i(g.get("DefRebound")),
            "assists": _i(g.get("Assist")),
            "steals": _i(g.get("Steal")),
            "blocks": _i(g.get("Blocks")),
            "turnovers": _i(g.get("Turnover")),
            "fouls": _i(g.get("Foul")),
            "opp_fouls": _i(g.get("OpponentFoul")),
            "plus_minus": _i(g.get("PlusMinus")),
            "kpi": _i(g.get("KPI")),
        })

    return log


# ─────────────────────────── Analytics ───────────────────────────────────────

def compute_season_analytics(seasons: List[Dict]) -> List[Dict]:
    """
    Groups seasons by season year (comp_id). For seasons with same year
    (player on multiple teams), aggregates the stats. Returns list sorted
    by comp_id ascending (oldest first).
    """
    by_comp: Dict[int, Dict] = {}
    for s in seasons:
        cid = s["comp_id"]
        if cid not in by_comp:
            by_comp[cid] = {
                "season": s["season"],
                "comp_id": cid,
                "teams": [],
                "games": 0, "in_games": 0, "starts": 0,
                "points": 0, "rebounds": 0, "reb_off": 0, "reb_def": 0,
                "assists": 0, "steals": 0, "blocks": 0, "turnovers": 0,
                "fg2_made": 0, "fg2_att": 0,
                "fg3_made": 0, "fg3_att": 0,
                "ft_made": 0, "ft_att": 0,
                "plus_minus": 0,
            }
        b = by_comp[cid]
        b["teams"].append(s["team"])
        for k in ("games", "in_games", "starts", "points", "rebounds", "reb_off",
                  "reb_def", "assists", "steals", "blocks", "turnovers",
                  "fg2_made", "fg2_att", "fg3_made", "fg3_att", "ft_made", "ft_att",
                  "plus_minus"):
            b[k] = b.get(k, 0) + s.get(k, 0)

    result = []
    for cid in sorted(by_comp.keys()):
        b = by_comp[cid]
        n = b["in_games"] or 1
        b["avg_points"] = round(b["points"] / n, 1)
        b["avg_rebounds"] = round(b["rebounds"] / n, 1)
        b["avg_assists"] = round(b["assists"] / n, 1)
        b["avg_steals"] = round(b["steals"] / n, 1)
        b["avg_blocks"] = round(b["blocks"] / n, 1)
        b["avg_turnovers"] = round(b["turnovers"] / n, 1)
        b["fg2_pct"] = _pct(b["fg2_made"], b["fg2_att"])
        b["fg3_pct"] = _pct(b["fg3_made"], b["fg3_att"])
        b["ft_pct"] = _pct(b["ft_made"], b["ft_att"])
        b["teams"] = list(dict.fromkeys(b["teams"]))  # dedupe, preserve order
        result.append(b)

    return result


def compute_career(game_log: List[Dict]) -> Dict:
    n = len(game_log)
    if not n:
        return {}

    def tot(k): return sum(g.get(k, 0) or 0 for g in game_log)

    fg2m, fg2a = tot("fg2_made"), tot("fg2_att")
    fg3m, fg3a = tot("fg3_made"), tot("fg3_att")
    ftm, fta = tot("ft_made"), tot("ft_att")

    wins = sum(1 for g in game_log if g.get("result") == "W")
    losses = sum(1 for g in game_log if g.get("result") == "L")

    best_pts = max(game_log, key=lambda g: g.get("points", 0) or 0)
    best_reb = max(game_log, key=lambda g: g.get("rebounds", 0) or 0)
    best_ast = max(game_log, key=lambda g: g.get("assists", 0) or 0)

    def avg(k): return round(tot(k) / n, 1)

    return {
        "games": n,
        "wins": wins,
        "losses": losses,
        "total_points": tot("points"),
        "total_rebounds": tot("rebounds"),
        "total_assists": tot("assists"),
        "avg_points": avg("points"),
        "avg_rebounds": avg("rebounds"),
        "avg_assists": avg("assists"),
        "avg_steals": avg("steals"),
        "avg_blocks": avg("blocks"),
        "avg_kpi": avg("kpi"),
        "fg2_made": fg2m, "fg2_att": fg2a, "fg2_pct": _pct(fg2m, fg2a),
        "fg3_made": fg3m, "fg3_att": fg3a, "fg3_pct": _pct(fg3m, fg3a),
        "ft_made": ftm, "ft_att": fta, "ft_pct": _pct(ftm, fta),
        "best_pts": best_pts.get("points", 0),
        "best_pts_date": best_pts.get("date", ""),
        "best_pts_opp": best_pts.get("opp_team", ""),
        "best_reb": best_reb.get("rebounds", 0),
        "best_ast": best_ast.get("assists", 0),
    }


# ─────────────────────────── Main runner ─────────────────────────────────────

async def analyze_player(person_id: int) -> Dict:
    print(f"\n🏀  Анализ игрока: personId={person_id}")
    print("=" * 60)

    async with aiohttp.ClientSession() as session:
        profile, raw_seasons = await asyncio.gather(
            fetch_player_profile(session, person_id),
            fetch_season_aggregates(session, person_id),
        )

        print(f"   👤  {profile.get('name', '?')}  |  {len(raw_seasons)} строк сезонов")

        # Get per-game data for each unique comp (season)
        comp_ids = sorted({s["comp_id"] for s in raw_seasons if s["comp_id"]})
        game_log: List[Dict] = []
        seen_gids: set = set()

        for comp_id in comp_ids:
            season_label = next(
                (s["season"] for s in raw_seasons if s["comp_id"] == comp_id), str(comp_id)
            )
            print(f"   📋  Сезон {season_label} (compId={comp_id})...")
            games = await fetch_game_log(session, person_id, comp_id, season_label)
            for g in games:
                gid = g.get("game_id")
                if gid and gid in seen_gids:
                    continue
                if gid:
                    seen_gids.add(gid)
                game_log.append(g)
            print(f"        ✅  {len(games)} сыгранных игр")
            await asyncio.sleep(0.2)

    # Sort game log by date
    def parse_date(d: str) -> datetime:
        try:
            return datetime.strptime(d, "%d.%m.%Y")
        except Exception:
            return datetime.min

    game_log.sort(key=lambda g: parse_date(g.get("date", "")))

    season_analytics = compute_season_analytics(raw_seasons)
    career = compute_career(game_log)

    print(f"\n   ✅  Итого: {career.get('games', 0)} игр, "
          f"{career.get('total_points', 0)} очков за карьеру")

    return {
        "profile": profile,
        "raw_seasons": raw_seasons,
        "season_analytics": season_analytics,
        "game_log": game_log,
        "career": career,
    }


# ─────────────────────────── Telegram message ────────────────────────────────

def format_telegram(data: Dict, person_id: int) -> str:
    p = data["profile"]
    c = data["career"]
    seasons = data["season_analytics"]
    if not c:
        return f"❌ Данные для игрока (personId={person_id}) не найдены."

    # Trend vs previous season
    trend_pts = trend_reb = trend_ast = ""
    if len(seasons) >= 2:
        cur, prev = seasons[-1], seasons[-2]
        a, col = _trend(cur["avg_points"], prev["avg_points"])
        trend_pts = f" {a}"
        a, col = _trend(cur["avg_rebounds"], prev["avg_rebounds"])
        trend_reb = f" {a}"
        a, col = _trend(cur["avg_assists"], prev["avg_assists"])
        trend_ast = f" {a}"

    cur_s = seasons[-1] if seasons else {}
    prev_s = seasons[-2] if len(seasons) >= 2 else {}

    lines = [
        f"🏀 <b>{p.get('name', f'ID {person_id}')}</b>",
        f"   {p.get('position', '')}  |  {p.get('height', '')} см  {p.get('weight', '')} кг  |  {p.get('age', '')} лет",
        "",
        f"📊 <b>Карьерная статистика ({c['games']} игр)</b>",
        f"   Очки: <b>{c['avg_points']}</b> / игру   (итого {c['total_points']})",
        f"   Подборы: <b>{c['avg_rebounds']}</b> / игру   (итого {c['total_rebounds']})",
        f"   Передачи: <b>{c['avg_assists']}</b> / игру   (итого {c['total_assists']})",
        f"   Победы/Поражения: {c['wins']}–{c['losses']}",
        "",
    ]

    if cur_s:
        n = cur_s.get("in_games", cur_s.get("games", 0))
        lines += [
            f"📈 <b>Текущий сезон</b> ({cur_s['season']})",
            f"   Команды: {', '.join(cur_s.get('teams', []))}",
            f"   Игр: {n}",
            f"   Очки: <b>{cur_s['avg_points']}</b>/игру{trend_pts}",
            f"   Подборы: <b>{cur_s['avg_rebounds']}</b>/игру{trend_reb}",
            f"   Передачи: <b>{cur_s['avg_assists']}</b>/игру{trend_ast}",
            f"   Броски: 2x {cur_s['fg2_pct']}  3x {cur_s['fg3_pct']}  ШТ {cur_s['ft_pct']}",
            "",
        ]

    if prev_s and prev_s != cur_s:
        lines += [
            f"⏮️ Прошлый сезон ({prev_s['season']}): "
            f"{prev_s['avg_points']} оч / {prev_s['avg_rebounds']} под / {prev_s['avg_assists']} пер"
            f"  ({prev_s.get('in_games', prev_s.get('games'))} игр)",
            "",
        ]

    if c["best_pts"]:
        lines.append(
            f"⭐ <b>Лучший результат:</b> {c['best_pts']} очков  "
            f"({c['best_pts_date']} vs {c['best_pts_opp']})"
        )

    lines += [
        f"   Рекорд подборов: {c['best_reb']}  |  Рекорд передач: {c['best_ast']}",
        "",
        "📄 Детальный отчёт — в прикреплённом HTML файле",
    ]

    return "\n".join(lines)


# ─────────────────────────── HTML report ─────────────────────────────────────

def generate_html(data: Dict, person_id: int) -> str:
    p = data["profile"]
    c = data["career"]
    seasons = data["season_analytics"]
    game_log = data["game_log"]

    if not c:
        return (
            "<!DOCTYPE html><html><head><meta charset='utf-8'></head><body>"
            f"<h1>Игрок (personId={person_id}) не найден</h1></body></html>"
        )

    # ── Season comparison table ──────────────────────────────────
    season_rows = ""
    for i, s in enumerate(seasons):
        prev = seasons[i - 1] if i > 0 else None

        def td_trend(key: str, hib: bool = True) -> str:
            val = s[key]
            if prev:
                arrow, col = _trend(val, prev[key], hib)
                delta = val - prev[key]
                sign = "+" if delta > 0 else ""
                hint = f"{sign}{round(delta,1)}"
                return (
                    f'<td>{val} '
                    f'<span style="color:{col};font-size:.75em" title="{hint}">{arrow}</span>'
                    f'</td>'
                )
            return f'<td>{val}</td>'

        n = s.get("in_games", s.get("games", 0))
        season_rows += (
            f"<tr>"
            f"<td>{s['season']}</td>"
            f"<td>{', '.join(s['teams'])}</td>"
            f"<td>{n}</td>"
            + td_trend("avg_points")
            + td_trend("avg_rebounds")
            + td_trend("avg_assists")
            + td_trend("avg_steals")
            + td_trend("avg_blocks")
            + td_trend("avg_turnovers", hib=False)
            + f"<td>{s['fg2_pct']}</td>"
            f"<td>{s['fg3_pct']}</td>"
            f"<td>{s['ft_pct']}</td>"
            f"<td>{s['plus_minus']:+}</td>"
            f"</tr>"
        )

    # ── Game log table ───────────────────────────────────────────
    game_rows = ""
    for g in reversed(game_log):
        r = g.get("result", "")
        r_col = "#22c55e" if r == "W" else ("#ef4444" if r == "L" else "#94a3b8")
        sc = ""
        if g.get("our_score") is not None:
            sc = f'{g["our_score"]}:{g["opp_score"]}'
        pts = g.get("points", 0)
        pts_bold = "font-weight:700;" if pts >= 15 else ""
        start_mark = "★" if g.get("is_start") else ""
        pm = g.get("plus_minus", 0)
        pm_col = "#22c55e" if pm > 0 else ("#ef4444" if pm < 0 else "#94a3b8")

        def sh(m: int, a: int) -> str:
            return f"{m}/{a}<small> ({_pct(m,a)})</small>" if a else "—"

        game_rows += (
            f"<tr>"
            f"<td>{g.get('date','')}</td>"
            f"<td style='text-align:left'>{g.get('our_team','')} vs {g.get('opp_team','')}</td>"
            f"<td>{sc}</td>"
            f"<td style='color:{r_col};font-weight:700'>{r}</td>"
            f"<td>{start_mark} {g.get('minutes','')}</td>"
            f"<td style='{pts_bold}'>{pts}</td>"
            f"<td>{g.get('rebounds',0)}</td>"
            f"<td>{g.get('assists',0)}</td>"
            f"<td>{g.get('steals',0)}</td>"
            f"<td>{g.get('blocks',0)}</td>"
            f"<td>{g.get('turnovers',0)}</td>"
            f"<td>{g.get('fouls',0)}</td>"
            f"<td style='font-size:.78em'>{sh(g.get('fg2_made',0),g.get('fg2_att',0))}</td>"
            f"<td style='font-size:.78em'>{sh(g.get('fg3_made',0),g.get('fg3_att',0))}</td>"
            f"<td style='font-size:.78em'>{sh(g.get('ft_made',0),g.get('ft_att',0))}</td>"
            f"<td style='color:{pm_col}'>{'+' if pm>0 else ''}{pm}</td>"
            f"<td>{g.get('kpi',0)}</td>"
            f"<td style='font-size:.72em;color:#64748b'>{g.get('season','')}</td>"
            f"</tr>"
        )

    # ── Progress chart: avg pts per season (CSS bars) ────────────
    max_pts = max((s["avg_points"] for s in seasons), default=1) or 1
    max_reb = max((s["avg_rebounds"] for s in seasons), default=1) or 1
    bars_pts = bars_reb = bars_ast = ""
    for s in seasons:
        lbl = s["season"].replace("Сезон ", "").replace("/20", "/")
        n = s.get("in_games", s.get("games", 0))

        def bar(val: float, mx: float, col: str) -> str:
            h = max(4, int(val / mx * 80))
            return (
                f'<div class="bc">'
                f'<div class="b" style="height:{h}px;background:{col}" title="{val}"></div>'
                f'<div class="bl">{lbl}<br><small>{n}игр</small></div>'
                f'<div class="bv">{val}</div>'
                f'</div>'
            )

        bars_pts += bar(s["avg_points"], max_pts, "#f97316")
        bars_reb += bar(s["avg_rebounds"], max_reb, "#3b82f6")
        bars_ast += bar(s["avg_assists"], max((s2["avg_assists"] for s2 in seasons), default=1) or 1, "#a855f7")

    # ── KPI cards ────────────────────────────────────────────────
    def kc(val: Any, lbl: str, sub: str = "") -> str:
        return (
            f'<div class="kc">'
            f'<div class="kv">{val}</div>'
            f'<div class="kl">{lbl}</div>'
            f'{"<div class=ks>" + sub + "</div>" if sub else ""}'
            f'</div>'
        )

    def sc_card(pct: str, lbl: str, m: int, a: int) -> str:
        fill = int(m / (a or 1) * 100)
        return (
            f'<div class="sc">'
            f'<div class="sp">{pct}</div>'
            f'<div class="sl">{lbl}</div>'
            f'<div class="sd">{m}/{a}</div>'
            f'<div class="bt"><div class="bf" style="width:{fill}%"></div></div>'
            f'</div>'
        )

    prof_line = "  |  ".join(filter(None, [
        p.get("position"),
        f"{p.get('height')} см" if p.get("height") else "",
        f"{p.get('weight')} кг" if p.get("weight") else "",
        f"Возраст: {p.get('age')}" if p.get("age") else "",
        f"Дата рождения: {p.get('birth')}" if p.get("birth") else "",
    ]))

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Статистика — {p.get('name', 'Игрок')}</title>
<style>
:root{{--bg:#0f1117;--card:#1a1d27;--acc:#f97316;--txt:#e2e8f0;--mut:#64748b;--brd:#2d3147;--grn:#22c55e;--red:#ef4444}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--txt);padding:20px;max-width:1200px;margin:0 auto}}
h2{{font-size:1rem;text-transform:uppercase;letter-spacing:.08em;color:var(--mut);margin:0 0 12px}}
.sec{{margin-bottom:32px}}
.hero{{background:var(--card);border:1px solid var(--brd);border-radius:16px;padding:24px;margin-bottom:28px;display:flex;gap:24px;align-items:flex-start;flex-wrap:wrap}}
.hero-name h1{{font-size:1.9rem;color:var(--acc)}}
.hero-name .sub{{color:var(--mut);font-size:.85rem;margin-top:6px}}
.kg{{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:10px}}
.kc{{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 10px;text-align:center}}
.kv{{font-size:1.7rem;font-weight:700;color:var(--acc)}}
.kl{{font-size:.7rem;color:var(--mut);margin-top:3px}}
.ks{{font-size:.65rem;color:var(--mut);margin-top:1px}}
.sg{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}}
.sc{{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px;text-align:center}}
.sp{{font-size:1.7rem;font-weight:700}}
.sl{{font-size:.78rem;color:var(--mut);margin-top:3px}}
.sd{{font-size:.7rem;color:var(--mut);margin-top:2px}}
.bt{{background:var(--brd);border-radius:4px;height:5px;margin-top:8px;overflow:hidden}}
.bf{{background:var(--acc);height:100%;border-radius:4px}}
.chart{{display:flex;align-items:flex-end;gap:8px;background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 12px 8px;min-height:120px;overflow-x:auto;flex-wrap:nowrap}}
.bc{{display:flex;flex-direction:column;align-items:center;min-width:70px}}
.b{{border-radius:4px 4px 0 0;width:40px;min-height:4px;transition:.2s}}
.b:hover{{opacity:.75}}
.bl{{font-size:.65rem;color:var(--mut);margin-top:4px;text-align:center;line-height:1.3}}
.bv{{font-size:.72rem;color:var(--txt);margin-top:2px}}
.chart-grid{{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}}
.chart-label{{font-size:.72rem;color:var(--mut);margin-bottom:6px;font-weight:600}}
.tw{{overflow-x:auto;border-radius:12px;border:1px solid var(--brd)}}
table{{width:100%;border-collapse:collapse;background:var(--card);font-size:.82rem}}
th{{background:#21253a;padding:9px 7px;text-align:center;color:var(--mut);font-size:.68rem;text-transform:uppercase;letter-spacing:.05em;white-space:nowrap}}
td{{padding:7px 6px;text-align:center;border-top:1px solid var(--brd);white-space:nowrap}}
tr:hover td{{background:#1e2236}}
.badge-w{{color:var(--grn);font-weight:700}}
.badge-l{{color:var(--red);font-weight:700}}
.trend-note{{font-size:.7rem;color:var(--mut);margin-top:8px}}
.foot{{text-align:center;color:var(--mut);font-size:.7rem;margin-top:32px}}
</style>
</head>
<body>

<div class="hero">
  <div class="hero-name">
    <h1>{p.get('name', f'ID {person_id}')}</h1>
    <div class="sub">{prof_line}</div>
  </div>
</div>

<div class="sec">
  <h2>Карьерная статистика · {c['games']} игр</h2>
  <div class="kg">
    {kc(c['games'], 'Игры', f"{c['wins']}–{c['losses']} В–П")}
    {kc(c['avg_points'], 'Очки / игру', f'итого {c["total_points"]}')}
    {kc(c['avg_rebounds'], 'Подборы / игру', f'итого {c["total_rebounds"]}')}
    {kc(c['avg_assists'], 'Передачи / игру', f'итого {c["total_assists"]}')}
    {kc(c['avg_steals'], 'Перехваты / игру', '')}
    {kc(c['avg_blocks'], 'Блоки / игру', '')}
    {kc(c['best_pts'], 'Рекорд очков', f'{c["best_pts_date"]}')}
    {kc(c['best_reb'], 'Рекорд подборов', '')}
    {kc(c['best_ast'], 'Рекорд передач', '')}
  </div>
</div>

<div class="sec">
  <h2>Реализация бросков (карьера)</h2>
  <div class="sg">
    {sc_card(c['fg2_pct'], '2-очковые', c['fg2_made'], c['fg2_att'])}
    {sc_card(c['fg3_pct'], '3-очковые', c['fg3_made'], c['fg3_att'])}
    {sc_card(c['ft_pct'], 'Штрафные', c['ft_made'], c['ft_att'])}
  </div>
</div>

<div class="sec">
  <h2>Прогресс по сезонам</h2>
  <div class="chart-grid">
    <div>
      <div class="chart-label">🎯 Очки в среднем</div>
      <div class="chart">{bars_pts}</div>
    </div>
    <div>
      <div class="chart-label">🏀 Подборы в среднем</div>
      <div class="chart">{bars_reb}</div>
    </div>
    <div>
      <div class="chart-label">🤝 Передачи в среднем</div>
      <div class="chart">{bars_ast}</div>
    </div>
  </div>
  <p class="trend-note">↑ зелёный = прогресс · ↓ красный = регресс (vs предыдущий сезон)</p>
</div>

<div class="sec">
  <h2>Сравнение сезонов</h2>
  <div class="tw">
    <table>
      <thead><tr>
        <th>Сезон</th><th>Команды</th><th>Игр</th>
        <th>Оч/и</th><th>Под/и</th><th>Пер/и</th>
        <th>Пхв/и</th><th>Бл/и</th><th>Пот/и</th>
        <th>2x%</th><th>3x%</th><th>ШТ%</th><th>+/−</th>
      </tr></thead>
      <tbody>{season_rows}</tbody>
    </table>
  </div>
</div>

<div class="sec">
  <h2>Все игры ({len(game_log)} записей)</h2>
  <div class="tw">
    <table>
      <thead><tr>
        <th>Дата</th><th>Матч</th><th>Счёт</th><th>В/П</th>
        <th>Мин</th><th>Оч</th><th>Под</th><th>Пер</th>
        <th>Пхв</th><th>Бл</th><th>Пот</th><th>Фол</th>
        <th>2x</th><th>3x</th><th>ШТ</th><th>+/−</th><th>КПИ</th>
        <th>Сезон</th>
      </tr></thead>
      <tbody>{game_rows}</tbody>
    </table>
  </div>
</div>

<div class="foot">
  Данные: reg.infobasket.su · personId={person_id} ·
  Сформировано {datetime.now().strftime('%d.%m.%Y %H:%M')} МСК
</div>
</body>
</html>"""


# ─────────────────────────── Entry point ─────────────────────────────────────

async def main(person_id: int, out_file: str, send_tg: bool) -> None:
    data = await analyze_player(person_id)

    html = generate_html(data, person_id)
    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"\n✅  HTML сохранён: {out_file}")

    msg = format_telegram(data, person_id)
    print("\n" + "=" * 60)
    print(msg)
    print("=" * 60)

    if send_tg and BOT_TOKEN and CHAT_ID:
        from telegram import Bot
        bot = Bot(token=BOT_TOKEN)
        chat_ids = [c.strip() for c in CHAT_ID.replace(",", " ").split() if c.strip()]
        for raw in chat_ids:
            cid: Any = int(raw) if raw.lstrip("-").isdigit() else raw
            try:
                await bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                doc = io.BytesIO(html.encode("utf-8"))
                doc.name = out_file
                name = data["profile"].get("name", f"player_{person_id}")
                await bot.send_document(
                    chat_id=cid, document=doc, filename=out_file,
                    caption=f"📊 Статистика: {name}"
                )
                print(f"✅  Отправлено в {cid}")
            except Exception as exc:
                print(f"❌  Ошибка отправки в {cid}: {exc}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Статистика игрока по personId Infobasket")
    ap.add_argument("--person-id", type=int, required=True,
                    help="PersonID в Infobasket (напр. 400566)")
    ap.add_argument("--no-telegram", action="store_true",
                    help="Не отправлять в Telegram")
    ap.add_argument("--output",
                    help="Имя HTML файла (по умолчанию stats_<id>.html)")
    ap.add_argument("--chat-id",
                    help="Telegram chat_id (переопределяет CHAT_ID из .env)")
    args = ap.parse_args()

    if args.chat_id:
        os.environ["CHAT_ID"] = args.chat_id

    out = args.output or f"stats_player_{args.person_id}.html"
    asyncio.run(main(args.person_id, out, not args.no_telegram))
