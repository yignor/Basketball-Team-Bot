#!/usr/bin/env python3
"""
Аналитика команды за всё время: результаты, лиги, тренды по сезонам.

Запуск:
  python team_stats.py --team-ids 36502 42347
  python team_stats.py --team-ids 36502 --no-telegram --output report.html
  python team_stats.py --team-ids 32855 36502 42347 --chat-id 123456789
"""

import argparse
import asyncio
import io
import os
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
API_WIDGET = "https://reg.infobasket.su/Widget"

KNOWN_SEASON_COMP_IDS = [73582, 88649, 108009]  # 2023/24, 2024/25, 2025/26
SEASON_LABELS = {73582: "2023/24", 88649: "2024/25", 108009: "2025/26"}


# ──────────────────────────────────────────────────────────────────────────────
# HTTP
# ──────────────────────────────────────────────────────────────────────────────

async def _get(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
    except Exception as exc:
        print(f"   ⚠️  {url[-70:]}: {exc}")
    return None


def _i(v: Any) -> int:
    try:
        return int(float(str(v or 0).replace(",", ".")))
    except Exception:
        return 0


# ──────────────────────────────────────────────────────────────────────────────
# Data fetching
# ──────────────────────────────────────────────────────────────────────────────

async def fetch_team_seasons(session: aiohttp.ClientSession, team_id: int) -> List[Dict]:
    data = await _get(session, f"{API_WIDGET}/GetTeamSeasons/{team_id}?format=json&lang=ru")
    if isinstance(data, list):
        return data
    return []


async def fetch_team_profile(session: aiohttp.ClientSession, team_id: int) -> Dict:
    data = await _get(session, f"{API_WIDGET}/TeamPage/{team_id}?format=json&lang=ru")
    if isinstance(data, dict):
        return data
    return {}


async def fetch_season_games(
    session: aiohttp.ClientSession, team_id: int, comp_id: int, season_label: str
) -> List[Dict]:
    data = await _get(session, f"{API_WIDGET}/TeamGames/{team_id}?compId={comp_id}&format=json&lang=ru")
    if not isinstance(data, list):
        return []

    games: List[Dict] = []
    for g in data:
        tid_a = g.get("TeamAid")
        is_home = (tid_a == team_id)
        score_a = g.get("ScoreA")
        score_b = g.get("ScoreB")
        has_score = score_a is not None and score_b is not None
        our_score = _i(score_a) if is_home else _i(score_b)
        opp_score = _i(score_b) if is_home else _i(score_a)
        result = ""
        if has_score:
            result = "W" if our_score > opp_score else ("L" if our_score < opp_score else "T")

        opp_name = (g.get("ShortTeamNameBru") if is_home else g.get("ShortTeamNameAru")) or "?"

        games.append({
            "game_id":     g.get("GameID"),
            "date":        g.get("GameDate") or "",
            "is_home":     is_home,
            "opp_name":    opp_name,
            "our_score":   our_score if has_score else None,
            "opp_score":   opp_score if has_score else None,
            "result":      result,
            "league":      g.get("LeagueShortNameRu") or g.get("LeagueNameRu") or "?",
            "league_full": g.get("LeagueNameRu") or "",
            "round":       g.get("CompNameRu") or "",
            "season":      season_label,
            "comp_id":     comp_id,
        })

    return games


async def fetch_all_team_data(session: aiohttp.ClientSession, team_id: int) -> Tuple[str, List[Dict]]:
    api_seasons = await fetch_team_seasons(session, team_id)
    api_comp_ids = {s["CompID"] for s in api_seasons}
    known_labels = {s["CompID"]: s["SeasonName"] for s in api_seasons}

    # Primary: seasons from API (correct labels); supplemental: extra known comp IDs
    primary_comp_ids   = sorted(api_comp_ids)
    supp_comp_ids      = sorted(set(KNOWN_SEASON_COMP_IDS) - api_comp_ids)
    ordered_comp_ids   = primary_comp_ids + supp_comp_ids

    tasks = []
    for cid in ordered_comp_ids:
        label = known_labels.get(cid) or SEASON_LABELS.get(cid) or f"CompID {cid}"
        tasks.append(fetch_season_games(session, team_id, cid, label))

    results = await asyncio.gather(*tasks)

    all_games: List[Dict] = []
    seen_ids: set = set()
    # Process primary seasons first so their labels win on duplicate game IDs
    for games_chunk in results:
        for g in games_chunk:
            gid = g["game_id"]
            if gid and gid in seen_ids:
                continue
            if gid:
                seen_ids.add(gid)
            all_games.append(g)

    profile = await fetch_team_profile(session, team_id)
    team_name = profile.get("TeamShortNameRu") or profile.get("TeamNameRu") or str(team_id)

    all_games.sort(key=lambda g: g["date"])
    return team_name, all_games


# ──────────────────────────────────────────────────────────────────────────────
# Analytics
# ──────────────────────────────────────────────────────────────────────────────

def compute_analytics(team_name: str, team_id: int, all_games: List[Dict]) -> Dict:
    played = [g for g in all_games if g.get("result")]
    if not played:
        return {"team_name": team_name, "team_id": team_id, "all_games": all_games,
                "played": [], "has_data": False}

    n = len(played)
    wins   = sum(1 for g in played if g["result"] == "W")
    losses = sum(1 for g in played if g["result"] == "L")
    total_our  = sum(g["our_score"] for g in played)
    total_opp  = sum(g["opp_score"] for g in played)
    home_g = [g for g in played if g["is_home"]]
    away_g = [g for g in played if not g["is_home"]]

    # By season
    seasons_order: List[str] = []
    by_season: Dict[str, Dict] = {}
    for g in played:
        s = g["season"]
        if s not in by_season:
            by_season[s] = {"w": 0, "l": 0, "our": 0, "opp": 0,
                            "leagues": set(), "games": 0, "comp_id": g["comp_id"]}
            seasons_order.append(s)
        by_season[s]["games"] += 1
        by_season[s]["our"]   += g["our_score"]
        by_season[s]["opp"]   += g["opp_score"]
        by_season[s]["leagues"].add(g["league"])
        if g["result"] == "W":
            by_season[s]["w"] += 1
        else:
            by_season[s]["l"] += 1

    # Sort seasons chronologically by comp_id (lower = older season)
    seasons_order.sort(key=lambda s: by_season[s]["comp_id"])

    for s in by_season:
        d = by_season[s]
        d["leagues"] = sorted(d["leagues"])
        g_cnt = d["w"] + d["l"]
        d["avg_our"]  = round(d["our"]  / g_cnt, 1) if g_cnt else 0
        d["avg_opp"]  = round(d["opp"]  / g_cnt, 1) if g_cnt else 0
        d["avg_diff"] = round((d["our"] - d["opp"]) / g_cnt, 1) if g_cnt else 0
        d["win_pct"]  = round(d["w"] / g_cnt * 100) if g_cnt else 0

    # By league (all time)
    by_league: Dict[str, Dict] = {}
    for g in played:
        lg = g["league"]
        if lg not in by_league:
            by_league[lg] = {"full": g["league_full"], "w": 0, "l": 0,
                             "our": 0, "opp": 0, "seasons": set()}
        by_league[lg]["seasons"].add(g["season"])
        by_league[lg]["our"]  += g["our_score"]
        by_league[lg]["opp"]  += g["opp_score"]
        if g["result"] == "W":
            by_league[lg]["w"] += 1
        else:
            by_league[lg]["l"] += 1

    for lg in by_league:
        d = by_league[lg]
        d["seasons"] = sorted(d["seasons"])
        t = d["w"] + d["l"]
        d["win_pct"]  = round(d["w"] / t * 100) if t else 0
        d["avg_our"]  = round(d["our"] / t, 1) if t else 0
        d["avg_opp"]  = round(d["opp"] / t, 1) if t else 0
        d["avg_diff"] = round((d["our"] - d["opp"]) / t, 1) if t else 0

    # Streak
    streak_char = played[-1]["result"]
    streak = 1
    for g in reversed(played[:-1]):
        if g["result"] == streak_char:
            streak += 1
        else:
            break

    best  = max(played, key=lambda g: g["our_score"] - g["opp_score"])
    worst = min(played, key=lambda g: g["our_score"] - g["opp_score"])

    return {
        "team_name": team_name, "team_id": team_id,
        "all_games": all_games, "played": played, "has_data": True,
        "n": n, "wins": wins, "losses": losses,
        "win_pct":  round(wins / n * 100),
        "avg_our":  round(total_our  / n, 1),
        "avg_opp":  round(total_opp  / n, 1),
        "avg_diff": round((total_our - total_opp) / n, 1),
        "home_w": sum(1 for g in home_g if g["result"] == "W"), "home_n": len(home_g),
        "away_w": sum(1 for g in away_g if g["result"] == "W"), "away_n": len(away_g),
        "by_season": by_season, "seasons_order": seasons_order,
        "by_league": by_league,
        "streak_char": streak_char, "streak": streak,
        "best": best, "worst": worst,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Telegram
# ──────────────────────────────────────────────────────────────────────────────

def format_telegram(teams_data: List[Dict]) -> str:
    parts: List[str] = []
    for td in teams_data:
        if not td.get("has_data"):
            parts.append(f"⚠️ Нет данных для {td.get('team_name', td.get('team_id'))}")
            continue

        slines = ""
        for s in td["seasons_order"]:
            sd = td["by_season"][s]
            ds = "+" if sd["avg_diff"] >= 0 else ""
            slines += f"\n   {s}: {sd['w']}–{sd['l']} ({sd['win_pct']}%)  ср. {sd['avg_our']}:{sd['avg_opp']}  ({ds}{sd['avg_diff']})"

        llines = ""
        for lg, ld in sorted(td["by_league"].items(), key=lambda x: -(x[1]["w"]+x[1]["l"])):
            total = ld["w"] + ld["l"]
            llines += f"\n  [{lg}] {ld['w']}–{ld['l']} ({ld['win_pct']}%)  ср. {ld['avg_our']}:{ld['avg_opp']}"

        streak_e = "🔥" if td["streak_char"] == "W" else "❄️"
        ds = "+" if td["avg_diff"] >= 0 else ""

        parts.append(
            f"🏀 <b>{td['team_name']}</b>\n"
            f"   Всего: <b>{td['n']} игр</b>  {td['wins']}–{td['losses']} ({td['win_pct']}%)\n"
            f"   Ср. счёт: {td['avg_our']}:{td['avg_opp']}  ({ds}{td['avg_diff']})\n"
            f"   Дома: {td['home_w']}/{td['home_n']}  ·  В гостях: {td['away_w']}/{td['away_n']}\n"
            f"   {streak_e} Текущая серия: {td['streak']}\n"
            f"\n📅 <b>По сезонам:</b>{slines}\n"
            f"\n🏆 <b>По лигам:</b>{llines}\n"
            f"\n⭐ Лучшая победа: {td['best']['our_score']}:{td['best']['opp_score']}"
            f" vs {td['best']['opp_name']} ({td['best']['date'][:10]})\n"
            f"💔 Худшее поражение: {td['worst']['our_score']}:{td['worst']['opp_score']}"
            f" vs {td['worst']['opp_name']} ({td['worst']['date'][:10]})"
        )

    return "\n\n".join(parts) + "\n\n📄 Детальный HTML отчёт прикреплён"


# ──────────────────────────────────────────────────────────────────────────────
# HTML
# ──────────────────────────────────────────────────────────────────────────────

def _svg_trend(seasons_order: List[str], by_season: Dict[str, Dict],
               key: str, label: str, color: str) -> str:
    vals = [by_season[s].get(key, 0) for s in seasons_order]
    if len(vals) < 2:
        return ""
    W, H, pad = 300, 90, 28
    iw, ih = W - pad * 2, H - pad
    mn, mx = min(vals), max(vals)
    span = mx - mn or 1

    def sx(i): return pad + i / (len(vals) - 1) * iw
    def sy(v): return H - pad - (v - mn) / span * ih

    pts = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in enumerate(vals))
    circles = "".join(
        f'<circle cx="{sx(i):.1f}" cy="{sy(v):.1f}" r="3.5" fill="{color}"/>'
        f'<text x="{sx(i):.1f}" y="{sy(v)-8:.1f}" text-anchor="middle" font-size="9" fill="#e2e8f0">{v}</text>'
        for i, v in enumerate(vals)
    )
    xlabels = "".join(
        f'<text x="{sx(i):.1f}" y="{H-4}" text-anchor="middle" font-size="8" fill="#64748b">{s}</text>'
        for i, s in enumerate(seasons_order)
    )
    return (
        f'<div class="svg-wrap"><div class="svg-lbl">{label}</div>'
        f'<svg viewBox="0 0 {W} {H}" width="100%" height="{H}">'
        f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2.5" '
        f'stroke-linejoin="round" stroke-linecap="round"/>'
        f'{circles}{xlabels}</svg></div>'
    )


def _kc(val: Any, label: str, sub: str = "") -> str:
    return (
        f'<div class="kc"><div class="kv">{val}</div>'
        f'<div class="kl">{label}</div>'
        f'{"<div class=ks>" + sub + "</div>" if sub else ""}</div>'
    )


def generate_html(teams_data: List[Dict]) -> str:
    sections = ""

    for td in teams_data:
        tname = td.get("team_name", str(td.get("team_id", "?")))
        if not td.get("has_data"):
            sections += f'<div class="sec"><h1>{tname}</h1><p style="color:#64748b">Нет данных.</p></div>'
            continue

        played        = td["played"]
        all_games     = td["all_games"]
        by_season     = td["by_season"]
        seasons_order = td["seasons_order"]
        by_league     = td["by_league"]
        streak_e      = "🔥" if td["streak_char"] == "W" else "❄️"
        diff_sign     = "+" if td["avg_diff"] >= 0 else ""
        diff_col      = "#22c55e" if td["avg_diff"] >= 0 else "#ef4444"

        # Season table
        season_rows = ""
        for s in seasons_order:
            sd = by_season[s]
            wc = "#22c55e" if sd["win_pct"] >= 50 else "#ef4444"
            dc = "#22c55e" if sd["avg_diff"] >= 0 else "#ef4444"
            ds = "+" if sd["avg_diff"] >= 0 else ""
            season_rows += (
                f'<tr><td>{s}</td><td>{sd["games"]}</td>'
                f'<td style="color:{wc};font-weight:700">{sd["w"]}–{sd["l"]}</td>'
                f'<td style="color:{wc}">{sd["win_pct"]}%</td>'
                f'<td>{sd["avg_our"]}:{sd["avg_opp"]}</td>'
                f'<td style="color:{dc};font-weight:700">{ds}{sd["avg_diff"]}</td>'
                f'<td style="font-size:.75em">{", ".join(sd["leagues"])}</td></tr>'
            )

        # League table
        league_rows = ""
        for lg, ld in sorted(by_league.items(), key=lambda x: -(x[1]["w"]+x[1]["l"])):
            total = ld["w"] + ld["l"]
            wc = "#22c55e" if ld["win_pct"] >= 50 else "#ef4444"
            dc = "#22c55e" if ld["avg_diff"] >= 0 else "#ef4444"
            ds = "+" if ld["avg_diff"] >= 0 else ""
            league_rows += (
                f'<tr><td style="text-align:left">{ld["full"] or lg}</td>'
                f'<td>[{lg}]</td><td>{total}</td>'
                f'<td style="color:{wc};font-weight:700">{ld["w"]}–{ld["l"]}</td>'
                f'<td style="color:{wc}">{ld["win_pct"]}%</td>'
                f'<td>{ld["avg_our"]}:{ld["avg_opp"]}</td>'
                f'<td style="color:{dc};font-weight:700">{ds}{ld["avg_diff"]}</td>'
                f'<td style="font-size:.75em">{", ".join(ld["seasons"])}</td></tr>'
            )

        # Bar chart (all played games)
        max_s = max(max(g["our_score"], g["opp_score"]) for g in played) or 1
        bars = ""
        for g in played:
            our, opp = g["our_score"], g["opp_score"]
            h_our = max(4, int(our / max_s * 80))
            h_opp = max(4, int(opp / max_s * 80))
            col   = "#22c55e" if our > opp else "#ef4444"
            bars += (
                f'<div class="bc" title="{g["season"]} · {g["round"]}">'
                f'<div class="sp">'
                f'<div class="b" style="height:{h_our}px;background:{col}"></div>'
                f'<div class="b opp" style="height:{h_opp}px"></div>'
                f'</div>'
                f'<div class="bl">{g["date"][:5]}<br><small>{g["opp_name"][:8]}</small></div>'
                f'<div class="bv">{our}:{opp}</div>'
                f'</div>'
            )

        # Game log
        game_rows = ""
        for g in reversed(all_games):
            r  = g.get("result", "")
            rc = "#22c55e" if r == "W" else ("#ef4444" if r == "L" else "#94a3b8")
            ha = "🏠" if g.get("is_home") else "✈️"
            sc = f"{g['our_score']}:{g['opp_score']}" if g.get("our_score") is not None else "—"
            diff = ""
            if r:
                d  = g["our_score"] - g["opp_score"]
                dc = "#22c55e" if d > 0 else "#ef4444"
                diff = f'<span style="color:{dc}">{d:+}</span>'
            op = 1.0 if r else 0.5
            game_rows += (
                f'<tr style="opacity:{op}">'
                f'<td>{g["date"][:10]}</td><td>{g["season"]}</td>'
                f'<td>{ha}</td>'
                f'<td style="text-align:left">{g["opp_name"]}</td>'
                f'<td><b>{sc}</b></td>'
                f'<td style="color:{rc};font-weight:700">{r}</td>'
                f'<td>{diff}</td>'
                f'<td style="font-size:.75em">{g["round"]}</td>'
                f'<td style="font-size:.75em">{g["league"]}</td>'
                f'</tr>'
            )

        # SVG trends
        trends = (
            _svg_trend(seasons_order, by_season, "win_pct",  "% побед",       "#f97316") +
            _svg_trend(seasons_order, by_season, "avg_our",  "Ср. набрано",   "#22c55e") +
            _svg_trend(seasons_order, by_season, "avg_opp",  "Ср. пропущено", "#ef4444") +
            _svg_trend(seasons_order, by_season, "avg_diff", "Разность",      "#60a5fa")
        )

        sections += f"""
<div class="sec">
  <h1>{tname}</h1>
  <div class="sub">ID {td['team_id']} · {len(seasons_order)} сезонов · {len(all_games)} матчей запланировано</div>

  <h2>Общая статистика за всё время</h2>
  <div class="kg">
    {_kc(f"{td['wins']}–{td['losses']}", "Победы–Поражения", f"{td['win_pct']}%")}
    {_kc(td['n'], "Сыграно матчей", f"{len(seasons_order)} сезонов")}
    {_kc(td['avg_our'], "Ср. набрано", "очков/игру")}
    {_kc(td['avg_opp'], "Ср. пропущено", "очков/игру")}
    {_kc(f'<span style="color:{diff_col}">{diff_sign}{td["avg_diff"]}</span>', "Разность", "очков/игру")}
    {_kc(f"{td['home_w']}/{td['home_n']}", "Дома", "победы/игр")}
    {_kc(f"{td['away_w']}/{td['away_n']}", "В гостях", "победы/игр")}
    {_kc(f"{streak_e} {td['streak']}", f'Серия {"побед" if td["streak_char"]=="W" else "поражений"}', "")}
  </div>

  <h2>Тренды по сезонам</h2>
  <div class="trends">{trends}</div>

  <h2>По сезонам</h2>
  <div class="tw"><table>
    <thead><tr><th>Сезон</th><th>Игр</th><th>В–П</th><th>%</th><th>Ср. счёт</th><th>Разн.</th><th>Лиги</th></tr></thead>
    <tbody>{season_rows}</tbody>
  </table></div>

  <h2>По лигам (за всё время)</h2>
  <div class="tw"><table>
    <thead><tr><th>Лига</th><th>Код</th><th>Игр</th><th>В–П</th><th>%</th><th>Ср. счёт</th><th>Разн.</th><th>Сезоны</th></tr></thead>
    <tbody>{league_rows}</tbody>
  </table></div>

  <h2>Счёт по матчам — все сезоны</h2>
  <div class="legend">
    <span class="dot" style="background:#22c55e"></span> Наша команда &nbsp;
    <span class="dot" style="background:#475569"></span> Соперник
  </div>
  <div class="chart">{bars}</div>

  <h2>Все матчи ({len(all_games)} запланировано · {len(played)} сыграно)</h2>
  <div class="tw"><table>
    <thead><tr><th>Дата</th><th>Сезон</th><th></th><th>Соперник</th><th>Счёт</th><th>В/П</th><th>+/−</th><th>Этап</th><th>Лига</th></tr></thead>
    <tbody>{game_rows}</tbody>
  </table></div>
</div>
"""

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Аналитика команды — все сезоны</title>
<style>
:root{{--bg:#0f1117;--card:#1a1d27;--acc:#f97316;--txt:#e2e8f0;--mut:#64748b;--brd:#2d3147}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--txt);padding:20px;max-width:1200px;margin:0 auto}}
.sec{{margin-bottom:56px;padding-bottom:40px;border-bottom:1px solid var(--brd)}}
.sec:last-child{{border-bottom:none}}
h1{{font-size:1.9rem;color:var(--acc);margin-bottom:4px}}
.sub{{color:var(--mut);font-size:.82rem;margin-bottom:20px}}
h2{{font-size:.7rem;text-transform:uppercase;letter-spacing:.1em;color:var(--mut);margin:24px 0 10px}}
.kg{{display:grid;grid-template-columns:repeat(auto-fill,minmax(115px,1fr));gap:10px;margin-bottom:16px}}
.kc{{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 10px;text-align:center}}
.kv{{font-size:1.45rem;font-weight:700;color:var(--acc)}}
.kl{{font-size:.7rem;color:var(--mut);margin-top:3px}}
.ks{{font-size:.62rem;color:var(--mut)}}
.trends{{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px;margin-bottom:16px}}
.svg-wrap{{background:var(--card);border:1px solid var(--brd);border-radius:10px;padding:12px}}
.svg-lbl{{font-size:.68rem;color:var(--mut);text-transform:uppercase;letter-spacing:.08em;margin-bottom:4px}}
.legend{{font-size:.72rem;color:var(--mut);display:flex;align-items:center;gap:4px;margin-bottom:6px}}
.dot{{display:inline-block;width:9px;height:9px;border-radius:50%}}
.chart{{display:flex;align-items:flex-end;gap:5px;background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 12px 8px;min-height:130px;overflow-x:auto;margin-bottom:16px}}
.bc{{display:flex;flex-direction:column;align-items:center;min-width:38px;flex-shrink:0}}
.sp{{display:flex;align-items:flex-end;gap:2px;height:88px}}
.b{{border-radius:3px 3px 0 0;width:16px;min-height:4px}}
.b.opp{{background:#475569!important}}
.bl{{font-size:.56rem;color:var(--mut);margin-top:3px;text-align:center;line-height:1.3}}
.bv{{font-size:.6rem;color:var(--txt);margin-top:1px}}
.tw{{overflow-x:auto;border-radius:12px;border:1px solid var(--brd);margin-bottom:16px}}
table{{width:100%;border-collapse:collapse;background:var(--card);font-size:.8rem}}
th{{background:#21253a;padding:8px 6px;text-align:center;color:var(--mut);font-size:.64rem;text-transform:uppercase;letter-spacing:.05em;white-space:nowrap}}
td{{padding:6px 5px;text-align:center;border-top:1px solid var(--brd);white-space:nowrap}}
tr:hover td{{background:#1e2236}}
.foot{{text-align:center;color:var(--mut);font-size:.68rem;margin-top:32px;padding-top:16px;border-top:1px solid var(--brd)}}
</style>
</head>
<body>
{sections}
<div class="foot">
  Данные: reg.infobasket.su · Все сезоны ·
  Отчёт сформирован {datetime.now().strftime('%d.%m.%Y %H:%M')} МСК
</div>
</body>
</html>"""


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

async def main(team_ids: List[int], out_file: str, send_tg: bool) -> None:
    print(f"\n🏀  Исторические данные: {team_ids}")
    print("=" * 60)

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_all_team_data(session, tid) for tid in team_ids]
        results = await asyncio.gather(*tasks)

    teams_data: List[Dict] = []
    for (tname, games), tid in zip(results, team_ids):
        played = [g for g in games if g.get("result")]
        print(f"   {tname} (ID {tid}): {len(games)} игр, {len(played)} сыграно")
        analytics = compute_analytics(tname, tid, games)
        teams_data.append(analytics)

    html = generate_html(teams_data)
    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"\n✅  HTML: {out_file}")

    msg = format_telegram(teams_data)
    print("\n" + "=" * 60)
    print(msg)
    print("=" * 60)

    if send_tg and BOT_TOKEN and CHAT_ID:
        from telegram import Bot
        bot = Bot(token=BOT_TOKEN)
        for raw in CHAT_ID.replace(",", " ").split():
            cid: Any = int(raw) if raw.lstrip("-").isdigit() else raw
            try:
                await bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                doc = io.BytesIO(html.encode("utf-8"))
                doc.name = out_file
                names = [td["team_name"] for td in teams_data]
                await bot.send_document(chat_id=cid, document=doc, filename=out_file,
                                        caption=f"📊 {', '.join(names)} — все сезоны")
                print(f"✅  Telegram → {cid}")
            except Exception as exc:
                print(f"❌  Ошибка {cid}: {exc}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Аналитика команды за все сезоны")
    ap.add_argument("--team-ids", type=int, nargs="+", required=True,
                    help="TeamID (напр: 36502 42347 32855)")
    ap.add_argument("--no-telegram", action="store_true")
    ap.add_argument("--output", help="Имя HTML файла")
    ap.add_argument("--chat-id", help="Telegram chat_id")
    args = ap.parse_args()

    if args.chat_id:
        os.environ["CHAT_ID"] = args.chat_id

    out = args.output or f"team_stats_{'_'.join(str(x) for x in args.team_ids)}.html"
    asyncio.run(main(args.team_ids, out, not args.no_telegram))
