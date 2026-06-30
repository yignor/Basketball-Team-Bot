#!/usr/bin/env python3
"""
Аналитика команды: результаты, лиги, тренды.

Запуск:
  python team_stats.py --team-ids 32855 36502 42347
  python team_stats.py --team-ids 32855 --person-id 400566 --chat-id 123456789
  python team_stats.py --team-ids 32855 --no-telegram --output team_report.html
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
API_COMP = "https://reg.infobasket.su/Comp"


# ─────────────────────────── HTTP helper ─────────────────────────────────────

async def _get(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                return await r.json(content_type=None)
    except Exception as exc:
        print(f"   ⚠️  {url[:80]}: {exc}")
    return None


def _i(v: Any, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(float(str(v).replace(",", ".")))
    except Exception:
        return default


def _f(v: Any) -> float:
    if v is None:
        return 0.0
    try:
        return round(float(str(v).replace(",", ".")), 1)
    except Exception:
        return 0.0


def _pct(m: int, a: int) -> str:
    return f"{round(m / a * 100)}%" if a else "—"


# ─────────────────────────── Data fetching ───────────────────────────────────

async def fetch_team_games(
    session: aiohttp.ClientSession, team_id: int
) -> Tuple[str, List[Dict]]:
    """Returns (team_name, list of game dicts)."""
    data = await _get(session, f"{API_WIDGET}/TeamGames/{team_id}?format=json&lang=ru")
    if not isinstance(data, list) or not data:
        return (str(team_id), [])

    team_name = ""
    games: List[Dict] = []

    for g in data:
        tid_a = g.get("TeamAid")
        is_home = (tid_a == team_id)

        if not team_name:
            team_name = (
                g.get("ShortTeamNameBru") if is_home is False
                else g.get("ShortTeamNameAru") or g.get("ShortTeamNameBru") or str(team_id)
            )
            # Determine our team's name from the response
            if tid_a == team_id:
                team_name = g.get("ShortTeamNameAru") or str(team_id)
            else:
                team_name = g.get("ShortTeamNameBru") or str(team_id)

        score_a = g.get("ScoreA")
        score_b = g.get("ScoreB")
        our_score = _i(score_a) if is_home else _i(score_b)
        opp_score = _i(score_b) if is_home else _i(score_a)
        has_score = score_a is not None and score_b is not None
        result = ""
        if has_score:
            result = "W" if our_score > opp_score else ("L" if our_score < opp_score else "T")

        opp_name = (
            g.get("ShortTeamNameBru") if is_home else g.get("ShortTeamNameAru")
        ) or "?"

        games.append({
            "game_id": g.get("GameID"),
            "date": g.get("GameDate") or "",
            "time": g.get("GameTimeMsk") or "",
            "arena": g.get("ArenaRu") or "",
            "is_home": is_home,
            "opp_name": opp_name,
            "our_score": our_score if has_score else None,
            "opp_score": opp_score if has_score else None,
            "result": result,
            "league": g.get("LeagueShortNameRu") or g.get("LeagueNameRu") or "?",
            "league_full": g.get("LeagueNameRu") or "",
            "round": g.get("CompNameRu") or "",
            "game_status": g.get("GameStatus", 0),
        })

    games.sort(key=lambda g: g["date"])
    return (team_name, games)


async def fetch_player_season_context(
    session: aiohttp.ClientSession, person_id: int
) -> List[Dict]:
    """Returns season aggregates keyed by team_id."""
    data = await _get(session, f"{API_WIDGET}/PlayerSeasonStats/{person_id}?format=json&lang=ru")
    if not data:
        return []

    rows: List[Dict] = []
    for s in data.get("SeasonStats") or []:
        comp = s.get("Season") or {}
        team_obj = s.get("TeamName") or {}
        rows.append({
            "season": comp.get("CompShortNameRu") or "",
            "comp_id": comp.get("CompID"),
            "team_id": s.get("TeamID"),
            "team": team_obj.get("CompTeamShortNameRu") or "",
            "games": _i(s.get("InGameCount")),
            "avg_points": _f(s.get("AvgPoints")),
            "avg_rebounds": _f(s.get("AvgRebound")),
            "avg_assists": _f(s.get("AvgAssist")),
        })
    return rows


# ─────────────────────────── Analytics ───────────────────────────────────────

def compute_team_analytics(team_name: str, team_id: int, games: List[Dict]) -> Dict:
    played = [g for g in games if g.get("result")]
    n = len(played)
    if not n:
        return {"team_name": team_name, "team_id": team_id, "games": games, "has_data": False}

    wins = sum(1 for g in played if g["result"] == "W")
    losses = sum(1 for g in played if g["result"] == "L")
    our_pts = sum(g.get("our_score", 0) or 0 for g in played)
    opp_pts = sum(g.get("opp_score", 0) or 0 for g in played)

    # By league
    by_league: Dict[str, Dict] = {}
    for g in played:
        lg = g["league"]
        if lg not in by_league:
            by_league[lg] = {"league_full": g["league_full"], "w": 0, "l": 0,
                             "our": 0, "opp": 0, "rounds": set()}
        by_league[lg]["rounds"].add(g["round"])
        if g["result"] == "W":
            by_league[lg]["w"] += 1
        else:
            by_league[lg]["l"] += 1
        by_league[lg]["our"] += g.get("our_score", 0) or 0
        by_league[lg]["opp"] += g.get("opp_score", 0) or 0
    for lg in by_league:
        by_league[lg]["rounds"] = sorted(by_league[lg]["rounds"])

    # Home/away splits
    home = [g for g in played if g["is_home"]]
    away = [g for g in played if not g["is_home"]]
    home_w = sum(1 for g in home if g["result"] == "W")
    away_w = sum(1 for g in away if g["result"] == "W")

    # Streak (current)
    streak_char = played[-1]["result"] if played else ""
    streak = 1
    for g in reversed(played[:-1]):
        if g["result"] == streak_char:
            streak += 1
        else:
            break

    # Best/worst game
    best = max(played, key=lambda g: (g.get("our_score", 0) or 0) - (g.get("opp_score", 0) or 0))
    worst = min(played, key=lambda g: (g.get("our_score", 0) or 0) - (g.get("opp_score", 0) or 0))

    return {
        "team_name": team_name,
        "team_id": team_id,
        "games": games,
        "played": played,
        "has_data": True,
        "n": n,
        "wins": wins,
        "losses": losses,
        "win_pct": round(wins / n * 100) if n else 0,
        "avg_scored": round(our_pts / n, 1),
        "avg_allowed": round(opp_pts / n, 1),
        "avg_diff": round((our_pts - opp_pts) / n, 1),
        "by_league": by_league,
        "home_w": home_w, "home_g": len(home),
        "away_w": away_w, "away_g": len(away),
        "streak_char": streak_char,
        "streak": streak,
        "best_game": best,
        "worst_game": worst,
    }


# ─────────────────────────── Telegram message ────────────────────────────────

def format_telegram(teams_data: List[Dict], person_ctx: List[Dict]) -> str:
    parts: List[str] = []

    for td in teams_data:
        if not td.get("has_data"):
            parts.append(f"⚠️ Нет данных для команды {td.get('team_name', td.get('team_id'))}")
            continue

        streak_emoji = "🔥" if td["streak_char"] == "W" else "❄️"

        block = [
            f"🏆 <b>{td['team_name']}</b>",
            f"   Текущий сезон: <b>{td['wins']}–{td['losses']}</b>"
            f"  ({td['win_pct']}% побед)",
            f"   Забивает: {td['avg_scored']} / пропускает: {td['avg_allowed']}"
            f"  (разница {td['avg_diff']:+.1f})",
            f"   Дома: {td['home_w']}/{td['home_g']}  "
            f"   В гостях: {td['away_w']}/{td['away_g']}",
            f"   {streak_emoji} Серия: {td['streak']} {'победы' if td['streak_char']=='W' else 'поражения'}",
            "",
        ]

        for lg, ld in td["by_league"].items():
            g_total = ld["w"] + ld["l"]
            block.append(
                f"  [{lg}] {ld['w']}–{ld['l']}"
                f"  ср. {round(ld['our']/g_total,1) if g_total else '—'}:"
                f"{round(ld['opp']/g_total,1) if g_total else '—'}"
            )

        best = td["best_game"]
        worst = td["worst_game"]
        block += [
            "",
            f"⭐ Лучшая победа: {best['our_score']}:{best['opp_score']}"
            f" vs {best['opp_name']} ({best['date']})",
            f"💔 Худшее поражение: {worst['our_score']}:{worst['opp_score']}"
            f" vs {worst['opp_name']} ({worst['date']})",
        ]

        parts.append("\n".join(block))

    result = "\n\n".join(parts)
    if person_ctx:
        result += "\n\n📄 Детальный отчёт — в прикреплённом HTML файле"
    return result


# ─────────────────────────── HTML report ─────────────────────────────────────

def generate_html(teams_data: List[Dict], person_ctx: List[Dict]) -> str:

    def kc(val: Any, lbl: str, sub: str = "") -> str:
        return (
            f'<div class="kc">'
            f'<div class="kv">{val}</div>'
            f'<div class="kl">{lbl}</div>'
            f'{"<div class=ks>" + sub + "</div>" if sub else ""}'
            f'</div>'
        )

    all_sections = ""

    for td in teams_data:
        tname = td.get("team_name", str(td.get("team_id", "?")))

        if not td.get("has_data"):
            all_sections += (
                f'<div class="sec"><h2>{tname}</h2>'
                '<p style="color:#64748b">Нет данных о сыгранных матчах.</p></div>'
            )
            continue

        played = td["played"]
        games = td["games"]

        # Score chart (per game)
        max_s = max(
            (max(g.get("our_score", 0) or 0, g.get("opp_score", 0) or 0) for g in played),
            default=1
        ) or 1
        bars = ""
        for g in played:
            d_short = g["date"][:5]
            our = g.get("our_score", 0) or 0
            opp = g.get("opp_score", 0) or 0
            h_our = max(4, int(our / max_s * 80))
            h_opp = max(4, int(opp / max_s * 80))
            w_col = "#22c55e" if our > opp else "#ef4444"
            opp_name_short = g.get("opp_name", "?")[:8]
            bars += (
                f'<div class="bc">'
                f'<div class="score-pair">'
                f'<div class="b" style="height:{h_our}px;background:{w_col}" title="{our}"></div>'
                f'<div class="b opp" style="height:{h_opp}px" title="{opp}"></div>'
                f'</div>'
                f'<div class="bl">{d_short}<br><small>{opp_name_short}</small></div>'
                f'<div class="bv">{our}:{opp}</div>'
                f'</div>'
            )

        # Game log table
        rows = ""
        for g in reversed(games):
            r = g.get("result", "")
            r_col = "#22c55e" if r == "W" else ("#ef4444" if r == "L" else "#94a3b8")
            ha = "🏠" if g.get("is_home") else "✈️"
            sc = f"{g['our_score']}:{g['opp_score']}" if g.get("our_score") is not None else "—"
            future = not g.get("result")
            row_style = "opacity:.6;" if future else ""
            diff = ""
            if g.get("result"):
                d = (g.get("our_score") or 0) - (g.get("opp_score") or 0)
                diff_col = "#22c55e" if d > 0 else "#ef4444"
                diff = f'<span style="color:{diff_col}">{d:+}</span>'

            rows += (
                f'<tr style="{row_style}">'
                f'<td>{g["date"]}</td>'
                f'<td>{ha}</td>'
                f'<td style="text-align:left">{g["opp_name"]}</td>'
                f'<td><b>{sc}</b></td>'
                f'<td style="color:{r_col};font-weight:700">{r}</td>'
                f'<td>{diff}</td>'
                f'<td style="font-size:.75em">{g["round"]}</td>'
                f'<td style="font-size:.75em">{g["league"]}</td>'
                f'</tr>'
            )

        # By-league table
        league_rows = ""
        for lg, ld in sorted(td["by_league"].items()):
            total = ld["w"] + ld["l"]
            pct = round(ld["w"] / total * 100) if total else 0
            avg_s = round(ld["our"] / total, 1) if total else "—"
            avg_a = round(ld["opp"] / total, 1) if total else "—"
            diff = round((ld["our"] - ld["opp"]) / total, 1) if total else 0
            diff_col = "#22c55e" if diff > 0 else "#ef4444"
            league_rows += (
                f'<tr>'
                f'<td style="text-align:left">{ld["league_full"] or lg}</td>'
                f'<td>[{lg}]</td>'
                f'<td>{total}</td>'
                f'<td style="color:#22c55e">{ld["w"]}</td>'
                f'<td style="color:#ef4444">{ld["l"]}</td>'
                f'<td>{pct}%</td>'
                f'<td>{avg_s}:{avg_a}</td>'
                f'<td style="color:{diff_col}">{diff:+.1f}</td>'
                f'</tr>'
            )

        streak_emoji = "🔥" if td["streak_char"] == "W" else "❄️"
        streak_label = "победных" if td["streak_char"] == "W" else "поражений"

        all_sections += f"""
<div class="sec">
  <h1>{tname}</h1>
  <div class="sub">Текущий сезон · ID {td['team_id']}</div>

  <h2 style="margin-top:24px">Общая статистика</h2>
  <div class="kg">
    {kc(f"{td['wins']}–{td['losses']}", 'Победы–Поражения', f"{td['win_pct']}% побед")}
    {kc(td['avg_scored'], 'Ср. набрано', 'очков/игру')}
    {kc(td['avg_allowed'], 'Ср. пропущено', 'очков/игру')}
    {kc(f"{td['avg_diff']:+.1f}", 'Разность', 'очков/игру')}
    {kc(f"{td['home_w']}/{td['home_g']}", 'Дома', 'победы/игр')}
    {kc(f"{td['away_w']}/{td['away_g']}", 'В гостях', 'победы/игр')}
    {kc(f"{streak_emoji} {td['streak']}", f'Серия {streak_label}', '')}
  </div>

  <h2>По лигам</h2>
  <div class="tw">
    <table>
      <thead><tr>
        <th>Лига</th><th>Код</th><th>Игр</th>
        <th>В</th><th>П</th><th>%</th>
        <th>Ср. счёт</th><th>Разн.</th>
      </tr></thead>
      <tbody>{league_rows}</tbody>
    </table>
  </div>

  <h2>Счёт по матчам</h2>
  <div class="score-legend">
    <span class="dot" style="background:#22c55e"></span> Наша команда &nbsp;&nbsp;
    <span class="dot" style="background:#475569"></span> Соперник
  </div>
  <div class="chart">{bars}</div>

  <h2>Все матчи ({len(games)} запланировано, {len(played)} сыграно)</h2>
  <div class="tw">
    <table>
      <thead><tr>
        <th>Дата</th><th></th><th>Соперник</th><th>Счёт</th>
        <th>В/П</th><th>+/−</th><th>Этап</th><th>Лига</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>
"""

    # Player context section (if person_id provided)
    player_ctx_html = ""
    if person_ctx:
        ctx_rows = ""
        for s in sorted(person_ctx, key=lambda x: (x.get("comp_id", 0), x.get("team", ""))):
            ctx_rows += (
                f'<tr>'
                f'<td>{s.get("season", "")}</td>'
                f'<td>{s.get("team", "")}</td>'
                f'<td>{s.get("games", 0)}</td>'
                f'<td>{s.get("avg_points", 0)}</td>'
                f'<td>{s.get("avg_rebounds", 0)}</td>'
                f'<td>{s.get("avg_assists", 0)}</td>'
                f'</tr>'
            )
        player_ctx_html = f"""
<div class="sec">
  <h2>Участие игрока в командах (по сезонам)</h2>
  <div class="tw">
    <table>
      <thead><tr><th>Сезон</th><th>Команда</th><th>Игр</th>
        <th>Оч/и</th><th>Под/и</th><th>Пер/и</th></tr></thead>
      <tbody>{ctx_rows}</tbody>
    </table>
  </div>
</div>"""

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Аналитика команды</title>
<style>
:root{{--bg:#0f1117;--card:#1a1d27;--acc:#f97316;--txt:#e2e8f0;--mut:#64748b;--brd:#2d3147;--grn:#22c55e;--red:#ef4444}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--txt);padding:20px;max-width:1200px;margin:0 auto}}
.sec{{margin-bottom:48px;padding-bottom:32px;border-bottom:1px solid var(--brd)}}
.sec:last-child{{border-bottom:none}}
h1{{font-size:1.8rem;color:var(--acc);margin-bottom:4px}}
.sub{{color:var(--mut);font-size:.85rem;margin-bottom:24px}}
h2{{font-size:.75rem;text-transform:uppercase;letter-spacing:.08em;color:var(--mut);margin:20px 0 12px}}
.kg{{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:10px;margin-bottom:20px}}
.kc{{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 10px;text-align:center}}
.kv{{font-size:1.5rem;font-weight:700;color:var(--acc)}}
.kl{{font-size:.72rem;color:var(--mut);margin-top:3px}}
.ks{{font-size:.65rem;color:var(--mut);margin-top:1px}}
.score-legend{{font-size:.75rem;color:var(--mut);margin-bottom:8px;display:flex;align-items:center;gap:4px}}
.dot{{display:inline-block;width:10px;height:10px;border-radius:50%}}
.chart{{display:flex;align-items:flex-end;gap:6px;background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 12px 8px;min-height:130px;overflow-x:auto}}
.bc{{display:flex;flex-direction:column;align-items:center;min-width:44px;flex-shrink:0}}
.score-pair{{display:flex;align-items:flex-end;gap:2px;height:90px}}
.b{{border-radius:3px 3px 0 0;width:18px;min-height:4px}}
.b.opp{{background:#475569!important}}
.bl{{font-size:.6rem;color:var(--mut);margin-top:4px;text-align:center;line-height:1.3}}
.bv{{font-size:.65rem;color:var(--txt);margin-top:2px}}
.tw{{overflow-x:auto;border-radius:12px;border:1px solid var(--brd);margin-bottom:20px}}
table{{width:100%;border-collapse:collapse;background:var(--card);font-size:.82rem}}
th{{background:#21253a;padding:9px 7px;text-align:center;color:var(--mut);font-size:.68rem;text-transform:uppercase;letter-spacing:.05em;white-space:nowrap}}
td{{padding:7px 6px;text-align:center;border-top:1px solid var(--brd);white-space:nowrap}}
tr:hover td{{background:#1e2236}}
.foot{{text-align:center;color:var(--mut);font-size:.7rem;margin-top:32px}}
</style>
</head>
<body>

{all_sections}
{player_ctx_html}

<div class="foot">
  Данные: reg.infobasket.su · Текущий сезон ·
  Сформировано {datetime.now().strftime('%d.%m.%Y %H:%M')} МСК
</div>
</body>
</html>"""


# ─────────────────────────── Main runner ─────────────────────────────────────

async def analyze_teams(team_ids: List[int], person_id: Optional[int]) -> Tuple[List[Dict], List[Dict]]:
    print(f"\n🏆  Аналитика команд: {team_ids}")
    if person_id:
        print(f"   + контекст игрока personId={person_id}")
    print("=" * 60)

    async with aiohttp.ClientSession() as session:
        team_tasks = [fetch_team_games(session, tid) for tid in team_ids]
        person_task = (
            fetch_player_season_context(session, person_id)
            if person_id else asyncio.coroutine(lambda: [])()
        )

        results = await asyncio.gather(*team_tasks, person_task)

        teams_raw = results[:-1]
        person_ctx = results[-1]

    teams_data: List[Dict] = []
    for (name, games), tid in zip(teams_raw, team_ids):
        print(f"   🏀  {name} (ID {tid}): {len(games)} игр")
        analytics = compute_team_analytics(name, tid, games)
        teams_data.append(analytics)

    return teams_data, list(person_ctx)


async def main(team_ids: List[int], person_id: Optional[int], out_file: str, send_tg: bool) -> None:
    teams_data, person_ctx = await analyze_teams(team_ids, person_id)

    html = generate_html(teams_data, person_ctx)
    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"\n✅  HTML сохранён: {out_file}")

    msg = format_telegram(teams_data, person_ctx)
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
                names = [td["team_name"] for td in teams_data]
                await bot.send_document(
                    chat_id=cid, document=doc, filename=out_file,
                    caption=f"📊 Аналитика: {', '.join(names)}"
                )
                print(f"✅  Отправлено в {cid}")
            except Exception as exc:
                print(f"❌  Ошибка отправки в {cid}: {exc}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Аналитика команды (Infobasket)")
    ap.add_argument("--team-ids", type=int, nargs="+", required=True,
                    help="TeamID команды (напр. 32855 36502 42347)")
    ap.add_argument("--person-id", type=int, default=None,
                    help="PersonID игрока для контекста (опционально)")
    ap.add_argument("--no-telegram", action="store_true",
                    help="Не отправлять в Telegram")
    ap.add_argument("--output",
                    help="Имя HTML файла (по умолчанию team_stats.html)")
    ap.add_argument("--chat-id",
                    help="Telegram chat_id (переопределяет CHAT_ID из .env)")
    args = ap.parse_args()

    if args.chat_id:
        os.environ["CHAT_ID"] = args.chat_id

    out = args.output or f"team_stats_{'_'.join(str(x) for x in args.team_ids)}.html"
    asyncio.run(main(args.team_ids, args.person_id, out, not args.no_telegram))
