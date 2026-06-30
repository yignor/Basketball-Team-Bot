#!/usr/bin/env python3
"""
Статистика игрока: поиск по всем играм сезона и персональный отчёт.

Запуск:
  python player_stats.py "Фамилия Имя"
  python player_stats.py "Иванов" --chat-id 123456789
  python player_stats.py "Иванов Иван" --no-telegram --output report.html
"""

import argparse
import asyncio
import io
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

REG_API = "https://reg.infobasket.su"


# ─────────────────────────────── Utilities ───────────────────────────────────

def _to_int(v: Any) -> Optional[int]:
    try:
        return int(str(v).strip())
    except Exception:
        return None


def _to_float(v: Any) -> float:
    try:
        return round(float(str(v).replace(",", ".")), 1)
    except Exception:
        return 0.0


def _pick_int(obj: Any, keys: List[str]) -> int:
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if v is not None:
                try:
                    return int(float(str(v)))
                except Exception:
                    pass
    return 0


def _pick_float(obj: Any, keys: List[str]) -> float:
    if isinstance(obj, dict):
        for k in keys:
            v = obj.get(k)
            if v is not None:
                try:
                    return round(float(str(v).replace(",", ".")), 1)
                except Exception:
                    pass
    return 0.0


def _pct(made: int, att: int) -> str:
    return f"{round(made / att * 100)}%" if att else "—"


# ──────────────────────────── Analyzer ───────────────────────────────────────

class PlayerStatsAnalyzer:
    def __init__(self, player_name: str) -> None:
        self.player_name = player_name.strip()
        self._tokens = [t.lower() for t in self.player_name.split() if t]

    # ── HTTP ──────────────────────────────────────────────────────

    @staticmethod
    async def _get(session: aiohttp.ClientSession, url: str) -> Optional[Any]:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    return await r.json(content_type=None)
        except Exception as exc:
            print(f"   ⚠️  {url[:80]}: {exc}")
        return None

    # ── Name matching ──────────────────────────────────────────────

    def _matches(self, last: str, first: str) -> bool:
        full = (last + " " + first).lower()
        return all(t in full for t in self._tokens)

    # ── Calendar / game list ───────────────────────────────────────

    async def _past_games(self, session: aiohttp.ClientSession) -> List[Dict]:
        from enhanced_duplicate_protection import duplicate_protection

        cfg = duplicate_protection.get_config_ids()
        comp_ids: List[int] = cfg.get("comp_ids", [])
        team_ids_set = {int(x) for x in cfg.get("team_ids", [])}

        if not comp_ids:
            print("⚠️  В конфиге нет comp_ids — добавьте их в Google Sheets")
            return []

        today = datetime.now().date()
        out: List[Dict] = []
        seen: set = set()

        for comp_id in comp_ids:
            url = f"{REG_API}/Comp/GetCalendar/?comps={comp_id}&format=json"
            data = await self._get(session, url)
            if not isinstance(data, list):
                continue

            for g in data:
                gid = _to_int(g.get("GameID"))
                if not gid or gid in seen:
                    continue

                date_str = g.get("GameDate", "")
                try:
                    if datetime.strptime(date_str, "%d.%m.%Y").date() >= today:
                        continue
                except ValueError:
                    continue

                t1 = _to_int(g.get("Team1ID") or g.get("TeamAid"))
                t2 = _to_int(g.get("Team2ID") or g.get("TeamBid"))

                if team_ids_set:
                    if t1 not in team_ids_set and t2 not in team_ids_set:
                        continue
                    our_id = t1 if t1 in team_ids_set else t2
                    opp_id = t2 if our_id == t1 else t1
                else:
                    our_id, opp_id = t1, t2

                seen.add(gid)
                out.append({
                    "game_id": gid,
                    "date": date_str,
                    "time": g.get("GameTimeMsk", ""),
                    "arena": g.get("ArenaRu", ""),
                    "comp": g.get("CompNameRu", ""),
                    "team_a": g.get("ShortTeamNameAru") or g.get("TeamNameAru", ""),
                    "team_b": g.get("ShortTeamNameBru") or g.get("TeamNameBru", ""),
                    "our_id": our_id,
                    "opp_id": opp_id,
                })

        out.sort(key=lambda g: g["date"])
        print(f"📋  Прошедших игр: {len(out)}")
        return out

    # ── Per-game stats ─────────────────────────────────────────────

    async def _player_stats_for_game(
        self, session: aiohttp.ClientSession, game: Dict
    ) -> Optional[Dict]:
        gid = game["game_id"]

        # Try Widget/GetOnline (has per-game box score after the game ends)
        data = await self._get(session, f"{REG_API}/Widget/GetOnline/{gid}?format=json&lang=ru")
        if data:
            rec = self._parse_online(data, game)
            if rec:
                return rec

        # Fallback: Comp/GetTeamStatsForPreview (season averages, still useful)
        data2 = await self._get(session, f"{REG_API}/Comp/GetTeamStatsForPreview/{gid}?compId=0")
        if isinstance(data2, list):
            rec = self._parse_preview(data2, game)
            if rec:
                return rec

        return None

    def _parse_online(self, data: Dict, game: Dict) -> Optional[Dict]:
        online = data.get("Online") or {}
        teams = data.get("GameTeams") or []

        score_a = _to_int(online.get("ScoreA") or online.get("Score1"))
        score_b = _to_int(online.get("ScoreB") or online.get("Score2"))

        for i, team_data in enumerate(teams):
            tid = _to_int(team_data.get("TeamID"))
            players = team_data.get("Players") or team_data.get("GamePlayers") or []
            for p in players:
                person = p.get("PersonInfo") or {}
                last = person.get("PersonLastNameRu") or person.get("PersonLastNameEn") or ""
                first = person.get("PersonFirstNameRu") or person.get("PersonFirstNameEn") or ""
                if not self._matches(last, first):
                    continue

                our_id = game.get("our_id")
                is_our = (tid == our_id) if our_id else True
                if is_our and score_a is not None and score_b is not None:
                    our_score = score_a if i == 0 else score_b
                    opp_score = score_b if i == 0 else score_a
                else:
                    our_score = opp_score = None

                s = p.get("GameStats") or p.get("Stats") or p
                return self._build_record(p, s, game, last, first, our_score, opp_score, is_avg=False)

        return None

    def _parse_preview(self, data: List, game: Dict) -> Optional[Dict]:
        for team_data in data:
            for p in (team_data.get("Players") or []):
                person = p.get("PersonInfo") or {}
                last = person.get("PersonLastNameRu") or person.get("PersonLastNameEn") or ""
                first = person.get("PersonFirstNameRu") or person.get("PersonFirstNameEn") or ""
                if not self._matches(last, first):
                    continue
                rec = self._build_record(p, p, game, last, first, None, None, is_avg=True)
                rec["points"] = _to_float(p.get("AvgPoints"))
                rec["rebounds"] = _to_float(p.get("AvgRebound"))
                rec["assists"] = _to_float(p.get("AvgAssist"))
                rec["steals"] = _to_float(p.get("AvgSteal"))
                rec["kpi"] = _to_float(p.get("AvgKPI"))
                return rec
        return None

    def _build_record(
        self, player: Dict, stats: Any, game: Dict,
        last: str, first: str, our_score: Any, opp_score: Any, is_avg: bool
    ) -> Dict:
        person = player.get("PersonInfo") or {}
        return {
            "game_id": game["game_id"],
            "date": game["date"],
            "time": game.get("time", ""),
            "arena": game.get("arena", ""),
            "comp": game.get("comp", ""),
            "team_a": game.get("team_a", ""),
            "team_b": game.get("team_b", ""),
            "our_score": our_score,
            "opp_score": opp_score,
            "player_name": f"{last} {first}".strip(),
            "number": str(
                player.get("DisplayNumber") or player.get("PlayerNumber") or "—"
            ),
            "is_avg": is_avg,
            "points": _pick_int(stats, ["Points", "Pts", "PTS", "TotalPoints"]),
            "rebounds": _pick_int(stats, ["TotalRebounds", "Rebounds", "Reb", "REB"]),
            "rebounds_off": _pick_int(stats, ["OffRebounds", "OffensiveRebounds"]),
            "rebounds_def": _pick_int(stats, ["DefRebounds", "DefensiveRebounds"]),
            "assists": _pick_int(stats, ["Assists", "Ast", "AST"]),
            "steals": _pick_int(stats, ["Steals", "Stl", "STL"]),
            "blocks": _pick_int(stats, ["Blocks", "Blk", "BLK"]),
            "turnovers": _pick_int(stats, ["Turnovers", "To", "TO", "TurnOvers"]),
            "fouls": _pick_int(stats, ["Fouls", "Pf", "PF", "PersonalFouls"]),
            "fg_made": _pick_int(stats, ["FgMade", "FieldGoalsMade", "FGM", "Fg2Made"]),
            "fg_att": _pick_int(stats, ["FgAttempted", "FieldGoalsAttempted", "FGA", "Fg2Attempted"]),
            "fg3_made": _pick_int(stats, ["Fg3Made", "ThreesMade", "ThreePointersMade"]),
            "fg3_att": _pick_int(stats, ["Fg3Attempted", "ThreesAttempted", "ThreePointersAttempted"]),
            "ft_made": _pick_int(stats, ["FtMade", "FreeThrowsMade", "FTM"]),
            "ft_att": _pick_int(stats, ["FtAttempted", "FreeThrowsAttempted", "FTA"]),
            "kpi": _pick_float(stats, ["KPI", "Kpi", "Efficiency", "EFF"]),
            "minutes": (
                stats.get("Minutes") or stats.get("Min") or
                stats.get("PlayedSeconds") or "—"
                if isinstance(stats, dict) else "—"
            ),
        }

    # ── Main run ───────────────────────────────────────────────────

    async def run(self) -> Dict:
        print(f"\n🏀  Статистика игрока: «{self.player_name}»")
        print("=" * 60)

        async with aiohttp.ClientSession() as session:
            past = await self._past_games(session)
            if not past:
                return {"found": False, "player_name": self.player_name, "games": []}

            records: List[Dict] = []
            for g in past:
                print(
                    f"   🔍  {g['date']}  GameID {g['game_id']}  "
                    f"{g['team_a']} vs {g['team_b']}"
                )
                rec = await self._player_stats_for_game(session, g)
                if rec:
                    records.append(rec)
                    tag = " (средние)" if rec["is_avg"] else ""
                    print(f"        ✅  {rec['points']} очков{tag}")
                else:
                    print("        ─  не участвовал")
                await asyncio.sleep(0.3)

        return self._aggregate(records)

    # ── Aggregation ────────────────────────────────────────────────

    def _aggregate(self, records: List[Dict]) -> Dict:
        if not records:
            return {"found": False, "player_name": self.player_name, "games": []}

        real = [r for r in records if not r["is_avg"]]
        use = real if real else records
        n = len(use)

        def tot(k): return sum(r.get(k, 0) or 0 for r in use)
        def avg(k): return round(tot(k) / n, 1)

        fg_m, fg_a = tot("fg_made"), tot("fg_att")
        fg3_m, fg3_a = tot("fg3_made"), tot("fg3_att")
        ft_m, ft_a = tot("ft_made"), tot("ft_att")

        best = max(use, key=lambda r: r.get("points", 0) or 0)

        return {
            "found": True,
            "player_name": records[0]["player_name"],
            "number": records[0]["number"],
            "games": records,
            "games_count": len(records),
            "has_per_game": bool(real),
            # per-game averages
            "avg_points": avg("points"),
            "avg_rebounds": avg("rebounds"),
            "avg_assists": avg("assists"),
            "avg_steals": avg("steals"),
            "avg_blocks": avg("blocks"),
            "avg_kpi": avg("kpi"),
            # totals
            "total_points": tot("points"),
            "total_assists": tot("assists"),
            "total_rebounds": tot("rebounds"),
            "total_steals": tot("steals"),
            # shooting
            "fg_made": fg_m, "fg_att": fg_a, "fg_pct": _pct(fg_m, fg_a),
            "fg3_made": fg3_m, "fg3_att": fg3_a, "fg3_pct": _pct(fg3_m, fg3_a),
            "ft_made": ft_m, "ft_att": ft_a, "ft_pct": _pct(ft_m, ft_a),
            # best
            "best_pts": best.get("points", 0),
            "best_date": best.get("date", ""),
            "best_match": f"{best.get('team_a','')} vs {best.get('team_b','')}",
        }

    # ── Telegram message ───────────────────────────────────────────

    def format_telegram(self, s: Dict) -> str:
        if not s["found"]:
            return (
                f"❌ Игрок <b>{self.player_name}</b> не найден ни в одной игре сезона.\n"
                "Проверьте написание (как в Infobasket, например «Иванов Иван»)."
            )

        n = s["games_count"]
        avg_note = " (сезонные средние)" if not s["has_per_game"] else ""

        lines = [
            f"🏀 <b>Статистика: {s['player_name']}</b>  #{s['number']}",
            f"📋 Найдено игр: <b>{n}</b>\n",
            f"📊 <b>Среднее за игру{avg_note}:</b>",
            f"  🎯 Очки: <b>{s['avg_points']}</b>",
            f"  🏀 Подборы: <b>{s['avg_rebounds']}</b>",
            f"  🤝 Передачи: <b>{s['avg_assists']}</b>",
            f"  🥷 Перехваты: <b>{s['avg_steals']}</b>",
            f"  📈 КПИ: <b>{s['avg_kpi']}</b>",
        ]

        if s["has_per_game"]:
            lines += [
                "",
                "🎯 <b>Броски за сезон:</b>",
                f"  2x: {s['fg_pct']}  ({s['fg_made']}/{s['fg_att']})",
                f"  3x: {s['fg3_pct']}  ({s['fg3_made']}/{s['fg3_att']})",
                f"  Штрафные: {s['ft_pct']}  ({s['ft_made']}/{s['ft_att']})",
            ]

        if s["best_pts"]:
            lines += [
                "",
                f"⭐ <b>Лучшая игра:</b> {s['best_pts']} очков",
                f"   {s['best_date']}  {s['best_match']}",
            ]

        lines.append("\n📄 Детальный отчёт — в прикреплённом HTML файле")
        return "\n".join(lines)

    # ── HTML report ────────────────────────────────────────────────

    def generate_html(self, s: Dict) -> str:
        if not s["found"]:
            return (
                "<!DOCTYPE html><html><head><meta charset='utf-8'></head><body>"
                f"<h1>Игрок «{self.player_name}» не найден</h1></body></html>"
            )

        games = s["games"]

        # Table rows
        rows = ""
        for g in reversed(games):
            sc = ""
            if g.get("our_score") is not None and g.get("opp_score") is not None:
                w = g["our_score"] > g["opp_score"]
                col = "#22c55e" if w else "#ef4444"
                sc = f'<span style="color:{col};font-weight:700">{g["our_score"]}:{g["opp_score"]}</span>'

            def sh(m, a):
                return f"{m}/{a}&nbsp;<small>({_pct(m,a)})</small>" if a else "—"

            pts = g.get("points", 0) or 0
            pw = "font-weight:700;" if pts >= 15 else ""
            rows += (
                f"<tr>"
                f"<td>{g.get('date','')}</td>"
                f"<td>{g.get('team_a','')} vs {g.get('team_b','')}</td>"
                f"<td>{sc}</td>"
                f"<td style='{pw}'>{pts}</td>"
                f"<td>{g.get('rebounds',0)}</td>"
                f"<td>{g.get('assists',0)}</td>"
                f"<td>{g.get('steals',0)}</td>"
                f"<td>{g.get('blocks',0)}</td>"
                f"<td>{g.get('turnovers',0)}</td>"
                f"<td>{g.get('fouls',0)}</td>"
                f"<td style='font-size:.8em'>{sh(g.get('fg_made',0),g.get('fg_att',0))}</td>"
                f"<td style='font-size:.8em'>{sh(g.get('fg3_made',0),g.get('fg3_att',0))}</td>"
                f"<td style='font-size:.8em'>{sh(g.get('ft_made',0),g.get('ft_att',0))}</td>"
                f"<td>{g.get('kpi',0)}</td>"
                f"</tr>"
            )

        # Bar chart
        pts_seq = [g.get("points", 0) or 0 for g in games]
        mxp = max(pts_seq, default=1) or 1
        bars = ""
        for g, pts in zip(games, pts_seq):
            h = max(4, int(pts / mxp * 90))
            d = g.get("date", "")[:5]
            bars += (
                f'<div class="bc">'
                f'<div class="b" style="height:{h}px" title="{pts} — {d}"></div>'
                f'<div class="bl">{d}</div><div class="bv">{pts}</div>'
                f'</div>'
            )

        avg_warn = (
            '<p class="warn">⚠️ Статистика рассчитана по сезонным средним '
            "(по-игровые данные Infobasket API не вернул).</p>"
            if not s["has_per_game"] else ""
        )

        n = s["games_count"]
        plural = "игра" if n == 1 else ("игры" if 2 <= n <= 4 else "игр")

        def kpi_card(val, lbl):
            return (
                f'<div class="kc"><div class="kv">{val}</div>'
                f'<div class="kl">{lbl}</div></div>'
            )

        def shoot_card(pct, lbl, m, a):
            fill = int(m / (a or 1) * 100)
            return (
                f'<div class="sc"><div class="sp">{pct}</div>'
                f'<div class="sl">{lbl}</div>'
                f'<div class="sd">{m}/{a}</div>'
                f'<div class="bar-track"><div class="bar-fill" style="width:{fill}%"></div></div>'
                f'</div>'
            )

        return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Статистика — {s['player_name']}</title>
<style>
:root{{--bg:#0f1117;--card:#1a1d27;--acc:#f97316;--txt:#e2e8f0;--mut:#64748b;--brd:#2d3147}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:var(--bg);color:var(--txt);padding:20px;max-width:1100px;margin:0 auto}}
.hero{{margin-bottom:28px;display:flex;align-items:center;gap:16px}}
.num{{background:var(--acc);color:#000;font-weight:800;border-radius:50%;width:56px;height:56px;line-height:56px;text-align:center;font-size:1.4rem;flex-shrink:0}}
h1{{font-size:1.8rem;color:var(--acc)}}
.sub{{color:var(--mut);font-size:.88rem;margin-top:4px}}
.warn{{color:#f97316;background:rgba(249,115,22,.1);border:1px solid rgba(249,115,22,.3);border-radius:8px;padding:10px 14px;margin-bottom:20px;font-size:.85rem}}
.sec{{margin-bottom:28px}}
.stt{{font-size:.7rem;text-transform:uppercase;letter-spacing:.1em;color:var(--mut);margin-bottom:10px}}
.kg{{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:10px}}
.kc{{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 10px;text-align:center}}
.kv{{font-size:1.8rem;font-weight:700;color:var(--acc)}}
.kl{{font-size:.7rem;color:var(--mut);margin-top:3px}}
.sg{{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}}
.sc{{background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px;text-align:center}}
.sp{{font-size:1.7rem;font-weight:700}}
.sl{{font-size:.78rem;color:var(--mut);margin-top:3px}}
.sd{{font-size:.7rem;color:var(--mut);margin-top:2px}}
.bar-track{{background:var(--brd);border-radius:4px;height:5px;margin-top:8px;overflow:hidden}}
.bar-fill{{background:var(--acc);height:100%;border-radius:4px}}
.chart{{display:flex;align-items:flex-end;gap:3px;background:var(--card);border:1px solid var(--brd);border-radius:12px;padding:14px 10px 8px;min-height:130px;overflow-x:auto}}
.bc{{display:flex;flex-direction:column;align-items:center;min-width:28px;flex-shrink:0}}
.b{{background:var(--acc);border-radius:3px 3px 0 0;width:20px;min-height:4px}}
.b:hover{{opacity:.75;cursor:default}}
.bl{{font-size:.55rem;color:var(--mut);margin-top:3px;writing-mode:vertical-rl;transform:rotate(180deg);white-space:nowrap}}
.bv{{font-size:.62rem;color:var(--txt);margin-top:2px}}
.tw{{overflow-x:auto;border-radius:12px;border:1px solid var(--brd)}}
table{{width:100%;border-collapse:collapse;background:var(--card);font-size:.82rem}}
th{{background:#21253a;padding:9px 7px;text-align:center;color:var(--mut);font-size:.68rem;text-transform:uppercase;letter-spacing:.05em;white-space:nowrap}}
td{{padding:7px 6px;text-align:center;border-top:1px solid var(--brd);white-space:nowrap}}
tr:hover td{{background:#1e2236}}
td:nth-child(1),td:nth-child(2){{text-align:left}}
.foot{{text-align:center;color:var(--mut);font-size:.7rem;margin-top:28px}}
</style>
</head>
<body>

<div class="hero">
  <div class="num">#{s['number']}</div>
  <div>
    <h1>{s['player_name']}</h1>
    <div class="sub">{n} {plural} найдено · Сезон 2024/25</div>
  </div>
</div>

{avg_warn}

<div class="sec">
  <div class="stt">Среднее за игру</div>
  <div class="kg">
    {kpi_card(s['avg_points'], 'Очки')}
    {kpi_card(s['avg_rebounds'], 'Подборы')}
    {kpi_card(s['avg_assists'], 'Передачи')}
    {kpi_card(s['avg_steals'], 'Перехваты')}
    {kpi_card(s['avg_blocks'], 'Блоки')}
    {kpi_card(s['avg_kpi'], 'КПИ')}
    {kpi_card(s['best_pts'], 'Рекорд')}
  </div>
</div>

<div class="sec">
  <div class="stt">Реализация бросков за сезон</div>
  <div class="sg">
    {shoot_card(s['fg_pct'], '2-очковые', s['fg_made'], s['fg_att'])}
    {shoot_card(s['fg3_pct'], '3-очковые', s['fg3_made'], s['fg3_att'])}
    {shoot_card(s['ft_pct'], 'Штрафные', s['ft_made'], s['ft_att'])}
  </div>
</div>

<div class="sec">
  <div class="stt">Очки по играм</div>
  <div class="chart">{bars}</div>
</div>

<div class="sec">
  <div class="stt">Статистика по играм</div>
  <div class="tw">
    <table>
      <thead><tr>
        <th>Дата</th><th>Матч</th><th>Счёт</th>
        <th>Оч</th><th>Под</th><th>Пер</th>
        <th>Пхв</th><th>Бл</th><th>Пот</th><th>Фол</th>
        <th>2x</th><th>3x</th><th>Шт</th><th>КПИ</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</div>

<div class="foot">Сформировано {datetime.now().strftime('%d.%m.%Y %H:%M')} МСК · Basketball Team Bot</div>
</body>
</html>"""


# ──────────────────────────── Entry point ────────────────────────────────────

async def main(player_name: str, out_file: str, send_tg: bool) -> Dict:
    analyzer = PlayerStatsAnalyzer(player_name)
    stats = await analyzer.run()

    html = analyzer.generate_html(stats)
    with open(out_file, "w", encoding="utf-8") as fh:
        fh.write(html)
    print(f"\n✅  HTML сохранён: {out_file}")

    msg = analyzer.format_telegram(stats)
    print("\n" + "=" * 60)
    print(msg)
    print("=" * 60)

    if send_tg and BOT_TOKEN and CHAT_ID:
        from telegram import Bot
        bot = Bot(token=BOT_TOKEN)
        chat_ids = [c.strip() for c in CHAT_ID.replace(",", " ").split() if c.strip()]
        for cid_raw in chat_ids:
            cid: Any = int(cid_raw) if cid_raw.lstrip("-").isdigit() else cid_raw
            try:
                await bot.send_message(chat_id=cid, text=msg, parse_mode="HTML")
                doc = io.BytesIO(html.encode("utf-8"))
                doc.name = out_file
                await bot.send_document(
                    chat_id=cid,
                    document=doc,
                    filename=out_file,
                    caption=f"📊 Статистика: {stats.get('player_name', player_name)}",
                )
                print(f"✅  Отправлено в {cid}")
            except Exception as exc:
                print(f"❌  Ошибка отправки в {cid}: {exc}")

    return stats


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Статистика игрока Basketball Team Bot")
    ap.add_argument("player_name", help='Имя как в Infobasket, напр. "Иванов Иван"')
    ap.add_argument("--no-telegram", action="store_true", help="Не отправлять в Telegram")
    ap.add_argument("--output", help="Имя HTML файла (по умолчанию stats_<имя>.html)")
    ap.add_argument("--chat-id", help="Telegram chat_id (переопределяет CHAT_ID из .env)")
    args = ap.parse_args()

    if args.chat_id:
        os.environ["CHAT_ID"] = args.chat_id

    safe = args.player_name.replace(" ", "_").replace("/", "-")
    out_path = args.output or f"stats_{safe}.html"

    asyncio.run(main(args.player_name, out_path, not args.no_telegram))
