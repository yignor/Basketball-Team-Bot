#!/usr/bin/env python3
"""
Подробный отчёт по команде одним HTML-файлом.

Зачем файл, а не сообщение: в чат помещается «что случилось в последней игре»
на пять строк, а тренеру нужны сезон, тренды и сравнения — это таблицы и
графики, которые в тексте нечитаемы. Поэтому в чат уходит короткая сводка, а
разбор приезжает вложением.

Файл СамоДостаточный: ни одного внешнего запроса, графики нарисованы в SVG
руками. Telegram открывает вложение во встроенном браузере, а он бывает без
сети (или с заблокированным CDN) — отчёт обязан открыться в любом случае.
Тёмная тема учтена: половина команды читает с телефона вечером.

Вкладки: Обзор · Сезон · Последняя игра · Прошлый сезон · Соперник · Промт.
Последняя — готовый текст для ИИ: те же цифры в компактном виде и просьба
разобрать их как аналитик. Пользователь копирует одной кнопкой.
"""

import html
import json
from typing import Any, Dict, List, Optional, Tuple

METRIC_TITLES = (
    ("pts", "Очки"), ("reb", "Подборы"), ("reb_off", "Подборы в атаке"),
    ("reb_def", "Подборы в защите"), ("ast", "Передачи"), ("stl", "Перехваты"),
    ("blk", "Блок-шоты"), ("tur", "Потери"), ("pf", "Фолы"), ("foul_on", "Фолы на нас"),
)


def _e(text: Any) -> str:
    return html.escape(str(text if text is not None else ""))


def _dmy(iso: str) -> str:
    return f"{iso[8:10]}.{iso[5:7]}" if len(iso) >= 10 else iso


def _pct(made: float, att: float) -> str:
    return f"{made / att * 100:.0f}%" if att else "—"


def _line_chart(points: List[Tuple[str, float]], title: str,
                zero_line: bool = True) -> str:
    """График «по играм» в SVG. Своими руками: библиотека потянула бы CDN,
    а файл должен открываться без сети."""
    if len(points) < 2:
        return f'<div class="empty">{_e(title)}: мало игр для графика</div>'
    w, h, pad = 640, 200, 28
    vals = [v for _l, v in points]
    lo, hi = min(vals), max(vals)
    if lo == hi:
        lo, hi = lo - 1, hi + 1
    span = hi - lo
    step = (w - pad * 2) / (len(points) - 1)

    def xy(i: int, v: float) -> Tuple[float, float]:
        return pad + i * step, h - pad - (v - lo) / span * (h - pad * 2)

    path = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}"
                    for i, (x, y) in enumerate(xy(i, v) for i, v in enumerate(vals)))
    dots = "".join(
        f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" class="{"pos" if v >= 0 else "neg"}">'
        f'<title>{_e(points[i][0])}: {v:+g}</title></circle>'
        for i, (v, (x, y)) in enumerate((v, xy(i, v)) for i, v in enumerate(vals)))
    zero = ""
    if zero_line and lo < 0 < hi:
        _zx, zy = xy(0, 0)
        zero = f'<line x1="{pad}" y1="{zy:.1f}" x2="{w - pad}" y2="{zy:.1f}" class="zero"/>'
    labels = "".join(
        f'<text x="{xy(i, vals[i])[0]:.1f}" y="{h - 6}" class="xl">{_e(l)}</text>'
        for i, (l, _v) in enumerate(points) if len(points) <= 12 or i % 2 == 0)
    return (f'<div class="chart"><div class="ct">{_e(title)}</div>'
            f'<svg viewBox="0 0 {w} {h}" preserveAspectRatio="xMidYMid meet">'
            f'{zero}<path d="{path}" class="ln"/>{dots}{labels}</svg></div>')


def _bars(rows: List[Tuple[str, float, float]], title: str,
          left: str, right: str) -> str:
    """Парные полосы «наше против сравнения» — так видно разрыв, а не числа."""
    if not rows:
        return ""
    top = max(max(abs(a), abs(b)) for _t, a, b in rows) or 1
    out = [f'<div class="chart"><div class="ct">{_e(title)}</div>',
           f'<div class="legend"><span class="k1"></span>{_e(left)}'
           f'<span class="k2"></span>{_e(right)}</div>']
    for label, a, b in rows:
        out.append(
            f'<div class="bar"><div class="bl">{_e(label)}</div>'
            f'<div class="bw"><div class="b1" style="width:{abs(a)/top*100:.1f}%"></div>'
            f'<div class="b2" style="width:{abs(b)/top*100:.1f}%"></div></div>'
            f'<div class="bv">{a:g} / {b:g}</div></div>')
    out.append("</div>")
    return "".join(out)


def _kpi(items: List[Tuple[str, str, str]]) -> str:
    """Плитки с главными числами: их читают первыми и часто единственными."""
    cells = "".join(
        f'<div class="kpi"><div class="kv">{_e(v)}</div>'
        f'<div class="kt">{_e(t)}</div><div class="kn">{_e(n)}</div></div>'
        for t, v, n in items)
    return f'<div class="kpis">{cells}</div>'


def _games_table(series: List[Dict[str, Any]], limit: int = 20) -> str:
    rows = []
    for g in reversed(series[-limit:]):
        rows.append(
            f'<tr class="{"w" if g["win"] else "l"}"><td>{_dmy(g["game_date"])}</td>'
            f'<td>{_e(g["opp_name"] or "—")}</td>'
            f'<td>{"дома" if g["home"] else "в гостях"}</td>'
            f'<td class="num">{g["our_score"]}:{g["their_score"]}</td>'
            f'<td class="num">{g["diff"]:+d}</td>'
            f'<td class="num">{g["reb"]}</td><td class="num">{g["ast"]}</td>'
            f'<td class="num">{g["tur"]}</td>'
            f'<td class="num">{_pct(g["fgm"], g["fga"])}</td></tr>')
    return (
        '<table><thead><tr><th>Дата</th><th>Соперник</th><th>Где</th><th>Счёт</th>'
        '<th>±</th><th>Подб</th><th>Пас</th><th>Пот</th><th>Броски</th></tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table>')


def _ai_prompt(team: str, cur: Dict[str, Any], prev: Optional[Dict[str, Any]],
               series: List[Dict[str, Any]], insights: List[str],
               data: Optional[Dict[str, Any]] = None) -> str:
    """Готовый текст для ИИ: цифры + роль + чего мы от него хотим.

    Кладём компактную таблицу, а не пересказ: модели проще считать самой, чем
    доверять нашим выводам, а выводы даём отдельно — пусть спорит."""
    data = data or {}
    lines = [
        "Ты профессиональный баскетбольный аналитик. Ниже — статистика "
        f"любительской команды «{team}» по играм. Разбери её как для тренера:",
        "1) три главные проблемы и чем они подтверждаются в цифрах;",
        "2) что команда делает хорошо и на чём это можно строить;",
        "3) как менялась игра по ходу сезона (тренд, а не разовые всплески);",
        "4) 3–5 конкретных упражнений или установок на тренировку;",
        "5) чего в этих данных не хватает, чтобы вывод был крепче.",
        "Не пересказывай таблицу, считай сам и делай выводы. Пиши по-русски, "
        "коротко и по делу.",
        "",
        f"КОМАНДА: {team}",
        f"ИГР: {cur.get('games', 0)}, баланс {cur.get('wins', 0)}–{cur.get('losses', 0)}, "
        f"забиваем {cur.get('for', 0)}, пропускаем {cur.get('against', 0)} за игру",
    ]
    if prev:
        lines.append(f"ПРОШЛЫЙ СЕЗОН: игр {prev.get('games')}, баланс "
                     f"{prev.get('wins')}–{prev.get('losses')}, разница {prev.get('diff'):+.1f}")
    lines.append("")
    lines.append("ПО ИГРАМ (дата; соперник; дома/в гостях; счёт; подборы; "
                 "атак.подборы; передачи; перехваты; потери; фолы; броски; трёхи; штрафные):")
    for g in series:
        lines.append(
            f"{g['game_date']}; {g['opp_name'] or 'н/д'}; "
            f"{'дома' if g['home'] else 'гости'}; {g['our_score']}:{g['their_score']}; "
            f"{g['reb']}; {g['reb_off']}; {g['ast']}; {g['stl']}; {g['tur']}; {g['pf']}; "
            f"{g['fgm']}/{g['fga']}; {g['tpm']}/{g['tpa']}; {g['ftm']}/{g['fta']}")
    if data.get("opp_avg"):
        o = data["opp_avg"]
        lines += ["", "СОПЕРНИКИ В ЭТИХ ЖЕ ИГРАХ (в среднем за игру): "
                      f"очки {o.get('pts')}, подборы {o.get('reb')}, "
                      f"передачи {o.get('ast')}, потери {o.get('tur')}, "
                      f"броски {o.get('fgm')}/{o.get('fga')}"]
    roster = data.get("roster") or []
    if roster:
        lines += ["", "СОСТАВ (игрок; игр; минут; очки; подборы; передачи; перехваты; "
                      "потери; броски; +/-; фэнтези-очки — всё в среднем за игру):"]
        for p_ in roster:
            lines.append(
                f"{p_['name']}; {p_['games']}; {p_['mins']}; {p_['pts']}; {p_['reb']}; "
                f"{p_['ast']}; {p_['stl']}; {p_['tur']}; {p_['fg']}; "
                f"{p_['plus_minus']:+g}; {p_['fp']}")
    st = data.get("standings") or []
    if st:
        ordered = sorted(st, key=lambda r: (-(r.get("wins") or 0), r.get("losses") or 0))
        lines += ["", "ТУРНИРНАЯ ТАБЛИЦА (место; команда; победы; поражения):"]
        for i, r in enumerate(ordered, 1):
            mark = " <- мы" if str(r.get("team_id")) == str(data.get("our_team_id")) else ""
            lines.append(f"{i}; {r.get('name')}; {r.get('wins')}; {r.get('losses')}{mark}")
    if insights:
        lines += ["", "ЧТО ЗАМЕТИЛ БОТ (проверь и поспорь, если не согласен):"]
        lines += [f"- {i}" for i in insights]
    return "\n".join(lines)


def _roster_table(roster: List[Dict[str, Any]]) -> str:
    """Состав: кто сколько играл и что принёс. Средние за игру, а не суммы —
    иначе сыгравший 11 матчей всегда выше сыгравшего три."""
    if not roster:
        return '<div class="empty">Нет данных по составу.</div>'
    rows = "".join(
        f'<tr><td>{_e(p["name"])}</td><td class="num">{p["games"]}</td>'
        f'<td class="num">{p["mins"] or "—"}</td><td class="num">{p["pts"]}</td>'
        f'<td class="num">{p["reb"]}</td><td class="num">{p["ast"]}</td>'
        f'<td class="num">{p["stl"]}</td><td class="num">{p["tur"]}</td>'
        f'<td class="num">{p["fg"]}</td>'
        f'<td class="num">{p["plus_minus"]:+g}</td>'
        f'<td class="num"><b>{p["fp"]}</b></td></tr>' for p in roster)
    return ('<table><thead><tr><th>Игрок</th><th>И</th><th>Мин</th><th>Очк</th>'
            '<th>Подб</th><th>Пас</th><th>Пх</th><th>Пот</th><th>Броски</th>'
            '<th>±</th><th>ФО</th></tr></thead>'
            f'<tbody>{rows}</tbody></table>')


def _standings_table(rows: List[Dict[str, Any]], our_id: str) -> str:
    """Турнирная таблица лиги — как она есть у самой лиги, с нашей строкой."""
    if not rows:
        return '<div class="empty">Турнирная таблица недоступна для этой лиги.</div>'
    ordered = sorted(rows, key=lambda r: (-(r.get("wins") or 0), r.get("losses") or 0))
    body = "".join(
        f'<tr class="{"me" if str(r.get("team_id")) == str(our_id) else ""}">'
        f'<td class="num">{i}</td><td>{_e(r.get("name") or r.get("team_id"))}</td>'
        f'<td class="num">{r.get("wins", 0)}</td><td class="num">{r.get("losses", 0)}</td>'
        f'<td class="num">{r.get("points", 0)}</td></tr>'
        for i, r in enumerate(ordered, 1))
    return ('<table><thead><tr><th>#</th><th>Команда</th><th>В</th><th>П</th>'
            f'<th>Очки</th></tr></thead><tbody>{body}</tbody></table>')


def build(data: Dict[str, Any]) -> str:
    """HTML-отчёт целиком. data — из team_progress.detailed_report()."""
    team = data.get("team_title") or "Команда"
    cur, prev = data.get("season") or {}, data.get("prev_season")
    series = data.get("series") or []
    last = data.get("last_game") or {}
    ins = data.get("insights") or []
    h2h = data.get("head_to_head") or []
    opp_name = (data.get("opponent") or {}).get("name") or ""

    kpi = _kpi([
        ("Игр в сезоне", str(cur.get("games", 0)),
         f"{cur.get('wins', 0)}–{cur.get('losses', 0)}"),
        ("Разница за игру", f"{cur.get('diff', 0):+.1f}",
         f"{cur.get('for', 0)} : {cur.get('against', 0)}"),
        ("Броски с игры", _pct(cur.get("avg", {}).get("fgm", 0),
                               cur.get("avg", {}).get("fga", 0)), "за игру"),
        ("Подборы", str(cur.get("avg", {}).get("reb", 0)),
         f"в атаке {cur.get('avg', {}).get('reb_off', 0)}"),
        ("Передачи", str(cur.get("avg", {}).get("ast", 0)), "за игру"),
        ("Потери", str(cur.get("avg", {}).get("tur", 0)), "за игру"),
    ])

    diff_chart = _line_chart([(_dmy(g["game_date"]), g["diff"]) for g in series],
                             "Разница очков по играм")
    pts_chart = _line_chart([(_dmy(g["game_date"]), g["our_score"] or 0) for g in series],
                            "Сколько забиваем", zero_line=False)

    last_rows = []
    if last and cur.get("avg"):
        for key, title in METRIC_TITLES:
            last_rows.append((title, last.get(key, 0), cur["avg"].get(key, 0)))
    last_bars = _bars(last_rows, "Последняя игра против среднего по сезону",
                      "последняя игра", "среднее")

    prev_rows = []
    if prev and cur.get("avg"):
        for key, title in METRIC_TITLES:
            prev_rows.append((title, cur["avg"].get(key, 0), prev["avg"].get(key, 0)))
    prev_bars = _bars(prev_rows, "Этот сезон против прошлого", "сейчас", "прошлый сезон")

    # Прошлого сезона может не быть вовсе (команда играет первый) — тогда
    # показываем динамику ВНУТРИ сезона: первая половина против второй.
    split = data.get("split")
    split_html = ""
    if not prev and split:
        f, sc = split["first"], split["second"]
        split_rows = [(t, sc["avg"].get(k, 0), f["avg"].get(k, 0)) for k, t in METRIC_TITLES]
        split_html = (
            f'<div class="sub">Прошлого сезона в базе нет, поэтому сравниваем '
            f'половины текущего: {_dmy(split["first_from"])}–{_dmy(split["first_to"])} '
            f'против {_dmy(split["second_from"])}–{_dmy(split["second_to"])}.</div>'
            + _kpi([
                ("Первая половина", f"{f['wins']}–{f['losses']}", f"разница {f['diff']:+.1f}"),
                ("Вторая половина", f"{sc['wins']}–{sc['losses']}", f"разница {sc['diff']:+.1f}"),
                ("Сдвиг", f"{sc['diff'] - f['diff']:+.1f}", "очков за игру"),
            ])
            + _bars(split_rows, "Вторая половина против первой", "вторая", "первая"))

    # «Мы против соперников» — по протоколам тех же игр, а не по чужим турнирам.
    opp_avg = data.get("opp_avg") or {}
    league_bars = ""
    if opp_avg and cur.get("avg"):
        league_rows = [(t, cur["avg"].get(k, 0), opp_avg.get(k, 0))
                       for k, t in METRIC_TITLES if k in opp_avg]
        league_bars = _bars(league_rows, "Мы против соперников (в наших же играх)",
                            "мы", "соперники")

    h2h_html = (f'<div class="empty">С «{_e(opp_name or "этим соперником")}» '
                f'играли впервые — сравнивать не с чем.</div>'
                if not h2h else _games_table(h2h, limit=20))

    prompt = _ai_prompt(team, cur, prev, series, ins, data)
    ins_html = "".join(f"<li>{_e(i)}</li>" for i in ins) or "<li>Пока без выводов.</li>"

    prev_title = ""
    if prev:
        prev_title = (f"Прошлый сезон: игр {prev.get('games')}, "
                      f"баланс {prev.get('wins')}–{prev.get('losses')}, "
                      f"разница {prev.get('diff'):+.1f}")

    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{_e(team)} — разбор</title>
<style>
:root {{ --bg:#fff; --fg:#111; --muted:#6b7280; --line:#e5e7eb; --card:#f6f7f9;
         --pos:#16a34a; --neg:#dc2626; --acc:#2563eb; }}
@media (prefers-color-scheme: dark) {{
  :root {{ --bg:#0f1216; --fg:#e8eaed; --muted:#9aa0a6; --line:#262b31; --card:#171b20;
           --pos:#4ade80; --neg:#f87171; --acc:#60a5fa; }} }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--fg); font:15px/1.5 -apple-system,
        BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; padding:16px; }}
h1 {{ font-size:20px; margin:0 0 2px; }}
.sub {{ color:var(--muted); font-size:13px; margin-bottom:14px; }}
.tabs {{ display:flex; gap:6px; overflow-x:auto; padding-bottom:10px; }}
.tab {{ padding:8px 12px; border:none; border-radius:10px; background:var(--card);
        color:var(--fg); font:600 14px inherit; cursor:pointer; white-space:nowrap; }}
.tab.on {{ background:var(--acc); color:#fff; }}
.pane {{ display:none; }} .pane.on {{ display:block; }}
.kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:10px;
         margin:12px 0 18px; }}
.kpi {{ background:var(--card); border-radius:12px; padding:12px; }}
.kv {{ font-size:24px; font-weight:700; }}
.kt {{ color:var(--muted); font-size:12px; }} .kn {{ color:var(--muted); font-size:12px; }}
.chart {{ background:var(--card); border-radius:12px; padding:12px; margin:0 0 16px; }}
.ct {{ font-weight:600; margin-bottom:8px; }}
svg {{ width:100%; height:auto; }}
.ln {{ fill:none; stroke:var(--acc); stroke-width:2; }}
.zero {{ stroke:var(--line); stroke-width:1; stroke-dasharray:4 4; }}
circle.pos {{ fill:var(--pos); }} circle.neg {{ fill:var(--neg); }}
.xl {{ fill:var(--muted); font-size:10px; text-anchor:middle; }}
.legend {{ color:var(--muted); font-size:12px; margin-bottom:8px; }}
.legend span {{ display:inline-block; width:10px; height:10px; border-radius:2px;
                margin:0 6px 0 12px; }}
.legend span:first-child {{ margin-left:0; }}
.k1 {{ background:var(--acc); }} .k2 {{ background:var(--muted); }}
.bar {{ display:flex; align-items:center; gap:8px; margin:6px 0; font-size:13px; }}
.bl {{ width:36%; color:var(--muted); }}
.bw {{ flex:1; }} .bv {{ width:22%; text-align:right; font-variant-numeric:tabular-nums; }}
.b1 {{ height:8px; background:var(--acc); border-radius:4px; margin-bottom:3px; }}
.b2 {{ height:8px; background:var(--muted); border-radius:4px; opacity:.6; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th,td {{ padding:7px 6px; border-bottom:1px solid var(--line); text-align:left; }}
th {{ color:var(--muted); font-weight:600; }}
td.num {{ text-align:right; font-variant-numeric:tabular-nums; }}
tr.w td:first-child {{ box-shadow:inset 3px 0 var(--pos); }}
tr.l td:first-child {{ box-shadow:inset 3px 0 var(--neg); }}
tr.me {{ background:color-mix(in srgb, var(--acc) 18%, transparent); font-weight:600; }}
ul.ins {{ padding-left:18px; }} ul.ins li {{ margin:6px 0; }}
.empty {{ color:var(--muted); padding:12px 0; }}
pre {{ background:var(--card); border-radius:12px; padding:12px; white-space:pre-wrap;
       word-break:break-word; font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; }}
button.copy {{ background:var(--acc); color:#fff; border:none; border-radius:10px;
               padding:10px 14px; font:600 14px inherit; cursor:pointer; margin-bottom:10px; }}
.tablewrap {{ overflow-x:auto; }}
</style></head><body>
<h1>{_e(team)}</h1>
<div class="sub">Разбор собран ботом · {_e(data.get('generated', ''))}</div>

<div class="tabs">
  <button class="tab on" data-p="p0">Обзор</button>
  <button class="tab" data-p="p1">Сезон</button>
  <button class="tab" data-p="p2">Последняя игра</button>
  <button class="tab" data-p="p3">Динамика</button>
  <button class="tab" data-p="p6">Состав</button>
  <button class="tab" data-p="p7">Лига</button>
  <button class="tab" data-p="p4">Соперник</button>
  <button class="tab" data-p="p5">Промт для ИИ</button>
</div>

<div class="pane on" id="p0">
  {kpi}
  <div class="chart"><div class="ct">Выводы</div><ul class="ins">{ins_html}</ul></div>
  {diff_chart}
</div>

<div class="pane" id="p1">
  {pts_chart}
  <div class="chart"><div class="ct">Все игры сезона</div>
    <div class="tablewrap">{_games_table(series, limit=40)}</div></div>
</div>

<div class="pane" id="p2">
  {last_bars or '<div class="empty">Нет данных по последней игре.</div>'}
</div>

<div class="pane" id="p3">
  {('<div class="sub">' + _e(prev_title) + '</div>' + prev_bars) if prev
   else (split_html or '<div class="empty">Игр пока мало: динамику покажем, '
                       'когда наберётся хотя бы шесть.</div>')}
</div>

<div class="pane" id="p6">
  <div class="chart"><div class="ct">Состав: в среднем за игру</div>
    <div class="tablewrap">{_roster_table(data.get('roster') or [])}</div></div>
</div>

<div class="pane" id="p7">
  <div class="chart"><div class="ct">Турнирная таблица</div>
    <div class="tablewrap">{_standings_table(data.get('standings') or [],
                                             data.get('our_team_id', ''))}</div></div>
  {league_bars}
</div>

<div class="pane" id="p4">
  <div class="ct" style="margin-bottom:8px">Встречи с «{_e(opp_name or '—')}»</div>
  <div class="tablewrap">{h2h_html}</div>
</div>

<div class="pane" id="p5">
  <button class="copy" onclick="cp()">Скопировать промт</button>
  <pre id="prompt">{_e(prompt)}</pre>
</div>

<script>
for (const t of document.querySelectorAll('.tab')) {{
  t.onclick = () => {{
    document.querySelectorAll('.tab').forEach(x => x.classList.toggle('on', x === t));
    document.querySelectorAll('.pane').forEach(p => p.classList.toggle('on', p.id === t.dataset.p));
  }};
}}
function cp() {{
  const text = document.getElementById('prompt').textContent;
  // clipboard API есть не везде (встроенный браузер Telegram, http) — запасной
  // путь через выделение, иначе кнопка молча ничего не делает.
  const done = () => {{ const b = document.querySelector('.copy');
                        b.textContent = 'Скопировано ✓';
                        setTimeout(() => b.textContent = 'Скопировать промт', 2000); }};
  if (navigator.clipboard) {{ navigator.clipboard.writeText(text).then(done, sel); }}
  else sel();
  function sel() {{
    const r = document.createRange(); r.selectNodeContents(document.getElementById('prompt'));
    const s = window.getSelection(); s.removeAllRanges(); s.addRange(r);
    try {{ document.execCommand('copy'); done(); }} catch (e) {{}}
  }}
}}
</script>
</body></html>"""
