#!/usr/bin/env python3
"""
Развёрнутый месячный отчёт игрока — один самодостаточный HTML-файл в личку.

Почему файл, а не сообщение: здесь помещается то, что в чат не влезает —
разбор каждой игры, динамика, сравнение с прошлым месяцем, броски, роль в
команде и фэнтези-лига.

Одна лента без скриптов и без свёрнутых блоков. Раньше разделы прятались в
<details>, но встроенный браузер Telegram не выполняет JS и раскрывал их через
раз — человек видел первый экран и считал, что отчёт пустой. Теперь всё идёт
подряд, оформление общее с командным разбором (team_report_html.PAGE_CSS).

Внизу — блок «Отдать ИИ»: готовый промпт и те же данные в компактном виде.
Файл можно скинуть в любой чат-бот и получить разбор глубже нашего: мы честно
считаем цифры, но не выдумываем тренерских советов, а модель с контекстом
сможет предложить трактовки.

Выводы формулируем как НАБЛЮДЕНИЯ с цифрами («трёхочковых 12% при 5 попытках»),
а не как указания: бокс-скор не знает ни установки тренера, ни качества броска.

ФИО в файл не попадает — только номера и идентификаторы ([[legal-data-invariant]]).
"""

import html
import json
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache
import fantasy_stats
import personal_report
import team_report_html as T      # общие SVG-графики, KPI-плитки и оформление


def _month_games(source: str, player_id: str, year: int, month: int) -> List[Dict[str, Any]]:
    sheets_cache.init_db()
    prefix = f"{year:04d}-{month:02d}"
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            """SELECT * FROM game_player_stats
               WHERE source = ? AND player_id = ? AND game_date LIKE ?
               ORDER BY game_date""", (source, str(player_id), prefix + "%"))]


def _game_context(source: str, game_id: str, my_team: str, me: str) -> Dict[str, Any]:
    """Счёт, соперник и вклад игрока в командные показатели той игры."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        meta = conn.execute(
            """SELECT home_team_id, guest_team_id, home_score, guest_score
               FROM game_meta WHERE source = ? AND game_id = ?""",
            (source, str(game_id))).fetchone()
        totals = conn.execute(
            """SELECT SUM(pts) AS pts, SUM(reb) AS reb, SUM(ast) AS ast
               FROM game_player_stats
               WHERE source = ? AND game_id = ? AND team_id = ?""",
            (source, str(game_id), str(my_team))).fetchone()
    ctx: Dict[str, Any] = {"team_pts": int((totals or {})["pts"] or 0) if totals else 0,
                           "team_reb": int((totals or {})["reb"] or 0) if totals else 0}
    if meta:
        at_home = str(meta["home_team_id"] or "") == str(my_team)
        ctx.update(opponent=str(meta["guest_team_id"] if at_home else meta["home_team_id"]),
                   at_home=at_home,
                   ours=int((meta["home_score"] if at_home else meta["guest_score"]) or 0),
                   theirs=int((meta["guest_score"] if at_home else meta["home_score"]) or 0))
    return ctx


def _prev_month(year: int, month: int) -> Tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)


def _avg(rows: List[Dict[str, Any]], key: str) -> float:
    if not rows:
        return 0.0
    return round(sum(float(r.get(key) or 0) for r in rows) / len(rows), 1)


def _totals(rows: List[Dict[str, Any]], key: str) -> int:
    return int(sum(int(r.get(key) or 0) for r in rows))


def observations(source: str, player_id: str, games: List[Dict[str, Any]]) -> List[str]:
    """Наблюдения с цифрами — без указаний, что делать."""
    out: List[str] = []
    if not games:
        return out

    # Дабл-даблы: их считают в любой лиге, и это понятная человеку веха.
    dd = 0
    for g in games:
        big = [k for k in ("pts", "reb", "ast", "stl", "blk")
               if int(g.get(k) or 0) >= 10]
        if len(big) >= 2:
            dd += 1
    if dd:
        out.append(f"Дабл-даблов за месяц: {dd} из {len(games)} игр.")

    secs = _totals(games, "secs")
    if secs:
        out.append(f"На площадке в среднем {round(secs / len(games) / 60, 1)} минут "
                   f"за игру ({round(secs / 60)} всего).")
    fo = _totals(games, "foul_on")
    if fo:
        out.append(f"Заработано фолов: {fo} — на тебе фолили "
                   f"{round(fo / len(games), 1)} раза за игру.")
    pm = [int(g.get("plus_minus") or 0) for g in games]
    if any(pm):
        out.append(f"Плюс-минус: {sum(pm):+d} за месяц, "
                   f"{round(sum(pm) / len(pm), 1):+g} за игру.")

    sh = personal_report.shooting(source, player_id, last_n=len(games))
    two, three = sh.get("2-очковые"), sh.get("3-очковые")
    if two and three and three["per_game"] >= 1:
        out.append(f"Трёхочковые: {three['now']}% при {three['per_game']} попытках за игру; "
                   f"двухочковые — {two['now']}% при {two['per_game']}.")
    ft = sh.get("штрафные")
    if ft and ft["att"] >= 5:
        out.append(f"Штрафные: {ft['now']}% ({ft['made']} из {ft['att']}).")

    role = personal_report.team_role(source, player_id, last_n=len(games))
    if role:
        out.append(f"На тебе {role['pts_share']}% очков команды и "
                   f"{role['reb_share']}% подборов за эти игры.")

    tur = [int(g.get("tur") or 0) for g in games]
    ast = [int(g.get("ast") or 0) for g in games]
    if sum(tur) and sum(ast):
        ratio = round(sum(ast) / sum(tur), 2)
        out.append(f"Передач на потерю: {ratio} ({sum(ast)} против {sum(tur)}).")

    fps = [fantasy_stats.fantasy_points(g) for g in games]
    if len(fps) >= 3:
        spread = round(max(fps) - min(fps), 1)
        out.append(f"Разброс по играм: от {min(fps):g} до {max(fps):g} "
                   f"(размах {spread:g}) — насколько ровно шёл месяц.")
    return out


def ai_prompt(payload: Dict[str, Any]) -> str:
    """Готовый промпт: человек копирует его вместе с данными в любой чат-бот."""
    return (
        "Ты — тренер по баскетболу. Ниже статистика одного игрока-любителя за "
        "месяц по официальным протоколам: каждая игра, соперник, счёт, а также "
        "его доля в командных показателях.\n\n"
        "Разбери: (1) в чём он прибавил и просел, с опорой на цифры; "
        "(2) какие показатели тянут команду, а какие вредят; (3) на что обратить "
        "внимание на тренировках; (4) чего по этим данным сказать НЕЛЬЗЯ — "
        "какие выводы были бы домыслом.\n\n"
        "Учти: это любительская лига, состав меняется от игры к игре, "
        "минуты не всегда репрезентативны.\n\n"
        "ДАННЫЕ:\n" + json.dumps(payload, ensure_ascii=False, indent=1)
    )


MONTHS_RU = {1: "январь", 2: "февраль", 3: "март", 4: "апрель", 5: "май",
             6: "июнь", 7: "июль", 8: "август", 9: "сентябрь", 10: "октябрь",
             11: "ноябрь", 12: "декабрь"}

# Что сравниваем с прошлым месяцем. Проценты попадания сюда не берём — их
# нельзя усреднять по играм, они считаются отдельно из попыток и попаданий.
COMPARE_KEYS = (("pts", "Очки"), ("reb", "Подборы"), ("ast", "Передачи"),
                ("stl", "Перехваты"), ("blk", "Блок-шоты"), ("tur", "Потери"),
                ("foul_on", "Заработано фолов"))


def _shot_rows(games: List[Dict[str, Any]]) -> List[Tuple[str, int, int]]:
    """(подпись, попал, бросил) по видам бросков за набор игр."""
    two_m = _totals(games, "fgm") - _totals(games, "tpm")
    two_a = _totals(games, "fga") - _totals(games, "tpa")
    return [("2-очковые", max(0, two_m), max(0, two_a)),
            ("3-очковые", _totals(games, "tpm"), _totals(games, "tpa")),
            ("Штрафные", _totals(games, "ftm"), _totals(games, "fta")),
            ("Все с игры", _totals(games, "fgm"), _totals(games, "fga"))]


def _fantasy_html(fan: Dict[str, Any]) -> str:
    """Раздел «Фэнтези-лига»: как оценивают тебя и как играешь ты сам."""
    if not fan:
        return ""
    out = []
    a = fan.get("asset") or {}
    if a:
        kpi = [("Брали в состав", str(a["picked_times"]),
                f"в {a['games_picked']} играх из {a['games_total']}"),
               ("Менеджеров", str(a["managers"]), "поставили тебя хоть раз"),
               ("Заработал бы им", f"{a['brought']:g}", f"{a['per_game']:g} за игру")]
        if a.get("popularity_place"):
            kpi.append(("По популярности", f"{a['popularity_place']}-й",
                        f"из {a['pool_size']} в пуле"))
        pr = a.get("price") or {}
        if pr.get("found"):
            kpi.append(("Твоя цена", str(pr.get("price", "—")),
                        f"ранг {pr.get('rank_title') or pr.get('rank') or '—'}"))
        head = ('Тебя выбирали' if a["picked_times"]
                else 'Тебя пока не выбирали')
        out.append(f'<div class="ct">{head}</div>' + T._kpi(kpi))
        if not a["picked_times"]:
            out.append('<div class="legend">В этом месяце тебя не поставил ни один '
                       'участник. «Заработал бы им» — сколько очков ты принёс бы '
                       'тому, кто взял бы тебя во все игры месяца.</div>')
        if pr.get("found") and pr.get("need_up_next") is not None:
            out.append(f'<div class="legend">До следующего ранга: набрать '
                       f'{pr["need_up_next"]:g} фэнтези-очков в ближайшей игре.</div>')
        if len(a.get("by_game") or []) >= 2:
            out.append(T._line_chart([(T._dmy(d), v) for d, v in a["by_game"]],
                                     "Сколько ты приносил по играм", zero_line=False))
        if a.get("best"):
            out.append(f'<div class="legend">Лучшая игра месяца по фэнтези-очкам: '
                       f'{T._dmy(a["best"][0])} — {a["best"][1]:g}.</div>')
    m = fan.get("manager") or {}
    if m:
        kpi = [("Твои очки", f"{m['points']:g}", f"{m['per_game']:g} за игру"),
               ("Место", f"{m['place']}-е" if m.get("place") else "—",
                f"из {m['of']} участников"),
               ("Лучший тур", f"{m['best_game']['points']:g}",
                T._dmy(m["best_game"]["date"]))]
        if m.get("leader") is not None:
            kpi.append(("У лидера", f"{m['leader']:g}",
                        f"отрыв {round(m['leader'] - m['points'], 1):g}"))
        out.append('<div class="ct" style="margin-top:14px">Ты выбирал</div>'
                   + T._kpi(kpi))
        if m.get("mode"):
            out.append(f'<div class="legend">Режим: {T._e(m["mode"])}.</div>')
    return f'<div class="chart">{"".join(out)}</div>' if out else ""


def build_html(source_title: str, source: str, player_id: str,
               year: int, month: int,
               team_names: Optional[Dict[str, str]] = None,
               tg_user_id: Optional[Any] = None,
               name: str = "") -> Optional[str]:
    """Самодостаточный HTML месячного отчёта. None — если игр в месяце не было.

    Одна лента без скриптов и без свёрнутых блоков: встроенный браузер Telegram
    не выполняет JS и не даёт переходов, а раскрывающиеся <details> там
    открывались через раз. Всё, что раньше пряталось за кликом, теперь просто
    идёт подряд."""
    games = _month_games(source, player_id, year, month)
    if not games:
        return None
    team_names = team_names or {}
    title_month = MONTHS_RU[month]
    prev_y, prev_m = _prev_month(year, month)
    prev_games = _month_games(source, player_id, prev_y, prev_m)

    # ── Итог месяца ─────────────────────────────────────────────────────────
    fps = [fantasy_stats.fantasy_points(g) for g in games]
    secs = _totals(games, "secs")
    kpi_items = [
        ("Игр за месяц", str(len(games)),
         f"{_d(games[0]['game_date'])} – {_d(games[-1]['game_date'])}"),
        ("Очки", f"{_avg(games, 'pts'):g}", f"{_totals(games, 'pts')} всего"),
        ("Подборы", f"{_avg(games, 'reb'):g}",
         f"в атаке {_avg(games, 'reb_off'):g}"),
        ("Передачи", f"{_avg(games, 'ast'):g}", f"потери {_avg(games, 'tur'):g}"),
        ("Фэнтези-очки", f"{round(sum(fps) / len(fps), 1):g}", "за игру"),
    ]
    if secs:
        kpi_items.append(("Минуты", f"{round(secs / len(games) / 60, 1):g}", "за игру"))
    kpi = T._kpi(kpi_items)

    obs = observations(source, player_id, games)
    obs_html = "".join(f"<li>{html.escape(o)}</li>" for o in obs) \
        or "<li>Данных за месяц мало.</li>"

    # ── Динамика ────────────────────────────────────────────────────────────
    fp_chart = T._line_chart([(_d(g["game_date"]), round(f, 1))
                              for g, f in zip(games, fps)],
                             "Фэнтези-очки по играм", zero_line=False)
    pts_chart = T._line_chart([(_d(g["game_date"]), int(g.get("pts") or 0))
                               for g in games], "Очки по играм", zero_line=False)

    # ── Против прошлого месяца ──────────────────────────────────────────────
    if prev_games:
        rows = [(t, _avg(games, k), _avg(prev_games, k)) for k, t in COMPARE_KEYS]
        prev_html = (f'<div class="legend">{MONTHS_RU[prev_m]}: игр '
                     f'{len(prev_games)}. Всё — в среднем за игру.</div>'
                     + T._bars(rows, "", "этот месяц", MONTHS_RU[prev_m]))
    else:
        prev_html = (f'<div class="empty">За {MONTHS_RU[prev_m]} игр в базе нет — '
                     f'сравнивать не с чем.</div>')

    # ── Броски ──────────────────────────────────────────────────────────────
    shot_now, shot_prev = _shot_rows(games), _shot_rows(prev_games)
    prev_by = {t: (m, a) for t, m, a in shot_prev}
    shot_rows = []
    for title, made, att in shot_now:
        pm, pa = prev_by.get(title, (0, 0))
        was = f"{pm / pa * 100:.0f}%" if pa else "—"
        shot_rows.append(
            f'<tr><td>{title}</td><td class="num">{made}/{att}</td>'
            f'<td class="num">{T._pct(made, att)}</td>'
            f'<td class="num">{round(att / len(games), 1):g}</td>'
            f'<td class="num">{was}</td></tr>')
    shots_html = (
        '<div class="tablewrap"><table><thead><tr><th>Бросок</th>'
        '<th>Попал/бросил</th><th>%</th><th>Попыток за игру</th>'
        f'<th>{MONTHS_RU[prev_m]}</th></tr></thead>'
        f'<tbody>{"".join(shot_rows)}</tbody></table></div>')

    # ── Роль в команде ──────────────────────────────────────────────────────
    role = personal_report.team_role(source, player_id, last_n=len(games))
    role_html = ""
    if role:
        role_html = T._kpi([
            ("Очки команды", f"{role['pts_share']}%", "приходятся на тебя"),
            ("Подборы команды", f"{role['reb_share']}%", "твои"),
            ("Передачи команды", f"{role.get('ast_share', 0)}%", "твои"),
        ])

    # ── Фэнтези ─────────────────────────────────────────────────────────────
    fan = {}
    try:
        import personal_fantasy
        fan = personal_fantasy.month(tg_user_id, source, player_id, year, month, name)
    except Exception as e:      # фэнтези — приятное дополнение, не причина падать
        print(f"⚠️  Фэнтези-раздел не собрался: {e}")
    fan_html = _fantasy_html(fan)

    # ── Против соперников ───────────────────────────────────────────────────
    vs = personal_report.vs_opponents(source, player_id, limit=5)
    vs_items = []
    for v in vs:
        opp_id = str(v["opponent"])
        nm = html.escape(team_names.get(opp_id) or f"соперник №{opp_id}")
        vs_items.append(
            f"<li>{nm}: встреч {v['meetings']}, побед {v['wins']}; "
            f"было {v['prev']['pts']} очк ({_d(v['prev_date'])}) → "
            f"стало {v['last']['pts']} очк ({_d(v['last_date'])}); "
            f"состав команды совпал на {v['roster_overlap']}%</li>")

    # ── Игры месяца ─────────────────────────────────────────────────────────
    rows_html: List[str] = []
    payload_games: List[Dict[str, Any]] = []
    for g, fp in zip(games, fps):
        ctx = _game_context(source, g["game_id"], g.get("team_id"), player_id)
        opp = ctx.get("opponent", "")
        opp_name = team_names.get(str(opp)) or (f"№{opp}" if opp else "—")
        score = f"{ctx['ours']}:{ctx['theirs']}" if "ours" in ctx else "—"
        won = ctx.get("ours", 0) > ctx.get("theirs", 0) if "ours" in ctx else None
        share = (round(int(g.get("pts") or 0) / ctx["team_pts"] * 100)
                 if ctx.get("team_pts") else None)
        payload_games.append({
            "дата": g["game_date"], "соперник": opp_name, "счёт": score,
            "дома": ctx.get("at_home"), "победа": won,
            "минуты": round(int(g.get("secs") or 0) / 60, 1),
            "очки": int(g.get("pts") or 0), "подборы": int(g.get("reb") or 0),
            "передачи": int(g.get("ast") or 0), "перехваты": int(g.get("stl") or 0),
            "блоки": int(g.get("blk") or 0), "потери": int(g.get("tur") or 0),
            "фолы": int(g.get("pf") or 0),
            "заработано_фолов": int(g.get("foul_on") or 0),
            "броски_с_игры": f"{g.get('fgm', 0)}/{g.get('fga', 0)}",
            "броски_3": f"{g.get('tpm', 0)}/{g.get('tpa', 0)}",
            "штрафные": f"{g.get('ftm', 0)}/{g.get('fta', 0)}",
            "фэнтези_очки": round(fp, 1),
            "доля_очков_команды_%": share,
        })
        mark = "✅" if won else ("❌" if won is False else "")
        rows_html.append(
            f'<tr class="{"w" if won else ("l" if won is False else "")}">'
            f'<td>{html.escape(_d(g["game_date"]))}</td>'
            f'<td>{html.escape(opp_name)} {mark}</td>'
            f'<td class="num">{html.escape(score)}</td>'
            f'<td class="num">{int(g.get("pts") or 0)}</td>'
            f'<td class="num">{int(g.get("reb") or 0)}</td>'
            f'<td class="num">{int(g.get("ast") or 0)}</td>'
            f'<td class="num">{int(g.get("stl") or 0)}</td>'
            f'<td class="num">{int(g.get("tur") or 0)}</td>'
            f'<td class="num">{int(g.get("fgm") or 0)}/{int(g.get("fga") or 0)}</td>'
            f'<td class="num">{int(g.get("tpm") or 0)}/{int(g.get("tpa") or 0)}</td>'
            f'<td class="num">{int(g.get("ftm") or 0)}/{int(g.get("fta") or 0)}</td>'
            f'<td class="num">{f"{share}%" if share is not None else "—"}</td>'
            f'<td class="num"><b>{fp:g}</b></td></tr>')

    payload = {
        "лига": source_title, "месяц": f"{title_month} {year}",
        "игр_в_месяце": len(games), "игры": payload_games,
        "наблюдения": obs,
        "броски_за_месяц": {t: f"{m}/{a}" for t, m, a in shot_now},
        "прошлый_месяц": ({"месяц": MONTHS_RU[prev_m], "игр": len(prev_games),
                           **{t: _avg(prev_games, k) for k, t in COMPARE_KEYS}}
                          if prev_games else None),
        "фэнтези": fan or None,
    }
    prompt = ai_prompt(payload)

    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Личный отчёт · {html.escape(title_month)} {year}</title>
<style>
{T.PAGE_CSS}
</style></head><body>
<h1>Личный отчёт · {html.escape(title_month)} {year}</h1>
<div class="sub">{html.escape(source_title)} · игр за месяц: {len(games)}</div>

<div class="toc">Всё одной страницей, просто листай вниз:<br>
<b>1</b> Итог месяца · <b>2</b> Динамика · <b>3</b> Против прошлого месяца ·
<b>4</b> Броски · <b>5</b> Роль в команде{' · <b>6</b> Фэнтези' if fan_html else ''} ·
<b>{6 + bool(fan_html)}</b> Против соперников ·
<b>{7 + bool(fan_html)}</b> Игры месяца ·
<b>{8 + bool(fan_html)}</b> Промт для ИИ</div>

<h2><span class="no">1.</span> Итог месяца</h2>
{kpi}
<div class="chart"><div class="ct">Что видно по цифрам</div>
  <ul class="ins">{obs_html}</ul>
  <div class="legend">Это наблюдения, а не указания: протокол не знает ни
    установки тренера, ни качества броска — выводы делаешь ты.</div></div>

<h2><span class="no">2.</span> Динамика</h2>
{fp_chart}
{pts_chart}

<h2><span class="no">3.</span> Против прошлого месяца</h2>
{prev_html}

<h2><span class="no">4.</span> Броски</h2>
{shots_html}

<h2><span class="no">5.</span> Роль в команде<small>какая доля командных
  показателей приходится на тебя в этих играх</small></h2>
{role_html or '<div class="empty">Не хватает данных по командам этих игр.</div>'}

{f'<h2><span class="no">6.</span> Фэнтези-лига<small>{html.escape(str(fan.get("season", "")))}</small></h2>{fan_html}' if fan_html else ''}

<h2><span class="no">{6 + bool(fan_html)}.</span> Против соперников</h2>
{f'<ul class="ins">{"".join(vs_items)}</ul>' if vs_items
 else '<div class="empty">Повторных встреч пока не было.</div>'}

<h2><span class="no">{7 + bool(fan_html)}.</span> Игры месяца</h2>
<div class="tablewrap"><table><thead><tr><th>Дата</th><th>Соперник</th>
<th>Счёт</th><th>Очк</th><th>Подб</th><th>Пас</th><th>Пх</th><th>Пот</th>
<th>Игра</th><th>3-оч</th><th>Штр</th><th>Доля</th><th>ФО</th></tr></thead>
<tbody>{''.join(rows_html)}</tbody></table></div>
<div class="swipe">Таблица шире экрана — тяни её вбок пальцем.</div>

<h2><span class="no">{8 + bool(fan_html)}.</span> Промт для ИИ<small>Долгое
  нажатие по тексту — выделить и скопировать, дальше вставить в любого
  чат-бота.</small></h2>
<pre>{html.escape(prompt)}</pre>
</body></html>"""


def _d(iso: str) -> str:
    try:
        d = date.fromisoformat(iso)
        return f"{d.day:02d}.{d.month:02d}"
    except (ValueError, TypeError):
        return iso


# ─────────────────────────── Запуск и отправка ───────────────────────────────

def _slpro_team_names() -> Dict[str, str]:
    """Названия команд — ИЗ ЛОКАЛЬНОЙ БАЗЫ, чтобы в отчёте был «Кирпичный
    Завод», а не №999.

    Раньше тут был живой запрос в лигу. Отчёт собирается в ответ на нажатие
    кнопки, а недоступная лига стоила минуту ожидания — теперь имена берём из
    протоколов, которые и так скачаны: `game_meta` хранит названия обеих
    команд каждой игры."""
    sheets_cache.init_db()
    names: Dict[str, str] = {}
    with sheets_cache.get_connection() as conn:
        for r in conn.execute(
                """SELECT home_team_id, home_name, guest_team_id, guest_name
                   FROM game_meta WHERE home_name != '' OR guest_name != ''"""):
            if r["home_name"]:
                names[str(r["home_team_id"])] = r["home_name"]
            if r["guest_name"]:
                names[str(r["guest_team_id"])] = r["guest_name"]
        for r in conn.execute("SELECT team_id, name FROM league_teams WHERE name != ''"):
            names.setdefault(str(r["team_id"]), r["name"])
    return names


def build_combined(profiles: List[tuple], year: int, month: int,
                   team_names: Optional[Dict[str, str]] = None,
                   tg_user_id: Optional[Any] = None) -> Optional[str]:
    """ОДИН файл по всем лигам игрока.

    Человек играет в двух лигах и хочет видеть себя целиком, а не два отдельных
    отчёта: сравнивать «там прибавил, тут просел» удобнее в одном месте."""
    import player_identity
    import player_names
    parts, any_games = [], False
    names = team_names if team_names is not None else _slpro_team_names()
    for src, pid in profiles:
        title = player_identity.SOURCE_TITLES.get(src, src)
        # Имя нужно ровно для поиска цены в листе «Игроки» — транзитно, из
        # реестра в памяти. Нет его — фэнтези-раздел просто будет без цены.
        htm = build_html(title, src, pid, year, month, team_names=names,
                         tg_user_id=tg_user_id,
                         name=player_names.get(src, pid))
        if not htm:
            continue
        any_games = True
        # Вырезаем тело каждого отчёта и склеиваем под общей шапкой.
        body = htm.split("<body>", 1)[1].rsplit("</body>", 1)[0]
        parts.append(body)
    if not any_games:
        return None
    if len(parts) == 1:
        head = build_html("", profiles[0][0], profiles[0][1], year, month)  # ради стилей
        return head.split("<body>")[0] + "<body>" + parts[0] + "</body></html>"
    shell = build_html("", profiles[0][0], profiles[0][1], year, month) or ""
    head = shell.split("<body>")[0]
    return head + "<body>" + "<hr style='margin:28px 0;opacity:.3'>".join(parts) + "</body></html>"


def build_for(tg_user_id: Optional[str], source: Optional[str], player_id: Optional[str],
              year: int, month: int) -> List[tuple]:
    """[(имя файла, html)] по всем привязанным профилям (или по указанному)."""
    import player_identity
    if source and player_id:
        pairs = [(source, player_id)]
    else:
        pairs = [(r["source"], r["player_id"])
                 for r in player_identity.get_identities(tg_user_id or "")]
    names = _slpro_team_names()
    out = []
    for src, pid in pairs:
        title = player_identity.SOURCE_TITLES.get(src, src)
        htm = build_html(title, src, pid, year, month,
                         team_names=names if src == "slpro" else {})
        if htm:
            out.append((f"otchet_{src}_{year}-{month:02d}.html", htm))
        else:
            print(f"ℹ️  {title}: игр в {month:02d}.{year} не найдено")
    return out


async def send_all(year: int, month: int, dry_run: bool = False) -> int:
    """Месячная рассылка: каждому, кто привязал профиль и не выключил
    уведомления, — один файл по всем его лигам, в ЛИЧКУ.

    Идемпотентно по last_sent: повторный запуск в том же месяце никого не
    задваивает, поэтому крон можно перезапускать без опаски."""
    import os
    import io
    import personal_report
    import player_identity
    from telegram import Bot

    token = os.getenv("BOT_TOKEN", "")
    if not token:
        print("❌ BOT_TOKEN не задан")
        return 2
    bot = Bot(token)
    sent = skipped = empty = 0
    for uid in player_identity.linked_users():
        prefs = personal_report.get_prefs(uid)
        if not personal_report.monthly_file_due(prefs):
            skipped += 1
            continue
        profiles = [(r["source"], r["player_id"])
                    for r in player_identity.get_identities(uid)]
        html_doc = build_combined(profiles, year, month, tg_user_id=uid)
        if not html_doc:
            empty += 1
            continue
        if dry_run:
            print(f"→ {uid}: файл готов ({len(html_doc)} байт), отправка отключена")
            sent += 1
            continue
        bio = io.BytesIO(html_doc.encode("utf-8"))
        name = f"otchet_{year}-{month:02d}.html"
        try:
            await bot.send_document(
                chat_id=int(uid), document=bio, filename=name,
                caption="📊 Отчёт за месяц по всем твоим лигам. Внизу файла — "
                        "готовый запрос для ИИ, если захочешь разбор поглубже.")
        except Exception as e:
            # Человек мог не запускать бота или заблокировать его — не повод
            # ронять рассылку остальным.
            print(f"⚠️ {uid}: не отправилось ({e})")
            continue
        personal_report.mark_sent(uid)
        sent += 1
    print(f"📨 Месячная рассылка {month:02d}.{year}: отправлено {sent}, "
          f"без игр {empty}, не время/выключено {skipped}")
    return 0


def main() -> int:
    import argparse
    import os

    ap = argparse.ArgumentParser(description="Месячный личный отчёт игрока (HTML)")
    ap.add_argument("--tg", help="Telegram id игрока (берём его привязанные профили)")
    ap.add_argument("--source", choices=["slpro", "infobasket"])
    ap.add_argument("--player-id", help="id в лиге, если профиль ещё не привязан")
    ap.add_argument("--month", help="YYYY-MM (по умолчанию прошлый месяц)")
    ap.add_argument("--out-dir", default="/tmp", help="куда положить файлы")
    ap.add_argument("--send", action="store_true",
                    help="отправить файл в ЛИЧКУ (--tg обязателен)")
    ap.add_argument("--all", action="store_true",
                    help="месячная рассылка всем, кто привязал профиль (для крона)")
    ap.add_argument("--dry-run", action="store_true",
                    help="с --all: собрать файлы, но не отправлять")
    args = ap.parse_args()

    if args.month:
        year, month = (int(x) for x in args.month.split("-"))
    else:
        today = date.today()
        year, month = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)

    if args.all:
        import asyncio as _asyncio
        return _asyncio.run(send_all(year, month, dry_run=args.dry_run))

    files = build_for(args.tg, args.source, args.player_id, year, month)
    if not files:
        print("Нечего отправлять.")
        return 0

    paths = []
    for name, htm in files:
        path = os.path.join(args.out_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            f.write(htm)
        os.chmod(path, 0o600)     # личная статистика — не для соседей по серверу
        paths.append(path)
        print(f"✅ {path} ({len(htm)} байт)")

    if not args.send:
        return 0
    if not args.tg:
        print("❌ --send без --tg: некуда слать")
        return 2
    if str(args.tg).startswith("-"):
        # Ровно на этом уже обжигались: личная статистика ушла в общий чат.
        print("❌ Отказ: id похож на групповой чат. Личный отчёт — только в приват.")
        return 2

    import asyncio
    from telegram import Bot
    token = os.getenv("BOT_TOKEN", "")
    if not token:
        print("❌ BOT_TOKEN не задан")
        return 2

    async def send():
        bot = Bot(token)
        for path in paths:
            with open(path, "rb") as f:
                await bot.send_document(
                    chat_id=int(args.tg), document=f,
                    filename=os.path.basename(path),
                    caption="📊 Развёрнутый отчёт за месяц. Внизу файла — готовый "
                            "запрос для ИИ, если захочешь разбор поглубже.")
            print(f"📨 отправлено: {os.path.basename(path)}")

    asyncio.run(send())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
