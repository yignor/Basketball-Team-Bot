#!/usr/bin/env python3
"""
Развёрнутый месячный отчёт игрока — один самодостаточный HTML-файл в личку.

Почему файл, а не сообщение: здесь помещается то, что в чат не влезает —
разбор каждой игры, сравнение с соперниками, броски. Разделы свёрнуты
(<details>), поэтому сверху виден итог, а подробности открываются по нажатию.

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
from typing import Any, Dict, List, Optional

import sheets_cache
import fantasy_stats
import personal_report


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


def observations(source: str, player_id: str, games: List[Dict[str, Any]]) -> List[str]:
    """Наблюдения с цифрами — без указаний, что делать."""
    out: List[str] = []
    if not games:
        return out

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


def build_html(source_title: str, source: str, player_id: str,
               year: int, month: int,
               team_names: Optional[Dict[str, str]] = None) -> Optional[str]:
    """Самодостаточный HTML месячного отчёта. None — если игр в месяце не было."""
    games = _month_games(source, player_id, year, month)
    if not games:
        return None
    team_names = team_names or {}
    title_month = {
        1: "январь", 2: "февраль", 3: "март", 4: "апрель", 5: "май", 6: "июнь",
        7: "июль", 8: "август", 9: "сентябрь", 10: "октябрь", 11: "ноябрь",
        12: "декабрь"}[month]

    rows_html: List[str] = []
    payload_games: List[Dict[str, Any]] = []
    for g in games:
        ctx = _game_context(source, g["game_id"], g.get("team_id"), player_id)
        opp = ctx.get("opponent", "")
        opp_name = team_names.get(str(opp)) or (f"соперник №{opp}" if opp else "соперник неизвестен")
        score = (f"{ctx['ours']}:{ctx['theirs']}" if "ours" in ctx else "—")
        won = ctx.get("ours", 0) > ctx.get("theirs", 0) if "ours" in ctx else None
        fp = fantasy_stats.fantasy_points(g)
        share = (round(int(g.get("pts") or 0) / ctx["team_pts"] * 100)
                 if ctx.get("team_pts") else None)

        payload_games.append({
            "дата": g["game_date"], "соперник": opp_name, "счёт": score,
            "дома": ctx.get("at_home"), "победа": won,
            "очки": int(g.get("pts") or 0), "подборы": int(g.get("reb") or 0),
            "передачи": int(g.get("ast") or 0), "перехваты": int(g.get("stl") or 0),
            "блоки": int(g.get("blk") or 0), "потери": int(g.get("tur") or 0),
            "фолы": int(g.get("pf") or 0),
            "броски_2": f"{g.get('fgm', 0)}/{g.get('fga', 0)}",
            "броски_3": f"{g.get('tpm', 0)}/{g.get('tpa', 0)}",
            "штрафные": f"{g.get('ftm', 0)}/{g.get('fta', 0)}",
            "доля_очков_команды_%": share,
        })

        rows_html.append(f"""
    <details>
      <summary><b>{html.escape(_d(g['game_date']))}</b> · {html.escape(opp_name)}
        · {html.escape(score)} {'✅' if won else ('❌' if won is False else '')}
        <span class="fp">{int(g.get('pts') or 0)} очк</span></summary>
      <table>
        <tr><td>Очки</td><td>{int(g.get('pts') or 0)}</td>
            <td>Подборы</td><td>{int(g.get('reb') or 0)}
              (в атаке {int(g.get('reb_off') or 0)} / в защите {int(g.get('reb_def') or 0)})</td></tr>
        <tr><td>Передачи</td><td>{int(g.get('ast') or 0)}</td>
            <td>Потери</td><td>{int(g.get('tur') or 0)}</td></tr>
        <tr><td>Перехваты</td><td>{int(g.get('stl') or 0)}</td>
            <td>Блок-шоты</td><td>{int(g.get('blk') or 0)}</td></tr>
        <tr><td>Фолы</td><td>{int(g.get('pf') or 0)}</td>
            <td>Фэнтези-очки</td><td>{fp:g}</td></tr>
        <tr><td>2-очковые</td><td>{int(g.get('fgm') or 0)}/{int(g.get('fga') or 0)}</td>
            <td>3-очковые</td><td>{int(g.get('tpm') or 0)}/{int(g.get('tpa') or 0)}</td></tr>
        <tr><td>Штрафные</td><td>{int(g.get('ftm') or 0)}/{int(g.get('fta') or 0)}</td>
            <td>Доля очков команды</td><td>{f'{share}%' if share is not None else '—'}</td></tr>
      </table>
    </details>""")

    obs = observations(source, player_id, games)
    vs = personal_report.vs_opponents(source, player_id, limit=5)
    vs_items: List[str] = []
    for v in vs:
        opp_id = str(v["opponent"])
        name = html.escape(team_names.get(opp_id) or f"соперник №{opp_id}")
        vs_items.append(
            f"<li>{name}: встреч {v['meetings']}, побед {v['wins']}; "
            f"было {v['prev']['pts']} очк ({_d(v['prev_date'])}) → "
            f"стало {v['last']['pts']} очк ({_d(v['last_date'])}); "
            f"состав команды совпал на {v['roster_overlap']}%</li>")
    vs_html = "".join(vs_items)

    payload = {
        "лига": source_title, "месяц": f"{title_month} {year}",
        "игр_в_месяце": len(games), "игры": payload_games,
        "наблюдения": obs,
    }
    prompt = ai_prompt(payload)

    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Отчёт · {html.escape(title_month)} {year}</title>
<style>
 :root {{ color-scheme: light dark; }}
 body {{ font-family: -apple-system, "Segoe UI", Roboto, sans-serif; margin: 0;
        padding: 16px; font-size: 16px; line-height: 1.45; }}
 h1 {{ font-size: 20px; margin: 0 0 4px; }}
 .sub {{ opacity: .7; font-size: 14px; margin-bottom: 18px; }}
 h2 {{ font-size: 16px; margin: 22px 0 8px; }}
 details {{ border: 1px solid rgba(128,128,128,.35); border-radius: 10px;
            padding: 10px 12px; margin: 8px 0; }}
 summary {{ cursor: pointer; }}
 .fp {{ float: right; opacity: .75; }}
 table {{ width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 15px; }}
 td {{ padding: 4px 6px; border-top: 1px solid rgba(128,128,128,.2); }}
 td:nth-child(odd) {{ opacity: .7; }}
 ul {{ padding-left: 20px; }}
 textarea {{ width: 100%; min-height: 160px; font-family: ui-monospace, monospace;
             font-size: 12px; padding: 8px; border-radius: 8px;
             border: 1px solid rgba(128,128,128,.35); background: transparent;
             color: inherit; }}
 .note {{ opacity: .7; font-size: 14px; }}
</style></head><body>
<h1>Личный отчёт · {html.escape(title_month)} {year}</h1>
<div class="sub">{html.escape(source_title)} · игр за месяц: {len(games)}</div>

<h2>Что видно по цифрам</h2>
<ul>{''.join(f'<li>{html.escape(o)}</li>' for o in obs) or '<li>Данных за месяц мало.</li>'}</ul>
<div class="note">Это наблюдения, а не указания: протокол не знает ни установки
тренера, ни качества броска — выводы делаешь ты.</div>

<h2>Игры месяца</h2>
<div class="note">Нажми на игру, чтобы раскрыть полный протокол.</div>
{''.join(rows_html)}

{f'<h2>Против соперников</h2><ul>{vs_html}</ul>' if vs_html else ''}

<h2>Отдать ИИ</h2>
<div class="note">Скопируй всё поле ниже и вставь в любой чат-бот — там уже
готовый запрос вместе с данными. Или пришли ему этот файл целиком.</div>
<textarea readonly onclick="this.select()">{html.escape(prompt)}</textarea>
</body></html>"""


def _d(iso: str) -> str:
    try:
        d = date.fromisoformat(iso)
        return f"{d.day:02d}.{d.month:02d}"
    except (ValueError, TypeError):
        return iso


# ─────────────────────────── Запуск и отправка ───────────────────────────────

def _slpro_team_names() -> Dict[str, str]:
    """Названия команд SLPRO — чтобы в отчёте был «Кирпичный Завод», а не №999.
    Не вышло (сеть, смена сезона) — покажем идентификаторы, это не повод падать."""
    try:
        import asyncio
        from slpro_client import SlproClient

        async def go():
            c = SlproClient()
            ctx = await c.discover_context(["PullUp Farm", "Pull Up Farm"])
            return await c.get_standings(ctx) if ctx else []

        rows = asyncio.run(go()) or []
        return {str(r.get("team_id")): str(r.get("name") or r.get("team_name") or "")
                for r in rows if r.get("team_id")}
    except Exception as e:
        print(f"⚠️  Названия команд SLPRO не получены: {e}")
        return {}


def build_combined(profiles: List[tuple], year: int, month: int,
                   team_names: Optional[Dict[str, str]] = None) -> Optional[str]:
    """ОДИН файл по всем лигам игрока.

    Человек играет в двух лигах и хочет видеть себя целиком, а не два отдельных
    отчёта: сравнивать «там прибавил, тут просел» удобнее в одном месте."""
    import player_identity
    parts, any_games = [], False
    for src, pid in profiles:
        title = player_identity.SOURCE_TITLES.get(src, src)
        htm = build_html(title, src, pid, year, month,
                         team_names=(team_names or {}) if src == "slpro" else {})
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
    args = ap.parse_args()

    if args.month:
        year, month = (int(x) for x in args.month.split("-"))
    else:
        today = date.today()
        year, month = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)

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
