#!/usr/bin/env python3
"""
Прогресс команды для тренера: что просело и кто в этом участвовал.

Отчёт отвечает на один вопрос — «что в этой игре пошло не как обычно». Не
таблица всех показателей (её тренер и так видит в протоколе), а несколько
ярких отклонений от СОБСТВЕННОГО среднего, с именами тех, кто их сделал.

Три принципа, из которых всё остальное следует:

1. **Сравниваем с собой, а не с нормой.** «Мало подборов» без базы — пустой
   звук: у одной команды 30 за игру нормально, у другой провал. База — наши
   же последние игры ЭТОЙ ЖЕ команды: Farm в SLPRO и основа в Инфобаскете
   играют в разном темпе, смешивать их нельзя.
2. **Только заметное.** Отклонение попадает в отчёт, если оно и в процентах
   велико, и в абсолюте не мелочь. Иначе список превращается в шум, который
   перестают читать.
3. **С именами.** «Просели подборы» — не действие. «Просели подборы, и вот
   двое, кто взял вдвое меньше своего среднего» — уже разговор на тренировке.

Юр-инвариант ([[legal-data-invariant]]): ФИО не храним, имена приходят
транзитно от вызывающего (пул лиги), внутри модуля — только id.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

OUR_TEAMS = ("707", "36502")          # Farm (SLPRO) и основа (Инфобаскет)

# (ключ, подпись, чем больше — тем лучше)
METRICS: Tuple[Tuple[str, str, bool], ...] = (
    ("pts", "Очки", True),
    ("reb", "Подборы", True),
    ("reb_off", "Подборы в атаке", True),
    ("reb_def", "Подборы в защите", True),
    ("ast", "Передачи", True),
    ("stl", "Перехваты", True),
    ("blk", "Блок-шоты", True),
    ("tur", "Потери", False),
    ("pf", "Фолы", False),
)

# Точность бросков считается отдельно: складывать проценты нельзя, их надо
# пересчитывать из попыток и попаданий.
SHOOTING: Tuple[Tuple[str, str, str], ...] = (
    ("fg", "Броски с игры", "fgm/fga"),
    ("tp", "Трёхочковые", "tpm/tpa"),
    ("ft", "Штрафные", "ftm/fta"),
)

# Что считать НАСТОЯЩЕЙ игрой. В базе попадаются огрызки протоколов: один
# игрок, пара бросков, ни одной передачи — это не игра, а остаток от старого
# парсинга или технарь. В базу сравнения такие попадать не должны: они тянут
# среднее вниз, и обычная игра начинает выглядеть выдающейся («30 передач
# против 9.9 в среднем» — половина этого среднего была из двух пустышек).
MIN_REAL_PLAYERS = 5
MIN_REAL_SHOTS = 15

MIN_BASE_GAMES = 3        # меньше — сравнивать не с чем
MIN_REL = 0.25            # отклонение меньше четверти считаем шумом
MIN_ABS = {"pts": 8, "reb": 5, "reb_off": 3, "reb_def": 4, "ast": 3,
           "stl": 3, "blk": 2, "tur": 3, "pf": 4}
MIN_ABS_PCT = 0.10        # проценты попадания: сдвиг меньше 10 п.п. — шум


def _totals(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for key in [m[0] for m in METRICS] + ["fgm", "fga", "tpm", "tpa", "ftm", "fta"]:
        out[key] = sum(float(r.get(key) or 0) for r in rows)
    return out


def _pct(made: float, att: float) -> Optional[float]:
    return round(made / att, 3) if att else None


def team_games(team_id: str, source: str, limit: int = 12) -> List[Dict[str, Any]]:
    """Последние игры команды: сумма по своим игрокам + счёт и четверти."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        games = conn.execute(
            """SELECT game_id, game_date FROM game_player_stats
               WHERE team_id = ? AND source = ? AND game_date != ''
               GROUP BY game_id ORDER BY game_date DESC LIMIT ?""",
            (str(team_id), source, limit)).fetchall()
        out = []
        for g in games:
            ours = [dict(r) for r in conn.execute(
                """SELECT * FROM game_player_stats
                   WHERE source = ? AND game_id = ? AND team_id = ?""",
                (source, g["game_id"], str(team_id)))]
            theirs = [dict(r) for r in conn.execute(
                """SELECT * FROM game_player_stats
                   WHERE source = ? AND game_id = ? AND team_id != ?""",
                (source, g["game_id"], str(team_id)))]
            meta = conn.execute("SELECT * FROM game_meta WHERE source = ? AND game_id = ?",
                                (source, g["game_id"])).fetchone()
            out.append({"game_id": g["game_id"], "date": g["game_date"],
                        "source": source, "team_id": str(team_id),
                        "us": _totals(ours), "them": _totals(theirs),
                        "players": ours, "meta": dict(meta) if meta else {}})
    return out


def is_real_game(game: Dict[str, Any]) -> bool:
    """Похоже ли это на полноценный протокол (см. MIN_REAL_*)."""
    return (len(game.get("players") or []) >= MIN_REAL_PLAYERS
            and game["us"].get("fga", 0) >= MIN_REAL_SHOTS)


def opponent(game: Dict[str, Any]) -> Dict[str, Any]:
    """Кто был соперником: id, название, наш ли это был домашний матч."""
    meta = game.get("meta") or {}
    home = str(meta.get("home_team_id")) == str(game["team_id"])
    return {
        "team_id": str(meta.get("guest_team_id") if home else meta.get("home_team_id") or ""),
        "name": (meta.get("guest_name") if home else meta.get("home_name")) or "",
        "we_home": home,
        "our_score": meta.get("home_score") if home else meta.get("guest_score"),
        "their_score": meta.get("guest_score") if home else meta.get("home_score"),
    }


def history_with(team_id: str, source: str, opp_id: str,
                 exclude_game: str = "", limit: int = 5) -> List[Dict[str, Any]]:
    """Прошлые встречи с этим же соперником — от свежих к старым.

    Тренеру важнее не «мы стали лучше вообще», а «мы стали лучше против НИХ»:
    соперники разной силы, и средняя по всем играм тут врёт."""
    if not opp_id:
        return []
    sheets_cache.init_db()
    out: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM game_meta
               WHERE source = ? AND game_id != ?
                 AND ((home_team_id = ? AND guest_team_id = ?)
                   OR (home_team_id = ? AND guest_team_id = ?))
               ORDER BY game_date DESC LIMIT ?""",
            (source, str(exclude_game), str(team_id), str(opp_id),
             str(opp_id), str(team_id), limit)).fetchall()
        for r in rows:
            m = dict(r)
            home = str(m.get("home_team_id")) == str(team_id)
            ours = m.get("home_score") if home else m.get("guest_score")
            theirs = m.get("guest_score") if home else m.get("home_score")
            out.append({"date": m.get("game_date", ""), "our_score": ours,
                        "their_score": theirs, "diff": (ours or 0) - (theirs or 0),
                        "game_id": m.get("game_id")})
    return out


def lineup(game: Dict[str, Any], names: Dict[str, str]) -> Dict[str, Any]:
    """Кто вышел на площадку и кто из них тащил.

    «Играл» — есть в протоколе с ненулевым временем или хоть каким-то
    действием: в заявке бывают те, кто просидел всю игру, и записывать их в
    состав нечестно."""
    played, bench = [], []
    for p in game.get("players") or []:
        active = (p.get("secs") or 0) > 0 or any(
            (p.get(k) or 0) for k in ("pts", "reb", "ast", "stl", "blk", "tur", "pf", "fga"))
        name = names.get(str(p["player_id"])) or f"№{p.get('number') or p['player_id']}"
        (played if active else bench).append(name)
    return {"played": sorted(played), "bench": sorted(bench)}


def _baseline(games: List[Dict[str, Any]], side: str = "us") -> Dict[str, float]:
    """Среднее по играм БАЗЫ (без разбираемой игры)."""
    if not games:
        return {}
    base: Dict[str, float] = {}
    keys = list(games[0][side].keys())
    for k in keys:
        base[k] = sum(g[side].get(k, 0) for g in games) / len(games)
    return base


def _deviations(game: Dict[str, Any], base: Dict[str, float]) -> List[Dict[str, Any]]:
    """Отклонения игры от базы — только заметные, отсортированы по величине."""
    out: List[Dict[str, Any]] = []
    for key, title, more_better in METRICS:
        now, avg = game["us"].get(key, 0), base.get(key, 0)
        if avg <= 0:
            continue
        diff = now - avg
        rel = abs(diff) / avg
        if rel < MIN_REL or abs(diff) < MIN_ABS.get(key, 3):
            continue
        good = (diff > 0) == more_better
        out.append({"kind": "count", "key": key, "title": title, "now": round(now, 1),
                    "avg": round(avg, 1), "diff": round(diff, 1), "rel": rel, "good": good})

    for key, title, _f in SHOOTING:
        m_key = {"fg": ("fgm", "fga"), "tp": ("tpm", "tpa"), "ft": ("ftm", "fta")}[key]
        now = _pct(game["us"].get(m_key[0], 0), game["us"].get(m_key[1], 0))
        avg = _pct(base.get(m_key[0], 0), base.get(m_key[1], 0))
        if now is None or avg is None:
            continue
        diff = now - avg
        if abs(diff) < MIN_ABS_PCT:
            continue
        out.append({"kind": "pct", "key": key, "title": title,
                    "now": now, "avg": avg, "diff": diff, "rel": abs(diff) / max(avg, 0.01),
                    "good": diff > 0,
                    "shots": (game["us"].get(m_key[0], 0), game["us"].get(m_key[1], 0))})
    out.sort(key=lambda d: d["rel"], reverse=True)
    return out


def _culprits(key: str, game: Dict[str, Any], base_games: List[Dict[str, Any]],
              direction: int, names: Dict[str, str], limit: int = 2) -> List[str]:
    """Кто сильнее прочих отклонился от СВОЕГО среднего по этому показателю.

    direction — куда двинулась команда: +1 (стало больше) или −1 (меньше).
    Ищем тех, кто двинулся ТУДА ЖЕ, а не «кто сыграл хуже»: у потерь и фолов
    «хуже» значит больше, у подборов — меньше, и путать это нельзя (иначе в
    строке про лишние потери оказываются те, кто вообще не терял мяч).

    Сравниваем игрока с ним самим: у одного 8 подборов — провал, у другого
    рекорд. Среднее считается по играм, где человек играл, иначе пропустивший
    месяц выглядел бы виноватым."""
    per_player: Dict[str, List[float]] = {}
    for g in base_games:
        for p in g["players"]:
            per_player.setdefault(str(p["player_id"]), []).append(float(p.get(key) or 0))
    out: List[Tuple[float, str]] = []
    for p in game["players"]:
        pid = str(p["player_id"])
        hist = per_player.get(pid) or []
        if len(hist) < 2:
            continue
        avg = sum(hist) / len(hist)
        now = float(p.get(key) or 0)
        delta = now - avg
        if delta * direction <= 0.5:
            continue                     # двинулся не туда или почти не двинулся
        name = names.get(pid) or f"№{p.get('number') or pid}"
        out.append((delta * direction, f"{name} {now:g} (обычно {avg:.1f})"))
    out.sort(key=lambda x: x[0], reverse=True)
    return [text for _d, text in out[:limit]]


def _quarters(game: Dict[str, Any]) -> Optional[str]:
    """Худшая четверть матча — она у тренера первый вопрос к команде."""
    try:
        qs = json.loads(game["meta"].get("quarters_json") or "[]")
    except (json.JSONDecodeError, TypeError):
        return None
    if not qs or len(qs) < 2:
        return None
    meta = game["meta"]
    home = str(meta.get("home_team_id")) == game["team_id"]
    diffs = [(q[0] - q[1]) if home else (q[1] - q[0]) for q in qs if len(q) >= 2]
    if not diffs:
        return None
    worst = min(range(len(diffs)), key=lambda i: diffs[i])
    q = qs[worst]
    ours, theirs = (q[0], q[1]) if home else (q[1], q[0])
    if diffs[worst] >= -4:
        return None                      # ровный матч по четвертям — не о чем
    return f"{worst + 1}-я четверть: {ours}:{theirs} ({diffs[worst]:+d})"


def game_report(team_id: str, source: str, names: Optional[Dict[str, str]] = None,
                base_limit: int = 8) -> Dict[str, Any]:
    """Разбор последней игры команды против её же среднего."""
    games = [g for g in team_games(team_id, source, limit=(base_limit + 1) * 2)
             if is_real_game(g)]
    if not games:
        return {"ok": False, "reason": "нет игр"}
    game, base_games = games[0], games[1:base_limit + 1]
    if len(base_games) < MIN_BASE_GAMES:
        return {"ok": False, "reason": f"мало игр для сравнения ({len(base_games)})"}

    base = _baseline(base_games)
    devs = _deviations(game, base)
    names = names or {}
    opp = opponent(game)
    hist = history_with(str(team_id), source, opp["team_id"], exclude_game=game["game_id"])
    for d in devs:
        if d["kind"] == "count":
            d["who"] = _culprits(d["key"], game, base_games,
                                 direction=1 if d["diff"] > 0 else -1, names=names)
        else:
            d["who"] = []
    return {"ok": True, "date": game["date"], "source": source, "team_id": str(team_id),
            "meta": game["meta"], "us": game["us"], "them": game["them"],
            "base_games": len(base_games), "deviations": devs,
            "quarter": _quarters(game), "opponent": opp, "history": hist,
            "lineup": lineup(game, names)}


def format_report(rep: Dict[str, Any], team_title: str = "") -> str:
    """Текст для Telegram. Коротко: тренеру нужны 3–5 строк, а не простыня."""
    if not rep.get("ok"):
        return f"📈 Прогресс команды\n\nПока не могу разобрать: {rep.get('reason')}."
    opp = rep.get("opponent") or {}
    d = rep["date"]
    score = ""
    if opp.get("our_score") is not None:
        score = f" · {opp['our_score']}:{opp['their_score']}"
    vs = f" против «{opp['name']}»" if opp.get("name") else ""
    where = "дома" if opp.get("we_home") else "в гостях"
    head = (f"📈 {team_title or 'Команда'} · игра {d[8:10]}.{d[5:7]}{score}\n"
            f"{vs.strip() or 'Соперник неизвестен'} · {where}")

    bad = [x for x in rep["deviations"] if not x["good"]][:3]
    good = [x for x in rep["deviations"] if x["good"]][:2]

    def line(x: Dict[str, Any]) -> str:
        if x["kind"] == "pct":
            made, att = x.get("shots", (0, 0))
            body = (f"• {x['title']}: {int(made)}/{int(att)} = {x['now']*100:.0f}% "
                    f"(обычно {x['avg']*100:.0f}%)")
        else:
            body = (f"• {x['title']}: {x['now']:g} против {x['avg']:g} в среднем "
                    f"({x['diff']:+g})")
        if x.get("who"):
            body += "\n   " + "; ".join(x["who"])
        return body

    parts = [head, ""]
    if rep.get("quarter"):
        parts += [f"⏱ {rep['quarter']}", ""]
    if bad:
        parts.append("Просело против нашего среднего:")
        parts += [line(x) for x in bad]
    if good:
        parts += ["", "Вышло лучше обычного:"] if bad else ["Вышло лучше обычного:"]
        parts += [line(x) for x in good]
    if not bad and not good:
        parts.append("Игра прошла ровно по нашим средним — ярких отклонений нет.")
    # История именно с ЭТИМ соперником: «стали ли мы лучше вообще» тренеру
    # менее интересно, чем «стали ли мы лучше против них».
    hist = rep.get("history") or []
    if hist:
        parts += ["", f"Прошлые встречи с «{opp.get('name') or 'ними'}»:"]
        for h in hist:
            hd = h["date"]
            parts.append(f"• {hd[8:10]}.{hd[5:7]} — {h['our_score']}:{h['their_score']} "
                         f"({h['diff']:+d})")
        now_diff = (opp.get("our_score") or 0) - (opp.get("their_score") or 0)
        was = hist[0]["diff"]
        delta = now_diff - was
        if delta > 0:
            parts.append(f"Сейчас {now_diff:+d} — на {delta} лучше прошлой встречи.")
        elif delta < 0:
            parts.append(f"Сейчас {now_diff:+d} — на {abs(delta)} хуже прошлой встречи.")
        else:
            parts.append(f"Сейчас {now_diff:+d} — ровно как в прошлый раз.")

    line_up = rep.get("lineup") or {}
    if line_up.get("played"):
        parts += ["", f"Играли ({len(line_up['played'])}): " + ", ".join(line_up["played"])]
    if line_up.get("bench"):
        parts.append(f"В заявке, но без игрового времени: " + ", ".join(line_up["bench"]))

    parts += ["", f"Сравнение с последними {rep['base_games']} играми этой команды."]
    return "\n".join(parts)


# ── Данные для подробного отчёта (сезоны, соперники, тренды) ───────────────
#
# Тут всё считается ОДНИМ запросом на сезон, без загрузки состава: подробный
# отчёт смотрит десятки игр, и построчный обход тормозил бы на ровном месте.

SERIES_SQL = """
    SELECT s.game_id, s.game_date, s.season_id,
           SUM(s.pts) pts, SUM(s.reb) reb, SUM(s.reb_off) reb_off,
           SUM(s.reb_def) reb_def, SUM(s.ast) ast, SUM(s.stl) stl, SUM(s.blk) blk,
           SUM(s.tur) tur, SUM(s.pf) pf, SUM(s.foul_on) foul_on,
           SUM(s.fgm) fgm, SUM(s.fga) fga, SUM(s.tpm) tpm, SUM(s.tpa) tpa,
           SUM(s.ftm) ftm, SUM(s.fta) fta, COUNT(*) players
    FROM game_player_stats s
    WHERE s.team_id = ? AND s.source = ? AND s.game_date != ''
    GROUP BY s.game_id
"""


def season_series(team_id: str, source: str) -> List[Dict[str, Any]]:
    """Все игры команды: наши суммы + счёт и соперник. От старых к новым."""
    sheets_cache.init_db()
    out: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        metas = {r["game_id"]: dict(r) for r in conn.execute(
            "SELECT * FROM game_meta WHERE source = ?", (source,))}
        for r in conn.execute(SERIES_SQL, (str(team_id), source)):
            g = dict(r)
            m = metas.get(g["game_id"], {})
            home = str(m.get("home_team_id")) == str(team_id)
            g.update({
                "opp_id": str((m.get("guest_team_id") if home else m.get("home_team_id")) or ""),
                "opp_name": (m.get("guest_name") if home else m.get("home_name")) or "",
                "our_score": m.get("home_score") if home else m.get("guest_score"),
                "their_score": m.get("guest_score") if home else m.get("home_score"),
                "home": home,
                "quarters_json": m.get("quarters_json") or "",
            })
            g["diff"] = (g["our_score"] or 0) - (g["their_score"] or 0)
            g["win"] = g["diff"] > 0
            out.append(g)
    out.sort(key=lambda x: x["game_date"])
    return [g for g in out if g["players"] >= MIN_REAL_PLAYERS and g["fga"] >= MIN_REAL_SHOTS]


def opponents_series(team_id: str, source: str,
                     game_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """Суммы СОПЕРНИКА в каждой нашей игре.

    Это честная база для сравнения «мы против лиги»: соперники в протоколе
    наши же, значит считаем по тем самым играм, а не по чужим турнирам, где
    другой темп и другой уровень."""
    if not game_ids:
        return {}
    sheets_cache.init_db()
    marks = ",".join("?" * len(game_ids))
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(f"""
            SELECT game_id, SUM(pts) pts, SUM(reb) reb, SUM(reb_off) reb_off,
                   SUM(reb_def) reb_def, SUM(ast) ast, SUM(stl) stl, SUM(blk) blk,
                   SUM(tur) tur, SUM(pf) pf, SUM(fgm) fgm, SUM(fga) fga,
                   SUM(tpm) tpm, SUM(tpa) tpa, SUM(ftm) ftm, SUM(fta) fta
            FROM game_player_stats
            WHERE source = ? AND team_id != ? AND game_id IN ({marks})
            GROUP BY game_id""", [source, str(team_id)] + list(game_ids)).fetchall()
    return {r["game_id"]: dict(r) for r in rows}


def roster_stats(team_id: str, source: str, series: List[Dict[str, Any]],
                 names: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
    """Разбор по составу: кто сколько играл и что принёс.

    Считаем только по играм ЭТОГО сезона (game_ids из series), иначе новичок и
    ветеран сравнивались бы на разных отрезках."""
    ids = [g["game_id"] for g in series]
    if not ids:
        return []
    import fantasy_stats
    sheets_cache.init_db()
    marks = ",".join("?" * len(ids))
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(f"""
            SELECT player_id, number, COUNT(*) games, SUM(secs) secs,
                   SUM(pts) pts, SUM(reb) reb, SUM(reb_off) reb_off, SUM(ast) ast,
                   SUM(stl) stl, SUM(blk) blk, SUM(tur) tur, SUM(pf) pf,
                   SUM(foul_on) foul_on, SUM(fgm) fgm, SUM(fga) fga,
                   SUM(tpm) tpm, SUM(tpa) tpa, SUM(ftm) ftm, SUM(fta) fta,
                   SUM(plus_minus) plus_minus
            FROM game_player_stats
            WHERE source = ? AND team_id = ? AND game_id IN ({marks})
            GROUP BY player_id""", [source, str(team_id)] + ids)]
    names = names or {}
    out = []
    for r in rows:
        n = r["games"] or 1
        fp = fantasy_stats.fantasy_points({k: (v or 0) for k, v in r.items()})
        out.append({
            "player_id": str(r["player_id"]),
            "number": str(r["number"] or ""),
            "name": names.get(str(r["player_id"])) or f"№{r['number'] or r['player_id']}",
            "games": r["games"], "mins": round((r["secs"] or 0) / 60 / n, 1),
            "pts": round((r["pts"] or 0) / n, 1), "reb": round((r["reb"] or 0) / n, 1),
            "ast": round((r["ast"] or 0) / n, 1), "stl": round((r["stl"] or 0) / n, 1),
            "blk": round((r["blk"] or 0) / n, 1), "tur": round((r["tur"] or 0) / n, 1),
            "pf": round((r["pf"] or 0) / n, 1),
            "fg": _pct(r["fgm"] or 0, r["fga"] or 0),
            "plus_minus": round((r["plus_minus"] or 0) / n, 1),
            "fp": round(fp / n, 1),
        })
    out.sort(key=lambda x: x["fp"], reverse=True)
    return out


# ── Лидеры по показателям: наши и соперника ────────────────────────────────

LEADER_METRICS: Tuple[Tuple[str, str], ...] = (
    ("pts", "Очки"),
    ("reb", "Подборы"),
    ("ast", "Передачи"),
    ("stl", "Перехваты"),
    ("blk", "Блок-шоты"),
    ("fp", "Общий вклад"),
)


def leaders(roster: List[Dict[str, Any]], games_total: int = 0) -> List[Dict[str, Any]]:
    """По каждому показателю — первый номер команды (в среднем за игру).

    Случайных людей в лидеры не пускаем: 20 очков в единственном матче — это
    не «лучший бомбардир», а один вечер. Порог — треть игр команды; если под
    него не проходит никто (команда только начала сезон), считаем по всем."""
    if not roster:
        return []
    total = games_total or max((r.get("games") or 0) for r in roster)
    need = max(1, round(total / 3))
    pool = [r for r in roster if (r.get("games") or 0) >= need] or roster
    out: List[Dict[str, Any]] = []
    for key, title in LEADER_METRICS:
        best = max(pool, key=lambda r: r.get(key) or 0)
        if not (best.get(key) or 0):
            continue
        out.append({"key": key, "title": title, "name": best["name"],
                    "number": best.get("number", ""), "value": best[key],
                    "games": best["games"]})
    return out


def last_opponent(team_id: str, source: str) -> Dict[str, Any]:
    """Кто соперник последней игры — чтобы вызывающий успел сходить в лигу за
    их заявкой ДО сборки отчёта (сам модуль в сеть не ходит)."""
    series = season_series(team_id, source)
    if not series:
        return {}
    g = series[-1]
    return {"team_id": g["opp_id"], "name": g["opp_name"],
            "season_id": str(g.get("season_id") or "")}


# ── Тренировки: явка за последний месяц ────────────────────────────────────

TRAINING_WINDOW_DAYS = 30


def training_attendance(days: int = TRAINING_WINDOW_DAYS,
                        today: Optional[str] = None) -> Dict[str, Any]:
    """Кто сколько раз был на тренировке за последние `days` дней.

    Считаем ровно по тем же правилам, что и лист «Тренировки»
    (attendance_summary): один опрос может закрывать две тренировки («Среда и
    пятница»), поэтому знаменатель — предложенные ДНИ, а не число опросов.
    Свой упрощённый подсчёт разъехался бы с отчётом, который тренер уже
    читает, и одному и тому же человеку показывал бы разные цифры."""
    from datetime import date, timedelta
    import attendance_summary
    from report_common import load_roster, make_resolver

    sheets_cache.init_db()
    end = date.fromisoformat(today) if today else date.today()
    start = end - timedelta(days=days - 1)
    empty = {"from": start.isoformat(), "to": end.isoformat(), "days": days,
             "trainings": 0, "polls": 0, "by_key": {}, "by_name": {}}
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT user_id, username, first_name, last_name, vote_text,
                      vote_type, training_date, revote_count
               FROM attendance
               WHERE training_date >= ? AND training_date <= ?
                 AND vote_type IN ('PRESENT', 'ABSENT')""",
            (start.isoformat(), end.isoformat()))]
    if not rows:
        return empty

    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        r["revotes"] = r.pop("revote_count", 0)
        by_date.setdefault(r["training_date"], []).append(r)
    events = []
    for iso, votes in by_date.items():
        try:
            events.append((date.fromisoformat(iso), votes))
        except ValueError:
            continue
    if not events:
        return empty

    stats = attendance_summary.aggregate(events, make_resolver(load_roster()))
    # Знаменатель — тот же, что в сводке листа: сколько тренировочных ДНЕЙ
    # вообще предлагалось за период.
    trainings = sum(len(attendance_summary._offered_days(v, d)) for d, v in events)

    by_key: Dict[str, Dict[str, Any]] = {}
    for name, p in stats.items():
        by_key[p["key"]] = {
            "name": name, "nick": p["nick"], "present": p["present"],
            "absent": p["absent"], "no_answer": max(0, trainings - p["voted"]),
        }
    return {"from": start.isoformat(), "to": end.isoformat(), "days": days,
            "trainings": trainings, "polls": len(events), "by_key": by_key,
            "by_name": {_norm_name(v["name"]): v for v in by_key.values()}}


def _norm_name(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def _attendance_index(source: str) -> Dict[str, str]:
    """{id игрока в лиге -> ключ человека в сводке тренировок}.

    Идём через привязку профиля (её игрок делает сам ссылкой) — это точное
    сопоставление. Совпадение по ФИО оставляем запасным путём в
    attach_attendance: однофамильцев в команде нет, но опечатка в листе не
    должна ломать всю колонку."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        links = {str(r["tg_user_id"]): int(r["player_row"]) for r in conn.execute(
            "SELECT tg_user_id, player_row FROM player_links")}
        idents = [(str(r["player_id"]), str(r["tg_user_id"])) for r in conn.execute(
            "SELECT player_id, tg_user_id FROM player_identities WHERE source = ?",
            (source,))]
    return {pid: f"row:{links[uid]}" for pid, uid in idents if uid in links}


def attach_attendance(roster: List[Dict[str, Any]], att: Dict[str, Any],
                      source: str) -> None:
    """Дописывает каждому в составе явку на тренировках (на месте).

    Три разных случая, и их нельзя смешивать:
      • голосовал — ставим, сколько раз был;
      • есть в листе «Игроки», но ни разу не ответил — это честный 0, человек
        на тренировках не появлялся;
      • в листе его нет вовсе (играет, но в состав не внесён) — None, прочерк:
        про его тренировки мы попросту ничего не знаем.
    В `att["unmatched"]` складываем обратный случай — ходит на тренировки, но в
    протоколах не появлялся: тренеру это отдельный разговор."""
    sheet_by_name, rows_in_sheet = _sheet_index()
    idx = _attendance_index(source)
    by_key, by_name = att.get("by_key") or {}, att.get("by_name") or {}
    used = set()
    for r in roster:
        norm = _norm_name(r["name"])
        key = idx.get(r["player_id"]) or sheet_by_name.get(norm, "")
        rec = by_key.get(key) if key else None
        if not rec:
            rec = by_name.get(norm)
            if rec and not key:
                key = next((k for k, v in by_key.items()
                            if _norm_name(v["name"]) == norm), "")
        if key:
            used.add(key)
        if rec:
            r["att_present"] = rec["present"]
        else:
            r["att_present"] = 0 if key in rows_in_sheet else None
        r["att_total"] = att.get("trainings") or 0
    # Только те, кто есть в листе «Игроки» (row:) — посторонние из чата в отчёт
    # о составе не попадают. И только реально ходившие: «ноль тренировок и ноль
    # игр» — это просто неактивный человек, отдельной строки он не стоит.
    att["unmatched"] = sorted(
        (v for k, v in by_key.items()
         if k.startswith("row:") and k not in used and v["present"] > 0),
        key=lambda v: -v["present"])


def _sheet_index() -> Tuple[Dict[str, str], set]:
    """{норм. ФИО из листа «Игроки» -> ключ человека} и множество этих ключей."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT row_index, surname, name FROM players WHERE surname != '' OR name != ''")]
    by_name = {_norm_name(f"{r['surname']} {r['name']}"): f"row:{r['row_index']}"
               for r in rows}
    return by_name, set(by_name.values())


def split_progress(series: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Первая половина сезона против второй — когда прошлого сезона ещё нет.

    «Прогресса не с чем сравнить» — плохой ответ тренеру: динамика внутри
    сезона видна и по одному сезону, надо только разрезать его пополам."""
    if len(series) < 6:
        return None
    half = len(series) // 2
    first, second = series[:half], series[half:]
    return {"first": summarize(first), "second": summarize(second),
            "first_from": first[0]["game_date"], "first_to": first[-1]["game_date"],
            "second_from": second[0]["game_date"], "second_to": second[-1]["game_date"]}


def seasons_of(series: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Сезоны в порядке «свежий первым»: id, период, сколько игр."""
    by: Dict[str, List[Dict[str, Any]]] = {}
    for g in series:
        by.setdefault(str(g.get("season_id") or ""), []).append(g)
    out = [{"season_id": sid, "games": len(gs),
            "from": gs[0]["game_date"], "to": gs[-1]["game_date"], "series": gs}
           for sid, gs in by.items()]
    out.sort(key=lambda x: x["to"], reverse=True)
    return out


def summarize(series: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Средние и итоги по набору игр."""
    if not series:
        return {}
    n = len(series)
    keys = ("pts", "reb", "reb_off", "reb_def", "ast", "stl", "blk", "tur", "pf",
            "foul_on", "fgm", "fga", "tpm", "tpa", "ftm", "fta")
    avg = {k: round(sum(g.get(k, 0) for g in series) / n, 1) for k in keys}
    wins = sum(1 for g in series if g["win"])
    home = [g for g in series if g["home"]]
    away = [g for g in series if not g["home"]]
    return {
        "games": n, "wins": wins, "losses": n - wins,
        "avg": avg,
        "for": round(sum(g["our_score"] or 0 for g in series) / n, 1),
        "against": round(sum(g["their_score"] or 0 for g in series) / n, 1),
        "diff": round(sum(g["diff"] for g in series) / n, 1),
        "fg_pct": _pct(avg["fgm"], avg["fga"]),
        "tp_pct": _pct(avg["tpm"], avg["tpa"]),
        "ft_pct": _pct(avg["ftm"], avg["fta"]),
        "home": {"games": len(home), "wins": sum(1 for g in home if g["win"])},
        "away": {"games": len(away), "wins": sum(1 for g in away if g["win"])},
        "best": max(series, key=lambda g: g["diff"]),
        "worst": min(series, key=lambda g: g["diff"]),
    }


def head_to_head(series: List[Dict[str, Any]], opp_id: str) -> List[Dict[str, Any]]:
    return [g for g in series if opp_id and g["opp_id"] == str(opp_id)]


def insights(cur: Dict[str, Any], prev: Optional[Dict[str, Any]],
             series: List[Dict[str, Any]],
             opp_avg: Optional[Dict[str, float]] = None) -> List[str]:
    """Выводы словами. Не пересказ таблицы, а то, что видно только в сравнении."""
    out: List[str] = []
    if not cur:
        return out
    out.append(f"Баланс {cur['wins']}–{cur['losses']}, разница {cur['diff']:+.1f} за игру "
               f"({cur['for']} забиваем, {cur['against']} пропускаем).")
    h, a = cur["home"], cur["away"]
    # Минимум по три игры с каждой стороны: на одной домашней «0 из 1» — это
    # не вывод, а совпадение, и тренеру такое показывать нельзя.
    if h["games"] >= 3 and a["games"] >= 3:
        hp, ap = h["wins"] / h["games"], a["wins"] / a["games"]
        if abs(hp - ap) >= 0.25:
            where = "дома" if hp > ap else "в гостях"
            out.append(f"Заметно сильнее {where}: {h['wins']} из {h['games']} дома против "
                       f"{a['wins']} из {a['games']} на выезде.")
    if cur.get("fg_pct") is not None and cur["fg_pct"] < 0.4:
        out.append(f"Реализация с игры {cur['fg_pct']*100:.0f}% — низкая; "
                   f"броски есть ({cur['avg']['fga']} за игру), точности нет.")
    if cur["avg"]["tur"] >= 15:
        out.append(f"Потери: {cur['avg']['tur']} за игру — это отдельная тренировка.")
    if cur["avg"]["reb_off"] and cur["avg"]["reb"]:
        share = cur["avg"]["reb_off"] / cur["avg"]["reb"]
        if share >= 0.35:
            out.append(f"Активны на чужом щите: {cur['avg']['reb_off']} подборов в атаке "
                       f"({share*100:.0f}% всех).")
    if prev:
        d_diff = cur["diff"] - prev["diff"]
        if abs(d_diff) >= 3:
            better = "лучше" if d_diff > 0 else "хуже"
            out.append(f"Против прошлого сезона разница на {abs(d_diff):.1f} {better} "
                       f"({prev['diff']:+.1f} → {cur['diff']:+.1f}).")
        for key, title in (("tur", "потери"), ("reb", "подборы"), ("ast", "передачи")):
            was, now = prev["avg"].get(key, 0), cur["avg"].get(key, 0)
            if was and abs(now - was) / was >= 0.2:
                sign = "выросли" if now > was else "упали"
                out.append(f"Сезон к сезону {title} {sign}: {was} → {now} за игру.")
    # Сравнение с соперниками — не с абстрактной нормой, а с теми, против кого
    # реально играли: их протоколы лежат в тех же играх.
    if opp_avg:
        for key, title, more_better in (("reb", "подборам", True),
                                        ("ast", "передачам", True),
                                        ("tur", "потерям", False)):
            ours, theirs = cur["avg"].get(key, 0), opp_avg.get(key, 0)
            if not theirs:
                continue
            d = ours - theirs
            if abs(d) / theirs >= 0.15:
                better = (d > 0) == more_better
                out.append(f"По {title} {'выигрываем' if better else 'проигрываем'} "
                           f"соперникам: {ours} против {theirs} за игру.")
        our_fg = _pct(cur["avg"].get("fgm", 0), cur["avg"].get("fga", 0))
        opp_fg = _pct(opp_avg.get("fgm", 0), opp_avg.get("fga", 0))
        if our_fg and opp_fg and abs(our_fg - opp_fg) >= 0.04:
            out.append(f"Реализация: у нас {our_fg*100:.0f}%, у соперников "
                       f"{opp_fg*100:.0f}% — {'лучше' if our_fg > opp_fg else 'хуже'}.")
    if len(series) >= 6:
        last3 = series[-3:]
        prev3 = series[-6:-3]
        d = (sum(g["diff"] for g in last3) / 3) - (sum(g["diff"] for g in prev3) / 3)
        if abs(d) >= 5:
            out.append(f"Форма: последние три игры на {abs(d):.0f} очков "
                       f"{'лучше' if d > 0 else 'хуже'} трёх предыдущих.")
    return out


def detailed_report(team_id: str, source: str, team_title: str = "",
                    names: Optional[Dict[str, str]] = None,
                    standings: Optional[List[Dict[str, Any]]] = None,
                    team_names: Optional[Dict[str, str]] = None,
                    opp_names: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Всё для подробного отчёта: сезон, динамика, лига, состав, соперник.

    standings/team_names приходят из лиги (их берёт вызывающий — модуль без
    сети). Названия команд оттуда же: в нашей базе они появляются только у
    перекачанных игр, а таблица лиги знает их всегда."""
    from datetime import datetime

    series = season_series(team_id, source)
    if not series:
        return {"ok": False, "reason": "нет игр"}
    team_names = {str(k): v for k, v in (team_names or {}).items()}
    for g in series:                       # дозаполняем имена соперников из лиги
        if not g["opp_name"] and g["opp_id"] in team_names:
            g["opp_name"] = team_names[g["opp_id"]]

    seasons = seasons_of(series)
    cur_season = seasons[0]
    prev_season = seasons[1] if len(seasons) > 1 else None
    cur = summarize(cur_season["series"])
    prev = summarize(prev_season["series"]) if prev_season else None
    last = series[-1]
    opp = {"team_id": last["opp_id"], "name": last["opp_name"]}

    # Соперники в НАШИХ играх — база для «мы против лиги».
    opps = opponents_series(team_id, source, [g["game_id"] for g in cur_season["series"]])
    opp_avg = {}
    if opps:
        n = len(opps)
        keys = ("pts", "reb", "reb_off", "reb_def", "ast", "stl", "blk", "tur", "pf",
                "fgm", "fga", "tpm", "tpa", "ftm", "fta")
        opp_avg = {k: round(sum(v.get(k, 0) or 0 for v in opps.values()) / n, 1)
                   for k in keys}

    # Лидеры соперника — по ИХ сезону, а не только по матчам с нами: тренеру
    # нужно знать, кого держать, а одна очная встреча про это не говорит.
    h2h = head_to_head(series, opp["team_id"])
    our_roster = roster_stats(team_id, source, cur_season["series"], names)
    opp_names = {str(k): v for k, v in (opp_names or {}).items()}
    opp_season = [g for g in season_series(opp["team_id"], source)
                  if str(g.get("season_id") or "") == str(cur_season["season_id"])
                  ] if opp["team_id"] else []
    lead = {
        "us": leaders(our_roster, len(cur_season["series"])),
        "them": leaders(roster_stats(opp["team_id"], source, opp_season, opp_names),
                        len(opp_season)) if opp_season else [],
        "us_games": len(cur_season["series"]), "them_games": len(opp_season),
    }
    h2h_lead = {}
    if h2h:
        h2h_lead = {
            "us": leaders(roster_stats(team_id, source, h2h, names), len(h2h)),
            "them": leaders(roster_stats(opp["team_id"], source, h2h, opp_names),
                            len(h2h)),
            "games": len(h2h),
        }

    # Тренировки: посещаемость за последний месяц рядом с игровой статистикой.
    # Игровой спад и пропуски тренировок разговор про них ведут вместе.
    att = training_attendance()
    attach_attendance(our_roster, att, source)

    return {
        "ok": True,
        "team_title": team_title or f"{source}:{team_id}",
        "generated": datetime.now().strftime("%d.%m.%Y %H:%M"),
        "series": cur_season["series"],
        "season": cur,
        "prev_season": prev,
        "split": split_progress(cur_season["series"]) if not prev else None,
        "last_game": last,
        "opponent": opp,
        "head_to_head": h2h,
        "roster": our_roster,
        "leaders": lead,
        "h2h_leaders": h2h_lead,
        "attendance": att,
        "standings": standings or [],
        "our_team_id": str(team_id),
        "opp_avg": opp_avg,
        "insights": insights(cur, prev, cur_season["series"], opp_avg),
    }


def short_summary(rep: Dict[str, Any], detail: Dict[str, Any]) -> str:
    """Пять строк в чат: остальное — во вложении.

    Сообщение читают на бегу, поэтому тут только результат, одна главная
    просадка и одна главная сила. Всё, что требует таблицы, — в файле."""
    if not rep.get("ok"):
        return f"📈 Прогресс команды\n\nПока не могу разобрать: {rep.get('reason')}."
    opp = rep.get("opponent") or {}
    d = rep["date"]
    head = f"📈 {detail.get('team_title', 'Команда')} · {d[8:10]}.{d[5:7]}"
    if opp.get("our_score") is not None:
        head += f" · {opp['our_score']}:{opp['their_score']}"
    if opp.get("name"):
        head += f" ({opp['name']})"
    bad = [x for x in rep["deviations"] if not x["good"]]
    good = [x for x in rep["deviations"] if x["good"]]

    def one(x: Dict[str, Any]) -> str:
        if x["kind"] == "pct":
            return f"{x['title']} {x['now']*100:.0f}% (обычно {x['avg']*100:.0f}%)"
        return f"{x['title']} {x['now']:g} против {x['avg']:g}"

    lines = [head, ""]
    if bad:
        lines.append(f"↓ {one(bad[0])}")
    if good:
        lines.append(f"↑ {one(good[0])}")
    if not bad and not good:
        lines.append("Игра прошла ровно по нашим средним.")
    season = detail.get("season") or {}
    if season:
        lines.append("")
        lines.append(f"Сезон: {season['wins']}–{season['losses']}, "
                     f"разница {season['diff']:+.1f} за игру.")
    lines.append("")
    lines.append("Подробный разбор — во вложении: сезон, состав, таблица лиги, "
                 "динамика и промт для ИИ.")
    return "\n".join(lines)

