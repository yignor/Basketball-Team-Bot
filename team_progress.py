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
              worse: bool, names: Dict[str, str], limit: int = 2) -> List[str]:
    """Кто сильнее прочих отклонился от СВОЕГО среднего по этому показателю.

    Сравниваем игрока с ним самим: у одного 8 подборов — провал, у другого
    рекорд. Средний считается только по играм, где человек играл, иначе
    пропустивший месяц выглядел бы виноватым."""
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
        if (worse and delta >= -0.5) or (not worse and delta <= 0.5):
            continue
        name = names.get(pid) or f"№{p.get('number') or pid}"
        out.append((delta, f"{name} {now:g} (обычно {avg:.1f})"))
    out.sort(key=lambda x: x[0], reverse=not worse)
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
    games = team_games(team_id, source, limit=base_limit + 1)
    if not games:
        return {"ok": False, "reason": "нет игр"}
    game, base_games = games[0], games[1:]
    if len(base_games) < MIN_BASE_GAMES:
        return {"ok": False, "reason": f"мало игр для сравнения ({len(base_games)})"}

    base = _baseline(base_games)
    devs = _deviations(game, base)
    names = names or {}
    for d in devs:
        if d["kind"] == "count":
            d["who"] = _culprits(d["key"], game, base_games, worse=not d["good"], names=names)
        else:
            d["who"] = []
    return {"ok": True, "date": game["date"], "source": source, "team_id": str(team_id),
            "meta": game["meta"], "us": game["us"], "them": game["them"],
            "base_games": len(base_games), "deviations": devs,
            "quarter": _quarters(game)}


def format_report(rep: Dict[str, Any], team_title: str = "") -> str:
    """Текст для Telegram. Коротко: тренеру нужны 3–5 строк, а не простыня."""
    if not rep.get("ok"):
        return f"📈 Прогресс команды\n\nПока не могу разобрать: {rep.get('reason')}."
    meta = rep.get("meta") or {}
    score = ""
    if meta:
        home = str(meta.get("home_team_id")) == rep["team_id"]
        ours = meta.get("home_score") if home else meta.get("guest_score")
        theirs = meta.get("guest_score") if home else meta.get("home_score")
        score = f" · {ours}:{theirs}"
    d = rep["date"]
    head = f"📈 {team_title or 'Команда'} · игра {d[8:10]}.{d[5:7]}{score}"

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
    parts += ["", f"Сравнение с последними {rep['base_games']} играми этой команды."]
    return "\n".join(parts)
