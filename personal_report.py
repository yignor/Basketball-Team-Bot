#!/usr/bin/env python3
"""
Личный отчёт игрока: «объёмная» картина вместо средних.

Средние сами по себе игроку ничего не говорят: 8 очков за игру — это много или
мало? Смысл появляется при сравнении с собой же в другом периоде. Поэтому здесь
считается не «сколько», а «что изменилось и насколько»: форма (последние игры)
против эталонного периода на выбор.

Показываем только заметные сдвиги. Если высыпать все метрики, отчёт читают один
раз, а потом перестают — сигнал тонет в шуме.

Идентификация — по числовому id лиги из привязки ([[personal-stats-identity]]),
ФИО нигде не хранится. Отчёт уходит ТОЛЬКО в личку.
"""

from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache
import fantasy_stats

# Каталог показателей: ключ в БД -> (название, «больше — лучше»). Игрок сам
# выбирает, что отслеживать: одному важны подборы, другому фолы, и общий набор
# для всех превращает отчёт в простыню.
ALL_METRICS: List[Tuple[str, str, bool]] = [
    ("pts", "очки", True),
    ("reb", "подборы", True),
    ("reb_off", "подборы в атаке", True),
    ("reb_def", "подборы в защите", True),
    ("ast", "передачи", True),
    ("stl", "перехваты", True),
    ("blk", "блок-шоты", True),
    ("tur", "потери", False),
    ("pf", "фолы", False),
    # КПИ не храним — считаем формулой, она одинакова для обеих лиг.
    ("kpi", "КПИ", True),
    # Плюс-минус отдаёт только Инфобаскет; у SLPRO его нет в протоколе.
    ("plus_minus", "плюс-минус", True),
    ("mins", "минуты", True),
]
DEFAULT_METRICS = ["pts", "reb", "ast", "stl", "blk", "tur"]
METRIC_TITLES = {k: t for k, t, _ in ALL_METRICS}


def metrics_of(prefs: Optional[Dict[str, Any]] = None) -> List[Tuple[str, str, bool]]:
    """Показатели, выбранные игроком (или набор по умолчанию)."""
    chosen = (prefs or {}).get("metrics") or ""
    keys = [k for k in chosen.split(",") if k] or DEFAULT_METRICS
    return [(k, t, hb) for k, t, hb in ALL_METRICS if k in keys]

# Режимы эталонного периода (с чем сравниваем текущую форму).
COMPARE_MODES = {
    "all": "за всё время",
    "season": "в этом сезоне",
    "prev_season": "в прошлом сезоне",
    "since": "с выбранной даты",
}

DEFAULT_FORM_GAMES = 5

# Порог заметности. Меньше — статистический шум: при 2 подборах за игру разница
# «было 2, стало 3» это +50%, но говорить о прогрессе рано.
MIN_REL_CHANGE = 0.15
MIN_ABS_CHANGE = 0.5


def _games(source: str, player_id: str) -> List[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            """SELECT * FROM game_player_stats
               WHERE source = ? AND player_id = ? AND game_date != ''
               ORDER BY game_date""", (source, str(player_id)))]


def seasons_of(rows: List[Dict[str, Any]]) -> List[str]:
    """Сезоны игрока от свежего к старому — по дате последней игры в каждом."""
    last: Dict[str, str] = {}
    for r in rows:
        sid = str(r.get("season_id") or "")
        if sid and r["game_date"] > last.get(sid, ""):
            last[sid] = r["game_date"]
    return [s for s, _ in sorted(last.items(), key=lambda x: x[1], reverse=True)]


def _reference(rows: List[Dict[str, Any]], mode: str,
               since: Optional[str]) -> List[Dict[str, Any]]:
    """Игры эталонного периода."""
    if mode == "season":
        seasons = seasons_of(rows)
        cur = seasons[0] if seasons else ""
        return [r for r in rows if str(r.get("season_id") or "") == cur]
    if mode == "prev_season":
        seasons = seasons_of(rows)
        prev = seasons[1] if len(seasons) > 1 else ""
        return [r for r in rows if prev and str(r.get("season_id") or "") == prev]
    if mode == "since" and since:
        return [r for r in rows if r["game_date"] >= since]
    return rows


def metric_value(row: Dict[str, Any], key: str) -> float:
    """Значение показателя за игру. Часть считается на лету, а не хранится."""
    if key == "kpi":
        # Классическая «эффективность»: полезное минус промахи и потери.
        made = float(row.get("fgm") or 0) + float(row.get("ftm") or 0)
        att = float(row.get("fga") or 0) + float(row.get("fta") or 0)
        return (float(row.get("pts") or 0) + float(row.get("reb") or 0)
                + float(row.get("ast") or 0) + float(row.get("stl") or 0)
                + float(row.get("blk") or 0) - (att - made) - float(row.get("tur") or 0))
    if key == "mins":
        return round(float(row.get("secs") or 0) / 60, 1)
    return float(row.get(key) or 0)


def _avg(rows: List[Dict[str, Any]], key: str) -> float:
    return round(sum(metric_value(r, key) for r in rows) / len(rows), 1) if rows else 0.0


def compare(source: str, player_id: str, mode: str = "all",
            since: Optional[str] = None,
            form_games: int = DEFAULT_FORM_GAMES,
            metrics: Optional[List[Tuple[str, str, bool]]] = None) -> Dict[str, Any]:
    """Форма (последние игры) против эталонного периода.

    Эталон берём БЕЗ игр формы: иначе сравниваем период сам с собой, и разница
    всегда получается меньше настоящей."""
    rows = _games(source, player_id)
    if not rows:
        return {"games": 0}

    form = rows[-form_games:]
    form_ids = {(r["source"], r["game_id"]) for r in form}
    ref = [r for r in _reference(rows, mode, since)
           if (r["source"], r["game_id"]) not in form_ids]

    changes: List[Dict[str, Any]] = []
    for key, title, higher_better in (metrics or metrics_of()):
        now, was = _avg(form, key), _avg(ref, key)
        if not ref:
            continue
        delta = round(now - was, 1)
        if abs(delta) < MIN_ABS_CHANGE:
            continue
        rel = abs(delta) / was if was else 1.0
        if rel < MIN_REL_CHANGE:
            continue
        improved = (delta > 0) == higher_better
        changes.append({"key": key, "title": title, "now": now, "was": was,
                        "delta": delta, "improved": improved,
                        "rel": round(rel * 100)})
    # Сначала самое заметное — на него и смотрит человек.
    changes.sort(key=lambda c: -c["rel"])

    best = max(form, key=lambda r: fantasy_stats.fantasy_points(r), default=None)
    return {
        "games": len(rows),
        "form_games": len(form),
        "ref_games": len(ref),
        "period_from": form[0]["game_date"] if form else "",
        "period_to": form[-1]["game_date"] if form else "",
        "changes": changes,
        "best": ({"date": best["game_date"], "pts": int(best.get("pts") or 0),
                  "reb": int(best.get("reb") or 0), "ast": int(best.get("ast") or 0),
                  "fp": fantasy_stats.fantasy_points(best)} if best else None),
        "avg_now": {k: _avg(form, k) for k, _, _ in (metrics or metrics_of())},
    }


def format_report(source_title: str, data: Dict[str, Any], mode: str) -> str:
    """Текст для личного сообщения. Без ФИО — человек и так знает, кто он."""
    if not data.get("games"):
        return f"• {source_title}: игр пока не нашёл."

    lines = [f"📈 {source_title} · форма за {data['form_games']} последних игр"]
    if data.get("period_from"):
        lines.append(f"   {_d(data['period_from'])} – {_d(data['period_to'])}")

    if not data.get("ref_games"):
        lines.append("\nСравнивать пока не с чем — нужен второй период. "
                     "Отчёт станет содержательным после следующих игр.")
        return "\n".join(lines)

    label = COMPARE_MODES.get(mode, COMPARE_MODES["all"])
    up = [c for c in data["changes"] if c["improved"]]
    down = [c for c in data["changes"] if not c["improved"]]

    if up:
        lines.append(f"\n✅ Прибавил (против того, что было {label}):")
        for c in up[:4]:
            lines.append(f"   • {c['title']}: {c['was']} → {c['now']} за игру "
                         f"({_sign(c['delta'])}, {c['rel']}%)")
    if down:
        lines.append(f"\n⚠️ Просело:")
        for c in down[:3]:
            lines.append(f"   • {c['title']}: {c['was']} → {c['now']} за игру "
                         f"({_sign(c['delta'])})")
    if not up and not down:
        lines.append(f"\n➖ Заметных изменений нет — держишь свой уровень "
                     f"{label}.")

    b = data.get("best")
    if b:
        lines.append(f"\n🏅 Лучшая игра периода {_d(b['date'])}: "
                     f"{b['pts']} очк · {b['reb']} подб · {b['ast']} пас")
    return "\n".join(lines)


def _sign(x: float) -> str:
    return f"+{x:g}" if x > 0 else f"{x:g}"


def _d(iso: str) -> str:
    try:
        d = date.fromisoformat(iso)
        return f"{d.day:02d}.{d.month:02d}"
    except (ValueError, TypeError):
        return iso


# ─────────────────────────── Личные настройки ────────────────────────────────

NOTIFY_MODES = {
    "game": "после каждой игры",
    "week": "раз в неделю",
    "month": "раз в месяц",
    "off": "не присылать",
}

DEFAULT_PREFS = {"compare_mode": "all", "compare_since": "",
                 "notify_mode": "game", "last_sent": "",
                 "metrics": ",".join(DEFAULT_METRICS)}


def get_prefs(tg_user_id: Any) -> Dict[str, Any]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM player_report_prefs WHERE tg_user_id = ?",
                           (str(tg_user_id),)).fetchone()
    return {**DEFAULT_PREFS, **(dict(row) if row else {})}


def set_pref(tg_user_id: Any, field: str, value: str) -> Dict[str, Any]:
    """Меняет одну настройку. Поле сверяем со списком — значение приходит из
    callback_data, то есть снаружи."""
    if field not in ("compare_mode", "compare_since", "notify_mode", "metrics"):
        return get_prefs(tg_user_id)
    prefs = get_prefs(tg_user_id)
    prefs[field] = value
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO player_report_prefs
               (tg_user_id, compare_mode, compare_since, notify_mode, metrics,
                last_sent, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tg_user_id) DO UPDATE SET
                   compare_mode=excluded.compare_mode,
                   compare_since=excluded.compare_since,
                   notify_mode=excluded.notify_mode,
                   metrics=excluded.metrics,
                   updated_at=excluded.updated_at""",
            (str(tg_user_id), prefs["compare_mode"], prefs["compare_since"],
             prefs["notify_mode"], prefs.get("metrics", ",".join(DEFAULT_METRICS)),
             prefs.get("last_sent", ""), sheets_cache.now_iso()))
        conn.commit()
    return prefs


def mark_sent(tg_user_id: Any, when_iso: Optional[str] = None) -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO player_report_prefs (tg_user_id, last_sent, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(tg_user_id) DO UPDATE SET
                   last_sent=excluded.last_sent, updated_at=excluded.updated_at""",
            (str(tg_user_id), when_iso or sheets_cache.now_iso(), sheets_cache.now_iso()))
        conn.commit()


def monthly_file_due(prefs: Dict[str, Any], today: Optional[date] = None) -> bool:
    """Пора ли слать месячный файл.

    Раз в месяц и только тем, кто не выключил уведомления совсем. Порог 25
    дней, а не 30: крон ходит первого числа, а месяцы разной длины — со
    строгими 30 февральская рассылка уехала бы на месяц вперёд."""
    today = today or date.today()
    if (prefs.get("notify_mode") or "game") == "off":
        return False
    last = (prefs.get("last_sent") or "")[:10]
    if not last:
        return True
    try:
        return (today - date.fromisoformat(last)).days >= 25
    except ValueError:
        return True


# ─────────────── Углублённая аналитика ───────────────────────────────────────
#
# Здесь считается то, чего не видно в средних: как игрок выглядит против
# конкретного соперника, какую долю командной работы берёт на себя и что у него
# с броском. Всё — из бокс-скоров, которые мы и так храним целиком; выдумывать
# «тренерские советы» сверх данных не будем, это были бы догадки.

def _opponent_of(conn, source: str, game_id: str, my_team: str) -> Tuple[str, bool, int, int]:
    """(id соперника, играли ли дома, мои очки команды, очки соперника)."""
    row = conn.execute(
        """SELECT home_team_id, guest_team_id, home_score, guest_score
           FROM game_meta WHERE source = ? AND game_id = ?""",
        (source, str(game_id))).fetchone()
    if not row:
        return "", False, 0, 0
    home = str(row["home_team_id"] or "")
    at_home = home == str(my_team)
    opp = str(row["guest_team_id"] or "") if at_home else home
    ours = int(row["home_score"] or 0) if at_home else int(row["guest_score"] or 0)
    theirs = int(row["guest_score"] or 0) if at_home else int(row["home_score"] or 0)
    return opp, at_home, ours, theirs


def _teammates(conn, source: str, game_id: str, my_team: str, me: str) -> set:
    """Кто ещё выходил за нашу команду в той игре — по бокс-скору."""
    return {str(r["player_id"]) for r in conn.execute(
        """SELECT player_id FROM game_player_stats
           WHERE source = ? AND game_id = ? AND team_id = ? AND player_id != ?""",
        (source, str(game_id), str(my_team), str(me)))}


def vs_opponents(source: str, player_id: str, limit: int = 3) -> List[Dict[str, Any]]:
    """Как игрок выглядит против конкретных соперников.

    Для последней встречи считаем, насколько совпал состав нашей команды с
    прошлой встречей: «сыграл хуже» при полностью другой пятёрке — это другая
    история, чем «сыграл хуже тем же составом»."""
    rows = _games(source, player_id)
    if not rows:
        return []
    sheets_cache.init_db()
    by_opp: Dict[str, List[Dict[str, Any]]] = {}
    with sheets_cache.get_connection() as conn:
        for r in rows:
            opp, at_home, ours, theirs = _opponent_of(conn, source, r["game_id"], r.get("team_id"))
            if not opp:
                continue          # нет меты — соперника не знаем, молчим
            r = {**r, "_opp": opp, "_home": at_home, "_ours": ours, "_theirs": theirs,
                 "_mates": _teammates(conn, source, r["game_id"], r.get("team_id"), player_id)}
            by_opp.setdefault(opp, []).append(r)

    out: List[Dict[str, Any]] = []
    for opp, games in by_opp.items():
        if len(games) < 2:
            continue              # одна встреча — сравнивать не с чем
        games.sort(key=lambda x: x["game_date"])
        last, prev = games[-1], games[-2]
        mates_now, mates_then = last["_mates"], prev["_mates"]
        overlap = (len(mates_now & mates_then) / len(mates_now | mates_then) * 100
                   if (mates_now | mates_then) else 0)
        out.append({
            "opponent": opp,
            "meetings": len(games),
            "last_date": last["game_date"],
            "prev_date": prev["game_date"],
            "last": {k: int(last.get(k) or 0) for k in ("pts", "reb", "ast", "tur")},
            "prev": {k: int(prev.get(k) or 0) for k in ("pts", "reb", "ast", "tur")},
            "avg_fp": round(sum(fantasy_stats.fantasy_points(g) for g in games) / len(games), 1),
            "roster_overlap": round(overlap),
            "wins": sum(1 for g in games if g["_ours"] > g["_theirs"]),
        })
    out.sort(key=lambda x: (-x["meetings"], x["last_date"]), reverse=False)
    return out[:limit]


def team_role(source: str, player_id: str, last_n: int = 5) -> Dict[str, Any]:
    """Доля игрока в работе команды: сколько её очков и подборов на нём.

    Средние не показывают роль: 8 очков в слабой игре команды весят больше, чем
    8 в разгроме."""
    rows = _games(source, player_id)
    if not rows:
        return {}
    sheets_cache.init_db()
    shares: List[Tuple[float, float]] = []
    with sheets_cache.get_connection() as conn:
        for r in rows[-last_n:]:
            tot = conn.execute(
                """SELECT SUM(pts) AS pts, SUM(reb) AS reb FROM game_player_stats
                   WHERE source = ? AND game_id = ? AND team_id = ?""",
                (source, str(r["game_id"]), str(r.get("team_id")))).fetchone()
            tp, tr = int(tot["pts"] or 0), int(tot["reb"] or 0)
            if tp or tr:
                shares.append((int(r.get("pts") or 0) / tp * 100 if tp else 0,
                               int(r.get("reb") or 0) / tr * 100 if tr else 0))
    if not shares:
        return {}
    return {"games": len(shares),
            "pts_share": round(sum(s[0] for s in shares) / len(shares)),
            "reb_share": round(sum(s[1] for s in shares) / len(shares))}


def shooting(source: str, player_id: str, last_n: int = 5) -> Dict[str, Any]:
    """Броски: проценты за форму против остальной карьеры."""
    rows = _games(source, player_id)
    if not rows:
        return {}
    form, rest = rows[-last_n:], rows[:-last_n]

    def pct(subset, made, att):
        m = sum(int(r.get(made) or 0) for r in subset)
        a = sum(int(r.get(att) or 0) for r in subset)
        return (round(m / a * 100), m, a) if a else (None, m, a)

    out = {}
    for label, made, att in (("2-очковые", "fgm", "fga"),
                             ("3-очковые", "tpm", "tpa"),
                             ("штрафные", "ftm", "fta")):
        now, m, a = pct(form, made, att)
        was, _, ra = pct(rest, made, att)
        if now is None or a < 3:
            continue              # 1–2 броска процентом называть нельзя
        out[label] = {"now": now, "was": was, "made": m, "att": a,
                      "per_game": round(a / len(form), 1)}
    return out
