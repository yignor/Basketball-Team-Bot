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

# Метрики отчёта: ключ в БД -> (название, «больше — лучше»).
METRICS: List[Tuple[str, str, bool]] = [
    ("pts", "очки", True),
    ("reb", "подборы", True),
    ("ast", "передачи", True),
    ("stl", "перехваты", True),
    ("blk", "блок-шоты", True),
    ("tur", "потери", False),
]

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


def _avg(rows: List[Dict[str, Any]], key: str) -> float:
    return round(sum(float(r.get(key) or 0) for r in rows) / len(rows), 1) if rows else 0.0


def compare(source: str, player_id: str, mode: str = "all",
            since: Optional[str] = None,
            form_games: int = DEFAULT_FORM_GAMES) -> Dict[str, Any]:
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
    for key, title, higher_better in METRICS:
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
        "avg_now": {k: _avg(form, k) for k, _, _ in METRICS},
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
                 "notify_mode": "game", "last_sent": ""}


def get_prefs(tg_user_id: Any) -> Dict[str, Any]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM player_report_prefs WHERE tg_user_id = ?",
                           (str(tg_user_id),)).fetchone()
    return {**DEFAULT_PREFS, **(dict(row) if row else {})}


def set_pref(tg_user_id: Any, field: str, value: str) -> Dict[str, Any]:
    """Меняет одну настройку. Поле сверяем со списком — значение приходит из
    callback_data, то есть снаружи."""
    if field not in ("compare_mode", "compare_since", "notify_mode"):
        return get_prefs(tg_user_id)
    prefs = get_prefs(tg_user_id)
    prefs[field] = value
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO player_report_prefs
               (tg_user_id, compare_mode, compare_since, notify_mode, last_sent, updated_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(tg_user_id) DO UPDATE SET
                   compare_mode=excluded.compare_mode,
                   compare_since=excluded.compare_since,
                   notify_mode=excluded.notify_mode,
                   updated_at=excluded.updated_at""",
            (str(tg_user_id), prefs["compare_mode"], prefs["compare_since"],
             prefs["notify_mode"], prefs.get("last_sent", ""), sheets_cache.now_iso()))
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


def due_for_report(prefs: Dict[str, Any], today: Optional[date] = None) -> bool:
    """Пора ли слать по расписанию. Для режима «после игры» решает не расписание,
    а факт сыгранной игры — здесь всегда False."""
    today = today or date.today()
    mode = prefs.get("notify_mode", "game")
    if mode in ("off", "game"):
        return False
    last = (prefs.get("last_sent") or "")[:10]
    if not last:
        return True
    try:
        gap = (today - date.fromisoformat(last)).days
    except ValueError:
        return True
    return gap >= (7 if mode == "week" else 30)
