"""Личный разбор одной игры — то самое «присылать после каждой игры».

Настройка «после каждой игры» в личном кабинете существовала давно, но её
никто не читал: сообщение уходило только раз в месяц файлом. Здесь — короткий
текст в личку сразу, как только протокол игры оказался в базе.

Считаем всё по локальной копии бокс-скоров: в сеть модуль не ходит вовсе.
Сравниваем игру с тем, как человек играл ДО неё, а не со всей историей вместе
с ней — иначе игрок сравнивается сам с собой и разница всегда меньше
настоящей.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import fantasy_stats
import personal_report
import sheets_cache

logger = logging.getLogger(__name__)

# Игру старше этого срока в личку не шлём: после простоя демона незачем
# заваливать человека разборами матчей, о которых он давно забыл.
MAX_AGE_DAYS = 3

# Матч, который человек ТОЛЬКО ЧТО отыграл: начался не больше этого срока
# назад. Считаем от начала, а не от появления протокола: 10.08.2026 протокол
# лёг в 04:31 — через четырнадцать часов после дневной игры, и по «протокол
# только что пришёл» разбор ушёл бы ночью.
FRESH_AFTER_START_HOURS = 4

# Сколько предыдущих игр берём за «обычно».
BASE_GAMES = 5

# Показатели строки игрока: ключ, подпись, «больше — лучше».
LINE_METRICS = (
    ("pts", "очки", True),
    ("reb", "подборы", True),
    ("ast", "передачи", True),
    ("stl", "перехваты", True),
    ("blk", "блок-шоты", True),
    ("tur", "потери", False),
)


def _rows(source: str, player_id: str) -> List[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            """SELECT * FROM game_player_stats
               WHERE source = ? AND player_id = ?
               ORDER BY game_date, game_id""", (source, str(player_id)))]


def started_at(source: str, game_id: Any) -> Optional[datetime]:
    """Когда матч начался по расписанию лиги. МСК; не нашли — None.

    Времени начала в статистике нет, оно живёт в служебных записях об опросе и
    анонсе. Форматы у лиг разошлись: SLPRO пишет id с приставкой и дату по
    ISO, Инфобаскет — голый id и дату «22.08.2026». Разбираем оба, иначе
    правило «только что отыграл» молча не сработает для одной из лиг."""
    from datetime_utils import MOSCOW_TZ
    sheets_cache.init_db()
    plain = str(game_id or "").strip()
    if not plain:
        return None
    ids = {plain, f"{source}-{plain}", plain.split("-")[-1]}
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT game_date, game_time FROM service_records
                WHERE deleted = 0 AND game_time != '' AND game_id IN (%s)
                ORDER BY updated_at DESC""" % ",".join("?" * len(ids)),
            tuple(ids)).fetchall()
    for row in rows:
        day, clock = str(row["game_date"] or ""), str(row["game_time"] or "")
        for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M"):
            try:
                got = datetime.strptime(f"{day} {clock}", fmt)
            except ValueError:
                continue
            return got.replace(tzinfo=MOSCOW_TZ)
    return None


def just_played(source: str, game: Dict[str, Any],
                now: Optional[datetime] = None) -> bool:
    """Правда ли, что человек только что отыграл этот матч.

    Нужно для позднего вечера: игры начинаются и в 21:50, заканчиваются к
    полуночи, и разбор в это время — не помеха, а то, чего ждут. А протокол,
    приехавший под утро по вчерашней игре, ждёт девяти часов."""
    from datetime_utils import get_moscow_time
    started = started_at(source, game.get("game_id"))
    if not started:
        return False
    passed = ((now or get_moscow_time()) - started).total_seconds() / 3600.0
    return 0 <= passed <= FRESH_AFTER_START_HOURS


def latest_game(source: str, player_id: str,
                max_age_days: int = MAX_AGE_DAYS) -> Optional[Dict[str, Any]]:
    """Последняя игра человека, если она свежая."""
    rows = _rows(source, player_id)
    if not rows:
        return None
    last = rows[-1]
    try:
        age = (date.today() - date.fromisoformat(str(last["game_date"])[:10])).days
    except ValueError:
        return None
    return last if 0 <= age <= max_age_days else None


def _opponent_name(source: str, game_id: str, my_team: str, opp_id: str) -> str:
    """Как называется соперник.

    Сначала протокол игры (лига пишет туда названия обеих команд), потом
    справочник лиг. В сеть не идём: имя соперника не стоит того, чтобы разбор
    ждал чужой сервер."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        meta = conn.execute(
            """SELECT home_team_id, home_name, guest_name FROM game_meta
               WHERE source = ? AND game_id = ?""",
            (source, str(game_id))).fetchone()
        if meta:
            at_home = str(meta["home_team_id"] or "") == str(my_team)
            name = str((meta["guest_name"] if at_home else meta["home_name"]) or "")
            if name.strip():
                return name.strip()
        if opp_id:
            row = conn.execute(
                "SELECT name FROM league_teams WHERE source = ? AND team_id = ?",
                (source, str(opp_id))).fetchone()
            if row and str(row["name"] or "").strip():
                return str(row["name"]).strip()
    return ""


MONTHS_GEN = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля",
              "августа", "сентября", "октября", "ноября", "декабря"]


def _human_day(iso: str) -> str:
    """'2026-08-02' -> '2 августа'."""
    try:
        d = date.fromisoformat(str(iso)[:10])
        return f"{d.day} {MONTHS_GEN[d.month - 1]}"
    except (ValueError, IndexError):
        return str(iso or "")


def _esc(text: str) -> str:
    return (str(text or "").replace("&", "&amp;")
            .replace("<", "&lt;").replace(">", "&gt;"))


def digest(source: str, source_title: str, player_id: str,
           game: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """Разбор последней игры для личного сообщения (HTML). None — если пусто.

    Собран блоками: результат, своя строка, броски, фэнтези, отличия от
    обычного. Сплошным текстом это читалось как выписка из протокола — а
    человек хочет за пять секунд понять, хорошо он сыграл или нет."""
    rows = _rows(source, player_id)
    if not rows:
        return None
    game = game or rows[-1]
    key = (str(game["game_id"]), str(game["game_date"]))
    before = [r for r in rows
              if (str(r["game_id"]), str(r["game_date"])) != key
              and str(r["game_date"]) <= str(game["game_date"])]
    base = before[-BASE_GAMES:]

    import monthly_report
    ctx = monthly_report._game_context(source, str(game["game_id"]),
                                       str(game["team_id"]), str(player_id))
    opp = _opponent_name(source, str(game["game_id"]), str(game["team_id"]),
                         str(ctx.get("opponent", ""))) or "соперник"

    # Шапка: сначала исход, потом с кем и когда. Счёт без исхода приходится
    # расшифровывать самому, а это первое, что человек хочет увидеть.
    ours, theirs = ctx.get("ours"), ctx.get("theirs")
    if ours is not None and theirs is not None and (ours or theirs):
        icon, word = ("🏆", "Победа") if ours > theirs else (
            ("🤝", "Ничья") if ours == theirs else ("💔", "Поражение"))
        head = f"{icon} <b>{word} {ours}:{theirs}</b> · {_esc(opp)}"
    else:
        head = f"🏀 <b>Игра с {_esc(opp)}</b>"
    lines = [head, f"{_human_day(str(game['game_date']))} · {_esc(source_title)}", ""]

    stat_bits = [f"{title.capitalize()} {int(game.get(k) or 0)}"
                 for k, title, _ in LINE_METRICS if int(game.get(k) or 0)]
    mins = int(game.get("secs") or 0) // 60
    lines.append("📊 <b>Твоя игра</b>")
    lines.append(" · ".join(stat_bits) if stat_bits else "В протоколе одни нули.")
    if mins:
        lines.append(f"{mins} мин на площадке")

    shots = []
    if int(game.get("fga") or 0):
        shots.append(f"С игры {int(game['fgm'])}/{int(game['fga'])}")
    if int(game.get("tpa") or 0):
        shots.append(f"Трёхочковые {int(game['tpm'])}/{int(game['tpa'])}")
    if int(game.get("fta") or 0):
        shots.append(f"Штрафные {int(game['ftm'])}/{int(game['fta'])}")
    if shots:
        lines += ["", "🎯 <b>Броски</b>", " · ".join(shots)]

    fp = fantasy_stats.fantasy_points(game)
    lines += ["", f"⚡️ <b>Фэнтези: {fp:g}</b>"]
    if base:
        base_fp = sum(fantasy_stats.fantasy_points(r) for r in base) / len(base)
        diff = round(fp - base_fp, 1)
        if diff > 0:
            lines.append(f"На {diff:g} выше твоего среднего за {len(base)} игр "
                         f"({base_fp:.1f})")
        elif diff < 0:
            lines.append(f"На {abs(diff):g} ниже твоего среднего за {len(base)} игр "
                         f"({base_fp:.1f})")
        else:
            lines.append(f"Ровно твой средний за {len(base)} игр")
    else:
        lines.append("Первая игра в базе — сравнивать пока не с чем")

    if base:
        better, worse = [], []
        for k, title, higher_better in LINE_METRICS:
            now = float(game.get(k) or 0)
            was = sum(float(r.get(k) or 0) for r in base) / len(base)
            delta = round(now - was, 1)
            if abs(delta) < 1:
                continue
            text = f"{title} {'+' if delta > 0 else '−'}{abs(delta):g}"
            (better if (delta > 0) == higher_better else worse).append((abs(delta), text))
        better.sort(key=lambda x: -x[0])
        worse.sort(key=lambda x: -x[0])
        if better or worse:
            lines.append("")
            if better:
                lines.append("📈 Лучше обычного: "
                             + ", ".join(t for _, t in better[:3]))
            if worse:
                lines.append("📉 Хуже обычного: "
                             + ", ".join(t for _, t in worse[:3]))

    # Тайм-коды выходов на площадку. Появляются не сразу: разметку тянет
    # фоновая дозагрузка после игры, и ссылка на запись ВК тоже находится не
    # мгновенно. Нет разметки — блока просто нет.
    try:
        import game_timeline
        import vk_video
        spans = game_timeline.format_block(
            source, str(game["game_id"]), str(player_id),
            vk_video.link_of(source, str(game["game_id"])))
        if spans:
            lines += ["", spans]
    except Exception as exc:  # разбор важнее тайм-кодов
        logger.warning("Тайм-коды в разборе %s/%s: %s", source, game.get("game_id"), exc)

    lines += ["", "<i>Как часто присылать — «📊 Моя статистика» → уведомления.</i>"]
    return "\n".join(lines)


# ─────────────────── Кому и что уже отправляли ─────────────────────────────

def sent_key(source: str, game_id: str, tg_user_id: Any) -> str:
    return f"pers:{source}:{game_id}:{tg_user_id}"


def already_sent(key: str) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return bool(conn.execute("SELECT 1 FROM pay_events WHERE event_key = ?",
                                 (key,)).fetchone())


def mark_sent(key: str, details: str = "") -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO pay_events (event_key, sent_at, details) "
            "VALUES (?, ?, ?)",
            (key, datetime.now().isoformat(timespec="seconds"), details))
        conn.commit()


def pending() -> List[Dict[str, Any]]:
    """Кому пора отправить разбор: [{uid, source, title, player_id, game, key}].

    Только те, у кого стоит «после каждой игры» и включена личная подписка."""
    import player_identity
    import subscriptions

    titles = {"slpro": "SLPRO", "infobasket": "Инфобаскет"}
    out: List[Dict[str, Any]] = []
    for uid in player_identity.linked_users():
        # Разбор с таймкодами — платная часть личной статистики, а не общая
        # рассылка. Кому раздел не открыт, тому и не уходит.
        if not personal_report.stats_open(uid):
            continue
        prefs = personal_report.get_prefs(uid)
        if (prefs.get("notify_mode") or "game") != "game":
            continue
        if not subscriptions.enabled(uid, "personal"):
            continue
        for ident in player_identity.get_identities(uid):
            source, pid = str(ident["source"]), str(ident["player_id"])
            game = latest_game(source, pid)
            if not game:
                continue
            key = sent_key(source, str(game["game_id"]), uid)
            if already_sent(key):
                continue
            out.append({"uid": str(uid), "source": source,
                        "title": titles.get(source, source),
                        "player_id": pid, "game": game, "key": key})
    return out
