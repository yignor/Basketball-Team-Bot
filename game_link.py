"""Одна игра или две: тренерская запись против находки в расписании лиги.

Организатор объявляет матч раньше, чем тот появляется в расписании, поэтому
тренер заводит игру руками (`coach_newgame`). Через день-другой лига публикует
ту же игру со своим GameID — и для бота это новая игра: он шлёт второй опрос,
команда голосует дважды, состав раздваивается, оплата считается по одной из
записей. Ровно это и произошло на боевых.

Здесь бот решает, встретил ли он ту же игру. Правило простое и намеренно
консервативное: **тот же соперник и та же дата** — одна игра. Время в расчёт
не берём: организатор называет одно, лига ставит другое, и по времени
совпадения почти не бывает. Зал тем более: он в лиге то и дело «Неизвестно».

Что делаем в каждом случае:

* **та же дата** — второго опроса нет. Лиговый GameID привязывается к
  тренерской записи, и дальше всё (состав, оплата, результат) идёт по ней.
  Тренеру уходит сообщение с кнопкой «Это разные игры» — на случай, если
  команда действительно играет с тем же соперником дважды за день.
* **другая дата** — это перенос. Сам опрос не рассылаем: бывало, что игру
  отменили, а в API её просто передвинули. Спрашиваем тренера кнопкой.

Связка живёт в своей таблице, а не в поле записи: у неё есть состояние
(«решено ботом» / «тренер подтвердил» / «тренер развязал»), и затирать его
пересинхронизацией сервисного листа нельзя.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import sheets_cache

logger = logging.getLogger(__name__)

# Заведённые тренером id: «m2608101430» и «slpro-m2608101430» (см.
# coach_newgame.new_game_id). Ни один id лиги так не выглядит — они числовые.
MANUAL_RE = re.compile(r"^(?:slpro-)?m\d{6,}$")

POLL_TYPES = {"infobasket": "ОПРОС_ИГРА", "slpro": "ОПРОС_ИГРА_SLPRO"}

# На сколько дней вокруг искать перенос. Больше двух недель — уже не перенос,
# а другая игра того же круга.
MOVE_WINDOW_DAYS = 14

# Состояния связки.
AUTO = "auto"            # бот решил сам, тренер не возражал
CONFIRMED = "confirmed"  # тренер подтвердил
SPLIT = "split"          # тренер сказал «это разные игры»


def is_manual(game_id: Any) -> bool:
    """Заведена ли игра тренером руками."""
    return bool(MANUAL_RE.match(str(game_id or "").strip()))


def _norm(text: Any) -> str:
    """Название команды к сравнимому виду: регистр, ё, кавычки, пробелы."""
    s = str(text or "").lower().replace("ё", "е")
    s = re.sub(r"[«»\"'`.,()\-–—]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def same_team(a: Any, b: Any) -> bool:
    """Одна ли это команда по названию.

    Лига и организатор пишут по-разному: «Атланты» и «БК Атланты», «30 FPS» и
    «30FPS», «Спартак-2» и «Спартак 2». Точное равенство тут не работает,
    поэтому сравниваем без пробелов и допускаем вложение одного в другое — но
    только если короткое достаточно длинное, иначе «БК» совпадёт со всеми
    подряд."""
    x = _norm(a).replace(" ", "")
    y = _norm(b).replace(" ", "")
    if not x or not y:
        return False
    if x == y:
        return True
    short, long = (x, y) if len(x) <= len(y) else (y, x)
    return len(short) >= 4 and short in long


def _as_date(value: Any) -> Optional[date]:
    """«2026-08-09» или «09.08.2026» → дата. Оба формата живут рядом: SLPRO
    пишет ISO, Инфобаскет — DD.MM.YYYY."""
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(text[:10], fmt).date()
        except ValueError:
            continue
    return None


def opponent_of(record: Dict[str, Any]) -> str:
    """Соперник из текста опроса: «🏀 Мы против Атлантов (SLPRO)» → «Атлантов».

    По id соперника сверять нечем: у товарищеских игр его нет вовсе, а у
    заведённых руками он заполняется, только если тренер выбрал команду из
    истории. Текст опроса есть всегда — его бот сам и составил."""
    text = str(record.get("additional_data") or "")
    m = re.search(r"против\s+([^\n(]+)", text)
    if not m:
        return ""
    return m.group(1).strip()


def manual_games(source: str, since: Optional[date] = None) -> List[Dict[str, Any]]:
    """Игры, заведённые тренером, по одной лиге. Свежие — сначала."""
    dtype = POLL_TYPES.get(source, POLL_TYPES["infobasket"])
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT * FROM service_records
                WHERE deleted = 0 AND data_type = ? AND game_id != ''""",
            (dtype,)).fetchall()
    out = []
    for r in rows:
        rec = dict(r)
        if not is_manual(rec.get("game_id")):
            continue
        day = _as_date(rec.get("game_date"))
        if day is None or (since and day < since):
            continue
        rec["day"] = day
        rec["opponent"] = opponent_of(rec)
        out.append(rec)
    out.sort(key=lambda x: x["day"], reverse=True)
    return out


def find_twin(source: str, day: Optional[date], opponent: str,
              window_days: int = MOVE_WINDOW_DAYS) -> Optional[Dict[str, Any]]:
    """Тренерская запись про ту же игру. `same_day` — совпала ли дата.

    Ищем не только на саму дату: перенос — тоже «та же игра», просто про него
    надо спросить, а не молча промолчать."""
    if not day or not opponent:
        return None
    best: Optional[Dict[str, Any]] = None
    for rec in manual_games(source, since=day - timedelta(days=window_days)):
        if abs((rec["day"] - day).days) > window_days:
            continue
        if not same_team(rec["opponent"], opponent):
            continue
        rec["same_day"] = rec["day"] == day
        # Точное совпадение даты сильнее любого переноса.
        if rec["same_day"]:
            return rec
        if best is None or abs((rec["day"] - day).days) < abs((best["day"] - day).days):
            best = rec
    return best


# ─────────────────────────── связки ────────────────────────────────────────


def _ensure_table() -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS game_aliases (
                source          TEXT NOT NULL,
                league_game_id  TEXT NOT NULL,
                coach_game_id   TEXT NOT NULL,
                state           TEXT NOT NULL DEFAULT 'auto',
                note            TEXT NOT NULL DEFAULT '',
                created_at      TEXT NOT NULL,
                PRIMARY KEY (source, league_game_id)
            )""")
        conn.commit()


def link(source: str, league_game_id: str, coach_game_id: str,
         state: str = AUTO, note: str = "") -> None:
    """Запоминает, что лиговая игра — это уже заведённая тренером."""
    _ensure_table()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_aliases (source, league_game_id, coach_game_id,
                                         state, note, created_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(source, league_game_id) DO UPDATE SET
                   coach_game_id=excluded.coach_game_id,
                   state=excluded.state, note=excluded.note""",
            (str(source), str(league_game_id), str(coach_game_id),
             str(state), str(note), sheets_cache.now_iso()))
        conn.commit()
    logger.info("Игра %s:%s — это заведённая тренером %s (%s)", source,
                league_game_id, coach_game_id, state)


def split(source: str, league_game_id: str) -> bool:
    """Тренер сказал «это разные игры»: связку снимаем, но помним решение —
    иначе следующий же прогон свяжет их обратно."""
    _ensure_table()
    with sheets_cache.get_connection() as conn:
        n = conn.execute(
            "UPDATE game_aliases SET state = ? WHERE source = ? AND league_game_id = ?",
            (SPLIT, str(source), str(league_game_id))).rowcount
        conn.commit()
    return bool(n)


def alias_of(source: str, league_game_id: str) -> Optional[Dict[str, Any]]:
    """Связка по лиговому id. None — не связывали."""
    _ensure_table()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT * FROM game_aliases WHERE source = ? AND league_game_id = ?",
            (str(source), str(league_game_id))).fetchone()
    return dict(row) if row else None


def already_handled(source: str, league_game_id: str) -> bool:
    """Нужно ли молчать по этой лиговой игре.

    True — опрос уже есть (тренерский), второй не нужен. Развязанная тренером
    игра сюда не попадает: он сказал, что это разные матчи."""
    a = alias_of(source, league_game_id)
    return bool(a and a["state"] in (AUTO, CONFIRMED))


def decided(source: str, league_game_id: str) -> bool:
    """Спрашивали ли уже про эту игру — чтобы не спрашивать каждый прогон."""
    return alias_of(source, league_game_id) is not None
