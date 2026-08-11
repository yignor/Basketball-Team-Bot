"""Игра, заведённая тренером руками.

Организатор часто объявляет матч раньше, чем тот появляется в расписании лиги,
и до сих пор бот об этой игре не знал ничего: ни опроса, ни состава, ни оплаты.
Здесь тренер собирает игру сам — лига (или товарищеский), соперник, дата,
время, место, форма — и отправляет опрос.

Ключевая мысль: созданная руками игра дальше живёт как обычная. Она пишется
в тот же реестр опросов (`service_records`), поэтому её видят и сбор состава,
и напоминания об оплате, и подсчёт долгов — без единой правки в тех местах.

Соперников и площадки не выдумываем: и то и другое берём из уже сыгранных игр
(`game_meta`) и прошлых опросов. Команда, которой у нас нет в истории, всё
равно вводится руками — лига могла добавить новичка.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

FRIENDLY = "friendly"
POLL_OPTIONS = ["✅ Готов", "❌ Нет", "👨‍🏫 Тренер"]
FORMS = {"dark": "тёмная", "light": "светлая"}

DAYS_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def leagues() -> List[Dict[str, str]]:
    """Куда можно завести игру: наши турниры из «Конфига» + товарищеский.

    Читаем локальный справочник лиг — тот же, что кормит пул фэнтези. Живых
    запросов тут быть не должно: тренер жмёт кнопку и ждёт ответа."""
    out: List[Dict[str, str]] = []
    try:
        import league_sync
        for team in league_sync.our_teams():
            src = str(team.get("source") or "")
            name = str(team.get("league") or team.get("name") or src)
            out.append({"key": src, "title": name, "source": src,
                        "team_id": str(team.get("team_id") or "")})
    except Exception as exc:
        logger.warning("Список лиг для новой игры: %s", exc)
    out.append({"key": FRIENDLY, "title": "🤝 Товарищеский матч",
                "source": "infobasket", "team_id": ""})
    return out


def find_teams(source: str, query: str, limit: int = 8) -> List[str]:
    """Соперники из уже сыгранных игр этой лиги — по части названия.

    Сравнение в Python, а не в SQL: lower() в SQLite не знает кириллицы, и
    «спартак» не находил бы «Спартак» (эта грабля в проекте уже была)."""
    import player_search
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT home_name AS n FROM game_meta WHERE source = ? AND home_name != ''
               UNION SELECT guest_name FROM game_meta WHERE source = ? AND guest_name != ''""",
            (str(source), str(source))).fetchall()
    names = sorted({str(r["n"]).strip() for r in rows if str(r["n"]).strip()})
    # Тот же поиск, что и по фамилиям: точное совпадение, начало, вхождение.
    # Раньше здесь было своё правило, и вело оно себя иначе, чем поиск игрока —
    # человек не должен помнить, где как ищется.
    return player_search.rank(query, names, lambda n: [n], limit)


def arenas(limit: int = 8) -> List[str]:
    """Площадки, где мы уже играли, — частые сверху.

    Игры почти всегда идут в одних и тех же залах, и выбор кнопкой избавляет
    от ручного ввода адреса с телефона."""
    sheets_cache.init_db()
    counts: Dict[str, int] = {}
    with sheets_cache.get_connection() as conn:
        for table, col in (("game_meta", "arena"), ("service_records", "arena")):
            for r in conn.execute(f"SELECT {col} AS a FROM {table} WHERE {col} != ''"):
                name = str(r["a"]).strip()
                if not name or name.lower() in ("неизвестно", "не указано"):
                    continue
                counts[name] = counts.get(name, 0) + 1
    return [n for n, _ in sorted(counts.items(), key=lambda kv: -kv[1])][:limit]


def parse_day(text: str, today: Optional[date] = None) -> Optional[date]:
    """«09.08», «09.08.2026», «2026-08-09» → дата. Без года — ближайшая.

    Без года берём ближайшую БУДУЩУЮ дату: игру заводят наперёд, и «09.08»,
    введённое в декабре, — это следующий год, а не прошедший август."""
    raw = str(text or "").strip().replace("/", ".").replace("-", ".")
    parts = [p for p in raw.split(".") if p]
    today = today or date.today()
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    try:
        if len(nums) == 3:
            if nums[0] > 31:                     # 2026.08.09
                return date(nums[0], nums[1], nums[2])
            year = nums[2] if nums[2] > 99 else 2000 + nums[2]
            return date(year, nums[1], nums[0])
        if len(nums) == 2:
            guess = date(today.year, nums[1], nums[0])
            return guess if guess >= today else date(today.year + 1, nums[1], nums[0])
    except ValueError:
        return None
    return None


def parse_time(text: str) -> str:
    """«18:30», «18.30», «1830» → «18:30». Пусто — не разобрали."""
    raw = re.sub(r"[^\d]", "", str(text or ""))
    if len(raw) == 3:
        raw = "0" + raw
    if len(raw) != 4:
        return ""
    hh, mm = int(raw[:2]), int(raw[2:])
    return f"{hh:02d}:{mm:02d}" if hh < 24 and mm < 60 else ""


def new_game_id(source: str, when: Optional[datetime] = None) -> str:
    """Свой id для игры без расписания.

    Префикс `slpro-` обязателен для игр SLPRO: по нему в проекте определяется
    источник (`game_roster.source_of`). Метка времени делает id уникальным, а
    буква «m» отличает заведённое руками от настоящего id лиги."""
    stamp = (when or datetime.now()).strftime("%y%m%d%H%M")
    return f"slpro-m{stamp}" if source == "slpro" else f"m{stamp}"


def poll_text(draft: Dict[str, Any]) -> str:
    """Текст опроса — ровно того же вида, что у лиговых игр.

    Форму и место потом читает сбор состава (`game_roster`), поэтому строки
    «👕 …» и «📍 …» должны выглядеть так же: иначе разбор их не найдёт."""
    day: date = draft["date"]
    league = draft.get("league_title") or ""
    tail = f" ({league})" if league and draft.get("key") != FRIENDLY else ""
    if draft.get("key") == FRIENDLY:
        tail = " (товарищеский)"
    lines = [f"🏀 {draft.get('our', 'Мы')} против {draft['opponent']}{tail}",
             f"📅 {day.strftime('%d.%m')}, {DAYS_RU[day.weekday()]}, {draft['time']}"]
    if draft.get("form") in FORMS:
        lines.append(f"👕 {FORMS[draft['form']]} форма")
    if draft.get("arena"):
        lines.append(f"📍 {draft['arena']}")
    return "\n".join(lines)


def summary(draft: Dict[str, Any]) -> str:
    """Что тренер увидит перед отправкой."""
    return ("📋 Проверь игру:\n\n" + poll_text(draft) +
            "\n\nПосле отправки опроса можно будет собрать состав "
            "и вести оплату — как по обычной игре.")


def register(draft: Dict[str, Any], polls: List[Dict[str, Any]]) -> str:
    """Кладёт игру в реестр опросов. Возвращает game_id.

    Две записи, как и у лиговых игр: сама игра (её читает сбор состава) и
    регистрация каждого опроса (по ней собираются голоса)."""
    from enhanced_duplicate_protection import duplicate_protection

    source = draft["source"]
    gid = draft.get("game_id") or new_game_id(source)
    day: date = draft["date"]
    dtype = "ОПРОС_ИГРА_SLPRO" if source == "slpro" else "ОПРОС_ИГРА"
    duplicate_protection.add_record(
        dtype, gid,
        status="ОПРОС СОЗДАН (тренер)",
        additional_data=poll_text(draft),
        alt_name=draft.get("our", ""),
        game_id=gid,
        game_date=day.isoformat(),
        game_time=draft.get("time", ""),
        arena=draft.get("arena", ""),
        team_b_id=str(draft.get("opponent_id") or ""),
    )
    for pm in polls:
        if not pm.get("poll_id"):
            continue
        duplicate_protection.add_record(
            "GAME_POLL_REG",
            f"GPOLL_{gid}_{pm['message_id']}",
            status="АКТИВЕН",
            additional_data=json.dumps({
                "tg_poll_id": pm["poll_id"],
                "options": POLL_OPTIONS,
                "chat_id": pm["chat_id"],
                "message_id": pm["message_id"],
                "game_id": gid,
            }, ensure_ascii=False),
            alt_name=gid,
            game_date=day.isoformat(),
        )
    logger.info("Тренер завёл игру %s: %s, %s %s", gid, draft.get("opponent"),
                day.isoformat(), draft.get("time"))
    return gid


def _norm(text: Any) -> str:
    return str(text or "").strip().lower().replace("ё", "е")
