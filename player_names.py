#!/usr/bin/env python3
"""
Имена игроков — В ОПЕРАТИВНОЙ ПАМЯТИ, на диск не попадают.

Почему так, а не таблицей ([[legal-data-invariant]], решение 31.07.2026):
ФИО — персональные данные, и главный риск для них — утечка файла. База уезжает
в бэкап, копируется вместе с диском, попадает в архив; оперативная память
умирает вместе с процессом и утечь этим путём не может. Поэтому на диске
лежат только идентификаторы (`league_rosters`), а имя к ним приклеивается уже
здесь, в памяти, и после перезапуска подтягивается заново.

Цена решения принята сознательно: сразу после рестарта имён нет, и отчёт,
собранный в эту минуту, покажет «№7» вместо фамилии. Наполняет реестр
league_sync — фоном, никогда из обработчика сообщения.

Ключ — `source:player_id`, как в остальном коде (`fantasy_stats.parse_ref`).
"""

import threading
import time
from typing import Dict, Iterable, Optional, Tuple

# Держим под замком: наполняет фоновая качалка, читают обработчики и веб-API.
_lock = threading.Lock()
_names: Dict[str, str] = {}
_filled_at: float = 0.0

# Сутки: заявки меняются редко, а «протухло» тут значит лишь «пора освежить»,
# а не «показывать нельзя» — старое имя всё равно вернём.
STALE_AFTER = 24 * 3600.0


def _key(source: str, player_id: object) -> str:
    return f"{'slpro' if source in ('slpro',) else 'infobasket'}:{player_id}"


def put(source: str, player_id: object, name: str) -> None:
    if not name:
        return
    name = _canonical(name)
    with _lock:
        _names[_key(source, player_id)] = name


def put_many(source: str, pairs: Iterable[Tuple[object, str]]) -> int:
    added = 0
    canon = _sheet_spellings()
    with _lock:
        for pid, name in pairs:
            if name:
                _names[_key(source, pid)] = _canonical(name, canon)
                added += 1
        if added:
            global _filled_at
            _filled_at = time.time()
    return added


def _norm(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def _sheet_spellings() -> Dict[str, str]:
    """{нормализованное ФИО -> написание из листа «Игроки»}.

    Лист — единственный источник правды по написанию: там имена ведёт тренер.
    Лиги пишут одного человека по-разному («Шлепикас Роман» в SLPRO, «Ромас
    Шлепикас» в Инфобаскете), и в отчётах он выглядел двумя людьми. Ключей на
    человека два — прямой и перевёрнутый порядок, — чтобы поймать обе лиги."""
    try:
        import sheets_cache
        sheets_cache.init_db()
        with sheets_cache.get_connection() as conn:
            rows = conn.execute(
                "SELECT surname, name FROM players WHERE surname != '' OR name != ''"
            ).fetchall()
    except Exception:
        return {}
    out: Dict[str, str] = {}
    for r in rows:
        full = f"{r['surname']} {r['name']}".strip()
        if not full:
            continue
        out[_norm(full)] = full
        out[_norm(f"{r['name']} {r['surname']}")] = full
    return out


def _canonical(name: str, canon: Optional[Dict[str, str]] = None) -> str:
    """Написание из листа, если человек там есть. Иначе — как прислала лига.

    Терпим одну опечатку: «Шлепикас Ромас» из протокола и «Шлепикас Роман» из
    листа — один человек, и в отчёте он должен быть одной строкой."""
    if canon is None:
        canon = _sheet_spellings()
    key = _norm(name)
    hit = canon.get(key)
    if hit:
        return hit
    parts = key.split()
    if len(parts) < 2:
        return name
    for other, val in canon.items():
        o = other.split()
        if len(o) < 2:
            continue
        if (o[0] == parts[0] and _lev1(" ".join(o[1:]), " ".join(parts[1:]))) or \
                (o[1:] == parts[1:] and _lev1(o[0], parts[0])):
            return val
    return name


def _lev1(a: str, b: str) -> bool:
    """Расстояние Левенштейна не больше 1."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la == lb:
        return sum(1 for x, y in zip(a, b) if x != y) <= 1
    if la > lb:
        a, b, la, lb = b, a, lb, la
    i = j = diff = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            diff += 1
            if diff > 1:
                return False
            j += 1
    return True


def get(source: str, player_id: object, default: str = "") -> str:
    with _lock:
        return _names.get(_key(source, player_id), default)


def get_all() -> Dict[str, str]:
    """Копия реестра: {source:player_id -> ФИО}. Копия, а не сам словарь —
    чтобы вызывающий не держал его под изменением из фоновой качалки."""
    with _lock:
        return dict(_names)


def by_player_id() -> Dict[str, str]:
    """{player_id -> ФИО} без источника — в таком виде имена ждут отчёты
    (team_progress, разбор игры). Совпадение id между лигами исключено:
    у Инфобаскета шестизначные, у SLPRO — короткие."""
    with _lock:
        return {k.split(":", 1)[1]: v for k, v in _names.items()}


def stats() -> Dict[str, object]:
    with _lock:
        age = time.time() - _filled_at if _filled_at else None
    return {"count": len(_names), "age_seconds": None if age is None else round(age),
            "stale": age is None or age > STALE_AFTER}


def is_cold() -> bool:
    """Реестр пуст — значит демон только что поднялся и качалка ещё не
    отработала. Вызывающему стоит показать номера вместо фамилий, а не ждать."""
    with _lock:
        return not _names


def clear() -> None:
    """Забыть все имена (тесты и кнопка «очистить» в админке)."""
    global _filled_at
    with _lock:
        _names.clear()
        _filled_at = 0.0
