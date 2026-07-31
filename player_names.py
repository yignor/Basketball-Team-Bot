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
    with _lock:
        _names[_key(source, player_id)] = name


def put_many(source: str, pairs: Iterable[Tuple[object, str]]) -> int:
    added = 0
    with _lock:
        for pid, name in pairs:
            if name:
                _names[_key(source, pid)] = name
                added += 1
        if added:
            global _filled_at
            _filled_at = time.time()
    return added


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
