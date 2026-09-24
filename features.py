"""Что бот делает, а что нет: выключатели для тренера.

Зачем. Бот умеет полтора десятка вещей сам: опросы, анонсы, напоминания об
оплате, дни рождения, фэнтези, разборы. Команде нужно не всё сразу — где-то
взносы собирают наличными, где-то дни рождения поздравляют сами, а опрос на
следующий месяц по тренировкам зимой нужен, летом нет. Раньше выключить это
можно было только правкой «Конфига» или кода, то есть не тренером.

Выключатель — это НЕ удаление: данные и экраны остаются, бот просто перестаёт
писать сам. Включил обратно — всё поехало дальше.

Значение по умолчанию у каждого своё (см. FEATURES): то, что команда уже
получает, не должно вдруг выключиться из-за появления этого модуля.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# key, название, что именно перестанет происходить, по умолчанию включено
FEATURES: List[Tuple[str, str, str, bool]] = [
    ("game_polls", "Опросы на игру",
     "опрос «готов / не готов» перед матчем", True),
    ("game_announcements", "Анонсы игр",
     "сообщение с местом, временем и формой в день игры", True),
    ("game_results", "Результаты игр",
     "итог матча со счётом и лучшими", True),
    ("game_video", "Трансляции и записи",
     "сообщения «идёт трансляция» и «появилась запись»", True),
    ("calendar_events", "Календарь игры",
     "файл .ics, чтобы игра попала в телефон", True),
    ("roster_collect", "Сбор состава на игру",
     "бот сам спрашивает игроков, кто едет", True),
    ("training_polls", "Опросы тренировок",
     "опрос присутствия в топике тренировок", True),
    ("dues_month", "Взносы за тренировки",
     "вопрос «будешь заниматься в следующем месяце» и напоминания об оплате",
     True),
    ("game_pay", "Оплата игр",
     "напоминания заплатить за игру", True),
    ("birthdays", "Дни рождения",
     "поздравления именинников в чате", True),
    ("fantasy", "Фэнтези",
     "недельные итоги и напоминания собрать состав", True),
    ("coach_reports", "Разбор игры тренеру",
     "личное сообщение тренеру после матча", True),
    ("personal_digests", "Личные итоги игрокам",
     "разбор своей игры тем, кто его включил", True),
    ("badges", "Значки и достижения",
     "выдача значков и сообщения о них", True),
    ("starting_lineups", "Стартовый состав тренерам",
     "напоминание с пятёркой за час до игры", True),
]

TITLES = {key: title for key, title, _, _ in FEATURES}
DEFAULTS = {key: default for key, _, _, default in FEATURES}


def _key(name: str) -> str:
    return f"feature:{name}"


def enabled(name: str) -> bool:
    """Включено ли. Незнакомое имя считаем включённым: выключатель, которого
    нет, не должен молча отключить работающую часть бота."""
    try:
        raw = sheets_cache.get_setting(_key(name), None)
    except Exception as exc:
        logger.warning("Выключатели не прочитались (%s): %s", name, exc)
        return DEFAULTS.get(name, True)
    if raw is None:
        return DEFAULTS.get(name, True)
    return str(raw).strip() not in ("0", "off", "нет", "")


def set_enabled(name: str, on: bool) -> None:
    sheets_cache.set_setting(_key(name), "1" if on else "0")


def states() -> Dict[str, bool]:
    return {key: enabled(key) for key, _, _, _ in FEATURES}


def off_list() -> List[str]:
    """Что сейчас выключено — для экрана и для журнала."""
    return [TITLES[key] for key, _, _, _ in FEATURES if not enabled(key)]
