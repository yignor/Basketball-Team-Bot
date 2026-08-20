#!/usr/bin/env python3
"""Тексты писем, которые тренер может переписать под себя.

Зачем проверка подстановок. Письмо — не просто текст: в нём живут месяц, сумма,
соперник. Свободная правка легко роняет подстановку, и вопрос «будешь
заниматься?» уходит без цифры взноса. Поэтому у каждого письма объявлен список
ОБЯЗАТЕЛЬНЫХ полей, и шаблон без них не сохраняется — с объяснением, какого
именно не хватает.

Почему хранение в app_settings, а не в отдельной таблице: правок тут единицы,
живут они вечно и читаются на каждую рассылку. Заводить таблицу ради шести
строк — лишняя сущность.

Свой текст ВСЕГДА можно вернуть к исходному: тренер экспериментирует с тем, что
уходит всей команде, и путь назад должен быть в одно нажатие.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

SETTING_PREFIX = "msgtpl_"

# Что можно переписать. Для каждого письма: заголовок, обязательные поля и
# подсказка про необязательные. Поля названы по-русски — тренер пишет текст,
# а не код, и «{сумма}» понятнее, чем «{amount}».
TEMPLATES: Dict[str, Dict[str, Any]] = {
    "ask": {
        "title": "🏋️ Вопрос про следующий месяц",
        # «сумма» здесь НЕ обязательна: во встроенном тексте её нет — это
        # предварительный опрос, а не счёт. Захочет тренер вернуть цифру в
        # вопрос — подставит сам, поле осталось доступным.
        "required": ("месяц",),
        "optional": ("сумма", "долг"),
        "note": "«{долг}» подставится, только если за человеком висит долг за "
                "текущий месяц. Нет долга — строка с ним исчезнет целиком. "
                "«{сумма}» во встроенном тексте не показывается: её человек "
                "видит после ответа «буду».",
    },
    "dues": {
        "title": "💰 Напоминание за тренировки",
        "required": ("месяц",),
        "optional": ("сумма",),
        "note": "«{сумма}» пропадёт, если взнос человеку не проставлен.",
    },
    "gameahead": {
        "title": "🏀 Завтра игра — про оплату",
        "required": ("игра", "сумма"),
        "optional": (),
        "note": "«{игра}» — это соперник и дата, например «Резалит · 24.08, 21:00».",
    },
    "gamedebt": {
        "title": "💸 Оплата игры после матча",
        "required": ("игра", "сумма"),
        "optional": (),
        "note": "«{игра}» — это соперник и дата.",
    },
}


def fields(key: str) -> Tuple[str, ...]:
    """Все подстановки письма — обязательные и нет."""
    meta = TEMPLATES.get(key) or {}
    return tuple(meta.get("required", ())) + tuple(meta.get("optional", ()))


def custom(key: str) -> str:
    """Текст, заданный тренером. Пусто — используется встроенный."""
    if key not in TEMPLATES:
        return ""
    try:
        return str(sheets_cache.get_setting(SETTING_PREFIX + key, "") or "")
    except Exception as exc:
        logger.warning("Шаблон %s не прочитался: %s", key, exc)
        return ""


def missing(key: str, text: str) -> List[str]:
    """Каких обязательных подстановок не хватает в тексте."""
    meta = TEMPLATES.get(key) or {}
    return [f for f in meta.get("required", ()) if ("{" + f + "}") not in text]


def save(key: str, text: str) -> Tuple[bool, str]:
    """Сохраняет свой текст. (получилось, что сказать тренеру)."""
    if key not in TEMPLATES:
        return False, "Такого письма нет."
    text = (text or "").strip()
    if not text:
        return False, "Пустой текст не сохраняю."
    lost = missing(key, text)
    if lost:
        names = ", ".join("{" + f + "}" for f in lost)
        return False, (f"Не хватает подстановок: {names}. Без них письмо уйдёт "
                       f"без этих данных — например без суммы. Добавь их в "
                       f"текст и пришли ещё раз.")
    sheets_cache.set_setting(SETTING_PREFIX + key, text)
    logger.info("Шаблон письма %s изменён тренером", key)
    return True, "Записал. Проверь в предпросмотре."


def reset(key: str) -> bool:
    """Возвращает встроенный текст."""
    if key not in TEMPLATES:
        return False
    sheets_cache.set_setting(SETTING_PREFIX + key, "")
    logger.info("Шаблон письма %s возвращён к исходному", key)
    return True


def render(key: str, default: str, **values: Any) -> str:
    """Свой текст с подставленными значениями. Нет своего — отдаём встроенный.

    Пустое значение необязательного поля убирает СТРОКУ целиком, а не оставляет
    дыру: «за тобой  ₽» хуже, чем отсутствие строки."""
    text = custom(key)
    if not text:
        return default
    out: List[str] = []
    for line in text.split("\n"):
        holes = [f for f in fields(key) if ("{" + f + "}") in line]
        if holes and all(not str(values.get(f, "")).strip() for f in holes):
            continue                      # в строке только пустые подстановки
        for field in fields(key):
            line = line.replace("{" + field + "}", str(values.get(field, "")))
        out.append(line)
    return "\n".join(out).strip()
