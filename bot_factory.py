"""Бот с ограничителем скорости — один на весь проект.

Телеграм считает лимиты **на токен**, а не на процесс. У нас же токеном
пользуются полтора десятка мест: демон, монитор игр, SLPRO, фэнтези,
дни рождения, месячные отчёты, сбор голосов. Каждое считало себя единственным
и слало в полную силу.

Пока спасала случайность: трафик идёт через VPN-туннель с обфускацией, и одна
отправка занимает сотни миллисекунд — это невольно держало темп ниже потолка.
На случайность полагаться нельзя, тем более что с ростом числа получателей
рассылка упрётся в лимит первой.

Что происходит при упоре: Телеграм отвечает 429 с полем `retry_after`, PTB
поднимает RetryAfter, цикл рассылки обрывается — и часть людей сообщение
**не получает вовсе**. Не позже, а никогда: повторов в коде нет.

Поэтому здесь одна точка, где бот собирается правильно. `max_retries` задан
явно: по умолчанию в библиотеке он НОЛЬ, то есть ограничитель темп держит, но
при 429 всё равно теряет сообщение — ровно то, ради чего его и ставили.

Числа взяты с запасом от документированных Телеграмом (~30/с суммарно,
~20/мин в группу): потолок — это точка отказа, а не рабочая скорость.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

# С запасом от потолка: 25 вместо 30 и 18 вместо 20. Телеграм называет свои
# числа приблизительными, и упираться в них ровно — значит ловить 429 на
# каждой рассылке.
OVERALL_RATE = float(os.getenv("TG_RATE_OVERALL", "25"))
OVERALL_PERIOD = float(os.getenv("TG_RATE_PERIOD", "1"))
GROUP_RATE = float(os.getenv("TG_RATE_GROUP", "18"))
GROUP_PERIOD = float(os.getenv("TG_RATE_GROUP_PERIOD", "60"))
# Три повтора покрывают обычный retry_after (секунды). Больше смысла нет: если
# лимит держится минутами, дело не в темпе, а в том, что рассылка спланирована
# неудачно.
MAX_RETRIES = int(os.getenv("TG_RATE_RETRIES", "3"))

# Таймауты: трафик идёт через туннель с обфускацией, джиттер добавляет
# задержку, и умолчание httpx (5 с) иногда не успевает.
TIMEOUT = float(os.getenv("TG_TIMEOUT", "20"))

_warned = False


def rate_limiter() -> Optional[Any]:
    """Ограничитель или None, если библиотека не поставлена.

    Не падаем: бот без ограничителя работает ровно как раньше, а деплой без
    зависимости не должен оставлять команду без бота."""
    global _warned
    try:
        from telegram.ext import AIORateLimiter
        return AIORateLimiter(overall_max_rate=OVERALL_RATE,
                              overall_time_period=OVERALL_PERIOD,
                              group_max_rate=GROUP_RATE,
                              group_time_period=GROUP_PERIOD,
                              max_retries=MAX_RETRIES)
    except Exception as exc:
        # Ловим ВСЁ, а не только ImportError: сам класс импортируется всегда, а
        # про отсутствующий aiolimiter он сообщает RuntimeError уже из
        # конструктора. На этом бот и слёг при первом деплое — предохранитель,
        # который не срабатывает, хуже отсутствующего.
        if not _warned:
            logger.warning("Ограничитель скорости не подключён (%s). "
                           "Ставится: pip install aiolimiter", exc)
            _warned = True
        return None


def make_bot(token: str = "") -> Any:
    """Бот для скриптов вне демона — с тем же ограничителем.

    Демон собирает своего через Application.builder().rate_limiter(...); здесь
    то же самое для крон-задач, у каждой из которых свой процесс."""
    from telegram.request import HTTPXRequest

    tok = token or os.getenv("BOT_TOKEN", "")
    limiter = rate_limiter()
    req = lambda: HTTPXRequest(connect_timeout=TIMEOUT, read_timeout=TIMEOUT,
                               write_timeout=TIMEOUT, pool_timeout=TIMEOUT)
    if limiter is None:
        from telegram import Bot
        return Bot(token=tok, request=req(), get_updates_request=req())
    from telegram.ext import ExtBot
    return ExtBot(token=tok, rate_limiter=limiter, request=req(),
                  get_updates_request=req())
