#!/usr/bin/env python3
"""
Лёгкий, не зависящий от GameSystemManager запрос статуса игры — для
адаптивного вотчера результатов (game_watcher.py) внутри bot_daemon.py.

Только чтение GameStatus/IsOnline/номера периода из того же эндпоинта
Widget/GetOnline, что уже используется в проекте (см.
game_system_manager.py:fetch_widget_game_details,
enhanced_game_parser.py:get_game_data_from_api) — НЕ парсит команды/счёт/
результат (этим занимается тяжёлый enhanced_game_parser.py, вызываемый уже
существующим game_results_monitor_final.py при фактической публикации).
"""

from dataclasses import dataclass

DEFAULT_API_BASE = "https://reg.infobasket.su"
PEEK_TIMEOUT_SECONDS = 10


@dataclass
class GamePeek:
    game_id: str
    game_status: int   # 0 = не начата/идёт, 1 = завершена (см. enhanced_game_parser.py)
    is_online: bool
    max_period: int     # 0, если данных о периодах ещё нет
    fetched_ok: bool     # False = сетевая ошибка/не 200 — отличать от "игра не началась"


async def peek_game(game_id: str, api_url: str = DEFAULT_API_BASE) -> GamePeek:
    import aiohttp

    url = f"{api_url}/Widget/GetOnline/{game_id}?format=json&lang=ru"
    try:
        timeout = aiohttp.ClientTimeout(total=PEEK_TIMEOUT_SECONDS)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    return GamePeek(game_id, 0, False, 0, fetched_ok=False)
                data = await response.json()
    except Exception:
        return GamePeek(game_id, 0, False, 0, fetched_ok=False)

    status = data.get("GameStatus", 0)
    is_online = bool(data.get("IsOnline", False))
    periods = data.get("OnlinePeriods") or []
    max_period = max((p.get("Period", 0) or 0 for p in periods), default=0)
    return GamePeek(game_id, status, is_online, max_period, fetched_ok=True)
