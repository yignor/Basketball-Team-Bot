#!/usr/bin/env python3
"""
Отладочный скрипт для проверки парсинга расписания.
Пример BasketStat SLPRO:
  python debug_schedule_parsing.py "https://slpro.basketstat.ru/schedule/2025-2026/SUMC/2/" PUP
"""

import asyncio
import sys

from fallback_game_monitor import FallbackGameMonitor, PLAYWRIGHT_AVAILABLE


async def debug_parse_page(url: str, team_name: str):
    """Отладочный парсинг страницы через FallbackGameMonitor."""
    print(f"🔍 Отладка парсинга {url} для команды '{team_name}'")
    print(f"   Playwright: {'да' if PLAYWRIGHT_AVAILABLE else 'нет'}\n")

    monitor = FallbackGameMonitor()
    team_variants = list(monitor._build_name_variants(team_name))

    import aiohttp

    async with aiohttp.ClientSession() as session:
        games = await monitor._parse_single_page(session, url, team_variants, team_name)

    if not games:
        print("⚠️ Игры не найдены")
        return

    print(f"✅ Найдено {len(games)} игр:\n")
    for i, game in enumerate(games, 1):
        home_away = 'дома' if game.get('is_home') else 'в гостях'
        print(
            f"   {i}. {game.get('date')} {game.get('time')} — "
            f"{game.get('team_name')} vs {game.get('opponent')} ({home_away}), "
            f"площадка: {game.get('venue')}, url: {game.get('url')}"
        )


if __name__ == '__main__':
    url = sys.argv[1] if len(sys.argv) > 1 else 'http://mb-78.ru/'
    team = sys.argv[2] if len(sys.argv) > 2 else 'Titans'
    asyncio.run(debug_parse_page(url, team))
