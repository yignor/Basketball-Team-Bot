#!/usr/bin/env python3
"""
Скрипт для запуска единой системы управления играми
Выполняет последовательно: парсинг → создание опросов → создание анонсов

--only polls          — только создание опросов по будущим играм
--only announcements  — только анонсы сегодняшних игр
(без флага — полный прогон, как раньше; именно так его вызывает cron)
"""

import argparse
import asyncio
from typing import Optional
from game_system_manager import GameSystemManager


async def main(only: Optional[str]) -> None:
    manager = GameSystemManager()
    await manager.run_full_system(only=only)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["polls", "announcements"], default=None,
                        help="Запустить только один шаг вместо полного прогона")
    args = parser.parse_args()
    asyncio.run(main(args.only))
