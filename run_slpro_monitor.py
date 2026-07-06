#!/usr/bin/env python3
"""
CLI-вход мониторинга лиги SLPRO (команда Pull Up Farm).

Без флага — полный прогон (опросы будущих игр + анонсы сегодняшних +
результаты завершённых). Так его вызывает cron.

--only polls          — только опросы по будущим играм
--only announcements  — только анонсы сегодняшних игр
--only results        — только публикация результатов завершённых игр
"""

import argparse
import asyncio

from slpro_manager import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["polls", "announcements", "results"], default=None,
                        help="Запустить только один шаг вместо полного прогона")
    args = parser.parse_args()
    asyncio.run(main(args.only))
