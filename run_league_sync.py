#!/usr/bin/env python3
"""
Разовый прогон качалки справочников лиг (команды и заявки).

Демон обновляет их сам раз в час — этот скрипт нужен для ручного прогона и для
cron как страховка: если демон лежит, справочники на диске всё равно остаются
свежими, и он поднимется уже с готовыми данными.

ВАЖНО про имена: ФИО живут в оперативной памяти процесса ([[player_names]]),
на диск не попадают. Отдельный процесс наполняет СВОЮ память и завершается,
поэтому демону его имена не достаются — тот подтягивает их сам своим циклом.
Здесь имена скачиваются только чтобы проверить, что заявка читается.

  python run_league_sync.py            # команды + заявки
  python run_league_sync.py --teams    # только справочник команд
"""

import argparse
import asyncio
import logging
import sys

import league_sync
import player_names


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--teams", action="store_true", help="только справочник команд")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s")

    res = await league_sync.refresh(teams_only=args.teams)
    line = (f"Справочники лиг: команд {res['teams']}, в заявках {res['rosters']}, "
            f"ошибок {res['failed']}")
    if not args.teams:
        line += f", имён прочитано {player_names.stats()['count']} (в памяти процесса)"
    print(line)
    for t in league_sync.our_teams():
        print(f"  {t['source']}:{t['team_id']} — {t['name']} ({t['league']}), "
              f"в заявке {len(league_sync.roster_of(t['source'], t['team_id']))}")
    # Ошибка сети — не повод для ненулевого кода: качалка обязана переживать
    # недоступную лигу молча, иначе cron начнёт слать письма на каждый сбой.
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
