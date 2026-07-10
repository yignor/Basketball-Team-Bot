#!/usr/bin/env python3
"""
Разовое восстановление опроса по конкретной игре основной команды.

Нужно, когда лига перенесла игру (или сменила соперника), бот прислал
уведомление об изменении, но опрос по новой дате не создал (баг до коммита,
где _process_future_game стал переопрашивать). Запись об игре при этом уже
переставлена на новую дату, поэтому обычный прогон её пропускает.

Скрипт снимает «пустую» опрос-запись и старые регистрации голосов, затем
создаёт свежий опрос по актуальной дате, минуя гейт времени (чтобы можно
было отправить не в 09:00).

    python recover_game_poll.py <game_id>

Запускать ОТ botuser: нужен доступ к .env (BOT_TOKEN) и запись в data/bot.db.
Реально отправляет опрос в командный чат — это осознанное действие.
"""

import asyncio
import sys

from enhanced_duplicate_protection import duplicate_protection as dp
from game_system_manager import GameSystemManager


async def main(game_id: str) -> int:
    r1 = dp.deactivate_records_by_prefix("GAME_POLL_REG", f"GAME_POLL_REG_GPOLL_{game_id}_")
    r2 = dp.deactivate_game_record("ОПРОС_ИГРА", str(game_id))
    print(f"снято старых регистраций голосов: {r1}, опрос-записей: {r2}")

    manager = GameSystemManager()
    manager._is_correct_time_for_polls = lambda: True  # разовый обход гейта времени

    games = await manager.fetch_infobasket_schedule()
    target = [g for g in games.get("future", []) if str(g.get("game_id")) == str(game_id)]
    print(f"игра {game_id} среди будущих: найдено {len(target)}")
    if not target:
        print("⚠️ Игра не в списке будущих (перенесена в прошлое или снята) — опрос не создан.")
        return 1

    ok = await manager._process_future_game(target[0])
    print(f"опрос создан: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: recover_game_poll.py <game_id>")
        sys.exit(2)
    sys.exit(asyncio.run(main(sys.argv[1])))
