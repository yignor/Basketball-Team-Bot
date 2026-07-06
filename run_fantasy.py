#!/usr/bin/env python3
"""
Оркестрация фэнтези-лиги (cron/админка).

--only ingest   — только выкачать статистику завершённых игр в кеш
--only weekly   — только понедельничный подсчёт + рассылка таблицы
(без флага — сначала ingest, затем weekly)

Понедельничная рассылка: считает очки за ПРОШЕДШУЮ неделю, блокирует её
составы, шлёт таблицу в общий чат и в личку каждому участнику.
"""

import argparse
import asyncio
from datetime import timedelta
from typing import Any, List, Optional

from datetime_utils import get_moscow_time
import fantasy
import fantasy_stats
from slpro_client import SlproClient


class FantasyRunner:
    def __init__(self):
        # GameSystemManager — источник разрешённого конфига (бот, чат, топик),
        # как в slpro_manager; Infobasket-методы не вызываем.
        from game_system_manager import (
            GameSystemManager, get_chat_ids_for_automation,
            AUTOMATION_KEY_GAME_ANNOUNCEMENTS,
        )
        self._get_chat_ids = get_chat_ids_for_automation
        self._ann_key = AUTOMATION_KEY_GAME_ANNOUNCEMENTS
        self.gsm = GameSystemManager()
        self.bot = self.gsm.bot
        self.client = SlproClient()

    async def ingest(self) -> int:
        team_names = fantasy_env_team_names()
        ctx = await self.client.discover_context(team_names)
        if not ctx or not ctx.get("team_id"):
            print("⚠️ Фэнтези ingest: команда не найдена в активных стадиях")
            return 0
        n = await fantasy_stats.ingest_slpro(self.client, ctx)
        print(f"📥 Фэнтези ingest: новых игр выкачано {n}")
        # TODO(F1): ingest основы (Infobasket) по связке ID.
        return n

    async def weekly(self) -> bool:
        season = fantasy.get_active_season()
        if not season:
            print("ℹ️ Фэнтези weekly: активного сезона нет, пропускаю")
            return False

        # Прошедшая неделя (при запуске в понедельник — прошлый пн–вс).
        today = get_moscow_time().date()
        report_week = (fantasy.week_start_of(today) - timedelta(days=7)).isoformat()
        print(f"🏆 Фэнтези weekly: сезон «{season['name']}», отчётная неделя {report_week}")

        # Свежая статистика перед подсчётом.
        await self.ingest()

        fantasy.lock_week(season["id"], report_week)
        fantasy.save_weekly_scores(season["id"], report_week)
        text = fantasy.format_weekly_table(season["id"], report_week)

        # Общий чат
        chat_ids = self._get_chat_ids(self._ann_key, self.gsm._get_automation_entry(self._ann_key))
        topic = self.gsm.game_announcement_topic_id
        await self._send_to_chats(chat_ids, text, topic)

        # Личка участникам недели
        participants = [r["user_id"] for r in fantasy.get_week_rosters(season["id"], report_week)]
        sent_dm = await self._send_dms(participants, text)
        print(f"📊 Фэнтези weekly: таблица в {len(chat_ids)} чат(ов), в личку {sent_dm}/{len(participants)}")
        return True

    async def _send_to_chats(self, chat_ids: List[str], text: str, topic: Optional[int]) -> None:
        if not self.bot:
            print("⚠️ Бот не настроен")
            return
        for chat_id in chat_ids:
            cid = self.gsm._to_int(chat_id) or chat_id
            kwargs: dict = {"chat_id": cid, "text": text}
            if topic is not None:
                kwargs["message_thread_id"] = topic
            try:
                await self.bot.send_message(**kwargs)
                print(f"✅ Фэнтези: таблица отправлена в чат {chat_id}")
            except Exception as e:
                if topic is not None and "Message thread not found" in str(e):
                    kwargs.pop("message_thread_id", None)
                    await self.bot.send_message(**kwargs)
                else:
                    print(f"❌ Фэнтези: ошибка отправки в чат {chat_id}: {e}")

    async def _send_dms(self, user_ids: List[str], text: str) -> int:
        if not self.bot:
            return 0
        sent = 0
        for uid in user_ids:
            try:
                await self.bot.send_message(chat_id=int(uid), text=text)
                sent += 1
            except Exception as e:
                # Пользователь мог не запускать бота / заблокировать — не критично.
                print(f"⚠️ Фэнтези: не удалось отправить в личку {uid}: {e}")
        return sent

    async def run(self, only: Optional[str] = None) -> None:
        if only in (None, "ingest"):
            await self.ingest()
        if only in (None, "weekly"):
            await self.weekly()


def fantasy_env_team_names() -> List[str]:
    import os
    raw = os.getenv("SLPRO_TEAM_NAMES", "PullUp Farm,Pull Up Farm")
    return [n.strip() for n in raw.split(",") if n.strip()]


async def main(only: Optional[str]) -> None:
    runner = FantasyRunner()
    await runner.run(only=only)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["ingest", "weekly"], default=None)
    args = ap.parse_args()
    asyncio.run(main(args.only))
