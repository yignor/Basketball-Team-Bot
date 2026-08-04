#!/usr/bin/env python3
"""
Оркестрация фэнтези-лиги (cron/админка).

--only ingest    — только выкачать статистику завершённых игр в кеш
--only weekly    — только понедельничный подсчёт + рассылка таблицы
--only schedule  — пересчёт окна набора (блокировка/открытие по расписанию)
(без флага — сначала ingest, затем weekly)

Понедельничная рассылка: считает очки за ПРОШЕДШУЮ неделю, блокирует её
составы, шлёт таблицу в общий чат и в личку каждому участнику.
"""

import argparse
import asyncio
import os
import time
from datetime import timedelta
from typing import Any, List, Optional

from telegram import KeyboardButton, ReplyKeyboardMarkup, WebAppInfo

from datetime_utils import get_moscow_time
import fantasy
import fantasy_stats
from slpro_client import SlproClient


def _fantasy_webapp_base() -> Optional[str]:
    """URL статики Mini App (GitHub Pages). None — фронт не настроен, кнопку
    запасного входа не вешаем. Данные поедут в #d=, не в query."""
    url = os.getenv("FANTASY_WEBAPP_URL", "").strip()
    return url or None


def _fantasy_reply_markup(base_url: str, payload: str) -> ReplyKeyboardMarkup:
    """Постоянная кнопка запасного входа: статика на Pages + данные в #d=.
    web_app (а не url) — чтобы из неё работал sendData и сохранение состава
    уходило боту в обход живого API. ?v= бьёт HTTP-кеш страницы, ?api= —
    переопределение адреса из env (обычно пусто: фронт идёт на Funnel)."""
    import fantasy_api
    sep = "&" if "?" in base_url else "?"
    url = f"{base_url}{sep}v={int(time.time())}"
    api = fantasy_api.public_api_url()
    if api:
        from urllib.parse import quote
        url += "&api=" + quote(api, safe="")
    url += f"#d={payload}"
    # Раскладка та же, что у бота в /start: понедельничная рассылка не должна
    # подменять человеку клавиатуру на «только фэнтези».
    return ReplyKeyboardMarkup(
        [[KeyboardButton("🏀 Фэнтези", web_app=WebAppInfo(url=url))],
         [KeyboardButton("💬 Написать админам")]],
        resize_keyboard=True, is_persistent=True)


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
        import slpro_client
        contexts = await slpro_client.team_contexts()
        if not contexts:
            print("⚠️ Фэнтези ingest: турниров SLPRO не найдено "
                  "(строки ТИП=SLPRO в листе «Конфиг»)")
            return 0
        n = 0
        for ctx in contexts:
            if not ctx.get("team_id"):
                continue
            n += await fantasy_stats.ingest_slpro(self.client, ctx)
        print(f"📥 Фэнтези ingest: новых игр выкачано {n} "
              f"(турниров {len(contexts)})")
        # TODO(F1): ingest основы (Infobasket) по связке ID.
        return n

    async def weekly(self) -> bool:
        seasons = fantasy.active_seasons()
        if not seasons:
            print("ℹ️ Фэнтези weekly: активных сезонов нет, пропускаю")
            return False

        # Прошедшая неделя (при запуске в понедельник — прошлый пн–вс).
        today = get_moscow_time().date()
        report_week = (fantasy.week_start_of(today) - timedelta(days=7)).isoformat()

        # Свежая статистика перед подсчётом — один раз на все лиги.
        await self.ingest()

        single = len(seasons) == 1
        chat_ids = self._get_chat_ids(self._ann_key, self.gsm._get_automation_entry(self._ann_key))
        topic = self.gsm.game_announcement_topic_id
        for season in seasons:
            print(f"🏆 Фэнтези weekly: «{season['name']}», неделя {report_week}")
            fantasy.lock_week(season["id"], report_week)
            fantasy.save_weekly_scores(season["id"], report_week)
            text = fantasy.format_weekly_table(season["id"], report_week)
            if not single:
                text = f"🏆 {season['name']}\n{text}"
            await self._send_to_chats(chat_ids, text, topic)
            # Не только те, кто пересобирал состав на этой неделе: у кого он
            # перенёсся с прошлой, очки тоже идут — и таблица им нужна.
            participants = [str(r["user_id"]) for r in
                            fantasy.weekly_standings(season["id"], report_week)]
            # Кто отписался в «Меню → Мои подписки», тому в личку не шлём:
            # в общий чат таблица всё равно ушла, и она никуда не денется.
            import subscriptions
            participants = subscriptions.filter_subscribed(participants, "fantasy")
            sent_dm = await self._send_dms(participants, text, with_fantasy_button=True)
            print(f"📊 «{season['name']}»: таблица в {len(chat_ids)} чат(ов), личка {sent_dm}/{len(participants)}")
            personal = await self._send_personal(season, report_week, participants)
            print(f"📨 «{season['name']}»: личных разбивок по играм — {personal}")
        return True

    async def _send_personal(self, season: Any, week: str,
                             participants: List[str]) -> int:
        """Каждому — разбивка по играм недели: сколько принёс его состав и
        сколько он набрал сам.

        Шлём не только участникам: человек мог не собирать состав, но играть —
        его очки как игрока тоже интересны. А кто не делал ни того, ни
        другого, сообщения не получит вовсе (format_weekly_personal вернёт
        пустую строку)."""
        import player_identity
        import subscriptions
        who = list(dict.fromkeys(list(participants) + player_identity.linked_users()))
        who = subscriptions.filter_subscribed(who, "fantasy")
        sent = 0
        for uid in who:
            try:
                text = fantasy.format_weekly_personal(season["id"], week, uid)
            except Exception as e:
                print(f"⚠️ Личная разбивка для {uid} не собралась: {e}")
                continue
            if not text:
                continue
            if await self._send_dms([uid], text):
                sent += 1
        return sent

    async def _refresh_auto_scopes(self, season: Any) -> None:
        """Обновляет «лиги, в которых команда играет сейчас» — SLPRO активная
        стадия + comp_id Инфобаскета из Конфига. Это дефолт для подсчёта очков,
        когда админ не выбрал турниры вручную (fantasy.effective_scopes)."""
        scopes: List[dict] = []
        try:
            import slpro_client
            for ctx in await slpro_client.team_contexts():
                if ctx.get("stage_id") is not None:
                    scopes.append(slpro_client.scope_of(ctx))
        except Exception as e:
            print(f"⚠️ auto-scope SLPRO: {e}")
        for comp in (self.gsm.config_comp_ids or []):
            scopes.append({"source": "infobasket", "season_id": str(comp),
                           "name": f"Инфобаскет comp {comp}"})
        if scopes:
            fantasy.set_auto_scopes(scopes, season["id"])
            print(f"🎯 Авто-лиги подсчёта: {fantasy.scopes_title(scopes)}")

    async def schedule(self) -> bool:
        """Пересчитывает окно набора КАЖДОГО активного сезона (их может быть
        несколько — параллельные лиги). Закрывает набор на первом анонсе недели,
        открывает следующий тур после статистики последней игры."""
        import fantasy_schedule
        seasons = fantasy.active_seasons()
        if not seasons:
            return False
        # Авто-лиги ставим только если сезон один; при нескольких параллельных
        # турниры у каждого свои — админ задаёт их явно.
        single = len(seasons) == 1
        announce_hhmm = str(
            (self.gsm._get_automation_entry(self._ann_key) or {}).get("notify_time")
            or fantasy_schedule.DEFAULT_ANNOUNCE_HHMM
        )
        for season in seasons:
            if single:
                await self._refresh_auto_scopes(season)
            # Рассылок о блокировке/открытии нет: состав закрыт ровно на время
            # игры, игрок узнаёт об этом при попытке его сменить, а об открытии
            # — из сообщения с результатом.
            state, _ = fantasy_schedule.tick(announce_hhmm=announce_hhmm, season=season)
            print(f"🗓️ Фэнтези «{season['name']}»: неделя {state.get('active_week')}, "
                  f"заблокирован={state.get('locked')} (игра {state.get('locked_game')})")
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

    async def _send_dms(self, user_ids: List[str], text: str,
                        with_fantasy_button: bool = False) -> int:
        if not self.bot:
            return 0
        # Свежую кнопку запасного входа считаем ОДИН раз (пул/таблица общие),
        # у каждого игрока меняется только его состав. Так офлайн-игроки на
        # открытии набора и в понедельник получают актуальный payload.
        shared, markup_url = None, _fantasy_webapp_base()
        if with_fantasy_button and markup_url:
            try:
                import fantasy_api
                shared = await fantasy_api.webapp_shared()
            except Exception as e:
                print(f"⚠️ Фэнтези: не собрать кнопку запасного входа: {e}")
        sent = 0
        for uid in user_ids:
            markup = None
            if shared:
                try:
                    import fantasy_api
                    payload = fantasy_api.encode_webapp_payload(shared, str(uid))
                    markup = _fantasy_reply_markup(markup_url, payload)
                except Exception:
                    markup = None
            try:
                await self.bot.send_message(chat_id=int(uid), text=text, reply_markup=markup)
                sent += 1
            except Exception as e:
                # Пользователь мог не запускать бота / заблокировать — не критично.
                print(f"⚠️ Фэнтези: не удалось отправить в личку {uid}: {e}")
        return sent

    async def run(self, only: Optional[str] = None) -> None:
        if only in (None, "ingest"):
            await self.ingest()
        if only == "schedule":
            await self.schedule()
        if only in (None, "weekly"):
            await self.weekly()


async def main(only: Optional[str]) -> None:
    runner = FantasyRunner()
    await runner.run(only=only)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", choices=["ingest", "weekly", "schedule"], default=None)
    args = ap.parse_args()
    asyncio.run(main(args.only))
