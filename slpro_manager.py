#!/usr/bin/env python3
"""
Оркестратор оповещений по лиге SLPRO (команда Pull Up Farm).

Отдельная лёгкая подсистема: SLPRO — другой источник с другим API, поэтому
НЕ прогоняется через Infobasket-завязанный GameSystemManager. Переиспользует
общие примитивы:
- конфиг чатов/топиков и объект бота — берём у экземпляра GameSystemManager
  (там уже разрешены topic_id/chat_id/BOT_TOKEN), не дублируя логику;
- duplicate_protection (service_records) для дедупа — но с ОТДЕЛЬНЫМИ
  data_type (*_SLPRO), чтобы Infobasket-вотчер/монитор результатов их не
  трогали и не было коллизий по game_id;
- механизм GAME_POLL_REG (Фаза C) — тот же data_type, чтобы демон собирал
  голоса, а game_report.py строил отчёты (они source-agnostic).

game_id внутри записей — с префиксом slpro-<id> для читаемости.
"""

import asyncio
import datetime
import json
import os
from typing import Any, Dict, List, Optional, Tuple

from enhanced_duplicate_protection import duplicate_protection
from game_system_manager import (
    GameSystemManager,
    get_chat_ids_for_automation,
    get_day_of_week,
    format_date_without_year,
    AUTOMATION_KEY_GAME_POLLS,
    AUTOMATION_KEY_GAME_ANNOUNCEMENTS,
)
from slpro_client import SlproClient
from slpro_game import parse_box_score, format_quarters, format_leaders
from datetime_utils import get_moscow_time, is_within_game_tracking_window

# Отдельные типы записей, чтобы Infobasket-конвейер (game_watcher.py,
# game_results_monitor_final.py — ищут АНОНС_ИГРА и пикают fbp.ru) их не касался.
DT_POLL = "ОПРОС_ИГРА_SLPRO"
DT_ANNOUNCE = "АНОНС_ИГРА_SLPRO"
DT_RESULT = "РЕЗУЛЬТАТ_ИГРА_SLPRO"

POLL_OPTIONS = ["✅ Готов", "❌ Нет", "👨‍🏫 Тренер"]

# Имена нашей команды в SLPRO (для автоопределения сезона). Переопределяются
# без правки кода через env SLPRO_TEAM_NAMES (имена через запятую).
DEFAULT_TEAM_NAMES = ["PullUp Farm", "Pull Up Farm", "Pull-Up Farm"]


def _team_names_from_env() -> List[str]:
    raw = os.getenv("SLPRO_TEAM_NAMES", "").strip()
    if not raw:
        return DEFAULT_TEAM_NAMES
    return [n.strip() for n in raw.split(",") if n.strip()]


class SlproManager:
    def __init__(self, team_names: Optional[List[str]] = None):
        self.client = SlproClient()
        self.team_names = team_names or _team_names_from_env()
        # Экземпляр GameSystemManager нужен только как источник разрешённого
        # конфига (бот, топики, chat_id) — Infobasket-методы не вызываем.
        self.gsm = GameSystemManager()
        self.bot = self.gsm.bot

    # ── Вспомогательное ──────────────────────────────────────────────────────

    @staticmethod
    def _slpro_game_id(game: Dict[str, Any]) -> str:
        return f"slpro-{game.get('game_id')}"

    @staticmethod
    def _date_ddmmyyyy(iso_date: str) -> str:
        try:
            return datetime.datetime.strptime(iso_date, "%Y-%m-%d").strftime("%d.%m.%Y")
        except (ValueError, TypeError):
            return iso_date or ""

    @staticmethod
    def _time_hhmm(t: str) -> str:
        return (t or "")[:5]

    def _our_side(self, game: Dict[str, Any], team_id: int) -> Tuple[str, str, Any, Any, bool]:
        """(наши, соперник, счёт_наши, счёт_соперник, дома_ли)."""
        if game.get("home_id") == team_id:
            return game.get("home_name", ""), game.get("guest_name", ""), \
                game.get("home_score"), game.get("guest_score"), True
        return game.get("guest_name", ""), game.get("home_name", ""), \
            game.get("guest_score"), game.get("home_score"), False

    def _poll_chat_ids(self) -> List[str]:
        entry = self.gsm._get_automation_entry(AUTOMATION_KEY_GAME_POLLS)
        return get_chat_ids_for_automation(AUTOMATION_KEY_GAME_POLLS, entry)

    def _announce_chat_ids(self) -> List[str]:
        entry = self.gsm._get_automation_entry(AUTOMATION_KEY_GAME_ANNOUNCEMENTS)
        return get_chat_ids_for_automation(AUTOMATION_KEY_GAME_ANNOUNCEMENTS, entry)

    async def _send_to_chats(self, chat_ids: List[str], text: str,
                             topic_id: Optional[int]) -> List[Any]:
        """Отправка текста в несколько чатов с деградацией топика (как в
        существующих методах)."""
        bot = self.bot
        messages: List[Any] = []
        for chat_id in chat_ids:
            cid = self.gsm._to_int(chat_id) or chat_id
            kwargs: Dict[str, Any] = {"chat_id": cid, "text": text, "parse_mode": "HTML"}
            if topic_id is not None:
                kwargs["message_thread_id"] = topic_id
            try:
                messages.append(await bot.send_message(**kwargs))
                print(f"✅ SLPRO: отправлено в чат {chat_id}")
            except Exception as e:
                if topic_id is not None and "Message thread not found" in str(e):
                    kwargs.pop("message_thread_id", None)
                    messages.append(await bot.send_message(**kwargs))
                    print(f"✅ SLPRO: отправлено в чат {chat_id} (без топика)")
                else:
                    print(f"❌ SLPRO: ошибка отправки в чат {chat_id}: {e}")
        return messages

    # ── Опрос готовности ─────────────────────────────────────────────────────

    async def create_poll(self, game: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
        sid = self._slpro_game_id(game)
        team_id = ctx["team_id"]
        our, opp, _, _, is_home = self._our_side(game, team_id)
        date_ddmm = self._date_ddmmyyyy(game["game_date"])
        time_hhmm = self._time_hhmm(game["game_time"])
        opp_id = game.get("guest_id") if is_home else game.get("home_id")

        # Дедуп с детекцией изменений: админы лиги переносят игры и меняют
        # соперника прямо в том же матче. Если слот или соперник отличаются от
        # того, по чему опрашивали, старый опрос неактуален — нужен новый.
        existing = duplicate_protection.get_game_record(DT_POLL, sid)
        change_note = ""
        if existing:
            old_slot = f"{existing.get('game_date', '')} {(existing.get('game_time') or '')[:5]}".strip()
            new_slot = f"{game['game_date']} {time_hhmm}".strip()
            old_opp = str(existing.get("team_b_id") or "")
            slot_changed = old_slot != new_slot
            opp_changed = bool(old_opp) and old_opp != str(opp_id or "")
            if not slot_changed and not opp_changed:
                print(f"⏭️ SLPRO: опрос для {sid} уже создан")
                return False
            bits = []
            if opp_changed:
                bits.append("сменился соперник")
            if slot_changed:
                bits.append(f"перенос на {format_date_without_year(date_ddmm)}, "
                            f"{get_day_of_week(date_ddmm)}, {time_hhmm}")
            change_note = "⚠️ Игра изменилась (" + ", ".join(bits) + "). Обновлённый опрос:"
            # Снимаем старые регистрации голосов (иначе голоса со снятого опроса
            # считались бы) и саму дедуп-запись опроса — уникальные индексы
            # частичные (deleted=0), поэтому без этого add_record не перезапишет
            # дату/соперника и на следующем прогоне снова сработала бы «смена».
            removed = duplicate_protection.deactivate_records_by_prefix(
                "GAME_POLL_REG", f"GAME_POLL_REG_GPOLL_{sid}_")
            duplicate_protection.deactivate_game_record(DT_POLL, sid)
            print(f"🔄 SLPRO: игра {sid} изменилась ({', '.join(bits)}), "
                  f"снято старых регистраций: {removed}")

        chat_ids = self._poll_chat_ids()
        if not chat_ids:
            print("❌ SLPRO: не настроены чаты для опросов")
            return False

        form = "светлая" if is_home else "тёмная"
        question = (
            f"🏀 {our} против {opp} (SLPRO)\n"
            f"📅 {format_date_without_year(date_ddmm)}, {get_day_of_week(date_ddmm)}, {time_hhmm}\n"
            f"👕 {form} форма\n"
            f"📍 {game.get('game_address', '')}"
        )

        bot = self.bot
        topic_id = self.gsm.game_poll_topic_id
        # Если игра менялась — предупреждаем чат перед новым опросом.
        if change_note:
            await self._send_to_chats(chat_ids, change_note, topic_id)
        poll_messages = []
        for chat_id in chat_ids:
            kwargs: Dict[str, Any] = {
                "chat_id": self.gsm._to_int(chat_id) or chat_id,
                "question": question,
                "options": POLL_OPTIONS,
                "is_anonymous": self.gsm.game_poll_is_anonymous,
                "allows_multiple_answers": self.gsm.game_poll_allows_multiple,
            }
            if topic_id is not None:
                kwargs["message_thread_id"] = topic_id
            try:
                poll_messages.append(await bot.send_poll(**kwargs))
                print(f"✅ SLPRO: опрос отправлен в чат {chat_id}")
            except Exception as e:
                if topic_id is not None and "Message thread not found" in str(e):
                    kwargs.pop("message_thread_id", None)
                    poll_messages.append(await bot.send_poll(**kwargs))
                    print(f"✅ SLPRO: опрос отправлен в чат {chat_id} (без топика)")
                else:
                    print(f"❌ SLPRO: ошибка отправки опроса в чат {chat_id}: {e}")

        if not poll_messages:
            return False

        game_date_iso = game["game_date"]
        # Регистрируем каждый опрос для сбора голосов (тот же механизм, что и
        # у основной команды — Фаза C). game_id только внутри JSON.
        for pm in poll_messages:
            tg_poll_id = pm.poll.id if pm.poll else None
            if not tg_poll_id:
                continue
            duplicate_protection.add_record(
                "GAME_POLL_REG",
                f"GPOLL_{sid}_{pm.message_id}",
                status="АКТИВЕН",
                additional_data=json.dumps({
                    "tg_poll_id": tg_poll_id,
                    "options": POLL_OPTIONS,
                    "chat_id": pm.chat.id if pm.chat else None,
                    "message_id": pm.message_id,
                    "game_id": sid,
                }, ensure_ascii=False),
                alt_name=sid,
                game_date=game_date_iso,
            )
        print(f"   📋 SLPRO: зарегистрировано опросов: {len(poll_messages)}")

        # Дедуп-запись опроса. При переопросе прежнюю запись мы сняли выше, так
        # что здесь вставляется свежая — с новыми датой/временем/соперником, и
        # повторный перенос той же игры снова будет распознан.
        duplicate_protection.add_record(
            DT_POLL, sid, status="ОПРОС СОЗДАН",
            additional_data=question,
            alt_name=ctx.get("team_name", ""),
            game_id=sid, game_date=game_date_iso,
            game_time=time_hhmm, arena=game.get("game_address", ""),
            team_a_id=team_id, team_b_id=opp_id,
        )
        return True

    # ── Анонс ────────────────────────────────────────────────────────────────

    async def send_announcement(self, game: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
        sid = self._slpro_game_id(game)
        if duplicate_protection.get_game_record(DT_ANNOUNCE, sid):
            print(f"⏭️ SLPRO: анонс для {sid} уже отправлен")
            return False

        chat_ids = self._announce_chat_ids()
        if not chat_ids:
            print("❌ SLPRO: не настроены чаты для анонсов")
            return False

        team_id = ctx["team_id"]
        our, opp, _, _, is_home = self._our_side(game, team_id)
        date_ddmm = self._date_ddmmyyyy(game["game_date"])
        time_hhmm = self._time_hhmm(game["game_time"])
        form = "светлая" if is_home else "тёмная"
        text = (
            f"🏀 Сегодня игра (SLPRO): <b>{our}</b> против <b>{opp}</b>\n"
            f"🕐 {time_hhmm}\n"
            f"👕 {form} форма\n"
            f"📍 {game.get('game_address', '')}"
        )

        messages = await self._send_to_chats(chat_ids, text, self.gsm.game_announcement_topic_id)
        if not messages:
            return False

        duplicate_protection.add_record(
            DT_ANNOUNCE, sid, status="АНОНС ОТПРАВЛЕН",
            additional_data=f"{date_ddmm} {time_hhmm} {our} vs {opp}",
            alt_name=ctx.get("team_name", ""),
            game_id=sid, game_date=game["game_date"],
            game_time=time_hhmm, arena=game.get("game_address", ""),
        )
        return True

    # ── Результат ────────────────────────────────────────────────────────────

    async def post_result(self, game: Dict[str, Any], ctx: Dict[str, Any]) -> bool:
        sid = self._slpro_game_id(game)
        if duplicate_protection.get_game_record(DT_RESULT, sid):
            print(f"⏭️ SLPRO: результат для {sid} уже опубликован")
            return False

        team_id = ctx["team_id"]
        our, opp, our_score, opp_score, _ = self._our_side(game, team_id)
        try:
            our_s, opp_s = int(our_score), int(opp_score)
        except (TypeError, ValueError):
            print(f"⚠️ SLPRO: нет счёта для {sid}, пропускаю результат")
            return False

        if our_s > opp_s:
            emoji, result_text = "✅", "ПОБЕДА"
        elif our_s < opp_s:
            emoji, result_text = "❌", "ПОРАЖЕНИЕ"
        else:
            emoji, result_text = "🤝", "НИЧЬЯ"

        date_ddmm = self._date_ddmmyyyy(game["game_date"])
        lines = [
            f"{emoji} {result_text}: {our} против {opp}",
            f"🏀 {our} {our_s}:{opp_s} {opp}",
        ]

        # Детальный box-score (четверти + MVP/лидеры + видео VK). Если не
        # получилось — не критично, публикуем результат по счёту.
        try:
            resp = await self.client.get_game(game.get("game_id"), ctx)
            box = parse_box_score(resp) if resp else None
        except Exception as e:
            print(f"⚠️ SLPRO: не удалось получить box-score {sid}: {e}")
            box = None
        if box:
            # Игра завершена и уже скачана — кладём в локальную копию, чтобы
            # бэкфилл и аналитика не ходили за ней в чужой API повторно.
            try:
                import fantasy_stats
                fantasy_stats.store_slpro_box(box, str(ctx.get("season_id") or ""),
                                              ctx.get("stage_id") or "")
            except Exception as e:
                print(f"⚠️ SLPRO: box-score {sid} не сохранён в базу: {e}")

            quarters = format_quarters(box, team_id)
            if quarters:
                lines.append(f"📈 Четверти: {quarters}")
            leaders = format_leaders(box, team_id)
            if leaders:
                lines.append(leaders)
            if box.video_vk:
                lines.append(f"📹 Видео: {box.video_vk}")

        lines.append(f"📅 {date_ddmm}")
        lines.append(f"🏀 SLPRO · {ctx.get('division_name', '')}")
        text = "\n".join(lines)
        # Результат публикуем в чат анонсов (или опросов — тот же общий чат).
        chat_ids = self._announce_chat_ids() or self._poll_chat_ids()
        messages = await self._send_to_chats(chat_ids, text, self.gsm.game_announcement_topic_id)
        if not messages:
            return False

        duplicate_protection.add_record(
            DT_RESULT, sid, status="ОТПРАВЛЕН",
            additional_data=f"{our} {our_s}:{opp_s} {opp}",
            alt_name=ctx.get("team_name", ""),
            game_id=sid, game_date=game["game_date"],
            game_time=self._time_hhmm(game["game_time"]),
            arena=game.get("game_address", ""),
        )
        return True

    # ── Оркестрация ──────────────────────────────────────────────────────────

    async def run(self, only: Optional[str] = None) -> None:
        print("🚀 SLPRO: запуск мониторинга (команда Pull Up Farm)")
        ctx = await self.client.discover_context(self.team_names)
        if not ctx or ctx.get("team_id") is None:
            print(f"⚠️ SLPRO: команда {self.team_names} не найдена в активных стадиях")
            return
        print(f"🔎 SLPRO: {ctx['team_name']} — сезон {ctx.get('season')}, "
              f"{ctx.get('division_name')} (team_id={ctx['team_id']}, stage_id={ctx['stage_id']})")

        games = await self.client.get_our_games(ctx)
        if not games:
            print("⚠️ SLPRO: игр нашей команды не найдено")
            return

        today = get_moscow_time().date()
        polls = announces = results = 0

        # Опросы/анонсы — не раньше настроенного времени (тот же столбец
        # «Время (МСК)» в Конфиге, что и у основной команды), чтобы cron рано
        # утром не публиковал раньше срока. Результаты НЕ гейтим — их надо
        # публиковать сразу по завершении игры.
        polls_time_ok = self.gsm._notify_time_reached(AUTOMATION_KEY_GAME_POLLS)
        announce_time_ok = self.gsm._notify_time_reached(AUTOMATION_KEY_GAME_ANNOUNCEMENTS)

        for game in games:
            try:
                gdate = datetime.datetime.strptime(game["game_date"], "%Y-%m-%d").date()
            except (ValueError, KeyError):
                continue

            status = game.get("status")
            if status == 2:
                # Завершена — публикуем результат, пока в окне отслеживания.
                if only in (None, "results") and is_within_game_tracking_window(
                        self._date_ddmmyyyy(game["game_date"]), self._time_hhmm(game["game_time"])):
                    if await self.post_result(game, ctx):
                        results += 1
            elif status == 0:
                if gdate > today:
                    if only in (None, "polls") and polls_time_ok and await self.create_poll(game, ctx):
                        polls += 1
                elif gdate == today:
                    if only in (None, "announcements") and announce_time_ok and await self.send_announcement(game, ctx):
                        announces += 1

        print(f"\n📊 SLPRO ИТОГИ: опросов {polls}, анонсов {announces}, результатов {results}")


async def main(only: Optional[str] = None) -> None:
    manager = SlproManager()
    await manager.run(only=only)


if __name__ == "__main__":
    asyncio.run(main())
