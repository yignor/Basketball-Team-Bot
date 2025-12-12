#!/usr/bin/env python3
"""Создание голосований по конфигурации из Google Sheets."""

import asyncio
import datetime as dt
import json
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, cast

from dotenv import load_dotenv
from telegram import Bot
from telegram.error import TelegramError

from datetime_utils import get_moscow_time
from enhanced_duplicate_protection import duplicate_protection

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
PLACEHOLDER_PATTERN = re.compile(r"\[([^\]]+)\]")
AUTOMATION_VOTING_KEY = "VOTING_POLLS"

WEEKDAY_ALIASES: Dict[str, int] = {
    "0": 0,
    "1": 1,
    "2": 2,
    "3": 3,
    "4": 4,
    "5": 5,
    "6": 6,
    "mon": 0,
    "monday": 0,
    "понедельник": 0,
    "понед": 0,
    "пн": 0,
    "tue": 1,
    "tuesday": 1,
    "вторник": 1,
    "вт": 1,
    "wed": 2,
    "wednesday": 2,
    "среда": 2,
    "ср": 2,
    "thu": 3,
    "thur": 3,
    "thursday": 3,
    "четверг": 3,
    "чт": 3,
    "fri": 4,
    "friday": 4,
    "пятница": 4,
    "пт": 4,
    "sat": 5,
    "saturday": 5,
    "суббота": 5,
    "сб": 5,
    "sun": 6,
    "sunday": 6,
    "воскресенье": 6,
    "вс": 6,
}


@dataclass
class VotingPollConfig:
    poll_id: str
    topic_template: str
    options: List[str]
    weekdays: List[int] = field(default_factory=list)
    parameters: Dict[str, Any] = field(default_factory=dict)
    comments: List[str] = field(default_factory=list)
    topic_id: Optional[int] = None

    def should_run_on(self, current: dt.datetime) -> bool:
        if not self.weekdays:
            return True
        return current.weekday() in self.weekdays


class VotingPollsManager:
    def __init__(self) -> None:
        self.bot: Optional[Bot] = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
        self.chat_ids: List[str] = self._get_chat_ids()
        self.automation_topics: Dict[str, Any] = {}

    def _get_chat_ids(self) -> List[str]:
        """Получает список ID чатов из переменной окружения CHAT_ID"""
        if not CHAT_ID:
            return []

        # Разделяем по запятой или пробелу
        chat_ids = []
        for part in CHAT_ID.replace(',', ' ').split():
            chat_id = part.strip()
            if chat_id:
                chat_ids.append(chat_id)

        return chat_ids

    async def create_due_polls(self) -> bool:
        if not self.bot or not self.chat_ids:
            print("❌ BOT_TOKEN / CHAT_ID не настроены – голосования не будут отправлены")
            return False

        configs = self._load_configs()
        if not configs:
            print("ℹ️ Конфигурации голосований не найдены")
            return False

        today = get_moscow_time()
        created_any = False
        for config in configs:
            if not config.should_run_on(today):
                continue
            try:
                created = await self._create_poll_for_config(config, today)
                created_any = created_any or created
            except Exception as error:
                print(f"❌ Не удалось создать голосование '{config.poll_id}': {error}")
        return created_any

    def _load_configs(self) -> List[VotingPollConfig]:
        snapshot = duplicate_protection.get_config_ids()
        raw_configs = snapshot.get("voting_polls", []) or []
        self.automation_topics = snapshot.get("automation_topics") or {}

        configs: List[VotingPollConfig] = []
        for raw in raw_configs:
            poll_id = str(raw.get("poll_id") or "").strip()
            if not poll_id or poll_id.startswith("-"):
                continue

            topic = (raw.get("topic_template") or "").strip()
            if not topic:
                print(f"⚠️ Пропускаем голосование {poll_id}: тема не задана")
                continue

            option_entries = raw.get("options") or []
            options = [str(opt.get("text")).strip() for opt in option_entries if opt.get("text")]
            if len(options) < 2:
                print(f"⚠️ Пропускаем голосование {poll_id}: недостаточно вариантов ответа")
                continue

            weekdays_raw = raw.get("weekdays") or []
            weekdays_norm: List[int] = []
            for value in weekdays_raw:
                parsed = self._parse_weekday_token(str(value))
                if parsed is not None and parsed not in weekdays_norm:
                    weekdays_norm.append(parsed)

            metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            comments = raw.get("comments") if isinstance(raw.get("comments"), list) else []
            topic_id_value = raw.get("topic_id")
            topic_id = self._parse_int(topic_id_value)

            configs.append(
                VotingPollConfig(
                    poll_id=poll_id,
                    topic_template=topic,
                    options=options,
                    weekdays=weekdays_norm,
                    parameters=metadata,
                    comments=comments,
                    topic_id=topic_id,
                )
            )

        return configs

    async def _create_poll_for_config(self, config: VotingPollConfig, today: dt.datetime) -> bool:
        unique_key = f"VOTING_{config.poll_id}_{today.strftime('%Y%m%d')}"

        duplicate = duplicate_protection.check_duplicate("ОПРОС_ГОЛОСОВАНИЕ", unique_key)
        if duplicate.get("exists"):
            print(f"⏭️ Голосование {config.poll_id} уже отправлялось сегодня (Google Sheets)")
            return False

        replacements = self._build_placeholder_replacements(config, today)
        question = self._render_text(config.topic_template, replacements).strip()
        options = [self._render_text(option, replacements).strip() for option in config.options if option.strip()]

        if len(options) < 2:
            print(f"⚠️ Голосование {config.poll_id}: после подстановки осталось меньше двух вариантов")
            return False

        params = config.parameters or {}
        automation_settings = self._get_automation_settings(AUTOMATION_VOTING_KEY)
        is_anonymous = self._resolve_bool_setting(params, automation_settings, "is_anonymous", False)
        allows_multiple = self._resolve_bool_setting(params, automation_settings, "allows_multiple_answers", True)

        open_period_source = params.get("open_period_minutes")
        if open_period_source is None:
            open_period_source = automation_settings.get("open_period_minutes")
        open_period = self._coerce_int(open_period_source)
        if open_period is not None:
            open_period = max(5, min(open_period, 600))

        close_date_source = params.get("close_date") or automation_settings.get("close_date")
        close_date = self._parse_close_date(close_date_source, today)

        params_topic_id = self._parse_int(params.get("topic_id"))
        automation_topic_id = self._get_automation_topic(AUTOMATION_VOTING_KEY)
        topic_id = None
        if config.topic_id is not None:
            topic_id = config.topic_id
        elif params_topic_id is not None:
            topic_id = params_topic_id
        elif automation_topic_id is not None:
            topic_id = automation_topic_id

        additional_info = f"{question} | " + " · ".join(options)
        record = duplicate_protection.add_record(
            "ОПРОС_ГОЛОСОВАНИЕ",
            unique_key,
            status="ОТПРАВЛЯЕТСЯ",
            additional_data=additional_info,
            game_link="",
            comp_id=None,
            team_id=None,
            alt_name=config.poll_id,
            settings=json.dumps(
                {
                    "poll_id": config.poll_id,
                    "parameters": params,
                    "question": question,
                    "options": options,
                },
                ensure_ascii=False,
            ),
        )
        if not record.get("success") and "error" in record:
            print(f"⚠️ Не удалось зафиксировать запись в Google Sheets: {record['error']}")

        sheet_unique_key = record.get("unique_key") if record.get("success") else None

        bot_instance = cast(Bot, self.bot)

        # Отправляем опрос во все настроенные чаты
        messages = []
        for chat_id in self.chat_ids:
            send_kwargs: Dict[str, Any] = {
                "chat_id": chat_id,
                "question": question,
                "options": options,
                "is_anonymous": is_anonymous,
                "allows_multiple_answers": allows_multiple,
            }
            if topic_id is not None:
                send_kwargs["message_thread_id"] = topic_id
            if open_period is not None:
                send_kwargs["open_period"] = open_period
            if close_date is not None:
                send_kwargs["close_date"] = close_date

            try:
                message = await bot_instance.send_poll(**send_kwargs)
                messages.append(message)
            except TelegramError as error:
                if "Message thread not found" in str(error) and "message_thread_id" in send_kwargs:
                    print(f"⚠️ Топик не найден в чате {chat_id}, отправляем голосование в основной чат")
                    send_kwargs.pop("message_thread_id", None)
                    message = await bot_instance.send_poll(**send_kwargs)
                    messages.append(message)
                else:
                    if sheet_unique_key:
                        duplicate_protection.update_record_status(sheet_unique_key, "ОШИБКА")
                    raise

        # Используем первое сообщение для совместимости
        message = messages[0] if messages else None

                if sheet_unique_key:
                    duplicate_protection.update_record_status(sheet_unique_key, "ОТПРАВЛЕН")
            print(f"✅ Голосование {config.poll_id} отправлено (message_id={message.message_id})")
        return True

    def _build_placeholder_replacements(
        self,
        config: VotingPollConfig,
        reference_dt: dt.datetime,
    ) -> Dict[str, str]:
        placeholders: List[str] = []
        placeholders.extend(PLACEHOLDER_PATTERN.findall(config.topic_template or ""))
        for option in config.options:
            placeholders.extend(PLACEHOLDER_PATTERN.findall(option or ""))

        replacements: Dict[str, str] = {}
        for placeholder in placeholders:
            if placeholder in replacements:
                continue
            weekday = self._parse_weekday_token(placeholder)
            if weekday is None:
                continue
            target_date = self._next_occurrence(reference_dt, weekday)
            replacements[placeholder] = target_date.strftime("%d.%m")
        return replacements

    def _render_text(self, text: str, replacements: Dict[str, str]) -> str:
        if not text:
            return ""
        result = text
        for placeholder, replacement in replacements.items():
            result = result.replace(f"[{placeholder}]", replacement)
        return result

    def _next_occurrence(self, reference_dt: dt.datetime, target_weekday: int) -> dt.date:
        days_ahead = (target_weekday - reference_dt.weekday()) % 7
        return (reference_dt + dt.timedelta(days=days_ahead)).date()

    def _get_automation_settings(self, key: str) -> Dict[str, Any]:
        if not key or not isinstance(key, str) or not self.automation_topics:
            return {}
        entry = self.automation_topics.get(key.upper())
        if isinstance(entry, dict):
            return entry
        return {}

    def _get_automation_topic(self, key: str) -> Optional[int]:
        entry = self._get_automation_settings(key)
        if not entry:
            return None
        topic_candidate = entry.get("topic_id")
        parsed = self._parse_int(topic_candidate)
        if parsed is not None:
            return parsed
        topic_raw = entry.get("topic_raw")
        return self._parse_int(topic_raw)

    def _parse_weekday_token(self, token: str) -> Optional[int]:
        normalized = token.strip().lower()
        return WEEKDAY_ALIASES.get(normalized)

    def _coerce_bool(self, value: Any, default: bool = False) -> bool:
        if value is None:
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes", "y", "да"}:
                return True
            if lowered in {"false", "0", "no", "n", "нет"}:
                return False
        return default

    def _coerce_int(self, value: Any, default: Optional[int] = None) -> Optional[int]:
        if value in (None, ""):
            return default
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return default

    def _parse_close_date(self, value: Any, reference_dt: dt.datetime) -> Optional[dt.datetime]:
        if not value:
            return None
        text = str(value).strip()
        tz = reference_dt.tzinfo or dt.timezone.utc
        for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
            try:
                parsed = dt.datetime.strptime(text, fmt)
                if fmt == "%d.%m.%Y":
                    parsed = parsed.replace(hour=23, minute=59)
                parsed = parsed.replace(tzinfo=tz)
                return parsed.astimezone(dt.timezone.utc)
            except ValueError:
                continue
        print(f"⚠️ Не удалось разобрать close_date='{value}', игнорируем параметр")
        return None

    def _resolve_bool_setting(
        self,
        params: Dict[str, Any],
        automation: Dict[str, Any],
        key: str,
        default: bool,
    ) -> bool:
        if key in params:
            return self._coerce_bool(params.get(key), default)
        if key in automation:
            return self._coerce_bool(automation.get(key), default)
        return default

    def _resolve_chat_id(self, value: Optional[str]) -> Optional[Any]:
        if not value:
            return None
        candidate = value.strip()
        if candidate.startswith("@"):
            return candidate
        try:
            return int(candidate)
        except ValueError:
            return candidate

    def _parse_int(self, value: Any) -> Optional[int]:
        try:
            if value in (None, "", "None"):
                return None
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None


async def main() -> None:
    print("📊 VOTING POLL MANAGER")
    print("=" * 40)
    manager = VotingPollsManager()
    created = await manager.create_due_polls()
    if created:
        print("✅ Хотя бы одно голосование создано")
    else:
        print("ℹ️ Новых голосований не создано")


if __name__ == "__main__":
    asyncio.run(main())

