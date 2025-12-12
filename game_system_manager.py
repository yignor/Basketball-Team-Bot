#!/usr/bin/env python3
"""
Единый модуль для управления системой игр
Выполняет последовательно: парсинг → создание опросов → создание анонсов
"""

import io
import os
import asyncio
import datetime
import json
import re
import uuid
from urllib.parse import urljoin
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, cast
from zoneinfo import ZoneInfo
from datetime_utils import get_moscow_time, is_today, log_current_time
from enhanced_duplicate_protection import duplicate_protection
from info_basket_client import InfoBasketClient
from infobasket_smart_parser import InfobasketSmartParser
from comp_names import get_comp_name
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from telegram import Bot
    import aiohttp

# Переменные окружения (загружаются из системы или .env файла)
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
GAMES_TOPIC_ID = os.getenv("GAMES_TOPIC_ID", "1282")  # Топик для опросов по играм
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"  # Тестовый режим

def get_chat_ids() -> List[str]:
    """Получает список ID чатов из переменной окружения CHAT_ID

    Поддерживает форматы:
    - одиночный ID: "123456789"
    - несколько ID через запятую: "123456789,987654321,111111111"
    - несколько ID через пробел: "123456789 987654321 111111111"
    """
    if not CHAT_ID:
        return []

    # Разделяем по запятой или пробелу
    chat_ids = []
    for part in CHAT_ID.replace(',', ' ').split():
        chat_id = part.strip()
        if chat_id:
            chat_ids.append(chat_id)

    return chat_ids

AUTOMATION_KEY_GAME_POLLS = "GAME_POLLS"
AUTOMATION_KEY_GAME_ANNOUNCEMENTS = "GAME_ANNOUNCEMENTS"
AUTOMATION_KEY_GAME_UPDATES = "GAME_UPDATES"
AUTOMATION_KEY_CALENDAR_EVENTS = "CALENDAR_EVENTS"

def create_game_key(game_info: Dict) -> str:
    """Создает уникальный ключ для игры"""
    # Нормализуем время (заменяем точку на двоеточие для единообразия)
    time_str = game_info['time'].replace('.', ':')
    # Включаем время в ключ для уникальности
    return f"{game_info['date']}_{time_str}_{game_info['team1']}_{game_info['team2']}"

def create_announcement_key(game_info: Dict) -> str:
    """Создает уникальный ключ для анонса"""
    # Нормализуем время (заменяем точку на двоеточие для единообразия)
    time_str = game_info['time'].replace('.', ':')
    # Включаем время в ключ для уникальности
    return f"{game_info['date']}_{time_str}_{game_info['team1']}_{game_info['team2']}"

def get_day_of_week(date_str: str) -> str:
    """Возвращает день недели на русском языке"""
    try:
        date_obj = datetime.datetime.strptime(date_str, '%d.%m.%Y')
        days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
        return days[date_obj.weekday()]
    except:
        return ""

def get_team_category_by_type(team_type: Optional[str]) -> str:
    """Возвращает читаемую категорию команды по типу"""
    if isinstance(team_type, str) and team_type.strip():
        normalized = team_type.strip()
        mapping = {
            "farm_team": "Команда",
            "first_team": "Команда",
            "configured": "Команда",
        }
        return mapping.get(normalized, normalized if normalized else "Команда")
    return "Команда"


def get_team_category_with_declension(team_type: Optional[str]) -> str:
    """Возвращает категорию команды с правильным склонением"""
    category = get_team_category_by_type(team_type)
    if not category:
        return "команды"
    lower = category.lower()
    if lower.endswith('а'):
        return f"{lower[:-1]}ы"
    if lower.endswith('я'):
        return f"{lower[:-1]}и"
    return lower


def determine_form_color(game_info: Dict) -> str:
    """Определяет цвет формы на основе позиции нашей команды"""
    our_team_id = game_info.get('our_team_id')
    if our_team_id:
        if our_team_id == game_info.get('team1_id'):
            return "светлая"
        if our_team_id == game_info.get('team2_id'):
            return "темная"
    return "светлая"

def format_date_without_year(date_str: str) -> str:
    """Форматирует дату без года (например, 27.08)"""
    try:
        from datetime import datetime
        date_obj = datetime.strptime(date_str, '%d.%m.%Y')
        return date_obj.strftime('%d.%m')
    except:
        return date_str

class GameSystemManager:
    """Единый класс для управления всей системой игр"""
    
    def __init__(self):
        # Type annotation for bot to help linter understand it's a Telegram Bot
        self.bot: Optional['Bot'] = None
        self.team_name_keywords: List[str] = []
        self.team_names_by_id: Dict[int, str] = {}
        self.team_configs: Dict[int, Dict[str, Any]] = {}
        self.training_poll_configs: List[Dict[str, Any]] = []
        self.voting_configs: List[Dict[str, Any]] = []
        self.fallback_sources: List[Dict[str, Any]] = []
        self.automation_topics: Dict[str, Any] = {}
        self.config_comp_ids: List[int] = []
        self.config_team_ids: List[int] = []
        self.config_comp_ids_set = set(self.config_comp_ids)
        self.config_team_ids_set = set(self.config_team_ids)
        
        # Кэш для проверок дублирования (чтобы избежать повторных запросов к API)
        # Ключ: (data_type, game_id), Значение: Optional[Dict] (None = не найдено, Dict = найдено)
        self._duplicate_check_cache: Dict[tuple, Optional[Dict[str, Any]]] = {}
        
        config_snapshot = duplicate_protection.get_config_ids()
        self.config_comp_ids = config_snapshot.get('comp_ids', [])
        self.config_team_ids = config_snapshot.get('team_ids', [])
        self.team_configs = config_snapshot.get('teams', {}) or {}
        self.training_poll_configs = config_snapshot.get('training_polls', []) or []
        self.voting_configs = config_snapshot.get('voting_polls', []) or []
        self.fallback_sources = config_snapshot.get('fallback_sources', []) or []
        self.automation_topics = config_snapshot.get('automation_topics', {}) or {}
        self.config_comp_ids_set = set(self.config_comp_ids)
        self.config_team_ids_set = set(self.config_team_ids)
        
        game_polls_entry = self._get_automation_entry(AUTOMATION_KEY_GAME_POLLS)
        self.game_poll_topic_id = self._resolve_automation_topic_id(game_polls_entry)
        self.game_poll_is_anonymous = self._resolve_automation_bool(game_polls_entry, "is_anonymous", False)
        self.game_poll_allows_multiple = self._resolve_automation_bool(game_polls_entry, "allows_multiple_answers", False)
        game_announcements_entry = self._get_automation_entry(AUTOMATION_KEY_GAME_ANNOUNCEMENTS)
        self.game_announcement_topic_id = self._resolve_automation_topic_id(game_announcements_entry)
        game_updates_entry = self._get_automation_entry(AUTOMATION_KEY_GAME_UPDATES)
        # Если топик не указан, будет None - отправка в общий чат
        self.game_updates_topic_id = self._resolve_automation_topic_id(game_updates_entry)
        calendar_events_entry = self._get_automation_entry(AUTOMATION_KEY_CALENDAR_EVENTS)
        # Если топик не указан, будет None - отправка в общий чат
        self.calendar_events_topic_id = self._resolve_automation_topic_id(calendar_events_entry)
        
        self._update_team_mappings()
        
        print(f"🔍 Инициализация GameSystemManager:")
        if self.config_comp_ids or self.config_team_ids:
            print(f"   ⚙️ Конфигурация соревнований: {self.config_comp_ids}")
            print(f"   ⚙️ Конфигурация команд: {self.config_team_ids}")
        else:
            print("   ⚠️ Конфигурация соревнований и команд не найдена в сервисном листе")
        print(
            "   🧩 GAME_POLLS: "
            f"topic={self.game_poll_topic_id}, anonymous={self.game_poll_is_anonymous}, "
            f"multiple={self.game_poll_allows_multiple}"
        )
        print(
            "   🧩 GAME_ANNOUNCEMENTS: "
            f"topic={self.game_announcement_topic_id}"
        )
        print(
            "   🧩 GAME_UPDATES: "
            f"topic={self.game_updates_topic_id}"
        )
        print(
            "   🧩 CALENDAR_EVENTS: "
            f"topic={self.calendar_events_topic_id}"
        )
        
        if BOT_TOKEN:
            from telegram import Bot
            self.bot = Bot(token=BOT_TOKEN)
    
    def _to_int(self, value: Any) -> Optional[int]:
        """Безопасно конвертирует значение в int"""
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _get_automation_entry(self, key: str) -> Dict[str, Any]:
        if not key:
            return {}
        entry = self.automation_topics.get(key.upper()) if hasattr(self, "automation_topics") else None
        if isinstance(entry, dict):
            return entry
        return {}

    def _resolve_automation_topic_id(
        self,
        entry: Dict[str, Any],
        fallback: Optional[int] = None,
    ) -> Optional[int]:
        """Разрешает ID топика из настроек автоматических сообщений.
        Если топик не указан, возвращает None (отправка в общий чат).
        Параметр fallback игнорируется для соответствия требованиям."""
        if not entry:
            return None
        topic_candidate = entry.get("topic_id")
        if topic_candidate is None:
            topic_candidate = entry.get("topic_raw")
        topic_value = self._to_int(topic_candidate)
        return topic_value

    def _resolve_automation_bool(
        self,
        entry: Dict[str, Any],
        key: str,
        default: bool,
    ) -> bool:
        if not entry or key not in entry:
            return default
        value = entry.get(key)
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

    @staticmethod
    def _normalize_name_for_search(name: str) -> str:
        """Нормализует имя команды для сравнения
        
        Учитывает дефисы в названиях команд (например, "Военмех-Vintage").
        Дефисы заменяются на пробелы для более гибкого поиска, но также сохраняется вариант с дефисом.
        """
        if not isinstance(name, str):
            return ""
        name = name.strip()
        # Заменяем множественные пробелы на один
        name = re.sub(r'\s+', ' ', name)
        # Для поиска: убираем пробелы, но сохраняем дефисы как разделители
        # Это позволяет находить "Военмех-Vintage" даже если в тексте "Военмех - Vintage"
        normalized = re.sub(r'\s+', '', name.lower())
        return normalized

    def _build_name_variants(self, *names: Optional[str]) -> Set[str]:
        """Формирует набор уникальных вариантов имени команды
        
        Учитывает команды с дефисами (например, "Военмех-Vintage"):
        - Оригинальное название с дефисом
        - Вариант с пробелом вместо дефиса
        - Нормализованный вариант без пробелов
        """
        variants: Set[str] = set()
        for name in names:
            if not name or not isinstance(name, str):
                continue
            stripped = name.strip()
            if stripped:
                # Оригинальное название
                variants.add(stripped)
                
                # Если есть дефис, добавляем варианты с пробелом и без разделителя
                if '-' in stripped or '–' in stripped or '—' in stripped:
                    # Вариант с пробелом вместо дефиса
                    variant_with_space = re.sub(r'[-–—]', ' ', stripped)
                    variants.add(variant_with_space)
                    variants.add(variant_with_space.strip())
                
                # Нормализованный вариант (без пробелов, с дефисами)
                normalized = self._normalize_name_for_search(stripped)
                if normalized:
                    variants.add(normalized)
                    
                # Вариант без дефисов (для поиска "ВоенмехVintage")
                variant_no_hyphen = re.sub(r'[-–—]', '', stripped)
                if variant_no_hyphen != stripped:
                    variants.add(variant_no_hyphen)
                    normalized_no_hyphen = self._normalize_name_for_search(variant_no_hyphen)
                    if normalized_no_hyphen:
                        variants.add(normalized_no_hyphen)
        return variants

    def _find_matching_variant(self, normalized_text: str, variants: Sequence[str]) -> Optional[str]:
        """Ищет первый вариант имени, встречающийся в нормализованном тексте"""
        for variant in variants:
            normalized_variant = self._normalize_name_for_search(variant)
            if normalized_variant and normalized_variant in normalized_text:
                return variant
        return None

    def resolve_team_config(self, team_name: str) -> Optional[Dict[str, Any]]:
        """Возвращает конфигурацию команды по названию (с учетом альтернатив и алиасов)"""
        if not team_name:
            return None
        normalized = self._normalize_name_for_search(team_name)
        if not normalized:
            return None
        for team_id, data in self.team_configs.items():
            metadata = data.get('metadata') or {}
            candidates = set()
            alt_name = data.get('alt_name')
            if isinstance(alt_name, str) and alt_name.strip():
                candidates.add(alt_name.strip())
            aliases = metadata.get('aliases') if isinstance(metadata, dict) else []
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str) and alias.strip():
                        candidates.add(alias.strip())
            for candidate in candidates:
                if self._normalize_name_for_search(candidate) == normalized:
                    return {
                        'team_id': team_id,
                        'alt_name': alt_name,
                        'metadata': metadata
                    }
        return None

    def _update_team_mappings(self) -> None:
        self.team_names_by_id = {}
        for team_id, data in self.team_configs.items():
            alt_name = data.get('alt_name')
            if isinstance(alt_name, str) and alt_name.strip():
                self.team_names_by_id[team_id] = alt_name.strip()
            metadata = data.get('metadata') or {}
            aliases = metadata.get('aliases') if isinstance(metadata, dict) else []
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str) and alias.strip() and team_id not in self.team_names_by_id:
                        self.team_names_by_id[team_id] = alias.strip()
        keyword_sources: Set[str] = set()
        keyword_sources.update(self.team_names_by_id.values())
        for data in self.team_configs.values():
            metadata = data.get('metadata') or {}
            aliases = metadata.get('aliases') if isinstance(metadata, dict) else []
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str) and alias.strip():
                        keyword_sources.add(alias.strip())
        for source in self.fallback_sources:
            name = source.get('name')
            if isinstance(name, str) and name.strip():
                keyword_sources.add(name.strip())
        self.team_name_keywords = sorted(keyword_sources)
    
    def _resolve_team_name(self, team_id: Optional[int], fallback: Optional[str] = None) -> Optional[str]:
        if team_id is None:
            return fallback
        config = self.team_configs.get(team_id) if isinstance(self.team_configs, dict) else None
        if isinstance(config, dict):
            alt_name = config.get('alt_name')
            if isinstance(alt_name, str) and alt_name.strip():
                return alt_name.strip()
            metadata = config.get('metadata') if isinstance(config, dict) else {}
            if isinstance(metadata, dict):
                display_name = metadata.get('display_name')
                if isinstance(display_name, str) and display_name.strip():
                    return display_name.strip()
        return fallback.strip() if isinstance(fallback, str) else fallback
    
    def _get_team_display_name(self, team_id: Optional[int], fallback: Optional[str] = None) -> str:
        resolved = self._resolve_team_name(team_id, fallback)
        if resolved:
            return resolved
        if isinstance(fallback, str) and fallback.strip():
            return fallback.strip()
        if team_id is None:
            return ""
        return str(team_id)
    
    @staticmethod
    def _escape_ics_text(text: Optional[str]) -> str:
        if not text:
            return ""
        escaped = str(text).replace('\\', '\\\\').replace('\n', '\\n')
        escaped = escaped.replace('\r', '').replace(',', '\\,').replace(';', '\\;')
        return escaped

    @staticmethod
    def _sanitize_filename(text: Optional[str]) -> str:
        if not text:
            return "event"
        sanitized = re.sub(r"[^0-9A-Za-zА-Яа-я\-_]+", "_", text.strip())
        return sanitized or "event"

    def _build_game_calendar_payload(
        self,
        game_info: Dict[str, Any],
        team_label: str,
        opponent: str,
        form_color: str,
    ) -> Optional[tuple]:
        date_str = game_info.get('date')
        time_raw = self._normalize_time_string(game_info.get('time'))
        if not date_str or not time_raw:
            return None
        try:
            naive_start = datetime.datetime.strptime(f"{date_str} {time_raw}", "%d.%m.%Y %H:%M")
        except ValueError:
            try:
                naive_start = datetime.datetime.strptime(f"{date_str} {time_raw}", "%d.%m.%Y %H.%M")
            except ValueError:
                print(f"⚠️ Не удалось разобрать дату/время для iCal: {date_str} {time_raw}")
                return None
        moscow_tz = ZoneInfo('Europe/Moscow')
        start_dt = naive_start.replace(tzinfo=moscow_tz)
        end_dt = start_dt + datetime.timedelta(hours=2)
        summary = f"{team_label} vs {opponent}".strip()
        location = game_info.get('venue') or ''
        description_parts = [f"Форма: {form_color}"]
        game_link = game_info.get('game_link')
        if isinstance(game_link, str) and game_link.strip():
            description_parts.append(f"Ссылка: {game_link.strip()}")
        description = "\n".join(description_parts)
        uid_source = game_info.get('game_id') or uuid.uuid4()
        uid = f"{uid_source}@telegram-game-bot"
        dtstamp = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        start_str = start_dt.strftime("%Y%m%dT%H%M%S")
        end_str = end_dt.strftime("%Y%m%dT%H%M%S")

        ics_lines = [
            "BEGIN:VCALENDAR",
            "VERSION:2.0",
            "PRODID:-//Telegram Game Bot//Calendar//RU",
            "CALSCALE:GREGORIAN",
            "METHOD:PUBLISH",
            "X-WR-TIMEZONE:Europe/Moscow",
            "BEGIN:VTIMEZONE",
            "TZID:Europe/Moscow",
            "X-LIC-LOCATION:Europe/Moscow",
            "BEGIN:STANDARD",
            "TZOFFSETFROM:+0300",
            "TZOFFSETTO:+0300",
            "TZNAME:MSK",
            "DTSTART:19700101T000000",
            "END:STANDARD",
            "END:VTIMEZONE",
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{dtstamp}",
            f"DTSTART;TZID=Europe/Moscow:{start_str}",
            f"DTEND;TZID=Europe/Moscow:{end_str}",
            f"SUMMARY:{self._escape_ics_text(summary)}",
            f"LOCATION:{self._escape_ics_text(location)}",
            f"DESCRIPTION:{self._escape_ics_text(description)}",
            "STATUS:CONFIRMED",
            "END:VEVENT",
            "END:VCALENDAR",
            "",
        ]

        content = "\r\n".join(ics_lines)
        filename_base = self._sanitize_filename(summary)
        filename = f"{start_dt.strftime('%Y%m%d')}-{filename_base}.ics"
        caption = f"Добавьте игру {summary} в календарь"
        return io.BytesIO(content.encode('utf-8')), filename, caption
    
    def find_target_teams_in_text(self, text: str) -> List[str]:
        """Находит целевые команды в тексте"""
        found_teams: List[str] = []
        
        search_names = []
        if self.team_name_keywords:
            search_names.extend(self.team_name_keywords)
        if self.team_names_by_id:
            search_names.extend(self.team_names_by_id.values())
        
        # Удаляем дубликаты и пустые значения
        search_names = [name for name in {name.strip() for name in search_names} if name]
        
        if not search_names:
            return found_teams
        
        text_normalized = re.sub(r"[\s\-_/]", "", text.lower())
        
        for name in search_names:
            normalized_name = re.sub(r"[\s\-_/]", "", name.lower())
            if normalized_name and normalized_name in text_normalized:
                found_teams.append(name)
                print(f"   ✅ Найдена команда по названию: {name}")
        
        if not found_teams:
            print(f"   ❌ Целевые команды не найдены в тексте: {text[:100]}...")
            print(f"   🔍 Нормализованный текст: {text_normalized[:100]}...")
        
        return found_teams
    
    def parse_schedule_text(self, text: str) -> List[Dict]:
        """Парсит текст расписания и извлекает информацию об играх"""
        games = []
        
        # Разбиваем текст на строки
        lines = text.strip().split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Паттерн для игр с датой и временем
            pattern1 = r'(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2})\s+(.+?)\s+vs\s+(.+?)\s+(.+)'
            match1 = re.search(pattern1, line)
            
            if match1:
                date, time, team1, team2, venue = match1.groups()
                games.append({
                    'date': date,
                    'time': time,
                    'team1': team1.strip(),
                    'team2': team2.strip(),
                    'venue': venue.strip(),
                    'full_text': line
                })
                continue
            
            # Паттерн для игр без времени (из табло)
            pattern2 = r'(.+?)\s+vs\s+(.+)'
            match2 = re.search(pattern2, line)
            
            if match2:
                team1, team2 = match2.groups()
                # Проверяем, есть ли наши команды
                game_text = f"{team1} {team2}"
                if self.find_target_teams_in_text(game_text):
                    games.append({
                        'date': get_moscow_time().strftime('%d.%m.%Y'),
                        'time': '20:30',  # Время по умолчанию
                        'team1': team1.strip(),
                        'team2': team2.strip(),
                        'venue': 'ВО СШОР Малый 66',  # Место по умолчанию
                        'full_text': line
                    })
        
        return games
    
    async def fetch_infobasket_schedule(self) -> Dict[str, List[Dict]]:
        """Получает расписание игр через Infobasket API"""
        try:
            print("🔍 Получение расписания через Infobasket Smart API...")
            print(f"   ➡️ ID соревнований для запроса: {self.config_comp_ids or 'не заданы'}")
            print(f"   ➡️ ID команд для фильтрации: {self.config_team_ids or 'не заданы'}")

            parser = InfobasketSmartParser(
                comp_ids=self.config_comp_ids,
                team_ids=self.config_team_ids,
                team_name_keywords=self.team_name_keywords
            )

            all_games = await parser.get_all_team_games()

            future_games: List[Dict] = []
            today_games: List[Dict] = []

            for team_type, games in all_games.items():
                for category, storage in (("future", future_games), ("today", today_games)):
                    for game in games[category]:
                        team1_id = self._to_int(game.get('Team1ID'))
                        team2_id = self._to_int(game.get('Team2ID'))
                        our_team_id = self._to_int(game.get('ConfiguredTeamID'))
                        opponent_team_id = self._to_int(game.get('OpponentTeamID'))

                        if our_team_id is None and self.config_team_ids_set:
                            if team1_id in self.config_team_ids_set:
                                our_team_id = team1_id
                                opponent_team_id = team2_id
                            elif team2_id in self.config_team_ids_set:
                                our_team_id = team2_id
                                opponent_team_id = team1_id

                        our_team_name = None
                        opponent_team_name = None

                        if our_team_id is not None:
                            if our_team_id == team1_id:
                                our_team_name = self._resolve_team_name(our_team_id, game.get('ShortTeamNameAru'))
                                opponent_team_name = self._resolve_team_name(opponent_team_id, game.get('ShortTeamNameBru'))
                            elif our_team_id == team2_id:
                                our_team_name = self._resolve_team_name(our_team_id, game.get('ShortTeamNameBru'))
                                opponent_team_name = self._resolve_team_name(opponent_team_id, game.get('ShortTeamNameAru'))

                        if our_team_id is not None and our_team_name:
                            self.team_names_by_id[our_team_id] = our_team_name
                            if our_team_name not in self.team_name_keywords:
                                self.team_name_keywords.append(our_team_name)

                        storage.append({
                            'date': game.get('GameDate'),
                            'time': game.get('GameTimeMsk'),
                            'team1': game.get('ShortTeamNameAru'),
                            'team2': game.get('ShortTeamNameBru'),
                            'venue': game.get('ArenaRu'),
                            'comp_name': game.get('CompNameRu'),
                            'comp_id': game.get('CompID'),
                            'game_id': game.get('GameID'),
                            'team_type': team_type,
                            'team1_id': team1_id,
                            'team2_id': team2_id,
                            'our_team_id': our_team_id,
                            'opponent_team_id': opponent_team_id,
                            'our_team_name': our_team_name,
                            'opponent_team_name': opponent_team_name,
                            'source': 'infobasket_smart_api',
                            'game_link': f"https://www.fbp.ru/game.html?gameId={game.get('GameID')}&apiUrl=https://reg.infobasket.su&lang=ru"
                        })

            print(f"✅ Infobasket Smart API: будущих игр {len(future_games)}, игр сегодня {len(today_games)}")
            return {'future': future_games, 'today': today_games}

        except Exception as e:
            print(f"❌ Ошибка Infobasket Smart API: {e}")
            return {'future': [], 'today': []}

    @staticmethod
    def _normalize_time_string(value: Optional[str]) -> str:
        if not value:
            return ""
        return value.replace('.', ':').strip()

    async def fetch_widget_game_details(self, game_id: int) -> Optional[Dict[str, Any]]:
        try:
            import aiohttp
            url = f"https://reg.infobasket.su/Widget/GetOnline/{game_id}?format=json&lang=ru"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"⚠️ Widget API вернул статус {response.status} для GameID {game_id}")
                        return None
                    data = await response.json()

            game_date = data.get('GameDate') or ''
            game_time = data.get('GameTimeMsk') or data.get('GameTime') or ''
            online_block = data.get('Online') or {}
            arena = online_block.get('Venue2') or online_block.get('Venue1') or data.get('ArenaRu') or ''
            teams = data.get('GameTeams') or []
            team_a_id = teams[0].get('TeamID') if len(teams) > 0 else None
            team_b_id = teams[1].get('TeamID') if len(teams) > 1 else None

            return {
                'game_date': game_date,
                'game_time': self._normalize_time_string(game_time),
                'arena': arena,
                'team_a_id': team_a_id,
                'team_b_id': team_b_id,
            }
        except Exception as e:
            print(f"⚠️ Ошибка получения данных Widget для GameID {game_id}: {e}")
            return None

    def _merge_widget_details(self, game_info: Dict[str, Any], widget_data: Dict[str, Any]) -> None:
        if not widget_data:
            return
        if widget_data.get('game_date'):
            game_info['date'] = widget_data['game_date']
        if widget_data.get('game_time'):
            game_info['time'] = widget_data['game_time']
        if widget_data.get('arena'):
            game_info['venue'] = widget_data['arena']
        if widget_data.get('team_a_id') is not None:
            game_info['team1_id'] = widget_data['team_a_id']
        if widget_data.get('team_b_id') is not None:
            game_info['team2_id'] = widget_data['team_b_id']

    def _game_record_matches(self, record: Dict[str, Any], game_info: Dict[str, Any]) -> bool:
        if not record:
            return False
        record_date = (record.get('game_date') or '').strip()
        record_time = self._normalize_time_string(record.get('game_time'))
        record_arena = (record.get('arena') or '').strip()
        record_team_a = (record.get('team_a_id') or '').strip()
        record_team_b = (record.get('team_b_id') or '').strip()

        game_date = (game_info.get('date') or '').strip()
        game_time = self._normalize_time_string(game_info.get('time'))
        game_arena = (game_info.get('venue') or '').strip()
        game_team_a = str(game_info.get('team1_id') or '').strip()
        game_team_b = str(game_info.get('team2_id') or '').strip()

        return (
            record_date == game_date
            and record_time == game_time
            and record_arena == game_arena
            and record_team_a == game_team_a
            and record_team_b == game_team_b
        )

    def _detect_game_changes(
        self,
        existing_record: Dict[str, Any],
        game_info: Dict[str, Any]
    ) -> Dict[str, Tuple[str, str]]:
        changes: Dict[str, Tuple[str, str]] = {}

        old_date = (existing_record.get('game_date') or '').strip()
        new_date = (game_info.get('date') or '').strip()
        if new_date and old_date != new_date:
            changes['date'] = (old_date, new_date)

        old_time = self._normalize_time_string(existing_record.get('game_time'))
        new_time = self._normalize_time_string(game_info.get('time'))
        if new_time and old_time != new_time:
            changes['time'] = (old_time, new_time)

        old_arena = (existing_record.get('arena') or '').strip()
        new_arena = (game_info.get('venue') or '').strip()
        if new_arena and old_arena != new_arena:
            changes['arena'] = (old_arena, new_arena)

        our_old_id = self._to_int(existing_record.get('team_id'))
        old_team_a = self._to_int(existing_record.get('team_a_id'))
        old_team_b = self._to_int(existing_record.get('team_b_id'))

        old_opponent_id: Optional[int] = None
        if our_old_id is not None:
            if old_team_a == our_old_id:
                old_opponent_id = old_team_b
            elif old_team_b == our_old_id:
                old_opponent_id = old_team_a
        if old_opponent_id is None:
            old_opponent_id = old_team_b if old_team_a == our_old_id else old_team_a

        new_opponent_id = self._to_int(game_info.get('opponent_team_id'))
        if new_opponent_id is None:
            new_our_id = self._to_int(game_info.get('our_team_id'))
            team1_id = self._to_int(game_info.get('team1_id'))
            team2_id = self._to_int(game_info.get('team2_id'))
            if new_our_id is not None:
                if team1_id == new_our_id:
                    new_opponent_id = team2_id
                elif team2_id == new_our_id:
                    new_opponent_id = team1_id
            if new_opponent_id is None:
                new_opponent_id = team2_id if team1_id == new_our_id else team1_id

        if (
            old_opponent_id is not None
            and new_opponent_id is not None
            and old_opponent_id != new_opponent_id
        ):
            old_name = self._get_team_display_name(old_opponent_id)
            new_name = self._get_team_display_name(new_opponent_id, game_info.get('opponent_team_name'))
            changes['opponent'] = (
                old_name or (f"ID {old_opponent_id}" if old_opponent_id is not None else ""),
                new_name or (f"ID {new_opponent_id}" if new_opponent_id is not None else "")
            )

        return changes

    def _format_changes_summary(self, changes: Dict[str, Tuple[str, str]]) -> str:
        labels = {
            'opponent': 'Соперник',
            'date': 'Дата',
            'time': 'Время',
            'arena': 'Арена',
        }
        parts: List[str] = []
        for key in ['opponent', 'date', 'time', 'arena']:
            if key in changes:
                old, new = changes[key]
                label = labels.get(key, key)
                parts.append(f"{label}: {old or '—'} → {new or '—'}")
        return '; '.join(parts)

    def _log_game_action(self, data_type: str, game_info: Dict[str, Any], status: str, additional_data: str) -> None:
        duplicate_protection.upsert_game_record(
            data_type=data_type,
            identifier=str(game_info.get('game_id')),
            status=status,
            additional_data=additional_data,
            game_link=game_info.get('game_link', ''),
            comp_id=game_info.get('comp_id'),
            team_id=game_info.get('our_team_id'),
            alt_name=game_info.get('our_team_name', ''),
            settings="",
            game_id=game_info.get('game_id'),
            game_date=game_info.get('date') or '',
            game_time=self._normalize_time_string(game_info.get('time')),
            arena=game_info.get('venue') or '',
            team_a_id=game_info.get('team1_id'),
            team_b_id=game_info.get('team2_id'),
        )
    
    async def _send_calendar_event(
        self,
        bot: Any,
        game_info: Dict[str, Any],
        team_label: str,
        opponent: str,
        form_color: str,
    ) -> None:
        chat_ids = get_chat_ids()
        if not chat_ids:
            print("⚠️ CHAT_ID отсутствует, пропускаем отправку календаря")
            return

        game_id = str(game_info.get('game_id') or '')
        if game_id:
            existing_calendar = duplicate_protection.get_game_record("КАЛЕНДАРЬ_ИГРА", game_id)
            if existing_calendar and self._game_record_matches(existing_calendar, game_info):
                print(f"⏭️ Календарное событие для GameID {game_id} уже отправлено")
                return

        payload = self._build_game_calendar_payload(game_info, team_label, opponent, form_color)
        if not payload:
            print("⚠️ Не удалось сформировать данные для календаря")
            return

        stream, filename, caption = payload
        ics_bytes = stream.getvalue()
        stream = io.BytesIO(ics_bytes)
        stream.name = filename
        try:
            from telegram import InputFile

            document = InputFile(stream, filename=filename)
        except Exception:
            document = stream

        try:
            chat_ids = get_chat_ids()
            if not chat_ids:
                print("❌ Не настроены ID чатов (CHAT_ID)")
                return

            message_thread_id: Optional[int] = self.calendar_events_topic_id

            # Отправляем календарь в каждый чат
            for chat_id in chat_ids:
                send_kwargs: Dict[str, Any] = {
                    "chat_id": self._to_int(chat_id) or chat_id,
                    "document": document,
                    "caption": caption,
                }
                if message_thread_id is not None:
                    send_kwargs["message_thread_id"] = message_thread_id

                try:
                    await bot.send_document(**send_kwargs)
                except Exception as primary_error:
                    if message_thread_id is not None and "Message thread not found" in str(primary_error):
                        print(f"⚠️ Топик {message_thread_id} не найден в чате {chat_id}, отправляем календарь в основной чат")
                        self.calendar_events_topic_id = None
                        send_kwargs.pop("message_thread_id", None)
                        await bot.send_document(**send_kwargs)
                    else:
                        raise primary_error

            print(f"📆 Отправлено календарное событие {filename}")
            self._log_game_action("КАЛЕНДАРЬ_ИГРА", game_info, "ICS ОТПРАВЛЁН", filename)

        except Exception as e:
            print(f"⚠️ Ошибка отправки календарного события: {e}")

    async def _notify_game_update(
        self,
        changes: Dict[str, Tuple[str, str]],
        game_info: Dict[str, Any]
    ) -> None:
        if not self.bot or not CHAT_ID:
            print("⚠️ Бот или CHAT_ID не настроены, уведомление об изменениях не отправлено")
            return

        bot = cast(Any, self.bot)
        opponent_id = self._to_int(game_info.get('opponent_team_id'))
        opponent_name = game_info.get('opponent_team_name')
        opponent_display = self._get_team_display_name(opponent_id, opponent_name)

        if 'opponent' in changes:
            opponent_display = changes['opponent'][1] or opponent_display

        our_team_display = self._get_team_display_name(
            self._to_int(game_info.get('our_team_id')),
            game_info.get('our_team_name')
        )

        labels = {
            'opponent': 'Соперник',
            'date': 'Дата',
            'time': 'Время',
            'arena': 'Арена',
        }

        lines = [
            f"⚠️ В игре против {opponent_display or 'неизвестного соперника'} обнаружены изменения:",
        ]

        for key in ['opponent', 'date', 'time', 'arena']:
            if key in changes:
                old, new = changes[key]
                label = labels.get(key, key)
                if key == 'opponent':
                    lines.append(f"• {label}: {old or '—'} → {new or '—'}")
                else:
                    lines.append(f"• {label}: {old or '—'} → {new or '—'}")

        message = "\n".join(lines)

        send_kwargs: Dict[str, Any] = {
            "chat_id": self._to_int(CHAT_ID) or CHAT_ID,
            "text": message,
        }
        message_thread_id: Optional[int] = self.game_updates_topic_id
        if message_thread_id is not None:
            send_kwargs["message_thread_id"] = message_thread_id

        try:
            try:
                await bot.send_message(**send_kwargs)
            except Exception as primary_error:
                if message_thread_id is not None and "Message thread not found" in str(primary_error):
                    print(f"⚠️ Топик {message_thread_id} не найден, отправляем обновление в основной чат")
                    self.game_updates_topic_id = None
                    send_kwargs.pop("message_thread_id", None)
                    await bot.send_message(**send_kwargs)
                else:
                    raise primary_error
        except Exception as e:
            print(f"⚠️ Ошибка отправки уведомления об изменениях: {e}")

    def _should_schedule_future_game(self, game_info: Dict[str, Any]) -> bool:
        try:
            game_date = datetime.datetime.strptime(game_info['date'], '%d.%m.%Y').date()
            today = get_moscow_time().date()
            if game_date <= today:
                print(f"⏭️ Игра {game_info['game_id']} запланирована на {game_info['date']} — опрос не требуется")
                return False
            return True
        except Exception as e:
            print(f"⚠️ Не удалось определить дату игры для GameID {game_info.get('game_id')}: {e}")
            return False

    async def _process_future_game(self, game_info: Dict[str, Any]) -> bool:
        if not self._is_correct_time_for_polls():
            return False

        if not self._should_schedule_future_game(game_info):
            return False

        game_id = game_info.get('game_id')
        if not game_id:
            print("⚠️ Нет GameID, пропускаем игру")
            return False

        # Проверяем кэш перед запросом к API
        cache_key = ("ОПРОС_ИГРА", str(game_id))
        cached_record = self._duplicate_check_cache.get(cache_key)
        
        if cached_record is not None:
            # Используем кэшированное значение
            if cached_record:
                print(f"⏭️ Опрос для GameID {game_id} уже есть (из кэша)")
                return False
        else:
            # Проверяем через API и кэшируем результат
            widget_data = await self.fetch_widget_game_details(int(game_id))
            if widget_data:
                self._merge_widget_details(game_info, widget_data)

            existing_record = duplicate_protection.get_game_record("ОПРОС_ИГРА", str(game_id))
            self._duplicate_check_cache[cache_key] = existing_record
            
            if existing_record:
                changes = self._detect_game_changes(existing_record, game_info)
                if changes:
                    await self._notify_game_update(changes, game_info)
                    summary = self._format_changes_summary(changes)
                    self._log_game_action("ОПРОС_ИГРА", game_info, "ДАННЫЕ ОБНОВЛЕНЫ", summary)
                else:
                    print(f"⏭️ Опрос для GameID {game_id} уже есть в сервисном листе")
                return False

        question = await self.create_game_poll(game_info)
        if not question:
            return False

        # Обновляем кэш после успешного создания опроса
        self._duplicate_check_cache[cache_key] = {"created": True}
        self._log_game_action("ОПРОС_ИГРА", game_info, "ОПРОС СОЗДАН", question)
        return True

    async def _process_today_game(self, game_info: Dict[str, Any]) -> bool:
        if not self._is_correct_time_for_announcements():
            return False

        game_id = game_info.get('game_id')
        if not game_id:
            print("⚠️ Нет GameID для анонса, пропускаем")
            return False

        widget_data = await self.fetch_widget_game_details(int(game_id))
        if widget_data:
            self._merge_widget_details(game_info, widget_data)

        existing_record = duplicate_protection.get_game_record("АНОНС_ИГРА", str(game_id))
        if existing_record and self._game_record_matches(existing_record, game_info):
            print(f"⏭️ Анонс для GameID {game_id} уже отправлен")
            return False

        announcement_sent = await self.send_game_announcement(game_info, game_link=game_info.get('game_link'))
        if not announcement_sent:
            return False

        summary = f"{game_info.get('date')} {game_info.get('time')} {game_info.get('team1')} vs {game_info.get('team2')}"
        self._log_game_action("АНОНС_ИГРА", game_info, "АНОНС ОТПРАВЛЕН", summary)
        return True

    async def fetch_letobasket_schedule(self) -> List[Dict]:
        """Получает расписание игр с сайта letobasket.ru"""
        try:
            import aiohttp
            from bs4 import BeautifulSoup
            
            url = "http://letobasket.ru/"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.text()
                        soup = BeautifulSoup(content, 'html.parser')
                        
                        # Получаем весь текст страницы
                        full_text = soup.get_text()
                        
                        # Ищем игры с нашими командами
                        games = []
                        
                        # Паттерн для игр в формате: дата время (место) - команда1 - команда2
                        # Поддерживаем разные форматы
                        game_patterns = [
                            # Основной паттерн: дата время (место) - команда1 - команда2
                            r'(\d{2}\.\d{2}\.\d{4})\s+(\d{2}\.\d{2})\s+\(([^)]+)\)\s*-\s*([^-]+?)\s*-\s*([^-]+?)(?:\n|$)',
                            # Паттерн для команд с пробелами и цифрами (например, "Атлант 40")
                            r'(\d{2}\.\d{2}\.\d{4})\s+(\d{2}\.\d{2})\s+\(([^)]+)\)\s*-\s*([^-]+?\s+\d+)\s*-\s*([^-]+?)(?:\n|$)',
                            r'(\d{2})\s+\(([^)]+)\)\s*-\s*([^-]+?)\s*-\s*([^-]+?)-(\d{2})',  # Новый формат с правильным захватом
                            r'(\d{2}\.\d{2}\.\d{4})\s*-\s*([^-]+?)\s*-\s*([^-]+?)\s+(\d+:\d+)',  # Формат с результатом: дата - команда1 - команда2 счет
                        ]
                        
                        # Дополнительный паттерн для строк с несколькими играми подряд
                        # Пример: "06.09.2025 12.30 (MarvelHall) - Team A - Team B-06.09.2025 14.00 (MarvelHall) - Team C - Team D"
                        # Исправленный паттерн для правильного захвата команд с дефсами
                        multi_game_pattern = r'(\d{2}\.\d{2}\.\d{4})\s+(\d{2}\.\d{2})\s+\(([^)]+)\)\s*-\s*([^-]+?)\s*-\s*([^-]+?)(?=-\d{2}\.\d{2}\.\d{4}|$)'
                        
                        # Дополнительный паттерн для команд с дефсами (например, "Team A-Team B")
                        multi_game_pattern_with_dash = r'(\d{2}\.\d{2}\.\d{4})\s+(\d{2}\.\d{2})\s+\(([^)]+)\)\s*-\s*([^-]+?)\s*-\s*([^-]+?-[^-]+?)(?=-\d{2}\.\d{2}\.\d{4}|$)'
                        
                        matches = []
                        for pattern in game_patterns:
                            pattern_matches = re.findall(pattern, full_text)
                            matches.extend(pattern_matches)
                        
                        # Обрабатываем паттерн для строк с несколькими играми
                        multi_game_matches = re.findall(multi_game_pattern, full_text)
                        matches.extend(multi_game_matches)
                        
                        # Обрабатываем паттерн для команд с дефсами
                        multi_game_dash_matches = re.findall(multi_game_pattern_with_dash, full_text)
                        matches.extend(multi_game_dash_matches)
                        
                        for match in matches:
                            # Проверяем формат матча
                            if len(match) == 5:
                                if len(match[0]) == 10:  # Старый формат: полная дата
                                    date, time, venue, team1, team2 = match
                                    # Нормализуем время (заменяем точку на двоеточие)
                                    time = time.replace('.', ':')
                                    
                                    # Исправляем год - игнорируем год с сайта и используем текущий
                                    date_parts = date.split('.')
                                    if len(date_parts) == 3:
                                        day, month, _ = date_parts  # Игнорируем год с сайта
                                        current_year = get_moscow_time().year
                                        date = f"{day}.{month}.{current_year}"
                                else:  # Новый формат: день месяца
                                    day, venue, team1, team2, month = match
                            elif len(match) == 4:  # Новый формат с результатом: дата - команда1 - команда2 - счет
                                date, team1, team2, score = match
                                # Исправляем год - игнорируем год с сайта и используем текущий
                                date_parts = date.split('.')
                                if len(date_parts) == 3:
                                    day, month, _ = date_parts  # Игнорируем год с сайта
                                    current_year = get_moscow_time().year
                                    date = f"{day}.{month}.{current_year}"
                                
                                # Устанавливаем время и место по умолчанию
                                time = "20:30"  # Время по умолчанию
                                venue = "ВО СШОР Малый 66"  # Место по умолчанию
                            else:
                                continue  # Пропускаем неправильные форматы
                            
                            # Очищаем названия команд от лишних пробелов и символов
                            team1 = team1.strip()
                            team2 = team2.strip()
                            
                            # Исправляем неправильно разделенные команды
                            # Здесь можно добавить пользовательские правила корректировки, если необходимо
                            
                            game_text = f"{team1} {team2}"
                            
                            # Проверяем, есть ли наши команды
                            if self.find_target_teams_in_text(game_text):
                                games.append({
                                    'date': date,
                                    'time': time,
                                    'team1': team1,
                                    'team2': team2,
                                    'venue': venue.strip(),
                                    'full_text': f"{date} {time} ({venue}) - {team1} - {team2}"
                                })
                        
                        if games:
                            # Исправляем год для всех игр (универсальное исправление)
                            current_year = get_moscow_time().year
                            for game in games:
                                date_parts = game['date'].split('.')
                                if len(date_parts) == 3:
                                    day, month, year = date_parts
                                    # Если год неправильный (например, 2022 вместо 2025), исправляем
                                    if int(year) != current_year:
                                        game['date'] = f"{day}.{month}.{current_year}"
                                        print(f"🔧 Исправлен год для игры: {day}.{month}.{year} → {game['date']}")
                            
                            print(f"✅ Найдено {len(games)} игр с нашими командами")
                            return games
                        else:
                            print("⚠️ Игры с нашими командами не найдены")
                            return []
                    else:
                        print(f"❌ Ошибка получения страницы: {response.status}")
                        return []
                        
        except Exception as e:
            print(f"❌ Ошибка получения расписания: {e}")
            return []
    
    def is_game_today(self, game_info: Dict) -> bool:
        """Проверяет, происходит ли игра сегодня"""
        try:
            return is_today(game_info['date'])
        except Exception as e:
            print(f"❌ Ошибка проверки даты игры: {e}")
            return False
    
    def should_create_poll(self, game_info: Dict) -> bool:
        """Проверяет, нужно ли создать опрос для игры"""
        # Проверяем время выполнения (расширенное окно)
        if not self._is_correct_time_for_polls():
            return False
        
        # Создаем уникальный ключ для игры
        game_key = create_game_key(game_info)
        print(f"🔍 Проверяем ключ опроса: {game_key}")
        
        # Проверяем защиту от дублирования через Google Sheets
        duplicate_result = duplicate_protection.check_duplicate("ОПРОС_ИГРА", game_key)
        if duplicate_result.get('exists', False):
            print(f"⏭️ Опрос для игры {game_key} уже создан (защита через Google Sheets)")
            return False
        
        # Проверяем, есть ли наши команды в игре
        target_teams: List[str] = []
        our_team_id = game_info.get('our_team_id')
        our_team_name = game_info.get('our_team_name')
        
        if our_team_id:
            label = our_team_name or f"Команда {our_team_id}"
            target_teams.append(label)
            print(f"✅ Найдена целевая команда по ID: {label} (ID {our_team_id})")
        else:
            game_text = f"{game_info.get('team1', '')} {game_info.get('team2', '')}"
            target_teams = self.find_target_teams_in_text(game_text)
        
        if not target_teams:
            print(f"ℹ️ Игра без наших команд: {game_info.get('team1', '')} vs {game_info.get('team2', '')}")
            return False
        
        print(f"✅ Найдены наши команды в игре: {', '.join(target_teams)}")
        
        # Проверяем, что игра в будущем (не создаем опросы для прошедших игр)
        game_date = None
        today = None
        try:
            game_date = datetime.datetime.strptime(game_info['date'], '%d.%m.%Y').date()
            today = get_moscow_time().date()
            
            if game_date < today:
                print(f"📅 Игра {game_info['date']} уже прошла, пропускаем")
                return False
        except Exception as e:
            print(f"⚠️ Ошибка проверки даты игры: {e}")
            return False  # Если не можем определить дату, не создаем опрос
        
        # Дополнительная проверка: не создаем опросы для игр, которые уже прошли по времени
        try:
            # Нормализуем время (заменяем точку на двоеточие)
            normalized_time = game_info['time'].replace('.', ':')
            game_time = datetime.datetime.strptime(normalized_time, '%H:%M').time()
            now = get_moscow_time().time()
            
            # Если игра сегодня и время уже прошло, не создаем опрос
            if game_date and today and game_date == today and game_time < now:
                print(f"⏰ Игра {game_info['date']} {game_info['time']} уже началась, пропускаем")
                return False
        except Exception as e:
            print(f"⚠️ Ошибка проверки времени игры: {e}")
        
        # Дополнительная проверка: не создаем опросы для игр, которые уже прошли
        try:
            # Нормализуем время (заменяем точку на двоеточие)
            normalized_time = game_info['time'].replace('.', ':')
            game_datetime = datetime.datetime.strptime(f"{game_info['date']} {normalized_time}", '%d.%m.%Y %H:%M')
            now = get_moscow_time()
            
            # Если игра уже прошла (более чем на 2 часа назад), не создаем опрос
            if game_datetime < now - datetime.timedelta(hours=2):
                print(f"⏰ Игра {game_info['date']} {game_info['time']} уже прошла, пропускаем")
                return False
        except Exception as e:
            print(f"⚠️ Ошибка проверки времени игры: {e}")
        
        # Ранее существовал ручной список исключений, но теперь вся логика опирается на данные из таблицы
        game_key = create_game_key(game_info)
        
        print(f"✅ Игра {game_info['date']} подходит для создания опроса")
        return True
    
    def should_send_announcement(self, game_info: Dict) -> bool:
        """Проверяет, нужно ли отправить анонс для игры"""
        # Проверяем время выполнения (расширенное окно)
        if not self._is_correct_time_for_announcements():
            return False
        
        # Создаем уникальный ключ для игры
        announcement_key = create_announcement_key(game_info)
        print(f"🔍 Проверяем ключ анонса: {announcement_key}")
        
        # Проверяем защиту от дублирования через Google Sheets
        duplicate_result = duplicate_protection.check_duplicate("АНОНС_ИГРА", announcement_key)
        if duplicate_result.get('exists', False):
            print(f"⏭️ Анонс для игры {announcement_key} уже отправлен (защита через Google Sheets)")
            return False
        
        # Проверяем, происходит ли игра сегодня
        if not self.is_game_today(game_info):
            print(f"📅 Игра {game_info['date']} не сегодня")
            return False
        
        # Проверяем, есть ли наши команды в игре
        our_team_id = game_info.get('our_team_id')
        our_team_name = game_info.get('our_team_name')
        target_teams: List[str] = []
        
        if our_team_id:
            label = our_team_name or f"Команда {our_team_id}"
            target_teams.append(label)
            print(f"✅ Найдена целевая команда по ID: {label} (ID {our_team_id})")
        else:
            game_text = f"{game_info.get('team1', '')} {game_info.get('team2', '')}"
            target_teams = self.find_target_teams_in_text(game_text)
        
        if not target_teams:
            print(f"ℹ️ Игра без наших команд: {game_info.get('team1', '')} vs {game_info.get('team2', '')}")
            return False
        
        print(f"✅ Найдены наши команды в игре: {', '.join(target_teams)}")
        print(f"✅ Игра {game_info['date']} подходит для анонса (сегодня)")
        return True
    
    def _is_correct_time_for_polls(self) -> bool:
        """Проверяет, подходящее ли время для создания опросов"""
        now = get_moscow_time()
        
        # Создаем опросы в течение всего дня (защита от дублирования через Google Sheets)
        print(f"🕐 Время подходящее для создания опросов: {now.strftime('%H:%M')} (весь день)")
        return True
    
    def _is_correct_time_for_announcements(self) -> bool:
        """Проверяет, подходящее ли время для отправки анонсов"""
        now = get_moscow_time()
        
        # Отправляем анонсы в течение всего дня (защита от дублирования через Google Sheets)
        print(f"🕐 Время подходящее для отправки анонсов: {now.strftime('%H:%M')} (весь день)")
        return True
    

    
    async def create_game_poll(self, game_info: Dict) -> Optional[str]:
        """Создает опрос для игры во все настроенные чаты и возвращает текст вопроса"""
        chat_ids = get_chat_ids()
        if not self.bot or not chat_ids:
            print("❌ Бот или CHAT_ID не настроены")
            return None
        
        try:
            bot = cast(Any, self.bot)
            # Определяем нашу команду и соперника
            team1 = game_info.get('team1', '')
            team2 = game_info.get('team2', '')
            team1_id = self._to_int(game_info.get('team1_id'))
            team2_id = self._to_int(game_info.get('team2_id'))
            
            # Находим нашу команду и соперника, опираясь на данные API
            our_team = game_info.get('our_team_name')
            opponent = game_info.get('opponent_team_name')
            our_team_id = self._to_int(game_info.get('our_team_id'))
            opponent_team_id = self._to_int(game_info.get('opponent_team_id'))
            
            if not our_team and our_team_id:
                if our_team_id == team1_id:
                    our_team = team1
                    opponent = opponent or team2
                elif our_team_id == team2_id:
                    our_team = team2
                    opponent = opponent or team1
            
            if not our_team:
                our_team = team1
                opponent = opponent or team2
            
            if our_team_id is not None:
                if our_team_id == team1_id:
                    fallback_name = team1
                elif our_team_id == team2_id:
                    fallback_name = team2
                else:
                    fallback_name = our_team
                our_team = self._resolve_team_name(our_team_id, fallback_name)

            if opponent_team_id is not None:
                if opponent_team_id == team1_id:
                    fallback_opponent = team1
                elif opponent_team_id == team2_id:
                    fallback_opponent = team2
                else:
                    fallback_opponent = opponent
                opponent = self._resolve_team_name(opponent_team_id, fallback_opponent)

            if not opponent:
                opponent = team2 if our_team == team1 else team1

            if not our_team:
                print(f"❌ Не удалось определить нашу команду в игре")
                return None
            
            # Определяем название команды для заголовка
            team_label = our_team.strip() if isinstance(our_team, str) and our_team.strip() else get_team_category_by_type(game_info.get('team_type'))
            day_of_week = get_day_of_week(game_info['date'])
            
            # Определяем цвет формы
            form_color = determine_form_color(game_info)
            
            # Форматируем дату без года
            date_short = format_date_without_year(game_info['date'])
            
            # Определяем название соревнования (если comp_id передан)
            comp_suffix = ""
            comp_id = game_info.get('comp_id')
            comp_name = get_comp_name(comp_id) if comp_id else ''
            if comp_name:
                comp_suffix = f" ({comp_name})"

            # Формируем вопрос в новом многострочном формате
            question = (
                f"🏀 {team_label} против {opponent}{comp_suffix}\n"
                f"📅 {date_short}, {day_of_week}, {game_info['time']}\n"
                f"👕 {form_color} форма\n"
                f"📍 {game_info['venue']}"
            )
            
            # Варианты ответов с эмодзи
            options = [
                "✅ Готов",
                "❌ Нет", 
                "👨‍🏫 Тренер"
            ]
            
            # Отправляем опрос во все настроенные чаты (с проверкой топика)
            try:
                chat_ids = get_chat_ids()
                if not chat_ids:
                    print("❌ Не настроены ID чатов (CHAT_ID)")
                    return None

                # Отправляем опрос в каждый чат
                poll_messages = []
                for chat_id in chat_ids:
                    send_kwargs: Dict[str, Any] = {
                        "chat_id": self._to_int(chat_id) or chat_id,
                        "question": question,
                        "options": options,
                        "is_anonymous": self.game_poll_is_anonymous,
                        "allows_multiple_answers": self.game_poll_allows_multiple,
                    }
                    message_thread_id = self.game_poll_topic_id
                    if message_thread_id is not None:
                        send_kwargs["message_thread_id"] = message_thread_id
                    poll_message = await bot.send_poll(**send_kwargs)
                    poll_messages.append(poll_message)

                # Используем первое сообщение для совместимости
                poll_message = poll_messages[0] if poll_messages else None
            except Exception as e:
                if "Message thread not found" in str(e):
                    thread_to_reset = send_kwargs.pop("message_thread_id", None)
                    if thread_to_reset is not None:
                        print(f"⚠️ Топик {thread_to_reset} не найден, отправляем в основной чат")
                        self.game_poll_topic_id = None
                    poll_message = await bot.send_poll(**send_kwargs)
                else:
                    raise e
            
            await self._send_calendar_event(bot, game_info, team_label, opponent, form_color)
            
            # Добавляем запись в сервисный лист для защиты от дублирования
            game_key = create_game_key(game_info)
            additional_info = f"{game_info['date']} {game_info['time']} vs {opponent} в {game_info['venue']}"
            print(f"✅ Опрос для игры создан в топике {self.game_poll_topic_id}")
            print(f"📊 ID опроса: {poll_message.poll.id}")
            print(f"📊 ID сообщения: {poll_message.message_id}")
            print(f"🏀 Формат: {question}")
            print(f"📅 Дата: {game_info['date']}")
            print(f"🕐 Время: {game_info['time']}")
            print(f"📍 Место: {game_info['venue']}")
            print(f"👥 Категория: {team_label}")
            print(f"👥 Наша команда: {our_team}")
            print(f"👥 Соперник: {opponent}")
            
            return question
            
        except Exception as e:
            print(f"❌ Ошибка создания опроса для игры: {e}")
            return None
    
    async def find_game_link(self, team1: str, team2: str) -> Optional[tuple]:
        """Ищет ссылку на игру, используя сервисный лист и fallback-источники"""
        try:
            sheet_link = duplicate_protection.find_game_link_for_today(team1, team2)
            if sheet_link:
                return sheet_link, None

            import aiohttp

            sources = self.fallback_sources or [{'url': 'http://letobasket.ru/'}]
            own_variants = self._build_name_variants(team1, *self.team_name_keywords)
            opponent_variants = self._build_name_variants(team2)

            async with aiohttp.ClientSession() as session:
                for source in sources:
                    url = source.get('url')
                    if not url:
                        continue
                    try:
                        result = await self._search_fallback_source(session, url, own_variants, opponent_variants)
                        if result:
                            return result
                    except Exception as source_error:
                        print(f"⚠️ Не удалось обработать fallback-источник {url}: {source_error}")

            print(f"⚠️ Ссылка на игру {team1} vs {team2} не найдена ни в одном fallback-источнике")
            return None
        except Exception as e:
            print(f"❌ Ошибка поиска ссылки на игру: {e}")
            return None

    async def _search_fallback_source(
        self,
        session: "aiohttp.ClientSession",
        url: str,
        own_variants: Set[str],
        opponent_variants: Set[str]
    ) -> Optional[tuple]:
        from bs4 import BeautifulSoup

        async with session.get(url) as response:
            if response.status != 200:
                print(f"⚠️ Fallback {url} вернул статус {response.status}")
                return None
            content = await response.text()

        soup = BeautifulSoup(content, 'html.parser')
        anchors = soup.find_all('a', href=True)
        print(f"🔗 {url}: найдено {len(anchors)} ссылок")

        # Сначала пробуем найти по тексту ссылки (быстрее, не требует загрузки страницы игры)
        for anchor in anchors:
            href = anchor.get('href')
            if not href:
                continue
            
            # Проверяем, что это ссылка на игру
            is_game_link = 'gameId=' in href or 'game.html' in href
            if not is_game_link:
                continue
            
            # Получаем текст ссылки (обычно содержит названия команд)
            link_text = anchor.get_text(strip=True)
            if not link_text:
                # Если текста нет, проверяем title или другие атрибуты
                link_text = anchor.get('title', '') or anchor.get('aria-label', '')
                if not link_text:
                continue
            
            # Нормализуем текст для поиска
            normalized_text = self._normalize_name_for_search(link_text)
            
            # Проверяем, есть ли обе команды в тексте ссылки
            own_match = self._find_matching_variant(normalized_text, list(own_variants))
            opponent_match = self._find_matching_variant(normalized_text, list(opponent_variants))
            
            # Если не нашли обе команды, пробуем более гибкий поиск:
            # Разбиваем текст ссылки по разделителям и проверяем каждую часть отдельно
            if not (own_match and opponent_match):
                # Разделители для парсинга названий команд в тексте ссылки
                separators = [r'\s*[-–—]\s*', r'\s+против\s+', r'\s+vs\s+', r'\s+и\s+', r'\s+vs\.\s+']
                
                for sep_pattern in separators:
                    parts = re.split(sep_pattern, link_text, flags=re.IGNORECASE)
                    if len(parts) >= 2:
                        # Проверяем каждую часть отдельно
                        for part in parts:
                            part_normalized = self._normalize_name_for_search(part)
                            if not own_match:
                                own_match = self._find_matching_variant(part_normalized, list(own_variants))
                            if not opponent_match:
                                opponent_match = self._find_matching_variant(part_normalized, list(opponent_variants))
                        
                        # Также проверяем комбинации соседних частей (для команд с дефисами)
                        # Например, "Военмех-Vintage" может быть разбито на "Военмех" и "Vintage"
                        for i in range(len(parts) - 1):
                            combined = f"{parts[i]}-{parts[i+1]}"
                            combined_normalized = self._normalize_name_for_search(combined)
                            if not own_match:
                                own_match = self._find_matching_variant(combined_normalized, list(own_variants))
                            if not opponent_match:
                                opponent_match = self._find_matching_variant(combined_normalized, list(opponent_variants))
                            
                            # Также пробуем без дефиса
                            combined_no_hyphen = f"{parts[i]}{parts[i+1]}"
                            combined_no_hyphen_normalized = self._normalize_name_for_search(combined_no_hyphen)
                            if not own_match:
                                own_match = self._find_matching_variant(combined_no_hyphen_normalized, list(own_variants))
                            if not opponent_match:
                                opponent_match = self._find_matching_variant(combined_no_hyphen_normalized, list(opponent_variants))
                        
                        if own_match and opponent_match:
                            break
                
                # Если все еще не нашли, проверяем родительский элемент (может содержать текст)
                if not (own_match and opponent_match):
                    parent = anchor.parent
                    if parent:
                        parent_text = parent.get_text(strip=True)
                        if parent_text and len(parent_text) > len(link_text):
                            parent_normalized = self._normalize_name_for_search(parent_text)
                            if not own_match:
                                own_match = self._find_matching_variant(parent_normalized, list(own_variants))
                            if not opponent_match:
                                opponent_match = self._find_matching_variant(parent_normalized, list(opponent_variants))
            
            if own_match and opponent_match:
                full_link = href if href.startswith('http') else urljoin(url, href)
                print(f"✅ Найдена подходящая игра в fallback по тексту ссылки: {full_link}")
                print(f"   Текст ссылки: {link_text}")
                print(f"   Наша команда: {own_match}, Соперник: {opponent_match}")
                return full_link, own_match
        
        # Если не нашли по тексту, пробуем старый способ (проверка содержимого страницы игры)
        print(f"🔍 Поиск по тексту ссылки не дал результатов, проверяем содержимое страниц игр...")
        for anchor in anchors:
            href = anchor.get('href')
            if not href or ('gameId=' not in href and 'game.html' not in href):
                continue
            full_link = urljoin(url, href)
            matched_name = await self._verify_game_link(session, full_link, own_variants, opponent_variants)
            if matched_name:
                print(f"✅ Найдена подходящая игра в fallback: {full_link}")
                return full_link, matched_name
        return None

    async def _verify_game_link(
        self,
        session: "aiohttp.ClientSession",
        link: str,
        own_variants: Set[str],
        opponent_variants: Set[str]
    ) -> Optional[str]:
        try:
            async with session.get(link) as response:
                if response.status != 200:
                    return None
                content = await response.text()
        except Exception as e:
            print(f"⚠️ Ошибка при проверке fallback ссылки {link}: {e}")
            return None

        normalized_content = self._normalize_name_for_search(content)
        own_match = self._find_matching_variant(normalized_content, list(own_variants))
        opponent_match = self._find_matching_variant(normalized_content, list(opponent_variants))

        if own_match and opponent_match:
            return own_match
        return None
    

    async def _fetch_opponent_highlights(self, game_info: Dict[str, Any]) -> List[str]:
        highlights: List[str] = []
        try:
            import aiohttp

            game_id = self._to_int(game_info.get('game_id') or game_info.get('GameID'))
            opponent_team_id = self._to_int(game_info.get('opponent_team_id') or game_info.get('opponentTeamId'))
            if not game_id or not opponent_team_id:
                return highlights

            url = f"https://reg.infobasket.su/Comp/GetTeamStatsForPreview/{game_id}?compId=0"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        print(f"⚠️ Не удалось получить превью статистику соперника: {response.status}")
                        return highlights
                    data = await response.json()

            if not isinstance(data, list):
                return highlights

            def safe_float(value: Any) -> float:
                if value is None:
                    return 0.0
                if isinstance(value, (int, float)):
                    return float(value)
                if isinstance(value, str):
                    value = value.replace(',', '.')
                    try:
                        return float(value)
                    except ValueError:
                        return 0.0
                return 0.0

            opponent_data: Optional[Dict[str, Any]] = None
            for team in data:
                if self._to_int(team.get('TeamID')) == opponent_team_id:
                    opponent_data = team
                    break

            if opponent_data is None and len(data) == 2:
                our_team_id = self._to_int(game_info.get('our_team_id'))
                for team in data:
                    if self._to_int(team.get('TeamID')) != our_team_id:
                        opponent_data = team
                        break

            if not opponent_data:
                return highlights

            players = opponent_data.get('Players') or []
            if not players:
                return highlights

            def build_name(player: Dict[str, Any]) -> str:
                person = player.get('PersonInfo') or {}
                last_name = person.get('PersonLastNameRu') or person.get('PersonLastNameEn') or ''
                first_name = person.get('PersonFirstNameRu') or person.get('PersonFirstNameEn') or ''
                full_name = (last_name + ' ' + first_name).strip()
                if not full_name:
                    full_name = player.get('PlayerName') or 'Игрок'
                return full_name

            def player_number(player: Dict[str, Any]) -> str:
                number = player.get('DisplayNumber') or player.get('PlayerNumber')
                if number in (None, ''):
                    return '--'
                return str(number)

            metrics = [
                ('AvgPoints', 'очки', 'очков'),
                ('AvgRebound', 'подборы', 'подборов'),
                ('AvgAssist', 'передачи', 'передач'),
                ('AvgSteal', 'перехваты', 'перехватов'),
                ('AvgKPI', 'КПИ', 'ед. КПИ'),
            ]

            player_entries: Dict[str, Dict[str, Any]] = {}
            player_order: List[str] = []

            for field, descriptor, unit in metrics:
                leader = None
                best_value = -1.0
                for player in players:
                    value = safe_float(player.get(field))
                    if value > best_value:
                        best_value = value
                        leader = player
                if not leader or best_value <= 0:
                    continue

                leader_id = (
                    leader.get('PersonID')
                    or leader.get('PlayerID')
                    or (leader.get('PersonInfo') or {}).get('PersonID')
                )
                if leader_id is None:
                    leader_id = f"{player_number(leader)}-{descriptor}"

                leader_key = str(leader_id)
                if leader_key not in player_entries:
                    player_entries[leader_key] = {
                        'name': build_name(leader),
                        'number': player_number(leader),
                        'entries': []
                    }
                    player_order.append(leader_key)

                player_entries[leader_key]['entries'].append(
                    f"{descriptor} ({best_value:.1f} {unit} за игру)"
                )

            for key in player_order:
                info = player_entries[key]
                entries_text = ', '.join(info['entries'])
                highlights.append(
                    f"• №{info['number']} {info['name']} — {entries_text}"
                )

            return highlights
        except Exception as error:
            print(f"⚠️ Не удалось подготовить подсказки по сопернику: {error}")
            return highlights


    def format_announcement_message(self, game_info: Dict, game_link: Optional[str] = None, found_team: Optional[str] = None, opponent_highlights: Optional[List[str]] = None) -> str:
        """Форматирует сообщение анонса игры"""
        team1 = game_info.get('team1', '')
        team2 = game_info.get('team2', '')
        our_team_id = game_info.get('our_team_id')
        
        our_team = found_team or game_info.get('our_team_name')
        opponent = game_info.get('opponent_team_name')
        
        if not our_team and our_team_id:
            if our_team_id == game_info.get('team1_id'):
                our_team = team1
                opponent = opponent or team2
            elif our_team_id == game_info.get('team2_id'):
                our_team = team2
                opponent = opponent or team1
        
        if not our_team:
            our_team = team1
        
        if not opponent:
            opponent = team2 if our_team == team1 else team1
        
        form_color = determine_form_color(game_info)
        normalized_time = (game_info.get('time') or '').replace('.', ':')
        venue = game_info.get('venue') or 'Место уточняется'

        announcement = (
            f"🏀 Сегодня игра {our_team} против {opponent}.\n"
            f"👕 {form_color} форма\n"
            f"📍 Место проведения: {venue}\n"
            f"🕐 Время игры: {normalized_time}"
        )
        
        if game_link:
            full_url = game_link if game_link.startswith('http') else f"http://letobasket.ru/{game_link}"
            announcement += f"\n🔗 Ссылка на игру: <a href=\"{full_url}\">тут</a>"

        if opponent_highlights:
            announcement += "\n\n⚠️ Лидеры соперника:\n"
            for highlight in opponent_highlights:
                announcement += f"{highlight}\n"

        return announcement
    
    def format_game_result_message(self, game_info: Dict, game_link: Optional[str] = None, our_team_leaders: Optional[Dict] = None) -> str:
        """Форматирует сообщение с результатами игры, включая лидеров нашей команды"""
        try:
            team1 = game_info.get('team1', '')
            team2 = game_info.get('team2', '')
            our_team_id = game_info.get('our_team_id')
            
            our_team = game_info.get('our_team_name')
            opponent = game_info.get('opponent_team_name')
            
            if not our_team and our_team_id:
                if our_team_id == game_info.get('team1_id'):
                    our_team = team1
                    opponent = opponent or team2
                elif our_team_id == game_info.get('team2_id'):
                    our_team = team2
                    opponent = opponent or team1
            
            if not our_team:
                our_team = team1
                opponent = opponent or team2
            
            if not opponent:
                opponent = team2 if our_team == team1 else team1
            
            team_category = get_team_category_with_declension(game_info.get('team_type'))
            
            our_score = game_info.get('our_score', '?')
            opponent_score = game_info.get('opponent_score', '?')
            
            if our_score != '?' and opponent_score != '?':
                try:
                    our_score_int = int(our_score)
                    opponent_score_int = int(opponent_score)
                    if our_score_int > opponent_score_int:
                        result_emoji = "✅"
                        result_text = "ПОБЕДА"
                    elif our_score_int < opponent_score_int:
                        result_emoji = "❌"
                        result_text = "ПОРАЖЕНИЕ"
                    else:
                        result_emoji = "🤝"
                        result_text = "НИЧЬЯ"
                except ValueError:
                    result_emoji = "🏀"
                    result_text = "РЕЗУЛЬТАТ"
            else:
                result_emoji = "🏀"
                result_text = "РЕЗУЛЬТАТ"
            
            message = (
                f"{result_emoji} {result_text}: {our_team} против {opponent}\n"
                f"🏀 {our_team} {our_score}:{opponent_score} {opponent}\n"
            )

            quarters_data = game_info.get('quarters')
            quarter_scores: List[str] = []
            if isinstance(quarters_data, list):
                for entry in quarters_data:
                    if isinstance(entry, dict):
                        score = entry.get('total')
                        if not score:
                            score1 = entry.get('score1')
                            score2 = entry.get('score2')
                            if score1 is not None and score2 is not None:
                                score = f"{score1}:{score2}"
                        if score:
                            quarter_scores.append(str(score))
                    elif entry is not None:
                        score = str(entry).strip()
                        if score:
                            quarter_scores.append(score)
            elif isinstance(quarters_data, str):
                cleaned = quarters_data.strip()
                if cleaned:
                    quarter_scores.append(cleaned)

            if quarter_scores:
                message += f"📈 Четверти: {' · '.join(quarter_scores)}\n"

            normalized_time = (game_info.get('time', '') or '').replace('.', ':')
            date_line = f"📅 {game_info.get('date', '')} в {normalized_time}\n"

            if game_link:
                full_url = game_link if game_link.startswith('http') else f"http://letobasket.ru/{game_link}"
                if '#protocol' not in full_url:
                    if '#' in full_url:
                        full_url = full_url.replace('#', '#protocol')
                    else:
                        full_url = f"{full_url}#protocol"
                message += f"🔗 <a href=\"{full_url}\">Протокол</a>\n"
                message += date_line
            else:
                message += date_line
            
            if our_team_leaders:
                our_score_val = game_info.get('our_score', '?')
                opponent_score_val = game_info.get('opponent_score', '?')
                is_victory = False
                try:
                    is_victory = int(our_score_val) > int(opponent_score_val)
                except (TypeError, ValueError):
                    is_victory = False
                
                if is_victory:
                    message += "\n😅 ЧТО НУЖНО УЛУЧШИТЬ:\n"
                    anti_leaders = our_team_leaders.get('anti_leaders', {})
                    if anti_leaders:
                        if 'worst_free_throw' in anti_leaders:
                            data = anti_leaders['worst_free_throw']
                            message += f"🏀 Штрафные: {data['name']} - {data['value']}%\n"
                        if 'worst_two_point' in anti_leaders:
                            data = anti_leaders['worst_two_point']
                            message += f"🎯 Двухочковые: {data['name']} - {data['value']}%\n"
                        if 'worst_three_point' in anti_leaders:
                            data = anti_leaders['worst_three_point']
                            message += f"🎯 Трехочковые: {data['name']} - {data['value']}%\n"
                        if 'turnovers' in anti_leaders:
                            data = anti_leaders['turnovers']
                            message += f"💥 Потери: {data['name']} - {data['value']}\n"
                        if 'fouls' in anti_leaders:
                            data = anti_leaders['fouls']
                            message += f"⚠️ Фолы: {data['name']} - {data['value']}\n"
                        if 'worst_kpi' in anti_leaders:
                            data = anti_leaders['worst_kpi']
                            message += f"📉 КПИ: {data['name']} - {data['value']}\n"
                else:
                    message += "\n🏆 ЛУЧШИЕ ИГРОКИ:\n"
                    if 'points' in our_team_leaders:
                        data = our_team_leaders['points']
                        message += f"🥇 Очки: {data['name']} - {data['value']} ({data.get('percentage', 0)}%)\n"
                    if 'rebounds' in our_team_leaders:
                        data = our_team_leaders['rebounds']
                        message += f"🏀 Подборы: {data['name']} - {data['value']}\n"
                    if 'assists' in our_team_leaders:
                        data = our_team_leaders['assists']
                        message += f"🎯 Передачи: {data['name']} - {data['value']}\n"
                    if 'steals' in our_team_leaders:
                        data = our_team_leaders['steals']
                        message += f"🥷 Перехваты: {data['name']} - {data['value']}\n"
                    if 'best_kpi' in our_team_leaders:
                        data = our_team_leaders['best_kpi']
                        message += f"📈 КПИ: {data['name']} - {data['value']}\n"
            
            return message
            
        except Exception as e:
            print(f"❌ Ошибка форматирования сообщения с результатами: {e}")
            return f"🏀 Результат игры: {game_info.get('team1', '')} vs {game_info.get('team2', '')}"
    
    async def send_game_announcement(self, game_info: Dict, game_position: int = 1, game_link: Optional[str] = None, found_team: Optional[str] = None) -> bool:
        """Отправляет анонс игры в основной топик"""
        if not self.bot or not CHAT_ID:
            print("❌ Бот или CHAT_ID не настроены")
            return False
        
        try:
            bot = cast(Any, self.bot)
            # Если game_link не передан, ищем ссылку на игру по командам
            if game_link is None:
                print(f"⚠️ Ссылка на игру для GameID {game_info.get('game_id')} не передана")
                found_team = None
            
            form_color = determine_form_color(game_info)
            game_info.setdefault('form_color', form_color)

            opponent_highlights = await self._fetch_opponent_highlights(game_info)

            # Формируем сообщение анонса
            announcement_text = self.format_announcement_message(game_info, game_link, found_team, opponent_highlights)

            # Мониторинг результатов будет запущен автоматически за 5 минут до игры через отдельный workflow
            if game_link:
                print("🎮 Мониторинг результатов будет запущен автоматически за 5 минут до игры")

            # Отправляем сообщение в основной топик (без указания топика)
            message = await bot.send_message(
                chat_id=int(CHAT_ID),
                text=announcement_text,
                parse_mode='HTML'
            )

            # Сохраняем информацию об анонсе в сервисный лист
            announcement_key = create_announcement_key(game_info)
            
            our_team_label = self._get_team_display_name(self._to_int(game_info.get('our_team_id')), game_info.get('our_team_name') or game_info.get('team1'))
            opponent_label = self._get_team_display_name(self._to_int(game_info.get('opponent_team_id')), game_info.get('opponent_team_name') or game_info.get('team2'))
            additional_info = " | ".join(filter(None, [
                f"{game_info.get('date', '')} {game_info.get('time', '')}".strip(),
                f"{our_team_label} vs {opponent_label}".strip(),
                f"Форма: {form_color}" if form_color else '',
                f"Место: {game_info.get('venue', '')}".strip()
            ]))

            duplicate_protection.add_record(
                "АНОНС_ИГРА",
                announcement_key,
                "ОТПРАВЛЕН",
                additional_info,
                game_link or '',
                comp_id=self._to_int(game_info.get('comp_id')),
                team_id=self._to_int(game_info.get('our_team_id')),
                alt_name=our_team_label,
                settings='',
                game_id=self._to_int(game_info.get('game_id')),
                game_date=game_info.get('date', ''),
                game_time=game_info.get('time', ''),
                arena=game_info.get('venue', ''),
                team_a_id=self._to_int(game_info.get('team1_id')),
                team_b_id=self._to_int(game_info.get('team2_id'))
            )

            print("✅ Анонс игры отправлен в основной топик")
            print(f"📊 ID сообщения: {message.message_id}")
            print(f"📅 Дата: {game_info.get('date', '')}")
            print(f"🕐 Время: {game_info.get('time', '')}")
            print(f"👕 Форма: {form_color}")
            print(f"📍 Место: {game_info.get('venue', '')}")
            print(f"🎯 Позиция в табло: {game_position}")
            if game_link:
                print(f"🔗 Ссылка: {game_link}")

            return True
            
        except Exception as e:
            print(f"❌ Ошибка отправки анонса игры: {e}")
            return False
    

    
    async def run_full_system(self):
        """Запускает полную систему: парсинг → опросы → анонсы"""
        try:
            print("🚀 ЗАПУСК ПОЛНОЙ СИСТЕМЫ УПРАВЛЕНИЯ ИГРАМИ")
            print("=" * 60)
            
            # Используем централизованное логирование времени
            time_info = log_current_time()
            print(f"🕐 Текущее время (Москва): {time_info['formatted_datetime']}")
            print(f"📅 День недели: {time_info['weekday_name']}")
            
            print(f"\n🔧 НАСТРОЙКИ:")
            latest_config = duplicate_protection.get_config_ids()
            self.config_comp_ids = latest_config.get('comp_ids', [])
            self.config_team_ids = latest_config.get('team_ids', [])
            self.config_comp_ids_set = set(self.config_comp_ids)
            self.config_team_ids_set = set(self.config_team_ids)
            self.team_configs = latest_config.get('teams', {}) or {}
            self.training_poll_configs = latest_config.get('training_polls', []) or []
            self.voting_configs = latest_config.get('voting_polls', []) or []
            self.fallback_sources = latest_config.get('fallback_sources', []) or []
            self.automation_topics = latest_config.get('automation_topics', {}) or {}
            self._update_team_mappings()
            print(f"   CHAT_ID: {CHAT_ID}")
            print(
                "   GAME_POLLS: "
                f"topic={self.game_poll_topic_id}, anonymous={self.game_poll_is_anonymous}, "
                f"multiple={self.game_poll_allows_multiple}"
            )
            print(
                "   GAME_ANNOUNCEMENTS: "
                f"topic={self.game_announcement_topic_id}"
            )
            print(f"   ТЕСТОВЫЙ РЕЖИМ: {'✅ ВКЛЮЧЕН' if TEST_MODE else '❌ ВЫКЛЮЧЕН'}")
            print(f"   ⚙️ Соревнования для мониторинга: {self.config_comp_ids or 'не заданы'}")
            print(f"   ⚙️ Команды (ID): {self.config_team_ids or 'не заданы'}")
            print(f"   ⚙️ Названия команд: {self.team_name_keywords or 'не заданы'}")
            print(f"   ⚙️ Конфигурации опросов тренировок: {len(self.training_poll_configs)}")
            print(f"   ⚙️ Конфигурации голосований: {len(self.voting_configs)}")
            print(f"   ⚙️ Fallback-источники: {len(self.fallback_sources)}")
            cleanup_result = duplicate_protection.cleanup_expired_records(30)
            if cleanup_result.get('success'):
                cleaned_count = cleanup_result.get('cleaned_count', 0)
                if cleaned_count > 0:
                    print(f"🧹 Автоочистка сервисного листа: удалено {cleaned_count} записей старше 30 дней")
                else:
                    print("🧹 Автоочистка сервисного листа: старые записи не найдены")
            else:
                print(f"⚠️ Не удалось выполнить автоочистку сервисного листа: {cleanup_result.get('error')}")
            
            # ШАГ 1: Парсинг расписания
            print(f"\n📊 ШАГ 1: ПАРСИНГ РАСПИСАНИЯ")
            print("-" * 40)
            games_by_status = await self.fetch_infobasket_schedule()
            future_games = games_by_status.get('future', [])
            today_games = games_by_status.get('today', [])
            total_games = len(future_games) + len(today_games)
            if total_games == 0:
                print("⚠️ Игры не найдены, завершаем работу")
                return
            print(f"✅ Найдено {total_games} игр (будущие: {len(future_games)}, сегодня: {len(today_games)})")
            
            # ШАГ 2: Создание опросов
            print(f"\n📊 ШАГ 2: СОЗДАНИЕ ОПРОСОВ")
            print("-" * 40)
            
            # Очищаем кэш перед обработкой новых игр
            self._duplicate_check_cache.clear()
            
            # Удаляем дубликаты из списка игр (по game_id)
            seen_game_ids = set()
            unique_future_games = []
            for game in future_games:
                game_id = game.get('game_id')
                if game_id and game_id not in seen_game_ids:
                    seen_game_ids.add(game_id)
                    unique_future_games.append(game)
                elif not game_id:
                    # Игры без game_id тоже добавляем (на случай fallback)
                    unique_future_games.append(game)
            
            if len(future_games) != len(unique_future_games):
                print(f"⚠️ Найдено {len(future_games) - len(unique_future_games)} дубликатов в списке игр, удалены")
            
            created_polls = 0
            for game in unique_future_games:
                print(f"\n🏀 Проверка игры (будущая): {game.get('team1', '')} vs {game.get('team2', '')}")
                if await self._process_future_game(game):
                    created_polls += 1
            print(f"✅ Создано {created_polls} опросов")
            
            # ШАГ 3: Создание анонсов
            print(f"\n📢 ШАГ 3: СОЗДАНИЕ АНОНСОВ")
            print("-" * 40)
            sent_announcements = 0
            for game in today_games:
                print(f"\n🏀 Проверка игры (сегодня): {game.get('team1', '')} vs {game.get('team2', '')}")
                if await self._process_today_game(game):
                    sent_announcements += 1
            print(f"✅ Отправлено {sent_announcements} анонсов")
            
            # Итоги
            print(f"\n📊 ИТОГИ РАБОТЫ:")
            print(f"   📊 Создано опросов: {created_polls}")
            print(f"   📢 Отправлено анонсов: {sent_announcements}")
            print(f"   📋 Всего игр обработано: {total_games}")
            
        except Exception as e:
            print(f"❌ Ошибка выполнения системы: {e}")

# Глобальный экземпляр
game_system_manager = GameSystemManager()

async def main():
    """Основная функция"""
    await game_system_manager.run_full_system()

if __name__ == "__main__":
    asyncio.run(main())
