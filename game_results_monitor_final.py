#!/usr/bin/env python3
# pyright: reportGeneralTypeIssues=false, reportArgumentType=false, reportCallIssue=false
"""
Финальная система мониторинга результатов игр
Production версия для ежедневного запуска
"""

import asyncio
import os
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from dotenv import load_dotenv
from telegram import Bot
from datetime_utils import get_moscow_time
from game_system_manager import GameSystemManager
from enhanced_duplicate_protection import duplicate_protection, TEST_MODE

# Централизованная загрузка переменных окружения
def load_environment():
    """Загружает переменные окружения с обработкой ошибок"""
    try:
        load_dotenv()
        print("✅ .env файл загружен успешно")
    except Exception as e:
        print(f"⚠️ Ошибка загрузки .env файла: {e}")
        print("📋 Продолжаем работу с переменными окружения из системы")

# Загружаем переменные окружения
load_environment()

# Переменные окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
ANNOUNCEMENTS_TOPIC_ID = os.getenv("ANNOUNCEMENTS_TOPIC_ID")

print(f"🔧 ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:")
print(f"   BOT_TOKEN: {'✅' if BOT_TOKEN else '❌'}")
print(f"   CHAT_ID: {'✅' if CHAT_ID else '❌'}")
print(f"   ANNOUNCEMENTS_TOPIC_ID: {'✅' if ANNOUNCEMENTS_TOPIC_ID else '❌'}")

class GameResultsMonitorFinal:
    """Финальная система мониторинга результатов игр"""
    
    def __init__(self):
        self.bot = None
        if BOT_TOKEN:
            self.bot = Bot(token=BOT_TOKEN)
        
        # Создаем экземпляр менеджера игр
        self.game_manager = GameSystemManager()
    
    def create_result_key(self, game_info: Dict) -> str:
        """Создает уникальный ключ для результата игры"""
        # Нормализуем названия команд для избежания дублирования
        team1 = game_info['team1'].strip().replace(' ', '_')
        team2 = game_info['team2'].strip().replace(' ', '_')
        date = game_info['date']
        
        key = f"result_{date}_{team1}_{team2}"
        print(f"🔑 Создан ключ результата: {key}")
        return key
    
    
    def should_check_results(self) -> bool:
        """Проверяет, нужно ли проверять результаты - всегда True"""
        now = get_moscow_time()
        print(f"🕐 Время запуска: {now.strftime('%H:%M:%S')} MSK")
        print(f"📅 День недели: {['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'][now.weekday()]}")
        print("✅ Бот всегда готов к проверке результатов")
        return True
    
    async def fetch_game_results(self) -> List[Dict]:
        """Получает результаты игр с сайта letobasket.ru"""
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
                        
                        # Ищем завершенные игры с нашими командами
                        games = []
                        
                        # Правильный паттерн для результатов игр на сайте
                        # Формат: дата - команда1 - команда2 счет (четверти)
                        # Пример: 23.08.2025- Team A - Team B 37:58 (0:12 11:10 15:10 11:26)
                        game_pattern = r'(\d{2}\.\д{2}\.\д{4})-\s*([^-]+)-\s*([^-]+)\s+(\д+):(\д+)\s+\(([^)]+)\)'
                        matches = re.findall(game_pattern, full_text)
                        
                        print(f"🔍 Найдено {len(matches)} потенциальных игр в тексте")
                        
                        for match in matches:
                            date, raw_team1, raw_team2, score1, score2, quarters = match
                            team1 = raw_team1.strip()
                            team2 = raw_team2.strip()
                            score1_int = int(score1)
                            score2_int = int(score2)
                            game_text = f"{team1} {team2}"
                            
                            # Проверяем, что игра сегодняшняя и содержит нашу команду
                            if self.game_manager.is_game_today({'date': date}) and self.game_manager.find_target_teams_in_text(game_text):
                                team1_config = self.game_manager.resolve_team_config(team1)
                                team2_config = self.game_manager.resolve_team_config(team2)
                                team1_matches = bool(team1_config) or bool(self.game_manager.find_target_teams_in_text(team1))
                                team2_matches = bool(team2_config) or bool(self.game_manager.find_target_teams_in_text(team2))
                                
                                our_team = None
                                opponent = None
                                matched_config = None
                                
                                if team1_matches and not team2_matches:
                                    our_team = team1
                                    opponent = team2
                                    matched_config = team1_config
                                elif team2_matches and not team1_matches:
                                    our_team = team2
                                    opponent = team1
                                    matched_config = team2_config
                                elif team1_matches and team2_matches:
                                    if team1_config:
                                        our_team = team1
                                        opponent = team2
                                        matched_config = team1_config
                                    elif team2_config:
                                        our_team = team2
                                        opponent = team1
                                        matched_config = team2_config
                                    else:
                                        # Оба названия совпали по текстовому поиску, выбираем первую команду
                                        our_team = team1
                                        opponent = team2
                                
                                if our_team:
                                    metadata = (matched_config or {}).get('metadata') or {}
                                    team_type = metadata.get('team_type') or metadata.get('type') or 'Команда'
                                    our_score = score1_int if our_team == team1 else score2_int
                                    opponent_score = score2_int if our_team == team1 else score1_int
                                    result = "победа" if our_score > opponent_score else "поражение" if our_score < opponent_score else "ничья"
                                    
                                    game_info = {
                                        'date': date,
                                        'team1': team1,
                                        'team2': team2,
                                        'score1': score1_int,
                                        'score2': score2_int,
                                        'quarters': quarters,
                                        'our_team': our_team,
                                        'opponent': opponent,
                                        'team_type': team_type,
                                        'our_score': our_score,
                                        'opponent_score': opponent_score,
                                        'result': result,
                                        'is_finished': True
                                    }
                                    games.append(game_info)
                                    print(f"🏀 Найдена завершенная игра: {team1} vs {team2} ({score1}:{score2})")
                                    print(f"   Дата: {date}, Тип: {team_type}, Результат: {result}")
                                    print(f"   Четверти: {quarters}")
                            else:
                                print(f"⏭️ Игра {team1} vs {team2} не соответствует условиям (дата: {date})")
                        
                        return games
                    else:
                        print(f"❌ Ошибка получения страницы: {response.status}")
                        return []
                        
        except Exception as e:
            print(f"❌ Ошибка получения результатов: {e}")
            return []
    
    async def fetch_game_results_from_links(self) -> List[Dict]:
        """Получает результаты игр используя ссылки из сервисного листа"""
        try:
            from enhanced_duplicate_protection import duplicate_protection
            from datetime_utils import get_moscow_time
            
            today = get_moscow_time().strftime('%d.%m.%Y')
            games = []
            
            # Получаем все данные из сервисного листа
            worksheet = duplicate_protection._get_service_worksheet()
            if not worksheet:
                print("❌ Сервисный лист недоступен")
                return []
            
            all_data = worksheet.get_all_values()
            
            # Ищем записи типа АНОНС_ИГРА за сегодня с ссылками
            for row in all_data:
                if (len(row) >= 6 and 
                    row[0] == "АНОНС_ИГРА" and 
                    today in row[1] and 
                    row[5]):  # Есть ссылка
                    
                    game_link = row[5]
                    if not game_link.startswith('http'):
                        game_link = f"http://letobasket.ru/{game_link}"
                    
                    print(f"🔍 Парсим игру по ссылке: {game_link}")
                    
                    # Парсим игру используя улучшенный парсер
                    game_info = await self.parse_game_from_link(game_link)
                    if game_info:
                        games.append(game_info)
                        print(f"✅ Игра добавлена: {game_info['our_team']} vs {game_info['opponent']} - {game_info['result']}")
                    else:
                        print(f"❌ Не удалось распарсить игру")
            
            return games
            
        except Exception as e:
            print(f"❌ Ошибка получения результатов по ссылкам: {e}")
            return []
    
    async def find_game_link(self, team1: str, team2: str, game_date: str = None) -> Optional[str]:
        """Ищет ссылку на игру по командам (сначала в сервисном листе, потом в анонсах, потом в табло)"""
        try:
            # 1. Сначала ищем в сервисном листе Google Sheets (самый надежный способ)
            from enhanced_duplicate_protection import duplicate_protection
            link_from_service_sheet = duplicate_protection.find_game_link_for_today(team1, team2)
            if link_from_service_sheet:
                print(f"🔗 Найдена ссылка в сервисном листе: {link_from_service_sheet}")
                return link_from_service_sheet
            
            # 2. Если не найдено в сервисном листе, ищем в анонсах игр
            print(f"🔍 Ссылка не найдена в сервисном листе, ищем в анонсах...")
            link_from_announcements = self.find_link_in_announcements(team1, team2, game_date)
            if link_from_announcements:
                print(f"🔗 Найдена ссылка в анонсах: {link_from_announcements}")
                return link_from_announcements
            
            # 3. Если не найдено в анонсах, ищем в табло
            print(f"🔍 Ссылка не найдена в анонсах, ищем в табло...")
            result = await self.game_manager.find_game_link(team1, team2)
            if result:
                game_link, found_team = result
                print(f"🔗 Найдена ссылка в табло: {game_link}")
                return game_link
            
            print(f"❌ Ссылка на игру не найдена ни в одном источнике")
            return None
        except Exception as e:
            print(f"❌ Ошибка поиска ссылки на игру: {e}")
            return None
    
    async def parse_game_from_link(self, game_link: str) -> Optional[Dict]:
        """Парсит игру по ссылке используя улучшенный парсер"""
        try:
            from enhanced_game_parser import EnhancedGameParser
            
            async with EnhancedGameParser(
                team_configs=self.game_manager.team_configs,
                team_keywords=self.game_manager.team_name_keywords,
            ) as parser:
                game_info = await parser.parse_game_from_url(game_link)
                if game_info and game_info.get('result'):
                    # Определяем статус игры
                    status = 'Завершена' if game_info.get('result') in ['победа', 'поражение', 'ничья'] else 'В процессе'

                    extracted_game_id = parser.extract_game_id_from_url(game_link)
                    teams = game_info.get('teams') or []
                    team1_entry = teams[0] if len(teams) > 0 else {}
                    team2_entry = teams[1] if len(teams) > 1 else {}

                    team1_id = team1_entry.get('id')
                    team2_id = team2_entry.get('id')
                    team1_name = team1_entry.get('name')
                    team2_name = team2_entry.get('name')

                    return {
                        'team1': game_info.get('our_team', '') or team1_name or '',
                        'team2': game_info.get('opponent', '') or team2_name or '',
                        'team1_id': team1_id,
                        'team2_id': team2_id,
                        'team1_name': team1_name or game_info.get('our_team', ''),
                        'team2_name': team2_name or game_info.get('opponent', ''),
                        'our_team': game_info.get('our_team', ''),
                        'opponent': game_info.get('opponent', ''),
                        'our_team_id': game_info.get('our_team_id'),
                        'opponent_team_id': game_info.get('opponent_team_id'),
                        'our_team_name': game_info.get('our_team_name') or game_info.get('our_team', ''),
                        'opponent_team_name': game_info.get('opponent_team_name') or game_info.get('opponent', ''),
                        'our_score': game_info.get('our_score', 0),
                        'opponent_score': game_info.get('opponent_score', 0),
                        'result': game_info.get('result', ''),
                        'status': status,
                        'date': game_info.get('date', ''),
                        'time': game_info.get('time', ''),
                        'venue': game_info.get('venue', ''),
                        'quarters': game_info.get('quarters', []),
                        'team_type': game_info.get('team_type') or 'Команда',
                        'game_link': game_link,  # Сохраняем исходную ссылку на игру
                        'game_id': extracted_game_id or game_info.get('game_id'),
                        'comp_id': game_info.get('comp_id') or game_info.get('competition_id'),
                        'league': game_info.get('league'),
                        'our_team_leaders': game_info.get('our_team_leaders', {})  # Добавляем лидеров команды
                    }
                return None
        except Exception as e:
            print(f"❌ Ошибка парсинга игры по ссылке: {e}")
            return None

    async def _compute_leaders_via_parser(self, game_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Пробует вычислить лидеров команды, если они отсутствуют"""
        game_link = game_info.get('game_link')
        if not game_link:
            return None

        try:
            from enhanced_game_parser import EnhancedGameParser

            async with EnhancedGameParser(
                team_configs=self.game_manager.team_configs,
                team_keywords=self.game_manager.team_name_keywords,
            ) as parser:
                game_id = parser.extract_game_id_from_url(game_link)
                api_url = parser.extract_api_url_from_url(game_link)
                if not game_id:
                    return None

                api_data = await parser.get_game_data_from_api(game_id, api_url)
                if not api_data:
                    return None

                player_stats = parser.extract_player_statistics(api_data)
                if not player_stats:
                    return None

                candidate_names: Set[str] = set()

                for key in ['our_team', 'our_team_name']:
                    value = game_info.get(key)
                    if isinstance(value, str) and value.strip():
                        candidate_names.add(value.strip())

                if not candidate_names:
                    for key in ['team1', 'team1_name', 'team2', 'team2_name']:
                        value = game_info.get(key)
                        if isinstance(value, str) and value.strip():
                            candidate_names.add(value.strip())

                configured_ids = set(self.game_manager.config_team_ids or [])
                online_teams = api_data.get('online', {}).get('OnlineTeams') or []
                for team in online_teams:
                    team_id = team.get('TeamID')
                    if team_id in configured_ids:
                        for key in ('TeamName2', 'TeamName1', 'ShortName2', 'ShortName1'):
                            value = team.get(key)
                            if isinstance(value, str) and value.strip():
                                candidate_names.add(value.strip())

                game_teams = api_data.get('online', {}).get('GameTeams') or api_data.get('game', {}).get('GameTeams') or []
                for idx, team in enumerate(game_teams):
                    team_id = team.get('TeamID') or team.get('team_id')
                    if team_id in configured_ids:
                        value = team.get('TeamName', {}).get('CompTeamNameRu') if isinstance(team.get('TeamName'), dict) else None
                        if isinstance(value, str) and value.strip():
                            candidate_names.add(value.strip())

                for team_id in self.game_manager.config_team_ids:
                    resolved = self.game_manager._resolve_team_name(team_id)
                    if isinstance(resolved, str) and resolved.strip():
                        candidate_names.add(resolved.strip())

                candidate_names.update(self.game_manager.team_name_keywords or [])

                if not candidate_names:
                    return None

                leaders = parser.find_our_team_leaders(player_stats.get('players', []), list(candidate_names))
                return leaders or None

        except Exception as e:
            print(f"⚠️ Не удалось вычислить лидеров через парсер: {e}")
            return None

    def find_link_in_announcements(self, team1: str, team2: str, game_date: str = None) -> Optional[str]:
        """Ищет ссылку на игру в сохраненных анонсах"""
        try:
            import json
            import os
            
            announcements_file = "game_announcements.json"
            if not os.path.exists(announcements_file):
                print(f"📄 Файл анонсов не найден: {announcements_file}")
                return None
            
            with open(announcements_file, 'r', encoding='utf-8') as f:
                announcements = json.load(f)
            
            print(f"📋 Загружено {len(announcements)} анонсов для поиска ссылки")
            
            # Ищем по разным вариантам ключей
            search_keys = []
            
            # Если есть дата, используем её
            if game_date:
                # Нормализуем время (заменяем точку на двоеточие)
                time_variants = ["12:00", "12.00", "14:00", "14.00", "16:00", "16.00", "18:00", "18.00", "20:00", "20.00"]
                for time_var in time_variants:
                    search_keys.append(f"{game_date}_{time_var}_{team1}_{team2}")
                    search_keys.append(f"{game_date}_{time_var}_{team2}_{team1}")
            else:
                # Ищем по всем возможным комбинациям
                for key in announcements.keys():
                    if team1 in key and team2 in key:
                        search_keys.append(key)
            
            print(f"🔍 Ищем по ключам: {search_keys[:3]}...")  # Показываем первые 3
            
            for key in search_keys:
                if key in announcements:
                    announcement = announcements[key]
                    game_link = announcement.get('game_link')
                    if game_link:
                        # Формируем полную ссылку
                        if game_link.startswith('http'):
                            full_link = game_link
                        else:
                            full_link = f"http://letobasket.ru/{game_link}"
                        print(f"✅ Найдена ссылка в анонсе {key}: {full_link}")
                        return full_link
            
            print(f"❌ Ссылка не найдена в анонсах")
            return None
            
        except Exception as e:
            print(f"❌ Ошибка поиска в анонсах: {e}")
            return None
    
    async def send_game_result(self, game_info: Dict) -> bool:
        """Отправляет результат игры в Telegram"""
        if not self.bot:
            print("❌ Бот не инициализирован")
            return False
        
        # Получаем список чатов для отправки результатов игр
        from game_system_manager import get_chat_ids_for_automation, AUTOMATION_KEY_GAME_RESULTS
        from enhanced_duplicate_protection import duplicate_protection
        
        automation_topics = duplicate_protection.get_config_ids().get("automation_topics") or {}
        game_results_entry = automation_topics.get(AUTOMATION_KEY_GAME_RESULTS, {})
        chat_ids = get_chat_ids_for_automation(AUTOMATION_KEY_GAME_RESULTS, game_results_entry)
        
        if not chat_ids:
            print("❌ Не настроены ID чатов для результатов игр (ни в таблице, ни в Secrets)")
            return False
        
        try:
            # Создаем ключ для проверки дублирования
            result_key = self.create_result_key(game_info)
            
            # Проверяем дублирование в Google Sheets
            print(f"🔍 Проверяем дублирование в Google Sheets для игры: {game_info['team1']} vs {game_info['team2']}")
            duplicate_check = duplicate_protection.check_duplicate("РЕЗУЛЬТАТ_ИГРА", result_key)
            
            if duplicate_check.get('exists'):
                print(f"⏭️ Результат для игры {game_info['team1']} vs {game_info['team2']} уже отправлен (найдено в Google Sheets)")
                print(f"   📅 Время отправки: {duplicate_check.get('data', ['', '', '', '', ''])[1]}")
                return False
            
            # Используем новую функцию форматирования с лидерами команды
            our_team_leaders = game_info.get('our_team_leaders', {})
            game_link = game_info.get('game_link')
            
            if not game_link:
                print(f"🔍 Ссылка не найдена в game_info, ищем заново...")
                game_link = await self.find_game_link(game_info['team1'], game_info['team2'], game_info.get('date'))

            if not our_team_leaders:
                computed_leaders = await self._compute_leaders_via_parser(game_info)
                if computed_leaders:
                    our_team_leaders = computed_leaders
                    game_info['our_team_leaders'] = computed_leaders

            # Формируем сообщение используя новую функцию
            message = self.game_manager.format_game_result_message(
                game_info=game_info,
                game_link=game_link,
                our_team_leaders=our_team_leaders
            )
            
            if game_link:
                print(f"🔗 Используется ссылка: {game_link}")
            else:
                print(f"❌ Ссылка на игру не найдена")
            
            # Сначала добавляем запись в Google Sheets для защиты от дублирования
            additional_info = f"{game_info['date']} {game_info['our_team']} vs {game_info['opponent']} ({game_info['our_score']}:{game_info['opponent_score']}) - {game_info['result']}"
            comp_id_value = self.game_manager._to_int(game_info.get('comp_id')) if hasattr(self.game_manager, '_to_int') else None
            if comp_id_value is None:
                comp_id_value = self.game_manager._to_int(game_info.get('competition_id')) if hasattr(self.game_manager, '_to_int') else None
            our_team_id = self.game_manager._to_int(game_info.get('our_team_id')) if hasattr(self.game_manager, '_to_int') else None
            opponent_team_id = self.game_manager._to_int(game_info.get('opponent_team_id')) if hasattr(self.game_manager, '_to_int') else None
            team_a_id = self.game_manager._to_int(game_info.get('team1_id')) if hasattr(self.game_manager, '_to_int') else None
            team_b_id = self.game_manager._to_int(game_info.get('team2_id')) if hasattr(self.game_manager, '_to_int') else None
            game_id_value = self.game_manager._to_int(game_info.get('game_id')) if hasattr(self.game_manager, '_to_int') else None

            our_team_label = self.game_manager._get_team_display_name(our_team_id, game_info.get('our_team')) if hasattr(self.game_manager, '_get_team_display_name') else game_info.get('our_team')
            opponent_label = self.game_manager._get_team_display_name(opponent_team_id, game_info.get('opponent')) if hasattr(self.game_manager, '_get_team_display_name') else game_info.get('opponent')

            protection_result = duplicate_protection.add_record(
                "РЕЗУЛЬТАТ_ИГРА",
                result_key,
                "ОТПРАВЛЯЕТСЯ",  # Временный статус
                additional_info,
                game_link or '',
                comp_id=comp_id_value,
                team_id=our_team_id,
                alt_name=our_team_label or '',
                settings='',
                game_id=game_id_value,
                game_date=game_info.get('date', ''),
                game_time=game_info.get('time', ''),
                arena=game_info.get('venue', ''),
                team_a_id=team_a_id,
                team_b_id=team_b_id
            )
            
            if not protection_result.get('success'):
                print(f"❌ Ошибка добавления записи в Google Sheets: {protection_result.get('error')}")
                # Продолжаем отправку, но логируем ошибку
            
            # Получаем topic_id для результатов игр
            game_results_topic_id = None
            if isinstance(game_results_entry, dict):
                topic_candidate = game_results_entry.get("topic_id")
                if topic_candidate is None:
                    topic_candidate = game_results_entry.get("topic_raw")
                try:
                    game_results_topic_id = int(topic_candidate) if topic_candidate is not None else None
                except (TypeError, ValueError):
                    game_results_topic_id = None
            
            # Отправляем сообщение во все настроенные чаты
            try:
                bot_instance = self.bot
                sent_messages = []
                
                for chat_id in chat_ids:
                    try:
                        target_chat_id = int(chat_id) if chat_id.isdigit() or (chat_id.startswith('-') and chat_id[1:].isdigit()) else chat_id
                        send_kwargs: Dict[str, Any] = {
                            "chat_id": target_chat_id,
                            "text": message,
                            "parse_mode": 'HTML'
                        }
                        if game_results_topic_id is not None:
                            send_kwargs["message_thread_id"] = game_results_topic_id
                        
                        sent_message = await bot_instance.send_message(**send_kwargs)
                        sent_messages.append(sent_message)
                        print(f"✅ Результат отправлен в чат {chat_id}")
                    except Exception as chat_error:
                        if "Message thread not found" in str(chat_error) and game_results_topic_id is not None:
                            print(f"⚠️ Топик {game_results_topic_id} не найден в чате {chat_id}, отправляем в основной чат")
                            send_kwargs.pop("message_thread_id", None)
                            sent_message = await bot_instance.send_message(**send_kwargs)
                            sent_messages.append(sent_message)
                            print(f"✅ Результат отправлен в чат {chat_id} (без топика)")
                        else:
                            print(f"❌ Ошибка отправки результата в чат {chat_id}: {chat_error}")
                            raise chat_error
                
                sent_message = sent_messages[0] if sent_messages else None
                
                # Обновляем статус в Google Sheets на "ОТПРАВЛЕНО"
                if protection_result.get('success') and protection_result.get('unique_key'):
                    duplicate_protection.update_record_status(protection_result['unique_key'], "ОТПРАВЛЕНО")
                    print(f"✅ Статус обновлен в Google Sheets: ОТПРАВЛЕНО")
                
            except Exception as send_error:
                print(f"❌ Ошибка отправки: {send_error}")
                # Обновляем статус на "ОШИБКА" если отправка не удалась
                if protection_result.get('success') and protection_result.get('unique_key'):
                    duplicate_protection.update_record_status(protection_result['unique_key'], "ОШИБКА")
                return False
            
            print(f"✅ Результат игры отправлен: {game_info['our_team']} vs {game_info['opponent']}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка отправки результата: {e}")
            return False
    
    async def run_game_results_monitor(self, force_run: bool = False):
        """Основная функция мониторинга результатов"""
        print("🏀 ЗАПУСК МОНИТОРИНГА РЕЗУЛЬТАТОВ ИГР")
        print("=" * 50)
        
        # Проверяем переменные окружения
        print("🔧 ПРОВЕРКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ:")
        print(f"BOT_TOKEN: {'✅' if BOT_TOKEN else '❌'}")
        print(f"CHAT_ID: {'✅' if CHAT_ID else '❌'}")
        print(f"ANNOUNCEMENTS_TOPIC_ID: {'✅' if ANNOUNCEMENTS_TOPIC_ID else '❌'}")
        print(f"ТЕСТОВЫЙ РЕЖИМ: {'✅ ВКЛЮЧЕН' if TEST_MODE else '❌ ВЫКЛЮЧЕН'}")
        
        # Показываем статистику из Google Sheets
        print(f"\n📊 Статистика из Google Sheets:")
        try:
            from enhanced_duplicate_protection import duplicate_protection
            stats = duplicate_protection.get_statistics()
            if 'РЕЗУЛЬТАТ_ИГРА' in stats:
                result_stats = stats['РЕЗУЛЬТАТ_ИГРА']
                print(f"   📈 Всего результатов: {result_stats.get('total', 0)}")
                print(f"   ✅ Отправлено: {result_stats.get('completed', 0)}")
                print(f"   🔄 В процессе: {result_stats.get('active', 0)}")
            else:
                print("   📈 Результатов игр в Google Sheets не найдено")
        except Exception as e:
            print(f"   ❌ Ошибка получения статистики: {e}")
        
        if not BOT_TOKEN or not CHAT_ID:
            print("❌ Не все переменные окружения настроены")
            return
        
        # Всегда проверяем результаты - расписание контролируется GitHub Actions
        print("🚀 Запуск проверки результатов игр...")
        self.should_check_results()  # Выводим информацию о времени запуска
        
        # Проверяем наличие ссылок на игры для сегодня
        print("\n🔍 Проверка наличия ссылок на игры для сегодня...")
        from enhanced_duplicate_protection import duplicate_protection
        
        # Ищем ссылки на игры в сервисном листе
        today_games_found = False
        try:
            from datetime_utils import get_moscow_time
            today = get_moscow_time().strftime('%d.%m.%Y')
            
            # Получаем все данные из сервисного листа
            worksheet = duplicate_protection._get_service_worksheet()
            if worksheet:
                all_data = worksheet.get_all_values()
                
                # Ищем записи типа АНОНС_ИГРА за сегодня
                for row in all_data:
                    if (len(row) >= 6 and 
                        row[0] == "АНОНС_ИГРА" and 
                        today in row[1] and  # Дата в колонке B
                        row[5]):  # Ссылка в колонке F
                        today_games_found = True
                        print(f"✅ Найдена игра на сегодня: {row[2]} (ссылка: {row[5]})")
                        break
                
                if not today_games_found:
                    print(f"❌ Игры на сегодня ({today}) не найдены в сервисном листе")
                    print("💡 Убедитесь, что анонсы игр были созданы и содержат ссылки")
                    return
            else:
                print("❌ Сервисный лист недоступен")
                return
                
        except Exception as e:
            print(f"❌ Ошибка проверки ссылок на игры: {e}")
            return
        
        # Получаем результаты игр используя ссылки из сервисного листа
        print("\n🔄 Получение результатов игр...")
        games = await self.fetch_game_results_from_links()
        
        if not games:
            print("⚠️ Завершенных игр не найдено")
            return
        
        print(f"\n📊 Найдено {len(games)} завершенных игр:")
        for i, game in enumerate(games, 1):
            print(f"   {i}. {game['our_team']} vs {game['opponent']} ({game['our_score']}:{game['opponent_score']}) - {game['result']}")
        
        # Отправляем результаты
        print(f"\n📤 Отправка результатов...")
        sent_count = 0
        
        for i, game in enumerate(games, 1):
            print(f"\n🎮 Отправка результата {i}/{len(games)}...")
            success = await self.send_game_result(game)
            
            if success:
                sent_count += 1
            
            # Небольшая пауза между отправками
            await asyncio.sleep(2)
        
        print(f"\n📊 ИТОГИ:")
        print(f"✅ Отправлено результатов: {sent_count}")
        print(f"📋 Всего игр: {len(games)}")
        
        if sent_count > 0:
            print("\n✅ Мониторинг результатов завершен успешно!")
        else:
            print("\n⚠️ Результаты не отправлены (возможно, уже были отправлены ранее)")

async def main():
    """Основная функция"""
    monitor = GameResultsMonitorFinal()
    await monitor.run_game_results_monitor()

if __name__ == "__main__":
    asyncio.run(main())
