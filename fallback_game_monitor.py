#!/usr/bin/env python3
"""
Модуль для мониторинга игр через fallback-источники из Google таблицы
Читает конфигурацию, парсит страницы и создает опросы при необходимости
"""

import asyncio
import aiohttp
import re
from typing import Any, Dict, List, Optional, Set
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime_utils import get_moscow_time
from enhanced_duplicate_protection import duplicate_protection
from infobasket_smart_parser import InfobasketSmartParser
from game_system_manager import GameSystemManager, create_game_key

# Попытка импортировать Playwright для парсинга JavaScript-контента
try:
    from playwright.async_api import async_playwright, Browser, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    print("ℹ️ Playwright не установлен. Для парсинга JavaScript-контента установите: pip install playwright && playwright install chromium")


class FallbackGameMonitor:
    """Мониторинг игр через fallback-источники"""
    
    def __init__(self):
        self.game_manager = GameSystemManager()
        self.config_worksheet = None
        self.all_configured_teams = []  # Все команды из конфигурации
        self._init_google_sheets()
        self._load_configured_teams()
    
    def _init_google_sheets(self):
        """Инициализация Google Sheets"""
        try:
            if duplicate_protection.config_worksheet:
                self.config_worksheet = duplicate_protection.config_worksheet
                print("✅ Лист 'Конфиг' подключен для fallback мониторинга")
            else:
                print("⚠️ Лист 'Конфиг' не найден")
        except Exception as e:
            print(f"❌ Ошибка инициализации Google Sheets: {e}")
    
    async def _load_configured_teams_async(self):
        """Загружает все команды из конфигурации (асинхронно, с запросом к API если нужно)"""
        try:
            team_ids = self.game_manager.config_team_ids or []
            team_names = set()
            
            # Читаем из таблицы напрямую
            if self.config_worksheet:
                try:
                    all_data = self.config_worksheet.get_all_values()
                    for row in all_data[1:]:
                        if not row or len(row) < 3:
                            continue
                        row_type = (row[0] or "").strip().upper()
                        team_id_cell = row[2] if len(row) > 2 else ""
                        alt_name = (row[3] or "").strip() if len(row) > 3 else ""
                        
                        # Если это CONFIG_TEAM и есть альтернативное имя
                        if row_type in {"CONFIG_TEAM", "TEAM_CONFIG"} and alt_name:
                            team_names.add(alt_name)
                        
                        # Если есть team_id, пробуем получить название через API
                        parsed_ids = duplicate_protection._parse_ids(team_id_cell)
                        for tid in parsed_ids:
                            if tid in team_ids:
                                # Пробуем получить название через game_manager
                                team_name = self.game_manager._resolve_team_name(tid)
                                if team_name:
                                    team_names.add(team_name.strip())
                except Exception as e:
                    print(f"⚠️ Ошибка чтения команд из таблицы: {e}")
            
            # Если названий нет, пробуем получить через API
            if not team_names and team_ids:
                print(f"   🔍 Названия команд не найдены в таблице, пробуем получить через API...")
                try:
                    import aiohttp
                    async with aiohttp.ClientSession() as session:
                        for team_id in team_ids[:10]:  # Увеличиваем лимит
                            try:
                                # Пробуем получить информацию о команде через API
                                url = f"https://reg.infobasket.su/Comp/GetTeamInfo?teamId={team_id}"
                                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as response:
                                    if response.status == 200:
                                        data = await response.json()
                                        # Ищем название команды в ответе
                                        team_name = (data.get('TeamNameRu') or 
                                                    data.get('ShortTeamNameRu') or 
                                                    data.get('TeamName') or 
                                                    data.get('ShortTeamName') or
                                                    data.get('Name'))
                                        if team_name:
                                            team_names.add(team_name.strip())
                                            print(f"      ✅ Получено название для ID {team_id}: {team_name}")
                            except Exception as api_error:
                                # Пробуем альтернативный способ - через календарь соревнований
                                try:
                                    # Получаем игры соревнования и ищем команду там
                                    comp_ids = self.game_manager.config_comp_ids or []
                                    for comp_id in comp_ids[:3]:  # Проверяем первые 3 соревнования
                                        calendar_url = f"https://reg.infobasket.su/Comp/GetCalendar/?comps={comp_id}&format=json"
                                        async with session.get(calendar_url, timeout=aiohttp.ClientTimeout(total=5)) as cal_response:
                                            if cal_response.status == 200:
                                                games_data = await cal_response.json()
                                                if isinstance(games_data, list):
                                                    for game in games_data[:20]:  # Проверяем первые 20 игр
                                                        if game.get('Team1ID') == team_id:
                                                            team_name = game.get('ShortTeamNameAru') or game.get('TeamNameAru')
                                                            if team_name:
                                                                team_names.add(team_name.strip())
                                                                break
                                                        if game.get('Team2ID') == team_id:
                                                            team_name = game.get('ShortTeamNameBru') or game.get('TeamNameBru')
                                                            if team_name:
                                                                team_names.add(team_name.strip())
                                                                break
                                                if team_names:
                                                    break
                                except:
                                    continue
                except Exception as e:
                    print(f"   ⚠️ Ошибка получения названий через API: {e}")
            
            self.all_configured_teams = sorted(list(team_names))
            print(f"📋 Загружено {len(self.all_configured_teams)} команд из конфигурации: {', '.join(self.all_configured_teams[:5])}{'...' if len(self.all_configured_teams) > 5 else ''}")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки команд из конфигурации: {e}")
            self.all_configured_teams = []
    
    def _load_configured_teams(self):
        """Загружает все команды из конфигурации (синхронная версия)"""
        # Для синхронной загрузки просто читаем из таблицы
        try:
            team_names = set()
            
            # Читаем из таблицы напрямую
            if self.config_worksheet:
                try:
                    all_data = self.config_worksheet.get_all_values()
                    for row in all_data[1:]:
                        if not row or len(row) < 3:
                            continue
                        row_type = (row[0] or "").strip().upper()
                        alt_name = (row[3] or "").strip() if len(row) > 3 else ""
                        
                        # Если это CONFIG_TEAM и есть альтернативное имя
                        if row_type in {"CONFIG_TEAM", "TEAM_CONFIG"} and alt_name:
                            team_names.add(alt_name)
                except:
                    pass
            
            self.all_configured_teams = sorted(list(team_names))
            if self.all_configured_teams:
                print(f"📋 Загружено {len(self.all_configured_teams)} команд из конфигурации: {', '.join(self.all_configured_teams[:5])}{'...' if len(self.all_configured_teams) > 5 else ''}")
        except Exception as e:
            print(f"⚠️ Ошибка загрузки команд из конфигурации: {e}")
            self.all_configured_teams = []
    
    def _normalize_name_for_search(self, text: str) -> str:
        """Нормализует текст для поиска (использует метод из GameSystemManager)"""
        return self.game_manager._normalize_name_for_search(text)
    
    def _find_matching_variant(self, text: str, variants: List[str]) -> Optional[str]:
        """Находит совпадение варианта названия в тексте (использует метод из GameSystemManager)"""
        normalized_text = self._normalize_name_for_search(text)
        return self.game_manager._find_matching_variant(normalized_text, variants)
    
    def _build_name_variants(self, name: str) -> Set[str]:
        """Строит варианты названия команды для поиска (использует метод из GameSystemManager)"""
        return self.game_manager._build_name_variants(name)
    
    def get_fallback_configs(self) -> List[Dict[str, Any]]:
        """Получает конфигурации fallback-источников из Google таблицы.
        
        Тип определяется автоматически по наличию полей:
        - Если есть URL → это fallback конфигурация (независимо от поля ТИП)
        - Если есть ИД команды → это CONFIG_TEAM (для других целей)
        
        Команда для поиска на сайте берется только из поля "КОМАНДА ДЛЯ FALLBACK".
        Альтернативное имя из CONFIG_TEAM НЕ используется для поиска.
        """
        if not self.config_worksheet:
            print("⚠️ Лист 'Конфиг' не доступен")
            return []
        
        try:
            all_data = self.config_worksheet.get_all_values()
            if not all_data or len(all_data) <= 1:
                return []
            
            fallback_configs = []
            found_end_marker = False
            
            for row in all_data[1:]:
                if not row or len(row) < 1:
                    continue
                
                # Расширяем строку до нужной длины (с колонкой ТИП - 8 колонок)
                row_extended = list(row)
                required_len = 8  # С колонкой ТИП: ТИП, ИД соревнования, ИД команды, АЛЬТЕРНАТИВНОЕ ИМЯ, НАСТРОЙКИ, ДНИ НЕДЕЛИ, URL FALLBACK, КОМАНДА ДЛЯ FALLBACK
                if len(row_extended) < required_len:
                    row_extended.extend([""] * (required_len - len(row_extended)))
                
                # Проверяем маркеры конца секций в первой колонке (ТИП)
                first_cell = (row_extended[0] or "").strip().upper()
                if first_cell in {"END", "END_CONFIG", "CONFIG_END", "END OF CONFIG", "КОНЕЦ", 
                                  "--- END ---", "=== END ==="}:
                    found_end_marker = True
                    continue
                
                # После маркера конца конфигурации не обрабатываем fallback
                if found_end_marker:
                    continue
                
                # Пропускаем заголовки секций
                if first_cell in {"ID ГОЛОСОВАНИЯ", "--- END VOTING ---", "ТИП"}:
                    continue
                
                # Определяем fallback конфигурацию по наличию URL
                # Структура С колонкой ТИП:
                # 0: ТИП
                # 1: ИД (СОРЕВНОВАНИЯ / ГОЛОСОВАНИЯ)
                # 2: ИД КОМАНДЫ / ПОРЯДОК
                # 3: АЛЬТЕРНАТИВНОЕ ИМЯ / ТЕКСТ
                # 4: НАСТРОЙКИ (JSON)
                # 5: ДНИ НЕДЕЛИ
                # 6: URL FALLBACK
                # 7: КОМАНДА ДЛЯ FALLBACK
                fallback_url = row_extended[6] if len(row_extended) > 6 else ""
                
                # Если URL валидный (начинается с http), это fallback конфигурация
                if not fallback_url.strip() or not fallback_url.strip().startswith(('http://', 'https://')):
                    continue  # Нет URL - пропускаем
                
                # Получаем данные из строки
                comp_id_cell = row_extended[1] if len(row_extended) > 1 else ""  # Колонка 1
                team_id_cell = row_extended[2] if len(row_extended) > 2 else ""  # Колонка 2
                fallback_name = row_extended[7] if len(row_extended) > 7 else ""  # Колонка 7
                
                # Парсим ID
                comp_ids = duplicate_protection._parse_ids(comp_id_cell)
                team_ids = duplicate_protection._parse_ids(team_id_cell)
                
                config = {
                    "comp_ids": comp_ids,
                    "team_ids": team_ids,
                    "url": fallback_url.strip(),
                    "name": fallback_name.strip(),  # "КОМАНДА ДЛЯ FALLBACK"
                }
                
                fallback_configs.append(config)
                print(f"📋 Найдена fallback конфигурация: URL={config['url']}, "
                      f"CompIDs={comp_ids}, TeamIDs={team_ids}")
                if config["name"]:
                    print(f"   Команда для поиска на сайте: '{config['name']}'")
                else:
                    print(f"   Команда для fallback не указана")
            
            return fallback_configs
            
        except Exception as e:
            print(f"❌ Ошибка чтения fallback конфигураций: {e}")
            return []
    
    async def parse_fallback_page(self, url: str, team_name: str) -> List[Dict[str, Any]]:
        """Парсит страницу fallback-источника на поиск расписания игр для указанной команды.
        
        Команда передается как параметр (обычно берется из CONFIG_TEAM в конфигурации).
        """
        if not url:
            return []
        
        if not team_name:
            print(f"⚠️ Не указано название команды для fallback парсинга")
            return []
        
        try:
            # Используем ТОЛЬКО команду из fallback конфигурации
            team_variants = list(self._build_name_variants(team_name))
            
            print(f"🔍 Парсинг расписания на странице {url}")
            print(f"   Ищем игры ТОЛЬКО для команды: {team_name}")
            
            # Для сайта globalleague.ru пробуем также страницу с таблицей и календарем
            additional_urls = []
            if 'globalleague.ru' in url:
                # Пробуем страницу турнирной таблицы
                table_url = url.rstrip('/') + '/table/'
                additional_urls.append(table_url)
                # Пробуем страницу календаря, если есть
                calendar_url = url.rstrip('/') + '/calendar/'
                additional_urls.append(calendar_url)
                # Также пробуем страницу со статистикой/играми
                games_url = url.rstrip('/') + '/games/'
                additional_urls.append(games_url)
            
            async with aiohttp.ClientSession() as session:
                # Парсим основную страницу
                games = await self._parse_single_page(session, url, team_variants, team_name)
                
                # Парсим дополнительные страницы (если есть)
                for additional_url in additional_urls:
                    print(f"   🔍 Проверяем дополнительную страницу: {additional_url}")
                    additional_games = await self._parse_single_page(session, additional_url, team_variants, team_name)
                    games.extend(additional_games)
                
                # Удаляем дубликаты (по дате и командам)
                unique_games = self._remove_duplicate_games(games)
                
                if unique_games:
                    print(f"   ✅ Найдено {len(unique_games)} игр в расписании:")
                    for i, game in enumerate(unique_games, 1):
                        print(f"      {i}. {game.get('date')} {game.get('time')} - {game.get('team_name')} vs {game.get('opponent')} ({game.get('venue')})")
                else:
                    print(f"   ⚠️ Игры в расписании не найдены")
                    # Для сайтов с JavaScript-контентом выводим предупреждение
                    if 'globalleague.ru' in url:
                        print(f"   💡 Примечание: Сайт {url} может загружать данные через JavaScript.")
                        print(f"      Расписание может быть недоступно для автоматического парсинга.")
                
                print(f"✅ Всего найдено {len(unique_games)} игр в расписании на странице {url}")
                return unique_games
                    
        except Exception as e:
            print(f"❌ Ошибка парсинга страницы {url}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def _parse_single_page(self, session: aiohttp.ClientSession, url: str, team_variants: List[str], team_name: str) -> List[Dict[str, Any]]:
        """Парсит одну страницу на поиск игр"""
        games = []
        try:
            # Для сайтов с JavaScript-контентом используем Playwright
            use_playwright = PLAYWRIGHT_AVAILABLE and self._needs_playwright(url)
            
            if use_playwright:
                content = await self._fetch_with_playwright(url)
            else:
                async with session.get(url) as response:
                    if response.status != 200:
                        return []
                    content = await response.text()
            
            if not content:
                return []
            
            soup = BeautifulSoup(content, 'html.parser')
            
            # Для сайтов с JavaScript-контентом проверяем, есть ли команда в тексте страницы
            if 'globalleague.ru' in url or 'neva-basket.ru' in url:
                page_text = soup.get_text()
                normalized_page = self._normalize_name_for_search(page_text)
                team_found_in_page = self._find_matching_variant(normalized_page, team_variants)
                if team_found_in_page:
                    print(f"   ✅ Команда '{team_name}' найдена в тексте страницы")
                else:
                    print(f"   ⚠️ Команда '{team_name}' НЕ найдена в тексте страницы")
                    print(f"      Варианты поиска: {list(team_variants)[:5]}")
                    # Показываем первые 500 символов текста для отладки
                    preview_text = page_text[:500].replace('\n', ' ').strip()
                    print(f"      Превью текста страницы: {preview_text}...")
            
            # Стратегия 1: Ищем таблицы с расписанием
            tables = soup.find_all('table')
            if url.endswith('/table/') or 'table' in url:
                print(f"   Найдено {len(tables)} таблиц на странице {url}")
                if len(tables) == 0 and ('globalleague.ru' in url or 'neva-basket.ru' in url):
                    print(f"   ⚠️ Таблицы не найдены - возможно, данные загружаются через JavaScript")
                    print(f"   💡 Для парсинга JavaScript-контента установите Playwright: pip install playwright && playwright install chromium")
            else:
                print(f"   Найдено {len(tables)} таблиц на странице")
            
            for table in tables:
                # Парсим таблицу только для указанной команды
                table_games = self._parse_schedule_table(table, team_variants, team_name, url)
                games.extend(table_games)
            
            # Стратегия 2: Ищем все блоки с датами, затем проверяем наличие команды в том же блоке или соседних
            date_pattern = r'\d{1,2}\.\d{1,2}\.\d{2,4}'
            # Ищем все элементы с датами
            elements_with_dates = soup.find_all(string=re.compile(date_pattern))
            print(f"   Найдено {len(elements_with_dates)} элементов с датами")
            
            # Для сайтов с JavaScript-контентом, если таблиц нет, но команда найдена в тексте,
            # пробуем найти команду в любом тексте страницы вместе с датами
            if len(tables) == 0 and len(elements_with_dates) == 0 and ('globalleague.ru' in url or 'neva-basket.ru' in url):
                # Ищем команду в тексте страницы и пытаемся найти рядом даты
                page_text = soup.get_text()
                # Ищем все даты в тексте (более гибкий паттерн)
                all_dates = re.findall(r'\d{1,2}\.\d{1,2}\.?\s*\d{2,4}', page_text)
                if all_dates:
                    print(f"   💡 Найдено {len(all_dates)} дат в тексте страницы (возможно, в JavaScript-контенте)")
                    print(f"      Первые даты: {all_dates[:5]}")
                    print(f"   ⚠️ Для парсинга JavaScript-контента требуется Playwright")
                    print(f"      Установите: pip install playwright && playwright install chromium")
            
            checked_blocks = set()
            for date_text in elements_with_dates[:150]:
                # Поднимаемся по дереву, чтобы найти родительский блок (tr, div, li и т.д.)
                parent = date_text.parent
                # Ищем родительский элемент, который может содержать расписание (таблица, список, div)
                while parent and parent.name not in ['tr', 'td', 'div', 'li', 'p', 'span', 'table', 'tbody']:
                    parent = parent.parent
                
                if not parent or id(parent) in checked_blocks:
                    continue
                
                checked_blocks.add(id(parent))
                parent_text = parent.get_text(separator=' ', strip=True)
                
                # Проверяем только указанную команду из fallback конфигурации
                normalized_parent = self._normalize_name_for_search(parent_text)
                team_match = self._find_matching_variant(normalized_parent, team_variants)
                
                if team_match:
                    # Нашли блок с датой и командой
                    block_games = self._parse_schedule_block(parent, team_variants, team_name, url)
                    if block_games:
                        games.extend(block_games)
                else:
                    # Проверяем соседние элементы (предыдущий и следующий)
                    if hasattr(parent, 'previous_sibling') and parent.previous_sibling:
                        prev_text = parent.previous_sibling.get_text(separator=' ', strip=True) if hasattr(parent.previous_sibling, 'get_text') else str(parent.previous_sibling)
                        if self._find_matching_variant(self._normalize_name_for_search(prev_text), team_variants):
                            # Объединяем тексты
                            combined_text = f"{prev_text} {parent_text}"
                            block_games = self._parse_schedule_block_from_text(combined_text, team_variants, team_name, url)
                            if block_games:
                                games.extend(block_games)
                    
                    if hasattr(parent, 'next_sibling') and parent.next_sibling:
                        next_text = parent.next_sibling.get_text(separator=' ', strip=True) if hasattr(parent.next_sibling, 'get_text') else str(parent.next_sibling)
                        if self._find_matching_variant(self._normalize_name_for_search(next_text), team_variants):
                            # Объединяем тексты
                            combined_text = f"{parent_text} {next_text}"
                            block_games = self._parse_schedule_block_from_text(combined_text, team_variants, team_name, url)
                            if block_games:
                                games.extend(block_games)
            
            # Стратегия 3: Ищем ссылки на игры (если есть)
            anchors = soup.find_all('a', href=True)
            for anchor in anchors:
                href = anchor.get('href')
                if not href:
                    continue
                
                # Проверяем, что это ссылка на игру
                is_game_link = 'gameId=' in href or 'game.html' in href or '/game/' in href or '/match/' in href or 'podrobno.php' in href
                if not is_game_link:
                    continue
                
                # Пытаемся извлечь информацию из текста ссылки или страницы
                # Проверяем только указанную команду из fallback конфигурации
                link_text = anchor.get_text(strip=True)
                normalized_link = self._normalize_name_for_search(link_text)
                
                # Также проверяем родительский элемент (может содержать название команды)
                parent_text = ""
                if anchor.parent:
                    parent_text = anchor.parent.get_text(separator=' ', strip=True)
                    normalized_parent = self._normalize_name_for_search(parent_text)
                
                team_match = self._find_matching_variant(normalized_link, team_variants) or \
                            self._find_matching_variant(normalized_parent, team_variants)
                
                if team_match:
                    # Проверяем, не найдена ли уже эта игра в таблице (чтобы избежать дубликатов)
                    # Если в тексте ссылки или родителя нет даты, скорее всего это уже обработано
                    has_date_in_link = bool(re.search(r'\d{1,2}\.\d{1,2}\.\d{2,4}', link_text + parent_text))
                    
                    # Если дата есть в тексте, пробуем извлечь
                    if has_date_in_link:
                        # Сначала пробуем извлечь из текста ссылки
                        game_info = self._extract_game_info_from_text(link_text, team_name)
                        
                        # Если не получилось, пробуем из родительского элемента
                        if not game_info and parent_text:
                            game_info = self._extract_game_info_from_text(parent_text, team_name)
                        
                        # Если все еще не получилось, загружаем страницу игры
                        if not game_info:
                            full_link = href if href.startswith('http') else urljoin(url, href)
                            page_game_info = await self._extract_game_info_from_page(session, full_link, team_name)
                            if page_game_info:
                                game_info = page_game_info
                        
                        if game_info:
                            # Проверяем, что дата в будущем (фильтруем прошедшие игры)
                            try:
                                from datetime import datetime
                                game_date = datetime.strptime(game_info['date'], '%d.%m.%Y').date()
                                today = get_moscow_time().date()
                                if game_date <= today:
                                    # Это прошедшая или сегодняшняя игра, пропускаем
                                    continue
                            except (ValueError, KeyError):
                                # Если не удалось распарсить дату, пропускаем
                                continue
                            
                            full_link = href if href.startswith('http') else urljoin(url, href)
                            game_info['url'] = full_link
                            game_info['team_name'] = team_name
                            # Извлекаем game_id из ссылки, если есть
                            game_id_match = re.search(r'gameId[=:](\d+)|/game/(\d+)|/match/(\d+)|id[=:](\d+)', href)
                            if game_id_match:
                                game_info['game_id'] = int(game_id_match.group(1) or game_id_match.group(2) or game_id_match.group(3) or game_id_match.group(4))
                            games.append(game_info)
                            print(f"      ✅ Найдена игра по ссылке: {game_info.get('date')} {game_info.get('time')} vs {game_info.get('opponent')}")
            
            # Стратегия 4: Для globalleague.ru - парсим календарь игр из таблиц или списков
            if 'globalleague.ru' in url and len(games) == 0:
                # Ищем блоки с играми в календаре
                # Обычно это div или tr элементы с датами и названиями команд
                calendar_blocks = soup.find_all(['div', 'tr', 'li'], class_=re.compile(r'game|match|calendar|schedule', re.I))
                if not calendar_blocks:
                    # Ищем любые блоки, содержащие даты и названия команд
                    page_text = soup.get_text()
                    # Ищем паттерны типа "DD.MM.YYYY Команда1 - Команда2"
                    calendar_pattern = r'(\d{1,2}\.\d{1,2}\.\d{2,4})\s+([^-]+)\s*[-–—]\s*([^-]+)'
                    matches = re.finditer(calendar_pattern, page_text)
                    for match in matches:
                        date_str, team1_text, team2_text = match.groups()
                        team1_normalized = self._normalize_name_for_search(team1_text.strip())
                        team2_normalized = self._normalize_name_for_search(team2_text.strip())
                        
                        # Проверяем, есть ли наша команда в матче
                        team_match = self._find_matching_variant(team1_normalized, team_variants) or \
                                    self._find_matching_variant(team2_normalized, team_variants)
                        
                        if team_match:
                            # Определяем соперника
                            if self._find_matching_variant(team1_normalized, team_variants):
                                opponent = team2_text.strip()
                            else:
                                opponent = team1_text.strip()
                            
                            # Проверяем, что дата в будущем
                            try:
                                from datetime import datetime
                                game_date = datetime.strptime(date_str, '%d.%m.%Y').date()
                                today = get_moscow_time().date()
                                if game_date > today:
                                    game_info = {
                                        'date': date_str,
                                        'time': '20:00',
                                        'opponent': opponent,
                                        'venue': 'Не указано',
                                        'team_name': team_name,
                                        'url': url
                                    }
                                    games.append(game_info)
                                    print(f"      ✅ Найдена игра в календаре: {date_str} {team_name} vs {opponent}")
                            except ValueError:
                                pass
            
            return games
        except Exception as e:
            print(f"   ⚠️ Ошибка парсинга страницы {url}: {e}")
            return []
    
    def _parse_schedule_table(self, table, team_variants: List[str], team_name: str, base_url: str) -> List[Dict[str, Any]]:
        """Парсит таблицу с расписанием игр для указанной команды"""
        games = []
        try:
            rows = table.find_all('tr')
            
            # Для турнирных таблиц (как на globalleague.ru) - ищем строку с нашей командой
            # и затем извлекаем даты из ячеек этой строки
            team_row = None
            team_row_idx = None
            
            # Ищем строку с командой (пропускаем заголовок)
            team_row = None
            for row_idx, row in enumerate(rows[1:], 1):  # Пропускаем первую строку (заголовок)
                cells = row.find_all(['td', 'th'])
                if len(cells) < 2:
                    continue
                
                # Для матрицы результатов вторая ячейка - название команды
                team_cell = cells[1] if len(cells) > 1 else None
                if not team_cell:
                    continue
                
                team_cell_text = team_cell.get_text(strip=True)
                normalized_text = self._normalize_name_for_search(team_cell_text)
                team_match = self._find_matching_variant(normalized_text, team_variants)
                
                if team_match:
                    team_row = row
                    print(f"      📌 Найдена строка с командой '{team_name}' (строка {row_idx}, ячейка команды: '{team_cell_text}')")
                    break
            
            if team_row:
                # Парсим ячейки строки команды для поиска дат будущих игр
                cells = team_row.find_all(['td', 'th'])
                headers = rows[0].find_all(['td', 'th']) if rows else []
                
                # Для матрицы результатов (как на globalleague.ru):
                # Первые 2 ячейки в строке - номер и название команды
                # В заголовках первые 2 столбца могут быть пустыми, затем идут названия команд
                # Нужно правильно сопоставить ячейки строки с заголовками
                
                print(f"      🔍 Анализ строки команды: найдено {len(cells)} ячеек, {len(headers)} заголовков")
                
                for cell_idx in range(2, len(cells)):  # Начинаем с 3-й ячейки (пропускаем номер и название)
                    cell = cells[cell_idx]
                    cell_text = cell.get_text(strip=True)
                    
                    # Получаем заголовок для этой ячейки
                    header_text = ""
                    if cell_idx < len(headers):
                        header_cell = headers[cell_idx]
                        header_text = header_cell.get_text(strip=True)
                    
                    if not cell_text:
                        continue
                    
                    # Отладочный вывод для первых нескольких ячеек
                    if cell_idx < 10:
                        print(f"      🔍 Ячейка {cell_idx}: '{cell_text}' (заголовок: '{header_text}')")
                    
                    # Ищем даты в формате DD.MM или DD.MM.YYYY
                    # Для globalleague.ru формат: "22.11 д" или "29.11 г" или "22.11" или просто "д" (домашняя игра)
                    # Также может быть формат с годом: "22.11.2025"
                    date_patterns = [
                        r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # DD.MM.YYYY
                        r'(\d{1,2})\.(\d{1,2})(?:\s+[дг])?',  # DD.MM д/г
                        r'(\d{1,2})\.(\d{1,2})',  # DD.MM
                    ]
                    
                    date_match = None
                    day, month, year = None, None, None
                    
                    for pattern in date_patterns:
                        date_match = re.search(pattern, cell_text)
                        if date_match:
                            groups = date_match.groups()
                            if len(groups) == 3:  # DD.MM.YYYY
                                day, month, year = groups
                            elif len(groups) == 2:  # DD.MM
                                day, month = groups
                                year = None
                            break
                    
                    if date_match and day and month:
                        # Определяем год (текущий или следующий)
                        from datetime import datetime, timedelta
                        today = get_moscow_time()
                        current_year = today.year
                        
                        # Если год не указан, определяем его
                        if not year:
                            year = str(current_year)
                            # Пробуем текущий год
                            try:
                                game_date = datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y').date()
                                # Если дата в прошлом более чем на 30 дней, пробуем следующий год
                                if game_date < today.date() - timedelta(days=30):
                                    year = str(current_year + 1)
                                    game_date = datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y').date()
                            except ValueError:
                                if cell_idx < 10:
                                    print(f"      ⚠️ Ошибка парсинга даты: {day}.{month}.{year}")
                                continue
                        else:
                            # Год указан в дате
                            try:
                                game_date = datetime.strptime(f"{day}.{month}.{year}", '%d.%m.%Y').date()
                            except ValueError:
                                if cell_idx < 10:
                                    print(f"      ⚠️ Ошибка парсинга даты с годом: {day}.{month}.{year}")
                                continue
                        
                        # Отладочный вывод для первых нескольких ячеек
                        if cell_idx < 10:
                            print(f"      📅 Найдена дата: {day}.{month}.{year} -> {game_date}, сегодня: {today.date()}, будущая: {game_date > today.date()}")
                        
                        # Проверяем, что это будущая игра
                        if game_date > today.date():
                            # Ищем название соперника в заголовке столбца
                            # В заголовках первые 2 столбца могут быть пустыми, поэтому используем cell_idx напрямую
                            opponent = "Соперник"
                            if cell_idx < len(headers):
                                header_cell = headers[cell_idx]
                                opponent_text = header_cell.get_text(strip=True)
                                # Пропускаем пустые заголовки и название нашей команды
                                if opponent_text and opponent_text.strip() and opponent_text != team_name:
                                    # Нормализуем для сравнения
                                    normalized_opponent = self._normalize_name_for_search(opponent_text)
                                    normalized_team = self._normalize_name_for_search(team_name)
                                    if normalized_opponent != normalized_team:
                                        opponent = opponent_text.strip()
                            
                            # Извлекаем время и место, если есть
                            time = "20:00"  # По умолчанию
                            venue = ""
                            
                            # Пробуем извлечь из текста ячейки
                            if ':' in cell_text:
                                time_match = re.search(r'(\d{1,2}):(\d{2})', cell_text)
                                if time_match:
                                    time = time_match.group(0)
                            
                            game_info = {
                                'date': game_date.strftime('%d.%m.%Y'),
                                'time': time,
                                'opponent': opponent,
                                'venue': venue,
                                'team_name': team_name,
                                'url': base_url
                            }
                            
                            print(f"         ✅ Извлечена будущая игра: {game_info.get('date')} {game_info.get('time')} vs {opponent}")
                            games.append(game_info)
            
            # Стандартный парсинг для обычных таблиц расписания (если не нашли в матрице результатов)
            if not games:  # Парсим стандартным способом только если не нашли игры в матрице
                for row_idx, row in enumerate(rows):
                    if row == team_row:  # Пропускаем уже обработанную строку
                        continue
                    
                    cells = row.find_all(['td', 'th'])
                    if len(cells) < 2:
                        continue
                    
                    # Пробуем парсить каждую ячейку отдельно (на случай, если игры в разных ячейках)
                    for cell_idx, cell in enumerate(cells):
                        cell_text = cell.get_text(strip=True)
                        if len(cell_text) < 10:
                            continue
                        
                        # Проверяем, есть ли в ячейке дата (признак расписания игры)
                        date_pattern = r'\d{1,2}\.\d{1,2}\.\d{2,4}'
                        if not re.search(date_pattern, cell_text):
                            continue
                        
                        # Проверяем, есть ли в ячейке наша команда
                        normalized_cell_text = self._normalize_name_for_search(cell_text)
                        team_match = self._find_matching_variant(normalized_cell_text, team_variants)
                        if not team_match:
                            continue
                        
                        # Проверяем, что команда действительно участвует в игре
                        # Формат должен быть: "Команда1 - Команда2" или "Команда1 против Команда2"
                        game_separators = [r'\s*[-–—]\s*', r'\s+против\s+', r'\s+vs\s+', r'\s+и\s+']
                        is_participant = False
                        
                        for sep_pattern in game_separators:
                            # Разбиваем текст ячейки по разделителю
                            parts = re.split(sep_pattern, cell_text, flags=re.IGNORECASE)
                            if len(parts) >= 2:
                                # Проверяем, есть ли наша команда в одной из частей
                                # Также проверяем комбинации соседних частей (для команд с дефисами)
                                for i, part in enumerate(parts):
                                    part_normalized = self._normalize_name_for_search(part)
                                    if self._find_matching_variant(part_normalized, team_variants):
                                        is_participant = True
                                        break
                                    
                                    # Проверяем комбинацию текущей части с предыдущей (для "Военмех-Vintage")
                                    if i > 0:
                                        combined = f"{parts[i-1]}-{part}"
                                        combined_normalized = self._normalize_name_for_search(combined)
                                        if self._find_matching_variant(combined_normalized, team_variants):
                                            is_participant = True
                                            break
                                    
                                    # Проверяем комбинацию текущей части со следующей
                                    if i < len(parts) - 1:
                                        combined = f"{part}-{parts[i+1]}"
                                        combined_normalized = self._normalize_name_for_search(combined)
                                        if self._find_matching_variant(combined_normalized, team_variants):
                                            is_participant = True
                                            break
                                    
                                    # Проверяем комбинацию без дефиса (для "ВоенмехVintage")
                                    if i < len(parts) - 1:
                                        combined_no_hyphen = f"{part}{parts[i+1]}"
                                        combined_normalized = self._normalize_name_for_search(combined_no_hyphen)
                                        if self._find_matching_variant(combined_normalized, team_variants):
                                            is_participant = True
                                            break
                                
                                if is_participant:
                                    break
                        
                        # Если команда не является участником этой игры, пропускаем
                        if not is_participant:
                            continue
                        
                        print(f"      📌 Найдена игра с командой '{team_name}' (строка {row_idx}, ячейка {cell_idx}): {cell_text[:100]}...")
                        # Извлекаем информацию об игре из ячейки
                        game_info = self._extract_game_info_from_schedule_row(cell_text, team_name, base_url)
                        
                        # Добавляем URL, если есть ссылка в ячейке
                        if not game_info.get('url'):
                            link_in_cell = cell.find('a', href=True)
                            if link_in_cell:
                                href = link_in_cell.get('href')
                                if href:
                                    game_info['url'] = href if href.startswith('http') else urljoin(base_url, href)
                        if game_info:
                            print(f"         ✅ Извлечена игра: {game_info.get('date')} {game_info.get('time')} vs {game_info.get('opponent')}")
                            game_info['team_name'] = team_name
                            games.append(game_info)
                    
                    # Также пробуем парсить всю строку целиком (для случаев, когда игры в одной строке)
                    row_text = row.get_text(separator=' ', strip=True)
                    if len(row_text) < 10:
                        continue
                    
                    # Проверяем, есть ли в строке дата (признак расписания игры)
                    date_pattern = r'\d{1,2}\.\d{1,2}\.\d{2,4}'
                    date_matches = list(re.finditer(date_pattern, row_text))
                    if not date_matches:
                        continue
                    
                    # Извлекаем игры по датам - каждая дата может быть началом новой игры
                    # Разбиваем строку на части по датам
                    for i, date_match in enumerate(date_matches):
                        # Определяем границы игры: от текущей даты до следующей даты или до конца строки
                        start_pos = date_match.start()
                        end_pos = date_matches[i + 1].start() if i + 1 < len(date_matches) else len(row_text)
                        game_text = row_text[start_pos:end_pos].strip()
                        
                        if len(game_text) < 10:
                            continue
                        
                        normalized_game_text = self._normalize_name_for_search(game_text)
                        
                        # Проверяем, что команда действительно участвует в этой конкретной игре
                        team_match = self._find_matching_variant(normalized_game_text, team_variants)
                        if not team_match:
                            continue
                        
                        # Проверяем, что команда является участником игры
                        game_separators = [r'\s*[-–—]\s*', r'\s+против\s+', r'\s+vs\s+', r'\s+и\s+']
                        is_participant = False
                        
                        for sep_pattern in game_separators:
                            parts = re.split(sep_pattern, game_text, flags=re.IGNORECASE)
                            if len(parts) >= 2:
                                # Проверяем отдельные части и комбинации (для команд с дефисами)
                                for i, part in enumerate(parts):
                                    part_normalized = self._normalize_name_for_search(part)
                                    if self._find_matching_variant(part_normalized, team_variants):
                                        is_participant = True
                                        break
                                    
                                    # Проверяем комбинацию с предыдущей частью
                                    if i > 0:
                                        combined = f"{parts[i-1]}-{part}"
                                        combined_normalized = self._normalize_name_for_search(combined)
                                        if self._find_matching_variant(combined_normalized, team_variants):
                                            is_participant = True
                                            break
                                    
                                    # Проверяем комбинацию со следующей частью
                                    if i < len(parts) - 1:
                                        combined = f"{part}-{parts[i+1]}"
                                        combined_normalized = self._normalize_name_for_search(combined)
                                        if self._find_matching_variant(combined_normalized, team_variants):
                                            is_participant = True
                                            break
                                    
                                    # Проверяем комбинацию без дефиса
                                    if i < len(parts) - 1:
                                        combined_no_hyphen = f"{part}{parts[i+1]}"
                                        combined_normalized = self._normalize_name_for_search(combined_no_hyphen)
                                        if self._find_matching_variant(combined_normalized, team_variants):
                                            is_participant = True
                                            break
                                
                                if is_participant:
                                    break
                        
                        if not is_participant:
                            continue
                        
                        # Проверяем, не обработали ли мы уже эту игру из ячейки
                        game_info = self._extract_game_info_from_schedule_row(game_text, team_name, base_url)
                        if game_info:
                            # Проверяем дубликаты по дате и времени
                            is_duplicate = False
                            for existing_game in games:
                                if (existing_game.get('date') == game_info.get('date') and 
                                    existing_game.get('time') == game_info.get('time')):
                                    is_duplicate = True
                                    break
                            
                            if not is_duplicate:
                                print(f"      📌 Найдена игра с командой '{team_name}' (строка {row_idx}, по дате): {game_text[:100]}...")
                                print(f"         ✅ Извлечена игра: {game_info.get('date')} {game_info.get('time')} vs {game_info.get('opponent')}")
                                game_info['team_name'] = team_name
                                games.append(game_info)
        except Exception as e:
            print(f"      ⚠️ Ошибка парсинга таблицы: {e}")
            import traceback
            traceback.print_exc()
        
        return games
    
    def _parse_schedule_block_from_text(self, text: str, team_variants: List[str], team_name: str, base_url: str) -> List[Dict[str, Any]]:
        """Парсит блок из текста"""
        games = []
        try:
            if len(text) < 15:
                return games
                
            normalized_text = self._normalize_name_for_search(text)
            team_match = self._find_matching_variant(normalized_text, team_variants)
            if team_match:
                # Проверяем, что команда действительно участвует в игре
                game_separators = [r'\s*[-–—]\s*', r'\s+против\s+', r'\s+vs\s+', r'\s+и\s+']
                is_participant = False
                
                for sep_pattern in game_separators:
                    parts = re.split(sep_pattern, text, flags=re.IGNORECASE)
                    if len(parts) >= 2:
                        # Проверяем отдельные части и комбинации (для команд с дефисами)
                        for i, part in enumerate(parts):
                            part_normalized = self._normalize_name_for_search(part)
                            if self._find_matching_variant(part_normalized, team_variants):
                                is_participant = True
                                break
                            
                            # Проверяем комбинацию с предыдущей частью
                            if i > 0:
                                combined = f"{parts[i-1]}-{part}"
                                combined_normalized = self._normalize_name_for_search(combined)
                                if self._find_matching_variant(combined_normalized, team_variants):
                                    is_participant = True
                                    break
                            
                            # Проверяем комбинацию со следующей частью
                            if i < len(parts) - 1:
                                combined = f"{part}-{parts[i+1]}"
                                combined_normalized = self._normalize_name_for_search(combined)
                                if self._find_matching_variant(combined_normalized, team_variants):
                                    is_participant = True
                                    break
                            
                            # Проверяем комбинацию без дефиса
                            if i < len(parts) - 1:
                                combined_no_hyphen = f"{part}{parts[i+1]}"
                                combined_normalized = self._normalize_name_for_search(combined_no_hyphen)
                                if self._find_matching_variant(combined_normalized, team_variants):
                                    is_participant = True
                                    break
                        
                        if is_participant:
                            break
                
                # Если команда не является участником игры, пропускаем
                if not is_participant:
                    return games
                
                game_info = self._extract_game_info_from_schedule_row(text, team_name, base_url)
                if game_info:
                    print(f"      📌 Найден блок с игрой: {text[:100]}...")
                    print(f"         ✅ Извлечена игра: {game_info.get('date')} {game_info.get('time')} vs {game_info.get('opponent')}")
                    game_info['team_name'] = team_name
                    games.append(game_info)
        except:
            pass
        return games
    
    def _parse_schedule_block(self, block, team_variants: List[str], team_name: str, base_url: str) -> List[Dict[str, Any]]:
        """Парсит блок (div/li/tr) с расписанием игры"""
        games = []
        try:
            block_text = block.get_text(separator=' ', strip=True)
            # Пропускаем очень короткие блоки
            if len(block_text) < 15:
                return games
                
            normalized_text = self._normalize_name_for_search(block_text)
            
            # Проверяем, есть ли наша команда в блоке
            team_match = self._find_matching_variant(normalized_text, team_variants)
            if team_match:
                # Проверяем, что команда действительно участвует в игре (формат "Команда1 - Команда2")
                game_separators = [r'\s*[-–—]\s*', r'\s+против\s+', r'\s+vs\s+', r'\s+и\s+']
                is_participant = False
                
                for sep_pattern in game_separators:
                    parts = re.split(sep_pattern, block_text, flags=re.IGNORECASE)
                    if len(parts) >= 2:
                        # Проверяем отдельные части и комбинации (для команд с дефисами)
                        for i, part in enumerate(parts):
                            part_normalized = self._normalize_name_for_search(part)
                            if self._find_matching_variant(part_normalized, team_variants):
                                is_participant = True
                                break
                            
                            # Проверяем комбинацию с предыдущей частью
                            if i > 0:
                                combined = f"{parts[i-1]}-{part}"
                                combined_normalized = self._normalize_name_for_search(combined)
                                if self._find_matching_variant(combined_normalized, team_variants):
                                    is_participant = True
                                    break
                            
                            # Проверяем комбинацию со следующей частью
                            if i < len(parts) - 1:
                                combined = f"{part}-{parts[i+1]}"
                                combined_normalized = self._normalize_name_for_search(combined)
                                if self._find_matching_variant(combined_normalized, team_variants):
                                    is_participant = True
                                    break
                            
                            # Проверяем комбинацию без дефиса
                            if i < len(parts) - 1:
                                combined_no_hyphen = f"{part}{parts[i+1]}"
                                combined_normalized = self._normalize_name_for_search(combined_no_hyphen)
                                if self._find_matching_variant(combined_normalized, team_variants):
                                    is_participant = True
                                    break
                        
                        if is_participant:
                            break
                
                # Если команда не является участником игры, пропускаем
                if not is_participant:
                    return games
                
                # Проверяем, есть ли в блоке дата, время и вторая команда
                game_info = self._extract_game_info_from_schedule_row(block_text, team_name, base_url)
                if game_info:
                    print(f"      📌 Найден блок с игрой: {block_text[:100]}...")
                    print(f"         ✅ Извлечена игра: {game_info.get('date')} {game_info.get('time')} vs {game_info.get('opponent')}")
                    game_info['team_name'] = team_name
                    games.append(game_info)
        except Exception as e:
            pass  # Тихая ошибка, чтобы не засорять вывод
        
        return games
    
    def _extract_game_info_from_schedule_row(self, text: str, team_name: str, base_url: str) -> Optional[Dict[str, Any]]:
        """Извлекает информацию об игре из строки расписания"""
        try:
            # Ищем дату в формате DD.MM.YYYY или DD.MM.YY
            date_pattern = r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})'
            date_matches = list(re.finditer(date_pattern, text))
            
            if not date_matches:
                return None
            
            # Берем первую дату (обычно это дата игры)
            date_match = date_matches[0]
            
            day, month, year = date_match.groups()
            if len(year) == 2:
                year = '20' + year
            
            date_str = f"{day.zfill(2)}.{month.zfill(2)}.{year}"
            
            # Проверяем, что дата в будущем (фильтруем прошедшие игры)
            try:
                from datetime import datetime
                game_date = datetime.strptime(date_str, '%d.%m.%Y').date()
                today = get_moscow_time().date()
                if game_date <= today:
                    # Это прошедшая или сегодняшняя игра, не извлекаем
                    return None
            except ValueError:
                # Если не удалось распарсить дату, пропускаем
                return None
            
            # Определяем позицию команды (первая или вторая) для правильного определения формы
            # Ищем формат "Команда1 - Команда2" или "Команда1 против Команда2"
            team_position = None  # 1 = первая команда (светлая форма), 2 = вторая команда (темная форма)
            team_variants = list(self._build_name_variants(team_name))
            game_separators = [r'\s*[-–—]\s*', r'\s+против\s+', r'\s+vs\s+', r'\s+и\s+']
            
            for sep_pattern in game_separators:
                parts = re.split(sep_pattern, text, flags=re.IGNORECASE)
                if len(parts) >= 2:
                    # Проверяем первую часть
                    part1_normalized = self._normalize_name_for_search(parts[0])
                    if self._find_matching_variant(part1_normalized, team_variants):
                        team_position = 1  # Первая команда = светлая форма
                        break
                    # Проверяем вторую часть
                    if len(parts) >= 2:
                        part2_normalized = self._normalize_name_for_search(parts[1])
                        if self._find_matching_variant(part2_normalized, team_variants):
                            team_position = 2  # Вторая команда = темная форма
                            break
                if team_position:
                    break
            
            # Ищем время в формате HH:MM или HH.MM
            # Ищем время после даты, но не в самой дате
            date_end_pos = date_match.end()
            text_after_date = text[date_end_pos:]
            
            # Ищем время после даты (формат HH:MM или HH.MM)
            time_pattern = r'(\d{1,2})[:.](\d{2})'
            time_matches = list(re.finditer(time_pattern, text_after_date[:100]))  # Ищем в первых 100 символах после даты
            time_str = "20:00"  # По умолчанию
            
            if time_matches:
                # Берем первое время, которое выглядит как время игры
                for match in time_matches:
                    hours_str = match.group(1)
                    minutes_str = match.group(2)
                    try:
                        hours = int(hours_str)
                        minutes = int(minutes_str)
                        # Проверяем, что это разумное время для игры (8:00 - 23:59)
                        if 8 <= hours <= 23 and 0 <= minutes <= 59:
                            time_str = f"{hours:02d}:{minutes:02d}"
                            break
                    except:
                        continue
            
            # Ищем место/арену (обычно после времени или в конце строки)
            venue = ""
            
            # Сначала ищем полное описание места (MarvelHall ул.Киевская 5)
            # Улучшенный паттерн: останавливаемся перед словами "начало", "в" и другими служебными словами
            full_venue_pattern = r'(MarvelHall[^.]*?ул\.?[^.]*?Киевская[^.]*?\d+[а-я]?)(?:\s|$|начало|в\s*\d|против|vs)'
            full_venue_match = re.search(full_venue_pattern, text, re.IGNORECASE)
            if full_venue_match:
                venue = full_venue_match.group(1).strip()  # Берем первую группу (без служебных слов)
            else:
                # Ищем СШОР с адресом (СШОР В.О.р-на Малый пр. 66)
                sсhor_pattern = r'(СШОР[^.]*?[А-Яа-я\w\s\-\.]*?(?:пр\.?|пр-т|ул\.?|улица)?[^.]*?\d+[а-я]?)'
                sсhor_match = re.search(sсhor_pattern, text, re.IGNORECASE)
                if sсhor_match:
                    venue = sсhor_match.group(0).strip()
                else:
                    # Ищем отдельные части
                    venue_patterns = [
                        r'(?:Зал|Арена|Стадион|Спорткомплекс|Дворец|Центр)[\s:]+([А-Яа-я\w\s\-]+?)(?:\s|$|,|\.)',
                        r'([А-Яа-я\w\s\-]+?)(?:\s+Зал|\s+Арена|\s+Стадион)',
                        r'(MarvelHall[^.]*?ул\.?[^.]*?Киевская[^.]*?\d+[а-я]?)(?:\s|$|начало|в\s*\d|против|vs)',  # MarvelHall с адресом (останавливаемся перед служебными словами)
                        r'(MarvelHall)',  # Просто MarvelHall
                        r'(СШОР[^.]*?[А-Яа-я\w\s\-\.]+)',  # СШОР с названием
                    ]
                    for pattern in venue_patterns:
                        venue_match = re.search(pattern, text, re.IGNORECASE)
                        if venue_match:
                            if len(venue_match.groups()) > 0 and venue_match.group(1):
                                venue = venue_match.group(1).strip()
                            else:
                                venue = venue_match.group(0).strip()
                            break
            
            # Ищем соперника - ищем вторую команду в тексте
            # Сначала убираем нашу команду, дату, время, место
            clean_text = text
            # Убираем название нашей команды (все варианты)
            for variant in self._build_name_variants(team_name):
                clean_text = re.sub(re.escape(variant), '', clean_text, flags=re.IGNORECASE)
            
            # Убираем все даты
            for date_match_obj in date_matches:
                clean_text = clean_text.replace(date_match_obj.group(0), ' ')
            
            # Убираем время
            if time_str != "20:00":
                clean_text = re.sub(re.escape(time_str), '', clean_text)
            clean_text = re.sub(time_pattern, '', clean_text)
            
            # Убираем место и адреса
            if venue:
                clean_text = re.sub(re.escape(venue), '', clean_text, flags=re.IGNORECASE)
            
            # Убираем адреса и места проведения (более полный список)
            # Улицы
            clean_text = re.sub(r'ул\.?\s*[А-Яа-я\w\s\-]*\s*\d+[а-я]?', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'улица\s+[А-Яа-я\w\s\-]*\s*\d+[а-я]?', '', clean_text, flags=re.IGNORECASE)
            # Проспекты
            clean_text = re.sub(r'пр\.?\s*[А-Яа-я\w\s\-]*\s*\d+[а-я]?', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'пр-т\s+[А-Яа-я\w\s\-]*\s*\d+[а-я]?', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'проспект\s+[А-Яа-я\w\s\-]*\s*\d+[а-я]?', '', clean_text, flags=re.IGNORECASE)
            # СШОР и другие спортивные объекты с адресами
            clean_text = re.sub(r'СШОР\s+[А-Яа-я\w\s\-\.]*\s*(?:пр\.?|пр-т|улица|ул\.?)?\s*[А-Яа-я\w\s\-]*\s*\d+[а-я]?', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'СШОР\s+[А-Яа-я\w\s\-\.]+', '', clean_text, flags=re.IGNORECASE)
            # Убираем известные места и их сокращения
            for known_place in ['MarvelHall', 'marvel', 'hall', 'киевская', 'СШОР', 'сшор', 'В.О.', 'В.О.р-на', 'Малый', 'пр.', 'пр-т']:
                clean_text = re.sub(re.escape(known_place), '', clean_text, flags=re.IGNORECASE)
            
            # Убираем служебные слова и разделители
            clean_text = re.sub(r'[-–—]', ' ', clean_text)
            
            # Сначала убираем слово "начало" и его варианты (даже если они идут без пробела)
            # Это нужно делать до общего удаления служебных слов, чтобы правильно обработать случаи типа "Lionначало"
            начало_patterns = [
                r'\bначало\b',  # Полное слово "начало"
                r'начало\s*в\s*\d',  # "начало в 20:00"
                r'начало\s*в',  # "начало в"
                r'начало',  # Любое вхождение "начало" (даже без пробелов)
                r'ачало',  # Остаток "ачало" (если "н" было удалено ранее)
                r'чало',  # Остаток "чало"
                r'начал',  # Часть "начал"
                r'нача',  # Часть "нача"
                r'нач',  # Часть "нач"
                r'ач\b',  # Остаток "ач" в конце слова
            ]
            for pattern in начало_patterns:
                clean_text = re.sub(pattern, ' ', clean_text, flags=re.IGNORECASE)
            
            # Убираем остальные служебные слова
            clean_text = re.sub(r'\b(против|vs|и|игра|матч|турнир|расписание|игр|соревнование|в|ул\.|ул|улица|зал|арена|стадион|центр|дворец|спорткомплекс|ск|цоп)\b', '', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'[\.:;,]', ' ', clean_text)
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            
            # Убираем остатки служебных слов, которые могли остаться из-за отсутствия пробелов
            # Например, "Lion ачало" -> "Lion" или "Lionначало" -> "Lion"
            # Убираем остатки слова "начало" в конце и в середине текста
            clean_text = re.sub(r'\s+[а-я]{1,5}\s*$', '', clean_text, flags=re.IGNORECASE)  # Убираем короткие слова в конце
            clean_text = re.sub(r'\s+[а-я]{1,5}\s+', ' ', clean_text, flags=re.IGNORECASE)  # Убираем короткие слова в середине
            # Также убираем остатки, которые могут быть приклеены к словам (например, "Lionачало")
            clean_text = re.sub(r'([А-Яа-я]+)(ачало|чало|начал|нача|нач|ач)(\s|$)', r'\1\3', clean_text, flags=re.IGNORECASE)
            clean_text = re.sub(r'\s+', ' ', clean_text).strip()
            
            # Ищем название команды соперника
            # Обычно это 1-4 слова с заглавными буквами или все заглавные
            opponent = None
            words = clean_text.split()
            
            # Слова, которые точно не являются частью названия команды (адреса, места)
            excluded_words = {'сшор', 'пр', 'пр-т', 'ул', 'улица', 'проспект', 'малый', 'большой', 
                            'северный', 'южный', 'восточный', 'западный', 'в.о.', 'р-на', 'на', 'в',
                            'начало', 'ачало', 'чало', 'начал', 'нача', 'нач', 'ач'}  # Остатки слова "начало"
            
            # Фильтруем слова - ищем значимые (не слишком короткие, не числа, не адреса)
            significant_words = []
            for word in words:
                word_clean = word.strip('.,;:()[]{}')
                word_lower = word_clean.lower()
                
                # Пропускаем короткие слова, числа, адреса, исключенные слова
                if (len(word_clean) >= 2 and 
                    not word_clean.isdigit() and 
                    not re.match(r'^\d+[а-я]?$', word_lower) and  # Не адреса типа "5", "5а"
                    word_lower not in excluded_words and
                    not word_lower.startswith('пр.') and  # Не "пр.66"
                    not word_lower.startswith('ул.')):  # Не "ул.Киевская"
                    significant_words.append(word_clean)
            
            if significant_words:
                # Берем первые 1-4 значимых слова как название команды
                # Но останавливаемся, если встретили слово, похожее на адрес
                opponent_parts = []
                for word in significant_words[:6]:  # Проверяем больше слов
                    word_lower = word.lower()
                    # Если встретили слово, которое может быть частью адреса, останавливаемся
                    if any(excluded in word_lower for excluded in excluded_words):
                        break
                    if re.match(r'^\d+[а-я]?$', word_lower):  # Число с буквой (типа "66", "5а")
                        break
                    opponent_parts.append(word)
                    if len(opponent_parts) >= 4:  # Максимум 4 слова
                        break
                
                if opponent_parts:
                    opponent = ' '.join(opponent_parts).strip()
            
            if not opponent or len(opponent) < 2:
                return None  # Не нашли соперника
            
            # Проверяем, что это не просто мусор или служебные слова
            opponent_lower = opponent.lower()
            if opponent_lower in ['против', 'vs', 'и', 'игра', 'матч', 'турнир', 'расписание', 'игр', 'соревнование', 'цоп', 'питер',
                                 'начало', 'ачало', 'чало', 'начал', 'нача', 'нач', 'ач']:  # Остатки слова "начало"
                return None
            
            # Дополнительная проверка: если название команды заканчивается на остатки слова "начало", убираем их
            opponent_cleaned = re.sub(r'(ачало|чало|начал|нача|нач|ач)$', '', opponent, flags=re.IGNORECASE).strip()
            if opponent_cleaned and opponent_cleaned != opponent:
                print(f"      🔧 Очищено название команды от остатков 'начало': '{opponent}' -> '{opponent_cleaned}'")
                opponent = opponent_cleaned
            
            # Если название слишком длинное, возможно это не команда
            if len(opponent) > 50:
                return None
            
            # Нормализуем venue - убираем лишние пробелы и остатки служебных слов
            if venue:
                venue = re.sub(r'\s+', ' ', venue).strip()
                # Убираем остатки слова "начало" и других служебных слов в конце venue
                venue = re.sub(r'\s+(начало|ачало|чало|начал|нача|нач|ач|в\s*\d+[:.]?\d*)\s*$', '', venue, flags=re.IGNORECASE)
                # Убираем одиночные буквы в конце (например, "5н" -> "5", но оставляем "5а")
                venue = re.sub(r'(\d+)([нвк])\s*$', r'\1', venue, flags=re.IGNORECASE)  # Убираем "н", "в", "к" после цифр
                venue = venue.strip()
            
            # Формируем результат с информацией о позиции команды для определения формы
            result = {
                'date': date_str,
                'time': time_str,
                'opponent': opponent,
                'venue': venue or 'Не указано',
                'url': '',  # Ссылка может быть, а может и не быть
            }
            
            # Добавляем информацию о позиции команды для определения формы
            # team1_id/team2_id используются в determine_form_color для определения цвета формы
            if team_position == 1:
                # Команда первая = светлая форма
                result['team1'] = team_name
                result['team2'] = opponent
            elif team_position == 2:
                # Команда вторая = темная форма
                result['team1'] = opponent
                result['team2'] = team_name
            
            return result
            
        except Exception as e:
            print(f"      ⚠️ Ошибка извлечения информации из строки: {e}")
            return None
    
    def _remove_duplicate_games(self, games: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Удаляет дубликаты игр (по дате и командам)"""
        seen = set()
        unique_games = []
        
        for game in games:
            date = game.get('date', '')
            opponent = game.get('opponent', '')
            team_name = game.get('team_name', '')
            
            # Создаем ключ для проверки дубликатов
            key = (date, self._normalize_name_for_search(opponent), self._normalize_name_for_search(team_name))
            
            if key not in seen:
                seen.add(key)
                unique_games.append(game)
        
        return unique_games
    
    async def _verify_game_link(self, session: aiohttp.ClientSession, link: str, team_variants: List[str]) -> Optional[str]:
        """Проверяет ссылку на игру и возвращает название команды, если найдено совпадение"""
        try:
            async with session.get(link) as response:
                if response.status != 200:
                    return None
                content = await response.text()
                normalized_content = self._normalize_name_for_search(content)
                return self._find_matching_variant(normalized_content, team_variants)
        except Exception as e:
            print(f"⚠️ Ошибка проверки ссылки {link}: {e}")
            return None
    
    async def _extract_game_info_from_page(self, session: aiohttp.ClientSession, url: str, team_name: str) -> Optional[Dict[str, Any]]:
        """Извлекает информацию об игре со страницы игры"""
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    return None
                content = await response.text()
                soup = BeautifulSoup(content, 'html.parser')
                
                # Пытаемся найти информацию об игре на странице
                text = soup.get_text()
                
                # Ищем дату в формате DD.MM.YYYY или DD.MM.YY
                date_pattern = r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})'
                date_matches = list(re.finditer(date_pattern, text))
                if not date_matches:
                    return None
                
                # Берем первую дату (обычно это дата игры)
                date_match = date_matches[0]
                day, month, year = date_match.groups()
                if len(year) == 2:
                    year = '20' + year
                date_str = f"{day.zfill(2)}.{month.zfill(2)}.{year}"
                
                # Проверяем, что дата в будущем (фильтруем прошедшие игры)
                try:
                    from datetime import datetime
                    game_date = datetime.strptime(date_str, '%d.%m.%Y').date()
                    today = get_moscow_time().date()
                    if game_date <= today:
                        # Это прошедшая или сегодняшняя игра, не извлекаем
                        return None
                except ValueError:
                    return None
                
                # Ищем время в формате HH:MM или HH.MM
                # Ищем время после даты, но не в самой дате
                date_end_pos = date_match.end()
                text_after_date = text[date_end_pos:]
                
                time_pattern = r'(\d{1,2})[:.](\d{2})'
                time_matches = list(re.finditer(time_pattern, text_after_date[:200]))  # Ищем в первых 200 символах после даты
                time_str = "20:00"  # По умолчанию
                
                if time_matches:
                    # Берем первое время, которое выглядит как время игры
                    for match in time_matches:
                        hours_str = match.group(1)
                        minutes_str = match.group(2)
                        try:
                            hours = int(hours_str)
                            minutes = int(minutes_str)
                            # Проверяем, что это разумное время для игры (8:00 - 23:59)
                            # И что это не часть даты (например, не 27.12)
                            if 8 <= hours <= 23 and 0 <= minutes <= 59:
                                time_str = f"{hours:02d}:{minutes:02d}"
                                break
                        except:
                            continue
                
                # Пытаемся найти название соперника
                # Ищем названия команд на странице
                opponent = 'Соперник'
                team_variants = list(self._build_name_variants(team_name))
                
                # Ищем паттерны типа "Команда1 - Команда2" или "Команда1 против Команда2"
                game_separators = [r'\s*[-–—]\s*', r'\s+против\s+', r'\s+vs\s+', r'\s+и\s+']
                for sep_pattern in game_separators:
                    parts = re.split(sep_pattern, text, flags=re.IGNORECASE)
                    if len(parts) >= 2:
                        # Проверяем, есть ли наша команда в одной из частей
                        for i, part in enumerate(parts):
                            part_normalized = self._normalize_name_for_search(part)
                            if self._find_matching_variant(part_normalized, team_variants):
                                # Нашли нашу команду, берем другую часть как соперника
                                if i == 0 and len(parts) > 1:
                                    opponent = parts[1].strip()[:50]  # Ограничиваем длину
                                elif i > 0:
                                    opponent = parts[0].strip()[:50]
                                break
                        if opponent != 'Соперник':
                            break
                
                # Ищем место/арену
                venue = ""
                venue_patterns = [
                    r'(MarvelHall[^.]*?ул\.?[^.]*?Киевская[^.]*?\d+[а-я]?)',
                    r'(СШОР[^.]*?[А-Яа-я\w\s\-\.]*?(?:пр\.?|пр-т|ул\.?|улица)?[^.]*?\d+[а-я]?)',
                    r'(?:Зал|Арена|Стадион|Спорткомплекс|Дворец|Центр)[\s:]+([А-Яа-я\w\s\-]+?)(?:\s|$|,|\.)',
                ]
                for pattern in venue_patterns:
                    venue_match = re.search(pattern, text, re.IGNORECASE)
                    if venue_match:
                        venue = venue_match.group(1 if venue_match.groups() else 0).strip()
                        break
                
                return {
                    'date': date_str,
                    'time': time_str,
                    'opponent': opponent,
                    'venue': venue,
                }
        except Exception as e:
            print(f"⚠️ Ошибка извлечения информации со страницы {url}: {e}")
            return None
    
    def _extract_game_info_from_text(self, text: str, team_name: str) -> Optional[Dict[str, Any]]:
        """Извлекает информацию об игре из текста ссылки"""
        try:
            # Пытаемся найти дату в формате DD.MM.YYYY или DD.MM.YY
            date_pattern = r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})'
            date_match = re.search(date_pattern, text)
            
            if not date_match:
                return None
            
            day, month, year = date_match.groups()
            if len(year) == 2:
                year = '20' + year
            
            date_str = f"{day.zfill(2)}.{month.zfill(2)}.{year}"
            
            # Пытаемся найти время в формате HH:MM или HH.MM
            # Ищем время после даты, но не в самой дате
            date_end_pos = date_match.end()
            text_after_date = text[date_end_pos:]
            
            time_pattern = r'(\d{1,2})[:.](\d{2})'
            time_matches = list(re.finditer(time_pattern, text_after_date[:200]))  # Ищем в первых 200 символах после даты
            time_str = "20:00"  # По умолчанию
            
            if time_matches:
                # Берем первое время, которое выглядит как время игры
                for match in time_matches:
                    hours_str = match.group(1)
                    minutes_str = match.group(2)
                    try:
                        hours = int(hours_str)
                        minutes = int(minutes_str)
                        # Проверяем, что это разумное время для игры (8:00 - 23:59)
                        # И что это не часть даты (например, не 27.12)
                        if 8 <= hours <= 23 and 0 <= minutes <= 59:
                            time_str = f"{hours:02d}:{minutes:02d}"
                            break
                    except:
                        continue
            
            # Пытаемся найти название соперника
            # Убираем название нашей команды и дату/время из текста
            opponent_text = text
            opponent_text = re.sub(team_name, '', opponent_text, flags=re.IGNORECASE)
            opponent_text = re.sub(date_pattern, '', opponent_text)
            opponent_text = re.sub(time_pattern, '', opponent_text)
            opponent_text = re.sub(r'[-–—]', ' ', opponent_text)
            opponent_text = re.sub(r'\s+', ' ', opponent_text).strip()
            
            # Берем первое слово как название соперника (можно улучшить)
            opponent = opponent_text.split()[0] if opponent_text.split() else "Соперник"
            
            return {
                'date': date_str,
                'time': time_str,
                'opponent': opponent,
                'venue': '',  # Можно попытаться извлечь из текста
            }
            
        except Exception as e:
            print(f"⚠️ Ошибка извлечения информации об игре из текста: {e}")
            return None
    
    async def get_games_from_api(self, comp_ids: List[int], team_ids: List[int]) -> List[Dict[str, Any]]:
        """Получает игры через API Infobasket по ID соревнований и команд"""
        if not comp_ids and not team_ids:
            return []
        
        try:
            parser = InfobasketSmartParser(
                comp_ids=comp_ids if comp_ids else None,
                team_ids=team_ids if team_ids else None
            )
            
            all_games = await parser.get_all_team_games()
            games_list = []
            
            for team_type, games_dict in all_games.items():
                for game in games_dict.get('future', []):
                    poll_data = parser.format_poll_data(game)
                    poll_data['team_type'] = team_type
                    games_list.append(poll_data)
            
            print(f"✅ Получено {len(games_list)} игр через API")
            return games_list
            
        except Exception as e:
            print(f"❌ Ошибка получения игр через API: {e}")
            return []
    
    def _compare_games(self, api_game: Dict[str, Any], site_game: Dict[str, Any]) -> bool:
        """Сравнивает игры из API и с сайта по дате и противнику"""
        api_date = api_game.get('date', '')
        api_opponent = api_game.get('team_b', '') or api_game.get('opponent', '') or api_game.get('team2', '')
        
        site_date = site_game.get('date', '')
        site_opponent = site_game.get('opponent', '') or site_game.get('team2', '')
        
        # Нормализуем даты для сравнения
        try:
            from datetime import datetime
            dates_match = False
            if api_date and site_date:
                try:
                    api_date_obj = datetime.strptime(api_date, '%d.%m.%Y').date()
                    site_date_obj = datetime.strptime(site_date, '%d.%m.%Y').date()
                    dates_match = api_date_obj == site_date_obj
                except ValueError:
                    dates_match = api_date == site_date
            else:
                dates_match = api_date == site_date
            
            if not dates_match:
                return False
            
            # Нормализуем названия противников для сравнения
            api_opponent_norm = self._normalize_name_for_search(api_opponent)
            site_opponent_norm = self._normalize_name_for_search(site_opponent)
            
            # Проверяем, есть ли совпадение в названиях
            opponents_match = (
                (api_opponent_norm and site_opponent_norm and (
                    api_opponent_norm in site_opponent_norm or
                    site_opponent_norm in api_opponent_norm or
                    api_opponent_norm == site_opponent_norm
                )) or (not api_opponent_norm and not site_opponent_norm)
            )
            
            return opponents_match
            
        except Exception as e:
            print(f"⚠️ Ошибка сравнения игр: {e}")
            return False
    
    async def process_fallback_config(self, config: Dict[str, Any]) -> None:
        """Обрабатывает одну fallback конфигурацию
        
        Логика работы:
        1. Если есть ИД команды и URL:
           - Сначала ищем расписание по ИД через API
           - Потом ищем на сайте по "КОМАНДА ДЛЯ FALLBACK"
           - При пересечении игр берем информацию с сайта (более актуальная)
           - Проверяем через сервисный лист для избежания дубликатов
        
        2. Если только URL (без ИД):
           - Ищем на сайте только по "КОМАНДА ДЛЯ FALLBACK"
        
        Альтернативное имя из CONFIG_TEAM НЕ используется для поиска.
        """
        comp_ids = config.get('comp_ids', [])
        team_ids = config.get('team_ids', [])
        url = config.get('url', '')
        fallback_name = config.get('name', '').strip()  # Поле "КОМАНДА ДЛЯ FALLBACK"
        
        print(f"\n🔄 Обработка fallback конфигурации: URL={url}")
        
        # Инициализируем переменные для игр
        api_games = []
        site_games = []
        
        # Шаг 1: Если есть ИД команды, получаем игры через API
        if comp_ids or team_ids:
            print(f"📋 Получение расписания через API: CompIDs={comp_ids}, TeamIDs={team_ids}")
            api_games = await self.get_games_from_api(comp_ids, team_ids)
            print(f"   ✅ Получено {len(api_games)} игр через API")
        
        # Шаг 2: Если есть URL, ищем игры на сайте
        if url:
            if not fallback_name:
                print("⚠️ URL указан, но команда для fallback не указана, пропускаем поиск на сайте")
            else:
                print(f"🔍 Поиск игр на сайте {url} для команды '{fallback_name}'")
                site_games = await self.parse_fallback_page(url, fallback_name)
                print(f"   ✅ Найдено {len(site_games)} игр на сайте")
        
        # Шаг 3: Обрабатываем игры
        if api_games and site_games:
            # Есть и API и сайт - сравниваем и объединяем
            print(f"\n📊 Сравнение игр из API и с сайта...")
            
            # Словарь для отслеживания обработанных игр из API
            processed_api_games = set()
            
            # Проходим по играм с сайта - они имеют приоритет
            for site_game in site_games:
                matched_api_game = None
                matched_api_idx = None
                
                # Ищем совпадение в API
                for idx, api_game in enumerate(api_games):
                    if idx in processed_api_games:
                        continue
                    if self._compare_games(api_game, site_game):
                        matched_api_game = api_game
                        matched_api_idx = idx
                        break
                
                if matched_api_game:
                    # Есть пересечение - используем данные с сайта (более актуальные)
                    print(f"✅ Игра найдена и в API и на сайте: {site_game.get('date')} "
                          f"против {site_game.get('opponent', 'Соперник')}")
                    print(f"   Используем данные с сайта (приоритет)")
                    
                    # Объединяем данные: базовые данные из API, но информация с сайта приоритетна
                    merged_game = {
                        **matched_api_game,  # Базовые данные из API
                        **site_game,  # Данные с сайта перезаписывают
                        'date': site_game.get('date') or matched_api_game.get('date'),
                        'time': site_game.get('time') or matched_api_game.get('time'),
                        'venue': site_game.get('venue') or matched_api_game.get('venue', 'Не указано'),
                        'opponent': site_game.get('opponent') or matched_api_game.get('team_b') or matched_api_game.get('opponent'),
                        'team_name': site_game.get('team_name') or matched_api_game.get('team_a') or matched_api_game.get('team_name'),
                    }
                    
                    await self._create_poll_if_needed(merged_game, source='site_priority')
                    processed_api_games.add(matched_api_idx)
                else:
                    # Игра только на сайте
                    print(f"⚠️ Игра найдена только на сайте: {site_game.get('date')} "
                          f"против {site_game.get('opponent', 'Соперник')}")
                    await self._create_poll_if_needed(site_game, source='site_only')
            
            # Обрабатываем игры из API, которых нет на сайте
            for idx, api_game in enumerate(api_games):
                if idx not in processed_api_games:
                    print(f"⚠️ Игра найдена только в API: {api_game.get('date')} "
                          f"против {api_game.get('team_b') or api_game.get('opponent', 'Соперник')}")
                    await self._create_poll_if_needed(api_game, source='api_only')
        
        elif api_games:
            # Только API
            print(f"\n📋 Обработка игр только из API...")
            for api_game in api_games:
                await self._create_poll_if_needed(api_game, source='api_only')
        
        elif site_games:
            # Только сайт
            print(f"\n📋 Обработка игр только с сайта...")
            for site_game in site_games:
                await self._create_poll_if_needed(site_game, source='site_only')
        
        else:
            print("⚠️ Игры не найдены ни в API, ни на сайте")
    
    def _needs_playwright(self, url: str) -> bool:
        """Определяет, нужен ли Playwright для парсинга этого сайта"""
        js_sites = ['globalleague.ru', 'neva-basket.ru']
        return any(site in url for site in js_sites)
    
    async def _fetch_with_playwright(self, url: str, timeout: int = 60000) -> Optional[str]:
        """Загружает страницу с помощью Playwright для рендеринга JavaScript"""
        if not PLAYWRIGHT_AVAILABLE:
            return None
        
        try:
            print(f"   🌐 Загрузка страницы через Playwright (рендеринг JavaScript)...")
            async with async_playwright() as p:
                # Запускаем браузер в headless режиме
                browser = await p.chromium.launch(
                    headless=True,
                    args=['--no-sandbox', '--disable-setuid-sandbox']  # Для стабильности в CI
                )
                context = await browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                page = await context.new_page()
                
                try:
                    # Загружаем страницу и ждем загрузки контента
                    await page.goto(url, wait_until='domcontentloaded', timeout=timeout)
                    
                    # Для globalleague.ru и других сайтов с динамическим контентом
                    # Пробуем несколько стратегий ожидания загрузки данных
                    
                    # Стратегия 1: Ждем появления таблиц
                    try:
                        await page.wait_for_selector('table', timeout=15000)
                        print(f"   ✅ Таблицы найдены на странице")
                    except:
                        print(f"   ⚠️ Таблицы не найдены, продолжаем...")
                    
                    # Стратегия 2: Ждем загрузки данных через AJAX/Angular
                    try:
                        # Для AngularJS приложений
                        await page.wait_for_function(
                            'document.querySelectorAll("table tbody tr").length > 0 || document.querySelectorAll("td, th").length > 10',
                            timeout=10000
                        )
                        print(f"   ✅ Данные загружены")
                    except:
                        pass
                    
                    # Стратегия 3: Дополнительное ожидание для полной загрузки
                    await page.wait_for_timeout(3000)
                    
                    # Стратегия 4: Прокручиваем страницу для загрузки lazy-loaded контента
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await page.wait_for_timeout(2000)
                    await page.evaluate('window.scrollTo(0, 0)')
                    await page.wait_for_timeout(1000)
                    
                    # Стратегия 5: Для globalleague.ru - ждем загрузки данных в таблице
                    if 'globalleague.ru' in url:
                        try:
                            # Ждем, пока таблица заполнится данными
                            await page.wait_for_function(
                                '''
                                () => {
                                    const tables = document.querySelectorAll('table');
                                    for (let table of tables) {
                                        const rows = table.querySelectorAll('tbody tr, tr');
                                        if (rows.length > 2) return true;
                                    }
                                    return false;
                                }
                                ''',
                                timeout=10000
                            )
                            print(f"   ✅ Таблица заполнена данными")
                        except:
                            pass
                    
                    # Получаем HTML после рендеринга JavaScript
                    content = await page.content()
                    
                    # Проверяем, что контент действительно загрузился
                    if len(content) < 1000:
                        print(f"   ⚠️ Получен слишком короткий контент ({len(content)} символов)")
                        return None
                    
                    print(f"   ✅ Страница загружена через Playwright ({len(content)} символов)")
                    return content
                finally:
                    await browser.close()
        except Exception as e:
            print(f"   ⚠️ Ошибка загрузки страницы через Playwright: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _create_game_key(self, date: str, time: str, team_name: str, opponent: str) -> str:
        """Создает ключ игры в том же формате, что и create_game_key из game_system_manager"""
        time_normalized = time.replace('.', ':')
        return create_game_key({
            'date': date,
            'time': time_normalized,
            'team1': team_name,
            'team2': opponent
        })
    
    async def _create_poll_if_needed(self, game_info: Dict[str, Any], source: str) -> None:
        """Создает опрос для игры, если нужно"""
        try:
            # Форматируем game_info в формат, который ожидает GameSystemManager
            game_id = game_info.get('game_id')
            date = game_info.get('date')
            time = game_info.get('time', '20:00')
            team_name = game_info.get('team_name') or game_info.get('team_a', '')
            opponent = game_info.get('opponent') or game_info.get('team_b', '')
            
            # Проверяем, что дата игры в будущем
            if date:
                try:
                    from datetime import datetime
                    game_date = datetime.strptime(date, '%d.%m.%Y').date()
                    today = get_moscow_time().date()
                    if game_date <= today:
                        print(f"⏭️ Игра {date} уже прошла или сегодня, опрос не создается")
                        return
                except ValueError:
                    print(f"⚠️ Некорректный формат даты: {date}")
            
            # Проверяем, не создан ли уже опрос для этой игры
            # Сначала проверяем по game_id, если есть
            if game_id:
                existing = duplicate_protection.get_game_record("ОПРОС_ИГРА", str(game_id))
                if existing:
                    print(f"⏭️ Опрос для игры {game_id} уже существует")
                    return
            
            # Убеждаемся, что opponent не содержит места проведения
            opponent_clean = opponent.strip()
            venue_clean = game_info.get('venue', '').strip()
            
            # Если opponent содержит venue, убираем его
            if venue_clean and venue_clean in opponent_clean:
                opponent_clean = opponent_clean.replace(venue_clean, '').strip()
                # Убираем лишние пробелы
                opponent_clean = re.sub(r'\s+', ' ', opponent_clean).strip()
            
            # Также проверяем по дате и командам (на случай, если game_id нет)
            # Используем тот же формат ключа, что и в create_game_key из game_system_manager
            game_key = None
            if date and team_name and opponent_clean:
                game_key = self._create_game_key(date, time, team_name, opponent_clean)
                duplicate_check = duplicate_protection.check_duplicate("ОПРОС_ИГРА", game_key)
                if duplicate_check.get('exists'):
                    print(f"⏭️ Опрос для игры {date} {team_name} vs {opponent_clean} уже существует (ключ: {game_key})")
                    return
            
            # Определяем позицию команды для правильного определения формы
            # Если команда указана как team1, то форма светлая, если team2 - темная
            team1_from_info = game_info.get('team1')
            team2_from_info = game_info.get('team2')
            
            # Если позиция команды определена при парсинге, используем её
            if team1_from_info and team2_from_info:
                # Позиция уже определена при парсинге
                our_team_id = None  # Для fallback нет ID команды
                if team1_from_info == team_name:
                    # Наша команда первая = светлая форма
                    team1_id = 1  # Временный ID для определения формы
                    team2_id = 2
                elif team2_from_info == team_name:
                    # Наша команда вторая = темная форма
                    team1_id = 2
                    team2_id = 1
                else:
                    # По умолчанию - первая команда
                    team1_id = 1
                    team2_id = 2
            else:
                # Позиция не определена, используем значения по умолчанию
                team1_from_info = team_name
                team2_from_info = opponent_clean
                team1_id = 1
                team2_id = 2
            
            formatted_game = {
                'game_id': game_id,
                'date': date,
                'time': time,
                'team1': team1_from_info or team_name,
                'team2': team2_from_info or opponent_clean,
                'team1_id': team1_id,  # Для определения формы
                'team2_id': team2_id,  # Для определения формы
                'our_team_id': team1_id if team1_from_info == team_name else team2_id,  # ID нашей команды для определения формы
                'venue': venue_clean or 'Не указано',
                'comp_id': game_info.get('comp_id'),
                'game_link': game_info.get('url') or game_info.get('game_link', ''),
                'our_team_name': team_name,
                'opponent_team_name': opponent_clean,  # Основное поле для соперника
                'team_type': game_info.get('team_type', 'configured'),
            }
            
            venue_display = venue_clean if venue_clean else 'Не указано'
            print(f"   🔍 Формирование опроса:")
            print(f"      Соперник (opponent_team_name): '{opponent_clean}'")
            print(f"      Соперник (team2): '{opponent_clean}'")
            print(f"      Место (venue): '{venue_display}'")
            
            # Создаем опрос через GameSystemManager
            question = await self.game_manager.create_game_poll(formatted_game)
            if question:
                print(f"✅ Создан опрос для игры из {source}: {question[:50]}...")
                # Логируем в сервисный лист с правильным ключом
                # Переиспользуем game_key, созданный выше для проверки дубликата
                if not game_key and date and team_name and opponent_clean:
                    game_key = self._create_game_key(date, time, team_name, opponent_clean)
                
                # Используем game_key как identifier для единообразия
                identifier = str(game_id) if game_id else (game_key or f"fallback_{date}_{team_name}_{opponent_clean}")
                duplicate_protection.add_record(
                    data_type="ОПРОС_ИГРА",
                    identifier=identifier,
                    status="АКТИВЕН",
                    additional_data=f"Источник: {source}",
                    game_link=formatted_game.get('game_link', ''),
                    comp_id=formatted_game.get('comp_id'),
                    team_id=None,  # Можно добавить, если есть
                    game_id=game_id,
                    game_date=formatted_game.get('date', ''),
                    game_time=formatted_game.get('time', ''),
                    arena=formatted_game.get('venue', ''),
                )
            else:
                print(f"⚠️ Не удалось создать опрос для игры из {source}")
                
        except Exception as e:
            print(f"❌ Ошибка создания опроса: {e}")
            import traceback
            traceback.print_exc()
    
    async def run_monitoring(self) -> None:
        """Запускает мониторинг всех fallback-источников"""
        print("🚀 Запуск мониторинга fallback-источников")
        
        configs = self.get_fallback_configs()
        if not configs:
            print("ℹ️ Fallback конфигурации не найдены")
            return
        
        print(f"📋 Найдено {len(configs)} fallback конфигураций")
        
        for config in configs:
            try:
                await self.process_fallback_config(config)
            except Exception as e:
                print(f"❌ Ошибка обработки конфигурации {config}: {e}")
        
        print("✅ Мониторинг fallback-источников завершен")


async def main():
    """Тестирование модуля"""
    monitor = FallbackGameMonitor()
    await monitor.run_monitoring()


if __name__ == "__main__":
    asyncio.run(main())

