#!/usr/bin/env python3
"""
Модуль для управления данными игроков через Google Sheets
"""

import os
import json
import datetime
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import gspread

# Загружаем переменные окружения
load_dotenv()

# Получаем переменные окружения
GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")

# Настройки Google Sheets
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

def _tidy(value) -> str:
    """Значение из листа без краёв и двойных пробелов.

    Лист заполняют руками, и лишний пробел в фамилии невидим, но ломает всё,
    что сверяет имена: поиск игрока, сопоставление с протоколом лиги, ключ
    цены. Чинить это в каждом потребителе бессмысленно — режем на входе."""
    return " ".join(str(value or "").split())


class PlayersManager:
    """Менеджер данных игроков"""
    
    def __init__(self):
        self.gc = None
        self.spreadsheet = None
        self.players_sheet = None
        self._init_google_sheets()
    
    def _init_google_sheets(self):
        """Инициализация Google Sheets"""
        try:
            if not GOOGLE_SHEETS_CREDENTIALS:
                print("⚠️ GOOGLE_SHEETS_CREDENTIALS не настроен")
                return
            
            if not SPREADSHEET_ID:
                print("⚠️ SPREADSHEET_ID не настроен")
                return
            
            print(f"🔍 Отладка: SPREADSHEET_ID = {SPREADSHEET_ID}")
            print(f"🔍 Отладка: GOOGLE_SHEETS_CREDENTIALS длина = {len(GOOGLE_SHEETS_CREDENTIALS)} символов")
            
            # Парсим JSON credentials с тщательной очисткой
            try:
                # Сначала пробуем прямой парсинг
                creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
                print("✅ JSON credentials успешно распарсен (прямой)")
            except json.JSONDecodeError as e:
                print(f"⚠️ Ошибка прямого парсинга: {e}")
                try:
                    # Тщательная очистка от всех проблемных символов
                    cleaned_credentials = GOOGLE_SHEETS_CREDENTIALS
                    
                    # Убираем экранированные символы
                    cleaned_credentials = cleaned_credentials.replace('\\n', '\n').replace('\\r', '\r').replace('\\t', '\t')
                    
                    # Убираем недопустимые управляющие символы
                    import re
                    cleaned_credentials = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', cleaned_credentials)
                    
                    # Убираем лишние пробелы
                    cleaned_credentials = cleaned_credentials.strip()
                    
                    # Содержимое credentials в вывод не печатаем никогда —
                    # это ключ сервисного аккаунта, а stdout уходит в логи.
                    creds_dict = json.loads(cleaned_credentials)
                    print("✅ JSON credentials успешно распарсен (после тщательной очистки)")
                except json.JSONDecodeError as e2:
                    print(f"❌ Ошибка парсинга JSON credentials: {e2}")
                    print(f"🔍 Длина оригинала: {len(GOOGLE_SHEETS_CREDENTIALS)}, после очистки: {len(cleaned_credentials)}")
                    return
            
            # Проверяем обязательные поля
            required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email']
            for field in required_fields:
                if field not in creds_dict:
                    print(f"❌ Отсутствует обязательное поле: {field}")
                    return
            
            print(f"✅ Все обязательные поля присутствуют")
            print(f"📧 Сервисный аккаунт: {creds_dict.get('client_email', 'Не найден')}")
            
            # Обрабатываем private_key - добавляем переносы строк
            if 'private_key' in creds_dict:
                private_key = creds_dict['private_key']
                if isinstance(private_key, str):
                    # Убираем экранированные символы из private_key
                    cleaned_private_key = private_key.replace('\\n', '\n').replace('\\r', '\r').replace('\\t', '\t')
                    
                    # Если ключ в одной строке, добавляем переносы строк
                    if '\n' not in cleaned_private_key:
                        print("⚠️ Private key в одной строке, добавляем переносы строк...")
                        
                        # Добавляем переносы строк в нужных местах
                        # Находим позиции для переносов строк (каждые ~64 символа)
                        key_content = cleaned_private_key.replace('-----BEGIN PRIVATE KEY-----', '').replace('-----END PRIVATE KEY-----', '')
                        key_content = key_content.strip()
                        
                        # Разбиваем на строки по 64 символа
                        lines = []
                        for i in range(0, len(key_content), 64):
                            lines.append(key_content[i:i+64])
                        
                        # Собираем обратно с переносами строк
                        formatted_key = '-----BEGIN PRIVATE KEY-----\n' + '\n'.join(lines) + '\n-----END PRIVATE KEY-----\n'
                        cleaned_private_key = formatted_key
                        print(f"✅ Private key отформатирован с переносами строк")
                    
                    creds_dict['private_key'] = cleaned_private_key
                    print(f"✅ Private key обработан (длина: {len(cleaned_private_key)}, строк: {cleaned_private_key.count(chr(10))})")
            
            # Авторизуемся через google-auth напрямую
            try:
                from google.oauth2.service_account import Credentials
                
                # Создаем credentials через google-auth
                creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
                print("✅ Credentials созданы через google-auth")
                
                # Авторизуемся через gspread
                self.gc = gspread.authorize(creds)
                print("✅ Авторизация в Google API успешна через google-auth")
                
            except Exception as e:
                print(f"❌ Ошибка авторизации через google-auth: {e}")
                print(f"🔍 Тип creds_dict: {type(creds_dict)}")
                print(f"🔍 Ключи в creds_dict: {list(creds_dict.keys())}")
                
                # Попробуем альтернативный способ с временным файлом
                try:
                    import tempfile
                    import os
                    
                    print("🔄 Пробуем альтернативный способ с временным файлом...")
                    
                    # Создаем временный файл с credentials
                    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                        json.dump(creds_dict, f, ensure_ascii=False, indent=2)
                        temp_file = f.name
                    
                    print(f"📁 Создан временный файл: {temp_file}")
                    
                    # Используем временный файл для авторизации
                    self.gc = gspread.service_account(temp_file)
                    print("✅ Авторизация в Google API успешна через временный файл")
                    
                    # Удаляем временный файл
                    os.unlink(temp_file)
                    print("🗑️ Временный файл удален")
                    
                except Exception as e2:
                    print(f"❌ Ошибка авторизации через временный файл: {e2}")
                    # Удаляем временный файл если он был создан
                    if 'temp_file' in locals():
                        try:
                            os.unlink(temp_file)
                        except:
                            pass
                    return
            
            # Открываем таблицу
            try:
                self.spreadsheet = self.gc.open_by_key(SPREADSHEET_ID)
                print(f"✅ Таблица найдена: {self.spreadsheet.title}")
            except gspread.SpreadsheetNotFound:
                print(f"❌ Таблица с ID {SPREADSHEET_ID} не найдена")
                return
            except gspread.APIError as e:
                print(f"❌ Ошибка API при открытии таблицы: {e}")
                return
            
            # Получаем или создаем лист "Игроки"
            try:
                # Сначала показываем все доступные листы
                all_worksheets = self.spreadsheet.worksheets()
                print(f"📋 Доступные листы в таблице:")
                for ws in all_worksheets:
                    print(f"   - {ws.title}")
                
                self.players_sheet = self.spreadsheet.worksheet("Игроки")
                print("✅ Лист 'Игроки' найден")
            except gspread.WorksheetNotFound:
                print("⚠️ Лист 'Игроки' не найден, создаем новый...")
                try:
                    # Создаем новый лист
                    self.players_sheet = self.spreadsheet.add_worksheet(
                        title="Игроки", 
                        rows=100, 
                        cols=10
                    )
                    
                    # Создаем заголовки
                    headers = [
                        "Фамилия", "Имя", "Ник", "Telegram ID", "Дата рождения", 
                        "Статус", "Команда", "Дата добавления", "Примечания"
                    ]
                    self.players_sheet.update('A1:I1', [headers])
                    print("✅ Лист 'Игроки' создан с заголовками")
                except Exception as e:
                    print(f"❌ Ошибка создания листа 'Игроки': {e}")
                    return
            except Exception as e:
                print(f"❌ Ошибка при работе с листом 'Игроки': {e}")
                return
            
            print("✅ Google Sheets подключен успешно")
                
        except Exception as e:
            print(f"❌ Ошибка инициализации Google Sheets: {e}")
            import traceback
            print(f"🔍 Подробности ошибки:")
            traceback.print_exc()
    
    def get_all_players(self) -> List[Dict[str, Any]]:
        """Получает всех игроков из таблицы"""
        try:
            if not self.players_sheet:
                print("❌ Лист 'Игроки' не доступен")
                return []
            
            # Получаем все данные
            all_records = self.players_sheet.get_all_records()
            
            players = []
            for record in all_records:
                # Проверяем обязательные поля
                if record.get('Имя') and record.get('Дата рождения'):
                    player = {
                        # Края и двойные пробелы срезаем сразу: «Кондратьев »
                        # с хвостом не совпадал с «Кондратьев» ни в поиске, ни
                        # при сверке с протоколом лиги, а глазами это не видно.
                        'surname': _tidy(record.get('Фамилия', '')),
                        'name': _tidy(record.get('Имя', '')),
                        'nickname': _tidy(record.get('Ник', '')),
                        'telegram_id': _tidy(record.get('Telegram ID', '')),
                        'birthday': _tidy(record.get('Дата рождения', '')),
                        'status': _tidy(record.get('Статус', 'Активный')),
                        'active_mark': str(record.get('Активность', '')).strip(),
                        'team': _tidy(record.get('Команда', '')),
                        'added_date': record.get('Дата добавления', ''),
                        'notes': record.get('Примечания', '')
                    }
                    players.append(player)
            
            print(f"✅ Загружено {len(players)} игроков")
            return players
            
        except Exception as e:
            print(f"❌ Ошибка получения игроков: {e}")
            return []
    
    def get_active_players(self) -> List[Dict[str, Any]]:
        """Получает только активных игроков"""
        all_players = self.get_all_players()
        return [p for p in all_players if p.get('status', '').lower() == 'активный']
    
    def get_players_with_birthdays_today(self) -> List[Dict[str, Any]]:
        """Именинники на сегодня — из ВСЕГО листа «Игроки».

        Поздравляем каждого, кто в листе, независимо от «Активности»: отметка
        там про оплату тренировок, а не про то, человек с нами или нет.
        Временно выпавший из состава на день рождения не перестаёт быть
        своим."""
        try:
            active_players = self.get_all_players()
            today = datetime.datetime.now()
            today_str = today.strftime("%m-%d")
            
            print(f"📅 Проверяем дни рождения на {today_str}")
            print(f"👥 Игроков в листе: {len(active_players)}")
            
            birthday_players = []
            for player in active_players:
                birthday = player.get('birthday', '')
                name = player.get('name', 'Unknown')
                surname = player.get('surname', '')
                
                if birthday:
                    try:
                        # Парсим дату рождения
                        if '-' in birthday:
                            # Формат YYYY-MM-DD
                            bd_date = datetime.datetime.strptime(birthday, "%Y-%m-%d")
                        elif '.' in birthday:
                            # Формат DD.MM.YYYY
                            bd_date = datetime.datetime.strptime(birthday, "%d.%m.%Y")
                        else:
                            print(f"⚠️ Неизвестный формат даты для {surname} {name}: {birthday}")
                            continue
                        
                        bd_str = bd_date.strftime("%m-%d")
                        print(f"🔍 Проверяем {surname} {name}: {birthday} -> {bd_str} vs {today_str}")
                        
                        if bd_str == today_str:
                            # Вычисляем возраст
                            age = today.year - bd_date.year
                            if today.month < bd_date.month or (today.month == bd_date.month and today.day < bd_date.day):
                                age -= 1
                            
                            player['age'] = age
                            birthday_players.append(player)
                            print(f"🎉 Найден именинник: {surname} {name} ({age} лет)")
                            
                    except ValueError:
                        print(f"⚠️ Неверный формат даты для {surname} {name}: {birthday}")
                        continue
                else:
                    print(f"⚠️ Нет даты рождения для {surname} {name}")
            
            print(f"🎂 Всего именинников сегодня: {len(birthday_players)}")
            return birthday_players
            
        except Exception as e:
            print(f"❌ Ошибка получения дней рождения: {e}")
            return []
    
    def add_player(self, name: str, birthday: str, nickname: str = "", 
                   telegram_id: str = "", team: str = "", notes: str = "", surname: str = "") -> bool:
        """Добавляет нового игрока"""
        try:
            if not self.players_sheet:
                print("❌ Лист 'Игроки' не доступен")
                return False
            
            # Проверяем обязательные поля
            if not name or not birthday:
                print("❌ Имя и дата рождения обязательны")
                return False
            
            # Подготавливаем данные (новая структура)
            row_data = [
                surname,  # Фамилия
                name,     # Имя
                nickname, # Ник
                telegram_id, # Telegram ID
                birthday, # Дата рождения
                "Активный", # Статус
                team,     # Команда
                datetime.datetime.now().strftime("%Y-%m-%d"), # Дата добавления
                notes     # Примечания
            ]
            
            # Добавляем строку
            self.players_sheet.append_row(row_data)
            print(f"✅ Игрок {surname} {name} добавлен")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка добавления игрока: {e}")
            return False
    
    def update_player_status(self, name: str, status: str) -> bool:
        """Обновляет статус игрока"""
        try:
            if not self.players_sheet:
                return False
            
            # Ищем игрока по имени
            all_records = self.players_sheet.get_all_records()
            for i, record in enumerate(all_records, start=2):  # Начинаем с 2 (после заголовков)
                if record.get('Имя') == name:
                    # Обновляем статус
                    self.players_sheet.update(f'E{i}', status)
                    print(f"✅ Статус игрока {name} обновлен на '{status}'")
                    return True
            
            print(f"❌ Игрок {name} не найден")
            return False
            
        except Exception as e:
            print(f"❌ Ошибка обновления статуса: {e}")
            return False
    
    def get_player_by_telegram_id(self, telegram_id: str) -> Optional[Dict[str, Any]]:
        """Находит игрока по Telegram ID"""
        try:
            all_players = self.get_all_players()
            for player in all_players:
                if player.get('telegram_id') == telegram_id:
                    return player
            return None
            
        except Exception as e:
            print(f"❌ Ошибка поиска игрока: {e}")
            return None

# Глобальный экземпляр менеджера
players_manager = PlayersManager()

def get_years_word(age: int) -> str:
    """Возвращает правильное склонение слова 'год'"""
    if age % 10 == 1 and age % 100 != 11:
        return "год"
    elif age % 10 in [2, 3, 4] and age % 100 not in [12, 13, 14]:
        return "года"
    else:
        return "лет"

def test_players_manager():
    """Тестирует функциональность менеджера игроков"""
    print("🧪 ТЕСТИРОВАНИЕ МЕНЕДЖЕРА ИГРОКОВ")
    print("=" * 50)
    
    # Проверяем подключение
    if not players_manager.players_sheet:
        print("❌ Google Sheets не подключен")
        return
    
    # Получаем всех игроков
    all_players = players_manager.get_all_players()
    print(f"📊 Всего игроков: {len(all_players)}")
    
    # Получаем активных игроков
    active_players = players_manager.get_active_players()
    print(f"✅ Активных игроков: {len(active_players)}")
    
    # Проверяем дни рождения сегодня
    birthday_players = players_manager.get_players_with_birthdays_today()
    print(f"🎂 Дней рождения сегодня: {len(birthday_players)}")
    
    if birthday_players:
        print("🎉 Именинники:")
        for player in birthday_players:
            age = player.get('age', 0)
            years_word = get_years_word(age)
            print(f"   - {player['name']} ({age} {years_word})")
    
    print("✅ Тестирование завершено")

if __name__ == "__main__":
    test_players_manager()
