#!/usr/bin/env python3
"""
Универсальная система защиты от дублирования
Использует лист "Сервисный" в Google таблице для централизованного контроля
"""

import os
import json
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
from datetime_utils import get_moscow_time

SERVICE_HEADER = [
    "ТИП ДАННЫХ",
    "ДАТА И ВРЕМЯ",
    "УНИКАЛЬНЫЙ КЛЮЧ",
    "СТАТУС",
    "ДОПОЛНИТЕЛЬНЫЕ ДАННЫЕ",
    "ССЫЛКА",
    "ИД СОРЕВНОВАНИЯ",
    "ИД КОМАНДЫ",
    "АЛЬТЕРНАТИВНОЕ ИМЯ",
    "НАСТРОЙКИ",
    "GAME ID",
    "GAME DATE",
    "GAME TIME",
    "АРЕНА",
    "TEAM A ID",
    "TEAM B ID",
]

# Индексы колонок (0-based)
TYPE_COL = 0
DATE_COL = 1
KEY_COL = 2
STATUS_COL = 3
ADDITIONAL_DATA_COL = 4
LINK_COL = 5
COMP_ID_COL = 6
TEAM_ID_COL = 7
ALT_NAME_COL = 8
CONFIG_COL = 9
GAME_ID_COL = 10
GAME_DATE_COL = 11
GAME_TIME_COL = 12
ARENA_COL = 13
TEAM_A_ID_COL = 14
TEAM_B_ID_COL = 15

END_COLUMN_LETTER = chr(ord('A') + len(SERVICE_HEADER) - 1)
CONFIG_WORKSHEET_NAME = "Конфиг"
CONFIG_HEADER = [
    "ТИП",
    "ИД (СОРЕВНОВАНИЯ / ГОЛОСОВАНИЯ)",
    "ИД КОМАНДЫ / ПОРЯДОК",
    "АЛЬТЕРНАТИВНОЕ ИМЯ / ТЕКСТ",
    "НАСТРОЙКИ (JSON)",
    "ДНИ НЕДЕЛИ",
    "URL FALLBACK",
    "КОМАНДА ДЛЯ FALLBACK"
]
CONFIG_SECTION_END_MARKERS = {
    "END",
    "END_CONFIG",
    "CONFIG_END",
    "END OF CONFIG",
    "КОНЕЦ",
    "--- END ---",
    "=== END ===",
}
DEFAULT_END_MARKER = "--- END ---"
VOTING_SECTION_END_MARKER = "--- END VOTING ---"
VOTING_SECTION_HEADER = [
    "ID голосования",
    "Тема",
    "Вариант ответа",
    "Дни запуска",
    "Анонимный",
    "Множественный выбор",
    "Время (мин)",
    "Закрыть (дата)",
    "ID топика",
    "Комментарий",
]
VOTING_GUIDE_ROWS = [
    [
        "# Подсказка",
        "",
        "",
        "дни через запятую",
        "Да / Нет",
        "Да / Нет",
        "5–600",
        "ДД.ММ.ГГГГ или ДД.ММ.ГГГГ ЧЧ:ММ",
        "Число или оставить пустым",
        "",
    ]
]
AUTOMATION_SECTION_HEADER = [
    "Автоматическое сообщение",
    "ID топика",
    "ID чата",
    "Анонимный",
    "Множественный выбор",
    "Комментарий",
]
AUTOMATION_SECTION_END_MARKER = "--- END AUTOMATIONS ---"
AUTOMATION_DEFAULT_ROWS = [
    {"key": "BIRTHDAY_NOTIFICATIONS", "name": "Уведомления о днях рождения", "comment": "Поздравления именинников"},
    {"key": "GAME_ANNOUNCEMENTS", "name": "Анонсы игр", "comment": "Сообщения с информацией об игре"},
    {"key": "GAME_POLLS", "name": "Опросы на игры", "comment": "Опрос о готовности на игру"},
    {"key": "GAME_UPDATES", "name": "Уведомления об изменениях", "comment": "Уведомления об изменениях в расписании игр"},
    {"key": "GAME_RESULTS", "name": "Результаты игр", "comment": "Уведомления о результатах завершенных игр"},
    {"key": "CALENDAR_EVENTS", "name": "Календарные события", "comment": "Отправка календарных событий (.ics файлов)"},
]
LEGACY_VOTING_HEADERS = [
    [
        "ТИП (ГОЛОСОВАНИЯ)",
        "ИД голосования",
        "Порядок / вспомогательное значение",
        "Тема или вариант",
        "Доп. настройки (JSON)",
        "Дни недели (через запятую)",
        "URL / резерв",
        "Комментарий / резерв",
    ],
    [
        "ID голосования",
        "Тема",
        "Вариант ответа",
        "Дни запуска",
        "Параметры (JSON)",
        "Комментарий",
    ],
]
AUTOMATION_NAME_TO_KEY = {
    row["name"].lower(): row["key"]
    for row in AUTOMATION_DEFAULT_ROWS
}
AUTOMATION_KEY_TO_NAME = {
    row["key"].upper(): row["name"]
    for row in AUTOMATION_DEFAULT_ROWS
}
LEGACY_AUTOMATION_HEADERS = [
    [
        "Автоматическое сообщение",
        "ID топика",
        "Комментарий",
    ],
]
# Загружаем переменные окружения
load_dotenv()

# Переменные окружения
GOOGLE_SHEETS_CREDENTIALS = os.getenv("GOOGLE_SHEETS_CREDENTIALS")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"  # Тестовый режим

# Настройки Google Sheets
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

MAX_CONFIG_COLUMNS = max(len(CONFIG_HEADER), len(VOTING_SECTION_HEADER))

class EnhancedDuplicateProtection:
    """Универсальная система защиты от дублирования"""
    
    def __init__(self):
        self.gc = None
        self.spreadsheet = None
        self.service_worksheet = None
        self.config_worksheet = None
        self._init_google_sheets()
    
    def _init_google_sheets(self):
        """Инициализация Google Sheets"""
        try:
            if not GOOGLE_SHEETS_CREDENTIALS:
                print("❌ GOOGLE_SHEETS_CREDENTIALS не настроен")
                return
            
            creds_dict = json.loads(GOOGLE_SHEETS_CREDENTIALS)
            creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            
            self.gc = gspread.authorize(creds)
            
            if SPREADSHEET_ID:
                self.spreadsheet = self.gc.open_by_key(SPREADSHEET_ID)
                print("✅ Google Sheets подключен успешно")
                
                # Получаем лист "Сервисный"
                try:
                    self.service_worksheet = self.spreadsheet.worksheet("Сервисный")
                    print("✅ Лист 'Сервисный' подключен")
                    self._ensure_service_header(self.service_worksheet)
                except gspread.WorksheetNotFound:
                    print("❌ Лист 'Сервисный' не найден")
                    print("💡 Запустите create_service_sheet.py для создания листа")

                try:
                    self.config_worksheet = self.spreadsheet.worksheet(CONFIG_WORKSHEET_NAME)
                    print(f"✅ Лист '{CONFIG_WORKSHEET_NAME}' подключен")
                    self._ensure_config_header()
                except gspread.WorksheetNotFound:
                    print(f"⚠️ Лист '{CONFIG_WORKSHEET_NAME}' не найден, создаём его")
                    self.config_worksheet = self.spreadsheet.add_worksheet(title=CONFIG_WORKSHEET_NAME, rows=200, cols=len(CONFIG_HEADER))
                    self._ensure_config_header()
            else:
                print("❌ SPREADSHEET_ID не настроен")
                
        except Exception as e:
            print(f"❌ Ошибка инициализации Google Sheets: {e}")
    
    def _ensure_service_header(self, worksheet) -> None:
        if not worksheet:
            return
        try:
            header = worksheet.row_values(1)
            if not header:
                worksheet.update(f'A1:{END_COLUMN_LETTER}1', [SERVICE_HEADER])
                return
            desired_length = len(SERVICE_HEADER)
            if len(header) < desired_length:
                header.extend([""] * (desired_length - len(header)))
            updated = False
            for index, expected in enumerate(SERVICE_HEADER):
                if not header[index]:
                    header[index] = expected
                    updated = True
            if updated:
                worksheet.update(f'A1:{END_COLUMN_LETTER}1', [header])
        except Exception as e:
            print(f"⚠️ Не удалось обновить заголовок сервисного листа: {e}")

    def _ensure_config_header(self) -> None:
        worksheet = self.config_worksheet
        if not worksheet:
            return
        try:
            header = worksheet.row_values(1)
            if not header:
                worksheet.update(f'A1:{chr(ord("A") + len(CONFIG_HEADER) - 1)}1', [CONFIG_HEADER])
                return
            desired_length = len(CONFIG_HEADER)
            if len(header) < desired_length:
                header.extend([""] * (desired_length - len(header)))
            updated = False
            for index, expected in enumerate(CONFIG_HEADER):
                current_value = header[index] if index < len(header) else ""
                # Обновляем, если ячейка пустая или значение не соответствует ожидаемому
                if not current_value or current_value.strip() != expected.strip():
                    header[index] = expected
                    updated = True
            if updated:
                worksheet.update(f'A1:{chr(ord("A") + len(CONFIG_HEADER) - 1)}1', [header])
                print(f"✅ Заголовки листа '{CONFIG_WORKSHEET_NAME}' обновлены")
            self._ensure_voting_section_structure(worksheet)
        except Exception as e:
            print(f"⚠️ Не удалось обновить заголовок листа '{CONFIG_WORKSHEET_NAME}': {e}")
 
    def _ensure_voting_section_structure(self, worksheet) -> None:
        """Гарантирует наличие раздела для конфигурации голосований"""
        try:
            total_columns = MAX_CONFIG_COLUMNS
            padded_header = VOTING_SECTION_HEADER + [""] * (total_columns - len(VOTING_SECTION_HEADER))

            all_data = worksheet.get_all_values()
            end_row_index: Optional[int] = None
            end_marker_value: Optional[str] = None
            for idx, row in enumerate(all_data, start=1):
                if row and row[0].strip() in CONFIG_SECTION_END_MARKERS:
                    end_row_index = idx
                    end_marker_value = row[0].strip()
                    break

            if end_row_index is None:
                end_row_index = len(all_data) + 1
                worksheet.append_row(
                    [DEFAULT_END_MARKER] + [""] * (total_columns - 1),
                    value_input_option="USER_ENTERED",
                )
                all_data.append([DEFAULT_END_MARKER])
            elif end_marker_value and end_marker_value != DEFAULT_END_MARKER:
                worksheet.update(f"A{end_row_index}", [[DEFAULT_END_MARKER]])
                all_data[end_row_index - 1][0] = DEFAULT_END_MARKER

            # Обновляем данные после возможных изменений
            all_data = worksheet.get_all_values()

            header_row_index: Optional[int] = None
            for idx in range(end_row_index + 1, len(all_data) + 1):
                row = all_data[idx - 1]
                normalized = [cell.strip() for cell in row]
                if not any(normalized):
                    continue
                if normalized[0].upper() == VOTING_SECTION_END_MARKER.upper():
                    break
                legacy_header_detected = any(
                    normalized[:len(legacy)] == [cell.strip() for cell in legacy]
                    for legacy in LEGACY_VOTING_HEADERS
                )
                if legacy_header_detected or normalized[:len(VOTING_SECTION_HEADER)] == VOTING_SECTION_HEADER or any(
                    "Параметры (JSON)" in cell for cell in normalized
                ):
                    header_row_index = idx
                    break

            if header_row_index is None:
                insert_index = end_row_index + 1
                if insert_index - 1 < len(all_data):
                    candidate = all_data[insert_index - 1]
                    if any(cell.strip() for cell in candidate):
                        insert_index += 1
                worksheet.insert_row(
                    padded_header,
                    insert_index,
                    value_input_option="USER_ENTERED",
                )
                header_row_index = insert_index
                all_data = worksheet.get_all_values()
            else:
                worksheet.update(
                    f"A{header_row_index}:{chr(ord('A') + total_columns - 1)}{header_row_index}",
                    [padded_header],
                )
                all_data[header_row_index - 1] = padded_header

            # Проверяем наличие маркера конца блока голосований
            has_voting_end_marker = any(
                row and row[0].strip().upper() == VOTING_SECTION_END_MARKER.upper()
                for row in worksheet.get_all_values()
            )
            if not has_voting_end_marker:
                worksheet.append_row(
                    [VOTING_SECTION_END_MARKER] + [""] * (total_columns - 1),
                    value_input_option="USER_ENTERED",
                )

            guide_exists = False
            for row in worksheet.get_all_values():
                if row and isinstance(row[0], str) and row[0].strip().startswith("# Подсказка"):
                    guide_exists = True
                    break
            if not guide_exists:
                for guide_row in VOTING_GUIDE_ROWS:
                    padded_instruction = guide_row + [""] * (total_columns - len(guide_row))
                    worksheet.append_row(
                        padded_instruction,
                        value_input_option="USER_ENTERED",
                    )
            self._ensure_automation_section_structure(worksheet)
        except Exception as error:
            print(f"⚠️ Не удалось гарантировать структуру раздела голосований: {error}")

    def _ensure_automation_section_structure(self, worksheet) -> None:
        """Гарантирует наличие раздела настроек автоматических сообщений"""
        try:
            total_columns = MAX_CONFIG_COLUMNS
            padded_header = AUTOMATION_SECTION_HEADER + [""] * (total_columns - len(AUTOMATION_SECTION_HEADER))
            all_data = worksheet.get_all_values()

            header_row_index: Optional[int] = None
            for idx, row in enumerate(all_data, start=1):
                normalized = [cell.strip() for cell in row]
                if not any(normalized):
                    continue
                if normalized[:len(AUTOMATION_SECTION_HEADER)] == AUTOMATION_SECTION_HEADER:
                    header_row_index = idx
                    break
                for legacy in LEGACY_AUTOMATION_HEADERS:
                    if normalized[:len(legacy)] == [cell.strip() for cell in legacy]:
                        header_row_index = idx
                        break
                if header_row_index is not None:
                    break

            if header_row_index is None:
                worksheet.append_row(
                    padded_header,
                    value_input_option="USER_ENTERED",
                )
                all_data = worksheet.get_all_values()
                header_row_index = len(all_data)
            else:
                # Всегда обновляем заголовок, чтобы убедиться что он соответствует текущей структуре
                # Проверяем, совпадает ли текущий заголовок с новым
                current_header = [cell.strip() for cell in all_data[header_row_index - 1][:len(AUTOMATION_SECTION_HEADER)]]
                if current_header != AUTOMATION_SECTION_HEADER:
                    # Заголовок устарел, обновляем его
                    worksheet.update(
                        f"A{header_row_index}:{chr(ord('A') + len(AUTOMATION_SECTION_HEADER) - 1)}{header_row_index}",
                        [AUTOMATION_SECTION_HEADER],
                    )
                    all_data = worksheet.get_all_values()
                    all_data[header_row_index - 1] = AUTOMATION_SECTION_HEADER + [""] * (len(all_data[header_row_index - 1]) - len(AUTOMATION_SECTION_HEADER))

            existing_entries: Dict[str, Dict[str, str]] = {}
            for row in all_data[header_row_index:]:
                if not row:
                    continue
                label = (row[0] or "").strip()
                if (
                    not label
                    or label == AUTOMATION_SECTION_HEADER[0]
                    or label.upper() == AUTOMATION_SECTION_END_MARKER.upper()
                    or label.startswith("#")
                    or label.upper() == "КОД"
                ):
                    continue
                mapped_key = AUTOMATION_NAME_TO_KEY.get(label.lower())
                key_upper = mapped_key.upper() if mapped_key else label.upper()
                display_name = AUTOMATION_KEY_TO_NAME.get(key_upper, label)
                topic_value = row[1] if len(row) > 1 else ""
                chat_id_value = row[2] if len(row) > 2 else ""
                anon_value = row[3] if len(row) > 3 else ""
                multiple_value = row[4] if len(row) > 4 else ""
                comment_value = row[5] if len(row) > 5 else ""
                existing_entries[key_upper] = {
                    "label": display_name,
                    "topic": topic_value,
                    "chat_id": chat_id_value,
                    "anon": anon_value,
                    "multiple": multiple_value,
                    "comment": comment_value,
                }

            rows_to_write: List[List[str]] = []
            for default in AUTOMATION_DEFAULT_ROWS:
                key_upper = default["key"].upper()
                existing = existing_entries.pop(key_upper, None)
                label = default["name"]
                topic_value = ""
                chat_id_value = ""
                anon_value = ""
                multiple_value = ""
                comment_value = default.get("comment", "")
                if existing:
                    label = existing.get("label") or label
                    topic_value = existing.get("topic", "")
                    chat_id_value = existing.get("chat_id", "")
                    anon_value = existing.get("anon", "")
                    multiple_value = existing.get("multiple", "")
                    comment_value = existing.get("comment", "") or comment_value
                rows_to_write.append([label, topic_value, chat_id_value, anon_value, multiple_value, comment_value])

            for key_upper, entry in existing_entries.items():
                rows_to_write.append([
                    entry.get("label") or key_upper,
                    entry.get("topic", ""),
                    entry.get("chat_id", ""),
                    entry.get("anon", ""),
                    entry.get("multiple", ""),
                    entry.get("comment", ""),
                ])

            rows_to_write.append([AUTOMATION_SECTION_END_MARKER] + [""] * (len(AUTOMATION_SECTION_HEADER) - 1))

            end_marker_row_index: Optional[int] = None
            for idx in range(header_row_index + 1, len(all_data) + 1):
                row = all_data[idx - 1]
                if row and (row[0] or "").strip().upper() == AUTOMATION_SECTION_END_MARKER.upper():
                    end_marker_row_index = idx

            existing_range_length = 0
            if end_marker_row_index:
                existing_range_length = end_marker_row_index - header_row_index
            else:
                existing_range_length = len(rows_to_write)

            range_length = max(existing_range_length, len(rows_to_write))
            rows_padded: List[List[str]] = []
            for idx in range(range_length):
                if idx < len(rows_to_write):
                    base_row = rows_to_write[idx]
                else:
                    base_row = [""] * len(AUTOMATION_SECTION_HEADER)
                padded = base_row + [""] * (total_columns - len(base_row))
                rows_padded.append(padded)

            worksheet.update(
                f"A{header_row_index + 1}:{chr(ord('A') + total_columns - 1)}{header_row_index + range_length}",
                rows_padded,
                value_input_option="USER_ENTERED",
            )
        except Exception as error:
            print(f"⚠️ Не удалось гарантировать структуру раздела автоматических сообщений: {error}")

    @staticmethod
    def _normalize_cell_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        return str(value).strip()

    @staticmethod
    def _try_parse_int(value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_weekday_value(value: Any) -> Optional[int]:
        text = EnhancedDuplicateProtection._normalize_cell_text(value).lower()
        if not text:
            return None
        mapping = {
            "0": 0,
            "1": 1,
            "2": 2,
            "3": 3,
            "4": 4,
            "5": 5,
            "6": 6,
            "mon": 0,
            "tue": 1,
            "wed": 2,
            "thu": 3,
            "fri": 4,
            "sat": 5,
            "sun": 6,
            "monday": 0,
            "tuesday": 1,
            "wednesday": 2,
            "thursday": 3,
            "friday": 4,
            "saturday": 5,
            "sunday": 6,
            "понедельник": 0,
            "вторник": 1,
            "среда": 2,
            "четверг": 3,
            "пятница": 4,
            "суббота": 5,
            "воскресенье": 6,
            "пн": 0,
            "вт": 1,
            "ср": 2,
            "чт": 3,
            "пт": 4,
            "сб": 5,
            "вс": 6,
        }
        return mapping.get(text)

    @staticmethod
    def _parse_bool_value(value: Any) -> Optional[bool]:
        text = EnhancedDuplicateProtection._normalize_cell_text(value).lower()
        if not text:
            return None
        truthy = {"true", "1", "yes", "y", "да", "д", "истина", "+", "on"}
        falsy = {"false", "0", "no", "n", "нет", "н", "ложь", "-", "off"}
        if text in truthy:
            return True
        if text in falsy:
            return False
        return None

    def _get_service_worksheet(self, raw: bool = False):
        """Получает лист 'Сервисный'"""
        if not self.spreadsheet:
            print("❌ Google Sheets не инициализирован")
            return None
            
        if not self.service_worksheet:
            try:
                self.service_worksheet = self.spreadsheet.worksheet("Сервисный")
            except gspread.WorksheetNotFound:
                print("❌ Лист 'Сервисный' не найден")
                return None
        
        if not raw:
            self._ensure_service_header(self.service_worksheet)
        return self.service_worksheet

    def _create_unique_key(self, data_type: str, identifier: str, **kwargs) -> str:
        """Создает уникальный ключ для записи"""
        # Базовый ключ
        base_key = f"{data_type}_{identifier}"
        
        # Добавляем префикс TEST_ в тестовом режиме
        if TEST_MODE:
            base_key = f"TEST_{base_key}"
        
        # Добавляем дополнительные параметры для уникальности
        if kwargs:
            additional = "_".join([f"{k}_{v}" for k, v in sorted(kwargs.items())])
            base_key = f"{base_key}_{additional}"
        
        return base_key
    
    def _get_current_datetime(self) -> str:
        """Получает текущую дату и время в московском часовом поясе"""
        now = get_moscow_time()
        return now.strftime('%d.%m.%Y %H:%M')
    
    def check_duplicate(self, data_type: str, identifier: str, **kwargs) -> Dict[str, Any]:
        """Проверяет существование дубликата с обработкой ошибок 429"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {'exists': False, 'error': 'Лист не найден'}
        
        def _check():
            # Создаем уникальный ключ
            unique_key = self._create_unique_key(data_type, identifier, **kwargs)
            
            # Получаем все данные
            all_data = worksheet.get_all_values()
            
            # Ищем дубликат по уникальному ключу (колонка C) И по типу данных (колонка A)
            for i, row in enumerate(all_data):
                if (len(row) >= 3 and 
                    row[0].upper() == data_type.upper() and 
                    row[2] == unique_key):
                    return {
                        'exists': True,
                        'row': i + 1,
                        'data': row,
                        'unique_key': unique_key
                    }
            
            # Дополнительная проверка: ищем по типу и идентификатору
            for i, row in enumerate(all_data):
                if (len(row) >= 3 and 
                    row[0].upper() == data_type.upper() and 
                    identifier in row[2]):
                    return {
                        'exists': True,
                        'row': i + 1,
                        'data': row,
                        'unique_key': row[2],
                        'reason': 'Найден по типу и идентификатору'
                    }
            
            return {'exists': False, 'unique_key': unique_key}
        
        try:
            return self._retry_with_backoff(_check)
        except Exception as e:
            print(f"⚠️ Ошибка проверки дубликата: {e}")
            return {'exists': False, 'error': str(e)}
    
    def add_record(
        self,
        data_type: str,
        identifier: str,
        status: str = "АКТИВЕН",
        additional_data: str = "",
        game_link: str = "",
        comp_id: Optional[int] = None,
        team_id: Optional[int] = None,
        alt_name: str = "",
        settings: str = "",
        game_id: Optional[int] = None,
        game_date: str = "",
        game_time: str = "",
        arena: str = "",
        team_a_id: Optional[int] = None,
        team_b_id: Optional[int] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Добавляет новую запись в сервисный лист"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {'success': False, 'error': 'Лист не найден'}
        
        try:
            # Проверяем дубликат
            duplicate_check = self.check_duplicate(data_type, identifier, **kwargs)
            
            if duplicate_check.get('exists'):
                return {
                    'success': False,
                    'error': 'Дубликат уже существует',
                    'duplicate_info': duplicate_check
                }
            
            # Создаем уникальный ключ
            unique_key = duplicate_check.get('unique_key') or self._create_unique_key(data_type, identifier, **kwargs)
            
            # Получаем текущую дату
            current_datetime = self._get_current_datetime()
            
            # Создаем новую запись
            new_record = [
                data_type.upper(),
                current_datetime,
                unique_key,
                status,
                additional_data,
                game_link,
                str(comp_id) if comp_id is not None else "",
                str(team_id) if team_id is not None else "",
                alt_name,
                settings,
                str(game_id) if game_id is not None else "",
                game_date,
                game_time,
                arena,
                str(team_a_id) if team_a_id is not None else "",
                str(team_b_id) if team_b_id is not None else "",
            ]
            
            if len(new_record) < len(SERVICE_HEADER):
                new_record.extend([""] * (len(SERVICE_HEADER) - len(new_record)))
            
            # Добавляем запись в начало (под заголовком)
            worksheet.insert_row(new_record, index=2)
            
            print(f"✅ Запись добавлена: {data_type} - {identifier}")
            
            return {
                'success': True,
                'unique_key': unique_key,
                'row': 2
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def find_game_link_for_today(self, team1: str, team2: str) -> Optional[str]:
        """Ищет ссылку на игру для сегодняшней даты"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            print("❌ Лист 'Сервисный' не найден")
            return None
        
        try:
            from datetime_utils import get_moscow_time
            today = get_moscow_time().strftime('%d.%m.%Y')
            
            # Получаем все данные
            all_data = worksheet.get_all_values()
            
            print(f"🔍 Ищем ссылку на игру для {today}: {team1} vs {team2}")
            
            # Ищем записи типа АНОНС_ИГРА за сегодня
            for row in all_data:
                if (len(row) > LINK_COL and 
                    row[TYPE_COL] == "АНОНС_ИГРА" and 
                    today in row[DATE_COL] and  # Дата в колонке B
                    row[LINK_COL]):  # Ссылка в колонке F
                    
                    # Более точный поиск команд
                    unique_key = row[2].lower()
                    team1_lower = team1.lower()
                    team2_lower = team2.lower()
                    
                    # Нормализуем названия команд для сравнения
                    def _normalize_team_name(name: str) -> str:
                        import re as _re
                        return _re.sub(r"[\W_]+", "", name.lower())

                    def _build_variants(name: str) -> Set[str]:
                        variants: Set[str] = set()
                        if not name:
                            return variants
                        lowered = name.lower()
                        variants.add(lowered)
                        variants.add(_normalize_team_name(name))
                        for part in lowered.replace('-', ' ').replace('_', ' ').split():
                            if len(part) > 2:
                                variants.add(part)
                        return {variant for variant in variants if variant}

                    team1_variants = _build_variants(team1)
                    team2_variants = _build_variants(team2)
                    unique_key_lower = unique_key.lower()
                    unique_key_normalized = _normalize_team_name(unique_key)

                    def _contains_variant(variants: Set[str]) -> bool:
                        for variant in variants:
                            if len(variant) <= 2:
                                continue
                            if variant in unique_key_lower or variant in unique_key_normalized:
                                return True
                        return False

                    team1_found = _contains_variant(team1_variants)
                    team2_found = _contains_variant(team2_variants)

                    # Если найдены обе команды — возвращаем ссылку
                    if team1_found and team2_found:
                        game_link = row[LINK_COL]
                        print(f"✅ Найдена точная ссылка в сервисном листе: {game_link}")
                        print(f"   По ключу: {row[2]}")
                        print(f"   Для команд: {team1} vs {team2}")
                        return game_link
            
            print(f"❌ Ссылка на игру не найдена в сервисном листе")
            return None
            
        except Exception as e:
            print(f"❌ Ошибка поиска ссылки в сервисном листе: {e}")
            return None
    
    def update_record_status(self, unique_key: str, new_status: str) -> Dict[str, Any]:
        """Обновляет статус существующей записи"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {'success': False, 'error': 'Лист не найден'}
        
        try:
            # Получаем все данные
            all_data = worksheet.get_all_values()
            
            # Ищем запись по уникальному ключу
            for i, row in enumerate(all_data):
                if len(row) >= 3 and row[2] == unique_key:
                    # Обновляем статус (колонка D)
                    worksheet.update(values=[[new_status]], range_name=f'D{i+1}')
                    
                    print(f"✅ Статус обновлен: {unique_key} -> {new_status}")
                    
                    return {
                        'success': True,
                        'row': i + 1,
                        'old_status': row[3] if len(row) > 3 else '',
                        'new_status': new_status
                    }
            
            return {'success': False, 'error': 'Запись не найдена'}
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_records_by_type(self, data_type: str) -> List[Dict[str, Any]]:
        """Получает все записи определенного типа"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return []
        
        try:
            all_data = worksheet.get_all_values()
            records = []
            
            for i, row in enumerate(all_data):
                if len(row) >= 1 and row[0].upper() == data_type.upper():
                    records.append({
                        'row': i + 1,
                        'type': row[TYPE_COL] if len(row) > TYPE_COL else '',
                        'date': row[DATE_COL] if len(row) > DATE_COL else '',
                        'unique_key': row[KEY_COL] if len(row) > KEY_COL else '',
                        'status': row[STATUS_COL] if len(row) > STATUS_COL else '',
                        'additional_data': row[ADDITIONAL_DATA_COL] if len(row) > ADDITIONAL_DATA_COL else '',
                        'link': row[LINK_COL] if len(row) > LINK_COL else '',
                        'comp_id': row[COMP_ID_COL] if len(row) > COMP_ID_COL else '',
                        'team_id': row[TEAM_ID_COL] if len(row) > TEAM_ID_COL else '',
                        'alt_name': row[ALT_NAME_COL] if len(row) > ALT_NAME_COL else '',
                        'settings': row[CONFIG_COL] if len(row) > CONFIG_COL else '',
                        'game_id': row[GAME_ID_COL] if len(row) > GAME_ID_COL else '',
                        'game_date': row[GAME_DATE_COL] if len(row) > GAME_DATE_COL else '',
                        'game_time': row[GAME_TIME_COL] if len(row) > GAME_TIME_COL else '',
                        'arena': row[ARENA_COL] if len(row) > ARENA_COL else '',
                        'team_a_id': row[TEAM_A_ID_COL] if len(row) > TEAM_A_ID_COL else '',
                        'team_b_id': row[TEAM_B_ID_COL] if len(row) > TEAM_B_ID_COL else ''
                    })
            
            return records
            
        except Exception as e:
            print(f"❌ Ошибка получения записей: {e}")
            return []
    
    def _retry_with_backoff(self, func, max_retries: int = 3, base_delay: float = 2.0):
        """Повторяет вызов функции с экспоненциальной задержкой при ошибках 429"""
        import time
        import gspread.exceptions
        
        for attempt in range(max_retries):
            try:
                return func()
            except gspread.exceptions.APIError as e:
                error_code = e.response.status_code if hasattr(e, 'response') else None
                error_message = str(e)
                
                # Проверяем, является ли это ошибкой 429 (Quota exceeded)
                if error_code == 429 or '429' in error_message or 'Quota exceeded' in error_message:
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)  # Экспоненциальная задержка: 2, 4, 8 секунд
                        print(f"⚠️ Quota exceeded (429), повтор через {delay:.1f} сек (попытка {attempt + 1}/{max_retries})")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"❌ Quota exceeded после {max_retries} попыток")
                        raise
                else:
                    # Другие ошибки API не повторяем
                    raise
            except Exception as e:
                # Другие исключения не повторяем
                raise
        
        return None
    
    def get_game_record(self, data_type: str, game_id: Any) -> Optional[Dict[str, Any]]:
        """Возвращает запись об игре по GameID с обработкой ошибок 429"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return None
        
        def _fetch_record():
            game_id_str = str(game_id)
            all_data = worksheet.get_all_values()
            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) <= max(GAME_ID_COL, TYPE_COL):
                    continue
                if row[TYPE_COL].upper() != data_type.upper():
                    continue
                if row[GAME_ID_COL] == game_id_str:
                    return {
                        'row': row_index,
                        'type': row[TYPE_COL],
                        'date': row[DATE_COL],
                        'unique_key': row[KEY_COL],
                        'status': row[STATUS_COL],
                        'additional_data': row[ADDITIONAL_DATA_COL],
                        'link': row[LINK_COL],
                        'comp_id': row[COMP_ID_COL],
                        'team_id': row[TEAM_ID_COL],
                        'alt_name': row[ALT_NAME_COL],
                        'settings': row[CONFIG_COL],
                        'game_id': row[GAME_ID_COL],
                        'game_date': row[GAME_DATE_COL],
                        'game_time': row[GAME_TIME_COL],
                        'arena': row[ARENA_COL],
                        'team_a_id': row[TEAM_A_ID_COL],
                        'team_b_id': row[TEAM_B_ID_COL],
                    }
            return None
        
        try:
            return self._retry_with_backoff(_fetch_record)
        except Exception as e:
            print(f"⚠️ Ошибка поиска записи игры: {e}")
            return None
    
    def upsert_game_record(
        self,
        data_type: str,
        identifier: str,
        status: str,
        additional_data: str,
        game_link: str,
        comp_id: Optional[int],
        team_id: Optional[int],
        alt_name: str,
        settings: str,
        game_id: Any,
        game_date: str,
        game_time: str,
        arena: str,
        team_a_id: Optional[int],
        team_b_id: Optional[int],
        **kwargs,
    ) -> Dict[str, Any]:
        """Создает или обновляет запись об игре"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {'success': False, 'error': 'Лист не найден'}
        
        try:
            game_id_str = str(game_id) if game_id is not None else ""
            existing = self.get_game_record(data_type, game_id_str) if game_id_str else None
            unique_key = existing.get('unique_key') if existing else self._create_unique_key(data_type, identifier, **kwargs)
            current_datetime = self._get_current_datetime()
            
            row_values = [
                data_type.upper(),
                current_datetime,
                unique_key,
                status,
                additional_data,
                game_link,
                str(comp_id) if comp_id is not None else "",
                str(team_id) if team_id is not None else "",
                alt_name,
                settings,
                game_id_str,
                game_date,
                game_time,
                arena,
                str(team_a_id) if team_a_id is not None else "",
                str(team_b_id) if team_b_id is not None else "",
            ]
            
            if existing:
                row_index = existing['row']
                worksheet.update(f"A{row_index}:{END_COLUMN_LETTER}{row_index}", [row_values])
                print(f"🔄 Обновлена запись {data_type} для GameID {game_id_str}")
                return {'success': True, 'action': 'updated', 'row': row_index}
            
            result = self.add_record(
                data_type=data_type,
                identifier=identifier,
                status=status,
                additional_data=additional_data,
                game_link=game_link,
                comp_id=comp_id,
                team_id=team_id,
                alt_name=alt_name,
                settings=settings,
                game_id=game_id,
                game_date=game_date,
                game_time=game_time,
                arena=arena,
                team_a_id=team_a_id,
                team_b_id=team_b_id,
                **kwargs,
            )
            result['action'] = 'inserted' if result.get('success') else 'error'
            return result
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_active_records(self, data_type: str) -> List[Dict[str, Any]]:
        """Получает активные записи определенного типа"""
        all_records = self.get_records_by_type(data_type)
        return [record for record in all_records if record.get('status') == 'АКТИВЕН']
    
    def cleanup_old_records(self, data_type: str, days_old: int = 30) -> Dict[str, Any]:
        """Очищает старые записи определенного типа"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {'success': False, 'error': 'Лист не найден'}
        
        try:
            all_data = worksheet.get_all_values()
            current_datetime = get_moscow_time()
            rows_to_delete: List[int] = []
            
            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) <= max(DATE_COL, TYPE_COL):
                    continue
                
                row_type = row[TYPE_COL].upper() if len(row) > TYPE_COL else ''
                if row_type != data_type.upper():
                    continue
                
                date_value = row[DATE_COL]
                if not date_value:
                    continue
                
                try:
                    from datetime import datetime as dt
                    record_date = dt.strptime(date_value, '%d.%m.%Y %H:%M')
                except ValueError:
                    continue
                
                record_date = record_date.replace(tzinfo=current_datetime.tzinfo)
                age_days = (current_datetime - record_date).days
                
                if age_days > days_old:
                    rows_to_delete.append(row_index)
            
            for row_index in reversed(rows_to_delete):
                worksheet.delete_rows(row_index)
            
            print(f"✅ Очищено {len(rows_to_delete)} старых записей типа {data_type}")
            
            return {
                'success': True,
                'cleaned_count': len(rows_to_delete),
                'data_type': data_type
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получает статистику по всем типам записей"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {'error': 'Лист не найден'}
        
        try:
            all_data = worksheet.get_all_values()
            stats = {}
            
            for row in all_data:
                if len(row) >= 1 and row[0]:
                    data_type = row[0]
                    if data_type.startswith('===') or data_type.startswith('ТИП ДАННЫХ'):
                        continue
                    
                    if data_type not in stats:
                        stats[data_type] = {'total': 0, 'active': 0, 'completed': 0}
                    
                    stats[data_type]['total'] += 1
                    
                    if len(row) >= 4:
                        status = row[3]
                        if status == 'АКТИВЕН':
                            stats[data_type]['active'] += 1
                        elif status in ['ЗАВЕРШЕН', 'ОТПРАВЛЕН', 'ОБРАБОТАН', 'ОТПРАВЛЕНО']:
                            stats[data_type]['completed'] += 1
            
            return stats
            
        except Exception as e:
            return {'error': str(e)}

    def cleanup_expired_records(self, max_age_days: int = 30) -> Dict[str, Any]:
        """Удаляет все записи старше указанного количества дней"""
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {'success': False, 'error': 'Лист не найден'}
        
        try:
            all_data = worksheet.get_all_values()
            if not all_data:
                return {'success': True, 'cleaned_count': 0, 'details': []}
            
            current_datetime = get_moscow_time()
            rows_to_delete: List[Tuple[int, str]] = []
            
            for row_index, row in enumerate(all_data[1:], start=2):
                if len(row) <= DATE_COL:
                    continue
                
                date_value = row[DATE_COL]
                if not date_value:
                    continue
                
                try:
                    from datetime import datetime as dt
                    record_date = dt.strptime(date_value, '%d.%m.%Y %H:%M')
                except ValueError:
                    continue
                
                record_date = record_date.replace(tzinfo=current_datetime.tzinfo)
                age_days = (current_datetime - record_date).days
                
                if age_days > max_age_days:
                    record_type = row[TYPE_COL] if len(row) > TYPE_COL else ''
                    rows_to_delete.append((row_index, record_type))
            
            for row_index, _ in reversed(rows_to_delete):
                worksheet.delete_rows(row_index)
            
            print(f"✅ Очищено {len(rows_to_delete)} записей старше {max_age_days} дней")
            
            return {
                'success': True,
                'cleaned_count': len(rows_to_delete),
                'details': rows_to_delete
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    @staticmethod
    def _parse_ids(cell_value: str) -> List[int]:
        """Парсит числовые ID из значения ячейки"""
        if not cell_value:
            return []
        
        normalized = cell_value.replace('\n', ',').replace(';', ',')
        parts = [part.strip() for part in normalized.split(',') if part.strip()]
        ids: List[int] = []
        for part in parts:
            matches = re.findall(r'\d+', part)
            for match in matches:
                try:
                    ids.append(int(match))
                except ValueError:
                    continue
        return ids
    
    @staticmethod
    def _parse_json_config(cell_value: str) -> Dict[str, Any]:
        """Парсит JSON из ячейки конфигурации"""
        if not cell_value:
            return {}
        if isinstance(cell_value, dict):
            return cell_value
        try:
            return json.loads(cell_value)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"⚠️ Некорректный JSON в конфигурации сервисного листа: {e}")
            return {}

    def get_full_config(self) -> Dict[str, Any]:
        config_data = self._read_config_from_config_sheet()
        if config_data.get('has_data'):
            return config_data['payload']

        print(f"⚠️ Лист '{CONFIG_WORKSHEET_NAME}' пуст — читаем настройки из 'Сервисного' (временный режим)")
        return self._read_config_from_service_sheet()

    def _read_config_from_config_sheet(self) -> Dict[str, Any]:
        worksheet = self.config_worksheet
        payload = {
            'comp_ids': set(),
            'team_ids': set(),
            'teams': {},
            'training_polls': [],
            'fallback_sources': [],
            'voting_polls': [],
            'automation_topics': {},
        }
        if not worksheet:
            return {'has_data': False, 'payload': payload}

        try:
            all_data = worksheet.get_all_values()
            if not all_data or len(all_data) <= 1:
                return {'has_data': False, 'payload': payload}

            comp_ids_set: Set[int] = set()
            team_ids_set: Set[int] = set()
            teams: Dict[int, Dict[str, Any]] = {}
            training_polls: List[Dict[str, Any]] = []
            fallback_sources: List[Dict[str, Any]] = []
            voting_entries: Dict[str, Dict[str, Any]] = {}
            automation_topics: Dict[str, Dict[str, Any]] = {}
            found_end_marker = False

            required_len = len(CONFIG_HEADER)

            for row in all_data[1:]:
                if not row or len(row) < 1:
                    continue

                row_extended = list(row)
                if len(row_extended) < required_len:
                    row_extended.extend([""] * (required_len - len(row_extended)))

                row_type = self._normalize_cell_text(row_extended[0]) if row_extended else ""
                normalized_type = row_type.upper()

                if not found_end_marker and normalized_type in CONFIG_SECTION_END_MARKERS:
                    found_end_marker = True
                    continue

                if found_end_marker and normalized_type == VOTING_SECTION_END_MARKER:
                    break

                if not found_end_marker:
                    comp_id_cell = row_extended[1]
                    team_id_cell = row_extended[2]
                    alt_name = self._normalize_cell_text(row_extended[3])
                    settings_json_cell = row_extended[4]
                    weekday_cell = row_extended[5]
                    fallback_url = row_extended[6] if len(row_extended) > 6 else ""
                    fallback_name = row_extended[7] if len(row_extended) > 7 else ""

                    row_comp_ids = self._parse_ids(comp_id_cell)
                    row_team_ids = self._parse_ids(team_id_cell)
                    config_payload = self._parse_json_config(settings_json_cell)

                    # Определяем fallback конфигурацию по наличию URL (независимо от типа)
                    # Это позволяет использовать fallback без указания типа FALLBACK в колонке ТИП
                    if fallback_url.strip() and fallback_url.strip().startswith(('http://', 'https://')):
                        fallback_entry = {
                            "name": fallback_name.strip() or alt_name,
                            "url": fallback_url.strip(),
                            "metadata": config_payload or {},
                        }
                        # Добавляем comp_ids и team_ids в metadata для fallback
                        if row_comp_ids:
                            fallback_entry["metadata"]["comp_ids"] = row_comp_ids
                        if row_team_ids:
                            fallback_entry["metadata"]["team_ids"] = row_team_ids
                        fallback_sources.append(fallback_entry)
                        # Продолжаем обработку, чтобы также добавить команды в teams, если есть team_ids
                    
                    if not normalized_type:
                        if row_team_ids:
                            normalized_type = "CONFIG_TEAM"
                        elif row_comp_ids:
                            normalized_type = "CONFIG_COMP"

                    if normalized_type in {"CONFIG_COMP", "COMP_CONFIG"}:
                        comp_ids_set.update(row_comp_ids)
                    elif normalized_type in {"CONFIG_TEAM", "TEAM_CONFIG"}:
                        comp_ids_set.update(row_comp_ids)
                        for team_id in row_team_ids:
                            team_ids_set.add(team_id)
                            team_entry = teams.setdefault(
                                team_id,
                                {"alt_name": None, "comp_ids": set(), "metadata": {}},
                            )
                            if alt_name:
                                team_entry["alt_name"] = alt_name
                            if row_comp_ids:
                                team_entry["comp_ids"].update(row_comp_ids)
                            if config_payload:
                                team_entry["metadata"].update(config_payload)
                    elif normalized_type in {"TRAINING_POLL", "TRAINING_CONFIG"}:
                        training_entry = {
                            "title": config_payload.get("title") or alt_name,
                            "weekday": config_payload.get("weekday"),
                            "time": config_payload.get("time"),
                            "location": config_payload.get("location"),
                            "topic_id": config_payload.get("topic_id"),
                            "metadata": config_payload,
                        }
                        training_polls.append(training_entry)
                    elif normalized_type in {"FALLBACK", "FALLBACK_SOURCE", "FALLBACK_CONFIG"}:
                        # Дополнительная проверка для явного типа FALLBACK (на случай, если URL не был распознан выше)
                        if not (fallback_url.strip() and fallback_url.strip().startswith(('http://', 'https://'))):
                            fallback_entry = {
                                "name": fallback_name.strip() or alt_name,
                                "url": fallback_url.strip(),
                                "metadata": config_payload or {},
                            }
                            if row_comp_ids:
                                fallback_entry["metadata"]["comp_ids"] = row_comp_ids
                            if row_team_ids:
                                fallback_entry["metadata"]["team_ids"] = row_team_ids
                            if fallback_entry["url"] or fallback_entry["name"]:
                                fallback_sources.append(fallback_entry)
                    else:
                        # Unknown types before the separator are ignored to keep backward compatibility
                        continue
                    continue

                # Everything below this point belongs to the voting configuration section
                poll_id_cell = row_extended[0]
                topic_cell = self._normalize_cell_text(row_extended[1])
                option_cell = self._normalize_cell_text(row_extended[2])
                weekday_cell = row_extended[3]
                if len(row_extended) < len(VOTING_SECTION_HEADER):
                    row_extended.extend([""] * (len(VOTING_SECTION_HEADER) - len(row_extended)))
                anon_cell = row_extended[4]
                multiple_cell = row_extended[5]
                open_period_cell = row_extended[6]
                close_date_cell = row_extended[7]
                topic_id_cell = self._normalize_cell_text(row_extended[8]) if len(row_extended) > 8 else ""
                comment_cell = self._normalize_cell_text(row_extended[9]) if len(row_extended) > 9 else ""
                header_candidate = [cell.strip() for cell in row_extended[:len(VOTING_SECTION_HEADER)]]
                if header_candidate == VOTING_SECTION_HEADER or any(
                    header_candidate[:len(legacy)] == [value.strip() for value in legacy[:len(header_candidate)]]
                    for legacy in LEGACY_VOTING_HEADERS
                ):
                    continue
                if normalized_type == VOTING_SECTION_END_MARKER:
                    continue

                poll_id = self._normalize_cell_text(poll_id_cell)
                if not poll_id:
                    continue

                entry = voting_entries.setdefault(
                    poll_id,
                    {
                        "poll_id": poll_id,
                        "topic_template": "",
                        "options_raw": [],
                        "weekdays": set(),
                        "metadata": {},
                        "comments": [],
                        "topic_id_value": "",
                    },
                )

                topic_value = topic_cell
                option_text = option_cell
                weekday_value = weekday_cell
                comment_value = comment_cell
                anon_value = self._parse_bool_value(anon_cell)
                multiple_value = self._parse_bool_value(multiple_cell)
                open_period_value = self._try_parse_int(open_period_cell)
                close_date_value = self._normalize_cell_text(close_date_cell)
                topic_id_value = topic_id_cell

                if topic_value:
                    entry["topic_template"] = topic_value

                if option_text:
                    entry["options_raw"].append(
                        {
                            "text": option_text,
                            "sequence": len(entry["options_raw"]),
                            "comment": comment_value,
                        }
                    )

                if weekday_value:
                    for part in re.split(r"[,\n;/]+", str(weekday_value)):
                        weekday = self._parse_weekday_value(part)
                        if weekday is not None:
                            entry["weekdays"].add(weekday)

                if comment_value:
                    entry["comments"].append(comment_value)
                if anon_value is not None:
                    entry["metadata"]["is_anonymous"] = anon_value
                if multiple_value is not None:
                    entry["metadata"]["allows_multiple_answers"] = multiple_value
                if open_period_value is not None:
                    entry["metadata"]["open_period_minutes"] = open_period_value
                if close_date_value:
                    entry["metadata"]["close_date"] = close_date_value
                if topic_id_value:
                    entry["topic_id_value"] = topic_id_value

            for team in teams.values():
                if isinstance(team.get("comp_ids"), set):
                    team["comp_ids"] = sorted(team["comp_ids"])
                if not team.get("metadata"):
                    team.pop("metadata", None)
                if not team.get("alt_name"):
                    team.pop("alt_name", None)

            voting_polls: List[Dict[str, Any]] = []
            for poll_id, data in voting_entries.items():
                options_raw = data.pop("options_raw", [])
                if options_raw:
                    options_sorted = sorted(options_raw, key=lambda item: item["sequence"])
                    data["options"] = [
                        {
                            "text": option["text"],
                            "comment": option["comment"],
                        }
                        for option in options_sorted
                    ]
                else:
                    data["options"] = []
                weekdays = data.get("weekdays", set())
                data["weekdays"] = sorted(weekdays) if isinstance(weekdays, set) else weekdays
                topic_id_value = data.pop("topic_id_value", "")
                if topic_id_value:
                    topic_id_parsed = self._try_parse_int(topic_id_value)
                    if topic_id_parsed is not None:
                        data["topic_id"] = topic_id_parsed
                    else:
                        data["topic_raw"] = topic_id_value
                if not data.get("comments"):
                    data.pop("comments", None)
                voting_polls.append(data)

            automation_header_index: Optional[int] = None
            for idx, row in enumerate(all_data):
                candidate = [cell.strip() for cell in row[:len(AUTOMATION_SECTION_HEADER)]]
                if candidate == AUTOMATION_SECTION_HEADER:
                    automation_header_index = idx
                    break
                for legacy in LEGACY_AUTOMATION_HEADERS:
                    if candidate[:len(legacy)] == [cell.strip() for cell in legacy]:
                        automation_header_index = idx
                        break
                if automation_header_index is not None:
                    break
            if automation_header_index is not None:
                for row in all_data[automation_header_index + 1:]:
                    if not row or len(row) == 0:
                        continue
                    raw_label = self._normalize_cell_text(row[0])
                    if not raw_label:
                        continue
                    if raw_label.upper() == AUTOMATION_SECTION_END_MARKER:
                        break
                    topic_raw = self._normalize_cell_text(row[1]) if len(row) > 1 else ""
                    chat_id_raw = self._normalize_cell_text(row[2]) if len(row) > 2 else ""
                    anon_raw = row[3] if len(row) > 3 else ""
                    multiple_raw = row[4] if len(row) > 4 else ""
                    comment_raw = self._normalize_cell_text(row[5]) if len(row) > 5 else ""
                    mapped_key = AUTOMATION_NAME_TO_KEY.get(raw_label.lower())
                    key_upper = mapped_key.upper() if mapped_key else raw_label.upper()
                    entry: Dict[str, Any] = {}
                    display_name = AUTOMATION_KEY_TO_NAME.get(key_upper, raw_label)
                    if display_name:
                        entry["name"] = display_name
                    topic_id_value = self._try_parse_int(topic_raw)
                    if topic_id_value is not None:
                        entry["topic_id"] = topic_id_value
                    elif topic_raw:
                        entry["topic_raw"] = topic_raw
                    # Сохраняем chat_id из таблицы (может быть пустым, одним ID или несколькими через запятую)
                    if chat_id_raw:
                        entry["chat_id"] = chat_id_raw
                    anon_value = self._parse_bool_value(anon_raw)
                    if anon_value is not None:
                        entry["is_anonymous"] = anon_value
                    multiple_value = self._parse_bool_value(multiple_raw)
                    if multiple_value is not None:
                        entry["allows_multiple_answers"] = multiple_value
                    if comment_raw:
                        entry["comment"] = comment_raw
                    automation_topics[key_upper] = entry

            has_data = bool(
                comp_ids_set
                or team_ids_set
                or teams
                or training_polls
                or fallback_sources
                or voting_polls
                or automation_topics
            )
            payload.update({
                'comp_ids': sorted(comp_ids_set),
                'team_ids': sorted(team_ids_set),
                'teams': teams,
                'training_polls': training_polls,
                'fallback_sources': fallback_sources,
                'voting_polls': sorted(voting_polls, key=lambda item: item.get("poll_id") or ""),
                'automation_topics': automation_topics,
            })
            return {'has_data': has_data, 'payload': payload}
        except Exception as e:
            print(f"⚠️ Ошибка чтения конфигурации из листа '{CONFIG_WORKSHEET_NAME}': {e}")
            return {'has_data': False, 'payload': payload}

    def _read_config_from_service_sheet(self) -> Dict[str, Any]:
        worksheet = self._get_service_worksheet()
        if not worksheet:
            return {
                'comp_ids': set(),
                'team_ids': set(),
                'teams': {},
                'training_polls': [],
                'fallback_sources': [],
                'voting_polls': []
            }

        try:
            all_data = worksheet.get_all_values()
            if not all_data:
                return {
                    'comp_ids': set(),
                    'team_ids': set(),
                    'teams': {},
                    'training_polls': [],
                    'fallback_sources': [],
                    'voting_polls': []
                }

            comp_ids_set: Set[int] = set()
            team_ids_set: Set[int] = set()
            teams: Dict[int, Dict[str, Any]] = {}
            training_polls: List[Dict[str, Any]] = []
            fallback_sources: List[Dict[str, Any]] = []

            for row in all_data[1:]:
                if len(row) <= TYPE_COL:
                    continue

                row_type = (row[TYPE_COL] or "").strip().upper()
                if not row_type:
                    continue

                row_comp_ids = self._parse_ids(row[COMP_ID_COL]) if len(row) > COMP_ID_COL else []
                row_team_ids = self._parse_ids(row[TEAM_ID_COL]) if len(row) > TEAM_ID_COL else []
                alt_name = (row[ALT_NAME_COL] or "").strip() if len(row) > ALT_NAME_COL else ""
                config_payload = self._parse_json_config(row[CONFIG_COL] if len(row) > CONFIG_COL else "")

                if row_type in {"CONFIG", "CONFIG_IDS", "CONFIG_ROW", "CONFIG_COMP", "COMP_CONFIG"}:
                    comp_ids_set.update(row_comp_ids)

                if row_type in {"CONFIG", "CONFIG_IDS", "CONFIG_ROW", "CONFIG_TEAM", "TEAM_CONFIG"}:
                    comp_ids_set.update(row_comp_ids)
                    for team_id in row_team_ids:
                        team_ids_set.add(team_id)
                        team_entry = teams.setdefault(team_id, {"alt_name": None, "comp_ids": set(), "metadata": {}})
                        if alt_name:
                            team_entry["alt_name"] = alt_name
                        if row_comp_ids:
                            team_entry["comp_ids"].update(row_comp_ids)
                        if config_payload:
                            team_entry["metadata"].update(config_payload)

                elif row_type in {"TRAINING_POLL", "TRAINING_CONFIG"}:
                    training_entry = {
                        "title": config_payload.get("title") or (row[ADDITIONAL_DATA_COL] if len(row) > ADDITIONAL_DATA_COL else ""),
                        "weekday": config_payload.get("weekday"),
                        "time": config_payload.get("time") or (row[STATUS_COL] if len(row) > STATUS_COL else ""),
                        "location": config_payload.get("location") or (row[LINK_COL] if len(row) > LINK_COL else ""),
                        "topic_id": config_payload.get("topic_id"),
                        "metadata": config_payload
                    }
                    training_polls.append(training_entry)

                elif row_type in {"FALLBACK", "FALLBACK_SOURCE", "FALLBACK_CONFIG"}:
                    fallback_entry = {
                        "name": config_payload.get("name") or alt_name or (row[ADDITIONAL_DATA_COL] if len(row) > ADDITIONAL_DATA_COL else ""),
                        "url": config_payload.get("url") or (row[LINK_COL] if len(row) > LINK_COL else ""),
                        "metadata": config_payload
                    }
                    fallback_sources.append(fallback_entry)

                else:
                    if row_comp_ids:
                        comp_ids_set.update(row_comp_ids)
                    if row_team_ids:
                        for team_id in row_team_ids:
                            team_ids_set.add(team_id)
                            team_entry = teams.setdefault(team_id, {"alt_name": None, "comp_ids": set(), "metadata": {}})
                            if alt_name:
                                team_entry["alt_name"] = alt_name
                            if config_payload:
                                team_entry["metadata"].update(config_payload)

            for team in teams.values():
                if isinstance(team.get("comp_ids"), set):
                    team["comp_ids"] = sorted(team["comp_ids"])
                if not team.get("metadata"):
                    team.pop("metadata", None)
                if not team.get("alt_name"):
                    team.pop("alt_name", None)

            return {
                'comp_ids': comp_ids_set,
                'team_ids': team_ids_set,
                'teams': teams,
                'training_polls': training_polls,
                'fallback_sources': fallback_sources,
                'voting_polls': [],
                'automation_topics': {}
            }
        except Exception as e:
            print(f"⚠️ Ошибка чтения конфигурации из сервисного листа: {e}")
            return {
                'comp_ids': set(),
                'team_ids': set(),
                'teams': {},
                'training_polls': [],
                'fallback_sources': [],
                'voting_polls': [],
                'automation_topics': {}
            }

    def get_config_ids(self) -> Dict[str, Any]:
        """Совместимая обёртка вокруг полной конфигурации"""
        full_config = self.get_full_config()
        return {
            'comp_ids': sorted(full_config.get('comp_ids', set())),
            'team_ids': sorted(full_config.get('team_ids', set())),
            'teams': full_config.get('teams', {}),
            'training_polls': full_config.get('training_polls', []),
            'fallback_sources': full_config.get('fallback_sources', []),
            'voting_polls': full_config.get('voting_polls', []),
            'automation_topics': full_config.get('automation_topics', {}),
        }

# Глобальный экземпляр для использования в других модулях
duplicate_protection = EnhancedDuplicateProtection()

def test_duplicate_protection():
    """Тестирует систему защиты от дублирования"""
    print("🧪 ТЕСТИРОВАНИЕ УСИЛЕННОЙ СИСТЕМЫ ЗАЩИТЫ ОТ ДУБЛИРОВАНИЯ")
    print("=" * 70)
    
    if not duplicate_protection.gc:
        print("❌ Google Sheets не подключен")
        return False
    
    if not duplicate_protection.service_worksheet:
        print("❌ Лист 'Сервисный' не найден")
        return False
    
    print("✅ Система готова к тестированию")
    
    # Тест 1: Проверка дубликата
    print(f"\n🧪 ТЕСТ 1: Проверка существующего дубликата")
    duplicate_check = duplicate_protection.check_duplicate("ОПРОС_ТРЕНИРОВКА", "5312150808802889330")
    print(f"   Результат: {duplicate_check}")
    
    # Тест 2: Добавление новой записи
    print(f"\n🧪 ТЕСТ 2: Добавление новой записи")
    new_record = duplicate_protection.add_record(
        "ТЕСТ_ЗАПИСЬ", 
        "test_001", 
        "АКТИВЕН", 
        "Тестовая запись для проверки"
    )
    print(f"   Результат: {new_record}")
    
    # Тест 3: Получение статистики
    print(f"\n🧪 ТЕСТ 3: Получение статистики")
    stats = duplicate_protection.get_statistics()
    print(f"   Статистика: {stats}")
    
    # Тест 4: Получение записей по типу
    print(f"\n🧪 ТЕСТ 4: Получение записей по типу")
    training_records = duplicate_protection.get_records_by_type("ОПРОС_ТРЕНИРОВКА")
    print(f"   Записи опросов тренировок: {len(training_records)}")
    
    print(f"\n✅ Тестирование завершено")
    return True

if __name__ == "__main__":
    test_duplicate_protection()
