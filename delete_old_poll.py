#!/usr/bin/env python3
"""
Скрипт для удаления старого опроса с неправильными данными
"""

from enhanced_duplicate_protection import duplicate_protection

def main():
    print("🔍 Поиск опроса для игры 23.11.2025 Titans vs YETI...\n")
    
    # Ищем записи типа ОПРОС_ИГРА
    records = duplicate_protection.get_records_by_type("ОПРОС_ИГРА")
    
    found = False
    for record in records:
        game_date = record.get('game_date', '')
        additional_data = record.get('additional_data', '')
        unique_key = record.get('unique_key', '')
        
        # Ищем запись с датой 23.11.2025
        if '23.11.2025' in game_date or '23.11.2025' in unique_key:
            print(f"📌 Найдена запись:")
            print(f"   Уникальный ключ: {unique_key}")
            print(f"   Дата игры: {game_date}")
            print(f"   Время игры: {record.get('game_time', '')}")
            print(f"   Арена: {record.get('arena', '')}")
            print(f"   Дополнительные данные: {additional_data}")
            print(f"   Строка: {record.get('row', '')}")
            
            # Удаляем запись
            if record.get('row'):
                try:
                    worksheet = duplicate_protection._get_service_worksheet()
                    if worksheet:
                        worksheet.delete_rows(record['row'])
                        print(f"   ✅ Запись удалена из строки {record['row']}")
                        found = True
                except Exception as e:
                    print(f"   ❌ Ошибка удаления: {e}")
    
    if not found:
        print("⚠️ Запись не найдена")
    else:
        print("\n✅ Старая запись удалена. При следующем запуске будет создан новый опрос с правильными данными.")

if __name__ == "__main__":
    main()


