#!/usr/bin/env python3
"""
Скрипт для принудительного обновления заголовков в листе "Конфиг"
Обновляет заголовок "НАЗВАНИЕ FALLBACK" на "КОМАНДА ДЛЯ FALLBACK"
"""

from enhanced_duplicate_protection import duplicate_protection

def main():
    print("🔄 Обновление заголовков листа 'Конфиг'...")
    
    if not duplicate_protection.config_worksheet:
        print("❌ Лист 'Конфиг' не найден")
        return
    
    try:
        # Принудительно обновляем заголовки
        duplicate_protection._ensure_config_header()
        print("✅ Заголовки обновлены успешно")
        
        # Показываем текущие заголовки
        header = duplicate_protection.config_worksheet.row_values(1)
        print(f"\n📋 Текущие заголовки:")
        for i, h in enumerate(header):
            print(f"   {chr(ord('A') + i)}: {h}")
            
    except Exception as e:
        print(f"❌ Ошибка обновления заголовков: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()


