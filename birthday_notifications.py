#!/usr/bin/env python3
"""
Модуль для уведомлений о днях рождения
Использует Google Sheets для получения данных игроков
"""

import os
import asyncio
import datetime
from dotenv import load_dotenv
from datetime_utils import get_moscow_time
from enhanced_duplicate_protection import duplicate_protection
from datetime_utils import log_current_time
from typing import Any, Dict, List, Optional

# Загружаем переменные окружения
load_dotenv()

def get_years_word(age: int) -> str:
    """Возвращает правильное склонение слова 'год'"""
    if age % 10 == 1 and age % 100 != 11:
        return "год"
    elif age % 10 in [2, 3, 4] and age % 100 not in [12, 13, 14]:
        return "года"
    else:
        return "лет"

async def check_birthdays():
    """Проверяет дни рождения и отправляет уведомления"""
    try:
        # Используем централизованное логирование времени
        time_info = log_current_time()
        
        print("🎂 Проверяем дни рождения...")
        
        # Импортируем менеджер игроков
        from players_manager import PlayersManager
        
        # Создаем менеджер
        manager = PlayersManager()
        
        # Получаем игроков с днями рождения сегодня
        birthday_players = manager.get_players_with_birthdays_today()
        
        if not birthday_players:
            print("📅 Сегодня нет дней рождения.")
            return
        
        print(f"🎉 Найдено {len(birthday_players)} именинников!")
        
        # Формируем сообщения для каждого именинника
        birthday_messages = []
        
        for player in birthday_players:
            # Получаем данные игрока
            surname = player.get('surname', '')  # Фамилия из столбца "Фамилия"
            nickname = player.get('nickname', '')  # Ник из столбца "Ник"
            telegram_id = player.get('telegram_id', '')  # Telegram ID
            first_name = player.get('name', '')  # Имя из столбца "Имя"
            age = player.get('age', 0)  # Возраст (уже вычислен)
            
            # Формируем сообщение
            if nickname and telegram_id:
                # Если есть ник и Telegram ID
                message = f"🎉 Сегодня день рождения у {surname} \"{nickname}\" ({telegram_id}) {first_name} ({age} {get_years_word(age)})!"
            elif nickname:
                # Если есть только ник
                message = f"🎉 Сегодня день рождения у {surname} \"{nickname}\" {first_name} ({age} {get_years_word(age)})!"
            elif telegram_id:
                # Если есть только Telegram ID
                message = f"🎉 Сегодня день рождения у {surname} ({telegram_id}) {first_name} ({age} {get_years_word(age)})!"
            else:
                # Если нет ни ника, ни Telegram ID
                message = f"🎉 Сегодня день рождения у {surname} {first_name} ({age} {get_years_word(age)})!"
            
            message += "\n Поздравляем! 🎂"
            birthday_messages.append(message)
        
        # Отправляем уведомления
        if birthday_messages:
            # Инициализируем бота напрямую
            bot_token = os.getenv("BOT_TOKEN")
            if not bot_token:
                print("❌ BOT_TOKEN не настроен")
                return
            
            from telegram import Bot
            current_bot = Bot(token=bot_token)
            
            # Получаем список чатов для отправки уведомлений о днях рождения
            automation_topics = duplicate_protection.get_config_ids().get("automation_topics") or {}
            birthday_settings = automation_topics.get("BIRTHDAY_NOTIFICATIONS", {})
            
            # Получаем chat_ids используя ту же логику что и в game_system_manager
            chat_id_from_secrets = os.getenv("CHAT_ID")
            chat_ids_from_secrets = []
            if chat_id_from_secrets:
                for part in chat_id_from_secrets.replace(',', ' ').split():
                    cid = part.strip()
                    if cid:
                        chat_ids_from_secrets.append(cid)
            
            chat_ids_from_table = []
            if isinstance(birthday_settings, dict) and birthday_settings.get("chat_id"):
                for part in birthday_settings.get("chat_id", "").replace(',', ' ').split():
                    cid = part.strip()
                    if cid:
                        chat_ids_from_table.append(cid)
            
            # Объединяем списки, убираем дубликаты
            all_chat_ids = []
            seen = set()
            for chat_id in chat_ids_from_table + chat_ids_from_secrets:
                if chat_id not in seen:
                    all_chat_ids.append(chat_id)
                    seen.add(chat_id)
            
            if not all_chat_ids:
                print("❌ Не настроены ID чатов для уведомлений о днях рождения (ни в таблице, ни в Secrets)")
                return
            
            birthday_topic_id = None
            if isinstance(birthday_settings, dict):
                topic_candidate = birthday_settings.get("topic_id")
                if topic_candidate is None:
                    topic_candidate = birthday_settings.get("topic_raw")
                try:
                    birthday_topic_id = int(topic_candidate) if topic_candidate is not None else None
                except (TypeError, ValueError):
                    birthday_topic_id = None

            # Отправляем каждое сообщение во все настроенные чаты
            for i, message in enumerate(birthday_messages, 1):
                player = birthday_players[i-1]
                surname = player.get('surname', '')
                first_name = player.get('name', '')
                age = player.get('age', 0)
                today = get_moscow_time().strftime('%d.%m.%Y')
                
                # Проверяем, не было ли уже отправлено уведомление для этого игрока сегодня
                birthday_key = f"birthday_{today}_{surname}_{first_name}"
                duplicate_check = duplicate_protection.check_duplicate("ДЕНЬ_РОЖДЕНИЯ", birthday_key)
                
                if duplicate_check.get('exists'):
                    print(f"⏭️ Уведомление о дне рождения для {surname} {first_name} уже отправлено сегодня, пропускаем")
                    continue
                
                # Отправляем сообщение во все настроенные чаты
                message_sent = False
                for chat_id in all_chat_ids:
                    try:
            try:
                target_chat_id: Any = int(chat_id)
            except (TypeError, ValueError):
                target_chat_id = chat_id
            
                    send_kwargs: Dict[str, Any] = {"chat_id": target_chat_id, "text": message}
                    if birthday_topic_id is not None:
                        send_kwargs["message_thread_id"] = birthday_topic_id
                    await current_bot.send_message(**send_kwargs)  # type: ignore[reportCallIssue]
                        print(f"✅ Отправлено уведомление {i} в чат {chat_id}: {message[:50]}...")
                        message_sent = True
                    except Exception as e:
                        print(f"❌ Ошибка отправки уведомления {i} в чат {chat_id}: {e}")
                    
                # Добавляем запись в сервисный лист для защиты от дублирования только если сообщение было отправлено
                if message_sent:
                    additional_info = f"{surname} {first_name} ({age} {get_years_word(age)})"
                    duplicate_protection.add_record(
                        "ДЕНЬ_РОЖДЕНИЯ",
                        birthday_key,
                        "ОТПРАВЛЕНО",
                        additional_info
                    )
                    print(f"✅ Запись добавлена в сервисный лист: {birthday_key}")
        
    except Exception as e:
        print(f"❌ Ошибка проверки дней рождения: {e}")

async def test_birthday_notifications():
    """Тестирует систему уведомлений о днях рождения"""
    print("🧪 ТЕСТ СИСТЕМЫ УВЕДОМЛЕНИЙ О ДНЯХ РОЖДЕНИЯ")
    print("=" * 60)
    
    try:
        from players_manager import PlayersManager
        
        # Создаем менеджер
        manager = PlayersManager()
        print("✅ PlayersManager инициализирован")
        
        # Получаем всех игроков
        all_players = manager.get_all_players()
        print(f"📊 Всего игроков: {len(all_players)}")
        
        # Получаем игроков с днями рождения сегодня
        birthday_players = manager.get_players_with_birthdays_today()
        print(f"🎂 Дней рождения сегодня: {len(birthday_players)}")
        
        if birthday_players:
            print("\n🎉 Именинники сегодня:")
            for i, player in enumerate(birthday_players, 1):
                surname = player.get('surname', '')
                nickname = player.get('nickname', '')
                telegram_id = player.get('telegram_id', '')
                first_name = player.get('name', '')
                age = player.get('age', 0)
                
                print(f"   {i}. {surname} {first_name} ({age} лет)")
                print(f"      Ник: {nickname or 'Не указан'}")
                print(f"      Telegram ID: {telegram_id or 'Не указан'}")
                
                # Показываем пример сообщения
                if nickname and telegram_id:
                    message = f"🎉 Сегодня день рождения у {surname} \"{nickname}\" ({telegram_id}) {first_name} ({age} {get_years_word(age)})!"
                elif nickname:
                    message = f"🎉 Сегодня день рождения у {surname} \"{nickname}\" {first_name} ({age} {get_years_word(age)})!"
                elif telegram_id:
                    message = f"🎉 Сегодня день рождения у {surname} ({telegram_id}) {first_name} ({age} {get_years_word(age)})!"
                else:
                    message = f"🎉 Сегодня день рождения у {surname} {first_name} ({age} {get_years_word(age)})!"
                
                message += "\n Поздравляем! 🎂"
                print(f"      Пример сообщения: {message}")
                print()
        else:
            print("📅 Сегодня нет дней рождения")
        
        # Показываем примеры для разных случаев
        print("📝 ПРИМЕРЫ СООБЩЕНИЙ:")
        print("-" * 40)
        
        # Пример 1: Полные данные
        print("1. С никнеймом и Telegram ID:")
        print("🎉 Сегодня день рождения у Шахманов \"Каша\" (@kkkkkkkkasha) Максим (19 лет)!")
        print(" Поздравляем! 🎂")
        print()
        
        # Пример 2: Только никнейм
        print("2. Только с никнеймом:")
        print("🎉 Сегодня день рождения у Шахманов \"Каша\" Максим (19 лет)!")
        print(" Поздравляем! 🎂")
        print()
        
        # Пример 3: Только Telegram ID
        print("3. Только с Telegram ID:")
        print("🎉 Сегодня день рождения у Шахманов (@kkkkkkkkasha) Максим (19 лет)!")
        print(" Поздравляем! 🎂")
        print()
        
        # Пример 4: Без дополнительных данных
        print("4. Без никнейма и Telegram ID:")
        print("🎉 Сегодня день рождения у Шахманов Максим (19 лет)!")
        print(" Поздравляем! 🎂")
        
        print("\n✅ Тестирование завершено")
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")

async def main():
    """Основная функция"""
    print("🎂 СИСТЕМА УВЕДОМЛЕНИЙ О ДНЯХ РОЖДЕНИЯ")
    print("=" * 60)
    
    # Тестируем систему
    await test_birthday_notifications()
    
    # Проверяем дни рождения (если время подходящее)
    await check_birthdays()

if __name__ == "__main__":
    asyncio.run(main())
