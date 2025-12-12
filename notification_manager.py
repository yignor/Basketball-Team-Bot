#!/usr/bin/env python3
"""
Общий модуль для управления уведомлениями
Устраняет дублирование функционала уведомлений между разными модулями
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any, Set
from telegram import Bot

# Настройка логирования
logger = logging.getLogger(__name__)

class NotificationManager:
    """Общий менеджер уведомлений"""
    
    def __init__(self):
        self.bot = None
        self.chat_ids = self._get_chat_ids()
        self.notifications_file = "sent_notifications.json"
        self._init_bot()

    def _get_chat_ids(self) -> List[str]:
        """Получает список ID чатов из переменной окружения CHAT_ID"""
        chat_id = os.getenv('CHAT_ID')
        if not chat_id:
            return []

        # Разделяем по запятой или пробелу
        chat_ids = []
        for part in chat_id.replace(',', ' ').split():
            cid = part.strip()
            if cid:
                chat_ids.append(cid)

        return chat_ids
        
        # Загружаем отправленные уведомления из файла
        self.sent_game_end_notifications: Set[str] = set()
        self.sent_game_start_notifications: Set[str] = set()
        self.sent_game_result_notifications: Set[str] = set()
        self.sent_morning_notifications: Set[str] = set()
        self._load_sent_notifications()
    
    def _init_bot(self):
        """Инициализация бота"""
        bot_token = os.getenv('BOT_TOKEN')
        if bot_token:
            try:
                self.bot = Bot(token=bot_token)
                logger.info("✅ Бот инициализирован успешно")
            except Exception as e:
                logger.error(f"❌ Ошибка инициализации бота: {e}")
        else:
            logger.error("❌ BOT_TOKEN не настроен")
    
    def _load_sent_notifications(self):
        """Загружает отправленные уведомления из файла"""
        try:
            if os.path.exists(self.notifications_file):
                with open(self.notifications_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.sent_game_end_notifications = set(data.get('game_end', []))
                    self.sent_game_start_notifications = set(data.get('game_start', []))
                    self.sent_game_result_notifications = set(data.get('game_result', []))
                    self.sent_morning_notifications = set(data.get('morning', []))
                logger.info(f"✅ Загружено {len(self.sent_game_end_notifications) + len(self.sent_game_start_notifications) + len(self.sent_game_result_notifications) + len(self.sent_morning_notifications)} отправленных уведомлений")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки отправленных уведомлений: {e}")
    
    def _save_sent_notifications(self):
        """Сохраняет отправленные уведомления в файл"""
        try:
            data = {
                'game_end': list(self.sent_game_end_notifications),
                'game_start': list(self.sent_game_start_notifications),
                'game_result': list(self.sent_game_result_notifications),
                'morning': list(self.sent_morning_notifications)
            }
            with open(self.notifications_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения отправленных уведомлений: {e}")
    
    async def send_game_end_notification(self, game_info: Dict[str, Any], game_url: str):
        """Отправляет уведомление о завершении игры"""
        if not self.bot or not self.chat_ids:
            logger.error("Бот или CHAT_ID не настроены")
            return
        bot = self.bot
        assert bot is not None
        
        # Создаем уникальный ID для уведомления
        notification_id = f"game_end_{game_url}"
        
        if notification_id in self.sent_game_end_notifications:
            logger.info("Уведомление о завершении игры уже отправлено")
            return
        
        try:
            team1 = game_info.get('team1', 'Команда 1')
            team2 = game_info.get('team2', 'Команда 2')
            score = game_info.get('score', 'Неизвестно')
            
            message = (
                f"🏁 Игра закончилась!\n\n"
                f"🏀 {team1} vs {team2}\n"
                f"📊 Счет: {score}\n\n"
                f"Ссылка на статистику: {game_url}"
            )
            
            for chat_id in self.chat_ids:
                await bot.send_message(chat_id=chat_id, text=message)  # type: ignore[reportCallIssue]
            self.sent_game_end_notifications.add(notification_id)
            self._save_sent_notifications()
            logger.info(f"✅ Отправлено уведомление о завершении игры: {score}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о завершении игры: {e}")
    
    async def send_game_start_notification(self, game_info: Dict[str, Any], game_url: str):
        """Отправляет уведомление о начале игры"""
        if not self.bot or not self.chat_ids:
            logger.error("Бот или CHAT_ID не настроены")
            return
        bot = self.bot
        assert bot is not None
        
        # Создаем уникальный ID для уведомления
        notification_id = f"game_start_{game_url}"
        
        if notification_id in self.sent_game_start_notifications:
            logger.info("Уведомление о начале игры уже отправлено")
            return
        
        try:
            team1 = game_info.get('team1', 'Команда 1')
            team2 = game_info.get('team2', 'Команда 2')
            game_time = game_info.get('time', 'Неизвестно')
            
            message = f"🏀 Игра {team1} против {team2} начинается в {game_time}!\n\nСсылка на игру: {game_url}"
            
            for chat_id in self.chat_ids:
                await bot.send_message(chat_id=chat_id, text=message)  # type: ignore[reportCallIssue]
            self.sent_game_start_notifications.add(notification_id)
            self._save_sent_notifications()
            logger.info(f"✅ Отправлено уведомление о начале игры: {team1} vs {team2} в {game_time}")
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о начале игры: {e}")
    
    async def send_game_result_notification(self, game_info: Dict[str, Any], poll_results: Optional[Dict[str, Any]] = None, game_link: Optional[str] = None):
        """Отправляет уведомление о результате игры с количеством участников"""
        our_team_id = game_info.get('our_team_id') or game_info.get('team1_id')
        opponent_team_id = game_info.get('opponent_team_id') or game_info.get('team2_id')
        our_team_name = game_info.get('our_team_name') or game_info.get('team1', '')
        opponent_team_name = game_info.get('opponent_team_name') or game_info.get('team2', '')
        date_key = game_info.get('date', '')

        identifier_parts = [
            str(our_team_id or '').strip() or our_team_name,
            str(opponent_team_id or '').strip() or opponent_team_name,
            date_key,
        ]
        safe_identifier_parts = [part for part in identifier_parts if part]
        notification_id = "game_result_" + "_".join(safe_identifier_parts)
        
        if notification_id in self.sent_game_result_notifications:
            logger.info("Уведомление о результате игры уже отправлено")
            return
        
        if not self.bot or not self.chat_ids:
            logger.error("Бот или CHAT_ID не настроены")
            # Сохраняем состояние даже при отсутствии бота, чтобы избежать повторных попыток
            self.sent_game_result_notifications.add(notification_id)
            self._save_sent_notifications()
            return
        bot = self.bot
        assert bot is not None
        
        try:
            # Используем новую функцию форматирования с лидерами команды
            from game_system_manager import GameSystemManager
            game_manager = GameSystemManager()
            
            # Получаем лидеров команды из game_info
            our_team_leaders = game_info.get('our_team_leaders', {})
            
            # Формируем основное сообщение с лидерами
            message = game_manager.format_game_result_message(
                game_info=game_info,
                game_link=game_link,
                our_team_leaders=our_team_leaders
            )
            
            # Добавляем статистику голосования, если есть
            if poll_results:
                votes = poll_results.get('votes', {})
                ready_count = votes.get('ready', 0)
                not_ready_count = votes.get('not_ready', 0)
                coach_count = votes.get('coach', 0)
                total_votes = votes.get('total', 0)
                
                message += f"\n\n📊 Статистика голосования:\n"
                message += f"✅ Готовы: {ready_count}\n"
                message += f"❌ Не готовы: {not_ready_count}\n"
                message += f"👨‍🏫 Тренер: {coach_count}\n"
                message += f"📈 Всего: {total_votes}\n"
                
                # Анализ посещаемости
                if ready_count > 0 and total_votes > 0:
                    attendance_rate = (ready_count / total_votes) * 100
                    if attendance_rate >= 80:
                        message += f"\n🎉 Отличная посещаемость! ({attendance_rate:.1f}%)"
                    elif attendance_rate >= 60:
                        message += f"\n👍 Хорошая посещаемость ({attendance_rate:.1f}%)"
                    else:
                        message += f"\n⚠️ Низкая посещаемость ({attendance_rate:.1f}%)"
            else:
                message += f"\n\n📊 Статистика голосования: Недоступна"
            
            await bot.send_message(chat_id=self.chat_id, text=message, parse_mode='HTML')  # type: ignore[reportCallIssue]
            self.sent_game_result_notifications.add(notification_id)
            self._save_sent_notifications()
            logger.info("✅ Отправлено уведомление о результате игры")
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о результате игры: {e}")
    
    async def send_morning_notification(self, games: List[Dict[str, Any]], date: str):
        """Отправляет утреннее уведомление о предстоящих играх"""
        if not self.bot or not self.chat_ids:
            logger.error("Бот или CHAT_ID не настроены")
            return
        bot = self.bot
        assert bot is not None
        
        # Создаем уникальный ID для уведомления
        notification_id = f"morning_{date}"
        
        if notification_id in self.sent_morning_notifications:
            logger.info("Утреннее уведомление уже отправлено")
            return
        
        try:
            if not games:
                return
            
            message = f"🌅 Доброе утро! Сегодня {date} у нас игры:\n\n"
            
            for i, game in enumerate(games, 1):
                team1 = game.get('team1', 'Команда 1')
                team2 = game.get('team2', 'Команда 2')
                game_time = game.get('time', 'Неизвестно')
                game_url = game.get('url', '')
                
                message += f"{i}. 🏀 {team1} vs {team2}\n"
                message += f"   ⏰ Время: {game_time}\n"
                if game_url:
                    message += f"   🔗 Ссылка: {game_url}\n"
                message += "\n"
            
            for chat_id in self.chat_ids:
                await bot.send_message(chat_id=chat_id, text=message)  # type: ignore[reportCallIssue]
            self.sent_morning_notifications.add(notification_id)
            self._save_sent_notifications()
            logger.info(f"✅ Отправлено утреннее уведомление для {len(games)} игр")
            
        except Exception as e:
            logger.error(f"Ошибка отправки утреннего уведомления: {e}")
    
    def clear_notifications(self):
        """Очищает все отслеживаемые уведомления (для тестирования)"""
        self.sent_game_end_notifications.clear()
        self.sent_game_start_notifications.clear()
        self.sent_game_result_notifications.clear()
        self.sent_morning_notifications.clear()
        self._save_sent_notifications()
        logger.info("✅ Все отслеживаемые уведомления очищены")

# Создаем глобальный экземпляр
notification_manager = NotificationManager()
