#!/usr/bin/env python3
"""
Отладочный скрипт для проверки парсинга расписания
"""

import asyncio
import aiohttp
import re
from bs4 import BeautifulSoup
from datetime import datetime
from datetime_utils import get_moscow_time

async def debug_parse_page(url: str, team_name: str):
    """Отладочный парсинг страницы"""
    print(f"🔍 Отладка парсинга страницы {url} для команды '{team_name}'\n")
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status != 200:
                print(f"❌ Ошибка: статус {response.status}")
                return
            
            content = await response.text()
            soup = BeautifulSoup(content, 'html.parser')
            
            # Ищем все даты на странице
            date_pattern = r'(\d{1,2})\.(\d{1,2})\.(\d{2,4})'
            all_text = soup.get_text(separator='\n', strip=True)
            
            print("📅 Все найденные даты на странице:\n")
            today = get_moscow_time().date()
            future_dates = []
            past_dates = []
            
            for line in all_text.split('\n'):
                if not line.strip():
                    continue
                
                date_matches = list(re.finditer(date_pattern, line))
                if date_matches:
                    for match in date_matches:
                        day, month, year = match.groups()
                        if len(year) == 2:
                            year = '20' + year
                        
                        try:
                            date_obj = datetime.strptime(f"{day.zfill(2)}.{month.zfill(2)}.{year}", '%d.%m.%Y').date()
                            if date_obj > today:
                                future_dates.append((date_obj, line[:150]))
                            elif date_obj < today:
                                past_dates.append((date_obj, line[:150]))
                        except:
                            pass
            
            print(f"🔮 Будущие даты ({len(future_dates)}):")
            for date_obj, line in sorted(future_dates)[:10]:
                print(f"   {date_obj.strftime('%d.%m.%Y')}: {line}")
            
            print(f"\n✅ Прошедшие даты ({len(past_dates)}):")
            for date_obj, line in sorted(past_dates, reverse=True)[:5]:
                print(f"   {date_obj.strftime('%d.%m.%Y')}: {line}")
            
            # Ищем строки с командой и датой
            print(f"\n🏀 Строки с командой '{team_name}' и датой:\n")
            team_variants = [team_name.lower(), team_name.upper(), team_name]
            for line in all_text.split('\n'):
                if not line.strip() or len(line) < 10:
                    continue
                
                line_lower = line.lower()
                has_team = any(variant.lower() in line_lower for variant in team_variants)
                has_date = bool(re.search(date_pattern, line))
                
                if has_team and has_date:
                    print(f"   {line[:200]}")

if __name__ == "__main__":
    asyncio.run(debug_parse_page("http://mb-78.ru/", "Titans"))


