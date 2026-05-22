# Настройка Playwright для парсинга JavaScript-контента

## Что такое Playwright?

Playwright - это инструмент для автоматизации браузеров, который позволяет парсить сайты с динамическим контентом, загружаемым через JavaScript.

## Зачем это нужно?

Некоторые сайты (например, globalleague.ru, neva-basket.ru, slpro.basketstat.ru) загружают данные через JavaScript, поэтому обычный парсинг HTML не видит таблицы и расписание игр. Playwright запускает реальный браузер и ждет загрузки JavaScript-контента.

## Автоматическая работа в GitHub Actions

Playwright уже настроен для работы в GitHub Actions:

1. ✅ Добавлен в `requirements-github.txt`
2. ✅ Настроен в `.github/workflows/fallback_game_monitor.yml`
3. ✅ Браузеры устанавливаются автоматически при каждом запуске

**Ничего дополнительно делать не нужно!** Просто запустите workflow в GitHub Actions.

## Локальная установка (для тестирования)

Если хотите протестировать локально:

```bash
# 1. Установите Playwright
pip install playwright

# 2. Установите браузеры (только один раз)
playwright install chromium

# 3. Запустите fallback мониторинг
python fallback_game_monitor.py
```

## Поддерживаемые сайты

Следующие сайты автоматически используют Playwright:

- ✅ `globalleague.ru` - Глобальная Лига
- ✅ `neva-basket.ru` - Невская Баскетбольная Лига
- ✅ `basketstat.ru` (в т.ч. `slpro.basketstat.ru`) - BasketStat SLPRO, календарь расписания

## Как добавить новый сайт с JavaScript?

Если нужно добавить поддержку другого сайта с JavaScript:

1. Откройте `fallback_game_monitor.py`
2. Найдите функцию `_needs_playwright()` (около строки 1528)
3. Добавьте домен сайта в список:

```python
def _needs_playwright(self, url: str) -> bool:
    """Определяет, нужен ли Playwright для парсинга этого сайта"""
    js_sites = ['globalleague.ru', 'neva-basket.ru', 'basketstat.ru', 'новый-сайт.ru']
    return any(site in url for site in js_sites)
```

## Отладка

Если Playwright не работает:

1. **Проверьте установку:**
   ```bash
   python -c "from playwright.async_api import async_playwright; print('OK')"
   ```

2. **Проверьте браузеры:**
   ```bash
   playwright install chromium
   ```

3. **Проверьте логи:**
   - В логах должно быть: `🌐 Загрузка страницы через Playwright`
   - Если видите: `Playwright не установлен` - установите его

## Производительность

- Playwright медленнее обычного парсинга (нужно запускать браузер)
- Используется только для сайтов с JavaScript
- Обычные сайты (mb-78.ru, letobasket.ru) парсятся быстрее без Playwright

## Ограничения

- Браузеры Playwright не хранятся в Git (слишком большие)
- В GitHub Actions браузеры устанавливаются при каждом запуске
- Локально нужно установить браузеры один раз командой `playwright install chromium`

