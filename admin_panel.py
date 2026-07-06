#!/usr/bin/env python3
"""
Экраны админ-меню бота: рендеринг текста для отдельных разделов
("Список пользователей" → "По таблице", "Лог действий" → "Лог бота").

Данные читаются из локального SQLite-кэша (sheets_cache.py), который сам
периодически синхронизируется с Google Sheets — см. bot_daemon.py. Здесь
только чтение кэша и форматирование.
"""

from datetime import datetime, timezone

import sheets_cache

PENDING_STATUSES = sheets_cache.PENDING_STATUSES

# Насколько старой должна быть последняя успешная синхронизация таблицы,
# прежде чем показывать предупреждение о протухших данных (2x интервал
# обновления кэша в bot_daemon.py, чтобы не ловить ложные срабатывания
# на обычном дрожании расписания).
STALE_THRESHOLD_SECONDS = 600


def staleness_banner() -> list:
    status = sheets_cache.get_sync_status()
    warnings = []
    for table in ("players", "attendance", "service_log"):
        info = status.get(table)
        if not info or not info.get("last_success_at"):
            warnings.append("⚠️ Кэш ещё не заполнен (первый запуск?) — данные могут быть неполными")
            break
        last_success = datetime.fromisoformat(info["last_success_at"])
        age = (datetime.now(timezone.utc) - last_success).total_seconds()
        if age > STALE_THRESHOLD_SECONDS:
            local_time = last_success.astimezone().strftime("%H:%M")
            minutes_ago = int(age // 60)
            warnings.append(f"⚠️ Данные могут быть устаревшими (последняя синхронизация: {local_time}, {minutes_ago} мин назад)")
            break
    return warnings


def render_users_table() -> str:
    """Список пользователей → По таблице (из кэша листов Игроки/Посещаемость)."""
    players = sheets_cache.get_players_stats()
    attendance = sheets_cache.get_attendance_stats()

    lines = [f"👥 Пользователи по таблице — {datetime.now().strftime('%d.%m.%Y %H:%M')}", ""]
    lines.extend(staleness_banner())

    lines.append("Игроки")
    lines.append(f"• Всего в базе: {players['total']}")
    lines.append(f"• С привязанным Telegram ID: {players['linked']}")
    lines.append("")

    lines.append("Голосования по тренировкам")
    lines.append(f"• Уникальных пользователей: {attendance['unique_users']}")
    lines.append(f"• Активны за 30 дней: {attendance['unique_30d']}")
    lines.append(f"• Всего голосов: {attendance['total_votes']}")

    return "\n".join(lines)


def render_bot_log(since_days: int = 1) -> str:
    """Лог действий → Лог бота: события за сегодня и since_days назад."""
    events = sheets_cache.get_recent_service_events(since_days=since_days)

    period = "сегодня" if since_days == 0 else f"последние {since_days + 1} дн."
    lines = [f"📋 Лог бота ({period}) — {datetime.now().strftime('%d.%m.%Y %H:%M')}", ""]
    lines.extend(staleness_banner())

    if events:
        for row in events:
            status = row["status"]
            emoji = "🟡" if status in PENDING_STATUSES else "✅"
            try:
                dt = datetime.strptime(row["logged_at"], "%d.%m.%Y %H:%M")
                when = dt.strftime("%d.%m %H:%M")
            except ValueError:
                when = row["logged_at"] or "?"
            lines.append(f"{emoji} {row['data_type']} — {status or '?'} ({when})")
    else:
        lines.append("Событий за этот период нет")

    return "\n".join(lines)
