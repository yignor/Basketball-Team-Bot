#!/usr/bin/env python3
"""
Админ-панель бота: статистика по игрокам/голосованиям и короткий лог
последних автоматических действий (опросы, анонсы, результаты, дни рождения).

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


def _staleness_banner() -> list:
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


def build_dashboard() -> str:
    players = sheets_cache.get_players_stats()
    attendance = sheets_cache.get_attendance_stats()
    automation = sheets_cache.get_service_activity_stats()
    recent = sheets_cache.get_recent_service_events()

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    lines = [f"📊 Админ-панель — {now}", ""]

    lines.extend(_staleness_banner())

    lines.append("👥 Игроки")
    lines.append(f"• Всего в базе: {players['total']}")
    lines.append(f"• С привязанным Telegram ID: {players['linked']}")
    lines.append("")

    lines.append("🗳 Голосования по тренировкам")
    lines.append(f"• Уникальных пользователей: {attendance['unique_users']}")
    lines.append(f"• Активны за 30 дней: {attendance['unique_30d']}")
    lines.append(f"• Всего голосов: {attendance['total_votes']}")
    lines.append("")

    if automation:
        lines.append("📋 Автоматизация (всего записей)")
        for data_type, s in automation.items():
            lines.append(f"• {data_type}: {s['total']} (активно {s['active']}, готово {s['done']})")
        lines.append("")

    lines.append("🕐 Последние события:")
    if recent:
        for row in recent:
            status = row["status"]
            emoji = "🟡" if status in PENDING_STATUSES else "✅"
            try:
                dt = datetime.strptime(row["logged_at"], "%d.%m.%Y %H:%M")
                when = dt.strftime("%d.%m %H:%M")
            except ValueError:
                when = row["logged_at"] or "?"
            lines.append(f"{emoji} {row['data_type']} — {status or '?'} ({when})")
    else:
        lines.append("нет данных")

    return "\n".join(lines)
