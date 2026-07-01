#!/usr/bin/env python3
"""
Админ-панель бота: статистика по игрокам/голосованиям и короткий лог
последних автоматических действий (опросы, анонсы, результаты, дни рождения).

Данные берутся из уже существующих листов Google Sheets ("Игроки",
"Посещаемость", "Сервисный") — здесь только чтение и форматирование.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from enhanced_duplicate_protection import TYPE_COL, DATE_COL, STATUS_COL

PLAYERS_SHEET_NAME = "Игроки"
ATTEND_SHEET_NAME = "Посещаемость"
SERVICE_SHEET_NAME = "Сервисный"

# Только эти типы записей "Сервисного" листа считаем событиями автоматизации
# (остальные строки там — служебная конфигурация, а не лог действий).
ACTIVITY_TYPES = [
    "ОПРОС_ГОЛОСОВАНИЕ",
    "ОПРОС_ИГРА",
    "АНОНС_ИГРА",
    "РЕЗУЛЬТАТ_ИГРА",
    "ДЕНЬ_РОЖДЕНИЯ",
    "КАЛЕНДАРЬ_ИГРА",
]

# В реальных данных статус — произвольная фраза ("ОПРОС СОЗДАН", "АНОНС ОТПРАВЛЕН"
# и т.п.), явного статуса ошибки система не пишет. Поэтому считаем "активными"
# (в процессе/ожидании) только явно временные статусы, всё остальное — готово.
PENDING_STATUSES = {"АКТИВЕН", "ОТПРАВЛЯЕТСЯ"}


def _players_stats(spreadsheet) -> Dict[str, int]:
    try:
        ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
        records = ws.get_all_records()
    except Exception:
        return {"total": 0, "linked": 0}

    total = linked = 0
    for r in records:
        if not r.get("Имя"):
            continue
        total += 1
        if str(r.get("Telegram ID", "")).strip():
            linked += 1
    return {"total": total, "linked": linked}


def _attendance_stats(spreadsheet) -> Dict[str, int]:
    try:
        ws = spreadsheet.worksheet(ATTEND_SHEET_NAME)
        rows = ws.get_all_values()[1:]
    except Exception:
        return {"unique_users": 0, "total_votes": 0, "unique_30d": 0}

    cutoff = datetime.now() - timedelta(days=30)
    unique_all: set = set()
    unique_30d: set = set()
    total_votes = 0
    for row in rows:
        if len(row) < 2 or not row[1]:
            continue
        user_id = row[1]
        unique_all.add(user_id)
        total_votes += 1
        updated_raw = row[9] if len(row) > 9 else ""
        try:
            if datetime.strptime(updated_raw, "%d.%m.%Y %H:%M") >= cutoff:
                unique_30d.add(user_id)
        except ValueError:
            pass
    return {
        "unique_users": len(unique_all),
        "total_votes": total_votes,
        "unique_30d": len(unique_30d),
    }


def _service_rows(spreadsheet) -> List[List[str]]:
    try:
        ws = spreadsheet.worksheet(SERVICE_SHEET_NAME)
        return ws.get_all_values()[1:]
    except Exception:
        return []


def _automation_stats(rows: List[List[str]]) -> Dict[str, Dict[str, int]]:
    stats: Dict[str, Dict[str, int]] = {}
    for row in rows:
        if not row or not row[TYPE_COL] or row[TYPE_COL] not in ACTIVITY_TYPES:
            continue
        bucket = stats.setdefault(row[TYPE_COL], {"total": 0, "active": 0, "done": 0})
        bucket["total"] += 1
        status = row[STATUS_COL] if len(row) > STATUS_COL else ""
        if status in PENDING_STATUSES:
            bucket["active"] += 1
        else:
            bucket["done"] += 1
    return stats


def _recent_events(rows: List[List[str]], limit: int = 8) -> List[str]:
    dated: List[Tuple[datetime, List[str]]] = []
    for row in rows:
        if not row or row[TYPE_COL] not in ACTIVITY_TYPES:
            continue
        date_raw = row[DATE_COL] if len(row) > DATE_COL else ""
        try:
            dt = datetime.strptime(date_raw, "%d.%m.%Y %H:%M")
        except ValueError:
            continue
        dated.append((dt, row))
    dated.sort(key=lambda pair: pair[0], reverse=True)

    lines = []
    for dt, row in dated[:limit]:
        status = row[STATUS_COL] if len(row) > STATUS_COL else ""
        emoji = "🟡" if status in PENDING_STATUSES else "✅"
        lines.append(f"{emoji} {row[TYPE_COL]} — {status or '?'} ({dt.strftime('%d.%m %H:%M')})")
    return lines


def build_dashboard(spreadsheet) -> str:
    players = _players_stats(spreadsheet)
    attendance = _attendance_stats(spreadsheet)
    service_rows = _service_rows(spreadsheet)
    automation = _automation_stats(service_rows)
    recent = _recent_events(service_rows)

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    lines = [f"📊 Админ-панель — {now}", ""]

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
    lines.extend(recent if recent else ["нет данных"])

    return "\n".join(lines)
