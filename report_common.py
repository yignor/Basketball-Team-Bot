#!/usr/bin/env python3
"""
Общие утилиты для отчётов посещаемости (тренировки и игры) — вынесено из
training_report.py, чтобы game_report.py не дублировал даты/Sheets-хелперы/
форматирование. training_report.py импортирует эти же имена (поведение не
меняется, вывод отчёта идентичен), game_report.py использует их для новой
логики отчёта по играм.
"""

import json
import os
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

GOOGLE_CREDS_JSON = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")
SPREADSHEET_ID    = os.getenv("SPREADSHEET_ID", "")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

MONTHS_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май",    6: "Июнь",    7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}
MONTHS_RU_GEN = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая",    6: "июня",    7: "июля",  8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}
DAYS_RU = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
DAYS_FULL_RU = ["Понедельник", "Вторник", "Среда", "Четверг", "Пятница", "Суббота", "Воскресенье"]

STATUS_EMOJI = {
    "PRESENT": "✅",
    "ABSENT":  "❌",
    "COACH":   "🎽",
    "REMOVED": "↩️",
}

PLAYERS_SHEET = "Игроки"


# ─────────────────────────── Google Sheets ───────────────────────────────────

def init_sheets():
    if not GOOGLE_CREDS_JSON or not SPREADSHEET_ID:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS или SPREADSHEET_ID не заданы")
    creds_data = json.loads(GOOGLE_CREDS_JSON)
    creds = Credentials.from_service_account_info(creds_data, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def get_or_create(spreadsheet, title: str, rows=2000, cols=12):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


def load_players(spreadsheet) -> Dict[str, Dict]:
    """Возвращает {username_lower: {surname, name, telegram_id}} и {"id:<tid>": ...}."""
    try:
        ws = spreadsheet.worksheet(PLAYERS_SHEET)
    except gspread.WorksheetNotFound:
        return {}

    rows = ws.get_all_values()
    if len(rows) < 2:
        return {}

    by_uname: Dict[str, Dict] = {}
    by_tid:   Dict[str, Dict] = {}

    for row in rows[1:]:
        if len(row) < 3 or not row[1]:
            continue
        p = {
            "surname":     row[0] if len(row) > 0 else "",
            "name":        row[1] if len(row) > 1 else "",
            "username":    (row[2] if len(row) > 2 else "").lstrip("@").lower(),
            "telegram_id": row[3] if len(row) > 3 else "",
            "status":      row[5] if len(row) > 5 else "",
        }
        if p["username"]:
            by_uname[p["username"]] = p
        if p["telegram_id"]:
            by_tid[p["telegram_id"]] = p

    return {**by_uname, **{f"id:{k}": v for k, v in by_tid.items()}}


def resolve_player(vote: Dict, players: Dict[str, Dict]) -> Tuple[str, str]:
    """Возвращает (Фамилия Имя, ник-для-отображения). vote должен иметь
    username/user_id/first_name/last_name — общий формат для голосов и за
    тренировки, и за игры."""
    uname = vote["username"].lower()
    tid   = f"id:{vote['user_id']}"

    p = players.get(uname) or players.get(tid)
    if p:
        return f"{p['surname']} {p['name']}".strip(), vote["username"] or vote["first_name"]

    display = (
        f"{vote['first_name']} {vote['last_name']}".strip()
        or vote["username"]
        or vote["user_id"]
    )
    return display, vote["username"] or vote["first_name"]


# ─────────────────────────── Date helpers ────────────────────────────────────

def iso_to_date(s: str) -> Optional[date]:
    try:
        return date.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


def week_range(d: date) -> Tuple[date, date]:
    """Возвращает (понедельник, воскресенье) недели для даты d."""
    start = d - timedelta(days=d.weekday())
    return start, start + timedelta(days=6)


def parse_period_args(args, format_error_month: str = "❌ Формат --month: YYYY-MM (например 2026-06)",
                       format_error_week: str = "❌ Формат --week: YYYY-WW (например 2026-27)"
                       ) -> Tuple[Optional[List[Tuple[int, int]]], Optional[Tuple[date, date]]]:
    """Общая логика разбора --all/--month/--week для CLI обоих отчётов.
    args — результат argparse.parse_args() с атрибутами all/month/week."""
    months: Optional[List[Tuple[int, int]]] = None
    week: Optional[Tuple[date, date]] = None

    if args.week:
        if args.week == "current":
            week = week_range(date.today())
        else:
            try:
                y, w = map(int, args.week.split("-"))
                week = week_range(date.fromisocalendar(y, w, 1))
            except ValueError:
                print(format_error_week)
                exit(1)
    elif args.month:
        try:
            y, m = map(int, args.month.split("-"))
            months = [(y, m)]
        except ValueError:
            print(format_error_month)
            exit(1)
    elif not args.all:
        today = date.today()
        months = [(today.year, today.month)]

    return months, week


# ─────────────────────────── Sheet formatting ────────────────────────────────

def apply_formatting(ws, all_rows: List[List[str]], extra_bold_patterns: Optional[List[str]] = None) -> None:
    """Применяет жирный шрифт к заголовочным строкам."""
    bold_patterns = [
        "═══", "────", "ПОСЕЩАЕМОСТЬ",
        "СВОДКИ", "ДЕТАЛЬНЫЕ", "Неделя:", "Сводка за",
        "Фамилия / Имя",
    ]
    if extra_bold_patterns:
        bold_patterns.extend(extra_bold_patterns)

    requests = []
    for i, row in enumerate(all_rows):
        text = row[0] if row else ""
        is_bold = any(p in text for p in bold_patterns)
        if is_bold:
            requests.append({
                "repeatCell": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": i,
                        "endRowIndex":   i + 1,
                        "startColumnIndex": 0,
                        # 9 колонок: в сводке к явке добавились «без ответа»,
                        # смены мнения, дни недели и дата последнего прихода.
                        "endColumnIndex":   9,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "backgroundColor": {
                                "red":   0.23 if "═══" in text else (0.17 if ("🏀" in text or "🏋️" in text) else 0.95),
                                "green": 0.27 if "═══" in text else (0.23 if ("🏀" in text or "🏋️" in text) else 0.95),
                                "blue":  0.40 if "═══" in text else (0.30 if ("🏀" in text or "🏋️" in text) else 0.95),
                            },
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,backgroundColor)",
                }
            })

    if requests:
        try:
            ws.spreadsheet.batch_update({"requests": requests})
        except Exception as e:
            print(f"   ⚠️  Форматирование: {e}")
