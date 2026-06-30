#!/usr/bin/env python3
"""
Генерация отчёта посещаемости тренировок в лист "Тренировки".

Структура листа (новые данные вверху):
  [МЕСЯЦ 2026] — сводка: кто сколько раз был
  [НЕДЕЛЯ 23–29 июня] — итог недели
    [Тренировка: 29 июня (вс)]
    Фамилия | Имя | Ответ | Статус
    ...
  [НЕДЕЛЯ 16–22 июня]
    ...
  [ПРЕДЫДУЩИЙ МЕСЯЦ] — сводка
  ...

Запуск:
  python training_report.py               # текущий месяц
  python training_report.py --all         # все данные
  python training_report.py --month 2026-06
"""

import argparse
import json
import os
from calendar import monthrange
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

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

ATTEND_SHEET  = "Посещаемость"
REPORT_SHEET  = "Тренировки"
PLAYERS_SHEET = "Игроки"

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

# ─────────────────────────── Google Sheets ───────────────────────────────────

def _init_sheets():
    if not GOOGLE_CREDS_JSON or not SPREADSHEET_ID:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS или SPREADSHEET_ID не заданы")
    creds_data = json.loads(GOOGLE_CREDS_JSON)
    creds = Credentials.from_service_account_info(creds_data, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def _get_or_create(spreadsheet, title: str, rows=2000, cols=12):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        return spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)


# ─────────────────────────── Data loading ────────────────────────────────────

def load_votes(spreadsheet) -> List[Dict]:
    """Загружает все голоса из листа Посещаемость."""
    try:
        ws = spreadsheet.worksheet(ATTEND_SHEET)
    except gspread.WorksheetNotFound:
        return []

    rows = ws.get_all_values()
    if len(rows) < 2:
        return []

    votes: List[Dict] = []
    for row in rows[1:]:  # skip header
        if len(row) < 8 or not row[0]:
            continue
        votes.append({
            "tg_poll_id":    row[0],
            "user_id":       row[1],
            "username":      row[2],
            "first_name":    row[3],
            "last_name":     row[4],
            "vote_text":     row[5],
            "vote_type":     row[6],   # PRESENT / ABSENT / COACH / REMOVED
            "training_date": row[7],   # YYYY-MM-DD
            "config_poll_id": row[8] if len(row) > 8 else "",
            "updated":       row[9]  if len(row) > 9 else "",
            "revotes":       int(row[10]) if len(row) > 10 and row[10].isdigit() else 0,
        })
    return votes


def load_players(spreadsheet) -> Dict[str, Dict]:
    """Возвращает {username_lower: {surname, name, telegram_id}} и {telegram_id: ...}."""
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
        if len(row) < 3 or not row[1]:  # need at least name
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
    """Возвращает (Фамилия Имя, ник-для-отображения)."""
    uname = vote["username"].lower()
    tid   = f"id:{vote['user_id']}"

    p = players.get(uname) or players.get(tid)
    if p:
        return f"{p['surname']} {p['name']}".strip(), vote["username"] or vote["first_name"]

    # Fallback: Telegram имя
    display = (
        f"{vote['first_name']} {vote['last_name']}".strip()
        or vote["username"]
        or vote["user_id"]
    )
    return display, vote["username"] or vote["first_name"]


def load_poll_registry(spreadsheet) -> Dict[str, Dict]:
    """Возвращает {training_date: {config_poll_id, options, ...}}."""
    try:
        ws = spreadsheet.worksheet("Сервисный")
    except gspread.WorksheetNotFound:
        return {}

    rows = ws.get_all_values()
    registry: Dict[str, Dict] = {}
    for row in rows:
        if len(row) >= 1 and row[0].upper() == "TRAINING_POLL_REG":
            try:
                meta = json.loads(row[4]) if len(row) > 4 else {}
                dt_str = row[11] if len(row) > 11 else ""
                if dt_str:
                    registry[dt_str] = {
                        "config_poll_id": row[8] if len(row) > 8 else "",
                        "options": meta.get("options", []),
                        "tg_poll_id": str(meta.get("tg_poll_id", "")),
                    }
            except (json.JSONDecodeError, IndexError):
                pass
    return registry


# ─────────────────────────── Data grouping ───────────────────────────────────

def group_by_training(votes: List[Dict]) -> Dict[str, List[Dict]]:
    """Группирует голоса по тренировке (training_date)."""
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for v in votes:
        if v["vote_type"] in ("PRESENT", "ABSENT"):  # only meaningful votes
            groups[v["training_date"]].append(v)
    return groups


def iso_to_date(s: str) -> Optional[date]:
    try:
        return date.fromisoformat(s)
    except (ValueError, AttributeError):
        return None


def week_range(d: date) -> Tuple[date, date]:
    """Возвращает (понедельник, воскресенье) недели для даты d."""
    start = d - timedelta(days=d.weekday())
    return start, start + timedelta(days=6)


# ─────────────────────────── Sheet building ──────────────────────────────────

class SheetBuilder:
    """Накапливает строки для записи в Google Sheets."""

    def __init__(self):
        self.rows: List[List[str]] = []

    def blank(self, n: int = 1):
        for _ in range(n):
            self.rows.append([""])

    def header_month(self, year: int, month: int, total_trainings: int):
        mon = MONTHS_RU.get(month, "?").upper()
        self.rows.append([f"═══ {mon} {year} ══════════════════════════════════════"])
        self.rows.append([f"Сводка за месяц · {total_trainings} тренировок"])

    def summary_table_header(self):
        self.rows.append(["Фамилия / Имя", "Ник", "Посетил", "Пропустил", "Всего", "% посещений"])

    def summary_row(self, full_name: str, nick: str, present: int, absent: int):
        total = present + absent
        pct   = f"{round(present / total * 100)}%" if total else "—"
        self.rows.append([full_name, f"@{nick}" if nick else "", str(present), str(absent), str(total), pct])

    def training_days_line(self, trainings_with_counts: List[Tuple[date, int, int]]):
        """Строка вида 'По дням: вт 10 июня – 8 · пт 13 июня – 6 · ...'"""
        parts = []
        for d, present, absent in trainings_with_counts:
            day_short = DAYS_RU[d.weekday()]
            mon_gen   = MONTHS_RU_GEN.get(d.month, "")
            parts.append(f"{day_short} {d.day} {mon_gen} – {present} чел.")
        self.rows.append([f"По дням: {' · '.join(parts)}"])

    def week_header(self, week_start: date, week_end: date,
                    trainings_with_counts: Optional[List[Tuple[date, int, int]]] = None):
        s = f"{week_start.day} {MONTHS_RU_GEN.get(week_start.month, '')}"
        e = f"{week_end.day} {MONTHS_RU_GEN.get(week_end.month, '')} {week_end.year}"
        self.rows.append([f"──── Неделя: {s} – {e} ────"])
        if trainings_with_counts:
            parts = []
            for d, present, absent in trainings_with_counts:
                day_short = DAYS_RU[d.weekday()]
                parts.append(f"{day_short} {d.day} – {present} чел.")
            self.rows.append([f"  {'  ·  '.join(parts)}"])

    def training_header(self, d: date, present_count: int, absent_count: int):
        day_name = DAYS_FULL_RU[d.weekday()]
        mon_gen  = MONTHS_RU_GEN.get(d.month, "")
        self.rows.append([
            f"🏀 Тренировка: {day_name}, {d.day} {mon_gen} {d.year}",
            "", f"✅ Пришли: {present_count}", f"❌ Пропустили: {absent_count}",
        ])

    def training_person_header(self):
        self.rows.append(["Фамилия / Имя", "Ник", "Ответ", "Статус", "Переголосований"])

    def training_person_row(self, full_name: str, nick: str, vote_text: str, vote_type: str, revotes: int):
        emoji = STATUS_EMOJI.get(vote_type, "?")
        label = "Придёт" if vote_type == "PRESENT" else ("Пропустит" if vote_type == "ABSENT" else vote_type)
        rv    = f"(x{revotes})" if revotes > 0 else ""
        self.rows.append([full_name, f"@{nick}" if nick else "", vote_text, f"{emoji} {label}", rv])

    def separator(self):
        self.rows.append([""])

    def meta_row(self, text: str):
        self.rows.append([text])


# ─────────────────────────── Report generation ───────────────────────────────

def build_report(
    votes: List[Dict],
    players: Dict[str, Dict],
    filter_months: Optional[List[Tuple[int, int]]] = None,
) -> List[List[str]]:
    """
    Строит список строк для листа Тренировки.
    Структура: месячная сводка (новые вверху), затем детали по неделям.
    """
    by_training = group_by_training(votes)

    # Parse dates and sort descending (newest first)
    training_dates_all = sorted(
        [(dt, iso_to_date(dt)) for dt in by_training.keys() if iso_to_date(dt)],
        key=lambda x: x[1],
        reverse=True,
    )

    # Filter by month if requested
    if filter_months:
        training_dates_all = [
            (s, d) for s, d in training_dates_all
            if (d.year, d.month) in {(y, m) for y, m in filter_months}
        ]

    if not training_dates_all:
        return [["Нет данных о тренировках."]]

    # Group by (year, month)
    months_seen: Dict[Tuple[int, int], List[Tuple[str, date]]] = defaultdict(list)
    for dt_str, d in training_dates_all:
        months_seen[(d.year, d.month)].append((dt_str, d))

    # ── Build sections: month summaries first (newest first), then details ──

    summary_sections: List[List[str]] = []   # summary blocks per month
    detail_sections:  List[List[str]] = []   # detailed week/training blocks

    for (year, month) in sorted(months_seen.keys(), reverse=True):
        month_trainings = months_seen[(year, month)]

        # ─ Monthly summary ─
        sb = SheetBuilder()
        sb.header_month(year, month, len(month_trainings))
        sb.blank()

        # Collect all unique players for this month
        month_votes_all: List[Dict] = []
        for dt_str, _ in month_trainings:
            month_votes_all.extend(by_training[dt_str])

        player_month: Dict[str, Dict] = defaultdict(lambda: {"present": 0, "absent": 0, "nick": ""})
        for v in month_votes_all:
            full_name, nick = resolve_player(v, players)
            key = full_name
            player_month[key]["nick"] = nick
            if v["vote_type"] == "PRESENT":
                player_month[key]["present"] += 1
            elif v["vote_type"] == "ABSENT":
                player_month[key]["absent"] += 1

        # Per-training day counts for this month (sorted oldest→newest for readability)
        month_day_counts: List[Tuple[date, int, int]] = []
        for dt_str, d in sorted(month_trainings, key=lambda x: x[1]):
            tvotes = by_training[dt_str]
            p_cnt  = sum(1 for v in tvotes if v["vote_type"] == "PRESENT")
            a_cnt  = sum(1 for v in tvotes if v["vote_type"] == "ABSENT")
            month_day_counts.append((d, p_cnt, a_cnt))

        sb.training_days_line(month_day_counts)
        sb.blank()
        sb.summary_table_header()
        # Sort by attendance desc
        for pname, pdata in sorted(player_month.items(), key=lambda x: -x[1]["present"]):
            sb.summary_row(pname, pdata["nick"], pdata["present"], pdata["absent"])

        sb.blank(2)
        summary_sections.append(sb.rows)

        # ─ Detail section: weekly blocks ─
        db = SheetBuilder()
        db.meta_row(f"──────── Детальные данные: {MONTHS_RU.get(month, '')} {year} ────────")
        db.blank()

        # Group this month's trainings by week
        weeks: Dict[Tuple[date, date], List[Tuple[str, date]]] = defaultdict(list)
        for dt_str, d in month_trainings:
            wk = week_range(d)
            weeks[wk].append((dt_str, d))

        for (wk_start, wk_end), wk_trainings in sorted(weeks.items(), reverse=True):
            # Build per-day counts for this week (chronological for the header line)
            wk_day_counts: List[Tuple[date, int, int]] = []
            for dt_str_w, d_w in sorted(wk_trainings):
                p_w = sum(1 for v in by_training[dt_str_w] if v["vote_type"] == "PRESENT")
                a_w = sum(1 for v in by_training[dt_str_w] if v["vote_type"] == "ABSENT")
                wk_day_counts.append((d_w, p_w, a_w))

            db.week_header(wk_start, wk_end, wk_day_counts)
            db.blank()

            for dt_str, d in sorted(wk_trainings, reverse=True):
                training_votes = by_training[dt_str]

                present_list = [v for v in training_votes if v["vote_type"] == "PRESENT"]
                absent_list  = [v for v in training_votes if v["vote_type"] == "ABSENT"]

                db.training_header(d, len(present_list), len(absent_list))
                db.training_person_header()

                # Sort: present first, then absent
                ordered = sorted(training_votes, key=lambda v: (0 if v["vote_type"] == "PRESENT" else 1))
                for v in ordered:
                    full_name, nick = resolve_player(v, players)
                    db.training_person_row(full_name, nick, v["vote_text"], v["vote_type"], v["revotes"])

                db.blank()

            db.blank()

        detail_sections.append(db.rows)

    # ── Assemble final output ──
    # Order: all monthly summaries (newest first), then all details (newest first)
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    header = [
        [f"ПОСЕЩАЕМОСТЬ ТРЕНИРОВОК · Обновлено: {now} МСК"],
        ["═" * 60],
        [""],
        ["СВОДКИ ПО МЕСЯЦАМ"],
        [""],
    ]

    detail_header = [
        [""],
        ["═" * 60],
        ["ДЕТАЛЬНЫЕ ДАННЫЕ ПО ТРЕНИРОВКАМ"],
        [""],
    ]

    all_rows: List[List[str]] = []
    all_rows.extend(header)
    for sec in summary_sections:
        all_rows.extend(sec)
    all_rows.extend(detail_header)
    for sec in detail_sections:
        all_rows.extend(sec)

    return all_rows


# ─────────────────────────── Sheet formatting ────────────────────────────────

def apply_formatting(ws, all_rows: List[List[str]]) -> None:
    """Применяет жирный шрифт к заголовочным строкам."""
    bold_patterns = [
        "═══", "────", "🏀 Тренировка", "ПОСЕЩАЕМОСТЬ",
        "СВОДКИ", "ДЕТАЛЬНЫЕ", "Неделя:", "Сводка за",
        "Фамилия / Имя",
    ]
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
                        "endColumnIndex":   8,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "backgroundColor": {
                                "red":   0.23 if "═══" in text else (0.17 if "🏀" in text else 0.95),
                                "green": 0.27 if "═══" in text else (0.23 if "🏀" in text else 0.95),
                                "blue":  0.40 if "═══" in text else (0.30 if "🏀" in text else 0.95),
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


# ─────────────────────────── Entry point ─────────────────────────────────────

def main(target_months: Optional[List[Tuple[int, int]]] = None) -> None:
    print(f"\n📋  Генерация отчёта посещаемости")
    print("=" * 50)

    spreadsheet = _init_sheets()

    votes   = load_votes(spreadsheet)
    players = load_players(spreadsheet)
    print(f"   Голосов в журнале: {len(votes)}")
    print(f"   Игроков в базе:    {len([p for p in players.values() if 'name' in p])}")

    if not votes:
        print("ℹ️  Нет данных для отчёта.")
        return

    all_rows = build_report(votes, players, filter_months=target_months)

    report_ws = _get_or_create(spreadsheet, REPORT_SHEET)

    # Ensure enough rows
    current_rows = report_ws.row_count
    if len(all_rows) + 10 > current_rows:
        report_ws.add_rows(len(all_rows) + 100 - current_rows)

    # Clear sheet and write
    report_ws.clear()

    # Pad rows to equal column count for batch update
    max_cols = max(len(r) for r in all_rows) if all_rows else 1
    padded = [r + [""] * (max_cols - len(r)) for r in all_rows]

    end_col_letter = chr(ord("A") + max_cols - 1)
    report_ws.update(
        f"A1:{end_col_letter}{len(padded)}",
        padded,
        value_input_option="USER_ENTERED",
    )

    # Set column widths
    try:
        spreadsheet.batch_update({"requests": [
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": report_ws.id, "dimension": "COLUMNS",
                              "startIndex": 0, "endIndex": 1},
                    "properties": {"pixelSize": 220},
                    "fields": "pixelSize",
                }
            },
            {
                "updateDimensionProperties": {
                    "range": {"sheetId": report_ws.id, "dimension": "COLUMNS",
                              "startIndex": 1, "endIndex": 2},
                    "properties": {"pixelSize": 130},
                    "fields": "pixelSize",
                }
            },
        ]})
    except Exception:
        pass

    apply_formatting(report_ws, all_rows)
    print(f"\n✅  Отчёт записан: {len(all_rows)} строк → лист '{REPORT_SHEET}'")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Отчёт посещаемости тренировок")
    ap.add_argument("--all",   action="store_true", help="Все доступные данные")
    ap.add_argument("--month", type=str,            help="Конкретный месяц: YYYY-MM")
    args = ap.parse_args()

    months: Optional[List[Tuple[int, int]]] = None
    if args.month:
        try:
            y, m = map(int, args.month.split("-"))
            months = [(y, m)]
        except ValueError:
            print("❌ Формат --month: YYYY-MM (например 2026-06)")
            exit(1)
    elif not args.all:
        today = date.today()
        months = [(today.year, today.month)]

    main(target_months=months)
