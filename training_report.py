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
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

import gspread

import attendance_summary
from report_common import (
    MONTHS_RU, MONTHS_RU_GEN, DAYS_RU, DAYS_FULL_RU, STATUS_EMOJI,
    load_roster, make_resolver, apply_percent_gradient,
    init_sheets as _init_sheets,
    get_or_create as _get_or_create,
    load_players,
    resolve_player,
    iso_to_date,
    week_range,
    parse_period_args,
    apply_formatting,
)

ATTEND_SHEET  = "Посещаемость"
REPORT_SHEET  = "Тренировки"
PLAYERS_SHEET = "Игроки"

# ─────────────────────────── Google Sheets ───────────────────────────────────
# _init_sheets/_get_or_create — см. report_common.py (общие для всех отчётов)


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


# iso_to_date/week_range — см. report_common.py (общие для всех отчётов)


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
        self.rows.append(["Фамилия / Имя", "Ник", "Пришёл", "Пропустил", "Без ответа",
                          "% посещений", "Менял мнение", "Ходит по дням", "Последняя"])

    def summary_row(self, full_name: str, nick: str, present: int, absent: int,
                    no_answer: int, revotes: int, weekdays: str, last_seen: str):
        # Процент считаем от ВСЕХ тренировок месяца, а не от числа ответов:
        # промолчать и не прийти — для тренера то же самое, что прийти не смог.
        total = present + absent + no_answer
        pct = f"{round(present / total * 100)}%" if total else "—"
        self.rows.append([full_name, f"@{nick}" if nick else "",
                          str(present), str(absent), str(no_answer) if no_answer else "",
                          pct, str(revotes) if revotes else "",
                          weekdays, last_seen])

    def totals_line(self, trainings: int, avg_present: float,
                    best: Optional[Tuple[date, int]], worst: Optional[Tuple[date, int]],
                    revotes: int, silent: int):
        """Строка «итого по команде» — то, ради чего тренер открывает лист."""
        parts = [f"Тренировок: {trainings}", f"средняя явка: {avg_present:g} чел."]
        if best:
            d, n = best
            parts.append(f"лучшая: {DAYS_RU[d.weekday()]} {d.day} {MONTHS_RU_GEN.get(d.month, '')} — {n}")
        if worst and worst[0] != (best or (None,))[0]:
            d, n = worst
            parts.append(f"слабее всех: {DAYS_RU[d.weekday()]} {d.day} {MONTHS_RU_GEN.get(d.month, '')} — {n}")
        if revotes:
            parts.append(f"смен мнения: {revotes}")
        if silent:
            parts.append(f"ни разу не ответили: {silent}")
        self.rows.append([" · ".join(parts)])

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

def _weekdays_label(counts: Dict[int, int]) -> str:
    """{1: 4, 4: 2} -> «вт 4 · пт 2» — по каким дням человек реально ходит."""
    if not counts:
        return ""
    order = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return " · ".join(f"{DAYS_RU[wd]} {n}" for wd, n in order if n)


def _date_label(d: Optional[date]) -> str:
    return f"{d.day} {MONTHS_RU_GEN.get(d.month, '')}" if d else "—"


def _silent_players(players: Dict[str, Dict], voted: Dict[str, Dict]) -> int:
    """Сколько человек из состава не ответили НИ РАЗУ. В таблицу их построчно не
    выводим (там осели бы и те, кто давно не в команде), но тренеру важно
    видеть само число."""
    seen = {name.strip().lower() for name in voted}
    roster = set()
    for p in players.values():
        full = f"{p.get('surname', '')} {p.get('name', '')}".strip()
        if full:
            roster.add(full.lower())
    return len(roster - seen)


def build_report(
    votes: List[Dict],
    players: Dict[str, Dict],
    filter_months: Optional[List[Tuple[int, int]]] = None,
    filter_week: Optional[Tuple[date, date]] = None,
) -> List[List[str]]:
    """
    Строит список строк для листа Тренировки.
    Структура: месячная сводка (новые вверху), затем детали по неделям.
    """
    # ФИО берём из состава: ник в Telegram меняется, а тренер должен видеть
    # человека. Голоса тех, кого в составе нет, помечаются отдельно.
    roster = load_roster()
    resolve = make_resolver(roster)
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

    # Filter by a single week (Monday..Sunday inclusive) if requested
    if filter_week:
        week_start, week_end = filter_week
        training_dates_all = [
            (s, d) for s, d in training_dates_all
            if week_start <= d <= week_end
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
                    full_name, nick = resolve(v)
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
    ]

    detail_header = [
        [""],
        ["═" * 60],
        ["ДЕТАЛЬНЫЕ ДАННЫЕ ПО ТРЕНИРОВКАМ"],
        [""],
    ]

    # Сводки строим по ВСЕЙ истории, а не по выбранному периоду: лист
    # перезаписывается целиком, и фильтр стирал бы прошлые месяцы. Фильтр
    # оставлен для детальных секций — иначе лист разрастается без предела.
    all_events: Dict[date, List[Dict]] = {}
    for dt_str, vlist in by_training.items():
        d = iso_to_date(dt_str)
        if d:
            all_events[d] = vlist
    roster_names = [f"{p['surname']} {p['name']}".strip() for p in roster.values()]
    summary_rows = attendance_summary.build_sections(
        all_events, resolve, unit="тренировок", roster_names=roster_names)

    all_rows: List[List[str]] = []
    all_rows.extend(header)
    all_rows.extend(summary_rows)
    all_rows.extend(detail_header)
    for sec in detail_sections:
        all_rows.extend(sec)

    return all_rows


# apply_formatting — см. report_common.py (общие паттерны + "🏀 Тренировка" передаётся отдельно)


# ─────────────────────────── Entry point ─────────────────────────────────────

def main(
    target_months: Optional[List[Tuple[int, int]]] = None,
    target_week: Optional[Tuple[date, date]] = None,
) -> None:
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

    all_rows = build_report(votes, players, filter_months=target_months, filter_week=target_week)

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

    apply_formatting(report_ws, all_rows, extra_bold_patterns=["🏀 Тренировка"])
    # Цветовая шкала на колонку «% посещений»: 0% красный → 100% зелёный.
    apply_percent_gradient(report_ws, attendance_summary.PCT_COLUMN_INDEX, len(all_rows))
    print(f"\n✅  Отчёт записан: {len(all_rows)} строк → лист '{REPORT_SHEET}'")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Отчёт посещаемости тренировок")
    ap.add_argument("--all",   action="store_true", help="Все доступные данные")
    ap.add_argument("--month", type=str,            help="Конкретный месяц: YYYY-MM")
    ap.add_argument("--week",  type=str, nargs="?", const="current", default=None,
                     help="Текущая неделя (без значения) или конкретная: YYYY-WW")
    args = ap.parse_args()
    months, week = parse_period_args(args)
    main(target_months=months, target_week=week)
