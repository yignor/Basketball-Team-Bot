#!/usr/bin/env python3
"""
Генерация отчёта посещаемости игровых опросов в лист "Игры".

Зеркалит training_report.py (см. report_common.py для общих утилит), но
читает голоса из локальной SQLite (sheets_cache.game_votes) — это новый
скрипт, писать его сразу под локально-первичную архитектуру, а не
мигрировать потом.

Запуск:
  python game_report.py               # текущий месяц
  python game_report.py --all         # все данные
  python game_report.py --month 2026-06
  python game_report.py --week        # текущая неделя
"""

import argparse
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from datetime_utils import get_moscow_time

import attendance_summary
import sheets_cache
from report_common import (
    init_sheets, get_or_create,
    load_roster, make_resolver, roster_size, apply_percent_gradient,
    fill_sparklines, sheet_locale,
    iso_to_date, parse_period_args, apply_formatting,
)

REPORT_SHEET = "Игры"


# ─────────────────────────── Data loading (из локальной БД) ──────────────────

def load_game_votes() -> List[Dict]:
    """Загружает все голоса за игры из локальной sheets_cache.game_votes."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT tg_poll_id, user_id, username, first_name, last_name, "
            "vote_text, vote_type, game_id, game_date, updated_at, revote_count "
            "FROM game_votes"
        ).fetchall()
    votes: List[Dict] = []
    for row in rows:
        votes.append({
            "tg_poll_id": row["tg_poll_id"],
            "user_id": row["user_id"],
            "username": row["username"],
            "first_name": row["first_name"],
            "last_name": row["last_name"],
            "vote_text": row["vote_text"],
            "vote_type": row["vote_type"],
            "game_id": row["game_id"],
            "game_date": row["game_date"],   # YYYY-MM-DD
            "updated": row["updated_at"],
            "revotes": row["revote_count"],
        })
    return votes


# ─────────────────────────── Data grouping ───────────────────────────────────

# ─────────────────────────── Sheet building ──────────────────────────────────

def build_report(
    votes: List[Dict],
    players: Optional[Dict[str, Dict]] = None,
    filter_months: Optional[List[Tuple[int, int]]] = None,
    filter_week: Optional[Tuple[date, date]] = None,
) -> List[List[str]]:
    """Лист «Игры» — только сводки: месяц, квартал, полугодие, год.

    Построчный список голосов по каждой игре убран: тренеру он не нужен, а лист
    от него разрастался. Аргументы периода оставлены ради совместимости с CLI и
    cron, но на вывод не влияют — сводки всегда считаются по всей истории, иначе
    запуск за пустую неделю стирал бы прошлые месяцы.
    """
    # ФИО — из состава (лист «Игроки»), а не из Telegram: ник меняется.
    roster = load_roster()
    resolve = make_resolver(roster)

    # Событие — ИГРА, а не день: в один день бывает две игры и переопрос после
    # переноса, из-за чего явка вылезала за 100%.
    by_event: Dict[str, List[Dict]] = defaultdict(list)
    for v in votes:
        if v["vote_type"] in ("PRESENT", "ABSENT"):
            by_event[str(v.get("game_id") or v.get("game_date"))].append(v)
    events: List[Tuple[date, List[Dict]]] = []
    for vlist in by_event.values():
        d = iso_to_date(vlist[0].get("game_date"))
        if d:
            events.append((d, vlist))

    # Сервер живёт по UTC, а подпись обещала МСК — время в шапке врало на 3 часа.
    now = get_moscow_time().strftime("%d.%m.%Y %H:%M")
    rows: List[List[str]] = [[f"ПОСЕЩАЕМОСТЬ ИГР · Обновлено: {now} МСК"],
                             ["═" * 60], [""]]
    if not events:
        return rows + [["Нет данных об играх."]]
    rows.extend(attendance_summary.build_sections(
        events, resolve, unit="игр", roster_total=roster_size(roster)))
    return rows


def main(
    target_months: Optional[List[Tuple[int, int]]] = None,
    target_week: Optional[Tuple[date, date]] = None,
) -> None:
    print(f"\n🏀  Генерация отчёта посещаемости игр")
    print("=" * 50)

    votes = load_game_votes()
    print(f"   Голосов в журнале: {len(votes)}")

    if not votes:
        print("ℹ️  Нет данных для отчёта.")
        return

    all_rows = build_report(votes, filter_months=target_months, filter_week=target_week)

    spreadsheet = init_sheets()
    report_ws = get_or_create(spreadsheet, REPORT_SHEET)

    current_rows = report_ws.row_count
    if len(all_rows) + 10 > current_rows:
        report_ws.add_rows(len(all_rows) + 100 - current_rows)

    report_ws.clear()

    # Шкала SPARKLINE: формула зависит от номера строки, поэтому метки
    # подменяем уже после того, как все строки собраны.
    fill_sparklines(all_rows, attendance_summary.SPARK_TOKEN,
                    attendance_summary.PCT_COLUMN_INDEX, sheet_locale(spreadsheet))

    max_cols = max(len(r) for r in all_rows) if all_rows else 1
    padded = [r + [""] * (max_cols - len(r)) for r in all_rows]

    end_col_letter = chr(ord("A") + max_cols - 1)
    report_ws.update(
        f"A1:{end_col_letter}{len(padded)}",
        padded,
        value_input_option="USER_ENTERED",
    )

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
    apply_percent_gradient(report_ws, attendance_summary.PCT_COLUMN_INDEX,
                           len(all_rows), sheet_locale(spreadsheet))
    print(f"\n✅  Отчёт записан: {len(all_rows)} строк → лист '{REPORT_SHEET}'")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Отчёт посещаемости игр")
    ap.add_argument("--all", action="store_true", help="Все доступные данные")
    ap.add_argument("--month", type=str, help="Конкретный месяц: YYYY-MM")
    ap.add_argument("--week", type=str, nargs="?", const="current", default=None,
                     help="Текущая неделя (без значения) или конкретная: YYYY-WW")
    args = ap.parse_args()
    months, week = parse_period_args(args)
    main(target_months=months, target_week=week)
