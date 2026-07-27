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
    MONTHS_RU, MONTHS_RU_GEN, DAYS_RU, DAYS_FULL_RU, STATUS_EMOJI,
    init_sheets, get_or_create, load_players, resolve_player,
    load_roster, make_resolver, roster_size, apply_percent_gradient,
    fill_sparklines, sheet_locale,
    iso_to_date, week_range, parse_period_args, apply_formatting,
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


def load_players_local() -> Dict[str, Dict]:
    """То же самое, что report_common.load_players(spreadsheet), но из
    локального зеркала листа 'Игроки' (sheets_cache.players) — не тянем
    Sheets лишний раз ради отчёта."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT surname, name, nickname, telegram_id FROM players"
        ).fetchall()
    by_uname: Dict[str, Dict] = {}
    by_tid: Dict[str, Dict] = {}
    for row in rows:
        if not row["name"]:
            continue
        p = {
            "surname": row["surname"] or "",
            "name": row["name"] or "",
            "username": (row["nickname"] or "").lstrip("@").lower(),
            "telegram_id": row["telegram_id"] or "",
        }
        if p["username"]:
            by_uname[p["username"]] = p
        if p["telegram_id"]:
            by_tid[p["telegram_id"]] = p
    return {**by_uname, **{f"id:{k}": v for k, v in by_tid.items()}}


# ─────────────────────────── Data grouping ───────────────────────────────────

def group_by_game(votes: List[Dict]) -> Dict[str, List[Dict]]:
    """Группирует голоса по игре (game_date) — та же дата может встречаться
    у нескольких игр, но для отчёта посещаемости это не критично (как и в
    training_report.py, где несколько тренировок в один день агрегируются
    по дате)."""
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for v in votes:
        if v["vote_type"] in ("PRESENT", "ABSENT"):
            groups[v["game_date"]].append(v)
    return groups


# ─────────────────────────── Sheet building ──────────────────────────────────

class GameSheetBuilder:
    """Аналог SheetBuilder из training_report.py, с игровой терминологией."""

    def __init__(self):
        self.rows: List[List[str]] = []

    def blank(self, n: int = 1):
        for _ in range(n):
            self.rows.append([""])

    def header_month(self, year: int, month: int, total_games: int):
        mon = MONTHS_RU.get(month, "?").upper()
        self.rows.append([f"═══ {mon} {year} ══════════════════════════════════════"])
        self.rows.append([f"Сводка за месяц · {total_games} игр"])

    def summary_table_header(self):
        self.rows.append(["Фамилия / Имя", "Ник", "Готов", "Не смог", "Всего", "% готовности"])

    def summary_row(self, full_name: str, nick: str, present: int, absent: int):
        total = present + absent
        pct = f"{round(present / total * 100)}%" if total else "—"
        self.rows.append([full_name, f"@{nick}" if nick else "", str(present), str(absent), str(total), pct])

    def game_days_line(self, games_with_counts: List[Tuple[date, int, int]]):
        parts = []
        for d, present, absent in games_with_counts:
            day_short = DAYS_RU[d.weekday()]
            mon_gen = MONTHS_RU_GEN.get(d.month, "")
            parts.append(f"{day_short} {d.day} {mon_gen} – {present} чел.")
        self.rows.append([f"По дням: {' · '.join(parts)}"])

    def week_header(self, week_start: date, week_end: date,
                    games_with_counts: Optional[List[Tuple[date, int, int]]] = None):
        s = f"{week_start.day} {MONTHS_RU_GEN.get(week_start.month, '')}"
        e = f"{week_end.day} {MONTHS_RU_GEN.get(week_end.month, '')} {week_end.year}"
        self.rows.append([f"──── Неделя: {s} – {e} ────"])
        if games_with_counts:
            parts = []
            for d, present, absent in games_with_counts:
                day_short = DAYS_RU[d.weekday()]
                parts.append(f"{day_short} {d.day} – {present} чел.")
            self.rows.append([f"  {'  ·  '.join(parts)}"])

    def game_header(self, d: date, present_count: int, absent_count: int):
        day_name = DAYS_FULL_RU[d.weekday()]
        mon_gen = MONTHS_RU_GEN.get(d.month, "")
        self.rows.append([
            f"🏀 Игра: {day_name}, {d.day} {mon_gen} {d.year}",
            "", f"✅ Готовы: {present_count}", f"❌ Не смогли: {absent_count}",
        ])

    def game_person_header(self):
        self.rows.append(["Фамилия / Имя", "Ник", "Ответ", "Статус", "Переголосований"])

    def game_person_row(self, full_name: str, nick: str, vote_text: str, vote_type: str, revotes: int):
        emoji = STATUS_EMOJI.get(vote_type, "?")
        label = "Готов" if vote_type == "PRESENT" else ("Не сможет" if vote_type == "ABSENT" else vote_type)
        rv = f"(x{revotes})" if revotes > 0 else ""
        self.rows.append([full_name, f"@{nick}" if nick else "", vote_text, f"{emoji} {label}", rv])

    def meta_row(self, text: str):
        self.rows.append([text])


# ─────────────────────────── Report generation ───────────────────────────────

def build_report(
    votes: List[Dict],
    players: Dict[str, Dict],
    filter_months: Optional[List[Tuple[int, int]]] = None,
    filter_week: Optional[Tuple[date, date]] = None,
) -> List[List[str]]:
    # ФИО — из состава (лист «Игроки»), а не из Telegram: ник меняется.
    roster = load_roster()
    resolve = make_resolver(roster)
    by_game = group_by_game(votes)

    game_dates_all = sorted(
        [(dt, iso_to_date(dt)) for dt in by_game.keys() if iso_to_date(dt)],
        key=lambda x: x[1],
        reverse=True,
    )

    if filter_months:
        game_dates_all = [
            (s, d) for s, d in game_dates_all
            if (d.year, d.month) in {(y, m) for y, m in filter_months}
        ]

    if filter_week:
        week_start, week_end = filter_week
        game_dates_all = [(s, d) for s, d in game_dates_all if week_start <= d <= week_end]

    # ВАЖНО: пустой период НЕ должен обнулять лист. Сводки строятся по всей
    # истории и не зависят от фильтра; пусто может быть только в деталях —
    # например, в понедельник, когда на новой неделе тренировок ещё не было.
    # Для сводок событие — ИГРА, а не день: в один день бывает две игры и
    # переопрос после переноса, из-за чего явка вылезала за 100%.
    _by_event: Dict[str, List[Dict]] = defaultdict(list)
    for _v in votes:
        if _v["vote_type"] in ("PRESENT", "ABSENT"):
            _by_event[str(_v.get("game_id") or _v.get("game_date"))].append(_v)
    all_events: List[Tuple[date, List[Dict]]] = []
    for _vlist in _by_event.values():
        _d = iso_to_date(_vlist[0].get("game_date"))
        if _d:
            all_events.append((_d, _vlist))
    summary_rows_all = attendance_summary.build_sections(
        all_events, resolve, unit="игр", roster_total=roster_size(roster))

    if not game_dates_all:
        now = get_moscow_time().strftime("%d.%m.%Y %H:%M")
        return ([[f"ПОСЕЩАЕМОСТЬ ИГР · Обновлено: {now} МСК"], ["═" * 60], [""]]
                + summary_rows_all
                + [[""], ["За выбранный период событий не было — показаны сводки."]])


    months_seen: Dict[Tuple[int, int], List[Tuple[str, date]]] = defaultdict(list)
    for dt_str, d in game_dates_all:
        months_seen[(d.year, d.month)].append((dt_str, d))

    detail_sections: List[List[str]] = []

    for (year, month) in sorted(months_seen.keys(), reverse=True):
        month_games = months_seen[(year, month)]

        db = GameSheetBuilder()
        db.meta_row(f"──────── Детальные данные: {MONTHS_RU.get(month, '')} {year} ────────")
        db.blank()

        weeks: Dict[Tuple[date, date], List[Tuple[str, date]]] = defaultdict(list)
        for dt_str, d in month_games:
            wk = week_range(d)
            weeks[wk].append((dt_str, d))

        for (wk_start, wk_end), wk_games in sorted(weeks.items(), reverse=True):
            wk_day_counts: List[Tuple[date, int, int]] = []
            for dt_str_w, d_w in sorted(wk_games):
                p_w = sum(1 for v in by_game[dt_str_w] if v["vote_type"] == "PRESENT")
                a_w = sum(1 for v in by_game[dt_str_w] if v["vote_type"] == "ABSENT")
                wk_day_counts.append((d_w, p_w, a_w))

            db.week_header(wk_start, wk_end, wk_day_counts)
            db.blank()

            for dt_str, d in sorted(wk_games, reverse=True):
                game_votes = by_game[dt_str]
                present_list = [v for v in game_votes if v["vote_type"] == "PRESENT"]
                absent_list = [v for v in game_votes if v["vote_type"] == "ABSENT"]

                db.game_header(d, len(present_list), len(absent_list))
                db.game_person_header()

                ordered = sorted(game_votes, key=lambda v: (0 if v["vote_type"] == "PRESENT" else 1))
                for v in ordered:
                    full_name, nick, _key = resolve(v)
                    db.game_person_row(full_name, nick, v["vote_text"], v["vote_type"], v["revotes"])

                db.blank()

            db.blank()

        detail_sections.append(db.rows)

    # Сервер живёт по UTC, а подпись обещала МСК — время в шапке врало на 3 часа.
    now = get_moscow_time().strftime("%d.%m.%Y %H:%M")
    header = [
        [f"ПОСЕЩАЕМОСТЬ ИГР · Обновлено: {now} МСК"],
        ["═" * 60],
        [""],
    ]
    detail_header = [
        [""],
        ["═" * 60],
        ["ДЕТАЛЬНЫЕ ДАННЫЕ ПО ИГРАМ"],
        [""],
    ]

    all_rows: List[List[str]] = []
    all_rows.extend(header)
    # Сводки — по всей истории (месяц/квартал/полугодие/год), детали — по фильтру.
    all_rows.extend(summary_rows_all)
    all_rows.extend(detail_header)
    for sec in detail_sections:
        all_rows.extend(sec)

    return all_rows


# ─────────────────────────── Entry point ─────────────────────────────────────

def main(
    target_months: Optional[List[Tuple[int, int]]] = None,
    target_week: Optional[Tuple[date, date]] = None,
) -> None:
    print(f"\n🏀  Генерация отчёта посещаемости игр")
    print("=" * 50)

    votes = load_game_votes()
    players = load_players_local()
    print(f"   Голосов в журнале: {len(votes)}")
    print(f"   Игроков в базе:    {len([p for p in players.values() if 'name' in p])}")

    if not votes:
        print("ℹ️  Нет данных для отчёта.")
        return

    all_rows = build_report(votes, players, filter_months=target_months, filter_week=target_week)

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

    apply_formatting(report_ws, all_rows, extra_bold_patterns=["🏀 Игра"])
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
