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

# Заливаем ТОЛЬКО заголовки: строки с фамилиями остаются белыми, иначе таблица
# превращается в сплошную полосу и читать её невозможно.
# На тёмном фоне текст обязателен светлый — иначе чёрным по тёмно-синему.
_DARK = ({"red": 0.23, "green": 0.27, "blue": 0.40}, {"red": 1.0, "green": 1.0, "blue": 1.0})
_EVENT = ({"red": 0.17, "green": 0.23, "blue": 0.30}, {"red": 1.0, "green": 1.0, "blue": 1.0})
_LIGHT = ({"red": 0.93, "green": 0.93, "blue": 0.93}, {"red": 0.0, "green": 0.0, "blue": 0.0})


def _header_style(text: str):
    if "═══" in text or "ПОСЕЩАЕМОСТЬ" in text or "СВОДКИ" in text:
        return _DARK
    if "🏀" in text or "🏋️" in text or "────" in text or "ДЕТАЛЬНЫЕ" in text:
        return _EVENT
    return _LIGHT


def apply_formatting(ws, all_rows: List[List[str]], extra_bold_patterns: Optional[List[str]] = None) -> None:
    """Оформляет заголовочные строки: жирный, фон и КОНТРАСТНЫЙ цвет текста."""
    bold_patterns = [
        "═══", "────", "ПОСЕЩАЕМОСТЬ",
        "СВОДКИ", "ДЕТАЛЬНЫЕ", "Неделя:", "Сводка за",
        "Фамилия / Имя",
    ]
    if extra_bold_patterns:
        bold_patterns.extend(extra_bold_patterns)

    width = max((len(r) for r in all_rows), default=1)
    # Сначала сбрасываем оформление на всём листе. worksheet.clear() стирает
    # ЗНАЧЕНИЯ, но не формат: раскладка между запусками сдвигается, и тёмная
    # шапка прошлого отчёта оставалась поверх строки с фамилией.
    requests = [{
        "repeatCell": {
            "range": {"sheetId": ws.id, "startRowIndex": 0,
                      "endRowIndex": max(len(all_rows) + 200, 1),
                      "startColumnIndex": 0, "endColumnIndex": max(width, 12)},
            "cell": {"userEnteredFormat": {
                "textFormat": {"bold": False,
                               "foregroundColor": {"red": 0, "green": 0, "blue": 0}},
                "backgroundColor": {"red": 1, "green": 1, "blue": 1},
            }},
            "fields": "userEnteredFormat(textFormat,backgroundColor)",
        }
    }]
    for i, row in enumerate(all_rows):
        text = row[0] if row else ""
        if not any(p in text for p in bold_patterns):
            continue
        bg, fg = _header_style(text)
        requests.append({
            "repeatCell": {
                "range": {"sheetId": ws.id, "startRowIndex": i, "endRowIndex": i + 1,
                          "startColumnIndex": 0, "endColumnIndex": max(width, 9)},
                "cell": {"userEnteredFormat": {
                    "textFormat": {"bold": True, "foregroundColor": fg},
                    "backgroundColor": bg,
                }},
                "fields": "userEnteredFormat(textFormat,backgroundColor)",
            }
        })

    if requests:
        try:
            ws.spreadsheet.batch_update({"requests": requests})
        except Exception as e:
            print(f"   ⚠️  Форматирование: {e}")


def apply_percent_gradient(ws, column_index: int, last_row: int,
                           locale: str = "ru_RU") -> None:
    """Цветовая шкала на колонку с процентом посещений: 0% — красный,
    50% — жёлтый, 100% — зелёный. Делается правилом самой таблицы, поэтому
    работает и при ручной правке, и не зависит от того, что мы записали."""
    rng = {"sheetId": ws.id, "startRowIndex": 0, "endRowIndex": max(last_row, 1),
           "startColumnIndex": column_index, "endColumnIndex": column_index + 1}
    # InterpolationPoint.value разбирается по локали ТАБЛИЦЫ: в русской «0.5»
    # отвергается с Invalid InterpolationPoint.value — нужна запятая.
    half = "0,5" if locale.startswith("ru") else "0.5"
    try:
        # Старые правила снимаем — иначе они копятся при каждом перезапуске.
        # fields обязателен: без него метаданные приходят БЕЗ conditionalFormats,
        # старые правила не находились и накапливались поверх новых.
        existing = ws.spreadsheet.fetch_sheet_metadata(
            params={"fields": "sheets(properties.sheetId,conditionalFormats)"}
        ).get("sheets", [])
        drops = []
        for sh in existing:
            if sh.get("properties", {}).get("sheetId") != ws.id:
                continue
            for idx in range(len(sh.get("conditionalFormats") or []) - 1, -1, -1):
                drops.append({"deleteConditionalFormatRule": {"sheetId": ws.id, "index": idx}})
        ws.spreadsheet.batch_update({"requests": drops + [{
            "addConditionalFormatRule": {
                "index": 0,
                "rule": {"ranges": [rng], "gradientRule": {
                    "minpoint": {"color": {"red": 0.96, "green": 0.60, "blue": 0.60},
                                 "type": "NUMBER", "value": "0"},
                    "midpoint": {"color": {"red": 1.0, "green": 0.90, "blue": 0.60},
                                 "type": "NUMBER", "value": half},
                    "maxpoint": {"color": {"red": 0.65, "green": 0.85, "blue": 0.65},
                                 "type": "NUMBER", "value": "1"},
                }},
            }
        }]})
    except Exception as e:
        print(f"   ⚠️  Цветовая шкала: {e}")


# ─────────────── Состав: ФИО берём из листа «Игроки», не из Telegram ─────────

def load_roster() -> Dict[str, Dict]:
    """Состав из локального зеркала листа «Игроки».

    Ключи для поиска: `id:<числовой telegram id>` и ник в нижнем регистре.
    Числовой id берём из колонки «Числовой TG ID» и из player_links (её бот
    заполняет сам при первом входе). Раньше сопоставление по числовому id не
    работало вовсе: ключи строились из колонки «Telegram ID», а там @ники —
    поэтому в отчёте появлялись имена из Telegram вместо состава.
    """
    import sheets_cache
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        players = [dict(r) for r in conn.execute(
            "SELECT row_index, surname, name, nickname, telegram_id, tg_user_id FROM players")]
        links = {str(r["player_row"]): str(r["tg_user_id"]) for r in conn.execute(
            "SELECT player_row, tg_user_id FROM player_links")}

    roster: Dict[str, Dict] = {}
    for p in players:
        if not (p.get("surname") or p.get("name")):
            continue
        # key — сам человек (строка листа). По нему считаем «ни разу не
        # ответили»: сверять по ФИО нельзя — тёзки схлопнутся, а у одного
        # человека имя в отчёте и в листе может отличаться пробелом.
        entry = {"surname": p.get("surname", ""), "name": p.get("name", ""),
                 "key": f"row:{p.get('row_index')}",
                 "nick": (p.get("telegram_id") or p.get("nickname") or "").lstrip("@")}
        for raw in (p.get("telegram_id"), p.get("nickname")):
            uname = (raw or "").strip().lstrip("@").lower()
            if uname and not uname.isdigit():
                roster[uname] = entry
        for numeric in (str(p.get("tg_user_id") or "").strip(),
                        links.get(str(p.get("row_index")), "")):
            if numeric.isdigit():
                roster[f"id:{numeric}"] = entry
    return roster


def roster_size(roster: Dict[str, Dict]) -> int:
    """Сколько РАЗНЫХ людей в составе (один человек лежит под несколькими
    ключами: числовой id и ник)."""
    return len({p["key"] for p in roster.values()})


def make_resolver(roster: Dict[str, Dict]):
    """(ФИО из состава, ник, ключ-человека) по голосу.

    Сверяем по числовому id, а если его нет — по нику; ФИО для сверки не
    используем вовсе. Кого в составе не нашли, помечаем: молча показать имя из
    Telegram — значит выдать чужака за игрока команды."""
    def resolve(vote: Dict) -> Tuple[str, str, str]:
        uid = str(vote.get("user_id") or "").strip()
        uname = (vote.get("username") or "").strip().lstrip("@").lower()
        p = (roster.get(f"id:{uid}") if uid else None) or (roster.get(uname) if uname else None)
        if p:
            return f"{p['surname']} {p['name']}".strip(), p["nick"] or uname, p["key"]
        shown = (f"{vote.get('first_name', '')} {vote.get('last_name', '')}".strip()
                 or uname or uid)
        return f"{shown} (нет в составе)", uname, f"x:{uid or uname}"
    return resolve


def fill_sparklines(all_rows: List[List[str]], token: str, pct_col: int,
                    locale: str = "ru") -> None:
    """Подменяет метку шкалы формулой SPARKLINE — рисует полоску прямо в ячейке.

    Цвет задаём через IF по значению процента: 100% зелёная, 0% красная,
    между — жёлтая. Делает это сама таблица, поэтому шкала живёт и при ручной
    правке, и понятна без легенды.

    Разделители зависят от локали таблицы: в русской аргументы разделяются `;`,
    а столбцы массива `\\`; в английской — запятыми. Подставим не те — Sheets
    покажет #ERROR! вместо шкалы.
    """
    arg = ";" if locale.startswith("ru") else ","
    col = ("\\" if locale.startswith("ru") else ",")
    letter = chr(ord("A") + pct_col)
    for i, row in enumerate(all_rows):
        for j, cell in enumerate(row):
            if cell != token:
                continue
            ref = f"{letter}{i + 1}"
            color = (f'IF({ref}>=0,999{arg}"#2e7d32"{arg}IF({ref}<=0,001{arg}"#c62828"{arg}"#f9a825"))'
                     if locale.startswith("ru") else
                     f'IF({ref}>=0.999,"#2e7d32",IF({ref}<=0.001,"#c62828","#f9a825"))')
            row[j] = (f'=IFERROR(SPARKLINE({ref}{arg}'
                      f'{{"charttype"{col}"bar"{arg}"max"{col}1{arg}"color1"{col}{color}}}){arg}"")')


def sheet_locale(spreadsheet) -> str:
    """Локаль таблицы — от неё зависят разделители в формулах."""
    try:
        meta = spreadsheet.fetch_sheet_metadata(params={"fields": "properties.locale"})
        return (meta.get("properties", {}) or {}).get("locale", "ru_RU")
    except Exception:
        return "ru_RU"
