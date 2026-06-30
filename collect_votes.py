#!/usr/bin/env python3
"""
Сбор голосов по тренировочным опросам из Telegram.

Запускается каждый час через GitHub Actions.
Читает poll_answer updates, определяет кто пришёл/пропустил,
сохраняет в лист "Посещаемость". Обрабатывает переголосования.

Запуск:
  python collect_votes.py
"""

import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from telegram import Bot

load_dotenv()

BOT_TOKEN            = os.getenv("BOT_TOKEN", "")
GOOGLE_CREDS_JSON    = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")
SPREADSHEET_ID       = os.getenv("SPREADSHEET_ID", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Классификация вариантов ответа
VOTE_COACH_WORDS  = {"тренер", "coach"}
VOTE_ABSENT_WORDS = {"нет", "не приду", "no", "не могу", "пропущу"}

ATTEND_SHEET  = "Посещаемость"
ATTEND_HEADER = [
    "TG_POLL_ID", "USER_ID", "USERNAME", "ИМЯ", "ФАМИЛИЯ",
    "ОТВЕТ", "ТИП", "ДАТА_ТРЕНИРОВКИ", "CONFIG_POLL_ID", "ОБНОВЛЕНО", "ПЕРЕГОЛОСОВАНИЙ",
]

# Столбцы (0-based)
COL_POLL_ID   = 0
COL_USER_ID   = 1
COL_USERNAME  = 2
COL_FNAME     = 3
COL_LNAME     = 4
COL_VOTE_TEXT = 5
COL_VOTE_TYPE = 6
COL_TRAIN_DT  = 7
COL_CFG_ID    = 8
COL_UPDATED   = 9
COL_REVOTES   = 10


# ─────────────────────────── Google Sheets init ───────────────────────────────

def _init_sheets():
    if not GOOGLE_CREDS_JSON or not SPREADSHEET_ID:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS или SPREADSHEET_ID не заданы")
    creds_data = json.loads(GOOGLE_CREDS_JSON)
    creds = Credentials.from_service_account_info(creds_data, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def _get_or_create_sheet(spreadsheet, title: str, rows: int, cols: int):
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
        return ws


def _ensure_attend_header(ws) -> None:
    vals = ws.row_values(1)
    if not vals or vals[0] != "TG_POLL_ID":
        ws.update("A1", [ATTEND_HEADER])


# ─────────────────────────── Offset management ───────────────────────────────

SERVICE_OFFSET_TYPE = "BOT_CONFIG"
SERVICE_OFFSET_KEY  = "TELEGRAM_OFFSET"


def _get_service_ws(spreadsheet):
    return spreadsheet.worksheet("Сервисный")


def get_saved_offset(service_ws) -> int:
    rows = service_ws.get_all_values()
    for row in rows:
        if len(row) >= 3 and row[0] == SERVICE_OFFSET_TYPE and row[2] == SERVICE_OFFSET_KEY:
            try:
                return int(row[3])
            except (ValueError, IndexError):
                return 0
    return 0


def save_offset(service_ws, offset: int) -> None:
    rows = service_ws.get_all_values()
    for i, row in enumerate(rows):
        if len(row) >= 3 and row[0] == SERVICE_OFFSET_TYPE and row[2] == SERVICE_OFFSET_KEY:
            service_ws.update(f"D{i+1}", [[str(offset)]])
            return
    # Not found — insert new row
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    service_ws.insert_row(
        [SERVICE_OFFSET_TYPE, now, SERVICE_OFFSET_KEY, str(offset), "Telegram update offset", "", "", "", "", "", ""],
        index=2,
    )


# ─────────────────────────── Poll registry ───────────────────────────────────

def load_training_polls(service_ws) -> Dict[str, Dict]:
    """Возвращает {tg_poll_id: {options, training_date, config_poll_id}}"""
    rows = service_ws.get_all_values()
    polls: Dict[str, Dict] = {}
    for row in rows:
        if len(row) >= 5 and row[0].upper() == "TRAINING_POLL_REG":
            try:
                meta = json.loads(row[4])  # ДОПОЛНИТЕЛЬНЫЕ ДАННЫЕ
                tg_id = str(meta.get("tg_poll_id", ""))
                if tg_id:
                    polls[tg_id] = {
                        "options":       meta.get("options", []),
                        "training_date": row[11] if len(row) > 11 else "",  # GAME DATE col
                        "config_poll_id": row[8] if len(row) > 8 else "",   # ALT NAME col
                    }
            except (json.JSONDecodeError, IndexError):
                pass
    return polls


# ─────────────────────────── Vote classification ──────────────────────────────

def classify_vote(option_text: str) -> str:
    """PRESENT | ABSENT | COACH | REMOVED"""
    if not option_text:
        return "REMOVED"
    low = option_text.strip().lower()
    if any(w in low for w in VOTE_COACH_WORDS):
        return "COACH"
    if any(w in low for w in VOTE_ABSENT_WORDS):
        return "ABSENT"
    return "PRESENT"


# ─────────────────────────── Attend sheet upsert ─────────────────────────────

def upsert_vote(
    attend_ws,
    tg_poll_id: str,
    user_id: str,
    username: str,
    first_name: str,
    last_name: str,
    vote_text: str,
    vote_type: str,
    training_date: str,
    config_poll_id: str,
) -> str:
    """Вставляет или обновляет строку голоса. Возвращает 'new'/'updated'/'skipped'."""
    now_str = datetime.now().strftime("%d.%m.%Y %H:%M")
    all_rows = attend_ws.get_all_values()

    # Find existing row for this poll+user
    for i, row in enumerate(all_rows):
        if i == 0:
            continue  # header
        if len(row) >= 2 and row[COL_POLL_ID] == tg_poll_id and row[COL_USER_ID] == user_id:
            # Re-vote: update in place
            old_type = row[COL_VOTE_TYPE] if len(row) > COL_VOTE_TYPE else ""
            revotes  = int(row[COL_REVOTES]) + 1 if (len(row) > COL_REVOTES and row[COL_REVOTES].isdigit()) else 1

            row_num = i + 1
            attend_ws.update(
                f"C{row_num}:K{row_num}",
                [[username, first_name, last_name, vote_text, vote_type,
                  training_date, config_poll_id, now_str, str(revotes)]]
            )
            action = "REMOVED→" if vote_type == "REMOVED" else f"{old_type}→{vote_type}"
            print(f"   🔄  Переголосование ({action}): {first_name} {last_name or username} [{training_date}]")
            return "updated"

    # New vote — only add if not REMOVED (user retracted vote before we saw first vote)
    if vote_type == "REMOVED":
        return "skipped"

    attend_ws.append_row([
        tg_poll_id, user_id, username, first_name, last_name,
        vote_text, vote_type, training_date, config_poll_id, now_str, "0",
    ])
    emoji = "✅" if vote_type == "PRESENT" else ("❌" if vote_type == "ABSENT" else "🎽")
    print(f"   {emoji}  Новый голос: {first_name} {last_name or username} → «{vote_text}» [{training_date}]")
    return "new"


# ─────────────────────────── Main collection ─────────────────────────────────

async def collect(bot: Bot, spreadsheet) -> None:
    service_ws = _get_service_ws(spreadsheet)
    attend_ws  = _get_or_create_sheet(spreadsheet, ATTEND_SHEET, rows=2000, cols=len(ATTEND_HEADER))
    _ensure_attend_header(attend_ws)

    # Load known training polls
    training_polls = load_training_polls(service_ws)
    if not training_polls:
        print("ℹ️  Нет зарегистрированных тренировочных опросов.")

    # Fetch new updates
    offset = get_saved_offset(service_ws)
    print(f"📥  getUpdates offset={offset}")

    updates = await bot.get_updates(
        offset=offset + 1 if offset else 0,
        limit=100,
        timeout=5,
        allowed_updates=["poll_answer"],
    )

    if not updates:
        print("ℹ️  Новых голосов нет.")
        return

    print(f"📬  Получено {len(updates)} updates")

    new_max_offset = offset
    stats = {"new": 0, "updated": 0, "skipped": 0, "ignored": 0}

    for upd in updates:
        new_max_offset = max(new_max_offset, upd.update_id)

        poll_answer = upd.poll_answer
        if not poll_answer:
            continue

        tg_poll_id = str(poll_answer.poll_id)
        if tg_poll_id not in training_polls:
            stats["ignored"] += 1
            continue

        poll_info     = training_polls[tg_poll_id]
        options_list  = poll_info["options"]
        training_date = poll_info["training_date"]
        config_poll_id = poll_info["config_poll_id"]

        user      = poll_answer.user
        user_id   = str(user.id)
        username  = (user.username or "").lstrip("@")
        first_name = user.first_name or ""
        last_name  = user.last_name or ""

        # Resolve voted option(s)
        if not poll_answer.option_ids:
            vote_text = ""
            vote_type = "REMOVED"
        else:
            chosen_texts = [
                options_list[i] for i in poll_answer.option_ids
                if i < len(options_list)
            ]
            vote_text = " + ".join(chosen_texts)
            # Classify by first option (polls allow_multiple=True but main intent = first)
            vote_type = classify_vote(chosen_texts[0] if chosen_texts else "")

        result = upsert_vote(
            attend_ws,
            tg_poll_id, user_id, username, first_name, last_name,
            vote_text, vote_type, training_date, config_poll_id,
        )
        stats[result] += 1

    # Save new offset
    if new_max_offset > offset:
        save_offset(service_ws, new_max_offset)
        print(f"💾  Offset сохранён: {new_max_offset}")

    print(
        f"\n📊  Итого: {stats['new']} новых · "
        f"{stats['updated']} переголосований · "
        f"{stats['skipped']} пропущено · "
        f"{stats['ignored']} не тренировочных"
    )


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN не задан")

    print(f"\n🏀  Сбор голосов тренировок")
    print("=" * 50)

    bot = Bot(token=BOT_TOKEN)
    spreadsheet = _init_sheets()
    await collect(bot, spreadsheet)


if __name__ == "__main__":
    asyncio.run(main())
