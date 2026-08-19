#!/usr/bin/env python3
"""Файл календаря по игре, заведённой тренером руками.

    python3 tests/test_calendar.py

Сеть не нужна. Лиговые игры получали .ics автоматически, а заведённые вручную
— нет: путь регистрации у них свой, и вызов просто некому было сделать.
Проверяем, что из тренерского черновика собирается корректное событие и что
вызов стоит в самом создании игры.
"""

from __future__ import annotations

import datetime
import os
import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def build(day: datetime.date, time: str, arena: str = "Зал на Пестеля"):
    """Собирает событие ровно из тех полей, что есть у черновика тренера."""
    from game_system_manager import GameSystemManager
    gsm = GameSystemManager.__new__(GameSystemManager)   # без сети и таблиц
    info = {"game_id": "slpro-m2608170822", "date": day.strftime("%d.%m.%Y"),
            "time": time, "venue": arena}
    return gsm._build_game_calendar_payload(info, "PullUp Farm", "BCC", "тёмная")


def test_event_from_coach_draft() -> None:
    print("\n=== событие собирается из черновика тренера ===")
    out = build(datetime.date(2026, 8, 29), "15:00")
    check(bool(out), "файл построен")
    if not out:
        return
    stream, filename, caption = out
    text = stream.getvalue().decode()

    check(filename.endswith(".ics"), f"это .ics: {filename}")
    check("20260829" in filename, f"дата в имени файла: {filename}")
    check("DTSTART;TZID=Europe/Moscow:20260829T150000" in text,
          "начало в московском времени, а не в UTC")
    # Матч длится не пять минут: календарь должен занять осмысленный слот.
    check("DTEND;TZID=Europe/Moscow:20260829T170000" in text, "конец через два часа")
    check("SUMMARY:PullUp Farm vs BCC" in text, "в названии обе команды")
    check("LOCATION:Зал на Пестеля" in text, "зал на месте")
    check("Форма: тёмная" in text, "форма подсказана в описании")
    check("BEGIN:VCALENDAR" in text and "END:VCALENDAR" in text, "структура целая")


def test_no_time_no_event() -> None:
    """Без времени события не бывает — лучше не слать файл, чем слать в полночь."""
    print("\n=== без времени файла нет ===")
    check(build(datetime.date(2026, 8, 29), "") is None, "пустое время — отказ")


def test_creation_calls_calendar() -> None:
    """Вызов стоит в самом создании игры, а не где-то рядом."""
    print("\n=== создание игры зовёт календарь ===")
    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index("async def _ng_send(")
    body = src[at:at + 4000]
    check("_send_game_calendar" in body, "после регистрации игры календарь отправляется")

    helper_at = src.index("async def _send_game_calendar(")
    helper = src[helper_at:helper_at + 1400]
    check("_send_calendar_event" in helper,
          "зовём ТУ ЖЕ сборку, что и лиговый путь, а не вторую свою")
    check("except Exception" in helper,
          "падение календаря не ломает создание игры — опрос уже ушёл")


def main() -> int:
    test_event_from_coach_draft()
    test_no_time_no_event()
    test_creation_calls_calendar()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("КАЛЕНДАРЬ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
