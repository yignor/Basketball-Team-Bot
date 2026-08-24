#!/usr/bin/env python3
"""Разбор протокола матча из PDF и догон пропущенных результатов.

    python3 tests/test_protocol_pdf.py

Сети нет. PDF собираем сами из текста — проверяем разбор, а не чужую
библиотеку. Нет pypdf — та часть пропускается, а не падает: на сервере её
может не быть, и тест об этом должен сказать, а не соврать.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                             # noqa: E402
sheets_cache.DB_PATH = Path(tempfile.mkdtemp()) / "bot.db"

import protocol_pdf                                             # noqa: E402
import result_catchup                                           # noqa: E402

bad: List[str] = []

# Шапка настоящего отчёта (матч 22.08.2026, с овертаймом).
HEAD = """15:43:12 22.08.2026СТАТИСТИЧЕСКИЙ ОТЧЕТ
Кирпичный Завод  Санкт -Петербург PULL UP  Санкт -Петербург –
90:85 (14:20,21:21,16:15,28:23) 1OT
Соревнование :  Группа Б Дата :  22.08.2026 Матч №  018"""


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def test_parse_head() -> None:
    """Разбор шапки: счёт, четверти, овертайм, команды, дата."""
    print("\n=== шапка протокола разбирается ===")
    # Читаем не файл, а текст: сборка PDF в тесте проверяла бы pypdf, а не нас.
    real, protocol_pdf.read_text = protocol_pdf.read_text, lambda data: HEAD
    try:
        got = protocol_pdf.parse(b"x")
    finally:
        protocol_pdf.read_text = real

    check(got["score"] == (90, 85), f"счёт: {got['score']}")
    check(got["quarters"] == ["14:20", "21:21", "16:15", "28:23"],
          f"четверти: {got['quarters']}")
    check(got["overtimes"] == 1, f"овертайм посчитан: {got['overtimes']}")
    check(got["teams"] == ["Кирпичный Завод", "PULL UP"],
          f"команды без города: {got['teams']}")
    check(got["date"] == "22.08.2026", f"дата: {got['date']}")


def test_no_score_is_refused() -> None:
    """Не тот файл — честный отказ, а не выдуманный счёт."""
    print("\n=== чужой файл не выдаём за протокол ===")
    real, protocol_pdf.read_text = protocol_pdf.read_text, lambda d: "Договор аренды зала"
    try:
        got = protocol_pdf.parse(b"x")
    finally:
        protocol_pdf.read_text = real
    check(got["score"] is None, "счёта нет")
    check("Не нашёл" in protocol_pdf.summary(got),
          f"и об этом сказано: {protocol_pdf.summary(got)[:60]}")


def test_no_library_says_so() -> None:
    """Без библиотеки — понятная ошибка, а не тишина."""
    print("\n=== без библиотеки понятная ошибка ===")
    try:
        protocol_pdf.read_text(b"%PDF-1.4 not a real pdf")
        check(True, "библиотека есть — разбор доступен")
    except protocol_pdf.NotAvailable as exc:
        check("pypdf" in str(exc), f"сказано, чего не хватает: {exc}")
    except Exception:
        check(True, "библиотека есть, файл просто не PDF")


def test_catchup_finds_missed_game() -> None:
    """Догон находит игру со счётом, по которой результат не публиковали.

    22.08.2026: счёт приехал ночным добором через двое суток, окно слежения
    (7 часов) к тому времени закрылось, и в чат не ушло ничего."""
    print("\n=== догон находит пропущенный итог ===")
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    today = date.today()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_meta")
        conn.execute("DELETE FROM service_records")
        conn.execute("DELETE FROM league_teams")
        conn.execute("INSERT INTO league_teams (source, team_id, name, ours, "
                     "fetched_at) VALUES ('infobasket','38116','PULL UP',1,?)", (now,))
        quarters = json.dumps([{"score1": 14, "score2": 20}, {"score1": 21, "score2": 21},
                               {"score1": 16, "score2": 15}, {"score1": 28, "score2": 23},
                               {"score1": 11, "score2": 6}])
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, home_name, "
            "guest_name, home_team_id, guest_team_id, home_score, guest_score, "
            "quarters_json, fetched_at) VALUES ('infobasket','1082250',?,"
            "'Кирпичный Завод','PULL UP','999','38116',90,85,?,?)",
            (today.isoformat(), quarters, now))
        conn.commit()

    got = result_catchup.pending()
    check(len(got) == 1, f"игра найдена: {len(got)}")

    text = result_catchup.text(got[0])
    check("85:90" in text, f"счёт нашей стороной: {text.splitlines()[1]}")
    check("ПОРАЖЕНИЕ" in text, "итог назван верно")
    check("20:14" in text, "четверти развёрнуты на нашу сторону")
    check("овертайм" in text.lower(), "про овертайм сказано")
    check("опозданием" in text, "и что итог задним числом — тоже")


def test_catchup_skips_published_and_old() -> None:
    print("\n=== догон не шумит зря ===")
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT INTO service_records (unique_key, logged_at, created_at, "
            "updated_at, data_type, game_id, game_date, deleted) "
            "VALUES ('r', ?, ?, ?, 'РЕЗУЛЬТАТ_ИГРА', '1082250', ?, 0)",
            (now, now, now, date.today().isoformat()))
        conn.commit()
    check(result_catchup.pending() == [], "опубликованное второй раз не шлём")

    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM service_records")
        old = (date.today() - timedelta(days=result_catchup.MAX_AGE_DAYS + 2)).isoformat()
        conn.execute("UPDATE game_meta SET game_date = ?", (old,))
        conn.commit()
    check(result_catchup.pending() == [],
          f"старее {result_catchup.MAX_AGE_DAYS} дней — уже не новость")


def main() -> int:
    test_parse_head()
    test_no_score_is_refused()
    test_no_library_says_so()
    test_catchup_finds_missed_game()
    test_catchup_skips_published_and_old()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ПРОТОКОЛ И ДОГОН: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
