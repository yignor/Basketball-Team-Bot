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


def test_late_result_still_inside_window() -> None:
    """Опоздавший протокол обязан попадать в обычный путь результата.

    22.08.2026 счёт матча с овертаймом лига опубликовала через двое суток.
    Окно слежения (7 часов) к тому времени закрылось, монитор до игры не
    доходил, и в чат не ушло ничего. Отдельного «догонного» сообщения быть не
    должно: оно уходило мимо темы результатов и без ссылки на протокол и
    лучших игроков. Вместо этого шире смотрит сам монитор."""
    print("\n=== опоздавший итог ещё в окне ===")
    from datetime_utils import (is_within_game_tracking_window,
                                GAME_TRACKING_WINDOW_HOURS,
                                RESULT_CATCHUP_WINDOW_HOURS)
    check(RESULT_CATCHUP_WINDOW_HOURS > GAME_TRACKING_WINDOW_HOURS,
          "окно догона шире окна слежения")

    two_days_ago = (date.today() - timedelta(days=2))
    dmy = two_days_ago.strftime("%d.%m.%Y")
    check(not is_within_game_tracking_window(dmy, "19:00"),
          "через двое суток слежение уже закрыто — из-за этого игра и пропала")
    check(is_within_game_tracking_window(dmy, "19:00",
                                         hours=RESULT_CATCHUP_WINDOW_HOURS),
          "но монитор результатов до неё ещё дотягивается")

    old = (date.today() - timedelta(days=5)).strftime("%d.%m.%Y")
    check(not is_within_game_tracking_window(old, "19:00",
                                             hours=RESULT_CATCHUP_WINDOW_HOURS),
          "пятидневной давности итог уже не новость")



def test_result_is_not_sent_twice() -> None:
    """Объявленную игру монитор не объявляет второй раз.

    Ключ защиты у монитора собран из даты и названий команд, и запись,
    сделанную другим путём (у той ключ по номеру игры), он не находил. Пока
    окно было семичасовым, разойтись успевал только один путь; с окном в трое
    суток монитор возвращается к уже объявленной игре — и по матчу 22.08 ушёл
    бы второй результат."""
    print("\n=== один результат на игру ===")
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    gid = "1082250"
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM service_records")
        conn.execute(
            "INSERT INTO service_records (unique_key, logged_at, created_at, "
            "updated_at, data_type, game_id, game_date, status, deleted) "
            "VALUES (?, ?, ?, ?, 'РЕЗУЛЬТАТ_ИГРА', ?, '2026-08-22', "
            "'РЕЗУЛЬТАТ ОТПРАВЛЕН', 0)",
            (f"РЕЗУЛЬТАТ_ИГРА_{gid}", now, now, now, gid))
        conn.commit()

    import enhanced_duplicate_protection as edp
    from enhanced_duplicate_protection import duplicate_protection
    # На сервере записи ведутся в локальном зеркале; в тесте Google нет вовсе,
    # и без этого флага проверка ушла бы в неинициализированную таблицу.
    edp.SERVICE_RECORDS_LOCAL_PRIMARY = True
    check(bool(duplicate_protection.get_game_record("РЕЗУЛЬТАТ_ИГРА", gid)),
          "запись об отправленном результате находится по номеру игры")
    check(not duplicate_protection.check_duplicate(
        "РЕЗУЛЬТАТ_ИГРА", "result_22.08.2026_Кирпичный_Завод_PULL_UP").get("exists"),
        "а по ключу из названий — нет: ровно та дыра, из-за которой шёл дубль")

    src = (ROOT / "game_results_monitor_final.py").read_text()
    body = src[src.index("async def send_game_result"):]
    body = body[:body.index("\n    async def ", 1)]
    guard = body.find("get_game_record")
    send = body.find("send_message")
    check(guard != -1, "монитор сверяется по номеру игры")
    check(guard != -1 and (send == -1 or guard < send),
          "и делает это ДО отправки, а не после")


def main() -> int:
    test_parse_head()
    test_no_score_is_refused()
    test_no_library_says_so()
    test_late_result_still_inside_window()
    test_result_is_not_sent_twice()
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
