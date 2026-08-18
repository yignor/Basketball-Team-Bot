#!/usr/bin/env python3
"""Сторож трансляций: когда смотреть и как называть найденное.

    python3 tests/test_video.py

База временная. Проверяем то, на что пожаловался пользователь 17.08.2026:
после игры 16.08 бот ещё полтора часа опрашивал ВК каждую минуту, потому что
окно закрывалось таймером в три часа от начала, а не по факту финала.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="video-test-")) / "bot.db"
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                             # noqa: E402
sheets_cache.DB_PATH = TMP

import vk_video                                                 # noqa: E402

GAME = "slpro-4558"
bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def seed(start: datetime) -> None:
    """Одна игра в расписании — опрос с датой и временем начала."""
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM service_records")
        conn.execute("DELETE FROM game_meta")
        conn.execute(
            "INSERT INTO service_records (unique_key, logged_at, created_at, "
            "updated_at, data_type, game_id, game_date, game_time, alt_name, "
            "additional_data, arena, deleted) "
            "VALUES (?, ?, ?, ?, 'ОПРОС_ИГРА_SLPRO', ?, ?, ?, 'PullUp Farm', "
            "'Соперник: Балтика', 'Зал', 0)",
            (f"test-{GAME}", now, now, now, GAME,
             start.date().isoformat(), start.strftime("%H:%M")))
        conn.commit()


def finish(home: int = 71, guest: int = 64) -> None:
    """Протокол приехал: в game_meta лёг счёт."""
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO game_meta (source, game_id, game_date, "
            "home_score, guest_score, fetched_at) VALUES ('slpro', ?, ?, ?, ?, ?)",
            (vk_video.meta_id(GAME), datetime.now().date().isoformat(),
             home, guest, sheets_cache.now_iso()))
        conn.commit()


def watched(now: datetime) -> bool:
    return bool(vk_video.live_candidates(now))


def test_window_opens_and_closes() -> None:
    print("\n=== окно сторожа ===")
    start = datetime.now().replace(microsecond=0)
    seed(start)

    check(not watched(start - timedelta(hours=2)), "за два часа до — не смотрим")
    check(watched(start - timedelta(minutes=10)), "за десять минут до — смотрим")
    check(watched(start + timedelta(minutes=40)), "по ходу матча — смотрим")
    check(not watched(start + timedelta(hours=5)),
          "через пять часов молчим и без результата")


def test_result_closes_window() -> None:
    """Главное: счёт закрывает окно, не дожидаясь трёх часов."""
    print("\n=== счёт закрывает окно ===")
    start = datetime.now().replace(microsecond=0)
    seed(start)
    after = start + timedelta(hours=2, minutes=30)

    check(watched(after), "без результата в хвосте окна ещё смотрим")
    finish()
    check(vk_video.is_finished("slpro", vk_video.meta_id(GAME)),
          "игра распознана как сыгранная")
    check(not watched(after), "с результатом сторож замолчал")
    check(not watched(start + timedelta(minutes=40)),
          "и по ходу окна тоже — счёт есть, искать нечего")


def test_empty_score_is_not_a_result() -> None:
    """Ноль-ноль — это заготовка расписания, а не сыгранный матч."""
    print("\n=== ноль-ноль за результат не считаем ===")
    start = datetime.now().replace(microsecond=0)
    seed(start)
    finish(0, 0)
    check(not vk_video.is_finished("slpro", vk_video.meta_id(GAME)),
          "пустой счёт результатом не признан")
    check(watched(start + timedelta(minutes=40)), "сторож продолжает работать")


def test_link_still_closes_window() -> None:
    """Ссылка уже есть — искать нечего, это правило никуда не делось."""
    print("\n=== найденная ссылка закрывает окно ===")
    start = datetime.now().replace(microsecond=0)
    seed(start)
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO game_meta (source, game_id, game_date, "
            "video_vk, fetched_at) VALUES ('slpro', ?, ?, ?, ?)",
            (vk_video.meta_id(GAME), start.date().isoformat(),
             "https://vk.com/video-1_2", sheets_cache.now_iso()))
        conn.commit()
    check(not watched(start + timedelta(minutes=40)), "со ссылкой не смотрим")


def test_label_follows_the_clock() -> None:
    """Найденное после финала называется записью, а не эфиром.

    Между финальной сиреной и протоколом окно ещё открыто, и раньше всё
    найденное в нём уходило с шапкой «Идёт трансляция»."""
    print("\n=== надпись честная ===")
    game = {"opponent": "Балтика", "game_date": "2026-08-16",
            "home_name": "Балтика", "guest_name": "PullUp Farm"}
    live = vk_video._announce_text(game, "https://vk.com/video-1_2", live=True)
    rec = vk_video._announce_text(game, "https://vk.com/video-1_2", live=False)
    check("трансляц" in live.lower(), f"эфир назван эфиром: {live.splitlines()[0]}")
    check("запись" in rec.lower(), f"запись названа записью: {rec.splitlines()[0]}")
    check(live != rec, "тексты разные")
    check(vk_video.LIVE_GAME_HOURS < vk_video.LIVE_TAIL_HOURS,
          "матч короче предельного окна — иначе честная надпись недостижима")


def main() -> int:
    print(f"База: {TMP}")
    test_window_opens_and_closes()
    test_result_closes_window()
    test_empty_score_is_not_a_result()
    test_link_still_closes_window()
    test_label_follows_the_clock()

    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ТРАНСЛЯЦИИ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
