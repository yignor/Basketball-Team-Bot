#!/usr/bin/env python3
"""Статистика из протокола PDF — запись в базу.

    python3 tests/test_protocol_import.py

Эти цифры питают фэнтези, поэтому проверяем не «записалось ли», а чего бот
делать НЕ должен: приписывать статистику однофамильцу, перетирать данные лиги
и выдумывать игроков, которых в лиге нет.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                             # noqa: E402
sheets_cache.DB_PATH = Path(tempfile.mkdtemp()) / "bot.db"

import player_names                                             # noqa: E402
import protocol_import as pi                                    # noqa: E402

SRC, GID, DAY = "infobasket", "1082250", "2026-08-22"
bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def seed() -> None:
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_player_stats")
        conn.execute("DELETE FROM league_rosters")
        conn.execute("DELETE FROM game_meta")
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, home_name, "
            "guest_name, fetched_at) VALUES (?,?,?,'Кирпичный Завод','PULL UP',?)",
            (SRC, GID, DAY, now))
        for pid in ("310690", "399211", "777001", "777002"):
            conn.execute(
                "INSERT INTO league_rosters (source, team_id, player_id, number, "
                "active, fetched_at) VALUES (?, '36502', ?, '', 1, ?)",
                (SRC, pid, now))
        conn.commit()
    player_names.clear()
    player_names.put(SRC, "310690", "Никулин Олег")
    player_names.put(SRC, "399211", "Амбразас Никита")
    # Однофамильцы: тёзка есть в двух записях лиги — такого брать нельзя.
    player_names.put(SRC, "777001", "Долгих Денис")
    player_names.put(SRC, "777002", "Долгих Денис")


def test_finds_the_game() -> None:
    print("\n=== игра по протоколу находится ===")
    seed()
    got = pi.find_game({"date": "22.08.2026",
                        "teams": ["Кирпичный Завод", "PULL UP"]})
    check(got and got["game_id"] == GID, f"игра найдена: {got and got['game_id']}")
    check(pi.find_game({"date": "01.01.2020", "teams": ["Кто-то"]}) is None,
          "чужую дату не притягиваем")


def test_matches_only_unique_names() -> None:
    """Однофамильца не берём: приписать ему чужие очки — значит начислить их
    не тому и потом не понять, откуда они."""
    print("\n=== опознаём только однозначно ===")
    rows = [{"name": "НИКУЛИН Олег", "number": "0", "pts": 16},
            {"name": "ДОЛГИХ Денис", "number": "7", "pts": 9},
            {"name": "ПОНКРАШОВ Григорий", "number": "18", "pts": 27}]
    ok, lost = pi.match(rows, SRC)
    names = [r["name"] for r in ok]
    check(names == ["НИКУЛИН Олег"], f"взяли только однозначного: {names}")
    check(ok[0]["player_id"] == "310690", f"id верный: {ok[0]['player_id']}")
    check(ok[0]["team_id"] == "36502", "команда подставлена из заявки лиги")
    check(any("ДОЛГИХ" in n for n in lost), f"двойник отложен: {lost}")
    check(any("нет в лиге" in n for n in lost),
          f"неизвестный назван честно: {lost}")


def test_case_and_order_do_not_matter() -> None:
    print("\n=== регистр и порядок слов не мешают ===")
    ok, _ = pi.match([{"name": "олег НИКУЛИН", "pts": 1}], SRC)
    check(len(ok) == 1, "«Олег Никулин» — тот же человек")
    # А вот опечатку тут НЕ прощаем: цена ошибки — чужие очки.
    ok, lost = pi.match([{"name": "НИКУЛОН Олег", "pts": 1}], SRC)
    check(not ok, f"опечатка не проходит: {lost}")


def test_league_data_wins() -> None:
    """Данные лиги главнее разобранных из бумаги."""
    print("\n=== чужое не перетираем ===")
    seed()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT INTO game_player_stats (source, game_id, game_date, "
            "player_id, team_id, pts, fetched_at) VALUES (?,?,?,'310690','36502',99,?)",
            (SRC, GID, DAY, now))
        conn.commit()
    check(pi.already_has_stats(SRC, GID) == 1, "видим, что статистика уже есть")

    written = pi.store(SRC, GID, DAY, [{"player_id": "310690", "team_id": "36502",
                                        "number": "0", "pts": 16}])
    check(written == 0, f"строку лиги не переписали: записано {written}")
    with sheets_cache.get_connection() as conn:
        got = conn.execute("SELECT pts FROM game_player_stats WHERE player_id='310690'").fetchone()
    check(got["pts"] == 99, f"осталось значение лиги: {got['pts']}")


def test_writes_new_rows() -> None:
    print("\n=== новых записываем целиком ===")
    seed()
    rows = [{"player_id": "310690", "team_id": "36502", "number": "0",
             "pts": 16, "fgm": 8, "fga": 18, "fg3m": 0, "fg3a": 5, "ftm": 0,
             "fta": 2, "ast": 2, "stl": 0, "blk": 4, "reb_off": 3,
             "reb_def": 16, "reb": 19, "tur": 0, "pf": 2, "foul_on": 2,
             "secs": 2188}]
    check(pi.store(SRC, GID, DAY, rows) == 1, "строка записана")
    with sheets_cache.get_connection() as conn:
        got = dict(conn.execute(
            "SELECT pts, fgm, fga, tpm, tpa, reb, secs, blk FROM game_player_stats "
            "WHERE player_id='310690'").fetchone())
    check(got["pts"] == 16 and got["reb"] == 19 and got["secs"] == 2188,
          f"цифры на месте: {got}")
    check(got["tpm"] == 0 and got["tpa"] == 5, "трёхочковые разложены верно")
    check(got["blk"] == 4, "блоки на месте")


def main() -> int:
    test_finds_the_game()
    test_matches_only_unique_names()
    test_case_and_order_do_not_matter()
    test_league_data_wins()
    test_writes_new_rows()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ИМПОРТ ПРОТОКОЛА: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
