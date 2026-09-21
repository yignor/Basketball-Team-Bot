#!/usr/bin/env python3
"""Стартовая пятёрка: цифры — по турниру ЭТОЙ игры.

    python3 tests/test_lineup_scope.py

Команда играет сразу в двух лигах. Пока средние и число игр складывались по
всем текущим турнирам, рядом с фамилией стояло «14 игр» — сумма двенадцати в
одной лиге и двух в другой. Тренер ставит состав на конкретную игру и читает
это как опыт именно в ней, поэтому считаем по её турниру.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

TMP = Path(tempfile.mkdtemp(prefix="lineup-test-")) / "bot.db"
DAY = date.today() + timedelta(days=2)

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def setup() -> Any:
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ["ADMIN_USER_IDS"] = "1"
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
    os.environ["SPREADSHEET_ID"] = ""
    import sheets_cache
    sheets_cache.DB_PATH = TMP
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        for row, sur, name in ((2, "Иванов", "Иван"), (3, "Петров", "Пётр")):
            conn.execute(
                "INSERT INTO players (row_index, surname, name, active_mark, "
                "synced_at) VALUES (?, ?, ?, '1', ?)", (row, sur, name, now))
        # Две наши команды: та же команда играет и в лиге SLPRO, и в Инфобаскете.
        conn.execute(
            "INSERT INTO league_teams (source, team_id, name, league, comp_id, "
            "season_id, stage_id, ours, fetched_at) VALUES ('slpro', '707', "
            "'PULL UP FARM', 'Осенняя лига', '', 'S26', 'ST1', 1, ?)", (now,))
        conn.execute(
            "INSERT INTO league_teams (source, team_id, name, league, comp_id, "
            "season_id, stage_id, ours, fetched_at) VALUES ('infobasket', "
            "'36502', 'PULL UP', 'Ночная лига', '140825', '140825', '', 1, ?)",
            (now,))
        # Один человек играет в обеих лигах: 2 игры в SLPRO и 12 в Инфобаскете.
        for ref, row in (("slpro:707:222", 2), ("ib:36502:111", 2),
                         ("slpro:707:333", 3)):
            conn.execute("INSERT INTO price_refs (ref, player_row, updated_at) "
                         "VALUES (?, ?, ?)", (ref, row, now))
        totals = (("slpro", "222", "S26", "ST1", 2, 20, 10, 4),
                  ("infobasket", "111", "140825", "", 12, 240, 60, 24),
                  # Прошлый сезон той же лиги — в счёт тоже не идёт.
                  ("slpro", "222", "S25", "ST1", 30, 300, 150, 60),
                  ("slpro", "333", "S26", "ST1", 4, 40, 20, 8))
        for src, pid, season, stage, g, pts, reb, tur in totals:
            conn.execute(
                "INSERT INTO player_totals (source, player_id, season_id, "
                "stage_id, games, pts, reb, tur, updated_at) VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (src, pid, season, stage, g, pts, reb, tur, now))
        # Игра лиги SLPRO: в расписании она «slpro-4600», в справочнике матчей
        # — голым числом.
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, season_id, "
            "stage_id, home_name, guest_name, fetched_at) VALUES ('slpro', "
            "'4600', ?, 'S26', 'ST1', 'PULL UP FARM', 'Кураж', ?)",
            (DAY.isoformat(), now))
        for gid in ("slpro-4600", "slpro-m2609211200"):
            conn.execute(
                "INSERT INTO service_records (data_type, unique_key, logged_at, "
                "game_id, game_date, alt_name, created_at, updated_at) VALUES "
                "('ОПРОС_ИГРА_SLPRO', ?, ?, ?, ?, 'Кураж', ?, ?)",
                (gid, now, gid, DAY.isoformat(), now, now))
            for row in (2, 3):
                conn.execute(
                    "INSERT INTO game_rosters (source, game_id, player_row, "
                    "added_at) VALUES ('slpro', ?, ?, ?)", (gid, row, now))
        conn.commit()
    import coach_lineup
    return coach_lineup


def test_scope_of_game(cl) -> None:
    print("\n=== турнир игры ===")
    scopes = cl.game_scope("slpro", "slpro-4600")
    check(scopes == [{"source": "slpro", "season_id": "S26", "stage_id": "ST1"}],
          f"у игры из расписания — её сезон и стадия: {scopes}")
    check(cl.game_scope("slpro", "4600") == scopes,
          "id с приставкой «slpro-» и без неё — одна и та же игра")
    hand = cl.game_scope("slpro", "slpro-m2609211200")
    check([s["source"] for s in hand] == ["slpro"],
          f"игра, заведённая руками: турниры только её лиги: {hand}")


def test_lineup_counts(cl) -> None:
    print("\n=== цифры в составе ===")
    data = cl.lineup("slpro", "slpro-4600", "name")
    by_row = {r["row"]: r for r in data["rows"]}
    ivan = (by_row.get(2) or {}).get("avg") or {}
    check(ivan.get("games") == 2,
          f"игр — только в турнире этой игры, не 14 и не 32: {ivan.get('games')}")
    check(ivan.get("pts") == 10, f"и средние оттуда же: {ivan.get('pts')} очк")
    check(((by_row.get(3) or {}).get("avg") or {}).get("games") == 4,
          "у второго — его 4 игры в этом турнире")

    card = cl.text(data)
    check("(2 игры)" in card, "в карточке у игрока — число игр этого турнира")
    check("по турниру «Осенняя лига»" in card,
          "подпись называет турнир, по которому посчитано")
    check("по текущим турнирам" not in card, "старой подписи «по текущим турнирам» нет")


def test_other_league_not_mixed(cl) -> None:
    print("\n=== вторая лига не подмешивается ===")
    ib = cl.averages([{"row": 2}], [{"source": "infobasket", "season_id": "140825",
                                     "stage_id": ""}])
    check((ib.get(2) or {}).get("games") == 12,
          "в турнире Инфобаскета у того же человека свои 12 игр")
    both = cl.averages([{"row": 2}], cl.current_scopes())
    check((both.get(2) or {}).get("games") == 14,
          "по всем текущим турнирам выходит 14 — ровно то, что тренера путало")


def main() -> int:
    print(f"База: {TMP}")
    cl = setup()
    test_scope_of_game(cl)
    test_lineup_counts(cl)
    test_other_league_not_mixed(cl)
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("СТАРТОВАЯ ПЯТЁРКА: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
