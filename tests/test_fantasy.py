#!/usr/bin/env python3
"""Фэнтези: за что человеку начислены очки и что показано в разбивке.

    python3 tests/test_fantasy.py

База временная. Проверяем ровно то, на что пожаловался пользователь 17.08.2026:
в шапке «1 игра в зачёте», а под ней три игры, две из них по нулю.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="fantasy-test-")) / "bot.db"
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                            # noqa: E402
sheets_cache.DB_PATH = TMP

import fantasy                                                 # noqa: E402

DAY = "2026-08-16"
SEASON = 1
bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def seed() -> None:
    """Один игровой день, три игры: наша и две чужие."""
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT INTO fantasy_seasons (id, name, format, status, started_at) "
            "VALUES (?, 'Тест', '3x3', 'active', ?)", (SEASON, DAY))
        # В нашей игре играют наши, в чужих — чужие.
        for gid, players in (("our", ["p1", "p2", "p3"]),
                             ("alien-1", ["x1", "x2"]),
                             ("alien-2", ["y1", "y2"])):
            for pid in players:
                conn.execute(
                    "INSERT INTO game_player_stats (source, game_id, game_date, "
                    "player_id, team_id, pts, reb, ast, stl, blk, tur, "
                    # Источник пишем так, как его видит парсер ссылок: «ib» в
                    # ссылке состава разворачивается в «infobasket».
                    "fetched_at) VALUES ('infobasket', ?, ?, ?, '1', 10, 5, 2, "
                    "1, 0, 1, ?)",
                    (gid, DAY, pid, now))
        # Состав участника на ту неделю: трое наших.
        wk = fantasy.week_start_of(date.fromisoformat(DAY)).isoformat()
        conn.execute(
            "INSERT INTO fantasy_rosters (season_id, user_id, week_start, "
            "player_refs_json, mode, updated_at) VALUES (?, '777', ?, ?, '', ?)",
            (SEASON, wk, json.dumps(["ib:1:p1", "ib:1:p2", "ib:1:p3"]), now))
        conn.commit()


def test_no_snapshot_for_alien_games() -> None:
    """Снимок пишется только по играм, где у человека кто-то был.

    Иначе участник копит строки «0 очков» за матчи чужих команд: на боевых
    данных таким мусором оказались 652 записи из 747."""
    print("\n=== чужие игры не попадают в зачёт ===")
    season = fantasy._get_season(SEASON)
    made = fantasy.backfill_game_scores(season)
    check(made > 0, f"бэкфилл отработал: игр разобрано {made}")

    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT game_id, points FROM fantasy_game_scores WHERE user_id='777'")]
    ids = sorted(r["game_id"] for r in rows)
    check(ids == ["our"], f"снимок только по своей игре: {ids}")
    check(rows and rows[0]["points"] > 0, f"очки начислены: {rows}")


def test_breakdown_matches_mode() -> None:
    """Разбивка показывает игры того же режима, что и шапка.

    Жалоба 17.08.2026: таблица «Бюджет» считала одну игру, а показывала три —
    две из них были снимками свободного режима."""
    print("\n=== разбивка не смешивает режимы ===")
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        # Тот же человек, тот же день: игра в бюджете и игра в свободном.
        for gid, mode, pts in (("g-budget", "budget", 82.7),
                               ("g-free", "free", 12.0)):
            conn.execute(
                "INSERT INTO fantasy_game_scores (user_id, season_id, source, "
                "game_id, game_date, points, mode, refs_json, computed_at) "
                "VALUES ('888', ?, 'infobasket', ?, ?, ?, ?, ?, ?)",
                (SEASON, gid, DAY, pts, mode, json.dumps(["ib:1:p1"]), now))
        conn.commit()

    rows = fantasy.top_participants(SEASON, DAY, DAY, 10, mode="budget")
    one = next((r for r in rows if r["user_id"] == "888"), None)
    check(bool(one), "участник в таблице бюджета есть")
    check(one["games"] == 1, f"в зачёте одна игра: {one['games']}")
    check(len(one["picks"]) == 1,
          f"и в разбивке одна строка, а не три: {len(one['picks'])}")
    check(abs(one["points"] - 82.7) < 0.01, f"очки бюджетные: {one['points']}")

    rows = fantasy.top_participants(SEASON, DAY, DAY, 10, mode="free")
    one = next((r for r in rows if r["user_id"] == "888"), None)
    check(one and len(one["picks"]) == 1 and abs(one["points"] - 12.0) < 0.01,
          f"в свободном — своя строка: {one['points'] if one else None}")


def test_prune_keeps_real_zeros() -> None:
    """Чистка убирает мусор, но не трогает настоящий ноль.

    Ноль от игрока, который вышел и ничего не набрал, — факт. Ноль за матч,
    где его не было, — нет."""
    print("\n=== чистка отличает ноль от отсутствия ===")
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        # Мусор: ноль за чужую игру.
        conn.execute(
            "INSERT INTO fantasy_game_scores (user_id, season_id, source, "
            "game_id, game_date, points, mode, refs_json, computed_at) "
            "VALUES ('999', ?, 'infobasket', 'alien-1', ?, 0, '', ?, ?)",
            (SEASON, DAY, json.dumps(["ib:1:p1"]), now))
        # Настоящий ноль: игрок в протоколе своей игры есть.
        conn.execute(
            "INSERT INTO fantasy_game_scores (user_id, season_id, source, "
            "game_id, game_date, points, mode, refs_json, computed_at) "
            "VALUES ('999', ?, 'infobasket', 'our', ?, 0, '', ?, ?)",
            (SEASON, DAY, json.dumps(["ib:1:p1"]), now))
        conn.commit()

    res = fantasy.prune_absent_snapshots(SEASON, apply=False)
    check(res["rows"] >= 1 and not res["applied"], f"посчитано без удаления: {res}")

    fantasy.prune_absent_snapshots(SEASON, apply=True)
    with sheets_cache.get_connection() as conn:
        left = sorted(r["game_id"] for r in conn.execute(
            "SELECT game_id FROM fantasy_game_scores WHERE user_id='999'"))
    check(left == ["our"], f"мусор убран, настоящий ноль остался: {left}")


def test_game_pick_beats_week() -> None:
    """Ставка на игру перекрывает недельный состав, но не отменяет его.

    Недельный остаётся запасным: кто ставку не сделал, доигрывает на нём —
    иначе переход на поигровую модель обнулил бы всех посреди сезона."""
    print("\n=== ставка на игру сильнее недельного состава ===")
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        # Двое: один со ставкой на игру, второй только с недельным составом.
        wk = fantasy.week_start_of(date.fromisoformat(DAY)).isoformat()
        for uid in ("111", "222"):
            conn.execute(
                "INSERT OR REPLACE INTO fantasy_rosters (season_id, user_id, "
                "week_start, player_refs_json, mode, updated_at) "
                "VALUES (?, ?, ?, ?, '', ?)",
                (SEASON, uid, wk, json.dumps(["ib:1:p1"]), now))
        conn.commit()
    # У 111 ставка на нашу игру — из ДРУГИХ игроков.
    fantasy.set_game_pick("111", SEASON, "infobasket", "our",
                          ["ib:1:p2", "ib:1:p3"])

    season = fantasy._get_season(SEASON)
    fantasy.record_game_scores(season, "infobasket", "our", DAY, inherit=False)
    with sheets_cache.get_connection() as conn:
        got = {r["user_id"]: json.loads(r["refs_json"]) for r in conn.execute(
            "SELECT user_id, refs_json FROM fantasy_game_scores "
            "WHERE game_id='our' AND user_id IN ('111','222')")}
    check(got.get("111") == ["ib:1:p2", "ib:1:p3"],
          f"у поставившего в снимок легла ставка: {got.get('111')}")
    check(got.get("222") == ["ib:1:p1"],
          f"у остальных — недельный состав: {got.get('222')}")

    # Снятая ставка возвращает человека на недельный состав.
    fantasy.set_game_pick("111", SEASON, "infobasket", "our", [])
    check("111" not in fantasy.game_picks_by_user(SEASON, "infobasket", "our"),
          "снятая ставка не участвует в подсчёте")


def test_declared_bridge() -> None:
    """Состав тренера сводится с пулом по ФИО, несведённые — видны.

    Тренер ведёт состав строками листа, пул адресуется id лиги, общего ключа
    нет. Молча спрятать неопознанного нельзя: участник будет гадать, почему
    игрока из состава нет в списке."""
    print("\n=== заявка тренера сводится с пулом ===")
    import fantasy_api as fa
    check(fa._same_person("Шлепикас Роман", "Роман Шлепикас"),
          "порядок слов не мешает")
    check(fa._same_person("Лысюк Денис", "Лисюк Денис"),
          "буква в фамилии не мешает")
    check(not fa._same_person("Долгих Денис", "Долгих Владислав"),
          "братьев не путаем")
    check(not fa._same_person("Иванов Иван", "Иванов"),
          "неполное ФИО совпадением не считаем")


def test_stats_cache_sees_the_list() -> None:
    """Кеш обогащённого пула ключуется по СОСТАВУ списка, а не только по сезону.

    Ключом был один сезон — пул считался «одинаковым для всех». С появлением
    ставок на игру это перестало быть правдой: экран матча передаёт заявку
    тренера, а получал из кеша полный ростер лиги. Фильтр считался верно и тут
    же выбрасывался, а снаружи выглядело, будто он не работает."""
    print("\n=== кеш не подменяет заявку полным ростером ===")
    import fantasy_api as fa
    full = [{"ref": f"ib:1:p{i}", "name": f"Игрок{i}"} for i in range(24)]
    part = full[:12]
    season = {"id": SEASON}

    got_full = fa.pool_with_stats_cached(full, season)
    got_part = fa.pool_with_stats_cached(part, season)
    got_again = fa.pool_with_stats_cached(full, season)

    check(len(got_full) == 24, f"полный пул отдан целиком: {len(got_full)}")
    check(len(got_part) == 12, f"заявка на игру не подменена: {len(got_part)}")
    check(len(got_again) == 24, f"полный пул из кеша не испорчен: {len(got_again)}")
    # Кеш обязан продолжать работать: второй одинаковый запрос — тот же объект.
    check(got_again is fa.pool_with_stats_cached(full, season),
          "повторный запрос берётся из кеша, а не считается заново")


def main() -> int:
    print(f"База: {TMP}")
    seed()
    test_no_snapshot_for_alien_games()
    test_breakdown_matches_mode()
    test_prune_keeps_real_zeros()
    test_game_pick_beats_week()
    test_declared_bridge()
    test_stats_cache_sees_the_list()

    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ФЭНТЕЗИ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
