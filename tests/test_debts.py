#!/usr/bin/env python3
"""Долги за игры: состав объявили — люди обязаны появиться в должниках.

    python3 tests/test_debts.py

База временная и своя: сценарии заводят игры и составы, а делать это на боевой
базе нельзя. Зато можно проверить ровно то, что случилось 11.08.2026, — когда
одиннадцать человек из объявленного состава молча не попали в долги.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="debts-test-")) / "bot.db"
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                            # noqa: E402
sheets_cache.DB_PATH = TMP

import game_roster                                             # noqa: E402

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def seed_player(row: int, surname: str, price: int = 900) -> None:
    """Игрок в зеркале листа «Игроки» — с ценой игры, иначе он не должник."""
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO players (row_index, surname, name, "
            "pay_game, pay_season, active_mark, synced_at) "
            "VALUES (?, ?, '', ?, 0, '1', ?)",
            (row, surname, price, datetime.now().isoformat(timespec="seconds")))
        conn.commit()


def post_roster(source: str, game_id: str, rows: List[int],
                game_date: str, posted_at: str = "") -> None:
    """Объявленный состав: люди в игре плюс строка состояния с публикацией."""
    for r in rows:
        game_roster.add(source, game_id, r, by="test")
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT INTO game_roster_state (source, game_id, game_date, "
            "opponent, posted_at) VALUES (?, ?, ?, '', ?) "
            "ON CONFLICT(source, game_id) DO UPDATE SET "
            "game_date = excluded.game_date, posted_at = excluded.posted_at",
            (source, game_id, game_date,
             posted_at or datetime.now().isoformat(timespec="seconds")))
        conn.commit()


def owed_by(row: int) -> int:
    return next((d["games"] for d in game_roster.game_debts()
                 if d["row"] == row), 0)


# ─────────────────────────── сценарии ──────────────────────────────────────


def test_posted_roster_counts() -> None:
    """Объявили состав — все в нём должны за игру."""
    print("\n=== объявленный состав попадает в долги ===")
    sheets_cache.init_db()
    day = (date.today() + timedelta(days=3)).isoformat()
    for i, name in enumerate(["Первый", "Второй", "Третий"], start=101):
        seed_player(i, name)
    post_roster("infobasket", "g-normal", [101, 102, 103], day)

    debts = game_roster.game_debts()
    got = {d["row"] for d in debts}
    check(got == {101, 102, 103}, f"должны все трое: {sorted(got)}")
    check(all(d["amount"] == 900 for d in debts),
          f"по цене игры: {[d['amount'] for d in debts]}")


def test_no_date_still_counts() -> None:
    """Игра без даты не должна терять весь состав.

    Так и случилось 11.08.2026: строку состояния создал первый же шаг работы
    с составом (форма, публикация, пятёрка), а даты он не знает. Пустая дата
    не проходила порог «считаем с такого-то числа», и одиннадцать человек
    просто не появились в долгах — молча, без единой ошибки в журнале."""
    print("\n=== состав без даты игры ===")
    for i, name in enumerate(["Четвёртый", "Пятый"], start=201):
        seed_player(i, name)
    posted = datetime.now().isoformat(timespec="seconds")
    post_roster("slpro", "slpro-m-nodate", [201, 202], "", posted_at=posted)

    check(owed_by(201) == 1 and owed_by(202) == 1,
          f"без даты состав всё равно считается: {owed_by(201)}, {owed_by(202)}")

    played = game_roster._played_games(201)
    dates = [g[2] for g in played]
    check(all(d for d in dates), f"дата подставлена из публикации: {dates}")


def test_ensure_state_fills_date() -> None:
    """Дата дописывается в уже созданную строку, а не игнорируется."""
    print("\n=== дата дописывается позже ===")
    game_roster.set_form("infobasket", "g-late", "dark")     # строка без даты
    with sheets_cache.get_connection() as conn:
        was = conn.execute("SELECT game_date FROM game_roster_state WHERE "
                           "game_id = 'g-late'").fetchone()["game_date"]
    check(not was, f"строку создали без даты: {was!r}")

    day = date.today() + timedelta(days=5)
    game_roster.ensure_state({"source": "infobasket", "game_id": "g-late",
                              "date": day, "opponent": "Кто-то"})
    with sheets_cache.get_connection() as conn:
        now = conn.execute("SELECT game_date, opponent, form FROM "
                           "game_roster_state WHERE game_id = 'g-late'").fetchone()
    check(now["game_date"] == day.isoformat(), f"дата дописалась: {now['game_date']}")
    check(now["form"] == "dark", "выбранная форма не затёрта")

    # Повторный вызов с другой датой не должен переписывать известную.
    game_roster.ensure_state({"source": "infobasket", "game_id": "g-late",
                              "date": day + timedelta(days=1), "opponent": "Другой"})
    with sheets_cache.get_connection() as conn:
        again = conn.execute("SELECT game_date, opponent FROM game_roster_state "
                             "WHERE game_id = 'g-late'").fetchone()
    check(again["game_date"] == day.isoformat(), "известную дату не переписали")
    check(again["opponent"] == "Кто-то", "и соперника тоже")


def test_repair_from_service_records() -> None:
    """Починка старых строк берёт дату из служебной записи об игре."""
    print("\n=== починка пустых дат ===")
    day = (date.today() + timedelta(days=4)).isoformat()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT INTO game_roster_state (source, game_id, game_date, "
            "opponent, posted_at) VALUES ('slpro', 'g-repair', '', '', ?)",
            (datetime.now().isoformat(timespec="seconds"),))
        conn.execute(
            "INSERT INTO service_records (data_type, unique_key, game_id, "
            "game_date, status, logged_at, created_at, updated_at) "
            "VALUES ('ОПРОС_ИГРА_SLPRO', 'k-repair', 'g-repair', ?, "
            "'ОПРОС СОЗДАН (тренер)', ?, ?, ?)",
            (day, datetime.now().strftime("%d.%m.%Y %H:%M"),
             sheets_cache.now_iso(), sheets_cache.now_iso()))
        conn.commit()

    fixed = game_roster.repair_dates()
    with sheets_cache.get_connection() as conn:
        now = conn.execute("SELECT game_date FROM game_roster_state WHERE "
                           "game_id = 'g-repair'").fetchone()["game_date"]
    check(fixed >= 1 and now == day, f"дата восстановлена: {now} (починено {fixed})")
    check(game_roster.repair_dates() == 0, "второй раз чинить нечего")


def test_roster_change_moves_debt() -> None:
    """Состав поменялся — долги обязаны поехать за ним.

    Требование тренера: состав меняется до последнего, и человек, которого
    убрали, не должен остаться должен за игру, где его не было. Считаем долги
    по живому составу, а не по снимку на момент публикации, — поэтому проверка
    именно на изменение уже объявленного состава."""
    print("\n=== состав изменился — долги пересчитались ===")
    day = (date.today() + timedelta(days=2)).isoformat()
    for i, name in enumerate(["Шестой", "Седьмой", "Восьмой"], start=301):
        seed_player(i, name)
    post_roster("infobasket", "g-change", [301, 302], day)
    check(owed_by(301) == 1 and owed_by(302) == 1 and owed_by(303) == 0,
          f"исходно двое: {owed_by(301)}, {owed_by(302)}, {owed_by(303)}")

    game_roster.add("infobasket", "g-change", 303, by="test")
    check(owed_by(303) == 1, f"дописали третьего — он должен: {owed_by(303)}")

    game_roster.remove("infobasket", "g-change", 301)
    check(owed_by(301) == 0, f"убрали первого — долг снят: {owed_by(301)}")
    check(owed_by(302) == 1 and owed_by(303) == 1, "остальных не задело")

    # Снимок публикации на подсчёт влиять не должен — он про другое.
    with sheets_cache.get_connection() as conn:
        snap = conn.execute("SELECT posted_rows FROM game_roster_state WHERE "
                            "game_id = 'g-change'").fetchone()["posted_rows"]
    check("301" not in str(snap) or owed_by(301) == 0,
          "долг считается по живому составу, а не по снимку")


def test_paid_closes_debt() -> None:
    """Оплата закрывает игру — иначе долг висел бы вечно."""
    print("\n=== оплата гасит долг ===")
    import coach_payments
    check(owed_by(101) == 1, "до оплаты должен")
    coach_payments.record(101, 900, coach_payments.KIND_GAME, 1,
                          paid_at=date.today().isoformat(), bank="", note="тест",
                          added_by="test", fp="", game_ref="infobasket:g-normal",
                          by_coach=True)
    check(owed_by(101) == 0, f"после оплаты не должен: {owed_by(101)}")
    check(owed_by(102) == 1, "у соседа долг на месте")


def test_mark_paid_is_idempotent() -> None:
    """За одну игру нельзя заплатить дважды, сколько ни жми.

    Жалоба 12.08.2026: составы объявлены, а долгов нет. Оказалось — пятеро
    числились оплатившими игру 09.08 по два раза, и лишние оплаты съели их
    долги за будущие игры. Отпечаток собирался из суммы и текущей МИНУТЫ, то
    есть защищал только от двойного нажатия подряд; нажатие через минуту
    заводило второй платёж."""
    print("\n=== повторная отметка оплаты ===")
    day = (date.today() + timedelta(days=6)).isoformat()
    seed_player(401, "Девятый")
    seed_player(402, "Десятый")
    post_roster("infobasket", "g-twice", [401, 402], day)
    post_roster("infobasket", "g-other", [401], day)

    game_roster.mark_paid(401, "infobasket", "g-twice", by="test")
    check(owed_by(401) == 1, f"после первой отметки осталась одна игра: {owed_by(401)}")

    again = game_roster.mark_paid(401, "infobasket", "g-twice", by="test")
    check(again.get("duplicate") is True, "повтор распознан как повтор")
    check(owed_by(401) == 1, f"долг не съеден повтором: {owed_by(401)}")

    # Другая игра тому же человеку — это уже не повтор.
    game_roster.mark_paid(401, "infobasket", "g-other", by="test")
    check(owed_by(401) == 0, f"вторая игра оплачена отдельно: {owed_by(401)}")
    check(owed_by(402) == 1, "соседа не задело")


def test_duplicate_marks_found() -> None:
    """Старые дубли находятся — иначе их не вычистить."""
    print("\n=== поиск старых дублей ===")
    check(game_roster.duplicate_marks() == [], "на чистых данных дублей нет")

    import coach_payments
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT id, player_row, game_ref FROM payments WHERE kind = ? "
            "AND COALESCE(game_ref,'') != '' LIMIT 1",
            (coach_payments.KIND_GAME,)).fetchone()
        conn.execute(
            "INSERT INTO payments (player_row, amount, kind, games, paid_at, "
            "bank, note, added_by, created_at, fingerprint, pushed, period, "
            "game_ref, by_coach) VALUES (?, 900, ?, 1, ?, '', 'дубль', 'test', "
            "?, ?, 0, '', ?, 1)",
            (row["player_row"], coach_payments.KIND_GAME,
             date.today().isoformat(), datetime.now().isoformat(timespec="seconds"),
             "fp-dup-test", row["game_ref"]))
        conn.commit()

    dups = game_roster.duplicate_marks()
    check(len(dups) == 1 and dups[0]["game_ref"] == row["game_ref"],
          f"дубль найден: {[(d['player_row'], d['game_ref']) for d in dups]}")
    check(dups[0]["id"] != row["id"], "самая ранняя запись остаётся, лишняя видна")


def main() -> int:
    print(f"База: {TMP}")
    test_posted_roster_counts()
    test_no_date_still_counts()
    test_ensure_state_fills_date()
    test_repair_from_service_records()
    test_roster_change_moves_debt()
    test_paid_closes_debt()
    test_mark_paid_is_idempotent()
    test_duplicate_marks_found()

    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ДОЛГИ ЗА ИГРЫ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
