#!/usr/bin/env python3
"""Состав на игру: гости не из листа и стартовая пятёрка.

    python3 tests/test_roster.py

База временная: сценарии заводят игры, составы и гостей. Проверяем то, чего
нельзя увидеть на экране, — что гость доходит до пятёрки, но не доходит до
денег, и что пятёрка не считает тех, кого из состава уже убрали.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="roster-test-")) / "bot.db"
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                            # noqa: E402
sheets_cache.DB_PATH = TMP

import coach_lineup                                            # noqa: E402
import game_roster                                             # noqa: E402

SRC, GID = "infobasket", "g-guest"
bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def seed() -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        for i, surname in enumerate(["Первый", "Второй", "Третий", "Четвёртый",
                                     "Пятый", "Шестой"], start=101):
            conn.execute(
                "INSERT OR REPLACE INTO players (row_index, surname, name, "
                "pay_game, pay_season, active_mark, synced_at) "
                "VALUES (?, ?, '', 900, 0, '1', ?)",
                (i, surname, datetime.now().isoformat(timespec="seconds")))
        conn.execute(
            "INSERT INTO game_roster_state (source, game_id, game_date, "
            "opponent, posted_at) VALUES (?, ?, ?, 'Соперник', ?)",
            (SRC, GID, (date.today() + timedelta(days=2)).isoformat(),
             datetime.now().isoformat(timespec="seconds")))
        conn.commit()
    for i in range(101, 107):
        game_roster.add(SRC, GID, i, by="test")


def test_guest_in_roster() -> None:
    """Человека не из листа можно дописать в состав."""
    print("\n=== гость в составе ===")
    was = len(game_roster.roster(SRC, GID))
    made = game_roster.add_guest(SRC, GID, "Гостев Иван", by="test")
    check(made.get("row", 0) < 0, f"у гостя свой отрицательный номер: {made}")

    now = game_roster.roster(SRC, GID)
    check(len(now) == was + 1, f"состав вырос: {was} → {len(now)}")
    one = next((p for p in now if p.get("guest")), None)
    check(bool(one) and one["title"] == "Гостев Иван", f"он в списке: {one}")
    check(one["surname"] == "Гостев", "фамилия разобрана — список сортируется")

    # Повтор не заводит второго. Регистр и лишние пробелы не в счёт: lower()
    # у SQLite кириллицу не знает, поэтому сравнение только в питоне.
    game_roster.add_guest(SRC, GID, "гостев  иван", by="test")
    guests = [p for p in game_roster.roster(SRC, GID) if p.get("guest")]
    check(len(guests) == 1, f"повтор не задвоил: {len(guests)}")


def test_guest_not_in_money() -> None:
    """Гость не попадает в долги: у него нет ни цены, ни телеграма.

    Требовать с него оплату бот не может и не должен — если деньги за него
    всё-таки ждут, для этого есть разовый долг со свободным именем."""
    print("\n=== гость мимо денег ===")
    debts = game_roster.game_debts()
    check(all(d["row"] > 0 for d in debts),
          f"в долгах только строки листа: {[d['row'] for d in debts]}")
    guest = next(p for p in game_roster.roster(SRC, GID) if p.get("guest"))
    check(game_roster.unpaid_games(guest["row"]) == [],
          "за гостем не числится игр")
    check(all(r["row"] > 0 for one in game_roster.debts_by_game()
              for r in one["rows"]), "и в разбивке по играм тоже")


def test_guest_to_start() -> None:
    """Гостя можно поставить в старт и снять обратно."""
    print("\n=== гость в стартовой пятёрке ===")
    guest = next(p for p in game_roster.roster(SRC, GID) if p.get("guest"))
    ok, note = coach_lineup.toggle_start(SRC, GID, guest["row"])
    check(ok, f"поставился в старт: {note}")
    check(guest["row"] in coach_lineup.start_five(SRC, GID), "он в пятёрке")

    data = coach_lineup.lineup(SRC, GID)
    card = " ".join(coach_lineup.player_card(
        next(r for r in data["rows"] if r["row"] == guest["row"])))
    check("гость" in card.lower(), f"в карточке помечен гостем: {card[:70]}")
    check("заявке" not in card, "и не обвинён в отсутствии в заявке лиги")

    ok, note = coach_lineup.toggle_start(SRC, GID, guest["row"])
    check(not ok, f"снялся обратно: {note}")
    check(guest["row"] not in coach_lineup.start_five(SRC, GID), "его нет в пятёрке")


def test_start_counts_only_live() -> None:
    """Пятёрка не считает тех, кого убрали из состава.

    На боевых данных нашлась ровно эта картина: в пятёрке числилось пятеро, из
    них один давно снят с игры. Тренер видел четверых и не мог добавить
    пятого — бот отвечал «в старте уже 5»."""
    print("\n=== снятый из состава не занимает место в пятёрке ===")
    for row in (101, 102, 103, 104, 105):
        coach_lineup.toggle_start(SRC, GID, row)
    check(len(coach_lineup.start_five(SRC, GID)) == 5, "пятёрка набрана")

    game_roster.remove(SRC, GID, 105)                 # передумали брать
    shown = coach_lineup.lineup(SRC, GID)["start"]
    check(len(shown) == 4, f"на экране четверо: {len(shown)}")

    ok, note = coach_lineup.toggle_start(SRC, GID, 106)
    check(ok, f"место освободилось, шестой встал: {note}")
    check(105 not in coach_lineup.start_five(SRC, GID),
          "снятый из состава вычищен из пятёрки")


def test_rename_guest() -> None:
    """Имя гостя можно поправить, не убирая его из состава."""
    print("\n=== правка имени гостя ===")
    guest = next(p for p in game_roster.roster(SRC, GID) if p.get("guest"))
    was = len(game_roster.roster(SRC, GID))

    ok = game_roster.rename_guest(SRC, GID, guest["row"], "  Гостев   Пётр ")
    check(ok, "переименование прошло")
    now = next(p for p in game_roster.roster(SRC, GID) if p.get("guest"))
    check(now["title"] == "Гостев Пётр", f"имя поправлено: {now['title']}")
    check(now["row"] == guest["row"], "номер тот же — связи не порвались")
    check(len(game_roster.roster(SRC, GID)) == was, "из состава не выпал")

    check(not game_roster.rename_guest(SRC, GID, guest["row"], "   "),
          "пустое имя не принимаем")
    check(not game_roster.rename_guest(SRC, GID, -99999, "Кто-то"),
          "чужой номер не переименовать")


def test_roster_reads_sheet_once() -> None:
    """Состав не перечитывает лист на каждого человека.

    Здесь стоял player_by_row() в цикле, а он читает лист целиком: двенадцать
    человек в заявке — двенадцать полных чтений. На экранах фэнтези эта
    функция зовётся по разу на игру, и ожидание складывалось в паузу при
    открытии приложения."""
    print("\n=== состав читает лист один раз ===")
    import coach_payments
    calls = {"n": 0}
    real = coach_payments.players

    def counted():
        calls["n"] += 1
        return real()

    coach_payments.players = counted
    try:
        rows = game_roster.roster(SRC, GID)
    finally:
        coach_payments.players = real
    check(calls["n"] <= 1, f"лист прочитан не больше одного раза: {calls['n']}")
    check(len(rows) > 0, f"состав при этом собран: {len(rows)}")


def test_votes_of_another_match_do_not_leak() -> None:
    """Голоса чужого матча не попадают в состав, и один человек — один раз.

    Жалоба 04.09.2026: в списке «отметились, но не в составе» Шлепикас Роман
    стоял дважды. Лига переприсваивает номера игр, и под id 1086119 накопились
    два опроса — 12 голосов за 01.09 и 17 за 05.09. Экран складывал оба."""
    print("\n=== голоса двух матчей под одним номером ===")
    import game_roster
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_votes WHERE game_id = 'dup-1'")
        for poll, day, uid in (("p1", "2026-09-01", "700001"),
                               ("p2", "2026-09-05", "700001"),
                               ("p2", "2026-09-05", "700002")):
            conn.execute(
                "INSERT INTO game_votes (tg_poll_id, user_id, username, "
                "first_name, last_name, vote_text, vote_type, game_id, "
                "game_date, updated_at, synced_at) VALUES (?, ?, '', ?, '', "
                "'✅ Готов', ?, 'dup-1', ?, ?, ?)",
                (poll, uid, "Имя" + uid[-1], game_roster.VOTE_READY, day, now, now))
        conn.commit()

    both = game_roster.voters("dup-1")
    check(len(both) == 2, f"без даты — по одному на человека: {len(both)}")

    fifth = game_roster.voters("dup-1", game_date="2026-09-05")
    check(len(fifth) == 2, f"за 05.09 двое: {len(fifth)}")
    first = game_roster.voters("dup-1", game_date="2026-09-01")
    check(len(first) == 1, f"за 01.09 один: {len(first)}")
    check([v["user_id"] for v in first] == ["700001"], "и это тот, кто голосовал")

    # Дата, под которую голосов нет: показываем всё, а не пустоту.
    none_day = game_roster.voters("dup-1", game_date="2026-12-31")
    check(len(none_day) == 2, "под неизвестную дату показываем всех, а не никого")

    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_votes WHERE game_id = 'dup-1'")
        conn.commit()


def main() -> int:
    print(f"База: {TMP}")
    seed()
    test_guest_in_roster()
    test_votes_of_another_match_do_not_leak()
    test_guest_not_in_money()
    test_guest_to_start()
    test_rename_guest()
    test_start_counts_only_live()
    test_roster_reads_sheet_once()

    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("СОСТАВ И ПЯТЁРКА: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
