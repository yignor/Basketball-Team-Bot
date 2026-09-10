#!/usr/bin/env python3
"""Отчество для заявки и глубокий инактив.

    python3 tests/test_gone_patronymic.py

Отчество: сверка с лигой строгая — лишнее отчество уйдёт в заявку, и вписанное
тренером не перезаписывается. Инактив: ушедший пропадает из сборов, заявок,
рассылок и списков, но строка и членство в группах остаются, и возврат —
одной кнопкой.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

TMP = Path(tempfile.mkdtemp(prefix="gone-test-")) / "bot.db"
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
        for row, sur, name, born in ((2, "Гиря", "Максим", "2003-04-10"),
                                     (3, "Иванов", "Иван", "1990-01-01"),
                                     (4, "Иванов", "Пётр", ""),
                                     (5, "Ушедший", "Игрок", "1995-05-05")):
            conn.execute(
                "INSERT INTO players (row_index, surname, name, birthday, "
                "active_mark, pay_season, pay_game, synced_at) VALUES "
                "(?, ?, ?, ?, '1', 5500, 900, ?)", (row, sur, name, born, now))
        conn.commit()
    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None
    return bd


def test_patronymic_match() -> None:
    print("\n=== отчество: строгая сверка ===")
    import coach_payments
    import patronymics

    league = [
        {"last": "Гиря", "first": "Максим", "second": "Дмитриевич", "birth": "10.04.2003"},
        # Тёзка с другой датой рождения — это другой человек.
        {"last": "Иванов", "first": "Иван", "second": "Петрович", "birth": "05.05.1985"},
        {"last": "Иванов", "first": "Пётр", "second": "Сергеевич", "birth": "01.02.1999"},
    ]
    people = coach_payments.players()
    found, missing = patronymics.match(people, league)
    check(found.get(2) == "Дмитриевич", f"имя и дата сошлись — вписали: {found.get(2)}")
    check(3 not in found, "тёзка с другой датой рождения — не наш, не вписали")
    check(found.get(4) == "Сергеевич",
          "дата неизвестна у нас — хватает фамилии и имени")
    check("Ушедший Игрок" in missing, "кого нет в лиге — в списке «не нашёл»")

    # Вписанное тренером не перезаписываем.
    people[0]["patronymic"] = "Правленый"
    again, _ = patronymics.match(people, league)
    check(2 not in again, "у кого уже стоит отчество — не трогаем")


def test_patronymic_in_export() -> None:
    print("\n=== отчество в заявке ===")
    import roster_export
    import sheets_cache
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE players SET patronymic = 'Дмитриевич' WHERE row_index = 2")
        conn.commit()
    table = roster_export.rows(roster_export.team_people())
    check(table[0][3] == "Отчество", f"столбец после имени: {table[0][:5]}")
    line = next(l for l in table[1:] if l[1] == "Гиря")
    check(line[3] == "Дмитриевич", "отчество попало в строку")


def test_gone_everywhere() -> None:
    print("\n=== ушёл из команды ===")
    import coach_payments
    import player_groups as pg
    import roster_export
    import sheets_cache
    import training_dues as td

    gid, _ = pg.create("Основа")
    pg.add(gid, 5)
    pg.add(gid, 2)

    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE players SET status = ?, active_mark = '' "
                     "WHERE row_index = 5", (coach_payments.GONE_STATUS,))
        conn.commit()

    me = coach_payments.player_by_row(5) or {}
    check(me.get("gone"), "признак ухода распознан")
    check(me.get("title") == "Ушедший Игрок",
          "но в карточке он есть — имя в истории платежей не пропадёт")

    period = td.FIRST_PERIOD
    check(all(int(r["row"]) != 5 for r in td.status(period, True)),
          "«будешь заниматься?» ему не уходит")
    check(all(int(r["row"]) != 5 for r in td.debtors(period)),
          "и взноса с него не ждём")
    check(all(int(p["row"]) != 5 for p in roster_export.team_people()),
          "в заявке его нет")
    check([int(p["row"]) for p in pg.members(gid)] == [2],
          "в составе группы для рассылки — нет")
    check(5 in pg.member_rows(gid),
          "но членство в группе сохранено — вернётся, и будет там же")

    import game_roster
    check(all(p["row"] != 5 for p in game_roster.search("Ушедший")),
          "в состав на игру не предлагается")

    # Возврат: статус пустой, активность стоит — и он снова везде.
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE players SET status = '', active_mark = '1' "
                     "WHERE row_index = 5")
        conn.commit()
    check(any(int(r["row"]) == 5 for r in td.status(period, True)),
          "вернули — снова в вопросе про месяц")
    check(sorted(int(p["row"]) for p in pg.members(gid)) == [2, 5],
          "и в группе, куда его ставили раньше")


def test_temporary_is_not_gone() -> None:
    """«Неактивен» и «заморожен» — пауза, а не уход."""
    print("\n=== временно — не значит ушёл ===")
    import coach_payments
    for word in ("неактивен", "заморожен", ""):
        check(not coach_payments.is_gone(word), f"«{word}» — не уход")
    for word in ("Ушёл из команды", "ушел", "выбыл", "архив"):
        check(coach_payments.is_gone(word), f"«{word}» — уход")


def test_leave_asks_and_card_offers_return(bd) -> None:
    print("\n=== кнопки в карточке ===")
    import coach_payments
    import sheets_cache
    text, markup = bd._field_card(2, "coach:field")
    cbs = [b.callback_data for row in markup.inline_keyboard for b in row]
    check("coach:field:leave:2" in cbs, "у действующего — «Ушёл из команды»")

    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE players SET status = ? WHERE row_index = 2",
                     (coach_payments.GONE_STATUS,))
        conn.commit()
    text, markup = bd._field_card(2, "coach:field")
    cbs = [b.callback_data for row in markup.inline_keyboard for b in row]
    check("coach:field:back:2" in cbs, "у ушедшего — «Вернуть в команду»")
    check("Ушёл из команды" in text, "и на карточке это сказано")

    screen, markup = bd._gone_screen("coach:field")
    cbs = [b.callback_data for row in markup.inline_keyboard for b in row]
    check("coach:field:pick:2" in cbs, "в списке ушедших он есть")

    listed, markup = bd._fields_screen(0, "coach:field")
    cbs = [b.callback_data for row in markup.inline_keyboard for b in row]
    check("coach:field:gone:0" in cbs, "из общего списка есть вход в «Ушедшие»")
    check("coach:field:pick:2" not in cbs, "а в общем списке его нет")
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE players SET status = '' WHERE row_index = 2")
        conn.commit()


def main() -> int:
    print(f"База: {TMP}")
    bd = setup()
    test_patronymic_match()
    test_patronymic_in_export()
    test_gone_everywhere()
    test_temporary_is_not_gone()
    test_leave_asks_and_card_offers_return(bd)
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ОТЧЕСТВО И ИНАКТИВ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
