#!/usr/bin/env python3
"""Правка листа «Игроки» через бота: все поля и заведение нового.

    python3 tests/test_players_edit.py

Google-таблицу подменяем заглушкой: проверяем не gspread, а свою логику —
какие поля вообще правятся, что попадает в зеркало и что бот отказывается
делать. Отказы тут важнее записей: испорченный лист чинится руками.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))

import sheets_cache                                             # noqa: E402
sheets_cache.DB_PATH = Path(tempfile.mkdtemp()) / "bot.db"

HEAD = ["Фамилия", "Имя", "Ник", "Дата рождения", "Статус", "Команда",
        "Активность", "Оплата сезона", "Оплата игры", "Стоимость", "Уровень",
        "Амплуа", "Числовой TG ID"]
bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


class FakeWS:
    """Лист: заголовок и строки. Пишет то же, что писал бы gspread."""

    def __init__(self, rows: List[List[str]]):
        self.rows = [HEAD] + rows
        self.appended: List[List[str]] = []
        self.updated: List[Any] = []

    def row_values(self, n): return self.rows[n - 1]
    def get_all_values(self): return self.rows
    def append_row(self, line, value_input_option=""): self.appended.append(line)

    def update_cell(self, row, col, value):
        self.updated.append((row, col, value))
        while len(self.rows[row - 1]) < col:
            self.rows[row - 1].append("")
        self.rows[row - 1][col - 1] = value

    def col_values(self, col):
        return [r[col - 1] if col - 1 < len(r) else "" for r in self.rows]


class FakeBook:
    def __init__(self, ws): self.ws = ws
    def worksheet(self, name): return self.ws


def seed_mirror() -> None:
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM players")
        conn.execute("INSERT INTO players (row_index, surname, name, synced_at) "
                     "VALUES (2, 'Иванов', 'Иван', ?)", (now,))
        conn.commit()


def test_all_columns_editable() -> None:
    """Правится весь лист, а не две колонки.

    Раньше через бота менялись только дата рождения и ник — за остальным
    тренер лез в таблицу."""
    print("\n=== правятся все поля листа ===")
    keys = set(sheets_cache.PLAYER_FIELDS)
    for need in ("surname", "name", "nick", "bd", "role", "team", "status",
                 "active", "season", "game", "price", "tier"):
        check(need in keys, f"поле «{need}» доступно для правки")

    # Порядок на карточке берётся из той же карты — второго списка быть не должно.
    import bot_daemon as bd
    check(set(bd.FIELD_ORDER) <= keys,
          f"карточка не знает лишних полей: {set(bd.FIELD_ORDER) - keys}")


def test_card_shows_every_field() -> None:
    """Карточка показывает ЗНАЧЕНИЯ всех полей, а не только их названия.

    Проверка на боевых 20.08.2026 поймала: «Стоимость» и «Уровень» стояли
    прочерком при любом содержимом листа — список игроков этих двух колонок
    просто не читал. Кнопка правки при этом работала, и дефект был незаметен."""
    print("\n=== на карточке видно значения, а не прочерки ===")
    import bot_daemon as bd
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM players")
        conn.execute(
            "INSERT INTO players (row_index, surname, name, nickname, birthday, "
            "role, status, team, active_mark, pay_season, pay_game, price, tier, "
            "synced_at) VALUES (2,'Абрамов','Платон','plat','2001-09-22','центровой',"
            "'основа','Farm','+',5500,900,55,'Золото',?)", (now,))
        conn.commit()

    text, markup = bd._field_card(2)
    for value in ("plat", "2001-09-22", "центровой", "основа", "Farm",
                  "5500", "900", "55", "Золото"):
        check(value in text, f"«{value}» видно на карточке")
    check(text.count("—") == 0, f"прочерков нет ни у одного заполненного поля:\n{text}")
    check(sum(len(r) for r in markup.inline_keyboard) == len(bd.FIELD_ORDER) + 1,
          "кнопок по числу полей плюс возврат")


def test_write_goes_to_sheet_and_mirror() -> None:
    print("\n=== запись доходит и в лист, и в зеркало ===")
    seed_mirror()
    ws = FakeWS([["Иванов", "Иван"] + [""] * (len(HEAD) - 2)])
    ok = sheets_cache.write_player_field(FakeBook(ws), 2, "team", "Farm", "Иванов Иван")
    check(ok, "запись прошла")
    check(ws.rows[1][HEAD.index("Команда")] == "Farm", f"в листе: {ws.rows[1]}")
    with sheets_cache.get_connection() as conn:
        got = conn.execute("SELECT team FROM players WHERE row_index=2").fetchone()
    check(got and got["team"] == "Farm", f"в зеркале: {dict(got) if got else None}")


def test_numbers_stay_numbers() -> None:
    """Денежные и ценовые колонки в зеркале числовые: строкой туда попадёт
    «5500», и сравнения «сколько должен» начнут врать."""
    print("\n=== числовые поля не становятся строкой ===")
    seed_mirror()
    ws = FakeWS([["Иванов", "Иван"] + [""] * (len(HEAD) - 2)])
    sheets_cache.write_player_field(FakeBook(ws), 2, "price", "55", "Иванов Иван")
    with sheets_cache.get_connection() as conn:
        got = conn.execute("SELECT price FROM players WHERE row_index=2").fetchone()
    check(got and got["price"] == 55, f"стоимость числом: {got and got['price']}")

    ok = sheets_cache.write_player_field(FakeBook(ws), 2, "season", "много",
                                         "Иванов Иван")
    check(not ok, "не-число в денежное поле не пишем")


def test_add_player() -> None:
    print("\n=== заведение игрока ===")
    ws = FakeWS([["Иванов", "Иван"] + [""] * (len(HEAD) - 2)])
    row = sheets_cache.add_player(FakeBook(ws), "Петров", "Пётр")
    check(row == 3, f"строка новая: {row}")
    check(ws.appended and ws.appended[0][0] == "Петров"
          and ws.appended[0][1] == "Пётр", f"в лист ушли ФИО: {ws.appended}")
    check(all(c == "" for c in ws.appended[0][2:]),
          "остальное пусто — умолчания за тренера не выдумываем")


def test_twin_is_refused() -> None:
    """Две одинаковые строки — это разъехавшиеся посещаемость и оплаты, и
    понять потом, где чей платёж, невозможно."""
    print("\n=== тёзку не заводим ===")
    ws = FakeWS([["Иванов", "Иван"] + [""] * (len(HEAD) - 2)])
    check(sheets_cache.add_player(FakeBook(ws), "Иванов", "Иван") is None,
          "точное совпадение отвергнуто")
    check(sheets_cache.add_player(FakeBook(ws), "ИВАНОВ", "иван") is None,
          "регистр не спасает от дубля")
    check(sheets_cache.add_player(FakeBook(ws), "", "") is None,
          "пустое ФИО не заводим")
    check(not ws.appended, "в лист при этом ничего не дописано")


def test_name_cannot_be_emptied() -> None:
    """ФИО — якорь, по которому строка ищется в листе. Опустошить его значит
    потерять человека для всех последующих правок."""
    print("\n=== ФИО пустым не оставляем ===")
    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index("async def handle_field_text")
    body = src[at:at + 3000]
    check("NAME_FIELDS" in body, "обработчик знает про поля-якоря")
    check("пустыми не оставляю" in body, "и отказывается их чистить")


def test_coach_has_the_same_editor() -> None:
    """Правит лист ТРЕНЕР, а не только админ.

    Первая версия жила в админ-панели, и тренер её просто не видел: раздел
    закрыт проверкой на админа. Экраны при этом общие — две копии разъехались
    бы при первой правке, а лист они правят один."""
    print("\n=== тренер видит тот же редактор ===")
    import bot_daemon as bd
    src = (ROOT / "bot_daemon.py").read_text()

    check('"👥 Игроки", callback_data="coach:field:list:0"' in src,
          "в разделе тренера есть вход")
    check("_players_editor" in src, "обработчик экранов общий")
    check(src.count("async def _players_editor") == 1,
          "и он ровно один, а не две копии")

    # Экраны умеют возвращать человека туда, откуда он пришёл.
    _, markup = bd._fields_screen(0, "coach:field")
    data = [b.callback_data for row in markup.inline_keyboard for b in row]
    check(any(d.startswith("coach:field:") for d in data),
          f"кнопки списка ведут в раздел тренера: {data[:3]}")
    check("coach:main" in data, "и «Назад» возвращает к тренеру, а не в админку")

    _, card = bd._field_card(2, "coach:field")
    cdata = [b.callback_data for row in card.inline_keyboard for b in row]
    check(all(d.startswith("coach:field:") for d in cdata),
          "на карточке тоже свой префикс")

    # А админский путь не сломан.
    _, adm = bd._fields_screen(0, "admin:field")
    adata = [b.callback_data for row in adm.inline_keyboard for b in row]
    check(any(d.startswith("admin:field:") for d in adata), "админский вход цел")
    check("admin:menu:main" in adata, "и возвращает в админку")

    # Правку принимаем и от тренера: раньше стояла проверка только на админа.
    at = src.index("async def handle_field_text")
    body = src[at:at + 1200]
    check("_can_see_reports" in body, "текст поля принимается и от тренера")


def test_search_by_surname_part() -> None:
    """Поиск по куску фамилии или имени.

    Сравниваем в Python, а не в SQL: у SQLite lower() кириллицу не трогает, и
    «КАТЮРГИН» не нашёлся бы по «катюрг» — на этом проект уже спотыкался при
    дедупликации гостей."""
    print("\n=== поиск по части фамилии ===")
    import bot_daemon as bd
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM players")
        for i, (sur, nm) in enumerate([("Абрамов", "Платон"), ("Шлепикас", "Роман"),
                                       ("Катюргин", "Даниил"), ("Лысюк", "Денис"),
                                       ("КАТЮРГИН", "Пётр")]):
            conn.execute("INSERT INTO players (row_index, surname, name, synced_at) "
                         "VALUES (?,?,?,?)", (i + 2, sur, nm, now))
        conn.commit()

    def found(q):
        text, markup = bd._fields_screen(0, "coach:field", q)
        picks = [b.callback_data for row in markup.inline_keyboard for b in row
                 if b.callback_data.startswith("coach:field:pick:")]
        return text, picks

    text, picks = found("катюрг")
    check(len(picks) == 2, f"часть фамилии находит обоих Катюргиных: {len(picks)}")
    check("нашлось 2" in text, f"счётчик в заголовке: {text.splitlines()[0]}")

    _, picks = found("КАТЮРГ")
    check(len(picks) == 2, "регистр не мешает — сравниваем не через SQL")

    _, picks = found("роман")
    check(len(picks) == 1, f"по имени тоже ищет: {len(picks)}")

    text, picks = found("неттакого")
    check(not picks, "чужого не выдумывает")
    check("Никого не нашёл" in text, f"и говорит об этом: {text.splitlines()[2]}")

    text, picks = found("")
    check(len(picks) == 5, f"пустой запрос — весь список: {len(picks)}")
    check("👥 Игроки" in text, "и обычный заголовок")

    # Кнопка сброса появляется только когда есть что сбрасывать.
    _, m = bd._fields_screen(0, "coach:field", "катюрг")
    data = [b.callback_data for r in m.inline_keyboard for b in r]
    check("coach:field:clear" in data, "при поиске есть «Сбросить»")
    _, m = bd._fields_screen(0, "coach:field", "")
    data = [b.callback_data for r in m.inline_keyboard for b in r]
    check("coach:field:clear" not in data, "без поиска лишней кнопки нет")
    check("coach:field:find" in data, "а «Поиск» есть всегда")


def main() -> int:
    test_all_columns_editable()
    test_card_shows_every_field()
    test_write_goes_to_sheet_and_mirror()
    test_numbers_stay_numbers()
    test_add_player()
    test_twin_is_refused()
    test_name_cannot_be_emptied()
    test_coach_has_the_same_editor()
    test_search_by_surname_part()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ЛИСТ ИГРОКОВ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
