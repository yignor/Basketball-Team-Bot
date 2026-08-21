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
    # Границу берём по следующей функции, а не по числу знаков: обработчик
    # растёт, и срез «первые 3000» однажды отрезал проверку от глаз теста.
    nxt = src.index("\nasync def ", at + 10)
    body = src[at:nxt]
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


def test_question_goes_to_everyone() -> None:
    """Вопрос «будешь заниматься?» уходит всему листу, а не только активным.

    Иначе человек, который сейчас не ходит, может вернуться только через
    тренера — а вопрос ровно про то, будет ли он заниматься. Долги при этом
    по-прежнему считаются только с тех, с кого взнос ждём."""
    print("\n=== вопрос уходит всем ===")
    import training_dues as td
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM players")
        conn.execute("INSERT INTO players (row_index, surname, name, pay_season, "
                     "active_mark, synced_at) VALUES (2,'Иванов','Иван',5500,'+',?)", (now,))
        conn.execute("INSERT INTO players (row_index, surname, name, pay_season, "
                     "active_mark, synced_at) VALUES (3,'Петров','Пётр',5500,'',?)", (now,))
        conn.commit()

    period = "2026-09"
    check(len(td.status(period, True)) == 2, "весь лист — двое")
    check(len(td.status(period)) == 1, "с кого ждём взнос — один")
    check(len(td.debtors(period)) <= 1, "долги считаем по-прежнему только с активных")

    # Предупреждение тренеру обязано совпадать с рассылкой.
    plan = td.plan_text(period)
    check("у 2 человек" in plan, f"тренеру обещаем те же двое: {plan.splitlines()[0]}")
    check("Иванов" in plan and "Петров" in plan, "оба названы поимённо")

    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index("async def _ask_next_month")
    check("training_dues.status, period, True" in src[at:at + 900],
          "рассылка спрашивает весь лист")


def test_preview_uses_real_builders() -> None:
    """Предпросмотр собирается ТЕМИ ЖЕ функциями, что и рассылка.

    Копия текста в предпросмотре разошлась бы с настоящим письмом на первой же
    правке — и тренер утверждал бы одно, а команда получала другое."""
    print("\n=== предпросмотр показывает настоящее письмо ===")
    import bot_daemon as bd
    import training_dues as td
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM players")
        conn.execute("INSERT INTO players (row_index, surname, name, pay_season, "
                     "pay_game, active_mark, synced_at) "
                     "VALUES (2,'Иванов','Иван',5500,900,'+',?)", (now,))
        conn.commit()

    keys = [k for k, _ in bd.PREVIEWS]
    check("ask" in keys and "debt" in keys, "есть письма игрокам")
    check("plan" in keys and "report" in keys, "и письма тренеру")

    ask = bd._preview_text("ask")
    period = td.next_period(td.period_of(__import__("datetime").date.today()))
    rows = td.status(period, True)
    check(ask == td.ask_text(period, rows[0], 0),
          "предпросмотр вопроса совпадает с рассылкой знак в знак")

    check(bd._preview_text("plan") == td.plan_text(period),
          "и письмо тренеру тоже")
    check(bd._preview_text("несуществующее") == "",
          "неизвестное письмо — пусто, а не выдумка")

    # Кнопки показываем ПОДПИСЯМИ: живые нажались бы на самого тренера —
    # «буду заниматься» за него или отметка оплаты. Предпросмотр не меняет
    # данных, иначе это уже не просмотр.
    real = [b.text for r in bd._ask_markup("2026-09", 2).inline_keyboard for b in r]
    check(bd._preview_buttons("ask") == real,
          f"подписи взяты из настоящей клавиатуры рассылки: {real}")
    check(bd._preview_buttons("report") == [bd.MARK_TRAIN_PAID],
          "у отчёта тренеру — та же кнопка, что придёт")
    # У писем игрокам про игру клавиатуры нет — приписать её значило бы соврать.
    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index("async def _remind_game_debtors")
    check("reply_markup" not in src[at:at + 1200],
          "письмо игроку про игру идёт без кнопок")
    check(bd._preview_buttons("gamedebt") == [],
          "и предпросмотр кнопок ему не рисует")


def test_offline_players() -> None:
    """Кому рассылка не дойдёт — видно ЗАРАНЕЕ, а не из отбивки постфактум.

    Причины разные, и делать с ними надо разное: одного попросить нажать
    «Старт», другому сперва вписать ник в лист. Одним списком это неотличимо."""
    print("\n=== видно, кто вне бота ===")
    import bot_daemon as bd
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM players")
        conn.execute("DELETE FROM player_links")
        conn.execute("INSERT INTO players (row_index, surname, name, nickname, "
                     "tg_user_id, synced_at) VALUES (2,'Иванов','Иван','vanya','12345',?)",
                     (now,))
        conn.execute("INSERT INTO players (row_index, surname, name, nickname, "
                     "synced_at) VALUES (3,'Петров','Пётр','petya',?)", (now,))
        conn.execute("INSERT INTO players (row_index, surname, name, synced_at) "
                     "VALUES (4,'Сидоров','Сидор',?)", (now,))
        conn.commit()

    groups = bd._offline_players()
    check([p["title"] for p in groups["ok"]] == ["Иванов Иван"],
          f"на связи только тот, у кого есть числовой id: {groups['ok']}")
    check([p["title"] for p in groups["no_start"]] == ["Петров Пётр"],
          "ник известен, но бота не запускал — своя группа")
    check([p["title"] for p in groups["unknown"]] == ["Сидоров Сидор"],
          "без ника и id — другая группа")

    text, markup = bd._offline_screen()
    check("Вне бота: 2 из 3" in text, f"счётчик честный: {text.splitlines()[0]}")
    check("@petya" in text, "у кого есть ник — показываем, чтобы было кого просить")
    check("нажал «Старт»" in text,
          "объяснено, почему бот не может написать первым")
    check("Иванов" not in text.split("✅ На связи")[0],
          "тех, кто на связи, в списках проблем нет")
    check(any("coach:field:list:0" == b.callback_data
              for r in markup.inline_keyboard for b in r),
          "отсюда можно уйти править лист")

    # Привязка по /start сильнее листа: человек мог сменить ник.
    with sheets_cache.get_connection() as conn:
        conn.execute("INSERT INTO player_links (player_row, tg_user_id, username, "
                     "linked_at) VALUES (3,'999','petya',?)", (now,))
        conn.commit()
    groups = bd._offline_players()
    check([p["title"] for p in groups["ok"]] == ["Иванов Иван", "Петров Пётр"],
          f"после /start человек уходит из проблемных: {groups['no_start']}")


def test_manual_reminder_asks_before_repeat() -> None:
    """Повторная ручная рассылка требует подтверждения.

    21.08.2026 команда получила требование оплаты дважды за 19 секунд: пока
    бот обходит десяток человек, экран не меняется, и рука жмёт кнопку ещё раз.
    Деньги — не та тема, где стоит дублировать сообщение молча."""
    print("\n=== повтор рассылки переспрашивает ===")
    import bot_daemon as bd

    bd._remind_at.clear()
    check(bd._remind_ago("game") is None, "первый раз — шлём без вопросов")

    bd._remind_mark("game")
    check(bd._remind_ago("game") == "только что",
          f"сразу после — «только что»: {bd._remind_ago('game')}")
    check(bd._remind_ago("season") is None,
          "тренировки и игры считаются отдельно")

    # Через положенное время вопрос снимается сам.
    import time as _t
    bd._remind_at["game"] = _t.time() - (bd.REMIND_QUIET_MINUTES + 1) * 60
    check(bd._remind_ago("game") is None,
          f"через {bd.REMIND_QUIET_MINUTES} мин снова молча")

    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index('elif what == "remind"')
    body = src[at:at + 1600]
    check("Рассылаю…" in body, "кнопка убирается ДО рассылки, а не после")
    check("coach:remind:{kind}:yes" in body, "у повтора есть явное подтверждение")
    check("Люди получат его повторно" in body,
          "и сказано, чем это обернётся для команды")


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
    test_question_goes_to_everyone()
    test_preview_uses_real_builders()
    test_offline_players()
    test_manual_reminder_asks_before_repeat()
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
