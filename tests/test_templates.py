#!/usr/bin/env python3
"""Тексты писем, переписанные тренером.

    python3 tests/test_templates.py

Главное здесь — не дать испортить письмо. Свободная правка легко роняет
подстановку, и вопрос «будешь заниматься?» уходит без цифры взноса.
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
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))

import sheets_cache                                             # noqa: E402
sheets_cache.DB_PATH = Path(tempfile.mkdtemp()) / "bot.db"

import message_templates as mt                                  # noqa: E402

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def test_default_until_changed() -> None:
    print("\n=== пока не трогали — встроенный текст ===")
    sheets_cache.init_db()
    check(mt.render("ask", "ВСТРОЕННЫЙ", месяц="сентябрь", сумма="5500")
          == "ВСТРОЕННЫЙ", "отдаём встроенный, а не пустоту")
    check(mt.custom("ask") == "", "своего текста нет")


def test_required_fields_guarded() -> None:
    """Без обязательной подстановки письмо уйдёт без суммы или без месяца."""
    print("\n=== подстановки стерегутся ===")
    ok, why = mt.save("ask", "Будешь заниматься в следующем месяце?")
    check(not ok, "текст без подстановок отвергнут")
    check("{месяц}" in why and "{сумма}" in why,
          f"сказано, каких именно не хватает: {why}")

    ok, why = mt.save("ask", "🏋️ {месяц}\nВзнос — {сумма} ₽.")
    check(ok, f"с обязательными — сохранился: {why}")
    check(mt.custom("ask").startswith("🏋️"), "текст лёг в настройки")

    ok, _ = mt.save("ask", "   ")
    check(not ok, "пустой текст не сохраняем")
    ok, _ = mt.save("такого-нет", "что угодно")
    check(not ok, "неизвестное письмо не заводим")


def test_empty_optional_drops_the_line() -> None:
    """«за тобой  ₽» хуже, чем отсутствие строки."""
    print("\n=== пустая необязательная подстановка убирает строку ===")
    mt.save("ask", "🏋️ {месяц}\nВзнос: {сумма} ₽.\nДолг: {долг} ₽.")
    with_debt = mt.render("ask", "", месяц="сентябрь", сумма="5500", долг="2000")
    without = mt.render("ask", "", месяц="сентябрь", сумма="5500", долг="")
    check("Долг: 2000" in with_debt, "долг подставился")
    check("Долг" not in without, f"без долга строка исчезла целиком:\n{without}")
    check("Взнос: 5500" in without, "остальное на месте")


def test_reset_returns_builtin() -> None:
    print("\n=== встроенный текст возвращается ===")
    mt.save("ask", "🏋️ {месяц} — {сумма}")
    check(mt.custom("ask") != "", "свой текст стоит")
    mt.reset("ask")
    check(mt.custom("ask") == "", "после сброса своего нет")
    check(mt.render("ask", "ВСТРОЕННЫЙ", месяц="x", сумма="1") == "ВСТРОЕННЫЙ",
          "и снова отдаётся встроенный")


def test_real_messages_use_templates() -> None:
    """Правка должна доходить до НАСТОЯЩЕЙ рассылки, а не только до экрана."""
    print("\n=== правка меняет то, что уходит людям ===")
    import training_dues as td
    import game_roster as gr

    mt.reset("ask")
    row = {"row": 2, "title": "Иванов Иван", "need": 5500, "period": "2026-09"}
    builtin = td.ask_text("2026-09", row, 0)
    check("Будешь заниматься" in builtin, "встроенный текст на месте")

    mt.save("ask", "Тренировки {месяц}: {сумма} ₽. Идёшь?")
    changed = td.ask_text("2026-09", row, 0)
    check(changed == "Тренировки сентябрь 2026: 5500 ₽. Идёшь?",
          f"рассылка отдаёт свой текст: {changed}")
    mt.reset("ask")

    # Письмо про игру — тот же механизм, другой набор подстановок.
    import datetime
    game = {"source": "slpro", "game_id": "1", "opponent": "Резалит",
            "date": datetime.date(2026, 8, 24), "time": "21:00"}
    person = {"row": 2, "title": "Иванов Иван", "pay_game": 900}
    mt.save("gameahead", "Завтра {игра}. Оплата {сумма} ₽.")
    got = gr.player_debt_text(game, person, ahead=True)
    check(got.startswith("Завтра Резалит"), f"игра подставилась: {got}")
    check("900" in got, "сумма подставилась")
    mt.reset("gameahead")


def test_reports_are_not_editable() -> None:
    """Отчёты тренеру — списки, собираемые строкой на человека. Свободным
    текстом они не задаются, и обещать это в интерфейсе нельзя."""
    print("\n=== отчёты-списки не выдаём за редактируемые ===")
    import bot_daemon as bd
    check("plan" not in mt.TEMPLATES and "report" not in mt.TEMPLATES,
          "их нет среди шаблонов")
    check("plan" not in bd._TPL_OF and "report" not in bd._TPL_OF,
          "и кнопка правки у них не появляется")
    editable = set(bd._TPL_OF.values())
    check(editable <= set(mt.TEMPLATES),
          f"каждая кнопка правки ведёт к существующему шаблону: {editable}")


def test_yes_sets_active_no_drops_it() -> None:
    """Цикл целиком: ответил «буду» — отметка встала, «не буду» — снялась.

    Пока вопрос уходил только активным, ставить отметку на «буду» было незачем
    — она уже стояла. С рассылкой всему листу без этого человек говорил «буду»,
    а напоминание об оплате ему не приходило: взносы ждут по отметке."""
    print("\n=== ответ меняет отметку активности ===")
    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index("async def handle_next_month")
    body = src[at:src.index("\ndef _set_active", at)]
    check("_set_active" in body, "на «буду» отметка ставится")
    check("_drop_active" in body, "на «не буду» снимается")

    # Первая отметка в листе меняет правило для ВСЕХ — об этом должно быть
    # сказано вслух.
    check("первая отметка активности" in body.lower(),
          "про первую отметку тренера предупреждают")
    check("не вышло" in body, "и про неудачную запись тоже")

    # Отметка пишется тем значением, которое лист считает активным.
    setter = src[src.index("def _set_active"):src.index("def _drop_active")]
    check("PLAYERS_ACTIVE_MARK" in setter,
          "пишем ровно то значение, которое лист признаёт активностью")
    check(sheets_cache.is_active_mark(sheets_cache.PLAYERS_ACTIVE_MARK),
          "и оно действительно им признаётся")


def test_reminder_follows_the_mark() -> None:
    """Напоминание об оплате идёт по отметке активности, а не всем подряд."""
    print("\n=== напоминание зависит от отметки ===")
    import coach_payments
    import training_dues as td
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM players")
        conn.execute("DELETE FROM payments")
        conn.execute("INSERT INTO players (row_index, surname, name, pay_season, "
                     "active_mark, synced_at) VALUES (2,'Иванов','Иван',5500,?,?)",
                     (sheets_cache.PLAYERS_ACTIVE_MARK, now))
        conn.execute("INSERT INTO players (row_index, surname, name, pay_season, "
                     "active_mark, synced_at) VALUES (3,'Петров','Пётр',5500,'',?)",
                     (now,))
        conn.commit()

    people = {p["title"]: p["pays_season"] for p in coach_payments.players()}
    check(people.get("Иванов Иван") is True, "с отмеченного взнос ждут")
    check(people.get("Петров Пётр") is False, "с неотмеченного — нет")

    debtors = [r["title"] for r in td.debtors("2026-09")]
    check(debtors == ["Иванов Иван"],
          f"напоминание уйдёт только отмеченному: {debtors}")

    # А вопрос «будешь заниматься?» — по-прежнему обоим.
    check(len(td.status("2026-09", True)) == 2, "вопрос уходит и тому, и другому")


def main() -> int:
    test_default_until_changed()
    test_required_fields_guarded()
    test_empty_optional_drops_the_line()
    test_reset_returns_builtin()
    test_real_messages_use_templates()
    test_reports_are_not_editable()
    test_yes_sets_active_no_drops_it()
    test_reminder_follows_the_mark()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ТЕКСТЫ ПИСЕМ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
