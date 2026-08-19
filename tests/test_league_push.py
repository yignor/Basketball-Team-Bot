#!/usr/bin/env python3
"""Отправка составов и заявок в лига-бот (SPB Basket).

    python3 tests/test_league_push.py

Сети нет — HTTP подменяем. Проверяем ровно то, на чём здесь легко молча слать
в никуда: формат идентификатора игры, разбор склеенных ссылок, подпись одним
словом и то, что отказ от соседа не роняет наше сохранение.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import league_push as lp                                        # noqa: E402

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def test_game_id_is_bare() -> None:
    """У лига-бота игры записаны голым номером.

    У нас та же игра — «slpro-4558». Отправленный без обрезки id получил бы
    `no_game` на каждый состав, и связка молча не работала бы."""
    print("\n=== идентификатор игры обрезается ===")
    check(lp.bare_game_id("slpro-4558") == "4558", "slpro-4558 → 4558")
    check(lp.bare_game_id("4558") == "4558", "голый номер не портим")
    check(lp.bare_game_id("") == "", "пустое остаётся пустым")


def test_refs_are_plain() -> None:
    """Склеенный игрок разбирается, чужая лига отбрасывается.

    Один человек, играющий в двух лигах, у нас склеен в составную ссылку
    «slpro:..+infobasket:..». Такую там не знают и вернут not_in_pool."""
    print("\n=== ссылки простые и только своей лиги ===")
    got = lp.slpro_refs(["slpro:707:1+infobasket:36502:9",
                         "infobasket:36502:5", "slpro:707:2", "slpro:707:1"])
    check(got == ["slpro:707:1", "slpro:707:2"],
          f"склейка разобрана, чужая лига и дубль убраны: {got}")
    check(lp.slpro_refs(["infobasket:1:1"]) == [],
          "состав без игроков этой лиги не отправляем вовсе")


def test_lineup_players() -> None:
    print("\n=== заявка переводится в игроков ===")
    got = lp.players_from_refs(["slpro:707:1+infobasket:36502:9", "slpro:707:2"])
    check(got == [{"player_id": 1, "team_id": 707},
                  {"player_id": 2, "team_id": 707}], f"пары id: {got}")
    check(lp.players_from_refs([]) == [],
          "пустая заявка — пустой список, это «сняли», а не «нечего слать»")


def test_nick_is_one_word() -> None:
    """ФИО не отправляем: принимающая сторона отбросит составное значение
    целиком, и участник останется без подписи."""
    print("\n=== подпись одним словом ===")
    check(lp.nick_of({"username": "@vasya"}) == "vasya", "ник без собаки")
    check(lp.nick_of({"first_name": "Иван Петров"}) == "Иван",
          "из имени берём одно слово")
    check(" " not in lp.nick_of({"username": "va sya"}), "пробелов не отправляем")
    check(lp.nick_of({}) == "", "нечего сказать — пусто, а не выдумка")


def test_disabled_without_token() -> None:
    print("\n=== без токена отправка выключена ===")
    было = os.environ.pop("LEAGUE_INGEST_TOKEN", None)
    try:
        check(not lp.enabled(), "пустой токен — выключено")
        check(asyncio.run(lp.send_pick("1", "v", "slpro-1", ["slpro:707:1"])) is None,
              "и ничего не отправляется")
    finally:
        if было is not None:
            os.environ["LEAGUE_INGEST_TOKEN"] = было


def test_failure_is_swallowed() -> None:
    """Отказ соседа не должен ронять наше сохранение."""
    print("\n=== чужая ошибка остаётся чужой ===")
    os.environ["LEAGUE_INGEST_TOKEN"] = "тест"
    sent: Dict[str, Any] = {}

    async def boom(path, body):
        sent["path"], sent["body"] = path, body
        raise RuntimeError("сосед лежит")

    real, lp._post = lp._post, boom
    try:
        got = asyncio.run(lp.send_pick("77", "vasya", "slpro-4558",
                                       ["slpro:707:1", "slpro:707:2", "slpro:707:3"]))
    except Exception as exc:                       # именно этого быть не должно
        check(False, f"исключение выпустили наружу: {exc}")
        got = None
    finally:
        lp._post = real
    check(got is None, "падение отправки не мешает вызвавшему")

    # А тело при этом собрано верно — иначе «работает» означало бы «молчит».
    check(sent.get("path") == "/ingest/picks", f"адрес: {sent.get('path')}")
    pick = (sent.get("body") or {}).get("picks", [{}])[0]
    check(pick.get("game_id") == "4558", f"игра голым номером: {pick.get('game_id')}")
    check(pick.get("user_id") == "77", "id строкой — он общий у обоих ботов")
    check(len(pick.get("refs") or []) == 3, "три ссылки")


def test_rejections_are_logged() -> None:
    """Причины отказа пишем целиком: проглоченная причина превращает разбор
    в гадание, почему у человека нет очков."""
    print("\n=== причины отказа не глотаем ===")
    import logging
    seen: List[str] = []

    class Catch(logging.Handler):
        def emit(self, record):
            seen.append(record.getMessage())

    h = Catch()
    lp.logger.addHandler(h)
    lp.logger.setLevel(logging.INFO)
    try:
        lp._log_rejections("составы", {"accepted": 1, "rejected": [
            {"user_id": "5", "game_id": "4558", "error": "over_budget"}]})
    finally:
        lp.logger.removeHandler(h)
    joined = " ".join(seen)
    check("over_budget" in joined, f"причина в журнале: {joined[:120]}")
    check("4558" in joined, "и по какой игре")


def main() -> int:
    test_game_id_is_bare()
    test_refs_are_plain()
    test_lineup_players()
    test_nick_is_one_word()
    test_disabled_without_token()
    test_failure_is_swallowed()
    test_rejections_are_logged()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ЛИГА-БОТ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
