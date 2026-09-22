#!/usr/bin/env python3
"""Команды в лигах: добавление из бота вместо правки «Конфига».

    python3 tests/test_league_teams.py

Главное: команда, заведённая в боте, попадает в ту же конфигурацию, что и
строки листа, — иначе половина бота знает про неё, а половина нет. И второе:
слежение не включается молча, потому что опросы уходят в общий чат.

В сеть не ходим: ответ лиги подменён.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fake_tg import (FakeBot, FakeContext, FakeMessage, FakeQuery, FakeUpdate,
                     FakeUser, buttons_of)

TMP = Path(tempfile.mkdtemp(prefix="teams-test-")) / "bot.db"
COACH = FakeUser(uid=800500, username="coach")
BOT = FakeBot()

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def setup() -> Any:
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ["ADMIN_USER_IDS"] = str(COACH.id)
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
    os.environ["SPREADSHEET_ID"] = ""
    import sheets_cache
    sheets_cache.DB_PATH = TMP
    sheets_cache.init_db()
    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None
    return bd


async def press(bd, data: str, who: FakeUser = COACH):
    q = FakeQuery(data, who, BOT)
    await bd.handle_coach_callback(FakeUpdate(query=q, user=who), FakeContext(BOT))
    last = (q.screens or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"], q


def cbs(markup) -> List[str]:
    return [b.callback_data for b in buttons_of(markup)]


FOUND = {"name": "Pull Up",
         "comps": [{"comp_id": 91098, "track": 91090, "league": "НБЛ. Третий Дивизион",
                    "comp": "Круговой турнир", "teams": 17, "place": 15,
                    "games": 16, "last_day": "27.10.2024"}]}


def test_storage() -> None:
    print("\n=== хранение ===")
    import league_setup as ls
    ls.add("32086", "Pull Up", [91090], added_by="coach")
    rows = ls.saved()
    check(len(rows) == 1 and rows[0]["team_id"] == "32086", "команда сохранена")
    check(rows[0]["comps"] == [91090], "и её турниры")
    ls.add("32086", "Pull Up", [91090, 104815])
    check(ls.saved()[0]["comps"] == [91090, 104815],
          "повторное добавление дополняет, а не двоит")
    check(ls.comp_ids() == [91090, 104815] and ls.team_ids() == ["32086"],
          "списки для конфигурации собираются")


def test_merge_into_config() -> None:
    print("\n=== попадает в общую конфигурацию ===")
    import league_setup as ls
    payload = {"comp_ids": [140825], "team_ids": [36502],
               "teams": {36502: {"alt_name": "PULL UP", "comp_ids": [140825]}}}
    merged = ls.merge_config(payload)
    check(merged["comp_ids"] == [91090, 104815, 140825], "турниры сложились")
    check(merged["team_ids"] == [32086, 36502], "команды тоже")
    check(merged["teams"][32086]["comp_ids"] == [91090, 104815],
          "у новой команды свои турниры")
    check(merged["teams"][36502]["comp_ids"] == [140825],
          "старую из «Конфига» не тронули")

    from enhanced_duplicate_protection import duplicate_protection
    cfg = duplicate_protection.get_config_ids()
    check(32086 in (cfg.get("team_ids") or []),
          "и это видно всем, кто читает конфигурацию: опросам, статистике, фэнтези")

    import hall_of_fame as hof
    check("32086" in hof.team_ids(), "зал славы берёт её оттуда же")


async def test_screens(bd) -> None:
    print("\n=== экраны ===")
    import league_setup as ls
    text, markup, _ = await press(bd, "coach:cfg")
    check("coach:rt:list" in cbs(markup), "в настройках есть «Соревнования»")
    text, markup, _ = await press(bd, "coach:rt:list")
    check("coach:tm:list" in cbs(markup), "а в них — добавление соревнования")

    text, markup, _ = await press(bd, "coach:tm:list")
    check("32086" in text, "добавленная команда в списке")
    check("из бота" in text, "и видно, что она заведена из бота, а не из «Конфига»")
    check("coach:tm:add" in cbs(markup), "можно добавить ещё")
    check("coach:tm:rm:32086" in cbs(markup), "и убрать эту")

    # Добавление: бот спрашивает лигу и ждёт подтверждения.
    await press(bd, "coach:tm:add")
    real = ls.discover

    async def fake_discover(team_id):
        return FOUND if str(team_id) == "36502" else {"name": "", "comps": []}

    ls.discover = fake_discover
    try:
        msg = FakeMessage(text="36502", bot=BOT, user=COACH)
        try:
            await bd.handle_hof_team(FakeUpdate(message=msg, user=COACH),
                                     FakeContext(BOT))
        except Exception as exc:
            if type(exc).__name__ != "ApplicationHandlerStop":
                raise
        shown = msg.replies[-1]["text"]
        check("НБЛ. Третий Дивизион" in shown, "показали найденные турниры")
        check("опросы" in shown, "и предупредили, что начнутся опросы")
        check(all(r["team_id"] != "36502" for r in ls.saved()),
              "до подтверждения ничего не включено")

        text, markup, _ = await press(bd, "coach:tm:ok")
        saved = {r["team_id"]: r for r in ls.saved()}
        check("36502" in saved, "после «Следить» команда добавлена")
        check(saved["36502"]["comps"] == [91090],
              "следим за этапом, а не за отдельной группой")

        # Неизвестный лиге номер.
        await press(bd, "coach:tm:add")
        msg = FakeMessage(text="999999", bot=BOT, user=COACH)
        try:
            await bd.handle_hof_team(FakeUpdate(message=msg, user=COACH),
                                     FakeContext(BOT))
        except Exception as exc:
            if type(exc).__name__ != "ApplicationHandlerStop":
                raise
        check(any("ничего не знает" in r["text"] for r in msg.replies),
              "про неизвестный номер сказали прямо")
    finally:
        ls.discover = real

    text, markup, _ = await press(bd, "coach:tm:rm:36502")
    check(all(r["team_id"] != "36502" for r in ls.saved()), "убирается кнопкой")


async def test_slpro(bd) -> None:
    print("\n=== SLPRO: дивизион и название ===")
    import league_setup as ls
    import slpro_client

    text, markup, _ = await press(bd, "coach:tm:list")
    check("coach:tm:slpro" in cbs(markup), "есть ввод для SLPRO")

    real = ls.discover_slpro

    async def fake(division, name):
        if division == "SUMC" and name.replace(" ", "").lower() == "pullupfarm":
            return {"found": True, "team_id": "707", "team_name": "PullUp Farm",
                    "season": "2025-2026", "stage_id": "160",
                    "division_name": "Летний Кубок Дивизион C", "near": []}
        return {"found": False, "near": ["PullUp Farm", "Резалит"],
                "division_name": "Летний Кубок Дивизион C"}

    ls.discover_slpro = fake
    try:
        # Промах в названии: бот показывает, кто в дивизионе есть.
        await press(bd, "coach:tm:slpro")
        msg = FakeMessage(text="SUMC Пул Ап", bot=BOT, user=COACH)
        try:
            await bd.handle_hof_team(FakeUpdate(message=msg, user=COACH),
                                     FakeContext(BOT))
        except Exception as exc:
            if type(exc).__name__ != "ApplicationHandlerStop":
                raise
        shown = msg.replies[-1]["text"]
        check("PullUp Farm" in shown, "подсказали точное название из лиги")
        check(not ls.saved("slpro"), "ничего не сохранили")

        await press(bd, "coach:tm:slpro")
        msg = FakeMessage(text="sumc PullUp Farm", bot=BOT, user=COACH)
        try:
            await bd.handle_hof_team(FakeUpdate(message=msg, user=COACH),
                                     FakeContext(BOT))
        except Exception as exc:
            if type(exc).__name__ != "ApplicationHandlerStop":
                raise
        check("Летний Кубок Дивизион C" in msg.replies[-1]["text"],
              "нашли команду и показали турнир")
        check(not ls.saved("slpro"), "но до подтверждения не включили")

        text, markup, _ = await press(bd, "coach:tm:oks")
        rows = ls.saved("slpro")
        check(len(rows) == 1 and rows[0]["team_id"] == "SUMC",
              "после «Следить» команда сохранена по коду дивизиона")
        check(rows[0]["name"] == "PullUp Farm",
              "имя взяли то, что в лиге, а не набранное тренером")
    finally:
        ls.discover_slpro = real

    # Главное: попадает в тот же список, из которого бот берёт турниры SLPRO.
    rows = slpro_client.leagues_from_config()
    check(any(r["division"] == "SUMC" and r["team_name"] == "PullUp Farm"
              for r in rows),
          "команда видна опросам, фэнтези и статистике SLPRO")

    text, markup, _ = await press(bd, "coach:tm:list")
    check("дивизион SUMC" in text, "и в списке команд она есть")
    await press(bd, "coach:tm:rms:SUMC")
    check(not ls.saved("slpro"), "убирается кнопкой")


def main() -> int:
    print(f"База: {TMP}")
    bd = setup()
    test_storage()
    test_merge_into_config()
    asyncio.run(test_screens(bd))
    asyncio.run(test_slpro(bd))
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("КОМАНДЫ В ЛИГАХ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
