#!/usr/bin/env python3
"""Зал славы: турниры, места, фото с награждения.

    python3 tests/test_hall_of_fame.py

Проверяем главное обещание: место, поставленное тренером, бот не переписывает
своим — он видел таблицу, а тренер был на награждении. И второе: команду
узнают по корню имени, потому что за годы она звалась и «PULL UP», и
«PullUp Farm», и «Pull Up 2».

В сеть отсюда не ходим: таблицы лиг подменены.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fake_tg import (FakeBot, FakeContext, FakeMessage, FakeQuery, FakeUpdate,
                     FakeUser, buttons_of)

TMP = Path(tempfile.mkdtemp(prefix="hof-test-")) / "bot.db"
COACH = FakeUser(uid=800400, username="coach")
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
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("INSERT INTO players (row_index, surname, name, active_mark, "
                     "synced_at) VALUES (2, 'Иванов', 'Иван', '1', ?)", (now,))
        conn.execute(
            "INSERT INTO league_teams (source, team_id, name, league, comp_id, "
            "season_id, stage_id, ours, ctx_json, fetched_at) VALUES ('slpro', "
            "'707', 'PullUp Farm', 'Летний Кубок Дивизион C', '', '17', '160', 1, "
            "?, ?)", ('{"season": "2025-2026", "division": "SUMC", "season_id": 17,'
                      ' "stage_id": 160, "division_id": 71}', now))
        # Наши игры этого турнира: две победы и поражение.
        games = (("4600", "2026-08-01", "707", "95", 80, 70),
                 ("4601", "2026-08-08", "95", "707", 60, 75),
                 ("4602", "2026-08-15", "707", "700", 55, 65))
        for gid, day, home, guest, hs, gs in games:
            conn.execute(
                "INSERT INTO game_meta (source, game_id, game_date, season_id, "
                "stage_id, home_team_id, guest_team_id, home_name, guest_name, "
                "home_score, guest_score, fetched_at) VALUES ('slpro', ?, ?, '17', "
                "'160', ?, ?, 'PullUp Farm', 'Соперник', ?, ?, ?)",
                (gid, day, home, guest, hs, gs, now))
        # Старый турнир, который бот вёл: команда звалась иначе.
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, season_id, "
            "stage_id, home_team_id, guest_team_id, home_name, guest_name, "
            "home_score, guest_score, fetched_at) VALUES ('slpro', '900', "
            "'2024-04-28', '5', '8', '65', '300', 'Pull Up', 'Малина', 70, 60, ?)",
            (now,))
        conn.commit()
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


def test_names() -> None:
    print("\n=== имя команды ===")
    import hall_of_fame as hof
    for name in ("PULL UP", "PullUp Farm", "Pull-Up", "Pull Up 2", "пулап"):
        check(hof.is_ours(name), f"«{name}» — наша")
    for name in ("Кураж", "LFB 3 - Теремок", "Пулково"):
        check(not hof.is_ours(name), f"«{name}» — чужая")


def test_places_and_record() -> None:
    print("\n=== места по очкам и свои победы ===")
    import hall_of_fame as hof
    teams = [{"team_id": 1, "points": 30, "wins": 14},
             {"team_id": 2, "points": 30, "wins": 14},
             {"team_id": 3, "points": 24, "wins": 8},
             {"team_id": 4, "points": 12, "wins": 1}]
    places = hof._slpro_places(teams)
    check(places["3"][0] == 3 and places["4"][0] == 4, "порядок по очкам")
    check(places["1"][1] and places["2"][1],
          "равенство очков и побед — место делится, бот не выдумывает")
    check(not places["3"][1], "у остальных место своё")

    wins, losses, last = hof._our_record("slpro", "17", "160", "707")
    check((wins, losses) == (2, 1), f"победы и поражения по своим играм: {wins}-{losses}")
    check(last == "2026-08-15", f"дата последней игры: {last}")


def test_tracked() -> None:
    print("\n=== какие турниры вели ===")
    import hall_of_fame as hof
    known = {(t["source"], t["season_id"], t["stage_id"]) for t in hof.tracked()}
    check(("slpro", "17", "160") in known, "текущий турнир виден")
    check(("slpro", "5", "8") in known,
          "и старый, где команда звалась «Pull Up» — по корню имени")


async def test_scan_keeps_confirmed() -> None:
    print("\n=== поиск не затирает подтверждённое ===")
    import hall_of_fame as hof
    hof.save("slpro", "17", "160", "707", place=1, guess=0, set_by="coach",
             league="Летний Кубок Дивизион C")

    async def fake_slpro():
        return [{"source": "slpro", "season_id": "17", "stage_id": "160",
                 "team_id": "707", "team_name": "PullUp Farm",
                 "league": "Летний Кубок Дивизион C", "season": "2025-2026",
                 "place": 2, "teams": 12, "wins": 14, "losses": 2,
                 "last_day": "2026-08-15", "guess": 1}]

    async def fake_ib(comps):
        return []

    async def fake_ctx():
        return {("5", "8"): {"season_id": 5, "stage_id": 8, "season": "2023-2024",
                             "division_name": "B"}}

    real = hof.scan_slpro, hof.scan_infobasket, hof._slpro_stage_ctx
    hof.scan_slpro, hof.scan_infobasket, hof._slpro_stage_ctx = (
        fake_slpro, fake_ib, fake_ctx)
    try:
        found, added = await hof.scan()
    finally:
        hof.scan_slpro, hof.scan_infobasket, hof._slpro_stage_ctx = real

    mine = hof.get("slpro", "17", "160", "707")
    check(int(mine["place"]) == 1, f"место тренера осталось: {mine['place']}")
    check(int(mine["guess"]) == 0, "и осталось подтверждённым")
    check(int(mine["teams"]) == 12, "но справочное бот дополнил")

    old = hof.get("slpro", "5", "8", "65")
    check(old is not None, "турнир, которого нет в таблице лиги, заведён пустым")
    check(int((old or {}).get("place") or 0) == 0, "без выдуманного места")
    check(str((old or {}).get("league") or "") == "B", "но с названием турнира")


async def test_screens(bd) -> None:
    print("\n=== экраны ===")
    import hall_of_fame as hof
    text, markup, _ = await press(bd, "coach:team")
    check("coach:hof" in cbs(markup), "в разделе «Команда» есть «Зал славы»")

    text, markup, _ = await press(bd, "coach:hof")
    check("Летний Кубок" in text and "🥇" in text, "турнир с медалью в списке")
    key = "slpro:17:160:707"
    check(f"coach:hof:one:{key}" in cbs(markup), "в турнир можно зайти")

    text, markup, _ = await press(bd, f"coach:hof:one:{key}")
    check("Место: 1" in text, "на карточке место")
    # Победы на карточке — те, что записаны у турнира: их кладёт поиск или
    # закрытие лиги (в этом тесте — поиск, 14-2).
    check("14 побед" in text and "2 поражен" in text, "и баланс побед")

    text, markup, _ = await press(bd, f"coach:hof:place:{key}")
    check(f"coach:hof:set:{key}:3" in cbs(markup), "место выбирается кнопкой")
    check(f"coach:hof:other:{key}" in cbs(markup), "и можно ввести своё")
    text, markup, _ = await press(bd, f"coach:hof:set:{key}:3")
    check(int(hof.get("slpro", "17", "160", "707")["place"]) == 3, "место поменялось")

    # Подтверждение посчитанного ботом.
    hof.save("slpro", "17", "160", "707", place=2, guess=1)
    text, markup, _ = await press(bd, f"coach:hof:one:{key}")
    check("подтверди" in text, "видно, что место посчитал бот")
    check(f"coach:hof:ok:{key}" in cbs(markup), "есть кнопка «да, так и было»")
    await press(bd, f"coach:hof:ok:{key}")
    check(int(hof.get("slpro", "17", "160", "707")["guess"]) == 0,
          "подтвердили — пометки «бот посчитал» больше нет")


async def test_photo(bd) -> None:
    print("\n=== фото с награждения ===")
    import hall_of_fame as hof
    key = "slpro:17:160:707"
    await press(bd, f"coach:hof:pic:{key}")

    class Photo:
        file_id = "AgACAgIAAxk-фото"

    msg = FakeMessage(text="", bot=BOT, user=COACH)
    msg.photo = [Photo()]
    try:
        await bd.handle_hof_photo(FakeUpdate(message=msg, user=COACH), FakeContext(BOT))
    except Exception as exc:
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    check(hof.get("slpro", "17", "160", "707")["photo_id"] == Photo.file_id,
          "фото сохранено")
    check(any("Сохранил" in r["text"] for r in msg.replies), "и тренеру сказано")

    text, markup, q = await press(bd, f"coach:hof:show:{key}")
    check(any(p["photo"] == Photo.file_id for p in q.message.photos),
          "«Показать» присылает именно это фото")
    check("📷" in text, "на карточке помечено, что фото есть")


async def test_close_asks_place(bd) -> None:
    print("\n=== закрыли лигу — спросили место ===")
    import hall_of_fame as hof
    hof.forget("slpro", "17", "160", "707")

    async def fake_look_up(source, season_id, stage_id, team_id, ctx=None):
        return {"place": 1, "teams": 12, "wins": 14, "losses": 2,
                "name": "PullUp Farm", "sure": False, "shared": False}

    real = hof.look_up
    hof.look_up = fake_look_up
    try:
        text, markup, _ = await press(bd, "coach:lg:close2:slpro:707")
    finally:
        hof.look_up = real
    check("Посчитал по очкам" in text, "сказано, откуда место")
    check("Место: 1" in text, "и какое оно")
    check("coach:hof:ok:slpro:17:160:707" in cbs(markup), "можно подтвердить")
    row = hof.get("slpro", "17", "160", "707")
    check(row is not None and int(row["guess"]) == 1,
          "записано как предположение, пока тренер не подтвердил")


def main() -> int:
    print(f"База: {TMP}")
    bd = setup()
    test_names()
    test_places_and_record()
    test_tracked()
    asyncio.run(test_scan_keeps_confirmed())
    asyncio.run(test_screens(bd))
    asyncio.run(test_photo(bd))
    asyncio.run(test_close_asks_place(bd))
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ЗАЛ СЛАВЫ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
