#!/usr/bin/env python3
"""Куда что писать: топики по видам сообщений и лигам.

    python3 tests/test_topic_routes.py

Главное обещание: пока тренер ничего не настроил, всё работает ровно как
раньше — топик берётся из «Конфига». Настроил — сообщения этой лиги уходят
туда, куда он сказал, а остальные лиги не трогаются.
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

TMP = Path(tempfile.mkdtemp(prefix="routes-test-")) / "bot.db"
COACH = FakeUser(uid=800600, username="coach")
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
    import hall_of_fame
    hall_of_fame.init()
    with sheets_cache.get_connection() as conn:
        # Две лиги в зале славы — из них берутся названия для экрана.
        for comp, org in (("142849", "Летняя лига"), ("91090", "Невская Баскетбольная Лига")):
            conn.execute(
                "INSERT INTO league_results (source, season_id, stage_id, team_id, "
                "org, league, season, place, teams, updated_at) VALUES "
                "('infobasket', ?, '', '36502', ?, 'Этап', '25/26', 3, 8, ?)",
                (comp, org, now))
        conn.commit()
    import league_setup
    league_setup.add("36502", "PULL UP", [142849, 91090], added_by="test")
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


def test_resolution() -> None:
    print("\n=== чей топик побеждает ===")
    import topic_routes as tr
    nbl = tr.scope_of("infobasket", 91090)
    summer = tr.scope_of("infobasket", 142849)
    check(nbl == "infobasket:91090", f"ключ лиги: {nbl}")
    check(tr.scope_of("slpro", "sumc") == "slpro:SUMC", "код дивизиона в верхнем регистре")

    check(tr.topic_for("GAME_POLLS", nbl, 1282) == 1282,
          "ничего не задано — топик из «Конфига»")

    tr.set_route("GAME_POLLS", tr.GENERAL, 55)
    check(tr.topic_for("GAME_POLLS", nbl, 1282) == 55, "общее правило перебивает «Конфиг»")

    tr.set_route("GAME_POLLS", nbl, 77)
    check(tr.topic_for("GAME_POLLS", nbl, 1282) == 77, "правило лиги перебивает общее")
    check(tr.topic_for("GAME_POLLS", summer, 1282) == 55,
          "а другой лиги не касается")
    check(tr.topic_for("GAME_RESULTS", nbl, 1282) == 1282,
          "и другого вида сообщений тоже")

    tr.set_route("GAME_POLLS", nbl, tr.TO_CHAT)
    check(tr.topic_for("GAME_POLLS", nbl, 1282) is None,
          "ноль — это «в общий чат», а не «не задано»")

    tr.set_route("GAME_POLLS", nbl, None)
    check(tr.topic_for("GAME_POLLS", nbl, 1282) == 55, "убрали правило — снова общее")
    tr.set_route("GAME_POLLS", tr.GENERAL, None)
    check(tr.topic_for("GAME_POLLS", nbl, 1282) == 1282, "убрали общее — снова «Конфиг»")


def test_senders_ask_routes() -> None:
    """Отправка должна спрашивать маршрут, а не брать топик из «Конфига» вслепую."""
    print("\n=== отправка смотрит в маршруты ===")
    import topic_routes as tr
    import game_system_manager as gsm_mod

    gsm = gsm_mod.GameSystemManager.__new__(gsm_mod.GameSystemManager)
    gsm.game_poll_topic_id = 1282
    tr.set_route("GAME_POLLS", tr.scope_of("infobasket", 91090), 777)
    try:
        got = gsm._topic_for("GAME_POLLS", 1282, {"comp_id": 91090})
        check(got == 777, f"опрос игры НБЛ уйдёт в свой топик: {got}")
        other = gsm._topic_for("GAME_POLLS", 1282, {"comp_id": 142849})
        check(other == 1282, f"а летней лиги — как было: {other}")
        blind = gsm._topic_for("GAME_POLLS", 1282, {})
        check(blind == 1282, "игра без турнира — по-старому")
    finally:
        tr.set_route("GAME_POLLS", tr.scope_of("infobasket", 91090), None)

    import slpro_manager
    mgr = slpro_manager.SlproManager.__new__(slpro_manager.SlproManager)
    tr.set_route("GAME_RESULTS", tr.scope_of("slpro", "SUMC"), 909)
    try:
        got = mgr._topic_for("GAME_RESULTS", 1282, {"division": "SUMC"})
        check(got == 909, f"результат SLPRO уйдёт в свой топик: {got}")
        check(mgr._topic_for("GAME_RESULTS", 1282, {"division": "SUMB"}) == 1282,
              "другой дивизион не задет")
    finally:
        tr.set_route("GAME_RESULTS", tr.scope_of("slpro", "SUMC"), None)


async def test_screens(bd) -> None:
    print("\n=== экраны тренера ===")
    import topic_routes as tr
    text, markup, _ = await press(bd, "coach:cfg")
    check("coach:rt:list" in cbs(markup), "в настройках тренера есть «Куда что писать»")

    text, markup, _ = await press(bd, "coach:rt:list")
    check("Невская Баскетбольная Лига" in text, "лиги названы по-человечески")
    check("coach:rt:s::" in cbs(markup), "есть общее правило")
    check("coach:rt:s:infobasket:91090" in cbs(markup), "и вход в лигу")
    check(all(len(c.encode()) <= 64 for c in cbs(markup)), "кнопки в пределах 64 байт")

    text, markup, _ = await press(bd, "coach:rt:s:infobasket:91090")
    check("Опросы на игру" in text and "Результаты игр" in text,
          "внутри — виды сообщений")
    kinds = [c for c in cbs(markup) if c.startswith("coach:rt:k:")]
    check(len(kinds) == len(tr.KINDS), "по кнопке на каждый вид")

    text, markup, _ = await press(bd, "coach:rt:k:infobasket:91090:0")
    check("Указать топик" in "".join(b.text for b in buttons_of(markup)),
          "можно указать топик")

    await press(bd, "coach:rt:set:infobasket:91090:0")
    msg = FakeMessage(text="https://t.me/c/1234567/1282/456", bot=BOT, user=COACH)
    try:
        await bd.handle_topic_id(FakeUpdate(message=msg, user=COACH), FakeContext(BOT))
    except Exception as exc:
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    check(tr.route("GAME_POLLS", "infobasket:91090") == 1282,
          "номер топика достали из ссылки")
    check(any("топик 1282" in r["text"] for r in msg.replies), "и подтвердили")

    text, markup, _ = await press(bd, "coach:rt:k:infobasket:91090:0")
    check("правило лиги" in text, "на карточке видно, что правило своё")
    text, markup, _ = await press(bd, "coach:rt:chat:infobasket:91090:0")
    check(tr.route("GAME_POLLS", "infobasket:91090") == tr.TO_CHAT,
          "«в общий чат» — тоже правило")
    text, markup, _ = await press(bd, "coach:rt:off:infobasket:91090:0")
    check(tr.route("GAME_POLLS", "infobasket:91090") is None, "правило убирается")

    text, markup, _ = await press(bd, "coach:rt:list")
    check("свои правила" not in text, "и лига больше не помечена")


async def test_close_comp(bd) -> None:
    print("\n=== закрыть соревнование ===")
    import league_setup as ls
    import league_sync
    import sheets_cache
    from enhanced_duplicate_protection import duplicate_protection

    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM league_teams")
        conn.execute(
            "INSERT INTO league_teams (source, team_id, name, league, comp_id, "
            "season_id, stage_id, ours, fetched_at) VALUES ('infobasket', '36502', "
            "'PULL UP', 'Ночная лига', '91090', '91090', '', 1, ?)", (now,))
        conn.commit()

    before = duplicate_protection.get_config_ids().get("comp_ids") or []
    check(91090 in before, f"турнир пока в конфигурации: {before}")

    text, markup, _ = await press(bd, "coach:rt:close:infobasket:91090")
    check("Закрыть" in text and "останется" in text,
          "перед закрытием сказано, что изменится")
    check(91090 in (duplicate_protection.get_config_ids().get("comp_ids") or []),
          "сам вопрос ничего не закрыл")

    text, markup, _ = await press(bd, "coach:rt:close2:infobasket:91090")
    after = duplicate_protection.get_config_ids().get("comp_ids") or []
    check(91090 not in after, f"бот больше не следит за турниром: {after}")
    check(league_sync.is_closed("infobasket", "36502"),
          "и сезон команды закрыт — фэнтези с составом его не считают")
    check("Невская" not in text.split("Закрытые")[0],
          "из списка соревнований он пропал")

    text, markup, _ = await press(bd, "coach:rt:shut")
    check("Невская Баскетбольная Лига" in text, "он в «Закрытых»")
    check("coach:rt:open:infobasket:91090" in cbs(markup), "и его можно вернуть")

    text, markup, _ = await press(bd, "coach:rt:open:infobasket:91090")
    check(91090 in (duplicate_protection.get_config_ids().get("comp_ids") or []),
          "вернули — снова следим")
    check(not league_sync.is_closed("infobasket", "36502"),
          "и сезон команды снова действующий")


async def test_close_slpro(bd) -> None:
    print("\n=== закрыть дивизион SLPRO ===")
    import league_setup as ls
    import slpro_client
    ls.add_slpro("SUMC", "PullUp Farm", "Летний Кубок Дивизион C", added_by="test")
    check(any(r["division"] == "SUMC" for r in slpro_client.leagues_from_config()),
          "дивизион в списке турниров SLPRO")
    await press(bd, "coach:rt:close2:slpro:SUMC")
    check(not any(r["division"] == "SUMC" for r in slpro_client.leagues_from_config()),
          "закрыли — опросы и анонсы по нему больше не идут")
    await press(bd, "coach:rt:open:slpro:SUMC")
    check(any(r["division"] == "SUMC" for r in slpro_client.leagues_from_config()),
          "вернули — снова в строю")
    ls.drop("SUMC", "slpro")


def main() -> int:
    print(f"База: {TMP}")
    bd = setup()
    test_resolution()
    test_senders_ask_routes()
    asyncio.run(test_screens(bd))
    asyncio.run(test_close_comp(bd))
    asyncio.run(test_close_slpro(bd))
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("МАРШРУТЫ ТОПИКОВ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
