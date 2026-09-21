#!/usr/bin/env python3
"""Закрытие лиги: сезон доигран, история цела.

    python3 tests/test_leagues_closed.py

Закрытие — пометка, а не удаление. Бот перестаёт считать турнир текущим:
не ищет игр, не зовёт его в пул и в цифры состава, не предлагает привязывать
к нему группы и сборы. Всё сыгранное остаётся на месте, а если лига заведёт
новый сезон, пометка снимается сама — иначе осенний старт прошёл бы мимо.
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

from fake_tg import FakeBot, FakeContext, FakeQuery, FakeUpdate, FakeUser, buttons_of

TMP = Path(tempfile.mkdtemp(prefix="leagues-test-")) / "bot.db"
COACH = FakeUser(uid=800300, username="coach")
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
        conn.execute(
            "INSERT INTO players (row_index, surname, name, active_mark, "
            "synced_at) VALUES (2, 'Иванов', 'Иван', '1', ?)", (now,))
        conn.execute(
            "INSERT INTO league_teams (source, team_id, name, league, comp_id, "
            "season_id, stage_id, ours, fetched_at) VALUES ('slpro', '707', "
            "'PullUp Farm', 'Летний Кубок', '', '17', '160', 1, ?)", (now,))
        conn.execute(
            "INSERT INTO league_teams (source, team_id, name, league, comp_id, "
            "season_id, stage_id, ours, fetched_at) VALUES ('infobasket', "
            "'36502', 'PULL UP', 'Ночная лига', '142849', '142849', '', 1, ?)",
            (now,))
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, season_id, "
            "stage_id, home_team_id, guest_team_id, home_name, guest_name, "
            "fetched_at) VALUES ('slpro', '4648', '2026-09-19', '17', '160', "
            "'707', '95', 'PullUp Farm', 'Теремок', ?)", (now,))
        conn.execute("UPDATE game_meta SET video_vk = 'https://vk.com/video-1_2' "
                     "WHERE source = 'slpro' AND game_id = '4648'")
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


async def test_screen(bd) -> None:
    print("\n=== экран лиг ===")
    text, markup, _ = await press(bd, "coach:cfg")
    check("coach:lg:list" in cbs(markup), "в настройках есть вход в «Лиги»")

    text, markup, _ = await press(bd, "coach:lg:list")
    check("Летний Кубок" in text and "Ночная лига" in text, "обе лиги в списке")
    check("последняя игра 19.09.2026" in text, "видно, когда играли в последний раз")
    check("coach:lg:one:slpro:707" in cbs(markup), "в лигу можно зайти")

    text, markup, _ = await press(bd, "coach:lg:close:slpro:707")
    check("Останется" in text and "результаты" in text,
          "перед закрытием сказано, что останется")
    check("coach:lg:close2:slpro:707" in cbs(markup), "закрытие — за подтверждением")
    import league_sync
    check(not league_sync.is_closed("slpro", "707"), "сам вопрос ничего не закрыл")


async def test_close_and_effects(bd) -> None:
    print("\n=== что меняет закрытие ===")
    import coach_lineup
    import league_sync
    import player_groups
    import season_fees
    before = {s["source"] for s in coach_lineup.current_scopes()}
    check(before == {"slpro", "infobasket"}, f"пока обе открыты — оба турнира: {before}")

    # Закрытие ведёт в зал славы — спросить про место. В сеть за таблицей
    # отсюда не ходим.
    import hall_of_fame as hof

    async def no_table(source, season_id, stage_id, team_id, ctx=None):
        return {}

    real, hof.look_up = hof.look_up, no_table
    try:
        text, markup, _ = await press(bd, "coach:lg:close2:slpro:707")
    finally:
        hof.look_up = real
    check(league_sync.is_closed("slpro", "707"), "лига закрыта")
    check("Лига закрыта" in text and "место" in text,
          "сразу спросили про итоговое место")
    check(any(c.startswith("coach:hof:place:") for c in cbs(markup)),
          "и дали его поставить")

    text, markup, _ = await press(bd, "coach:lg:one:slpro:707")
    check("Вернуть" in "".join(b.text for b in buttons_of(markup)),
          "на карточке лиги предложено вернуть")

    after = {s["source"] for s in coach_lineup.current_scopes()}
    check(after == {"infobasket"}, f"текущим остался только Инфобаскет: {after}")
    check([t["source"] for t in league_sync.our_teams()] == ["infobasket"],
          "закрытая не попадает в «наши команды»")
    check(len(league_sync.our_teams(include_closed=True)) == 2,
          "но из справочника не пропала")
    check(all("Летний" not in l["title"] for l in player_groups.leagues()),
          "группам закрытую лигу не предлагаем")
    check(all("Летний" not in l["title"] for l in season_fees.leagues()),
          "и сборам тоже")
    check("закрыта" in player_groups.league_title("slpro", "707"),
          "уже привязанная группа видит лигу и её пометку")

    # История цела.
    import video_notes
    check(any(str(g["game_id"]) == "4648" for g in video_notes.games_with_video()),
          "игра закрытой лиги осталась в разборе записи")
    import sheets_cache
    with sheets_cache.get_connection() as conn:
        n = conn.execute("SELECT COUNT(*) c FROM game_meta WHERE source='slpro'").fetchone()["c"]
    check(n == 1, "сыгранное осталось в справочнике матчей")


async def test_reopen(bd) -> None:
    print("\n=== вернуть ===")
    import coach_lineup
    import league_sync
    await press(bd, "coach:lg:open:slpro:707")
    check(not league_sync.is_closed("slpro", "707"), "лига снова действующая")
    check({s["source"] for s in coach_lineup.current_scopes()} == {"slpro", "infobasket"},
          "и снова считается текущим турниром")


def test_new_season_reopens() -> None:
    """Лига вернулась с новым сезоном — пометка снимается сама."""
    print("\n=== новый сезон ===")
    import league_sync
    league_sync.close("slpro", "707", True)
    same = [{"source": "slpro", "team_id": "707", "name": "PullUp Farm",
             "league": "Летний Кубок", "comp_id": "", "season_id": "17",
             "stage_id": "160", "ctx": None}]
    league_sync._store_teams(same)
    check(league_sync.is_closed("slpro", "707"),
          "тот же сезон приехал снова — лига остаётся закрытой")

    fresh = [dict(same[0], season_id="18", stage_id="171", league="Осенний Кубок")]
    league_sync._store_teams(fresh)
    check(not league_sync.is_closed("slpro", "707"),
          "новый сезон — пометка снята, бот снова ищет игры")


def main() -> int:
    print(f"База: {TMP}")
    bd = setup()
    asyncio.run(test_screen(bd))
    asyncio.run(test_close_and_effects(bd))
    asyncio.run(test_reopen(bd))
    test_new_season_reopens()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ЛИГИ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
