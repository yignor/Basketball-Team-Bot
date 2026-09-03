#!/usr/bin/env python3
"""Ачивки: выдача, показ и то, чего показывать нельзя.

    python3 tests/test_achievements.py

База временная. Главное, за чем следим: значок — украшение, и оно не должно
ни на что влиять; человек сам решает, что видно; правило выдаёт, но не
отбирает выбранное человеком.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fake_tg import (FakeBot, FakeContext, FakeMessage, FakeQuery, FakeUpdate,
                     FakeUser, buttons_of)

TMP = Path(tempfile.mkdtemp(prefix="ach-test-")) / "bot.db"
ADMIN = FakeUser(uid=900100, username="boss")
BOT = FakeBot()
ME, OTHER = "900100", "900200"

# Однопиксельный PNG — настоящий, чтобы ужатие работало как в бою.
PNG_PIXEL = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000002000000020806000000"
    "72b60d240000001549444154789c63fccfc0f09f818181810944803000"
    "1f1702020247b3140000000049454e44ae426082")


class FakeApp:
    """У демона на руках Application, а нам нужен только его бот."""

    def __init__(self, bot):
        self.bot = bot


bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def setup() -> Any:
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ["ADMIN_USER_IDS"] = str(ADMIN.id)
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
    os.environ["SPREADSHEET_ID"] = ""
    import sheets_cache
    sheets_cache.DB_PATH = TMP
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM fantasy_rosters")
        # Двое собирали состав в фэнтези — им и положен значок бетатестера.
        for uid in (ME, OTHER):
            conn.execute(
                "INSERT INTO fantasy_rosters (user_id, season_id, week_start, "
                "player_refs_json, mode, updated_at) VALUES (?, 1, '2026-08-17',"
                " '[]', 'classic', ?)", (uid, now))
        conn.commit()
    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None
    return bd


async def press(bd, data: str):
    q = FakeQuery(data, ADMIN, BOT)
    await bd.handle_admin_callback(FakeUpdate(query=q, user=ADMIN), FakeContext(BOT))
    last = (q.screens or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"]


async def say(bd, text: str):
    msg = FakeMessage(text=text, bot=BOT, user=ADMIN)
    try:
        await bd.handle_ach_text(FakeUpdate(message=msg, user=ADMIN), FakeContext(BOT))
    except Exception as exc:
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    last = (msg.replies or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"]


def cbs(markup) -> List[str]:
    return [b.callback_data for b in buttons_of(markup) if b.callback_data]


# ─────────────────────────── сценарии ──────────────────────────────────────


async def test_create_through_admin(bd) -> int:
    """Админ заводит ачивку, пишет описание и правило."""
    print("\n=== ачивка заводится из админки ===")
    import achievements as ach

    text, markup = await press(bd, "admin:ach:list")
    check("Ачивки" in text, "раздел открылся")
    check("admin:ach:new" in cbs(markup), "есть кнопка создания")

    await press(bd, "admin:ach:new")
    text, _ = await say(bd, "Бетатестер")
    check("Бетатестер" in text, f"создана: {text.splitlines()[0]}")
    ach_id = ach.all_achievements()[0]["id"]

    await press(bd, f"admin:ach:desc:{ach_id}")
    await say(bd, "Был с нами, когда фэнтези только запускалась.")
    check("Был с нами" in (ach.get(ach_id) or {}).get("description", ""),
          "описание сохранилось")

    await press(bd, f"admin:ach:setrule:{ach_id}:fantasy")
    check((ach.get(ach_id) or {}).get("rule") == "fantasy", "правило выбрано")
    return int(ach_id)


async def test_cutoff_limits_the_rule(bd, ach_id: int) -> None:
    """«Первопроходец» — тем, кто был здесь ДО дня отсечки, и только им.

    Без границы значок доставался бы и тому, кто зайдёт в фэнтези завтра, —
    а смысл его ровно в обратном."""
    print("\n=== отсечка по дате ===")
    import achievements as ach
    import sheets_cache

    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        # Новичок, собравший состав сегодня.
        conn.execute(
            "INSERT INTO fantasy_rosters (user_id, season_id, week_start, "
            "player_refs_json, mode, updated_at) VALUES ('900300', 1, "
            "'2026-08-24', '[]', 'classic', ?)", (now,))
        # А ME и OTHER пришли в июле — их заготовка ставит с этой датой.
        conn.execute("UPDATE fantasy_rosters SET updated_at = '2026-07-16T20:00:00+00:00' "
                     "WHERE user_id IN (?, ?)", (ME, OTHER))
        conn.commit()

    today = now[:10]
    check(ach.parse_day("25.08.2026") == "2026-08-25", "дата по-русски разбирается")
    check(ach.parse_day("не дата") == "", "мусор не проходит за дату")

    everyone = ach._fantasy_users()
    check("900300" in everyone, "без границы новичок тоже участник")

    # «По сегодняшний день» — включительно: тот, кто собрал состав сегодня,
    # участвовал, и значок за участие ему положен.
    by_today = ach._fantasy_users(today)
    check(ME in by_today and OTHER in by_today, "июльские попали")
    check("900300" in by_today, "и сегодняшний новичок тоже — день входит")

    yesterday = (date.fromisoformat(today) - timedelta(days=1)).isoformat()
    by_yesterday = ach._fantasy_users(yesterday)
    check("900300" not in by_yesterday,
          "а по вчерашний день его уже нет — граница работает")
    check(ME in by_yesterday, "июльские при этом остались")

    ach.update(ach_id, rule_arg=today)
    ach.recount()
    check(set(ach.holders(ach_id)) == {ME, OTHER, "900300"},
          f"значок у всех, кто хоть раз играл: {sorted(ach.holders(ach_id))}")

    got = ach.get(ach_id) or {}
    check("по" in got.get("rule_title", ""),
          f"на карточке видно границу: {got.get('rule_title')}")


async def test_rule_gives_it_out(bd, ach_id: int) -> None:
    """Правило выдаёт значок всем подходящим и не трогает остальных."""
    print("\n=== правило выдаёт само ===")
    import achievements as ach

    check(set(ach.holders(ach_id)) == {ME, OTHER, "900300"},
          "значок у тех, кто играл")
    again = ach.recount()
    check(again["given"] == 0, "повторный пересчёт ничего не задваивает")


async def test_person_chooses_what_is_visible(bd, ach_id: int) -> None:
    """Человек сам решает, что видно, и больше лимита показать нельзя."""
    print("\n=== видно то, что выбрал человек ===")
    import achievements as ach

    shown = ach.of_user(ME, shown_only=True)
    check(len(shown) == 1, "свежевыданный значок сразу виден")

    ok, note = ach.set_shown(ME, [])
    check(ok and not ach.of_user(ME, shown_only=True), "и его можно спрятать")

    # Заводим лишние, чтобы упереться в предел.
    extra = [ach.create(f"Значок {i}")[0] for i in range(ach.SHOWN_LIMIT + 1)]
    for e in extra:
        ach.award(e, ME)
    ok, note = ach.set_shown(ME, extra)
    check(not ok, f"больше {ach.SHOWN_LIMIT} показать не дают: {note}")

    ok, _ = ach.set_shown(ME, extra[:ach.SHOWN_LIMIT])
    check(ok and len(ach.of_user(ME, shown_only=True)) == ach.SHOWN_LIMIT,
          "ровно предел — можно")

    ok, note = ach.set_shown(ME, [max(extra) + 999])
    check(not ok, f"чужой значок показать нельзя: {note}")

    # Главное про правила: пересчёт не возвращает спрятанное.
    ach.set_shown(ME, [])
    ach.recount()
    check(not ach.of_user(ME, shown_only=True),
          "пересчёт не вернул спрятанное в таблицу за спиной человека")


async def test_season_rules(bd, ach_id: int) -> None:
    """Место в зачёте и лучший в ранге — за конкретный сезон."""
    print("\n=== места и ранги ===")
    import achievements as ach
    import sheets_cache

    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO fantasy_seasons (id, name, format, status, "
            "started_at, settings_json) VALUES (7, 'Летний кубок 2026', '5x5', "
            "'ended', ?, ?)",
            (now, '{"modes": ["free"], "scopes": []}'))
        # Очки: ME впереди OTHER.
        conn.execute("DELETE FROM fantasy_game_scores")
        for uid, pts in ((ME, 120), (OTHER, 80)):
            conn.execute(
                "INSERT INTO fantasy_game_scores (season_id, user_id, source, "
                "game_id, game_date, points, mode, computed_at) VALUES "
                "(7, ?, 'slpro', 'g1', '2026-08-01', ?, 'free', ?)",
                (uid, pts, now))
        conn.commit()

    first, _ = ach.create("Чемпион кубка")
    ach.update(first, rule="place", rule_arg="1", season_id=7)
    got = ach.get(first) or {}
    check("Летний кубок 2026" in got.get("rule_title", ""),
          f"на карточке видно сезон: {got.get('rule_title')}")
    check("Первое место" in got.get("rule_title", ""), "и место названо словом")

    winners = ach._place_winners(7, 1)
    check([w[0] for w in winners] == [ME], f"первое место у лидера: {winners}")
    check(ach._place_winners(7, 2)[0][0] == OTHER, "второе — у второго")

    ach.recount()
    check(ach.holders(first) == [ME], "значок ушёл чемпиону")

    summary = ach.season_summary(first, ME)
    check(summary.get("season") == "Летний кубок 2026", "итог знает сезон")
    check(any("120" in l for l in summary.get("lines", [])),
          f"и очки за сезон: {summary.get('lines')}")
    check(not ach.season_summary(first, OTHER).get("lines"),
          "чужому итог этого значка не приписываем")

    # Без сезона правило молчит, а не выдаёт кому попало.
    lost, _ = ach.create("Без сезона")
    ach.update(lost, rule="place", rule_arg="1")
    ach.recount()
    check(not ach.holders(lost), "правило без сезона никого не награждает")
    check("сезон не выбран" in (ach.get(lost) or {}).get("rule_title", ""),
          "и об этом сказано на карточке")


async def test_person_is_told_about_the_badge(bd, ach_id: int) -> None:
    """Человеку пишут о выданном значке — один раз и картинкой."""
    print("\n=== письмо о значке ===")
    import achievements as ach

    ach.hush()                       # старое считаем рассказанным
    check(not ach.unannounced(), "старые выдачи людям задним числом не летят")

    fresh, _ = ach.create("Свежая", description="За то, что дочитал")
    ach.set_image(fresh, PNG_PIXEL, "image/png")
    ach.award(fresh, ME)
    todo = ach.unannounced()
    check(len(todo) == 1 and todo[0]["user_id"] == ME,
          f"в очереди ровно одна выдача: {len(todo)}")

    before = len(BOT.photos)
    await bd._tell_about_badges(FakeApp(BOT))
    sent = BOT.photos[before:]
    check(len(sent) == 1, f"ушла одна картинка: {len(sent)}")
    if sent:
        cap = sent[0].get("caption", "")
        check("Свежая" in cap, f"название названо: {cap.splitlines()[0]}")
        check("дочитал" in cap, "описание приложено")
        check("Мой кабинет" in cap, "сказано, где это спрятать")
    check(not ach.unannounced(), "очередь пуста — второй раз не напишем")

    await bd._tell_about_badges(FakeApp(BOT))
    check(len(BOT.photos) - before == 1, "повторный проход молчит")

    # Без картинки — просто текстом, а не молчанием.
    plain, _ = ach.create("Без картинки")
    ach.award(plain, ME)
    было = len(BOT.sent)
    await bd._tell_about_badges(FakeApp(BOT))
    check(len(BOT.sent) > было, "значок без картинки уходит текстом")


async def test_table_gets_badges_in_one_go(bd, ach_id: int) -> None:
    """Для таблицы значки собираются одним запросом, не по строке на человека."""
    print("\n=== значки для таблицы ===")
    import achievements as ach
    ach.set_shown(OTHER, [ach_id])
    ach.set_shown(ME, [])            # у ME значки есть, но он их спрятал
    got = ach.shown_map([ME, OTHER, "нет-такого"])
    check(list(got) == [OTHER],
          f"вернулись только те, у кого есть ВИДИМЫЕ: {list(got)}")
    check(got[OTHER][0]["title"] == "Бетатестер", "с названием и описанием")
    check("image" not in got[OTHER][0], "картинку в таблицу не тащим, только признак")


async def test_player_card_finds_the_owner(bd, ach_id: int) -> None:
    """Значок виден и в карточке игрока — через привязку профиля лиги.

    Карточка знает человека только по номеру в лиге, а значок выдан
    telegram-пользователю. Без обратного поиска значки в карточке не появились
    бы вовсе, и «везде в приложении» осталось бы обещанием."""
    print("\n=== значки в карточке игрока ===")
    import achievements as ach
    import player_identity
    import sheets_cache

    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO player_identities (tg_user_id, source, "
            "player_id, linked_at) VALUES (?, 'infobasket', '55555', ?)",
            (OTHER, now))
        conn.commit()

    check(player_identity.user_of("infobasket", "55555") == OTHER,
          "профиль лиги нашёл своего человека")
    check(not player_identity.user_of("infobasket", "00000"),
          "чужой профиль никому не принадлежит")

    ach.set_shown(OTHER, [ach_id])
    import fantasy_api
    got = fantasy_api._badges_of_ref("ib:36502:55555")
    check(len(got) == 1 and got[0]["id"] == ach_id,
          f"в карточке виден его значок: {got}")
    check(fantasy_api._badges_of_ref("ib:36502:00000") == [],
          "у чужого игрока значков нет")


async def test_image_is_checked(bd, ach_id: int) -> None:
    """Картинку принимаем не любую: она грузится у каждого в таблице."""
    print("\n=== картинка значка ===")
    import achievements as ach

    # Заголовок PNG есть, а данных за ним нет — Pillow такое не откроет.
    # Проверяем запасной путь: значок важнее аккуратности, кладём как прислали.
    ok, note = ach.set_image(ach_id, b"\x89PNG\r\n\x1a\n" + b"0" * 100, "image/png")
    check(ok and "как прислали" in note,
          f"нечитаемую картинку не теряем: {note}")
    data, kind = ach.image(ach_id)
    check(data and kind == "image/png", "и достаётся обратно тем же типом")

    ok, note = ach.set_image(ach_id, b"%PDF-1.4", "application/pdf")
    check(not ok and "PNG" in note, f"чужой формат отбит: {note}")

    ok, note = ach.set_image(ach_id, b"0" * (ach.MAX_IMAGE_BYTES + 1), "image/png")
    check(not ok and "КБ" in note, f"тяжёлый файл отбит с объяснением: {note}")

    data, _ = ach.image(ach_id)
    check(data and data.startswith(b"\x89PNG"), "прежняя картинка на месте")

    # Ужатие: кладём заведомо большую картинку и проверяем, что она худеет.
    from PIL import Image
    import io as _io
    buf = _io.BytesIO()
    Image.new("RGB", (1400, 1400), (20, 90, 160)).save(buf, format="JPEG", quality=95)
    fat = buf.getvalue()
    ach.set_image(ach_id, fat, "image/jpeg")
    check(ach.image_size(ach_id) < len(fat),
          f"при загрузке ужали: {len(fat)} -> {ach.image_size(ach_id)}")
    small = Image.open(_io.BytesIO(ach.image(ach_id)[0]))
    check(max(small.size) <= ach.STORE_SIDE,
          f"и по стороне уложились в {ach.STORE_SIDE}: {small.size}")

    ok, note = ach.reshrink(ach_id)
    check(not ok and "уже" in note, f"второй раз жать нечего: {note}")


async def test_delete_takes_awards(bd) -> None:
    """Удалённая ачивка не остаётся висеть у людей."""
    print("\n=== удаление ачивки ===")
    import achievements as ach
    gone, _ = ach.create("На выброс")
    ach.award(gone, ME)
    check(ach.holders(gone) == [ME], "выдана")
    ach.delete(gone)
    check(not ach.holders(gone), "выдача ушла вместе с ачивкой")
    check(all(b["id"] != gone for b in ach.of_user(ME)), "и из значков человека")


async def test_only_admin_gets_in(bd) -> None:
    """Раздел ачивок — админский."""
    print("\n=== посторонним нельзя ===")
    stranger = FakeUser(uid=900999, username="nobody")
    q = FakeQuery("admin:ach:list", stranger, BOT)
    await bd.handle_admin_callback(FakeUpdate(query=q, user=stranger),
                                   FakeContext(BOT))
    check(not q.screens, "чужому раздел не открылся")

    msg = FakeMessage(text="Чужая ачивка", bot=BOT, user=stranger)
    bd._awaiting_ach[stranger.id] = "new"
    try:
        await bd.handle_ach_text(FakeUpdate(message=msg, user=stranger),
                                 FakeContext(BOT))
    except Exception as exc:
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    check(stranger.id not in bd._awaiting_ach, "и ввод у него сбросили")


async def test_start_clears_badge_dialogs(bd) -> None:
    print("\n=== /start закрывает диалоги ачивок ===")
    bd._awaiting_ach[ADMIN.id] = "new"
    bd._awaiting_badge[ADMIN.id] = 1
    bd._clear_pending(ADMIN.id)
    check(ADMIN.id not in bd._awaiting_ach, "ожидание текста снято")
    check(ADMIN.id not in bd._awaiting_badge, "ожидание картинки тоже")


async def run() -> None:
    bd = setup()
    ach_id = await test_create_through_admin(bd)
    await test_cutoff_limits_the_rule(bd, ach_id)
    await test_rule_gives_it_out(bd, ach_id)
    await test_person_chooses_what_is_visible(bd, ach_id)
    await test_season_rules(bd, ach_id)
    await test_person_is_told_about_the_badge(bd, ach_id)
    await test_table_gets_badges_in_one_go(bd, ach_id)
    await test_player_card_finds_the_owner(bd, ach_id)
    await test_image_is_checked(bd, ach_id)
    await test_delete_takes_awards(bd)
    await test_only_admin_gets_in(bd)
    await test_start_clears_badge_dialogs(bd)


def main() -> int:
    print(f"База: {TMP}")
    asyncio.run(run())
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("АЧИВКИ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
