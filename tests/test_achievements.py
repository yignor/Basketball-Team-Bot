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


async def test_rule_gives_it_out(bd, ach_id: int) -> None:
    """Правило выдаёт значок всем подходящим и не трогает остальных."""
    print("\n=== правило выдаёт само ===")
    import achievements as ach

    res = ach.recount()
    check(res["given"] == 2, f"выдано двоим участникам фэнтези: {res['given']}")
    check(set(ach.holders(ach_id)) == {ME, OTHER}, "именно тем, кто играл")

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


async def test_table_gets_badges_in_one_go(bd, ach_id: int) -> None:
    """Для таблицы значки собираются одним запросом, не по строке на человека."""
    print("\n=== значки для таблицы ===")
    import achievements as ach
    ach.set_shown(OTHER, [ach_id])
    got = ach.shown_map([ME, OTHER, "нет-такого"])
    check(list(got) == [OTHER], f"вернулись только те, у кого есть видимые: {list(got)}")
    check(got[OTHER][0]["title"] == "Бетатестер", "с названием и описанием")
    check("image" not in got[OTHER][0], "картинку в таблицу не тащим, только признак")


async def test_image_is_checked(bd, ach_id: int) -> None:
    """Картинку принимаем не любую: она грузится у каждого в таблице."""
    print("\n=== картинка значка ===")
    import achievements as ach

    ok, note = ach.set_image(ach_id, b"\x89PNG\r\n\x1a\n" + b"0" * 100, "image/png")
    check(ok, f"PNG принят: {note}")
    data, kind = ach.image(ach_id)
    check(data and kind == "image/png", "и достаётся обратно тем же типом")

    ok, note = ach.set_image(ach_id, b"%PDF-1.4", "application/pdf")
    check(not ok and "PNG" in note, f"чужой формат отбит: {note}")

    ok, note = ach.set_image(ach_id, b"0" * (ach.MAX_IMAGE_BYTES + 1), "image/png")
    check(not ok and "КБ" in note, f"тяжёлый файл отбит с объяснением: {note}")

    data, _ = ach.image(ach_id)
    check(data and data.startswith(b"\x89PNG"), "прежняя картинка на месте")


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
    await test_rule_gives_it_out(bd, ach_id)
    await test_person_chooses_what_is_visible(bd, ach_id)
    await test_table_gets_badges_in_one_go(bd, ach_id)
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
