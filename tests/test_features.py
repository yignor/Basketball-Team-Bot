#!/usr/bin/env python3
"""Выключатели: тренер отключает то, что команде не нужно.

    python3 tests/test_features.py

Главное: выключатель — это тишина, а не удаление. И обратное важнее: пока
ничего не трогали, всё работает как раньше — появление выключателей само по
себе не должно ничего отключить.
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

TMP = Path(tempfile.mkdtemp(prefix="features-test-")) / "bot.db"
COACH = FakeUser(uid=800700, username="coach")
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


def test_defaults() -> None:
    print("\n=== по умолчанию всё работает ===")
    import features
    for key, title, _, default in features.FEATURES:
        check(features.enabled(key) == default,
              f"«{title}» — как было ({'вкл' if default else 'выкл'})")
    check(features.off_list() == [], "выключенного нет")
    check(features.enabled("что-то новое"), "незнакомый выключатель не глушит бота")


def test_switch() -> None:
    print("\n=== выключатель ===")
    import features
    features.set_enabled("training_polls", False)
    check(not features.enabled("training_polls"), "опросы тренировок выключены")
    check("Опросы тренировок" in features.off_list(), "и это видно в списке")
    check(features.enabled("game_polls"), "соседнее не задето")
    features.set_enabled("training_polls", True)
    check(features.enabled("training_polls"), "включается обратно")


def test_senders_respect_switch() -> None:
    """Отправители обязаны спрашивать выключатель, а не слать вслепую."""
    print("\n=== отправители слушаются ===")
    import features
    import game_system_manager as gsm_mod

    gsm = gsm_mod.GameSystemManager.__new__(gsm_mod.GameSystemManager)
    features.set_enabled("game_polls", False)
    try:
        check(not gsm._feature_on("game_polls"), "опросы на игру выключены для бота")
        check(gsm._feature_on("game_announcements"), "анонсы при этом работают")
    finally:
        features.set_enabled("game_polls", True)

    root = Path(__file__).resolve().parent.parent
    for name, key in (("vk_video.py", "game_video"),
                      ("birthday_notifications.py", "birthdays"),
                      ("training_polls_enhanced.py", "training_polls"),
                      ("game_results_monitor_final.py", "game_results"),
                      ("run_fantasy.py", "fantasy")):
        body = (root / name).read_text()
        check("features" in body and key in body,
              f"{name} спрашивает выключатель {key}")


async def test_screen(bd) -> None:
    print("\n=== экран тренера ===")
    import features
    text, markup, _ = await press(bd, "coach:cfg")
    check("coach:ft:list" in cbs(markup), "в настройках есть «Что делает бот»")

    text, markup, _ = await press(bd, "coach:ft:list")
    check("Опросы тренировок" in text and "Взносы за тренировки" in text,
          "перечислено, что можно выключить")
    check("coach:ft:dues_month" in cbs(markup), "у каждого своя кнопка")
    check(all(len(c.encode()) <= 64 for c in cbs(markup)), "кнопки в пределах 64 байт")

    text, markup, _ = await press(bd, "coach:ft:dues_month")
    check(not features.enabled("dues_month"), "нажали — выключилось")
    check("Выключил" in text and "Выключено:" in text, "и сказано, что именно")
    text, markup, _ = await press(bd, "coach:ft:dues_month")
    check(features.enabled("dues_month"), "нажали ещё раз — включилось")
    check("работает всё" in text, "и список выключенного пуст")


def main() -> int:
    print(f"База: {TMP}")
    bd = setup()
    test_defaults()
    test_switch()
    test_senders_respect_switch()
    asyncio.run(test_screen(bd))
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ВЫКЛЮЧАТЕЛИ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
