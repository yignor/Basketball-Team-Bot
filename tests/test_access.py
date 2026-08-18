#!/usr/bin/env python3
"""Граница платного: что видит человек до оплаты и что после.

    python3 tests/test_access.py

База временная. Проверяем решение от 17.08.2026: привязка профиля бесплатна и
открыта всем, платный — только разбор. Раньше привязку мог сделать лишь админ,
и из 46 человек команды привязались двое.

Главное, что здесь стережётся: дразнилка не должна протекать. Она называет
охват (сколько игр в базе) — и ни одной цифры из самого разбора.
"""

from __future__ import annotations

import os
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="access-test-")) / "bot.db"
os.environ.setdefault("BOT_TOKEN", "0:test")
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""
# Админов нет: иначе проверяемый ниже «человек без доступа» окажется админом.
os.environ["ADMIN_USER_IDS"] = ""
os.environ["ADMIN_USER_ID"] = ""
os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))

import sheets_cache                                             # noqa: E402
sheets_cache.DB_PATH = TMP

import bot_daemon as bd                                         # noqa: E402
import personal_report                                          # noqa: E402

SOURCE = "slpro"
PLAYER = "933"
PAID, FREE = 111, 222
bad: List[str] = []


class FakeUser:
    """Ровно то, что читают проверки доступа: id и ник."""

    def __init__(self, uid: int) -> None:
        self.id = uid
        self.username = f"u{uid}"


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def seed() -> None:
    """Десять игр одного человека — чтобы у разбора было что показывать."""
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        for i in range(10):
            day = (date(2026, 5, 1) + timedelta(days=i * 7)).isoformat()
            conn.execute(
                "INSERT OR REPLACE INTO game_player_stats (source, game_id, "
                "game_date, player_id, team_id, pts, reb, ast, stl, blk, tur, "
                "fetched_at) VALUES (?, ?, ?, ?, '707', ?, 5, 3, 1, 0, 2, ?)",
                (SOURCE, f"g{i}", day, PLAYER, 10 + i, now))
        conn.commit()


def test_link_is_free() -> None:
    """Привязка не требует доступа — иначе воронка не начинается."""
    print("\n=== привязаться может любой ===")
    import player_identity
    res = player_identity.link_identity(
        FREE, {"source": SOURCE, "player_id": PLAYER, "api_url": ""})
    check(bool(res) and not res.get("error"), f"привязка прошла: {res}")
    ids = player_identity.get_identities(FREE)
    check(len(ids) == 1 and ids[0]["player_id"] == PLAYER,
          f"профиль запомнен: {ids}")
    check(not personal_report.stats_open(FREE),
          "но разбор ему по-прежнему закрыт")


def test_teaser_does_not_leak() -> None:
    """Дразнилка называет охват и молчит про содержимое разбора."""
    print("\n=== дразнилка не протекает ===")
    teaser = bd._progress_or_teaser(FakeUser(FREE), SOURCE, PLAYER)
    full = bd._format_progress(SOURCE, PLAYER)

    check("10" in teaser, f"охват назван: {teaser.splitlines()[0]}")
    check("🔒" in teaser, "видно, что раздел закрыт")
    check(teaser != full, "это не тот же текст, что для оплативших")

    # Слова «средние», «форма» в дразнилке есть намеренно — это перечисление
    # того, что покупают. Течь может не слово, а число, поэтому сверяем строки
    # разбора целиком: в платном тексте все они с отступа.
    leaked = [ln.strip() for ln in full.splitlines()
              if ln.startswith("   ") and ln.strip() and ln.strip() in teaser]
    check(not leaked, f"ни одна строка разбора не попала в дразнилку: {leaked}")

    # Средние по нашим данным: очки идут 10..19, среднее 14.5.
    check("14.5" not in teaser, "среднее не просочилось")


def test_access_opens_and_expires() -> None:
    """Доступ выдаётся до даты и сам заканчивается."""
    print("\n=== доступ живёт до числа ===")
    tomorrow = (date.today() + timedelta(days=1)).isoformat()
    sheets_cache.grant_access_id(sheets_cache.ACCESS_PERSONAL, str(PAID),
                                 f"u{PAID}", "тест", tomorrow)
    check(personal_report.stats_open(PAID), f"открыт до {tomorrow}")
    full = bd._progress_or_teaser(FakeUser(PAID), SOURCE, PLAYER)
    check("в среднем" in full.lower(), "оплативший видит разбор")

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    sheets_cache.grant_access_id(sheets_cache.ACCESS_PERSONAL, str(PAID),
                                 f"u{PAID}", "тест", yesterday)
    check(not personal_report.stats_open(PAID),
          "вчерашняя дата доступа не даёт")
    back = bd._progress_or_teaser(FakeUser(PAID), SOURCE, PLAYER)
    check("🔒" in back, "и человек снова видит дразнилку, а не разбор")


def test_old_buttons_die_with_access() -> None:
    """Кнопки под уже отправленными сообщениями живут вечно.

    Сообщение с разбором остаётся в переписке навсегда, и кнопки под ним
    нажимаются и через год. Проверка обязана стоять на самом нажатии, а не
    только на входе в раздел."""
    print("\n=== старая кнопка не переживает доступ ===")
    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index("async def handle_report_prefs_callback")
    head = src[at:at + 1200]
    check("_can_see_personal" in head,
          "обработчик кнопок rep:* проверяет доступ на каждом нажатии")

    # Рассылки — тот же продукт, что и кнопки: без проверки они продолжали бы
    # приходить сами, и закрытая кнопка ничего бы не значила.
    for mod in ("personal_game.py", "monthly_report.py"):
        check("stats_open" in (ROOT / mod).read_text(),
              f"{mod} не рассылает закрытое")


def test_admin_owns_rebinding() -> None:
    """Сменить привязку может только тренер.

    Иначе оплативший перецепляется на товарища и пересказывает ему платный
    разбор — подписка расходится по команде за вечер."""
    print("\n=== перепривязка только через админа ===")
    src = (ROOT / "bot_daemon.py").read_text()
    at = src.index("async def handle_profile_link")
    body = src[at:at + 3000]
    # Ищем сам текст отказа, а не слово «админ»: оно встречается и в
    # пояснении наверху функции, и позиция тогда сравнивается не с тем.
    refusal = "🔒 Профиль уже привязан"
    check(refusal in body,
          "игроку на попытку смены отвечают отказом, а не молчанием")
    check(body.index("_awaiting_identity") < body.index(refusal),
          "ссылка админа уходит выбранному игроку раньше проверки на смену")

    # Экран привязки живёт в админ-панели, а она целиком за _is_admin. Тренеру
    # туда хода нет намеренно: он рядом с командой каждый день, и просьбу
    # «перецепи на минутку» ему проще выполнить, чем отказать.
    at_router = src.index("async def handle_admin_callback")
    check("_is_admin" in src[at_router:at_router + 400],
          "весь админ-роутер, включая привязку, закрыт проверкой на админа")

    # Инструмент, ради которого запрет вообще возможен.
    for what in ("_identity_screen", "_identity_who", "_identity_set",
                 "_identity_off"):
        check(what in src, f"у админа есть {what}")


def test_candidates_by_name() -> None:
    """Кандидатов ищем по именам, которые лиги уже прислали в протоколах.

    Это те же имена, по которым игроки видят себя в фэнтези, — значит,
    привязать команду можно не дожидаясь, пока каждый пришлёт ссылку."""
    print("\n=== кандидаты подбираются по ФИО ===")
    import player_identity
    import player_names
    player_names.clear()
    player_names.put(SOURCE, PLAYER, "Шлепикас Роман")
    player_names.put(SOURCE, "555", "Иванов Иван")
    # Инфобаскет пишет того же человека наоборот — это должно совпасть.
    player_names.put("infobasket", "400566", "Роман Шлепикас")

    found = player_identity.suggest_for_name("Шлепикас Роман")
    ids = sorted((c["source"], c["player_id"]) for c in found)
    check(ids == [("infobasket", "400566"), (SOURCE, PLAYER)],
          f"нашлись обе лиги, чужой не попал: {ids}")
    check(found[0]["player_id"] == PLAYER,
          f"первым тот, у кого игры есть: {found[0]}")

    # Опечатка в одну букву — тоже он: лист и лига расходятся ровно на это.
    check(any(c["player_id"] == PLAYER
              for c in player_identity.suggest_for_name("Шлепикас Романа")),
          "опечатка в одну букву не рвёт совпадение")
    check(not player_identity.suggest_for_name("Петров Пётр"),
          "чужому ФИО кандидатов не выдумываем")


def test_unlink() -> None:
    print("\n=== отвязать можно ===")
    import player_identity
    check(player_identity.unlink(FREE, SOURCE) == 1, "привязка снята")
    check(not player_identity.get_identities(FREE), "и её больше нет")


def main() -> int:
    print(f"База: {TMP}")
    seed()
    test_link_is_free()
    test_teaser_does_not_leak()
    test_access_opens_and_expires()
    test_old_buttons_die_with_access()
    test_admin_owns_rebinding()
    test_candidates_by_name()
    test_unlink()

    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ДОСТУП: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
