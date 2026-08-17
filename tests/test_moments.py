#!/usr/bin/env python3
"""Твои моменты: отдельные действия игрока с привязкой к записи.

    python3 tests/test_moments.py

База временная, сеть не нужна. Проверяем разбор хроники и показ: ссылка должна
открывать запись чуть РАНЬШЕ действия, промахов в списке быть не должно, а
коды Инфобаскета — оставаться теми, что сверены с бокс-скором.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="moments-test-")) / "bot.db"
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                             # noqa: E402
sheets_cache.DB_PATH = TMP

import game_timeline as gt                                      # noqa: E402

SOURCE, GAME, ME = "slpro", "4558", "1559"
VIDEO = "https://vk.com/video-50561253_456243901"
bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def seed() -> None:
    sheets_cache.init_db()
    gt.store_moments(SOURCE, GAME, [
        {"player_id": ME, "kind": "pts3", "period": 1, "left": 362,
         "real": 400, "order": 1},
        {"player_id": ME, "kind": "reb", "period": 2, "left": 120,
         "real": 1500, "order": 2},
        {"player_id": ME, "kind": "stl", "period": 4, "left": 45,
         "real": 3000, "order": 3},
        {"player_id": ME, "kind": "tur", "period": 2, "left": 300,
         "real": 1200, "order": 5},
        {"player_id": ME, "kind": "pf", "period": 3, "left": 200,
         "real": 2200, "order": 6},
        {"player_id": "999", "kind": "pts2", "period": 1, "left": 300,
         "real": 500, "order": 4},
    ])


def test_only_mine() -> None:
    print("\n=== чужие моменты не показываем ===")
    mine = gt.moments(SOURCE, GAME, ME)
    check(len(mine) == 5, f"мои пять: {len(mine)}")
    check(all(m["player_id"] == ME for m in mine), "и все мои")
    check(len(gt.moments(SOURCE, GAME)) == 6, "без фильтра видно всю игру")


def test_link_opens_before_the_action() -> None:
    """Ссылка ведёт РАНЬШЕ действия.

    Открыть ровно на отметке протокола — значит показать мяч уже в кольце.
    Человек хочет увидеть проход и пас, из которых бросок получился."""
    print("\n=== ссылка открывает до броска ===")
    items = gt.moment_codes(SOURCE, GAME, ME, VIDEO)
    three = next(m for m in items if m["kind"] == "pts3")
    check(three["at"] == 400 - gt.MOMENT_LEAD_SECONDS,
          f"отступ назад на {gt.MOMENT_LEAD_SECONDS} с: {three['at']}")
    check("?t=" in three["link"], f"ссылка с меткой времени: {three['link']}")
    check(items[0]["at"] <= items[-1]["at"], "по возрастанию времени")

    # Начало записи: отступ не должен уводить в минус.
    gt.store_moments(SOURCE, "edge", [
        {"player_id": ME, "kind": "pts2", "period": 1, "left": 600,
         "real": 2, "order": 1}])
    edge = gt.moment_codes(SOURCE, "edge", ME, VIDEO)
    check(edge[0]["at"] == 0, f"у самого начала не уходим в минус: {edge[0]['at']}")


def test_text_has_no_misses() -> None:
    """Потери и фолы показываем, промахи — нет.

    Потерю и фол пересматривают, чтобы понять, что пошло не так. А бросок мимо
    виден и по статистике: ссылка на каждый превратила бы список в перечень
    неудач."""
    print("\n=== потери и фолы есть, промахов нет ===")
    text = gt.format_moments(SOURCE, GAME, ME, VIDEO)
    check("Твои моменты" in text, "заголовок на месте")
    check("трёхочковый" in text and "подбор" in text and "перехват" in text,
          "удачное названо")
    check("потеря" in text and "фол" in text, "потери и фолы тоже")
    for word in ("промах", "мимо"):
        check(word not in text.lower(), f"нет «{word}» — этого никто не просил")
    check("на табло" in text, "есть сверка с табло")

    # Сводка начинается с того, ради чего запись открывают.
    summary = text.splitlines()[1]
    check(summary.index("подбор") < summary.index("потеря"),
          f"плохое в сводке идёт после хорошего: {summary}")
    check(not gt.format_moments(SOURCE, GAME, "нет-такого"),
          "у кого моментов нет — пустая строка, а не пустой заголовок")


def test_ib_codes_are_the_verified_ones() -> None:
    """Коды Инфобаскета сверены с бокс-скором игры 1081391.

    Совпало число событий каждого типа по каждому игроку, и 1×1 + 2×2 + 3×3
    сошлось с очками. Если кто-то поправит словарь наугад, человек пойдёт
    смотреть чужой момент — поэтому таблица зафиксирована здесь."""
    print("\n=== коды лиг не разъехались ===")
    check(gt.IB_MOMENTS == {1: "ft", 2: "pts2", 3: "pts3", 26: "stl",
                            27: "blk", 28: "reb", 11: "tur",
                            40: "pf", 41: "pf", 42: "pf"},
          f"Инфобаскет: {gt.IB_MOMENTS}")
    # Фол — сумма трёх кодов: 41 и 42 редкие, по одному-два за матч, и на
    # одной игре разница потерялась бы. Сверено на трёх сразу.
    check(sorted(k for k, v in gt.IB_MOMENTS.items() if v == "pf") == [40, 41, 42],
          "фол собирается из трёх кодов")
    check(4 not in gt.IB_MOMENTS and 5 not in gt.IB_MOMENTS
          and 6 not in gt.IB_MOMENTS, "промахи (4/5/6) в моменты не берём")
    # У Инфобаскета кода передачи я не нашёл — и не выдумывал.
    check("ast" not in gt.IB_MOMENTS.values(),
          "передачи Инфобаскета не угаданы наугад")
    check(gt.SLPRO_MOMENTS.get("ast") == "ast"
          and gt.SLPRO_MOMENTS.get("rebD") == "reb"
          and gt.SLPRO_MOMENTS.get("rebA") == "reb"
          and gt.SLPRO_MOMENTS.get("tur") == "tur"
          and gt.SLPRO_MOMENTS.get("foul") == "pf",
          f"SLPRO: {gt.SLPRO_MOMENTS}")
    # Фол НА игроке — другое событие, и в один список с «твой фол» не идёт.
    check("foul_on_player" not in gt.SLPRO_MOMENTS,
          "фол на игроке не путаем с фолом игрока")
    check(all(k in gt.MOMENT_TITLES and k in gt.MOMENT_ICONS
              for k in set(gt.IB_MOMENTS.values()) | set(gt.SLPRO_MOMENTS.values())
              | {"pts2", "pts3", "ft", "tur", "pf"}),
          "у каждого вида есть подпись и значок")


def test_note_not_doubled() -> None:
    """Приписку про точность сдвига даёт только первый блок."""
    print("\n=== приписка не двоится ===")
    with_note = gt.format_moments(SOURCE, GAME, ME, VIDEO, with_note=True)
    without = gt.format_moments(SOURCE, GAME, ME, VIDEO, with_note=False)
    check(len(with_note) > len(without), "без приписки текст короче")
    check("<i>" not in without, "и тега приписки в нём нет")


def test_admin_can_show_anyone() -> None:
    """Админ показывает тайм-коды за любого игрока — это демонстрация.

    Через «🎬 Я в записи» так не сделать: там человек видит только себя. А
    показать вживую, за что просим деньги, надо ещё до того, как человек
    привязал профиль и заплатил."""
    print("\n=== админ показывает за любого ===")
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    import bot_daemon as bd
    import player_names

    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO game_meta (source, game_id, game_date, "
            "home_name, guest_name, home_team_id, guest_team_id, video_vk, "
            "fetched_at) VALUES (?, ?, '2026-08-16', 'Балтика', 'PullUp Farm', "
            "'711', '707', ?, ?)", (SOURCE, GAME, VIDEO, now))
        for pid, team, pts in ((ME, "707", 18), ("999", "711", 25)):
            conn.execute(
                "INSERT OR REPLACE INTO game_player_stats (source, game_id, "
                "game_date, player_id, team_id, number, pts, reb, ast, "
                "fetched_at) VALUES (?, ?, '2026-08-16', ?, ?, '7', ?, 4, 2, ?)",
                (SOURCE, GAME, pid, team, pts, now))
        conn.commit()
    player_names.put(SOURCE, ME, "Шлепикас Роман")

    text, markup = bd._tc_players(SOURCE, GAME)
    data = [b.callback_data for row in markup.inline_keyboard for b in row]
    check(any(f"admin:tc:show:{SOURCE}:{GAME}:{ME}" == d for d in data),
          "свой игрок в списке")
    check(any(f"admin:tc:show:{SOURCE}:{GAME}:999" == d for d in data),
          "игрок соперника тоже — протокол лиги публичный")
    labels = " ".join(b.text for row in markup.inline_keyboard for b in row)
    check("Шлепикас Роман" in labels, f"имя подставлено: {labels[:60]}")
    check("✨" in labels, "у кого моменты разобраны — помечен")

    # Тот же экран, что видит игрок, только кнопки возврата свои: показывать
    # разное было бы двумя разными правдами.
    back = [[bd.InlineKeyboardButton("назад", callback_data="admin:tc:games")]]
    shown, markup2 = bd._my_video_game(SOURCE, GAME, ME, back)
    tail = [b.callback_data for row in markup2.inline_keyboard for b in row]
    check("admin:tc:games" in tail, f"возврат ведёт в админку: {tail}")
    check("rep:back" not in tail, "и не в личный отчёт игрока")
    check("Твои моменты" in shown, "моменты показаны")


def main() -> int:
    print(f"База: {TMP}")
    seed()
    test_only_mine()
    test_link_opens_before_the_action()
    test_text_has_no_misses()
    test_ib_codes_are_the_verified_ones()
    test_note_not_doubled()
    test_admin_can_show_anyone()

    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("МОМЕНТЫ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
