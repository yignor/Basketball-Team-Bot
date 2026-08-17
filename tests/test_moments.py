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
        {"player_id": "999", "kind": "pts2", "period": 1, "left": 300,
         "real": 500, "order": 4},
    ])


def test_only_mine() -> None:
    print("\n=== чужие моменты не показываем ===")
    mine = gt.moments(SOURCE, GAME, ME)
    check(len(mine) == 3, f"мои три: {len(mine)}")
    check(all(m["player_id"] == ME for m in mine), "и все мои")
    check(len(gt.moments(SOURCE, GAME)) == 4, "без фильтра видно всю игру")


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
    print("\n=== в списке только удачное ===")
    text = gt.format_moments(SOURCE, GAME, ME, VIDEO)
    check("Твои моменты" in text, "заголовок на месте")
    check("трёхочковый" in text and "подбор" in text and "перехват" in text,
          "все три действия названы")
    for word in ("промах", "мимо", "потер"):
        check(word not in text.lower(), f"нет «{word}» — этого никто не просил")
    check("на табло" in text, "есть сверка с табло")
    check(not gt.format_moments(SOURCE, GAME, "нет-такого"),
          "у кого моментов нет — пустая строка, а не пустой заголовок")


def test_ib_codes_are_the_verified_ones() -> None:
    """Коды Инфобаскета сверены с бокс-скором игры 1081391.

    Совпало число событий каждого типа по каждому игроку, и 1×1 + 2×2 + 3×3
    сошлось с очками. Если кто-то поправит словарь наугад, человек пойдёт
    смотреть чужой момент — поэтому таблица зафиксирована здесь."""
    print("\n=== коды лиг не разъехались ===")
    check(gt.IB_MOMENTS == {1: "ft", 2: "pts2", 3: "pts3",
                            26: "stl", 27: "blk", 28: "reb"},
          f"Инфобаскет: {gt.IB_MOMENTS}")
    check(4 not in gt.IB_MOMENTS and 5 not in gt.IB_MOMENTS
          and 6 not in gt.IB_MOMENTS, "промахи (4/5/6) в моменты не берём")
    # У Инфобаскета кода передачи я не нашёл — и не выдумывал.
    check("ast" not in gt.IB_MOMENTS.values(),
          "передачи Инфобаскета не угаданы наугад")
    check(gt.SLPRO_MOMENTS.get("ast") == "ast"
          and gt.SLPRO_MOMENTS.get("rebD") == "reb"
          and gt.SLPRO_MOMENTS.get("rebA") == "reb",
          f"SLPRO: {gt.SLPRO_MOMENTS}")
    check(all(k in gt.MOMENT_TITLES and k in gt.MOMENT_ICONS
              for k in set(gt.IB_MOMENTS.values()) | set(gt.SLPRO_MOMENTS.values())
              | {"pts2", "pts3", "ft"}),
          "у каждого вида есть подпись и значок")


def test_note_not_doubled() -> None:
    """Приписку про точность сдвига даёт только первый блок."""
    print("\n=== приписка не двоится ===")
    with_note = gt.format_moments(SOURCE, GAME, ME, VIDEO, with_note=True)
    without = gt.format_moments(SOURCE, GAME, ME, VIDEO, with_note=False)
    check(len(with_note) > len(without), "без приписки текст короче")
    check("<i>" not in without, "и тега приписки в нём нет")


def main() -> int:
    print(f"База: {TMP}")
    seed()
    test_only_mine()
    test_link_opens_before_the_action()
    test_text_has_no_misses()
    test_ib_codes_are_the_verified_ones()
    test_note_not_doubled()

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
