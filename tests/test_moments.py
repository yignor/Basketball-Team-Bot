#!/usr/bin/env python3
"""Твои моменты: отдельные действия игрока с привязкой к записи.

    python3 tests/test_moments.py

База временная, сеть не нужна. Проверяем разбор хроники и показ: ссылка должна
открывать запись чуть РАНЬШЕ действия, неудачное — идти после удачного, а коды
Инфобаскета оставаться теми, что сверены с бокс-скором.
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
        {"player_id": ME, "kind": "miss3", "period": 1, "left": 500,
         "real": 700, "order": 7},
        {"player_id": ME, "kind": "missft", "period": 4, "left": 20,
         "real": 3200, "order": 8},
        {"player_id": "999", "kind": "pts2", "period": 1, "left": 300,
         "real": 500, "order": 4},
    ])


def test_only_mine() -> None:
    print("\n=== чужие моменты не показываем ===")
    mine = gt.moments(SOURCE, GAME, ME)
    check(len(mine) == 7, f"мои семь: {len(mine)}")
    check(all(m["player_id"] == ME for m in mine), "и все мои")
    check(len(gt.moments(SOURCE, GAME)) == 8, "без фильтра видно всю игру")


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


def test_bad_goes_after_good() -> None:
    """В списке всё: и удачное, и потери с фолами и промахами.

    Порядок в сводке при этом жёсткий — сначала сделанное. Иначе она
    открывается фолом, а это не то, ради чего лезут в запись."""
    print("\n=== неудачное идёт после удачного ===")
    text, _, _ = gt.format_moments_page(SOURCE, GAME, ME, VIDEO)
    check("Твои моменты" in text, "заголовок на месте")
    check("трёхочковый" in text and "подбор" in text and "перехват" in text,
          "удачное названо")
    check("потеря" in text and "фол" in text, "потери и фолы тоже")
    check("промах трёхочкового" in text and "промах штрафного" in text,
          "промахи названы по типу броска, а не одним словом")
    check("на табло" in text, "есть сверка с табло")

    summary = text.splitlines()[1]
    first_bad = min(summary.index(gt.MOMENT_TITLES[k])
                    for k in gt.MOMENT_BAD if gt.MOMENT_TITLES[k] in summary)
    last_good = max(summary.index(gt.MOMENT_TITLES[k])
                    for k in ("reb", "stl", "pts3") if gt.MOMENT_TITLES[k] in summary)
    check(last_good < first_bad, f"всё плохое после всего хорошего: {summary}")
    empty, _, pages = gt.format_moments_page(SOURCE, GAME, "нет-такого")
    check(not empty and pages == 0,
          "у кого моментов нет — пусто, а не пустой заголовок")


def test_ib_codes_are_the_verified_ones() -> None:
    """Коды Инфобаскета сверены с бокс-скором игры 1081391.

    Совпало число событий каждого типа по каждому игроку, и 1×1 + 2×2 + 3×3
    сошлось с очками. Если кто-то поправит словарь наугад, человек пойдёт
    смотреть чужой момент — поэтому таблица зафиксирована здесь."""
    print("\n=== коды лиг не разъехались ===")
    check(gt.IB_MOMENTS == {1: "ft", 2: "pts2", 3: "pts3", 26: "stl",
                            27: "blk", 28: "reb", 11: "tur",
                            40: "pf", 41: "pf", 42: "pf",
                            4: "missft", 5: "miss2", 6: "miss3"},
          f"Инфобаскет: {gt.IB_MOMENTS}")
    # Фол — сумма трёх кодов: 41 и 42 редкие, по одному-два за матч, и на
    # одной игре разница потерялась бы. Сверено на трёх сразу.
    check(sorted(k for k, v in gt.IB_MOMENTS.items() if v == "pf") == [40, 41, 42],
          "фол собирается из трёх кодов")
    # Промахи сверены тождествами: 1+4 = попытки штрафных, 3+6 = попытки
    # трёхочковых, 2+3+5+6 = все броски с игры.
    check(gt.IB_MOMENTS[4] == "missft" and gt.IB_MOMENTS[5] == "miss2"
          and gt.IB_MOMENTS[6] == "miss3", "промахи разложены по типу броска")
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
              | {"pts2", "pts3", "ft", "tur", "pf",
                 "miss2", "miss3", "missft"}),
          "у каждого вида есть подпись и значок")


def test_pages_show_everything() -> None:
    """Список не обрывается: длинный режется на страницы, но целиком.

    Было «…и ещё 27», и досмотреть остальное было нельзя. Режем по ДЛИНЕ
    строк, а не по их числу: у лиг разной длины ссылки, и по счёту строк
    сообщение то не добирало до предела, то не влезало в него."""
    print("\n=== видно всё, страницами ===")
    long_id = "многобукв"
    gt.store_moments(SOURCE, "big", [
        {"player_id": long_id, "kind": "pts2", "period": 1 + i // 10,
         "left": 600 - i, "real": 100 + i * 20, "order": i}
        for i in range(60)])
    lines = gt.moment_lines(SOURCE, "big", long_id, VIDEO)
    check(len(lines) == 60, f"строк по числу моментов: {len(lines)}")

    pages = gt.moment_pages(lines)
    check(len(pages) > 1, f"длинный список разложен на страницы: {len(pages)}")
    check(sum(len(p) for p in pages) == 60, "ни одна строка не потерялась")
    check(all(p for p in pages), "пустых страниц нет")

    seen = []
    for i in range(len(pages)):
        text, page, total = gt.format_moments_page(SOURCE, "big", long_id, VIDEO, i)
        check(page == i and total == len(pages), f"страница {i}: {page}/{total}")
        check(len(text) < 4096, f"страница {i} влезает в сообщение: {len(text)}")
        check("Страница" in text, "номер страницы виден")
        # Признак строки момента — «-й период»: сводка сверху начинается с
        # того же значка, и по значку она попала бы в счёт.
        seen += [ln for ln in text.split("\n") if "-й период" in ln]
    check(len(seen) == 60, f"за все страницы показаны все моменты: {len(seen)}")

    # Выход за края не роняет экран, а прижимается к последней странице.
    _, page, _ = gt.format_moments_page(SOURCE, "big", long_id, VIDEO, 99)
    check(page == len(pages) - 1, f"номер страницы больше последней прижат: {page}")

    # Короткому списку страницы не нужны — незачем показывать «1 из 1».
    short, _, total = gt.format_moments_page(SOURCE, GAME, ME, VIDEO)
    check(total == 1 and "Страница" not in short, "короткому списку номера не нужны")


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


def test_broken_anchors_do_not_squash_the_game() -> None:
    """Негодная привязка лиги не должна сминать период в мгновение.

    В протоколе матча 22.08.2026 отметки видео за весь первый период сдвинулись
    на одиннадцать секунд: десять минут игры уместились в мгновение. Все моменты
    периода вставали вплотную к спорному, и промах «на табло 3:59» показывался
    на 3:30 записи — раньше, чем начался матч. Второй дефект того же протокола:
    одиночный якорь в овертайме, улетевший на восемнадцать часов вперёд.

    Данные здесь — точная выжимка того протокола, сеть не нужна."""
    print("\n=== негодная привязка лиги ===")
    offsets = {1: 0.0, 2: 600.0, 3: 1200.0, 4: 1800.0, 5: 2400.0}
    zero = 1787398013.0

    # Первый период: часы идут, видео стоит. Остальные — живые.
    anchors = {1: [(c, zero + c * 0.018) for c in range(0, 601, 60)],
               2: [(600 + c, zero + 11 + c * 1.35) for c in range(0, 601, 60)],
               3: [(1200 + c, zero + 1019 + c * 1.64) for c in range(0, 601, 60)],
               4: [(1800 + c, zero + 2075 + c * 2.55) for c in range(0, 601, 60)]}
    # Овертайм: нормальный ряд плюс одна отметка на восемнадцать часов позже.
    anchors[5] = [(2400 + c, zero + 3686 + c * 2.2) for c in range(0, 301, 60)]
    anchors[5].append((2700.0, zero + 70600.0))

    cleaned = gt._drop_outliers(sorted(anchors[5]))
    check(len(cleaned) == len(anchors[5]) - 1,
          f"улетевший якорь выброшен: было {len(anchors[5])}, стало {len(cleaned)}")

    check(not (gt.SANE_PACE[0] <= (gt._pace_of(sorted(anchors[1])) or 0)
               <= gt.SANE_PACE[1]),
          "замерший первый период признан негодным")

    plan, trusted = gt._timeline(anchors, offsets, zero)
    check(not trusted, "игра помечена как посчитанная не целиком по протоколу")
    check(plan[1][0] == "model", "первый период посчитан по среднему темпу")

    # Ключевое: моменты идут по возрастанию и не налезают на спорный.
    every = sorted(a for items in anchors.values() for a in items)
    at_359 = gt._at(plan, 1, 361.0, every) - zero      # на табло 3:59
    at_322 = gt._at(plan, 1, 398.0, every) - zero      # на табло 3:22
    check(at_359 > 300, f"момент шестой минуты не у самого начала: {at_359:.0f} с")
    check(at_322 > at_359, "более поздний момент идёт позже")
    grew = (at_322 - at_359) / (398.0 - 361.0)
    check(1.0 <= grew <= 4.0, f"запись идёт быстрее игры, но не втрое: {grew:.2f}")

    # И периоды не наезжают друг на друга.
    ends = [gt._at(plan, p, offsets[p] + 600.0, every) - zero for p in (1, 2, 3, 4)]
    starts = [gt._at(plan, p, offsets[p], every) - zero for p in (1, 2, 3, 4, 5)]
    order = all(starts[i + 1] > ends[i] for i in range(4))
    check(order, f"периоды идут подряд: {[round(x) for x in starts]}")


def test_note_tells_about_modelled_time() -> None:
    """Про посчитанное по модели человеку говорят прямо."""
    print("\n=== оговорка про приблизительность ===")
    gt.set_timing(SOURCE, GAME, False)
    notes = gt._notes(SOURCE, GAME)
    check(any("среднему темпу" in n for n in notes),
          "оговорка появилась: " + " | ".join(n[:40] for n in notes))
    gt.set_timing(SOURCE, GAME, True)
    notes = gt._notes(SOURCE, GAME)
    check(not any("среднему темпу" in n for n in notes),
          "и пропадает, когда разметка честная")
    check(len(notes) == 1, "лишних оговорок не добавилось")


def test_digest_waits_for_a_decent_hour() -> None:
    """Разбор своей игры не ждёт утра, чужой ночной протокол — ждёт.

    Жалоба 25.08.2026: матч был 24-го в 21:10, статистика легла в 22:46, а
    разбор пришёл в 9:00 следующего дня. Окно личных сообщений кончалось в
    22:00, и после вечерней игры разбор не мог прийти в тот же день никогда:
    из четырнадцати разных времён начала пять приходятся на 20:00 и позже.

    Обратный случай тоже настоящий: 10.08.2026 протокол дневной игры лёг в
    04:31, и разбор ушёл ночью. Его придерживать по-прежнему надо."""
    print("\n=== разбор и тихие часы ===")
    import personal_game
    from datetime import datetime, timedelta
    from datetime_utils import MOSCOW_TZ

    sheets_cache.init_db()
    now_iso = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM service_records WHERE game_id IN "
                     "('slpro-4586','1082250')")
        # SLPRO: приставка в id и дата по ISO. Инфобаскет: голый id и ДД.ММ.ГГГГ.
        conn.execute(
            "INSERT INTO service_records (unique_key, logged_at, created_at, "
            "updated_at, data_type, game_id, game_date, game_time, deleted) "
            "VALUES ('a', ?, ?, ?, 'АНОНС_ИГРА_SLPRO', 'slpro-4586', "
            "'2026-08-24', '21:10', 0)", (now_iso, now_iso, now_iso))
        conn.execute(
            "INSERT INTO service_records (unique_key, logged_at, created_at, "
            "updated_at, data_type, game_id, game_date, game_time, deleted) "
            "VALUES ('b', ?, ?, ?, 'ОПРОС_ИГРА', '1082250', "
            "'22.08.2026', '14:00', 0)", (now_iso, now_iso, now_iso))
        conn.commit()

    started = personal_game.started_at("slpro", "4586")
    check(started is not None and started.hour == 21,
          f"время начала найдено по id без приставки: {started}")
    check(personal_game.started_at("infobasket", "1082250") is not None,
          "и у второй лиги, с другим форматом даты")

    evening = datetime(2026, 8, 24, 22, 46, tzinfo=MOSCOW_TZ)
    check(personal_game.just_played("slpro", {"game_id": "4586"}, evening),
          "через полтора часа после начала — «только что отыграл»")

    midnight = datetime(2026, 8, 25, 0, 25, tzinfo=MOSCOW_TZ)
    check(personal_game.just_played("slpro", {"game_id": "4586"}, midnight),
          "и в 00:25 ещё считается своей игрой")

    late = datetime(2026, 8, 25, 4, 31, tzinfo=MOSCOW_TZ)
    check(not personal_game.just_played("slpro", {"game_id": "4586"}, late),
          "а в 04:31 уже нет — это ночь, а не после матча")

    # Дневная игра, протокол под утро: ровно случай 10.08.
    night = datetime(2026, 8, 23, 4, 31, tzinfo=MOSCOW_TZ)
    check(not personal_game.just_played("infobasket", {"game_id": "1082250"}, night),
          "ночной протокол дневной игры свежим не считается")

    check(personal_game.FRESH_AFTER_START_HOURS >= 3,
          "запаса хватает на матч с овертаймом и задержкой протокола")

    # Хвост в демоне кончается на 00:30 — иначе правило некуда приложить.
    src = (ROOT / "bot_daemon.py").read_text()
    check("NIGHT_TAIL_MINUTE = 30" in src, "вечерний хвост открыт до 00:30")
    body = src[src.index("async def _personal_digests"):]
    body = body[:body.index("\nasync def ", 1)]
    check("just_played" in body, "поштучная проверка стоит в отправке")


def main() -> int:
    print(f"База: {TMP}")
    seed()
    test_broken_anchors_do_not_squash_the_game()
    test_note_tells_about_modelled_time()
    test_digest_waits_for_a_decent_hour()
    test_only_mine()
    test_link_opens_before_the_action()
    test_bad_goes_after_good()
    test_ib_codes_are_the_verified_ones()
    test_pages_show_everything()
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
