#!/usr/bin/env python3
"""Частные занятия тренера: сквозные сценарии на чистой базе.

    python3 tests/test_private.py

База — временная и своя. Это не экономия: раздел не должен зависеть ни от
одной командной таблицы, и тест на пустой базе — самая честная проверка этого
обещания. Если что-то здесь потянется к листу «Игроки», сценарий упадёт.

Наружу ничего не уходит: бот подменён заглушкой, и один из тестов прямо
следит, чтобы за весь прогон из раздела не ушло ни одного сообщения.
"""

from __future__ import annotations

import ast
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

TMP = Path(tempfile.mkdtemp(prefix="priv-test-")) / "priv.db"

COACH = FakeUser(uid=700100, username="coach")
OTHER = FakeUser(uid=700200, username="other_coach")
BOT = FakeBot()

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def setup() -> Any:
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ["ADMIN_USER_IDS"] = f"{COACH.id},{OTHER.id}"
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
    os.environ["SPREADSHEET_ID"] = ""
    import sheets_cache
    sheets_cache.DB_PATH = TMP
    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None
    return bd


async def press(bd, data: str, who: FakeUser = COACH):
    q = FakeQuery(data, who, BOT)
    await bd.handle_private_callback(FakeUpdate(query=q, user=who), FakeContext(BOT))
    last = (q.screens or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"], q


async def say(bd, text: str, who: FakeUser = COACH):
    msg = FakeMessage(text=text, bot=BOT, user=who)
    try:
        await bd.handle_private_text(FakeUpdate(message=msg, user=who),
                                     FakeContext(BOT))
    except Exception as exc:                     # ApplicationHandlerStop — норма
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    last = (msg.replies or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"]


def cbs(markup) -> List[str]:
    return [b.callback_data for b in buttons_of(markup) if b.callback_data]


def rub(amount: int) -> str:
    """Сумма ровно так, как её печатает бот.

    Внутри неразрывный пробел, и глазами он неотличим от обычного: сравнивать
    с литералом «1 500 ₽» бесполезно — тест падал бы на верном коде."""
    import bot_daemon as bd
    return bd._rub(amount)


def newest(pl, uid: int) -> int:
    """id только что заведённого занятия.

    Список отсортирован по дате, а не по времени создания: «ближайшее сверху»
    — то, что нужно тренеру, но не то, что нужно тесту сразу после ввода."""
    return max(s["id"] for s in pl.sessions(uid, limit=200))


def find(markup, needle: str) -> str:
    """callback первой кнопки, чей текст содержит needle."""
    for b in buttons_of(markup):
        if needle.lower() in (b.text or "").lower():
            return b.callback_data
    return ""


# ─────────────────────────── сценарии ──────────────────────────────────────


async def test_full_path(bd) -> None:
    """Путь тренера целиком: от пустого раздела до закрытого долга.

    Ровно то, ради чего раздел заводится: составить список, выбрать, кто
    придёт, назначить цену, отметить оплату и увидеть долг оставшегося."""
    print("\n=== от пустого раздела до закрытого долга ===")
    import private_lessons as pl

    text, markup, _ = await press(bd, "pl:main")
    check("Частные занятия" in text, "раздел открывается")
    check("не задана" in text, "пустая цена названа прямо, а не подразумевается")

    await press(bd, "pl:price")
    text, _ = await say(bd, "1500")
    check(rub(1500) in text, f"цена занятия записана: {text.splitlines()[0]}")

    await press(bd, "pl:add")
    text, _ = await say(bd, "Иванов Иван Иванович")
    check("Иванов И. И." in text, f"полное имя укорочено: {text.splitlines()[0]}")
    await press(bd, "pl:add")
    await say(bd, "Петров Пётр")

    folk = pl.people(COACH.id)
    check(len(folk) == 2, f"в списке двое: {[p['label'] for p in folk]}")
    ivan = next(p for p in folk if p["label"].startswith("Иванов"))
    petr = next(p for p in folk if p["label"].startswith("Петров"))

    # Петрову — своя цена: у частных занятий это обычное дело.
    await press(bd, f"pl:pprice:{petr['id']}")
    text, _ = await say(bd, "1000")
    check(f"{rub(1000)} (своя)" in text, f"личная цена в карточке: {text}")

    await press(bd, "pl:new")
    text, markup = await say(bd, "12.08 19:00 Зал на Ленина")
    check("Зал на Ленина" in text, f"занятие заведено одной строкой: {text[:60]}")
    check(any(c.startswith("pl:t:") for c in cbs(markup)),
          "сразу спросили, кто идёт — без лишнего шага")

    sid = newest(pl, COACH.id)
    await press(bd, f"pl:t:{sid}:{ivan['id']}")
    text, markup, _ = await press(bd, f"pl:t:{sid}:{petr['id']}")
    check(sum(1 for b in buttons_of(markup) if (b.text or "").startswith("✅")) == 2,
          "оба отмечены галочкой")

    text, markup, _ = await press(bd, f"pl:s:{sid}")
    check(rub(2500) in text, f"итог занятия считает личную цену: {text}")

    text, markup, _ = await press(bd, f"pl:done:{sid}")
    check("оплатил" in text or "должен" in text, "после занятия видно, кто заплатил")
    check(pl.balance(COACH.id, ivan["id"]) == 1500,
          "начислено по общей цене")
    check(pl.balance(COACH.id, petr["id"]) == 1000,
          "начислено по личной цене")

    paid = find(markup, "Иванов")
    check(bool(paid), "на проведённом занятии человек — кнопка отметки оплаты")
    text, markup, _ = await press(bd, paid)
    check(pl.balance(COACH.id, ivan["id"]) == 0, "оплата закрыла начисление")

    text, markup, _ = await press(bd, "pl:debts")
    check("Петров" in text and "Иванов" not in text,
          f"в должниках только неоплативший: {text.splitlines()[2:4]}")


async def test_price_frozen(bd) -> None:
    """Цена выросла — старые долги не выросли.

    Иначе поднятие цены задним числом переписывало бы всё, за что человек уже
    рассчитывался, и разговор с ним становился невозможным."""
    print("\n=== цена меняется вперёд, а не назад ===")
    import private_lessons as pl
    folk = pl.people(COACH.id)
    petr = next(p for p in folk if p["label"].startswith("Петров"))
    was = pl.balance(COACH.id, petr["id"])

    await press(bd, "pl:price")
    await say(bd, "2000")
    check(pl.balance(COACH.id, petr["id"]) == was,
          f"старое начисление не тронуто: было {was}, стало "
          f"{pl.balance(COACH.id, petr['id'])}")

    # А новое занятие уже по новой цене — но у Петрова личная, она сильнее.
    await press(bd, "pl:new")
    await say(bd, "завтра 19:00")
    sid = newest(pl, COACH.id)
    ivan = next(p for p in folk if p["label"].startswith("Иванов"))
    await press(bd, f"pl:t:{sid}:{ivan['id']}")
    s = pl.session(COACH.id, sid)
    check(s["members"][0]["price"] == 2000, f"новое занятие по новой цене: {s['total']}")


async def test_advance(bd) -> None:
    """Заплатили вперёд — это аванс, а не долг наоборот."""
    print("\n=== аванс и отмена занятия ===")
    import private_lessons as pl
    folk = pl.people(COACH.id)
    petr = next(p for p in folk if p["label"].startswith("Петров"))

    await press(bd, f"pl:pay:{petr['id']}")
    text, _ = await say(bd, "5000 за пять занятий")
    check("аванс" in text.lower(), f"переплата названа авансом: {text[:80]}")
    check(pl.balance(COACH.id, petr["id"]) < 0, "баланс ушёл в минус")

    # Отменяем проведённое занятие: начисление снимается, деньги остаются.
    done = [s for s in pl.sessions(COACH.id) if s["status"] == pl.DONE][0]
    before = pl.balance(COACH.id, petr["id"])
    await press(bd, f"pl:offok:{done['id']}")
    after = pl.balance(COACH.id, petr["id"])
    check(after == before - 1000,
          f"начисление снято, деньги остались: {before} → {after}")
    check(all(s["id"] != done["id"] for s in pl.sessions(COACH.id)),
          "отменённое занятие ушло из списка")


async def test_free_input(bd) -> None:
    """Свободный ввод: тупика быть не должно нигде."""
    print("\n=== свободный ввод и понятные отказы ===")
    import private_lessons as pl

    await press(bd, "pl:add")
    text, _ = await say(bd, "Иванов Иван Иванович")
    check("уже есть" in text, f"тёзка отсекается и объясняет, что делать: {text[:70]}")
    text, _ = await say(bd, "Иванов И. мл")
    check("Иванов И. мл" in text, f"пометка тренера уцелела: {text.splitlines()[0]}")

    await press(bd, "pl:new")
    text, _ = await say(bd, "как-нибудь потом")
    check("Не понял дату" in text, "непонятную дату переспрашиваем")
    text, markup = await say(bd, "сегодня")
    check(any(c.startswith("pl:t:") for c in cbs(markup)),
          "после переспроса ввод принят, а не потерян")

    await press(bd, "pl:price")
    text, _ = await say(bd, "дорого")
    check("числом" in text, "на «дорого» просим число, а не молчим")
    await say(bd, "1500")


async def test_owner_isolation(bd) -> None:
    """Чужой раздел не виден даже другому тренеру.

    Частный заработок — личное дело человека. Тренеров в боте несколько, и
    показать соседу «заодно» его людей и его деньги нельзя."""
    print("\n=== у каждого тренера свой раздел ===")
    import private_lessons as pl

    text, markup, _ = await press(bd, "pl:who", who=OTHER)
    check("Пока никого" in text, f"второй тренер видит пустой список: {text[:40]}")
    check(pl.people(OTHER.id) == [], "и в базе у него ничего нет")

    mine = pl.people(COACH.id)
    text, _, q = await press(bd, f"pl:p:{mine[0]['id']}", who=OTHER)
    check("нет" in text.lower(),
          f"по прямой ссылке чужая карточка не открывается: {text[:50]}")

    sid = pl.sessions(COACH.id)[0]["id"]
    text, _, _ = await press(bd, f"pl:s:{sid}", who=OTHER)
    check("не найдено" in text.lower(), "и чужое занятие тоже")

    # Порча чужого через кнопку тоже не проходит.
    await press(bd, f"pl:off:{sid}", who=OTHER)
    await press(bd, f"pl:offok:{sid}", who=OTHER)
    check(any(s["id"] == sid for s in pl.sessions(COACH.id)),
          "чужое занятие не отменилось")


async def test_back_steps(bd) -> None:
    """«Назад» ведёт на шаг назад, а не в корень — жалоба от 11.08.2026."""
    print("\n=== «Назад» на шаг назад ===")
    import private_lessons as pl
    pid = pl.people(COACH.id)[0]["id"]
    sid = pl.sessions(COACH.id)[0]["id"]
    for start, expect in ((f"pl:hist:{pid}", f"pl:p:{pid}"),
                          (f"pl:p:{pid}", "pl:who"),
                          ("pl:debts", "pl:cash"),
                          ("pl:last", "pl:cash"),
                          ("pl:cash", "pl:main"),
                          (f"pl:pick:{sid}", f"pl:s:{sid}"),
                          (f"pl:s:{sid}", "pl:days"),
                          ("pl:days", "pl:main"),
                          ("pl:arc", "pl:who")):
        _, markup, _ = await press(bd, start)
        got = [c for c in cbs(markup) if c in (expect,)]
        check(bool(got), f"{start} → {expect}")


async def test_repeat(bd) -> None:
    """Занятие повторяется само, и повторение не мешает жить.

    Проверяем не только «даты завелись», а три места, где такие функции обычно
    ломаются: повторный проход не плодит дубли, отменённая дата не воскресает,
    а правка одной даты не расползается на остальные."""
    print("\n=== повторение раз в неделю ===")
    import private_lessons as pl
    from datetime import date, datetime, timedelta

    folk = pl.people(COACH.id)
    ivan = next(p for p in folk if p["label"].startswith("Иванов И. И"))

    # Заводим занятие на ближайшую среду и записываем на него человека.
    when = date.today() + timedelta(days=(2 - date.today().weekday()) % 7 or 7)
    await press(bd, "pl:new")
    await say(bd, f"{when:%d.%m} 19:00 Зал")
    sid = newest(pl, COACH.id)
    await press(bd, f"pl:t:{sid}:{ivan['id']}")

    text, markup, _ = await press(bd, f"pl:rep:{sid}")
    check("каждую среду" in text, f"предложение названо по-русски: {text[:60]}")
    on = find(markup, "Каждую неделю")
    check(bool(on), "есть кнопка «каждую неделю»")

    text, markup, _ = await press(bd, on)
    mine = sorted([s for s in pl.sessions(COACH.id, limit=200)
                   if int(s.get("series_id") or 0)], key=lambda s: s["day"])
    days = [datetime.strptime(s["day"], "%Y-%m-%d").date() for s in mine]
    # Точное число дат зависит от того, какой сегодня день недели: горизонт
    # считается от сегодня, а не от занятия-образца. Поэтому проверяем свойства
    # расписания, а не заранее угаданное количество.
    horizon = date.today() + timedelta(weeks=pl.AHEAD_WEEKS)
    check(len(days) >= 3, f"дат завелось: {len(days)}")
    check(days[0] == when, f"первая дата — само занятие: {days[0]} vs {when}")
    check(max(days) <= horizon, f"дальше горизонта не лезем: {max(days)}")
    check(all((b - a).days == 7 for a, b in zip(days, days[1:])),
          f"ровно через неделю: {[(b - a).days for a, b in zip(days, days[1:])]}")
    check(all(d.weekday() == 2 for d in days), "все даты — среды")
    check(all(int(s["going"]) == 1 for s in mine),
          f"люди подставились на каждую дату: {[s['going'] for s in mine]}")

    # Повторный заход в раздел не должен плодить дубли.
    await press(bd, "pl:days")
    await press(bd, "pl:main")
    again = [s for s in pl.sessions(COACH.id, limit=200) if int(s.get("series_id") or 0)]
    check(len(again) == len(mine), f"дубли не наплодились: {len(mine)} → {len(again)}")

    # Одну дату отменяем — расписание живёт, а дата не воскресает.
    victim = sorted(mine, key=lambda s: s["day"])[2]
    await press(bd, f"pl:offok:{victim['id']}")
    await press(bd, "pl:days")
    left = [s for s in pl.sessions(COACH.id, limit=200) if int(s.get("series_id") or 0)]
    check(all(s["id"] != victim["id"] for s in left), "отменённая дата ушла")
    check(all(s["day"] != victim["day"] for s in left),
          "и не воскресла при следующем открытии раздела")
    check(len(left) == len(mine) - 1, f"остальные на месте: {len(left)}")

    # Правка одной даты не расползается на соседние.
    other = sorted(left, key=lambda s: s["day"])[1]
    await press(bd, f"pl:sprice:{other['id']}")
    await say(bd, "3000")
    prices = {s["id"]: pl.session(COACH.id, s["id"])["price"] for s in left}
    check(prices.pop(other["id"]) == 3000, "цена изменилась там, где меняли")
    check(all(p == 0 for p in prices.values()),
          f"и не поехала у соседних дат: {sorted(set(prices.values()))}")

    # Останов: будущие пустые даты убираются, прошедшие — нет.
    row = pl.series(COACH.id, sorted(left, key=lambda s: s["day"])[0]["series_id"])
    text, markup, _ = await press(bd, f"pl:repoff:{row['id']}:{sid}")
    check("останов" in text.lower(), f"перед остановом спрашивают: {text[:50]}")
    await press(bd, f"pl:repoff2:{row['id']}:{sid}")
    rest = [s for s in pl.sessions(COACH.id, limit=200)
            if int(s.get("series_id") or 0) == int(row["id"])]
    check(rest == [] or all(s["day"] < date.today().isoformat() for s in rest),
          f"впереди пусто: {[s['day'] for s in rest]}")
    await press(bd, "pl:main")
    back = [s for s in pl.sessions(COACH.id, limit=200)
            if int(s.get("series_id") or 0) == int(row["id"])
            and s["day"] >= date.today().isoformat()]
    check(not back, "и после остановки даты не заводятся заново")


async def test_rename(bd) -> None:
    """Переименование меняет подпись и ничего больше.

    Смысл в том, что деньги и занятия держатся на id: если после правки имени
    у человека обнулился долг, тренер об этом узнает в самый неподходящий
    момент — при разговоре о деньгах."""
    print("\n=== правка имени ===")
    import private_lessons as pl
    who = next(p for p in pl.people(COACH.id) if p["label"].startswith("Петров"))
    before = pl.balance(COACH.id, who["id"])
    visits = pl.person_stats(COACH.id, who["id"])["visits"]

    text, markup, _ = await press(bd, f"pl:rename:{who['id']}")
    check("Петров П." in text, f"показано, что меняем: {text[:45]}")
    text, _ = await say(bd, "Петров Пётр Сергеевич")
    check("Петров П. С." in text, f"новое имя укорочено так же: {text[:40]}")
    check(pl.balance(COACH.id, who["id"]) == before, "деньги остались те же")
    check(pl.person_stats(COACH.id, who["id"])["visits"] == visits,
          "и занятия тоже")

    # Занятое имя — внятный отказ, а не молчание и не второй такой же.
    other = next(p for p in pl.people(COACH.id) if p["label"].startswith("Иванов И. И"))
    await press(bd, f"pl:rename:{other['id']}")
    text, _ = await say(bd, "Петров Пётр Сергеевич")
    check("занято" in text, f"тёзку не пускаем: {text[:60]}")
    text, _ = await say(bd, "Иванов Иван")
    check("Иванов И." in text, "после отказа имя всё же меняется")


async def test_delete(bd) -> None:
    """Удаление — настоящее, и о цене предупреждает заранее."""
    print("\n=== удаление человека ===")
    import private_lessons as pl

    # Заведён по ошибке: за ним ничего нет — сносим без нотаций.
    await press(bd, "pl:add")
    await say(bd, "Ошибочный")
    junk = next(p for p in pl.people(COACH.id) if p["label"] == "Ошибочный")
    text, markup, _ = await press(bd, f"pl:del:{junk['id']}")
    check("без следа" in text, f"пустого сносим без предупреждений: {text[:60]}")
    check(not any("архив" in (b.text or "").lower() for b in buttons_of(markup)),
          "и архив ему не навязываем")
    await press(bd, f"pl:del2:{junk['id']}")
    check(all(p["label"] != "Ошибочный" for p in pl.people(COACH.id)),
          "удалён")

    # А у кого история — сначала показываем, что пропадёт.
    who = next(p for p in pl.people(COACH.id) if p["label"].startswith("Петров"))

    # Ставим его в расписание: удаление обязано вычистить и оттуда, иначе
    # повторяющиеся занятия продолжат записывать призрака по средам.
    await press(bd, "pl:new")
    await say(bd, "завтра 18:00")
    sid = newest(pl, COACH.id)
    await press(bd, f"pl:t:{sid}:{who['id']}")
    await press(bd, f"pl:repon:{sid}:1")
    plans = pl.series_list(COACH.id)
    check(bool(plans) and any(int(who["id"]) in pl.series(COACH.id, s["id"])["people"]
                              for s in plans),
          "человек попал в расписание")

    was = pl.person_stats(COACH.id, who["id"])
    check(was["records"] > 0, f"у него есть история: {was}")
    text, markup, _ = await press(bd, f"pl:del:{who['id']}")
    check("пропадут" in text, "сказано, что пропадёт")
    check(str(was["visits"]) in text, f"записи на занятия названы числом: {was['visits']}")
    check("итоги месяца" in text, "предупредили про итоги месяца")
    check(any("архив" in (b.text or "").lower() for b in buttons_of(markup)),
          "рядом предложен архив")

    # Удаляем — и проверяем, что ушло всё, включая расписание.
    money_before = pl.month(COACH.id)["paid"]
    await press(bd, f"pl:del2:{who['id']}")
    check(all(int(p["id"]) != int(who["id"]) for p in pl.people(COACH.id)),
          "человека нет в списке")
    check(pl.history(COACH.id, who["id"]) == [], "денег за ним не осталось")
    check(pl.month(COACH.id)["paid"] != money_before or was["paid"] == 0,
          "итог месяца пересчитался")
    left = pl.series_list(COACH.id)
    check(bool(left), "расписание на месте")
    check(all(int(who["id"]) not in pl.series(COACH.id, s["id"])["people"]
              for s in left), "вычищен из расписания")
    # И следующая раскладка не заводит его заново.
    await press(bd, "pl:main")
    ghosts = [m["id"] for x in pl.sessions(COACH.id, limit=200)
              for m in pl.session(COACH.id, x["id"])["members"]]
    check(int(who["id"]) not in ghosts, "и не всплыл в составах занятий")


async def test_spot_price(bd) -> None:
    """Разовая цена на одно занятие: сегодня с человека меньше.

    Просьба тренера 14.08.2026. Менять ради этого его постоянную цену нельзя —
    она вернётся не сразу и не вспомнится, а в следующий раз он заплатит не
    столько."""
    print("\n=== разовая цена на одно занятие ===")
    import private_lessons as pl
    folk = pl.people(COACH.id)
    who = folk[0]
    await press(bd, "pl:new")
    await say(bd, "завтра 12:00")
    sid = newest(pl, COACH.id)
    await press(bd, f"pl:t:{sid}:{who['id']}")

    было = pl.session(COACH.id, sid)["members"][0]["price"]
    text, markup, _ = await press(bd, f"pl:pp:{sid}")
    check("Цена на" in text, f"экран цен по людям открылся: {text[:30]}")
    btn = find(markup, who["label"][:10])
    check(bool(btn), "человек — кнопка")

    await press(bd, btn)
    text, _ = await say(bd, "700")
    m = pl.session(COACH.id, sid)["members"][0]
    check(m["price"] == 700, f"разовая цена применилась: {m['price']}")
    check("разовая" in text, "и помечена как разовая")

    # Постоянная цена человека не изменилась.
    check(int(pl.person(COACH.id, who["id"])["price"] or 0) == int(who["price"] or 0),
          "постоянная цена не тронута")

    # На другом занятии — снова обычная.
    await press(bd, "pl:new")
    await say(bd, "послезавтра 12:00")
    other = newest(pl, COACH.id)
    await press(bd, f"pl:t:{other}:{who['id']}")
    check(pl.session(COACH.id, other)["members"][0]["price"] == было,
          "на соседнюю дату не поехала")

    # Ноль возвращает обычную.
    await press(bd, f"pl:sp:{sid}:{who['id']}")
    await say(bd, "0")
    check(pl.session(COACH.id, sid)["members"][0]["price"] == было,
          "ноль вернул обычную цену")


async def test_button_width(bd) -> None:
    """Подписи не должны обрезаться на телефоне.

    Обходчик кнопок проверяет это по всему боту, но до заполненных экранов
    раздела он не доберётся: у тестового пользователя частных занятий нет.
    Здесь список уже с людьми и ценами — как раз тот случай, где подпись
    длиннее всего («✅ Иванов И. И. · 1500 ₽»)."""
    print("\n=== подписи влезают в кнопку ===")
    from test_buttons import wide_buttons
    import private_lessons as pl
    pid = pl.people(COACH.id)[0]["id"]
    sid = newest(pl, COACH.id)
    for data in ("pl:main", "pl:days", "pl:who", "pl:cash", "pl:debts",
                 f"pl:s:{sid}", f"pl:pick:{sid}", f"pl:p:{pid}", f"pl:hist:{pid}"):
        _, markup, _ = await press(bd, data)
        wide = wide_buttons(markup)
        check(not wide, f"{data}: " + ("влезают" if not wide else "; ".join(wide)))


async def test_nothing_leaves(bd) -> None:
    """Из раздела не уходит ни одного сообщения.

    Это и есть «в отрыве от команды» на практике: частные занятия не должны
    попасть ни в общий чат, ни в личку игрокам. Людей этих бот в Телеграме не
    знает вовсе, и знать не должен."""
    print("\n=== наружу ничего не уходит ===")
    check(BOT.sent == [], f"за весь прогон бот не отправил ничего: {len(BOT.sent)}")


def test_no_team_imports() -> None:
    """Раздел не тянется к командным таблицам — проверяем по импортам."""
    print("\n=== раздел не зависит от команды ===")
    tree = ast.parse((ROOT / "private_lessons.py").read_text(encoding="utf-8"))
    names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
    local = {n for n in names if (ROOT / f"{n}.py").exists()}
    check(local == {"sheets_cache"},
          f"из своих модулей нужен только доступ к базе: {sorted(local) or '—'}")


async def main() -> int:
    bd = setup()
    print(f"База: {TMP}")
    await test_full_path(bd)
    await test_price_frozen(bd)
    await test_advance(bd)
    await test_free_input(bd)
    await test_owner_isolation(bd)
    await test_back_steps(bd)
    await test_repeat(bd)
    await test_rename(bd)
    await test_spot_price(bd)
    await test_button_width(bd)
    await test_delete(bd)
    await test_nothing_leaves(bd)
    test_no_team_imports()

    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ЧАСТНЫЕ ЗАНЯТИЯ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
