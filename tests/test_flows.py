"""Сценарии целиком: не «экран рисуется», а «дело доводится до конца».

Обходчик кнопок ловит поломки навигации. Здесь — то, что он проверить не может:
ввод текста, переходы между шагами, попадание в нужного человека. Каждый
сценарий начинается с настоящей жалобы, чтобы тест жил не сам по себе.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from typing import Any, List, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fake_tg import (FakeBot, FakeContext, FakeMessage, FakeQuery, FakeUpdate,
                     FakeUser, buttons_of)

USER = FakeUser(uid=111222333, username="tester")
BOT = FakeBot()


def setup() -> Any:
    db = os.getenv("BOT_TEST_DB") or str(ROOT / "data" / "bot.db")
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ["ADMIN_USER_IDS"] = str(USER.id)
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
    os.environ["SPREADSHEET_ID"] = ""
    import sheets_cache
    sheets_cache.DB_PATH = Path(db)
    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None
    return bd


async def press(bd, handler, data: str):
    q = FakeQuery(data, USER, BOT)
    await handler(FakeUpdate(query=q, user=USER), FakeContext(BOT))
    last = (q.screens or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"], q


async def say(bd, handler, text: str):
    msg = FakeMessage(text=text, bot=BOT, user=USER)
    try:
        await handler(FakeUpdate(message=msg, user=USER), FakeContext(BOT))
    except Exception as exc:               # ApplicationHandlerStop — норма
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    last = (msg.replies or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"]


def cbs(markup) -> List[str]:
    return [b.callback_data for b in buttons_of(markup) if b.callback_data]


async def test_add_debt(bd) -> List[str]:
    """«Добавить долг»: пишу фамилию — бот молчит; листаю — выкидывает на старт.

    Жалоба от 11.08.2026. Обе причины были в одном месте: стрелки листания вели
    в поток ОПЛАТЫ (жёстко зашитый coach:page), а текст на этом экране не
    ловился вовсе."""
    bad: List[str] = []
    import coach_payments
    people = coach_payments.players()
    if not people:
        return ["в базе нет игроков — сценарий не проверить"]

    text, markup, _ = await press(bd, bd.handle_coach_callback, "coach:adddebt")
    if "фамилию" not in text:
        bad.append("на экране долга не сказано, что можно писать фамилию")

    # Листание обязано остаться в потоке долга, а не увести в оплату.
    flips = [c for c in cbs(markup) if "page" in c]
    if not flips:
        bad.append("нет кнопок листания — список игроков не разбит на страницы")
    elif any(c.startswith("coach:page") for c in flips):
        bad.append(f"листание уводит в поток оплаты: {flips}")

    if flips:
        text2, markup2, _ = await press(bd, bd.handle_coach_callback, flips[-1])
        if text2.startswith("🧑‍🏫") or "Раздел тренера" in text2[:30]:
            bad.append("листание выбросило в корень раздела")
        if not cbs(markup2):
            bad.append("вторая страница пустая")

    # Фамилия текстом: один человек — спрашиваем, за что долг.
    surname = people[0]["surname"]
    await press(bd, bd.handle_coach_callback, "coach:adddebt")
    reply, markup = await say(bd, bd.handle_money_text, surname)
    if "за что" not in reply.lower():
        bad.append(f"по фамилии «{surname}» не спросили, за что: {reply[:70]!r}")
    if people[0]["title"] not in reply and len(
            [p for p in people if p["surname"] == surname]) == 1:
        bad.append(f"выбран не тот человек: {reply[:70]!r}")
    kinds = [c for c in cbs(markup) if c.startswith("coach:debtwhy")]
    if len(kinds) < 3:
        bad.append(f"нет выбора «за что»: {kinds}")
    else:
        text, _, _ = await press(bd, bd.handle_coach_callback, "coach:debtwhy:train")
        if "сумм" not in text.lower() or "тренировка" not in text.lower():
            bad.append(f"после «Тренировка» не спросили сумму: {text[:70]!r}")

    # Несуществующая фамилия — внятный отказ, а не молчание.
    await press(bd, bd.handle_coach_callback, "coach:adddebt")
    reply, markup = await say(bd, bd.handle_money_text, "Йцукенг")
    if "нет" not in reply.lower() and "не наш" not in reply.lower():
        bad.append(f"на пустой поиск нет внятного ответа: {reply[:70]!r}")
    if not any("debtfree" in c for c in cbs(markup)):
        bad.append("на пустой поиск не предложено завести произвольное имя")
    return bad


async def test_free_debt(bd) -> List[str]:
    """Долг тому, кого нет в листе: гость на игру, родитель.

    Просьба от 11.08.2026: как с названием команды в мастере игры, должно быть
    можно завести произвольное имя, если ничего похожего не нашлось."""
    import coach_payments
    bad: List[str] = []
    name = "Гостевой Игрок Тестовый"

    await press(bd, bd.handle_coach_callback, "coach:adddebt")
    reply, markup = await say(bd, bd.handle_money_text, name)
    free = [c for c in cbs(markup) if "debtfree" in c]
    if not free:
        bad.append(f"нет кнопки «записать на имя»: {cbs(markup)}")
        return bad

    text, markup, _ = await press(bd, bd.handle_coach_callback, free[0])
    if "за что" not in text.lower() or name not in text:
        bad.append(f"после кнопки не спросили, за что, для «{name}»: {text[:70]!r}")

    # Своё пояснение: бот спрашивает словами, потом сумму.
    text, _, _ = await press(bd, bd.handle_coach_callback, "coach:debtwhy:own")
    if "за что" not in text.lower():
        bad.append(f"«Своё» не спросило пояснение: {text[:70]!r}")
    reply, _ = await say(bd, bd.handle_money_text, "мяч на турнир")
    if "сумм" not in reply.lower() or "мяч на турнир" not in reply:
        bad.append(f"после пояснения не спросили сумму: {reply[:70]!r}")

    before = len(coach_payments.extra_debts())
    reply, _ = await say(bd, bd.handle_money_text, "700")
    after = coach_payments.extra_debts()
    if len(after) != before + 1:
        bad.append(f"долг не записался: было {before}, стало {len(after)}")
        return bad

    debt = after[0]
    if int(debt["player_row"]) != 0:
        bad.append(f"свободный долг привязался к строке листа: {debt['player_row']}")
    if coach_payments.debt_title(debt) != name:
        bad.append(f"имя потерялось: {coach_payments.debt_title(debt)!r}")
    if debt["amount"] != 700:
        bad.append(f"сумма не та: {debt['amount']}")
    if "мяч на турнир" not in (debt["note"] or ""):
        bad.append(f"пояснение не сохранилось: {debt['note']!r}")
    if "не из состава" not in reply:
        bad.append("не предупредили, что напоминание такому человеку не уйдёт")

    # Долг виден на экране и гасится, как обычный.
    screen, markup, _ = await press(bd, bd.handle_coach_callback, "coach:debts")
    if name not in screen:
        bad.append("свободный долг не показан на экране долгов")
    close = [c for c in cbs(markup) if c.startswith("coach:closedebt")]
    if not close:
        bad.append("свободный долг нечем погасить")
    else:
        await press(bd, bd.handle_coach_callback, f"coach:closedebt:{debt['id']}")
        if any(d["id"] == debt["id"] for d in coach_payments.extra_debts()):
            bad.append("долг не погасился")

    # Прибираем за собой, что бы ни случилось выше.
    import sheets_cache
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM extra_debts WHERE who = ?", (name,))
        conn.commit()
    return bad


async def test_pay_calendars(bd) -> List[str]:
    """Календари оплат — те, что задал пользователь 11.08.2026.

    Тренировки: 20-е тренеру, 25-е игрокам с кнопками, предпоследний день
    тренеру, последний игрокам, дальше тренеру каждые 4 дня (4, 8, 12…),
    игроку каждые 5 (5, 10, 15…).
    Игра: −2 тренеру, −1 игроку, +2 тренеру, +3 игроку, +5 тренеру, +8 игроку.
    В один день одной стороне — не больше одного сообщения."""
    from datetime import date, datetime, timedelta, timezone
    import training_dues as td
    import game_roster
    bad: List[str] = []

    # ── тренировки ──────────────────────────────────────────────────────
    got = {}
    for d in range(1, 32):
        try:
            today = date(2026, 10, d)
        except ValueError:
            break
        got[d] = [k for _, _, k in td.due_events(today)]

    want = {20: "coach_plan", 25: "player_ask", 30: "coach_end", 31: "player_last"}
    for d, kind in want.items():
        if kind not in got.get(d, []):
            bad.append(f"тренировки, {d}-е: нет {kind} (есть {got.get(d)})")
    for d in (4, 8, 12, 16, 24, 28):
        if "coach_debt" not in got.get(d, []) and d not in want:
            bad.append(f"тренировки, {d}-е: нет напоминания тренеру")
    for d in (5, 10, 15, 20):
        if "player_debt" not in got.get(d, []):
            bad.append(f"тренировки, {d}-е: нет напоминания игроку")
    for d, kinds in got.items():
        sides = [("coach" if k.startswith("coach") else "player") for k in kinds]
        if len(sides) != len(set(sides)):
            bad.append(f"тренировки, {d}-е: две весточки одной стороне: {kinds}")

    # ── игра ────────────────────────────────────────────────────────────
    MSK = timezone(timedelta(hours=3))
    game = next((g for g in game_roster.games()
                 if g["date"].isoformat() >= game_roster.PAY_SINCE
                 and game_roster.is_posted(g["source"], g["game_id"])), None)
    if not game:
        return bad
    seen = {}
    for d in range(-4, 12):
        when = datetime.combine(game["date"] + timedelta(days=d),
                                datetime.min.time(), tzinfo=MSK).replace(hour=10)
        # Ключ: game:<источник>:<id игры>:<вид>[:<шаг>] — нас интересует хвост.
        seen[d] = [":".join(k.split(":")[3:])
                   for k, g, _ in game_roster.due_events(now=when)
                   if g["game_id"] == game["game_id"]]
    for d, kind in ((-2, "coach_pay:-2"), (2, "coach_pay:2"), (3, "player_pay:3"),
                    (5, "coach_pay:5"), (8, "player_pay:8")):
        if kind not in seen.get(d, []):
            bad.append(f"игра, день {d:+d}: нет {kind} (есть {seen.get(d)})")
    if any("pay" in k for k in seen.get(0, [])):
        bad.append("в день игры бот пишет про деньги — людям не до того")
    return bad


async def test_roster_ready_check(bd) -> List[str]:
    """Отправка состава сверяется с опросом ещё раз.

    Жалоба тренера 11.08.2026: бот предложил не всех, кто голосовал. Причина —
    экран состава это снимок: Морозов нажал «Готов» в ту же минуту, когда
    уходил состав, и в список не попал. Теперь перед отправкой бот проверяет,
    не появился ли кто-то ещё."""
    import game_roster
    bad: List[str] = []
    game = next((g for g in game_roster.games()
                 if game_roster.voters(str(g["game_id"]))), None)
    if not game:
        return []
    source, gid = game["source"], game["game_id"]
    missing = bd._ready_but_out(source, gid)
    picked = {p["row"] for p in game_roster.roster(source, gid)}
    ready = [v for v in game_roster.voters(str(gid)) if v["linked"]]

    # Проверка обязана находить ровно тех, кто голосовал, но не в составе.
    want = {v["row"] for v in ready} - picked
    if {m["row"] for m in missing} != want:
        bad.append(f"сверка с опросом врёт: нашла {[m['title'] for m in missing]}, "
                   f"а не в составе {len(want)} чел.")

    text, markup, _ = await press(bd, bd.handle_roster_callback,
                                  f"rost:post:{source}:{gid}")
    if missing:
        if "Готов" not in text or "не" not in text.lower():
            bad.append(f"о непопавших не предупредили: {text[:70]!r}")
        cb = cbs(markup)
        if not any("postall" in c for c in cb):
            bad.append(f"нет кнопки «добавить и отправить»: {cb}")
        if not any("post2" in c for c in cb):
            bad.append(f"нет кнопки «отправить как есть»: {cb}")
    return bad


async def test_roster_form(bd) -> List[str]:
    """Форма в составе на игру: кнопки «Тёмная»/«Светлая» роняли обработчик.

    `row = int(parts[4])` разбирал пятый кусок как строку листа, а там лежит
    «dark»/«light» — падало раньше, чем доходило до ветки."""
    import game_roster
    games = game_roster.games()
    if not games:
        return []
    g = games[-1]
    bad = []
    for form in ("dark", "light"):
        data = f"rost:form:{g['source']}:{g['game_id']}:{form}"
        try:
            await press(bd, bd.handle_roster_callback, data)
        except Exception as exc:
            bad.append(f"{data} -> {type(exc).__name__}: {exc}")
    return bad


async def test_start_five(bd) -> List[str]:
    """Стартовая пятёрка: выбор, а не только правка амплуа.

    Жалоба 11.08.2026: «выбираю игрока — бот снова спрашивает амплуа, а в
    состав не добавляет; как понять, кто попал?». Экран умел ровно одно —
    менять позицию, самой пятёрки не было."""
    import coach_lineup as cl
    import game_roster
    bad: List[str] = []
    game = next((g for g in game_roster.games()
                 if game_roster.roster(g["source"], g["game_id"])), None)
    if not game:
        return []
    source, gid = game["source"], str(game["game_id"])
    people = game_roster.roster(source, gid)

    # Чисто: начинаем с пустой пятёрки.
    for r in cl.start_five(source, gid):
        cl.toggle_start(source, gid, r)

    text, markup, _ = await press(bd, bd.handle_coach_callback,
                                  f"coach:start:{source}:{gid}:name")
    if "СТАРТ · 0 из" not in text or "Пусто" not in text:
        bad.append(f"пустая пятёрка не объяснена: {text[:70]!r}")
    picks = [c for c in cbs(markup) if c.startswith("coach:sf:")]
    if len(picks) < min(5, len(people)):
        bad.append(f"нажать на фамилию нельзя: {cbs(markup)[:4]}")
        return bad

    # Нажатие ставит в старт, повторное — снимает.
    text, _, _ = await press(bd, bd.handle_coach_callback, picks[0])
    if "СТАРТ · 1 из" not in text:
        bad.append(f"после выбора не видно, кто в старте: {text[:80]!r}")
    text, _, _ = await press(bd, bd.handle_coach_callback, picks[0])
    if "СТАРТ · 0 из" not in text:
        bad.append("повторное нажатие не снимает из старта")

    # Больше пяти не берём.
    for c in picks[:6]:
        await press(bd, bd.handle_coach_callback, c)
    if len(cl.start_five(source, gid)) > cl.START_SIZE:
        bad.append(f"в старте больше {cl.START_SIZE}: {len(cl.start_five(source, gid))}")

    # Амплуа — за отдельной кнопкой, и там позиции с номерами.
    text, markup, _ = await press(bd, bd.handle_coach_callback,
                                  f"coach:roles:{source}:{gid}:name")
    if "Амплуа" not in text:
        bad.append(f"режим амплуа не открылся: {text[:60]!r}")
    role_btn = [c for c in cbs(markup) if c.startswith("coach:role:")]
    if not role_btn:
        bad.append("в режиме амплуа нечего нажать")
    else:
        text, markup, _ = await press(bd, bd.handle_coach_callback, role_btn[0])
        labels = [b.text for b in buttons_of(markup)]
        if not any("№1" in l and "азыгрыв" in l for l in labels):
            bad.append(f"позиции без номеров или без названий: {labels[:3]}")

    # Карточка игрока: три строки, два раздела, средние из статистики лиг.
    for c in picks[:2]:
        await press(bd, bd.handle_coach_callback, c)
    card = cl.text(cl.lineup(source, gid, "name"))
    for must in ("СТАРТ ·", "СКАМЕЙКА ·", "трен."):
        if must not in card:
            bad.append(f"в карточке нет «{must}»")
    # Средние — только по турнирам из «Конфига», не по всей истории.
    scopes = cl.current_scopes()
    if not scopes:
        bad.append("турниры из «Конфига» не читаются — средних не будет ни у кого")
    rows_now = cl.lineup(source, gid, "name")["rows"]
    all_time = cl.averages(rows_now, scopes=None if not scopes else [
        {"source": s_["source"], "season_id": s_["season_id"], "stage_id": s_["stage_id"]}
        for s_ in scopes])
    for r in rows_now:
        got = (r.get("avg") or {}).get("games", 0)
        if got and got != (all_time.get(r["row"], {}) or {}).get("games", 0):
            bad.append(f"экран считает не по «Конфигу»: {r['title']}")
    # У кого цифр нет — карточка обязана объяснить почему, а не молчать.
    for r in rows_now:
        avg = r.get("avg") or {}
        if not avg.get("games") and r["title"] in card:
            chunk = card.split(r["title"], 1)[1][:160]
            if "заявке" not in chunk and "не играл" not in chunk:
                bad.append(f"{r['title']}: нет цифр и не сказано почему")
    with_stats = [r for r in rows_now if (r.get("avg") or {}).get("games")]
    if with_stats and "очк" not in card:
        bad.append("средние за игру не показаны, хотя статистика есть")
    # В листе у части игроков амплуа записано цифрой — след старой поломки,
    # когда бот переписал названия позиций номерами. Показывать «· 2» нельзя,
    # надо «№2 · атакующий защитник».
    #
    # Ищем в СТРОКЕ ИГРОКА, а не по всей карточке. Цифра сама по себе там
    # встречается на каждом шагу — «· 7 трен.», «· 2.3 подб», — и проверка
    # вхождением дважды срабатывала вхолостую на исправном экране. Спрашиваем
    # прямо то, чего добиваемся: рядом с человеком позиция названа словами.
    for r in cl.lineup(source, gid, "name")["rows"]:
        if not str(r.get("role") or "").isdigit() or r["title"] not in card:
            continue
        line = card.split(r["title"], 1)[1][:80]
        if cl.role_title(r["role"]) not in line:
            bad.append(f"амплуа «{r['role']}» не расшифровано: {line[:45]!r}")

    for r in cl.start_five(source, gid):
        cl.toggle_start(source, gid, r)
    return bad


async def test_search_everywhere(bd) -> List[str]:
    """Поиск один на весь бот: по фамилии, по имени, по названию команды."""
    import coach_payments
    import coach_newgame
    import player_search
    bad = []
    people = coach_payments.players()
    if not people:
        return []
    p = people[0]
    for query, why in ((p["surname"], "фамилия"),
                       (p["name"], "имя"),
                       (f"{p['name']} {p['surname']}", "имя и фамилия")):
        if not query.strip():
            continue
        hits = player_search.rank(query, people, player_search.person_fields)
        if not any(h["row"] == p["row"] for h in hits):
            bad.append(f"поиск по {why} «{query}» не нашёл {p['title']}")
    # Команды ищутся тем же правилом.
    teams = coach_newgame.find_teams("slpro", "бал")
    if teams and not any("бал" in t.lower() for t in teams):
        bad.append(f"поиск команд отдаёт не то: {teams[:3]}")
    return bad


async def main() -> int:
    bd = setup()
    tests = [("«Добавить долг»", test_add_debt),
             ("Долг не из состава", test_free_debt),
             ("Календари оплат", test_pay_calendars),
             ("Сверка состава с опросом", test_roster_ready_check),
             ("Форма в составе", test_roster_form),
             ("Стартовая пятёрка", test_start_five),
             ("Общий поиск", test_search_everywhere)]
    failed = 0
    for name, fn in tests:
        try:
            bad = await fn(bd)
        except Exception as exc:
            bad = [f"сам тест упал: {type(exc).__name__}: {exc}"]
        print(("✅ " if not bad else "❌ ") + name)
        for b in bad:
            print("   •", b)
        failed += len(bad)
    print("\n" + ("ВСЁ ЗЕЛЁНОЕ" if not failed else f"ЗАМЕЧАНИЙ: {failed}"))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
