"""Обход всех кнопок бота: нажать каждую и убедиться, что под ней что-то есть.

Зачем именно так. Обычные тесты проверяют, что функция считает верно. А ломалось
другое: экран рисуется, кнопка на месте, а нажатие ничего не делает или уводит в
корень раздела. Разрыв между «кнопка есть» и «функция работает» руками не
ловится — экранов под сотню, и каждый релиз проверять их пальцем невозможно.

Поэтому обходчик идёт по дереву сам: нажал кнопку, получил экран, нажал его
кнопки, и так вглубь. Проверяет три вещи:

  1. обработчик не упал;
  2. экран сменился (а не «ответ в пустоту»);
  3. нажатие не выбросило в корень раздела, если из корня не приходили —
     ровно эта примета и была у сломанного пролистывания в «Добавить долг».

Наружу ничего не уходит: бот подменён, база — копия, Google не задействован.
Разрушающие кнопки (удалить, забрать доступ, разослать, запустить скрипт)
не нажимаются никогда — их список ниже и он намеренно широкий.
"""

from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fake_tg import (FakeBot, FakeContext, FakeQuery, FakeUpdate, FakeUser,
                     buttons_of)

# Кнопки, которые ничего не ломают, но действуют наружу или необратимы.
# Список намеренно широкий: пропустить проверку безопаснее, чем разослать
# команде тестовое сообщение или снести чужой платёж.
FORBIDDEN = re.compile(
    r"(:del|:un2?:|:off:|:revoke|remind|send|sync|backup|launch|:do:|:set:"
    r"|accset|accoff|delpay|startsend|:new:|:split:|:keep:|finish|start:"
    r"|admin:report|fantasy:ingest|fantasy:prices|fscope|admin:stats"
    # «Объявить результат» пишет в общий чат всей команде.
    r"|coach:repub2"
    # Выгрузка шлёт файл со списком команды — в обходе это лишний документ
    # в личку и лишний поход в лист «Игроки».
    r"|coach:csv|pg:csv"
    # Частные занятия: это чужие деньги. Обход ходит по настоящей базе, и
    # начислить долг или снять оплату «просто чтобы проверить кнопку» нельзя.
    r"|pl:(t|done|paid|offok|arch|back|rm|repon|repoff2):"
    # Группы: обход ходит по настоящей базе. «Отправить» разошлёт письмо
    # живой команде, а del2/t меняют настоящие составы и шаблоны.
    r"|pg:(go|del2|tdel|rdel|rsw|t|lgset|use2?):)")

# Корневые экраны разделов: попасть сюда можно только осознанно, кнопкой
# «в раздел». Если нажатие увело сюда откуда-то из глубины — это и есть
# «выкинуло на старт».
ROOTS = {"coach:main", "admin:menu:main", "menu:main", "pl:main",
         "pg:main"}

# Подписи, обещающие возврат. «Назад» и «В раздел» человек читает одинаково —
# «на шаг вверх», и если они кидают в корень с третьего этажа, он теряет место,
# где был. «К оплате», «К списку», «К играм» называют цель прямо и проверяются
# так же: сказал куда — туда и веди.
BACK_LABELS = ("назад", "в раздел", "к оплате", "к списку", "к играм",
               "к составу", "к пятёрке", "к амплуа", "к доступам", "к отчёту")

# Сколько ждём один экран. В проекте правило: в ответе человеку живых запросов
# наружу быть не должно, всё берётся из локальной базы. Значит любой экран
# обязан уложиться в секунды — если не уложился, это само по себе находка.
PRESS_TIMEOUT = float(os.getenv("BOT_TEST_TIMEOUT", "10"))

NET_MARK = "СЕТЬ-В-ТЕСТЕ-ЗАПРЕЩЕНА"

# Экраны, которые ходят в лигу прямо по нажатию. Это против правила «в ответе
# человеку живых запросов нет», но они работают и переделка их — отдельная
# задача. Держим на виду предупреждением, чтобы список не рос молча.
KNOWN_SLOW = {"prog:team", "admin:fantasy:scope"}

# Сколько знаков помещается в подпись кнопки на телефоне. Замерено по живому
# экрану: в двух колонках обрезалось «Напомнить: тр…» и «Кто сколько вн…».
# В одну колонку помещается заметно больше — там порог мягкий.
# Эмодзи считаем за два знака — он и занимает примерно вдвое.
WIDTH_BY_COLUMNS = {1: 40, 2: 15, 3: 9}


def visual_len(text: str) -> int:
    return sum(2 if ord(ch) > 0x2000 else 1 for ch in text or "")


def wide_buttons(markup) -> List[str]:
    """Подписи, которые на телефоне обрежутся многоточием.

    Обрезанная кнопка — это не косметика: «Напомнить: тр…» и «Напомнить: иг…»
    рядом неразличимы, и человек жмёт наугад. Ловим механически, потому что
    глазами это видно только на телефоне и только после деплоя."""
    if markup is None or not getattr(markup, "inline_keyboard", None):
        return []
    bad = []
    for row in markup.inline_keyboard:
        limit = WIDTH_BY_COLUMNS.get(len(row), 8)
        for b in row:
            if visual_len(b.text) > limit:
                bad.append(f"{b.text} ({visual_len(b.text)} > {limit}, "
                           f"{len(row)} в ряду)")
    return bad


def block_network() -> None:
    """Рубим сеть на время теста.

    Две причины. Первая: тест не должен ничего слать наружу — ни в Телеграм, ни
    в Google, ни в лиги. Вторая, важнее: так видно экраны, которые ходят в сеть
    в ответ на нажатие. По архитектуре проекта их быть не должно — человек
    ждёт ответ, а не поход в чужой API."""
    import socket

    def deny(*a, **k):
        raise OSError(NET_MARK)

    socket.socket.connect = deny
    socket.socket.connect_ex = deny
    socket.create_connection = deny
    socket.getaddrinfo = deny


class Crawler:
    def __init__(self, router, entry: str, name: str, admin_id: int = 111222333):
        # router — тот же выбор обработчика по префиксу, что и в бою: кнопка
        # одного раздела может вести в другой (из админки в «Прогресс»), и
        # обходчик обязан ходить так же, иначе покажет несуществующую поломку.
        self.router = router
        self.entry = entry
        self.name = name
        self.user = FakeUser(uid=admin_id, username="tester")
        self.bot = FakeBot()
        self.seen: Set[str] = set()
        self.problems: List[Dict[str, Any]] = []
        self.net: List[str] = []          # экраны, полезшие в сеть
        self.wide: List[Tuple[str, str]] = []   # обрезающиеся подписи
        self.wide_seen: Set[str] = set()
        self.slow: List[Tuple[str, float]] = []
        self.slow_known: List[str] = []
        self.parent_keys: Tuple[str, ...] = ()
        self.pressed = 0

    async def press(self, data: str) -> Tuple[str, Any, FakeQuery]:
        handler = self.router(data)
        if handler is None:
            return "", None, FakeQuery(data, self.user, self.bot)
        q = FakeQuery(data, self.user, self.bot)
        upd = FakeUpdate(query=q, user=self.user)
        ctx = FakeContext(self.bot)
        await handler(upd, ctx)
        self.pressed += 1
        # Экран показывают двумя способами: правкой сообщения и ответом новым
        # (так делают карточки и отчёты). Оба считаем ответом.
        shown = q.screens + [{"text": r["text"], "markup": r["markup"]}
                             for r in q.message.replies]
        if not shown:
            return "", None, q
        return shown[-1]["text"], shown[-1]["markup"], q

    @staticmethod
    def screen_key(data: str) -> str:
        """Экран без номера страницы: «admin:field:list:8» -> «admin:field:list».

        Листание — это тот же экран, а не шаг вглубь. «Назад» со второй
        страницы должно выводить из списка, а не возвращать на первую."""
        parts = data.split(":")
        while parts and parts[-1].isdigit():
            parts.pop()
        return ":".join(parts)

    def check_back(self, data: str, markup, parent: str, path: List[str]) -> None:
        """«Назад» должно вести на шаг назад, а не на главную."""
        if not parent or parent in ROOTS:
            return                      # пришли с корня — туда и вернуться верно
        if self.screen_key(parent) == self.screen_key(data):
            return                      # это листание, а не переход вглубь
        # Экран, показавший МЕНЮ СВОЕГО раздела, — сам себе этаж, и его возврат
        # в корень законен (так у фэнтези: «Старт сезона» снова рисует меню
        # фэнтези). Признак: среди кнопок есть соседи с тем же префиксом.
        family = ":".join(data.split(":")[:2])
        siblings = [b for b in buttons_of(markup)
                    if (getattr(b, "callback_data", "") or "").startswith(family + ":")
                    and not any(w in (b.text or "").lower() for w in BACK_LABELS)]
        if siblings:
            return
        # Экран не сменился — нажатие было ДЕЙСТВИЕМ, а не переходом (сменить
        # форму, отметить оплату). Тогда «шаг назад» — это откуда пришли ДО
        # действия, и спрашивать с текущей кнопки нечего.
        keys = tuple(sorted(getattr(b, "callback_data", "") or ""
                            for b in buttons_of(markup)))
        if keys and keys == self.parent_keys:
            return
        for b in buttons_of(markup):
            cb = getattr(b, "callback_data", "") or ""
            label = (b.text or "").lower()
            if not any(w in label for w in BACK_LABELS):
                continue
            if cb == parent or cb in (data,):
                continue
            if cb in ROOTS:
                self.problems.append({
                    "data": data, "path": path,
                    "beda": f"«{b.text}» ведёт на главную ({cb}), "
                            f"а шаг назад — это {parent}"})

    async def walk(self, data: str, path: List[str], depth: int = 0,
                   parent: str = "") -> None:
        if data in self.seen or depth > 6 or FORBIDDEN.search(data):
            return
        self.seen.add(data)
        import time
        t0 = time.monotonic()
        try:
            text, markup, q = await asyncio.wait_for(self.press(data), PRESS_TIMEOUT)
        except asyncio.TimeoutError:
            known = any(data.startswith(k) for k in KNOWN_SLOW)
            (self.slow_known if known else self.problems).append(
                {"data": data, "path": path,
                 "beda": f"экран не ответил за {PRESS_TIMEOUT:g} с — человек столько не ждёт"}
                if not known else data)
            return
        except Exception as exc:
            self.problems.append({"data": data, "path": path,
                                  "beda": f"обработчик упал: {type(exc).__name__}: {exc}"})
            return
        spent = time.monotonic() - t0
        if spent > 1.0:
            self.slow.append((data, spent))
        if NET_MARK in text:
            self.net.append(data)

        for bad in wide_buttons(markup):
            key = f"{data}|{bad}"
            if key not in self.wide_seen:
                self.wide_seen.add(key)
                self.wide.append((data, bad))

        alert = next((a["text"] for a in q.answers if a.get("alert")), "")
        if not text and not markup:
            # «noop» — счётчик страниц: он намеренно ничего не делает.
            if not alert and not data.endswith(":noop"):
                self.problems.append({"data": data, "path": path,
                                      "beda": "нажатие ничего не показало"})
            return

        # Увело в корень раздела, хотя шли из глубины — примета сломанной кнопки.
        if depth > 0 and data not in ROOTS:
            for root_text in ("🧑‍🏫", "📊 Админ-панель", "🎾"):
                if text.startswith(root_text) and not data.endswith(":main"):
                    self.problems.append({
                        "data": data, "path": path,
                        "beda": f"выбросило в корень раздела: «{text[:40]}»"})
                    break

        self.check_back(data, markup, parent, path)

        keys = tuple(sorted(getattr(b, "callback_data", "") or ""
                            for b in buttons_of(markup)))
        for b in buttons_of(markup):
            cb = getattr(b, "callback_data", None)
            if cb:
                self.parent_keys = keys
                await self.walk(cb, path + [b.text], depth + 1, parent=data)

    async def run(self) -> None:
        await self.walk(self.entry, [self.entry])


def _prepare_db() -> Path:
    """База — копия, а не боевая. Без копии тест запускать нельзя."""
    for env in ("BOT_TEST_DB", "TEST_DB"):
        if os.getenv(env):
            return Path(os.getenv(env))
    for guess in (ROOT / "data" / "bot.db", ROOT / "tests" / "bot.db"):
        if guess.exists():
            return guess
    raise SystemExit("Не нашёл базу для теста. Укажи BOT_TEST_DB=<путь к копии>")


def test_same_message_is_not_an_error(bd) -> bool:
    """Повторное нажатие той же кнопки — не ошибка.

    Телеграм отвечает «Message is not modified», когда правка ничего не
    меняет: человек жмёт «Назад» с экрана, который и так открыт. Раньше это
    падало в журнал ошибок и пряталось среди настоящих поломок."""
    print("\n=== «сообщение не изменилось» не считается ошибкой ===")
    from telegram.error import BadRequest
    ok = BadRequest("Message is not modified: specified new message content "
                    "and reply markup are exactly the same as a current content")
    bad = [BadRequest("Message to edit not found"),
           ValueError("can't subtract offset-naive and offset-aware datetimes")]
    good = bd._same_message(ok)
    print(("  ✅ " if good else "  ❌ ") + "повтор распознан")
    left = [e for e in bad if bd._same_message(e)]
    print(("  ✅ " if not left else "  ❌ ") + f"настоящие ошибки не глушим: {left}")

    src = (ROOT / "bot_daemon.py").read_text()
    hooked = "app.add_error_handler(_on_error)" in src
    print(("  ✅ " if hooked else "  ❌ ") + "общий перехват подключён")
    return good and not left and hooked


def test_coach_root_stays_short(bd) -> bool:
    """Корень раздела тренера не должен снова расползтись в список.

    Каждая новая возможность добавляла туда строку, и первый экран превратился
    в девять кнопок, по которым нужное ищешь глазами. Держим предел: на корне
    только области, действия живут внутри них."""
    print("\n=== корень раздела тренера ===")
    rows = bd._coach_markup().inline_keyboard
    ok = len(rows) <= 6
    print(("  ✅ " if ok else "  ❌ ") + f"кнопок на корне: {len(rows)} (предел 6)")

    # Каждая ведёт в область, а не в конкретное действие с аргументами.
    deep = [b.callback_data for row in rows for b in row
            if (b.callback_data or "").count(":") > 1]
    clean = not deep
    print(("  ✅ " if clean else "  ❌ ")
          + f"на корне нет действий с аргументами: {deep}")

    # Всё, что переехало, обязано остаться достижимым.
    reachable = set()
    for markup in (bd._coach_markup(), bd._team_markup(), bd._cfg_markup(),
                   bd._play_markup(), bd._money_markup()):
        for row in markup.inline_keyboard:
            for b in row:
                reachable.add(b.callback_data or "")
    want = {"coach:money", "coach:play", "coach:field:list:0", "pg:main",
            "coach:offline", "coach:prev", "coach:sched", "coach:prog",
            "pl:main"}
    lost = sorted(want - reachable)
    print(("  ✅ " if not lost else "  ❌ ") + f"ничего не потерялось: {lost}")
    return ok and clean and not lost


def test_republish_asks_first(bd) -> bool:
    """«Объявить результат» пишет всей команде — только после вопроса.

    Кнопка нужна, когда лига отдала протокол не целиком и в чат ушёл огрызок.
    Но само нажатие не должно ничего публиковать: сообщение увидят все, и
    промах здесь не отменишь."""
    print("\n=== объявление результата спрашивает ===")
    import inspect
    from game_results_monitor_final import GameResultsMonitorFinal

    inside = [b.callback_data for row in bd._play_markup().inline_keyboard
              for b in row]
    listed = "coach:repub" in inside
    print(("  ✅ " if listed else "  ❌ ") + "кнопка живёт в разделе «Игры»")

    text, markup = bd._republish_ask("нет-такой-игры")
    fell_back = "Объявить результат" in text
    print(("  ✅ " if fell_back else "  ❌ ")
          + "неизвестная игра возвращает к списку, а не падает")

    named = bd._game_title_of(
        {"unique_key": "АНОНС_ИГРА_22.08.2026_14:00_Кирпичный Завод_PULL UP"})
    # Подчёркивание в ключе — и разделитель, и часть слова «АНОНС_ИГРА»:
    # деление по первому уводило в подпись «ИГРА_22.08.2026…».
    titled = named == "Кирпичный Завод — PULL UP"
    print(("  ✅ " if titled else "  ❌ ") + f"игра подписана соперниками: {named}")

    src = (ROOT / "bot_daemon.py").read_text()
    body = src[src.index("def _republish_ask"):src.index("async def _republish_result")]
    warned = "уже объявляли" in body and "общий чат" in body
    print(("  ✅ " if warned else "  ❌ ") + "предупреждаем про общий чат и повтор")
    confirms = "coach:repub2" in body
    print(("  ✅ " if confirms else "  ❌ ") + "публикует только отдельное «Да»")

    sig = inspect.signature(GameResultsMonitorFinal.send_game_result)
    quiet = sig.parameters["force"].default is False
    print(("  ✅ " if quiet else "  ❌ ") + "сам монитор никогда не форсирует")
    return listed and fell_back and titled and warned and confirms and quiet


def test_team_is_named_by_the_league(bd) -> bool:
    """Свою команду называем так, как её зовёт лига, а не «Конфиг».

    В колонке D листа «Конфиг» стоит название ТУРНИРА («Как показывать турнир
    в боте и админке»), а читается оно в alt_name команды. Пока конфиг был
    главнее имени лиги, в общий чат уходило «ПОРАЖЕНИЕ: Летняя лига · Группа 4
    против Кирпичный Завод» — вместо имени своей же команды."""
    print("\n=== команду называет лига ===")
    from enhanced_game_parser import team_display_name
    from game_system_manager import GameSystemManager

    tournament = {"alt_name": "Летняя лига · Группа 4", "metadata": {}}
    named = team_display_name("PULL UP", tournament)
    right = named == "PULL UP"
    print(("  ✅ " if right else "  ❌ ") + f"имя лиги главнее конфига: {named}")

    spare = team_display_name("", tournament)
    kept = spare == "Летняя лига · Группа 4"
    print(("  ✅ " if kept else "  ❌ ")
          + f"но когда лига молчит, конфиг выручает: {spare}")

    alias = team_display_name(None, {"alt_name": "",
                                     "metadata": {"display_name": "PullUp Farm"}})
    fell = alias == "PullUp Farm"
    print(("  ✅ " if fell else "  ❌ ") + f"и display_name тоже: {alias}")

    # То же правило в менеджере игр — оно там появилось раньше, и разъезжаться
    # им нельзя: сообщения собираются из обоих источников.
    gsm = GameSystemManager()
    gsm.team_configs = {36502: dict(tournament)}
    same = gsm._resolve_team_name(36502, "PULL UP") == "PULL UP"
    print(("  ✅ " if same else "  ❌ ") + "менеджер игр держится того же правила")
    return right and kept and fell and same


async def main() -> int:
    db = _prepare_db()
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ["ADMIN_USER_IDS"] = "111222333"
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    # Google в тестах не нужен: код обязан переживать его отсутствие.
    os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
    os.environ["SPREADSHEET_ID"] = ""

    import sheets_cache
    sheets_cache.DB_PATH = db

    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None          # никаких походов в Google
    block_network()                             # и вообще никуда наружу

    print(f"База: {db}")
    # Те же префиксы, что в add_handler(CallbackQueryHandler(..., pattern=...)).
    ROUTES = [
        ("admin:", bd.handle_admin_callback),
        ("coach:", bd.handle_coach_callback),
        ("prog:", bd.handle_prog_callback),
        ("menu:", bd.handle_menu_callback),
        ("joke:", bd.handle_joke_callback),
        ("rost:", bd.handle_roster_callback),
        ("gl:", bd.handle_gamelink_callback),
        ("rep:", bd.handle_report_prefs_callback),
        ("pl:", bd.handle_private_callback),
        ("pg:", bd.handle_group_callback),
    ]

    def router(data: str):
        for prefix, fn in ROUTES:
            if data.startswith(prefix):
                return fn
        return None

    plans = [
        ("coach:main", "Раздел тренера"),
        ("admin:menu:main", "Админ-панель"),
        ("menu:main", "Меню"),
        ("prog:list", "Прогресс команды"),
        ("pl:main", "Частные занятия"),
        ("pg:main", "Группы и рассылки"),
    ]
    total_problems: List[Dict[str, Any]] = []
    for entry, name in plans:
        c = Crawler(router, entry, name)
        await c.run()
        mark = "✅" if not c.problems else "❌"
        print(f"\n{mark} {name}: нажато {c.pressed} кнопок, "
              f"замечаний {len(c.problems)}")
        for p in c.problems:
            print(f"   • {p['data']}")
            print(f"     путь: {' → '.join(p['path'][-3:])}")
            print(f"     {p['beda']}")
        if c.net:
            print(f"   ⚠️ лезут в сеть в ответ на нажатие: {', '.join(c.net[:6])}")
        if c.wide:
            print(f"   ✂️ подписи обрежутся на телефоне ({len(c.wide)}):")
            for data, bad in c.wide[:8]:
                print(f"      {bad}   [{data}]")
        if c.slow_known:
            print("   ⏳ известные медленные (ходят в лигу по нажатию): "
                  + ", ".join(sorted(set(c.slow_known))))
        if c.slow:
            worst = sorted(c.slow, key=lambda x: -x[1])[:3]
            print("   ⏱ медленные: "
                  + ", ".join(f"{d} ({s:.1f} с)" for d, s in worst))
        total_problems += c.problems

    if not test_coach_root_stays_short(bd):
        total_problems.append({"data": "корень раздела тренера"})
    if not test_team_is_named_by_the_league(bd):
        total_problems.append({"data": "имя команды"})
    if not test_republish_asks_first(bd):
        total_problems.append({"data": "объявление результата"})
    if not test_same_message_is_not_an_error(bd):
        total_problems = list(total_problems) + ["повтор нажатия считается ошибкой"]

    print("\n" + ("ВСЁ ЗЕЛЁНОЕ" if not total_problems
                  else f"ПРОБЛЕМ: {len(total_problems)}"))
    return 1 if total_problems else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
