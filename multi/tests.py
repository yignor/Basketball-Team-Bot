#!/usr/bin/env python3
"""Проверки заготовки многокомандного бота.

    python3 multi/tests.py

Работают во временной папке, боевых данных не касаются. Без pytest и новых
зависимостей — по той же причине, что и остальные тесты проекта: их установка
на сервере требует пароля.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

TMP = Path(tempfile.mkdtemp(prefix="multi-test-"))
os.environ["MULTI_DATA_DIR"] = str(TMP)

from multi import db, membership, onboarding, schema, tenants  # noqa: E402

bad: list = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def test_registry() -> None:
    print("\n=== реестр команд ===")
    a = tenants.register("Pull Up Farm", chat_id="-100111")
    b = tenants.register("Спартак", chat_id="-100222")
    check(a["slug"] == "pull-up-farm", f"имя из названия: {a['slug']}")
    check(b["slug"] == "spartak", f"кириллица в латиницу: {b['slug']}")
    check(tenants.db_path(a["slug"]).exists(), "файл базы создан сразу")

    # Две команды с одним названием — обычное дело (два «Динамо» в городе).
    c = tenants.register("Спартак", chat_id="-100333")
    check(c["slug"] == "spartak-2", f"тёзка получил своё имя: {c['slug']}")

    check(tenants.by_chat("-100222")["slug"] == "spartak", "команда ищется по чату")
    check(tenants.by_chat("-100999") is None, "чужой чат не находится")
    check(len(tenants.all_teams()) == 3, "в списке три команды")

    try:
        tenants.register("Чужак", chat_id="-100222")
        check(False, "один чат нельзя отдать двум командам")
    except Exception:
        check(True, "один чат нельзя отдать двум командам")

    try:
        tenants.db_path("../../etc/passwd")
        check(False, "имя команды не пускает в чужие каталоги")
    except ValueError:
        check(True, "имя команды не пускает в чужие каталоги")


def test_isolation() -> None:
    print("\n=== данные команд не смешиваются ===")
    for slug, value in (("pull-up-farm", "наши"), ("spartak", "чужие")):
        with db.use(slug), db.connection() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS probe (v TEXT)")
            conn.execute("INSERT INTO probe VALUES (?)", (value,))
            conn.commit()
    with db.use("pull-up-farm"), db.connection() as conn:
        got = [r["v"] for r in conn.execute("SELECT v FROM probe")]
    check(got == ["наши"], f"своя база отдаёт своё: {got}")
    with db.use("spartak"), db.connection() as conn:
        got = [r["v"] for r in conn.execute("SELECT v FROM probe")]
    check(got == ["чужие"], f"соседняя — своё: {got}")


def test_no_tenant() -> None:
    print("\n=== без выбранной команды — падаем, а не угадываем ===")
    try:
        with db.connection():
            check(False, "обращение без команды обязано падать")
    except db.NoTenant as exc:
        check("use(" in str(exc), f"ошибка объясняет, что делать: {str(exc)[:60]}…")


def test_nesting() -> None:
    print("\n=== вложенность и возврат контекста ===")
    with db.use("pull-up-farm"):
        with db.use("spartak"):
            inner = db.current()
        outer = db.current()
    check(inner == "spartak" and outer == "pull-up-farm",
          f"после вложенного вернулись к своей: {inner} -> {outer}")
    check(db.current() == "", "снаружи команда не выбрана")


async def _concurrent() -> tuple:
    """Две задачи одновременно — команда не должна протечь между ними."""
    seen = {}

    async def worker(slug: str, pause: float) -> None:
        with db.use(slug):
            await asyncio.sleep(pause)          # уступаем управление другой
            seen[slug] = await asyncio.to_thread(db.current)

    await asyncio.gather(worker("pull-up-farm", 0.02), worker("spartak", 0.01))
    return seen.get("pull-up-farm"), seen.get("spartak")


def test_concurrency() -> None:
    print("\n=== параллельные обновления не путают команды ===")
    a, b = asyncio.run(_concurrent())
    check(a == "pull-up-farm" and b == "spartak",
          f"каждая задача осталась при своей: {a}, {b}")


def test_run_all() -> None:
    print("\n=== служебная задача по всем командам ===")
    touched = []

    def job(team) -> None:
        if team["slug"] == "spartak":
            raise RuntimeError("база битая")
        with db.connection() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS seen (v TEXT)")
            conn.commit()
        touched.append(db.current())

    res = db.run_all(job)
    check(len(res["done"]) == 2 and "spartak" in res["failed"],
          f"сбой одной не уносит остальных: {res}")
    check(touched == res["done"], "внутри задачи выбрана та команда, чья очередь")


def test_paid_and_forget() -> None:
    print("\n=== оплата и уход клиента ===")
    from datetime import date, timedelta
    tenants.set_field("spartak", "paid_until", (date.today() - timedelta(days=1)).isoformat())
    team = tenants.by_slug("spartak")
    check(not tenants.is_paid(team), "просроченная оплата видна")
    check(not tenants.working(team), "неоплаченный не считается работающим")
    tenants.set_field("spartak", "paid_until", "")
    check(tenants.is_paid(tenants.by_slug("spartak")),
          "пустая дата — без ограничения (свои и тестовые)")

    path = tenants.db_path("spartak-2")
    with db.use("spartak-2"), db.connection() as conn:
        conn.execute("CREATE TABLE t (v TEXT)")
        conn.commit()
    check(path.exists(), "база была")
    res = tenants.forget("spartak-2")
    check(not path.exists(), f"после ухода файла нет: {res['removed']}")
    check(tenants.by_slug("spartak-2")["status"] == tenants.GONE,
          "след в реестре остался")
    check(tenants.by_chat("-100333") is None, "чат освободился")
    check(len(tenants.all_teams()) == 2, "в активных его больше нет")


def test_parsers() -> None:
    print("\n=== разбор того, что пишет тренер ===")
    for text, want in (
            ("среда и пятница 20:30", ([2, 4], "20:30")),
            ("ср, пт 20.30", ([2, 4], "20:30")),
            ("понедельник четверг 19:00", ([0, 3], "19:00")),
            ("вторник 7:05", ([1], "07:05")),
    ):
        got = onboarding.parse_trainings(text)
        ok = got and (got["days"], got["time"]) == want
        check(bool(ok), f"«{text}» -> {got}")
    check(onboarding.parse_trainings("по вторникам") is None,
          "без времени — переспрашиваем, а не выдумываем")
    check(onboarding.parse_trainings("25:99") is None, "чушь во времени отвергаем")

    check(onboarding.parse_dues("5000 и 500") == {"season": 5000, "game": 500},
          "две суммы")
    check(onboarding.parse_dues("5000") == {"season": 5000, "game": 0},
          "одна сумма — это месяц")
    check(onboarding.parse_dues("потом") is None, "без чисел — не угадываем")

    people = onboarding.parse_roster(
        "1. Иванов Иван\n2) Петров Пётр @petrov 7\n\nСидоров\n")
    check(len(people) == 3, f"строк разобрано: {len(people)}")
    check(people[1] == {"surname": "Петров", "name": "Пётр",
                        "username": "petrov", "number": "7"},
          f"ник и номер отделены: {people[1]}")
    check(people[2]["surname"] == "Сидоров" and people[2]["name"] == "",
          "одна фамилия тоже годится")


def test_wizard() -> None:
    print("\n=== мастер подключения ===")
    uid = "5551"
    onboarding.drop(uid)
    st = onboarding.start(uid)
    check(st["step"] == "chat", "без чата первым делом просим добавить в чат")
    check("ничего вводить" in onboarding.question(uid)["text"].lower(),
          "и обещаем, что вводить ничего не нужно")

    onboarding.set_chat(uid, "-100777", title="Соколы")
    q = onboarding.question(uid)
    check(q["step"] == "title" and "Соколы" in q["text"],
          "название группы предложено как название команды")

    # Пустой ответ = «да, предложенное подходит»: тренер жмёт кнопку, а не
    # перепечатывает название своей же группы.
    res = onboarding.accept(uid, "")
    check(res["ok"] and res["step"] == "trainings",
          "пустой ответ принимает предложенное название")
    check(onboarding.state(uid)["data"]["title"] == "Соколы",
          "и запоминает именно его")

    bad_res = onboarding.accept(uid, "по вторникам")
    check(not bad_res["ok"] and "дни и время" in bad_res["error"],
          f"непонятное расписание объясняется: {bad_res['error']}")
    check(onboarding.state(uid)["step"] == "trainings",
          "после ошибки шаг не съехал")

    check(onboarding.accept(uid, "вт и чт 20:00")["step"] == "dues", "расписание принято")
    check(onboarding.accept(uid, "пропустить")["step"] == "roster",
          "взносы можно пропустить")
    res = onboarding.accept(uid, "Орлов Пётр @orlov 10\nСоколов Иван")
    check(res["ok"] and res["done"], "состав принят, мастер дошёл до конца")

    text = onboarding.summary(uid)
    check("Соколы" in text and "вторник" in text and "2" in text,
          f"итог показывает собранное:\n{text}")

    made = onboarding.finish(uid)
    team = made["team"]
    check(team["chat_id"] == "-100777", "команда заведена с тем самым чатом")
    check(made["players"] == 2, "состав перенесён")
    check(onboarding.state(uid) is None, "мастер за собой прибрал")

    with db.use(team["slug"]):
        check(schema.setting("training_time") == "20:00", "расписание сохранено")
        check(schema.setting("coach_id") == uid, "тренер запомнен")
        check(schema.setting("dues_season") == "", "пропущенное не выдумано")
        names = [p["title"] for p in schema.players()]
        check(names == ["Орлов Пётр", "Соколов Иван"], f"состав по алфавиту: {names}")
        me = schema.link_player("9001", username="orlov")
        check(me and me["surname"] == "Орлов", "игрок опознан по нику из списка")
        check(schema.link_player("9002", username="orlov") is None,
              "второй раз тот же ник не отдаётся")


class _Chat:
    def __init__(self, cid, ctype="private", title=""):
        self.id, self.type, self.title = cid, ctype, title


class _User:
    def __init__(self, uid, username="coach"):
        self.id, self.username = uid, username


class _Msg:
    def __init__(self, text="", chat=None, user=None, thread=None):
        self.text, self.chat, self.from_user = text, chat, user
        self.message_thread_id = thread
        self.said = []

    async def reply_text(self, text="", **kw):
        self.said.append(text)


class _Bot:
    def __init__(self):
        self.sent = []

    async def send_message(self, chat_id=None, text="", **kw):
        self.sent.append((str(chat_id), text))


class _Upd:
    def __init__(self, message=None, member=None, user=None, chat=None):
        self.message = self.effective_message = message
        self.my_chat_member = member
        self.callback_query = None
        self.effective_user = user or (message.from_user if message else None)
        self.effective_chat = chat or (message.chat if message else None)


class _Ctx:
    def __init__(self, bot):
        self.bot = bot


class _Member:
    def __init__(self, chat, user, status="administrator"):
        self.chat, self.from_user = chat, user
        self.new_chat_member = type("m", (), {"status": status})()


def test_telegram_layer() -> None:
    print("\n=== телеграмный слой: путь целиком ===")
    import asyncio as aio
    from multi import bot as mb

    coach, bot = _User(7001), _Bot()
    group = _Chat(-100888, "supergroup", title="БК Молния")

    async def run():
        # 1. Бота добавили в группу — чат он узнаёт сам.
        await mb.on_added(_Upd(member=_Member(group, coach)), _Ctx(bot))
        st = onboarding.state(coach.id)
        check(st and st["chat_id"] == "-100888", "чат узнан из события, а не введён")
        check(any("личку" in t for _, t in bot.sent), "в группе объяснено, что дальше")
        check(any(c == "7001" for c, _ in bot.sent), "тренеру написали в личку")

        # 2. Отвечаем на вопросы.
        private = _Chat(7001, "private")
        for answer in ("", "ср и пт 20:30", "пропустить", "Быков Илья @bykov 9"):
            msg = _Msg(answer, private, coach)
            if answer == "":
                # Пустой ответ приходит кнопкой «Да, название подходит».
                res = onboarding.accept(coach.id, "")
                check(res["ok"], "название принято кнопкой")
                continue
            await mb.on_text(_Upd(message=msg), _Ctx(bot))
        check(onboarding.state(coach.id) is None, "мастер закончил и прибрался")

        team = tenants.by_chat("-100888")
        check(team is not None and team["title"] == "БК Молния",
              f"команда заведена: {team and team['title']}")
        check(any("подключена" in t for c, t in bot.sent if c == "-100888"),
              "в чат команды пришло подтверждение")
        with db.use(team["slug"]):
            check(schema.setting("training_time") == "20:30", "расписание записано")
            check(len(schema.players()) == 1, "состав перенесён")

        # 3. Повторное добавление в тот же чат ничего не ломает.
        before = len(tenants.all_teams())
        await mb.on_added(_Upd(member=_Member(group, coach)), _Ctx(bot))
        check(len(tenants.all_teams()) == before, "второй раз команду не заводим")
        check(any("уже работаю" in t for _, t in bot.sent), "и говорим об этом")

        # 4. Топик узнаём из того, где написали команду.
        msg = _Msg("/сюда", group, coach, thread=42)
        await mb.on_here(_Upd(message=msg, chat=group), _Ctx(bot))
        with db.use(team["slug"]):
            check(schema.setting("topic_id") == "42",
                  f"id топика взят из сообщения: {schema.setting('topic_id')}")

        # 5. Человек из команды опознаётся в личке.
        with db.use(team["slug"]):
            schema.link_player(9100, username="bykov")
        got = membership.teams_of(9100)
        check(len(got) == 1 and not got[0]["is_coach"], "игрок нашёл свою команду")
        got = membership.teams_of(coach.id)
        check(got and got[0]["is_coach"], "тренер опознан тренером")
        check(membership.teams_of(999999) == [], "посторонний ни в какой команде")

    aio.run(run())


def test_token_guard() -> None:
    print("\n=== предохранитель на токен ===")
    import os as _os
    from multi import bot as mb
    old = dict(_os.environ)
    try:
        _os.environ.pop("MULTI_BOT_TOKEN", None)
        _os.environ["BOT_TOKEN"] = "живой-токен"
        try:
            mb._token()
            check(False, "без своего токена не стартуем")
        except SystemExit:
            check(True, "без своего токена не стартуем")
        _os.environ["MULTI_BOT_TOKEN"] = "живой-токен"
        try:
            mb._token()
            check(False, "с боевым токеном не стартуем")
        except SystemExit:
            check(True, "с боевым токеном не стартуем")
        _os.environ["MULTI_BOT_TOKEN"] = "свой-токен"
        check(mb._token() == "свой-токен", "со своим токеном стартуем")
    finally:
        _os.environ.clear()
        _os.environ.update(old)


def main() -> int:
    print(f"песочница: {TMP}")
    for fn in (test_registry, test_isolation, test_no_tenant, test_nesting,
               test_concurrency, test_run_all, test_paid_and_forget,
               test_parsers, test_wizard, test_telegram_layer,
               test_token_guard):
        fn()
    shutil.rmtree(TMP, ignore_errors=True)
    print("\n" + ("ВСЁ ЗЕЛЁНОЕ" if not bad else f"ЗАМЕЧАНИЙ: {len(bad)}"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
