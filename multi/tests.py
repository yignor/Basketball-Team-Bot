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

from multi import db, tenants          # noqa: E402  (после подмены папки)

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


def main() -> int:
    print(f"песочница: {TMP}")
    for fn in (test_registry, test_isolation, test_no_tenant, test_nesting,
               test_concurrency, test_run_all, test_paid_and_forget):
        fn()
    shutil.rmtree(TMP, ignore_errors=True)
    print("\n" + ("ВСЁ ЗЕЛЁНОЕ" if not bad else f"ЗАМЕЧАНИЙ: {len(bad)}"))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
