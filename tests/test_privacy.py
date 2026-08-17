#!/usr/bin/env python3
"""След на диске: что бот записывает туда, откуда это потом не стереть.

    python3 tests/test_privacy.py

У проекта есть жёсткое правило: **ФИО не живут на диске**. В таблицах —
идентификаторы (строка листа, telegram id, federation_id), имена подтягиваются
в момент показа и живут в оперативной памяти. Правило соблюдали в схемах базы,
а про журнал забыли: качалка писала в него «Иванов Иван и Иванов Иван — один
человек», и вместе с датой рождения. Журнал ложится на диск, уезжает в бэкапы
и живёт столько, сколько настроена ротация.

Отсюда проверка: ни одна строка журнала не должна собираться из полей, в
которых лежит имя. Разбирается исходник, а не поведение, — иначе поймать можно
только ту ветку, которая случилась в тесте, а ошибка приходит из редкой.

Файл не про экраны и не про сценарии, поэтому лежит отдельно: сюда же
складывать всё, что проверяет след бота на диске.
"""

from __future__ import annotations

import ast
import os
import sys
import tempfile
from pathlib import Path
from typing import List, Tuple

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
os.environ["SPREADSHEET_ID"] = ""

import sheets_cache                                             # noqa: E402

# Поля, в которых лежит имя человека. Ключами их читают из листа «Игроки»
# (`title` — «Фамилия Имя»), из заявок лиг (`name`) и из карточек (`surname`).
# `birth` — дата рождения: сама по себе не имя, но в паре с чем угодно даёт
# определяемое лицо, а нужна она только для склейки и только в памяти.
NAMED = ("['name']", '["name"]', "['title']", '["title"]',
         "['surname']", '["surname"]', "['birth']", '["birth"]')

LOG_CALLS = ("info", "warning", "error", "debug", "exception")

# Куда не заглядываем: чужой код и сами тесты.
SKIP_DIRS = {"tests", "__pycache__", "node_modules"}

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def own_files() -> List[Path]:
    """Свои .py: без venv, без скрытых каталогов, без тестов."""
    out = []
    for p in sorted(ROOT.rglob("*.py")):
        rel = p.relative_to(ROOT)
        if any(part.startswith(".") or part in SKIP_DIRS for part in rel.parts):
            continue
        out.append(p)
    return out


def log_calls_with_names(path: Path) -> List[Tuple[int, str]]:
    """Строки, где в журнал уходит поле с именем."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError:
        return []
    found = []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
            continue
        if node.func.attr not in LOG_CALLS:
            continue
        # Ловим и log.info(...), и self.logger.warning(...): важно не кто
        # пишет, а что уходит в строку.
        src = ast.unparse(node)
        for mark in NAMED:
            if mark in src:
                found.append((node.lineno, src[:120]))
                break
    return found


def test_no_names_in_logs() -> None:
    print("\n=== имена не уходят в журнал ===")
    files = own_files()
    check(len(files) > 20, f"файлов на разбор: {len(files)}")
    dirty = []
    for p in files:
        for line, src in log_calls_with_names(p):
            dirty.append(f"{p.relative_to(ROOT)}:{line}  {src}")
    check(not dirty, "ни одной строки журнала с именем"
          + ("" if not dirty else ":\n     " + "\n     ".join(dirty[:8])))


def test_guard_works() -> None:
    """Проверка обязана ловить то, ради чего написана.

    Тест на тест: без этого «зелено» может значить и «всё чисто», и «разбор
    сломался и ничего не находит». Второе неотличимо от первого ровно до того
    дня, когда понадобится."""
    print("\n=== проверка ловит подделку ===")
    fake = ROOT / "tests" / "_privacy_probe.py"
    fake.write_text(
        "import logging\n"
        "log = logging.getLogger()\n"
        "def f(a, b):\n"
        "    log.info(f\"качалка: {a['name']} и {b['name']} — один человек\")\n",
        encoding="utf-8")
    try:
        got = log_calls_with_names(fake)
        check(len(got) == 1, f"подделка поймана: {got}")
    finally:
        fake.unlink(missing_ok=True)


def test_db_is_closed_to_strangers() -> None:
    """База закрыта от посторонних пользователей сервера.

    Причина та же, по которой закрыты логи: файл ложится на диск и уезжает в
    бэкапы, а внутри вся команда — ФИО, ники, дни рождения, оплаты и долги.
    17.08.2026 обнаружилось, что боевая база лежала с правами 644, то есть
    читать её мог любой в системе.

    Права выставляет сам sheets_cache при подключении, а не разово руками:
    базу открывает ещё десяток cron-скриптов, и файлы -wal/-shm создаёт тот
    процесс, который пришёл первым, — с обычным umask, то есть снова широко."""
    import stat
    import subprocess
    import sys as _sys
    print("\n=== база закрыта от посторонних ===")

    tmp = Path(tempfile.mkdtemp()) / "data" / "bot.db"
    old, sheets_cache.DB_PATH = sheets_cache.DB_PATH, tmp
    try:
        sheets_cache.init_db()
        conn = sheets_cache._connect()
        conn.execute("CREATE TABLE IF NOT EXISTS t (a)")
        conn.execute("INSERT INTO t VALUES (1)")
        seen = {}
        for path in (tmp.parent, tmp, tmp.with_name(tmp.name + "-wal"),
                     tmp.with_name(tmp.name + "-shm")):
            if path.exists():
                seen[path.name] = stat.S_IMODE(path.stat().st_mode)
        conn.close()

        check(seen.get("data") == 0o750, f"каталог 750: {oct(seen.get('data', 0))}")
        for name in ("bot.db", "bot.db-wal", "bot.db-shm"):
            check(seen.get(name) == 0o640,
                  f"{name} — 640, посторонним не читается: {oct(seen.get(name, 0))}")

        # Чужой процесс с открытым umask не должен вернуть широкие права.
        subprocess.run(
            [_sys.executable, "-c",
             "import os, pathlib, sys\n"
             "os.environ['GOOGLE_SHEETS_CREDENTIALS'] = ''\n"
             "os.environ['SPREADSHEET_ID'] = ''\n"
             f"sys.path.insert(0, {str(ROOT)!r})\n"
             "os.umask(0)\n"
             "import sheets_cache\n"
             f"sheets_cache.DB_PATH = pathlib.Path({str(tmp)!r})\n"
             "with sheets_cache.get_connection() as c: c.execute('SELECT 1')"],
            check=True, capture_output=True)
        check(stat.S_IMODE(tmp.stat().st_mode) == 0o640,
              "после чужого процесса с umask 0 права держатся")
    finally:
        sheets_cache.DB_PATH = old


def main() -> int:
    test_no_names_in_logs()
    test_db_is_closed_to_strangers()
    test_guard_works()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("СЛЕД НА ДИСКЕ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
