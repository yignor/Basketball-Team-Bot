#!/usr/bin/env python3
"""Все тесты одной командой. Запускать перед деплоем.

    python3 tests/run.py                 # на копии боевой базы из data/
    BOT_TEST_DB=/путь/копия.db python3 tests/run.py

Работает без pytest и вообще без новых зависимостей: их установка на сервере
требует пароля, а тест, который нельзя запустить там, где живёт бот, —
бесполезен.

База нужна ЛЮБАЯ настоящая: тесты ходят по реальным экранам с реальными
списками. Пишут они только во временные таблицы, но копию всё равно берите —
привычка дороже.
"""

import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SUITES = ["test_buttons.py", "test_flows.py", "test_private.py",
          "test_privacy.py", "test_debts.py", "test_roster.py",
          "test_fantasy.py", "test_video.py", "test_access.py",
          "test_moments.py", "test_webapp.py", "test_calendar.py", "test_league_push.py", "test_players_edit.py", "test_templates.py"]


def main() -> int:
    bad = []
    for name in SUITES:
        print(f"\n{'=' * 60}\n▶ {name}\n{'=' * 60}")
        code = subprocess.call([sys.executable, str(HERE / name)])
        if code:
            bad.append(name)
    print(f"\n{'=' * 60}")
    if bad:
        print("НЕ ПРОШЛИ: " + ", ".join(bad))
        return 1
    print("ВСЕ ТЕСТЫ ЗЕЛЁНЫЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
