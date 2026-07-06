#!/usr/bin/env python3
"""
Запуск скриптов проекта подпроцессом — общий механизм для кнопок
"Запуск оповещений" в /admin (bot_daemon.py) и для game_watcher.py.
Вынесено в отдельный модуль, чтобы избежать циклического импорта
(game_watcher.py импортирует это, а bot_daemon.py импортирует
game_watcher.py).
"""

import asyncio
import sys
from pathlib import Path
from typing import List, Tuple

REPO_DIR = Path(__file__).parent


async def run_script(script_name: str, args: List[str]) -> Tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable, str(REPO_DIR / script_name), *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        cwd=str(REPO_DIR),
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode or 0, stdout.decode(errors="replace"), stderr.decode(errors="replace")


def summarize_output(stdout: str, max_lines: int = 12) -> str:
    """Скрипты печатают много построчной отладки (например, каждого
    проверяемого игрока) — оставляем только содержательные строки
    (со статус-эмодзи), чтобы в Telegram было видно, что реально
    произошло, а не просто 'готово'."""
    noisy_prefixes = ("🔍", "   ", "--", "=")
    meaningful = [
        line.strip() for line in stdout.splitlines()
        if line.strip() and not line.strip().startswith(noisy_prefixes)
    ]
    if not meaningful:
        return "(скрипт не вывел статусных строк, см. полный лог в journalctl)"
    return "\n".join(meaningful[-max_lines:])
