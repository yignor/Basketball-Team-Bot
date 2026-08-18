"""Текущая команда и её база.

Вся схема многокомандности держится на одной мысли: **соединение открывает файл
той команды, чьё сообщение сейчас обрабатывается**. Никаких «где тут признак
арендатора» в запросах — их просто негде забыть.

Текущую команду держим в `contextvars`, а не в обычной переменной модуля. Это
не украшение: бот обрабатывает обновления параллельно (`concurrent_updates`),
и глобальная переменная означала бы, что одно сообщение подменяет команду
другому прямо посреди обработки. `contextvars` изолирован по задаче и —
проверено — доживает до рабочих потоков `asyncio.to_thread`, которыми в боте
сделаны все походы в базу.

Главный предохранитель: `connection()` падает, если команда не задана. Забытый
контекст обязан ронять запрос с внятной ошибкой, а не молча открывать чужую
базу. Ошибку видно на первом же прогоне тестов; молчаливая утечка не видна
никогда — до жалобы клиента.
"""

from __future__ import annotations

import contextvars
import sqlite3
from contextlib import contextmanager
from typing import Any, Callable, Dict, Iterator, List, Optional

from . import tenants

# Имя текущей команды. Пусто — команда не выбрана, и это ошибка, а не «ничего».
_current: contextvars.ContextVar[str] = contextvars.ContextVar("tenant", default="")


class NoTenant(RuntimeError):
    """Обращение к базе без выбранной команды."""


def current() -> str:
    return _current.get()


@contextmanager
def use(slug: str) -> Iterator[str]:
    """Работаем от имени команды. По выходе возвращаем как было.

    Вложенность допустима: служебная задача может пройтись по всем командам,
    а внутри — уйти в конкретную."""
    if not tenants.SLUG_RE.match(slug or ""):
        raise ValueError(f"Недопустимое имя команды: {slug!r}")
    token = _current.set(slug)
    try:
        yield slug
    finally:
        _current.reset(token)


def _connect(slug: str) -> sqlite3.Connection:
    path = tenants.db_path(slug)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=8.0)
    # Те же настройки, что у нынешнего бота: WAL, чтобы чтение не ждало записи,
    # и NORMAL, чтобы не платить fsync на каждый commit (сервер не эфемерный).
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 8000")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def connection(slug: str = "") -> Iterator[sqlite3.Connection]:
    """Соединение с базой текущей (или явно названной) команды."""
    slug = slug or current()
    if not slug:
        raise NoTenant(
            "Команда не выбрана. Оберни работу в multi.db.use(<команда>) — "
            "иначе непонятно, чью базу открывать, и молча открыть чужую нельзя.")
    conn = _connect(slug)
    try:
        yield conn
    finally:
        conn.close()


def for_each(status: Optional[str] = tenants.ACTIVE) -> Iterator[Dict[str, Any]]:
    """Обход команд для служебных задач: рассылок, миграций, уборки.

    Именно явный обход, а не «сделай для всех разом»: фоновая задача обязана
    видеть, что работает с каждой командой отдельно, — тогда и сбой у одной не
    уносит остальных (см. run_all)."""
    for team in tenants.all_teams(status=status):
        with use(team["slug"]):
            yield team


def run_all(job: Callable[[Dict[str, Any]], Any],
            status: Optional[str] = tenants.ACTIVE) -> Dict[str, Any]:
    """Прогоняет задачу по всем командам. Сбой одной не мешает остальным.

    Так устроены все ночные работы: одна команда с битой базой или пустым
    «Конфигом» не должна оставлять без напоминаний остальных четырнадцать."""
    done, failed = [], {}
    for team in for_each(status=status):
        try:
            job(team)
            done.append(team["slug"])
        except Exception as exc:
            failed[team["slug"]] = f"{type(exc).__name__}: {exc}"
    return {"done": done, "failed": failed}
