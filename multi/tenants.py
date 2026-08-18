"""Реестр команд: кто подключён, где его база, что оплачено.

Сам реестр — отдельная маленькая база. Держать его внутри чьей-то команды
нельзя: он про всех сразу, и удаление одной команды не должно задевать список
остальных.

Что здесь НЕ хранится и почему:

* Имена и телефоны людей — они живут в базе своей команды. Реестр читают все
  служебные задачи подряд, и чем меньше в нём личного, тем спокойнее.
* Токен бота — он один на всех и лежит в окружении. Появятся команды со своим
  токеном (упрёмся в лимиты Телеграма) — добавим колонку, схема к этому готова.
"""

from __future__ import annotations

import os
import re
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

ROOT = Path(os.getenv("MULTI_DATA_DIR", Path(__file__).resolve().parent.parent / "data" / "multi"))

ACTIVE = "active"      # работает
PAUSED = "paused"      # не платит или сам попросил остановить
GONE = "gone"          # ушёл; база уже удалена, строка осталась как след

SCHEMA = """
CREATE TABLE IF NOT EXISTS tenants (
    slug        TEXT PRIMARY KEY,      -- короткое имя, оно же имя файла базы
    title       TEXT NOT NULL,         -- как команда называет себя
    chat_id     TEXT NOT NULL DEFAULT '',  -- общий чат: по нему узнаём команду
    status      TEXT NOT NULL DEFAULT 'active',
    paid_until  TEXT NOT NULL DEFAULT '',  -- ISO-дата; пусто — не ограничено
    created_at  TEXT NOT NULL,
    note        TEXT NOT NULL DEFAULT ''
);
-- Чат ищем на каждом входящем сообщении, поэтому индекс. UNIQUE намеренно:
-- один чат не может принадлежать двум командам, и поймать это надо на записи,
-- а не в момент, когда бот показывает не тот состав.
CREATE UNIQUE INDEX IF NOT EXISTS uq_tenants_chat
    ON tenants(chat_id) WHERE chat_id != '';
"""

SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,30}$")


def registry_path() -> Path:
    return ROOT / "tenants.db"


def db_path(slug: str) -> Path:
    """Файл базы команды. Имя проверено SLUG_RE — в путь не подставить «..»."""
    if not SLUG_RE.match(slug or ""):
        raise ValueError(f"Недопустимое имя команды: {slug!r}")
    return ROOT / "teams" / f"{slug}.db"


@contextmanager
def _registry() -> Iterator[sqlite3.Connection]:
    ROOT.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(registry_path(), timeout=8.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        yield conn
    finally:
        conn.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def slugify(title: str) -> str:
    """«Pull Up Farm» -> «pull-up-farm». Имя видно только нам, но оно же имя
    файла базы, поэтому только латиница, цифры и дефис."""
    table = str.maketrans({
        "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
        "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
        "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
        "ф": "f", "х": "h", "ц": "c", "ч": "ch", "ш": "sh", "щ": "sch",
        "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
    })
    low = str(title or "").strip().lower().translate(table)
    slug = re.sub(r"[^a-z0-9]+", "-", low).strip("-")[:31]
    return slug or "team"


def free_slug(title: str) -> str:
    """Свободное имя: «спартак», «спартак-2», «спартак-3»…

    Команды с одинаковыми названиями встречаются чаще, чем кажется, — в одном
    городе может быть два «Динамо»."""
    base = slugify(title)
    taken = {t["slug"] for t in all_teams(status=None)}
    if base not in taken:
        return base
    for n in range(2, 100):
        candidate = f"{base[:28]}-{n}"
        if candidate not in taken:
            return candidate
    raise RuntimeError(f"Не нашёл свободного имени для {title!r}")


def register(title: str, chat_id: str = "", slug: str = "",
             note: str = "") -> Dict[str, Any]:
    """Заводит команду и создаёт файл её базы."""
    slug = slug or free_slug(title)
    if not SLUG_RE.match(slug):
        raise ValueError(f"Недопустимое имя команды: {slug!r}")
    with _registry() as conn:
        conn.execute(
            """INSERT INTO tenants (slug, title, chat_id, status, created_at, note)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (slug, str(title).strip(), str(chat_id or ""), ACTIVE, _now(), note))
        conn.commit()
    path = db_path(slug)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    return by_slug(slug) or {}


def by_slug(slug: str) -> Optional[Dict[str, Any]]:
    with _registry() as conn:
        row = conn.execute("SELECT * FROM tenants WHERE slug = ?", (str(slug),)).fetchone()
    return dict(row) if row else None


def by_chat(chat_id: Any) -> Optional[Dict[str, Any]]:
    """Команда по общему чату — так бот узнаёт, чьё сообщение пришло."""
    with _registry() as conn:
        row = conn.execute("SELECT * FROM tenants WHERE chat_id = ?",
                           (str(chat_id),)).fetchone()
    return dict(row) if row else None


def all_teams(status: Optional[str] = ACTIVE) -> List[Dict[str, Any]]:
    """Все команды. status=None — включая приостановленных и ушедших."""
    with _registry() as conn:
        if status is None:
            rows = conn.execute("SELECT * FROM tenants ORDER BY created_at")
        else:
            rows = conn.execute(
                "SELECT * FROM tenants WHERE status = ? ORDER BY created_at",
                (status,))
        return [dict(r) for r in rows]


def set_field(slug: str, field: str, value: Any) -> bool:
    """Правит одно поле. Список полей закрыт: значение приходит снаружи."""
    if field not in ("title", "chat_id", "status", "paid_until", "note"):
        raise ValueError(f"Такое поле не правим: {field!r}")
    with _registry() as conn:
        n = conn.execute(f"UPDATE tenants SET {field} = ? WHERE slug = ?",
                         (str(value), str(slug))).rowcount
        conn.commit()
    return bool(n)


def is_paid(team: Dict[str, Any], today: Optional[date] = None) -> bool:
    """Оплачено ли. Пустая дата — не ограничено (свои, тестовые, бесплатные)."""
    until = str(team.get("paid_until") or "").strip()
    if not until:
        return True
    try:
        return date.fromisoformat(until[:10]) >= (today or date.today())
    except ValueError:
        return True


def working(team: Dict[str, Any]) -> bool:
    return team.get("status") == ACTIVE and is_paid(team)


def forget(slug: str, keep_row: bool = True) -> Dict[str, Any]:
    """Клиент ушёл: сносим его базу.

    Одна кнопка, один файл — в договоре это сильный пункт, а в общей базе
    честно удалить клиента почти невозможно: его строки размазаны по сорока
    таблицам, и всегда найдётся забытая.

    Строку в реестре по умолчанию оставляем (status=gone): так видно, что имя
    занято и что команда была. Полное удаление — keep_row=False."""
    removed = []
    path = db_path(slug)
    for p in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        if p.exists():
            p.unlink()
            removed.append(p.name)
    with _registry() as conn:
        if keep_row:
            conn.execute("UPDATE tenants SET status = ?, chat_id = '' WHERE slug = ?",
                         (GONE, str(slug)))
        else:
            conn.execute("DELETE FROM tenants WHERE slug = ?", (str(slug),))
        conn.commit()
    return {"slug": slug, "removed": removed, "row_kept": keep_row}
