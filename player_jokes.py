#!/usr/bin/env python3
"""
Шутки к фамилиям: фраза от своих, которую бот дописывает к строке игрока в
сообщении о результате.

Смысл фичи — не в данных, а в том, что сообщение о результате перестаёт быть
сводкой робота. «🥇 Очки: Дроздов — 24 · опять всех перекидал (с) @kolya» —
это уже разговор команды, а не выгрузка из протокола.

Правила, о которых договорились:
  • писать может только тот, кто есть в листе «Игроки» — чужие шутки про нашу
    команду в общий чат не летят;
  • фраза адресуется фамилией (своей или чужой), случай выбирается: победа,
    поражение или любой исход;
  • фраз на человека может быть много — при публикации берётся случайная;
  • автор подписывается ником, и это не украшение: подпись — единственное, что
    удерживает от анонимной грубости.

Юр-инвариант ([[legal-data-invariant]]): адресат хранится СТРОКОЙ листа
«Игроки» (row_index), не ФИО. Ник автора храним осознанно — он публикуется.
"""

import html
import random
import re
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

OCCASIONS = {"win": "после победы", "loss": "после поражения", "any": "в любом случае"}

MAX_LEN = 120          # длиннее — это уже не подпись к строке, а сообщение
MAX_PER_AUTHOR = 20    # чтобы один человек не забил ленту результатов
MAX_PER_MESSAGE = 2    # больше — и сообщение о результате превращается в балаган


def _norm(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def find_player(name: str) -> List[Dict[str, Any]]:
    """Кандидаты из листа «Игроки» по тому, что человек ввёл руками.

    Ищем по фамилии, а не по полному совпадению: «Дроздов» должно находить
    «Дроздов Даниил». Возвращаем всех подходящих — выбирать будет человек,
    иначе однофамильцы молча получат чужую шутку."""
    key = _norm(name)
    if not key:
        return []
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            "SELECT row_index, surname, name FROM players WHERE surname != '' OR name != ''")]
    exact, partial = [], []
    for r in rows:
        full = _norm(f"{r['surname']} {r['name']}")
        sur = _norm(r["surname"])
        if full == key or sur == key:
            exact.append(r)
        elif key and (sur.startswith(key) or full.startswith(key)):
            partial.append(r)
    return exact or partial


def validate(text: str) -> Optional[str]:
    """Причина отказа или None. Проверяем форму, а не содержание: цензуру
    наводит подпись автора и админ, который видит весь список."""
    t = (text or "").strip()
    if len(t) < 3:
        return "Слишком коротко — напиши хотя бы пару слов."
    if len(t) > MAX_LEN:
        return f"Слишком длинно: {len(t)} символов, а помещается {MAX_LEN}."
    if re.search(r"https?://|t\.me/|@[A-Za-z0-9_]{4,}", t):
        return "Без ссылок и упоминаний — это подпись к строке, а не сообщение."
    if "\n" in t:
        return "Одной строкой, пожалуйста."
    return None


def add(target_row: int, occasion: str, text: str,
        author_id: Any, author_nick: str = "") -> Tuple[bool, str]:
    """Добавляет фразу. (получилось, что сказать человеку)."""
    err = validate(text)
    if err:
        return False, err
    if occasion not in OCCASIONS:
        occasion = "any"
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        mine = conn.execute(
            "SELECT COUNT(*) n FROM player_jokes WHERE author_id = ? AND active = 1",
            (str(author_id),)).fetchone()["n"]
        if mine >= MAX_PER_AUTHOR:
            return False, (f"У тебя уже {mine} фраз — это предел. "
                           f"Удали ненужные в «Мои фразы».")
        # Сравниваем в Python, а не в SQL: sqlite-шный lower() кириллицу не
        # трогает, и «ОПЯТЬ» с «опять» прошли бы как разные фразы.
        existing = [r["text"] for r in conn.execute(
            "SELECT text FROM player_jokes WHERE target_row = ? AND active = 1",
            (int(target_row),))]
        if _norm(text) in {_norm(x) for x in existing}:
            return False, "Такая фраза для этого игрока уже есть."
        conn.execute(
            """INSERT INTO player_jokes
               (target_row, occasion, text, author_id, author_nick, created_at, active)
               VALUES (?, ?, ?, ?, ?, ?, 1)""",
            (int(target_row), occasion, text.strip(), str(author_id),
             (author_nick or "").lstrip("@"), sheets_cache.now_iso()))
        conn.commit()
    return True, "Готово. Фраза появится в сообщении о результате."


def remove(joke_id: int, author_id: Optional[Any] = None) -> bool:
    """Прячет фразу. Автор убирает свою, админ — любую (author_id=None)."""
    sheets_cache.init_db()
    sql = "UPDATE player_jokes SET active = 0 WHERE id = ?"
    args: List[Any] = [int(joke_id)]
    if author_id is not None:
        sql += " AND author_id = ?"
        args.append(str(author_id))
    with sheets_cache.get_connection() as conn:
        n = conn.execute(sql, args).rowcount
        conn.commit()
    return bool(n)


def listing(author_id: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Фразы автора (или все, если author_id=None) с ФИО адресата.

    ФИО подставляем ИЗ ЛИСТА при чтении — в самой таблице шуток его нет."""
    sheets_cache.init_db()
    sql = ("""SELECT j.*, p.surname, p.name FROM player_jokes j
              LEFT JOIN players p ON p.row_index = j.target_row
              WHERE j.active = 1""")
    args: List[Any] = []
    if author_id is not None:
        sql += " AND j.author_id = ?"
        args.append(str(author_id))
    sql += " ORDER BY j.id DESC"
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(sql, args)]
    for r in rows:
        r["target"] = f"{r.get('surname') or ''} {r.get('name') or ''}".strip() \
            or f"строка {r['target_row']}"
    return rows


class Jokes:
    """Подписи для ОДНОГО сообщения о результате.

    Живёт на время сборки сообщения: помнит, сколько шуток уже вставила и кому,
    чтобы результат не превратился в ленту юмора. Одному игроку — одна фраза,
    на сообщение — не больше MAX_PER_MESSAGE.
    """

    def __init__(self, won: Optional[bool], limit: int = MAX_PER_MESSAGE):
        self.occasions = ("any",) if won is None else \
            (("win", "any") if won else ("loss", "any"))
        self.limit = limit
        self.used = 0
        self.seen: set = set()
        self._by_row: Dict[int, List[Dict[str, Any]]] = {}
        self._names: Dict[str, int] = {}
        self._load()

    def _load(self) -> None:
        try:
            sheets_cache.init_db()
            marks = ",".join("?" * len(self.occasions))
            with sheets_cache.get_connection() as conn:
                for r in conn.execute(
                        f"""SELECT j.id, j.target_row, j.text, j.author_nick
                            FROM player_jokes j
                            WHERE j.active = 1 AND j.occasion IN ({marks})""",
                        list(self.occasions)):
                    self._by_row.setdefault(int(r["target_row"]), []).append(dict(r))
                if self._by_row:
                    for r in conn.execute(
                            "SELECT row_index, surname, name FROM players"):
                        full = _norm(f"{r['surname']} {r['name']}")
                        if full:
                            self._names[full] = int(r["row_index"])
                            self._names.setdefault(_norm(r["surname"]), int(r["row_index"]))
        except Exception:
            # Шутки — украшение. Любая беда с базой не должна помешать команде
            # узнать счёт, поэтому молча остаёмся без них.
            self._by_row, self._names = {}, {}

    def for_name(self, name: str) -> str:
        """« · фраза (с) @ник» для игрока или пустая строка."""
        if self.used >= self.limit or not self._by_row:
            return ""
        row = self._names.get(_norm(name))
        if row is None:
            # Имя из протокола может отличаться от листа («Шлепикас Ромас» /
            # «Шлепикас Роман») — пробуем по фамилии.
            row = self._names.get(_norm(name).split(" ")[0] if name else "")
        if row is None or row in self.seen:
            return ""
        pool = self._by_row.get(row) or []
        if not pool:
            return ""
        pick = random.choice(pool)
        self.seen.add(row)
        self.used += 1
        # Оба сообщения о результате уходят с parse_mode='HTML'. Человек мог
        # написать «<3» или «Гиря & Ко» — без экранирования Telegram отверг бы
        # ВСЁ сообщение, и команда осталась бы без счёта из-за одной шутки.
        text = html.escape(pick["text"])
        nick = html.escape(pick["author_nick"])
        sign = f" (с) @{nick}" if nick else ""
        return f" · {text}{sign}"


def decorate(name: str, won: Optional[bool]) -> str:
    """Разовая подпись, когда сообщение собирается вне класса Jokes."""
    return Jokes(won, limit=1).for_name(name)
