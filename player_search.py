"""Поиск игрока по тому, что человек набрал руками.

Одно место на весь бот: состав на игру, оплаты, шутки к фамилиям. Раньше в
каждом экране был свой поиск со своими правилами, и «здов» находило игрока в
составе, но не находило в шутках — предсказать поведение было нельзя.

Правила простые и одинаковые везде:
  • регистр и «ё» не важны;
  • сначала точные совпадения, потом начало фамилии, потом любое вхождение;
  • ищем и по фамилии, и по имени: тренер пишет «Даниил», когда фамилию не
    помнит, и это тоже должно работать.

SQL для этого не годится: sqlite-функция lower() кириллицу не трогает, и
«дроздов» не нашло бы «Дроздов». Сравниваем в Python.
"""

from __future__ import annotations

from typing import Any, Dict, List

import sheets_cache

MIN_QUERY = 2          # с одной буквы список бессмысленный
DEFAULT_LIMIT = 8


def norm(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def _rows() -> List[Dict[str, Any]]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT row_index, surname, name FROM players "
            "WHERE surname != '' OR name != '' ORDER BY surname, name")]


def find(query: str, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
    """[{row, surname, name, title}] — от самых точных к самым приблизительным.

    Пустой ответ значит «никого похожего», а не «ошибка»: вызывающий сам
    решает, спросить ли ещё раз или показать полный список."""
    key = norm(query)
    if len(key) < MIN_QUERY:
        return []
    buckets: List[List[Dict[str, Any]]] = [[], [], [], []]
    for r in _rows():
        surname, name = norm(r["surname"]), norm(r["name"])
        full = norm(f"{r['surname']} {r['name']}")
        item = {"row": int(r["row_index"]), "surname": (r["surname"] or "").strip(),
                "name": (r["name"] or "").strip(),
                "title": f"{r['surname']} {r['name']}".strip()}
        if key in (surname, full, name):
            buckets[0].append(item)
        elif surname.startswith(key) or full.startswith(key):
            buckets[1].append(item)
        elif name.startswith(key):
            buckets[2].append(item)
        elif key in full:
            buckets[3].append(item)
    out: List[Dict[str, Any]] = []
    for b in buckets:
        out.extend(b)
    return out[:limit] if limit else out


def one(query: str) -> Dict[str, Any]:
    """Единственный кандидат или {} — когда их ноль либо несколько."""
    found = find(query, limit=2)
    return found[0] if len(found) == 1 else {}
