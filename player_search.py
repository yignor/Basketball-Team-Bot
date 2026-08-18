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

from typing import Any, Callable, Dict, List, Sequence

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


def rank(query: str, items: Sequence[Any], fields: Callable[[Any], Sequence[str]],
         limit: int = DEFAULT_LIMIT) -> List[Any]:
    """Отбор и сортировка чего угодно по тому же правилу, что и игроки.

    Обобщение появилось из практики: одинаково ищут игрока в составе, игрока
    для долга, соперника при создании игры — а поиск в каждом месте писали
    заново, и вёл он себя по-разному. `fields` отдаёт строки, по которым можно
    найти вещь (для игрока — фамилия, имя, «фамилия имя»; для команды — её
    название).

    Порядок вёдер и есть правило: точное совпадение, начало, вхождение. Внутри
    ведра сохраняется исходный порядок — обычно алфавитный, и он предсказуем."""
    key = norm(query)
    if len(key) < MIN_QUERY:
        return []
    buckets: List[List[Any]] = [[], [], []]
    for it in items:
        vals = [norm(v) for v in fields(it) if v]
        if not vals:
            continue
        if key in vals:
            buckets[0].append(it)
        elif any(v.startswith(key) for v in vals):
            buckets[1].append(it)
        elif any(key in v for v in vals):
            buckets[2].append(it)
    out: List[Any] = []
    for b in buckets:
        out.extend(b)
    return out[:limit] if limit else out


def find(query: str, limit: int = DEFAULT_LIMIT) -> List[Dict[str, Any]]:
    """[{row, surname, name, title}] — от самых точных к самым приблизительным.

    Пустой ответ значит «никого похожего», а не «ошибка»: вызывающий сам
    решает, спросить ли ещё раз или показать полный список."""
    people = [{"row": int(r["row_index"]),
               "surname": (r["surname"] or "").strip(),
               "name": (r["name"] or "").strip(),
               "title": f"{r['surname']} {r['name']}".strip()}
              for r in _rows()]
    return rank(query, people, person_fields, limit)


def person_fields(p: Any) -> List[str]:
    """По чему ищем человека. Годится и для карточки из player_search, и для
    карточки из coach_payments — у обеих одни и те же имена полей."""
    get = p.get if isinstance(p, dict) else (lambda k, d="": getattr(p, k, d))
    surname, name = get("surname", ""), get("name", "")
    title = get("title", "") or f"{surname} {name}".strip()
    # И «Иванов Иван», и «Иван Иванов»: тренер набирает как привык.
    return [surname, name, title, f"{name} {surname}".strip()]


def one(query: str) -> Dict[str, Any]:
    """Единственный кандидат или {} — когда их ноль либо несколько."""
    found = find(query, limit=2)
    return found[0] if len(found) == 1 else {}
