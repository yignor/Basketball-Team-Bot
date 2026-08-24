#!/usr/bin/env python3
"""Выгрузка состава в табличный файл — под заявку в лигу.

Зачем. Заявку в лигу тренер набивает руками каждый сезон, хотя все эти люди у
бота уже есть: фамилия, имя, дата рождения. Переписывание сорока строк из чата
в таблицу — работа, которой быть не должно.

Почему CSV, а не xlsx. Библиотек для xlsx на сервере нет, а поставить их туда
нельзя: окружение бота принадлежит другому пользователю, и установка требует
пароля. CSV открывается и Excel, и Google Таблицами, и в заявку оттуда
копируется так же. Две мелочи, без которых он открывается плохо:

* **BOM в начале** — иначе русский Excel читает файл как cp1251 и показывает
  кракозябры;
* **точка с запятой вместо запятой** — в русской локали Excel делит строки
  именно по ней, а по запятой сваливает всё в один столбец.

Дата рождения хранится как «2001-09-22», а в заявках её пишут «22.09.2001» —
переворачиваем здесь, чтобы тренер не правил сорок ячеек руками.

ФИО тут есть, и это не противоречит юр-инварианту проекта: лист «Игроки» —
собственная таблица команды, тренер видит эти имена и в боте. Наружу файл не
уходит: он приходит в личку тому, кто нажал.
"""

from __future__ import annotations

import csv
import io
from datetime import date
from typing import Any, Dict, List, Sequence, Tuple

# Столбцы заявки: что показываем и откуда берём. Порядок — как в бланке.
COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("№", "_index"),
    ("Фамилия", "surname"),
    ("Имя", "name"),
    ("Дата рождения", "birthday"),
    ("Роль", "role"),
    ("Команда", "team"),
)

SEPARATOR = ";"
BOM = "﻿"


def human_date(raw: Any) -> str:
    """«2001-09-22» → «22.09.2001». Непонятное отдаём как есть."""
    got = str(raw or "").strip()
    if len(got) == 10 and got[4] == "-" and got[7] == "-":
        year, month, day = got[:4], got[5:7], got[8:10]
        if year.isdigit() and month.isdigit() and day.isdigit():
            return f"{day}.{month}.{year}"
    return got


def rows(people: Sequence[Dict[str, Any]]) -> List[List[str]]:
    """Строки таблицы: заголовок и люди. Порядок — как пришли."""
    out = [[title for title, _ in COLUMNS]]
    for number, person in enumerate(people, start=1):
        line = []
        for _, key in COLUMNS:
            if key == "_index":
                line.append(str(number))
            elif key == "birthday":
                line.append(human_date(person.get("birthday")))
            else:
                line.append(str(person.get(key) or "").strip())
        out.append(line)
    return out


def csv_bytes(people: Sequence[Dict[str, Any]]) -> bytes:
    """Готовый файл. Пустой состав тоже отдаём — с одним заголовком."""
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=SEPARATOR, lineterminator="\r\n",
                        quoting=csv.QUOTE_MINIMAL)
    writer.writerows(rows(people))
    return (BOM + buf.getvalue()).encode("utf-8")


def file_name(what: str, today: date = None) -> str:
    """Имя файла: «Заявка — Второй состав — 24.08.2026.csv».

    Дата в имени не украшение: заявку подают несколько раз за сезон, и две
    версии в загрузках без даты не различить."""
    day = (today or date.today()).strftime("%d.%m.%Y")
    clean = " ".join(str(what or "").split()) or "команда"
    # Знаки, на которых спотыкаются файловые системы и Telegram.
    for bad in '\\/:*?"<>|':
        clean = clean.replace(bad, " ")
    return f"Заявка — {' '.join(clean.split())} — {day}.csv"


def team_people() -> List[Dict[str, Any]]:
    """Все из листа «Игроки» — по алфавиту, как в самом листе."""
    import coach_payments
    return coach_payments.players()


def group_people(group_id: int) -> List[Dict[str, Any]]:
    """Состав одной группы."""
    import player_groups
    return player_groups.members(group_id)


def missing_birthday(people: Sequence[Dict[str, Any]]) -> List[str]:
    """Кто без даты рождения — заявку с пустой клеткой не примут."""
    return [str(p.get("title") or f"{p.get('surname')} {p.get('name')}").strip()
            for p in people if not str(p.get("birthday") or "").strip()]
