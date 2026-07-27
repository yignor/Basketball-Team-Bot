#!/usr/bin/env python3
"""
Сводки посещаемости для листов «Тренировки» и «Игры».

Один модуль на оба отчёта: тренеру нужна одна и та же картина, а два похожих,
но разных куска кода неизбежно разъезжаются.

Что даёт:
  • сводку за период — кто сколько раз пришёл, пропустил, промолчал, менял
    мнение, и ОТДЕЛЬНО по каждому дню недели («ср 3/4» — был на трёх средах
    из четырёх). Среда и пятница считаются раздельно: ходить только по средам
    и ходить всегда — разные вещи, а общий процент их уравнивал;
  • накопительные своды: месяц, квартал, полугодие, год. Своды считаются
    из тех же голосов, но не повторяют их построчно.

Периоды берём только ЗАВЕРШЁННЫЕ (кроме текущего месяца — он обновляется по
ходу дела): незаконченный квартал в таблице менялся бы каждую неделю и сбивал
с толку.

ФИО берём из состава (лист «Игроки»), а не из Telegram: ник меняется, а в
отчёте тренер должен видеть человека.
"""

from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

DAYS_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
MONTHS_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
    7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}
MONTHS_GEN = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля", 5: "мая", 6: "июня",
    7: "июля", 8: "августа", 9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

# Колонка с процентом фиксирована (F) — на неё вешается цветовая шкала в
# Sheets. Колонки дней недели идут ПОСЛЕ неё, поэтому их переменное число
# ничего не ломает.
PCT_COLUMN_INDEX = 5

# Шкала рисуется формулой SPARKLINE в самой таблице (столбец сразу за
# процентом). Формула зависит от номера строки, а он известен только при записи
# листа, поэтому здесь ставим метку — её подменяет report_common.fill_sparklines.
SPARK_TOKEN = "{SPARK}"
BASE_HEADERS = ["Фамилия / Имя", "Ник", "Пришёл", "Пропустил",
                "Без ответа", "% посещений", "Шкала", "Менял мнение"]


# Дни недели узнаём из ТЕКСТА ответа, а не из даты опроса. Опрос выходит раз в
# неделю (в воскресенье) и предлагает варианты вроде «Среда», «Пятница»,
# «Среда и пятница» — все они одинаково считаются явкой, а какой это день,
# сохраняется только в тексте варианта. По дате опроса всё складывалось в один
# день недели, и «хожу только по средам» было не отличить от «хожу всегда».
DAY_STEMS = [("понедельник", 0), ("вторник", 1), ("сред", 2), ("четверг", 3),
             ("пятниц", 4), ("суббот", 5), ("воскресен", 6)]


def days_in_text(text: Optional[str]) -> set:
    low = (text or "").lower()
    return {wd for stem, wd in DAY_STEMS if stem in low}


def _vote_days(vote: Dict[str, Any], event_date: date) -> set:
    """Дни, к которым относится ответ. Если в тексте дней нет (игровой опрос,
    «Готов»/«Не готов») — это сам день события."""
    return days_in_text(vote.get("vote_text")) or {event_date.weekday()}


def _offered_days(votes: Sequence[Dict[str, Any]], event_date: date) -> set:
    """Какие дни вообще предлагал этот опрос — объединение дней из всех
    ответов. Это знаменатель: нельзя пропустить пятницу, которой не было."""
    days = set()
    for v in votes:
        days |= days_in_text(v.get("vote_text"))
    return days or {event_date.weekday()}


def aggregate(events: Sequence[Tuple[date, List[Dict[str, Any]]]],
              resolve) -> Dict[str, Dict[str, Any]]:
    """Считает по каждому человеку статистику за период.

    events — СПИСОК (дата, голоса): в один день бывает две игры и переопрос
    после переноса, а словарь по дате их схлопывал — знаменатель занижался и
    являлась «явка 200%». resolve(vote) -> (ФИО, ник, ключ).
    Отдельно копим явку по дням недели: это и есть ответ на вопрос
    «ходит только по средам или во все дни»."""
    out: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"nick": "", "key": "", "present": 0, "absent": 0, "voted": 0,
                 "revotes": 0, "by_weekday": defaultdict(int), "slots": set(),
                 "last": None})
    for idx, (d, votes) in enumerate(sorted(events, key=lambda x: x[0])):
        offered = _offered_days(votes, d)
        for v in votes:
            name, nick, key = resolve(v)
            p = out[name]
            if nick:
                p["nick"] = nick
            p["key"] = key
            p["revotes"] += int(v.get("revotes") or 0)
            if v.get("vote_type") == "PRESENT":
                # Считаем в ДНЯХ: «Среда и пятница» — это две тренировки, а не
                # одна. Иначе тот, кто ходит дважды в неделю, и тот, кто раз,
                # выглядят одинаково.
                chosen = _vote_days(v, d) & offered or _vote_days(v, d)
                # Считаем СЛОТЫ (событие + день), а не голоса: если человек
                # почему-то оказался в одном опросе дважды, явка не удвоится.
                for wd in chosen:
                    if (idx, wd) in p["slots"]:
                        continue
                    p["slots"].add((idx, wd))
                    p["present"] += 1
                    p["voted"] += 1
                    p["by_weekday"][wd] += 1
                if p["last"] is None or d > p["last"]:
                    p["last"] = d
            elif v.get("vote_type") == "ABSENT":
                for wd in offered:
                    if (idx, wd) in p["slots"]:
                        continue
                    p["slots"].add((idx, wd))
                    p["absent"] += 1
                    p["voted"] += 1
    return out


def summary_rows(title: str, events: Sequence[Tuple[date, List[Dict[str, Any]]]],
                 resolve, unit: str = "тренировок",
                 roster_total: int = 0) -> List[List[str]]:
    """Блок сводки за период: заголовок, строка итогов, таблица по людям."""
    events = sorted(events, key=lambda x: x[0])
    dates = [d for d, _ in events]
    if not dates:
        return []
    stats = aggregate(events, resolve)
    # Знаменатель для «3/4» — сколько РАЗ предлагался этот день недели.
    total_by_wd: Dict[int, int] = defaultdict(int)
    for d, votes in events:
        for wd in _offered_days(votes, d):
            total_by_wd[wd] += 1
    weekdays = sorted(total_by_wd)
    total_slots = sum(total_by_wd.values())    # всего тренировочных дней

    rows: List[List[str]] = [[f"═══ {title} ═══"]]

    # Явка на «событие» = сколько человеко-дней собрал этот опрос.
    present_per_event = []
    for d, votes in events:
        offered = _offered_days(votes, d)
        present_per_event.append(sum(
            len(_vote_days(v, d) & offered) or 0
            for v in votes if v.get("vote_type") == "PRESENT"))
    avg = round(sum(present_per_event) / len(present_per_event), 1) if present_per_event else 0
    best_i = max(range(len(dates)), key=lambda i: present_per_event[i]) if dates else None
    worst_i = min(range(len(dates)), key=lambda i: present_per_event[i]) if dates else None
    parts = [f"{unit.capitalize()}: {total_slots}"]
    # «опросов» показываем, только когда это не то же самое число: у игр опрос
    # = игра, и повтор выглядел бы как ошибка. У тренировок один опрос может
    # покрывать два дня — вот там разница осмысленна.
    if len(events) != total_slots:
        parts.append(f"опросов: {len(events)}")
    parts.append(f"средняя явка: {avg:g} чел.")
    if best_i is not None:
        d = dates[best_i]
        parts.append(f"лучшая: {DAYS_SHORT[d.weekday()]} {d.day} {MONTHS_GEN[d.month]}"
                     f" — {present_per_event[best_i]}")
    if worst_i is not None and worst_i != best_i:
        d = dates[worst_i]
        parts.append(f"слабее всех: {DAYS_SHORT[d.weekday()]} {d.day} {MONTHS_GEN[d.month]}"
                     f" — {present_per_event[worst_i]}")
    revotes_total = sum(p["revotes"] for p in stats.values())
    if revotes_total:
        parts.append(f"смен мнения: {revotes_total}")
    if roster_total:
        # Считаем по ключу человека (строка листа «Игроки»), а не по ФИО:
        # тёзки схлопывались бы, а лишний пробел в имени ломал сверку.
        answered = len({p["key"] for p in stats.values() if p["key"].startswith("row:")})
        if roster_total > answered:
            parts.append(f"ни разу не ответили: {roster_total - answered}")
    # Разбивка по дням недели: сколько всего и какая явка в среднем.
    wd_parts = []
    for wd in weekdays:
        came = sum(p["by_weekday"].get(wd, 0) for p in stats.values())
        wd_avg = round(came / total_by_wd[wd], 1) if total_by_wd[wd] else 0
        wd_parts.append(f"{DAYS_SHORT[wd]} ×{total_by_wd[wd]} (в среднем {wd_avg:g})")
    rows.append([" · ".join(parts)])
    rows.append(["По дням недели: " + " · ".join(wd_parts)])
    rows.append([""])

    rows.append(BASE_HEADERS + [DAYS_SHORT[wd] for wd in weekdays] + ["Последняя"])
    for name, p in sorted(stats.items(), key=lambda x: (-x[1]["present"], x[0])):
        no_answer = max(0, total_slots - p["voted"])
        pct = f"{round(p['present'] / total_slots * 100)}%" if total_slots else ""
        row = [name, f"@{p['nick']}" if p["nick"] else "",
               str(p["present"]), str(p["absent"]),
               str(no_answer) if no_answer else "",
               pct, SPARK_TOKEN, str(p["revotes"]) if p["revotes"] else ""]
        for wd in weekdays:
            # «4 из 4», а НЕ «4/4»: Sheets принимает «4/4» за дату и показывает
            # «4 апреля». Выдавало себя то, что «0/4» оставалось текстом —
            # нулевого месяца не бывает.
            row.append(f"{p['by_weekday'].get(wd, 0)} из {total_by_wd[wd]}")
        last = p["last"]
        row.append(f"{last.day} {MONTHS_GEN[last.month]}" if last else "—")
        rows.append(row)
    rows.append([""])
    return rows


# ─────────────────────────── Периоды ─────────────────────────────────────────

def _period_key(d: date, kind: str) -> Tuple:
    if kind == "month":
        return (d.year, d.month)
    if kind == "quarter":
        return (d.year, (d.month - 1) // 3 + 1)
    if kind == "half":
        return (d.year, 1 if d.month <= 6 else 2)
    return (d.year,)


def _period_title(key: Tuple, kind: str) -> str:
    if kind == "month":
        return f"{MONTHS_RU[key[1]].upper()} {key[0]}"
    if kind == "quarter":
        return f"{key[1]} КВАРТАЛ {key[0]}"
    if kind == "half":
        return f"{'ПЕРВОЕ' if key[1] == 1 else 'ВТОРОЕ'} ПОЛУГОДИЕ {key[0]}"
    return f"ГОД {key[0]}"


def _period_finished(key: Tuple, kind: str, today: date) -> bool:
    """Свод показываем только за завершённый период — кроме текущего месяца,
    который тренер смотрит по ходу дела."""
    if kind == "month":
        return True
    if kind == "quarter":
        return (key[0], key[1]) < (today.year, (today.month - 1) // 3 + 1)
    if kind == "half":
        return (key[0], key[1]) < (today.year, 1 if today.month <= 6 else 2)
    return key[0] < today.year


def build_sections(events: Sequence[Tuple[date, List[Dict[str, Any]]]], resolve,
                   unit: str = "тренировок",
                   roster_total: int = 0,
                   today: Optional[date] = None) -> List[List[str]]:
    """Все сводки: месяцы (свежие сверху), затем кварталы, полугодия, годы.

    Своды не повторяют голоса построчно — только агрегаты, поэтому лист не
    распухает от дублей."""
    today = today or date.today()
    out: List[List[str]] = []
    for kind, caption in (("month", "СВОДКИ ПО МЕСЯЦАМ"),
                          ("quarter", "СВОДКИ ПО КВАРТАЛАМ"),
                          ("half", "СВОДКИ ПО ПОЛУГОДИЯМ"),
                          ("year", "СВОДКИ ПО ГОДАМ")):
        buckets: Dict[Tuple, List[Tuple[date, List[Dict[str, Any]]]]] = defaultdict(list)
        for d, votes in events:
            buckets[_period_key(d, kind)].append((d, votes))
        keys = [k for k in buckets if _period_finished(k, kind, today)]
        if not keys:
            continue
        block: List[List[str]] = [[caption], [""]]
        wrote = False
        for key in sorted(keys, reverse=True):
            rows = summary_rows(_period_title(key, kind), buckets[key], resolve,
                                unit=unit, roster_total=roster_total)
            if rows:
                block.extend(rows)
                wrote = True
        if wrote:
            out.extend(block)
    return out
