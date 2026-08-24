#!/usr/bin/env python3
"""Разбор протокола матча из PDF, присланного тренером.

Зачем. За результатом следит монитор, но лига публикует счёт не всегда и не
сразу: 22.08.2026 матч с овертаймом попал в базу лишь через двое суток, а в чат
не ушло ничего. Протокол при этом был на руках у тренера в тот же вечер.

Что берём и чего НЕ берём. Берём то, что в отчёте написано отдельной строкой и
разбирается однозначно: команды, финальный счёт, счёт по четвертям, овертайм,
дату. Индивидуальную статистику НЕ берём: в отчёте её колонки слипаются
(«168/13 620/5 08/18»), и разделить их можно только гаданием. Гадание в цифрах
игрока хуже, чем их отсутствие: неверную статистику никто не перепроверит.

Читаем ТОЛЬКО первую страницу: заголовок с результатом всегда на ней.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# «90:85 (14:20,21:21,16:15,28:23) 1OT» — финальный счёт, четверти, овертаймы.
SCORE_RE = re.compile(
    r"^\s*(\d{1,3})\s*:\s*(\d{1,3})\s*\(([^)]*)\)\s*(\d*)\s*OT\s*$|"
    r"^\s*(\d{1,3})\s*:\s*(\d{1,3})\s*\(([^)]*)\)\s*$")
DATE_RE = re.compile(r"Дата\s*:\s*(\d{2}\.\d{2}\.\d{4})")
QUARTER_RE = re.compile(r"(\d{1,3})\s*:\s*(\d{1,3})")


class NotAvailable(RuntimeError):
    """Читать PDF нечем — библиотека не установлена."""


# Колонки таблицы игроков, в порядке отчёта. «З/В» — пары «забито/всего», они
# приходят одним фрагментом («8/13») и разбираются сами; остальные — числа.
STAT_COLUMNS = ("pts", "pct2", "pct3", "pct_fg", "pct_ft", "ast", "stl", "blk",
                "reb_def", "reb_off", "reb", "tur", "foul_on", "pf", "secs",
                "plus_minus", "eff")
PAIR_COLUMNS = ("fg2", "fg3", "fg", "ft")

# Насколько значение может отстоять от своей колонки. Числа выровнены вправо, и
# у однозначного левый край смещён относительно двузначного на пару пунктов —
# восьми хватает с запасом и не даёт перепутать соседние колонки (шаг ~15).
COLUMN_TOLERANCE = 8.0


def _fragments(data: bytes) -> List[tuple]:
    """[(y, x, текст)] со всей первой страницы.

    Координаты — единственный способ разобрать таблицу: пустые клетки в отчёте
    ПРОПУЩЕНЫ, а не занулены, и по порядку значений колонку не определить.
    В плоском тексте они к тому же слипаются («168/13 620/5»)."""
    reader = _reader(data)
    if not reader.pages:
        return []
    out: List[tuple] = []

    def visit(text, cm, tm, font, size):
        got = (text or "").strip()
        if got:
            out.append((round(tm[5], 1), round(tm[4], 1), got))

    reader.pages[0].extract_text(visitor_text=visit)
    return out


def _rows(parts: List[tuple]) -> Dict[float, List[tuple]]:
    """Фрагменты по строкам. Имя игрока лига печатает чуть выше цифр, поэтому
    близкие y считаем одной строкой."""
    rows: Dict[float, List[tuple]] = {}
    for y, x, t in parts:
        key = next((k for k in rows if abs(k - y) < 1.5), y)
        rows.setdefault(key, []).append((x, t))
    return rows


def _columns(rows: Dict[float, List[tuple]]) -> tuple:
    """Позиции колонок из строки «Итого»: она заполнена всегда и целиком.

    Возвращает (числовые колонки, колонки пар «забито/всего»). Пары считаем
    отдельно: они приходят одним фрагментом и в общий ряд не встают."""
    for items in rows.values():
        if not any(t == "Итого" for _, t in items):
            continue
        plain, pairs = [], []
        for x, t in items:
            if t == "Итого":
                continue
            if "/" in t:
                if all(part.isdigit() for part in t.split("/")):
                    pairs.append(x)
            elif t.replace(":", "").isdigit():
                plain.append(x)
        return sorted(plain), sorted(pairs)
    return [], []


def players(data: bytes) -> List[Dict[str, Any]]:
    """Строки игроков из протокола: номер, фамилия и вся статистика.

    Сверено с тем, что по этой же игре отдал API лиги: совпало у всех до
    единого, включая пропущенные нули. Значит отчёт можно разбирать, не
    дожидаясь, пока лига опубликует статистику."""
    parts = _fragments(data)
    if not parts:
        return []
    rows = _rows(parts)
    cols, pair_cols = _columns(rows)
    if len(cols) != len(STAT_COLUMNS) or len(pair_cols) != len(PAIR_COLUMNS):
        logger.info("Протокол PDF: колонок %d/%d, ждал %d/%d — не разбираю",
                    len(cols), len(pair_cols), len(STAT_COLUMNS), len(PAIR_COLUMNS))
        return []

    out: List[Dict[str, Any]] = []
    team = ""
    for y in sorted(rows, reverse=True):
        items = sorted(rows[y])
        # Команду запоминаем по заголовку «Команда А : …». Без неё строки двух
        # команд неразличимы: номера повторяются, и №0 одной затирает №0 другой.
        head = "".join(t for _, t in items)
        got_team = re.search(r"Команда\s*[АБAB]\s*:\s*(.+)", head)
        if got_team:
            team = got_team.group(1).strip()
            continue
        name = next((t for x, t in items if _is_name(t)), "")
        # У стартовой пятёрки номер приходит вместе со звёздочкой одним
        # фрагментом («* 11»), поэтому чистим и её, и пробелы.
        number = next((t for x, t in items
                       if x < 50 and t.replace("*", "").strip().isdigit()), "")
        values = [t for x, t in items if _is_value(t) and x > 150]
        # Строка игрока — это номер СЛЕВА, фамилия и цифры справа. Заголовки
        # («СТАТИСТИЧЕСКИЙ ОТЧЕТ») и названия команд тоже набраны прописными,
        # но ни номера, ни статистики у них нет.
        # Номера достаточно: у заголовков и строки тренера его нет. Порога по
        # числу цифр не ставим — у вышедшего на минуту их всего три, и он
        # выпадал из протокола целиком.
        if not name or not number or not values:
            continue
        got: Dict[str, Any] = {"number": number.replace("*", "").strip(),
                               "name": name, "team": team, "start": "*" in number}
        for x, t in items:
            if t == name or not _is_value(t):
                continue
            if "/" in t:
                made, _, total = t.partition("/")
                key = _pair_key(x, pair_cols)
                if key and made.isdigit() and total.isdigit():
                    got[key + "m"], got[key + "a"] = int(made), int(total)
                continue
            near = min(range(len(cols)), key=lambda i: abs(cols[i] - x))
            if abs(cols[near] - x) > COLUMN_TOLERANCE:
                continue
            got[STAT_COLUMNS[near]] = _value(t)
        out.append(got)
    return out


def _pair_key(x: float, pair_cols: List[float]) -> str:
    """Какая из четырёх пар «забито/всего» стоит в этой позиции.

    Позиции берём из «Итого», а не зашиваем числами: сменит лига вёрстку —
    зашитые координаты молча начнут врать, а вычисленные просто переедут."""
    near = min(range(len(pair_cols)), key=lambda i: abs(pair_cols[i] - x))
    if abs(pair_cols[near] - x) > COLUMN_TOLERANCE + 6:
        return ""
    return PAIR_COLUMNS[near]


def _is_name(text: str) -> bool:
    """Фамилия набрана прописными — так лига печатает игроков и только их."""
    letters = [c for c in text if c.isalpha()]
    return bool(letters) and sum(1 for c in letters if c.isupper()) >= 3 \
        and " " in text.strip() and "Итого" not in text


def _is_value(text: str) -> bool:
    return bool(re.fullmatch(r"[\d/:]+", text))


def _value(text: str) -> Any:
    if ":" in text:                      # сыгранное время «36:28» — в секунды
        mm, _, ss = text.partition(":")
        return int(mm) * 60 + int(ss) if mm.isdigit() and ss.isdigit() else 0
    return int(text) if text.isdigit() else 0


def _reader(data: bytes):
    try:
        from pypdf import PdfReader
    except ImportError:
        try:
            from PyPDF2 import PdfReader
        except ImportError as exc:
            raise NotAvailable(
                "На сервере нет библиотеки для чтения PDF (pypdf)") from exc
    import io
    return PdfReader(io.BytesIO(data))


def read_text(data: bytes) -> str:
    """Текст первой страницы. Без библиотеки — понятная ошибка, не заглушка."""
    reader = _reader(data)
    if not reader.pages:
        return ""
    return reader.pages[0].extract_text() or ""


def parse(data: bytes) -> Dict[str, Any]:
    """Что удалось понять из протокола.

    Пустой `score` означает «не разобрал» — публиковать по такому нельзя."""
    text = read_text(data)
    lines = [ln.rstrip() for ln in text.split("\n")]
    out: Dict[str, Any] = {"teams": [], "score": None, "quarters": [],
                           "overtimes": 0, "date": "", "raw_head": ""}

    for i, line in enumerate(lines):
        m = SCORE_RE.match(line)
        if not m:
            continue
        if m.group(1):
            home, guest, quarters, ot = m.group(1), m.group(2), m.group(3), m.group(4)
        else:
            home, guest, quarters, ot = m.group(5), m.group(6), m.group(7), ""
        out["score"] = (int(home), int(guest))
        out["quarters"] = [f"{a}:{b}" for a, b in QUARTER_RE.findall(quarters)]
        # «1OT» — один овертайм, «2OT» — два. Голое «OT» тоже считаем за один.
        out["overtimes"] = int(ot) if ot.isdigit() else (1 if ot == "" and "OT" in line else 0)
        # Названия команд — строкой выше, разделены городом и тире.
        if i:
            out["raw_head"] = lines[i - 1].strip()
            out["teams"] = _teams(lines[i - 1])
        break

    got = DATE_RE.search(text)
    if got:
        out["date"] = got.group(1)
    if not out["score"]:
        logger.info("Протокол PDF: строку со счётом не нашёл")
    return out


def _teams(line: str) -> List[str]:
    """«Кирпичный Завод Санкт-Петербург PULL UP Санкт-Петербург –» → две команды.

    Город лига пишет после названия и повторяет у обеих, поэтому режем по нему.
    Не сошлось — отдаём строку целиком: пусть тренер увидит её и поправит сам,
    это честнее, чем уверенно показать половину названия."""
    raw = re.sub(r"\s*–\s*$", "", line.strip())
    raw = raw.replace("–", "-").replace("—", "-")
    # Город в отчёте набран с разрядкой («Санкт -Петербург») — нормализуем.
    flat = re.sub(r"\s*-\s*", "-", raw)
    city = re.search(r"([А-ЯЁ][а-яё]+-[А-ЯЁ][а-яё]+)", flat)
    if city:
        parts = [p.strip(" ,") for p in flat.split(city.group(1)) if p.strip(" ,")]
        if len(parts) == 2:
            return [p.strip() for p in parts]
    return [raw.strip()] if raw.strip() else []


def box_score(rows: List[Dict[str, Any]], limit: int = 6) -> str:
    """Кратко о разобранных игроках — чтобы тренер сверил глазами.

    Показываем самых заметных, а не всех: в чате нужен итог, а не таблица на
    девятнадцать строк. Полная таблица и так у тренера в PDF."""
    if not rows:
        return ""
    best = sorted(rows, key=lambda r: -int(r.get("pts") or 0))[:limit]
    out = []
    for r in best:
        bits = [f"{r.get('pts', 0)} очк"]
        if r.get("reb"):
            bits.append(f"{r['reb']} подб")
        if r.get("ast"):
            bits.append(f"{r['ast']} пас")
        out.append(f"• {r['name']} — " + " · ".join(bits))
    return "\n".join(out)


def summary(got: Dict[str, Any]) -> str:
    """Человеческое изложение разобранного — для сверки перед публикацией."""
    if not got.get("score"):
        return ("Не нашёл в этом файле строку с результатом. Нужен "
                "статистический отчёт матча — тот, где вверху счёт и четверти.")
    home, guest = got["score"]
    teams = got.get("teams") or []
    names = " — ".join(teams) if len(teams) == 2 else (got.get("raw_head") or "команды")
    lines = [f"📄 Разобрал протокол:", "", f"{names}", f"Счёт: {home}:{guest}"]
    if got.get("quarters"):
        lines.append("По четвертям: " + ", ".join(got["quarters"]))
    if got.get("overtimes"):
        lines.append(f"Овертаймов: {got['overtimes']}")
    if got.get("date"):
        lines.append(f"Дата: {got['date']}")
    return "\n".join(lines)
