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


def read_text(data: bytes) -> str:
    """Текст первой страницы. Без библиотеки — понятная ошибка, не заглушка."""
    try:
        from pypdf import PdfReader           # предпочтительный, поддерживается
    except ImportError:
        try:
            from PyPDF2 import PdfReader      # старое имя того же проекта
        except ImportError as exc:
            raise NotAvailable(
                "На сервере нет библиотеки для чтения PDF (pypdf)") from exc
    import io
    reader = PdfReader(io.BytesIO(data))
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
