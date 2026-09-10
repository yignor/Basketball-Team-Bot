#!/usr/bin/env python3
"""Отчества для заявки: подтянуть из Инфобаскета тем, у кого их нет.

Бланк заявки в лигу просит ФИО целиком, а в листе «Игроки» отчества не было.
Вписывать его руками сорок раз незачем: Инфобаскет знает отчество каждого,
кто хоть раз был в нашей заявке (`PersonSecondNameRu`).

**Кого берём — только своих.** Перебираем заявку НАШЕЙ команды в Инфобаскете,
а не всех, кого бот видел в протоколах: отчество соперника незачем ни в листе,
ни где-либо ещё ([[legal-data-invariant]]).

**Сверка строгая.** Лишнее отчество в заявке хуже пустого места: его отправят
в лигу. Поэтому совпасть должны и фамилия, и имя, а если у обоих известна дата
рождения — то и она. Не сошлось хоть в чём-то — не ставим.

**Что уже вписано — не трогаем.** Тренер мог исправить отчество руками, и
перезаписать его ответом лиги значило бы стереть его правку.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Tuple

import sheets_cache

logger = logging.getLogger(__name__)


def _key(text: Any) -> str:
    return " ".join(str(text or "").lower().replace("ё", "е").split())


def _day(text: Any) -> str:
    """Дата рождения в одном виде: «22.09.2001» и «2001-09-22» → «2001-09-22»."""
    got = str(text or "").strip()[:10]
    if len(got) == 10 and got[2] == "." and got[5] == ".":
        return f"{got[6:10]}-{got[3:5]}-{got[0:2]}"
    return got


def match(people: List[Dict[str, Any]],
          league: List[Dict[str, str]]) -> Tuple[Dict[int, str], List[str]]:
    """Сопоставляет лист с ответами лиги. ({строка: отчество}, [кого не нашли]).

    Совпасть должны фамилия и имя; дата рождения — если известна у обоих.
    Однофамильцы с одним именем без даты — не решаем за тренера, пропускаем."""
    by_name: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    for item in league:
        if item.get("second"):
            by_name.setdefault((_key(item.get("last")), _key(item.get("first"))),
                               []).append(item)
    found: Dict[int, str] = {}
    missing: List[str] = []
    for p in people:
        if str(p.get("patronymic") or "").strip():
            continue                      # уже вписано — не трогаем
        cands = by_name.get((_key(p.get("surname")), _key(p.get("name"))), [])
        mine = _day(p.get("birthday"))
        if mine:
            dated = [c for c in cands if _day(c.get("birth"))]
            if dated:
                cands = [c for c in dated if _day(c.get("birth")) == mine]
        seconds = {c["second"] for c in cands}
        if len(seconds) == 1:
            found[int(p["row"])] = seconds.pop()
        else:
            missing.append(str(p.get("title") or ""))
    return found, missing


async def from_infobasket() -> List[Dict[str, str]]:
    """Отчества игроков НАШЕЙ заявки в Инфобаскете. Транзитно."""
    import stats_backfill
    with sheets_cache.get_connection() as conn:
        ids = [str(r["player_id"]) for r in conn.execute(
            """SELECT DISTINCT r.player_id FROM league_rosters r
                 JOIN league_teams t ON t.source = r.source AND t.team_id = r.team_id
                WHERE r.source = 'infobasket' AND t.ours = 1""")]
    out: List[Dict[str, str]] = []
    # По одному, с паузой: лига — чужой сервер, и сорок запросов разом — это
    # способ получить отказ на половине, а не ускорение.
    for pid in ids:
        info = await stats_backfill.fetch_infobasket_person_info(pid)
        if info.get("second"):
            out.append(info)
        await asyncio.sleep(0.3)
    return out


def write(spreadsheet, found: Dict[int, str]) -> Dict[str, int]:
    """Вписывает отчества в лист одной записью столбца. {written, skipped}.

    Одна запись, а не сорок: по ячейке — это сорок обращений к Google и риск
    упереться в лимит на середине. Столбец читаем целиком, меняем только пустые
    клетки из found и пишем обратно — уже вписанное остаётся как было."""
    import gspread.utils as gutils
    import coach_payments
    if spreadsheet is None or not found:
        return {"written": 0, "skipped": len(found)}
    coach_payments.ensure_player_columns(spreadsheet)
    ws = spreadsheet.worksheet(sheets_cache.PLAYERS_SHEET_NAME)
    values = ws.get_all_values()
    head = values[0] if values else []
    try:
        col = head.index(sheets_cache.PLAYERS_PATRONYMIC_HEADER)
        i_sur, i_name = head.index("Фамилия"), head.index("Имя")
    except ValueError:
        return {"written": 0, "skipped": len(found)}
    people = {int(p["row"]): p for p in coach_payments.players()}
    column, written = [], 0
    for idx, line in enumerate(values[1:], start=2):
        cur = line[col] if len(line) > col else ""
        new = cur
        want = found.get(idx)
        if want and not cur.strip():
            # Строка в листе могла уехать — сверяем, что это тот же человек.
            here = _key(" ".join([line[i_sur] if len(line) > i_sur else "",
                                  line[i_name] if len(line) > i_name else ""]))
            if here == _key((people.get(idx) or {}).get("title")):
                new = want
                written += 1
        column.append([new])
    if not written:
        return {"written": 0, "skipped": len(found)}
    letter = gutils.rowcol_to_a1(1, col + 1).rstrip("0123456789")
    ws.update(values=column, range_name=f"{letter}2:{letter}{len(values)}")
    with sheets_cache.get_connection() as conn:
        for row, value in found.items():
            conn.execute("UPDATE players SET patronymic = ? WHERE row_index = ? "
                         "AND TRIM(patronymic) = ''", (value, int(row)))
        conn.commit()
    # В журнал — числа, без имён: ФИО туда не пишем.
    logger.info("Отчества из Инфобаскета вписаны: %d", written)
    return {"written": written, "skipped": len(found) - written}
