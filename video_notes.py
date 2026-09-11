#!/usr/bin/env python3
"""Заметки тренера по записи игры: тайм-код, комментарий, кому это.

Зачем. Тренер пересматривает игру и видит: вот тут Иванов не закрыл, а тут
команда хорошо отработала заслон. Раньше это жило в его голове или в блокноте,
и до игроков доходило пересказом «помнишь, во второй четверти…». Здесь каждая
мысль привязана к секунде записи, и из заметок собирается разбор со ссылками:
нажал — и видео открылось ровно на этом месте.

**Время — по записи, а не по табло.** Тренер смотрит видео в ВК и видит там
«12:34» — это и вводит. Переводить в игровое время незачем: ссылка ведёт в
запись, а привязка по протоколу бывает неточной (22.08 лига испортила разметку
целого периода). Секунда, которую тренер видел своими глазами, — точнее.

**Кому.** Заметка либо про конкретного игрока, либо про команду целиком. Разбор
собирается и для одного человека (только его моменты плюс общие), и для всех.

**Отправка — только тренеру.** Бот присылает готовый разбор ему, а переслать
он решает сам: игроку лично, в чат, куда угодно. Так решено 11.09.2026, и это
же правило раздела тренера — разборы в общий чат сами не уходят.

Голос не храним: из него получается текст, файл удаляется сразу. Хранить
запись голоса тренера о своих игроках незачем.
"""

from __future__ import annotations

import html
import re
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

SCHEMA = """
CREATE TABLE IF NOT EXISTS video_notes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    source     TEXT NOT NULL,
    game_id    TEXT NOT NULL,
    at_sec     INTEGER NOT NULL,
    text       TEXT NOT NULL,
    player_row INTEGER NOT NULL DEFAULT 0,   -- 0 — про команду целиком
    author_id  TEXT NOT NULL DEFAULT '',
    by_voice   INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS video_notes_game ON video_notes (source, game_id);
"""

# Сколько знаков в одном сообщении разбора. Телеграм режет на 4096, а ссылки
# в разметке считаются целиком — оставляем запас.
PAGE_BUDGET = 3600

_ready = False


def init() -> None:
    global _ready
    if _ready:
        return
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.executescript(SCHEMA)
        conn.commit()
    _ready = True


# ─────────────────────────── тайм-код ───────────────────────────

_TIME = re.compile(r"^\s*(\d{1,2})[:.\s](\d{2})(?:[:.\s](\d{2}))?(?=\s|$|[,—–-])")


def parse_time(text: str) -> Optional[int]:
    """«12:34», «1:02:15», «12.34» → секунды. Не время — None.

    Минуты и секунды проверяем: «12:75» — опечатка, а не время, и молча
    превратить её в 13:15 значило бы увести ссылку не туда."""
    got = split_time(text)
    return got[0] if got else None


def split_time(text: str) -> Optional[Tuple[int, str]]:
    """Время в начале строки и остаток: «12:34 не закрыл» → (754, «не закрыл»).

    Так тренер может прислать всё одним сообщением, не дожидаясь вопроса."""
    m = _TIME.match(str(text or ""))
    if not m:
        return None
    a, b, c = m.group(1), m.group(2), m.group(3)
    if c is not None:
        hours, minutes, seconds = int(a), int(b), int(c)
    else:
        hours, minutes, seconds = 0, int(a), int(b)
    if minutes > 59 or seconds > 59:
        return None
    rest = str(text)[m.end():].strip().lstrip(",—–-").strip()
    return hours * 3600 + minutes * 60 + seconds, rest


def human_time(seconds: int) -> str:
    """754 → «12:34», 3735 → «1:02:15» — как в плеере ВК."""
    seconds = max(0, int(seconds))
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


# ─────────────────────────── заметки ───────────────────────────


def add(source: str, game_id: str, at_sec: int, text: str,
        player_row: int = 0, author_id: Any = "", by_voice: bool = False) -> int:
    init()
    body = str(text or "").strip()
    if not body:
        raise ValueError("пустая заметка")
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO video_notes (source, game_id, at_sec, text, player_row, "
            "author_id, by_voice, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (str(source), str(game_id), int(at_sec), body, int(player_row),
             str(author_id), 1 if by_voice else 0, sheets_cache.now_iso()))
        conn.commit()
        return int(cur.lastrowid)


def update(note_id: int, **fields: Any) -> None:
    init()
    allowed = {"text", "at_sec", "player_row"}
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets:
        return
    names = ", ".join(f"{k} = ?" for k in sets)
    with sheets_cache.get_connection() as conn:
        conn.execute(f"UPDATE video_notes SET {names} WHERE id = ?",
                     (*sets.values(), int(note_id)))
        conn.commit()


def delete(note_id: int) -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM video_notes WHERE id = ?", (int(note_id),))
        conn.commit()


def get(note_id: int) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM video_notes WHERE id = ?",
                           (int(note_id),)).fetchone()
    return dict(row) if row else None


def of_game(source: str, game_id: str) -> List[Dict[str, Any]]:
    """Заметки игры по порядку записи — так их и смотрят."""
    init()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            "SELECT * FROM video_notes WHERE source = ? AND game_id = ? "
            "ORDER BY at_sec, id", (str(source), str(game_id)))]


def counts() -> Dict[Tuple[str, str], int]:
    """{(источник, игра): заметок} — для списка игр."""
    init()
    with sheets_cache.get_connection() as conn:
        return {(r["source"], str(r["game_id"])): int(r["n"]) for r in conn.execute(
            "SELECT source, game_id, COUNT(*) AS n FROM video_notes "
            "GROUP BY source, game_id")}


# ─────────────────────────── игры с записью ───────────────────────────


def games_with_video(limit: int = 12) -> List[Dict[str, Any]]:
    """Наши игры, у которых есть запись в ВК, свежие сначала."""
    import game_timeline
    init()
    with sheets_cache.get_connection() as conn:
        rows = game_timeline.our_games(conn, limit=limit)
        links = {(r["source"], str(r["game_id"])): r["video_vk"] for r in conn.execute(
            "SELECT source, game_id, video_vk FROM game_meta WHERE video_vk != ''")}
    for r in rows:
        r["video"] = links.get((r["source"], str(r["game_id"])), "")
    return [r for r in rows if r["video"]]


def game_of(source: str, game_id: str) -> Optional[Dict[str, Any]]:
    for g in games_with_video(limit=200):
        if g["source"] == source and str(g["game_id"]) == str(game_id):
            return g
    return None


def game_title(game: Dict[str, Any]) -> str:
    day = str(game.get("game_date") or "")[:10]
    if len(day) == 10 and day[4] == "-":
        day = f"{day[8:10]}.{day[5:7]}"
    return f"{day} · {game.get('home_name', '')} — {game.get('guest_name', '')}".strip()


# ─────────────────────────── разбор ───────────────────────────


def link(video: str, at_sec: int) -> str:
    import game_timeline
    return game_timeline.vk_link(video, int(at_sec))


def breakdown(source: str, game_id: str,
              player_row: Optional[int] = None) -> List[str]:
    """Разбор игры страницами HTML — готово, чтобы переслать.

    player_row None — для всей команды, по порядку записи, с пометкой, про кого
    каждая заметка. Номер строки — для одного человека: его моменты и общие для
    команды, без чужих, — чужие разборы ему ни к чему."""
    import coach_payments
    game = game_of(source, game_id) or {}
    video = str(game.get("video") or "")
    notes = of_game(source, game_id)
    if player_row is not None:
        notes = [n for n in notes if int(n["player_row"]) in (0, int(player_row))]
    names = {int(p["row"]): p["title"] for p in coach_payments.players()}

    head = [f"🎬 <b>Разбор игры</b> · {html.escape(game_title(game))}"]
    if player_row:
        head.append(f"Для: {html.escape(names.get(int(player_row), 'игрока'))}")
    if video:
        head.append(f'<a href="{html.escape(video)}">Запись целиком</a>')
    head.append("")

    lines: List[str] = []
    for n in notes:
        when = human_time(int(n["at_sec"]))
        stamp = (f'<a href="{html.escape(link(video, n["at_sec"]))}">{when}</a>'
                 if video else when)
        who = ""
        if player_row is None:
            row = int(n["player_row"])
            who = (f" · <b>{html.escape(names.get(row, 'игрок'))}</b>" if row
                   else " · <b>команда</b>")
        lines.append(f"▶️ {stamp}{who}\n{html.escape(str(n['text']))}")
    if not lines:
        return ["\n".join(head + ["Заметок пока нет."])]

    pages: List[str] = []
    cur = list(head)
    size = sum(len(x) + 1 for x in cur)
    for line in lines:
        if size + len(line) + 2 > PAGE_BUDGET and len(cur) > len(head):
            pages.append("\n".join(cur))
            cur, size = [], 0
        cur.append(line)
        cur.append("")
        size += len(line) + 2
    pages.append("\n".join(cur).rstrip())
    return pages
