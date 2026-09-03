#!/usr/bin/env python3
"""Ачивки: значки, которые человек получает и показывает рядом со своим именем.

Зачем. Турнирная таблица — это столбик цифр, и по ней не видно ничего, кроме
места. Значок «бетатестер» рядом с именем говорит то, чего в цифрах нет:
человек был здесь с самого начала. Это украшение, и относиться к нему надо как
к украшению — оно не должно ничего решать в подсчётах.

**Кому принадлежит.** Значок выдаётся telegram-пользователю, а не строке листа
«Игроки»: в фэнтези играют по своему id, кабинет тоже у каждого свой, и часть
участников в лист не входит вовсе.

**Две дороги к значку.** У ачивки либо правило, которое бот считает сам
(участвовал в фэнтези, сыграл столько-то игр), либо ручная выдача — списком
людей. Одноразовое «Игрок сезона» правилом не выразить, а бетатестеров руками
отмечать глупо, поэтому нужны обе.

**Показывать или нет — решает человек.** Выданное не отбирается, но в таблице
видно только то, что он сам оставил видимым, и не больше SHOWN_LIMIT штук:
иначе строка зачёта превращается в ленту значков и перестаёт читаться.

**Картинки лежат в базе.** Не в репозитории: он публичный, и заливать туда
файлы ради каждой новой ачивки — это деплой вместо кнопки. База уходит в
ночной бэкап, значит, картинки тоже.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# Сколько значков человек может показывать разом. Больше — и таблица зачёта
# превращается в ленту иконок, по которой уже не найти своё место.
SHOWN_LIMIT = 3

# Картинка значка. Больше мегабайта — это не иконка 64×64, а чья-то ошибка:
# такой файл будет грузиться в таблице у каждого и на мобильном интернете.
MAX_IMAGE_BYTES = 1024 * 1024
IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp"}

# До какого размера ужимаем перед тем, как класть в базу. В таблице значок
# показывается 18 точками, в окне — 160; с запасом на плотные экраны хватает
# 256. Присланная админом фотография на 225 КБ после этого весит десятки.
STORE_SIDE = 256

# Правила, которые бот умеет считать сам. Ключ → (подпись, нужен ли аргумент).
RULES: Dict[str, Tuple[str, bool]] = {
    "": ("Только вручную", False),
    "fantasy": ("Участвовал в фэнтези", True),
    "games": ("Сыграл игр (не меньше)", True),
    "place": ("Место в зачёте фэнтези", True),
    "rank": ("Лучший в ранге", True),
}

# Что значит аргумент у правила — от этого зависит и подсказка, и разбор.
RULE_ARG_KIND = {"fantasy": "date", "games": "number",
                 "place": "number", "rank": "rank"}

# Правила, которым нужен сезон. Значок за место даётся ЗА КОНКРЕТНЫЙ сезон:
# летний кубок 2026 и сезон 26/27 — разные награды с разными картинками, а не
# одна, которая переезжает к новому чемпиону и пропадает у старого.
NEEDS_SEASON = {"place", "rank"}

PLACE_TITLES = {1: "Первое место", 2: "Второе место", 3: "Третье место"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS achievements (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    season_id   INTEGER NOT NULL DEFAULT 0,
    title       TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    emoji       TEXT NOT NULL DEFAULT '',
    rule        TEXT NOT NULL DEFAULT '',
    rule_arg    TEXT NOT NULL DEFAULT '',
    image       BLOB,
    image_type  TEXT NOT NULL DEFAULT '',
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS achievement_awards (
    ach_id     INTEGER NOT NULL,
    user_id    TEXT NOT NULL,
    awarded_at TEXT NOT NULL,
    by_rule    INTEGER NOT NULL DEFAULT 0,
    shown      INTEGER NOT NULL DEFAULT 1,
    told       INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (ach_id, user_id)
);
CREATE INDEX IF NOT EXISTS achievement_awards_user
    ON achievement_awards (user_id);
"""

_ready = False


def init() -> None:
    """Идемпотентно создаёт свои таблицы."""
    global _ready
    if _ready:
        return
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.executescript(SCHEMA)
        # CREATE TABLE IF NOT EXISTS не добавит колонку в уже созданную
        # таблицу: у тех, кто успел завести ачивки до этой правки, её нет.
        have = [r[1] for r in conn.execute("PRAGMA table_info(achievement_awards)")]
        if "told" not in have:
            conn.execute("ALTER TABLE achievement_awards "
                         "ADD COLUMN told INTEGER NOT NULL DEFAULT 0")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(achievements)")]
        if "season_id" not in cols:
            conn.execute("ALTER TABLE achievements "
                         "ADD COLUMN season_id INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    _ready = True


def _now() -> str:
    return sheets_cache.now_iso()


def parse_day(text: str) -> str:
    """«25.08.2026» или «2026-08-25» → ISO. Не разобрали — пусто."""
    got = str(text or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d.%m.%y"):
        try:
            return datetime.strptime(got, fmt).date().isoformat()
        except ValueError:
            continue
    return ""


def human_day(iso: str) -> str:
    """ISO → «25.08.2026». Не дата — отдаём как есть."""
    got = str(iso or "").strip()
    try:
        return datetime.strptime(got, "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return got


def season_name(season_id: Any) -> str:
    """Название сезона. Нет такого — «сезон N», чтобы не показывать пустоту."""
    try:
        sid = int(season_id or 0)
    except (TypeError, ValueError):
        return ""
    if not sid:
        return ""
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT name FROM fantasy_seasons WHERE id = ?",
                           (sid,)).fetchone()
    return (str(row["name"]) if row and row["name"] else f"сезон {sid}")


def rule_title(rule: str, arg: str = "", season_id: Any = 0) -> str:
    """Человеческая подпись правила: «Сыграл игр (не меньше): 10»."""
    key = str(rule or "")
    title, needs = RULES.get(key, ("Своё правило", False))
    if key == "place":
        try:
            title = PLACE_TITLES.get(int(arg or 0), title)
        except ValueError:
            pass
    elif key == "rank" and arg:
        title = f"Лучший в ранге «{arg}»"
    elif needs and arg:
        if RULE_ARG_KIND.get(key) == "date":
            title = f"{title} по {human_day(arg)}"
        else:
            title = f"{title}: {arg}"
    if key in NEEDS_SEASON:
        where = season_name(season_id)
        title += f" — {where}" if where else " — сезон не выбран"
    return title


# ─────────────────────────── ачивки ───────────────────────────


def _row(conn, ach_id: int) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM achievements WHERE id = ?",
                        (int(ach_id),)).fetchone()


def _shape(row: sqlite3.Row, holders: int = 0) -> Dict[str, Any]:
    """Ачивка без самой картинки: её возят отдельным запросом."""
    got = {k: row[k] for k in row.keys() if k != "image"}
    got["has_image"] = bool(row["image"])
    got["holders"] = holders
    keys = row.keys()
    got["season_id"] = int(row["season_id"]) if "season_id" in keys else 0
    got["rule_title"] = rule_title(row["rule"], row["rule_arg"], got["season_id"])
    return got


def all_achievements(active_only: bool = False) -> List[Dict[str, Any]]:
    init()
    where = " WHERE active = 1" if active_only else ""
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM achievements" + where + " ORDER BY id").fetchall()
        counts = {int(r["ach_id"]): int(r["n"]) for r in conn.execute(
            "SELECT ach_id, COUNT(*) AS n FROM achievement_awards GROUP BY ach_id")}
    return [_shape(r, counts.get(int(r["id"]), 0)) for r in rows]


def get(ach_id: int) -> Optional[Dict[str, Any]]:
    init()
    with sheets_cache.get_connection() as conn:
        row = _row(conn, ach_id)
        if not row:
            return None
        holders = int(conn.execute(
            "SELECT COUNT(*) FROM achievement_awards WHERE ach_id = ?",
            (int(ach_id),)).fetchone()[0])
    return _shape(row, holders)


def create(title: str, description: str = "", emoji: str = "",
           rule: str = "", rule_arg: str = "") -> Tuple[Optional[int], str]:
    """Заводит ачивку. (id, объяснение)."""
    init()
    name = " ".join(str(title or "").split())
    if not name:
        return None, "У ачивки должно быть название."
    if str(rule or "") not in RULES:
        return None, "Такого правила нет."
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT INTO achievements (title, description, emoji, rule, "
            "rule_arg, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (name, str(description or "").strip(), str(emoji or "").strip(),
             str(rule or ""), str(rule_arg or ""), _now()))
        conn.commit()
        return int(cur.lastrowid), f"Ачивка «{name}» создана."


def update(ach_id: int, **fields: Any) -> None:
    """Правит поля ачивки. Картинку — через set_image."""
    init()
    allowed = {"title", "description", "emoji", "rule", "rule_arg", "active",
               "season_id"}
    sets = {k: v for k, v in fields.items() if k in allowed}
    if not sets:
        return
    names = ", ".join(f"{k} = ?" for k in sets)
    with sheets_cache.get_connection() as conn:
        conn.execute(f"UPDATE achievements SET {names} WHERE id = ?",
                     (*sets.values(), int(ach_id)))
        conn.commit()


def delete(ach_id: int) -> None:
    """Удаляет ачивку вместе с выдачами: висеть у людей она не должна."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM achievement_awards WHERE ach_id = ?",
                     (int(ach_id),))
        conn.execute("DELETE FROM achievements WHERE id = ?", (int(ach_id),))
        conn.commit()


def shrink(data: bytes, kind: str, side: int = STORE_SIDE) -> Tuple[bytes, str, str]:
    """Ужимает картинку до значка. (данные, тип, что сказать человеку).

    Прозрачность решает формат: с ней — PNG, без неё — JPEG, он заметно легче
    на фотографии. Библиотеки нет или картинка не открылась — кладём как
    прислали: значок важнее аккуратности, а тяжесть видна и так."""
    try:
        from PIL import Image
    except ImportError:
        return data, kind, ("Ужать не могу — на сервере нет Pillow, "
                            "картинка легла как есть.")
    import io
    try:
        img = Image.open(io.BytesIO(data))
        img.load()
    except Exception as exc:
        logger.warning("Картинка значка не открылась: %s", exc)
        return data, kind, "Не смог разобрать картинку — положил как прислали."

    was = len(data)
    if img.width > side or img.height > side:
        img.thumbnail((side, side), Image.LANCZOS)
    buf = io.BytesIO()
    clear = img.mode in ("RGBA", "LA") or "transparency" in img.info
    if clear:
        img.convert("RGBA").save(buf, format="PNG", optimize=True)
        out_kind = "image/png"
    else:
        img.convert("RGB").save(buf, format="JPEG", quality=85, optimize=True)
        out_kind = "image/jpeg"
    small = buf.getvalue()
    if len(small) >= was:
        # Ужимать нечего: прислали уже готовый значок. Не портим его лишним
        # пережатием — оно только добавит артефактов.
        return data, kind, ""
    return small, out_kind, (f"Ужал: было {was // 1024} КБ, стало "
                             f"{max(1, len(small) // 1024)} КБ.")


def set_image(ach_id: int, data: bytes, mime: str) -> Tuple[bool, str]:
    """Кладёт картинку в базу, ужав её до размера значка."""
    init()
    kind = str(mime or "").split(";")[0].strip().lower()
    if kind not in IMAGE_TYPES:
        return False, ("Нужна картинка PNG, JPEG или WebP — "
                       f"а это «{kind or 'неизвестно что'}».")
    if not data:
        return False, "Файл пустой."
    if len(data) > MAX_IMAGE_BYTES:
        return False, (f"Картинка тяжёлая: {len(data) // 1024} КБ. "
                       f"Предел {MAX_IMAGE_BYTES // 1024} КБ — значок грузится "
                       "у каждого в таблице.")
    data, kind, note = shrink(data, kind)
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE achievements SET image = ?, image_type = ? WHERE id = ?",
                     (sqlite3.Binary(data), kind, int(ach_id)))
        conn.commit()
    return True, ("Картинка на месте. " + note).strip()


def image(ach_id: int) -> Tuple[Optional[bytes], str]:
    """Картинка значка и её тип. Нет — (None, '')."""
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT image, image_type FROM achievements WHERE id = ?",
            (int(ach_id),)).fetchone()
    if not row or not row["image"]:
        return None, ""
    return bytes(row["image"]), str(row["image_type"] or "image/png")


def image_size(ach_id: int) -> int:
    """Сколько весит картинка значка, байт. Нет — ноль."""
    init()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT LENGTH(image) AS n FROM achievements WHERE id = ?",
                           (int(ach_id),)).fetchone()
    return int((row["n"] if row else 0) or 0)


def reshrink(ach_id: int) -> Tuple[bool, str]:
    """Ужимает уже лежащую в базе картинку.

    Нужно для значков, залитых до появления сжатия: перезаливать файл ради
    этого — лишняя работа человеку, который его уже прислал."""
    data, kind = image(ach_id)
    if not data:
        return False, "У этой ачивки нет картинки."
    small, out_kind, note = shrink(data, kind)
    if len(small) >= len(data):
        return False, "Ужимать нечего — картинка уже лёгкая."
    with sheets_cache.get_connection() as conn:
        conn.execute("UPDATE achievements SET image = ?, image_type = ? WHERE id = ?",
                     (sqlite3.Binary(small), out_kind, int(ach_id)))
        conn.commit()
    return True, note or "Готово."


# ─────────────────────────── выдача ───────────────────────────


def award(ach_id: int, user_id: Any, by_rule: bool = False) -> bool:
    """Выдаёт значок. True — если раньше его не было.

    Повторная выдача ничего не меняет и, главное, не сбрасывает выбор
    человека: он мог спрятать значок, и правило не должно возвращать его
    в таблицу за его спиной."""
    init()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute(
            "INSERT OR IGNORE INTO achievement_awards (ach_id, user_id, "
            "awarded_at, by_rule) VALUES (?, ?, ?, ?)",
            (int(ach_id), str(user_id), _now(), 1 if by_rule else 0))
        conn.commit()
        return cur.rowcount > 0


def revoke(ach_id: int, user_id: Any) -> None:
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "DELETE FROM achievement_awards WHERE ach_id = ? AND user_id = ?",
            (int(ach_id), str(user_id)))
        conn.commit()


def holders(ach_id: int) -> List[str]:
    init()
    with sheets_cache.get_connection() as conn:
        return [str(r["user_id"]) for r in conn.execute(
            "SELECT user_id FROM achievement_awards WHERE ach_id = ?",
            (int(ach_id),))]


def unannounced() -> List[Dict[str, Any]]:
    """Выданное, о чём человеку ещё не сказали.

    Отдельным списком, а не отправкой изнутри выдачи: слать умеет только
    демон, а выдавать — и админка, и почасовой пересчёт. Сведение в одно
    место заодно избавляет от двух писем об одном значке."""
    init()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT w.ach_id, w.user_id, a.title, a.description, a.emoji
                 FROM achievement_awards w JOIN achievements a ON a.id = w.ach_id
                WHERE w.told = 0 AND a.active = 1
                ORDER BY w.awarded_at""").fetchall()
    return [dict(r) for r in rows]


def mark_told(ach_id: int, user_id: Any) -> None:
    """Человеку сказали. Ставим отметку ДО отправки: упасть на середине
    списка и написать половине дважды хуже, чем не написать вовсе."""
    init()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "UPDATE achievement_awards SET told = 1 "
            "WHERE ach_id = ? AND user_id = ?", (int(ach_id), str(user_id)))
        conn.commit()


def hush(user_id: Any = None) -> int:
    """Помечает всё выданное как рассказанное, ничего не отправляя.

    Нужно на первом включении: значки, выданные до появления писем, не должны
    прилететь людям пачкой задним числом."""
    init()
    with sheets_cache.get_connection() as conn:
        if user_id is None:
            cur = conn.execute("UPDATE achievement_awards SET told = 1 WHERE told = 0")
        else:
            cur = conn.execute(
                "UPDATE achievement_awards SET told = 1 WHERE told = 0 AND user_id = ?",
                (str(user_id),))
        conn.commit()
        return cur.rowcount


def of_user(user_id: Any, shown_only: bool = False) -> List[Dict[str, Any]]:
    """Значки человека: сначала видимые, потом спрятанные."""
    init()
    sql = ("""SELECT a.*, w.shown, w.awarded_at AS got_at, w.by_rule
                FROM achievement_awards w JOIN achievements a ON a.id = w.ach_id
               WHERE w.user_id = ? AND a.active = 1""")
    if shown_only:
        sql += " AND w.shown = 1"
    sql += " ORDER BY w.shown DESC, w.awarded_at"
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(sql, (str(user_id),)).fetchall()
    out = []
    for r in rows:
        got = _shape(r)
        got["shown"] = bool(r["shown"])
        got["got_at"] = r["got_at"]
        out.append(got)
    return out[:SHOWN_LIMIT] if shown_only else out


def shown_map(user_ids: Sequence[Any]) -> Dict[str, List[Dict[str, Any]]]:
    """Видимые значки сразу для многих — чтобы таблица зачёта не делала
    по запросу на строку."""
    init()
    ids = [str(u) for u in user_ids if str(u or "").strip()]
    if not ids:
        return {}
    marks = ",".join("?" * len(ids))
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            f"""SELECT w.user_id, a.id, a.title, a.description, a.emoji,
                       a.image IS NOT NULL AS has_image
                  FROM achievement_awards w
                  JOIN achievements a ON a.id = w.ach_id
                 WHERE w.shown = 1 AND a.active = 1 AND w.user_id IN ({marks})
                 ORDER BY w.awarded_at""", ids).fetchall()
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        mine = out.setdefault(str(r["user_id"]), [])
        if len(mine) >= SHOWN_LIMIT:
            continue
        mine.append({"id": int(r["id"]), "title": r["title"],
                     "description": r["description"], "emoji": r["emoji"],
                     "has_image": bool(r["has_image"]),
                     # Владельца кладём рядом: по нажатию фронт спрашивает
                     # итог сезона именно этого человека, а не вообще.
                     "owner": str(r["user_id"])})
    return out


def set_shown(user_id: Any, ach_ids: Sequence[Any]) -> Tuple[bool, str]:
    """Человек выбрал, что показывать. Лишнее молча не отрезаем."""
    init()
    wanted = {int(a) for a in ach_ids}
    if len(wanted) > SHOWN_LIMIT:
        return False, f"Показывать можно не больше {SHOWN_LIMIT} значков."
    with sheets_cache.get_connection() as conn:
        mine = {int(r["ach_id"]) for r in conn.execute(
            "SELECT ach_id FROM achievement_awards WHERE user_id = ?",
            (str(user_id),))}
        unknown = wanted - mine
        if unknown:
            return False, "Такого значка у тебя нет."
        conn.execute("UPDATE achievement_awards SET shown = 0 WHERE user_id = ?",
                     (str(user_id),))
        for ach_id in wanted:
            conn.execute(
                "UPDATE achievement_awards SET shown = 1 "
                "WHERE user_id = ? AND ach_id = ?", (str(user_id), ach_id))
        conn.commit()
    return True, "Готово."


# ─────────────────────────── правила ───────────────────────────


def _fantasy_users(until: str = "") -> List[str]:
    """Кто хоть раз собирал состав в фэнтези. until — «по этот день включительно».

    Достаточно ОДНОГО состава: значок за участие, а не за постоянство. Границу
    считаем включительно — «по сегодняшний день» значит и сегодня тоже."""
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT user_id, MIN(updated_at) AS first_seen
                 FROM fantasy_rosters WHERE user_id != '' GROUP BY user_id""")
        out = []
        for r in rows:
            if until and str(r["first_seen"] or "")[:10] > until:
                continue
            out.append(str(r["user_id"]))
    return out


def _games_played(user_id: Any) -> int:
    """Сколько игр человек отыграл по протоколам лиг.

    Считаем по всем его лигам сразу: один и тот же человек играет и в
    Инфобаскете, и в SLPRO, и делить это по источникам незачем."""
    import player_identity
    total = 0
    with sheets_cache.get_connection() as conn:
        for ident in player_identity.get_identities(user_id):
            row = conn.execute(
                """SELECT COUNT(DISTINCT game_id) AS n FROM game_player_stats
                    WHERE source = ? AND player_id = ?""",
                (str(ident["source"]), str(ident["player_id"]))).fetchone()
            total += int(row["n"] or 0)
    return total


def _row_of_user(user_id: Any) -> int:
    """Строка листа «Игроки» этого telegram-человека. Нет привязки — 0."""
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT player_row FROM player_links WHERE tg_user_id = ? LIMIT 1",
            (str(user_id),)).fetchone()
    return int(row["player_row"]) if row and row["player_row"] else 0


def _season(season_id: Any) -> Optional[Dict[str, Any]]:
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM fantasy_seasons WHERE id = ?",
                           (int(season_id or 0),)).fetchone()
    return dict(row) if row else None


def _place_winners(season_id: Any, place: int) -> List[Tuple[str, str, int]]:
    """Кто занял это место. [(user_id, режим, очки)].

    По одному на КАЖДЫЙ включённый режим: режимы играют по разным правилам, и
    чемпион у каждого свой. Сводить их в одну таблицу нельзя — она сравнивала
    бы несравнимое, а объявлять чемпионом только одного из двух не за что."""
    import fantasy
    import fantasy_modes
    season = _season(season_id)
    if not season or place < 1:
        return []
    out = []
    for mode in fantasy_modes.enabled(season):
        rows = fantasy.season_standings_live(int(season_id), mode=mode)
        if len(rows) < place:
            continue
        row = rows[place - 1]
        title = fantasy_modes.MODE_TITLES.get(mode, mode)
        out.append((str(row["user_id"]), title, int(row.get("points") or 0)))
    return out


def _rank_leader(season_id: Any, rank: str) -> Tuple[str, float]:
    """Лучший по очкам сезона среди игроков этого ранга. (user_id, очки).

    Ранг берём из листа «Игроки» — его ведёт тренер, и он же показывается на
    карточке. Считаем только тех, кто привязан к telegram: значок надо кому-то
    вручить, а игроку соперника его не отдать."""
    import fantasy
    import fantasy_stats
    import player_identity
    season = _season(season_id)
    if not season or not rank:
        return "", 0.0
    weights = fantasy.season_weights(season)
    scopes = fantasy.effective_scopes(season)
    agg = fantasy_stats.player_aggregates(weights, scope=scopes)
    by_row = {int(v["row"]): v for v in sheets_cache.get_player_prices().values()}
    best, best_fp = "", -1.0
    for uid in player_identity.linked_users():
        info = by_row.get(_row_of_user(uid))
        if not info or str(info.get("tier") or "").strip() != rank:
            continue
        points = 0.0
        for ident in player_identity.get_identities(uid):
            key = f"{ident['source']}:{ident['player_id']}"
            points += float((agg.get(key) or {}).get("fp") or 0)
        if points > best_fp:
            best, best_fp = str(uid), points
    return best, max(0.0, best_fp)


def season_summary(ach_id: int, user_id: Any) -> Dict[str, Any]:
    """Краткий итог сезона, за который человеку дали этот значок.

    Показывается при нажатии на значок — любому, кто на него нажал. Значок без
    этого объяснения выглядит как наклейка: «первое место» само по себе не
    говорит ни за какой сезон, ни с каким результатом."""
    init()
    ach = get(ach_id)
    if not ach or not ach["season_id"]:
        return {}
    out: Dict[str, Any] = {"season": season_name(ach["season_id"]), "lines": []}
    rule, arg = str(ach["rule"] or ""), str(ach["rule_arg"] or "")
    try:
        if rule == "place":
            place = int(arg or 0)
            for uid, mode, points in _place_winners(ach["season_id"], place):
                if str(uid) == str(user_id):
                    out["lines"].append(f"{PLACE_TITLES.get(place, arg)} · "
                                        f"режим «{mode}»")
                    out["lines"].append(f"Очки за сезон: {points}")
        elif rule == "rank":
            leader, points = _rank_leader(ach["season_id"], arg)
            if str(leader) == str(user_id):
                out["lines"].append(f"Ранг «{arg}» — первый по очкам сезона")
                out["lines"].append(f"Фэнтези-очки: {round(points, 1)}")
    except Exception as exc:
        logger.warning("Итог сезона для значка %s не собрался: %s", ach_id, exc)
    return out


def recount(ach_id: Optional[int] = None) -> Dict[str, int]:
    """Проходит по правилам и выдаёт значки, кому положено.

    Ничего не отбирает: сыгранные игры не отменяются, а участие в фэнтези тем
    более. Отобрать значок может только человек — спрятав его, или админ —
    сняв выдачу руками."""
    import player_identity
    init()
    given = 0
    checked = 0
    for ach in all_achievements(active_only=True):
        if ach_id is not None and int(ach["id"]) != int(ach_id):
            continue
        rule = str(ach["rule"] or "")
        if not rule:
            continue
        checked += 1
        if rule == "fantasy":
            people = _fantasy_users(str(ach["rule_arg"] or ""))
        elif rule == "games":
            try:
                need = int(ach["rule_arg"] or 0)
            except ValueError:
                continue
            people = [u for u in player_identity.linked_users()
                      if _games_played(u) >= need]
        elif rule == "place":
            if not ach["season_id"]:
                continue
            try:
                place = int(ach["rule_arg"] or 0)
            except ValueError:
                continue
            people = [u for u, _mode, _pts in _place_winners(ach["season_id"], place)]
        elif rule == "rank":
            if not ach["season_id"]:
                continue
            leader, _pts = _rank_leader(ach["season_id"], str(ach["rule_arg"] or ""))
            people = [leader] if leader else []
        else:
            continue
        for user in people:
            if award(int(ach["id"]), user, by_rule=True):
                given += 1
    if given:
        logger.info("Ачивки: выдано по правилам %d", given)
    return {"rules": checked, "given": given}
