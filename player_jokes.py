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
# В блоке шесть показателей; подписываем половину. Все шесть — балаган, одна —
# теряется. Какие именно, решает жребий (Jokes.plan), а не порядок строк.
MAX_PER_MESSAGE = 3


def _norm(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def find_player(name: str) -> List[Dict[str, Any]]:
    """Кандидаты из листа «Игроки» по тому, что человек ввёл руками.

    Поиск общий на весь бот (player_search): понимает часть фамилии и имя.
    Возвращаем всех подходящих — выбирать будет человек, иначе однофамильцы
    молча получат чужую шутку."""
    import player_search
    return [{"row_index": p["row"], "surname": p["surname"], "name": p["name"]}
            for p in player_search.find(name)]


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


POLL_TYPES = ("ОПРОС_ИГРА", "ОПРОС_ИГРА_SLPRO", "АНОНС_ИГРА", "АНОНС_ИГРА_SLPRO")


def _iso(date_str: str) -> str:
    """«27.06.2026» и «2026-06-27» — к одному виду. В служебных записях
    встречаются оба: инфобаскетовские идут в русском формате, SLPRO в ISO."""
    s = (date_str or "").strip()
    if len(s) == 10 and s[2] == "." and s[5] == ".":
        return f"{s[6:]}-{s[3:5]}-{s[:2]}"
    return s


def upcoming_games(limit: int = 5) -> List[Dict[str, Any]]:
    """Ближайшие игры из локальных служебных записей — без сети.

    Берём то, по чему уже создан опрос или анонс: раз бот о матче объявил,
    значит матч наш и состоится. Расписание целиком мы не зеркалим, а для
    выбора «на какую игру шутка» этого хватает."""
    from datetime import date
    sheets_cache.init_db()
    today = date.today().isoformat()
    marks = ",".join("?" * len(POLL_TYPES))
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            f"""SELECT data_type, game_id, game_date, game_time, additional_data,
                       team_a_id, team_b_id, alt_name
                FROM service_records
                WHERE data_type IN ({marks}) AND game_date != ''""", POLL_TYPES)]
        teams = {str(r["team_id"]): r["name"] for r in conn.execute(
            "SELECT team_id, name FROM league_teams WHERE name != ''")}
        for r in conn.execute("""SELECT home_team_id, home_name, guest_team_id, guest_name
                                 FROM game_meta WHERE home_name != '' OR guest_name != ''"""):
            teams.setdefault(str(r["home_team_id"]), r["home_name"])
            teams.setdefault(str(r["guest_team_id"]), r["guest_name"])

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        iso = _iso(r["game_date"])
        if iso < today:
            continue
        slpro = r["data_type"].endswith("_SLPRO")
        source = "slpro" if slpro else "infobasket"
        gid = str(r["game_id"] or "").replace("slpro-", "")
        if not gid:
            continue
        # Соперник — тот из двух id, что не наш. Название берём из справочника,
        # а если его нет — вытаскиваем из текста опроса («X против Y»).
        ours = str(r["team_a_id"] or ""), str(r["team_b_id"] or "")
        opp = ""
        head = (r["additional_data"] or "").split("\n")[0]
        m = re.search(r"против\s+(.+?)(?:\s*\(|$)", head)
        if m:
            opp = m.group(1).strip()
        if not opp:
            for tid in ours:
                nm = teams.get(tid, "")
                if nm and nm != (r["alt_name"] or ""):
                    opp = nm
                    break
        key = f"{source}:{gid}"
        out.setdefault(key, {
            "source": source, "game_id": gid, "date": iso,
            "time": r["game_time"] or "", "opponent": opp or "соперник",
            "league": "SLPRO" if slpro else "Инфобаскет",
        })
    games = sorted(out.values(), key=lambda g: (g["date"], g["time"]))
    for g in games:
        d = g["date"]
        g["label"] = f"{d[8:10]}.{d[5:7]} · {g['opponent']} · {g['league']}"
    return games[:limit]


def add(target_row: int, occasion: str, text: str,
        author_id: Any, author_nick: str = "",
        game_source: str = "", game_id: str = "",
        game_label: str = "", game_date: str = "") -> Tuple[bool, str]:
    """Добавляет фразу. (получилось, что сказать человеку).

    game_id пустой — «на ближайшую игру этого человека»."""
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
               (target_row, occasion, text, author_id, author_nick, created_at,
                active, game_source, game_id, game_label, game_date)
               VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)""",
            (int(target_row), occasion, text.strip(), str(author_id),
             (author_nick or "").lstrip("@"), sheets_cache.now_iso(),
             game_source, str(game_id or ""), game_label, game_date))
        conn.commit()
    where = f"начиная с игры {game_label}" if game_label else "в ближайшей его игре"
    when = {"win": " и только после победы", "loss": " и только после поражения"}.get(
        occasion, "")
    return True, (f"Готово. Фраза прозвучит {where}{when} — один раз. "
                  f"Не попал в строку — подождёт следующей игры.")


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
              WHERE j.active = 1 AND j.used_at = ''""")
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

    Берём только те фразы, что назначены ЭТОЙ игре, плюс «на ближайшую»
    (game_id пустой). Сработавшую сразу гасим: смысл в том, чтобы шутка
    прозвучала один раз и не превратилась в подпись навсегда.
    """

    def __init__(self, won: Optional[bool], source: str = "", game_id: Any = "",
                 game_date: str = "", limit: int = MAX_PER_MESSAGE):
        self.occasions = ("any",) if won is None else \
            (("win", "any") if won else ("loss", "any"))
        self.source = "slpro" if source == "slpro" else \
            ("infobasket" if source else "")
        self.game_id = str(game_id or "")
        self.game_date = str(game_date or "")
        self.limit = limit
        self.used = 0
        self.seen: set = set()
        self.chosen: Optional[set] = None      # заполняет plan()
        self._by_row: Dict[int, List[Dict[str, Any]]] = {}
        self._names: Dict[str, int] = {}
        self._load()

    def plan(self, names: List[str]) -> None:
        """Кому из перечисленных достанутся фразы.

        Без этого шутки всегда доставались первым строкам — очкам и подборам, —
        а до перехватов и фолов не доходило никогда. Здесь вызывающий сообщает
        ВЕСЬ список имён, которые попадут в сообщение, и жребий решает, кто из
        них получит подпись."""
        have = [n for n in dict.fromkeys(names) if n and self._row_of(n) is not None]
        if len(have) <= self.limit:
            self.chosen = set(have)
        else:
            self.chosen = set(random.sample(have, self.limit))

    def _load(self) -> None:
        try:
            sheets_cache.init_db()
            marks = ",".join("?" * len(self.occasions))
            with sheets_cache.get_connection() as conn:
                for r in conn.execute(
                        f"""SELECT j.id, j.target_row, j.text, j.author_nick,
                                   j.game_id, j.game_source, j.game_date
                            FROM player_jokes j
                            WHERE j.active = 1 AND j.used_at = ''
                              AND j.occasion IN ({marks})""",
                        list(self.occasions)):
                    row = dict(r)
                    if not self._eligible(row):
                        continue
                    self._by_row.setdefault(int(row["target_row"]), []).append(row)
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

    def _eligible(self, row: Dict[str, Any]) -> bool:
        """Подходит ли фраза к этой игре.

        Выбранная игра — это «не раньше», а не «только тогда». Человек мог не
        попасть ни в одну строку (сыграл ровно, лимит на сообщение выбрали
        другие) — фраза не должна пропасть. Она ждёт следующих его игр, пока не
        прозвучит. Поэтому: точное совпадение id ИЛИ игра уже прошла."""
        if not row["game_id"]:
            return True                       # «на ближайшую» — подходит любая
        if row["game_source"] and self.source and row["game_source"] != self.source:
            # Другая лига — но если её игра уже позади, фраза всё равно ждёт.
            return bool(row["game_date"] and self.game_date
                        and self.game_date > row["game_date"])
        if row["game_id"] == self.game_id:
            return True
        return bool(row["game_date"] and self.game_date
                    and self.game_date > row["game_date"])

    def _row_of(self, name: str) -> Optional[int]:
        row = self._names.get(_norm(name))
        if row is None and name:
            row = self._names.get(_norm(name).split(" ")[0])
        return row if (row is not None and self._by_row.get(row)) else None

    def _burn(self, joke_id: int) -> None:
        """Гасим сработавшую фразу. Делаем это в момент выбора, а не после
        отправки: если сообщение не уйдёт, потеряется одна шутка — это дешевле,
        чем риск отправить её дважды."""
        try:
            with sheets_cache.get_connection() as conn:
                conn.execute("UPDATE player_jokes SET used_at = ? WHERE id = ?",
                             (sheets_cache.now_iso(), int(joke_id)))
                conn.commit()
        except Exception:
            pass

    def for_name(self, name: str) -> str:
        """« · фраза (с) @ник» для игрока или пустая строка."""
        if self.used >= self.limit or not self._by_row:
            return ""
        # Если жребий брошен (plan), подписываем только выбранных.
        if self.chosen is not None and name not in self.chosen:
            return ""
        row = self._row_of(name)
        if row is None or row in self.seen:
            return ""
        pool = self._by_row.get(row) or []
        if not pool:
            return ""
        pick = random.choice(pool)
        self.seen.add(row)
        self.used += 1
        self._burn(pick["id"])
        # Оба сообщения о результате уходят с parse_mode='HTML'. Человек мог
        # написать «<3» или «Гиря & Ко» — без экранирования Telegram отверг бы
        # ВСЁ сообщение, и команда осталась бы без счёта из-за одной шутки.
        text = html.escape(pick["text"])
        nick = html.escape(pick["author_nick"])
        sign = f" — @{nick}" if nick else ""
        # Своей строкой, а не хвостом к статистике. В строку «🏀 Штрафные:
        # Иванов - 40% · фраза (с) @ник» фраза не читалась вовсе: глаз ищет в
        # блоке цифры и проскакивает всё, что стоит после них.
        return f"\n     💬 <i>{text}</i>{sign}"


def decorate(name: str, won: Optional[bool]) -> str:
    """Разовая подпись, когда сообщение собирается вне класса Jokes."""
    return Jokes(won, limit=1).for_name(name)
