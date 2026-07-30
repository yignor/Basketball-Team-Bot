#!/usr/bin/env python3
"""
Движок статистики и очков фэнтези-лиги.

Кеш статистики по игроку за игру (`game_player_stats`) — основа очков.
Ключ (source, game_id, player_id): завершённая игра тянется РАЗ и навсегда
(см. `game_stats_fetched`), раздаётся всем участникам/фичам — нагрузка растёт
от числа новых игр, а не пользователей.

Юр-инвариант ([[legal-data-invariant]]): храним только player_id + номер,
ФИО не сохраняем (display_name из box-score в БД не идёт — показываем
транзитно в сообщениях).

Источники: 'slpro' (basketstat.su, box-score через slpro_game.parse_box_score)
и 'infobasket' (reg.infobasket.su, enhanced_game_parser.extract_player_statistics).
"""

import json
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

# Веса по умолчанию (настраиваемые: хранятся в fantasy_seasons.scoring_json,
# админ меняет без кода). Ключи — категории статистики.
DEFAULT_WEIGHTS: Dict[str, float] = {
    "pts": 1.0,
    "reb": 1.2,
    "ast": 1.5,
    "stl": 3.0,
    "blk": 3.0,
    "tur": -1.0,
    # Ниже — то, чего в протоколе нет отдельной строкой: считаем сами из
    # попыток и попаданий (см. derived_stats). Штрафы за брак делают дорогим
    # не того, кто много бросает, а того, кто попадает.
    "miss": -0.5,      # промах с игры
    "ftmiss": -0.5,    # промах штрафного
    "pf": -0.5,        # свой фол
    "foul_on": 1.0,    # фол, заработанный игроком (на нём сфолили)
    "dd": 3.0,         # дабл-дабл
    "td": 5.0,         # трипл-дабл (вместо дабл-дабла, не сверх него)
}

# Показатели, по которым считаются дабл-дабл и трипл-дабл.
DOUBLE_KEYS = ("pts", "reb", "ast", "stl", "blk")

# ВСЁ, что можно оценить в очках, — каталог для админки. Веса сезона хранят
# ровно эти ключи; чего нет в настройках, то весит 0 и в подсчёт не входит.
# Порядок — как читается протокол: сначала результативные действия, потом
# брак, потом бонусы и служебное.
SCORING_KEYS: Tuple[Tuple[str, str], ...] = (
    ("pts", "Очко"),
    ("reb", "Подбор (всего)"),
    ("reb_off", "Подбор в атаке"),
    ("reb_def", "Подбор в защите"),
    ("ast", "Передача"),
    ("stl", "Перехват"),
    ("blk", "Блок-шот"),
    ("foul_on", "Фол на игроке"),
    ("tur", "Потеря"),
    ("pf", "Свой фол"),
    ("miss", "Промах с игры"),
    ("ftmiss", "Промах штрафного"),
    ("fgm", "Попадание с игры"),
    ("fga", "Бросок с игры"),
    ("tpm", "Трёхочковое попадание"),
    ("tpa", "Трёхочковый бросок"),
    ("ftm", "Штрафной точный"),
    ("fta", "Штрафной бросок"),
    ("dd", "Дабл-дабл"),
    ("td", "Трипл-дабл"),
    ("mins", "Минута на площадке"),
    ("plus_minus", "Плюс-минус"),
)

# Подборы всего = атака + защита, попадания входят в броски. Задать и то и
# другое можно (иногда так и хотят), но это двойной счёт — админку надо
# предупредить, а не запрещать.
OVERLAPPING = {"reb": ("reb_off", "reb_def"), "fga": ("fgm",), "fta": ("ftm",),
               "tpa": ("tpm",)}


def _time_to_secs(value: Any) -> int:
    """«12:34» -> 754. Инфобаскет отдаёт время строкой, SLPRO — секундами."""
    if value is None or value == "":
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    parts = str(value).strip().split(":")
    try:
        nums = [int(x) for x in parts]
    except ValueError:
        return 0
    if len(nums) == 3:
        return nums[0] * 3600 + nums[1] * 60 + nums[2]
    if len(nums) == 2:
        return nums[0] * 60 + nums[1]
    return nums[0] * 60 if nums else 0


def derived_stats(row: Dict[str, Any]) -> Dict[str, float]:
    """Промахи и дабл-даблы — их нет в протоколе строкой, но они есть в цифрах.

    Промахи с игры считаем как попытки минус попадания (fga уже включает
    трёхочковые у обеих лиг). Трипл-дабл НЕ складывается с дабл-даблом:
    три показателя по 10 — это +5, а не +8."""
    def num(key: str) -> float:
        return float(row.get(key, 0) or 0)

    doubles = sum(1 for k in DOUBLE_KEYS if num(k) >= 10)
    return {
        "miss": max(0.0, num("fga") - num("fgm")),
        "ftmiss": max(0.0, num("fta") - num("ftm")),
        "dd": 1.0 if doubles == 2 else 0.0,
        "td": 1.0 if doubles >= 3 else 0.0,
        # Минуты, а не секунды: коэффициент «за секунду» никто не осмыслит.
        "mins": round(num("secs") / 60.0, 2),
    }

SOURCE_SLPRO = "slpro"
SOURCE_INFOBASKET = "infobasket"


# ─────────────────────────── Хранилище ───────────────────────────────────────

def is_game_fetched(source: str, game_id: str) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM game_stats_fetched WHERE source = ? AND game_id = ?",
            (source, str(game_id)),
        ).fetchone()
    return row is not None


def _store_player_row(conn, source: str, game_id: str, game_date: str,
                      season_id: str, team_id: str, s: Dict[str, Any],
                      stage_id: Any = "") -> None:
    """s — нормализованная статистика игрока (ключи как в game_player_stats)."""
    conn.execute(
        """
        INSERT INTO game_player_stats
        (source, game_id, player_id, team_id, number, game_date, season_id, stage_id,
         pts, reb, reb_off, reb_def, ast, stl, blk, tur, pf,
         foul_on, fgm, fga, tpm, tpa, ftm, fta, secs, plus_minus, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source, game_id, player_id) DO UPDATE SET
            team_id=excluded.team_id, number=excluded.number,
            game_date=excluded.game_date, season_id=excluded.season_id,
            stage_id=excluded.stage_id,
            pts=excluded.pts, reb=excluded.reb, reb_off=excluded.reb_off,
            reb_def=excluded.reb_def, ast=excluded.ast, stl=excluded.stl,
            blk=excluded.blk, tur=excluded.tur, pf=excluded.pf,
            foul_on=excluded.foul_on,
            fgm=excluded.fgm, fga=excluded.fga, tpm=excluded.tpm, tpa=excluded.tpa,
            ftm=excluded.ftm, fta=excluded.fta,
            secs=excluded.secs, plus_minus=excluded.plus_minus,
            fetched_at=excluded.fetched_at
        """,
        (source, str(game_id), str(s["player_id"]), str(team_id), str(s.get("number", "")),
         game_date, str(season_id), str(stage_id or ""),
         s.get("pts", 0), s.get("reb", 0), s.get("reb_off", 0), s.get("reb_def", 0),
         s.get("ast", 0), s.get("stl", 0), s.get("blk", 0), s.get("tur", 0), s.get("pf", 0),
         s.get("foul_on", 0),
         s.get("fgm", 0), s.get("fga", 0), s.get("tpm", 0), s.get("tpa", 0),
         s.get("ftm", 0), s.get("fta", 0),
         s.get("secs", 0), s.get("plus_minus", 0), sheets_cache.now_iso()),
    )


def _mark_fetched(conn, source: str, game_id: str, game_date: str) -> None:
    conn.execute(
        """INSERT INTO game_stats_fetched (source, game_id, game_date, fetched_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(source, game_id) DO UPDATE SET fetched_at=excluded.fetched_at""",
        (source, str(game_id), game_date, sheets_cache.now_iso()),
    )


def _store_game_meta(conn, source: str, game_id: Any, meta: Dict[str, Any]) -> None:
    """Матч целиком: счёт, соперник, стадия, четверти.

    Названия команд храним — это не персональные данные, а публичное имя
    клуба, и без него отчёт тренеру пишет «против 38116» вместо соперника.
    ФИО игроков по-прежнему не храним ([[legal-data-invariant]])."""
    conn.execute(
        """INSERT INTO game_meta (source, game_id, game_date, game_time, season_id, stage_id,
                                  home_team_id, guest_team_id, home_name, guest_name,
                                  home_score, guest_score,
                                  quarters_json, arena, video_vk, fetched_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(source, game_id) DO UPDATE SET
               game_date=excluded.game_date, game_time=excluded.game_time,
               season_id=excluded.season_id, stage_id=excluded.stage_id,
               home_team_id=excluded.home_team_id, guest_team_id=excluded.guest_team_id,
               home_name=excluded.home_name, guest_name=excluded.guest_name,
               home_score=excluded.home_score, guest_score=excluded.guest_score,
               quarters_json=excluded.quarters_json, arena=excluded.arena,
               video_vk=excluded.video_vk, fetched_at=excluded.fetched_at""",
        # У старых игр arena/video приходят как null. `.get(k, "")` тут не
        # помогает: ключ есть, а значение None — колонки NOT NULL это роняло.
        (source, str(game_id), meta.get("game_date") or "", meta.get("game_time") or "",
         str(meta.get("season_id") or ""), str(meta.get("stage_id") or ""),
         str(meta.get("home_team_id") or ""), str(meta.get("guest_team_id") or ""),
         str(meta.get("home_name") or ""), str(meta.get("guest_name") or ""),
         int(meta.get("home_score", 0) or 0), int(meta.get("guest_score", 0) or 0),
         json.dumps(meta.get("quarters") or [], ensure_ascii=False),
         meta.get("arena") or "", meta.get("video_vk") or "", sheets_cache.now_iso()),
    )


def store_slpro_box(box, season_id: str = "", stage_id: Any = "") -> int:
    """Сохраняет статистику всех игроков из BoxScore (slpro_game). Возвращает
    число сохранённых игроков. ФИО (display_name) НЕ сохраняем."""
    sheets_cache.init_db()
    game_date = box.game_date or ""
    count = 0
    with sheets_cache.get_connection() as conn:
        _store_game_meta(conn, SOURCE_SLPRO, box.game_id, {
            "game_date": game_date, "game_time": box.game_time,
            "season_id": season_id, "stage_id": stage_id,
            "home_team_id": box.home_id, "guest_team_id": box.guest_id,
            "home_name": box.home_name, "guest_name": box.guest_name,
            "home_score": box.home_score, "guest_score": box.guest_score,
            "quarters": [list(q) for q in (box.quarters or [])],
            "arena": box.arena, "video_vk": box.video_vk,
        })
        for p in box.players.values():
            team_id = box.home_id if p.is_home else box.guest_id
            row = {
                "player_id": p.player_id, "number": p.number,
                "pts": p.pts, "reb": p.reb, "reb_off": p.reb_o, "reb_def": p.reb_d,
                "ast": p.ast, "stl": p.stl, "blk": p.blk, "tur": p.tur, "pf": p.pf,
                "foul_on": p.foul_on,
                "fgm": p.fg2m + p.fg3m, "fga": p.fg2a + p.fg3a,
                "tpm": p.fg3m, "tpa": p.fg3a, "ftm": p.ftm, "fta": p.fta,
                # Время SLPRO отдаёт секундами; плюс-минус в бокс-скоре нет.
                "secs": getattr(p, "time_played", 0) or 0,
                "plus_minus": getattr(p, "plus_minus", 0) or 0,
            }
            _store_player_row(conn, SOURCE_SLPRO, box.game_id, game_date, season_id, team_id, row,
                              stage_id=stage_id)
            count += 1
        _mark_fetched(conn, SOURCE_SLPRO, box.game_id, game_date)
        conn.commit()
    return count


def store_infobasket_game(game_info: Dict[str, Any], season_id: str = "") -> int:
    """Сохраняет статистику игроков из распарсенной Infobasket-игры
    (enhanced_game_parser: game_info['player_stats']['players'] с person_id).
    game_info должен содержать game_id и date (ISO или DD.MM.YYYY)."""
    sheets_cache.init_db()
    stats = (game_info.get("player_stats") or {}).get("players") or []
    game_id = str(game_info.get("game_id") or "").strip()
    if not game_id:
        # Без id все игры схлопнулись бы в одну строку по первичному ключу, а
        # реестр скачанного стал бы бесполезен — лучше громко ничего не сделать.
        raise ValueError("store_infobasket_game: пустой game_id")
    game_date = _to_iso_date(game_info.get("date") or "")
    count = 0
    teams = game_info.get("teams") or []
    # У игрока Infobasket есть название команды, но не её id. Сопоставляем по
    # названию; если парсер не дотянулся до имён и подставил "Team 1"/"Team 2" —
    # по номеру.
    by_name = {str(t.get("name", "")): t.get("id", "") for t in teams}
    by_index = [t.get("id", "") for t in teams]

    def _team_id(player: Dict[str, Any]) -> Any:
        name = str(player.get("team", ""))
        if name in by_name:
            return by_name[name]
        if name.startswith("Team ") and len(by_index) == 2:
            n = name[5:].strip()
            if n in ("1", "2"):
                return by_index[int(n) - 1]
        return ""

    with sheets_cache.get_connection() as conn:
        if len(teams) == 2:
            # quarters здесь не сохраняем: enhanced_game_parser разворачивает их
            # под наш взгляд («мы:соперник») для сообщения в чат, и по game_info
            # уже не отличить, перевёрнуты они или нет.
            _store_game_meta(conn, SOURCE_INFOBASKET, game_id, {
                "game_date": game_date, "game_time": game_info.get("time", ""),
                "season_id": season_id,
                "home_team_id": teams[0].get("id", ""), "guest_team_id": teams[1].get("id", ""),
                "home_name": teams[0].get("name", ""), "guest_name": teams[1].get("name", ""),
                "home_score": teams[0].get("score", 0), "guest_score": teams[1].get("score", 0),
                "arena": game_info.get("venue", ""),
            })
        for p in stats:
            pid = p.get("person_id")
            if not pid:
                continue
            row = {
                "player_id": pid, "number": p.get("jersey_number", ""),
                "pts": p.get("points", 0), "reb": p.get("rebounds", 0),
                "reb_off": p.get("offensive_rebounds", 0), "reb_def": p.get("defensive_rebounds", 0),
                "ast": p.get("assists", 0), "stl": p.get("steals", 0),
                "blk": p.get("blocks", 0), "tur": p.get("turnovers", 0), "pf": p.get("fouls", 0),
                # Инфобаскет отдаёт это готовым полем OpponentFoul.
                "foul_on": p.get("opponent_fouls", 0),
                # Время и плюс-минус лига тоже отдаёт (PlayedTime, PlusMinus) —
                # просто раньше мы их не забирали, и в админке эти два
                # показателя работали бы только у SLPRO.
                "secs": _time_to_secs(p.get("minutes")),
                "plus_minus": p.get("plus_minus", 0) or 0,
                "fgm": (p.get("field_goals_made", 0) + p.get("three_pointers_made", 0)),
                "fga": (p.get("field_goals_attempted", 0) + p.get("three_pointers_attempted", 0)),
                "tpm": p.get("three_pointers_made", 0), "tpa": p.get("three_pointers_attempted", 0),
                "ftm": p.get("free_throws_made", 0), "fta": p.get("free_throws_attempted", 0),
            }
            team_id = p.get("team_id") or _team_id(p) or ""
            _store_player_row(conn, SOURCE_INFOBASKET, game_id, game_date, season_id, team_id, row)
            count += 1
        _mark_fetched(conn, SOURCE_INFOBASKET, game_id, game_date)
        conn.commit()
    return count


def _to_iso_date(d: str) -> str:
    """DD.MM.YYYY -> YYYY-MM-DD; ISO оставляем как есть."""
    if not d:
        return ""
    if "-" in d and len(d) >= 10:
        return d[:10]
    try:
        from datetime import datetime
        return datetime.strptime(d, "%d.%m.%Y").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return d


# ─────────────────────────── Ingest SLPRO ────────────────────────────────────

async def ingest_slpro(client, ctx: Dict[str, Any]) -> int:
    """Тянет box-score всех завершённых игр нашей команды в SLPRO, которых
    ещё нет в кеше. Возвращает число новых выкачанных игр."""
    import slpro_game
    games = await client.get_our_games(ctx)
    season_id = str(ctx.get("season_id") or "")
    # Стадию обязательно тянем из контекста: подсчёт очков фильтруется по
    # (source, season_id, stage_id), и игра без стадии молча выпадает из
    # зачёта — при том что скачана и лежит в базе.
    stage_id = ctx.get("stage_id") or ""
    new_games = 0
    for g in games:
        if g.get("status") != 2:            # только завершённые
            continue
        gid = str(g.get("game_id"))
        if is_game_fetched(SOURCE_SLPRO, gid):
            continue
        resp = await client.get_game(gid, ctx)
        box = slpro_game.parse_box_score(resp) if resp else None
        if not box:
            continue
        store_slpro_box(box, season_id, stage_id)
        new_games += 1
    return new_games


# ─────────────────────────── Очки ────────────────────────────────────────────

def fantasy_points(stat_row: Dict[str, Any], weights: Optional[Dict[str, float]] = None) -> float:
    """Очки за одну игру игрока по весам."""
    w = weights or DEFAULT_WEIGHTS
    row = {**stat_row, **derived_stats(stat_row)}
    total = 0.0
    for key, coeff in w.items():
        total += float(row.get(key, 0) or 0) * coeff
    return round(total, 2)


def parse_ref(ref: str) -> Tuple[str, str]:
    """"slpro:707:XXXX" / "ib:36502:XXXXXX" -> (source, player_id).
    source-псевдонимы: slpro -> slpro, ib/infobasket -> infobasket."""
    parts = ref.split(":")
    src = parts[0].lower()
    src = SOURCE_INFOBASKET if src in ("ib", "infobasket") else SOURCE_SLPRO
    player_id = parts[-1]
    return src, player_id


def parse_ref_full(ref: str) -> Tuple[str, str, str]:
    """"slpro:707:XXXX" -> (source, team_id, player_id). Команда нужна, чтобы
    сравнить игры игрока с играми его команды (посещаемость)."""
    parts = str(ref).split(":")
    src, pid = parse_ref(ref)
    team = parts[1] if len(parts) >= 3 else ""
    return src, team, pid


def scope_where(scopes: Any) -> Tuple[str, List[Any]]:
    """Условие «считать только эти турниры». Принимает список scope-словарей
    {source, season_id, stage_id} ИЛИ один словарь (обратная совместимость).
    Несколько турниров объединяются по ИЛИ — команда играет сразу в двух лигах.

    Пусто/None = считать всё (так было исторически, пока в базе жили только
    наши игры; после бэкфилла это опасно — попадёт вся лига за все сезоны)."""
    if not scopes:
        return "", []
    if isinstance(scopes, dict):
        scopes = [scopes]
    ors: List[str] = []
    params: List[Any] = []
    for sc in scopes:
        if not isinstance(sc, dict) or not sc:
            continue
        conds = []
        for column in ("source", "season_id", "stage_id"):
            value = sc.get(column)
            if value not in (None, ""):
                conds.append(f"{column} = ?")
                params.append(str(value))
        if conds:
            ors.append("(" + " AND ".join(conds) + ")")
    if not ors:
        return "", []
    return " AND (" + " OR ".join(ors) + ")", params


def expand_refs(refs: Any) -> List[str]:
    """Разворачивает составные ссылки. Один физический игрок, играющий в двух
    лигах, кодируется одной composite-ссылкой «slpro:..+ib:..» — его очки надо
    складывать, а не задваивать. Здесь режем по «+» в плоский список лиговых
    ссылок. Обычная одиночная ссылка возвращается как есть."""
    out: List[str] = []
    for r in (refs or []):
        out.extend(str(r).split("+"))
    return out


def combine_agg(parts: List[Dict[str, Any]], weights: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    """Складывает агрегаты нескольких лиг одного игрока в один (для пула)."""
    w = weights or DEFAULT_WEIGHTS
    parts = [p for p in parts if p]
    if not parts:
        return {}
    out: Dict[str, Any] = {"games": 0}
    for c in AGG_KEYS:
        out[c] = 0
    for a in parts:
        out["games"] += int(a.get("games", 0) or 0)
        for c in AGG_KEYS:
            out[c] += int(a.get(c, 0) or 0)
    out["fp"] = round(sum(float(out.get(k, 0)) * coeff for k, coeff in w.items()), 2)
    out["fp_avg"] = round(out["fp"] / out["games"], 2) if out["games"] else 0.0
    return out


def player_points(refs: List[str], weights: Optional[Dict[str, float]] = None,
                  date_from: Optional[str] = None, date_to: Optional[str] = None,
                  scope: Optional[Dict[str, Any]] = None) -> float:
    """Сумма очков игрока за период по всем его связанным ID (агрегация
    SLPRO + Infobasket). refs — список ссылок вида source:team:player_id
    (или составных «slpro:..+ib:..» — один игрок в двух лигах).
    Повторы в refs умножают очки: можно поставить всё на одного.
    Период — ISO-даты включительно, scope — турнир подсчёта."""
    sheets_cache.init_db()
    w = weights or DEFAULT_WEIGHTS
    scope_sql, scope_params = scope_where(scope)
    total = 0.0
    with sheets_cache.get_connection() as conn:
        for ref in expand_refs(refs):
            src, pid = parse_ref(ref)
            query = "SELECT * FROM game_player_stats WHERE source = ? AND player_id = ?"
            params: List[Any] = [src, pid]
            if date_from:
                query += " AND game_date >= ?"; params.append(date_from)
            if date_to:
                query += " AND game_date <= ?"; params.append(date_to)
            query += scope_sql
            params.extend(scope_params)
            for r in conn.execute(query, params).fetchall():
                total += fantasy_points(dict(r), w)
    return round(total, 2)


def career_summary(source: str, player_id: str, form_n: int = 5) -> Dict[str, Any]:
    """Личный прогресс игрока по локальной копии протоколов: всего игр, средние
    за игру, последняя игра и форма (среднее за последние N игр против
    предыдущих N). Без фильтра по турниру — это карьера, а не зачёт лиги."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT * FROM game_player_stats
               WHERE source = ? AND player_id = ? AND game_date != ''
               ORDER BY game_date""", (source, str(player_id)))]
    if not rows:
        return {"games": 0}

    def avg(subset: List[Dict[str, Any]], key: str) -> float:
        return round(sum(float(r.get(key, 0) or 0) for r in subset) / len(subset), 1) if subset else 0.0

    def avg_fp(subset: List[Dict[str, Any]]) -> float:
        return round(sum(fantasy_points(r) for r in subset) / len(subset), 1) if subset else 0.0

    recent, earlier = rows[-form_n:], rows[-2 * form_n:-form_n]
    last = rows[-1]
    return {
        "games": len(rows),
        "first_date": rows[0]["game_date"],
        "last_date": last["game_date"],
        "avg": {k: avg(rows, k) for k in ("pts", "reb", "ast", "stl", "blk", "tur")},
        "avg_fp": avg_fp(rows),
        "last": {"date": last["game_date"], "fp": fantasy_points(last),
                 **{k: int(last.get(k, 0) or 0) for k in ("pts", "reb", "ast", "stl", "blk", "tur")}},
        "form": {"recent": avg_fp(recent), "earlier": avg_fp(earlier),
                 "n": len(recent), "prev_n": len(earlier)},
    }


def last_game_date(scope: Optional[Any] = None,
                   teams: Optional[List[Tuple[str, str]]] = None) -> str:
    """Дата последней игры, попавшей в локальную статистику (в рамках турниров
    подсчёта). Нужна для среза «по последней игре».

    teams — пары (источник, team_id) наших команд. Без них берётся последняя
    игра ВСЕГО турнира: в базе лежит вся лига, и срез «последняя игра» уезжал
    на чужой матч, сыгранный позже нашего, — экран показывал пустоту."""
    sheets_cache.init_db()
    scope_sql, params = scope_where(scope)
    params = list(params)
    team_sql = ""
    if teams:
        team_sql = " AND (" + " OR ".join(
            "(source = ? AND team_id = ?)" for _ in teams) + ")"
        for src, tid in teams:
            params.extend([str(src), str(tid)])
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            f"""SELECT MAX(game_date) AS d FROM game_player_stats
                WHERE game_date != ''{scope_sql}{team_sql}""", params).fetchone()
    return (row["d"] or "") if row else ""


def game_points(refs: List[str], source: str, game_id: Any,
                weights: Optional[Dict[str, float]] = None) -> float:
    """Сколько состав набрал в ОДНОЙ конкретной игре. Нужно, чтобы зафиксировать
    очки в момент результата: состав размораживается после каждой игры, и
    пересчитывать задним числом «текущим» составом нельзя.

    Повторы в refs умножают очки — как и в player_points (можно поставить
    несколько слотов на одного игрока)."""
    sheets_cache.init_db()
    w = weights or DEFAULT_WEIGHTS
    total = 0.0
    with sheets_cache.get_connection() as conn:
        for ref in expand_refs(refs):
            src, pid = parse_ref(ref)
            if src != source:
                continue          # игрок из другой лиги — этой игры не касается
            row = conn.execute(
                """SELECT * FROM game_player_stats
                   WHERE source = ? AND game_id = ? AND player_id = ?""",
                (src, str(game_id), pid)).fetchone()
            if row:
                total += fantasy_points(dict(row), w)
    return round(total, 2)


# Колонки, по которым имеет смысл суммировать (покрывают любые веса сезона).
AGG_COLUMNS = ("pts", "reb", "reb_off", "reb_def", "ast", "stl", "blk", "tur",
               "pf", "foul_on", "fgm", "fga", "tpm", "tpa", "ftm", "fta")

# Те же derived_stats, но на языке SQL: промахи складываются из сумм, а
# дабл-даблы — только поштучно по играм, суммой их не восстановить.
_DOUBLES = " + ".join(f"({k} >= 10)" for k in DOUBLE_KEYS)
AGG_DERIVED = {
    "miss": "SUM(MAX(fga - fgm, 0))",
    "ftmiss": "SUM(MAX(fta - ftm, 0))",
    "mins": "ROUND(SUM(secs) / 60.0, 2)",
    "dd": f"SUM(CASE WHEN ({_DOUBLES}) = 2 THEN 1 ELSE 0 END)",
    "td": f"SUM(CASE WHEN ({_DOUBLES}) >= 3 THEN 1 ELSE 0 END)",
}
AGG_KEYS = AGG_COLUMNS + tuple(AGG_DERIVED)


def player_aggregates(weights: Optional[Dict[str, float]] = None,
                      date_from: Optional[str] = None,
                      date_to: Optional[str] = None,
                      scope: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
    """Суммарная статистика каждого игрока за период и турнир (scope).
    Ключ — "source:player_id".

    Фэнтези-очки линейны по весам, поэтому итог считается от сумм, а не
    пересчётом по каждой игре."""
    sheets_cache.init_db()
    w = weights or DEFAULT_WEIGHTS
    sums = ", ".join([f"SUM({c}) AS {c}" for c in AGG_COLUMNS] +
                     [f"{expr} AS {name}" for name, expr in AGG_DERIVED.items()])
    query = f"SELECT source, player_id, COUNT(*) AS games, {sums} FROM game_player_stats WHERE 1=1"
    params: List[Any] = []
    if date_from:
        query += " AND game_date >= ?"; params.append(date_from)
    if date_to:
        query += " AND game_date <= ?"; params.append(date_to)
    scope_sql, scope_params = scope_where(scope)
    query += scope_sql
    params.extend(scope_params)
    query += " GROUP BY source, player_id"

    out: Dict[str, Dict[str, Any]] = {}
    with sheets_cache.get_connection() as conn:
        for row in conn.execute(query, params).fetchall():
            r = dict(row)
            games = int(r["games"] or 0)
            agg: Dict[str, Any] = {"games": games}
            for c in AGG_KEYS:
                agg[c] = int(r[c] or 0)
            agg["fp"] = round(sum(float(agg.get(k, 0)) * coeff for k, coeff in w.items()), 2)
            agg["fp_avg"] = round(agg["fp"] / games, 2) if games else 0.0
            out[f"{r['source']}:{r['player_id']}"] = agg
    return out


def player_last_fp(weights: Optional[Dict[str, float]] = None,
                   scope: Optional[Any] = None) -> Dict[str, Dict[str, Any]]:
    """Последняя игра каждого игрока: {"source:player_id": {date, fp, pts}}.
    Для строки «под фамилией» в пуле."""
    sheets_cache.init_db()
    w = weights or DEFAULT_WEIGHTS
    scope_sql, scope_params = scope_where(scope)
    # Берём по каждому игроку строку с максимальной датой игры.
    query = (
        "SELECT gps.* FROM game_player_stats gps WHERE gps.game_date = ("
        "  SELECT MAX(g2.game_date) FROM game_player_stats g2"
        "  WHERE g2.source=gps.source AND g2.player_id=gps.player_id" + scope_sql +
        ")" + scope_sql
    )
    params = list(scope_params) + list(scope_params)
    out: Dict[str, Dict[str, Any]] = {}
    with sheets_cache.get_connection() as conn:
        for row in conn.execute(query, params).fetchall():
            r = dict(row)
            key = f"{r['source']}:{r['player_id']}"
            if key in out:
                continue  # если в один день две игры — берём любую
            out[key] = {"date": r["game_date"], "fp": fantasy_points(r, w),
                        "pts": int(r.get("pts", 0) or 0)}
    return out


def _pct(made: int, attempted: int) -> Optional[float]:
    return round(made * 100.0 / attempted, 1) if attempted else None


def _game_line(row: Dict[str, Any], weights: Dict[str, float]) -> Dict[str, Any]:
    """Строка одной игры игрока: сухие цифры + фэнтези-очки за неё."""
    line = {k: int(row.get(k, 0) or 0) for k in AGG_COLUMNS}
    line["game_id"] = row["game_id"]
    line["game_date"] = row["game_date"]
    line["fp"] = fantasy_points(row, weights)
    return line


def player_profile(ref: str, scope: Optional[Dict[str, Any]] = None,
                   weights: Optional[Dict[str, float]] = None,
                   log_size: int = 8) -> Dict[str, Any]:
    """Карточка игрока в рамках турнира (scope).

    Считает посещаемость (игры игрока против игр его команды), серию
    пропущенных последних матчей, суммы/средние за турнир, проценты бросков,
    последнюю сыгранную игру и журнал последних игр с очками за каждую."""
    sheets_cache.init_db()
    w = weights or DEFAULT_WEIGHTS
    scope_sql, scope_params = scope_where(scope)
    # Игрок может быть в двух лигах (составная ссылка) — объединяем игры по всем.
    triples = [parse_ref_full(lr) for lr in expand_refs([ref])]
    primary_team = triples[0][1] if triples else ""

    team_games: List[Dict[str, Any]] = []
    player_rows: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        for src, team_id, pid in triples:
            for r in conn.execute(
                "SELECT game_id, game_date, home_team_id, guest_team_id, home_score, guest_score "
                "FROM game_meta WHERE source = ? AND (home_team_id = ? OR guest_team_id = ?)"
                + scope_sql + " ORDER BY game_date, game_id",
                [src, team_id, team_id] + scope_params).fetchall():
                d = dict(r); d["source"] = src; d["_team"] = team_id
                team_games.append(d)
            for r in conn.execute(
                "SELECT * FROM game_player_stats WHERE source = ? AND player_id = ?"
                + scope_sql + " ORDER BY game_date, game_id",
                [src, pid] + scope_params).fetchall():
                d = dict(r); d["_team"] = team_id
                player_rows.append(d)
    team_games.sort(key=lambda g: (g["game_date"], str(g["game_id"])))
    player_rows.sort(key=lambda r: (r["game_date"], str(r["game_id"])))

    played_ids = {(r["source"], str(r["game_id"])) for r in player_rows}

    # Сколько матчей команды подряд, начиная с последнего, игрок пропустил.
    missed_streak = 0
    for g in reversed(team_games):
        if (g["source"], str(g["game_id"])) in played_ids:
            break
        missed_streak += 1

    totals = {c: sum(int(r.get(c, 0) or 0) for r in player_rows) for c in AGG_COLUMNS}
    games = len(player_rows)
    fp_total = round(sum(float(totals.get(k, 0)) * coeff for k, coeff in w.items()), 2)

    season = {
        "games": games,
        "team_games": len(team_games),
        "missed_streak": missed_streak,
        "fp": fp_total,
        "fp_avg": round(fp_total / games, 2) if games else 0.0,
        "totals": totals,
        "avg": {c: round(totals[c] / games, 1) for c in AGG_COLUMNS} if games else {},
        "fg_pct": _pct(totals["fgm"], totals["fga"]),
        "tp_pct": _pct(totals["tpm"], totals["tpa"]),
        "ft_pct": _pct(totals["ftm"], totals["fta"]),
    }

    last = None
    if player_rows:
        row = player_rows[-1]
        last = _game_line(row, w)
        meta = next((g for g in team_games if g["source"] == row["source"]
                     and str(g["game_id"]) == str(row["game_id"])), None)
        if meta:
            we_home = str(meta["home_team_id"]) == str(row["_team"])
            last.update({
                "opponent_id": meta["guest_team_id"] if we_home else meta["home_team_id"],
                "our_score": meta["home_score"] if we_home else meta["guest_score"],
                "their_score": meta["guest_score"] if we_home else meta["home_score"],
                "home": we_home,
            })

    return {
        "ref": ref, "team_id": primary_team,
        "season": season,
        "last": last,
        "log": [_game_line(r, w) for r in player_rows[-log_size:]][::-1],
    }
