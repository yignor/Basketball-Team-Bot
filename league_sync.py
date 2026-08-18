#!/usr/bin/env python3
"""
Качалка справочников лиг: команды, заявки, имена.

Единственное место, которое ходит в чужие API ради СПРАВОЧНИКОВ (протоколы
качают stats_backfill/run_backfill). Всё остальное — пул фэнтези, разбор
команды, клавиатура — читает результат из локальной базы и памяти и в сеть
не ходит вовсе.

Так решено после 31.07.2026, когда весь трафик демона ушёл в VPN, лига через
него перестала отвечать, и каждое действие игрока начало стоить минуту
таймаутов. Правило простое: **в ответе человеку живых запросов быть не
должно**. Лига недоступна — работаем на том, что скачано вчера, и пишем об
этом в журнал, а не заставляем ждать.

Что куда ложится:
  • команды и стадии  -> таблица league_teams (id и названия команд — не ПДн);
  • состав заявки     -> таблица league_rosters (ТОЛЬКО id и номер);
  • ФИО               -> player_names, оперативная память, на диск не идёт
                         ([[legal-data-invariant]]).

Запускается: фоновым циклом демона (раз в час) и `run_league_sync.py` по cron.
"""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import player_names
import sheets_cache

log = logging.getLogger(__name__)

# Заявки последнего прохода вместе с датами рождения: {source:team_id -> [люди]}.
# Живут только в памяти — даты рождения на диск не идут ([[legal-data-invariant]]).
# Нужны, чтобы искать пары «старый id из протокола ↔ действующий из заявки».
_roster_cache: Dict[str, List[Dict[str, Any]]] = {}


# ── Команды: кого вообще считаем «нашими» ───────────────────────────────────

async def _slpro_teams() -> List[Dict[str, Any]]:
    """Наши команды SLPRO с полным контекстом стадии."""
    out: List[Dict[str, Any]] = []
    try:
        import slpro_client
        for ctx in await slpro_client.team_contexts():
            tid = ctx.get("team_id")
            if tid is None:
                continue
            out.append({
                "source": "slpro", "team_id": str(tid),
                "name": ctx.get("team_name") or "SLPRO",
                "league": slpro_client.scope_of(ctx).get("name", ""),
                "comp_id": "", "season_id": str(ctx.get("season_id") or ""),
                "stage_id": str(ctx.get("stage_id") or ""),
                "ctx": ctx,
            })
    except Exception as e:
        log.warning(f"качалка: команды SLPRO — {e}")
    return out


async def _infobasket_teams() -> List[Dict[str, Any]]:
    """Наши команды Инфобаскета: id и comp_id берутся из «Конфига» (локально),
    название спрашиваем у лиги."""
    out: List[Dict[str, Any]] = []
    try:
        from enhanced_duplicate_protection import duplicate_protection
        import stats_backfill
        cfg = duplicate_protection.get_config_ids()
        comps = cfg.get("comp_ids") or []
        comp = str(comps[0]) if comps else ""
        for tid in (cfg.get("team_ids") or []):
            entry = (cfg.get("teams") or {}).get(tid) or {}
            name = ""
            try:
                info = await stats_backfill.fetch_infobasket_team(tid, comp)
                name = info.get("name") or ""
            except Exception as e:
                log.warning(f"качалка: название команды Инфобаскета {tid} — {e}")
            out.append({
                "source": "infobasket", "team_id": str(tid),
                "name": name or f"Команда {tid}",
                "league": entry.get("alt_name") or "Инфобаскет",
                "comp_id": comp, "season_id": comp, "stage_id": "", "ctx": None,
            })
    except Exception as e:
        log.warning(f"качалка: команды Инфобаскета — {e}")
    return out


def _store_teams(teams: List[Dict[str, Any]]) -> int:
    """Пишем ТОЛЬКО то, что реально приехало: пустой ответ лиги не должен
    стирать вчерашний справочник — иначе один сбой сети оставляет бота без
    команд, а игрока без пула."""
    if not teams:
        return 0
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        for t in teams:
            conn.execute(
                """INSERT INTO league_teams
                   (source, team_id, name, league, comp_id, season_id, stage_id,
                    ours, ctx_json, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                   ON CONFLICT(source, team_id) DO UPDATE SET
                     name = excluded.name, league = excluded.league,
                     comp_id = excluded.comp_id, season_id = excluded.season_id,
                     stage_id = excluded.stage_id, ours = 1,
                     ctx_json = excluded.ctx_json, fetched_at = excluded.fetched_at""",
                (t["source"], t["team_id"], t["name"], t["league"], t["comp_id"],
                 t["season_id"], t["stage_id"],
                 json.dumps(t["ctx"], ensure_ascii=False) if t.get("ctx") else "", now))
        conn.commit()
    return len(teams)


def our_teams(source: Optional[str] = None) -> List[Dict[str, Any]]:
    """Наши команды из локального справочника. Без сети."""
    sheets_cache.init_db()
    sql = "SELECT * FROM league_teams WHERE ours = 1"
    args: List[Any] = []
    if source:
        sql += " AND source = ?"
        args.append(source)
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(sql + " ORDER BY source, team_id", args)]
    for r in rows:
        r["ctx"] = json.loads(r["ctx_json"]) if r["ctx_json"] else None
    return rows


# ── Заявки: id и номера на диск, ФИО в память ───────────────────────────────

async def _fetch_roster(team: Dict[str, Any]) -> List[Dict[str, Any]]:
    """[{player_id, number, name, birth, active}] из заявки лиги.

    `birth` — транзитно, только чтобы понять «это один и тот же человек»
    (detect_same_person). На диск не попадает: в league_rosters такой колонки
    нет и не будет."""
    src, tid = team["source"], team["team_id"]
    if src == "slpro":
        from slpro_client import SlproClient
        rows = await SlproClient().get_roster(int(tid))
        return [{"player_id": str(p.get("player_id")), "number": str(p.get("number") or ""),
                 "name": f"{p.get('surname', '')} {p.get('name', '')}".strip(),
                 # У SLPRO поле называется birth_day («2001-09-22»), у
                 # Инфобаскета PersonBirth («22.09.2001»). Формат не важен:
                 # сравниваем строки на равенство внутри одной лиги.
                 "birth": str(p.get("birth_day") or p.get("birthday") or ""),
                 "active": True}
                for p in rows if p.get("player_id") is not None]
    import stats_backfill
    rows = await stats_backfill.fetch_infobasket_roster(tid, team.get("comp_id"))
    return [{"player_id": str(p["player_id"]), "number": str(p.get("number") or ""),
             "name": p.get("name") or "", "birth": p.get("birth") or "",
             "active": bool(p.get("active", True))}
            for p in rows if p.get("player_id") is not None]


def _store_roster(team: Dict[str, Any], players: List[Dict[str, Any]]) -> int:
    """Заявка на диск — БЕЗ ФИО. Пустую заявку не применяем по той же причине,
    что и пустой справочник команд."""
    if not players:
        return 0
    now = sheets_cache.now_iso()
    src, tid = team["source"], team["team_id"]
    with sheets_cache.get_connection() as conn:
        # Выбывших помечаем неактивными, а не удаляем: они остаются в наших
        # протоколах, и пулу нужно знать их номер.
        conn.execute("UPDATE league_rosters SET active = 0 WHERE source = ? AND team_id = ?",
                     (src, tid))
        for p in players:
            conn.execute(
                """INSERT INTO league_rosters
                   (source, team_id, player_id, number, comp_id, active, fetched_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(source, team_id, player_id) DO UPDATE SET
                     number = CASE WHEN excluded.number != '' THEN excluded.number
                                   ELSE league_rosters.number END,
                     comp_id = excluded.comp_id, active = excluded.active,
                     fetched_at = excluded.fetched_at""",
                (src, tid, p["player_id"], p["number"], str(team.get("comp_id") or ""),
                 1 if p["active"] else 0, now))
        conn.commit()
    return len(players)


def roster_of(source: str, team_id: Any) -> List[Dict[str, Any]]:
    """Заявка команды из локальной базы: [{player_id, number, active}]."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(
            """SELECT player_id, number, active FROM league_rosters
               WHERE source = ? AND team_id = ?""", (source, str(team_id)))]


# ── Полный проход ───────────────────────────────────────────────────────────

async def refresh(teams_only: bool = False) -> Dict[str, Any]:
    """Обновить справочники. Возвращает сводку для журнала/админки.

    Ошибки не поднимаем наверх: качалка обязана пережить недоступную лигу
    молча — с точки зрения бота это просто «сегодня свежих данных нет»."""
    sheets_cache.init_db()
    teams = await _slpro_teams() + await _infobasket_teams()
    stored_teams = _store_teams(teams)
    out = {"teams": stored_teams, "rosters": 0, "names": 0, "merged": 0, "failed": 0}
    if teams_only:
        return out

    # Заявки берём по локальному справочнику: если лига сейчас молчит, работаем
    # по вчерашнему списку команд, а не остаёмся вовсе ни с чем.
    for team in (teams or our_teams()):
        try:
            players = await _fetch_roster(team)
        except Exception as e:
            log.warning(f"качалка: заявка {team['source']}:{team['team_id']} — {e}")
            out["failed"] += 1
            continue
        out["rosters"] += _store_roster(team, players)
        out["names"] += player_names.put_many(
            team["source"], ((p["player_id"], p["name"]) for p in players))
        _roster_cache[f"{team['source']}:{team['team_id']}"] = players
        out["merged"] += _store_pairs(
            detect_same_person(players, team["source"], team["team_id"]))
    return out


# ── «Это один и тот же человек»: склейка дублей внутри лиги ─────────────────

def _lev1(a: str, b: str) -> bool:
    """Расстояние Левенштейна не больше 1. Одна опечатка — всё ещё тот же
    человек; две — уже гадание, и на этом лучше остановиться."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la == lb:
        return sum(1 for x, y in zip(a, b) if x != y) <= 1
    if la > lb:
        a, b, la, lb = b, a, lb, la
    i = j = diff = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1
            j += 1
        else:
            diff += 1
            if diff > 1:
                return False
            j += 1
    return True


def _norm(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def _birth_key(text: str) -> str:
    """Дата рождения к одному виду.

    SLPRO отдаёт «1993-05-17», Инфобаскет — «17.05.1993». Пока сравнивали
    строки как есть, один и тот же человек в двух лигах никогда не совпадал —
    и склейка по дате между лигами не срабатывала вообще."""
    s = (text or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    if len(s) == 10 and s[2] == "." and s[5] == ".":
        return f"{s[6:]}-{s[3:5]}-{s[:2]}"
    return s


def detect_same_person(people: List[Dict[str, Any]], source: str,
                       team_id: str) -> List[Tuple[str, str]]:
    """Пары id, за которыми стоит один человек.

    Правило (сформулировано пользователем 31.07.2026): **одна дата рождения
    плюс расхождение в имени или фамилии не больше чем на символ — это один
    человек**. Лига заводит людей заново каждый сезон и иногда с опечаткой, и
    тогда в отчёте по команде один игрок идёт двумя строками.

    Дата рождения сюда приходит транзитно и никуда не сохраняется: наружу
    отдаём только пары идентификаторов."""
    by_birth: Dict[str, List[Dict[str, Any]]] = {}
    for p in people:
        birth = _birth_key(p.get("birth") or "")
        if birth and p.get("name"):
            by_birth.setdefault(birth, []).append(p)
    pairs: List[Tuple[str, str]] = []
    pref = "slpro" if source == "slpro" else "ib"
    for birth, group in by_birth.items():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                if str(a["player_id"]) == str(b["player_id"]):
                    continue
                na, nb = _norm(a["name"]).split(), _norm(b["name"]).split()
                if not na or not nb:
                    continue
                # Совпала фамилия и почти совпало имя — или наоборот.
                same = ((na[0] == nb[0] and _lev1(" ".join(na[1:]), " ".join(nb[1:])))
                        or (na[1:] == nb[1:] and _lev1(na[0], nb[0])))
                if same:
                    one = f"{pref}:{team_id}:{a['player_id']}"
                    two = f"{pref}:{team_id}:{b['player_id']}"
                    pairs.append((one, two))
                    # В журнал — ссылки, не имена и не дата рождения. Журнал
                    # ложится на диск и уезжает в бэкапы, а ФИО по инварианту
                    # проекта живут только в памяти качалки. Для разбора
                    # ссылки и удобнее: ровно по ним лежит запись в
                    # player_merges, к которой этот лог и относится.
                    log.info(f"качалка: {one} и {two} — один человек, склеиваю")
    return pairs


def detect_across_leagues(by_team: Dict[str, List[Dict[str, Any]]]) -> List[Tuple[str, str]]:
    """Один человек в РАЗНЫХ лигах: Farm в SLPRO и основа в Инфобаскете — это
    часто одни и те же люди, и лиги пишут их по-разному («Шлепикас Роман» и
    «Ромас Шлепикас»). Тут склейка держится на дате рождения, а имя нужно лишь
    чтобы не слепить двух однодневок: совпасть должна фамилия или имя.

    by_team: {"source:team_id" -> [{player_id, name, birth}]}."""
    flat: List[Tuple[str, str, Dict[str, Any]]] = []
    for key, people in by_team.items():
        source, team_id = key.split(":", 1)
        for p in people:
            if p.get("name") and _birth_key(p.get("birth") or ""):
                flat.append((source, team_id, p))
    pairs: List[Tuple[str, str]] = []
    for i in range(len(flat)):
        for j in range(i + 1, len(flat)):
            (sa, ta, a), (sb, tb, b) = flat[i], flat[j]
            if sa == sb:
                continue                      # внутри лиги — другая функция
            if _birth_key(a["birth"]) != _birth_key(b["birth"]):
                continue
            na, nb = set(_norm(a["name"]).split()), set(_norm(b["name"]).split())
            # Порядок «фамилия имя» у лиг разный, поэтому сравниваем множества
            # слов: хотя бы одно должно совпасть точно.
            if not (na & nb):
                continue
            pa = "slpro" if sa == "slpro" else "ib"
            pb = "slpro" if sb == "slpro" else "ib"
            one = f"{pa}:{ta}:{a['player_id']}"
            two = f"{pb}:{tb}:{b['player_id']}"
            pairs.append((one, two))
            log.info(f"качалка: {one} и {two} — один человек по дате рождения, "
                     f"склеиваю")
    return pairs


def _store_pairs(pairs: List[Tuple[str, str]]) -> int:
    """Пары в player_merges — ГРУППАМИ, а не по две.

    Наивная запись «пара за парой» разъезжается, как только у человека три
    ссылки: пара A–B пишет «A+B», пара B–C пишет «B+C», и A с C оказываются
    разными людьми. Поэтому сначала собираем связные группы (включая уже
    записанные ранее), и только потом пишем всем участникам одну общую ссылку.
    Пока у наших игроков по две ссылки, но третья появится в первый же день,
    когда лига заведёт кому-нибудь новый id."""
    if not pairs:
        return 0
    sheets_cache.init_db()
    parent: Dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    with sheets_cache.get_connection() as conn:
        # Уже известные группы тоже в расчёт: иначе новая пара разорвала бы
        # вчерашнюю склейку.
        for r in conn.execute("SELECT ref, canonical FROM player_merges"):
            refs = str(r["canonical"]).split("+") or [r["ref"]]
            for one in refs:
                union(refs[0], one)
            union(refs[0], r["ref"])
        for a, b in pairs:
            union(a, b)

        groups: Dict[str, set] = {}
        for ref in list(parent):
            groups.setdefault(find(ref), set()).add(ref)
        now = sheets_cache.now_iso()
        for members in groups.values():
            if len(members) < 2:
                continue
            canonical = "+".join(sorted(members))
            for one in members:
                conn.execute(
                    """INSERT INTO player_merges (ref, canonical, fetched_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(ref) DO UPDATE SET
                         canonical = excluded.canonical,
                         fetched_at = excluded.fetched_at""", (one, canonical, now))
        conn.commit()
    return len(pairs)


async def fill_missing_names(limit: int = 40) -> int:
    """Имена тех, кто есть в протоколах, но выпал из заявки.

    Такие в пуле висят как «№10», и не понять ни кто это, ни не тот ли это
    человек, что уже есть в списке под другим написанием. Ходим по одному
    игроку и только фоном; `limit` — чтобы один прогон не растянулся на
    полтысячи запросов."""
    known = set(player_names.get_all())
    teams = our_teams()
    todo: List[tuple] = []
    with sheets_cache.get_connection() as conn:
        for t in teams:
            for r in conn.execute(
                    """SELECT DISTINCT player_id FROM game_player_stats
                       WHERE source = ? AND team_id = ?""", (t["source"], t["team_id"])):
                pid = str(r["player_id"])
                if f"{t['source']}:{pid}" not in known:
                    todo.append((t["source"], pid))
    got = 0
    # Заодно собираем дату рождения: старые id живут только в протоколах, и
    # именно там прячутся дубли — человека завели заново, а прежняя карточка
    # осталась. По заявке их не найти, она знает только действующих.
    extra: Dict[str, List[Dict[str, Any]]] = {}
    for source, pid in todo[:limit]:
        name, birth = "", ""
        try:
            if source == "slpro":
                from slpro_client import SlproClient
                info = await SlproClient().get_player_info(pid)
                if info:
                    name = f"{info.get('surname', '')} {info.get('name', '')}".strip()
                    birth = str(info.get("birth_day") or info.get("birthday") or "")
            else:
                import stats_backfill
                one = await stats_backfill.fetch_infobasket_person_info(pid)
                name, birth = one["name"], one["birth"]
        except Exception as e:
            log.warning(f"качалка: имя {source}:{pid} — {e}")
        if name:
            player_names.put(source, pid, name)
            got += 1
        if name and birth:
            extra.setdefault(source, []).append(
                {"player_id": pid, "name": name, "birth": birth})

    # Ищем дубли по ВСЕМ, кого знаем про команду: заявка (даты запомнила
    # refresh) плюс те, кто остался только в протоколах. Пара «старый id из
    # протокола ↔ действующий из заявки» — самый частый случай, и увидеть её
    # можно только в общем списке.
    merged = 0
    full: Dict[str, List[Dict[str, Any]]] = {}
    for t in teams:
        people = list(_roster_cache.get(f"{t['source']}:{t['team_id']}") or [])
        people += extra.get(t["source"], [])
        full[f"{t['source']}:{t['team_id']}"] = people
        pairs = detect_same_person([p for p in people if p.get("birth")],
                                   t["source"], t["team_id"])
        merged += _store_pairs(pairs)
    # И между лигами: Farm в SLPRO и основа в Инфобаскете — во многом одни люди.
    merged += _store_pairs(detect_across_leagues(full))
    if merged:
        log.info(f"качалка: склеено пар «это один человек»: {merged}")
    return got


async def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    res = await refresh()
    extra = await fill_missing_names()
    print(f"Справочники лиг обновлены: команд {res['teams']}, в заявках {res['rosters']}, "
          f"имён {res['names']} (+{extra} из протоколов), ошибок {res['failed']}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(asyncio.run(main()))
