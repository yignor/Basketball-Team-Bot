#!/usr/bin/env python3
"""
Привязка игрока к его профилям в лигах.

Человек присылает боту ссылку на свой профиль, бот достаёт оттуда числовой id и
запоминает связку «Telegram id -> id в лиге». Дальше по этому id собирается
личная статистика: у нас уже лежит копия протоколов, искать человека по фамилии
не нужно (а на фамилиях такое строить и нельзя — однофамильцы и опечатки молча
подмешают чужие игры).

ФИО здесь не хранится: только числовые идентификаторы — см. юр-инвариант.
Имя показывается транзитно из публичного ростера лиги, как и везде.

Поддерживаемые ссылки:
  https://slpro.basketstat.ru/player/XXXX
  https://www.fbp.ru/player.html?personId=XXXXXX&apiUrl=https://reg.infobasket.su&compId=XXXXXX&lang=ru
"""

from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import sheets_cache

SOURCE_SLPRO = "slpro"
SOURCE_INFOBASKET = "infobasket"

# Хосты, которым доверяем разбор ссылки. Инфобаскет — «движок» под многими
# сайтами федераций (fbp.ru и другие), поэтому опознаём по параметрам ссылки,
# а адрес API берём из неё же (apiUrl), но только если он инфобаскетовский.
_SLPRO_HOSTS = ("slpro.basketstat.ru", "basketstat.ru", "basketstat.su")
_IB_API_HOSTS = ("reg.infobasket.su", "infobasket.su")


def parse_profile_link(url: str) -> Optional[Dict[str, str]]:
    """Ссылка на профиль -> {source, player_id, comp_id, api_url} или None.

    Ничего не выдумывает: если id в ссылке нет — возвращает None, чтобы бот
    честно сказал «не понял ссылку», а не привязал мусор."""
    url = (url or "").strip()
    if not url:
        return None
    try:
        u = urlparse(url if "//" in url else "https://" + url)
    except ValueError:
        return None
    host = (u.netloc or "").lower().split(":")[0]
    host = host[4:] if host.startswith("www.") else host
    q = parse_qs(u.query or "")

    # SLPRO: числовой id прямо в пути — /player/XXXX
    if host in _SLPRO_HOSTS:
        parts = [p for p in (u.path or "").split("/") if p]
        if len(parts) >= 2 and parts[-2] == "player" and parts[-1].isdigit():
            return {"source": SOURCE_SLPRO, "player_id": parts[-1],
                    "comp_id": "", "api_url": ""}
        return None

    # Инфобаскет: personId в параметрах, сайт-обёртка может быть любой.
    person = (q.get("personId") or q.get("PersonID") or [""])[0].strip()
    if person.isdigit():
        api = (q.get("apiUrl") or [""])[0].strip().rstrip("/")
        api_host = urlparse(api).netloc.lower() if api else ""
        if api and not any(api_host.endswith(h) for h in _IB_API_HOSTS):
            return None          # чужой apiUrl — не ходим по нему
        comp = (q.get("compId") or [""])[0].strip()
        return {"source": SOURCE_INFOBASKET, "player_id": person,
                "comp_id": comp if comp.isdigit() else "",
                "api_url": api or "https://reg.infobasket.su"}
    return None


def get_identities(tg_user_id: Any) -> List[Dict[str, Any]]:
    """Привязанные профили пользователя (по одному на лигу)."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM player_identities WHERE tg_user_id = ? ORDER BY source",
            (str(tg_user_id),)).fetchall()
    return [dict(r) for r in rows]


def link_identity(tg_user_id: Any, parsed: Dict[str, str]) -> Dict[str, Any]:
    """Привязывает профиль. Возвращает {ok, changed, previous}.

    changed=True — привязка по этой лиге уже была и заменена; счётчик смен
    храним, на нём потом сядет платная смена id (чтобы подписку не передавали
    по кругу всей команде)."""
    sheets_cache.init_db()
    uid = str(tg_user_id)
    with sheets_cache.get_connection() as conn:
        prev = conn.execute(
            "SELECT player_id, changes FROM player_identities WHERE tg_user_id = ? AND source = ?",
            (uid, parsed["source"])).fetchone()
        if prev and str(prev["player_id"]) == str(parsed["player_id"]):
            return {"ok": True, "changed": False, "previous": None, "same": True}
        changes = (int(prev["changes"]) + 1) if prev else 0
        conn.execute(
            """INSERT INTO player_identities
               (tg_user_id, source, player_id, comp_id, api_url, linked_at, changes)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(tg_user_id, source) DO UPDATE SET
                   player_id=excluded.player_id, comp_id=excluded.comp_id,
                   api_url=excluded.api_url, linked_at=excluded.linked_at,
                   changes=excluded.changes""",
            (uid, parsed["source"], str(parsed["player_id"]), parsed.get("comp_id", ""),
             parsed.get("api_url", ""), sheets_cache.now_iso(), changes))
        conn.commit()
    return {"ok": True, "changed": bool(prev), "same": False,
            "previous": str(prev["player_id"]) if prev else None}


def have_games(source: str, player_id: str) -> int:
    """Сколько игр этого человека уже есть в локальной копии. Ноль означает,
    что его соревнование мы не зеркалим — статистику придётся дотянуть."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            """SELECT COUNT(DISTINCT game_id) AS n FROM game_player_stats
               WHERE source = ? AND player_id = ?""",
            (source, str(player_id))).fetchone()
    return int(row["n"] or 0) if row else 0


SOURCE_TITLES = {SOURCE_SLPRO: "SLPRO", SOURCE_INFOBASKET: "Инфобаскет"}
