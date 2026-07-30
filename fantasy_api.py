#!/usr/bin/env python3
"""
HTTP-API фэнтези для Mini App (aiohttp). Живёт в процессе демона
(bot_daemon.on_startup подвешивает сервер в тот же event loop), наружу
отдаётся через Tailscale Funnel. Фронт (GitHub Pages) ходит сюда за пулом/
составом/таблицей и сохраняет состав.

Авторизация — по Telegram `initData` (подпись WebApp), проверяется HMAC от
токена бота. Участники v1 — только игроки команды (Telegram ID в листе
«Игроки»); проверка через players-таблицу.

Юр-инвариант: в БД (fantasy_rosters) кладём только ссылки source:team:player_id.
Имена в пуле — транзитно из публичного ростера федерации, в наших таблицах
не хранятся.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl
from datetime import date, timedelta

from aiohttp import web

import sheets_cache
import fantasy
import fantasy_modes
import fantasy_prices
import fantasy_stats

log = logging.getLogger(__name__)

# Публичный адрес живого API. Транспорт — Tailscale Funnel, он зашит во фронте
# и не меняется. Здесь остаётся только запасной рычаг: переопределить адрес
# через env, не пересобирая фронт (переезд tailnet, замена канала). Пусто —
# фронт идёт на Funnel.
#
# Cloudflare quick-tunnel убран 30.07.2026. Он давал адрес, который менялся при
# каждом рестарте, и бот подмешивал его в ссылку кнопки (?api=). Ссылка в
# кнопке живёт до следующего /start, поэтому она регулярно указывала на
# туннель, которого уже нет: имя переставало резолвиться, фронт упирался в
# мёртвый адрес и уходил в запасной режим — при живом Funnel рядом.

# Публичный вход, который видят игроки: Tailscale Funnel. Держим его тёплым
# (см. keep_funnel_warm) и заодно измеряем — жалобы «не грузится» иначе
# невозможно отличить от «у человека плохая сеть».
FUNNEL_URL = os.getenv("FANTASY_FUNNEL_URL", "https://botpc.tail5ed4ef.ts.net").rstrip("/")

_dns_cache: Dict[str, Tuple[float, bool]] = {}
_DNS_TTL = 300


def _resolves(url: str) -> bool:
    """Резолвится ли хост адреса. Быстрый DNS-запрос, ответ кешируем на 5 минут.

    Страховка для env-override: мёртвый адрес хуже отсутствующего, потому что
    во фронте он стоит ПЕРВЫМ и заслоняет рабочий Funnel."""
    import socket
    from urllib.parse import urlparse
    host = urlparse(url).hostname or ""
    if not host:
        return False
    now = time.time()
    hit = _dns_cache.get(host)
    if hit and now - hit[0] < _DNS_TTL:
        return hit[1]
    try:
        socket.getaddrinfo(host, None)
        ok = True
    except OSError:
        ok = False
        log.info("адрес живого API не резолвится, отдаём фронту Funnel: %s", host)
    _dns_cache[host] = (now, ok)
    return ok


def public_api_url() -> str:
    """Адрес живого API для фронта: env-override FANTASY_API_PUBLIC_URL.
    Пусто (обычный случай) — фронт идёт на зашитый Funnel."""
    url = os.getenv("FANTASY_API_PUBLIC_URL", "").strip().rstrip("/")
    return url if url and _resolves(url) else ""


_funnel_ips: Tuple[float, List[str]] = (0.0, [])
_FUNNEL_IP_TTL = 3600
_funnel_seen = False


async def _funnel_public_ips() -> List[str]:
    """Публичные адреса ingress'а Funnel — через сторонний DNS-over-HTTPS.

    Системный резолвер сервера отвечать на этот вопрос не годится: MagicDNS
    Tailscale подставляет tailnet-адрес (100.x), и запрос уходит в самого себя
    по локальной сети. Ingress при этом остаётся холодным — то есть «прогрев»
    грел бы петлю, а не тот путь, по которому идут игроки."""
    global _funnel_ips
    now = time.time()
    if now - _funnel_ips[0] < _FUNNEL_IP_TTL and _funnel_ips[1]:
        return _funnel_ips[1]
    import aiohttp
    from urllib.parse import urlparse
    host = urlparse(FUNNEL_URL).hostname or ""
    ips: List[str] = []
    for doh in ("https://dns.google/resolve", "https://cloudflare-dns.com/dns-query"):
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(doh, params={"name": host, "type": "A"},
                                    headers={"accept": "application/dns-json"},
                                    timeout=aiohttp.ClientTimeout(total=10)) as r:
                    data = await r.json(content_type=None)
            ips = [a["data"] for a in (data.get("Answer") or [])
                   if a.get("type") == 1 and a.get("data")]
            if ips:
                break
        except Exception:
            continue
    if ips:
        _funnel_ips = (now, ips)
    return ips


async def keep_funnel_warm(timeout: float = 30.0) -> Optional[float]:
    """Стучится в СВОЙ публичный вход снаружи. Возвращает время ответа, сек
    (None — не достучались).

    Funnel засыпает: первый запрос после простоя поднимает соединение и идёт
    около 15 секунд — ровно столько же, сколько ждёт фронт, поэтому игрок
    получал таймаут и уходил в запасной режим на пустом месте. Регулярный пинг
    не даёт каналу остыть.

    Идём по ПУБЛИЧНОМУ ip ingress'а, подставляя имя в Host и SNI: только так
    греется тот же путь, по которому приходят игроки. По имени запрос
    заворачивался бы MagicDNS обратно в машину (проверено: 8 мс против 300 —
    это и была подсказка, что грелась петля)."""
    if not FUNNEL_URL:
        return None
    import aiohttp
    from urllib.parse import urlparse
    host = urlparse(FUNNEL_URL).hostname or ""
    ips = await _funnel_public_ips()
    started = time.time()
    try:
        async with aiohttp.ClientSession() as sess:
            if ips:
                r = await sess.get(f"https://{ips[0]}/health", headers={"Host": host},
                                   server_hostname=host,
                                   timeout=aiohttp.ClientTimeout(total=timeout))
            else:
                # Публичные адреса не выяснили — лучше согреть хоть как-то.
                r = await sess.get(f"{FUNNEL_URL}/health",
                                   timeout=aiohttp.ClientTimeout(total=timeout))
            async with r:
                await r.read()
            took = time.time() - started
            global _funnel_seen
            if took > 3:
                log.info("Funnel прогрет за %.1fс (был холодный)", took)
            elif not _funnel_seen:
                # Один раз после старта — чтобы в журнале была видна работа
                # прогрева. Дальше молчим: раз в 10 минут писать «всё хорошо»
                # значит утопить в этом остальной журнал.
                log.info("Прогрев Funnel работает: %s, ответ за %.2fс",
                         ips[0] if ips else "по имени", took)
            _funnel_seen = True
            return took
    except Exception as e:
        log.warning("Funnel не отвечает (%.0fс): %s", time.time() - started, e)
        return None


# ─────────────────────────── initData auth ───────────────────────────────────

def verify_init_data_detailed(init_data: str, bot_token: str,
                              max_age_sec: int = 86400) -> Tuple[Optional[Dict[str, Any]], str]:
    """Проверяет подпись Telegram WebApp initData. Возвращает (пользователь,
    причина отказа). Алгоритм: secret = HMAC_SHA256(key="WebAppData",
    msg=bot_token); hash = HMAC_SHA256(key=secret, msg=data_check_string).

    Причина возвращается отдельно, чтобы в логе было видно, что именно не
    сошлось: пустой заголовок, подпись или срок годности — это разные болезни."""
    if not init_data:
        return None, "заголовка нет"
    bot_token = (bot_token or "").strip()
    if not bot_token:
        return None, "у сервера нет токена бота"
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    except (ValueError, TypeError):
        return None, "не разобрать как query string"
    received_hash = pairs.pop("hash", None)
    if not received_hash:
        return None, "нет поля hash"

    # Bot API 7.10 добавил поле `signature` (Ed25519 для сторонней проверки).
    # Входит ли оно в data_check_string — вопрос версии клиента, поэтому
    # проверяем обе строки. Подделать HMAC без токена всё равно нельзя.
    signature = pairs.pop("signature", None)
    candidates = [("без signature", pairs)]
    if signature is not None:
        candidates.append(("с signature", {**pairs, "signature": signature}))

    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    matched = ""
    for label, fields in candidates:
        dcs = "\n".join(f"{k}={fields[k]}" for k in sorted(fields))
        calc = hmac.new(secret, dcs.encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(calc, received_hash):
            matched = label
            break
    if not matched:
        bot_id = bot_token.split(":", 1)[0]
        return None, (f"подпись не сошлась ни без signature, ни с ним; "
                      f"поля: {', '.join(sorted(pairs))}; бот {bot_id}")
    log.debug("initData: подпись сошлась в варианте «%s»", matched)
    try:
        auth_date = int(pairs.get("auth_date", "0"))
    except (ValueError, TypeError):
        return None, "auth_date не число"
    age = time.time() - auth_date
    if max_age_sec and age > max_age_sec:
        return None, f"подпись устарела на {int(age - max_age_sec)}с"
    try:
        user = json.loads(pairs.get("user", "null"))
    except (json.JSONDecodeError, TypeError):
        return None, "поле user не разобрать"
    if not user:
        return None, "в initData нет пользователя"
    return user, ""


def _is_team_member(telegram_id: Any, username: str = "") -> bool:
    """Он же ensure_player_link — оставлен как привычное имя внутри API."""
    return ensure_player_link(telegram_id, username)


def ensure_player_link(telegram_id: Any, username: str = "") -> bool:
    """Опознаёт человека как игрока команды и ЗАКРЕПЛЯЕТ за строкой листа.

    Зовётся из двух мест: живого API (кто вправе читать фэнтези) и /start
    (там бот впервые видит числовой id и @ник). Раньше только из API — и
    опознание требовало пройти всю цепочку «нажал /start → открыл Mini App →
    достучался до сервера». Кто спотыкался на любом шаге, оставался
    неопознанным навсегда, хотя его ник в листе стоял с самого начала.

    Ник — только для первого знакомства: в листе колонка «Telegram ID»
    исторически заполнена @никами, поэтому при первом входе находим строку по
    нику и закрепляем за ней числовой id (см. sheets_cache.player_links).

    Дальше ник уже не нужен: сменил @ — доступ остался; освободил @ и его занял
    посторонний — тот НЕ пройдёт, строка занята. Окно доверия к нику сужается
    до одного первого входа настоящего игрока.

    Участвуют только игроки команды (лист «Игроки»); доступ держим на ЧИСЛОВОМ
    Telegram id — он вечный и не переуступается."""
    sheets_cache.init_db()
    uid = str(telegram_id)

    # 1. Уже привязан — пускаем по числовому id.
    if sheets_cache.get_player_link(uid):
        return True

    # 2. Числовой id проставлен в самом листе (админ вписал руками).
    uname = (username or "").lstrip("@").lower()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT row_index FROM players WHERE tg_user_id = ? LIMIT 1", (uid,)).fetchone()
        if row:
            sheets_cache.link_player(uid, uname, row["row_index"])
            _push_tg_id_to_sheet(row["row_index"], uid, uname)
            return True
        # 3. Первое знакомство: ищем строку по нику — но только СВОБОДНУЮ.
        if not uname:
            return False
        cand = conn.execute(
            """SELECT row_index FROM players
               WHERE lower(ltrim(telegram_id, '@')) = ? LIMIT 1""", (uname,)).fetchone()
    if not cand:
        return False
    if sheets_cache.is_row_linked(cand["row_index"]):
        log.warning(f"фэнтези: ник @{uname} совпал со строкой {cand['row_index']}, "
                    f"но она уже закреплена за другим id — отказ")
        return False
    if not sheets_cache.link_player(uid, uname, cand["row_index"]):
        return False
    log.info(f"фэнтези: @{uname} закреплён за строкой {cand['row_index']} (id {uid})")
    _push_tg_id_to_sheet(cand["row_index"], uid, uname)
    return True


def _push_tg_id_to_sheet(player_row: int, tg_user_id: str, username: str = "") -> None:
    """Best-effort: показать в листе числовой id и актуальный @ник. Доступ живёт
    в локальной player_links, поэтому недоступность Sheets вход не ломает."""
    try:
        from collect_votes import _init_sheets
        ss = _init_sheets()
        sheets_cache.write_player_tg_id(ss, player_row, tg_user_id)
        # Ник в таблице устаревает — обновляем на тот, под которым человек
        # реально пришёл. Иначе в листе остаётся адрес, которого больше нет.
        sheets_cache.write_player_nickname(ss, player_row, username)
    except Exception as e:
        log.warning(f"фэнтези: связка не записана в лист: {e}")


# ─────────────────────────── Пул драфта ──────────────────────────────────────

_pool_cache: Dict[str, Dict[str, Any]] = {}      # season_id -> {at, data}


def invalidate_pool(season_id: Any = None) -> None:
    """Сбросить кеш пула: цены поменялись — карточки обязаны показать новые."""
    if season_id is None:
        _pool_cache.clear()
    else:
        _pool_cache.pop(str(season_id), None)
_POOL_TTL = 3600.0


async def derive_pool_teams() -> List[Dict[str, Any]]:
    """Кандидаты для пула из настроек поиска игр: SLPRO-команда (по имени) +
    команда(ы) Инфобаскета (team_id × comp_id из Конфига)."""
    teams: List[Dict[str, Any]] = []
    try:
        import slpro_client
        seen_ids = set()
        for ctx in await slpro_client.team_contexts():
            tid = ctx.get("team_id")
            if tid and tid not in seen_ids:
                seen_ids.add(tid)
                teams.append({"source": "slpro", "team_id": tid,
                              "name": ctx.get("team_name", "SLPRO"),
                              # Имя лиги: как назвал админ в «Конфиге», иначе —
                              # сезон и дивизион из справочника лиги.
                              "league": slpro_client.scope_of(ctx)["name"]})
    except Exception as e:
        log.warning(f"пул: SLPRO-команда — {e}")
    try:
        from enhanced_duplicate_protection import duplicate_protection
        cfg = duplicate_protection.get_config_ids()
        comps = cfg.get("comp_ids") or []
        comp = comps[0] if comps else None
        for tid in (cfg.get("team_ids") or []):
            # Название команды спрашиваем у лиги, а лигу называем так, как её
            # назвал админ в «Конфиге» (АЛЬТЕРНАТИВНОЕ ИМЯ). Хардкод «Инфобаскет»
            # был неверен: команда там называется иначе, чем в SLPRO.
            entry = (cfg.get("teams") or {}).get(tid) or {}
            teams.append({"source": "infobasket", "team_id": tid, "comp_id": comp,
                          "name": await _ib_team_name(tid, comp),
                          "league": entry.get("alt_name") or "Инфобаскет"})
    except Exception as e:
        log.warning(f"пул: команды Инфобаскета — {e}")
    return teams


_ib_names: Dict[str, str] = {}


async def _ib_team_name(team_id: Any, comp_id: Any) -> str:
    """Название команды Инфобаскета из ответа лиги (с кешем на процесс:
    имя меняется раз в сезон, а админку открывают часто)."""
    key = f"{team_id}:{comp_id}"
    if key not in _ib_names:
        try:
            import stats_backfill
            info = await stats_backfill.fetch_infobasket_team(team_id, comp_id)
            _ib_names[key] = info.get("name") or f"Команда {team_id}"
        except Exception as e:
            log.warning(f"название команды Инфобаскета {key}: {e}")
            return f"Команда {team_id}"
    return _ib_names[key]


_person_names: Dict[str, str] = {}


async def _person_name(source: str, player_id: Any) -> str:
    """ФИО игрока по id ИЗ ЛИГИ — транзитно, в наших таблицах не храним.
    Нужно тем, кто есть в протоколах, но выпал из заявки: без имени в пуле
    висит «№10», и не понять ни кто это, ни не тот ли это человек, что уже
    есть в списке под другим написанием.

    Кеш на процесс: имя меняется раз в жизнь, а пул пересобирается по часам."""
    key = f"{source}:{player_id}"
    if key in _person_names:
        return _person_names[key]
    name = ""
    try:
        if source == "slpro":
            from slpro_client import SlproClient
            info = await SlproClient().get_player_info(player_id)
            if info:
                name = f"{info.get('surname', '')} {info.get('name', '')}".strip()
        else:
            import stats_backfill
            name = await stats_backfill.fetch_infobasket_person(player_id)
    except Exception as e:
        log.warning(f"имя игрока {key}: {e}")
    _person_names[key] = name
    return name


def _protocol_players(source: str, team_id: Any) -> List[Dict[str, Any]]:
    """Кто РЕАЛЬНО играл за команду — из наших протоколов (локальная база).
    Заявка бывает неполной: игрок мог сыграть и уже выбыть из списка. ФИО не
    храним, поэтому отдаём только id + номер; имя подставит заявка, если он
    там есть."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT player_id, number, MAX(game_date) last_game, COUNT(*) games
               FROM game_player_stats WHERE source = ? AND team_id = ?
               GROUP BY player_id ORDER BY last_game DESC""",
            (source, str(team_id))).fetchall()
    return [{"pid": r["player_id"], "number": r["number"] or "",
             "games": r["games"], "last_game": r["last_game"]} for r in rows]


async def _current_pool_teams(season: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Команды пула ПЕРЕД правкой из админки. Пока админ ничего не выбирал, в
    сезоне пусто, а действуют «наши» команды по умолчанию — их и материализуем,
    иначе первое «убрать» вычитало бы из пустого списка и не делало ничего."""
    explicit = fantasy.pool_teams(season) if season else []
    return explicit if explicit else await derive_pool_teams()


async def _resolve_pool_teams(season: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Команды пула сезона: явный выбор админа (fantasy.pool_teams) или
    дефолт — все кандидаты из настроек поиска игр."""
    if season is None:
        season = fantasy.get_active_season()
    explicit = fantasy.pool_teams(season) if season else []
    return explicit if explicit else await derive_pool_teams()


async def build_pool(force: bool = False,
                     season: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Пул драфта: игроки команд из fantasy.pool_teams (SLPRO + Инфобаскет).
    Ref = source:team_id:player_id. Имя — транзитно из публичного ростера,
    в наших таблицах не хранится.

    Кеш в памяти на час и ОТДЕЛЬНО НА СЕЗОН: параллельные лиги набираются из
    разных команд (в этом и смысл «фэнтези для чужой команды»), общий кеш
    показывал бы всем пул той лиги, которая обновилась последней."""
    if season is None:
        season = fantasy.get_active_season()
    key = str((season or {}).get("id") or "-")
    now = time.time()
    entry = _pool_cache.get(key)
    if not force and entry and (now - entry["at"]) < _POOL_TTL:
        return entry["data"]

    pool: List[Dict[str, Any]] = []
    seen = set()
    for team in await _resolve_pool_teams(season):
        src = team.get("source")
        tid = team.get("team_id")
        if tid is None:
            continue
        try:
            if src == "slpro":
                from slpro_client import SlproClient
                roster = await SlproClient().get_roster(tid)
                players = [{"pid": p.get("player_id"), "number": p.get("number", ""),
                            "name": f"{p.get('surname', '')} {p.get('name', '')}".strip()}
                           for p in roster]
            else:  # infobasket
                import stats_backfill
                roster = await stats_backfill.fetch_infobasket_roster(tid, team.get("comp_id"))
                players = [{"pid": p["player_id"], "number": p["number"], "name": p["name"]}
                           for p in roster if p.get("active", True)]
        except Exception as e:
            log.warning(f"пул: ростер {src}:{tid} — {e}")
            players = []          # заявка недоступна — опираемся на протоколы
        src_db = "slpro" if src == "slpro" else "infobasket"
        pref = "slpro" if src == "slpro" else "ib"
        by_pid = {str(p["pid"]): p for p in players if p.get("pid") is not None}

        # Пул = заявка ∪ протоколы. Заявка даёт ФИО и новичков, ещё не игравших;
        # протоколы — тех, кто реально играл, но из заявки уже выпал.
        for pp in _protocol_players(src_db, tid):
            pid = str(pp["pid"])
            if pid in by_pid:
                if not by_pid[pid].get("number"):
                    by_pid[pid]["number"] = pp["number"]   # номер из протокола
                continue
            by_pid[pid] = {"pid": pid, "number": pp["number"],
                           # ФИО не храним — для выбывших из заявки имени нет
                           "name": (await _person_name(src_db, pid)
                                    or (f"№{pp['number']}" if pp["number"] else f"ID {pid}")),
                           "off_roster": True}

        for p in by_pid.values():
            ref = f"{pref}:{tid}:{p['pid']}"
            if ref in seen:
                continue
            seen.add(ref)
            pool.append({"ref": ref, "number": str(p["number"] or ""),
                         "name": p["name"], "team": team.get("name", ""),
                         "league": team.get("league") or team.get("name", ""),
                         "off_roster": bool(p.get("off_roster"))})

    # Один физический игрок может быть в ДВУХ лигах (SLPRO Farm + Инфобаскет) с
    # разными id. Склеиваем по ФИО в одну карточку с составной ссылкой
    # «slpro:..+ib:..» — очки суммируются, а не задваиваются.
    pool = _merge_pool_by_name(pool)
    _pool_cache[key] = {"at": now, "data": pool}
    return pool


def _norm_name(name: str) -> str:
    return " ".join((name or "").lower().replace("ё", "е").split())


def _lev1(a: str, b: str) -> bool:
    """Расстояние Левенштейна между a и b не больше 1 (ранний выход)."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la == lb:  # одна замена
        return sum(1 for x, y in zip(a, b) if x != y) <= 1
    if la > lb:  # ровно одна вставка/удаление
        a, b, la, lb = b, a, lb, la
    i = j = diff = 0
    while i < la and j < lb:
        if a[i] == b[j]:
            i += 1; j += 1
        else:
            diff += 1; j += 1
            if diff > 1:
                return False
    return True


def _srcset(ref: str) -> set:
    return {r.split(":", 1)[0] for r in ref.split("+")}


def _consolidate_similar(merged: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Склеивает почти-одинаковые ФИО ИЗ РАЗНЫХ ЛИГ: расходиться может ЛИБО
    имя, ЛИБО фамилия, и не больше чем на букву — «Шлепикас Роман» ↔
    «Шлепикас Ромас», «Лысюк Денис» ↔ «Лисюк Денис» (реальные случаи: лиги
    записали одного человека по-разному).

    Требуем ровно одно расхождение и разные лиги: внутри одной лиги двух людей
    сливать нельзя, а «Долгих Денис» и «Долгих Владислав» — братья, и их
    различают имена целиком. Более сложные расхождения — руками в «Игроки»."""
    result: List[Dict[str, Any]] = []
    for e in merged:
        if _norm_name(e["name"]).startswith(("№", "id ")):   # имени нет, сравнивать нечего
            result.append(dict(e))
            continue
        eparts = _norm_name(e["name"]).split()
        e_sur = eparts[0] if eparts else ""
        e_first = eparts[1] if len(eparts) > 1 else ""
        e_src = _srcset(e["ref"])
        hit = None
        for g in result:
            if _norm_name(g["name"]).startswith(("№", "id ")):
                continue
            gparts = _norm_name(g["name"]).split()
            g_sur = gparts[0] if gparts else ""
            g_first = gparts[1] if len(gparts) > 1 else ""
            if not e_sur or not g_sur or (_srcset(g["ref"]) & e_src):
                continue
            same_sur, same_first = g_sur == e_sur, g_first == e_first
            near_sur, near_first = _lev1(e_sur, g_sur), _lev1(e_first, g_first)
            # одна часть совпадает точно, вторая — с точностью до буквы
            if (same_sur and near_first) or (same_first and near_sur):
                hit = g
                break
        if hit:
            hit["ref"] = "+".join(sorted(set(hit["ref"].split("+")) | set(e["ref"].split("+"))))
            if not hit["number"]:
                hit["number"] = e["number"]
            for lg in e.get("leagues") or []:
                if lg not in hit.setdefault("leagues", []):
                    hit["leagues"].append(lg)
            hit["team"] = " · ".join(hit.get("leagues") or [])
            hit["off_roster"] = bool(hit.get("off_roster")) and bool(e.get("off_roster"))
        else:
            result.append(dict(e))
    return result


def _merge_pool_by_name(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_name: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for p in pool:
        key = _norm_name(p["name"])
        # Склеиваем по ФИО — в том числе выбывших из заявки: их имя мы теперь
        # спрашиваем у лиги, и это единственный способ увидеть, что «выпавший»
        # игрок — тот же человек, что уже есть в списке. Имя не достали (осталось
        # «№13») — склейка по нему слепила бы разных людей с одним номером.
        if not key or key.startswith(("№", "id ")):
            key = p["ref"]
        if key not in by_name:
            by_name[key] = {"refs": [p["ref"]], "number": p["number"],
                            "name": p["name"], "team": p["team"],
                            "leagues": [p.get("league") or p["team"]],
                            "off_roster": bool(p.get("off_roster"))}
            order.append(key)
        else:
            by_name[key]["refs"].append(p["ref"])
            lg = p.get("league") or p["team"]
            if lg and lg not in by_name[key]["leagues"]:
                by_name[key]["leagues"].append(lg)
            if not by_name[key]["number"]:
                by_name[key]["number"] = p["number"]
            # «Нет в заявке» — только если человека нет в заявке НИ ОДНОЙ лиги:
            # выпасть из состава в одной и играть в другой — обычное дело.
            by_name[key]["off_roster"] &= bool(p.get("off_roster"))
    merged = []
    for key in order:
        e = by_name[key]
        # Подпись под именем — ЛИГИ игрока: человек играет и в SLPRO, и в
        # Инфобаскете, и видеть одну команду вместо обеих лиг бесполезно.
        merged.append({"ref": "+".join(sorted(e["refs"])), "number": e["number"],
                       "name": e["name"], "team": " · ".join(e["leagues"]),
                       "leagues": e["leagues"], "off_roster": e["off_roster"]})
    return _consolidate_similar(merged)


# ─────────────────────────── Хендлеры ────────────────────────────────────────

def _cors(resp: web.Response) -> web.Response:
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Headers"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return resp


@web.middleware
async def cors_middleware(request: web.Request, handler):
    if request.method == "OPTIONS":
        return _cors(web.Response())
    resp = await handler(request)
    # Логируем только свои пути и только метод+путь+код (без строки запроса,
    # где могла бы оказаться подпись). Шум от сканеров сюда не попадает.
    if request.path.startswith("/fantasy/"):
        log.info(f"фэнтези-API: {request.method} {request.path} -> {resp.status}")
    return _cors(resp)


def _auth_user(request: web.Request) -> Optional[Dict[str, Any]]:
    """Подпись принимаем ТОЛЬКО заголовком: строка запроса попадает в access-log
    целиком, а initData — это ключ доступа, действующий сутки."""
    bot_token = request.app["bot_token"]
    raw = request.headers.get("X-Init-Data", "")
    user, reason = verify_init_data_detailed(raw, bot_token)
    if user is None:
        # Само значение не пишем — это ключ доступа. Только длина и причина.
        log.warning(f"фэнтези-API 401: {request.method} {request.path} — {reason} ({len(raw)} симв.)")
    return user


def _season(request: web.Request) -> Optional[Dict[str, Any]]:
    """Сезон запроса. Mini App передаёт ?season=<id> (выбранная лига); без него
    или если лига уже закрыта — последний активный."""
    sid = request.query.get("season")
    if sid:
        s = fantasy.get_active_by_id(sid)
        if s:
            return s
    return fantasy.get_active_season()


def _can_view(user: Optional[Dict[str, Any]]) -> bool:
    """Кто вправе ЧИТАТЬ фэнтези: игрок команды (лист «Игроки») или админ.
    Подпись initData сама по себе не пропуск — её получит любой, кто открыл
    бота. А в пуле/таблице видны ФИО, поэтому посторонним доступа нет
    (см. юр-инвариант: ФИО показываем только своим)."""
    if not user:
        return False
    return (_is_team_member(str(user.get("id")), user.get("username", ""))
            or _is_admin(user))


def _is_admin(user: Optional[Dict[str, Any]]) -> bool:
    """Админ Mini App — тот же список, что у бота (ADMIN_USER_IDS в .env).
    Подпись initData уже проверена, id подделать нельзя."""
    if not user:
        return False
    admins = {x.strip() for x in
              os.getenv("ADMIN_USER_IDS", os.getenv("ADMIN_USER_ID", "")).split(",") if x.strip()}
    return str(user.get("id")) in admins


def _profile_links(ref: str) -> List[Dict[str, str]]:
    """[{title, url}] по каждой лиге, где у игрока есть id."""
    import player_identity
    out = []
    for one in fantasy_stats.expand_refs([ref]):
        src, pid = fantasy_stats.parse_ref(one)
        url = player_identity.profile_url(src, pid)
        if url:
            out.append({"title": f"{player_identity.SOURCE_TITLES.get(src, src)} · {pid}",
                        "url": url})
    return out


def _price_key(name: str) -> str:
    return " ".join((name or "").lower().replace("ё", "е").split())


def _lookup_price(name: str, prices: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Цена игрока из листа «Игроки» с поправкой на написание.

    Лиги пишут одного человека по-разному («Лисюк» в SLPRO, «Лысюк» в
    Инфобаскете), карточка в пуле получает одно из написаний, а в таблице
    стоит другое — по точному совпадению цена бы потерялась."""
    key = _price_key(name)
    hit = prices.get(key)
    if hit:
        return hit
    parts = key.split()
    if len(parts) < 2:
        return {}
    sur, first = parts[0], parts[1]
    for other, val in prices.items():
        o = other.split()
        if len(o) < 2:
            continue
        if (o[0] == sur and _lev1(first, o[1])) or (o[1] == first and _lev1(sur, o[0])):
            return val
    return {}


def _pool_with_stats(pool: List[Dict[str, Any]], season: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Дополняет пул суммарной статистикой игрока за всё время (для сортировки
    в Mini App). Агрегаты живут отдельно от пула — пул кешируется на час, а
    статистика меняется после каждого ingest."""
    weights = fantasy.season_weights(season) if season else fantasy_stats.DEFAULT_WEIGHTS
    scopes = fantasy.effective_scopes(season) if season else []
    agg = fantasy_stats.player_aggregates(weights, scope=scopes)
    last = fantasy_stats.player_last_fp(weights, scope=scopes)
    excluded = set(fantasy.pool_excluded_names(season or {}))
    # Стоимость и уровень («карточка») ведёт тренер в листе «Игроки».
    prices = sheets_cache.get_player_prices()
    enriched = []
    for p in pool:
        keys = [f"{fantasy_stats.parse_ref(lr)[0]}:{fantasy_stats.parse_ref(lr)[1]}"
                for lr in fantasy_stats.expand_refs([p["ref"]])]
        combined = fantasy_stats.combine_agg([agg.get(k, {}) for k in keys], weights)
        lasts = [last[k] for k in keys if last.get(k)]
        last_one = max(lasts, key=lambda x: x.get("date", ""), default={})
        pr = _lookup_price(p["name"], prices)
        # Уровень ВСЕГДА считаем от цены, а не читаем из таблицы: цена —
        # единственный источник правды, её правит тренер, и значок обязан
        # идти за ней сам. Столбец «Уровень» в листе — формула для глаз.
        tier = fantasy_modes.tier_for(pr.get("price"))
        enriched.append({**p, "stats": combined, "last": last_one,
                         "price": pr.get("price", 0), "tier": tier,
                         "excluded": fantasy.norm_player_name(p["name"]) in excluded})
    return enriched


TOP_PERIODS = ("last", "week", "month", "all")


def _period_bounds(period: str, scopes: Any,
                   teams: Any = None) -> Tuple[Optional[str], Optional[str], str]:
    """(с какой даты, по какую, подпись) для среза топа игроков."""
    today = date.today()
    if period == "week":
        return (today - timedelta(days=7)).isoformat(), None, "за последние 7 дней"
    if period == "month":
        return (today - timedelta(days=30)).isoformat(), None, "за последние 30 дней"
    if period == "last":
        d = fantasy_stats.last_game_date(scopes, teams)
        # Берём именно дату последней игры, а не «сегодня»: игры бывают раз в
        # неделю, и срез «последняя игра» должен показывать её, а не пустоту.
        return (d or None), (d or None), (f"игра {d[8:10]}.{d[5:7]}" if d else "последняя игра")
    return None, None, "за всё время"


async def handle_top(request: web.Request) -> web.Response:
    """Топ игроков команды по фэнтези-очкам за период. Имена берём из пула —
    транзитно из публичных API лиг, у себя ФИО не храним."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    period = request.query.get("period", "all")
    if period not in TOP_PERIODS:
        period = "all"

    weights = fantasy.season_weights(season) if season else fantasy_stats.DEFAULT_WEIGHTS
    scopes = fantasy.effective_scopes(season) if season else []
    pool = await build_pool(season=season)
    # Команды берём из самого пула: чей это топ — тот и определяет, какая игра
    # «последняя». Пул уже собран по командам сезона.
    teams = sorted({(fantasy_stats.parse_ref(one)[0], one.split(":")[1])
                    for p in pool for one in fantasy_stats.expand_refs([p["ref"]])
                    if len(one.split(":")) >= 3})
    d_from, d_to, title = _period_bounds(period, scopes, teams)
    agg = fantasy_stats.player_aggregates(weights, date_from=d_from, date_to=d_to, scope=scopes)

    rows = []
    for p in pool:
        keys = [f"{fantasy_stats.parse_ref(lr)[0]}:{fantasy_stats.parse_ref(lr)[1]}"
                for lr in fantasy_stats.expand_refs([p["ref"]])]
        st = fantasy_stats.combine_agg([agg.get(k, {}) for k in keys], weights)
        if not st or not st.get("games"):
            continue        # не играл в этом срезе — в топе ему делать нечего
        rows.append({"ref": p["ref"], "name": p["name"], "number": p.get("number", ""),
                     "fp": st["fp"], "fp_avg": st["fp_avg"], "games": st["games"],
                     "pts": st["pts"], "reb": st["reb"], "ast": st["ast"]})
    rows.sort(key=lambda x: x["fp"], reverse=True)

    # Вторая таблица — топ угадавших: кто из участников набрал больше очков за
    # тот же период. Считается из тех же снимков по играм.
    # Топ угадавших + чей состав играл: ФИО подставляем из пула (у себя не
    # храним), а если игрока в пуле уже нет — показываем его номер из ссылки.
    guessers = fantasy.top_participants(season["id"], d_from, d_to) if season else []
    by_ref = {p["ref"]: p for p in await build_pool(season=season)}
    for g in guessers:
        for pick in g.get("picks") or []:
            pick["players"] = [_ref_title(r, by_ref) for r in pick.pop("refs", [])]
    return web.json_response({"period": period, "title": title,
                              "top": rows[:30], "guessers": guessers})


def _ref_title(ref: str, by_ref: Dict[str, Any]) -> str:
    """Имя игрока по ссылке. Ссылка может быть старой формы (до склейки лиг) —
    ищем карточку, которая её содержит."""
    entry = by_ref.get(ref) or by_ref.get(fantasy.canonical_ref(ref, by_ref) or "")
    if entry:
        return entry["name"]
    src, pid = fantasy_stats.parse_ref(ref.split("+")[0])
    return f"{src}:{pid}"


async def available_scopes() -> List[Dict[str, Any]]:
    """Турниры, которые вообще можно поставить в зачёт: активная стадия SLPRO и
    соревнования Инфобаскета из листа «Конфиг».

    Нужны, чтобы убранный турнир можно было ВЕРНУТЬ. Раньше интерфейс показывал
    только выбранные, и снятый со счёта турнир исчезал безвозвратно."""
    out: List[Dict[str, Any]] = []
    # Турниры SLPRO — из листа «Конфиг» (а если он пуст, автоопределением по
    # названию команды): и то и другое отдаёт team_contexts.
    try:
        import slpro_client
        for ctx in await slpro_client.team_contexts():
            if ctx.get("stage_id") is not None:
                out.append(slpro_client.scope_of(ctx))
    except Exception as e:
        log.warning(f"админка: турниры SLPRO не определены: {e}")
    try:
        from enhanced_duplicate_protection import duplicate_protection
        for comp in (duplicate_protection.get_config_ids().get("comp_ids") or []):
            out.append({"source": "infobasket", "season_id": str(comp),
                        "name": f"Инфобаскет comp {comp}"})
    except Exception as e:
        log.warning(f"админка: comp_id Инфобаскета не прочитаны: {e}")
    return out


async def handle_admin_state(request: web.Request) -> web.Response:
    """Состояние админки: все активные лиги со своими настройками.

    Каждая лига отдаётся отдельно и правится по своему id — инлайн-кнопки в чате
    этого не умели и при двух активных лигах били по последней."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _is_admin(user):
        return web.json_response({"error": "forbidden"}, status=403)

    candidates = await derive_pool_teams()
    seasons = []
    for s in fantasy.active_seasons():
        scopes = fantasy.effective_scopes(s)
        teams = fantasy.pool_teams(s)
        # Пул явно не задан — значит действуют «наши» команды по умолчанию;
        # показываем их отмеченными, иначе кажется, что пул пуст.
        in_pool = {(str(t.get("source")), str(t.get("team_id")))
                   for t in (teams or candidates)}
        players = [{"ref": p["ref"], "name": p["name"], "team": p.get("team", ""),
                    "number": p.get("number", ""), "excluded": p["excluded"],
                    # ФИО не храним, поэтому «кто это» решается только ссылкой
                    # на профиль в лиге — особенно для тех, кто выпал из заявки
                    # и подписан номером.
                    "off_roster": bool(p.get("off_roster")),
                    "games": (p.get("stats") or {}).get("games", 0),
                    "profiles": _profile_links(p["ref"])}
                   for p in _pool_with_stats(await build_pool(season=s), s)]
        players.sort(key=lambda p: (p["excluded"], p["name"]))
        seasons.append({
            "id": s["id"], "name": s["name"], "format": s.get("format", "3x3"),
            "roster_size": fantasy.roster_size(s),
            "max_per_player": fantasy.max_per_player(s),
            "weights": fantasy.season_weights(s),
            "scopes": scopes,
            "scopes_title": fantasy.scopes_title(scopes),
            "manual_scopes": bool(fantasy.season_scopes(s)),
            "pool_teams": teams,
            "manual_pool": bool(teams),
            "modes": fantasy_modes.settings(s),
            "prices": fantasy_prices.describe(s),
            "all_modes": [{"id": m, "title": fantasy_modes.MODE_TITLES[m]}
                          for m in fantasy_modes.ALL_MODES],
            # Команды-кандидаты с отметкой «в пуле» — чтобы команду можно было
            # и убрать, и вернуть, а не только увидеть список выбранных.
            "teams": [{**t, "in_pool": (str(t.get("source")), str(t.get("team_id"))) in in_pool}
                      for t in candidates],
            "players": players,
        })
    avail = await available_scopes()
    for s_ in seasons:
        keys = {fantasy._scope_key(x) for x in s_["scopes"]}
        s_["can_add"] = [a for a in avail if fantasy._scope_key(a) not in keys]
    return web.json_response({"seasons": seasons, "available": avail,
                              "weight_keys": list(fantasy_stats.DEFAULT_WEIGHTS)})


async def handle_admin_action(request: web.Request) -> web.Response:
    """Действия админки. Все — с явным season_id, чтобы правка попадала именно
    в ту лигу, которую админ видит на экране."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _is_admin(user):
        return web.json_response({"error": "forbidden"}, status=403)
    try:
        body = await request.json()
    except (json.JSONDecodeError, TypeError):
        return web.json_response({"error": "bad_request"}, status=400)

    action = str(body.get("action") or "")
    sid = body.get("season_id")
    try:
        sid = int(sid) if sid is not None else None
    except (TypeError, ValueError):
        return web.json_response({"error": "bad_season"}, status=400)

    if action == "start":
        name = str(body.get("name") or "").strip()
        if not name:
            return web.json_response({"error": "no_name"}, status=400)
        fantasy.start_season(name, str(body.get("format") or "3x3"))
    elif sid is None:
        return web.json_response({"error": "no_season"}, status=400)
    elif action == "format":
        fantasy.set_format(str(body.get("value") or "3x3"), season_id=sid)
    elif action == "max_per":
        try:
            fantasy.set_max_per_player(int(body.get("value")), season_id=sid)
        except (TypeError, ValueError):
            return web.json_response({"error": "bad_value"}, status=400)
    elif action == "weights":
        w = body.get("value")
        if not isinstance(w, dict):
            return web.json_response({"error": "bad_value"}, status=400)
        fantasy.set_weights(w, sid)
    elif action == "scope_toggle":
        scope = body.get("value")
        if not isinstance(scope, dict):
            return web.json_response({"error": "bad_value"}, status=400)
        fantasy.toggle_season_scope(scope, season_id=sid)
    elif action == "pool_add":
        # Команды пула — на сезон. Отсюда и масштабирование: фэнтези для чужой
        # команды или для дивизиона — это просто сезон с другими командами.
        team = body.get("value")
        if not isinstance(team, dict) or not team.get("team_id"):
            return web.json_response({"error": "bad_value"}, status=400)
        season = fantasy._get_season(sid)
        teams = [t for t in await _current_pool_teams(season)
                 if str(t.get("team_id")) != str(team["team_id"])] + [team]
        fantasy.set_pool_teams(teams, sid)
        _pool_cache.pop(str(sid), None)
    elif action == "pool_remove":
        season = fantasy._get_season(sid)
        tid = str(body.get("value") or "")
        fantasy.set_pool_teams(
            [t for t in await _current_pool_teams(season) if str(t.get("team_id")) != tid], sid)
        _pool_cache.pop(str(sid), None)
    elif action == "prices_recalc":
        # Ручной прогон того же пересчёта, что идёт после игры: удобно, когда
        # тренер только что поправил цены и хочет увидеть, что выйдет.
        res = await asyncio.get_running_loop().run_in_executor(
            None, lambda: fantasy_prices.recalc(fantasy._get_season(sid),
                                                dry_run=bool(body.get("value"))))
        out = await handle_admin_state(request)
        payload = json.loads(out.body.decode())
        payload["recalc"] = res
        return web.json_response(payload)
    elif action in ("rank_up_games", "rank_down_games", "price_step"):
        try:
            fantasy._update_settings(fantasy._get_season(sid), **{action: int(body.get("value"))})
        except (TypeError, ValueError):
            return web.json_response({"error": "bad_value"}, status=400)
    elif action == "mode_toggle":
        # Режимы включаются набором: можно оставить один, можно дать выбор.
        # Пустой набор запрещаем — иначе фэнтези становится нечем играть.
        season = fantasy._get_season(sid)
        cur = fantasy_modes.enabled(season)
        want = str(body.get("value") or "")
        if want not in fantasy_modes.ALL_MODES:
            return web.json_response({"error": "bad_value"}, status=400)
        new_modes = [m for m in cur if m != want] if want in cur else cur + [want]
        if not new_modes:
            return web.json_response({"error": "need_one_mode"}, status=400)
        order = {m: i for i, m in enumerate(fantasy_modes.ALL_MODES)}
        fantasy._update_settings(season, modes=sorted(set(new_modes), key=order.get))
    elif action == "budget":
        try:
            fantasy._update_settings(fantasy._get_season(sid), budget=max(1, int(body.get("value"))))
        except (TypeError, ValueError):
            return web.json_response({"error": "bad_value"}, status=400)
    elif action == "multiplier":
        try:
            fantasy._update_settings(fantasy._get_season(sid),
                                     cat_multiplier=max(1.0, float(body.get("value"))))
        except (TypeError, ValueError):
            return web.json_response({"error": "bad_value"}, status=400)
    elif action == "player_toggle":
        # Игрока убираем по ИМЕНИ: у него бывает по id в каждой лиге, а решение
        # админа — про человека, а не про строчку ростера. Уже набранные на нём
        # очки зафиксированы снимками по играм и не пропадают.
        ref = str(body.get("value") or "")
        season = fantasy._get_season(sid)
        entry = next((p for p in await build_pool(season=season) if p["ref"] == ref), None)
        if not entry:
            return web.json_response({"error": "unknown_player"}, status=404)
        fantasy.toggle_pool_exclude_name(entry["name"], season_id=sid)
    elif action == "finish":
        fantasy.end_season(sid)
    else:
        return web.json_response({"error": "unknown_action"}, status=400)

    return await handle_admin_state(request)


# ─── payload запасного входа (постоянная кнопка в Telegram) ──────────────────
#
# Живой вход (кнопка меню -> живой API) не работает у части игроков: то сеть до
# ts.net не пускает, то ник в таблице не совпал. Запасной вход — постоянная
# reply-кнопка, в URL которой бот запекает данные (#d=base64url(JSON)), а
# сохранение состава уходит обратно через Telegram sendData. Так вход не зависит
# от доступности живого API. Данные едут ПРИВАТНО в личном чате игрока, в
# публичный репозиторий ничего не коммитится — ФИО-инвариант соблюдён.
#
# Гарантия «без удвоения»: и живой вход, и sendData сохраняют состав под ОДНИМ
# числовым telegram id, а fantasy_rosters уникальна по (user_id, сезон, тур).
# Через какой вход игрок ни зайди — это один участник, один состав, один счёт.


def _compress_pool(enriched: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ужимаем обогащённый пул до коротких ключей — payload едет в URL кнопки,
    длину надо экономить. Исключённых игроков не кладём: запасной вход всегда
    не-админ, вернуть их в нём всё равно нельзя, а выбрать их не должно быть
    можно (сервер исключение по имени не проверяет, фильтр только на фронте)."""
    out: List[Dict[str, Any]] = []
    for p in enriched:
        if p.get("excluded"):
            continue
        st = p.get("stats") or {}
        lg = p.get("last") or {}
        s = ({"g": st.get("games", 0), "p": st.get("pts", 0), "rb": st.get("reb", 0),
              "a": st.get("ast", 0), "s": st.get("stl", 0), "b": st.get("blk", 0),
              "t": st.get("tur", 0), "f": st.get("fp", 0),
              "lf": lg.get("fp"), "ld": lg.get("date")} if st else {})
        # "n" — игровой номер: в бюджетном режиме он стоит рядом с фамилией
        # (в кружке — цена), и без него запасной вход показывал бы игроков
        # без номеров вовсе. Пустые не кладём — payload и так на пределе.
        item = {"r": p["ref"], "m": p["name"], "s": s,
                "c": p.get("price", 0), "t": p.get("tier", "")}
        if p.get("number"):
            item["n"] = p["number"]
        out.append(item)
    return out


async def webapp_shared() -> Optional[Dict[str, Any]]:
    """Общая (одинаковая для всех игроков) часть payload: сезон, пул со
    статистикой, таблица, окно набора. Считаем один раз и переиспользуем в
    рассылке — у каждого игрока меняется только его собственный состав."""
    season = fantasy.get_active_season()
    if not season:
        return None
    pool = _compress_pool(_pool_with_stats(await build_pool(season=season), season))
    week_start, sched_locked = fantasy.active_selection(season)
    # Запасной вход отдаёт таблицу ПЕРВОГО включённого режима: payload и так
    # на пределе, а класть в кнопку три таблицы — верный способ снова упереться
    # в лимит клавиатуры. Живой вход показывает все.
    table = fantasy.season_standings_live(season["id"],
                                          mode=fantasy_modes.enabled(season)[0])
    names = fantasy.display_names([str(r["user_id"]) for r in table])
    standings = [{"name": names.get(str(r["user_id"]), "Участник"),
                  "points": r["points"], "history": r.get("history", [])} for r in table]
    return {
        "season_id": season["id"],
        "season": {"name": season["name"], "format": season["format"],
                   "roster_size": fantasy.roster_size(season),
                   "max_per_player": fantasy.max_per_player(season),
                   "weights": fantasy.season_weights(season),
                   "modes": fantasy_modes.describe(season),
                   "ranks": fantasy_prices.describe(season)},
        "pool": pool,
        "week_start": week_start,
        "sched_locked": sched_locked,
        "standings": standings,
    }


# Сколько символов payload помещается в кнопку. Telegram отвергает слишком
# длинную клавиатуру («Reply markup is too long»), и с ростом сезона мы в этот
# предел упёрлись: полный payload дорос до 26 КБ и кнопка перестала уходить
# ВООБЩЕ (в журнале с 26.07). Поэтому режем данные до бюджета, а не надеемся.
PAYLOAD_LIMIT = int(os.getenv("WEBAPP_PAYLOAD_LIMIT", "8000"))


def _payload_variants(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Ступени урезания — от самой полной к самой скромной.

    Порядок по полезности для запасного входа: там собирают состав, поэтому
    статистика игроков важнее таблицы, а история очков в таблице — вообще
    самое тяжёлое (10 участников × 20 игр ≈ 18 КБ из 26)."""
    lite_standings = [{k: v for k, v in s.items() if k != "history"}
                      for s in data.get("standings") or []]
    # Без статистики, но С ЦЕНОЙ, уровнем и номером: в бюджетном режиме цена —
    # это правила игры, без неё экран бесполезен, а статистика лишь помогает
    # выбирать. Раньше следующая ступень выкидывала всё сразу, и запасной вход
    # показывал голый список фамилий.
    # Уровень («t») тоже выкидываем: он ОДНОЗНАЧНО следует из цены, а полосы
    # рангов уже едут в season.ranks — фронт выведет значок сам.
    nostat_pool = [{k: v for k, v in p.items() if k not in ("s", "t")}
                   for p in data.get("pool") or []]
    bare_pool = [{"r": p.get("r"), "m": p.get("m")} for p in data.get("pool") or []]
    return [
        data,
        {**data, "standings": lite_standings},
        {**data, "standings": []},
        {**data, "standings": [], "pool": nostat_pool},
        {**data, "standings": [], "pool": bare_pool},
    ]


def _encode(data: Dict[str, Any]) -> str:
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def encode_webapp_payload(shared: Dict[str, Any], user_id: str,
                          max_len: Optional[int] = None) -> str:
    """Персональный payload = общая часть + состав игрока, base64url для #d=.
    Состав берём из БД по числовому id — тому же, под которым пишет живой вход,
    поэтому оба входа показывают один состав, а очки не задваиваются.

    Не влезает в бюджет — отдаём урезанную версию (см. _payload_variants), а не
    полную: лучше кнопка без статистики, чем никакой кнопки."""
    # Состав держится, пока игрок его не поменял, — значит и в запасном входе
    # показываем унаследованный. Иначе офлайн-игрок видел бы пустой экран и
    # думал, что состав слетел (в живом API он при этом есть).
    r = fantasy.get_roster_effective(str(user_id), shared["season_id"], shared["week_start"])
    data = {
        "season": shared["season"],
        "pool": shared["pool"],
        "roster": r["refs"] if r else [],
        "inherited": bool(r.get("inherited")) if r else False,
        "locked": shared["sched_locked"] or (bool(r["locked"]) if r else False),
        "week_start": shared["week_start"],
        "standings": shared["standings"],
    }
    budget = PAYLOAD_LIMIT if max_len is None else max_len
    if budget <= 0:
        return _encode(data)
    encoded = ""
    for variant in _payload_variants(data):
        encoded = _encode(variant)
        if len(encoded) <= budget:
            return encoded
    log.warning(f"payload кнопки не влез в {budget}: даже урезанный {len(encoded)}")
    return encoded


async def build_webapp_payload(user_id: str, max_len: Optional[int] = None) -> Optional[str]:
    """Payload запасного входа для одного игрока. None — нет активного сезона."""
    shared = await webapp_shared()
    return encode_webapp_payload(shared, user_id, max_len=max_len) if shared else None


async def handle_pool(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    pool = _pool_with_stats(await build_pool(season=season), season)
    # Список активных лиг — для переключателя в Mini App (когда их несколько).
    seasons = [{"id": s["id"], "name": s["name"], "format": s["format"]}
               for s in fantasy.active_seasons()]
    return web.json_response({
        "season": season and {"id": season["id"], "name": season["name"], "format": season["format"],
                              "roster_size": fantasy.roster_size(season),
                              "max_per_player": fantasy.max_per_player(season),
                              # Веса — чтобы экран правил показывал настоящие
                              # начисления сезона, а не переписанный текст.
                              "weights": fantasy.season_weights(season),
                              "modes": fantasy_modes.describe(season),
                              "ranks": fantasy_prices.describe(season)},
        "seasons": seasons,
        "pool": pool,
        "member": _is_team_member(str(user.get("id")), user.get("username", "")),
        "admin": _is_admin(user),
        # Нашёлся ли сам человек среди игроков пула — от этого зависит, есть ли
        # у него личный кабинет. Участник фэнтези и игрок команды — не одно и
        # то же: играть может и тот, кто сам на площадку не выходит.
        "player": bool(await _my_card(user, season)),
    })


async def handle_get_roster(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    if not season:
        return web.json_response({"roster": None, "week_start": None})
    week_start, locked = fantasy.active_selection(season)
    # Состав держится, пока игрок его не поменял: на новой неделе показываем
    # унаследованный, а не пустой — иначе кажется, что состав слетел.
    r = fantasy.get_roster_effective(str(user["id"]), season["id"], week_start)
    det = fantasy.lock_details() if locked else {}
    return web.json_response({
        "roster": r["refs"] if r else [],
        "mode": (r.get("mode") if r else "") or fantasy_modes.default_mode(season),
        "cats": ((r.get("meta") or {}).get("cats") if r else []) or [],
        # перенесён с прошлого раза, а не собран заново — покажем это игроку
        "inherited": bool(r.get("inherited")) if r else False,
        # блокировка — по идущей игре (расписание), даже если своей записи ещё нет
        "locked": locked or (bool(r["locked"]) if r else False),
        "locked_since": det.get("started_hhmm", ""),
        "week_start": week_start,
    })


async def handle_save_roster(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    uid = str(user["id"])
    if not _is_team_member(uid, user.get("username", "")):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    if not season:
        return web.json_response({"error": "no_active_season"}, status=400)

    try:
        body = await request.json()
        refs = body.get("refs") or []
        mode = fantasy_modes.normalize(season, body.get("mode"))
        cats = body.get("cats") or []
    except (json.JSONDecodeError, TypeError):
        return web.json_response({"error": "bad_request"}, status=400)
    meta = {"cats": list(cats)} if mode == fantasy_modes.CATEGORY else {}

    week_start, locked = fantasy.active_selection(season)
    if locked:
        # Игрок узнаёт о блокировке здесь (рассылок про неё больше нет), поэтому
        # отдаём подробности: с какого времени и почему.
        det = fantasy.lock_details()
        return web.json_response({"error": "locked", "since": det.get("started_hhmm", "")},
                                 status=409)

    all_pool = await build_pool(season=season)
    pool_refs = {p["ref"] for p in all_pool}
    prices = {p["ref"]: p.get("price", 0)
              for p in _pool_with_stats(all_pool, season)}
    err = fantasy.validate_roster(season, refs, pool_refs, mode=mode, meta=meta, prices=prices)
    if err:
        return web.json_response(
            {"error": err, "expected": fantasy_modes.roster_size(season, mode),
             "budget": fantasy_modes.settings(season)["budget"],
             "cost": fantasy_modes.cost(refs, prices)}, status=400)
    # Убранных админом игроков брать нельзя.
    excluded_names = set(fantasy.pool_excluded_names(season))
    by_ref = {p["ref"]: p for p in all_pool}
    # Состав мог прийти со старой формой ссылки (кешированное приложение) —
    # сверяем по канонической.
    if any(fantasy.norm_player_name(
            by_ref.get(fantasy.canonical_ref(r, by_ref) or r, {}).get("name", "")) in excluded_names
           for r in refs):
        return web.json_response({"error": "excluded_player"}, status=400)

    res = fantasy.save_roster(uid, season["id"], week_start, refs, mode=mode, meta=meta)
    if not res.get("ok"):
        return web.json_response({"error": res.get("error", "save_failed")}, status=409)
    return web.json_response({"ok": True, "week_start": week_start})


_teams_cache: Dict[str, Any] = {"at": 0.0, "key": "", "data": {}}


async def _team_names(scopes: List[Dict[str, Any]]) -> Dict[str, str]:
    """team_id -> название, транзитно из публичных таблиц SLPRO-стадий в scope.
    В нашу базу названия не пишем (там только id) — подтягиваем на лету и
    кешируем на TTL пула."""
    slpro = [s for s in (scopes or []) if s.get("source") == fantasy_stats.SOURCE_SLPRO]
    if not slpro:
        return {}
    key = ";".join(sorted(f"{s.get('season_id')}:{s.get('stage_id')}" for s in slpro))
    now = time.time()
    if _teams_cache["key"] == key and (now - _teams_cache["at"]) < _POOL_TTL:
        return _teams_cache["data"]
    names: Dict[str, str] = {}
    try:
        from slpro_client import SlproClient
        client = SlproClient()
        stages = await client.iter_stages()      # в scope нет division_id — по стадии
        for sc in slpro:
            for st in stages:
                if (str(st["season_id"]) == str(sc.get("season_id"))
                        and str(st["stage_id"]) == str(sc.get("stage_id"))):
                    for t in await client.get_standings(st):
                        names[str(t.get("team_id"))] = t.get("name", "")
                    break
    except Exception as e:
        log.warning(f"фэнтези-API: не удалось получить названия команд: {e}")
        return {}
    _teams_cache.update(at=now, key=key, data=names)
    return names


async def handle_player(request: web.Request) -> web.Response:
    """Карточка игрока: посещаемость, пропуски, статистика за турнир, журнал."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    ref = request.query.get("ref", "")
    pool = await build_pool()
    entry = next((p for p in pool if p["ref"] == ref), None)
    if not entry:
        return web.json_response({"error": "unknown_player"}, status=404)

    season = _season(request)
    weights = fantasy.season_weights(season) if season else fantasy_stats.DEFAULT_WEIGHTS
    scopes = fantasy.effective_scopes(season) if season else []
    profile = fantasy_stats.player_profile(ref, scopes, weights)

    names = await _team_names(scopes)
    if profile.get("last") and profile["last"].get("opponent_id") is not None:
        profile["last"]["opponent"] = names.get(str(profile["last"]["opponent_id"]), "Соперник")
    profile["name"] = entry["name"]          # транзитно, как и в пуле
    profile["number"] = entry.get("number", "")
    # Турнир в шапке — только по источникам игрока (у составного игрока их два).
    psrcs = {fantasy_stats.parse_ref(lr)[0] for lr in fantasy_stats.expand_refs([ref])}
    own = [s for s in scopes if s.get("source") in psrcs]
    profile["tournament"] = fantasy.scopes_title(own or scopes)
    return web.json_response(profile)


def _my_player_row(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Строка листа «Игроки», закреплённая за этим Telegram id."""
    link = sheets_cache.get_player_link(str(user.get("id")))
    if not link:
        return None
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM players WHERE row_index = ?",
                           (int(link["player_row"]),)).fetchone()
    return dict(row) if row else None


async def _card_for_ref(ref: str, season: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return next((p for p in await build_pool(season=season) if p["ref"] == ref), None)


async def _my_card(user: Dict[str, Any],
                   season: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Карточка пула, соответствующая человеку.

    Мостик тот же, что и у цены: строка листа -> ФИО -> карточка пула, с
    поправкой на разное написание в лигах. Своего id в лиге человек нам не
    сообщал, а ФИО в листе ведёт тренер — это единственная связка."""
    row = _my_player_row(user)
    if not row:
        return None
    key = _price_key(f"{row.get('surname', '')} {row.get('name', '')}")
    if not key.strip():
        return None
    pool = await build_pool(season=season)
    exact = next((p for p in pool if _price_key(p["name"]) == key), None)
    if exact:
        return exact
    # Одна буква разницы («Лысюк»/«Лисюк») — тот же человек.
    parts = key.split()
    if len(parts) < 2:
        return None
    sur, first = parts[0], parts[1]
    for p in pool:
        o = _price_key(p["name"]).split()
        if len(o) >= 2 and ((o[0] == sur and _lev1(first, o[1]))
                            or (o[1] == first and _lev1(sur, o[0]))):
            return p
    return None


async def handle_me(request: web.Request) -> web.Response:
    """Личный кабинет игрока: ранг, форма, что нужно для подъёма и падения.

    Только для опознанных игроков команды — как и всё остальное в фэнтези.
    Админ может открыть чужой кабинет (?ref=…): вопросы «а почему у меня так»
    приходят ему, и отвечать вслепую невозможно."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    ref = request.query.get("ref", "")
    if ref and not _is_admin(user):
        return web.json_response({"error": "forbidden"}, status=403)

    card = await _card_for_ref(ref, season) if ref else await _my_card(user, season)
    if not card:
        return web.json_response({
            "found": False,
            "reason": "no_player" if not ref else "unknown_player",
            "admin": _is_admin(user)})

    enriched = _pool_with_stats([card], season)[0]
    data = fantasy_prices.progress(enriched.get("price"), [card["ref"]], season)
    data.update({"ref": card["ref"], "name": card["name"],
                 "number": card.get("number", ""), "tier": enriched.get("tier", ""),
                 "stats": enriched.get("stats", {}), "admin": _is_admin(user),
                 "mine": not ref})
    return web.json_response(data)


async def handle_standings(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    if not season:
        return web.json_response({"standings": []})
    # Таблица — суммарные очки за всю лигу (не за неделю): не «пропадает» при
    # смене недели. История по турам — в каждой строке, для тапа.
    #
    # У каждого режима таблица своя: правила разные, и общий зачёт сравнивал бы
    # несравнимое. Отдаём все включённые сразу — переключение вкладок на фронте
    # не должно ходить в сеть.
    enabled = fantasy_modes.enabled(season)
    tables = {}
    for m in enabled:
        rows = fantasy.season_standings_live(season["id"], mode=m)
        names = fantasy.display_names([r["user_id"] for r in rows])
        for r in rows:
            r["name"] = names.get(str(r["user_id"]), "")
        tables[m] = rows
    # standings — таблица режима, в котором играет сам участник: старые версии
    # приложения знают только это поле.
    roster = fantasy.get_roster_effective(str(user["id"]), season["id"],
                                          fantasy.active_selection(season)[0]) or {}
    mine = fantasy_modes.normalize(season, roster.get("mode"))
    return web.json_response({"standings": tables.get(mine, tables.get(enabled[0], [])),
                              "tables": tables, "my_mode": mine,
                              "modes": fantasy_modes.describe(season)})


def create_app(bot_token: str) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["bot_token"] = bot_token
    app.add_routes([
        web.get("/fantasy/pool", handle_pool),
        web.get("/fantasy/roster", handle_get_roster),
        web.post("/fantasy/roster", handle_save_roster),
        web.get("/fantasy/player", handle_player),
        web.get("/fantasy/me", handle_me),
        web.get("/fantasy/standings", handle_standings),
        web.get("/fantasy/top", handle_top),
        web.get("/fantasy/admin", handle_admin_state),
        web.post("/fantasy/admin", handle_admin_action),
        web.get("/health", lambda r: web.json_response({"ok": True})),
    ])
    return app
