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
import league_sync
import player_names

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


_players_reread_at = 0.0
PLAYERS_REREAD_EVERY = 300      # секунд между перечитываниями листа


def _reread_players() -> None:
    """Перечитывает лист «Игроки», но не чаще раза в пять минут.

    Тренер меняет @ники прямо в таблице, а зеркало обновлялось по расписанию —
    человек уже исправлен, а бот всё ещё отвечает «тебя нет в составе». Ждать
    следующей синхронизации в такой момент невыносимо, ходить в Google на
    каждый запрос — тоже нельзя."""
    global _players_reread_at
    now = time.time()
    if now - _players_reread_at < PLAYERS_REREAD_EVERY:
        return
    _players_reread_at = now
    try:
        import report_common
        book = report_common.init_sheets()
        if book is not None:
            sheets_cache.sync_players(book)
            log.info("Лист «Игроки» перечитан: ищем изменившиеся ники")
    except Exception as exc:
        log.warning(f"Перечитать лист «Игроки» не вышло: {exc}")


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
            _push_tg_id_to_sheet(row["row_index"], uid, uname, _row_name(conn, row["row_index"]))
            return True
        # 3. Первое знакомство: ищем строку по нику — но только СВОБОДНУЮ.
        if not uname:
            return False
        cand = conn.execute(
            """SELECT row_index FROM players
               WHERE lower(ltrim(telegram_id, '@')) = ? LIMIT 1""", (uname,)).fetchone()
    if not cand:
        # Ника нет в зеркале — возможно, тренер только что его поправил в
        # листе. Перечитываем лист (не чаще раза в пять минут) и пробуем ещё
        # раз: иначе человек слышит «тебя нет в составе» до следующей плановой
        # синхронизации, хотя он там уже есть.
        _reread_players()
        with sheets_cache.get_connection() as conn:
            cand = conn.execute(
                """SELECT row_index FROM players
                   WHERE lower(ltrim(telegram_id, '@')) = ? LIMIT 1""",
                (uname,)).fetchone()
    if not cand:
        return False
    if sheets_cache.is_row_linked(cand["row_index"]):
        log.warning(f"фэнтези: ник @{uname} совпал со строкой {cand['row_index']}, "
                    f"но она уже закреплена за другим id — отказ")
        return False
    if not sheets_cache.link_player(uid, uname, cand["row_index"]):
        return False
    log.info(f"фэнтези: @{uname} закреплён за строкой {cand['row_index']} (id {uid})")
    _push_tg_id_to_sheet(cand["row_index"], uid, uname, _row_name(None, cand["row_index"]))
    return True


def _row_name(conn: Any, player_row: int) -> str:
    """ФИО из строки зеркала — чтобы запись в лист могла себя проверить."""
    def fetch(c):
        r = c.execute("SELECT surname, name FROM players WHERE row_index = ?",
                      (int(player_row),)).fetchone()
        return f"{r['surname']} {r['name']}".strip() if r else ""
    if conn is not None:
        return fetch(conn)
    sheets_cache.init_db()
    with sheets_cache.get_connection() as c:
        return fetch(c)


def _push_tg_id_to_sheet(player_row: int, tg_user_id: str, username: str = "",
                         expect: str = "") -> None:
    """Best-effort: показать в листе числовой id и актуальный @ник. Доступ живёт
    в локальной player_links, поэтому недоступность Sheets вход не ломает."""
    try:
        from collect_votes import _init_sheets
        ss = _init_sheets()
        sheets_cache.write_player_tg_id(ss, player_row, tg_user_id, expect)
        # Ник в таблице устаревает — обновляем на тот, под которым человек
        # реально пришёл. Иначе в листе остаётся адрес, которого больше нет.
        sheets_cache.write_player_nickname(ss, player_row, username, expect)
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
        import league_sync
        import slpro_client
        seen_ids = set()
        # Сначала зеркало (без сети), живой запрос — только если оно пусто.
        contexts = [t["ctx"] for t in league_sync.our_teams("slpro") if t.get("ctx")]
        if not contexts:
            contexts = await slpro_client.team_contexts()
        for ctx in contexts:
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
    """Название команды Инфобаскета: сперва из локального справочника лиг,
    и только если его там нет — живым запросом (с кешем на процесс).

    Справочник наполняет league_sync, он же держит его свежим. Живой запрос
    здесь стоил админке пары секунд на ровном месте, а при недоступной лиге —
    всего её таймаута."""
    key = f"{team_id}:{comp_id}"
    if key not in _ib_names:
        try:
            import league_sync
            local = next((t for t in league_sync.our_teams("infobasket")
                          if str(t.get("team_id")) == str(team_id) and t.get("name")), None)
            if local:
                _ib_names[key] = local["name"]
                return _ib_names[key]
        except Exception as e:
            log.warning(f"справочник команд Инфобаскета: {e}")
        try:
            import stats_backfill
            info = await stats_backfill.fetch_infobasket_team(team_id, comp_id)
            _ib_names[key] = info.get("name") or f"Команда {team_id}"
        except Exception as e:
            log.warning(f"название команды Инфобаскета {key}: {e}")
            return f"Команда {team_id}"
    return _ib_names[key]


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


def _resolve_pool_teams_local(season: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Команды пула — из локального справочника, без сети.

    Явный выбор админа (fantasy.pool_teams) хранится в сезоне; если админ
    ничего не выбирал, берём наши команды, которые записала качалка."""
    if season is None:
        season = fantasy.get_active_season()
    explicit = fantasy.pool_teams(season) if season else []
    if explicit:
        return explicit
    return [{"source": t["source"], "team_id": t["team_id"], "comp_id": t["comp_id"],
             "name": t["name"], "league": t["league"]} for t in league_sync.our_teams()]


async def _resolve_pool_teams(season: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Команды пула сезона: явный выбор админа (fantasy.pool_teams) или
    дефолт — кандидаты из справочника. Живой вариант остался для админки:
    там уместно спросить лигу, когда команду только настраивают."""
    if season is None:
        season = fantasy.get_active_season()
    explicit = fantasy.pool_teams(season) if season else []
    if explicit:
        return explicit
    local = _resolve_pool_teams_local(season)
    return local if local else await derive_pool_teams()


def pool_is_warm(season: Optional[Dict[str, Any]] = None) -> bool:
    """Готов ли пул прямо сейчас, без похода в лиги.

    Нужно тем, кто отвечает человеку: собрать пул — это сходить в API двух лиг,
    и если одна из них молчит, ожидание измеряется минутами. Такой ценой
    запасные данные в кнопке не нужны — лучше отдать её пустой и согреть пул
    фоном."""
    if season is None:
        season = fantasy.get_active_season()
    entry = _pool_cache.get(str((season or {}).get("id") or "-"))
    return bool(entry and (time.time() - entry["at"]) < _POOL_TTL)


async def build_pool(force: bool = False,
                     season: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Пул драфта — ЦЕЛИКОМ ИЗ ЛОКАЛЬНОЙ БАЗЫ, без единого запроса в лиги.

    Ref = source:team_id:player_id. Состав берём из зеркала заявок
    (`league_rosters`) и своих протоколов, имена — из памяти (player_names),
    цены — из зеркала листа «Игроки». Наполняет всё это league_sync, фоном.

    Так сделано после 31.07.2026: раньше пул собирался походом в API двух лиг,
    и когда одна из них замолчала, каждое действие игрока стало стоить минуту
    таймаутов. В ответе человеку живых запросов быть не должно — лига
    недоступна, значит работаем на скачанном вчера.

    Функция осталась async: её зовут из обработчиков, и менять полсотни мест
    ради синхронности незачем. Кеш в памяти на час и ОТДЕЛЬНО НА СЕЗОН:
    параллельные лиги набираются из разных команд, общий кеш показывал бы всем
    пул той лиги, которая обновилась последней."""
    if season is None:
        season = fantasy.get_active_season()
    key = str((season or {}).get("id") or "-")
    now = time.time()
    entry = _pool_cache.get(key)
    if not force and entry and (now - entry["at"]) < _POOL_TTL:
        return entry["data"]

    teams = _resolve_pool_teams_local(season)
    if not teams:
        # Справочник ещё пуст — так бывает ровно один раз, до первого прохода
        # качалки. Тут поход в лигу оправдан: пустой пул означал бы приложение
        # без единого игрока, а это хуже секундного ожидания.
        log.info("пул: локальный справочник команд пуст — спрашиваю лигу разово")
        teams = await derive_pool_teams()

    pool: List[Dict[str, Any]] = []
    seen = set()
    for team in teams:
        src = team.get("source")
        tid = team.get("team_id")
        if tid is None:
            continue
        src_db = "slpro" if src == "slpro" else "infobasket"
        pref = "slpro" if src == "slpro" else "ib"

        # Пул = заявка ∪ протоколы. Заявка даёт новичков, ещё не игравших;
        # протоколы — тех, кто реально играл, но из заявки уже выпал.
        by_pid: Dict[str, Dict[str, Any]] = {}
        for r in league_sync.roster_of(src_db, tid):
            if not r.get("active", 1):
                continue
            by_pid[str(r["player_id"])] = {"pid": str(r["player_id"]),
                                           "number": r.get("number") or ""}
        for pp in _protocol_players(src_db, tid):
            pid = str(pp["pid"])
            if pid in by_pid:
                if not by_pid[pid].get("number"):
                    by_pid[pid]["number"] = pp["number"]   # номер из протокола
                continue
            by_pid[pid] = {"pid": pid, "number": pp["number"], "off_roster": True}

        # Имя — из памяти. Нет его (демон только поднялся, качалка ещё не
        # отработала) — показываем номер: ждать лигу тут нельзя.
        for p in by_pid.values():
            p["name"] = (player_names.get(src_db, p["pid"])
                         or (f"№{p['number']}" if p["number"] else f"ID {p['pid']}"))

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


# ─────────────────── Заявка тренера на конкретную игру ──────────────────────
#
# Пул собран по заявкам лиг и адресуется id лиги, а состав на игру тренер ведёт
# строками листа «Игроки». Общего ключа между ними нет — сводим по ФИО, тем же
# способом, каким уже склеены один человек из SLPRO и из Инфобаскета.
#
# Сойдётся не всё: лига пишет «Шлепикас Ромас», лист — «Шлепикас Роман». Кого
# не опознали, ПОКАЗЫВАЕМ с пометкой и без возможности поставить: молча убрать
# человека из состава — значит заставить участника гадать, почему его нет.


def _declared_names(source: str, game_id: str) -> List[Dict[str, Any]]:
    """Кого тренер заявил на игру: [{row, title}]. Пусто — состав не собран."""
    import game_roster
    return [{"row": int(p["row"]), "title": str(p.get("title") or "").strip()}
            for p in game_roster.roster(source, str(game_id))
            if str(p.get("title") or "").strip()]


def _same_person(a: str, b: str) -> bool:
    """Одно ФИО с точностью до буквы и порядка слов.

    Порядок разный (лига пишет «Имя Фамилия», лист — «Фамилия Имя»), а буква
    расходится в написании. Требуем совпадения ВСЕХ слов: «Долгих Денис» и
    «Долгих Владислав» — братья, и путать их нельзя."""
    pa, pb = sorted(_norm_name(a).split()), sorted(_norm_name(b).split())
    if not pa or len(pa) != len(pb):
        return False
    return all(x == y or _lev1(x, y) for x, y in zip(pa, pb))


async def game_pool(source: str, game_id: str,
                    season: Optional[Dict[str, Any]] = None,
                    pool: Optional[List[Dict[str, Any]]] = None
                    ) -> List[Dict[str, Any]]:
    """Пул для ставки на конкретную игру — только заявленные тренером.

    Возвращает карточки пула с полем `declared`, плюс тех из состава, кого в
    пуле не нашли, — с `unlinked` и без ссылок: поставить на них нельзя.

    `pool` передают уже ОБОГАЩЁННЫМ статистикой: иначе экран матча считал бы
    агрегаты заново для своих двенадцати человек, хотя те же числа только что
    посчитаны для полного ростера. Сначала обогащаем целое, потом отбираем
    часть — не наоборот."""
    declared = _declared_names(source, str(game_id))
    if pool is None:
        pool = await build_pool(season=season)
    # Состава ещё нет — ставят из полного ростера лиги. Пустой список тут
    # означал бы «игры нет», а игра есть: тренер её объявил.
    if not declared:
        return [dict(e) for e in pool]
    out: List[Dict[str, Any]] = []
    taken: set = set()
    for person in declared:
        hit = next((e for e in pool
                    if id(e) not in taken and _same_person(e["name"], person["title"])), None)
        if hit is None:
            out.append({"refs": [], "name": person["title"], "number": "",
                        "team": "", "leagues": [], "row": person["row"],
                        "declared": True, "unlinked": True})
            continue
        taken.add(id(hit))
        out.append({**hit, "row": person["row"], "declared": True,
                    "unlinked": False})
    return out


def upcoming_games(limit: int = 6) -> List[Dict[str, Any]]:
    """Все ближайшие игры, известные боту, — ставить можно на любую.

    Раньше сюда попадали только матчи с РАЗОСЛАННЫМ составом, и список у
    тренера, объявившего три игры, оставался пустым: состав он к тому моменту
    ещё не собрал. Заявка нужна, чтобы сузить пул, а не чтобы решать, есть
    игра или нет: пока состава нет, ставят из полного ростера лиги.

    `declared` — собран ли состав. По нему приложение объясняет человеку,
    почему список игроков полный."""
    import game_roster
    from datetime_utils import get_moscow_time
    today = get_moscow_time().date()
    out = []
    for g in game_roster.games(from_day=today):
        out.append({"source": g["source"], "game_id": g["game_id"],
                    "date": g["date"].isoformat(), "time": g.get("time") or "",
                    "opponent": g.get("opponent") or "",
                    "label": game_roster.game_label(g),
                    "declared": bool(_declared_names(g["source"], g["game_id"]))})
        if len(out) >= limit:
            break
    return out


async def push_lineup(source: str, game_id: str,
                      season: Optional[Dict[str, Any]] = None) -> None:
    """Отправляет в лига-бот текущую заявку тренера на игру.

    Зовётся при КАЖДОМ изменении состава: заявка там заменяется целиком, и
    пустой список означает «сняли», а не «нечего слать». Лига публикует свою
    заявку только к стартовому свистку, а тренер знает её накануне — этим
    вызовом фильтр «кто сегодня играет» включается заранее."""
    if source != "slpro":
        return
    try:
        import league_push
        if not league_push.enabled():
            return
        entries = await game_pool(source, str(game_id), season)
        refs: List[str] = []
        for e in entries:
            if e.get("ref"):
                refs.append(str(e["ref"]))
            refs.extend(str(r) for r in (e.get("refs") or []))
        await league_push.send_lineup(game_id, refs)
    except Exception as exc:
        log.warning("лига-бот: заявка на %s не отправлена — %s", game_id, exc)


async def picks_hit_by(source: str, game_id: str, player_row: int,
                       season: Optional[Dict[str, Any]] = None
                       ) -> List[Dict[str, Any]]:
    """Чьи ставки задевает снятие этого человека с игры: [{user_id, name}].

    Тренер снимает игрока строкой листа, а ставка адресуется id лиги — сводим
    по ФИО тем же способом, что и заявку. Никого не нашли (человека нет в
    лиге) — значит и в ставках его быть не могло, отвечаем пустым списком."""
    import coach_payments
    import fantasy
    if season is None:
        season = fantasy.get_active_season()
    if not season:
        return []
    person = coach_payments.player_by_row(int(player_row))
    title = str((person or {}).get("title") or "").strip()
    if not title:
        return []

    pool = await build_pool(season=season)
    hit = next((e for e in pool if _same_person(e["name"], title)), None)
    if not hit:
        return []
    refs = set(hit.get("refs") or ([hit["ref"]] if hit.get("ref") else []))
    if not refs:
        return []

    picks = await asyncio.to_thread(fantasy.game_picks_by_user, season["id"],
                                    source, str(game_id))
    return [{"user_id": uid, "name": hit["name"]}
            for uid, entry in picks.items()
            if refs & set(entry.get("refs") or [])]


def declared_where(games: List[Dict[str, Any]]) -> Dict[int, List[str]]:
    """{строка листа: в каких из этих игр человек заявлен} — подписи игр.

    Ради значка «играет ещё и там»: заявленный на два матча набирает очки в
    обоих, и участнику стоит знать об этом до того, как он выберет игру. А на
    экране «все игры» из этого же видно, что заявки по матчам разные."""
    where: Dict[int, List[str]] = {}
    for g in games:
        for person in _declared_names(g["source"], g["game_id"]):
            where.setdefault(person["row"], []).append(g["label"])
    return where


def declared_counts(games: List[Dict[str, Any]]) -> Dict[int, int]:
    """{строка листа: на скольких из этих игр человек заявлен}."""
    return {row: len(labels) for row, labels in declared_where(games).items()}


async def all_games_pool(games: List[Dict[str, Any]],
                         season: Optional[Dict[str, Any]] = None,
                         pool: Optional[List[Dict[str, Any]]] = None
                         ) -> Tuple[List[Dict[str, Any]], bool]:
    """Пул для ставки СРАЗУ НА ВСЕ игры плюс признак «заявки разные».

    Здесь показываем всех: один состав играет во всех матчах, и запрещать
    выбор по заявке одной игры было бы неверно — в другой этот человек есть.
    Но разметить обязаны: у кого в каких матчах он заявлен и совпадают ли
    заявки вообще. Иначе человек соберёт состав, половина которого выйдет
    только в одном матче из трёх, и не поймёт, почему очков мало."""
    if pool is None:
        pool = await build_pool(season=season)
    where = declared_where(games)
    if not where:
        return [dict(e) for e in pool], False

    # Заявки считаем разными, если хоть кто-то заявлен не во все игры.
    total = len(games)
    differ = any(len(labels) != total for labels in where.values())

    # Ссылку пула на строку листа сводим по ФИО — тем же способом, что и
    # заявка на одну игру.
    by_row: Dict[int, str] = {}
    import coach_payments
    titles = {int(p["row"]): str(p.get("title") or "")
              for p in coach_payments.players()}
    out = []
    for entry in pool:
        e = dict(entry)
        row = next((r for r in where
                    if r in titles and _same_person(e["name"], titles[r])), None)
        labels = where.get(row or -1, [])
        e["in_games"] = labels
        e["games"] = len(labels)
        # Никем не заявлен — в матчах его не будет вовсе; показываем, но
        # честно говорим об этом.
        e["not_declared"] = bool(where) and not labels
        out.append(e)
        if row is not None:
            by_row[row] = e["name"]
    return out, differ


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


def _load_merges() -> Dict[str, str]:
    """{одиночная ссылка -> составная}. Склейка, посчитанная тогда, когда имена
    были на руках."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return {r["ref"]: r["canonical"] for r in
                conn.execute("SELECT ref, canonical FROM player_merges")}


def _save_merges(merged: List[Dict[str, Any]]) -> None:
    """Запоминаем склейку. Только пары идентификаторов, ФИО тут нет."""
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        for m in merged:
            parts = str(m["ref"]).split("+")
            if len(parts) < 2:
                continue          # одиночную ссылку помнить незачем
            for one in parts:
                conn.execute(
                    """INSERT INTO player_merges (ref, canonical, fetched_at)
                       VALUES (?, ?, ?)
                       ON CONFLICT(ref) DO UPDATE SET
                         canonical = excluded.canonical,
                         fetched_at = excluded.fetched_at""", (one, m["ref"], now))
        conn.commit()


def _merge_pool_by_name(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Склейка по ФИО работает, только пока имена есть. Они живут в памяти и
    # после перезапуска демона пусты — а собранный в этот момент пул показывал
    # бы одного человека дважды и рвал бы сохранённые составы. Поэтому сначала
    # применяем склейку, посчитанную раньше и записанную в базу.
    known = _load_merges()
    by_name: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for p in pool:
        key = _norm_name(p["name"])
        if (not key or key.startswith(("№", "id "))) and p["ref"] in known:
            key = known[p["ref"]]          # имени нет — берём вчерашнюю склейку
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
    out = _consolidate_similar(merged)
    # Запоминаем склейку — но только когда имена были: собранная на номерах,
    # она закрепила бы ошибку.
    try:
        if not player_names.is_cold():
            _save_merges(out)
    except Exception as e:
        log.warning(f"склейка пула не записана: {e}")
    return out


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


def _can_view(user: Optional[Dict[str, Any]], season: Optional[Dict[str, Any]] = None) -> bool:
    """Кто вправе ЧИТАТЬ фэнтези: игрок команды, админ — и все желающие, если
    лигу открыли кнопкой «Открыть для всех» в админке.

    По умолчанию лига закрыта: она задумана для команды, и в пуле видны имена.
    Открытие — сознательное решение на конкретную лигу, а не общий режим бота;
    командные разделы (тренер, отчёты, оплаты, шутки, личная статистика) им не
    затрагиваются вовсе, у них свои проверки."""
    if not user:
        return False
    if _is_team_member(str(user.get("id")), user.get("username", "")) or _is_admin(user):
        return True
    try:
        import fantasy as _f
        return _f.is_open(season or _f.get_active_season() or {})
    except Exception:
        return False


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


def _remember_price_refs(pairs: List[Tuple[str, int]]) -> None:
    """Пишет связки ОДНОЙ транзакцией: по отдельной на каждую — это фиксация
    на диск за штуку, и на пуле в четыре десятка карточек уже заметно."""
    if not pairs:
        return
    try:
        sheets_cache.init_db()
        now = sheets_cache.now_iso()
        with sheets_cache.get_connection() as conn:
            conn.executemany(
                """INSERT INTO price_refs (ref, player_row, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(ref) DO UPDATE SET player_row = excluded.player_row,
                                                  updated_at = excluded.updated_at""",
                [(str(r), int(w), now) for r, w in pairs])
            conn.commit()
    except Exception as e:            # связка — удобство, а не обязательство
        log.debug(f"связки цен не сохранились: {e}")


def remember_price_refs(pool: List[Dict[str, Any]]) -> int:
    """Проставляет связки «карточка -> строка листа» для всего пула.

    Зовётся демоном при прогреве: только у него тёплый реестр имён. Пересчёт
    цен живёт в кроне, где имён нет, и без этих связок он не находит никого."""
    prices = sheets_cache.get_player_prices()
    pairs = []
    for card in pool:
        pr = _lookup_price(card.get("name", ""), prices)
        if pr.get("row"):
            pairs.append((card["ref"], int(pr["row"])))
    _remember_price_refs(pairs)
    return len(pairs)


def price_row_of(ref: str) -> int:
    """Строка листа для карточки — по запомненной связке. 0, если её нет."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT player_row FROM price_refs WHERE ref = ?",
                           (str(ref),)).fetchone()
    return int(row["player_row"]) if row else 0


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


_stats_cache: Dict[str, Dict[str, Any]] = {}
STATS_TTL = 60          # секунд: статистика меняется после игры, не чаще


def pool_with_stats_cached(pool: List[Dict[str, Any]],
                           season: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Тот же обогащённый пул, но не пересобираемый на каждый запрос.

    Приложение открывает три экрана разом (пул, состав, таблица), и каждый
    заново считал агрегаты по всей статистике. Минуты жизни кеша достаточно, а
    ожидание у человека сокращается кратно.

    Ключ — сезон И СОСТАВ ПЕРЕДАННОГО СПИСКА. Раньше ключом был только сезон,
    и пул считался «одинаковым для всех» — это перестало быть правдой, когда
    появились ставки на игру: экран матча передаёт сюда заявку тренера (12
    человек), а получал из кеша полный ростер лиги (24). Фильтр считался
    верно и тут же выбрасывался — снаружи это выглядело как «фильтр не
    работает»."""
    refs = "|".join(sorted(str(e.get("ref") or ",".join(e.get("refs") or []))
                           for e in pool))
    key = str((season or {}).get("id", "")) + ":" + hashlib.md5(
        refs.encode("utf-8")).hexdigest()[:12]
    hit = _stats_cache.get(key)
    now = time.time()
    if hit and now - hit["at"] < STATS_TTL:
        return hit["data"]
    data = _pool_with_stats(pool, season)
    _stats_cache[key] = {"at": now, "data": data}
    return data


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
        # Связки «карточка -> строка» тут НЕ пишем: этот код выполняется на
        # каждую загрузку пула, и запись по строке превращалась в 37 отдельных
        # транзакций за запрос — приложение заметно тормозило. Их проставляет
        # демон при прогреве, одним разом (remember_price_refs).
        # Уровень ВСЕГДА считаем от цены, а не читаем из таблицы: цена —
        # единственный источник правды, её правит тренер, и значок обязан
        # идти за ней сам. Столбец «Уровень» в листе — формула для глаз.
        tier = fantasy_modes.tier_for(pr.get("price"))
        enriched.append({**p, "stats": combined, "last": last_one,
                         "price": pr.get("price", 0), "tier": tier,
                         "excluded": fantasy.norm_player_name(p["name"]) in excluded})
    return enriched


# Показатели на карточке игрока: ключ агрегата и подпись.
CARD_STATS = (("pts", "очки"), ("reb", "подборы"), ("ast", "передачи"),
              ("stl", "перехваты"), ("blk", "блоки"))


def _pool_scale(pool: List[Dict[str, Any]]) -> Dict[str, float]:
    """Лучшее в пуле по каждому показателю — шкала полосок на карточке.

    Меряем «за игру», иначе шкалу задаёт тот, кто просто больше сыграл. Ноль
    не отдаём: на него потом делить."""
    scale: Dict[str, float] = {}
    for key, _ in CARD_STATS:
        best = 0.0
        for p in pool:
            st = p.get("stats") or {}
            games = float(st.get("games") or 0)
            if games:
                best = max(best, float(st.get(key) or 0) / games)
        scale[key] = round(best, 2) or 1.0
    return scale


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


async def handle_history(request: web.Request) -> web.Response:
    """Что случилось с ценами после каждой игры — свежие первыми.

    ФИО в price_history нет, поэтому имена подставляем из пула: он и так
    собран в памяти, а на диске имён у нас не бывает."""
    import fantasy_prices
    # Тот же пропуск, что и у остальных экранов: подпись initData обязательна.
    # Без неё эндпоинт торчал бы наружу открытым — а туннель публичный.
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    pool = await build_pool(season=season)
    # Команды сезона — чтобы в истории были ИГРЫ, а не только записанные
    # движения цен: до 03.08 их никто не сохранял, и экран выглядел бы пустым.
    teams = sorted({(fantasy_stats.parse_ref(one)[0], one.split(":")[1])
                    for p in pool for one in fantasy_stats.expand_refs([p["ref"]])
                    if len(one.split(":")) >= 3})

    def collect() -> List[Dict[str, Any]]:
        """Всё, что лезет в базу, — в отдельном потоке.

        Раньше разбор шёл прямо в обработчике: с полусотней карточек и
        поиском цены по каждой он ощутимо держал цикл событий, и медленным
        становился ВЕСЬ бот, а не только этот экран."""
        rows = fantasy_prices.history(teams)
        prices = sheets_cache.get_player_prices()
        by_row: Dict[int, Dict[str, Any]] = {}
        for card in pool:
            pr = _lookup_price(card.get("name", ""), prices)
            if pr.get("row"):
                by_row.setdefault(int(pr["row"]), card)
        by_ref = {p["ref"]: p for p in pool}
        out = []
        for g in rows:
            changes = []
            for ch in g["changes"]:
                card = by_ref.get(ch["ref"]) or by_row.get(ch["row"])
                changes.append({**ch, "name": (card or {}).get("name") or ""})
            out.append({**g, "changes": changes, "title": _history_title(g)})
        return out

    games = await asyncio.get_running_loop().run_in_executor(None, collect)
    return web.json_response({"games": games,
                              "since": fantasy_prices.HISTORY_SINCE})


def _history_title(game: Dict[str, Any]) -> str:
    """«02.08 · SLPRO · PullUp Farm — Атланты 78:75»."""
    import fantasy_prices
    when0 = f"{game['date'][8:10]}.{game['date'][5:7]}" if len(game["date"]) >= 10 else ""
    if game["source"] == fantasy_prices.MANUAL_SOURCE:
        return " · ".join(x for x in (when0, "Ручной пересчёт") if x)
    league = "SLPRO" if game["source"] == "slpro" else "Инфобаскет"
    when = f"{game['date'][8:10]}.{game['date'][5:7]}" if len(game["date"]) >= 10 else ""
    who = " — ".join(x for x in (game.get("home"), game.get("guest")) if x)
    tail = " ".join(x for x in (who, game.get("score")) if x)
    return " · ".join(x for x in (when, league, tail) if x)


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
    # Каждому режиму — своя таблица: правила разные, и общий список был бы
    # топом режима, а не людей (решение 30.07, см. fantasy-scoring-invariant).
    by_mode = fantasy.top_participants_by_mode(season["id"], d_from, d_to) if season else []
    by_ref = {p["ref"]: p for p in await build_pool(season=season)}
    for block in by_mode:
        for g in block["rows"]:
            for pick in g.get("picks") or []:
                pick["players"] = [_ref_title(r, by_ref) for r in pick.pop("refs", [])]
    guessers = by_mode[0]["rows"] if by_mode else []
    return web.json_response({"period": period, "title": title,
                              "top": rows[:30], "guessers": guessers,
                              "guessers_by_mode": by_mode})


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
    # Из ЛОКАЛЬНОГО справочника лиг: контекст стадии SLPRO там уже лежит
    # (league_sync складывает его в league_teams.ctx_json). Живой запрос
    # оставлен на случай пустого зеркала — но именно он делал открытие админки
    # тридцатисекундным, когда лига не отвечала: у SLPRO большой таймаут, а
    # ждал его человек перед экраном.
    try:
        import league_sync
        import slpro_client
        local = [t for t in league_sync.our_teams("slpro") if t.get("ctx")]
        if local:
            for t in local:
                ctx = t["ctx"]
                if ctx.get("stage_id") is not None:
                    out.append(slpro_client.scope_of(ctx))
        else:
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
                    # Цена и ранг — чтобы тренер правил их здесь же, не открывая
                    # таблицу. Пишем всё равно в лист: он остаётся источником.
                    "price": p.get("price", 0), "tier": p.get("tier", ""),
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
            "open_to_all": fantasy.is_open(s),
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
    return web.json_response({
        "seasons": seasons, "available": avail,
        # Полный каталог показателей с подписями — админка правит любой из них,
        # а не только те, что уже включены.
        "weight_keys": [{"key": k, "title": t} for k, t in fantasy_stats.SCORING_KEYS],
        "weight_overlap": fantasy_stats.OVERLAPPING,
    })


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
    elif action == "weights":
        # Пришёл весь набор разом: правится обычно несколько строк сразу, и
        # сохранять их по одной значило бы гонять сезон через десяток запросов.
        value = body.get("value")
        if not isinstance(value, dict):
            return web.json_response({"error": "bad_value"}, status=400)
        fantasy.set_weights(value, sid)
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
    elif action == "open_toggle":
        season = fantasy._get_season(sid)
        fantasy.set_open(season, not fantasy.is_open(season))
    elif action == "price_set":
        # Ручная цена игрока. Пишем в тот же столбец листа, что и автоматика:
        # лист остаётся единственным источником правды, и следующий пересчёт
        # оттолкнётся от того, что поставил тренер, а не от старого значения.
        ref = str(body.get("ref") or "")
        try:
            value = int(body.get("value"))
        except (TypeError, ValueError):
            return web.json_response({"error": "bad_value"}, status=400)
        # Верх шкалы — 100, как бюджет режима: цена дороже всего бюджета
        # сделала бы игрока невыбираемым. Ноль разрешён: это «цены нет».
        if not 0 <= value <= 100:
            return web.json_response({"error": "bad_value"}, status=400)
        row = await asyncio.to_thread(price_row_of, ref)
        if not row:
            # Связка «карточка → строка листа» появляется при прогреве демона.
            # Молчать нельзя: тренер жмёт, цена не меняется, и виноват бот.
            return web.json_response({"error": "unknown_row"}, status=404)
        try:
            import report_common
            book = await asyncio.to_thread(report_common.init_sheets)
            written = await asyncio.to_thread(
                sheets_cache.write_player_prices, book, {int(row): value})
        except Exception as exc:
            log.warning("Ручная цена %s: %s", ref, exc)
            return web.json_response({"error": "sheet_write_failed"}, status=502)
        if not written:
            return web.json_response({"error": "sheet_write_failed"}, status=502)
        invalidate_pool()
    elif action == "prices_since":
        # «Цены проставлены, считай с этого дня». Игры до этой даты в движение
        # цены не идут вовсе: тренер их уже учёл, выставляя цену руками, и
        # первый же пересчёт иначе двинул бы человека по старой истории.
        # Пустое значение снимает точку отсчёта (считаем всю историю).
        from datetime import date
        value = body.get("value")
        since = "" if value is False else (str(value) if value else date.today().isoformat())
        fantasy._update_settings(fantasy._get_season(sid), price_since=since)
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
        # Кабинет уходит РАНЬШЕ цен: без цен в бюджетном режиме нечего делать
        # вообще, а кабинет — приятное дополнение.
        {**data, "standings": [], "pool": nostat_pool, "me": {}},
        {**data, "standings": [], "pool": bare_pool, "me": {}},
    ]


def _payload_me(shared: Dict[str, Any], user_id: str) -> Dict[str, Any]:
    """Кабинет игрока для запасного входа.

    Ищем человека в УЖЕ сжатом пуле (там есть и ссылка, и цена) — иначе
    пришлось бы второй раз собирать пул ради одного имени. Не нашли (человек
    не игрок команды или ФИО в листе расходится с ростером) — пустой словарь,
    фронт просто не покажет вкладку."""
    row = _my_player_row({"id": user_id})
    if not row:
        return {}
    key = _price_key(f"{row.get('surname', '')} {row.get('name', '')}")
    if not key.strip():
        return {}
    entry = next((p for p in shared["pool"] if _price_key(p.get("m", "")) == key), None)
    if not entry:
        parts = key.split()
        if len(parts) >= 2:
            sur, first = parts[0], parts[1]
            for p in shared["pool"]:
                o = _price_key(p.get("m", "")).split()
                if len(o) >= 2 and ((o[0] == sur and _lev1(first, o[1]))
                                    or (o[1] == first and _lev1(sur, o[0]))):
                    entry = p
                    break
    if not entry or not entry.get("c"):
        return {}
    season = fantasy._get_season(shared["season_id"])
    data = fantasy_prices.progress(entry["c"], [entry["r"]], season)
    # Ужимаем: в кнопке каждый символ на счету. Последних игр хватит трёх, а
    # причина движения цены — служебная строка, экран её не показывает.
    data["games"] = (data.get("games") or [])[:3]
    if isinstance(data.get("next"), dict):
        data["next"] = {k: v for k, v in data["next"].items() if k != "reason"}
    data.update({"ref": entry["r"], "name": entry.get("m", ""),
                 "number": entry.get("n", ""), "mine": True})
    return data


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
        "mode": (r or {}).get("mode", ""),
        "cats": ((r or {}).get("meta") or {}).get("cats", []),
        "inherited": bool(r.get("inherited")) if r else False,
        "locked": shared["sched_locked"] or (bool(r["locked"]) if r else False),
        "week_start": shared["week_start"],
        "standings": shared["standings"],
        # Личный кабинет — тоже в кнопку. Он маленький (пара сотен символов),
        # а без него человек без живого API остаётся без ответа на главный свой
        # вопрос: где я и что мне сделать.
        "me": _payload_me(shared, user_id),
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


async def handle_games(request: web.Request) -> web.Response:
    """Игры, на которые сейчас можно ставить, и заявка тренера по каждой.

    Пул на игру собирается здесь же: приложение показывает список заявленных,
    а не весь состав лиги. Значок «играет ещё и там» — из счётчика по всем
    открытым играм: заявленный на два матча наберёт очки в обоих."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    games = await asyncio.to_thread(upcoming_games)
    counts = await asyncio.to_thread(declared_counts, games)

    want = request.query.get("game_id") or ""
    want_src = request.query.get("source") or ""

    # «Все игры» — псевдо-игра: один состав на все матчи. Хранится он там же,
    # где и раньше (недельный состав), поэтому отдельной таблицы не нужно —
    # это ровно то, чем недельный состав и был.
    if want == "all":
        full = await build_pool(season=season)
        rich = await asyncio.to_thread(pool_with_stats_cached, full, season)
        pool_all, differ = await all_games_pool(games, season, rich)
        return web.json_response({
            "games": games, "game": {"game_id": "all", "source": "",
                                     "label": "Все игры"},
            "pool": pool_all, "pick": [], "all": True, "differ": differ,
            "season": season and {"id": season["id"], "name": season["name"],
                                  "roster_size": fantasy.roster_size(season),
                                  "max_per_player": fantasy.max_per_player(season)},
            "locked": fantasy.lock_details(),
        })

    chosen = next((g for g in games if str(g["game_id"]) == want
                   and (not want_src or g["source"] == want_src)), None)
    if chosen is None and games:
        chosen = games[0]

    pool: List[Dict[str, Any]] = []
    pick: List[str] = []
    if chosen:
        # Статистику считаем для ПОЛНОГО ростера — этот результат уже лежит в
        # кеше после экрана пула — и только потом отбираем заявленных.
        full = await build_pool(season=season)
        rich = await asyncio.to_thread(pool_with_stats_cached, full, season)
        pool = await game_pool(chosen["source"], chosen["game_id"], season, rich)
        # Значок «играет ещё и там» — только когда состав объявлен: без заявки
        # считать не по чему, и число было бы выдумано.
        if chosen.get("declared"):
            for entry in pool:
                entry["games"] = counts.get(int(entry.get("row") or 0), 1)
        if season:
            saved = await asyncio.to_thread(
                fantasy.get_game_pick, str(user["id"]), season["id"],
                chosen["source"], chosen["game_id"])
            pick = (saved or {}).get("refs") or []

    return web.json_response({
        "games": games,
        "game": chosen,
        "pool": pool,
        "pick": pick,
        # Состав тренер ещё не объявил — приложение объяснит, почему список
        # игроков полный.
        "declared": bool(chosen and chosen.get("declared")),
        "season": season and {"id": season["id"], "name": season["name"],
                              "roster_size": fantasy.roster_size(season),
                              "max_per_player": fantasy.max_per_player(season)},
        "locked": fantasy.lock_details(),
    })


async def handle_save_game_pick(request: web.Request) -> web.Response:
    """Сохранить ставку на игру. Пустой состав — снять ставку."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    if not season:
        return web.json_response({"error": "no_season"}, status=400)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "bad_json"}, status=400)

    source = str(body.get("source") or "")
    game_id = str(body.get("game_id") or "")
    refs = [str(r) for r in (body.get("refs") or [])]
    if not source or not game_id:
        return web.json_response({"error": "no_game"}, status=400)

    # Блокировка — та же, что и у недельного состава: идёт игра, состав не
    # трогаем. Проверяем на сервере, а не в приложении: приложение можно
    # открыть заранее и нажать «сохранить» уже после свистка.
    lock = fantasy.lock_details()
    if lock.get("locked"):
        return web.json_response({"error": "locked", "lock": lock}, status=409)

    # Ставить можно только на заявленных: список в приложении мог устареть,
    # пока человек его листал, а тренер тем временем поправил состав.
    #
    # Берём и `ref`, и `refs`. Один человек, играющий в двух лигах, склеен в
    # одну карточку с СОСТАВНОЙ ссылкой («slpro:..+ib:..»), и списка `refs` у
    # неё уже нет — приложение присылает именно составную. Пока здесь стоял
    # только `refs`, разрешённое множество выходило пустым и не сохранялся
    # вообще никакой состав.
    allowed = set()
    for e in await game_pool(source, game_id, season):
        if e.get("ref"):
            allowed.add(str(e["ref"]))
        for r in (e.get("refs") or []):
            allowed.add(str(r))
    bad = [r for r in refs if r not in allowed]
    if bad:
        return web.json_response({"error": "not_declared", "refs": bad}, status=400)

    size = fantasy.roster_size(season)
    if refs and len(refs) != size:
        return web.json_response({"error": "wrong_size", "need": size,
                                  "got": len(refs)}, status=400)

    await asyncio.to_thread(fantasy.set_game_pick, str(user["id"]), season["id"],
                            source, game_id, refs, body.get("mode") or "")

    # Копия в лига-бот — СРАЗУ, а не пачкой к вечеру: он проверяет своё окно и
    # откажет по игре, которая уже началась. Только СЛПРО: остальные лиги ему
    # неинтересны. Ошибка отправки не должна портить сохранение — человек свой
    # состав сохранил, а то, что копия не уехала, наша забота, не его.
    if source == "slpro":
        try:
            import league_push
            await league_push.send_pick(user["id"], league_push.nick_of(user),
                                        game_id, refs)
        except Exception as exc:
            log.warning("лига-бот: состав не отправлен — %s", exc)

    return web.json_response({"ok": True, "refs": refs})


async def handle_pool(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = _season(request)
    started = time.perf_counter()
    raw = await build_pool(season=season)
    built = time.perf_counter()
    # В ПОТОК: это чистые вычисления по базе, а event loop у нас общий с ботом
    # и фоновым циклом — блокировать его на время сборки пула нельзя.
    pool = await asyncio.to_thread(pool_with_stats_cached, raw, season)
    stats_done = time.perf_counter()
    # Список активных лиг — для переключателя в Mini App (когда их несколько).
    seasons = [{"id": s["id"], "name": s["name"], "format": s["format"]}
               for s in fantasy.active_seasons()]
    # Список игр — тем же ответом, а не вторым запросом. Он собирается из
    # локального зеркала расписания (никаких походов в лиги), стоит копейки, а
    # отдельный запрос мог не дойти — и экран матчей оставался пустым.
    try:
        upcoming = await asyncio.to_thread(upcoming_games)
    except Exception as exc:
        log.warning("список игр для приложения не собрался: %s", exc)
        upcoming = []
    resp = web.json_response({
        "games": upcoming,
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
        # Двери — чтобы приложение знало, какую можно не пробовать.
        "doors": doors_state(),
        "member": _is_team_member(str(user.get("id")), user.get("username", "")),
        "admin": _is_admin(user),
        # Нашёлся ли сам человек среди игроков пула — от этого зависит, есть ли
        # у него личный кабинет. Участник фэнтези и игрок команды — не одно и
        # то же: играть может и тот, кто сам на площадку не выходит.
        "player": bool(await _my_card(user, season)),
    })
    total = time.perf_counter() - started
    # Жалобы «фэнтези не открывается» — почти всегда про ожидание. Пишем в лог
    # только медленные ответы: по ним видно, что именно тормозит.
    if total > 1.5:
        log.warning("фэнтези-API /pool медленно: %.1fс (пул %.1fс, статистика "
                    "%.1fс, остальное %.1fс)", total, built - started,
                    stats_done - built, time.perf_counter() - stats_done)
    return resp


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
    season = _season(request)
    if not season:
        return web.json_response({"error": "no_active_season"}, status=400)
    if not _can_view(user, season):
        return web.json_response({"error": "not_a_member"}, status=403)

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
    # Карточка: цена и ранг те же, что в пуле, плюс шкала — лучшее в пуле по
    # каждому показателю. Без шкалы полоски пришлось бы мерить от выдуманного
    # потолка, а «сколько это вообще много» знает только сама команда.
    priced = await asyncio.to_thread(_pool_with_stats, pool, season)
    mine = next((p for p in priced if p["ref"] == ref), {})
    profile["price"] = mine.get("price", 0)
    profile["tier"] = mine.get("tier", "")
    profile["scale"] = _pool_scale(priced)
    # Турнир в шапке — только по источникам игрока (у составного игрока их два).
    psrcs = {fantasy_stats.parse_ref(lr)[0] for lr in fantasy_stats.expand_refs([ref])}
    own = [s for s in scopes if s.get("source") in psrcs]
    profile["tournament"] = fantasy.scopes_title(own or scopes)
    return web.json_response(profile)


def _my_player_row(user: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Строка листа «Игроки», закреплённая за этим Telegram id.

    Сначала ищем строку, в которой стоит его числовой id: ячейка едет вместе
    со строкой, поэтому она всегда при своём человеке, а запомненный номер
    строки устаревает при любой правке листа (см. reconcile_player_links).
    Из-за этого игрок открывал кабинет и видел карточку соседа."""
    uid = str(user.get("id"))
    link = sheets_cache.get_player_link(uid)
    if not link:
        return None
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute("SELECT * FROM players WHERE tg_user_id = ? LIMIT 1",
                           (uid,)).fetchone()
        if not row:
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
    started = time.perf_counter()
    enabled = fantasy_modes.enabled(season)

    def _build_tables() -> Dict[str, Any]:
        out = {}
        import achievements
        for m in enabled:
            rows = fantasy.season_standings_live(season["id"], mode=m)
            names = fantasy.display_names([r["user_id"] for r in rows])
            # Значки берём одним запросом на всю таблицу: по запросу на строку
            # зачёт из тридцати человек стоил бы тридцати походов в базу.
            badges = achievements.shown_map([r["user_id"] for r in rows])
            for r in rows:
                r["name"] = names.get(str(r["user_id"]), "")
                r["badges"] = badges.get(str(r["user_id"]), [])
            out[m] = rows
        return out

    # Считается по всей истории очков и по каждому режиму отдельно — это
    # чистые вычисления, и держать на них общий event loop нельзя: рядом
    # обслуживаются пул, состав и сам бот.
    tables = await asyncio.to_thread(_build_tables)
    # standings — таблица режима, в котором играет сам участник: старые версии
    # приложения знают только это поле.
    roster = fantasy.get_roster_effective(str(user["id"]), season["id"],
                                          fantasy.active_selection(season)[0]) or {}
    mine = fantasy_modes.normalize(season, roster.get("mode"))
    total = time.perf_counter() - started
    if total > 1.5:
        log.warning("фэнтези-API /standings медленно: %.1fс", total)
    return web.json_response({"standings": tables.get(mine, tables.get(enabled[0], [])),
                              "tables": tables, "my_mode": mine,
                              "modes": fantasy_modes.describe(season)})


# Двери наружу: id → (подпись, адрес, ключ настройки). Порядок — порядок
# перебора во фронте: Cloudflare первым, у него охват шире.
DOORS = [
    ("cf", "Cloudflare", "https://api.one4two.ru", "door_cf_enabled"),
    ("funnel", "Tailscale Funnel", FUNNEL_URL, "door_funnel_enabled"),
]


def doors_state() -> List[Dict[str, Any]]:
    """Какие двери сейчас предлагаем фронту. Выключенную не перебираем: 8
    секунд на заведомо мёртвом канале — это 8 секунд белого экрана."""
    out = []
    for key, title, url, setting in DOORS:
        out.append({"id": key, "title": title, "url": url,
                    "enabled": bool(sheets_cache.get_int_setting(setting, 1))})
    return out


async def handle_badge_image(request: web.Request) -> web.StreamResponse:
    """Картинка значка. Единственная ручка без подписи — и намеренно.

    Тег <img> в WebView заголовков не ставит, подписать его нечем. Отдаём
    только само изображение: ни имени, ни владельца, ни чего бы то ни было
    личного здесь нет — это рисунок, лежащий в базе."""
    import achievements
    try:
        ach_id = int(request.match_info.get("ach_id", "0"))
    except ValueError:
        raise web.HTTPNotFound()
    data, kind = await asyncio.to_thread(achievements.image, ach_id)
    if not data:
        raise web.HTTPNotFound()
    return web.Response(body=data, content_type=kind,
                        headers={"Cache-Control": "public, max-age=86400"})


async def handle_badges(request: web.Request) -> web.Response:
    """Мои значки: что выдано, что показываю, и сколько можно показывать."""
    import achievements
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    mine = await asyncio.to_thread(achievements.of_user, str(user["id"]))
    return web.json_response({"badges": mine, "limit": achievements.SHOWN_LIMIT})


async def handle_save_badges(request: web.Request) -> web.Response:
    """Человек выбрал, какие значки показывать рядом со своим именем."""
    import achievements
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "bad_json"}, status=400)
    picked = body.get("shown") or []
    if not isinstance(picked, list):
        return web.json_response({"error": "bad_shown"}, status=400)
    ok, note = await asyncio.to_thread(
        achievements.set_shown, str(user["id"]), picked)
    if not ok:
        return web.json_response({"error": note}, status=400)
    mine = await asyncio.to_thread(achievements.of_user, str(user["id"]))
    return web.json_response({"ok": True, "badges": mine,
                              "limit": achievements.SHOWN_LIMIT})


async def handle_ping(request: web.Request) -> web.Response:
    """Проба двери. Без подписи и без данных: этим эндпоинтом человек
    проверяет, доходит ли он до сервера вообще, — и как раз в этот момент
    подпись у него может не работать."""
    return web.json_response({"ok": True, "doors": doors_state()})


def create_app(bot_token: str) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["bot_token"] = bot_token
    app.add_routes([
        web.get("/fantasy/pool", handle_pool),
        web.get("/fantasy/games", handle_games),
        web.post("/fantasy/games", handle_save_game_pick),
        web.get("/fantasy/roster", handle_get_roster),
        web.post("/fantasy/roster", handle_save_roster),
        web.get("/fantasy/player", handle_player),
        web.get("/fantasy/me", handle_me),
        web.get("/fantasy/standings", handle_standings),
        web.get("/fantasy/top", handle_top),
        web.get("/fantasy/history", handle_history),
        web.get("/fantasy/admin", handle_admin_state),
        web.post("/fantasy/admin", handle_admin_action),
        web.get("/fantasy/badges", handle_badges),
        web.post("/fantasy/badges", handle_save_badges),
        web.get("/fantasy/badge/{ach_id}", handle_badge_image),
        web.get("/fantasy/ping", handle_ping),
        web.get("/health", lambda r: web.json_response({"ok": True})),
    ])
    return app
