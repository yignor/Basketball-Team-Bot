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

import hashlib
import hmac
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qsl
from datetime import date

from aiohttp import web

import sheets_cache
import fantasy
import fantasy_stats

log = logging.getLogger(__name__)

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


def verify_init_data(init_data: str, bot_token: str, max_age_sec: int = 86400) -> Optional[Dict[str, Any]]:
    """Совместимая обёртка: только пользователь, без причины отказа."""
    return verify_init_data_detailed(init_data, bot_token, max_age_sec)[0]


def _is_team_member(telegram_id: Any, username: str = "") -> bool:
    """v1: участвуют только игроки команды (есть в листе «Игроки»).

    В листе колонка «Telegram ID» исторически заполнена @юзернеймами, а не
    числовыми id — поэтому сверяем и с тем, и с другим."""
    sheets_cache.init_db()
    uname = (username or "").lstrip("@").lower()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            """SELECT 1 FROM players
               WHERE telegram_id = ?
                  OR (? != '' AND lower(ltrim(telegram_id, '@')) = ?)
               LIMIT 1""",
            (str(telegram_id), uname, uname),
        ).fetchone()
    return row is not None


# ─────────────────────────── Пул драфта ──────────────────────────────────────

_pool_cache: Dict[str, Any] = {"at": 0.0, "data": None}
_POOL_TTL = 3600.0


async def derive_pool_teams() -> List[Dict[str, Any]]:
    """Кандидаты для пула из настроек поиска игр: SLPRO-команда (по имени) +
    команда(ы) Инфобаскета (team_id × comp_id из Конфига)."""
    teams: List[Dict[str, Any]] = []
    try:
        from slpro_client import SlproClient
        names = [n.strip() for n in os.getenv("SLPRO_TEAM_NAMES", "PullUp Farm,Pull Up Farm").split(",") if n.strip()]
        ctx = await SlproClient().discover_context(names)
        if ctx and ctx.get("team_id"):
            teams.append({"source": "slpro", "team_id": ctx["team_id"],
                          "name": ctx.get("team_name", "SLPRO")})
    except Exception as e:
        log.warning(f"пул: SLPRO-команда — {e}")
    try:
        from enhanced_duplicate_protection import duplicate_protection
        cfg = duplicate_protection.get_config_ids()
        comps = cfg.get("comp_ids") or []
        comp = comps[0] if comps else None
        for tid in (cfg.get("team_ids") or []):
            teams.append({"source": "infobasket", "team_id": tid, "comp_id": comp,
                          "name": "Инфобаскет"})
    except Exception as e:
        log.warning(f"пул: команды Инфобаскета — {e}")
    return teams


async def _resolve_pool_teams() -> List[Dict[str, Any]]:
    """Команды пула: явный выбор админа (fantasy.pool_teams) или дефолт —
    все кандидаты из настроек поиска игр."""
    season = fantasy.get_active_season()
    explicit = fantasy.pool_teams(season) if season else []
    return explicit if explicit else await derive_pool_teams()


async def build_pool(force: bool = False) -> List[Dict[str, Any]]:
    """Пул драфта: игроки команд из fantasy.pool_teams (SLPRO + Инфобаскет).
    Ref = source:team_id:player_id. Имя — транзитно из публичного ростера,
    в наших таблицах не хранится. Кешируется в памяти (TTL 1ч)."""
    now = time.time()
    if not force and _pool_cache["data"] is not None and (now - _pool_cache["at"]) < _POOL_TTL:
        return _pool_cache["data"]

    pool: List[Dict[str, Any]] = []
    seen = set()
    for team in await _resolve_pool_teams():
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
            continue
        pref = "slpro" if src == "slpro" else "ib"
        for p in players:
            if p["pid"] is None:
                continue
            ref = f"{pref}:{tid}:{p['pid']}"
            if ref in seen:
                continue
            seen.add(ref)
            pool.append({"ref": ref, "number": str(p["number"] or ""),
                         "name": p["name"], "team": team.get("name", "")})

    # Один физический игрок может быть в ДВУХ лигах (SLPRO Farm + Инфобаскет) с
    # разными id. Склеиваем по ФИО в одну карточку с составной ссылкой
    # «slpro:..+ib:..» — очки суммируются, а не задваиваются.
    pool = _merge_pool_by_name(pool)
    _pool_cache["data"] = pool
    _pool_cache["at"] = now
    return pool


def _norm_name(name: str) -> str:
    return " ".join((name or "").lower().replace("ё", "е").split())


def _merge_pool_by_name(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_name: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for p in pool:
        key = _norm_name(p["name"])
        if not key:
            key = p["ref"]  # без имени — не склеиваем
        if key not in by_name:
            by_name[key] = {"refs": [p["ref"]], "number": p["number"],
                            "name": p["name"], "team": p["team"]}
            order.append(key)
        else:
            by_name[key]["refs"].append(p["ref"])
            if not by_name[key]["number"]:
                by_name[key]["number"] = p["number"]
    merged = []
    for key in order:
        e = by_name[key]
        merged.append({"ref": "+".join(sorted(e["refs"])), "number": e["number"],
                       "name": e["name"], "team": e["team"]})
    return merged


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


def _is_admin(user: Optional[Dict[str, Any]]) -> bool:
    """Админ Mini App — тот же список, что у бота (ADMIN_USER_IDS в .env).
    Подпись initData уже проверена, id подделать нельзя."""
    if not user:
        return False
    admins = {x.strip() for x in
              os.getenv("ADMIN_USER_IDS", os.getenv("ADMIN_USER_ID", "")).split(",") if x.strip()}
    return str(user.get("id")) in admins


def _pool_with_stats(pool: List[Dict[str, Any]], season: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Дополняет пул суммарной статистикой игрока за всё время (для сортировки
    в Mini App). Агрегаты живут отдельно от пула — пул кешируется на час, а
    статистика меняется после каждого ingest."""
    weights = fantasy.season_weights(season) if season else fantasy_stats.DEFAULT_WEIGHTS
    scopes = fantasy.effective_scopes(season) if season else []
    agg = fantasy_stats.player_aggregates(weights, scope=scopes)
    last = fantasy_stats.player_last_fp(weights, scope=scopes)
    excluded = set(fantasy.pool_excluded_names(season or {}))
    enriched = []
    for p in pool:
        keys = [f"{fantasy_stats.parse_ref(lr)[0]}:{fantasy_stats.parse_ref(lr)[1]}"
                for lr in fantasy_stats.expand_refs([p["ref"]])]
        combined = fantasy_stats.combine_agg([agg.get(k, {}) for k in keys], weights)
        lasts = [last[k] for k in keys if last.get(k)]
        last_one = max(lasts, key=lambda x: x.get("date", ""), default={})
        enriched.append({**p, "stats": combined, "last": last_one,
                         "excluded": fantasy.norm_player_name(p["name"]) in excluded})
    return enriched


async def handle_pool(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = _season(request)
    pool = _pool_with_stats(await build_pool(), season)
    # Список активных лиг — для переключателя в Mini App (когда их несколько).
    seasons = [{"id": s["id"], "name": s["name"], "format": s["format"]}
               for s in fantasy.active_seasons()]
    return web.json_response({
        "season": season and {"id": season["id"], "name": season["name"], "format": season["format"],
                              "roster_size": fantasy.roster_size(season),
                              "max_per_player": fantasy.max_per_player(season)},
        "seasons": seasons,
        "pool": pool,
        "member": _is_team_member(str(user.get("id")), user.get("username", "")),
        "admin": _is_admin(user),
    })


async def handle_exclude(request: web.Request) -> web.Response:
    """Админ убирает/возвращает игрока в пул (по ФИО из пула). Только админ."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _is_admin(user):
        return web.json_response({"error": "forbidden"}, status=403)
    try:
        body = await request.json()
        ref = str(body.get("ref") or "")
    except (json.JSONDecodeError, TypeError):
        return web.json_response({"error": "bad_request"}, status=400)
    entry = next((p for p in await build_pool() if p["ref"] == ref), None)
    if not entry:
        return web.json_response({"error": "unknown_player"}, status=404)
    season = _season(request)
    now_excluded, _ = fantasy.toggle_pool_exclude_name(entry["name"],
                                                       season["id"] if season else None)
    return web.json_response({"ok": True, "ref": ref, "excluded": now_excluded})


async def handle_get_roster(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = _season(request)
    if not season:
        return web.json_response({"roster": None, "week_start": None})
    week_start, locked = fantasy.active_selection(season)
    r = fantasy.get_roster(str(user["id"]), season["id"], week_start)
    return web.json_response({
        "roster": r["refs"] if r else [],
        # блокировка — по окну набора (расписание), даже если своей записи ещё нет
        "locked": locked or (bool(r["locked"]) if r else False),
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
    except (json.JSONDecodeError, TypeError):
        return web.json_response({"error": "bad_request"}, status=400)

    week_start, locked = fantasy.active_selection(season)
    if locked:
        return web.json_response({"error": "locked"}, status=409)

    all_pool = await build_pool()
    pool_refs = {p["ref"] for p in all_pool}
    err = fantasy.validate_roster(season, refs, pool_refs)
    if err:
        return web.json_response({"error": err, "expected": fantasy.roster_size(season)}, status=400)
    # Убранных админом игроков брать нельзя.
    excluded_names = set(fantasy.pool_excluded_names(season))
    by_ref = {p["ref"]: p for p in all_pool}
    if any(fantasy.norm_player_name(by_ref.get(r, {}).get("name", "")) in excluded_names for r in refs):
        return web.json_response({"error": "excluded_player"}, status=400)

    res = fantasy.save_roster(uid, season["id"], week_start, refs)
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


async def handle_standings(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = _season(request)
    if not season:
        return web.json_response({"standings": []})
    week_start = fantasy.week_start_of(date.today()).isoformat()
    table = fantasy.weekly_standings(season["id"], week_start)
    # user_id -> отображаемое имя (из players, транзитно — в таблицах фэнтези
    # ФИО не храним; здесь только показываем).
    names = fantasy.display_names([r["user_id"] for r in table])
    for r in table:
        r["name"] = names.get(str(r["user_id"]), "")
        r.pop("refs", None)
    return web.json_response({"week_start": week_start, "standings": table})


def create_app(bot_token: str) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["bot_token"] = bot_token
    app.add_routes([
        web.get("/fantasy/pool", handle_pool),
        web.get("/fantasy/roster", handle_get_roster),
        web.post("/fantasy/roster", handle_save_roster),
        web.post("/fantasy/exclude", handle_exclude),
        web.get("/fantasy/player", handle_player),
        web.get("/fantasy/standings", handle_standings),
        web.get("/health", lambda r: web.json_response({"ok": True})),
    ])
    return app
