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


async def build_pool(force: bool = False) -> List[Dict[str, Any]]:
    """Пул драфта: игроки Pull Up Farm (SLPRO) [+ основа Infobasket — позже].
    Ref = source:team:player_id. Имя — транзитно из публичного ростера,
    в наших таблицах не хранится. Кешируется в памяти (TTL 1ч)."""
    now = time.time()
    if not force and _pool_cache["data"] is not None and (now - _pool_cache["at"]) < _POOL_TTL:
        return _pool_cache["data"]

    from slpro_client import SlproClient
    client = SlproClient()
    pool: List[Dict[str, Any]] = []
    team_names = os.getenv("SLPRO_TEAM_NAMES", "PullUp Farm,Pull Up Farm").split(",")
    ctx = await client.discover_context([n.strip() for n in team_names if n.strip()])
    if ctx and ctx.get("team_id"):
        tid = ctx["team_id"]
        for p in await client.get_roster(tid):
            pid = p.get("player_id")
            if pid is None:
                continue
            pool.append({
                "ref": f"slpro:{tid}:{pid}",
                "number": str(p.get("number", "") or ""),
                "name": f"{p.get('surname', '')} {p.get('name', '')}".strip(),  # транзитно
                "team": ctx.get("team_name", "Pull Up Farm"),
            })
    # TODO(F1): добавить ростер основы (Infobasket) по связке ID из «Игроки».
    _pool_cache["data"] = pool
    _pool_cache["at"] = now
    return pool


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


def _pool_with_stats(pool: List[Dict[str, Any]], season: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Дополняет пул суммарной статистикой игрока за всё время (для сортировки
    в Mini App). Агрегаты живут отдельно от пула — пул кешируется на час, а
    статистика меняется после каждого ingest."""
    weights = fantasy.season_weights(season) if season else fantasy_stats.DEFAULT_WEIGHTS
    agg = fantasy_stats.player_aggregates(weights, scope=fantasy.season_scope(season) if season else None)
    enriched = []
    for p in pool:
        src, pid = fantasy_stats.parse_ref(p["ref"])
        enriched.append({**p, "stats": agg.get(f"{src}:{pid}", {})})
    return enriched


async def handle_pool(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = fantasy.get_active_season()
    pool = _pool_with_stats(await build_pool(), season)
    return web.json_response({
        "season": season and {"id": season["id"], "name": season["name"], "format": season["format"],
                              "roster_size": fantasy.roster_size(season),
                              "max_per_player": fantasy.max_per_player(season)},
        "pool": pool,
        "member": _is_team_member(str(user.get("id")), user.get("username", "")),
    })


async def handle_get_roster(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = fantasy.get_active_season()
    if not season:
        return web.json_response({"roster": None, "week_start": None})
    week_start = fantasy.week_start_of(date.today()).isoformat()
    r = fantasy.get_roster(str(user["id"]), season["id"], week_start)
    return web.json_response({
        "roster": r["refs"] if r else [],
        "locked": bool(r["locked"]) if r else False,
        "week_start": week_start,
    })


async def handle_save_roster(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    uid = str(user["id"])
    if not _is_team_member(uid, user.get("username", "")):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = fantasy.get_active_season()
    if not season:
        return web.json_response({"error": "no_active_season"}, status=400)

    try:
        body = await request.json()
        refs = body.get("refs") or []
    except (json.JSONDecodeError, TypeError):
        return web.json_response({"error": "bad_request"}, status=400)

    pool_refs = {p["ref"] for p in await build_pool()}
    err = fantasy.validate_roster(season, refs, pool_refs)
    if err:
        return web.json_response({"error": err, "expected": fantasy.roster_size(season)}, status=400)

    week_start = fantasy.week_start_of(date.today()).isoformat()
    res = fantasy.save_roster(uid, season["id"], week_start, refs)
    if not res.get("ok"):
        return web.json_response({"error": res.get("error", "save_failed")}, status=409)
    return web.json_response({"ok": True, "week_start": week_start})


_teams_cache: Dict[str, Any] = {"at": 0.0, "key": "", "data": {}}


async def _team_names(scope: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """team_id -> название, транзитно из публичной таблицы лиги. В нашу базу
    названия не пишем (там только id), поэтому подтягиваем на лету и кешируем."""
    if not scope or scope.get("source") != fantasy_stats.SOURCE_SLPRO:
        return {}
    key = f"{scope.get('season_id')}:{scope.get('stage_id')}"
    now = time.time()
    if _teams_cache["key"] == key and (now - _teams_cache["at"]) < _POOL_TTL:
        return _teams_cache["data"]
    names: Dict[str, str] = {}
    try:
        from slpro_client import SlproClient
        client = SlproClient()
        # В scope нет division_id — восстанавливаем его по стадии.
        for st in await client.iter_stages():
            if (str(st["season_id"]) == str(scope.get("season_id"))
                    and str(st["stage_id"]) == str(scope.get("stage_id"))):
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

    season = fantasy.get_active_season()
    weights = fantasy.season_weights(season) if season else fantasy_stats.DEFAULT_WEIGHTS
    scope = fantasy.season_scope(season) if season else None
    profile = fantasy_stats.player_profile(ref, scope, weights)

    names = await _team_names(scope)
    if profile.get("last") and profile["last"].get("opponent_id") is not None:
        profile["last"]["opponent"] = names.get(str(profile["last"]["opponent_id"]), "Соперник")
    profile["name"] = entry["name"]          # транзитно, как и в пуле
    profile["number"] = entry.get("number", "")
    profile["tournament"] = fantasy.scope_title(scope) if scope else ""
    return web.json_response(profile)


async def handle_standings(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = fantasy.get_active_season()
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
        web.get("/fantasy/player", handle_player),
        web.get("/fantasy/standings", handle_standings),
        web.get("/health", lambda r: web.json_response({"ok": True})),
    ])
    return app
