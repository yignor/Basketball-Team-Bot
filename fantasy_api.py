#!/usr/bin/env python3
"""
HTTP-API фэнтези для Mini App (aiohttp). Живёт в процессе демона
(bot_daemon.on_startup подвешивает сервер в тот же event loop), наружу
отдаётся через Cloudflare Tunnel. Фронт (GitHub Pages) ходит сюда за пулом/
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
import os
import time
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qsl
from datetime import date

from aiohttp import web

import sheets_cache
import fantasy
import fantasy_stats

# ─────────────────────────── initData auth ───────────────────────────────────

def verify_init_data(init_data: str, bot_token: str, max_age_sec: int = 86400) -> Optional[Dict[str, Any]]:
    """Проверяет подпись Telegram WebApp initData. Возвращает dict пользователя
    ({id, username, first_name, ...}) или None. Алгоритм: secret =
    HMAC_SHA256(key="WebAppData", msg=bot_token); hash = HMAC_SHA256(key=secret,
    msg=data_check_string)."""
    if not init_data or not bot_token:
        return None
    try:
        pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    except (ValueError, TypeError):
        return None
    received_hash = pairs.pop("hash", None)
    if not received_hash:
        return None
    data_check_string = "\n".join(f"{k}={pairs[k]}" for k in sorted(pairs))
    secret = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
    calc = hmac.new(secret, data_check_string.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(calc, received_hash):
        return None
    # свежесть
    try:
        auth_date = int(pairs.get("auth_date", "0"))
        if max_age_sec and (time.time() - auth_date) > max_age_sec:
            return None
    except (ValueError, TypeError):
        return None
    try:
        return json.loads(pairs.get("user", "null"))
    except (json.JSONDecodeError, TypeError):
        return None


def _is_team_member(telegram_id: str) -> bool:
    """v1: участвуют только игроки команды (есть в players по Telegram ID)."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM players WHERE telegram_id = ? LIMIT 1", (str(telegram_id),)
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
    return _cors(resp)


def _auth_user(request: web.Request) -> Optional[Dict[str, Any]]:
    bot_token = request.app["bot_token"]
    init_data = request.headers.get("X-Init-Data") or request.query.get("initData", "")
    return verify_init_data(init_data, bot_token)


async def handle_pool(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = fantasy.get_active_season()
    pool = await build_pool()
    return web.json_response({
        "season": season and {"id": season["id"], "name": season["name"], "format": season["format"],
                              "roster_size": fantasy.roster_size(season)},
        "pool": pool,
        "member": _is_team_member(str(user.get("id"))),
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
    if not _is_team_member(uid):
        return web.json_response({"error": "not_a_member"}, status=403)
    season = fantasy.get_active_season()
    if not season:
        return web.json_response({"error": "no_active_season"}, status=400)

    try:
        body = await request.json()
        refs = body.get("refs") or []
    except (json.JSONDecodeError, TypeError):
        return web.json_response({"error": "bad_request"}, status=400)

    size = fantasy.roster_size(season)
    if not isinstance(refs, list) or len(refs) != size or len(set(refs)) != size:
        return web.json_response({"error": "invalid_roster", "expected": size}, status=400)
    pool_refs = {p["ref"] for p in await build_pool()}
    if any(r not in pool_refs for r in refs):
        return web.json_response({"error": "unknown_player"}, status=400)

    week_start = fantasy.week_start_of(date.today()).isoformat()
    res = fantasy.save_roster(uid, season["id"], week_start, refs)
    if not res.get("ok"):
        return web.json_response({"error": res.get("error", "save_failed")}, status=409)
    return web.json_response({"ok": True, "week_start": week_start})


async def handle_standings(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    season = fantasy.get_active_season()
    if not season:
        return web.json_response({"standings": []})
    week_start = fantasy.week_start_of(date.today()).isoformat()
    table = fantasy.weekly_standings(season["id"], week_start)
    # user_id -> отображаемое имя (из players, транзитно)
    return web.json_response({"week_start": week_start, "standings": table})


def create_app(bot_token: str) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app["bot_token"] = bot_token
    app.add_routes([
        web.get("/fantasy/pool", handle_pool),
        web.get("/fantasy/roster", handle_get_roster),
        web.post("/fantasy/roster", handle_save_roster),
        web.get("/fantasy/standings", handle_standings),
        web.get("/health", lambda r: web.json_response({"ok": True})),
    ])
    return app
