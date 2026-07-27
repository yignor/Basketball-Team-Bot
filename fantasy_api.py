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
import fantasy_stats

log = logging.getLogger(__name__)

# Публичный адрес живого API. Основной транспорт — Cloudflare quick-tunnel: его
# адрес меняется при каждом рестарте, поэтому deploy/cloudflared-fantasy.sh
# пишет его в этот файл, а демон/рассылка подмешивают в ссылку Mini App (?api=).
# Tailscale Funnel остаётся запасным: если тут пусто, фронт откатывается на него.
_TUNNEL_URL_FILE = os.getenv(
    "FANTASY_API_URL_FILE",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "fantasy_api_url.txt"))


def public_api_url() -> str:
    """Текущий публичный адрес живого API для фронта. Приоритет: env-override
    (FANTASY_API_PUBLIC_URL) -> файл от cloudflared-fantasy. Пусто -> фронт сам
    уйдёт на Funnel (в т.ч. когда туннель лёг: скрипт стирает файл)."""
    override = os.getenv("FANTASY_API_PUBLIC_URL", "").strip()
    if override:
        return override.rstrip("/")
    try:
        with open(_TUNNEL_URL_FILE, encoding="utf-8") as f:
            return f.read().strip().rstrip("/")
    except OSError:
        return ""


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
    """Участвуют только игроки команды (лист «Игроки»).

    Доступ держим на ЧИСЛОВОМ Telegram id: он вечный и не переуступается.
    Ник — только для первого знакомства: в листе колонка «Telegram ID»
    исторически заполнена @никами, поэтому при первом входе находим строку по
    нику и закрепляем за ней числовой id (см. sheets_cache.player_links).

    Дальше ник уже не нужен: сменил @ — доступ остался; освободил @ и его занял
    посторонний — тот НЕ пройдёт, строка занята. Окно доверия к нику сужается
    до одного первого входа настоящего игрока."""
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
    _push_tg_id_to_sheet(cand["row_index"], uid)
    return True


def _push_tg_id_to_sheet(player_row: int, tg_user_id: str) -> None:
    """Best-effort: показать числовой id в листе. Доступ живёт в локальной
    player_links, поэтому недоступность Sheets вход не ломает."""
    try:
        from collect_votes import _init_sheets
        sheets_cache.write_player_tg_id(_init_sheets(), player_row, tg_user_id)
    except Exception as e:
        log.warning(f"фэнтези: числовой id не записан в лист: {e}")


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
                           "name": f"№{pp['number']}" if pp["number"] else f"ID {pid}",
                           "off_roster": True}

        for p in by_pid.values():
            ref = f"{pref}:{tid}:{p['pid']}"
            if ref in seen:
                continue
            seen.add(ref)
            pool.append({"ref": ref, "number": str(p["number"] or ""),
                         "name": p["name"], "team": team.get("name", ""),
                         "off_roster": bool(p.get("off_roster"))})

    # Один физический игрок может быть в ДВУХ лигах (SLPRO Farm + Инфобаскет) с
    # разными id. Склеиваем по ФИО в одну карточку с составной ссылкой
    # «slpro:..+ib:..» — очки суммируются, а не задваиваются.
    pool = _merge_pool_by_name(pool)
    _pool_cache["data"] = pool
    _pool_cache["at"] = now
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
    """Склеивает почти-одинаковые ФИО ИЗ РАЗНЫХ ЛИГ: та же фамилия, имя
    отличается ≤1 буквы (напр. «Шлепикас Роман» ↔ «Шлепикас Ромас»). Разные
    лиги — чтобы не слить двух разных людей внутри одной лиги. Фамилию требуем
    точную; более сложные расхождения — вручную/через связку id в «Игроки»."""
    result: List[Dict[str, Any]] = []
    for e in merged:
        if e.get("off_roster"):        # «имя» = номер, сравнивать нечего
            result.append(dict(e))
            continue
        eparts = _norm_name(e["name"]).split()
        e_sur = eparts[0] if eparts else ""
        e_first = eparts[1] if len(eparts) > 1 else ""
        e_src = _srcset(e["ref"])
        hit = None
        for g in result:
            if g.get("off_roster"):
                continue
            gparts = _norm_name(g["name"]).split()
            g_sur = gparts[0] if gparts else ""
            g_first = gparts[1] if len(gparts) > 1 else ""
            if e_sur and g_sur == e_sur and not (_srcset(g["ref"]) & e_src) and _lev1(e_first, g_first):
                hit = g
                break
        if hit:
            hit["ref"] = "+".join(sorted(set(hit["ref"].split("+")) | set(e["ref"].split("+"))))
            if not hit["number"]:
                hit["number"] = e["number"]
        else:
            result.append(dict(e))
    return result


def _merge_pool_by_name(pool: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_name: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    for p in pool:
        key = _norm_name(p["name"])
        # У выбывших из заявки «имя» — это «№13»: склеивать по нему нельзя,
        # иначе два разных игрока с одним номером из разных лиг слипнутся.
        if not key or p.get("off_roster"):
            key = p["ref"]
        if key not in by_name:
            by_name[key] = {"refs": [p["ref"]], "number": p["number"],
                            "name": p["name"], "team": p["team"],
                            "off_roster": bool(p.get("off_roster"))}
            order.append(key)
        else:
            by_name[key]["refs"].append(p["ref"])
            if not by_name[key]["number"]:
                by_name[key]["number"] = p["number"]
    merged = []
    for key in order:
        e = by_name[key]
        merged.append({"ref": "+".join(sorted(e["refs"])), "number": e["number"],
                       "name": e["name"], "team": e["team"],
                       "off_roster": e["off_roster"]})
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


TOP_PERIODS = ("last", "week", "month", "all")


def _period_bounds(period: str, scopes: Any) -> Tuple[Optional[str], Optional[str], str]:
    """(с какой даты, по какую, подпись) для среза топа игроков."""
    today = date.today()
    if period == "week":
        return (today - timedelta(days=7)).isoformat(), None, "за последние 7 дней"
    if period == "month":
        return (today - timedelta(days=30)).isoformat(), None, "за последние 30 дней"
    if period == "last":
        d = fantasy_stats.last_game_date(scopes)
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
    d_from, d_to, title = _period_bounds(period, scopes)
    agg = fantasy_stats.player_aggregates(weights, date_from=d_from, date_to=d_to, scope=scopes)

    rows = []
    for p in await build_pool():
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
    guessers = fantasy.top_participants(season["id"], d_from, d_to) if season else []
    return web.json_response({"period": period, "title": title,
                              "top": rows[:30], "guessers": guessers})


async def handle_admin_state(request: web.Request) -> web.Response:
    """Состояние админки: все активные лиги со своими настройками.

    Каждая лига отдаётся отдельно и правится по своему id — инлайн-кнопки в чате
    этого не умели и при двух активных лигах били по последней."""
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _is_admin(user):
        return web.json_response({"error": "forbidden"}, status=403)

    seasons = []
    for s in fantasy.active_seasons():
        scopes = fantasy.effective_scopes(s)
        seasons.append({
            "id": s["id"], "name": s["name"], "format": s.get("format", "3x3"),
            "roster_size": fantasy.roster_size(s),
            "max_per_player": fantasy.max_per_player(s),
            "weights": fantasy.season_weights(s),
            "scopes": scopes,
            "scopes_title": fantasy.scopes_title(scopes),
            "manual_scopes": bool(fantasy.season_scopes(s)),
        })
    return web.json_response({"seasons": seasons,
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
        out.append({"r": p["ref"], "m": p["name"], "s": s})
    return out


async def webapp_shared() -> Optional[Dict[str, Any]]:
    """Общая (одинаковая для всех игроков) часть payload: сезон, пул со
    статистикой, таблица, окно набора. Считаем один раз и переиспользуем в
    рассылке — у каждого игрока меняется только его собственный состав."""
    season = fantasy.get_active_season()
    if not season:
        return None
    pool = _compress_pool(_pool_with_stats(await build_pool(), season))
    week_start, sched_locked = fantasy.active_selection(season)
    table = fantasy.season_standings_live(season["id"])
    names = fantasy.display_names([str(r["user_id"]) for r in table])
    standings = [{"name": names.get(str(r["user_id"]), "Участник"),
                  "points": r["points"], "history": r.get("history", [])} for r in table]
    return {
        "season_id": season["id"],
        "season": {"name": season["name"], "format": season["format"],
                   "roster_size": fantasy.roster_size(season),
                   "max_per_player": fantasy.max_per_player(season)},
        "pool": pool,
        "week_start": week_start,
        "sched_locked": sched_locked,
        "standings": standings,
    }


def encode_webapp_payload(shared: Dict[str, Any], user_id: str) -> str:
    """Персональный payload = общая часть + состав игрока, base64url для #d=.
    Состав берём из БД по числовому id — тому же, под которым пишет живой вход,
    поэтому оба входа показывают один состав, а очки не задваиваются."""
    r = fantasy.get_roster(str(user_id), shared["season_id"], shared["week_start"])
    data = {
        "season": shared["season"],
        "pool": shared["pool"],
        "roster": r["refs"] if r else [],
        "locked": shared["sched_locked"] or (bool(r["locked"]) if r else False),
        "week_start": shared["week_start"],
        "standings": shared["standings"],
    }
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


async def build_webapp_payload(user_id: str) -> Optional[str]:
    """Payload запасного входа для одного игрока. None — нет активного сезона."""
    shared = await webapp_shared()
    return encode_webapp_payload(shared, user_id) if shared else None


async def handle_pool(request: web.Request) -> web.Response:
    user = _auth_user(request)
    if not user:
        return web.json_response({"error": "unauthorized"}, status=401)
    if not _can_view(user):
        return web.json_response({"error": "not_a_member"}, status=403)
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
    except (json.JSONDecodeError, TypeError):
        return web.json_response({"error": "bad_request"}, status=400)

    week_start, locked = fantasy.active_selection(season)
    if locked:
        # Игрок узнаёт о блокировке здесь (рассылок про неё больше нет), поэтому
        # отдаём подробности: с какого времени и почему.
        det = fantasy.lock_details()
        return web.json_response({"error": "locked", "since": det.get("started_hhmm", "")},
                                 status=409)

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
    table = fantasy.season_standings_live(season["id"])
    names = fantasy.display_names([r["user_id"] for r in table])
    for r in table:
        r["name"] = names.get(str(r["user_id"]), "")
    return web.json_response({"standings": table})


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
        web.get("/fantasy/top", handle_top),
        web.get("/fantasy/admin", handle_admin_state),
        web.post("/fantasy/admin", handle_admin_action),
        web.get("/health", lambda r: web.json_response({"ok": True})),
    ])
    return app
