#!/usr/bin/env python3
"""
Клиент JSON-API лиги SLPRO (basketstat.su).

Официально «API нет» — сайт https://slpro.basketstat.ru это Vue SPA, но за
ним лежит полноценный JSON-API, доступный анонимно. Все запросы:
    POST https://api.basketstat.su/v1/{route}
    тело JSON с "url":"{route}", "tournament": slug, для защищённых
    роутов — "auth_token":"" (пустая строка = аноним, проходит).

Сезон/дивизион/стадия/team_id НЕ хардкодятся — discover_context() находит
их по имени команды через settings + tournament/teams, поэтому смена
сезона лигой не требует правок.
"""

import asyncio
import re
import time
from typing import Any, Dict, List, Optional

import aiohttp

API_BASE = "https://api.basketstat.su/v1"
ORIGIN = "https://slpro.basketstat.ru"
DEFAULT_TOURNAMENT = "slpro"

# ── Предохранитель на недоступный API ────────────────────────────────────────
#
# Один неудачный запрос стоит 20с × 3 попытки + паузы ≈ 66 секунд. Пока API
# отвечал, это была разумная страховка от сетевых судорог. Когда он перестаёт
# отвечать совсем (упал сайт, маршрут увёл трафик не туда), каждая мелочь
# начинает стоить минуту: игрок жмёт /start, а бот в это время трижды ждёт
# ответа от мёртвого хоста. За минуту таких «мелочей» набирается на пять.
#
# Поэтому: первая же серия неудач переводит клиент в режим «лига недоступна»
# на COOLDOWN секунд — все запросы отвечают None мгновенно. Первый запрос
# после паузы идёт по-настоящему и, если API ожил, снимает предохранитель.
_DOWN_COOLDOWN = 300.0
_down_until: float = 0.0
_down_reason: str = ""


def api_down() -> Optional[str]:
    """Причина, по которой API считается недоступным (или None)."""
    return _down_reason if time.time() < _down_until else None


def _mark_down(reason: str) -> None:
    global _down_until, _down_reason
    _down_until = time.time() + _DOWN_COOLDOWN
    _down_reason = reason
    print(f"⚠️ SLPRO: API не отвечает ({reason}). Пропускаю обращения "
          f"{int(_DOWN_COOLDOWN / 60)} минут, чтобы бот не ждал таймаутов.")


def _mark_up() -> None:
    global _down_until, _down_reason
    if _down_reason:
        print("✅ SLPRO: API снова отвечает.")
    _down_until, _down_reason = 0.0, ""


def _normalize_name(name: Optional[str]) -> str:
    """Нормализация названия команды для сравнения (как в enhanced_game_parser)."""
    if not isinstance(name, str):
        return ""
    return re.sub(r"[\s\-_/.]", "", name.strip().lower())


class SlproClient:
    def __init__(self, tournament: str = DEFAULT_TOURNAMENT, timeout: float = 20.0):
        self.tournament = tournament
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def _post(self, route: str, retries: int = 3, base_delay: float = 2.0,
                    **params: Any) -> Optional[Dict[str, Any]]:
        """POST на {API_BASE}/{route}. Подставляет url/tournament/auth_token.
        Возвращает распарсенный JSON или None при ошибке (с ретраями)."""
        payload: Dict[str, Any] = {
            "url": route,
            "tournament": self.tournament,
            "auth_token": "",
        }
        payload.update(params)
        headers = {"Origin": ORIGIN, "Content-Type": "application/json"}

        down = api_down()
        if down:
            return None                      # предохранитель: не ждём таймаутов

        last_error: Optional[str] = None
        network_failed = False
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.post(f"{API_BASE}/{route}", json=payload, headers=headers) as resp:
                        text = await resp.text()
                        if not (200 <= resp.status < 300):
                            last_error = f"HTTP {resp.status}: {text[:200]}"
                        else:
                            data = await resp.json(content_type=None)
                            if isinstance(data, dict) and data.get("error"):
                                last_error = f"API error: {data['error']}"
                            else:
                                _mark_up()
                                return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = f"{type(e).__name__}: {e}"
                network_failed = True
            if attempt < retries - 1:
                await asyncio.sleep(base_delay * (2 ** attempt))

        print(f"⚠️ SLPRO {route}: не удалось получить данные ({last_error})")
        # Сеть молчит — это про хост целиком, а не про конкретный запрос:
        # остальным обращениям ждать того же таймаута незачем. Ответ самого
        # API об ошибке (HTTP 4xx/5xx с телом) предохранителем не считаем —
        # хост жив, а значит следующий запрос может и получиться.
        if network_failed:
            _mark_down(last_error or "нет ответа")
        return None

    # ── Справочники ──────────────────────────────────────────────────────────

    async def get_settings(self) -> Optional[Dict[str, Any]]:
        return await self._post("settings")

    async def iter_stages(self) -> List[Dict[str, Any]]:
        """Все стадии всех сезонов турнира: [{season_id, season, division_id,
        division_name, stage_id, group_id, active}]. Активные — первыми."""
        settings = await self.get_settings()
        if not settings or "seasons" not in settings:
            return []
        stages: List[Dict[str, Any]] = []
        for season in settings.get("seasons", []):
            for division in season.get("divisions", []):
                for stage in division.get("stages", []):
                    stages.append({
                        "season_id": season.get("season_id"),
                        "season": season.get("season"),
                        "division_id": division.get("division_id"),
                        "division": division.get("division"),
                        "division_name": division.get("division_name"),
                        "stage_id": stage.get("stage_id"),
                        "group_id": (stage.get("groups") or [{}])[0].get("group_id"),
                        "active": bool(stage.get("active")),
                    })
        stages.sort(key=lambda s: not s["active"])
        return stages

    async def discover_context(self, team_names: List[str]) -> Optional[Dict[str, Any]]:
        """Находит текущую (активную) стадию, в которой играет наша команда,
        по имени. Возвращает {season_id, division_id, stage_id, group_id,
        team_id, team_name, season, division_name} или None.

        Перебираем активные стадии всех сезонов (active-флаг у стадии), в
        каждой запрашиваем tournament/teams и ищем совпадение имени. Так id
        не хардкодятся и переживают смену сезона."""
        wanted = {_normalize_name(n) for n in team_names if n}
        if not wanted:
            return None

        # Кандидаты-стадии: сперва active, затем остальные (fallback).
        for ctx in await self.iter_stages():
            teams = await self.get_standings(ctx)
            for team in teams:
                if _normalize_name(team.get("name")) in wanted:
                    return {
                        **ctx,
                        "team_id": team.get("team_id"),
                        "team_name": team.get("name"),
                    }
        return None

    async def get_standings(self, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = await self._post(
            "tournament/teams",
            season_id=ctx.get("season_id"),
            division_id=ctx.get("division_id"),
            stage_id=ctx.get("stage_id"),
        )
        return (data or {}).get("teams", []) or []

    async def get_schedule(self, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = await self._post(
            "tournament/schedule",
            season_id=ctx.get("season_id"),
            division_id=ctx.get("division_id"),
            stage_id=ctx.get("stage_id"),
        )
        return (data or {}).get("schedule", []) or []

    async def get_roster(self, team_id: int) -> List[Dict[str, Any]]:
        data = await self._post("teams/get-players", team_id=team_id)
        return (data or {}).get("players", []) or []

    async def get_player_info(self, player_id: Any) -> Optional[Dict[str, Any]]:
        """Карточка игрока: `career[]` (сезон × команда + агрегат сезона) и
        `last_games[]` (последние 10). Нужна, чтобы проверить, что присланный
        id вообще существует, и показать разбивку по сезонам, как на сайте.

        `player_id` шлём СТРОКОЙ: на числе их бэкенд падает в 500
        («trim(): Argument #1 must be of type string, int given»).
        Роут найден в чанке страницы игрока (759.js) — перебором не давался.
        """
        data = await self._post("players/get-player-info", player_id=str(player_id))
        player = (data or {}).get("player")
        return player if isinstance(player, dict) else None

    async def get_game(self, game_id: Any, ctx: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Полный box-score одной игры: {game, players:{home_players,
        guest_players}, log:[...]}. Требует tournament_id + season_id — берём
        из ctx (discover_context) либо дефолт (tournament_id=2)."""
        params: Dict[str, Any] = {"game_id": str(game_id), "tournament_id": 2}
        if ctx:
            if ctx.get("season_id") is not None:
                params["season_id"] = ctx["season_id"]
        return await self._post("games", **params)

    async def get_our_games(self, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Игры нашей команды (по team_id из ctx) из расписания стадии."""
        team_id = ctx.get("team_id")
        if team_id is None:
            return []
        games = await self.get_schedule(ctx)
        return [g for g in games if g.get("home_id") == team_id or g.get("guest_id") == team_id]


async def _demo() -> None:
    """Ручная проверка: python3 slpro_client.py"""
    client = SlproClient()
    ctx = await client.discover_context(["PullUp Farm", "Pull Up Farm"])
    print("context:", ctx)
    if ctx:
        games = await client.get_our_games(ctx)
        print(f"наших игр: {len(games)}")
        for g in games:
            print(f"  {g['game_id']} {g['game_date']} {g['game_time']} "
                  f"{g['home_name']} {g['home_score']}:{g['guest_score']} {g['guest_name']} "
                  f"(status={g['status']})")


if __name__ == "__main__":
    asyncio.run(_demo())


# ─────────────── Лига из листа «Конфиг» (а не из env) ────────────────────────

_SLPRO_TYPES = {"SLPRO", "СЛПРО", "SL PRO", "SL-PRO"}
# Заголовок таблицы (и его вариант из памятки) — не данные.
_HEADER_CELLS = {"ТИП", "ИД", "ИД КОМАНДЫ"}


def env_team_names() -> List[str]:
    """Названия нашей команды в SLPRO из env (запасной путь, если «Конфиг»
    не заполнен). Одно определение на весь проект — раньше эта строчка была
    скопирована в пяти местах с разными дефолтами."""
    import os
    raw = os.getenv("SLPRO_TEAM_NAMES", "").strip()
    if not raw:
        return ["PullUp Farm", "Pull Up Farm", "Pull-Up Farm"]
    return [n.strip() for n in raw.split(",") if n.strip()]


def leagues_from_config() -> List[Dict[str, Any]]:
    """Турниры SLPRO из листа «Конфиг».

    Формат под то, что человек ВИДИТ, а не под внутренние id:
        ТИП        = SLPRO
        ИД         = код дивизиона из ссылки (например SUMC)
        ИД КОМАНДЫ = НАЗВАНИЕ команды (числового id команды на сайте не видно)
        АЛЬТ. ИМЯ  = как показывать

    Числовые season_id/stage_id бот находит сам по коду дивизиона и названию —
    заставлять админа выковыривать их из запросов неправильно.

    Пустой список — не ошибка: значит SLPRO в «Конфиге» не заведён, и вызывающий
    откатывается на автоопределение по названию команды из env.
    """
    try:
        import config_sheet
        import sheets_cache
        rows = config_sheet.split(sheets_cache.get_config_rows() or [])[config_sheet.GAME]
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        cells = [str(c or "").strip() for c in list(row) + [""] * 4]
        if cells[0].upper() not in _SLPRO_TYPES:
            continue
        division, team_name, alt = cells[1].strip(), cells[2].strip(), cells[3].strip()
        if not team_name or team_name.upper() in _HEADER_CELLS:
            continue
        key = (division.upper(), _normalize_name(team_name))
        if key in seen:
            continue
        seen.add(key)
        out.append({"source": "slpro", "division": division.upper(),
                    "team_name": team_name,
                    "name": alt or f"SLPRO {division.upper()}".strip()})
    return out


def config_team_names() -> List[str]:
    """Названия команд SLPRO из «Конфига» (для сообщений и автоопределения)."""
    return [lg["team_name"] for lg in leagues_from_config() if lg.get("team_name")]


# Резолв ходит в сеть (settings + tournament/teams по стадиям), а зовут его
# и демон, и админка, и cron. Кэшируем на 10 минут; пустой результат — на
# минуту, чтобы сетевой сбой не «залипал» на весь интервал.
_ctx_cache: Dict[str, Any] = {"at": 0.0, "key": None, "data": None}
_CTX_TTL = 600.0
_CTX_TTL_EMPTY = 60.0


async def resolve_config_contexts(force: bool = False) -> List[Dict[str, Any]]:
    """Достраивает строки «Конфига» до полноценных контекстов: находит
    season_id, division_id, stage_id, group_id и team_id по коду дивизиона и
    названию команды. Возвращает то же, что discover_context, плюс `name`.

    Почему не discover_context: у SLPRO ВСЕ стадии помечены active, поэтому
    поиск по одному лишь названию берёт первый попавшийся сезон — как только
    команда сыграет два сезона подряд, бот уедет в прошлогодний. Код дивизиона
    из «Конфига» снимает эту неоднозначность.
    """
    import time
    rows = leagues_from_config()
    key = repr([(r["division"], r["team_name"]) for r in rows])
    age = time.time() - _ctx_cache["at"]
    ttl = _CTX_TTL if _ctx_cache["data"] else _CTX_TTL_EMPTY
    if not force and _ctx_cache["key"] == key and age < ttl:
        return list(_ctx_cache["data"] or [])
    if not rows:
        _ctx_cache.update(at=time.time(), key=key, data=[])
        return []

    client = SlproClient()
    stages = await client.iter_stages()
    if not stages:
        # Отличаем «API недоступен» от «команду не нашли»: раньше обе ситуации
        # выглядели как «не нашёл ни одной стадии».
        print("⚠️ SLPRO «Конфиг»: справочник сезонов не получен (сеть/API), "
              "турниры не резолвятся")
        _ctx_cache.update(at=time.time(), key=key, data=[])
        return []

    standings: Dict[Any, List[Dict[str, Any]]] = {}
    out: List[Dict[str, Any]] = []
    for cfg in rows:
        # Дивизион указан — сузим поиск; не указан — ищем по всем стадиям.
        cands = [st for st in stages
                 if not cfg["division"] or str(st.get("division", "")).upper() == cfg["division"]]
        if cfg["division"] and not cands:
            print(f"⚠️ SLPRO «Конфиг»: дивизиона {cfg['division']} нет в справочнике "
                  f"(проверь код в адресе турнира)")
            continue
        found = None
        for st in cands:
            sid = st.get("stage_id")
            if sid not in standings:
                standings[sid] = await client.get_standings(st)
            for team in standings[sid]:
                if _normalize_name(team.get("name")) == _normalize_name(cfg["team_name"]):
                    found = {**st, "team_id": team.get("team_id"),
                             "team_name": team.get("name")}
                    break
            if found:
                break
        if not found:
            print(f"⚠️ SLPRO «Конфиг»: не нашёл команду «{cfg['team_name']}» "
                  f"в дивизионе {cfg['division'] or '(любом)'}")
            continue
        out.append({**found, "source": "slpro", "name": cfg["name"]})

    _ctx_cache.update(at=time.time(), key=key, data=out)
    return list(out)


async def team_contexts(team_names: Optional[List[str]] = None,
                        force: bool = False) -> List[Dict[str, Any]]:
    """Все турниры SLPRO, в которых играет команда.

    Порядок источников: лист «Конфиг» → автоопределение по названию (env).
    Список, а не один контекст: команда может играть в двух турнирах сразу
    (кубок + регулярка), и админ заводит их строками в таблице."""
    ctxs = await resolve_config_contexts(force=force)
    if ctxs:
        return ctxs
    ctx = await SlproClient().discover_context(team_names or env_team_names())
    return [ctx] if ctx else []


def scope_of(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Контекст → турнир подсчёта очков фэнтези (season_id + stage_id + имя)."""
    name = ctx.get("name") or (f"SLPRO {ctx.get('season')} · "
                               f"{ctx.get('division_name') or ctx.get('division')}")
    return {"source": "slpro", "season_id": str(ctx.get("season_id") or ""),
            "stage_id": str(ctx.get("stage_id") or ""), "name": name}

