#!/usr/bin/env python3
"""
Ранги игроков и движение цены по форме.

Цена нужна режиму «Бюджет»: за 100 очков собираешь пятёрку. Чтобы цена не
была разовым снимком, ранг игрока меняется по последним играм — но не от
одного удачного матча:

- у каждого ранга своя полоса цен (бронза 5–14, серебро 15–29, золото 30–49,
  платина 50–69, элита 70–100);
- **потолок** — сколько надо набирать за игру, чтобы подняться в следующий ранг;
- **пол** — ниже какого уровня надо провалиться, чтобы выпасть в предыдущий;
- пороги СМЕЩЕНЫ друг относительно друга (гистерезис): войти в серебро — 8
  очков за игру, а выпасть из него — только ниже 6. Иначе игрок на границе
  прыгал бы туда-обратно после каждой игры.

Пороги посчитаны из той же кривой, которой считались стартовые цены: 8 / 14 /
21 / 28 фэнтези-очков за игру — это ровно цены 15 / 30 / 50 / 70. То есть
«потолок ранга» и «нижняя цена следующего» — одно и то же число, просто в
разных единицах.

Шкалу пересчитывали дважды, и оба раза по одному правилу: **поменял формулу —
подвинь пороги на столько же**, иначе команда меняет ранги, не изменив игру.
- 30.07.2026, промахи/фолы/дабл-даблы: новая ≈ 0.92·старой − 1.8, лестница
  9/16/23/30 стала 7/13/19/26;
- 30.07.2026, заработанные фолы (+1 за фол на игроке): новая ≈ 1.079·старой +
  0.39 (в среднем +1.42 очка за игру), лестница 7/13/19/26 стала 8/14/21/28.
Проверка одна и та же: прогнать последние 5 игр каждого по старой и новой
шкале — ранг должен совпасть. Во второй раз совпал у всех 20 из 20.

Окна (сколько последних игр смотреть) задаются в админке отдельно для подъёма
и падения: подняться сложнее — окно длиннее.

Двигается цена только у тех, кто в этой игре ИГРАЛ (см. recalc): форма берётся
по последним сыгранным матчам, и без этого правила пропустивший месяц получал
бы повышение в вечер, когда его даже не было в зале.
"""

from typing import Any, Dict, List, Optional, Tuple

BRONZE, SILVER, GOLD = "Бронза", "Серебро", "Золото"
PLATINUM, ELITE = "Платина", "Элита"
RANK_ORDER = (BRONZE, SILVER, GOLD, PLATINUM, ELITE)

# (нижняя цена, верхняя цена, порог подъёма, порог падения) — пороги в
# фэнтези-очках за игру. У бронзы падать некуда, у элиты расти некуда.
# Лестница ровная: каждый следующий ранг — плюс ~7 очков за игру (8/14/21/28),
# а порог падения на 2–3 ниже своего порога входа. Этот зазор и есть разница
# между полом и потолком: на границе игрок не прыгает туда-обратно.
RANKS: Dict[str, Dict[str, Any]] = {
    BRONZE:   {"low": 5,  "high": 14,  "up": 8.0,  "down": None},
    SILVER:   {"low": 15, "high": 29,  "up": 14.0, "down": 6.0},
    GOLD:     {"low": 30, "high": 49,  "up": 21.0, "down": 11.0},
    PLATINUM: {"low": 50, "high": 69,  "up": 28.0, "down": 18.0},
    ELITE:    {"low": 70, "high": 100, "up": None, "down": 24.0},
}

DEFAULT_UP_GAMES = 5        # подняться — доказать на дистанции
DEFAULT_DOWN_GAMES = 5      # упасть — тоже не с одного матча
DEFAULT_STEP = 3            # насколько цена двигается внутри ранга за игру


def settings(season: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Настройки движения цен (окна и шаг) — из настроек сезона."""
    import fantasy
    s = fantasy.season_settings(season or {})

    def num(key: str, default: int, lo: int, hi: int) -> int:
        try:
            return max(lo, min(hi, int(s.get(key) or default)))
        except (TypeError, ValueError):
            return default

    return {"up_games": num("rank_up_games", DEFAULT_UP_GAMES, 1, 20),
            "down_games": num("rank_down_games", DEFAULT_DOWN_GAMES, 1, 20),
            "step": num("price_step", DEFAULT_STEP, 1, 25)}


def rank_of(price: Any) -> str:
    """Ранг по цене. Цена вне полос (тренер поставил своё) — ближайший ранг."""
    try:
        value = int(price)
    except (TypeError, ValueError):
        return ""
    if value <= 0:
        return ""
    for name in reversed(RANK_ORDER):
        if value >= RANKS[name]["low"]:
            return name
    return BRONZE


def neighbour(rank: str, up: bool) -> Optional[str]:
    try:
        i = RANK_ORDER.index(rank)
    except ValueError:
        return None
    j = i + (1 if up else -1)
    return RANK_ORDER[j] if 0 <= j < len(RANK_ORDER) else None


def form_fp(refs: List[str], games: int) -> Tuple[float, int]:
    """(среднее фэнтези-очков за игру по последним N играм, сколько игр нашли).

    Игрок может числиться в двух лигах — берём его игры из обеих и сортируем
    по дате: форма про человека, а не про турнир."""
    import fantasy_stats
    import sheets_cache
    sheets_cache.init_db()
    rows: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        for one in fantasy_stats.expand_refs(refs):
            src, pid = fantasy_stats.parse_ref(one)
            rows.extend(dict(r) for r in conn.execute(
                """SELECT * FROM game_player_stats
                   WHERE source = ? AND player_id = ? AND game_date != ''
                   ORDER BY game_date DESC LIMIT ?""", (src, str(pid), games)))
    if not rows:
        return 0.0, 0
    rows.sort(key=lambda r: r.get("game_date") or "", reverse=True)
    recent = rows[:games]
    total = sum(fantasy_stats.fantasy_points(r) for r in recent)
    return round(total / len(recent), 2), len(recent)


def next_price(price: Any, refs: List[str],
               season: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Новая цена игрока после игры: {price, rank, moved, reason}.

    Ранг меняется только по форме и только при полном окне: пока игр меньше,
    чем окно, двигать человека не на чем — цена стоит. Внутри ранга цена
    ползёт к уровню формы шагом, но не выходит за полосу ранга."""
    cfg = settings(season)
    rank = rank_of(price)
    if not rank:
        return {"price": 0, "rank": "", "moved": 0, "reason": "нет цены"}
    try:
        cur = int(price)
    except (TypeError, ValueError):
        return {"price": 0, "rank": "", "moved": 0, "reason": "нет цены"}

    band = RANKS[rank]
    up_fp, up_n = form_fp(refs, cfg["up_games"])
    down_fp, down_n = form_fp(refs, cfg["down_games"])

    # Подъём: форма держит потолок ранга на всём окне.
    if band["up"] is not None and up_n >= cfg["up_games"] and up_fp >= band["up"]:
        higher = neighbour(rank, up=True)
        if higher:
            new = RANKS[higher]["low"]
            return {"price": new, "rank": higher, "moved": new - cur,
                    "reason": f"форма {up_fp} за {up_n} игр — выше потолка {band['up']}"}

    # Падение: форма ниже пола ранга на всём окне.
    if band["down"] is not None and down_n >= cfg["down_games"] and down_fp < band["down"]:
        lower = neighbour(rank, up=False)
        if lower:
            new = RANKS[lower]["high"]
            return {"price": new, "rank": lower, "moved": new - cur,
                    "reason": f"форма {down_fp} за {down_n} игр — ниже пола {band['down']}"}

    # Игр в базе нет вообще (новичок, травма, ещё не выкачали box-score) —
    # цена стоит. Иначе «форма 0» утащила бы человека на дно ранга за пару
    # пересчётов просто потому, что о нём ничего не известно.
    if not up_n and not down_n:
        return {"price": cur, "rank": rank, "moved": 0, "reason": "нет игр"}

    # Ранг тот же: цена подтягивается к форме внутри полосы.
    target = _price_for_fp(up_fp if up_n else down_fp)
    target = max(band["low"], min(band["high"], target))
    step = cfg["step"]
    new = cur + max(-step, min(step, target - cur))
    new = max(band["low"], min(band["high"], int(round(new))))
    return {"price": new, "rank": rank, "moved": new - cur,
            "reason": "движение внутри ранга" if new != cur else "без изменений"}


def _price_for_fp(fp: float) -> int:
    """Фэнтези-очки за игру -> цена по тем же порогам, что и ранги.

    Между узлами (8→15, 14→30, 21→50, 28→70) считаем линейно: точнее кривой
    здесь не нужно, цена всё равно двигается шагом."""
    points = [(0.0, 5), (8.0, 15), (14.0, 30), (21.0, 50), (28.0, 70), (38.0, 100)]
    if fp <= points[0][0]:
        return points[0][1]
    for (x1, y1), (x2, y2) in zip(points, points[1:]):
        if fp <= x2:
            k = (fp - x1) / (x2 - x1) if x2 > x1 else 0
            return int(round(y1 + k * (y2 - y1)))
    return points[-1][1]


def describe(season: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Пороги и окна для экрана правил и админки."""
    cfg = settings(season)
    return {
        "up_games": cfg["up_games"], "down_games": cfg["down_games"],
        "step": cfg["step"],
        "ranks": [{"rank": r, "low": RANKS[r]["low"], "high": RANKS[r]["high"],
                   "up": RANKS[r]["up"], "down": RANKS[r]["down"]} for r in reversed(RANK_ORDER)],
    }


# ── Пересчёт цен по итогам игры ───────────────────────────────────────────
#
# Правило владения столбцом: СТАРТОВАЯ ТОЧКА — то, что стоит в листе сейчас.
# Бот не помнит «свою» цену и не спорит с тренером: поправил руками — со
# следующей игры отсчёт пойдёт от новой цифры. Пишем только изменившиеся
# строки и только столбец «Стоимость».

def recalc(season: Optional[Dict[str, Any]] = None, spreadsheet: Any = None,
           dry_run: bool = False, source: Optional[str] = None,
           game_id: Any = None) -> Dict[str, Any]:
    """Пересчёт цен. {updated, checked, changes: [{name, old, new, reason}]}.

    source/game_id — двигаем ТОЛЬКО тех, кто есть в протоколе этой игры. Цена
    меняется по итогам матча, в котором человек играл: иначе пропустивший месяц
    получал повышение по старым играм в тот вечер, когда его даже не было в
    зале. Без игры (ручной прогон из админки) пересчитываются все."""
    import asyncio
    import fantasy
    import fantasy_api
    import sheets_cache

    season = season or fantasy.get_active_season()

    if spreadsheet is None and not dry_run:
        import report_common
        spreadsheet = report_common.init_sheets()
    if spreadsheet is not None:
        # Читаем лист заново: правки тренера должны попасть в расчёт ДО того,
        # как мы посчитаем от них новую цену.
        sheets_cache.sync_players(spreadsheet)

    prices = sheets_cache.get_player_prices()

    # Кто играл в этой игре. Пустой протокол (статистика ещё не приехала) — не
    # повод двигать всех: тогда лучше не двигать никого.
    only: Optional[set] = None
    if source and game_id:
        sheets_cache.init_db()
        with sheets_cache.get_connection() as conn:
            only = {(source, str(r["player_id"])) for r in conn.execute(
                "SELECT player_id FROM game_player_stats WHERE source = ? AND game_id = ?",
                (source, str(game_id)))}
        if not only:
            return {"updated": 0, "checked": 0, "dry_run": dry_run, "changes": [],
                    "skipped": "в протоколе игры нет игроков"}

    try:
        pool = asyncio.run(fantasy_api.build_pool(force=True, season=season))
    except RuntimeError:                       # уже внутри event loop
        loop = asyncio.new_event_loop()
        try:
            pool = loop.run_until_complete(fantasy_api.build_pool(force=True, season=season))
        finally:
            loop.close()

    # Строка листа -> все ссылки этого человека: одна фамилия может прийти
    # карточками из двух лиг, а цена у неё одна.
    by_row: Dict[int, Dict[str, Any]] = {}
    import fantasy_stats
    for card in pool:
        if only is not None and not any(
                fantasy_stats.parse_ref(one) in only
                for one in fantasy_stats.expand_refs([card["ref"]])):
            continue                       # в этой игре не играл — цену не трогаем
        pr = fantasy_api._lookup_price(card.get("name", ""), prices)
        row, price = pr.get("row"), int(pr.get("price") or 0)
        if not row or price <= 0:
            continue
        item = by_row.setdefault(int(row), {"price": price, "refs": [],
                                            "name": card.get("name", "")})
        item["refs"].append(card["ref"])

    changes: List[Dict[str, Any]] = []
    updates: Dict[int, int] = {}
    for row, item in by_row.items():
        res = next_price(item["price"], item["refs"], season)
        if res["price"] and res["price"] != item["price"]:
            updates[row] = res["price"]
            changes.append({"name": item["name"], "old": item["price"],
                            "new": res["price"], "rank": res["rank"],
                            "reason": res["reason"]})

    written = 0
    if updates and not dry_run:
        written = sheets_cache.write_player_prices(spreadsheet, updates)
        fantasy_api.invalidate_pool()
    changes.sort(key=lambda c: abs(c["new"] - c["old"]), reverse=True)
    return {"updated": written, "checked": len(by_row), "dry_run": dry_run,
            "changes": changes}


# ── Личный кабинет игрока ─────────────────────────────────────────────────
#
# Игрок должен видеть не «цену 63», а понятную дорогу: где он, что держит его
# здесь и что нужно сделать. Все числа берутся из того же движка, которым бот
# двигает цену, — иначе кабинет обещал бы одно, а пересчёт делал другое.

def progress(price: Any, refs: List[str],
             season: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Где игрок и что ему нужно: ранг, форма, сколько до подъёма и падения."""
    import fantasy_stats
    import sheets_cache

    # Одиночную ссылку принимаем как есть: строка развернулась бы посимвольно.
    if isinstance(refs, str):
        refs = [refs]
    cfg = settings(season)
    rank = rank_of(price)
    try:
        cur = int(price)
    except (TypeError, ValueError):
        cur = 0
    if not rank:
        return {"found": False, "reason": "нет цены"}
    band = RANKS[rank]

    # Последние игры человека — по ним считается всё остальное.
    sheets_cache.init_db()
    rows: List[Dict[str, Any]] = []
    window = max(cfg["up_games"], cfg["down_games"])
    with sheets_cache.get_connection() as conn:
        for one in fantasy_stats.expand_refs(refs):
            src, pid = fantasy_stats.parse_ref(one)
            rows.extend(dict(r) for r in conn.execute(
                """SELECT * FROM game_player_stats
                   WHERE source = ? AND player_id = ? AND game_date != ''
                   ORDER BY game_date DESC LIMIT ?""", (src, str(pid), window)))
    rows.sort(key=lambda r: r.get("game_date") or "", reverse=True)
    weights = None
    if season:
        import fantasy
        weights = fantasy.season_weights(season)
    games = [{"date": r.get("game_date", ""),
              "fp": fantasy_stats.fantasy_points(r, weights)} for r in rows[:window]]

    up_fp, up_n = form_fp(refs, cfg["up_games"])
    down_fp, down_n = form_fp(refs, cfg["down_games"])
    higher, lower = neighbour(rank, up=True), neighbour(rank, up=False)

    def need_next(threshold: Optional[float], games_n: int) -> Optional[float]:
        """Сколько набрать в СЛЕДУЮЩЕЙ игре, чтобы среднее окна вышло на порог.

        Окно скользит: самая старая игра из него выпадает, поэтому считаем от
        суммы последних (N-1), а не от текущего среднего."""
        if threshold is None:
            return None
        kept = [g["fp"] for g in games[:games_n - 1]]
        if len(kept) < games_n - 1:
            return None                    # игр ещё не хватает — окно не полное
        return round(threshold * games_n - sum(kept), 1)

    return {
        "found": True,
        "price": cur, "rank": rank, "low": band["low"], "high": band["high"],
        "up_rank": higher, "down_rank": lower,
        "up_threshold": band["up"], "down_threshold": band["down"],
        "up_games": cfg["up_games"], "down_games": cfg["down_games"], "step": cfg["step"],
        "form_up": up_fp, "form_up_games": up_n,
        "form_down": down_fp, "form_down_games": down_n,
        "need_up_next": need_next(band["up"], cfg["up_games"]),
        "keep_next": need_next(band["down"], cfg["down_games"]),
        "games": games,
        # Что бот сделает с ценой после ближайшей игры при нынешней форме —
        # тот же самый вызов, никакой отдельной «витринной» математики.
        "next": next_price(cur, refs, season),
    }

