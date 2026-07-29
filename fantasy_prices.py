#!/usr/bin/env python3
"""
Ранги игроков и движение цены по форме.

Цена нужна режиму «Бюджет»: за 100 очков собираешь пятёрку. Чтобы цена не
была разовым снимком, ранг игрока меняется по последним играм — но не от
одного удачного матча:

- у каждого ранга своя полоса цен (бронза 5–14, серебро 15–29, золото 30–49,
  платина 50–100);
- **потолок** — сколько надо набирать за игру, чтобы подняться в следующий ранг;
- **пол** — ниже какого уровня надо провалиться, чтобы выпасть в предыдущий;
- пороги СМЕЩЕНЫ друг относительно друга (гистерезис): войти в серебро — 9
  очков за игру, а выпасть из него — только ниже 7. Иначе игрок на границе
  прыгал бы туда-обратно после каждой игры.

Пороги посчитаны из той же кривой, которой считались стартовые цены: 9 / 16 /
23 фэнтези-очка за игру — это ровно цены 15 / 30 / 50. То есть «потолок ранга»
и «нижняя цена следующего» — одно и то же число, просто в разных единицах.

Окна (сколько последних игр смотреть) задаются в админке отдельно для подъёма
и падения: подняться сложнее — окно длиннее.
"""

from typing import Any, Dict, List, Optional, Tuple

BRONZE, SILVER, GOLD, PLATINUM = "Бронза", "Серебро", "Золото", "Платина"
RANK_ORDER = (BRONZE, SILVER, GOLD, PLATINUM)

# (нижняя цена, верхняя цена, порог подъёма, порог падения) — пороги в
# фэнтези-очках за игру. У бронзы падать некуда, у платины расти некуда.
RANKS: Dict[str, Dict[str, Any]] = {
    BRONZE:   {"low": 5,  "high": 14,  "up": 9.0,  "down": None},
    SILVER:   {"low": 15, "high": 29,  "up": 16.0, "down": 7.0},
    GOLD:     {"low": 30, "high": 49,  "up": 23.0, "down": 13.0},
    PLATINUM: {"low": 50, "high": 100, "up": None, "down": 19.0},
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

    Между узлами (9→15, 16→30, 23→50) считаем линейно: точнее кривой здесь
    не нужно, цена всё равно двигается шагом."""
    points = [(0.0, 5), (9.0, 15), (16.0, 30), (23.0, 50), (35.0, 100)]
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
