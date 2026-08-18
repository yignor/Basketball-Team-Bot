#!/usr/bin/env python3
"""
Режимы участия в фэнтези (концепция от 29.07.2026).

Движок подсчёта не трогаем: очки по-прежнему фиксируются снимком по игре
([[fantasy-scoring-invariant]]). Режим — это ПРАВИЛА СБОРА состава и способ
посчитать его очки за игру:

- `free` — как было с самого начала: N игроков, без бюджета, одного можно
  взять несколько раз. Остаётся режимом по умолчанию, чтобы включение новых
  ничего не сломало у тех, кто уже играет.
- `budget` — «классическая команда» из концепции: бюджет очков на игру,
  состав в его рамках, начисление за ВСЕ действия. Цена игрока — из листа
  «Игроки» (её ведёт тренер).
- `category` — по одному игроку в каждую категорию. Игрок приносит очки
  ТОЛЬКО за свою категорию, зато с повышающим коэффициентом: иначе режим
  сильнее первого, ведь под каждую категорию берётся её лучший исполнитель.

Какие режимы включены, бюджет и коэффициент — настройки сезона (админка).
"""

from typing import Any, Dict, List, Optional, Tuple

FREE = "free"
BUDGET = "budget"
CATEGORY = "category"
ALL_MODES = (FREE, BUDGET, CATEGORY)

MODE_TITLES = {
    FREE: "Свободный",
    BUDGET: "Бюджет",
    CATEGORY: "По категориям",
}

# Пятая категория — очки; в концепции она названа как «Очки (или другая)».
DEFAULT_CATEGORIES = ["pts", "reb", "ast", "stl", "blk"]
CATEGORY_TITLES = {
    "pts": "Очки", "reb": "Подборы", "ast": "Передачи",
    "stl": "Перехваты", "blk": "Блок-шоты", "tur": "Потери",
}

# Карточка игрока по его цене. Тренер вправе поставить любую цену руками —
# уровень обязан следовать за ней, иначе значок разойдётся с ценником.
TIERS = ((70, "Элита"), (50, "Платина"), (30, "Золото"),
         (15, "Серебро"), (0, "Бронза"))


def tier_for(price: Any) -> str:
    try:
        value = int(price)
    except (TypeError, ValueError):
        return ""
    if value <= 0:
        return ""
    for edge, title in TIERS:
        if value >= edge:
            return title
    return "Бронза"


DEFAULT_BUDGET = 100
DEFAULT_MULTIPLIER = 1.0


def settings(season: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Настройки режимов сезона с безопасными значениями по умолчанию."""
    import fantasy
    s = fantasy.season_settings(season or {})
    modes = [m for m in (s.get("modes") or []) if m in ALL_MODES] or [FREE]
    cats = [c for c in (s.get("categories") or []) if c in CATEGORY_TITLES] or DEFAULT_CATEGORIES
    try:
        budget = int(s.get("budget") or DEFAULT_BUDGET)
    except (TypeError, ValueError):
        budget = DEFAULT_BUDGET
    # Повышающий коэффициент категорий отменён (решение 04.08.2026): режимы не
    # уравниваются между собой — у каждого своя таблица, сравнивать их всё
    # равно нельзя. Значение из старых настроек намеренно игнорируем: иначе
    # убранная из интерфейса кнопка продолжала бы молча множить очки на 1.5.
    return {"modes": modes, "budget": max(1, budget),
            "cat_multiplier": 1.0, "categories": cats}


def enabled(season: Optional[Dict[str, Any]]) -> List[str]:
    return settings(season)["modes"]


def default_mode(season: Optional[Dict[str, Any]]) -> str:
    """Режим, в котором сохраняется состав, если участник ничего не выбирал."""
    return enabled(season)[0]


def normalize(season: Optional[Dict[str, Any]], mode: Any) -> str:
    """Приводит присланный режим к включённому. Чужой/выключенный режим не
    ошибка на входе — просто откатываемся к первому включённому."""
    mode = str(mode or "").strip()
    return mode if mode in enabled(season) else default_mode(season)


def roster_size(season: Optional[Dict[str, Any]], mode: str) -> int:
    """Сколько игроков в составе. У категорийного режима — по числу категорий,
    у остальных — размер состава сезона."""
    import fantasy
    if mode == CATEGORY:
        return len(settings(season)["categories"])
    return fantasy.roster_size(season or {})


def validate(season: Optional[Dict[str, Any]], mode: str, refs: List[str],
             meta: Optional[Dict[str, Any]] = None,
             prices: Optional[Dict[str, int]] = None) -> Optional[str]:
    """Проверка состава по правилам режима. Код ошибки или None.

    Общие проверки (размер, пул, лимит копий) остаются в fantasy.validate_roster:
    здесь только то, что добавляет режим."""
    cfg = settings(season)
    if mode == BUDGET:
        if prices is None:
            return None            # цен нет (не смогли прочитать лист) — не мешаем
        total = sum(int(prices.get(r, 0)) for r in refs)
        if total > cfg["budget"]:
            return "over_budget"
        if any(not prices.get(r) for r in refs):
            return "no_price"
    elif mode == CATEGORY:
        cats = list((meta or {}).get("cats") or [])
        if len(cats) != len(refs) or set(cats) != set(cfg["categories"]):
            return "bad_categories"
        if len(set(refs)) != len(refs):
            return "duplicate_player"   # 1 игрок = 1 категория
    return None


def cost(refs: List[str], prices: Optional[Dict[str, int]]) -> int:
    return sum(int((prices or {}).get(r, 0)) for r in refs)


def game_points(season: Optional[Dict[str, Any]], mode: str, refs: List[str],
                meta: Optional[Dict[str, Any]], source: str, game_id: Any,
                weights: Dict[str, float]) -> float:
    """Очки состава за ОДНУ игру по правилам режима.

    free/budget — обычный подсчёт по всем действиям. category — только
    статистика выбранной категории, умноженная на коэффициент сезона."""
    import fantasy_stats
    if mode != CATEGORY:
        return fantasy_stats.game_points(refs, source, game_id, weights)

    cfg = settings(season)
    cats = list((meta or {}).get("cats") or [])
    if len(cats) != len(refs):
        # Состав без разметки категорий (например, пришёл из другого режима) —
        # считаем как обычный, но НЕ роняем начисление.
        return fantasy_stats.game_points(refs, source, game_id, weights)

    import sheets_cache
    sheets_cache.init_db()
    total = 0.0
    with sheets_cache.get_connection() as conn:
        for ref, cat in zip(refs, cats):
            for one in fantasy_stats.expand_refs([ref]):
                src, pid = fantasy_stats.parse_ref(one)
                if src != source:
                    continue
                row = conn.execute(
                    """SELECT * FROM game_player_stats
                       WHERE source = ? AND game_id = ? AND player_id = ?""",
                    (src, str(game_id), pid)).fetchone()
                if not row:
                    continue
                value = float(dict(row).get(cat) or 0)
                total += value * float(weights.get(cat, 1.0)) * cfg["cat_multiplier"]
    return round(total, 2)


def describe(season: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Описание включённых режимов — для экрана правил и админки."""
    cfg = settings(season)
    out = []
    for m in cfg["modes"]:
        item = {"id": m, "title": MODE_TITLES[m]}
        if m == BUDGET:
            item["budget"] = cfg["budget"]
        if m == CATEGORY:
            item["multiplier"] = cfg["cat_multiplier"]
            item["categories"] = [{"id": c, "title": CATEGORY_TITLES.get(c, c)}
                                  for c in cfg["categories"]]
        out.append(item)
    return out
