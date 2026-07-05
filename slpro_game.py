#!/usr/bin/env python3
"""
Разбор box-score игры SLPRO (эндпоинт `games`) в нормализованную структуру:
счёт по четвертям, статистика по игрокам, лидеры/MVP, ссылка на VK-видео.

Юр-инвариант (см. память legal-data-invariant): имя игрока допускается ТОЛЬКО
для разового отображения (сообщение о результате, транзитно из публичного
API) — в поле `display_name`. В хранимую аналитику класть исключительно
`player_id` + `number`, поле `display_name` НЕ сохранять.

Семантика лога (`log[].action`):
  points  — забито, points=1/2/3 (штрафной/2-очковый/3-очковый)
  miss    — промах, points=1/2/3 (стоимость попытки)
  rebD/rebA — подбор в защите/нападении
  ast/stl/tur — передача/перехват/потеря
  block/block_on_player — блок-шот поставлен/получен
  foul/foul_on_player/unsportfoul — фол совершён/получен/неспортивный
  status/timeout/contest — служебные (не считаем в индивидуальную статистику)
`team_id` в логе — строка, приводим к int.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class PlayerStat:
    player_id: int
    number: str
    is_home: bool
    display_name: str = ""       # только для разового показа, НЕ хранить
    pts: int = 0
    ftm: int = 0
    fta: int = 0
    fg2m: int = 0
    fg2a: int = 0
    fg3m: int = 0
    fg3a: int = 0
    reb_o: int = 0
    reb_d: int = 0
    ast: int = 0
    stl: int = 0
    tur: int = 0
    blk: int = 0
    pf: int = 0
    time_played: int = 0
    start_five: bool = False

    @property
    def reb(self) -> int:
        return self.reb_o + self.reb_d

    @property
    def efficiency(self) -> int:
        """Простой КПИ (в духе NBA EFF): результативные действия минус
        промахи и потери."""
        missed = (self.fta - self.ftm) + (self.fg2a - self.fg2m) + (self.fg3a - self.fg3m)
        return self.pts + self.reb + self.ast + self.stl + self.blk - missed - self.tur


@dataclass
class BoxScore:
    game_id: int
    game_date: str
    game_time: str
    arena: str
    home_id: int
    guest_id: int
    home_name: str
    guest_name: str
    home_score: int
    guest_score: int
    video_vk: str = ""
    quarters: List[Tuple[int, int]] = field(default_factory=list)   # (home, guest) по периодам
    players: Dict[int, PlayerStat] = field(default_factory=dict)     # player_id -> PlayerStat

    def team_players(self, team_id: int) -> List[PlayerStat]:
        is_home = (team_id == self.home_id)
        return [p for p in self.players.values() if p.is_home == is_home]

    def mvp(self, team_id: Optional[int] = None) -> Optional[PlayerStat]:
        pool = self.team_players(team_id) if team_id is not None else list(self.players.values())
        pool = [p for p in pool if p.pts or p.reb or p.ast]
        return max(pool, key=lambda p: p.efficiency, default=None)


_MADE = "points"
_MISS = "miss"


def parse_box_score(resp: Dict[str, Any]) -> Optional[BoxScore]:
    """resp — ответ эндпоинта `games` ({game, players, log})."""
    if not resp or "game" not in resp:
        return None
    g = resp["game"]

    box = BoxScore(
        game_id=g.get("game_id"),
        game_date=g.get("game_date", ""),
        game_time=g.get("game_time", ""),
        arena=g.get("game_address", ""),
        home_id=g.get("home_id"),
        guest_id=g.get("guest_id"),
        home_name=g.get("home_name", ""),
        guest_name=g.get("guest_name", ""),
        home_score=g.get("home_score", 0),
        guest_score=g.get("guest_score", 0),
        video_vk=(g.get("video_vk") or "").strip(),
    )

    # Составы (player_id -> заготовка со стороной/номером/именем-для-показа)
    players_block = resp.get("players") or {}
    for side_key, is_home in (("home_players", True), ("guest_players", False)):
        for pl in players_block.get(side_key, []) or []:
            pid = pl.get("player_id")
            if pid is None:
                continue
            box.players[pid] = PlayerStat(
                player_id=pid,
                number=str(pl.get("number", "") or ""),
                is_home=is_home,
                display_name=f"{pl.get('surname', '')} {pl.get('name', '')}".strip(),
                time_played=pl.get("time_played", 0) or 0,
                start_five=bool(pl.get("start_five")),
            )

    # Разбор лога
    quarter_pts: Dict[int, List[int]] = {}   # period -> [home, guest]
    for e in resp.get("log", []) or []:
        action = e.get("action")
        pid = e.get("player_id")
        try:
            team_id = int(e.get("team_id"))
        except (TypeError, ValueError):
            team_id = None
        pts = e.get("points") or 0

        # Счёт по четвертям (по забитым)
        if action == _MADE and team_id is not None:
            period = e.get("period") or 0
            slot = quarter_pts.setdefault(period, [0, 0])
            if team_id == box.home_id:
                slot[0] += pts
            elif team_id == box.guest_id:
                slot[1] += pts

        # Индивидуальная статистика
        p = box.players.get(pid) if pid is not None else None
        if p is None:
            continue
        if action == _MADE:
            p.pts += pts
            if pts == 1:
                p.ftm += 1; p.fta += 1
            elif pts == 2:
                p.fg2m += 1; p.fg2a += 1
            elif pts == 3:
                p.fg3m += 1; p.fg3a += 1
        elif action == _MISS:
            if pts == 1:
                p.fta += 1
            elif pts == 2:
                p.fg2a += 1
            elif pts == 3:
                p.fg3a += 1
        elif action == "rebD":
            p.reb_d += 1
        elif action == "rebA":
            p.reb_o += 1
        elif action == "ast":
            p.ast += 1
        elif action == "stl":
            p.stl += 1
        elif action == "tur":
            p.tur += 1
        elif action == "block":
            p.blk += 1
        elif action in ("foul", "unsportfoul"):
            p.pf += 1

    box.quarters = [(quarter_pts[per][0], quarter_pts[per][1]) for per in sorted(quarter_pts)]
    return box


# ─────────────────────────── Форматирование ──────────────────────────────────

def format_quarters(box: BoxScore, our_team_id: int) -> str:
    """Счёт по четвертям в порядке наши:соперник (учитываем сторону)."""
    parts = []
    for home, guest in box.quarters:
        if our_team_id == box.home_id:
            parts.append(f"{home}:{guest}")
        else:
            parts.append(f"{guest}:{home}")
    return " · ".join(parts)


def format_leaders(box: BoxScore, our_team_id: int, top_scorers: int = 3) -> str:
    """Короткий блок лидеров нашей команды для сообщения о результате.
    Имена берём транзитно (display_name) — в хранилище это не идёт."""
    ours = [p for p in box.team_players(our_team_id) if p.pts or p.reb or p.ast]
    if not ours:
        return ""
    lines = []
    mvp = box.mvp(our_team_id)
    if mvp:
        name = mvp.display_name or f"№{mvp.number}"
        lines.append(f"⭐️ MVP: {name} — {mvp.pts} очк, {mvp.reb} подб, {mvp.ast} пас (КПИ {mvp.efficiency})")
    top = sorted(ours, key=lambda p: p.pts, reverse=True)[:top_scorers]
    scorers = ", ".join(f"{(p.display_name or ('№'+p.number))} {p.pts}" for p in top)
    if scorers:
        lines.append(f"🏀 Очки: {scorers}")
    top_reb = max(ours, key=lambda p: p.reb, default=None)
    top_ast = max(ours, key=lambda p: p.ast, default=None)
    extra = []
    if top_reb and top_reb.reb:
        extra.append(f"подборы — {(top_reb.display_name or ('№'+top_reb.number))} ({top_reb.reb})")
    if top_ast and top_ast.ast:
        extra.append(f"передачи — {(top_ast.display_name or ('№'+top_ast.number))} ({top_ast.ast})")
    if extra:
        lines.append("📊 " + "; ".join(extra))
    return "\n".join(lines)
