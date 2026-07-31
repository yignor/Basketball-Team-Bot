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
    foul_on: int = 0             # фолы, заработанные игроком (на нём сфолили)
    time_played: int = 0
    plus_minus: int = 0
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

        # Плюс-минус. В событии заброшенного мяча приходит массив `players` —
        # кто в этот момент был на площадке. Значит считается точно, а не по
        # прикидке: своим на паркете плюсуем очки, чужим — минусуем.
        if action == _MADE and team_id is not None:
            for on_court in (e.get("players") or []):
                q = box.players.get(on_court)
                if q is None:
                    continue
                same_side = (team_id == box.home_id) == q.is_home
                q.plus_minus += pts if same_side else -pts

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
        elif action == "foul_on_player":
            # Отдельное событие: на ком сфолили. В протоколе оно есть почти у
            # каждого фола (31 из 32 в проверенной игре), поэтому заработанные
            # фолы можно считать честно, а не выводить из штрафных бросков.
            p.foul_on += 1

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


def format_leaders(box: BoxScore, our_team_id: int,
                   won: Optional[bool] = None,
                   jokes: Any = None) -> str:
    """Блок игроков для сообщения о результате — как у Инфобаскета.

    Логика та же и намеренно вывернутая: **выиграли — показываем, что нужно
    улучшить; проиграли — кто всё-таки вытащил**. Хвалить после победы незачем,
    а после поражения важно, чтобы люди не расходились с одним чувством вины.

    По одному игроку на показатель: список из трёх фамилий никто не читает.
    Имена транзитные (display_name), в хранилище не идут. `jokes` — реестр
    шуток от своих (player_jokes.Jokes), дописывает фразу к строке игрока."""
    ours = [p for p in box.team_players(our_team_id) if p.pts or p.reb or p.ast or p.pf]
    if not ours:
        return ""

    def nm(p: Any) -> str:
        return p.display_name or f"№{p.number}"

    def joke(p: Any) -> str:
        return jokes.for_name(p.display_name) if (jokes and p.display_name) else ""

    def pct(made: int, att: int) -> int:
        return round(made / att * 100) if att else 0

    # Сначала решаем, КОМУ достанутся фразы: иначе они всегда доставались бы
    # первым строкам блока, а до последних не доходило никогда.
    if jokes is not None:
        heroes = []
        ft = [p for p in ours if p.fta >= 2]
        two = [p for p in ours if p.fg2a >= 3]
        three = [p for p in ours if p.fg3a >= 3]
        if won:
            if ft:
                heroes.append(min(ft, key=lambda p: pct(p.ftm, p.fta)))
            if two:
                heroes.append(min(two, key=lambda p: pct(p.fg2m, p.fg2a)))
            if three:
                heroes.append(min(three, key=lambda p: pct(p.fg3m, p.fg3a)))
            heroes += [max(ours, key=lambda p: p.tur), max(ours, key=lambda p: p.pf),
                       min(ours, key=lambda p: p.efficiency)]
        else:
            heroes += [max(ours, key=lambda p: p.pts), max(ours, key=lambda p: p.reb),
                       max(ours, key=lambda p: p.ast), max(ours, key=lambda p: p.stl),
                       max(ours, key=lambda p: p.efficiency)]
        jokes.plan([h.display_name for h in heroes if h and h.display_name])

    lines: List[str] = []
    if won:
        lines.append("😅 ЧТО НУЖНО УЛУЧШИТЬ:")
        # Проценты считаем только тем, кто реально бросал: 0% с одной попытки
        # — не показатель, а случайность.
        ft = [p for p in ours if p.fta >= 2]
        if ft:
            w = min(ft, key=lambda p: pct(p.ftm, p.fta))
            lines.append(f"🏀 Штрафные: {nm(w)} — {pct(w.ftm, w.fta)}% "
                         f"({w.ftm}/{w.fta}){joke(w)}")
        two = [p for p in ours if p.fg2a >= 3]
        if two:
            w = min(two, key=lambda p: pct(p.fg2m, p.fg2a))
            lines.append(f"🎯 Двухочковые: {nm(w)} — {pct(w.fg2m, w.fg2a)}% "
                         f"({w.fg2m}/{w.fg2a}){joke(w)}")
        three = [p for p in ours if p.fg3a >= 3]
        if three:
            w = min(three, key=lambda p: pct(p.fg3m, p.fg3a))
            lines.append(f"🎯 Трехочковые: {nm(w)} — {pct(w.fg3m, w.fg3a)}% "
                         f"({w.fg3m}/{w.fg3a}){joke(w)}")
        w = max(ours, key=lambda p: p.tur)
        if w.tur:
            lines.append(f"💥 Потери: {nm(w)} — {w.tur}{joke(w)}")
        w = max(ours, key=lambda p: p.pf)
        if w.pf:
            lines.append(f"⚠️ Фолы: {nm(w)} — {w.pf}{joke(w)}")
        # Шестая строка — как у Инфобаскета: там блок ровно такой же, и два
        # разных набора показателей у одного бота выглядели бы небрежностью.
        w = min(ours, key=lambda p: p.efficiency)
        lines.append(f"📉 КПИ: {nm(w)} — {w.efficiency}{joke(w)}")
    else:
        lines.append("🏆 ЛУЧШИЕ ИГРОКИ:")
        w = max(ours, key=lambda p: p.pts)
        if w.pts:
            lines.append(f"🥇 Очки: {nm(w)} — {w.pts}{joke(w)}")
        w = max(ours, key=lambda p: p.reb)
        if w.reb:
            lines.append(f"🏀 Подборы: {nm(w)} — {w.reb}{joke(w)}")
        w = max(ours, key=lambda p: p.ast)
        if w.ast:
            lines.append(f"🎯 Передачи: {nm(w)} — {w.ast}{joke(w)}")
        w = max(ours, key=lambda p: p.stl)
        if w.stl:
            lines.append(f"🥷 Перехваты: {nm(w)} — {w.stl}{joke(w)}")
        w = max(ours, key=lambda p: p.efficiency)
        lines.append(f"📈 КПИ: {nm(w)} — {w.efficiency}{joke(w)}")
    return "\n".join(lines) if len(lines) > 1 else ""
