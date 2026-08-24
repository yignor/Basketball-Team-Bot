"""Тайм-коды присутствия игрока на площадке — чтобы смотреть в записи себя.

Обе лиги ведут живой протокол и помечают в нём выход/уход каждого игрока. У
Infobasket это события типов 8/9 (`Widget/GetOnline`), у SLPRO — записи
`action="status"` со значением 1/0. Из них складываются отрезки «вышел —
ушёл».

Главное, ради чего всё затевалось: перевести отрезки в РЕАЛЬНОЕ время, а не
игровое. На записи 10 игровых минут занимают 18–20 реальных, и по «минуте
игрового времени» видео не отмотать. Реальное время берём из самого
протокола:

* SLPRO — поле `date_add` (секретарь ведёт протокол вживую, это часы на стене);
* Infobasket — поле `VideoFrom` у части событий: абсолютный unix-таймстемп
  куска видео. Он есть примерно у четверти событий, остальные разносим
  линейной интерполяцией между соседними якорями.

Ноль отсчёта в записи — момент, с которого включили трансляцию. Его знает сам
ВК (`video.get` отдаёт время начала эфира), поэтому положение матча в записи
получается вычитанием двух реальных моментов: эфир в 20:00, свисток в 21:00 —
спорный на 1:00:00 записи, выход игрока в 21:20 — на 1:20:00. Началом игры
считаем не первую запись в протоколе, а момент, когда пошли игровые часы:
состав секретарь заводит заранее, иногда за десять минут.

Если начало эфира узнать не удалось (нет токена, видео удалили), остаётся
оценка: трансляцию включают ко времени из расписания. Тогда сдвиг — «начало
игры минус расписание», и под тайм-кодами висит приписка, что возможно
смещение. Какой способ сработал, видно по `game_video_sync.set_by`.

Имён здесь нет и не будет — только id игрока ([[legal-data-invariant]]).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

IB_ONLINE_URL = "https://reg.infobasket.su/Widget/GetOnline/{game_id}?format=json&lang=ru"
IB_PLAY_IN = 8       # выход на площадку
IB_PLAY_OUT = 9      # уход с площадки
IB_PERIOD_START = 21  # старт периода — у него всегда есть VideoFrom
HTTP_TIMEOUT = 25

# Сколько реального времени идёт одна игровая секунда. Десять игровых минут
# занимают на записи 15–30 реальных, то есть 1.5–3.0; допуск берём шире, чтобы
# не забраковать живой протокол из-за одного затянутого периода.
SANE_PACE = (0.8, 5.0)
# Чем считаем, когда лига не дала годной привязки вовсе.
MODEL_PACE = 1.8
# Перерывы между периодами: короткий и большой (после второго).
BREAK_SECONDS = 120
HALFTIME_SECONDS = 600
# На сколько секунд якорь может отстоять от общей линии, оставаясь якорем.
ANCHOR_JUMP = 180

# Что человек хочет пересмотреть. Ключи — наш общий словарь: у Инфобаскета коды
# числовые, у SLPRO строковые, и сводить их надо здесь, а не в показе.
MOMENT_TITLES = {
    "pts3": "трёхочковый",
    "pts2": "двухочковый",
    "ft": "штрафной",
    "reb": "подбор",
    "stl": "перехват",
    "blk": "блок-шот",
    "ast": "передача",
    "tur": "потеря",
    "pf": "фол",
    "miss3": "промах трёхочкового",
    "miss2": "промах двухочкового",
    "missft": "промах штрафного",
}
MOMENT_ICONS = {"pts3": "🎯", "pts2": "🏀", "ft": "⚪️", "reb": "🔄",
                "stl": "🧤", "blk": "🚫", "ast": "🎁",
                "tur": "❌", "pf": "🟡",
                # Все промахи одним значком: это одна категория, и в сводке
                # три разных кружка читались бы как три разных события.
                "miss3": "⭕️", "miss2": "⭕️", "missft": "⭕️"}

# Своё и чужое в одном списке разделяем порядком: сначала то, что сделал, потом
# то, что потерял. Иначе сводка начинается с фолов, а это не то, ради чего
# запись открывают.
MOMENT_BAD = ("tur", "pf", "miss3", "miss2", "missft")

# Коды Инфобаскета опознаны сверкой с бокс-скором: количество событий каждого
# типа по каждому игроку совпало с его строкой протокола, а 1×1 + 2×2 + 3×3
# сошлось с очками у всех до единого (игра 1081391).
#
# Фолы — сумма трёх кодов (40 + 41 + 42), сверено на трёх играх сразу: 41 и 42
# редкие, по одному-два за матч, и на одной игре разница потерялась бы.
#
# Потери (11) — единственное, что сверено НЕ на равенство: у игрока их всегда
# не больше, чем в протоколе, а недостача (41 из 125 на трёх играх) — потери,
# которые лига никому не приписала. Значит код тот, просто живой протокол
# полнее не бывает: покажем те, что найдены.
#
# Промахи (4/5/6) сверены теми же тождествами: 1+4 сошлось с попытками
# штрафных, 3+6 — с попытками трёхочковых, 2+3+5+6 — со всеми бросками с игры.
IB_MOMENTS = {1: "ft", 2: "pts2", 3: "pts3", 26: "stl", 27: "blk", 28: "reb",
              11: "tur", 40: "pf", 41: "pf", 42: "pf",
              4: "missft", 5: "miss2", 6: "miss3"}

# У SLPRO события названы словами — гадать не приходится. Передачи здесь есть,
# а у Инфобаскета кода передачи я не нашёл и выдумывать не стал: неверная
# подпись отправит человека смотреть чужой момент.
#
# `foul` — фол, который человек совершил. Есть и `foul_on_player` — фол,
# который совершили на нём; это другое событие, и в один список с «твой фол»
# оно не идёт.
SLPRO_MOMENTS = {"ast": "ast", "stl": "stl", "block": "blk",
                 "rebD": "reb", "rebA": "reb", "tur": "tur", "foul": "pf"}

# Длина периода по умолчанию, если лига не прислала свою (десятые доли секунды
# в Infobasket, секунды в SLPRO).
DEFAULT_PERIOD_SECONDS = 600

# Отрезки короче — это «вышел и сразу ушёл» на замене-формальности; в списке
# тайм-кодов они только мешают.
MIN_SHIFT_SECONDS = 10

# Сколько дней назад ищем игры без разметки при фоновой дозагрузке.
BACKFILL_DAYS = 30

# Обе лиги играют по московскому времени, и расписание тоже московское.
MSK = timezone(timedelta(hours=3))

# Насколько поздно матч может начаться относительно расписания, чтобы в это
# ещё верилось. Больше часа — значит расписание в кеше не про эту игру
# (перенос, спаренный тур), и сдвиг лучше не выдумывать.
MAX_LATE_START = 3600

# Для отсчёта от эфира запас больше: трансляцию включают и за час, и за два —
# перед нашей игрой в зале идёт чужая. Но не сутки: такой разрыв означает, что
# ссылка ведёт не на ту запись.
MAX_STREAM_LEAD = 6 * 3600

# Разметка, сделанная раньше этой отметки, пересчитывается заново. Причины
# копились по мере проверок на реальных записях: ноль отсчёта у SLPRO брался по
# первой записи протокола вместо начала игры; момент спорного не сохранялся, и
# сдвиг нечем было уточнить по эфиру; замена вешалась на конец мёртвой пачки
# событий вместо её середины; 17.08.2026 — появились моменты (game_moments), а
# у размеченных раньше игр их просто нет: протокол надо перечитать; в тот же
# день чуть позже — в моменты добавились потери, фолы и промахи, и по той же
# причине. Отметка стоит РАНЬШЕ ближайшего прохода крона: иначе перечитанное
# снова оказалось бы «старым» и качалось бы по кругу до наступления отметки.
# Время — UTC, как и fetched_at (сервер живёт в UTC): с московской отметкой
# условие оставалось бы верным ещё три часа, и каждый прогон ingest
# перекачивал бы все протоколы заново.
# Разметку, снятую раньше этой отметки, перекладываем заново. Двигаем её,
# когда меняется сам способ счёта: 24.08.2026 — проверка периодов на
# здравый темп (см. _timeline), до неё негодные отметки лиги ложились в
# базу как есть. Отметка — момент выкладки (UTC, как fetched_at):
# ставить её в будущее нельзя, иначе уже переложенное переберётся
# снова на каждом заходе добора.
REDO_BEFORE = "2026-08-24T09:34:49"

# Начинаем показывать чуть раньше самого выхода: попасть ровно в секунду
# замены бесполезно — человек хочет увидеть, как он выходит, а не догонять
# уже идущий эпизод.
LEAD_SECONDS = 5

# Приписка зависит от того, чем меряли. По эфиру — это вычитание двух
# реальных моментов, извиняться не за что; по расписанию — оценка.
NOTE_EXACT = ("<i>Время считаем от начала эфира: когда ВК включил трансляцию "
              "и когда в протоколе пошли часы.</i>")
NOTE_GUESS = ("<i>Начало эфира неизвестно, считаем от времени по расписанию — "
              "возможно смещение на минуту-другую.</i>")
NOTE_HAND = ("<i>Время начала матча выставлено вручную по записи — "
             "если не сходится, поправь кнопкой ниже.</i>")
# Отдельная оговорка про качество разметки внутри матча. Сдвиг может быть
# выставлен хоть вручную и точно — но если лига в каком-то периоде дала
# негодные отметки видео, места внутри игры посчитаны по среднему темпу, и
# молчать об этом нельзя: человек мотает запись и не понимает, почему мимо.
NOTE_MODEL = ("<i>В протоколе этой игры лига испортила привязку к видео: "
              "часть моментов расставлена по среднему темпу матча, "
              "погрешность — до полуминуты.</i>")


def _notes(source: str, game_id: Any) -> List[str]:
    """Оговорки под тайм-кодами: чем меряли сдвиг и как размечали игру."""
    kind = offset_kind(source, game_id)
    out = [NOTE_HAND if kind == "hand"
           else NOTE_EXACT if kind == "vk" else NOTE_GUESS]
    if timing_kind(source, game_id) == "model":
        out.append(NOTE_MODEL)
    return out


# ───────────────────────────── Infobasket ──────────────────────────────────

def _ib_fetch(game_id: str) -> Dict[str, Any]:
    req = urllib.request.Request(
        IB_ONLINE_URL.format(game_id=game_id),
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.load(resp)


def _ib_period_offsets(data: Dict[str, Any]) -> Dict[int, float]:
    """Сколько игровых секунд прошло к началу каждого периода."""
    offsets, total = {}, 0.0
    periods = data.get("OnlinePeriods") or []
    for item in sorted(periods, key=lambda p: int(p.get("Period") or 0)):
        num = int(item.get("Period") or 0)
        offsets[num] = total
        total += float(item.get("Seconds") or DEFAULT_PERIOD_SECONDS)
    return offsets


def _elapsed(period: int, play_second: Any, offsets: Dict[int, float]) -> float:
    """Игровое время события от начала матча, в секундах.

    PlaySecond у Infobasket — десятые доли секунды ВНУТРИ периода и растёт от
    нуля (в конце периода 6000 при десятиминутке)."""
    base = offsets.get(int(period))
    if base is None:  # овертайм, которого не было в OnlinePeriods
        base = max(offsets.values(), default=0.0) + DEFAULT_PERIOD_SECONDS
    return base + float(play_second or 0) / 10.0


def _interpolate(anchors: List[Tuple[float, float]], clock: float) -> float:
    """Реальное время по игровому — линейно между соседними якорями.

    Якорь — событие, у которого лига проставила настоящий таймстемп. За
    пределами крайних якорей продолжаем наклон ближайшего отрезка: замена в
    первые секунды матча иначе съезжала бы на начало записи."""
    if not anchors:
        return clock
    if len(anchors) == 1:
        return anchors[0][1] + (clock - anchors[0][0])
    if clock <= anchors[0][0]:
        (c1, r1), (c2, r2) = anchors[0], anchors[1]
    elif clock >= anchors[-1][0]:
        (c1, r1), (c2, r2) = anchors[-2], anchors[-1]
    else:
        lo, hi = 0, len(anchors) - 1
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if anchors[mid][0] <= clock:
                lo = mid
            else:
                hi = mid
        (c1, r1), (c2, r2) = anchors[lo], anchors[hi]
    if c2 == c1:
        return r1
    return r1 + (r2 - r1) * (clock - c1) / (c2 - c1)


def _place(anchors: List[Tuple[float, float]], clock: float) -> float:
    """Реальное время замены по игровым часам.

    Замены всегда происходят при остановленных часах, и в протоколе целая
    пачка событий стоит на ОДНОЙ отметке часов: фол, штрафные, замены, снова
    штрафные. Реальное время есть не у всех, и линейная интерполяция вешала
    замену на последний якорь пачки — то есть в конец простоя. На проверке по
    видео это дало +25 секунд: игрок уже минуту как на площадке.

    Поэтому: если на этой же отметке часов есть якоря, берём СЕРЕДИНУ их
    промежутка — замены случаются между свистком и возобновлением игры.
    Отметки нет — работает прежняя интерполяция между соседями."""
    same = [real for c, real in anchors if abs(c - clock) < 1.0]
    if same:
        return (min(same) + max(same)) / 2.0
    return _interpolate(anchors, clock)


def _drop_outliers(items: List[Tuple[float, float]]) -> List[Tuple[float, float]]:
    """Выбрасывает якоря, улетевшие от общей линии периода.

    В овертайме матча 22.08.2026 последняя отметка стояла на восемнадцать часов
    позже остальных: запись правили на следующий день. Один такой якорь тянул
    за собой интерполяцию всего периода."""
    if len(items) < 3:
        return items
    steps = [(v2 - v1) / (c2 - c1)
             for (c1, v1), (c2, v2) in zip(items, items[1:]) if c2 > c1]
    if not steps:
        return items
    steps.sort()
    step = steps[len(steps) // 2]
    if step <= 0:
        return items
    kept = [items[0]]
    for clock, video in items[1:]:
        c0, v0 = kept[-1]
        expect = v0 + step * (clock - c0)
        if abs(video - expect) <= ANCHOR_JUMP + step * (clock - c0) * 0.5:
            kept.append((clock, video))
    return kept


def _pace_of(items: List[Tuple[float, float]]) -> Optional[float]:
    """Сколько реальных секунд пришлось на игровую по этим якорям."""
    if len(items) < 2:
        return None
    span = items[-1][0] - items[0][0]
    if span <= 0:
        return None
    return (items[-1][1] - items[0][1]) / span


def _period_length(period: int, offsets: Dict[int, float]) -> float:
    got = offsets.get(period + 1, 0.0) - offsets.get(period, 0.0)
    return got if got > 0 else DEFAULT_PERIOD_SECONDS


def _timeline(per_anchors: Dict[int, List[Tuple[float, float]]],
              offsets: Dict[int, float], zero: float) -> Tuple[Dict[int, Any], bool]:
    """Как переводить игровые часы в реальное время — по каждому периоду.

    Лига иногда отдаёт негодную привязку. В матче 22.08.2026 отметки видео за
    весь первый период сдвинулись на одиннадцать секунд: десять минут игры
    уместились в мгновение, и все моменты периода вставали вплотную к спорному
    — промах «на табло 3:59» показывался на 3:30 записи, раньше начала игры.

    Поэтому каждый период проверяем на здравый темп. Годный размечаем его
    собственными якорями. Негодный считаем по среднему темпу этой же игры, а
    следующие периоды сдвигаем следом — иначе они остались бы приклеены к
    съеденному времени и всё дальнейшее уехало бы на четверть часа вперёд.

    Возвращаем разметку и признак «всё из протокола»: посчитанное по модели —
    оценка, и человеку об этом говорят, а не выдают за точность."""
    periods = sorted(set(list(per_anchors) + list(offsets)))
    clean = {p: _drop_outliers(sorted(per_anchors.get(p) or []))
             for p in periods}
    paces = {p: _pace_of(items) for p, items in clean.items()}
    good = {p for p, v in paces.items()
            if v is not None and SANE_PACE[0] <= v <= SANE_PACE[1]}
    usable = sorted(paces[p] for p in good)
    model = usable[len(usable) // 2] if usable else MODEL_PACE

    plan: Dict[int, Any] = {}
    prev_end: Optional[float] = None
    shift = 0.0
    trusted = True
    for period in periods:
        items = clean.get(period) or []
        lo = offsets.get(period, 0.0)
        length = _period_length(period, offsets)
        gap = HALFTIME_SECONDS if period == 3 else BREAK_SECONDS
        if period in good and items:
            start = _interpolate(items, lo)
            if prev_end is not None and start + shift < prev_end:
                # Период уехал назад во времени — значит, до него что-то
                # считали по модели. Подвинем весь его якорный ряд следом.
                shift = prev_end + gap - start
                trusted = False
            plan[period] = ("anchors", items, shift)
            prev_end = _interpolate(items, lo + length) + shift
        else:
            start = zero if prev_end is None else prev_end + gap
            plan[period] = ("model", start, model, lo)
            prev_end = start + length * model
            trusted = False
    return plan, trusted


def _at(plan: Dict[int, Any], period: int, clock: float,
        every: List[Tuple[float, float]], substitution: bool = False) -> float:
    """Реальный момент события по игровым часам."""
    how = plan.get(period)
    if not how:
        return _place(every, clock) if substitution else _interpolate(every, clock)
    if how[0] == "model":
        _, start, pace, lo = how
        return start + (clock - lo) * pace
    _, items, shift = how
    found = _place(items, clock) if substitution else _interpolate(items, clock)
    return found + shift


def _ib_shifts(game_id: str) -> Tuple[List[Dict[str, Any]], Optional[float],
                                      Optional[int], List[Dict[str, Any]], bool]:
    data = _ib_fetch(str(game_id))
    plays = data.get("OnlinePlays") or []
    starts = data.get("OnlineStarts") or []
    if not plays:
        return [], None, None, [], True

    # StartID — номер заявки в этой игре; настоящий id игрока лежит в PersonID.
    person = {int(s.get("StartID") or 0): str(s.get("PersonID") or "")
              for s in starts if s.get("PersonID")}
    offsets = _ib_period_offsets(data)

    # Якоря разложены ПО ПЕРИОДАМ, и это принципиально. Конец третьего и
    # начало четвёртого приходятся на одну и ту же отметку игровых часов, а в
    # записи между ними перерыв в пару минут. Сквозная интерполяция этот
    # перерыв съедала: игрок, отыгравший без замены, получал два стыкующихся
    # отрезка, и второй начинался на две минуты раньше, чем на видео.
    per_anchors: Dict[int, List[Tuple[float, float]]] = {}
    for p in plays:
        video = p.get("VideoFrom")
        if not video:
            continue
        period = int(p.get("PlayPeriod") or 0)
        per_anchors.setdefault(period, []).append(
            (_elapsed(period, p.get("PlaySecond"), offsets), float(video)))
    for items in per_anchors.values():
        items.sort()
    if not per_anchors:
        logger.info("Тайм-коды infobasket/%s: в протоколе нет привязки к видео", game_id)
        return [], None, None, [], True
    every = sorted(a for items in per_anchors.values() for a in items)
    # Ноль — старт первого периода (у него якорь есть всегда, событие типа 21).
    zero = per_anchors.get(1, every)[0][1]
    plan, trusted = _timeline(per_anchors, offsets, zero)
    if not trusted:
        logger.info("Тайм-коды infobasket/%s: привязка лиги местами негодная, "
                    "часть периодов посчитана по среднему темпу", game_id)

    events: List[Dict[str, Any]] = []
    for p in plays:
        kind = int(p.get("PlayTypeID") or 0)
        if kind not in (IB_PLAY_IN, IB_PLAY_OUT):
            continue
        pid = person.get(int(p.get("StartID") or 0))
        if not pid:
            continue
        period = int(p.get("PlayPeriod") or 0)
        clock = _elapsed(period, p.get("PlaySecond"), offsets)
        in_period = clock - offsets.get(period, 0.0)
        # Длина периода своя (овертайм — пять минут), берём из расписания лиги.
        period_len = offsets.get(period + 1, 0.0) - offsets.get(period, 0.0) \
            or DEFAULT_PERIOD_SECONDS
        events.append({
            "player_id": pid,
            "period": period,
            "clock": clock,
            # Сколько было на табло: по этому числу человек сверяет ссылку с
            # записью, не зная, кого высматривать среди пяти замен подряд.
            "left": max(0.0, period_len - in_period),
            "real": _at(plan, period, clock, every, substitution=True) - zero,
            "in": kind == IB_PLAY_IN,
            "order": int(p.get("PlaySortOrder") or 0),
        })

    moments: List[Dict[str, Any]] = []
    for p in plays:
        what = IB_MOMENTS.get(int(p.get("PlayTypeID") or 0))
        pid = person.get(int(p.get("StartID") or 0))
        if not what or not pid:
            continue
        period = int(p.get("PlayPeriod") or 0)
        clock = _elapsed(period, p.get("PlaySecond"), offsets)
        in_period = clock - offsets.get(period, 0.0)
        period_len = offsets.get(period + 1, 0.0) - offsets.get(period, 0.0) \
            or DEFAULT_PERIOD_SECONDS
        moments.append({
            "player_id": pid, "kind": what, "period": period,
            "left": max(0.0, period_len - in_period),
            "real": _at(plan, period, clock, every) - zero,
            "order": int(p.get("PlaySortOrder") or 0),
        })
    return (_pair(events), zero,
            _seconds_of_day(datetime.fromtimestamp(zero, MSK)), moments, trusted)


# ─────────────────────────────── SLPRO ─────────────────────────────────────

def _seconds_of_day(moment: datetime) -> int:
    return moment.hour * 3600 + moment.minute * 60 + moment.second


def _scheduled_seconds(game_time: Any) -> Optional[int]:
    """Время из расписания в секундах от полуночи. Форматы у лиг разные:
    SLPRO пишет «21:10:00», Infobasket — «20.00»."""
    raw = str(game_time or "").strip().replace(".", ":")
    if not raw:
        return None
    parts = raw.split(":")
    try:
        nums = [int(p) for p in parts[:3]]
    except ValueError:
        return None
    if not nums or not (0 <= nums[0] < 24):
        return None
    while len(nums) < 3:
        nums.append(0)
    return nums[0] * 3600 + nums[1] * 60 + nums[2]


def _slpro_time(value: Any) -> Optional[float]:
    """Отметка секретаря → unix. Часы в зале московские, и сказать это надо
    явно: сервер живёт в UTC, и без tzinfo момент уехал бы на три часа — а
    сравнивать его теперь приходится с временем начала эфира в ВК."""
    try:
        naive = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None
    return naive.replace(tzinfo=MSK).timestamp()


async def _slpro_shifts(client, game_id: str) -> Tuple[List[Dict[str, Any]],
                                                       Optional[float], Optional[int],
                                                       List[Dict[str, Any]], bool]:
    game = await client.get_game(game_id)
    log = (game or {}).get("log") or []
    if not log:
        return [], None, None, [], True

    stamps = [t for t in (_slpro_time(e.get("date_add")) for e in log) if t]
    if not stamps:
        logger.info("Тайм-коды slpro/%s: в протоколе нет времени событий", game_id)
        return [], None, None, [], True

    # game_time у SLPRO идёт на УБЫВАНИЕ (600 → 0) и считается внутри периода.
    per_len = max((float(e.get("game_time") or 0) for e in log),
                  default=DEFAULT_PERIOD_SECONDS) or DEFAULT_PERIOD_SECONDS

    # Ноль — не первая запись в протоколе, а момент, когда ПОШЛИ ИГРОВЫЕ ЧАСЫ.
    # Секретарь заводит стартовые пятёрки заранее и по-разному: обычно за
    # полминуты, а 19.07 — за двенадцать минут до спорного. По первой записи
    # весь матч уезжал на эти минуты вперёд.
    running = [t for t in (_slpro_time(e.get("date_add")) for e in log
                           if int(e.get("period") or 0) == 1
                           and float(e.get("game_time") or per_len) < per_len) if t]
    zero = min(running) if running else min(stamps)

    def _elapsed_of(e: Dict[str, Any]) -> float:
        period = int(e.get("period") or 0)
        return (period - 1) * per_len + (per_len - float(e.get("game_time") or 0))

    # Якоря — ИГРОВЫЕ события (бросок, подбор, фол): их секретарь отмечает по
    # ходу. Замену он вбивает когда успеет — до ближайшего игрового события
    # медиана 19 секунд, максимум 50. Поэтому замену ставим не по её отметке, а
    # по тому, что происходило на площадке в этот момент часов.
    anchors: List[Tuple[float, float]] = []
    for e in log:
        stamp = _slpro_time(e.get("date_add"))
        if stamp is None or str(e.get("action") or "") == "status":
            continue
        anchors.append((_elapsed_of(e), stamp))
    anchors.sort()

    events: List[Dict[str, Any]] = []
    for idx, e in enumerate(log):
        if str(e.get("action") or "") != "status":
            continue
        pid = str(e.get("player_id") or "")
        stamp = _slpro_time(e.get("date_add"))
        if not pid or stamp is None:
            continue
        elapsed = _elapsed_of(e)
        real = _place(anchors, elapsed) if anchors else stamp
        events.append({
            "player_id": pid,
            "period": int(e.get("period") or 0),
            "clock": elapsed,
            "left": float(e.get("game_time") or 0),
            # Отметка секретаря остаётся крайним случаем: у стартовых пятёрок
            # (часы ещё не шли) игровых событий на этой отметке нет.
            "real": (real if anchors else stamp) - zero,
            "in": int(e.get("value") or 0) == 1,
            "order": idx,
        })

    moments: List[Dict[str, Any]] = []
    for idx, e in enumerate(log):
        action = str(e.get("action") or "")
        pid = str(e.get("player_id") or "")
        if not pid:
            continue
        if action in ("points", "miss"):
            # Сколько очков стоил бросок, лига пишет прямо в событии — и у
            # забитого, и у промаха.
            table = ({1: "ft", 2: "pts2", 3: "pts3"} if action == "points"
                     else {1: "missft", 2: "miss2", 3: "miss3"})
            what = table.get(int(e.get("points") or 0))
        else:
            what = SLPRO_MOMENTS.get(action)
        if not what:
            continue
        elapsed = _elapsed_of(e)
        stamp = _slpro_time(e.get("date_add"))
        real = _place(anchors, elapsed) if anchors else stamp
        if real is None:
            continue
        moments.append({
            "player_id": pid, "kind": what, "period": int(e.get("period") or 0),
            "left": float(e.get("game_time") or 0),
            "real": real - zero, "order": idx,
        })
    return (_pair(events), zero,
            _seconds_of_day(datetime.fromtimestamp(zero, MSK)), moments, True)


# ──────────────────────────── Общая сборка ─────────────────────────────────

def _pair(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Складывает «вышел»/«ушёл» в отрезки. Незакрытые отбрасываем.

    Незакрытый отрезок означает дыру в протоколе (секретарь не отметил уход) —
    и лучше показать меньше, чем нарисовать человеку отрезок до конца матча,
    которого не было."""
    events.sort(key=lambda e: (e["period"], e["clock"], e["order"]))
    open_at: Dict[str, Dict[str, Any]] = {}
    shifts: List[Dict[str, Any]] = []
    for e in events:
        pid = e["player_id"]
        if e["in"]:
            open_at[pid] = e
            continue
        start = open_at.pop(pid, None)
        if not start:
            continue
        # Стартовую пятёрку заводят до спорного, поэтому её «выход» приходится
        # на отрицательное время. Для записи это ноль: игра начинается тогда,
        # когда начинается.
        start_real, end_real = max(0.0, start["real"]), e["real"]
        if end_real < start_real:
            continue
        shifts.append({
            "player_id": pid,
            "period": start["period"],
            "start_sec": int(round(start_real)),
            "end_sec": int(round(end_real)),
            "clock_sec": int(round(max(0.0, e["clock"] - start["clock"]))),
            "start_left": int(round(start.get("left") or 0)),
        })
    if open_at:
        logger.info("Тайм-коды: %d незакрытых отрезков в протоколе", len(open_at))
    shifts.sort(key=lambda s: (s["player_id"], s["start_sec"]))
    return shifts


def store(source: str, game_id: Any, shifts: List[Dict[str, Any]]) -> int:
    """Переписывает разметку игры целиком (протокол могли поправить задним
    числом — дописывать к старому нельзя)."""
    if not shifts:
        return 0
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_shifts WHERE source = ? AND game_id = ?",
                     (source, str(game_id)))
        conn.executemany(
            """INSERT OR REPLACE INTO game_shifts
                   (source, game_id, player_id, period, start_sec, end_sec,
                    clock_sec, start_left, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(source, str(game_id), s["player_id"], s["period"], s["start_sec"],
              s["end_sec"], s["clock_sec"], s.get("start_left", 0), now)
             for s in shifts])
        conn.commit()
    return len(shifts)


def store_moments(source: str, game_id: Any, items: List[Dict[str, Any]]) -> int:
    """Переписывает моменты игры целиком — как и отрезки: протокол правят."""
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_moments WHERE source = ? AND game_id = ?",
                     (source, str(game_id)))
        conn.executemany(
            """INSERT OR REPLACE INTO game_moments
                   (source, game_id, player_id, kind, period, real_sec,
                    left_sec, ord, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [(source, str(game_id), m["player_id"], m["kind"], int(m["period"]),
              int(m["real"]), int(m.get("left") or 0), int(m.get("order") or 0), now)
             for m in items])
        conn.commit()
    return len(items)


def moments(source: str, game_id: Any, player_id: Any = None) -> List[Dict[str, Any]]:
    """Моменты из кеша. Наружу отсюда не ходим — это путь ответа человеку."""
    sheets_cache.init_db()
    sql = ("SELECT player_id, kind, period, real_sec, left_sec FROM game_moments "
           "WHERE source = ? AND game_id = ?")
    args: List[Any] = [source, str(game_id)]
    if player_id is not None:
        sql += " AND player_id = ?"
        args.append(str(player_id))
    sql += " ORDER BY real_sec, ord"
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(sql, args)]


def has_shifts(source: str, game_id: Any) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM game_shifts WHERE source = ? AND game_id = ? LIMIT 1",
            (source, str(game_id))).fetchone()
    return bool(row)


def shifts(source: str, game_id: Any, player_id: Any = None) -> List[Dict[str, Any]]:
    """Разметка из кеша. Живых запросов отсюда не делаем — это путь ответа
    человеку, наружу ходит только фоновая дозагрузка."""
    sheets_cache.init_db()
    sql = ("SELECT player_id, period, start_sec, end_sec, clock_sec, start_left "
           "FROM game_shifts WHERE source = ? AND game_id = ?")
    args: List[Any] = [source, str(game_id)]
    if player_id is not None:
        sql += " AND player_id = ?"
        args.append(str(player_id))
    sql += " ORDER BY start_sec"
    with sheets_cache.get_connection() as conn:
        return [dict(r) for r in conn.execute(sql, args)]


async def refresh(source: str, game_id: Any, client=None) -> int:
    """Тянет протокол игры и кладёт отрезки в кеш. Возвращает число отрезков."""
    source = str(source)
    try:
        if source == "infobasket":
            found, at, tipoff, mom, exact = await asyncio.to_thread(
                _ib_shifts, str(game_id))
        elif source == "slpro":
            if client is None:
                from slpro_client import SlproClient
                client = SlproClient()
            found, at, tipoff, mom, exact = await _slpro_shifts(client, str(game_id))
        else:
            return 0
    except Exception as exc:  # сеть/формат — не роняем ingest из-за тайм-кодов
        logger.warning("Тайм-коды %s/%s: %s", source, game_id, exc)
        return 0
    # Моменты кладём и тогда, когда отрезков не вышло: замену секретарь мог не
    # отметить ни разу, а броски отмечает всегда.
    if mom:
        store_moments(source, game_id, mom)
    set_timing(source, game_id, exact)
    n = store(source, game_id, found)
    if n:
        await sync_offset(source, game_id, at, tipoff)
    return n


async def sync_offset(source: str, game_id: Any, tipoff_epoch: Optional[float],
                      tipoff_sec: Optional[int]) -> int:
    """На какой секунде записи спорный мяч. Считает и запоминает.

    Точный путь: у ВК спрашиваем, когда НАЧАЛСЯ эфир, и вычитаем его из
    времени спорного по протоколу. Обе величины — реальные моменты, никаких
    допущений: эфир в 20:00, свисток в 21:00 — спорный на 1:00:00 записи.

    Запасной путь (эфир не нашли, токена нет, видео удалили): считаем, что
    трансляцию включили ко времени из расписания. Тогда сдвиг — «начало игры
    минус расписание», по нашим играм это от минуты до двенадцати.

    Отрицательная разница означает, что эфир начали уже после свистка: спорный
    в самом начале записи, сдвиг ноль, в минус не отматываем."""
    started = 0
    if tipoff_epoch:
        try:
            import vk_video
            started = vk_video.video_started_at(source, game_id)
            if not started:
                link = vk_video.link_of(source, game_id)
                meta = await vk_video.video_meta(link) if link else {}
                if meta.get("started_at"):
                    vk_video.store_video_meta(source, game_id, meta)
                    started = int(meta["started_at"])
        except Exception as exc:
            logger.warning("Тайм-коды %s/%s: начало эфира не узнать: %s",
                           source, game_id, exc)

    if offset_kind(source, game_id) == "hand":
        # Руками выставленное не трогаем никогда: человек смотрел запись, а
        # автоматика — нет. Момент спорного всё равно обновим, он пригодится,
        # если ручную привязку потом снимут.
        if tipoff_epoch:
            _remember_tipoff(source, game_id, tipoff_epoch)
        return offset(source, game_id)

    if started and tipoff_epoch:
        shift = int(round(tipoff_epoch - started))
        way = "по эфиру"
    else:
        if tipoff_sec is None:
            return 0
        sheets_cache.init_db()
        with sheets_cache.get_connection() as conn:
            row = conn.execute(
                "SELECT game_time FROM game_meta WHERE source = ? AND game_id = ?",
                (source, str(game_id))).fetchone()
        planned = _scheduled_seconds(row["game_time"]) if row else None
        if planned is None:
            return 0
        shift = tipoff_sec - planned
        way = "по расписанию"
    limit = MAX_LATE_START if way == "по расписанию" else MAX_STREAM_LEAD
    if shift < 0 or shift > limit:
        shift = 0
    set_offset(source, game_id, shift, "auto" if way == "по расписанию" else "vk",
               tipoff_at=tipoff_epoch or 0)
    logger.info("Тайм-коды %s/%s: спорный на %s записи (%s)",
                source, game_id, hhmmss(shift), way)
    return shift


def our_games(conn, extra_where: str = "", args: Optional[List[Any]] = None,
              limit: int = 20) -> List[Dict[str, Any]]:
    """Игры НАШИХ команд с записью ВК, свежие сначала.

    Кеш статистики держит всю лигу (по ней считаются рейтинги), и без этого
    фильтра в разметку лезли чужие матчи: протоколы качались зря, а тренеру в
    списке записей показывали «КУРАЖ — Кудрово»."""
    ours = [str(r["team_id"]) for r in conn.execute(
        "SELECT team_id FROM league_teams WHERE ours = 1")]
    if not ours:
        return []
    marks = ",".join("?" * len(ours))
    sql = (f"""SELECT m.source, m.game_id, m.game_date, m.home_name, m.guest_name,
                      (SELECT COUNT(*) FROM game_shifts s
                        WHERE s.source = m.source AND s.game_id = m.game_id) AS shifts
                 FROM game_meta m
                WHERE m.video_vk != '' AND m.game_date != ''
                  AND (m.home_team_id IN ({marks}) OR m.guest_team_id IN ({marks}))
                  {extra_where}
                ORDER BY m.game_date DESC LIMIT ?""")
    return [dict(r) for r in conn.execute(sql, ours + ours + list(args or []) + [int(limit)])]


def player_games(identities: List[Tuple[str, Any]], limit: int = 8) -> List[Dict[str, Any]]:
    """Игры с разметкой, где играл этот человек, свежие сначала.

    identities — пары (источник, id игрока в лиге): у большинства их две,
    основа и Farm."""
    sheets_cache.init_db()
    found: List[Dict[str, Any]] = []
    with sheets_cache.get_connection() as conn:
        for source, player_id in identities:
            found += [dict(r, player_id=str(player_id)) for r in conn.execute(
                """SELECT m.source, m.game_id, m.game_date, m.home_name,
                          m.guest_name, m.video_vk, COUNT(*) AS shifts
                     FROM game_shifts s
                     JOIN game_meta m ON m.source = s.source AND m.game_id = s.game_id
                    WHERE s.source = ? AND s.player_id = ?
                    GROUP BY s.source, s.game_id
                    ORDER BY m.game_date DESC LIMIT ?""",
                (str(source), str(player_id), int(limit)))]
    found.sort(key=lambda g: str(g["game_date"]), reverse=True)
    return found[:limit]


def games_without_shifts(limit: int = 20, days: int = BACKFILL_DAYS) -> List[Dict[str, Any]]:
    """Наши сыгранные игры, которым разметки не хватает.

    Ограничиваемся играми с записью ВК: тайм-коды без видео некуда
    прикладывать. Берём и те, что размечены, но без привязки к записи, — так
    игры, размеченные до появления авторасчёта сдвига, дотянутся сами."""
    sheets_cache.init_db()
    since_iso = datetime.fromordinal(
        datetime.now().date().toordinal() - days).date().isoformat()
    with sheets_cache.get_connection() as conn:
        return our_games(
            conn,
            extra_where=("AND m.game_date >= ? "
                         "AND (NOT EXISTS (SELECT 1 FROM game_shifts s "
                         "                  WHERE s.source = m.source AND s.game_id = m.game_id) "
                         "  OR NOT EXISTS (SELECT 1 FROM game_video_sync v "
                         "                  WHERE v.source = m.source AND v.game_id = m.game_id) "
                         "  OR EXISTS (SELECT 1 FROM game_shifts s "
                         "              WHERE s.source = m.source AND s.game_id = m.game_id "
                         "                AND s.fetched_at < ?)) "
                         "AND EXISTS (SELECT 1 FROM game_player_stats p "
                         "             WHERE p.source = m.source AND p.game_id = m.game_id)"),
            args=[since_iso, REDO_BEFORE], limit=limit)


async def backfill(limit: int = 10, client=None) -> int:
    """Фоновая дозагрузка разметки. Зовётся из ingest, не из ответа человеку."""
    total = 0
    for game in games_without_shifts(limit=limit):
        n = await refresh(game["source"], game["game_id"], client=client)
        total += bool(n)
        if n:
            logger.info("Тайм-коды %s/%s: отрезков %d",
                        game["source"], game["game_id"], n)
    total += await resync_offsets(limit=limit)
    return total


async def resync_offsets(limit: int = 10) -> int:
    """Переводит сдвиги с расписания на реальное начало эфира.

    Ссылку на запись бот находит не сразу — иногда через день после игры.
    К этому времени разметка уже сделана и сдвиг посчитан по расписанию;
    протокол ради уточнения перекачивать не надо, момент спорного мы сохранили.
    """
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT v.source, v.game_id, v.tipoff_at
                 FROM game_video_sync v
                 JOIN game_meta m ON m.source = v.source AND m.game_id = v.game_id
                WHERE v.set_by != 'vk' AND v.tipoff_at > 0 AND m.video_vk != ''
                ORDER BY m.game_date DESC LIMIT ?""", (int(limit),))]
    done = 0
    for r in rows:
        before = offset(r["source"], r["game_id"])
        await sync_offset(r["source"], r["game_id"], float(r["tipoff_at"]), None)
        if offset_kind(r["source"], r["game_id"]) == "vk":
            done += 1
            logger.info("Тайм-коды %s/%s: сдвиг уточнён по эфиру, было %s стало %s",
                        r["source"], r["game_id"], hhmmss(before),
                        hhmmss(offset(r["source"], r["game_id"])))
    return done


# ───────────────────────── Привязка к записи ВК ────────────────────────────

def offset(source: str, game_id: Any) -> int:
    """На какой секунде записи спорный мяч. 0 — сдвиг не задан."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT offset_sec FROM game_video_sync WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return int(row["offset_sec"]) if row else 0


def _remember_tipoff(source: str, game_id: Any, tipoff_at: float) -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "UPDATE game_video_sync SET tipoff_at = ? WHERE source = ? AND game_id = ?",
            (int(tipoff_at), source, str(game_id)))
        conn.commit()


def set_timing(source: str, game_id: Any, exact: bool) -> None:
    """Запоминает, целиком ли разметка пришла из протокола.

    Пишем даже когда строки ещё нет: сдвиг могут выставить позже, а знать про
    качество привязки надо уже сейчас — по ней решается, показывать ли
    человеку оговорку «время примерное»."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT INTO game_video_sync (source, game_id, timing) VALUES (?, ?, ?) "
            "ON CONFLICT(source, game_id) DO UPDATE SET timing = excluded.timing",
            (source, str(game_id), "league" if exact else "model"))
        conn.commit()


def timing_kind(source: str, game_id: Any) -> str:
    """«league» — всё из протокола, «model» — часть посчитана по темпу."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT timing FROM game_video_sync WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return str(row["timing"] or "league") if row else "league"


def drop_offset(source: str, game_id: Any) -> None:
    """Снимает ручную привязку — сдвиг снова посчитает автоматика."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute("DELETE FROM game_video_sync WHERE source = ? AND game_id = ?",
                     (source, str(game_id)))
        conn.commit()


def parse_offset(text: str) -> Optional[int]:
    """«5:33», «1:02:15» или «333» → секунды. None — если не разобрали."""
    raw = str(text or "").strip().replace(",", ":").replace(".", ":")
    if not raw:
        return None
    parts = raw.split(":")
    try:
        nums = [int(x) for x in parts]
    except ValueError:
        return None
    if not nums or len(nums) > 3 or any(n < 0 for n in nums):
        return None
    total = 0
    for n in nums:
        total = total * 60 + n
    return total


def set_offset(source: str, game_id: Any, seconds: int, who: Any = "",
               tipoff_at: float = 0) -> None:
    """Запоминает сдвиг и сам момент спорного.

    Момент нужен, чтобы позже пересчитать сдвиг по начавшемуся эфиру, не
    выкачивая протокол заново: ссылку на запись ВК бот находит уже после игры,
    а иногда и через день."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO game_video_sync (source, game_id, offset_sec, tipoff_at,
                                            set_by, set_at)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(source, game_id) DO UPDATE SET
                   offset_sec = excluded.offset_sec,
                   tipoff_at = CASE WHEN excluded.tipoff_at > 0 THEN excluded.tipoff_at
                                    ELSE game_video_sync.tipoff_at END,
                   set_by = excluded.set_by, set_at = excluded.set_at""",
            (source, str(game_id), int(seconds), int(tipoff_at or 0), str(who),
             sheets_cache.now_iso()))
        conn.commit()


def hhmmss(seconds: int) -> str:
    seconds = max(0, int(seconds))
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def vk_time(seconds: int) -> str:
    """Секунды в формате перемотки ВК: 26m5s, 1h20m5s."""
    seconds = max(0, int(seconds))
    h, rest = divmod(seconds, 3600)
    m, sec = divmod(rest, 60)
    return (f"{h}h" if h else "") + (f"{m}m" if h or m else "") + f"{sec}s"


def vk_link(video_url: str, seconds: int) -> str:
    """Ссылка на запись с перемоткой. Прежний `?t=` в ссылке заменяем: ВК
    берёт первый параметр, и второй просто не сработал бы."""
    if not video_url:
        return ""
    base = re.sub(r"[?&]t=[^&#]*", "", str(video_url).split("#")[0])
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}t={vk_time(seconds)}"


def offset_kind(source: str, game_id: Any) -> str:
    """Чем меряли сдвиг: vk — по началу эфира, auto — по расписанию,
    hand — выставлено человеком (за двоеточием остаётся, кем именно)."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT set_by FROM game_video_sync WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return str(row["set_by"]).split(":")[0] if row else ""


def timecodes(source: str, game_id: Any, player_id: Any,
              video_url: str = "") -> List[Dict[str, Any]]:
    """Отрезки игрока, готовые к показу: время в записи + ссылка."""
    shift = offset(source, game_id)
    out = []
    for s in shifts(source, game_id, player_id):
        start = max(0, s["start_sec"] + shift - LEAD_SECONDS)
        end = s["end_sec"] + shift
        left = int(s["start_left"] or 0)
        out.append({
            "period": s["period"],
            "start": start,
            "end": end,
            "label": f"{hhmmss(start)}–{hhmmss(end)}",
            "clock_sec": s["clock_sec"],
            "left": left,
            "left_label": f"{left // 60}:{left % 60:02d}",
            "link": vk_link(video_url, start),
        })
    return out


# За сколько до самого действия открывать запись. Бросок — это ещё и то, что
# было секунду назад: проход, заслон, пас. Открывать ровно на отметке протокола
# значит показать мяч уже в кольце.
MOMENT_LEAD_SECONDS = 8


def moment_codes(source: str, game_id: Any, player_id: Any,
                 video_url: str = "") -> List[Dict[str, Any]]:
    """Действия игрока, готовые к показу: время в записи + ссылка."""
    shift = offset(source, game_id)
    out = []
    for m in moments(source, game_id, player_id):
        at = max(0, int(m["real_sec"]) + shift - MOMENT_LEAD_SECONDS)
        left = int(m["left_sec"] or 0)
        out.append({
            "kind": m["kind"],
            "title": MOMENT_TITLES.get(m["kind"], m["kind"]),
            "icon": MOMENT_ICONS.get(m["kind"], "•"),
            "period": m["period"],
            "at": at,
            "label": hhmmss(at),
            "left": left,
            "left_label": f"{left // 60}:{left % 60:02d}",
            "link": vk_link(video_url, at),
        })
    return out


# Сколько знаков отдаём под строки моментов на одной странице. Телеграм режет
# сообщение на 4096, а строка со ссылкой — полторы сотни знаков; считаем по
# длине, а не по числу строк, потому что длина ссылки у лиг разная.
MOMENTS_BUDGET = 2600


def moment_lines(source: str, game_id: Any, player_id: Any,
                 video_url: str = "") -> List[str]:
    """Готовые строки списка моментов — по одной на действие."""
    out = []
    for m in moment_codes(source, game_id, player_id, video_url):
        mark = f"{m['period']}-й период"
        if m.get("left"):
            mark += f", на табло {m['left_label']}"
        if video_url:
            out.append(f"{m['icon']} <a href=\"{m['link']}\">{m['label']}</a> "
                       f"— {m['title']}, {mark}")
        else:
            out.append(f"{m['icon']} {m['label']} — {m['title']}, {mark}")
    return out


def moment_pages(lines: List[str], budget: int = MOMENTS_BUDGET) -> List[List[str]]:
    """Режет список на страницы по длине.

    Раньше список обрывался на «…и ещё 27», и остальное посмотреть было
    нельзя. Теперь видно всё, просто по частям."""
    pages: List[List[str]] = [[]]
    used = 0
    for line in lines:
        if pages[-1] and used + len(line) > budget:
            pages.append([])
            used = 0
        pages[-1].append(line)
        used += len(line) + 1
    return pages


def format_moments_page(source: str, game_id: Any, player_id: Any,
                        video_url: str = "", page: int = 0) -> Tuple[str, int, int]:
    """Одна страница списка моментов: (текст, номер страницы, всего страниц)."""
    lines = moment_lines(source, game_id, player_id, video_url)
    if not lines:
        return "", 0, 0
    pages = moment_pages(lines)
    page = max(0, min(int(page), len(pages) - 1))
    head = [_moments_head(source, game_id, player_id, video_url, len(lines))]
    if len(pages) > 1:
        head.append(f"Страница {page + 1} из {len(pages)}")
    body = head + [""] + pages[page] + _notes(source, game_id)
    return "\n".join(body), page, len(pages)


def _moments_head(source: str, game_id: Any, player_id: Any,
                  video_url: str, total: int) -> str:
    """Заголовок со сводкой по видам — он же одинаков на всех страницах."""
    by_kind: Dict[str, int] = {}
    for m in moments(source, game_id, player_id):
        by_kind[m["kind"]] = by_kind.get(m["kind"], 0) + 1
    summary = " · ".join(
        f"{MOMENT_ICONS.get(k, '•')} {n} {MOMENT_TITLES.get(k, k)}"
        for k, n in sorted(by_kind.items(),
                           key=lambda kv: (kv[0] in MOMENT_BAD, -kv[1])))
    head = f"✨ <b>Твои моменты</b> · {total}"
    if video_url:
        head += f" · <a href=\"{video_url}\">запись</a>"
    return head + "\n" + summary


def format_block(source: str, game_id: Any, player_id: Any,
                 video_url: str = "", max_items: int = 8) -> str:
    """Блок «где ты на записи» для личного разбора (HTML). Пусто — если нечего.

    Короткие выходы-однодневки прячем, но в счётчике их не теряем: человек
    сверяет число отрезков со своей памятью об игре."""
    items = [t for t in timecodes(source, game_id, player_id, video_url)]
    if not items:
        return ""
    shown = [t for t in items if (t["end"] - t["start"]) >= MIN_SHIFT_SECONDS] or items
    total = sum(s["clock_sec"] for s in shifts(source, game_id, player_id))
    # Считаем по показанным: заголовок «8 выходов» над списком из семи строк
    # выглядит как потерянная строка, а не как отброшенная секундная замена.
    count = len(shown)
    head = (f"⏱ <b>Ты на площадке</b> · {count} "
            f"{'выход' if count % 10 == 1 and count != 11 else 'выхода' if count % 10 in (2, 3, 4) and count not in (12, 13, 14) else 'выходов'}")
    if video_url:
        head += f" · <a href=\"{video_url}\">запись</a>"
    lines = [head]
    for t in shown[:max_items]:
        # На табло в этот момент — чтобы ссылку можно было проверить, не зная,
        # кого высматривать: остановил видео, сравнил цифры на табло.
        mark = f"{t['period']}-й период"
        if t.get("left"):
            mark += f", на табло {t['left_label']}"
        # Ссылка спрятана под время: в сообщении остаётся «0:00–8:02», а не
        # простыня из vk.com/video-...?t=. Иначе блок из восьми выходов
        # раздувает разбор так, что своей статистики уже не видно.
        if video_url:
            lines.append(f"• <a href=\"{t['link']}\">{t['label']}</a> · {mark}")
        else:
            lines.append(f"• {t['label']} · {mark}")
    if len(shown) > max_items:
        lines.append(f"…и ещё {len(shown) - max_items}")
    if total:
        lines.append(f"Итого {total // 60} мин игрового времени")
    lines += _notes(source, game_id)
    return "\n".join(lines)
