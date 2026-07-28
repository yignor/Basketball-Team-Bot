#!/usr/bin/env python3
"""
Адаптивный вотчер результатов игр — заменяет "слепой" опрос каждые 30 минут
на конечный автомат, привязанный к реальному прогрессу матча.

Явного таймера/обратного отсчёта в API Infobasket нет — вместо этого
ориентируемся на GameStatus (0/1) и номер текущего периода
(game_peek.peek_game): редко проверяем, пока не начался 4-й период, затем
часто, и публикуем результат сразу по GameStatus==1.

Обе лиги: Infobasket пикаем через game_peek (GameStatus + номер периода),
SLPRO — через его же JSON-API (status матча). Публикацию в чат в обоих
случаях делает существующий скрипт, вотчер лишь решает, КОГДА его звать.

Живёт только в памяти демона (bot_daemon.py вызывает tick() из своего
фонового цикла) — не переживает рестарт. Это нормально: список игр на
сегодня восстанавливается заново из service_records при каждом
refresh_watch_list(), а неизменный 30-минутный cron
(run_game_results_monitor_final.py) остаётся подстраховкой на случай
рестарта демона или бага в этой логике.

Фактическую проверку счёта/публикацию результата НЕ дублируем — по
готовности запускаем существующий, не изменённый (кроме flock-блокировки)
game_results_monitor_final.py подпроцессом через script_runner.
"""

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, Optional

import sheets_cache
import script_runner
import game_peek
from datetime_utils import (
    get_moscow_time,
    parse_game_datetime,
    is_within_game_tracking_window,
    GAME_TRACKING_WINDOW_HOURS,
)

log = logging.getLogger(__name__)

GAME_WATCHER_ENABLED = os.getenv("GAME_WATCHER_ENABLED", "true").lower() == "true"

# Интервалы между проверками по фазам (секунды)
WATCHING_INTERVAL_SECONDS = 12 * 60
FINAL_PERIOD_INTERVAL_SECONDS = 60
FINAL_PERIOD_SAFETY_INTERVAL_SECONDS = 5 * 60
STALE_START_INTERVAL_SECONDS = 30 * 60
SCHEDULED_RECHECK_SECONDS = 30

# Пороги перехода/сдачи
START_WATCHING_BEFORE_SECONDS = 10 * 60      # переходим в watching за 10 мин до начала
STALE_START_AFTER_SECONDS = 3 * 60 * 60      # 3ч без данных о периодах -> stale_start
STALE_START_GIVEUP_SECONDS = 6 * 60 * 60      # ещё 3ч в stale_start -> сдаёмся (итого 6ч)
FINAL_PERIOD_GIVEUP_SECONDS = 5 * 60 * 60     # 5ч в final_period -> подозрение на баг, отдаём крону


@dataclass
class WatchedGame:
    game_id: str
    game_date: str   # "DD.MM.YYYY"
    game_time: str    # "HH:MM"
    source: str = "infobasket"   # "infobasket" | "slpro"
    phase: str = "scheduled"
    next_check_at: float = 0.0
    phase_entered_at: float = field(default_factory=time.time)
    last_result_trigger_at: float = 0.0


_watched_games: Dict[str, WatchedGame] = {}

# Как часто спрашивать SLPRO, пока матч идёт (у него нет номера периода, есть
# только статус, поэтому «финальную» фазу не выделяем — просто чаще смотрим).
SLPRO_LIVE_INTERVAL_SECONDS = 5 * 60


def _as_dmy(game_date: str) -> str:
    """ISO-дата SLPRO -> DD.MM.YYYY, с которой работают datetime_utils."""
    d = (game_date or "").strip()
    if len(d) == 10 and d[4] == "-":
        return f"{d[8:10]}.{d[5:7]}.{d[:4]}"
    return d


async def _peek_slpro(game_id: str) -> Optional[int]:
    """Статус матча в SLPRO (2 — сыгран) или None, если не дозвонились."""
    from slpro_client import SlproClient
    raw = game_id.split("-", 1)[1] if game_id.startswith("slpro-") else game_id
    data = await SlproClient(timeout=10).get_game(raw)
    game = (data or {}).get("game") or {}
    status = game.get("status")
    return int(status) if isinstance(status, (int, str)) and str(status).isdigit() else None


def refresh_watch_list() -> None:
    """Подтягивает анонсированные игры без результата из локальной
    service_records — сегодняшние и вчерашние (ночные игры могут начаться
    поздно вечером и идти после полуночи, поэтому одной календарной даты
    недостаточно; точный отсев — по фактическому времени с начала игры,
    см. is_within_game_tracking_window). Записи без game_id (~5% случаев)
    не добавляются — peek невозможен без ID, их по-прежнему покрывает
    неизменный cron (работает по ссылке, не по ID)."""
    now_dt = get_moscow_time()
    # Infobasket пишет дату как DD.MM.YYYY, SLPRO — как YYYY-MM-DD: спрашиваем
    # оба формата, чтобы не заводить конверсию в SQL.
    days = [now_dt, now_dt - timedelta(days=1)]
    dmy = [d.strftime("%d.%m.%Y") for d in days]
    iso = [d.strftime("%Y-%m-%d") for d in days]

    rows = []
    for source, dt_announce, dt_result, dates in (
        ("infobasket", "АНОНС_ИГРА", "РЕЗУЛЬТАТ_ИГРА", dmy),
        ("slpro", "АНОНС_ИГРА_SLPRO", "РЕЗУЛЬТАТ_ИГРА_SLPRO", iso),
    ):
        try:
            with sheets_cache.get_connection() as conn:
                found = conn.execute(
                    """
                    SELECT game_id, game_date, game_time FROM service_records
                    WHERE deleted = 0 AND data_type = ? AND game_date IN (?, ?)
                      AND game_id != ''
                      AND game_id NOT IN (
                          SELECT game_id FROM service_records
                          WHERE deleted = 0 AND data_type = ? AND game_id != ''
                      )
                    """,
                    (dt_announce, dates[0], dates[1], dt_result),
                ).fetchall()
        except Exception as e:
            log.warning(f"game_watcher: не удалось обновить список игр ({source}): {e}")
            continue
        rows.extend((source, r) for r in found)

    for source, row in rows:
        game_id = row["game_id"]
        if game_id in _watched_games:
            continue
        game_date = _as_dmy(row["game_date"])
        if not is_within_game_tracking_window(game_date, row["game_time"]):
            continue
        watch = WatchedGame(game_id=game_id, game_date=game_date,
                            game_time=row["game_time"], source=source)
        watch.next_check_at = time.time()
        _watched_games[game_id] = watch
        log.info(f"game_watcher: слежу за игрой {game_id} ({source}, "
                 f"{game_date} {row['game_time']})")


async def _advance(watch: WatchedGame) -> None:
    now = time.time()
    moscow_now = get_moscow_time()

    # Абсолютный потолок вне зависимости от фазы: игры бывают ночными,
    # поэтому дата не годится как граница — отсчитываем от фактического
    # времени начала игры.
    if not is_within_game_tracking_window(watch.game_date, watch.game_time):
        log.info(f"game_watcher: {watch.game_id} — прошло больше {GAME_TRACKING_WINDOW_HOURS}ч "
                 f"с предполагаемого начала игры, прекращаю слежение")
        del _watched_games[watch.game_id]
        return

    if watch.phase == "scheduled":
        game_dt = parse_game_datetime(watch.game_date, watch.game_time)
        if game_dt is None:
            log.warning(f"game_watcher: не удалось разобрать время игры {watch.game_id} "
                        f"({watch.game_date} {watch.game_time}), передаю только крону")
            del _watched_games[watch.game_id]
            return
        if moscow_now >= game_dt - timedelta(seconds=START_WATCHING_BEFORE_SECONDS):
            watch.phase = "watching"
            watch.phase_entered_at = now
            watch.next_check_at = now
            log.info(f"game_watcher: {watch.game_id} -> watching")
        else:
            watch.next_check_at = now + SCHEDULED_RECHECK_SECONDS
        return

    if watch.phase == "watching" and watch.source == "slpro":
        status = await _peek_slpro(watch.game_id)
        if status == 2:
            watch.phase = "posting"
            watch.next_check_at = now
            log.info(f"game_watcher: {watch.game_id} — SLPRO отдал финальный статус")
            return
        watch.next_check_at = now + (SLPRO_LIVE_INTERVAL_SECONDS if status == 1
                                     else WATCHING_INTERVAL_SECONDS)
        return

    if watch.phase == "watching":
        peek = await game_peek.peek_game(watch.game_id)
        if not peek.fetched_ok:
            watch.next_check_at = now + WATCHING_INTERVAL_SECONDS
            return
        if peek.max_period >= 4 or peek.game_status == 1:
            watch.phase = "final_period"
            watch.phase_entered_at = now
            watch.last_result_trigger_at = now
            watch.next_check_at = now
            log.info(f"game_watcher: {watch.game_id} -> final_period "
                     f"(период={peek.max_period}, статус={peek.game_status})")
            return
        if now - watch.phase_entered_at > STALE_START_AFTER_SECONDS:
            watch.phase = "stale_start"
            watch.phase_entered_at = now
            log.info(f"game_watcher: {watch.game_id} -> stale_start "
                     f"(нет данных о периодах {STALE_START_AFTER_SECONDS // 3600}ч)")
        watch.next_check_at = now + WATCHING_INTERVAL_SECONDS
        return

    if watch.phase == "final_period":
        peek = await game_peek.peek_game(watch.game_id)
        should_trigger = peek.fetched_ok and peek.game_status == 1
        if not should_trigger and now - watch.last_result_trigger_at > FINAL_PERIOD_SAFETY_INTERVAL_SECONDS:
            should_trigger = True  # safety-триггер независимо от статуса
        if should_trigger:
            watch.phase = "posting"
            watch.next_check_at = now
            return
        if now - watch.phase_entered_at > FINAL_PERIOD_GIVEUP_SECONDS:
            log.warning(f"game_watcher: {watch.game_id} слишком долго в final_period "
                        f"({FINAL_PERIOD_GIVEUP_SECONDS // 3600}ч), отдаю крону")
            del _watched_games[watch.game_id]
            return
        watch.next_check_at = now + FINAL_PERIOD_INTERVAL_SECONDS
        return

    if watch.phase == "posting":
        script, args = (("run_slpro_monitor.py", ["--only", "results"])
                        if watch.source == "slpro"
                        else ("game_results_monitor_final.py", []))
        log.info(f"game_watcher: {watch.game_id} — похоже, игра завершена, запускаю {script}")
        watch.last_result_trigger_at = now
        try:
            code, out, err = await script_runner.run_script(script, args)
            if code != 0:
                log.error(f"game_watcher: {script} завершился с кодом {code}: {err[-1000:]}")
                sheets_cache.report_error("game_watcher", f"{script} exit {code}: {err[-1000:]}")
        except Exception as e:
            log.error(f"game_watcher: ошибка запуска {script}: {e}")
            sheets_cache.report_error("game_watcher", str(e))
        del _watched_games[watch.game_id]
        return

    if watch.phase == "stale_start":
        peek = await game_peek.peek_game(watch.game_id)
        if peek.fetched_ok and peek.max_period >= 1:
            watch.phase = "watching"
            watch.phase_entered_at = now
            watch.next_check_at = now
            return
        if now - watch.phase_entered_at > STALE_START_GIVEUP_SECONDS:
            log.info(f"game_watcher: {watch.game_id} — сдаюсь после "
                     f"{STALE_START_GIVEUP_SECONDS // 3600}ч без признаков начала")
            del _watched_games[watch.game_id]
            return
        watch.next_check_at = now + STALE_START_INTERVAL_SECONDS
        return


async def tick() -> None:
    """Вызывается из фонового цикла демона каждые ~30с."""
    if not GAME_WATCHER_ENABLED:
        return
    refresh_watch_list()
    now = time.time()
    for game_id in list(_watched_games.keys()):
        watch = _watched_games.get(game_id)
        if watch is None or now < watch.next_check_at:
            continue
        try:
            await _advance(watch)
        except Exception as e:
            log.error(f"game_watcher: ошибка обработки игры {game_id}: {e}")
            watch.next_check_at = now + WATCHING_INTERVAL_SECONDS
