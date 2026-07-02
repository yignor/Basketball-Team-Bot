#!/usr/bin/env python3
"""
Адаптивный вотчер результатов игр — заменяет "слепой" опрос каждые 30 минут
на конечный автомат, привязанный к реальному прогрессу матча.

Явного таймера/обратного отсчёта в API Infobasket нет — вместо этого
ориентируемся на GameStatus (0/1) и номер текущего периода
(game_peek.peek_game): редко проверяем, пока не начался 4-й период, затем
часто, и публикуем результат сразу по GameStatus==1.

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
from datetime import datetime, timedelta
from typing import Dict, Optional

import sheets_cache
import script_runner
import game_peek
from datetime_utils import get_moscow_time

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
    phase: str = "scheduled"
    next_check_at: float = 0.0
    phase_entered_at: float = field(default_factory=time.time)
    last_result_trigger_at: float = 0.0


_watched_games: Dict[str, WatchedGame] = {}


def _parse_game_datetime(game_date: str, game_time: str) -> Optional[datetime]:
    try:
        naive = datetime.strptime(f"{game_date} {game_time}", "%d.%m.%Y %H:%M")
        return naive.replace(tzinfo=get_moscow_time().tzinfo)
    except ValueError:
        return None


def refresh_watch_list() -> None:
    """Подтягивает сегодняшние анонсированные игры без результата из
    локальной service_records. Записи без game_id (~5% случаев) не
    добавляются — peek невозможен без ID, их по-прежнему покрывает
    неизменный cron (работает по ссылке, не по ID)."""
    today = get_moscow_time().strftime("%d.%m.%Y")
    try:
        with sheets_cache.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT unique_key, game_id, game_date, game_time FROM service_records
                WHERE deleted = 0 AND data_type = 'АНОНС_ИГРА' AND game_date = ?
                  AND game_id != ''
                  AND game_id NOT IN (
                      SELECT game_id FROM service_records
                      WHERE deleted = 0 AND data_type = 'РЕЗУЛЬТАТ_ИГРА' AND game_id != ''
                  )
                """,
                (today,),
            ).fetchall()
    except Exception as e:
        log.warning(f"game_watcher: не удалось обновить список наблюдаемых игр: {e}")
        return

    for row in rows:
        game_id = row["game_id"]
        if game_id in _watched_games:
            continue
        watch = WatchedGame(game_id=game_id, game_date=row["game_date"], game_time=row["game_time"])
        watch.next_check_at = time.time()
        _watched_games[game_id] = watch
        log.info(f"game_watcher: слежу за игрой {game_id} ({row['game_date']} {row['game_time']})")


async def _advance(watch: WatchedGame) -> None:
    now = time.time()
    moscow_now = get_moscow_time()

    if watch.phase == "scheduled":
        game_dt = _parse_game_datetime(watch.game_date, watch.game_time)
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

    if watch.phase == "watching":
        peek = await game_peek.peek_game(watch.game_id)
        if not peek.fetched_ok:
            watch.next_check_at = now + WATCHING_INTERVAL_SECONDS
            return
        if peek.max_period >= 4 or peek.game_status == 1:
            watch.phase = "final_period"
            watch.phase_entered_at = now
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
        log.info(f"game_watcher: {watch.game_id} — похоже, игра завершена, запускаю проверку результатов")
        watch.last_result_trigger_at = now
        try:
            code, out, err = await script_runner.run_script("game_results_monitor_final.py", [])
            if code != 0:
                log.error(f"game_watcher: game_results_monitor_final.py завершился с кодом {code}: {err[-1000:]}")
                sheets_cache.report_error(
                    "game_watcher",
                    f"game_results_monitor_final.py exit {code}: {err[-1000:]}",
                )
        except Exception as e:
            log.error(f"game_watcher: ошибка запуска game_results_monitor_final.py: {e}")
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
