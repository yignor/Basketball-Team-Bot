#!/usr/bin/env python3
"""Что бот знает о человеке и как это стереть.

Согласие на обработку бот не собирает — так решено 10.09.2026. Но человеку
должно быть где прочитать, что о нём хранится и зачем, и должна быть кнопка,
которая это убирает. Одно без другого не работает: кнопка «забыть меня» без
объяснения пугает, объяснение без кнопки — пустые слова.

**Что делает «забыть меня» — по таблицам, а не «всё подряд».** Данные человека
лежат в двадцати с лишним местах, и обращаться с ними приходится по-разному:

* **удаляем** то, что касается только его: привязку к листу, профили лиг,
  подписки, настройки, значки, открытые доступы, свои шутки, имя и ник;
* **обезличиваем** то, на чём стоят чужие таблицы: голоса в опросах и история
  фэнтези. Строка остаётся, но вместо его id — случайная метка, которую назад
  не развернуть. Иначе у остальных поехал бы зачёт фэнтези и посещаемость;
* **не трогаем** то, что принадлежит не ему: платежи — это учёт тренера, строка
  в листе «Игроки» — его список команды, частные занятия — дела тренера.

Про лист «Игроки» говорим прямо: пока человек в списке команды и пишет боту,
бот его снова узнает. Убрать себя из списка может только тренер.
"""

from __future__ import annotations

import logging
import secrets
from typing import Any, Dict, List, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# Что удаляем целиком: (таблица, столбец с telegram id).
DELETE: Tuple[Tuple[str, str], ...] = (
    ("player_links", "tg_user_id"),
    ("player_identities", "tg_user_id"),
    ("player_report_prefs", "tg_user_id"),
    ("player_subscriptions", "user_id"),
    ("subscriptions", "user_id"),
    ("fantasy_notify_prefs", "user_id"),
    ("achievement_awards", "user_id"),
    ("feature_access", "tg_user_id"),
    ("player_jokes", "author_id"),
    ("bot_users", "telegram_id"),
)

# Что обезличиваем: (таблица, столбец с id, столбцы с именем — их обнуляем).
MASK: Tuple[Tuple[str, str, Tuple[str, ...]], ...] = (
    ("game_votes", "user_id", ("username", "first_name", "last_name")),
    ("attendance", "user_id", ("username", "first_name")),
    ("fantasy_rosters", "user_id", ()),
    ("fantasy_game_picks", "user_id", ()),
    ("fantasy_game_scores", "user_id", ()),
    ("fantasy_weekly_scores", "user_id", ()),
    ("feedback", "user_id", ("username",)),
)

ABOUT = (
    "🔒 Какие данные о тебе хранит бот\n\n"
    "Бот — помощник команды, и знает ровно то, что нужно для этого.\n\n"
    "📋 Из списка команды, который ведёт тренер: фамилия, имя, отчество, дата "
    "рождения, суммы взносов. Отчество нужно для заявки в лигу и берётся из "
    "самой лиги. Это таблица тренера — бот её читает и пишет туда только по "
    "его кнопкам.\n\n"
    "💬 Из Telegram: твой id и ник — чтобы писать тебе в личку и узнавать "
    "в опросах.\n\n"
    "🏀 Из протоколов лиг: статистика игр. Её публикует сама лига.\n\n"
    "🗳 Твои ответы в опросах, состав в фэнтези, подписки, значки.\n\n"
    "💳 Платежи, которые отметил тренер или пришли по СМС.\n\n"
    "ФИО из протоколов лиг на диск не записывается — оно держится только в "
    "памяти бота и исчезает при перезапуске.\n\n"
    "Никому за пределами команды бот данные не передаёт. Общий чат видит только "
    "то, что и так общее: составы, результаты, опросы.\n\n"
    "Хочешь, чтобы бот тебя забыл, — кнопка ниже. Перед удалением покажу, "
    "что уйдёт, а что останется."
)


def _table_cols(conn, table: str) -> List[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})")]


def footprint(user_id: Any) -> Dict[str, int]:
    """Сколько записей о человеке где лежит. Для экрана подтверждения."""
    sheets_cache.init_db()
    uid = str(user_id)
    out: Dict[str, int] = {}
    with sheets_cache.get_connection() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")}
        for table, col in DELETE:
            if table in tables and col in _table_cols(conn, table):
                n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?",
                                 (uid,)).fetchone()[0]
                if n:
                    out[table] = int(n)
        for table, col, _names in MASK:
            if table in tables and col in _table_cols(conn, table):
                n = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} = ?",
                                 (uid,)).fetchone()[0]
                if n:
                    out[table] = int(n)
    return out


def forget(user_id: Any) -> Dict[str, int]:
    """Забывает человека. {deleted, masked} — сколько строк удалено и обезличено.

    Всё в одной транзакции: половинчатое забвение хуже никакого — человек
    решит, что его стёрли, а часть данных останется."""
    sheets_cache.init_db()
    uid = str(user_id)
    # Метка случайная, а не из хеша id: telegram-id всего десять цифр, и хеш
    # перебором разворачивается обратно за часы. Случайную — нельзя.
    token = "forgotten-" + secrets.token_hex(6)
    deleted = masked = 0
    with sheets_cache.get_connection() as conn:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'")}
        try:
            for table, col in DELETE:
                if table not in tables or col not in _table_cols(conn, table):
                    continue
                cur = conn.execute(f"DELETE FROM {table} WHERE {col} = ?", (uid,))
                deleted += cur.rowcount
            for table, col, names in MASK:
                cols = _table_cols(conn, table) if table in tables else []
                if col not in cols:
                    continue
                blank = [n for n in names if n in cols]
                sets = ", ".join([f"{col} = ?"] + [f"{n} = ''" for n in blank])
                cur = conn.execute(f"UPDATE {table} SET {sets} WHERE {col} = ?",
                                   (token, uid))
                masked += cur.rowcount
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    # В журнал — только числа. Сам id не пишем: человек просил его забыть.
    logger.info("Забыт пользователь: удалено %d, обезличено %d", deleted, masked)
    return {"deleted": deleted, "masked": masked}
