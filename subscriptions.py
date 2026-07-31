#!/usr/bin/env python3
"""
Подписки: что человек хочет получать в личку.

До этого рассылки были устроены «всем, кого знаем»: понедельничная таблица
фэнтези уходила каждому участнику, месячный отчёт — каждому привязанному.
Отписаться было нельзя, и единственный способ перестать получать — попросить
админа. Теперь у каждой рассылки есть выключатель, и он у самого человека.

Три вида:
  • `team`     — результаты игр в личку (в общий чат они идут всегда);
  • `fantasy`  — понедельничная таблица фэнтези;
  • `personal` — личная статистика (месячный файл и разборы).

По умолчанию всё ВКЛЮЧЕНО: строки в таблице нет — значит подписан. Так
поведение не меняется для тех, кто ничего не трогал, и не приходится
заполнять таблицу при первом запуске.

Ключ — числовой Telegram id: ник меняется и переуступается, id — нет.
"""

from typing import Any, Dict, List, Optional

import sheets_cache

KINDS = {
    "team": "🏀 Результаты игр в личку",
    "fantasy": "🏆 Таблица фэнтези по понедельникам",
    "personal": "📊 Личная статистика и месячный отчёт",
}

# Пояснение под каждым тумблером — человеку должно быть понятно, от чего он
# отписывается, до того как отпишется.
HINTS = {
    "team": "счёт, лучшие игроки и ссылка на протокол — сразу после игры",
    "fantasy": "кто сколько набрал за неделю и как идут дела в сезоне",
    "personal": "твои игры, форма и файл за месяц (только в личку)",
}


def enabled(user_id: Any, kind: str) -> bool:
    """Подписан ли. Нет записи — да: по умолчанию всё включено."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT enabled FROM subscriptions WHERE user_id = ? AND kind = ?",
            (str(user_id), kind)).fetchone()
    return True if row is None else bool(row["enabled"])


def all_of(user_id: Any) -> Dict[str, bool]:
    return {k: enabled(user_id, k) for k in KINDS}


def toggle(user_id: Any, kind: str) -> bool:
    """Переключает и возвращает новое состояние."""
    if kind not in KINDS:
        return True
    new = not enabled(user_id, kind)
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO subscriptions (user_id, kind, enabled, updated_at)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(user_id, kind) DO UPDATE SET
                 enabled = excluded.enabled, updated_at = excluded.updated_at""",
            (str(user_id), kind, 1 if new else 0, sheets_cache.now_iso()))
        conn.commit()
    return new


def filter_subscribed(user_ids: List[Any], kind: str) -> List[str]:
    """Оставляет тех, кто не отписался. Одним запросом: рассылка идёт по
    десяткам людей, и дёргать базу на каждого незачем."""
    ids = [str(u) for u in user_ids]
    if not ids or kind not in KINDS:
        return ids
    sheets_cache.init_db()
    marks = ",".join("?" * len(ids))
    with sheets_cache.get_connection() as conn:
        off = {str(r["user_id"]) for r in conn.execute(
            f"""SELECT user_id FROM subscriptions
                WHERE kind = ? AND enabled = 0 AND user_id IN ({marks})""",
            [kind] + ids)}
    return [u for u in ids if u not in off]


def subscribers(kind: str) -> List[str]:
    """Кому слать результаты в личку: все, кто когда-либо запускал бота и не
    отписался. Список нарочно строим от bot_users, а не от листа «Игроки»:
    подписка — про человека, а не про игровой статус."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        known = [str(r["telegram_id"]) for r in conn.execute(
            "SELECT telegram_id FROM bot_users WHERE telegram_id != ''")]
    return filter_subscribed(known, kind)
