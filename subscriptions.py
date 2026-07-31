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

# Что включено, пока человек ничего не трогал.
#
# Фэнтези и личная статистика приходили и раньше — выключить их было нельзя,
# поэтому оставляем как было: подписан, пока не отписался.
#
# А вот результаты в личку — НОВОЕ. Включи их по умолчанию, и вся команда
# внезапно начнёт получать дубль того, что и так видит в общем чате. Такие
# вещи включает человек сам.
DEFAULTS = {"team": False, "fantasy": True, "personal": True}

# Пояснение под каждым тумблером — человеку должно быть понятно, от чего он
# отписывается, до того как отпишется.
HINTS = {
    "team": "счёт, лучшие игроки и ссылка на протокол — сразу после игры",
    "fantasy": "кто сколько набрал за неделю и как идут дела в сезоне",
    "personal": "твои игры, форма и файл за месяц (только в личку)",
}


def enabled(user_id: Any, kind: str) -> bool:
    """Подписан ли. Нет записи — берём значение по умолчанию для этого вида."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT enabled FROM subscriptions WHERE user_id = ? AND kind = ?",
            (str(user_id), kind)).fetchone()
    return DEFAULTS.get(kind, True) if row is None else bool(row["enabled"])


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
    """Кому слать в личку.

    Для видов, выключенных по умолчанию (результаты игр), это ровно те, кто
    подписался явно — читаем прямо из таблицы. Для включённых по умолчанию —
    все, кто запускал бота и не отписался. Список строим от bot_users, а не от
    листа «Игроки»: подписка про человека, а не про игровой статус."""
    sheets_cache.init_db()
    if not DEFAULTS.get(kind, True):
        with sheets_cache.get_connection() as conn:
            return [str(r["user_id"]) for r in conn.execute(
                "SELECT user_id FROM subscriptions WHERE kind = ? AND enabled = 1",
                (kind,))]
    with sheets_cache.get_connection() as conn:
        known = [str(r["telegram_id"]) for r in conn.execute(
            "SELECT telegram_id FROM bot_users WHERE telegram_id != ''")]
    return filter_subscribed(known, kind)


# ── Подписка на конкретного игрока ──────────────────────────────────────────

def player_toggle(user_id: Any, ref: str) -> bool:
    """Подписаться/отписаться от игрока. Возвращает новое состояние."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT 1 FROM player_subscriptions WHERE user_id = ? AND ref = ?",
            (str(user_id), ref)).fetchone()
        if row:
            conn.execute(
                "DELETE FROM player_subscriptions WHERE user_id = ? AND ref = ?",
                (str(user_id), ref))
            conn.commit()
            return False
        conn.execute(
            """INSERT INTO player_subscriptions (user_id, ref, created_at)
               VALUES (?, ?, ?)""", (str(user_id), ref, sheets_cache.now_iso()))
        conn.commit()
    return True


def my_players(user_id: Any) -> List[str]:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return [r["ref"] for r in conn.execute(
            "SELECT ref FROM player_subscriptions WHERE user_id = ?", (str(user_id),))]


def watchers_of_game(source: str, game_id: Any) -> List[str]:
    """Кто подписан хоть на кого-то из ВЫШЕДШИХ в этой игре.

    Сверяем по протоколу, а не по заявке: подписка на игрока — это «покажи мне
    матч, когда он играет», а не «когда его команда играет»."""
    sheets_cache.init_db()
    pref = "slpro" if source == "slpro" else "ib"
    with sheets_cache.get_connection() as conn:
        played = {f"{pref}:{r['team_id']}:{r['player_id']}" for r in conn.execute(
            """SELECT team_id, player_id FROM game_player_stats
               WHERE source = ? AND game_id = ?""", (source, str(game_id)))}
        if not played:
            return []
        # Ссылка подписки может быть составной («ib:..+slpro:..»): человек один,
        # а id у него два. Разворачиваем и сравниваем по частям.
        out: List[str] = []
        for r in conn.execute("SELECT user_id, ref FROM player_subscriptions"):
            if set(str(r["ref"]).split("+")) & played:
                out.append(str(r["user_id"]))
    return sorted(set(out))


async def deliver_to(bot: Any, user_ids: List[str], text: str,
                     parse_mode: Optional[str] = "HTML") -> int:
    """Разослать конкретным людям. Ошибка у одного не роняет остальных."""
    sent = 0
    for uid in dict.fromkeys(str(u) for u in user_ids):
        try:
            await bot.send_message(chat_id=int(uid), text=text,
                                   parse_mode=parse_mode,
                                   disable_web_page_preview=True)
            sent += 1
        except Exception as e:
            print(f"⚠️ Рассылка: не доставлено {uid} — {e}")
    return sent


async def deliver(bot: Any, kind: str, text: str,
                  parse_mode: Optional[str] = "HTML") -> int:
    """Разослать текст подписчикам в личку. Возвращает, сколько дошло.

    Ошибка у одного (закрыл личку, заблокировал бота) не должна ронять
    рассылку остальным — поэтому каждый в своём try."""
    sent = 0
    for uid in subscribers(kind):
        try:
            await bot.send_message(chat_id=int(uid), text=text,
                                   parse_mode=parse_mode,
                                   disable_web_page_preview=True)
            sent += 1
        except Exception as e:
            print(f"⚠️ Подписка «{kind}»: не доставлено {uid} — {e}")
    return sent
