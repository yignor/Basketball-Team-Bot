"""Кто в какой команде: по личке не видно, в каком чате пишет человек.

В группе всё просто — команду определяет чат. В личке чата нет, а именно в
личке живёт почти всё: раздел тренера, оплаты, своя статистика. Поэтому по
telegram-id надо уметь найти команды, к которым человек имеет отношение.

Ищем перебором баз. Это выглядит расточительно, но на нужном масштабе — нет:
пятнадцать команд это пятнадцать открытий файла по 20 КБ. Городить общий
индекс «человек → команда» раньше времени значит завести второе место, где
хранится то же самое, и потом ловить их расхождение.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from . import db, tenants


def teams_of(user_id: Any) -> List[Dict[str, Any]]:
    """Команды, где человек тренер или игрок. Тренерские — первыми."""
    uid = str(user_id)
    out: List[Dict[str, Any]] = []
    for team in tenants.all_teams():
        try:
            with db.use(team["slug"]), db.connection() as conn:
                is_coach = bool(conn.execute(
                    "SELECT 1 FROM settings WHERE key = 'coach_id' AND value = ?",
                    (uid,)).fetchone())
                row = conn.execute(
                    "SELECT id, surname, name FROM players WHERE tg_user_id = ?",
                    (uid,)).fetchone()
        except Exception:
            # Битая или недоразвёрнутая база одной команды не должна прятать
            # от человека остальные.
            continue
        if is_coach or row:
            out.append({**team, "is_coach": is_coach,
                        "player_id": int(row["id"]) if row else 0})
    out.sort(key=lambda t: (not t["is_coach"], t["title"]))
    return out


def only_team(user_id: Any) -> Optional[Dict[str, Any]]:
    """Единственная команда человека — или None, если их несколько.

    Несколько команд у одного человека — не редкость (играет за две, тренирует
    третью). Угадывать в таком случае нельзя: покажем не те долги и не тот
    состав. Пусть выбирает явно."""
    got = teams_of(user_id)
    return got[0] if len(got) == 1 else None
