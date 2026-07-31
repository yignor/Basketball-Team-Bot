#!/usr/bin/env python3
"""
Ссылки на трансляции игр из VK.

У SLPRO ссылка на видео приходит прямо в протоколе — там ничего искать не надо.
А Инфобаскет видео не отдаёт: записи выкладывает лига, у себя в группе, обычным
постом «Команда А — Команда Б». Этот модуль их и находит.

Как ищем (без выдумок и без нейросетей):
  1. берём стену группы за окрестность даты игры (±2 дня — записи выкладывают
     не всегда в тот же вечер);
  2. оставляем посты, где встречаются ОБА названия команд;
  3. берём из поста видео-вложение и складываем ссылку.

Совпадение по одной команде не годится: в туре лига публикует десяток записей,
и «PULL UP» встретится в половине из них.

Найденное кладём в `game_meta.video_vk` — тот же столбец, куда SLPRO пишет свою
ссылку. Дальше сообщение о результате и отчёты берут её из локальной базы и в
VK не ходят: правило «в ответе человеку живых запросов нет» тут такое же.

Что нужно настроить (один раз):
  • VK_TOKEN в .env на сервере — сервисный ключ доступа из своего приложения
    VK (vk.com/apps?act=manage → создать → «Ключи доступа»). Прав на чтение
    стены открытой группы хватает, никаких пользовательских данных не нужно.
  • группы — строками в листе «Конфиг» с ТИП=VK (значение: короткое имя вроде
    `basketspb` или числовой id), либо VK_GROUPS через запятую в .env.
"""

import os
import re
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

API = "https://api.vk.com/method"
API_VERSION = "5.199"

# Сколько дней вокруг игры смотреть: запись могут выложить и на следующий день,
# и накануне (анонс с прошлой игрой в том же посте).
WINDOW_DAYS = 2
WALL_COUNT = 100


def token() -> str:
    return (os.getenv("VK_TOKEN") or os.getenv("VK_SERVICE_TOKEN") or "").strip()


def groups() -> List[str]:
    """Группы, где лига публикует записи.

    Читаем лист «Конфиг» тем же способом, что и лиги: строка с ТИП=VK, в
    колонке ИД — короткое имя группы (`basketspb`) или числовой id. Так админу
    не нужно лезть в .env ради того, что он и так настраивает в таблице.
    Запасной путь — VK_GROUPS через запятую в окружении."""
    out: List[str] = []
    try:
        import config_sheet
        rows = config_sheet.split(sheets_cache.get_config_rows() or [])[config_sheet.GAME]
        for row in rows:
            cells = [str(c or "").strip() for c in list(row) + [""] * 3]
            if cells[0].upper() != "VK":
                continue
            val = cells[1] or cells[2]
            if val and val.upper() not in ("ИД", "ID"):
                out.append(val)
    except Exception:
        pass
    if not out:
        raw = os.getenv("VK_GROUPS", "")
        out = [x.strip() for x in raw.split(",") if x.strip()]
    return out


def _norm(text: str) -> str:
    """Название команды к сравнимому виду: регистр, ё, пробелы и дефисы."""
    return re.sub(r"[\s\-_/.]", "", (text or "").lower().replace("ё", "е"))


async def _call(method: str, **params: Any) -> Optional[Dict[str, Any]]:
    """Вызов VK API. None — если не настроено или не ответили."""
    import aiohttp
    tok = token()
    if not tok:
        return None
    params.update(access_token=tok, v=API_VERSION)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{API}/{method}", params=params,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                data = await r.json(content_type=None)
    except Exception as e:
        print(f"⚠️ VK {method}: {e}")
        return None
    if isinstance(data, dict) and data.get("error"):
        err = data["error"]
        print(f"⚠️ VK {method}: {err.get('error_msg')} (код {err.get('error_code')})")
        return None
    return (data or {}).get("response")


def _video_link(post: Dict[str, Any]) -> str:
    """Ссылка на первое видео поста. Пусто — если видео в посте нет."""
    for att in (post.get("attachments") or []):
        if att.get("type") != "video":
            continue
        v = att.get("video") or {}
        owner, vid = v.get("owner_id"), v.get("id")
        if owner is not None and vid is not None:
            link = f"https://vk.com/video{owner}_{vid}"
            key = v.get("access_key")
            return f"{link}?list={key}" if key else link
    # Видео может лежать не вложением, а ссылкой в тексте.
    m = re.search(r"https?://(?:www\.)?vk\.com/video-?\d+_\d+\S*", post.get("text") or "")
    return m.group(0) if m else ""


async def find_for_game(game_date: str, team_a: str, team_b: str,
                        group: Optional[str] = None) -> str:
    """Ссылка на запись игры или пустая строка.

    game_date — ISO. Названия команд — как их пишет лига; сравниваем нестрого
    (регистр, дефисы, ё), потому что в постах их сокращают."""
    import datetime as _dt
    if not (game_date and team_a and team_b):
        return ""
    try:
        day = _dt.date.fromisoformat(game_date)
    except ValueError:
        return ""
    lo = int(_dt.datetime.combine(day - _dt.timedelta(days=WINDOW_DAYS),
                                  _dt.time.min).timestamp())
    hi = int(_dt.datetime.combine(day + _dt.timedelta(days=WINDOW_DAYS),
                                  _dt.time.max).timestamp())
    want = (_norm(team_a), _norm(team_b))

    for g in ([group] if group else groups()):
        owner = g if str(g).lstrip("-").isdigit() else None
        params: Dict[str, Any] = {"count": WALL_COUNT}
        if owner:
            params["owner_id"] = -abs(int(owner))
        else:
            params["domain"] = str(g).lstrip("@")
        resp = await _call("wall.get", **params)
        for post in ((resp or {}).get("items") or []):
            ts = int(post.get("date") or 0)
            if not (lo <= ts <= hi):
                continue
            text = _norm(post.get("text") or "")
            if not all(w in text for w in want):
                continue
            link = _video_link(post)
            if link:
                return link
    return ""


def store(source: str, game_id: Any, link: str) -> bool:
    """Кладёт ссылку в game_meta. Пустую не пишем и чужую не затираем."""
    if not link:
        return False
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        n = conn.execute(
            """UPDATE game_meta SET video_vk = ?
               WHERE source = ? AND game_id = ? AND video_vk = ''""",
            (link, source, str(game_id))).rowcount
        conn.commit()
    return bool(n)


def games_without_video(limit: int = 20) -> List[Dict[str, Any]]:
    """Наши сыгранные игры без ссылки на запись — кандидаты на поиск."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        ours = [str(r["team_id"]) for r in conn.execute(
            "SELECT team_id FROM league_teams WHERE ours = 1")]
        if not ours:
            return []
        marks = ",".join("?" * len(ours))
        return [dict(r) for r in conn.execute(
            f"""SELECT source, game_id, game_date, home_name, guest_name
                FROM game_meta
                WHERE video_vk = '' AND game_date != ''
                  AND home_name != '' AND guest_name != ''
                  AND (home_team_id IN ({marks}) OR guest_team_id IN ({marks}))
                ORDER BY game_date DESC LIMIT ?""", ours + ours + [limit])]


def _announce_text(game: Dict[str, Any], link: str) -> str:
    """Текст оповещения о появившейся записи."""
    d = str(game.get("game_date") or "")
    when = f"{d[8:10]}.{d[5:7]}" if len(d) >= 10 else d
    return (f"📹 Появилась запись игры\n"
            f"🏀 {game.get('home_name')} — {game.get('guest_name')}"
            f"{f' · {when}' if when else ''}\n\n"
            f"<a href=\"{link}\">Смотреть</a>")


async def announce(bot: Any, game: Dict[str, Any], link: str,
                   chat_ids: Optional[List[Any]] = None,
                   topic_id: Optional[int] = None) -> Dict[str, int]:
    """Оповестить о найденной записи: чат команды и подписчики.

    Три адресата, и они не пересекаются по смыслу:
      • общий чат — всем, кто и так тут;
      • подписка на команду — тем, кого в чате нет;
      • подписка на игрока — тем, кто следит за конкретным человеком, и
        только если он в ЭТОЙ игре выходил на площадку.
    Кто попал сразу в две личные рассылки, получит одно сообщение."""
    import subscriptions
    text = _announce_text(game, link)
    out = {"chat": 0, "team": 0, "players": 0}
    for cid in (chat_ids or []):
        try:
            kwargs: Dict[str, Any] = {"chat_id": cid, "text": text,
                                      "parse_mode": "HTML",
                                      "disable_web_page_preview": False}
            if topic_id is not None:
                kwargs["message_thread_id"] = topic_id
            await bot.send_message(**kwargs)
            out["chat"] += 1
        except Exception as e:
            print(f"⚠️ VK: оповещение в чат {cid} не ушло — {e}")

    team = subscriptions.subscribers("team")
    watchers = subscriptions.watchers_of_game(game["source"], game["game_id"])
    out["team"] = await subscriptions.deliver_to(bot, team, text)
    # Тем, кто уже получил как подписчик команды, второй раз не шлём.
    only_players = [u for u in watchers if u not in set(team)]
    out["players"] = await subscriptions.deliver_to(bot, only_players, text)
    return out


async def sync(limit: int = 20, bot: Any = None,
               chat_ids: Optional[List[Any]] = None,
               topic_id: Optional[int] = None) -> Dict[str, int]:
    """Пройтись по играм без записи и поискать их в VK. Для фонового цикла.

    Нашли новую — сразу оповещаем (если передан bot): запись тем и ценна, что
    её можно посмотреть, а не узнать о ней через неделю из отчёта."""
    out = {"looked": 0, "found": 0, "notified": 0}
    if not token() or not groups():
        return out
    for g in games_without_video(limit):
        out["looked"] += 1
        link = await find_for_game(g["game_date"], g["home_name"], g["guest_name"])
        if not (link and store(g["source"], g["game_id"], link)):
            continue
        out["found"] += 1
        print(f"📹 VK: {g['home_name']} — {g['guest_name']} ({g['game_date']}): {link}")
        if bot is not None:
            try:
                res = await announce(bot, g, link, chat_ids, topic_id)
                out["notified"] += res["chat"] + res["team"] + res["players"]
                print(f"   оповещено: чатов {res['chat']}, по команде {res['team']}, "
                      f"по игрокам {res['players']}")
            except Exception as e:
                print(f"⚠️ VK: оповещение не ушло — {e}")
    return out


async def main() -> int:
    import asyncio
    if not token():
        print("VK_TOKEN не задан — искать нечем. См. шапку файла.")
        return 1
    if not groups():
        print("Группы не настроены: строки ТИП=VK в «Конфиге» или VK_GROUPS в .env.")
        return 1
    res = await sync()
    print(f"VK: просмотрено игр {res['looked']}, найдено записей {res['found']}")
    return 0


if __name__ == "__main__":
    import asyncio
    import sys
    sys.exit(asyncio.run(main()))
