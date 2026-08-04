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

# Оповещаем только о СВЕЖИХ играх. Первый проход разбирает всю историю разом,
# и без этого порога чат получает пачку уведомлений о матчах месячной давности
# — ровно так и вышло при первом запуске. Ссылки к старым играм всё равно
# сохраняются, просто молча: в отчёте и в сообщении о результате они появятся.
ANNOUNCE_MAX_AGE_DAYS = 3

# Трансляция появляется в группе около начала матча, поэтому смотреть начинаем
# заранее и продолжаем, пока игра идёт. Раньше бот искал только ЗАПИСИ уже
# сыгранных игр — ссылка приходила на следующий день, когда смотреть незачем.
LIVE_LEAD_MINUTES = 15          # за сколько до начала начинаем смотреть
LIVE_TAIL_HOURS = 3             # сколько ещё смотрим после начала
LIVE_SCAN_SECONDS = 180         # как часто дёргаем VK по одной игре


_env_loaded = False


def token() -> str:
    """Токен ВК. Если в окружении пусто — дочитываем .env.

    Демон грузит .env сам, а кроновые процессы (ingest фэнтези, где теперь
    уточняется начало эфира) — нет: без этого VK-часть у них молча
    отключалась, и тайм-коды навсегда оставались посчитанными по расписанию."""
    global _env_loaded
    tok = (os.getenv("VK_TOKEN") or os.getenv("VK_SERVICE_TOKEN") or "").strip()
    if not tok and not _env_loaded:
        _env_loaded = True
        try:
            from dotenv import load_dotenv
            load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
        except Exception:
            return ""
        tok = (os.getenv("VK_TOKEN") or os.getenv("VK_SERVICE_TOKEN") or "").strip()
    return tok


def groups() -> List[str]:
    """Группы, где лига публикует записи.

    Читаем лист «Конфиг» тем же способом, что и лиги: строка с ТИП=VK, в
    колонке ИД — короткое имя группы (`basketspb`) или числовой id. Так админу
    не нужно лезть в .env ради того, что он и так настраивает в таблице.
    Запасной путь — VK_GROUPS через запятую в окружении."""
    out: List[str] = []
    try:
        import config_sheet
        from enhanced_duplicate_protection import VK_TYPE_ALIASES
        rows = config_sheet.split(sheets_cache.get_config_rows() or [])[config_sheet.GAME]
        for row in rows:
            cells = [str(c or "").strip() for c in list(row) + [""] * 3]
            # Тип пишут и латиницей, и кириллицей («вк») — принимаем оба.
            if cells[0].upper() not in VK_TYPE_ALIASES:
                continue
            # Имя группы кладут то в «ИД», то в «ИД команды» — обе колонки
            # выглядят подходящими. Берём ту, что заполнена.
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


VIDEO_ID_RE = re.compile(r"(?:video|live|clip)(-?\d+)_(\d+)")


def parse_link(link: str) -> Tuple[str, str, str]:
    """(owner_id, video_id, access_key) из ссылки. Пусто — если не разобрали.

    Одно и то же видео ВК отдаёт под тремя адресами (video/live/clip) и на двух
    доменах — берём из ссылки только id, всё остальное спросим у API."""
    m = VIDEO_ID_RE.search(str(link or ""))
    if not m:
        return "", "", ""
    key = re.search(r"[?&]list=([\w-]+)", str(link))
    return m.group(1), m.group(2), (key.group(1) if key else "")


async def video_meta(link: str) -> Dict[str, Any]:
    """Когда началась трансляция и сколько идёт: {started_at, seconds}.

    started_at — то, ради чего это всё: реальное время начала эфира. Зная его
    и время спорного из протокола, положение матча в записи считается
    вычитанием, а не догадкой «трансляцию включили по расписанию».

    У законченного эфира ВК держит время начала в самом видео (`date`), а у
    идущего — ещё и в `live_start_time`; берём то, что есть."""
    owner, vid, key = parse_link(link)
    if not owner or not vid:
        return {}
    # Ключ из ссылки (`list=`) подходит не всегда: у публичного видео его либо
    # нет, либо это ключ плейлиста. Пробуем с ним и без него, чтобы из-за
    # лишнего хвоста не потерять время начала эфира.
    items = []
    for videos in ([f"{owner}_{vid}_{key}"] if key else []) + [f"{owner}_{vid}"]:
        res = await _call("video.get", videos=videos, count=1)
        items = (res or {}).get("items") or []
        if items:
            break
    if not items:
        return {}
    v = items[0]
    started = int(v.get("live_start_time") or v.get("date") or 0)
    return {"started_at": started, "seconds": int(v.get("duration") or 0),
            "live": str(v.get("live_status") or "")}


def store_video_meta(source: str, game_id: Any, meta: Dict[str, Any]) -> bool:
    """Кладёт время начала эфира в game_meta. Ноль не пишем."""
    if not meta.get("started_at"):
        return False
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        n = conn.execute(
            """UPDATE game_meta SET video_started_at = ?, video_seconds = ?
                WHERE source = ? AND game_id = ?""",
            (int(meta["started_at"]), int(meta.get("seconds") or 0),
             source, str(game_id))).rowcount
        conn.commit()
    return bool(n)


def video_started_at(source: str, game_id: Any) -> int:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT video_started_at FROM game_meta WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return int(row["video_started_at"] or 0) if row else 0


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
    # team_b пустой — ищем по одному названию: у трансляции в посте часто
    # только соперник и время, своего названия может не быть вовсе.
    if not (game_date and (team_a or team_b)):
        return ""
    try:
        day = _dt.date.fromisoformat(game_date)
    except ValueError:
        return ""
    lo = int(_dt.datetime.combine(day - _dt.timedelta(days=WINDOW_DAYS),
                                  _dt.time.min).timestamp())
    hi = int(_dt.datetime.combine(day + _dt.timedelta(days=WINDOW_DAYS),
                                  _dt.time.max).timestamp())
    want = tuple(w for w in (_norm(team_a), _norm(team_b)) if w)

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


def store(source: str, game_id: Any, link: str,
          game_date: str = "", home: str = "", guest: str = "") -> bool:
    """Кладёт ссылку в game_meta. Пустую не пишем и чужую не затираем.

    Игра могла ещё не состояться — тогда строки в game_meta просто нет
    (её заводит выкачка бокс-скора). Заводим заготовку с тем, что знаем из
    расписания: иначе найденную трансляцию некуда положить."""
    if not link:
        return False
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        n = conn.execute(
            """UPDATE game_meta SET video_vk = ?
               WHERE source = ? AND game_id = ? AND video_vk = ''""",
            (link, source, str(game_id))).rowcount
        if not n:
            exists = conn.execute(
                "SELECT 1 FROM game_meta WHERE source = ? AND game_id = ?",
                (source, str(game_id))).fetchone()
            if not exists:
                conn.execute(
                    """INSERT INTO game_meta (source, game_id, game_date,
                                              home_name, guest_name, video_vk, fetched_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (source, str(game_id), game_date, home, guest, link,
                     sheets_cache.now_iso()))
                n = 1
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


def is_fresh(game_date: str, today: Optional[Any] = None) -> bool:
    """Стоит ли вообще шуметь об этой записи.

    Запись игры интересна, пока игру помнят. Через неделю оповещение — это уже
    не новость, а спам, и особенно неприятно, когда таких сразу четыре."""
    import datetime as _dt
    try:
        day = _dt.date.fromisoformat(str(game_date))
    except ValueError:
        return False
    now = today or _dt.date.today()
    return 0 <= (now - day).days <= ANNOUNCE_MAX_AGE_DAYS


def _announce_text(game: Dict[str, Any], link: str, live: bool = False) -> str:
    """Текст оповещения. Трансляция и запись — разные новости."""
    d = str(game.get("game_date") or "")
    when = f"{d[8:10]}.{d[5:7]}" if len(d) >= 10 else d
    head = "📺 Идёт трансляция" if live else "📹 Появилась запись игры"
    action = "Смотреть эфир" if live else "Смотреть"
    return (f"{head}\n"
            f"🏀 {game.get('home_name')} — {game.get('guest_name')}"
            f"{f' · {when}' if when else ''}\n\n"
            f"<a href=\"{link}\">{action}</a>")


async def announce(bot: Any, game: Dict[str, Any], link: str,
                   chat_ids: Optional[List[Any]] = None,
                   topic_id: Optional[int] = None,
                   live: bool = False) -> Dict[str, int]:
    """Оповестить о найденной записи: чат команды и подписчики.

    Три адресата, и они не пересекаются по смыслу:
      • общий чат — всем, кто и так тут;
      • подписка на команду — тем, кого в чате нет;
      • подписка на игрока — тем, кто следит за конкретным человеком, и
        только если он в ЭТОЙ игре выходил на площадку.
    Кто попал сразу в две личные рассылки, получит одно сообщение."""
    import subscriptions
    text = _announce_text(game, link, live)
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


# Когда последний раз смотрели VK по конкретной игре: {source:game_id: время}.
# В памяти — после рестарта проверим заново, это дешевле, чем таблица.
_live_seen: Dict[str, float] = {}


def live_candidates(now: Optional[Any] = None) -> List[Dict[str, Any]]:
    """Игры, у которых прямо сейчас может идти трансляция.

    Берём из расписания (записи опросов), а не из game_meta: там игра
    появляется только после выкачки бокс-скора, то есть уже сыгранной."""
    import datetime as _dt
    import game_roster
    now = now or _dt.datetime.now()
    today = now.date()
    out = []
    for g in game_roster.games(from_day=today - _dt.timedelta(days=1),
                               until_day=today + _dt.timedelta(days=1)):
        start = _game_start(g)
        if not start:
            continue
        if not (start - _dt.timedelta(minutes=LIVE_LEAD_MINUTES) <= now
                <= start + _dt.timedelta(hours=LIVE_TAIL_HOURS)):
            continue
        if link_of(g["source"], g["game_id"]):
            continue                      # ссылка уже есть — искать нечего
        out.append({"source": g["source"], "game_id": g["game_id"],
                    "game_date": g["date"].isoformat(),
                    "home_name": g.get("opponent") or "",
                    "guest_name": g.get("title") or "",
                    "opponent": g.get("opponent") or "", "start": start})
    return out


def _game_start(game: Dict[str, Any]) -> Optional[Any]:
    """Дата и время начала матча из расписания. Без времени игру не сторожим:
    сканировать сутки напролёт ради одной ссылки незачем."""
    import datetime as _dt
    raw = str(game.get("time") or "").strip()
    if not raw:
        return None
    for fmt in ("%H:%M:%S", "%H:%M", "%H.%M"):
        try:
            t = _dt.datetime.strptime(raw[:8] if len(raw) >= 8 else raw, fmt).time()
        except ValueError:
            continue
        return _dt.datetime.combine(game["date"], t)
    return None


def link_of(source: str, game_id: Any) -> str:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT video_vk FROM game_meta WHERE source = ? AND game_id = ?",
            (source, str(game_id))).fetchone()
    return str((row["video_vk"] if row else "") or "")


async def watch_live(bot: Any = None, chat_ids: Optional[List[Any]] = None,
                     topic_id: Optional[int] = None) -> Dict[str, int]:
    """Сторожит трансляции идущих матчей. Зовётся из фонового цикла.

    Сама решает, когда работать: вне окна матча не делает ни одного запроса,
    а по одной игре дёргает VK не чаще, чем раз в LIVE_SCAN_SECONDS."""
    import time as _time
    out = {"watching": 0, "found": 0, "notified": 0}
    if not (token() and groups()):
        return out
    now = _time.time()
    for g in live_candidates():
        key = f"{g['source']}:{g['game_id']}"
        if now - _live_seen.get(key, 0) < LIVE_SCAN_SECONDS:
            continue
        _live_seen[key] = now
        out["watching"] += 1
        # Ищем по сопернику: своё название команды в постах группы лиги
        # пишут не всегда, а соперника — почти обязательно.
        link = await find_for_game(g["game_date"], g["opponent"], "")
        if not link:
            continue
        if not store(g["source"], g["game_id"], link, g["game_date"],
                     g["home_name"], g["guest_name"]):
            continue
        out["found"] += 1
        print(f"📺 VK: трансляция {g['opponent']} ({g['game_date']}): {link}")
        if bot is not None:
            try:
                res = await announce(bot, g, link, chat_ids, topic_id, live=True)
                out["notified"] += res["chat"] + res["team"] + res["players"]
            except Exception as e:
                print(f"⚠️ VK: оповещение о трансляции не ушло — {e}")
    return out


async def sync(limit: int = 20, bot: Any = None,
               chat_ids: Optional[List[Any]] = None,
               topic_id: Optional[int] = None) -> Dict[str, int]:
    """Пройтись по играм без записи и поискать их в VK. Для фонового цикла.

    Нашли новую — сразу оповещаем (если передан bot): запись тем и ценна, что
    её можно посмотреть, а не узнать о ней через неделю из отчёта."""
    out = {"looked": 0, "found": 0, "notified": 0, "skipped": ""}
    # Молчать о том, что не настроено, — плохая идея: снаружи это выглядит как
    # «работает, но ничего не находит», и причину приходится искать вручную.
    if not token():
        out["skipped"] = "нет VK_TOKEN в окружении"
    elif not groups():
        out["skipped"] = "не заданы группы (строка ТИП=вк в «Конфиге» или VK_GROUPS)"
    if out["skipped"]:
        print(f"ℹ️ VK: пропускаю — {out['skipped']}")
        return out
    for g in games_without_video(limit):
        out["looked"] += 1
        link = await find_for_game(g["game_date"], g["home_name"], g["guest_name"])
        if not (link and store(g["source"], g["game_id"], link)):
            continue
        out["found"] += 1
        print(f"📹 VK: {g['home_name']} — {g['guest_name']} ({g['game_date']}): {link}")
        if bot is not None and not is_fresh(g["game_date"]):
            print(f"   игра старше {ANNOUNCE_MAX_AGE_DAYS} дней — ссылку сохранил молча")
        elif bot is not None:
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
