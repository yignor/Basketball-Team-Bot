#!/usr/bin/env python3
"""Отправка составов и заявок в лига-бот (SPB Basket).

Связь СТРОГО односторонняя: мы отправляем, оттуда не читаем ничего. Составы,
поставленные в лига-боте, в нашей базе появляться не должны, поэтому здесь нет
и не будет ни одной функции чтения.

Адрес локальный: боты живут на одной машине, и запрос между ними не должен
зависеть ни от туннеля, ни от TLS, ни от Cloudflare. Пустой токен — отправка
молча выключена: это нормальное состояние, а не поломка.

Что важно знать про формат (выяснено по коду принимающей стороны, потому что
разойтись здесь означает молча слать в никуда):

* `game_id` у них ГОЛЫЙ — «4558». У нас та же игра записана как «slpro-4558»,
  и отправленный без обрезки id получил бы `no_game` на каждый состав.
* ссылки должны быть простые — `slpro:707:933`. У нас один человек, играющий
  в двух лигах, склеен в составную ссылку «slpro:..+infobasket:..»; такую там
  не знают и вернут `not_in_pool`. Разбираем и оставляем только slpro.
* все трое обязаны быть из одной команды — это их правило, мы его не
  проверяем: пусть отказ придёт от того, кто владеет правилом.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Свои настройки — не путать с LEAGUE_BOT_TOKEN лига-бота: тот его собственный
# ключ Телеграма, а этот общий секрет только для ручек приёма.
URL = os.getenv("LEAGUE_INGEST_URL", "http://127.0.0.1:8092").rstrip("/")
TIMEOUT = 8                      # секунд; повторов в обработчике нет намеренно
ORIGIN = "pullup"


def token() -> str:
    return (os.getenv("LEAGUE_INGEST_TOKEN") or "").strip()


def enabled() -> bool:
    """Настроена ли отправка. Нет токена — молчим и ничего не шлём."""
    return bool(token())


def bare_game_id(game_id: Any) -> str:
    """«slpro-4558» → «4558». У лига-бота игры записаны голым номером."""
    gid = str(game_id or "")
    return gid.split("-", 1)[1] if gid.startswith("slpro-") else gid


def league_game_id(game_id: Any) -> str:
    """Настоящий id лиги для этой игры. Пусто — такой игры у лиги нет.

    Тренер заводит матч раньше, чем лига кладёт его в расписание, и до тех пор
    id у игры свой («slpro-m2608170821»). Когда игра появляется в расписании,
    монитор узнаёт её и запоминает связку — вот её и спрашиваем. Без этого
    составы на такую игру уезжали бы с придуманным номером и получали
    `no_game`, хотя игра у лиги давно есть (проверено на боевых 19.08.2026)."""
    bare = bare_game_id(game_id)
    if bare.isdigit():
        return bare
    try:
        import game_link
        found = game_link.league_id_of("slpro", str(game_id))
    except Exception as exc:
        logger.warning("лига-бот: связку игры %s не спросил — %s", game_id, exc)
        return ""
    return bare_game_id(found) if found else ""


def is_league_game(game_id: Any) -> bool:
    """Настоящая ли это игра лиги.

    Тренер может завести игру руками — тогда id получает метку «m»
    («slpro-m2608170821»), а у лиговых id голые цифры. Такой игры в зеркале
    соседа нет и быть не может: он вернёт `no_game`, и предупреждение об этом
    падало бы в журнал при каждом сохранении, заслоняя настоящие отказы."""
    bare = bare_game_id(game_id)
    return bool(bare) and bare.isdigit()


def slpro_refs(refs: List[str]) -> List[str]:
    """Только простые ссылки этой лиги, склейка разобрана.

    Порядок сохраняем: по нему человек узнаёт свой состав в чужом интерфейсе."""
    out: List[str] = []
    for ref in refs or []:
        for part in str(ref).split("+"):
            part = part.strip()
            if part.startswith("slpro:") and part not in out:
                out.append(part)
    return out


def nick_of(user: Dict[str, Any]) -> str:
    """Подпись участника — ОДНИМ словом.

    ФИО не отправляем: у нас пишут фамилию вперёд, у Телеграма наоборот, и из
    двух слов принимающая сторона не отличит имя от фамилии — она отбрасывает
    составное значение целиком, и человек остаётся без подписи. Лучше отдать
    username, а если его нет — только имя одним словом."""
    nick = str((user or {}).get("username") or "").strip().lstrip("@")
    if nick:
        return nick.split()[0]
    name = str((user or {}).get("first_name") or "").strip()
    return name.split()[0] if name else ""


def players_from_refs(refs: List[str]) -> List[Dict[str, int]]:
    """Ссылки → [{player_id, team_id}] для заявки тренера."""
    out: List[Dict[str, int]] = []
    seen = set()
    for ref in slpro_refs(refs):
        parts = ref.split(":")
        if len(parts) != 3:
            continue
        try:
            team_id, player_id = int(parts[1]), int(parts[2])
        except ValueError:
            continue
        if player_id in seen:
            continue
        seen.add(player_id)
        out.append({"player_id": player_id, "team_id": team_id})
    return out


async def _post(path: str, body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """POST с общим токеном. None — не отправилось (причина уже в журнале).

    Исключений наружу не выпускаем: отправка копии не должна ронять то, ради
    чего человек нажимал кнопку."""
    if not enabled():
        return None
    import aiohttp
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                    f"{URL}{path}", json=body,
                    headers={"X-Ingest-Token": token()},
                    timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    logger.warning("лига-бот %s: HTTP %s — %s",
                                   path, resp.status, text[:200])
                    return None
                return json.loads(text)
    except Exception as exc:
        logger.warning("лига-бот %s: не отправилось — %s", path, exc)
        return None


def _log_rejections(what: str, answer: Dict[str, Any]) -> None:
    """Причины отказа пишем ЦЕЛИКОМ: они объясняют, почему у человека нет
    очков, и проглоченная причина превращает разбор в гадание."""
    rejected = (answer or {}).get("rejected") or []
    if rejected:
        logger.warning("лига-бот %s: отклонено %d — %s",
                       what, len(rejected),
                       json.dumps(rejected, ensure_ascii=False)[:600])
    accepted = (answer or {}).get("accepted")
    if accepted:
        logger.info("лига-бот %s: принято %s", what, accepted)


async def send_pick(user_id: Any, nick: str, game_id: Any,
                    refs: List[str]) -> Optional[Dict[str, Any]]:
    """Состав участника на игру СЛПРО. Шлём сразу при сохранении.

    Не пачкой к вечеру: принимающая сторона проверяет своё окно и откажет по
    игре, которая уже началась, — и правильно сделает."""
    clean = slpro_refs(refs)
    if not clean:
        return None
    gid = league_game_id(game_id)
    if not gid:
        logger.info("лига-бот: игра %s ещё не появилась в расписании лиги — "
                    "состав не отправляю", game_id)
        return None
    body = {"origin": ORIGIN, "picks": [{
        "user_id": str(user_id), "nick": nick,
        "game_id": gid, "refs": clean}]}
    # Ловим и здесь, а не только внутри _post. Вызывающий — обработчик
    # сохранения состава: человек уже нажал кнопку, и падение отправки копии
    # не должно доехать до него ни при каких обстоятельствах.
    try:
        answer = await _post("/ingest/picks", body)
    except Exception as exc:
        logger.warning("лига-бот: состав не отправлен — %s", exc)
        return None
    if answer:
        _log_rejections("составы", answer)
    return answer


async def send_lineup(game_id: Any, refs: List[str]) -> Optional[Dict[str, Any]]:
    """Заявка тренера на игру. Заменяет прежнюю ЦЕЛИКОМ.

    Пустой список — это «заявку сняли», а не «нечего отправлять»: у лига-бота
    иначе останется висеть вчерашняя."""
    gid = league_game_id(game_id)
    if not gid:
        logger.info("лига-бот: игра %s ещё не в расписании лиги — заявку "
                    "не отправляю", game_id)
        return None
    body = {"origin": ORIGIN, "game_id": gid,
            "players": players_from_refs(refs)}
    try:
        answer = await _post("/ingest/lineup", body)
    except Exception as exc:
        logger.warning("лига-бот: заявка не отправлена — %s", exc)
        return None
    if answer and not answer.get("ok"):
        logger.warning("лига-бот заявка: %s", json.dumps(answer, ensure_ascii=False)[:300])
    elif answer:
        logger.info("лига-бот заявка на игру %s: принято игроков %s",
                    body["game_id"], answer.get("players"))
    return answer


async def ping() -> Tuple[bool, str]:
    """Проверка, что дверь открыта: пустой список составов.

    Ответ {"ok": true, "accepted": 0} — токен сошёлся; 403 — нет."""
    if not enabled():
        return False, "токен не задан — отправка выключена"
    answer = await _post("/ingest/picks", {"origin": ORIGIN, "picks": []})
    if answer is None:
        return False, "нет ответа (см. журнал: адрес, токен или бот не поднят)"
    return bool(answer.get("ok")), json.dumps(answer, ensure_ascii=False)
