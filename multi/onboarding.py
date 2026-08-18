"""Мастер подключения команды: пять минут и ни одного идентификатора руками.

Почему это функция №1 продукта. Сегодня подключение выглядит так: заведи бота у
BotFather, создай таблицу Google, открой к ней доступ служебному аккаунту,
заполни лист «Конфиг» — три блока, около двадцати строк, среди них id чата и id
топиков. Этого не сделает ни один тренер. Пока подключение требует такого,
продавать нечего, сколько функций ни перенеси.

Поэтому здесь правило: **человек не вводит ни одного идентификатора**. Чат бот
узнаёт сам — из события «меня добавили в группу». Топик — из того, где написали
команду. Состав — из списка фамилий, который тренер и так держит в заметках.

Логика намеренно без Телеграма: шаги, вопросы и разбор ответов — обычные
функции, которые можно прогнать тестами без сети. Телеграмный слой сверху
тонкий и занимается только «показать вопрос, принять ответ».

Состояние мастера лежит в базе, а не в памяти: подключение делают один раз, и
потерять его на середине из-за перезапуска — худшее первое впечатление.
"""

from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

from . import tenants

# Шаги по порядку. «chat» первым не случайно: пока бот не знает, о каком чате
# речь, спрашивать что-либо бессмысленно.
STEPS = ["chat", "title", "trainings", "dues", "roster", "done"]

DAYS_RU = ["понедельник", "вторник", "среда", "четверг", "пятница",
           "суббота", "воскресенье"]
DAYS_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS onboarding (
    user_id     TEXT PRIMARY KEY,      -- кто подключает: он же станет тренером
    chat_id     TEXT NOT NULL DEFAULT '',
    step        TEXT NOT NULL DEFAULT 'chat',
    data_json   TEXT NOT NULL DEFAULT '{}',
    started_at  TEXT NOT NULL
);
"""


@contextmanager
def _registry() -> Iterator[sqlite3.Connection]:
    tenants.ROOT.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(tenants.registry_path(), timeout=8.0)
    conn.row_factory = sqlite3.Row
    try:
        conn.executescript(SCHEMA)
        yield conn
    finally:
        conn.close()


# ─────────────────────────── состояние ─────────────────────────────────────


def start(user_id: Any, chat_id: Any = "") -> Dict[str, Any]:
    """Начинает подключение. Повторный вызов продолжает начатое."""
    got = state(user_id)
    if got:
        if chat_id and not got["chat_id"]:
            set_chat(user_id, chat_id)
            return state(user_id) or {}
        return got
    with _registry() as conn:
        conn.execute(
            "INSERT INTO onboarding (user_id, chat_id, step, data_json, started_at) "
            "VALUES (?, ?, ?, '{}', ?)",
            (str(user_id), str(chat_id or ""),
             "title" if chat_id else "chat",
             datetime.now(timezone.utc).isoformat(timespec="seconds")))
        conn.commit()
    return state(user_id) or {}


def state(user_id: Any) -> Optional[Dict[str, Any]]:
    with _registry() as conn:
        row = conn.execute("SELECT * FROM onboarding WHERE user_id = ?",
                           (str(user_id),)).fetchone()
    if not row:
        return None
    got = dict(row)
    got["data"] = json.loads(got.pop("data_json") or "{}")
    return got


def _save(user_id: Any, step: str, data: Dict[str, Any]) -> None:
    with _registry() as conn:
        conn.execute(
            "UPDATE onboarding SET step = ?, data_json = ? WHERE user_id = ?",
            (step, json.dumps(data, ensure_ascii=False), str(user_id)))
        conn.commit()


def set_chat(user_id: Any, chat_id: Any, title: str = "") -> Dict[str, Any]:
    """Чат узнали сами — из события «бота добавили в группу».

    Заодно подставляем название группы как название команды: чаще всего оно и
    есть верное, а поправить одним нажатием проще, чем набирать заново."""
    got = state(user_id) or start(user_id)
    data = dict(got["data"])
    if title:
        data.setdefault("suggested_title", title)
    with _registry() as conn:
        conn.execute(
            "UPDATE onboarding SET chat_id = ?, step = ?, data_json = ? "
            "WHERE user_id = ?",
            (str(chat_id), "title", json.dumps(data, ensure_ascii=False),
             str(user_id)))
        conn.commit()
    return state(user_id) or {}


def drop(user_id: Any) -> None:
    with _registry() as conn:
        conn.execute("DELETE FROM onboarding WHERE user_id = ?", (str(user_id),))
        conn.commit()


# ─────────────────────────── разбор ответов ────────────────────────────────


def parse_trainings(text: str) -> Optional[Dict[str, Any]]:
    """«ср и пт 20:30», «понедельник, четверг 19.00» → дни и время.

    Тренер пишет расписание так, как говорит вслух. Требовать формат — значит
    получить на первом же шаге «не понял, попробуйте ещё раз», а это то место,
    где люди бросают подключение."""
    raw = str(text or "").lower().replace("ё", "е")
    days: List[int] = []
    for i, (full, short) in enumerate(zip(DAYS_RU, DAYS_SHORT)):
        # Полное название или сокращение как отдельное слово: «ср», но не
        # «среда» внутри «среди» и не «вт» внутри «автобус».
        if re.search(rf"\b{full[:-1]}", raw) or re.search(rf"\b{short}\b", raw):
            days.append(i)
    m = re.search(r"\b(\d{1,2})[:.](\d{2})\b", raw)
    if not days or not m:
        return None
    hh, mm = int(m.group(1)), int(m.group(2))
    if not (0 <= hh < 24 and 0 <= mm < 60):
        return None
    return {"days": sorted(set(days)), "time": f"{hh:02d}:{mm:02d}"}


def trainings_title(plan: Dict[str, Any]) -> str:
    names = ", ".join(DAYS_RU[d] for d in plan["days"])
    return f"{names} в {plan['time']}"


def parse_dues(text: str) -> Optional[Dict[str, int]]:
    """«5000 и 500», «5000/500», «5000» → взнос за месяц и за игру.

    Одно число — это месяц: за игру платят не все команды, а за зал платят
    почти все."""
    nums = [int(x) for x in re.findall(r"\d+", str(text or ""))][:2]
    if not nums:
        return None
    return {"season": nums[0], "game": nums[1] if len(nums) > 1 else 0}


def parse_roster(text: str) -> List[Dict[str, str]]:
    """Список из заметок тренера → игроки.

    Строка на человека: «Иванов Иван», можно с ником и номером — «Иванов Иван
    @ivanov 7». Порядок «фамилия имя»: так ведут состав, и так же отсортирован
    любой список команды.

    Пустые строки и нумерацию («1.», «1)») пропускаем: список почти всегда
    копируют откуда-то, где он уже пронумерован."""
    out: List[Dict[str, str]] = []
    for line in str(text or "").splitlines():
        line = re.sub(r"^\s*\d+\s*[.)]\s*", "", line).strip()
        if not line:
            continue
        nick = ""
        m = re.search(r"@([A-Za-z0-9_]{3,})", line)
        if m:
            nick = m.group(1)
            line = line.replace(m.group(0), " ")
        number = ""
        m = re.search(r"\b(\d{1,2})\b\s*$", line)
        if m:
            number = m.group(1)
            line = line[:m.start()]
        words = [w for w in re.split(r"[\s,]+", line) if w]
        if not words:
            continue
        out.append({"surname": words[0].strip("-–—"),
                    "name": " ".join(words[1:]).strip(),
                    "username": nick, "number": number})
    return out


# ─────────────────────────── вопросы шагов ─────────────────────────────────


def question(user_id: Any) -> Dict[str, Any]:
    """Что спросить сейчас: {step, text, skip (можно ли пропустить)}."""
    got = state(user_id)
    if not got:
        return {"step": "chat", "text": ASK_CHAT, "skip": False}
    step, data = got["step"], got["data"]
    if step == "chat":
        return {"step": step, "text": ASK_CHAT, "skip": False}
    if step == "title":
        hint = data.get("suggested_title") or ""
        return {"step": step, "skip": bool(hint),
                "text": ASK_TITLE + (f"\n\nПредлагаю: «{hint}» — подойдёт?"
                                     if hint else "")}
    if step == "trainings":
        return {"step": step, "text": ASK_TRAININGS, "skip": True}
    if step == "dues":
        return {"step": step, "text": ASK_DUES, "skip": True}
    if step == "roster":
        return {"step": step, "text": ASK_ROSTER, "skip": True}
    return {"step": "done", "text": "", "skip": False}


ASK_CHAT = (
    "Добавь меня в чат команды и сделай администратором.\n\n"
    "Как только добавишь — я сам пойму, что это за чат, и продолжим здесь. "
    "Ничего вводить не нужно.")

ASK_TITLE = "Как называется команда? Так я буду подписывать сообщения."

ASK_TRAININGS = (
    "Когда тренировки? Напиши как говоришь: «среда и пятница 20:30».\n\n"
    "Я буду сам собирать опрос перед каждой неделей. "
    "Нет постоянного расписания — пропусти этот шаг.")

ASK_DUES = (
    "Сколько стоит месяц занятий и одна игра? Например: «5000 и 500».\n\n"
    "По этим суммам я буду считать долги. Можно пропустить и задать потом.")

ASK_ROSTER = (
    "Пришли список команды — по человеку в строке:\n\n"
    "<code>Иванов Иван\nПетров Пётр @petrov 7</code>\n\n"
    "Ник и номер необязательны. Можно пропустить: игроки добавятся сами, "
    "когда нажмут «Старт», — просто это дольше.")


def accept(user_id: Any, answer: str) -> Dict[str, Any]:
    """Принимает ответ на текущий шаг.

    Возвращает {ok, error, step, done}. Ошибка — это не тупик: текст объясняет,
    как написать, и шаг остаётся тем же."""
    got = state(user_id)
    if not got:
        return {"ok": False, "error": "Подключение не начато.", "step": "chat"}
    step, data = got["step"], dict(got["data"])
    text = str(answer or "").strip()
    skipped = text.lower() in ("пропустить", "потом", "-", "нет")

    if step == "title":
        title = text or data.get("suggested_title", "")
        if len(title) < 2:
            return {"ok": False, "step": step,
                    "error": "Слишком коротко. Напиши название команды."}
        data["title"] = title[:60]
        _save(user_id, "trainings", data)

    elif step == "trainings":
        if not skipped:
            plan = parse_trainings(text)
            if not plan:
                return {"ok": False, "step": step,
                        "error": "Не понял. Нужны дни и время: «среда и "
                                 "пятница 20:30»."}
            data["trainings"] = plan
        _save(user_id, "dues", data)

    elif step == "dues":
        if not skipped:
            dues = parse_dues(text)
            if not dues:
                return {"ok": False, "step": step,
                        "error": "Нужны суммы числом: «5000 и 500» или просто "
                                 "«5000»."}
            data["dues"] = dues
        _save(user_id, "roster", data)

    elif step == "roster":
        if not skipped:
            people = parse_roster(text)
            if not people:
                return {"ok": False, "step": step,
                        "error": "Не разобрал ни одной строки. По человеку в "
                                 "строке: «Иванов Иван»."}
            data["roster"] = people
        _save(user_id, "done", data)

    else:
        return {"ok": False, "step": step, "error": "Этот шаг уже пройден."}

    now = state(user_id) or {}
    return {"ok": True, "step": now.get("step"), "error": "",
            "done": now.get("step") == "done"}


def summary(user_id: Any) -> str:
    """Что получилось — перед тем, как заводить команду."""
    got = state(user_id) or {}
    data = got.get("data", {})
    lines = [f"Команда: {data.get('title', '—')}",
             f"Чат: {'подключён' if got.get('chat_id') else 'не подключён'}"]
    plan = data.get("trainings")
    lines.append("Тренировки: " + (trainings_title(plan) if plan else "не задано"))
    dues = data.get("dues")
    if dues:
        lines.append(f"Взносы: {dues['season']} ₽ в месяц"
                     + (f", {dues['game']} ₽ за игру" if dues["game"] else ""))
    else:
        lines.append("Взносы: не заданы")
    people = data.get("roster") or []
    lines.append(f"В составе: {len(people)}" if people
                 else "Состав: добавится, когда игроки нажмут «Старт»")
    return "\n".join(lines)


def summary_of(team: Dict[str, Any], players: int) -> str:
    """Итог после заведения команды — уже по её базе, а не по черновику.

    Отдельно от summary(): черновик к этому моменту стёрт, и показывать надо
    то, что действительно записалось. Иначе легко пообещать человеку то, чего
    в базе нет."""
    from . import db, schema
    with db.use(team["slug"]):
        got = schema.settings()
    lines = [f"Команда: {team['title']}"]
    days = [d for d in str(got.get("training_days", "")).split(",") if d.strip()]
    if days and got.get("training_time"):
        names = ", ".join(DAYS_RU[int(d)] for d in days)
        lines.append(f"Тренировки: {names} в {got['training_time']}")
    if got.get("dues_season"):
        tail = (f", {got['dues_game']} ₽ за игру"
                if got.get("dues_game") and got["dues_game"] != "0" else "")
        lines.append(f"Взносы: {got['dues_season']} ₽ в месяц{tail}")
    lines.append(f"В составе: {players}" if players
                 else "Состав: добавится, когда игроки нажмут «Старт»")
    return "\n".join(lines)


def finish(user_id: Any) -> Dict[str, Any]:
    """Заводит команду по собранному: реестр, база, настройки, состав."""
    from . import db, schema
    got = state(user_id)
    if not got or got["step"] != "done":
        raise RuntimeError("Мастер ещё не дошёл до конца")
    data = got["data"]
    team = tenants.register(data.get("title") or "Команда",
                            chat_id=got.get("chat_id", ""))
    with db.use(team["slug"]):
        schema.create()
        schema.set_setting("title", team["title"])
        schema.set_setting("coach_id", str(user_id))
        plan = data.get("trainings")
        if plan:
            schema.set_setting("training_days", ",".join(str(d) for d in plan["days"]))
            schema.set_setting("training_time", plan["time"])
        dues = data.get("dues")
        if dues:
            schema.set_setting("dues_season", str(dues["season"]))
            schema.set_setting("dues_game", str(dues["game"]))
        added = schema.add_players(data.get("roster") or [])
    drop(user_id)
    return {"team": team, "players": added}
