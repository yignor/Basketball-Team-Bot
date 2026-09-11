#!/usr/bin/env python3
"""Разбор записи: заметки тренера по тайм-кодам.

    python3 tests/test_video_notes.py

Главное обещание раздела — ссылка из разбора открывает запись ровно на той
секунде, которую тренер назвал, и разбор уходит только ему самому. Голос
распознаётся моделью на сервере; здесь она подменена: проверяем путь от
голосового до сохранённой заметки и то, что файл голоса не остаётся лежать.
"""

from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, List

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fake_tg import (FakeBot, FakeContext, FakeMessage, FakeQuery, FakeUpdate,
                     FakeUser, buttons_of)

TMP = Path(tempfile.mkdtemp(prefix="vnotes-test-")) / "bot.db"

COACH = FakeUser(uid=800200, username="coach")
STRANGER = FakeUser(uid=800201, username="stranger")
BOT = FakeBot()
VIDEO = "https://vk.com/video-123_456"
GAME = ("infobasket", "901234")

bad: List[str] = []


def check(cond: bool, what: str) -> None:
    print(("  ✅ " if cond else "  ❌ ") + what)
    if not cond:
        bad.append(what)


def setup() -> Any:
    os.environ.setdefault("BOT_TOKEN", "0:test")
    os.environ["ADMIN_USER_IDS"] = str(COACH.id)
    os.environ.setdefault("DAEMON_LOG_PATH", str(ROOT / "tests" / "test.log"))
    os.environ["GOOGLE_SHEETS_CREDENTIALS"] = ""
    os.environ["SPREADSHEET_ID"] = ""
    import sheets_cache
    sheets_cache.DB_PATH = TMP
    sheets_cache.init_db()
    now = sheets_cache.now_iso()
    with sheets_cache.get_connection() as conn:
        for row, sur, name in ((2, "Иванов", "Иван"), (3, "Петров", "Пётр"),
                               (4, "Сидоров", "Семён")):
            conn.execute(
                "INSERT INTO players (row_index, surname, name, active_mark, "
                "synced_at) VALUES (?, ?, ?, '1', ?)", (row, sur, name, now))
        conn.execute(
            "INSERT INTO league_teams (source, team_id, name, ours, fetched_at) "
            "VALUES ('infobasket', '707', 'PULL UP', 1, ?)", (now,))
        # Своя игра с записью, своя без записи и чужая с записью.
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, home_team_id, "
            "guest_team_id, home_name, guest_name, video_vk, fetched_at) VALUES "
            "(?, ?, '2026-09-05', '707', '800', 'PULL UP', 'Кураж', ?, ?)",
            (*GAME, VIDEO, now))
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, home_team_id, "
            "guest_team_id, home_name, guest_name, video_vk, fetched_at) VALUES "
            "('infobasket', '901235', '2026-09-06', '707', '801', 'PULL UP', "
            "'Без записи', '', ?)", (now,))
        conn.execute(
            "INSERT INTO game_meta (source, game_id, game_date, home_team_id, "
            "guest_team_id, home_name, guest_name, video_vk, fetched_at) VALUES "
            "('infobasket', '901236', '2026-09-07', '802', '803', 'Чужие', "
            "'Тоже чужие', 'https://vk.com/video-1_2', ?)", (now,))
        conn.commit()
    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None
    return bd


async def press(bd, data: str, who: FakeUser = COACH):
    q = FakeQuery(data, who, BOT)
    await bd.handle_coach_callback(FakeUpdate(query=q, user=who), FakeContext(BOT))
    last = (q.screens or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"], q


async def say(bd, text: str, who: FakeUser = COACH):
    msg = FakeMessage(text=text, bot=BOT, user=who)
    try:
        await bd.handle_note_text(FakeUpdate(message=msg, user=who), FakeContext(BOT))
    except Exception as exc:                     # ApplicationHandlerStop — норма
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    last = (msg.replies or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"]


class FakeFile:
    def __init__(self, seen: List[str]):
        self.seen = seen

    async def download_to_drive(self, path: str):
        Path(path).write_bytes(b"OggS-fake")
        self.seen.append(path)


class FakeVoice:
    def __init__(self, seen: List[str]):
        self.seen = seen

    async def get_file(self):
        return FakeFile(self.seen)


async def speak(bd, seen: List[str], who: FakeUser = COACH):
    msg = FakeMessage(text="", bot=BOT, user=who)
    msg.voice = FakeVoice(seen)
    msg.audio = None
    try:
        await bd.handle_note_voice(FakeUpdate(message=msg, user=who), FakeContext(BOT))
    except Exception as exc:
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    texts = [r["text"] for r in msg.replies]
    last = (msg.replies or [{"text": "", "markup": None}])[-1]
    return texts, last["markup"]


def cbs(markup) -> List[str]:
    return [b.callback_data for b in buttons_of(markup)]


def test_time() -> None:
    print("\n=== тайм-код ===")
    import video_notes as vn
    check(vn.parse_time("12:34") == 754, "«12:34» — 754 с")
    check(vn.parse_time("1:02:15") == 3735, "«1:02:15» — с часами")
    check(vn.parse_time("12.34") == 754, "точка вместо двоеточия — тоже время")
    check(vn.parse_time("12:75") is None, "«12:75» — опечатка, не время")
    check(vn.parse_time("Иванов 12:34") is None, "время только в начале")
    check(vn.split_time("5:07, заслон") == (307, "заслон"), "запятая после времени")
    check(vn.split_time("12:34 — не закрыл") == (754, "не закрыл"), "тире после времени")
    check(vn.human_time(754) == "12:34" and vn.human_time(3735) == "1:02:15",
          "обратно — как в плеере")


def test_games_and_breakdown() -> None:
    print("\n=== игры и разбор ===")
    import video_notes as vn
    games = vn.games_with_video()
    ids = [g["game_id"] for g in games]
    check(ids == [GAME[1]], f"в списке только своя игра с записью: {ids}")

    vn.add(*GAME, 754, "Иванов не закрыл угол", 2, COACH.id)
    vn.add(*GAME, 60, "Хорошо начали: быстрый отрыв", 0, COACH.id)
    vn.add(*GAME, 1500, "Петров <b>поздно</b> в защиту", 3, COACH.id)

    notes = vn.of_game(*GAME)
    check([n["at_sec"] for n in notes] == [60, 754, 1500], "заметки — по порядку записи")

    team = "\n".join(vn.breakdown(*GAME))
    check(f"{VIDEO}?t=12m34s" in team, "ссылка ведёт на 12:34")
    check("Иванов Иван" in team and "Петров Пётр" in team and "команда" in team,
          "в общем разборе у каждой заметки — про кого она")
    check("&lt;b&gt;поздно" in team, "текст тренера экранирован — разметку не ломает")

    ivanov = "\n".join(vn.breakdown(*GAME, 2))
    check("угол" in ivanov and "отрыв" in ivanov, "игроку — его момент и общий")
    check("Петров" not in ivanov, "чужого момента в личном разборе нет")

    many = [vn.add(*GAME, 2000 + i, "длинный комментарий " * 12, 4) for i in range(40)]
    pages = vn.breakdown(*GAME)
    check(len(pages) > 1, f"большой разбор разбит на сообщения: {len(pages)}")
    check(all(len(p) <= 4096 for p in pages), "каждое влезает в сообщение Telegram")
    for note_id in many:
        vn.delete(note_id)


async def test_flow(bd) -> None:
    print("\n=== тренер в боте ===")
    import video_notes as vn
    src, gid = GAME
    text, markup, _ = await press(bd, "coach:play")
    check("coach:vn:list" in cbs(markup), "в «Играх» есть «Разбор записи»")

    text, markup, _ = await press(bd, "coach:vn:list")
    check(f"coach:vn:g:{src}:{gid}" in cbs(markup), "в списке — игра с записью")
    check("3 зам." in text or any("3 зам." in b.text for b in buttons_of(markup)),
          "и сколько по ней заметок")
    check(all(len(c.encode()) <= 64 for c in cbs(markup)), "кнопки в пределах 64 байт")

    text, markup, _ = await press(bd, f"coach:vn:g:{src}:{gid}")
    check("12:34" in text and "Иванов Иван" in text, "заметки игры — со временем и игроком")

    # Всё одним сообщением: время и текст.
    await press(bd, f"coach:vn:add:{src}:{gid}")
    text, markup = await say(bd, "18:05 Сидоров не добежал в отбор")
    check("18:05" in text and "не добежал" in text, "черновик показан до сохранения")
    check(len(vn.of_game(src, gid)) == 3, "до «Сохранить» в базе ничего нового")
    text, markup, _ = await press(bd, "coach:vn:whom")
    check("coach:vn:who:4" in cbs(markup), "в «Кому» — игроки")
    text, markup, _ = await press(bd, "coach:vn:who:4")
    check("Сидоров Семён" in text, "выбрали игрока — он в черновике")
    text, markup, _ = await press(bd, "coach:vn:save")
    saved = [n for n in vn.of_game(src, gid) if n["at_sec"] == 18 * 60 + 5]
    check(len(saved) == 1 and saved[0]["player_row"] == 4, "сохранено про Сидорова")

    # По шагам: время, потом текст.
    await press(bd, f"coach:vn:add:{src}:{gid}")
    text, _ = await say(bd, "за углом")
    check("Не понял время" in text, "без времени — переспрашиваем")
    text, _ = await say(bd, "20:00")
    check("20:00" in text and "голосом или текстом" in text, "время принято — ждём текст")
    text, markup = await say(bd, "Команда держала зону")
    text, markup, _ = await press(bd, "coach:vn:cancel")
    check(not [n for n in vn.of_game(src, gid) if n["at_sec"] == 1200],
          "«Отмена» — ничего не сохранилось")

    # Правка и удаление.
    note = saved[0]
    await press(bd, f"coach:vn:etime:{note['id']}")
    text, _ = await say(bd, "18:10")
    check(vn.get(note["id"])["at_sec"] == 1090, "время заметки поправлено")
    await press(bd, f"coach:vn:etext:{note['id']}")
    await say(bd, "Сидоров не вернулся в защиту")
    check("вернулся" in vn.get(note["id"])["text"], "текст заметки поправлен")
    text, markup, _ = await press(bd, f"coach:vn:ewho:{note['id']}")
    text, markup, _ = await press(bd, "coach:vn:who:0")
    check(vn.get(note["id"])["player_row"] == 0, "«кому» поправлено — теперь команде")
    await press(bd, f"coach:vn:del:{note['id']}")
    check(vn.get(note["id"]) is None, "удалена")

    # Разбор — тренеру, сообщением.
    text, markup, q = await press(bd, f"coach:vn:out:{src}:{gid}:0")
    sent = [r["text"] for r in q.message.replies]
    check(sent and f"{VIDEO}?t=12m34s" in sent[0], "разбор пришёл тренеру со ссылкой")
    check(not BOT.sent or all(m.get("chat_id") in (None, COACH.id) for m in BOT.sent),
          "никому, кроме тренера, бот ничего не отправлял")

    text, markup, _ = await press(bd, f"coach:vn:outp:{src}:{gid}")
    first = buttons_of(markup)[0]
    check(first.text.startswith("📝"), "в выборе игрока первыми — про кого есть заметки")
    text, markup, q = await press(bd, f"coach:vn:out:{src}:{gid}:2")
    sent = "\n".join(r["text"] for r in q.message.replies)
    check("угол" in sent and "Петров" not in sent, "личный разбор — без чужих моментов")


async def test_voice(bd) -> None:
    print("\n=== голосом ===")
    import speech
    import video_notes as vn
    src, gid = GAME

    real = speech.available, speech.transcribe
    got: List[Any] = []

    def fake_transcribe(path, language="ru", hint=""):
        got.append((Path(path).exists(), hint))
        return "Сидоров поздно закрыл трёхочкового", 4.0

    speech.available = lambda: (True, "")
    speech.transcribe = fake_transcribe
    try:
        seen: List[str] = []
        await press(bd, f"coach:vn:add:{src}:{gid}")
        await say(bd, "7:40")
        texts, markup = await speak(bd, seen)
        check(got and got[0][0], "модель получила файл голоса")
        check("Сидоров" in got[0][1] and "заслон" in got[0][1],
              "подсказка: фамилии своих и слова игры")
        check(seen and not Path(seen[0]).exists(), "файл голоса удалён сразу")
        check(any("поздно закрыл" in t and "Распознано по голосу" in t for t in texts),
              "черновик с распознанным текстом и просьбой проверить")
        await press(bd, "coach:vn:save")
        voiced = [n for n in vn.of_game(src, gid) if n["at_sec"] == 460]
        check(len(voiced) == 1 and voiced[0]["by_voice"] == 1, "сохранено с пометкой «голосом»")

        # Голос раньше времени: не теряем, спрашиваем время.
        await press(bd, f"coach:vn:add:{src}:{gid}")
        texts, _ = await speak(bd, seen)
        check(any("А на какой секунде" in t for t in texts), "голос без времени — спросили время")
        text, _ = await say(bd, "33:12")
        check("поздно закрыл" in text and "33:12" in text, "время приложилось к голосу")
        await press(bd, "coach:vn:cancel")
    finally:
        speech.available, speech.transcribe = real

    # Распознавания нет — говорим и не теряем шаг.
    speech.available = lambda: (False, "распознавание голоса не установлено на сервере — "
                                       "пришли комментарий текстом")
    try:
        await press(bd, f"coach:vn:add:{src}:{gid}")
        await say(bd, "9:00")
        texts, _ = await speak(bd, [])
        check(any("текстом" in t for t in texts), "без модели — просим текстом")
        text, _ = await say(bd, "Текстом тоже можно")
        check("Текстом тоже можно" in text, "и текст после этого принимается")
        await press(bd, "coach:vn:cancel")
    finally:
        speech.available = real[0]


async def test_stranger(bd) -> None:
    print("\n=== чужой ===")
    text, markup, q = await press(bd, "coach:vn:list", STRANGER)
    check(q.answers and q.answers[-1]["alert"], "без доступа к разделу тренера — отказ")
    bd._awaiting_note[STRANGER.id] = {"stage": "time", "src": GAME[0], "gid": GAME[1]}
    text, _ = await say(bd, "12:00 что-то", STRANGER)
    check(STRANGER.id not in bd._awaiting_note and not text,
          "текст от не-тренера заметкой не становится")


def test_idle_unload() -> None:
    print("\n=== модель не держим в памяти зря ===")
    import speech
    speech._model, speech._used_at = object(), 1000.0
    check(not speech.unload_if_idle(now=1000.0 + 10), "только что пользовались — держим")
    check(speech.unload_if_idle(now=1000.0 + speech.IDLE_UNLOAD + 1), "простой — выгрузили")
    check(speech._model is None, "памяти больше не занимает")


def main() -> int:
    print(f"База: {TMP}")
    bd = setup()
    test_time()
    test_games_and_breakdown()
    asyncio.run(test_flow(bd))
    asyncio.run(test_voice(bd))
    asyncio.run(test_stranger(bd))
    test_idle_unload()
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("РАЗБОР ЗАПИСИ: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
