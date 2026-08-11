"""Телеграмный слой нового бота: свой токен, свой процесс.

Запуск:

    MULTI_BOT_TOKEN=<токен нового бота> python3 -m multi.bot

**Свой токен обязателен.** Если переменной нет или в ней токен боевого бота —
не стартуем вовсе. Это не перестраховка: запустить новый код со старым токеном
значит подключить недоделанного бота к живой команде, и заметят это не по
логам, а по сообщениям в чате.

Слой намеренно тонкий: показать вопрос, принять ответ, позвать логику. Все
решения — в onboarding.py, и они проверяются тестами без сети.
"""

from __future__ import annotations

import html
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from telegram import (InlineKeyboardButton, InlineKeyboardMarkup, Update)
from telegram.constants import ChatType
from telegram.ext import (Application, ChatMemberHandler, CommandHandler,
                          ContextTypes, MessageHandler, filters)

from . import db, membership, onboarding, schema, tenants

log = logging.getLogger("multi")

TOKEN_ENV = "MULTI_BOT_TOKEN"


def _token() -> str:
    """Токен нового бота — и проверка, что это НЕ боевой."""
    token = os.getenv(TOKEN_ENV, "").strip()
    live = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise SystemExit(
            f"Нет {TOKEN_ENV}. Заведи отдельного бота у @BotFather и положи его "
            f"токен в {TOKEN_ENV}: со старым токеном этот код подключится к "
            "живой команде.")
    if live and token == live:
        raise SystemExit(
            f"{TOKEN_ENV} совпадает с боевым BOT_TOKEN. Так делать нельзя: "
            "новый бот начнёт отвечать в чате команды вместо старого.")
    return token


# ─────────────────────────── подключение ───────────────────────────────────


async def on_added(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Бота добавили в группу — отсюда мы и узнаём чат команды.

    Идентификатор чата человек не вводит никогда: он его и не знает, а
    попытка объяснить, где его взять, — то место, где подключение бросают."""
    member = update.my_chat_member
    if not member or member.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    status = member.new_chat_member.status
    if status not in ("member", "administrator"):
        return
    who = member.from_user
    chat = member.chat
    known = tenants.by_chat(chat.id)
    if known:
        await context.bot.send_message(
            chat_id=chat.id,
            text=f"Я уже работаю с командой «{known['title']}». Всё на месте.")
        return
    onboarding.start(who.id, chat_id=chat.id)
    onboarding.set_chat(who.id, chat.id, title=chat.title or "")
    await context.bot.send_message(
        chat_id=chat.id,
        text="Привет! Я помогаю собирать состав, вести посещаемость и не "
             "терять взносы.\n\nЧтобы включиться, допишите мне в личку — "
             "займёт пару минут.")
    try:
        await context.bot.send_message(chat_id=who.id, text=_ask_text(who.id),
                                       reply_markup=_ask_markup(who.id),
                                       parse_mode="HTML")
    except Exception as exc:
        # Личку он мог не открыть — тогда объясняем прямо в группе.
        log.info("не смог написать в личку %s: %s", who.id, exc)
        await context.bot.send_message(
            chat_id=chat.id,
            text="Напишите мне в личку «Старт» — там продолжим настройку.")


def _ask_text(user_id: Any) -> str:
    q = onboarding.question(user_id)
    return q["text"]


def _ask_markup(user_id: Any) -> Optional[InlineKeyboardMarkup]:
    q = onboarding.question(user_id)
    rows: List[List[InlineKeyboardButton]] = []
    got = onboarding.state(user_id) or {}
    hint = (got.get("data") or {}).get("suggested_title")
    if q["step"] == "title" and hint:
        rows.append([InlineKeyboardButton(f"✅ Да, «{hint}»", callback_data="ob:ok")])
    if q.get("skip"):
        rows.append([InlineKeyboardButton("Пропустить", callback_data="ob:skip")])
    return InlineKeyboardMarkup(rows) if rows else None


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/start в личке: продолжаем подключение или показываем команду."""
    user, chat = update.effective_user, update.effective_chat
    if not user or not chat or chat.type != ChatType.PRIVATE:
        return
    if onboarding.state(user.id):
        await update.message.reply_text(_ask_text(user.id),
                                        reply_markup=_ask_markup(user.id),
                                        parse_mode="HTML")
        return
    teams = membership.teams_of(user.id)
    if teams:
        await update.message.reply_text(_teams_text(teams))
        return
    # Человек ниоткуда: либо тренер, который ещё не добавил бота в чат, либо
    # игрок, чей тренер уже добавил, но не вписал его в состав.
    await update.message.reply_text(
        "Привет! Я бот для баскетбольной команды.\n\n"
        "Если вы тренер — добавьте меня в чат команды и сделайте "
        "администратором, дальше я всё спрошу сам.\n\n"
        "Если вы игрок — попросите тренера подключить команду.")


def _teams_text(teams: List[Dict[str, Any]]) -> str:
    if len(teams) == 1:
        t = teams[0]
        role = "тренер" if t["is_coach"] else "игрок"
        return f"Команда «{t['title']}», вы — {role}.\n\nМеню скоро появится."
    names = "\n".join(f"• {t['title']}" + (" (тренер)" if t["is_coach"] else "")
                      for t in teams)
    return ("Вы в нескольких командах:\n\n" + names +
            "\n\nВыбор команды скоро появится.")


async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Ответы мастера. В личке и только пока подключение не закончено."""
    user, chat, msg = update.effective_user, update.effective_chat, update.message
    if not user or not chat or chat.type != ChatType.PRIVATE or not msg:
        return
    if not onboarding.state(user.id):
        return
    await _advance(update, context, msg.text or "")


async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    await query.answer()
    answer = "" if (query.data or "").endswith(":ok") else "пропустить"
    await _advance(update, context, answer, edit=True)


async def _advance(update: Update, context: ContextTypes.DEFAULT_TYPE,
                   answer: str, edit: bool = False) -> None:
    """Общий шаг мастера: принять ответ и показать следующий вопрос."""
    user = update.effective_user
    say = (update.callback_query.edit_message_text if edit and update.callback_query
           else update.effective_message.reply_text)
    res = onboarding.accept(user.id, answer)
    if not res["ok"]:
        await say(res["error"] + "\n\n" + _ask_text(user.id),
                  reply_markup=_ask_markup(user.id), parse_mode="HTML")
        return
    if not res.get("done"):
        await say(_ask_text(user.id), reply_markup=_ask_markup(user.id),
                  parse_mode="HTML")
        return

    made = onboarding.finish(user.id)
    team = made["team"]
    await say(
        "✅ Готово, команда подключена.\n\n" + html.escape(
            onboarding.summary_of(team, made["players"])) +
        "\n\nЧто дальше: я сам соберу опрос на тренировку и буду напоминать "
        "про взносы. Остальное — в меню.", parse_mode="HTML")
    if team.get("chat_id"):
        try:
            await context.bot.send_message(
                chat_id=int(team["chat_id"]),
                text=f"Команда «{team['title']}» подключена. "
                     "Нажмите «Старт» в личке со мной, чтобы я вас узнал.")
        except Exception as exc:
            log.info("не смог написать в чат команды: %s", exc)


async def on_here(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/сюда в нужной теме — так бот узнаёт id топика, не спрашивая его.

    Идентификатор топика человеку взять неоткуда: в интерфейсе Телеграма его
    просто нет. Единственный честный способ — написать команду там, где надо."""
    msg, chat = update.effective_message, update.effective_chat
    if not msg or not chat or chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    team = tenants.by_chat(chat.id)
    if not team:
        await msg.reply_text("Этот чат ещё не подключён к команде.")
        return
    topic = msg.message_thread_id
    with db.use(team["slug"]):
        schema.set_setting("topic_id", str(topic or ""))
    await msg.reply_text("Понял, буду писать сюда." if topic
                         else "Понял, буду писать в общий чат.")


def build() -> Application:
    import bot_factory                      # общий с боевым: только лимиты и таймауты
    builder = (Application.builder()
               .token(_token())
               .connect_timeout(20).read_timeout(20)
               .write_timeout(20).pool_timeout(20)
               .concurrent_updates(True))
    limiter = bot_factory.rate_limiter()
    if limiter is not None:
        builder = builder.rate_limiter(limiter)
    app = builder.build()
    app.add_handler(ChatMemberHandler(on_added, ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_handler(CommandHandler("start", on_start))
    app.add_handler(CommandHandler(["сюда", "here"], on_here))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND, on_text))
    from telegram.ext import CallbackQueryHandler
    app.add_handler(CallbackQueryHandler(on_button, pattern=r"^ob:"))
    return app


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s")
    app = build()
    log.info("Новый бот запущен. Команд в реестре: %d", len(tenants.all_teams()))
    app.run_polling(allowed_updates=Update.ALL_TYPES)
    return 0


if __name__ == "__main__":
    sys.exit(main())
