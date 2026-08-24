"""Заглушки Телеграма: нажать кнопку и посмотреть, что вышло, без сети.

Смысл в том, чтобы проверять не «функция считает верно» (это проверяется
обычными тестами), а «кнопка есть — и под ней работает». Ровно этот разрыв и
ловили руками: экран рисуется, а нажатие уводит в корень раздела.

Наружу ничего не уходит: бот подменён, отправки только записываются. Ни одного
сообщения в чат, ни одного запроса в Телеграм.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class FakeUser:
    def __init__(self, uid: int = 111222333, username: str = "tester"):
        self.id = uid
        self.username = username
        self.first_name = "Тест"
        self.last_name = ""
        self.full_name = "Тест"
        self.is_bot = False


class FakeBot:
    """Всё, что «отправлено», оседает здесь списком."""

    def __init__(self):
        self.sent: List[Dict[str, Any]] = []

    async def send_message(self, chat_id=None, text="", **kw):
        self.sent.append({"kind": "message", "chat_id": chat_id, "text": text, **kw})
        return FakeMessage(text=text)

    async def send_document(self, chat_id=None, document=None, **kw):
        self.sent.append({"kind": "document", "chat_id": chat_id, **kw})
        return FakeMessage()

    async def send_poll(self, chat_id=None, question="", options=None, **kw):
        self.sent.append({"kind": "poll", "chat_id": chat_id, "question": question,
                          "options": options or [], **kw})
        return FakeMessage(text=question)

    async def edit_message_text(self, chat_id=None, message_id=None, text="", **kw):
        self.sent.append({"kind": "edit", "chat_id": chat_id, "text": text, **kw})
        return FakeMessage(text=text)

    async def edit_message_reply_markup(self, **kw):
        self.sent.append({"kind": "edit_markup", **kw})

    async def set_chat_menu_button(self, **kw):
        self.sent.append({"kind": "menu_button", **kw})

    async def get_me(self):
        return FakeUser(uid=1, username="testbot")


class FakeMessage:
    def __init__(self, text: str = "", chat_type: str = "private",
                 bot: Optional[FakeBot] = None, user: Optional[FakeUser] = None):
        self.text = text
        self.message_id = 1
        self.chat = FakeChat(chat_type)
        self.chat_id = self.chat.id
        self.from_user = user or FakeUser()
        self._bot = bot or FakeBot()
        self.replies: List[Dict[str, Any]] = []
        # Отправленные файлы держим целиком: выгрузку состава проверяют по
        # содержимому, а не по факту «что-то ушло».
        self.documents: List[Dict[str, Any]] = []

    def get_bot(self):
        return self._bot

    async def reply_text(self, text="", reply_markup=None, **kw):
        self.replies.append({"text": text, "markup": reply_markup})
        return FakeMessage(text=text, bot=self._bot)

    async def reply_document(self, document=None, **kw):
        self.replies.append({"text": "<файл>", "markup": None})
        self.documents.append({"document": document,
                               "filename": kw.get("filename", ""),
                               "caption": kw.get("caption", "")})
        return FakeMessage(bot=self._bot)


class FakeChat:
    def __init__(self, chat_type: str = "private"):
        self.id = 555000
        self.type = chat_type


class FakeQuery:
    """Нажатие на кнопку. Помнит последний показанный экран."""

    def __init__(self, data: str, user: FakeUser, bot: FakeBot):
        self.data = data
        self.from_user = user
        self._bot = bot
        self.message = FakeMessage(bot=bot, user=user)
        self.answers: List[Dict[str, Any]] = []
        self.screens: List[Dict[str, Any]] = []

    def get_bot(self):
        return self._bot

    async def answer(self, text: str = "", show_alert: bool = False, **kw):
        self.answers.append({"text": text, "alert": show_alert})

    async def edit_message_text(self, text: str = "", reply_markup=None, **kw):
        self.screens.append({"text": text, "markup": reply_markup})
        return FakeMessage(text=text, bot=self._bot)

    async def edit_message_reply_markup(self, reply_markup=None, **kw):
        self.screens.append({"text": "", "markup": reply_markup})

    async def delete_message(self, **kw):
        self.screens.append({"text": "<удалено>", "markup": None})


class FakeUpdate:
    def __init__(self, query: Optional[FakeQuery] = None,
                 message: Optional[FakeMessage] = None,
                 user: Optional[FakeUser] = None):
        self.callback_query = query
        self.message = message
        self.effective_message = message or (query.message if query else None)
        self.effective_user = user or (query.from_user if query else
                                       message.from_user if message else None)
        self.effective_chat = (self.effective_message.chat
                               if self.effective_message else None)


class FakeApp:
    def __init__(self, bot: FakeBot):
        self.bot = bot


class FakeContext:
    def __init__(self, bot: FakeBot):
        self.bot = bot
        self.application = FakeApp(bot)
        self.args: List[str] = []
        self.user_data: Dict[str, Any] = {}
        self.chat_data: Dict[str, Any] = {}


def buttons_of(markup) -> List[Any]:
    """Плоский список кнопок разметки."""
    if markup is None or not getattr(markup, "inline_keyboard", None):
        return []
    return [b for row in markup.inline_keyboard for b in row]
