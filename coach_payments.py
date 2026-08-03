"""Учёт оплат: разбор СМС от банка, сопоставление с игроком, запись платежа.

Тренеру на телефон приходит СМС о поступлении — он вставляет её текст в бота.
Дальше всё делает бот: достаёт сумму, дату и отправителя, ищет игрока, решает,
игра это или сезон, и кладёт платёж в базу и в листы.

Где что живёт:
  • лист «Игроки», столбцы «Оплата сезона» и «Оплата игры» — сколько игрок
    ДОЛЖЕН. Их ведёт тренер руками, бот только читает.
  • таблица payments (SQLite) — история платежей, источник истины для бота.
  • лист «Логи оплаты» — та же история строчка за строчкой. Пишем туда следом
    за базой; недоступный Google означает отложенную строку (push_pending).
  • лист «Оплаты» — сводка: сколько кто внёс всего и по месяцам, отдельно
    игры. Он собирается заново из базы (build_summary_sheet), править его
    руками бессмысленно — перезапишется.

ФИО в базе не храним ([[legal-data-invariant]]): платёж привязан к номеру
строки в листе «Игроки». Имя отправителя из СМС нужно ровно на время
сопоставления и в базу не попадает — от него остаётся только отпечаток
(sha256): по одному ловим повторную вставку той же СМС, по другому узнаём
человека в следующий раз (payment_senders).
"""

from __future__ import annotations

import hashlib
import logging
import re
import sqlite3
from collections import Counter
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import sheets_cache

logger = logging.getLogger(__name__)

# Сырой журнал платежей — «Логи оплаты». Лист «Оплаты» теперь про сводку:
# сколько внесено всего и по месяцам (build_summary_sheet).
PAYMENTS_LOG_SHEET_NAME = "Логи оплаты"
PAYMENTS_SHEET_NAME = "Оплаты"
PAYMENTS_LOG_HEADER = ["Дата", "Игрок", "Сумма", "Тип", "Игр", "Банк",
                         "Примечание", "Записано"]

# Цена игры по умолчанию — на случай, если столбец «Оплата игры» ещё не
# заполнен. Сумма, кратная цене игры, считается оплатой игр (900 = одна,
# 1800 = две и т.д.), всё остальное — взносом за сезон.
DEFAULT_GAME_PRICE = 900

KIND_GAME = "game"
KIND_SEASON = "season"
KIND_UNKNOWN = ""            # не угадали — спрашиваем тренера
KIND_TITLES = {KIND_GAME: "игра", KIND_SEASON: "сезон"}

# Больше трёх игр одним переводом не бывает — дальше сумма скорее случайно
# кратна цене игры, чем действительно за игры (правило тренера).
MAX_GAMES_PER_PAYMENT = 3


# ─────────────────────────── Разбор СМС ────────────────────────────────────

# Банки пишут сумму по-разному: «900р», «900 RUB», «900,00 ₽», «1 800.00 руб».
# Ловим число с необязательным разделителем тысяч и копейками, а «валютность»
# подтверждаем либо значком рядом, либо словом-маркером перед числом.
_AMOUNT_RE = re.compile(
    r"(?<![\d,.])(\d{1,3}(?:[   ]\d{3})+|\d+)(?:[.,](\d{1,2}))?\s*"
    r"(?:р(?:уб)?\.?|RUB|₽|р\b)", re.IGNORECASE)

# Слова, после которых идёт сумма поступления. Нужны, когда значок валюты не
# написан вовсе («Пополнение 900 от Иван И.»).
_INCOME_WORDS = ("перевод", "поступление", "пополнение", "зачисление",
                 "перечисление", "получен", "зачислен")

# «Баланс», «доступно», «остаток» — тоже суммы, но не наши. Строки с этими
# словами при поиске суммы пропускаем.
_BALANCE_WORDS = ("баланс", "доступно", "остаток", "на счете", "на счёте",
                  "комисси")

# Отправитель: «от Иван И.», «от ИВАНОВ ИВАН», «отправитель: Иван И.».
# Имя бывает и капсом (Сбер шлёт «от ИВАН И.»), поэтому слово — это заглавная
# буква плюс любые буквы, а не обязательно строчные.
_WORD = r"[А-ЯЁ][А-ЯЁа-яё]*\.?"
_SENDER_RE = re.compile(
    rf"(?:от|отправител[ья]\s*:?|плательщик\s*:?)\s+({_WORD}(?:\s+{_WORD}){{0,2}})")

# Запасной разбор, когда слова «от» в СМС нет вовсе (Т-Банк: «900 RUB.
# Иван И. Доступно…»): ищем «Имя + инициал» где угодно в тексте.
_NAME_INITIAL_RE = re.compile(rf"\b([А-ЯЁ][А-ЯЁа-яё]{{2,}}\s+[А-ЯЁ]\.)")

# Слова, которые выглядят как имя, но именем не являются.
_NOT_A_NAME = {
    "перевод", "пополнение", "поступление", "зачисление", "списание", "счет",
    "баланс", "доступно", "остаток", "карта", "карты", "сбп", "банк", "онлайн",
    "оплата", "покупка", "снятие", "комиссия", "вам", "руб", "rub",
}

# Расход, а не приход. Такую СМС проводить нельзя — предупреждаем тренера.
_OUTGOING_WORDS = ("списание", "оплата в", "покупка", "снятие", "выдача",
                   "перевод на карту", "отправлен")

_DATE_RE = re.compile(r"\b(\d{1,2})[.\-/](\d{1,2})(?:[.\-/](\d{2,4}))?\b")

_BANKS = {
    "сбер": "Сбербанк", "sber": "Сбербанк",
    "тинькофф": "Т-Банк", "т-банк": "Т-Банк", "tinkoff": "Т-Банк",
    "альфа": "Альфа-Банк", "alfa": "Альфа-Банк",
    "втб": "ВТБ", "vtb": "ВТБ",
    "райффайзен": "Райффайзен", "озон": "Озон Банк", "яндекс": "Яндекс Пэй",
    "газпромбанк": "Газпромбанк", "росбанк": "Росбанк", "почта банк": "Почта Банк",
    "сбп": "СБП", "открытие": "Открытие", "мтс": "МТС Банк",
}


def _norm(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def parse_sms(text: str) -> Dict[str, Any]:
    """Достаёт из текста СМС сумму, отправителя, дату и банк.

    Ничего не выдумывает: не нашёл сумму — вернёт amount=0, и дальше тренера
    попросят ввести её руками. Отпечаток считаем всегда — по нему ловим
    повторную вставку той же СМС."""
    raw = (text or "").strip()
    out: Dict[str, Any] = {
        "amount": 0, "sender": "", "paid_at": "", "bank": "",
        "outgoing": False,
        "fingerprint": fingerprint(raw), "raw_len": len(raw),
    }
    if not raw:
        return out

    low = _norm(raw)
    for marker, title in _BANKS.items():
        if marker in low:
            out["bank"] = title
            break
    out["outgoing"] = (any(w in low for w in _OUTGOING_WORDS)
                       and not any(w in low for w in _INCOME_WORDS))

    out["amount"] = _find_amount(raw)
    out["sender"] = _find_sender(raw)

    d = _find_date(raw)
    if d:
        out["paid_at"] = d.isoformat()
    return out


def _find_amount(raw: str) -> int:
    """Сумма поступления в рублях (копейки отбрасываем — счёт в рублях).

    Порядок важен: сначала числа рядом со словом-маркером («перевод 900»),
    потом любые «900р» вне строк с балансом. Иначе на «Перевод 900р. Баланс
    12 500р» бот запишет двенадцать тысяч."""
    parts = [p for p in re.split(r"[\n;]+", raw) if p.strip()]
    candidates: List[Tuple[int, int]] = []          # (приоритет, сумма)
    for part in parts:
        low = _norm(part)
        if any(w in low for w in _BALANCE_WORDS):
            # В одной строке может быть и перевод, и баланс — отрезаем хвост
            # начиная с первого «балансового» слова.
            cut = min((low.find(w) for w in _BALANCE_WORDS if w in low), default=len(part))
            part = part[:cut]
            low = _norm(part)
            if not part.strip():
                continue
        has_marker = any(w in low for w in _INCOME_WORDS)
        for m in _AMOUNT_RE.finditer(part):
            value = _to_rub(m.group(1), m.group(2))
            if value:
                candidates.append((0 if has_marker else 1, value))
        if has_marker and not candidates:
            # Валюта не написана вовсе: «Пополнение 900 от Иван И.»
            m = re.search(r"(?<![\d,.])(\d{1,3}(?:[   ]\d{3})+|\d+)"
                          r"(?:[.,](\d{1,2}))?", part)
            if m:
                value = _to_rub(m.group(1), m.group(2))
                if value:
                    candidates.append((2, value))
    if not candidates:
        return 0
    best = min(candidates)[0]
    return max(v for p, v in candidates if p == best)


def _to_rub(whole: str, cents: Optional[str]) -> int:
    try:
        rub = int(re.sub(r"[^\d]", "", whole))
    except ValueError:
        return 0
    if cents and int(cents.ljust(2, "0")) >= 50:
        rub += 1                                    # округляем по-честному
    return rub


def _find_sender(raw: str) -> str:
    m = _SENDER_RE.search(raw)
    if m:
        cleaned = _clean_sender(m.group(1))
        if cleaned:
            return cleaned
    m = _NAME_INITIAL_RE.search(raw)
    if m:
        return _clean_sender(m.group(1))
    return ""


def _clean_sender(name: str) -> str:
    """Убирает хвосты вроде «от Иван И. Баланс» → «Иван И.»."""
    words = []
    for w in (name or "").split():
        bare = _norm(w).strip(".,;:")
        if bare in _NOT_A_NAME or bare == "через":
            break
        words.append(w.strip(",;"))
    return " ".join(words).strip()


def _find_date(raw: str) -> Optional[date]:
    today = date.today()
    for m in _DATE_RE.finditer(raw):
        day, month, year = m.group(1), m.group(2), m.group(3)
        try:
            y = int(year) if year else today.year
            if y < 100:
                y += 2000
            d = date(y, int(month), int(day))
        except ValueError:
            continue
        # Дата из будущего — это не дата платежа, а что-то другое в тексте.
        if d <= today:
            return d
    return None


def fingerprint(text: str) -> str:
    """Отпечаток СМС для защиты от повторного ввода. Хранит хеш, не текст."""
    return hashlib.sha256(_norm(text).encode("utf-8")).hexdigest()[:32]


# ─────────────────────── Игроки и сопоставление ────────────────────────────

def players() -> List[Dict[str, Any]]:
    """Игроки из зеркала листа «Игроки» с требуемыми суммами."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT row_index, surname, name, status, team, pay_season, pay_game, "
            "active_mark FROM players ORDER BY surname, name").fetchall()
    people = [r for r in rows if (r["surname"] or "").strip()
              or (r["name"] or "").strip()]
    # Пока «+» не стоит НИ У КОГО, столбцом просто не пользуются — считаем
    # активными всех. Иначе появление пустого столбца молча выключило бы учёт
    # оплат всей команде. Появился хоть один «+» — правило работает буквально.
    marks = [str(r["active_mark"] or "").strip() for r in people]
    column_in_use = any(marks)
    out = []
    for r, mark in zip(people, marks):
        surname = (r["surname"] or "").strip()
        name = (r["name"] or "").strip()
        out.append({
            "row": int(r["row_index"]),
            "surname": surname,
            "name": name,
            "title": f"{surname} {name}".strip(),
            "status": (r["status"] or "").strip(),
            "team": (r["team"] or "").strip(),
            "pay_season": int(r["pay_season"] or 0),
            "pay_game": int(r["pay_game"] or 0),
            "active_mark": mark,
            "active": (mark == sheets_cache.PLAYERS_ACTIVE_MARK) if column_in_use
                      else _is_active_by_status(r["status"]),
        })
    return out


def _is_active_by_status(status: Any) -> bool:
    """Старый признак — по столбцу «Статус». Остался запасным на время, пока
    «Активность» не заполнена."""
    s = _norm(str(status or ""))
    return not any(w in s for w in ("неактив", "ушел", "архив", "выбыл", "заморож"))


def active_stats() -> Dict[str, int]:
    """Сколько в обойме и сколько временно вне — для экранов и лога."""
    people = players()
    return {"all": len(people),
            "active": sum(1 for p in people if p["active"]),
            "marked": sum(1 for p in people if p["active_mark"])}


def game_price(player: Optional[Dict[str, Any]] = None) -> int:
    """Цена одной игры: у игрока своя, иначе самая частая по команде.

    Тренер может поставить разную цену разным людям (аренда, форма), поэтому
    сначала смотрим строку игрока. Команда целиком — когда игрок ещё не
    опознан: сумму надо как-то классифицировать уже в момент разбора СМС."""
    if player and int(player.get("pay_game") or 0) > 0:
        return int(player["pay_game"])
    prices = [p["pay_game"] for p in players() if p["pay_game"] > 0]
    if prices:
        return Counter(prices).most_common(1)[0][0]
    return DEFAULT_GAME_PRICE


def season_price(player: Optional[Dict[str, Any]] = None) -> int:
    """Взнос за сезон (он же за тренировки): свой у игрока, иначе частый по команде."""
    if player and int(player.get("pay_season") or 0) > 0:
        return int(player["pay_season"])
    prices = [p["pay_season"] for p in players() if p["pay_season"] > 0]
    if prices:
        return Counter(prices).most_common(1)[0][0]
    return 0


def classify(amount: int, player: Optional[Dict[str, Any]] = None) -> Tuple[str, int]:
    """(тип платежа, сколько игр покрывает). KIND_UNKNOWN — когда не понять.

    Правила тренера: сумма, кратная «Оплате игры» и покрывающая не больше
    трёх игр, — это игры; сумма ровно как в «Оплате сезона» — сезон. Всё
    остальное бот не угадывает, а спрашивает: людей, которые переводят
    произвольные суммы, хватает, и молча записать такое не туда — хуже, чем
    задать один вопрос.

    Совпало и то и другое (взнос за сезон равен цене двух игр) — тоже вопрос:
    выбор тут за тренером, а не за ботом."""
    gprice, sprice = game_price(player), season_price(player)
    as_games = (amount > 0 and gprice > 0 and amount % gprice == 0
                and 1 <= amount // gprice <= MAX_GAMES_PER_PAYMENT)
    as_season = amount > 0 and sprice > 0 and amount == sprice
    if as_games and not as_season:
        return KIND_GAME, amount // gprice
    if as_season and not as_games:
        return KIND_SEASON, 0
    return KIND_UNKNOWN, 0


# ────────────────── Кого бот уже узнаёт в СМС ──────────────────────────────

def _sender_key(sender: str) -> str:
    """Отпечаток подписи отправителя. Не имя: в базе лежит только хеш."""
    key = _norm(sender)
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32] if key else ""


def known_sender(sender: str) -> Optional[Dict[str, Any]]:
    """Кому засчитывали платежи с такой подписью раньше."""
    key = _sender_key(sender)
    if not key:
        return None
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT player_row, seen FROM payment_senders WHERE sender_hash = ?",
            (key,)).fetchone()
    if not row:
        return None
    player = player_by_row(int(row["player_row"]))
    if player:
        player = {**player, "seen": int(row["seen"] or 1)}
    return player


def remember_sender(sender: str, player_row: int) -> bool:
    """Запоминает связку «подпись в СМС -> игрок».

    Второй раз бот уже не спрашивает, кто это. Если тренер засчитал платёж
    другому человеку — связка переезжает на него: последнее решение тренера
    всегда важнее прошлого."""
    key = _sender_key(sender)
    if not key:
        return False
    now = _now()
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            """INSERT INTO payment_senders (sender_hash, player_row, seen, first_at, last_at)
               VALUES (?, ?, 1, ?, ?)
               ON CONFLICT(sender_hash) DO UPDATE SET
                   player_row = excluded.player_row,
                   seen = payment_senders.seen + 1,
                   last_at = excluded.last_at""",
            (key, int(player_row), now, now))
        conn.commit()
    return True


def forget_sender(sender: str) -> bool:
    key = _sender_key(sender)
    if not key:
        return False
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute("DELETE FROM payment_senders WHERE sender_hash = ?", (key,))
        conn.commit()
        return cur.rowcount > 0


def known_senders_count() -> int:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return int(conn.execute("SELECT COUNT(*) FROM payment_senders").fetchone()[0])


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def match_player(sender: str) -> List[Dict[str, Any]]:
    """Кандидаты по имени отправителя, лучшие — первыми.

    Банк почти никогда не пишет фамилию целиком: в СМС приходит «Иван И.» —
    имя и первая буква фамилии. Поэтому одного совпадения по фамилии мало,
    основной случай — «имя + инициал»."""
    tokens = [t for t in re.split(r"[^А-Яа-яЁёA-Za-z]+", sender or "") if t]
    if not tokens:
        return []
    norm_tokens = [_norm(t) for t in tokens]
    initials = {t[0] for t in norm_tokens if len(t) == 1}
    words = {t for t in norm_tokens if len(t) > 1}

    scored: List[Tuple[int, Dict[str, Any]]] = []
    for p in players():
        surname, name = _norm(p["surname"]), _norm(p["name"])
        score = 0
        if surname and surname in words:
            score += 10
        if name and name in words:
            score += 6
        # «Иван И.» — имя целиком, фамилия одной буквой.
        if name and name in words and surname and surname[:1] in initials:
            score += 8
        # «И. Иванов» — наоборот.
        if surname and surname in words and name and name[:1] in initials:
            score += 4
        if score and not p["active"]:
            score -= 3
        if score > 0:
            scored.append((score, p))
    scored.sort(key=lambda x: (-x[0], x[1]["title"]))
    best = scored[0][0] if scored else 0
    # Отдаём только равных лидеру: «Иван И.» при двух Иванах — это выбор
    # тренера, а не догадка бота.
    return [p for s, p in scored if s == best]


def player_by_row(row: int) -> Optional[Dict[str, Any]]:
    for p in players():
        if p["row"] == int(row):
            return p
    return None


# ─────────────────────────── Запись платежа ────────────────────────────────

def already_recorded(fp: str) -> Optional[Dict[str, Any]]:
    """Тот же текст СМС уже проводили? Возвращает прошлый платёж."""
    if not fp:
        return None
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        r = conn.execute(
            "SELECT id, player_row, amount, kind, paid_at FROM payments "
            "WHERE fingerprint = ? LIMIT 1", (fp,)).fetchone()
    return dict(r) if r else None


def record(player_row: int, amount: int, kind: str, games: int,
           paid_at: str = "", bank: str = "", note: str = "",
           added_by: str = "", fp: str = "") -> Dict[str, Any]:
    """Кладёт платёж в базу. Возвращает запись (или прошлую, если это повтор)."""
    dup = already_recorded(fp) if fp else None
    if dup:
        dup["duplicate"] = True
        return dup
    if not fp:
        # Ручной ввод: отпечаток от самих реквизитов, чтобы не провести
        # одно и то же дважды подряд по ошибке.
        fp = fingerprint(f"manual|{player_row}|{amount}|{paid_at}|{datetime.now():%Y-%m-%dT%H:%M}")
    paid_at = paid_at or date.today().isoformat()
    now = datetime.now().isoformat(timespec="seconds")
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        try:
            cur = conn.execute(
                """INSERT INTO payments
                   (player_row, amount, kind, games, paid_at, bank, note,
                    added_by, created_at, fingerprint, pushed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
                (int(player_row), int(amount), kind, int(games), paid_at,
                 bank, note, str(added_by), now, fp))
            conn.commit()
        except sqlite3.IntegrityError:
            # Тот же отпечаток уже в базе: двойное нажатие «Записать» или
            # повторно введённая руками та же строка в ту же минуту.
            conn.rollback()
            dup = already_recorded(fp) or {}
            dup["duplicate"] = True
            return dup
        pid = cur.lastrowid
    return {"id": pid, "player_row": int(player_row), "amount": int(amount),
            "kind": kind, "games": int(games), "paid_at": paid_at,
            "bank": bank, "note": note, "duplicate": False}


def delete(payment_id: int) -> bool:
    """Отмена ошибочного платежа. Из листа строка не исчезает — там пометка."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        cur = conn.execute("DELETE FROM payments WHERE id = ?", (int(payment_id),))
        conn.commit()
        return cur.rowcount > 0


def push_pending(spreadsheet, limit: int = 50) -> int:
    """Дописывает в лист «Оплаты» всё, что ещё не выгружено.

    Лист — интерфейс для человека, а не хранилище: база уже содержит платёж,
    и недоступный Google означает лишь отложенную строку, а не потерю денег."""
    if spreadsheet is None:
        return 0
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM payments WHERE pushed = 0 ORDER BY id LIMIT ?",
            (int(limit),)).fetchall()
    if not rows:
        return 0
    by_row = {p["row"]: p for p in players()}
    ws = sheets_cache._get_or_create_ws(spreadsheet, PAYMENTS_LOG_SHEET_NAME,
                                        PAYMENTS_LOG_HEADER)
    lines, ids = [], []
    for r in rows:
        p = by_row.get(int(r["player_row"]))
        lines.append([
            _human_date(r["paid_at"]),
            p["title"] if p else f"строка {r['player_row']}",
            int(r["amount"]),
            KIND_TITLES.get(r["kind"], r["kind"]),
            int(r["games"]) or "",
            r["bank"] or "",
            r["note"] or "",
            _human_date(r["created_at"], with_time=True),
        ])
        ids.append(int(r["id"]))
    ws.append_rows(lines, value_input_option="USER_ENTERED")
    with sheets_cache.get_connection() as conn:
        conn.executemany("UPDATE payments SET pushed = 1 WHERE id = ?",
                         [(i,) for i in ids])
        conn.commit()
    return len(ids)


def ensure_player_columns(spreadsheet) -> List[str]:
    """Заводит в листе «Игроки» столбцы, которые бот только читает.

    «Оплата сезона», «Оплата игры» — суммы ставит тренер. «Активность» — «+»
    у тех, кто в обойме. Сами столбцы должен создать бот: иначе первый же
    разговор про оплату упирается в «а куда это писать». Пишем только
    заголовки и только если их нет."""
    if spreadsheet is None:
        return []
    ws = spreadsheet.worksheet(sheets_cache.PLAYERS_SHEET_NAME)
    header = ws.row_values(1)
    wanted = (sheets_cache.PLAYERS_PAY_SEASON_HEADER,
              sheets_cache.PLAYERS_PAY_GAME_HEADER,
              sheets_cache.PLAYERS_ACTIVE_HEADER)
    missing = [t for t in wanted if t not in header]
    if not missing:
        return []
    # Лист заведён ровно под свои столбцы, и записи в тринадцатый Google
    # отвечает «exceeds grid limits» — сетку надо сначала расширить.
    _grow(ws, 1, len(header) + len(missing))
    added = []
    for title in missing:
        header.append(title)
        ws.update_cell(1, len(header), title)
        added.append(title)
    if sheets_cache.PLAYERS_ACTIVE_HEADER in missing:
        marked = _prefill_active(ws, header.index(sheets_cache.PLAYERS_ACTIVE_HEADER) + 1)
        logger.info("«Активность»: проставлен «+» всем, кто уже в листе (%s)", marked)
    if added:
        logger.info("В листе «Игроки» добавлены столбцы: %s", ", ".join(added))
    return added


def _prefill_active(ws, col: int) -> int:
    """Ставит «+» всем, кто уже есть в листе, в момент создания столбца.

    Пустой столбец означал бы «неактивны все» — то есть ни поздравлений, ни
    учёта оплат у целой команды из-за одного нового заголовка. Поэтому
    фиксируем нынешнее положение дел: сейчас в листе «Игроки» активны все, а
    ушедшие живут на отдельном листе. Тренеру останется снять «+» у тех, кто
    временно вне обоймы."""
    import gspread.utils as gutils
    values = ws.get_all_values()
    last = 0
    for i, row in enumerate(values[1:], start=2):
        if any((c or "").strip() for c in row[:2]):      # есть фамилия или имя
            last = i
    if last < 2:
        return 0
    letter = gutils.rowcol_to_a1(1, col).rstrip("0123456789")
    marks = []
    for i in range(2, last + 1):
        row = values[i - 1]
        filled = any((c or "").strip() for c in row[:2])
        marks.append([sheets_cache.PLAYERS_ACTIVE_MARK if filled else ""])
    ws.update(values=marks, range_name=f"{letter}2:{letter}{last}")
    return sum(1 for m in marks if m[0])


def by_month() -> Dict[int, Dict[str, Dict[str, int]]]:
    """{строка игрока: {'2026-08': {season, games, game_amount, total}}}."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT player_row, substr(paid_at, 1, 7) AS ym, kind,
                      SUM(amount) AS amount, SUM(games) AS games
               FROM payments WHERE paid_at != ''
               GROUP BY player_row, ym, kind""").fetchall()
    out: Dict[int, Dict[str, Dict[str, int]]] = {}
    for r in rows:
        cell = out.setdefault(int(r["player_row"]), {}).setdefault(
            str(r["ym"]), {"season": 0, "games": 0, "game_amount": 0, "total": 0})
        amount = int(r["amount"] or 0)
        cell["total"] += amount
        if r["kind"] == KIND_GAME:
            cell["games"] += int(r["games"] or 0)
            cell["game_amount"] += amount
        else:
            cell["season"] += amount
    return out


def months_seen() -> List[str]:
    """Месяцы от первого платежа до текущего — сплошным рядом, без дыр."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        first = conn.execute(
            "SELECT MIN(substr(paid_at, 1, 7)) FROM payments WHERE paid_at != ''"
        ).fetchone()[0]
    today = date.today()
    if not first:
        return [f"{today:%Y-%m}"]
    y, m = int(first[:4]), int(first[5:7])
    out = []
    while (y, m) <= (today.year, today.month):
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


MONTH_TITLES = ["янв", "фев", "мар", "апр", "май", "июн",
                "июл", "авг", "сен", "окт", "ноя", "дек"]


def _month_title(ym: str) -> str:
    try:
        return f"{MONTH_TITLES[int(ym[5:7]) - 1]} {ym[:4]}"
    except (ValueError, IndexError):
        return ym


def build_summary_sheet(spreadsheet) -> int:
    """Перерисовывает лист «Оплаты»: итоги, деньги по месяцам, игры по месяцам.

    Лист собирается целиком из базы одним batch_update — дописывать по строке
    нельзя: игрок мог появиться, платёж — уехать в другой месяц задним числом.
    Возвращает число строк игроков."""
    if spreadsheet is None:
        return 0
    everyone = players()
    people = [p for p in everyone if p["active"]]
    aside = [p for p in everyone if not p["active"]]
    if not people:
        return 0
    paid, monthly, months = totals(), by_month(), months_seen()
    titles = [_month_title(m) for m in months]

    rows: List[List[Any]] = []
    stamp = datetime.now().strftime("%d.%m.%Y %H:%M")
    rows.append([f"Оплаты — сводка. Собрана ботом {stamp}, править руками "
                 "бесполезно: перезапишется."])
    rows.append([])

    rows.append(["ИТОГО ПО ИГРОКАМ"])
    rows.append(["Игрок", "Всего ₽", "Сезон внёс", "Сезон надо", "Долг ₽",
                 "За игры ₽", "Игр оплачено", "Последний платёж"])
    for p in people:
        t = paid.get(p["row"], {"season": 0, "games": 0, "game_amount": 0, "last": ""})
        debt = max(0, p["pay_season"] - t["season"]) if p["pay_season"] else 0
        rows.append([p["title"], t["season"] + t["game_amount"], t["season"],
                     p["pay_season"] or "", debt or "", t["game_amount"],
                     t["games"] or "", _human_date(t["last"]) if t["last"] else ""])
    rows.append([])

    rows.append(["ДЕНЬГИ ПО МЕСЯЦАМ, ₽"])
    rows.append(["Игрок"] + titles + ["Итого"])
    for p in people:
        cells = [monthly.get(p["row"], {}).get(m, {}).get("total", 0) for m in months]
        rows.append([p["title"]] + [c or "" for c in cells] + [sum(cells) or ""])
    rows.append([])

    rows.append(["ОПЛАЧЕНО ИГР ПО МЕСЯЦАМ, шт."])
    rows.append(["Игрок"] + titles + ["Итого"])
    for p in people:
        cells = [monthly.get(p["row"], {}).get(m, {}).get("games", 0) for m in months]
        rows.append([p["title"]] + [c or "" for c in cells] + [sum(cells) or ""])

    # Кто без «+» — в сводке не считается, но и молча пропадать не должен:
    # иначе «а куда делся человек» превращается в поиск ошибки в боте.
    if aside:
        rows.append([])
        rows.append([f"Временно вне обоймы (нет «+» в «Активности»), "
                     f"в сводку не попали: {', '.join(p['title'] for p in aside)}"])

    width = max(len(r) for r in rows)
    rows = [r + [""] * (width - len(r)) for r in rows]

    ws = sheets_cache._get_or_create_ws(spreadsheet, PAYMENTS_SHEET_NAME,
                                        ["Оплаты"])
    _grow(ws, len(rows) + 10, width)
    ws.clear()
    ws.update(values=rows, range_name="A1")
    return len(people)


def _grow(ws, rows_needed: int, cols_needed: int) -> None:
    """Расширяет сетку листа под нужный размер (Google не пишет за границу)."""
    have_rows = int(getattr(ws, "row_count", 0) or 0)
    have_cols = int(getattr(ws, "col_count", 0) or 0)
    if have_rows and rows_needed > have_rows:
        ws.add_rows(rows_needed - have_rows)
    if have_cols and cols_needed > have_cols:
        ws.add_cols(cols_needed - have_cols)


def plural(n: int, one: str, few: str, many: str) -> str:
    """«1 игра», «2 игры», «5 игр» — вместо «игр(ы)»."""
    n = abs(int(n))
    if n % 10 == 1 and n % 100 != 11:
        return f"{n} {one}"
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return f"{n} {few}"
    return f"{n} {many}"


def games_word(n: int) -> str:
    return plural(n, "игра", "игры", "игр")


def _human_date(iso: str, with_time: bool = False) -> str:
    try:
        dt = datetime.fromisoformat(str(iso))
    except (TypeError, ValueError):
        return str(iso or "")
    return dt.strftime("%d.%m.%Y %H:%M" if with_time else "%d.%m.%Y")


# ─────────────────────────── Кто сколько внёс ──────────────────────────────

def totals() -> Dict[int, Dict[str, int]]:
    """{строка игрока: {season, games, game_amount, last}} — что уже уплачено."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT player_row, kind, SUM(amount) AS amount, SUM(games) AS games,
                      MAX(paid_at) AS last
               FROM payments GROUP BY player_row, kind""").fetchall()
    out: Dict[int, Dict[str, int]] = {}
    for r in rows:
        cur = out.setdefault(int(r["player_row"]),
                             {"season": 0, "games": 0, "game_amount": 0, "last": ""})
        if r["kind"] == KIND_GAME:
            cur["games"] += int(r["games"] or 0)
            cur["game_amount"] += int(r["amount"] or 0)
        else:
            cur["season"] += int(r["amount"] or 0)
        if str(r["last"] or "") > cur["last"]:
            cur["last"] = str(r["last"] or "")
    return out


def balances(only_active: bool = True) -> List[Dict[str, Any]]:
    """Сводка по каждому: сколько должен за сезон, сколько внёс, чего не хватает.

    Игры считаем в штуках, а не в рублях: «оплачено 3 игры» тренеру понятнее,
    чем «2700 ₽», а цена игры у людей может отличаться."""
    paid = totals()
    out = []
    for p in players():
        if only_active and not p["active"]:
            continue
        t = paid.get(p["row"], {"season": 0, "games": 0, "game_amount": 0, "last": ""})
        debt = max(0, p["pay_season"] - t["season"]) if p["pay_season"] else 0
        out.append({**p, "paid_season": t["season"], "paid_games": t["games"],
                    "paid_game_amount": t["game_amount"], "last": t["last"],
                    "debt": debt,
                    "season_done": bool(p["pay_season"] and debt == 0)})
    out.sort(key=lambda x: (-x["debt"], x["title"]))
    return out


def recent(limit: int = 10) -> List[Dict[str, Any]]:
    """Последние платежи — чтобы тренер видел, что запись прошла."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            "SELECT * FROM payments ORDER BY id DESC LIMIT ?", (int(limit),)).fetchall()
    by_row = {p["row"]: p for p in players()}
    out = []
    for r in rows:
        p = by_row.get(int(r["player_row"]))
        out.append({"id": int(r["id"]), "amount": int(r["amount"]),
                    "kind": r["kind"], "games": int(r["games"]),
                    "paid_at": r["paid_at"], "bank": r["bank"] or "",
                    "title": p["title"] if p else f"строка {r['player_row']}"})
    return out
