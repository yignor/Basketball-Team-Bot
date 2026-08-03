"""Учёт оплат: разбор СМС от банка, сопоставление с игроком, запись платежа.

Тренеру на телефон приходит СМС о поступлении — он вставляет её текст в бота.
Дальше всё делает бот: достаёт сумму, дату и отправителя, ищет игрока, решает,
игра это или сезон, и кладёт платёж в базу + в лист «Оплаты».

Где что живёт:
  • лист «Игроки», столбцы «Оплата сезона» и «Оплата игры» — сколько игрок
    ДОЛЖЕН. Их ведёт тренер руками, бот только читает.
  • таблица payments (SQLite) — история платежей, источник истины для бота.
  • лист «Оплаты» — та же история глазами человека. Пишем туда следом за базой;
    если Google недоступен, платёж всё равно записан (см. push_pending).

ФИО в базе не храним ([[legal-data-invariant]]): платёж привязан к номеру
строки в листе «Игроки». Имя отправителя из СМС нужно ровно на время
сопоставления и в базу не попадает — от него остаётся только отпечаток
(sha256), чтобы одну и ту же СМС нельзя было провести дважды.
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

PAYMENTS_SHEET_NAME = "Оплаты"
PAYMENTS_SHEET_HEADER = ["Дата", "Игрок", "Сумма", "Тип", "Игр", "Банк",
                         "Примечание", "Записано"]

# Цена игры по умолчанию — на случай, если столбец «Оплата игры» ещё не
# заполнен. Сумма, кратная цене игры, считается оплатой игр (900 = одна,
# 1800 = две и т.д.), всё остальное — взносом за сезон.
DEFAULT_GAME_PRICE = 900

KIND_GAME = "game"
KIND_SEASON = "season"
KIND_TITLES = {KIND_GAME: "игра", KIND_SEASON: "сезон"}


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
            "SELECT row_index, surname, name, status, team, pay_season, pay_game "
            "FROM players ORDER BY surname, name").fetchall()
    out = []
    for r in rows:
        surname = (r["surname"] or "").strip()
        name = (r["name"] or "").strip()
        if not (surname or name):
            continue
        out.append({
            "row": int(r["row_index"]),
            "surname": surname,
            "name": name,
            "title": f"{surname} {name}".strip(),
            "status": (r["status"] or "").strip(),
            "team": (r["team"] or "").strip(),
            "pay_season": int(r["pay_season"] or 0),
            "pay_game": int(r["pay_game"] or 0),
            "active": _is_active(r["status"]),
        })
    return out


def _is_active(status: Any) -> bool:
    s = _norm(str(status or ""))
    return not any(w in s for w in ("неактив", "ушел", "архив", "выбыл", "заморож"))


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


def classify(amount: int, player: Optional[Dict[str, Any]] = None) -> Tuple[str, int]:
    """(тип платежа, сколько игр покрывает). Кратно цене игры → это игры."""
    price = game_price(player)
    if amount > 0 and price > 0 and amount % price == 0:
        return KIND_GAME, amount // price
    return KIND_SEASON, 0


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
    ws = sheets_cache._get_or_create_ws(spreadsheet, PAYMENTS_SHEET_NAME,
                                        PAYMENTS_SHEET_HEADER)
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


def ensure_payment_columns(spreadsheet) -> List[str]:
    """Заводит в листе «Игроки» столбцы «Оплата сезона» и «Оплата игры».

    Суммы в них ставит тренер, но сами столбцы должен создать бот: иначе
    первый же разговор про оплату упирается в «а куда это писать». Пишем
    только заголовки и только если их нет."""
    if spreadsheet is None:
        return []
    ws = spreadsheet.worksheet(sheets_cache.PLAYERS_SHEET_NAME)
    header = ws.row_values(1)
    added = []
    for title in (sheets_cache.PLAYERS_PAY_SEASON_HEADER,
                  sheets_cache.PLAYERS_PAY_GAME_HEADER):
        if title not in header:
            header.append(title)
            ws.update_cell(1, len(header), title)
            added.append(title)
    if added:
        logger.info("В листе «Игроки» добавлены столбцы: %s", ", ".join(added))
    return added


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
