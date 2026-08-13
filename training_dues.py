"""Взносы за тренировки: кто за какой месяц заплатил и кого пора теребить.

Считаем помесячно, начиная с сентября 2026 (август тренер считает оплаченным).
Взнос за месяц берём из столбца «Оплата сезона» листа «Игроки» — там у
каждого своя сумма. Спрашиваем взнос только с тех, у кого стоит отметка в
«Активности»: остальные временно не тренируются.

Расписание для месяца M (даты — по Москве):
  • за 2 дня до конца M   — тренерам список должников за M;
  • 1-е число M+1         — должникам напоминание, тренерам отбивка о доставке;
  • за 3 дня до середины  — тренеру предупреждение о будущей рассылке;
  • середина M+1          — второе напоминание должникам + отбивка.

Здесь только расчёты и тексты: кому и когда слать, решает bot_daemon, он же
единственный, кто умеет отправлять. Всё тренерское уходит в личку
([[coach-messages-private-only]]).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import coach_payments
import sheets_cache

logger = logging.getLogger(__name__)

# С какого месяца считаем взносы. Всё, что раньше, тренер считает закрытым.
FIRST_PERIOD = "2026-09"

# Даты по умолчанию. Живые значения — в app_settings: их правит тренер из бота
# («🗓 Даты оповещений»), потому что удобный день зависит от того, когда в зале
# берут деньги, а это меняется.
MID_DAY = 15
AHEAD_DAY = 25              # заранее про следующий месяц
FIRST_DAY = 1               # в начале месяца — за начавшийся
COACH_WARN_BEFORE_MID = 3   # тренеру перед повтором
COACH_REPORT_BEFORE_END = 2  # тренеру перед концом месяца

SCHEDULE = {
    "dues_plan_day": ("Тренеру: кому уйдёт вопрос, число", 20, 1, 28),
    "dues_ahead_day": ("Игрокам: вопрос про следующий месяц, число", AHEAD_DAY, 1, 28),
    "dues_first_day": ("За начавшийся месяц, число", FIRST_DAY, 1, 28),
    "dues_mid_day": ("Повтор должникам, число", MID_DAY, 1, 28),
    "dues_coach_warn": ("Тренеру перед повтором, за сколько дней",
                        COACH_WARN_BEFORE_MID, 0, 10),
    "dues_coach_end": ("Тренеру перед концом месяца, за сколько дней",
                       COACH_REPORT_BEFORE_END, 0, 10),
}


def day(key: str) -> int:
    """Настроенное значение даты/сдвига. Без настройки — как было."""
    title, default, low, high = SCHEDULE[key]
    value = sheets_cache.get_int_setting(key, default)
    return max(low, min(high, value))

MONTHS_RU = ["январь", "февраль", "март", "апрель", "май", "июнь", "июль",
             "август", "сентябрь", "октябрь", "ноябрь", "декабрь"]
# Родительный падеж: «считаем с сентября», а не «с сентябрь».
MONTHS_RU_GEN = ["января", "февраля", "марта", "апреля", "мая", "июня", "июля",
                 "августа", "сентября", "октября", "ноября", "декабря"]
# Предложный падеж: «занимаешься в сентябре», а не «в сентябрь».
MONTHS_RU_PRE = ["январе", "феврале", "марте", "апреле", "мае", "июне", "июле",
                 "августе", "сентябре", "октябре", "ноябре", "декабре"]


def period_of(day: date) -> str:
    return f"{day:%Y-%m}"


def month_title(period: str) -> str:
    """'2026-09' -> 'сентябрь 2026'."""
    try:
        return f"{MONTHS_RU[int(period[5:7]) - 1]} {period[:4]}"
    except (ValueError, IndexError):
        return period


def month_title_gen(period: str) -> str:
    """'2026-09' -> 'сентября 2026' — для «считаем с …»."""
    try:
        return f"{MONTHS_RU_GEN[int(period[5:7]) - 1]} {period[:4]}"
    except (ValueError, IndexError):
        return period


def month_title_pre(period: str) -> str:
    """'2026-09' -> 'сентябре 2026' — для «занимаешься в …»."""
    try:
        return f"{MONTHS_RU_PRE[int(period[5:7]) - 1]} {period[:4]}"
    except (ValueError, IndexError):
        return period


def next_period(period: str) -> str:
    y, m = int(period[:4]), int(period[5:7])
    y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return f"{y:04d}-{m:02d}"


def month_end(period: str) -> date:
    nxt = next_period(period)
    return date(int(nxt[:4]), int(nxt[5:7]), 1) - timedelta(days=1)


def counts(period: str) -> bool:
    """Считаем ли взносы за этот месяц (сентябрь 2026 и позже)."""
    return period >= FIRST_PERIOD


def paid_periods(player_row: int) -> List[str]:
    """За какие месяцы у человека есть взнос за тренировки."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT DISTINCT period FROM payments
               WHERE player_row = ? AND kind = ? AND period != ''""",
            (int(player_row), coach_payments.KIND_SEASON)).fetchall()
    return sorted(str(r["period"]) for r in rows)


def _paid_map(period: str) -> Dict[int, int]:
    """{строка игрока: сколько внесено за этот месяц}."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = conn.execute(
            """SELECT player_row, SUM(amount) AS amount FROM payments
               WHERE kind = ? AND period = ? GROUP BY player_row""",
            (coach_payments.KIND_SEASON, period)).fetchall()
    return {int(r["player_row"]): int(r["amount"] or 0) for r in rows}


def status(period: str) -> List[Dict[str, Any]]:
    """Кто сколько внёс за месяц. Только те, с кого взнос ждём."""
    paid = _paid_map(period)
    out = []
    for p in coach_payments.players():
        if not p["pays_season"]:
            continue
        need = int(p["pay_season"] or 0)
        got = paid.get(p["row"], 0)
        out.append({**p, "period": period, "need": need, "paid": got,
                    "debt": max(0, need - got) if need else 0,
                    "ok": bool(need) and got >= need})
    out.sort(key=lambda x: (x["ok"], x["title"]))
    return out


def debtors(period: str) -> List[Dict[str, Any]]:
    """Кто не закрыл месяц. Без проставленной суммы взноса — не должник:
    считать нечего, и дёргать человека не за что."""
    return [r for r in status(period) if r["need"] and not r["ok"]]


def debts_by_month(today: Optional[date] = None) -> List[Dict[str, Any]]:
    """Долги по месяцам: [{period, rows, total}], от старых к новым.

    Непогашенный месяц не растворяется в следующем: наступил октябрь, а
    сентябрь висит — тренер видит оба, каждый со своим списком. Иначе разговор
    получается про «сколько всего», а платят люди за конкретный месяц."""
    today = today or date.today()
    out: List[Dict[str, Any]] = []
    period, cur = FIRST_PERIOD, period_of(today)
    while period <= cur:
        if counts(period):
            rows = debtors(period)
            if rows:
                out.append({"period": period, "rows": rows,
                            "total": sum(int(r["debt"]) for r in rows)})
        period = next_period(period)
    return out


def mark_paid(player_row: int, period: str, by: str = "",
              amount: Optional[int] = None) -> Dict[str, Any]:
    """Тренер отметил: деньги были, чек не присылали.

    Пишем обычный платёж с пометкой by_coach — чтобы в сводке было видно,
    что он появился не из СМС, и чтобы его можно было найти и отменить."""
    player = coach_payments.player_by_row(player_row)
    need = amount if amount is not None else int((player or {}).get("pay_season") or 0)
    rec = coach_payments.record(
        player_row, need, coach_payments.KIND_SEASON, 0,
        paid_at=date.today().isoformat(), bank="", note="отметил тренер",
        added_by=str(by), fp="", period=period, by_coach=True)
    return rec


# ─────────────────────────── Тексты ────────────────────────────────────────

def coach_report(period: str, when: str = "end") -> str:
    """Список должников для тренера. when: end | first | mid | warn."""
    rows = debtors(period)
    title = month_title(period)
    if when == "warn":
        head = (f"⏳ Через 3 дня напомню должникам про {title}.\n\n"
                "Если кто-то уже занёс деньги — отметь, и я его не трону.")
    elif when == "end":
        head = f"📅 {title} заканчивается. Не оплатили тренировки:"
    else:
        head = f"💰 Взносы за {title}. Не оплатили:"
    if not rows:
        return f"✅ {title}: взносы за тренировки внесли все."
    lines = [head, ""]
    for r in rows:
        got = f" (внёс {r['paid']} из {r['need']})" if r["paid"] else ""
        lines.append(f"• {r['title']} — {r['debt']} ₽{got}")
    lines += ["", "Кнопкой ниже можно отметить тех, кто заплатил без чека."]
    return "\n".join(lines)


def player_reminder(row: Dict[str, Any], ahead: bool = False) -> str:
    """Личное напоминание игроку. ahead — про следующий месяц, заранее.

    Заранее и по факту — разные разговоры: в первом случае человек ничего не
    нарушил, и требовать с него нечего."""
    # В строке лежит «need» (сколько ждём) — «price» тут не было никогда, и
    # сумма в напоминании молча не показывалась.
    money = int(row.get("need") or row.get("pay_season") or 0)
    sum_part = f" — {money} ₽" if money else ""
    if ahead:
        return (f"🏋️ Взнос за тренировки на {month_title(row['period'])}"
                f"{sum_part}.\n\n"
                f"Напоминаю заранее: за зал платим вперёд, до начала месяца. "
                f"Реквизиты у тренера.")
    return (f"🏋️ Взнос за тренировки за {month_title(row['period'])}"
            f"{sum_part}.\n\n"
            f"Если уже переводил — просто скажи тренеру, отмечу.")


def plan_text(period: str) -> str:
    """Тренеру 20-го: кому уйдёт вопрос про следующий месяц.

    Смысл предупреждения — дать день на правку. Ушёл человек из команды или
    вернулся, поменялась сумма — поправить надо ДО того, как бот спросит
    двадцать человек, будут ли они заниматься."""
    rows = status(period)
    title = month_title(period)
    if not rows:
        return (f"🗓 {title}: ждать взносы не с кого — в листе «Игроки» ни у "
                "кого не стоит отметка активности.")
    total = sum(int(r["need"] or 0) for r in rows)
    days = day("dues_ahead_day") - day("dues_plan_day")
    lines = [f"🗓 {title}: через "
             f"{coach_payments.plural(days, 'день', 'дня', 'дней')} спрошу у "
             f"{len(rows)} человек, будут ли они заниматься.", "",
             "Вот у кого:"]
    for r in rows:
        lines.append(f"• {r['title']} — {r['need']} ₽")
    lines += ["", f"Всего ожидаем: {total} ₽.", "",
              "Если кто-то уже не в команде — сними отметку активности в листе "
              "«Игроки», и вопрос ему не уйдёт."]
    return "\n".join(lines)


def ask_text(period: str, row: Dict[str, Any], debt: int = 0) -> str:
    """Игроку 25-го: будешь заниматься в следующем месяце?

    Долг за текущий месяц дописываем сюда же, а не шлём вторым сообщением:
    25-е — ещё и день долгового напоминания, и два письма подряд человек
    читает как спам."""
    title = month_title(period)
    lines = [f"🏋️ {title}", "",
             f"Будешь заниматься? Взнос за месяц — {row['need']} ₽."]
    if debt:
        lines += ["", f"И ещё: за {month_title(period_of(date.today()))} "
                      f"за тобой {debt} ₽."]
    lines += ["", "Ответь кнопкой — тренеру нужно понимать, на сколько человек "
              "брать зал."]
    return "\n".join(lines)


def rub(amount: int) -> str:
    """5500 → «5 500 ₽». Неразрывный пробел: сумма не должна рваться переносом."""
    return f"{int(amount):,}".replace(",", " ") + " ₽"


def confirmed_text(period: str, player_row: int) -> str:
    """Ответ человеку, который подтвердил тренировки в следующем месяце.

    Главное здесь — назвать сумму и срок. Человек только что сказал «буду», и
    это единственный момент, когда он думает про следующий месяц: если сумму не
    назвать сейчас, он вспомнит о ней от напоминания через две недели.

    Долг за текущий месяц дописываем и выводим итог. Иначе получается разговор
    в двух валютах: бот просит «5 500 за сентябрь», а тренер ждёт ещё и старое,
    и человек считает сам."""
    player = coach_payments.player_by_row(player_row) or {}
    # Берём сумму из листа как есть, БЕЗ подстановки типовой по команде.
    # Так же считает debtors(): без проставленной суммы человек не должник.
    # Подставить «как у всех» значило бы назвать цифру, которую бот потом сам
    # же не спросит, — и разойтись с тем, что видит тренер.
    need = int(player.get("pay_season") or 0)
    title = month_title(period)
    lines = [f"✅ Записал: занимаешься в {month_title_pre(period)}."]

    if not need:
        # Сумма не проставлена — врать про ноль нельзя.
        lines += ["", "Сколько платить — уточни у тренера: сумма взноса пока "
                      "не проставлена."]
        return "\n".join(lines)

    already = _paid_map(period).get(int(player_row), 0)
    left = max(0, need - already)
    lines += ["", f"Взнос за {title} — {rub(need)}."]
    if already >= need:
        lines += ["", "И он уже оплачен — спасибо, ничего не нужно."]
        return "\n".join(lines)
    if already:
        lines += [f"Из них внесено {rub(already)}, остаётся {rub(left)}."]

    # Долг за текущий месяц — только если этот месяц вообще считается.
    # Без проверки бот сообщал бы долг за август, который тренер объявил
    # оплаченным: status() считает по всем, а не только по месяцам режима.
    cur = period_of(date.today())
    debt = 0
    if counts(cur) and cur != period:
        old = [r for r in debtors(cur) if r["row"] == int(player_row)]
        debt = int(old[0]["debt"]) if old else 0
    if debt:
        lines += ["", f"Плюс за {month_title(cur)} за тобой {rub(debt)}.",
                  f"Итого к оплате: {rub(left + debt)}."]

    lines += ["", "Занеси тренеру до начала месяца — за зал платят вперёд."]
    return "\n".join(lines)


def last_call_text(row: Dict[str, Any], period: str) -> str:
    """Должнику в последний день месяца."""
    return (f"🏋️ {month_title(period)} заканчивается, а взнос "
            f"{row['debt']} ₽ ещё не пришёл.\n\n"
            "Занеси, пожалуйста, тренеру — за зал платят вперёд.")


def delivery_report(period: str, sent: List[str], failed: List[str],
                    unknown: List[str]) -> str:
    """Отбивка тренеру: кому дошло, кому нет и почему."""
    title = month_title(period)
    lines = [f"📨 Напоминание про {title} разослано.", ""]
    lines.append(f"Дошло: {len(sent)}")
    if sent:
        lines.append("  " + ", ".join(sent))
    if failed:
        lines += ["", f"Не дошло — закрыли личку или заблокировали бота: {len(failed)}",
                  "  " + ", ".join(failed)]
    if unknown:
        lines += ["", f"Не написать — не запускали бота: {len(unknown)}",
                  "  " + ", ".join(unknown)]
    if failed or unknown:
        lines += ["", "С этими придётся поговорить лично."]
    return "\n".join(lines)


# ─────────────────────── Что пора сделать сегодня ──────────────────────────

# Цикл взносов (решение пользователя 11.08.2026). Тренер везде узнаёт раньше
# игрока — он должен успеть поправить суммы и активность до того, как бот
# начнёт требовать деньги.
PLAN_DAY = 20        # тренеру: кому уйдёт вопрос про следующий месяц
ASK_DAY = 25         # игрокам: «будешь заниматься?» двумя кнопками
# Пока долг висит. Тренеру чаще, чем игроку: у него список, он с ним работает,
# и лишнее письмо ему не в тягость. Игрока дёргаем реже — иначе напоминание
# превращается в фон, который перестают читать.
DEBT_COACH_DAYS = (4, 8, 12, 16, 20, 24, 28)     # шаг 4 дня
DEBT_PLAYER_DAYS = (5, 10, 15, 20, 25, 30)       # шаг 5 дней


def due_events(today: Optional[date] = None) -> List[Tuple[str, str, str]]:
    """Что должно сработать сегодня: [(ключ события, период, вид)].

    Виды:
      coach_plan   — 20-го: тренеру список тех, кому уйдёт вопрос;
      player_ask   — 25-го: игрокам вопрос про следующий месяц (две кнопки);
      coach_end    — предпоследний день месяца: тренеру долги за месяц;
      player_last  — последний день месяца: должникам сумма и просьба;
      coach_debt   — 4, 9, 14… числа: тренеру, пока долги висят;
      player_debt  — 5, 10, 15… числа: должникам, пока долг не погашен.

    Ключ уникален на (вид, месяц) — по нему bot_daemon помнит, что отправил.
    В один день одной и той же стороне уходит не больше одного сообщения:
    25-е — это и вопрос про следующий месяц, и день долгового напоминания,
    а два письма подряд человек читает как спам. Долг в таком случае
    дописывается в сам вопрос."""
    today = today or date.today()
    out: List[Tuple[str, str, str]] = []
    cur = period_of(today)
    nxt = next_period(cur)
    end = month_end(cur)

    # ── про следующий месяц ──────────────────────────────────────────────
    if today.day == day("dues_plan_day") and counts(nxt):
        out.append((f"train:{nxt}:coach_plan", nxt, "coach_plan"))
    if today.day == day("dues_ahead_day") and counts(nxt):
        out.append((f"train:{nxt}:player_ask", nxt, "player_ask"))

    # ── долги за текущий месяц ───────────────────────────────────────────
    if not counts(cur):
        return out
    if today == end - timedelta(days=1):
        out.append((f"train:{cur}:coach_end", cur, "coach_end"))
    if today == end:
        out.append((f"train:{cur}:player_last", cur, "player_last"))
    if today.day in DEBT_COACH_DAYS:
        out.append((f"train:{cur}:coach_debt:{today.day}", cur, "coach_debt"))
    if today.day in DEBT_PLAYER_DAYS:
        out.append((f"train:{cur}:player_debt:{today.day}", cur, "player_debt"))

    # Не больше одного сообщения в день каждой стороне.
    seen: set = set()
    single: List[Tuple[str, str, str]] = []
    for key, period, kind in out:
        side = "coach" if kind.startswith("coach") else "player"
        if side in seen:
            continue
        seen.add(side)
        single.append((key, period, kind))
    return single


def chat_id_of(player_row: int) -> str:
    """Личный чат игрока: сначала привязка по /start, потом id из листа.

    Пусто — человек бота не запускал, и написать ему нельзя: об этом честно
    говорим тренеру в отбивке, а не делаем вид, что напоминание ушло."""
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        row = conn.execute(
            "SELECT tg_user_id FROM player_links WHERE player_row = ? LIMIT 1",
            (int(player_row),)).fetchone()
        if row and str(row["tg_user_id"] or "").strip():
            return str(row["tg_user_id"]).strip()
        row = conn.execute(
            "SELECT tg_user_id FROM players WHERE row_index = ? LIMIT 1",
            (int(player_row),)).fetchone()
    uid = str((row["tg_user_id"] if row else "") or "").strip()
    return uid if uid.isdigit() else ""


def event_done(key: str) -> bool:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        return bool(conn.execute("SELECT 1 FROM pay_events WHERE event_key = ?",
                                 (key,)).fetchone())


def mark_event(key: str, details: str = "") -> None:
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO pay_events (event_key, sent_at, details) "
            "VALUES (?, ?, ?)",
            (key, datetime.now().isoformat(timespec="seconds"), details))
        conn.commit()
