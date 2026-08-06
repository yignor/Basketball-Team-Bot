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
    "dues_ahead_day": ("За следующий месяц, число", AHEAD_DAY, 1, 28),
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

def due_events(today: Optional[date] = None) -> List[Tuple[str, str, str]]:
    """Что должно сработать сегодня: [(ключ события, период, вид)].

    Вид: coach_end | player_first | coach_warn | player_mid. Ключ уникален на
    (вид, месяц) — по нему bot_daemon помнит, что уже отправил."""
    today = today or date.today()
    out: List[Tuple[str, str, str]] = []
    cur = period_of(today)

    # Перед концом месяца — отчёт тренерам по текущему месяцу.
    if counts(cur) and (month_end(cur) - today).days == day("dues_coach_end"):
        out.append((f"train:{cur}:coach_end", cur, "coach_end"))

    # Начало месяца — напоминание игрокам за начавшийся месяц.
    if today.day == day("dues_first_day") and counts(cur):
        out.append((f"train:{cur}:player_first", cur, "player_first"))

    # Перед повтором — предупреждение тренеру.
    if today.day == day("dues_mid_day") - day("dues_coach_warn") and counts(cur):
        out.append((f"train:{cur}:coach_warn", cur, "coach_warn"))

    # Второе напоминание должникам.
    if today.day == day("dues_mid_day") and counts(cur):
        out.append((f"train:{cur}:player_mid", cur, "player_mid"))

    # 25-е — всем активным напоминание про СЛЕДУЮЩИЙ месяц. Это не про долг:
    # человек ещё ничего не должен, но за зал платят вперёд, и тренеру важно
    # собрать деньги до начала месяца, а не догонять их в середине.
    nxt = next_period(cur)
    if today.day == day("dues_ahead_day") and counts(nxt):
        out.append((f"train:{nxt}:player_ahead", nxt, "player_ahead"))
    return out


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
