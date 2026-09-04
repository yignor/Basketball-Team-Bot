#!/usr/bin/env python3
"""Взнос за турнир целиком: третий вид оплаты рядом с двумя прежними.

    python3 tests/test_season_fees.py

Главное, за чем следим: турнирные деньги не смешиваются с месячными взносами
и оплатой игр; состав собирает тренер; исключение по сумме живёт у человека и
переживает смену базовой цены; напоминания уходят только в личку.
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

TMP = Path(tempfile.mkdtemp(prefix="fees-test-")) / "bot.db"
COACH = FakeUser(uid=910100, username="coach")
BOT = FakeBot()

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
        conn.execute("DELETE FROM players")
        conn.execute("DELETE FROM player_links")
        for i, (sur, name) in enumerate([("Первый", "Игрок"), ("Второй", "Игрок"),
                                         ("Третий", "Игрок")], start=2):
            conn.execute(
                "INSERT INTO players (row_index, surname, name, active_mark, "
                "pay_season, pay_game, synced_at) VALUES (?, ?, ?, '1', "
                "5500, 900, ?)", (i, sur, name, now))
        # Двое запускали бота, третий нет — напоминание обязано это различать.
        for row, uid in ((2, 920002), (3, 920003)):
            conn.execute(
                "INSERT INTO player_links (tg_user_id, username, player_row, "
                "linked_at) VALUES (?, '', ?, ?)", (str(uid), row, now))
        conn.commit()
    import bot_daemon as bd
    bd._get_spreadsheet = lambda: None
    return bd


class FakeApp:
    def __init__(self, bot):
        self.bot = bot


async def press(bd, data: str):
    q = FakeQuery(data, COACH, BOT)
    await bd._fee_admin(q, FakeContext(BOT), COACH, data.split(":"))
    last = (q.screens or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"]


async def say(bd, text: str):
    msg = FakeMessage(text=text, bot=BOT, user=COACH)
    try:
        await bd.handle_fee_text(FakeUpdate(message=msg, user=COACH), FakeContext(BOT))
    except Exception as exc:
        if type(exc).__name__ != "ApplicationHandlerStop":
            raise
    last = (msg.replies or [{"text": "", "markup": None}])[-1]
    return last["text"], last["markup"]


def cbs(markup) -> List[str]:
    return [b.callback_data for b in buttons_of(markup) if b.callback_data]


# ─────────────────────────── сценарии ──────────────────────────────────────


async def test_create_and_fill(bd) -> int:
    print("\n=== сбор заводится ===")
    import season_fees as sf

    text, markup = await press(bd, "coach:fee:list")
    check("Взносы за турнир" in text, "раздел открылся")

    await press(bd, "coach:fee:new")
    text, _ = await say(bd, "Зимний кубок 2027")
    check("Зимний кубок 2027" in text, f"сбор заведён: {text.splitlines()[0]}")
    fee_id = sf.all_fees()[0]["id"]

    text, _ = await press(bd, f"coach:fee:one:{fee_id}")
    check("Сумма не задана" in text, "без суммы прямо сказано, что не ждём")

    await press(bd, f"coach:fee:amount:{fee_id}")
    await say(bd, "7000")
    check(int((sf.fee(fee_id) or {})["amount"]) == 7000, "базовая сумма записана")

    await press(bd, f"coach:fee:due:{fee_id}")
    await say(bd, "30.09.2026")
    check((sf.fee(fee_id) or {})["due_date"] == "2026-09-30", "срок записан")
    return int(fee_id)


async def test_coach_picks_who_pays(bd, fee_id: int) -> None:
    print("\n=== состав собирает тренер ===")
    import season_fees as sf

    check(not sf.status(fee_id), "пока никого не отметили — платить некому")

    _, markup = await press(bd, f"coach:fee:who:{fee_id}:0")
    picks = [c for c in cbs(markup) if c.startswith(f"coach:fee:t:{fee_id}:")]
    check(len(picks) == 3, f"в списке весь лист: {len(picks)}")
    for c in picks[:2]:
        await press(bd, c)
    check(len(sf.member_rows(fee_id)) == 2, "двое платят")

    await press(bd, picks[0])
    check(len(sf.member_rows(fee_id)) == 1, "нажал ещё раз — снял")
    await press(bd, picks[0])

    t = sf.totals(fee_id)
    check(t["need"] == 14000, f"ждём с двоих по 7000: {t}")


async def test_personal_amount_survives(bd, fee_id: int) -> None:
    """Своя сумма живёт у человека и переживает смену базовой цены."""
    print("\n=== исключение по сумме ===")
    import season_fees as sf

    row = sf.member_rows(fee_id)[0]
    sf.set_personal(fee_id, row, 3000)
    mine = [r for r in sf.status(fee_id) if int(r["row"]) == row][0]
    check(mine["need"] == 3000 and mine["own"], "своя сумма применилась")

    sf.update(fee_id, amount=9000)
    mine = [r for r in sf.status(fee_id) if int(r["row"]) == row][0]
    other = [r for r in sf.status(fee_id) if int(r["row"]) != row][0]
    check(mine["need"] == 3000, "договорённость не съехала за базовой ценой")
    check(other["need"] == 9000, "а остальных подвинуло")

    sf.set_personal(fee_id, row, 0)
    mine = [r for r in sf.status(fee_id) if int(r["row"]) == row][0]
    check(mine["need"] == 9000, "ноль возвращает базовую цену")
    sf.update(fee_id, amount=7000)


async def test_payment_is_its_own_kind(bd, fee_id: int) -> None:
    """Турнирные деньги не смешиваются с месячными и с оплатой игр."""
    print("\n=== отдельный вид оплаты ===")
    import coach_payments
    import season_fees as sf
    import training_dues as td

    row = sf.member_rows(fee_id)[0]
    res = sf.mark_paid(fee_id, row, by="test")
    check(not res.get("error"), f"платёж записан: {res.get('error') or 'ок'}")

    mine = [r for r in sf.status(fee_id) if int(r["row"]) == row][0]
    check(mine["ok"] and not mine["debt"], "взнос закрыт")
    check(not any(int(r["row"]) == row for r in sf.debtors(fee_id)),
          "из должников сбора пропал")

    # Главное: месячные взносы этого не заметили.
    period = td.FIRST_PERIOD
    dues = [r for r in td.status(period) if int(r["row"]) == row]
    check(dues and dues[0]["paid"] == 0,
          f"на взносы за тренировки не повлияло: {dues[0]['paid'] if dues else '?'}")

    last = coach_payments.recent_payments(limit=5)
    check(last and last[0]["kind"] == sf.KIND,
          f"вид платежа свой: {last[0]['kind'] if last else '?'}")
    check(last and last[0]["period"] == sf.ref(fee_id),
          "и помечен своим сбором")
    check(coach_payments.KIND_TITLES.get(sf.KIND) == "турнир",
          "в сводках подписан по-человечески")


async def test_reminder_goes_private(bd, fee_id: int) -> None:
    print("\n=== напоминание должникам ===")
    import season_fees as sf

    before = len(BOT.sent)
    note = await bd._fee_remind(FakeApp(BOT), fee_id)
    fresh = BOT.sent[before:]
    owing = sf.debtors(fee_id)
    check(len(fresh) == 1, f"ушло одному оставшемуся должнику: {len(fresh)}")
    if fresh:
        check(str(fresh[0]["chat_id"]) == "920003", "в личку, по его id")
        check("7000" in str(fresh[0]["text"]), "сумма названа")
        check("30.09.2026" in str(fresh[0]["text"]), "и срок")
    check("Отправлено: 1" in note, f"тренеру отчитались: {note}")
    check(not [m for m in fresh if str(m.get("chat_id", "")).startswith("-")],
          "в общий чат ничего не ушло")


async def test_delete_keeps_money(bd) -> None:
    """Удаление сбора не стирает внесённые деньги."""
    print("\n=== удаление сбора ===")
    import coach_payments
    import season_fees as sf

    gone, _ = sf.create("На выброс", 1000)
    sf.toggle(gone, 2)
    sf.mark_paid(gone, 2, by="test")
    was = len(coach_payments.recent_payments(limit=50))
    sf.delete(gone)
    check(not sf.fee(gone), "сбор удалён")
    check(len(coach_payments.recent_payments(limit=50)) == was,
          "а платёж остался: деньги были")


async def test_start_clears_fee_dialog(bd) -> None:
    print("\n=== /start закрывает диалог сбора ===")
    bd._awaiting_fee[COACH.id] = "new"
    bd._clear_pending(COACH.id)
    check(COACH.id not in bd._awaiting_fee, "ожидание ввода снято")


async def run() -> None:
    bd = setup()
    fee_id = await test_create_and_fill(bd)
    await test_coach_picks_who_pays(bd, fee_id)
    await test_personal_amount_survives(bd, fee_id)
    await test_payment_is_its_own_kind(bd, fee_id)
    await test_reminder_goes_private(bd, fee_id)
    await test_delete_keeps_money(bd)
    await test_start_clears_fee_dialog(bd)


def main() -> int:
    print(f"База: {TMP}")
    asyncio.run(run())
    print("\n" + "=" * 60)
    if bad:
        print(f"НЕ ПРОШЛО ({len(bad)}):")
        for b in bad:
            print("  • " + b)
        return 1
    print("ВЗНОСЫ ЗА ТУРНИР: ВСЁ ЗЕЛЁНОЕ")
    return 0


if __name__ == "__main__":
    sys.exit(main())
