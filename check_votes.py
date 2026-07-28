#!/usr/bin/env python3
"""Показать сырые голоса игрока — чтобы сверять отчёт с журналом, а не гадать.

Запуск:  venv/bin/python check_votes.py maxx1one
"""
import sys

import sheets_cache
import attendance_summary as asum


def main() -> int:
    if len(sys.argv) < 2:
        print("Укажи ник или числовой id: check_votes.py maxx1one")
        return 2
    who = sys.argv[1].lstrip("@").lower()
    sheets_cache.init_db()
    with sheets_cache.get_connection() as conn:
        rows = [dict(r) for r in conn.execute(
            """SELECT training_date, vote_text, vote_type, revote_count, updated_at
               FROM attendance
               WHERE lower(username) = ? OR user_id = ?
               ORDER BY training_date""", (who, sys.argv[1]))]
    if not rows:
        print(f"Голосов от {who} в журнале тренировок нет.")
        return 0
    print(f"{'дата':<12} {'ответ':<34} {'тип':<8} {'дни':<10} {'засчитано'}")
    for r in rows:
        days = asum.days_in_text(r["vote_text"])
        names = ",".join(asum.DAYS_SHORT[d] for d in sorted(days)) or "—"
        # Как это трактует отчёт: явка — только когда назван день.
        counted = "приход" if (r["vote_type"] == "PRESENT" and days) else "пропуск"
        rv = f" (менял {r['revote_count']})" if r["revote_count"] else ""
        print(f"{r['training_date']:<12} {str(r['vote_text'])[:33]:<34} "
              f"{r['vote_type']:<8} {names:<10} {counted}{rv}")
    print(f"\nВсего голосов: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
