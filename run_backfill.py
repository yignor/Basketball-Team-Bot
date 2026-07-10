#!/usr/bin/env python3
"""
Копирование статистики лиг в локальную базу — разовыми порциями.

    python run_backfill.py --source slpro --scope league --dry-run
    python run_backfill.py --source all --limit 200
    python run_backfill.py --summary

ВАЖНО: запускать от botuser. Трафик остальных пользователей уходит в
AmneziaWG (выход в Варшаве), а basketstat.su и reg.infobasket.su не пускают
иностранные адреса — получите таймаут. Cron уже настроен правильно.

Прогон резюмируемый: скачанные игры помечаются в game_stats_fetched и
повторно не запрашиваются. Потолок --limit существует, чтобы не выкачивать
чужой сервер залпом; остаток доберётся следующей ночью.
"""

import argparse
import asyncio
import logging
import os
import sys

import stats_backfill

# Соревнования Infobasket: сезоны 2023/24, 2024/25, 2025/26.
DEFAULT_IB_COMPS = [73582, 88649, 108009]


def _ib_comps() -> list:
    raw = os.getenv("INFOBASKET_COMP_IDS", "").strip()
    if not raw:
        return DEFAULT_IB_COMPS
    out = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            out.append(int(part))
    return out or DEFAULT_IB_COMPS


def _team_names() -> list:
    raw = os.getenv("SLPRO_TEAM_NAMES", "PullUp Farm,Pull Up Farm")
    return [n.strip() for n in raw.split(",") if n.strip()]


async def main() -> int:
    ap = argparse.ArgumentParser(description="Бэкфилл статистики лиг в локальную базу")
    ap.add_argument("--source", choices=["slpro", "infobasket", "all"], default="all")
    ap.add_argument("--scope", choices=["team", "league", "all"], default="league",
                    help="slpro: team — только наши игры; league — весь текущий сезон; all — все сезоны")
    ap.add_argument("--limit", type=int, default=stats_backfill.DEFAULT_LIMIT,
                    help="максимум игр за прогон (0 — без потолка)")
    ap.add_argument("--delay", type=float, default=stats_backfill.DEFAULT_DELAY,
                    help="пауза между запросами к чужому API, сек")
    ap.add_argument("--dry-run", action="store_true", help="только посчитать, что предстоит скачать")
    ap.add_argument("--summary", action="store_true", help="показать, что уже в локальной копии")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.summary:
        summary = stats_backfill.local_summary()
        if not summary:
            print("Локальная копия пуста.")
            return 0
        print("Локальная копия статистики:")
        for source, s in sorted(summary.items()):
            print(f"  {source:12} игр {s.get('games', 0):>5}  "
                  f"(со счётом {s.get('with_meta', 0):>5})  "
                  f"строк {s.get('rows', 0):>6}  игроков {s.get('players', 0):>5}  "
                  f"период {s.get('first') or '?'} … {s.get('last') or '?'}")
        return 0

    rc = 0
    if args.source in ("slpro", "all"):
        from slpro_client import SlproClient
        st = await stats_backfill.backfill_slpro(
            SlproClient(), scope=args.scope, team_names=_team_names(),
            limit=args.limit, delay=args.delay, dry_run=args.dry_run)
        print(f"SLPRO:      {st}")
        rc |= 1 if st.failed and not st.fetched else 0

    if args.source in ("infobasket", "all"):
        st = await stats_backfill.backfill_infobasket(
            _ib_comps(), limit=args.limit, delay=args.delay, dry_run=args.dry_run)
        print(f"Infobasket: {st}")
        rc |= 1 if st.failed and not st.fetched else 0

    return rc


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
