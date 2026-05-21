#!/usr/bin/env python3
"""Unit tests for BasketStat SLPRO schedule date helpers and HTML parsing."""

import unittest
from datetime import date

from bs4 import BeautifulSoup

from fallback_game_monitor import (
    FallbackGameMonitor,
    basketstat_parse_ru_month,
    basketstat_resolve_day_date,
)

BASKETSTAT_FIXTURE = """
<div class="calendar-container">
  <span class="main_select_selected_text">Май</span>
  <span class="main_select_selected_text">2026</span>
  <div class="day-cell other-month"><span>27</span></div>
  <div class="day-cell">
    <span>24</span>
    <div class="matches-container desktop-only">
      <a class="match" href="/game/4111">
        <span class="team-name">ЦБ</span>
        <span class="match-time">21:20</span>
        <span class="team-name">PUP</span>
      </a>
    </div>
  </div>
  <div class="day-cell">
    <span>28</span>
    <a class="match" href="/game/4112">
      <span class="team-name">PUP</span>
      <span class="match-time">21:10</span>
      <span class="team-name">GeNex</span>
    </a>
  </div>
</div>
"""


class TestBasketstatDateHelpers(unittest.TestCase):
    def test_parse_ru_month(self):
        self.assertEqual(basketstat_parse_ru_month('Май'), 5)
        self.assertEqual(basketstat_parse_ru_month('май'), 5)
        self.assertEqual(basketstat_parse_ru_month('Декабрь'), 12)
        self.assertIsNone(basketstat_parse_ru_month(''))

    def test_resolve_numeric_day(self):
        today = date(2026, 5, 20)
        resolved = basketstat_resolve_day_date('24', 5, 2026, today=today)
        self.assertEqual(resolved, date(2026, 5, 24))

    def test_resolve_today_tomorrow(self):
        today = date(2026, 5, 20)
        self.assertEqual(
            basketstat_resolve_day_date('Сегодня', 5, 2026, today=today),
            today,
        )
        self.assertEqual(
            basketstat_resolve_day_date('Завтра', 5, 2026, today=today),
            date(2026, 5, 21),
        )


class TestBasketstatScheduleParsing(unittest.TestCase):
    def setUp(self):
        self.monitor = FallbackGameMonitor.__new__(FallbackGameMonitor)

        def normalize(text: str) -> str:
            return ''.join(ch for ch in text.lower() if ch.isalnum())

        def find_match(text: str, variants):
            n = normalize(text)
            for v in variants:
                if normalize(v) == n or normalize(v) in n:
                    return v
            return None

        self.monitor._normalize_name_for_search = normalize
        self.monitor._find_matching_variant = find_match

    def _variants(self, name: str):
        return list({'PUP', name, name.lower(), name.upper()})

    def test_parse_fixture_future_games(self):
        soup = BeautifulSoup(BASKETSTAT_FIXTURE, 'html.parser')
        games = self.monitor._parse_basketstat_schedule(
            soup,
            self._variants('PUP'),
            'PUP',
            'https://slpro.basketstat.ru/schedule/2025-2026/SUMC/2/',
        )
        self.assertEqual(len(games), 2)
        dates = {g['date'] for g in games}
        self.assertIn('24.05.2026', dates)
        self.assertIn('28.05.2026', dates)
        by_date = {g['date']: g for g in games}
        self.assertEqual(by_date['24.05.2026']['time'], '21:20')
        self.assertEqual(by_date['24.05.2026']['opponent'], 'ЦБ')
        self.assertEqual(by_date['28.05.2026']['time'], '21:10')
        self.assertEqual(by_date['28.05.2026']['opponent'], 'GeNex')
        self.assertTrue(by_date['28.05.2026']['is_home'])


if __name__ == '__main__':
    unittest.main()
