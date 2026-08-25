#!/usr/bin/env python3
"""
Локальный SQLite-кэш/зеркало данных из Google Sheets.

Это НЕ замена Google Sheets — Sheets остаётся источником истины и местом,
которое видят и редактируют люди. Этот модуль только читает Sheets и
складывает копию в SQLite для быстрого чтения (см. admin_panel.py).

Инвариант: только sync_all() (вызываемый из bot_daemon.py) пишет в bot.db.
Все остальные читатели должны только делать SELECT.

Кэш полностью одноразовый и восстанавливаемый: удаление data/bot.db ничего
не портит, следующая же синхронизация заново наполнит его из Sheets. Поэтому
при переезде на другой сервер файл базы можно не переносить вообще.
"""

import json
import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "data" / "bot.db"

PLAYERS_SHEET_NAME = "Игроки"
# Столбец, куда бот сам вписывает числовой Telegram id при первом входе игрока.
# Колонка «Telegram ID» исторически заполнена @никами, а ник в TG меняется и
# переуступается — доступ держим на числовом id, а этот столбец ещё и наглядно
# показывает, кто уже подключился.
PLAYERS_TG_ID_HEADER = "Числовой TG ID"
# Столбцы фэнтези в листе «Игроки»: стоимость игрока и его уровень (карточка).
# Ведёт тренер — бот только читает, как и всё остальное на этом листе.
PLAYERS_PRICE_HEADER = "Стоимость"
PLAYERS_TIER_HEADER = "Уровень"
# Сколько игрок ДОЛЖЕН заплатить — ведёт тренер в листе «Игроки». Сами платежи
# живут отдельно (лист «Оплаты» и таблица payments): лист «Игроки» — про
# требования, а не про историю.
PLAYERS_PAY_SEASON_HEADER = "Оплата сезона"
PLAYERS_PAY_GAME_HEADER = "Оплата игры"
# «1» в этом столбце — с человека ждём оплату ТРЕНИРОВОК (взнос за сезон).
# Пусто — временно не тренируется, взнос с него не спрашиваем. За игры платят
# по составу на игру, к этой отметке оплата игр отношения не имеет. С днём
# рождения поздравляем всех из листа независимо от отметки.
PLAYERS_ACTIVE_HEADER = "Активность"
# Амплуа: «1»…«5», «Разыгрывающий», «Большой». Ведёт тренер — из бота или в
# листе, бот только читает и пишет по его команде.
PLAYERS_ROLE_HEADER = "Амплуа"
PLAYERS_ACTIVE_MARK = "1"
# «+» стоял в столбце до 03.08.2026 и означал то же самое. Читаем его наравне
# с «1», пока лист не переписан; сам лист бот переписывает при старте.
PLAYERS_ACTIVE_LEGACY = ("+",)


def is_active_mark(value: Any) -> bool:
    mark = str(value or "").strip()
    return mark == PLAYERS_ACTIVE_MARK or mark in PLAYERS_ACTIVE_LEGACY
# Листы-логи: сырые записи, которые ведёт бот. Названы «Логи …», чтобы их не
# путали с отчётами — те строит человек и читает глазами.
ATTEND_SHEET_NAME = "Логи посещаемости"
SERVICE_SHEET_NAME = "Сервисный"
BOT_USERS_SHEET_NAME = "Пользователи бота"
ERRORS_SHEET_NAME = "Ошибки"

# Старые названия листов -> новые. Переименовываем сами при старте: лист
# «Оплаты» стал сводкой по игрокам, а сырые записи ушли в «Логи оплаты», и
# путаница «где отчёт, где лог» стоила бы дороже одной операции в Google.
SHEET_RENAMES = {
    "Посещаемость": ATTEND_SHEET_NAME,
    "Посещаемость игр": "Логи игр",
    # «Оплаты» -> «Логи оплаты» здесь больше нет намеренно: переезд уже
    # состоялся, а имя «Оплаты» с тех пор занято сводкой. Оставь эту строку —
    # и каждый старт будет ругаться «существуют оба, разберись руками», хотя
    # разбираться не с чем.
}

ACTIVITY_TYPES = [
    "ОПРОС_ГОЛОСОВАНИЕ",
    "ОПРОС_ИГРА",
    "АНОНС_ИГРА",
    "РЕЗУЛЬТАТ_ИГРА",
    "ДЕНЬ_РОЖДЕНИЯ",
    "КАЛЕНДАРЬ_ИГРА",
]

PENDING_STATUSES = {"АКТИВЕН", "ОТПРАВЛЯЕТСЯ"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS players (
    row_index     INTEGER PRIMARY KEY,
    surname       TEXT NOT NULL DEFAULT '',
    name          TEXT NOT NULL DEFAULT '',
    nickname      TEXT NOT NULL DEFAULT '',
    telegram_id   TEXT NOT NULL DEFAULT '',
    tg_user_id    TEXT NOT NULL DEFAULT '',   -- числовой TG id из листа (наглядность)
    birthday      TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL DEFAULT '',
    team          TEXT NOT NULL DEFAULT '',
    added_date    TEXT NOT NULL DEFAULT '',
    notes         TEXT NOT NULL DEFAULT '',
    price         INTEGER NOT NULL DEFAULT 0,   -- «Стоимость» из листа (фэнтези)
    tier          TEXT NOT NULL DEFAULT '',     -- «Уровень»: Платина/Золото/…
    pay_season    INTEGER NOT NULL DEFAULT 0,   -- «Оплата сезона»: сколько должен
    pay_game      INTEGER NOT NULL DEFAULT 0,   -- «Оплата игры»: цена одной игры
    active_mark   TEXT NOT NULL DEFAULT '',     -- «Активность»: «+» или пусто
    role          TEXT NOT NULL DEFAULT '',     -- «Амплуа»: 1–5, разыгрывающий, большой
    synced_at     TEXT NOT NULL
);

-- Привязка «строка листа Игроки ↔ числовой Telegram id», закреплённая при
-- первом входе. Ник в Telegram меняется и переуступается, числовой id — нет,
-- поэтому доступ держим на нём. Таблица НЕ подчищается sync_players (он
-- перезаливает players целиком из листа), иначе привязка терялась бы.
CREATE TABLE IF NOT EXISTS player_links (
    tg_user_id  TEXT PRIMARY KEY,
    username    TEXT NOT NULL DEFAULT '',
    player_row  INTEGER NOT NULL,
    linked_at   TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_player_links_row ON player_links(player_row);
CREATE INDEX IF NOT EXISTS idx_players_telegram_id ON players(telegram_id);

-- Кому что открыто: kind = 'team' (прогресс команды) или 'personal' (личная
-- статистика). Выдаётся по @нику (админ знает людей по нику, а не по числовому
-- id), но живёт на числовом id: ник меняется и переуступается, id — нет. Ник
-- тут — окно доверия ровно на один первый вход, как и у привязки игроков.
CREATE TABLE IF NOT EXISTS feature_access (
    kind        TEXT NOT NULL,
    username    TEXT NOT NULL,                  -- без @, нижним регистром
    tg_user_id  TEXT NOT NULL DEFAULT '',       -- заполнится при первом входе
    granted_at  TEXT NOT NULL DEFAULT '',
    granted_by  TEXT NOT NULL DEFAULT '',
    until       TEXT NOT NULL DEFAULT '',       -- ISO-дата; пусто — бессрочно
    PRIMARY KEY (kind, username)
);

CREATE TABLE IF NOT EXISTS attendance (
    tg_poll_id       TEXT NOT NULL,
    user_id          TEXT NOT NULL,
    username         TEXT NOT NULL DEFAULT '',
    first_name       TEXT NOT NULL DEFAULT '',
    last_name        TEXT NOT NULL DEFAULT '',
    vote_text        TEXT NOT NULL DEFAULT '',
    vote_type        TEXT NOT NULL DEFAULT '',
    training_date    TEXT NOT NULL DEFAULT '',
    config_poll_id   TEXT NOT NULL DEFAULT '',
    updated_at       TEXT NOT NULL DEFAULT '',
    revote_count     INTEGER NOT NULL DEFAULT 0,
    row_index        INTEGER NOT NULL,
    synced_at        TEXT NOT NULL,
    dirty            INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tg_poll_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_attendance_training_date ON attendance(training_date);

-- Голоса за игровые опросы ("Готов"/"Нет"/"Тренер") — локально-первична
-- с самого начала (в отличие от attendance, появившейся ещё до перехода
-- на локальную БД), поэтому без config_poll_id/row_index — они там были
-- нужны только для сопоставления со старым Sheets-ориентированным кодом.
CREATE TABLE IF NOT EXISTS game_votes (
    tg_poll_id    TEXT NOT NULL,
    user_id       TEXT NOT NULL,
    username      TEXT NOT NULL DEFAULT '',
    first_name    TEXT NOT NULL DEFAULT '',
    last_name     TEXT NOT NULL DEFAULT '',
    vote_text     TEXT NOT NULL DEFAULT '',
    vote_type     TEXT NOT NULL DEFAULT '',
    game_id       TEXT NOT NULL DEFAULT '',
    game_date     TEXT NOT NULL DEFAULT '',
    updated_at    TEXT NOT NULL DEFAULT '',
    revote_count  INTEGER NOT NULL DEFAULT 0,
    synced_at     TEXT NOT NULL,
    dirty         INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tg_poll_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_game_votes_game_date ON game_votes(game_date);

CREATE TABLE IF NOT EXISTS service_log (
    row_index         INTEGER PRIMARY KEY,
    data_type         TEXT NOT NULL,
    logged_at         TEXT NOT NULL DEFAULT '',
    unique_key        TEXT NOT NULL DEFAULT '',
    status            TEXT NOT NULL DEFAULT '',
    additional_data   TEXT NOT NULL DEFAULT '',
    link              TEXT NOT NULL DEFAULT '',
    comp_id           TEXT NOT NULL DEFAULT '',
    team_id           TEXT NOT NULL DEFAULT '',
    alt_name          TEXT NOT NULL DEFAULT '',
    settings          TEXT NOT NULL DEFAULT '',
    game_id           TEXT NOT NULL DEFAULT '',
    game_date         TEXT NOT NULL DEFAULT '',
    game_time         TEXT NOT NULL DEFAULT '',
    arena             TEXT NOT NULL DEFAULT '',
    team_a_id         TEXT NOT NULL DEFAULT '',
    team_b_id         TEXT NOT NULL DEFAULT '',
    synced_at         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_service_log_type ON service_log(data_type);

CREATE TABLE IF NOT EXISTS sync_meta (
    table_name        TEXT PRIMARY KEY,
    last_success_at   TEXT,
    last_attempt_at   TEXT NOT NULL,
    last_error        TEXT,
    row_count         INTEGER NOT NULL DEFAULT 0
);

-- Эти две таблицы, в отличие от остальных, НЕ зеркала Sheets — бот сам
-- пишет сюда (и дублирует в соответствующие листы, чтобы было видно в
-- таблице). SQLite здесь для мгновенного показа в /admin.

CREATE TABLE IF NOT EXISTS bot_users (
    telegram_id   TEXT PRIMARY KEY,
    username      TEXT NOT NULL DEFAULT '',
    first_name    TEXT NOT NULL DEFAULT '',
    first_seen_at TEXT NOT NULL
);

-- Обратная связь от игроков (бэклог п.12). Пишет бот по команде /feedback,
-- читает админка. Живёт локально: это переписка с админом, не отчётность.
CREATE TABLE IF NOT EXISTS feedback (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     TEXT NOT NULL,
    username    TEXT NOT NULL DEFAULT '',
    name        TEXT NOT NULL DEFAULT '',
    message     TEXT NOT NULL,
    logged_at   TEXT NOT NULL,
    answered    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS errors (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    source        TEXT NOT NULL,
    message       TEXT NOT NULL,
    logged_at     TEXT NOT NULL
);

-- ── Локально-первичные данные (SERVICE_RECORDS_LOCAL_PRIMARY) ──────────────
-- В отличие от players/attendance/service_log (чистые зеркала для чтения),
-- эта таблица — основной рабочий слой для EnhancedDuplicateProtection,
-- когда флаг включён: пишут ~10 cron-скриптов и демон, читают тоже они.
-- Google Sheets становится периодическим экспортом (push_service_records),
-- не источником истины на каждый вызов. См. дизайн в плане:
-- уникальные индексы + INSERT ... ON CONFLICT DO NOTHING/DO UPDATE делают
-- проверку-и-запись одной атомарной операцией — гонка между процессами
-- невозможна структурно, а не "потому что мы аккуратно написали код".

CREATE TABLE IF NOT EXISTS service_records (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    data_type         TEXT NOT NULL,
    unique_key        TEXT NOT NULL,
    logged_at         TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'АКТИВЕН',
    additional_data   TEXT NOT NULL DEFAULT '',
    link              TEXT NOT NULL DEFAULT '',
    comp_id           TEXT NOT NULL DEFAULT '',
    team_id           TEXT NOT NULL DEFAULT '',
    alt_name          TEXT NOT NULL DEFAULT '',
    settings          TEXT NOT NULL DEFAULT '',
    game_id           TEXT NOT NULL DEFAULT '',
    game_date         TEXT NOT NULL DEFAULT '',
    game_time         TEXT NOT NULL DEFAULT '',
    arena             TEXT NOT NULL DEFAULT '',
    team_a_id         TEXT NOT NULL DEFAULT '',
    team_b_id         TEXT NOT NULL DEFAULT '',
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    sheet_row_hint    INTEGER,
    dirty             INTEGER NOT NULL DEFAULT 1,
    deleted           INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_service_records_type_key
    ON service_records(data_type, unique_key) WHERE deleted = 0;
CREATE UNIQUE INDEX IF NOT EXISTS uq_service_records_type_gameid
    ON service_records(data_type, game_id) WHERE deleted = 0 AND game_id != '';
CREATE INDEX IF NOT EXISTS idx_service_records_dirty ON service_records(dirty) WHERE dirty = 1;
CREATE INDEX IF NOT EXISTS idx_service_records_type ON service_records(data_type) WHERE deleted = 0;

-- "Конфиг" — люди правят руками, поэтому только pull (та же схема, что
-- players/attendance): сырые колонки, парсинг остаётся в
-- enhanced_duplicate_protection.py как есть.
-- Колонок ровно столько, сколько занимает самая широкая секция листа: у
-- голосований это 10 (по J), там «ID топика» и «Комментарий». Обрезали до H —
-- топик опроса терялся, и опрос уходил не в свой топик.
CREATE TABLE IF NOT EXISTS config_rows (
    row_index   INTEGER PRIMARY KEY,
    col_a       TEXT NOT NULL DEFAULT '',
    col_b       TEXT NOT NULL DEFAULT '',
    col_c       TEXT NOT NULL DEFAULT '',
    col_d       TEXT NOT NULL DEFAULT '',
    col_e       TEXT NOT NULL DEFAULT '',
    col_f       TEXT NOT NULL DEFAULT '',
    col_g       TEXT NOT NULL DEFAULT '',
    col_h       TEXT NOT NULL DEFAULT '',
    col_i       TEXT NOT NULL DEFAULT '',
    col_j       TEXT NOT NULL DEFAULT '',
    synced_at   TEXT NOT NULL
);

-- ── Фэнтези-лига (бэклог п.3) ─────────────────────────────────────────────
-- Кеш статистики по игроку за игру — основа очков фэнтези. Ключ
-- (source, game_id, player_id): завершённая игра тянется РАЗ и навсегда,
-- раздаётся всем участникам/фичам. По юр-инварианту храним только
-- player_id + номер (display_name НЕ храним, показываем транзитно).
-- source: 'slpro' | 'infobasket'. player_id — строка (у источников разный тип).
CREATE TABLE IF NOT EXISTS game_player_stats (
    source        TEXT NOT NULL,
    game_id       TEXT NOT NULL,
    player_id     TEXT NOT NULL,
    team_id       TEXT NOT NULL DEFAULT '',
    number        TEXT NOT NULL DEFAULT '',
    game_date     TEXT NOT NULL DEFAULT '',   -- ISO YYYY-MM-DD
    season_id     TEXT NOT NULL DEFAULT '',
    stage_id      TEXT NOT NULL DEFAULT '',   -- стадия/дивизион: ею отличаем турниры
    pts           INTEGER NOT NULL DEFAULT 0,
    reb           INTEGER NOT NULL DEFAULT 0,
    reb_off       INTEGER NOT NULL DEFAULT 0,
    reb_def       INTEGER NOT NULL DEFAULT 0,
    ast           INTEGER NOT NULL DEFAULT 0,
    stl           INTEGER NOT NULL DEFAULT 0,
    blk           INTEGER NOT NULL DEFAULT 0,
    tur           INTEGER NOT NULL DEFAULT 0,
    pf            INTEGER NOT NULL DEFAULT 0,
    foul_on       INTEGER NOT NULL DEFAULT 0,   -- фолы, заработанные игроком
    fgm           INTEGER NOT NULL DEFAULT 0,
    fga           INTEGER NOT NULL DEFAULT 0,
    tpm           INTEGER NOT NULL DEFAULT 0,
    tpa           INTEGER NOT NULL DEFAULT 0,
    ftm           INTEGER NOT NULL DEFAULT 0,
    fta           INTEGER NOT NULL DEFAULT 0,
    secs          INTEGER NOT NULL DEFAULT 0,   -- время на площадке, секунды
    plus_minus    INTEGER NOT NULL DEFAULT 0,   -- есть у Инфобаскета; SLPRO не отдаёт
    fetched_at    TEXT NOT NULL,
    PRIMARY KEY (source, game_id, player_id)
);
CREATE INDEX IF NOT EXISTS idx_gps_player ON game_player_stats(source, player_id);
-- «последняя игра игрока» ищется через MAX(game_date) по игроку: без даты в
-- индексе это перебор всех его строк на каждого игрока пула.
CREATE INDEX IF NOT EXISTS idx_gps_player_date ON game_player_stats(source, player_id, game_date);
CREATE INDEX IF NOT EXISTS idx_gps_date ON game_player_stats(game_date);

-- Какие игры уже выкачаны (чтобы не дёргать API повторно; отдельно от
-- game_player_stats, т.к. игра могла быть без нашей команды/пустой).
CREATE TABLE IF NOT EXISTS game_stats_fetched (
    source        TEXT NOT NULL,
    game_id       TEXT NOT NULL,
    game_date     TEXT NOT NULL DEFAULT '',
    fetched_at    TEXT NOT NULL,
    PRIMARY KEY (source, game_id)
);

-- Матч целиком: счёт, соперники, стадия. Нужен для командной аналитики и
-- чтобы не ходить в чужой API за тем, что уже скачано.
-- Юр-инвариант: только id команд, без названий и без ФИО.
CREATE TABLE IF NOT EXISTS game_meta (
    source          TEXT NOT NULL,
    game_id         TEXT NOT NULL,
    game_date       TEXT NOT NULL DEFAULT '',   -- ISO YYYY-MM-DD
    game_time       TEXT NOT NULL DEFAULT '',
    season_id       TEXT NOT NULL DEFAULT '',
    stage_id        TEXT NOT NULL DEFAULT '',
    home_team_id    TEXT NOT NULL DEFAULT '',
    guest_team_id   TEXT NOT NULL DEFAULT '',
    home_name       TEXT NOT NULL DEFAULT '',   -- транзитно из лиги, для отчётов
    guest_name      TEXT NOT NULL DEFAULT '',
    home_score      INTEGER NOT NULL DEFAULT 0,
    guest_score     INTEGER NOT NULL DEFAULT 0,
    quarters_json   TEXT NOT NULL DEFAULT '',   -- [[home, guest], ...] по периодам
    arena           TEXT NOT NULL DEFAULT '',
    video_vk        TEXT NOT NULL DEFAULT '',
    -- Когда ВК начал трансляцию (unix) и сколько она идёт. Это точка отсчёта
    -- тайм-кодов: зная её и время спорного из протокола, положение матча в
    -- записи считается вычитанием, а не догадкой про расписание.
    video_started_at INTEGER NOT NULL DEFAULT 0,
    video_seconds    INTEGER NOT NULL DEFAULT 0,
    fetched_at      TEXT NOT NULL,
    PRIMARY KEY (source, game_id)
);
CREATE INDEX IF NOT EXISTS idx_game_meta_date ON game_meta(game_date);
CREATE INDEX IF NOT EXISTS idx_game_meta_teams ON game_meta(home_team_id, guest_team_id);

CREATE TABLE IF NOT EXISTS fantasy_seasons (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    name          TEXT NOT NULL DEFAULT '',
    format        TEXT NOT NULL DEFAULT '3x3',
    status        TEXT NOT NULL DEFAULT 'active',   -- active | ended
    scoring_json  TEXT NOT NULL DEFAULT '',
    settings_json TEXT NOT NULL DEFAULT '',         -- правила сезона (max_per_player и т.п.)
    started_at    TEXT NOT NULL DEFAULT '',
    ended_at      TEXT NOT NULL DEFAULT ''
);

-- Состав участника на конкретную игровую неделю (история по неделям).
-- player_refs_json — список выбранных игроков пула (ключи вида
-- "slpro:707:XXXX" / "ib:36502:XXXXXX" — источник:team:player_id).
CREATE TABLE IF NOT EXISTS fantasy_rosters (
    user_id          TEXT NOT NULL,
    season_id        INTEGER NOT NULL,
    week_start       TEXT NOT NULL,             -- ISO дата понедельника недели
    player_refs_json TEXT NOT NULL DEFAULT '[]',
    mode             TEXT NOT NULL DEFAULT '',   -- режим сбора (fantasy_modes)
    meta_json        TEXT NOT NULL DEFAULT '',   -- разметка режима (категории)
    locked           INTEGER NOT NULL DEFAULT 0,
    updated_at       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (user_id, season_id, week_start)
);
CREATE INDEX IF NOT EXISTS idx_fantasy_rosters_week ON fantasy_rosters(season_id, week_start);

-- Ставка на КОНКРЕТНУЮ игру. Недельный состав (fantasy_rosters) остаётся
-- запасным: нет ставки на игру — очки считаются по нему, иначе переход на
-- поигровую модель обнулил бы всех посреди сезона.
--
-- Ключ — игра, а не неделя: на два матча одной недели ставят по-разному, и
-- заявка тренера у них разная.
CREATE TABLE IF NOT EXISTS fantasy_game_picks (
    user_id          TEXT NOT NULL,
    season_id        INTEGER NOT NULL,
    source           TEXT NOT NULL,
    game_id          TEXT NOT NULL,
    player_refs_json TEXT NOT NULL DEFAULT '[]',
    mode             TEXT NOT NULL DEFAULT '',
    meta_json        TEXT NOT NULL DEFAULT '',
    updated_at       TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (user_id, season_id, source, game_id)
);
CREATE INDEX IF NOT EXISTS idx_fantasy_game_picks_game
    ON fantasy_game_picks(season_id, source, game_id);

CREATE TABLE IF NOT EXISTS fantasy_weekly_scores (
    user_id       TEXT NOT NULL,
    season_id     INTEGER NOT NULL,
    week_start    TEXT NOT NULL,
    points        REAL NOT NULL DEFAULT 0,
    computed_at   TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (user_id, season_id, week_start)
);
CREATE INDEX IF NOT EXISTS idx_fantasy_scores_season ON fantasy_weekly_scores(season_id);

-- Очки участника за КОНКРЕТНУЮ игру, зафиксированные в момент её результата.
-- Состав теперь размораживается после каждой игры, поэтому пересчитывать неделю
-- «текущим» составом нельзя: игрок увидел бы результат, переставил людей и
-- получил их очки задним числом. Здесь лежит снимок: чей состав и сколько ему
-- принесла именно эта игра. refs_json — состав на момент игры (для разбора
-- спорных случаев).
CREATE TABLE IF NOT EXISTS fantasy_game_scores (
    user_id      TEXT NOT NULL,
    season_id    INTEGER NOT NULL,
    source       TEXT NOT NULL,
    game_id      TEXT NOT NULL,
    game_date    TEXT NOT NULL DEFAULT '',
    points       REAL NOT NULL DEFAULT 0,
    mode         TEXT NOT NULL DEFAULT '',     -- режим, которым состав собирали
    refs_json    TEXT NOT NULL DEFAULT '[]',
    computed_at  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (user_id, season_id, source, game_id)
);
CREATE INDEX IF NOT EXISTS idx_fantasy_game_scores_season ON fantasy_game_scores(season_id, game_date);
CREATE INDEX IF NOT EXISTS idx_fantasy_game_scores_user ON fantasy_game_scores(season_id, user_id, game_date);

-- История движения цен: кто куда переехал после конкретной игры. Раньше
-- пересчёт возвращал изменения вызывающему и забывал их, поэтому «почему у
-- меня упала цена» было не на чем показать. ФИО не храним — только строка
-- листа и ссылка карточки, имя подставляется при показе.
CREATE TABLE IF NOT EXISTS price_history (
    source      TEXT NOT NULL,
    game_id     TEXT NOT NULL,
    game_date   TEXT NOT NULL DEFAULT '',
    player_row  INTEGER NOT NULL,
    ref         TEXT NOT NULL DEFAULT '',
    old_price   INTEGER NOT NULL DEFAULT 0,
    new_price   INTEGER NOT NULL DEFAULT 0,
    old_rank    TEXT NOT NULL DEFAULT '',
    new_rank    TEXT NOT NULL DEFAULT '',
    reason      TEXT NOT NULL DEFAULT '',
    changed_at  TEXT NOT NULL,
    PRIMARY KEY (source, game_id, player_row)
);
CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history(game_date DESC);

-- Какая карточка пула какой строке листа «Игроки» соответствует. Связь
-- держится на ФИО, а они живут только в памяти демона: в отдельном процессе
-- (крон после игры) реестр пуст, карточка приходит как «№88», цена по имени
-- не находится — и пересчёт цен молча не делает ничего. Запоминаем связку
-- ОДНИМИ ИДЕНТИФИКАТОРАМИ, пока имена под рукой, и пользуемся ею, когда их нет.
CREATE TABLE IF NOT EXISTS price_refs (
    ref         TEXT PRIMARY KEY,          -- ссылка карточки пула
    player_row  INTEGER NOT NULL,          -- строка листа «Игроки»
    updated_at  TEXT NOT NULL
);

-- Привязка «Telegram id -> id игрока в лиге». Человек присылает ссылку на свой
-- профиль, бот достаёт из неё числовой id: искать по фамилии нельзя (однофамильцы
-- и опечатки подмешают чужие игры). ФИО тут нет — только идентификаторы.
-- changes — сколько раз менял привязку; на этот счётчик сядет платная смена id.
CREATE TABLE IF NOT EXISTS player_identities (
    tg_user_id  TEXT NOT NULL,
    source      TEXT NOT NULL,             -- slpro | infobasket
    player_id   TEXT NOT NULL,
    comp_id     TEXT NOT NULL DEFAULT '',  -- соревнование из ссылки (Инфобаскет)
    api_url     TEXT NOT NULL DEFAULT '',
    linked_at   TEXT NOT NULL DEFAULT '',
    changes     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tg_user_id, source)
);
CREATE INDEX IF NOT EXISTS idx_player_identities_player ON player_identities(source, player_id);

-- Личные настройки отчёта: с чем сравнивать форму и как часто присылать.
-- Отчёт уходит ТОЛЬКО в личку, поэтому и настройки персональные.
CREATE TABLE IF NOT EXISTS player_report_prefs (
    tg_user_id    TEXT PRIMARY KEY,
    compare_mode  TEXT NOT NULL DEFAULT 'all',    -- all|season|prev_season|since
    compare_since TEXT NOT NULL DEFAULT '',       -- ISO-дата для режима since
    notify_mode   TEXT NOT NULL DEFAULT 'game',   -- game|week|month|off
    metrics       TEXT NOT NULL DEFAULT '',       -- какие показатели отслеживать
    last_sent     TEXT NOT NULL DEFAULT '',
    updated_at    TEXT NOT NULL DEFAULT ''
);

-- Личные настройки уведомлений фэнтези. По умолчанию (нет строки) — уведомляем.
-- open — «открыт набор», lock — «набор закрыт».
CREATE TABLE IF NOT EXISTS fantasy_notify_prefs (
    user_id      TEXT PRIMARY KEY,
    notify_open  INTEGER NOT NULL DEFAULT 1,
    notify_lock  INTEGER NOT NULL DEFAULT 1,
    updated_at   TEXT NOT NULL DEFAULT ''
);

-- Зеркало заявок лиг: КТО числится в команде. Нужно, чтобы пул фэнтези
-- собирался из локальной базы, а не походом в чужой API на каждое действие
-- игрока (один недоступный сайт превращал /start в минуты ожидания).
-- Юр-инвариант: тут только идентификаторы и номер. ФИО на диск не попадает
-- никогда — имена живут в оперативной памяти (player_names.py) и после
-- перезапуска подтягиваются заново.
CREATE TABLE IF NOT EXISTS league_rosters (
    source      TEXT NOT NULL,             -- slpro | infobasket
    team_id     TEXT NOT NULL,
    player_id   TEXT NOT NULL,
    number      TEXT NOT NULL DEFAULT '',
    comp_id     TEXT NOT NULL DEFAULT '',  -- соревнование (Инфобаскет)
    active      INTEGER NOT NULL DEFAULT 1,
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (source, team_id, player_id)
);

-- Шутки к фамилиям: фраза, которую бот дописывает к строке игрока в сообщении
-- о результате. Автор пишет её сам, себе или товарищу по команде.
-- Юр-инвариант: адресат хранится СТРОКОЙ ЛИСТА «Игроки» (row_index), а не ФИО.
-- Ник автора храним осознанно — он публикуется в «(с) @ник», в этом и смысл.
-- Фраза живёт РОВНО ОДНУ игру: иначе одна и та же шутка повторялась бы в
-- каждом результате и приедалась за неделю. Игру автор выбирает сам
-- (game_source+game_id) либо ставит «на ближайшую» (game_id пустой) — тогда
-- фраза срабатывает на первом же матче, где этот игрок выйдет на площадку.
-- used_at проставляется в момент публикации и закрывает шутку навсегда.
CREATE TABLE IF NOT EXISTS player_jokes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    target_row   INTEGER NOT NULL,          -- строка листа «Игроки»
    occasion     TEXT NOT NULL DEFAULT 'any',   -- win | loss | any
    text         TEXT NOT NULL,
    author_id    TEXT NOT NULL DEFAULT '',
    author_nick  TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT '',
    active       INTEGER NOT NULL DEFAULT 1,
    game_source  TEXT NOT NULL DEFAULT '',  -- slpro | infobasket | '' (любая)
    game_id      TEXT NOT NULL DEFAULT '',  -- '' = на ближайшую игру
    game_label   TEXT NOT NULL DEFAULT '',  -- как показать автору: «02.08 · Атланты»
    game_date    TEXT NOT NULL DEFAULT '',  -- ISO выбранной игры: с неё фраза «живёт»
    used_at      TEXT NOT NULL DEFAULT ''   -- сработала — больше не появится
);

-- На что человек подписан лично, в дополнение к общему чату. Хранится по
-- числовому Telegram id — он вечный, в отличие от ника.
CREATE TABLE IF NOT EXISTS subscriptions (
    user_id     TEXT NOT NULL,
    kind        TEXT NOT NULL,             -- team | fantasy | personal
    enabled     INTEGER NOT NULL DEFAULT 1,
    updated_at  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (user_id, kind)
);

-- Подписка на КОНКРЕТНОГО игрока: трансляция придёт, если он есть в протоколе
-- этой игры. Отдельной таблицей, а не видом в subscriptions: там ключ
-- «человек + вид», а тут их много на человека.
-- `ref` — ссылка пула (`ib:36502:123`, возможно составная через «+»), то есть
-- идентификаторы. ФИО не храним ([[legal-data-invariant]]).
CREATE TABLE IF NOT EXISTS player_subscriptions (
    user_id     TEXT NOT NULL,
    ref         TEXT NOT NULL,
    created_at  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (user_id, ref)
);
CREATE INDEX IF NOT EXISTS idx_player_subs_ref ON player_subscriptions(ref);
CREATE INDEX IF NOT EXISTS idx_player_jokes_target ON player_jokes(target_row, active);

-- «Это один и тот же человек»: ссылка лиги -> составная ссылка пула.
-- Один игрок выступает и в SLPRO, и в Инфобаскете под разными id; склейка
-- делается по ФИО, но ФИО живут в памяти и после перезапуска демона исчезают.
-- Без этой таблицы холодный старт собирал бы пул, где человек представлен
-- дважды, — и сохранённые составы ссылались бы на несуществующие карточки.
-- Здесь только идентификаторы, ФИО нет.
CREATE TABLE IF NOT EXISTS player_merges (
    ref         TEXT PRIMARY KEY,          -- одиночная ссылка лиги
    canonical   TEXT NOT NULL,             -- составная ссылка пула
    fetched_at  TEXT NOT NULL
);

-- Платежи игроков (раздел тренера). Источник истины — здесь, лист «Оплаты»
-- лишь читаемая копия: Google может быть недоступен в момент, когда тренер
-- вносит платёж, а деньги терять нельзя.
-- ФИО тут нет: платёж привязан к строке листа «Игроки». От СМС остаётся
-- только отпечаток (sha256) — по нему ловим повторную вставку того же текста.
CREATE TABLE IF NOT EXISTS payments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    player_row  INTEGER NOT NULL,           -- строка листа «Игроки»
    amount      INTEGER NOT NULL,           -- рубли
    kind        TEXT NOT NULL,              -- 'game' | 'season'
    games       INTEGER NOT NULL DEFAULT 0, -- сколько игр покрывает
    paid_at     TEXT NOT NULL,              -- дата платежа (ISO)
    bank        TEXT NOT NULL DEFAULT '',
    note        TEXT NOT NULL DEFAULT '',
    added_by    TEXT NOT NULL DEFAULT '',   -- tg id того, кто внёс
    created_at  TEXT NOT NULL,
    fingerprint TEXT NOT NULL UNIQUE,
    pushed      INTEGER NOT NULL DEFAULT 0, -- выгружен ли в лист «Оплаты»
    period      TEXT NOT NULL DEFAULT '',   -- 'YYYY-MM' для тренировок
    game_ref    TEXT NOT NULL DEFAULT '',   -- 'source:game_id' для игры
    by_coach    INTEGER NOT NULL DEFAULT 0  -- отмечено тренером без СМС
);
CREATE INDEX IF NOT EXISTS idx_payments_player ON payments(player_row);

-- Состав на игру: кого тренер заявил. Именно с этого списка ждём оплату игры —
-- не с проголосовавших и не со всей команды. ФИО нет, только строки листа.
CREATE TABLE IF NOT EXISTS game_rosters (
    source      TEXT NOT NULL,
    game_id     TEXT NOT NULL,
    player_row  INTEGER NOT NULL,
    added_by    TEXT NOT NULL DEFAULT '',   -- tg id тренера
    added_at    TEXT NOT NULL,
    PRIMARY KEY (source, game_id, player_row)
);

-- Гости на одну игру: люди не из листа «Игроки». Подмена, легионер, отец
-- игрока — они играют, попадают в состав и в стартовую пятёрку, но заводить
-- их в лист нельзя: там они получат опросы, взносы и статистику, к которым
-- отношения не имеют.
--
-- В game_rosters такой человек лежит с ОТРИЦАТЕЛЬНЫМ player_row (= -id). Так
-- ключ состава остаётся одним числом, и всё, что построено на номерах строк —
-- стартовая пятёрка, кнопки, снимок публикации, — работает без переделки.
CREATE TABLE IF NOT EXISTS game_guests (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    source     TEXT NOT NULL,
    game_id    TEXT NOT NULL,
    title      TEXT NOT NULL,
    added_by   TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS game_guests_game ON game_guests (source, game_id);

-- Состояние сбора состава: черновик, отправлен ли в чат, когда.
CREATE TABLE IF NOT EXISTS game_roster_state (
    source      TEXT NOT NULL,
    game_id     TEXT NOT NULL,
    game_date   TEXT NOT NULL DEFAULT '',
    opponent    TEXT NOT NULL DEFAULT '',
    posted_at   TEXT NOT NULL DEFAULT '',   -- пусто = ещё не отправляли в топик
    -- Куда именно ушло сообщение: [{chat_id, message_id}]. Состав правят после
    -- отправки (кто-то заболел, кто-то добавился), и правильный ответ —
    -- отредактировать то же сообщение, а не слать в чат ещё одно.
    posted_json TEXT NOT NULL DEFAULT '',
    -- Каким состав был в момент отправки: по нему видно, что он устарел.
    posted_rows TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (source, game_id)
);

-- Что бот уже разослал по расписанию: один ключ — одно событие. Оплаты
-- («train:…», «game:…») и личные разборы игр («pers:…»). Без этой отметки
-- фоновый цикл слал бы одно и то же каждые полминуты.
CREATE TABLE IF NOT EXISTS pay_events (
    event_key   TEXT PRIMARY KEY,           -- 'train:2026-09:coach_pre' и т.п.
    sent_at     TEXT NOT NULL,
    details     TEXT NOT NULL DEFAULT ''
);

-- Кого бот уже научился узнавать в СМС. Ключ — sha256 от имени отправителя,
-- не само имя: спрашивать «кто это?» второй раз не нужно, а ФИО из банковской
-- СМС на диск не кладём. Точное совпадение строки — ровно то, что нужно:
-- банк присылает одну и ту же подпись при каждом переводе.
CREATE TABLE IF NOT EXISTS payment_senders (
    sender_hash TEXT PRIMARY KEY,
    player_row  INTEGER NOT NULL,
    seen        INTEGER NOT NULL DEFAULT 1,   -- сколько раз пригодилось
    first_at    TEXT NOT NULL,
    last_at     TEXT NOT NULL
);

-- Какие разборы игр уже ушли тренерам в личку. Без этой отметки фоновый цикл
-- слал бы один и тот же отчёт каждые пять минут.
CREATE TABLE IF NOT EXISTS coach_report_sent (
    source      TEXT NOT NULL,
    team_id     TEXT NOT NULL,
    game_id     TEXT NOT NULL,
    sent_at     TEXT NOT NULL,
    PRIMARY KEY (source, team_id, game_id)
);

-- Свёрнутая статистика игрока: суммы по (источник, игрок, сезон, стадия).
-- Сыгранная игра больше не меняется, поэтому пересчитывать 40+ тысяч строк на
-- каждый запрос незачем: держим готовые суммы и трогаем только те строки, к
-- которым относится новая игра. Все запросы «за всё время» читают отсюда, и
-- вместо десятков тысяч строк складываются сотни.
CREATE TABLE IF NOT EXISTS player_totals (
    source      TEXT NOT NULL,
    player_id   TEXT NOT NULL,
    season_id   TEXT NOT NULL DEFAULT '',
    stage_id    TEXT NOT NULL DEFAULT '',
    games       INTEGER NOT NULL DEFAULT 0,
    pts INTEGER NOT NULL DEFAULT 0, reb INTEGER NOT NULL DEFAULT 0,
    reb_off INTEGER NOT NULL DEFAULT 0, reb_def INTEGER NOT NULL DEFAULT 0,
    ast INTEGER NOT NULL DEFAULT 0, stl INTEGER NOT NULL DEFAULT 0,
    blk INTEGER NOT NULL DEFAULT 0, tur INTEGER NOT NULL DEFAULT 0,
    pf INTEGER NOT NULL DEFAULT 0, foul_on INTEGER NOT NULL DEFAULT 0,
    fgm INTEGER NOT NULL DEFAULT 0, fga INTEGER NOT NULL DEFAULT 0,
    tpm INTEGER NOT NULL DEFAULT 0, tpa INTEGER NOT NULL DEFAULT 0,
    ftm INTEGER NOT NULL DEFAULT 0, fta INTEGER NOT NULL DEFAULT 0,
    secs INTEGER NOT NULL DEFAULT 0,
    -- Промахи и дабл-даблы суммой по играм не восстановишь, поэтому считаем
    -- их здесь же, на уровне отдельных матчей, и складываем уже готовыми.
    miss INTEGER NOT NULL DEFAULT 0, ftmiss INTEGER NOT NULL DEFAULT 0,
    dd INTEGER NOT NULL DEFAULT 0, td INTEGER NOT NULL DEFAULT 0,
    updated_at  TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (source, player_id, season_id, stage_id)
);
CREATE INDEX IF NOT EXISTS idx_player_totals_scope
    ON player_totals(source, season_id, stage_id);

-- Настройки бота, которые правит человек из интерфейса. Отдельно от листа
-- «Конфиг»: тот про лиги и топики, а это про поведение самого бота, и менять
-- их надо кнопкой, а не через таблицу.
CREATE TABLE IF NOT EXISTS app_settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL DEFAULT '',
    updated_at  TEXT NOT NULL DEFAULT ''
);

-- Долг, добавленный тренером руками: пропущенная аренда, штраф, разбитый мяч.
-- К требованиям из листа «Игроки» он не относится — тот столбец про регулярные
-- взносы, а это разовое. Закрывается кнопкой, а не платежом: сумма может
-- прийти вместе с другими деньгами, и угадывать, что именно ею погашено,
-- нельзя.
CREATE TABLE IF NOT EXISTS extra_debts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    player_row  INTEGER NOT NULL,
    amount      INTEGER NOT NULL,
    note        TEXT NOT NULL DEFAULT '',
    added_by    TEXT NOT NULL DEFAULT '',
    created_at  TEXT NOT NULL,
    closed_at   TEXT NOT NULL DEFAULT ''
);

-- Отрезки на площадке: когда игрок выходил и уходил. start_sec/end_sec —
-- РЕАЛЬНЫЕ секунды от спорного мяча (не игровые), потому что запись идёт в
-- реальном времени: по ним человек мотает видео. clock_sec — игровое время
-- отрезка, оно нужно только для сверки с протоколом (сумма ≈ «минуты»).
-- Имён нет, только id игрока ([[legal-data-invariant]]).
CREATE TABLE IF NOT EXISTS game_shifts (
    source      TEXT NOT NULL,
    game_id     TEXT NOT NULL,
    player_id   TEXT NOT NULL,
    period      INTEGER NOT NULL DEFAULT 0,
    start_sec   INTEGER NOT NULL,
    end_sec     INTEGER NOT NULL,
    clock_sec   INTEGER NOT NULL DEFAULT 0,
    start_left  INTEGER NOT NULL DEFAULT 0,  -- сколько было на табло при выходе
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (source, game_id, player_id, start_sec)
);

-- Отдельные действия игрока с привязкой к записи: бросок, подбор, перехват.
-- Та же хроника лиги, из которой берутся выходы на площадку (game_shifts), но
-- по одному событию, а не отрезком: человек хочет посмотреть свой трёхочковый,
-- а не восемь минут, внутри которых он был.
--
-- kind — наш словарь, а не код лиги: коды у Инфобаскета числовые и разъезжаются
-- с названиями SLPRO. Приводим к общему виду при разборе.
CREATE TABLE IF NOT EXISTS game_moments (
    source      TEXT NOT NULL,
    game_id     TEXT NOT NULL,
    player_id   TEXT NOT NULL,
    kind        TEXT NOT NULL,           -- pts3/pts2/ft/miss/reb/stl/blk/ast
    period      INTEGER NOT NULL DEFAULT 0,
    real_sec    INTEGER NOT NULL,        -- секунда записи от спорного
    left_sec    INTEGER NOT NULL DEFAULT 0,  -- сколько было на табло
    ord         INTEGER NOT NULL DEFAULT 0,
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (source, game_id, player_id, kind, real_sec, ord)
);
CREATE INDEX IF NOT EXISTS idx_game_moments_who
    ON game_moments(source, game_id, player_id);

-- Привязка записи ВК к игре: на какой секунде видео происходит спорный мяч.
-- Трансляцию включают заранее и по-разному, поэтому сдвиг задаётся руками
-- один раз на игру; пока не задан — считаем тайм-коды от спорного (0).
CREATE TABLE IF NOT EXISTS game_video_sync (
    source      TEXT NOT NULL,
    game_id     TEXT NOT NULL,
    offset_sec  INTEGER NOT NULL DEFAULT 0,
    tipoff_at   INTEGER NOT NULL DEFAULT 0,   -- unix-момент спорного по протоколу
    set_by      TEXT NOT NULL DEFAULT '',
    set_at      TEXT NOT NULL DEFAULT '',
    -- «league» — разметка целиком из протокола, «model» — часть периодов
    -- посчитана по среднему темпу, потому что лига дала негодные отметки.
    timing      TEXT NOT NULL DEFAULT 'league',
    PRIMARY KEY (source, game_id)
);

-- Зеркало справочника команд/турниров лиги: id, название, стадия. Название
-- команды — не персональные данные, его храним. Сюда же складываем то, что
-- раньше резолвилось живым запросом на каждый показ (team_contexts).
CREATE TABLE IF NOT EXISTS league_teams (
    source      TEXT NOT NULL,
    team_id     TEXT NOT NULL,
    name        TEXT NOT NULL DEFAULT '',
    league      TEXT NOT NULL DEFAULT '',  -- как названо у нас в «Конфиге»
    comp_id     TEXT NOT NULL DEFAULT '',
    season_id   TEXT NOT NULL DEFAULT '',
    stage_id    TEXT NOT NULL DEFAULT '',
    ours        INTEGER NOT NULL DEFAULT 0, -- наша команда (кандидат в пул)
    ctx_json    TEXT NOT NULL DEFAULT '',   -- полный контекст стадии (SLPRO)
    fetched_at  TEXT NOT NULL,
    PRIMARY KEY (source, team_id)
);
"""

# Порядок колонок в листе "Сервисный" — должен совпадать с SERVICE_HEADER
# в enhanced_duplicate_protection.py (индексы TYPE_COL..TEAM_B_ID_COL).
SERVICE_SHEET_COLUMNS = [
    "data_type", "logged_at", "unique_key", "status", "additional_data", "link",
    "comp_id", "team_id", "alt_name", "settings", "game_id", "game_date",
    "game_time", "arena", "team_a_id", "team_b_id",
]
CONFIG_SHEET_NAME = "Конфиг"
# Сколько колонок листа «Конфиг» зеркалим. 10 = по J: столько занимает секция
# голосований («ID топика», «Комментарий»). Меняешь тут — добавь колонки в
# схему config_rows и миграцию в init_db.
CONFIG_COLUMNS = 10


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


now_iso = _now_iso  # публичный алиас для enhanced_duplicate_protection.py


# Права базы выставляем сами и в каждом процессе. Причина та же, по которой
# закрыты логи: файл ложится на диск и уезжает в бэкапы, а внутри вся команда —
# ФИО, ники, дни рождения, оплаты и долги. Лежала она при этом с правами 644,
# то есть на чтение любому пользователю сервера.
#
# Почему здесь, а не разово руками и не только в демоне: базу открывают ещё
# десяток cron-скриптов, и файлы -wal/-shm создаёт тот процесс, который пришёл
# первым, — с обычным umask, то есть снова широко. Так права возвращаются на
# место сами после любого запуска.
_DB_DIR_MODE = 0o750
_DB_FILE_MODE = 0o640
_hardened = False


def _harden_db() -> None:
    """Закрывает базу и её спутников от посторонних. Раз на процесс.

    Не владелец — молча уходим: чужой процесс не обязан уметь это чинить, а
    падать из-за прав на ЧТЕНИИ кэша нельзя."""
    global _hardened
    if _hardened:
        return
    _hardened = True
    for path, mode in ((DB_PATH.parent, _DB_DIR_MODE),
                       (DB_PATH, _DB_FILE_MODE),
                       (DB_PATH.with_name(DB_PATH.name + "-wal"), _DB_FILE_MODE),
                       (DB_PATH.with_name(DB_PATH.name + "-shm"), _DB_FILE_MODE)):
        try:
            if path.exists():
                os.chmod(path, mode)
        except OSError:
            pass


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=8.0)
    conn.execute("PRAGMA journal_mode = WAL")
    # После включения WAL — тогда -wal и -shm уже созданы и их тоже закроем.
    _harden_db()
    # 8с (было 5с) — теперь пишут не только периодический sync демона, но и
    # ~10 cron-скриптов при включённом SERVICE_RECORDS_LOCAL_PRIMARY.
    conn.execute("PRAGMA busy_timeout = 8000")
    # NORMAL безопасен в WAL-режиме (не теряет закоммиченные транзакции при
    # обычном падении процесса, только при потере питания) и не платит за
    # fsync на каждый commit — сервер не эфемерный контейнер.
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def _connection() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


# Публичный алиас — используется enhanced_duplicate_protection.py для
# атомарных операций над service_records (ON CONFLICT ...), которым нужен
# прямой доступ к соединению, а не готовая обёртка-функция.
get_connection = _connection


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, coltype: str, default: str) -> None:
    """CREATE TABLE IF NOT EXISTS не добавляет колонки в уже существующую
    таблицу — нужна ручная миграция для баз, созданных до появления
    колонки (например data/bot.db на сервере с Phase 1)."""
    cols = [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype} DEFAULT {default}")


def init_db() -> None:
    """Идемпотентно создаёт схему. Безопасно вызывать при каждом старте
    демона и в любом месте, откуда читают кэш."""
    with _connection() as conn:
        conn.executescript(SCHEMA)
        _ensure_column(conn, "attendance", "dirty", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "fantasy_seasons", "settings_json", "TEXT NOT NULL", "''")
        _ensure_column(conn, "game_player_stats", "secs", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "game_player_stats", "plus_minus", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "players", "tg_user_id", "TEXT NOT NULL", "''")
        _ensure_column(conn, "game_player_stats", "stage_id", "TEXT NOT NULL", "''")
        _ensure_column(conn, "game_player_stats", "foul_on", "INTEGER NOT NULL", "0")
        # Привязка шутки к конкретной игре появилась позже самой таблицы.
        _ensure_column(conn, "player_jokes", "game_source", "TEXT NOT NULL", "''")
        _ensure_column(conn, "player_jokes", "game_id", "TEXT NOT NULL", "''")
        _ensure_column(conn, "player_jokes", "game_label", "TEXT NOT NULL", "''")
        _ensure_column(conn, "player_jokes", "used_at", "TEXT NOT NULL", "''")
        _ensure_column(conn, "player_jokes", "game_date", "TEXT NOT NULL", "''")
        _ensure_column(conn, "players", "pay_season", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "players", "pay_game", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "players", "active_mark", "TEXT NOT NULL", "''")
        _ensure_column(conn, "payments", "period", "TEXT NOT NULL", "''")
        _ensure_column(conn, "payments", "game_ref", "TEXT NOT NULL", "''")
        _ensure_column(conn, "payments", "by_coach", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "game_roster_state", "posted_json", "TEXT NOT NULL", "''")
        _ensure_column(conn, "game_roster_state", "posted_rows", "TEXT NOT NULL", "''")
        _ensure_column(conn, "game_meta", "home_name", "TEXT NOT NULL", "''")
        _ensure_column(conn, "game_meta", "guest_name", "TEXT NOT NULL", "''")
        _ensure_column(conn, "game_meta", "video_started_at", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "game_meta", "video_seconds", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "game_video_sync", "tipoff_at", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "game_video_sync", "timing", "TEXT NOT NULL", "'league'")
        _ensure_column(conn, "game_shifts", "start_left", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "game_roster_state", "form", "TEXT NOT NULL", "''")
        _ensure_column(conn, "players", "role", "TEXT NOT NULL", "''")
        _ensure_column(conn, "feature_access", "until", "TEXT NOT NULL", "''")
        # Долг человеку, которого нет в листе «Игроки»: гость на одну игру,
        # соперник, кто угодно. Тогда player_row = 0, а имя лежит здесь.
        _ensure_column(conn, "extra_debts", "who", "TEXT NOT NULL", "''")
        # Кто выходит в стартовой пятёрке. Отдельно от состава на игру: в
        # составе двенадцать человек, начинают пятеро, и это разные решения.
        _ensure_column(conn, "game_roster_state", "start_rows", "TEXT NOT NULL", "''")
        # Колонка появилась вместе с выбором показателей в личной статистике:
        # на сервере таблица уже существовала, и кнопка настроек падала на
        # «no column named metrics».
        _ensure_column(conn, "player_report_prefs", "metrics", "TEXT NOT NULL", "''")
        # «ID топика» и «Комментарий» секции голосований — колонки I и J листа.
        _ensure_column(conn, "fantasy_rosters", "mode", "TEXT NOT NULL", "''")
        _ensure_column(conn, "fantasy_rosters", "meta_json", "TEXT NOT NULL", "''")
        _ensure_column(conn, "fantasy_game_scores", "mode", "TEXT NOT NULL", "''")
        _ensure_column(conn, "players", "price", "INTEGER NOT NULL", "0")
        _ensure_column(conn, "players", "tier", "TEXT NOT NULL", "''")
        _ensure_column(conn, "config_rows", "col_i", "TEXT NOT NULL", "''")
        _ensure_column(conn, "config_rows", "col_j", "TEXT NOT NULL", "''")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gps_scope "
                     "ON game_player_stats(source, season_id, stage_id)")
        conn.commit()


def _mark_sync_result(conn: sqlite3.Connection, table_name: str, row_count: int, error: Optional[str]) -> None:
    now = _now_iso()
    if error is None:
        conn.execute(
            """
            INSERT INTO sync_meta (table_name, last_success_at, last_attempt_at, last_error, row_count)
            VALUES (?, ?, ?, NULL, ?)
            ON CONFLICT(table_name) DO UPDATE SET
                last_success_at = excluded.last_success_at,
                last_attempt_at = excluded.last_attempt_at,
                last_error = NULL,
                row_count = excluded.row_count
            """,
            (table_name, now, now, row_count),
        )
    else:
        conn.execute(
            """
            INSERT INTO sync_meta (table_name, last_success_at, last_attempt_at, last_error, row_count)
            VALUES (?, NULL, ?, ?, 0)
            ON CONFLICT(table_name) DO UPDATE SET
                last_attempt_at = excluded.last_attempt_at,
                last_error = excluded.last_error
            """,
            (table_name, now, error),
        )
    conn.commit()


# ── Sync (WRITE side — вызывается только из bot_daemon.py) ────────────────

def sync_players(spreadsheet) -> None:
    init_db()
    need_reconcile = False
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
            records = ws.get_all_records()
            now = _now_iso()
            rows = []
            for idx, r in enumerate(records, start=2):
                if not r.get("Имя"):
                    continue
                rows.append((
                    idx,
                    str(r.get("Фамилия", "")),
                    str(r.get("Имя", "")),
                    str(r.get("Ник", "")),
                    str(r.get("Telegram ID", "")),
                    str(r.get(PLAYERS_TG_ID_HEADER, "")),
                    str(r.get("Дата рождения", "")),
                    str(r.get("Статус", "")),
                    str(r.get("Команда", "")),
                    str(r.get("Дата добавления", "")),
                    str(r.get("Примечания", "")),
                    _to_int(r.get(PLAYERS_PRICE_HEADER)),
                    str(r.get(PLAYERS_TIER_HEADER, "")).strip(),
                    _to_int(r.get(PLAYERS_PAY_SEASON_HEADER)),
                    _to_int(r.get(PLAYERS_PAY_GAME_HEADER)),
                    str(r.get(PLAYERS_ACTIVE_HEADER, "")).strip(),
                    str(r.get(PLAYERS_ROLE_HEADER, "")).strip(),
                    now,
                ))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM players")
            conn.executemany(
                """
                INSERT INTO players
                (row_index, surname, name, nickname, telegram_id, tg_user_id, birthday, status, team, added_date, notes, price, tier, pay_season, pay_game, active_mark, role, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "players", len(rows), None)
            need_reconcile = True
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "players", 0, str(e))
            raise
    # Строки листа могли сдвинуться (добавили игрока, отсортировали) — именно
    # здесь это становится видно. Сверяем привязки сразу, а не ждём, пока
    # человек увидит в приложении чужую карточку.
    if need_reconcile:
        reconcile_player_links()


# ───────────── Привязка игрока к числовому Telegram id ──────────────────────

def get_player_link(tg_user_id: str) -> Optional[Dict[str, Any]]:
    """Закреплённая привязка по числовому id (или None)."""
    init_db()
    with _connection() as conn:
        row = conn.execute(
            "SELECT * FROM player_links WHERE tg_user_id = ?", (str(tg_user_id),)).fetchone()
    return dict(row) if row else None


def is_row_linked(player_row: int) -> bool:
    """Занята ли строка листа кем-то (защита от захвата освободившегося ника)."""
    init_db()
    with _connection() as conn:
        return conn.execute(
            "SELECT 1 FROM player_links WHERE player_row = ?", (int(player_row),)).fetchone() is not None


def link_player(tg_user_id: str, username: str, player_row: int) -> bool:
    """Закрепляет строку листа за числовым id. Идемпотентно; если строка уже
    за кем-то другим или id уже привязан к другой строке — отказ."""
    init_db()
    with _connection() as conn:
        try:
            conn.execute(
                """INSERT INTO player_links (tg_user_id, username, player_row, linked_at)
                   VALUES (?, ?, ?, ?)""",
                (str(tg_user_id), (username or "").lstrip("@"), int(player_row), _now_iso()))
            conn.execute("UPDATE players SET tg_user_id = ? WHERE row_index = ?",
                         (str(tg_user_id), int(player_row)))
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            conn.rollback()
            return False


def _norm_name(text: str) -> str:
    return " ".join((text or "").lower().replace("ё", "е").split())


def _verify_player_row(ws, player_row: int, expect: str) -> Optional[int]:
    """Та ли это строка. Возвращает номер строки для записи (или None).

    Номер приходит из зеркала, а лист живёт своей жизнью: тренер отсортировал
    его по ФИО между синхронизациями — и запись по старому номеру попадёт в
    чужую строку. Поэтому перед записью сверяем ФИО, а если не сошлось —
    ищем строку заново. Не нашли — лучше не писать вовсе."""
    if not expect:
        return int(player_row)
    header = ws.row_values(1)
    try:
        i_sur, i_name = header.index("Фамилия"), header.index("Имя")
    except ValueError:
        return int(player_row)
    row = ws.row_values(int(player_row))
    here = _norm_name(" ".join([row[i_sur] if len(row) > i_sur else "",
                                row[i_name] if len(row) > i_name else ""]))
    if here == _norm_name(expect):
        return int(player_row)
    for idx, r in enumerate(ws.get_all_values()[1:], start=2):
        cand = _norm_name(" ".join([r[i_sur] if len(r) > i_sur else "",
                                    r[i_name] if len(r) > i_name else ""]))
        if cand == _norm_name(expect):
            logger.warning("Лист сдвинулся: «%s» теперь в строке %s, а не %s",
                           expect, idx, player_row)
            return idx
    logger.warning("Не нашёл «%s» в листе — записывать вслепую не буду", expect)
    return None


def write_player_tg_id(spreadsheet, player_row: int, tg_user_id: str,
                       expect: str = "") -> bool:
    """Вписывает числовой id в лист «Игроки» — чтобы было видно, кто подключился.
    Best-effort: доступ держится на локальной player_links, поэтому сбой записи
    в Sheets не должен ломать вход."""
    try:
        ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
        row_no = _verify_player_row(ws, player_row, expect)
        if row_no is None:
            return False
        header = ws.row_values(1)
        if PLAYERS_TG_ID_HEADER not in header:
            header.append(PLAYERS_TG_ID_HEADER)
            ws.update_cell(1, len(header), PLAYERS_TG_ID_HEADER)
        col = header.index(PLAYERS_TG_ID_HEADER) + 1
        ws.update_cell(row_no, col, str(tg_user_id))
        return True
    except Exception as e:
        logger.warning(f"Не удалось записать числовой TG id в лист: {e}")
        return False


PLAYERS_NICK_HEADER = "Telegram ID"       # исторически там @ники, не числа


def write_player_nickname(spreadsheet, player_row: int, username: str,
                          expect: str = "") -> bool:
    """Обновляет @ник в листе «Игроки» после опознания.

    Ник в таблице устаревает: человек сменил @, и строка перестаёт совпадать с
    ним — следующий игрок с похожим ником уже не опознается, а админ видит в
    листе адрес, которого нет. Пишем только когда ник реально изменился, чтобы
    не дёргать Sheets на каждом входе."""
    uname = (username or "").lstrip("@").strip()
    if not uname:
        return False
    init_db()
    with _connection() as conn:
        row = conn.execute("SELECT telegram_id FROM players WHERE row_index = ?",
                           (int(player_row),)).fetchone()
    current = (row["telegram_id"] if row else "").lstrip("@").strip().lower()
    if current == uname.lower():
        return False
    try:
        ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
        header = ws.row_values(1)
        if PLAYERS_NICK_HEADER not in header:
            logger.warning("В листе «Игроки» нет столбца «Telegram ID» — ник не пишем")
            return False
        row_no = _verify_player_row(ws, player_row, expect)
        if row_no is None:
            return False
        col = header.index(PLAYERS_NICK_HEADER) + 1
        ws.update_cell(row_no, col, f"@{uname}")
    except Exception as e:
        logger.warning(f"Не удалось обновить ник в листе: {e}")
        return False
    with _connection() as conn:
        conn.execute("UPDATE players SET telegram_id = ? WHERE row_index = ?",
                     (f"@{uname}", int(player_row)))
        conn.commit()
    logger.info(f"Ник в листе обновлён: строка {player_row} -> @{uname}")
    return True


def sync_attendance(spreadsheet) -> None:
    init_db()
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(ATTEND_SHEET_NAME)
            all_rows = ws.get_all_values()[1:]  # skip header
            now = _now_iso()
            rows = []
            for idx, row in enumerate(all_rows, start=2):
                if len(row) < 2 or not row[1]:
                    continue
                row = row + [""] * (11 - len(row))
                revote_count = int(row[10]) if row[10].isdigit() else 0
                rows.append((
                    row[0], row[1], row[2], row[3], row[4],
                    row[5], row[6], row[7], row[8], row[9],
                    revote_count, idx, now,
                ))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM attendance")
            conn.executemany(
                """
                INSERT INTO attendance
                (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
                 training_date, config_poll_id, updated_at, revote_count, row_index, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "attendance", len(rows), None)
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "attendance", 0, str(e))
            raise


def sync_service_log(spreadsheet) -> None:
    init_db()
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(SERVICE_SHEET_NAME)
            all_rows = ws.get_all_values()[1:]  # skip header
            now = _now_iso()
            rows = []
            for idx, row in enumerate(all_rows, start=2):
                if not row or not row[0] or row[0] not in ACTIVITY_TYPES:
                    continue
                row = row + [""] * (16 - len(row))
                rows.append((idx, *row[:16], now))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM service_log")
            conn.executemany(
                """
                INSERT INTO service_log
                (row_index, data_type, logged_at, unique_key, status, additional_data, link,
                 comp_id, team_id, alt_name, settings, game_id, game_date, game_time, arena,
                 team_a_id, team_b_id, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "service_log", len(rows), None)
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "service_log", 0, str(e))
            raise


def sync_config(spreadsheet) -> None:
    """Зеркалит лист 'Конфиг' целиком, сырыми колонками — сама разбор
    логика (несколько секций с маркерами) остаётся в
    enhanced_duplicate_protection.py и не меняется."""
    init_db()
    with _connection() as conn:
        try:
            ws = spreadsheet.worksheet(CONFIG_SHEET_NAME)
            all_rows = ws.get_all_values()[1:]  # skip header
            now = _now_iso()
            rows = []
            for idx, row in enumerate(all_rows, start=2):
                row = list(row) + [""] * (CONFIG_COLUMNS - len(row))
                rows.append((idx, *row[:CONFIG_COLUMNS], now))
            conn.execute("BEGIN")
            conn.execute("DELETE FROM config_rows")
            conn.executemany(
                """
                INSERT INTO config_rows
                (row_index, col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h,
                 col_i, col_j, synced_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
            _mark_sync_result(conn, "config_rows", len(rows), None)
        except Exception as e:
            conn.rollback()
            _mark_sync_result(conn, "config_rows", 0, str(e))
            raise


def _to_int(value: Any) -> int:
    try:
        return int(float(str(value).replace(",", ".").strip()))
    except (TypeError, ValueError):
        return 0


def get_player_prices() -> Dict[str, Dict[str, Any]]:
    """{нормализованное ФИО: {price, tier, row}} из листа «Игроки».

    Цена в листе — ВСЕГДА стартовая точка пересчёта: бот считает новую цену от
    того, что стоит в таблице сейчас. Значит правка тренера не «перебивается»
    ботом, а становится новым началом отсчёта (fantasy_prices.recalc)."""
    init_db()
    with _connection() as conn:
        rows = conn.execute(
            "SELECT row_index, surname, name, price, tier FROM players "
            "WHERE price > 0 OR tier != ''"
        ).fetchall()
    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = " ".join(f"{r['surname']} {r['name']}".lower().replace("ё", "е").split())
        if key.strip():
            out[key] = {"price": int(r["price"] or 0), "tier": r["tier"] or "",
                        "row": int(r["row_index"])}
    return out


def write_player_prices(spreadsheet, updates: Dict[int, int]) -> int:
    """Вписывает новые цены в столбец «Стоимость» листа «Игроки».

    Единственное место, где бот пишет в этот лист цену. Столбец «Уровень» не
    трогаем: там формула, она пойдёт за ценой сама. Возвращает число строк."""
    if not updates:
        return 0
    ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
    header = ws.row_values(1)
    if PLAYERS_PRICE_HEADER not in header:
        logger.warning("В листе «Игроки» нет столбца «Стоимость» — цены не пишем")
        return 0
    col = header.index(PLAYERS_PRICE_HEADER) + 1
    letter = chr(ord("A") + col - 1) if col <= 26 else None
    if not letter:
        logger.warning("Столбец «Стоимость» слишком далеко (%s) — пропуск", col)
        return 0
    data = [{"range": f"{letter}{row}", "values": [[int(price)]]}
            for row, price in sorted(updates.items())]
    ws.batch_update(data)
    init_db()
    with _connection() as conn:
        conn.executemany("UPDATE players SET price = ? WHERE row_index = ?",
                         [(int(p), int(r)) for r, p in updates.items()])
        conn.commit()
    return len(data)


# Какие столбцы листа «Игроки» правим из админки и куда они ложатся в зеркале.
PLAYER_FIELDS = {
    "bd": ("Дата рождения", "birthday", "🎂 Дата рождения"),
    "nick": ("Ник", "nickname", "✏️ Ник"),
    "role": (PLAYERS_ROLE_HEADER, "role", "🎽 Амплуа"),
    # Суммы, которые ждём с человека. Правятся из раздела тренера, чтобы не
    # лезть в таблицу ради одной цифры.
    "season": (PLAYERS_PAY_SEASON_HEADER, "pay_season", "🏋️ Взнос за тренировки"),
    "game": (PLAYERS_PAY_GAME_HEADER, "pay_game", "🏀 Оплата игры"),
    # Остальные колонки листа — чтобы тренеру не приходилось открывать таблицу
    # ради строчки. ФИО тоже правится: люди меняют фамилии, и опечатки бывают.
    "surname": ("Фамилия", "surname", "👤 Фамилия"),
    "name": ("Имя", "name", "👤 Имя"),
    "status": ("Статус", "status", "📋 Статус"),
    "team": ("Команда", "team", "🏳️ Команда"),
    "active": (PLAYERS_ACTIVE_HEADER, "active_mark", "✅ Активность"),
    "price": (PLAYERS_PRICE_HEADER, "price", "💎 Стоимость"),
    "tier": (PLAYERS_TIER_HEADER, "tier", "🏅 Уровень"),
}

# ФИО — якорь, по которому строка ищется в листе перед записью. Меняя его, надо
# сверяться со СТАРЫМ значением, иначе проверка не найдёт строку и правка молча
# не пройдёт.
NAME_FIELDS = ("surname", "name")


def add_player(spreadsheet, surname: str, name: str) -> Optional[int]:
    """Заводит игрока в листе «Игроки». Возвращает номер строки.

    Пишем ТОЛЬКО фамилию и имя: остальное тренер заполняет полями на карточке,
    и подставлять туда умолчания опаснее, чем оставить пусто — «Активность» по
    умолчанию означала бы взнос со всей команды.

    Тёзку не заводим: две одинаковые строки в листе — это разъехавшиеся
    посещаемость и оплаты, а понять потом, где чей платёж, невозможно."""
    surname, name = str(surname or "").strip(), str(name or "").strip()
    if not surname and not name:
        return None
    ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
    head = ws.row_values(1)
    for need in ("Фамилия", "Имя"):
        if need not in head:
            logger.warning("В листе «Игроки» нет столбца «%s»", need)
            return None

    want = f"{surname} {name}".strip().lower().replace("ё", "е")
    values = ws.get_all_values()
    i_sur, i_name = head.index("Фамилия"), head.index("Имя")
    for row in values[1:]:
        got = f"{_cell(row, i_sur)} {_cell(row, i_name)}".strip().lower().replace("ё", "е")
        if got and got == want:
            logger.info("Игрок «%s %s» в листе уже есть", surname, name)
            return None

    line = [""] * len(head)
    line[i_sur], line[i_name] = surname, name
    ws.append_row(line, value_input_option="USER_ENTERED")
    row_no = len(values) + 1
    logger.info("В лист «Игроки» добавлен игрок, строка %s", row_no)
    return row_no


def _cell(row: List[str], idx: int) -> str:
    return str(row[idx]).strip() if idx < len(row) else ""


def write_player_field(spreadsheet, player_row: int, field: str, value: str,
                       expect: str = "") -> bool:
    """Пишет одну ячейку игрока в лист и в зеркало.

    Строку сверяем по ФИО тем же способом, что и при записи числового id: лист
    живёт своей жизнью, и запись по устаревшему номеру попала бы к соседу."""
    meta = PLAYER_FIELDS.get(field)
    if not meta:
        # Молча вернуть False нельзя: снаружи это неотличимо от «таблица не
        # ответила», и опечатка в ключе живёт месяцами. Ровно так и вышло с
        # «active_mark» вместо «active»: тренеру шло «сними руками», а в
        # таблицу никто не ходил.
        logger.error("Поле «%s» листу «Игроки» неизвестно. Есть: %s",
                     field, ", ".join(sorted(PLAYER_FIELDS)))
        return False
    header, column, _ = meta
    ws = spreadsheet.worksheet(PLAYERS_SHEET_NAME)
    row_no = _verify_player_row(ws, player_row, expect)
    if row_no is None:
        return False
    head = ws.row_values(1)
    if header not in head:
        logger.warning("В листе «Игроки» нет столбца «%s»", header)
        return False
    ws.update_cell(row_no, head.index(header) + 1, str(value))
    init_db()
    with _connection() as conn:
        # Денежные колонки в зеркале числовые: строкой туда попадёт «5500» и
        # сравнения «сколько должен» начнут врать.
        # Числовые колонки зеркала: строкой туда попадёт «5500», и сравнения
        # «сколько должен» начнут врать. «Стоимость» — из той же породы: по ней
        # считается бюджет фэнтези.
        if column in ("pay_season", "pay_game", "price"):
            try:
                stored = int(str(value).strip() or 0)
            except ValueError:
                logger.warning("Поле %s: «%s» — не число, в зеркало не пишу",
                               column, value)
                return False
        else:
            stored = str(value)
        conn.execute(f"UPDATE players SET {column} = ? WHERE row_index = ?",
                     (stored, int(row_no)))
        conn.commit()
    return True


def get_setting(key: str, default: Any = None) -> Any:
    """Значение настройки. default — когда её ни разу не меняли."""
    init_db()
    with _connection() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?",
                           (str(key),)).fetchone()
    return row["value"] if row else default


def get_int_setting(key: str, default: int) -> int:
    try:
        return int(get_setting(key, default))
    except (TypeError, ValueError):
        return default


def set_setting(key: str, value: Any) -> None:
    init_db()
    with _connection() as conn:
        conn.execute(
            """INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value,
                                              updated_at = excluded.updated_at""",
            (str(key), str(value), _now_iso()))
        conn.commit()


def get_config_rows() -> List[List[str]]:
    """Возвращает сырые строки листа 'Конфиг' из локального зеркала —
    пустой список, если синхронизация ещё ни разу не проходила (вызывающий
    код должен в этом случае сам сделать fallback на живой Sheets-запрос)."""
    init_db()
    with _connection() as conn:
        rows = conn.execute(
            "SELECT col_a, col_b, col_c, col_d, col_e, col_f, col_g, col_h, col_i, col_j "
            "FROM config_rows ORDER BY row_index"
        ).fetchall()
    return [list(r) for r in rows]


def bootstrap_service_records(spreadsheet) -> Dict[str, Any]:
    """Разовая (но идемпотентная — безопасно перезапускать) заливка ВСЕХ
    строк/колонок листа 'Сервисный' (не только ACTIVITY_TYPES, в отличие
    от sync_service_log) в service_records, перед включением
    SERVICE_RECORDS_LOCAL_PRIMARY. INSERT ... ON CONFLICT DO NOTHING —
    повторный запуск не затирает то, что уже успело появиться локально."""
    init_db()
    ws = spreadsheet.worksheet(SERVICE_SHEET_NAME)
    all_rows = ws.get_all_values()[1:]  # skip header
    now = _now_iso()
    inserted = 0
    with _connection() as conn:
        conn.execute("BEGIN")
        for idx, row in enumerate(all_rows, start=2):
            if not row or not row[0]:
                continue
            data_type = row[0].strip()
            row = row + [""] * (16 - len(row))
            unique_key = row[2].strip()
            if not unique_key:
                continue
            cur = conn.execute(
                """
                INSERT INTO service_records
                (data_type, unique_key, logged_at, status, additional_data, link,
                 comp_id, team_id, alt_name, settings, game_id, game_date, game_time, arena,
                 team_a_id, team_b_id, created_at, updated_at, sheet_row_hint, dirty)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                ON CONFLICT(data_type, unique_key) WHERE deleted = 0 DO NOTHING
                """,
                (data_type, unique_key, row[1], row[3], row[4], row[5], row[6], row[7],
                 row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],
                 now, now, idx),
            )
            inserted += cur.rowcount
        conn.commit()
    return {"sheet_rows": len(all_rows), "inserted": inserted}


def sync_all(spreadsheet) -> Dict[str, Any]:
    """Синхронизирует таблицы независимо — ошибка в одной не должна мешать
    остальным.

    "attendance" сюда намеренно не входит: голоса теперь локально-первичные
    (пишет upsert_vote_local в реальном времени), периодический pull из
    Sheets затирал бы ещё не выгруженные (dirty=1) локальные изменения.
    sync_attendance() остаётся доступной отдельно — для разового
    bootstrap существующих голосов при первом включении."""
    summary: Dict[str, Any] = {}
    for name, fn in (
        ("players", sync_players),
        ("service_log", sync_service_log),
        ("config_rows", sync_config),
    ):
        try:
            fn(spreadsheet)
            summary[name] = "ok"
        except Exception as e:
            summary[name] = f"error: {e}"
    return summary


# ── Read side (admin_panel.py и другие потребители) ────────────────────────

def get_players_stats() -> Dict[str, int]:
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        linked = conn.execute(
            "SELECT COUNT(*) FROM players WHERE telegram_id != ''"
        ).fetchone()[0]
    return {"total": total, "linked": linked}


def get_players_page(offset: int = 0, limit: int = 8) -> Dict[str, Any]:
    """Постраничный список игроков (для показа в /admin по 5-10 за раз)."""
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM players").fetchone()[0]
        rows = conn.execute(
            """
            SELECT surname, name, nickname, telegram_id, status
            FROM players ORDER BY row_index LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "limit": limit}


def get_attendance_stats() -> Dict[str, int]:
    init_db()
    with _connection() as conn:
        unique_users = conn.execute(
            "SELECT COUNT(DISTINCT user_id) FROM attendance"
        ).fetchone()[0]
        total_votes = conn.execute("SELECT COUNT(*) FROM attendance").fetchone()[0]
        unique_30d = conn.execute(
            """
            SELECT COUNT(DISTINCT user_id) FROM attendance
            WHERE updated_at != '' AND
                  substr(updated_at, 7, 4) || '-' || substr(updated_at, 4, 2) || '-' || substr(updated_at, 1, 2)
                  >= date('now', '-30 days')
            """
        ).fetchone()[0]
    return {
        "unique_users": unique_users,
        "total_votes": total_votes,
        "unique_30d": unique_30d,
    }


def get_recent_service_events(limit: int = 8, since_days: Optional[int] = None) -> List[sqlite3.Row]:
    """limit — обычный лимит по количеству; since_days — вместо/вместе с
    limit можно попросить все события за последние N дней (для "Лог бота",
    например since_days=1 — сегодня и вчера)."""
    init_db()
    where = ""
    params: tuple = ()
    if since_days is not None:
        where = """
            WHERE substr(logged_at, 7, 4) || '-' || substr(logged_at, 4, 2) || '-' || substr(logged_at, 1, 2)
                  >= date('now', ?)
        """
        params = (f"-{since_days} days",)
    with _connection() as conn:
        rows = conn.execute(
            f"""
            SELECT data_type, status, logged_at FROM service_log
            {where}
            ORDER BY
                substr(logged_at, 7, 4) || substr(logged_at, 4, 2) || substr(logged_at, 1, 2) ||
                substr(logged_at, 12, 2) || substr(logged_at, 15, 2) DESC
            LIMIT ?
            """,
            (*params, limit if since_days is None else 200),
        ).fetchall()
    return rows


def get_sync_status() -> Dict[str, Dict[str, Any]]:
    init_db()
    with _connection() as conn:
        rows = conn.execute("SELECT * FROM sync_meta").fetchall()
    return {row["table_name"]: dict(row) for row in rows}


# ── Пользователи бота ("В боте") и лог ошибок ───────────────────────────────
# В отличие от остального модуля, эти функции сами являются источником
# истины для SQLite (не просто кэш) и при наличии spreadsheet дублируют
# запись в соответствующий лист Google Sheets — по желанию пользователя
# видеть всё в таблицах. Ошибка записи в Sheets никогда не должна ронять
# вызывающий код (это часто сам обработчик ошибок).

def _get_or_create_ws(spreadsheet, title: str, header: List[str]):
    import gspread
    try:
        ws = spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=1000, cols=len(header))
        ws.update("A1", [header])
    return ws


def record_bot_user(spreadsheet, telegram_id: str, username: str, first_name: str) -> bool:
    """Возвращает True, если пользователь новый (первый /start)."""
    init_db()
    with _connection() as conn:
        existing = conn.execute(
            "SELECT 1 FROM bot_users WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
        if existing:
            return False
        now = _now_iso()
        conn.execute(
            "INSERT INTO bot_users (telegram_id, username, first_name, first_seen_at) VALUES (?, ?, ?, ?)",
            (telegram_id, username, first_name, now),
        )
        conn.commit()
    if spreadsheet is not None:
        try:
            ws = _get_or_create_ws(spreadsheet, BOT_USERS_SHEET_NAME, ["Telegram ID", "Username", "Имя", "Первый /start"])
            ws.append_row([telegram_id, username, first_name, datetime.now().strftime("%d.%m.%Y %H:%M")])
        except Exception:
            pass
    return True


def get_bot_users_page(offset: int = 0, limit: int = 8) -> Dict[str, Any]:
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM bot_users").fetchone()[0]
        rows = conn.execute(
            "SELECT telegram_id, username, first_name, first_seen_at FROM bot_users ORDER BY first_seen_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "limit": limit}


def link_from_votes() -> List[Dict[str, Any]]:
    """Опознаёт по голосам в опросах тех, кто в личку бота не заходил.

    Привязка «человек ↔ строка листа» рождалась только при /start или открытии
    приложения. Но человек может год голосовать в общем чате и ни разу не
    написать боту — и оставаться «не игроком команды»: ровно так вышло с
    @prikolexa, который голосовал и в играх, и в тренировках.

    В голосе есть и числовой id, и @ник — этого достаточно. Окно доверия то
    же, что при /start: ник должен совпасть, а строка листа быть свободной."""
    init_db()
    linked: List[Dict[str, Any]] = []
    with _connection() as conn:
        voters = conn.execute(
            """SELECT user_id, username FROM (
                   SELECT user_id, username FROM game_votes
                   UNION SELECT user_id, username FROM attendance)
                WHERE username != ''
                  AND user_id NOT IN (SELECT tg_user_id FROM player_links)""").fetchall()
        rows = conn.execute(
            """SELECT row_index, surname, name, telegram_id FROM players
                WHERE telegram_id != ''
                  AND row_index NOT IN (SELECT player_row FROM player_links)""").fetchall()
    # Сравнение в Python: lower() в SQLite не знает кириллицы, а ники бывают
    # записаны и с «@», и без, и в другом регистре.
    free = {str(r["telegram_id"]).lstrip("@").strip().lower(): dict(r) for r in rows}
    for v in voters:
        nick = str(v["username"] or "").lstrip("@").strip().lower()
        target = free.get(nick)
        if not target:
            continue
        if link_player(str(v["user_id"]), nick, int(target["row_index"])):
            free.pop(nick, None)
            linked.append({"tg_user_id": str(v["user_id"]), "username": nick,
                           "player_row": int(target["row_index"]),
                           "title": f"{target['surname']} {target['name']}".strip()})
    return linked


def unlinked_bot_users() -> List[Dict[str, Any]]:
    """Кого бот видел, но не сопоставил со строкой листа.

    Считаем всех, о ком вообще что-то знаем: и нажимавших /start, и просто
    голосовавших в общем чате. Голосующие важнее — их большинство, а в личку
    бота заходят единицы; раньше они в этот список не попадали вовсе, и
    привязать их вручную было негде.

    Автоматика опознаёт только точное совпадение ника (link_from_votes), а
    здесь остаются те, у кого ник в листе устарел: сравнить «@Gudi» из листа с
    «@gudi20» из голоса может только человек."""
    init_db()
    with _connection() as conn:
        rows = conn.execute(
            """SELECT telegram_id, username, first_name, first_seen_at, source FROM (
                   SELECT telegram_id, username, first_name, first_seen_at,
                          'бот' AS source
                     FROM bot_users
                   UNION
                   SELECT user_id, username, first_name, MAX(updated_at),
                          'опрос' AS source
                     FROM (SELECT user_id, username, first_name, updated_at FROM game_votes
                           UNION ALL
                           SELECT user_id, username, first_name, updated_at FROM attendance)
                    GROUP BY user_id)
                WHERE telegram_id NOT IN (SELECT tg_user_id FROM player_links)
                ORDER BY first_seen_at DESC""").fetchall()
    # Один и тот же человек мог и голосовать, и запускать бота — оставляем по
    # одной строке на числовой id.
    seen, out = set(), []
    for r in rows:
        key = str(r["telegram_id"])
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(r))
    return out


def free_player_rows() -> List[Dict[str, Any]]:
    """Строки листа «Игроки», ещё ни за кем не закреплённые."""
    init_db()
    with _connection() as conn:
        rows = conn.execute(
            """SELECT row_index, surname, name, telegram_id, tg_user_id FROM players
               WHERE name != ''
                 AND row_index NOT IN (SELECT player_row FROM player_links)
               ORDER BY surname, name""").fetchall()
    return [dict(r) for r in rows]


def rename_legacy_sheets(spreadsheet) -> List[str]:
    """Приводит названия листов к нынешним (см. SHEET_RENAMES).

    Идемпотентно: переименовываем, только если старый лист есть, а нового ещё
    нет. Если оба существуют — не трогаем ничего и говорим об этом в лог:
    сливать два листа автоматически нельзя, это данные."""
    if spreadsheet is None:
        return []
    try:
        titles = {ws.title: ws for ws in spreadsheet.worksheets()}
    except Exception as e:
        logger.warning("Не смог перечислить листы для переименования: %s", e)
        return []
    done = []
    for old, new in SHEET_RENAMES.items():
        if old not in titles:
            continue
        if new in titles:
            logger.warning("Листы «%s» и «%s» существуют оба — переименование "
                           "пропущено, разберись руками", old, new)
            continue
        try:
            titles[old].update_title(new)
            titles[new] = titles.pop(old)
            done.append(f"{old} -> {new}")
            logger.info("Лист переименован: «%s» -> «%s»", old, new)
        except Exception as e:
            logger.warning("Лист «%s» не переименовался: %s", old, e)
    return done


def coach_report_sent(source: str, team_id: str, game_id: str) -> bool:
    init_db()
    with _connection() as conn:
        return bool(conn.execute(
            "SELECT 1 FROM coach_report_sent WHERE source=? AND team_id=? AND game_id=?",
            (str(source), str(team_id), str(game_id))).fetchone())


def mark_coach_report(source: str, team_id: str, game_id: str) -> None:
    init_db()
    with _connection() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO coach_report_sent (source, team_id, game_id, sent_at) "
            "VALUES (?, ?, ?, ?)",
            (str(source), str(team_id), str(game_id), _now_iso()))
        conn.commit()


# ─────────────── Доступ к закрытым разделам ─────────────────────────────────
#
# Два вида: 'personal' — личная статистика игрока, 'team' — раздел тренера
# (разбор игр и учёт оплат).
# Даются независимо: тренеру может быть нужна команда без личной, игроку —
# наоборот. Админу оба открыты и без записи в таблице.

ACCESS_TEAM = "team"
ACCESS_PERSONAL = "personal"
ACCESS_TITLES = {ACCESS_TEAM: "Раздел тренера", ACCESS_PERSONAL: "Личная статистика"}


def _clean_nick(username: str) -> str:
    return (username or "").lstrip("@").strip().lower()


def grant_access(kind: str, username: str, granted_by: str = "",
                 days: int = 0, until: str = "") -> bool:
    """Открывает раздел по @нику. Числовой id подтянется при первом входе.

    days > 0 — доступ на срок: он закончится сам, и о нём не придётся помнить.
    Так проще выдавать разово (посмотреть отчёт перед турниром, помочь с
    разбором), не превращая временное в постоянное.

    until — конкретная дата (ISO). Она сильнее days: доступ продают до числа
    («оплатил месяц» — это до такого-то), а не на «столько-то суток от
    сегодня»."""
    nick = _clean_nick(username)
    if not nick or kind not in ACCESS_TITLES:
        return False
    return _write_access(kind, nick, "", granted_by, _until_value(days, until))


def grant_access_id(kind: str, tg_user_id: str = "", username: str = "",
                    granted_by: str = "", until: str = "") -> bool:
    """Открывает раздел конкретному человеку — по id, нику или обоим.

    Тренер выдаёт доступ из списка игроков, а не по нику: он знает людей по
    фамилии, и половина команды ник вообще не показывает. Поэтому ключом
    строки может быть и числовой id — тогда ник не нужен вовсе и первый вход
    ничего не «закрепляет», доступ работает сразу.

    Если строка на этот id уже есть, продлеваем её, а не заводим вторую: иначе
    у человека, сменившего ник, копились бы дубли с разными сроками."""
    uid, nick = str(tg_user_id or "").strip(), _clean_nick(username)
    if kind not in ACCESS_TITLES or not (uid or nick):
        return False
    key = nick or f"id:{uid}"
    if uid:
        init_db()
        with _connection() as conn:
            row = conn.execute(
                "SELECT username FROM feature_access WHERE kind = ? AND tg_user_id = ?",
                (kind, uid)).fetchone()
        if row:
            key = str(row["username"])
    return _write_access(kind, key, uid, granted_by, _until_value(0, until))


def _until_value(days: int, until: str) -> str:
    """Дата окончания доступа в ISO. Пусто — бессрочно."""
    text = str(until or "").strip()
    if text:
        from datetime import date as _date
        try:
            return _date.fromisoformat(text[:10]).isoformat()
        except ValueError:
            return ""
    if int(days or 0) > 0:
        from datetime import date as _date, timedelta as _td
        return (_date.today() + _td(days=int(days))).isoformat()
    return ""


def _write_access(kind: str, key: str, tg_user_id: str, granted_by: str,
                  until: str) -> bool:
    """Одна запись о доступе. Уже известный id не затираем пустым: строку
    могли завести по нику, а человек с тех пор зашёл и закрепился."""
    init_db()
    with _connection() as conn:
        conn.execute(
            """INSERT INTO feature_access (kind, username, tg_user_id, granted_at,
                                           granted_by, until)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(kind, username) DO UPDATE SET
                   granted_at=excluded.granted_at,
                   granted_by=excluded.granted_by,
                   until=excluded.until,
                   tg_user_id=CASE WHEN excluded.tg_user_id != ''
                                   THEN excluded.tg_user_id
                                   ELSE feature_access.tg_user_id END""",
            (kind, key, str(tg_user_id), _now_iso(), str(granted_by), until))
        conn.commit()
    logger.info("Доступ «%s» выдан %s%s (кем: %s)", ACCESS_TITLES[kind], key,
                f" до {until}" if until else " бессрочно", granted_by)
    return True


def _access_expired(row: Any) -> bool:
    """Истёк ли срок. Пустое поле — бессрочно."""
    from datetime import date as _date
    until = str((row["until"] if row else "") or "").strip()
    if not until:
        return False
    try:
        return _date.fromisoformat(until) < _date.today()
    except ValueError:
        return False


def purge_expired_access() -> int:
    """Снимает доступы, у которых вышел срок. Возвращает сколько снял."""
    from datetime import date as _date
    init_db()
    today = _date.today().isoformat()
    with _connection() as conn:
        n = conn.execute(
            "DELETE FROM feature_access WHERE until != '' AND until < ?",
            (today,)).rowcount
        conn.commit()
    if n:
        logger.info("Доступов с истёкшим сроком снято: %s", n)
    return n


def revoke_access(kind: str, username: str) -> bool:
    init_db()
    with _connection() as conn:
        n = conn.execute("DELETE FROM feature_access WHERE kind = ? AND username = ?",
                         (kind, _clean_nick(username))).rowcount
        conn.commit()
    return bool(n)


def revoke_access_id(kind: str, tg_user_id: str = "", username: str = "") -> bool:
    """Закрывает раздел человеку — и по id, и по нику.

    Обе стороны сразу: доступ мог быть выдан по нику до того, как человек
    зашёл, и снять надо оба следа, иначе закрытый доступ откроется обратно при
    первом же входе."""
    uid, nick = str(tg_user_id or "").strip(), _clean_nick(username)
    init_db()
    n = 0
    with _connection() as conn:
        if uid:
            n += conn.execute(
                "DELETE FROM feature_access WHERE kind = ? AND tg_user_id = ?",
                (kind, uid)).rowcount
        if nick:
            n += conn.execute(
                "DELETE FROM feature_access WHERE kind = ? AND username = ?",
                (kind, nick)).rowcount
        conn.commit()
    return bool(n)


def access_until(kind: str, tg_user_id: str = "", username: str = "") -> Optional[str]:
    """До какого числа открыт раздел: None — не открыт, '' — бессрочно.

    Отличать «нет доступа» от «бессрочно» приходится явно: и то и другое —
    пустая дата, а на экране это противоположные состояния."""
    uid, nick = str(tg_user_id or "").strip(), _clean_nick(username)
    init_db()
    with _connection() as conn:
        row = None
        if uid:
            row = conn.execute(
                "SELECT until FROM feature_access WHERE kind = ? AND tg_user_id = ?",
                (kind, uid)).fetchone()
        if row is None and nick:
            row = conn.execute(
                "SELECT until FROM feature_access WHERE kind = ? AND username = ?",
                (kind, nick)).fetchone()
    if row is None or _access_expired(row):
        return None
    return str(row["until"] or "")


def access_list(kind: str = "") -> List[Dict[str, Any]]:
    init_db()
    with _connection() as conn:
        if kind:
            rows = conn.execute(
                "SELECT * FROM feature_access WHERE kind = ? ORDER BY granted_at", (kind,))
        else:
            rows = conn.execute("SELECT * FROM feature_access ORDER BY kind, granted_at")
        return [dict(r) for r in rows]


def has_access(kind: str, tg_user_id: str, username: str = "") -> bool:
    """Открыт ли раздел. При первом совпадении по нику закрепляет числовой id.

    Дальше ник не нужен: сменил @ — доступ остался; освободил @ и его занял
    посторонний — тот НЕ пройдёт, строка уже закреплена за прежним id."""
    uid, nick = str(tg_user_id), _clean_nick(username)
    init_db()
    with _connection() as conn:
        by_id = conn.execute(
            "SELECT until FROM feature_access WHERE kind = ? AND tg_user_id = ?",
            (kind, uid)).fetchone()
        if by_id:
            # Срок проверяем на каждом входе, а не только уборкой по расписанию:
            # иначе между полуночью и первой уборкой доступ ещё живой.
            return not _access_expired(by_id)
        if not nick:
            return False
        row = conn.execute(
            "SELECT tg_user_id, until FROM feature_access WHERE kind = ? AND username = ?",
            (kind, nick)).fetchone()
        if not row or row["tg_user_id"] or _access_expired(row):
            return False                    # нет доступа, ник занят или срок вышел
        conn.execute(
            "UPDATE feature_access SET tg_user_id = ? WHERE kind = ? AND username = ?",
            (uid, kind, nick))
        conn.commit()
    logger.info("Доступ «%s»: @%s закреплён за id %s", ACCESS_TITLES.get(kind, kind), nick, uid)
    return True


def reconcile_player_links() -> List[Dict[str, Any]]:
    """Переставляет привязки на строки, где человек стоит СЕЙЧАС.

    Привязка помнит номер строки листа, а номер — не ключ: тренер добавил
    игрока, удалил или отсортировал столбец, и строка под старым номером уже
    чужая. Молча и незаметно: человек открывает приложение и видит карточку
    соседа по алфавиту.

    Якорь — числовой id, который бот сам вписал в лист: ячейка едет вместе со
    строкой, поэтому она всегда при своём человеке. Нет id — ищем по нику.
    Занятую строку не отбираем, но за проход чужие привязки успевают уехать на
    свои места, поэтому проходов несколько."""
    init_db()
    fixed: List[Dict[str, Any]] = []
    with _connection() as conn:
        names = {int(r["row_index"]): f"{r['surname']} {r['name']}".strip()
                 for r in conn.execute("SELECT row_index, surname, name FROM players")}
        movers = []
        for l in conn.execute("SELECT * FROM player_links").fetchall():
            uid = str(l["tg_user_id"])
            row = conn.execute(
                "SELECT row_index FROM players WHERE tg_user_id = ? LIMIT 1", (uid,)).fetchone()
            if not row and l["username"]:
                row = conn.execute(
                    """SELECT row_index FROM players
                       WHERE lower(ltrim(telegram_id, '@')) = ? LIMIT 1""",
                    (str(l["username"]).lower(),)).fetchone()
            if row and int(row["row_index"]) != int(l["player_row"]):
                movers.append((uid, l["username"], int(l["player_row"]), int(row["row_index"])))
        if not movers:
            return []
        # Сначала СНИМАЕМ всех переезжающих со своих строк и только потом
        # расставляем. Иначе двое, поменявшихся местами (обычная сортировка
        # листа), встают в клинч: каждый ждёт, пока освободится строка соседа.
        parked = {uid: -(cur + 1) for uid, _, cur, _ in movers}
        for uid, park in parked.items():
            conn.execute("UPDATE player_links SET player_row = ? WHERE tg_user_id = ?",
                         (park, uid))
        for uid, uname, was_row, now_row in movers:
            busy = conn.execute(
                "SELECT tg_user_id FROM player_links WHERE player_row = ? AND tg_user_id != ?",
                (now_row, uid)).fetchone()
            if busy:                  # строку держит тот, кто никуда не едет
                conn.execute("UPDATE player_links SET player_row = ? WHERE tg_user_id = ?",
                             (was_row, uid))
                logger.warning("Привязка @%s не переехала: строка %s занята другим id %s",
                               uname or uid, now_row, busy["tg_user_id"])
                continue
            conn.execute("UPDATE player_links SET player_row = ? WHERE tg_user_id = ?",
                         (now_row, uid))
            fixed.append({"tg_user_id": uid, "username": uname,
                          "was_row": was_row, "was": names.get(was_row, ""),
                          "now_row": now_row, "now": names.get(now_row, "")})
        conn.commit()
    for f in fixed:
        logger.warning("Привязка съехала: @%s был на «%s» (строка %s), вернул на «%s» (строка %s)",
                       f["username"] or f["tg_user_id"], f["was"], f["was_row"],
                       f["now"], f["now_row"])
    return fixed


def linked_players() -> List[Dict[str, Any]]:
    """Кто за какой строкой листа закреплён — для проверки и отвязки."""
    init_db()
    with _connection() as conn:
        rows = conn.execute(
            """SELECT l.tg_user_id, l.username, l.player_row, l.linked_at,
                      p.surname, p.name
               FROM player_links l LEFT JOIN players p ON p.row_index = l.player_row
               ORDER BY p.surname, p.name""").fetchall()
    return [dict(r) for r in rows]


def unlink_player(tg_user_id: str) -> Optional[int]:
    """Снимает привязку. Возвращает освобождённую строку листа (или None).

    Чистим и `players.tg_user_id`: пока числовой id стоит в строке, доступ
    восстанавливается сам при следующем входе (это шаг 2 опознания) — и
    «отвязка» была бы отвязкой лишь до первого касания."""
    init_db()
    with _connection() as conn:
        row = conn.execute("SELECT player_row FROM player_links WHERE tg_user_id = ?",
                           (str(tg_user_id),)).fetchone()
        if not row:
            return None
        player_row = int(row["player_row"])
        conn.execute("DELETE FROM player_links WHERE tg_user_id = ?", (str(tg_user_id),))
        conn.execute("UPDATE players SET tg_user_id = '' WHERE row_index = ?", (player_row,))
        conn.commit()
    return player_row


def report_error(source: str, message: str, spreadsheet=None) -> None:
    """Логирует ошибку в SQLite (для быстрого показа в /admin) и, если
    передан spreadsheet, дублирует в лист "Ошибки". Сама никогда не
    бросает исключение — безопасно вызывать из любого except-блока."""
    now = _now_iso()
    message = message[:2000]
    try:
        init_db()
        with _connection() as conn:
            conn.execute(
                "INSERT INTO errors (source, message, logged_at) VALUES (?, ?, ?)",
                (source, message, now),
            )
            conn.commit()
    except Exception:
        pass
    if spreadsheet is not None:
        try:
            ws = _get_or_create_ws(spreadsheet, ERRORS_SHEET_NAME, ["Источник", "Сообщение", "Когда"])
            ws.append_row([source, message, datetime.now().strftime("%d.%m.%Y %H:%M")])
        except Exception:
            pass


def add_feedback(user_id: Any, username: str, name: str, message: str) -> int:
    """Сохраняет обращение игрока. Возвращает его номер — его же показываем
    человеку, чтобы он мог сослаться («по обращению №7…»)."""
    init_db()
    with _connection() as conn:
        cur = conn.execute(
            """INSERT INTO feedback (user_id, username, name, message, logged_at)
               VALUES (?, ?, ?, ?, ?)""",
            (str(user_id), username or "", name or "", message[:4000], _now_iso()))
        conn.commit()
        return int(cur.lastrowid)


def get_feedback_page(offset: int = 0, limit: int = 5) -> Dict[str, Any]:
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
        rows = conn.execute(
            """SELECT id, user_id, username, name, message, logged_at, answered
               FROM feedback ORDER BY id DESC LIMIT ? OFFSET ?""",
            (limit, offset)).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "limit": limit}


def mark_feedback_answered(feedback_id: Any) -> None:
    init_db()
    with _connection() as conn:
        conn.execute("UPDATE feedback SET answered = 1 WHERE id = ?", (int(feedback_id),))
        conn.commit()


def get_errors_page(offset: int = 0, limit: int = 8) -> Dict[str, Any]:
    init_db()
    with _connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM errors").fetchone()[0]
        rows = conn.execute(
            "SELECT source, message, logged_at FROM errors ORDER BY id DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
    return {"rows": rows, "total": total, "offset": offset, "limit": limit}


def get_user_action_log(offset: int = 0, limit: int = 10) -> Dict[str, Any]:
    """Объединённый лог действий пользователей: /start + голоса за
    тренировки, отсортированные по времени (новые сверху)."""
    init_db()
    with _connection() as conn:
        starts = conn.execute(
            "SELECT 'СТАРТ' as kind, telegram_id as user_id, username, first_name, '' as detail, first_seen_at as ts FROM bot_users"
        ).fetchall()
        votes = conn.execute(
            "SELECT 'ГОЛОС' as kind, user_id, username, first_name, vote_text as detail, updated_at as ts FROM attendance"
        ).fetchall()

    def _parse_ts(ts: str):
        # Оба формата приводим к наивному datetime — сервер работает в UTC,
        # так что оба варианта фактически в одной шкале, а сравнивать
        # tz-aware и tz-naive datetime напрямую нельзя.
        try:
            return datetime.fromisoformat(ts).replace(tzinfo=None)
        except ValueError:
            pass
        try:
            return datetime.strptime(ts, "%d.%m.%Y %H:%M")
        except ValueError:
            return datetime.min

    combined = [dict(r) for r in starts] + [dict(r) for r in votes]
    combined.sort(key=lambda item: _parse_ts(item["ts"]), reverse=True)
    total = len(combined)
    return {"rows": combined[offset:offset + limit], "total": total, "offset": offset, "limit": limit}


# ── Push: локальные изменения → Google Sheets ───────────────────────────────
# Обратное направление относительно всего остального модуля. Политика при
# конфликте: локальные данные всегда побеждают — это бот-генерируемые
# записи (не редактируются людьми), в отличие от players/config_rows,
# которые остаются pull-only в обратную сторону.

_SERVICE_END_COL = chr(ord('A') + len(SERVICE_SHEET_COLUMNS) - 1)


def push_service_records(spreadsheet, batch_size: int = 200) -> Dict[str, Any]:
    """Выгружает накопленные dirty=1 записи в лист 'Сервисный'.

    Ищет существующую строку по unique_key через ws.find() (а не по
    запомненному номеру строки) — надёжнее при параллельных
    удалениях/сдвигах строк, ценой одного API-вызова на запись. Push не на
    горячем пути (раз в 6 часов или по кнопке), так что это приемлемо."""
    init_db()
    ws = spreadsheet.worksheet(SERVICE_SHEET_NAME)

    with _connection() as conn:
        dirty_rows = conn.execute(
            "SELECT * FROM service_records WHERE dirty = 1 ORDER BY id LIMIT ?",
            (batch_size,),
        ).fetchall()

    if not dirty_rows:
        return {"pushed": 0, "inserted": 0, "updated": 0, "deleted": 0}

    inserted = updated = deleted = 0
    to_append: List[List[str]] = []
    to_append_ids: List[int] = []
    pushed_ids: List[int] = []

    for r in dirty_rows:
        try:
            cell = ws.find(r["unique_key"], in_column=3)
        except Exception:
            cell = None

        if r["deleted"]:
            if cell:
                try:
                    ws.delete_rows(cell.row)
                    deleted += 1
                except Exception:
                    continue  # не помечаем dirty=0 — попробуем в следующий push
            pushed_ids.append(r["id"])
            continue

        values = [str(r[col]) for col in SERVICE_SHEET_COLUMNS]
        if cell:
            try:
                ws.update(f"A{cell.row}:{_SERVICE_END_COL}{cell.row}", [values])
                updated += 1
                pushed_ids.append(r["id"])
            except Exception:
                continue
        else:
            to_append.append(values)
            to_append_ids.append(r["id"])

    if to_append:
        try:
            ws.append_rows(to_append, value_input_option="USER_ENTERED")
            inserted += len(to_append)
            pushed_ids.extend(to_append_ids)
        except Exception:
            pass  # не помечаем dirty=0 — попробуем весь append в следующий push

    if pushed_ids:
        with _connection() as conn:
            conn.executemany("UPDATE service_records SET dirty = 0 WHERE id = ?", [(i,) for i in pushed_ids])
            conn.commit()

    return {"pushed": len(pushed_ids), "inserted": inserted, "updated": updated, "deleted": deleted}


# ── Голоса за тренировки — локально-первичные (пишет только демон) ─────────

def upsert_vote_local(
    tg_poll_id: str, user_id: str, username: str, first_name: str, last_name: str,
    vote_text: str, vote_type: str, training_date: str, config_poll_id: str,
) -> str:
    """Атомарный upsert по (tg_poll_id, user_id) — заменяет прямую запись в
    Sheets на горячем пути bot_daemon.py:handle_poll_answer. Возвращает
    'new'/'updated'/'skipped' (тот же контракт, что и старый upsert_vote в
    collect_votes.py, для единообразия логов)."""
    init_db()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with _connection() as conn:
        existing = conn.execute(
            "SELECT revote_count FROM attendance WHERE tg_poll_id = ? AND user_id = ?",
            (tg_poll_id, user_id),
        ).fetchone()

        if vote_type == "REMOVED" and not existing:
            return "skipped"  # ретракт голоса, которого мы ещё не видели

        revotes = (existing["revote_count"] + 1) if existing else 0
        conn.execute(
            """
            INSERT INTO attendance
            (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
             training_date, config_poll_id, updated_at, revote_count, row_index, synced_at, dirty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, 1)
            ON CONFLICT(tg_poll_id, user_id) DO UPDATE SET
                username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name,
                vote_text=excluded.vote_text, vote_type=excluded.vote_type,
                training_date=excluded.training_date, config_poll_id=excluded.config_poll_id,
                updated_at=excluded.updated_at, revote_count=excluded.revote_count, dirty=1
            """,
            (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
             training_date, config_poll_id, now, revotes, _now_iso()),
        )
        conn.commit()
    return "updated" if existing else "new"


def push_attendance(spreadsheet, batch_size: int = 500) -> Dict[str, Any]:
    """Выгружает накопленные dirty=1 голоса в лист 'Логи посещаемости'. Один
    объёмный get_all_values() строит индекс существующих строк по
    (tg_poll_id, user_id) вместо ws.find() на каждую запись — голосов за
    сезон может быть заметно больше, чем строк в 'Сервисный'."""
    init_db()
    with _connection() as conn:
        dirty_rows = conn.execute(
            "SELECT * FROM attendance WHERE dirty = 1 LIMIT ?", (batch_size,)
        ).fetchall()
    if not dirty_rows:
        return {"pushed": 0, "inserted": 0, "updated": 0}

    ws = spreadsheet.worksheet(ATTEND_SHEET_NAME)
    all_values = ws.get_all_values()
    index: Dict[Tuple[str, str], int] = {}
    for i, row in enumerate(all_values[1:], start=2):
        if len(row) >= 2:
            index[(row[0], row[1])] = i

    updates: List[Tuple[int, List[str]]] = []
    to_append: List[List[str]] = []
    pushed_keys: List[Tuple[str, str]] = []

    for r in dirty_rows:
        key = (r["tg_poll_id"], r["user_id"])
        values = [r["tg_poll_id"], r["user_id"], r["username"], r["first_name"], r["last_name"],
                  r["vote_text"], r["vote_type"], r["training_date"], r["config_poll_id"],
                  r["updated_at"], str(r["revote_count"])]
        if key in index:
            updates.append((index[key], values))
        else:
            to_append.append(values)
        pushed_keys.append(key)

    updated = inserted = 0
    for row_num, values in updates:
        try:
            ws.update(f"A{row_num}:K{row_num}", [values])
            updated += 1
        except Exception:
            pushed_keys = [k for k in pushed_keys if k != (values[0], values[1])]
    if to_append:
        try:
            ws.append_rows(to_append, value_input_option="USER_ENTERED")
            inserted += len(to_append)
        except Exception:
            append_keys = {(v[0], v[1]) for v in to_append}
            pushed_keys = [k for k in pushed_keys if k not in append_keys]

    if pushed_keys:
        with _connection() as conn:
            conn.executemany(
                "UPDATE attendance SET dirty = 0 WHERE tg_poll_id = ? AND user_id = ?",
                pushed_keys,
            )
            conn.commit()

    return {"pushed": len(pushed_keys), "inserted": inserted, "updated": updated}


# ── Реестр опросов (тренировки + игры) — читает то же место, куда пишет ────
# add_record() для TRAINING_POLL_REG/GAME_POLL_REG. Раньше тренировочный
# реестр читался напрямую из живого Sheets (collect_votes.load_training_polls),
# а писался уже в локальную БД (через add_record, когда включён
# SERVICE_RECORDS_LOCAL_PRIMARY) — из-за этого новый опрос мог быть не
# виден демону до следующего push (до 6 часов), и голоса за него терялись
# бы. Эта функция читает то же самое service_records, куда идёт запись —
# без окна рассинхронизации.

def load_poll_registrations_local(data_type: str) -> Dict[str, Dict[str, Any]]:
    """{tg_poll_id: {options, training_date, config_poll_id, game_id}} —
    game_id пуст для TRAINING_POLL_REG, training_date хранит и game_date
    для GAME_POLL_REG (то же поле, разный смысл по контексту вызова)."""
    init_db()
    with _connection() as conn:
        rows = conn.execute(
            "SELECT * FROM service_records WHERE data_type = ? AND deleted = 0 ORDER BY id",
            (data_type.upper(),),
        ).fetchall()
    registry: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        try:
            meta = json.loads(r["additional_data"]) if r["additional_data"] else {}
        except (json.JSONDecodeError, TypeError):
            continue
        tg_id = str(meta.get("tg_poll_id", ""))
        if not tg_id:
            continue
        registry[tg_id] = {
            "options": meta.get("options", []),
            "training_date": r["game_date"],
            "config_poll_id": r["alt_name"],
            "game_id": meta.get("game_id", ""),
        }
    return registry


# ── Голоса за игровые опросы — локально-первичные (пишет только демон) ─────

def upsert_game_vote_local(
    tg_poll_id: str, user_id: str, username: str, first_name: str, last_name: str,
    vote_text: str, vote_type: str, game_id: str, game_date: str,
) -> str:
    """Атомарный upsert по (tg_poll_id, user_id) — тот же контракт, что и
    upsert_vote_local(), для голосов по игровым опросам."""
    init_db()
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    with _connection() as conn:
        existing = conn.execute(
            "SELECT revote_count FROM game_votes WHERE tg_poll_id = ? AND user_id = ?",
            (tg_poll_id, user_id),
        ).fetchone()

        if vote_type == "REMOVED" and not existing:
            return "skipped"

        revotes = (existing["revote_count"] + 1) if existing else 0
        conn.execute(
            """
            INSERT INTO game_votes
            (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
             game_id, game_date, updated_at, revote_count, synced_at, dirty)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            ON CONFLICT(tg_poll_id, user_id) DO UPDATE SET
                username=excluded.username, first_name=excluded.first_name, last_name=excluded.last_name,
                vote_text=excluded.vote_text, vote_type=excluded.vote_type,
                game_id=excluded.game_id, game_date=excluded.game_date,
                updated_at=excluded.updated_at, revote_count=excluded.revote_count, dirty=1
            """,
            (tg_poll_id, user_id, username, first_name, last_name, vote_text, vote_type,
             game_id, game_date, now, revotes, _now_iso()),
        )
        conn.commit()
    return "updated" if existing else "new"


GAME_ATTEND_SHEET_NAME = "Логи игр"
GAME_ATTEND_HEADER = ["TG_POLL_ID", "USER_ID", "USERNAME", "ИМЯ", "ФАМИЛИЯ",
                       "ОТВЕТ", "ТИП", "GAME_ID", "ДАТА_ИГРЫ", "ОБНОВЛЕНО", "ПЕРЕГОЛОСОВАНИЙ"]


def push_game_votes(spreadsheet, batch_size: int = 500) -> Dict[str, Any]:
    """Выгружает накопленные dirty=1 голоса за игры в лист 'Логи игр' (создаётся при необходимости) — по образцу push_attendance()."""
    init_db()
    with _connection() as conn:
        dirty_rows = conn.execute(
            "SELECT * FROM game_votes WHERE dirty = 1 LIMIT ?", (batch_size,)
        ).fetchall()
    if not dirty_rows:
        return {"pushed": 0, "inserted": 0, "updated": 0}

    ws = _get_or_create_ws(spreadsheet, GAME_ATTEND_SHEET_NAME, GAME_ATTEND_HEADER)
    all_values = ws.get_all_values()
    index: Dict[Tuple[str, str], int] = {}
    for i, row in enumerate(all_values[1:], start=2):
        if len(row) >= 2:
            index[(row[0], row[1])] = i

    updates: List[Tuple[int, List[str]]] = []
    to_append: List[List[str]] = []
    pushed_keys: List[Tuple[str, str]] = []

    for r in dirty_rows:
        key = (r["tg_poll_id"], r["user_id"])
        values = [r["tg_poll_id"], r["user_id"], r["username"], r["first_name"], r["last_name"],
                  r["vote_text"], r["vote_type"], r["game_id"], r["game_date"],
                  r["updated_at"], str(r["revote_count"])]
        if key in index:
            updates.append((index[key], values))
        else:
            to_append.append(values)
        pushed_keys.append(key)

    updated = inserted = 0
    for row_num, values in updates:
        try:
            ws.update(f"A{row_num}:K{row_num}", [values])
            updated += 1
        except Exception:
            pushed_keys = [k for k in pushed_keys if k != (values[0], values[1])]
    if to_append:
        try:
            ws.append_rows(to_append, value_input_option="USER_ENTERED")
            inserted += len(to_append)
        except Exception:
            append_keys = {(v[0], v[1]) for v in to_append}
            pushed_keys = [k for k in pushed_keys if k not in append_keys]

    if pushed_keys:
        with _connection() as conn:
            conn.executemany(
                "UPDATE game_votes SET dirty = 0 WHERE tg_poll_id = ? AND user_id = ?",
                pushed_keys,
            )
            conn.commit()

    return {"pushed": len(pushed_keys), "inserted": inserted, "updated": updated}
