#!/usr/bin/env python3
"""
Разметка листа «Конфиг» блоками.

Лист ведёт человек, и рядом с настройками живут подсказки, примеры и заметки.
Раньше парсер читал всё подряд «до маркера конца», и любая строка-пояснение
могла приехать как настройка: подсказка «Инфобаскет · compId=140825 и
teamId=36502» превращалась в турнир с двумя соревнованиями.

Теперь каждая секция обрамляется маркерами, и бот читает ТОЛЬКО то, что внутри:

    --- START GAME ---          турниры команды (ТИП / ИД / ИД команды / имя)
    ...
    --- END GAME ---
    --- START VOTING ---        опросы (тренировки)
    ...
    --- END VOTING ---
    --- START AUTOMATIONS ---   топики и чаты автоматических сообщений
    ...
    --- END AUTOMATIONS ---

Всё, что вне блоков, — комментарии для человека. Маркеры распознаются с любым
числом дефисов и пробелов и в любом регистре: «--- END  GAME---» (как набралось
руками) читается так же, как «--- END GAME ---».

Модуль намеренно без зависимостей: его импортируют и enhanced_duplicate_protection,
и slpro_client, и fallback_game_monitor — тянуть за собой Google-клиент нельзя.
"""

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

GAME = "GAME"
VOTING = "VOTING"
AUTOMATIONS = "AUTOMATIONS"
BLOCKS = (GAME, VOTING, AUTOMATIONS)

# Синонимы имён блоков: пишут и по-русски, и в единственном числе.
_ALIASES = {
    "GAME": GAME, "GAMES": GAME, "ИГРЫ": GAME, "ИГРА": GAME, "CONFIG": GAME,
    "VOTING": VOTING, "VOTE": VOTING, "POLLS": VOTING, "ГОЛОСОВАНИЯ": VOTING,
    "AUTOMATIONS": AUTOMATIONS, "AUTOMATION": AUTOMATIONS, "АВТОМАТИЗАЦИИ": AUTOMATIONS,
}

_MARKER_RE = re.compile(
    r"^[-–—=\s]*(START|END|СТАРТ|НАЧАЛО|КОНЕЦ)\s*([A-ZА-ЯЁ_]*)[-–—=\s]*$"
)
_STARTS = {"START", "СТАРТ", "НАЧАЛО"}

# Маркеры старой разметки (без START): по ним делили лист до перехода на блоки.
_LEGACY_GAME_END = {"END", "END_CONFIG", "CONFIG_END", "END OF CONFIG", "КОНЕЦ",
                    "--- END ---", "=== END ==="}
_LEGACY_VOTING_END = "--- END VOTING ---"
_LEGACY_AUTOMATIONS_END = "--- END AUTOMATIONS ---"


def _cell(row: Sequence[Any], idx: int = 0) -> str:
    try:
        value = row[idx]
    except (IndexError, TypeError):
        return ""
    return str(value or "").strip()


def parse_marker(cell: Any) -> Optional[Tuple[str, Optional[str]]]:
    """('start'|'end', имя блока или None) — либо None, если это не маркер."""
    text = re.sub(r"\s+", " ", str(cell or "")).strip().upper()
    if not text or not any(ch in text for ch in "-–—="):
        return None
    m = _MARKER_RE.match(text)
    if not m:
        return None
    word, name = m.group(1), m.group(2)
    kind = "start" if word in _STARTS else "end"
    return kind, _ALIASES.get(name)


def has_blocks(rows: Sequence[Sequence[Any]]) -> bool:
    """Размечен ли лист по-новому (есть хоть один START)."""
    for row in rows:
        marker = parse_marker(_cell(row))
        if marker and marker[0] == "start" and marker[1]:
            return True
    return False


def split(rows: Sequence[Sequence[Any]],
          strict: bool = False) -> Dict[str, List[List[str]]]:
    """Строки листа → {'GAME': [...], 'VOTING': [...], 'AUTOMATIONS': [...]}.

    Сами маркеры и всё, что вне блоков, отбрасываются. Если START-маркеров на
    листе нет вовсе — читаем по старой разметке, чтобы не сломать таблицы,
    которые ещё не переразметили.

    Блок, оказавшийся пустым (маркеры не поставили или поставили не там), при
    `strict=False` добирается по старой разметке и о нём печатается
    предупреждение: настройки живые, и молча терять топики автоматизаций
    из-за неудачно стоящего маркера нельзя. `strict=True` — только блоки,
    для диагностики «что бот видит по маркерам»."""
    if not has_blocks(rows):
        return legacy_split(rows)

    out: Dict[str, List[List[str]]] = {name: [] for name in BLOCKS}
    current: Optional[str] = None
    for row in rows:
        marker = parse_marker(_cell(row))
        if marker:
            kind, name = marker
            if kind == "start":
                current = name          # неизвестное имя -> None: строки не в счёт
            else:
                # «END GAME» закрывает GAME; голый «END» — то, что открыто сейчас
                if name is None or name == current:
                    current = None
            continue
        if current:
            out[current].append([str(c or "") for c in row])

    if not strict:
        legacy = None
        for name in BLOCKS:
            if out[name]:
                continue
            if legacy is None:
                legacy = legacy_split(rows)
            if legacy[name]:
                print(f"⚠️ «Конфиг»: блок {name} пуст — маркеры "
                      f"--- START {name} --- / --- END {name} --- стоят не вокруг "
                      f"своих строк. Читаю по-старому ({len(legacy[name])} строк).")
                out[name] = legacy[name]
    return out


def _is_header(row: Sequence[Any], first_cell: str) -> bool:
    return _cell(row).upper() == first_cell


def legacy_split(rows: Sequence[Sequence[Any]]) -> Dict[str, List[List[str]]]:
    """Старая разметка: секции разделялись только END-маркерами, а блок
    автоматизаций опознавался по строке-заголовку."""
    out: Dict[str, List[List[str]]] = {name: [] for name in BLOCKS}
    section: Optional[str] = GAME
    for row in rows:
        head = _cell(row).upper()
        if parse_marker(head):
            if section == GAME:
                section = VOTING
            elif section == VOTING:
                section = AUTOMATIONS
            else:
                section = None
            continue
        if _is_header(row, "АВТОМАТИЧЕСКОЕ СООБЩЕНИЕ"):
            section = AUTOMATIONS       # заголовок секции — вход в неё
            continue
        if _is_header(row, "ID ГОЛОСОВАНИЯ") or _is_header(row, "ТИП"):
            continue
        if section:
            out[section].append([str(c or "") for c in row])
    return out


def describe(rows: Sequence[Sequence[Any]]) -> str:
    """Короткая сводка для лога/админки: сколько строк в каждом блоке."""
    blocks = split(rows)
    mode = "блоки" if has_blocks(rows) else "старая разметка"
    parts = ", ".join(f"{name}: {len(blocks[name])}" for name in BLOCKS)
    return f"«Конфиг» ({mode}) — {parts}"
