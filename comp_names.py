#!/usr/bin/env python3
"""
Справочник названий соревнований по CompID и хелперы для доступа.

Использование:
    from comp_names import get_comp_name, register_comp_name
    name = get_comp_name(107896)  # вернет сохранённое имя или пустую строку

Вы можете расширять словарь вручную ниже, либо вызывать register_comp_name
во время рантайма после получения данных Widget/CompIssue/{CompID}.
"""

# Keep-alive note: repository activity marker.
from typing import Dict, Optional

_COMP_NAMES: Dict[int, str] = {
    # Примеры: заполните по мере необходимости
    # 107896: "Лига Развития — Группа A",
    # 109465: "Первая Лига — Группа A",
}

def get_comp_name(comp_id: Optional[int]) -> str:
    if not isinstance(comp_id, int):
        return ""
    return _COMP_NAMES.get(comp_id, "")

def register_comp_name(comp_id: int, name: str) -> None:
    if isinstance(comp_id, int) and name:
        _COMP_NAMES[comp_id] = name.strip()

