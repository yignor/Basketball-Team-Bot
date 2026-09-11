#!/usr/bin/env python3
"""Голос тренера — в текст. Прямо на сервере, наружу ничего не уходит.

Тот же движок, что уже работает на этом сервере в мосте WhatsApp:
faster-whisper, модель small с int8-квантованием. На i7-9700T она идёт примерно
втрое быстрее реального времени — заметка на полминуты распознаётся секунд за
десять. Ключей и оплаты не требует: голос тренера о своих игроках остаётся на
его сервере.

**Модель берём у бота, а не из моста.** Мост живёт под другим пользователем, и
его папка боту закрыта. Модель (≈460 МБ) один раз копируется в data/whisper —
она в git не попадает.

**Память.** Загруженная модель занимает около гигабайта, а на этом сервере
крутятся и другие боты. Грузим лениво — при первой заметке — и выгружаем после
простоя: разбор игры идёт сеансом, минут двадцать подряд, а потом неделю
тишина. Держать гигабайт ради одного сеанса в неделю незачем.

**По одному.** Распознавание прожорливо и не потокобезопасно, а ядер боту
отдаём не все: рядом работают другие. Две заметки подряд встают в очередь.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
MODEL_NAME = os.environ.get("BOT_WHISPER_MODEL", "small")
MODEL_DIR = Path(os.environ.get("BOT_WHISPER_DIR", str(ROOT / "data" / "whisper")))
# Ядер — половина: мост WhatsApp берёт четыре, и вместе они не должны
# вытеснять боевые боты.
THREADS = int(os.environ.get("BOT_WHISPER_THREADS", "3"))
# Через сколько секунд простоя выгружать модель из памяти.
IDLE_UNLOAD = int(os.environ.get("BOT_WHISPER_IDLE", "900"))

_model = None
_used_at = 0.0
_lock = threading.Lock()


def available() -> Tuple[bool, str]:
    """Можно ли распознавать. (да/нет, почему нет — человеческими словами)."""
    try:
        import faster_whisper  # noqa: F401
    except ImportError:
        return False, ("распознавание голоса не установлено на сервере — "
                       "пришли комментарий текстом")
    if not any(MODEL_DIR.glob(f"models--*faster-whisper-{MODEL_NAME}*")):
        return False, ("модель распознавания не скопирована на сервер — "
                       "пришли комментарий текстом")
    return True, ""


def _load():
    global _model
    if _model is None:
        from faster_whisper import WhisperModel
        started = time.time()
        # local_files_only: модель уже лежит рядом, в сеть за ней не ходим —
        # иначе первая заметка ждала бы полгигабайта загрузки.
        _model = WhisperModel(MODEL_NAME, device="cpu", compute_type="int8",
                              cpu_threads=THREADS, download_root=str(MODEL_DIR),
                              local_files_only=True)
        logger.info("Распознавание: модель %s загружена за %.1f с",
                    MODEL_NAME, time.time() - started)
    return _model


def transcribe(path: str, language: str = "ru",
               hint: str = "") -> Tuple[str, float]:
    """Распознаёт файл. (текст, сколько секунд длилась запись).

    Язык задаём русский сразу: определять его на коротких фразах с фамилиями и
    баскетбольными словами — лишний шанс, что модель решит, будто это другой
    язык, и выдаст транслит.

    hint — подсказка модели: фамилии игроков и слова игры. Без неё «Гиря»
    выходит «гиря», а «пик-н-ролл» — чем угодно. Модель берёт из подсказки
    написание, но сама её в текст не вставляет."""
    global _used_at
    with _lock:
        model = _load()
        segments, info = model.transcribe(
            path, language=language, beam_size=5, vad_filter=True,
            vad_parameters={"min_silence_duration_ms": 500},
            initial_prompt=hint or None)
        text = " ".join(s.text.strip() for s in segments).strip()
        _used_at = time.time()
    return text, float(info.duration)


def unload_if_idle(now: Optional[float] = None) -> bool:
    """Выгружает модель, если ею давно не пользовались. True — выгрузили."""
    global _model
    if _model is None:
        return False
    if (now or time.time()) - _used_at < IDLE_UNLOAD:
        return False
    with _lock:
        _model = None
    logger.info("Распознавание: модель выгружена после простоя")
    return True
