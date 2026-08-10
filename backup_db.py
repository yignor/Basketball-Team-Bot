"""Резервная копия базы: на диск сервера и на Google Диск.

Вся работа бота живёт в одном файле SQLite — 13 МБ, 44 таблицы. Часть данных
восстановима (протоколы и статистику можно скачать из API лиг заново, служебные
записи и оплаты уезжают в Google-листы), но часть существует ТОЛЬКО здесь:
составы фэнтези, которые собирали участники, и зафиксированные по играм очки.
Очки принципиально не пересчитываются задним числом — значит, потеряв файл, их
не получить ничем. Ради этого всё и затевается.

Как снимаем: не `cp`. База работает в режиме WAL, и простое копирование файла
во время записи даёт битую копию без части свежих транзакций. Родной
`sqlite3.backup()` делает согласованный снимок на живой базе, не останавливая
бота. Каждую копию тут же проверяем `PRAGMA integrity_check` — резервная копия,
про которую неизвестно, целая ли она, хуже честного отсутствия.

Где храним: две независимые площадки. Локальные копии спасают от «сломал сам»
(снёс таблицу, кривая миграция), копии на Диске — от «умер сервер». Одна без
другой закрывает только половину бед.

Хранение: 7 последних ежедневных плюс воскресные за 8 недель. Ежедневных хватает
заметить свежую поломку, воскресные ловят ту, что тихо жила месяц.

Про Google Диск: заливаем в папку, доступную владельцу таблицы. Файлы приватные —
ими не делятся ни с кем, кроме него.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import sqlite3
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

DB_PATH = Path(os.getenv("BACKUP_DB_PATH", "")) if os.getenv("BACKUP_DB_PATH") \
    else Path(__file__).parent / "data" / "bot.db"
LOCAL_DIR = Path(os.getenv("BACKUP_DIR", str(Path(__file__).parent / "backups")))

KEEP_DAILY = int(os.getenv("BACKUP_KEEP_DAILY", "7"))
KEEP_WEEKLY = int(os.getenv("BACKUP_KEEP_WEEKLY", "8"))

# Папка на Диске: можно задать готовую (BACKUP_DRIVE_FOLDER_ID), иначе заводим
# свою по имени и открываем доступ владельцу таблицы.
DRIVE_FOLDER_ID = os.getenv("BACKUP_DRIVE_FOLDER_ID", "").strip()
DRIVE_FOLDER_NAME = os.getenv("BACKUP_DRIVE_FOLDER_NAME",
                              "Basketball Bot — резервные копии")
DRIVE_OWNER = os.getenv("BACKUP_DRIVE_OWNER", "").strip()
DRIVE_KEEP = int(os.getenv("BACKUP_DRIVE_KEEP", "14"))

PREFIX = "bot-db-"
SUFFIX = ".sqlite.gz"

API = "https://www.googleapis.com/drive/v3/files"
UPLOAD_API = "https://www.googleapis.com/upload/drive/v3/files"


# ─────────────────────────── снимок ────────────────────────────────────────


def snapshot(dst: Path) -> Dict[str, Any]:
    """Согласованная копия живой базы + проверка целостности.

    `backup()` идёт страницами и сам разбирается с WAL, поэтому бот в это время
    может писать. Проверку делаем на КОПИИ, а не на оригинале: нас интересует,
    годится ли то, что мы только что сняли."""
    if not DB_PATH.exists():
        raise FileNotFoundError(f"База не найдена: {DB_PATH}")
    src = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=30)
    out = sqlite3.connect(dst)
    try:
        src.backup(out)
    finally:
        out.close()
        src.close()

    check = sqlite3.connect(dst)
    try:
        verdict = check.execute("PRAGMA integrity_check").fetchone()[0]
        tables = check.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'").fetchone()[0]
        rows = check.execute(
            "SELECT COUNT(*) FROM game_player_stats").fetchone()[0]
    finally:
        check.close()
    if verdict != "ok":
        raise RuntimeError(f"Копия повреждена: {verdict}")
    return {"tables": tables, "stats_rows": rows}


def make_local() -> Dict[str, Any]:
    """Снимает базу, жмёт и кладёт в папку копий. Возвращает описание."""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    target = LOCAL_DIR / f"{PREFIX}{stamp}{SUFFIX}"
    with tempfile.TemporaryDirectory() as tmp:
        raw = Path(tmp) / "snapshot.db"
        info = snapshot(raw)
        # Сжимаем во временный файл и переименовываем: если процесс умрёт на
        # середине, в папке не останется обрезанного архива, который потом
        # примут за годную копию.
        part = target.with_suffix(target.suffix + ".part")
        with open(raw, "rb") as fin, gzip.open(part, "wb", compresslevel=6) as fout:
            shutil.copyfileobj(fin, fout, length=1024 * 1024)
        part.replace(target)
        info["raw_bytes"] = raw.stat().st_size
    info["path"] = target
    info["bytes"] = target.stat().st_size
    return info


# ─────────────────────────── ротация ───────────────────────────────────────


def _day_of(path: Path) -> Optional[date]:
    try:
        return date.fromisoformat(path.name[len(PREFIX):len(PREFIX) + 10])
    except ValueError:
        return None


def keep_set(days: List[date], today: Optional[date] = None) -> set:
    """Какие даты оставляем: последние N ежедневных + воскресенья за M недель.

    Воскресные держим дольше не из любви к неделям: поломку, которая тихо жила
    месяц, ежедневными копиями уже не отловить — все семь будут с ней."""
    today = today or date.today()
    keep = set(sorted(days, reverse=True)[:KEEP_DAILY])
    weekly = [d for d in days if d.weekday() == 6
              and (today - d).days <= KEEP_WEEKLY * 7]
    keep |= set(sorted(weekly, reverse=True)[:KEEP_WEEKLY])
    return keep


def rotate_local() -> List[Path]:
    """Убирает лишние копии. Возвращает список удалённых."""
    files = {p: _day_of(p) for p in LOCAL_DIR.glob(f"{PREFIX}*{SUFFIX}")}
    files = {p: d for p, d in files.items() if d}
    keep = keep_set(list(files.values()))
    dropped = []
    for path, day in files.items():
        if day not in keep:
            path.unlink()
            dropped.append(path)
    return dropped


# ─────────────────────────── Google Диск ───────────────────────────────────


def _session():
    """Авторизованный HTTP к Диску под тем же служебным аккаунтом, что и листы.

    Права `drive` в нём уже есть — отдельных ключей заводить не надо."""
    import google.auth.transport.requests
    import requests
    from google.oauth2.service_account import Credentials

    raw = os.getenv("GOOGLE_SHEETS_CREDENTIALS", "")
    if not raw:
        raise RuntimeError("GOOGLE_SHEETS_CREDENTIALS не задан")
    creds = Credentials.from_service_account_info(
        json.loads(raw), scopes=["https://www.googleapis.com/auth/drive"])
    creds.refresh(google.auth.transport.requests.Request())
    s = requests.Session()
    s.headers["Authorization"] = f"Bearer {creds.token}"
    return s, json.loads(raw).get("client_email", "")


def _check(resp) -> Dict[str, Any]:
    if resp.status_code >= 400:
        raise RuntimeError(f"Диск ответил {resp.status_code}: {resp.text[:300]}")
    return resp.json() if resp.content else {}


def folder_id(session) -> str:
    """Папка для копий: заданная в настройках либо своя, по имени."""
    if DRIVE_FOLDER_ID:
        return DRIVE_FOLDER_ID
    q = ("mimeType='application/vnd.google-apps.folder' and trashed=false and "
         f"name='{DRIVE_FOLDER_NAME}'")
    found = _check(session.get(API, params={"q": q, "fields": "files(id,name)"}))
    files = found.get("files") or []
    if files:
        return files[0]["id"]
    made = _check(session.post(API, json={
        "name": DRIVE_FOLDER_NAME,
        "mimeType": "application/vnd.google-apps.folder"}))
    fid = made["id"]
    if DRIVE_OWNER:
        # Доступ ровно одному человеку — владельцу таблицы. Ссылками не делимся:
        # в базе телефоны команды, оплаты и telegram-id.
        _check(session.post(f"{API}/{fid}/permissions",
                            json={"type": "user", "role": "writer",
                                  "emailAddress": DRIVE_OWNER}))
        print(f"📂 Завёл папку «{DRIVE_FOLDER_NAME}» и открыл доступ {DRIVE_OWNER}")
    return fid


def upload(session, path: Path, parent: str) -> Dict[str, Any]:
    """Кладёт архив на Диск. Файл приватный: видит только тот, кому открыт."""
    meta = {"name": path.name, "parents": [parent]}
    with open(path, "rb") as f:
        files = {
            "metadata": ("metadata", json.dumps(meta), "application/json"),
            "file": (path.name, f, "application/gzip"),
        }
        resp = session.post(UPLOAD_API,
                            params={"uploadType": "multipart",
                                    "fields": "id,name,size,createdTime"},
                            files=files)
    return _check(resp)


def rotate_drive(session, parent: str) -> List[str]:
    """Держим на Диске столько же поколений, сколько локально."""
    q = f"'{parent}' in parents and trashed=false and name contains '{PREFIX}'"
    found = _check(session.get(API, params={
        "q": q, "fields": "files(id,name)", "orderBy": "name desc",
        "pageSize": 200}))
    files = found.get("files") or []
    days = {}
    for f in files:
        d = _day_of(Path(f["name"]))
        if d:
            days[f["id"]] = d
    keep = keep_set(list(days.values()))
    dropped = []
    for fid, day in days.items():
        if day not in keep:
            session.delete(f"{API}/{fid}")
            dropped.append(fid)
    return dropped


# ─────────────────────────── запуск ────────────────────────────────────────


def human(n: int) -> str:
    return f"{n / 1024 / 1024:.1f} МБ" if n >= 1024 * 1024 else f"{n / 1024:.0f} КБ"


def main() -> int:
    ap = argparse.ArgumentParser(description="Резервная копия базы бота")
    ap.add_argument("--no-drive", action="store_true", help="только локально")
    ap.add_argument("--list", action="store_true", help="показать, что уже есть")
    args = ap.parse_args()

    if args.list:
        local = sorted(LOCAL_DIR.glob(f"{PREFIX}*{SUFFIX}"))
        print(f"Локально ({LOCAL_DIR}): {len(local)}")
        for p in local:
            print(f"  {p.name}  {human(p.stat().st_size)}")
        if not args.no_drive:
            try:
                session, _ = _session()
                parent = folder_id(session)
                q = f"'{parent}' in parents and trashed=false"
                got = _check(session.get(API, params={
                    "q": q, "fields": "files(name,size,createdTime)",
                    "orderBy": "name desc", "pageSize": 200}))
                rows = got.get("files") or []
                print(f"\nНа Диске: {len(rows)}")
                for f in rows:
                    print(f"  {f['name']}  {human(int(f.get('size') or 0))}")
            except Exception as e:
                print(f"⚠️ Диск недоступен: {e}")
        return 0

    try:
        info = make_local()
    except Exception as e:
        print(f"❌ Копия не снялась: {e}")
        _report(f"снимок не снялся: {e}")
        return 1
    print(f"✅ Копия: {info['path'].name}  {human(info['bytes'])} "
          f"(из {human(info['raw_bytes'])}), таблиц {info['tables']}, "
          f"строк статистики {info['stats_rows']}")

    for p in rotate_local():
        print(f"🧹 Убрал старую: {p.name}")

    if args.no_drive:
        return 0

    try:
        session, who = _session()
        parent = folder_id(session)
        got = upload(session, info["path"], parent)
        print(f"☁️ На Диске: {got.get('name')} ({human(int(got.get('size') or 0))}), "
              f"служебный аккаунт {who}")
        for _ in rotate_drive(session, parent):
            print("🧹 Убрал старую копию на Диске")
    except Exception as e:
        # Локальная копия уже есть — это не провал ночи, но знать надо.
        print(f"⚠️ На Диск не уехало: {e}")
        _report(f"копия не уехала на Диск: {e}")
        return 2
    return 0


def _report(text: str) -> None:
    """Кладёт беду в лист «Ошибки» — туда же, куда и остальные сбои бота."""
    try:
        import sheets_cache
        sheets_cache.report_error("backup_db", text, None)
    except Exception:
        pass


if __name__ == "__main__":
    sys.exit(main())
