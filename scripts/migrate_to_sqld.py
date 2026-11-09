#!/usr/bin/env python3
"""Миграция локального ``bot_state.db`` (SQLite) в удалённый sqld (libSQL).

Фаза 3 выноса БД Jarvis: переносит ВСЮ историю бота (сеансы, очереди job'ов,
ask_user, логи, дельты интеграций, таблицы плагинов) из файла в сетевую БД, чтобы
инстансы на разных машинах ходили в одну историю.

ВАЖНО — консистентный снимок
----------------------------
Бот пишет в ``bot_state.db`` непрерывно. Мигрируйте снимок, в который никто не
пишет, иначе часть строк потеряется:
  - либо остановите ``jarvis-bot`` на время миграции (по правилу проекта — только
    с явного разрешения оператора);
  - либо мигрируйте КОПИЮ файла, снятую в момент затишья:
        sqlite3 bot_state.db ".backup /tmp/bot_state.snapshot.db"
    и передайте её через ``--source /tmp/bot_state.snapshot.db``.

Назначение (куда переносим)
---------------------------
Берётся из env, как у самого бота:
  JARVIS_DB_URL         — ``https://host`` (реальный sqld) ИЛИ путь к .db-файлу
                          (сухой прогон sqlite→sqlite, без сети).
  JARVIS_DB_AUTH_TOKEN  — JWT (нужен только для http(s)-назначения).

Что делает
----------
1. Снимает схему из ИСХОДНИКА (таблицы + индексы из ``sqlite_master``) и
   воссоздаёт её в назначении (идемпотентно; с ``--reset`` — сносит таблицы
   назначения и создаёт заново).
2. Копирует строки таблица-за-таблицей пакетно, СОХРАНЯЯ значения всех колонок,
   включая первичные ключи (id сеансов/job'ов/ask остаются прежними —
   AUTOINCREMENT сам подтянет ``sqlite_sequence`` на вставках).
3. Сверяет число строк в каждой таблице (источник == назначение).

Примеры
-------
  # сухой прогон в локальный sqlite (ничего сетевого не трогает)
  JARVIS_DB_URL=/tmp/dry.db python scripts/migrate_to_sqld.py --reset

  # реальная миграция в опубликованный sqld
  JARVIS_DB_URL=https://jarvis-db.queuewarden.ru \
  JARVIS_DB_AUTH_TOKEN=<JWT> \
  python scripts/migrate_to_sqld.py --source /tmp/bot_state.snapshot.db --reset
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BATCH = 500


def _user_tables(conn: sqlite3.Connection) -> list[str]:
    """Пользовательские таблицы источника: без служебных ``sqlite_*`` и без
    временного ``*_old`` (артефакт миграции схемы sessions в bot/db.py)."""
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%\\_old' ESCAPE '\\' "
        "ORDER BY name"
    ).fetchall()
    return [r[0] for r in rows]


def _schema_sql(conn: sqlite3.Connection, tables: set[str]) -> tuple[list[str], list[str]]:
    """DDL таблиц и индексов источника (в порядке создания)."""
    table_ddl, index_ddl = [], []
    for name, typ, sql in conn.execute(
        "SELECT name, type, sql FROM sqlite_master "
        "WHERE type IN ('table','index') AND sql IS NOT NULL ORDER BY rowid"
    ).fetchall():
        if typ == "table" and name in tables:
            table_ddl.append(sql)
        elif typ == "index":
            # индекс относится к пользовательской таблице?
            tbl = conn.execute(
                "SELECT tbl_name FROM sqlite_master WHERE type='index' AND name=?",
                (name,),
            ).fetchone()
            if tbl and tbl[0] in tables:
                index_ddl.append(sql)
    return table_ddl, index_ddl


# --- Назначение: единый интерфейс поверх turso_serverless или sqlite3 ---------

class _Dest:
    def __init__(self, url: str, token: str | None):
        self.is_remote = url.startswith(("http://", "https://"))
        if self.is_remote:
            import turso_serverless
            self._conn = turso_serverless.connect(url, auth_token=token)
            self.kind = f"sqld {url}"
        else:
            self._conn = sqlite3.connect(url)
            self.kind = f"sqlite {url}"

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def executemany(self, sql, seq):
        return self._conn.executemany(sql, seq)

    def existing_tables(self) -> set[str]:
        rows = self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        return {r[0] for r in rows}

    def count(self, table: str) -> int:
        return self._conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Миграция bot_state.db -> sqld")
    ap.add_argument("--source", help="путь к исходному sqlite (умолч. bot.settings.DB_PATH)")
    ap.add_argument("--reset", action="store_true",
                    help="снести таблицы назначения перед переносом (чистая миграция)")
    ap.add_argument("--dry-run", action="store_true",
                    help="только показать план (ничего не писать)")
    args = ap.parse_args()

    source = args.source
    if not source:
        from bot.settings import DB_PATH
        source = DB_PATH
    if not os.path.exists(source):
        print(f"ОШИБКА: источник не найден: {source}", file=sys.stderr)
        return 2

    url = os.environ.get("JARVIS_DB_URL", "").strip()
    token = os.environ.get("JARVIS_DB_AUTH_TOKEN", "").strip() or None
    if not url:
        print("ОШИБКА: задайте JARVIS_DB_URL (https://host или путь к .db)", file=sys.stderr)
        return 2

    src = sqlite3.connect(source)
    tables = _user_tables(src)
    table_ddl, index_ddl = _schema_sql(src, set(tables))
    print(f"источник:   {source}")
    print(f"таблиц к переносу: {len(tables)} -> {tables}")

    counts = {t: src.execute(f'SELECT COUNT(*) FROM "{t}"').fetchone()[0] for t in tables}
    total = sum(counts.values())
    print("строк в источнике:", counts, f"(всего {total})")

    if args.dry_run:
        print("[dry-run] выход без записи")
        return 0

    dst = _Dest(url, token)
    print(f"назначение: {dst.kind}")

    if args.reset:
        for t in tables:
            dst.execute(f'DROP TABLE IF EXISTS "{t}"')
        dst.commit()
        print(f"reset: снесено таблиц назначения: {len(tables)}")

    # 1. схема
    present = dst.existing_tables()
    for ddl in table_ddl:
        dst.execute(ddl)
    for ddl in index_ddl:
        dst.execute(ddl)
    dst.commit()
    print(f"схема в назначении готова (было таблиц: {len(present)})")

    # 2. строки
    for t in tables:
        cols = [r[1] for r in src.execute(f'PRAGMA table_info("{t}")').fetchall()]
        if counts[t] == 0:
            print(f"  {t}: 0 строк, пропуск")
            continue
        collist = ", ".join(f'"{c}"' for c in cols)
        ph = ", ".join("?" * len(cols))
        insert = f'INSERT INTO "{t}" ({collist}) VALUES ({ph})'
        cur = src.execute(f'SELECT {collist} FROM "{t}"')
        moved = 0
        while True:
            batch = cur.fetchmany(BATCH)
            if not batch:
                break
            dst.executemany(insert, [tuple(r) for r in batch])
            dst.commit()
            moved += len(batch)
        print(f"  {t}: перенесено {moved}")

    # 3. сверка
    print("=== сверка числа строк ===")
    ok = True
    for t in tables:
        s, d = counts[t], dst.count(t)
        mark = "OK" if s == d else "!! РАСХОЖДЕНИЕ"
        if s != d:
            ok = False
        print(f"  {t}: источник={s} назначение={d} {mark}")

    src.close()
    dst.close()
    print("\n=== МИГРАЦИЯ ЗАВЕРШЕНА ===" if ok else "\n=== ЕСТЬ РАСХОЖДЕНИЯ — ПРОВЕРЬТЕ ===")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
