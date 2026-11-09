"""Выбор бэкенда bot_state.db: локальный sqlite3 (дефолт) или удалённый
libSQL/sqld.

Зачем это есть
--------------
Вся история бота (сеансы, очереди job'ов, ask_user, дельты интеграций) живёт
одним файлом ``bot_state.db`` на одной машине. Чтобы инстанс Jarvis можно было
поднять и на Linux, и на Windows против ОДНОЙ истории (взаимозаменяемо — у
Telegram один поллер на токен), БД выносится в сетевой sqld (libSQL) на сервере.

Код завязан на идиомы ``sqlite3``: ``row["col"]``, ``cur.lastrowid``,
``PRAGMA table_info``, ``with conn:`` и перехват ``sqlite3.OperationalError``.
Клиент ``turso_serverless`` (чистый Python DB-API 2.0, встаёт на 3.14 без Rust —
в отличие от нативного ``libsql``) повторяет их не один в один: строки приходят
кортежами без доступа по имени, а исключения — своей иерархии. Здесь тонкий слой
совместимости, который делает удалённое соединение неотличимым для остального
кода от ``bot.db._ClosingConnection``.

Выбор бэкенда
-------------
- ``JARVIS_DB_BACKEND`` = ``sqlite`` (дефолт) | ``libsql``.
- Для ``libsql`` нужны ``JARVIS_DB_URL`` (``https://host``) и
  ``JARVIS_DB_AUTH_TOKEN`` (JWT).

Дефолт — sqlite3: поведение бота и тесты (запускаются с ``JARVIS_DOTENV=0`` и
чистым окружением) не меняются. Модуль намеренно лёгкий (только ``os`` +
ленивый импорт ``turso_serverless``), чтобы его одинаково безопасно тянули оба
процесса — бот и отдельный MCP-сервер.

Транзакции
----------
Удалённое соединение работает в autocommit (``isolation_level=None``): каждый
statement фиксируется сразу. Это совпадает с режимом MCP-сервера
(``sqlite3.connect(isolation_level=None)``) и защищает от потери записи вне
``with``-блока. Обратная сторона: ``with conn:`` на удалённом бэкенде не даёт
атомарности нескольких statement'ов. Для схемы это безопасно — вся эволюция в
``bot/db.py`` идемпотентна и аддитивна (``IF NOT EXISTS`` / ``ADD COLUMN``), а
прикладные записи однострочны.
"""

from __future__ import annotations

import logging
import os

# Импорт config грузит .env в os.environ (кроме JARVIS_DOTENV=0 в тестах),
# поэтому и бот, и отдельный процесс MCP-сервера видят JARVIS_DB_* одинаково.
import config  # noqa: F401  (нужен ради побочного эффекта load_dotenv)

logger = logging.getLogger(__name__)


def backend_name() -> str:
    """``sqlite`` (дефолт) | ``libsql`` из env ``JARVIS_DB_BACKEND``."""
    return (os.environ.get("JARVIS_DB_BACKEND") or "sqlite").strip().lower()


def is_remote() -> bool:
    return backend_name() == "libsql"


def _remote_config() -> tuple[str, str]:
    url = (os.environ.get("JARVIS_DB_URL") or "").strip()
    token = (os.environ.get("JARVIS_DB_AUTH_TOKEN") or "").strip()
    if not url:
        raise RuntimeError(
            "JARVIS_DB_BACKEND=libsql, но JARVIS_DB_URL не задан"
        )
    if not token:
        raise RuntimeError(
            "JARVIS_DB_BACKEND=libsql, но JARVIS_DB_AUTH_TOKEN не задан"
        )
    return url, token


class _Row:
    """Строка с доступом и по индексу (``row[0]``), и по имени (``row["col"]``),
    итерируемая и распаковываемая (``a, b = row``) — как ``sqlite3.Row``.

    ``turso_serverless`` отдаёт голые кортежи; имена колонок берём из
    ``cursor.description`` (DB-API: первый элемент каждого дескриптора — имя).
    """

    __slots__ = ("_values", "_names", "_index")

    def __init__(self, values: tuple, names: tuple[str, ...], index: dict[str, int]):
        self._values = values
        self._names = names
        self._index = index

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                return self._values[self._index[key]]
            except KeyError:
                raise IndexError(f"no such column: {key!r}") from None
        return self._values[key]

    def __iter__(self):
        return iter(self._values)

    def __len__(self):
        return len(self._values)

    def keys(self) -> list[str]:
        return list(self._names)

    def __contains__(self, key) -> bool:
        # sqlite3.Row проверяет членство по значениям; код пользуется только
        # `name in row.keys()`, но на всякий случай повторяем семантику Row.
        return key in self._values

    def __repr__(self) -> str:
        return f"_Row({dict(zip(self._names, self._values))!r})"


def _wrap_error(exc: Exception) -> Exception:
    """Любую ошибку бэкенда — в ``sqlite3.OperationalError``, которую ловит код.

    ``sqlite3.OperationalError`` — подкласс ``sqlite3.Error``, поэтому
    совместим и с ``except sqlite3.OperationalError``, и с ``except
    sqlite3.Error`` в существующих местах.
    """
    import sqlite3

    if isinstance(exc, sqlite3.Error):
        return exc
    return sqlite3.OperationalError(str(exc))


def _names_and_index(description) -> tuple[tuple[str, ...], dict[str, int]]:
    if not description:
        return (), {}
    names = tuple(d[0] for d in description)
    index: dict[str, int] = {}
    for i, name in enumerate(names):
        index.setdefault(name, i)  # первое вхождение при дубликатах имён
    return names, index


class _Cursor:
    """Обёртка курсора: оборачивает строки в ``_Row`` и переводит исключения."""

    def __init__(self, raw):
        self._raw = raw
        self._names: tuple[str, ...] = ()
        self._index: dict[str, int] = {}

    def execute(self, sql: str, params=()):
        if _is_noop_pragma(sql):
            self._names, self._index = (), {}
            return self
        try:
            self._raw.execute(sql, params)
        except Exception as exc:  # noqa: BLE001 — переводим в sqlite3.*
            raise _wrap_error(exc) from exc
        self._names, self._index = _names_and_index(self._raw.description)
        return self

    def _wrap(self, values):
        if values is None:
            return None
        return _Row(tuple(values), self._names, self._index)

    def fetchone(self):
        try:
            row = self._raw.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc
        return self._wrap(row)

    def fetchall(self):
        try:
            rows = self._raw.fetchall()
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc
        return [self._wrap(r) for r in rows]

    def fetchmany(self, size=None):
        try:
            rows = self._raw.fetchmany(size) if size is not None else self._raw.fetchmany()
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc
        return [self._wrap(r) for r in rows]

    def __iter__(self):
        return iter(self.fetchall())

    @property
    def lastrowid(self):
        return self._raw.lastrowid

    @property
    def rowcount(self):
        return self._raw.rowcount

    @property
    def description(self):
        return self._raw.description

    def close(self):
        try:
            self._raw.close()
        except Exception:  # noqa: BLE001
            logger.debug("remote cursor close failed", exc_info=True)


_NOOP_PRAGMA_PREFIXES = ("pragma journal_mode", "pragma synchronous")


def _is_noop_pragma(sql: str) -> bool:
    """``PRAGMA journal_mode``/``synchronous`` — про файловый WAL локального
    sqlite; на сервере sqld бессмысленны, глушим их, не доходя до сети."""
    return sql.strip().lower().startswith(_NOOP_PRAGMA_PREFIXES)


class RemoteConnection:
    """Соединение с sqld, совместимое по интерфейсу с ``bot.db._ClosingConnection``.

    - ``execute()`` — ярлык как у ``sqlite3.Connection`` (создаёт курсор).
    - строки — ``_Row`` (индекс + имя), исключения — ``sqlite3.OperationalError``.
    - ``with conn:`` закрывает соединение на выходе (на Windows незакрытое
      держит ресурс; на сервере — просто гигиена).
    - ``row_factory`` присваивается молча (код ставит ``sqlite3.Row``); мы и так
      всегда отдаём именованные строки, так что присваивание — no-op.
    """

    row_factory = None  # присваивается кодом; игнорируется (строки уже именованные)

    def __init__(self, url: str, token: str):
        import turso_serverless

        self._ts = turso_serverless
        try:
            # isolation_level=None — autocommit (см. docstring модуля).
            self._conn = turso_serverless.connect(
                url, auth_token=token, isolation_level=None
            )
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc

    def cursor(self) -> _Cursor:
        try:
            return _Cursor(self._conn.cursor())
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc

    def execute(self, sql: str, params=()) -> _Cursor:
        cur = self.cursor()
        return cur.execute(sql, params)

    def executescript(self, script: str):
        try:
            return self._conn.executescript(script)
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc

    def commit(self):
        try:
            self._conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc

    def rollback(self):
        try:
            self._conn.rollback()
        except Exception as exc:  # noqa: BLE001
            raise _wrap_error(exc) from exc

    def close(self):
        try:
            self._conn.close()
        except Exception:  # noqa: BLE001
            logger.debug("remote connection close failed", exc_info=True)

    def __enter__(self) -> "RemoteConnection":
        return self

    def __exit__(self, exc_type, exc, tb):
        # autocommit: фиксировать нечего. Повторяем контракт _ClosingConnection —
        # на выходе из блока закрываем соединение.
        self.close()
        return False


def connect() -> RemoteConnection:
    """Открыть удалённое соединение. Вызывать только при ``is_remote()``."""
    url, token = _remote_config()
    return RemoteConnection(url, token)
