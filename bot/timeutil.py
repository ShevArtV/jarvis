"""Время для БД: naive UTC, как его исторически пишут все таблицы.

``datetime.utcnow()`` устарел с Python 3.12; формат ISO-строк в БД менять
нельзя (их читают MCP-сервер и внешние интеграторы), поэтому tzinfo
отбрасываем."""

from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)
