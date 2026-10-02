"""Best-effort session context usage inspection for Jarvis engines."""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Приблизительные цены (USD за 1M токенов): (input, output, cache_write, cache_read).
# Официального total_cost_usd в jsonl-транскрипте нет (он есть только в живом
# stream-json от CLI и никуда не сохраняется) — считаем сами по учтённым тарифам.
# Для моделей не из списка цена не считается (см. PeriodUsage.unpriced_tokens).
MODEL_PRICING: dict[str, tuple[float, float, float, float]] = {
    "claude-sonnet-5": (3.00, 15.00, 3.75, 0.30),
    "claude-opus-4-8": (15.00, 75.00, 18.75, 1.50),
    "claude-opus-4-8[1m]": (15.00, 75.00, 18.75, 1.50),
    "claude-haiku-4-5-20251001": (1.00, 5.00, 1.25, 0.10),
}


@dataclass
class SessionUsage:
    engine: str
    session_id: str
    source: str
    context_tokens: int | None = None
    input_tokens: int | None = None
    cache_read_tokens: int | None = None
    cache_write_tokens: int | None = None
    output_tokens: int | None = None
    estimated_tokens: int | None = None
    bytes_size: int | None = None
    path: str | None = None
    note: str | None = None

    @property
    def threshold_tokens(self) -> int | None:
        return self.context_tokens or self.estimated_tokens

    @property
    def is_estimate(self) -> bool:
        return self.context_tokens is None and self.estimated_tokens is not None


def inspect_session_usage(engine: str, session_id: str, cwd: str | None) -> SessionUsage:
    engine = (engine or "").strip().lower()
    if engine == "claude":
        return _inspect_claude(session_id, cwd)
    if engine == "codex":
        return _inspect_codex(session_id)
    if engine == "opencode":
        return _inspect_opencode(session_id)
    return SessionUsage(engine=engine, session_id=session_id, source="unknown")


@dataclass
class ModelTotals:
    n_messages: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_write_tokens: int = 0
    cache_read_tokens: int = 0


@dataclass
class PeriodUsage:
    """Сумма usage по ВСЕМ сообщениям сессии (и её сабагентов) за период —
    в отличие от SessionUsage, который берёт только последний снэпшот
    (нужен для порога автокомпакта, а не для учёта расхода)."""

    engine: str
    session_id: str
    since: datetime | None
    until: datetime | None
    by_model: dict[str, ModelTotals] = field(default_factory=dict)
    files_scanned: list[str] = field(default_factory=list)
    note: str | None = None

    @property
    def n_messages(self) -> int:
        return sum(m.n_messages for m in self.by_model.values())

    @property
    def input_tokens(self) -> int:
        return sum(m.input_tokens for m in self.by_model.values())

    @property
    def output_tokens(self) -> int:
        return sum(m.output_tokens for m in self.by_model.values())

    @property
    def cache_write_tokens(self) -> int:
        return sum(m.cache_write_tokens for m in self.by_model.values())

    @property
    def cache_read_tokens(self) -> int:
        return sum(m.cache_read_tokens for m in self.by_model.values())

    @property
    def cost_usd(self) -> float:
        total = 0.0
        for model, m in self.by_model.items():
            price = MODEL_PRICING.get(model)
            if not price:
                continue
            p_in, p_out, p_cw, p_cr = price
            total += (
                m.input_tokens * p_in
                + m.output_tokens * p_out
                + m.cache_write_tokens * p_cw
                + m.cache_read_tokens * p_cr
            ) / 1_000_000
        return total

    @property
    def unpriced_models(self) -> list[str]:
        return [m for m in self.by_model if m not in MODEL_PRICING]


def _parse_ts(raw: Any) -> datetime | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _claude_usage_dedupe_key(row: dict[str, Any], msg: dict[str, Any]) -> str | None:
    for key in ("requestId", "request_id"):
        value = row.get(key)
        if isinstance(value, str) and value:
            return f"request:{value}"
    value = msg.get("id")
    if isinstance(value, str) and value:
        return f"message:{value}"
    value = row.get("uuid")
    if isinstance(value, str) and value:
        return f"uuid:{value}"
    return None


def aggregate_claude_usage(
    session_id: str,
    cwd: str | None,
    since: datetime | None = None,
    until: datetime | None = None,
    include_subagents: bool = True,
) -> PeriodUsage:
    """Просуммировать usage всех ответов ассистента в jsonl-транскрипте данной
    сессии (+ её сабагентов, запущенных через Task) за [since, until].

    Область — конкретный ``session_id``, а не вся папка проекта: если та же
    cwd используется другим топиком/терминальной сессией, её токены сюда не
    попадут. Обратная сторона: сессии topика ДО последнего /new не видны —
    в БД бота хранится только текущий session_id на топик."""
    effective_cwd = cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))
    session_dir = _claude_sessions_dir_for(effective_cwd)
    result = PeriodUsage(engine="claude", session_id=session_id, since=since, until=until)

    main_file = session_dir / f"{session_id}.jsonl"
    files = [main_file] if main_file.is_file() else []
    if include_subagents:
        sub_dir = session_dir / session_id / "subagents"
        if sub_dir.is_dir():
            files.extend(sorted(sub_dir.glob("*.jsonl")))

    if not files:
        result.note = f"no transcript files found under {session_dir}"
        return result

    seen_usage_keys: set[str] = set()
    duplicate_usage_rows = 0
    for path in files:
        result.files_scanned.append(str(path))
        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or '"usage"' not in line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    ts = _parse_ts(row.get("timestamp"))
                    if ts is None:
                        continue
                    if since is not None and ts < since:
                        continue
                    if until is not None and ts > until:
                        continue
                    msg = row.get("message")
                    if not (isinstance(msg, dict) and msg.get("role") == "assistant"):
                        continue
                    usage = msg.get("usage")
                    if not isinstance(usage, dict):
                        continue
                    dedupe_key = _claude_usage_dedupe_key(row, msg)
                    if dedupe_key is not None:
                        scoped_key = f"{row.get('sessionId') or session_id}:{dedupe_key}"
                        if scoped_key in seen_usage_keys:
                            duplicate_usage_rows += 1
                            continue
                        seen_usage_keys.add(scoped_key)
                    model = msg.get("model") or "unknown"
                    bucket = result.by_model.setdefault(model, ModelTotals())
                    bucket.n_messages += 1
                    bucket.input_tokens += int(usage.get("input_tokens") or 0)
                    bucket.output_tokens += int(usage.get("output_tokens") or 0)
                    bucket.cache_write_tokens += int(usage.get("cache_creation_input_tokens") or 0)
                    bucket.cache_read_tokens += int(usage.get("cache_read_input_tokens") or 0)
        except OSError as exc:
            result.note = f"read failed for {path}: {exc}"
    if duplicate_usage_rows and result.note is None:
        result.note = f"deduplicated {duplicate_usage_rows} repeated Claude usage rows"
    return result


def aggregate_usage(
    engine: str,
    session_id: str,
    cwd: str | None,
    since: datetime | None = None,
    until: datetime | None = None,
) -> PeriodUsage:
    engine = (engine or "").strip().lower()
    if engine == "claude":
        return aggregate_claude_usage(session_id, cwd, since, until)
    return PeriodUsage(
        engine=engine, session_id=session_id, since=since, until=until,
        note=f"period aggregation not implemented for engine={engine!r}",
    )


def _estimate_tokens_from_bytes(size: int | None) -> int | None:
    if not size:
        return None
    # Conservative approximation for mixed RU/EN/code JSONL transcripts.
    return max(1, size // 4)


def _claude_sessions_dir_for(cwd: str) -> Path:
    # Та же кодировка, что у Claude CLI и engines.claude_engine._sessions_dir_for.
    encoded = re.sub(r"[^A-Za-z0-9]", "-", cwd)
    return Path.home() / ".claude" / "projects" / encoded


def _inspect_claude(session_id: str, cwd: str | None) -> SessionUsage:
    effective_cwd = cwd or os.environ.get("CLAUDE_CWD", str(Path.home()))
    path = _claude_sessions_dir_for(effective_cwd) / f"{session_id}.jsonl"
    usage = SessionUsage(
        engine="claude",
        session_id=session_id,
        source="claude-jsonl",
        path=str(path),
    )
    if not path.is_file():
        usage.note = "session file not found"
        return usage
    try:
        usage.bytes_size = path.stat().st_size
        last_usage: dict[str, Any] | None = None
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                msg = row.get("message")
                if isinstance(msg, dict) and isinstance(msg.get("usage"), dict):
                    last_usage = msg["usage"]
        if last_usage:
            inp = _as_int(last_usage.get("input_tokens"))
            cache_read = _as_int(last_usage.get("cache_read_input_tokens"))
            cache_write = _as_int(last_usage.get("cache_creation_input_tokens"))
            out = _as_int(last_usage.get("output_tokens"))
            usage.input_tokens = inp
            usage.cache_read_tokens = cache_read
            usage.cache_write_tokens = cache_write
            usage.output_tokens = out
            usage.context_tokens = sum(v or 0 for v in (inp, cache_read, cache_write))
        else:
            usage.estimated_tokens = _estimate_tokens_from_bytes(usage.bytes_size)
            usage.source = "claude-jsonl-estimate"
            usage.note = "usage block not found; estimated from file size"
    except OSError as exc:
        usage.note = f"read failed: {exc}"
    return usage


def _codex_sessions_root() -> Path:
    return Path(os.environ.get("CODEX_SESSIONS_DIR", Path.home() / ".codex" / "sessions"))


def _inspect_codex(session_id: str) -> SessionUsage:
    usage = SessionUsage(engine="codex", session_id=session_id, source="codex-jsonl-estimate")
    if session_id.startswith("placeholder-"):
        usage.note = "placeholder session; codex has not returned real thread_id yet"
        return usage
    root = _codex_sessions_root()
    if not root.is_dir():
        usage.note = "sessions root not found"
        return usage
    path = None
    try:
        for match in root.rglob(f"rollout-*-{session_id}.jsonl"):
            path = match
            break
    except OSError as exc:
        usage.note = f"scan failed: {exc}"
        return usage
    if path is None or not path.is_file():
        usage.note = "session file not found"
        return usage
    usage.path = str(path)
    try:
        usage.bytes_size = path.stat().st_size
        usage.estimated_tokens = _estimate_tokens_from_bytes(usage.bytes_size)
        usage.note = "codex local session log does not expose stable usage; estimated from file size"
    except OSError as exc:
        usage.note = f"stat failed: {exc}"
    return usage


def _inspect_opencode(session_id: str) -> SessionUsage:
    usage = SessionUsage(engine="opencode", session_id=session_id, source="opencode-db")
    db_path = Path(os.environ.get("OPENCODE_DB", Path.home() / ".local/share/opencode/opencode.db"))
    usage.path = str(db_path)
    if session_id.startswith("placeholder-"):
        usage.note = "placeholder session; opencode has not returned real session id yet"
        return usage
    if not db_path.is_file():
        usage.note = "opencode db not found"
        return usage
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT tokens_input, tokens_output, tokens_cache_read, "
            "tokens_cache_write FROM session WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row:
            usage.input_tokens = _as_int(row["tokens_input"])
            usage.output_tokens = _as_int(row["tokens_output"])
            usage.cache_read_tokens = _as_int(row["tokens_cache_read"])
            usage.cache_write_tokens = _as_int(row["tokens_cache_write"])
        msg = conn.execute(
            "SELECT data FROM message WHERE session_id = ? "
            "ORDER BY time_updated DESC LIMIT 20",
            (session_id,),
        ).fetchall()
        for item in msg:
            try:
                data = json.loads(item["data"])
            except (TypeError, json.JSONDecodeError):
                continue
            tokens = data.get("tokens")
            if isinstance(tokens, dict):
                usage.context_tokens = _as_int(tokens.get("total"))
                usage.input_tokens = _as_int(tokens.get("input")) or usage.input_tokens
                usage.output_tokens = _as_int(tokens.get("output")) or usage.output_tokens
                cache = tokens.get("cache")
                if isinstance(cache, dict):
                    usage.cache_read_tokens = _as_int(cache.get("read")) or usage.cache_read_tokens
                    usage.cache_write_tokens = _as_int(cache.get("write")) or usage.cache_write_tokens
                break
        if row is None and usage.context_tokens is None:
            usage.note = "session row not found"
    except sqlite3.Error as exc:
        usage.note = f"db read failed: {exc}"
    finally:
        try:
            conn.close()
        except Exception:
            logger.debug("cannot close sqlite connection", exc_info=True)
    return usage


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
