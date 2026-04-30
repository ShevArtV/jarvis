#!/usr/bin/env python3
"""Синхронизация пользовательских правил → ~/.codex/AGENTS.md.

Формирует файл user-global инструкций для Codex CLI из источников, которые
поддерживает пользователь для Claude:

  1) ~/.claude/CLAUDE.md — глобальные правила (с раскрытием @~/... импортов inline,
     т.к. синтаксис @ у Claude специфичен).
  2) ~/.claude/projects/-home-shevartv/memory/MEMORY.md как «индекс» + содержимое
     ВСЕХ файлов memory/*.md — полный инлайн.
  3) ~/projects/knowledge-base/AGENTS.md и README.md — основные правила и индекс
     единой базы знаний.

Идемпотентен: повторный запуск перезаписывает ~/.codex/AGENTS.md.

Использование:
    python3 scripts/sync_codex_knowledge.py

Опционально:
    CODEX_AGENTS_MD=/custom/path python3 scripts/sync_codex_knowledge.py
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HOME = Path.home()

# Источники.
CLAUDE_MD = HOME / ".claude" / "CLAUDE.md"
MEMORY_DIR = HOME / ".claude" / "projects" / "-home-shevartv" / "memory"
KNOWLEDGE_BASE = HOME / "projects" / "knowledge-base"
KNOWLEDGE_BASE_AGENTS = KNOWLEDGE_BASE / "AGENTS.md"
KNOWLEDGE_BASE_README = KNOWLEDGE_BASE / "README.md"

# Куда писать. Можно переопределить через env (например, CODEX_HOME другой).
DEFAULT_TARGET = HOME / ".codex" / "AGENTS.md"

# Синтаксис импорта Claude: строки вида `@~/path/to/file.md`.
IMPORT_RE = re.compile(r"^\s*@(~?/[^\s]+\.md)\s*$", re.MULTILINE)


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""
    except OSError as exc:
        print(f"[warn] не смог прочитать {path}: {exc}", file=sys.stderr)
        return ""


def _expand_path(raw: str) -> Path:
    if raw.startswith("~/"):
        return HOME / raw[2:]
    if raw == "~":
        return HOME
    return Path(raw)


def _expand_claude_imports(text: str, source: Path, depth: int = 0) -> str:
    """Раскрывает @~/path.md → содержимое файла inline. Защита от циклов/глубины."""
    if depth > 5:
        return text

    def replace(match: re.Match) -> str:
        raw_path = match.group(1)
        target = _expand_path(raw_path)
        body = _read(target)
        if not body:
            return f"<!-- missing import: {raw_path} -->"
        # Рекурсивно раскрываем импорты внутри.
        expanded = _expand_claude_imports(body, target, depth=depth + 1)
        header = f"\n\n<!-- begin import: {raw_path} -->\n"
        footer = f"\n<!-- end import: {raw_path} -->\n"
        return header + expanded + footer

    return IMPORT_RE.sub(replace, text)


def _first_descriptive_line(text: str) -> str:
    """Вытаскивает first meaningful line: description из YAML frontmatter (если есть),
    иначе первый markdown-заголовок, иначе первая непустая строка (не разделитель
    frontmatter). Обрезает до 200 символов."""
    lines = text.splitlines()
    # YAML frontmatter: если файл начинается с '---', ищем description внутри.
    if lines and lines[0].strip() == "---":
        for i in range(1, len(lines)):
            line = lines[i].strip()
            if line == "---":
                break
            m = re.match(r"^description\s*:\s*(.+?)\s*$", line, re.IGNORECASE)
            if m:
                val = m.group(1).strip().strip('"').strip("'")
                if val:
                    return val[:200]
        # Промотаем lines до закрывающего '---'.
        end = 0
        for i in range(1, len(lines)):
            if lines[i].strip() == "---":
                end = i + 1
                break
        lines = lines[end:]
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped == "---":
            continue
        # Markdown H1/H2 → уберём ведущие #.
        if stripped.startswith("#"):
            stripped = stripped.lstrip("# ").strip()
            if stripped:
                return stripped[:200]
            continue
        return stripped[:200]
    return ""


def _build_index(dir_path: Path, label: str) -> str:
    """Индекс для ~/knowledge/<access|packages>/: имя + one-liner, без тел."""
    if not dir_path.is_dir():
        return f"### {label}\n\n(папка {dir_path} отсутствует)\n"

    entries: list[str] = []
    for entry in sorted(dir_path.iterdir(), key=lambda p: p.name.lower()):
        if entry.name.startswith("."):
            continue
        if entry.is_dir():
            # Для подпапок показываем имя; можно рекурсивно, но тут намеренно
            # ограничиваемся первой линией README.md, если есть.
            readme = entry / "README.md"
            summary = _first_descriptive_line(_read(readme)) if readme.exists() else "(директория)"
            entries.append(f"- `{entry.name}/` — {summary or '(без описания)'}")
        elif entry.suffix.lower() == ".md":
            summary = _first_descriptive_line(_read(entry))
            entries.append(f"- `{entry.name}` — {summary or '(без описания)'}")

    if not entries:
        body = "(пусто)"
    else:
        body = "\n".join(entries)
    return f"### {label}\n\n{body}\n"


def _build_memory_section() -> str:
    """Содержимое ВСЕХ memory/*.md + их порядковый индекс."""
    if not MEMORY_DIR.is_dir():
        return f"(папка {MEMORY_DIR} отсутствует)\n"

    files = sorted(
        [p for p in MEMORY_DIR.iterdir() if p.is_file() and p.suffix.lower() == ".md"],
        key=lambda p: p.name.lower(),
    )
    if not files:
        return "(memory пуст)\n"

    parts: list[str] = []
    # Сначала MEMORY.md (как индекс) — если есть.
    memory_index = next((p for p in files if p.name == "MEMORY.md"), None)
    rest = [p for p in files if p.name != "MEMORY.md"]
    if memory_index:
        parts.append(f"#### {memory_index.name} (индекс)\n\n{_read(memory_index).rstrip()}\n")
    for p in rest:
        parts.append(f"#### {p.name}\n\n{_read(p).rstrip()}\n")
    return "\n".join(parts)


def build_agents_md() -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines: list[str] = []
    lines.append(
        f"<!-- Автоматически сгенерировано scripts/sync_codex_knowledge.py ({now}). "
        f"Не редактируй вручную: изменения затрутся. -->\n"
    )
    lines.append("# Глобальные инструкции для Codex CLI\n")
    lines.append(
        "Этот файл собран из пользовательских настроек Claude Code и общей базы "
        "знаний (`~/projects/knowledge-base/`). Codex CLI читает `~/.codex/AGENTS.md` как "
        "user-global инструкции, аналогично тому, как Claude читает "
        "`~/.claude/CLAUDE.md`.\n"
    )

    # 1) CLAUDE.md + раскрытые импорты.
    claude_body = _read(CLAUDE_MD)
    if claude_body:
        expanded = _expand_claude_imports(claude_body, CLAUDE_MD)
        lines.append("## 1. Пользовательские правила (из ~/.claude/CLAUDE.md)\n")
        lines.append(expanded.rstrip() + "\n")
    else:
        lines.append("## 1. Пользовательские правила\n\n(не найдены)\n")

    # 2) Полные memory-файлы.
    lines.append("\n## 2. Memory (полные файлы из ~/.claude/projects/-home-shevartv/memory/)\n")
    lines.append(_build_memory_section())

    # 3) Основная БЗ.
    lines.append("\n## 3. Основная база знаний (~/projects/knowledge-base/)\n")
    lines.append(
        "Этот раздел инлайнит правила работы с БЗ, чтобы новые Codex-сессии "
        "сразу знали актуальный порядок чтения, обновления и гигиены файлов.\n"
    )

    agents_body = _read(KNOWLEDGE_BASE_AGENTS)
    if agents_body:
        lines.append("### knowledge-base/AGENTS.md\n")
        lines.append(agents_body.rstrip() + "\n")
    else:
        lines.append(f"### knowledge-base/AGENTS.md\n\n(не найден {KNOWLEDGE_BASE_AGENTS})\n")

    readme_body = _read(KNOWLEDGE_BASE_README)
    if readme_body:
        lines.append("\n### knowledge-base/README.md\n")
        lines.append(readme_body.rstrip() + "\n")
    else:
        lines.append(f"\n### knowledge-base/README.md\n\n(не найден {KNOWLEDGE_BASE_README})\n")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    target = Path(os.environ.get("CODEX_AGENTS_MD", str(DEFAULT_TARGET)))
    target.parent.mkdir(parents=True, exist_ok=True)

    content = build_agents_md()
    target.write_text(content, encoding="utf-8")
    size_kb = len(content.encode("utf-8")) / 1024
    print(f"[sync_codex_knowledge] wrote {target} ({size_kb:.1f} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
