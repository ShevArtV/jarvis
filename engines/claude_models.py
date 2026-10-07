"""Список моделей claude CLI для меню выбора."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from pathlib import Path

from engines.claude_cli import CLAUDE_BIN
from engines.model_cache import remember_labels, split_models
from engines.process_control import run_cli

logger = logging.getLogger(__name__)


# Алиасы последних моделей: CLI принимает их всегда, независимо от аккаунта.
DEFAULT_CLAUDE_MODELS = ["opus", "sonnet", "haiku"]


def _models_from_claude_config() -> list[str]:
    """Модели, доступные аккаунту сверх базовых алиасов.

    Полного списка claude CLI наружу не отдаёт (команды `claude models` нет),
    но то, что доступно сверх алиасов, кладёт в ~/.claude.json →
    additionalModelOptionsCache: полные имена вроде 'claude-fable-5[1m]', для
    которых короткого алиаса не существует.
    """
    cfg_path = Path(
        os.environ.get("CLAUDE_CONFIG_FILE", Path.home() / ".claude.json")
    )
    if not cfg_path.is_file():
        return []
    try:
        data = json.loads(cfg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        logger.warning("cannot read claude config: %s", cfg_path, exc_info=True)
        return []
    if not isinstance(data, dict):
        return []

    models: list[str] = []
    for item in data.get("additionalModelOptionsCache", []) or []:
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        if isinstance(value, str) and value:
            models.append(value)
    return models


def _models_from_claude_init(timeout: float = 30.0) -> list[str]:
    """Модели аккаунта из ответа CLI на control-запрос ``initialize``.

    Тот же список, что в меню /model: алиасы и полные имена прошлых версий.
    Сообщение модели не уходит — токены не тратятся, ~2с на старт CLI.
    displayName ('Opus 5.5') запоминается для кнопок: по алиасу 'opus'
    не видно, какая это версия.
    ``default`` отбрасываем: это не модель, а «дефолт CLI» — он и так
    получается без --model.
    """
    request = json.dumps({
        "type": "control_request",
        "request_id": "jarvis-models",
        "request": {"subtype": "initialize"},
    })
    try:
        proc = run_cli(
            [CLAUDE_BIN, "-p", "--input-format", "stream-json",
             "--output-format", "stream-json", "--verbose"],
            input=request + "\n", capture_output=True, timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired):
        logger.warning("claude model discovery failed", exc_info=True)
        return []
    for line in proc.stdout.splitlines():
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("type") != "control_response":
            continue
        response = (ev.get("response") or {}).get("response") or {}
        models: list[str] = []
        labels: dict[str, str] = {}
        for item in response.get("models") or []:
            if not isinstance(item, dict):
                continue
            value = item.get("value")
            if not isinstance(value, str) or not value or value == "default":
                continue
            models.append(value)
            label = item.get("displayName")
            if isinstance(label, str) and label:
                labels[value] = label
        remember_labels(labels)
        return models
    logger.warning("claude model discovery: no control_response, rc=%s", proc.returncode)
    return []


def _discover_claude_models() -> list[str]:
    return (
        split_models(os.environ.get("CLAUDE_MODELS"))
        or _models_from_claude_init()
        or DEFAULT_CLAUDE_MODELS + _models_from_claude_config()
    )
