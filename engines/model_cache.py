"""Списки моделей движков — реальные, а не захардкоженные.

Каждый адаптер отдаёт `models` как property, за которым стоит этот кэш: свой
источник (env-override → конфиг/кэш CLI → `<cli> models`) опрашивается не чаще
JARVIS_MODELS_TTL секунд.

Опрос бывает медленным (`opencode models` — ~1.5с), а `models` читается из
async-хендлеров Telegram, поэтому протухший список отдаётся сразу, а обновление
уходит в фоновый поток. Синхронным опрос остаётся только при холодном кэше —
его закрывает prewarm_models() на старте бота.
"""

from __future__ import annotations

import logging
import os
import subprocess
import threading
import time
from typing import Callable, Iterable

logger = logging.getLogger(__name__)

MODELS_TTL = float(os.environ.get("JARVIS_MODELS_TTL", "600"))

_lock = threading.Lock()
_cache: dict[str, tuple[float, list[str]]] = {}
_refreshing: set[str] = set()
# Человекочитаемые имена моделей, если CLI их отдаёт (claude: 'opus' → 'Opus 5.5').
_labels: dict[str, str] = {}


def remember_labels(labels: dict[str, str]) -> None:
    """Запомнить имена моделей для кнопок и списков в UI."""
    with _lock:
        _labels.update(labels)


def label_for(model: str) -> str | None:
    """Имя модели от CLI или None, если CLI его не сообщал."""
    with _lock:
        return _labels.get(model)


def split_models(raw: str | None) -> list[str]:
    """CSV из env → список моделей."""
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if part.strip()]


def dedup(models: Iterable[str]) -> list[str]:
    """Убрать дубли, сохранив порядок: он же порядок кнопок в UI."""
    seen: set[str] = set()
    out: list[str] = []
    for model in models:
        if model and model not in seen:
            seen.add(model)
            out.append(model)
    return out


def cli_models(cmd: list[str], timeout: float = 20.0) -> list[str]:
    """Спросить у CLI его список моделей. Любая осечка → пустой список."""
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired):
        logger.warning("model discovery failed: %s", " ".join(cmd), exc_info=True)
        return []
    if proc.returncode != 0:
        logger.warning(
            "model discovery rc=%s: %s: %s",
            proc.returncode, " ".join(cmd), (proc.stderr or "").strip()[:200],
        )
        return []
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def cached_models(
    engine: str,
    discover: Callable[[], list[str]],
    fallback: list[str],
) -> list[str]:
    """Список моделей движка. Пустой ответ источника → fallback (хардкод)."""
    with _lock:
        hit = _cache.get(engine)
    if hit is None:
        return _refresh(engine, discover, fallback)
    cached_at, models = hit
    if time.monotonic() - cached_at >= MODELS_TTL:
        _refresh_in_background(engine, discover, fallback)
    return models


def prewarm(engine: str, discover: Callable[[], list[str]], fallback: list[str]) -> None:
    """Заполнить кэш до первого обращения из хендлера. Блокирует — звать из потока."""
    _refresh(engine, discover, fallback)


def _refresh(
    engine: str,
    discover: Callable[[], list[str]],
    fallback: list[str],
) -> list[str]:
    try:
        models = dedup(discover())
    except Exception:
        logger.exception("model discovery crashed: engine=%s", engine)
        models = []
    if not models:
        logger.warning("engine=%s: список моделей пуст, беру дефолт", engine)
        models = list(fallback)
    with _lock:
        _cache[engine] = (time.monotonic(), models)
    logger.info("engine=%s models: %s", engine, ", ".join(models))
    return models


def _refresh_in_background(
    engine: str,
    discover: Callable[[], list[str]],
    fallback: list[str],
) -> None:
    with _lock:
        if engine in _refreshing:
            return
        _refreshing.add(engine)

    def _run() -> None:
        try:
            _refresh(engine, discover, fallback)
        finally:
            with _lock:
                _refreshing.discard(engine)

    threading.Thread(target=_run, name=f"models-{engine}", daemon=True).start()
