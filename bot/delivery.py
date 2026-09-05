"""Отправка в топик: текст, документы, журнал хода, файлы по маркеру.

Три вещи, которых требует Telegram и которых не требует CLI:

* **HTML с фолбэком.** Разметка склеивается из ответа модели и может оказаться
  невалидной; тогда сообщение уходит второй попыткой как plain text, а не
  теряется.
* **Журнал хода.** Шаги агента копятся в ОДНОМ сообщении и остаются в топике
  после ответа. Раньше они писались в индикатор, где каждый апдейт затирал
  предыдущий, а в конце индикатор удалялся — ход работы исчезал бесследно.
* **Длинные ответы.** Больше MSG_LIMIT — уходит .md-файлом с коротким превью,
  потому что 4096 символов Telegram не переживает.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import tempfile
import time
from datetime import datetime

from telegram.constants import ParseMode
from telegram.error import BadRequest, NetworkError, RetryAfter, TimedOut
from telegram.ext import Application

from bot.db import _db, log_message
from bot.formatting import md_to_html, split_html_for_telegram
from bot.settings import (
    FILE_MARKER_RE,
    MEDIA_DIR,
    MSG_LIMIT,
    TG_FILE_LIMIT_MB,
    TG_HARD_LIMIT,
)
from bot.topics import (
    latest_topic_message_id,
    note_topic_message,
    resolve_service_topic,
    save_message_context,
)

logger = logging.getLogger(__name__)

JOURNAL_MAX_CHARS = 3400   # запас до TG_HARD_LIMIT на HTML-разметку
JOURNAL_LINE_CHARS = 400   # длинную строку шага режем — журнал, не транскрипт
JOURNAL_STALE_CHECK_SEC = 2.0    # как часто журнал сверяется с хвостом топика
JOURNAL_HEARTBEAT_SEC = 60.0     # молчание агента, после которого пишем «ещё работаю»
JOURNAL_FAIL_LIMIT = 3           # подряд НЕтранзиентных ошибок до отключения журнала

# Сетевой таймаут и флуд-контроль — не отказ, а обычная жизнь Telegram: их
# лечит повтор, а не капитуляция. Всё остальное (нет прав, топик удалён,
# сообщение слишком длинное) повтором не лечится.
TRANSIENT_TG_ERRORS = (RetryAfter, TimedOut, NetworkError)


async def _retry_transient(factory, what: str, attempts: int = 3):
    """Выполнить отправку с повторами на транзиентных ошибках Telegram.

    `factory` — функция без аргументов, возвращающая корутину (новую на каждую
    попытку). Возвращает результат последней успешной попытки; исчерпав
    попытки, пробрасывает последнюю ошибку.
    """
    delay = 1.0
    last: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await factory()
        except RetryAfter as exc:
            last = exc
            wait = float(getattr(exc, "retry_after", delay) or delay) + 0.5
        except (TimedOut, NetworkError) as exc:
            last = exc
            wait = delay
            delay *= 2
        if attempt < attempts:
            logger.info(
                "%s: transient telegram error (%s), retry %s/%s in %.1fs",
                what, type(last).__name__, attempt, attempts, wait,
            )
            await asyncio.sleep(wait)
    logger.warning("%s: giving up after %s attempts: %s", what, attempts, last)
    raise last  # type: ignore[misc]


async def _send_manager_notice(
    app: Application,
    text: str,
    kind: str = "job_notification",
    target_role: str = "secretary",
    target: tuple[int, int] | None = None,
) -> int | None:
    """Шлёт plain-сообщение в служебный топик, логирует и будит его.

    Помимо доставки plain-сообщения через Telegram, ставит auto-job
    source='self_notice' в очередь адресата, чтобы worker запустил его
    LLM-сессию и тот прочитал свой inbox. Дедуп: если у адресата уже
    есть pending/in_progress job — не создаём, он обработает все
    свежие нотисы вместе при текущем запуске.

    target_role='secretary' — коммуникационные уведомления/reminders.
    target_role='teamlead' — инженерные job/heartbeat/mxBoard-notices.
    Старое имя функции сохраняется как compatibility API.

    target=(chat_id, thread_id) перебивает роль: нотис по job уходит топику,
    который его делегировал (jobs.origin_*), а роль остаётся fallback'ом на
    случай, когда инициатор неизвестен. Без этого любой job будил Тимлида —
    он вклинивался в чужую задачу и ронял сессию исполнителя.
    """
    target = target or resolve_service_topic(target_role)
    if not target:
        return None
    chat_id, thread_id = target
    try:
        chat = await app.bot.get_chat(chat_id)
        sent = await send_to_topic(chat, thread_id, text)
        msg_id = sent.message_id if sent is not None else None
        log_message(chat_id, thread_id, "out", kind, text, msg_id)
    except Exception:
        logger.exception(
            "_send_manager_notice failed (kind=%s target_role=%s)",
            kind, target_role,
        )
        return None

    # Auto-kick: создать job для служебного топика, чтобы он сам активировался и
    # обработал свежие нотисы. С дедупом — один job на всю серию нотисов
    # пока адресат их не разгребёт.
    try:
        now = datetime.utcnow().isoformat()
        with _db() as conn:
            existing = conn.execute(
                "SELECT COUNT(*) FROM jobs "
                "WHERE chat_id = ? AND thread_id = ? "
                "AND status IN ('pending', 'in_progress')",
                (chat_id, thread_id),
            ).fetchone()
            if existing and existing[0] > 0:
                return msg_id
            conn.execute(
                "INSERT INTO jobs(chat_id, thread_id, text, source, status, "
                "created_at) VALUES (?, ?, ?, 'self_notice', 'pending', ?)",
                (
                    chat_id, thread_id,
                    "[AUTO-KICK] В твой топик пришли новые нотисы от бота. "
                    "Прочитай свой manager_inbox(thread_id=<свой>) и решай "
                    "что с ними делать.",
                    now,
                ),
            )
        logger.info(
            "service auto-kick job created role=%s chat=%s thread=%s",
            target_role, chat_id, thread_id,
        )
    except Exception:
        logger.exception(
            "auto-kick INSERT failed (kind=%s target_role=%s)",
            kind, target_role,
        )
    return msg_id


async def _send_with_html_fallback(send_func, text: str, **kwargs):
    """Шлёт text как HTML; при ошибке парсинга — повторяет без parse_mode (plain)."""
    try:
        return await send_func(text=text, parse_mode=ParseMode.HTML, **kwargs)
    except BadRequest as exc:
        if "parse" in str(exc).lower() or "entit" in str(exc).lower():
            logger.warning("HTML parse failed, falling back to plain: %s", exc)
            kwargs.pop("parse_mode", None)
            return await send_func(text=text, **kwargs)
        raise


async def send_to_topic(chat, thread_id: int, text: str, **kwargs):
    if thread_id:
        kwargs.setdefault("message_thread_id", thread_id)
    sent = await chat.send_message(text=text, **kwargs)
    note_topic_message(
        getattr(chat, "id", None), thread_id, getattr(sent, "message_id", None),
    )
    return sent


async def send_document_to_topic(chat, thread_id: int, document, **kwargs):
    if thread_id:
        kwargs.setdefault("message_thread_id", thread_id)
    sent = await chat.send_document(document=document, **kwargs)
    note_topic_message(
        getattr(chat, "id", None), thread_id, getattr(sent, "message_id", None),
    )
    return sent


class ProgressJournal:
    """Журнал хода работы агента — одно накопительное сообщение в топике.

    Раньше промежуточные шаги писались в сообщение-индикатор: каждый апдейт
    ЗАТИРАЛ предыдущий, а в конце индикатор удалялся — так что ход работы
    (какие команды агент выполнял, что читал, о чём рассуждал) исчезал целиком.
    Теперь шаги дописываются и остаются в топике после ответа.

    Когда сообщение упирается в лимит Telegram, журнал продолжается в новом;
    заполненное остаётся в истории как есть. Если агент не сделал ни одного
    шага, стартовая плашка удаляется, чтобы не мусорить в топике.

    **Журнал всегда пишет в ХВОСТ топика.** Если ниже его сообщения успело
    уехать что-то ещё — вопрос ask_user, реплика пользователя, ответ бота, —
    он отцепляется от своего сообщения и продолжает трансляцию новым, внизу.
    Иначе после интерактива (вопрос с кнопками занимает весь экран) шаги
    дописывались в сообщение, которого уже не видно, и работа выглядела
    зависшей.

    **Журнал не глохнет от сетевой икоты.** Таймаут и флуд-контроль Telegram —
    транзиентные: строки остаются в буфере и уедут со следующим флешем.
    Выключается журнал только после JOURNAL_FAIL_LIMIT подряд ошибок, которые
    повтором не лечатся (топик удалён, отняты права).
    """

    def __init__(self, chat, thread_id: int, prefix: str = "",
                 header: str = "⏳ Думаю..."):
        self.chat = chat
        self.thread_id = thread_id
        self.prefix = prefix
        self.header = header
        self.msg = None
        self.lines: list[str] = []   # строки ТЕКУЩЕГО сообщения
        self.total_steps = 0
        self._broken = False         # Telegram не даёт писать — тихо выключаемся
        self._fail_streak = 0        # подряд НЕтранзиентных ошибок
        self._flushed = 0            # сколько строк из self.lines уже отрисовано
        self._last_stale_check = 0.0
        self._started_at = time.monotonic()
        self._last_step_at = time.monotonic()
        self._hb_suffix = ""         # строка «ещё работаю», не входит в lines
        self._hb_task: asyncio.Task | None = None

    async def start(self) -> None:
        try:
            self.msg = await send_to_topic(
                self.chat, self.thread_id, f"{self.prefix}{self.header}",
            )
        except Exception:
            logger.exception("journal: failed to post header")
            self.msg = None
        self._hb_task = asyncio.create_task(self._heartbeat_loop())

    def _render(self) -> str:
        body = self.prefix + "\n".join(self.lines)
        if self._hb_suffix:
            body = (body + "\n" + self._hb_suffix) if self.lines else (
                self.prefix + self._hb_suffix
            )
        return body

    def _detach(self) -> None:
        """Отцепиться от текущего сообщения: дальше пишем в новое, ниже.

        Уже отрисованные строки остаются в старом сообщении, в новое уходит
        только неотрисованный хвост — иначе шаги задвоятся.
        """
        self.lines = self.lines[self._flushed:]
        self._flushed = 0
        self.msg = None

    def _is_stale(self) -> bool:
        """Появилось ли в топике сообщение НИЖЕ нашего."""
        if self.msg is None:
            return False
        now = time.monotonic()
        if now - self._last_stale_check < JOURNAL_STALE_CHECK_SEC:
            return False
        self._last_stale_check = now
        chat_id = getattr(self.chat, "id", None)
        if chat_id is None:
            return False
        latest = latest_topic_message_id(chat_id, self.thread_id)
        return bool(latest and latest > self.msg.message_id)

    async def _heartbeat_loop(self) -> None:
        """Пока агент молчит — показывать, что работа идёт, а не зависла."""
        try:
            while not self._broken:
                await asyncio.sleep(JOURNAL_HEARTBEAT_SEC / 2)
                idle = time.monotonic() - self._last_step_at
                if idle < JOURNAL_HEARTBEAT_SEC:
                    continue
                mins = int((time.monotonic() - self._started_at) // 60)
                self._hb_suffix = (
                    f"⏳ работаю {mins} мин, шагов {self.total_steps} "
                    f"(тишина {int(idle)} с)"
                )
                await self._flush()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("journal: heartbeat loop stopped", exc_info=True)

    async def append(self, chunk: str) -> None:
        """Дописать шаги. `chunk` — только новое с прошлого флеша (движки шлют
        дельту, а не весь буфер)."""
        if self._broken:
            return
        new_lines = [
            line.strip()[:JOURNAL_LINE_CHARS]
            for line in chunk.splitlines() if line.strip()
        ]
        if not new_lines:
            return

        self._last_step_at = time.monotonic()
        self._hb_suffix = ""
        for line in new_lines:
            # Не влезает в текущее сообщение — начинаем новое, старое остаётся.
            if self.lines and len(self._render()) + len(line) + 1 > JOURNAL_MAX_CHARS:
                self.msg = None
                self.lines = []
                self._flushed = 0
            self.lines.append(line)
            self.total_steps += 1

        await self._flush()

    async def _flush(self) -> None:
        if self._broken:
            return
        if self._is_stale():
            logger.info(
                "journal: topic moved on, detaching from msg %s",
                getattr(self.msg, "message_id", None),
            )
            self._detach()
        if not self.lines and not self._hb_suffix:
            return
        body = md_to_html(self._render())
        pending = len(self.lines)
        try:
            if self.msg is None:
                self.msg = await _send_with_html_fallback(
                    self.chat.send_message, body,
                    **({"message_thread_id": self.thread_id} if self.thread_id else {}),
                )
                note_topic_message(
                    getattr(self.chat, "id", None), self.thread_id,
                    getattr(self.msg, "message_id", None),
                )
            else:
                try:
                    await self.msg.edit_text(body, parse_mode=ParseMode.HTML)
                except BadRequest as exc:
                    text = str(exc).lower()
                    if "not modified" in text:
                        pass
                    elif "parse" in text or "entit" in text:
                        await self.msg.edit_text(self._render())
                    else:
                        raise
            self._flushed = pending
            self._fail_streak = 0
        except TRANSIENT_TG_ERRORS as exc:
            # Флуд-контроль или сетевой таймаут. Пропускаем ЭТОТ апдейт —
            # строки уже в self.lines и уедут со следующим флешем. Глушить
            # журнал нельзя: на длинном ходе это норма, а не отказ.
            logger.info(
                "journal: transient telegram error (%s), skipping this update",
                type(exc).__name__,
            )
        except Exception:
            # Удалённый топик, потеря прав, слишком длинное сообщение — повтор
            # не поможет. Но одну ошибку прощаем: следующий флеш пойдёт в новое
            # сообщение, и молчим только если и это не сработало.
            self._fail_streak += 1
            logger.warning(
                "journal: update failed (%s/%s)",
                self._fail_streak, JOURNAL_FAIL_LIMIT, exc_info=True,
            )
            self._detach()
            if self._fail_streak >= JOURNAL_FAIL_LIMIT:
                logger.warning("journal: giving up on updates")
                self._broken = True

    def _strip_final_echo(self, final_text: str) -> None:
        """Убрать из хвоста журнала текст, который сейчас уйдёт финальным ответом.

        Движки шлют в промежуточный поток и текстовые блоки ассистента — для
        claude это вообще единственная видимая часть его рассуждений (блок
        thinking приходит пустым), так что выбрасывать их нельзя. Но ПОСЛЕДНИЙ
        такой блок и есть финальный ответ: без этой чистки он виден дважды —
        в журнале и отдельным сообщением.

        Режем только хвост и только строки без префикса шага (🔧 / 💭):
        промежуточные реплики агента («сейчас проверю X») в финал не входят и
        должны остаться.
        """
        final = (final_text or "").strip()
        if not final:
            return
        while self.lines:
            last = self.lines[-1]
            if last.startswith(("🔧", "💭")):
                break
            # Строки в журнале обрезаны (JOURNAL_LINE_CHARS), поэтому ищем
            # вхождение, а не равенство.
            if last and last in final:
                self.lines.pop()
                self.total_steps -= 1
                self._flushed = min(self._flushed, len(self.lines))
                continue
            break

    async def finish(self, final_text: str | None = None) -> None:
        """Оставить журнал в топике, вычистив эхо финального ответа.

        Журнал без единого шага (или из одного лишь финального текста) —
        удаляем: он ничего не добавляет к ответу.
        """
        if self._hb_task is not None:
            self._hb_task.cancel()
            self._hb_task = None
        self._hb_suffix = ""
        if final_text:
            self._strip_final_echo(final_text)
        if self.total_steps > 0 and self.lines:
            await self._flush()   # перерисовать без вырезанного хвоста
            return
        if self.msg is None:
            return
        try:
            await self.msg.delete()
        except Exception:
            pass


def extract_file_markers(text: str) -> tuple[str, list[tuple[str, str | None]]]:
    """Парсит маркеры [[FILE: /path]] / [[FILE: /path | caption]] на отдельных строках.
    Возвращает (текст без маркеров, список (path, caption|None))."""
    markers: list[tuple[str, str | None]] = []

    def _collect(m: re.Match) -> str:
        path = m.group("path").strip()
        cap = m.group("caption")
        cap = cap.strip() if cap else None
        markers.append((path, cap))
        return ""

    cleaned = FILE_MARKER_RE.sub(_collect, text)
    # Подчищаем пустые строки, оставшиеся после вырезания маркеров.
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, markers


async def deliver_file_markers(
    chat,
    thread_id: int,
    markers: list[tuple[str, str | None]],
    notice_prefix: str = "",
) -> None:
    """Отправляет файлы по списку маркеров. Ошибки сообщает текстом в тот же топик
    (с notice_prefix, например '[#xxxx] '). Сами файлы шлёт без префикса."""
    for path, caption in markers:
        try:
            if not os.path.isabs(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ путь не абсолютный: {path}",
                )
                continue
            if not os.path.exists(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ файл {path} не найден",
                )
                continue
            if not os.path.isfile(path):
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ {path} не является обычным файлом",
                )
                continue
            size = os.path.getsize(path)
            size_mb = size / (1024 * 1024)
            if size_mb > TG_FILE_LIMIT_MB:
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ файл {path} слишком большой "
                    f"({size_mb:.1f} MB), лимит Telegram {TG_FILE_LIMIT_MB} MB",
                )
                continue
            kwargs = {"filename": os.path.basename(path)}
            if caption:
                kwargs["caption"] = caption[:1024]
            with open(path, "rb") as fh:
                await send_document_to_topic(chat, thread_id, document=fh, **kwargs)
            logger.info("delivered file marker: %s (%.1f MB)", path, size_mb)
        except Exception as exc:
            logger.exception("deliver_file_markers failed for %s", path)
            try:
                await send_to_topic(
                    chat, thread_id,
                    f"{notice_prefix}⚠️ не удалось отправить {path}: {exc}",
                )
            except Exception:
                pass


async def send_claude_reply(
    chat, thread_id: int, text: str, meta: dict, filename_prefix: str = "reply",
    html_prefix: str = "",
):
    """Короткий текст — send_message с HTML-форматированием; длинный — .md вложение.
    `html_prefix` (например '[#xxxx] ') добавляется как уже готовый HTML-фрагмент
    перед сконвертированным телом."""
    log_kind = "spawn_reply" if meta.get("spawn_id") else "bot_reply"

    if len(text) <= MSG_LIMIT:
        html_body = html_prefix + md_to_html(text)
        chunks = split_html_for_telegram(html_body, TG_HARD_LIMIT)
        send_kwargs = {}
        if thread_id:
            send_kwargs["message_thread_id"] = thread_id
        sent = None
        try:
            for chunk in chunks:
                # Ответ агента терять нельзя: флуд-контроль и сетевой таймаут
                # повторяем, а не молча роняем — из-за этого сообщения от
                # агента переставали доходить в топик.
                sent = await _retry_transient(
                    lambda chunk=chunk: _send_with_html_fallback(
                        chat.send_message, chunk, **send_kwargs,
                    ),
                    "send_claude_reply",
                )
        except Exception:
            logger.exception("send_claude_reply: text delivery failed, falling back to file")
            sent = None
        if sent is not None:
            note_topic_message(chat.id, thread_id, sent.message_id)
            try:
                save_message_context(chat.id, sent.message_id, meta)
                log_message(
                    chat.id, thread_id, "out", log_kind, text, sent.message_id,
                )
            except Exception:
                logger.exception("save_message_context failed")
            return sent
        # Текстом не получилось — уводим ответ во вложение (ниже общий путь).

    plain_prefix = re.sub(r"<[^>]+>", "", html_prefix) if html_prefix else ""
    if len(text) <= MSG_LIMIT:
        # Сюда попадаем только фолбэком: текстом Telegram ответ не принял.
        preview = (
            plain_prefix + "⚠️ Telegram не принял ответ текстом — он во вложении."
        )
    else:
        preview = plain_prefix + text[:200].rstrip() + "...\n\nполный ответ во вложении"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".md", delete=False, encoding="utf-8", dir=MEDIA_DIR,
    ) as f:
        f.write(text)
        path = f.name
    try:
        async def _send_doc():
            with open(path, "rb") as fh:
                return await send_document_to_topic(
                    chat, thread_id,
                    document=fh,
                    filename=(
                        f"{filename_prefix}_"
                        f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.md"
                    ),
                    caption=preview[:1024],
                )

        try:
            sent = await _retry_transient(_send_doc, "send_claude_reply(file)")
        except Exception:
            logger.exception("send_claude_reply: file delivery failed too")
            return None
        try:
            save_message_context(chat.id, sent.message_id, meta)
            log_message(
                chat.id, thread_id, "out", log_kind, text,
                sent.message_id if sent is not None else None,
            )
        except Exception:
            logger.exception("save_message_context failed")
        return sent
    finally:
        try:
            os.unlink(path)
        except OSError:
            pass
