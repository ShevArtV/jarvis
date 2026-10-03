# Jarvis

[![tests](https://github.com/ShevArtV/jarvis/actions/workflows/tests.yml/badge.svg)](https://github.com/ShevArtV/jarvis/actions/workflows/tests.yml)

Тонкая обёртка Telegram-бота над LLM CLI (`claude`, `codex`, `opencode` или `cursor`). Один топик = одна непрерывная сессия.
Пишешь в Telegram — получаешь ответ, как если бы запускал CLI в терминале.

## Quick start (English)

Jarvis is a Telegram bot that drives the LLM CLIs you already use — `claude`,
`codex`, `opencode` or `cursor` (Cursor CLI, `cursor-agent`). Each forum topic is one long-running CLI session with its
own working directory, engine and model. The rest of this README is in Russian;
this section is enough to get the bot running.

**Requirements:** Python 3.11+, at least one of `claude` / `codex` / `opencode` / `cursor-agent`
installed and logged in under the same OS user, a bot token from
[@BotFather](https://t.me/BotFather) and your Telegram user id (e.g. from
`@userinfobot`). Add the bot to a forum group with topics enabled, or just talk
to it in a private chat.

### Linux / macOS

```bash
git clone https://github.com/ShevArtV/jarvis.git
cd jarvis
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
cp .env.example .env          # set TELEGRAM_TOKEN and ALLOWED_USER_IDS
claude -p "hello"             # make sure the engine answers from this shell
./venv/bin/python telegram_bot.py
```

To keep it running, install the systemd user unit from `systemd/` (see
[«Автозапуск через systemd»](#автозапуск-через-systemd-user-unit); adjust
`WorkingDirectory` and `ExecStart` if the repo is not in `~/projects/jarvis`).

### Windows (native, no WSL)

```powershell
git clone https://github.com/ShevArtV/jarvis.git
cd jarvis
py -3 -m venv venv
.\venv\Scripts\pip install -r requirements.txt
Copy-Item .env.example .env   # set TELEGRAM_TOKEN and ALLOWED_USER_IDS
claude -p "hello"
$env:PYTHONUTF8 = "1"
.\venv\Scripts\python telegram_bot.py
```

npm-installed CLIs (`.cmd` shims) are found automatically. Autostart via Task
Scheduler or NSSM and the Windows-specific notes are in
[docs/windows.md](docs/windows.md).

### Next steps

- In a topic: `/engine` shows and switches the engine and model, `/bind <path>`
  sets the topic's working directory; `/session`, `/stop`, `/close` do what
  they say.
- Default engine for new topics: `JARVIS_ENGINE=claude|codex|opencode|cursor` in `.env`.
- Every other setting is documented in `.env.example`.

### Plugins

Integrations live in `plugins/<name>/` and are **off by default**. Enable them
with a comma-separated list in `.env` and restart the bot:

```bash
JARVIS_PLUGINS=reminders,imap
```

| Plugin | What it does | Portable? |
|---|---|---|
| `reminders` | the agent schedules reminders (`daily 09:00`, `once 2026-12-31 18:00`, …) via MCP tools; they are posted to the topic they were set in | yes |
| `imap` | watches IMAP mailboxes, posts "new mail" notices (`JARVIS_IMAP_ACCOUNTS`) | yes |
| `webhook` | receives **Bitrix24** webhooks at `/hook/bitrix24` (`JARVIS_WEBHOOK_TOKEN`) | Bitrix24 only; other services need a new route |
| `activecollab` | MCP tools for ActiveCollab tasks, comments, time records | if you use ActiveCollab |
| `queuewarden` | QueueWarden notifications and the `/board` mini app | only with a QueueWarden install |
| `support_topic` | makes the bot ignore one topic owned by another bot | yes |

`imap`, `webhook` and `queuewarden` post their notices to a **service topic**
and wake the agent there. Set at least `JARVIS_MANAGER_CHAT_ID` and
`JARVIS_MANAGER_THREAD_ID`; with no service topic configured, those notices
are silently dropped. Both ids show up in the bot log: post in the topic and
look for `update …: chat=<CHAT_ID> thread=<THREAD_ID>`.

Writing a plugin: create `plugins/<name>/plugin.py` exposing
`PLUGIN = Plugin(name=..., workers=..., commands=..., setup=...,
trigger_sources=..., schema=...)` (contract in `bot/plugins.py`), and
optionally `plugins/<name>/mcp_tools.py` with `register(mcp)` for agent tools.
The Russian section «Плагины» below has a full example.

## Что умеет

- Передаёт любые текстовые запросы в выбранный движок (`claude`, OpenAI `codex`, `opencode` или `cursor`).
- Запоминает session-id на каждый топик — контекст диалога сохраняется.
- Движок и модель per-topic: `JARVIS_ENGINE=claude|codex|opencode|cursor` задаёт
  дефолтный движок для новых топиков; в любом топике можно переключиться
  командой `/engine <name> [model-substring]`.
- `/engine` — показать текущий движок и список доступных; `/engine <name>` —
  переключить движок, `/engine codex gpt-5.4-mini` — переключить движок и модель
  (новый session_id под новый движок, cwd сохраняется, контекст прежнего диалога
  не переносится).
- `/close` — закрыть сеанс (контекст сбрасывается, топик остаётся). Сеанс
  закрывается и сам — после `JARVIS_SESSION_IDLE_MINUTES` простоя. См. «Сеансы».
- `/new` или `/reset` — закрыть сеанс и сразу открыть новый.
- `/session` — session-id, cwd, движок, браузер и состояние сеанса.
- `/tokens` — показать оценку размера текущей LLM-сессии.
- `/usage` — остаток лимитов подписки: claude (из кэша CLI `~/.claude.json`),
  codex (из последнего `rollout-*.jsonl`), opencode и cursor — не отслеживаются.
- `/browser [on|off]` — включить/выключить браузер (Playwright MCP) для топика.
  По умолчанию **выключен** (on-demand): браузерные tools грузятся в контекст
  только там, где реально нужны — иначе ~30 `browser_*` тулов висят в каждом
  запросе и зря жгут токены.
- `/persistent [on|off]` — живой процесс для `claude`, `codex` и `opencode`:
  новое сообщение во время активного хода не ждёт topic-lock, а дописывается в
  текущую работу (`claude` через stream-json stdin, `codex` через app-server
  `turn/steer`, `opencode` через `opencode serve` и `prompt_async`); `cursor` — не поддержан.
- **Журнал хода** — шаги агента (инструменты, рассуждения, промежуточный текст)
  копятся в одном сообщении и **остаются** в топике после ответа. Раньше они
  писались в индикатор, где каждый апдейт затирал предыдущий, а в конце
  индикатор удалялся — ход работы исчезал. Переполнение сообщения → журнал
  продолжается в новом. См. «Что видно из рассуждений агента».
- Длинные ответы (> 3500 символов) присылаются как `.md`-файл с коротким превью.
- Reply-to на сообщение бота → в запрос подмешивается скрытый контекст о том, на что ты отвечаешь.
- Фото/документы скачиваются локально, путь прокидывается в prompt (`[Прикреплён файл: ...]`).
- Playwright MCP — **on-demand** для `claude`/`codex`/`opencode`:
  включается per-topic командой `/browser on` и инъектируется в CLI на каждый
  запрос, без постоянной глобальной регистрации. `cursor` — не поддержан: у
  `cursor-agent` нет per-invocation подключения MCP.
- Внешние MCP-серверы — **по роли топика**: топик-оркестратор («Менеджер») и
  рабочие топики ходят во внешний сервис разными кредами, не перемешивая
  личности. Объявляются одним JSON-файлом; движок на роль не влияет.
- Голосовые не поддерживаются.
- Whitelist по `user_id` (см. `ALLOWED_USER_IDS`).

## Требования

- **Python 3.11+** (проверяется в CI на 3.11 и 3.12).
- Хотя бы один LLM CLI, установленный и авторизованный: `claude`, `codex`,
  `opencode` или `cursor-agent`. Jarvis их не устанавливает и ключей не хранит — он вызывает то, что
  уже работает у вас в терминале.
- Для role-based topic MCP через Codex нужен `codex-cli >= 0.134.0`: начиная с
  этой версии `--profile` загружает отдельный
  `$CODEX_HOME/<name>.config.toml`.
- Токен бота от [@BotFather](https://t.me/BotFather) и свой Telegram user-id
  (например через `@userinfobot`).

Форум-чат не обязателен: в обычном чате бот работает одним топиком
(`thread_id=0`). Но темы Telegram — это то, ради чего всё сделано: топик = проект
со своим каталогом, движком и моделью.

## Установка

### Linux / macOS

```bash
git clone https://github.com/ShevArtV/jarvis.git
cd jarvis
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
cp .env.example .env
# отредактировать .env: TELEGRAM_TOKEN + ALLOWED_USER_IDS
```

### Windows

Нативно, без WSL — PowerShell, `PYTHONUTF8=1`, автозапуск через Планировщик
задач или NSSM: [docs/windows.md](docs/windows.md).

### Проверка движка

Убедись, что `claude` доступен в PATH и авторизован:

```bash
claude --version
claude -p "hello"   # проверка, что авторизация работает
```

> **`.env` не перебивает окружение.** `python-dotenv` заполняет только те
> переменные, которых в окружении ещё нет. Если `TELEGRAM_TOKEN` уже экспортирован
> в шелле (или унаследован от родительского процесса), правка `.env` **молча ни на
> что не влияет** — бот поднимется со старым значением. Наступали 2026-07-25:
> тестовый клон в `/tmp` подхватил токен боевого бота и на полминуты устроил
> обоим `409 Conflict`. Проверяйте `env | grep TELEGRAM_TOKEN`, если поведение не
> соответствует файлу.

### Плагины

Ядро Jarvis — топики, сеансы, движки, очереди `jobs`/`agent_triggers` и Manager
MCP. Всё, что ходит во внешние сервисы, вынесено в плагины `plugins/<имя>/` и по
умолчанию **выключено**: чистая установка не тянет чужих зависимостей и никуда
не стучится.

**Включение** — список имён через запятую в `.env`, затем перезапуск бота:

```bash
JARVIS_PLUGINS=reminders,imap
```

Список читают и бот (фоновые задачи, команды, таблицы), и MCP-сервер (тулы
плагина для агента; `.env` он ищет рядом с `--db`). Имя без учёта регистра.
Плагин, который не импортировался, пропускается с ошибкой в журнале — остальные
работают. В журнале при старте: `plugins loaded: …`.

| Плагин | Что делает | Настройка | Где пригоден |
|---|---|---|---|
| `reminders` | агент ставит напоминания по расписанию (`daily 09:00`, `weekly mon,thu 10:00`, `once 2026-12-31 18:00`…) тулами `manager_remind_*`; бот присылает их в топик, где они созданы | `JARVIS_REMINDERS_TZ`, `JARVIS_REMINDERS_INTERVAL` — необязательно | везде |
| `imap` | следит за ящиками IMAP и присылает нотис «📧 Новое письмо» с отправителем и темой | `JARVIS_IMAP_ACCOUNTS` (JSON, пароль — через `password_env`), `JARVIS_IMAP_INTERVAL` | любой IMAP-ящик; нужен служебный топик (ниже) |
| `webhook` | HTTP-приёмник событий **Битрикс24** (`/hook/bitrix24`): новые задачи, дела CRM, события календаря → нотис | `JARVIS_WEBHOOK_TOKEN` (без него сервер не стартует), `JARVIS_WEBHOOK_HOST`, `JARVIS_WEBHOOK_PORT`; наружу — через reverse proxy с TLS | только Битрикс24; другой сервис — новый маршрут в `plugins/webhook/server.py`; нужен служебный топик |
| `activecollab` | MCP-тулы `manager_activecollab_*`: свои задачи и обновления, комментарий, учёт времени, перенос между стадиями | `ACTIVE_COLLAB_URL`, `ACTIVE_COLLAB_TOKEN` | у кого ActiveCollab; см. раздел «ActiveCollab» |
| `queuewarden` | уведомления из QueueWarden будят агента в топике, команда `/board` открывает доску-миниапп | `QUEUEWARDEN_*`, `BOARD_MINIAPP*`; см. разделы «QueueWarden» | только с развёрнутым QueueWarden |
| `support_topic` | бот полностью пропускает один топик — там отвечает другой бот (поддержка клиентов), и LLM не запускается | `JARVIS_SUPPORT_CHAT_ID` и `JARVIS_SUPPORT_THREAD_ID`, оба обязательны | везде, где бот делит форум с другим ботом |

#### Куда приходят нотисы

`imap`, `webhook` и `queuewarden` шлют нотисы не в произвольный топик, а в
**служебный**, и заодно будят в нём агента, чтобы тот разобрал свежие
события:

- `imap`, `webhook` — в топик Секретаря: `JARVIS_SECRETARY_CHAT_ID`/`_THREAD_ID`,
  иначе `JARVIS_MANAGER_CHAT_ID`/`_THREAD_ID`;
- `queuewarden` — `JARVIS_QW_NOTICE_*`, иначе Тимлид (`JARVIS_TEAMLEAD_*`),
  иначе Секретарь.

**Ни одна пара не задана — нотисы молча не отправляются.** Минимальная
настройка — один топик Менеджера: `JARVIS_MANAGER_CHAT_ID` и
`JARVIS_MANAGER_THREAD_ID`. Оба id видны в журнале бота: напишите в нужный
топик и найдите строку `update …: chat=<CHAT_ID> thread=<THREAD_ID>`.
`reminders` от служебных топиков не зависит: напоминание приходит туда, где его
поставили, а в служебном топике ещё и будит агента.

#### Свой плагин

Пакет `plugins/<имя>/` с файлом `plugin.py`, где объявлен `PLUGIN`
(контракт — `bot/plugins.py`):

```python
from bot.plugins import Command, Plugin, TriggerSource

async def my_worker(app):            # фоновая задача, живёт всё время работы бота
    ...

async def cmd_hello(update, context):
    await update.effective_message.reply_text("hi")

PLUGIN = Plugin(
    name="myplugin",
    workers=(my_worker,),
    commands=(Command("hello", cmd_hello, "поздороваться"),),  # + в меню бота
    setup=None,                      # Application -> None: свои хендлеры, группы
    trigger_sources={"myplugin": TriggerSource(build_prompt=lambda texts: "\n".join(texts))},
    schema=("CREATE TABLE IF NOT EXISTS myplugin_state (id INTEGER PRIMARY KEY)",),
)
```

- `workers` — корутины от `Application`, бот запускает их при старте.
- `commands` регистрируются с тем же whitelist, что и команды ядра, и попадают в
  меню.
- `trigger_sources` — как исполнять строки `agent_triggers` с этим `source`
  (см. «Внешние триггеры»): `build_prompt` собирает промпт хода,
  `coalesce_seconds` склеивает серию триггеров топика в один ход,
  `allow_silent` разрешает агенту промолчать ответом `[[SILENT]]`.
- `schema` — идемпотентный DDL своих таблиц, выполняется при старте в общей
  `bot_state.db`.
- MCP-тулы для агента — `plugins/<имя>/mcp_tools.py` с функцией
  `register(mcp)`, внутри — обычные `@mcp.tool(...)`. MCP-сервер работает
  отдельным процессом: этот модуль не должен импортировать Telegram-часть
  бота.
- `plugin.py` импортирует только бот; держите тяжёлые зависимости внутри
  плагина и добавьте их в `requirements.txt` с пометкой, какому плагину они нужны.

### Whitelist

`ALLOWED_USER_IDS` в `.env` — запятая-разделённый список Telegram user-id, которым разрешено
писать боту. Узнать свой id можно через `@userinfobot`.

### Переменные окружения (опционально)

- `JARVIS_ENGINE` — `claude` (дефолт), `codex`, `opencode` или `cursor`. Задаёт **дефолтный
  движок для новых топиков**. Существующие топики хранят свой движок в БД и
  не пересоздаются при смене env — для переключения активного топика используй
  команду `/engine <name>` прямо в Telegram.
- `CLAUDE_BIN` — путь к бинарю claude (по умолчанию `claude`).
- `CODEX_BIN` — путь к бинарю codex (по умолчанию `codex`).
- `OPENCODE_BIN` — путь к бинарю opencode (по умолчанию `opencode`).
- `CURSOR_BIN` — путь к бинарю Cursor CLI (по умолчанию `cursor-agent`).
- `CLAUDE_CWD` — дефолтный рабочий каталог (общий для всех движков).
- `CLAUDE_TIMEOUT` — таймаут claude, секунд (по умолчанию `3600`).
- `CODEX_TIMEOUT` — таймаут codex, секунд (по умолчанию `3600`).
- `OPENCODE_TIMEOUT` — таймаут opencode, секунд (по умолчанию `3600`).
- `CURSOR_TIMEOUT` — таймаут cursor, секунд (по умолчанию `3600`).
- `CODEX_MODEL` — дефолтная модель для Codex CLI, если в топике модель не
  выбрана явно через `/engine`.
- `CLAUDE_MODELS`, `CODEX_MODELS`, `OPENCODE_MODELS`, `CURSOR_MODELS` — запятая-разделённые
  списки моделей для UI `/engine`. Задавать не нужно: без них Jarvis
  спрашивает списки у самих CLI (см. «Списки моделей» ниже), env — это
  override, когда нужно показать только часть моделей или свою.
- `JARVIS_MODELS_TTL` — как часто перечитывать списки моделей, секунд
  (по умолчанию `600`).
- `OPENCODE_MODEL`, `OPENCODE_AGENT`, `OPENCODE_VARIANT` — опциональные параметры
  для `opencode run`; если не заданы, используются настройки самого opencode.
- `CURSOR_MODEL` — дефолтная модель для `cursor-agent` (`--model`), если в топике
  модель не выбрана через `/engine`; без неё CLI берёт свою.
- `CURSOR_MODELS_LIMIT` — сколько первых моделей из `cursor-agent --list-models`
  показывать в `/engine` (по умолчанию `30`): CLI отдаёт ~250 моделей, а у
  inline-клавиатуры Telegram потолок 100 кнопок.
- `CURSOR_MCP_CONFIG` — путь к `mcp.json` cursor, куда регистрируется Manager MCP
  (по умолчанию `~/.cursor/mcp.json`).
- `CURSOR_API_KEY` — ключ API вместо `cursor-agent login` (читает сам CLI).
- `PLAYWRIGHT_MCP_NPX` — абсолютный путь к `npx` для Playwright MCP. Если не
  задан, runtime-хелпер ищет `npx` в `PATH` и `~/.nvm/versions/node/*/bin/npx`.
- `PLAYWRIGHT_MCP_PACKAGE` — npm-пакет MCP-сервера (по умолчанию
  `@playwright/mcp@latest`).
- `PLAYWRIGHT_MCP_MODE` — `cdp` (по умолчанию) или `launch`.
- `PLAYWRIGHT_MCP_CDP_ENDPOINT` — endpoint для CDP. По умолчанию `chrome`,
  но можно указать `http://127.0.0.1:9222` или другой доступный endpoint.
- `PLAYWRIGHT_MCP_ARGS` — дополнительные аргументы Playwright MCP. Для CDP
  режима обычно используют capabilities, например `--caps=vision,pdf,devtools`.
- `JARVIS_PLAYWRIGHT_MCP` — `1`/`0`, глобальный рубильник браузера. `1`
  (по умолчанию) — `/browser on` разрешён, Playwright инъектируется per-topic.
  `0` — браузер недоступен вообще (даже если флаг топика включён).
- `JARVIS_SESSION_IDLE_MINUTES` — сколько минут простоя переживает сеанс,
  прежде чем закрыться сам. Дефолт `180`; `0` — авто-закрытие выключено.
- `JARVIS_CONTEXT_WARN_TOKENS` — порог предупреждения о большом контексте
  после ответа движка. Дефолт `150000`; `0` — предупреждения выключены.
- `JARVIS_DONE_CONFIRM_ON_DONE` — `1`/`0`, спрашивать ли после успешного
  done-похожего ответа “Задача завершена?” с кнопками закрытия сессии.
  Дефолт `1`. Старый `JARVIS_AUTOCLOSE_ON_DONE=0` поддержан как alias для
  отключения этого вопроса.
- `JARVIS_MANAGER_MCP` — `1`/`0`, аналогично для Jarvis Manager MCP (см. ниже).
- `JARVIS_MCP_NAME` — имя MCP-сервера в конфигах движков (по умолчанию `jarvis`).
- `JARVIS_MCP_PYTHON` — путь к Python для запуска MCP-сервера (по умолчанию
  `venv/bin/python` репозитория jarvis).
- `JARVIS_MCP_SCRIPT` / `JARVIS_MCP_DB` — переопределить пути к серверу/БД.
- `JARVIS_TOPIC_MCP_CONFIG` — путь к JSON с внешними MCP-серверами по роли
  топика (см. «Внешние MCP-серверы по роли топика»). Не задан или файла нет →
  таких серверов нет, это не ошибка.
- `JARVIS_TOPIC_MCP` — `1`/`0`, глобальный рубильник для них. Дефолт `1`.
- `JARVIS_SECRETARY_CHAT_ID` / `JARVIS_SECRETARY_THREAD_ID` — топик Секретаря:
  коммуникационный triage, reminders, webhook/IMAP notices. Если не заданы,
  используется совместимый alias `JARVIS_MANAGER_CHAT_ID` /
  `JARVIS_MANAGER_THREAD_ID` — старый топик Менеджера становится Секретарём.
- `JARVIS_TEAMLEAD_CHAT_ID` / `JARVIS_TEAMLEAD_THREAD_ID` — топик Тимлида:
  инженерные job/heartbeat notices и события трекера задач. Если не заданы,
  инженерные notices падают обратно в Секретаря, чтобы уведомления не терялись
  до миграции. Это **fallback-адресат**: нотис по job уходит топику, который
  job делегировал (см. «Кому уходит нотис по job»), и в Тимлида — только когда
  инициатор неизвестен.
- `JARVIS_LOG_TTL_DAYS` — сколько дней хранить записи `messages_log`,
  завершённые (`done`/`failed`/`cancelled`) `jobs` и завершённые
  `agent_triggers`. Дефолт `30`. `0`, `none`, `off`, `false`, `no` —
  отключают авто-cleanup. `pending` jobs/triggers (включая scheduled jobs с
  `not_before` в будущем) **никогда** не удаляются.
- `JARVIS_JOBS_CONCURRENCY` — сколько делегированных Менеджером задач
  выполняется параллельно. Дефолт `5`, минимум `1`. Задачи разных топиков идут
  одновременно; внутри одного топика — строго по очереди (per-topic лок). При
  `1` поведение как раньше (одна задача за раз).
- `JARVIS_AGENT_TRIGGERS_CONCURRENCY` — сколько внешних non-job триггеров
  (`agent_triggers`, см. «Внешние триггеры») выполнять параллельно. Дефолт `5`,
  минимум `1`. Триггеры не имеют `job_id` и не участвуют в `manager_interrupt`
  / heartbeat jobs.
- `JARVIS_HEARTBEAT_INTERVAL` — частота сканирования in_progress job'ов
  (секунды). Дефолт `300` (5 мин), минимум `30`.
- `JARVIS_HEARTBEAT_WARN` — после скольких секунд работы job'а слать
  инициатору job'а нотис «работает долго». Дефолт `900` (15 мин).
- `JARVIS_HEARTBEAT_FAIL` — после скольких секунд принудительно помечать
  job как failed. Subprocess сам по себе не убивается — для реального
  прерывания агент Менеджер использует `manager_interrupt`. Дефолт
  `3600` (60 мин).
- `JARVIS_REMINDERS_INTERVAL` — частота сканирования `reminders` (секунды).
  Дефолт `60`.
- `JARVIS_REMINDERS_TZ` — таймзона для парсинга времён в schedule
  (`daily HH:MM` и т.п.). Дефолт `Europe/Moscow`.

### Playwright MCP — on-demand для всех движков

Jarvis сам не является MCP-клиентом: браузерные tools поднимают внешние CLI.
~30 `browser_*` тулов — это заметный объём контекста в каждом запросе, поэтому
Playwright **не** регистрируется глобально, а инъектируется **per-invocation**
только для топиков, где включён флаг `mcp_playwright` (команда `/browser on`
или MCP-tool `manager_set_browser`). Manager MCP, напротив, остаётся глобальным
(он лёгкий и нужен оркестрации) — см. ниже.

Единый источник спеки сервера — `playwright_command_args()` в
`engines/playwright_mcp.py` (резолвит `npx` + аргументы). Каждый адаптер
переводит её в свой диалект CLI на каждый вызов:

- **claude**: флаг `--mcp-config '<inline-json>'` (аддитивно к глобальному
  Manager MCP, без `--strict-mcp-config`).
- **codex**: оверрайды `-c mcp_servers.playwright.command=… -c …args=[…] -c
  …enabled=true` поверх `~/.codex/config.toml`.
- **cursor**: не поддержан. `cursor-agent` читает MCP только из `~/.cursor/mcp.json`
  и `<cwd>/.cursor/mcp.json`, per-invocation подключения нет. `/browser on` в
  cursor-топике ставит флаг, но бот предупреждает: браузер заработает только
  после смены движка.
- **opencode**: у `opencode run` нет per-invocation MCP-флага, поэтому Jarvis
  клонирует глобальный `opencode.json` (Manager MCP и provider-настройки
  сохраняются), добавляет `mcp.playwright` во временный файл и подсовывает его
  через `OPENCODE_CONFIG=<tempfile>` на конкретный запуск. Temp-файл удаляется
  после ответа.

Команда MCP по умолчанию: абсолютный `npx -y @playwright/mcp@latest --cdp-endpoint=chrome`.
Абсолютный путь важен для systemd: в сервисе Jarvis nvm обычно не попадает в
`PATH`, а `npx` лежит именно там.

Если нужен порт, а не channel name, задай `PLAYWRIGHT_MCP_CDP_ENDPOINT=http://127.0.0.1:9222`.

При старте/активации движка Jarvis ещё и **снимает** старую глобальную
регистрацию Playwright (`disable_global_playwright_mcp`), если она осталась от
прежних версий — чтобы on-demand-модель не нарушалась always-on сервером.

> **codex/opencode — проверка на боевой версии CLI.** Парсинг `-c
> mcp_servers.playwright.args=[…]` у codex и поведение `OPENCODE_CONFIG` у
> opencode зависят от версии CLI. Если браузер в этих движках не поднимается:
>
> - **codex**: пропиши Playwright руками в `~/.codex/config.toml`
>   (`[mcp_servers.playwright]` с `command`/`args`/`enabled = true`) — тогда
>   браузер будет always-on для codex; либо проверь формат `-c` своей версии
>   (`codex exec -c 'mcp_servers.playwright.enabled=true' …`).
> - **opencode**: пропиши `mcp.playwright` прямо в
>   `~/.config/opencode/opencode.json` (always-on), либо убедись, что твоя
>   версия читает `OPENCODE_CONFIG`.
>
> claude (основной канал) работает через `--mcp-config` без оговорок.

### Внешние MCP-серверы по роли топика

Один форум часто обслуживает несколько личностей: служебные топики
Секретаря/Тимлида и рабочие топики проектов. Если у них один и тот же внешний
сервис — трекер задач, доска, внутреннее API — им обычно нужны разные credentials:
ход исполнителя не должен выглядеть как ход manager-level пользователя.

Jarvis решает это ролью топика. `resolve_topic_role()` отдаёт:

- `secretary` — explicit `JARVIS_SECRETARY_*`, иначе совместимый alias
  `JARVIS_MANAGER_*`;
- `teamlead` — explicit `JARVIS_TEAMLEAD_*`;
- `agent` — все остальные проектные/исполнительские топики.

`resolve_manager_topic()` сохранён как compatibility API и теперь указывает на
Секретаря. Выбранный движок на роль не влияет — роль принадлежит топику.

Сами серверы объявляются в JSON-файле из `JARVIS_TOPIC_MCP_CONFIG` — про сам
сервис Jarvis не знает ничего:

```json
{
  "servers": [
    {
      "name": "tracker",
      "url": "https://example.org/rest-mcp.php",
      "roles": {
        "manager":   {"headers": {"Authorization": "Bearer <manager-token>"}},
        "secretary": {"headers": {"Authorization": "Bearer <manager-token>"}},
        "teamlead":  {"headers": {"Authorization": "Bearer <manager-token>"}},
        "agent":     {"headers": {"Authorization": "Bearer <agent-token>"}}
      }
    }
  ]
}
```

- `roles` необязателен: без него сервер подключается любой роли с общими
  `headers`. С ним — только перечисленным ролям, а `headers` роли перекрывают
  общие. Роль может переопределить и `url`.
- Для совместимости `secretary` и `teamlead` берут роль `manager`, если в
  конфиге нет явной записи под новую роль. Это позволяет старому
  `jarvis-topic-mcp.json` сразу дать обоим служебным топикам manager-level
  доступ к сервису.
- `enabled: false` выключает запись, не удаляя её.
- Поддерживаются только **remote HTTP** серверы: роль здесь — это креды, а
  stdio-сервер несёт их в argv/env, откуда они видны в списке процессов.
- Файл перечитывается по mtime — правка токена подхватывается без рестарта.
- `JARVIS_TOPIC_MCP=0` — глобальный рубильник.

**Отсутствие конфига — не ошибка.** Нет переменной, нет файла, битый JSON, плохая
запись: Jarvis пишет это в лог и работает без этих серверов. Топик без тула —
неудобство, бот, который вообще не отвечает, — авария; до 2026-07-25 это было
ровно второе (нехватка файла роняла `RuntimeError` на КАЖДОМ сообщении).

Инъекция по движкам:

- **claude**: `--mcp-config` с remote HTTP server и headers.
- **codex**: `codex exec` получает временный
  `$CODEX_HOME/jarvis-topic-mcp-*.config.toml` + `--profile <name>`, чтобы
  токены не попадали в process argv. Файл создаётся с mode `0600` и удаляется
  после завершения процесса. Jarvis ставит `--profile` перед subcommand:
  `codex --profile <name> exec resume ...`.
  Persistent `codex app-server` временный профиль не использует, поэтому там
  используется `-c mcp_servers.<name>.*` (токен при этом в argv — цена
  persistent-режима).
- **opencode**: временный `OPENCODE_CONFIG` — клон глобального `opencode.json`
  плюс `mcp.<name>`; удаляется после ответа. Если подключать нечего, temp-файл
  НЕ создаётся и opencode идёт со своим штатным конфигом.
- **cursor**: не поддержан (topic-MCP `JARVIS_TOPIC_MCP_CONFIG` до cursor не доходит —
  нет per-invocation MCP).

Глобальные user-scope регистрации этих серверов в `~/.codex/config.toml` и
`~/.claude.json` должны отсутствовать, иначе identity снова станет зависеть от
конфига CLI, а не от роли топика.

### Внешние триггеры (`agent_triggers`)

Публичный контракт для любой интеграции — трекера задач, CI, cron: вставь строку
в `agent_triggers`, и бот проведёт обычный LLM turn в топике.

```sql
INSERT INTO agent_triggers(chat_id, thread_id, text, source, role, status, created_at)
VALUES (?, ?, ?, 'mytracker', 'executor', 'pending', ?);
```

Почему отдельная таблица, а не `jobs`: job-обёртка создаёт служебные
manager-notice на финальном ответе и interrupt, что даёт ложные пробуждения
Менеджера. `agent_triggers_worker` запускает ход через тот же topic-lock и
`/persistent`-путь, но **без** `job_id`, heartbeat и `manager_interrupt`. Перед
запуском входящее логируется в `messages_log` как `<source>_inject`.

- `source` — свободная метка интеграции, нужна для логов и текста отказа
  `ask_user`.
- `role` — кому адресован триггер: `'executor'` или `'manager'`. Пишет
  интегратор, читает `ask_user` (см. ниже). `NULL` = роль неизвестна, такие
  триггеры гард не блокирует.

### Вопросы агента пользователю (`ask_user`)

Агент запускается неинтерактивно и перебить его нельзя — но он может сам
спросить и дождаться ответа. MCP-инструмент `ask_user(question, thread_id,
options=[...])` публикует вопрос в топик и **блокирует агента**, пока не придёт
ответ. Работает для всех трёх движков: Manager MCP зарегистрирован глобально.

- `options` рендерятся inline-кнопками. В `callback_data` идёт **индекс**
  варианта, а не текст — Telegram ограничивает `callback_data` 64 байтами.
- Ответить можно и обычным сообщением в топик. Бот перехватывает его **до**
  постановки в очередь — иначе ответ ушёл бы агенту вторым, отдельным ходом
  (агент в этот момент держит lock топика, стоя в `ask_user`).
- Гонок нет: ответ пишется через `UPDATE ... WHERE status='pending'`, так что
  второй ответ (нажали кнопку после того, как написали текстом) отклоняется.
- Таймаут (`timeout_seconds`, дефолт 1800, потолок 3600) **не считается
  согласием**: агент получает `status='timed_out'` и `answer=null`, кнопки
  гаснут, под вопросом появляется «⌛ Вопрос истёк». Явный `default` вернётся
  только если его передали.
- **Grace-окно 15 минут.** Ответ текстом на уже истёкший вопрос не пропадает:
  бот помечает вопрос `answered_late` и отправляет сообщение обычным ходом, но
  с пометкой «[Это ответ на твой вопрос «…», истёкший по таймауту]» — иначе
  агент читает реплику как пришедшую из ниоткуда. Окно считается от момента
  истечения (`answered_at`), а не от создания вопроса: ожидание само по себе
  длиннее grace-окна.
- Вопрос пишется и в `messages_log` (`kind='ask_user'`) — по нему журнал хода
  видит, что топик ушёл вперёд, и продолжает трансляцию ниже вопроса.
- Верхняя граница ожидания — время жизни процесса агента (`CLAUDE_TIMEOUT` и
  аналоги, по умолчанию 3600 с).

Канал между процессами — таблица `ask_requests` в `bot_state.db`: MCP-сервер
пишет вопрос и поллит ответ, бот принимает нажатие кнопки или сообщение и
кладёт ответ туда же.

Системный `[SYSTEM:]`-блок велит агенту звать `ask_user` перед опасными или
необратимыми действиями и при неоднозначной задаче — и не дёргать человека по
тому, что можно выяснить самому (прочитать код, запустить команду, глянуть git).

#### Запрет для исполнителя внешней задачи (с 2026-07-22)

Если ход поднят внешней интеграцией и адресован **исполнителю**
(`agent_triggers` этого топика: `status='in_progress'`, `role='executor'`), любой
вызов `ask_user` отклоняется: в Telegram ничего не уходит, агент получает
`{status:'blocked', task:'#N', source:..., error:...}` с требованием задать
вопрос комментарием в задаче и завершить ход. Ответ человека интеграция
подхватит и разбудит агента новым триггером — диалог по задаче целиком остаётся
в её истории, а не растекается по чату.

Гард смотрит на `role`, а **не** на `source`: контракт общий для любого трекера.
До 2026-07-25 в SQL был зашит `source` одной конкретной интеграции, и чужая интеграция гарда не
получала.

Менеджера запрет не касается (`role='manager'`), как и обычных ходов от
сообщений человека. `role IS NULL` или отсутствие колонки → запрет **не**
срабатывает: правка не должна включаться вслепую на неперезапущенном стеке.
Номер задачи берётся регуляркой из текста триггера; не распознан — в ответе
`'#?'`, блокировка всё равно работает.

### Живой процесс `/persistent`

Обычный путь Jarvis сериализует сообщения в топике через topic-lock: если агент
уже работает, следующее сообщение ждёт очереди. `/persistent on` включает
исключение для текущего движка (`claude`, `codex` или `opencode`; `cursor` не
поддержан): живой subprocess держится
между ходами, а сообщение, пришедшее во время активного хода, отправляется в
него сразу и подтверждается фразой «добавил к текущей работе».

- `claude`: запускается `claude --print --input-format stream-json
  --output-format stream-json`; новые сообщения пишутся в stdin.
- `codex`: запускается experimental `codex app-server --listen stdio://`.
  Jarvis делает JSON-RPC `initialize` с `experimentalApi: true`, затем
  `thread/start` или `thread/resume`; новый ход идёт через `turn/start`, а
  сообщение во время активного хода — через `turn/steer` с `expectedTurnId`.
- `opencode`: запускается `opencode serve --hostname 127.0.0.1 --port 0`
  (`engines/persistent_opencode.py`), по одному серверу на топик. Jarvis
  подписывается на SSE `/event`, открывает или создаёт сессию (`POST /session`
  с правилом «разрешено всё», аналог `--dangerously-skip-permissions`) и шлёт
  сообщения через `POST /session/{id}/prompt_async`. Сообщение во время хода
  уходит тем же вызовом: сервер принимает его в ту же сессию, и модель
  учитывает его на следующем шаге того же хода. Конец хода — `session.idle`.
  `[SYSTEM:]`-блок и FILE-маркер передаются полем `system` на каждом
  сообщении. `permission.asked` для старых сессий из `opencode run`
  автоматически получает ответ `always`, `question.asked` отклоняется.
  Живая проверка: `scripts/smoke_persistent_opencode.py`.

Важно для Codex: `codex exec` не подходит для true persistent. Проверено на
`codex-cli 0.131.0`: `codex exec --input-format stream-json --help` падает с
`unexpected argument '--input-format'`; `codex exec --json -` без EOF не
стартует как интерактивный поток; `codex exec --json 'Reply exactly: OK'`
делает один turn и завершает процесс. При этом `codex app-server --listen
stdio://` отвечает на `initialize`, создаёт thread через `thread/start`, даёт
`inProgress` turn через `turn/start` и принимает `turn/steer` во время активного
turn.

Playwright MCP в persistent Codex подключается через `-c
mcp_servers.playwright.*` overrides при старте app-server; topic-MCP — через
`-c mcp_servers.<name>.*`, потому что `app-server` не принимает
временный topic-профиль. Роль топика и флаг `/browser` нужно выставить до
первого persistent-сообщения в топике: уже запущенный живой процесс не
перечитывает MCP-конфиг до перезапуска worker-а (`/stop`, `/new`, `/reset`,
`/engine`, `/persistent off/on` или idle reaper).

### Журнал хода: всегда в хвосте топика

Журнал — одно накопительное сообщение, которое редактируется по мере шагов. Две
вещи, без которых он выглядит зависшим:

- **Отвязка от устаревшего сообщения.** Перед каждым обновлением журнал
  сверяется с хвостом топика (реестр `bot/topics.py: last_topic_message` плюс
  `messages_log` и `ask_requests` — вопросы шлёт другой процесс). Если ниже уже
  уехал вопрос `ask_user`, ответ или реплика человека, журнал отцепляется и
  продолжает трансляцию **новым сообщением внизу**; отрисованное остаётся в
  старом, дублей нет. Раньше после интерактива шаги дописывались в сообщение,
  уехавшее за верхний край экрана.
- **Терпимость к сетевой икоте.** `RetryAfter`, `TimedOut`, `NetworkError` —
  не отказ: строки ждут в буфере и уедут следующим флешем. Журнал замолкает
  только после трёх подряд ошибок, которые повтором не лечатся (топик удалён,
  отняты права). До этого один `ReadTimeout` глушил трансляцию до конца хода.
- **Heartbeat.** Если агент молчит дольше минуты, в журнале появляется строка
  «⏳ работаю N мин, шагов M (тишина S с)» — видно, что процесс жив.

Ответ агента доставляется через `AIORateLimiter` (флуд-контроль Telegram
выдерживается, а не превращается в ошибку) плюс три повтора с backoff; если
текстом не вышло совсем — ответ уходит `.md`-вложением.

Пустой ответ движка не показывается как «(пустой ответ)»: бот один раз
переспрашивает агента в той же сессии («доложи статус: что сделал, где
остановился, что мешает»), и только если и это пусто — печатает диагностику
(движок, session_id, число шагов, что делать дальше).

### Что видно из рассуждений агента

Ход работы пишется в журнал (одно накопительное сообщение на запрос). Что
именно туда попадает — зависит от движка, и разница принципиальная:

| Движок | Инструменты | Рассуждения |
|---|---|---|
| `codex` | ✅ `🔧 exec …` | ✅ **текстом** (событие `reasoning`) |
| `claude` | ✅ `🔧 <tool> …` | ⚠️ только факт: `💭 размышляет…` |
| `opencode` | ✅ `🔧 …` | ❌ отдельных событий нет |
| `cursor` | ✅ `💻/📖/✏️/🔌 …` | ❌ отдельных событий нет |

**У claude текст рассуждений получить нельзя.** CLI отдаёт блок `thinking` с
пустым полем и одной лишь `signature`; с `--include-partial-messages` приходят
`thinking_delta`, но и там поле `thinking` пустое — только счётчик
`estimated_tokens` (проверено на 2.1.207, в т.ч. с `--effort high`). Поэтому в
журнал пишется лишь пометка, что ход включал размышление.

У codex рассуждения приходят полноценным текстом — если нужен видимый ход
мысли, это единственный движок, который его отдаёт.

### Сеансы

Топик — это рабочее место (cwd, движок, модель), а не бесконечная сессия.
Внутри него живут **сеансы** — как окна терминала:

- открывается первым сообщением;
- закрывается командой `/close` или сам, после `JARVIS_SESSION_IDLE_MINUTES`
  простоя (дефолт 180);
- при закрытии сбрасывается только контекст LLM-сессии; топик остаётся.

Признак закрытого сеанса в БД — `sessions.last_activity_at IS NULL`. Новый
`session_id` создаётся лениво, при следующем сообщении, поэтому пустые сессии
не плодятся.

**Зачем.** Стоимость хода линейна по размеру контекста, а агентный цикл
переотправляет его на каждой итерации. Вечная сессия разрастается до потолка
окна модели, и каждое сообщение начинает стоить максимум. Короткие сеансы —
единственный работающий ограничитель: кэш (94–96% попаданий) эту проблему не
решает, потому что платится и за чтение кэша тоже.

Контекст прошлых разговоров не переносится — как и в терминале, где новый
процесс восстанавливает понимание из кода и CLAUDE.md. Переписка при этом
никуда не девается: она лежит в `messages_log`, и движок может поднять её сам
через MCP-инструмент `manager_inbox` (координаты топика ему выдаёт
`[SYSTEM:]`-блок).

Команды:

- `/close` — закрыть сеанс сейчас.
- `/new`, `/reset` — закрыть и сразу открыть новый.
- `manager_close_session` (MCP) — то же самое из другого топика: Менеджер
  закрывает чужой сеанс, не заходя в него. MCP-сервер — отдельный процесс и
  не видит `active_procs` / `persistent_workers` бота, поэтому он закрывает
  сеанс в БД и ставит `sessions.close_requested`; `close_requests_worker`
  бота раз в 2 с добивает процессы топика и пишет туда нотис
  (`kind='session_closed'`). Без этого шага топик с `/persistent on`
  продолжил бы отвечать из живого процесса со старым контекстом.
- `manager_archive_topic` / `manager_delete_topic` (MCP) — конец жизни самого
  топика, а не сеанса. Оба сначала закрывают сеанс и **ждут**, пока бот
  погасит `close_requested`: до этого момента процессы топика ещё живы, и
  свёрнутый (тем более удалённый) топик они бы пережили. Архивация обратима
  и потому дефолт; удаление сносит тред вместе с сообщениями и чистит
  состояние в БД — `sessions`, напоминания топика, а pending job'ы, триггеры
  и вопросы `ask_user` переводит в `cancelled`, чтобы они не выстрелили в
  несуществующий тред.
- `/session` — состояние сеанса: сколько простаивает, когда закроется.
- `/tokens` — оценка размера контекста текущей сессии.
- `/usage` — остаток лимитов подписки claude и codex по локальным источникам
  (`engines/limits.py`); сети не требует, показывает то, что записал CLI.
- `scripts/session_tokens.py --chat-id <id> --thread-id <id>` — та же
  диагностика из shell. Можно также передать `--engine <name> --session-id <id>`.

При смене движка через `/engine` бот спрашивает, переносить ли контекст. «Да»
больше **не** означает «старый движок пишет резюме» — это был самый дорогой
вызов из возможных (полный проход по всей истории). Вместо этого новый движок
получает указание поднять историю топика самому через `manager_inbox` и платит
только за то, что реально прочитал.

Источники оценки:

- `claude` — точный последний `message.usage` из
  `~/.claude/projects/<cwd>/<session_id>.jsonl` (`input + cache_read +
  cache_creation`).
- `opencode` — точные токены последнего assistant-message из
  `~/.local/share/opencode/opencode.db`.
- `cursor` — не отслеживается.
- `codex` — best-effort estimate по размеру локального JSONL, потому что
  локальный session-log Codex CLI пока не даёт стабильного usage-поля.

### Списки моделей

Меню моделей в `/engine` не захардкожено — каждый адаптер спрашивает свой CLI
(`engines/model_cache.py`):

| Движок | Откуда берётся список |
|---|---|
| claude | алиасы `opus`/`sonnet`/`haiku` (CLI принимает их всегда) + `additionalModelOptionsCache` из `~/.claude.json` — то, что доступно аккаунту сверх алиасов, вроде `claude-fable-5[1m]` |
| codex | `~/.codex/models_cache.json` (только модели с `visibility: list`) |
| opencode | вывод `opencode models` — все сконфигурированные провайдеры |
| cursor | вывод `cursor-agent --list-models` (строки «id - Название», названия идут в подписи кнопок), первые `CURSOR_MODELS_LIMIT`; фолбэк — `auto` |

Env `CLAUDE_MODELS` / `CODEX_MODELS` / `OPENCODE_MODELS` / `CURSOR_MODELS` перекрывают источник.
Если источник молчит (CLI не установлен, конфиг битый), адаптер отдаёт свой
фолбэк-список — меню не пустеет никогда.

Опрос кэшируется на `JARVIS_MODELS_TTL` секунд (600). Он не мгновенный
(`opencode models` — ~1.5с), а `engine.models` читают async-хендлеры, поэтому:
кэш прогревается в потоке на старте (`prewarm_models()` в `_post_init`), а
протухший список отдаётся сразу и обновляется фоновым потоком. Новый адаптер
должен реализовать `models` (property) и `prewarm_models()` — проще всего через
`cached_models()`/`prewarm()` из `engines/model_cache.py`.

### Как подключить новый движок

Per-topic MCP — часть контракта `Engine.call_stream` (см.
`engines/__init__.py`). Новый адаптер обязан принять и обработать три
параметра:

- `system_prefix: str | None` — постоянный `[SYSTEM:]`-блок. Положи его в
  системный канал своего CLI (как `--append-system-prompt` у claude). Если
  канала нет — префиксуй prompt **только на новой сессии** (на resume он уже
  в транскрипте), как сделано в codex/opencode/cursor.
- `mcp_playwright: bool` — если `True`, инъектируй Playwright per-invocation:
  возьми спеку из `playwright_command_args()` и переведи в механизм своего CLI
  (флаг конфига / оверрайд / временный конфиг через env). Если CLI вообще не
  умеет per-invocation MCP — задокументируй ручную глобальную настройку как
  фолбэк (раздел выше).
- `mcp_topic_role: str | None` — если задано, инъектируй внешние MCP-серверы
  этой роли через `engines.topic_mcp.servers_for_role()` (пустой список —
  штатная ситуация, серверов нет). Для CLI, где секреты могли
  бы попасть в argv, используй файл/профиль, а не command-line override.

### Jarvis Manager MCP (для агента-Менеджера)

По той же схеме Jarvis регистрирует свой собственный stdio MCP-сервер,
дающий доступ к состоянию бота и согласованным действиям Менеджера. Сервер
живёт в пакете `mcp_server/` (точка входа — `scripts/jarvis_mcp_server.py`) и
запускается тем же venv-Python'ом.

Доступные tools (Этап 1):

- `manager_topics` — список всех топиков (chat_id, thread_id, title, cwd,
  engine, model, session_id, updated_at, last_message_at). Фильтры:
  `cwd_contains`, `engine`, `limit`.
- `manager_inbox` — лог сообщений одного топика. Параметры: `chat_id`,
  `thread_id`, `since` (ISO UTC), `limit`, `text_limit`, `direction`.
- `manager_set_browser` — включить/выключить браузер (Playwright MCP) для
  топика (`thread_id`, `enabled`). Аналог команды `/browser`. Применяется со
  следующего сообщения/джоба, контекст сессии сохраняется.
- `manager_close_session` — закрыть сеанс топика (`thread_id`,
  `interrupt_active=true`). Аналог команды `/close`: контекст движка
  сбрасывается, топик (cwd/engine/model/флаги) остаётся. По умолчанию сначала
  прерывает активные job'ы топика, как `manager_interrupt`. Возвращает
  `was_open` и `interrupted_jobs`.
- `manager_archive_topic` — свернуть топик в Telegram (`closeForumTopic`):
  история и настройки на месте, писать нельзя. `reopen=true` разворачивает
  обратно. Обратимая операция — ею и заканчивается жизнь временного топика.
- `manager_delete_topic` — удалить топик со всеми сообщениями
  (`deleteForumTopic`) и убрать его состояние из БД. Необратимо, поэтому с
  guard'ами: отказ на топике Менеджера, на General, на топике с `in_progress`
  job'ом и на неподтверждённом закрытии сеанса (обходится `force=true`).
  `messages_log` по умолчанию сохраняется, `purge_log=true` сносит и его.

(Это не полный список — есть и write-tools: `manager_send`, `manager_set_engine`,
`manager_create_topic` и др. См. декораторы `@mcp.tool` в `mcp_server/tools/` и
`plugins/*/mcp_tools.py`.)

#### Кому уходит нотис по job

`manager_send(as_user=True)` ставит job в очередь топика-исполнителя. Когда job
отвечает (а также при `manager_interrupt` и при heartbeat-предупреждениях), бот
шлёт короткий нотис «есть ответ» и будит адресата AUTO-KICK'ом.

Адресат — **топик, который job делегировал**: `manager_send` пишет его в
`jobs.origin_chat_id` / `jobs.origin_thread_id`. Сервер общий на все топики и
вызывающего сам не знает, поэтому агент обязан передавать
`origin_thread_id=<свой thread_id>` (он есть в его `[SYSTEM:]`-блоке). Если
`origin_*` пусты — нотис уходит по роли `teamlead`, как раньше.

Зачем: до 27.08.2026 адресат был зашит константой, и job, делегированный
Секретарю, будил ещё и Тимлида. Тот вклинивался в чужую задачу и слал в тот же
топик свой `manager_send` — две сессии на один топик, `thread-store conflict`
у codex и два упавших job'а.

### QueueWarden: канал уведомлений «Бот»

Плагин `queuewarden` (включается в `JARVIS_PLUGINS`).
`plugins/queuewarden/notifications.py` держит long-poll `GET <url>/api/bot/notifications?wait=25`
токеном учётки-моста по каждой установке QW — отдельной задачей, со своим backoff:

```
QUEUEWARDEN_INSTALLATIONS=alpha,beta
QUEUEWARDEN_ALPHA_URL=https://alpha.example.com
QUEUEWARDEN_ALPHA_TOKEN=<токен моста alpha>
QUEUEWARDEN_BETA_URL=https://beta.example.com
QUEUEWARDEN_BETA_TOKEN=<токен моста beta>
```

Без `QUEUEWARDEN_INSTALLATIONS` — одна установка из `QUEUEWARDEN_URL` и
`QUEUEWARDEN_MCP_TOKEN` (нужны оба), MCP `queuewarden`.
Каждое уведомление — отдельный `agent_triggers` с `source='queuewarden'` в топик Тимлида
(переопределение — `JARVIS_QW_NOTICE_CHAT_ID`/`JARVIS_QW_NOTICE_THREAD_ID`); в тексте — установка
и MCP-сервер `queuewarden_<slug>`, через который агент сверяет уведомление с задачей. Эти серверы Тимлиду выдаёт `JARVIS_TOPIC_MCP_CONFIG` (роль `teamlead`, токен моста
установки). Повторы гасятся по `notificationId` (отметка в `integration_seen_items` с kind
`bot_notification:<slug>`, в одной транзакции с триггером), ack — только после коммита.
Нет установок или `JARVIS_QW_NOTIFICATIONS=0` — воркер выключен; 404 (канал не выложен) или
отвергнутый токен — установка тихо ждёт, остальные работают.

Уведомления идут сериями, поэтому `agent_triggers_worker` берёт триггер QW, только когда самому
старому из ожидающих исполнилось `JARVIS_QW_COALESCE_SECONDS` (по умолчанию 60), и забирает
вместе с ним все ожидающие триггеры QW этого топика — Тимлид получает серию одним ходом
(`build_batch_prompt`) и видит актуальное состояние, а не хвост очереди. Тимлид — технический
советник: пишет оператору только о плане, переводе в «Готово», отказе из бэклога, проблемах и
human gate (правила — раздел «QueueWarden» в `AGENTS.md` рабочего каталога топика), каждый блок с шапкой
`[<установка>] <задача> · <проект> · <название>`. Писать не о чем — отвечает `[[SILENT]]`: такой
ответ в топик не уходит, журнал хода удаляется. Маркер действует только для триггеров QW.

### QueueWarden: доски в `/board`

`/board` показывает кнопку на каждую доску из `BOARD_MINIAPPS` (`подпись|https-адрес|short_name`
через запятую):

```
BOARD_MINIAPPS=alpha|https://alpha.example.com/miniapp|qwalpha,beta|https://beta.example.com/miniapp|qwbeta
```

В личке кнопка — web_app, в группах/форумах (там web_app запрещён) — ссылка
`t.me/<бот>/<short_name>`; `short_name` — direct link, заведённый в BotFather на этот адрес.
Доска без `short_name` видна только в личке. Без списка — одна доска из `BOARD_MINIAPP_URL`/
`BOARD_MINIAPP_SHORT_NAME`. Чтобы миниапп пустил, в панели установки (Настройки → Telegram)
должен стоять токен этого бота.

### ActiveCollab

Если в локальном `.env` заданы `ACTIVE_COLLAB_URL` и `ACTIVE_COLLAB_TOKEN`,
Менеджеру доступны следующие MCP-инструменты:

- `manager_activecollab_my_tasks`, `manager_activecollab_task`,
  `manager_activecollab_stages`, `manager_activecollab_job_types` — чтение
  задач, комментариев, стадий и типов работ;
- `manager_activecollab_check_updates` — хранит в `bot_state.db` baseline и
  при следующих вызовах возвращает только новые назначенные задачи и новые
  уведомления о комментариях к ним;
- `manager_activecollab_add_comment`, `manager_activecollab_track_time`,
  `manager_activecollab_move_task` — внешние write-действия. Вызывать их можно
  только после явного поручения оператора с задачей и данными действия.

Стадия в этой установке ActiveCollab — это task list; `move_task` принимает её
точное название, например `В работе` или `Можно тестировать`.

Точки записи в `messages_log`: входящие пользовательские реплики
(`direction='in'`, `kind='user_text'`) и финальные ответы бота
(`direction='out'`, `kind='bot_reply'` / `'spawn_reply'` /
`'session_closed'`). Промежуточные tool-use'ы не логируются — они шумные.

Проверка:

```bash
claude mcp list
codex mcp list
opencode mcp list
```

У cursor Manager MCP лежит в `~/.cursor/mcp.json` (ключ `mcpServers.jarvis`).

### Переход на Codex CLI

1. `npm i -g @openai/codex`, затем `codex login` (ChatGPT) или `export OPENAI_API_KEY=...`.
2. Добавить в `.env`: `JARVIS_ENGINE=codex`. При необходимости зафиксировать
   модель: `CODEX_MODEL=gpt-5.5`.
3. `systemctl --user restart jarvis-bot.service`.

### Переход на opencode

1. Убедиться, что `opencode` установлен и авторизован:
   ```bash
   opencode --version
   opencode auth list
   ```
2. При необходимости задать модель/агента через opencode config или env:
   `OPENCODE_MODEL=provider/model`, `OPENCODE_AGENT=build`.
3. Добавить в `.env`: `JARVIS_ENGINE=opencode`. Если `opencode` установлен через nvm
   и не виден systemd-сервису, также задать `OPENCODE_BIN=/полный/путь/к/opencode`.
4. `systemctl --user restart jarvis-bot.service`.

### Переход на cursor

1. Установить Cursor CLI и авторизоваться:
   ```bash
   curl https://cursor.com/install -fsS | bash
   cursor-agent login      # либо env CURSOR_API_KEY
   cursor-agent status
   ```
2. Добавить в `.env`: `JARVIS_ENGINE=cursor`. Если `cursor-agent` не виден
   systemd-сервису, задать `CURSOR_BIN=/полный/путь/к/cursor-agent`.
3. `systemctl --user restart jarvis-bot.service`.

Ограничения cursor: нет `/persistent`, Playwright (`/browser`) и topic-MCP; лимиты
подписки (`/usage`) и расход контекста сессии не отслеживаются.

## Запуск вручную

```bash
./venv/bin/python telegram_bot.py
```

## Автозапуск через systemd (user unit)

Юнит рассчитан на репозиторий в `~/projects/jarvis`; если он лежит в другом
месте, поправьте `WorkingDirectory` и `ExecStart`. На Windows — см.
[docs/windows.md](docs/windows.md#автозапуск).

```bash
mkdir -p ~/.config/systemd/user
cp systemd/jarvis-bot.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now jarvis-bot.service

# чтобы бот жил без активной сессии:
sudo loginctl enable-linger "$USER"
```

Полезные команды:

```bash
systemctl --user status jarvis-bot
systemctl --user restart jarvis-bot
systemctl --user stop jarvis-bot
journalctl --user -u jarvis-bot -f
```

## Файлы

`telegram_bot.py` — точка входа (~60 строк). Весь код живёт в пакете `bot/`;
до 2026-07-25 это был один файл на 4924 строки.

Слои — строго вниз, без циклов: `settings` → `db` → `queues`/`topics` →
`sessions` → `delivery` → `llm` → `handlers` → `jobs`/`workers` → `app`.

| Модуль | Что внутри |
|---|---|
| `bot/settings.py` | env и константы; ничего не импортирует из `bot.*` |
| `bot/db.py` | схема SQLite, идемпотентные миграции, `log_message`. `DB_PATH` здесь — единственное, что решает, с каким файлом работает бот |
| `bot/queues.py` | `jobs` и `agent_triggers`: атомарный claim/finish, уборка старых записей |
| `bot/topics.py` | топик как единица работы: ключ, роль, реестры локов/процессов/живых воркеров |
| `bot/sessions.py` | сеансы, флаги топика, handoff при смене движка |
| `bot/asks.py` | `ask_user` — вопрос агента и приём ответа |
| `bot/formatting.py` | Markdown → HTML Telegram, нарезка по лимиту |
| `bot/delivery.py` | отправка, журнал хода, длинные ответы файлом, маркеры `[[FILE:]]` |
| `bot/llm.py` | системный префикс и `call_llm_stream` |
| `bot/rich_message.py` | Rich Messages (Bot API 10.1+) → markdown для агента; PTB этого поля пока не знает |
| `bot/plugins.py` | контракт плагина (`Plugin`, `Command`, `TriggerSource`) и загрузка по `JARVIS_PLUGINS` |
| `bot/timeutil.py` | `utcnow()` — наивное UTC-время, в котором хранятся даты в БД |
| `bot/handlers/commands.py` | простые команды: `/start`, `/session`, `/bind`, … |
| `bot/handlers/engine.py` | `/engine` и его инлайн-диалог выбора движка и модели |
| `bot/handlers/toggles.py` | `/browser`, `/persistent`, вопрос «задача завершена?» |
| `bot/handlers/messages.py` | текст, фото, документы; обычный ход и ход через живой процесс |
| `bot/jobs.py` | выполнение job Менеджера, `/spawn`, внешних триггеров |
| `bot/workers.py` | фоновые циклы ядра: heartbeat, очереди, reaper, уборка (циклы плагинов — в `plugins/`) |
| `bot/app.py` | сборка `Application`, регистрация хендлеров, старт воркеров |

Прочее:

- `config.py` — чтение `.env`, токен и whitelist.
- `plugins/` — подключаемые интеграции (QueueWarden, ActiveCollab, напоминания, IMAP,
  webhook, топик бота поддержки). Включаются списком `JARVIS_PLUGINS`; каждая
  объявляет в `plugin.py` объект `PLUGIN` (`bot/plugins.py`): фоновые задачи,
  команды, источники триггеров, свои таблицы. MCP-тулы — в `mcp_tools.py`.
- `engines/` — адаптеры CLI: контракт `Engine` и общая база в `base.py`, общие куски
  (журнал хода, чтение JSONL, учёт процессов) в `common.py`; Playwright MCP,
  topic-MCP, кэш моделей (см. выше).
- `scripts/jarvis_mcp_server.py` — точка входа Jarvis Manager MCP (путь прописан в конфигах движков).
- `mcp_server/` — сам MCP-сервер: `common.py` (БД, Telegram API, объект FastMCP),
  `tools/` (`ask_user`, `manager_*`), `server.py` (`main`, загрузка тулов плагинов
  из `JARVIS_PLUGINS` через `plugins/<имя>/mcp_tools.py`).
- `bot_state.db` — sqlite: сессии, `jobs`, `agent_triggers`, `messages_log`,
  таблицы включённых плагинов, метаданные исходящих сообщений (для reply-to).
- `temp/media/` — скачанные пользовательские вложения.
- `systemd/jarvis-bot.service` — user-unit.

## Тесты

```bash
JARVIS_DOTENV=0 ./venv/bin/python -m unittest discover -s tests -t .
./venv/bin/pip install -r requirements-dev.txt && ./venv/bin/ruff check .
```

Внешних сервисов и токенов не требуют: конфиги подкладываются во временные
каталоги, Telegram API и запуски CLI мокаются.

На каждый push и pull request то же самое прогоняет GitHub Actions
(`.github/workflows/tests.yml`) на Python 3.11 и 3.12: юнит-тесты, `ruff`, `pyflakes`
без поблажек (неиспользованный импорт тоже красит сборку) и смоук свежей
установки — бот обязан собираться с `.env` из двух строк, без Менеджера и без
внешних MCP-серверов.

Два теста стоят особняком и страхуют структурные правки:

- `test_wiring_snapshot.py` — снапшот проводки: каждая команда и каждый
  callback-паттерн ведут в функцию с тем же именем, `unauthorized_handler`
  остаётся последним. Ловит хендлер, потерянный при переносе кода.
- `test_no_undefined_names.py` — статический анализ неопределённых имён
  (pyflakes из `requirements-dev.txt`; без него тест пропускается). Нужен
  потому, что `NameError` внутри тела функции не проявляется ни при импорте
  модуля, ни в остальных тестах — только при вызове, то есть в Telegram.

## Известные ограничения

- Session-id у `claude` генерируется ботом и передаётся через `--session-id`; если удалить каталог
  `~/.claude/projects/...` или история будет повреждена, сессия «забудет» контекст.
- У `cursor` id, как у `claude`, назначает бот (uuid4): `--resume` на несуществующий
  чат создаёт чат с этим id; наличие сессии проверяется по
  `<CURSOR_CONFIG_DIR | $XDG_CONFIG_HOME/cursor | ~/.cursor>/chats/<md5(cwd)>/<id>/store.db`.
- У `codex` и `opencode` настоящий id создаёт сам CLI; до первого ответа в БД лежит
  временный placeholder, затем бот заменяет его на реальный id.
- `claude` запускается с `--permission-mode bypassPermissions`, чтобы не зависать
  на подтверждениях tool-use. Это значит, что агент может делать в `CLAUDE_CWD`
  всё, что умеет. Ограничивай каталог по необходимости.
- Голосовые не распознаются — нужно печатать или диктовать с клавиатуры телефона.
- Telegram-лимит на документ — 50 МБ; на текст — 4096 символов.

## Лицензия

[MIT](LICENSE) © 2026 ShevArtV
