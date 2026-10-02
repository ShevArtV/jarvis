# Jarvis на Windows

Бот работает на Windows 10/11 нативно, без WSL. Код один и тот же; отличаются
установка, запуск службы и пара мелочей ниже.

## Что нужно

- Python 3.11+ с [python.org](https://www.python.org/downloads/windows/) (галочка
  «Add python.exe to PATH»).
- Node.js 20+ — для `codex`, `opencode` и Playwright MCP.
- Хотя бы один CLI движка, авторизованный под тем же пользователем Windows,
  от которого будет работать бот:
  - claude — нативный установщик (`claude.exe`) или `npm i -g @anthropic-ai/claude-code`;
  - codex — `npm i -g @openai/codex`;
  - opencode — `npm i -g opencode-ai`.

npm ставит CLI как `.cmd`-обёртки. Jarvis сам находит за обёрткой скрипт и
запускает `node` напрямую, минуя `cmd.exe`, — промпт с переводами строк, `%` и
кавычками доходит до движка без искажений. Указывать полный путь в
`CLAUDE_BIN`/`CODEX_BIN`/`OPENCODE_BIN` не нужно, если CLI есть в `PATH`.

## Установка (PowerShell)

```powershell
git clone https://github.com/ShevArtV/jarvis.git
cd jarvis
py -3 -m venv venv
.\venv\Scripts\pip install -r requirements.txt
Copy-Item .env.example .env
notepad .env   # TELEGRAM_TOKEN и ALLOWED_USER_IDS
```

Проверить, что движок отвечает из той же консоли:

```powershell
claude -p "hello"
```

`CLAUDE_CWD` и пути в `/bind` — обычные Windows-пути: `C:\Users\me\projects\site`.

## Запуск

```powershell
$env:PYTHONUTF8 = "1"
.\venv\Scripts\python telegram_bot.py
```

`PYTHONUTF8=1` переводит Python в UTF-8 целиком: без него файлы без явной
кодировки читаются в cp1251.

## Автозапуск

Боту нужен пользовательский профиль: CLI движков хранят авторизацию в
`%USERPROFILE%`. Поэтому служба запускается от вашей учётной записи, а не от
LocalSystem.

**Планировщик задач** — без сторонних программ:

```powershell
$action  = New-ScheduledTaskAction -Execute "$PWD\venv\Scripts\python.exe" `
             -Argument "telegram_bot.py" -WorkingDirectory "$PWD"
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet -RestartCount 999 `
             -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit 0
[Environment]::SetEnvironmentVariable("PYTHONUTF8", "1", "User")
Register-ScheduledTask -TaskName "Jarvis" -Action $action -Trigger $trigger -Settings $settings
```

**[NSSM](https://nssm.cc/)** — если нужна настоящая служба с логом в файл:

```powershell
nssm install Jarvis "$PWD\venv\Scripts\python.exe" telegram_bot.py
nssm set Jarvis AppDirectory "$PWD"
nssm set Jarvis AppEnvironmentExtra PYTHONUTF8=1
nssm set Jarvis ObjectName ".\$env:USERNAME" "<пароль>"
nssm set Jarvis AppStdout "$PWD\bot.log"
nssm set Jarvis AppStderr "$PWD\bot.log"
nssm start Jarvis
```

В `PATH` службы должны быть каталоги CLI (`%APPDATA%\npm`, каталог `claude.exe`)
и Node.js. Проще всего — запустить службу от своей учётной записи, тогда `PATH`
пользователя подхватится сам.

## Отличия от Linux

- `/stop`, таймауты и закрытие сеанса гасят дерево процессов через
  `taskkill /T /F`: мягкой остановки (SIGTERM) у Windows нет.
- Права `0600` на временные профили MCP с токенами не ставятся — файлы защищены
  ACL вашего профиля.
- Playwright MCP ищет `npx` в `PATH`; если Node.js стоит через nvm-windows и не в
  `PATH`, задайте `PLAYWRIGHT_MCP_NPX` полным путём к `npx.cmd`.
- `systemd/` и рецепты с `journalctl` из README к Windows не относятся.
