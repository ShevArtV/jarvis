#!/usr/bin/env bash
# Ensure jarvis venv matches the current system Python and can import runtime deps.
# Wired as ExecStartPre for jarvis-bot.service so apt python upgrades self-heal.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$ROOT/venv"
REQ="$ROOT/requirements.txt"

if ! command -v python3 >/dev/null 2>&1; then
  echo "ensure_venv: no python3 interpreter on PATH" >&2
  exit 1
fi

# Prefer versioned binary (python3.14) over floating python3 symlink, so pyvenv.cfg
# records a concrete executable that survives `python3` retargeting within the
# same minor — and so recreate uses the OS default major.minor after upgrades.
mm="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
if command -v "python${mm}" >/dev/null 2>&1; then
  PY="python${mm}"
else
  PY=python3
fi

need_recreate=0
reason=

if [[ ! -x "$VENV/bin/python" ]]; then
  need_recreate=1
  reason="missing venv python"
elif ! "$VENV/bin/python" -c 'import telegram, dotenv, httpx, mcp, starlette, uvicorn' 2>/dev/null; then
  need_recreate=1
  reason="runtime imports failed"
else
  cfg_ver="$(awk -F' *= *' '/^version/{print $2; exit}' "$VENV/pyvenv.cfg" 2>/dev/null || true)"
  run_ver="$("$VENV/bin/python" -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])')"
  cfg_mm="${cfg_ver%.*}"
  run_mm="${run_ver%.*}"
  # major.minor drift (3.12 → 3.14) means site-packages path no longer matches
  if [[ -n "$cfg_mm" && "$cfg_mm" != "$run_mm" ]]; then
    need_recreate=1
    reason="pyvenv.cfg $cfg_ver vs running $run_ver"
  fi
  # Also recreate if the base executable recorded in cfg is gone (apt removed it)
  cfg_exe="$(awk -F' *= *' '/^executable/{print $2; exit}' "$VENV/pyvenv.cfg" 2>/dev/null || true)"
  if [[ -n "$cfg_exe" && ! -x "$cfg_exe" ]]; then
    need_recreate=1
    reason="base executable missing: $cfg_exe"
  fi
fi

if [[ "$need_recreate" -eq 0 ]]; then
  exit 0
fi

echo "ensure_venv: recreating ($reason) with $PY" >&2
ts="$(date +%Y%m%d-%H%M%S)"
if [[ -d "$VENV" ]]; then
  mv "$VENV" "$VENV.bak-$ts"
fi
"$PY" -m venv "$VENV"
"$VENV/bin/python" -m pip install -U pip setuptools wheel
"$VENV/bin/pip" install -r "$REQ"
"$VENV/bin/python" -c 'import telegram, dotenv, httpx, mcp, starlette, uvicorn'
echo "ensure_venv: ready ($("$VENV/bin/python" -V))" >&2
