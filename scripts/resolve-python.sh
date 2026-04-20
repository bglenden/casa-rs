#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
set -euo pipefail

min_version="${1:-3.10}"

die() {
  echo "resolve-python.sh: $*" >&2
  exit 1
}

resolve_candidate() {
  local candidate="$1"

  if [[ -z "$candidate" ]]; then
    return 1
  fi

  if [[ "$candidate" == */* ]]; then
    [[ -x "$candidate" ]] || return 1
    printf '%s\n' "$candidate"
    return 0
  fi

  command -v "$candidate" 2>/dev/null || return 1
}

python_meets_minimum() {
  local python_bin="$1"
  local required="$2"

  "$python_bin" - "$required" <<'PY'
import os
import sys

required = tuple(int(part) for part in sys.argv[1].split("."))
current = sys.version_info[: len(required)]
if current < required:
    raise SystemExit(1)

print(os.path.realpath(sys.executable))
PY
}

declare -a candidates=()

add_candidate() {
  local candidate="$1"
  local existing

  [[ -n "$candidate" ]] || return 0

  for existing in "${candidates[@]:-}"; do
    if [[ "$existing" == "$candidate" ]]; then
      return 0
    fi
  done

  candidates+=( "$candidate" )
}

if [[ -n "${PYTHON_BIN:-}" ]]; then
  resolved="$(resolve_candidate "$PYTHON_BIN")" || die "PYTHON_BIN does not resolve to an executable: $PYTHON_BIN"
  selected="$(python_meets_minimum "$resolved" "$min_version")" || die "PYTHON_BIN=$resolved is older than the required Python $min_version"
  printf '%s\n' "$selected"
  exit 0
fi

add_candidate "$HOME/.pyenv/shims/python3"
add_candidate "$HOME/.pyenv/shims/python"

shopt -s nullglob
for candidate in "$HOME"/.pyenv/versions/*/bin/python3 "$HOME"/.pyenv/versions/*/bin/python; do
  add_candidate "$candidate"
done
shopt -u nullglob

add_candidate "/opt/homebrew/bin/python3"
add_candidate "/usr/local/bin/python3"
add_candidate "python3.13"
add_candidate "python3.12"
add_candidate "python3.11"
add_candidate "python3.10"
add_candidate "python3"
add_candidate "python"

for candidate in "${candidates[@]}"; do
  resolved="$(resolve_candidate "$candidate")" || continue
  if selected="$(python_meets_minimum "$resolved" "$min_version" 2>/dev/null)"; then
    printf '%s\n' "$selected"
    exit 0
  fi
done

die "failed to locate Python $min_version or newer; set PYTHON_BIN to a suitable interpreter"
