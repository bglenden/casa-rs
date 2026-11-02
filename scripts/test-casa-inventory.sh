#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Compare the committed CASA task inventory with an installed CASA 6.7.6.14
# (#931 4a). CASA compatibility acceptance requires this comparison, so a
# missing CASA fails it: it is never skipped. CASA's Python is
# $CASA_RS_CASA_PYTHON, or else /Applications/CASA.app's.
#
#   scripts/test-casa-inventory.sh           # compare
#   scripts/test-casa-inventory.sh --write   # re-record the fixtures

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

casa_python="${CASA_RS_CASA_PYTHON:-/Applications/CASA.app/Contents/MacOS/python3}"
if [[ ! -x "$casa_python" ]]; then
  echo "test-casa-inventory.sh: no CASA Python at $casa_python; compatibility acceptance needs CASA 6.7.6.14 (set CASA_RS_CASA_PYTHON)" >&2
  exit 1
fi

inventory="resources/casa-compat/casa-6.7.6.14"
case "${1:-}" in
  "")
    "$casa_python" -I tools/casa-compat/dump_casa_task_inventory.py --check "$inventory"
    ;;
  --write)
    "$casa_python" -I tools/casa-compat/dump_casa_task_inventory.py --out "$inventory"
    python3 scripts/check-casa-inventory.py --write
    ;;
  *)
    echo "usage: $0 [--write]" >&2
    exit 2
    ;;
esac
