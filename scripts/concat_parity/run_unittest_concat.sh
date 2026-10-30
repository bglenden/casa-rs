#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
# casa-rs's creation of CASA's unittest/concat cases (#937), compared with
# CASA 6.7.6's concat of the same inputs (casa_concat_refs.py).
#
# Usage: run_unittest_concat.sh INPUTS CASA_REFS OUTPUT
#   INPUTS     a copy of casatestdata's unittest/concat inputs
#   CASA_REFS  casa_concat_refs.py's OUTPUT (CASA_REFS/<case>/<case>.ms)
#   OUTPUT     where casa-rs's outputs and <case>.compare.json go
# Environment:
#   MS_CREATE_FROM  the casa-ms ms_create_from example, built --release
#   CASA_PYTHON     CASA's Python (casatools importable)
set -euo pipefail

inputs=$1
refs=$2
output=$3
here=$(cd "$(dirname "$0")" && pwd)
: "${MS_CREATE_FROM:?the ms_create_from example}"
: "${CASA_PYTHON:?the Python of a CASA install}"
accept=sigma,row-flags,flag-categories,data-groups,tables,columns,defaulted

rm -rf "$output"
mkdir -p "$output"
run() {
  local name=$1
  shift
  echo "== $name"
  "$MS_CREATE_FROM" "$output/$name.ms" "$@" --sort --accept "$accept" \
    > "$output/$name.json" 2> "$output/$name.err" ||
    echo "failed: $(tail -3 "$output/$name.err")"
}
run test1 "$inputs/part1.ms" "$inputs/part2.ms" "$inputs/part3.ms" "$inputs/part4.ms"
run test2 "$inputs/part1.ms=3.0" "$inputs/part2-mod.ms=2.0" "$inputs/part3.ms=1.0"
run test3 "$inputs/part1.ms" "$inputs/part2-mod2.ms" "$inputs/part3.ms"
run test8 "$inputs/sim7.ms" "$inputs/sim8.ms"
run test16 "$inputs/X39a.pm03.scan3.ms" "$inputs/X425.pm03.scan4.ms" "$inputs/X425.pm04.scan4.ms"
run test17 "$inputs/uid___A002_Xab8dc1_X95a-shrunk.ms" "$inputs/uid___A002_Xab8dc1_Xf13-shrunk.ms"
for name in test1 test2 test3 test8 test16 test17; do
  if [ -d "$output/$name.ms" ] && [ -d "$refs/$name/$name.ms" ]; then
    "$CASA_PYTHON" "$here/compare_concat.py" "$output/$name.ms" "$refs/$name/$name.ms" \
      > "$output/$name.compare.json" 2>&1 || true
  fi
done
echo "unittest/concat done"
