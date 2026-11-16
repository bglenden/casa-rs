#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
# The 12 m + 7 m evidence of #937 on simalma's m51c pair, in three
# directions: casa-rs's creation against CASA's concat (ours), casa-rs
# appending the 7 m to CASA's one-input concat (b-ours), and CASA appending
# the 7 m to casa-rs's 12 m (a). Each is compared with CASA's concat of the
# pair (ref.ms).
#
# Usage: run_m51c.sh WORK
#   WORK  holds m51c.alma_0.5arcsec.ms and m51c.aca.i.ms (simalma's m51c
#         outputs). CASA's concat refuses paths with spaces, so WORK must
#         have none.
# Environment:
#   CONCAT_BIN      the native concat task, built --release
#   CASA_PYTHON     CASA's Python (casatasks and casatools importable)
set -euo pipefail

work=$1
here=$(cd "$(dirname "$0")" && pwd)
: "${CONCAT_BIN:?the native concat task}"
: "${CASA_PYTHON:?the Python of a CASA install}"
twelve=$work/m51c.alma_0.5arcsec.ms
seven=$work/m51c.aca.i.ms

"$CASA_PYTHON" "$here/m51c_casa.py" refs "$work" > "$work/refs.log" 2>&1
rm -rf "$work/ours.ms" "$work/ours12.ms" "$work/b-ours.ms"
"$CONCAT_BIN" --concatvis "$work/ours.ms" --vis "$twelve,$seven" --visweightscale '[1.0,0.34]' \
  > "$work/ours.json"
"$CONCAT_BIN" --concatvis "$work/ours12.ms" --vis "$twelve" > "$work/ours12.json"
cp -R "$work/b.ms" "$work/b-ours.ms"
"$CONCAT_BIN" --concatvis "$work/b-ours.ms" --vis "$seven" --visweightscale 0.34 \
  > "$work/b-ours.json"
for name in ours b-ours; do
  "$CASA_PYTHON" "$here/compare_concat.py" "$work/$name.ms" "$work/ref.ms" \
    > "$work/$name.compare.json" 2> "$work/$name.compare.log"
done
# CASA appending to casa-rs's 12 m, last: a failure here hides nothing.
if "$CASA_PYTHON" "$here/m51c_casa.py" append "$work" > "$work/append.log" 2>&1; then
  "$CASA_PYTHON" "$here/compare_concat.py" "$work/a.ms" "$work/ref.ms" \
    > "$work/a.compare.json" 2> "$work/a.compare.log"
else
  echo "CASA's append failed; see $work/append.log" >&2
  exit 1
fi
echo "m51c done"
