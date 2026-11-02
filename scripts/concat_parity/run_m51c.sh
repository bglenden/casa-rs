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
#   MS_CREATE_FROM  the casa-ms ms_create_from example, built --release
#   CASA_PYTHON     CASA's Python (casatasks and casatools importable)
set -euo pipefail

work=$1
here=$(cd "$(dirname "$0")" && pwd)
: "${MS_CREATE_FROM:?the ms_create_from example}"
: "${CASA_PYTHON:?the Python of a CASA install}"
accept=sigma,row-flags,flag-categories,data-groups,tables,columns,defaulted
twelve=$work/m51c.alma_0.5arcsec.ms
seven=$work/m51c.aca.i.ms

"$CASA_PYTHON" "$here/m51c_casa.py" refs "$work" > "$work/refs.log" 2>&1
rm -rf "$work/ours.ms" "$work/ours12.ms" "$work/b-ours.ms"
"$MS_CREATE_FROM" "$work/ours.ms" "$twelve" "$seven=0.34" --sort --accept "$accept" \
  > "$work/ours.json"
"$MS_CREATE_FROM" "$work/ours12.ms" "$twelve" --accept "$accept" > "$work/ours12.json"
cp -R "$work/b.ms" "$work/b-ours.ms"
"$MS_CREATE_FROM" "$work/b-ours.ms" "$seven=0.34" --replace --accept "$accept" \
  > "$work/b-ours.json"
for name in ours b-ours; do
  "$CASA_PYTHON" "$here/compare_concat.py" "$work/$name.ms" "$work/ref.ms" \
    > "$work/$name.compare.json" 2>&1 || true
done
# CASA appending to casa-rs's 12 m, last: a failure here hides nothing.
if "$CASA_PYTHON" "$here/m51c_casa.py" append "$work" > "$work/append.log" 2>&1; then
  "$CASA_PYTHON" "$here/compare_concat.py" "$work/a.ms" "$work/ref.ms" \
    > "$work/a.compare.json" 2>&1 || true
else
  echo "CASA's append failed: $(grep -m1 SEVERE "$work/append.log" || true)"
fi
echo "m51c done"
