#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Regenerate the trial's sources-and-lines fixtures in
# crates/casa-ms/tests/fixtures/ms_trial/ (#976): a simulated MSv2 of one
# field in two spectral windows whose SOURCE states its source's lines in
# each (ms_trial_lines.ms), and XRADIO's conversion of it
# (ms_trial_lines.ps.zarr). The other fixtures are untouched. Needs cargo
# and uv; the XRADIO oracle environment is tools/ms-trial/uv.lock.

set -euo pipefail

repo="$(git rev-parse --show-toplevel)"
cd "$repo"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
destination="crates/casa-ms/tests/fixtures/ms_trial"

cargo run -q -p casa-ms --example ms_trial_lines -- "$work/ms_trial_lines.ms"
uv run -q --project tools/ms-trial python tools/ms-trial/make_lines_fixtures.py \
  "$work/ms_trial_lines.ms" "$work"

pack() {
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$work" -cf - "$1" | gzip -n -9 >"$destination/$1.tgz"
}
pack ms_trial_lines.ms
pack ms_trial_lines.ps.zarr
ls -l "$destination"
