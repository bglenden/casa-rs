#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Regenerate the trial's ephemeris fixtures in crates/casa-ms/tests/fixtures/ms_trial/
# (docs/measurement-set/trial/README.md): the trial MSv2 with its field
# following a Moon-like body (ms_trial_ephemeris.ms), and XRADIO's
# conversions of it in its two layouts, the samples on their own times
# (ms_trial_ephemeris.ps.zarr) and, XRADIO's default, interpolated onto the
# visibility times (ms_trial_ephemeris_interpolated.ps.zarr). It starts from
# the committed ms_trial.ms, so the other fixtures are untouched. Needs cargo
# and uv; the XRADIO oracle environment is tools/ms-trial/uv.lock.

set -euo pipefail

repo="$(git rev-parse --show-toplevel)"
cd "$repo"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
destination="crates/casa-ms/tests/fixtures/ms_trial"

tar -xzf "$destination/ms_trial.ms.tgz" -C "$work"
mv "$work/ms_trial.ms" "$work/ms_trial_ephemeris.ms"
cargo run -q -p casa-ms --example ms_trial_ephemeris -- "$work/ms_trial_ephemeris.ms"
uv run -q --project tools/ms-trial python tools/ms-trial/make_ephemeris_fixtures.py \
  "$work/ms_trial_ephemeris.ms" "$work"

pack() {
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$work" -cf - "$1" | gzip -n -9 >"$destination/$1.tgz"
}
pack ms_trial_ephemeris.ms
pack ms_trial_ephemeris.ps.zarr
pack ms_trial_ephemeris_interpolated.ps.zarr
ls -l "$destination"
