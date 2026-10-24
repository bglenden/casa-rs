#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Regenerate the MSv2/MSv4 trial fixtures in crates/casa-ms/tests/fixtures/ms_trial/
# (docs/measurement-set/trial/README.md). Needs cargo and uv; the XRADIO oracle
# environment is tools/ms-trial/uv.lock. The payload is the same on every run;
# XRADIO's creation dates in the store's metadata are not.

set -euo pipefail

repo="$(git rev-parse --show-toplevel)"
cd "$repo"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
destination="crates/casa-ms/tests/fixtures/ms_trial"

cargo run -q -p casa-ms --example ms_trial_fixture -- "$work/base.ms" >/dev/null
uv run -q --project tools/ms-trial python tools/ms-trial/make_fixtures.py "$work/base.ms" "$work/out"

mkdir -p "$destination"
pack() {
  tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
    -C "$work/out" -cf - "$1" | gzip -n -9 >"$destination/$1.tgz"
}
pack ms_trial.ms
pack ms_trial.ps.zarr
cp "$work/out/manifest.json" "$destination/manifest.json"
ls -l "$destination"
