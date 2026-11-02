#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Regenerate XRADIO's conversion of CASA's ALMA test MeasurementSet
# (crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz) in
# crates/casa-ms/tests/fixtures/ms_trial/mssel_test_small.ps.zarr.tgz
# (#1014): its spectral window 1 descends in frequency, which XRADIO writes
# ascending. Default partitioning, no pointing. The other fixtures are
# untouched. Needs uv; the XRADIO oracle environment is tools/ms-trial/uv.lock.

set -euo pipefail

repo="$(git rev-parse --show-toplevel)"
cd "$repo"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
destination="crates/casa-ms/tests/fixtures/ms_trial"

tar -xzf crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz -C "$work"
uv run -q --project tools/ms-trial python tools/ms-trial/check_convert.py xradio-convert \
  --ms "$work/mssel_test_small.ms" --out "$work/mssel_test_small.ps.zarr"

tar --sort=name --mtime=@0 --owner=0 --group=0 --numeric-owner \
  -C "$work" -cf - mssel_test_small.ps.zarr | gzip -n -9 >"$destination/mssel_test_small.ps.zarr.tgz"
ls -l "$destination/mssel_test_small.ps.zarr.tgz"
