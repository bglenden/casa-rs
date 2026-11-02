#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# The trial's ephemeris check (docs/measurement-set/trial/README.md,
# "Ephemerides in MSv4"), on the committed fixtures: the converter writes the
# moving-field MSv2 as MSv4, and XRADIO must open it, schema-check it and find
# its field_and_source_ephemeris equal to XRADIO's own conversion. Writes the
# JSON report to OUT (default docs/measurement-set/trial/interop) and exits
# non-zero if the check fails or cannot run. Needs cargo and uv.

set -euo pipefail

repo="$(git rev-parse --show-toplevel)"
cd "$repo"
out="${1:-docs/measurement-set/trial/interop}"
mkdir -p "$out"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
fixtures="crates/casa-ms/tests/fixtures/ms_trial"
tar -xzf "$fixtures/ms_trial_ephemeris.ms.tgz" -C "$work"
tar -xzf "$fixtures/ms_trial_ephemeris.ps.zarr.tgz" -C "$work"

cargo run -q -p casa-ms --features msv4 --example ms_trial_convert -- \
  to-msv4 "$work/ms_trial_ephemeris.ms" "$work/rust.ps.zarr" --accept sigma,tables,columns \
  >"$out/ephemeris_conversion.json"
uv run -q --project tools/ms-trial python tools/ms-trial/check_ephemeris.py \
  --ours "$work/rust.ps.zarr" --theirs "$work/ms_trial_ephemeris.ps.zarr" \
  >"$out/ephemeris_rust_writes_xradio_reads.json"
python3 -c 'import json, sys; r = json.load(open(sys.argv[1])); print("%s: %d checks, %d failed" % (sys.argv[1], r["checks"], r["failed"]))' \
  "$out/ephemeris_rust_writes_xradio_reads.json"
