#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# The MSv2/MSv4 trial's bidirectional XRADIO check
# (docs/measurement-set/trial/README.md), on the committed fixtures:
#
#   1. XRADIO writes, Rust reads: the trial reader dumps what it delivers from
#      XRADIO's store, and XRADIO's own load of that store must equal it.
#   2. Rust writes, XRADIO reads: the trial writer converts the MSv2 through
#      batches into a fresh store, which XRADIO must open, schema-check and
#      fully load, equal to XRADIO's conversion and to the MSv2 itself.
#
# Writes both JSON reports to OUT (default docs/measurement-set/trial/interop)
# and exits non-zero if either check fails or cannot run. Needs cargo and uv.

set -euo pipefail

repo="$(git rev-parse --show-toplevel)"
cd "$repo"
out="${1:-docs/measurement-set/trial/interop}"
mkdir -p "$out"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
fixtures="crates/casa-ms/tests/fixtures/ms_trial"
tar -xzf "$fixtures/ms_trial.ms.tgz" -C "$work"
tar -xzf "$fixtures/ms_trial.ps.zarr.tgz" -C "$work"

run() { cargo run -q -p casa-ms --features msv4 --example ms_trial_interop -- "$@"; }
run dump "$work/ms_trial.ps.zarr" "$work/dump" >/dev/null
run write "$work/ms_trial.ms" "$work/rust.ps.zarr" >"$out/rust_write_counters.json"

oracle() { uv run -q --project tools/ms-trial python tools/ms-trial/check_interop.py "$@"; }
status=0
oracle rust-read --store "$work/ms_trial.ps.zarr" --dump "$work/dump" \
  >"$out/xradio_writes_rust_reads.json" 2>"$work/read.log" || status=1
oracle rust-written --store "$work/rust.ps.zarr" --reference "$work/ms_trial.ps.zarr" \
  --ms "$work/ms_trial.ms" >"$out/rust_writes_xradio_reads.json" 2>"$work/written.log" || status=1
for report in "$out"/xradio_writes_rust_reads.json "$out"/rust_writes_xradio_reads.json; do
  python3 -c 'import json, sys; r = json.load(open(sys.argv[1])); print("%s: %d checks, %d failed" % (sys.argv[1], r["checks"], r["failed"]))' "$report"
done
exit "$status"
