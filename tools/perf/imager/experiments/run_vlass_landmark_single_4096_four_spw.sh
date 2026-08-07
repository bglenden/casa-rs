#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
binary="${CASA_RS_VLASS_EXPERIMENT_BINARY:-$repo_root/target/release/casars-imager}"
fftw_library_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:?CASA_RS_VLASS_FFTW_LIBRARY_DIR is required}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:?CASA_RS_VLASS_MEASURES_DIR is required}"
receipt_date="${CASA_RS_VLASS_RECEIPT_DATE:-$(date -u +%Y%m%d)}"
source_revision="$(git -C "$repo_root" rev-parse HEAD)"
label="${CASA_RS_VLASS_LABEL_OVERRIDE:-vlass-landmark-single-4096-4spw-clean-n2000-${source_revision:0:12}}"
log="$root/receipts/runs/${receipt_date}-${label}.log"
receipt="$root/receipts/runs/${receipt_date}-${label}.landmark.json"

if [[ -n "$(git -C "$repo_root" status --porcelain --untracked-files=normal)" ]]; then
    echo "landmark source tree must be clean so the release executable is commit-bound" >&2
    exit 2
fi
if [[ "$binary" != "$repo_root/target/release/casars-imager" ]]; then
    echo "landmark executable must be the canonical release target for the clean source tree" >&2
    exit 2
fi
/usr/bin/env -i \
    HOME="$HOME" \
    PATH="$PATH" \
    TMPDIR="${TMPDIR:-/tmp}" \
    CARGO_HOME="${CARGO_HOME:-$HOME/.cargo}" \
    RUSTUP_HOME="${RUSTUP_HOME:-$HOME/.rustup}" \
    CARGO_INCREMENTAL=0 \
    CARGO_TARGET_DIR="$repo_root/target" \
    cargo build --locked --manifest-path "$repo_root/Cargo.toml" -p casars-imager --release
if [[ "$(git -C "$repo_root" rev-parse HEAD)" != "$source_revision" \
    || -n "$(git -C "$repo_root" status --porcelain --untracked-files=normal)" ]]; then
    echo "landmark source revision or clean-tree state changed during the release build" >&2
    exit 2
fi
if [[ ! -x "$binary" ]]; then
    echo "release executable is missing or not executable: $binary" >&2
    exit 2
fi
if [[ "$(basename "$(dirname "$binary")")" != "release" ]]; then
    echo "landmark executable must come from a release directory: $binary" >&2
    exit 2
fi
if [[ -e "$log" || -e "$receipt" ]]; then
    echo "refusing to overwrite an existing landmark log or receipt" >&2
    exit 2
fi
/usr/bin/env -i \
    HOME="$HOME" \
    PATH="$PATH" \
    TMPDIR="${TMPDIR:-/tmp}" \
    CASA_RS_VLASS_EXPERIMENT_ROOT="$root" \
    CASA_RS_VLASS_EXPERIMENT_BINARY="$binary" \
    CASA_RS_VLASS_FFTW_LIBRARY_DIR="$fftw_library_dir" \
    CASA_RS_VLASS_MEASURES_DIR="$measures_dir" \
    CASA_RS_VLASS_RECEIPT_DATE="$receipt_date" \
    CASA_RS_VLASS_LABEL_OVERRIDE="$label" \
    CASA_RS_VLASS_NITER=2000 \
    CASA_RS_VLASS_FFTW_THREADS=8 \
    CASA_RS_VLASS_GRID_THREADS=2 \
    CASA_RS_VLASS_MODEL_FFT_THREADS=8 \
    CASA_RS_VLASS_STANDARD_MFS_ACCELERATION=metal \
    CASA_RS_VLASS_IMAGE_RESPONSE_CACHE=1 \
    CASA_RS_VLASS_MODEL_DELTA_CENSUS=1 \
    CASA_RS_VLASS_RADIX_MADFM=1 \
    CASA_RS_VLASS_CACHE_REFRESHED_NSIGMA=1 \
    CASA_RS_VLASS_SPARSE_MASK_PEAK_SEARCH=1 \
    CASA_RS_VLASS_PARALLEL_MODEL_TERM_FFT=1 \
    CASA_RS_VLASS_SPARSE_MODEL_PREP=1 \
    bash "$repo_root/tools/perf/imager/experiments/run_vlass_clean_4096_four_spw_sparse_fftw.sh"

wall_seconds="$(
    awk '$1 == "real" && NF == 2 { value = $2 } END { print value }' "$log"
)"
if [[ -z "$wall_seconds" ]]; then
    echo "landmark log is missing /usr/bin/time wall output: $log" >&2
    exit 1
fi

PYTHONPATH="$repo_root/tools/perf/imager" \
    python3 "$repo_root/tools/perf/imager/vlass_landmark_guard.py" \
    --landmark-id VLASS-LANDMARK-SINGLE-4096-4SPW-CLEAN-N2000-v1 \
    --log "$log" \
    --binary "$binary" \
    --source-revision "$source_revision" \
    --wall-seconds "$wall_seconds" \
    --output "$receipt"

shasum -a 256 "$binary" "$log" "$receipt"
