#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 1 ]]; then
    echo "usage: $0 [CASA_RS_LABEL]" >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
receipt_date="${CASA_RS_VLASS_RECEIPT_DATE:-$(date -u +%Y%m%d)}"
casa_python="${CASA_RS_VLASS_CASA_PYTHON:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python}"
label="${1:-vlass-production-clean-4096-full-16-spw-fftw-t1-gridt2-niter2000-image-response-cache-promoted-stack-dyadic-tiles-radix-madfm-accel-metal-v1}"
workload="$repo_root/tools/perf/imager/workloads/vlass-fragment-single-field-clean-4096-full-16-spw.json"
rust_prefix="$root/artifacts/products/$label/rust"
casa_prefix="$root/casa-reduced-clean/4096-full-16-spw/casa"
rust_log="$root/receipts/runs/${receipt_date}-$label.log"
casa_log="$root/receipts/runs/20260729-vlass-reduced-casa-clean-4096-full-16-spw.casa.log"
comparison_prefix="$root/receipts/runs/${receipt_date}-$label.comparison"
trace_output="$root/receipts/runs/${receipt_date}-$label.trace-comparison.json"

for required in \
    "$casa_python" \
    "$workload" \
    "$rust_log" \
    "$rust_prefix.image.tt0" \
    "$casa_log" \
    "$casa_prefix.image.tt0"; do
    if [[ ! -e "$required" ]]; then
        echo "required full-16-SPW comparison input does not exist: $required" >&2
        exit 2
    fi
done

comparison_status=0
trace_status=0
python3 "$repo_root/tools/perf/imager/vlass_compare_frozen_products.py" \
    "$workload" \
    "$rust_prefix" \
    "$casa_prefix" \
    "$comparison_prefix" \
    --casa-python "$casa_python" \
    || comparison_status=$?
python3 "$repo_root/tools/perf/imager/experiments/vlass_clean_major_cycle_trace_compare.py" \
    --casa-log "$casa_log" \
    --rust-log "$rust_log" \
    --output "$trace_output" \
    || trace_status=$?

shasum -a 256 \
    "$rust_log" \
    "$comparison_prefix.json" \
    "$trace_output"
if [[ "$comparison_status" -ne 0 || "$trace_status" -ne 0 ]]; then
    echo "post-run validation failed: comparison=$comparison_status trace=$trace_status" >&2
    exit 1
fi
