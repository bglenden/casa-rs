#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

: "${CASA_RS_IMPERF_DATA_ROOT:?set CASA_RS_IMPERF_DATA_ROOT}"
: "${CASA_RS_CASA_PYTHON:?set CASA_RS_CASA_PYTHON}"
: "${CASA_RS_ISSUE607_STANDARD_OUTPUT:?set CASA_RS_ISSUE607_STANDARD_OUTPUT}"

runner="tools/perf/imager/run_workload.py"
workload="tools/perf/imager/workloads/issue607-standard-mfs-representative.json"
dirty_workload="tools/perf/imager/workloads/issue607-standard-mfs-dirty-representative.json"
common=(--repeats 1 --output-dir "$CASA_RS_ISSUE607_STANDARD_OUTPUT")

"$runner" "${common[@]}" --run-label issue607-natural-dirty "$dirty_workload"
"$runner" "${common[@]}" --run-label issue607-briggs-hogbom "$workload"
"$runner" "${common[@]}" --run-label issue607-briggs-clark \
  --set-imaging deconvolver=clark "$workload"
"$runner" "${common[@]}" --run-label issue607-briggs-multiscale \
  --set-imaging deconvolver=multiscale --set-imaging scales=0,5,15 "$workload"
"$runner" "${common[@]}" --run-label issue607-box-mask \
  --set-imaging mask_box=208,208,303,303 "$workload"
"$runner" "${common[@]}" --run-label issue607-automultithresh \
  --set-imaging usemask=auto-multithresh --set-imaging mask_box=null "$workload"
"$runner" "${common[@]}" --run-label issue607-modelcolumn \
  --set-imaging savemodel=modelcolumn --set-imaging parallel=true "$workload"
