#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
library="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib/libcasacpp_synthesis.6.dylib"
output="${CASA_RS_VLASS_DIVSC3_DIRECT_PROBE_OUTPUT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446/receipts/diagnostics/20260731-vlass-casa-aw-divsc3-direct-probe-v1.json}"

python3 \
  "${repo_root}/tools/perf/imager/experiments/vlass_casa_aw_divsc3_direct_probe.py" \
  --library "${library}" \
  --output "${output}"
shasum -a 256 "${output}"
