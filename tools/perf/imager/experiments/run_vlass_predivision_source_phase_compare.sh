#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
diagnostics="${external_root}/receipts/diagnostics"
term_comparison="${diagnostics}/20260731-vlass-4096-4spw-wide-division-term-comparison-v3.json"
phase_receipt="${diagnostics}/20260731-vlass-4096-4spw-source-phase-placement-replay-v2.json"
wide_sidecar_host="${diagnostics}/20260731-vlass-4096-4spw-wide-division-sidecar-v2.host.json"
callsite_comparison="${diagnostics}/20260731-vlass-casa-aw-divsc3-callsite-comparison-v8.json"
output="${diagnostics}/20260731-vlass-4096-4spw-predivision-source-phase-v1.json"

fail() {
  echo "VLASS pre-division source-phase certificate: $*" >&2
  exit 1
}

inputs=(
  "${term_comparison}"
  "${phase_receipt}"
  "${wide_sidecar_host}"
  "${callsite_comparison}"
)
expected_sha256=(
  "c0aaccfd0a457e695153cf14e72e0d654bad1fa250c0e55ec019bec8e12c54b8"
  "7203078f269405c0ca58fd37abe4566e6b2edb188181de6c02e9c1a839de1fde"
  "79f6dd1799c10d287d812d7cd27e91920b55682173a811d37feac4e1ac85a2ac"
  "de844de18173771a7ce3b1e7919fe5438e017d716632377e1832c1523a0054ac"
)
for index in "${!inputs[@]}"; do
  input="${inputs[${index}]}"
  [[ -f "${input}" ]] || fail "missing frozen input ${input}"
  actual="$(shasum -a 256 "${input}" | awk '{print $1}')"
  [[ "${actual}" == "${expected_sha256[${index}]}" ]] ||
    fail "frozen input hash differs for ${input}: ${actual}"
done
[[ ! -e "${output}" ]] || fail "refusing to overwrite ${output}"

python3 "${experiment_dir}/vlass_predivision_source_phase_compare.py" \
  --term-comparison "${term_comparison}" \
  --phase-receipt "${phase_receipt}" \
  --wide-sidecar-host "${wide_sidecar_host}" \
  --callsite-comparison "${callsite_comparison}" \
  --output "${output}"

shasum -a 256 "${output}"
