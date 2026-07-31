#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
source_ms="${external_root}/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
model_prefix="${external_root}/casa-reduced-clean/4096-four-spw/casa"
cfcache="${external_root}/cf-cache/6.7.5.18/single-field-4096-four-spw"
output_dir="${external_root}/artifacts/experiments/vlass-4spw-casa-aw-degrid-prefix-current-v1"
scratch_ms="${output_dir}/scratch.ms"
output_prefix="${output_dir}/casa"
trace="${external_root}/receipts/diagnostics/20260731-vlass-4spw-casa-aw-degrid-prefix-current-v1.txt"
log="${external_root}/receipts/runs/20260731-vlass-4spw-casa-aw-degrid-prefix-current-v1.log"
casa_log="${external_root}/receipts/runs/20260731-vlass-4spw-casa-aw-degrid-prefix-current-v1.casa.log"
unreachable_npz="${output_dir}/unreachable.npz"
casa_python="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python"
interposer="${CASA_AW_DEGRID_PREFIX_INTERPOSER:-${TMPDIR:-/tmp}/casa_aw_degrid_prefix_interpose.dylib}"
timeout_command="/opt/homebrew/bin/timeout"

fail() {
    echo "CASA AW degrid-prefix oracle: $*" >&2
    exit 1
}

[[ ! -e "${output_dir}" ]] ||
    fail "refusing to overwrite experiment directory ${output_dir}"
for artifact in "${trace}" "${log}" "${casa_log}"; do
    [[ ! -e "${artifact}" ]] ||
        fail "refusing to overwrite artifact ${artifact}"
done
[[ -d "${source_ms}" ]] || fail "missing frozen source MS ${source_ms}"
[[ -d "${model_prefix}.model.tt0" && -d "${model_prefix}.model.tt1" ]] ||
    fail "missing frozen four-SPW model terms"
[[ -d "${cfcache}" ]] || fail "missing frozen four-SPW CF cache"
[[ -x "${casa_python}" ]] || fail "missing CASA 6.7.5.18 Python"
[[ -x "${timeout_command}" ]] || fail "missing GNU timeout watchdog"

bash "${experiment_dir}/build_casa_aw_degrid_prefix_interpose.sh"
[[ -f "${interposer}" ]] || fail "interposer build did not create ${interposer}"

mkdir -p "${output_dir}" "$(dirname "${trace}")" "$(dirname "${log}")"
cp -cR "${source_ms}" "${scratch_ms}"
cp -cR "${model_prefix}".* "${output_dir}/"

set +e
env \
    CASA_VLASS_DEGRID_PREFIX_TRACE="${trace}" \
    CASA_VLASS_DEGRID_TRACE_ROW=0 \
    CASA_VLASS_DEGRID_TRACE_CHANNEL=11 \
    CASA_VLASS_DEGRID_TRACE_POL=0 \
    CASA_VLASS_DEGRID_TRACE_MCOL=0 \
    DYLD_INSERT_LIBRARIES="${interposer}" \
    OMP_NUM_THREADS=1 \
    "${timeout_command}" --signal=TERM --kill-after=15s 300s \
    "${casa_python}" \
    "${experiment_dir}/vlass_casa_prediction_trace.py" \
    --source-ms "${source_ms}" \
    --scratch-ms "${scratch_ms}" \
    --zero-prefix "${model_prefix}" \
    --model-prefix "${model_prefix}" \
    --output-prefix "${output_prefix}" \
    --output-npz "${unreachable_npz}" \
    --casa-log "${casa_log}" \
    --cfcache "${cfcache}" \
    --profile four-spw \
    --resume-existing \
    >"${log}" 2>&1
status=$?
set -e

[[ "${status}" == "86" ]] ||
    fail "CASA exited ${status}, expected 86; see ${log}"
[[ -f "${trace}" ]] || fail "CASA exited 86 without the prefix trace"
[[ "$(grep -c '^meta ' "${trace}")" == "1" ]] ||
    fail "prefix trace does not contain exactly one meta record"
[[ "$(grep -c '^tap ' "${trace}")" == "361" ]] ||
    fail "prefix trace does not contain exactly 361 tap records"
[[ "$(grep -c '^result ' "${trace}")" == "1" ]] ||
    fail "prefix trace does not contain exactly one result record"
[[ ! -e "${unreachable_npz}" ]] ||
    fail "CASA continued beyond the interposed first-source boundary"

shasum -a 256 "${trace}"
echo "${log}"
