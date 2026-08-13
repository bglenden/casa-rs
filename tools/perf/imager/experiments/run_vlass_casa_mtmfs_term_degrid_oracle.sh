#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
source_ms="${external_root}/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
model_prefix="${external_root}/casa-reduced-clean/4096-four-spw/casa"
cfcache="${external_root}/cf-cache/6.7.5.18/single-field-4096-four-spw"
output_dir="${external_root}/artifacts/experiments/vlass-4spw-casa-mtmfs-term-degrid-oracle-v2"
scratch_ms="${output_dir}/scratch.ms"
output_prefix="${output_dir}/casa"
diagnostic_prefix="${external_root}/receipts/diagnostics/20260731-vlass-4spw-casa-mtmfs-term-degrid-oracle-v2"
binary="${diagnostic_prefix}.bin"
receipt="${diagnostic_prefix}.host.json"
log="${external_root}/receipts/runs/20260731-vlass-4spw-casa-mtmfs-term-degrid-oracle-v2.log"
casa_log="${external_root}/receipts/runs/20260731-vlass-4spw-casa-mtmfs-term-degrid-oracle-v2.casa.log"
unreachable_npz="${output_dir}/unreachable.npz"
casa_python="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python"
interposer="${CASA_MTMFS_TERM_DEGRID_INTERPOSER:-${TMPDIR:-/tmp}/casa_mtmfs_term_degrid_interpose.dylib}"
timeout_command="/opt/homebrew/bin/timeout"

fail() {
    echo "CASA MT-MFS term-degrid oracle: $*" >&2
    exit 1
}

[[ ! -e "${output_dir}" ]] ||
    fail "refusing to overwrite experiment directory ${output_dir}"
for artifact in "${binary}" "${receipt}" "${log}" "${casa_log}"; do
    [[ ! -e "${artifact}" ]] ||
        fail "refusing to overwrite artifact ${artifact}"
done
[[ -d "${source_ms}" ]] || fail "missing frozen source MS ${source_ms}"
[[ -d "${model_prefix}.model.tt0" && -d "${model_prefix}.model.tt1" ]] ||
    fail "missing frozen four-SPW model terms"
[[ -d "${cfcache}" ]] || fail "missing frozen four-SPW CF cache"
[[ -x "${casa_python}" ]] || fail "missing CASA 6.7.5.18 Python"
[[ -x "${timeout_command}" ]] || fail "missing GNU timeout watchdog"

bash "${experiment_dir}/build_casa_mtmfs_term_degrid_interpose.sh"
[[ -f "${interposer}" ]] || fail "interposer build did not create ${interposer}"

mkdir -p "${output_dir}" "$(dirname "${binary}")" "$(dirname "${log}")"
cp -cR "${source_ms}" "${scratch_ms}"
cp -cR "${model_prefix}".* "${output_dir}/"

set +e
env \
    CASA_MTMFS_TERM_DEGRID_BINARY="${binary}" \
    CASA_MTMFS_TERM_DEGRID_RECEIPT="${receipt}" \
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
[[ -f "${binary}" && -f "${receipt}" ]] ||
    fail "CASA exited 86 without the binary and receipt"
[[ ! -e "${unreachable_npz}" ]] ||
    fail "CASA continued beyond the interposed finalizeToVis boundary"

"${casa_python}" -c \
    'import json,sys; r=json.load(open(sys.argv[1])); assert r["status"] == "completed-before-finalize-to-vis"; assert r["term_count"] == 2; assert r["binary_record_size"] == 104; assert r["binary_record_count"] > 0; assert r["formed_residual"] is False; assert r["residual_grid_dispatch"] is False; assert r["fft"] == "not-entered"; print(json.dumps(r, sort_keys=True))' \
    "${receipt}"
echo "${log}"
