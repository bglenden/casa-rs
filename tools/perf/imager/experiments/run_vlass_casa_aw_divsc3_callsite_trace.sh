#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
diagnostics="${external_root}/receipts/diagnostics"
runs="${external_root}/receipts/runs"
manifest="${diagnostics}/20260731-vlass-casa-aw-divsc3-callsite-manifest-v1.json"
raw_trace="${diagnostics}/20260731-vlass-casa-aw-divsc3-callsite-raw-trace-v5.json"
comparison="${diagnostics}/20260731-vlass-casa-aw-divsc3-callsite-comparison-v5.json"
ready="${diagnostics}/20260731-vlass-casa-aw-divsc3-callsite-ready-v5.json"
lldb_log="${runs}/20260731-vlass-casa-aw-divsc3-callsite-trace-v5.log"
casa_log="${runs}/20260731-vlass-casa-aw-divsc3-callsite-trace-v5.casa.log"
launch_log="${runs}/20260731-vlass-casa-aw-divsc3-callsite-launch-v5.log"
npz="${runs}/20260730-vlass-4spw-frozen-casa-model-prediction-boundary-v2.npz"
source_trace="${diagnostics}/20260730-vlass-4096-4spw-casars-prediction-source-trace-v1.json"
term_oracle="${diagnostics}/20260731-vlass-4spw-casa-mtmfs-term-degrid-oracle-v2.bin"
library="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib/libcasacpp_synthesis.6.dylib"
casa_python="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python"
casa_source="/tmp/casa-6.7.5.18-oracle-source/casatools/src/code/synthesis/TransformMachines2/AWVisResampler.cc"
source_ms="${external_root}/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
model_prefix="${external_root}/casa-reduced-clean/4096-four-spw/casa"
output_dir="${external_root}/artifacts/experiments/vlass-4spw-casa-aw-divsc3-callsite-trace-v5"
scratch_ms="${output_dir}/scratch.ms"
output_prefix="${output_dir}/casa"
unreachable_npz="${output_dir}/unreachable.npz"
timeout_command="/opt/homebrew/bin/timeout"
expected_manifest_sha256="929e77423638bbd0d0f29102182b055fb3516fdfd504d00ee759b0eeb6ff75f6"

fail() {
  echo "CASA AW division call-site trace: $*" >&2
  exit 1
}

for required in "${manifest}" "${npz}" "${source_trace}" "${term_oracle}" "${library}" \
  "${casa_python}" "${casa_source}" "${timeout_command}"; do
  [[ -e "${required}" ]] || fail "missing required input ${required}"
done
[[ -d "${source_ms}" ]] || fail "missing frozen source MS"
[[ -d "${model_prefix}.model.tt0" && -d "${model_prefix}.model.tt1" ]] ||
  fail "missing frozen model terms"
[[ ! -e "${output_dir}" ]] ||
  fail "refusing to overwrite fresh call-site bundle ${output_dir}"
for artifact in "${raw_trace}" "${comparison}" "${ready}" "${lldb_log}" \
  "${casa_log}" "${launch_log}" "${unreachable_npz}"; do
  [[ ! -e "${artifact}" ]] || fail "refusing to overwrite ${artifact}"
done
mkdir -p "${diagnostics}" "${runs}"
mkdir -p "${output_dir}"
cp -cR "${source_ms}" "${scratch_ms}"
cp -cR "${model_prefix}".* "${output_dir}/"

actual_manifest_sha256="$(shasum -a 256 "${manifest}" | awk '{print $1}')"
[[ "${actual_manifest_sha256}" == "${expected_manifest_sha256}" ]] ||
  fail "frozen call-site manifest checksum changed"

env \
  CASA_VLASS_CALLSITE_MANIFEST="${manifest}" \
  CASA_VLASS_CALLSITE_RAW_TRACE="${raw_trace}" \
  CASA_VLASS_CALLSITE_DEBUGGER_READY="${ready}" \
  OMP_NUM_THREADS=1 \
  "${casa_python}" \
  "${experiment_dir}/vlass_casa_prediction_trace.py" \
  --source-ms "${source_ms}" \
  --scratch-ms "${scratch_ms}" \
  --zero-prefix "${model_prefix}" \
  --model-prefix "${model_prefix}" \
  --output-prefix "${output_prefix}" \
  --output-npz "${unreachable_npz}" \
  --casa-log "${casa_log}" \
  --cfcache "${external_root}/cf-cache/6.7.5.18/single-field-4096-four-spw" \
  --profile four-spw \
  --resume-existing \
  >"${launch_log}" 2>&1 &
casa_pid=$!

cleanup() {
  if kill -0 "${casa_pid}" 2>/dev/null; then
    kill -TERM "${casa_pid}" 2>/dev/null || true
    wait "${casa_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

ready_deadline=$((SECONDS + 120))
while [[ ! -f "${ready}" ]]; do
  kill -0 "${casa_pid}" 2>/dev/null ||
    fail "CASA exited before the debugger-ready boundary; see ${launch_log}"
  ((SECONDS < ready_deadline)) ||
    fail "CASA did not reach the debugger-ready boundary within 120 seconds"
  sleep 0.1
done
ready_pid="$(
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["pid"])' "${ready}"
)"
[[ "${ready_pid}" == "${casa_pid}" ]] ||
  fail "debugger-ready PID ${ready_pid} differs from launched CASA PID ${casa_pid}"

set +e
env \
  CASA_VLASS_CALLSITE_MANIFEST="${manifest}" \
  CASA_VLASS_CALLSITE_RAW_TRACE="${raw_trace}" \
  "${timeout_command}" --signal=TERM --kill-after=15s 300s \
  lldb -b \
  -p "${casa_pid}" \
  -o "settings set auto-confirm true" \
  -o "command script import ${experiment_dir}/vlass_casa_aw_divsc3_callsite_trace_lldb.py" \
  -o "vlass-install-callsite-breakpoint" \
  -o "process continue" \
  -o "vlass-require-callsite-trace" \
  -o "process kill" \
  -o "quit" \
  >"${lldb_log}" 2>&1
lldb_status=$?
set -e
wait "${casa_pid}" 2>/dev/null || true
trap - EXIT

[[ "${lldb_status}" == "0" ]] ||
  fail "LLDB exited ${lldb_status}; see ${lldb_log}"
[[ -f "${raw_trace}" ]] || fail "LLDB did not emit the raw trace"
[[ ! -e "${unreachable_npz}" ]] ||
  fail "CASA continued beyond the source-1446 return boundary"

python3 "${experiment_dir}/vlass_casa_aw_divsc3_callsite_trace.py" analyze \
  --manifest "${manifest}" \
  --raw-trace "${raw_trace}" \
  --library "${library}" \
  --output "${comparison}"

shasum -a 256 \
  "${manifest}" \
  "${ready}" \
  "${raw_trace}" \
  "${comparison}" \
  "${lldb_log}" \
  "${casa_log}" \
  "${launch_log}"
