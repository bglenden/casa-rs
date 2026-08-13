#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
frozen_prefix="${external_root}/casa-reduced-clean/4096-full-16-spw/casa"
output_dir="${external_root}/artifacts/experiments/casa-aw-datagrid-bracket-4096-full16-one-block-v5"
prepared_prefix="${output_dir}/casa"
receipt="${output_dir}/receipt.json"
log="${output_dir}/casa.log"
casa_python="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python"
interposer="${TMPDIR:-/tmp}/casa_aw_datatogrid_bracket_interpose.dylib"
timeout_command="/opt/homebrew/bin/timeout"

if [[ -e "${output_dir}" ]]; then
  echo "refusing to overwrite one-block bracket output: ${output_dir}" >&2
  exit 1
fi
if [[ ! -x "${casa_python}" ]]; then
  echo "missing CASA 6.7.5.18 Python: ${casa_python}" >&2
  exit 1
fi
if [[ ! -x "${timeout_command}" ]]; then
  echo "missing GNU timeout watchdog: ${timeout_command}" >&2
  exit 1
fi
if [[ "$(find "$(dirname "${frozen_prefix}")" -maxdepth 1 -type d -name 'casa.*' | wc -l | tr -d ' ')" != "19" ]]; then
  echo "frozen 4096 full-16-SPW prefix does not contain exactly 19 products" >&2
  exit 1
fi

"${experiment_dir}/build_casa_aw_datatogrid_bracket_interpose.sh"
if [[ ! -f "${interposer}" ]]; then
  echo "bracket build did not create ${interposer}" >&2
  exit 1
fi

mkdir -p "${output_dir}"
cp -cR "${frozen_prefix}".* "${output_dir}/"

set +e
env \
  CASA_RS_VLASS_EXPERIMENT_ROOT="${external_root}" \
  CASA_AW_BRACKET_OUTPUT="${receipt}" \
  CASA_AW_BRACKET_EXPECT_NXY=4096 \
  CASA_AW_BRACKET_BLOCKS=1 \
  CASA_AW_BRACKET_TERMS=2 \
  DYLD_INSERT_LIBRARIES="${interposer}" \
  OMP_NUM_THREADS=1 \
  "${timeout_command}" --signal=TERM --kill-after=15s 300s \
  "${casa_python}" \
  "${experiment_dir}/vlass_casa_aw_datatogrid_bracket.py" \
  --prepared-prefix "${prepared_prefix}" \
  >"${log}" 2>&1
status=$?
set -e

if [[ "${status}" != "86" ]]; then
  echo "CASA AW one-block bracket exited ${status}, expected 86; see ${log}" >&2
  exit 1
fi
if [[ ! -f "${receipt}" ]]; then
  echo "CASA AW one-block bracket exited 86 without ${receipt}" >&2
  exit 1
fi

"${casa_python}" -c \
  'import json,sys; p=sys.argv[1]; r=json.load(open(p)); assert r["status"] == "completed-before-finalize"; assert r["completed_calls"] == 2; assert r["completed_blocks"] == 1; assert r["formed_image"] is False; assert r["normalization"] == "not-entered"; assert r["fft"] == "not-entered"; v=r["native_first_vb"]; assert 0 <= v["begin_row"] < v["end_row"] <= v["n_row"]; assert v["row_ids_count"] == len(v["row_ids"]) == v["n_row"]; assert v["row_ids"][0] == v["row_id_first"]; assert v["row_ids"][-1] == v["row_id_last"]; assert v["n_data_chan"] == v["chan_map_count"] == v["freq_count"]; assert v["n_data_pol"] == v["pol_map_count"]; print(json.dumps(r, sort_keys=True))' \
  "${receipt}"
