#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

# This launcher is intentionally one-shot. Once the immutable case directory
# exists, every later invocation fails before CASA is started.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
expected_head="${1:-${CASA_RS_VLASS_EXPECTED_HEAD:-}}"
branch="codex/vlass-w1-evidence-fiducials"
case_dir="${external_root}/artifacts/experiments/casa-aw-datagrid-native-components-4096-full16-first-vb-v1"
frozen_prefix="${external_root}/casa-reduced-clean/4096-full-16-spw/casa"
prepared_prefix="${case_dir}/casa"
receipt="${case_dir}/receipt.json"
run_log="${case_dir}/casa.log"
comparison="${case_dir}/comparison.json"
comparison_log="${case_dir}/comparison.log"
provenance="${case_dir}/provenance.tsv"
cf_cache_before="${case_dir}/cf-cache-metadata-before.tsv"
cf_cache_after="${case_dir}/cf-cache-metadata-after.tsv"
casa_v5_receipt="${external_root}/artifacts/experiments/casa-aw-datagrid-bracket-4096-full16-one-block-v5/receipt.json"
casa_clean_receipt="${external_root}/receipts/runs/20260729-vlass-reduced-casa-clean-4096-full-16-spw.json"
ms="${external_root}/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
tclean_last="${external_root}/data/frozen-clean-b80d5e87487a/tclean.last"
cf_cache="${external_root}/cf-cache/6.7.5.18/single-field-4096-full-16-spw"
mask="${external_root}/masks/vlass-single-field-peak-box-4096.mask"
source_archive="${CASA_RS_VLASS_SOURCE_ARCHIVE:-/Volumes/GLENDENNING/vlass_test.tgz}"
dataset_geometry="${repo_root}/tools/perf/imager/recipes/vlass-fragment-dataset-geometry.json"
interposer_source="${experiment_dir}/casa_aw_datatogrid_native_components_interpose.cc"
build_script="${experiment_dir}/build_casa_aw_datatogrid_native_components_interpose.sh"
python_launcher="${experiment_dir}/vlass_casa_aw_datatogrid_native_components.py"
validator="${experiment_dir}/vlass_casa_aw_datatogrid_native_components_validate.py"
casa_python="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python"
official_synthesis="/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib/libcasacpp_synthesis.6.dylib"
interposer="${TMPDIR:-/tmp}/casa_aw_datatogrid_native_components_interpose.dylib"
timeout_command="/opt/homebrew/bin/timeout"
casa_source_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"
casacore_source_commit="25b653f6963a78a1dcfc8e16954081e091a50fbe"
casa_v5_receipt_sha="fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f"
casa_clean_receipt_sha="f2f8034eaa9ec9d5d3f1d5cc3c5e628fe68e5e82cf8e38d735ff29d65004cec8"
official_synthesis_sha="0e86c46963025b4deac2bd2b795788dac46f333b4c72a966846b96a8afb2f697"
tclean_last_sha="a64e6213d66436fee6d602eb5bbda3ac8667b8df2491ea7310557748bbbf15b5"
source_archive_sha="b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a"
ms_tree_sha="037db124913cdf66de670698536f1bb38c9dbac3725a561fd79eee8bb055fd91"
dataset_receipt_sha="ba6fe4482b89297da3cb1d2856a2d47037e767f016d7c63efa7a186ec7c89628"
dataset_geometry_sha="28b1350f2754e4439a0ac94480eb4efb054ecf03f221c805e98cf34c6b5f77f1"

fail() {
    echo "VLASS CASA native-component oracle: $*" >&2
    exit 1
}

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

snapshot_cf_cache() {
    python3 -c '
import os
import stat
import sys

root = os.path.realpath(sys.argv[1])
output = sys.argv[2]
root_metadata = os.lstat(root)
records = [
    f".\tdirectory\t{root_metadata.st_size}\t{root_metadata.st_mtime_ns}"
]
file_count = 0
file_bytes = 0
for directory, directories, files in os.walk(root, topdown=True, followlinks=False):
    directories.sort()
    files.sort()
    for name in directories + files:
        path = os.path.join(directory, name)
        metadata = os.lstat(path)
        mode = metadata.st_mode
        kind = (
            "directory" if stat.S_ISDIR(mode)
            else "file" if stat.S_ISREG(mode)
            else "symlink" if stat.S_ISLNK(mode)
            else "other"
        )
        relative = os.path.relpath(path, root)
        records.append(
            f"{relative}\t{kind}\t{metadata.st_size}\t{metadata.st_mtime_ns}"
        )
        if stat.S_ISREG(mode):
            file_count += 1
            file_bytes += metadata.st_size
records.sort()
with open(output, "x", encoding="utf-8", newline="\n") as handle:
    handle.write("\n".join(records) + "\n")
print(f"{file_count}\t{file_bytes}")
' "${cf_cache}" "$1"
}

require_clean_checkout() {
    [[ -z "$(git -C "${repo_root}" status --porcelain)" ]] ||
        fail "worktree is not clean"
}

require_exact_checkpoint() {
    [[ "$(git -C "${repo_root}" rev-parse HEAD)" == "${expected_head}" ]] ||
        fail "local HEAD does not match the requested checkpoint"
    [[ "$(git -C "${repo_root}" rev-parse "refs/remotes/origin/${branch}")" == "${expected_head}" ]] ||
        fail "origin/${branch} does not match the requested checkpoint; fetch or push first"
    remote_head="$(
        git -C "${repo_root}" ls-remote --exit-code origin "refs/heads/${branch}" |
            awk 'NR == 1 { print $1 }'
    )"
    [[ "${remote_head}" == "${expected_head}" ]] ||
        fail "live origin/${branch} does not match the requested checkpoint"
}

[[ "$(uname -s)" == "Darwin" ]] || fail "the diagnostic is macOS-only"
[[ "${external_root}" = /* ]] || fail "experiment root must be absolute"
[[ "${expected_head}" =~ ^[0-9a-f]{40}$ ]] ||
    fail "pass the exact 40-hex checkpoint as argument 1 or CASA_RS_VLASS_EXPECTED_HEAD"
[[ "$(git -C "${repo_root}" branch --show-current)" == "${branch}" ]] ||
    fail "expected branch ${branch}"
require_exact_checkpoint
require_clean_checkout

[[ ! -e "${case_dir}" ]] ||
    fail "refusing to overwrite or rerun immutable case ${case_dir}"
for required_path in \
    "${frozen_prefix}.model.tt0" \
    "${frozen_prefix}.model.tt1" \
    "${casa_v5_receipt}" \
    "${casa_clean_receipt}" \
    "${ms}" \
    "${tclean_last}" \
    "${cf_cache}" \
    "${mask}" \
    "${source_archive}" \
    "${dataset_geometry}" \
    "${interposer_source}" \
    "${build_script}" \
    "${python_launcher}" \
    "${validator}" \
    "${casa_python}" \
    "${official_synthesis}"; do
    [[ -e "${required_path}" ]] || fail "missing required input ${required_path}"
done
[[ -x "${casa_python}" ]] || fail "CASA Python is not executable"
[[ -x "${timeout_command}" ]] || fail "missing GNU timeout at ${timeout_command}"
[[ "$(find "$(dirname "${frozen_prefix}")" -maxdepth 1 -type d -name 'casa.*' | wc -l | tr -d ' ')" == "19" ]] ||
    fail "frozen CASA prefix does not contain exactly 19 products"
[[ "$(find "${cf_cache}" -maxdepth 1 -type d -name 'CFS*' | wc -l | tr -d ' ')" == "1024" ]] ||
    fail "CF cache does not contain exactly 1024 CFS entries"
[[ "$(find "${cf_cache}" -maxdepth 1 -type d -name 'WTCFS*' | wc -l | tr -d ' ')" == "1024" ]] ||
    fail "CF cache does not contain exactly 1024 WTCFS entries"
[[ "$(sha256 "${casa_v5_receipt}")" == "${casa_v5_receipt_sha}" ]] ||
    fail "frozen CASA v5 receipt SHA-256 changed"
[[ "$(sha256 "${casa_clean_receipt}")" == "${casa_clean_receipt_sha}" ]] ||
    fail "frozen CASA clean receipt SHA-256 changed"
[[ "$(sha256 "${official_synthesis}")" == "${official_synthesis_sha}" ]] ||
    fail "official CASA synthesis dylib SHA-256 changed"
[[ "$(sha256 "${tclean_last}")" == "${tclean_last_sha}" ]] ||
    fail "frozen tclean.last SHA-256 changed"
[[ "$(sha256 "${source_archive}")" == "${source_archive_sha}" ]] ||
    fail "frozen VLASS source archive SHA-256 changed"
[[ "$(sha256 "${dataset_geometry}")" == "${dataset_geometry_sha}" ]] ||
    fail "frozen dataset-geometry receipt SHA-256 changed"
python3 -c \
    'import json,sys; r=json.load(open(sys.argv[1])); v=r["native_first_vb"]; assert r["casa_source_commit"] == sys.argv[2]; assert v["begin_row"] == 0 and v["end_row"] == v["n_row"] == 325 and v["spw_id"] == 2; assert v["n_data_chan"] == 64 and v["n_data_pol"] == 4; assert [c["source_count"] for c in r["calls"]] == [12359,12359]; assert [c["stream_hash"] for c in r["calls"]] == [4740440223154359747,4740440223154359747]; assert [c["geometry_hash"] for c in r["calls"]] == [15079793846523608377,14381099959812707833]' \
    "${casa_v5_receipt}" "${casa_source_commit}"
python3 -c \
    'import json,sys; r=json.load(open(sys.argv[1])); d=r["dataset"]; assert d["archive_sha256"] == sys.argv[2]; assert d["tree_sha256"] == sys.argv[3]; assert r["source_receipts"]["dataset_receipt_sha256"] == sys.argv[4]; assert r["selections"]["single_field"]["selected_rows"] == 10400' \
    "${dataset_geometry}" "${source_archive_sha}" "${ms_tree_sha}" "${dataset_receipt_sha}"

output_parent="$(dirname "${case_dir}")"
python3 -c \
    'import os,sys; repo=os.path.realpath(sys.argv[1]); out=os.path.realpath(sys.argv[2]); assert os.path.commonpath([repo,out]) != repo' \
    "${repo_root}" "${output_parent}" ||
    fail "artifact output must be outside the repository"
[[ "$(stat -f '%d' "$(dirname "${frozen_prefix}")")" == "$(stat -f '%d' "${output_parent}")" ]] ||
    fail "frozen products and output parent are on different devices; COW clone is unavailable"

bash "${build_script}"
[[ -f "${interposer}" ]] || fail "build did not create ${interposer}"
require_clean_checkout
require_exact_checkpoint

mkdir "${case_dir}"
cp -cR "${frozen_prefix}".* "${case_dir}/"
[[ "$(find "${case_dir}" -maxdepth 1 -type d -name 'casa.*' | wc -l | tr -d ' ')" == "19" ]] ||
    fail "copy-on-write clone did not produce exactly 19 products; preserve ${case_dir}"
{
    printf 'schema\tcasa-rs-vlass-casa-aw-native-components-provenance-v1\n'
    printf 'started_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'branch\t%s\n' "${branch}"
    printf 'revision\t%s\n' "${expected_head}"
    printf 'launcher_sha256\t%s\n' "$(sha256 "${BASH_SOURCE[0]}")"
    printf 'interposer_source_sha256\t%s\n' "$(sha256 "${interposer_source}")"
    printf 'interposer_binary_sha256\t%s\n' "$(sha256 "${interposer}")"
    printf 'build_script_sha256\t%s\n' "$(sha256 "${build_script}")"
    printf 'python_launcher_sha256\t%s\n' "$(sha256 "${python_launcher}")"
    printf 'validator_sha256\t%s\n' "$(sha256 "${validator}")"
    printf 'casa_python_sha256\t%s\n' "$(sha256 "${casa_python}")"
    printf 'official_synthesis_sha256\t%s\n' "${official_synthesis_sha}"
    printf 'casa_source_commit\t%s\n' "${casa_source_commit}"
    printf 'casacore_source_commit\t%s\n' "${casacore_source_commit}"
    printf 'casa_v5_receipt_sha256\t%s\n' "${casa_v5_receipt_sha}"
    printf 'casa_clean_receipt_sha256\t%s\n' "${casa_clean_receipt_sha}"
    printf 'tclean_last_sha256\t%s\n' "${tclean_last_sha}"
    printf 'source_archive_sha256\t%s\n' "${source_archive_sha}"
    printf 'declared_measurement_set_tree_sha256\t%s\n' "${ms_tree_sha}"
    printf 'dataset_receipt_sha256\t%s\n' "${dataset_receipt_sha}"
    printf 'dataset_geometry_sha256\t%s\n' "${dataset_geometry_sha}"
    printf 'measurement_set\t%s\n' "${ms}"
    printf 'cf_cache\t%s\n' "${cf_cache}"
    printf 'mask\t%s\n' "${mask}"
    printf 'frozen_product_prefix\t%s\n' "${frozen_prefix}"
} >"${provenance}"

cf_before_summary="$(snapshot_cf_cache "${cf_cache_before}")"
printf 'cf_cache_metadata_before_sha256\t%s\n' "$(sha256 "${cf_cache_before}")" >>"${provenance}"
printf 'cf_cache_regular_files_before\t%s\n' "${cf_before_summary%%$'\t'*}" >>"${provenance}"
printf 'cf_cache_regular_bytes_before\t%s\n' "${cf_before_summary#*$'\t'}" >>"${provenance}"

set +e
env \
    CASA_RS_VLASS_EXPERIMENT_ROOT="${external_root}" \
    CASA_AW_NATIVE_COMPONENTS_OUTPUT="${receipt}" \
    CASA_AW_NATIVE_COMPONENTS_EXPECT_NXY=4096 \
    DYLD_INSERT_LIBRARIES="${interposer}" \
    OMP_NUM_THREADS=1 \
    "${timeout_command}" --signal=TERM --kill-after=15s 300s \
    "${casa_python}" \
    "${python_launcher}" \
    --prepared-prefix "${prepared_prefix}" \
    >"${run_log}" 2>&1
run_status=$?
set -e

cf_after_summary="$(snapshot_cf_cache "${cf_cache_after}")"
printf 'cf_cache_metadata_after_sha256\t%s\n' "$(sha256 "${cf_cache_after}")" >>"${provenance}"
printf 'cf_cache_regular_files_after\t%s\n' "${cf_after_summary%%$'\t'*}" >>"${provenance}"
printf 'cf_cache_regular_bytes_after\t%s\n' "${cf_after_summary#*$'\t'}" >>"${provenance}"
if cmp -s "${cf_cache_before}" "${cf_cache_after}"; then
    printf 'cf_cache_metadata_unchanged\ttrue\n' >>"${provenance}"
else
    printf 'cf_cache_metadata_unchanged\tfalse\n' >>"${provenance}"
    fail "shared CF-cache metadata changed during the diagnostic; preserve ${case_dir}"
fi
printf 'casa_exit\t%s\n' "${run_status}" >>"${provenance}"
[[ "${run_status}" == "86" ]] ||
    fail "CASA exited ${run_status}, expected controlled status 86; preserve ${case_dir}"
[[ -s "${receipt}" ]] ||
    fail "controlled exit 86 did not create a non-empty receipt; preserve ${case_dir}"
[[ "$(find "${case_dir}" -maxdepth 1 -type d -name 'casa.*' | wc -l | tr -d ' ')" == "19" ]] ||
    fail "the diagnostic changed the CASA product inventory; preserve ${case_dir}"

set +e
python3 "${validator}" \
    --candidate "${receipt}" \
    --casa-v5 "${casa_v5_receipt}" \
    --output "${comparison}" \
    >"${comparison_log}" 2>&1
comparison_status=$?
set -e
printf 'receipt_sha256\t%s\n' "$(sha256 "${receipt}")" >>"${provenance}"
printf 'comparison_exit\t%s\n' "${comparison_status}" >>"${provenance}"
[[ "${comparison_status}" == "0" ]] ||
    fail "native-component evidence failed validation; preserve ${case_dir}"
[[ -s "${comparison}" ]] ||
    fail "validator did not create ${comparison}; preserve ${case_dir}"
classification="$(
    python3 -c \
        'import json,sys; print(json.load(open(sys.argv[1]))["comparison"]["classification"])' \
        "${comparison}"
)"
[[ "${classification}" == "exact-frozen-v5-native-components" ]] ||
    fail "unexpected validator classification ${classification}; preserve ${case_dir}"
printf 'comparison_sha256\t%s\n' "$(sha256 "${comparison}")" >>"${provenance}"
printf 'classification\t%s\n' "${classification}" >>"${provenance}"
printf 'completed_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"${provenance}"
printf 'result\tvalid-exact-frozen-v5-native-components\n' >>"${provenance}"
require_clean_checkout
require_exact_checkpoint
echo "VLASS CASA native-component oracle completed: ${comparison}"
