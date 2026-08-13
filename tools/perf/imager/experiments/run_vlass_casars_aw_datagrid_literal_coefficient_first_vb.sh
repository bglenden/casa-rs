#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
expected_head="${1:-${CASA_RS_VLASS_EXPECTED_HEAD:-}}"
branch="codex/vlass-w1-evidence-fiducials"
case_dir="${external_root}/artifacts/experiments/casa-rs-aw-datagrid-literal-coefficient-4096-full16-first-vb-v1"
receipt="${case_dir}/receipt.json"
run_log="${case_dir}/casars-imager.log"
comparison="${case_dir}/comparison.json"
comparison_log="${case_dir}/comparison.log"
provenance="${case_dir}/provenance.tsv"
image_prefix="${case_dir}/rust"
ms="${external_root}/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
tclean_last="${external_root}/data/frozen-clean-b80d5e87487a/tclean.last"
cf_cache="${external_root}/cf-cache/6.7.5.18/single-field-4096-full-16-spw"
mask="${external_root}/masks/vlass-single-field-peak-box-4096.mask"
casa_rs_v4_receipt="${external_root}/artifacts/experiments/casa-rs-aw-datagrid-bracket-4096-full16-first-vb-v4/receipt.json"
casa_v5_receipt="${external_root}/artifacts/experiments/casa-aw-datagrid-bracket-4096-full16-one-block-v5/receipt.json"
arithmetic_v1_receipt="${external_root}/artifacts/experiments/casa-rs-aw-datagrid-tt0-arithmetic-compat-4096-full16-first-vb-v1/receipt.json"
arithmetic_v1_comparison="${external_root}/artifacts/experiments/casa-rs-aw-datagrid-tt0-arithmetic-compat-4096-full16-first-vb-v1/comparison.json"
source_archive="${CASA_RS_VLASS_SOURCE_ARCHIVE:-/Volumes/GLENDENNING/vlass_test.tgz}"
dataset_geometry="${repo_root}/tools/perf/imager/recipes/vlass-fragment-dataset-geometry.json"
validator="${experiment_dir}/vlass_aw_datagrid_literal_coefficient_compare.py"
parent_validator="${experiment_dir}/vlass_aw_datagrid_tt0_arithmetic_compat_compare.py"
cargo_lock="${repo_root}/Cargo.lock"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-${HOME}/.casa/data}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
timeout_command="/opt/homebrew/bin/timeout"
binary="${repo_root}/target/release/casars-imager"
casa_rs_v4_receipt_sha="1c52961a3058f8f362e9d554c64b69a077f9414a7a44c738bed5351e6df59b40"
casa_rs_v4_evidence_sha="5783293d3401f97b12742d8c89bd98e2b0d1303cabf4e19505f245db7cbe9e0a"
casa_rs_v4_revision="11cdeec698b63b9023233f3d7855d6c07d47284f"
casa_v5_receipt_sha="fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f"
arithmetic_v1_receipt_sha="a9c7fc453d343a48745269744ffd257a5ca8c532ccefe4ac74ba5a85b0ce9271"
arithmetic_v1_evidence_sha="c2b2bc4daafe12aa0090d9d00e8cdd02ca627c2fa671f846fb6625aad912af99"
arithmetic_v1_comparison_sha="e50bf9642a442688dc2f5f37390c63e1a04cd0ad19729f4daea4a0bf43be608e"
arithmetic_v1_comparison_evidence_sha="dfcd28767cb60a727f1486a49a9a9b9ad96748114ff69d47d9a8e3c8dec5f73b"
arithmetic_v1_revision="dc159dc629c5e09c83d2027d06b5d909bf4f4c0a"
casa_source_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"
casacore_source_commit="25b653f6963a78a1dcfc8e16954081e091a50fbe"
tclean_last_sha="a64e6213d66436fee6d602eb5bbda3ac8667b8df2491ea7310557748bbbf15b5"
source_archive_sha="b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a"
ms_tree_sha="037db124913cdf66de670698536f1bb38c9dbac3725a561fd79eee8bb055fd91"
dataset_receipt_sha="ba6fe4482b89297da3cb1d2856a2d47037e767f016d7c63efa7a186ec7c89628"
dataset_geometry_sha="28b1350f2754e4439a0ac94480eb4efb054ecf03f221c805e98cf34c6b5f77f1"

fail() {
    echo "VLASS casa-rs literal-coefficient audit: $*" >&2
    exit 1
}

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

require_clean_checkout() {
    [[ -z "$(git -C "${repo_root}" status --porcelain)" ]] ||
        fail "worktree is not clean"
}

require_exact_checkpoint() {
    [[ "$(git -C "${repo_root}" rev-parse HEAD)" == "${expected_head}" ]] ||
        fail "local HEAD does not match the requested checkpoint"
    [[ "$(git -C "${repo_root}" rev-parse "refs/remotes/origin/${branch}")" == "${expected_head}" ]] ||
        fail "origin/${branch} does not match the requested checkpoint; fetch or push before running"
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
    fail "pass the exact 40-hex checkpoint commit as argument 1 or CASA_RS_VLASS_EXPECTED_HEAD"
[[ "$(git -C "${repo_root}" branch --show-current)" == "${branch}" ]] ||
    fail "expected branch ${branch}"
require_exact_checkpoint
require_clean_checkout

[[ ! -e "${case_dir}" ]] || fail "refusing to overwrite ${case_dir}"
for required_path in \
    "${ms}" \
    "${tclean_last}" \
    "${cf_cache}" \
    "${mask}" \
    "${casa_rs_v4_receipt}" \
    "${casa_v5_receipt}" \
    "${arithmetic_v1_receipt}" \
    "${arithmetic_v1_comparison}" \
    "${source_archive}" \
    "${dataset_geometry}" \
    "${validator}" \
    "${parent_validator}" \
    "${cargo_lock}" \
    "${measures_dir}" \
    "${fftw_dir}"; do
    [[ -e "${required_path}" ]] || fail "missing required input ${required_path}"
done
[[ -x "${timeout_command}" ]] || fail "missing GNU timeout at ${timeout_command}"
[[ "$(sha256 "${casa_rs_v4_receipt}")" == "${casa_rs_v4_receipt_sha}" ]] ||
    fail "frozen casa-rs v4 receipt SHA-256 changed"
[[ "$(sha256 "${casa_v5_receipt}")" == "${casa_v5_receipt_sha}" ]] ||
    fail "frozen CASA v5 receipt SHA-256 changed"
[[ "$(sha256 "${arithmetic_v1_receipt}")" == "${arithmetic_v1_receipt_sha}" ]] ||
    fail "frozen arithmetic-v1 receipt SHA-256 changed"
[[ "$(sha256 "${arithmetic_v1_comparison}")" == "${arithmetic_v1_comparison_sha}" ]] ||
    fail "frozen arithmetic-v1 comparison SHA-256 changed"
[[ "$(sha256 "${tclean_last}")" == "${tclean_last_sha}" ]] ||
    fail "frozen tclean.last SHA-256 changed"
[[ "$(sha256 "${source_archive}")" == "${source_archive_sha}" ]] ||
    fail "frozen VLASS source archive SHA-256 changed"
[[ "$(sha256 "${dataset_geometry}")" == "${dataset_geometry_sha}" ]] ||
    fail "frozen VLASS dataset-geometry receipt SHA-256 changed"
python3 "${validator}" \
    --casa-rs-v4 "${casa_rs_v4_receipt}" \
    --casa-v5 "${casa_v5_receipt}" \
    --arithmetic-v1 "${arithmetic_v1_receipt}" \
    --arithmetic-v1-comparison "${arithmetic_v1_comparison}" \
    --validate-parents-only
python3 -c \
    'import json,sys; r=json.load(open(sys.argv[1])); d=r["dataset"]; assert d["archive_sha256"] == sys.argv[2]; assert d["tree_sha256"] == sys.argv[3]; assert r["source_receipts"]["dataset_receipt_sha256"] == sys.argv[4]; assert r["selections"]["single_field"]["selected_rows"] == 10400' \
    "${dataset_geometry}" "${source_archive_sha}" "${ms_tree_sha}" "${dataset_receipt_sha}"
[[ "$(find "${cf_cache}" -maxdepth 1 -type d -name 'CFS*' | wc -l | tr -d ' ')" == "1024" ]] ||
    fail "CF cache does not contain exactly 1024 CFS entries"
[[ "$(find "${cf_cache}" -maxdepth 1 -type d -name 'WTCFS*' | wc -l | tr -d ' ')" == "1024" ]] ||
    fail "CF cache does not contain exactly 1024 WTCFS entries"

output_parent="$(dirname "${case_dir}")"
python3 -c \
    'import os,sys; repo=os.path.realpath(sys.argv[1]); out=os.path.realpath(sys.argv[2]); assert os.path.commonpath([repo,out]) != repo' \
    "${repo_root}" "${output_parent}" ||
    fail "artifact output must be outside the repository"
[[ "$(stat -f '%d' "${ms}")" == "$(stat -f '%d' "${cf_cache}")" ]] ||
    fail "MeasurementSet and CF cache are on different devices"
[[ "$(stat -f '%d' "${ms}")" == "$(stat -f '%d' "${output_parent}")" ]] ||
    fail "inputs and output parent are on different devices"

(
    cd "${repo_root}"
    CARGO_INCREMENTAL=0 cargo build --locked --release -p casars-imager --bin casars-imager
)
require_clean_checkout
require_exact_checkpoint
[[ -x "${binary}" ]] || fail "release build did not create ${binary}"
num_complex_version="$(
    awk '
        $0 == "name = \"num-complex\"" { in_package = 1; next }
        in_package && /^version = / {
            gsub(/^version = "|"$|"/, "")
            print
            exit
        }
    ' "${cargo_lock}"
)"
[[ -n "${num_complex_version}" ]] ||
    fail "Cargo.lock does not contain a num-complex package"
rustc_vv="$(rustc -Vv | tr '\n' '|' | sed 's/|$//')"

mkdir "${case_dir}"
{
    printf 'schema\tcasa-rs-vlass-aw-datagrid-literal-coefficient-provenance-v1\n'
    printf 'started_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'branch\t%s\n' "${branch}"
    printf 'revision\t%s\n' "${expected_head}"
    printf 'binary_sha256\t%s\n' "$(sha256 "${binary}")"
    printf 'launcher_sha256\t%s\n' "$(sha256 "${BASH_SOURCE[0]}")"
    printf 'validator_sha256\t%s\n' "$(sha256 "${validator}")"
    printf 'parent_validator_sha256\t%s\n' "$(sha256 "${parent_validator}")"
    printf 'cargo_lock_sha256\t%s\n' "$(sha256 "${cargo_lock}")"
    printf 'num_complex_version\t%s\n' "${num_complex_version}"
    printf 'rustc_vv\t%s\n' "${rustc_vv}"
    printf 'casa_rs_v4_receipt_sha256\t%s\n' "${casa_rs_v4_receipt_sha}"
    printf 'casa_rs_v4_evidence_sha256\t%s\n' "${casa_rs_v4_evidence_sha}"
    printf 'casa_rs_v4_revision\t%s\n' "${casa_rs_v4_revision}"
    printf 'casa_v5_receipt_sha256\t%s\n' "${casa_v5_receipt_sha}"
    printf 'arithmetic_v1_receipt_sha256\t%s\n' "${arithmetic_v1_receipt_sha}"
    printf 'arithmetic_v1_evidence_sha256\t%s\n' "${arithmetic_v1_evidence_sha}"
    printf 'arithmetic_v1_comparison_sha256\t%s\n' "${arithmetic_v1_comparison_sha}"
    printf 'arithmetic_v1_comparison_evidence_sha256\t%s\n' "${arithmetic_v1_comparison_evidence_sha}"
    printf 'arithmetic_v1_revision\t%s\n' "${arithmetic_v1_revision}"
    printf 'casa_source_commit\t%s\n' "${casa_source_commit}"
    printf 'casacore_source_commit\t%s\n' "${casacore_source_commit}"
    printf 'tclean_last_sha256\t%s\n' "${tclean_last_sha}"
    printf 'source_archive_sha256\t%s\n' "${source_archive_sha}"
    printf 'declared_measurement_set_tree_sha256\t%s\n' "${ms_tree_sha}"
    printf 'dataset_receipt_sha256\t%s\n' "${dataset_receipt_sha}"
    printf 'dataset_geometry_sha256\t%s\n' "${dataset_geometry_sha}"
    printf 'source_archive\t%s\n' "${source_archive}"
    printf 'measurement_set\t%s\n' "${ms}"
    printf 'cf_cache\t%s\n' "${cf_cache}"
    printf 'mask\t%s\n' "${mask}"
} >"${provenance}"

set +e
"${timeout_command}" --signal=TERM --kill-after=15s 300s \
    env -i \
    PATH="/opt/homebrew/bin:/usr/bin:/bin" \
    HOME="${HOME}" \
    CASA_RS_MEASURESPATH="${measures_dir}" \
    CASA_RS_FFTW_LIBRARY_DIR="${fftw_dir}" \
    DYLD_LIBRARY_PATH="${fftw_dir}" \
    CASA_RS_FFTW_THREADS=1 \
    CASA_RS_AW_LITERAL_COEFFICIENT_AUDIT_OUTPUT_V1="${receipt}" \
    CASA_RS_AW_LITERAL_COEFFICIENT_AUDIT_EXPECT_NXY=4096 \
    CASA_RS_AW_LITERAL_COEFFICIENT_AUDIT_BLOCKS=1 \
    CASA_RS_AW_LITERAL_COEFFICIENT_AUDIT_TERMS=1 \
    "${binary}" \
    --ms "${ms}" \
    --imagename "${image_prefix}" \
    --imsize 4096 \
    --cell-arcsec 0.6 \
    --field 1525 \
    --phasecenter-field 1525 \
    --spw 2~17 \
    --channel-start 0 \
    --channel-count 64 \
    --specmode mfs \
    --gridder awproject \
    --interpolation linear \
    --projection SIN \
    --datacolumn data \
    --stokes I \
    --uvrange '<12km' \
    --intent 'OBSERVE_TARGET#UNSPECIFIED' \
    --usepointing \
    --weighting briggs \
    --robust 1.0 \
    --perchanweightdensity \
    --deconvolver mtmfs \
    --nterms 2 \
    --scales 0,5,12 \
    --niter 0 \
    --standard-mfs-acceleration cpu \
    --no-parallel \
    --standard-mfs-grid-threads 1 \
    --imaging-fft-precision f64 \
    --imaging-fft-backend fftw \
    --imaging-memory-target-mb 16384 \
    --imaging-memory-pressure-policy auto \
    --imaging-row-block-rows 325 \
    --imaging-prepare-workers 1 \
    --imaging-read-ahead-blocks 1 \
    --hogbom-iteration-mode strict \
    --gain 0.1 \
    --threshold-jy 0.0 \
    --nsigma 5.0 \
    --psfcutoff 0.35 \
    --pblimit 0.0001 \
    --write-pb \
    --minor-cycle-length 1 \
    --cyclefactor 3.0 \
    --minpsffraction 0.05 \
    --maxpsffraction 0.8 \
    --wterm wproject \
    --wprojplanes 32 \
    --cfcache "${cf_cache}" \
    --cf-resident-mb 256 \
    --facets 1 \
    --computepastep 360.0 \
    --rotatepastep 360.0 \
    --pointingoffsetsigdev 0.0 \
    --normtype flatnoise \
    --aterm \
    --no-psterm \
    --wbawp \
    --conjbeams \
    --no-mosweight \
    --smallscalebias 0.0 \
    --usemask user \
    --savemodel none \
    --restoringbeam common \
    --mask-image "${mask}" \
    --no-preview-pngs \
    >"${run_log}" 2>&1
run_status=$?
set -e

printf 'casars_imager_exit\t%s\n' "${run_status}" >>"${provenance}"
[[ "${run_status}" == "1" ]] ||
    fail "casars-imager exited ${run_status}, expected diagnostic exit 1; preserve ${case_dir}"
[[ -s "${receipt}" ]] ||
    fail "diagnostic exit 1 did not create a non-empty receipt; preserve ${case_dir}"
rg -F 'AWProject literal-coefficient audit stopped before production dispatch' \
    "${run_log}" >/dev/null ||
    fail "completion marker is absent from ${run_log}; preserve ${case_dir}"
[[ -z "$(find "${case_dir}" -maxdepth 1 -name 'rust.*' -print -quit)" ]] ||
    fail "the diagnostic unexpectedly wrote an imaging product; preserve ${case_dir}"

set +e
python3 "${validator}" \
    --candidate "${receipt}" \
    --casa-rs-v4 "${casa_rs_v4_receipt}" \
    --casa-v5 "${casa_v5_receipt}" \
    --arithmetic-v1 "${arithmetic_v1_receipt}" \
    --arithmetic-v1-comparison "${arithmetic_v1_comparison}" \
    --output "${comparison}" \
    >"${comparison_log}" 2>&1
comparison_status=$?
set -e

printf 'receipt_sha256\t%s\n' "$(sha256 "${receipt}")" >>"${provenance}"
printf 'comparison_exit\t%s\n' "${comparison_status}" >>"${provenance}"
[[ "${comparison_status}" == "0" ]] ||
    fail "literal-coefficient evidence was invalid; preserve ${case_dir}"
[[ -s "${comparison}" ]] ||
    fail "validator did not create ${comparison}; preserve ${case_dir}"
classification="$(
    python3 -c \
        'import json,sys; print(json.load(open(sys.argv[1]))["comparison"]["classification"])' \
        "${comparison}"
)"
printf 'comparison_sha256\t%s\n' "$(sha256 "${comparison}")" >>"${provenance}"
printf 'classification\t%s\n' "${classification}" >>"${provenance}"
printf 'completed_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"${provenance}"
require_clean_checkout
require_exact_checkpoint
printf 'result\tvalidly-classified-literal-coefficient-evidence\n' >>"${provenance}"
echo "VLASS casa-rs literal-coefficient evidence classified as ${classification}: ${comparison}"
