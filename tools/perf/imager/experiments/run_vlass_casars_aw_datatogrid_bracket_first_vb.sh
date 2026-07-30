#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
expected_head="${1:-${CASA_RS_VLASS_EXPECTED_HEAD:-}}"
branch="codex/vlass-w1-evidence-fiducials"
case_dir="${external_root}/artifacts/experiments/casa-rs-aw-datagrid-bracket-4096-full16-first-vb-v3"
receipt="${case_dir}/receipt.json"
run_log="${case_dir}/casars-imager.log"
comparison_log="${case_dir}/comparison.log"
provenance="${case_dir}/provenance.tsv"
image_prefix="${case_dir}/rust"
ms="${external_root}/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
tclean_last="${external_root}/data/frozen-clean-b80d5e87487a/tclean.last"
cf_cache="${external_root}/cf-cache/6.7.5.18/single-field-4096-full-16-spw"
mask="${external_root}/masks/vlass-single-field-peak-box-4096.mask"
casa_receipt="${external_root}/artifacts/experiments/casa-aw-datagrid-bracket-4096-full16-one-block-v5/receipt.json"
source_archive="${CASA_RS_VLASS_SOURCE_ARCHIVE:-/Volumes/GLENDENNING/vlass_test.tgz}"
dataset_geometry="${repo_root}/tools/perf/imager/recipes/vlass-fragment-dataset-geometry.json"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-${HOME}/.casa/data}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
timeout_command="/opt/homebrew/bin/timeout"
binary="${repo_root}/target/release/casars-imager"
casa_receipt_sha="fe3d5ba3bff1ba925f63f0f088df602692655131c86d6319210ffa90e067ea1f"
tclean_last_sha="a64e6213d66436fee6d602eb5bbda3ac8667b8df2491ea7310557748bbbf15b5"
source_archive_sha="b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a"
ms_tree_sha="037db124913cdf66de670698536f1bb38c9dbac3725a561fd79eee8bb055fd91"
dataset_receipt_sha="ba6fe4482b89297da3cb1d2856a2d47037e767f016d7c63efa7a186ec7c89628"
dataset_geometry_sha="28b1350f2754e4439a0ac94480eb4efb054ecf03f221c805e98cf34c6b5f77f1"

fail() {
    echo "VLASS casa-rs first-VB bracket: $*" >&2
    exit 1
}

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

require_clean_checkout() {
    [[ -z "$(git -C "${repo_root}" status --porcelain)" ]] ||
        fail "worktree is not clean"
}

[[ "$(uname -s)" == "Darwin" ]] || fail "the bracket is macOS-only"
[[ "${expected_head}" =~ ^[0-9a-f]{40}$ ]] ||
    fail "pass the exact 40-hex checkpoint commit as argument 1 or CASA_RS_VLASS_EXPECTED_HEAD"
[[ "$(git -C "${repo_root}" branch --show-current)" == "${branch}" ]] ||
    fail "expected branch ${branch}"
[[ "$(git -C "${repo_root}" rev-parse HEAD)" == "${expected_head}" ]] ||
    fail "local HEAD does not match the requested checkpoint"
[[ "$(git -C "${repo_root}" rev-parse "refs/remotes/origin/${branch}")" == "${expected_head}" ]] ||
    fail "origin/${branch} does not match the requested checkpoint; fetch or push before running"
require_clean_checkout

[[ ! -e "${case_dir}" ]] || fail "refusing to overwrite ${case_dir}"
for required_path in \
    "${ms}" \
    "${tclean_last}" \
    "${cf_cache}" \
    "${mask}" \
    "${casa_receipt}" \
    "${source_archive}" \
    "${dataset_geometry}" \
    "${measures_dir}" \
    "${fftw_dir}"; do
    [[ -e "${required_path}" ]] || fail "missing required input ${required_path}"
done
[[ -x "${timeout_command}" ]] || fail "missing GNU timeout at ${timeout_command}"
[[ "$(sha256 "${casa_receipt}")" == "${casa_receipt_sha}" ]] ||
    fail "frozen CASA v5 receipt SHA-256 changed"
[[ "$(sha256 "${tclean_last}")" == "${tclean_last_sha}" ]] ||
    fail "frozen tclean.last SHA-256 changed"
[[ "$(sha256 "${source_archive}")" == "${source_archive_sha}" ]] ||
    fail "frozen VLASS source archive SHA-256 changed"
[[ "$(sha256 "${dataset_geometry}")" == "${dataset_geometry_sha}" ]] ||
    fail "frozen VLASS dataset-geometry receipt SHA-256 changed"
python3 -c \
    'import json,sys; r=json.load(open(sys.argv[1])); d=r["dataset"]; assert d["archive_sha256"] == sys.argv[2]; assert d["tree_sha256"] == sys.argv[3]; assert r["source_receipts"]["dataset_receipt_sha256"] == sys.argv[4]; assert r["selections"]["single_field"]["selected_rows"] == 10400' \
    "${dataset_geometry}" "${source_archive_sha}" "${ms_tree_sha}" "${dataset_receipt_sha}"
[[ "$(find "${cf_cache}" -maxdepth 1 -type d -name 'CFS*' | wc -l | tr -d ' ')" == "1024" ]] ||
    fail "CF cache does not contain exactly 1024 CFS entries"
[[ "$(find "${cf_cache}" -maxdepth 1 -type d -name 'WTCFS*' | wc -l | tr -d ' ')" == "1024" ]] ||
    fail "CF cache does not contain exactly 1024 WTCFS entries"

output_parent="$(dirname "${case_dir}")"
[[ "$(stat -f '%d' "${ms}")" == "$(stat -f '%d' "${cf_cache}")" ]] ||
    fail "MeasurementSet and CF cache are on different devices"
[[ "$(stat -f '%d' "${ms}")" == "$(stat -f '%d' "${output_parent}")" ]] ||
    fail "inputs and output parent are on different devices"

(
    cd "${repo_root}"
    CARGO_INCREMENTAL=0 cargo build --locked --release -p casars-imager --bin casars-imager
)
require_clean_checkout
[[ "$(git -C "${repo_root}" rev-parse HEAD)" == "${expected_head}" ]] ||
    fail "HEAD changed during the release build"
[[ -x "${binary}" ]] || fail "release build did not create ${binary}"

mkdir "${case_dir}"
{
    printf 'schema\tcasa-rs-vlass-aw-datagrid-bracket-provenance-v3\n'
    printf 'started_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'branch\t%s\n' "${branch}"
    printf 'revision\t%s\n' "${expected_head}"
    printf 'binary_sha256\t%s\n' "$(sha256 "${binary}")"
    printf 'launcher_sha256\t%s\n' "$(sha256 "${BASH_SOURCE[0]}")"
    printf 'casa_receipt_sha256\t%s\n' "${casa_receipt_sha}"
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
    CASA_RS_AW_BRACKET_OUTPUT="${receipt}" \
    CASA_RS_AW_BRACKET_EXPECT_NXY=4096 \
    CASA_RS_AW_BRACKET_BLOCKS=1 \
    CASA_RS_AW_BRACKET_TERMS=2 \
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
rg -F \
    'AWProject DataToGrid bracket captured the first observed-selection TT0/TT1 complete source block' \
    "${run_log}" >/dev/null ||
    fail "completion marker is absent from ${run_log}; preserve ${case_dir}"
[[ -z "$(find "${case_dir}" -maxdepth 1 -name 'rust.*' -print -quit)" ]] ||
    fail "the bracket unexpectedly wrote an imaging product; preserve ${case_dir}"

python3 -c \
    'import json,sys; r=json.load(open(sys.argv[1])); assert r["schema"] == "casa-rs-aw-datagrid-bracket-envelope-v3"; e=r["evidence"]; assert e["schema"] == "casa-rs-aw-datagrid-bracket-v3"; assert e["status"] == "completed-before-finalize"; assert e["reason"] == "observed-selection-grid-and-production-sumwt-boundary"; assert e["formed_image"] is False; assert e["normalization"] == "not-entered"; assert e["fft"] == "not-entered"; assert e["products"] == "not-entered"; assert e["completed_calls"] == 2; assert e["completed_blocks"] == 1; assert e["last_window_in_replay_block"] is True; assert e["observed_first_buffer"]["row_ids_count"] == 325; assert e["absolute_main_rows"]["first"] == 353600; assert e["absolute_main_rows"]["last"] == 353924; assert len(e["block_boundaries"]) == 1; assert len(e["block_boundaries"][0]["terms"]) == 2' \
    "${receipt}"

set +e
(
    cd "${repo_root}"
    CASA_RS_VLASS_AW_DATATOGRID_BRACKET_RECEIPT="${receipt}" \
        CASA_VLASS_AW_DATATOGRID_BRACKET_RECEIPT="${casa_receipt}" \
        CARGO_INCREMENTAL=0 \
        RUST_TEST_THREADS=1 \
        cargo test -p casa-imaging --lib \
        vlass_aw_datatogrid_bracket_stops_before_fft_with_content_addressed_hashes \
        -- --ignored --nocapture
) >"${comparison_log}" 2>&1
comparison_status=$?
set -e

printf 'receipt_sha256\t%s\n' "$(sha256 "${receipt}")" >>"${provenance}"
printf 'comparison_exit\t%s\n' "${comparison_status}" >>"${provenance}"
printf 'completed_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"${provenance}"
[[ "${comparison_status}" == "0" ]] ||
    fail "frozen CASA comparison failed; preserve ${case_dir} and inspect ${comparison_log}"
require_clean_checkout
printf 'result\texact-grid-and-production-sumwt-match\n' >>"${provenance}"
echo "VLASS casa-rs first-buffer bracket exactly matched the frozen CASA v5 observed selection/map/frequency, grid, and production sumwt boundaries: ${receipt}"
