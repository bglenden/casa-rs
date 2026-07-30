#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

# One-shot launcher for the casa-rs native-component V4 diagnostic.  The
# immutable case directory is reserved only after every source/input preflight
# and the release build have succeeded.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
external_root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
expected_head="${1:-${CASA_RS_VLASS_EXPECTED_HEAD:-}}"
branch="codex/vlass-w1-evidence-fiducials"
case_name="casa-rs-aw-datagrid-native-components-4096-full16-first-vb-v4"
case_dir="${external_root}/artifacts/experiments/${case_name}"
receipt="${case_dir}/receipt.json"
run_log="${case_dir}/casars-imager.log"
comparison="${case_dir}/comparison.json"
comparison_log="${case_dir}/comparison.log"
provenance="${case_dir}/provenance.tsv"
cf_cache_before="${case_dir}/cf-cache-metadata-before.tsv"
cf_cache_after="${case_dir}/cf-cache-metadata-after.tsv"
image_prefix="${case_dir}/rust"

ms="${external_root}/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
tclean_last="${external_root}/data/frozen-clean-b80d5e87487a/tclean.last"
cf_cache="${external_root}/cf-cache/6.7.5.18/single-field-4096-full-16-spw"
mask="${external_root}/masks/vlass-single-field-peak-box-4096.mask"
source_archive="${CASA_RS_VLASS_SOURCE_ARCHIVE:-/Volumes/GLENDENNING/vlass_test.tgz}"
dataset_geometry="${repo_root}/tools/perf/imager/recipes/vlass-fragment-dataset-geometry.json"
casa_native_components="${external_root}/artifacts/experiments/casa-aw-datagrid-native-components-4096-full16-first-vb-v1/receipt.json"
validator="${experiment_dir}/vlass_casars_aw_datatogrid_native_components_validate.py"
tree_identity_module="${repo_root}/tools/perf/imager/perf_harness/tree_identity.py"
cargo_lock="${repo_root}/Cargo.lock"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-${HOME}/.casa/data}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
timeout_command="/opt/homebrew/bin/timeout"
binary="${repo_root}/target/release/casars-imager"

casa_native_components_sha="cc30d5492f6654336f46617a696f9a7fc8da9006df4e5ae9a3c64a6a9f401644"
casa_native_components_evidence_sha="ab762c9a9a479b97338a30a09204a717c9acd0222d912fd4d5983d8da4e42729"
casa_source_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"
casacore_source_commit="25b653f6963a78a1dcfc8e16954081e091a50fbe"
tclean_last_sha="a64e6213d66436fee6d602eb5bbda3ac8667b8df2491ea7310557748bbbf15b5"
source_archive_sha="b80d5e87487ab8ab01faa064c4cd48db6d93446fd0add208c051dd574e0d353a"
ms_tree_sha="037db124913cdf66de670698536f1bb38c9dbac3725a561fd79eee8bb055fd91"
dataset_receipt_sha="ba6fe4482b89297da3cb1d2856a2d47037e767f016d7c63efa7a186ec7c89628"
dataset_geometry_sha="28b1350f2754e4439a0ac94480eb4efb054ecf03f221c805e98cf34c6b5f77f1"
mask_tree_sha="2673e626301031fd85efb863167766e59f34338d4f8e77b1b2709ffaee411126"
cf_cache_tree_sha="f8fd10b133235e04f75f903fde38d68aa446e1892143fb6bf12b82b1e3cfff68"
controlled_stop_marker="AWProject native-components-v4 audit stopped before casa-imaging core/CF/cache/grid dispatch"

fail() {
    echo "VLASS casa-rs native-component V4 audit: $*" >&2
    exit 1
}

sha256() {
    shasum -a 256 "$1" | awk '{print $1}'
}

tree_identity() {
    python3 -c '
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
from perf_harness.tree_identity import tree_identity
identity = tree_identity(Path(sys.argv[2]))
print("{}\t{}\t{}".format(
    identity["tree_sha256"],
    identity["file_count"],
    identity["size_bytes"],
))
' "${repo_root}/tools/perf/imager" "$1"
}

snapshot_cf_cache() {
    python3 -c '
import os
import stat
import sys

root = os.path.realpath(sys.argv[1])
output = sys.argv[2]
metadata = os.lstat(root)
records = [f".\tdirectory\t{metadata.st_size}\t{metadata.st_mtime_ns}"]
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
        fail "tracking origin/${branch} does not match the requested checkpoint"
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
    "${ms}" \
    "${tclean_last}" \
    "${cf_cache}" \
    "${mask}" \
    "${source_archive}" \
    "${dataset_geometry}" \
    "${casa_native_components}" \
    "${validator}" \
    "${tree_identity_module}" \
    "${cargo_lock}" \
    "${measures_dir}" \
    "${fftw_dir}"; do
    [[ -e "${required_path}" ]] || fail "missing required input ${required_path}"
done
[[ -x "${timeout_command}" ]] || fail "missing GNU timeout at ${timeout_command}"
[[ "$(sha256 "${casa_native_components}")" == "${casa_native_components_sha}" ]] ||
    fail "frozen CASA native-component receipt SHA-256 changed"
[[ "$(sha256 "${tclean_last}")" == "${tclean_last_sha}" ]] ||
    fail "frozen tclean.last SHA-256 changed"
[[ "$(sha256 "${source_archive}")" == "${source_archive_sha}" ]] ||
    fail "frozen source archive SHA-256 changed"
[[ "$(sha256 "${dataset_geometry}")" == "${dataset_geometry_sha}" ]] ||
    fail "frozen dataset-geometry SHA-256 changed"

python3 -c '
import json
import sys
receipt = json.load(open(sys.argv[1], encoding="utf-8"))
evidence = receipt["evidence"]
assert receipt["schema"] == "casa-aw-datagrid-native-components-envelope-v1"
assert receipt["content_address"]["digest"] == sys.argv[2]
assert evidence["schema"] == "casa-aw-datagrid-native-components-v1"
assert evidence["result"] == "completed-native-components-exact-frozen-v5"
assert evidence["component_hashes"]["admission"] == 14184653015859831397
assert evidence["counts"]["admitted_channels"] == 12359
assert evidence["recomputed_frozen_hashes"][0]["stream_hash"] == 4740440223154359747
' "${casa_native_components}" "${casa_native_components_evidence_sha}"
python3 -c \
    'import json,sys; r=json.load(open(sys.argv[1])); d=r["dataset"]; assert d["archive_sha256"] == sys.argv[2]; assert d["tree_sha256"] == sys.argv[3]; assert r["source_receipts"]["dataset_receipt_sha256"] == sys.argv[4]; assert r["selections"]["single_field"]["selected_rows"] == 10400' \
    "${dataset_geometry}" "${source_archive_sha}" "${ms_tree_sha}" "${dataset_receipt_sha}"

mask_identity="$(tree_identity "${mask}")"
[[ "${mask_identity%%$'\t'*}" == "${mask_tree_sha}" ]] ||
    fail "mask tree SHA-256 changed"
cf_identity_before="$(tree_identity "${cf_cache}")"
[[ "${cf_identity_before%%$'\t'*}" == "${cf_cache_tree_sha}" ]] ||
    fail "CF-cache content tree SHA-256 changed"
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
    printf 'schema\tcasa-rs-vlass-aw-native-components-provenance-v4\n'
    printf 'case_name\t%s\n' "${case_name}"
    printf 'started_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'branch\t%s\n' "${branch}"
    printf 'revision\t%s\n' "${expected_head}"
    printf 'binary_sha256\t%s\n' "$(sha256 "${binary}")"
    printf 'launcher_sha256\t%s\n' "$(sha256 "${BASH_SOURCE[0]}")"
    printf 'validator_sha256\t%s\n' "$(sha256 "${validator}")"
    printf 'cargo_lock_sha256\t%s\n' "$(sha256 "${cargo_lock}")"
    printf 'num_complex_version\t%s\n' "${num_complex_version}"
    printf 'rustc_vv\t%s\n' "${rustc_vv}"
    printf 'casa_native_components_sha256\t%s\n' "${casa_native_components_sha}"
    printf 'casa_native_components_evidence_sha256\t%s\n' "${casa_native_components_evidence_sha}"
    printf 'casa_source_commit\t%s\n' "${casa_source_commit}"
    printf 'casacore_source_commit\t%s\n' "${casacore_source_commit}"
    printf 'tclean_last_sha256\t%s\n' "${tclean_last_sha}"
    printf 'source_archive_sha256\t%s\n' "${source_archive_sha}"
    printf 'declared_measurement_set_tree_sha256\t%s\n' "${ms_tree_sha}"
    printf 'dataset_receipt_sha256\t%s\n' "${dataset_receipt_sha}"
    printf 'dataset_geometry_sha256\t%s\n' "${dataset_geometry_sha}"
    printf 'mask_tree_sha256\t%s\n' "${mask_tree_sha}"
    printf 'cf_cache_tree_sha256_before\t%s\n' "${cf_cache_tree_sha}"
    printf 'measurement_set\t%s\n' "${ms}"
    printf 'cf_cache\t%s\n' "${cf_cache}"
    printf 'mask\t%s\n' "${mask}"
} >"${provenance}"

cf_before_summary="$(snapshot_cf_cache "${cf_cache_before}")"
printf 'cf_cache_metadata_before_sha256\t%s\n' "$(sha256 "${cf_cache_before}")" >>"${provenance}"
printf 'cf_cache_regular_files_before\t%s\n' "${cf_before_summary%%$'\t'*}" >>"${provenance}"
printf 'cf_cache_regular_bytes_before\t%s\n' "${cf_before_summary#*$'\t'}" >>"${provenance}"

set +e
"${timeout_command}" --signal=TERM --kill-after=15s 300s \
    env -i \
    PATH="/opt/homebrew/bin:/usr/bin:/bin" \
    HOME="${HOME}" \
    CASA_RS_MEASURESPATH="${measures_dir}" \
    CASA_RS_FFTW_LIBRARY_DIR="${fftw_dir}" \
    DYLD_LIBRARY_PATH="${fftw_dir}" \
    CASA_RS_FFTW_THREADS=1 \
    CASA_RS_AW_NATIVE_COMPONENTS_AUDIT_OUTPUT_V4="${receipt}" \
    CASA_RS_AW_NATIVE_COMPONENTS_AUDIT_EXPECT_NXY_V4=4096 \
    CASA_RS_AW_NATIVE_COMPONENTS_AUDIT_BLOCKS_V4=1 \
    CASA_RS_AW_NATIVE_COMPONENTS_AUDIT_TERMS_V4=2 \
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

cf_after_summary="$(snapshot_cf_cache "${cf_cache_after}")"
cf_identity_after="$(tree_identity "${cf_cache}")"
printf 'cf_cache_metadata_after_sha256\t%s\n' "$(sha256 "${cf_cache_after}")" >>"${provenance}"
printf 'cf_cache_regular_files_after\t%s\n' "${cf_after_summary%%$'\t'*}" >>"${provenance}"
printf 'cf_cache_regular_bytes_after\t%s\n' "${cf_after_summary#*$'\t'}" >>"${provenance}"
printf 'cf_cache_tree_sha256_after\t%s\n' "${cf_identity_after%%$'\t'*}" >>"${provenance}"
if cmp -s "${cf_cache_before}" "${cf_cache_after}" &&
    [[ "${cf_identity_after}" == "${cf_identity_before}" ]]; then
    printf 'cf_cache_unchanged\ttrue\n' >>"${provenance}"
else
    printf 'cf_cache_unchanged\tfalse\n' >>"${provenance}"
    fail "shared CF cache changed during the diagnostic; preserve ${case_dir}"
fi

printf 'casars_imager_exit\t%s\n' "${run_status}" >>"${provenance}"
[[ "${run_status}" == "1" ]] ||
    fail "casars-imager exited ${run_status}, expected controlled exit 1; preserve ${case_dir}"
[[ -s "${receipt}" ]] ||
    fail "controlled exit 1 did not create a non-empty receipt; preserve ${case_dir}"
rg -F "${controlled_stop_marker}" "${run_log}" >/dev/null ||
    fail "controlled-stop marker is absent from ${run_log}; preserve ${case_dir}"
[[ -z "$(find "${case_dir}" -maxdepth 1 -name 'rust.*' -print -quit)" ]] ||
    fail "the diagnostic unexpectedly wrote an imaging product; preserve ${case_dir}"

set +e
python3 "${validator}" \
    --candidate "${receipt}" \
    --casa-native-components "${casa_native_components}" \
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
case "${classification}" in
    exact-frozen-casa-native-components | valid-native-component-mismatch) ;;
    *) fail "unexpected validator classification ${classification}; preserve ${case_dir}" ;;
esac
python3 -c '
import json
import sys
comparison = json.load(open(sys.argv[1], encoding="utf-8"))["comparison"]
scope = comparison["scope"]
assert scope["production_dispatch"] == "not-entered"
assert scope["cf_cache"] == "not-opened"
assert scope["grid_storage"] == "not-allocated"
assert scope["grid_dispatch"] == "not-entered"
assert scope["products"] == "not-entered"
assert comparison["row_checkpoints"]["count"] == 325
assert scope["raw_slots_compared"] == 20800
' "${comparison}"
printf 'comparison_sha256\t%s\n' "$(sha256 "${comparison}")" >>"${provenance}"
printf 'classification\t%s\n' "${classification}" >>"${provenance}"
printf 'completed_at_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"${provenance}"
printf 'result\tvalidly-classified-native-components-v4\n' >>"${provenance}"
require_clean_checkout
require_exact_checkpoint
echo "VLASS casa-rs native-component V4 evidence classified as ${classification}: ${comparison}"
