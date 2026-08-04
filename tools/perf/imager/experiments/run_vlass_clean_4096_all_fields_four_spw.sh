#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
binary="${CASA_RS_VLASS_EXPERIMENT_BINARY:?set CASA_RS_VLASS_EXPERIMENT_BINARY to the frozen release casars-imager}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-$HOME/.casa/data}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib}"
cf_cache="${CASA_RS_VLASS_CF_CACHE:-$root/cf-cache/6.7.5.9/db96e297401b0f5c90f1494844fd9a1d49ad5023be44987ce7076afac513d856}"
mask="${CASA_RS_VLASS_MASK:-$root/masks/vlass-source-box-4096-spectral.mask}"
run_id="${CASA_RS_VLASS_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-vlass-all63-clean-4096-four-spw}"
run_root="${CASA_RS_VLASS_RUN_ROOT:-$root/recovery-candidates/runs/$run_id}"
selected_exact_hybrid="${CASA_RS_VLASS_SELECTED_EXACT_HYBRID:-0}"
replay_retention_bytes="${CASA_RS_VLASS_REPLAY_RETENTION_BYTES:-0}"
prediction_sidecar_prefix="${CASA_RS_VLASS_PREDICTION_SIDECAR_PREFIX:-}"
prediction_sidecar_source_limit="${CASA_RS_VLASS_PREDICTION_SIDECAR_SOURCE_LIMIT:-}"
prediction_prefix_trace="${CASA_RS_VLASS_PREDICTION_PREFIX_TRACE:-}"
prediction_prefix_source_ordinal="${CASA_RS_VLASS_PREDICTION_PREFIX_SOURCE_ORDINAL:-}"
field_selection="${CASA_RS_VLASS_FIELD_SELECTION:-1107~1127,1512~1532,1542~1562}"
frozen_model_prefix="${CASA_RS_VLASS_FROZEN_MODEL_PREFIX:-}"
frozen_weight_image="${CASA_RS_VLASS_FROZEN_WEIGHT_IMAGE:-}"
ms="$root/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
output="$run_root/rust"
log="$run_root/casa-rs.log"
provenance="$run_root/provenance.txt"
expected_mask_sha256="8490acb911cbbba78f7a20ba4a1d379e227c3a42dfc7eefcc9b7fd5f4139572f"
experimental_environment=(
    CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS=1
    CASA_RS_EXPERIMENTAL_MT_MFS_CASA_FFT0=1
)

case "$selected_exact_hybrid" in
    0)
        ;;
    1)
        case "$replay_retention_bytes" in
            ''|*[!0-9]*)
                echo "CASA_RS_VLASS_REPLAY_RETENTION_BYTES must be a non-negative integer" >&2
                exit 2
                ;;
        esac
        experimental_environment+=(
            CASA_RS_EXPERIMENTAL_AWPROJECT_WINDOWED_HYBRID_CLEAN=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_PREDIVISION_SOURCE_PHASE=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_RAW_FRAME_TAYLOR=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_ONLY_REFRESH=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_RETENTION_BYTES="$replay_retention_bytes"
            CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_CACHE=1
            CASA_RS_EXPERIMENTAL_SPARSE_AWPROJECT_MODEL_PREP=1
            CASA_RS_EXPERIMENTAL_SPARSE_MASK_PEAK_SEARCH=1
            CASA_RS_EXPERIMENTAL_CACHE_REFRESHED_NSIGMA=1
            CASA_RS_EXPERIMENTAL_RADIX_MADFM=1
            CASA_RS_EXPERIMENTAL_PARALLEL_MODEL_TERM_FFT=1
            CASA_RS_EXPERIMENTAL_PARALLEL_RESIDUAL_TERM_FFT=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_LIVE_CFS_ONLY=1
        )
        ;;
    *)
        echo "CASA_RS_VLASS_SELECTED_EXACT_HYBRID must be 0 or 1" >&2
        exit 2
        ;;
esac

if [[ -n "$prediction_sidecar_prefix" && -n "$prediction_prefix_trace" ]]; then
    echo "the prediction sidecar and prediction-prefix trace are mutually exclusive" >&2
    exit 2
fi

if [[ -n "$prediction_sidecar_prefix" || -n "$prediction_prefix_trace" ]]; then
    if [[ "$selected_exact_hybrid" != "1" ]]; then
        echo "all-field prediction diagnostics require the selected exact hybrid candidate" >&2
        exit 2
    fi
fi

if [[ -n "$prediction_sidecar_prefix" ]]; then
    for suffix in audit.bin results.bin casa-values.bin cf-metadata.bin host.json; do
        if [[ -e "${prediction_sidecar_prefix}.${suffix}" ]]; then
            echo "refusing to overwrite prediction sidecar ${prediction_sidecar_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_SIDECAR_PREFIX="$prediction_sidecar_prefix"
    )
    if [[ -n "$prediction_sidecar_source_limit" ]]; then
        experimental_environment+=(
            CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_SIDECAR_SOURCE_LIMIT="$prediction_sidecar_source_limit"
        )
    fi
fi

if [[ -n "$prediction_prefix_trace" ]]; then
    case "$prediction_prefix_source_ordinal" in
        ''|*[!0-9]*)
            echo "CASA_RS_VLASS_PREDICTION_PREFIX_SOURCE_ORDINAL must be a non-negative integer" >&2
            exit 2
            ;;
    esac
    if [[ -e "$prediction_prefix_trace" ]]; then
        echo "refusing to overwrite prediction-prefix trace $prediction_prefix_trace" >&2
        exit 2
    fi
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_PREFIX_TRACE="$prediction_prefix_trace"
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_PREFIX_SOURCE_ORDINAL="$prediction_prefix_source_ordinal"
    )
fi

if [[ -n "$prediction_sidecar_prefix" || -n "$prediction_prefix_trace" ]]; then
    if [[ -z "$frozen_model_prefix" ]]; then
        echo "prediction diagnostics require CASA_RS_VLASS_FROZEN_MODEL_PREFIX" >&2
        exit 2
    fi
    if [[ -z "$frozen_weight_image" ]]; then
        frozen_weight_image="${frozen_model_prefix}.weight.tt0"
    fi
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_MODEL_PREFIX="$frozen_model_prefix"
        CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_WEIGHT_IMAGE="$frozen_weight_image"
    )
fi

for required in "$binary" "$measures_dir" "$fftw_dir" "$cf_cache" "$mask" "$ms"; do
    if [[ ! -e "$required" ]]; then
        echo "required matched-row input does not exist: $required" >&2
        exit 2
    fi
done
if [[ -n "$frozen_model_prefix" ]]; then
    for required in \
        "${frozen_model_prefix}.model.tt0" \
        "${frozen_model_prefix}.model.tt1" \
        "$frozen_weight_image"; do
        if [[ ! -e "$required" ]]; then
            echo "required frozen-model input does not exist: $required" >&2
            exit 2
        fi
    done
fi
mask_sha256="$(
    PYTHONPATH="$repo_root/tools/perf/imager" python3 -c \
        'import pathlib,sys; from perf_harness.tree_identity import tree_identity; print(tree_identity(pathlib.Path(sys.argv[1]), excluded_names={"table.lock"})["tree_sha256"])' \
        "$mask"
)"
if [[ "$mask_sha256" != "$expected_mask_sha256" ]]; then
    echo "mask identity mismatch: expected $expected_mask_sha256, got $mask_sha256" >&2
    exit 2
fi
if [[ "$selected_exact_hybrid" == "1" ]] &&
    ! /usr/bin/strings "$binary" |
        /usr/bin/grep -a 'CASA_RS_EXPERIMENTAL_AWPROJECT_WINDOWED_HYBRID_CLEAN' >/dev/null; then
    echo "selected release binary does not contain the windowed hybrid clean path" >&2
    exit 2
fi
if [[ -e "$run_root" ]]; then
    echo "refusing to overwrite matched-row run root: $run_root" >&2
    exit 2
fi

mkdir -p "$run_root"
{
    printf 'run_id\t%s\n' "$run_id"
    printf 'binary\t%s\n' "$binary"
    printf 'binary_sha256\t'
    /usr/bin/shasum -a 256 "$binary" | /usr/bin/awk '{print $1}'
    printf 'ms\t%s\n' "$ms"
    printf 'cf_cache\t%s\n' "$cf_cache"
    printf 'mask\t%s\n' "$mask"
    printf 'mask_sha256\t%s\n' "$mask_sha256"
    printf 'field\t%s\n' "$field_selection"
    printf 'phasecenter_field\t%s\n' '1525'
    printf 'spw\t%s\n' '2,7,12,17'
    printf 'imsize\t%s\n' '4096'
    printf 'niter\t%s\n' '2000'
    printf 'mtmfs_rhs\t%s\n' 'mask-sparse-full-fft-sampled'
    printf 'mtmfs_basis\t%s\n' 'casa-fft0-f32'
    printf 'selected_exact_hybrid\t%s\n' "$selected_exact_hybrid"
    if [[ "$selected_exact_hybrid" == "1" ]]; then
        printf 'prediction_arithmetic\t%s\n' 'casa-wide-division-source-phase-raw-frame-taylor'
        printf 'replay_retention_bytes\t%s\n' "$replay_retention_bytes"
        printf 'acceleration_stack\t%s\n' 'bounded-windowed-metal-hybrid-image-response-sparse-radix'
    fi
    if [[ -n "$prediction_sidecar_prefix" ]]; then
        printf 'prediction_sidecar_prefix\t%s\n' "$prediction_sidecar_prefix"
        printf 'prediction_sidecar_source_limit\t%s\n' "$prediction_sidecar_source_limit"
        printf 'frozen_model_prefix\t%s\n' "$frozen_model_prefix"
        printf 'frozen_weight_image\t%s\n' "$frozen_weight_image"
        printf 'prediction_sidecar_role\t%s\n' 'bounded-correctness-diagnostic-not-performance-evidence'
    fi
    if [[ -n "$prediction_prefix_trace" ]]; then
        printf 'prediction_prefix_trace\t%s\n' "$prediction_prefix_trace"
        printf 'prediction_prefix_source_ordinal\t%s\n' "$prediction_prefix_source_ordinal"
        printf 'frozen_model_prefix\t%s\n' "$frozen_model_prefix"
        printf 'frozen_weight_image\t%s\n' "$frozen_weight_image"
        printf 'prediction_prefix_role\t%s\n' 'bounded-correctness-diagnostic-not-performance-evidence'
    fi
    printf 'execution\t%s\n' 'release-metal-grid2-casa-fftw310-f64-t8-memory-auto-16GiB'
} >"$provenance"

set +e
/usr/bin/time -lp env -i \
    PATH=/opt/homebrew/bin:/usr/bin:/bin \
    HOME="$HOME" \
    CASA_RS_MEASURESPATH="$measures_dir" \
    CASA_RS_FFTW_LIBRARY_DIR="$fftw_dir" \
    DYLD_LIBRARY_PATH="$fftw_dir" \
    CASA_RS_VLASS_EXPERIMENT_RUNNER=1 \
    CASA_RS_FFTW_THREADS=8 \
    CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1 \
    "${experimental_environment[@]}" \
    "$binary" \
    --ms "$ms" \
    --imagename "$output" \
    --imsize 4096 \
    --cell-arcsec 0.6 \
    --field "$field_selection" \
    --phasecenter-field 1525 \
    --spw 2,7,12,17 \
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
    --standard-mfs-acceleration metal \
    --imaging-fft-precision f64 \
    --imaging-fft-backend fftw \
    --parallel \
    --standard-mfs-grid-threads 2 \
    --imaging-memory-target-mb 16384 \
    --imaging-memory-pressure-policy auto \
    --imaging-prepare-workers 1 \
    --imaging-read-ahead-blocks 1 \
    --hogbom-iteration-mode strict \
    --nterms 2 \
    --scales 0,5,12 \
    --niter 2000 \
    --gain 0.1 \
    --threshold-jy 0.0 \
    --nsigma 5.0 \
    --psfcutoff 0.35 \
    --pblimit 0.0001 \
    --write-pb \
    --minor-cycle-length 2000 \
    --cyclefactor 3.0 \
    --minpsffraction 0.05 \
    --maxpsffraction 0.8 \
    --wterm wproject \
    --wprojplanes 32 \
    --cfcache "$cf_cache" \
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
    --mask-image "$mask" \
    --savemodel none \
    --restoringbeam common \
    --no-preview-pngs \
    >"$log" 2>&1
status=$?
set -e

printf 'exit_status\t%s\n' "$status" >>"$provenance"
printf 'log_sha256\t' >>"$provenance"
/usr/bin/shasum -a 256 "$log" | /usr/bin/awk '{print $1}' >>"$provenance"
if [[ -n "$prediction_sidecar_prefix" ]]; then
    if [[ "$status" != "1" ]]; then
        echo "prediction sidecar exited $status, expected fail-closed status 1" >&2
        exit 2
    fi
for suffix in audit.bin results.bin casa-values.bin cf-metadata.bin host.json; do
        if [[ ! -s "${prediction_sidecar_prefix}.${suffix}" ]]; then
            echo "prediction sidecar did not create ${prediction_sidecar_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    if ! /usr/bin/grep -a \
        'prediction sidecar completed .*before residual gridding' "$log" >/dev/null; then
        echo "prediction sidecar completion marker is absent from $log" >&2
        exit 2
    fi
    exit 0
fi
if [[ -n "$prediction_prefix_trace" ]]; then
    if [[ "$status" != "1" ]]; then
        echo "prediction-prefix trace exited $status, expected fail-closed status 1" >&2
        exit 2
    fi
    if [[ ! -s "$prediction_prefix_trace" ]]; then
        echo "prediction-prefix trace did not create $prediction_prefix_trace" >&2
        exit 2
    fi
    if ! /usr/bin/grep -a \
        'prediction-prefix trace completed before Metal prediction or residual gridding' \
        "$log" >/dev/null; then
        echo "prediction-prefix completion marker is absent from $log" >&2
        exit 2
    fi
    exit 0
fi
printf '%s\n' "$run_root"
exit "$status"
