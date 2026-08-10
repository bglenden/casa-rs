#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
receipt_date="${CASA_RS_VLASS_RECEIPT_DATE:-$(date -u +%Y%m%d)}"
binary="${CASA_RS_VLASS_EXPERIMENT_BINARY:-$repo_root/target/release/casars-imager}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
fftw_threads="${CASA_RS_VLASS_FFTW_THREADS:-1}"
model_fft_threads="${CASA_RS_VLASS_MODEL_FFT_THREADS:-8}"
memory_pressure_policy="${CASA_RS_VLASS_MEMORY_PRESSURE_POLICY:-auto}"
grid_threads="${CASA_RS_VLASS_GRID_THREADS:-2}"
plan_threads="${CASA_RS_VLASS_AW_PLAN_THREADS:-1}"
pack_threads="${CASA_RS_VLASS_AW_PACK_THREADS:-1}"
standard_mfs_acceleration="${CASA_RS_VLASS_STANDARD_MFS_ACCELERATION:-metal}"
imaging_fft_precision="${CASA_RS_VLASS_IMAGING_FFT_PRECISION:-f64}"
cf_resident_mb="${CASA_RS_VLASS_CF_RESIDENT_MB:-256}"
replay_retention_bytes="${CASA_RS_VLASS_REPLAY_RETENTION_BYTES:-4294967296}"
replay_compact_programs="${CASA_RS_VLASS_REPLAY_COMPACT_PROGRAMS:-0}"
prime_replay_initial_dirty="${CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY:-0}"
tapless_phase="${CASA_RS_VLASS_TAPLESS_PHASE:-0}"
tapless_phase_census="${CASA_RS_VLASS_TAPLESS_PHASE_CENSUS:-0}"
niter="${CASA_RS_VLASS_NITER:-2000}"
image_response_cache="${CASA_RS_VLASS_IMAGE_RESPONSE_CACHE:-0}"
image_response_dyadic_tiles="${CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_TILES:-0}"
radix_madfm="${CASA_RS_VLASS_RADIX_MADFM:-0}"
mtmfs_sparse_rhs_fft_seed="${CASA_RS_VLASS_MT_MFS_SPARSE_RHS_FFT_SEED:-0}"
mtmfs_full_fft_basis="${CASA_RS_VLASS_MT_MFS_FULL_FFT_BASIS:-0}"
mtmfs_casa_fft0="${CASA_RS_VLASS_MT_MFS_CASA_FFT0:-0}"
mtmfs_casa_fft0_threads="${CASA_RS_VLASS_MT_MFS_CASA_FFT0_THREADS:-$fftw_threads}"
mtmfs_force_unit_scale_fft="${CASA_RS_VLASS_MT_MFS_FORCE_UNIT_SCALE_FFT:-0}"
frozen_model_prefix="${CASA_RS_VLASS_FROZEN_MODEL_PREFIX:-}"
frozen_weight_image="${CASA_RS_VLASS_FROZEN_WEIGHT_IMAGE:-}"
prediction_trace_limit="${CASA_RS_VLASS_PREDICTION_TRACE_LIMIT:-0}"
prediction_trace_stride="${CASA_RS_VLASS_PREDICTION_TRACE_STRIDE:-1}"
cpp_complex="${CASA_RS_VLASS_CPP_COMPLEX:-0}"
usepointing="${CASA_RS_VLASS_USEPOINTING:-1}"
source_major_full_compensation="${CASA_RS_VLASS_SOURCE_MAJOR_FULL_COMPENSATION:-0}"
pointing_trace="${CASA_RS_VLASS_POINTING_TRACE:-0}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-$HOME/.casa/data}"
ms="$root/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
cf_cache="$root/cf-cache/6.7.5.18/single-field-4096-full-16-spw"
mask="$root/masks/vlass-single-field-peak-box-4096.mask"
experimental_environment=(CASA_RS_VLASS_EXPERIMENT_RUNNER=1)
parallel_argument=(--no-parallel)
pointing_argument=(--usepointing)
label="vlass-production-clean-4096-full-16-spw-fftw-t${fftw_threads}-gridt${grid_threads}-niter${niter}"

if [[ "$source_major_full_compensation" == "1" ]]; then
    label="${label}-source-major-full-compensation"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_SOURCE_MAJOR_FULL_COMPENSATION=1
    )
elif [[ "$source_major_full_compensation" != "0" ]]; then
    echo "CASA_RS_VLASS_SOURCE_MAJOR_FULL_COMPENSATION must be 0 or 1" >&2
    exit 2
fi

if [[ "$image_response_cache" == "1" ]]; then
    label="${label}-image-response-cache-promoted-stack"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_CACHE=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_ONLY_REFRESH=1
        CASA_RS_AWPROJECT_MODEL_FFT_THREADS_EXPERIMENT="$model_fft_threads"
        CASA_RS_EXPERIMENTAL_PARALLEL_MODEL_TERM_FFT=1
        CASA_RS_EXPERIMENTAL_SPARSE_AWPROJECT_MODEL_PREP=1
        CASA_RS_EXPERIMENTAL_PARALLEL_RESIDUAL_TERM_FFT=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_RESIDENT_TILE_CHAIN=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GPU_RESIDUAL_REPLAY=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_TILE_SIDE=16
        CASA_RS_EXPERIMENTAL_CACHE_REFRESHED_NSIGMA=1
        CASA_RS_EXPERIMENTAL_SPARSE_MASK_PEAK_SEARCH=1
    )
elif [[ "$image_response_cache" != "0" ]]; then
    echo "CASA_RS_VLASS_IMAGE_RESPONSE_CACHE must be 0 or 1" >&2
    exit 2
fi
if [[ "$replay_compact_programs" == "1" ]]; then
    if [[ "$image_response_cache" != "1" ]]; then
        echo "CASA_RS_VLASS_REPLAY_COMPACT_PROGRAMS requires CASA_RS_VLASS_IMAGE_RESPONSE_CACHE=1" >&2
        exit 2
    fi
    label="${label}-compact-replay-programs"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_RESIDENT_PROGRAM_COMPACTION=1
    )
elif [[ "$replay_compact_programs" != "0" ]]; then
    echo "CASA_RS_VLASS_REPLAY_COMPACT_PROGRAMS must be 0 or 1" >&2
    exit 2
fi
if [[ "$prime_replay_initial_dirty" == "1" ]]; then
    if [[ "$replay_compact_programs" != "1" ]]; then
        echo "CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY requires CASA_RS_VLASS_REPLAY_COMPACT_PROGRAMS=1" >&2
        exit 2
    fi
    label="${label}-prime-replay-initial-dirty"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PRIME_REPLAY_INITIAL_DIRTY=1
    )
elif [[ "$prime_replay_initial_dirty" != "0" ]]; then
    echo "CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY must be 0 or 1" >&2
    exit 2
fi
if [[ "$tapless_phase" == "1" ]]; then
    label="${label}-tapless-phase-atlas"
    experimental_environment+=(
        CASA_RS_AWPROJECT_TAPLESS_PHASE_EXPERIMENT=1
    )
elif [[ "$tapless_phase" != "0" ]]; then
    echo "CASA_RS_VLASS_TAPLESS_PHASE must be 0 or 1" >&2
    exit 2
fi
if [[ "$tapless_phase_census" == "1" ]]; then
    label="${label}-tapless-phase-census"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_TAPLESS_PHASE_CENSUS=1
    )
elif [[ "$tapless_phase_census" != "0" ]]; then
    echo "CASA_RS_VLASS_TAPLESS_PHASE_CENSUS must be 0 or 1" >&2
    exit 2
fi
if [[ "$image_response_dyadic_tiles" == "1" ]]; then
    if [[ "$image_response_cache" != "1" ]]; then
        echo "CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_TILES requires CASA_RS_VLASS_IMAGE_RESPONSE_CACHE=1" >&2
        exit 2
    fi
    label="${label}-dyadic-tiles"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_DYADIC_TILES=1
    )
elif [[ "$image_response_dyadic_tiles" != "0" ]]; then
    echo "CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_TILES must be 0 or 1" >&2
    exit 2
fi
if [[ "$radix_madfm" == "1" ]]; then
    label="${label}-radix-madfm"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_RADIX_MADFM=1)
elif [[ "$radix_madfm" != "0" ]]; then
    echo "CASA_RS_VLASS_RADIX_MADFM must be 0 or 1" >&2
    exit 2
fi
if [[ "$mtmfs_sparse_rhs_fft_seed" == "1" ]]; then
    label="${label}-mtmfs-sparse-rhs-fft-seed"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS_FFT_SEED=1)
elif [[ "$mtmfs_sparse_rhs_fft_seed" != "0" ]]; then
    echo "CASA_RS_VLASS_MT_MFS_SPARSE_RHS_FFT_SEED must be 0 or 1" >&2
    exit 2
fi
if [[ "$mtmfs_full_fft_basis" == "1" ]]; then
    label="${label}-mtmfs-full-fft-basis"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_MT_MFS_FULL_FFT_BASIS=1)
elif [[ "$mtmfs_full_fft_basis" != "0" ]]; then
    echo "CASA_RS_VLASS_MT_MFS_FULL_FFT_BASIS must be 0 or 1" >&2
    exit 2
fi
if [[ "$mtmfs_casa_fft0" == "1" ]]; then
    case "$mtmfs_casa_fft0_threads" in
        ''|*[!0-9]*|0)
            echo "CASA_RS_VLASS_MT_MFS_CASA_FFT0_THREADS must be a positive integer" >&2
            exit 2
            ;;
    esac
    label="${label}-mtmfs-casa-fft0-t${mtmfs_casa_fft0_threads}"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_MT_MFS_CASA_FFT0=1
        CASA_RS_EXPERIMENTAL_MT_MFS_CASA_FFT0_THREADS="$mtmfs_casa_fft0_threads"
    )
elif [[ "$mtmfs_casa_fft0" != "0" ]]; then
    echo "CASA_RS_VLASS_MT_MFS_CASA_FFT0 must be 0 or 1" >&2
    exit 2
fi
if [[ "$mtmfs_force_unit_scale_fft" == "1" ]]; then
    label="${label}-mtmfs-force-unit-scale-fft"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_MT_MFS_FORCE_UNIT_SCALE_FFT=1)
elif [[ "$mtmfs_force_unit_scale_fft" != "0" ]]; then
    echo "CASA_RS_VLASS_MT_MFS_FORCE_UNIT_SCALE_FFT must be 0 or 1" >&2
    exit 2
fi
if [[ -n "$frozen_model_prefix" ]]; then
    for term in 0 1; do
        if [[ ! -d "${frozen_model_prefix}.model.tt${term}" ]]; then
            echo "frozen MT-MFS model term does not exist: ${frozen_model_prefix}.model.tt${term}" >&2
            exit 2
        fi
    done
    label="${label}-frozen-model-refresh"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_MODEL_PREFIX="$frozen_model_prefix"
    )
fi
if [[ -n "$frozen_weight_image" ]]; then
    if [[ -z "$frozen_model_prefix" ]]; then
        echo "CASA_RS_VLASS_FROZEN_WEIGHT_IMAGE requires CASA_RS_VLASS_FROZEN_MODEL_PREFIX" >&2
        exit 2
    fi
    if [[ ! -d "$frozen_weight_image" ]]; then
        echo "frozen AWProject prediction weight image does not exist: $frozen_weight_image" >&2
        exit 2
    fi
    label="${label}-frozen-weight"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_WEIGHT_IMAGE="$frozen_weight_image"
    )
fi
case "$prediction_trace_limit" in
    ''|*[!0-9]*)
        echo "CASA_RS_VLASS_PREDICTION_TRACE_LIMIT must be a non-negative integer" >&2
        exit 2
        ;;
esac
case "$prediction_trace_stride" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_PREDICTION_TRACE_STRIDE must be a positive integer" >&2
        exit 2
        ;;
esac
if [[ "$prediction_trace_limit" != "0" ]]; then
    label="${label}-prediction-trace"
    experimental_environment+=(
        CASA_RS_AWPROJECT_PREDICTION_TRACE_LIMIT="$prediction_trace_limit"
        CASA_RS_AWPROJECT_PREDICTION_TRACE_STRIDE="$prediction_trace_stride"
    )
fi
if [[ "$cpp_complex" == "1" ]]; then
    label="${label}-cpp-complex"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_CPP_COMPLEX=1)
elif [[ "$cpp_complex" != "0" ]]; then
    echo "CASA_RS_VLASS_CPP_COMPLEX must be 0 or 1" >&2
    exit 2
fi
if [[ "$usepointing" == "0" ]]; then
    label="${label}-no-pointing"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_DISABLE_POINTING_PHASE=1
    )
elif [[ "$usepointing" != "1" ]]; then
    echo "CASA_RS_VLASS_USEPOINTING must be 0 or 1" >&2
    exit 2
fi
if [[ "$pointing_trace" == "1" ]]; then
    label="${label}-pointing-trace"
    experimental_environment+=(CASA_RS_AWPROJECT_POINTING_TRACE=1)
elif [[ "$pointing_trace" != "0" ]]; then
    echo "CASA_RS_VLASS_POINTING_TRACE must be 0 or 1" >&2
    exit 2
fi
case "$standard_mfs_acceleration" in
    cpu)
        if [[ "$grid_threads" != "1" ]]; then
            parallel_argument=(--parallel)
        fi
        ;;
    metal)
        label="${label}-accel-metal"
        parallel_argument=(--parallel)
        ;;
    *)
        echo "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION must be cpu or metal" >&2
        exit 2
        ;;
esac
case "$imaging_fft_precision" in
    f32|f64)
        ;;
    *)
        echo "CASA_RS_VLASS_IMAGING_FFT_PRECISION must be f32 or f64" >&2
        exit 2
        ;;
esac
case "$cf_resident_mb" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_CF_RESIDENT_MB must be a positive integer" >&2
        exit 2
        ;;
esac
case "$replay_retention_bytes" in
    ''|*[!0-9]*)
        echo "CASA_RS_VLASS_REPLAY_RETENTION_BYTES must be a non-negative integer" >&2
        exit 2
        ;;
esac
experimental_environment+=(
    CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_RETENTION_BYTES="$replay_retention_bytes"
)
label="${label}-v1"
if [[ -n "${CASA_RS_VLASS_LABEL_OVERRIDE:-}" ]]; then
    case "$CASA_RS_VLASS_LABEL_OVERRIDE" in
        *[!A-Za-z0-9._-]*)
            echo "CASA_RS_VLASS_LABEL_OVERRIDE may contain only letters, digits, dot, underscore, and hyphen" >&2
            exit 2
            ;;
    esac
    label="$CASA_RS_VLASS_LABEL_OVERRIDE"
fi
output="$root/artifacts/products/$label/rust"
log="$root/receipts/runs/${receipt_date}-$label.log"

case "$fftw_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_FFTW_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$model_fft_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_MODEL_FFT_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$memory_pressure_policy" in
    auto|conservative-no-swap|aggressive|oversubscribe|stage-aware|hybrid)
        ;;
    *)
        echo "CASA_RS_VLASS_MEMORY_PRESSURE_POLICY must be auto, conservative-no-swap, aggressive, oversubscribe, stage-aware, or hybrid" >&2
        exit 2
        ;;
esac
case "$grid_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_GRID_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$plan_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_AW_PLAN_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$pack_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_AW_PACK_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
if [[ "$plan_threads" != "1" ]]; then
    label="${label}-awplant${plan_threads}"
    experimental_environment+=(CASA_RS_AWPROJECT_PLAN_THREADS="$plan_threads")
fi
if [[ "$pack_threads" != "1" ]]; then
    label="${label}-awpackt${pack_threads}"
    experimental_environment+=(CASA_RS_AWPROJECT_PACK_THREADS="$pack_threads")
fi
case "$niter" in
    ''|*[!0-9]*)
        echo "CASA_RS_VLASS_NITER must be a non-negative integer" >&2
        exit 2
        ;;
esac
for required in "$binary" "$fftw_dir" "$measures_dir" "$ms" "$cf_cache" "$mask"; do
    if [[ ! -e "$required" ]]; then
        echo "required full-16-SPW promotion input does not exist: $required" >&2
        exit 2
    fi
done
if [[ -e "$output.image.tt0" || -e "$log" ]]; then
    echo "refusing to overwrite existing full-16-SPW promotion products" >&2
    exit 2
fi

mkdir -p "$(dirname "$output")" "$(dirname "$log")"
/usr/bin/time -p env -i \
    PATH=/usr/bin:/bin \
    HOME="$HOME" \
    CASA_RS_MEASURESPATH="$measures_dir" \
    CASA_RS_FFTW_LIBRARY_DIR="$fftw_dir" \
    DYLD_LIBRARY_PATH="$fftw_dir" \
    CASA_RS_FFTW_THREADS="$fftw_threads" \
    CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1 \
    CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS=1 \
    "${experimental_environment[@]}" \
    "$binary" \
    --ms "$ms" \
    --imagename "$output" \
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
    --uvrange "<12km" \
    --intent "OBSERVE_TARGET#UNSPECIFIED" \
    "${pointing_argument[@]}" \
    --weighting briggs \
    --robust 1.0 \
    --perchanweightdensity \
    --deconvolver mtmfs \
    --standard-mfs-acceleration "$standard_mfs_acceleration" \
    --imaging-fft-precision "$imaging_fft_precision" \
    --imaging-fft-backend fftw \
    "${parallel_argument[@]}" \
    --standard-mfs-grid-threads "$grid_threads" \
    --imaging-memory-target-mb 16384 \
    --imaging-memory-pressure-policy "$memory_pressure_policy" \
    --imaging-prepare-workers 1 \
    --imaging-read-ahead-blocks 1 \
    --hogbom-iteration-mode strict \
    --nterms 2 \
    --scales 0,5,12 \
    --niter "$niter" \
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
    --cf-resident-mb "$cf_resident_mb" \
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
    --mask-image "$mask" \
    --no-preview-pngs \
    >"$log" 2>&1

echo "$log"
