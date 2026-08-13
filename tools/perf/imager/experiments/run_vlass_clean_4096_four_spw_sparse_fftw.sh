#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
receipt_date="${CASA_RS_VLASS_RECEIPT_DATE:-$(date -u +%Y%m%d)}"
binary="${CASA_RS_VLASS_EXPERIMENT_BINARY:-$repo_root/target/release/casars-imager}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
fftw_threads="${CASA_RS_VLASS_FFTW_THREADS:-1}"
grid_threads="${CASA_RS_VLASS_GRID_THREADS:-1}"
plan_threads="${CASA_RS_VLASS_AW_PLAN_THREADS:-1}"
pack_threads="${CASA_RS_VLASS_AW_PACK_THREADS:-1}"
standard_mfs_acceleration="${CASA_RS_VLASS_STANDARD_MFS_ACCELERATION:-cpu}"
niter="${CASA_RS_VLASS_NITER:-6}"
residual_only_refresh="${CASA_RS_VLASS_RESIDUAL_ONLY_REFRESH:-0}"
tapless_phase="${CASA_RS_VLASS_TAPLESS_PHASE:-0}"
replay_compact_programs="${CASA_RS_VLASS_REPLAY_COMPACT_PROGRAMS:-0}"
prime_replay_initial_dirty="${CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY:-0}"
model_sparsity_profile="${CASA_RS_VLASS_MODEL_SPARSITY_PROFILE:-0}"
model_fft_threads="${CASA_RS_VLASS_MODEL_FFT_THREADS:-}"
sparse_model_dft_max_pixels="${CASA_RS_VLASS_SPARSE_MODEL_DFT_MAX_PIXELS:-}"
linear_madfm="${CASA_RS_VLASS_LINEAR_MADFM:-0}"
keyed_madfm="${CASA_RS_VLASS_KEYED_MADFM:-0}"
radix_madfm="${CASA_RS_VLASS_RADIX_MADFM:-0}"
cache_refreshed_nsigma="${CASA_RS_VLASS_CACHE_REFRESHED_NSIGMA:-0}"
sparse_mask_peak_search="${CASA_RS_VLASS_SPARSE_MASK_PEAK_SEARCH:-0}"
parallel_model_term_fft="${CASA_RS_VLASS_PARALLEL_MODEL_TERM_FFT:-0}"
model_fft_timing="${CASA_RS_VLASS_MODEL_FFT_TIMING:-0}"
fftw_f64_timing="${CASA_RS_VLASS_FFTW_F64_TIMING:-0}"
fftw_f64_wisdom="${CASA_RS_VLASS_FFTW_F64_WISDOM:-}"
fftw_f32_wisdom="${CASA_RS_VLASS_FFTW_F32_WISDOM:-}"
sparse_model_prep="${CASA_RS_VLASS_SPARSE_MODEL_PREP:-0}"
parallel_residual_term_fft="${CASA_RS_VLASS_PARALLEL_RESIDUAL_TERM_FFT:-0}"
persistent_metal_pack="${CASA_RS_VLASS_PERSISTENT_METAL_PACK:-0}"
residual_live_cfs_only="${CASA_RS_VLASS_RESIDUAL_LIVE_CFS_ONLY:-0}"
metal_f32_residual_fft="${CASA_RS_VLASS_METAL_F32_RESIDUAL_FFT:-0}"
metal_prediction_probe="${CASA_RS_VLASS_METAL_PREDICTION_PROBE:-0}"
metal_tile_grid_probe="${CASA_RS_VLASS_METAL_TILE_GRID_PROBE:-0}"
metal_resident_chain_probe="${CASA_RS_VLASS_METAL_RESIDENT_CHAIN_PROBE:-0}"
metal_resident_tile_chain="${CASA_RS_VLASS_METAL_RESIDENT_TILE_CHAIN:-0}"
metal_gpu_residual_replay="${CASA_RS_VLASS_METAL_GPU_RESIDUAL_REPLAY:-0}"
metal_global_tile_replay="${CASA_RS_VLASS_METAL_GLOBAL_TILE_REPLAY:-0}"
prediction_grid_census="${CASA_RS_VLASS_PREDICTION_GRID_CENSUS:-0}"
model_delta_census="${CASA_RS_VLASS_MODEL_DELTA_CENSUS:-0}"
incremental_model_probe="${CASA_RS_VLASS_INCREMENTAL_MODEL_PROBE:-0}"
incremental_model_runtime="${CASA_RS_VLASS_INCREMENTAL_MODEL_RUNTIME:-0}"
selected_model_dft="${CASA_RS_VLASS_SELECTED_MODEL_DFT:-0}"
image_response_cache="${CASA_RS_VLASS_IMAGE_RESPONSE_CACHE:-0}"
image_response_dyadic_census="${CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_CENSUS:-0}"
image_response_dyadic_tiles="${CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_TILES:-0}"
incremental_model_max_delta_positions="${CASA_RS_VLASS_INCREMENTAL_MODEL_MAX_DELTA_POSITIONS:-1}"
metal_tile_side="${CASA_RS_VLASS_METAL_TILE_SIDE:-16}"
replay_retention_bytes="${CASA_RS_VLASS_REPLAY_RETENTION_BYTES:-}"
frozen_model_prefix="${CASA_RS_VLASS_FROZEN_MODEL_PREFIX:-}"
frozen_weight_image="${CASA_RS_VLASS_FROZEN_WEIGHT_IMAGE:-}"
frozen_restoring_beam="${CASA_RS_VLASS_FROZEN_RESTORING_BEAM:-}"
frozen_final_state_checkpoints="${CASA_RS_VLASS_FROZEN_FINAL_STATE_CHECKPOINTS:-0}"
prediction_sidecar_prefix="${CASA_RS_VLASS_PREDICTION_SIDECAR_PREFIX:-}"
wide_division_sidecar_prefix="${CASA_RS_VLASS_WIDE_DIVISION_SIDECAR_PREFIX:-}"
hybrid_residual_prefix="${CASA_RS_VLASS_HYBRID_RESIDUAL_PREFIX:-}"
hybrid_clean="${CASA_RS_VLASS_HYBRID_CLEAN:-0}"
predivision_source_phase="${CASA_RS_VLASS_PREDIVISION_SOURCE_PHASE:-0}"
raw_frame_taylor="${CASA_RS_VLASS_RAW_FRAME_TAYLOR:-0}"
prediction_prefix_trace="${CASA_RS_VLASS_PREDICTION_PREFIX_TRACE:-}"
prediction_prefix_source_ordinal="${CASA_RS_VLASS_PREDICTION_PREFIX_SOURCE_ORDINAL:-}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-$HOME/.casa/data}"
ms="$root/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
residual_only_label=""
tapless_phase_label=""
replay_compact_programs_label=""
prime_replay_initial_dirty_label=""
plan_threads_label=""
pack_threads_label=""
model_fft_label=""
sparse_model_dft_label=""
linear_madfm_label=""
keyed_madfm_label=""
radix_madfm_label=""
cache_refreshed_nsigma_label=""
sparse_mask_peak_search_label=""
parallel_model_term_fft_label=""
model_fft_timing_label=""
fftw_f64_timing_label=""
fftw_f64_wisdom_label=""
fftw_f32_wisdom_label=""
sparse_model_prep_label=""
parallel_residual_term_fft_label=""
persistent_metal_pack_label=""
residual_live_cfs_only_label=""
metal_f32_residual_fft_label=""
metal_prediction_probe_label=""
metal_tile_grid_probe_label=""
metal_resident_chain_probe_label=""
metal_resident_tile_chain_label=""
metal_gpu_residual_replay_label=""
metal_global_tile_replay_label=""
prediction_grid_census_label=""
model_delta_census_label=""
incremental_model_probe_label=""
incremental_model_runtime_label=""
selected_model_dft_label=""
image_response_cache_label=""
image_response_dyadic_census_label=""
image_response_dyadic_tiles_label=""
prediction_sidecar_label=""
wide_division_sidecar_label=""
hybrid_residual_label=""
hybrid_clean_label=""
predivision_source_phase_label=""
raw_frame_taylor_label=""
prediction_prefix_trace_label=""
grid_threads_label=""
acceleration_label=""
parallel_label=""
parallel_argument=(--no-parallel)
experimental_environment=(CASA_RS_VLASS_EXPERIMENT_RUNNER=1)
if [[ -n "$frozen_model_prefix" ]]; then
    for term in 0 1; do
        if [[ ! -d "${frozen_model_prefix}.model.tt${term}" ]]; then
            echo "frozen MT-MFS model term does not exist: ${frozen_model_prefix}.model.tt${term}" >&2
            exit 2
        fi
    done
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
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_WEIGHT_IMAGE="$frozen_weight_image"
    )
fi
if [[ -n "$frozen_restoring_beam" ]]; then
    if [[ -z "$frozen_model_prefix" ]]; then
        echo "CASA_RS_VLASS_FROZEN_RESTORING_BEAM requires CASA_RS_VLASS_FROZEN_MODEL_PREFIX" >&2
        exit 2
    fi
    if [[ ! "$frozen_restoring_beam" =~ ^[0-9]+([.][0-9]+)?(e[-+]?[0-9]+)?,[0-9]+([.][0-9]+)?(e[-+]?[0-9]+)?,-?[0-9]+([.][0-9]+)?(e[-+]?[0-9]+)?$ ]]; then
        echo "CASA_RS_VLASS_FROZEN_RESTORING_BEAM must be major_arcsec,minor_arcsec,position_angle_deg" >&2
        exit 2
    fi
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_RESTORING_BEAM="$frozen_restoring_beam"
    )
fi
if [[ "$frozen_final_state_checkpoints" == "1" ]]; then
    if [[ -z "$frozen_model_prefix" || -z "$frozen_restoring_beam" ]]; then
        echo "CASA_RS_VLASS_FROZEN_FINAL_STATE_CHECKPOINTS requires a frozen model and restoring beam" >&2
        exit 2
    fi
    if [[ "$residual_only_refresh" != "1" \
        || "$residual_live_cfs_only" != "1" \
        || "$prime_replay_initial_dirty" != "1" \
        || "$metal_global_tile_replay" != "1" \
        || "$image_response_cache" != "0" ]]; then
        echo "frozen-final-state checkpoints require residual-only live-CF refresh, initial-dirty replay priming, global Metal replay, and no image-response cache" >&2
        exit 2
    fi
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_FROZEN_FINAL_STATE_CHECKPOINTS=1
    )
elif [[ "$frozen_final_state_checkpoints" != "0" ]]; then
    echo "CASA_RS_VLASS_FROZEN_FINAL_STATE_CHECKPOINTS must be 0 or 1" >&2
    exit 2
fi
if [[ -n "$prediction_sidecar_prefix" ]]; then
    if [[ "$frozen_final_state_checkpoints" != "1" ]]; then
        echo "CASA_RS_VLASS_PREDICTION_SIDECAR_PREFIX requires frozen-final-state checkpoints" >&2
        exit 2
    fi
    for suffix in audit.bin results.bin host.json; do
        if [[ -e "${prediction_sidecar_prefix}.${suffix}" ]]; then
            echo "refusing to overwrite prediction-sidecar artifact: ${prediction_sidecar_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    prediction_sidecar_label="-prediction-sidecar"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_SIDECAR_PREFIX="$prediction_sidecar_prefix"
    )
fi
if [[ -n "$wide_division_sidecar_prefix" ]]; then
    if [[ -n "$prediction_sidecar_prefix" ]]; then
        echo "prediction and wide-division sidecars are mutually exclusive" >&2
        exit 2
    fi
    if [[ "$frozen_final_state_checkpoints" != "1" ]]; then
        echo "CASA_RS_VLASS_WIDE_DIVISION_SIDECAR_PREFIX requires frozen-final-state checkpoints" >&2
        exit 2
    fi
    for suffix in \
        raw.bin host.json \
        current.audit.bin current.results.bin current.host.json \
        wide.audit.bin wide.results.bin wide.host.json; do
        if [[ -e "${wide_division_sidecar_prefix}.${suffix}" ]]; then
            echo "refusing to overwrite wide-division sidecar artifact: ${wide_division_sidecar_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    wide_division_sidecar_label="-wide-division-sidecar"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_WIDE_DIVISION_SIDECAR_PREFIX="$wide_division_sidecar_prefix"
    )
fi
if [[ -n "$hybrid_residual_prefix" ]]; then
    if [[ -n "$prediction_sidecar_prefix" || -n "$wide_division_sidecar_prefix" \
        || "$hybrid_clean" == "1" ]]; then
        echo "hybrid residual diagnostic, hybrid clean, and prediction sidecars are mutually exclusive" >&2
        exit 2
    fi
    if [[ "$frozen_final_state_checkpoints" != "1" \
        || "$predivision_source_phase" != "1" \
        || "$raw_frame_taylor" != "1" ]]; then
        echo "CASA_RS_VLASS_HYBRID_RESIDUAL_PREFIX requires frozen-final-state checkpoints, pre-division source phase, and raw-frame Taylor ordering" >&2
        exit 2
    fi
    for suffix in \
        prediction.json normalized.tt0.f32le normalized.tt1.f32le \
        normalized.mask.u8 normalized.json; do
        if [[ -e "${hybrid_residual_prefix}.${suffix}" ]]; then
            echo "refusing to overwrite hybrid residual artifact: ${hybrid_residual_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    hybrid_residual_label="-hybrid-residual"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_RESIDUAL_PREFIX="$hybrid_residual_prefix"
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_EXPECTED_OBSERVED_SHA256=3601b5c6ebf749d58c80bc16b329db68a94557e5d7cbb477034b061ef89f2172
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_EXPECTED_CONTROL_PREDICTION_SHA256=68d6dc8c6b4ec45b8cad8d17ee44cdc1a1220e0ae261c251a35b75899ecb0bf9
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_EXPECTED_CONTROL_RESIDUAL_SHA256=3ab0ed020a6b75ed54aadd91606c7d6e0fc8424575f77f931654e1addb3b6f98
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_EXPECTED_CANDIDATE_PREDICTION_SHA256=2c6a3072a7f5556c81cc5b691a8d0ac2d7b055010bb8f171ed207d5d1a5d1e5d
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_EXPECTED_CANDIDATE_CANONICAL_RESIDUAL_SHA256=4db5487bff286e841718aec4a600f3b5c1ebf3aa602c5120a0796832355ad6d9
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_EXPECTED_CANDIDATE_TILE_RESIDUAL_SHA256=6f48df4cfed851012bbc84b3ceb125a7113cff221f819b770fc49f546781e21f
    )
fi
if [[ "$hybrid_clean" == "1" ]]; then
    if [[ -n "$prediction_sidecar_prefix" || -n "$wide_division_sidecar_prefix" \
        || -n "$hybrid_residual_prefix" || -n "$prediction_prefix_trace" ]]; then
        echo "hybrid clean, prediction diagnostics, and residual diagnostics are mutually exclusive" >&2
        exit 2
    fi
    if [[ "$frozen_final_state_checkpoints" != "0" \
        || -n "$frozen_model_prefix" \
        || -n "$frozen_weight_image" \
        || -n "$frozen_restoring_beam" ]]; then
        echo "CASA_RS_VLASS_HYBRID_CLEAN requires a clean-from-zero workload without frozen state" >&2
        exit 2
    fi
    if [[ "$predivision_source_phase" != "1" \
        || "$raw_frame_taylor" != "1" \
        || "$metal_global_tile_replay" != "1" ]]; then
        echo "CASA_RS_VLASS_HYBRID_CLEAN requires pre-division source phase, raw-frame Taylor ordering, and global Metal replay" >&2
        exit 2
    fi
    hybrid_clean_label="-hybrid-clean"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_CLEAN=1
    )
elif [[ "$hybrid_clean" != "0" ]]; then
    echo "CASA_RS_VLASS_HYBRID_CLEAN must be 0 or 1" >&2
    exit 2
fi
if [[ "$predivision_source_phase" == "1" ]]; then
    if [[ -z "$wide_division_sidecar_prefix" && -z "$hybrid_residual_prefix" \
        && "$hybrid_clean" != "1" ]]; then
        echo "CASA_RS_VLASS_PREDIVISION_SOURCE_PHASE requires a wide-division sidecar, hybrid residual diagnostic, or hybrid clean" >&2
        exit 2
    fi
    predivision_source_phase_label="-predivision-source-phase"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDIVISION_SOURCE_PHASE=1
    )
elif [[ "$predivision_source_phase" != "0" ]]; then
    echo "CASA_RS_VLASS_PREDIVISION_SOURCE_PHASE must be 0 or 1" >&2
    exit 2
fi
if [[ "$raw_frame_taylor" == "1" ]]; then
    if [[ "$predivision_source_phase" != "1" ]]; then
        echo "CASA_RS_VLASS_RAW_FRAME_TAYLOR requires CASA_RS_VLASS_PREDIVISION_SOURCE_PHASE=1" >&2
        exit 2
    fi
    raw_frame_taylor_label="-raw-frame-taylor"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_RAW_FRAME_TAYLOR=1
    )
elif [[ "$raw_frame_taylor" != "0" ]]; then
    echo "CASA_RS_VLASS_RAW_FRAME_TAYLOR must be 0 or 1" >&2
    exit 2
fi
if [[ -n "$prediction_prefix_trace" ]]; then
    if [[ -n "$prediction_sidecar_prefix" \
        || -n "$wide_division_sidecar_prefix" \
        || -n "$hybrid_residual_prefix" \
        || "$hybrid_clean" == "1" ]]; then
        echo "prediction-prefix trace, prediction sidecars, hybrid residual diagnostic, and hybrid clean are mutually exclusive" >&2
        exit 2
    fi
    if [[ "$frozen_final_state_checkpoints" != "1" ]]; then
        echo "CASA_RS_VLASS_PREDICTION_PREFIX_TRACE requires frozen-final-state checkpoints" >&2
        exit 2
    fi
    case "$prediction_prefix_source_ordinal" in
        ''|*[!0-9]*)
            echo "CASA_RS_VLASS_PREDICTION_PREFIX_SOURCE_ORDINAL must be a non-negative integer" >&2
            exit 2
            ;;
    esac
    if [[ -e "$prediction_prefix_trace" ]]; then
        echo "refusing to overwrite prediction-prefix trace: $prediction_prefix_trace" >&2
        exit 2
    fi
    prediction_prefix_trace_label="-prediction-prefix-source${prediction_prefix_source_ordinal}"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_PREFIX_TRACE="$prediction_prefix_trace"
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_PREFIX_SOURCE_ORDINAL="$prediction_prefix_source_ordinal"
    )
fi
if [[ "$tapless_phase" == "1" ]]; then
    tapless_phase_label="-tapless-phase-atlas"
    experimental_environment+=(CASA_RS_AWPROJECT_TAPLESS_PHASE_EXPERIMENT=1)
elif [[ "$tapless_phase" != "0" ]]; then
    echo "CASA_RS_VLASS_TAPLESS_PHASE must be 0 or 1" >&2
    exit 2
fi
if [[ "$replay_compact_programs" == "1" ]]; then
    if [[ "$image_response_cache" != "1" ]]; then
        echo "CASA_RS_VLASS_REPLAY_COMPACT_PROGRAMS requires CASA_RS_VLASS_IMAGE_RESPONSE_CACHE=1" >&2
        exit 2
    fi
    replay_compact_programs_label="-compact-replay-programs"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_RESIDENT_PROGRAM_COMPACTION=1
    )
elif [[ "$replay_compact_programs" != "0" ]]; then
    echo "CASA_RS_VLASS_REPLAY_COMPACT_PROGRAMS must be 0 or 1" >&2
    exit 2
fi
if [[ "$prime_replay_initial_dirty" == "1" ]]; then
    if [[ "$replay_compact_programs" != "1" && "$frozen_final_state_checkpoints" != "1" ]]; then
        echo "CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY requires compact replay programs or the frozen-final-state checkpoint diagnostic" >&2
        exit 2
    fi
    prime_replay_initial_dirty_label="-prime-replay-initial-dirty"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PRIME_REPLAY_INITIAL_DIRTY=1
    )
elif [[ "$prime_replay_initial_dirty" != "0" ]]; then
    echo "CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY must be 0 or 1" >&2
    exit 2
fi
if [[ "$residual_only_refresh" == "1" ]]; then
    residual_only_label="-residual-only"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_ONLY_REFRESH=1)
elif [[ "$residual_only_refresh" != "0" ]]; then
    echo "CASA_RS_VLASS_RESIDUAL_ONLY_REFRESH must be 0 or 1" >&2
    exit 2
fi
if [[ "$linear_madfm" == "1" ]]; then
    linear_madfm_label="-linear-madfm"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_LINEAR_MADFM=1)
elif [[ "$linear_madfm" != "0" ]]; then
    echo "CASA_RS_VLASS_LINEAR_MADFM must be 0 or 1" >&2
    exit 2
fi
if [[ "$keyed_madfm" == "1" ]]; then
    keyed_madfm_label="-keyed-madfm"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_KEYED_MADFM=1)
elif [[ "$keyed_madfm" != "0" ]]; then
    echo "CASA_RS_VLASS_KEYED_MADFM must be 0 or 1" >&2
    exit 2
fi
if [[ "$radix_madfm" == "1" ]]; then
    radix_madfm_label="-radix-madfm"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_RADIX_MADFM=1)
elif [[ "$radix_madfm" != "0" ]]; then
    echo "CASA_RS_VLASS_RADIX_MADFM must be 0 or 1" >&2
    exit 2
fi
if [[ $((linear_madfm + keyed_madfm + radix_madfm)) -gt 1 ]]; then
    echo "CASA_RS_VLASS_LINEAR_MADFM, CASA_RS_VLASS_KEYED_MADFM, and CASA_RS_VLASS_RADIX_MADFM are mutually exclusive" >&2
    exit 2
fi
if [[ "$cache_refreshed_nsigma" == "1" ]]; then
    cache_refreshed_nsigma_label="-cached-nsigma"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_CACHE_REFRESHED_NSIGMA=1)
elif [[ "$cache_refreshed_nsigma" != "0" ]]; then
    echo "CASA_RS_VLASS_CACHE_REFRESHED_NSIGMA must be 0 or 1" >&2
    exit 2
fi
if [[ "$sparse_mask_peak_search" == "1" ]]; then
    sparse_mask_peak_search_label="-sparse-mask-peaks"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_SPARSE_MASK_PEAK_SEARCH=1)
elif [[ "$sparse_mask_peak_search" != "0" ]]; then
    echo "CASA_RS_VLASS_SPARSE_MASK_PEAK_SEARCH must be 0 or 1" >&2
    exit 2
fi
if [[ "$parallel_model_term_fft" == "1" ]]; then
    parallel_model_term_fft_label="-parallel-model-terms"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_PARALLEL_MODEL_TERM_FFT=1)
elif [[ "$parallel_model_term_fft" != "0" ]]; then
    echo "CASA_RS_VLASS_PARALLEL_MODEL_TERM_FFT must be 0 or 1" >&2
    exit 2
fi
if [[ "$model_fft_timing" == "1" ]]; then
    model_fft_timing_label="-model-fft-timing"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_MODEL_FFT_TIMING=1)
elif [[ "$model_fft_timing" != "0" ]]; then
    echo "CASA_RS_VLASS_MODEL_FFT_TIMING must be 0 or 1" >&2
    exit 2
fi
if [[ "$fftw_f64_timing" == "1" ]]; then
    fftw_f64_timing_label="-fftw-f64-timing"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_FFTW_F64_TIMING=1)
elif [[ "$fftw_f64_timing" != "0" ]]; then
    echo "CASA_RS_VLASS_FFTW_F64_TIMING must be 0 or 1" >&2
    exit 2
fi
if [[ -n "$fftw_f64_wisdom" ]]; then
    if [[ ! -f "$fftw_f64_wisdom" ]]; then
        echo "CASA_RS_VLASS_FFTW_F64_WISDOM must name an existing wisdom file" >&2
        exit 2
    fi
    fftw_f64_wisdom_label="-fftw-f64-wisdom"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_FFTW_F64_WISDOM="$fftw_f64_wisdom"
    )
fi
if [[ -n "$fftw_f32_wisdom" ]]; then
    if [[ ! -f "$fftw_f32_wisdom" ]]; then
        echo "CASA_RS_VLASS_FFTW_F32_WISDOM must name an existing wisdom file" >&2
        exit 2
    fi
    fftw_f32_wisdom_label="-fftw-f32-wisdom"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_FFTW_F32_WISDOM="$fftw_f32_wisdom"
    )
fi
if [[ "$sparse_model_prep" == "1" ]]; then
    sparse_model_prep_label="-sparse-model-prep"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_SPARSE_AWPROJECT_MODEL_PREP=1)
elif [[ "$sparse_model_prep" != "0" ]]; then
    echo "CASA_RS_VLASS_SPARSE_MODEL_PREP must be 0 or 1" >&2
    exit 2
fi
if [[ "$parallel_residual_term_fft" == "1" ]]; then
    parallel_residual_term_fft_label="-parallel-residual-terms"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_PARALLEL_RESIDUAL_TERM_FFT=1)
elif [[ "$parallel_residual_term_fft" != "0" ]]; then
    echo "CASA_RS_VLASS_PARALLEL_RESIDUAL_TERM_FFT must be 0 or 1" >&2
    exit 2
fi
if [[ "$persistent_metal_pack" == "1" ]]; then
    persistent_metal_pack_label="-persistent-metal-pack"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_PERSISTENT_METAL_PACK=1)
elif [[ "$persistent_metal_pack" != "0" ]]; then
    echo "CASA_RS_VLASS_PERSISTENT_METAL_PACK must be 0 or 1" >&2
    exit 2
fi
if [[ "$residual_live_cfs_only" == "1" ]]; then
    residual_live_cfs_only_label="-residual-live-cfs"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_RESIDUAL_LIVE_CFS_ONLY=1)
elif [[ "$residual_live_cfs_only" != "0" ]]; then
    echo "CASA_RS_VLASS_RESIDUAL_LIVE_CFS_ONLY must be 0 or 1" >&2
    exit 2
fi
if [[ "$metal_f32_residual_fft" == "1" ]]; then
    metal_f32_residual_fft_label="-metal-f32-residual-fft"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_F32_RESIDUAL_FFT=1)
elif [[ "$metal_f32_residual_fft" != "0" ]]; then
    echo "CASA_RS_VLASS_METAL_F32_RESIDUAL_FFT must be 0 or 1" >&2
    exit 2
fi
if [[ "$metal_prediction_probe" == "1" ]]; then
    metal_prediction_probe_label="-mpred"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_PREDICTION_PROBE=1)
elif [[ "$metal_prediction_probe" != "0" ]]; then
    echo "CASA_RS_VLASS_METAL_PREDICTION_PROBE must be 0 or 1" >&2
    exit 2
fi
if [[ "$metal_tile_grid_probe" == "1" ]]; then
    case "$metal_tile_side" in
        8|16|32)
            ;;
        *)
            echo "CASA_RS_VLASS_METAL_TILE_SIDE must be 8, 16, or 32" >&2
            exit 2
            ;;
    esac
    metal_tile_grid_probe_label="-mtile${metal_tile_side}"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_TILE_GRID_PROBE=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_TILE_SIDE="$metal_tile_side"
    )
elif [[ "$metal_tile_grid_probe" != "0" ]]; then
    echo "CASA_RS_VLASS_METAL_TILE_GRID_PROBE must be 0 or 1" >&2
    exit 2
fi
if [[ "$metal_resident_chain_probe" == "1" ]]; then
    case "$metal_tile_side" in
        8|16|32)
            ;;
        *)
            echo "CASA_RS_VLASS_METAL_TILE_SIDE must be 8, 16, or 32" >&2
            exit 2
            ;;
    esac
    metal_resident_chain_probe_label="-mchain${metal_tile_side}"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_RESIDENT_CHAIN_PROBE=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_TILE_SIDE="$metal_tile_side"
    )
elif [[ "$metal_resident_chain_probe" != "0" ]]; then
    echo "CASA_RS_VLASS_METAL_RESIDENT_CHAIN_PROBE must be 0 or 1" >&2
    exit 2
fi
if [[ "$metal_resident_tile_chain" == "1" ]]; then
    case "$metal_tile_side" in
        8|16|32)
            ;;
        *)
            echo "CASA_RS_VLASS_METAL_TILE_SIDE must be 8, 16, or 32" >&2
            exit 2
            ;;
    esac
    metal_resident_tile_chain_label="-mrtile${metal_tile_side}"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_RESIDENT_TILE_CHAIN=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_TILE_SIDE="$metal_tile_side"
    )
elif [[ "$metal_resident_tile_chain" != "0" ]]; then
    echo "CASA_RS_VLASS_METAL_RESIDENT_TILE_CHAIN must be 0 or 1" >&2
    exit 2
fi
if [[ "$metal_gpu_residual_replay" == "1" ]]; then
    metal_gpu_residual_replay_label="-gpu-residual-replay"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GPU_RESIDUAL_REPLAY=1
    )
elif [[ "$metal_gpu_residual_replay" != "0" ]]; then
    echo "CASA_RS_VLASS_METAL_GPU_RESIDUAL_REPLAY must be 0 or 1" >&2
    exit 2
fi
if [[ "$metal_global_tile_replay" == "1" ]]; then
    metal_global_tile_replay_label="-global-tile-replay"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GLOBAL_TILE_REPLAY=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GPU_RESIDUAL_REPLAY=1
    )
elif [[ "$metal_global_tile_replay" != "0" ]]; then
    echo "CASA_RS_VLASS_METAL_GLOBAL_TILE_REPLAY must be 0 or 1" >&2
    exit 2
fi
if [[ "$prediction_grid_census" == "1" ]]; then
    prediction_grid_census_label="-prediction-grid-census"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_PREDICTION_GRID_CENSUS=1
    )
elif [[ "$prediction_grid_census" != "0" ]]; then
    echo "CASA_RS_VLASS_PREDICTION_GRID_CENSUS must be 0 or 1" >&2
    exit 2
fi
if [[ "$model_delta_census" == "1" ]]; then
    model_delta_census_label="-model-delta-census"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_MODEL_DELTA_CENSUS=1
    )
elif [[ "$model_delta_census" != "0" ]]; then
    echo "CASA_RS_VLASS_MODEL_DELTA_CENSUS must be 0 or 1" >&2
    exit 2
fi
if [[ "$incremental_model_probe" == "1" ]]; then
    case "$incremental_model_max_delta_positions" in
        *[!0-9]*)
            echo "CASA_RS_VLASS_INCREMENTAL_MODEL_MAX_DELTA_POSITIONS must be a non-negative integer" >&2
            exit 2
            ;;
    esac
    incremental_model_probe_label="-incremental-model-probe"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_INCREMENTAL_MODEL_PROBE=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_INCREMENTAL_MODEL_MAX_DELTA_POSITIONS="$incremental_model_max_delta_positions"
    )
elif [[ "$incremental_model_probe" != "0" ]]; then
    echo "CASA_RS_VLASS_INCREMENTAL_MODEL_PROBE must be 0 or 1" >&2
    exit 2
fi
if [[ "$incremental_model_runtime" == "1" ]]; then
    case "$incremental_model_max_delta_positions" in
        *[!0-9]*)
            echo "CASA_RS_VLASS_INCREMENTAL_MODEL_MAX_DELTA_POSITIONS must be a non-negative integer" >&2
            exit 2
            ;;
    esac
    incremental_model_runtime_label="-incremental-model-runtime"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_INCREMENTAL_MODEL_RUNTIME=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_INCREMENTAL_MODEL_MAX_DELTA_POSITIONS="$incremental_model_max_delta_positions"
    )
elif [[ "$incremental_model_runtime" != "0" ]]; then
    echo "CASA_RS_VLASS_INCREMENTAL_MODEL_RUNTIME must be 0 or 1" >&2
    exit 2
fi
if [[ "$incremental_model_probe" == "1" && "$incremental_model_runtime" == "1" ]]; then
    echo "CASA_RS_VLASS_INCREMENTAL_MODEL_PROBE and CASA_RS_VLASS_INCREMENTAL_MODEL_RUNTIME are mutually exclusive" >&2
    exit 2
fi
if [[ "$selected_model_dft" == "1" ]]; then
    selected_model_dft_label="-selected-model-dft"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_SELECTED_MODEL_DFT=1
    )
elif [[ "$selected_model_dft" != "0" ]]; then
    echo "CASA_RS_VLASS_SELECTED_MODEL_DFT must be 0 or 1" >&2
    exit 2
fi
if [[ "$selected_model_dft" == "1" \
    && ( "$incremental_model_probe" == "1" || "$incremental_model_runtime" == "1" ) ]]; then
    echo "CASA_RS_VLASS_SELECTED_MODEL_DFT is mutually exclusive with incremental model experiments" >&2
    exit 2
fi
if [[ "$image_response_cache" == "1" ]]; then
    image_response_cache_label="-image-response-cache"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_CACHE=1
    )
elif [[ "$image_response_cache" != "0" ]]; then
    echo "CASA_RS_VLASS_IMAGE_RESPONSE_CACHE must be 0 or 1" >&2
    exit 2
fi
if [[ "$image_response_cache" == "1" \
    && ( "$selected_model_dft" == "1" \
        || "$incremental_model_probe" == "1" \
        || "$incremental_model_runtime" == "1" ) ]]; then
    echo "CASA_RS_VLASS_IMAGE_RESPONSE_CACHE is mutually exclusive with model Fourier experiments" >&2
    exit 2
fi
if [[ "$image_response_dyadic_census" == "1" ]]; then
    if [[ "$image_response_cache" != "1" ]]; then
        echo "CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_CENSUS requires CASA_RS_VLASS_IMAGE_RESPONSE_CACHE=1" >&2
        exit 2
    fi
    image_response_dyadic_census_label="-dyadic-census"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_DYADIC_CENSUS=1
    )
elif [[ "$image_response_dyadic_census" != "0" ]]; then
    echo "CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_CENSUS must be 0 or 1" >&2
    exit 2
fi
if [[ "$image_response_dyadic_tiles" == "1" ]]; then
    if [[ "$image_response_cache" != "1" ]]; then
        echo "CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_TILES requires CASA_RS_VLASS_IMAGE_RESPONSE_CACHE=1" >&2
        exit 2
    fi
    image_response_dyadic_tiles_label="-dyadic-tiles"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_DYADIC_TILES=1
    )
elif [[ "$image_response_dyadic_tiles" != "0" ]]; then
    echo "CASA_RS_VLASS_IMAGE_RESPONSE_DYADIC_TILES must be 0 or 1" >&2
    exit 2
fi
if [[ -n "$replay_retention_bytes" ]]; then
    case "$replay_retention_bytes" in
        *[!0-9]*)
            echo "CASA_RS_VLASS_REPLAY_RETENTION_BYTES must be a non-negative integer" >&2
            exit 2
            ;;
    esac
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_REPLAY_RETENTION_BYTES="$replay_retention_bytes"
    )
fi
if [[ -n "$sparse_model_dft_max_pixels" ]]; then
    case "$sparse_model_dft_max_pixels" in
        *[!0-9]*|0)
            echo "CASA_RS_VLASS_SPARSE_MODEL_DFT_MAX_PIXELS must be a positive integer" >&2
            exit 2
            ;;
    esac
    sparse_model_dft_label="-sparse-model-dft${sparse_model_dft_max_pixels}"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_SPARSE_MODEL_DFT_MAX_PIXELS="$sparse_model_dft_max_pixels"
    )
fi
if [[ -n "$model_fft_threads" ]]; then
    case "$model_fft_threads" in
        *[!0-9]*|0)
            echo "CASA_RS_VLASS_MODEL_FFT_THREADS must be a positive integer" >&2
            exit 2
            ;;
    esac
    model_fft_label="-modelfft-t${model_fft_threads}"
    experimental_environment+=(
        CASA_RS_AWPROJECT_MODEL_FFT_THREADS_EXPERIMENT="$model_fft_threads"
    )
fi
if [[ "$model_sparsity_profile" == "1" ]]; then
    experimental_environment+=(CASA_RS_EXPERIMENTAL_AWPROJECT_MODEL_SPARSITY_PROFILE=1)
elif [[ "$model_sparsity_profile" != "0" ]]; then
    echo "CASA_RS_VLASS_MODEL_SPARSITY_PROFILE must be 0 or 1" >&2
    exit 2
fi
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
if [[ "$plan_threads" != "1" ]]; then
    plan_threads_label="-awplant${plan_threads}"
    experimental_environment+=(CASA_RS_AWPROJECT_PLAN_THREADS="$plan_threads")
fi
case "$pack_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_AW_PACK_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
if [[ "$pack_threads" != "1" ]]; then
    pack_threads_label="-awpackt${pack_threads}"
    experimental_environment+=(CASA_RS_AWPROJECT_PACK_THREADS="$pack_threads")
fi
case "$standard_mfs_acceleration" in
    cpu)
        ;;
    metal)
        acceleration_label="-accel-metal"
        parallel_argument=(--parallel)
        ;;
    *)
        echo "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION must be cpu or metal" >&2
        exit 2
        ;;
esac
if [[ "$metal_resident_tile_chain" == "1" && "$standard_mfs_acceleration" != "metal" ]]; then
    echo "CASA_RS_VLASS_METAL_RESIDENT_TILE_CHAIN requires CASA_RS_VLASS_STANDARD_MFS_ACCELERATION=metal" >&2
    exit 2
fi
if [[ "$metal_gpu_residual_replay" == "1" && "$standard_mfs_acceleration" != "metal" ]]; then
    echo "CASA_RS_VLASS_METAL_GPU_RESIDUAL_REPLAY requires CASA_RS_VLASS_STANDARD_MFS_ACCELERATION=metal" >&2
    exit 2
fi
if [[ "$metal_global_tile_replay" == "1" && "$standard_mfs_acceleration" != "metal" ]]; then
    echo "CASA_RS_VLASS_METAL_GLOBAL_TILE_REPLAY requires CASA_RS_VLASS_STANDARD_MFS_ACCELERATION=metal" >&2
    exit 2
fi
if [[ "$grid_threads" != "1" ]]; then
    grid_threads_label="-gridt${grid_threads}"
    parallel_label="-parallel"
    parallel_argument=(--parallel)
fi
label="vlass-production-clean-4096-four-spw-sparse-fftw-t${fftw_threads}-niter${niter}${tapless_phase_label}${replay_compact_programs_label}${prime_replay_initial_dirty_label}${residual_only_label}${residual_live_cfs_only_label}${metal_f32_residual_fft_label}${metal_prediction_probe_label}${metal_tile_grid_probe_label}${metal_resident_chain_probe_label}${metal_resident_tile_chain_label}${metal_gpu_residual_replay_label}${metal_global_tile_replay_label}${prediction_grid_census_label}${model_delta_census_label}${incremental_model_probe_label}${incremental_model_runtime_label}${selected_model_dft_label}${image_response_cache_label}${image_response_dyadic_census_label}${image_response_dyadic_tiles_label}${prediction_sidecar_label}${wide_division_sidecar_label}${hybrid_residual_label}${hybrid_clean_label}${predivision_source_phase_label}${raw_frame_taylor_label}${prediction_prefix_trace_label}${model_fft_label}${sparse_model_dft_label}${linear_madfm_label}${keyed_madfm_label}${radix_madfm_label}${cache_refreshed_nsigma_label}${sparse_mask_peak_search_label}${parallel_model_term_fft_label}${model_fft_timing_label}${fftw_f64_timing_label}${fftw_f64_wisdom_label}${fftw_f32_wisdom_label}${sparse_model_prep_label}${parallel_residual_term_fft_label}${persistent_metal_pack_label}${plan_threads_label}${pack_threads_label}${grid_threads_label}${parallel_label}${acceleration_label}-v1"
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
cf_cache="$root/cf-cache/6.7.5.18/single-field-4096-four-spw"
mask="$root/masks/vlass-single-field-peak-box-4096.mask"

case "$fftw_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_FFTW_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$niter" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_NITER must be a positive integer" >&2
        exit 2
        ;;
esac
for required in "$binary" "$fftw_dir" "$measures_dir" "$ms" "$cf_cache" "$mask"; do
    if [[ ! -e "$required" ]]; then
        echo "required sparse-FFTW diagnostic input does not exist: $required" >&2
        exit 2
    fi
done
if [[ -e "$output.image.tt0" || -e "$log" ]]; then
    echo "refusing to overwrite existing sparse-FFTW diagnostic products" >&2
    exit 2
fi

mkdir -p "$(dirname "$output")" "$(dirname "$log")"
set +e
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
    --spw 2,7,12,17 \
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
    --usepointing \
    --weighting briggs \
    --robust 1.0 \
    --perchanweightdensity \
    --deconvolver mtmfs \
    --standard-mfs-acceleration "$standard_mfs_acceleration" \
    --imaging-fft-precision f64 \
    --imaging-fft-backend fftw \
    "${parallel_argument[@]}" \
    --standard-mfs-grid-threads "$grid_threads" \
    --imaging-memory-target-mb 16384 \
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
    --mask-image "$mask" \
    --no-preview-pngs \
    >"$log" 2>&1
run_status=$?
set -e

if [[ -n "$prediction_sidecar_prefix" ]]; then
    if [[ "$run_status" -eq 0 ]]; then
        echo "prediction-sidecar run unexpectedly continued beyond its fail-closed stop" >&2
        exit 2
    fi
    for suffix in audit.bin results.bin host.json; do
        if [[ ! -f "${prediction_sidecar_prefix}.${suffix}" ]]; then
            echo "prediction-sidecar run stopped without ${prediction_sidecar_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    if ! /usr/bin/grep -Fq \
        "AWProject frozen-model prediction sidecar completed before residual gridding" \
        "$log"; then
        echo "prediction-sidecar run failed before its intended terminal boundary" >&2
        exit 2
    fi
    echo "$log"
    exit 0
fi
if [[ -n "$wide_division_sidecar_prefix" ]]; then
    if [[ "$run_status" -eq 0 ]]; then
        echo "wide-division sidecar run unexpectedly continued beyond its fail-closed stop" >&2
        exit 2
    fi
    for suffix in \
        raw.bin host.json \
        current.audit.bin current.results.bin current.host.json \
        wide.audit.bin wide.results.bin wide.host.json; do
        if [[ ! -f "${wide_division_sidecar_prefix}.${suffix}" ]]; then
            echo "wide-division sidecar run stopped without ${wide_division_sidecar_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    if ! /usr/bin/grep -Fq \
        "AWProject wide-division sidecar completed before residual gridding" \
        "$log"; then
        echo "wide-division sidecar run failed before its intended terminal boundary" >&2
        exit 2
    fi
    echo "$log"
    exit 0
fi
if [[ -n "$prediction_prefix_trace" ]]; then
    if [[ "$run_status" -eq 0 ]]; then
        echo "prediction-prefix trace unexpectedly continued beyond its fail-closed stop" >&2
        exit 2
    fi
    if [[ ! -f "$prediction_prefix_trace" ]]; then
        echo "prediction-prefix run stopped without $prediction_prefix_trace" >&2
        exit 2
    fi
    if ! /usr/bin/grep -Fq \
        "AWProject prediction-prefix trace completed before Metal prediction or residual gridding" \
        "$log"; then
        echo "prediction-prefix run failed before its intended terminal boundary" >&2
        exit 2
    fi
    echo "$log"
    exit 0
fi
if [[ -n "$hybrid_residual_prefix" ]]; then
    if [[ "$run_status" -eq 0 ]]; then
        echo "hybrid residual run unexpectedly continued beyond its fail-closed stop" >&2
        exit 2
    fi
    for suffix in \
        prediction.json normalized.tt0.f32le normalized.tt1.f32le \
        normalized.mask.u8 normalized.json; do
        if [[ ! -f "${hybrid_residual_prefix}.${suffix}" ]]; then
            echo "hybrid residual run stopped without ${hybrid_residual_prefix}.${suffix}" >&2
            exit 2
        fi
    done
    if ! /usr/bin/grep -Fq \
        "AWProject hybrid residual diagnostic completed after normalized residuals and before products" \
        "$log"; then
        echo "hybrid residual run failed before its intended terminal boundary" >&2
        exit 2
    fi
    echo "$log"
    exit 0
fi
if [[ "$run_status" -ne 0 ]]; then
    exit "$run_status"
fi
echo "$log"
