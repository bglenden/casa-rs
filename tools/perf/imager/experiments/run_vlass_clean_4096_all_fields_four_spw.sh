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
windowed_hybrid_clean="${CASA_RS_VLASS_WINDOWED_HYBRID_CLEAN:-auto}"
compact_global_metal_replay="${CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY:-0}"
metal_tile_side="${CASA_RS_VLASS_METAL_TILE_SIDE:-16}"
replay_retention_bytes="${CASA_RS_VLASS_REPLAY_RETENTION_BYTES:-0}"
grouped_segment_target_bytes="${CASA_RS_VLASS_GROUPED_SEGMENT_TARGET_BYTES:-}"
all_field_tuned_replay="${CASA_RS_VLASS_ALL_FIELD_TUNED_REPLAY:-0}"
all_field_spatial_replay="${CASA_RS_VLASS_ALL_FIELD_SPATIAL_REPLAY:-0}"
spatial_neon_2x2="${CASA_RS_VLASS_SPATIAL_NEON_2X2:-1}"
spatial_tapless_phase="${CASA_RS_VLASS_SPATIAL_TAPLESS_PHASE:-1}"
tap_budget_mb="${CASA_RS_VLASS_TAP_BUDGET_MB:-1792}"
direct_cf_select="${CASA_RS_VLASS_DIRECT_CF_SELECT:-1}"
packed_cf="${CASA_RS_VLASS_PACKED_CF:-}"
packed_cf_sha256="${CASA_RS_VLASS_PACKED_CF_SHA256:-}"
niter="${CASA_RS_VLASS_NITER:-2000}"
standard_mfs_acceleration="${CASA_RS_VLASS_STANDARD_MFS_ACCELERATION:-metal}"
grid_threads="${CASA_RS_VLASS_GRID_THREADS:-2}"
fft_backend="${CASA_RS_VLASS_FFT_BACKEND:-fftw}"
memory_pressure_policy_arg="${CASA_RS_VLASS_MEMORY_PRESSURE_POLICY_ARG:-1}"
prediction_sidecar_prefix="${CASA_RS_VLASS_PREDICTION_SIDECAR_PREFIX:-}"
prediction_sidecar_source_limit="${CASA_RS_VLASS_PREDICTION_SIDECAR_SOURCE_LIMIT:-}"
prediction_prefix_trace="${CASA_RS_VLASS_PREDICTION_PREFIX_TRACE:-}"
prediction_prefix_source_ordinal="${CASA_RS_VLASS_PREDICTION_PREFIX_SOURCE_ORDINAL:-}"
field_selection="${CASA_RS_VLASS_FIELD_SELECTION:-1107~1127,1512~1532,1542~1562}"
frozen_model_prefix="${CASA_RS_VLASS_FROZEN_MODEL_PREFIX:-}"
frozen_weight_image="${CASA_RS_VLASS_FROZEN_WEIGHT_IMAGE:-}"
exact_refresh_interval="${CASA_RS_VLASS_EXACT_REFRESH_INTERVAL:-}"
secant_response="${CASA_RS_VLASS_SECANT_RESPONSE:-0}"
secant_response_relative_remainder="${CASA_RS_VLASS_SECANT_RESPONSE_RELATIVE_REMAINDER:-0.001}"
secant_response_max_basis="${CASA_RS_VLASS_SECANT_RESPONSE_MAX_BASIS:-6}"
model_delta_census="${CASA_RS_VLASS_MODEL_DELTA_CENSUS:-0}"
prime_replay_initial_dirty="${CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY:-0}"
logical_tap_budget_mib="${CASA_RS_VLASS_LOGICAL_TAP_BUDGET_MIB:-}"
initial_dirty_backend="${CASA_RS_VLASS_INITIAL_DIRTY_BACKEND:-}"
residual_backend="${CASA_RS_VLASS_RESIDUAL_BACKEND:-}"
ms="$root/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
output="$run_root/rust"
log="$run_root/casa-rs.log"
provenance="$run_root/provenance.txt"
expected_mask_sha256="8490acb911cbbba78f7a20ba4a1d379e227c3a42dfc7eefcc9b7fd5f4139572f"
expected_packed_cf_sha256="a35083dcd4bd84f06c3576a74c12bc42676ea4a14237d87cb5a1b55c9bf74219"
experimental_environment=(
    CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS=1
    CASA_RS_EXPERIMENTAL_MT_MFS_CASA_FFT0=1
)

if [[ -n "$grouped_segment_target_bytes" ]]; then
    case "$grouped_segment_target_bytes" in
        *[!0-9]*|0)
            echo "CASA_RS_VLASS_GROUPED_SEGMENT_TARGET_BYTES must be a positive integer" >&2
            exit 2
            ;;
    esac
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_GROUPED_SEGMENT_TARGET_BYTES="$grouped_segment_target_bytes"
    )
fi

if [[ "${CASA_RS_VLASS_SEPARABLE_GLOBAL_PHASE+x}" == "x" ]]; then
    echo "CASA_RS_VLASS_SEPARABLE_GLOBAL_PHASE is no longer supported; separable Metal phase replay is the production default" >&2
    exit 2
fi

case "$niter" in
    ''|*[!0-9]*)
        echo "CASA_RS_VLASS_NITER must be a non-negative integer" >&2
        exit 2
        ;;
esac
case "$standard_mfs_acceleration" in
    cpu|metal)
        ;;
    *)
        echo "CASA_RS_VLASS_STANDARD_MFS_ACCELERATION must be cpu or metal" >&2
        exit 2
        ;;
esac
case "$grid_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_GRID_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$memory_pressure_policy_arg" in
    0|1)
        ;;
    *)
        echo "CASA_RS_VLASS_MEMORY_PRESSURE_POLICY_ARG must be 0 or 1" >&2
        exit 2
        ;;
esac
case "$tap_budget_mb" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_TAP_BUDGET_MB must be a positive integer" >&2
        exit 2
        ;;
esac
case "$direct_cf_select" in
    0|1)
        ;;
    *)
        echo "CASA_RS_VLASS_DIRECT_CF_SELECT must be 0 or 1" >&2
        exit 2
        ;;
esac
case "$exact_refresh_interval" in
    '')
        ;;
    *[!0-9]*|0|1)
        echo "CASA_RS_VLASS_EXACT_REFRESH_INTERVAL must be an integer greater than one" >&2
        exit 2
        ;;
esac
case "$secant_response" in
    0|1)
        ;;
    *)
        echo "CASA_RS_VLASS_SECANT_RESPONSE must be 0 or 1" >&2
        exit 2
        ;;
esac
case "$secant_response_max_basis" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_SECANT_RESPONSE_MAX_BASIS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$model_delta_census" in
    0)
        ;;
    1)
        experimental_environment+=(
            CASA_RS_EXPERIMENTAL_AWPROJECT_MODEL_DELTA_CENSUS=1
        )
        ;;
    *)
        echo "CASA_RS_VLASS_MODEL_DELTA_CENSUS must be 0 or 1" >&2
        exit 2
        ;;
esac
case "$prime_replay_initial_dirty" in
    0)
        ;;
    1)
        if [[ "$compact_global_metal_replay" != "1" ]]; then
            echo "CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY=1 requires compact global Metal replay" >&2
            exit 2
        fi
        experimental_environment+=(
            CASA_RS_EXPERIMENTAL_AWPROJECT_PRIME_REPLAY_INITIAL_DIRTY=1
        )
        ;;
    *)
        echo "CASA_RS_VLASS_PRIME_REPLAY_INITIAL_DIRTY must be 0 or 1" >&2
        exit 2
        ;;
esac
case "$logical_tap_budget_mib" in
    '')
        ;;
    *[!0-9]*|0)
        echo "CASA_RS_VLASS_LOGICAL_TAP_BUDGET_MIB must be a positive integer" >&2
        exit 2
        ;;
    *)
        experimental_environment+=(
            CASA_RS_AWPROJECT_TAP_BUDGET_MB_EXPERIMENT="$logical_tap_budget_mib"
        )
        ;;
esac
for backend in "$initial_dirty_backend" "$residual_backend"; do
    case "$backend" in
        ''|cpu|metal|metal-row-run|metal-row-run-grouped)
            ;;
        *)
            echo "invalid explicit standard-MFS backend: $backend" >&2
            exit 2
            ;;
    esac
done
case "$windowed_hybrid_clean" in
    auto)
        if [[ "$residual_backend" == "cpu" ]] ||
            [[ -z "$residual_backend" && "$standard_mfs_acceleration" == "cpu" ]]; then
            windowed_hybrid_clean=0
        else
            windowed_hybrid_clean=1
        fi
        ;;
    0|1)
        ;;
    *)
        echo "CASA_RS_VLASS_WINDOWED_HYBRID_CLEAN must be auto, 0, or 1" >&2
        exit 2
        ;;
esac
if [[ "$windowed_hybrid_clean" == "1" ]] &&
    { [[ "$residual_backend" == "cpu" ]] ||
        [[ -z "$residual_backend" && "$standard_mfs_acceleration" == "cpu" ]]; }; then
    echo "CASA_RS_VLASS_WINDOWED_HYBRID_CLEAN=1 requires a Metal residual backend" >&2
    exit 2
fi
case "$compact_global_metal_replay" in
    0)
        ;;
    1)
        if [[ "$selected_exact_hybrid" != "1" ]]; then
            echo "CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY=1 requires the selected exact hybrid candidate" >&2
            exit 2
        fi
        if [[ "$windowed_hybrid_clean" != "0" ]]; then
            echo "compact global Metal replay and windowed hybrid clean are mutually exclusive" >&2
            exit 2
        fi
        if [[ "$residual_backend" == "cpu" ]] ||
            [[ -z "$residual_backend" && "$standard_mfs_acceleration" == "cpu" ]]; then
            echo "CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY=1 requires a Metal residual backend" >&2
            exit 2
        fi
        if [[ "$replay_retention_bytes" == "0" ]]; then
            echo "CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY=1 requires positive replay retention" >&2
            exit 2
        fi
        case "$metal_tile_side" in
            8|16|32)
                ;;
            *)
                echo "CASA_RS_VLASS_METAL_TILE_SIDE must be 8, 16, or 32" >&2
                exit 2
                ;;
        esac
        experimental_environment+=(
            CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GLOBAL_TILE_REPLAY=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_GPU_RESIDUAL_REPLAY=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_RESIDENT_PROGRAM_COMPACTION=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_HYBRID_CLEAN=1
            CASA_RS_EXPERIMENTAL_AWPROJECT_METAL_TILE_SIDE="$metal_tile_side"
        )
        ;;
    *)
        echo "CASA_RS_VLASS_COMPACT_GLOBAL_METAL_REPLAY must be 0 or 1" >&2
        exit 2
        ;;
esac

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
        if [[ "$windowed_hybrid_clean" == "1" ]]; then
            experimental_environment+=(
                CASA_RS_EXPERIMENTAL_AWPROJECT_WINDOWED_HYBRID_CLEAN=1
            )
        fi
        ;;
    *)
        echo "CASA_RS_VLASS_SELECTED_EXACT_HYBRID must be 0 or 1" >&2
        exit 2
        ;;
esac
if [[ -n "$exact_refresh_interval" ]]; then
    if [[ "$selected_exact_hybrid" != "1" ]]; then
        echo "CASA_RS_VLASS_EXACT_REFRESH_INTERVAL requires the selected exact hybrid candidate" >&2
        exit 2
    fi
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_EXACT_REFRESH_INTERVAL="$exact_refresh_interval"
    )
fi
if [[ "$secant_response" == "1" ]]; then
    if [[ "$selected_exact_hybrid" != "1" ]]; then
        echo "CASA_RS_VLASS_SECANT_RESPONSE=1 requires the selected exact hybrid candidate" >&2
        exit 2
    fi
    if [[ -n "$exact_refresh_interval" ]]; then
        echo "the exact-refresh interval and secant-response experiments are mutually exclusive" >&2
        exit 2
    fi
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_SECANT_RESPONSE_CACHE=1
        CASA_RS_EXPERIMENTAL_AWPROJECT_SECANT_RESPONSE_RELATIVE_REMAINDER="$secant_response_relative_remainder"
        CASA_RS_EXPERIMENTAL_AWPROJECT_SECANT_RESPONSE_MAX_BASIS="$secant_response_max_basis"
    )
fi

case "$all_field_tuned_replay" in
    0)
        if [[ -n "$packed_cf" ]]; then
            echo "CASA_RS_VLASS_PACKED_CF requires CASA_RS_VLASS_ALL_FIELD_TUNED_REPLAY=1" >&2
            exit 2
        fi
        ;;
    1)
        echo "CASA_RS_VLASS_ALL_FIELD_TUNED_REPLAY=1 is retired: the frozen packed-CF archive fails the approved all-field scientific contract; use the source CF cache" >&2
        exit 2
        ;;
    *)
        echo "CASA_RS_VLASS_ALL_FIELD_TUNED_REPLAY must be 0 or 1" >&2
        exit 2
        ;;
esac

case "$all_field_spatial_replay" in
    0)
        ;;
    1)
        experimental_environment+=(
            CASA_RS_AWPROJECT_DYNAMIC_SPARSE_TILE_TASKS_EXPERIMENT=1
            CASA_RS_AWPROJECT_SPARSE_TILES_EXPERIMENT=1
            CASA_RS_AWPROJECT_SPATIAL_TILE_SIDE_EXPERIMENT=192
        )
        case "$spatial_neon_2x2" in
            0)
                ;;
            1)
                experimental_environment+=(CASA_RS_AWPROJECT_NEON_2X2_EXPERIMENT=1)
                ;;
            *)
                echo "CASA_RS_VLASS_SPATIAL_NEON_2X2 must be 0 or 1" >&2
                exit 2
                ;;
        esac
        case "$spatial_tapless_phase" in
            0)
                ;;
            1)
                experimental_environment+=(CASA_RS_AWPROJECT_TAPLESS_PHASE_EXPERIMENT=1)
                ;;
            *)
                echo "CASA_RS_VLASS_SPATIAL_TAPLESS_PHASE must be 0 or 1" >&2
                exit 2
                ;;
        esac
        ;;
    *)
        echo "CASA_RS_VLASS_ALL_FIELD_SPATIAL_REPLAY must be 0 or 1" >&2
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
if [[ "$selected_exact_hybrid" == "1" && "$windowed_hybrid_clean" == "1" ]] &&
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
    printf 'mtmfs_rhs\t%s\n' 'mask-sparse-full-fft-sampled'
    printf 'mtmfs_basis\t%s\n' 'casa-fft0-f32'
    printf 'selected_exact_hybrid\t%s\n' "$selected_exact_hybrid"
    printf 'windowed_hybrid_clean\t%s\n' "$windowed_hybrid_clean"
    printf 'compact_global_metal_replay\t%s\n' "$compact_global_metal_replay"
    printf 'metal_tile_side\t%s\n' "$metal_tile_side"
    printf 'all_field_tuned_replay\t%s\n' "$all_field_tuned_replay"
    printf 'all_field_spatial_replay\t%s\n' "$all_field_spatial_replay"
    printf 'spatial_neon_2x2\t%s\n' "$spatial_neon_2x2"
    printf 'spatial_tapless_phase\t%s\n' "$spatial_tapless_phase"
    printf 'direct_cf_select\t%s\n' "$direct_cf_select"
    printf 'niter\t%s\n' "$niter"
    printf 'standard_mfs_acceleration\t%s\n' "$standard_mfs_acceleration"
    printf 'grid_threads\t%s\n' "$grid_threads"
    printf 'fft_backend\t%s\n' "$fft_backend"
    printf 'memory_pressure_policy_arg\t%s\n' "$memory_pressure_policy_arg"
    printf 'exact_refresh_interval\t%s\n' "${exact_refresh_interval:-exact-every-cycle}"
    printf 'secant_response\t%s\n' "$secant_response"
    printf 'secant_response_relative_remainder\t%s\n' "$secant_response_relative_remainder"
    printf 'secant_response_max_basis\t%s\n' "$secant_response_max_basis"
    printf 'model_delta_census\t%s\n' "$model_delta_census"
    printf 'prime_replay_initial_dirty\t%s\n' "$prime_replay_initial_dirty"
    printf 'logical_tap_budget_mib\t%s\n' "${logical_tap_budget_mib:-planner-default}"
    printf 'initial_dirty_backend\t%s\n' "${initial_dirty_backend:-planner-default}"
    printf 'residual_backend\t%s\n' "${residual_backend:-planner-default}"
    printf 'grouped_segment_target_bytes\t%s\n' "${grouped_segment_target_bytes:-planner-default}"
    if [[ "$all_field_tuned_replay" == "1" ]]; then
        printf 'packed_cf\t%s\n' "$packed_cf"
        printf 'packed_cf_sha256\t%s\n' "$actual_packed_cf_sha256"
        printf 'packed_cf_hash_mode\t%s\n' 'computed-and-frozen-identity-checked-before-run'
        printf 'tap_budget_mb\t%s\n' "$tap_budget_mb"
        printf 'replay_working_set_policy\t%s\n' \
            "mapped-packed-cf-direct-select-prefetch-phase-table-${tap_budget_mb}MiB"
    fi
    if [[ "$all_field_spatial_replay" == "1" ]]; then
        printf 'spatial_replay_policy\t%s\n' \
            'sparse-192px-dynamic-weighted-tasks-neon2x2-exact-source-order'
    fi
    if [[ "$selected_exact_hybrid" == "1" ]]; then
        printf 'prediction_arithmetic\t%s\n' 'casa-wide-division-source-phase-raw-frame-taylor'
        printf 'replay_retention_bytes\t%s\n' "$replay_retention_bytes"
        printf 'acceleration_stack\t%s\n' \
            "bounded-hybrid-windowed-${windowed_hybrid_clean}-global-metal-${compact_global_metal_replay}-image-response-sparse-radix"
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
    printf 'execution\t%s\n' "release-${standard_mfs_acceleration}-grid${grid_threads}-casa-fftw310-f64-t8-memory-auto-16GiB"
} >"$provenance"

set +e
command_args=(
    "$binary"
    --ms "$ms"
    --imagename "$output"
    --imsize 4096
    --cell-arcsec 0.6
    --field "$field_selection"
    --phasecenter-field 1525
    --spw 2,7,12,17
    --channel-start 0
    --channel-count 64
    --specmode mfs
    --gridder awproject
    --interpolation linear
    --projection SIN
    --datacolumn data
    --stokes I
    --uvrange '<12km'
    --intent 'OBSERVE_TARGET#UNSPECIFIED'
    --usepointing
    --weighting briggs
    --robust 1.0
    --perchanweightdensity
    --deconvolver mtmfs
    --standard-mfs-acceleration "$standard_mfs_acceleration"
    --imaging-fft-precision f64
    --imaging-fft-backend "$fft_backend"
    --parallel
    --standard-mfs-grid-threads "$grid_threads"
    --imaging-memory-target-mb 16384
    --imaging-prepare-workers 1
    --imaging-read-ahead-blocks 1
    --hogbom-iteration-mode strict
    --nterms 2
    --scales 0,5,12
    --niter "$niter"
    --gain 0.1
    --threshold-jy 0.0
    --nsigma 5.0
    --psfcutoff 0.35
    --pblimit 0.0001
    --write-pb
    --minor-cycle-length 2000
    --cyclefactor 3.0
    --minpsffraction 0.05
    --maxpsffraction 0.8
    --wterm wproject
    --wprojplanes 32
    --cfcache "$cf_cache"
    --cf-resident-mb 256
    --facets 1
    --computepastep 360.0
    --rotatepastep 360.0
    --pointingoffsetsigdev 0.0
    --normtype flatnoise
    --aterm
    --no-psterm
    --wbawp
    --conjbeams
    --no-mosweight
    --smallscalebias 0.0
    --usemask user
    --mask-image "$mask"
    --savemodel none
    --restoringbeam common
    --no-preview-pngs
)
if [[ "$memory_pressure_policy_arg" == "1" ]]; then
    command_args+=(--imaging-memory-pressure-policy auto)
fi
if [[ -n "$initial_dirty_backend" ]]; then
    command_args+=(--standard-mfs-initial-dirty-backend "$initial_dirty_backend")
fi
if [[ -n "$residual_backend" ]]; then
    command_args+=(--standard-mfs-residual-backend "$residual_backend")
fi
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
    "${command_args[@]}" \
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
