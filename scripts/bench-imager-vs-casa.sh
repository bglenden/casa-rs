#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

if [[ -z "${CASA_RS_TESTDATA_ROOT:-}" && -d "/Volumes/home/casatestdata" ]]; then
  export CASA_RS_TESTDATA_ROOT="/Volumes/home/casatestdata"
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" && -x "$HOME/SoftwareProjects/casa-build/venv/bin/python" ]]; then
  export CASA_RS_CASA_PYTHON="$HOME/SoftwareProjects/casa-build/venv/bin/python"
fi

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [measurementset-path]" >&2
  exit 2
fi

if [[ $# -eq 1 ]]; then
  ms_path="$1"
elif [[ -n "${CASA_RS_TESTDATA_ROOT:-}" ]]; then
  ms_path="$CASA_RS_TESTDATA_ROOT/measurementset/vla/ngc5921.ms"
else
  echo "error: pass a MeasurementSet path or set CASA_RS_TESTDATA_ROOT" >&2
  exit 2
fi

if [[ ! -d "$ms_path" ]]; then
  echo "error: MeasurementSet not found: $ms_path" >&2
  exit 2
fi

if [[ -z "${CASA_RS_CASA_PYTHON:-}" ]]; then
  echo "error: CASA_RS_CASA_PYTHON is not set and no default CASA python was found" >&2
  exit 2
fi

repeats="${BENCH_REPEATS:-${IMAGER_BENCH_REPEATS:-5}}"
warmups="${BENCH_WARMUPS:-${IMAGER_BENCH_WARMUPS:-0}}"
profile_repeats="${BENCH_PROFILE_REPEATS:-${IMAGER_BENCH_PROFILE_REPEATS:-$repeats}}"
profile_warmups="${BENCH_PROFILE_WARMUPS:-${IMAGER_BENCH_PROFILE_WARMUPS:-0}}"
field="${IMAGER_BENCH_FIELD:-0}"
phasecenter_field="${IMAGER_BENCH_PHASECENTER_FIELD:-}"
spw="${IMAGER_BENCH_SPW:-0}"
channel_start="${IMAGER_BENCH_CHANNEL_START:-0}"
channel_count="${IMAGER_BENCH_CHANNEL_COUNT:-1}"
cube_start="${IMAGER_BENCH_CUBE_START:-}"
cube_width="${IMAGER_BENCH_CUBE_WIDTH:-}"
specmode="${IMAGER_BENCH_SPECMODE:-mfs}"
gridder="${IMAGER_BENCH_GRIDDER:-standard}"
casa_gridder="${IMAGER_BENCH_CASA_GRIDDER:-$gridder}"
interpolation="${IMAGER_BENCH_INTERPOLATION:-linear}"
imsize="${IMAGER_BENCH_IMSIZE:-128}"
cell_arcsec="${IMAGER_BENCH_CELL_ARCSEC:-30}"
minor_cycle_length="${IMAGER_BENCH_MINOR_CYCLE_LENGTH:-2}"
cyclefactor="${IMAGER_BENCH_CYCLEFACTOR:-1.0}"
min_psf_fraction="${IMAGER_BENCH_MIN_PSFFRACTION:-0.05}"
max_psf_fraction="${IMAGER_BENCH_MAX_PSFFRACTION:-0.8}"
weighting="${IMAGER_BENCH_WEIGHTING:-natural}"
robust="${IMAGER_BENCH_ROBUST:-0.5}"
perchanweightdensity="${IMAGER_BENCH_PERCHANWEIGHTDENSITY:-}"
deconvolver="${IMAGER_BENCH_DECONVOLVER:-hogbom}"
standard_mfs_acceleration="${IMAGER_BENCH_STANDARD_MFS_ACCELERATION:-auto}"
standard_mfs_grid_threads="${IMAGER_BENCH_STANDARD_MFS_GRID_THREADS:-}"
standard_mfs_metal_minor_cycle_chunk="${IMAGER_BENCH_STANDARD_MFS_METAL_MINOR_CYCLE_CHUNK:-}"
imaging_fft_precision="${IMAGER_BENCH_IMAGING_FFT_PRECISION:-auto}"
imaging_fft_backend="${IMAGER_BENCH_IMAGING_FFT_BACKEND:-auto}"
hogbom_iteration_mode="${IMAGER_BENCH_HOGBOM_ITERATION_MODE:-strict}"
nterms="${IMAGER_BENCH_NTERMS:-1}"
scales="${IMAGER_BENCH_SCALES:-}"
wterm="${IMAGER_BENCH_WTERM:-none}"
wprojplanes="${IMAGER_BENCH_WPROJPLANES:-}"
casa_wprojplanes="${IMAGER_BENCH_CASA_WPROJPLANES:-}"
datacolumn="${IMAGER_BENCH_DATACOLUMN:-DATA}"
stokes="${IMAGER_BENCH_STOKES:-I}"
projection="${IMAGER_BENCH_PROJECTION:-SIN}"
uvrange="${IMAGER_BENCH_UVRANGE:-}"
intent="${IMAGER_BENCH_INTENT:-}"
cfcache="${IMAGER_BENCH_CFCACHE:-}"
cf_resident_mb="${IMAGER_BENCH_CF_RESIDENT_MB:-256}"
facets="${IMAGER_BENCH_FACETS:-1}"
psfphasecenter="${IMAGER_BENCH_PSFPHASECENTER:-}"
vptable="${IMAGER_BENCH_VPTABLE:-}"
aterm="${IMAGER_BENCH_ATERM:-1}"
psterm="${IMAGER_BENCH_PSTERM:-0}"
wbawp="${IMAGER_BENCH_WBAWP:-1}"
conjbeams="${IMAGER_BENCH_CONJBEAMS:-1}"
computepastep="${IMAGER_BENCH_COMPUTEPASTEP:-360}"
rotatepastep="${IMAGER_BENCH_ROTATEPASTEP:-360}"
pointingoffsetsigdev="${IMAGER_BENCH_POINTINGOFFSETSIGDEV:-0}"
mosweight="${IMAGER_BENCH_MOSWEIGHT:-0}"
normtype="${IMAGER_BENCH_NORMTYPE:-flatnoise}"
usepointing="${IMAGER_BENCH_USEPOINTING:-0}"
imaging_memory_target_mb="${IMAGER_BENCH_IMAGING_MEMORY_TARGET_MB:-}"
imaging_prepare_buffer_mb="${IMAGER_BENCH_IMAGING_PREPARE_BUFFER_MB:-}"
imaging_row_block_rows="${IMAGER_BENCH_IMAGING_ROW_BLOCK_ROWS:-}"
imaging_prepare_workers="${IMAGER_BENCH_IMAGING_PREPARE_WORKERS:-}"
imaging_read_ahead_blocks="${IMAGER_BENCH_IMAGING_READ_AHEAD_BLOCKS:-}"
parallel="${IMAGER_BENCH_PARALLEL:-}"
chanchunks="${IMAGER_BENCH_CHANCHUNKS:-}"
mode="${IMAGER_BENCH_MODE:-dirty}"
niter="${IMAGER_BENCH_NITER:-4}"
gain="${IMAGER_BENCH_GAIN:-0.1}"
threshold_jy="${IMAGER_BENCH_THRESHOLD_JY:-0}"
nsigma="${IMAGER_BENCH_NSIGMA:-0}"
psfcutoff="${IMAGER_BENCH_PSFCUTOFF:-0.35}"
pblimit="${IMAGER_BENCH_PBLIMIT:-0.2}"
write_pb="${IMAGER_BENCH_WRITE_PB:-0}"
pbcor="${IMAGER_BENCH_PBCOR:-0}"
smallscalebias="${IMAGER_BENCH_SMALLSCALEBIAS:-0}"
restoration="${IMAGER_BENCH_RESTORATION:-1}"
restoringbeam="${IMAGER_BENCH_RESTORINGBEAM:-}"
interactive="${IMAGER_BENCH_INTERACTIVE:-0}"
usemask="${IMAGER_BENCH_USEMASK:-user}"
mask_image="${IMAGER_BENCH_MASK_IMAGE:-}"
restart="${IMAGER_BENCH_RESTART:-0}"
savemodel="${IMAGER_BENCH_SAVEMODEL:-none}"
calcres="${IMAGER_BENCH_CALCRES:-1}"
calcpsf="${IMAGER_BENCH_CALCPSF:-1}"
keep_output_root="${IMAGER_BENCH_KEEP_OUTPUT_ROOT:-}"
ms_staging="${IMAGER_BENCH_MS_STAGING:-direct}"
default_tmp_root="${TMPDIR:-/tmp}"
if [[ -d "/Volumes/GLENDENNING" ]]; then
  default_tmp_root="/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/tmp"
fi
tmp_root="${IMAGER_BENCH_TMP_ROOT:-$default_tmp_root}"
phase_probe="${IMAGER_BENCH_PHASE_PROBE:-0}"
skip_casa="${IMAGER_BENCH_SKIP_CASA:-0}"
skip_rust="${IMAGER_BENCH_SKIP_RUST:-0}"
skip_profile="${IMAGER_BENCH_SKIP_PROFILE:-0}"
reuse_rust_prefix="${IMAGER_BENCH_REUSE_RUST_PREFIX:-}"
reuse_casa_prefix="${IMAGER_BENCH_REUSE_CASA_PREFIX:-}"

parse_boolean() {
  local name="$1"
  local value="$2"
  case "$value" in
    1|true|TRUE|yes|YES|on|ON)
      echo 1
      ;;
    0|false|FALSE|no|NO|off|OFF|"")
      echo 0
      ;;
    *)
      echo "error: $name must be 0/1, true/false, yes/no, or on/off" >&2
      return 2
      ;;
  esac
}

case "$gridder" in
  wproject|widefield|awproject|awp2|awphpg)
    gridder_uses_wproject_wterm=1
    ;;
  *)
    gridder_uses_wproject_wterm=0
    ;;
esac

if [[ -z "$casa_wprojplanes" ]]; then
  case "$casa_gridder" in
    wproject|widefield)
      casa_wprojplanes=-1
      ;;
    awproject)
      casa_wprojplanes="$wprojplanes"
      ;;
  esac
fi

if [[ "$wterm" != "none" && ! ( "$gridder_uses_wproject_wterm" == "1" && "$wterm" == "wproject" ) ]]; then
  echo "error: scripts/bench-imager-vs-casa.sh supports IMAGER_BENCH_WTERM=none, or wproject with gridder=wproject/widefield/AW aliases" >&2
  exit 2
fi
if [[ -n "$wprojplanes" && ! "$wprojplanes" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_WPROJPLANES must be an unsigned integer" >&2
  exit 2
fi
if [[ -n "$casa_wprojplanes" && "$casa_wprojplanes" != "-1" && ! "$casa_wprojplanes" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_CASA_WPROJPLANES must be -1 or an unsigned integer" >&2
  exit 2
fi
if [[ -n "$standard_mfs_grid_threads" && "$standard_mfs_grid_threads" != "auto" && ! "$standard_mfs_grid_threads" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_STANDARD_MFS_GRID_THREADS must be auto or an unsigned integer" >&2
  exit 2
fi
if [[ -n "$standard_mfs_metal_minor_cycle_chunk" && "$standard_mfs_metal_minor_cycle_chunk" != "auto" && "$standard_mfs_metal_minor_cycle_chunk" != "full" && ! "$standard_mfs_metal_minor_cycle_chunk" =~ ^auto:[0-9]+([.][0-9]+)?$ && ! "$standard_mfs_metal_minor_cycle_chunk" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_STANDARD_MFS_METAL_MINOR_CYCLE_CHUNK must be auto, auto:<positive-ms>, full, or an unsigned integer" >&2
  exit 2
fi
if [[ "$standard_mfs_metal_minor_cycle_chunk" == "0" || "$standard_mfs_metal_minor_cycle_chunk" == "auto:0" || "$standard_mfs_metal_minor_cycle_chunk" == "auto:0.0" ]]; then
  echo "error: IMAGER_BENCH_STANDARD_MFS_METAL_MINOR_CYCLE_CHUNK must be auto, auto:<positive-ms>, full, or a positive integer" >&2
  exit 2
fi
if [[ "$imaging_fft_precision" != "auto" && "$imaging_fft_precision" != "f64" && "$imaging_fft_precision" != "f32" && "$imaging_fft_precision" != "fast-f32" && "$imaging_fft_precision" != "auto-f32" ]]; then
  echo "error: IMAGER_BENCH_IMAGING_FFT_PRECISION must be auto, f64, or f32" >&2
  exit 2
fi
if [[ "$imaging_fft_backend" != "auto" && "$imaging_fft_backend" != "rustfft" && "$imaging_fft_backend" != "accelerate" && "$imaging_fft_backend" != "metal-mpsgraph" ]]; then
  echo "error: IMAGER_BENCH_IMAGING_FFT_BACKEND must be auto, rustfft, accelerate, or metal-mpsgraph" >&2
  exit 2
fi
if [[ -n "$imaging_memory_target_mb" && ! "$imaging_memory_target_mb" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_IMAGING_MEMORY_TARGET_MB must be an unsigned integer" >&2
  exit 2
fi
if [[ -n "$imaging_prepare_buffer_mb" && ! "$imaging_prepare_buffer_mb" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_IMAGING_PREPARE_BUFFER_MB must be an unsigned integer" >&2
  exit 2
fi
if [[ -n "$imaging_row_block_rows" && ! "$imaging_row_block_rows" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_IMAGING_ROW_BLOCK_ROWS must be an unsigned integer" >&2
  exit 2
fi
if [[ -n "$imaging_prepare_workers" && ! "$imaging_prepare_workers" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_IMAGING_PREPARE_WORKERS must be an unsigned integer" >&2
  exit 2
fi
if [[ -n "$imaging_read_ahead_blocks" && ! "$imaging_read_ahead_blocks" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_IMAGING_READ_AHEAD_BLOCKS must be an unsigned integer" >&2
  exit 2
fi
if [[ -n "$chanchunks" && ! "$chanchunks" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_CHANCHUNKS must be an unsigned integer" >&2
  exit 2
fi
if [[ ! "$warmups" =~ ^[0-9]+$ ]]; then
  echo "error: IMAGER_BENCH_WARMUPS must be an unsigned integer" >&2
  exit 2
fi

if [[ "$specmode" != "mfs" && "$specmode" != "cube" && "$specmode" != "cubedata" ]]; then
  echo "error: IMAGER_BENCH_SPECMODE must be mfs, cube, or cubedata" >&2
  exit 2
fi

if [[ "$gridder" != "standard" && "$gridder" != "mosaic" && "$gridder" != "wproject" && "$gridder" != "widefield" && "$gridder" != "awproject" && "$gridder" != "awp2" && "$gridder" != "awphpg" ]]; then
  echo "error: IMAGER_BENCH_GRIDDER must be standard, mosaic, wproject, widefield, awproject, awp2, or awphpg" >&2
  exit 2
fi
if [[ "$casa_gridder" != "standard" && "$casa_gridder" != "mosaic" && "$casa_gridder" != "wproject" && "$casa_gridder" != "widefield" && "$casa_gridder" != "awproject" && "$casa_gridder" != "awp2" && "$casa_gridder" != "awphpg" ]]; then
  echo "error: IMAGER_BENCH_CASA_GRIDDER must be standard, mosaic, wproject, widefield, awproject, awp2, or awphpg" >&2
  exit 2
fi

if [[ "$interpolation" != "nearest" && "$interpolation" != "linear" ]]; then
  echo "error: IMAGER_BENCH_INTERPOLATION must be nearest or linear" >&2
  exit 2
fi

case "$hogbom_iteration_mode" in
  strict|casa|casa-inclusive|casa_inclusive)
    ;;
  *)
    echo "error: IMAGER_BENCH_HOGBOM_ITERATION_MODE must be strict or casa" >&2
    exit 2
    ;;
esac
if [[ "$hogbom_iteration_mode" == "casa_inclusive" ]]; then
  hogbom_iteration_mode="casa"
fi

if [[ "$ms_staging" != "copy" && "$ms_staging" != "direct" ]]; then
  echo "error: IMAGER_BENCH_MS_STAGING must be copy or direct" >&2
  exit 2
fi

case "$phase_probe" in
  1|true|TRUE|yes|YES|on|ON)
    phase_probe_enabled=1
    ;;
  0|false|FALSE|no|NO|off|OFF|"")
    phase_probe_enabled=0
    ;;
  *)
    echo "error: IMAGER_BENCH_PHASE_PROBE must be 0/1, true/false, yes/no, or on/off" >&2
    exit 2
    ;;
esac

case "$skip_profile" in
  1|true|TRUE|yes|YES|on|ON)
    skip_profile_enabled=1
    ;;
  0|false|FALSE|no|NO|off|OFF|"")
    skip_profile_enabled=0
    ;;
  *)
    echo "error: IMAGER_BENCH_SKIP_PROFILE must be 0/1, true/false, yes/no, or on/off" >&2
    exit 2
    ;;
esac

case "$skip_rust" in
  1|true|TRUE|yes|YES|on|ON)
    skip_rust_enabled=1
    ;;
  0|false|FALSE|no|NO|off|OFF|"")
    skip_rust_enabled=0
    ;;
  *)
    echo "error: IMAGER_BENCH_SKIP_RUST must be 0/1, true/false, yes/no, or on/off" >&2
    exit 2
    ;;
esac

case "$write_pb" in
  1|true|TRUE|yes|YES|on|ON)
    write_pb_enabled=1
    ;;
  0|false|FALSE|no|NO|off|OFF|"")
    write_pb_enabled=0
    ;;
  *)
    echo "error: IMAGER_BENCH_WRITE_PB must be 0/1, true/false, yes/no, or on/off" >&2
    exit 2
    ;;
esac

case "$pbcor" in
  1|true|TRUE|yes|YES|on|ON)
    pbcor_enabled=1
    ;;
  0|false|FALSE|no|NO|off|OFF|"")
    pbcor_enabled=0
    ;;
  *)
    echo "error: IMAGER_BENCH_PBCOR must be 0/1, true/false, yes/no, or on/off" >&2
    exit 2
    ;;
esac

aterm_enabled="$(parse_boolean IMAGER_BENCH_ATERM "$aterm")"
psterm_enabled="$(parse_boolean IMAGER_BENCH_PSTERM "$psterm")"
wbawp_enabled="$(parse_boolean IMAGER_BENCH_WBAWP "$wbawp")"
conjbeams_enabled="$(parse_boolean IMAGER_BENCH_CONJBEAMS "$conjbeams")"
mosweight_enabled="$(parse_boolean IMAGER_BENCH_MOSWEIGHT "$mosweight")"
usepointing_enabled="$(parse_boolean IMAGER_BENCH_USEPOINTING "$usepointing")"
restoration_enabled="$(parse_boolean IMAGER_BENCH_RESTORATION "$restoration")"
interactive_enabled="$(parse_boolean IMAGER_BENCH_INTERACTIVE "$interactive")"
restart_enabled="$(parse_boolean IMAGER_BENCH_RESTART "$restart")"
calcres_enabled="$(parse_boolean IMAGER_BENCH_CALCRES "$calcres")"
calcpsf_enabled="$(parse_boolean IMAGER_BENCH_CALCPSF "$calcpsf")"

if [[ ! "$cf_resident_mb" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: IMAGER_BENCH_CF_RESIDENT_MB must be a positive integer" >&2
  exit 2
fi
if [[ ! "$facets" =~ ^[1-9][0-9]*$ ]]; then
  echo "error: IMAGER_BENCH_FACETS must be a positive integer" >&2
  exit 2
fi
if [[ "$projection" != "SIN" && "$projection" != "sin" ]]; then
  echo "error: IMAGER_BENCH_PROJECTION must be SIN" >&2
  exit 2
fi
if [[ "$normtype" != "flatnoise" && "$normtype" != "flatsky" && "$normtype" != "pbsquare" ]]; then
  echo "error: IMAGER_BENCH_NORMTYPE must be flatnoise, flatsky, or pbsquare" >&2
  exit 2
fi
if [[ "$usemask" != "user" && "$usemask" != "auto-multithresh" ]]; then
  echo "error: IMAGER_BENCH_USEMASK must be user or auto-multithresh" >&2
  exit 2
fi
if [[ -n "$mask_image" && ! -d "$mask_image" ]]; then
  echo "error: IMAGER_BENCH_MASK_IMAGE does not exist: $mask_image" >&2
  exit 2
fi
if [[ "$skip_rust_enabled" == "0" && ( "$restoration_enabled" != "1" || "$interactive_enabled" != "0" || "$restart_enabled" != "0" || "$calcres_enabled" != "1" || "$calcpsf_enabled" != "1" ) ]]; then
  echo "error: the Rust benchmark path requires restoration=true, interactive=false, restart=false, calcres=true, and calcpsf=true" >&2
  exit 2
fi
if [[ "$skip_rust_enabled" == "0" && "$savemodel" != "none" ]]; then
  echo "error: the benchmark path requires IMAGER_BENCH_SAVEMODEL=none to avoid mutating the input MeasurementSet" >&2
  exit 2
fi
if [[ "$gridder" == "awproject" && "$aterm_enabled" == "0" && ( "$wbawp_enabled" == "1" || "$conjbeams_enabled" == "1" ) ]]; then
  echo "error: wbawp or conjbeams requires aterm" >&2
  exit 2
fi
if [[ -n "$cfcache" && "$gridder" != "awproject" ]]; then
  echo "error: IMAGER_BENCH_CFCACHE requires IMAGER_BENCH_GRIDDER=awproject" >&2
  exit 2
fi
if [[ "$skip_rust_enabled" == "0" && "$gridder" == "awproject" ]]; then
  if [[ -z "$cfcache" || ! -d "$cfcache" ]]; then
    echo "error: true Rust AWProject requires an existing IMAGER_BENCH_CFCACHE directory" >&2
    exit 2
  fi
elif [[ "$skip_rust_enabled" == "0" && ( "$gridder" == "awp2" || "$gridder" == "awphpg" || "$gridder" == "widefield" ) ]]; then
  echo "error: $gridder is not a Rust AWProject alias; use gridder=awproject with an explicit CF cache, or gridder=wproject for W-only projection" >&2
  exit 2
fi

rust_parallel_flags=()
case "$parallel" in
  1|true|TRUE|yes|YES|on|ON)
    rust_parallel_flags+=(--parallel)
    ;;
  0|false|FALSE|no|NO|off|OFF)
    rust_parallel_flags+=(--no-parallel)
    ;;
  "")
    ;;
  *)
    echo "error: IMAGER_BENCH_PARALLEL must be 0/1, true/false, yes/no, or on/off" >&2
    exit 2
    ;;
esac

if [[ -z "$perchanweightdensity" ]]; then
  if [[ "$specmode" == "cube" || "$specmode" == "cubedata" ]]; then
    perchanweightdensity_enabled=1
  else
    perchanweightdensity_enabled=0
  fi
else
  case "$perchanweightdensity" in
    1|true|TRUE|yes|YES|on|ON)
      perchanweightdensity_enabled=1
      ;;
    0|false|FALSE|no|NO|off|OFF)
      perchanweightdensity_enabled=0
      ;;
    *)
      echo "error: IMAGER_BENCH_PERCHANWEIGHTDENSITY must be 0/1, true/false, yes/no, or on/off" >&2
      exit 2
      ;;
  esac
fi

mkdir -p "$tmp_root"
if [[ "$tmp_root" == /Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts* ]]; then
  marker="/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete/imperformance-artifacts/README_SAFE_TO_DELETE.txt"
  if [[ ! -e "$marker" ]]; then
    cat >"$marker" <<'EOF'
This directory contains generated casa-rs ImPerformance artifacts.
It is safe to delete when no benchmark run is actively using it.
Recreate the contents by rerunning the relevant tools/perf/imager command.
EOF
  fi
fi

if [[ "$mode" == "dirty" ]]; then
  dirty_flag="--dirty-only"
  casa_niter="0"
else
  dirty_flag=""
  casa_niter="$niter"
fi

median_from_file() {
  python3 - "$1" <<'PY'
import statistics
import sys

with open(sys.argv[1], "r", encoding="utf-8") as handle:
    values = [float(line.strip()) for line in handle if line.strip()]
if not values:
    raise SystemExit("no benchmark values provided")
print(f"{statistics.median(values):.6f}")
PY
}

run_timed_command() {
  local stderr_file="$1"
  shift
  local start
  local status
  start="$(python3 - <<'PY'
import time
print(f"{time.perf_counter():.9f}")
PY
)"
  set +e
  if [[ "${IMAGER_BENCH_STREAM_LOG:-0}" == "1" || "${IMAGER_BENCH_STREAM_LOG:-}" == "true" || "${IMAGER_BENCH_STREAM_LOG:-}" == "yes" || "${IMAGER_BENCH_STREAM_LOG:-}" == "on" ]]; then
    : >"$stderr_file"
    tail -f "$stderr_file" >&2 &
    local tail_pid="$!"
    "$@" >/dev/null 2>>"$stderr_file" &
    local command_pid="$!"
    local heartbeat_start="$SECONDS"
    local last_heartbeat="$SECONDS"
    while kill -0 "$command_pid" 2>/dev/null; do
      sleep 1
      if (( SECONDS - last_heartbeat >= 30 )) && kill -0 "$command_pid" 2>/dev/null; then
        echo "benchmark_command_progress command=$(basename "$1") elapsed_s=$((SECONDS - heartbeat_start))" >&2
        last_heartbeat="$SECONDS"
      fi
    done
    wait "$command_pid"
    status="$?"
    kill "$tail_pid" 2>/dev/null
    wait "$tail_pid" 2>/dev/null
  else
    "$@" >/dev/null 2>"$stderr_file"
    status="$?"
  fi
  set -e
  python3 - "$start" "$stderr_file" <<'PY'
import sys
import time

start = float(sys.argv[1])
stderr_file = sys.argv[2]
elapsed = time.perf_counter() - start
with open(stderr_file, "a", encoding="utf-8") as handle:
    handle.write(f"real {elapsed:.6f}\n")
PY
  return "$status"
}

emit_rust_backend_diagnostics() {
  local stderr_file="$1"
  if [[ ! -s "$stderr_file" ]]; then
    return 0
  fi
  grep -E \
    '^(awproject_(plan|cache|mtmfs_sample_census)|single_plane_execution_plan|standard_mfs_runtime_plan|standard_mfs_memory_plan_actual|imaging_source_read_ahead_summary|standard_mfs_source_read_ahead_summary|dirty_product_(fft_timing|gpu_resident|gpu_resident_fallback)|mosaic_dirty_product_gpu_resident|mosaic_mtmfs_(direct_metal_tile_parallel|residual_gpu_resident)|visibility_source_stream_consumer|standard_mfs_profile_run|standard_mfs_(hogbom|clark|multiscale)_minor_cycle_summary|standard_mfs_multiscale_metal_(minor_cycle_summary|indirect_summary)|standard_mfs_clean_residual_refresh_summary|standard_mfs_metal_(residual_refresh|residual_refresh_detail|row_run_residual_refresh|row_run_residual_refresh_detail|row_run_grouped_residual_refresh|row_run_grouped_append_detail)|spectral_slab_plan|spectral_slab_event|spectral_slab_memory|visibility_geometry_cache_summary|image_product_write|mosaic_cube_slab_(plane|executor_summary)|cube_per_plane_backend_summary|cube_slab_executor_limitation|cube_source_row_blocks|cube_plane_state_store_summary|cube_resident_clean_(control|executor_summary|stage_summary|finish_plane|finish_plane_stage_detail)|cube_shared_(direct_)?plane_executor_summary|cube_shared_direct_dirty_eligibility|cube_shared_direct_dirty_source|independent_plane_executor_owned_streaming_done|frontend stage=(prepare_plane_input/(data_coverage|accumulate_rows/detail|finish_cube_source_row_blocks)|write_products|cube_slab/|cube_resident_clean/|cli/))' \
    "$stderr_file" || true
}

echo "ms_path=$ms_path"
echo "CASA_RS_CASA_PYTHON=$CASA_RS_CASA_PYTHON"
echo "mode=$mode specmode=$specmode gridder=$gridder casa_gridder=$casa_gridder field=$field phasecenter_field=$phasecenter_field spw=$spw channel_start=$channel_start channel_count=$channel_count datacolumn=$datacolumn stokes=$stokes projection=$projection uvrange=$uvrange intent=$intent cube_start=$cube_start cube_width=$cube_width interpolation=$interpolation weighting=$weighting robust=$robust perchanweightdensity=$perchanweightdensity_enabled deconvolver=$deconvolver standard_mfs_acceleration=$standard_mfs_acceleration imaging_fft_precision=$imaging_fft_precision imaging_fft_backend=$imaging_fft_backend parallel=$parallel chanchunks=$chanchunks hogbom_iteration_mode=$hogbom_iteration_mode nterms=$nterms scales=$scales smallscalebias=$smallscalebias wterm=$wterm wprojplanes=$wprojplanes casa_wprojplanes=$casa_wprojplanes cfcache=$cfcache cf_resident_mb=$cf_resident_mb facets=$facets psfphasecenter=$psfphasecenter vptable=$vptable aterm=$aterm_enabled psterm=$psterm_enabled wbawp=$wbawp_enabled conjbeams=$conjbeams_enabled computepastep=$computepastep rotatepastep=$rotatepastep pointingoffsetsigdev=$pointingoffsetsigdev mosweight=$mosweight_enabled normtype=$normtype usepointing=$usepointing_enabled imaging_memory_target_mb=$imaging_memory_target_mb imaging_prepare_buffer_mb=$imaging_prepare_buffer_mb imaging_row_block_rows=$imaging_row_block_rows imaging_prepare_workers=$imaging_prepare_workers imaging_read_ahead_blocks=$imaging_read_ahead_blocks imsize=$imsize cell_arcsec=$cell_arcsec repeats=$repeats warmups=$warmups profile_repeats=$profile_repeats profile_warmups=$profile_warmups niter=$niter nsigma=$nsigma cycleniter=$minor_cycle_length cyclefactor=$cyclefactor minpsffraction=$min_psf_fraction maxpsffraction=$max_psf_fraction pblimit=$pblimit write_pb=$write_pb_enabled pbcor=$pbcor_enabled restoration=$restoration_enabled restoringbeam=$restoringbeam interactive=$interactive_enabled usemask=$usemask mask_image=$mask_image restart=$restart_enabled savemodel=$savemodel calcres=$calcres_enabled calcpsf=$calcpsf_enabled ms_staging=$ms_staging phase_probe=$phase_probe_enabled skip_casa=$skip_casa skip_rust=$skip_rust_enabled skip_profile=$skip_profile_enabled reuse_rust_prefix=$reuse_rust_prefix reuse_casa_prefix=$reuse_casa_prefix"
echo

if [[ "$skip_rust_enabled" == "0" ]]; then
  cargo build --release -p casars-imager --bin casars-imager --example profile_imager >/dev/null
fi

tmpdir="$(mktemp -d "$tmp_root/casa-rs-imager-bench.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT
if [[ "$ms_staging" == "copy" ]]; then
  staged_ms_path="$tmpdir/benchmark.ms"
  cp -R "$ms_path" "$staged_ms_path"
  ms_path="$staged_ms_path"
fi
if [[ -n "$keep_output_root" ]]; then
  mkdir -p "$keep_output_root/rust" "$keep_output_root/casa"
  rust_keep_prefix="$keep_output_root/rust/rust"
  casa_keep_prefix="$keep_output_root/casa/casa"
else
  rust_keep_prefix=""
  casa_keep_prefix=""
fi
rust_pb_flags=(--pblimit "$pblimit")
if [[ "$write_pb_enabled" == "1" ]]; then
  rust_pb_flags+=(--write-pb)
fi
if [[ "$pbcor_enabled" == "1" ]]; then
  rust_pb_flags+=(--pbcor)
fi
rust_wproject_flags=()
if [[ -n "$wprojplanes" ]]; then
  rust_wproject_flags+=(--wprojplanes "$wprojplanes")
fi
rust_cube_axis_flags=()
if [[ -n "$cube_start" ]]; then
  rust_cube_axis_flags+=(--start "$cube_start")
fi
if [[ -n "$cube_width" ]]; then
  rust_cube_axis_flags+=(--width "$cube_width")
fi
# chanchunks is retained in archived MFS recipes as an inactive CASA default.
# Forward it only for the cube modes where the production Rust contract gives
# it semantics; passing it to MFS correctly fails production validation.
if [[ -n "$chanchunks" && "$specmode" != "mfs" ]]; then
  rust_cube_axis_flags+=(--chanchunks "$chanchunks")
fi
rust_density_flags=()
if [[ "$perchanweightdensity_enabled" == "1" ]]; then
  rust_density_flags+=(--perchanweightdensity)
else
  rust_density_flags+=(--no-perchanweightdensity)
fi
rust_source_stream_flags=()
if [[ -n "$imaging_memory_target_mb" ]]; then
  rust_source_stream_flags+=(--imaging-memory-target-mb "$imaging_memory_target_mb")
fi
if [[ -n "$imaging_prepare_buffer_mb" ]]; then
  rust_source_stream_flags+=(--imaging-prepare-buffer-mb "$imaging_prepare_buffer_mb")
fi
if [[ -n "$imaging_row_block_rows" ]]; then
  rust_source_stream_flags+=(--imaging-row-block-rows "$imaging_row_block_rows")
fi
if [[ -n "$imaging_prepare_workers" ]]; then
  rust_source_stream_flags+=(--imaging-prepare-workers "$imaging_prepare_workers")
fi
if [[ -n "$imaging_read_ahead_blocks" ]]; then
  rust_source_stream_flags+=(--imaging-read-ahead-blocks "$imaging_read_ahead_blocks")
fi
rust_thread_flags=()
if [[ -n "$standard_mfs_grid_threads" ]]; then
  rust_thread_flags+=(--standard-mfs-grid-threads "$standard_mfs_grid_threads")
fi
if [[ -n "$standard_mfs_metal_minor_cycle_chunk" ]]; then
  rust_thread_flags+=(--standard-mfs-metal-minor-cycle-chunk "$standard_mfs_metal_minor_cycle_chunk")
fi
rust_selection_flags=(
  --projection "$projection"
  --datacolumn "$datacolumn"
  --stokes "$stokes"
)
if [[ -n "$uvrange" ]]; then
  rust_selection_flags+=(--uvrange "$uvrange")
fi
if [[ -n "$intent" ]]; then
  rust_selection_flags+=(--intent "$intent")
fi
if [[ "$usepointing_enabled" == "1" ]]; then
  rust_selection_flags+=(--usepointing)
fi
rust_awproject_flags=()
if [[ "$gridder" == "awproject" ]]; then
  rust_awproject_flags+=(
    --cfcache "$cfcache"
    --cf-resident-mb "$cf_resident_mb"
    --facets "$facets"
    --computepastep "$computepastep"
    --rotatepastep "$rotatepastep"
    --pointingoffsetsigdev "$pointingoffsetsigdev"
    --normtype "$normtype"
  )
  if [[ -n "$psfphasecenter" ]]; then
    rust_awproject_flags+=(--psfphasecenter "$psfphasecenter")
  fi
  if [[ -n "$vptable" ]]; then
    rust_awproject_flags+=(--vptable "$vptable")
  fi
  if [[ "$aterm_enabled" == "1" ]]; then
    rust_awproject_flags+=(--aterm)
  else
    rust_awproject_flags+=(--no-aterm)
  fi
  if [[ "$psterm_enabled" == "1" ]]; then
    rust_awproject_flags+=(--psterm)
  else
    rust_awproject_flags+=(--no-psterm)
  fi
  if [[ "$wbawp_enabled" == "1" ]]; then
    rust_awproject_flags+=(--wbawp)
  else
    rust_awproject_flags+=(--no-wbawp)
  fi
  if [[ "$conjbeams_enabled" == "1" ]]; then
    rust_awproject_flags+=(--conjbeams)
  else
    rust_awproject_flags+=(--no-conjbeams)
  fi
  if [[ "$mosweight_enabled" == "1" ]]; then
    rust_awproject_flags+=(--mosweight)
  else
    rust_awproject_flags+=(--no-mosweight)
  fi
fi
rust_product_flags=(
  --smallscalebias "$smallscalebias"
  --usemask "$usemask"
  --savemodel "$savemodel"
)
if [[ -n "$restoringbeam" ]]; then
  rust_product_flags+=(--restoringbeam "$restoringbeam")
fi
if [[ -n "$mask_image" ]]; then
  rust_product_flags+=(--mask-image "$mask_image")
fi
rust_scale_flags=()
if [[ -n "$scales" ]]; then
  rust_scale_flags+=(--scales "$scales")
fi
rust_dirty_flags=()
if [[ -n "$dirty_flag" ]]; then
  rust_dirty_flags+=("$dirty_flag")
fi

echo "Rust release CLI timings (seconds):"
rust_cli_file="$tmpdir/rust-cli.txt"
run_with_optional_phasecenter() {
  if [[ -n "$phasecenter_field" ]]; then
    "$@" --phasecenter-field "$phasecenter_field"
  else
    "$@"
  fi
}
run_rust_cli() {
  local stderr_file="$1"
  local prefix="$2"
  run_with_optional_phasecenter run_timed_command "$stderr_file" target/release/casars-imager \
    --ms "$ms_path" \
    --imagename "$prefix" \
    --imsize "$imsize" \
    --cell-arcsec "$cell_arcsec" \
    --field "$field" \
    --spw "$spw" \
    --channel-start "$channel_start" \
    --channel-count "$channel_count" \
    --specmode "$specmode" \
    --gridder "$gridder" \
    --interpolation "$interpolation" \
    ${rust_cube_axis_flags[@]+"${rust_cube_axis_flags[@]}"} \
    ${rust_selection_flags[@]+"${rust_selection_flags[@]}"} \
    --weighting "$weighting" \
    --robust "$robust" \
    ${rust_density_flags[@]+"${rust_density_flags[@]}"} \
    --deconvolver "$deconvolver" \
    --standard-mfs-acceleration "$standard_mfs_acceleration" \
    --imaging-fft-precision "$imaging_fft_precision" \
    --imaging-fft-backend "$imaging_fft_backend" \
    ${rust_parallel_flags[@]+"${rust_parallel_flags[@]}"} \
    ${rust_thread_flags[@]+"${rust_thread_flags[@]}"} \
    ${rust_source_stream_flags[@]+"${rust_source_stream_flags[@]}"} \
    --hogbom-iteration-mode "$hogbom_iteration_mode" \
    --nterms "$nterms" \
    ${rust_scale_flags[@]+"${rust_scale_flags[@]}"} \
    --niter "$niter" \
    --gain "$gain" \
    --threshold-jy "$threshold_jy" \
    --nsigma "$nsigma" \
    --psfcutoff "$psfcutoff" \
    ${rust_pb_flags[@]+"${rust_pb_flags[@]}"} \
    --minor-cycle-length "$minor_cycle_length" \
    --cyclefactor "$cyclefactor" \
    --minpsffraction "$min_psf_fraction" \
    --maxpsffraction "$max_psf_fraction" \
    --wterm "$wterm" \
    ${rust_wproject_flags[@]+"${rust_wproject_flags[@]}"} \
    ${rust_awproject_flags[@]+"${rust_awproject_flags[@]}"} \
    ${rust_product_flags[@]+"${rust_product_flags[@]}"} \
    --no-preview-pngs \
    ${rust_dirty_flags[@]+"${rust_dirty_flags[@]}"}
}
run_rust_profile() {
  run_with_optional_phasecenter target/release/examples/profile_imager \
    "$ms_path" \
    --field "$field" \
    --spw "$spw" \
    --channel-start "$channel_start" \
    --channel-count "$channel_count" \
    --specmode "$specmode" \
    --gridder "$gridder" \
    --interpolation "$interpolation" \
    ${rust_cube_axis_flags[@]+"${rust_cube_axis_flags[@]}"} \
    ${rust_selection_flags[@]+"${rust_selection_flags[@]}"} \
    --weighting "$weighting" \
    --robust "$robust" \
    ${rust_density_flags[@]+"${rust_density_flags[@]}"} \
    --deconvolver "$deconvolver" \
    --standard-mfs-acceleration "$standard_mfs_acceleration" \
    --imaging-fft-precision "$imaging_fft_precision" \
    --imaging-fft-backend "$imaging_fft_backend" \
    ${rust_parallel_flags[@]+"${rust_parallel_flags[@]}"} \
    ${rust_thread_flags[@]+"${rust_thread_flags[@]}"} \
    ${rust_source_stream_flags[@]+"${rust_source_stream_flags[@]}"} \
    --hogbom-iteration-mode "$hogbom_iteration_mode" \
    --nterms "$nterms" \
    ${rust_scale_flags[@]+"${rust_scale_flags[@]}"} \
    --imsize "$imsize" \
    --cell-arcsec "$cell_arcsec" \
    --niter "$niter" \
    --gain "$gain" \
    --threshold-jy "$threshold_jy" \
    --nsigma "$nsigma" \
    --psfcutoff "$psfcutoff" \
    ${rust_pb_flags[@]+"${rust_pb_flags[@]}"} \
    --minor-cycle-length "$minor_cycle_length" \
    --cyclefactor "$cyclefactor" \
    --minpsffraction "$min_psf_fraction" \
    --maxpsffraction "$max_psf_fraction" \
    --wterm "$wterm" \
    ${rust_wproject_flags[@]+"${rust_wproject_flags[@]}"} \
    ${rust_awproject_flags[@]+"${rust_awproject_flags[@]}"} \
    ${rust_product_flags[@]+"${rust_product_flags[@]}"} \
    ${rust_dirty_flags[@]+"${rust_dirty_flags[@]}"} \
    --repeats "$profile_repeats" \
    --warmups "$profile_warmups"
}
if [[ "$skip_rust_enabled" == "1" ]]; then
  echo "  skipped; IMAGER_BENCH_SKIP_RUST=$skip_rust"
  if [[ -n "$reuse_rust_prefix" ]]; then
    echo "  kept_rust_prefix=$reuse_rust_prefix"
  fi
else
for ((warmup = 1; warmup <= warmups; warmup++)); do
  prefix="$tmpdir/rust-warmup-$warmup"
  rust_stderr="$tmpdir/rust-warmup-$warmup.stderr"
  echo "rust_warmup_start warmup=$warmup prefix=$prefix"
  if ! run_rust_cli "$rust_stderr" "$prefix"; then
    echo "error: Rust casars-imager warmup $warmup failed" >&2
    cat "$rust_stderr" >&2
    exit 1
  fi
  real_seconds="$(awk '/^real / {print $2}' "$rust_stderr")"
  printf "  warmup=%s real=%s\n" "$warmup" "$real_seconds"
  emit_rust_backend_diagnostics "$rust_stderr"
done
for run in $(seq 1 "$repeats"); do
  if [[ -n "$rust_keep_prefix" && "$run" == "$repeats" ]]; then
    prefix="$rust_keep_prefix"
  else
    prefix="$tmpdir/rust-run-$run"
  fi
  echo "rust_run_start run=$run prefix=$prefix"
  rust_stderr="$tmpdir/rust-$run.stderr"
  if ! run_rust_cli "$rust_stderr" "$prefix"; then
    echo "error: Rust casars-imager run $run failed" >&2
    cat "$rust_stderr" >&2
    exit 1
  fi
  real_seconds="$(awk '/^real / {print $2}' "$rust_stderr")"
  printf "  run=%s real=%s\n" "$run" "$real_seconds"
  printf "%s\n" "$real_seconds" >>"$rust_cli_file"
  emit_rust_backend_diagnostics "$rust_stderr"
done
echo "  median=$(median_from_file "$rust_cli_file")"
if [[ -n "$rust_keep_prefix" ]]; then
  echo "  kept_rust_prefix=$rust_keep_prefix"
fi
fi
echo

echo "Rust stage medians (milliseconds):"
if [[ "$skip_rust_enabled" == "1" ]]; then
  echo "  skipped=1"
elif [[ "$skip_profile_enabled" == "1" ]]; then
  echo "  skipped=1"
else
  run_rust_profile | sed 's/^/  /'
fi
echo

cat >"$tmpdir/casa-imager-bench.py" <<'PY'
import os
import statistics
import tempfile
import time
from casatasks import tclean

vis = os.environ["CASA_RS_BENCH_MS_PATH"]
repeats = int(os.environ["CASA_RS_BENCH_REPEATS"])
warmups = int(os.environ["CASA_RS_BENCH_WARMUPS"])
field = os.environ["CASA_RS_BENCH_FIELD"]
phasecenter_field = os.environ["CASA_RS_BENCH_PHASECENTER_FIELD"]
spw = os.environ["CASA_RS_BENCH_SPW"]
chan_start = int(os.environ["CASA_RS_BENCH_CHANNEL_START"])
chan_count = int(os.environ["CASA_RS_BENCH_CHANNEL_COUNT"])
cube_start = os.environ.get("CASA_RS_BENCH_CUBE_START", "")
cube_width = os.environ.get("CASA_RS_BENCH_CUBE_WIDTH", "")
imsize = int(os.environ["CASA_RS_BENCH_IMSIZE"])
cell_arcsec = os.environ["CASA_RS_BENCH_CELL_ARCSEC"]
niter = int(os.environ["CASA_RS_BENCH_NITER"])
gain = float(os.environ["CASA_RS_BENCH_GAIN"])
threshold_jy = os.environ["CASA_RS_BENCH_THRESHOLD_JY"]
nsigma = float(os.environ["CASA_RS_BENCH_NSIGMA"])
psfcutoff = float(os.environ["CASA_RS_BENCH_PSFCUTOFF"])
pblimit = float(os.environ["CASA_RS_BENCH_PBLIMIT"])
pbcor = os.environ["CASA_RS_BENCH_PBCOR"].lower() in ("1", "true", "yes", "on")
cycleniter = int(os.environ["CASA_RS_BENCH_MINOR_CYCLE_LENGTH"])
cyclefactor = float(os.environ["CASA_RS_BENCH_CYCLEFACTOR"])
minpsffraction = float(os.environ["CASA_RS_BENCH_MIN_PSFFRACTION"])
maxpsffraction = float(os.environ["CASA_RS_BENCH_MAX_PSFFRACTION"])
weighting = os.environ["CASA_RS_BENCH_WEIGHTING"]
robust = float(os.environ["CASA_RS_BENCH_ROBUST"])
perchanweightdensity = os.environ["CASA_RS_BENCH_PERCHANWEIGHTDENSITY"].lower() in ("1", "true", "yes", "on")
deconvolver = os.environ["CASA_RS_BENCH_DECONVOLVER"]
nterms = int(os.environ["CASA_RS_BENCH_NTERMS"])
casa_gridder = os.environ.get("CASA_RS_BENCH_CASA_GRIDDER", os.environ["CASA_RS_BENCH_GRIDDER"])
wprojplanes_env = os.environ.get("CASA_RS_BENCH_WPROJPLANES", "")
scales = [] if os.environ["CASA_RS_BENCH_SCALES"] == "" else [int(float(v)) for v in os.environ["CASA_RS_BENCH_SCALES"].split(",")]
specmode = os.environ["CASA_RS_BENCH_SPECMODE"]
interpolation = os.environ["CASA_RS_BENCH_INTERPOLATION"]
datacolumn = os.environ["CASA_RS_BENCH_DATACOLUMN"]
stokes = os.environ["CASA_RS_BENCH_STOKES"]
projection = os.environ["CASA_RS_BENCH_PROJECTION"]
uvrange = os.environ["CASA_RS_BENCH_UVRANGE"]
intent = os.environ["CASA_RS_BENCH_INTENT"]
cfcache = os.environ["CASA_RS_BENCH_CFCACHE"]
facets = int(os.environ["CASA_RS_BENCH_FACETS"])
psfphasecenter = os.environ["CASA_RS_BENCH_PSFPHASECENTER"]
vptable = os.environ["CASA_RS_BENCH_VPTABLE"]
aterm = os.environ["CASA_RS_BENCH_ATERM"] == "1"
psterm = os.environ["CASA_RS_BENCH_PSTERM"] == "1"
wbawp = os.environ["CASA_RS_BENCH_WBAWP"] == "1"
conjbeams = os.environ["CASA_RS_BENCH_CONJBEAMS"] == "1"
computepastep = float(os.environ["CASA_RS_BENCH_COMPUTEPASTEP"])
rotatepastep = float(os.environ["CASA_RS_BENCH_ROTATEPASTEP"])
pointingoffsetsigdev_values = [float(value) for value in os.environ["CASA_RS_BENCH_POINTINGOFFSETSIGDEV"].split(",")]
pointingoffsetsigdev = pointingoffsetsigdev_values[0] if len(pointingoffsetsigdev_values) == 1 else pointingoffsetsigdev_values
mosweight = os.environ["CASA_RS_BENCH_MOSWEIGHT"] == "1"
normtype = os.environ["CASA_RS_BENCH_NORMTYPE"]
usepointing = os.environ["CASA_RS_BENCH_USEPOINTING"] == "1"
smallscalebias = float(os.environ["CASA_RS_BENCH_SMALLSCALEBIAS"])
restoration = os.environ["CASA_RS_BENCH_RESTORATION"] == "1"
restoringbeam = os.environ["CASA_RS_BENCH_RESTORINGBEAM"]
interactive = os.environ["CASA_RS_BENCH_INTERACTIVE"] == "1"
usemask = os.environ["CASA_RS_BENCH_USEMASK"]
mask_image = os.environ["CASA_RS_BENCH_MASK_IMAGE"]
restart = os.environ["CASA_RS_BENCH_RESTART"] == "1"
savemodel = os.environ["CASA_RS_BENCH_SAVEMODEL"]
calcres = os.environ["CASA_RS_BENCH_CALCRES"] == "1"
calcpsf = os.environ["CASA_RS_BENCH_CALCPSF"] == "1"
keep_output_root = os.environ.get("CASA_RS_BENCH_KEEP_OUTPUT_ROOT", "")
casa_keep_prefix = os.path.join(keep_output_root, "casa", "casa") if keep_output_root else ""
spw_selector = f"{spw}:{chan_start}" if chan_count == 1 else f"{spw}:{chan_start}~{chan_start + chan_count - 1}"
times = []

with tempfile.TemporaryDirectory() as td:
    for iteration in range(warmups + repeats):
        is_warmup = iteration < warmups
        run = iteration - warmups
        if not is_warmup and casa_keep_prefix and run == repeats - 1:
            os.makedirs(os.path.dirname(casa_keep_prefix), exist_ok=True)
            prefix = casa_keep_prefix
        else:
            label = f"warmup-{iteration}" if is_warmup else f"run-{run}"
            prefix = os.path.join(td, label)
        start = time.perf_counter()
        kwargs = dict(
            vis=vis,
            imagename=prefix,
            datacolumn=datacolumn,
            field=field,
            uvrange=uvrange,
            intent=intent,
            stokes=stokes,
            projection=projection,
            specmode=specmode,
            gridder=casa_gridder,
            weighting=weighting,
            perchanweightdensity=perchanweightdensity,
            deconvolver=deconvolver,
            nterms=nterms,
            scales=scales,
            smallscalebias=smallscalebias,
            imsize=imsize,
            cell=f"{cell_arcsec}arcsec",
            niter=niter,
            cycleniter=cycleniter,
            robust=robust,
            gain=gain,
            threshold=f"{threshold_jy}Jy",
            nsigma=nsigma,
            cyclefactor=cyclefactor,
            minpsffraction=minpsffraction,
            maxpsffraction=maxpsffraction,
            restoration=restoration,
            calcpsf=calcpsf,
            calcres=calcres,
            restart=restart,
            interactive=interactive,
            parallel=False,
            pblimit=pblimit,
            pbcor=pbcor,
            usemask=usemask,
            mask=mask_image,
            savemodel=savemodel,
            psfcutoff=psfcutoff,
        )
        if restoringbeam:
            kwargs["restoringbeam"] = restoringbeam
        if casa_gridder == "awproject":
            kwargs.update(
                cfcache=cfcache,
                facets=facets,
                psfphasecenter=psfphasecenter,
                vptable=vptable,
                aterm=aterm,
                psterm=psterm,
                wbawp=wbawp,
                conjbeams=conjbeams,
                computepastep=computepastep,
                rotatepastep=rotatepastep,
                pointingoffsetsigdev=pointingoffsetsigdev,
                mosweight=mosweight,
                normtype=normtype,
                usepointing=usepointing,
            )
        if specmode in ("cube", "cubedata"):
            casa_start = int(cube_start) if cube_start else chan_start
            casa_width = int(cube_width) if cube_width else 1
            kwargs.update(
                spw=str(spw),
                interpolation=interpolation,
                nchan=chan_count,
                start=casa_start,
                width=casa_width,
            )
        else:
            kwargs.update(spw=spw_selector)
        if wprojplanes_env:
            kwargs["wprojplanes"] = int(wprojplanes_env)
        elif casa_gridder in ("wproject", "widefield"):
            # CASA defaults wprojplanes to one, which silently selects ordinary
            # 2-D gridding. Match casa-rs Auto by requesting CASA's Auto mode.
            kwargs["wprojplanes"] = -1
        if phasecenter_field:
            kwargs["phasecenter"] = int(phasecenter_field)
        tclean(**kwargs)
        elapsed = time.perf_counter() - start
        if is_warmup:
            print(f"warmup={iteration + 1} real={elapsed:.6f}")
        else:
            times.append(elapsed)
            print(f"run={run + 1} real={elapsed:.6f}")

print(f"median={statistics.median(times):.6f}")
if casa_keep_prefix:
    print(f"kept_casa_prefix={casa_keep_prefix}")
PY

if [[ "$skip_casa" == "1" || "$skip_casa" == "true" || "$skip_casa" == "yes" || "$skip_casa" == "on" || -n "$reuse_casa_prefix" ]]; then
  echo "CASA tclean timings (seconds):"
  if [[ -n "$reuse_casa_prefix" ]]; then
    echo "  reused; IMAGER_BENCH_REUSE_CASA_PREFIX=$reuse_casa_prefix"
    echo "  kept_casa_prefix=$reuse_casa_prefix"
  else
    echo "  skipped; IMAGER_BENCH_SKIP_CASA=$skip_casa"
  fi
else
  echo "CASA tclean timings (seconds):"
  echo "casa_run_start repeats=$repeats"
  CASA_RS_BENCH_MS_PATH="$ms_path" \
  CASA_RS_BENCH_REPEATS="$repeats" \
  CASA_RS_BENCH_WARMUPS="$warmups" \
  CASA_RS_BENCH_FIELD="$field" \
  CASA_RS_BENCH_PHASECENTER_FIELD="$phasecenter_field" \
  CASA_RS_BENCH_SPW="$spw" \
  CASA_RS_BENCH_CHANNEL_START="$channel_start" \
  CASA_RS_BENCH_CHANNEL_COUNT="$channel_count" \
  CASA_RS_BENCH_CUBE_START="$cube_start" \
  CASA_RS_BENCH_CUBE_WIDTH="$cube_width" \
  CASA_RS_BENCH_SPECMODE="$specmode" \
  CASA_RS_BENCH_GRIDDER="$gridder" \
  CASA_RS_BENCH_CASA_GRIDDER="$casa_gridder" \
  CASA_RS_BENCH_WPROJPLANES="$casa_wprojplanes" \
  CASA_RS_BENCH_DATACOLUMN="$datacolumn" \
  CASA_RS_BENCH_STOKES="$stokes" \
  CASA_RS_BENCH_PROJECTION="$projection" \
  CASA_RS_BENCH_UVRANGE="$uvrange" \
  CASA_RS_BENCH_INTENT="$intent" \
  CASA_RS_BENCH_CFCACHE="$cfcache" \
  CASA_RS_BENCH_CF_RESIDENT_MB="$cf_resident_mb" \
  CASA_RS_BENCH_FACETS="$facets" \
  CASA_RS_BENCH_PSFPHASECENTER="$psfphasecenter" \
  CASA_RS_BENCH_VPTABLE="$vptable" \
  CASA_RS_BENCH_ATERM="$aterm_enabled" \
  CASA_RS_BENCH_PSTERM="$psterm_enabled" \
  CASA_RS_BENCH_WBAWP="$wbawp_enabled" \
  CASA_RS_BENCH_CONJBEAMS="$conjbeams_enabled" \
  CASA_RS_BENCH_COMPUTEPASTEP="$computepastep" \
  CASA_RS_BENCH_ROTATEPASTEP="$rotatepastep" \
  CASA_RS_BENCH_POINTINGOFFSETSIGDEV="$pointingoffsetsigdev" \
  CASA_RS_BENCH_MOSWEIGHT="$mosweight_enabled" \
  CASA_RS_BENCH_NORMTYPE="$normtype" \
  CASA_RS_BENCH_USEPOINTING="$usepointing_enabled" \
  CASA_RS_BENCH_IMSIZE="$imsize" \
  CASA_RS_BENCH_CELL_ARCSEC="$cell_arcsec" \
  CASA_RS_BENCH_WEIGHTING="$weighting" \
  CASA_RS_BENCH_ROBUST="$robust" \
  CASA_RS_BENCH_PERCHANWEIGHTDENSITY="$perchanweightdensity_enabled" \
  CASA_RS_BENCH_DECONVOLVER="$deconvolver" \
  CASA_RS_BENCH_NTERMS="$nterms" \
  CASA_RS_BENCH_SCALES="$scales" \
  CASA_RS_BENCH_SMALLSCALEBIAS="$smallscalebias" \
  CASA_RS_BENCH_NITER="$casa_niter" \
  CASA_RS_BENCH_GAIN="$gain" \
  CASA_RS_BENCH_THRESHOLD_JY="$threshold_jy" \
  CASA_RS_BENCH_NSIGMA="$nsigma" \
  CASA_RS_BENCH_PSFCUTOFF="$psfcutoff" \
  CASA_RS_BENCH_PBLIMIT="$pblimit" \
  CASA_RS_BENCH_PBCOR="$pbcor_enabled" \
  CASA_RS_BENCH_RESTORATION="$restoration_enabled" \
  CASA_RS_BENCH_RESTORINGBEAM="$restoringbeam" \
  CASA_RS_BENCH_INTERACTIVE="$interactive_enabled" \
  CASA_RS_BENCH_USEMASK="$usemask" \
  CASA_RS_BENCH_MASK_IMAGE="$mask_image" \
  CASA_RS_BENCH_RESTART="$restart_enabled" \
  CASA_RS_BENCH_SAVEMODEL="$savemodel" \
  CASA_RS_BENCH_CALCRES="$calcres_enabled" \
  CASA_RS_BENCH_CALCPSF="$calcpsf_enabled" \
  CASA_RS_BENCH_MINOR_CYCLE_LENGTH="$minor_cycle_length" \
  CASA_RS_BENCH_CYCLEFACTOR="$cyclefactor" \
  CASA_RS_BENCH_MIN_PSFFRACTION="$min_psf_fraction" \
  CASA_RS_BENCH_MAX_PSFFRACTION="$max_psf_fraction" \
  CASA_RS_BENCH_INTERPOLATION="$interpolation" \
  CASA_RS_BENCH_KEEP_OUTPUT_ROOT="$keep_output_root" \
    "$CASA_RS_CASA_PYTHON" "$tmpdir/casa-imager-bench.py" | sed 's/^/  /'
fi
echo

if [[ -n "$keep_output_root" ]]; then
  echo "Kept benchmark products:"
  echo "  product_root=$keep_output_root"
  if [[ -n "$reuse_rust_prefix" ]]; then
    echo "  rust_prefix=$reuse_rust_prefix"
  else
    echo "  rust_prefix=$rust_keep_prefix"
  fi
  if [[ -n "$reuse_casa_prefix" ]]; then
    echo "  casa_prefix=$reuse_casa_prefix"
  else
    echo "  casa_prefix=$casa_keep_prefix"
  fi
  echo
fi

if [[ "$phase_probe_enabled" == "1" && -z "$reuse_casa_prefix" && ! ( "$skip_casa" == "1" || "$skip_casa" == "true" || "$skip_casa" == "yes" || "$skip_casa" == "on" ) ]]; then
  echo "CASA PySynthesisImager stage medians (milliseconds):"
  CASA_RS_BENCH_MS_PATH="$ms_path" \
  CASA_RS_BENCH_REPEATS="$repeats" \
  CASA_RS_BENCH_WARMUPS="$warmups" \
  CASA_RS_BENCH_FIELD="$field" \
  CASA_RS_BENCH_PHASECENTER_FIELD="$phasecenter_field" \
  CASA_RS_BENCH_SPW="$spw" \
  CASA_RS_BENCH_CHANNEL_START="$channel_start" \
  CASA_RS_BENCH_CHANNEL_COUNT="$channel_count" \
  CASA_RS_BENCH_CUBE_START="$cube_start" \
  CASA_RS_BENCH_CUBE_WIDTH="$cube_width" \
  CASA_RS_BENCH_SPECMODE="$specmode" \
  CASA_RS_BENCH_GRIDDER="$gridder" \
  CASA_RS_BENCH_CASA_GRIDDER="$casa_gridder" \
  CASA_RS_BENCH_WPROJPLANES="$casa_wprojplanes" \
  CASA_RS_BENCH_DATACOLUMN="$datacolumn" \
  CASA_RS_BENCH_STOKES="$stokes" \
  CASA_RS_BENCH_PROJECTION="$projection" \
  CASA_RS_BENCH_UVRANGE="$uvrange" \
  CASA_RS_BENCH_INTENT="$intent" \
  CASA_RS_BENCH_CFCACHE="$cfcache" \
  CASA_RS_BENCH_FACETS="$facets" \
  CASA_RS_BENCH_PSFPHASECENTER="$psfphasecenter" \
  CASA_RS_BENCH_VPTABLE="$vptable" \
  CASA_RS_BENCH_ATERM="$aterm_enabled" \
  CASA_RS_BENCH_PSTERM="$psterm_enabled" \
  CASA_RS_BENCH_WBAWP="$wbawp_enabled" \
  CASA_RS_BENCH_CONJBEAMS="$conjbeams_enabled" \
  CASA_RS_BENCH_COMPUTEPASTEP="$computepastep" \
  CASA_RS_BENCH_ROTATEPASTEP="$rotatepastep" \
  CASA_RS_BENCH_POINTINGOFFSETSIGDEV="$pointingoffsetsigdev" \
  CASA_RS_BENCH_MOSWEIGHT="$mosweight_enabled" \
  CASA_RS_BENCH_NORMTYPE="$normtype" \
  CASA_RS_BENCH_USEPOINTING="$usepointing_enabled" \
  CASA_RS_BENCH_IMSIZE="$imsize" \
  CASA_RS_BENCH_CELL_ARCSEC="$cell_arcsec" \
  CASA_RS_BENCH_WEIGHTING="$weighting" \
  CASA_RS_BENCH_ROBUST="$robust" \
  CASA_RS_BENCH_PERCHANWEIGHTDENSITY="$perchanweightdensity_enabled" \
  CASA_RS_BENCH_DECONVOLVER="$deconvolver" \
  CASA_RS_BENCH_NTERMS="$nterms" \
  CASA_RS_BENCH_SCALES="$scales" \
  CASA_RS_BENCH_SMALLSCALEBIAS="$smallscalebias" \
  CASA_RS_BENCH_NITER="$casa_niter" \
  CASA_RS_BENCH_GAIN="$gain" \
  CASA_RS_BENCH_THRESHOLD_JY="$threshold_jy" \
  CASA_RS_BENCH_NSIGMA="$nsigma" \
  CASA_RS_BENCH_PSFCUTOFF="$psfcutoff" \
  CASA_RS_BENCH_PBLIMIT="$pblimit" \
  CASA_RS_BENCH_PBCOR="$pbcor_enabled" \
  CASA_RS_BENCH_MINOR_CYCLE_LENGTH="$minor_cycle_length" \
  CASA_RS_BENCH_CYCLEFACTOR="$cyclefactor" \
  CASA_RS_BENCH_MIN_PSFFRACTION="$min_psf_fraction" \
  CASA_RS_BENCH_MAX_PSFFRACTION="$max_psf_fraction" \
  CASA_RS_BENCH_INTERPOLATION="$interpolation" \
  CASA_RS_BENCH_RESTORATION="$restoration_enabled" \
  CASA_RS_BENCH_RESTORINGBEAM="$restoringbeam" \
  CASA_RS_BENCH_INTERACTIVE="$interactive_enabled" \
  CASA_RS_BENCH_USEMASK="$usemask" \
  CASA_RS_BENCH_MASK_IMAGE="$mask_image" \
  CASA_RS_BENCH_RESTART="$restart_enabled" \
  CASA_RS_BENCH_SAVEMODEL="$savemodel" \
  CASA_RS_BENCH_CALCRES="$calcres_enabled" \
  CASA_RS_BENCH_CALCPSF="$calcpsf_enabled" \
    "$CASA_RS_CASA_PYTHON" "$repo_root/tools/perf/imager/casa_phase_bench.py" | sed 's/^/  /'
else
  echo "CASA PySynthesisImager stage medians (milliseconds):"
  echo "  skipped; set IMAGER_BENCH_PHASE_PROBE=1 for CASA phase diagnostics"
fi
