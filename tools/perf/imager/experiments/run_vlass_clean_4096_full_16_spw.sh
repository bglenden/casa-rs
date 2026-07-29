#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
binary="${CASA_RS_VLASS_EXPERIMENT_BINARY:-$repo_root/target/release/casars-imager}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
fftw_threads="${CASA_RS_VLASS_FFTW_THREADS:-1}"
grid_threads="${CASA_RS_VLASS_GRID_THREADS:-2}"
standard_mfs_acceleration="${CASA_RS_VLASS_STANDARD_MFS_ACCELERATION:-metal}"
replay_retention_bytes="${CASA_RS_VLASS_REPLAY_RETENTION_BYTES:-4294967296}"
niter="${CASA_RS_VLASS_NITER:-2000}"
image_response_cache="${CASA_RS_VLASS_IMAGE_RESPONSE_CACHE:-0}"
radix_madfm="${CASA_RS_VLASS_RADIX_MADFM:-0}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-$HOME/.casa/data}"
ms="$root/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
cf_cache="$root/cf-cache/6.7.5.18/single-field-4096-full-16-spw"
mask="$root/masks/vlass-single-field-peak-box-4096.mask"
experimental_environment=(CASA_RS_VLASS_EXPERIMENT_RUNNER=1)
parallel_argument=(--no-parallel)
label="vlass-production-clean-4096-full-16-spw-fftw-t${fftw_threads}-gridt${grid_threads}-niter${niter}"

if [[ "$image_response_cache" == "1" ]]; then
    label="${label}-image-response-cache"
    experimental_environment+=(
        CASA_RS_EXPERIMENTAL_AWPROJECT_IMAGE_RESPONSE_CACHE=1
    )
elif [[ "$image_response_cache" != "0" ]]; then
    echo "CASA_RS_VLASS_IMAGE_RESPONSE_CACHE must be 0 or 1" >&2
    exit 2
fi
if [[ "$radix_madfm" == "1" ]]; then
    label="${label}-radix-madfm"
    experimental_environment+=(CASA_RS_EXPERIMENTAL_RADIX_MADFM=1)
elif [[ "$radix_madfm" != "0" ]]; then
    echo "CASA_RS_VLASS_RADIX_MADFM must be 0 or 1" >&2
    exit 2
fi
case "$standard_mfs_acceleration" in
    cpu)
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
log="$root/receipts/runs/20260729-$label.log"

case "$fftw_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_FFTW_THREADS must be a positive integer" >&2
        exit 2
        ;;
esac
case "$grid_threads" in
    ''|*[!0-9]*|0)
        echo "CASA_RS_VLASS_GRID_THREADS must be a positive integer" >&2
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

echo "$log"
