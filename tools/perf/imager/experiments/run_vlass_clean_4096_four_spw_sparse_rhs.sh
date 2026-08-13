#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
binary="${CASA_RS_VLASS_EXPERIMENT_BINARY:-$repo_root/target/release/casars-imager}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-$HOME/.casa/data}"
ms="$root/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
output="$root/artifacts/products/vlass-production-clean-4096-four-spw-sparse-rhs-v1/rust"
log="$root/receipts/runs/20260729-vlass-production-clean-4096-four-spw-sparse-rhs-v1.log"
cf_cache="$root/cf-cache/6.7.5.18/single-field-4096-four-spw"
mask="$root/masks/vlass-single-field-peak-box-4096.mask"

for required in "$binary" "$measures_dir" "$ms" "$cf_cache" "$mask"; do
    if [[ ! -e "$required" ]]; then
        echo "required sparse-RHS experiment input does not exist: $required" >&2
        exit 2
    fi
done
if [[ -e "$output.image.tt0" || -e "$log" ]]; then
    echo "refusing to overwrite existing sparse-RHS experiment products" >&2
    exit 2
fi

mkdir -p "$(dirname "$output")" "$(dirname "$log")"
/usr/bin/time -p env -i \
    PATH=/usr/bin:/bin \
    HOME="$HOME" \
    CASA_RS_MEASURESPATH="$measures_dir" \
    CASA_RS_FFTW_LIBRARY_DIR="$fftw_dir" \
    DYLD_LIBRARY_PATH="$fftw_dir" \
    CASA_RS_FFTW_THREADS=1 \
    CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1 \
    CASA_RS_EXPERIMENTAL_MT_MFS_SPARSE_RHS=1 \
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
    --standard-mfs-acceleration cpu \
    --imaging-fft-precision f64 \
    --imaging-fft-backend rustfft \
    --no-parallel \
    --standard-mfs-grid-threads 1 \
    --imaging-memory-target-mb 16384 \
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
    --savemodel none \
    --restoringbeam common \
    --mask-image "$mask" \
    --no-preview-pngs \
    >"$log" 2>&1

echo "$log"
