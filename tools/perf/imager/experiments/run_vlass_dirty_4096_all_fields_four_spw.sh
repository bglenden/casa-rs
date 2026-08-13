#!/usr/bin/env bash
set -euo pipefail

root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
binary="${CASA_RS_VLASS_EXPERIMENT_BINARY:?set CASA_RS_VLASS_EXPERIMENT_BINARY to the frozen release casars-imager}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-$HOME/.casa/data}"
fftw_dir="${CASA_RS_VLASS_FFTW_LIBRARY_DIR:-/opt/homebrew/opt/fftw/lib}"
cf_cache="${CASA_RS_VLASS_CF_CACHE:-$root/cf-cache/6.7.5.9/db96e297401b0f5c90f1494844fd9a1d49ad5023be44987ce7076afac513d856}"
run_id="${CASA_RS_VLASS_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-vlass-all63-dirty-4096-four-spw}"
run_root="${CASA_RS_VLASS_RUN_ROOT:-$root/recovery-candidates/runs/$run_id}"
ms="$root/data/frozen-clean-b80d5e87487a/VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms"
output="$run_root/rust"
log="$run_root/casa-rs.log"
provenance="$run_root/provenance.txt"

for required in "$binary" "$measures_dir" "$fftw_dir" "$cf_cache" "$ms"; do
    if [[ ! -e "$required" ]]; then
        echo "required matched-row input does not exist: $required" >&2
        exit 2
    fi
done
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
    printf 'field\t%s\n' '1107~1127,1512~1532,1542~1562'
    printf 'phasecenter_field\t%s\n' '1525'
    printf 'spw\t%s\n' '2,7,12,17'
    printf 'imsize\t%s\n' '4096'
    printf 'niter\t%s\n' '0'
    printf 'execution\t%s\n' 'release-metal-grid2-fftw-f64-t8-memory-auto-16GiB'
} >"$provenance"

set +e
/usr/bin/time -lp env -i \
    PATH=/opt/homebrew/bin:/usr/bin:/bin \
    HOME="$HOME" \
    CASA_RS_MEASURESPATH="$measures_dir" \
    CASA_RS_FFTW_LIBRARY_DIR="$fftw_dir" \
    DYLD_LIBRARY_PATH="$fftw_dir" \
    CASA_RS_FFTW_THREADS=8 \
    CASA_RS_STANDARD_MFS_PROFILE_DETAIL=1 \
    "$binary" \
    --ms "$ms" \
    --imagename "$output" \
    --imsize 4096 \
    --cell-arcsec 0.6 \
    --field '1107~1127,1512~1532,1542~1562' \
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
    --niter 0 \
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
    --savemodel none \
    --restoringbeam common \
    --dirty-only \
    --no-preview-pngs \
    >"$log" 2>&1
status=$?
set -e

printf 'exit_status\t%s\n' "$status" >>"$provenance"
printf 'log_sha256\t' >>"$provenance"
/usr/bin/shasum -a 256 "$log" | /usr/bin/awk '{print $1}' >>"$provenance"
printf '%s\n' "$run_root"
exit "$status"
