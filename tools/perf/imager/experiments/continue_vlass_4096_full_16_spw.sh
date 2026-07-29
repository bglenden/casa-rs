#!/usr/bin/env bash
set -euo pipefail

root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
casa_receipt="$root/receipts/runs/20260729-vlass-reduced-casa-clean-4096-full-16-spw.json"
casa_task_log="$root/receipts/runs/20260729-vlass-reduced-casa-clean-4096-full-16-spw.casa.log"
cf_cache="$root/cf-cache/6.7.5.18/single-field-4096-full-16-spw"

for required in "$casa_receipt" "$casa_task_log" "$cf_cache"; do
    if [[ ! -e "$required" ]]; then
        echo "CASA full-16-SPW oracle is incomplete: missing $required" >&2
        exit 1
    fi
done

cfs_count=$(find "$cf_cache" -maxdepth 1 -type d -name 'CFS_*.im' | wc -l | tr -d ' ')
wtcfs_count=$(find "$cf_cache" -maxdepth 1 -type d -name 'WTCFS_*.im' | wc -l | tr -d ' ')
if [[ "$cfs_count" -ne 1024 || "$wtcfs_count" -ne 1024 ]]; then
    echo "full-16-SPW CF cache inventory is incomplete: CFS=$cfs_count WTCFS=$wtcfs_count" >&2
    exit 1
fi

shasum -a 256 "$casa_receipt" "$casa_task_log"
CASA_RS_VLASS_IMAGE_RESPONSE_CACHE=1 \
CASA_RS_VLASS_RADIX_MADFM=1 \
    bash "$(dirname "$0")/run_vlass_clean_4096_full_16_spw.sh"
