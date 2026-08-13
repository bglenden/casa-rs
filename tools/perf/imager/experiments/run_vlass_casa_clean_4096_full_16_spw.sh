#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
root="${CASA_RS_VLASS_EXPERIMENT_ROOT:-/Volumes/GLENDENNING/casa-rs-vlass/issue-446}"
casa_python="${CASA_RS_VLASS_CASA_PYTHON:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/bin/python}"
measures_dir="${CASA_RS_VLASS_MEASURES_DIR:-$HOME/.casa/data}"
site_config="$repo_root/tools/perf/imager/experiments/casasiteconfig_vlass.py"
casa_log="$root/receipts/runs/20260729-vlass-reduced-casa-clean-4096-full-16-spw.casa.log"
mpl_config="${CASA_RS_VLASS_MPLCONFIGDIR:-/private/tmp/casa-mpl-vlass}"

for required in "$casa_python" "$measures_dir" "$site_config"; do
    if [[ ! -e "$required" ]]; then
        echo "required CASA oracle runtime input does not exist: $required" >&2
        exit 2
    fi
done

mkdir -p "$(dirname "$casa_log")" "$mpl_config"
env \
    CASASITECONFIG="$site_config" \
    CASA_RS_VLASS_EXPERIMENT_ROOT="$root" \
    CASA_RS_VLASS_MEASURES_DIR="$measures_dir" \
    CASA_RS_VLASS_CASA_LOGFILE="$casa_log" \
    MPLCONFIGDIR="$mpl_config" \
    "$casa_python" \
    "$repo_root/tools/perf/imager/experiments/vlass_reduced_casa_clean_4096_full_16_spw.py"
