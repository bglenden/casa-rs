#!/bin/zsh
# SPDX-License-Identifier: LGPL-3.0-or-later
# Simulation Phase 1d no-regression benchmark (#841): today's simulator.
#
#   tools/simulation/bench_old_simulator.sh [bench_old_simulator.py options]
#
# The defaults are the bg-mac-mini's layout; override them with
#   CASA_PY        CASA's python3
#   OLD_BIN        simobserve built at e73ae19af2 (bench_old_simulator.json)
#   OLD_MANIFESTS  the campaign's logs directory, holding <case>.casa-manifest.json
#   OLD_OUT        the output directory (requests, model FITS, logs, results)
#   OLD_REFERENCE  a campaign results.json to set beside (default: the campaign's, if present)
#   OLD_NAS        where the summary is copied (default: the NAS's
#                  casa-rs/issue-841/oldsim-<date> when /Volumes/home is mounted)
#
# Build the binary in its own worktree and target directory:
#   git -C ~/ext/SoftwareProjects/casa-rs worktree add --detach \
#       ~/ext/SoftwareProjects/casa-rs-data/oldsim-src e73ae19af2
#   cd ~/ext/SoftwareProjects/casa-rs-data/oldsim-src && \
#     PKG_CONFIG_PATH=~/ext/DeveloperTools/fftw-3.3.11-simd/lib/pkgconfig \
#     CARGO_TARGET_DIR=~/ext/SoftwareProjects/casa-rs-data/target-oldsim CARGO_INCREMENTAL=0 \
#     cargo build --release -p casa-ms --bin simobserve
#
# Launch detached, with the option set in the shell that backgrounds the job
# (a background job otherwise runs at nice 5):
#   zsh -lc 'setopt no_bg_nice; nohup zsh tools/simulation/bench_old_simulator.sh \
#       > ~/ext/SoftwareProjects/casa-rs-data/oldsim/oldsim.log 2>&1 < /dev/null &'
setopt no_bg_nice
HERE=${0:A:h}
DATA=$HOME/ext/SoftwareProjects/casa-rs-data
CASA_PY=${CASA_PY:-$HOME/ext/DeveloperTools/CASA.app/Contents/MacOS/python3}
OLD_BIN=${OLD_BIN:-$DATA/target-oldsim/release/simobserve}
OLD_MANIFESTS=${OLD_MANIFESTS:-$DATA/bench1d/logs}
OLD_OUT=${OLD_OUT:-$DATA/oldsim}
OLD_REFERENCE=${OLD_REFERENCE:-$DATA/bench1d/results.json}
if [[ -z $OLD_NAS && -d /Volumes/home/casa-rs ]]; then
  OLD_NAS=/Volumes/home/casa-rs/issue-841/oldsim-$(date +%Y%m%d-%H%M)
fi
for need in $CASA_PY $OLD_BIN; do
  if [[ ! -x $need ]]; then
    print -u2 "missing: $need"
    exit 1
  fi
done
echo "nice $(ps -o nice= -p $$), host $(hostname), $(date)"
EXTRA=()
[[ -n $OLD_NAS ]] && EXTRA+=(--nas-dir $OLD_NAS)
[[ -f $OLD_REFERENCE ]] && EXTRA+=(--reference-results $OLD_REFERENCE)
exec /usr/bin/python3 -u $HERE/bench_old_simulator.py --manifests $OLD_MANIFESTS --out $OLD_OUT \
  --bin $OLD_BIN --casa-python $CASA_PY $EXTRA "$@"
