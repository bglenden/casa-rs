#!/bin/zsh
# SPDX-License-Identifier: LGPL-3.0-or-later
# Simulation Phase 1d benchmark campaign (#841): CASA's sm against casa-rs.
#
#   tools/simulation/bench_campaign.sh [--smoke] [bench_campaign.py options]
#
# The defaults are the bg-mac-mini's layout; override them with
#   CASA_PY     CASA's python3
#   BENCH_BIN   the release observe_scenarios example
#   BENCH_OUT   the campaign directory (models, logs, results)
#   BENCH_NAS   where each case's summary is copied (default: the NAS's
#               casa-rs/issue-841/bench1d-<date> when /Volumes/home is mounted)
# --smoke runs bench_scenarios_smoke.json with 2 s rests into $BENCH_OUT/smoke.
#
# Launch detached, with the option set in the shell that backgrounds the job
# (a background job otherwise runs at nice 5):
#   zsh -lc 'setopt no_bg_nice; nohup zsh tools/simulation/bench_campaign.sh \
#       > ~/ext/SoftwareProjects/casa-rs-data/bench1d/campaign.log 2>&1 < /dev/null &'
setopt no_bg_nice
HERE=${0:A:h}
CASA_PY=${CASA_PY:-$HOME/ext/DeveloperTools/CASA.app/Contents/MacOS/python3}
BENCH_BIN=${BENCH_BIN:-$HOME/ext/SoftwareProjects/casa-rs-data/target-bench/release/examples/observe_scenarios}
BENCH_OUT=${BENCH_OUT:-$HOME/ext/SoftwareProjects/casa-rs-data/bench1d}
CASES=$HERE/bench_scenarios.json
REST=60
OUT=$BENCH_OUT
if [[ $1 == --smoke ]]; then
  shift
  CASES=$HERE/bench_scenarios_smoke.json
  REST=2
  OUT=$BENCH_OUT/smoke
fi
if [[ -z $BENCH_NAS && -d /Volumes/home/casa-rs ]]; then
  BENCH_NAS=/Volumes/home/casa-rs/issue-841/bench1d-$(date +%Y%m%d-%H%M)
  [[ $CASES == *smoke* ]] && BENCH_NAS=$BENCH_NAS-smoke
fi
for need in $CASA_PY $BENCH_BIN; do
  if [[ ! -x $need ]]; then
    print -u2 "missing: $need"
    exit 1
  fi
done
echo "nice $(ps -o nice= -p $$), host $(hostname), $(date)"
NAS_ARGS=()
[[ -n $BENCH_NAS ]] && NAS_ARGS=(--nas-dir $BENCH_NAS)
exec /usr/bin/python3 -u $HERE/bench_campaign.py --cases $CASES --out $OUT \
  --bin $BENCH_BIN --casa-python $CASA_PY --rest-s $REST $NAS_ARGS "$@"
