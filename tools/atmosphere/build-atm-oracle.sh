#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
# Build the ATM C++ oracle from CASA's sources and regenerate the fixtures
# casa-atmosphere's tests compare against.
#
#   tools/atmosphere/build-atm-oracle.sh ~/SoftwareProjects/casa/casatools/src/code/atmosphere/ATM
#   tools/atmosphere/build-atm-oracle.sh <ATM directory> --time 1024
#   tools/atmosphere/build-atm-oracle.sh <ATM directory> --time-profile 2048
#   tools/atmosphere/build-atm-oracle.sh <ATM directory> --as-shipped --out <directory>
#   tools/atmosphere/build-atm-oracle.sh <ATM directory> [--as-shipped] --binary <path>
#
# The fixtures are committed, so CI needs neither CASA nor a C++ compiler.
# Rebuild them only when the oracle's inputs, the corrections or CASA's ATM
# change.
# The oracle compiles a copy of the sources with `atm-corrections.patch`
# applied, the corrections casa-atmosphere makes to ATM's data errors
# (`docs/CASA (C++) bugs.md`); `extract_atm_tables.py` reads the same copy.
# `--as-shipped` compiles ATM as CASA ships it instead, to measure what the
# corrections change; it writes its fixtures to `--out`, never over the
# committed ones.
# FP contraction is off so the C++ rounds each operation as the Rust does.
# With `--time N` it times the species refractivities for N channels
# instead (compare `cargo run --release -p casa-atmosphere --example
# refractivity_timing -- N`), and with `--time-profile N` building a
# RefractiveIndexProfile (compare the `profile_timing` example). With
# `--binary PATH` it keeps the oracle at PATH and writes nothing else:
# `at_tool_check.py` runs both builds' `--at-tool` mode.
set -euo pipefail

usage() {
  echo "usage: $0 <CASA ATM source directory> [--as-shipped]" \
    "[--out <directory> | --binary <path> | --time <channels> | --time-profile <channels>]" >&2
  exit 2
}
[[ $# -ge 1 ]] || usage
atm=$(cd "$1" && pwd)
shift
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out="$root/crates/casa-atmosphere/tests/data"
as_shipped=0
out_given=0
binary=""
timing=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --as-shipped) as_shipped=1; shift ;;
    --out) [[ $# -ge 2 ]] || usage; out=$2; out_given=1; shift 2 ;;
    --binary) [[ $# -ge 2 && -n $2 ]] || usage; binary=$2; shift 2 ;;
    --time | --time-profile) [[ $# -ge 2 && -z $timing ]] || usage; timing="$1 $2"; shift 2 ;;
    *) usage ;;
  esac
done
# One of the fixtures, the binary or a timing; an as-shipped build never
# writes the committed fixtures.
modes=$((out_given + (${#binary} > 0) + (${#timing} > 0)))
if [[ $modes -gt 1 ]] || [[ $as_shipped -eq 1 && $modes -eq 0 ]]; then
  usage
fi
build=$(mktemp -d "${TMPDIR:-/tmp}/atm-oracle.XXXXXX")
trap 'rm -rf "$build"' EXIT

mkdir "$build/atm"
cp "$atm"/*.cc "$atm"/*.h "$build/atm/"
if [[ $as_shipped -eq 0 ]]; then
  # Exactly as written: no fuzz, and never in reverse.
  patch -N -s -p1 -F0 -d "$build/atm" -i "$here/atm-corrections.patch" < /dev/null
fi
flags=(-std=c++17 -O2 -ffp-contract=off -w -I"$build/atm")
for source in "$build/atm"/*.cc "$here"/atm_oracle.cc "$here"/atm_oracle_sky.cc "$here"/atm_oracle_at_tool.cc; do
  clang++ "${flags[@]}" -c "$source" -o "$build/$(basename "$source" .cc).o" &
done
wait
clang++ "${flags[@]}" "$build"/*.o -o "$build/atm_oracle"
if [[ -n $binary ]]; then
  cp "$build/atm_oracle" "$binary"
  echo "wrote $binary"
  exit 0
fi
if [[ -n $timing ]]; then
  # shellcheck disable=SC2086 # "--time N" is two arguments
  "$build/atm_oracle" $timing
  exit 0
fi
mkdir -p "$out"
"$build/atm_oracle" "$out"
echo "wrote $(ls "$out" | tr '\n' ' ')to $out"
