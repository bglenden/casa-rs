#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
# Build the ATM C++ oracle from CASA's sources and regenerate the fixtures
# casa-atmosphere's tests compare against.
#
#   tools/atmosphere/build-atm-oracle.sh ~/SoftwareProjects/casa/casatools/src/code/atmosphere/ATM
#   tools/atmosphere/build-atm-oracle.sh <ATM directory> --time 1024
#   tools/atmosphere/build-atm-oracle.sh <ATM directory> --time-profile 2048
#
# The fixtures are committed, so CI needs neither CASA nor a C++ compiler.
# Rebuild them only when the oracle's inputs change or CASA's ATM changes.
# FP contraction is off so the C++ rounds each operation as the Rust does.
# With `--time N` it times the species refractivities for N channels
# instead (compare `cargo run --release -p casa-atmosphere --example
# refractivity_timing -- N`), and with `--time-profile N` building a
# RefractiveIndexProfile (compare the `profile_timing` example).
set -euo pipefail

if [[ $# -ne 1 && !( $# -eq 3 && ( $2 == --time || $2 == --time-profile ) ) ]]; then
  echo "usage: $0 <CASA ATM source directory> [--time <channels> | --time-profile <channels>]" >&2
  exit 2
fi
atm=$(cd "$1" && pwd)
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out="$root/crates/casa-atmosphere/tests/data"
build=$(mktemp -d "${TMPDIR:-/tmp}/atm-oracle.XXXXXX")
trap 'rm -rf "$build"' EXIT

flags=(-std=c++17 -O2 -ffp-contract=off -w -I"$atm")
for source in "$atm"/*.cc "$here"/atm_oracle.cc "$here"/atm_oracle_sky.cc; do
  clang++ "${flags[@]}" -c "$source" -o "$build/$(basename "$source" .cc).o" &
done
wait
clang++ "${flags[@]}" "$build"/*.o -o "$build/atm_oracle"
if [[ $# -eq 3 ]]; then
  "$build/atm_oracle" "$2" "$3"
  exit 0
fi
mkdir -p "$out"
"$build/atm_oracle" "$out"
echo "wrote $(ls "$out" | tr '\n' ' ')to $out"
