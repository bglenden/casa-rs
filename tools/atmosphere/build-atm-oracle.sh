#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
# Build the ATM C++ oracle from CASA's sources and regenerate the fixtures
# casa-atmosphere's tests compare against.
#
#   tools/atmosphere/build-atm-oracle.sh ~/SoftwareProjects/casa/casatools/src/code/atmosphere/ATM
#   tools/atmosphere/build-atm-oracle.sh <ATM directory> --time 1024
#
# The fixtures are committed, so CI needs neither CASA nor a C++ compiler.
# Rebuild them only when the oracle's inputs change or CASA's ATM changes.
# FP contraction is off so the C++ rounds each operation as the Rust does.
# With `--time N` it times the species refractivities for N channels
# instead (compare `cargo run --release -p casa-atmosphere --example
# refractivity_timing -- N`).
set -euo pipefail

if [[ $# -ne 1 && !( $# -eq 3 && $2 == --time ) ]]; then
  echo "usage: $0 <CASA ATM source directory> [--time <channels>]" >&2
  exit 2
fi
atm=$(cd "$1" && pwd)
here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)
out="$root/crates/casa-atmosphere/tests/data"
build=$(mktemp -d "${TMPDIR:-/tmp}/atm-oracle.XXXXXX")
trap 'rm -rf "$build"' EXIT

flags=(-std=c++17 -O2 -ffp-contract=off -w -I"$atm")
for source in "$atm"/*.cc; do
  clang++ "${flags[@]}" -c "$source" -o "$build/$(basename "$source" .cc).o" &
done
wait
clang++ "${flags[@]}" "$here/atm_oracle.cc" "$build"/*.o -o "$build/atm_oracle"
if [[ $# -eq 3 ]]; then
  "$build/atm_oracle" --time "$3"
  exit 0
fi
mkdir -p "$out"
"$build/atm_oracle" "$out"
echo "wrote $(ls "$out" | tr '\n' ' ')to $out"
