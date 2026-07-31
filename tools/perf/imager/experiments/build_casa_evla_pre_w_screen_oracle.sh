#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
exact_checkout="${CASA_67518_SOURCE_CHECKOUT:-/tmp/casa-6.7.5.18-oracle-source}"
local_casatools="${CASA_LOCAL_HEADER_ROOT:-/Users/brianglendenning/SoftwareProjects/casa/casatools}"
casa_lib_dir="${CASA_67518_LIBRARY_DIR:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib}"
dependency_include="${CASA_DEPENDENCY_INCLUDE:-/opt/homebrew/include}"
output="${CASA_EVLA_PRE_W_SCREEN_ORACLE_BIN:-${TMPDIR:-/tmp}/casa_evla_pre_w_screen_oracle}"
expected_casa_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"

fail() {
    echo "CASA EVLA pre-W screen oracle build: $*" >&2
    exit 1
}

[[ "$(uname -s)" == "Darwin" ]] || fail "the oracle is macOS-only"
actual_casa_commit="$(git -C "${exact_checkout}" rev-parse HEAD 2>/dev/null)" ||
    fail "missing exact CASA 6.7.5.18 checkout: ${exact_checkout}"
[[ "${actual_casa_commit}" == "${expected_casa_commit}" ]] ||
    fail "CASA source checkout is not exact 6.7.5.18 commit ${expected_casa_commit}"
[[ -f "${exact_checkout}/casatools/src/code/synthesis/TransformMachines2/EVLAAperture.h" ]] ||
    fail "missing exact EVLAAperture header"
[[ -f "${local_casatools}/include/casacore/casa/config.h" ]] ||
    fail "missing generated casacore config header under ${local_casatools}"
[[ -f "${casa_lib_dir}/libcasacpp_synthesis.6.dylib" ]] ||
    fail "missing CASA synthesis dylib under ${casa_lib_dir}"

clang++ \
    -std=c++17 \
    -O2 \
    -Wall \
    -Wextra \
    -Werror \
    -Wno-error=deprecated-declarations \
    -Wno-error=inconsistent-missing-override \
    -Wno-non-c-typedef-for-linkage \
    -I"${exact_checkout}/casatools/src/code" \
    -I"${local_casatools}/include" \
    -I"${local_casatools}/casacore" \
    -I"${local_casatools}/src/code" \
    -I"${dependency_include}" \
    "${experiment_dir}/casa_evla_pre_w_screen_oracle.cc" \
    "${casa_lib_dir}/libcasacpp_synthesis.6.dylib" \
    "${casa_lib_dir}/libcasa_images.9.dylib" \
    "${casa_lib_dir}/libcasa_coordinates.9.dylib" \
    "${casa_lib_dir}/libcasa_lattices.9.dylib" \
    "${casa_lib_dir}/libcasa_scimath.9.dylib" \
    "${casa_lib_dir}/libcasa_measures.9.dylib" \
    "${casa_lib_dir}/libcasa_tables.9.dylib" \
    "${casa_lib_dir}/libcasa_casa.9.dylib" \
    -Wl,-rpath,"${casa_lib_dir}" \
    -o "${output}"

for library in \
    libcasacpp_synthesis.6.dylib \
    libcasa_images.9.dylib \
    libcasa_coordinates.9.dylib \
    libcasa_lattices.9.dylib \
    libcasa_scimath.9.dylib \
    libcasa_measures.9.dylib \
    libcasa_tables.9.dylib \
    libcasa_casa.9.dylib
do
    install_name_tool -change "${library}" "@rpath/${library}" "${output}" 2>/dev/null || true
done

echo "${output}"
