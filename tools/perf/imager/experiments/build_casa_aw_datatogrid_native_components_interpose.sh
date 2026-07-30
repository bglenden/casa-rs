#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
exact_checkout="${CASA_67518_SOURCE_CHECKOUT:-/tmp/casa-6.7.5.18-oracle-source}"
local_casatools="${CASA_LOCAL_HEADER_ROOT:-/Users/brianglendenning/SoftwareProjects/casa/casatools}"
official_lib_dir="${CASA_67518_LIBRARY_DIR:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib}"
dependency_include="${CASA_DEPENDENCY_INCLUDE:-/opt/homebrew/include}"
output="${CASA_AW_NATIVE_COMPONENTS_INTERPOSER:-${TMPDIR:-/tmp}/casa_aw_datatogrid_native_components_interpose.dylib}"
expected_casa_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"
expected_casacore_commit="25b653f6963a78a1dcfc8e16954081e091a50fbe"
exact_casatools="${exact_checkout}/casatools"
local_casacore="${local_casatools}/casacore"
official_synthesis="${official_lib_dir}/libcasacpp_synthesis.6.dylib"
dcomplex_symbol="__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIdEEEEvRN8casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb"
complex_symbol="__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIfEEEEvRN8casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb"

fail() {
    echo "CASA native-component interposer build: $*" >&2
    exit 1
}

[[ "$(uname -s)" == "Darwin" ]] || fail "the interposer is macOS-only"
actual_casa_commit="$(git -C "${exact_checkout}" rev-parse HEAD 2>/dev/null)" ||
    fail "missing exact CASA 6.7.5.18 checkout: ${exact_checkout}"
[[ "${actual_casa_commit}" == "${expected_casa_commit}" ]] ||
    fail "CASA source checkout is not exact 6.7.5.18 commit ${expected_casa_commit}"
git -C "${exact_checkout}" diff --quiet --ignore-submodules=dirty ||
    fail "CASA source checkout has unstaged tracked changes"
git -C "${exact_checkout}" diff --cached --quiet --ignore-submodules=dirty ||
    fail "CASA source checkout has staged tracked changes"
[[ -f "${exact_casatools}/src/code/synthesis/TransformMachines2/AWVisResampler.h" ]] ||
    fail "missing exact AWVisResampler header under ${exact_casatools}"
actual_casacore_commit="$(git -C "${local_casacore}" rev-parse HEAD 2>/dev/null)" ||
    fail "missing casacore source checkout under ${local_casacore}"
[[ "${actual_casacore_commit}" == "${expected_casacore_commit}" ]] ||
    fail "casacore source checkout is not exact commit ${expected_casacore_commit}"
git -C "${local_casacore}" diff --quiet ||
    fail "casacore source checkout has unstaged tracked changes"
git -C "${local_casacore}" diff --cached --quiet ||
    fail "casacore source checkout has staged tracked changes"
[[ -f "${local_casatools}/include/casacore/casa/config.h" ]] ||
    fail "missing generated casacore config header under ${local_casatools}"
[[ -f "${official_synthesis}" ]] ||
    fail "missing official CASA 6.7.5.18 synthesis dylib: ${official_synthesis}"

symbols="$(nm -gU "${official_synthesis}")"
grep -Fq "${dcomplex_symbol}" <<<"${symbols}" ||
    fail "official CASA dylib does not export the expected DComplex symbol"
grep -Fq "${complex_symbol}" <<<"${symbols}" ||
    fail "official CASA dylib does not export the expected Complex symbol"

clang++ \
    -std=c++17 \
    -O2 \
    -Wall \
    -Wextra \
    -Werror \
    -Wno-error=deprecated-declarations \
    -Wno-error=inconsistent-missing-override \
    -fvisibility=hidden \
    -dynamiclib \
    -Wl,-undefined,dynamic_lookup \
    -I"${exact_casatools}/src/code" \
    -I"${local_casatools}/include" \
    -I"${local_casatools}/casacore" \
    -I"${local_casatools}/src/code" \
    -I"${dependency_include}" \
    "${experiment_dir}/casa_aw_datatogrid_native_components_interpose.cc" \
    "${official_synthesis}" \
    -Wl,-rpath,"${official_lib_dir}" \
    -o "${output}"

install_name_tool \
    -change "libcasacpp_synthesis.6.dylib" \
    "@rpath/libcasacpp_synthesis.6.dylib" \
    "${output}" 2>/dev/null || true
codesign --force --sign - "${output}" >/dev/null

built_exports="$(nm -gU "${output}")"
grep -Fq "_casa_aw_datatogrid_native_components_ready_v1" <<<"${built_exports}" ||
    fail "built interposer does not export its v1 readiness marker"
built_inventory="$(nm -g "${output}")"
grep -Fq "${dcomplex_symbol}" <<<"${built_inventory}" ||
    fail "built interposer does not bind the exact DComplex symbol"
grep -Fq "${complex_symbol}" <<<"${built_inventory}" ||
    fail "built interposer does not bind the exact Complex symbol"
built_bindings="$(nm -m "${output}")"
grep -Fq "${dcomplex_symbol} (from libcasacpp_synthesis)" <<<"${built_bindings}" ||
    fail "built DComplex import is not two-level-bound to CASA synthesis"
grep -Fq "${complex_symbol} (from libcasacpp_synthesis)" <<<"${built_bindings}" ||
    fail "built Complex import is not two-level-bound to CASA synthesis"

file "${output}"
otool -L "${output}"
echo "${output}"
