#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
exact_checkout="${CASA_67518_SOURCE_CHECKOUT:-/tmp/casa-6.7.5.18-oracle-source}"
local_casatools="${CASA_LOCAL_HEADER_ROOT:-/Users/brianglendenning/SoftwareProjects/casa/casatools}"
official_lib_dir="${CASA_67518_LIBRARY_DIR:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib}"
dependency_include="${CASA_DEPENDENCY_INCLUDE:-/opt/homebrew/include}"
output="${CASA_AW_DEGRID_PREFIX_INTERPOSER:-${TMPDIR:-/tmp}/casa_aw_degrid_prefix_interpose.dylib}"
expected_casa_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"
expected_casacore_commit="25b653f6963a78a1dcfc8e16954081e091a50fbe"
exact_casatools="${exact_checkout}/casatools"
local_casacore="${local_casatools}/casacore"
official_synthesis="${official_lib_dir}/libcasacpp_synthesis.6.dylib"
original_symbol="__ZN4casa5refim14AWVisResampler10GridToDataERNS0_7VBStoreERKN8casacore5ArrayINSt3__17complexIfEEEE"
traced_symbol="__ZN4casa5refim14AWVisResampler16GridToDataTracedERNS0_7VBStoreERKN8casacore5ArrayINSt3__17complexIfEEEE"

fail() {
    echo "CASA AW degrid-prefix interposer build: $*" >&2
    exit 1
}

[[ "$(uname -s)" == "Darwin" ]] || fail "the interposer is macOS-only"
actual_casa_commit="$(git -C "${exact_checkout}" rev-parse HEAD 2>/dev/null)" ||
    fail "missing exact CASA 6.7.5.18 checkout: ${exact_checkout}"
[[ "${actual_casa_commit}" == "${expected_casa_commit}" ]] ||
    fail "CASA source checkout is not exact 6.7.5.18 commit ${expected_casa_commit}"
git -C "${exact_checkout}" diff --quiet --ignore-submodules=dirty ||
    fail "exact CASA source checkout has unstaged tracked changes"
git -C "${exact_checkout}" diff --cached --quiet --ignore-submodules=dirty ||
    fail "exact CASA source checkout has staged tracked changes"
actual_casacore_commit="$(git -C "${local_casacore}" rev-parse HEAD 2>/dev/null)" ||
    fail "missing casacore source checkout under ${local_casacore}"
[[ "${actual_casacore_commit}" == "${expected_casacore_commit}" ]] ||
    fail "casacore source checkout is not exact commit ${expected_casacore_commit}"
git -C "${local_casacore}" diff --quiet ||
    fail "casacore source checkout has unstaged tracked changes"
git -C "${local_casacore}" diff --cached --quiet ||
    fail "casacore source checkout has staged tracked changes"
[[ -f "${official_synthesis}" ]] ||
    fail "missing official CASA synthesis dylib: ${official_synthesis}"
official_symbols="$(nm -gU "${official_synthesis}")"
grep -Fq "${original_symbol}" <<<"${official_symbols}" ||
    fail "official CASA dylib does not export the expected GridToData symbol"

build_root="$(mktemp -d "${TMPDIR:-/tmp}/casa-aw-prefix.XXXXXX")"
trap 'rm -rf "${build_root}"' EXIT
patched_root="${build_root}/patched"
patched_transform="${patched_root}/casatools/src/code/synthesis/TransformMachines2"
mkdir -p "${patched_transform}"
cp \
    "${exact_casatools}/src/code/synthesis/TransformMachines2/AWVisResampler.cc" \
    "${patched_transform}/AWVisResampler.cc"
cp \
    "${exact_casatools}/src/code/synthesis/TransformMachines2/accumulateFromGrid.inc" \
    "${patched_transform}/accumulateFromGrid.inc"
patch -s -d "${patched_root}" -p1 <"${experiment_dir}/casa_aw_degrid_prefix_trace.patch"

clang++ \
    -std=c++17 \
    -O2 \
    -Wall \
    -Wextra \
    -Werror \
    '-Wno-error=#warnings' \
    -Wno-error=deprecated-declarations \
    -Wno-error=inconsistent-missing-override \
    -Wno-error=unused-parameter \
    -Wno-error=unused-variable \
    -DGridToData=GridToDataTraced \
    -fvisibility=hidden \
    -c \
    -I"${patched_root}/casatools/src/code" \
    -I"${exact_casatools}/src/code" \
    -I"${local_casatools}/include" \
    -I"${local_casatools}/casacore" \
    -I"${local_casatools}/src/code" \
    -I"${dependency_include}" \
    "${patched_transform}/AWVisResampler.cc" \
    -o "${build_root}/AWVisResampler.o"

object_symbols="$(nm -gU "${build_root}/AWVisResampler.o")"
grep -Fq "${traced_symbol}" <<<"${object_symbols}" ||
    fail "patched object does not define the expected GridToDataTraced symbol"

clang++ \
    -std=c++17 \
    -O2 \
    -Wall \
    -Wextra \
    -Werror \
    '-Wno-error=#warnings' \
    -Wno-error=deprecated-declarations \
    -Wno-error=inconsistent-missing-override \
    -Wno-error=unused-parameter \
    -Wno-error=unused-variable \
    -fvisibility=hidden \
    -dynamiclib \
    -Wl,-undefined,dynamic_lookup \
    -I"${exact_casatools}/src/code" \
    -I"${local_casatools}/include" \
    -I"${local_casatools}/casacore" \
    -I"${local_casatools}/src/code" \
    -I"${dependency_include}" \
    "${experiment_dir}/casa_aw_degrid_prefix_interpose.cc" \
    "${build_root}/AWVisResampler.o" \
    "${official_synthesis}" \
    -Wl,-rpath,"${official_lib_dir}" \
    -o "${output}"

install_name_tool \
    -change "libcasacpp_synthesis.6.dylib" \
    "@rpath/libcasacpp_synthesis.6.dylib" \
    "${output}" 2>/dev/null || true
codesign --force --sign - "${output}" >/dev/null

built_exports="$(nm -gU "${output}")"
grep -Fq "_casa_aw_degrid_prefix_oracle_ready_v1" <<<"${built_exports}" ||
    fail "built interposer does not export its v1 readiness marker"
all_symbols="$(nm "${output}")"
grep -Fq "${traced_symbol}" <<<"${all_symbols}" ||
    fail "built interposer does not contain the traced replacement"
built_bindings="$(nm -m "${output}")"
grep -Fq "${original_symbol} (from libcasacpp_synthesis)" <<<"${built_bindings}" ||
    fail "original GridToData import is not bound to CASA synthesis"
interpose_layout="$(otool -l "${output}")"
grep -Fq "sectname __interpose" <<<"${interpose_layout}" ||
    fail "built dylib lacks a dyld interpose section"
grep -Fq "size 0x0000000000000010" <<<"${interpose_layout}" ||
    fail "dyld interpose section does not contain exactly one arm64 entry"

file "${output}"
otool -L "${output}"
echo "${output}"
