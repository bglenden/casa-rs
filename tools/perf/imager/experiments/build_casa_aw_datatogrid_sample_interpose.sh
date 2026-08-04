#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
exact_checkout="${CASA_67518_SOURCE_CHECKOUT:-/tmp/casa-6.7.5.18-oracle-source}"
local_casatools="${CASA_LOCAL_HEADER_ROOT:-/Users/brianglendenning/SoftwareProjects/casa/casatools}"
official_lib_dir="${CASA_67518_LIBRARY_DIR:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib}"
dependency_include="${CASA_DEPENDENCY_INCLUDE:-/opt/homebrew/include}"
output="${CASA_AW_DATATOGRID_SAMPLE_INTERPOSER:-${TMPDIR:-/tmp}/casa_aw_datatogrid_sample_interpose.dylib}"
expected_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"
exact_casatools="${exact_checkout}/casatools"
official_synthesis="${official_lib_dir}/libcasacpp_synthesis.6.dylib"
dcomplex_symbol="__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIdEEEEvRN8casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb"

[[ "$(git -C "${exact_checkout}" rev-parse HEAD)" == "${expected_commit}" ]] || {
    echo "exact CASA 6.7.5.18 source checkout is missing or changed" >&2
    exit 1
}
[[ -f "${official_synthesis}" ]] || {
    echo "official CASA synthesis library is missing" >&2
    exit 1
}
symbol_inventory="$(nm -gU "${official_synthesis}")"
grep -Fq "${dcomplex_symbol}" <<<"${symbol_inventory}" || {
    echo "official CASA library does not export DComplex DataToGrid" >&2
    exit 1
}

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
    "${experiment_dir}/casa_aw_datatogrid_sample_interpose.cc" \
    "${official_synthesis}" \
    -Wl,-rpath,"${official_lib_dir}" \
    -o "${output}"

install_name_tool \
    -change "libcasacpp_synthesis.6.dylib" \
    "@rpath/libcasacpp_synthesis.6.dylib" \
    "${output}" 2>/dev/null || true
codesign --force --sign - "${output}" >/dev/null
built_exports="$(nm -gU "${output}")"
grep -Fq "_casa_aw_datatogrid_sample_ready_v1" <<<"${built_exports}"
interpose_layout="$(otool -l "${output}")"
grep -Fq "sectname __interpose" <<<"${interpose_layout}"
file "${output}"
echo "${output}"
