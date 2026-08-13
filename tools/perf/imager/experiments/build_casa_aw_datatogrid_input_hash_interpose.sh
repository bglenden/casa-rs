#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
exact_checkout="${CASA_67518_SOURCE_CHECKOUT:-/tmp/casa-6.7.5.18-oracle-source}"
local_casatools="${CASA_LOCAL_HEADER_ROOT:-/Users/brianglendenning/SoftwareProjects/casa/casatools}"
official_lib_dir="${CASA_67518_LIBRARY_DIR:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib}"
dependency_include="${CASA_DEPENDENCY_INCLUDE:-/opt/homebrew/include}"
output="${CASA_AW_INPUT_HASH_INTERPOSER:-${TMPDIR:-/tmp}/casa_aw_datatogrid_input_hash_interpose.dylib}"
expected_commit="418bb1a26df7c4aba663ff123b038b75a6fa0295"
exact_casatools="${exact_checkout}/casatools"
official_synthesis="${official_lib_dir}/libcasacpp_synthesis.6.dylib"

if ! git -C "${exact_checkout}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "missing exact CASA 6.7.5.18 checkout: ${exact_checkout}" >&2
  echo "create it with: git clone --depth 1 --branch 6.7.5.18 https://open-bitbucket.nrao.edu/scm/casa/casa6.git ${exact_checkout}" >&2
  exit 1
fi
actual_commit="$(git -C "${exact_checkout}" rev-parse HEAD)"
if [[ "${actual_commit}" != "${expected_commit}" ]]; then
  echo "CASA source checkout is ${actual_commit}, expected 6.7.5.18 ${expected_commit}" >&2
  exit 1
fi
if [[ ! -f "${exact_casatools}/src/code/synthesis/TransformMachines2/AWVisResampler.h" ]]; then
  echo "missing exact AWVisResampler headers under ${exact_casatools}" >&2
  exit 1
fi
if [[ ! -f "${local_casatools}/include/casacore/casa/config.h" ]]; then
  echo "missing generated casacore config header under ${local_casatools}" >&2
  exit 1
fi
if [[ ! -f "${official_synthesis}" ]]; then
  echo "missing official CASA 6.7.5.18 synthesis dylib: ${official_synthesis}" >&2
  exit 1
fi

dcomplex_symbol="__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIdEEEEvRN8casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb"
complex_symbol="__ZN4casa5refim14AWVisResampler16DataToGridImpl_pINSt3__17complexIfEEEEvRN8casacore5ArrayIT_EERNS0_7VBStoreERNS6_6MatrixIdEERKbb"
symbol_inventory="$(nm -gU "${official_synthesis}")"
if ! grep -Fq "${dcomplex_symbol}" <<<"${symbol_inventory}"; then
  echo "official CASA dylib does not export the expected DComplex DataToGrid symbol" >&2
  exit 1
fi
if ! grep -Fq "${complex_symbol}" <<<"${symbol_inventory}"; then
  echo "official CASA dylib does not export the expected Complex DataToGrid symbol" >&2
  exit 1
fi

clang++ \
  -std=c++17 \
  -O2 \
  -Wall \
  -Wextra \
  -Werror \
  -Wno-deprecated-declarations \
  -Wno-inconsistent-missing-override \
  -fvisibility=hidden \
  -dynamiclib \
  -Wl,-undefined,dynamic_lookup \
  -I"${exact_casatools}/src/code" \
  -I"${local_casatools}/include" \
  -I"${local_casatools}/casacore" \
  -I"${local_casatools}/src/code" \
  -I"${dependency_include}" \
  "${experiment_dir}/casa_aw_datatogrid_input_hash_interpose.cc" \
  "${official_synthesis}" \
  -Wl,-rpath,"${official_lib_dir}" \
  -o "${output}"

install_name_tool \
  -change "libcasacpp_synthesis.6.dylib" \
  "@rpath/libcasacpp_synthesis.6.dylib" \
  "${output}" 2>/dev/null || true
codesign --force --sign - "${output}" >/dev/null

file "${output}"
echo "${output}"
