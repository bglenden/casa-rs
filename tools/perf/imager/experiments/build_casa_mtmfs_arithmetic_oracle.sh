#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
experiment_dir="${repo_root}/tools/perf/imager/experiments"
casa_source="${CASA_SOURCE_ROOT:-/Users/brianglendenning/SoftwareProjects/casa/casatools}"
casa_lib_dir="${CASA_LIBRARY_DIR:-/Volumes/GLENDENNING/DeveloperTools/CASA/6.7.5.18-laptop/venv-py312/lib/python3.12/site-packages/casatools/__casac__/lib}"
dependency_include="${CASA_DEPENDENCY_INCLUDE:-/opt/homebrew/include}"
output="${CASA_MTMFS_ORACLE_BIN:-${TMPDIR:-/tmp}/casa_mtmfs_arithmetic_oracle}"

if [[ ! -f "${casa_source}/src/code/synthesis/MeasurementEquations/MatrixCleaner.h" ]]; then
  echo "missing CASA source headers under ${casa_source}" >&2
  exit 1
fi
if [[ ! -f "${casa_source}/include/casacore/casa/config.h" ]]; then
  echo "missing generated casacore config header under ${casa_source}" >&2
  exit 1
fi
if [[ ! -f "${casa_lib_dir}/libcasacpp_synthesis.6.dylib" ]]; then
  echo "missing CASA synthesis dylib under ${casa_lib_dir}" >&2
  exit 1
fi

clang++ \
  -std=c++17 \
  -O2 \
  -Wall \
  -Wextra \
  -Werror \
  -I"${casa_source}/include" \
  -I"${casa_source}/casacore" \
  -I"${casa_source}/src/code" \
  -I"${dependency_include}" \
  "${experiment_dir}/casa_mtmfs_arithmetic_oracle.cc" \
  "${casa_lib_dir}/libcasacpp_synthesis.6.dylib" \
  "${casa_lib_dir}/libcasa_images.9.dylib" \
  "${casa_lib_dir}/libcasa_lattices.9.dylib" \
  "${casa_lib_dir}/libcasa_scimath.9.dylib" \
  "${casa_lib_dir}/libcasa_tables.9.dylib" \
  "${casa_lib_dir}/libcasa_casa.9.dylib" \
  -Wl,-rpath,"${casa_lib_dir}" \
  -o "${output}"

for library in \
  libcasacpp_synthesis.6.dylib \
  libcasa_images.9.dylib \
  libcasa_lattices.9.dylib \
  libcasa_scimath.9.dylib \
  libcasa_tables.9.dylib \
  libcasa_casa.9.dylib
do
  install_name_tool -change "${library}" "@rpath/${library}" "${output}"
done

echo "${output}"
