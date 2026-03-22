#!/usr/bin/env bash

set -euo pipefail

readonly spdx_header='SPDX-License-Identifier: LGPL-3.0-or-later'

usage() {
  cat <<'EOF'
Usage: scripts/check-spdx.sh [--staged] [path ...]

Checks Rust source files for the required SPDX header.

Modes:
  no arguments  Check all Rust files under crates/ in the working tree
  --staged      Check staged Rust files as they are currently indexed
  path ...      Check the given working-tree paths
EOF
}

mode="worktree"
declare -a files=()
declare -a missing=()

if [[ $# -gt 0 && "$1" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -gt 0 && "$1" == "--staged" ]]; then
  mode="staged"
  shift
fi

if [[ $# -gt 0 && "$mode" == "staged" ]]; then
  echo "check-spdx.sh: do not combine --staged with explicit paths" >&2
  exit 1
fi

if [[ $# -gt 0 ]]; then
  files=("$@")
elif [[ "$mode" == "staged" ]]; then
  while IFS= read -r -d '' path; do
    files+=("$path")
  done < <(git diff --cached --name-only --diff-filter=ACMR -z -- '*.rs')
else
  while IFS= read -r -d '' path; do
    files+=("$path")
  done < <(find crates -type f -name '*.rs' -print0)
fi

if [[ ${#files[@]} -eq 0 ]]; then
  exit 0
fi

for path in "${files[@]}"; do
  if [[ "$mode" == "staged" ]]; then
    if ! grep -q "$spdx_header" < <(git show ":$path"); then
      missing+=("$path")
    fi
    continue
  fi

  if [[ ! -f "$path" ]]; then
    continue
  fi

  if ! grep -q "$spdx_header" "$path"; then
    missing+=("$path")
  fi
done

if [[ ${#missing[@]} -eq 0 ]]; then
  exit 0
fi

echo "Files missing SPDX license header:" >&2
for path in "${missing[@]}"; do
  echo "  $path" >&2
done
echo >&2
echo "Add this first line to each file:" >&2
echo "  // $spdx_header" >&2
exit 1
