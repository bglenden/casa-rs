#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
git_dir="$(git rev-parse --git-dir)"

interval_seconds="${CARGO_SWEEP_INTERVAL_SECONDS:-86400}"
state_file="$git_dir/.cargo-sweep-last-run"
lock_dir="$git_dir/.cargo-sweep-lock"

if ! [[ "$interval_seconds" =~ ^[0-9]+$ ]]; then
  echo "cargo-sweep-if-due: CARGO_SWEEP_INTERVAL_SECONDS must be an integer" >&2
  exit 1
fi

now="$(date +%s)"
last_run=0
if [[ -f "$state_file" ]]; then
  last_run="$(cat "$state_file" 2>/dev/null || echo 0)"
fi

if ! [[ "$last_run" =~ ^[0-9]+$ ]]; then
  last_run=0
fi

if (( now - last_run < interval_seconds )); then
  exit 0
fi

if ! mkdir "$lock_dir" 2>/dev/null; then
  exit 0
fi

cleanup() {
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup EXIT

if ! command -v cargo >/dev/null 2>&1; then
  echo "cargo-sweep-if-due: skipping because cargo is not installed" >&2
  exit 0
fi

if ! cargo sweep --help >/dev/null 2>&1; then
  echo "cargo-sweep-if-due: skipping because cargo-sweep is not installed" >&2
  exit 0
fi

if (
  cd "$repo_root"
  cargo sweep --time 5
); then
  printf '%s\n' "$now" > "$state_file"
else
  echo "cargo-sweep-if-due: cargo sweep failed; leaving last-run timestamp unchanged" >&2
fi
