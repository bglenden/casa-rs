#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

# Daily housekeeping of this clone's own disposable state, run in the
# background by .githooks/post-commit (or by hand, with --now and optionally
# --dry-run). It touches nothing outside the repository and its cargo target
# directory:
#
# - merged and squash-merged worktrees and local branches
#   (scripts/tidy-git.sh --apply), keeping a worktree used in the last
#   HOUSEKEEPING_IDLE_HOURS hours (default 24);
# - cargo build output older than five days (cargo sweep, when installed);
# - tool and test scratch in the target directory: every top-level entry that
#   is not cargo's own and has had nothing written for
#   HOUSEKEEPING_SCRATCH_DAYS days (default 7).
#
# It runs at most once per HOUSEKEEPING_INTERVAL_SECONDS (default a day) and
# logs to housekeeping.log in the git directory.

set -euo pipefail

usage() {
  echo "Usage: scripts/housekeeping-if-due.sh [--now] [--dry-run]" >&2
}

now_flag=0
dry_run=0
for arg in "$@"; do
  case "$arg" in
    --now) now_flag=1 ;;
    --dry-run) dry_run=1 ;;
    -h | --help) usage; exit 0 ;;
    *) usage; exit 2 ;;
  esac
done

repo_root="$(git rev-parse --show-toplevel)"
git_dir="$(git rev-parse --path-format=absolute --git-common-dir)"
interval_seconds="${HOUSEKEEPING_INTERVAL_SECONDS:-86400}"
idle_hours="${HOUSEKEEPING_IDLE_HOURS:-24}"
scratch_days="${HOUSEKEEPING_SCRATCH_DAYS:-7}"
target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"
state_file="$git_dir/.housekeeping-last-run"
lock_dir="$git_dir/.housekeeping-lock"

for value in "$interval_seconds" "$idle_hours" "$scratch_days"; do
  if ! [[ "$value" =~ ^[0-9]+$ ]]; then
    echo "housekeeping: intervals must be whole numbers" >&2
    exit 2
  fi
done

now="$(date +%s)"
last_run="$(cat "$state_file" 2>/dev/null || echo 0)"
[[ "$last_run" =~ ^[0-9]+$ ]] || last_run=0
if ((now_flag == 0 && now - last_run < interval_seconds)); then
  exit 0
fi

if ! mkdir "$lock_dir" 2>/dev/null; then
  exit 0
fi
trap 'rmdir "$lock_dir" 2>/dev/null || true' EXIT
((dry_run == 1)) || printf '%s\n' "$now" >"$state_file"

echo "housekeeping: $(date -u +%Y-%m-%dT%H:%M:%SZ) in $repo_root"

# Worktrees and branches already merged into origin/main.
tidy_args=()
((dry_run == 1)) || tidy_args=(--apply)
TIDY_MIN_IDLE_HOURS="$idle_hours" "$repo_root/scripts/tidy-git.sh" ${tidy_args[@]+"${tidy_args[@]}"} ||
  echo "housekeeping: tidy failed; continuing"

# Cargo's own build output.
if command -v cargo >/dev/null 2>&1 && cargo sweep --help >/dev/null 2>&1; then
  if ((dry_run == 1)); then
    echo "  would run: cargo sweep --time 5"
  else
    (cd "$repo_root" && cargo sweep --time 5) || echo "housekeeping: cargo sweep failed"
  fi
else
  echo "housekeeping: cargo sweep not installed; build output left alone"
fi

# Scratch that tools and tests leave in the target directory. Cargo's own
# entries are kept: its profile directories, documentation, packaging and
# metadata files, and any target-triple directory (one holding a profile).
is_cargo_entry() {
  local name
  name="$(basename "$1")"
  case "$name" in
    debug | release | doc | nextest | package | tmp | wheels | llvm-cov-target | \
      rust-analyzer | flycheck* | CACHEDIR.TAG | .rustc_info.json | \
      .future-incompat-report.json)
      return 0
      ;;
  esac
  [[ -d "$1/debug" || -d "$1/release" ]]
}

if [[ -d "$target_dir" ]]; then
  for entry in "$target_dir"/* "$target_dir"/.[!.]*; do
    [[ -e "$entry" ]] || continue
    is_cargo_entry "$entry" && continue
    if [[ -n "$(find "$entry" -mtime -"$scratch_days" -print -quit 2>/dev/null)" ]]; then
      continue
    fi
    if ((dry_run == 1)); then
      echo "  would remove scratch: $entry"
    else
      rm -rf -- "$entry" && echo "  removed scratch: $entry"
    fi
  done
fi
