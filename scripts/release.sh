#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/release.sh <version> [--push] [--full]
       scripts/release.sh --patch [--push] [--full]
       scripts/release.sh --minor [--push] [--full]

Runs the repository release process:
  1. Verifies a clean worktree
  2. Runs the default local release gates
  3. Bumps [workspace.package].version in Cargo.toml
  4. Commits the version bump as "Release <version>"
  5. Creates tag "v<version>"
  6. Optionally pushes the commit and tag when --push is given

Default local release gates:
  - cargo fmt --all -- --check
  - cargo clippy --workspace --all-targets -- -D warnings
  - cargo test --workspace
  - scripts/test-release-cpp-interop.sh
  - scripts/test-python-package.sh
  - scripts/test-smoke.sh
  - scripts/test-install-suite.sh
  - scripts/run-coverage.sh --ci-like

Use --full to additionally run:
  - scripts/test-release-perf.sh
  - scripts/test-slow.sh
  - scripts/build-python-docs.sh

Examples:
  scripts/release.sh 0.3.1
  scripts/release.sh 0.4.0-rc1
  scripts/release.sh --patch
  scripts/release.sh --minor --push
  scripts/release.sh --minor --full
  scripts/release.sh 0.3.1 --push
EOF
}

die() {
  echo "release.sh: $*" >&2
  exit 1
}

format_elapsed() {
  local total_seconds="$1"
  local minutes=$(( total_seconds / 60 ))
  local seconds=$(( total_seconds % 60 ))
  printf '%dm%02ds' "$minutes" "$seconds"
}

run_timed_step() {
  local label="$1"
  shift
  local started_at
  local finished_at
  started_at="$(date +%s)"
  echo "==> $label"
  "$@"
  finished_at="$(date +%s)"
  echo "==> $label completed in $(format_elapsed $(( finished_at - started_at )))"
}

parse_version() {
  local value="$1"
  local prefix="$2"

  if [[ ! "$value" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-rc([0-9]+))?$ ]]; then
    return 1
  fi

  eval "${prefix}_major=${BASH_REMATCH[1]}"
  eval "${prefix}_minor=${BASH_REMATCH[2]}"
  eval "${prefix}_patch=${BASH_REMATCH[3]}"
  if [[ -n "${BASH_REMATCH[4]:-}" ]]; then
    eval "${prefix}_is_rc=true"
    eval "${prefix}_rc=${BASH_REMATCH[5]}"
  else
    eval "${prefix}_is_rc=false"
    eval "${prefix}_rc=0"
  fi
}

version_core_compare() {
  local left="$1"
  local right="$2"

  local left_major left_minor left_patch
  local right_major right_minor right_patch
  eval "left_major=\${${left}_major}"
  eval "left_minor=\${${left}_minor}"
  eval "left_patch=\${${left}_patch}"
  eval "right_major=\${${right}_major}"
  eval "right_minor=\${${right}_minor}"
  eval "right_patch=\${${right}_patch}"

  if (( left_major != right_major )); then
    (( left_major > right_major )) && return 1 || return 2
  fi
  if (( left_minor != right_minor )); then
    (( left_minor > right_minor )) && return 1 || return 2
  fi
  if (( left_patch != right_patch )); then
    (( left_patch > right_patch )) && return 1 || return 2
  fi
  return 0
}

validate_requested_version() {
  local current="$1"
  local requested="$2"

  parse_version "$current" current || die "current workspace version is not x.y.z or x.y.z-rcN"
  parse_version "$requested" requested || die "version must look like x.y.z or x.y.z-rcN"

  if [[ "$current" == "$requested" ]]; then
    die "workspace version is already $requested"
  fi

  version_core_compare requested current
  case $? in
    1)
      return 0
      ;;
    2)
      die "requested version $requested must be greater than current version $current"
      ;;
    0)
      ;;
  esac

  if [[ "$current_is_rc" != "true" ]]; then
    die "requested version $requested must be greater than current version $current"
  fi

  if [[ "$requested_is_rc" == "true" ]]; then
    if (( requested_rc <= current_rc )); then
      die "requested version $requested must be greater than current version $current"
    fi
    return 0
  fi

  return 0
}

next_patch_version() {
  local current="$1"
  parse_version "$current" current || die "current workspace version is not x.y.z or x.y.z-rcN"
  [[ "$current_is_rc" == "false" ]] || die "--patch is not supported when current version is a release candidate; pass an explicit version"
  echo "${current_major}.${current_minor}.$((current_patch + 1))"
}

next_minor_version() {
  local current="$1"
  parse_version "$current" current || die "current workspace version is not x.y.z or x.y.z-rcN"
  [[ "$current_is_rc" == "false" ]] || die "--minor is not supported when current version is a release candidate; pass an explicit version"
  echo "${current_major}.$((current_minor + 1)).0"
}

main() {
  if [[ $# -lt 1 || $# -gt 3 ]]; then
    usage
    exit 1
  fi

  version_arg="$1"
  push_release="false"
  run_full="false"
  shift
  for flag in "$@"; do
    case "$flag" in
      --push)
        push_release="true"
        ;;
      --full)
        run_full="true"
        ;;
      *)
        usage
        exit 1
        ;;
    esac
  done

  repo_root="$(git rev-parse --show-toplevel)"
  cd "$repo_root"

  if [[ -n "$(git status --short)" ]]; then
    die "worktree must be clean before cutting a release"
  fi

  current_version="$(
    sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1
  )"
  [[ -n "$current_version" ]] || die "failed to read workspace version from Cargo.toml"
  parse_version "$current_version" current || die "current workspace version is not x.y.z or x.y.z-rcN"

  case "$version_arg" in
    --patch)
      version="$(next_patch_version "$current_version")"
      ;;
    --minor)
      version="$(next_minor_version "$current_version")"
      ;;
    *)
      version="$version_arg"
      validate_requested_version "$current_version" "$version"
      ;;
  esac

  tag="v$version"
  git rev-parse --verify "$tag" >/dev/null 2>&1 && die "tag $tag already exists"

  release_started_at="$(date +%s)"

  echo "==> Running default local release gates"
  run_timed_step "cargo fmt" cargo fmt --all -- --check
  run_timed_step "cargo clippy" cargo clippy --workspace --all-targets -- -D warnings
  run_timed_step "cargo test" cargo test --workspace
  run_timed_step "C++ interop release gate" bash scripts/test-release-cpp-interop.sh
  run_timed_step "python package gate" scripts/test-python-package.sh
  run_timed_step "smoke gate" bash scripts/test-smoke.sh
  run_timed_step "suite install gate" scripts/test-install-suite.sh
  run_timed_step "CI-like coverage gate" scripts/run-coverage.sh --ci-like

  if [[ "$run_full" == "true" ]]; then
    run_timed_step "release performance suite" bash scripts/test-release-perf.sh
    run_timed_step "slow parity gate" scripts/test-slow.sh
    run_timed_step "python docs build" scripts/build-python-docs.sh
  fi

  for cargo_toml in crates/*/Cargo.toml; do
    if ! grep -Eq '^version\.workspace = true$' "$cargo_toml"; then
      die "$cargo_toml does not use version.workspace = true"
    fi
  done

  echo "==> Updating workspace version $current_version -> $version"
  perl -0pi -e 's/^version = "\Q'"$current_version"'\E"$/version = "'"$version"'"/m' Cargo.toml

  if git diff --quiet -- Cargo.toml; then
    die "version update did not modify Cargo.toml"
  fi

  run_timed_step "refreshing Cargo.lock metadata" cargo metadata --format-version=1 >/dev/null

  git add Cargo.toml Cargo.lock
  git commit -m "Release $version"
  git tag "$tag"

  if [[ "$push_release" == "true" ]]; then
    run_timed_step "pushing release commit" git push origin HEAD
    run_timed_step "pushing release tag" git push origin "$tag"
  fi

  release_finished_at="$(date +%s)"
  echo "Release $version created successfully in $(format_elapsed $(( release_finished_at - release_started_at )))"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
