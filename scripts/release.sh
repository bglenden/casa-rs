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

Use --full to additionally run:
  - scripts/test-slow.sh
  - scripts/run-coverage.sh --ci-like

Examples:
  scripts/release.sh 0.3.1
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
[[ "$current_version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]] || die "current workspace version is not x.y.z"
current_major="${BASH_REMATCH[1]}"
current_minor="${BASH_REMATCH[2]}"
current_patch="${BASH_REMATCH[3]}"

case "$version_arg" in
  --patch)
    version="${current_major}.${current_minor}.$((current_patch + 1))"
    ;;
  --minor)
    version="${current_major}.$((current_minor + 1)).0"
    ;;
  *)
    version="$version_arg"
    [[ "$version" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)$ ]] || die "version must look like x.y.z"
    new_major="${BASH_REMATCH[1]}"
    new_minor="${BASH_REMATCH[2]}"
    new_patch="${BASH_REMATCH[3]}"
    if (( new_major < current_major )) \
      || (( new_major == current_major && new_minor < current_minor )) \
      || (( new_major == current_major && new_minor == current_minor && new_patch <= current_patch )); then
      die "requested version $version must be greater than current version $current_version"
    fi
    ;;
esac

[[ "$current_version" != "$version" ]] || die "workspace version is already $version"

tag="v$version"
git rev-parse --verify "$tag" >/dev/null 2>&1 && die "tag $tag already exists"

release_started_at="$(date +%s)"

echo "==> Running default local release gates"
run_timed_step "cargo fmt" cargo fmt --all -- --check
run_timed_step "cargo clippy" cargo clippy --workspace --all-targets -- -D warnings
run_timed_step "cargo test" cargo test --workspace

if [[ "$run_full" == "true" ]]; then
  run_timed_step "slow parity gate" scripts/test-slow.sh
  run_timed_step "CI-like coverage gate" scripts/run-coverage.sh --ci-like
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
