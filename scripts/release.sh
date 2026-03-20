#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/release.sh <version> [--push]
       scripts/release.sh --patch [--push]
       scripts/release.sh --minor [--push]

Runs the repository release process:
  1. Verifies a clean worktree
  2. Runs the AGENTS.md quality gates
  3. Bumps [workspace.package].version in Cargo.toml
  4. Commits the version bump as "Release <version>"
  5. Creates tag "v<version>"
  6. Optionally pushes the commit and tag when --push is given

Examples:
  scripts/release.sh 0.3.1
  scripts/release.sh --patch
  scripts/release.sh --minor --push
  scripts/release.sh 0.3.1 --push
EOF
}

die() {
  echo "release.sh: $*" >&2
  exit 1
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage
  exit 1
fi

version_arg="$1"
push_release="false"
if [[ $# -eq 2 ]]; then
  if [[ "$2" != "--push" ]]; then
    usage
    exit 1
  fi
  push_release="true"
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ -n "$(git status --short)" ]]; then
  die "worktree must be clean before cutting a release"
fi

tag="v$version"
git rev-parse --verify "$tag" >/dev/null 2>&1 && die "tag $tag already exists"

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

echo "==> Running quality gates"
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75

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

git add Cargo.toml
git commit -m "Release $version"
git tag "$tag"

if [[ "$push_release" == "true" ]]; then
  echo "==> Pushing commit and tag"
  git push origin HEAD
  git push origin "$tag"
fi

echo "Release $version created successfully"
