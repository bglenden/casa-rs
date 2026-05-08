#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/install-local-suite.sh [options]

Builds the current checkout as a release-style suite bundle and installs it
through scripts/install-suite.sh.

Options:
  --python <python>         Python interpreter to use
                            (default: repo resolver for Python >=3.10)
  --install-root <dir>      Suite install root passed to install-suite.sh
                            (default: ~/.local/opt/casa-rs)
  --bin-dir <dir>           PATH-facing launcher directory
                            (default: ~/.local/bin)
  --activate                Update current/bin links to this install
  --no-activate             Do not update current/bin links
  --force                   Replace any existing install for this version
  --keep-dist               Keep the temporary built bundle directory
  -h, --help                Show this help
EOF
}

die() {
  echo "install-local-suite.sh: $*" >&2
  exit 1
}

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python_bin=""
install_root="$HOME/.local/opt/casa-rs"
bin_dir="$HOME/.local/bin"
activate="true"
force="false"
keep_dist="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      python_bin="${2:-}"
      shift 2
      ;;
    --install-root)
      install_root="${2:-}"
      shift 2
      ;;
    --bin-dir)
      bin_dir="${2:-}"
      shift 2
      ;;
    --activate)
      activate="true"
      shift
      ;;
    --no-activate)
      activate="false"
      shift
      ;;
    --force)
      force="true"
      shift
      ;;
    --keep-dist)
      keep_dist="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      die "unexpected argument: $1"
      ;;
  esac
done

if [[ -z "$python_bin" ]]; then
  python_bin="$("$repo_root/scripts/resolve-python.sh" 3.10)"
else
  command -v "$python_bin" >/dev/null 2>&1 || die "python interpreter not found: $python_bin"
fi

version="$(
  sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1
)"
[[ -n "$version" ]] || die "failed to determine workspace version from Cargo.toml"
platform="$("$repo_root/scripts/install-suite.sh" --print-platform)"

dist_root="$(mktemp -d)"
cleanup() {
  if [[ "$keep_dist" != "true" ]]; then
    rm -rf "$dist_root"
  fi
}
trap cleanup EXIT

echo "==> Building release binaries for local suite install"
while read -r package binary; do
  cargo build --release -p "$package" --bin "$binary"
done < <("$repo_root/scripts/list-suite-binaries.py" --package-binary)

echo "==> Building Python distribution artifacts"
"$repo_root/scripts/build-python-dist.sh" "$dist_root"

echo "==> Packaging local suite bundle"
"$repo_root/scripts/package-suite-bundle.sh" \
  --version "$version" \
  --platform "$platform" \
  --bin-dir "$repo_root/target/release" \
  --wheel-dir "$dist_root" \
  --out-dir "$dist_root"

bundle_path="$dist_root/casa-rs-suite-$version-$platform.tar.gz"
[[ -f "$bundle_path" ]] || die "expected bundle $bundle_path"

echo "==> Installing local suite bundle"
install_args=(
  --bundle "$bundle_path"
  --python "$python_bin"
  --install-root "$install_root"
  --bin-dir "$bin_dir"
)
if [[ "$activate" == "true" ]]; then
  install_args+=(--activate)
else
  install_args+=(--no-activate)
fi
if [[ "$force" == "true" ]]; then
  install_args+=(--force)
fi
"$repo_root/scripts/install-suite.sh" "${install_args[@]}"

if [[ "$keep_dist" == "true" ]]; then
  echo "==> Kept local build artifacts at $dist_root"
fi
