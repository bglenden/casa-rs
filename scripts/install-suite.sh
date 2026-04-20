#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/install-suite.sh (--version <version> | --bundle <path-or-url>) [options]
       scripts/install-suite.sh --print-platform

Installs a published casa-rs suite bundle into a local suite root, creates a
Python virtual environment from the bundled wheel, and manages stable/rc
symlinks plus PATH-facing launcher links.

Options:
  --version <version>       Version to install, for example 0.15.0 or 0.16.0-rc1
  --bundle <path-or-url>    Local bundle path or explicit bundle URL
  --repo <owner/name>       GitHub repository to download from
                            (default: bglenden/casa-rs)
  --python <python>         Python interpreter to use
                            (default: repo resolver for Python >=3.10)
  --install-root <dir>      Suite install root (default: ~/.local/opt/casa-rs)
  --bin-dir <dir>           PATH-facing symlink directory (default: ~/.local/bin)
  --activate                Update the generic current/bin links to this install
  --no-activate             Do not update the generic current/bin links
  --force                   Replace an existing install directory for this version
  --print-platform          Print the installer platform label and exit
  -h, --help                Show this help
EOF
}

die() {
  echo "install-suite.sh: $*" >&2
  exit 1
}

detect_platform() {
  local os
  local arch
  os="$(uname -s)"
  arch="$(uname -m)"
  case "$os/$arch" in
    Darwin/arm64)
      echo "macos-arm64"
      ;;
    Linux/x86_64)
      echo "linux-x86_64"
      ;;
    *)
      die "unsupported platform $os/$arch"
      ;;
  esac
}

is_url() {
  [[ "$1" =~ ^https?:// ]]
}

download_or_copy() {
  local source="$1"
  local dest="$2"
  if is_url "$source"; then
    curl -fsSL "$source" -o "$dest"
  else
    cp "$source" "$dest"
  fi
}

bundle_url_for() {
  local repo="$1"
  local version="$2"
  local platform="$3"
  local asset="casa-rs-suite-$version-$platform.tar.gz"
  echo "https://github.com/$repo/releases/download/v$version/$asset"
}

read_manifest_field() {
  local manifest="$1"
  local field="$2"
  local python_bin="$3"
  "$python_bin" - "$manifest" "$field" <<'PY'
import json
import pathlib
import sys

manifest = pathlib.Path(sys.argv[1])
field = sys.argv[2]
data = json.loads(manifest.read_text())
value = data[field]
if isinstance(value, str):
    print(value)
else:
    raise SystemExit(f"manifest field {field!r} is not a string")
PY
}

python_tag() {
  local python_bin="$1"
  "$python_bin" - <<'PY'
import sys

if sys.implementation.name != "cpython":
    raise SystemExit("only CPython is supported by the bundled wheels")

major = sys.version_info.major
minor = sys.version_info.minor
print(f"cp{major}{minor}")
PY
}

select_wheel() {
  local wheels_dir="$1"
  local python_bin="$2"
  local tag
  tag="$(python_tag "$python_bin")"

  shopt -s nullglob
  local matches=( "$wheels_dir"/*-"$tag"-"$tag"-*.whl )
  shopt -u nullglob

  if (( ${#matches[@]} == 1 )); then
    printf '%s\n' "${matches[0]}"
    return 0
  fi

  if (( ${#matches[@]} == 0 )); then
    die "no bundled wheel matched interpreter tag $tag in $wheels_dir"
  fi

  die "multiple bundled wheels matched interpreter tag $tag in $wheels_dir"
}

version=""
bundle_source=""
repo="${CASARS_INSTALL_REPOSITORY:-bglenden/casa-rs}"
python_bin="${PYTHON_BIN:-}"
install_root="$HOME/.local/opt/casa-rs"
bin_dir="$HOME/.local/bin"
activate=""
force="false"
print_platform="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      version="${2:-}"
      shift 2
      ;;
    --bundle)
      bundle_source="${2:-}"
      shift 2
      ;;
    --repo)
      repo="${2:-}"
      shift 2
      ;;
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
    --print-platform)
      print_platform="true"
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

platform="$(detect_platform)"
if [[ "$print_platform" == "true" ]]; then
  echo "$platform"
  exit 0
fi

if [[ -z "$version" && -z "$bundle_source" ]]; then
  usage
  die "one of --version or --bundle is required"
fi

if [[ -z "$python_bin" ]]; then
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  python_bin="$("$script_dir/resolve-python.sh" 3.10)"
else
  command -v "$python_bin" >/dev/null 2>&1 || die "python interpreter not found: $python_bin"
fi

command -v curl >/dev/null 2>&1 || die "curl is required"
command -v tar >/dev/null 2>&1 || die "tar is required"

if [[ -z "$bundle_source" ]]; then
  bundle_source="$(bundle_url_for "$repo" "$version" "$platform")"
fi

tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
}
trap cleanup EXIT

bundle_archive="$tmp_root/bundle.tar.gz"
download_or_copy "$bundle_source" "$bundle_archive"

extract_root="$tmp_root/extracted"
mkdir -p "$extract_root"
tar -xzf "$bundle_archive" -C "$extract_root"

bundle_root="$(find "$extract_root" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
[[ -n "$bundle_root" ]] || die "bundle did not contain a top-level directory"
manifest_path="$bundle_root/bundle-manifest.json"
[[ -f "$manifest_path" ]] || die "bundle is missing bundle-manifest.json"

manifest_version="$(read_manifest_field "$manifest_path" version "$python_bin")"
manifest_platform="$(read_manifest_field "$manifest_path" platform "$python_bin")"
channel="$(read_manifest_field "$manifest_path" channel "$python_bin")"
[[ "$manifest_platform" == "$platform" ]] || die "bundle platform $manifest_platform does not match installer platform $platform"

if [[ -n "$version" && "$manifest_version" != "$version" ]]; then
  die "bundle version $manifest_version does not match requested version $version"
fi
version="$manifest_version"

install_dir="$install_root/$version"
if [[ -e "$install_dir" ]]; then
  if [[ "$force" == "true" ]]; then
    rm -rf "$install_dir"
  else
    die "install directory already exists: $install_dir (use --force to replace it)"
  fi
fi

mkdir -p "$install_dir" "$bin_dir"
cp -R "$bundle_root/bin" "$install_dir/"
cp -R "$bundle_root/wheels" "$install_dir/"
cp "$manifest_path" "$install_dir/"

wheel_path="$(select_wheel "$install_dir/wheels" "$python_bin")"
"$python_bin" -m venv "$install_dir/python"
"$install_dir/python/bin/python" -m pip install --upgrade pip
"$install_dir/python/bin/python" -m pip install "$wheel_path"

ln -sfn "$install_dir" "$install_root/$channel"
ln -sfn "$install_root/$channel/bin/casars" "$bin_dir/casars-$channel"
ln -sfn "$install_root/$channel/bin/calibrate" "$bin_dir/calibrate-$channel"

if [[ -z "$activate" ]]; then
  if [[ "$channel" == "stable" ]]; then
    activate="true"
  else
    activate="false"
  fi
fi

if [[ "$activate" == "true" ]]; then
  ln -sfn "$install_dir" "$install_root/current"
  ln -sfn "$install_root/current/bin/casars" "$bin_dir/casars"
  ln -sfn "$install_root/current/bin/calibrate" "$bin_dir/calibrate"
fi

cat <<EOF
Installed casa-rs $version for $platform
  suite root: $install_dir
  channel link: $install_root/$channel
  channel commands: $bin_dir/casars-$channel, $bin_dir/calibrate-$channel
EOF

if [[ "$activate" == "true" ]]; then
  cat <<EOF
  active commands: $bin_dir/casars, $bin_dir/calibrate
  active suite root: $install_root/current
EOF
else
  cat <<EOF
  active commands unchanged; use --activate to switch the default suite
EOF
fi

cat <<EOF

If $bin_dir is not already on PATH, add:
  export PATH="$bin_dir:\$PATH"

Activate the suite Python environment with:
  source "$install_dir/python/bin/activate"
EOF
