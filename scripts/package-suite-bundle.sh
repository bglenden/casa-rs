#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/package-suite-bundle.sh --version <version> --platform <platform> \
       --bin-dir <dir> --wheel-dir <dir> --out-dir <dir>

Creates release bundle archives for one supported platform:
  - casa-rs-suite-<version>-<platform>.tar.gz
  - casa-rs-binaries-<version>-<platform>.tar.gz

The suite bundle contains:
  - bin/casars
  - bin/calibrate
  - wheels/*.whl
  - bundle-manifest.json

The binaries bundle contains:
  - bin/casars
  - bin/calibrate
EOF
}

die() {
  echo "package-suite-bundle.sh: $*" >&2
  exit 1
}

version=""
platform=""
bin_dir=""
wheel_dir=""
out_dir=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      version="${2:-}"
      shift 2
      ;;
    --platform)
      platform="${2:-}"
      shift 2
      ;;
    --bin-dir)
      bin_dir="${2:-}"
      shift 2
      ;;
    --wheel-dir)
      wheel_dir="${2:-}"
      shift 2
      ;;
    --out-dir)
      out_dir="${2:-}"
      shift 2
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

[[ -n "$version" ]] || { usage; die "--version is required"; }
[[ -n "$platform" ]] || { usage; die "--platform is required"; }
[[ -n "$bin_dir" ]] || { usage; die "--bin-dir is required"; }
[[ -n "$wheel_dir" ]] || { usage; die "--wheel-dir is required"; }
[[ -n "$out_dir" ]] || { usage; die "--out-dir is required"; }

[[ -x "$bin_dir/casars" ]] || die "missing executable $bin_dir/casars"
[[ -x "$bin_dir/calibrate" ]] || die "missing executable $bin_dir/calibrate"
[[ -d "$wheel_dir" ]] || die "wheel directory does not exist: $wheel_dir"

shopt -s nullglob
wheels=( "$wheel_dir"/*.whl )
shopt -u nullglob
(( ${#wheels[@]} > 0 )) || die "no wheels found in $wheel_dir"

channel="stable"
if [[ "$version" == *-rc* ]]; then
  channel="rc"
fi

mkdir -p "$out_dir"
tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
}
trap cleanup EXIT

suite_dir="$tmp_root/casa-rs-suite-$version-$platform"
binaries_dir="$tmp_root/casa-rs-binaries-$version-$platform"
mkdir -p "$suite_dir/bin" "$suite_dir/wheels" "$binaries_dir/bin"

cp "$bin_dir/casars" "$suite_dir/bin/"
cp "$bin_dir/calibrate" "$suite_dir/bin/"
cp "$bin_dir/casars" "$binaries_dir/bin/"
cp "$bin_dir/calibrate" "$binaries_dir/bin/"
cp "${wheels[@]}" "$suite_dir/wheels/"

manifest_path="$suite_dir/bundle-manifest.json"
wheel_json="$(
  printf '%s\n' "${wheels[@]}" | while IFS= read -r wheel; do
    python3 - "$wheel" <<'PY'
import json
import pathlib
import sys

print(json.dumps(pathlib.Path(sys.argv[1]).name))
PY
  done | paste -sd, -
)"

cat >"$manifest_path" <<EOF
{
  "bundle_format": 1,
  "suite_name": "casa-rs",
  "version": "$version",
  "platform": "$platform",
  "channel": "$channel",
  "binaries": ["casars", "calibrate"],
  "wheel_files": [${wheel_json}]
}
EOF

suite_archive="$out_dir/casa-rs-suite-$version-$platform.tar.gz"
binaries_archive="$out_dir/casa-rs-binaries-$version-$platform.tar.gz"

tar -C "$tmp_root" -czf "$suite_archive" "$(basename "$suite_dir")"
tar -C "$tmp_root" -czf "$binaries_archive" "$(basename "$binaries_dir")"

echo "Created:"
echo "  $suite_archive"
echo "  $binaries_archive"
