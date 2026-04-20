#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

python_bin="$("$repo_root/scripts/resolve-python.sh" 3.10)"
tmp_root="$(mktemp -d)"
cleanup() {
  rm -rf "$tmp_root"
}
trap cleanup EXIT

wheel_dir="$tmp_root/dist"
install_root="$tmp_root/install-root"
bin_dir="$tmp_root/bin"

echo "==> Building release binaries for suite install smoke test"
cargo build --release -p casars --bin casars
cargo build --release -p casa-calibration --bin calibrate

echo "==> Building Python wheel artifacts for suite install smoke test"
scripts/build-python-dist.sh "$wheel_dir"

platform="$(scripts/install-suite.sh --print-platform)"
version="$(
  sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1
)"
[[ -n "$version" ]] || {
  echo "test-install-suite.sh: failed to determine workspace version" >&2
  exit 1
}

echo "==> Packaging suite bundle"
scripts/package-suite-bundle.sh \
  --version "$version" \
  --platform "$platform" \
  --bin-dir target/release \
  --wheel-dir "$wheel_dir" \
  --out-dir "$wheel_dir"

bundle_path="$wheel_dir/casa-rs-suite-$version-$platform.tar.gz"
[[ -f "$bundle_path" ]] || {
  echo "test-install-suite.sh: expected bundle $bundle_path" >&2
  exit 1
}

echo "==> Installing suite bundle into temporary root"
scripts/install-suite.sh \
  --bundle "$bundle_path" \
  --python "$python_bin" \
  --install-root "$install_root" \
  --bin-dir "$bin_dir" \
  --activate

echo "==> Verifying installed launchers"
"$bin_dir/casars" --help >/dev/null
"$bin_dir/calibrate" --protocol-info >/dev/null
"$bin_dir/casars-stable" --help >/dev/null
"$bin_dir/calibrate-stable" --protocol-info >/dev/null

echo "==> Verifying installed Python package"
source "$install_root/$version/python/bin/activate"
python - <<'PY'
import casars
from casars.tasks import calibrate

assert casars.__version__
info = calibrate.protocol_info()
assert info.protocol_name == "casa_calibration_task"
assert info.protocol_version == 1
PY
deactivate

echo "==> Suite install smoke test completed"
