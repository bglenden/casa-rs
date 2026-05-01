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
cargo build --release -p casars-importvla --bin casars-importvla
cargo build --release -p casa-ms --bin msexplore --bin mstransform
cargo build --release -p casars-imager --bin casars-imager
cargo build --release -p casa-images --bin imexplore --bin immoments --bin impv --bin exportfits --bin importfits

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
"$install_root/$version/bin/casars-importvla" --protocol-info >/dev/null
"$install_root/$version/bin/msexplore" --protocol-info >/dev/null
test -x "$install_root/$version/bin/mstransform"
"$install_root/$version/bin/casars-imager" --protocol-info >/dev/null
"$install_root/$version/bin/imexplore" --protocol-info >/dev/null
"$install_root/$version/bin/immoments" --protocol-info >/dev/null
"$install_root/$version/bin/impv" --protocol-info >/dev/null
"$install_root/$version/bin/exportfits" --protocol-info >/dev/null
"$install_root/$version/bin/importfits" --protocol-info >/dev/null

echo "==> Verifying installed Python package"
source "$install_root/$version/python/bin/activate"
python - <<'PY'
import casars
from casars.tasks import calibrate, image_analysis, imager, importvla, msexplore

assert casars.__version__
info = calibrate.protocol_info()
assert info.protocol_name == "casa_calibration_task"
assert info.protocol_version == 1
assert importvla.protocol_info().protocol_name == "casa_importvla_task"
assert msexplore.protocol_info().protocol_name == "casa_msexplore_task"
assert imager.protocol_info().protocol_name == "casa_imager_task"
assert image_analysis.immoments_protocol_info().protocol_name == "casa_image_analysis_task"
assert image_analysis.impv_protocol_info().protocol_name == "casa_image_analysis_task"
assert image_analysis.exportfits_protocol_info().protocol_name == "casa_image_analysis_task"
assert image_analysis.importfits_protocol_info().protocol_name == "casa_image_analysis_task"
PY
deactivate

echo "==> Suite install smoke test completed"
