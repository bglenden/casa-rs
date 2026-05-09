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
while read -r package binary; do
  cargo build --release -p "$package" --bin "$binary"
done < <(scripts/list-suite-binaries.py --package-binary)

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
while read -r binary; do
  installed="$install_root/$version/bin/$binary"
  test -x "$installed"
  case "$binary" in
    casars)
      "$installed" --help >/dev/null
      ;;
    flagdata|flagmanager)
      "$installed" --help >/dev/null
      ;;
    mstransform)
      ;;
    casars-casa-task)
      "$installed" --task plotcal --protocol-info >/dev/null
      ;;
    *)
      "$installed" --protocol-info >/dev/null
      ;;
  esac
done < <(scripts/list-suite-binaries.py)

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
assert image_analysis.imsubimage_protocol_info().protocol_name == "casa_image_analysis_task"
assert image_analysis.immath_protocol_info().protocol_name == "casa_image_analysis_task"
assert image_analysis.exportfits_protocol_info().protocol_name == "casa_image_analysis_task"
assert image_analysis.importfits_protocol_info().protocol_name == "casa_image_analysis_task"
PY
deactivate

echo "==> Suite install smoke test completed"
