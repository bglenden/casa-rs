#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/test-install-suite.sh [--hide-checkout]

Builds the suite's release binaries and wheel, packages and installs the
suite into a temporary root, and checks the installed suite from outside the
checkout: the commands on PATH (never split, feather, immath or impv), every
executable's discovery, a task run from its command and from the suite's
Python, and the executable Python chose.

--hide-checkout moves the source checkout aside while the installed suite is
checked, and back afterwards, so nothing the suite runs can reach it. CI
passes it; a working checkout is left in place without it.
EOF
}

hide_checkout="false"
case "${1:-}" in
  "") ;;
  --hide-checkout) hide_checkout="true" ;;
  -h|--help) usage; exit 0 ;;
  *) usage >&2; exit 2 ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
export CARGO_INCREMENTAL=0

python_bin="$("$repo_root/scripts/resolve-python.sh" 3.10)"
tmp_root="$(mktemp -d)"
hidden_root="$repo_root.hidden-for-suite-test"
cleanup() {
  if [[ -d "$hidden_root" && ! -e "$repo_root" ]]; then
    mv "$hidden_root" "$repo_root"
  fi
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

# What the checks need from the checkout, copied out before it is hidden.
suite_binaries=()
while read -r binary; do
  suite_binaries+=( "$binary" )
done < <(scripts/list-suite-binaries.py)
work="$tmp_root/work"
mkdir -p "$work" "$bin_dir"
tar -xzf crates/casa-ms/tests/fixtures/mssel_test_small.ms.tgz -C "$work"
cp scripts/install-suite.sh "$tmp_root/install-suite.sh"
# A link an older suite left for the retired casars launcher, and a user's
# own command with a suite command's name, which the installer must keep.
ln -s "$install_root/current/bin/casars" "$bin_dir/casars"
printf '#!/bin/sh\necho my imregrid\n' > "$bin_dir/imregrid"
chmod +x "$bin_dir/imregrid"

echo "==> Installing suite bundle into temporary root"
"$tmp_root/install-suite.sh" \
  --bundle "$bundle_path" \
  --python "$python_bin" \
  --install-root "$install_root" \
  --bin-dir "$bin_dir" \
  --activate

if [[ "$hide_checkout" == "true" ]]; then
  echo "==> Hiding the checkout at $hidden_root"
  mv "$repo_root" "$hidden_root"
fi
cd "$work"
# The installed suite runs with no override, suite root or cargo setting.
for variable in $(compgen -e); do
  case "$variable" in
    CASARS_*|CARGO_TARGET_DIR) unset "$variable" ;;
  esac
done

echo "==> Verifying the commands on PATH"
[[ ! -e "$bin_dir/casars" && ! -L "$bin_dir/casars" ]] || {
  echo "test-install-suite.sh: the stale casars link was kept" >&2
  exit 1
}
for binary in "${suite_binaries[@]}"; do
  installed="$install_root/$version/bin/$binary"
  test -x "$installed"
  "$installed" --protocol-info >/dev/null
  case "$binary" in
    split|feather|immath|impv)
      for link in "$bin_dir/$binary" "$bin_dir/$binary-stable"; do
        [[ ! -e "$link" && ! -L "$link" ]] || {
          echo "test-install-suite.sh: $link shadows a system command or generic word" >&2
          exit 1
        }
      done
      ;;
    imregrid)
      [[ "$("$bin_dir/imregrid")" == "my imregrid" ]] || {
        echo "test-install-suite.sh: the installer replaced a user's own imregrid" >&2
        exit 1
      }
      "$bin_dir/$binary-stable" --protocol-info >/dev/null
      ;;
    *)
      "$bin_dir/$binary" --protocol-info >/dev/null
      "$bin_dir/$binary-stable" --protocol-info >/dev/null
      ;;
  esac
done

echo "==> Running a task from its command"
"$bin_dir/flagmanager" --vis mssel_test_small.ms --mode list > flagmanager.txt
grep -q '"name": "main"' flagmanager.txt
# The command line records Last in its workspace.
test -f .casa-rs/parameters/flagmanager/last-successful.toml

echo "==> Verifying installed Python package"
source "$install_root/$version/python/bin/activate"
python - "$install_root/$version" <<'PY'
import inspect
import pathlib
import sys

import casars
from casars import tasks
from casars.parameters import TaskParameters

suite = pathlib.Path(sys.argv[1]).resolve()
assert casars.__version__
for name in ("calibrate", "importvla", "msexplore", "imager", "immoments", "impv", "imsubimage", "immath", "exportfits", "importfits"):
    assert callable(getattr(tasks, name))
assert "vis" in inspect.signature(tasks.calibrate).parameters
assert TaskParameters.defaults("calibrate").surface == "calibrate"

# The suite's Python runs the suite's own executable.
completion = tasks.flagmanager(vis="mssel_test_small.ms", mode="list")
assert completion.successful, completion.describe()
executable = pathlib.Path(completion.command[0]).resolve()
assert executable.parent == suite / "bin", executable
assert completion.value[0]["name"] == "main", completion.value
PY
deactivate

echo "==> Suite install smoke test completed"
