#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
ARTIFACT_ROOT="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}"
DERIVED_DATA="$ARTIFACT_ROOT/DerivedData"
RESULT_BUNDLE="$ARTIFACT_ROOT/CasarsMacUITests.xcresult"
DESTINATION="${CASA_RS_GUI_TEST_DESTINATION:-platform=macOS,arch=$(uname -m)}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "just gui-test requires macOS with Xcode and an interactive GUI session" >&2
  exit 2
fi

command -v xcodebuild >/dev/null

rm -rf "$RESULT_BUNDLE"
mkdir -p "$ARTIFACT_ROOT"

cd "$REPO_ROOT"
CARGO_INCREMENTAL=0 cargo build -p casars-frontend-services

echo "==> Running CasarsMacUITests"
echo "==> Destination: $DESTINATION"
echo "==> Result bundle: $RESULT_BUNDLE"

NSUnbufferedIO=YES xcodebuild test \
  -project "$ROOT_DIR/CasarsMac.xcodeproj" \
  -scheme CasarsMacGUI \
  -configuration Debug \
  -destination "$DESTINATION" \
  -derivedDataPath "$DERIVED_DATA" \
  -resultBundlePath "$RESULT_BUNDLE"
