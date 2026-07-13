#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
ARTIFACT_ROOT="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}"
DERIVED_DATA="$ARTIFACT_ROOT/DerivedData"
RESULT_BUNDLE="$ARTIFACT_ROOT/CasarsMacUITests.xcresult"
DESTINATION="${CASA_RS_GUI_TEST_DESTINATION:-platform=macOS,arch=$(uname -m)}"
COUNTDOWN_SECONDS="${CASA_RS_GUI_TEST_COUNTDOWN_SECONDS:-10}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  echo "just gui-test requires macOS with Xcode and an interactive GUI session" >&2
  exit 2
fi

command -v xcodebuild >/dev/null

if [[ -z "${CASA_RS_GUI_TEST_PYTHON:-}" ]]; then
  CASA_RS_GUI_TEST_PYTHON="$("$REPO_ROOT/scripts/resolve-python.sh" 3.10)"
fi
export CASA_RS_GUI_TEST_PYTHON

if ! [[ "$COUNTDOWN_SECONDS" =~ ^[0-9]+$ ]]; then
  echo "CASA_RS_GUI_TEST_COUNTDOWN_SECONDS must be a non-negative integer" >&2
  exit 2
fi

rm -rf "$RESULT_BUNDLE"
mkdir -p "$ARTIFACT_ROOT"

cd "$REPO_ROOT"
CARGO_INCREMENTAL=0 cargo build -p casars-frontend-services

echo "==> Building CasarsMacUITests without launching the app"
echo "==> Destination: $DESTINATION"
echo "==> Result bundle: $RESULT_BUNDLE"
echo "==> Python: $CASA_RS_GUI_TEST_PYTHON"

xcodebuild build-for-testing \
  -project "$ROOT_DIR/CasarsMac.xcodeproj" \
  -scheme CasarsMacGUI \
  -configuration Debug \
  -destination "$DESTINATION" \
  -derivedDataPath "$DERIVED_DATA"

notify_local() {
  if [[ -n "${CI:-}" || "${CASA_RS_GUI_TEST_EXCLUSIVE_NOTICE:-1}" == "0" ]]; then
    return
  fi
  /usr/bin/osascript -e "display notification \"$1\" with title \"casa-rs GUI tests\"" \
    >/dev/null 2>&1 || true
}

if [[ -z "${CI:-}" && "${CASA_RS_GUI_TEST_EXCLUSIVE_NOTICE:-1}" != "0" ]]; then
  echo
  echo "==> EXCLUSIVE GUI TEST WINDOW"
  echo "==> Do not use the keyboard, mouse, or switch applications until the completion notification."
  notify_local "Starting an exclusive foreground test window. Please leave the Mac idle."
  for ((remaining = COUNTDOWN_SECONDS; remaining > 0; remaining--)); do
    printf "\r==> Starting all GUI tests in %2d second(s)..." "$remaining"
    sleep 1
  done
  printf "\r==> Starting all GUI tests now.                 \n\a"
fi

echo "==> Running all CasarsMacUITests as one foreground batch"
if NSUnbufferedIO=YES xcodebuild test-without-building \
  -project "$ROOT_DIR/CasarsMac.xcodeproj" \
  -scheme CasarsMacGUI \
  -configuration Debug \
  -destination "$DESTINATION" \
  -derivedDataPath "$DERIVED_DATA" \
  -resultBundlePath "$RESULT_BUNDLE"
then
  notify_local "All GUI tests passed. You can use the Mac again."
  echo "==> GUI test window complete: PASS"
else
  status=$?
  notify_local "GUI tests failed. You can use the Mac again; inspect the xcresult bundle."
  echo "==> GUI test window complete: FAIL" >&2
  exit "$status"
fi
