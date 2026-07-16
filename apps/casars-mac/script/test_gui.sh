#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
ARTIFACT_ROOT="${CASA_RS_GUI_TEST_ARTIFACT_ROOT:-$ROOT_DIR/.gui-test}"
DERIVED_DATA="${CASA_RS_GUI_TEST_DERIVED_DATA:-$ARTIFACT_ROOT/DerivedData}"
RESULT_BUNDLE="${CASA_RS_GUI_TEST_RESULT_BUNDLE:-$ARTIFACT_ROOT/CasarsMacUITests.xcresult}"
DESTINATION="${CASA_RS_GUI_TEST_DESTINATION:-platform=macOS,arch=$(uname -m)}"
COUNTDOWN_SECONDS="${CASA_RS_GUI_TEST_COUNTDOWN_SECONDS:-10}"
ONLY_TESTING="${CASA_RS_GUI_TEST_ONLY:-}"
REUSE_BUILD="${CASA_RS_GUI_TEST_REUSE_BUILD:-0}"
TEST_SELECTION_ARGS=()
TEST_DESCRIPTION="all CasarsMacUITests"

if [[ -n "$ONLY_TESTING" ]]; then
  IFS=',' read -r -a selected_tests <<< "$ONLY_TESTING"
  for selected_test in "${selected_tests[@]}"; do
    TEST_SELECTION_ARGS+=(-only-testing "$selected_test")
  done
  TEST_DESCRIPTION="${#selected_tests[@]} selected GUI test(s)"
fi

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
if [[ "$REUSE_BUILD" == "1" ]]; then
  if [[ ! -d "$DERIVED_DATA/Build/Products/Debug/CasarsMacUITests-Runner.app" ]]; then
    echo "CASA_RS_GUI_TEST_REUSE_BUILD=1 requested but no built UI test products exist" >&2
    exit 2
  fi
  echo "==> Reusing unchanged CasarsMacUITests build products"
else
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
fi

notify_local() {
  if [[ -n "${CI:-}" || "${CASA_RS_GUI_TEST_EXCLUSIVE_NOTICE:-1}" == "0" ]]; then
    return
  fi
  /usr/bin/osascript -e "display notification \"$1\" with title \"casa-rs GUI tests\"" \
    >/dev/null 2>&1 || true
}

codex_hidden=0
codex_guard_pid=""
restore_codex_app() {
  if [[ "$codex_hidden" == "1" ]]; then
    if [[ -n "$codex_guard_pid" ]]; then
      kill "$codex_guard_pid" >/dev/null 2>&1 || true
      wait "$codex_guard_pid" 2>/dev/null || true
    fi
    /usr/bin/osascript -e 'tell application id "com.openai.codex" to activate' \
      >/dev/null 2>&1 || true
  fi
}

hide_codex_app() {
  if [[ -n "${CI:-}" ]]; then return; fi
  if /usr/bin/osascript \
    -e 'tell application "System Events"' \
    -e 'if exists (first application process whose bundle identifier is "com.openai.codex") then' \
    -e 'set visible of (first application process whose bundle identifier is "com.openai.codex") to false' \
    -e 'return "hidden"' \
    -e 'end if' \
    -e 'end tell' 2>/dev/null | /usr/bin/grep -q hidden
  then
    codex_hidden=1
    (
      while true; do
        /usr/bin/osascript \
          -e 'tell application "System Events" to set visible of (first application process whose bundle identifier is "com.openai.codex") to false' \
          >/dev/null 2>&1 || true
        sleep 0.5
      done
    ) &
    codex_guard_pid=$!
    trap restore_codex_app EXIT
  fi
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

hide_codex_app

echo "==> Running $TEST_DESCRIPTION as one foreground batch"
if NSUnbufferedIO=YES xcodebuild test-without-building \
  -project "$ROOT_DIR/CasarsMac.xcodeproj" \
  -scheme CasarsMacGUI \
  -configuration Debug \
  -destination "$DESTINATION" \
  -derivedDataPath "$DERIVED_DATA" \
  -resultBundlePath "$RESULT_BUNDLE" \
  ${TEST_SELECTION_ARGS[@]+"${TEST_SELECTION_ARGS[@]}"}
then
  notify_local "GUI tests passed. You can use the Mac again."
  echo "==> GUI test window complete: PASS"
else
  status=$?
  notify_local "GUI tests failed. You can use the Mac again; inspect the xcresult bundle."
  echo "==> GUI test window complete: FAIL" >&2
  exit "$status"
fi
