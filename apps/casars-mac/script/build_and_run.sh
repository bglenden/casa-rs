#!/usr/bin/env bash

set -euo pipefail

MODE="run"
OPEN_PROJECT=""
USE_TEMP_REAL_PROJECT="1"
APP_NAME="casars-mac"
BUNDLE_ID="org.casa-rs.casars-mac"
MIN_SYSTEM_VERSION="14.0"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"
APP_BUNDLE="$DIST_DIR/$APP_NAME.app"
APP_CONTENTS="$APP_BUNDLE/Contents"
APP_MACOS="$APP_CONTENTS/MacOS"
APP_FRAMEWORKS="$APP_CONTENTS/Frameworks"
APP_BINARY="$APP_MACOS/$APP_NAME"
IMAGER_HELPER="$APP_MACOS/casars-imager"
INFO_PLIST="$APP_CONTENTS/Info.plist"
FRONTEND_DYLIB_NAME="libcasars_frontend_services.dylib"
FRONTEND_DYLIB="$REPO_ROOT/target/debug/$FRONTEND_DYLIB_NAME"
IMAGER_BINARY="$REPO_ROOT/target/debug/casars-imager"
REAL_PROJECT_FIXTURE="$REPO_ROOT/crates/casa-ms/tests/fixtures/mssel_test_small_multifield_spw.ms.tgz"
TEMP_REAL_PROJECT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    run|--debug|debug|--logs|logs|--verify|verify)
      MODE="$1"
      shift
      ;;
    --project|--open-project)
      if [[ $# -lt 2 ]]; then
        echo "missing path after $1" >&2
        exit 2
      fi
      OPEN_PROJECT="$2"
      USE_TEMP_REAL_PROJECT="0"
      shift 2
      ;;
    --empty)
      OPEN_PROJECT=""
      USE_TEMP_REAL_PROJECT="0"
      shift
      ;;
    *)
      echo "usage: $0 [run|--debug|--logs|--verify] [--project PATH|--empty]" >&2
      exit 2
      ;;
  esac
done

stage_temp_real_project() {
  if [[ "$USE_TEMP_REAL_PROJECT" != "1" || -n "$OPEN_PROJECT" ]]; then
    return
  fi
  if [[ ! -f "$REAL_PROJECT_FIXTURE" ]]; then
    echo "warning: real demo fixture not found; opening empty workbench: $REAL_PROJECT_FIXTURE" >&2
    return
  fi

  TEMP_REAL_PROJECT="$(mktemp -d "${TMPDIR:-/tmp}/casars-mac-real-project.XXXXXX")"
  tar -xzf "$REAL_PROJECT_FIXTURE" -C "$TEMP_REAL_PROJECT"
  OPEN_PROJECT="$TEMP_REAL_PROJECT"
  echo "==> Opening temporary real-data project: $OPEN_PROJECT"
  echo "==> Project will be removed after $APP_NAME exits."
}

cleanup_temp_real_project_now() {
  if [[ -n "$TEMP_REAL_PROJECT" && -d "$TEMP_REAL_PROJECT" ]]; then
    rm -rf "$TEMP_REAL_PROJECT"
  fi
}

schedule_temp_real_project_cleanup() {
  if [[ -z "$TEMP_REAL_PROJECT" || ! -d "$TEMP_REAL_PROJECT" ]]; then
    return
  fi

  local app_pid="${1:-}"
  if [[ -z "$app_pid" ]]; then
    return
  fi

  (
    while ps -p "$app_pid" >/dev/null 2>&1; do
      sleep 2
    done
    rm -rf "$TEMP_REAL_PROJECT"
  ) >/dev/null 2>&1 &
}

cd "$REPO_ROOT"
stage_temp_real_project
"$REPO_ROOT/scripts/generate-frontend-bindings.sh" "$REPO_ROOT/target/frontend-bindings"
cargo build -p casars-imager

cd "$ROOT_DIR"

pkill -x "$APP_NAME" >/dev/null 2>&1 || true

swift build
BUILD_BINARY="$(swift build --show-bin-path)/$APP_NAME"

rm -rf "$APP_BUNDLE"
mkdir -p "$APP_MACOS" "$APP_FRAMEWORKS"
cp "$BUILD_BINARY" "$APP_BINARY"
cp "$FRONTEND_DYLIB" "$APP_FRAMEWORKS/$FRONTEND_DYLIB_NAME"
cp "$IMAGER_BINARY" "$IMAGER_HELPER"
chmod +x "$APP_BINARY" "$IMAGER_HELPER"

frontend_dependency="$(
  otool -L "$APP_BINARY" \
    | awk '/libcasars_frontend_services\.dylib/ {print $1; exit}'
)"
if [[ -n "$frontend_dependency" ]]; then
  install_name_tool \
    -change "$frontend_dependency" "@executable_path/../Frameworks/$FRONTEND_DYLIB_NAME" \
    "$APP_BINARY"
fi

cat >"$INFO_PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleExecutable</key>
  <string>$APP_NAME</string>
  <key>CFBundleIdentifier</key>
  <string>$BUNDLE_ID</string>
  <key>CFBundleName</key>
  <string>casa-rs Workbench</string>
  <key>CFBundlePackageType</key>
  <string>APPL</string>
  <key>LSMinimumSystemVersion</key>
  <string>$MIN_SYSTEM_VERSION</string>
  <key>NSPrincipalClass</key>
  <string>NSApplication</string>
</dict>
</plist>
PLIST

codesign --force --sign - "$APP_FRAMEWORKS/$FRONTEND_DYLIB_NAME" >/dev/null
codesign --force --sign - "$IMAGER_HELPER" >/dev/null
codesign --force --sign - "$APP_BINARY" >/dev/null
codesign --force --sign - "$APP_BUNDLE" >/dev/null

open_app() {
  if [[ -n "$OPEN_PROJECT" ]]; then
    /usr/bin/open -n "$APP_BUNDLE" --args --open-project "$OPEN_PROJECT"
  else
    /usr/bin/open -n "$APP_BUNDLE"
  fi
}

launched_app_pid() {
  pgrep -n -x "$APP_NAME"
}

debug_app() {
  if [[ -n "$OPEN_PROJECT" ]]; then
    lldb -- "$APP_BINARY" --open-project "$OPEN_PROJECT"
  else
    lldb -- "$APP_BINARY"
  fi
}

case "$MODE" in
  run)
    open_app
    sleep 1
    if app_pid="$(launched_app_pid)"; then
      schedule_temp_real_project_cleanup "$app_pid"
    fi
    ;;
  --debug|debug)
    debug_app
    cleanup_temp_real_project_now
    ;;
  --logs|logs)
    open_app
    sleep 1
    if app_pid="$(launched_app_pid)"; then
      schedule_temp_real_project_cleanup "$app_pid"
    fi
    /usr/bin/log stream --info --style compact --predicate "process == \"$APP_NAME\""
    ;;
  --verify|verify)
    open_app
    sleep 1
    app_pid="$(launched_app_pid)"
    [[ -n "$app_pid" ]]
    schedule_temp_real_project_cleanup "$app_pid"
    ;;
  *)
    echo "usage: $0 [run|--debug|--logs|--verify] [--project PATH|--empty]" >&2
    exit 2
    ;;
esac
