#!/usr/bin/env bash

set -euo pipefail

MODE="run"
OPEN_PROJECT=""
OPEN_TUTORIAL_PACK=""
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
APP_RESOURCES="$APP_CONTENTS/Resources"
APP_BINARY="$APP_MACOS/$APP_NAME"
IMAGER_HELPER="$APP_MACOS/casars-imager"
INFO_PLIST="$APP_CONTENTS/Info.plist"
APP_ICON_SOURCE="$REPO_ROOT/branding/app-icon/casa-rs-app-icon.icns"
APP_ICON_NAME="casa-rs-app-icon.icns"
FRONTEND_DYLIB_NAME="libcasars_frontend_services.dylib"
BUILD_CONFIGURATION="release"
RUST_PROFILE_DIR="$REPO_ROOT/target/release"
FRONTEND_DYLIB="$RUST_PROFILE_DIR/$FRONTEND_DYLIB_NAME"
IMAGER_BINARY="$RUST_PROFILE_DIR/casars-imager"
TUTORIAL_DATA_ROOT="${CASA_RS_TUTORIAL_DATA_ROOT:-$HOME/SoftwareProjects/casa-tutorial-data}"
TUTORIAL_DEMO_ARCHIVE="$TUTORIAL_DATA_ROOT/tutorial-parity/alma/first-look/twhya/twhya_calibrated.ms.tar"
FALLBACK_REAL_PROJECT_FIXTURE="$REPO_ROOT/crates/casa-ms/tests/fixtures/mssel_test_small_multifield_spw.ms.tgz"
TEMP_REAL_PROJECT=""
TEMP_REAL_PROJECT_SOURCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    run|--debug|debug|--logs|logs|--verify|verify|--stage-only|stage)
      MODE="$1"
      shift
      ;;
    --project|--open-project)
      if [[ $# -lt 2 ]]; then
        echo "missing path after $1" >&2
        exit 2
      fi
      OPEN_PROJECT="$2"
      OPEN_TUTORIAL_PACK=""
      USE_TEMP_REAL_PROJECT="0"
      shift 2
      ;;
    --tutorial-pack|--open-tutorial-pack)
      if [[ $# -lt 2 ]]; then
        echo "missing path after $1" >&2
        exit 2
      fi
      OPEN_TUTORIAL_PACK="$2"
      OPEN_PROJECT=""
      USE_TEMP_REAL_PROJECT="0"
      shift 2
      ;;
    --empty)
      OPEN_PROJECT=""
      USE_TEMP_REAL_PROJECT="0"
      shift
      ;;
    *)
      echo "usage: $0 [run|--debug|--logs|--verify|--stage-only] [--project PATH|--tutorial-pack PATH|--empty]" >&2
      exit 2
      ;;
  esac
done

stage_temp_real_project() {
  if [[ "$USE_TEMP_REAL_PROJECT" != "1" || -n "$OPEN_PROJECT" ]]; then
    return
  fi

  local archive=""
  local archive_kind=""
  if [[ -f "$TUTORIAL_DEMO_ARCHIVE" ]]; then
    archive="$TUTORIAL_DEMO_ARCHIVE"
    archive_kind="tutorial TW Hya MeasurementSet"
  elif [[ -f "$FALLBACK_REAL_PROJECT_FIXTURE" ]]; then
    archive="$FALLBACK_REAL_PROJECT_FIXTURE"
    archive_kind="bundled fallback MeasurementSet"
  else
    echo "warning: no real demo MeasurementSet found; opening empty workbench" >&2
    echo "warning: checked $TUTORIAL_DEMO_ARCHIVE" >&2
    echo "warning: checked $FALLBACK_REAL_PROJECT_FIXTURE" >&2
    return
  fi

  TEMP_REAL_PROJECT="$(mktemp -d "${TMPDIR:-/tmp}/casars-mac-real-project.XXXXXX")"
  TEMP_REAL_PROJECT_SOURCE="$archive"
  case "$archive" in
    *.tgz|*.tar.gz)
      tar -xzf "$archive" -C "$TEMP_REAL_PROJECT"
      ;;
    *)
      tar -xf "$archive" -C "$TEMP_REAL_PROJECT"
      ;;
  esac
  OPEN_PROJECT="$TEMP_REAL_PROJECT"
  echo "==> Opening temporary real-data project: $OPEN_PROJECT"
  echo "==> Source: $archive_kind ($TEMP_REAL_PROJECT_SOURCE)"
  echo "==> Project will be removed after $APP_NAME exits."
  trap cleanup_temp_real_project_now EXIT
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

  nohup /bin/sh -c '
    app_pid="$1"
    project="$2"
    while kill -0 "$app_pid" 2>/dev/null; do
      sleep 2
    done
    rm -rf "$project"
  ' sh "$app_pid" "$TEMP_REAL_PROJECT" >/dev/null 2>&1 &
  trap - EXIT
}

cd "$REPO_ROOT"
if [[ "$MODE" == "--stage-only" || "$MODE" == "stage" ]]; then
  USE_TEMP_REAL_PROJECT="0"
fi
stage_temp_real_project
"$REPO_ROOT/scripts/generate-frontend-bindings.sh" "$REPO_ROOT/target/frontend-bindings"
cargo build --release -p casars-frontend-services -p casars-imager

cd "$ROOT_DIR"

pkill -x "$APP_NAME" >/dev/null 2>&1 || true

swift build -c "$BUILD_CONFIGURATION"
BUILD_BINARY="$(swift build -c "$BUILD_CONFIGURATION" --show-bin-path)/$APP_NAME"

rm -rf "$APP_BUNDLE"
mkdir -p "$APP_MACOS" "$APP_FRAMEWORKS" "$APP_RESOURCES"
cp "$BUILD_BINARY" "$APP_BINARY"
cp "$FRONTEND_DYLIB" "$APP_FRAMEWORKS/$FRONTEND_DYLIB_NAME"
cp "$IMAGER_BINARY" "$IMAGER_HELPER"
cp "$APP_ICON_SOURCE" "$APP_RESOURCES/$APP_ICON_NAME"
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
  <key>CFBundleIconFile</key>
  <string>$APP_ICON_NAME</string>
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
  local wait_for_exit="${1:-0}"
  local open_flags=(-n)
  if [[ "$wait_for_exit" == "1" ]]; then
    open_flags=(-W -n)
  fi

  if [[ -n "$OPEN_TUTORIAL_PACK" ]]; then
    /usr/bin/open "${open_flags[@]}" "$APP_BUNDLE" --args --open-tutorial-pack "$OPEN_TUTORIAL_PACK"
  elif [[ -n "$OPEN_PROJECT" ]]; then
    /usr/bin/open "${open_flags[@]}" "$APP_BUNDLE" --args --open-project "$OPEN_PROJECT"
  else
    /usr/bin/open "${open_flags[@]}" "$APP_BUNDLE"
  fi
}

launched_app_pid() {
  pgrep -n -x "$APP_NAME"
}

debug_app() {
  if [[ -n "$OPEN_TUTORIAL_PACK" ]]; then
    lldb -- "$APP_BINARY" --open-tutorial-pack "$OPEN_TUTORIAL_PACK"
  elif [[ -n "$OPEN_PROJECT" ]]; then
    lldb -- "$APP_BINARY" --open-project "$OPEN_PROJECT"
  else
    lldb -- "$APP_BINARY"
  fi
}

case "$MODE" in
  run)
    open_app 1
    cleanup_temp_real_project_now
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
    if [[ -n "$TEMP_REAL_PROJECT" ]]; then
      kill "$app_pid" >/dev/null 2>&1 || true
      sleep 2
      cleanup_temp_real_project_now
    else
      schedule_temp_real_project_cleanup "$app_pid"
    fi
    ;;
  --stage-only|stage)
    echo "==> Staged $APP_BUNDLE"
    ;;
  *)
    echo "usage: $0 [run|--debug|--logs|--verify|--stage-only] [--project PATH|--tutorial-pack PATH|--empty]" >&2
    exit 2
    ;;
esac
