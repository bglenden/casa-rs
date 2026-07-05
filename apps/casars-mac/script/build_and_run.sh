#!/usr/bin/env bash

set -euo pipefail

MODE="run"
OPEN_PROJECT=""
OPEN_IMAGER_MS=""
OPEN_TUTORIAL_PACK=""
OPEN_TUTORIAL_SECTION=""
SHOW_IMAGER_PROGRESS_MOCKUP="0"
RUN_ACTIVE_TASK="0"
USE_TEMP_REAL_PROJECT="1"
EXTRA_APP_ARGS=()
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
INFO_PLIST="$APP_CONTENTS/Info.plist"
APP_ICON_SOURCE="$REPO_ROOT/branding/app-icon/casa-rs-app-icon.icns"
APP_ICON_NAME="casa-rs-app-icon.icns"
FRONTEND_DYLIB_NAME="libcasars_frontend_services.dylib"
BUILD_CONFIGURATION="release"
RUST_PROFILE_DIR="$REPO_ROOT/target/release"
FRONTEND_DYLIB="$RUST_PROFILE_DIR/$FRONTEND_DYLIB_NAME"
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
      OPEN_IMAGER_MS=""
      OPEN_TUTORIAL_PACK=""
      USE_TEMP_REAL_PROJECT="0"
      shift 2
      ;;
    --imager-ms|--open-imager-ms)
      if [[ $# -lt 2 ]]; then
        echo "missing MeasurementSet path after $1" >&2
        exit 2
      fi
      OPEN_IMAGER_MS="$2"
      OPEN_PROJECT=""
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
      OPEN_IMAGER_MS=""
      OPEN_PROJECT=""
      USE_TEMP_REAL_PROJECT="0"
      shift 2
      ;;
    --tutorial-section|--open-tutorial-section)
      if [[ $# -lt 2 ]]; then
        echo "missing section id after $1" >&2
        exit 2
      fi
      OPEN_TUTORIAL_SECTION="$2"
      shift 2
      ;;
    --empty)
      OPEN_PROJECT=""
      OPEN_IMAGER_MS=""
      OPEN_TUTORIAL_PACK=""
      USE_TEMP_REAL_PROJECT="0"
      shift
      ;;
    --show-imager-progress-mockup)
      SHOW_IMAGER_PROGRESS_MOCKUP="1"
      shift
      ;;
    --run-active-task)
      RUN_ACTIVE_TASK="1"
      shift
      ;;
    --imagename|--output-prefix|--image-size|--imsize|--image-width|--image-height|--cell-arcsec|--spectral-mode|--specmode|--channel-start|--channel-count|--niter|--threshold-jy|--dirty-only)
      if [[ $# -lt 2 ]]; then
        echo "missing value after $1" >&2
        exit 2
      fi
      EXTRA_APP_ARGS+=("$1" "$2")
      shift 2
      ;;
    *)
      echo "usage: $0 [run|--debug|--logs|--verify|--stage-only] [--project PATH|--imager-ms PATH|--tutorial-pack PATH [--tutorial-section ID]|--empty] [--show-imager-progress-mockup] [--run-active-task] [imager launch overrides]" >&2
      exit 2
      ;;
  esac
done

if [[ -n "$OPEN_TUTORIAL_SECTION" && -z "$OPEN_TUTORIAL_PACK" ]]; then
  echo "--tutorial-section requires --tutorial-pack" >&2
  exit 2
fi

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
cargo build --release -p casars-frontend-services --lib
TASK_HELPER_SPECS=()
if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" != "1" ]]; then
  while IFS= read -r spec; do
    TASK_HELPER_SPECS+=("$spec")
  done < <(
    python3 - "$REPO_ROOT/resources/task-catalog.json" <<'PY'
import json
import sys

catalog = json.load(open(sys.argv[1], encoding="utf-8"))
seen = set()
for task in catalog["tasks"]:
    if not task.get("show_in_swift"):
        continue
    package = task["cargo_package"]
    binary = task["binary_name"]
    key = (package, binary)
    if key in seen:
        continue
    seen.add(key)
    print(f"{package}:{binary}")
PY
  )
fi
if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" != "1" ]]; then
  for spec in "${TASK_HELPER_SPECS[@]}"; do
    package="${spec%%:*}"
    binary="${spec#*:}"
    cargo build --release -p "$package" --bin "$binary"
  done
fi

cd "$ROOT_DIR"

pkill -x "$APP_NAME" >/dev/null 2>&1 || true

swift build -c "$BUILD_CONFIGURATION"
BUILD_BINARY="$(swift build -c "$BUILD_CONFIGURATION" --show-bin-path)/$APP_NAME"

rm -rf "$APP_BUNDLE"
mkdir -p "$APP_MACOS" "$APP_FRAMEWORKS" "$APP_RESOURCES"
cp "$BUILD_BINARY" "$APP_BINARY"
cp "$FRONTEND_DYLIB" "$APP_FRAMEWORKS/$FRONTEND_DYLIB_NAME"
if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" != "1" ]]; then
  for spec in "${TASK_HELPER_SPECS[@]}"; do
    binary="${spec#*:}"
    cp "$RUST_PROFILE_DIR/$binary" "$APP_MACOS/$binary"
  done
fi
cp "$APP_ICON_SOURCE" "$APP_RESOURCES/$APP_ICON_NAME"
chmod +x "$APP_BINARY"
if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" != "1" ]]; then
  for spec in "${TASK_HELPER_SPECS[@]}"; do
    binary="${spec#*:}"
    chmod +x "$APP_MACOS/$binary"
  done
fi

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
if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" != "1" ]]; then
  for spec in "${TASK_HELPER_SPECS[@]}"; do
    binary="${spec#*:}"
    codesign --force --sign - "$APP_MACOS/$binary" >/dev/null
  done
fi
codesign --force --sign - "$APP_BINARY" >/dev/null
codesign --force --sign - "$APP_BUNDLE" >/dev/null

open_app() {
  local wait_for_exit="${1:-0}"
  local open_flags=(-n)
  if [[ "$wait_for_exit" == "1" ]]; then
    open_flags=(-W -n)
  fi

  local app_args=(-ApplePersistenceIgnoreState YES)
  if [[ -n "$OPEN_TUTORIAL_PACK" ]]; then
    app_args+=(--open-tutorial-pack "$OPEN_TUTORIAL_PACK")
    if [[ -n "$OPEN_TUTORIAL_SECTION" ]]; then
      app_args+=(--open-tutorial-section "$OPEN_TUTORIAL_SECTION")
    fi
  elif [[ -n "$OPEN_IMAGER_MS" ]]; then
    app_args+=(--open-imager-ms "$OPEN_IMAGER_MS")
  elif [[ -n "$OPEN_PROJECT" ]]; then
    app_args+=(--open-project "$OPEN_PROJECT")
  fi
  if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" == "1" ]]; then
    app_args+=(--show-imager-progress-mockup)
  fi
  if [[ "$RUN_ACTIVE_TASK" == "1" ]]; then
    app_args+=(--run-active-task)
  fi
  app_args+=("${EXTRA_APP_ARGS[@]}")

  /usr/bin/open "${open_flags[@]}" "$APP_BUNDLE" --args "${app_args[@]}"
}

launched_app_pid() {
  pgrep -n -x "$APP_NAME"
}

debug_app() {
  local app_args=(-ApplePersistenceIgnoreState YES)
  if [[ -n "$OPEN_TUTORIAL_PACK" ]]; then
    app_args+=(--open-tutorial-pack "$OPEN_TUTORIAL_PACK")
    if [[ -n "$OPEN_TUTORIAL_SECTION" ]]; then
      app_args+=(--open-tutorial-section "$OPEN_TUTORIAL_SECTION")
    fi
  elif [[ -n "$OPEN_IMAGER_MS" ]]; then
    app_args+=(--open-imager-ms "$OPEN_IMAGER_MS")
  elif [[ -n "$OPEN_PROJECT" ]]; then
    app_args+=(--open-project "$OPEN_PROJECT")
  fi
  if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" == "1" ]]; then
    app_args+=(--show-imager-progress-mockup)
  fi
  if [[ "$RUN_ACTIVE_TASK" == "1" ]]; then
    app_args+=(--run-active-task)
  fi
  app_args+=("${EXTRA_APP_ARGS[@]}")

  lldb -- "$APP_BINARY" "${app_args[@]}"
}

case "$MODE" in
  run)
    if [[ "$SHOW_IMAGER_PROGRESS_MOCKUP" == "1" ]]; then
      open_app
      sleep 1
      if app_pid="$(launched_app_pid)"; then
        schedule_temp_real_project_cleanup "$app_pid"
      fi
    else
      open_app 1
      cleanup_temp_real_project_now
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
    echo "usage: $0 [run|--debug|--logs|--verify|--stage-only] [--project PATH|--imager-ms PATH|--tutorial-pack PATH|--empty] [--show-imager-progress-mockup] [--run-active-task] [imager launch overrides]" >&2
    exit 2
    ;;
esac
