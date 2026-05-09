#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: apps/casars-mac/script/install-local-gui.sh [options]

Builds and installs the local Swift GUI app bundle.

Options:
  --install-root <dir>      Suite install root
                            (default: ~/.local/opt/casa-rs)
  --bin-dir <dir>           PATH-facing launcher directory
                            (default: ~/.local/bin)
  --activate                Update current and casars-mac launcher links
  --no-activate             Do not update current or launcher links
  --force                   Replace any existing installed GUI app for this version
  --verify                  Launch the installed app briefly after install
  --python <python>         Accepted for parity with install-local; ignored
  --keep-dist               Accepted for parity with install-local; ignored
  -h, --help                Show this help
EOF
}

die() {
  echo "install-local-gui.sh: $*" >&2
  exit 1
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "$ROOT_DIR/../.." && pwd)"
APP_NAME="casars-mac"
SOURCE_APP="$ROOT_DIR/dist/$APP_NAME.app"

install_root="$HOME/.local/opt/casa-rs"
bin_dir="$HOME/.local/bin"
activate="true"
force="false"
verify="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-root)
      install_root="${2:-}"
      shift 2
      ;;
    --bin-dir)
      bin_dir="${2:-}"
      shift 2
      ;;
    --activate)
      activate="true"
      shift
      ;;
    --no-activate)
      activate="false"
      shift
      ;;
    --force)
      force="true"
      shift
      ;;
    --verify)
      verify="true"
      shift
      ;;
    --python)
      shift 2
      ;;
    --keep-dist)
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      die "unexpected argument: $1"
      ;;
  esac
done

[[ "$(uname -s)" == "Darwin" ]] || die "Swift GUI local install is macOS-only"

version="$(
  sed -n 's/^version = "\(.*\)"/\1/p' "$REPO_ROOT/Cargo.toml" | head -n 1
)"
[[ -n "$version" ]] || die "failed to determine workspace version from Cargo.toml"

echo "==> Building local Swift GUI app bundle"
bash "$ROOT_DIR/script/build_and_run.sh" --stage-only

install_dir="$install_root/$version"
applications_dir="$install_dir/Applications"
target_app="$applications_dir/$APP_NAME.app"
if [[ -e "$target_app" ]]; then
  if [[ "$force" == "true" ]]; then
    rm -rf "$target_app"
  else
    die "installed GUI app already exists: $target_app (use --force to replace it)"
  fi
fi

mkdir -p "$applications_dir" "$bin_dir"
cp -R "$SOURCE_APP" "$target_app"

if [[ "$activate" == "true" ]]; then
  ln -sfn "$install_dir" "$install_root/current"
  launcher="$bin_dir/$APP_NAME"
  cat >"$launcher" <<EOF
#!/usr/bin/env bash
exec /usr/bin/open -n "$install_root/current/Applications/$APP_NAME.app" --args "\$@"
EOF
  chmod +x "$launcher"
fi

echo "Installed $APP_NAME $version"
echo "  app: $target_app"
if [[ "$activate" == "true" ]]; then
  echo "  launcher: $bin_dir/$APP_NAME"
fi

if [[ "$verify" == "true" ]]; then
  echo "==> Verifying installed Swift GUI app"
  /usr/bin/open -n "$target_app"
  sleep 1
  app_pid="$(pgrep -n -x "$APP_NAME" || true)"
  [[ -n "$app_pid" ]] || die "failed to launch $APP_NAME"
  kill "$app_pid" >/dev/null 2>&1 || true
fi
