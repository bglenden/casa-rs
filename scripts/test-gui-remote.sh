#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/test-gui-remote.sh [gui-test|notebook-roundtrip-gui]

Run one native macOS GUI gate on a dedicated, logged-in remote Mac. The local
HEAD must be clean and pushed to its same-named origin branch. Required setup:

  CASA_RS_GUI_TEST_REMOTE=user@host

Optional configuration:

  CASA_RS_GUI_TEST_REMOTE_IDENTITY=/absolute/path/to/ssh-key
  CASA_RS_GUI_TEST_REMOTE_ROOT=/absolute/path/to/remote/checkout
  CASA_RS_GUI_TEST_REMOTE_STORAGE=/absolute/path/to/remote/build-storage
  CASA_RS_GUI_TEST_REMOTE_DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer
  CASA_RS_GUI_TEST_REMOTE_PYTHON=/absolute/path/to/python
  CASA_RS_GUI_TEST_REMOTE_CODEX=/absolute/path/to/codex
  CASA_RS_GUI_TEST_REMOTE_ONLY=TestTarget/TestClass/testMethod
EOF
}

repo_root="$(git rev-parse --show-toplevel)"
mode="${1:-gui-test}"
remote="${CASA_RS_GUI_TEST_REMOTE:-}"
remote_root="${CASA_RS_GUI_TEST_REMOTE_ROOT:-/Volumes/Extra Storage (not encrypted)/casa-rs-gui-worker}"
remote_storage="${CASA_RS_GUI_TEST_REMOTE_STORAGE:-/Volumes/Extra Storage (not encrypted)/casa-rs-gui-worker-state}"
developer_dir="${CASA_RS_GUI_TEST_REMOTE_DEVELOPER_DIR:-/Applications/Xcode.app/Contents/Developer}"
remote_python="${CASA_RS_GUI_TEST_REMOTE_PYTHON:-}"
remote_codex="${CASA_RS_GUI_TEST_REMOTE_CODEX:-}"
remote_only="${CASA_RS_GUI_TEST_REMOTE_ONLY:-}"

case "$mode" in
  gui-test | notebook-roundtrip-gui) ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac

if [[ -z "$remote" ]]; then
  echo "CASA_RS_GUI_TEST_REMOTE=user@host is required" >&2
  exit 2
fi

if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  echo "remote GUI gates require a clean local checkout" >&2
  exit 2
fi

branch="$(git -C "$repo_root" branch --show-current)"
revision="$(git -C "$repo_root" rev-parse HEAD)"
if [[ -z "$branch" ]]; then
  echo "remote GUI gates require a named local branch" >&2
  exit 2
fi

ssh_args=(
  -o BatchMode=yes
  -o ConnectTimeout=10
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=4
)
if [[ -n "${CASA_RS_GUI_TEST_REMOTE_IDENTITY:-}" ]]; then
  ssh_args+=(-i "$CASA_RS_GUI_TEST_REMOTE_IDENTITY")
fi

encode_remote_arg() {
  printf 'x'
  printf '%s' "$1" | /usr/bin/base64
}

run_id="$(date -u +%Y%m%dT%H%M%SZ)-${revision:0:12}-$mode"
remote_artifacts="$remote_storage/artifacts/$run_id"
remote_target="$remote_storage/target"

echo "==> Remote GUI worker: $remote"
echo "==> Revision: $revision ($branch)"
echo "==> Checkout: $remote_root"
echo "==> Build cache: $remote_target"
echo "==> Artifacts: $remote_artifacts"

remote_args=(
  "$remote_root" "$remote_storage" "$remote_artifacts" "$remote_target"
  "$developer_dir" "$branch" "$revision" "$mode" "$remote_python"
  "$remote_codex" "$remote_only"
)
encoded_remote_args=()
for arg in "${remote_args[@]}"; do
  encoded_remote_args+=("$(encode_remote_arg "$arg")")
done

set +e
ssh "${ssh_args[@]}" "$remote" /bin/bash -s -- "${encoded_remote_args[@]}" <<'REMOTE_RUN'
set -euo pipefail

decode_arg() {
  printf '%s' "${1#x}" | /usr/bin/base64 -D
}

repo_root="$(decode_arg "$1")"
storage_root="$(decode_arg "$2")"
artifact_root="$(decode_arg "$3")"
target_dir="$(decode_arg "$4")"
derived_data="$storage_root/DerivedData"
developer_dir="$(decode_arg "$5")"
branch="$(decode_arg "$6")"
revision="$(decode_arg "$7")"
mode="$(decode_arg "$8")"
python_command="$(decode_arg "$9")"
codex_command="$(decode_arg "${10}")"
only_testing="$(decode_arg "${11}")"

export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export DEVELOPER_DIR="$developer_dir"
export CARGO_INCREMENTAL=0
export CARGO_TARGET_DIR="$target_dir"
export CASA_RS_GUI_TEST_ARTIFACT_ROOT="$artifact_root"
export CASA_RS_GUI_TEST_DERIVED_DATA="$derived_data"
export CASA_RS_GUI_TEST_COUNTDOWN_SECONDS=0
export CASA_RS_GUI_TEST_EXCLUSIVE_NOTICE=0

git -C "$repo_root" rev-parse --git-dir >/dev/null 2>&1 || {
  echo "remote GUI checkout is missing: $repo_root" >&2
  exit 2
}
[[ -x "$developer_dir/usr/bin/xcodebuild" ]] || {
  echo "full Xcode is missing: $developer_dir" >&2
  exit 2
}
if ! /usr/sbin/DevToolsSecurity -status 2>&1 | /usr/bin/grep -q enabled; then
  echo "Developer Tools mode is disabled on the remote Mac" >&2
  echo "run: sudo /usr/sbin/DevToolsSecurity -enable" >&2
  exit 2
fi
if ! "$developer_dir/usr/bin/xcodebuild" -checkFirstLaunchStatus; then
  echo "Xcode license/first-launch setup is incomplete on the remote Mac" >&2
  exit 2
fi
if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  echo "remote GUI checkout has local changes: $repo_root" >&2
  exit 2
fi

git -C "$repo_root" fetch --quiet origin "refs/heads/$branch"
remote_revision="$(git -C "$repo_root" rev-parse FETCH_HEAD)"
if [[ "$remote_revision" != "$revision" ]]; then
  echo "local revision $revision is not the pushed tip of origin/$branch ($remote_revision)" >&2
  exit 2
fi
git -C "$repo_root" switch --quiet --detach "$revision"

mkdir -p "$storage_root" "$artifact_root" "$target_dir" "$derived_data"
repo_target="$repo_root/target"
if [[ -e "$repo_target" || -L "$repo_target" ]]; then
  if [[ ! -L "$repo_target" || ! "$repo_target" -ef "$target_dir" ]]; then
    echo "remote checkout target must be absent or link to $target_dir" >&2
    exit 2
  fi
else
  ln -s "$target_dir" "$repo_target"
fi
cd "$repo_root"

/usr/bin/caffeinate -dimsu -w $$ >/dev/null 2>&1 &
caffeinate_pid=$!
stop_caffeinate() {
  kill "$caffeinate_pid" >/dev/null 2>&1 || true
  wait "$caffeinate_pid" 2>/dev/null || true
}
trap stop_caffeinate EXIT

if [[ -n "$python_command" ]]; then
  export CASA_RS_GUI_TEST_PYTHON="$python_command"
fi
if [[ -n "$codex_command" ]]; then
  export CASA_RS_CODEX_COMMAND="$codex_command"
fi
if [[ -n "$only_testing" ]]; then
  export CASA_RS_GUI_TEST_ONLY="$only_testing"
fi

case "$mode" in
  gui-test)
    bash apps/casars-mac/script/test_gui.sh
    ;;
  notebook-roundtrip-gui)
    bash apps/casars-mac/script/test_notebook_roundtrip_gui.sh
    ;;
esac
REMOTE_RUN
status=$?
set -e

echo "==> Remote artifacts retained at $remote:$remote_artifacts"
if [[ "$mode" == "notebook-roundtrip-gui" && "$status" == "0" ]]; then
  local_report="$repo_root/apps/casars-mac/.gui-test/remote/NotebookRoundTripGUI.report.json"
  mkdir -p "$(dirname "$local_report")"
  encoded_report="$(encode_remote_arg "$remote_artifacts/NotebookRoundTripGUI.report.json")"
  ssh "${ssh_args[@]}" "$remote" /bin/bash -s -- \
    "$encoded_report" >"$local_report" <<'REMOTE_REPORT'
set -euo pipefail
report_path="$(printf '%s' "${1#x}" | /usr/bin/base64 -D)"
cat "$report_path"
REMOTE_REPORT
  echo "==> Copied sanitized report to $local_report"
fi

exit "$status"
