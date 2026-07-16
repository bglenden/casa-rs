#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/test-gui-remote.sh [gui-test|notebook-roundtrip-gui|tutorial-journey-gui]

Run one native macOS GUI gate on a dedicated, logged-in remote Mac. The local
HEAD must be clean and pushed to its same-named origin branch. Required setup:

  CASA_RS_GUI_TEST_REMOTE=user@host

Optional configuration:

  CASA_RS_GUI_TEST_REMOTE_IDENTITY=/absolute/path/to/ssh-key
  CASA_RS_GUI_TEST_REMOTE_ROOT=/absolute/path/to/remote/checkout
  CASA_RS_GUI_TEST_REMOTE_STORAGE=/absolute/path/to/remote/build-storage
  CASA_RS_GUI_TEST_REMOTE_DERIVED_DATA=/absolute/path/to/xcode-derived-data
  CASA_RS_GUI_TEST_REMOTE_SIGNING_CONFIG=/absolute/path/to/signing-config
  CASA_RS_GUI_TEST_REMOTE_DEVELOPER_DIR=/Applications/Xcode.app/Contents/Developer
  CASA_RS_GUI_TEST_REMOTE_PYTHON=/absolute/path/to/python
  CASA_RS_GUI_TEST_REMOTE_CODEX=/absolute/path/to/codex
  CASA_RS_GUI_TEST_REMOTE_ONLY=TestTarget/TestClass/testMethod
EOF
}

repo_root="$(git rev-parse --show-toplevel)"
mode="${1:-gui-test}"
remote="${CASA_RS_GUI_TEST_REMOTE:-}"
remote_root="${CASA_RS_GUI_TEST_REMOTE_ROOT:-@HOME@/Library/Caches/casa-rs-gui-worker/source}"
remote_storage="${CASA_RS_GUI_TEST_REMOTE_STORAGE:-/Volumes/Extra Storage (not encrypted)/casa-rs-gui-worker-state}"
remote_derived_data="${CASA_RS_GUI_TEST_REMOTE_DERIVED_DATA:-@HOME@/Library/Developer/Xcode/DerivedData/casa-rs-gui-worker}"
remote_signing_config="${CASA_RS_GUI_TEST_REMOTE_SIGNING_CONFIG:-@HOME@/.config/casa-rs/gui-worker-signing.env}"
developer_dir="${CASA_RS_GUI_TEST_REMOTE_DEVELOPER_DIR:-/Applications/Xcode.app/Contents/Developer}"
remote_python="${CASA_RS_GUI_TEST_REMOTE_PYTHON:-}"
remote_codex="${CASA_RS_GUI_TEST_REMOTE_CODEX:-}"
remote_only="${CASA_RS_GUI_TEST_REMOTE_ONLY:-}"

case "$mode" in
  gui-test | notebook-roundtrip-gui | tutorial-journey-gui) ;;
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
echo "==> Xcode build cache: $remote_derived_data"
echo "==> Build cache: $remote_target"
echo "==> Artifacts: $remote_artifacts"

remote_args=(
  "$remote_root" "$remote_storage" "$remote_derived_data" "$remote_artifacts"
  "$remote_target" "$developer_dir" "$branch" "$revision" "$mode"
  "$remote_python" "$remote_codex" "$remote_only" "$remote_signing_config"
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

expand_remote_home() {
  case "$1" in
    @HOME@/*) printf '%s/%s' "$HOME" "${1#@HOME@/}" ;;
    *) printf '%s' "$1" ;;
  esac
}

repo_root="$(expand_remote_home "$(decode_arg "$1")")"
storage_root="$(expand_remote_home "$(decode_arg "$2")")"
derived_data="$(expand_remote_home "$(decode_arg "$3")")"
artifact_root="$(expand_remote_home "$(decode_arg "$4")")"
target_dir="$(expand_remote_home "$(decode_arg "$5")")"
developer_dir="$(expand_remote_home "$(decode_arg "$6")")"
branch="$(decode_arg "$7")"
revision="$(decode_arg "$8")"
mode="$(decode_arg "$9")"
python_command="$(expand_remote_home "$(decode_arg "${10}")")"
codex_command="$(expand_remote_home "$(decode_arg "${11}")")"
only_testing="$(decode_arg "${12}")"
signing_config="$(expand_remote_home "$(decode_arg "${13}")")"

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
if [[ ! -f "$signing_config" ]]; then
  echo "stable GUI-worker signing is not configured: $signing_config" >&2
  echo "run scripts/setup-gui-remote-signing.sh once on the worker" >&2
  exit 2
fi
# shellcheck disable=SC1090 -- private worker config created by the setup script.
source "$signing_config"
: "${CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY:?missing code-sign identity in $signing_config}"
: "${CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN:?missing keychain path in $signing_config}"
: "${CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD:?missing keychain password in $signing_config}"
/usr/bin/security unlock-keychain \
  -p "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD" \
  "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN"
if ! /usr/bin/security find-identity -v -p codesigning \
  "$CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN" |
  /usr/bin/grep -Fq "$CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY"
then
  echo "stable GUI-worker signing identity is unavailable" >&2
  exit 2
fi
export CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY
export CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN
unset CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD
checkout_changes="$(git -C "$repo_root" status --porcelain --untracked-files=all)"
repo_target="$repo_root/target"
if [[ -L "$repo_target" && "$repo_target" -ef "$target_dir" ]]; then
  checkout_changes="$(printf '%s\n' "$checkout_changes" | /usr/bin/awk '$0 != "?? target"')"
fi
if [[ -n "$checkout_changes" ]]; then
  echo "remote GUI checkout has local changes: $repo_root" >&2
  printf '%s\n' "$checkout_changes" >&2
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
  tutorial-journey-gui)
    bash apps/casars-mac/script/test_tutorial_journey_gui.sh
    ;;
esac
REMOTE_RUN
status=$?
set -e

echo "==> Remote artifacts retained at $remote:$remote_artifacts"
if [[ "$status" == "0" && ( "$mode" == "notebook-roundtrip-gui" || "$mode" == "tutorial-journey-gui" ) ]]; then
  if [[ "$mode" == "notebook-roundtrip-gui" ]]; then
    report_name="NotebookRoundTripGUI.report.json"
  else
    report_name="TutorialJourneyGUI.report.json"
  fi
  local_report="$repo_root/apps/casars-mac/.gui-test/remote/$report_name"
  mkdir -p "$(dirname "$local_report")"
  encoded_report="$(encode_remote_arg "$remote_artifacts/$report_name")"
  ssh "${ssh_args[@]}" "$remote" /bin/bash -s -- \
    "$encoded_report" >"$local_report" <<'REMOTE_REPORT'
set -euo pipefail
report_path="$(printf '%s' "${1#x}" | /usr/bin/base64 -D)"
cat "$report_path"
REMOTE_REPORT
  echo "==> Copied sanitized report to $local_report"
fi

exit "$status"
