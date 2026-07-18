#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

usage() {
  printf '%s\n' \
    'Usage: scripts/test-gui-remote.sh [gui-test|notebook-roundtrip-gui|tutorial-journey-gui]' \
    '' \
    'Run the selected checked-in GUI journey contract at the exact local revision.' \
    'CASA_RS_GUI_TEST_REMOTE=user@host is required.'
}

repo_root="$(git rev-parse --show-toplevel)"
harness="$repo_root/apps/casars-mac/script/gui_acceptance.py"
journey="${1:-gui-test}"
remote="${CASA_RS_GUI_TEST_REMOTE:-}"

case "$journey" in
  -h | --help)
    usage
    exit 0
    ;;
esac

journey_json="$(python3 "$harness" describe "$journey")" || {
  usage >&2
  exit 2
}
remote_supported="$(printf '%s' "$journey_json" | python3 -c 'import json,sys; print("1" if json.load(sys.stdin)["remote_supported"] else "0")')"
if [[ "$remote_supported" != "1" ]]; then
  echo "journey does not support remote execution: $journey" >&2
  exit 2
fi
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

remote_root="${CASA_RS_GUI_TEST_REMOTE_ROOT:-@HOME@/Library/Caches/casa-rs-gui-worker/source}"
remote_storage="${CASA_RS_GUI_TEST_REMOTE_STORAGE:-/Volumes/Extra Storage (not encrypted)/casa-rs-gui-worker-state}"
remote_derived_data="${CASA_RS_GUI_TEST_REMOTE_DERIVED_DATA:-@HOME@/Library/Developer/Xcode/DerivedData/casa-rs-gui-worker}"
remote_signing_config="${CASA_RS_GUI_TEST_REMOTE_SIGNING_CONFIG:-@HOME@/.config/casa-rs/gui-worker-signing.env}"
developer_dir="${CASA_RS_GUI_TEST_REMOTE_DEVELOPER_DIR:-/Applications/Xcode.app/Contents/Developer}"
remote_python="${CASA_RS_GUI_TEST_REMOTE_PYTHON:-}"
remote_codex="${CASA_RS_GUI_TEST_REMOTE_CODEX:-}"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-${revision:0:12}-$journey"
remote_artifacts="$remote_storage/artifacts/$run_id"
remote_target="$remote_storage/target"

request="$({
  python3 - "$journey" "$branch" "$revision" "$remote_root" "$remote_storage" \
    "$remote_derived_data" "$remote_artifacts" "$remote_target" "$developer_dir" \
    "$remote_python" "$remote_codex" "$remote_signing_config" <<'PY'
import json
import sys

keys = (
    "journey", "branch", "revision", "repo_root", "storage_root",
    "derived_data", "artifact_root", "target_dir", "developer_dir",
    "python", "codex", "signing_config",
)
print(json.dumps({"schema_version": 1, **dict(zip(keys, sys.argv[1:]))}, separators=(",", ":")))
PY
} | /usr/bin/base64 | /usr/bin/tr -d '\n')"

ssh_args=(-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=15 -o ServerAliveCountMax=4)
if [[ -n "${CASA_RS_GUI_TEST_REMOTE_IDENTITY:-}" ]]; then
  ssh_args+=(-i "$CASA_RS_GUI_TEST_REMOTE_IDENTITY")
fi

echo "==> Remote GUI worker: $remote"
echo "==> Journey: $journey"
echo "==> Revision: $revision ($branch)"
echo "==> Remote artifacts: $remote_artifacts"

set +e
ssh "${ssh_args[@]}" "$remote" /bin/bash -s -- "$request" <<'REMOTE_RUN'
set -euo pipefail

request_file="$(mktemp "${TMPDIR:-/tmp}/casa-rs-gui-request.XXXXXX.json")"
cleanup_request() { rm -f "$request_file"; }
trap cleanup_request EXIT
printf '%s' "$1" | /usr/bin/base64 -D >"$request_file"

json_get() {
  /usr/bin/plutil -extract "$1" raw -o - "$request_file"
}
expand_home() {
  case "$1" in
    @HOME@/*) printf '%s/%s' "$HOME" "${1#@HOME@/}" ;;
    *) printf '%s' "$1" ;;
  esac
}

[[ "$(json_get schema_version)" == "1" ]] || {
  echo "unsupported remote GUI request schema" >&2
  exit 2
}
journey="$(json_get journey)"
branch="$(json_get branch)"
revision="$(json_get revision)"
repo_root="$(expand_home "$(json_get repo_root)")"
storage_root="$(expand_home "$(json_get storage_root)")"
derived_data="$(expand_home "$(json_get derived_data)")"
artifact_root="$(expand_home "$(json_get artifact_root)")"
target_dir="$(expand_home "$(json_get target_dir)")"
developer_dir="$(expand_home "$(json_get developer_dir)")"
python_command="$(expand_home "$(json_get python)")"
codex_command="$(expand_home "$(json_get codex)")"
signing_config="$(expand_home "$(json_get signing_config)")"

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
  exit 2
fi
"$developer_dir/usr/bin/xcodebuild" -checkFirstLaunchStatus

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
  echo "local revision is not the pushed tip of origin/$branch" >&2
  exit 2
fi
git -C "$repo_root" switch --quiet --detach "$revision"

# shellcheck source=gui-signing-config.sh
source "$repo_root/scripts/gui-signing-config.sh"
gui_load_signing_config "$signing_config"
gui_verify_signing_identity || {
  echo "stable GUI-worker signing identity is unavailable" >&2
  exit 2
}
export CASA_RS_GUI_TEST_CODE_SIGN_IDENTITY CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN
unset CASA_RS_GUI_TEST_CODE_SIGN_KEYCHAIN_PASSWORD

mkdir -p "$storage_root" "$artifact_root" "$target_dir" "$derived_data"
if [[ -e "$repo_target" || -L "$repo_target" ]]; then
  if [[ ! -L "$repo_target" || ! "$repo_target" -ef "$target_dir" ]]; then
    echo "remote checkout target must link to $target_dir" >&2
    exit 2
  fi
else
  ln -s "$target_dir" "$repo_target"
fi
if [[ -n "$python_command" ]]; then export CASA_RS_GUI_TEST_PYTHON="$python_command"; fi
if [[ -n "$codex_command" ]]; then export CASA_RS_CODEX_COMMAND="$codex_command"; fi

/usr/bin/caffeinate -dimsu -w $$ >/dev/null 2>&1 &
caffeinate_pid=$!
stop_caffeinate() {
  kill "$caffeinate_pid" >/dev/null 2>&1 || true
  wait "$caffeinate_pid" 2>/dev/null || true
  cleanup_request
}
trap stop_caffeinate EXIT
cd "$repo_root"
python3 apps/casars-mac/script/gui_acceptance.py run "$journey"
REMOTE_RUN
status=$?
set -e

echo "==> Remote artifacts retained at $remote:$remote_artifacts"
local_artifacts="$repo_root/apps/casars-mac/.gui-test/remote/$run_id"
mkdir -p "$local_artifacts"
while IFS= read -r artifact; do
  local_path="$local_artifacts/$artifact"
  remote_path="$remote_artifacts/$artifact"
  encoded_path="$(printf '%s' "$remote_path" | /usr/bin/base64 | /usr/bin/tr -d '\n')"
  if ssh "${ssh_args[@]}" "$remote" /bin/bash -s -- \
    "$encoded_path" >"$local_path" <<'REMOTE_ARTIFACT'
set -euo pipefail
artifact_path="$(printf '%s' "$1" | /usr/bin/base64 -D)"
test -f "$artifact_path"
cat "$artifact_path"
REMOTE_ARTIFACT
  then
    echo "==> Copied sanitized artifact: $local_path"
  else
    rm -f "$local_path"
  fi
done < <(python3 "$harness" artifacts "$journey" --transport)

exit "$status"
