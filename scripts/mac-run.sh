#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/mac-run.sh COMMAND [ARG ...]
       scripts/mac-run.sh --sync-only | --clean

Runs COMMAND in a copy of this working tree on the owner's Mac mini, for work
a Linux cloud session cannot do itself: the macOS-only crates and Metal tests,
the CASA oracle, or a second build beside the local one.

Each run first mirrors the working tree into ROOT/src/BRANCH/ on the Mac with
rsync over SSH: tracked, modified and untracked files and .git, but not the
files git ignores. COMMAND then runs there in the owner's login shell, with
CARGO_TARGET_DIR=ROOT/target shared by every copy and CARGO_INCREMENTAL=0;
~/.cargo/bin and /opt/homebrew/bin are appended to PATH. Its output streams
back and its exit status is this script's.

  --sync-only  mirror the tree and run nothing
  --clean      delete this branch's copy on the Mac (the shared target stays)

Environment:
  CASA_RS_MAC_HOST  SSH destination (default bg-mac-mini). In a cloud
                    session, .claude/hooks/session-start.sh configures its
                    user and key from CASA_RS_MAC_USER and CASA_RS_MAC_SSH_KEY.
  CASA_RS_MAC_ROOT  directory on the Mac, absolute or relative to the home
                    directory (default ext/casa-rs-cloud when ~/ext exists,
                    else casa-rs-cloud)

Examples:
  scripts/mac-run.sh cargo nextest run -p casa-imaging-metal
  scripts/mac-run.sh just quick
EOF
}

mode="run"
case "${1:-}" in
  "")
    usage >&2
    exit 2
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  --sync-only)
    mode="sync"
    shift
    ;;
  --clean)
    mode="clean"
    shift
    ;;
esac
if [[ "$mode" != "run" && $# -gt 0 ]]; then
  echo "mac-run: --sync-only and --clean take no command" >&2
  exit 2
fi

host="${CASA_RS_MAC_HOST:-bg-mac-mini}"
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

branch="$(git symbolic-ref --quiet --short HEAD || echo "detached-$(git rev-parse --short HEAD)")"
name="$(printf '%s' "$branch" | tr -c 'A-Za-z0-9._-' '-')"

q() { printf '%q' "$1"; }

# Remote shell code that sets $root to the absolute ROOT; it runs from the
# home directory, in whatever shell the owner logs in with.
resolve_root="root=$(q "${CASA_RS_MAC_ROOT:-}")
if [ -z \"\$root\" ]; then
  if [ -d ext ]; then root=ext/casa-rs-cloud; else root=casa-rs-cloud; fi
fi
mkdir -p \"\$root\" && root=\$(cd \"\$root\" && pwd)"

remote() {
  local status=0
  ssh -n "$host" "$1" || status=$?
  if ((status == 255)); then
    echo "mac-run: cannot reach $host over SSH. In a cloud session, check that the" \
      "tailnet is up (tailscale status) and that CASA_RS_MAC_SSH_KEY is set" \
      "(docs/agent-reference.md, \"Cloud Sessions\")." >&2
  fi
  return "$status"
}

if [[ "$mode" == "clean" ]]; then
  remote "$resolve_root && rm -rf \"\$root/src/$name\" && echo \"removed $host:\$root/src/$name\""
  exit 0
fi

# One round trip: create the directories, report ROOT, and prefer Homebrew's
# rsync over the system openrsync when it is installed.
probe="$(remote "$resolve_root && mkdir -p \"\$root/src/$name\" \"\$root/target\" && echo \"\$root\" &&
if [ -x /opt/homebrew/bin/rsync ]; then echo /opt/homebrew/bin/rsync; else echo rsync; fi")"
root="$(tail -n 2 <<<"$probe" | head -n 1)"
remote_rsync="$(tail -n 1 <<<"$probe")"
dest="$root/src/$name"

# Ignored paths as anchored literal excludes, which every rsync understands.
# Checksums rather than times decide what changed, and times are not copied,
# so files whose content is unchanged keep their Mac mtimes and cargo does not
# rebuild them.
excludes="$(mktemp)"
trap 'rm -f "$excludes"' EXIT
git ls-files -z --others --ignored --exclude-standard --directory |
  while IFS= read -r -d '' path; do printf '/%s\n' "$path"; done >"$excludes"

echo "mac-run: syncing to $host:$dest" >&2
start=$SECONDS
rsync -rlpc --delete --exclude-from="$excludes" --rsync-path="$remote_rsync" \
  ./ "$host:$dest/"
echo "mac-run: synced in $((SECONDS - start))s" >&2

if [[ "$mode" == "sync" ]]; then
  exit 0
fi

# A login shell reads the profile files but not ~/.zshrc, so rustup's and
# Homebrew's usual directories follow whatever PATH the profile set.
run="cd $(q "$dest") && export PATH=\"\$PATH:\$HOME/.cargo/bin:/opt/homebrew/bin\""
run+=" CARGO_TARGET_DIR=$(q "$root/target") CARGO_INCREMENTAL=0 && exec"
for arg in "$@"; do
  run+=" $(q "$arg")"
done
exec ssh "$host" "exec \"\${SHELL:-/bin/zsh}\" -lc $(q "$run")"
