#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# SessionStart hook for Claude Code cloud sessions. Installs what `just quick`
# needs on the Ubuntu cloud image, mirroring the CI jobs in
# .github/workflows/ci.yml, and exports the same environment. It is
# idempotent and does nothing outside a cloud session.
#
# Claude Code kills a SessionStart hook after 10 minutes, and the network can
# be very slow while the container starts, so the local steps run first and
# every network step is bounded by a shared budget well inside that limit. A
# step that runs out of time is logged and skipped; the session starts without
# it. The Python packages for scripts/test-perf-tools.sh are not installed
# here; that script installs them on first use in a cloud session.
#
# When the environment defines TS_AUTHKEY it also joins the session to the
# owner's tailnet (see tailscale-up.sh), in the background and before anything
# else, so the slower steps cannot delay or prevent it.
#
# When the environment defines CASA_RS_MAC_SSH_KEY it also sets up SSH to the
# owner's Mac mini for scripts/mac-run.sh (docs/agent-reference.md, "Cloud
# Sessions").

set -euo pipefail

if [[ "${CLAUDE_CODE_REMOTE:-}" != "true" ]]; then
  exit 0
fi

repo_root="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$repo_root"

log() { echo "session-start: $*" >&2; }

# Seconds the network steps may use in total, against the 600 s hook limit.
deadline=$((SECONDS + 480))

# bounded CAP DESC CMD...: runs CMD for at most CAP seconds and never past the
# shared deadline; on failure or timeout it logs DESC and returns 0.
bounded() {
  local cap="$1" desc="$2" left
  shift 2
  left=$((deadline - SECONDS))
  ((left < cap)) && cap="$left"
  if ((cap <= 0)); then
    log "$desc: skipped, the time budget is spent"
    return 0
  fi
  local status=0
  timeout "$cap" "$@" || status=$?
  if ((status == 124)); then
    log "$desc: stopped after ${cap}s; the session continues without it"
  elif ((status != 0)); then
    log "$desc: failed (exit $status); the session continues without it"
  fi
  return 0
}

export_var() {
  if [[ -n "${CLAUDE_ENV_FILE:-}" ]]; then
    echo "export $1=\"$2\"" >>"$CLAUDE_ENV_FILE"
  fi
  export "$1=$2"
}

# Tailnet: in the background, detached from the hook's output, so it neither
# waits for nor holds up the rest. Its log is /var/log/tailscale-up.log.
if [[ -n "${TS_AUTHKEY:-}" ]]; then
  log "joining the tailnet in the background (log: /var/log/tailscale-up.log)"
  setsid nohup timeout 300 "$repo_root/.claude/hooks/tailscale-up.sh" \
    >/var/log/tailscale-up.log 2>&1 </dev/null &
fi

# Local steps first: they are fast and need no network.

# Measures runtime, as CI installs it for the integration tests.
measures_root="$HOME/.cache/casa-rs/casa-measures"
measures_provenance="crates/casa-measures-data/data/casa-measures-runtime.provenance.json"
if ! cmp -s "$measures_provenance" "$measures_root/.casa-rs-measures-provenance.json"; then
  log "unpacking the bundled measures runtime"
  rm -rf "$measures_root"
  mkdir -p "$measures_root"
  tar --warning=no-unknown-keyword -xzf crates/casa-measures-data/data/casa-measures-runtime.tar.gz -C "$measures_root"
  cp "$measures_provenance" "$measures_root/.casa-rs-measures-provenance.json"
fi

export_var CASA_RS_MEASURESPATH "$measures_root"
export_var CARGO_INCREMENTAL 0
# Line tables keep file:line in backtraces at a fraction of full debug info's
# disk use, as in CI; the cloud disk allowance is small.
export_var CARGO_PROFILE_DEV_DEBUG line-tables-only
# Debug builds of the imaging stack exceed the default test-thread stack.
export_var RUST_MIN_STACK 33554432

# Repository git hooks (SPDX pre-commit check).
git config core.hooksPath .githooks

# SSH to the owner's Mac mini. CASA_RS_MAC_SSH_KEY holds a private key without
# a passphrase, either as ssh-keygen wrote it or base64-encoded onto one line.
mac_key="$HOME/.ssh/casa-rs-mac"
if [[ -n "${CASA_RS_MAC_SSH_KEY:-}" ]]; then
  install -d -m 700 "$HOME/.ssh"
  (
    umask 077
    if [[ "$CASA_RS_MAC_SSH_KEY" == -----BEGIN* ]]; then
      printf '%s\n' "$CASA_RS_MAC_SSH_KEY" >"$mac_key"
    else
      printf '%s' "$CASA_RS_MAC_SSH_KEY" | tr -d ' \t\r\n' | base64 -d >"$mac_key" 2>/dev/null
    fi
  ) || {
    log "CASA_RS_MAC_SSH_KEY is neither a key nor base64; skipping SSH to the Mac"
    rm -f "$mac_key"
  }
fi
if [[ -f "$mac_key" ]]; then
  cat >"$HOME/.ssh/casa-rs-mac.conf" <<EOF
Host ${CASA_RS_MAC_HOST:-bg-mac-mini}
  User ${CASA_RS_MAC_USER:-brianglendenning}
  IdentityFile $mac_key
  IdentitiesOnly yes
  BatchMode yes
  StrictHostKeyChecking accept-new
  ConnectTimeout 30
  ServerAliveInterval 30
EOF
  if ! grep -qx 'Include casa-rs-mac.conf' "$HOME/.ssh/config" 2>/dev/null; then
    { echo 'Include casa-rs-mac.conf'; cat "$HOME/.ssh/config" 2>/dev/null || true; } >"$HOME/.ssh/config.new"
    mv "$HOME/.ssh/config.new" "$HOME/.ssh/config"
  fi
fi

# Network steps, most needed first.

# System packages: FFTW (casa-fft links it through pkg-config) and just.
missing_pkgs=()
dpkg -s libfftw3-dev >/dev/null 2>&1 || missing_pkgs+=(libfftw3-dev)
dpkg -s pkg-config >/dev/null 2>&1 || missing_pkgs+=(pkg-config)
command -v just >/dev/null 2>&1 || missing_pkgs+=(just)
if [[ -f "$mac_key" ]]; then
  command -v ssh >/dev/null 2>&1 || missing_pkgs+=(openssh-client)
  command -v rsync >/dev/null 2>&1 || missing_pkgs+=(rsync)
fi
if ((${#missing_pkgs[@]})); then
  log "installing ${missing_pkgs[*]}"
  bounded 180 "apt-get install ${missing_pkgs[*]}" bash -c '
    apt-get update -qq &&
      DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends "$@" >/dev/null
  ' apt-get "${missing_pkgs[@]}"
fi
if [[ -f "$mac_key" ]] && command -v ssh-keygen >/dev/null 2>&1 &&
  ! ssh-keygen -y -f "$mac_key" </dev/null >/dev/null 2>&1; then
  log "CASA_RS_MAC_SSH_KEY is not a private key without a passphrase; SSH to the Mac will fail"
fi

# Rust: the workspace needs rust-version 1.99 or newer.
rustc_minor="$(rustc --version | awk '{split($2, v, "."); print v[2]}')"
if ((rustc_minor < 99)); then
  log "updating the stable Rust toolchain ($(rustc --version))"
  bounded 180 "rustup update" rustup update stable --no-self-update >/dev/null
fi
bounded 120 "rustup component add" rustup component add rustfmt clippy >/dev/null 2>&1

# cargo-nextest: prebuilt binary when reachable, else build it.
if ! cargo nextest --version >/dev/null 2>&1; then
  log "installing cargo-nextest"
  bounded 60 "cargo-nextest download" bash -c \
    'curl -fsSL --max-time 60 https://get.nexte.st/latest/linux | tar -xzf - -C "$HOME/.cargo/bin"'
  if ! cargo nextest --version >/dev/null 2>&1; then
    bounded 300 "cargo install cargo-nextest" cargo install --locked --quiet cargo-nextest
  fi
fi

log "fetching crate dependencies"
bounded 300 "cargo fetch" cargo fetch --quiet

log "ready after ${SECONDS}s: $(rustc --version), $(cargo nextest --version 2>/dev/null | head -1 || echo 'no cargo-nextest'), just $(just --version 2>/dev/null | awk '{print $2}')"
