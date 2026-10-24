#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# SessionStart hook for Claude Code cloud sessions. Installs what `just quick`
# and the Python package tests need on the Ubuntu cloud image, mirroring the
# CI jobs in .github/workflows/ci.yml, and exports the same environment. It is
# idempotent and does nothing outside a cloud session.
#
# When the environment defines TS_AUTHKEY it also joins the session to the
# owner's tailnet (see tailscale-up.sh).

set -euo pipefail

if [[ "${CLAUDE_CODE_REMOTE:-}" != "true" ]]; then
  exit 0
fi

repo_root="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$repo_root"

log() { echo "session-start: $*" >&2; }

export_var() {
  if [[ -n "${CLAUDE_ENV_FILE:-}" ]]; then
    echo "export $1=\"$2\"" >>"$CLAUDE_ENV_FILE"
  fi
  export "$1=$2"
}

# System packages: FFTW (casa-fft links it through pkg-config) and just.
missing_pkgs=()
dpkg -s libfftw3-dev >/dev/null 2>&1 || missing_pkgs+=(libfftw3-dev)
dpkg -s pkg-config >/dev/null 2>&1 || missing_pkgs+=(pkg-config)
command -v just >/dev/null 2>&1 || missing_pkgs+=(just)
if ((${#missing_pkgs[@]})); then
  log "installing ${missing_pkgs[*]}"
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends "${missing_pkgs[@]}" >/dev/null
fi

# Rust: the workspace needs rust-version 1.99 or newer.
rustc_minor="$(rustc --version | awk '{split($2, v, "."); print v[2]}')"
if ((rustc_minor < 99)); then
  log "updating the stable Rust toolchain ($(rustc --version))"
  rustup update stable --no-self-update >/dev/null
fi
rustup component add rustfmt clippy >/dev/null 2>&1

# cargo-nextest: prebuilt binary when reachable, else build it.
if ! cargo nextest --version >/dev/null 2>&1; then
  log "installing cargo-nextest"
  if ! curl -fsSL --max-time 60 https://get.nexte.st/latest/linux 2>/dev/null | tar -xzf - -C "$HOME/.cargo/bin" 2>/dev/null; then
    cargo install --locked --quiet cargo-nextest
  fi
fi

# Python packages for scripts/test-perf-tools.sh (CI's perf_tools job).
if ! python3 -c 'import pytest, numpy, scipy, matplotlib, astropy' >/dev/null 2>&1; then
  log "installing Python test packages"
  python3 -m pip install --quiet --disable-pip-version-check --root-user-action=ignore \
    pytest numpy scipy matplotlib astropy
fi

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

log "fetching crate dependencies"
cargo fetch --quiet

if [[ -n "${TS_AUTHKEY:-}" ]]; then
  "$repo_root/.claude/hooks/tailscale-up.sh" || log "tailscale: not joined; the session continues without the tailnet"
fi

log "ready: $(rustc --version), $(cargo nextest --version 2>/dev/null | head -1), just $(just --version | awk '{print $2}')"
