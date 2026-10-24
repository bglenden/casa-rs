#!/usr/bin/env bash
# SPDX-License-Identifier: LGPL-3.0-or-later
#
# Joins a Claude Code cloud session to the owner's tailnet. Called by
# session-start.sh when TS_AUTHKEY is set in the cloud environment's
# variables; use an ephemeral, pre-approved, tagged auth key so each
# session's node removes itself when the container goes away.
#
# Optional variables (the names follow the official Tailscale container):
#   TS_HOSTNAME    node name (default casa-rs-cloud)
#   TS_EXTRA_ARGS  extra `tailscale up` flags, e.g. --accept-routes
#   TS_USERSPACE   "true" forces userspace networking, which serves a
#                  SOCKS5/HTTP proxy on localhost:1055 instead of a TUN device

set -euo pipefail

log() { echo "tailscale-up: $*" >&2; }

if [[ "${CLAUDE_CODE_REMOTE:-}" != "true" || -z "${TS_AUTHKEY:-}" ]]; then
  exit 0
fi

if ! command -v tailscaled >/dev/null 2>&1; then
  case "$(uname -m)" in
    x86_64) arch=amd64 ;;
    aarch64) arch=arm64 ;;
    *) log "unsupported architecture $(uname -m)"; exit 1 ;;
  esac
  if ! releases="$(curl -fsSL --max-time 30 'https://pkgs.tailscale.com/stable/?mode=json' 2>/dev/null)"; then
    log "cannot reach pkgs.tailscale.com; the environment needs Full network access"
    exit 1
  fi
  version="$(python3 -c 'import json, sys; d = json.loads(sys.argv[1]); print(d.get("TarballsVersion") or d["Version"])' "$releases")"
  log "installing tailscale $version"
  tmp="$(mktemp -d)"
  curl -fsSL --max-time 120 "https://pkgs.tailscale.com/stable/tailscale_${version}_${arch}.tgz" |
    tar -xzf - -C "$tmp"
  install -m 0755 "$tmp"/tailscale_*/tailscale "$tmp"/tailscale_*/tailscaled /usr/local/bin/
  rm -rf "$tmp"
fi

if ! pgrep -x tailscaled >/dev/null 2>&1; then
  daemon_args=(--state=mem: --socket=/var/run/tailscale/tailscaled.sock)
  if [[ "${TS_USERSPACE:-}" == "true" || ! -c /dev/net/tun ]]; then
    daemon_args+=(--tun=userspace-networking
      --socks5-server=localhost:1055 --outbound-http-proxy-listen=localhost:1055)
  fi
  mkdir -p /var/run/tailscale
  setsid nohup tailscaled "${daemon_args[@]}" >/var/log/tailscaled.log 2>&1 </dev/null &
  for _ in $(seq 1 20); do
    [[ -S /var/run/tailscale/tailscaled.sock ]] && break
    sleep 0.5
  done
fi

# shellcheck disable=SC2086 # TS_EXTRA_ARGS is a list of flags.
tailscale up --auth-key="$TS_AUTHKEY" --hostname="${TS_HOSTNAME:-casa-rs-cloud}" \
  --timeout=60s ${TS_EXTRA_ARGS:-}
log "joined as $(tailscale ip -4 2>/dev/null | head -1)"
