#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

cargo test -p casacore-ms --features slow-tests --test msexplore_casa_parity
