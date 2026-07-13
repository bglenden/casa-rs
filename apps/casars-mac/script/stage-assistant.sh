#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: stage-assistant.sh <app-resources-directory>" >&2
  exit 2
fi

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
assistant_source="$root_dir/../casars-assistant"
destination="$1/casars-assistant"

[[ -d "$assistant_source/node_modules" ]] || {
  echo "casars-assistant dependencies are missing; run just setup" >&2
  exit 2
}

npm --prefix "$assistant_source" run build
rm -rf "$destination"
mkdir -p "$destination"
cp "$assistant_source/package.json" "$assistant_source/package-lock.json" "$destination/"
cp -R "$assistant_source/dist" "$assistant_source/node_modules" "$destination/"
npm --prefix "$destination" prune --omit=dev --ignore-scripts --offline

