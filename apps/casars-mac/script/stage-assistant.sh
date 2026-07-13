#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: stage-assistant.sh <app-resources-directory>" >&2
  exit 2
fi

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_root="$(cd "$root_dir/../.." && pwd)"
assistant_source="$root_dir/../casars-assistant"
destination="$1/casars-assistant"
source_destination="$1/casars-source"

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

source_list="$(mktemp "${TMPDIR:-/tmp}/casars-source-files.XXXXXX")"
trap 'rm -f "$source_list"' EXIT
git -C "$repo_root" ls-files | awk '
  /^(ARCHITECTURE|TESTING)\.md$/ { print; next }
  /^(crates|apps|docs|scripts)\// && /\.(md|txt|rst|toml|rs|swift|ts|py)$/ { print }
' >"$source_list"
rm -rf "$source_destination"
mkdir -p "$source_destination"
rsync -a --files-from="$source_list" "$repo_root/" "$source_destination/"
release="$(sed -n 's/^version = "\(.*\)"/\1/p' "$repo_root/Cargo.toml" | head -n 1)"
commit="$(git -C "$repo_root" rev-parse HEAD)"
printf '{"schema_version":1,"release":"%s","commit":"%s"}\n' \
  "$release" "$commit" >"$source_destination/casars-source.json"
