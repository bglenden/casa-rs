#!/bin/bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../../.." && pwd)"
destination="${1:?destination directory is required}"

rm -rf "$destination"
mkdir -p "$destination/docs/adr"

for file in ARCHITECTURE.md TESTING.md Cargo.toml; do
  cp "$repo_root/$file" "$destination/$file"
done

for file in \
  docs/assistant-security.md \
  docs/provider-contracts.md \
  docs/scientific-notebooks-and-assistant.md \
  docs/adr/0007-scientific-notebooks-and-assistant-boundary.md
do
  mkdir -p "$destination/$(dirname "$file")"
  cp "$repo_root/$file" "$destination/$file"
done

while IFS= read -r -d '' source; do
  relative="${source#"$repo_root/"}"
  mkdir -p "$destination/$(dirname "$relative")"
  cp "$source" "$destination/$relative"
done < <(find "$repo_root/crates" "$repo_root/apps/casars-mac/Sources" \
  -type f \( -name '*.rs' -o -name '*.swift' -o -name 'Cargo.toml' \) -print0)

release="$(git -C "$repo_root" describe --tags --always 2>/dev/null || printf 'unreleased')"
commit="$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || printf 'unknown')"
printf '{"schema_version":1,"release":"%s","commit":"%s"}\n' \
  "$release" "$commit" > "$destination/casars-source.json"
