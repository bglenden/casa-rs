#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

required_files=(
  "AGENTS.md"
  "ARCHITECTURE.md"
  "TESTING.md"
  "docs/adr/TEMPLATE.md"
)

required_sections=(
  "## Major modules / crates / packages"
  "## Dependency direction"
  "## Runtime model"
  "## Persistence / external systems"
  "## Public interfaces"
  "## ADR index"
)

for path in "${required_files[@]}"; do
  [[ -f "$path" ]] || {
    echo "arch-check: missing required file $path" >&2
    exit 1
  }
done

for heading in "${required_sections[@]}"; do
  grep -Fq "$heading" ARCHITECTURE.md || {
    echo "arch-check: ARCHITECTURE.md is missing section: $heading" >&2
    exit 1
  }
done

adr_ids=()
while IFS= read -r adr_id; do
  adr_ids+=("$adr_id")
done < <(
  awk -F'|' '
    /^\| [0-9][0-9][0-9][0-9] / {
      gsub(/ /, "", $2)
      print $2
    }
  ' ARCHITECTURE.md
)

for adr_id in "${adr_ids[@]}"; do
  shopt -s nullglob
  matches=(docs/adr/"${adr_id}"-*.md)
  shopt -u nullglob
  if [[ ${#matches[@]} -ne 1 ]]; then
    echo "arch-check: expected exactly one ADR file for $adr_id, found ${#matches[@]}" >&2
    exit 1
  fi

  grep -Eq '^Status: (proposed|accepted|superseded|retired)$' "${matches[0]}" || {
    echo "arch-check: ADR ${matches[0]} is missing a valid Status line" >&2
    exit 1
  }
done
