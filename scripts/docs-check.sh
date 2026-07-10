#!/usr/bin/env bash

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

required_headers=(
  "README.md:Truth class: current descriptive"
  "AGENTS.md:Truth class: normative"
  "ARCHITECTURE.md:Truth class: current descriptive"
  "TESTING.md:Truth class: normative"
  "docs/README.md:Truth class: current descriptive"
  "docs/Planning/Phase 2 - Table fillout/README.md:Truth class: historical"
  "docs/Planning/Phase 2 - Table fillout/WAVE_TEMPLATE.md:Truth class: historical"
  "docs/Planning/Phase 3 - Quanta Measures Coordinates/README.md:Truth class: historical"
  "docs/Planning/Phase 3 - Quanta Measures Coordinates/WAVE_TEMPLATE.md:Truth class: historical"
  "docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/README.md:Truth class: historical"
  "docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/WAVE_TEMPLATE.md:Truth class: historical"
  "docs/Planning/Phase 5 - Lattices Coordinates Images/WAVE_TEMPLATE.md:Truth class: historical"
  "docs/Planning/Phase 5 - Lattices Coordinates Images/Deterministic Imaging Parity Program.md:Truth class: current descriptive"
)

for spec in "${required_headers[@]}"; do
  path="${spec%%:*}"
  header="${spec#*:}"
  [[ -f "$path" ]] || {
    echo "docs-check: missing required file $path" >&2
    exit 1
  }
  grep -Fq "$header" "$path" || {
    echo "docs-check: $path is missing required header '$header'" >&2
    exit 1
  }
done

python3 scripts/check-tutorial-pack-contract.py
python3 scripts/generate-parameter-reference.py --check
python3 scripts/generate-python-parameter-wrappers.py --check

[[ -f "CLAUDE.md" ]] || {
  echo "docs-check: missing CLAUDE.md" >&2
  exit 1
}

grep -Fxq '@AGENTS.md' CLAUDE.md || {
  echo "docs-check: CLAUDE.md must start with @AGENTS.md" >&2
  exit 1
}

for forbidden in \
  "README.md:Detailed phase and backlog tracking still lives in the phase-specific" \
  "docs/Planning/Phase 2 - Table fillout/README.md:Wave file names are the only status source of truth" \
  "docs/Planning/Phase 3 - Quanta Measures Coordinates/README.md:Wave file names are the only status source of truth" \
  "docs/Planning/Phase 4 - MeasurementSet and Derived Calibration Workflows/README.md:Wave file names are the only status source of truth" \
  "docs/Planning/Phase 5 - Lattices Coordinates Images/Deterministic Imaging Parity Program.md:single active implementation plan"
do
  path="${forbidden%%:*}"
  pattern="${forbidden#*:}"
  if grep -Fq "$pattern" "$path"; then
    echo "docs-check: forbidden stale planning text remains in $path" >&2
    exit 1
  fi
done

for skill_path in .agents/skills/*/SKILL.md; do
  [[ -f "$skill_path" ]] || continue

  skill_dir="$(basename "$(dirname "$skill_path")")"
  frontmatter="$(awk '
    NR == 1 && $0 == "---" { in_frontmatter = 1; next }
    in_frontmatter && $0 == "---" { exit }
    in_frontmatter { print }
  ' "$skill_path")"

  [[ -n "$frontmatter" ]] || {
    echo "docs-check: missing YAML frontmatter in $skill_path" >&2
    exit 1
  }

  skill_name="$(printf '%s\n' "$frontmatter" | sed -n 's/^name: //p' | head -n 1)"
  skill_description="$(printf '%s\n' "$frontmatter" | sed -n 's/^description: //p' | head -n 1)"

  [[ -n "$skill_name" ]] || {
    echo "docs-check: missing skill name in $skill_path" >&2
    exit 1
  }

  [[ -n "$skill_description" ]] || {
    echo "docs-check: missing skill description in $skill_path" >&2
    exit 1
  }

  [[ "$skill_name" == "$skill_dir" ]] || {
    echo "docs-check: skill name '$skill_name' does not match folder '$skill_dir' in $skill_path" >&2
    exit 1
  }

  case "$skill_description" in
    \"*\"|\'*\' )
      ;;
    *": "*)
      echo "docs-check: skill description in $skill_path contains an unquoted colon; quote the YAML string" >&2
      exit 1
      ;;
  esac
done
