---
name: wdad-architecture-review
description: Closeout sidecar used by `wdad-wave-closeout` to review a branch or diff for architecture drift, boundary violations, unapproved dependencies, public API changes, duplication, or entropy risks. Read-only by default.
---

# Skill: Architecture review

## Purpose
Review changes for architectural coherence and entropy risk.

## Mode
Read-only/planning mode. Do not edit files unless separately instructed.

## Required inputs
- Current branch/diff
- Current wave issue
- AGENTS.md
- ARCHITECTURE.md
- TESTING.md
- accepted ADRs

## Procedure
1. Compare diff against wave scope and authority limits.
2. Check module/package/crate boundaries.
3. Check accepted ADRs and architecture constraints.
4. Check public APIs, data formats, and dependencies.
5. Look for duplicate subsystems, hidden coupling, over-generalization, and architecture shortcuts.
6. Classify findings as blocking or non-blocking.

## Output
- Blocking issues
- Non-blocking issues
- ADR/doc updates needed
- Suggested follow-up issues
- Confidence level and rationale

## Blocking examples
- Violates accepted ADR
- Adds unapproved dependency
- Changes public API without approval
- Crosses forbidden boundary
- Weakens tests
