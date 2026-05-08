---
name: wdad-architecture-review
description: Use for a dedicated architecture pass on a branch or diff. Checks boundaries, dependencies, public contracts, and entropy risk against the scaled-layer docs.
---

# Skill: Architecture review

## Purpose

Detect architecture drift before it lands.

## Mode

Read-only/planning mode.

## Required inputs

- current diff or branch
- wave issue or wave-lead issue
- `ARCHITECTURE.md`
- accepted ADRs, if any

## Procedure

1. Compare the diff to the approved scope.
2. Check boundaries and dependency direction.
3. Check public contracts and persisted formats.
4. Flag duplication, hidden coupling, or policy drift.
5. Treat unsigned deferral or descoping of approved outcome, included issues, or acceptance checks as blocking.
6. Classify findings as blocking or follow-up. Follow-ups are only for newly discovered adjacent work or approved-scope deferrals with explicit user signoff.

## Output

- blocking findings
- follow-up findings
- doc or ADR updates needed
- confidence level
