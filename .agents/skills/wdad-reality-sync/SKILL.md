---
name: wdad-reality-sync
description: Use after a scaled-layer wave or at architecture checkpoints. Compares code and tests with ARCHITECTURE.md, TESTING.md, ADRs, and generated artifacts.
---

# Skill: Reality sync

## Purpose

Keep the durable scaled-layer docs trustworthy.

## Mode

Read-only/planning mode first.

## Required inputs

- current diff or merged wave
- `ARCHITECTURE.md`
- `TESTING.md`
- accepted ADRs, if any
- generated artifacts, if any

## Procedure

1. Identify what actually changed.
2. Compare those changes to the maintained docs.
3. Check whether any maintained doc now treats approved outcome, included issues, or acceptance checks as deferred, follow-up, non-goal, or out-of-scope.
4. Classify drift as doc update, ADR update, generated-artifact refresh, follow-up issue, or no action. Follow-ups are valid for newly discovered adjacent work; approved-scope deferrals require explicit user signoff.
5. Propose the smallest honest follow-up.

## Output

- drift classification
- minimal update list
- follow-up issues, if any
