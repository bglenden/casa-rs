---
name: wdad-reality-sync
description: Use at wave closeout or architecture checkpoints to compare actual code behavior against ARCHITECTURE.md, TESTING.md, ADRs, and generated artifacts. Proposes minimal doc/code follow-up actions.
---

# Skill: Reality sync

## Purpose
Keep durable docs trustworthy by reconciling them with actual code and tests.

## Mode
Planning/read-only mode first. Edits only after discrepancies are classified.

## Required inputs
- Current branch/diff or merged wave
- ARCHITECTURE.md
- TESTING.md
- accepted ADRs
- docs/generated/, if present
- issue closeout

## Procedure
1. Identify actual changes in behavior, boundaries, APIs, dependencies, tests, and generated artifacts.
2. Compare those changes to maintained docs and ADRs.
3. Classify each discrepancy.
4. Propose minimal edits or follow-up issues.
5. Update issue closeout with reality-sync result if authorized.

## Classification
- Doc should update
- Doc should be retired/marked historical
- ADR needs supersession
- Generated artifact needs regeneration
- Follow-up issue needed
- No action

## Do not
- Create new planning markdown files
- Rewrite accepted ADR decisions silently
