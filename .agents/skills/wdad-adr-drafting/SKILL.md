---
name: wdad-adr-drafting
description: Use when an architecture decision is needed or when drift must be accepted/superseded. Drafts ADRs for human acceptance; does not mark them accepted unless explicitly told.
---

# Skill: ADR drafting

## Purpose
Draft architecture decision records that capture decisions, consequences, enforcement, and drift detection.

## Mode
Planning mode unless explicitly asked to edit ADR files.

## Required inputs
- Decision or problem statement
- Relevant issue/PR
- ARCHITECTURE.md
- existing ADRs
- relevant code context

## Procedure
1. Determine whether this belongs in an ADR.
2. Identify context and forces.
3. State the decision in unambiguous present/future-tense language.
4. List alternatives considered.
5. List consequences and paths closed.
6. Define enforcement: tests, lint/import/dependency rules, CI, review trigger, or advisory only.
7. Define drift detection.
8. If superseding, link old and new ADRs.

## Output
- ADR draft with Status: proposed unless human says accepted
- Suggested ARCHITECTURE.md update, if needed
- Suggested mechanical enforcement, if possible

## Do not
- Approve the decision yourself
- Rewrite old ADR bodies except supersession metadata when explicitly asked
