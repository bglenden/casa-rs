---
name: wdad-stabilization-wave
description: "Use to plan and execute a no-feature wave focused on entropy reduction: refactoring hotspots, tightening tests, removing dead code, updating diagrams, or strengthening architecture checks."
---

# Skill: Stabilization wave

## Purpose
Run a bounded no-feature wave to reduce entropy and improve maintainability.

## Mode
Start in planning mode. Implementation may proceed after the stabilization wave issue is approved.

## Required inputs
- Recent architecture/test/reality-sync findings
- Current ARCHITECTURE.md, TESTING.md, ADRs
- Project board issues tagged refactor/test/docs/architecture

## Procedure
1. Identify entropy hotspots and quality gaps.
2. Choose a narrow stabilization outcome.
3. Define non-goals; no feature work.
4. Define measurable acceptance checks: tests improved, dependencies reduced, module split, graph regenerated, checks tightened.
5. Shape a wave issue.
6. Implement only after approval.
7. Run architecture review, test-adversary review, and reality sync.

## Output
- Stabilization wave issue draft
- Quality gates to prove improvement
- Risks and rollback plan

## Do not
- Mix feature work into stabilization
- Start open-ended cleanup
