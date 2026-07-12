---
name: wdad-backlog-to-ready
description: Use when moving work from backlog to ready. Produces a single-issue or wave-lead issue with outcome, acceptance checks, stop conditions, and a short shaping interview when the request is still sparse.
---

# Skill: Backlog to ready

## Purpose

Turn rough work or a sparse backlog item into a wave-ready issue.

## Mode

Planning/read-only mode. Do not edit code.

## Required inputs

- backlog issue or rough request
- `README.md`
- `AGENTS.md`
- `ARCHITECTURE.md` and `TESTING.md`, if present

## Procedure

1. Start with a short shaping interview unless the backlog item already defines the outcome, constraints, and review surface clearly.
2. Ask about delivered functionality: what should be true for the user when the work is done, and what is deliberately out of scope.
3. Ask how the work might extend in future so the wave does not lock the implementation into an obviously brittle direction.
4. Ask about architecture consistency: expected boundaries, persisted formats, public contracts, dependency constraints, and any repo conventions that must stay intact.
5. Ask about preferred UI outcomes when the work is user-visible: workflows, layout priorities, tone, and what would count as a good interaction outcome.
6. For native macOS GUI work, identify the critical launched-app workflows that
   require XCTest/XCUIAutomation, the deterministic fixture launch states, and
   the stable accessibility identifiers needed to test them. Do not treat
   screenshots, debug state, or manual/computer-use review as a substitute for
   the executable UI-test plan.
7. If UI direction is still ambiguous and rough visual exploration would help, offer a few lightweight alternatives using an image-generation skill as preview-only mockups rather than implementation specs.
8. Decide whether this should be one issue or a wave-lead issue over a few related issues.
9. Check wave suitability: one review boundary, one verification path, one main area, no unresolved architecture split.
10. Write falsifiable acceptance checks.
11. Identify architecture/test impact and stop conditions.
12. Identify the verification path: `quick`, `verify` before `In Progress -> Review`, `gui-test` for user-visible native macOS GUI changes once #368 lands, and any release-only checks. For each required gate, designate one authoritative local or hosted execution; do not plan duplicate cross-environment runs unless platform-specific risk or the user requires them.
13. Draft closeout expectations and follow-up hooks for adjacent work. Do not use follow-up hooks to weaken the approved outcome or acceptance checks.
14. State that once approved, outcome, included issues, and acceptance checks cannot be deferred or descoped without explicit user signoff.
15. State that code waves must record a bounded refactor pass before review, or a not-applicable rationale when the wave has no code surface.

## Output

- outcome
- included issues
- non-goals
- current understanding
- shaping interview notes when the initial request was sparse
- acceptance checks
- architecture/test impact
- stop conditions
- closeout expectations
- approved-scope deferral signoff expectation
- refactor-pass expectation for code waves

## Do not

- start implementation
- create local planning markdown files
- group unrelated issues into one wave just to reduce issue count
- skip the shaping interview just because the initial issue text is short or vague
