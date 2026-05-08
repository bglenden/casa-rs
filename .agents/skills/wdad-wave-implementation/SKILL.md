---
name: wdad-wave-implementation
description: Use to implement an approved wave on a branch. Keeps work inside the approved boundary without descoping approved work, runs preflight only when needed, commits and pushes the wave branch, and creates or updates the PR.
---

# Skill: Wave implementation

## Purpose

Implement a wave without silently widening or narrowing the approved scope, then produce a review-ready PR.

## Mode

Implementation is allowed only after the wave is approved. Invoking this skill authorizes commits, push, and PR creation for the approved wave branch.

## Required inputs

- current wave issue or wave-lead issue
- `AGENTS.md`
- `ARCHITECTURE.md` and `TESTING.md`, if present
- accepted ADRs, if any

## Procedure

1. Confirm the approved scope contract: outcome, included issues, acceptance checks, non-goals, and stop conditions.
2. Confirm work is on a wave branch. If on `main`, create or switch to a wave branch before editing or committing.
3. Decide whether a preflight-style planning pass is needed before coding.
4. Run a compact preflight when the wave touches public APIs, persisted formats, new dependencies, subsystem boundaries, runtime/concurrency behavior, performance-sensitive code, or has unclear code-path impact.
5. In that preflight, inspect likely files or modules, list tests to add or update, check architecture impact, and define explicit stop conditions before proceeding.
6. Implement the smallest coherent change set.
7. Run `quick` during the edit loop.
8. Add or update tests for the stated acceptance checks.
9. Run `verify` before moving the wave into `Review`.
10. If an approved outcome, included issue, or acceptance check cannot be completed, stop and ask for explicit user signoff before creating a deferral, follow-up, non-goal, or out-of-scope classification for that approved work.
11. Update issue closeout and PR material so they match the actual implementation and verification result, including any approved-scope deferral and the user signoff location.
12. Commit the finished wave changes on the wave branch.
13. Push the wave branch and create or update the PR.
14. Move the wave to `Review` only after the PR exists, carries the latest `verify` result, and has no unsigned approved-scope deferral.

## Stop immediately before

- changing a public contract without approval
- adding a substantial dependency
- changing runtime or boundary direction
- deferring or descoping approved outcome, included issues, or acceptance checks without explicit user signoff
- weakening tests without replacement
- committing directly to `main`
- merging the PR or deleting branches/worktrees; that belongs to `wdad-pr-merge`
