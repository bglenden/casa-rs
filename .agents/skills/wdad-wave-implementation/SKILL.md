---
name: wdad-wave-implementation
description: Use to implement an approved wave on a branch. Keeps work inside the approved boundary without silently descoping accepted work, runs preflight and refactor passes only where bounded by the wave, commits and pushes the wave branch, and creates or updates the PR.
---

# Skill: Wave implementation

## Purpose

Implement a wave without silently widening or narrowing the approved scope, run a bounded refactor pass on the involved code, then produce a review-ready PR.

## Mode

Implementation is allowed only after the wave is approved. Invoking this skill authorizes commits, push, PR creation, and a bounded refactor pass for the approved wave branch, but not deferral or expansion of approved scope.

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
9. For user-visible native macOS GUI changes, add or update launched-app
   XCTest/XCUIAutomation coverage and run `just gui-test`. Issue #368 must be
   implemented and green before the current notebook wave enters production
   integration, and lands with the completed wave PR. If the test target or
   supported runner is unavailable, stop and record the blocker;
   screenshots, debug JSON, or manual/computer-use evidence do not waive it.
10. Run the `refactor` skill on the code involved in the current wave before finalizing the PR. If the wave has no code refactor surface, record why it is not applicable.
11. Keep the refactor pass bounded to touched or directly exposed code. If `refactor` identifies a larger coherent refactor, produce the brief and ask before expanding the wave.
12. Run `verify` before moving the wave into `Review`.
13. If an approved outcome, included issue, or acceptance check cannot be completed, stop and ask for explicit user signoff before creating a deferral, follow-up, non-goal, or out-of-scope classification for that approved work.
14. Update issue closeout and PR material so they match the actual implementation, refactor pass, verification result, and any approved-scope deferral and the user signoff location.
15. Commit the finished wave changes on the wave branch.
16. Push the wave branch and create or update the PR.
17. Move the wave to `Review` only after the PR exists, carries the latest `verify` result, records the refactor pass or not-applicable rationale, and has no unsigned approved-scope deferral.

## Stop immediately before

- changing a public contract without approval
- adding a substantial dependency
- changing runtime or boundary direction
- deferring or descoping approved outcome, included issues, or acceptance checks without explicit user signoff
- weakening tests without replacement
- using the refactor pass to expand beyond the code involved in the approved wave without explicit user signoff
- committing directly to `main`
- merging the PR or deleting branches/worktrees; that belongs to `wdad-pr-merge`
