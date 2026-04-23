---
name: wdad-wave-execution
description: "Primary board-transition skill for `Shaped -> Current Wave / Implementing`. It combines wave start and implementation: confirms readiness, decides whether preflight is required, establishes the branch/PR linkage contract, then executes the approved wave inside its boundaries."
---

# Skill: Wave execution

## Purpose
Move a shaped wave into active execution and carry it through implementation
without forcing the human or agent to choose between two skills that are
normally run back-to-back.

## Mode
Start in planning mode. Implementation may begin once the issue is confirmed
shaped, any required preflight is complete, and no blocked architectural
decision remains.

## Required inputs
- Current wave issue
- AGENTS.md
- ARCHITECTURE.md
- TESTING.md
- accepted ADRs
- relevant code context

## Procedure
1. Confirm the issue is actually wave-ready: shaped scope, non-goals,
   acceptance checks, test plan, and stop conditions exist.
2. Decide whether `wdad-implementation-preflight` is required.
3. If preflight is required, run it before editing.
4. Establish the execution surface:
   - branch/worktree for the wave
   - PR body contract:
     - issue-driven wave: `Wave issue: #N`
     - automation/gate-originated repair with no real issue: `Wave source: automation <name>`
5. Move the board item into active execution:
   - `Current Wave` before coding begins
   - `Implementing` once a draft PR or comparable active implementation thread exists
6. Implement the smallest coherent change set.
7. Add or update tests for the acceptance checks.
8. Run `just quick` during iteration.
9. Run `just verify` before handing off to closeout.
10. Update issue closeout with actual outcome, verification evidence,
    deviations, docs/ADR updates, and risks.
11. Prepare the PR review packet for `wdad-wave-closeout`.

## Output
- Start/no-start decision
- Whether preflight is required
- Branch/worktree and PR linkage contract
- Immediate implementation scope and commands
- Updated issue closeout and PR review packet

## Stop immediately before
- Starting code from an unshaped issue
- Public API or data format changes not already approved
- New substantial dependencies
- Boundary/concurrency/runtime/performance architecture changes
- Weakening or deleting tests without replacement

## Do not
- Open an issue-driven wave PR without `Wave issue: #N`
- Invent a post-facto issue just to satisfy the PR linkage contract for automation-originated work
- Skip preflight when the risk triggers in AGENTS require it
- Expand scope because it seems convenient
- Create extra markdown planning files
