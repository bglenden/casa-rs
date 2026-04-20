---
name: wdad-wave-implementation
description: Use to implement an approved wave after shaping and, when required, implementation preflight. Keeps work inside approved boundaries and updates tests, issue closeout, and review packet.
---

# Skill: Wave implementation

## Purpose
Implement an approved wave under the methodology contract.

## Mode
Implementation mode is allowed only if the wave is approved and no unresolved architecture decision remains.

## Required inputs
- Current wave issue
- Preflight plan, if required
- AGENTS.md
- ARCHITECTURE.md
- TESTING.md
- accepted ADRs

## Procedure
1. Verify the wave issue has scope, non-goals, acceptance checks, test plan, and authority limits.
2. If preflight is required but missing, stop and run `wdad-implementation-preflight`.
3. Implement the smallest coherent change set.
4. Add/update tests for acceptance checks.
5. Run `just quick` frequently.
6. Run `just verify` before finalizing.
7. Update issue closeout with actual outcome, verification evidence, deviations, docs/ADR updates, and risks.
8. Prepare PR review packet.

## Stop immediately before
- Public API or data format changes not already approved
- New substantial dependencies
- Boundary/concurrency/runtime/performance architecture changes
- Weakening or deleting tests without replacement

## Do not
- Expand scope because it seems convenient
- Create extra markdown planning files
