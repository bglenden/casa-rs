---
name: wdad-pr-merge
description: Use for final PR review and housekeeping. If serious issues are found, leave detailed PR comments and stop. If the PR is ready, merge to main, close linked issues, prune the branch, delete the temporary worktree, and report the result.
---

# Skill: PR merge

## Purpose

Perform the final independent review and complete PR housekeeping.

## Mode

This is a write-capable housekeeping skill. Invoking it authorizes final review, merge, linked issue closure, branch pruning, and temporary worktree deletion when checks pass.

## Required inputs

- current PR or unambiguous PR URL/number
- wave issue or wave-lead issue
- linked issues covered by the PR
- `README.md`
- `AGENTS.md`
- `ARCHITECTURE.md` and `TESTING.md`, if present

## Procedure

1. Confirm the PR targets `main` and comes from a wave branch, not direct work on `main`.
2. Review the PR against the wave outcome, non-goals, acceptance checks, and stop conditions.
3. Confirm that any follow-up, deferral, non-goal, or out-of-scope classification is either newly discovered adjacent work or has explicit user signoff when it moves approved outcome, included issues, or acceptance checks out of the wave.
4. Confirm that a bounded `refactor` pass ran on the code involved in the wave, or that the PR records a credible not-applicable rationale for a no-code wave.
5. Confirm one current green `verify` result and any release-only evidence
   required for this wave. Accept local or genuinely equivalent hosted evidence;
   do not require both or wait for duplicate cross-environment assurance.
6. Decide whether deeper review is warranted:
   - architecture-review style checks for boundaries, dependencies, contracts, or public surfaces
   - test-adversary style checks when test evidence looks thin or the wave is medium/high risk
   - reality-sync style checks when durable docs or policy may now drift from reality
7. If serious issues, unsigned deferrals, or missing refactor evidence are found, leave detailed PR comments and stop without merging.
8. If ready, merge the PR to `main`.
9. Close the PR and linked issues. Do not delete issues.
10. Prune the merged branch.
11. Delete the temporary worktree used for the wave, if one exists and is safe to remove.
12. Report what merged, which issues closed, which branch was pruned, which worktree was deleted, what remains open, and where approved-scope deferral signoff is documented if any exists.

## Output

- final decision: merged / blocked
- blocking comments posted, if any
- merge result
- linked issues closed
- refactor pass evidence
- branch pruning result
- worktree cleanup result
- follow-up issues or risks left open
- approved-scope deferrals and signoff location, if any

## Do not

- merge if acceptance checks, verification, or closeout evidence is missing
- merge if the refactor pass is missing for a code wave
- merge if approved outcome, included issues, or acceptance checks were moved to follow-up, deferral, non-goal, or out-of-scope language without explicit user signoff
- delete issues
- delete a worktree unless it is clearly temporary for this wave
- hide serious issues behind follow-up cleanup
