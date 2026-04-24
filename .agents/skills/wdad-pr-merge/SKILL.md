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
3. Confirm the latest `verify` result and any release-only evidence required for this wave.
4. Decide whether deeper review is warranted:
   - architecture-review style checks for boundaries, dependencies, contracts, or public surfaces
   - test-adversary style checks when test evidence looks thin or the wave is medium/high risk
   - reality-sync style checks when durable docs or policy may now drift from reality
5. If serious issues are found, leave detailed PR comments and stop without merging.
6. If ready, merge the PR to `main`.
7. Close the PR and linked issues. Do not delete issues.
8. Prune the merged branch.
9. Delete the temporary worktree used for the wave, if one exists and is safe to remove.
10. Report what merged, which issues closed, which branch was pruned, which worktree was deleted, and what remains open.

## Output

- final decision: merged / blocked
- blocking comments posted, if any
- merge result
- linked issues closed
- branch pruning result
- worktree cleanup result
- follow-up issues or risks left open

## Do not

- merge if acceptance checks, verification, or closeout evidence is missing
- delete issues
- delete a worktree unless it is clearly temporary for this wave
- hide serious issues behind follow-up cleanup
