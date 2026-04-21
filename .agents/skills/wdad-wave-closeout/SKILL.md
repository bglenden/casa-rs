---
name: wdad-wave-closeout
description: Primary board-transition skill for `Implementing -> Review -> Done`. Runs the closeout packet, uses the review sidecar skills, and finishes merge/issue/board closure.
---

# Skill: Wave closeout

## Purpose
Move an active wave through review and into done with a predictable closeout
packet and board state.

## Mode
Planning/read-only first. Edits are allowed for issue/PR closeout text, doc
repairs, and narrowly scoped follow-up fixes found during closeout review.

## Required inputs
- Current wave issue
- Current PR or intended PR
- AGENTS.md
- ARCHITECTURE.md
- TESTING.md
- accepted ADRs
- verification evidence

## Procedure
1. Confirm the implementation scope matches the wave issue.
2. Confirm the PR body includes `Wave issue: #N`.
3. Fill the issue closeout block with:
   - implemented outcome
   - changed-from-plan notes
   - deferred work
   - verification evidence
   - docs / ADR updates
   - known risks
4. Run the required closeout sidecars when the risk level warrants them:
   - `wdad-architecture-review`
   - `wdad-test-adversary-review`
   - `wdad-reality-sync`
5. Assemble or refresh the PR review packet and verification summary.
6. Move the work into review by using a non-draft PR. The board automation treats
   a non-draft PR with `Wave issue: #N` as `Review`.
7. Merge only after required CI and human decisions are clear.
8. After merge, sync the local `main` checkout in `~/SoftwareProjects/casa-rs` to
   the GitHub-merged `origin/main` when that can be done safely with a fast-forward.
   Do not overwrite, stash, or force through unrelated local changes; report the
   blocker instead if local state prevents a safe fast-forward.
9. Close the wave issue and confirm the project item lands on `Done`.

## Output
- Closeout packet
- Required follow-up issues
- Merge / no-merge recommendation
- Final board-state confirmation
- Local `main` sync confirmation or blocker

## Do not
- Merge a wave whose issue closeout is still empty
- Assume a merged PR alone closes the wave unless the wave issue was meant to auto-close
- Use `Closes #N` for the wave issue by default; reserve that for issues meant to close immediately on merge
- Force local `main` forward through unrelated local changes just to finish closeout
