---
name: wdad-planning-board-audit
description: Use on a schedule to audit a project's planning board for state/metadata consistency. Flags items that were moved into stages like Ready, Review, or Done without the supporting detail or evidence those stages imply.
---

# Skill: Planning board audit

## Purpose

Keep the planning board honest when humans can move items manually.

## Mode

Read-only/planning mode. Do not move cards or edit issues unless separately instructed.

## Required inputs

- project board URL, project identifier, or another unambiguous way to locate the board
- the repo's stage semantics from `README.md` or `AGENTS.md`
- issue template expectations
- access to the linked issues and PRs

## Procedure

1. Load the current board items and their stages.
2. For each item, identify the linked issue or PR that acts as the source of truth.
3. Validate the board state against WDAD semantics.
4. Flag items that are missing the evidence required for their current stage.
5. Recommend the smallest corrective action for each invalid item.
6. Summarize whether the board is currently consistent enough to trust operationally.

## Default checks

- `Backlog -> Ready`: outcome, non-goals, acceptance checks, stop conditions, and verification path exist.
- `Ready -> In Progress`: the wave is shaped enough to start and has no unresolved blocking decision recorded.
- `In Progress -> Review`: the PR or equivalent review artifact exists, the current `verify` result is recorded, a bounded refactor pass is recorded or marked not applicable for a no-code wave, and no approved outcome, included issue, or acceptance check was deferred or descoped without explicit user signoff recorded in the issue or PR.
- `Review -> Done`: closeout is updated, review evidence exists, follow-up issues are created where needed, and any approved-scope deferral records explicit user signoff.
- `Parked`: the deferral reason is clear enough that the item can be revisited later; parking approved scope is invalid without explicit user signoff.

## Output

- board health summary
- invalid items grouped by stage
- missing detail or evidence for each invalid item
- recommended corrective action for each item
- any repeated failure pattern that suggests the workflow or template should change

## Do not

- silently move cards back
- silently rewrite issues
- treat cosmetic issue-template differences as board inconsistency unless they block real stage meaning
