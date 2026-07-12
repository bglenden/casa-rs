---
name: wdad-ci-failure-diagnosis
description: Use when `verify`, release, or scheduled CI checks fail repeatedly. Produces a diagnosis, a likely root cause, and the smallest safe next step.
---

# Skill: CI failure diagnosis

## Purpose

Shorten the loop when the slower gates fail.

## Mode

Read-only first. Do not guess blindly.

## Required inputs

- failing command or CI log
- current wave issue
- recent related changes
- `TESTING.md`, if present

## Procedure

1. Reproduce or inspect the failure.
2. Identify whether it belongs to `quick`, `verify`, or `release/scheduled`.
3. Check whether the same required gate already has a current green result from
   an accepted local or hosted environment. If so, treat the second-environment
   failure as non-blocking unless it reveals unresolved platform-specific risk
   or the user explicitly asked for both environments.
4. Narrow the likely root cause only as far as needed for the current decision;
   at the current project stage, turnaround is more important than repairing a
   duplicate environment signal.
5. Decide whether the fix belongs in the current wave or a follow-up.
6. If the failure blocks an approved outcome, included issue, or acceptance check, do not recommend deferring it without explicit user signoff.
7. Recommend the smallest safe next step, including stopping without a repair
   when equivalent accepted evidence is already green.

## Output

- failure summary
- likely root cause
- immediate next step
- whether the gate placement itself should change
