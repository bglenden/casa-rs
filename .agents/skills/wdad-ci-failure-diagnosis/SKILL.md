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
3. Narrow the likely root cause.
4. Decide whether the fix belongs in the current wave or a follow-up.
5. Recommend the smallest safe next step.

## Output

- failure summary
- likely root cause
- immediate next step
- whether the gate placement itself should change
