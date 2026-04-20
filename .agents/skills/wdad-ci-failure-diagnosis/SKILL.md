---
name: wdad-ci-failure-diagnosis
description: Use after repeated CI/test failures or repair loops. Analyzes logs and diff in planning/read-only mode before further edits.
---

# Skill: CI failure diagnosis

## Purpose
Stop blind repair loops by diagnosing repeated failures before further edits.

## Mode
Planning/read-only mode. Do not edit files.

## Required inputs
- CI logs or local failure output
- Current diff
- Build/test configuration
- Relevant source/tests

## Procedure
1. Summarize the failing command and first meaningful error.
2. Determine whether the failure is implementation, test, environment, architecture, dependency, or flaky.
3. Check whether the failure indicates a wrong wave assumption or ADR/doc mismatch.
4. Propose the minimal repair plan.
5. State whether it is safe to resume implementation mode.

## Output
- Root cause
- Failure category
- Minimal repair plan
- Scope/ADR/doc/test changes needed
- Safe-to-edit recommendation

## Do not
- Patch failures before diagnosing
- Delete or weaken tests to get green
