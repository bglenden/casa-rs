---
name: wdad-test-adversary-review
description: Use to review tests skeptically after implementation. Assumes the implementation may be wrong and identifies shallow, tautological, over-mocked, or missing tests. Read-only by default.
---

# Skill: Test-adversary review

## Purpose
Find ways the implementation could be wrong while tests still pass.

## Mode
Read-only/planning mode. Do not edit files unless asked after reporting.

## Required inputs
- Current branch/diff
- Wave issue acceptance checks
- Changed tests
- Relevant implementation files
- TESTING.md

## Procedure
1. Map every acceptance check to test evidence.
2. Identify happy-path-only tests.
3. Identify over-mocking and internal mocks.
4. Identify tautological tests that repeat implementation logic.
5. Identify missing boundary, error, regression, integration, GUI, or performance tests.
6. Suggest concrete tests by behavior.

## Output
- Acceptance-check coverage table
- Weak or shallow tests
- Missing tests
- Tests that should be integration rather than mocked unit tests
- Whether current tests are sufficient to merge

## Do not
- Judge code style unless it affects test meaning
- Treat line coverage as proof of behavioral coverage
