---
name: wdad-test-adversary-review
description: Use for a skeptical review of the tests after implementation. Looks for shallow coverage, over-mocking, and ways the implementation could still be wrong while tests pass.
---

# Skill: Test-adversary review

## Purpose

Find gaps between claimed behavior and actual test evidence.

## Mode

Read-only/planning mode.

## Required inputs

- wave acceptance checks
- changed tests
- relevant implementation files
- `TESTING.md`

## Procedure

1. Map acceptance checks to concrete evidence.
2. Look for happy-path-only tests.
3. Look for tautological or over-mocked tests.
4. Identify missing regression, integration, or edge-case coverage.
5. For user-visible native macOS GUI behavior, require executable
   XCTest/XCUIAutomation evidence for critical launched-app workflows. Verify
   that tests use stable accessibility identifiers, deterministic fixture
   launch arguments, meaningful state/gesture assertions, and production-route
   isolation. Screenshots, accessibility-tree inspection, debug JSON, and
   manual/computer-use sessions are complementary evidence, not substitutes.
6. Treat acceptance checks moved to follow-up, deferral, non-goal, or out-of-scope language without explicit user signoff as not covered.
7. Recommend the smallest meaningful additions.

## Output

- acceptance coverage summary
- weak or shallow tests
- missing tests
- merge readiness from a testing perspective
