---
name: wdad-wave-shaping
description: Use when turning a backlog idea or rough GitHub issue into a wave-ready issue with outcome, non-goals, acceptance checks, architecture impact, and test plan. Does not edit code.
---

# Skill: Wave shaping

## Purpose
Turn rough work into a wave-ready issue.

## Mode
Planning/read-only mode. Do not edit code.

## Required inputs
- Backlog issue or rough request
- AGENTS.md
- ARCHITECTURE.md
- TESTING.md
- relevant ADRs

## Procedure
1. Read the source issue/request and durable repo files.
2. Identify the outcome and non-goals.
3. Write falsifiable acceptance checks.
4. Identify modules likely touched.
5. Identify architecture impact and ADR need.
6. Draft a test plan and agent authority limits.
7. Ask only blocking questions.
8. Put the result in the wave issue body or comment if authorized.

## Output
- Outcome
- Non-goals
- Current understanding
- Acceptance checks
- Architecture impact
- Test plan
- Agent authority and stop conditions
- Blocking questions

## Do not
- Start implementation
- Create local wave planning files
