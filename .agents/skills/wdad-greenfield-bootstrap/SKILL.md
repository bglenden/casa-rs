---
name: wdad-greenfield-bootstrap
description: Use when setting up Wave-Driven Agentic Development in a new or nearly empty repository. Triggers before product implementation; stays in planning/read-only mode until the human approves architecture and Wave 1.
---

# Skill: Greenfield bootstrap

## Purpose
Install the methodology surface for a new repo without starting product implementation.

## Mode
Use planning/read-only mode until the human approves the proposed files, initial ADRs, quality gates, and first wave.

## Required inputs
- Project purpose or initial README, if available
- Dominant language/toolchain
- Desired first feature or capability, if known

## Procedure
1. Inspect the repo and infer the project profile.
2. Propose the durable file set: README.md, AGENTS.md, ARCHITECTURE.md, TESTING.md, docs/adr/, justfile, GitHub issue and PR templates.
3. Draft architecture boundaries and language-specific quality gates.
4. Draft initial ADRs only for load-bearing decisions.
5. Propose GitHub Project statuses and fields.
6. Propose the first modest wave issue.
7. Stop for human review.

## Output
- Files to create or update
- Initial architecture assumptions
- ADRs needing human acceptance
- First `just quick` / `just verify` proposal
- First wave issue draft

## Do not
- Implement product code
- Create local backlog or wave markdown files
- Approve architecture decisions yourself
