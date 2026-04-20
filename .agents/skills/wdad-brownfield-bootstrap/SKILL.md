---
name: wdad-brownfield-bootstrap
description: Use when adopting Wave-Driven Agentic Development in an existing repository. Performs an audit first and documents reality before proposing process files or refactors.
---

# Skill: Brownfield bootstrap

## Purpose
Adopt the methodology in an existing repo without pretending it is cleaner than it is.

## Mode
Use planning/read-only mode for the audit. Do not edit until the human accepts the adoption plan.

## Required inputs
- Repository root
- Any known build/test commands
- Whether adoption should be light or strict

## Procedure
1. Inventory dependencies, build commands, test commands, CI, and package/module layout.
2. Identify actual architecture: boundaries, public interfaces, data formats, runtime model, persistence, adapters.
3. Identify drift: duplicated patterns, conflicting libraries, unclear ownership, missing tests, obsolete docs.
4. Propose an honest partial `just verify` that reports exclusions rather than hiding them.
5. Draft durable files to describe current reality.
6. Draft ADRs for existing load-bearing decisions.
7. Propose the first two waves, biased toward setup/stabilization.
8. Stop for human review.

## Output
- Audit summary
- Reality vs aspiration gaps
- Proposed durable docs
- Initial quality gate
- Initial ADR drafts
- First waves

## Do not
- Start broad cleanup during audit
- Rewrite architecture opportunistically
- Fail the whole adoption because old code is not yet ideal
