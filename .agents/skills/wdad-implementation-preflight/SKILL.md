---
name: wdad-implementation-preflight
description: Specialist planning gate used by `wdad-wave-execution` immediately before coding a risky approved wave. Use it for first waves, cross-module work, public API/data format changes, dependencies, performance-sensitive work, or ambiguity. Produces a compact plan and stop conditions without editing files.
---

# Skill: Implementation preflight

## Purpose
Inspect before coding and expose architecture/test risks when a `Shaped` item is
about to become active implementation.

## Mode
Planning/read-only mode. Do not edit files.

## Required inputs
- Current wave issue
- AGENTS.md
- ARCHITECTURE.md
- TESTING.md
- accepted ADRs
- relevant code

## Procedure
1. Confirm the wave is shaped and approved.
2. Inspect likely code paths.
3. Identify files/modules likely to change.
4. Define tests to add/update.
5. Check for architecture impact: APIs, data formats, dependencies, boundaries, concurrency, performance.
6. State whether an ADR or human decision is required.
7. Define explicit stop conditions.
8. List commands to run.

## Output
Use this format:

```md
## Implementation plan
Issue: #N <title>

### Scope understood
...

### Files/modules likely touched
- ...

### Tests to add/update
- ...

### Architecture impact
- Public API change: yes/no
- Data format change: yes/no
- New dependency: yes/no
- Boundary change: yes/no
- ADR needed: yes/no

### Stop conditions
- ...

### Commands
- just quick
- just verify
```

## Do not
- Implement code
- Treat this plan as approval for architecture changes
