# WDAD skills

These repo-local Codex skills mirror the WDAD v0.4 starter-kit skill set, with
casa-rs-specific Scaled WDAD policy layered in `AGENTS.md`, `ARCHITECTURE.md`,
and `TESTING.md`.

Core skills:

- `wdad-backlog-to-ready`
- `wdad-wave-implementation`
- `wdad-pr-merge`

Scaled skills:

- `wdad-architecture-review`
- `wdad-test-adversary-review`
- `wdad-reality-sync`
- `wdad-ci-failure-diagnosis`
- `wdad-planning-board-audit`
- `wdad-stabilization-wave`

casa-rs domain skills:

- `casa-rs-imaging-performance`

Durable policy belongs in `AGENTS.md`, `ARCHITECTURE.md`, `TESTING.md`,
accepted ADRs, tests, and CI. Skills are reusable procedures, not hidden
sources of truth.

The repo-level WDAD scope contract in `AGENTS.md` applies to all skills:
approved outcome, included issues, and acceptance checks cannot be deferred or
descoped without explicit user signoff.

WDAD v0.4 also requires code waves to run a bounded `refactor` pass on the code
involved in the current wave before moving to `Review`. If a wave has no code
surface, record a not-applicable rationale. Missing refactor evidence blocks
`Review -> Done` for code waves.
