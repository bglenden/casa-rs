# Agent Operating Contract

Truth class: normative
Last reality check: 2026-04-24
Verification: just docs-check

## Purpose

Implement native Rust libraries and applications for casacore-compatible
persistent data while preserving on-disk interoperability.

## Truth Order

1. Code, tests, CI, and interoperability behavior.
2. Accepted ADRs.
3. `ARCHITECTURE.md` and `TESTING.md` for scaled-layer policy.
4. GitHub issues and board state for planning history.
5. `docs/Planning/` is historical unless a file says otherwise.

## Commands

- Setup: `just setup`
- Fast loop: `just quick`
- Pre-review gate: `just verify`
- Smoke/release gate: `just smoke`
- Lint/type/test: `just lint`, `just typecheck`, `just test`
- Architecture/docs: `just arch-check`, `just docs-check`, `just graph`
- C++ interop release gate: `just release-cpp-interop`
- Performance release evidence: `just release-perf`
- Slow parity: `scripts/test-slow.sh`
- Release: `scripts/release.sh <version>`

## WDAD Workflow

- Board columns: `Backlog`, `Ready`, `In Progress`, `Review`, `Done`, `Parked`.
- `Backlog -> Ready`: use `wdad-backlog-to-ready`.
- `Ready -> In Progress`: use `wdad-wave-implementation`.
- `In Progress -> Review`: use `wdad-wave-implementation` after a PR exists and `just verify` is recorded.
- `Review -> Done`: use `wdad-pr-merge`.
- Use scaled sidecars when risk justifies them: `wdad-architecture-review`,
  `wdad-test-adversary-review`, `wdad-reality-sync`,
  `wdad-ci-failure-diagnosis`, `wdad-stabilization-wave`, and
  `wdad-planning-board-audit`.

## PR Linkage

Issue-driven wave PRs must include:

`Wave issue: #N`

Automation- or gate-originated repair PRs without a real wave issue must include:

`Wave source: automation <name>`

Use `Closes #N` only for issues that should auto-close on merge.

## Stop And Ask Before

- adding a new top-level crate, package, or app family
- changing public APIs, persisted formats, provider-contract bundles, or other external contracts
- adding substantial dependencies
- changing dependency direction, runtime model, concurrency, or major performance algorithms
- weakening or deleting tests without replacement
- editing accepted ADRs except to add explicitly requested supersession metadata
- committing directly to `main`
- merging, pruning branches, or deleting worktrees unless the user invoked `wdad-pr-merge` or asked directly

## Project Rules

- Prefer idiomatic Rust over direct C++ API mirroring; this is not a Rust wrapper around casacore C++.
- Use `casa-*` for reusable libraries and `casars-*` for app/runtime crates.
- API docs belong in source comments rendered by `cargo doc`.
- For `casars` TUI work, follow `docs/casars-tui-framework.md`.
- For provider contracts, follow `docs/provider-contracts.md`; versioned schema bundles are the boundary contract.
- Prefer red/green development when practical.
- When implementing new casacore-C++ functionality, document public items at roughly the corresponding upstream doxygen level.
- Shared CASA datasets default to `../casatestdata`; override with `CASA_RS_TESTDATA_ROOT`.
- Measures runtime data defaults to `~/.casa/data`; override with `CASA_RS_MEASURESPATH`.
- `CASA_RS_DATA` is deprecated compatibility only.
- Do not use `/private/tmp` as the canonical shared CASA dataset location.
- Small bundled real-MS CI fixtures live in `crates/casa-ms/tests/fixtures/`.
- Do not add backlog-style `TODO`, `FIXME`, `XXX`, or `HACK` comments unless they reference a GitHub issue.

## Verification Policy

- `just quick` is the normal local iteration gate.
- `just verify` is the default full wave gate.
- `just smoke`, `scripts/test-install-suite.sh`, and
  `scripts/run-coverage.sh --ci-like` are release/tag-oriented heavy gates.
- Routine branch merges should not run release/tag-only heavy gates unless requested.
- GitHub PR CI runs the lighter lint/test and Python package checks.
- Version-tag CI additionally runs smoke, suite-install, and CI-like coverage.
- Coverage must stay above the 75% CI threshold; target at least 78%.
- Reproduce GitHub Actions locally with `scripts/ci-local.sh build`, then
  `scripts/ci-local.sh pr` or `scripts/ci-local.sh tag`.

## Done

A wave is done only after relevant tests pass, `just verify` passes or exclusions
are recorded, issue closeout records the actual result, docs/ADRs are updated
when reality changed, and medium/high-risk work gets the needed architecture,
test-adversary, or reality-sync review.
