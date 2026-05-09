# Agent Operating Contract

Truth class: normative
Last reality check: 2026-05-09
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

## Main Commands

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
- Local install from checkout: `just install-local`
- Local suite install: `just install-local-suite`
- Local GUI install: `just install-local-gui`
- Release install: `just install-release <version>`
- Local CASA/C++ task runs use `/Users/brianglendenning/SoftwareProjects/casa-build/venv/bin/python`, which has the `~/SoftwareProjects/casa` built `casatasks` and `casatools` wheels installed.

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

## WDAD Scope Contract

Once a wave or issue is approved, its outcome, included issues, acceptance
checks, and stop conditions are the approved scope contract.

- Follow-up issues are allowed for newly discovered adjacent work.
- Approved outcome, included issues, or acceptance checks may be moved to a
  follow-up, deferral, non-goal, or out-of-scope bucket only after explicit user
  signoff.
- If approved work cannot be completed in the current wave, stop and ask before
  changing the scope contract.
- Any approved-scope deferral must record where the user signoff is documented
  in the issue closeout and PR.
- Review and Done are blocked when approved scope was deferred without explicit
  user signoff.
- Parked is invalid for approved-scope deferral unless the deferral reason and
  user signoff are both recorded.

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
- moving approved outcome, included issues, or acceptance checks into
  follow-up tickets, deferrals, non-goals, or out-of-scope language
- weakening or deleting tests without replacement
- editing accepted ADRs except to add explicitly requested supersession metadata
- committing directly to `main`
- merging, pruning branches, or deleting worktrees unless the user invoked `wdad-pr-merge` or asked directly

## Project Rules

- Before implementing behavior that exists in CASA/casacore C++, inspect the
  relevant upstream task/tool/library path first and preserve its semantics
  unless there is an explicit reason to diverge.
- Use the shared least-squares helper and its well-exercised linear algebra
  backend for polynomial or linear least-squares solves; do not add ad hoc
  normal-equation or Gaussian-elimination solvers.
- Prefer idiomatic Rust over direct C++ API mirroring; this is not a Rust wrapper around casacore C++.
- Use `casa-*` for reusable libraries and `casars-*` for app/runtime crates.
- API docs belong in source comments rendered by `cargo doc`.
- For `casars` TUI work, follow `docs/casars-tui-framework.md`.
- For provider contracts, follow `docs/provider-contracts.md`; versioned schema bundles are the boundary contract.
- Prefer red/green development when practical.
- When implementing new casacore-C++ functionality, document public items at roughly the corresponding upstream doxygen level.
- Shared CASA C++ test datasets default to `../casatestdata`; override with `CASA_RS_TESTDATA_ROOT`.
- CASA tutorial datasets default to `~/SoftwareProjects/casa-tutorial-data`; override with `CASA_RS_TUTORIAL_DATA_ROOT`.
- Default gates may use only `CASA_RS_TESTDATA_ROOT`, `../casatestdata`, or
  `~/SoftwareProjects/casatestdata` for shared data discovery.
- Slow, release, parity, and tutorial gates must run the shared test-data
  preflight and report the selected root before exercising external data.
- Tutorial data registry entries live in `casa-test-support`; local tutorial
  artifacts are staged under `CASA_RS_TUTORIAL_DATA_ROOT/tutorial-parity/...`
  or `~/SoftwareProjects/casa-tutorial-data/tutorial-parity/...`.
- Measures runtime data defaults to `~/.casa/data`; override with `CASA_RS_MEASURESPATH`.
- `CASA_RS_DATA` is deprecated compatibility only.
- Do not use `/private/tmp` as the canonical shared CASA dataset location.
- Do not add personal workstation archives as implicit default-gate fallbacks;
  require an explicit env var for one-off local data.
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
when reality changed, any approved-scope deferral records explicit user signoff,
and medium/high-risk work gets the needed architecture, test-adversary, or
reality-sync review.
