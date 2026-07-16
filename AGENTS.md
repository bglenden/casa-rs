# Agent Operating Contract

Truth class: normative
Last reality check: 2026-07-04
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
- CASA `plotms` oracle PNGs should use the headless export path:
  set `DISPLAY=${DISPLAY:-:99}`, `QT_QPA_PLATFORM=${QT_QPA_PLATFORM:-offscreen}`,
  and `MPLBACKEND=${MPLBACKEND:-Agg}`, then call `casaplotms.plotms`
  with `showgui=False`, `plotfile=...`, `expformat="png"`, and
  `overwrite=True`. On the local macOS CASA build, no Xvfb command is part of
  the working recipe; `plotms` still requires `DISPLAY` to be present.
  Do not run this CASA/Qt path inside a shell sandbox that blocks `sysctl`
  CPU-feature queries: Qt may mis-detect the arm64 NEON feature and print
  `Incompatible processor`. Run CASA oracle exports in the normal user
  environment or an explicitly unsandboxed command runner.

## WDAD Workflow

This repo uses Scaled WDAD v0.4.

- Board columns: `Backlog`, `Ready`, `In Progress`, `Review`, `Done`, `Parked`.
- `Backlog -> Ready`: use `wdad-backlog-to-ready`.
- `Ready -> In Progress`: use `wdad-wave-implementation`.
- `In Progress -> Review`: use `wdad-wave-implementation` after a PR exists,
  `just verify` is recorded, and the bounded refactor pass is recorded or
  marked not applicable for a no-code wave.
- `Review -> Done`: use `wdad-pr-merge`; missing refactor evidence blocks code
  wave merge.
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

## WDAD Refactor Gate

Before a code wave moves to `Review`, run the `refactor` skill on the code
involved in the current wave and record the result in the issue closeout or PR.

- Keep the refactor pass bounded to touched or directly exposed code.
- If the wave has no code refactor surface, record a not-applicable rationale.
- If the refactor pass finds a larger coherent cleanup outside the approved
  wave, capture the brief and ask before expanding scope.
- If the refactor pass cannot run for a code wave, stop and ask instead of
  silently skipping it.

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
- expanding a pre-review refactor beyond the code involved in the approved wave
- weakening or deleting tests without replacement
- editing accepted ADRs except to add explicitly requested supersession metadata
- committing directly to `main`
- merging, pruning branches, or deleting worktrees unless the user invoked `wdad-pr-merge` or asked directly

## Project Rules

- For the scientific-notebook program governed by ADR-0007, every wave begins
  with a launchable, fully interactive `casars-mac` prototype backed only by
  deterministic fixture adapters. Record live-app, accessibility, debug-state,
  and visual evidence and obtain explicit interaction approval before wiring
  real persistence, tasks, Python, downloads, retrieval, or model providers.
  Fixture schemas and dummy adapters must not become public or persisted
  contracts. A material interaction change returns the wave to this prototype
  gate.
- Native macOS GUI behavior uses XCTest/XCUIAutomation as the executable
  end-to-end interaction layer. Issue #368 establishes the thin Xcode app host,
  macOS UI Testing Bundle, and stable `just gui-test` entrypoint for the current
  notebook wave; it must be implemented and green before Wave 1 production
  integration begins, and lands with the completed wave PR.
  Thereafter, every user-visible `casars-mac` wave must identify its critical
  workflows during shaping, add or update stable accessibility identifiers and
  XCUITest coverage during implementation, and record a green `just gui-test`
  result from either the consolidated local run or the hosted macOS job before
  Review; both are not required. Core/store tests, debug JSON, accessibility
  inspection, screenshots, and manual/computer-use sessions remain complementary evidence;
  they do not substitute for executable UI tests when claimed interaction
  behavior changes. If the UI-test target or supported macOS runner is
  unavailable, stop and record the blocker instead of silently waiving the
  gate.
- Batch local native GUI automation into exclusive foreground windows. Finish
  compilation before the window, announce it before focus is taken, and run
  the complete suite as one uninterrupted batch. During normal implementation
  use core tests, debug-state checks, and deterministic capture; accumulate
  interaction changes for prototype-handoff and pre-Review GUI gates. Use a
  focused XCUITest run only to diagnose a failure from the consolidated gate.
- Prefer the dedicated logged-in remote Mac worker for consolidated native GUI
  gates when it is available, using `just gui-test-remote` or
  `just notebook-roundtrip-gui-remote`. Keep its checkout clean, select the
  exact pushed revision, and keep the checkout and Xcode DerivedData on its
  internal disk so regenerated app bundles do not repeatedly request removable
  volume access. Place the large Cargo target and retained artifacts on its
  configured external storage. A green remote run is the GUI gate; do not
  repeat it locally solely for duplicate assurance. Local foreground automation
  remains the fallback and focused diagnostic surface.
- Before implementing behavior that exists in CASA/casacore C++, inspect the
  relevant upstream task/tool/library path first and preserve its semantics
  unless there is an explicit reason to diverge.
- When investigating parity or correctness differences against CASA/casacore
  C++, favor targeted instrumentation of both implementations over blind
  parameter experiments or speculative fixes.
- Before adding new library functionality, search the existing Rust library
  surface for reusable or composable behavior; there is now a meaningful chance
  the needed functionality already exists. Unless the current agent is already
  doing that repository-search work as its normal role, run the search in a
  sub-agent so the main implementation context stays focused.
- Use the shared least-squares helper and its well-exercised linear algebra
  backend for polynomial or linear least-squares solves; do not add ad hoc
  normal-equation or Gaussian-elimination solvers.
- Prefer idiomatic Rust over direct C++ API mirroring; this is not a Rust wrapper around casacore C++.
- Use `casa-*` for reusable libraries and `casars-*` for app/runtime crates.
- API docs belong in source comments rendered by `cargo doc`.
- For `casars` TUI work, follow `docs/casars-tui-framework.md`.
- For tutorial or regression evidence that needs `casars` TUI screenshots,
  use the GhosttyKit surface helper in `tools/ghostty-surface-capture`, not
  visible terminal/window screenshots. It runs the TUI in an offscreen macOS
  Ghostty surface with `TERM=xterm-ghostty` and writes PNG screenshots from
  Ghostty's renderer layer, so Kitty graphics and terminal cells are one real
  captured artifact.
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

- At the current project stage, fast turnaround takes precedence over duplicate
  cross-environment assurance. For each required gate, one current green run in
  either the local or hosted environment is sufficient when it executes the
  same command or documented equivalent coverage. Do not wait for, rerun, or
  repair the other environment solely to obtain a second green result. Use the
  second environment only for diagnosis, unresolved platform-specific risk, or
  when the user explicitly requests it. A lighter hosted job does not substitute
  for a broader required gate such as `just verify`.
- Reuse recent green gate evidence when no intervening code, test, build,
  dependency, or runtime-configuration change could affect it. Starting final
  review, rebasing without content changes, or adding documentation/process-only
  commits does not by itself require a rerun. If relevant executable changes
  intervened, rerun only the affected gate.
- `just quick` is the normal local iteration gate.
- `just verify` is the default full wave gate.
- `just smoke`, `scripts/test-install-suite.sh`, and
  `scripts/run-coverage.sh --ci-like` are release/tag-oriented heavy gates.
- One-off repository gates must run Rust checks/tests with
  `CARGO_INCREMENTAL=0` to avoid accumulating stale incremental cache entries
  in local and Codex worktree `target/` directories.
- Routine branch merges should not run release/tag-only heavy gates unless requested.
- GitHub PR CI runs the lighter lint/test and Python package checks.
- Version-tag CI additionally runs smoke, suite-install, and CI-like coverage.
- Coverage must stay above the 75% CI threshold; target at least 78%.
- Reproduce GitHub Actions locally with `scripts/ci-local.sh build`, then
  `scripts/ci-local.sh pr` or `scripts/ci-local.sh tag`.

## Done

A wave is done only after relevant tests pass; one current `just verify` result
is recorded from a local run or documented equivalent hosted coverage, or
exclusions are recorded; code-wave refactor evidence is recorded or no-code
not-applicable rationale exists, issue closeout records the actual result,
docs/ADRs are updated when reality changed, any approved-scope deferral records
explicit user signoff, and medium/high-risk work gets the needed architecture,
test-adversary, or reality-sync review.
