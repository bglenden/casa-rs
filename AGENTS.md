# Agent Operating Contract

Truth class: normative
Last reality check: 2026-04-19
Verification: just verify

## Project purpose

Implement native Rust libraries and applications that read, write, and
manipulate casacore-compatible persistent data while preserving on-disk
interoperability.

## Project profile

Primary language: Rust
Secondary languages: Python and shell for packaging, docs, test harnesses, and automation
Project type: workspace libraries + CLI/TUI apps + Python package
Architecture style: layered workspace crates with explicit persistence and interoperability boundaries

## Truth hierarchy

1. Code, tests, CI, and on-disk interoperability behavior are operational truth.
2. Accepted ADRs are architectural decision truth.
3. `ARCHITECTURE.md` describes the current workspace structure and must match reality.
4. GitHub Issues / Project record planning and history, not current architecture.
5. `docs/Planning/` is historical or program-reference material unless a file explicitly says otherwise.

## References

- C++ source: `~/SoftwareProjects/casacore`
- CASA app/source reference: `~/SoftwareProjects/casa`
- Upstream: `https://github.com/casacore/casacore`
- Local C++ headers/libs installed via Homebrew.

## Worktrees

- `main` is in `~/SoftwareProjects/casa-rs`.

## Standard commands

- Setup: `just setup`
- Fast check: `just quick`
- Full wave verification: `just verify`
- Smoke gate: `just smoke`
- Lint: `just lint`
- Typecheck: `just typecheck`
- Test: `just test`
- Architecture checks: `just arch-check`
- Docs check: `just docs-check`
- Graphs/maps: `just graph`
- Blocking C++ interop release gate: `just release-cpp-interop`
- Informational release performance suite: `just release-perf`
- Release-only install gate: `scripts/test-install-suite.sh`
- Release/tag-only CI-like coverage gate: `scripts/run-coverage.sh --ci-like`
- Slow parity suites: `scripts/test-slow.sh`
- Release: `scripts/release.sh <version>`

## Planning mode policy

Use planning/read-only mode before editing when:

- adopting or changing the methodology surface
- shaping a wave
- crossing crate/package boundaries
- changing public APIs, file formats, schemas, provider contracts, dependencies, concurrency, or performance-sensitive algorithms
- diagnosing repeated CI failures
- performing architecture review, test-adversary review, or reality sync

Planning output must include:

- files/modules likely touched
- tests to add/update
- architecture impact
- stop conditions
- commands to run

Planning mode may draft ADRs and issue updates.
Planning mode may not approve architecture changes.

## Skills policy

Use repository skills in `.agents/skills/` for repeated WDAD procedures.
Skills may define checklists and output formats.
Skills must not be the only source of architectural policy or planning state.

## Board transition map

Primary WDAD entry points should match the GitHub Project transition being performed:

- `Backlog -> Shaped`: use `wdad-wave-shaping`
- `Shaped -> Current Wave / Implementing`: use `wdad-wave-execution`
- `Implementing -> Review`: use `wdad-wave-closeout`
- `Review -> Done`: use `wdad-wave-closeout`

Use the specialist sidecar skills only when the transition skill calls for them or
when the user asks for the narrower review directly:

- `wdad-implementation-preflight` is the planning gate used inside
  `wdad-wave-execution` when a wave is risky enough to require preflight.
- `wdad-architecture-review`, `wdad-test-adversary-review`, and
  `wdad-reality-sync` are closeout sidecars used by `wdad-wave-closeout`.
- `wdad-stabilization-wave` is the specialized shaping/start path for no-feature
  entropy-reduction waves.
- `wdad-project-board-setup` exists for creating or repairing the GitHub Project
  surface itself, not for ordinary wave progression.

### PR linkage contract

Every issue-driven wave PR must include an explicit board link in the PR body:

`Wave issue: #N`

Automation- or gate-originated repair PRs that did not start from a real wave
issue must instead include an explicit provenance marker in the PR body:

`Wave source: automation <name>`

Use `Wave source:` only when the work genuinely did not originate from a shaped
or backlog-tracked issue. If the automation or gate failure reveals broader
product, architecture, or stabilization work that should remain on the board,
open or link the real issue up front and use `Wave issue: #N` instead of
backfilling a synthetic issue after implementation.

Use `Closes #N` only for issues that should auto-close on merge. Do not use
`Closes #N` for the wave issue unless the merge itself is intended to close the
wave. The project-sync automation uses the explicit `Wave issue: #N` marker to
drive board transitions. `Wave source:` is canonical PR provenance for
automation-originated work but does not by itself drive board transitions.

## Decision authority

Agents may decide:

- implementation details inside an existing crate or module boundary
- local refactors that preserve public behavior
- test structure for approved behavior
- small private helper APIs

Agents must stop before:

- adding a new top-level crate, package, or app family
- changing public APIs or persisted data formats
- changing versioned provider-contract bundles or other external contracts
- adding substantial dependencies
- changing dependency direction between major crate layers
- changing concurrency or runtime model
- changing major performance-sensitive algorithms
- weakening or deleting tests without replacement
- editing accepted ADRs except to add supersession metadata when explicitly asked

## Project-specific rules

- Prefer idiomatic Rust APIs over direct C++ API mirroring.
- Use `casa-*` names for reusable libraries and `casars-*` names for app/runtime crates. This repo is a native Rust implementation, not a Rust wrapper around casacore C++.
- Do not reimplement commodity infrastructure when a good Rust crate already exists; for example prefer `ndarray`.
- API docs belong in source (`///`, `//!`) and are rendered by `cargo doc`.
- For `casars` TUI work, follow `docs/casars-tui-framework.md`; new apps must conform to its shell-family conventions instead of inventing app-local UI structure.
- For functionality-provider contracts, follow `docs/provider-contracts.md`. Treat the versioned schema bundle as the boundary contract; UI views are derived projections, not separate sources of truth.
- Prefer red/green development: add a failing regression test first when practical.
- If C++ has a demo for a supported module, provide a Rust equivalent demo.
- When implementing new casacore-c++ functionality, document public items at roughly the corresponding C++ doxygen level and reference the upstream class or function names.
- Shared CASA dataset root defaults to `../casatestdata`; override with `CASA_RS_TESTDATA_ROOT`.
- Canonical measures runtime data lives in a CASA-compatible table tree rooted at `~/.casa/data`; override with `CASA_RS_MEASURESPATH`.
- `CASA_RS_DATA` is deprecated compatibility only; do not prefer it in new code or docs.
- Reuse upstream CASA/casaconfig tables and bundle layouts for measures data; do not introduce new embedded/raw measures assets when the CASA-table runtime model can be used instead.
- Do not treat `/private/tmp` as the canonical home for shared CASA datasets.
- Small bundled real-MS CI fixtures live in `crates/casa-ms/tests/fixtures/`.
- Do not introduce new backlog-style `TODO`, `FIXME`, `XXX`, or `HACK` comments as source of truth. If a code comment needs a backlog reference, point at a GitHub issue.

## Merge policy

- Squash-on-merge into `main`: one commit per wave or PR.
- During development, checkpoint commits are fine.
- Use `git merge --squash` locally or GitHub's "Squash and merge".
- WDAD closeout should fast-forward the local `main` checkout in `~/SoftwareProjects/casa-rs`
  to `origin/main` after a successful GitHub merge when that can be done safely.
  If unrelated local changes or branch state block a safe fast-forward, report the
  blocker instead of forcing the update.

## Definition of Done

A change is not done until:

- `just verify` passes, or any intentional exclusion is called out explicitly in the wave closeout
- relevant tests were added or updated
- bug fixes include regression coverage
- docs or ADRs were updated if reality changed
- issue closeout records actual outcome, verification evidence, and remaining risks
- no new backlog comments were added without an issue reference

## Verification policy

- `just quick` is the normal local iteration gate.
- `just verify` is the default full wave gate: SPDX, format, lint, workspace tests, and the editable Python package test surface.
- `just smoke` is the named smoke/release gate for Python wheel-install checks and Rust demo/example verification.
- Python-facing scripts must resolve Python `>=3.10` through the repo resolver rather than assuming `python3` in the current shell.
- `scripts/test-release-cpp-interop.sh` is the blocking release gate for Rust/C++ interop suites and stays outside the default `cargo test --workspace` path.
- `scripts/test-release-perf.sh` is informational release evidence by default; set `CASA_RS_ENFORCE_PERF=1` to turn named perf thresholds back into hard failures.
- `scripts/test-install-suite.sh` and `scripts/run-coverage.sh --ci-like` are release-oriented heavy gates excluded from `just verify`; releases and version-tag CI run them explicitly.
- Routine branch merges should not run `scripts/test-install-suite.sh` or `scripts/run-coverage.sh --ci-like` unless the user explicitly asks for release/tag-equivalent validation or for direct reproduction of those heavy gates.
- GitHub PR CI runs the lighter `lint_test` and Python package checks; version-tag pushes additionally run the smoke, suite-install, and CI-like coverage gates.
- `scripts/test-slow.sh` is opt-in for heavy CASA parity suites and must stay outside the default `cargo test --workspace` path.
- Coverage changes should maintain a safety margin above the CI threshold; with the current 75% requirement, target at least 78%.
- To reproduce GitHub Actions locally, use `scripts/ci-local.sh build`, then `scripts/ci-local.sh pr` for pull-request jobs or `scripts/ci-local.sh tag` for version-tag jobs. Individual job repro commands are `lint_test`, `python_package`, `smoke`, `suite_install`, and `coverage`.
- For imaging-program work, wave closure still means implementation, gate verification, and end-of-wave review/fix pass once the GitHub wave is approved.
- Release tags must be cut with `scripts/release.sh`; do not create release tags directly with `git tag`.

## Documentation rules

- Use GitHub Issues / Project for active wave planning.
- Do not create new local markdown planning files unless explicitly asked.
- Treat `docs/Planning/` as historical or program-reference material, not canonical wave status.
- Use ADRs only for architecturally significant decisions.
- Mark generated docs as generated and obsolete docs as historical or delete them.

## Review rules

Before finalizing:

1. perform an independent architecture review pass for medium/high-risk work
2. perform a test-adversary review for medium/high-risk work
3. perform a reality-sync review against `ARCHITECTURE.md`, `TESTING.md`, ADRs, and generated artifacts when reality changed
