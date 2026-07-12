# Testing Strategy

Truth class: normative
Last reality check: 2026-07-11
Verification: just verify

## Test categories

- Unit tests: pure value, parsing, math, and domain logic inside a single crate
- Integration tests: cross-crate behavior, on-disk persistence, contract boundaries, and CLI surfaces
- Interoperability tests: Rust/C++/CASA read/write matrices when on-disk bytes or metadata matter
- Packaging/runtime tests: editable Python package checks, wheel-install smoke checks, installer validation, demos, and suite layout checks
- Performance tests: smoke benchmarks and parity timing when performance is part of the requirement
- Native GUI interaction tests: application-hosted XCTest/XCUIAutomation flows
  that launch `casars-mac`, address controls through stable accessibility
  identifiers, and verify critical user workflows against deterministic fixture
  adapters

## Required discipline

- Every wave defines falsifiable acceptance checks and ships verification evidence.
- Bug fixes need regression tests.
- Cross-crate and boundary changes need integration or contract coverage.
- On-disk metadata or byte changes need the applicable 2x2 interop matrix: RR, RC, CR, CC.
- Binary serialization changes need endian coverage.
- Measures-data dependent tests must skip cleanly when runtime tables are unavailable.
- C++ dependent tests must skip cleanly when `pkg-config casacore` is unavailable.
- Tests and scripts that use shared CASA C++ datasets must use the shared
  resolver policy: default fixtures may search `CASA_RS_TESTDATA_ROOT`,
  `../casatestdata`, and `~/SoftwareProjects/casatestdata`; long gates may also
  consider the shared `/Volumes/home/casatestdata` mirror after preflight. When
  a long gate declares required dataset paths, preflight should select the first
  available root that contains those paths instead of stopping at an incomplete
  earlier mirror.
- Tutorial parity datasets are registry-backed and staged separately under
  `CASA_RS_TUTORIAL_DATA_ROOT/tutorial-parity/...` or
  `~/SoftwareProjects/casa-tutorial-data/tutorial-parity/...`; default gates
  must not select them implicitly.
- Heavy parity suites stay behind explicit opt-in gates such as `scripts/test-slow.sh`.
- Release-only Cargo integration suites should stay out of the default compile path via explicit `[[test]]` entries and `required-features`, not only file-local `cfg` guards.

## Mocking policy

- Prefer real fixtures and integration tests over internal mocks at crate boundaries.
- Do not mock workspace crates unless the alternative would make the test non-deterministic or prohibitively expensive.
- Mock third-party or process I/O only when the behavior under test is not the external tool itself.

## Default commands

- Fast local gate: `just quick`
- Full default wave gate: `just verify`
- Smoke/release gate: `just smoke`
- Blocking C++ interop release gate: `just release-cpp-interop`
- Informational release performance suite: `just release-perf`
- Release-only install gate: `scripts/test-install-suite.sh`
- Heavy parity suites: `scripts/test-slow.sh`
- Release/tag-only CI-like coverage: `scripts/run-coverage.sh --ci-like`
- GitHub Actions reproduction: `scripts/ci-local.sh pr` for pull-request jobs or `scripts/ci-local.sh tag` for version-tag jobs
- GitHub PR CI: lint/test plus editable Python package checks
- GitHub tag CI: PR CI plus smoke, suite-install, and CI-like coverage

## Evidence source and turnaround

At the current project stage, prefer fast feedback over duplicate
cross-environment verification. A required gate needs one current green result
from either a local run or a hosted run of the same command or documented
equivalent coverage; it does not need both. Once one accepted environment is
green, do not wait for or repair the other solely to duplicate the result.
Use the second environment only to diagnose a failure, resolve genuine
platform-specific uncertainty, or satisfy an explicit user request. A narrower
hosted job cannot stand in for a broader required gate such as `just verify`.
- Native macOS GUI prototype and frontend services:
  `cargo test -p casa-notebook --test wave1_contract`,
  `cargo test -p casars-frontend-services`,
  `cargo test -p casars-imager dirty_imaging_json_request_accepts_gui_selection_fields`,
  `scripts/test-frontend-services-python.sh`, `swift test` from
  `apps/casars-mac`,
  `swift run casars-mac --dump-debug-state --show-prototype python
  --prototype-state happy-path` for the fixture-only Wave 2 Python projection,
  and
  `CASARS_IMAGER_BIN=target/debug/casars-imager swift run casars-mac
  --dump-debug-state --simulate-main-flow --open-project <fixture-or-project>`
  for the headless debug-state smoke path that includes the dirty-imaging task
  run.
- Native macOS launched-app interaction gate: `just gui-test` from the
  repository root. Its disposable DerivedData and retained `.xcresult` bundle
  live under `apps/casars-mac/.gui-test/`.

Local native GUI automation owns an exclusive foreground window. `just
gui-test` performs all compilation first, announces a countdown, then runs the
complete XCUITest suite in one `test-without-building` batch. Do not use the
Mac during that window. During implementation, prefer core Swift tests,
debug-state assertions, and deterministic capture; batch user-visible
interaction changes and run the GUI suite at coherent prototype-handoff and
pre-Review checkpoints. Use isolated focused XCUITest runs only to diagnose a
failure found by a consolidated run, not as the normal edit loop.

## Coverage / confidence policy

- CI enforces 75% line coverage.
- Local goal is at least 78% to preserve a safety margin.
- Explain coverage regressions rather than hiding them.
- Do not trade meaningful behavioral tests for raw numeric coverage.
- Do not run `scripts/run-coverage.sh --ci-like` for routine branch merges unless the user explicitly asks for release/tag-level validation or direct reproduction of the heavy coverage gate.

## Native macOS executable GUI testing

XCTest/XCUIAutomation is the canonical end-to-end test layer for user-visible
`casars-mac` behavior. Issue #368 establishes a thin Xcode app host around the
existing Swift package sources, a macOS UI Testing Bundle, and the stable
`just gui-test` command. This gate must be implemented and green before Wave 1
production adapters are connected, lands with the completed Wave 1 PR, and
remains required for later user-visible GUI waves.

The executable GUI layer follows these rules:

- Launch deterministic fixture states with `XCUIApplication.launchArguments`.
  Production-boundary persistence tests may create and remove a unique
  test-owned temporary project; never open user projects, contact providers or
  networks, run scientific tasks, or leave project/notebook data behind.
- Select normal controls through stable accessibility identifiers. Coordinate-
  only automation is not an acceptable default path.
- Cover the smallest critical set of complete user workflows: editing and
  focus, view-mode changes, disclosure/gesture routing, tab navigation,
  conflict decisions, cancellation/retry, and isolation from production routes.
- Attach screenshots and useful accessibility diagnostics on failure, but do
  not use screenshot review as the only assertion that an interaction works.
- Keep Core/store tests as the broad, fast base of the pyramid. UI tests prove
  only behavior that requires the launched application boundary.
- Keep the same `just gui-test` command available locally and in the supported
  macOS CI job. One green consolidated run in either environment satisfies the
  interaction gate; the other is optional unless needed for diagnosis or
  platform-specific evidence. If neither environment can support UI automation,
  stop and record the blocker rather than replacing the gate with manual or
  computer-use testing.
- Pin the CI job's Xcode selection explicitly; the current gate uses the
  `macos-15` image with Xcode 26.2, matching the locally established compiler.
- Run locally from a logged-in GUI session with Xcode automation permission and
  no active system-authentication prompt. Diagnose failures from
  `apps/casars-mac/.gui-test/CasarsMacUITests.xcresult`, which retains the
  screenshot and accessibility hierarchy attached by the failing workflow.
- Accessibility-audit exclusions must identify a specific framework/OS
  artifact or a verified false positive. They may not blanket-exclude an audit
  category or newly introduced actionable controls.

## Wave expectations

For each wave:

- scientific-notebook waves first pass the ADR-0007 prototype gate: the live
  fixture-backed app covers primary, failure, cancellation, retry, and restart
  states; meaningful controls have accessibility IDs; debug JSON and visual
  evidence are recorded; and explicit interaction approval precedes real
  adapter integration
- user-visible native macOS GUI changes identify critical XCUITest workflows
  during shaping and record a green local or hosted `just gui-test` result
  before Review; for
  Wave 1, #368 must be implemented and green before Phase B begins and lands
  with the completed wave
- after prototype approval, real adapters must match the accepted interaction
  and state contract; deterministic fixture adapters remain available for
  regression tests and may not be treated as evidence that persistence,
  execution, download, Python, retrieval, or model integration works
- Wave 1 notebook contract tests use disposable absolute project roots and
  cover byte-preserving Markdown/future cells, atomic default/named saves,
  explicit external-edit reconciliation, concurrent immutable revisions,
  active-attempt leases and interrupted recovery, replay drift, export
  boundaries, GUI/TUI/CLI/Python recording and one-run bypasses, production
  debug state, authored task cells without receipts, clean/dirty task-tab
  parameter replacement, and launched-app persistence/task-tab interaction
- Wave 2 Phase A XCUITests cover plot regeneration and insertion, latest-first
  execution status with routine streams, paths, failures, and prior revisions
  collapsed by default, explicit producer-declared plot/image aspect behavior,
  explorer snapshot
  enlargement and parameter restoration, no live notebook mutation, New plot
  and immutable Update actions, observable running state, ordered failure
  output and retry, a nonresponsive cell's interrupt/restart path, exact-source
  AI approval invalidation after editing, accessibility audit, and zero
  production-boundary calls. Core tests cover the same fixture state
  transitions and exact source digests. These tests do not claim that Python,
  project persistence, matplotlib, or production explorer integration exists.
- Wave 2 Phase B adds production evidence for exact-source receipt-v2 Python
  execution, compact-by-default ordered output and immutable artifacts
  surviving a fresh notebook
  load, persistent namespace plus interrupt/restart recovery, independent
  Seatbelt AI-worker denial of network/outside writes/symlink escape/credential
  inheritance, tutorial-backed native MeasurementSet and image-plane data with
  editable Matplotlib/WCSAxes objects, and Rust-owned explorer snapshot New,
  Update, immutable history, and exact reopen intent. Launched-app coverage
  executes a production Python cell from a disposable project and verifies its
  on-disk receipt and reloaded output; the fixture XCUITests remain the
  deterministic interaction evidence for explorer New/Update/Open behavior.
- acceptance checks have direct verification evidence
- changed behavior has matching tests or explicit justified exclusions
- medium/high-risk work gets architecture review and test-adversary review
- code waves record a bounded `refactor` pass on the involved code before
  review, or a not-applicable rationale for no-code waves
- reality-sync happens when docs, interfaces, or boundaries changed
- approved outcome, included issues, and acceptance checks are not deferred or
  descoped without explicit user signoff recorded in the issue or PR
- release work also runs the smoke gate, the blocking C++ interop gate, and the suite-install gate; CI-like coverage remains a version-tag CI gate and is run locally only for `scripts/release.sh --full` or explicit coverage reproduction
- ordinary non-release merges stay on `just verify` plus targeted tests unless the user explicitly asks to exercise release/tag-only heavy gates
- release performance evidence is informational by default and becomes blocking only when `CASA_RS_ENFORCE_PERF=1`
- slow CASA parity checks run when the wave touches those concerns

## Done gate

A wave is not done until:

- `just verify` passes or any intentional exclusion is called out explicitly
- tests cover the claimed behavior
- native macOS GUI waves pass `just gui-test` locally or in hosted CI for
  changed critical interactions
  once the #368 test target is present
- code-wave refactor evidence is recorded, or a no-code not-applicable
  rationale exists
- reviewers checked for shallow or tautological tests on medium/high-risk work
- docs or ADRs were updated if reality changed
- any approved-scope deferral records explicit user signoff
