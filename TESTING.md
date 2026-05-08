# Testing Strategy

Truth class: normative
Last reality check: 2026-05-04
Verification: just verify

## Test categories

- Unit tests: pure value, parsing, math, and domain logic inside a single crate
- Integration tests: cross-crate behavior, on-disk persistence, contract boundaries, and CLI surfaces
- Interoperability tests: Rust/C++/CASA read/write matrices when on-disk bytes or metadata matter
- Packaging/runtime tests: editable Python package checks, wheel-install smoke checks, installer validation, demos, and suite layout checks
- Performance tests: smoke benchmarks and parity timing when performance is part of the requirement

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
- Native macOS GUI prototype and frontend services:
  `cargo test -p casars-frontend-services`,
  `cargo test -p casars-imager dirty_imaging_json_request_accepts_gui_selection_fields`,
  `scripts/test-frontend-services-python.sh`, `swift test` from
  `apps/casars-mac`, and
  `CASARS_IMAGER_BIN=target/debug/casars-imager swift run casars-mac
  --dump-debug-state --simulate-main-flow --open-project <fixture-or-project>`
  for the headless debug-state smoke path that includes the dirty-imaging task
  run.

## Coverage / confidence policy

- CI enforces 75% line coverage.
- Local goal is at least 78% to preserve a safety margin.
- Explain coverage regressions rather than hiding them.
- Do not trade meaningful behavioral tests for raw numeric coverage.
- Do not run `scripts/run-coverage.sh --ci-like` for routine branch merges unless the user explicitly asks for release/tag-level validation or direct reproduction of the heavy coverage gate.

## Wave expectations

For each wave:

- acceptance checks have direct verification evidence
- changed behavior has matching tests or explicit justified exclusions
- medium/high-risk work gets architecture review and test-adversary review
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
- reviewers checked for shallow or tautological tests on medium/high-risk work
- docs or ADRs were updated if reality changed
- any approved-scope deferral records explicit user signoff
