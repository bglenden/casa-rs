# ADR-0004: Tiered verification and heavy parity gates

Status: accepted
Date: 2026-04-19
Truth class: normative
Supersedes:
Superseded by:

## Context

The repo has a broad workspace test surface, editable and wheel-based Python
package checks, demos, Rust/C++ interop suites, performance probes, coverage
collection, and heavy CASA parity suites. Running every slow interop, parity,
packaging, and performance workload in the default loop would make daily
iteration too slow, but pretending those suites do not matter would weaken
interoperability and release confidence.

## Decision

The repo uses tiered verification:

- `just quick` is the fast local iteration gate
- `just verify` is the default full wave gate for workspace tests and editable Python package verification
- `just smoke` is the named smoke/release gate for Python wheel-install checks and Rust demo verification
- Python-facing scripts resolve Python `>=3.10` through a repo-level resolver instead of assuming the shell's `python3`
- `scripts/test-release-cpp-interop.sh` is the blocking release gate for Rust/C++ interop suites and remains outside the default `cargo test --workspace` path
- `scripts/test-release-perf.sh` is informational release evidence by default; perf thresholds only become blocking when `CASA_RS_ENFORCE_PERF=1`
- `scripts/test-install-suite.sh` is a release-oriented heavy gate kept outside `just verify` and run by the default local release path
- `scripts/run-coverage.sh --ci-like` is a release/tag-oriented heavy gate kept outside `just verify` and the default local release path; it runs in version-tag CI and by `scripts/release.sh --full`
- GitHub pull requests run the lighter CI subset, while version-tag pushes run the release-oriented CI gates
- `scripts/ci-local.sh` mirrors that split with explicit `pr` and `tag` reproduction commands instead of folding smoke or coverage work back into the default PR path
- `scripts/test-slow.sh` is the explicit opt-in gate for heavy CASA parity suites
- `scripts/run-coverage.sh --ci-like` remains the canonical coverage command because it matches GitHub Actions behavior more closely than ad hoc local runs

Heavy interop, parity, performance, and release-only checks remain important,
but they do not live in the default `cargo test --workspace` path.

## Consequences

Positive:
- the common local loop stays productive
- heavy interop/parity suites still have a clear home and remain part of release confidence
- the Python package gate stops depending on which shell startup files happened to run
- wheel-install and demo smoke checks keep a named home without slowing normal wave closeout
- commands used by humans, agents, CI, and release scripts become more consistent

Negative:
- contributors must understand which gate is appropriate for a given wave
- some correctness or performance evidence still requires explicit extra commands
- GitHub tag CI does not fully replace local release-time interop evidence on machines that have the required C++ stack installed

Neutral / tradeoffs:
- `just verify` is intentionally heavier than `just quick`, but lighter than named smoke/release packaging and coverage work

## Alternatives considered

1. Put all slow interop and parity suites into the default workspace test path.
2. Treat slow parity, interop, and coverage work as optional ad hoc work with no named gate.

## Enforcement

This decision is enforced by:
- tests: default and slow suites stay separated by command surface
- lint/import/dependency rules: `justfile` and release/CI scripts define the named gate boundaries
- CI checks: GitHub pull requests run the lighter subset; version tags additionally run `scripts/test-smoke.sh`, `scripts/test-install-suite.sh`, and `scripts/run-coverage.sh --ci-like`
- review trigger: stop for review when moving heavy interop/parity work into the default loop or weakening named gates
- none / guidance only:

## Drift detection

Suspect drift if:
- C++ interop or slow parity suites re-enter the default `cargo test --workspace` path
- wheel-install or demo smoke checks re-enter `just verify` without an explicit decision
- release-only install or coverage gates re-enter `just verify` without an explicit decision
- Python package checks start depending on ambient shell `python3` resolution again
- contributors start using ad hoc one-off command bundles instead of the named repo gates
- coverage expectations diverge from the CI-like coverage script
