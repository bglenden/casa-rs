# ADR-0002: Native Rust implementation with casacore-compatible persistence

Status: accepted
Date: 2026-04-19
Truth class: normative
Supersedes:
Superseded by:

## Context

The project exists to interoperate with casacore-compatible persistent data and
workflows. The repo could either wrap casacore C++ directly or implement the
behavior natively in Rust while validating against casacore/CASA truth
oracles. The repo already favors native Rust crates and Rust ecosystem
dependencies, but that preference needs to be explicit because it affects API
shape, testing strategy, and how new functionality is added.

## Decision

`casa-rs` is a native Rust implementation of casacore-compatible persistence
and related workflows. The project does not mirror the C++ APIs mechanically
and is not a Rust wrapper around casacore C++.

New functionality should:

- prefer idiomatic Rust APIs
- preserve on-disk interoperability with casacore-compatible data where the repo claims support
- use casacore/CASA behavior as the comparison oracle for interop, parity, and documentation when relevant
- use established Rust crates for commodity infrastructure instead of rebuilding them locally

## Consequences

Positive:
- Rust users get APIs shaped for Rust rather than C++ compatibility wrappers
- the codebase can use ecosystem crates such as `ndarray` without apologizing for it
- interop can be validated by behavior and on-disk artifacts rather than FFI surface parity

Negative:
- some differences from C++ names and object shapes require more documentation for cross-referencing
- parity work must be proven with tests and fixtures rather than assuming FFI makes behavior identical

Neutral / tradeoffs:
- the repo still depends on local casacore/CASA tooling for some parity tests even though the implementation is native Rust

## Alternatives considered

1. Wrap casacore C++ directly and expose a thin Rust veneer.
2. Reproduce C++ APIs as closely as possible even when they are awkward in Rust.

## Enforcement

This decision is enforced by:
- tests: on-disk interoperability, parity, demo, and regression tests
- lint/import/dependency rules: review blocks commodity reinvention and wrapper-oriented architecture changes
- CI checks: `just verify`; heavier parity remains opt-in when needed
- review trigger: stop for review when a change proposes new raw embedded measures assets, wrapper-first APIs, or incompatible persistence behavior
- none / guidance only:

## Drift detection

Suspect drift if:
- new public APIs look like direct C++ mirror layers without Rust-specific value
- on-disk compatibility claims are made without interop evidence
- new code introduces wrapper-oriented architecture instead of native library behavior
