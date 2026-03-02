# AGENTS.md

Developer notes for this repo. Keep this file short.

## Goal

Implement Rust APIs that can read/write casacore-compatible persistent data.
Use modern Rust crates where appropriate, but keep on-disk interoperability.

## References

- C++ source: `~/SoftwareProjects/casacore`
- Upstream: `https://github.com/casacore/casacore`
- Local C++ headers/libs installed via Homebrew.

## Rules

- Prefer idiomatic Rust APIs over direct C++ API mirroring.
- Do not reimplement commodity infrastructure (for example use `ndarray`).
- API docs belong in source (`///`, `//!`) and are rendered by `cargo doc`.
- Prefer red/green development: add failing regression test first.
- C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.
- If C++ has a demo for a supported module, provide a Rust equivalent demo.

## Quality Gates

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`

## Architecture Decisions (Condensed)

- Public API surface: `casacore-types` and `casacore-tables`.
- Internal implementation crates:
  - `casacore-aipsio` (`publish = false`)
  - `casacore-test-support` (`publish = false`)
- `casacore-tables` keeps codec/storage helpers crate-internal.
- `casacore-aipsio` provides full AipsIO-style framing + `tAipsIO` parity demo/tests.
