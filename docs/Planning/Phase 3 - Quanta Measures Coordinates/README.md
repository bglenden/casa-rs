# Phase 3 Implementation Rules

Truth class: historical
Last reality check: 2026-04-19
Verification: none

This file is retained as historical process context for archived Phase 3 wave
work. Canonical active planning and wave status now live in GitHub Issues /
Project.

**Required first step:** read `/AGENTS.md`. When this file conflicts with
current repo policy, `AGENTS.md` wins.

## Purpose

Deliver casacore-compatible units/quanta, measures, table measure metadata, and
coordinate core primitives needed by table and TaQL workflows.

## Historical status model

While Phase 3 was active, wave file names were used as the local status model:
- `Wave N - <name>.md` => planned
- `Wave N - <name> - TAKEN.md` => in progress
- `Wave N - <name> - FINISHED.md` => closed

## Historical wave lifecycle

1. Choose one GitHub issue (or one tightly coupled issue pair).
2. Create `Wave N - <name>.md` from `WAVE_TEMPLATE.md`.
3. Copy the issue scope into the wave file and link the issue number/title.
4. Pass the wave's Definition of Ready checklist before coding.
5. On implementation start, append ` - TAKEN` to the filename.
6. At closeout, fill `Results` and `Lessons learned`, then append ` - FINISHED`.

## Wave sizing rules

- Target work that touches 2-6 source files plus tests/support.
- Split waves that mix unrelated concepts (quanta vs measures vs coordinates).
- Keep explicit `Non-goals` in every wave file.
- Prefer simple Rust APIs that preserve all C++ capabilities.

## Required tests per wave

- 2x2 interop matrix where on-disk metadata/bytes are touched: RR, RC, CR, CC.
- Endian matrix when binary serialization is touched.
- Edge cases: undefined values, empty arrays, variable refs/offsets/units.
- C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.
- Data-dependent measure tests must skip cleanly when measures data tables are
  unavailable.

## Performance rule

- If conversion engines or serialization paths change, run a release-mode smoke
  benchmark.
- Flag and document follow-up when Rust is slower than 2x C++.

## Closeout gates (canonical)

Run all before `-FINISHED`:

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`
- `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`

## Required evidence in wave `Results`

- Date and commit SHA.
- Commands run with pass/fail summary.
- Interop matrix outcomes.
- Benchmark numbers + Rust/C++ ratio (if performance applies).
- Data-table availability/skips, blockers, and follow-up items.
