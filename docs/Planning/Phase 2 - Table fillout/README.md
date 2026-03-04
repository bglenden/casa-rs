# Phase 2 Implementation Rules

**Required first step:** read `/AGENTS.md`. It is mandatory and overrides this
file when rules conflict.

## Purpose

Close remaining Tables interoperability gaps from `BACKLOG.md` using small,
auditable waves.

## Status board

| Wave | Backlog | Topic | Status | File |
|---|---|---|---|---|
| 1 | 1.1 | Variable-shape arrays (AipsIO + SSM) | TAKEN | [Wave 1](Wave 1 - Variable-shape arrays in StManAipsIO and StandardStMan - TAKEN.md) |

Update this table on create/take/block/finish.

## Wave lifecycle

1. Choose one backlog item (or one tightly coupled pair).
2. Create `Wave N - <name>.md` from `WAVE_TEMPLATE.md`.
3. Copy detail from `BACKLOG.md`, then remove moved item(s) from backlog.
4. Pass the wave's Definition of Ready checklist before coding.
5. On implementation start, append ` - TAKEN` to the filename.
6. At closeout, fill `Results` and `Lessons learned`, then append ` - FINISHED`.

## Wave sizing rules

- Target work that touches 1-3 source files plus tests/support.
- Split waves that mix unrelated features.
- Keep explicit `Non-goals` in every wave file.
- Prefer simple Rust APIs that preserve all C++ capabilities.

## Required tests per wave

- 2x2 interop matrix where applicable: RR, RC, CR, CC.
- Endian matrix when on-disk bytes/serialization are touched.
- Edge cases: undefined cells, empty values, and boundary shapes/sizes.
- C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.

## Performance rule

- If storage or serialization changes, run a release-mode smoke benchmark.
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
- Skips, blockers, and follow-up items.
