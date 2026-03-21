# AGENTS.md

Developer notes for this repo. Keep this file short.

## Goal

Implement Rust APIs that can read/write casacore-compatible persistent data.
Use modern Rust crates where appropriate, but keep on-disk interoperability.

## References

- C++ source: `~/SoftwareProjects/casacore`
- Upstream: `https://github.com/casacore/casacore`
- Local C++ headers/libs installed via Homebrew.

## Worktrees

- Worktrees should go in ../casa-rs-worktrees
- main is in ~/SoftwareProjects/casa-rs

## Rules

- Prefer idiomatic Rust APIs over direct C++ API mirroring.
- Do not reimplement commodity infrastructure (for example use `ndarray`).
- API docs belong in source (`///`, `//!`) and are rendered by `cargo doc`.
- Prefer red/green development: add failing regression test first.
- C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.
- Shared CASA dataset root defaults to `../casatestdata`.
- Override the shared dataset root with `CASA_RS_TESTDATA_ROOT`.
- Do not treat `/private/tmp` as the canonical home for shared CASA datasets.
- If C++ has a demo for a supported module, provide a Rust equivalent demo.
- When implementing new casacore-c++ functionality, document all public types and
  methods at a level comparable to the C++ doxygen in the corresponding .h files.
  Use `///` for items and `//!` for modules. Reference the C++ class/function names
  so users can cross-reference.

## Merge Policy

- Squash-on-merge into main: one commit per wave/PR.
- During development, commit freely (checkpoints, incremental progress).
- Use `git merge --squash` locally or "Squash and merge" on GitHub PRs.

## Quality Gates

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `cargo tarpaulin --workspace --timeout 120 --out Stdout --fail-under 75`

## Releases

- Use `scripts/release.sh <version>` for every release tag; do not create release
  tags directly with `git tag`.
- Common bumps:
  `scripts/release.sh --patch` and `scripts/release.sh --minor`.
- Use `scripts/release.sh <version> --push` to push the release commit and tag.
- When asking for a release, say something like `use the release script to cut
  and push release 0.3.1` or `use the release script to cut the next patch
  release` rather than `tag as 0.3.1`.

## Architecture Decisions (Condensed)

- Public API surface: `casacore-types` and `casacore-tables`.
- Internal implementation crates:
  - `casacore-aipsio` (`publish = false`)
  - `casacore-test-support` (`publish = false`)
- `casacore-tables` keeps codec/storage helpers crate-internal.
- `casacore-aipsio` provides full AipsIO-style framing + `tAipsIO` parity demo/tests.
