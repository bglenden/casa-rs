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
- Use `casa-*` names for reusable libraries and `casars-*` names for app/runtime
  crates. The repo is a native Rust implementation of casacore-compatible data,
  not a Rust wrapper around casacore C++.
- API docs belong in source (`///`, `//!`) and are rendered by `cargo doc`.
- For `casars` TUI work, follow `docs/casars-tui-framework.md`; new apps must
  conform to its shell-family conventions instead of inventing app-local UI
  structure.
- Prefer red/green development: add failing regression test first.
- C++-dependent tests must skip cleanly when `pkg-config casacore` is missing.
- Shared CASA dataset root defaults to `../casatestdata`.
- Override the shared dataset root with `CASA_RS_TESTDATA_ROOT`.
- Canonical measures runtime data lives in a CASA-compatible table tree rooted
  at `~/.casa/data`.
- Override the measures runtime root with `CASA_RS_MEASURESPATH`.
- `CASA_RS_DATA` is a deprecated compatibility alias; do not introduce new
  code or docs that prefer it over `CASA_RS_MEASURESPATH`.
- Reuse upstream CASA/casaconfig tables and bundle layouts for measures data;
  do not introduce new embedded/raw measures assets when the CASA-table runtime
  model can be used instead.
- Do not treat `/private/tmp` as the canonical home for shared CASA datasets.
- Small bundled real-MS CI fixtures live in `crates/casa-ms/tests/fixtures/`.
- If C++ has a demo for a supported module, provide a Rust equivalent demo.
- When implementing new casacore-c++ functionality, document all public types and
  methods at a level comparable to the C++ doxygen in the corresponding .h files.
  Use `///` for items and `//!` for modules. Reference the C++ class/function names
  so users can cross-reference.

## Backlog Tracking

- Canonical backlog tracking lives in GitHub issues, not local `BACKLOG.md` files.
- Use GitHub issue titles and bodies as the source of truth for deferred work.
- Legacy phase backlog files may remain temporarily in areas that have not been migrated yet; do not add new items to them.
- Do not introduce new backlog-style `TODO` / `FIXME` / `XXX` / `HACK` comments as the source of truth.
- If a local code comment is still useful, keep it brief and reference the GitHub issue instead.

## Merge Policy

- Squash-on-merge into main: one commit per wave/PR.
- During development, commit freely (checkpoints, incremental progress).
- Use `git merge --squash` locally or "Squash and merge" on GitHub PRs.

## Quality Gates

- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`
- `scripts/test-slow.sh`
- `scripts/run-coverage.sh --ci-like`
- Evaluate coverage with `scripts/run-coverage.sh --ci-like`, because plain
  local coverage can drift from what GitHub Actions measures.
- Coverage changes should maintain a safety margin of at least 3 percentage
  points above the enforced CI threshold; with the current 75% requirement,
  target at least 78% rather than barely passing.
- Small performance guards stay in the default `cargo test --workspace` path.
- Long parity/coverage/perf work should run less often, usually via CI,
  daily automation, or explicit full runs.
- Heavy CASA parity suites must not stay in the default `cargo test --workspace`
  path. Gate them behind an explicit opt-in like the `slow-tests` feature and
  run them via `scripts/test-slow.sh`.
- To reproduce the GitHub Actions environment locally, use
  `scripts/ci-local.sh build` and then `scripts/ci-local.sh lint_test`,
  `scripts/ci-local.sh coverage`, or `scripts/ci-local.sh all`.

## Releases

- Use `scripts/release.sh <version>` for every release tag; do not create release
  tags directly with `git tag`.
- Common bumps:
  `scripts/release.sh --patch` and `scripts/release.sh --minor`.
- The default release script runs the fast local gates:
  `fmt`, `clippy`, and `cargo test --workspace`.
- Use `scripts/release.sh <version> --full` to additionally run
  `scripts/test-slow.sh` and CI-like coverage.
- Use `scripts/release.sh <version> --push` to push the release commit and tag.
- If measures/runtime changes land near a release, refresh or verify the
  packaged CASA-table snapshot in `crates/casa-measures-data/data/` and keep
  `casa-measures-runtime.provenance.json` aligned with it.
- When asking for a release, say something like `use the release script to cut
  and push release 0.3.1` or `use the release script to cut the next patch
  release` rather than `tag as 0.3.1`.
- To mirror the full suite locally, run:
  `cargo fmt --all -- --check`
  `cargo clippy --workspace --all-targets -- -D warnings`
  `cargo test --workspace`
  `scripts/test-slow.sh`
  `scripts/run-coverage.sh --ci-like`
  `cargo run -p casa-aipsio --example t_aipsio`
  `cargo run -p casa-tables --example t_table`

## Architecture Decisions (Condensed)

- Public API surface: `casa-types` and `casa-tables`.
- Internal implementation crates:
  - `casa-values` (`publish = false`)
  - `casa-aipsio` (`publish = false`)
  - `casa-table-read` (`publish = false`)
  - `casa-test-support` (`publish = false`)
- `casa-values` owns the generic scalar/array/record model below measures/quanta.
- `casa-table-read` owns the minimal read-only CASA table loader shared by runtime data loaders.
- `casa-tables` keeps the broader codec/storage/write path crate-internal.
- `casa-aipsio` provides full AipsIO-style framing + `tAipsIO` parity demo/tests.
