# Architecture

Truth class: current descriptive
Last reality check: 2026-04-19
Verification: just arch-check

## System purpose

`casa-rs` implements native Rust libraries and applications that read, write,
and manipulate casacore-compatible tables, MeasurementSets, images,
coordinates, measures, and related workflows.

## Major modules / crates / packages

| Module | Responsibility | May depend on |
|---|---|---|
| core codecs (`casa-values`, `casa-aipsio`) | Internal generic value model and AipsIO-style framing used by higher layers | Rust ecosystem crates only |
| foundation crates (`casa-types`, `casa-table-read`, `casa-measures-data`, `casa-measures-tools`) | Public scalar/quanta/measures types, minimal read-only table loading, and runtime measures data access | core codecs |
| persistent storage (`casa-tables`) | CASA table persistence, codecs, data managers/storage backends, schema/mutation APIs, and TaQL engine | core codecs, foundation crates |
| domain libraries (`casa-ms`, `casa-lattices`, `casa-coordinates`, `casa-images`, `casa-imaging`, `casa-calibration`, `casa-vla`) | Higher-level astronomy data models and algorithms built on table/image persistence | foundation crates, `casa-tables`, selected peer domain crates where documented |
| boundary contracts (`casa-provider-contracts`, `casars-imagebrowser-protocol`, `casars-tablebrowser-protocol`) | Versioned schema bundles and protocol surfaces between providers, apps, and Python/runtime layers | domain libraries and foundation crates; must not become a second source of truth |
| apps and runtimes (`casars`, `casars-imager`, `casars-importvla`, `casars-python`, `ratatui-graphics`) | Terminal shells, orchestration binaries, Python bindings/package, and rendering/runtime support | boundary contracts, domain libraries, foundation crates |
| test support (`casa-test-support`) | Cross-language parity harnesses, fixtures, integration helpers, and performance guards | any workspace crates needed for testing only |

## Dependency direction

Preferred direction is:

`core codecs -> foundations -> persistent storage / domain libraries -> boundary contracts -> apps/runtimes`

with `casa-test-support` outside the product dependency chain.

Additional constraints:

- `casa-values` and `casa-aipsio` stay internal implementation crates.
- `casa-table-read` owns the minimal shared read-only loader used by runtime data loaders.
- `casa-tables` keeps the broader storage/write path crate-internal even when user-facing table APIs are exposed from the crate.
- Within `casa-tables`, lazy read paths are safe to share across threads under an in-process multi-reader, single-writer contract; shared tiled reads use a process-wide bounded cache, while dirty write state stays under exclusive mutable ownership.
- Within `casa-tables`, row/column/cell accessor objects are the public table-data surface; prepared-row accessors provide the reusable selected-column row fast path, and the old table-level convenience wrappers have been removed from the public API.
- Versioned provider bundles are boundary contracts; UI projections are derived views, not separate truth sources.

## Runtime model

Most crates are synchronous Rust libraries with CLI/TUI frontends and test
harnesses on top. There is no repo-wide async runtime contract today.
Long-running interoperability, parity, and packaging work is driven by
shell/Python scripts, integration tests, or subprocess orchestration rather
than a shared background service model.

## Persistence / external systems

- casacore-compatible table trees and image tables on local disk
- MeasurementSet and CASA image fixtures under the shared dataset root (`../casatestdata` by default, override `CASA_RS_TESTDATA_ROOT`)
- measures runtime data in a CASA-compatible table tree rooted at `~/.casa/data` (override `CASA_RS_MEASURESPATH`)
- local casacore C++ installations via Homebrew for parity tests and demos when available
- GitHub Actions as the canonical CI environment, with `scripts/ci-local.sh` as local reproduction support

## Public interfaces

- published Rust library crates, especially `casa-types`, `casa-tables`, `casa-ms`, `casa-lattices`, `casa-coordinates`, and `casa-images`
- CLI/TUI apps such as `casars`, `msexplore`, `tablebrowser`, `imexplore`, `calibrate`, and `casars-importvla`
- Python package `casars-python`
- persisted CASA-compatible on-disk table, image, and related data formats
- versioned provider contract bundles and protocol schemas

## Approved dependency classes

- N-dimensional arrays and numeric containers: `ndarray`
- FFT and spectral transforms: `rustfft`
- error types: `thiserror`
- terminal rendering and TUI support: `ratatui`, `ratatui-graphics`, `plotters`
- Adding a second library in the same category requires review.

## Known constraints

- On-disk interoperability with casacore-compatible formats is more important than mirroring C++ APIs directly.
- Heavy CASA parity suites must stay opt-in rather than in the default `cargo test --workspace` path.
- Some cross-language and parity tests must skip cleanly when `pkg-config casacore` or measures data are unavailable.
- The methodology/process surface is newly installed; some architecture checks are still lightweight/document-oriented rather than full mechanical boundary enforcement.

## Known current gaps / debt

- GitHub Project/issue adoption is now the planning source of truth, but older `docs/Planning/` material still exists and may need incremental retirement or summarization.
- `just` provides a stable command vocabulary, but some contributors may still use the underlying `cargo` and `scripts/*` commands directly until it is installed locally.
- Boundary enforcement is partly manual today; `just arch-check` currently validates the documented surface and ADR index rather than all crate dependency rules mechanically.

## ADR index

| ADR | Title | Status |
|---|---|---|
| 0001 | Public surface and workspace layering | accepted |
| 0002 | Native Rust implementation with casacore-compatible persistence | accepted |
| 0003 | Provider schema bundle as boundary contract | accepted |
| 0004 | Tiered verification and heavy parity gates | accepted |
