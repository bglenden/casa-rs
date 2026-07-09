# Architecture

Truth class: current descriptive
Last reality check: 2026-07-09
Verification: just docs-check

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
| boundary contracts (`casa-provider-contracts`, `casars-imagebrowser-protocol`, `casars-tablebrowser-protocol`) | Versioned provider bundles, parameter catalogs, task/session surface definitions, and protocol surfaces between providers, apps, and Python/runtime layers | domain libraries and foundation crates; must not become a second source of truth |
| parameter runtime (`casa-task-runtime`) | Format-neutral parameter resolution, sparse TOML profiles, migrations, typed parameter sessions, and managed Last storage | boundary contracts and `casa-types`; must not implement provider science behavior |
| apps and runtimes (`casars`, `casars-imager`, `casars-importvla`, `casars-python`, `casars-frontend-services`, `ratatui-graphics`, `apps/casars-mac`) | Terminal shells, orchestration binaries, Python bindings/package, frontend service bindings, rendering/runtime support, and the native macOS GUI prototype | boundary contracts, domain libraries, foundation crates; lightweight frontend services may expose read-only domain-library probes through UniFFI |
| test support (`casa-test-support`) | Cross-language parity harnesses, fixtures, integration helpers, and performance guards | any workspace crates needed for testing only |

## Dependency direction

Preferred direction is:

`core codecs -> foundations -> persistent storage / domain libraries -> boundary contracts -> parameter runtime -> apps/runtimes`

with `casa-test-support` outside the product dependency chain.

Additional constraints:

- `casa-values` and `casa-aipsio` stay internal implementation crates.
- `casa-table-read` owns the minimal shared read-only loader used by runtime data loaders.
- `casa-tables` keeps the broader storage/write path crate-internal even when user-facing table APIs are exposed from the crate.
- Within `casa-tables`, lazy read paths are safe to share across threads under an in-process multi-reader, single-writer contract; shared tiled reads use a process-wide bounded cache, while dirty write state stays under exclusive mutable ownership.
- Within `casa-tables`, row/column/cell accessor objects are the public table-data surface; prepared-row accessors provide the reusable selected-column row fast path, and the old table-level convenience wrappers have been removed from the public API.
- Versioned provider bundles are boundary contracts; UI projections are derived
  views, not separate truth sources.
- Parameter concepts live in the checked aggregate `ParameterCatalog` in
  `casa-provider-contracts`. Each provider bundle embeds the exact referenced
  concepts so the boundary remains self-contained. Task and session
  `SurfaceDefinition` bindings supply defaults, conditional activation,
  narrowing refinements, migrations, presentation, and projection metadata;
  they cannot redefine concept meaning, normalization, units, role, or
  persistence. Frontends may not redefine those semantics locally.
- `casa-task-runtime` owns profile mechanics and managed state, while providers
  retain ownership of task adapters and session startup semantics. The runtime
  must not become a central implementation of domain behavior.
- `apps/casars-mac` keeps fixture schemas inside its SwiftPM core when modeling
  proposed UI behavior. Real, read-only dataset discovery enters through
  `casars-frontend-services`, whose Rust API is exposed to Swift and Python
  with UniFFI.
- `casars-frontend-services` is an apps/runtime boundary crate. It may compose
  domain-library reads into GUI-appropriate summaries, but it must not become a
  second implementation of persistence, task semantics, or provider contracts.

## Runtime model

Most crates are synchronous Rust libraries with CLI/TUI frontends and test
harnesses on top. There is no repo-wide async runtime contract today.
Long-running interoperability, parity, and packaging work is driven by
shell/Python scripts, integration tests, or subprocess orchestration rather
than a shared background service model.

ADR-0006 adds one synchronous parameter-session model shared by task and
browser-session consumers. A task resolves sparse user intent into one
provider invocation. A browser session resolves only durable startup settings;
its subsequent command/event stream remains owned by the session protocol.
Parameter resolution and Last persistence do not introduce a provider daemon
or repo-wide async runtime.

`apps/casars-mac` is a SwiftPM package for the macOS-native GUI prototype. Its
workbench state remains headlessly testable in SwiftPM. GUI-Wave-1 introduces a
small UniFFI runtime boundary through `casars-frontend-services` for read-only
project and dataset probing. GUI-Wave-3 extends that boundary with a narrow
MeasurementSet explorer plot API: Rust owns `casa-ms` / `msexplore` plot payload
construction and PNG rendering, while Swift owns native controls, layout, and
debug-state projection. GUI-Wave-4 adds the first real task-execution vertical:
the Swift workbench supervises a short-lived `casars-imager --json-run`
process for dirty imaging, records logs/results/products in processing history,
and exposes the request/run state through the debug snapshot. This remains a
narrow process-supervision path, not a shared background service, provider
daemon, or repo-wide async runtime contract. Issue #194 adds the first durable
Swift-side workbench job coordinator for independent tab work: MeasurementSet
plot rendering and dirty-imaging subprocess runs register per-tab jobs with
pending/running/succeeded/failed/cancelled state, cancellation projection, logs,
results/errors, and debug-snapshot visibility. The coordinator is intentionally
local to `apps/casars-mac`; it does not introduce a provider daemon, durable
project-history format, or repo-wide async runtime contract.
GUI-Wave-5 keeps that runtime shape and adds the native explorer spine: real
MeasurementSet, CASA image, and table probes are routed into typed Swift
explorer tabs, and dirty-imaging artifacts are grouped under their originating
in-memory run state so generated products can be reopened without adding a
project-history persistence format or background service.

### Imaging execution

`casars-imager` owns MeasurementSet selection, bounded source streaming, mode
dispatch, runtime policy, protocol telemetry, and persisted product writing.
`casa-imaging` remains the prepared-visibility computation boundary for
weighting, gridding/degridding, FFTs, normalization, deconvolution, restoration,
and product semantics.

The app uses one shared bounded producer/consumer primitive for source
read-ahead across standard MFS, mosaic MFS replay, supported mosaic MT-MFS,
standard and mosaic cube slabs, cubedata preparation, and trace preparation.
`imaging_read_ahead_blocks` is a maximum live row-block count, not a queue-depth
request. The current implementation caps it at two: one producer-owned block
and one consumer-owned block. Queue capacity is `max_live_row_blocks - 2`, so
the two-block case uses a rendezvous channel and cannot retain a third queued
block. A value of one runs synchronously. Full-slab spectral routes remain
single-block by default and accept explicit two-block read-ahead only when the
memory planner does not lose plane residency or row locality.

The first bounded mosaic MT-MFS slice supports one MeasurementSet,
`specmode='mfs'`, `gridder='mosaic'`, `nterms <= 2`, no W term, natural,
uniform, or Briggs weighting, user masks, clean or dirty products, and optional
PB/PB-corrected products. Each weighting, initial-dirty, and residual-refresh
pass replays the same bounded row stream; Briggs density uses a raw-UVW sidecar
so CASA's density cell conventions remain independent of mosaic projection
coordinates. Broader W/AW, pointing, start-model, outlier, multi-MS, and
higher-term combinations still reject during planning.

Imager task protocol v3 carries the local execution controls (`parallel`,
`chanchunks`, shared source memory/row-block/worker/read-ahead settings, and
dirty-product FFT precision/backend policy). Diagnostic progress events expose
planned and measured memory, source bytes and read bandwidth, read/prepare
overlap, producer/consumer blocking, live-block high water, worker/queue state,
stage timings, and backend selection or fallback reasons. The task protocol is
v3, the newline-delimited progress event schema is v1, and the embedded
observability snapshot schema is v2. `parallel=false` selects the serial CPU
comparison surface, including one live source block and RustFFT product
transforms.

On Apple platforms, eligible f32 dirty/PSF/residual transforms can keep grids
resident through MPSGraph FFT, correction, normalization, and peak reduction
for standard and mosaic products. Explicit `metal-mpsgraph` requests select
that path when supported. `auto` applies a work-size/batch-shape profitability
guard and keeps small batches on the CPU; unsupported shapes, unavailable
devices, resident-command failures, and f64 product transforms use the shared
CPU product finisher. Backend and fallback decisions are reported in diagnostic
telemetry rather than changing product membership or persistence semantics.

## Persistence / external systems

- casacore-compatible table trees and image tables on local disk
- sparse user-authored parameter profiles in arbitrary user-selected locations
- managed parameter state under
  `<workspace>/.casa-rs/parameters/<surface-id>/`, optionally redirected by
  `CASA_RS_STATE_DIR`
- MeasurementSet and CASA image fixtures under the shared dataset root (`../casatestdata` by default, override `CASA_RS_TESTDATA_ROOT`)
- measures runtime data in a CASA-compatible table tree rooted at `~/.casa/data` (override `CASA_RS_MEASURESPATH`)
- local casacore C++ installations via Homebrew for parity tests and demos when available
- GitHub Actions as the canonical CI environment, with `scripts/ci-local.sh` as local reproduction support

## Public interfaces

- published Rust library crates, especially `casa-types`, `casa-tables`, `casa-ms`, `casa-lattices`, `casa-coordinates`, and `casa-images`
- CLI/TUI apps such as `casars`, `msexplore`, `tablebrowser`, `imexplore`, `calibrate`, and `casars-importvla`
- native macOS GUI prototype package `apps/casars-mac`
- experimental UniFFI frontend service bindings generated from
  `casars-frontend-services`
- Python package `casars-python`
- persisted CASA-compatible on-disk table, image, and related data formats
- versioned provider contract bundles and protocol schemas
- versioned sparse TOML task and session parameter profiles

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
- WDAD uses the scaled v0.4 layer in this repo, including the bounded
  refactor-pass gate before code waves move to review; some architecture checks
  are still lightweight/document-oriented rather than full mechanical boundary
  enforcement.

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
| 0005 | Native macOS GUI prototype boundary | accepted |
| 0006 | Unified parameter catalog and sparse profiles | accepted |
