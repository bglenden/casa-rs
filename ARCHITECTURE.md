# Architecture

Truth class: current descriptive
Last reality check: 2026-07-11
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
| notebook runtime (`casa-notebook`) | Source-preserving Markdown/cell parsing, stable notebook/cell/run identity, atomic project persistence, immutable execution receipts, conflict handling, and portable/advanced exports | parameter value serialization and general-purpose ecosystem crates; must not own provider execution or frontend state |
| apps and runtimes (`casars`, `casars-imager`, `casars-importvla`, `casars-python`, `casars-frontend-services`, `ratatui-graphics`, `apps/casars-mac`) | Terminal shells, orchestration binaries, Python bindings/package, frontend service bindings, rendering/runtime support, and the native macOS GUI prototype | boundary contracts, domain libraries, foundation crates; lightweight frontend services may expose read-only domain-library probes through UniFFI |
| test support (`casa-test-support`) | Cross-language parity harnesses, fixtures, integration helpers, and performance guards | any workspace crates needed for testing only |

## Dependency direction

Preferred direction is:

`core codecs -> foundations -> persistent storage / domain libraries -> boundary contracts -> parameter runtime / notebook runtime -> apps/runtimes`

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
  domain-library reads and `casa-notebook` operations into GUI-appropriate
  projections, but it must not become a second implementation of persistence,
  task semantics, or provider contracts.

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

Issue #368 adds a checked-in Xcode app host and macOS UI Testing Bundle around
that same SwiftPM package. The host compiles the existing SwiftUI app sources
and links the local `CasarsMacCore` product solely to provide an application
boundary for XCTest/XCUIAutomation. It is test infrastructure, not another app
family, state owner, fixture schema, runtime, or distribution path.

ADR-0007 defines the runtime boundary for the scientific-notebook program.
`casa-notebook` now owns the Wave 1 Markdown/cell, execution-receipt, locking,
conflict, and export contracts shared by GUI, TUI, CLI, and Python. App surfaces
record through this crate on an explicitly selected project root; recorder
failure is a warning and never changes the scientific operation result. Swift
uses DTO projections from `casars-frontend-services` and does not own the
persisted schema. Each pending attempt holds an advisory per-run lease for its
process lifetime, so projection refreshes and other processes cannot classify
a live run as interrupted; recovery claims only a released lease. Parameter
replay opens a fresh canonical task tab when no unambiguous target exists,
replaces a clean matching target, and requires a typed diff confirmation before
replacing a dirty target. It reports current contract/default drift without
claiming exact reproduction. GUI and TUI image-region and mask writes use the
same operation-receipt path as tasks. Direct provider binaries remain outside
implicit project recording; project-mediated execution enters through `casars
run` or another app surface with an explicit workspace, and CLI/Python callers
may route to an existing named notebook explicitly.

The planned later-wave tutorial-manifest support and `casars-assistant`
TypeScript sidecar are isolated behind a
CASA-RS-owned JSONL/stdio protocol and may use Pi as a replaceable model/auth
adapter. Swift remains the native interaction layer, and
`casars-frontend-services` remains a projection layer rather than a persistence
implementation. The assistant and later-wave tutorial/kernel modules remain
accepted architecture but do not exist until their implementation waves land.

Every notebook-program wave starts with a launchable deterministic GUI
prototype and an explicit approval gate before real adapters are connected.
The prototype state belongs in `CasarsMacCore`; it may not establish persisted
or provider semantics that bypass the Rust-owned contracts.

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
memory planner does not lose plane residency or row locality. Consumer failure
sets a shared cancellation token, drops the rendezvous receiver to wake a
blocked producer, and prevents another bounded source read after the current
in-flight read; the original consumer error remains the returned context.

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

`chanchunks` supplies a minimum spectral-slab residency shape, not an exact
worker cap or a switch for shared-source concurrency. For every cube plan, the
runtime derives active-plane and worker concurrency from plane/channel geometry,
hardware capacity, the exact source-cache size, the per-plane working set, and
the run-level memory target. If all planes fit, it uses the ordinary one-slab
route. Any selected multi-slab shape is eligible for bounded shared-source reuse
when the same formula proves the source cache and concurrent plane state
resident; neither dataset identity nor a particular `chanchunks` value selects
that route.

On Apple platforms, eligible f32 standard and single-term mosaic dirty products
can keep grids resident through MPSGraph FFT, correction, normalization, and
peak reduction. Mosaic MT-MFS keeps its multi-plane input resident through the
batched inverse FFT, then performs Taylor-term image correction and PB
normalization on the CPU. Explicit `metal-mpsgraph` requests select the resident
path when supported and fail closed on backend errors. `auto` compares exact
input-boundary movement from shape, batch, precision, and placement instead of
using an image-size crossover: host-resident grids stay on CPU, while
Metal-shared grids stay on Metal and avoid host materialization. Under `auto`,
unsupported shapes, unavailable devices, resident-command failures, and f64
product transforms use the CPU finisher. Standard and mosaic MFS recover
retained shared grids directly. When `auto` must recover an MT-MFS Metal
attempt, it replays the bounded source stream to rebuild equivalent host grids;
that recovery route does not alter the normal direct Metal-shared MT-MFS
accumulation path. Backend and fallback decisions are reported in diagnostic
telemetry rather than changing product membership or persistence semantics.

W-projection Auto plane selection follows the CASA geometric relation using
the selected rows' observed maximum absolute projected W, CASA's 1.05 W-range
safety factor, and the actual rectangular half-field angle. It does not use the
array's longest physical baseline as a proxy, round to a power of two, or clamp
to a tested image-size regime; like CASA, the positive plane-count expression
is truncated to an integer. Explicit `wprojplanes` remains an accuracy/cost
choice; both explicit and Auto plans scale their quadratic W coordinates to the
same safety-expanded observed W range.

W-projection has one Metal dispatch and reduction implementation for both
materialized sample slices and bounded replay chunks. The partial-grid count is
derived from sample count, grid cells, convolution-kernel cells, output-grid
count, and live Metal working-set headroom. Its square-root update-density
balance minimizes the sum of per-partial atomic depth and final reduction
depth; it is not selected by dataset or image identity. Each replay chunk uses
that shared plan, and completed chunk grids are combined in deterministic host
f64 order before the one final f32 narrowing. The replay producer includes any
already prepared first block in the same bounded stream as later source blocks,
so the normal two-live-block policy can overlap the next source read with CPU
or Metal gridding without a separate cached-block execution path.

Large mosaic MFS and MT-MFS can write directly into the Metal-shared f32 FFT
input through disjoint output-owned tiles. CPU workers route exact convolution
plan records to disjoint tiles and convolve without atomics or full-grid worker
replicas. Standard MFS retains its established 256-pixel tiling; MT-MFS derives
its tile edge and count from grid geometry, kernel support, requested workers,
and the available scratch budget. MT-MFS keeps f64 PSF moments and Complex64
dirty moments per complete plan key, applies the Taylor residual identity before
gridding, and narrows only at the bounded f32 tile. The complete key includes
grid location, subpixel offset, support, and clipped tap ranges; projector/PB
identity remains fixed by the metadata group. MT-MFS processes one metadata
group and bounded compaction chunk at a time. The frontend derives the requested
scratch from image cells, Taylor plane count, and planned workers, then caps it
by the run-level memory target after fixed products, caches, and one source row
block are reserved. The core reduces worker count when a support-sized tile
cannot fit, subtracts exact worker-tile storage, and converts the remainder into
a raw-sample limit from the actual compact record layout and geometry-derived
route-copy bound. Reusable standard-MFS tap plans likewise receive an exact byte
budget instead of a sample-count cutoff. The frontend memory planner and core
executor share these formulas; no dataset identity or benchmark-specific sample
threshold participates in the decision. Image-domain correction, PB
normalization, and product semantics stay after the FFT. No generic
compatibility block facade or normal-path host full-grid upload is retained.

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
- accepted future notebook state from ADR-0007: visible Markdown and assets
  under `notebooks/`, copied project documents under `documents/`, and versioned
  managed receipts, transcripts, tutorial locks, Python environments, and local
  corpus indexes under `.casa-rs/`

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
| 0007 | Scientific notebooks and assistant boundary | accepted |
