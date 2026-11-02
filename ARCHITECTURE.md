# Architecture

Truth class: current descriptive
Last reality check: 2026-11-02
Verification: just docs-check

## System purpose

`casa-rs` implements native Rust libraries and applications that read, write,
and manipulate casacore-compatible tables, MeasurementSets, images,
coordinates, measures, and related workflows.

## Major modules / crates / packages

| Module | Responsibility | May depend on |
|---|---|---|
| core codecs (`casa-values`, `casa-aipsio`) | Internal generic value model and AipsIO framing used by higher layers | Rust ecosystem crates only |
| foundation crates (`casa-types`, `casa-measures-data`, `casa-measures-tools`) | Scalars, quanta and measures algorithms and contracts; validation, loading, installation and maintenance of measures runtime data | core codecs; `casa-measures-data` also uses `casa-tables` accessors |
| shared numerics (`casa-numerics`, `casa-fft`) | Domain-neutral numerical algorithms and CASA's common primary-beam patterns; direct, reusable FFTW plans | Rust ecosystem crates and FFTW only |
| logging (`casa-logging`) | CASA's priority model, log tables and sinks under `tracing` | `casa-tables`, `casa-types` |
| atmosphere (`casa-atmosphere`) | The native port of ATM, CASA's atmospheric transmission model | Rust ecosystem crates only (a science-layer leaf) |
| persistent storage (`casa-tables`) | CASA table persistence, data managers, schema and mutation APIs, TaQL | core codecs, foundation crates |
| observation boundary (`casa-observation`) | The contracts MSv2 and MSv4 stores share: catalogue, `AccessRequest`, traversal and `VisibilityBatch` | `casa-types` |
| native imaging (`casa-imaging-model`, `-operator`, `-deconvolution`, `-products`, `-runtime`, `-metal`) | Request science and compiled problem; the measurement operator; image-domain reconstruction; normalization and products; host resources, admission and the major-cycle pass; the Metal backend (see Imaging ownership) | the ADR-0016 layering below |
| single-dish imaging (`casa-imaging-singledish`) | CASA's `SDGrid` as its own operator family (ADR-0016 decision 13) | execution layer of the ADR-0016 layering |
| imaging application (`casa-imaging-application`) | Sole production composition of observation, reconstruction, products, resources and execution | native imaging owners, `casa-ms`, `casa-observation` |
| simulation (`casa-simulation`) | CASA's `sm` tool, `simutil` and the `simobserve` task | an application of the ADR-0016 layering |
| domain libraries (`casa-ms`, `casa-simulation-synthesis`, `casa-lattices`, `casa-coordinates`, `casa-images`, `casa-calibration`, `casa-vla`, `casa-components`) | Astronomy data models and algorithms over table and image persistence | foundation crates, `casa-tables`, documented peer domain crates |
| boundary contracts (`casa-provider-contracts`) | Task-provider envelope, parameter and application catalogs, task surfaces, the task-executable resolver | domain libraries and foundation crates; never a second source of truth |
| parameter and task runtime (`casa-task-runtime`) | Parameter resolution, sparse TOML profiles, migrations, Last/Last Successful, the shared task front end and its `--json-run` invocation | boundary contracts, `casa-types`, `casa-logging`; no provider science |
| apps and runtimes (`casars-imager`, `casars-imexplore`, `casars-msconvert`, `casars-simulation`, `casars-python`) | Task executables that are not binaries of a domain crate, each on the shared task front end, and the Python package with its PyO3 extension `casars._core` | boundary contracts, domain libraries, foundation crates |
| test support (`casa-test-support`) | Cross-language parity harnesses, fixtures, integration helpers | any workspace crate, for tests only |

## Dependency direction

General direction:

`core codecs -> foundations -> persistent storage / domain libraries -> boundary contracts -> parameter runtime -> apps/runtimes`

Imaging follows the stricter ADR-0016 layering:

`science -> observation / reconstruction -> products -> execution -> backends -> application -> frontends`

`resources/imaging-architecture/dependency-policy.json` lists each layer's
crates and each native crate's exact workspace dependencies.
`scripts/check-imaging-dependencies.py` (in `just arch-check`) enforces it and
rejects edges outside the layering, inexact dependency sets, definitional
crates that reach beyond one another or do I/O, device APIs outside
`casa-imaging-metal`, and environment reads, `eprintln!` and content hashing
in imaging crates. Its grandfathered-file lists only shrink.

The definitional crates, `casa-values`, `casa-types` and `casa-coordinates`,
sit beneath every layer: they define values, units, quanta, measures and
coordinate systems and the pure functions on them, do no file, network,
process or environment I/O, start no threads, use no device and depend only
on one another, so every crate in the layering may use them (ADR-0016
decision 10). Adding a crate to that set is the owner's decision.

`casa-test-support` stays outside the product dependency chain.

### Crate boundaries

- `casa-values` and `casa-aipsio` stay internal. `casa-aipsio` owns the one
  framed, bounded-buffer AipsIO codec; storage managers select byte order
  explicitly and keep no local codecs.
- `casa-types` owns the pure measures algorithms and the `MeasuresProvider`
  contract; `casa-measures-data::MeasuresRuntime` is its fallible I/O
  implementation. An application acquires one runtime at an operation
  boundary and passes it inward. Discovery never installs data; installation
  is an explicit maintenance action.
- `casa-tables` keeps its storage and write path crate-internal; row, column
  and cell accessors are the public table-data surface. Lazy reads may be
  shared across threads (multi-reader, single-writer in one process); dirty
  write state stays under exclusive mutable ownership. Large lattices and
  images cross it through the typed `TiledArrayStorage` seam.
- Provider bundles are versioned boundary contracts; CLI parsers are views
  derived from them.
- `casa-provider-contracts::ApplicationCatalog` is the one application
  inventory; every entry is a task. `resolve_task_executable` is the one
  resolver of a task's executable: the catalog override
  (`CASARS_<APP>_BIN`), else a suite root's `bin/` (`CASARS_SUITE_ROOT`, or
  the installed suite the Python package belongs to), else the newest
  `debug` or `release` build of the development checkout the package was
  built from. The first step that applies decides; a missing file is an
  error naming the fix, never a fall-through to a later step, `PATH` or
  `cargo run`.
- Parameter concepts live in the checked `ParameterCatalog`; each provider
  bundle embeds the concepts it references. A task's `SurfaceDefinition`
  supplies defaults, activation, narrowing, migrations and presentation, but
  cannot redefine a concept's meaning, normalization, units, role or
  persistence, and neither can a frontend. An empty migration set declares a
  surface current-only (ADR-0006).
- `casa-task-runtime` owns profile mechanics, managed state, the task
  lifecycle from parsing through Last/Last Successful to completion,
  provider discovery, diagnostics, exit classification and the common help
  block. Providers keep typed science adapters; apps add no lifecycle maps,
  writers, CLI hosts or launch fallbacks.
- `casars-python` binds Rust to Python through PyO3 alone, in
  `casars._core` (ADR-0017, ADR-0018). It projects the parameter runtime,
  task completion, the application catalog, the resolver and `casa-ms` plot
  documents, and is never a second implementation of persistence, task
  semantics or provider contracts. A task call starts the task's executable
  as a child process; no crate spawns task executables on Python's behalf.
- Frontends select a request and a `ResourcePolicy`; they do not inspect
  hosts or devices, select implementations, allocate work buffers or define
  scientific products.

### Imaging ownership

- `casa-imaging-model` owns the request science, the compiled problem (one
  `ProblemInput`, compiled at one site in the application), the Observation
  Snapshot and the geometry of observation batches (`GeometryPlan`,
  `BatchGeometry`) over `casa-observation`. It depends on no
  MeasurementSet, backend, device, cache or allocation API.
- `casa-imaging-operator` owns the measurement operator A and its adjoint:
  the four kernel sets (`Spheroidal`, `WPlanes`, `AwCatalog`, `MosaicPb`),
  the `GridBackend` contract and its CPU implementation, imaging weights, the
  one spectral resampler and CASA's visibility-domain continuum fit.
- `casa-imaging-deconvolution` owns the model lifecycle, the normal state a
  pass forms, the `MajorCycle` that pairs them, the masks and the minor-cycle
  driver and solvers.
- `casa-imaging-products` owns normalization, restoration, Taylor, PB and
  pbcor products and the CASA image writer. Its normalization rules are also
  the ones the minor cycle uses, so the units a solve sees and the units
  products publish are one definition.
- `casa-imaging-runtime` owns host resources, admission, the major-cycle
  pass and its worker teams, cancellation, the paged cube state and the run
  summary. `casa-imaging-metal` implements `GridBackend` on Metal.
- `casa-imaging-application` is the sole production composition. Its input
  is one `ImagingRequest`, made from the `tclean` parameters the provider
  catalog resolves; every route (command line, `--json-run`, Python) reaches
  it through that resolution, so the catalog is the only source of defaults.
  It opens one Measures provider for compile and run and injects it; no
  adapter or geometry discovers runtime data. `casars-imager` only resolves
  parameters, speaks the task protocol and presents the result.
- Availability is checked once (`availability::check`), on the compiled
  problem, backend, grid precision and host, before any phase. An
  unsupported request returns a typed unavailable result and starts no work;
  a selected implementation's failure is terminal, with no alternate
  implementation, retry or stage-level delegation. Component presence alone
  never makes a route available.

## Runtime model

Most crates are synchronous Rust libraries under command-line providers, the
Python package and test harnesses. There is no repo-wide async runtime or
background service; long-running parity and packaging work runs from scripts,
integration tests or subprocesses.

Every task consumer shares one synchronous parameter lifecycle (ADR-0006),
owned by `casa-task-runtime`: a task resolves sparse user intent, records its
attempted state and applies the completion transition around one provider
invocation. The task front end records Last and Last Successful for a
command-line run; `--json-run`, which Python's task calls use, records none.

A Python task call is one child process. Python resolves the parameters,
starts the executable the resolver names with `--json-run -`, writes the
values the call set to its stdin, forwards its log lines to `sys.stderr` and
classifies the typed reply on its stdout. A `KeyboardInterrupt` is forwarded
as SIGINT; the task has ten seconds to stop before it is killed, and the
interrupt is then re-raised.

### Imaging execution

Every request runs one cycle loop over one major-cycle pass
(`casa_imaging_runtime::pass`): a density pass for uniform and Briggs
weights, an initial pass forming the dirty image and PSF, minor cycles,
residual passes, and a final pass that writes `MODEL_DATA` or
`CORRECTED_DATA` when asked. Standard, W, AW and mosaic gridding, MFS,
MT-MFS, cubes, facets and outlier domains are parameterisations of this one
path; there is no per-mode driver. Later major cycles re-read the
observation and grid residual samples; no replay cache is kept.

The host is detected once per process (`HostResources`). The run's
`ResourcePolicy` sets its memory and its core allocation: a compute team for
minor cycles and products, a streaming team for passes. Each phase admits
the bytes its owners report and holds them in a `Reservation` until it ends;
a phase that does not fit is refused, typed, before it starts. Reusable
buffers never form a second budget. When every plane does not fit, a pass
runs in waves with model and normal state paged through one disk-backed cube
state; a run that writes visibilities must hold every plane in its final
pass.

The source fails a run on a non-finite value, weight or UVW in an unflagged
sample, naming the MAIN row, channel and correlation, where CASA would image
NaN everywhere. SIGINT becomes the run's `Cancel`: the run stops at the next
block boundary or phase, removes its staging and paged state, publishes
nothing and exits with status 130.

`backend` (`auto`, `cpu`, `gpu`) resolves once the problem is compiled.
`auto` takes the GPU only for problems a T1 Metal law compares with the CPU
and `availability::check` admits; extending it needs such a law first.
`gridprecision = auto` is `f64` on the CPU, as CASA grids, and `f32` on
Metal, which accumulates in `f32` only. The run summary records the device
and precision used.

Diagnostics are request fields (`stage_timing`, `metal_stage_timing`,
`science_trace`) that add `tracing` events under `casa_imaging::*` targets;
no imaging library reads the environment. All production FFTs use FFTW;
there is no FFT selector or fallback.

## Imaging code rules

Every imaging change is reviewed against these rules, beside the ADRs and
the dependency checker. They are numbered so that code can cite them.

1. **Parse at the boundary, trust inside.** Validate user input,
   MeasurementSet contents and external files once, where they are read,
   and return a type that carries the proof. Nothing below re-checks the
   shape, finiteness, identity or ordering of what it received typed. A
   check needed in two places means the type is wrong.
2. **Typestate, not lifecycle checks.** "X before Y", "bound to this
   problem" and "sealed" are encoded by consuming `self` and returning the
   next state, not by flags, generation counters or `Result`s that fail only
   through programmer error.
3. **No silent fallbacks on required data.** `unwrap_or_default`,
   `unwrap_or(0)`, `.ok()?` and an `if let Some` that skips work are only for
   data whose type says it is optional.
4. **No content hashing for trust** (ADR-0014). No SHA-256 or CRC
   over scientific arrays, plans, summaries or same-run spill; identity is
   ownership. Checksums an external format defines are the only exception.
5. **One owner per invariant.** Each fact (worker count, memory budget, grid
   shape, channel map, weighting) is computed once, stored once and passed;
   projections of it are deleted, not synchronised. The owner of an
   allocation charges what it holds: retained capacities, overlapping
   temporaries and storage read footprints, not lengths. A change that adds
   or moves scratch ownership adds a counting law that measures the owner's
   peak against its charge under the inputs that grow it (wide cells, high
   native-to-output spectral ratios, several live owners).
6. **Bookkeeping does not scale with data.** Telemetry and the run summary
   are O(1) per phase. Nothing serialises, hashes or walks whole plans,
   models, sample streams or history for progress.
7. **No one-use abstractions.** A trait needs two production
   implementations (`GridBackend`, `ConvolutionFunctionSet`, `Solver`),
   except one that erases a type inside one owner or places an
   implementation behind a dependency boundary, whose rustdoc names that
   reason. Test doubles do not count. Factories that build one thing,
   wrappers that forward every method and builders for structs with public
   fields are removed.
8. **Errors are types.** Each crate has one `Error` enum whose variants a
   caller can act on; no `io::Error::other(format!(..))` for domain
   failures. Anything else is a bug and panics naming the invariant;
   `unreachable!` and `assert!` in production carry a message and are rare.
9. **No environment switches in libraries.** Diagnostic switches are
   `ImagingRequest.diagnostics` fields; a library reads no `std::env`.
10. **No narration.** Comments say why, not what: no step numbering,
    restated signatures or history. Rustdoc on public items states the
    contract.
11. **Delete in the same change.** A replacement migrates its callers and
    deletes the displaced code, tests, docs and resources in one PR; git
    history is the reference.
12. **Tests test laws and contracts:** adjoint identities, conservation,
    tolerance against a reference and resource bounds, not fixture-specific
    numbers. A test of an implementation detail that is not a promised
    contract is deleted. Resource-bound laws compare a charge with measured
    allocation (`capacity()`, RSS, footprint), not one formula with another.
    Interop laws use an independent casacore reader in its own process, held
    open across the write.
13. **No mode, flag or route to preserve old machinery.** A mechanism the
    shared types cannot express is raised as an issue, not kept as a second
    path.

## Persistence / external systems

- casacore-compatible table trees and image tables on local disk. Writes
  follow ADR-0008: data-manager bindings are chosen at creation and
  preserved; MeasurementSet producers use one bounded plan whose memory
  ceiling covers every sink; new tables publish from staging; an in-place
  change holds casacore's table write lock throughout and adds nothing CASA
  would not write (no keywords, marker files, generations or identities).
  A lock another process holds is waited for in the kernel, as casacore
  waits, with the waiter in the lock file's request list so an `AutoLocking`
  holder releases it; casa-rs itself does not yet release a lock on request.
  A conflicting handle in the same process is refused.
  Where the file system has no locks (`ENOLCK`, or `ENOTSUP` on macOS SMB),
  tables are used unlocked with a warning. Rollback, snapshots, journaling
  and copy-on-write generations are not part of the contract.
- Imaging products: each member is generated into private staging beside its
  target and moved into place once all are generated. A failure or cancel
  before then publishes nothing; a failure while moving leaves an incomplete
  set to regenerate, with no rollback or resumable recovery (ADR-0014).
- `MODEL_DATA` and `CORRECTED_DATA`: the final pass writes the selected cells
  in place through the observation boundary's update session
  (`casa_ms::observation_io`) under the table lock, creating `MODEL_DATA`
  when MAIN lacks it, as CASA does. Only `TiledShapeStMan` and
  `TiledColumnStMan` destinations are accepted; any other manager, and an
  MSv4 without the group to write, is refused before anything changes.
  Interruption may leave partial values, as in CASA. Products and these
  columns are independent side effects; a failure of one never undoes the
  other.
- AW convolution functions: one catalog of CASA `CFS_*`/`WTCFS_*` images,
  either an existing CASA cache, read as it is and never modified, or a
  native EVLA cache that casa-rs writes in the same format. A cell is reused
  because its file exists; there are no cache identities or hashes. The
  EVLA dish surface data is an explicit input (`evla_surface`), never
  discovered in an installed CASA.
- A run summary beside the products (`<imagename>.summary.json`), written
  however the run ends.
- sparse user-authored parameter profiles anywhere the user chooses, and
  managed parameter state under `<workspace>/.casa-rs/parameters/<surface-id>/`
  (or `CASA_RS_STATE_DIR`)
- measures runtime data in an explicitly selected CASA-compatible table
  tree; discovery may offer `CASA_RS_MEASURESPATH` and `~/.casa/data`
  candidates but never installs or repairs them
- MeasurementSet and image fixtures under the shared dataset root
  (`../casatestdata` by default, `CASA_RS_TESTDATA_ROOT` to override)
- local casacore C++ installations for parity tests when available
- GitHub Actions as the canonical CI, with `scripts/ci-local.sh` to reproduce
  it locally

## Public interfaces

- published Rust library crates, especially `casa-types`, `casa-tables`,
  `casa-ms`, `casa-lattices`, `casa-coordinates` and `casa-images`
- the task executables serving the catalog's tasks, each on the shared task
  front end (for example `msexplore`, `imexplore`, `calibrate`, `importvla`,
  `casars-imager`, `simobserve`)
- the Python package `casars-python`, with its one native module
  `casars._core`
- persisted CASA-compatible table, image and related formats
- versioned provider contract bundles and protocol schemas
- versioned sparse TOML task parameter profiles

## Approved dependency classes

- N-dimensional arrays and numeric containers: `ndarray`
- FFT: direct, in-place FFTW 3 through `casa-fft`
- error types: `thiserror`
- Adding a second library in the same category requires review.

## Known constraints

- On-disk interoperability with casacore is more important than mirroring
  its C++ APIs.
- Heavy CASA parity suites stay opt-in, outside the default
  `cargo test --workspace`.
- Cross-language and parity tests skip cleanly when `pkg-config casacore` or
  measures data are unavailable.

## ADR index

| ADR | Title | Status |
|---|---|---|
| 0006 | Unified parameter catalog and sparse profiles | accepted |
| 0008 | Casacore storage and bounded MeasurementSet writes | accepted |
| 0009 | Mathematical imaging architecture | accepted |
| 0011 | Continuum-line coupling | retired |
| 0014 | Identity is ownership, without content attestation | accepted |
| 0016 | Imaging foundation | accepted |
| 0017 | Python and headless task providers are the product surface | accepted |
| 0018 | Rust computes; Python is the interface | accepted |
| 0019 | CASA tools are Rust-declared pyo3 objects in `casars.tools` | accepted |
