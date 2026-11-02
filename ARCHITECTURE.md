# Architecture

Truth class: current descriptive
Last reality check: 2026-10-28
Verification: just docs-check

## System purpose

`casa-rs` implements native Rust libraries and applications that read, write,
and manipulate casacore-compatible tables, MeasurementSets, images,
coordinates, measures, and related workflows.

## Major modules / crates / packages

| Module | Responsibility | May depend on |
|---|---|---|
| core codecs (`casa-values`, `casa-aipsio`) | Internal generic value model and AipsIO-style framing used by higher layers | Rust ecosystem crates only |
| foundation crates (`casa-types`, `casa-measures-data`, `casa-measures-tools`) | Public scalar/quanta/measures algorithms and contracts plus explicit runtime-data validation, loading, installation, and maintenance | core codecs; `casa-measures-data` also uses canonical `casa-tables` accessors |
| shared numerics (`casa-numerics`) | Domain-neutral numerical algorithms reused by observation, calibration, and imaging owners, and CASA's common primary-beam patterns (`PBMath`'s Airy and EVLA beams by telescope) | Rust numerical ecosystem crates only |
| atmosphere (`casa-atmosphere`) | The native port of ATM, CASA's atmospheric transmission model: layered profiles from ground weather, gas refractivities (absorption and phase), spectral grids, zenith opacities, phase delays and path lengths over a grid (full or compact, buildable channel by channel on a caller's threads), and forward radiative transfer for sky brightness; tested against CASA's ATM C++ and `at` tool through `tools/atmosphere` | Rust ecosystem crates only (a science-layer leaf in the dependency policy) |
| persistent storage (`casa-tables`) | CASA table persistence, codecs, data managers/storage backends, schema/mutation APIs, and TaQL engine | core codecs, foundation crates |
| native imaging (`casa-imaging-model`, `casa-imaging-operator`, `casa-imaging-deconvolution`, `casa-imaging-products`, `casa-imaging-runtime`, `casa-imaging-metal`) | The request science and compiled problem, over the definitional crates, and the geometry of observation batches over the observation boundary; the measurement operator (kernel sets, gridding and degridding, FFT and correction, imaging weights, spectral resampling, continuum fitting); image-domain reconstruction (the model lifecycle, the normal state a pass forms, masks and the minor-cycle solvers); normalization, restoration, Taylor, PB and pbcor products, written in bounded windows by the products' CASA image writer with atomic individual-image replacement; host resources, admission, the major-cycle pass, its worker teams and the paged cube state; the Metal backend | ADR-0016 layering, enforced by `scripts/check-imaging-dependencies.py` and `resources/imaging-architecture/dependency-policy.json`: every imaging crate may use the definitional crates (`casa-values`, `casa-types`, `casa-coordinates`); beyond them, `casa-imaging-model` depends only on `casa-observation`, whose catalogue, batches and pointing samples its batch geometry reads (imaging port decision 1, #932); the operator depends on the model, `casa-fft`, `casa-numerics` and, for AW convolution-function images, `casa-images`; deconvolution depends on the model, `casa-fft` and `casa-numerics`; products depend on the model, deconvolution, `casa-fft` and `casa-numerics` and, for their writer, on `casa-images`; runtime composes the operator, deconvolution, products and the Metal crate over `casa-ms`, `casa-tables` and `casa-lattices`; the Metal crate depends on the operator and `casa-numerics` |
| single-dish imaging (`casa-imaging-singledish`) | CASA's `SDGrid` as its own operator family (ADR-0016 decision 13): the single-dish kernels (`BOX`, `SF`, `GAUSS`, `GJINC`, `PB`), the image-plane gridder over in-memory spectra and the degridder the simulator's total-power prediction uses, the spectra's Stokes weights and `tsdimaging`'s weight mask, the raster-sampling estimate and `sdbeamutil`'s theoretical beam; tested against CASA's `tsdimaging` through `tools/singledish` | An execution-layer crate of the ADR-0016 layering: the operator (`grdsf`), the runtime (admission and the worker team) and `casa-numerics` (CASA's primary-beam patterns); reading MeasurementSets is the task's, through `casa-observation` (#938) |
| imaging application composition (`casa-imaging-application`) | Sole production composition seam across MeasurementSet authority, reconstruction, products, resources, execution, typed installed-implementation availability, and CASA product publication | Native imaging owners, `casa-ms` and the observation boundary (`casa-observation`); unavailable requests invoke no execution implementation |
| simulation (`casa-simulation`) | CASA's `sm` tool: the session the setters and `observe` describe, the deferred plan of `predict` and `corrupt` steps, model preparation through the measurement operator, thermal noise and antenna-based corruption (gains and leakage), and one streamed MeasurementSet write at `close`, admitted against the imaging runtime's resource policy and predicting through its block predictor on the CPU or Metal; `simutil` and the `simobserve` task (`casa_simulation::simobserve`), which take CASA's data directories and measures as request fields | An application in the ADR-0016 layering (the dependency policy lists its exact set): the imaging model, operator and runtime (Metal only through the runtime), `casa-imaging-singledish` (the total-power kernels), `casa-ms`, `casa-tables`, `casa-images`, `casa-components` (component lists for `predict`), `casa-fft` (the gains' fractional-Brownian-motion screens), `casa-calibration` (the calibration tables of simulated gains and leakage) and `casa-numerics`, plus the definitional crates |
| domain libraries (`casa-ms`, `casa-simulation-synthesis`, `casa-lattices`, `casa-coordinates`, `casa-images`, `casa-calibration`, `casa-vla`, `casa-components`) | Higher-level astronomy data models and algorithms built on table/image persistence; simulation synthesis owns only the serial model predictor and Airy voltage pattern used by MeasurementSet simulation; `casa-components` owns CASA component lists (sky components with their flux, shape and spectral model) and reads and writes their `ComponentList` tables as CASA does | foundation crates, `casa-tables`, selected peer domain crates where documented; `casa-components` depends only on `casa-tables` and `casa-types` |
| boundary contracts (`casa-provider-contracts`) | The generic task-provider envelope, canonical parameter and application catalogs, task surface definitions, the one resolver of a task's executable (`resolve_task_executable`), and protocol surfaces between providers and Python | domain libraries and foundation crates; must not become a second source of truth |
| parameter and task runtime (`casa-task-runtime`) | Format-neutral parameter resolution, sparse TOML profiles, migrations, task Last/Last Successful coordination, managed Last storage, the shared task front end with its typed replies (#931), on which every task binary runs, and the one `--json-run` invocation a run is projected to | boundary contracts, `casa-types` and `casa-logging` (the front end installs a run's logging from the configuration and stderr the binary's `main` passes); must not implement provider science behavior |
| apps and runtimes (`casars-imager`, `casars-imexplore`, `casars-simulation`, `casars-python`) | The task executables that are not binaries of a domain crate (as `calibrate`, `msexplore` and `immoments` are), each on the shared task front end: the imager application (`casars-imager`), the image-inspection family `imexplore` (`imhead`, `imstat`) and the simulation tasks' host (`simobserve`; `simanalyze` and `simalma` to come); and the Python package with its PyO3 extension `casars._core`, which starts each task's executable directly | boundary contracts, domain libraries, foundation crates |
| test support (`casa-test-support`) | Cross-language parity harnesses, fixtures, integration helpers, and performance guards | any workspace crates needed for testing only |

## Dependency direction

Preferred direction is:

`core codecs -> foundations -> persistent storage / domain libraries -> boundary contracts -> parameter runtime -> apps/runtimes`

Native imaging follows its stricter accepted direction:

`science -> observation / reconstruction -> products -> execution -> backends -> application -> frontends`

Beneath every layer sit the definitional crates, `casa-values`, `casa-types`
and `casa-coordinates`: they define values, units, quanta, measures and
coordinate systems and the pure functions on them, do no file, network,
process or environment I/O, start no threads, use no device and depend only on
one another, so any crate in the layering may use them (ADR-0016 decision 10,
amended 2026-10-25). The dependency policy names them and lists each crate's
other workspace dependencies exactly; adding a crate to the set is the
owner's decision.

`casa-imaging-model` owns the science, reconstruction, and product schemas and
commitments introduced by ADR-0009, over the definitional crates alone, and
the row geometry of observation batches, which reads the observation
boundary (`casa-observation`; ADR-0016 decision 10, amended 2026-10-28, and
imaging port decision 1, #932).
`casa-imaging-operator` owns the measurement operator A and its adjoint
(plan section 5.3): the kernel sets (`Spheroidal`, `WPlanes`, `AwCatalog`,
`MosaicPb`), the `GridBackend` contract and its CPU implementation, the
accumulator, the FFT and gridding correction, imaging weights, the one
spectral resampler and CASA's visibility-domain continuum fit.
`casa-imaging-deconvolution` owns image-domain reconstruction: the model
lifecycle and its storage seam, the normal state a pass forms (the
unnormalised residual, PSF moments, sensitivity and `sumwt` per plane,
resident or paged) and the `MajorCycle` that pairs it with its model, the
masks (boxes, reprojection, CASA `auto-multithresh`), and the minor-cycle
driver and solvers. Both depend inward on the model and the domain-neutral
`casa-fft` and `casa-numerics` crates.
`casa-imaging-products` owns normalization (plan section 5.7), restoration,
Taylor products, PB and pbcor. Its normalization rules map the raw normal state
to CASA's image units and the solved model back to the physical model; the
runtime's minor cycle uses the same rules, so the units a solve sees and the
units products publish are one definition. Applications bind the requested
normalization explicitly. Products may use domain-neutral `casa-numerics`
algorithms directly.
ADR-0014 requires trusted in-process generation to transfer bounded owned windows
directly into private CASA image staging without product content attestation,
verification-only array rereads or an intermediate readable product store.
Inventory, shape against the compiled problem, complete writes, metadata and
I/O checks remain. Each image replacement is atomic; a failure leaves the run and
its output set incomplete and requires rerun. There is no per-member resumable
recovery, content-based idempotency or whole-set rollback protocol.
Generated publication has only generation/write and terminal publication work;
it does not schedule an empty observation-consistency check. Planning and
execution share one immutable routing inventory. The generation writer owns
window shape, finite-value and complete-coverage validation; the CASA writer
owns physical I/O. Explicit pending, generated, published and consumed states
make generation/publication failures terminal and completion available once.
The same ownership rule applies to model lifecycle and normal-state completion:
validate scientific values/support at introduction or modification; a major
cycle owns its final model and the normal state its pass forms and releases
them together, so a residual stays paired with its model without identities;
and do not hash or reread full owned arrays just to assign completion
authority. Routine telemetry stays
indexed in memory and persists a useful final summary, not full-plan checkpoints
at each work/fence event or scans of historical receipts during admission.
`casa-imaging-runtime` owns imaging execution (see Imaging execution): host
resources and the admission of each phase's memory, the major-cycle pass on its
worker team and bounded source stream, cooperative cancellation, the paged cube
state, the minor-cycle adapter and the run summary. It depends inward on the
model, deconvolution, products, the operator and the Metal backend. Metal
runs inside the pass: when the run grids on the GPU every owner grids through
its own `casa_imaging_metal::MetalBackend`.

AW projection reads its convolution functions from one catalog of CASA
`CFS_*`/`WTCFS_*` images (`casa_imaging_operator::aw`), opened header-first and
read cell by cell into a cache bounded by `cf_resident_mb`. Its default,
`auto`, opens the cache at 256 MiB and raises it, once the worker fit has
chosen, into the memory those workers leave free, up to every cell. The
catalog is either an existing CASA cache, read as it is and never
modified, or a native EVLA cache that casa-rs generates in the same CASA format
(`AWConvFunc::makeConvFunction2`, `CFCell::makePersistent`). Before the run the
application compares the request's cells with the cache directory
(`NativeAwCachePolicy`): reuse needs every cell, generation fills in the absent
ones, and regeneration clears the cache first. The loader then checks each
cell's sky increment against the image. A cell is reused because its file
exists; there are no artifact or cache identities and no content hashes. The
native generator is EVLA-specific, not evidence of general telescope support,
and its dish surface data is an explicit input (`evla_surface`), never
discovered in an installed CASA runtime.

`casa-imaging-application` owns production composition across
MeasurementSet observation authority, reconstruction, products, and physical
execution. It compiles the logical request, checks it against the implementation
installed in the build, and either invokes that implementation or returns a
typed unavailable result before planning. A selected production failure is
terminal; there is no alternate
implementation, retry path, or stage-level delegation. Its input is one
`ImagingRequest`, read from the `tclean` parameters the provider catalog
resolves (CASA's, then casa-rs's own controls) and validated once. Every
route (command line, `--json-run` and Python) reaches it through the same
catalog resolution, so the catalog is the only source of defaults and refuses
a casa-rs control its gridder does not read; a CASA parameter its mode does
not use is ignored, as CASA's tclean ignores it. `casars-imager` is a thin frontend over
this interface: it resolves its parameters through the catalog and presents
the result. Unsupported capabilities remain typed unavailable until their
ticket adds one final-owner implementation.

`casa-test-support` stays outside the product dependency chain.

Additional constraints:

- `casa-values` and `casa-aipsio` stay internal implementation crates.
- `casa-aipsio` owns the single framed and bounded-buffer AipsIO codec; storage
  managers select byte order explicitly and do not maintain local detectors or
  primitive codecs.
- `casa-types` owns pure measures algorithms and the `MeasuresProvider`
  contract, including the provider-owned immutable scientific-state identity
  and exact retained-residency projection. `casa-measures-data::MeasuresRuntime`
  is the explicit fallible I/O implementation; applications acquire one
  runtime at an operation boundary and pass it inward. Discovery never
  installs data, and installation is an explicit caller-selected maintenance
  action.
- `casa-tables` keeps the broader storage/write path crate-internal even when user-facing table APIs are exposed from the crate.
- Large lattices and images cross `casa-tables` through the typed
  `TiledArrayStorage` seam; raw tiled-file mechanics remain crate-internal.
  `TileLayoutPlanner` is the sole checked byte-aware physical-layout policy,
  with a 4 MiB default I/O target and exact preservation of legal explicit
  tile shapes. `casa-lattices` exposes one `TraversalSpec` traversal contract
  and one checked byte-aware execution planner used by lattice statistics and
  image expressions, plus byte-based `TempStoragePolicy`/`TempStoragePlan`.
  `casa-images` expressions use construction-only builders and one owned
  compiled numeric/mask evaluator; parsed and persisted expressions compile
  once into that same graph. `casa-coordinates`
  stores its five supported kinds in the closed `CoordinateModel` enum and
  serializes `CoordinateSystem` through one strict casacore codec.
- Within `casa-tables`, lazy read paths are safe to share across threads under an in-process multi-reader, single-writer contract; shared tiled reads use a process-wide bounded cache, while dirty write state stays under exclusive mutable ownership.
- Within `casa-tables`, row/column/cell accessor objects are the public
  table-data surface. `PreparedRowAppender` and prepared mutable rows compile
  schema/column slots once for high-throughput mutation, while `TableWritePlan`
  validates persistence scope before I/O. Public promise-based unchecked write
  methods are not part of the API.
- ADR-0008 defines persistent-table writes: per-column casacore data-manager
  bindings are chosen at creation and preserved when opening or mutating an
  existing table; heterogeneous `TiledShapeStMan` rows share one hypercube per
  distinct shape. MeasurementSet producers use one bounded plan/session whose
  memory ceiling includes every owned scalar and array sink. New tables publish
  from staging; in-place changes hold casacore's table write lock for the whole
  change and add nothing CASA would not write (no keywords, marker files,
  generations or identities). A lock another process holds is waited for, as
  casacore waits, blocked in the kernel so that a deadlocking wait is refused,
  with the waiter in the lock file's request list so that a casacore
  `AutoLocking` holder releases it at its next inspection; a conflicting
  handle in the same process is refused at once, and casa-rs does not yet
  release on request (#694). On a file system without lock support
  (`ENOLCK`, or `ENOTSUP` on macOS SMB) tables are used unlocked with a
  warning, as casacore does for `ENOLCK`. General rollback, snapshots,
  journaling, and copy-on-write generations are not part of the persistence
  contract.
- Versioned provider bundles are boundary contracts; CLI parser projections
  are derived views, not separate truth sources.
- `casa-provider-contracts::ApplicationCatalog` is the sole application
  inventory and launch-metadata owner; every entry is a task. Python,
  packaging, and generators project it directly. One resolver,
  `casa_provider_contracts::resolve_task_executable`, finds a task's
  executable: the task's catalog override (`CASARS_<APP>_BIN`), else a suite
  root's `bin/` (`CASARS_SUITE_ROOT`, or the installed suite the Python
  package belongs to), else the newest `debug` or `release` build in the
  development checkout the package was built from. The first step that
  applies decides; one that lacks the file is an error naming the fix, never
  a fall-through to a later step, `PATH` or `cargo run`.
- Parameter concepts live in the checked aggregate `ParameterCatalog` in
  `casa-provider-contracts`. Each provider bundle embeds the exact referenced
  concepts so the boundary remains self-contained. Task
  `SurfaceDefinition` bindings supply defaults, conditional activation,
  narrowing refinements, optional ordered migrations, presentation, and
  projection metadata; under ADR-0012, an empty migration set declares the
  surface current-only. Bindings cannot redefine concept meaning, normalization,
  units, role, or persistence. Frontends may not redefine those semantics
  locally.
- `casa-task-runtime` owns profile mechanics, managed state, and application
  lifecycle transitions from source parsing and resolution through Last/
  LastSuccessful persistence and task completion. It also owns task-provider
  discovery actions, JSON source loading, diagnostics, serialization, exit
  classification, and the generated common help block. Providers retain typed
  science adapters and domain-specific human parsing; apps may not add
  alternate lifecycle maps, writers, CLI hosts, or launch fallbacks.
- `casars-python` binds Rust to Python through PyO3 alone, in the extension
  `casars._core` (ADR-0017 item 4). Beside the `casars.data` objects and the
  `casars.tools` classes, it projects the parameter runtime, task completion,
  the application catalog, the task-executable resolver and `casa-ms` plot
  documents for `casars.parameters`, `casars.tasks` and `casars.data`, and
  must not become a second implementation of persistence, task semantics, or
  provider contracts. A task call starts the task's own executable as a child
  process (ADR-0017 item 4); no crate spawns task executables on Python's
  behalf.
  Plot documents are `casa_ms::plot_document` serde types, which depend on no
  binding crate.
- Native frontends select a versioned request and `ResourcePolicy`; they do not
  inspect hosts or devices, select imaging implementations, allocate work
  buffers, or define scientific products. Native model code depends on no
  MeasurementSet, backend, device, cache, or allocation API. The machine-
  readable dependency policy under `resources/imaging-architecture/` is
  enforced by `scripts/check-imaging-dependencies.py` in `just arch-check`
  (ADR-0016).

The model's compile input is one `ProblemInput`, compiled at one site in the
application. `compile` validates and canonicalizes logical science, including
immutable coordinate and image-domain geometry; callers supply geometry laws.
Observation pointing records the selected MeasurementSet
column and meaning plus timestamp, interpolation, extrapolation, and missing-row
policies without evaluating rows. Spectral geometry retains exact channel
centres and N+1 boundaries, or a linear WCS law that derives both exactly;
spectral transforms remain unevaluated. `compile_observation` builds the
Observation Snapshot, which is its sources in request order. Each source holds
its position (`input_ordinal`), a provenance that is only the MeasurementSet
locator, the canonical field, UV-distance and intent predicate, the selected
partitions of the observation boundary (`PartitionSelection`: each one's
spectral window and correlation products), the windows' selected channels,
the selected data, flag and weight columns, whether the store holds the
corrected data, and how many samples the selection holds. Neither the
snapshot nor its sources carry an identity, and no sample list is kept: each
pass binds the selected partitions' fields and channels on the boundary,
which delivers whole times of them, and tests the UV distance per sample.

The run reads its observation through the observation boundary
(`casa-observation`, `docs/measurement-set/contracts.md`), from either store:
an MSv4 processing set (XRADIO's schema on local Zarr v3) images as a
MeasurementSet does (imaging port step 6c, owner decision 5). Compile opens
the store once, a MeasurementSet as an `Msv2Observation`, whose opening grids
MAIN's key columns into partitions in bounded row chunks, or a processing set
as an `Msv4Observation`; reads the request's windows, fields, sources,
observations, intents and antennas from its catalogue and its frames from
`MsCalEngine::from_catalogue`; and surveys the partitions the
request selects with one traversal that reads no visibilities (UVW only when
a UV-distance interval or a W-projecting gridder needs it): their selected
samples, fields, times, observations, W range and the epochs at which each
window's frequencies are converted. The run's `ObservationSource` binds one
`AccessRequest` per partition and wave, admitted from the observation's
estimate, and packs each `VisibilityBatch` into the runtime's native rows.

The application opens one Measures provider (`open_measures_runtime`) for
compile and the run; the batch geometry receives it, and neither the
boundary's adapters nor the geometry discover runtime data (the dependency
policy's `observation-geometry-provider-injection`).

`casa-imaging-model` owns the geometry over the observation boundary's
batches (imaging port step 3b, #932): `GeometryPlan` compiles once
from a problem and a `casa_observation::Catalogue` and evaluates each
`VisibilityBatch` into a `BatchGeometry` (per row sample and chart the
reprojected UVW and phase shift, pointing, parallactic angles, AW pointing
pixels and aperture classes; per time the output-frame frequencies). Its
arithmetic is casacore's `UVWMachine`, the prepared frequency conversion and
the position angle in `casa-types`, and CASA's `FTMachine` reprojections
(`GridFtProjection`), the pointing law and the AW pointing groups in
`casa-imaging-model`.

`casa-imaging-model` carries the model-state schemas and the
compiler-owned lifecycle commitment: the target shape, the bounds and the
arithmetic precision. `casa_imaging_deconvolution::ModelLifecycle` owns a
run's model: every run starts from the empty generation, and each major cycle's
final model is the previous generation updated by the minor cycle's sparse
terms, which must be canonical, non-zero, within valid support and within the
bounds. Invalid support never aliases numeric zero. A `MajorCycle` owns its
final model and the normal state its pass forms with that model, and releases
them together as one `MajorCycleCompletion`, which the next major cycle
consumes. Generations, completions and masks carry no identities: ownership
pairs what belongs together. Callers cannot construct raw generations.

The Compiled Problem also derives one Observation Transaction: the canonical
read set of every consumed per-MS selection and selected columns, and the
exact selected-cell scope of an optional `MODEL_DATA` or `CORRECTED_DATA`
write. No
runtime plan binds it: the application opens the observation once, every pass reads it
through the batch source, and the final major-cycle pass writes the
visibilities (see Imaging execution). Conventional image members and
`MODEL_DATA` are separate side effects: failure of one never rolls back or
hides a result already completed by the other. Every product member is
generated into private staging beside its target and moved into place only
after all of them have been generated; a failure or cancellation before then
removes the staging and publishes nothing, and a failure while moving leaves an
incomplete set that must be regenerated, without rollback or a resumable
per-member recovery ledger.
`MODEL_DATA` and `CORRECTED_DATA` follow ADR-0008: the final major-cycle
pass writes selected cells in place through the observation boundary's
update session (`casa_ms::observation_io`) under casacore's table lock,
creating `MODEL_DATA` when MAIN lacks it, as CASA does. An MSv4's session
replaces whole chunks of its `model` and `corrected` groups and cannot add a
group, so a run that saves the model into an MSv4 without one is refused
before it images. Each batch is a
bounded mutation of its rows that patches their tiles, so the session
accepts only destinations in `TiledShapeStMan` or `TiledColumnStMan`, the
managers CASA and casa-rs store visibilities in. It refuses any other
manager, whose save would rewrite every row of MAIN, when the writing pass
binds, before anything changes. Interruption may leave
partial derived values, as in CASA; the next run recomputes them. No
backup column, full-column staging copy, content digest, rollback, snapshot, or
copy-on-write generation is part of this path. Users may retain or delete
conventional products independently; `MODEL_DATA` remains a distinct
MeasurementSet side effect.

The Observation Transaction does not itself change `casa-ms`, casacore
metadata, or persistent bytes. The selected-visibility writer that implements
the `MODEL_DATA` commit is the interoperability boundary and passes the
applicable Rust/C++ RR, RC, CR, and CC matrix.

Installed-implementation availability is an application-owned result checked
once, by `availability::check` on the compiled problem, the run's backend and
grid precision, and the host, during preparation and before any execution
phase, so a typed-unavailable request starts no work. A run leaves
one record beside its products, the run summary of plan section 8.3
(`<imagename>.summary.json` for `casars-imager`): the request echo, the
status (completed, failed with its error, or cancelled), the policy's
memory capacity, the worker count, the backend and resolved grid
precision, the CASA stop reason, the minor-cycle totals, the product images
published, and each phase, preparation first, with whether it completed,
its wall time, the process's resident high-water mark at its end, and the
most and the remaining memory the process's reservations held. Every step
that can fail runs inside a phase (each minor cycle's preparation is one),
so a failed run's last phase is the one that failed. The application writes
it however the run ends (ADR-0014's final success/failure summary); a later
failure keeps the totals of the cycles that completed and lists any images
published before a failure while publishing, and a cancelled run publishes
no product.

## Runtime model

Most crates are synchronous Rust libraries with command-line providers, the
Python package and test harnesses on top. There is no repo-wide async runtime contract today.
Long-running interoperability, parity, and packaging work is driven by
shell/Python scripts, integration tests, or subprocess orchestration rather
than a shared background service model.

ADR-0006 adds one synchronous parameter lifecycle shared by every task
consumer (a task executable's command line and Python). `casa-task-runtime`
is the sole lifecycle owner: a task resolves sparse user intent, records its
attempted state, and applies the completion transition around one provider
invocation. Last has one owner, the task front end, which records Last and
Last Successful for a command-line run of the executable; `--json-run`, the
path Python's task calls take, reads and writes no Last, so a Python call
writes none (#931). Parameter
resolution and Last persistence do not introduce a provider daemon or
repo-wide async runtime.

A Python task call is one child process. Python resolves the parameters
through the parameter runtime, starts the executable the resolver names with
its surface's fixed arguments and `--json-run -`, writes the CASA-named
values the call set to its stdin, passes its log lines to `sys.stderr` as
they arrive, and classifies the typed reply on its stdout. The task runs in
the current directory, or in the workspace `casars.tasks.run` is given. An
interrupt (`KeyboardInterrupt`) is forwarded to the task as SIGINT; the task
has ten seconds to stop before it is killed, and the interrupt is then
re-raised.

### Imaging execution

`casars-imager` owns only catalog resolution of its parameters, the task
protocol, and result presentation. `casa-imaging-application` owns the
`ImagingRequest` and its validation, MeasurementSet expression
resolution, bounded source access, installed-implementation admission, runtime policy, and
independently atomic product publication. The resolved, immutable selection
belongs to `casa-imaging-model`'s Observation Snapshot compiler. Scientific
weighting, gridding/degridding, FFT, normalization, deconvolution, restoration,
and product meaning reside only in their declared native owners.

The application compiles one backend-independent problem, rejects unsupported
requirements before any phase, and runs the cycle loop directly.
`casa-imaging-runtime` detects the host once per process (`HostResources`:
threads, performance cores, free memory, a Metal device). The run's
`ResourcePolicy` (interactive, balanced, exclusive, or explicit ceilings) sets
its memory and its core allocation (`ResourcePolicy::cores`): a compute team
for the minor cycles and products, and a streaming team for the passes, one
worker narrower when the source's producer thread would otherwise take the
run past the policy's core budget (the performance cores, for balanced). Each
phase admits the bytes its owners
report (`admit`: the source envelope, the paged cube cache, one wave of each
pass, the kept imaging weights, the visibility writer, the product writer) and
holds them in a `Reservation` until it ends; a phase that does not fit is
refused, typed, before it starts. Frontends neither calculate science nor
inspect execution devices.
Reusable buffers may reduce allocation churn but do not form a second memory
budget or admission authority.

| Policy | Compute workers | Streaming workers | Memory |
|---|---|---|---|
| interactive | ⌈threads/2⌉ | budget − 1 | half the free memory |
| balanced (`parallel=true`) | ⌈3·threads/4⌉, at most the performance cores | performance cores − 1 | 3/4 of the free memory |
| exclusive | all threads | budget − 1 | all the free memory |
| explicit (`parallel=false` is one worker) | the ceiling, at most the threads | budget − 1 | the ceiling, at most the free memory |

The streaming budget is the performance cores for balanced and the compute
workers otherwise; the streaming width is at least one and at most the
compute width (`STREAM_PRODUCERS`, `ResourcePolicy::cores`). `WorkerTeams`
holds one pool when the widths agree. Free memory is `vm_stat`'s free,
inactive and speculative pages on macOS and `MemAvailable` on Linux, read
once per process.

Every installed request runs through one major-cycle pass
(`casa_imaging_runtime::pass`, plan section 5.4), driven by
`casa-imaging-application`'s cycle loop: a density pass for uniform and Briggs
weights, an initial pass that forms the dirty image and PSF, the minor cycle,
residual passes, and a final pass that writes `MODEL_DATA` or `CORRECTED_DATA`
when requested. Standard MFS, MT-MFS, channel-local cubes, dirty-only runs and
outlier domains share this path; there is no per-mode driver. A pass reads
native row blocks, each row projected on every image domain, from the bounded
source through a two-slot stream (one block filled by a producer thread while
the caller consumes the other), places each row on every domain with that
domain's spectral resampler and the imaging weights, and accumulates `V − A·m`
when a model is present and `V` otherwise. With several domains, or a linear
cube, the pass forms CASA's residual at native channels
(`SIMapperCollection::degrid` then `grid`): every domain's model is predicted
at each native channel, the predictions are summed, and the difference is
gridded into every domain. Cancellation and failure stop the producer at the
next block boundary. `casars-imager` turns the first SIGINT into the run's
`Cancel`: the run stops at the next block boundary or before its next phase,
removes its staging and paged state, publishes nothing, and exits with status
130.

Accumulation is split among the workers of one `WorkerTeam`.
`Partition::Planes` gives each worker a disjoint range of channel-local planes,
so nothing is merged and the result is bitwise independent of the worker count;
`Partition::Regions` gives each worker a horizontal strip of the grid plus the
kernel halo and adds the tiles in region order, so a run is deterministic for
a given worker count and agrees with one worker to rounding. Per-plane FFTs run single-threaded
on each worker. `Residency::plan` holds every plane when the memory the policy
leaves free allows; otherwise the pass runs consecutive waves of planes, one
traversal each, with model and normal state paged through one disk-backed cube
state that keeps every domain at its own size. A wave's model includes a halo
of planes sized from the native channel spacing, since a native channel's
prediction interpolates output channels around it. A wave reads only the
native channels that feed it unless a prediction or a continuum fit needs the
whole row. The residency is planned once per major cycle for the pass and its
paged normal state; a run that writes visibilities must hold every plane in
its final pass and is refused at the start otherwise.

Each traversal names what it reads of each sample (`SampleContent`): the
gridding, prediction and writing passes read values, flags and weights; the
density pass of uniform and Briggs weighting reads flags and weights only, as
CASA's does, so it opens no visibility column. The source fails a run on an
unflagged non-finite input (`FiniteValuePolicy::RejectUnflaggedInput`): a
non-finite value or weight in an unflagged sample, or non-finite UVW in a row
that is imaged, stops the first pass that reads it with an error naming the
MAIN row, channel and correlation, where CASA would image NaN everywhere; a
flagged one is ignored. A run with one image domain, every plane resident and
a constant or Taylor basis keeps its samples' imaging weights across passes
(`WeightCache`, 4 bytes a sample, admitted as "kept imaging weights" when it
fits a sixteenth of the free memory): the first complete pass fills it and
later passes read it in place of the flag test and the density lookup.
Channel-local cubes, outlier domains and runs in waves recompute the weights
each pass.

A backend receives one `Work` per sample block (`GridBackend::apply`):
`Grid` (data, PSF or weight terms), `Predict`, `ResidualGrid` (predict,
subtract and grid the residual; its optional read-back of the residual
samples serves the T0 laws, and the pass leaves it off), `PredictNative`
(predict a row chunk and interpolate it to
native cells, adding when several domains sum) and `NativeResidualGrid` (grid
values less the model at each sample's native endpoints). Work is complete
when `apply` returns, except that a device backend may leave grid-only work
and `PredictNative` running on device cells: the cells (`DeviceCells`) make
the host's next access wait, and a wave calls `MetalBackend::complete`, which
waits for every dispatch and reports the first failure, on each Metal owner
and predictor before it reads an accumulator.

Imager task protocol v13 carries the resolved `tclean` parameters by catalog
name, on stdin for `--json-run`; the result echoes them, and the reply's
value is CASA's tclean summary record. The imager emits no
progress events; the cycle loop logs worker counts and cycle records through
`tracing`, and the run summary records each phase. The request's
diagnostics (`stage_timing`, `metal_stage_timing`, `science_trace`, plan
section 8.1) add structured events under their own `casa_imaging::*`
targets; no imaging library reads the environment. `parallel=false` (the
default) runs the pass with one worker. All production FFTs use FFTW; there is
no FFT backend selector or fallback.

W-projection, AW-projection, mosaics and mosaic cubes run on the pass; an AW
cube is refused until its per-plane weights have a CASA comparison (#719).
`facets = N` (standard gridder, constant basis; #664) makes each of the main
domain's N×N facets a chart: a pass domain of its own, a facet-sized operator
about the facet's centre, onto which casa-ms projects every row with CASA's
`UVWMachine(project=true)`. The pass sees charts, the normal state and
products see image domains: the stitch divides each facet's residual by its own
`sumwt` (scaled to a reference facet's, facet 0 unless it is empty), zeroes a
facet whose `sumwt` is at most 1e-7 (`divideImageByWeightVal`), places the
reference facet's PSF at the image centre
(`SynthesisNormalizer::setPsfFromOneFacet`), and `.sumwt` is N×N with each
facet's own sum. Taylor and
cube bases, and other gridders, with facets are typed unavailability in
`availability::check`. `mtmfs` via cube and cubic spectral interpolation are
not in the catalog, so no request names them (cubic #42).

The request's `backend` (`auto`, the default; `cpu`; or `gpu`) resolves to a
device once the problem is compiled (`availability::resolve_backend`). `cpu`
and `gpu` are taken as asked, and `availability::check` refuses a `gpu` run
the device cannot do. `auto` takes the GPU only where a T1 Metal law compares
it with the CPU and `check` admits it there: one image domain (facets
allowed, any basis), Stokes I, no `MODEL_DATA` or `CORRECTED_DATA` write, the
standard kernel set, a Metal device, and `gridprecision` not `f64`; the CPU
otherwise. `gridprecision = auto` resolves once, beside the backend
(`imaging::measurement::precision`): `f64` on the CPU for every basis,
cubes included, as CASA grids, and `f32` on Metal; an explicit `f32` or `f64`
wins. The run summary records the device and the precision.

The GPU (macOS, a unified-memory Metal 3 device) grids every pass on
the Metal device: `casa-imaging-metal` implements the operator's
`GridBackend` with support-generic kernels (one thread per sample for
supports of at most 81 taps, one SIMD group per sample for wider ones; each
grid polarization and Mueller plane gathered and spread once; `f32` atomic
adds) into accumulators whose cells live in shared device memory
(`GridStorage::Device`). The host locates every sample with the operator's
rounding rule and accumulates `sumwt` in `f64`, so tap selection and `sumwt`
equal the CPU's. Each `apply` is cut into three sub-blocks over a six-slot
ring and spreads its samples in a coprime-stride order, so neighbouring
threads rarely add to the same cell (#716). Completion follows the
`GridBackend` contract above. Operators are `f32` for every basis on Metal
(D2). Placement stays on the CPU workers. The
native-channel residual of a multi-domain or linearly interpolated pass is
formed on the device: each row chunk's predictor predicts its batch and
interpolates it to the rows' native cells (`Work::PredictNative`) in cells
from `MetalBackend::native_cells`, and the owners subtract that model at each
sample's native endpoints as they grid (`Work::NativeResidualGrid`); nothing
is read back unless a visibility writer asks for the model.
Metal grids the standard and mosaic kernel sets (#787). A mosaic's weight
image is summed by contribution before it is gridded, so the f32 grid keeps
its precision (#792). Each Metal backend copies the kernel cells it meets to
the device, in an arena reserved whole from the set's declared footprint;
the wave demand admits one copy of the set per owner and per predictor
(`casa_imaging_metal::kernel_bytes`, #793). W projection and AW projection
run on the CPU, and `availability::check` refuses them on Metal.
Application availability is the capability boundary: component presence alone
never makes a route available.

## Persistence / external systems

- casacore-compatible table trees and image tables on local disk
- sparse user-authored parameter profiles in arbitrary user-selected locations
- managed parameter state under
  `<workspace>/.casa-rs/parameters/<surface-id>/`, optionally redirected by
  `CASA_RS_STATE_DIR`
- MeasurementSet and CASA image fixtures under the shared dataset root (`../casatestdata` by default, override `CASA_RS_TESTDATA_ROOT`)
- measures runtime data in an explicitly selected CASA-compatible table tree;
  discovery may offer complete `CASA_RS_MEASURESPATH` and `~/.casa/data`
  candidates but never installs or repairs them
- local casacore C++ installations via Homebrew for parity tests and demos when available
- GitHub Actions as the canonical CI environment, with `scripts/ci-local.sh` as local reproduction support

## Public interfaces

- published Rust library crates, especially `casa-types`, `casa-tables`, `casa-ms`, `casa-lattices`, `casa-coordinates`, and `casa-images`
- the task executables that serve the catalog's 26 tasks, each run directly
  on the shared task front end, such as `msexplore`, `imexplore` (`imhead`,
  `imstat`), `calibrate`, `importvla`, `casars-imager` and `simobserve`
- Python package `casars-python`, with its one native module `casars._core`
- persisted CASA-compatible on-disk table, image, and related data formats
- versioned provider contract bundles and protocol schemas
- versioned sparse TOML task parameter profiles

## Approved dependency classes

- N-dimensional arrays and numeric containers: `ndarray`
- FFT and spectral transforms: direct, in-place FFTW 3 through `casa-fft`
- error types: `thiserror`
- Adding a second library in the same category requires review.

## Known constraints

- On-disk interoperability with casacore-compatible formats is more important than mirroring C++ APIs directly.
- Heavy CASA parity suites must stay opt-in rather than in the default `cargo test --workspace` path.
- Some cross-language and parity tests must skip cleanly when `pkg-config casacore` or measures data are unavailable.
- GitHub issues and pull requests are the authoritative work record.

## Known current gaps / debt

- `just` provides a stable command vocabulary, but some contributors may still use the underlying `cargo` and `scripts/*` commands directly until it is installed locally.
- Imaging capabilities that are not installed return typed unavailability from
  `casa-imaging-application` before planning; production never enters a
  displaced implementation. `scripts/check-imaging-dependencies.py` (in `just
  arch-check`) rejects crate edges outside the ADR-0016 layering, non-exact
  native dependency sets, definitional crates that reach beyond one another or
  do I/O, device APIs outside `casa-imaging-metal`, and
  environment reads, `eprintln!` and content hashing in imaging crates. Its
  only grandfathered files are three environment readers outside the imaging
  crates (`casa-measures-data`, `casa-ms`'s simulation task and
  `casa-task-runtime`'s state storage); the lists only shrink.
- The imaging foundation refactor (#648, ADR-0016) is complete. The plan in
  `docs/imaging-architecture/imaging-foundation-plan-20261007.md` records
  its decisions, the deviations its tickets took and the review gates; the
  open capability and performance work it left is in #445, #447–#450, #653, #690,
  #714 and #719–#721.

## ADR index

| ADR | Title | Status |
|---|---|---|
| 0001 | Public surface and workspace layering | accepted |
| 0002 | Native Rust implementation with casacore-compatible persistence | accepted |
| 0003 | Provider schema bundle as boundary contract | accepted |
| 0004 | Tiered verification and heavy parity gates | accepted |
| 0005 | Native macOS GUI prototype boundary | retired |
| 0006 | Unified parameter catalog and sparse profiles | accepted |
| 0007 | Scientific notebooks and assistant boundary | retired |
| 0008 | Casacore storage and bounded MeasurementSet writes | accepted |
| 0009 | Mathematical imaging architecture | accepted |
| 0010 | Unified imaging resource authority | superseded |
| 0011 | Distinct sequential and joint continuum-line reconstruction | accepted |
| 0012 | Current-only sparse profile contracts | accepted |
| 0013 | Non-cryptographic integrity for private run-scoped spill artifacts | accepted |
| 0014 | Trusted product generation without content attestation | accepted |
| 0015 | Run-local imaging ownership without content attestation | accepted |
| 0016 | Imaging foundation | accepted |
| 0017 | Python and headless task providers are the product surface | accepted |
| 0018 | Rust computes; Python is the interface | accepted |
| 0019 | CASA tools are Rust-declared pyo3 objects in `casars.tools` | accepted |
