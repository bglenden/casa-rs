# Architecture

Truth class: current descriptive
Last reality check: 2026-08-23
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
| shared numerics (`casa-numerics`) | Domain-neutral numerical algorithms reused by observation, calibration, and imaging owners | Rust numerical ecosystem crates only |
| persistent storage (`casa-tables`) | CASA table persistence, codecs, data managers/storage backends, schema/mutation APIs, and TaQL engine | core codecs, foundation crates |
| native imaging contracts (`casa-imaging-model`, `casa-imaging-operator`, `casa-imaging-deconvolution`, `casa-imaging-reconstruction`, `casa-imaging-products`, `casa-imaging-runtime`, `casa-imaging-metal`) | Dependency-free logical schemas and commitments; the measurement operator (weighting, spectral resampling, gridding, degridding, FFT and normalization); minor-cycle solvers; authoritative model generations, deltas and final-model completion; continuum product algorithms with bounded owned windows streamed directly to CASA staging and atomic individual-image replacement; process-level resource topology, policies, demand envelopes, arbitration and leases; the major-cycle pass, worker team and paged cube state; the Metal backend | ADR-0016 layering, enforced by `scripts/check-imaging-dependencies.py`: `casa-imaging-model` has no workspace dependencies; the operator and deconvolution depend on the model, `casa-fft` and `casa-numerics`; reconstruction and products depend inward on the model and domain-neutral numerics, with products also depending on reconstruction completions; runtime composes the operator, reconstruction and products; the Metal crate depends only on the operator |
| imaging application composition (`casa-imaging-application`) | Sole production composition seam across MeasurementSet authority, reconstruction, products, resources, execution, typed installed-implementation availability, and CASA product publication | Native imaging owners only; unavailable requests invoke no execution implementation |
| domain libraries (`casa-ms`, `casa-simulation-synthesis`, `casa-lattices`, `casa-coordinates`, `casa-images`, `casa-calibration`, `casa-vla`) | Higher-level astronomy data models and algorithms built on table/image persistence; simulation synthesis owns only the serial model predictor and Airy voltage pattern used by MeasurementSet simulation | foundation crates, `casa-tables`, selected peer domain crates where documented |
| boundary contracts (`casa-provider-contracts`, `casars-imagebrowser-protocol`, `casars-tablebrowser-protocol`) | The generic provider envelope, canonical parameter and application catalogs, task/session surface definitions, and protocol surfaces between providers, apps, and Python/runtime layers | domain libraries and foundation crates; must not become a second source of truth |
| parameter and task runtime (`casa-task-runtime`) | Format-neutral parameter resolution, sparse TOML profiles, migrations, typed task/session lifecycle coordination, managed Last storage, and the common one-shot task CLI host | boundary contracts and `casa-types`; must not implement provider science behavior |
| notebook runtime (`casa-notebook`) | Source-preserving Markdown/cell parsing, stable notebook/cell/run identity, atomic project persistence, immutable execution receipts, conflict handling, and portable/advanced exports | parameter value serialization and general-purpose ecosystem crates; must not own provider execution or frontend state |
| apps and runtimes (`casars`, `casars-imager`, `casars-python`, `casars-frontend-services`, `ratatui-graphics`, `apps/casars-mac`) | Terminal shells, orchestration binaries, Python bindings/package, frontend service bindings, rendering/runtime support, and the native macOS GUI prototype | boundary contracts, domain libraries, foundation crates; lightweight frontend services may expose read-only domain-library probes through UniFFI |
| test support (`casa-test-support`) | Cross-language parity harnesses, fixtures, integration helpers, and performance guards | any workspace crates needed for testing only |

## Dependency direction

Preferred direction is:

`core codecs -> foundations -> persistent storage / domain libraries -> boundary contracts -> parameter runtime / notebook runtime -> apps/runtimes`

Native imaging follows its stricter accepted direction:

`science -> observation / reconstruction -> products -> execution -> backends -> application -> frontends`

`casa-imaging-model` owns the dependency-free science, reconstruction, and
product schemas and commitments introduced by ADR-0009.
`casa-imaging-reconstruction` owns model-state algorithms and opaque
reconstruction completions and depends only inward on the model and the
domain-neutral `casa-numerics` algorithms.
Its image-response view owns the scientific mapping between raw normal state,
minor-cycle image coordinates, and physical model increments. Applications bind
the requested normalization explicitly; products reuse this mapping before
model publication and restoration without changing the raw normal state.
`casa-imaging-products` owns continuum product algorithms and may use the same
domain-neutral `casa-numerics` algorithms directly; numerical helpers do not
become reconstruction-owned merely because both owners require them.
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
model, reconstruction, deconvolution, products, the operator and the Metal
backend. Metal runs inside the pass: with
`backend = metal` every owner grids through its own
`casa_imaging_metal::MetalBackend`.

AW projection reads its convolution functions from one catalog of CASA
`CFS_*`/`WTCFS_*` images (`casa_imaging_operator::aw`), opened header-first and
read cell by cell into a cache bounded by `cf_resident_mb`. The catalog is
either an existing CASA cache, read as it is and never modified, or a native
EVLA cache that casa-rs generates in the same CASA format
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
`ImagingRequest`, deserialized from the imager parameters the provider
catalog resolves and validated once. Every route (command line,
`--json-run`, TUI, Python and the workbench) reaches it through the same
catalog resolution, so the catalog is the only source of defaults and refuses
a parameter its gridder does not read. `casars-imager` is a thin frontend over
this interface: it resolves its parameters through the catalog and presents
the result. Unsupported capabilities remain typed unavailable until their
ticket adds one final-owner implementation.

The remaining programme introduces the product owner only in the ticket that
can migrate all callers and enforce its exact dependencies.
The target tranche split and corrected T13-T23 order are recorded in
[`docs/imaging-architecture/lessons-and-next-tranche.md`](docs/imaging-architecture/lessons-and-next-tranche.md).
Until those crates land, this package table remains descriptive: no branch may
import an anticipated interface from another ticket or treat composition WIP
as a dependency.

with `casa-test-support` outside the product dependency chain.

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
  generations or identities). A held lock is refused at once (one attempt,
  where casacore by default waits); on a file system without lock support
  (`ENOLCK`, or `ENOTSUP` on macOS SMB) tables are used unlocked with a
  warning, as casacore does for `ENOLCK`. General rollback, snapshots,
  journaling, and copy-on-write generations are not part of the persistence
  contract.
- Versioned provider bundles are boundary contracts; UI projections are derived
  views, not separate truth sources.
- `casa-provider-contracts::ApplicationCatalog` is the sole application
  inventory and launch-metadata owner. TUI, Swift, Python, project MCP,
  assistant, packaging, and generators project it directly. Installed-suite
  and development-workspace launch modes resolve exact paths and never fall
  back to each other, PATH discovery, or repository probing.
- Parameter concepts live in the checked aggregate `ParameterCatalog` in
  `casa-provider-contracts`. Each provider bundle embeds the exact referenced
  concepts so the boundary remains self-contained. Task and session
  `SurfaceDefinition` bindings supply defaults, conditional activation,
  narrowing refinements, optional ordered migrations, presentation, and
  projection metadata; under ADR-0012, an empty migration set declares the
  surface current-only. Bindings cannot redefine concept meaning, normalization,
  units, role, or persistence. Frontends may not redefine those semantics
  locally.
- `casa-task-runtime` owns profile mechanics, managed state, and application
  lifecycle transitions from source parsing and resolution through Last/
  LastSuccessful persistence, task completion, and session debounce/coalescing.
  It also owns task-provider discovery actions, JSON source loading,
  diagnostics, serialization, exit classification, and the generated common
  help block. Providers retain typed science adapters, domain-specific human
  parsing, and session command/event semantics; apps may not add alternate
  lifecycle maps, timers, writers, CLI hosts, or launch fallbacks.
- `apps/casars-mac` keeps fixture schemas inside its SwiftPM core when modeling
  proposed UI behavior. Real, read-only dataset discovery enters through
  `casars-frontend-services`, whose Rust API is exposed to Swift and Python
  with UniFFI.
- `casars-frontend-services` is an apps/runtime boundary crate. It may compose
  domain-library reads and `casa-notebook` operations into GUI-appropriate
  projections, but it must not become a second implementation of persistence,
  task semantics, or provider contracts.
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
locator, the canonical field, UV-distance and intent predicate, the DDID,
SPW/channel and correlation selection, the selected data, flag and weight
columns, and whether MAIN has `CORRECTED_DATA`. Selected MAIN rows are kept as
the MAIN row count, the selected row count and the used-DDID set, not as a
row list. Neither the snapshot nor its sources carry an identity. Each pass
re-evaluates the row predicate in physical MAIN order and keeps only the
current bounded block, so a sparse selection reads but never materializes the
intervening rows.

`casa-ms` owns the bounded Selected Observation read path. A run opens one
`BoundSelectedObservation`, which holds a casacore read lock on each source's
tables. Each pass turns it into one block stream (`into_block_stream`):
`SelectedObservationBlockSource::fill_next` walks MAIN in physical order,
applies the row predicate, and fills one reusable block with consecutive
selected rows of one DDID: it keeps the row metadata the walk read, reads
their visibility, flag and weight columns over the selected channel span and
evaluates each row's geometry. `complete` refuses a stream that is
not exhausted and returns the access for the next pass. For a channel-local
cube with a linear spectral WCS and no continuum transform, a wave that covers
only part of the cube and need not read whole rows streams through
`into_windowed_block_stream` instead: it reads only the channels whose
output-frame frequencies reach the wave, keeping the straddling channel at
each edge, and skips a block that reaches none.

Each source's content plan prices the retained table
metadata, geometry engine, selection, predicate and coordinate catalogs, the
Measures provider and ephemeris (charged once, to the first source), the
POINTING catalog or AW pointing-epoch plan when the geometry needs one, one
block over the selected channel span, and `CONSTRUCTION_SLACK_BYTES` for
construction scratch. The block's row count is the largest whose envelope fits
the source budget.

The application passes a Measures provider in the
`SelectedObservationResolutionRequest`; it supplies EOP values, TAI-UTC, IGRF
coefficients, observatory positions, named-source directions and rest-line
frequencies. Resolution wraps it in a `SelectedObservationMeasures`, whose
constructor calls the provider's `prepare_bounded_state` (for
`MeasuresRuntime`, eager materialization of all six catalogs) and refuses a
provider that cannot report its retained bytes. The bound sources' geometry
engines receive that provider and never discover runtime data inside the
selected-observation path.

`casa-imaging-model` owns the backend-free values a block lends to its
consumers: per-row metadata, sample coordinates, per-domain UVW and
phase-shift projections, pointing directions and antenna response classes
(`SelectedObservationRunRow`, `SelectedObservationRunChannel`), and the
borrowed visibility, flag and weight columns of a row (`SelectedNumericRow`).
`casa-ms` fills them from MAIN and the compiled geometry; the application's
`MeasurementSetSource` converts each block into the runtime's native rows.

`casa-imaging-model` carries the dependency-free model-state schemas and the
compiler-owned lifecycle commitment: the target shape, the bounds and the
arithmetic precision. `casa-imaging-reconstruction::ModelLifecycle` owns a
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
runtime plan binds it: the application resolves the selected observation once, every pass reads it
through the bounded source, and the final major-cycle pass writes the
visibilities (see Imaging execution). Conventional image members and
`MODEL_DATA` are separate side effects: failure of one never rolls back or
hides a result already completed by the other. Every product member is
generated into private staging beside its target and moved into place only
after all of them have been generated; a failure or cancellation before then
removes the staging and publishes nothing, and a failure while moving leaves an
incomplete set that must be regenerated, without rollback or a resumable
per-member recovery ledger.
`MODEL_DATA` and `CORRECTED_DATA` follow ADR-0008: the final major-cycle
pass writes selected cells in place through casa-ms's selected-visibility
writer under casacore's table lock, creating `MODEL_DATA` when MAIN lacks it,
as CASA does. Interruption may leave partial derived values, as in CASA; the
next run recomputes them. No
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
grid precision, and the host, before any phase, so a typed-unavailable request
starts no work. A run leaves
one record beside its products, the run summary of plan section 8.3
(`<imagename>.summary.json` for `casars-imager`): the request echo, each
phase with whether it completed, its wall time and the process's peak
resident memory, the worker count, the backend, the minor-cycle totals, the
product list, and the error of a failed run. The application writes it when
the run completes or fails (ADR-0014's final success/failure summary); a
cancelled run writes nothing.

## Runtime model

Most crates are synchronous Rust libraries with CLI/TUI frontends and test
harnesses on top. There is no repo-wide async runtime contract today.
Long-running interoperability, parity, and packaging work is driven by
shell/Python scripts, integration tests, or subprocess orchestration rather
than a shared background service model.

ADR-0006 adds one synchronous parameter lifecycle shared by task and
browser-session consumers. `casa-task-runtime` is the sole lifecycle owner: a
task resolves sparse user intent, records its attempted state, and applies the
completion transition around one provider invocation; a browser session
resolves durable startup settings and delegates accepted-setting debounce and
coalescing to the same runtime. The subsequent command/event stream remains
owned by the session protocol. Parameter resolution and Last persistence do
not introduce a provider daemon or repo-wide async runtime.

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
conflict, export, receipt-v2 Python input/environment, and immutable explorer
visualization-revision contracts shared by GUI, TUI, CLI, and Python. App surfaces
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

Wave 2 adds one persistent, visible, interruptible Python subprocess per open
notebook. Swift supervises its CASA-RS JSONL protocol and owns interrupt,
terminate/kill, restart, Run All, and explicit project-environment actions;
`casa-notebook` owns the durable execution evidence. Wave 4 coding-agent Python
uses the user-selected or inherited scientific environment under the active
agent authority preset; it is not forced into a separate fixed worker.

Renderer-neutral MeasurementSet plot data is owned by `casa-ms`; UniFFI and
PyO3 project that same Rust structure. `casars-python` adds NumPy-native MS
series and image-plane/WCS records, with Matplotlib and Astropy confined to the
optional `plot` extra. Swift explorer fixtures and frontend DTOs are not
persisted contracts. Explicit MS/image snapshots are copied and versioned by
`casa-notebook`; canonical typed explorer parameters are retained solely as
reopen intent, never inserted as input forms or live links in Markdown.

`casa-notebook` owns portable tutorial-template v1, immutable template forking,
the versioned URI-handler registry, exact acquisition approvals, integrity and
bounded extraction, and `.casa-rs/tutorials/<notebook-id>/lock.toml`.
`casars-frontend-services` projects those Rust contracts as JSON through
UniFFI; Swift owns interaction and asynchronous orchestration only. The
package-internal Wave 3 prototype remains deterministic review state and never
becomes a persisted or public contract. `tutorial-pack.v0` has no runtime
reader or GUI state; an explicit Rust one-shot migrator converts its prose,
native GUI task steps, and regression overlay into v1.

Wave 4 replaces the bespoke model sidecar with a user-installed coding agent.
A CASA-owned agent-session interface contains the runtime-specific shapes. The
initial adapter spawns the official Codex App Server directly and speaks its
JSON-RPC protocol over stdio; a future ACP adapter is the extension point for
OpenCode and other agents. The metered OpenAI Responses API and Agents SDK are
not initial backends. The Codex adapter invokes ChatGPT subscription login and
account state without copying credentials into CASA projects or processes.
Raw JSON-RPC, method names, IDs, and trusted tool-result decoding stop inside
that private adapter. One typed request tracker resolves outbound lifecycles,
and one assistant controller owns transient state, event reduction, timers,
and host-effect requests outside the general Workbench store.
The native interaction keeps model, reasoning effort, and subscription usage
remaining immediately visible. Agent/account, authority, and Python selection
are consolidated behind one secondary settings surface; AI invocation and
AI-suggested state use purple consistently, apart from safety-severity colors.

`casa-rs-agent-profile/v1` defines invariant guidance, a bundled CASA skill,
the verified project MCP identity, backend resume metadata, an agent-neutral
authority vector, and per-adapter capability declarations. **Explore**,
**Work**, and **Full access** are GUI projections of that vector, not Codex or
ACP modes. Explore launches from a neutral directory with project instructions
disabled. Work uses the trusted project and the user's normal shell/Python
environment with native Codex approvals. Full access is an explicit visible
expert opt-in. Behavioral conformance verifies actual denial/escalation,
profile/MCP identity, cancellation, and resume rather than merely checking a
capability list.

The project-scoped CASA MCP server exposes typed open-tab state, task schemas
and parameters, persistent-data semantics, receipts, typed task suggestions,
host-action descriptions, and cited corpus/source retrieval. Its unique
nonce-derived session name, host-owned executable registration, and nonce on
every tool call prevent a user-configured server from shadowing it. Generic
command, file, network, and Python approval stays with
App Server. CASA owns only canonical semantic actions such as notebook append,
task Run, typed data mutation, and tutorial acquisition, avoiding duplicate
prompts. An explicit **Add to notebook** click is itself sufficient authority
for one idempotent append at the chronological tail; it does not trigger a
second confirmation.
One typed tool registry binds schema, argument decoding, context requirement,
and dispatch. Nonce authentication occurs once before typed handlers delegate
catalog and parameter behavior to canonical owners and corpus retrieval to
`casa-notebook`.

`casa-notebook` continues to own durable agent-neutral visible conversations,
citations, immutable pins, context-use records, and scientific receipts.
Hidden reasoning and raw App Server/ACP envelopes are not persisted. A backend
session is resumed only after the authority vector, profile, capabilities, and
CASA MCP registration are reverified; otherwise CASA records a visible handoff
to a new session.

The CASA-RS-owned corpus combines a redistribution-cleared baseline, user
project documents, release source/docs, and an optional commit-keyed live
overlay; it never depends on a separate Radio Astronomy Oracle checkout.
SQLite/FTS5 is the initial replaceable retrieval implementation. The removed
384-dimensional feature hash is not an embedding; a real local embedding model
requires retrieval-evaluation evidence. "Full context" means the agent can
query complete typed semantic projections and retrieval tools as needed, not
that raw arrays or entire corpora are copied into every prompt. CASA records
used domain tools/resources and citations but does not claim an exact model-
egress manifest for a coding agent with shell and filesystem authority. See
`docs/assistant-security.md` for the executable runtime and authority contract.
Context projection and corpus-result capacity come from one deterministic
resource plan using backend-reported model capacity, output and conversation
reserves, selected-tab priority, and checked UTF-8-unit arithmetic. Missing
capacity disables both allocations explicitly; there is no fixed fallback.

Project-document maintenance is host-notified but database-correct: recursive
macOS filesystem events are debounced hints, while a complete metadata-only
inventory and SQLite-owned fingerprints decide what changed. Fingerprints bind
the relative path, type, size, mtime, ctime, and filesystem identity so atomic
replacement and preserved-mtime edits are detected. Only changed sources are
read or passed through PDF extraction/OCR. The source snapshot atomically
removes deleted or renamed documents; failed or concurrently changing sources
retain their last valid indexed content and remain scheduled for retry. Project
watch events never refresh the independent baseline or source-code layers, and
there is no periodic full-content scan.
Each refresh first prepares an immutable reconciliation carrying the complete
validated source inventory, its digest, scope, and generation. Host extraction
returns one typed outcome for every requested path, and Rust validates that
exact prepared value before the single atomic apply. A Swift coordinator owns
coalescing and rejects stale generations.

The baseline radio-astronomy layer is a versioned `casars-mac` app resource,
installed once rather than copied into projects. Its schema-v3 manifest binds
each compact page/slide source to an authoritative origin, source and content
digests, license metadata, redistribution basis, and exact citation kind. The
runtime accepts only the current normalized-page representation and verifies
content digests before indexing. Baseline replacement removes
only the baseline layer, preserving project documents and conversations. See
`docs/assistant-standard-corpus.md` for the selected sources, maintenance
workflow, and measured cost.

Every notebook-program wave starts with a launchable deterministic GUI
prototype and an explicit approval gate before real adapters are connected.
The prototype state belongs in `CasarsMacCore`; it may not establish persisted
or provider semantics that bypass the Rust-owned contracts.

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
its worker team and its memory, and each phase admits the bytes its owners
report (`admit`: the source envelope, the paged cube cache, one wave of each
pass, the product writer) and holds them in a `Reservation` until it ends; a
phase that does not fit is refused, typed, before it starts. Frontends neither
calculate science nor inspect execution devices.
Reusable buffers may reduce allocation churn but do not form a second memory
budget or admission authority.

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

Imager task protocol v12 carries the resolved imager parameters by catalog
name, on stdin for `--json-run`; the result echoes them. The imager emits no
progress events; the cycle loop logs worker counts and stage timings through
`tracing`, and the run summary records each phase. `parallel=false` (the
default) runs the pass with one worker. All production FFTs use FFTW; there is
no FFT backend selector or fallback.

W-projection, AW-projection and mosaics run on the pass. Facets,
`mtmfs` via cube and cubic spectral interpolation are not in the catalog, so no
request names them; a compiled problem with faceted geometry is typed
unavailability in `availability::check` (facets #664, cubic #42).

`backend = metal` (macOS, a unified-memory Metal 3 device) grids every pass on
the Metal device: `casa-imaging-metal` implements the operator's
`GridBackend` with support-generic kernels (one SIMD group per sample, `f32`
atomic adds) into accumulators whose cells live in shared device memory
(`GridStorage::Device`). The host locates every sample with the operator's
rounding rule and accumulates `sumwt` in `f64`, so tap selection and `sumwt`
equal the CPU's; every dispatch completes inside `apply`, cut into
sub-blocks over a three-slot ring. Operators are `f32` for every basis on
Metal (D2). Placement and the native-channel predictions of multi-domain or
linearly interpolated residuals stay on the CPU workers.
Metal grids the standard kernel set only; W projection, mosaics and AW
projection run on the CPU, and `availability::check` refuses them on Metal.
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
- accepted future notebook state from ADR-0007: visible Markdown and assets
  under `notebooks/`, copied project documents under `documents/`, and versioned
  managed receipts, transcripts, tutorial locks, Python environments, and local
  corpus indexes under `.casa-rs/`

## Public interfaces

- published Rust library crates, especially `casa-types`, `casa-tables`, `casa-ms`, `casa-lattices`, `casa-coordinates`, and `casa-images`
- CLI/TUI apps such as `casars`, `msexplore`, `tablebrowser`, `imexplore`, `calibrate`, and `importvla`
- The `casars` runtime package owns the `tablebrowser` and `imexplore` session engines,
  rendering/movie coordination, and executable targets. `casa-tables` and `casa-images` expose
  only reusable table/image domain capabilities and do not depend on browser protocol crates.
- native macOS GUI prototype package `apps/casars-mac`
- experimental UniFFI frontend service bindings generated from
  `casars-frontend-services`
- Python package `casars-python`
- persisted CASA-compatible on-disk table, image, and related data formats
- versioned provider contract bundles and protocol schemas
- versioned sparse TOML task and session parameter profiles

## Approved dependency classes

- N-dimensional arrays and numeric containers: `ndarray`
- FFT and spectral transforms: direct, in-place FFTW 3 through `casa-fft`
- error types: `thiserror`
- terminal rendering and TUI support: `ratatui`, `ratatui-graphics`, `plotters`
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
  native dependency sets, device APIs outside `casa-imaging-metal`, and
  environment reads, `eprintln!` and content hashing in imaging crates. Its
  grandfathered-file lists only shrink as tickets IF-1 to IF-9 delete the
  listed files.
- The imaging foundation refactor (#648, ADR-0016) is in progress; the plan in
  `docs/imaging-architecture/imaging-foundation-plan-20261007.md` is the
  target.

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
| 0008 | Casacore storage and bounded MeasurementSet writes | accepted |
| 0009 | Mathematical imaging architecture | accepted |
| 0010 | Unified imaging resource authority | superseded |
| 0011 | Distinct sequential and joint continuum-line reconstruction | accepted |
| 0012 | Current-only sparse profile contracts | accepted |
| 0013 | Non-cryptographic integrity for private run-scoped spill artifacts | accepted |
| 0014 | Trusted product generation without content attestation | accepted |
| 0015 | Run-local imaging ownership without content attestation | accepted |
| 0016 | Imaging foundation | accepted |
