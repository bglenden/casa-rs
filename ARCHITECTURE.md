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
| native imaging contracts (`casa-imaging-model`, `casa-imaging-reconstruction`, `casa-imaging-products`, `casa-imaging-runtime`) | Dependency-free logical schemas and commitments; authoritative model-state ingest, reprojection, delta, and completion algorithms; continuum product algorithms and the Product Generation Authority with planned generations, artifact identities, seals, and independently atomic member publication; process-level resource topology, policies, demand envelopes, arbitration, and leases | `casa-imaging-model` has no workspace dependencies; `casa-imaging-reconstruction` and `casa-imaging-products` depend inward on the model and domain-neutral numerics, with products also depending on reconstruction completions; `casa-imaging-runtime` depends on the model and the reconstruction-owned executable-problem brand at its execution boundary and composes product authority at its publication boundary |
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
`casa-imaging-products` owns continuum product algorithms and may use the same
domain-neutral `casa-numerics` algorithms directly; numerical helpers do not
become reconstruction-owned merely because both owners require them.
`casa-imaging-runtime` owns execution-resource contracts introduced by ADR-0010
and depends inward on the model plus reconstruction's opaque executable-problem
brand. That reconstruction edge is limited to admitting owner-prepared model
inputs at the execution and receipt boundary; runtime does not own or invoke
reprojection algorithms. Runtime also
owns content-addressed prepared implementation artifacts: an exact artifact
identity, a separate cache identity, bounded generation or load, integrity
validation, private atomic caching, and deterministic eviction. Artifact
identity commits to the registry/catalog owner, implementation version,
compiled scientific commitments, artifact kind, and canonical named layout;
`CacheIdentity` separately commits to the canonical private root and full cache
policy. Neither identity includes an execution-local source path. The selected
implementation owner derives artifact identity from exact `CompiledProblem`
commitments, an owner-derived per-cell scientific key, named segment semantics,
and the registry owner; it derives cache identity from that owner, the canonical
private root, and the full cache policy. The plan-selected operation
implementation identity is domain-separated and length-prefixed; its owner
registry is checked against the actual plan and running implementation registry,
so caller-authored metadata cannot substitute a registry entry. Cold generation,
cold load, and warm reuse have distinct plan node, implementation, and
artifact-role bindings; their canonical measurements reach the immutable
execution receipt, including `RejectedStale` evidence for a missing,
incomplete, incompatible, corrupt, or nonfinite warm candidate. Rejection
evidence uses the existing typed observed-identity field and records the actual
cache operations and bytes inspected. Successful `PreparedArtifact` handles
are identity-only values with no payload access or file descriptors; the cache
node closes every descriptor before returning its measurements. Store I/O
evidence covers the private-store operation through
final validation; later consumer copies are separately owned. Opening the store
canonicalizes and checks the private root before creating anything, and performs
no hidden lookup, integrity work, or eviction. That private casa-rs cache is not
Product Graph authority or a CASA-visible persisted format. Existing CASA
CFS/WTCFS directories remain read-only interoperability inputs supplied through
the validated import adapter; the runtime neither mutates them nor creates
sidecars. Cold load accepts only bounded absolute regular-file source
descriptors through a content-committed source artifact listed in the canonical
plan, owned by an exact predecessor/import node, and retained in that
predecessor's receipt. Generation is pathless: the selected generator supplies
bytes through the store-owned bounded buffer, with zero source reads or
source-descriptor residency and a two-file-descriptor peak. Cold load opens at
most one source while the execution lease is live, rejects CASA image,
MeasurementSet, or table ancestry, verifies source digests while its lease is
live, and reserves the source-read ceiling, source-descriptor residency, and
three-file-descriptor peak. Reuse reserves two descriptors and no source reads.
Source descriptors, the canonicalized source path, and the shared streaming
buffer are charged inside one
`StorageManager`-backed physical-memory allocation rather than a duplicate
`SourceReadAhead` slot. Every `WorkImplementation` also states its failure
measurement policy explicitly; there is no default that can silently discard
completed I/O or mutation evidence.
`casa-imaging-application` owns production composition across
MeasurementSet observation authority, reconstruction, products, and physical
execution. It compiles the logical request, checks it against the implementation
installed in the build, and either invokes that implementation or returns a
typed unavailable result before planning. The repository migration matrix is
planning and acceptance evidence only and is never compiled or interpreted at
runtime. A selected production failure is terminal; there is no alternate
implementation, retry path, or stage-level delegation. `casars-imager` is a
thin frontend projection over this interface:
it owns parsing, unit and representation conversion, canonical request
construction, and result presentation only. Unsupported capabilities remain
typed unavailable until their ticket adds one final-owner implementation.

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
  from staging, while in-place changes use only an incomplete marker; general
  rollback, snapshots, journaling, and copy-on-write generations are not part
  of the persistence contract.
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
  narrowing refinements, migrations, presentation, and projection metadata;
  they cannot redefine concept meaning, normalization, units, role, or
  persistence. Frontends may not redefine those semantics locally.
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
  readable dependency policy and migration matrix under
  `resources/imaging-architecture/` are enforced by `just arch-check`.

The native interface has one `ImagingRequest` contract (version 3) and exactly
one `compile` / `plan` / `run` sequence. `compile` validates and canonicalizes
logical science, including immutable coordinate and image-domain geometry.
Compiled Geometry identity is derived only by `compile`; callers supply geometry
laws, not an identity. Observation pointing records the selected MeasurementSet
column and meaning plus timestamp, interpolation, extrapolation, and missing-row
policies without evaluating rows. Spectral geometry retains exact channel
centres and N+1 boundaries, or a linear WCS law that derives both exactly;
spectral transforms remain unevaluated. `compile_observation` is the sole
constructor of Observation Snapshot identity. It canonicalizes each resolved
MeasurementSet field/time/UV/baseline/scan/observation/intent/array predicate,
SPW/DDID/channel selection, correlation coordinate, selected data/flag/weight
column, coordinate and metadata generation, independent optional `MODEL_DATA`
state, consistency token, reference-data identity, and input-model identity.
Selected MAIN rows retain source and selected counts, the exact used-DDID set,
and the content-derived identity of the canonical ordered
`(physical row, DATA_DESC_ID)` sequence. They do not retain the row corpus.
Visibility samples, stored MAIN facts, content blocks, and execution order
remain absent. Retained access re-evaluates the row predicate in physical MAIN
order, validates the resulting sequence against that compact commitment, and
keeps only the current bounded source block; a sparse selection therefore reads
but never materializes the intervening physical row span. The snapshot is the
storage owner's logical-manifest commitment, not an intrinsic digest of all
MeasurementSet science bytes. Its identity is independent of source location
and request order, while a separate provenance identity retains both.

T17/#503 adds the native bounded Selected Observation path. `casa-ms` owns the
retained read capability, canonical one-pass traversal, per-sample validation,
and owner-minted terminal completion. Binding first reconciles the freshly
probed selected-row, column, metadata, model-column, and consistency generations
exactly with the compiled source snapshot. Its sole content plan may then read
ahead into the explicitly charged live-block count and rotates those blocks in
canonical consumer order; block size, read-ahead, and buffer slot are absent
from content identity. The source budget charges the full retained lazy,
read-locked MeasurementSet/Table object graph (schemas, keywords, column
keywords, data-manager descriptors, paths, and lock state), the shared immutable
selection and generation manifests, retained geometry and selected-coordinate
catalogs, coordinate-construction and digest scratch, request and retained
row-index vectors, POINTING queries/candidates/brackets/outputs, one-at-a-time
variable direction-reference cell scratch, transient column-accessor names,
and every simultaneously live content block before constructing the geometry
engine or reading content.
The canonical owner measures the allocated `Vec<BoundObservationSource>`
capacity and charges its complete slot payload exactly once to the first
source. That payload includes every inline identity, `MeasurementSet`, geometry
engine, predicate and coordinate header, content plan, row-count flag, and
padding; nested projections therefore charge only their heap allocations.
While that owner is being built, it projects the complete live
`Vec<ObservationSourceBinding>` graph before removing any binding. The
first-source initialization peak charges the outer allocation at its actual
capacity, every binding-owned `SourceGenerations` vector capacity, and each
selected-row Arc allocation not already shared with a compiled source or an
earlier binding. Outer slots already contain the inline source state, so
per-source equality validation is allocation-free and does not recharge inline
state or its manifests. The whole graph is charged once and is absent from
retained residency after construction.
Retained geometry and coordinate arrays use fixed boxed slices, including the
mount-flag slice, so their payload is the exact immutable element count rather
than a guessed collection capacity.
Direction-reference evaluation borrows fixed and integer-mapped MEASINFO rather
than cloning its retained `TabRefTypes`/`TabRefCodes` payload. The application
also injects one explicitly acquired `SelectedObservationMeasures` capability.
Its constructor accepts only the provider: the provider's prepared state
supplies the inseparable authoritative identity, so a caller cannot relabel a
foreign provider with the compiled Measures identity. `MeasuresRuntime`
eagerly materializes all six immutable catalogs and derives that identity from
their canonical scientific contents and snapshot provenance. Bounded binding
rejects missing, foreign, opaque, or changed provider state; charges the
provider's alignment-aware shared allocation and catalog residency once across
the canonical source set; and rechecks both identity and residency before
traversal and terminal completion. Geometry engines receive only that injected
provider state and never discover runtime data inside the selected-observation
path.
`casa-imaging-model` owns the closed backend-free sample schema,
compiler commitment, a closed validation pass, and a distinct content-derived
generation computed from the actual canonical sample stream. Its affine
incremental inspection cursor is an opaque workspace coordination type: only a
compiled problem can construct it, its fields and generation encoder remain
private, and applications and frontends cannot mint or complete one. This lets
`casa-ms` validate bounded blocks without moving model semantics into the
storage owner. The storage
completion binds logical snapshot identity, provenance, physical access,
content generation, and retained-access traversal. After synchronous work
completion and every declared fence, if any, of the owning typed observation-read node
settles, the runtime supplies a fresh, affine authority that binds that opaque
storage completion to the execution attempt, owning node, exact settled fence
set, and live lease epoch. A physical I/O fence alone cannot mint this proof.
Equal science content may share a content generation while remaining distinct
logical source and access commitments; prediction destination is provenance and
therefore cannot split prediction and residual views of the same content
generation. The displaced `casars-imager`
`MsSelection`/`resolve_selection` preparation route is removed:
its frontend projection delegates to the same bounded `casa-ms` predicate as
native Selected Observation access.

`casa-imaging-model` carries the dependency-free model-state schemas and the
compiler-owned lifecycle commitment introduced by T28/#514.
`casa-imaging-reconstruction::ModelLifecycle` is the solver-independent owner
of model generations, ingest, reprojection, deltas, and affine final-model
completion. The Compiled Problem commitment binds the sole target shape,
observation and initial-model identities, typed WCS/basis/polarization source
provenance, an opaque owner-derived reprojection commitment, exact bounds,
Product Graph, Numerics Contract, selected conversion precision, canonical
finite-`f64` state encoding, and explicit validity semantics. Reconstruction
alone prepares the target-ordered samples; the model contract derives and
validates the preparation, mapping, support, and proof identities atomically.
Another Product or Numerics Contract is rejected.
Execution receipts preserve the lifecycle and compiler-owned reprojection
identities, bind the Product Graph and Numerics identities and conversion
precision, and carry the five closed direction/basis/polarization/support-policy
tags. Receipt validation canonically recomputes the lifecycle and input proof
from its typed target, bounds, precision, support, and input projections, rejects
zero sentinels, and cross-checks duplicate audit fields; those fields never
grant execution authority. Aligned ingest is a fallible one-pass stream
whose terminal source errors remain source errors. Reconstruction-derived
target-ordered reprojection accepts only the closed direction, spectral-basis,
and real parallel-hand polarization conversions owned by reconstruction: a
source reader supplies typed geometry and samples but cannot supply mappings or
fallbacks. The owner derives stencils, projected support, and exact evidence
with only the target generation plus the current bounded stencil resident.
Invalid support never aliases numeric zero; any invalid non-zero contributor
invalidates its target, and uncovered targets remain invalid. Sparse deltas
must be non-empty, non-zero, canonical, base-bound, within valid support and
explicit bounds, and are applied in place only by the private authority seal
that minted them. Resume preserves the exact generation named by the compiled
input, after
which new deltas bind the current attempt and epoch. The sole final affine
operation consumes its private finalization authority and returns the next
generation plus a distinct opaque reconstruction completion, never a Product
Generation seal. Major Cycle, Minor Cycle, and Product Generation owners may
consume this surface but cannot construct raw generations. No
MeasurementSet/image persistence, runner, or model-column writer changes in
T28.

The Compiled Problem also owns one compiler-derived Observation Transaction.
Its canonical read set contains every consumed per-MS selection, selected MAIN
column generation, metadata generation, and consistency token. Optional
`MODEL_DATA` writes carry exact selected-cell scope plus an absent-or-generation
precondition captured independently of whether `MODEL_DATA` was read. Every
`PhysicalWorkBinding` passed to the sole `plan` entrypoint must type every
MeasurementSet source operation as `ObservationRead`; a terminal replay that
also writes bounded predictions in place to `MODEL_DATA` is
typed `ObservationReadWriteback`. Each transaction declares final
complete-data reconciliation, private per-product staging events, and the
applicable product Publication nodes. `plan`
mechanically derives every
observation-read terminal event and binds the declaration against the exact
`CompiledProblem`; the crate-private binder rejects untyped lock-bearing I/O,
product keys that differ from `ProductRequirements`, missing completion
fences, any post-commit work, or any bypass publication. Conventional image
members and `MODEL_DATA` are separate side effects: failure of one never rolls
back or hides a result already completed by the other. The sealed plan retains the
compiled-problem, logical-transaction, and physical-work identities, and its
versioned identity includes the complete transaction declaration. The initial
consistency check precedes every observation read; every read precedes
reconciliation; and each terminal commit waits for every node and fence in its
own transaction. Controller polling ends when that transaction's Publication launches.
Initial-check, observation-read, and writeback nodes reserve one table lock
per source; every read revalidates under those locks. Staging storage,
writeback/publication buffers, and commit fences are ordinary Resource Authority
claims. Product Publication activates exactly one conventional product member.
`MODEL_DATA` instead follows ADR-0008: the terminal replay writes selected cells
in place under the retained lock and a small incomplete-write marker, then
updates the owner generation and removes the marker only after a successful
flush. Interruption may leave partial derived values, as in CASA, but the marker
makes that state fail closed until explicit recovery or recomputation. No
backup column, full-column staging copy, content digest, rollback, snapshot, or
copy-on-write generation is part of this path. Users may retain or delete
conventional products independently; `MODEL_DATA` remains a distinct
MeasurementSet side effect.

The Observation Transaction contract and binder do not themselves change
`casa-ms`, casacore metadata, or persistent bytes. A storage adapter that
implements the declared `MODEL_DATA` commit is the interoperability boundary
and must pass the applicable Rust/C++ RR, RC, CR, and CC matrix before it can
be selected by a production plan.

`plan` is the only transaction-sealing path. It gives every structurally valid
planner candidate to Resource Authority, which selects the first capable
candidate feasible under the current topology, policy, pressure, and
reservations. The provisional selection lease is released before `plan` seals
that one candidate and its transaction declaration to the exact compiled
problem and
geometry, the complete Observation Snapshot, Numerics Contract,
implementation-registry snapshot, Resource Policy, and reviewed planner
cost-model profile. The plan owns the complete immutable physical work DAG:
explicit nodes and dependencies, per-node implementation identities, resource
claims, logical allocation lifetimes, compatible reusable physical slots,
asynchronous fences, quiescence points, and pre-authorized adaptations. `run`
validates the bindings and registry snapshot, reacquires the selected plan's
sole Resource Authority lease, then privately drives every node and fence to a
terminal outcome. Registry adapters execute only one exact scheduled node and
receive only scheduler-issued, lease-epoch-bound resource and allocation
capabilities for that call. Fence callbacks receive a freshly narrowed context
containing only claims and allocations still live through that exact fence;
work-scoped capabilities are never replayed after synchronous completion. No
whole-plan executor or public scheduler can bypass the compile/plan/run seam.
The transaction's initial consistency node alone receives the expected
observation transaction, typed observation reads receive its exact read set,
and the private writeback node receives its exact write set. The terminal
combined replay performs any requested bounded `MODEL_DATA` mutation directly;
there is no later model-column publication plan. Conventional product
publication remains independently planned and receipted.
Controllers can observe only plan-listed transitions eligible at the current
global cut. Cancellation, rejected directives, and adapter errors drain every
launched fence before `run` returns; mapped pages and storage-manager state also
execute their plan-owned terminal release nodes in dependency order. If release
cannot be established, the scheduler drains all other work and fences, then
retains only each failed physical-slot reservation fail-closed.
Typed I/O-buffer ceilings bound concurrent logical activity while MemoryDemand
and plan-owned physical slots are the sole physical-byte charge, allowing
compatible buffers from disjoint processing segments to reuse storage.
Execution receipts retain the plan and outcome evidence defined by the runtime
receipt schema; they do not embed the repository migration-matrix schema,
contract revision, migration disposition, or row ledger. Installed-
implementation availability is an application-owned pre-plan result, so a
typed-unavailable request invokes no runtime and creates no execution receipt.
Stores opened on the same canonical receipt root share one process-wide mutation lock and
must agree on one registered retention policy, so pruning and persistence
enforce one aggregate retention ceiling.
Receipt schema 7 stores the Product Graph identity, schema, complete node
ordinals, and exact atomic-publication member ordinals as a closed typed
projection; publication validation never reconstructs authority from the open
Compiled Problem audit field map. This T13 receipt projection and the version-3
request/identity ratchets are CASA-RS control and evidence schemas only. They do
not change casacore MeasurementSet or image-table columns, keywords, data
managers, bytes, or on-disk identities. T13's focused gates therefore do not
claim the programme's final Rust/C++ persistent-interoperability evidence,
which remains required after the production storage adapters are integrated.
Before the sole external publication operation, the runtime durably records a
non-prunable `PublicationPrepared` receipt with exact Staged outputs and
pre-syncs its terminal candidate. Prepared and terminal bytes are charged
together, and the shared-root mutation guard remains held through publication
and promotion. Publication success is the final runtime result:
terminal-candidate promotion cannot turn visible output into a failed run, and
a failed promotion leaves the prepared receipt for fail-closed reconciliation
without republishing. Receipt-owned staging files use a closed name and are
removed and directory-synced under that same guard when a store reopens after
an interrupted process.

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

`casars-imager` owns only user-facing parsing, unit and representation
conversion, canonical task-request projection, protocol telemetry, and result
presentation. `casa-imaging-application` owns MeasurementSet expression
resolution, bounded source access, installed-implementation admission, runtime policy, and
independently atomic product publication. Resolved immutable selection identity
belongs to `casa-imaging-model`'s Observation Snapshot compiler. Scientific
weighting, gridding/degridding, FFT, normalization, deconvolution, restoration,
and product meaning reside only in their declared native owners.

The application compiles one backend-independent problem, rejects unsupported
requirements before planning, and then composes the runtime plan and sole
installed executor directly. Runtime resource policy owns admitted workers,
buffers, queues, caches, storage, and device resources; frontends neither
calculate science nor inspect execution devices.
Reusable buffers may reduce allocation churn but do not form a second memory
budget or admission authority.

The app uses one shared bounded producer/consumer primitive for source
read-ahead across standard MFS, mosaic MFS replay, supported mosaic MT-MFS,
standard and mosaic cube slabs, cubedata preparation, and trace preparation.
`imaging_read_ahead_blocks` is a maximum live row-block count, not a queue-depth
request. The default is two: one producer-owned block and one consumer-owned
block. An explicit larger cap is admitted only within the assigned CPU and
memory slice. Queue capacity is `max_live_row_blocks - 2`, so the default
two-block case uses a rendezvous channel and cannot retain a third queued block.
A value of one runs synchronously. Full-slab spectral routes remain single-block
by default and accept explicit read-ahead only when the planner does not lose
plane residency or row locality. Consumer failure
sets a shared cancellation token, drops the rendezvous receiver to wake a
blocked producer, and prevents another bounded source read after the current
in-flight read; the original consumer error remains the returned context.

The bounded mosaic MT-MFS path supports one MeasurementSet, `specmode='mfs'`,
`nterms <= 2`, natural, uniform, or Briggs weighting, user masks, clean or
dirty products, and optional PB/PB-corrected products. The ordinary mosaic
gridder remains a no-W path. The EVLA AWProject specialization additionally
consumes a validated CASA CF cache, aligned RR/LL hands, POINTING-derived phase
gradients, wideband conjugate-frequency selection, A/W terms, and its distinct
imaging, PSF, and weight projections. Each weighting, initial-dirty, and
residual-refresh pass replays the same bounded row stream; Briggs density uses
a raw-UVW sidecar so CASA's density cell conventions remain independent of
mosaic projection coordinates. Start-model, outlier, multi-MS, and higher-term
combinations outside those admitted slices still reject during planning.

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
that route. Dirty cubes may therefore execute as multiple bounded slabs. Cube
CLEAN is the deliberate exception: synchronized minor/major-cycle control must
retain every nonblank plane state together, so planning admits it only when one
planner-charged shape holds all output planes. A requested CLEAN that would
require multiple slabs fails during planning instead of accumulating uncharged
plane state across slabs.

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
is truncated to an integer. Auto plans scale their quadratic W coordinates to
that safety-expanded observed W range. Explicit `wprojplanes` remains an
accuracy/cost choice and, matching CASA's explicit-plane path, spans the
cell-size-derived W range instead of the selected-data range.

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
identity remains fixed by the metadata group. Ordinary mosaic MT-MFS processes
one metadata group and bounded compaction chunk at a time. Eligible Metal
AWProject MT-MFS clean runs instead compile each admitted source-order segment
once after bounded effective-support specialization. The compiler preserves
the original sample/role order, records the final group ordinal for each role,
and persists the sorted group plans and grouped tile route. Major-cycle replay
then performs only the exact-order f64 residual reduction, one f32 narrowing,
and the unchanged grouped Metal dispatch; it does not rebuild grouping, sorting,
or route topology. The private spill is integrity-checked, unlinked, and owned
only for the lifetime of the imaging execution. It is an internal execution
representation, not a reusable public cache format.

The frontend derives requested scratch from image cells, Taylor plane count,
and planned workers, then caps it by the run-level memory target after fixed
products, caches, and one source row block are reserved. For grouped AWProject
replay it separately admits persistent residual-stage replay bytes and
capture-time compiler bytes from the initial-grid lifetime ledger. Segment size
is the remaining compiler headroom after persistent replay retention, bounded
between 512 MiB and 8 GiB; allocator uncertainty is charged explicitly. The
spill directory is the output image directory, so the task's output-volume
choice also owns temporary replay I/O. Existing memory-target and
memory-pressure parameters govern admission; grouped replay is not a separate
user-facing science parameter. AWProject charges its full-cell LRU and compact
tap ceiling as independent allocations; neither may consume the remaining
safety reserve. The core reduces worker count when a support-sized tile cannot
fit, subtracts exact worker-tile storage, and converts the remainder into a
raw-sample limit from the actual compact record layout and geometry-derived
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
- FFT and spectral transforms: `rustfft`
- error types: `thiserror`
- terminal rendering and TUI support: `ratatui`, `ratatui-graphics`, `plotters`
- Adding a second library in the same category requires review.

## Known constraints

- On-disk interoperability with casacore-compatible formats is more important than mirroring C++ APIs directly.
- Heavy CASA parity suites must stay opt-in rather than in the default `cargo test --workspace` path.
- Some cross-language and parity tests must skip cleanly when `pkg-config casacore` or measures data are unavailable.
- GitHub issues and pull requests are the authoritative work record.

## Known current gaps / debt

- GitHub Project/issue adoption is now the planning source of truth, but older `docs/Planning/` material still exists and may need incremental retirement or summarization.
- `just` provides a stable command vocabulary, but some contributors may still use the underlying `cargo` and `scripts/*` commands directly until it is installed locally.
- Imaging capabilities whose authoritative tickets have not landed are
  `TemporarilyUnavailable`; `casa-imaging-application` returns typed installed-
  implementation unavailability before planning, and production never enters a
  displaced implementation. `just arch-check` validates the repository
  migration ledger and rejects runtime migration-matrix interpretation; it also
  rejects unclassified workspace packages, non-exact native dependency sets,
  forbidden Rust/Swift source imports, and unapproved dependency exceptions.

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
| 0010 | Unified imaging resource authority | accepted |
