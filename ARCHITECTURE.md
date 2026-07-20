# Architecture

Truth class: current descriptive
Last reality check: 2026-07-19
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
| persistent storage (`casa-tables`) | CASA table persistence, codecs, data managers/storage backends, schema/mutation APIs, and TaQL engine | core codecs, foundation crates |
| domain libraries (`casa-ms`, `casa-lattices`, `casa-coordinates`, `casa-images`, `casa-imaging`, `casa-calibration`, `casa-vla`) | Higher-level astronomy data models and algorithms built on table/image persistence | foundation crates, `casa-tables`, selected peer domain crates where documented |
| boundary contracts (`casa-provider-contracts`, `casars-imagebrowser-protocol`, `casars-tablebrowser-protocol`) | The generic provider envelope, canonical parameter and application catalogs, task/session surface definitions, and protocol surfaces between providers, apps, and Python/runtime layers | domain libraries and foundation crates; must not become a second source of truth |
| parameter and task runtime (`casa-task-runtime`) | Format-neutral parameter resolution, sparse TOML profiles, migrations, typed task/session lifecycle coordination, managed Last storage, and the common one-shot task CLI host | boundary contracts and `casa-types`; must not implement provider science behavior |
| notebook runtime (`casa-notebook`) | Source-preserving Markdown/cell parsing, stable notebook/cell/run identity, atomic project persistence, immutable execution receipts, conflict handling, and portable/advanced exports | parameter value serialization and general-purpose ecosystem crates; must not own provider execution or frontend state |
| apps and runtimes (`casars`, `casars-imager`, `casars-python`, `casars-frontend-services`, `ratatui-graphics`, `apps/casars-mac`) | Terminal shells, orchestration binaries, Python bindings/package, frontend service bindings, rendering/runtime support, and the native macOS GUI prototype | boundary contracts, domain libraries, foundation crates; lightweight frontend services may expose read-only domain-library probes through UniFFI |
| test support (`casa-test-support`) | Cross-language parity harnesses, fixtures, integration helpers, and performance guards | any workspace crates needed for testing only |

## Dependency direction

Preferred direction is:

`core codecs -> foundations -> persistent storage / domain libraries -> boundary contracts -> parameter runtime / notebook runtime -> apps/runtimes`

with `casa-test-support` outside the product dependency chain.

Additional constraints:

- `casa-values` and `casa-aipsio` stay internal implementation crates.
- `casa-aipsio` owns the single framed and bounded-buffer AipsIO codec; storage
  managers select byte order explicitly and do not maintain local detectors or
  primitive codecs.
- `casa-types` owns pure measures algorithms and the `MeasuresProvider`
  contract. `casa-measures-data::MeasuresRuntime` is the explicit fallible I/O
  implementation; applications acquire one runtime at an operation boundary
  and pass it inward. Discovery never installs data, and installation is an
  explicit caller-selected maintenance action.
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

`casars-imager` owns MeasurementSet selection, bounded source streaming, mode
dispatch, runtime policy, protocol telemetry, and persisted product writing.
`casa-imaging` remains the prepared-visibility computation boundary for
weighting, gridding/degridding, FFTs, normalization, deconvolution, restoration,
and product semantics.

The application admits one immutable `ImagingResolvedPlan` from explicit task
policy, workload shape, and a reservation in its process resource ledger.
Pure checked formulas live in `casa-imaging`; core execution consumes the
admitted workers, ingest blocks, FFT chunks, tile/queue residency, spectral
schedule, caches, and Metal schedule without reading process environment or
host availability. Operating-system available/reclaimable memory is an
application runtime input, and an explicit memory control caps that assignment.
On heterogeneous Apple Silicon, the application assigns the performance-core
slice to the latency-critical single-owner mosaic grid topology; independently
tiled work may use the complete logical CPU slice. Explicit worker controls are
still caps within the assigned topology resources.
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
