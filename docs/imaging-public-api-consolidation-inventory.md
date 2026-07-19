# Imaging Public API Consolidation Inventory

Truth class: implementation inventory
Last reality check: 2026-07-10
Verification: targeted `rg` call-site searches; `python3 -m py_compile tools/perf/imager/imaging_interface_metrics.py`; `python3 tools/perf/imager/imaging_interface_metrics.py --base origin/main --format markdown`; `cargo fmt --all -- --check`; `cargo check -p casa-imaging -p casa-ms -p casars-imager`; focused `casa-imaging`, `casa-ms`, and `casars-imager` regression tests for source views, auto-multithresh, PB products, coordinate systems, density translation, product-plane helpers, cube-Briggs formulas, clean-cycle/Hogbom helpers, and standard-MFS/mosaic/PB app routes; `just quick`; `just docs-check`; `just verify`; `tools/perf/imager/run_workload.py --artifact-root target/imperformance-interface-consolidation-final-r3/artifacts --repeats 3 --run-label imaging-interface-consolidation-final-r3 --storage-label local-testdata --output-dir target/imperformance-interface-consolidation-final-r3/smoke wave1-standard-mfs-dirty-smoke`; same three-repeat workload at `origin/main` commit `e5df883d1b465f87661322fec875dacd05e5fc0f` in `/private/tmp/casa-rs-origin-imaging-interface-20260702`

## Scope

This inventory records the standard-MFS imaging execution surface after issue
#319. It focuses on the places where the Rust public/private boundary had
grown around implementation variants instead of domain concepts:

- `crates/casa-imaging/src/lib.rs`
- `crates/casa-imaging/src/execution.rs`
- `crates/casa-imaging/src/types.rs`
- `crates/casars-imager/src/lib.rs`
- `crates/casa-ms/src/visibility_buffer.rs`
- `crates/casa-ms/src/spectral_selection.rs`

Persisted image/MS formats and output product names are unchanged. The current
imaging-performance wave extends the canonical task JSON/CLI surface through
protocol v3 controls and telemetry, and adds a bounded mosaic MT-MFS concept
entrypoint to the core API; those changes are inventoried below.

## Audit Method

- Enumerated public and crate-visible items with:
  `rg -n "^pub (fn|struct|enum|trait|type|const|static|mod|use)\b|^\s*pub\(crate\)" crates/casa-imaging/src/lib.rs crates/casa-imaging/src/execution.rs crates/casa-imaging/src/types.rs crates/casars-imager/src/lib.rs crates/casa-ms/src/visibility_buffer.rs crates/casa-ms/src/spectral_selection.rs`.
- Searched workspace call sites for each old standard-MFS runner, source trait,
  prepared clean type, routed sample type, and new canonical facade.
- Treated same-name `cfg` alternatives as platform variants, not duplicates
  eligible for naive deletion.

## Disposition Legend

| Disposition | Meaning |
|---|---|
| Keep public | User-meaningful or cross-crate domain API. |
| Make private | Implementation variant with no external workspace callers. |
| Merge behind smaller public abstraction | Keep behavior but route callers through a concept-oriented public API. |
| Remove without replacement | Internal variant; no replacement beyond the retained concept API. |
| Transitional public | Still app-facing because a broader source/planning boundary is needed before demotion. |

## Issue #319 Current Progress

Issue #319 is now the full imaging architecture consolidation wave. The first
implemented slice replaced the standard-MFS route-specific function boundary
with a small plan/session facade:

| New facade | Purpose | Status |
|---|---|---|
| `StandardMfsPlan` and `run_standard_mfs_plan` | Canonical standard-MFS runner for weighted batches, planned sample run blocks, and routed visibility runs. | Public concept boundary. |
| `StandardMfsDirtyPlan` and `run_standard_mfs_dirty_plan` | Dirty-only standard-MFS runner for planned sample blocks. | Public concept boundary. |
| `StandardMfsCleanPlan`, `StandardMfsCleanFinishPlan`, and `StandardMfsCleanSession` | Prepared clean session boundary for clean-plane preparation, skip-threshold handling, full finish, and one-major-cycle finish. | Public concept boundary. |

The old route-specific public runners now survive only as private
implementation functions inside `casa-imaging`. App code calls the facade and
does not cross the crate boundary through prepared clean internals or source
traits. The current source-boundary slice also introduced neutral
`VisibilitySource` / `VisibilityBlockStream` / `VisibilityBlockView` vocabulary
and moved the `VisibilityBuffer` to imaging-block adapter into `casa-ms`. The
tested auto-multithresh clean-mask builder has also moved from app-private test
helpers into `CubeAutoMultiThresholdConfig`, leaving CLI mask parsing in the
app and array/beam mask computation in the imaging library. The app-local
paired-hand collapse enum and helper are now folded into
`StandardMfsPairCollapseTransform`, `ParallelHandBatch`, and `PlaneStokes`
methods, including the correlation-pair selection rule.

The same source-boundary slice removed the app-local `VisibilitySourceBlock`
row adapter, both `get_ms_values_into_*_processing_buffer` wrappers, and the
single-implementation `VisibilitySourceRows` trait. The standard-MFS, mosaic,
one-channel cube, and trace source-row routes now read through the
`VisibilityBuffer`/`ColumnarPreparedSource` path. `casa-ms` also owns the
canonical MAIN-row selection intent, resolved physical facts, and bounded
read-block planning through `MsSelection`, `ResolvedMsSelection`,
`MsSelectionIoBudget`, and `MsSelectionRowBlock`. Array-cell consumers retain
`VisibilityChannelReadRange` and `VisibilityReadBlockPlan`. The one-channel
cube Briggs and mosaic density essentials readers now also adapt from the
shared `VisibilityBuffer` path with data omitted. The remaining
`read_visibility_source_columns` usage is the older essentials/routed reader,
but its standard-MFS handoff now goes through public row/block/cache facade
contracts rather than public backend payloads.

The route-plan vocabulary has also been tightened: `SpectralRoutePlan`,
`SourceChannelRoute`, and `OutputPlaneContribution` no longer expose public
mutable fields. The pure geometric `phase_rotate_visibility` helper now lives
in `casa-imaging`, leaving the app to call shared imaging math rather than own
that computation locally. `StandardMfsWeightedSample`,
`StandardMfsPlannedWeightedSample*`, `StandardMfsRoutedVisibility*`, and
`StandardMfsMetalGroupedInputCache*` are now private backend IR. The app uses
`ScalarVisibilitySample`, `StandardMfsPlannedSampleBlock`,
`StandardMfsVisibilityRow`, `StandardMfsRoutedVisibilityBlock`, and
`StandardMfsRoutedInputCache*` as the public source/cache facade. Primary-beam
product math and CASA-compatible coordinate-system construction have also moved into
`casa-imaging`; the app still infers MS-specific PB metadata and owns filenames,
writer calls, previews, and product replacement policy. Standard-MFS density row
translation has also moved into `casa-imaging` through row-major and
`VisibilityBlockView` helpers; the app retains MS row streaming, worker-local
density accumulator orchestration, and timing/progress reporting. Product-side
PB support masks, pbcor products, alpha PB correction, and clean-mask product
materialization now use `casa-imaging` helpers. Product membership, units, beam
metadata, default masks, and derived side products now flow through
`ImageProductSet`, `ImageProduct`, and `ImageProductMetadata`; the app remains
responsible for filenames, `PagedImage` writes, previews, and replacement
policy. MFS product plane extraction/expansion, masked MFS peak measurement,
and clean-mask pixel counting are also library helpers now. Joint-outlier Hogbom now also uses the
shared finite masked peak search, plane minor-cycle loop, PSF subtraction,
CASA-inclusive iteration accounting, and clean-cycle threshold calculation from
`casa-imaging`. CASA cube-Briggs density-cell lookup, robust scale, and
preweighting denominator formulas are also library helpers now. Worker-count
planning and single-plane execution/product/capability planning have moved to
`casa-imaging`; the app maps CLI flags and runtime facts into those contracts.
App code keeps the MS/source grouping and streaming orchestration.

The imaging-performance wave also consolidated source overlap behind one
app-private bounded producer/consumer primitive. Standard MFS, mosaic MFS and
MT-MFS replay, standard/mosaic cube, cubedata, and trace preparation use the
same read-ahead ownership rule: the public `imaging_read_ahead_blocks` control
names the maximum live row blocks, while the private queue capacity is
`max_live_row_blocks - 2`. The implementation currently caps the value at two,
accounting for one producer-owned and one consumer-owned block; it does not
expose a queue or source-thread implementation type as library API.

The first mosaic MT-MFS slice adds
`run_mosaic_mtmfs_from_single_plane_stream` and
`MosaicMtmfsVisibilityBlock`. It reuses `SinglePlaneStreamPass` and the
mosaic-specific `GroupedVisibilityMetadataBatch` contract, while carrying an
optional raw-UVW density sidecar separately from the projected visibility
batch so Briggs/uniform weighting can preserve CASA density-cell semantics.
The approved performance-wave API consolidation removed the generic
`SinglePlaneVisibilityBlock` and `SinglePlaneGridderMetadata` facade without a
compatibility layer: standard, cube, and cubedata paths use their existing
mode-specific boundaries, while mosaic MFS and MT-MFS use
`MosaicVisibilityBlock` and `MosaicMtmfsVisibilityBlock`. The task edge limits
the MT-MFS entrypoint to the supported single-MS, MFS, `nterms <= 2`, no-W-term
mosaic slice; the core validates the same projection and weighting constraints.

## Current Public Surface Snapshot

### `casa-imaging`

| Item or family | Current status | In-repo callers | Disposition | Replacement guidance |
|---|---:|---|---|---|
| `run_imaging`, `run_imaging_owned`, `run_imaging_owned_with_execution_config` | Public | `casa-imaging`, `casars-imager` | Keep public | Primary concept-oriented imaging entrypoints. |
| `run_standard_mfs_plan` / `StandardMfsPlan` | Public | `casars-imager`, tests | Keep public | Canonical standard-MFS facade for weighted, planned-run-block, and routed-visibility execution. |
| `run_standard_mfs_dirty_plan` / `StandardMfsDirtyPlan` | Public | `casars-imager`, tests | Keep public | Canonical dirty-only facade where the app already has planned sample blocks. |
| `StandardMfsCleanSession` with `StandardMfsCleanPlan` / `StandardMfsCleanFinishPlan` | Public | `casars-imager` clean and resident cube paths | Keep public | Session facade replaces prepared-clean free functions and hides `StandardMfsPreparedCleanPlane`. |
| `run_standard_mfs_weighted_streaming_with_execution_config` | Private | `casa-imaging` facade only | Make private | Use `run_standard_mfs_plan(StandardMfsPlan::weighted_batches(...))`. |
| `run_standard_mfs_planned_sample_run_block_streaming_with_execution_config` | Private | `casa-imaging` facade only | Make private | Use `run_standard_mfs_plan(StandardMfsPlan::planned_sample_run_blocks(...))`. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config_and_metal_grouped_input_cache` | Private | `casa-imaging` facade only | Make private | Use `run_standard_mfs_plan(StandardMfsPlan::routed_visibility_runs(...))`. |
| `run_standard_mfs_planned_sample_block_source_streaming_with_execution_config` | Private | `casa-imaging` facade/session only | Make private | Use `run_standard_mfs_dirty_plan(...)` or `StandardMfsCleanSession`. |
| Prepared-clean free functions | Private | `casa-imaging` session only | Make private | Use `StandardMfsCleanSession::{prepare, skip_with_cycle_threshold, finish, finish_one_major_cycle}`. |
| `VisibilitySource`, `VisibilityBlockStream`, `VisibilityBlockView`, `ReplayCapability`, `VisibilityStreamBounds` | Public | `casa-ms`, `casars-imager`, tests | Keep public | Neutral bounded source contract with replay and residency semantics. |
| `StandardMfsPlannedSampleBlockSource`, `StandardMfsRoutedVisibilityRunSource` | `pub(crate)` | `casa-imaging` only | Make private | Source traits are implementation adapters behind the facade. |
| `StandardMfsPreparedCleanPlane` | `pub(crate)` | `casa-imaging` session only | Make private | Session object is the public handle. |
| `StandardMfsRoutedSample`, `StandardMfsRoutedSampleRunBlock` | `pub(crate)` | `casa-imaging` only | Make private | Routed-sample backend IR is not crate-root API. |
| `StandardMfsRoutableSample` | Removed | none | Remove without replacement | Dead compatibility shape. |
| `StandardMfsRoutedVisibilityRunBlock`, `StandardMfsRoutedVisibilityRunBlockSource` | Removed | none | Remove without replacement | Dead row-run block wrapper after the routed-run source facade moved to direct runs. |
| `StandardMfsExecutionConfig` | Public | `casa-imaging`, `casars-imager` | Keep public | User-visible execution knobs still cross the app/library boundary. |
| `PlaneStokes::derive_pair_selection`, `StandardMfsPairCollapseTransform::collapse`, `ParallelHandBatch::collapse_with_transform`, `PlaneStokes::paired_sumwt_factor` | Public methods | `casars-imager`, tests | Keep public | Paired-hand Stokes-like collapse is imaging math; apps should select a transform, not duplicate the pair-selection rule, operation, or batch reduction. |
| `ScalarVisibilitySample`, `StandardMfsPlannedSampleBlock`, `StandardMfsVisibilityRow`, `StandardMfsRoutedVisibilityBlock`, `StandardMfsRoutedInputCache*` | Public | `casars-imager` streaming/replay paths | Keep public | Explicit source/cache facade replaces public standard-MFS backend payloads while preserving bounded replay and Metal prefill performance. |
| `StandardMfsWeightedSample`, `StandardMfsPlannedWeightedSample*`, `StandardMfsRoutedVisibility*`, `StandardMfsMetalGroupedInputCache*` | `pub(crate)` | `casa-imaging` only | Make private | Backend IR is internal; apps append scalar/row inputs to opaque planned/routed/cache blocks. |
| `CubeAutoMultiThresholdConfig::build_cube_clean_mask` | Public method | tests, future shared CLEAN controllers | Keep public | Clean-mask array generation is imaging computation; apps should pass parameters and selected beams, not own the threshold/grow/prune algorithm. |
| `PrimaryBeamProductRequest` / `primary_beam_product` and PB cutoff helpers | Public | `casars-imager`, tests | Keep public | PB array generation, support masks, pbcor division, and alpha PB products are imaging math; apps should keep MS inference and file writing. |
| `SinglePlaneExecutionPlan` and `SinglePlaneExecutionPlanInput` | Public | `casars-imager`, tests | Keep public | Library-owned projection/product/backend-capability plan; app only maps CLI/runtime facts into the input. |
| `ImagingWorkerPlan` and `ImagingWorkerPlanInput` | Public | `casars-imager`, spectral slab planner tests | Keep public | Shared worker-count and backend-command-target model used by app/runtime planners. |
| MFS image-product plane helpers and `ImageProductSet` | Public | `casars-imager`, tests | Keep public | Product-plane extraction/expansion, masked peak measurement, clean-mask pixel counting, product membership, and product metadata are product semantics; the app serializes supplied products rather than owning product-selection logic. |
| CASA cube-Briggs formula helpers | Public | `casars-imager`, tests | Keep public | Cube Briggs density-cell lookup, f2, and preweighting denominator formulas are imaging weighting math; apps should not duplicate them. |
| `clean_cycle_threshold` and `run_hogbom_plane_minor_cycle` | Public | `casars-imager`, tests | Transitional public | Shared clean-control policy and the finite masked Hogbom plane loop now live with the deconvolution code; a future concept-level clean session should absorb these helpers. |
| `build_image_coordinate_system` | Public | `casars-imager`, tests | Keep public | CASA-compatible image coordinate metadata belongs with imaging product semantics, not the application writer. |
| `accumulate_standard_mfs_density_row_from_arrays` / `accumulate_standard_mfs_density_row_from_visibility_block` | Public | `casars-imager`, tests | Transitional public | Standard-MFS density row math is library computation; app still owns MS streaming and worker scheduling until the neutral source boundary moves farther down. |
| `SinglePlaneVisibilityBlock`, `SinglePlaneGridderMetadata` | Removed | mosaic callers | Remove generic facade; migrate mosaic callers directly | Replace `SinglePlaneVisibilityBlock` with `MosaicVisibilityBlock` and replace `SinglePlaneGridderMetadata::Mosaic(metadata)` with `gridder_metadata: metadata`. Standard imaging uses its existing standard-mode boundaries and has no replacement generic block. No compatibility layer is retained. |
| `SinglePlaneStreamPass` | Public | `casars-imager` replay paths | Keep public | Shared semantic pass identity without exposing a generic block payload. |
| `run_mosaic_mfs_from_single_plane_stream` / `MosaicVisibilityBlock` | Public | `casars-imager` mosaic path | Keep public | Current mosaic-specific streaming API; the optional raw-UVW density sidecar remains separate from projected gridding metadata. |
| `run_mtmfs` | Public | `casars-imager` standard-gridder MT-MFS path | Keep public | Current concept API for standard-gridder MT-MFS. |
| `run_mosaic_mtmfs_from_single_plane_stream` / `MosaicMtmfsVisibilityBlock` | Public | `casars-imager` supported mosaic MT-MFS path | Keep public | Replayable first-slice mosaic MT-MFS boundary; shares `SinglePlaneStreamPass` and carries sample frequencies, projected mosaic metadata, and optional raw-UVW density input per block. |
| `plan_mosaic_mtmfs_direct_scratch` / `MosaicMtmfsDirectScratchPlan` | Public | `casars-imager` memory planner and `casa-imaging` executor | Keep public | One shape/plane/worker-count formula prevents frontend and executor residency estimates from drifting; no dataset identity enters the estimate. |
| `standard_mfs_metal_device_available` | Public | `casars-imager` policy/export paths | Keep public for now | Capability query, not an implementation duplicate; broader execution-capability API remains future work. |

### `casars-imager`

`casars-imager` still exports user-facing config, protocol-v3 task contract,
oracle, managed-output, and run-summary types. It no longer calls the old
route-specific standard-MFS public functions. It still owns substantial
essentials/routed source shaping, density planning, routed visibility grouping,
product assembly, and writer-policy code; physical MAIN-row selection now lives
in `casa-ms`, and single-plane execution/product planning now lives in
`casa-imaging`.

Protocol v3 exposes `parallel`, `chanchunks`, shared imaging memory/prepare/
row-block/read-ahead controls, and dirty-product FFT precision/backend policy.
Diagnostic progress telemetry reports memory-ledger categories, source read
bytes and bandwidth, producer/consumer overlap and blocking, worker/queue state,
stage timing, and backend selection/fallback data. These are task/runtime
contracts; the producer channel, FFT policy guard, and Apple resident-product
implementation remain private.

| Item or family | Current status | Disposition | Replacement guidance |
|---|---:|---|---|
| `run_with_cli_args`, `run_from_request`, `ImagerRunTaskRequest`, `run_from_config`, `CliConfig`, `RunSummary` | Public | Keep public | `ImagerRunTaskRequest`/`run_from_request` is the canonical task execution contract; `CliConfig`/`run_from_config` is the retained command-line parser compatibility wrapper. |
| Task contract and schema exports | Public | Keep public | Contract surface; do not change in API consolidation cleanup. |
| Oracle and managed-output exports | Public | Keep public | Test/oracle and output-management boundary. |
| Standard-MFS calls into `casa-imaging` | Private app orchestration | Merge behind smaller public abstraction | Calls now use `StandardMfsPlan`, `StandardMfsDirtyPlan`, and `StandardMfsCleanSession`. |
| Source row-block prep, channel routing, density planning, product assembly | Private app implementation | Transitional debt | Standard MFS, mosaic, one-channel cube, and trace row-block reads now share `VisibilityBuffer`; physical row selection is in `casa-ms`; remaining essentials/routed readers move next toward bounded reads and `casa-imaging` source/product semantics. |

### `casa-ms`

`casa-ms` already owns reusable measurement-set selection and bounded buffer
building primitives such as `MsSelection`, channel-selection resolution,
`VisibilityBufferRequest`, `VisibilityBuffer`, and source partitions. The
current path adds `VisibilityBuffer::as_visibility_block_view()`, canonical
`ResolvedMsSelection` construction with `MsSelectionRowBlock` planning,
`VisibilityChannelReadRange`, and `VisibilityReadBlockPlan` so selected MS rows
and filled buffers can cross into the neutral imaging source contract without
app-private adapter code.

Remaining source-boundary debt: the older essentials/routed column-read
orchestration still lives in `casars-imager`; the active row-block source
routes now share `VisibilityBuffer`, but `read_visibility_source_columns`
remains for essentials/routed replay helpers.

## Standard-MFS Function Family After #319

| Old public function | Status after #319 | Replacement |
|---|---:|---|
| `run_standard_mfs_weighted_streaming_with_execution_config` | Private | `run_standard_mfs_plan(StandardMfsPlan::weighted_batches(...))` |
| `run_standard_mfs_planned_sample_run_block_streaming_with_execution_config` | Private | `run_standard_mfs_plan(StandardMfsPlan::planned_sample_run_blocks(...))` |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config_and_metal_grouped_input_cache` | Private | `run_standard_mfs_plan(StandardMfsPlan::routed_visibility_runs(...))` |
| `run_standard_mfs_planned_sample_block_source_streaming_with_execution_config` | Private | `run_standard_mfs_dirty_plan(...)` or `StandardMfsCleanSession` |
| `prepare_standard_mfs_planned_sample_run_block_clean_plane_with_execution_config` | Private | `StandardMfsCleanSession::prepare(...)` |
| `prepare_standard_mfs_planned_sample_block_source_clean_plane_with_execution_config` | Private | `StandardMfsCleanSession::prepare(...)` |
| `skip_standard_mfs_prepared_clean_plane_with_cycle_threshold` | Private | `StandardMfsCleanSession::skip_with_cycle_threshold(...)` |
| `finish_standard_mfs_prepared_clean_plane_with_execution_config` | Private | `StandardMfsCleanSession::finish(...)` |
| `finish_standard_mfs_prepared_clean_plane_one_major_cycle_with_execution_config` | Removed | `StandardMfsCleanSession::finish_one_major_cycle(...)` |
| `run_standard_mfs_planned_sample_block_source_streaming_with_execution_config` | Private | `run_standard_mfs_dirty_plan(...)` |
| `run_standard_mfs_dirty_planned_sample_block_source_streaming_with_execution_config` | Removed wrapper | `run_standard_mfs_dirty_plan(...)` |

## Metric Snapshot

These metrics are captured with
`python3 tools/perf/imager/imaging_interface_metrics.py --base origin/main --format markdown`.
They cover the expanded imaging scope in that script, not only the original
phase-1 file list.

| Metric | Baseline | Current | Delta |
|---|---:|---:|---:|
| Public symbol-like items in expanded imaging scope | 623 | 626 | +3 |
| Public fields in expanded imaging scope | 1,418 | 1,429 | +11 |
| `pub(crate)` symbol-like items in expanded imaging scope | 427 | 467 | +40 |
| Private/internal symbol-like items in expanded imaging scope | 4,081 | 4,310 | +229 |
| App-private functions in `casars-imager` | 681 | 713 | +32 |
| App standard-MFS density row helpers | 2 | 2 | 0 |
| App product-plane helpers | 0 | 0 | 0 |
| App cube-Briggs formula helpers | 0 | 0 | 0 |
| Legacy app `get_ms_values_into_*` helpers | 0 | 0 | 0 |
| Legacy app `VisibilitySourceBlock*` concrete types | 0 | 0 | 0 |
| Old route-specific public standard-MFS runners/free functions | 1 | 1 | 0 |
| Public `StandardMfs*` backend payload subset | 10 | 10 | 0 |
| Public `StandardMfsPlannedWeightedSample` fields | 0 | 0 | 0 |
| Rust code lines in expanded imaging scope | 145,410 | 157,474 | +12,064 |

The current wave adds two public domain symbols while removing the generic
single-plane mosaic block facade without replacement. Most growth is private or
`pub(crate)` implementation for bounded replay, planner telemetry, exact-plan
compaction, and Metal tile execution. The app still does not construct or
observe planned/routed/Metal backend IR directly.

## Performance Smoke Snapshot

The current worktree and `origin/main` were run through the same
`wave1-standard-mfs-dirty-smoke` CASA-vs-Rust workload with three repeats and
artifact scratch forced under writable local `target` paths. This is smoke
evidence, not a medium/large benchmark conclusion.

| Run | Rust runs | Rust median | CASA median | Rust/CASA | Product comparison |
|---|---:|---:|---:|---:|---|
| `origin/main` `e5df883d1b465f87661322fec875dacd05e5fc0f` | `0.933943, 0.050690, 0.049200 s` | `0.050690 s` | `0.107767 s` | `0.470x` | completed |
| Current worktree | `1.165246, 0.046237, 0.045941 s` | `0.046237 s` | `0.110095 s` | `0.420x` | completed |

Head Rust median was `0.912x` of the `origin/main` Rust median for this smoke
workload. The first run in each row includes cold process/data effects, so this
is a guardrail against obvious regressions rather than a broad speedup claim.
The `.image`, `.residual`, and `.psf` product deltas were identical between the
two runs: `.image`/`.residual` `diff_rms_over_casa_rms =
7.513996986357205e-05`, and `.psf` `diff_rms_over_casa_rms =
2.0025219261353267e-04`.
