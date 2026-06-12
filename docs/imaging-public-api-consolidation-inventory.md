# Imaging Public API Consolidation Inventory

Truth class: implementation inventory
Last reality check: 2026-06-12
Verification: targeted `rg` call-site searches; `just quick`; `just verify`

## Scope

This inventory records the Wave 3 imaging execution and orchestration surface
that is variant-heavy enough to need deliberate public API shrinkage. It covers:

- `crates/casa-imaging/src/lib.rs`
- `crates/casa-imaging/src/execution.rs`
- `crates/casars-imager/src/lib.rs`

The dispositions here apply only to Rust public or crate-visible implementation
surface. Provider contracts, persisted image/MS formats, CLI flags, task JSON,
output product names, and numerical algorithms are unchanged.

## Audit Method

- Enumerated public items with `rg -n "^pub (fn|struct|enum|trait|type|const|static|mod|use)\b|^\s*pub\(crate\)" crates/casa-imaging/src/lib.rs crates/casa-imaging/src/execution.rs crates/casars-imager/src/lib.rs`.
- Searched workspace call sites for the standard-MFS, `casars-imager`,
  mosaic, and W-projection functions listed in issues #289 through #292.
- Treated same-name `cfg` alternatives as platform variants, not duplicates
  eligible for naive deletion.

## Disposition Legend

| Disposition | Meaning |
|---|---|
| Keep public | User-meaningful or cross-crate domain API. |
| Make private | Implementation variant with no external workspace callers. |
| Merge behind smaller public abstraction | Keep behavior but route callers through a concept-oriented public API. |
| Remove without replacement | Internal variant; no replacement beyond the retained concept API. |
| Recorded remaining debt | Related duplicate family outside this wave's implemented boundary. |

## Current Public Surface Snapshot

### `casa-imaging`

The public execution/orchestration surface is concentrated around these items:

| Item or family | Current status | In-repo callers | Disposition | Replacement guidance |
|---|---:|---|---|---|
| `run_imaging`, `run_imaging_owned`, `run_imaging_owned_with_execution_config` | Public | `casa-imaging`, `casars-imager` | Keep public | Primary concept-oriented imaging entrypoints. |
| `run_standard_mfs_weighted_streaming_with_execution_config` | Public | `casa-imaging` tests, `casars-imager` dirty path | Keep public for now | Streaming standard-MFS concept API. Later wave may rename or wrap with a smaller request type. |
| `run_standard_mfs_weighted_sample_streaming_with_execution_config` | Test-only private as of this inventory | `casa-imaging` tests only | Remove without replacement | Internal scalar sample variant; use retained streaming or source-block APIs. |
| `run_standard_mfs_weighted_sample_block_streaming_with_execution_config` | Test-only private as of this inventory | `casa-imaging` tests only | Remove without replacement | Internal row-block sample variant; use retained streaming or source-block APIs. |
| `run_standard_mfs_planned_sample_block_streaming_with_execution_config` | Test-only private as of this inventory | `casa-imaging` tests only | Remove without replacement | Internal planned-sample variant; source-block API remains the cross-crate boundary. |
| `run_standard_mfs_planned_sample_run_block_streaming_with_execution_config` | Public | `casars-imager` clean path | Keep public for now | Retained app/library boundary; now routes through the private `StandardMfsReplayPlan`. |
| `run_standard_mfs_routed_sample_run_block_streaming_with_execution_config` | Removed as of #290 | no external workspace caller found | Remove without replacement | Internal routed-sample run-block variant; routed visibility-run API remains the live boundary. |
| `run_standard_mfs_routed_visibility_run_block_streaming_with_execution_config` | Removed as of #290 | no external workspace caller found | Remove without replacement | Internal block adapter; no replacement beyond retained routed visibility-run API. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config` | Removed as of #290 | no external workspace caller found | Remove without replacement | Internal no-cache adapter; Metal-cache-capable sibling is the app boundary. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config_and_metal_grouped_input_cache` | Public | `casars-imager` Metal path | Keep public for now | Retained app/library boundary; now routes through the private `StandardMfsReplayPlan`. |
| `run_standard_mfs_planned_sample_block_source_streaming_with_execution_config` | Public | `casars-imager` clean path | Keep public for now | Retained app/library boundary; now routes through the private `StandardMfsReplayPlan`. |
| `run_standard_mfs_routed_visibility_run_block_source_streaming_with_execution_config` | Removed as of #290 | no external workspace caller found | Remove without replacement | Internal source-block adapter; no replacement. |
| `run_standard_mfs_routed_visibility_run_source_streaming_with_execution_config` | Removed as of #290 | no external workspace caller found | Remove without replacement | Internal no-cache source adapter; no replacement. |
| `run_standard_mfs_routed_sample_block_source_streaming_with_execution_config` | Removed as of #290 | no external workspace caller found | Remove without replacement | Internal routed-sample source-block variant; no replacement. |
| `StandardMfsExecutionConfig` | Public | `casa-imaging`, `casars-imager` | Keep public for now | User-visible backend knobs currently cross the app/library boundary. |
| `StandardMfsPlannedSampleBuilder` and source traits | Public | `casa-imaging`, `casars-imager` | Merge behind smaller public abstraction | Retained app/library source-build boundary; not changed by this wave. |
| `StandardMfsDirtyAccumulator*` | Public | `casa-imaging` and app export tooling | Keep public for now | Fixture/export path still uses this as a boundary object. |
| `StandardMfsMetalGroupedInputCache*` | Public | `casars-imager` Metal prefill/export paths | Merge behind backend abstraction | Keep until Metal grouped-input cache is represented as private backend strategy. |
| `SinglePlaneVisibilityBlock`, `SinglePlaneStreamPass`, `SinglePlaneGridderMetadata` | Public | `casars-imager` mosaic/cube paths | Keep public | User-meaningful stream boundary for single-plane imaging. |
| `run_mosaic_mfs_from_single_plane_stream` | Public | `casars-imager` mosaic path | Keep public | Current concept API for mosaic MFS streaming. |
| `run_mtmfs` | Public | `casars-imager` MT-MFS path | Keep public | Current concept API for MT-MFS. |
| `standard_mfs_metal_device_available` | Public | `casars-imager` policy/export paths | Keep public | Capability query, not an implementation duplicate. |

### `casa-imaging/src/execution.rs`

Most execution-layer types and functions are already `pub(crate)`. This is the
right direction: fixed-tile planning, tile buckets, planned samples, workspace
state, CPU/Metal executors, visibility plans, and accumulation helpers are
implementation details owned by `casa-imaging`.

| Item or family | Current status | In-repo callers | Disposition | Replacement guidance |
|---|---:|---|---|---|
| `StandardMfsBackend`, CPU/Metal executor types, tile partitioning, tile buckets | `pub(crate)` | `casa-imaging` only | Keep crate-private | No public replacement. |
| Replay accumulation helpers for dirty, PSF, and residual | `pub(crate)` methods | `casa-imaging` only | Merge behind internal request/plan | Planned-sample and routed-visibility sources now share `StandardMfsReplayPlan`; weighted-batch streaming remains separate because it also owns W-projection replay. |
| `StandardMfsVisibilityPlan`, `StandardMfsPlannedSample`, `StandardMfsWorkspace` | `pub(crate)` | `casa-imaging` only | Keep crate-private | No public replacement. |
| `StandardMfsMetalGroupedInputCache*` | Public type exported by `lib.rs` | `casars-imager` Metal paths | Merge behind backend abstraction | Keep only until Metal grouped-input cache is no longer an app-facing implementation type. |

### `casars-imager`

`casars-imager` exports user-facing config, task contract, oracle, managed
output, and run-summary types. The row-block and streaming functions listed in
#291 are private today, so the consolidation target is code duplication and
crate-visible churn rather than Rust public API removal.

| Item or family | Current status | In-repo callers | Disposition | Replacement guidance |
|---|---:|---|---|---|
| `run_with_cli_args`, `run_from_config`, `CliConfig`, `RunSummary` | Public | binary, tests, examples | Keep public | User-facing app/runtime boundary. |
| Task contract and schema exports | Public | Python/frontend/task callers | Keep public | Contract surface; do not change in stabilization cleanup. |
| Oracle and managed-output exports | Public | examples/tests | Keep public | Test/oracle and output-management boundary. |
| Source row-block prep functions | Private | `casars-imager` only | Merge behind smaller internal abstraction | Shared through private `SourceRowBlockPlaneDescriptor`; no public replacement. |
| Open-MS mode orchestration functions | Private | `run_from_config` only | Recorded remaining debt | Keep user-visible modes explicit; #291 consolidated the shared row-block preparation cluster. |
| Standard-MFS density/planned/routed streaming stages | Private | `casars-imager` only | Merge behind smaller internal abstraction | Dead routed-sample run-block stage removed; retained planned and routed-visibility stages feed the reduced `casa-imaging` APIs. |

## Standard-MFS Variant Families

| Function | Status after this inventory | Call-site result | Disposition |
|---|---:|---|---|
| `run_standard_mfs_weighted_streaming_with_execution_config` | Public | `casa-imaging` tests; `casars-imager` dirty streaming | Keep public for now. |
| `run_standard_mfs_weighted_sample_streaming_with_execution_config` | Test-only private | `casa-imaging` tests only | Internal variant, no replacement. |
| `run_standard_mfs_weighted_sample_block_streaming_with_execution_config` | Test-only private | `casa-imaging` tests only | Internal variant, no replacement. |
| `run_standard_mfs_planned_sample_block_streaming_with_execution_config` | Test-only private | `casa-imaging` tests only | Internal variant, no replacement. |
| `run_standard_mfs_planned_sample_run_block_streaming_with_execution_config` | Public | `casars-imager` clean streaming | Retained boundary; shared private replay plan. |
| `run_standard_mfs_routed_sample_run_block_streaming_with_execution_config` | Removed | no external workspace caller found | Internal variant, no replacement. |
| `run_standard_mfs_routed_visibility_run_block_streaming_with_execution_config` | Removed | no external workspace caller found | Internal adapter, no replacement. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config` | Removed | no external workspace caller found | Internal no-cache adapter, no replacement. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config_and_metal_grouped_input_cache` | Public | `casars-imager` Metal prefill path | Retained boundary; shared private replay plan. |
| `run_standard_mfs_planned_sample_block_source_streaming_with_execution_config` | Public | `casars-imager` clean streaming | Retained boundary; shared private replay plan. |
| `run_standard_mfs_routed_visibility_run_block_source_streaming_with_execution_config` | Removed | no external workspace caller found | Internal adapter, no replacement. |
| `run_standard_mfs_routed_visibility_run_source_streaming_with_execution_config` | Removed | no external workspace caller found | Internal no-cache adapter, no replacement. |
| `run_standard_mfs_routed_sample_block_source_streaming_with_execution_config` | Removed | no external workspace caller found | Internal variant, no replacement. |

The API reduction is now broader than the first inventory pass: the scalar
sample, weighted sample-block, and planned sample-block wrappers are test-only
helpers, six routed implementation wrappers were removed, the routed-sample
block-source trait was removed from the public surface, and
`StandardMfsRoutedSampleRunBlock` is no longer re-exported from the crate root.
These removals have no replacement beyond the retained concept-oriented
streaming/source APIs.

## Standard-MFS Replay Families

The dirty/PSF/residual replay helpers are already private functions in
`casa-imaging`. They still duplicate the same axes across products:

| Family | Current status | Disposition |
|---|---:|---|
| Tiled replay dirty/PSF/residual | Private | Kept separate because weighted-batch streaming also owns W-projection replay. |
| Sample replay dirty/PSF/residual | Private | Routed through `StandardMfsReplayPlan`. |
| Routed sample replay dirty/PSF/residual | Removed from live API path | Internal routed-sample run-block backend is no longer reachable. |
| Routed visibility-run replay dirty/PSF/residual | Private | Routed through `StandardMfsReplayPlan`, including optional Metal grouped-input cache. |

The shared private request/plan now captures product selection, source family,
routing strategy, block/run grouping, executor config, and optional Metal cache
for the planned-sample and routed-visibility paths. This implements issue #290
without changing gridding math, weighting math, task/provider contracts, or
persisted products.

## `casars-imager` Row-Block and Streaming Families

All functions listed in issue #291 are private today. They form two clusters:

| Family | Current status | Disposition |
|---|---:|---|
| Source row-block preparation for mosaic MFS, cube one-channel, trace, and no-trace paths | Private | Shared private `SourceRowBlockPlaneDescriptor`. |
| Open-MS mode orchestration for mosaic, cube, standard-MFS, and MT-MFS | Private | Recorded remaining debt; keep mode entrypoints explicit. |
| Standard-MFS density, planned-sample, routed-sample, routed-visibility, and Metal-prefill streaming stages | Private | Dead routed-sample run-block stage removed; retained stages feed reduced `casa-imaging` APIs. |

The descriptor captures the MS handle, selection, DDID/SPW/polarization table
context, loaded geometry/data/flag/weight columns, derived engine, optional
standard-MFS table values, optional cube context, finish mode, reusable spectral
plan, and cube-density/mosaic-metadata policy. This implements the core #291
row-block consolidation while keeping user-visible mode entrypoints explicit.

## Mosaic and W-Projection Backend Families

The functions listed in issue #292 are private today, except where behavior is
reachable through concept-oriented public APIs like `run_imaging` and
`run_mosaic_mfs_from_single_plane_stream`.

| Family | Current status | Disposition |
|---|---:|---|
| Mosaic dirty grouped CPU, Metal, and sample-range variants | Private | Consolidated behind `MosaicDirtyGroupBackend` / `MosaicDirtyGroupStrategy`. |
| Mosaic residual grouped CPU, Metal, and sample-range variants | Private | Recorded remaining debt; #292 chose mosaic dirty grouping as the primary family. |
| `accumulate_mosaic_grid_metal_samples` same-name `cfg` alternatives | Private wrapper plus named platform impls | Wrapper delegates to `accumulate_mosaic_grid_metal_samples_impl` under platform `cfg`. |
| W-projection PSF/dirty/residual concept functions | Private through public `run_imaging` | Recorded remaining debt outside the chosen #292 family. |
| W-projection streaming/replay serial, parallel, sample, and Metal variants | Private | Recorded remaining debt outside the chosen #292 family. |
| `accumulate_w_project_grid_metal_streaming_replay` same-name `cfg` alternatives | Private platform variants | Keep compile-gated; rename or module-split in chosen backend wave. |

For #292 this PR chooses mosaic dirty grouping as the primary backend family.
The private strategy captures the selected dirty backend (`Metal`,
`GroupedCpu`, `SampleRanges`, or `Scalar`), projectors, weight plan, and optional
prepared Metal/grouped samples. Mosaic residual and W-projection remain recorded
families, but are not changed by this wave.

## Implemented Stabilization Scope

- Issue #289: checked-in inventory plus first API shrink.
- Issue #290: standard-MFS public wrapper shrinkage, routed-sample block-source
  removal, and shared private replay plan for planned-sample and
  routed-visibility sources.
- Issue #291: shared source-row-block descriptor in `casars-imager` and removal
  of the dead routed-sample essentials run-block stage.
- Issue #292: mosaic dirty backend strategy boundary and clearer Metal `cfg`
  implementation split.

Remaining duplicate families are implementation debt, not approved-scope
deferrals for these issues: weighted-batch standard-MFS replay stays separate
because it also handles W-projection replay, and mosaic residual / W-projection
backend families remain unchanged because #292 required one primary family.
