# Imaging Public API Consolidation Inventory

Truth class: implementation inventory
Last reality check: 2026-06-11
Verification: targeted `rg` call-site searches plus `cargo test -p casa-imaging sample_streaming_weighted_standard_mfs_clean_matches_batch_streaming`

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
| Follow-up wave | Needs a separate review boundary before implementation. |

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
| `run_standard_mfs_planned_sample_run_block_streaming_with_execution_config` | Public | `casars-imager` clean path | Merge behind smaller public abstraction | Standard-MFS follow-up should hide planned-run grouping behind a request/plan type. |
| `run_standard_mfs_routed_sample_run_block_streaming_with_execution_config` | Public | no external workspace caller found | Make private in follow-up | Internal routed-sample variant, no replacement unless a caller appears. |
| `run_standard_mfs_routed_visibility_run_block_streaming_with_execution_config` | Public | no external workspace caller found | Make private in follow-up | Internal routed-visibility block variant, no replacement unless a caller appears. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config` | Public | no external workspace caller found | Make private in follow-up | Internal routed-visibility streaming variant; Metal-cache sibling is the current app boundary. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config_and_metal_grouped_input_cache` | Public | `casars-imager` Metal path | Merge behind smaller public abstraction | Preserve behavior, but hide Metal cache as a backend strategy. |
| `run_standard_mfs_planned_sample_block_source_streaming_with_execution_config` | Public | `casars-imager` clean path | Merge behind smaller public abstraction | Retain until standard-MFS request/plan boundary replaces source-block plumbing. |
| `run_standard_mfs_routed_visibility_run_block_source_streaming_with_execution_config` | Public | no external workspace caller found | Make private in follow-up | Internal source-block routed-visibility variant, no replacement. |
| `run_standard_mfs_routed_visibility_run_source_streaming_with_execution_config` | Public | no external workspace caller found | Make private in follow-up | Internal source-stream routed-visibility variant, no replacement. |
| `run_standard_mfs_routed_sample_block_source_streaming_with_execution_config` | Public | no external workspace caller found | Make private in follow-up | Internal routed-sample source-block variant, no replacement. |
| `StandardMfsExecutionConfig` | Public | `casa-imaging`, `casars-imager` | Keep public for now | User-visible backend knobs currently cross the app/library boundary. |
| `StandardMfsPlannedSampleBuilder` and source traits | Public | `casa-imaging`, `casars-imager` | Merge behind smaller public abstraction | Retain while `casars-imager` builds source blocks; follow-up should hide implementation axes. |
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
| Replay accumulation helpers for dirty, PSF, and residual | `pub(crate)` methods | `casa-imaging` only | Merge behind internal request/plan | Standard-MFS follow-up should share a private replay request path. |
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
| Source row-block prep functions | Private | `casars-imager` only | Follow-up wave | Consolidate around a private descriptor/builder; no public replacement. |
| Open-MS mode orchestration functions | Private | `run_from_config` only | Follow-up wave | Keep user-visible modes explicit, share internal open-MS and source-block plumbing. |
| Standard-MFS density/planned/routed streaming stages | Private | `casars-imager` only | Follow-up wave | Collapse into strategy choices feeding smaller `casa-imaging` public APIs. |

## Standard-MFS Variant Families

| Function | Status after this inventory | Call-site result | Disposition |
|---|---:|---|---|
| `run_standard_mfs_weighted_streaming_with_execution_config` | Public | `casa-imaging` tests; `casars-imager` dirty streaming | Keep public for now. |
| `run_standard_mfs_weighted_sample_streaming_with_execution_config` | Test-only private | `casa-imaging` tests only | Internal variant, no replacement. |
| `run_standard_mfs_weighted_sample_block_streaming_with_execution_config` | Test-only private | `casa-imaging` tests only | Internal variant, no replacement. |
| `run_standard_mfs_planned_sample_block_streaming_with_execution_config` | Test-only private | `casa-imaging` tests only | Internal variant, no replacement. |
| `run_standard_mfs_planned_sample_run_block_streaming_with_execution_config` | Public | `casars-imager` clean streaming | Follow-up: merge behind smaller public standard-MFS request/plan. |
| `run_standard_mfs_routed_sample_run_block_streaming_with_execution_config` | Public | no external workspace caller found | Follow-up: make private or remove without replacement. |
| `run_standard_mfs_routed_visibility_run_block_streaming_with_execution_config` | Public | no external workspace caller found | Follow-up: make private or remove without replacement. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config` | Public | no external workspace caller found | Follow-up: make private or remove without replacement. |
| `run_standard_mfs_routed_visibility_run_streaming_with_execution_config_and_metal_grouped_input_cache` | Public | `casars-imager` Metal prefill path | Follow-up: hide behind backend strategy. |
| `run_standard_mfs_planned_sample_block_source_streaming_with_execution_config` | Public | `casars-imager` clean streaming | Follow-up: hide source-block plumbing behind request/plan. |
| `run_standard_mfs_routed_visibility_run_block_source_streaming_with_execution_config` | Public | no external workspace caller found | Follow-up: make private or remove without replacement. |
| `run_standard_mfs_routed_visibility_run_source_streaming_with_execution_config` | Public | no external workspace caller found | Follow-up: make private or remove without replacement. |
| `run_standard_mfs_routed_sample_block_source_streaming_with_execution_config` | Public | no external workspace caller found | Follow-up: make private or remove without replacement. |

The first API reduction from this inventory is intentionally small: the scalar
sample, weighted sample-block, and planned sample-block wrappers are no longer
public or part of normal product builds. They remain as test-only helpers for
the existing regression that compares the old implementation axes. They have no
replacement beyond the retained concept-oriented streaming/source APIs.

## Standard-MFS Replay Families

The dirty/PSF/residual replay helpers are already private functions in
`casa-imaging`. They still duplicate the same axes across products:

| Family | Current status | Disposition |
|---|---:|---|
| Tiled replay dirty/PSF/residual | Private | Follow-up: share private replay request/plan. |
| Sample replay dirty/PSF/residual | Private | Follow-up: share private replay request/plan. |
| Routed sample replay dirty/PSF/residual | Private | Follow-up: share private replay request/plan. |
| Routed visibility-run replay dirty/PSF/residual | Private | Follow-up: share private replay request/plan. |

The shared private request/plan should capture product selection, sample source,
routing strategy, block/run grouping, executor/backend, and optional Metal
cache. This is the review boundary for issue #290.

## `casars-imager` Row-Block and Streaming Families

All functions listed in issue #291 are private today. They form two clusters:

| Family | Current status | Disposition |
|---|---:|---|
| Source row-block preparation for mosaic MFS, cube one-channel, trace, and no-trace paths | Private | Follow-up: shared private descriptor/builder. |
| Open-MS mode orchestration for mosaic, cube, standard-MFS, and MT-MFS | Private | Follow-up: keep mode entrypoints, share open-MS and row-block plumbing. |
| Standard-MFS density, planned-sample, routed-sample, routed-visibility, and Metal-prefill streaming stages | Private | Follow-up: strategy choices feeding reduced `casa-imaging` APIs. |

The descriptor should capture MS handle, selection, channel/plane selection,
trace policy, source/block policy, mode, output requirements, and cache needs.
This is the review boundary for issue #291.

## Mosaic and W-Projection Backend Families

The functions listed in issue #292 are private today, except where behavior is
reachable through concept-oriented public APIs like `run_imaging` and
`run_mosaic_mfs_from_single_plane_stream`.

| Family | Current status | Disposition |
|---|---:|---|
| Mosaic dirty grouped CPU, Metal, and sample-range variants | Private | Follow-up: private backend/strategy boundary. |
| Mosaic residual grouped CPU, Metal, and sample-range variants | Private | Follow-up: private backend/strategy boundary. |
| `accumulate_mosaic_grid_metal_samples` same-name `cfg` alternatives | Private platform variants | Keep compile-gated; make easier to audit before any deletion. |
| W-projection PSF/dirty/residual concept functions | Private through public `run_imaging` | Follow-up only if chosen as primary backend family. |
| W-projection streaming/replay serial, parallel, sample, and Metal variants | Private | Follow-up: private backend/strategy boundary. |
| `accumulate_w_project_grid_metal_streaming_replay` same-name `cfg` alternatives | Private platform variants | Keep compile-gated; rename or module-split in chosen backend wave. |

The backend strategy should capture product kind, CPU/Metal backend,
serial/parallel execution, sample-range grouping, replay/streaming input, and
precomputed projector/data preparation. macOS/Metal and non-Metal alternatives
must remain compile-gated cleanly.

## Follow-Up Review Boundaries

- Issue #290: standard-MFS public wrapper shrinkage plus shared private replay
  request/plan.
- Issue #291: `casars-imager` row-block preparation and streaming
  orchestration descriptor.
- Issue #292: pick mosaic or W-projection as the primary backend family, then
  consolidate that family behind a private strategy boundary.

These are separate implementation review boundaries because they touch
different owners and risk surfaces. Combining all three with the initial
inventory would make it difficult to prove that API shrinkage did not hide
algorithmic, task-contract, or backend-selection behavior changes.
