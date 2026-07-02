# Imaging Public API Consolidation Inventory

Truth class: implementation inventory
Last reality check: 2026-07-02
Verification: targeted `rg` call-site searches; `cargo check -p casa-imaging -p casars-imager`; `cargo test -p casa-imaging standard_mfs --lib`; `cargo test -p casars-imager standard_mfs --lib`; `cargo test -p casars-imager source_stream --lib`; `cargo test -p casa-ms fill_visibility_buffer --lib`

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

Provider contracts, persisted image/MS formats, CLI flags, task JSON, output
product names, and numerical algorithms are unchanged.

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

## Issue #319 Result

Issue #319 replaced the standard-MFS route-specific function boundary with a
small plan/session facade:

| New facade | Purpose | Status |
|---|---|---|
| `StandardMfsPlan` and `run_standard_mfs_plan` | Canonical standard-MFS runner for weighted batches, planned sample run blocks, and routed visibility runs. | Public concept boundary. |
| `StandardMfsDirtyPlan` and `run_standard_mfs_dirty_plan` | Dirty-only standard-MFS runner for planned sample blocks. | Public concept boundary. |
| `StandardMfsCleanPlan`, `StandardMfsCleanFinishPlan`, and `StandardMfsCleanSession` | Prepared clean session boundary for clean-plane preparation, skip-threshold handling, full finish, and one-major-cycle finish. | Public concept boundary. |

The old route-specific public runners now survive only as private
implementation functions inside `casa-imaging`. App code calls the facade and
does not cross the crate boundary through prepared clean internals or source
traits.

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
| `StandardMfsPlannedSampleBlockSource`, `StandardMfsRoutedVisibilityRunBlockSource`, `StandardMfsRoutedVisibilityRunSource` | `pub(crate)` | `casa-imaging` only | Make private | Source traits are implementation adapters behind the facade. |
| `StandardMfsPreparedCleanPlane` | `pub(crate)` | `casa-imaging` session only | Make private | Session object is the public handle. |
| `StandardMfsRoutedSample`, `StandardMfsRoutedSampleRunBlock` | `pub(crate)` | `casa-imaging` only | Make private | Routed-sample backend IR is not crate-root API. |
| `StandardMfsRoutableSample` | Removed | none | Remove without replacement | Dead compatibility shape. |
| `StandardMfsExecutionConfig` | Public | `casa-imaging`, `casars-imager` | Keep public | User-visible execution knobs still cross the app/library boundary. |
| `StandardMfsWeightedSample`, `StandardMfsPlannedWeightedSample*`, `StandardMfsRoutedVisibility*` | Public | `casars-imager` source/routing stages | Transitional public | Full demotion requires moving app-owned source/routing construction behind a neutral bounded source contract. |
| `StandardMfsMetalGroupedInputCache*` | Public | `casars-imager` Metal prefill/export paths | Transitional public | Keep until Metal grouped-input staging is represented as private backend strategy behind the source contract. |
| `SinglePlaneVisibilityBlock`, `SinglePlaneStreamPass`, `SinglePlaneGridderMetadata` | Public | `casars-imager` mosaic/cube paths | Keep public | User-meaningful stream boundary for single-plane imaging. |
| `run_mosaic_mfs_from_single_plane_stream` | Public | `casars-imager` mosaic path | Keep public | Current concept API for mosaic MFS streaming. |
| `run_mtmfs` | Public | `casars-imager` MT-MFS path | Keep public | Current concept API for MT-MFS. |
| `standard_mfs_metal_device_available` | Public | `casars-imager` policy/export paths | Keep public for now | Capability query, not an implementation duplicate; broader execution-capability API remains future work. |

### `casars-imager`

`casars-imager` still exports user-facing config, task contract, oracle,
managed-output, and run-summary types. It no longer calls the old
route-specific standard-MFS public functions. It still owns substantial MS
selection, source-row shaping, density planning, routed visibility grouping,
product assembly, and writer-policy code.

| Item or family | Current status | Disposition | Replacement guidance |
|---|---:|---|---|
| `run_with_cli_args`, `run_from_config`, `CliConfig`, `RunSummary` | Public | Keep public | User-facing app/runtime boundary. |
| Task contract and schema exports | Public | Keep public | Contract surface; do not change in API consolidation cleanup. |
| Oracle and managed-output exports | Public | Keep public | Test/oracle and output-management boundary. |
| Standard-MFS calls into `casa-imaging` | Private app orchestration | Merge behind smaller public abstraction | Calls now use `StandardMfsPlan`, `StandardMfsDirtyPlan`, and `StandardMfsCleanSession`. |
| Source row-block prep, channel routing, density planning, product assembly | Private app implementation | Transitional debt | Move toward `casa-ms` bounded reads and `casa-imaging` source/product semantics in a later wave. |

### `casa-ms`

`casa-ms` already owns reusable measurement-set selection and bounded buffer
building primitives such as `MsSelection`, channel-selection resolution,
`VisibilityBufferRequest`, `VisibilityBuffer`, and source partitions. Issue
#319 did not move the remaining app-local row readers into `casa-ms`; that is a
larger source-boundary change than the standard-MFS facade slice.

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

These metrics were captured after the Rust implementation and before final
issue closeout.

| Metric | Baseline | Current | Delta |
|---|---:|---:|---:|
| Public symbols in the issue files | 170 | 160 | -10 |
| `pub(crate)` symbols in the issue files | 29 | 35 | +6 |
| Combined public + `pub(crate)` symbols | 199 | 195 | -4 |
| Old route-specific public standard-MFS runners/free functions | 10 | 0 | -10 |
| Public `StandardMfs*` backend payload subset | 14 | 11 | -3 |
| Rust line count in the issue files | 116,183 | 116,129 | -54 |
| `cloc` Rust code lines for the measured issue files | 109,983 | 109,905 | -78 |

The largest useful reduction is not raw SLOC; it is removing the old public
function matrix as a crate boundary. Remaining public payload types are a
measured transition point: the app still constructs source/routing IR directly,
so demoting those types without a broader source contract would either flatten
bounded replay into owned batches or move substantial app source construction
without the corresponding `casa-ms` boundary.
