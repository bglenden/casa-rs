# ImPerformance Wave 2 Standard-MFS Experiment Summary

Truth class: historical evidence snapshot
Last reality check: 2026-05-25
Verification: `cargo check -p casa-imaging -p casars-imager`; `cargo test -p casa-imaging trace_residual_refresh_matches_fft_residual_and_prediction_order --lib`; `cargo test -p casa-imaging owned_standard_mfs_briggs_clean_matches_borrowed_run --lib`; `cargo test -p casars-imager standard_mfs_memory_planner_thread_parser_matches_core_spelling --lib`; `cargo test -p casars-imager standard_mfs_trace_free_prepare_matches_forced_trace_path --lib`; `cargo test -p casars-imager managed_output --lib`; `cargo test -p casars-imager task_contract --lib`; `just docs-check`; `git diff --check`

Wave issue: #263

This document is the consolidated closeout record for ImPerformance Wave 2
standard-MFS acceleration. It replaces the long acceleration ledger and the
separate streaming-tile and producer/consumer design notes. The goal is to keep
the useful results, rejected paths, and cross-mode lessons in one place before
similar questions recur in cube, mosaic, and other imaging modes.

## Retained Runtime Policy

The standalone imager now has an automatic standard-MFS acceleration planner:

```text
Default runtime policy: --standard-mfs-acceleration auto
CPU backend: fixed-tile multi-CPU when the standard-MFS workload is eligible
Default CPU worker cap: 4
Default CPU tile anchor: center_quadrants
macOS clean backend: grouped Metal initial dirty/PSF and residual refresh when eligible and memory-planned
Correctness/reference path: CPU standard-MFS path remains available
```

Every planned run logs `standard_mfs_runtime_plan` with the selected CPU
backend, worker count, tile anchor, Metal stage choices, and whether the choice
came from auto planning, a CLI override, or a legacy environment override.

Explicit overrides:

```text
--standard-mfs-acceleration auto|cpu|multi-cpu|metal
--standard-mfs-backend cpu|fixed_tile|metal|metal-row-run|metal-row-run-grouped
--standard-mfs-grid-threads N|auto
--standard-mfs-tile-anchor zero|center_boundary|center_quadrants
--standard-mfs-residual-backend cpu|metal|metal-row-run|metal-row-run-grouped
--standard-mfs-initial-dirty-backend cpu|metal-row-run-grouped
--standard-mfs-metal-grouped-input-cache true|false
```

The planner intentionally errs toward using the retained accelerated paths when
the workload is eligible and the central memory plan can account for them. This
will give future runs enough logged evidence to improve the planner rather than
leaving acceleration hidden behind environment variables.

## Retained CPU Shape

The retained CPU backend is the fixed-tile tile-inbox scheduler with four
center-quadrant halo tiles. The partition places the standard gridder's integer
center at the intersection of four tiles. This is only a load-balance heuristic:
correctness comes from deterministic ownership by the gridder's integer tap
center and full-halo merge into the global grid.

Retained CPU contracts:

- do not retain full-MeasurementSet prepared visibility plans;
- do not retain persistent per-sample `PositiveTapSet` plans;
- do not allocate full stage grids per worker;
- keep global MFS density and global output grids resident for standard MFS;
- preserve row-shaped MS data as row/channel visibility runs until worker-side
  gridding;
- push tile-local visibility runs into per-tile inboxes;
- have workers update resident tile buffers directly;
- flush resident tiles deterministically at stage or eviction boundaries;
- keep major buffers visible to the centralized standard-MFS memory planner.

Natural weighting can stream directly. Uniform, Briggs, and BriggsBwTaper keep
the exact two-pass logical shape: density and robust statistics first, then
weighted gridding. Density remains global for standard MFS in this wave.

## Retained Metal Shape

Metal is now an intended optional macOS backend track, not a detached prototype.
The retained production direction is grouped row-run work units with memory
planner accounting. The Swift harness under `tools/experiments/metal/` remains
the shader and benchmark workbench; Rust production integration is through the
standard-MFS backend boundary.

Retained Metal contracts:

- consume the same bounded row/run work-unit shape as the CPU path;
- do not use a full-MS retained visibility plan;
- do not use persistent per-sample tap plans;
- do not use CPU-expanded full tap-contribution lists as the production
  contract;
- account for grouped input cache, staging, device buffers, and readback in the
  central memory plan;
- keep CPU fallback/reference behavior available.

Direct global-atomic Metal scatter was rejected for the measured workloads.
Grouped row-run Metal is retained because it moved the heavy residual path from
CPU gridding into a smaller GPU kernel. The retained implementation stages the
same grid-coordinate convention that the CPU router used to form tap centers;
the Metal kernels must not recompute centers from a subtly different
floating-point path. The row-run ABI is versioned so cached grouped input is
invalidated when the lane layout changes.

The current next high-value boundary is still CPU-side grouped input
construction. The next Metal redesign should move center calculation, lane
grouping, and group finalization onto the device or otherwise remove that
host-cache work.

## Key Measurements

| Workload | Artifact | Frontend | Core | Important stages | Peak memory | Correctness | Decision |
| --- | --- | ---: | ---: | --- | ---: | --- | --- |
| full-shape one-worker, 512-channel, 2048 image, Briggs, `niter=2` | `target/imperformance-wave2/single-worker-full-shape-20260522/full-shape-one-worker-fixed-tile-sample-stream.log` | 328.937s | 294.998s | PSF 69.104s; residual 218.544s; major refresh 149.613s | 10.27GB | GREEN vs saved CASA `.image`, `.residual`, `.psf`, `.model` | retained memory-controlled full-shape path |
| heavy CPU clean, 64-channel, 1024 image, Briggs, `niter=500`, `minor_cycle_length=50`, 4 workers | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w.log` | 97.81s | 85.17s | PSF 3.86s; residual 70.81s; major refresh 67.49s; residual worker utilization 50-51% | 9.55GB | GREEN targeted tests | retained CPU baseline winner |
| final heavy Metal clean, grouped initial dirty/residual cache path | `target/imperformance-wave2/ms-read-parallel-20260525/heavy-final-check.log` | 18.746s | 5.115s | prepare 9.631s; get MS values 5.363s; prepare buffer 4.268s; PSF 0.459s; residual 2.784s; major refresh 2.484s | about 9.55GiB | GREEN targeted tests | retained accelerated screen before paired CASA product audit |
| heavy 1k paired audit, 64-channel, 1024 image, 0.25arcsec cell, Briggs, `niter=500`, auto Metal + 4 CPU | `target/imperformance-wave2/medium-divergence-20260525/niter_500` | 41.790s CLI; 41.527s profiler frontend | 23.930s | prepare 13.131s; PSF 1.821s; residual 16.436s; major refresh 15.215s; minor 2.890s | not recorded | GREEN vs CASA: image `1.72e-5`, residual `3.53e-5`, model `1.02e-5`, psf `3.04e-5` RMS ratios | retained paired correctness gate |
| heavy 1k direct recheck, same parameters as paired audit | `target/imperformance-wave2/heavy-1k-current-rust-timed/rust` | 35.47s CLI | n/a | auto planner selected fixed-tile 4 CPU plus grouped Metal dirty/PSF and residual | not recorded | completion check only | confirms corrected path remains in the old 34s speed class outside comparison harness |
| sparse touched-group Metal host grouping | `target/imperformance-wave2/structural-followups-20260525/heavy-sparse-metal-groups.log` | 21.873s | n/a | prefill append 2.33s; group finalize 711ms vs 546ms reference | n/a | GREEN targeted tests | rejected |

The full-shape product check compared `.image`, `.residual`, `.psf`, and
`.model` against saved CASA products. Max absolute normalized deltas remained
small: `.image` `1.85e-5` of CASA peak, `.residual` `3.26e-6`, `.psf`
`2.10e-6`, and `.model` `3.00e-7`.

The final paired 1k `niter=500` CASA comparison used the same 64-channel medium
VLA MeasurementSet and showed CASA `tclean` at 733.660s versus casa-rs auto at
41.790s in the comparison harness, or 35.47s in a direct timed CLI recheck. The
paired-harness speedup is 17.6x; the direct-rust timing against the same CASA
run is 20.7x. An earlier 34.122s / 1100.377s artifact produced a nominal 32.3x
ratio, but that artifact is not retained as correctness evidence: PSF matched,
while the image, residual, and model products diverged materially. The retained
Wave 2 claim is therefore the corrected Metal path with CASA-compatible product
tolerance, not the faster-but-wrong intermediate artifact.

## What Worked

| Change | Evidence | Retained lesson |
| --- | --- | --- |
| Central memory target and planner accounting | Full-shape fixed-tile run stayed around 10.27GB RSS under the 16GiB target. | Make active buffers visible to the standard-MFS memory plan before claiming scalability. |
| Streaming fixed-tile memory repair | Removed the rejected full-MS retained visibility shape and transient per-task tile buffers. | Runtime memory must scale with bounded row/run queues and resident tiles, not with the full MeasurementSet. |
| Row/channel visibility runs | Replaced scalar inbox payloads and preserved the MS row-shaped data model. | Do not scalarize visibilities before the tile worker unless instrumentation proves the vector shape is the bottleneck. |
| Direct resident tile updates | Removed millions of scratch tile buffers and main-thread scratch merges. | Workers should update resident tile buffers directly; private tile scratch is a hot-splitting tool, not the default. |
| Center-quadrant CPU tiling | Beat fine tiles and adaptive chunks on the measured VLA medium target. | A small number of coarse centered tiles gave the best demonstrated CPU balance/overhead tradeoff. |
| Largest-ready tile scheduling | Improved the four-quadrant path versus FIFO-like behavior. | Schedule by ready tile work; do not wait for arbitrary queue thresholds when useful work is available. |
| Worker-side final Briggs weighting and hot inlining | Reduced producer work and helped bounded serial/multi-worker paths. | Producer should route work; lane-level imaging semantics belong near the worker kernel. |
| Direct row-run publish and shared tap centers | Reduced queued payloads and staging overhead. | The producer/core boundary should pass compact row/run work units, not duplicate scalar vectors. |
| Grouped Metal row-run residual refresh | Moved residual refresh into a much smaller GPU kernel and enabled cache reuse across major cycles. | Metal is promising only with grouped/reduced work units, not direct global atomic scatter. |
| Metal grouped input cache and no-copy cache hits | Heavy run reached 18.746s frontend with controlled memory. | Cache production-shaped grouped input when the planner can budget it; avoid duplicate resident Metal buffers without wall gain. |
| CPU-grid-coordinate staging for Metal row runs | `target/imperformance-wave2/medium-divergence-20260525/niter_500` matches CASA at `1e-5` to `4e-5` RMS ratios for image/model/residual/PSF. | Metal row-run kernels must consume the same discrete grid-coordinate convention used by the CPU router; do not recompute it independently on device. |

## What Did Not Work

| Candidate | Evidence | Result | Reason rejected |
| --- | --- | ---: | --- |
| coarse 256 zero-anchored tiles | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge256-zero.log` | 50.897s frontend; hottest tile 53.95% | A single tile capped useful parallelism. |
| fine 64 center-boundary tiles | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge64-center.log` | 41.477s frontend | Useful diagnostic, but beaten by simpler center quadrants. |
| fine 32 center-boundary tiles | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge32-center.log` | 39.693s frontend | Better load balance, but more tile, halo, and scheduler overhead than the retained coarse path. |
| scalar direct inbox | `target/imperformance-wave2/run-inbox-20260523/medium-briggs-run-inbox-summary.md` | 258.34s wall; 1020.53s sys; 14.4M drains | Per-scalar synchronization dominated useful gridding. |
| ready threshold 1 | `target/imperformance-wave2/run-inbox-threshold-20260523/medium-briggs-ready-min1-10w.log` | 97.56s frontend | Too many ready heads and drains; scheduler traffic dominated. |
| temporal hot splitting | `target/imperformance-wave2/hot-split-20260524/medium-briggs-temporal-split-profile-imager-hogbom-10w.log` | 26.40s vs 21.20s reference | Private whole-tile scratch and reduction cost exceeded the idle-tail benefit. |
| spatial hot splitting | `target/imperformance-wave2/spatial-split-20260524/medium-briggs-hot-tile3-quadtree-4w.log` | 19.56s dirty screen; heavy rerun effectively flat | Small/noisy gains did not justify keeping manual split code. |
| adaptive per-chunk equal-work tiles | `target/imperformance-wave2/adaptive-chunks-20260524/medium-briggs-niter500-cycle50-adaptive-4w.log` | 193.52s | Repeated planning/routing per chunk and residual refresh dominated. |
| adaptive partition cache | `target/imperformance-wave2/adaptive-chunks-20260524/medium-briggs-niter500-cycle50-adaptive-cache-4w.log` | 152.88s | Better than uncached adaptive, still far slower than center quadrants. |
| deep queue target | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w-deepqueue10m.log` | 118.99s | More queued memory did not improve utilization enough to offset overhead. |
| residual metadata cache | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w-metadata-cache.log` | 95.90s first run; 98.78s RSS rerun | Modest/noisy signal with about 6.10GB logical cache; any future equivalent must be planner-budgeted. |
| direct global-atomic Metal row-run | `target/imperformance-wave2/structural-followups-20260525/heavy-residual-metal-row-run.log` | 51.329s frontend | Repeated reread/reroute plus global atomics dominated. |
| sparse touched-group host grouping | `target/imperformance-wave2/structural-followups-20260525/heavy-sparse-metal-groups.log` | 21.873s frontend | Sorting/touched tracking cost more than dense counter scanning. |
| broad MS/Table read rewrite screen | `target/imperformance-wave2/structural-followups-20260525/ms-read-storage-profile-one-block.log` | 3.53GiB logical payload in 1.869s | Raw storage read was not dominant enough for this workload to justify the rewrite in Wave 2. |
| specialized selected-correlation/channel packing | `target/imperformance-wave2/ms-read-parallel-20260525/heavy-packed-corr-metal-cache.log`; `target/imperformance-wave2/ms-read-parallel-20260525/heavy-metal-complex-bulk-copy.log` | 19.360s and 21.145s frontend | Packing changes worsened the measured heavy path. |
| pre-fix 34s Metal paired artifact | `target/imperformance-wave2/heavy-1k-niter500-cell025-paired-20260525` | 34.122s casa-rs; 1100.377s CASA | Rejected as final evidence despite speed: image diff RMS was 0.130 of CASA RMS, residual 0.255, and model 0.064 in the raw comparison. |

## Design Lessons To Carry Forward

The most important cross-mode lessons are structural:

- Preserve bounded residency. Cubes and mosaics must not inherit any design that
  assumes all planes, all visibilities, or all per-sample tap plans fit in
  memory.
- Keep row-shaped data from the MS as long as possible. Scalar payloads created
  synchronization and duplicate work.
- Separate routing from imaging semantics. The producer should read, classify
  enough to route, and enqueue; workers should do flags, weights, tap planning,
  gridding, degridding, and scalar reductions whenever practical.
- Stage-level UV distributions are not enough. Live queue snapshots showed that
  active work moves dynamically; static hot-tile decisions often helped less
  than expected.
- More memory did not automatically help. Deeper tile queues and larger caches
  only matter when they convert producer-only time into worker/producer overlap
  without adding routing or RSS cost.
- Hot splitting should be explicit and measured. Small fairness drains and
  temporal scratch are not substitutes for a spatial split whose scratch and
  reduction costs are budgeted.
- Metal needs a grouped/reduced input contract. If center calculation,
  lane grouping, and group finalization stay as host work, kernel tuning alone
  will not move the total wall time much.

## Current Follow-Up Direction

The retained Wave 2 path is good enough to use routinely through the runtime
planner. Future work should not revive the rejected experiment surfaces unless
new instrumentation changes the cost model.

Most promising next steps:

- teach the runtime planner from real run logs and workload features;
- redesign the Metal grouped input contract so more center/group/finalize work
  happens on device or in a lower-copy producer path;
- carry the fixed-tile memory planner discipline into cube/slab execution;
- add cross-mode profiling artifacts that report the same high-level fields:
  frontend, core, prepare, grid/degrid, major refresh, peak memory, selected
  backend, worker count, and product correctness.
