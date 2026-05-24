# ImPerformance Wave 2 Standard-MFS Experiment Summary

Truth class: current descriptive
Last reality check: 2026-05-24
Verification: `cargo check -p casa-imaging -p casars-imager`

Wave issue: #263

This note records the standard-MFS CPU scheduling experiments from Wave 2 and
the retained backend decision. The purpose is to preserve the reasoning for
future imaging modes without keeping every experiment as a production code path.

## Retained Backend

The retained CPU backend is the fixed-tile tile-inbox scheduler with four
center-quadrant tiles:

```text
Backend: fixed-tile standard-MFS CPU path
Tile anchor: CASA_RS_STANDARD_MFS_TILE_ANCHOR=center_quadrants
Worker count used for current retained measurements: CASA_RS_STANDARD_MFS_GRID_THREADS=4
Residual metadata cache: removed from the retained path
Manual hot splitting, temporal splitting, adaptive chunks, queue snapshots: removed or deferred
```

The center-quadrant partition places the standard gridder's integer center at
the intersection of four fixed tiles. It is not a physical-correctness
assumption; it is a load-balance heuristic. Correctness still comes from
deterministic ownership by the gridder's integer tap center and full-halo
merging into the global grid.

The best retained heavy-clean measurement is:

| Workload | Artifact | Frontend | Core | PSF grid | Residual grid/degrid | Major refresh | Worker utilization | Peak RSS | Decision |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | --- |
| 64-channel, 1024-pixel, Briggs, multiscale, `niter=500`, `minor_cycle_length=50`, 4 workers, center quadrants | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w.log` | 97.81s | 85.17s | 3.86s | 70.81s | 67.49s | 50-51% residual stages | 9.55GB | retained |

## What Worked

| Change | Result | Retained lesson |
| --- | --- | --- |
| Streaming fixed-tile memory repair | Full-shape 10-worker run stayed near 10.2GB RSS under a 16GiB target. | Standard MFS can avoid full-MS visibility retention and full-grid-per-worker allocation. |
| Row/channel visibility runs | Avoided scalar per-visibility queue payloads and kept the row-shaped MS data model intact. | Queue row/channel runs, not scalar visibilities, when the MS gives row-shaped arrays. |
| Center-quadrant tiles | Medium dirty and heavy-clean paths beat fine-tile and split variants. | For the current VLA medium target, four coarse center quadrants are the best demonstrated CPU tiling strategy. |
| Largest-ready tile scheduling | Better than FIFO for quadrant queues. | Pick the ready tile with the most queued work; do not wait for arbitrary chunk thresholds when work is already available. |
| Direct resident tile updates | Removed millions of transient tile buffers and kept memory controlled. | Workers should update resident tile buffers directly unless a measured hot-splitting path justifies private scratch. |

## What Did Not Work

| Candidate | Evidence | Result | Reason rejected |
| --- | --- | ---: | --- |
| Coarse 256 zero-anchored tiles | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge256-zero.log` | 50.897s frontend; hottest tile 53.95% | A single tile capped useful parallelism. |
| Fine 64 center-boundary tiles | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge64-center.log` | 41.477s frontend | Useful diagnostic, but beaten by simpler center quadrants. |
| Fine 32 center-boundary tiles | `target/imperformance-wave2/fine-tile-scheduler-20260521/bounded-10w-edge32-center.log` | 39.693s frontend | Better load balance, but more tile/halo/scheduler overhead than the retained coarse path. |
| Scalar direct inbox | `target/imperformance-wave2/run-inbox-20260523/medium-briggs-run-inbox-summary.md` | 258.34s wall, 1020.53s sys, 14.4M drains | Per-scalar synchronization dominated useful gridding. |
| Run inbox with ready threshold | `target/imperformance-wave2/run-inbox-threshold-20260523/medium-briggs-threshold-10w.log` | 38.45s wall | Better than scalar, still not better than center-quadrant retained path. |
| Temporal hot splitting | `target/imperformance-wave2/hot-split-20260524/medium-briggs-temporal-split-profile-imager-hogbom-10w.log` | 26.40s vs 21.20s reference | Private whole-tile scratch buffers and merges added more work than parallelism removed. |
| Spatial hot split of tile 3 | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-hot-tile3-4w.log` | 97.25s vs 97.81s retained baseline | Tiny, unstable gain; not enough to keep manual split code. |
| Adaptive per-chunk equal-work tiles | `target/imperformance-wave2/adaptive-chunks-20260524/medium-briggs-niter500-cycle50-adaptive-4w.log` | 193.52s | Planning/routing repeated per chunk and per residual refresh dominated. |
| Adaptive partition cache | `target/imperformance-wave2/adaptive-chunks-20260524/medium-briggs-niter500-cycle50-adaptive-cache-4w.log` | 152.88s | Faster than uncached adaptive, still far slower than center quadrants. |
| Deep queue target | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w-deepqueue10m.log` | 118.99s | More queued memory did not improve utilization enough to offset overhead. |
| Residual metadata cache | `target/imperformance-wave2/heavy-clean-20260524/medium-briggs-niter500-cycle50-center-quadrants-4w-metadata-cache.log` | 95.90s first run; 98.78s RSS rerun | Modest/noisy speed signal with about 6.10GB logical cache. Removed until the memory planner can budget any equivalent cache explicitly. |

## Cross-Mode Lessons

- Do not keep the full MeasurementSet or persistent per-sample tap plans in
  memory. Queue bounded work and keep queue memory visible to the central
  planner.
- Preserve row-shaped data from the MS as long as possible. Scalarizing too
  early creates duplicate work and synchronization overhead.
- Avoid solving hot tiles with private full-tile scratch unless the scratch and
  reduction cost is already known to be smaller than the idle tail.
- Static stage-level UV distributions are not enough. The row-order and live
  queue distributions can be much more dynamic than the whole-stage picture.
- Equal-work adaptive tiling needs a cheap planning contract before it can help.
  Recomputing tile geometry and rerouting for every major-cycle refresh is too
  expensive.
- Queue visualizations were useful diagnostics, but they are not retained as a
  production profiling interface. Keep future visualization hooks offline and
  artifact-driven unless they are explicitly budgeted.

## Current Follow-Up Direction

The next CPU work should start from the retained four-quadrant path. Likely
future work is narrower instrumentation around producer/router cost and tile
worker useful compute, then measured changes that improve that retained path.
GPU and cube/slab work should consume the same bounded row-shaped work-unit
ideas, not the rejected scalar or full-grid-per-worker shapes.
