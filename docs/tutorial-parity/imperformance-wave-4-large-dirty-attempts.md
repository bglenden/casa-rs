# ImPerformance Wave 4 Large Dirty Attempts

Truth class: evidence log
Last reality check: 2026-06-16
Verification: Wave 4 benchmark JSONs under `/Volumes/GLENDENNING/casa-rs-imperformance/_tmp_safe_to_delete`

## Baseline

Current best large dirty spectral cube run:

- Run: `20260616T150436Z-wave1-alma-mosaic-large-standard-cube-line-0531c480`
- Runtime: 524.143 s
- Plan: slab-first, 120 active planes, 9 slabs, 308525 row block rows, 10 workers
- Memory target: 24.000 GB decimal, with 7.858 GB visibility/source-side buffers and 14.205 GB slab/product-side scratch
- Modeled I/O: 153.781 GB total, 16.342 GB source read, 137.439 GB product write
- Measured stages: 37.337 s source read, 481.638 s backend, 58.435 s product write
- Plane timing: 4.782 s median, 6.115 s p95
- Peak RSS: 14.170 GB

## Closeout Evidence

The 24 GB decimal memory-cap run fixes the previous large regression and is now
the best large dirty cube row. Whole-plane product writes are active and
efficient at the storage boundary. The run is still dominated by the per-plane
backend path, especially the scalar FFT pair.

| Workload | Run | Rust s | CASA s | Speedup | Source read s | Backend s | Product write s | Source read GB | Product GB | Modeled I/O GB | Schedule | Slabs | Active planes | Workers | Visibility-side GB | Slab/product GB | Plane median / p95 s | Peak RSS GB | Product tile counters |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Large previous best | `20260614T215220Z-wave1-alma-mosaic-large-standard-cube-line-de8dae03` | 572.315 | n/a | n/a | 34.293 | 533.973 | 172.592 | 15.989 | 137.439 | 153.532 | slab-first | 3 | 347 | 10 | 18.233 | 11.771 | 5.203 / 6.423 | 17.250 | older log shape; no direct counters |
| Large regressed 30 GB plan | `20260615T042423Z-wave1-alma-mosaic-large-standard-cube-line-1727f925` | 923.208 | n/a | n/a | 34.194 | 883.179 | 118.526 | 15.982 | 137.439 | 153.490 | slab-first | 2 | 514 | 10 | 20.165 | 9.839 | 8.808 / 9.716 | 18.498 | 2048 full-plane direct writes, 2048 direct tiles, 1024 Fortran calls, 0 C-order calls, 0 LRU reads, 0 zero-fill |
| Large current 24 GB cap | `20260616T150436Z-wave1-alma-mosaic-large-standard-cube-line-0531c480` | 524.143 | n/a | n/a | 37.337 | 481.638 | 58.435 | 16.030 | 137.439 | 153.781 | slab-first | 9 | 120 | 10 | 7.858 | 14.205 | 4.782 / 6.115 | 14.170 | 2048 full-plane direct writes, 2048 direct tiles, 1024 Fortran calls, 0 C-order calls, 0 LRU reads, 0 zero-fill |
| Medium current | `20260615T044356Z-wave1-vla-single-medium-standard-cube-line-f77694da` | 165.212 | 1918.001 | 11.609x | 43.234 | 110.209 | 7.164 | 28.492 | 17.180 | 45.919 | slab-first | 2 | 256 | 10 | recorded in JSON | recorded in JSON | 2.064 / 3.201 | 18.581 | 1024 full-plane direct writes, 1024 direct tiles, 512 Fortran calls, 0 C-order calls, 0 LRU reads, 0 zero-fill |

Medium correctness from the closeout bundle:

| Product | Structured label | Difference summary |
| --- | --- | --- |
| `.image` | good | RMS difference is 6.82e-7 of CASA RMS; correlation is effectively 1.0. |
| `.residual` | good | RMS difference is 6.82e-7 of CASA RMS; correlation is effectively 1.0. |
| `.psf` | good | RMS difference is 5.94e-6 of CASA RMS; correlation is effectively 1.0. |
| `.sumwt` | investigate | Absolute differences are tiny, with RMS difference 1.01e-8 of CASA RMS and max difference 8.10e-8 of CASA peak; the structured checker flags it for review because the product is low dimensional and highly uniform. |

Closeout decision:

- Keep the 24 GB decimal memory cap as the current laptop-safe large-row target. It lowered peak RSS to 14.170 GB and improved wall time to 524.143 s, better than both the 923.208 s regressed run and the previous 572.315 s best baseline.
- Do not spend more W4-14 time on slab-first versus source-first for large dirty unless source reads become dominant. The current source-read bucket is 37.337 s out of 524.143 s, about 7% of wall time.
- Keep whole-plane product tiles and the zero-copy direct write path. Product write is now 58.435 s for 137.439 GB, with direct full-plane writes and no readback, zero-fill, or C-order conversion.
- Treat the remaining large dirty cost as a per-plane backend problem. Median plane time is 4.782 s and median core time is 4.172 s; the median FFT pair is 2.977 s, larger than replay, grid update, and normalization/correction combined.
- The next distinct architectural bet remains source-major or batched plane execution, tracked as optional Wave 4 follow-up #311: one pass over shared source blocks should feed multiple plane grids, with shared geometry/tap work and batched FFT/backend execution where possible. Continuing isolated scalar plane workers is unlikely to recover the next factor.

## Attempts

### Direct full physical tile overwrite

Status: rejected

Hypothesis: for full-plane product writes, skipping LRU tile read or zero-fill before complete physical tile overwrite would reduce product write time.

Implementation tested: fresh LRU slots could be marked dirty without read or zero-fill when a write covered the full physical tile.

Evidence:

- Run: `20260614T221210Z-wave1-alma-mosaic-large-standard-cube-line-cfb34597`
- Runtime worsened from 572.315 s to 761.280 s.
- Source read worsened from 34.293 s to 82.477 s in this noisy sample.
- Backend worsened from 533.973 s to 672.478 s.
- Product write worsened from 172.592 s to 182.785 s.
- Plane median worsened from 5.203 s to 6.539 s; p95 worsened from 6.423 s to 9.101 s.
- Peak RSS increased from 17.250 GB to 18.212 GB.

Decision: do not keep this path. Product write did not improve, total wall time regressed materially, and worker timing regressed. The code change was reverted.

### Deep product tile batching

Status: rejected

Hypothesis: use multi-plane product tiles and hold product results in batches so `.image` and `.residual` writes coalesce fewer, deeper tile flushes.

Implementation tested: planner modeled a product group overhead term and selected `product_batch_planes=10` for the large dirty cube. The product tile shape depth became 10 planes for image-like products.

Evidence:

- Run: `20260614T224929Z-wave1-alma-mosaic-large-standard-cube-line-54221e5e`
- Status: failed, `scripts/bench-imager-vs-casa.sh` reported `Killed: 9`; the benchmark JSON recorded Rust status `not_run`.
- The run only completed unique planes 0 through 228 of slab 0 before termination.
- Plan shifted from 347 active planes, 3 slabs, 11.771 GB product scratch to 258 active planes, 4 slabs, 16.336 GB product scratch.
- Slab 0 source read was 4.030 GB in 38.617 s; extrapolated source time would no longer be negligible.
- Partial plane median worsened from 5.203 s to 5.963 s; partial p95 worsened from 6.423 s to 9.009 s.
- Partial average per-plane FFT buckets were 1.746 s PSF FFT and 1.843 s residual FFT.

Decision: do not keep this planner/writer path. The deep product batch consumed too much scratch, reduced active-plane residency, increased slab count, worsened observed per-plane timing, and failed before a complete large run. Future product write work should avoid holding many full plane results pending and should instead target true bulk/tile write APIs or lower-copy direct tile overwrites with bounded memory.

### Row-tile direct product writes

Status: rejected as default; keep the product-write evidence

Hypothesis: choose image product tiles shaped as full rows, `[1, ny, 1, 1]`, so each C-order plane is tile-contiguous and the direct tiled writer can avoid per-128x128-tile transpose work.

Implementation tested: full-plane product writes used `TiledFileIO::put_aligned_c_order_tiles`, which writes complete tile-aligned planes directly to the tiled data file and records direct-write counters. The row-tile shape made each plane copy contiguous before the direct file write.

Evidence:

- Medium probe run: `20260614T231356Z-wave1-vla-single-medium-standard-cube-line-f18b845b`
- Medium product write improved to 8.925 s for 17.180 GB, versus 23.072 s with the 128x128 direct-tile probe.
- Medium total runtime was not useful evidence because the source-read bucket spiked in that sample.
- Large run: `20260614T231826Z-wave1-alma-mosaic-large-standard-cube-line-961420d9`
- Runtime worsened from 572.315 s to 677.660 s.
- Product write improved from 172.592 s to 74.770 s for the same 137.439 GB.
- Direct writes covered 137.439 GB through 2048 direct write calls and 8388608 row tiles.
- Peak RSS increased from 17.250 GB to 18.005 GB.
- Slab plane execution regressed: slab wall times were 210.667 s, 197.270 s, and 229.136 s, with a late-run slowdown where many planes rose above 8-12 s.

Decision: do not keep row-tile product layout as the default. It gives a real product-write improvement but regresses the large dirty wall clock by about 105 s, so the backend/cache side effect is larger than the write gain. Restore the prior bounded spatial tile shape for default cube products. Future product-write work should preserve backend layout behavior and isolate direct writes behind a path that does not increase per-plane FFT/grid/correction cost.

### Planned-sample compact tap payload and stage split

Status: pending evidence

Hypothesis: planned scalar samples should carry the compact prolate-spheroidal tap identity needed by the scalar grid-update loop. Recomputing compact taps from `(u,v)` inside each worker is redundant per-sample backend work and hides inside the broad grid bucket.

Implementation under test:

- `StandardMfsPlannedWeightedSample` now carries `x_start`, `y_start`, `x_weight_index`, and `y_weight_index`.
- `StandardMfsPlannedSampleBuilder::plan_sample` computes the compact tap identity once while constructing the planned sample.
- The scalar planned-sample PSF, dirty residual, and residual-refresh loops consume those compact tap fields directly.
- Core diagnostics now split `planned_sample_replay` from `grid_update`, while preserving the existing aggregate `psf_grid` and `residual_degrid_grid` timers.

Verification so far:

- `cargo test -p casa-imaging fft --lib`
- `cargo test -p casa-imaging gridder --lib`
- `cargo test -p casars-imager managed_output --lib`
- `cargo test -p casa-images put_slice_view_direct_writes_aligned_tiled_plane --lib`
- `python3 -m pytest tools/perf/imager/test_run_workload.py -q`
- `cargo build --release -p casars-imager`

Timing status: diagnostic only, not acceptance evidence. At 2026-06-14 17:59 local time, `uptime` reported load averages `10.58 7.73 8.98`; `spotlightknowledged`, `backupd`, Codex helper processes, and `syspolicyd` were consuming substantial CPU. A medium dirty diagnostic was still run to validate counters:

- Run: `20260615T000217Z-wave1-vla-single-medium-standard-cube-line-5dce0de2`
- Runtime: 202.640 s.
- Source read: 76.183 s, versus 42.154 s in the earlier medium direct-tile probe.
- Product write: 31.268 s, versus 23.072 s in the earlier medium direct-tile probe.
- Product bytes: 17.180 GB through 1024 direct-write calls and 262144 direct tiles.
- Plane median/p95: 2.052 s / 3.113 s.
- Median per-plane core split: total 1.988 s, sample replay 0.729 s, grid update 0.572 s, PSF FFT 0.267 s, residual FFT 0.284 s.
- Peak RSS: 19.161 GB.

Decision: do not classify this total runtime as performance evidence because the source and product buckets were materially slower than prior medium probes under a known noisy system load. Keep the counter split. It shows sample replay/build and grid update are both material per-plane backend costs, and it gives a concrete target for a shared planned-sample or batched-plane backend optimization.

### Storage-order product planes and direct Fortran tiled writes

Status: pending timing evidence

Hypothesis: the default 128x128 product tile shape is better for the rest of the imaging run than row tiles, but serial product writing still pays a costly C-order plane to Fortran-order tile transpose. If final plane products are expanded into Fortran-memory-order arrays inside the existing plane workers, the writer can pack product tiles with contiguous row copies instead of a per-element serial transpose. This should preserve CASA-visible shape and tile layout while moving unavoidable layout work out of the serial product writer.

Implementation under test:

- `expand_plane` now creates `(nx, ny, 1, 1)` product arrays in Fortran memory order.
- `PagedImage::put_slice_view` detects Fortran-contiguous full-plane views and routes them to a direct tiled writer path before the C-order fallback.
- `TiledFileIO::put_aligned_fortran_order_tiles` writes full tile-aligned spatial planes directly to the TSM file.
- Direct tiled writer diagnostics now split `tiled_direct_pack_ns`, `tiled_direct_swap_ns`, and `tiled_direct_write_ns`, while keeping direct write call/tile/byte counters.

Verification so far:

- `cargo fmt --check`
- `cargo test -p casa-images put_slice_view_direct_writes --lib`
- `cargo test -p casa-imaging gridder --lib`
- `cargo test -p casa-tables direct --lib`
- `cargo test -p casars-imager cube_dirty_multi_channel_slab_runner_writes_casa_products --lib`
- `python3 -m pytest tools/perf/imager/test_run_workload.py -q`
- `cargo build --release -p casars-imager`

Timing status: medium diagnostic complete, large run in progress. At 2026-06-14 18:23 local time, `spotlightknowledged` was still near one full core and `backupd` was consuming substantial CPU, so the medium dirty run below is diagnostic for total wall time, but useful for the product writer counters.

Evidence:

- Medium run: `20260615T002513Z-wave1-vla-single-medium-standard-cube-line-ca211e53`
- Runtime: 206.750 s.
- Source read: 77.348 s, versus 42.154 s in the earlier medium direct-tile probe and 76.183 s in the noisy compact-tap probe.
- Product write: 13.996 s for 17.180 GB, versus 23.072 s in the earlier 128x128 direct-tile probe and 31.268 s in the noisy compact-tap probe.
- Direct write split: 5.470 s pack, 0.000 s swap, 8.138 s physical write.
- Direct write coverage: 17.180 GB through 1024 direct write calls and 262144 direct tiles.
- Product writer no longer used the C-order direct path: `tiled_c_order_calls=0`.
- Plane median/p95: 2.275 s / 2.918 s.
- Median per-plane core split: total 1.991 s, sample replay 0.727 s, grid update 0.560 s, PSF FFT 0.236 s, residual FFT 0.255 s.
- Peak RSS: 18.903 GB.

Interim decision: keep this change for large evidence. It preserves the default 128x128 spatial product tile shape, removes the C-order serial transpose path from image-like dirty products, and improves the medium product-write bucket by about 9.1 s relative to the earlier comparable 128x128 direct-tile probe. Do not use the medium total wall time as acceptance evidence until the host is less noisy.

### Dirty-only cube products

Status: keep as a structural product-write change; total timing evidence is contaminated

Hypothesis: dirty cube imaging should not materialize or write clean-only image products. For `niter=0`, `.image` is a CASA-compatible alias of `.residual`, and `.model` is empty. Removing those arrays from the dirty worker result and product writer should cut dirty product traffic and serial product work without changing `.psf`, `.residual`, `.sumwt`, or the `.image` alias.

Implementation under test:

- Dirty shared cube planes now return `DirtyImagingResult`, carrying only `.psf`, `.residual`, `.sumwt`, beam metadata, and diagnostics.
- Dirty cube slab publishing writes only `.psf`, `.residual`, and `.sumwt`; it keeps the existing `.image` hard-link alias to `.residual` at finish time and removes `.model`.
- Product memory planning charges dirty plane result scratch for two image-like planes instead of the clean four-image-product result.
- The existing shared read-only slab source and one-worker-per-plane execution path are unchanged.

Verification so far:

- `cargo fmt --check`
- `cargo test -p casa-images put_slice_view_direct_writes --lib`
- `cargo test -p casa-imaging gridder --lib`
- `cargo test -p casa-tables direct --lib`
- `cargo test -p casars-imager cube_dirty_multi_channel_slab_runner_writes_casa_products --lib`
- `python3 -m pytest tools/perf/imager/test_run_workload.py -q`
- `cargo build --release -p casars-imager`

Evidence:

- Medium run: `20260615T005443Z-wave1-vla-single-medium-standard-cube-line-45018408`
- Runtime: 286.244 s. Do not use this as acceptance evidence: during follow-up inspection, `tmutil status` reported Time Machine `backupd` actively copying a 443 GB backup with roughly 8201 s remaining, and `ps` showed substantial unrelated CPU load.
- Source read: 80.094 s, versus 42.154 s in the earlier medium direct-tile probe.
- Product write: 15.323 s for 17.180 GB, with only dirty `.psf` and `.residual` image-like products written.
- Direct write split: 6.828 s pack, 0.000 s swap, 8.263 s physical write.
- Direct write coverage: 17.180 GB through 1024 direct write calls and 262144 direct tiles.
- Product writer did not use the C-order direct path: `tiled_c_order_calls=0`, `tiled_fortran_calls=512`.
- Plane median/p95: 3.915 s / 4.644 s, contaminated in the same direction as source read and FFT/gridding buckets.
- Median per-plane core split: total 3.720 s, sample replay 1.338 s, grid update 1.050 s, PSF FFT 0.433 s, residual FFT 0.469 s.
- Peak RSS: 18.567 GB.

Decision: keep this code path as a product-write and architecture cleanup, but do not claim a total-wall performance win from the contaminated medium run. The useful product-write result is that dirty product bytes are now the two image-like products that are actually needed. The next clean-host large run should test whether large dirty product write drops from the old 137.439 GB / 172.592 s bucket to roughly the expected 34.36 GB scale before further product batching work is considered.

### Centered-IFFT checkerboard shift removal

Status: keep; large dirty timing still backend-bound

Hypothesis: the large dirty per-plane FFT bucket includes avoidable full-grid quadrant swaps. For even grid shapes, `ifft(ifftshift(F))` is equivalent to `ifft(F)` followed by a checkerboard sign. Applying the sign during the existing FFT scale pass should remove one full-grid `ifftshift` per PSF and residual FFT without changing the centered image semantics.

Implementation under test:

- `centered_ifft2_f64_owned_unshifted_even` now detects even contiguous grids and runs the inverse FFT directly on the centered-frequency grid.
- The previous pre-FFT quadrant swap is replaced by `(-1)^(x+y)` folded into the inverse-FFT scale pass.
- The fallback path for odd or non-contiguous grids is unchanged.

Verification so far:

- `cargo test -p casa-imaging fft --lib`
- `cargo fmt --check`
- `cargo test -p casars-imager spectral_slab --lib`
- `cargo build --release -p casars-imager`

Evidence:

- Medium run: `20260615T021214Z-wave1-vla-single-medium-standard-cube-line-e00aaf6d`
- Runtime: 165.103 s.
- Product write: 8.596 s for 17.180 GB through 1024 full-plane direct write calls.
- Plane median/p95: 1.750 s / 4.343 s.
- Median per-plane core split: total 1.686 s, FFT 0.440 s, grid 1.115 s.
- Peak RSS: 18.408 GB.
- Large run: `20260615T021534Z-wave1-alma-mosaic-large-standard-cube-line-d6e03f00`
- Runtime: 636.612 s, worse than the 572.315 s baseline but better than grouped whole-plane's 679.452 s.
- Product write: 158.687 s for 137.439 GB through 2048 full-plane direct write calls.
- Product write split: 18.412 s pack, 0.000 s swap, 140.137 s physical write.
- Plane median/p95: 5.541 s / 6.930 s.
- Median per-plane core split: total 4.958 s, FFT 3.358 s, grid 0.773 s.
- Peak RSS: 18.035 GB.

Decision: keep the centered-IFFT change and whole-plane product tile layout for now, but treat the large result as evidence that product-result dataflow still is not exploiting whole-plane writes well enough. The large run remains dominated by per-plane backend work and full-plane physical write bandwidth; more slab/source scheduling work is not indicated.

### Whole-plane product tiles without multi-plane result grouping

Status: keep as the current default; do not re-enable multi-plane product grouping without a lower-copy executor path

Hypothesis: whole-plane product tiles are still the right product layout, but the previous grouped whole-plane run regressed because it merged many plane results into new `Array4` product groups before writing. Keep whole-plane physical product tiles while publishing and writing one plane at a time.

Implementation tested:

- `ExecutorScratchShape::max_product_batch_planes` now returns `1`.
- Product tiles remain full-plane tiles.
- The executor still writes through the direct Fortran full-plane path.

Evidence:

- Medium run: `20260615T021214Z-wave1-vla-single-medium-standard-cube-line-e00aaf6d`
- Runtime: 165.103 s, slower than grouped whole-plane's 150.2 s but faster than the pre-batch 181.8 s reference.
- Product write: 8.596 s, with 512 product groups and 1024 full-plane direct writes.
- Large run: `20260615T021534Z-wave1-alma-mosaic-large-standard-cube-line-d6e03f00`
- Runtime: 636.612 s, better than grouped whole-plane's 679.452 s but still worse than the 572.315 s baseline.
- Product write: 158.687 s, with 1024 product groups and 2048 full-plane direct writes.
- Grouped whole-plane had product write around 92.6 s, but total wall worsened to 679.5 s and product/result consume was 115.5 s. One-plane whole-plane removes the merge-batch memory pressure but exposes the same backend lower bound.

Decision: do not abandon whole-plane product tiles. The failed strategy is multi-plane product-result grouping in the current executor, not the whole-plane tile shape. The next product-write attempt should preserve whole-plane tiles and remove the serial dataflow limits, for example by writing whole-plane products directly from plane workers to deterministic non-overlapping plane regions or by adding a real bulk tiled cube write API that accepts a plane slab without constructing merged intermediate arrays.

### Whole-plane Fortran zero-copy direct writes

Status: keep for the product writer; total runtime is backend-regressed

Hypothesis: with whole-plane product tiles, the Fortran-order plane array already has the same element order as one physical tile. The writer was still copying the whole plane into `direct_write_buffer` before writing it. Removing that copy should preserve the whole-plane tile strategy while proving whether the remaining product bucket is physical write bandwidth or dataflow overhead.

Implementation tested:

- `TiledFileIO::put_aligned_fortran_order_tiles` now writes the caller's Fortran-order slice directly when the spatial tile is the whole plane and no byte swap is required.
- Partial/spatial-tiled writes and byte-swapped writes keep the existing pack-buffer path.
- `PagedImage::put_slice_view` still routes Fortran-contiguous product arrays through the direct tiled path before the older fallbacks.
- Added `put_slice_view_direct_writes_fortran_whole_plane_without_pack_copy` to assert that the whole-plane direct path writes one physical tile with zero pack time and round-trips the pixels.

Verification:

- `cargo fmt --check`
- `cargo test -p casa-images put_slice_view_direct_writes_fortran_whole_plane_without_pack_copy --lib -- --nocapture`
- `cargo test -p casa-images put_slice_view_direct_writes_fortran_aligned_tiled_plane --lib`
- `cargo test -p casars-imager cube_dirty_multi_channel_slab_runner_writes_casa_products --lib`
- `cargo build --release -p casars-imager`

Medium evidence:

- Medium run: `20260615T030452Z-wave1-vla-single-medium-standard-cube-line-62d23440`
- Product write: 7.279 s for 17.180 GB, with 1024 full-plane direct writes across the two dirty image-like products.
- Direct write split: 0.000 s pack, 0.000 s swap, about 6.999 s physical write.
- Product writer path: `tiled_fortran_calls=512`, `tiled_c_order_calls=0`, `tiled_direct_write_tiles=1024`.
- Previous comparable one-plane whole-plane medium reference: 8.596 s product write for 17.180 GB in `20260615T021214Z-wave1-vla-single-medium-standard-cube-line-e00aaf6d`.
- Total runtime was 196.661 s versus 165.103 s in the previous reference. Do not treat this as a total-wall win; slab 1 compute/source-read buckets were materially slower in this sample.

Large evidence:

- Large run: `20260615T031133Z-wave1-alma-mosaic-large-standard-cube-line-ed98d543`
- Runtime: 747.728 s versus 636.612 s in the previous one-plane whole-plane reference and 572.315 s in the earlier best large dirty baseline.
- Source read: 33.013 s total across both slabs, consistent with the previous large shape not being source-read dominated.
- Backend execution: 709.358 s total across both slabs, versus 533.973 s in the earlier best large dirty baseline.
- Product write: 101.156 s for 137.439 GB, versus 158.687 s in the previous one-plane whole-plane reference and 172.592 s in the earlier best baseline.
- Product write split: 0.000 s pack, 0.000 s swap, 100.508 s direct physical write.
- Direct write coverage: 137.439 GB through 2048 full-plane direct write calls and 2048 full-plane direct tiles.
- Product writer path: `tiled_fortran_calls=1024`, `tiled_c_order_calls=0`.
- Peak RSS: 18.831 GB.

Decision: keep the zero-copy whole-plane direct write path. It removes real redundant memory traffic from the storage boundary and materially improves the large product-write bucket without changing the whole-plane tile layout. Do not claim a total-wall win: the large run regressed because per-plane backend execution ballooned. Product writing is now mostly physical write bandwidth; the next optimization should focus on the plane backend and the dataflow feeding FFT/grid work, not abandoning whole-plane product tiles.

### Blocked-transpose inverse FFT

Status: rejected before large run

Hypothesis: the large dirty median plane spends about 3.36 s in the two inverse FFTs. Replacing the strided column FFT pass with blocked transposes plus contiguous row FFTs might cut the FFT bucket enough to produce a material large-wall win.

Implementation tested:

- For large contiguous even-centered inverse FFTs, row FFTs were followed by a blocked transpose, row FFTs on the transposed grid, and a blocked transpose back with the checkerboard scale.
- The memory planner was temporarily updated to charge one extra complex grid of worker scratch for the transpose buffer.

Verification:

- `cargo fmt --check`
- `cargo test -p casa-imaging fft --lib`
- `cargo test -p casars-imager cube_slab_executor_scratch_includes_standard_mfs_workspace --lib`
- `cargo build --release -p casars-imager`

Evidence:

- Medium run: `20260615T023252Z-wave1-vla-single-medium-standard-cube-line-ca46cc13`
- Runtime regressed to 204.415 s versus the current medium reference of 165.103 s.
- Slab backend elapsed regressed to 140.730 s total.
- Product write stayed small at 10.252 s for 17.180 GB, so the regression was backend-side.
- Median/p95 plane time regressed to 2.298 s / 5.231 s.
- Median per-plane core split: total 2.179 s, FFT 0.537 s, grid 1.477 s.
- Peak RSS rose to 18.976 GB.

Decision: reverted. The added transpose buffer and data movement worsened medium, violating the preserve-medium rule. No large run was justified. Future FFT work should use a real batched/vectorized/Metal FFT backend or a layout that avoids extra full-grid copies, not blocked transpose around the existing scalar `rustfft` path.

### Unmerged whole-plane product groups

Status: rejected after medium run

Hypothesis: the previous grouped whole-plane writer regressed because it merged individual plane results into new grouped `Array4` products before writing. A no-merge writer could keep the full-plane tile shape and write a product group from separate per-plane Fortran-order views directly into the tiled writer's direct buffer, reducing physical write calls without the merged intermediate arrays.

Implementation tested:

- Added a temporary `PagedImage`/`TiledFileIO` path that packs separate Fortran-order plane slices into one direct full-plane group write.
- Re-enabled planner-selected product batches from modeled product batch and pending-result bytes.
- Dirty cube product publication used the unmerged path for one-plane dirty results.

Verification:

- `cargo fmt --check`
- `cargo test -p casa-images put_slice_view_direct_writes_fortran_aligned_tiled_plane_group --lib`
- `cargo test -p casa-images put_plane_group_views_direct_writes_fortran_aligned_tiled_planes --lib`
- `cargo test -p casars-imager plane_group_publisher_is_result_type_generic --lib`
- `cargo test -p casars-imager independent_plane_streaming_consumer_receives_completion_order --lib`
- `cargo test -p casars-imager memory_planner_batches_large_dirty_cube_product_writes_within_budget --lib`
- `cargo test -p casars-imager cube_dirty_multi_channel_slab_runner_writes_casa_products --lib`

Evidence:

- Medium run: `20260615T024746Z-wave1-vla-single-medium-standard-cube-line-29445f3b`
- Runtime: 164.624 s versus 165.103 s for the current one-plane whole-plane reference.
- Planner selected `product_batch_planes=64`.
- Product write regressed to 15.406 s for 17.180 GB, versus 8.596 s in the current one-plane whole-plane reference.
- Direct image writes dropped from 1024 to 20, but direct pack/write time still totaled about 7.7 s and product publication/consume remained about 15.6 s.
- First slab product summary: 8.215 s product write, 10 direct image writes, 8.590 GB.
- Second slab product summary: 7.079 s product write, 10 direct image writes, 8.590 GB.
- Peak RSS: 18.465 GB.

Decision: revert. The no-merge path reduced direct write call count but did not improve total wall time and made product write worse on the medium evidence row. Whole-plane tile shape remains the current direction; planner-selected product grouping is not keepable until the writer can reduce actual physical write time or avoid blocking plane consumption.

### F32 routed dirty grids

Status: rejected after medium run

Hypothesis: the per-plane backend bucket was dominated by grid and FFT memory bandwidth. Holding the routed dirty PSF and residual grids as `Complex32` instead of `Complex64` would halve grid memory traffic and reduce inverse FFT cost, while final image products remain `f32`.

Implementation tested:

- Added a temporary f32 routed-dirty tile store and f32 gridding helpers for the initial dirty cube path.
- Added a temporary f32 unshifted centered inverse FFT finalizer.
- Left the whole-plane direct product writer in place, so the test isolated the dirty-grid precision/backend route rather than product tiling.

Verification:

- `cargo test -p casa-imaging standard_mfs_dirty --lib -- --nocapture`
- `cargo test -p casars-imager cube_dirty_multi_channel_slab_runner_writes_casa_products --lib -- --nocapture`
- `cargo build --release -p casars-imager`

Evidence:

- Medium run: `20260615T033935Z-wave1-vla-single-medium-standard-cube-line-ddd53e0b`
- Runtime: 206.913 s, worse than the latest accepted whole-plane zero-copy medium evidence at 196.661 s and the earlier one-plane whole-plane reference at 165.103 s.
- Source read: 49.134 s total across two slabs.
- Backend execution: 145.716 s total across two slabs.
- Product write: 7.625 s for 17.180 GB, with whole-plane direct writes still active and zero pack time.
- Slab product write remained healthy: 3.460 s and 3.921 s.
- Slab backend buckets regressed to 60.473 s and 85.243 s.

Decision: revert. The f32 route did make the FFT component cheaper, but total runtime regressed because the plane feed/replay/grid-update path became the effective bottleneck. Do not revisit this version. The useful learning is that whole-plane product storage is not the problem exposed by this attempt; the planes are being fed and manipulated inefficiently before product write.

### Direct dirty replay split instrumentation

Status: keep

Hypothesis: the remaining medium/large dirty runtime is dominated by how each plane is fed from the shared slab source, not by whole-plane product writes. Add low-overhead counters around the direct dirty replay path so each plane reports row scan, planned-sample construction, and grid-consume time separately.

Implementation kept:

- `DirtyCubeImagingResult` carries `DirectDirtyCubePlaneReplayTimings`.
- The direct dirty slab worker logs `cube_shared_direct_plane_replay` with blocks, rows seen, rows flagged, missing/empty assignments, rejected samples, planned samples, `build_planned_ms`, and `consume_ms`.
- The counter is per block/plane, not per sample, so it is acceptable for medium/large diagnostics.

Evidence:

- Medium diagnostic run: `20260615T035513Z-wave1-vla-single-medium-standard-cube-line-c3f26bc5`
- Runtime: 241.139 s with CASA skipped; use this as diagnostic attribution, not acceptance evidence.
- Slab 0 product write: 3.524 s for 8.590 GB.
- Slab 1 product write: 4.701 s for 8.590 GB.
- Slab 0 backend wall: 63.404 s; slab 1 backend wall: 115.619 s.
- Each plane scanned roughly 3,086,235 rows and planned roughly 3.07-3.09 million samples.
- Early slab-0 planes showed `build_planned_ms` around 0.67-0.80 s and `consume_ms` around 0.39-0.47 s.
- Later planes and slab 1 rose substantially; many slab-1 planes showed `build_planned_ms` around 1.6-2.6 s and `consume_ms` around 1.2-2.0 s.

Decision: keep the instrumentation. It confirms that whole-plane product writes are no longer the primary medium bottleneck: product write is only 8.494 s total for 17.180 GB in this diagnostic, while backend/replay dominates. The next real optimization must change the source-to-plane feed or gridding dataflow rather than the product tile shape.

### Indexed spectral assignment lookup

Status: rejected before completing the medium run

Hypothesis: the direct dirty replay path was wasting time by linearly searching `grid_channel_contributions` for every row and every plane. Building a reusable output-channel-to-grid-assignment index once per spectral-plan entry should reduce `build_planned_ms`.

Implementation tested and reverted:

- Added a `grid_assignment_indexes` map to `CubeRowSpectralReusablePlan`.
- Built a `Vec<u32>` output-channel index for each row spectral contribution entry.
- Routed direct dirty replay through the keyed index instead of `assignment_for_output` linear search.

Evidence:

- Interrupted medium diagnostic: `20260615T040533Z-wave1-vla-single-medium-standard-cube-line-d43ae43d`
- The run was stopped after the first ~100 slab-0 planes because it was already failing the target metric.
- Reusable plan build cost increased to 10.316 s for slab 0.
- Slab 0 source read was also noisy at 45.395 s, so total wall time is not usable.
- The targeted replay metric did not improve: early planes reported `build_planned_ms` around 1.7-1.9 s, and planes 80-99 were still roughly 1.18-1.64 s.
- By contrast, the previous direct replay diagnostic had many early slab-0 planes around 0.67-1.1 s.

Decision: revert. The index increased resident plan work and did not improve the measured planned-sample build bucket. The assignment lookup is not the dominant replay cost in this path; the next attempt should target repeated per-plane row scanning, sample construction, tap planning, or a source-major/slab-major fanout that avoids rebuilding nearly identical per-row geometry for every plane.

### Fast single-contribution direct dirty replay

Status: keep as a narrow feed-path improvement; rejected as the large dirty win

Hypothesis: the direct dirty replay path was still paying generic extraction overhead for the common cube case where each output plane receives one source-channel contribution from `Complex32` DATA, `Float32` WEIGHT, and no WEIGHT_SPECTRUM. Whole-plane product writes are now efficient enough that removing per-sample enum matching, contribution-loop overhead, and paired-correlation helper calls should reduce the source-to-plane feed cost without changing the product layout.

Implementation kept at the time, then removed by the later shared-source simplification:

- Added a typed read-only fast block accessor over `VisibilityBuffer` DATA/FLAG/WEIGHT.
- Routed single-contribution explicit and collapsed-pair dirty samples through direct `Complex32`/`Float32` slice access.
- Preserved the generic helper path for multiple contributions, Complex64, Float64, WEIGHT_SPECTRUM, and other uncommon cases.
- Added fast/fallback sample counters to `cube_shared_direct_plane_replay` so medium/large runs showed whether the hot path was active.

Verification:

- `cargo fmt --check`
- `cargo check -p casars-imager`
- `cargo test -p casars-imager cube_dirty_multi_channel_slab_runner_writes_casa_products --lib -- --nocapture`
- `cargo build --release -p casars-imager`

Evidence:

- Medium diagnostic run: `20260615T041840Z-wave1-vla-single-medium-standard-cube-line-e89c1271`
- Runtime: 207.791 s with CASA skipped; diagnostic attribution only, not acceptance evidence.
- Previous comparable direct replay diagnostic: 241.139 s in `20260615T035513Z-wave1-vla-single-medium-standard-cube-line-c3f26bc5`.
- Product write stayed small: 8.069 s total for 17.180 GB, with slab product writes of 3.591 s and 4.236 s.
- Source read was 20.981 s for slab 0 and 29.303 s for slab 1.
- Slab backend wall was 52.601 s for slab 0 and 92.380 s for slab 1.
- Peak RSS: 18.749 GB.
- The fast path covered the hot case: sampled planes reported `fallback_samples=0` and `fast_samples` equal to accepted planned samples.
- Slab 0 first-batch planes reported `build_planned_ms` around 0.70-0.80 s, matching or modestly improving the best previous early-slab replay timings. Later slab-1 planes still rose to about 1.1-1.7 s `build_planned_ms`, and grid consume/FFT/correction were comparable or larger.
- Large run: `20260615T042423Z-wave1-alma-mosaic-large-standard-cube-line-1727f925`
- Runtime regressed to 923.208 s versus the 572.315 s best large dirty baseline.
- Source read remained small after de-duplicating repeated log records: about 34.2 s for 31.963 GB across the two slab reads.
- Backend execution regressed to 883.179 s across the two slabs, versus 533.973 s in the best large dirty baseline.
- Product write improved versus the best baseline but not enough: 118.526 s for 137.439 GB, versus 172.592 s in the best baseline.
- Whole-plane direct writes were active: 137.439 GB through 2048 direct write calls and 2048 full-plane direct tiles.
- Product writer counters showed no tile readback or zero-fill, `tiled_c_order_calls=0`, and `tiled_fortran_calls=1024`.
- Slab product write was uneven: slab 0 wrote 68.988 GB in 36.356 s, while slab 1 wrote 68.451 GB in 82.171 s.
- The fast path covered the large hot case too: streamed plane logs reported `fallback_samples=0` and `fast_samples=866313` for the representative full-sample planes.
- Peak RSS: 18.498 GB.

Decision: keep the fast single-contribution path and its counters because it removes real generic feed-path overhead and improves the current medium diagnostic relative to the previous replay-instrumented run. Reject it as the large dirty performance solution: large total wall time regressed badly because per-plane backend execution ballooned, even though product writes used whole-plane direct tiles with no readback, zero-fill, or C-order conversion. Whole-plane product tiles remain the right product layout; this evidence points to changing the plane feed/grid/FFT execution model rather than backing away from whole-plane writes.

### 24 GB decimal memory cap retest

Status: keep as current large dirty evidence

Hypothesis: the 923.208 s large regression may have come from planning against a
30.065 GB target on a 32 GiB laptop. A stricter 24 GB decimal cap should reduce
resident pressure and avoid the high-active-plane plan without losing product
write efficiency.

Implementation tested:

- `imaging_memory_target_mb=22888`, which is 23,999,807,488 bytes and therefore
  just under 24 GB decimal.
- Same large standard cube dirty workload, CASA skipped, profile skipped.
- Same whole-plane direct product writer and shared read-only slab source path.

Evidence:

- Run: `20260616T150436Z-wave1-alma-mosaic-large-standard-cube-line-0531c480`
- Runtime improved to 524.143 s, versus 923.208 s for the regressed 30 GB plan
  and 572.315 s for the previous best large baseline.
- Plan changed to 120 active planes, 9 slabs, 308525 row block rows, 10 workers.
- Planned active memory was 24.000 GB, with 7.858 GB visibility/source-side
  buffers and 14.205 GB slab/product-side scratch.
- Peak RSS was 14.170 GB, below the 24 GB decimal target and below both previous
  large rows.
- Source read was 37.337 s for 16.030 GB measured source bytes.
- Backend execution was 481.638 s, still the dominant bucket.
- Product write was 58.435 s for 137.439 GB, with 2048 full-plane direct writes,
  1024 Fortran-order full-plane calls, no C-order calls, no LRU reads, and no
  zero-fill.
- Plane timing improved to 4.782 s median and 6.115 s p95.
- Median per-plane core split: 4.172 s core total, 2.977 s FFT pair, 0.160 s
  sample replay, 0.396 s grid update, 0.280 s PSF grid, 0.280 s residual grid,
  and 0.233 s combined correction/normalization.

Decision: keep the 24 GB decimal cap as the current laptop-safe large dirty
target. This alone fixes the large regression and improves over the previous
best large row. It does not remove the remaining backend lower bound: FFTs and
per-plane backend work are now the next optimization target.

## Current Direction

Do not spend more time on slab-first/source-first while source reads remain below 15% of total wall time. The credible levers are:

- Memory target: use 24 GB decimal for laptop large-dirty evidence. The 30.065 GB plan is preserved only as a rejected/regressed row.
- Product write: keep whole-plane tiles and the zero-copy direct writer. Do not abandon whole-plane layout; current product write is 58.435 s for 137.439 GB with no readback or zero-fill.
- Per-plane backend: target the feed/replay/grid-update/FFT path before more product-layout changes. The failed f32 and indexed-assignment runs showed that cheaper FFTs or faster assignment lookup alone do not help if each plane still scans the same rows and rebuilds scalar planned samples independently. The 24 GB run shows the FFT pair is now the largest median core bucket. The next material step likely needs source-major fanout, batched plane kernels, or a batched FFT/backend so one pass over the shared source feeds multiple plane grids without treating each plane as an isolated scalar unit.
- Batched execution: group planes only where it reduces source replay, grid layout/allocation cost, FFT setup, or enables true bulk tiled writes. Plane grouping that merely holds many complete plane results is rejected.
