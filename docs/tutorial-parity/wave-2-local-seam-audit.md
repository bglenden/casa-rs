<!-- SPDX-License-Identifier: LGPL-3.0-or-later -->

# Wave 2 Local Seam Audit

Truth class: current descriptive
Last reality check: 2026-04-27
Verification: `cargo test -p casa-images --lib image_view`, `cargo test -p casa-calibration --lib`

Wave issue: #139
Child issues: #133, #135

## Hotspot Split Decision

Issue #133 allowed at most two extra hotspot extractions beyond the dedicated
plot, listobs, and imaging-orchestration child issues. The first pass audited
the listed non-test modules by size and selected one extra extraction:

- `crates/casa-images/src/image_view.rs`: selected. Plane raster statistics,
  stretch settings, value-grid formatting helpers, and downsampling helpers now
  live in `image_view/plane_render.rs`.
- `crates/casa-ms/src/spectral_selection.rs`: deferred. It is tutorial-critical,
  but its channel-contribution and frame-conversion seams are correctness
  sensitive and are better split when the spectral preparation wave owns those
  semantics.
- `crates/casa-tables/src/storage/tiled_stman.rs` and
  `crates/casa-tables/src/storage/mod.rs`: deferred. These are persistence
  internals where a split could obscure byte-compatibility review unless paired
  with storage-specific interop evidence.
- `crates/casa-images/src/image_expr.rs` and `image.rs`: deferred. The existing
  expression/compiled split gives a safe first boundary; further extraction
  should follow a focused image-expression issue.
- `crates/casa-vla/src/importer.rs`, `crates/casa-calibration/src/cli.rs`,
  `crates/casa-calibration/src/execute.rs`, and
  `crates/casa-tables/src/taql/functions.rs`: deferred except for the local
  clone cleanup below, because the current tutorial work does not need a
  broader module split there.

The selected split is internal-only: public exports, serialized payloads,
persisted bytes, and UI behavior are unchanged.

## Allocation / Copy Cleanup

Issue #135 found one clearly avoidable clone pattern on a tutorial-scale
calibration path:

- `crates/casa-calibration/src/execute.rs` prefetched owned `DATA`, `FLAG`, and
  optional `WEIGHT` array cells, then cloned each selected row's owned value
  when building `PrefetchedExecutionRowInputs`.

The loop now consumes the prefetched vectors with `into_iter()` and moves each
owned `ArrayValue` into the row input. This keeps the same memory-bounded
prefetch shape while avoiding one data-array clone, one flag-array clone, and
when `calwt` is active one weight-array clone per selected row.

This is a local ownership cleanup. It does not change the runtime model,
concurrency, algorithms, public APIs, provider contracts, or persisted formats.
