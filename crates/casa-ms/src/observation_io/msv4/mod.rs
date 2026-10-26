// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MSv4 adapter: XRADIO `VisibilityXds` schema 4.0.1 on local Zarr v3.
//!
//! The trial profile reads and writes what the trial fixtures hold:
//! `complex64` visibilities, boolean flags, `float32` weights and `float64`
//! UVW, each `[time, baseline_id, frequency, polarization]` (UVW
//! `[time, baseline_id, uvw_label]`), regular chunk grids, uncompressed,
//! Blosc or Zstandard bytes codecs, `unix`-format times and the units the
//! schema requires. Anything else is refused with a typed error.

mod cursor;
mod metadata;
mod read;
pub(super) mod store;
pub(super) mod update;
mod window;
mod write;

pub use cursor::Msv4Cursor;
pub use read::Msv4Observation;

/// The codec options of every MSv4 read, write and update: one chunk at a
/// time, on the calling thread, so that parallelism is the caller's (its
/// workers) and no work falls through to a pool the process did not admit.
///
/// The concurrency target alone does not do this: zarrs 0.23.14 encodes and
/// decodes `min(chunk_concurrent_minimum, chunks)` chunks of one access at
/// once on rayon's global pool whatever the target, and the minimum
/// defaults to 4 (trial README, "Converter"). Building the options still
/// starts rayon's global pool, which then stays idle.
pub(super) fn codec_options() -> zarrs::array::CodecOptions {
    zarrs::array::CodecOptions::default()
        .with_concurrent_target(1)
        .with_chunk_concurrent_minimum(1)
}
pub use write::{
    Msv4Codec, Msv4ProcessingSetWriter, Msv4StorageOptions, Msv4WriteCounters, Msv4Writer,
};
