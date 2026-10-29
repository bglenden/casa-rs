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
mod ephemeris;
mod metadata;
mod pointing;
mod read;
mod sources;
pub(super) mod store;
pub(super) mod update;
mod window;
mod write;

pub use cursor::Msv4Cursor;
pub(crate) use pointing::POINTING_FRAMES;
pub use read::Msv4Observation;
pub(in crate::observation_io) use write::{check_fields, kept_samples, unix_times};

/// XRADIO's name for an absent source, which the writer writes for a field
/// without one and the reader reads as none.
pub(super) const UNKNOWN_SOURCE: &str = "Unknown";

/// Telescopes whose antennas XRADIO's converter names `NAME_STATION` and
/// marks `relocatable_antennas` (`create_antenna_xds.py`); others keep
/// their names.
pub(in crate::observation_io) const STATION_NAMED: [&str; 4] = ["ALMA", "VLA", "NOEMA", "EVLA"];

/// Whether `name` is XRADIO's placeholder for an absent source:
/// [`UNKNOWN_SOURCE`], or `Unknown_<SOURCE_ID>` where its converter pads a
/// field's missing `SOURCE` row in a partition of several fields.
pub(super) fn is_unknown_source(name: &str) -> bool {
    name.strip_prefix(UNKNOWN_SOURCE).is_some_and(|rest| {
        rest.is_empty()
            || rest
                .strip_prefix('_')
                .is_some_and(|id| id.parse::<i64>().is_ok())
    })
}

/// The codec options of every MSv4 payload read, write and update: one chunk at a
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

#[cfg(test)]
mod tests {
    use super::is_unknown_source;

    #[test]
    fn xradios_placeholders_name_no_source() {
        for name in ["Unknown", "Unknown_5", "Unknown_-1"] {
            assert!(is_unknown_source(name), "{name}");
        }
        for name in ["Unknown_3C286", "Unknownish", "3C286_0", ""] {
            assert!(!is_unknown_source(name), "{name}");
        }
    }
}
