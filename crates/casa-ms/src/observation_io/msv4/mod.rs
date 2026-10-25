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
mod write;

pub use cursor::Msv4Cursor;
pub use read::Msv4Observation;
pub use write::{Msv4Codec, Msv4StorageOptions, Msv4WriteCounters, Msv4Writer};
