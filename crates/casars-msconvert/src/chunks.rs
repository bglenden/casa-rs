// SPDX-License-Identifier: LGPL-3.0-or-later
//! The default chunk shape of an MSv4 destination: XRADIO's.
//!
//! XRADIO's converter (`default_main_chunksize`, XRADIO 1.3) chunks a main
//! dataset along time only, every other axis whole, with as many times as
//! make the chunk of its largest data variable about
//! [`DEFAULT_CHUNK_BYTES`] uncompressed, and at least one. Only where one
//! time of that variable is more than Blosc encodes in one buffer
//! ([`BLOSC_MAX_BUFFER_BYTES`]) does it split frequency, and then
//! baselines, to chunks of about [`DEFAULT_CHUNK_BYTES`].
//!
//! The converter takes one shape for every dataset and clips each axis to
//! the dataset's extent, where XRADIO sizes each dataset by itself. The
//! shape here is XRADIO's for the dataset whose time is widest: the same
//! as XRADIO's for every dataset when they share a shape, and for the
//! others a chunk of no more times (and so no more bytes) than XRADIO's.

/// XRADIO's `DEFAULT_MAIN_CHUNK_BYTES`: the uncompressed bytes of the
/// largest variable's chunk the default aims at.
pub const DEFAULT_CHUNK_BYTES: u64 = 128 << 20;

/// XRADIO's `BLOSC_MAX_BUFFER_BYTES`: the largest buffer Blosc encodes.
pub const BLOSC_MAX_BUFFER_BYTES: u64 = (1 << 31) - 1;

/// Bytes of one `VISIBILITY` element (`complex64`), the widest per channel.
const VISIBILITY_BYTES: u64 = 8;
/// Bytes of one baseline's `UVW` (three `float64`), the widest variable
/// without a frequency axis.
const UVW_BYTES: u64 = 24;

/// One dataset's extents besides time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadShape {
    /// Baselines.
    pub baselines: u64,
    /// Channels.
    pub channels: u64,
    /// Correlation products.
    pub correlations: u64,
}

impl PayloadShape {
    /// Bytes of the largest variable's chunk of one time, `channels`
    /// channels and `baselines` baselines.
    fn chunk_bytes(self, baselines: u64, channels: u64) -> u64 {
        (VISIBILITY_BYTES * baselines * channels * self.correlations).max(UVW_BYTES * baselines)
    }
}

/// The chunk shape `[time, baseline, frequency, polarization]` XRADIO's
/// converter gives the widest of `shapes`, 0 for a whole axis.
#[must_use]
pub fn default_chunks(shapes: &[PayloadShape]) -> [u64; 4] {
    let Some(widest) = shapes
        .iter()
        .copied()
        .max_by_key(|shape| shape.chunk_bytes(shape.baselines, shape.channels))
    else {
        return [0; 4];
    };
    let per_time = widest.chunk_bytes(widest.baselines, widest.channels).max(1);
    let mut chunks = [(DEFAULT_CHUNK_BYTES / per_time).max(1), 0, 0, 0];
    if per_time <= BLOSC_MAX_BUFFER_BYTES {
        return chunks;
    }
    // One time is too wide for Blosc: chunks of one time, then frequency
    // split, then baselines, until the chunk fits.
    let per_channel = (VISIBILITY_BYTES * widest.baselines * widest.correlations).max(1);
    let channels = (DEFAULT_CHUNK_BYTES / per_channel).clamp(1, widest.channels.max(1));
    chunks[2] = channels;
    if widest.chunk_bytes(widest.baselines, channels) > BLOSC_MAX_BUFFER_BYTES {
        let per_baseline = widest.chunk_bytes(1, channels).max(1);
        chunks[1] = (DEFAULT_CHUNK_BYTES / per_baseline).clamp(1, widest.baselines.max(1));
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(baselines: u64, channels: u64, correlations: u64) -> PayloadShape {
        PayloadShape {
            baselines,
            channels,
            correlations,
        }
    }

    #[test]
    fn a_dataset_is_chunked_along_time_to_about_128_mib() {
        // pilot.ms: 351 baselines, 64 channels, 4 correlations; one time of
        // VISIBILITY is 718,848 bytes, so 186 times make 128 MiB.
        assert_eq!(default_chunks(&[shape(351, 64, 4)]), [186, 0, 0, 0]);
        // A time wider than 128 MiB still makes a chunk of one time.
        assert_eq!(default_chunks(&[shape(1225, 4096, 4)]), [1, 0, 0, 0]);
        // UVW is the widest variable of a single channel and correlation.
        assert_eq!(
            default_chunks(&[shape(1000, 1, 1)]),
            [DEFAULT_CHUNK_BYTES / 24_000, 0, 0, 0]
        );
    }

    #[test]
    fn the_widest_dataset_sets_the_times() {
        let narrow = shape(351, 64, 4);
        let wide = shape(351, 1024, 4);
        assert_eq!(
            default_chunks(&[narrow, wide, narrow]),
            default_chunks(&[wide])
        );
        assert_eq!(default_chunks(&[wide]), [11, 0, 0, 0]);
    }

    #[test]
    fn frequency_and_then_baselines_split_only_past_blosc_s_limit() {
        // 2.4 GiB a time: one time, 3,423 channels a chunk.
        assert_eq!(default_chunks(&[shape(1225, 65_536, 4)]), [1, 0, 3423, 0]);
        // A time of one channel still past the limit splits baselines too.
        let huge = shape(100_000_000, 2, 4);
        assert_eq!(default_chunks(&[huge]), [1, 4_194_304, 1, 0]);
    }

    #[test]
    fn no_dataset_is_whole_axes() {
        assert_eq!(default_chunks(&[]), [0; 4]);
    }
}
