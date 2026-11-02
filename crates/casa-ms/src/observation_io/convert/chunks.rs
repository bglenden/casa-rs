// SPDX-License-Identifier: LGPL-3.0-or-later
//! XRADIO's default chunk shape of an MSv4 dataset ([`xradio_chunks`]).
//!
//! XRADIO's converter (`default_main_chunksize`, XRADIO 1.3) chunks a main
//! dataset along time only, every other axis whole, with as many times as
//! make the chunk of its largest data variable about
//! [`DEFAULT_CHUNK_BYTES`] uncompressed, and at least one. Only where one
//! time of that variable is more than Blosc encodes in one buffer
//! ([`BLOSC_MAX_BUFFER_BYTES`]) does it split frequency, and then
//! baselines, to chunks of about [`DEFAULT_CHUNK_BYTES`]. Each dataset is
//! sized by itself, so a continuum window of few channels gets as many
//! times a chunk as a line window of many gets few.

/// XRADIO's `DEFAULT_MAIN_CHUNK_BYTES`: the uncompressed bytes of the
/// largest variable's chunk the default aims at.
pub const DEFAULT_CHUNK_BYTES: u64 = 128 << 20;

/// XRADIO's `BLOSC_MAX_BUFFER_BYTES`: the largest buffer Blosc encodes.
pub const BLOSC_MAX_BUFFER_BYTES: u64 = (1 << 31) - 1;

/// Bytes of one `VISIBILITY` element (`complex64`), the widest per channel
/// of what the writer stores.
const VISIBILITY_BYTES: u64 = 8;
/// Bytes of one baseline's `UVW` (three `float64`), the widest variable
/// without a frequency axis.
const UVW_BYTES: u64 = 24;

/// Bytes of the largest variable's chunk of one time, `baselines`
/// baselines, `channels` channels and `correlations` correlations.
fn chunk_bytes(baselines: u64, channels: u64, correlations: u64) -> u64 {
    (VISIBILITY_BYTES * baselines * channels * correlations).max(UVW_BYTES * baselines)
}

/// The chunk shape `[time, baseline, frequency, polarization]` XRADIO's
/// converter gives a dataset of `extents` (in the same order), 0 for a
/// whole axis.
#[must_use]
pub fn xradio_chunks(extents: [usize; 4]) -> [u64; 4] {
    let [times, baselines, channels, correlations] = extents.map(|extent| extent as u64);
    let per_time = chunk_bytes(baselines, channels, correlations).max(1);
    let mut chunks = [
        (DEFAULT_CHUNK_BYTES / per_time).clamp(1, times.max(1)),
        0,
        0,
        0,
    ];
    if per_time <= BLOSC_MAX_BUFFER_BYTES {
        return chunks;
    }
    // One time is too wide for Blosc: chunks of one time, then frequency
    // split, then baselines, until the chunk fits.
    let per_channel = (VISIBILITY_BYTES * baselines * correlations).max(1);
    let channel_chunk = (DEFAULT_CHUNK_BYTES / per_channel).clamp(1, channels.max(1));
    chunks[2] = channel_chunk;
    if chunk_bytes(baselines, channel_chunk, correlations) > BLOSC_MAX_BUFFER_BYTES {
        let per_baseline = chunk_bytes(1, channel_chunk, correlations).max(1);
        chunks[1] = (DEFAULT_CHUNK_BYTES / per_baseline).clamp(1, baselines.max(1));
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dataset_is_chunked_along_time_to_about_128_mib() {
        // pilot.ms: 351 baselines, 64 channels, 4 correlations; one time of
        // VISIBILITY is 718,848 bytes, so 186 times make 128 MiB.
        assert_eq!(xradio_chunks([1000, 351, 64, 4]), [186, 0, 0, 0]);
        // Never more times than the dataset holds.
        assert_eq!(xradio_chunks([12, 351, 64, 4]), [12, 0, 0, 0]);
        // A time wider than 128 MiB still makes a chunk of one time.
        assert_eq!(xradio_chunks([100, 1225, 4096, 4]), [1, 0, 0, 0]);
        // UVW is the widest variable of a single channel and correlation.
        assert_eq!(
            xradio_chunks([100_000, 1000, 1, 1]),
            [DEFAULT_CHUNK_BYTES / 24_000, 0, 0, 0]
        );
    }

    #[test]
    fn each_dataset_is_sized_by_itself() {
        // A one-channel average beside a 3,840-channel window, as raw ALMA
        // data holds them: each chunk is about 128 MiB (the average's of
        // UVW, its widest variable).
        let line = xradio_chunks([10_000, 990, 3840, 2]);
        let average = xradio_chunks([10_000, 990, 1, 2]);
        assert_eq!(line, [2, 0, 0, 0]);
        assert_eq!(average, [5648, 0, 0, 0]);
    }

    #[test]
    fn frequency_and_then_baselines_split_only_past_blosc_s_limit() {
        // 2.4 GiB a time: one time, 3,423 channels a chunk.
        assert_eq!(xradio_chunks([10, 1225, 65_536, 4]), [1, 0, 3423, 0]);
        // A time of one channel still past the limit splits baselines too.
        assert_eq!(xradio_chunks([10, 100_000_000, 2, 4]), [1, 4_194_304, 1, 0]);
    }

    #[test]
    fn an_empty_dataset_is_one_time_of_whole_axes() {
        assert_eq!(xradio_chunks([0, 0, 0, 0]), [1, 0, 0, 0]);
    }
}
