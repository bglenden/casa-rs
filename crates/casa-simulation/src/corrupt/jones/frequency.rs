// SPDX-License-Identifier: LGPL-3.0-or-later
//! A frequency-dependent table uses the same receptor equation per channel.
use super::*;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct FrequencyValues {
    /// Antenna-major channel values within each solution block.
    pub(super) values: Vec<[Complex32; 2]>,
    /// Block boundaries in values, including the final end boundary.
    offsets: Vec<usize>,
}
impl FrequencyValues {
    pub(super) fn bytes(&self) -> u64 {
        self.values.capacity() as u64 * VALUE_BYTES
            + (self.offsets.capacity() * size_of::<usize>()) as u64
    }
    pub(super) fn apply(
        &self,
        kind: JonesKind,
        antennas: usize,
        block: usize,
        (p, q): (usize, usize),
        receptors: &[[usize; 2]],
        cell: &mut [Complex32],
    ) {
        let start = self.offsets[block];
        let channels = (self.offsets[block + 1] - start) / antennas;
        for (channel, sample) in cell.chunks_exact_mut(receptors.len()).enumerate() {
            debug_assert!(channel < channels);
            apply_receptors(
                kind,
                self.values[start + p * channels + channel],
                self.values[start + q * channels + channel],
                receptors,
                sample,
            );
        }
    }
}

pub(crate) fn applied_frequency(
    kind: JonesKind,
    antennas: usize,
    chunks: Chunks,
    values: Vec<[Complex32; 2]>,
    offsets: Vec<usize>,
) -> JonesTerm {
    debug_assert_eq!(offsets.len(), chunks.count as usize + 1);
    debug_assert_eq!(offsets.last().copied(), Some(values.len()));
    JonesTerm {
        kind,
        antennas,
        solutions: chunks.by_window,
        values: JonesValues::Frequency(FrequencyValues { values, offsets }),
    }
}

impl TermBytes {
    /// Frequency-dependent receptor grids plus bounded resampling scratch.
    pub(crate) fn frequency(
        per_window: &[usize],
        channels: &[usize],
        antennas: usize,
        max_cell_values: usize,
    ) -> Self {
        let chunks = per_window.iter().sum::<usize>();
        let values = per_window
            .iter()
            .zip(channels)
            .map(|(count, channels)| count * channels * antennas)
            .sum::<usize>();
        Self {
            kept: solutions_bytes(per_window)
                + values as u64 * VALUE_BYTES
                + ((chunks + 1) * size_of::<usize>()) as u64,
            forming: (max_cell_values as u64) * 64
                + channels.iter().copied().max().unwrap_or(0) as u64 * 32,
        }
    }
}
