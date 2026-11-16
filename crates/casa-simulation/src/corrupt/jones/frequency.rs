// SPDX-License-Identifier: LGPL-3.0-or-later
//! A frequency-dependent table uses the same receptor equation per channel.
use super::*;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct FrequencyValues<const N: usize = 2> {
    /// Antenna-major channel values within each solution block.
    pub(super) values: Vec<[Complex32; N]>,
    /// Block boundaries in values, including the final end boundary.
    offsets: Vec<usize>,
}
impl<const N: usize> FrequencyValues<N> {
    pub(super) fn bytes(&self) -> u64 {
        (self.values.capacity() * size_of::<[Complex32; N]>()) as u64
            + (self.offsets.capacity() * size_of::<usize>()) as u64
    }
}
impl FrequencyValues {
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

impl FrequencyValues<4> {
    pub(super) fn apply_matrix(
        &self,
        antennas: usize,
        block: usize,
        (p, q): (usize, usize),
        receptors: &[[usize; 2]],
        cell: &mut [Complex32],
    ) {
        let start = self.offsets[block];
        let channels = (self.offsets[block + 1] - start) / antennas;
        // Circular-feed F is diagonal and also applies to one/two products.
        // Full-matrix operators are checked for four products before recording.
        if receptors.len() != 4 {
            for (channel, sample) in cell.chunks_exact_mut(receptors.len()).enumerate() {
                let channel = if channels == 1 { 0 } else { channel };
                let p = self.values[start + p * channels + channel];
                let q = self.values[start + q * channels + channel];
                for (value, &[r1, r2]) in sample.iter_mut().zip(receptors) {
                    *value *= p[3 * r1] * q[3 * r2].conj();
                }
            }
            return;
        }
        let positions: [usize; 4] = std::array::from_fn(|index| {
            receptors
                .iter()
                .position(|&pair| pair == [index / 2, index % 2])
                .expect("full Jones application requires four correlations")
        });
        for (channel, sample) in cell.chunks_exact_mut(receptors.len()).enumerate() {
            let channel = if channels == 1 { 0 } else { channel };
            let p = self.values[start + p * channels + channel];
            let q = self.values[start + q * channels + channel].map(|value| value.conj());
            let v = positions.map(|index| sample[index]);
            let left = [
                p[0] * v[0] + p[1] * v[2],
                p[0] * v[1] + p[1] * v[3],
                p[2] * v[0] + p[3] * v[2],
                p[2] * v[1] + p[3] * v[3],
            ];
            let output = [
                left[0] * q[0] + left[1] * q[1],
                left[0] * q[2] + left[1] * q[3],
                left[2] * q[0] + left[3] * q[1],
                left[2] * q[2] + left[3] * q[3],
            ];
            for (index, value) in positions.into_iter().zip(output) {
                sample[index] = value;
            }
        }
    }
}

pub(crate) fn applied_matrices(
    kind: JonesKind,
    antennas: usize,
    chunks: Chunks,
    values: Vec<[Complex32; 4]>,
    offsets: Vec<usize>,
) -> JonesTerm {
    debug_assert_eq!(offsets.len(), chunks.count as usize + 1);
    debug_assert_eq!(offsets.last().copied(), Some(values.len()));
    JonesTerm {
        kind,
        antennas,
        solutions: chunks.by_window,
        values: JonesValues::Matrices(FrequencyValues { values, offsets }),
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
            forming: if chunks == 0 {
                0
            } else {
                (max_cell_values as u64) * 64
                    + channels.iter().copied().max().unwrap_or(0) as u64 * 32
            },
        }
    }
}
