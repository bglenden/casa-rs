// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-correlation visibility spectra after averaging to each solve interval.

use super::{Complex32, SolveAccumulator, SolveEdgeAccumulation};

/// One time-averaged baseline spectrum before the solver collapses it.
pub(in crate::solve) struct DelaySpectrum<'a> {
    pub(in crate::solve) antennas: (i32, i32),
    pub(in crate::solve) buffer: (i32, i32, u64),
    pub(in crate::solve) correlation: usize,
    pub(in crate::solve) parallel_hand: bool,
    pub(in crate::solve) receptor: usize,
    pub(in crate::solve) time_seconds: f64,
    values: &'a [SolveEdgeAccumulation],
}

impl DelaySpectrum<'_> {
    pub(in crate::solve) fn channels(&self) -> impl Iterator<Item = (Complex32, f32)> + '_ {
        self.values
            .iter()
            .map(|value| (value.weighted_sum, value.weight))
    }
}

impl SolveAccumulator {
    pub(in crate::solve) fn delay_spectra(&self) -> impl Iterator<Item = DelaySpectrum<'_>> {
        self.receptor_accumulations
            .iter()
            .enumerate()
            .flat_map(move |(receptor, baselines)| {
                baselines.iter().map(move |(key, values)| DelaySpectrum {
                    antennas: key.baseline.antennas,
                    buffer: (
                        key.baseline.data_desc_id,
                        key.baseline.field_id,
                        key.baseline.start_bits,
                    ),
                    correlation: key.correlation_index,
                    parallel_hand: key.parallel_hand,
                    receptor,
                    time_seconds: self
                        .baseline_centroids
                        .get(&key.baseline)
                        .or_else(|| {
                            self.baseline_centroids
                                .get(&key.baseline.time_bucket_key(true))
                        })
                        .and_then(|centroid| centroid.mean())
                        .unwrap_or(self.min_time),
                    values,
                })
            })
    }
}
