// SPDX-License-Identifier: LGPL-3.0-or-later
//! Original T/Tf noise-scale solutions, shared by output and later observations.
use casa_calibration::{SimulatedJones, SimulatedSolution, SolutionValues, simulated_table_bytes};
use casa_imaging_runtime::Reservation;
use casa_types::Complex32;

#[derive(Debug)]
pub(crate) struct Chunk {
    pub(crate) time_s: f64,
    pub(crate) field: i32,
    pub(crate) window: i32,
    pub(crate) channels: usize,
    pub(crate) offset: usize,
}

#[derive(Debug)]
pub(crate) struct TsysSnapshot {
    pub(crate) metadata: casa_calibration::SimulatedTableMetadata,
    pub(crate) kind: SimulatedJones,
    pub(crate) antennas: usize,
    pub(crate) chunks: Vec<Chunk>,
    pub(crate) values: Vec<f32>,
    pub(crate) order: Vec<usize>,
    pub(crate) reservation: Reservation,
}

impl TsysSnapshot {
    pub(crate) fn finish_index(&mut self) {
        self.order.extend(0..self.chunks.len());
        self.order.sort_unstable_by(|&a, &b| {
            let (a, b) = (&self.chunks[a], &self.chunks[b]);
            a.window
                .cmp(&b.window)
                .then(a.time_s.total_cmp(&b.time_s))
                .then(a.offset.cmp(&b.offset))
        });
    }

    pub(crate) fn covers(&self, window: usize) -> bool {
        let at = self
            .order
            .partition_point(|&i| self.chunks[i].window < window as i32);
        self.order
            .get(at)
            .is_some_and(|&i| self.chunks[i].window == window as i32)
    }

    pub(crate) fn gains(&self, window: usize, time: f64, antennas: [usize; 2]) -> (&[f32], &[f32]) {
        let start = self
            .order
            .partition_point(|&i| self.chunks[i].window < window as i32);
        let end = self
            .order
            .partition_point(|&i| self.chunks[i].window <= window as i32);
        let order = &self.order[start..end];
        let after = order.partition_point(|&i| self.chunks[i].time_s < time);
        let nearest = match (after.checked_sub(1), order.get(after)) {
            (Some(before), Some(&later))
                if self.chunks[later].time_s - time < time - self.chunks[order[before]].time_s =>
            {
                after
            }
            (Some(before), _) => before,
            _ => 0,
        };
        let chunk = &self.chunks[order[nearest]];
        let gain = |antenna| {
            let begin = chunk.offset + antenna * chunk.channels;
            &self.values[begin..begin + chunk.channels]
        };
        (gain(antennas[0]), gain(antennas[1]))
    }

    pub(crate) fn solutions(&self) -> impl Iterator<Item = SimulatedSolution> + '_ {
        self.chunks.iter().flat_map(move |chunk| {
            (0..self.antennas).map(move |antenna| {
                let offset = chunk.offset + antenna * chunk.channels;
                SimulatedSolution {
                    time_s: chunk.time_s,
                    field_id: chunk.field,
                    spectral_window_id: chunk.window,
                    antenna: antenna as i32,
                    values: SolutionValues::Channels(
                        self.values[offset..offset + chunk.channels]
                            .iter()
                            .map(|&gain| Complex32::new(gain, 0.))
                            .collect(),
                    ),
                }
            })
        })
    }

    pub(crate) fn write_bytes(&self) -> u64 {
        simulated_table_bytes(
            (self.chunks.len() * self.antennas) as u64,
            self.values.len() as u64,
        )
    }
}
