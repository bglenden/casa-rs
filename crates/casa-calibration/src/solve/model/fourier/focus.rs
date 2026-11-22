// SPDX-License-Identifier: LGPL-3.0-or-later
//! Finite-distance refocusing from the baselines in a visibility buffer.

use std::collections::BTreeMap;

use casa_imaging_model::GridFtProjection;

use super::super::uvw;
use super::{FourierRow, GainSolveError, model_error};

pub(super) struct Refocus {
    distance_m: f64,
    positions: BTreeMap<i32, [f64; 3]>,
    buffer: Option<usize>,
}

impl Refocus {
    pub(super) fn from_distance(distance_m: f64) -> Result<Option<Self>, GainSolveError> {
        if !distance_m.is_finite() {
            return Err(model_error(
                "virtual Fourier grid has an invalid focus distance",
            ));
        }
        Ok((distance_m != 0.0).then(|| Self {
            distance_m,
            positions: BTreeMap::new(),
            buffer: None,
        }))
    }

    pub(super) fn apply(
        &mut self,
        row: &FourierRow<'_>,
        projection: &GridFtProjection,
    ) -> Result<([f64; 3], f64), GainSolveError> {
        let key = row
            .focus_rows
            .first()
            .ok_or_else(|| model_error("virtual Fourier focus has no visibility buffer"))?
            .index;
        if self.buffer != Some(key) {
            self.positions = antenna_positions(row, projection)?;
            self.buffer = Some(key);
        }
        let [Some(first), Some(second)] =
            [row.row.antenna1, row.row.antenna2].map(|antenna| self.positions.get(&antenna))
        else {
            return Err(model_error(
                "virtual Fourier focus has no position for a visibility antenna",
            ));
        };
        let distance = self.distance_m;
        let range = |[u, v, w]: [f64; 3]| (u * u + v * v + (distance - w) * (distance - w)).sqrt();
        let first_range = range(*first);
        let second_range = range(*second);
        let (_, mut phase_shift) = projection.apply(row.uvw);
        for axis in 0..2 {
            phase_shift += (first[axis].powi(2) - second[axis].powi(2)) / (2.0 * distance);
        }
        let mut uvw = std::array::from_fn(|axis| {
            distance * (first[axis] / first_range - second[axis] / second_range)
        });
        // FTM gives the gridder -dphase as the native phase shift.
        uvw[2] -= phase_shift;
        Ok((uvw, phase_shift))
    }
}

fn antenna_positions(
    row: &FourierRow<'_>,
    projection: &GridFtProjection,
) -> Result<BTreeMap<i32, [f64; 3]>, GainSolveError> {
    let reference = row
        .focus_rows
        .iter()
        .map(|sample| sample.antennas[0])
        .min()
        .ok_or_else(|| model_error("virtual Fourier focus has no visibility buffer"))?;
    let mut sums = BTreeMap::<i32, ([f64; 3], usize)>::new();
    for sample in row.focus_rows {
        let (uvw, _) = projection.apply(uvw(row.ms, sample.index, row.uncached_main_reads)?);
        for (antenna, sign) in sample.antennas.into_iter().zip([1.0, -1.0]) {
            let (sum, count) = sums.entry(antenna).or_default();
            for (coordinate, value) in sum.iter_mut().zip(uvw) {
                *coordinate += sign * value;
            }
            *count += 1;
        }
    }
    let (origin, count) = sums[&reference];
    let origin = origin.map(|coordinate| coordinate / count as f64);
    Ok(sums
        .into_iter()
        .map(|(antenna, (sum, count))| {
            (
                antenna,
                std::array::from_fn(|axis| sum[axis] / count as f64 - origin[axis]),
            )
        })
        .collect())
}
