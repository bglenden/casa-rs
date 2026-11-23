// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed wrappers for the POINTING subtable.
//!
//! Cf. C++ `MSPointingColumns`.

use std::collections::BTreeMap;

use casa_types::{ArrayValue, measures::direction::MDirection};

use crate::derived::engine::resolve_direction_reference;
use crate::schema::{self, SubtableId};
use crate::{MsError, MsResult};

mod metadata;
mod single_dish;

crate::subtables::define_generic_subtable!(
    read = MsPointing,
    write = MsPointingMut,
    id = SubtableId::Pointing,
    required = schema::pointing::REQUIRED_COLUMNS,
    optional = schema::pointing::OPTIONAL_COLUMNS,
    table_name = "POINTING",
    read_doc = "Read-only typed wrapper for the POINTING subtable.",
    write_doc = "Mutable typed wrapper for the POINTING subtable."
);

impl MsPointing<'_> {
    /// Resolve the recorded direction for each `(MAIN row, antenna, time)`
    /// as `pointingdirection(interpolate=False, initialrow=MAIN row)` does.
    ///
    /// # Errors
    /// A pointing is missing or its direction or time samples are invalid.
    pub fn metadata_directions(&self, requests: &[(usize, i32, f64)]) -> MsResult<Vec<MDirection>> {
        let mut antennas: BTreeMap<_, Vec<_>> = requests
            .iter()
            .map(|&(_, antenna, _)| (antenna, Vec::new()))
            .collect();
        for row in 0..self.row_count() {
            if let Some(samples) = antennas.get_mut(&self.i32(row, "ANTENNA_ID")?) {
                samples.push((row, self.f64(row, "TIME")?, self.f64(row, "INTERVAL")?));
            }
        }
        for samples in antennas.values_mut() {
            infer_buffer_half_intervals(samples);
        }
        let mut directions = BTreeMap::new();
        requests
            .iter()
            .map(|&(guess, antenna, time)| {
                let samples = &antennas[&antenna];
                let start = samples.partition_point(|&(row, _, _)| row < guess);
                let &(row, _, _) = samples[start..]
                    .iter()
                    .chain(&samples[..start])
                    .find(|&&(_, epoch, interval)| {
                        interval <= 0.0 || (epoch - time).abs() <= interval
                    })
                    .ok_or_else(|| {
                        MsError::InvalidInput(format!(
                            "No POINTING direction for antenna {antenna} at time {time}"
                        ))
                    })?;
                if let Some(direction) = directions.get(&row) {
                    return Ok(MDirection::clone(direction));
                }
                let direction = self.read_direction(row, None)?;
                directions.insert(row, direction.clone());
                Ok(direction)
            })
            .collect()
    }

    /// Read the pointing used by CASA's legacy visibility buffer for an
    /// antenna and visibility time. The returned direction is the recorded
    /// direction at the pointing row's epoch.
    ///
    /// # Errors
    /// The POINTING metadata or selected direction cell is invalid.
    pub fn legacy_direction(&self, antenna: i32, time: f64) -> MsResult<Option<MDirection>> {
        let mut rows = Vec::new();
        for row in 0..self.row_count() {
            if self.i32(row, "ANTENNA_ID")? == antenna {
                rows.push((row, self.f64(row, "TIME")?, self.f64(row, "INTERVAL")?));
            }
        }
        let nearest_sample = rows
            .iter()
            .filter(|(_, _, interval)| *interval <= 0.)
            .min_by(|a, b| (a.1 - time).abs().total_cmp(&(b.1 - time).abs()));
        let selected = rows
            .iter()
            .enumerate()
            .find(|&(index, &(_, epoch, interval))| {
                if interval > 0. {
                    return (epoch - time).abs() <= interval / 2.;
                }
                let neighbor = rows[index + 1..]
                    .iter()
                    .chain(rows[..index].iter().rev())
                    .find(|(_, candidate, _)| *candidate != epoch);
                neighbor
                    .is_none_or(|(_, next, _)| (epoch - time).abs() <= (epoch - next).abs() / 2.)
            })
            .map(|(_, sample)| sample);
        let Some(&(row, _, _)) = (match (selected, nearest_sample) {
            (Some(covering), Some(closest))
                if (closest.1 - time).abs() <= (covering.1 - time).abs() =>
            {
                Some(closest)
            }
            (covering, closest) => covering.or(closest),
        }) else {
            return Ok(None);
        };
        self.read_direction(row, None).map(Some)
    }

    /// Resolve the first antenna's directions for successive legacy
    /// visibility buffers. Each request contains the antenna, its epoch in
    /// MeasurementSet seconds and the buffer interval passed by VisBuffer
    /// to `MSPointingColumns::directionMeas` for polynomial directions.
    /// A missing row leaves that buffer's direction to FIELD.
    ///
    /// # Errors
    /// The POINTING metadata or a selected direction cell is invalid.
    pub fn legacy_buffer_directions(
        &self,
        buffers: &[(i32, f64, f64)],
    ) -> MsResult<Vec<Option<MDirection>>> {
        let mut by_antenna: BTreeMap<_, Vec<_>> = buffers
            .iter()
            .map(|&(antenna, _, _)| (antenna, Vec::new()))
            .collect();
        if by_antenna.is_empty() {
            return Ok(Vec::new());
        }
        for row in 0..self.row_count() {
            if let Some(samples) = by_antenna.get_mut(&self.i32(row, "ANTENNA_ID")?) {
                samples.push((row, self.f64(row, "TIME")?, self.f64(row, "INTERVAL")?));
            }
        }
        for samples in by_antenna.values_mut() {
            infer_buffer_half_intervals(samples);
        }
        let mut last = 0;
        let mut directions = BTreeMap::new();
        buffers
            .iter()
            .map(|&(antenna, time, interval)| {
                let row = select_buffer_pointing(&by_antenna[&antenna], last, time);
                last = row.unwrap_or(0);
                let Some(row) = row else {
                    return Ok(None);
                };
                let order = self.i32(row, "NUM_POLY")?;
                let key = (row, if order == 0 { 0 } else { interval.to_bits() });
                if let Some(direction) = directions.get(&key) {
                    return Ok(Some(MDirection::clone(direction)));
                }
                let polynomial = (order != 0).then_some((order, interval));
                let direction = self.read_direction(row, polynomial)?;
                directions.insert(key, direction.clone());
                Ok(Some(direction))
            })
            .collect()
    }

    fn read_direction(&self, row: usize, polynomial: Option<(i32, f64)>) -> MsResult<MDirection> {
        self.read_direction_column(row, "DIRECTION", polynomial)
    }

    fn read_direction_column(
        &self,
        row: usize,
        column: &str,
        polynomial: Option<(i32, f64)>,
    ) -> MsResult<MDirection> {
        let angles = self.direction_angles(self.array(row, column)?, row, column, polynomial)?;
        let reference = resolve_direction_reference(self.table(), "POINTING", column, row)?;
        Ok(MDirection::from_angles(angles[0], angles[1], reference))
    }

    fn direction_angles(
        &self,
        values: &ArrayValue,
        row: usize,
        column: &str,
        polynomial: Option<(i32, f64)>,
    ) -> MsResult<[f64; 2]> {
        let ArrayValue::Float64(values) = values else {
            return Err(MsError::InvalidInput(format!(
                "POINTING {column} must be double"
            )));
        };
        let first = |axis| {
            if values.ndim() == 1 {
                values.get(ndarray::IxDyn(&[axis]))
            } else {
                values.get(ndarray::IxDyn(&[axis, 0]))
            }
        };
        let (Some(&lon), Some(&lat)) = (first(0), first(1)) else {
            return Err(MsError::InvalidInput(format!(
                "invalid POINTING {column} shape"
            )));
        };
        let [lon, lat] = if let Some((order, time)) = polynomial {
            let order = usize::try_from(order)
                .map_err(|_| MsError::InvalidInput("negative POINTING polynomial order".into()))?;
            if values
                .shape()
                .get(1)
                .is_none_or(|&columns| columns <= order)
            {
                return Err(MsError::InvalidInput(
                    "invalid POINTING polynomial shape".into(),
                ));
            }
            let epoch = self.f64(row, "TIME")?;
            let delta = if time == 0. { 0. } else { time - epoch };
            std::array::from_fn(|axis| {
                (0..=order).rev().fold(0., |value, coefficient| {
                    value * delta + values[ndarray::IxDyn(&[axis, coefficient])]
                })
            })
        } else {
            [lon, lat]
        };
        let [lon_scale, lat_scale] =
            super::metadata_field::direction_radians(self.table(), column)?;
        Ok([lon * lon_scale, lat * lat_scale])
    }
}

fn infer_buffer_half_intervals(samples: &mut [(usize, f64, f64)]) {
    let mut groups = samples
        .chunk_by_mut(|left, right| left.1 == right.1)
        .peekable();
    let mut previous = None;
    while let Some(group) = groups.next() {
        let epoch = group[0].1;
        let neighbor = groups.peek().map(|group| group[0].1).or(previous);
        for (_, _, interval) in group {
            *interval = if *interval == 0. {
                neighbor.map_or(0., |time| (epoch - time).abs() / 2.)
            } else {
                *interval / 2.
            };
        }
        previous = Some(epoch);
    }
}

fn select_buffer_pointing(samples: &[(usize, f64, f64)], start: usize, time: f64) -> Option<usize> {
    let split = samples.partition_point(|sample| sample.0 < start);
    samples[split..]
        .iter()
        .chain(&samples[..split])
        .find(|&&(_, epoch, half)| {
            if half > 0. {
                epoch >= time - half && epoch <= time + half
            } else {
                true
            }
        })
        .map(|sample| sample.0)
}

#[cfg(test)]
mod tests {
    use super::{infer_buffer_half_intervals, select_buffer_pointing};

    #[test]
    fn legacy_buffer_pointing_wraps_and_leaves_stale_intervals_unmatched() {
        let mut samples = [(4, 10., 0.), (6, 20., 0.), (8, 22., 6.)];
        infer_buffer_half_intervals(&mut samples);
        assert_eq!(select_buffer_pointing(&samples, 8, 10.), Some(4));
        assert_eq!(select_buffer_pointing(&samples, 6, 21.), Some(6));
        assert_eq!(select_buffer_pointing(&samples, 8, 21.), Some(8));
        assert_eq!(select_buffer_pointing(&samples, 8, 30.), None);
        samples[1].2 = -1.;
        assert_eq!(select_buffer_pointing(&samples, 8, 30.), Some(6));
    }
}
