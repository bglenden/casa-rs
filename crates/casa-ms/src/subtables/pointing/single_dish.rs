// SPDX-License-Identifier: LGPL-3.0-or-later
//! Legacy SDGrid pointing selection and fast-scan interpolation.

use std::collections::BTreeMap;

use casa_types::measures::direction::MDirection;

use super::{MsPointing, MsResult};

#[derive(Clone, Copy)]
struct PointingSample {
    row: usize,
    time: f64,
    interval: f64,
}

#[derive(Default)]
struct AntennaPointings {
    samples: Vec<PointingSample>,
    last: usize,
    curve: Option<PointingCurve>,
}

impl MsPointing<'_> {
    /// Resolve `SDGrid` pointings in visibility order. Each request gives the
    /// first antenna, MAIN time and MAIN interval in seconds. A nonempty
    /// POINTING table leaves unmatched samples blank. Faster visibility
    /// sampling interpolates between each antenna's recorded pointings.
    ///
    /// # Errors
    /// A pointing row or its direction is malformed.
    pub fn single_dish_directions(
        &self,
        requests: &[(i32, f64, f64)],
    ) -> MsResult<Vec<Option<MDirection>>> {
        let mut antennas: BTreeMap<_, _> = requests
            .iter()
            .map(|&(antenna, _, _)| (antenna, AntennaPointings::default()))
            .collect();
        for row in 0..self.row_count() {
            if let Some(antenna) = antennas.get_mut(&self.i32(row, "ANTENNA_ID")?) {
                antenna.samples.push(PointingSample {
                    row,
                    time: self.f64(row, "TIME")?,
                    interval: self.f64(row, "INTERVAL")?,
                });
            }
        }
        let mut directions = BTreeMap::new();
        requests
            .iter()
            .map(|&(id, time, interval)| {
                antennas.get_mut(&id).expect("requested antenna").direction(
                    self,
                    time,
                    interval,
                    &mut directions,
                )
            })
            .collect()
    }
}

impl AntennaPointings {
    fn direction(
        &mut self,
        table: &MsPointing<'_>,
        time: f64,
        interval: f64,
        directions: &mut BTreeMap<usize, MDirection>,
    ) -> MsResult<Option<MDirection>> {
        let find = |tolerance: f64| {
            (self.last..self.samples.len())
                .chain((0..self.last).rev())
                .find_map(|index| {
                    let sample = self.samples[index];
                    if sample.interval < 0. {
                        Some(self.last)
                    } else {
                        ((sample.time - time).abs() <= sample.interval / 2. + tolerance)
                            .then_some(index)
                    }
                })
        };
        let Some(index) = find(time.abs() * f64::EPSILON).or_else(|| find(interval / 2.)) else {
            return Ok(None);
        };
        self.last = index;
        let sample = self.samples[index];
        let mut read = |row| {
            if let Some(direction) = directions.get(&row) {
                return Ok(MDirection::clone(direction));
            }
            let direction = table.read_direction(row, None)?;
            directions.insert(row, direction.clone());
            Ok::<_, crate::MsError>(direction)
        };
        let point = read(sample.row)?;
        if interval >= sample.interval || time == sample.time {
            return Ok(Some(point));
        }
        if self.curve.is_none() {
            let knots = self
                .samples
                .iter()
                .map(|sample| {
                    let point = read(sample.row)?;
                    let (longitude, latitude) = point.as_angles();
                    Ok((sample.time, [longitude, latitude]))
                })
                .collect::<MsResult<Vec<_>>>()?;
            self.curve = Some(PointingCurve::new(knots));
        }
        let coordinates = self
            .curve
            .as_ref()
            .expect("prepared pointing curve")
            .at(time);
        Ok(Some(
            coordinates.map_or(point.clone(), |[longitude, latitude]| {
                MDirection::from_angles(longitude, latitude, point.refer())
            }),
        ))
    }
}

struct PointingCurve {
    knots: Vec<(f64, [f64; 2])>,
    second_derivatives: Vec<[f64; 2]>,
}

impl PointingCurve {
    fn new(mut knots: Vec<(f64, [f64; 2])>) -> Self {
        knots.sort_by(|left, right| left.0.total_cmp(&right.0));
        knots.dedup_by(|left, right| left.0 == right.0);
        let mut second_derivatives = vec![[0.; 2]; knots.len()];
        // SDPosInterpolator uses natural boundary conditions with four or
        // more pointings. The shorter curve has zero second derivatives.
        if knots.len() >= 4 {
            let mut upper = vec![0.; knots.len()];
            for index in 1..knots.len() - 1 {
                let (earlier, prev) = knots[index - 1];
                let (time, middle) = knots[index];
                let (later, next) = knots[index + 1];
                let left = time - earlier;
                let right = later - time;
                let pivot = 2. * (left + right) - left * upper[index - 1];
                upper[index] = right / pivot;
                second_derivatives[index] = std::array::from_fn(|axis| {
                    let slopes =
                        (next[axis] - middle[axis]) / right - (middle[axis] - prev[axis]) / left;
                    (6. * slopes - left * second_derivatives[index - 1][axis]) / pivot
                });
            }
            for index in (1..knots.len() - 1).rev() {
                let next = second_derivatives[index + 1];
                for (current, next) in second_derivatives[index].iter_mut().zip(next) {
                    *current -= upper[index] * next;
                }
            }
        }
        Self {
            knots,
            second_derivatives,
        }
    }

    fn at(&self, epoch: f64) -> Option<[f64; 2]> {
        if self.knots.len() < 2 {
            return None;
        }
        let left = self
            .knots
            .partition_point(|(time, _)| *time <= epoch)
            .saturating_sub(1)
            .min(self.knots.len() - 2);
        let (start, first) = self.knots[left];
        let (end, last) = self.knots[left + 1];
        let width = end - start;
        let b = (epoch - start) / width;
        let a = 1. - b;
        let cubic = |weight: f64| (weight.powi(3) - weight) * width * width / 6.;
        Some(std::array::from_fn(|axis| {
            a * first[axis]
                + b * last[axis]
                + cubic(a) * self.second_derivatives[left][axis]
                + cubic(b) * self.second_derivatives[left + 1][axis]
        }))
    }
}
