// SPDX-License-Identifier: LGPL-3.0-or-later
//! Legacy SDGrid pointing selection and fast-scan interpolation.

use std::collections::BTreeMap;

use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::{EpochRef, MEpoch, MeasFrame};

use crate::derived::engine::{MsCalEngine, detect_epoch_reference};

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
    /// Interpolate DIRECTION for map calibration at the requested antenna and
    /// epoch in MAIN seconds. Use finite endpoints outside the sampled range.
    /// CASA uses linear interpolation within three knots of a sampling gap.
    ///
    /// # Errors
    /// A requested antenna has no usable pointing or its frame is invalid.
    pub fn scan_directions(
        &self,
        requests: &[(i32, f64)],
        engine: &MsCalEngine,
    ) -> MsResult<Vec<MDirection>> {
        let Some(&(first_antenna, first_time)) = requests.first() else {
            return Ok(Vec::new());
        };
        let pointing_reference = detect_epoch_reference(self.table(), "TIME", EpochRef::UTC);
        let mut antennas = requests
            .iter()
            .map(|&(id, _)| (id, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        let mut shared = Vec::new();
        let mut sample_counts = BTreeMap::<_, usize>::new();
        for row in 0..self.row_count() {
            let antenna = self.i32(row, "ANTENNA_ID")?;
            *sample_counts.entry(antenna).or_default() += 1;
            let samples = if antenna == -1 {
                Some(&mut shared)
            } else {
                antennas.get_mut(&antenna)
            };
            if let Some(samples) = samples {
                let time = self.f64(row, "TIME")?;
                let time = if pointing_reference == EpochRef::UTC {
                    time
                } else {
                    let antenna = if antenna == -1 {
                        first_antenna
                    } else {
                        antenna
                    };
                    let frame = engine.antenna_frame(first_time, antenna as usize)?;
                    utc_seconds(time, pointing_reference, &frame)?
                };
                let interval = self.f64(row, "INTERVAL")?;
                let direction = self.read_direction(row, None)?;
                samples.push((time, interval, direction));
            }
        }
        for samples in antennas.values_mut().filter(|samples| samples.is_empty()) {
            samples.clone_from(&shared);
        }
        let interpolate_spline = sample_counts.values().all(|&count| count >= 4);
        let curves = antennas
            .into_iter()
            .map(|(antenna, mut samples)| {
                samples.sort_by(|left, right| left.0.total_cmp(&right.0));
                samples.dedup_by(|left, right| left.0 == right.0);
                let knots = samples
                    .iter()
                    .map(|(time, _, dir)| {
                        let (lon, lat) = dir.as_angles();
                        // casacore presents the signed longitude to the spline.
                        let lon = (lon + std::f64::consts::PI).rem_euclid(std::f64::consts::TAU)
                            - std::f64::consts::PI;
                        (*time, [lon, lat])
                    })
                    .collect();
                let curve = PointingCurve::new(knots);
                let mut linear = vec![!interpolate_spline; samples.len()];
                if samples.len() >= 7 {
                    let rounded = |seconds: f64| (seconds * 10_000.).round();
                    for (index, pair) in samples.windows(2).enumerate() {
                        if rounded(pair[1].0 - pair[0].0) > rounded(pair[1].1) {
                            let low = (index + 1).saturating_sub(3);
                            let high = (index + 5).min(samples.len());
                            linear[low..high].fill(true);
                        }
                    }
                }
                (antenna, (samples, curve, linear))
            })
            .collect::<BTreeMap<_, _>>();
        requests
            .iter()
            .map(|&(antenna, time)| {
                let (samples, curve, linear) = &curves[&antenna];
                let frame = engine.antenna_frame(time, antenna as usize)?;
                let time = utc_seconds(time, engine.time_reference(), &frame)?;
                let index = samples.partition_point(|&(epoch, _, _)| epoch < time);
                let direction = if let Some((_, _, direction)) = samples
                    .get(index)
                    .filter(|(epoch, _, _)| *epoch == time || index == 0)
                    .or_else(|| samples.last().filter(|_| index == samples.len()))
                {
                    direction.clone()
                } else if let (Some(left), Some(right)) =
                    (samples.get(index.wrapping_sub(1)), samples.get(index))
                {
                    let (lon, lat) = if linear[index] || left.2.refer() != right.2.refer() {
                        let frame = frame
                            .clone()
                            .with_epoch(MEpoch::from_mjd_seconds(right.0, EpochRef::UTC));
                        let right_dir = right.2.convert_to(left.2.refer(), &frame)?;
                        let to_signed = |value: f64| value.sin().atan2(value.cos());
                        let (left_lon, left_lat) = left.2.as_angles();
                        let (right_lon, right_lat) = right_dir.as_angles();
                        let part = (time - left.0) / (right.0 - left.0);
                        (
                            to_signed(left_lon)
                                + part * (to_signed(right_lon) - to_signed(left_lon)),
                            left_lat + part * (right_lat - left_lat),
                        )
                    } else {
                        let [lon, lat] = curve.at(time).expect("interior pointing sample");
                        (lon, lat)
                    };
                    MDirection::from_angles(lon, lat, left.2.refer())
                } else {
                    return Err(crate::MsError::InvalidInput(format!(
                        "No pointing for antenna {antenna}"
                    )));
                };
                Ok(direction.convert_to(DirectionRef::J2000, &frame)?)
            })
            .collect()
    }

    /// Resolve `SDGrid` pointings in visibility order. Each request gives the
    /// first antenna, MAIN time and MAIN interval in seconds. A nonempty
    /// POINTING table leaves unmatched samples blank. Faster visibility
    /// sampling interpolates between each antenna's recorded pointings.
    /// Missing optional `column` values use DIRECTION. With `convert_first`,
    /// each direction is converted in its recorded antenna frame and epoch
    /// before interpolation.
    ///
    /// # Errors
    /// A pointing row or its direction is malformed.
    pub fn single_dish_directions(
        &self,
        requests: &[(i32, f64, f64)],
        column: &str,
        convert_first: Option<(&MsCalEngine, DirectionRef)>,
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
        let column = if self
            .table()
            .schema()
            .is_some_and(|schema| schema.contains_column(column))
        {
            column
        } else {
            "DIRECTION"
        };
        requests
            .iter()
            .map(|&(id, time, interval)| {
                let read = |sample: PointingSample| {
                    if let Some(direction) = directions.get(&sample.row) {
                        return Ok(MDirection::clone(direction));
                    }
                    let mut direction = self.read_direction_column(sample.row, column, None)?;
                    if let Some((engine, target)) = convert_first {
                        let frame = engine.antenna_frame(sample.time, id as usize)?;
                        direction = direction.convert_to(target, &frame)?;
                    }
                    directions.insert(sample.row, direction.clone());
                    Ok(direction)
                };
                antennas
                    .get_mut(&id)
                    .expect("requested antenna")
                    .direction(time, interval, read)
            })
            .collect()
    }
}

fn utc_seconds(seconds: f64, reference: EpochRef, frame: &MeasFrame) -> MsResult<f64> {
    if reference == EpochRef::UTC {
        return Ok(seconds);
    }
    let epoch = MEpoch::from_mjd_seconds(seconds, reference).convert_to(EpochRef::UTC, frame)?;
    let time = epoch.value();
    Ok(time.day() * 86_400. + time.frac_as_seconds())
}

impl AntennaPointings {
    fn direction(
        &mut self,
        time: f64,
        interval: f64,
        mut read: impl FnMut(PointingSample) -> MsResult<MDirection>,
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
        let point = read(sample)?;
        if interval >= sample.interval || time == sample.time {
            return Ok(Some(point));
        }
        if self.curve.is_none() {
            let knots = self
                .samples
                .iter()
                .map(|sample| {
                    let point = read(*sample)?;
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
