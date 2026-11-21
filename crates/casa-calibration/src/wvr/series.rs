// SPDX-License-Identifier: LGPL-3.0-or-later
//! Time-domain WVR operations and antenna diagnostics.

use std::fmt::Write;

use casa_ms::MeasurementSet;

use super::data::{Observation, RadiometerIntegration, RadiometerSeries, Temperatures};
use super::paths::{Retrieval, thermal_error_meters};
use super::retrieval::Posterior;
use super::{WvrAntennaReport, WvrError, WvrRequest};

pub(super) fn smooth(points: &mut [RadiometerIntegration], samples: usize) {
    if samples < 2 {
        return;
    }
    let radius = samples / 2;
    for run in points.chunk_by_mut(|a, b| a.source_id == b.source_id && a.state_id == b.state_id) {
        for index in radius..run.len().saturating_sub(radius) {
            for antenna in 0..run[index].temperatures.len() {
                let mut value = [0.0; 4];
                for offset in 0..=2 * radius {
                    let point = &run[index - radius + offset];
                    let temperatures = point.temperatures[antenna].unwrap_or([0.0; 4]);
                    let weight =
                        if samples.is_multiple_of(2) && (offset == 0 || offset == 2 * radius) {
                            0.5
                        } else {
                            1.0
                        };
                    for (sum, temperature) in value.iter_mut().zip(temperatures) {
                        *sum += temperature * weight;
                    }
                }
                run[index].temperatures[antenna] = Some(value.map(|sum| sum / samples as f64));
            }
        }
    }
}

pub(super) struct AntennaReplacement {
    pub flagged: Vec<bool>,
    pub impossible: Vec<bool>,
    pub reported_fraction: Vec<f64>,
}

pub(super) fn replace_bad_antennas(
    ms: &MeasurementSet,
    data: &mut RadiometerSeries,
    request: &WvrRequest,
    failed_reference: Option<usize>,
) -> Result<AntennaReplacement, WvrError> {
    let antennas = ms.antenna()?;
    let count = antennas.row_count();
    let without_radiometer = (0..count)
        .map(|id| antennas.name(id).map(|name| name.starts_with("CM")))
        .collect::<Result<Vec<_>, _>>()?;
    let flagged_rows = (0..count)
        .map(|id| antennas.flag_row(id))
        .collect::<Result<Vec<_>, _>>()?;
    if (0..count).all(|id| {
        flagged_rows[id] || without_radiometer[id] || request.flagged_antennas.contains(&id)
    }) {
        return Err(WvrError::Invalid(
            "No good antennas with WVR data found".into(),
        ));
    }
    let minimum_fraction = request.minimum_good_fraction.clamp(0.0, 1.0);
    let low_fraction = |id: usize| {
        data.fitted_antennas[id]
            && (data.valid_counts[id] == 0 || good_fraction(data, id) < minimum_fraction)
    };
    let interpolate: Vec<_> = (0..count)
        .map(|id| {
            without_radiometer[id]
                || request.flagged_antennas.contains(&id)
                || low_fraction(id)
                || Some(id) == failed_reference
        })
        .collect();
    let mut result = AntennaReplacement {
        flagged: (0..count)
            .map(|id| {
                request.flagged_antennas.contains(&id)
                    || flagged_rows[id]
                    || low_fraction(id)
                    || Some(id) == failed_reference
            })
            .collect(),
        impossible: vec![false; count],
        reported_fraction: (0..count)
            .map(|id| {
                if low_fraction(id) {
                    good_fraction(data, id)
                } else {
                    1.0
                }
            })
            .collect(),
    };
    if !interpolate.iter().any(|&flag| flag) {
        return Ok(result);
    }
    let positions = (0..count)
        .map(|id| antennas.position(id))
        .collect::<Result<Vec<_>, _>>()?;
    for antenna in 0..count {
        if !interpolate[antenna] {
            continue;
        }
        let mut donors: Vec<_> = (0..count)
            .filter(|&id| !interpolate[id] && !flagged_rows[id])
            .filter_map(|id| {
                let separation = positions[antenna]
                    .into_iter()
                    .zip(positions[id])
                    .map(|(left, right)| (left - right).powi(2))
                    .sum::<f64>()
                    .sqrt();
                (separation <= request.max_distance_meters).then_some((separation, id))
            })
            .collect();
        donors.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        donors.truncate(3);
        let usable = donors.len() >= request.minimum_neighbors && !donors.is_empty();
        result.impossible[antenna] = !usable;
        let total = donors.iter().map(|entry| entry.0).sum::<f64>();
        for point in &mut data.integrations {
            point.temperatures[antenna] = usable.then(|| {
                std::array::from_fn(|channel| {
                    let mut value = 0.0;
                    for &(distance, donor) in &donors {
                        let reading =
                            point.temperatures[donor].map_or(0.0, |sample| sample[channel]);
                        if reading <= 0.0 {
                            return 0.0;
                        }
                        let weight = if total > 0.0 {
                            distance / total
                        } else {
                            1.0 / donors.len() as f64
                        };
                        value += reading * weight;
                    }
                    value
                })
            });
        }
    }
    Ok(result)
}

pub(super) fn reference_antenna(
    data: &RadiometerSeries,
    replaced: &AntennaReplacement,
    request: &WvrRequest,
) -> Result<usize, WvrError> {
    let usable = |&id: &usize| replaced.impossible.get(id) == Some(&false);
    let candidate = if request.reference_antennas.is_empty() {
        (0..data.sample_counts.len())
            .filter(|&id| data.fitted_antennas[id])
            .find(usable)
    } else {
        request.reference_antennas.iter().copied().find(usable)
    };
    candidate.ok_or(WvrError::ReferenceUnavailable)
}

pub(super) struct FitInterval {
    pub reference: usize,
    pub boundaries: [f64; 2],
}

pub(super) fn fit_intervals(
    points: &[RadiometerIntegration],
    observations: &[Observation],
    request: &WvrRequest,
) -> Vec<FitInterval> {
    if request.continuum {
        let midpoint = points.len() / 2;
        return vec![FitInterval {
            reference: midpoint,
            boundaries: [points[midpoint].time_seconds; 2],
        }];
    }
    if !request.segment_sources {
        let stride = points.len() / request.solution_count.saturating_add(1);
        return (1..=request.solution_count.min(points.len()))
            .map(|slot| {
                let index = stride * slot;
                FitInterval {
                    reference: index,
                    boundaries: [points[index].time_seconds; 2],
                }
            })
            .collect();
    }
    observations
        .chunk_by(|left, right| {
            left.source_id == right.source_id
                || request.tied_sources.iter().any(|group| {
                    group.contains(&left.source_id) && group.contains(&right.source_id)
                })
        })
        .map(|segment| {
            let start = segment[0].time_seconds;
            let end = segment[segment.len() - 1].time_seconds;
            let middle = 0.5 * (start + end);
            let reference = points
                .partition_point(|point| point.time_seconds < middle)
                .min(points.len() - 1);
            FitInterval {
                reference,
                boundaries: [start, end],
            }
        })
        .collect()
}

pub(super) fn statistics_intervals(data: &RadiometerSeries, request: &WvrRequest) -> Vec<[f64; 2]> {
    let observations = &data.observations;
    if request.statistic_sources.is_empty() && request.statistic_fields.is_empty() {
        return vec![[
            observations[0].time_seconds,
            observations[observations.len() - 1].time_seconds,
        ]];
    }
    let matches = |point: &Observation| {
        if request.statistic_sources.is_empty() {
            request.statistic_fields.contains(&point.field_id)
        } else {
            request.statistic_sources.contains(&point.source_id)
        }
    };
    let mut intervals = Vec::new();
    let mut start = None;
    let mut previous = observations[0].time_seconds;
    for (index, point) in observations.iter().enumerate() {
        if Some(point.window_id) != data.statistic_window {
            continue;
        }
        let selected = matches(point);
        if selected && start.is_none() {
            start = Some(point.time_seconds);
        }
        if (!selected || index == observations.len() - 1)
            && let Some(begin) = start.take()
        {
            let end = if selected && point.repeated {
                point.time_seconds
            } else {
                previous
            };
            intervals.push([begin, end]);
        }
        previous = point.time_seconds;
    }
    intervals
}

#[derive(Default)]
struct Variance {
    count: usize,
    mean: f64,
    residual_squared: f64,
}

impl Variance {
    fn observe(&mut self, value: f64) {
        self.count += 1;
        let residual = value - self.mean;
        self.mean += residual / self.count as f64;
        self.residual_squared += residual * (value - self.mean);
    }

    fn rms_meters(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        (self.residual_squared.max(0.0) / self.count as f64).sqrt()
    }

    fn rms_microns(&self) -> f64 {
        let micros = self.rms_meters() * 1e6;
        if micros == 0.0 {
            return 0.0;
        }
        let scale = 10.0_f64.powf(2.0 - micros.log10().floor());
        (micros * scale).round_ties_even() / scale
    }
}

pub(super) struct PathDiagnostics {
    antennas: Vec<WvrAntennaReport>,
    zenith: Vec<Variance>,
    discrepancy: Vec<Variance>,
    baseline: Vec<Variance>,
}

impl PathDiagnostics {
    pub fn new(names: Vec<String>, replaced: &AntennaReplacement) -> Self {
        let count = names.len();
        let antennas = names
            .into_iter()
            .enumerate()
            .map(|(id, name)| WvrAntennaReport {
                has_radiometer: !name.starts_with("CM"),
                name,
                flagged: replaced.flagged[id],
                good_fraction: replaced.reported_fraction[id],
                rms_microns: 0.0,
                discrepancy_microns: 0.0,
            })
            .collect();
        Self {
            antennas,
            zenith: (0..count).map(|_| Variance::default()).collect(),
            discrepancy: (0..count).map(|_| Variance::default()).collect(),
            baseline: (0..count * count.saturating_sub(1) / 2)
                .map(|_| Variance::default())
                .collect(),
        }
    }

    pub fn observe(
        &mut self,
        antenna: usize,
        path: f64,
        elevation: f64,
        reading: Option<Temperatures>,
        derivative: [f64; 4],
    ) {
        if path > 0.0 {
            self.zenith[antenna].observe(path * elevation.sin());
        }
        if let Some(reading) = reading {
            let left = reading[1] / derivative[1];
            let right = reading[3] / derivative[3];
            if left.is_finite() && right.is_finite() && left > 0.0 && right > 0.0 {
                self.discrepancy[antenna].observe(left - right);
            }
        }
    }

    pub fn baselines(&mut self, paths: &[f64]) {
        let mut index = 0;
        for (left, &first) in paths.iter().enumerate() {
            for &second in &paths[left + 1..] {
                if first > 0.0 && second > 0.0 {
                    self.baseline[index].observe(first - second);
                }
                index += 1;
            }
        }
    }

    pub fn finish(mut self) -> (Vec<WvrAntennaReport>, f64) {
        for (id, antenna) in self.antennas.iter_mut().enumerate() {
            antenna.rms_microns = self.zenith[id].rms_microns();
            antenna.discrepancy_microns = self.discrepancy[id].rms_microns();
        }
        let largest = self
            .baseline
            .iter()
            .map(Variance::rms_meters)
            .fold(0.0, f64::max);
        (self.antennas, largest)
    }
}

fn good_fraction(data: &RadiometerSeries, antenna: usize) -> f64 {
    let total = data.sample_counts[antenna];
    if total == 0 {
        0.0
    } else {
        data.valid_counts[antenna] as f64 / total as f64
    }
}

pub(super) fn render_log(
    antennas: &[WvrAntennaReport],
    fits: &[Posterior],
    reference: usize,
    intervals: &[[f64; 2]],
    first_time: f64,
    representative: Retrieval,
    largest_baseline_meters: f64,
) -> String {
    let mut log = String::from("WVR phase calibration\n\n");
    let _ = writeln!(
        log,
        "Choosing antenna {reference} == {} as reference antenna for dTdL calculations.",
        antennas[reference].name
    );
    log.push_str("       Retrieved parameters\n----------------------------------------------------------------\n");
    log.push_str("Evidence\tPWV\tPWV Error\tdT1dL\tdT2dL\tdT3dL\tdT4dL\n");
    for fit in fits {
        let _ = write!(
            log,
            "{:.8e}\t{:.6}\t{:.6}",
            fit.evidence, fit.precipitable_water_mm, fit.precipitable_water_error_mm
        );
        for derivative in fit.coefficients.temperature_per_path_k_per_meter {
            let _ = write!(log, "\t{:.8}", derivative * 1e-3);
        }
        log.push('\n');
    }
    log.push_str(
        "\nTimes used for the statistics calculation (in seconds from first astro datum)\n",
    );
    log.push_str("-----------------------------------------------------------------------------\n");
    for &[start, end] in intervals {
        let _ = writeln!(log, "({}, {})", start - first_time, end - first_time);
    }
    log.push_str("     Antenna/WVR information:\n-----------------------------------------------------------------------\n");
    log.push_str("#\tName\tWVR?\tFlag?\tRMS (um)\tDisc (um)\n");
    for (id, report) in antennas.iter().enumerate() {
        let yes_no = |value| if value { "Yes" } else { "No" };
        let _ = writeln!(
            log,
            "{id}\t{}\t{}\t{}\t{}\t{}",
            report.name,
            yes_no(report.has_radiometer),
            yes_no(report.flagged),
            report.rms_microns,
            report.discrepancy_microns
        );
    }
    let coefficients = representative.temperature_per_path_k_per_meter;
    let thermal = thermal_error_meters(coefficients) * 1e6;
    let strongest = largest_baseline_meters * 1e6;
    // CASA's diagnostic converts the second moment numerically from its fit's
    // K/mm scale by 1e3. Our stored variance already uses K/m squared.
    let coefficient_error =
        strongest * representative.coefficient_second_moment[1] / (1e3 * coefficients[1]);
    log.push_str("  Expected performance\n------------------------------------------------------------------\n");
    let _ = writeln!(
        log,
        "* Estimated WVR thermal contribution to path fluctuations (micron per antenna): {thermal:.6}"
    );
    let _ = writeln!(
        log,
        "* Greatest estimated path fluctuation (micron on a baseline): {strongest:.6}"
    );
    let _ = writeln!(
        log,
        "* Rough estimate of path error due to coefficient error (micron on a baseline): {coefficient_error:.6e}"
    );
    log.push_str("\nWriting gain table ...\n");
    log
}
