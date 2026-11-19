// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fit the VLA dry and wet zenith opacity over CASA's reference frequencies.

use std::collections::BTreeSet;
use std::ops::Range;

use casa_atmosphere::{AbsorptionProfile, AtmosphereProfile, ProfileParameters, SpectralGrid};

const CHANNELS: usize = 2000;
const START_HZ: f64 = 12_500_000.0;
const STEP_HZ: f64 = 25_000_000.0;
const PROFILES: usize = 20;

pub(super) struct OpacityFit {
    samples: Vec<Range<usize>>,
    indices: Vec<usize>,
    curves: Vec<OpacityCurve>,
}

impl OpacityFit {
    pub(super) fn new(windows_ghz: &[f64], whole_spectrum: bool) -> Result<Self, String> {
        let samples = windows_ghz
            .iter()
            .copied()
            .map(interpolation_samples)
            .collect::<Result<Vec<_>, _>>()?;
        // CASA fits all 2,000 channels even when only the SPW return values
        // will be used. Fit those same sample frequencies on demand.
        let indices = if whole_spectrum {
            (0..CHANNELS).collect::<Vec<_>>()
        } else {
            samples
                .iter()
                .cloned()
                .flatten()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect()
        };
        let frequencies = indices
            .iter()
            .copied()
            .map(frequency_hz)
            .collect::<Vec<_>>();
        let curves = fit_opacity(&frequencies)?;
        Ok(Self {
            samples,
            indices,
            curves,
        })
    }

    pub(super) fn windows(&self, windows_ghz: &[f64], water_mm: f64) -> Vec<f64> {
        windows_ghz
            .iter()
            .zip(&self.samples)
            .map(|(&frequency, sample)| {
                let point =
                    sample_number(frequency).clamp(sample.start as f64, (sample.end - 1) as f64);
                let lower = point.floor() as usize;
                let upper = point.ceil() as usize;
                let value = |index| {
                    let index = self
                        .indices
                        .binary_search(&index)
                        .expect("opacity sample was fitted");
                    self.curves[index].at(water_mm)
                };
                let start = value(lower);
                start + (point - lower as f64) * (value(upper) - start)
            })
            .collect()
    }

    pub(super) fn frequencies_ghz(&self) -> Vec<f64> {
        self.indices
            .iter()
            .map(|&index| frequency_hz(index) * 1e-9)
            .collect()
    }

    pub(super) fn evaluate(&self, water_mm: f64) -> Vec<f64> {
        self.curves.iter().map(|curve| curve.at(water_mm)).collect()
    }
}

fn frequency_hz(index: usize) -> f64 {
    START_HZ + STEP_HZ * index as f64
}

fn sample_number(frequency_ghz: f64) -> f64 {
    (frequency_ghz * 1e9 - START_HZ) / STEP_HZ
}

fn interpolation_samples(frequency_ghz: f64) -> Result<Range<usize>, String> {
    if !frequency_ghz.is_finite() {
        return Err("spectral-window frequency must be finite".into());
    }
    let sample = sample_number(frequency_ghz).clamp(0.0, (CHANNELS - 1) as f64);
    let lower = sample.floor();
    let nearest = lower as usize + usize::from(sample - lower > 0.5);
    if nearest < 2 {
        return Err("opacity interpolation has no samples for this low frequency".into());
    }
    Ok(nearest - 2..(nearest + 2).min(CHANNELS))
}

#[derive(Clone, Copy)]
struct OpacityCurve {
    intercept: f64,
    slope: f64,
}

impl OpacityCurve {
    fn at(self, water_mm: f64) -> f64 {
        self.intercept + self.slope * water_mm
    }
}

fn fit_opacity(frequencies: &[f64]) -> Result<Vec<OpacityCurve>, String> {
    if frequencies.is_empty() {
        return Ok(Vec::new());
    }
    let mut grid = SpectralGrid::new();
    grid.add_channels(frequencies, 0)
        .map_err(|error| error.to_string())?;
    let mut sums = vec![(0.0, 0.0); frequencies.len()];
    let mut sum_water = 0.;
    let mut sum_water_squared = 0.;
    for profile in 1..=PROFILES {
        let parameters = ProfileParameters {
            altitude_m: 2125.0,
            ground_pressure_hpa: 790.0,
            relative_humidity_percent: 20.0 * profile as f64,
            ..ProfileParameters::default()
        };
        let atmosphere = AtmosphereProfile::new(&parameters).map_err(|error| error.to_string())?;
        let water_m = atmosphere.ground_water_column_m();
        let water_mm = 1000.0 * water_m;
        let absorption = AbsorptionProfile::new_totals(atmosphere, grid.clone());
        sum_water += water_mm;
        sum_water_squared += water_mm * water_mm;
        for (channel, (sum_opacity, sum_opacity_water)) in sums.iter_mut().enumerate() {
            let opacity =
                absorption.dry_opacity(channel) + absorption.wet_opacity(channel, water_m);
            *sum_opacity += opacity;
            *sum_opacity_water += opacity * water_mm;
        }
    }
    let mean_water = sum_water / PROFILES as f64;
    let spread = sum_water_squared - sum_water * mean_water;
    Ok(sums
        .into_iter()
        .map(|(sum_opacity, sum_opacity_water)| {
            let slope = (sum_opacity_water - mean_water * sum_opacity) / spread;
            OpacityCurve {
                intercept: sum_opacity / PROFILES as f64 - slope * mean_water,
                slope,
            }
        })
        .collect())
}
