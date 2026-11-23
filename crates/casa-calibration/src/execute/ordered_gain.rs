// SPDX-License-Identifier: LGPL-3.0-or-later
//! Float diagonal Jones application for the calibration library.

use std::collections::HashMap;
use std::sync::Arc;

use casa_types::Complex32;
use ndarray::{ArrayD, Ix2};

use crate::lookup::complex_from_casa_polar;

use super::{
    ApplyBuffer, ApplyExecutionError, CalibrationGrid, GainGrid, bandpass, correcting_gain,
    correlation_receptors, stokes_name,
};

#[derive(Clone, Copy)]
struct Sample {
    inverse: Complex32,
    power: f32,
    flagged: bool,
}

impl Sample {
    fn new(value: Complex32, flag: bool, round_trip: bool) -> Self {
        let (value, flagged) = correcting_gain(value, flag);
        let value = if round_trip && !flagged {
            let phase = value.im.atan2(value.re);
            let amplitude = value.re.hypot(value.im);
            complex_from_casa_polar(amplitude, phase)
        } else {
            value
        };
        let power = value.re * value.re + value.im * value.im;
        let inverse = if power.is_normal() {
            Complex32::new(value.re / power, -value.im / power)
        } else {
            // Preserve the range of the scaled complex division for a very
            // small or very large finite gain.
            let re = f64::from(value.re);
            let im = f64::from(value.im);
            let norm = re * re + im * im;
            Complex32::new((re / norm) as f32, (-im / norm) as f32)
        };
        Self {
            inverse,
            power,
            flagged,
        }
    }
}

struct OrderedGrid {
    samples: Vec<Sample>,
    receptors: usize,
    channels: usize,
}

impl OrderedGrid {
    fn from_solution(grid: &GainGrid, round_trip: bool) -> Self {
        let samples = (0..grid.receptor_count)
            .flat_map(|receptor| {
                (0..grid.channel_count).map(move |channel| {
                    Sample::new(
                        grid.value_at(receptor, channel),
                        grid.flag_at(receptor, channel),
                        round_trip,
                    )
                })
            })
            .collect();
        Self {
            samples,
            receptors: grid.receptor_count,
            channels: grid.channel_count,
        }
    }

    fn sample(&self, receptor: usize, channel: usize) -> Sample {
        let receptor = receptor.min(self.receptors.saturating_sub(1));
        let channel = channel.min(self.channels.saturating_sub(1));
        self.samples
            .get(receptor * self.channels + channel)
            .copied()
            .unwrap_or(Sample {
                inverse: Complex32::new(1., 0.),
                power: 1.,
                flagged: true,
            })
    }
}

struct CachedGrid {
    solution: Arc<CalibrationGrid>,
    round_trip: bool,
    ordered: Arc<OrderedGrid>,
}

#[derive(Default)]
pub(super) struct Cache {
    recent: HashMap<(usize, i32), CachedGrid>,
}

#[derive(Clone, Copy)]
pub(super) struct Antenna<'a> {
    pub(super) id: i32,
    pub(super) grid: &'a Arc<CalibrationGrid>,
    pub(super) round_trip: bool,
}

impl Cache {
    fn grid(&mut self, table: usize, sample: Antenna<'_>) -> Option<Arc<OrderedGrid>> {
        let CalibrationGrid::Complex(solution) = sample.grid.as_ref() else {
            return None;
        };
        let make = || CachedGrid {
            solution: Arc::clone(sample.grid),
            round_trip: sample.round_trip,
            ordered: Arc::new(OrderedGrid::from_solution(solution, sample.round_trip)),
        };
        let entry = self.recent.entry((table, sample.id)).or_insert_with(make);
        if entry.round_trip != sample.round_trip || !Arc::ptr_eq(&entry.solution, sample.grid) {
            *entry = make();
        }
        Some(Arc::clone(&entry.ordered))
    }
}

pub(super) struct Options<'a> {
    pub(super) table: usize,
    pub(super) calibrate_weights: bool,
    pub(super) bandpass_weights: Option<&'a [f32]>,
    pub(super) first_receptor_only: bool,
}

pub(super) fn apply(
    cache: &mut Cache,
    antennas: [Antenna<'_>; 2],
    options: Options<'_>,
    output: &mut ApplyBuffer<'_>,
    weights: [Option<&mut ArrayD<f32>>; 2],
) -> Result<bool, ApplyExecutionError> {
    let Some(first) = cache.grid(options.table, antennas[0]) else {
        return Ok(false);
    };
    let Some(second) = cache.grid(options.table, antennas[1]) else {
        return Ok(false);
    };
    let mut data = output
        .corrected
        .view_mut()
        .into_dimensionality::<Ix2>()
        .expect("validated DATA");
    let mut flags = output.propagate_flags.then(|| {
        output
            .flags
            .view_mut()
            .into_dimensionality::<Ix2>()
            .expect("validated FLAG")
    });
    let mut weights = weights.map(|spectrum| {
        spectrum.map(|values| {
            values
                .view_mut()
                .into_dimensionality::<Ix2>()
                .expect("validated weight spectrum")
        })
    });
    for (corr, &code) in output.correlation_types.iter().enumerate() {
        let (left, right) = if options.first_receptor_only {
            bandpass::receptors(output.correlation_types, corr)
        } else {
            correlation_receptors(code)
        }
        .ok_or_else(|| ApplyExecutionError::UnsupportedCorrelationLayout {
            data_desc_id: output.data_desc_id,
            correlation_types: output
                .correlation_types
                .iter()
                .map(|&code| stokes_name(code).to_string())
                .collect(),
        })?;
        let left_scalar = (first.channels == 1).then(|| first.sample(left, 0));
        let right_scalar = (second.channels == 1).then(|| second.sample(right, 0));
        let bandpass_power = options.bandpass_weights.map(|weights| weights[corr]);
        for chan in 0..data.ncols() {
            let original = output.data_channels.original_index(chan);
            let left = left_scalar.unwrap_or_else(|| first.sample(left, original));
            let right = right_scalar.unwrap_or_else(|| second.sample(right, original));
            // VisJones applies the left station first, rounding to Float
            // before applying the conjugated right station.
            let visibility = &mut data[[corr, chan]];
            *visibility *= left.inverse;
            *visibility *= right.inverse.conj();
            if let Some(flags) = &mut flags
                && (left.flagged || right.flagged)
                && !flags[[corr, chan]]
            {
                flags[[corr, chan]] = true;
                *output.newly_flagged_samples += 1;
            }
            if options.calibrate_weights {
                let factor = bandpass_power.unwrap_or(left.power * right.power);
                for spectrum in weights.iter_mut().flatten() {
                    spectrum[[corr, chan]] *= factor;
                }
            }
        }
    }
    Ok(true)
}
