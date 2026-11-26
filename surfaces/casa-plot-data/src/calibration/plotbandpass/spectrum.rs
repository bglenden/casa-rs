// SPDX-License-Identifier: LGPL-3.0-or-later
//! Numerical preparation of selected bandpass spectra and channel derivatives.

use casa_core::{ArrayValue, Complex32, Complex64};

use super::table::Spectrum;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ordinate {
    Amplitude,
    Decibels,
    Phase,
    Delay,
}

pub(super) struct Curve {
    pub(super) x: Vec<f64>,
    pub(super) y: Vec<f64>,
}

pub(super) struct Derivative {
    pub(super) curve: Curve,
    pub(super) deviation: f64,
    pub(super) rms: f64,
}

pub(super) struct Outliers {
    pub(super) count: usize,
    pub(super) worst_sigma: f64,
    pub(super) coordinate: f64,
}

impl Derivative {
    pub(super) fn outliers(&self, sigma: f64, edge: usize) -> Option<Outliers> {
        let edge = edge.min(self.curve.y.len().saturating_sub(1) / 2);
        let mut count = 0;
        let mut worst: Option<usize> = None;
        for (index, &value) in self
            .curve
            .y
            .iter()
            .enumerate()
            .skip(edge)
            .take(self.curve.y.len() - 2 * edge)
        {
            if value.abs() > sigma * self.deviation {
                count += 1;
                if worst.is_none_or(|prior| value.abs() > self.curve.y[prior].abs()) {
                    worst = Some(index);
                }
            }
        }
        let index = worst?;
        Some(Outliers {
            count,
            worst_sigma: self.curve.y[index] / self.deviation,
            coordinate: index as f64 + self.curve.x[0],
        })
    }
}

impl Spectrum {
    pub(super) fn curve(
        &self,
        receptor: usize,
        frequencies_ghz: Option<&[f64]>,
        ordinate: Ordinate,
        show_flagged: bool,
    ) -> Result<Curve, String> {
        let (parameters, flags) = match self {
            Self::Samples { parameters, flags } => (parameters, flags),
            Self::Polynomial(poly) => return poly.curve(receptor, ordinate),
        };
        let shape = parameters.shape();
        let [receptors, channels, ..] = shape else {
            return Err(format!("unsupported calibration spectrum shape {shape:?}"));
        };
        if receptor >= *receptors {
            return Err(format!("spectrum has no receptor {receptor}"));
        }
        if let Some(frequencies) = frequencies_ghz
            && frequencies.len() != *channels
        {
            return Err(format!(
                "calibration spectrum has {channels} channels, with {} channel frequencies",
                frequencies.len()
            ));
        }
        let ArrayValue::Bool(flags) = flags else {
            return Err("calibration spectrum flags must be Boolean".into());
        };
        let mut index = vec![0; shape.len()];
        index[0] = receptor;
        let mut curve = Curve {
            x: Vec::with_capacity(*channels),
            y: Vec::with_capacity(*channels),
        };
        for channel in 0..*channels {
            index[1] = channel;
            if flags[index.as_slice()] && !show_flagged {
                continue;
            }
            let x = frequencies_ghz.map_or(channel as f64, |values| values[channel]);
            let y = amplitude_phase(parameters, &index, ordinate)?;
            curve.x.push(x);
            curve.y.push(y);
        }
        Ok(curve)
    }
}

fn amplitude_phase(array: &ArrayValue, index: &[usize], ordinate: Ordinate) -> Result<f64, String> {
    let (amplitude, phase) = match array {
        ArrayValue::Complex32(data) => complex32(data[index]),
        ArrayValue::Complex64(data) => complex64(data[index]),
        ArrayValue::Float32(data) => (f64::from(data[index]), 0.0),
        ArrayValue::Float64(data) => (data[index], 0.0),
        _ => return Err("bandpass solutions need a real or complex array".into()),
    };
    Ok(match ordinate {
        Ordinate::Amplitude => amplitude,
        Ordinate::Decibels => 10.0 * amplitude.log10(),
        Ordinate::Phase => phase,
        Ordinate::Delay => amplitude * 1e9,
    })
}

fn complex32(value: Complex32) -> (f64, f64) {
    (f64::from(value.norm()), f64::from(value.arg().to_degrees()))
}

fn complex64(value: Complex64) -> (f64, f64) {
    (value.norm(), value.arg().to_degrees())
}

impl Curve {
    pub(super) fn differentiate(&self, normalize: bool, edge: usize) -> Derivative {
        let typical = median(&self.y);
        let mut curve = channel_differences(self);
        if normalize {
            for sample in &mut curve.y {
                *sample *= 100.0 / typical;
            }
        }
        let edge = edge.min(curve.y.len().saturating_sub(1) / 2);
        let central = &curve.y[edge..curve.y.len() - edge];
        let center = median(central);
        let differences = central.iter().map(|value| (value - center).abs());
        let deviation = median(&differences.collect::<Vec<_>>()) / 0.6745;
        let mean = central.iter().sum::<f64>() / central.len() as f64;
        let variance = central
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / central.len() as f64;
        Derivative {
            curve,
            deviation,
            rms: variance.sqrt(),
        }
    }
}

/// Detect a large inside-channel derivative after leaving the edge.
pub(super) fn platforming(values: &[f64], threshold: f64) -> bool {
    let length = values.len() as f64;
    let start = length / 32.0 - 1.0;
    let end = length * 31.0 / 32.0 + 1.0;
    if start <= 0.0 || end >= length {
        return false;
    }
    // CASA calculates real endpoints, then passes them to Python 3 range().
    // Use the corresponding integer channel indices.
    let middle = ((start + end) * 0.5) as usize;
    let mut away_from_edge = false;
    let detected = |away_from_edge: &mut bool, value: &f64| {
        if value.abs() > threshold {
            *away_from_edge
        } else {
            *away_from_edge = true;
            false
        }
    };
    if values[start as usize..=middle]
        .iter()
        .any(|value| detected(&mut away_from_edge, value))
    {
        return true;
    }
    away_from_edge = false;
    values[middle + 1..=end as usize]
        .iter()
        .rev()
        .any(|value| detected(&mut away_from_edge, value))
}

fn channel_differences(source: &Curve) -> Curve {
    if source.x.len() < 2 {
        return Curve {
            x: source.x.clone(),
            y: source.y.clone(),
        };
    }
    let channel_width = source.x[1] - source.x[0];
    let pairs = source.x.windows(2).zip(source.y.windows(2));
    let (x, y) = pairs
        .map(|(x, y)| {
            let midpoint = (x[0] + x[1]) / 2.0;
            let difference = (y[1] - y[0]) / (x[1] - x[0]) * channel_width;
            (midpoint, difference)
        })
        .unzip();
    Curve { x, y }
}

pub fn median(values: &[f64]) -> f64 {
    if values.is_empty() || values.iter().any(|value| value.is_nan()) {
        return f64::NAN;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}
