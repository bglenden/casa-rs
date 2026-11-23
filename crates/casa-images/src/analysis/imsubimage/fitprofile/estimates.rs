// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::LN_2;
use std::path::Path;

use crate::ImageError;

use super::models::{LineEstimate, LineKind};

pub(super) fn read(path: &Path) -> Result<Vec<LineEstimate>, ImageError> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        invalid(format!(
            "Could not read estimates file {}: {error}",
            path.display()
        ))
    })?;
    text.lines()
        .enumerate()
        .filter(|(_, row)| !row.starts_with('#') && !row.trim().is_empty())
        .map(|(index, row)| {
            parse(row).map_err(|error| {
                invalid(format!(
                    "Estimates file {}, line {}: {error}",
                    path.display(),
                    index + 1
                ))
            })
        })
        .collect()
}

fn parse(row: &str) -> Result<LineEstimate, String> {
    let fields: Vec<_> = row.split(',').map(str::trim).collect();
    let [amplitude, center, width, rest @ ..] = fields.as_slice() else {
        return Err("Expected peak, center and FWHM".into());
    };
    if rest.len() > 1 {
        return Err("Expected at most one fixed-parameter field".into());
    }
    let mut values = [0.; 3];
    for ((target, input), name) in values
        .iter_mut()
        .zip([amplitude, center, width])
        .zip(["peak", "position", "FWHM"])
    {
        *target = input
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .ok_or_else(|| format!("{name} value {input:?} is not numeric"))?;
    }
    let flags = rest.first().copied().unwrap_or("");
    if let Some(flag) = flags.bytes().find(|flag| !b"pcf".contains(flag)) {
        return Err(format!(
            "fixed parameter ID {:?} is not recognized",
            char::from(flag)
        ));
    }
    LineEstimate::new(
        LineKind::Gaussian,
        values,
        ["p", "c", "f"].map(|flag| flags.contains(flag)),
    )
    .map_err(|error| error.to_string())
}

pub(super) fn automatic(profile: &[f32], maximum: usize) -> Vec<LineEstimate> {
    if profile.len() < 3 || maximum == 0 {
        return Vec::new();
    }
    // ProfileFit1D uses five neighboring channels on each side for its
    // smoothed second derivative.
    const HALF_WINDOW: isize = 5;
    const DERIVATIVE_SCALE: f64 = 90. / (5. * 6. * 99. * 13.);
    let derivative: Vec<_> = (0..profile.len())
        .map(|index| {
            let (sum, moment) = (-HALF_WINDOW..=HALF_WINDOW)
                .filter_map(|delta| {
                    index
                        .checked_add_signed(delta)
                        .and_then(|channel| profile.get(channel))
                        .map(|&sample| (f64::from(sample), delta))
                })
                .fold((0., 0.), |(sum, moment), (sample, delta)| {
                    (sum + sample, moment + sample * delta as f64 * delta as f64)
                });
            DERIVATIVE_SCALE * (moment - 10. * sum)
        })
        .collect();
    let mut last_peak = None;
    let mut estimates = Vec::new();
    for (index, &value) in derivative.iter().enumerate() {
        if value <= 0.
            || (index > 0 && derivative[index - 1] >= value)
            || (index + 1 < profile.len() && derivative[index + 1] >= value)
        {
            continue;
        }
        if let Some(previous) = last_peak.replace(index)
            && let Some(values) = estimate_between(profile, &derivative, previous, index)
        {
            estimates.push(LineEstimate {
                kind: LineKind::Gaussian,
                values,
                fixed: [false; 3],
            });
        }
    }
    estimates.sort_by(|a, b| b.values[0].total_cmp(&a.values[0]));
    estimates.truncate(maximum);
    estimates
}

fn estimate_between(
    data: &[f32],
    derivative: &[f64],
    lower: usize,
    upper: usize,
) -> Option<[f64; 3]> {
    let intercept = derivative[lower];
    let slope = (derivative[upper] - intercept) / (upper - lower) as f64;
    let mut moment = [0.; 3];
    let mut negative = 0.;
    for (offset, &value) in derivative[lower..=upper].iter().enumerate() {
        negative += value.min(0.);
        let weight = value - slope * offset as f64 - intercept;
        let pixel = (lower + offset) as f64;
        moment[0] += weight;
        moment[1] += weight * pixel;
        moment[2] += weight * pixel * pixel;
    }
    let [m0, m1, m2] = moment;
    let determinant = m2 * m0 - m1 * m1;
    if determinant <= 0. || negative.abs() <= f64::from(f32::EPSILON) {
        return None;
    }
    let center = m1 / m0;
    let sigma = 1.69 * determinant.sqrt() / m0.abs();
    if !sigma.is_finite() || sigma <= 0. || !(0. ..data.len() as f64).contains(&center) {
        return None;
    }
    let offset = (1.73 * sigma + 0.5) as usize;
    let middle = (center + 0.5) as usize;
    let sample = |index: Option<usize>| {
        index
            .and_then(|i| data.get(i))
            .copied()
            .map_or(0., f64::from)
    };
    let peak = f64::from(*data.get(middle)?);
    let shoulders = 0.5 * (sample(middle.checked_sub(offset)) + sample(middle.checked_add(offset)));
    let denominator = 1. - (-0.5 * (offset as f64 / sigma).powi(2)).exp();
    let height = if denominator > 0. {
        ((peak - shoulders) / denominator).min(peak)
    } else {
        0.
    };
    (height > 0.).then_some([height, center, sigma * (8. * LN_2).sqrt()])
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
