// SPDX-License-Identifier: LGPL-3.0-or-later
//! Piecewise cubic models with continuous first and second derivatives.

use super::linear_fit::{Fit, LinearFit};

pub(super) const INVALID_FIT_ARGUMENT: &str =
    "sakura_LSQFitCubicSplineFloat() failed -- InvalidArgument";

/// Choose the knots at the same quantiles of the original fit mask as
/// libsakura. Clipping does not move the knots during a refit.
fn boundaries(mask: &[bool], pieces: usize) -> Vec<usize> {
    let valid = mask
        .iter()
        .enumerate()
        .filter_map(|(channel, &included)| included.then_some(channel))
        .collect::<Vec<_>>();
    std::iter::once(0)
        .chain((1..pieces).map(|piece| {
            let index = (valid.len() as u128 * piece as u128).div_ceil(pieces as u128);
            valid[index as usize]
        }))
        .chain(std::iter::once(mask.len()))
        .collect()
}

pub(super) fn fit(
    spectrum: &[f32],
    included: &[bool],
    pieces: usize,
    clip_sigma: f64,
    max_fits: usize,
) -> Result<Option<Fit>, String> {
    let terms = pieces.checked_add(3).ok_or("Too many spline pieces.")?;
    if terms > spectrum.len() {
        return Err("sakura_CreateLSQFitContextFloat() failed -- InvalidArgument".into());
    }
    if included.iter().filter(|&&valid| valid).count() < terms {
        return Ok(None);
    }
    let boundaries = boundaries(included, pieces);
    let scale = (spectrum.len() - 1) as f64;
    let knots = boundaries[1..pieces]
        .iter()
        .map(|&index| index as f64 / scale)
        .collect::<Vec<_>>();
    // The truncated-power basis ensures continuity at every internal knot.
    let basis = (0..spectrum.len())
        .map(|channel| {
            let x = channel as f64 / scale;
            let mut row = vec![1., x, x * x, x.powi(3)];
            row.extend(knots.iter().map(|knot| (x - knot).max(0.).powi(3)));
            row
        })
        .collect();
    let linear = LinearFit {
        basis,
        terms,
        segment_ends: boundaries[1..].to_vec(),
        failure: "sakura_LSQFitCubicSplineFloat() failed -- NG",
    };
    let mut fit = linear.solve(spectrum, included, clip_sigma, max_fits, None)?;
    if let Some(fit) = &mut fit {
        fit.coefficients = table_coefficients(&fit.coefficients, &knots, scale);
        fit.boundaries = boundaries;
    }
    Ok(fit)
}

fn table_coefficients(solution: &[f64], knots: &[f64], scale: f64) -> Vec<f64> {
    let mut terms = [solution[0], solution[1], solution[2], solution[3]];
    let mut coefficients = Vec::with_capacity((knots.len() + 1) * terms.len());
    let units = [1., scale, scale * scale, scale.powi(3)];
    coefficients.extend(
        terms
            .into_iter()
            .zip(units)
            .map(|(value, unit)| value / unit),
    );
    for (&knot, &delta) in knots.iter().zip(&solution[4..]) {
        terms[0] -= delta * knot.powi(3);
        terms[1] += 3. * delta * knot * knot;
        terms[2] -= 3. * delta * knot;
        terms[3] += delta;
        coefficients.extend(
            terms
                .into_iter()
                .zip(units)
                .map(|(value, unit)| value / unit),
        );
    }
    coefficients
}

pub(super) fn subtract(
    spectrum: &mut [f32],
    coefficients: &[f64],
    boundaries: &[usize],
) -> Result<(), String> {
    if boundaries.first() != Some(&0)
        || boundaries.last() != Some(&spectrum.len())
        || boundaries.windows(2).any(|pair| pair[0] >= pair[1])
        || coefficients.len() != 4 * boundaries.len().saturating_sub(1)
    {
        return Err("Invalid boundaries or coefficients in cubic-spline baseline table.".into());
    }
    for (pair, terms) in boundaries.windows(2).zip(coefficients.as_chunks::<4>().0) {
        for (channel, sample) in spectrum[pair[0]..pair[1]].iter_mut().enumerate() {
            let x = (channel + pair[0]) as f64;
            let baseline = terms.iter().rev().fold(0., |value, term| value * x + term);
            *sample -= baseline as f32;
        }
    }
    Ok(())
}
