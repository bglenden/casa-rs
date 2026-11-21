// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::LN_2;

use casa_numerics::solve_symmetric_ldlt_casacore;

use crate::{GaussianBeam, ImageError};

use super::{GaussianSamples, PlaneConvolver, unwrap_position_angle};

/// The numerical peak and fitted physical FWHM of two convolved beam stamps.
pub(crate) fn sampled_beam_convolution(
    input: GaussianSamples,
    kernel: GaussianSamples,
    sky: [[f64; 2]; 2],
) -> Result<(f64, GaussianBeam), ImageError> {
    // The fit is over the convolution kernel's footprint, as in Fit2D.
    let plane = [kernel.side; 2];
    if input.side.max(kernel.side) > 513 {
        return Err(ImageError::InvalidMetadata(
            "Gaussian beam fit exceeds the sample workspace".into(),
        ));
    }
    let mut convolution = PlaneConvolver::new(plane, input.side, &input.collect()?)?;
    let stamp = convolution.convolve(&kernel.collect()?)?;
    let peak = stamp.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    if !peak.is_finite() || peak <= 0. {
        return Err(failed());
    }
    let center = kernel.side / 2;
    let samples: Vec<_> = stamp
        .into_iter()
        .enumerate()
        .filter(|&(_, value)| value >= peak / 10.)
        .map(|(index, value)| {
            let x = (index % kernel.side) as f64 - center as f64;
            let y = (index / kernel.side) as f64 - center as f64;
            let extent = center.max(1) as f64;
            let (x, y) = (x / extent, y / extent);
            ([1., x * x, y * y, x * y], value / peak)
        })
        .collect();
    let fit = fit_profile(&samples).ok_or_else(failed)?;
    let pixel = [[-fit[1], -fit[3] / 2.], [-fit[3] / 2., -fit[2]]];
    let projection = inverse(sky).ok_or_else(failed)?;
    let q = |i: usize, j: usize| {
        (0..2)
            .map(|x| {
                (0..2)
                    .map(|y| projection[x][i] * pixel[x][y] * projection[y][j])
                    .sum::<f64>()
            })
            .sum::<f64>()
    };
    let (a, b, cross) = (q(0, 0), q(1, 1), q(0, 1));
    let separation = (a - b).hypot(2. * cross);
    let large = (a + b + separation) / 2.;
    let small = (a * b - cross * cross) / large;
    if !(small > 0. && large.is_finite()) {
        return Err(failed());
    }
    let scale = 2. * (center.max(1) as f64) * LN_2.sqrt();
    let pa = 0.5 * (-2. * cross).atan2(a - b);
    let physical = GaussianBeam::new(
        scale / small.sqrt(),
        scale / large.sqrt(),
        unwrap_position_angle(pa),
    );
    Ok((peak, physical))
}

fn failed() -> ImageError {
    ImageError::CasaSelection("Failed to fit the output beam".into())
}

fn inverse(matrix: [[f64; 2]; 2]) -> Option<[[f64; 2]; 2]> {
    let [[a, b], [c, d]] = matrix;
    let det = a * d - b * c;
    (det.is_finite() && det != 0.).then(|| [[d / det, -b / det], [-c / det, a / det]])
}

type ProfileLinearization<'a> = dyn Fn(&[f64; 4], f64) -> ([f64; 4], f64) + 'a;

fn fit_profile(points: &[([f64; 4], f64)]) -> Option<[f64; 4]> {
    let normal = |terms: &ProfileLinearization<'_>| {
        let mut matrix = [[0.; 4]; 4];
        let mut rhs = [0.; 4];
        for (row, value) in points {
            let (slope, residual) = terms(row, *value);
            for i in 0..4 {
                rhs[i] += slope[i] * residual;
                for j in i..4 {
                    matrix[i][j] += slope[i] * slope[j];
                }
            }
        }
        (matrix, rhs)
    };
    let (matrix, rhs) = normal(&|row, value| (row.map(|term| term * value), value * value.ln()));
    let mut fit = solve_symmetric_ldlt_casacore(matrix, rhs)?;
    let model = |terms: &[f64; 4], coefficients: &[f64; 4]| {
        terms
            .iter()
            .zip(coefficients)
            .map(|(a, b)| a * b)
            .sum::<f64>()
            .exp()
    };
    let residual = |coefficients: &[f64; 4]| {
        points
            .iter()
            .map(|(row, value)| (value - model(row, coefficients)).powi(2))
            .sum::<f64>()
    };
    let mut cost = residual(&fit);
    let mut damping = 1e-6;
    for _ in 0..30 {
        let (mut matrix, rhs) = normal(&|row, value| {
            let modeled = model(row, &fit);
            (row.map(|term| term * modeled), value - modeled)
        });
        for (i, row) in matrix.iter_mut().enumerate() {
            row[i] *= 1. + damping;
        }
        let step = solve_symmetric_ldlt_casacore(matrix, rhs)?;
        let next = std::array::from_fn(|i| fit[i] + step[i]);
        let next_cost = residual(&next);
        if next_cost < cost {
            fit = next;
            if (cost - next_cost) < 1e-15 {
                break;
            }
            cost = next_cost;
            damping *= 0.25;
        } else {
            damping *= 10.;
        }
    }
    fit.iter().all(|value| value.is_finite()).then_some(fit)
}
