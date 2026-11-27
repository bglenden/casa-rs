// SPDX-License-Identifier: LGPL-3.0-or-later

use std::f64::consts::{LN_2, PI};

use casa_numerics::solve_symmetric_ldlt_casacore;

use casa_images::{GaussianBeam, ImageError};

use super::{GaussianSamples, PlaneConvolver, lattice_gaussian_volume, unwrap_position_angle};

/// The numerical peak and fitted physical FWHM of two convolved beam stamps.
pub(crate) fn sampled_beam_convolution(
    input: GaussianSamples,
    kernel: GaussianSamples,
    sky: [[f64; 2]; 2],
) -> Result<(f64, GaussianBeam), ImageError> {
    // The fit is over the convolution kernel's footprint, as in Fit2D.
    let plane = [kernel.side; 2];
    if input.side.max(kernel.side) > 513 {
        return wide_beam_convolution(input, kernel, sky);
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
    let physical = beam_from_fit(&samples, [center.max(1) as f64; 2], sky)?;
    Ok((peak, physical))
}

fn beam_from_fit(
    samples: &[([f64; 4], f64)],
    scale: [f64; 2],
    sky: [[f64; 2]; 2],
) -> Result<GaussianBeam, ImageError> {
    let fit = fit_profile(samples).ok_or_else(failed)?;
    let cross = -fit[3] / (2. * scale[0] * scale[1]);
    let pixel = [
        [-fit[1] / scale[0].powi(2), cross],
        [cross, -fit[2] / scale[1].powi(2)],
    ];
    projected_beam(pixel, sky)
}

fn projected_beam(pixel: [[f64; 2]; 2], sky: [[f64; 2]; 2]) -> Result<GaussianBeam, ImageError> {
    let projection = inverse(sky).ok_or_else(failed)?;
    let q = |i: usize, j: usize| {
        let left = [projection[0][i], projection[1][i]];
        let right = [projection[0][j], projection[1][j]];
        dot(left, apply(pixel, right))
    };
    let (a, b, cross) = (q(0, 0), q(1, 1), q(0, 1));
    let separation = (a - b).hypot(2. * cross);
    let large = (a + b + separation) / 2.;
    let small = (a * b - cross * cross) / large;
    if !(small > 0. && large.is_finite()) {
        return Err(failed());
    }
    let scale = 2. * LN_2.sqrt();
    let pa = 0.5 * (-2. * cross).atan2(a - b);
    Ok(GaussianBeam::new(
        scale / small.sqrt(),
        scale / large.sqrt(),
        unwrap_position_angle(pa),
    ))
}

fn wide_beam_convolution(
    input: GaussianSamples,
    kernel: GaussianSamples,
    sky: [[f64; 2]; 2],
) -> Result<(f64, GaussianBeam), ImageError> {
    let a = input.precision();
    let b = kernel.precision();
    let add = |left: [[f64; 2]; 2], right: [[f64; 2]; 2]| {
        std::array::from_fn(|i| std::array::from_fn(|j| left[i][j] + right[i][j]))
    };
    let precision = add(a, b);
    let combined = add(
        inverse(a).ok_or_else(failed)?,
        inverse(b).ok_or_else(failed)?,
    );
    let determinant = det(precision);
    if input.well_sampled() && kernel.well_sampled() {
        // The lattice samples resolve both beams. The continuous Gaussian
        // convolution avoids a large FFT with negligible tail truncation.
        let peak = PI / (4. * LN_2 * determinant.sqrt());
        let fitted = inverse(combined)
            .ok_or_else(failed)?
            .map(|row| row.map(|value| value * 4. * LN_2));
        return Ok((peak, projected_beam(fitted, sky)?));
    }

    // For a wide, undersampled stamp, evaluate a bounded grid of the actual
    // discrete convolution by completing the square in each lattice sum.
    let inverse_precision = inverse(precision).ok_or_else(failed)?;
    let input_mid = (input.side / 2) as isize;
    let kernel_mid = (kernel.side / 2) as isize;
    let value = |position: [isize; 2]| {
        let output = position.map(|at| at as f64);
        let mean = apply(inverse_precision, apply(b, output));
        let shifted = [output[0] - mean[0], output[1] - mean[1]];
        let envelope =
            (-4. * LN_2 * (dot(mean, apply(a, mean)) + dot(shifted, apply(b, shifted)))).exp();
        let bounds = position.map(|at| {
            [
                (-input_mid).max(at - kernel_mid),
                input_mid.min(at + kernel_mid),
            ]
        });
        envelope * lattice_gaussian_volume(precision, determinant, mean, bounds)
    };
    let peak = value([0, 0]);
    if !peak.is_finite() || peak <= 0. {
        return Err(failed());
    }
    let scale = [combined[0][0].sqrt().max(1.), combined[1][1].sqrt().max(1.)];
    let positions = |extent: f64| {
        let radius = ((extent * 0.95).ceil() as usize).min(kernel_mid as usize);
        let step = radius.div_ceil(24).max(1);
        let positive = (step..=radius).step_by(step).collect::<Vec<_>>();
        positive
            .iter()
            .rev()
            .map(|&at| -(at as isize))
            .chain(std::iter::once(0))
            .chain(positive.iter().map(|&at| at as isize))
            .collect::<Vec<_>>()
    };
    let mut samples = Vec::new();
    let xs = positions(scale[0]);
    for y in positions(scale[1]) {
        for &x in &xs {
            let value = value([x, y]) / peak;
            if value >= 0.1 {
                let (x, y) = (x as f64 / scale[0], y as f64 / scale[1]);
                samples.push(([1., x * x, y * y, x * y], value));
            }
        }
    }
    Ok((peak, beam_from_fit(&samples, scale, sky)?))
}

fn dot(a: [f64; 2], b: [f64; 2]) -> f64 {
    a[0] * b[0] + a[1] * b[1]
}

fn apply(matrix: [[f64; 2]; 2], vector: [f64; 2]) -> [f64; 2] {
    matrix.map(|row| dot(row, vector))
}

fn det([[a, b], [c, d]]: [[f64; 2]; 2]) -> f64 {
    a * d - b * c
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
