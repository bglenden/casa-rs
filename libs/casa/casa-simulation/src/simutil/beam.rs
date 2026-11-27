// SPDX-License-Identifier: LGPL-3.0-or-later
//! `simutil.sfBeam1d`: Gaussian primary beam convolved with the angular
//! prolate spheroid and optional pointing-spacing box, then its spline FWHM.
//! The sample grid and convolution offsets follow CASA's numerical helper.

use super::SimutilError;

/// Estimate the gridded single-dish FWHM in arcseconds. All lengths are
/// arcseconds; omitted/nonpositive sampling skips the box convolution.
/// Support below -1 means 3, while CASA's default -1 uses its literal
/// negative support width (the spheroid is even).
///
/// # Errors
/// Invalid beam/cell, zero support, an empty sampled kernel, or a profile
/// too large to represent within this small numerical diagnostic.
pub fn spheroidal_beam(
    beam: f64,
    cell: f64,
    support: i64,
    sampling: Option<f64>,
) -> Result<f64, SimutilError> {
    let error = |message: &str| SimutilError::Arithmetic(message.to_string());
    if !beam.is_finite() || beam <= 0.0 || !cell.is_finite() || cell <= 0.0 {
        return Err(error("beam and cell must be positive finite angles"));
    }
    let support = if support < -1 { 3 } else { support };
    if support == 0 {
        return Err(error("max() iterable argument is empty"));
    }
    let count = ((6.0 * beam + 0.1) / 0.2).ceil();
    if !(4.0..=1_000_000.0).contains(&count) || support > 1000 {
        return Err(error("beam profile is empty or too large"));
    }
    // numpy.arange multiplies by the rounded (start + step) - start.
    // Its tiny offset determines which strict kernel-edge samples enter.
    let start = -3.0 * beam;
    let step = (start + 0.2) - start;
    let axis: Vec<f64> = (0..count as usize)
        .map(|i| start + i as f64 * step)
        .collect();
    let c = ((2.0 * support as f64 + 1.0) * std::f64::consts::FRAC_PI_2).abs();
    // SciPy's pro_ang1_cv returns the constant order-zero profile for
    // CASA's literal negative-c default, rather than the positive-c PSWF.
    let coefficients = if support == -1 {
        vec![2.0_f64.sqrt()]
    } else {
        spheroid_coefficients(c)
    };
    let radius = (support as f64 * cell).abs();
    let kernel: Vec<f64> = axis
        .iter()
        .map(|x| {
            let u = x / radius;
            if u.abs() < 1.0 {
                spheroid(&coefficients, u)
            } else {
                0.0
            }
        })
        .collect();
    if !kernel.iter().any(|value| *value > 0.0) {
        return Err(error("max() iterable argument is empty"));
    }
    let scale = 0.5 / (beam / 2.354_820_1).powi(2);
    let gaussian: Vec<f64> = axis.iter().map(|x| (-scale * x * x).exp()).collect();
    let mut result = convolve(&gaussian, &kernel);
    if let Some(width) = sampling.filter(|width| *width > 0.0) {
        let boxcar: Vec<f64> = axis
            .iter()
            .map(|x| f64::from(u8::from(x.abs() < width / 2.0)))
            .collect();
        result = convolve(&result, &boxcar);
    }
    let peak = result.iter().copied().fold(0.0, f64::max);
    if !peak.is_finite() || peak <= 0.0 {
        return Err(error("beam convolution has no positive finite peak"));
    }
    for value in &mut result {
        *value = *value / peak - 0.5;
    }
    let roots = spline_roots(&axis, &result);
    if roots.len() != 2 {
        return Err(error(
            "beam profile does not have two half-maximum crossings",
        ));
    }
    Ok((roots[1] - roots[0]).abs())
}

/// Lowest eigenfunction of -((1-x²)y')' + c²x²y, in the orthonormal even
/// Legendre basis. Multiplication by x twice makes its matrix tridiagonal.
/// Sturm bisection and inverse iteration avoid a new special-function library.
fn spheroid_coefficients(c: f64) -> Vec<f64> {
    let n = 64.max((1.5 * c + 16.0) as usize);
    let a = |l: usize| (l + 1) as f64 / (((2 * l + 1) * (2 * l + 3)) as f64).sqrt();
    let diagonal: Vec<f64> = (0..n)
        .map(|k| {
            let l = 2 * k;
            (l * (l + 1)) as f64
                + c * c * (a(l).powi(2) + if l == 0 { 0.0 } else { a(l - 1).powi(2) })
        })
        .collect();
    let off: Vec<f64> = (0..n - 1)
        .map(|k| c * c * a(2 * k) * a(2 * k + 1))
        .collect();
    let (mut low, mut high) = (0.0, diagonal[0]);
    for _ in 0..100 {
        let middle = 0.5 * (low + high);
        let mut pivot = diagonal[0] - middle;
        let mut negative = usize::from(pivot < 0.0);
        for i in 1..n {
            if pivot.abs() < 1e-280 {
                pivot = -1e-280;
            }
            pivot = diagonal[i] - middle - off[i - 1].powi(2) / pivot;
            negative += usize::from(pivot < 0.0);
        }
        if negative == 0 {
            low = middle;
        } else {
            high = middle;
        }
    }
    let shifted: Vec<f64> = diagonal.iter().map(|x| x - low + 1e-10).collect();
    let mut values = vec![1.0; n];
    for _ in 0..4 {
        values = tridiagonal(&off, &shifted, &off, &values);
        let norm = values.iter().map(|x| x * x).sum::<f64>().sqrt();
        for value in &mut values {
            *value /= norm;
        }
    }
    let centre = spheroid(&values, 0.0);
    for value in &mut values {
        *value /= centre;
    }
    values
}

fn spheroid(coefficients: &[f64], x: f64) -> f64 {
    let (mut previous, mut current) = (1.0, x);
    let mut sum = coefficients[0] / 2.0_f64.sqrt();
    for l in 2..2 * coefficients.len() {
        let next = ((2 * l - 1) as f64 * x * current - (l - 1) as f64 * previous) / l as f64;
        if l % 2 == 0 {
            sum += coefficients[l / 2] * ((2 * l + 1) as f64 / 2.0).sqrt() * next;
        }
        (previous, current) = (current, next);
    }
    sum
}

fn tridiagonal(lower: &[f64], diagonal: &[f64], upper: &[f64], right: &[f64]) -> Vec<f64> {
    let mut d = diagonal.to_vec();
    let mut result = right.to_vec();
    for i in 1..d.len() {
        let factor = lower[i - 1] / d[i - 1];
        d[i] -= factor * upper[i - 1];
        result[i] -= factor * result[i - 1];
    }
    let last = d.len() - 1;
    result[last] /= d[last];
    for i in (0..last).rev() {
        result[i] = (result[i] - upper[i] * result[i + 1]) / d[i];
    }
    result
}

fn convolve(signal: &[f64], kernel: &[f64]) -> Vec<f64> {
    let n = signal.len();
    let shift = (n - 1) / 2;
    let mut result = vec![0.0; n];
    for (k, weight) in kernel
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, weight)| *weight != 0.0)
    {
        for (i, value) in result.iter_mut().enumerate() {
            if let Some(j) = (i + shift).checked_sub(k).filter(|j| *j < n) {
                *value += weight * signal[j];
            }
        }
    }
    result
}

/// Not-a-knot cubic, like scipy.interpolate.UnivariateSpline(s=0), on
/// CASA's uniform grid. Eliminate the two endpoint second derivatives.
fn spline_roots(x: &[f64], y: &[f64]) -> Vec<f64> {
    let n = y.len();
    let h = x[1] - x[0];
    let mut diagonal = vec![4.0; n - 2];
    diagonal[0] = 6.0;
    diagonal[n - 3] = 6.0;
    let mut lower = vec![1.0; n - 3];
    let mut upper = lower.clone();
    lower[n - 4] = 0.0;
    upper[0] = 0.0;
    let right: Vec<f64> = y
        .windows(3)
        .map(|v| 6.0 * (v[0] - 2.0 * v[1] + v[2]) / (h * h))
        .collect();
    let interior = tridiagonal(&lower, &diagonal, &upper, &right);
    let mut second = vec![2.0 * interior[0] - interior[1]];
    second.extend_from_slice(&interior);
    second.push(2.0 * interior[n - 3] - interior[n - 4]);
    let mut roots = Vec::new();
    for i in 0..n - 1 {
        if y[i] * y[i + 1] > 0.0 {
            continue;
        }
        let value = |t: f64| {
            let u = 1.0 - t;
            u * y[i]
                + t * y[i + 1]
                + h * h / 6.0 * ((u.powi(3) - u) * second[i] + (t.powi(3) - t) * second[i + 1])
        };
        let (mut low, mut high) = (0.0, 1.0);
        for _ in 0..50 {
            let middle = 0.5 * (low + high);
            if value(middle) * y[i] > 0.0 {
                low = middle;
            } else {
                high = middle;
            }
        }
        roots.push(x[i] + h * 0.5 * (low + high));
    }
    roots
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn casa_gaussian_spheroid_and_sampling_widths() {
        // CASA 6.7.6.14, with only its NumPy-2 scalar extraction corrected.
        for (beam, cell, support, sampling, expected) in [
            (20.0, 3.0, -2, Some(5.0), 21.213_463_170_477_02),
            (20.0, 3.0, -1, Some(5.0), 20.703_862_739_960_563),
            (20.0, 3.0, 1, Some(5.0), 20.505_777_771_059_783),
            (20.0, 3.0, 3, Some(5.0), 21.213_463_170_477_02),
            (20.0, 3.0, 5, Some(5.0), 21.940_147_757_150_73),
            (60.0, 8.0, 3, None, 62.226_853_768_511_37),
            (18.0, 0.5, 4, Some(7.2), 18.715_734_226_018_41),
        ] {
            let actual = spheroidal_beam(beam, cell, support, sampling).expect("beam");
            assert!(
                (actual / expected - 1.0).abs() < 1e-9,
                "{support}: {actual} vs {expected}"
            );
        }
    }
}
