// SPDX-License-Identifier: LGPL-3.0-or-later
//! The restoring beam `tsdimaging` gives its image: CASA's
//! `sdbeamutil.TheoreticalBeam`, a one-dimensional model of the antenna's
//! beam smeared by the gridding kernel and by the raster sampling.
//!
//! 1. The antenna's beam is a Gaussian whose full width is Baars' formula
//!    for a 10 dB edge taper (Baars 2007, eq. 4.13) times Schroeder's
//!    central-obstruction factor (*Astronomical Optics*, table 10.1),
//!    `b λ/D`.
//! 2. It is convolved with a cut through the gridding kernel and normalised
//!    to a peak of 1, then convolved with a box as wide as the sampling
//!    interval and normalised again.
//! 3. The beam is the full width of the Gaussian, centred and of peak 1,
//!    fitted to that by least squares. With two sampling intervals (or a
//!    `BOX` kernel on non-square pixels), the second axis is fitted too and
//!    the beam is the geometric mean of the two widths.
//!
//! The profiles are sampled every 0.02″, coarser for a wide beam or fine
//! sampling, as CASA samples them, and convolved as `numpy.convolve(…,
//! mode='same')` convolves them.
//!
//! One departure (`docs/CASA (C++) bugs.md`): CASA convolves with its own
//! models of the kernels, not the kernels the gridder uses. Its `GAUSS`
//! takes `gwidth` as a full width where the gridder takes a half width,
//! and truncates at 1.5 `gwidth` where the gridder truncates at
//! `truncate`; its `GJINC` ignores `truncate`; its `SF` is SciPy's prolate
//! spheroid of `c = 5.356π/2`, which approximates the gridder's; its `PB`
//! is an Airy pattern scaled to the beam of step 1, where the gridder
//! takes the telescope's pattern; and its `BOX` is one pixel wide whatever
//! `convsupport` is. casa-rs cuts through the gridder's own table
//! ([`KernelTable`]), so the beam describes the image the gridder makes;
//! a `BOX` of support `s` is `2s + 1` pixels wide.

use crate::kernel::{GridFunction, KernelTable, SAMPLING};

/// `primaryBeamArcsec`: the full width at half maximum, arcsec, of the beam
/// of a dish of `diameter_m` with a central obscuration of `obscuration_m`
/// and an edge taper of `taper_db`, at `frequency_hz`.
#[must_use]
pub fn primary_beam_fwhm_arcsec(
    frequency_hz: f64,
    diameter_m: f64,
    obscuration_m: f64,
    taper_db: f64,
) -> f64 {
    let wavelength_m = 2.997_924_58e8 / frequency_hz;
    let factor =
        baars_taper_factor(taper_db.abs()) * central_obstruction_factor(obscuration_m / diameter_m);
    factor * wavelength_m * 3600.0 * 180.0 / (diameter_m * std::f64::consts::PI)
}

/// `baarsTaperFactor` (Baars 2007, eq. 4.13): the constant `b` in
/// `b λ/D` for a parabolic illumination with an edge taper of `taper_db`.
fn baars_taper_factor(taper_db: f64) -> f64 {
    let tau = 10_f64.powf(-0.05 * taper_db);
    1.269 - 0.566 * tau + 0.534 * tau * tau - 0.208 * tau * tau * tau
}

/// `centralObstructionFactor`: Schroeder's table 10.1 of an Airy pattern's
/// width against the obscuration ratio `epsilon`, relative to the
/// unobstructed width, interpolated as SciPy's `UnivariateSpline(s=0)`
/// interpolates it: the not-a-knot cubic spline through the five points,
/// whose one interior knot is at 0.2.
fn central_obstruction_factor(epsilon: f64) -> f64 {
    let (low, high) = schroeder_spline();
    let [a, b, c, d] = if epsilon <= 0.2 { low } else { high };
    let x = epsilon - 0.2;
    (a + x * (b + x * (c + x * d))) / 1.22
}

/// Schroeder's points: obscuration ratio and relative width (× 1.22).
const SCHROEDER: [(f64, f64); 5] = [
    (0.0, 1.22),
    (0.1, 1.205),
    (0.2, 1.167),
    (0.33, 1.098),
    (0.4, 1.058),
];

/// The not-a-knot cubic through [`SCHROEDER`], as two cubics in powers of
/// `ε − 0.2`, below and above the knot at 0.2, that agree there in value,
/// slope and curvature: on each side `y = y₂ + s x + (c/2) x² + dᵢ x³`,
/// with the shared slope `s` and curvature `c`, and each side's `dᵢ` fixed
/// by its two other points.
fn schroeder_spline() -> ([f64; 4], [f64; 4]) {
    let y2 = SCHROEDER[2].1;
    // Each other point gives `s x + (c/2) x² + d x³ = y − y₂`; the two on a
    // side share `d`, so eliminating it leaves one equation in `s` and `c`
    // per side.
    let side = |i: usize, j: usize| {
        let (xi, xj) = (SCHROEDER[i].0 - 0.2, SCHROEDER[j].0 - 0.2);
        let (ri, rj) = (
            (SCHROEDER[i].1 - y2) / (xi * xi * xi),
            (SCHROEDER[j].1 - y2) / (xj * xj * xj),
        );
        // s (1/xi² − 1/xj²) + (c/2) (1/xi − 1/xj) = ri − rj
        [
            1.0 / (xi * xi) - 1.0 / (xj * xj),
            0.5 * (1.0 / xi - 1.0 / xj),
            ri - rj,
        ]
    };
    let low = side(0, 1);
    let high = side(3, 4);
    let determinant = low[0] * high[1] - low[1] * high[0];
    let slope = (low[2] * high[1] - low[1] * high[2]) / determinant;
    let curvature = (low[0] * high[2] - low[2] * high[0]) / determinant;
    let cube = |i: usize| {
        let x = SCHROEDER[i].0 - 0.2;
        (SCHROEDER[i].1 - y2 - slope * x - 0.5 * curvature * x * x) / (x * x * x)
    };
    (
        [y2, slope, 0.5 * curvature, cube(0)],
        [y2, slope, 0.5 * curvature, cube(4)],
    )
}

/// What the theoretical beam is computed from.
#[derive(Debug, Clone, Copy)]
pub struct BeamSetup<'a> {
    /// The antenna's diameter, m.
    pub diameter_m: f64,
    /// Its central obscuration's diameter, m (`tsdimaging`: 0.75 m for
    /// ALMA's antennas, else 0).
    pub obscuration_m: f64,
    /// The frequency, Hz (`tsdimaging`: the rest frequency).
    pub frequency_hz: f64,
    /// The image's cell along each axis, arcsec, unsigned.
    pub cell_arcsec: [f64; 2],
    /// The kernel the image is gridded with, and its table.
    pub kernel: (GridFunction, &'a KernelTable),
    /// The sampling intervals, arcsec ([`crate::beam_sampling`]): one, or
    /// along and across the scan.
    pub sampling_arcsec: &'a [f64],
}

/// `TheoreticalBeam.get_beamsize_image`: the beam's full width at half
/// maximum, arcsec, which `tsdimaging` sets as a circular restoring beam.
#[must_use]
pub fn theoretical_beam_arcsec(setup: &BeamSetup<'_>) -> f64 {
    let fwhm = primary_beam_fwhm_arcsec(
        setup.frequency_hz,
        setup.diameter_m,
        setup.obscuration_m,
        10.0,
    );
    let step = profile_step(fwhm, setup.cell_arcsec, setup.sampling_arcsec);
    let axis = arange(-3.0 * fwhm, 3.0 * fwhm + 0.5 * step, step);
    let sigma = fwhm / 2.354_820_1;
    let beam: Vec<f64> = axis
        .iter()
        .map(|x| (-(x * x) / (2.0 * sigma * sigma)).exp())
        .collect();
    let (function, table) = setup.kernel;
    let cut = |cell: f64| kernel_cut(&axis, function, table, cell);
    let mut gridded = convolve_same(&beam, &cut(setup.cell_arcsec[0]));
    normalise(&mut gridded);
    let along = box_of(&axis, setup.sampling_arcsec[0]);
    let mut result = convolve_same(&gridded, &along);
    normalise(&mut result);
    let width = gaussian_fit(&axis, &result);
    let box_kernel = matches!(function, GridFunction::Box { .. });
    let square = setup.cell_arcsec[0] == setup.cell_arcsec[1];
    if setup.sampling_arcsec.len() == 1 && (square || !box_kernel) {
        return width;
    }
    // The second axis.
    let across = match setup.sampling_arcsec.get(1) {
        Some(&interval) => box_of(&axis, interval),
        None => along,
    };
    if setup.sampling_arcsec.len() == 1 {
        gridded = convolve_same(&beam, &cut(setup.cell_arcsec[1]));
        normalise(&mut gridded);
    }
    let mut result = convolve_same(&gridded, &across);
    normalise(&mut result);
    (width * gaussian_fit(&axis, &result)).sqrt()
}

/// `get_antenna_beam`'s sample spacing, arcsec: 0.02″, finer for sampling
/// below 0.1″, coarser when the smallest of the cells, the beam and the
/// sampling exceeds 1,000 samples, or the beam 5,000.
fn profile_step(fwhm: f64, cell: [f64; 2], sampling: &[f64]) -> f64 {
    let mut step = 0.02;
    let finest = sampling.iter().copied().fold(f64::INFINITY, f64::min);
    if finest < 5.0 * step {
        step = finest / 5.0;
    }
    let smallest = cell.iter().chain(sampling).copied().fold(fwhm, f64::min);
    if smallest > 1000.0 * step {
        step = smallest / 1000.0;
    }
    if fwhm > 5000.0 * step {
        step = (fwhm / 5000.0).min(smallest / 10.0);
    }
    step
}

/// The kernel along `axis` (arcsec) on pixels of `cell` arcsec: the table at
/// each radius truncated to a hundredth of a pixel, as the gridder reads
/// it, and for `BOX` the `2s + 1` pixels a spectrum reaches.
fn kernel_cut(axis: &[f64], function: GridFunction, table: &KernelTable, cell: f64) -> Vec<f64> {
    axis.iter()
        .map(|x| {
            let pixels = x.abs() / cell;
            if let GridFunction::Box { support } = function {
                return f64::from(u8::from(pixels <= support as f64 + 0.5));
            }
            let index = (pixels * SAMPLING as f64) as usize;
            f64::from(table.values().get(index).copied().unwrap_or(0.0))
        })
        .collect()
}

/// `get_box_kernel`: 1 where `|x| ≤ width/2`.
fn box_of(axis: &[f64], width: f64) -> Vec<f64> {
    axis.iter()
        .map(|x| f64::from(u8::from(x.abs() <= width / 2.0)))
        .collect()
}

/// `numpy.arange(start, stop, step)`.
fn arange(start: f64, stop: f64, step: f64) -> Vec<f64> {
    let count = ((stop - start) / step).ceil().max(0.0) as usize;
    (0..count).map(|i| start + i as f64 * step).collect()
}

/// `numpy.convolve(signal, kernel, mode='same')` of two profiles of one
/// length `n`: the full convolution's samples from `(n − 1)/2`. Only the
/// kernel's nonzero samples are visited.
fn convolve_same(signal: &[f64], kernel: &[f64]) -> Vec<f64> {
    let n = signal.len();
    let shift = (n - 1) / 2;
    let taps: Vec<(usize, f64)> = kernel
        .iter()
        .copied()
        .enumerate()
        .filter(|&(_, value)| value != 0.0)
        .collect();
    (0..n)
        .map(|i| {
            // `full[i + shift] = Σⱼ signal[j] kernel[i + shift − j]`.
            taps.iter()
                .filter_map(|&(k, value)| {
                    (i + shift)
                        .checked_sub(k)
                        .and_then(|j| signal.get(j))
                        .map(|s| s * value)
                })
                .sum()
        })
        .collect()
}

/// Divide `profile` by its largest value.
fn normalise(profile: &mut [f64]) {
    let peak = profile.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    profile.iter_mut().for_each(|value| *value /= peak);
}

/// `gaussfit`: the full width of the Gaussian of peak 1 centred at 0 that
/// fits `profile` on `axis` by least squares (`scipy.optimize.leastsq`
/// from the half-power width), here by Gauss–Newton iteration to
/// convergence.
fn gaussian_fit(axis: &[f64], profile: &[f64]) -> f64 {
    let mut width = half_power_width(axis, profile);
    for _ in 0..100 {
        let sigma = width / 2.354_820_1;
        // r = y − g, ∂g/∂w = g x² / (σ² w).
        let (mut gradient, mut curvature) = (0.0, 0.0);
        for (x, y) in axis.iter().zip(profile) {
            let g = (-(x * x) / (2.0 * sigma * sigma)).exp();
            let slope = g * x * x / (sigma * sigma * width);
            gradient += (y - g) * slope;
            curvature += slope * slope;
        }
        if curvature == 0.0 {
            break;
        }
        let change = gradient / curvature;
        width += change;
        if change.abs() <= 1.0e-14 * width {
            break;
        }
    }
    width
}

/// The width at half maximum of a profile with one peak, interpolated
/// linearly between samples (or the axis's extent where the profile does
/// not fall to half): the fit's starting point (`findFWHM`).
fn half_power_width(axis: &[f64], profile: &[f64]) -> f64 {
    let centre = profile.iter().enumerate().fold(
        0,
        |best, (i, value)| if *value > profile[best] { i } else { best },
    );
    let half = 0.5 * profile[centre];
    let crossing = |outward: &mut dyn Iterator<Item = usize>| {
        let mut previous = centre;
        for i in outward {
            if profile[i] <= half {
                let t = (profile[previous] - half) / (profile[previous] - profile[i]);
                return axis[previous] + t * (axis[i] - axis[previous]);
            }
            previous = i;
        }
        axis[previous]
    };
    let right = crossing(&mut (centre + 1..axis.len()));
    let left = crossing(&mut (0..centre).rev());
    right - left
}

#[cfg(test)]
mod tests;
