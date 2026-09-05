// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA-style restoring-beam fitting from a PSF plane.
//!
//! This follows the `StokesImageUtil::FitGaussianPSF` workflow: find the PSF
//! peak, walk the main-lobe samples above `psfcutoff`, expand a padded square
//! window, bicubically oversample it, renormalize, collect second-pass
//! samples above the cutoff, and fit an elliptical Gaussian with damped
//! Levenberg-Marquardt refinement whose damped normal equations solve in
//! casacore `LSQFit` LDLT order. Fitted values pass through casacore's
//! Float rounding before becoming beam metadata.

use casa_numerics::{
    EllipticalGaussian, common_enclosing_gaussian, deconvolving_gaussian,
    solve_symmetric_ldlt_casacore,
};
use ndarray::Array2;

use thiserror::Error;

/// Exact reason a CASA-style PSF Gaussian fit could not be completed.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0}")]
pub struct PsfBeamFitError(String);

const CASA_FWHM_TO_INTERNAL: f64 = 0.600_561_204_393_224_9;
const MAX_FIT_ATTEMPTS: usize = 50;
const MIN_SIGMA_RAD: f64 = 1.0e-12;
const TARGET_INTERPOLATED_POINTS: usize = 3001;
const PSF_PATCH_RADIUS: usize = 20;
const REGION_PADDING: usize = 5;

/// CASA's default main-lobe cutoff used by both restoring-beam fitting and
/// the fitted-Gaussian PSF sidelobe measurement.
pub const DEFAULT_PSF_FIT_CUTOFF: f32 = 0.35;

/// One fitted restoring beam in CASA metadata semantics.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RestoringBeam {
    major_fwhm_rad: f64,
    minor_fwhm_rad: f64,
    position_angle_rad: f64,
}

impl RestoringBeam {
    /// Construct a finite CASA restoring beam.
    ///
    /// The major axis must be at least the minor axis and both widths must be
    /// strictly positive. Position angle is wrapped to CASA's canonical
    /// `[-pi/2, pi/2]` interval.
    ///
    /// # Errors
    ///
    /// Returns [`PsfBeamFitError`] for non-finite, non-positive, or inverted
    /// axes.
    pub fn new(
        major_fwhm_rad: f64,
        minor_fwhm_rad: f64,
        position_angle_rad: f64,
    ) -> Result<Self, PsfBeamFitError> {
        if !major_fwhm_rad.is_finite()
            || !minor_fwhm_rad.is_finite()
            || !position_angle_rad.is_finite()
            || major_fwhm_rad <= 0.0
            || minor_fwhm_rad <= 0.0
            || major_fwhm_rad < minor_fwhm_rad
        {
            return Err(PsfBeamFitError(
                "restoring beam requires finite positive major >= minor axes".to_string(),
            ));
        }
        Ok(Self {
            major_fwhm_rad,
            minor_fwhm_rad,
            position_angle_rad: casa_wrap_beam_position_angle(position_angle_rad),
        })
    }

    /// Major full width at half maximum in radians.
    #[must_use]
    pub const fn major_fwhm_rad(&self) -> f64 {
        self.major_fwhm_rad
    }

    /// Minor full width at half maximum in radians.
    #[must_use]
    pub const fn minor_fwhm_rad(&self) -> f64 {
        self.minor_fwhm_rad
    }

    /// Position angle measured east from north in radians.
    #[must_use]
    pub const fn position_angle_rad(&self) -> f64 {
        self.position_angle_rad
    }

    /// Return the Gaussian area in steradians.
    #[must_use]
    pub fn area_sr(self) -> f64 {
        numeric_beam(self).area()
    }

    /// Compute CASA's minimum-area common enclosing restoring beam.
    ///
    /// # Errors
    ///
    /// Returns [`PsfBeamFitError`] when the set is empty or contains an
    /// invalid beam.
    pub fn common_enclosing(beams: &[Self]) -> Result<Self, PsfBeamFitError> {
        common_enclosing_gaussian(&beams.iter().copied().map(numeric_beam).collect::<Vec<_>>())
            .map(restoring_beam)
            .map_err(|error| PsfBeamFitError(error.to_string()))
    }

    /// Compute the beam that convolves `source` to this target beam.
    ///
    /// Returns `Ok(None)` when the beams are effectively identical.
    ///
    /// # Errors
    ///
    /// Returns [`PsfBeamFitError`] when either beam is invalid or the target
    /// is smaller than the source.
    pub fn deconvolving_beam(self, source: Self) -> Result<Option<Self>, PsfBeamFitError> {
        deconvolving_gaussian(numeric_beam(self), numeric_beam(source))
            .map(|beam| beam.map(restoring_beam))
            .map_err(|error| PsfBeamFitError(error.to_string()))
    }
}

fn numeric_beam(beam: RestoringBeam) -> EllipticalGaussian {
    EllipticalGaussian::new(
        beam.major_fwhm_rad,
        beam.minor_fwhm_rad,
        beam.position_angle_rad,
    )
}

fn restoring_beam(beam: EllipticalGaussian) -> RestoringBeam {
    RestoringBeam {
        major_fwhm_rad: beam.major,
        minor_fwhm_rad: beam.minor,
        position_angle_rad: beam.position_angle,
    }
}

/// Fit a restoring beam from one real PSF plane stored x-major.
///
/// The plane is indexed `[x * height + y]` like every normal-state primitive.
/// `cutoff` is the CASA `psfcutoff` fraction of the peak that bounds the main
/// lobe; failed fits retry at progressively lower cutoffs exactly like the
/// reference workflow. Pixel sizes are radians per pixel on each axis.
///
/// # Errors
///
/// Returns [`PsfBeamFitError`] when no attempt converges.
pub fn fit_restoring_beam(
    psf: &[f32],
    shape: [usize; 2],
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> Result<RestoringBeam, PsfBeamFitError> {
    let Some((peak_index, peak)) = peak_flat(psf) else {
        return Err(PsfBeamFitError("PSF is empty".to_string()));
    };
    if !(peak.is_finite() && peak > 0.0) {
        return Err(PsfBeamFitError(format!(
            "PSF peak is not positive and finite ({peak})"
        )));
    }
    let mut active_cutoff = cutoff.clamp(0.01, 0.99);
    let mut last_warning = String::new();
    for _attempt in 1..=MAX_FIT_ATTEMPTS {
        let Some((samples, _active)) =
            extract_fit_samples(psf, shape, peak_index, peak, cell_size_rad, active_cutoff)
        else {
            last_warning =
                format!("failed to isolate a main-lobe region at psfcutoff={active_cutoff:.6}");
            active_cutoff /= 1.5;
            continue;
        };
        if samples.len() < 12 {
            last_warning = format!(
                "found only {} fit samples at psfcutoff={active_cutoff:.6}",
                samples.len()
            );
            active_cutoff /= 1.5;
            continue;
        }
        if let Some(beam) = fit_gaussian_beam_casa(&samples, cell_size_rad) {
            return Ok(beam);
        }
        last_warning = format!("fit failed to converge at psfcutoff={active_cutoff:.6}");
        active_cutoff /= 1.5;
    }
    Err(PsfBeamFitError(format!(
        "beam fit failed after {MAX_FIT_ATTEMPTS} attempts starting from psfcutoff={cutoff:.3}: {last_warning}"
    )))
}

/// Measure the CASA cycle-threshold sidelobe fraction from a fitted Gaussian.
///
/// CASA defines the positive sidelobe as `max(psf - fitted_gaussian)` while
/// preserving the magnitude of the most-negative original PSF sample. The
/// Gaussian is unit peak and is scaled to the measured PSF peak before the
/// subtraction. Pixel units are sufficient here because both the fit and the
/// generated Gaussian use the same unit cell scale.
///
/// # Errors
///
/// Returns [`PsfBeamFitError`] if the Gaussian fit cannot be completed.
pub fn fitted_psf_sidelobe_fraction(
    psf: &[f32],
    shape: [usize; 2],
) -> Result<f64, PsfBeamFitError> {
    if psf.len() != shape[0].saturating_mul(shape[1]) {
        return Err(PsfBeamFitError(
            "PSF payload length does not match its shape".to_string(),
        ));
    }
    let beam = fit_restoring_beam(psf, shape, [1.0, 1.0], DEFAULT_PSF_FIT_CUTOFF)?;
    fitted_psf_sidelobe_fraction_with_beam(psf, shape, beam)
}

pub(crate) fn fitted_psf_sidelobe_fraction_with_beam(
    psf: &[f32],
    shape: [usize; 2],
    beam: RestoringBeam,
) -> Result<f64, PsfBeamFitError> {
    if psf.len() != shape[0].saturating_mul(shape[1]) {
        return Err(PsfBeamFitError(
            "PSF payload length does not match its shape".to_string(),
        ));
    }
    let Some((peak_index, peak)) = peak_flat(psf) else {
        return Err(PsfBeamFitError("PSF is empty".to_string()));
    };
    if !(peak.is_finite() && peak > 0.0) {
        return Err(PsfBeamFitError(format!(
            "PSF peak is not positive and finite ({peak})"
        )));
    }
    let peak_pixel = [peak_index / shape[1], peak_index % shape[1]];
    let sigma_major = beam.major_fwhm_rad() / 2.354_820_045_030_949_3;
    let sigma_minor = beam.minor_fwhm_rad() / 2.354_820_045_030_949_3;
    let cos_pa = beam.position_angle_rad().cos();
    let sin_pa = beam.position_angle_rad().sin();
    let mut minimum = 0.0_f32;
    let mut maximum_delobed = 0.0_f32;
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let index = x * shape[1] + y;
            let sample = psf[index];
            minimum = minimum.min(sample);
            let dx = x as f64 - peak_pixel[0] as f64;
            let dy = y as f64 - peak_pixel[1] as f64;
            let u = dx * cos_pa + dy * sin_pa;
            let v = -dx * sin_pa + dy * cos_pa;
            let gaussian = (-0.5 * ((u / sigma_minor).powi(2) + (v / sigma_major).powi(2))).exp()
                as f32
                * peak;
            maximum_delobed = maximum_delobed.max(sample - gaussian);
        }
    }
    Ok(f64::from(minimum.abs().max(maximum_delobed)) / f64::from(peak))
}

struct FitSample {
    x_rad: f64,
    y_rad: f64,
    value: f64,
}

#[derive(Debug, Clone, Copy)]
struct SampleIndex {
    x_index: usize,
    y_index: usize,
}

fn peak_flat(psf: &[f32]) -> Option<(usize, f32)> {
    let mut best: Option<(usize, f32)> = None;
    for (index, value) in psf.iter().enumerate() {
        match best {
            None => best = Some((index, *value)),
            Some((_, best_value)) if *value > best_value => best = Some((index, *value)),
            _ => {}
        }
    }
    best
}

#[derive(Debug, Clone)]
struct LobeSearchResult {
    samples: Vec<SampleIndex>,
    blc: (usize, usize),
    trc: (usize, usize),
}

/// Faithful port of `StokesImageUtil::extractCasaFitSamples`.
///
/// `cutoff` is the CASA `psfcutoff` fraction of the ACTUAL PSF peak: the
/// reference workflow normalizes the local PSF by its fitted peak
/// (`lpsf /= bamp`) before walking the main lobe, so the cutoff always
/// compares against `cutoff * peak`, never a bare sample value.
fn extract_fit_samples(
    psf: &[f32],
    shape: [usize; 2],
    peak_index: usize,
    peak: f32,
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> Option<(Vec<FitSample>, f32)> {
    let deltas = [cell_size_rad[0].abs(), cell_size_rad[1].abs()];
    let nrow = PSF_PATCH_RADIUS
        .min(shape[0].saturating_sub(1))
        .min(shape[1].saturating_sub(1));
    let first_pass = find_points_in_lobe(psf, shape, peak_index, nrow, cutoff * peak)?;

    let mut blc_x = first_pass.blc.0.saturating_sub(REGION_PADDING);
    let mut blc_y = first_pass.blc.1.saturating_sub(REGION_PADDING);
    let mut trc_x = (first_pass.trc.0 + REGION_PADDING).min(shape[0] - 1);
    let mut trc_y = (first_pass.trc.1 + REGION_PADDING).min(shape[1] - 1);

    // Match CASA FindNpoints(): force the selected bounding box square before
    // the expanded resampling window is derived from it.
    let side = (trc_x - blc_x + 1).max(trc_y - blc_y + 1);
    if trc_x - blc_x + 1 < side {
        let deficit = side - (trc_x - blc_x + 1);
        blc_x = blc_x.saturating_sub(deficit / 2);
        trc_x = (blc_x + side - 1).min(shape[0] - 1);
        blc_x = trc_x + 1 - side;
    }
    if trc_y - blc_y + 1 < side {
        let deficit = side - (trc_y - blc_y + 1);
        blc_y = blc_y.saturating_sub(deficit / 2);
        trc_y = (blc_y + side - 1).min(shape[1] - 1);
        blc_y = trc_y + 1 - side;
    }

    let nx = trc_x - blc_x + 1;
    let ny = trc_y - blc_y + 1;
    let mut window = Array2::<f32>::zeros((nx, ny));
    for x in 0..nx {
        for y in 0..ny {
            window[(x, y)] = psf[(blc_x + x) * shape[1] + blc_y + y] / peak;
        }
    }

    let mut oversampling = TARGET_INTERPOLATED_POINTS / (nx * ny);
    oversampling = (oversampling as f64).sqrt() as usize;
    if oversampling == 0 {
        oversampling = 1;
    }
    let mut resampled = resample_psf(&window, oversampling);
    let max_value = peak_max_value_f32(&resampled);
    if !(max_value.is_finite() && max_value > 0.0) {
        return None;
    }
    resampled.mapv_inplace(|value| value / max_value);
    let (resampled_peak, _) = peak_location(&resampled)?;
    let min_len = nx.min(ny);
    let nrow_re = (oversampling * min_len).saturating_sub(1) / 2;
    let resampled_deltas = [
        deltas[0] / oversampling as f64,
        deltas[1] / oversampling as f64,
    ];
    let resampled_shape = [resampled.shape()[0], resampled.shape()[1]];
    let second_pass = find_points_in_lobe(
        resampled.as_slice()?,
        resampled_shape,
        resampled_peak.0 * resampled_shape[1] + resampled_peak.1,
        nrow_re,
        cutoff,
    )?;
    let samples = second_pass
        .samples
        .iter()
        .map(|sample| FitSample {
            x_rad: (sample.x_index as isize - resampled_peak.0 as isize) as f64
                * resampled_deltas[0],
            y_rad: (sample.y_index as isize - resampled_peak.1 as isize) as f64
                * resampled_deltas[1],
            value: resampled[[sample.x_index, sample.y_index]] as f64,
        })
        .collect::<Vec<_>>();
    Some((samples, cutoff))
}

/// Port of casacore's `findPointsInLobe` main-lobe sample walk: spiral out
/// from the peak through quadrant flips and stop each ray below the cutoff.
fn find_points_in_lobe(
    psf: &[f32],
    shape: [usize; 2],
    peak_index: usize,
    nrow: usize,
    amin: f32,
) -> Option<LobeSearchResult> {
    let [width, height] = shape;
    let px = (peak_index / height) as isize;
    let py = (peak_index % height) as isize;
    let maxnpoints = (2 * nrow + 1) * (2 * nrow + 1);
    let mut samples = Vec::new();
    let mut blc = (width - 1, height - 1);
    let mut trc = (0_usize, 0_usize);
    let mut iflip = 1_isize;
    let mut jflip = 1_isize;

    'search: for jlo in 0..2 {
        jflip *= -1;
        for j in jlo..=nrow {
            let jrow = py + j as isize * jflip;
            for ilo in 0..2 {
                iflip *= -1;
                if jrow > height as isize - 1 || jrow < 0 {
                    break;
                }
                let mut inlobe = psf[(px as usize) * height + jrow as usize] > amin;
                for i in ilo..=nrow {
                    if samples.len() >= maxnpoints {
                        break 'search;
                    }
                    let irow = px + i as isize * iflip;
                    if irow > width as isize - 1 || irow < 0 {
                        break;
                    }
                    let value = psf[irow as usize * height + jrow as usize];
                    if inlobe && value < amin {
                        break;
                    }
                    if value > amin {
                        inlobe = true;
                        let ux = irow as usize;
                        let uy = jrow as usize;
                        blc.0 = blc.0.min(ux);
                        blc.1 = blc.1.min(uy);
                        trc.0 = trc.0.max(ux);
                        trc.1 = trc.1.max(uy);
                        samples.push(SampleIndex {
                            x_index: ux,
                            y_index: uy,
                        });
                    }
                }
            }
        }
    }

    if samples.is_empty() {
        return None;
    }
    Some(LobeSearchResult { samples, blc, trc })
}

fn casa_bcucof(y: [f64; 4], y1: [f64; 4], y2: [f64; 4], y12: [f64; 4]) -> [[f64; 4]; 4] {
    const WT: [[f64; 16]; 16] = [
        [
            1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        [
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        [
            -3.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, -2.0, 0.0, 0.0, -1.0, 0.0, 0.0, 0.0, 0.0,
        ],
        [
            2.0, 0.0, 0.0, -2.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0,
        ],
        [
            0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        [
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0,
        ],
        [
            0.0, 0.0, 0.0, 0.0, -3.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0, -2.0, 0.0, 0.0, -1.0,
        ],
        [
            0.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, -2.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 1.0,
        ],
        [
            -3.0, 3.0, 0.0, 0.0, -2.0, -1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        [
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, -3.0, 3.0, 0.0, 0.0, -2.0, -1.0, 0.0, 0.0,
        ],
        [
            9.0, -9.0, 9.0, -9.0, 6.0, 3.0, -3.0, -6.0, 6.0, -6.0, -3.0, 3.0, 4.0, 2.0, 1.0, 2.0,
        ],
        [
            -6.0, 6.0, -6.0, 6.0, -4.0, -2.0, 2.0, 4.0, -3.0, 3.0, 3.0, -3.0, -2.0, -1.0, -1.0,
            -2.0,
        ],
        [
            2.0, -2.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
        ],
        [
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0, -2.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.0,
        ],
        [
            -6.0, 6.0, -6.0, 6.0, -3.0, -3.0, 3.0, 3.0, -4.0, 4.0, 2.0, -2.0, -2.0, -2.0, -1.0,
            -1.0,
        ],
        [
            4.0, -4.0, 4.0, -4.0, 2.0, 2.0, -2.0, -2.0, 2.0, -2.0, -2.0, 2.0, 1.0, 1.0, 1.0, 1.0,
        ],
    ];

    let packed = [
        y[0], y[1], y[2], y[3], y1[0], y1[1], y1[2], y1[3], y2[0], y2[1], y2[2], y2[3], y12[0],
        y12[1], y12[2], y12[3],
    ];
    let mut cl = [0.0f64; 16];
    for row in 0..16 {
        for col in 0..16 {
            cl[row] += WT[row][col] * packed[col];
        }
    }
    let mut coeffs = [[0.0f64; 4]; 4];
    for row in 0..4 {
        for col in 0..4 {
            coeffs[row][col] = cl[row * 4 + col];
        }
    }
    coeffs
}

fn casa_interp_cubic(data: &Array2<f32>, x: f64, y: f64) -> f64 {
    let nx = data.shape()[0] as isize;
    let ny = data.shape()[1] as isize;
    let i = x as isize;
    let j = y as isize;
    if i <= 0 || i >= nx - 2 || j <= 0 || j >= ny - 2 {
        return casa_interp_linear(data, x, y);
    }

    let tt = x - i as f64;
    let uu = y - j as f64;

    let mut vals = [0.0f64; 4];
    let mut dx_vals = [0.0f64; 4];
    let mut dy_vals = [0.0f64; 4];
    let mut dxy_vals = [0.0f64; 4];

    vals[0] = data[(i as usize, j as usize)] as f64;
    vals[1] = data[((i + 1) as usize, j as usize)] as f64;
    vals[2] = data[((i + 1) as usize, (j + 1) as usize)] as f64;
    vals[3] = data[(i as usize, (j + 1) as usize)] as f64;

    // Interpolate2D<T=Float> evaluates these source-pixel expressions in
    // Float before assigning them to its Double derivative arrays.
    dx_vals[0] =
        f64::from(data[((i + 1) as usize, j as usize)] - data[((i - 1) as usize, j as usize)]);
    dx_vals[1] = f64::from(data[((i + 2) as usize, j as usize)] - data[(i as usize, j as usize)]);
    dx_vals[2] = f64::from(
        data[((i + 2) as usize, (j + 1) as usize)] - data[(i as usize, (j + 1) as usize)],
    );
    dx_vals[3] = f64::from(
        data[((i + 1) as usize, (j + 1) as usize)] - data[((i - 1) as usize, (j + 1) as usize)],
    );

    dy_vals[0] =
        f64::from(data[(i as usize, (j + 1) as usize)] - data[(i as usize, (j - 1) as usize)]);
    dy_vals[1] = f64::from(
        data[((i + 1) as usize, (j + 1) as usize)] - data[((i + 1) as usize, (j - 1) as usize)],
    );
    dy_vals[2] = f64::from(
        data[((i + 1) as usize, (j + 2) as usize)] - data[((i + 1) as usize, j as usize)],
    );
    dy_vals[3] = f64::from(data[(i as usize, (j + 2) as usize)] - data[(i as usize, j as usize)]);

    dxy_vals[0] = f64::from(
        data[((i + 1) as usize, (j + 1) as usize)] + data[((i - 1) as usize, (j - 1) as usize)]
            - data[((i - 1) as usize, (j + 1) as usize)]
            - data[((i + 1) as usize, (j - 1) as usize)],
    );
    dxy_vals[1] = f64::from(
        data[((i + 2) as usize, (j + 1) as usize)] + data[(i as usize, (j - 1) as usize)]
            - data[(i as usize, (j + 1) as usize)]
            - data[((i + 2) as usize, (j - 1) as usize)],
    );
    dxy_vals[2] = f64::from(
        data[((i + 2) as usize, (j + 2) as usize)] + data[(i as usize, j as usize)]
            - data[(i as usize, (j + 2) as usize)]
            - data[((i + 2) as usize, j as usize)],
    );
    dxy_vals[3] = f64::from(
        data[((i + 1) as usize, (j + 2) as usize)] + data[((i - 1) as usize, j as usize)]
            - data[((i - 1) as usize, (j + 2) as usize)]
            - data[((i + 1) as usize, j as usize)],
    );

    for axis in 0..4 {
        dx_vals[axis] /= 2.0;
        dy_vals[axis] /= 2.0;
        dxy_vals[axis] /= 4.0;
    }

    let coeffs = casa_bcucof(vals, dx_vals, dy_vals, dxy_vals);
    let mut result = 0.0f32;
    for row in (0..4).rev() {
        result = (tt * f64::from(result)
            + ((coeffs[row][3] * uu + coeffs[row][2]) * uu + coeffs[row][1]) * uu
            + coeffs[row][0]) as f32;
    }
    f64::from(result)
}

fn casa_interp_linear(data: &Array2<f32>, x: f64, y: f64) -> f64 {
    let nx = data.shape()[0];
    let ny = data.shape()[1];
    let mut i = x as usize;
    let mut j = y as usize;
    let si = nx - 1;
    let sj = ny - 1;
    if i == si {
        i -= 1;
    }
    if j == sj {
        j -= 1;
    }
    let tt = x - i as f64;
    let uu = y - j as f64;
    (1.0 - tt) * (1.0 - uu) * f64::from(data[(i, j)])
        + tt * (1.0 - uu) * f64::from(data[(i + 1, j)])
        + tt * uu * f64::from(data[(i + 1, j + 1)])
        + (1.0 - tt) * uu * f64::from(data[(i, j + 1)])
}

fn resample_psf(psf: &Array2<f32>, oversampling: usize) -> Array2<f32> {
    let nx = psf.shape()[0];
    let ny = psf.shape()[1];
    let nx_re = nx * oversampling - oversampling + 1;
    let ny_re = ny * oversampling - oversampling + 1;
    let mut resampled = Array2::<f32>::zeros((nx_re, ny_re));
    for i in 0..nx_re {
        for j in 0..ny_re {
            // StokesImageUtil::ResamplePSF performs both operands of this
            // division in Float before assigning the result to its
            // Vector<Double> interpolation position.
            let x = f64::from(i as f32 / oversampling as f32);
            let y = f64::from(j as f32 / oversampling as f32);
            resampled[(i, j)] = casa_interp_cubic(psf, x, y) as f32;
        }
    }
    resampled
}

#[derive(Debug, Clone, Copy)]
struct CasaParams {
    width_fwhm_rad: f64,
    axial_ratio: f64,
    position_angle_rad: f64,
}

fn peak_location(image: &Array2<f32>) -> Option<((usize, usize), f32)> {
    image
        .indexed_iter()
        .fold(None, |best, (index, value)| match best {
            None => Some((index, *value)),
            Some((_, best_value)) if *value > best_value => Some((index, *value)),
            _ => best,
        })
}

fn peak_max_value_f32(image: &Array2<f32>) -> f32 {
    image
        .iter()
        .fold(f32::NEG_INFINITY, |best, value| best.max(*value))
}

fn fit_gaussian_beam_casa(samples: &[FitSample], cell_size_rad: [f64; 2]) -> Option<RestoringBeam> {
    // StokesImageUtil::FitGaussianPSF seeds the fit with a 2.5-pixel-wide
    // Gaussian at axial ratio 0.5 and position angle zero.
    let accepted_params = CasaParams {
        width_fwhm_rad: 2.5 * cell_size_rad[0].abs().max(cell_size_rad[1].abs()),
        axial_ratio: 0.5,
        position_angle_rad: 0.0,
    };
    let mut accepted_params = accepted_params;
    let (mut accepted_normal, mut accepted_known) =
        casa_normal_equations(samples, accepted_params)?;
    accepted_known.iter_mut().for_each(|value| *value = -*value);
    let mut accepted_cost = casa_gaussian_cost(samples, accepted_params)?;
    let mut lambda = 1.0e-3;
    let mut step_factor = 2.0;
    let mut candidate_params = accepted_params;
    let mut candidate_delta = [0.0; 3];
    let mut first_iteration = true;

    for _ in 0..1_000 {
        if !first_iteration {
            let Some(candidate_cost) = casa_gaussian_cost(samples, candidate_params) else {
                // The step left the representable parameter region; keep the
                // best accepted candidate, like casacore's best-effort return.
                break;
            };
            let expected_reduction = candidate_delta
                .iter()
                .enumerate()
                .map(|(axis, delta)| {
                    delta * (lambda * delta * accepted_normal[axis][axis] + accepted_known[axis])
                })
                .sum::<f64>()
                * 0.5;
            let actual_reduction = (accepted_cost - candidate_cost) * 0.5;
            if expected_reduction > 0.0 && actual_reduction > 0.0 {
                accepted_params = candidate_params;
                accepted_cost = candidate_cost;
                let Some((normal, known)) = casa_normal_equations(samples, accepted_params) else {
                    break;
                };
                accepted_normal = normal;
                accepted_known = known;
                accepted_known.iter_mut().for_each(|value| *value = -*value);
                lambda *= 0.3;
                step_factor = 2.0;
                if accepted_known.iter().all(|value| value.abs() <= 1.0e-8) {
                    break;
                }
            } else {
                lambda *= step_factor;
                step_factor *= 2.0;
                if step_factor > 1.0e10 {
                    break;
                }
            }
        }

        let mut damped_normal = accepted_normal;
        for (axis, row) in damped_normal.iter_mut().enumerate() {
            row[axis] *= 1.0 + lambda;
        }
        let Some(solved_delta) = solve_symmetric_ldlt_casacore(damped_normal, accepted_known)
        else {
            break;
        };
        candidate_delta = solved_delta;
        candidate_params = CasaParams {
            width_fwhm_rad: accepted_params.width_fwhm_rad + candidate_delta[0],
            axial_ratio: accepted_params.axial_ratio + candidate_delta[1],
            position_angle_rad: accepted_params.position_angle_rad + candidate_delta[2],
        };
        first_iteration = false;

        let delta_norm = candidate_delta.iter().map(|v| v * v).sum::<f64>().sqrt();
        let parameter_norm = [
            accepted_params.width_fwhm_rad,
            accepted_params.axial_ratio,
            accepted_params.position_angle_rad,
        ]
        .iter()
        .map(|value| value * value)
        .sum::<f64>()
        .sqrt();
        if delta_norm <= 1.0e-8 * (parameter_norm + 1.0e-8) {
            break;
        }
    }

    // NonLinearFitLM::fitIt performs one final undamped Gauss-Newton solve
    // from the last candidate before returning the solution.
    let final_params = match casa_normal_equations(samples, candidate_params) {
        Some((final_normal, mut final_known)) => {
            final_known.iter_mut().for_each(|value| *value = -*value);
            match solve_symmetric_ldlt_casacore(final_normal, final_known) {
                Some(final_delta) => CasaParams {
                    width_fwhm_rad: candidate_params.width_fwhm_rad + final_delta[0],
                    axial_ratio: candidate_params.axial_ratio + final_delta[1],
                    position_angle_rad: candidate_params.position_angle_rad + final_delta[2],
                },
                None => candidate_params,
            }
        }
        None => candidate_params,
    };
    casa_params_to_beam(final_params)
}

fn casa_normal_equations(
    samples: &[FitSample],
    params: CasaParams,
) -> Option<([[f64; 3]; 3], [f64; 3])> {
    let mut normal = [[0.0f64; 3]; 3];
    let mut gradient = [0.0f64; 3];
    for sample in samples {
        let (model, jacobian) =
            casa_gaussian_value_and_jacobian(params, sample.x_rad, sample.y_rad)?;
        let residual = model - sample.value;
        for row in 0..3 {
            gradient[row] += jacobian[row] * residual;
            for col in row..3 {
                normal[row][col] += jacobian[row] * jacobian[col];
            }
        }
    }
    #[allow(clippy::needless_range_loop)]
    for row in 0..3 {
        for col in 0..row {
            normal[row][col] = normal[col][row];
        }
    }
    Some((normal, gradient))
}

fn casa_gaussian_value_and_jacobian(
    params: CasaParams,
    x_rad: f64,
    y_rad: f64,
) -> Option<(f64, [f64; 3])> {
    let width = params.width_fwhm_rad;
    let ratio = params.axial_ratio;
    if !(width.is_finite()
        && width.abs() > MIN_SIGMA_RAD
        && ratio.is_finite()
        && ratio.abs() > 1.0e-6
        && params.position_angle_rad.is_finite())
    {
        return None;
    }
    let cos_pa = params.position_angle_rad.cos();
    let sin_pa = params.position_angle_rad.sin();
    let xnorm = x_rad * cos_pa + y_rad * sin_pa;
    let ynorm = -x_rad * sin_pa + y_rad * cos_pa;
    let xnorm2 = xnorm * xnorm;
    let ynorm2 = ynorm * ynorm;
    let xwidth = width * ratio;
    let xwidth2 = xwidth * xwidth * CASA_FWHM_TO_INTERNAL * CASA_FWHM_TO_INTERNAL;
    let ywidth2 = width * width * CASA_FWHM_TO_INTERNAL * CASA_FWHM_TO_INTERNAL;
    let x2w = 2.0 * xnorm / xwidth2;
    let y2w = 2.0 * ynorm / ywidth2;
    let x2w2 = x2w * xnorm;
    let y2w2 = y2w * ynorm;
    let model = (-(xnorm2 / xwidth2 + ynorm2 / ywidth2)).exp();
    let dev = model;
    let jacobian = [
        dev * ((x2w2 + y2w2) / width),
        dev * x2w2 * width / xwidth,
        -dev * (x2w * (-x_rad * sin_pa + y_rad * cos_pa)
            + y2w * (-x_rad * cos_pa - y_rad * sin_pa)),
    ];
    (model.is_finite() && jacobian.iter().all(|value| value.is_finite()))
        .then_some((model, jacobian))
}

fn casa_gaussian_cost(samples: &[FitSample], params: CasaParams) -> Option<f64> {
    if !(params.width_fwhm_rad.is_finite()
        && params.width_fwhm_rad.abs() > MIN_SIGMA_RAD
        && params.axial_ratio.is_finite()
        && params.position_angle_rad.is_finite())
    {
        return None;
    }
    let mut cost = 0.0_f64;
    let mut evaluated = true;
    for sample in samples {
        match casa_gaussian_value_and_jacobian(params, sample.x_rad, sample.y_rad) {
            Some((model, _)) => {
                let residual = model - sample.value;
                cost += residual * residual;
            }
            None => evaluated = false,
        }
    }
    (evaluated && cost.is_finite()).then_some(cost)
}

fn casa_params_to_beam(params: CasaParams) -> Option<RestoringBeam> {
    if !(params.width_fwhm_rad.is_finite()
        && params.axial_ratio.is_finite()
        && params.position_angle_rad.is_finite())
    {
        return None;
    }
    let (major, minor, pa) = if params.axial_ratio.abs() > 1.0 {
        (
            (params.width_fwhm_rad * params.axial_ratio).abs(),
            params.width_fwhm_rad.abs(),
            params.position_angle_rad - std::f64::consts::FRAC_PI_2,
        )
    } else {
        (
            params.width_fwhm_rad.abs(),
            (params.width_fwhm_rad * params.axial_ratio).abs(),
            params.position_angle_rad,
        )
    };
    let mut major = major;
    let mut minor = minor;
    let mut pa = casa_wrap_beam_position_angle(pa);
    if minor > major {
        std::mem::swap(&mut major, &mut minor);
        pa = casa_wrap_beam_position_angle(pa + std::f64::consts::FRAC_PI_2);
    }
    // StokesImageUtil::FitGaussianPSF solves in Double, then assigns the
    // fitted arcsec/arcsec/degree values to Vector<Float> before constructing
    // GaussianBeam. Preserve that rounding so metadata matches CASA's actual
    // published beam.
    let radians_to_degrees = 180.0 / std::f64::consts::PI;
    let radians_to_arcseconds = 3_600.0 * radians_to_degrees;
    Some(RestoringBeam {
        major_fwhm_rad: f64::from((major * radians_to_arcseconds) as f32) / radians_to_arcseconds,
        minor_fwhm_rad: f64::from((minor * radians_to_arcseconds) as f32) / radians_to_arcseconds,
        position_angle_rad: f64::from((pa * radians_to_degrees) as f32) / radians_to_degrees,
    })
}

fn casa_wrap_beam_position_angle(mut angle: f64) -> f64 {
    let pi = std::f64::consts::PI;
    let half_pi = std::f64::consts::FRAC_PI_2;
    let three_half_pi = 3.0 * half_pi;
    while (angle / half_pi).abs() > 1.0 {
        if angle > three_half_pi {
            angle -= 2.0 * pi;
        } else if angle > half_pi {
            angle -= pi;
        } else if angle < -three_half_pi {
            angle += 2.0 * pi;
        } else {
            angle += pi;
        }
    }
    angle
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lm_recovers_sigma_from_dense_exact_samples() {
        // Dense exact samples of a known Gaussian through the LM alone.
        let cell = [1.0e-4_f64, 1.0e-4];
        let sigma_true = 3.0 * cell[0] / 2.354_820_045_030_949_3;
        let mut samples = Vec::new();
        for i in -30..=30 {
            for j in -30..=30 {
                let x = i as f64 * cell[0];
                let y = j as f64 * cell[0];
                let value = (-0.5 * ((x / sigma_true).powi(2) + (y / sigma_true).powi(2))).exp();
                if value > 0.35 {
                    samples.push(FitSample {
                        x_rad: x,
                        y_rad: y,
                        value,
                    });
                }
            }
        }
        let beam = fit_gaussian_beam_casa(&samples, cell).expect("LM converges");
        assert!((beam.major_fwhm_rad() - 3.0 * cell[0]).abs() <= 0.02 * 3.0 * cell[0]);
    }

    /// A circular Gaussian sampled on the native grid must recover its width
    /// through the faithful two-pass extraction and LM refinement.
    #[test]
    fn lm_fit_recovers_circular_gaussian_width() {
        for (width_px, expected) in [(3.0_f64, 3.0), (4.0, 4.0)] {
            let shape = [32_usize, 32];
            let cell = [1.0e-4_f64, 1.0e-4];
            let centre = 16.0_f64;
            let sigma = width_px * cell[0] / 2.354_820_045_030_949_3;
            let mut psf = Vec::with_capacity(shape[0] * shape[1]);
            for x in 0..shape[0] {
                for y in 0..shape[1] {
                    let dx = (x as f64 - centre) * cell[0];
                    let dy = (y as f64 - centre) * cell[1];
                    psf.push((-0.5 * ((dx / sigma).powi(2) + (dy / sigma).powi(2))).exp() as f32);
                }
            }
            let beam = fit_restoring_beam(&psf, shape, cell, 0.35).expect("fit converges");
            assert!(
                (beam.major_fwhm_rad() - expected * cell[0]).abs() <= 0.05 * expected * cell[0],
                "width {width_px}: fitted {}",
                beam.major_fwhm_rad()
            );
        }
    }

    #[test]
    fn fitted_sidelobe_removes_the_gaussian_lobe_and_preserves_signed_extrema() {
        let shape = [32_usize, 32];
        let centre = [16_usize, 16];
        let sigma = 2.0_f64;
        let mut psf = Vec::with_capacity(shape[0] * shape[1]);
        for x in 0..shape[0] {
            for y in 0..shape[1] {
                let dx = x as f64 - centre[0] as f64;
                let dy = y as f64 - centre[1] as f64;
                psf.push((-0.5 * (dx * dx + dy * dy) / (sigma * sigma)).exp() as f32);
            }
        }
        psf[3 * shape[1] + 4] = 0.2;
        psf[27 * shape[1] + 26] = -0.4;
        let sidelobe = fitted_psf_sidelobe_fraction(&psf, shape).expect("Gaussian fit");
        assert!((sidelobe - 0.4).abs() <= 1.0e-6, "{sidelobe}");
    }

    #[test]
    fn restoring_beam_common_envelope_matches_casa_6_7_6_14() {
        let arcsec = std::f64::consts::PI / (180.0 * 3_600.0);
        let beams = [
            RestoringBeam::new(7.0 * arcsec, 4.0 * arcsec, 35_f64.to_radians()).unwrap(),
            RestoringBeam::new(6.0 * arcsec, 5.0 * arcsec, -20_f64.to_radians()).unwrap(),
        ];
        let common = RestoringBeam::common_enclosing(&beams).expect("common beam");
        assert!((common.major_fwhm_rad() / arcsec - 7.116_149_425_836_256).abs() < 1.0e-6);
        assert!((common.minor_fwhm_rad() / arcsec - 5.640_938_804_984_346).abs() < 1.0e-6);
        assert!((common.position_angle_rad().to_degrees() - 23.593_728_922_728_804).abs() < 1.0e-6);
    }

    #[test]
    fn restoring_beam_deconvolution_preserves_axis_aligned_covariance() {
        let source = RestoringBeam::new(4.0e-6, 2.0e-6, 0.1).unwrap();
        let target = RestoringBeam::new(5.0e-6, 3.0e-6, 0.1).unwrap();
        let smoothing = target
            .deconvolving_beam(source)
            .expect("deconvolution")
            .expect("nonzero smoothing beam");
        assert!(
            (source.major_fwhm_rad().hypot(smoothing.major_fwhm_rad()) - target.major_fwhm_rad())
                .abs()
                < 1.0e-15
        );
        assert!(
            (source.minor_fwhm_rad().hypot(smoothing.minor_fwhm_rad()) - target.minor_fwhm_rad())
                .abs()
                < 1.0e-15
        );
    }
}
