// SPDX-License-Identifier: LGPL-3.0-or-later
//! PSF beam fitting and restoration helpers following CASA's `psfcutoff` flow.

use ndarray::Array2;
use num_complex::Complex32;

use crate::fft::{centered_fft2, centered_ifft2};
use crate::{BeamFit, BeamFitDebugSummary};

const CASA_FWHM_TO_INTERNAL: f64 = 0.600_561_204_393_224_9;
const PSF_PATCH_RADIUS: usize = 20;
const REGION_PADDING: usize = 5;
const TARGET_INTERPOLATED_POINTS: usize = 3001;
const MAX_FIT_ATTEMPTS: usize = 50;
const MIN_SIGMA_RAD: f64 = 1.0e-12;

pub(crate) struct BeamFitOutcome {
    pub beam: Option<BeamFit>,
    pub warnings: Vec<String>,
    pub attempts: usize,
    pub cutoff_used: Option<f32>,
    pub debug: Option<BeamFitDebugSummary>,
}

pub(crate) fn fit_beam_from_psf(
    psf: &Array2<f32>,
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> BeamFitOutcome {
    let Some((peak_index, peak)) = peak_location(psf) else {
        return BeamFitOutcome {
            beam: None,
            warnings: vec!["PSF beam fit skipped: PSF is empty".to_string()],
            attempts: 0,
            cutoff_used: None,
            debug: None,
        };
    };
    if !(peak.is_finite() && peak > 0.0) {
        return BeamFitOutcome {
            beam: None,
            warnings: vec![format!(
                "PSF beam fit skipped: PSF peak is not a positive finite value ({peak})"
            )],
            attempts: 0,
            cutoff_used: None,
            debug: None,
        };
    }

    let mut warnings = Vec::new();
    let mut active_cutoff = cutoff.clamp(0.01, 0.99);

    for attempt in 1..=MAX_FIT_ATTEMPTS {
        let Some((samples, debug)) =
            extract_casa_fit_samples(psf, peak_index, peak, cell_size_rad, active_cutoff)
        else {
            warnings.push(format!(
                "PSF beam fit attempt {attempt} failed to isolate a main-lobe region at psfcutoff={active_cutoff:.6}"
            ));
            active_cutoff /= 1.5;
            continue;
        };
        if samples.len() < 12 {
            warnings.push(format!(
                "PSF beam fit attempt {attempt} found only {} fit samples at psfcutoff={active_cutoff:.6}",
                samples.len()
            ));
            active_cutoff /= 1.5;
            continue;
        }
        if let Some(beam) = fit_gaussian_beam_casa(&samples, cell_size_rad, attempt) {
            if attempt > 1 {
                warnings.push(format!(
                    "PSF beam fit converged after {attempt} attempts with psfcutoff={active_cutoff:.6}"
                ));
            }
            return BeamFitOutcome {
                beam: Some(beam),
                warnings,
                attempts: attempt,
                cutoff_used: Some(active_cutoff),
                debug: Some(debug),
            };
        }
        warnings.push(format!(
            "PSF beam fit attempt {attempt} failed to converge at psfcutoff={active_cutoff:.6}"
        ));
        active_cutoff /= 1.5;
    }

    warnings.push(format!(
        "PSF beam fit failed after {MAX_FIT_ATTEMPTS} attempts starting from psfcutoff={cutoff:.3}"
    ));
    BeamFitOutcome {
        beam: None,
        warnings,
        attempts: MAX_FIT_ATTEMPTS,
        cutoff_used: None,
        debug: None,
    }
}

pub(crate) fn restore_model(
    model: &Array2<f32>,
    cell_size_rad: [f64; 2],
    beam: Option<BeamFit>,
) -> Array2<f32> {
    if model.iter().all(|flux| flux.abs() <= 1.0e-12) {
        return model.clone();
    }

    let Some(beam) = beam else {
        return model.clone();
    };

    let gaussian_psf = make_casa_gaussian_psf_image(model.raw_dim(), cell_size_rad, beam, false);
    fft_convolve_real_casa_restore(model, &gaussian_psf)
}

fn fft_convolve_real_casa_restore(model: &Array2<f32>, psf: &Array2<f32>) -> Array2<f32> {
    let model_complex = model.mapv(|value| Complex32::new(value, 0.0));
    let psf_complex = psf.mapv(|value| Complex32::new(value, 0.0));
    let product = centered_fft2(&model_complex) * centered_fft2(&psf_complex);
    centered_ifft2(&product).mapv(|value| value.re)
}

pub(crate) fn estimate_psf_sidelobe_level(
    psf: &Array2<f32>,
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> f32 {
    let BeamFitOutcome { beam, .. } = fit_beam_from_psf(psf, cell_size_rad, cutoff);
    let Some(beam) = beam else {
        return 0.0;
    };

    let gaussian_psf = make_casa_gaussian_psf_image(psf.raw_dim(), cell_size_rad, beam, false);
    estimate_psf_sidelobe_level_with_beam(psf, &gaussian_psf)
}

pub(crate) fn estimate_psf_sidelobe_level_for_beam(
    psf: &Array2<f32>,
    cell_size_rad: [f64; 2],
    beam: Option<BeamFit>,
) -> f32 {
    let Some(beam) = beam else {
        return 0.0;
    };
    let gaussian_psf = make_casa_gaussian_psf_image(psf.raw_dim(), cell_size_rad, beam, false);
    estimate_psf_sidelobe_level_with_beam(psf, &gaussian_psf)
}

fn estimate_psf_sidelobe_level_with_beam(psf: &Array2<f32>, gaussian_psf: &Array2<f32>) -> f32 {
    let mut all_min = 0.0f32;
    let mut all_max = 0.0f32;
    for ((x, y), value) in psf.indexed_iter() {
        all_min = all_min.min(*value);
        all_max = all_max.max(*value - gaussian_psf[(x, y)]);
    }
    all_min.abs().max(all_max.abs())
}

fn make_casa_gaussian_psf_image(
    shape: ndarray::Ix2,
    cell_size_rad: [f64; 2],
    beam: BeamFit,
    normalize_volume: bool,
) -> Array2<f32> {
    let nx = shape[0];
    let ny = shape[1];
    let mut image = Array2::<f32>::zeros(shape);
    if !(beam.major_fwhm_rad.is_finite()
        && beam.major_fwhm_rad > 0.0
        && beam.minor_fwhm_rad.is_finite()
        && beam.minor_fwhm_rad > 0.0)
    {
        return image;
    }

    // CASA's MakeGaussianPSF converts GaussianBeam PA into the x/y image
    // convention by adding 90 degrees before evaluating the analytic beam.
    let pa = beam.position_angle_rad + std::f64::consts::FRAC_PI_2;
    let cos_pa = pa.cos();
    let sin_pa = pa.sin();
    let ref_x = (nx / 2) as f64;
    let ref_y = (ny / 2) as f64;
    let dx = cell_size_rad[0].abs();
    let dy = cell_size_rad[1].abs();
    let sbmaj = 4.0 * std::f64::consts::LN_2 * (1.0 / beam.major_fwhm_rad).powi(2);
    let sbmin = 4.0 * std::f64::consts::LN_2 * (1.0 / beam.minor_fwhm_rad).powi(2);

    let mut volume = 0.0f64;
    for y in 0..ny {
        for x in 0..nx {
            let major = cos_pa * (x as f64 - ref_x) * dx + sin_pa * (y as f64 - ref_y) * dy;
            let minor = -sin_pa * (x as f64 - ref_x) * dx + cos_pa * (y as f64 - ref_y) * dy;
            let radius = sbmaj * major.powi(2) + sbmin * minor.powi(2);
            let value = if radius < 20.0 {
                (-radius).exp_m1() + 1.0
            } else {
                0.0
            };
            image[(x, y)] = value as f32;
            volume += value;
        }
    }
    if normalize_volume && volume > 0.0 {
        image.mapv_inplace(|value| (f64::from(value) / volume) as f32);
    }
    image
}

#[derive(Clone, Copy)]
struct FitSample {
    x_rad: f64,
    y_rad: f64,
    value: f64,
}

#[derive(Clone, Copy)]
struct CasaBeamFitParams {
    width_fwhm_rad: f64,
    axial_ratio: f64,
    position_angle_rad: f64,
}

struct LobeSearchResult {
    samples: Vec<FitSample>,
    blc: (usize, usize),
    trc: (usize, usize),
}

fn extract_casa_fit_samples(
    psf: &Array2<f32>,
    peak_index: (usize, usize),
    peak: f32,
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> Option<(Vec<FitSample>, BeamFitDebugSummary)> {
    let deltas = [cell_size_rad[0].abs(), cell_size_rad[1].abs()];
    let first_pass = find_points_in_lobe(psf, peak_index, PSF_PATCH_RADIUS, cutoff, deltas)?;

    let mut blc_x = first_pass.blc.0.saturating_sub(REGION_PADDING);
    let mut blc_y = first_pass.blc.1.saturating_sub(REGION_PADDING);
    let mut trc_x = (first_pass.trc.0 + REGION_PADDING).min(psf.shape()[0] - 1);
    let mut trc_y = (first_pass.trc.1 + REGION_PADDING).min(psf.shape()[1] - 1);

    let side = (trc_x - blc_x + 1).max(trc_y - blc_y + 1);
    if trc_x - blc_x + 1 < side {
        let deficit = side - (trc_x - blc_x + 1);
        blc_x = blc_x.saturating_sub(deficit / 2);
        trc_x = (blc_x + side - 1).min(psf.shape()[0] - 1);
        blc_x = trc_x + 1 - side;
    }
    if trc_y - blc_y + 1 < side {
        let deficit = side - (trc_y - blc_y + 1);
        blc_y = blc_y.saturating_sub(deficit / 2);
        trc_y = (blc_y + side - 1).min(psf.shape()[1] - 1);
        blc_y = trc_y + 1 - side;
    }

    let nx = trc_x - blc_x + 1;
    let ny = trc_y - blc_y + 1;
    let mut window = Array2::<f32>::zeros((nx, ny));
    for x in 0..nx {
        for y in 0..ny {
            window[(x, y)] = psf[(blc_x + x, blc_y + y)] / peak;
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
    let resampled_peak = peak_location(&resampled)?.0;
    let min_len = nx.min(ny);
    let nrow_re = (oversampling * min_len).saturating_sub(1) / 2;
    let resampled_deltas = [
        deltas[0] / oversampling as f64,
        deltas[1] / oversampling as f64,
    ];
    let second_pass = find_points_in_lobe(
        &resampled,
        resampled_peak,
        nrow_re,
        cutoff,
        resampled_deltas,
    )?;
    let debug = BeamFitDebugSummary {
        peak_index,
        peak_value: peak,
        first_pass_points: first_pass.samples.len(),
        first_pass_blc: first_pass.blc,
        first_pass_trc: first_pass.trc,
        expanded_window_shape: (nx, ny),
        oversampling,
        resampled_shape: (resampled.shape()[0], resampled.shape()[1]),
        second_pass_points: second_pass.samples.len(),
        second_pass_blc: second_pass.blc,
        second_pass_trc: second_pass.trc,
    };
    Some((second_pass.samples, debug))
}

fn find_points_in_lobe(
    psf: &Array2<f32>,
    peak_index: (usize, usize),
    nrow: usize,
    amin: f32,
    deltas: [f64; 2],
) -> Option<LobeSearchResult> {
    let mut samples = Vec::new();
    let mut blc = (psf.shape()[0] - 1, psf.shape()[1] - 1);
    let mut trc = (0usize, 0usize);
    let mut iflip = 1isize;
    let mut jflip = 1isize;
    let px = peak_index.0 as isize;
    let py = peak_index.1 as isize;
    let maxnpoints = (2 * nrow + 1) * (2 * nrow + 1);

    'search: for jlo in 0..2 {
        jflip *= -1;
        for j in jlo..=nrow {
            let jrow = py + j as isize * jflip;
            for ilo in 0..2 {
                iflip *= -1;
                if jrow > psf.shape()[1] as isize - 1 || jrow < 0 {
                    break;
                }
                let mut inlobe = psf[(px as usize, jrow as usize)] > amin;
                for i in ilo..=nrow {
                    if samples.len() >= maxnpoints {
                        break 'search;
                    }
                    let irow = px + i as isize * iflip;
                    if irow > psf.shape()[0] as isize - 1 || irow < 0 {
                        break;
                    }
                    let value = psf[(irow as usize, jrow as usize)];
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
                        samples.push(FitSample {
                            x_rad: (irow - px) as f64 * deltas[0],
                            y_rad: (jrow - py) as f64 * deltas[1],
                            value: value as f64,
                        });
                    }
                }
            }
        }
    }

    if samples.is_empty() {
        return None;
    }

    // Match CASA FindNpoints(): force the selected bounding box square before
    // the expanded resampling window is derived from it.
    if blc.0 > blc.1 {
        blc.0 = blc.1;
    } else {
        blc.1 = blc.0;
    }
    if trc.0 > trc.1 {
        trc.1 = trc.0;
    } else {
        trc.0 = trc.1;
    }

    Some(LobeSearchResult { samples, blc, trc })
}

fn resample_psf(psf: &Array2<f32>, oversampling: usize) -> Array2<f32> {
    let nx = psf.shape()[0];
    let ny = psf.shape()[1];
    let nx_re = nx * oversampling - oversampling + 1;
    let ny_re = ny * oversampling - oversampling + 1;
    let mut resampled = Array2::<f32>::zeros((nx_re, ny_re));
    for i in 0..nx_re {
        for j in 0..ny_re {
            let x = i as f64 / oversampling as f64;
            let y = j as f64 / oversampling as f64;
            resampled[(i, j)] = casa_interp_cubic(psf, x, y) as f32;
        }
    }
    resampled
}

fn fit_gaussian_beam_casa(
    samples: &[FitSample],
    cell_size_rad: [f64; 2],
    _attempt: usize,
) -> Option<BeamFit> {
    fit_gaussian_beam_casa_seeded(samples, cell_size_rad, 1.0).map(|(beam, _)| beam)
}

fn fit_gaussian_beam_casa_seeded(
    samples: &[FitSample],
    cell_size_rad: [f64; 2],
    seed_angle_rad: f64,
) -> Option<(BeamFit, f64)> {
    let mut params = CasaBeamFitParams {
        width_fwhm_rad: 2.5 * cell_size_rad[0].abs(),
        axial_ratio: 0.5,
        position_angle_rad: seed_angle_rad,
    };
    stabilize_casa_params(&mut params);
    let mut lambda = 1.0e-3;
    let mut best_cost = casa_gaussian_cost(samples, params)?;

    for _ in 0..50 {
        let (mut normal, gradient) = casa_normal_equations(samples, params)?;
        for (axis, row) in normal.iter_mut().enumerate() {
            let damping = row[axis].abs().max(1.0e-12) * lambda;
            row[axis] += damping;
        }
        let rhs = gradient.map(|value| -value);
        let delta = solve_3x3(normal, rhs)?;
        if delta.iter().all(|value| value.abs() < 1.0e-12) {
            break;
        }

        let mut candidate = params;
        candidate.width_fwhm_rad += delta[0];
        candidate.axial_ratio += delta[1];
        candidate.position_angle_rad += delta[2];
        stabilize_casa_params(&mut candidate);
        let Some(candidate_cost) = casa_gaussian_cost(samples, candidate) else {
            lambda *= 4.0;
            continue;
        };
        if candidate_cost < best_cost {
            params = candidate;
            if (best_cost - candidate_cost) / best_cost.max(1.0e-12) < 1.0e-6 {
                break;
            }
            best_cost = candidate_cost;
            lambda = (lambda / 3.0).max(1.0e-6);
        } else {
            lambda = (lambda * 5.0).min(1.0e6);
        }
    }

    stabilize_casa_params(&mut params);
    casa_params_to_beam(params).map(|beam| (beam, best_cost))
}

fn casa_gaussian_cost(samples: &[FitSample], params: CasaBeamFitParams) -> Option<f64> {
    if !(params.width_fwhm_rad.is_finite()
        && params.width_fwhm_rad.abs() > MIN_SIGMA_RAD
        && params.axial_ratio.is_finite()
        && params.position_angle_rad.is_finite())
    {
        return None;
    }
    let cost = samples
        .iter()
        .map(|sample| {
            let residual = casa_gaussian_value(params, sample.x_rad, sample.y_rad) - sample.value;
            residual * residual
        })
        .sum::<f64>();
    cost.is_finite().then_some(cost)
}

fn casa_normal_equations(
    samples: &[FitSample],
    params: CasaBeamFitParams,
) -> Option<([[f64; 3]; 3], [f64; 3])> {
    let steps = casa_parameter_steps(params);
    let mut normal = [[0.0f64; 3]; 3];
    let mut gradient = [0.0f64; 3];

    for sample in samples {
        let model = casa_gaussian_value(params, sample.x_rad, sample.y_rad);
        let residual = model - sample.value;
        let mut jacobian = [0.0f64; 3];
        for axis in 0..3 {
            let mut forward = params;
            let mut backward = params;
            match axis {
                0 => {
                    forward.width_fwhm_rad += steps[0];
                    backward.width_fwhm_rad -= steps[0];
                }
                1 => {
                    forward.axial_ratio += steps[1];
                    backward.axial_ratio -= steps[1];
                }
                _ => {
                    forward.position_angle_rad += steps[2];
                    backward.position_angle_rad -= steps[2];
                }
            }
            stabilize_casa_params(&mut forward);
            stabilize_casa_params(&mut backward);
            let fwd = casa_gaussian_value(forward, sample.x_rad, sample.y_rad);
            let bwd = casa_gaussian_value(backward, sample.x_rad, sample.y_rad);
            jacobian[axis] = (fwd - bwd) / (2.0 * steps[axis]);
        }
        for row in 0..3 {
            gradient[row] += jacobian[row] * residual;
            for col in row..3 {
                normal[row][col] += jacobian[row] * jacobian[col];
            }
        }
    }

    for row in 0..3 {
        let (previous_rows, current_and_later) = normal.split_at_mut(row);
        let current_row = &mut current_and_later[0];
        for (col, source_row) in previous_rows.iter().enumerate() {
            current_row[col] = source_row[row];
        }
    }
    Some((normal, gradient))
}

fn casa_gaussian_value(params: CasaBeamFitParams, x_rad: f64, y_rad: f64) -> f64 {
    if !(params.width_fwhm_rad.is_finite()
        && params.width_fwhm_rad.abs() > MIN_SIGMA_RAD
        && params.axial_ratio.is_finite()
        && params.axial_ratio.abs() > 1.0e-6
        && params.position_angle_rad.is_finite())
    {
        return 0.0;
    }

    // Match casacore Gaussian2D exactly:
    // x' = cos(pa) * x + sin(pa) * y
    // y' = -sin(pa) * x + cos(pa) * y
    // exp(-(x'/(width*ratio*fwhm2int))^2 - (y'/(width*fwhm2int))^2)
    let cos_pa = params.position_angle_rad.cos();
    let sin_pa = params.position_angle_rad.sin();
    let xnorm = cos_pa * x_rad + sin_pa * y_rad;
    let ynorm = -sin_pa * x_rad + cos_pa * y_rad;
    let denom_x = params.width_fwhm_rad * params.axial_ratio * CASA_FWHM_TO_INTERNAL;
    let denom_y = params.width_fwhm_rad * CASA_FWHM_TO_INTERNAL;
    if denom_x.abs() <= MIN_SIGMA_RAD || denom_y.abs() <= MIN_SIGMA_RAD {
        return 0.0;
    }
    let exponent = -((xnorm / denom_x).powi(2) + (ynorm / denom_y).powi(2));
    exponent.exp()
}

fn casa_parameter_steps(params: CasaBeamFitParams) -> [f64; 3] {
    [
        (params.width_fwhm_rad.abs() * 5.0e-2).max(1.0e-8),
        (params.axial_ratio.abs() * 5.0e-2).max(1.0e-6),
        1.0e-3,
    ]
}

fn stabilize_casa_params(params: &mut CasaBeamFitParams) {
    if params.width_fwhm_rad.abs() < MIN_SIGMA_RAD {
        params.width_fwhm_rad = MIN_SIGMA_RAD.copysign(params.width_fwhm_rad);
    }
    if params.axial_ratio.abs() < 1.0e-6 {
        params.axial_ratio = 1.0e-6_f64.copysign(params.axial_ratio);
    }
}

fn casa_params_to_beam(params: CasaBeamFitParams) -> Option<BeamFit> {
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
    let mut beam = BeamFit {
        major_fwhm_rad: major,
        minor_fwhm_rad: minor,
        position_angle_rad: casa_wrap_beam_position_angle(pa),
    };
    if beam.minor_fwhm_rad > beam.major_fwhm_rad {
        std::mem::swap(&mut beam.major_fwhm_rad, &mut beam.minor_fwhm_rad);
        beam.position_angle_rad =
            casa_wrap_beam_position_angle(beam.position_angle_rad + std::f64::consts::FRAC_PI_2);
    }
    Some(beam)
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

fn solve_3x3(mut a: [[f64; 3]; 3], mut b: [f64; 3]) -> Option<[f64; 3]> {
    for pivot in 0..3 {
        let (best_row, best_value) = (pivot..3)
            .map(|row| (row, a[row][pivot].abs()))
            .max_by(|left, right| left.1.partial_cmp(&right.1).unwrap())?;
        if best_value <= 1.0e-18 {
            return None;
        }
        if best_row != pivot {
            a.swap(best_row, pivot);
            b.swap(best_row, pivot);
        }
        let pivot_value = a[pivot][pivot];
        for value in &mut a[pivot][pivot..] {
            *value /= pivot_value;
        }
        b[pivot] /= pivot_value;
        let pivot_row = a[pivot];
        for row in 0..3 {
            if row == pivot {
                continue;
            }
            let factor = a[row][pivot];
            if factor.abs() <= 1.0e-18 {
                continue;
            }
            for (value, pivot_value) in a[row][pivot..].iter_mut().zip(pivot_row[pivot..].iter()) {
                *value -= factor * *pivot_value;
            }
            b[row] -= factor * b[pivot];
        }
    }
    Some(b)
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

    dx_vals[0] =
        data[((i + 1) as usize, j as usize)] as f64 - data[((i - 1) as usize, j as usize)] as f64;
    dx_vals[1] =
        data[((i + 2) as usize, j as usize)] as f64 - data[(i as usize, j as usize)] as f64;
    dx_vals[2] = data[((i + 2) as usize, (j + 1) as usize)] as f64
        - data[(i as usize, (j + 1) as usize)] as f64;
    dx_vals[3] = data[((i + 1) as usize, (j + 1) as usize)] as f64
        - data[((i - 1) as usize, (j + 1) as usize)] as f64;

    dy_vals[0] =
        data[(i as usize, (j + 1) as usize)] as f64 - data[(i as usize, (j - 1) as usize)] as f64;
    dy_vals[1] = data[((i + 1) as usize, (j + 1) as usize)] as f64
        - data[((i + 1) as usize, (j - 1) as usize)] as f64;
    dy_vals[2] = data[((i + 1) as usize, (j + 2) as usize)] as f64
        - data[((i + 1) as usize, j as usize)] as f64;
    dy_vals[3] =
        data[(i as usize, (j + 2) as usize)] as f64 - data[(i as usize, j as usize)] as f64;

    dxy_vals[0] = data[((i + 1) as usize, (j + 1) as usize)] as f64
        + data[((i - 1) as usize, (j - 1) as usize)] as f64
        - data[((i - 1) as usize, (j + 1) as usize)] as f64
        - data[((i + 1) as usize, (j - 1) as usize)] as f64;
    dxy_vals[1] = data[((i + 2) as usize, (j + 1) as usize)] as f64
        + data[(i as usize, (j - 1) as usize)] as f64
        - data[(i as usize, (j + 1) as usize)] as f64
        - data[((i + 2) as usize, (j - 1) as usize)] as f64;
    dxy_vals[2] = data[((i + 2) as usize, (j + 2) as usize)] as f64
        + data[(i as usize, j as usize)] as f64
        - data[(i as usize, (j + 2) as usize)] as f64
        - data[((i + 2) as usize, j as usize)] as f64;
    dxy_vals[3] = data[((i + 1) as usize, (j + 2) as usize)] as f64
        + data[((i - 1) as usize, j as usize)] as f64
        - data[((i - 1) as usize, (j + 2) as usize)] as f64
        - data[((i + 1) as usize, j as usize)] as f64;

    for axis in 0..4 {
        dx_vals[axis] /= 2.0;
        dy_vals[axis] /= 2.0;
        dxy_vals[axis] /= 4.0;
    }

    let coeffs = casa_bcucof(vals, dx_vals, dy_vals, dxy_vals);
    let mut result = 0.0f64;
    for row in (0..4).rev() {
        result = tt * result
            + ((coeffs[row][3] * uu + coeffs[row][2]) * uu + coeffs[row][1]) * uu
            + coeffs[row][0];
    }
    result
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

#[cfg(test)]
mod tests {
    use ndarray::Array2;

    use super::{fit_beam_from_psf, make_casa_gaussian_psf_image, restore_model};
    use crate::BeamFit;

    fn synthetic_gaussian_psf(
        shape: (usize, usize),
        cell_size_rad: [f64; 2],
        beam: BeamFit,
    ) -> Array2<f32> {
        let mut psf = Array2::<f32>::zeros(shape);
        let center = (shape.0 as f64 / 2.0, shape.1 as f64 / 2.0);
        let sigma_major = beam.major_fwhm_rad / 2.354_820_045_030_949_3;
        let sigma_minor = beam.minor_fwhm_rad / 2.354_820_045_030_949_3;
        let cos_pa = beam.position_angle_rad.cos();
        let sin_pa = beam.position_angle_rad.sin();
        for x in 0..shape.0 {
            for y in 0..shape.1 {
                let dx = (x as f64 - center.0) * cell_size_rad[0];
                let dy = (y as f64 - center.1) * cell_size_rad[1];
                let major = -dx * sin_pa + dy * cos_pa;
                let minor = dx * cos_pa + dy * sin_pa;
                let exponent =
                    -0.5 * ((major / sigma_major).powi(2) + (minor / sigma_minor).powi(2));
                psf[(x, y)] = exponent.exp() as f32;
            }
        }
        psf
    }

    #[test]
    fn fitted_beam_tracks_elliptical_psf() {
        let expected = BeamFit {
            major_fwhm_rad: 5.0e-4,
            minor_fwhm_rad: 3.0e-4,
            position_angle_rad: 0.35,
        };
        let psf = synthetic_gaussian_psf((64, 64), [1.0e-4, 1.0e-4], expected);
        let outcome = fit_beam_from_psf(&psf, [1.0e-4, 1.0e-4], 0.35);
        let beam = outcome.beam.expect("fit beam");
        assert!((beam.major_fwhm_rad - expected.major_fwhm_rad).abs() < 7.5e-5);
        assert!((beam.minor_fwhm_rad - expected.minor_fwhm_rad).abs() < 7.5e-5);
        assert!((beam.position_angle_rad - expected.position_angle_rad).abs() < 0.15);
    }

    #[test]
    fn beam_fit_retries_lower_cutoffs() {
        let expected = BeamFit {
            major_fwhm_rad: 4.0e-4,
            minor_fwhm_rad: 2.5e-4,
            position_angle_rad: -0.2,
        };
        let psf = synthetic_gaussian_psf((64, 64), [1.0e-4, 1.0e-4], expected);
        let outcome = fit_beam_from_psf(&psf, [1.0e-4, 1.0e-4], 0.95);
        assert!(outcome.attempts > 1);
        assert!(outcome.cutoff_used.expect("cutoff") < 0.95);
        assert!(outcome.beam.is_some());
    }

    #[test]
    fn restoration_spreads_delta_component() {
        let mut model = Array2::<f32>::zeros((16, 16));
        model[(8, 8)] = 1.0;
        let restored = restore_model(
            &model,
            [1.0e-4, 1.0e-4],
            Some(BeamFit {
                major_fwhm_rad: 4.0e-4,
                minor_fwhm_rad: 3.0e-4,
                position_angle_rad: 0.0,
            }),
        );
        assert!(restored[(8, 8)] > restored[(8, 9)]);
        assert!(restored[(8, 8)] <= 1.0);
        assert!(restored[(8, 9)] > 0.0);
    }

    #[test]
    fn restoration_uses_casa_style_circular_fft_boundary() {
        let mut model = Array2::<f32>::zeros((16, 16));
        model[(0, 8)] = 1.0;
        let restored = restore_model(
            &model,
            [1.0e-4, 1.0e-4],
            Some(BeamFit {
                major_fwhm_rad: 4.0e-4,
                minor_fwhm_rad: 3.0e-4,
                position_angle_rad: 0.0,
            }),
        );
        assert!(restored[(0, 8)] > 0.0);
        assert!(
            restored[(15, 8)] > 0.0,
            "CASA StokesImageUtil::Convolve uses full-plane FFT convolution, so restored model flux wraps at image boundaries"
        );
    }

    #[test]
    fn restoration_skips_fft_for_empty_model() {
        let model = Array2::<f32>::zeros((16, 16));
        let restored = restore_model(
            &model,
            [1.0e-4, 1.0e-4],
            Some(BeamFit {
                major_fwhm_rad: 10.0,
                minor_fwhm_rad: 10.0,
                position_angle_rad: 0.0,
            }),
        );
        assert_eq!(restored, model);
    }

    #[test]
    fn casa_gaussian_psf_image_decays_away_from_center() {
        let beam = BeamFit {
            major_fwhm_rad: 4.0e-4,
            minor_fwhm_rad: 3.0e-4,
            position_angle_rad: 0.0,
        };
        let image =
            make_casa_gaussian_psf_image(ndarray::Ix2(32, 32), [1.0e-4, 1.0e-4], beam, false);
        assert!(image[(16, 16)] > image[(15, 16)]);
        assert!(image[(15, 16)] > image[(14, 16)]);
        assert!(image[(16, 16)] <= 1.0);
    }
}
