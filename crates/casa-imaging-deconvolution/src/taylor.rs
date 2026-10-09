// SPDX-License-Identifier: LGPL-3.0-or-later
//! Multi-term multi-frequency CLEAN, as CASA's `SDAlgorithmMSMFS` runs
//! `MultiTermMatrixCleaner::mtclean`
//! (`synthesis/MeasurementEquations/MultiTermMatrixCleaner.cc`).

use casa_numerics::solve_symmetric_ldlt_casacore_dynamic;

use crate::Error;
use crate::patch::subtract_window;
use crate::plane::{PlaneShape, Support, beam_patch, casacore_max_abs, exceeds};
use crate::scales::ScaleBank;
use crate::solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepEnd, StepStop};

/// The scale-mask threshold of the multi-term cleaner (`setupUserMask`).
const SCALE_MASK_THRESHOLD: f64 = 0.1;
/// Mean ratio difference below which two Hessian rows count as dependent.
const DEPENDENT_ROWS: f64 = 1.0e-4;

/// Multi-term CLEAN of `terms` Taylor coefficients on one plane.
///
/// Each iteration solves the per-scale Hessian at every pixel for the
/// Taylor coefficients, picks the pixel and scale of largest
/// `bias · Σ_t c_t R_t`, and subtracts the Hessian-weighted PSF patches. The
/// update is confined to the `findBeamPatch` patch of the PSF (at least 80
/// pixels), and the step stops when the principal residual, the point-scale
/// residual over the Hessian's first entry, falls below the threshold.
/// tclean's positive loop gain disables the cleaner's adaptive-gain
/// divergence checks, so there are none.
#[derive(Clone, Debug, PartialEq)]
pub struct Taylor {
    terms: usize,
    scales: Vec<f64>,
    small_scale_bias: f64,
}

impl Taylor {
    /// Multi-term CLEAN of `terms` coefficients on scales `scales` (pixels,
    /// ascending) with CASA's small-scale bias.
    #[must_use]
    pub fn new(terms: usize, scales: Vec<f64>, small_scale_bias: f64) -> Self {
        Self {
            terms,
            scales,
            small_scale_bias,
        }
    }
}

/// Working state of one multi-term step.
pub struct TaylorState {
    shape: PlaneShape,
    terms: usize,
    threshold: f64,
    bank: ScaleBank,
    patch: usize,
    /// Per scale, the inverse Hessian (row-major `terms × terms`) and its
    /// first entry.
    inverse: Vec<Vec<f64>>,
    principal: f64,
    /// `PSF_{t1+t2} ⊛ scale_s1 ⊛ scale_s2` patches, `patch × patch`.
    hessian: Vec<Vec<f64>>,
    /// Residual term `t` convolved with scale `s`, at `t · scales + s`.
    rhs: Vec<Vec<f64>>,
    /// The coefficients at every pixel, at `t · scales + s`.
    coefficients: Vec<Vec<f64>>,
    /// `Σ_t c_t R_t` per scale.
    work: Vec<Vec<f64>>,
    masks: Vec<Support>,
    /// The region whose right-hand sides changed in the last update.
    window: Option<([usize; 2], [usize; 2])>,
}

impl TaylorState {
    fn scales(&self) -> usize {
        self.bank.len()
    }

    /// `IND4`: the packed Hessian patch of term pair `(t1, t2)` and scale
    /// pair `(s1, s2)`.
    fn hessian_index(&self, t1: usize, t2: usize, s1: usize, s2: usize) -> usize {
        let (t1, t2) = (t1.max(t2), t1.min(t2));
        let (s1, s2) = (s1.max(s2), s1.min(s2));
        let scale_pairs = self.scales() * (self.scales() + 1) / 2;
        (t1 * (t1 + 1) / 2 + t2) * scale_pairs + s1 * (s1 + 1) / 2 + s2
    }

    /// `checkConvergence`'s principal residual: the point-scale residual's
    /// masked peak over the Hessian's first entry.
    fn principal_peak(&self) -> f64 {
        (casacore_max_abs(&self.rhs[0], &self.masks[0]).1 / self.principal).abs()
    }

    /// Recompute coefficients and work over `[low, high]` (inclusive).
    fn solve_window(&mut self, low: [usize; 2], high: [usize; 2]) {
        let scales = self.scales();
        let shape = self.shape;
        for scale in 0..scales {
            let inverse = &self.inverse[scale];
            for x in low[0]..=high[0] {
                for y in low[1]..=high[1] {
                    let index = shape.index(x, y);
                    let mut work = 0.0;
                    for t1 in 0..self.terms {
                        let coefficient = (0..self.terms)
                            .map(|t2| {
                                inverse[t1 * self.terms + t2] * self.rhs[t2 * scales + scale][index]
                            })
                            .sum::<f64>();
                        self.coefficients[t1 * scales + scale][index] = coefficient;
                        work += coefficient * self.rhs[t1 * scales + scale][index];
                    }
                    self.work[scale][index] = work;
                }
            }
        }
    }
}

impl Solver for Taylor {
    type State = TaylorState;

    fn initialize(
        &self,
        view: &MinorCycleView<'_>,
        residual: &[Vec<f64>],
        threshold: f64,
    ) -> Result<TaylorState, Error> {
        let shape = view.shape;
        let terms = self.terms;
        // Scales larger than half the image are dropped (verifyScaleSizes).
        let sizes = self
            .scales
            .iter()
            .copied()
            .filter(|size| *size <= (shape.nx / 2) as f64 && *size <= (shape.ny / 2) as f64)
            .collect::<Vec<_>>();
        if sizes.is_empty() {
            return Err(Error::NoScale);
        }
        let mut bank = ScaleBank::new(&sizes, self.small_scale_bias, shape)?;
        let scales = bank.len();
        let patch = beam_patch(sizes[scales - 1], shape);
        let [px, py] = shape.pixel(view.summary.peak());
        let psf_spectra = view
            .psf
            .iter()
            .map(|psf| bank.fft.forward(psf))
            .collect::<Result<Vec<_>, _>>()?;
        let patch_shape = PlaneShape::new(patch, patch);
        let mut hessian = Vec::new();
        for t1 in 0..terms {
            for t2 in 0..=t1 {
                for s1 in 0..scales {
                    for s2 in 0..=s1 {
                        let full = bank.convolve(&psf_spectra[t1 + t2], &[s1, s2])?;
                        let mut cut = vec![0.0; patch_shape.len()];
                        for x in 0..patch {
                            for y in 0..patch {
                                let sx = (px + shape.nx + x - patch / 2) % shape.nx;
                                let sy = (py + shape.ny + y - patch / 2) % shape.ny;
                                cut[patch_shape.index(x, y)] = full[shape.index(sx, sy)];
                            }
                        }
                        hessian.push(cut);
                    }
                }
            }
        }
        let rhs = residual
            .iter()
            .map(|term| {
                let spectrum = bank.fft.forward(term)?;
                (0..scales)
                    .map(|scale| bank.convolve(&spectrum, &[scale]))
                    .collect::<Result<Vec<_>, Error>>()
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let masks = bank.masks(view.support, SCALE_MASK_THRESHOLD, true)?;
        let mut state = TaylorState {
            shape,
            terms,
            threshold,
            bank,
            patch,
            inverse: Vec::new(),
            principal: 0.0,
            hessian,
            rhs,
            coefficients: vec![vec![0.0; shape.len()]; terms * scales],
            work: vec![vec![0.0; shape.len()]; scales],
            masks,
            window: None,
        };
        let centre = patch_shape.index(patch / 2, patch / 2);
        for scale in 0..scales {
            let mut normal = vec![0.0; terms * terms];
            for t1 in 0..terms {
                for t2 in 0..terms {
                    normal[t1 * terms + t2] =
                        state.hessian[state.hessian_index(t1, t2, scale, scale)][centre];
                }
            }
            state.inverse.push(invert(&normal, terms)?);
            if scale == 0 {
                state.principal = normal[0];
            }
        }
        Ok(state)
    }

    fn next(
        &self,
        state: &mut TaylorState,
        _: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
    ) -> Result<Next, Error> {
        let peak = state.principal_peak();
        if !peak.is_finite() {
            return Err(Error::NonFinite);
        }
        if peak < state.threshold {
            return Ok(Next::Stop(StepStop::Threshold));
        }
        let shape = state.shape;
        let (low, high) = state
            .window
            .unwrap_or(([0, 0], [shape.nx - 1, shape.ny - 1]));
        state.solve_window(low, high);
        let mut best = (-1.0e10_f64, 0_usize, 0_usize);
        for scale in 0..state.scales() {
            let (index, value) = casacore_max_abs(&state.work[scale], &state.masks[scale]);
            if exceeds(value * state.bank.bias[scale], best.0) {
                best = (value * state.bank.bias[scale], scale, index);
            }
        }
        let (_, scale, index) = best;
        let strength = state.coefficients[scale][index];
        if !strength.is_finite() {
            return Err(Error::NonFinite);
        }
        Ok(Next::Clean(Candidate::Pixel {
            index,
            scale,
            strength,
        }))
    }

    fn accept(
        &self,
        state: &mut TaylorState,
        _: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
        candidate: Candidate,
        gain: f64,
        delta: &mut Delta,
    ) -> Result<(), Error> {
        let Candidate::Pixel { index, scale, .. } = candidate;
        let shape = state.shape;
        let scales = state.scales();
        let half = state.patch / 2;
        let [x, y] = shape.pixel(index);
        let coefficients = (0..state.terms)
            .map(|term| state.coefficients[term * scales + scale][index])
            .collect::<Vec<_>>();
        let in_patch = |at: usize, d: isize, extent: usize| {
            d >= -(half as isize)
                && d < half as isize
                && (0..extent as isize).contains(&(at as isize + d))
        };
        for (term, coefficient) in coefficients.iter().enumerate() {
            for &(dx, dy, value) in &state.bank.functions[scale].samples {
                if in_patch(x, dx, shape.nx) && in_patch(y, dy, shape.ny) {
                    let target =
                        shape.index((x as isize + dx) as usize, (y as isize + dy) as usize);
                    delta.add(term, target, gain * coefficient * value);
                }
            }
        }
        let patch_shape = PlaneShape::new(state.patch, state.patch);
        for t1 in 0..state.terms {
            for other in 0..scales {
                for (t2, coefficient) in coefficients.iter().enumerate() {
                    let source = state.hessian_index(t1, t2, other, scale);
                    subtract_window(
                        &mut state.rhs[t1 * scales + other],
                        shape,
                        &state.hessian[source],
                        patch_shape,
                        [half, half],
                        [x, y],
                        [half, half],
                        gain * coefficient,
                    )?;
                }
            }
        }
        let low = [x.saturating_sub(half), y.saturating_sub(half)];
        let high = [
            (x + half).saturating_sub(1).min(shape.nx - 1),
            (y + half).saturating_sub(1).min(shape.ny - 1),
        ];
        state.window = Some((low, high));
        Ok(())
    }

    fn finalize(
        &self,
        state: TaylorState,
        _: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        _: &Delta,
    ) -> Result<StepEnd, Error> {
        let peak = state.principal_peak();
        let scales = state.scales();
        // The point-scale right-hand sides are the residuals the cleaner
        // leaves (`mtclean` copies them back).
        for (term, plane) in residual.iter_mut().enumerate() {
            plane.clone_from(&state.rhs[term * scales]);
        }
        Ok(StepEnd { peak, refreshes: 0 })
    }
}

/// The inverse of one scale's Hessian, refused when a diagonal entry is
/// zero, two rows are nearly dependent, or the solve fails
/// (`computeHessianPeak`).
fn invert(normal: &[f64], terms: usize) -> Result<Vec<f64>, Error> {
    if (0..terms).any(|term| normal[term * terms + term] == 0.0) {
        return Err(Error::SingularHessian);
    }
    for row in 0..terms.saturating_sub(1) {
        let ratios = (0..terms)
            .map(|column| normal[row * terms + column] / normal[(row + 1) * terms + column])
            .collect::<Vec<_>>();
        let spread = ratios
            .windows(2)
            .map(|pair| (pair[0] - pair[1]).abs())
            .sum::<f64>()
            / (terms - 1) as f64;
        if spread < DEPENDENT_ROWS {
            return Err(Error::SingularHessian);
        }
    }
    let mut inverse = vec![0.0; terms * terms];
    for column in 0..terms {
        let mut unit = vec![0.0; terms];
        unit[column] = 1.0;
        let solution = solve_symmetric_ldlt_casacore_dynamic(normal.to_vec(), &unit)
            .ok_or(Error::SingularHessian)?;
        for (row, value) in solution.into_iter().enumerate() {
            inverse[row * terms + column] = value;
        }
    }
    Ok(inverse)
}
