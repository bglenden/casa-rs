// SPDX-License-Identifier: LGPL-3.0-or-later
//! Multiscale CLEAN, as CASA's `SDAlgorithmMSClean` runs
//! `MatrixCleaner::clean` (`synthesis/MeasurementEquations/MatrixCleaner.cc`).

use crate::Error;
use crate::patch::subtract_window;
use crate::plane::{PlaneShape, Support, casacore_max_abs, peak_magnitude};
use crate::scales::ScaleBank;
use crate::solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepStop};

/// The scale-mask threshold of `MatrixCleaner` (`itsMaskThreshold`): a
/// scale may be centred where at least 90% of it lies inside the mask.
const SCALE_MASK_THRESHOLD: f64 = 0.9;

/// Multiscale CLEAN on one plane.
#[derive(Clone, Debug, PartialEq)]
pub struct Multiscale {
    scales: Vec<f64>,
    small_scale_bias: f64,
}

impl Multiscale {
    /// Multiscale CLEAN on scales `scales` (pixels, ascending) with CASA's
    /// small-scale bias.
    #[must_use]
    pub fn new(scales: Vec<f64>, small_scale_bias: f64) -> Self {
        Self {
            scales,
            small_scale_bias,
        }
    }
}

/// Working state of one multiscale step.
pub struct MultiscaleState {
    shape: PlaneShape,
    threshold: f64,
    bank: ScaleBank,
    psf_peak: [usize; 2],
    /// The step's residual convolved with each scale.
    dirty: Vec<Vec<f64>>,
    /// `PSF ⊛ scale_s ⊛ scale_o` for `s <= o`, peaking at the PSF peak.
    cross: Vec<Vec<f64>>,
    /// The signed peak of `PSF ⊛ scale_s ⊛ scale_s`.
    psf_scale_peak: Vec<f64>,
    masks: Vec<Support>,
    /// The strength of the step's first component (`tmpMaximumResidual`).
    first: Option<f64>,
}

/// Position of the `(low, high)` cross term, `low <= high`, in the packed
/// upper triangle.
fn cross_index(low: usize, high: usize, scales: usize) -> usize {
    low * scales - low * (low + 1) / 2 + high
}

impl Solver for Multiscale {
    type State = MultiscaleState;

    fn charges_stop(&self) -> bool {
        // MatrixCleaner counts the iteration that found the stop.
        true
    }

    fn initialize(
        &self,
        view: &MinorCycleView<'_>,
        residual: &[Vec<f64>],
        threshold: f64,
    ) -> Result<MultiscaleState, Error> {
        let shape = view.shape;
        let mut bank = ScaleBank::new(&self.scales, self.small_scale_bias, shape)?;
        let scales = bank.len();
        let psf_spectrum = bank.fft.forward(&view.psf[0])?;
        let mut cross = Vec::with_capacity(scales * (scales + 1) / 2);
        for low in 0..scales {
            for high in low..scales {
                cross.push(bank.convolve(&psf_spectrum, &[low, high])?);
            }
        }
        let full = Support::full(shape);
        let psf_scale_peak = (0..scales)
            .map(|scale| casacore_max_abs(&cross[cross_index(scale, scale, scales)], &full).1)
            .collect::<Vec<_>>();
        if psf_scale_peak.iter().any(|peak| *peak <= 0.0) {
            return Err(Error::NegativeScalePeak);
        }
        let residual_spectrum = bank.fft.forward(&residual[0])?;
        let dirty = (0..scales)
            .map(|scale| bank.convolve(&residual_spectrum, &[scale]))
            .collect::<Result<Vec<_>, _>>()?;
        let masks = bank.masks(view.support, SCALE_MASK_THRESHOLD, false)?;
        Ok(MultiscaleState {
            shape,
            threshold,
            bank,
            psf_peak: shape.pixel(view.summary.peak()),
            dirty,
            cross,
            psf_scale_peak,
            masks,
            first: None,
        })
    }

    fn next(
        &self,
        state: &mut MultiscaleState,
        _: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
    ) -> Result<Next, Error> {
        let mut best = (0.0_f64, 0_usize, 0_usize);
        for scale in 0..state.bank.len() {
            let (index, value) = casacore_max_abs(&state.dirty[scale], &state.masks[scale]);
            // b·v²/P: the response at the peak selects the scale.
            let maximum = value / state.psf_scale_peak[scale]
                * state.bank.bias[scale]
                * state.dirty[scale][index];
            if maximum.abs() > best.0.abs() {
                best = (maximum, scale, index);
            }
        }
        let (maximum, scale, index) = best;
        if maximum == 0.0 {
            return Ok(Next::Stop(StepStop::Threshold));
        }
        let strength = maximum / state.bank.bias[scale] / state.dirty[scale][index];
        if !strength.is_finite() {
            return Err(Error::NonFinite);
        }
        let first = *state.first.get_or_insert(strength.abs());
        if strength.abs() < state.threshold {
            return Ok(Next::Stop(StepStop::Threshold));
        }
        // A component half again as strong as the step's first: diverging.
        if strength.abs() - first > first / 2.0 {
            return Ok(Next::Stop(StepStop::Diverged));
        }
        Ok(Next::Clean(Candidate::Pixel {
            index,
            scale,
            strength,
        }))
    }

    fn accept(
        &self,
        state: &mut MultiscaleState,
        _: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
        candidate: Candidate,
        gain: f64,
        delta: &mut Delta,
    ) -> Result<(), Error> {
        let Candidate::Pixel {
            index,
            scale,
            strength,
        } = candidate;
        let shape = state.shape;
        let flux = gain * strength;
        let [x, y] = shape.pixel(index);
        let half = shape.centre();
        // The update box is the full image centred on the component.
        let in_box = |at: usize, d: isize, half: usize, extent: usize| {
            d >= -(half as isize)
                && d < half as isize
                && (0..extent as isize).contains(&(at as isize + d))
        };
        for &(dx, dy, value) in &state.bank.functions[scale].samples {
            if in_box(x, dx, half[0], shape.nx) && in_box(y, dy, half[1], shape.ny) {
                let target = shape.index((x as isize + dx) as usize, (y as isize + dy) as usize);
                delta.add(0, target, flux * value);
            }
        }
        let scales = state.bank.len();
        for (other, dirty) in state.dirty.iter_mut().enumerate() {
            let cross = &state.cross[cross_index(other.min(scale), other.max(scale), scales)];
            subtract_window(
                dirty,
                shape,
                cross,
                shape,
                state.psf_peak,
                [x, y],
                half,
                flux,
            )?;
        }
        Ok(())
    }

    fn finalize(
        &self,
        mut state: MultiscaleState,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        delta: &Delta,
    ) -> Result<f64, Error> {
        if !delta.is_empty() {
            // SDAlgorithmMSClean: the step's residual less the PSF convolved
            // with the step's model, by circular FFT.
            let shape = state.shape;
            let model = delta.dense(0, shape.len());
            let mut wrapped = vec![0.0; shape.len()];
            for (index, value) in view.psf[0].iter().enumerate() {
                let [x, y] = shape.pixel(index);
                let wx = (x + shape.nx - state.psf_peak[0]) % shape.nx;
                let wy = (y + shape.ny - state.psf_peak[1]) % shape.ny;
                wrapped[shape.index(wx, wy)] = *value;
            }
            let kernel = state.bank.fft.forward(&wrapped)?;
            let mut spectrum = state.bank.fft.forward(&model)?;
            for (value, factor) in spectrum.iter_mut().zip(&kernel) {
                *value *= factor;
            }
            let predicted = state.bank.fft.inverse(spectrum)?;
            for (value, subtracted) in residual[0].iter_mut().zip(&predicted) {
                *value -= subtracted;
            }
        }
        Ok(peak_magnitude(&residual[0], view.support))
    }
}
