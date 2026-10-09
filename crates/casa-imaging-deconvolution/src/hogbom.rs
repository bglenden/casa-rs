// SPDX-License-Identifier: LGPL-3.0-or-later
//! Högbom CLEAN, as CASA's `SDAlgorithmHogbomClean` runs the Fortran
//! `hclean` (casacore `scimath_f/hclean.f`).

use crate::Error;
use crate::patch::subtract_shifted;
use crate::plane::{casacore_max_abs, first_peak, peak_magnitude};
use crate::solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepEnd, StepStop};

/// Working state of one Högbom step.
#[derive(Clone, Copy, Debug)]
pub struct HogbomState {
    threshold: f64,
    /// The step's starting peak, the scale of its rounding ties.
    tie_scale: f64,
}

/// Högbom's point CLEAN on one plane.
///
/// Each iteration finds the supported pixel of largest magnitude (the first
/// in x-fastest order on ties), stops when it is strictly below the
/// threshold, and otherwise subtracts the gain-scaled PSF shifted to it.
/// With `inclusive`, a step runs CASA's `do iter = 0, niter` loop: one more
/// component than its budget, charged as the budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hogbom {
    inclusive: bool,
}

impl Hogbom {
    /// Högbom with CASA's inclusive iteration count when `inclusive`.
    #[must_use]
    pub const fn new(inclusive: bool) -> Self {
        Self { inclusive }
    }
}

impl Solver for Hogbom {
    type State = HogbomState;

    fn extra_iterations(&self) -> usize {
        usize::from(self.inclusive)
    }

    fn initialize(
        &self,
        view: &MinorCycleView<'_>,
        residual: &[Vec<f64>],
        threshold: f64,
    ) -> Result<HogbomState, Error> {
        Ok(HogbomState {
            threshold,
            tie_scale: peak_magnitude(&residual[0], view.support),
        })
    }

    fn next(
        &self,
        state: &mut HogbomState,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
    ) -> Result<Next, Error> {
        let Some((index, value)) = first_peak(&residual[0], view.support, state.tie_scale) else {
            return Ok(Next::Stop(StepStop::Exhausted));
        };
        if !value.is_finite() {
            return Err(Error::NonFinite);
        }
        if value.abs() < state.threshold {
            return Ok(Next::Stop(StepStop::Threshold));
        }
        Ok(Next::Clean(Candidate::Pixel {
            index,
            scale: 0,
            strength: value,
        }))
    }

    fn accept(
        &self,
        _: &mut HogbomState,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        candidate: Candidate,
        gain: f64,
        delta: &mut Delta,
    ) -> Result<(), Error> {
        let Candidate::Pixel {
            index, strength, ..
        } = candidate;
        let flux = gain * strength;
        delta.add(0, index, flux);
        subtract_shifted(
            &mut residual[0],
            &view.psf[0],
            view.shape,
            view.summary.peak(),
            index,
            flux,
        )
    }

    fn finalize(
        &self,
        state: HogbomState,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        _: &Delta,
    ) -> Result<StepEnd, Error> {
        // SDAlgorithmHogbomClean::takeOneStep reports the signed extreme.
        Ok(StepEnd {
            peak: casacore_max_abs(&residual[0], view.support, state.tie_scale).1,
            refreshes: 0,
        })
    }
}
