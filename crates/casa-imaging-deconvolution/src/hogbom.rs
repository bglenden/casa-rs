// SPDX-License-Identifier: LGPL-3.0-or-later
//! Högbom CLEAN, as CASA's `SDAlgorithmHogbomClean` runs the Fortran
//! `hclean` (casacore `scimath_f/hclean.f`).

use crate::Error;
use crate::patch::subtract_shifted;
use crate::plane::{PlaneShape, first_peak, peak_magnitude};
use crate::solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepEnd};

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
    /// The step threshold.
    type State = f64;

    fn extra_iterations(&self) -> usize {
        usize::from(self.inclusive)
    }

    fn initialize(
        &self,
        _: &MinorCycleView<'_>,
        _: &[Vec<f64>],
        threshold: f64,
    ) -> Result<f64, Error> {
        Ok(threshold)
    }

    fn next(
        &self,
        threshold: &mut f64,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
    ) -> Result<Next, Error> {
        let Some((index, value)) = first_peak(&residual[0], view.support) else {
            return Ok(Next::Stop);
        };
        if !value.is_finite() {
            return Err(Error::NonFinite);
        }
        if value.abs() < *threshold {
            return Ok(Next::Stop);
        }
        Ok(Next::Clean(Candidate::Pixel {
            index,
            scale: 0,
            strength: value,
        }))
    }

    fn accept(
        &self,
        _: &mut f64,
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
        _: f64,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        _: &Delta,
    ) -> Result<StepEnd, Error> {
        // SDAlgorithmHogbomClean::takeOneStep reports SDAlgorithmBase's
        // findMaxAbsMask, a magnitude (unlike casacore's signed one).
        Ok(StepEnd {
            peak: peak_magnitude(&residual[0], view.support),
            refreshes: 0,
        })
    }

    /// Nothing: Högbom works in the driver's residual copy.
    fn working_bytes(&self, _shape: PlaneShape, _terms: usize) -> u64 {
        0
    }

    /// A point component.
    fn component_cells(&self, _shape: PlaneShape) -> usize {
        1
    }
}
