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
        // `hclean` takes the PSF at the plane's centre, whatever its peak.
        let [x, y] = view.shape.centre();
        subtract_shifted(
            &mut residual[0],
            &view.psf[0],
            view.shape,
            view.shape.index(x, y),
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

#[cfg(test)]
mod hclean_tests {
    use super::*;
    use crate::{CycleControls, PlaneStatistics, PsfSummary, Support, run_plane};

    /// `hclean` subtracts the PSF from the plane's centre even where a
    /// higher point lies elsewhere in it: a unit lobe at the centre and a
    /// slightly higher one four pixels from it in x (CASA 6.7.6.14 on channel
    /// 0 of `refim_point_linRL.ms`, a PSF this shape).
    #[test]
    fn the_psf_is_taken_at_the_planes_centre() {
        let shape = PlaneShape::new(64, 64);
        let mut psf = vec![0.0; 64 * 64];
        for x in 0..64_i32 {
            for y in 0..64_i32 {
                let lobe = |cx: i32, cy: i32, peak: f64| {
                    let (dx, dy) = (f64::from(x - cx), f64::from(y - cy));
                    peak * (-(dx * dx + dy * dy) / 2.0).exp()
                };
                psf[shape.index(x as usize, y as usize)] = lobe(32, 32, 0.9998) + lobe(36, 32, 1.0);
            }
        }
        let residual = vec![psf.clone()];
        let psfs = vec![psf];
        let summary = PsfSummary::new(&psfs[0], shape).expect("a summary");
        let support = Support::new(shape, vec![true; shape.len()]);
        let view = MinorCycleView {
            shape,
            residual: &residual,
            psf: &psfs,
            summary: &summary,
            support: &support,
        };
        let statistics = PlaneStatistics::measure(&residual[0], &support, &support, 0.0, false);
        let controls = CycleControls {
            iterations: 1,
            threshold: 0.0,
            threshold_reached: true,
            gain: 1.0,
            nsigma: 0.0,
        };
        let outcome = run_plane(&Hogbom::new(false), &view, &controls, &statistics, 0).unwrap();
        let end = &outcome.end_residual[0];
        // The peak residual is at the higher lobe. There the subtraction reads
        // the PSF's centre, not its peak, which would leave almost nothing.
        let at = shape.index(36, 32);
        assert!(
            (end[at] - (residual[0][at] - residual[0][at] * psfs[0][shape.index(32, 32)])).abs()
                < 1e-9
        );
    }
}
