// SPDX-License-Identifier: LGPL-3.0-or-later
//! Clark CLEAN, as CASA's `SDAlgorithmClarkClean2` runs
//! `ClarkCleanLatModel::solve` (`synthesis/MeasurementEquations`).

use std::cell::Cell;

use crate::Error;
use crate::plane::PlaneShape;
use crate::psf::ClarkPatch;
use crate::refresh::LinearRefresh;
use crate::solver::{Candidate, Delta, MinorCycleView, Next, Solver, StepEnd};

/// Clark's major cycles per step (`setMaxNumberMajorCycles(10)`).
const MAJOR_CYCLES: usize = 10;

/// Clark's point CLEAN on one plane.
///
/// Each Clark cycle builds the active list of supported pixels above
/// `max(threshold, peak × exterior sidelobe × factor)`, cleans it with
/// subtraction inside the PSF patch only, and then refreshes the whole
/// residual exactly ([`LinearRefresh`]). A step ends after ten Clark cycles
/// or when the peak reaches the threshold; its peak residual is the active
/// list's last maximum, as CASA reports it.
#[derive(Default)]
pub struct Clark {
    refresh: Cell<Option<LinearRefresh>>,
}

impl Clark {
    /// Clark that reuses `refresh` when it serves this plane's PSF.
    #[must_use]
    pub fn new(refresh: Option<LinearRefresh>) -> Self {
        Self {
            refresh: Cell::new(refresh),
        }
    }

    /// The refresh, to reuse in later cycles of the same PSF.
    #[must_use]
    pub fn into_refresh(self) -> Option<LinearRefresh> {
        self.refresh.into_inner()
    }
}

struct Active {
    index: usize,
    value: f64,
}

/// Working state of one Clark step.
pub struct ClarkState {
    shape: PlaneShape,
    psf_peak: [usize; 2],
    patch: ClarkPatch,
    threshold: f64,
    factor: f64,
    max_residual: f64,
    flux_limit: f64,
    iteration_flux_limit: f64,
    fac: f64,
    fmn: f64,
    cycle_iterations: usize,
    maximum_cycle_iterations: usize,
    cycles: usize,
    step_components: usize,
    active: Vec<Active>,
    refresh: LinearRefresh,
}

impl Solver for Clark {
    type State = ClarkState;

    fn initialize(
        &self,
        view: &MinorCycleView<'_>,
        residual: &[Vec<f64>],
        threshold: f64,
    ) -> Result<ClarkState, Error> {
        let shape = view.shape;
        let peak = view.summary.peak();
        let refresh = match self.refresh.take() {
            Some(refresh) if refresh.serves(shape, peak) => refresh,
            _ => LinearRefresh::new(&view.psf[0], shape, peak)?,
        };
        let patch = view.summary.clark();
        // A PSF whose sidelobes outside the patch are large gets short Clark
        // cycles (ClarkCleanLatModel::solve).
        let maximum_cycle_iterations = if patch.exterior_sidelobe > 0.5 {
            5
        } else if patch.exterior_sidelobe > 0.35 {
            50
        } else {
            usize::MAX
        };
        let max_residual = residual[0]
            .iter()
            .zip(view.support.as_slice())
            .filter(|(_, supported)| **supported)
            .fold(0.0_f64, |peak, (value, _)| peak.max(value.abs()));
        let mut state = ClarkState {
            shape,
            psf_peak: shape.pixel(peak),
            patch,
            threshold,
            factor: 1.0 / 3.0,
            max_residual,
            flux_limit: 0.0,
            iteration_flux_limit: 0.0,
            fac: 0.0,
            fmn: 0.0,
            cycle_iterations: 0,
            maximum_cycle_iterations,
            cycles: 0,
            step_components: 0,
            active: Vec::new(),
            refresh,
        };
        state.begin(&residual[0], view);
        Ok(state)
    }

    fn next(
        &self,
        state: &mut ClarkState,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
    ) -> Result<Next, Error> {
        loop {
            // ABSMAXF: the first largest in list order.
            let peak = state.active.iter().fold(None::<&Active>, |best, pixel| {
                if best.is_none_or(|current| pixel.value.abs() > current.value.abs()) {
                    Some(pixel)
                } else {
                    best
                }
            });
            if state.cycle_iterations < state.maximum_cycle_iterations
                && let Some(pixel) = peak
                && pixel.value.abs() > state.iteration_flux_limit
            {
                if !pixel.value.is_finite() {
                    return Err(Error::NonFinite);
                }
                return Ok(Next::Clean(Candidate::Pixel {
                    index: pixel.index,
                    scale: 0,
                    strength: pixel.value,
                }));
            }
            if state.cycle_iterations == 0 {
                return Ok(Next::Stop);
            }
            state.refresh_pending(&mut residual[0], view)?;
            if state.max_residual <= state.threshold || state.cycles >= MAJOR_CYCLES {
                return Ok(Next::Stop);
            }
            state.begin(&residual[0], view);
        }
    }

    fn accept(
        &self,
        state: &mut ClarkState,
        view: &MinorCycleView<'_>,
        _: &mut [Vec<f64>],
        candidate: Candidate,
        gain: f64,
        delta: &mut Delta,
    ) -> Result<(), Error> {
        let Candidate::Pixel {
            index, strength, ..
        } = candidate;
        let flux = gain * strength;
        delta.add(0, index, flux);
        state.refresh.add(index, flux);
        let shape = state.shape;
        let [px, py] = shape.pixel(index);
        let patch = state.patch;
        let psf = &view.psf[0];
        for pixel in &mut state.active {
            let [tx, ty] = shape.pixel(pixel.index);
            let relative = [tx as isize - px as isize, ty as isize - py as isize];
            if relative[0] < -(patch.radius[0] as isize)
                || relative[0] >= (patch.size[0] - patch.radius[0]) as isize
                || relative[1] < -(patch.radius[1] as isize)
                || relative[1] >= (patch.size[1] - patch.radius[1]) as isize
            {
                continue;
            }
            let source = [
                state.psf_peak[0] as isize + relative[0],
                state.psf_peak[1] as isize + relative[1],
            ];
            if source[0] < 0
                || source[1] < 0
                || source[0] >= shape.nx as isize
                || source[1] >= shape.ny as isize
            {
                continue;
            }
            pixel.value -= flux * psf[shape.index(source[0] as usize, source[1] as usize)];
        }
        state.cycle_iterations += 1;
        state.step_components += 1;
        state.fmn += state.fac / state.step_components as f64;
        state.iteration_flux_limit = if state.flux_limit > 0.0 {
            (state.flux_limit * state.fmn).max(state.threshold)
        } else {
            // Without an exterior sidelobe the flux limit is 0, CASA's
            // `Fac = absRes / fluxLimit` (speedup −1) is infinite and its
            // iteration limit `max(0 · ∞, threshold)` is NaN, so the Clark
            // cycle ends after one component.
            f64::INFINITY
        };
        Ok(())
    }

    fn finalize(
        &self,
        mut state: ClarkState,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        _: &Delta,
    ) -> Result<StepEnd, Error> {
        if state.cycle_iterations > 0 {
            state.refresh_pending(&mut residual[0], view)?;
        }
        let end = StepEnd {
            peak: state.max_residual,
            refreshes: state.cycles,
        };
        self.refresh.set(Some(state.refresh));
        Ok(end)
    }

    /// The refresh ([`LinearRefresh::bytes`]) and the active list, which
    /// grows to at most every pixel: an upper bound, since the list holds
    /// only the supported pixels above the cycle's flux limit.
    fn working_bytes(&self, shape: PlaneShape, _terms: usize) -> u64 {
        let active = shape.len().next_power_of_two() * size_of::<Active>();
        LinearRefresh::bytes(shape) + active as u64
    }

    /// A point component.
    fn component_cells(&self, _shape: PlaneShape) -> usize {
        1
    }
}

impl ClarkState {
    /// Start a Clark cycle: the flux limit from the exterior sidelobe and
    /// the active list of supported pixels above it.
    fn begin(&mut self, residual: &[f64], view: &MinorCycleView<'_>) {
        self.flux_limit = self.max_residual * self.patch.exterior_sidelobe * self.factor;
        if self.factor > 1.0 {
            self.flux_limit = self.flux_limit.min(0.95 * self.max_residual);
        }
        let cutoff = self.flux_limit.max(self.threshold);
        // GETBIMF: supported pixels at or above the cutoff, x outer and y
        // inner, which is storage order. (CASA lists a tiled residual tile
        // by tile; a plane in one tile is listed in this order.)
        self.active.clear();
        for (index, (&value, &supported)) in
            residual.iter().zip(view.support.as_slice()).enumerate()
        {
            if supported && value.abs() >= cutoff {
                self.active.push(Active { index, value });
            }
        }
        let peak = self
            .active
            .iter()
            .fold(0.0_f64, |best, pixel| best.max(pixel.value.abs()));
        // `pow(fluxLimit / absRes, speedup)` with CASA's speedup of −1.
        self.fac = if self.flux_limit > 0.0 {
            peak / self.flux_limit
        } else {
            0.0
        };
        self.fmn = 0.0;
        self.iteration_flux_limit = cutoff;
        self.cycle_iterations = 0;
    }

    /// End a Clark cycle: refresh the residual exactly and widen the next
    /// cycle's flux limit when the peak grew.
    fn refresh_pending(
        &mut self,
        residual: &mut [f64],
        view: &MinorCycleView<'_>,
    ) -> Result<(), Error> {
        let previous = self.max_residual;
        self.max_residual = self
            .active
            .iter()
            .fold(0.0_f64, |peak, pixel| peak.max(pixel.value.abs()));
        self.refresh.refresh(residual, &view.psf[0])?;
        self.cycles += 1;
        self.cycle_iterations = 0;
        if self.max_residual > previous {
            self.factor *= 3.0;
            self.maximum_cycle_iterations = 10;
        }
        Ok(())
    }
}
