// SPDX-License-Identifier: LGPL-3.0-or-later
//! The minor-cycle driver: one plane, step by step, under the plane
//! controller (`SDAlgorithmBase::deconvolve`).

use crate::Error;
use crate::controller::{CycleControls, PlaneControl, PlaneStatistics, PlaneStop};
use crate::solver::{Candidate, Delta, MinorCycleView, Next, Solver};

/// One accepted component, for diagnostics.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Component {
    /// Storage index of the component centre.
    pub index: usize,
    /// Scale ordinal (0 for a point).
    pub scale: usize,
    /// Term-0 flux after the loop gain.
    pub flux: f64,
}

/// What one plane's minor cycle did.
#[derive(Clone, Debug, PartialEq)]
pub struct PlaneOutcome {
    /// The model update, per term.
    pub delta: Delta,
    /// Iterations charged to the cycle (CASA's `iterdone`).
    pub iterations: usize,
    /// Components actually cleaned.
    pub components: usize,
    /// Sum of the absolute term-0 component fluxes.
    pub absolute_flux: f64,
    /// The peak residual on entry.
    pub start_peak: f64,
    /// The peak residual the last step reported.
    pub peak: f64,
    /// Why the plane stopped.
    pub stop: PlaneStop,
    /// The first components, up to the requested count.
    pub trace: Vec<Component>,
    /// Exact whole-plane residual refreshes.
    pub refreshes: usize,
}

/// Run one plane's minor cycle.
///
/// `statistics` is the plane's own measurement before the cycle (its entry
/// peak and noise). The plane is skipped when it has no cleanable pixel or
/// its peak is already at the cycle threshold; otherwise the solver runs in
/// steps of [`CycleControls::step_iterations`] until the plane controller
/// stops it. `trace` asks for the first components.
///
/// # Errors
///
/// [`Error::NonFinite`] when a plane to clean holds a NaN or an infinity in
/// a residual or PSF term; otherwise when the solver fails.
pub fn run_plane<S: Solver>(
    solver: &S,
    view: &MinorCycleView<'_>,
    cycle: &CycleControls,
    statistics: &PlaneStatistics,
    trace: usize,
) -> Result<PlaneOutcome, Error> {
    let start_peak = statistics.peak();
    let mut control = PlaneControl::new(cycle, statistics, start_peak);
    let mut outcome = PlaneOutcome {
        delta: Delta::new(view.residual.len()),
        iterations: 0,
        components: 0,
        absolute_flux: 0.0,
        start_peak,
        peak: start_peak,
        stop: PlaneStop::ZeroMask,
        trace: Vec::new(),
        refreshes: 0,
    };
    let mut stop = control.stop(start_peak);
    if view.support.is_empty() || stop.is_some() {
        outcome.stop = stop.unwrap_or(PlaneStop::ZeroMask);
        return Ok(outcome);
    }
    if view
        .residual
        .iter()
        .chain(view.psf)
        .flatten()
        .any(|value| !value.is_finite())
    {
        return Err(Error::NonFinite);
    }
    let mut residual = view.residual.to_vec();
    let threshold = control.step_threshold();
    let step_budget = cycle.step_iterations();
    while stop.is_none() {
        // SDAlgorithmBase::deconvolve: setPeakResidual with the previous
        // step's peak, takeOneStep, then checkStop on the new peak.
        control.observe(outcome.peak);
        let step = run_step(
            solver,
            view,
            &mut residual,
            step_budget,
            threshold,
            cycle.gain,
            trace,
            &mut outcome,
        )?;
        control.charge(step);
        stop = control.stop(outcome.peak);
        if stop.is_none() && step != step_budget {
            stop = Some(PlaneStop::Exited);
        }
    }
    outcome.stop = stop.expect("the step loop ends on a stop");
    Ok(outcome)
}

/// One solver step (`takeOneStep`): returns the iterations it charges.
#[allow(clippy::too_many_arguments)]
fn run_step<S: Solver>(
    solver: &S,
    view: &MinorCycleView<'_>,
    residual: &mut [Vec<f64>],
    budget: usize,
    threshold: f64,
    gain: f64,
    trace: usize,
    outcome: &mut PlaneOutcome,
) -> Result<usize, Error> {
    let mut state = solver.initialize(view, residual, threshold)?;
    let mut delta = Delta::new(residual.len());
    let limit = budget + solver.extra_iterations();
    let mut components = 0;
    let mut charged_stop = 0;
    while components < limit {
        match solver.next(&mut state, view, residual)? {
            Next::Stop(_) => {
                charged_stop = usize::from(solver.charges_stop());
                break;
            }
            Next::Clean(candidate) => {
                solver.accept(&mut state, view, residual, candidate, gain, &mut delta)?;
                let Candidate::Pixel {
                    index,
                    scale,
                    strength,
                } = candidate;
                let flux = gain * strength;
                outcome.absolute_flux += flux.abs();
                if outcome.trace.len() < trace {
                    outcome.trace.push(Component { index, scale, flux });
                }
                components += 1;
            }
        }
    }
    let end = solver.finalize(state, view, residual, &delta)?;
    outcome.peak = end.peak;
    outcome.refreshes += end.refreshes;
    outcome.delta.merge(&delta);
    outcome.components += components;
    let charged = components.min(budget) + charged_stop;
    outcome.iterations += charged;
    Ok(charged)
}
