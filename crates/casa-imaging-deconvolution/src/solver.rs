// SPDX-License-Identifier: LGPL-3.0-or-later
//! The solver interface the minor-cycle driver runs.

use std::collections::BTreeMap;

use crate::Error;
use crate::plane::{PlaneShape, Support};
use crate::psf::PsfSummary;

/// What one minor cycle of one plane works on.
///
/// Planes are x-major ([`PlaneShape`]). The residual is in CASA's
/// normalised units (Jy/beam) and the PSF is normalised so that term 0
/// peaks at 1 at [`PsfSummary::peak`]; a Taylor view holds `N_t` residual
/// terms and `2·N_t − 1` PSF terms, every other view one of each.
#[derive(Clone, Copy, Debug)]
pub struct MinorCycleView<'a> {
    /// The plane shape.
    pub shape: PlaneShape,
    /// The residual terms.
    pub residual: &'a [Vec<f64>],
    /// The PSF terms.
    pub psf: &'a [Vec<f64>],
    /// What is known about PSF term 0.
    pub summary: &'a PsfSummary,
    /// Where components may be placed.
    pub support: &'a Support,
}

/// One component a solver proposes.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub enum Candidate {
    /// A pixel component at storage index `index` on scale `scale` (0 is a
    /// point), with term-0 strength `strength` before the loop gain.
    Pixel {
        /// Storage index of the component centre.
        index: usize,
        /// Scale ordinal.
        scale: usize,
        /// Term-0 strength before the loop gain.
        strength: f64,
    },
}

/// The outcome of asking a solver for its next component.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Next {
    /// Clean this component.
    Clean(Candidate),
    /// The step ends by the solver's own rule: its threshold, its
    /// divergence rule (multiscale's 50%) or no more work (Clark's ten
    /// refreshes, no supported pixel). The plane controller judges the
    /// plane from the step's peak and charge, as CASA's does.
    Stop,
}

/// A plane solver: Högbom, Clark, multiscale or multi-term.
///
/// The driver runs one step as `initialize`, then `next`/`accept` until the
/// budget is spent or `next` stops, then `finalize`. Each step starts from
/// the residual the previous step left.
pub trait Solver {
    /// Working state of one step.
    type State;

    /// Iterations the solver may do beyond the step budget without charging
    /// them: one for CASA's inclusive Högbom loop, otherwise none.
    fn extra_iterations(&self) -> usize {
        0
    }

    /// Whether a stop by the solver's own rule charges one iteration, as
    /// CASA's `MatrixCleaner` counts the iteration that found it.
    fn charges_stop(&self) -> bool {
        false
    }

    /// Prepare one step from `residual`, cleaning down to `threshold`.
    ///
    /// # Errors
    ///
    /// When the PSF or the residual cannot support the solver.
    fn initialize(
        &self,
        view: &MinorCycleView<'_>,
        residual: &[Vec<f64>],
        threshold: f64,
    ) -> Result<Self::State, Error>;

    /// The next component, tested against the step threshold.
    ///
    /// # Errors
    ///
    /// When the arithmetic leaves the finite numbers.
    fn next(
        &self,
        state: &mut Self::State,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
    ) -> Result<Next, Error>;

    /// The flux `candidate` adds to each model term at loop gain `gain`,
    /// term 0 first, pushed onto `fluxes` before [`Solver::accept`] applies
    /// it: the single term `gain × strength` unless the solver has more.
    fn component_fluxes(
        &self,
        _state: &Self::State,
        candidate: Candidate,
        gain: f64,
        fluxes: &mut Vec<f64>,
    ) {
        let Candidate::Pixel { strength, .. } = candidate;
        fluxes.push(gain * strength);
    }

    /// Clean `candidate` with loop gain `gain`, adding the model update to
    /// `delta`.
    ///
    /// # Errors
    ///
    /// When the arithmetic leaves the finite numbers.
    fn accept(
        &self,
        state: &mut Self::State,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        candidate: Candidate,
        gain: f64,
        delta: &mut Delta,
    ) -> Result<(), Error>;

    /// End the step: bring `residual` up to date with the step's `delta`
    /// where the solver defers that, and report the peak residual CASA
    /// reports for the step.
    ///
    /// # Errors
    ///
    /// When the arithmetic leaves the finite numbers.
    fn finalize(
        &self,
        state: Self::State,
        view: &MinorCycleView<'_>,
        residual: &mut [Vec<f64>],
        delta: &Delta,
    ) -> Result<StepEnd, Error>;

    /// Heap bytes one step's working state holds at most, transients
    /// included, for planes of `shape` with `terms` residual terms; the view
    /// and the driver's residual copy are the driver's
    /// ([`crate::solve_bytes`]). The minor cycle admits this before it runs.
    fn working_bytes(&self, shape: PlaneShape, terms: usize) -> u64;
}

/// How a step ended.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StepEnd {
    /// The peak residual magnitude CASA reports for the step.
    pub peak: f64,
    /// Exact whole-plane residual refreshes the step made (Clark's cycles,
    /// multiscale's terminal convolution).
    pub refreshes: usize,
}

/// The model update of a minor cycle: flux per storage index, per Taylor
/// term.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Delta {
    terms: Vec<BTreeMap<usize, f64>>,
}

impl Delta {
    /// An empty update of `terms` terms.
    #[must_use]
    pub fn new(terms: usize) -> Self {
        Self {
            terms: vec![BTreeMap::new(); terms],
        }
    }

    /// Add `flux` at storage index `index` of term `term`.
    pub fn add(&mut self, term: usize, index: usize, flux: f64) {
        *self.terms[term].entry(index).or_insert(0.0) += flux;
    }

    /// Add every flux of `other`.
    pub fn merge(&mut self, other: &Self) {
        for (term, fluxes) in other.terms.iter().enumerate() {
            for (&index, &flux) in fluxes {
                self.add(term, index, flux);
            }
        }
    }

    /// Number of terms.
    #[must_use]
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// The non-zero fluxes of term `term` in storage order.
    pub fn term(&self, term: usize) -> impl Iterator<Item = (usize, f64)> + '_ {
        self.terms[term]
            .iter()
            .filter(|(_, flux)| **flux != 0.0)
            .map(|(index, flux)| (*index, *flux))
    }

    /// Whether the update changes nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.terms
            .iter()
            .all(|fluxes| fluxes.values().all(|flux| *flux == 0.0))
    }

    /// Term `term` as a dense plane of `len` pixels.
    #[must_use]
    pub fn dense(&self, term: usize, len: usize) -> Vec<f64> {
        let mut plane = vec![0.0; len];
        for (index, flux) in self.term(term) {
            plane[index] = flux;
        }
        plane
    }
}
