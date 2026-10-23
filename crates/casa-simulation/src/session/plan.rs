// SPDX-License-Identifier: LGPL-3.0-or-later
//! The deferred plan of DATA: what the session's DATA steps asked for,
//! applied when `close` writes each row.
//!
//! In CASA every step that changes DATA (`predict`, `corrupt`) rewrites the
//! rows that exist when it is called, which are those of the scans observed
//! so far; later scans are untouched. Each step here records how many scans
//! it covers, so for scan `s` DATA is the fold of the steps covering `s`, in
//! the order they were taken, over the zero DATA `observe` writes. Nothing
//! is read back from disk, and a sequence of steps costs one pass.
//!
//! A `predict` that replaces DATA covers every scan an earlier step covers
//! (each covers a prefix of the scans), so it clears the plan before it is
//! recorded; every step left then adds: DATA is the sum of the predictions
//! covering the scan and the noise of the corruptions covering it. The fold
//! applies each step to the running DATA (`D ← step(D)`), so multiplicative
//! gains (`D ← G(D)`) slot in as further [`DataStep`] variants.
//!
//! Scans are only ever added, so each step covers at least the scans of the
//! one before it, and the steps covering a scan are the last ones taken.
//! Every corruption's applications are kept end to end in one list, so a
//! scan's noise is the end of that list: `close` reads it where it is, and
//! N interleaved scans and corruptions keep N lists' worth, not N²
//! (#853 review).

use std::ops::Range;
use std::sync::Arc;

use super::prediction::Prediction;
use crate::corrupt::NoiseApplication;
use crate::predict::ComponentPrediction;

/// One `setnoise` term: CASA's `ANoise` calibration term, kept until
/// `reset`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct NoiseTerm {
    /// The seed in force at `setnoise`.
    pub(crate) seed: i32,
    /// σ of each real and imaginary draw, Jy.
    pub(crate) sigma_jy: f32,
    /// How many `corrupt` calls have applied the term.
    pub(crate) applications: u32,
}

/// A step that changes DATA, covering the scans observed before it.
#[derive(Debug, Clone)]
pub(crate) enum DataStep {
    /// `predict`: add the model's prediction. A replacing `predict` is the
    /// first step of the plan, so adding to the zero DATA replaces it.
    Predict {
        /// The prepared model.
        prediction: Arc<Prediction>,
        /// The step covers scans `0..scans`.
        scans: usize,
    },
    /// `predict(complist=…)`: add the component list's visibilities. A
    /// `predict` of an image and a list records both steps, the list's
    /// first, as CASA predicts its components first.
    PredictComponents {
        /// The prepared list.
        components: Arc<ComponentPrediction>,
        /// The step covers scans `0..scans`.
        scans: usize,
    },
    /// `corrupt`: add each application's noise.
    Corrupt {
        /// Its applications, one per noise term in the order the terms
        /// were set, as their range of the plan's noise.
        applications: Range<usize>,
        /// The step covers scans `0..scans`.
        scans: usize,
    },
}

impl DataStep {
    /// The step covers scans `0..scans()`.
    pub(crate) fn scans(&self) -> usize {
        match self {
            Self::Predict { scans, .. }
            | Self::PredictComponents { scans, .. }
            | Self::Corrupt { scans, .. } => *scans,
        }
    }
}

/// The session's DATA steps, in the order they were taken.
#[derive(Debug, Clone, Default)]
pub(crate) struct DataPlan {
    steps: Vec<DataStep>,
    /// Every corruption's applications, in the order they were taken.
    noise: Vec<NoiseApplication>,
}

impl DataPlan {
    /// Record a `predict` of `prediction` covering scans `0..scans`, after
    /// the steps already taken.
    pub(crate) fn predict(&mut self, prediction: Arc<Prediction>, scans: usize) {
        self.push(DataStep::Predict { prediction, scans });
    }

    /// Record a `predict` of the component list `components` covering scans
    /// `0..scans`, after the steps already taken.
    pub(crate) fn predict_components(
        &mut self,
        components: Arc<ComponentPrediction>,
        scans: usize,
    ) {
        self.push(DataStep::PredictComponents { components, scans });
    }

    /// Record a `corrupt` adding `applications` to scans `0..scans`, after
    /// the steps already taken.
    pub(crate) fn corrupt(
        &mut self,
        applications: impl IntoIterator<Item = NoiseApplication>,
        scans: usize,
    ) {
        let start = self.noise.len();
        self.noise.extend(applications);
        self.push(DataStep::Corrupt {
            applications: start..self.noise.len(),
            scans,
        });
    }

    fn push(&mut self, step: DataStep) {
        debug_assert!(
            self.steps
                .last()
                .is_none_or(|last| last.scans() <= step.scans()),
            "a step covers at least the scans of the one before it"
        );
        self.steps.push(step);
    }

    /// Forget every step: a replacing `predict` overwrites what they did.
    pub(crate) fn clear(&mut self) {
        self.steps.clear();
        self.noise.clear();
    }

    /// The steps covering scan `scan`, in the order they were taken: the
    /// last ones.
    pub(crate) fn steps_for_scan(&self, scan: usize) -> &[DataStep] {
        let first = self.steps.partition_point(|step| step.scans() <= scan);
        &self.steps[first..]
    }

    /// The noise to add to scan `scan`'s rows, in the order it was asked
    /// for: the applications of its corruptions, all of which add, which
    /// are the last ones taken.
    pub(crate) fn noise_for_scan(&self, scan: usize) -> &[NoiseApplication] {
        let start = self
            .steps_for_scan(scan)
            .iter()
            .find_map(|step| match step {
                DataStep::Corrupt { applications, .. } => Some(applications.start),
                DataStep::Predict { .. } | DataStep::PredictComponents { .. } => None,
            })
            .unwrap_or(self.noise.len());
        &self.noise[start..]
    }

    /// The predict steps covering scan `scan`, as indices of
    /// [`Self::predictions`], in the order they were taken: their
    /// predictions add. They are the last ones.
    pub(crate) fn predictions_for_scan(&self, scan: usize) -> Range<usize> {
        let all = self.predictions().count();
        let covering = self
            .steps_for_scan(scan)
            .iter()
            .filter(|step| matches!(step, DataStep::Predict { .. }))
            .count();
        all - covering..all
    }

    /// Every predict step's prepared model and the scans it covers, in the
    /// order they were taken.
    pub(crate) fn predictions(&self) -> impl Iterator<Item = (&Prediction, usize)> {
        self.steps.iter().filter_map(|step| match step {
            DataStep::Predict { prediction, scans } => Some((prediction.as_ref(), *scans)),
            DataStep::PredictComponents { .. } | DataStep::Corrupt { .. } => None,
        })
    }

    /// The component lists the steps covering scan `scan` predict, in the
    /// order they were taken: their visibilities add.
    pub(crate) fn components_for_scan(
        &self,
        scan: usize,
    ) -> impl Iterator<Item = &ComponentPrediction> {
        self.steps_for_scan(scan)
            .iter()
            .filter_map(|step| match step {
                DataStep::PredictComponents { components, .. } => Some(components.as_ref()),
                DataStep::Predict { .. } | DataStep::Corrupt { .. } => None,
            })
    }

    /// Whether any step predicts a component list.
    pub(crate) fn predicts_components(&self) -> bool {
        self.steps
            .iter()
            .any(|step| matches!(step, DataStep::PredictComponents { .. }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn noise(seed: i32, ordinal: u32) -> NoiseApplication {
        NoiseApplication {
            seed,
            sigma_jy: 0.5,
            ordinal,
        }
    }

    #[test]
    fn a_step_covers_only_the_scans_observed_before_it() {
        let mut plan = DataPlan::default();
        plan.corrupt([noise(1, 0)], 1);
        plan.corrupt([noise(1, 1), noise(2, 0)], 3);
        assert_eq!(
            plan.noise_for_scan(0),
            [noise(1, 0), noise(1, 1), noise(2, 0)]
        );
        assert_eq!(plan.noise_for_scan(2), [noise(1, 1), noise(2, 0)]);
        assert_eq!(plan.noise_for_scan(3), []);
        assert_eq!(plan.steps_for_scan(1).len(), 1);
        assert!(plan.predictions_for_scan(0).is_empty());
        plan.clear();
        assert_eq!(plan.noise_for_scan(0), []);
    }

    #[test]
    fn interleaved_corruptions_share_their_applications() {
        let mut plan = DataPlan::default();
        for scans in 1..=4 {
            plan.corrupt([noise(1, scans as u32 - 1)], scans);
        }
        for scan in 0..4 {
            let expected = (scan as u32..4).map(|ordinal| noise(1, ordinal));
            assert!(plan.noise_for_scan(scan).iter().copied().eq(expected));
        }
        // Each scan's noise is the end of the one list.
        let all = plan.noise_for_scan(0).as_ptr_range();
        assert_eq!(plan.noise_for_scan(3).as_ptr_range().end, all.end);
        assert_eq!(plan.noise_for_scan(4), []);
    }
}
