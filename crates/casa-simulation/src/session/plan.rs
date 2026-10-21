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

use std::sync::Arc;

use super::prediction::Prediction;
use crate::corrupt::NoiseApplication;

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
    /// `corrupt`: add each application's noise.
    Corrupt {
        /// One per noise term, in the order the terms were set.
        applications: Vec<NoiseApplication>,
        /// The step covers scans `0..scans`.
        scans: usize,
    },
}

impl DataStep {
    /// The step covers scans `0..scans()`.
    pub(crate) fn scans(&self) -> usize {
        match self {
            Self::Predict { scans, .. } | Self::Corrupt { scans, .. } => *scans,
        }
    }
}

/// The session's DATA steps, in the order they were taken.
#[derive(Debug, Clone, Default)]
pub(crate) struct DataPlan {
    steps: Vec<DataStep>,
}

impl DataPlan {
    /// Record a step after those already taken.
    pub(crate) fn push(&mut self, step: DataStep) {
        self.steps.push(step);
    }

    /// Forget every step: a replacing `predict` overwrites what they did.
    pub(crate) fn clear(&mut self) {
        self.steps.clear();
    }

    /// The steps covering scan `scan`, in the order they were taken.
    pub(crate) fn steps_for_scan(&self, scan: usize) -> impl Iterator<Item = &DataStep> {
        self.steps.iter().filter(move |step| step.scans() > scan)
    }

    /// The noise to add to scan `scan`'s rows, in the order it was asked
    /// for: the fold of its corruptions, all of which add.
    pub(crate) fn noise_for_scan(&self, scan: usize) -> Vec<NoiseApplication> {
        self.steps_for_scan(scan)
            .flat_map(|step| match step {
                DataStep::Corrupt { applications, .. } => applications.as_slice(),
                DataStep::Predict { .. } => &[],
            })
            .copied()
            .collect()
    }

    /// The predict steps covering scan `scan`, as indices of
    /// [`Self::predictions`], in the order they were taken: their
    /// predictions add.
    pub(crate) fn predictions_for_scan(&self, scan: usize) -> Vec<usize> {
        self.predictions()
            .enumerate()
            .filter(|(_, (_, scans))| *scans > scan)
            .map(|(index, _)| index)
            .collect()
    }

    /// Every predict step's prepared model and the scans it covers, in the
    /// order they were taken.
    pub(crate) fn predictions(&self) -> impl Iterator<Item = (&Prediction, usize)> {
        self.steps.iter().filter_map(|step| match step {
            DataStep::Predict { prediction, scans } => Some((prediction.as_ref(), *scans)),
            DataStep::Corrupt { .. } => None,
        })
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
        plan.push(DataStep::Corrupt {
            applications: vec![noise(1, 0)],
            scans: 1,
        });
        plan.push(DataStep::Corrupt {
            applications: vec![noise(1, 1), noise(2, 0)],
            scans: 3,
        });
        assert_eq!(
            plan.noise_for_scan(0),
            vec![noise(1, 0), noise(1, 1), noise(2, 0)]
        );
        assert_eq!(plan.noise_for_scan(2), vec![noise(1, 1), noise(2, 0)]);
        assert_eq!(plan.noise_for_scan(3), Vec::new());
        assert_eq!(plan.steps_for_scan(1).count(), 1);
        assert!(plan.predictions_for_scan(0).is_empty());
        plan.clear();
        assert_eq!(plan.noise_for_scan(0), Vec::new());
    }
}
