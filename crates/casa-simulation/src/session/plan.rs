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
//! Phase 1d's steps add noise; the fold applies each step to the running
//! DATA (`D ← step(D)`), so prediction (`D ← P`, or `D ← D + P` when
//! incremental) and multiplicative gains (`D ← G(D)`) slot in as further
//! [`DataStep`] variants.

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
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DataStep {
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
            Self::Corrupt { scans, .. } => *scans,
        }
    }
}

/// The session's DATA steps, in the order they were taken.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct DataPlan {
    steps: Vec<DataStep>,
}

impl DataPlan {
    /// Record a step after those already taken.
    pub(crate) fn push(&mut self, step: DataStep) {
        self.steps.push(step);
    }

    /// The steps covering scan `scan`, in the order they were taken.
    pub(crate) fn steps_for_scan(&self, scan: usize) -> impl Iterator<Item = &DataStep> {
        self.steps.iter().filter(move |step| step.scans() > scan)
    }

    /// The noise to add to scan `scan`'s rows, in the order it was asked
    /// for: the fold of its steps, all of which add.
    pub(crate) fn noise_for_scan(&self, scan: usize) -> Vec<NoiseApplication> {
        self.steps_for_scan(scan)
            .flat_map(|step| match step {
                DataStep::Corrupt { applications, .. } => applications.iter().copied(),
            })
            .collect()
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
    }
}
