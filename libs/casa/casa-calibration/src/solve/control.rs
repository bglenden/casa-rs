// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cooperative control for calculation and private calibration-table staging.

use casa_core::cancellation::Cancellation;

use super::GainSolveError;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SolveControl<'a>(pub(crate) Option<&'a Cancellation>);

impl SolveControl<'_> {
    pub(crate) fn is_cancelled(self) -> bool {
        self.0.is_some_and(Cancellation::is_cancelled)
    }

    pub(crate) fn check(self) -> Result<(), GainSolveError> {
        if self.is_cancelled() {
            return Err(GainSolveError::Interrupted);
        }
        Ok(())
    }

    #[cfg(feature = "solve-test-hooks")]
    pub(crate) fn observe(self, checkpoint: crate::solve_test_hooks::Checkpoint) {
        if self.0.is_some() {
            crate::solve_test_hooks::observe(checkpoint);
        }
    }
}
