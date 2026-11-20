// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bind the shared virtual visibility predictor for the flagdata task.

use casa_ms::flagging::{FlagDataModelPredictor, FlaggingError};
use casa_ms::{MeasurementSet, ResolvedMsSelectionRow};
use casa_types::Complex32;
use ndarray::ArrayD;

use crate::solve::{GainSolveError, grouping::VisibilityModels};

/// Bind the Fourier and component recipes in the selected visibility buffers.
/// A malformed recipe is reported when the clipping agent reaches its buffer.
pub fn flagdata_model(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
) -> Result<Box<dyn FlagDataModelPredictor>, FlaggingError> {
    let models =
        VisibilityModels::for_flagdata(ms, rows).map_err(|error| model_error(ms, error))?;
    Ok(Box::new(models))
}

impl FlagDataModelPredictor for VisibilityModels {
    fn predict(
        &self,
        ms: &MeasurementSet,
        row: usize,
        correlations: &[i32],
        flags: &ArrayD<bool>,
    ) -> Result<Option<ArrayD<Complex32>>, FlaggingError> {
        self.predict(ms, row, correlations, flags)
            .map_err(|error| model_error(ms, error))
    }
}

fn model_error(ms: &MeasurementSet, error: GainSolveError) -> FlaggingError {
    FlaggingError::MutateFlags {
        path: ms
            .path()
            .map_or_else(|| "<in-memory>".into(), |path| path.display().to_string()),
        reason: format!("read clipping model: {error}"),
    }
}
