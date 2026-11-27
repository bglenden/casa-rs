// SPDX-License-Identifier: LGPL-3.0-or-later
//! Bind the shared virtual visibility predictor for the flagdata task.

use casa_core::Complex32;
use casa_flagging::FlagDataModelPredictor;
use casa_ms::{MeasurementSet, ResolvedMsSelectionRow};
use ndarray::{ArrayD, Axis};

use crate::solve::grouping::VisibilityModels;

/// Bind the Fourier and component recipes in the selected visibility buffers.
/// A malformed recipe is reported when the clipping agent reaches its buffer.
pub fn flagdata_model(
    ms: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
) -> Result<Box<dyn FlagDataModelPredictor>, String> {
    let models = VisibilityModels::for_flagdata(ms, rows).map_err(|error| error.to_string())?;
    Ok(Box::new(models))
}

impl FlagDataModelPredictor for VisibilityModels {
    fn predict(
        &self,
        ms: &MeasurementSet,
        row: usize,
        correlations: &[i32],
        flags: &ArrayD<bool>,
    ) -> Result<Option<ArrayD<Complex32>>, String> {
        if flags.ndim() != 2 {
            return Err(format!(
                "FLAG row {row} must have correlation and channel axes"
            ));
        }
        // The flagging iterator supplies the channel flag to the virtual
        // Fourier machine. Its flag is the union of the stored correlations.
        let mut channel_flags = None;
        for (index, channel) in flags.axis_iter(Axis(1)).enumerate() {
            if channel.iter().any(|&flag| flag) && channel.iter().any(|&flag| !flag) {
                channel_flags
                    .get_or_insert_with(|| flags.clone())
                    .index_axis_mut(Axis(1), index)
                    .fill(true);
            }
        }
        self.predict(
            ms,
            row,
            correlations,
            channel_flags.as_ref().unwrap_or(flags),
        )
        .map_err(|error| error.to_string())
    }
}
