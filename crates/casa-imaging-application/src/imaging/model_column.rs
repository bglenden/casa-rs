// SPDX-License-Identifier: LGPL-3.0-or-later
//! The final model visibilities written into `MODEL_DATA`.

use std::path::PathBuf;
use std::sync::Arc;

use casa_imaging_model::{MsColumnKind, ObservationSelection, ObservationSourceState};
use casa_imaging_reconstruction::ModelGenerationId;
use casa_imaging_runtime::pass::{NativeBlock, SourceError};
use casa_ms::{
    SelectedVisibilityWrite, SelectedVisibilityWriteGenerations, SelectedVisibilityWriteTargets,
};
use num_complex::Complex32;

/// Where the model column goes: the selected MeasurementSet, the source
/// state its readers saw, and the selection whose cells are written.
#[derive(Clone)]
pub(crate) struct ModelColumnTarget {
    pub(crate) path: PathBuf,
    pub(crate) expected: ObservationSourceState,
    pub(crate) selection: Arc<ObservationSelection>,
}

/// An open in-place `MODEL_DATA` write of every selected sample.
pub(crate) struct ModelColumnWriter {
    writer: SelectedVisibilityWrite,
    samples: u64,
}

impl ModelColumnWriter {
    /// Take the MeasurementSet's write lock and create `MODEL_DATA` when
    /// absent (casa-ms owner rules).
    pub(crate) fn begin(target: &ModelColumnTarget) -> Result<Self, SourceError> {
        Ok(Self {
            writer: SelectedVisibilityWrite::begin(
                &target.path,
                &target.expected,
                &target.selection,
                SelectedVisibilityWriteTargets::new(true, false),
            )?,
            samples: 0,
        })
    }

    /// Write one block's predictions, `[row][channel][correlation]`.
    pub(crate) fn write(
        &mut self,
        block: &NativeBlock,
        predictions: &[Complex32],
    ) -> Result<(), SourceError> {
        let mut values = predictions.iter();
        for row in 0..block.len() {
            let physical_row = block.header(row).address.physical_row;
            for channel in block.channel_indices() {
                for correlation in block.correlation_indices() {
                    let value = *values
                        .next()
                        .ok_or("a block holds fewer predictions than samples")?;
                    self.writer.write(
                        MsColumnKind::ModelData,
                        physical_row,
                        *channel,
                        *correlation,
                        value,
                    )?;
                    self.samples += 1;
                }
            }
        }
        Ok(())
    }

    /// Flush, record `final_model` as the column's generation and release
    /// the lock; returns the number of samples written.
    pub(crate) fn complete(self, final_model: ModelGenerationId) -> Result<u64, SourceError> {
        self.writer.complete(SelectedVisibilityWriteGenerations {
            model_data: Some(final_model.identity()),
            corrected_data: None,
        })?;
        Ok(self.samples)
    }
}
