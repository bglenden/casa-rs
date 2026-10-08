// SPDX-License-Identifier: LGPL-3.0-or-later
//! Visibilities written back in the final pass: the final model into
//! `MODEL_DATA` and the continuum-subtracted data into `CORRECTED_DATA`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use casa_imaging_model::{
    LogicalIdentity, MsColumnKind, ObservationSelection, ObservationSourceState,
    SequentialContinuumTransform,
};
use casa_imaging_reconstruction::ModelGenerationId;
use casa_imaging_runtime::pass::{NativeBlock, SourceError};
use casa_ms::{
    SelectedVisibilityWrite, SelectedVisibilityWriteGenerations, SelectedVisibilityWriteTargets,
};
use num_complex::Complex32;

/// Where the visibilities go: the selected MeasurementSet, the source state
/// its readers saw, the selection whose cells are written, and the columns.
#[derive(Clone)]
pub(crate) struct VisibilityWriteTarget {
    pub(crate) path: PathBuf,
    pub(crate) expected: ObservationSourceState,
    pub(crate) selection: Arc<ObservationSelection>,
    pub(crate) model_data: bool,
    pub(crate) corrected_data: bool,
}

/// An open in-place write of every selected sample.
pub(crate) struct VisibilityWriter<'a> {
    writer: SelectedVisibilityWrite,
    model_data: bool,
    corrected_data: Option<&'a SequentialContinuumTransform>,
    windows: BTreeMap<u32, u32>,
    samples: u64,
}

impl<'a> VisibilityWriter<'a> {
    /// Take the MeasurementSet's write lock and create the columns when
    /// absent (casa-ms owner rules). `CORRECTED_DATA` holds the output of
    /// `transform`, so it needs one.
    pub(crate) fn begin(
        target: &VisibilityWriteTarget,
        transform: Option<&'a SequentialContinuumTransform>,
    ) -> Result<Self, SourceError> {
        let corrected_data = match (target.corrected_data, transform) {
            (false, _) => None,
            (true, Some(transform)) => Some(transform),
            (true, None) => {
                return Err("CORRECTED_DATA is written only by a continuum transform".into());
            }
        };
        Ok(Self {
            writer: SelectedVisibilityWrite::begin(
                &target.path,
                &target.expected,
                &target.selection,
                SelectedVisibilityWriteTargets::new(target.model_data, target.corrected_data),
            )?,
            model_data: target.model_data,
            corrected_data,
            windows: target
                .selection
                .data_descriptions()
                .iter()
                .map(|selection| {
                    (
                        selection.data_description_id(),
                        selection.spectral_window_id(),
                    )
                })
                .collect(),
            samples: 0,
        })
    }

    /// Whether the write needs model predictions.
    pub(crate) const fn needs_predictions(&self) -> bool {
        self.model_data
    }

    /// Write one block: its predictions, `[row][channel][correlation]`, to
    /// `MODEL_DATA`, and its transformed values on the channels that reach
    /// the line output to `CORRECTED_DATA`.
    pub(crate) fn write(
        &mut self,
        block: &NativeBlock,
        predictions: &[Complex32],
    ) -> Result<(), SourceError> {
        let cells = block.channels() * block.correlations();
        for row in 0..block.len() {
            let header = block.header(row);
            let physical_row = header.address.physical_row;
            if self.model_data {
                let predicted = &predictions[row * cells..(row + 1) * cells];
                self.write_row(
                    MsColumnKind::ModelData,
                    block,
                    physical_row,
                    predicted,
                    |_| true,
                )?;
            }
            if let Some(transform) = self.corrected_data {
                let window = u32::try_from(header.address.data_description)
                    .ok()
                    .and_then(|description| self.windows.get(&description))
                    .ok_or("a written row's data description is not selected")?;
                let rule = transform.rule(header.context.field as i32, *window);
                let values = block.row(row).values;
                self.write_row(
                    MsColumnKind::CorrectedData,
                    block,
                    physical_row,
                    values,
                    |channel| {
                        rule.is_none_or(|rule| {
                            rule.channel_use(channel)
                                .is_some_and(|role| role.contributes_to_output())
                        })
                    },
                )?;
            }
        }
        Ok(())
    }

    fn write_row(
        &mut self,
        column: MsColumnKind,
        block: &NativeBlock,
        physical_row: u64,
        values: &[Complex32],
        mut written: impl FnMut(u32) -> bool,
    ) -> Result<(), SourceError> {
        let mut values = values.iter();
        for channel in block.channel_indices() {
            let output = written(*channel);
            for correlation in block.correlation_indices() {
                let value = *values
                    .next()
                    .ok_or("a block holds fewer values than samples")?;
                if output {
                    self.writer
                        .write(column, physical_row, *channel, *correlation, value)?;
                    self.samples += 1;
                }
            }
        }
        Ok(())
    }

    /// Flush, record `final_model` as `MODEL_DATA`'s generation and the
    /// transform contract as `CORRECTED_DATA`'s, and release the lock;
    /// returns the number of cells written.
    pub(crate) fn complete(self, final_model: ModelGenerationId) -> Result<u64, SourceError> {
        self.writer.complete(SelectedVisibilityWriteGenerations {
            model_data: self.model_data.then(|| final_model.identity()),
            corrected_data: self
                .corrected_data
                .map(|transform| LogicalIdentity::from_sha256(transform.contract_id().as_bytes())),
        })?;
        Ok(self.samples)
    }
}
