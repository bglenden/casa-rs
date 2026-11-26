// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native single-dish spectral preparation shared by the task and tool.

pub mod atmosphere;
pub mod baseline;
pub mod import;
pub mod line_fit;
mod smoothing;

use std::path::{Path, PathBuf};

use casa_ms::MsSelection;
use casa_ms::transform::{
    MsTransformReport, MsTransformRequest, TransformDataColumn, TransformTaskHistory,
    convolve_spectra,
};

use smoothing::Smoothing;

/// Single-dish tool state. The path and selection are resolved by the operation.
#[derive(Default)]
pub struct SingleDishSession {
    name: Option<PathBuf>,
    selection: MsSelection,
    reindex: Option<bool>,
    line_averaging: line_fit::LineAveraging,
}

/// Parameters of a selected single-dish convolution.
pub struct SingleDishSmoothing<'a> {
    /// CASA kernel name.
    pub kernel: &'a str,
    /// Width in channels. The Gaussian width is its FWHM.
    pub width: f32,
    /// CASA input data column name.
    pub datacolumn: &'a str,
    /// Destination MeasurementSet.
    pub output: &'a Path,
    /// Allow replacement of a previously published MeasurementSet.
    pub overwrite: bool,
    /// Task provenance. Direct tool calls do not add a task HISTORY entry.
    pub history: Option<TransformTaskHistory>,
}

impl SingleDishSession {
    /// Set the path for future operations and discard any previous selection.
    pub fn open(&mut self, name: impl Into<PathBuf>) -> bool {
        *self = Self {
            name: Some(name.into()),
            ..Self::default()
        };
        true
    }

    /// Detach the current MeasurementSet, if any.
    pub fn close(&mut self) -> bool {
        *self = Self::default();
        true
    }

    /// Return the path as supplied to `open`.
    pub fn name(&self) -> Result<&Path, &'static str> {
        self.name
            .as_deref()
            .ok_or("No MeasurementSet has been assigned, please run open.")
    }

    /// Replace the previous selection. Selectors are evaluated during processing.
    pub fn set_selection(
        &mut self,
        selection: MsSelection,
        reindex: bool,
    ) -> Result<bool, &'static str> {
        self.name()?;
        self.selection = MsSelection {
            selectdata: true,
            ..selection
        };
        self.reindex = Some(reindex);
        Ok(true)
    }

    /// Apply a convolution to the current selection and publish its MeasurementSet.
    pub fn smooth(&self, operation: SingleDishSmoothing<'_>) -> Result<MsTransformReport, String> {
        let input = self.name()?;
        let data_column = data_column(operation.datacolumn)?;
        let mut smoothing = Smoothing::new(operation.kernel, operation.width)?;
        let mut options = self.line_averaging.operation_options()?;
        options.history = operation.history;
        convolve_spectra(
            &MsTransformRequest {
                input_ms: input.to_path_buf(),
                output_ms: operation.output.to_path_buf(),
                spw: self.selection.spw.clone().unwrap_or_default(),
                width: 1,
                data_column,
                selection: self.selection.clone(),
                keep_flags: true,
            },
            &options,
            self.reindex.unwrap_or(true),
            operation.overwrite,
            &mut smoothing,
        )
        .map_err(|error| error.to_string())
    }
}

/// Map a CASA data column string to `TransformDataColumn`.
pub fn data_column(name: &str) -> Result<TransformDataColumn, String> {
    match name.to_ascii_lowercase().as_str() {
        "data" => Ok(TransformDataColumn::Data),
        "float_data" => Ok(TransformDataColumn::FloatData),
        "corrected" => Ok(TransformDataColumn::CorrectedData),
        _ => Err("Invalid data column name".into()),
    }
}
