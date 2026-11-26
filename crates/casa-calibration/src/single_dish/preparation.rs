// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native single-dish spectral preparation shared by the task and tool.

pub(crate) mod atmosphere;
pub(crate) mod baseline;
pub(crate) mod import;
pub(crate) mod line_fit;
mod smoothing;

use std::path::{Path, PathBuf};

use casa_contracts::CasaValue;
use casa_ms::MsSelection;
use casa_ms::transform::{
    MsTransformReport, MsTransformRequest, TransformDataColumn, TransformTaskHistory,
    convolve_spectra,
};
use casa_task_runtime::{TaskFailure, TaskInvocation, TaskSuccess};

use super::selector;
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

fn data_column(name: &str) -> Result<TransformDataColumn, String> {
    match name.to_ascii_lowercase().as_str() {
        "data" => Ok(TransformDataColumn::Data),
        "float_data" => Ok(TransformDataColumn::FloatData),
        "corrected" => Ok(TransformDataColumn::CorrectedData),
        _ => Err("Invalid data column name".into()),
    }
}

pub(crate) fn run_sdsmooth(task: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
    let mut session = SingleDishSession::default();
    session.open(task.input_path("infile")?);
    session
        .set_selection(
            MsSelection {
                antenna: selector(task, "antenna")?,
                field: selector(task, "field")?,
                spw: selector(task, "spw")?,
                timerange: selector(task, "timerange")?,
                scan: selector(task, "scan")?,
                correlation: selector(task, "pol")?,
                intent: selector(task, "intent")?,
                ..MsSelection::default()
            },
            task.flag("reindex")?,
        )
        .map_err(TaskFailure::execution)?;
    let outfile = task.text("outfile")?.unwrap_or_default();
    let overwrite = task.flag("overwrite")?;
    if outfile.is_empty() {
        return Err(TaskFailure::validation("outfile is empty.").at("outfile"));
    }
    let output = Path::new(&outfile);
    if output.exists() && !overwrite {
        return Err(TaskFailure::execution(format!("{outfile} exists.")).at("outfile"));
    }
    let report = session
        .smooth(SingleDishSmoothing {
            kernel: &task.text("kernel")?.unwrap_or_else(|| "gaussian".into()),
            width: task.integer("kwidth")? as f32,
            datacolumn: &task.text("datacolumn")?.unwrap_or_else(|| "data".into()),
            output,
            overwrite,
            history: Some(TransformTaskHistory {
                task: "sdsmooth".into(),
                parameters: Vec::new(),
            }),
        })
        .map_err(TaskFailure::execution)?;
    TaskSuccess::new(CasaValue::None, &report)
}
