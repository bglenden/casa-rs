// SPDX-License-Identifier: LGPL-3.0-or-later
//! Corrected-data MS export for iterative self-calibration workflows.

use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::{MsError, selection::MsSelection};
use casa_tables::TableError;
use casa_types::Value;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Request to materialize an MS with `CORRECTED_DATA` copied into `DATA`.
#[derive(Debug, Clone)]
pub struct ExportCorrectedDataRequest {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// Optional row selection to materialize in the output MS.
    pub selection: MsSelection,
}

/// Report returned after exporting corrected data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ExportCorrectedDataReport {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// Number of MAIN rows copied.
    pub row_count: usize,
    /// Source column copied into output `DATA`.
    pub source_column: String,
    /// Output visibility data column populated for later imaging.
    pub output_column: String,
}

/// Errors returned by corrected-data export.
#[derive(Debug, Error)]
pub enum ExportCorrectedDataError {
    /// Opening the input MS failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path that was being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// The input lacks `CORRECTED_DATA`.
    #[error("MeasurementSet {path} does not contain CORRECTED_DATA")]
    MissingCorrectedData {
        /// Input MS path.
        path: String,
    },

    /// The selection could not be evaluated.
    #[error("failed to select rows from MeasurementSet {path}: {source}")]
    SelectRows {
        /// Input MS path.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// The selection produced no rows.
    #[error("corrected-data export selection produced no rows for {path}")]
    EmptySelection {
        /// Input MS path.
        path: String,
    },

    /// Input and output roots are the same path.
    #[error("input and output MeasurementSet paths must differ: {path}")]
    SameInputOutput {
        /// Duplicated path.
        path: String,
    },

    /// A MAIN-table mutation failed.
    #[error("failed to copy CORRECTED_DATA into DATA for {path}: {source}")]
    MutateMeasurementSet {
        /// Input MS path.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Filesystem preparation for the output root failed.
    #[error("failed to prepare output MeasurementSet {path}: {reason}")]
    PrepareOutput {
        /// Output path.
        path: String,
        /// Error context.
        reason: String,
    },

    /// Saving the output MS failed.
    #[error("failed to save MeasurementSet {path}: {source}")]
    SaveMeasurementSet {
        /// Output path.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },
}

/// Materialize an output MS whose `DATA` column contains the input `CORRECTED_DATA`.
pub fn export_corrected_data(
    request: &ExportCorrectedDataRequest,
) -> Result<ExportCorrectedDataReport, ExportCorrectedDataError> {
    if request.input_ms == request.output_ms {
        return Err(ExportCorrectedDataError::SameInputOutput {
            path: request.input_ms.display().to_string(),
        });
    }

    let mut ms = MeasurementSet::open(&request.input_ms).map_err(|source| {
        ExportCorrectedDataError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
    if !ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(VisibilityDataColumn::CorrectedData.name()))
    {
        return Err(ExportCorrectedDataError::MissingCorrectedData {
            path: request.input_ms.display().to_string(),
        });
    }

    let selected_rows =
        request
            .selection
            .apply(&ms)
            .map_err(|source| ExportCorrectedDataError::SelectRows {
                path: request.input_ms.display().to_string(),
                source,
            })?;
    if selected_rows.is_empty() {
        return Err(ExportCorrectedDataError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }

    for &row_index in &selected_rows {
        let corrected = ms
            .main_table()
            .cell_accessor(row_index, VisibilityDataColumn::CorrectedData.name())
            .and_then(|cell| cell.array())
            .map_err(|source| ExportCorrectedDataError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })?
            .clone();
        ms.main_table_mut()
            .cell_accessor_mut(row_index, VisibilityDataColumn::Data.name())
            .and_then(|mut cell| cell.set(Value::Array(corrected)))
            .map_err(|source| ExportCorrectedDataError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })?;
    }

    if selected_rows.len() != ms.row_count() {
        let mut keep = vec![false; ms.row_count()];
        for &row_index in &selected_rows {
            keep[row_index] = true;
        }
        let rows_to_remove = keep
            .into_iter()
            .enumerate()
            .filter_map(|(row_index, keep)| (!keep).then_some(row_index))
            .collect::<Vec<_>>();
        ms.main_table_mut()
            .remove_rows(&rows_to_remove)
            .map_err(|source| ExportCorrectedDataError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })?;
    }

    prepare_output_root(&request.output_ms)?;
    ms.save_as_assuming_valid(&request.output_ms)
        .map_err(|source| ExportCorrectedDataError::SaveMeasurementSet {
            path: request.output_ms.display().to_string(),
            source,
        })?;

    Ok(ExportCorrectedDataReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count: selected_rows.len(),
        source_column: VisibilityDataColumn::CorrectedData.name().to_string(),
        output_column: VisibilityDataColumn::Data.name().to_string(),
    })
}

fn prepare_output_root(path: &Path) -> Result<(), ExportCorrectedDataError> {
    if path.exists() {
        fs::remove_dir_all(path)
            .or_else(|_| fs::remove_file(path))
            .map_err(|error| ExportCorrectedDataError::PrepareOutput {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| ExportCorrectedDataError::PrepareOutput {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    Ok(())
}
