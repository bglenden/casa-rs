// SPDX-License-Identifier: LGPL-3.0-or-later
//! Corrected-data MS export for iterative self-calibration workflows.

use std::fs;
use std::path::{Path, PathBuf};

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetCreateTarget,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, MsError, MsTransformRequest,
    TransformDataColumn, maximum_visibility_cell_elements, mstransform, selection::MsSelection,
};
use casa_tables::TableError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::session::resolve_calibration_selection;

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
    let target =
        MeasurementSetCreateTarget::prepare(&request.output_ms, true).map_err(|error| {
            ExportCorrectedDataError::PrepareOutput {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            }
        })?;
    let mut physical_request = request.clone();
    physical_request.output_ms = target.staging_path().to_path_buf();
    let mut report = export_corrected_data_staged(&physical_request)?;
    target
        .commit()
        .map_err(|error| ExportCorrectedDataError::PrepareOutput {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    report.output_ms = request.output_ms.clone();
    Ok(report)
}

fn export_corrected_data_staged(
    request: &ExportCorrectedDataRequest,
) -> Result<ExportCorrectedDataReport, ExportCorrectedDataError> {
    let ms = MeasurementSet::open(&request.input_ms).map_err(|source| {
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

    let selected_rows = resolve_calibration_selection(&ms, &request.selection)
        .map_err(|error| ExportCorrectedDataError::SelectRows {
            path: request.input_ms.display().to_string(),
            source: match error {
                casa_ms::MsSelectionError::Domain(source) => source,
                other => MsError::InvalidInput(other.to_string()),
            },
        })?
        .row_indices()
        .collect::<Vec<_>>();
    if selected_rows.is_empty() {
        return Err(ExportCorrectedDataError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    if selected_rows.len() == ms.row_count() {
        let row_count = selected_rows.len();
        drop(ms);
        export_all_corrected_data_by_copy(&request.input_ms, &request.output_ms)?;
        return Ok(ExportCorrectedDataReport {
            input_ms: request.input_ms.clone(),
            output_ms: request.output_ms.clone(),
            row_count,
            source_column: VisibilityDataColumn::CorrectedData.name().to_string(),
            output_column: VisibilityDataColumn::Data.name().to_string(),
        });
    }

    drop(ms);
    let transformed = mstransform(&MsTransformRequest {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        spw: String::new(),
        width: 1,
        data_column: TransformDataColumn::CorrectedData,
        selection: request.selection.clone(),
        keep_flags: true,
    })
    .map_err(|error| ExportCorrectedDataError::SaveMeasurementSet {
        path: request.output_ms.display().to_string(),
        source: MsError::InvalidInput(error.to_string()),
    })?;

    Ok(ExportCorrectedDataReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count: transformed.row_count,
        source_column: VisibilityDataColumn::CorrectedData.name().to_string(),
        output_column: VisibilityDataColumn::Data.name().to_string(),
    })
}

fn export_all_corrected_data_by_copy(
    input_ms: &Path,
    output_ms: &Path,
) -> Result<(), ExportCorrectedDataError> {
    prepare_output_root(output_ms)?;
    copy_dir_recursive(input_ms, output_ms).map_err(|error| {
        ExportCorrectedDataError::PrepareOutput {
            path: output_ms.display().to_string(),
            reason: error.to_string(),
        }
    })?;
    let mut output = MeasurementSet::open(output_ms).map_err(|source| {
        ExportCorrectedDataError::OpenMeasurementSet {
            path: output_ms.display().to_string(),
            source,
        }
    })?;
    let row_indices = (0..output.row_count()).collect::<Vec<_>>();
    let maximum_data_bytes = maximum_visibility_cell_elements(&output)
        .and_then(|elements| {
            elements
                .checked_mul(8)
                .ok_or(casa_ms::MeasurementSetWriteError::ByteOverflow)
        })
        .map_err(|error| ExportCorrectedDataError::MutateMeasurementSet {
            path: output_ms.display().to_string(),
            source: Box::new(TableError::Storage(error.to_string())),
        })?;
    let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(|error| {
        ExportCorrectedDataError::MutateMeasurementSet {
            path: output_ms.display().to_string(),
            source: Box::new(TableError::Storage(error.to_string())),
        }
    })?;
    let write_plan = MeasurementSetWritePlan::selected_row_mutation(
        row_indices,
        vec![MeasurementSetWriteColumnPlan {
            name: VisibilityDataColumn::Data.name().to_string(),
            bytes_per_row: maximum_data_bytes,
            mode: MeasurementSetColumnWriteMode::Replace,
            storage_manager: MeasurementSetColumnStorage::Persisted,
            tile_shape: None,
            create_source_column: None,
        }],
        resources,
    )
    .map_err(|error| ExportCorrectedDataError::MutateMeasurementSet {
        path: output_ms.display().to_string(),
        source: Box::new(TableError::Storage(error.to_string())),
    })?;
    let mut write_session =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut output, write_plan).map_err(
            |error| ExportCorrectedDataError::MutateMeasurementSet {
                path: output_ms.display().to_string(),
                source: Box::new(TableError::Storage(error.to_string())),
            },
        )?;
    loop {
        let rows = write_session
            .next_mutation_rows()
            .map_err(|error| ExportCorrectedDataError::MutateMeasurementSet {
                path: output_ms.display().to_string(),
                source: Box::new(TableError::Storage(error.to_string())),
            })?
            .to_vec();
        if rows.is_empty() {
            break;
        }
        let corrected = output
            .main_table()
            .column_accessor(VisibilityDataColumn::CorrectedData.name())
            .and_then(|column| column.array_cells_owned(&rows))
            .map_err(|source| ExportCorrectedDataError::MutateMeasurementSet {
                path: output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let mut values = Vec::with_capacity(rows.len());
        for (&row, corrected) in rows.iter().zip(corrected) {
            let corrected =
                corrected.ok_or_else(|| ExportCorrectedDataError::MutateMeasurementSet {
                    path: output_ms.display().to_string(),
                    source: Box::new(TableError::ColumnNotFound {
                        row_index: row,
                        column: VisibilityDataColumn::CorrectedData.name().to_string(),
                    }),
                })?;
            if corrected.is_empty() {
                values.push(
                    output
                        .main_table()
                        .column_accessor(VisibilityDataColumn::Data.name())
                        .and_then(|column| column.array_cell(row).cloned())
                        .map_err(|source| ExportCorrectedDataError::MutateMeasurementSet {
                            path: output_ms.display().to_string(),
                            source: Box::new(source),
                        })?,
                );
            } else {
                values.push(corrected);
            }
        }
        write_session
            .write_mutation_batch(
                &mut output,
                MeasurementSetMutationBatch {
                    row_indices: rows,
                    columns: vec![MeasurementSetMutationColumnBatch {
                        name: VisibilityDataColumn::Data.name().to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(values),
                    }],
                },
            )
            .map_err(|error| ExportCorrectedDataError::MutateMeasurementSet {
                path: output_ms.display().to_string(),
                source: Box::new(TableError::Storage(error.to_string())),
            })?;
    }
    write_session.finish_mutation().map_err(|error| {
        ExportCorrectedDataError::MutateMeasurementSet {
            path: output_ms.display().to_string(),
            source: Box::new(TableError::Storage(error.to_string())),
        }
    })?;
    Ok(())
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

fn copy_dir_recursive(source: &Path, destination: &Path) -> std::io::Result<()> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_dir_recursive(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path)?;
        } else if file_type.is_symlink() {
            let target = fs::read_link(&source_path)?;
            std::os::unix::fs::symlink(target, destination_path)?;
        }
    }
    Ok(())
}
