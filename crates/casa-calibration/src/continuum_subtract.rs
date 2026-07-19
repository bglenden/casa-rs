// SPDX-License-Identifier: LGPL-3.0-or-later
//! UV continuum subtraction for line-imaging workflows.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::main_table::VisibilityDataColumn;
use casa_ms::selection::MsSelection;
use casa_ms::selection::syntax::{ChannelSelection, parse_spw_selector};
use casa_ms::{
    MeasurementSetColumnStorage, MeasurementSetColumnWriteMode, MeasurementSetCreateTarget,
    MeasurementSetMutationBatch, MeasurementSetMutationColumnBatch,
    MeasurementSetMutationColumnValues, MeasurementSetWriteColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, MsError, MsTransformRequest,
    TransformDataColumn, maximum_visibility_cell_elements, mstransform,
};
use casa_tables::TableError;
use casa_types::{ArrayValue, ScalarValue};
use num_complex::Complex32;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::session::resolve_calibration_selection;
use casa_ms::least_squares::solve_weighted_least_squares;

/// Input visibility column used for continuum subtraction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ContinuumSubtractionDataColumn {
    /// MAIN.DATA.
    Data,
    /// MAIN.CORRECTED_DATA.
    #[default]
    CorrectedData,
}

impl ContinuumSubtractionDataColumn {
    fn visibility_column(self) -> VisibilityDataColumn {
        match self {
            Self::Data => VisibilityDataColumn::Data,
            Self::CorrectedData => VisibilityDataColumn::CorrectedData,
        }
    }

    fn name(self) -> &'static str {
        self.visibility_column().name()
    }
}

/// Request to produce a continuum-subtracted MeasurementSet.
#[derive(Debug, Clone)]
pub struct ContinuumSubtractionRequest {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// CASA-style line-free channel selector, e.g. `0:0~500;900~1919`.
    pub fit_spw: String,
    /// Polynomial order fitted independently to real and imaginary visibilities.
    pub fit_order: usize,
    /// Input data column to subtract.
    pub data_column: ContinuumSubtractionDataColumn,
    /// Optional row selection to materialize in the output MS.
    pub selection: MsSelection,
}

/// Report returned after UV continuum subtraction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ContinuumSubtractionReport {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// Number of MAIN rows selected for output.
    pub row_count: usize,
    /// Number of rows with at least one fitted correlation.
    pub fitted_row_count: usize,
    /// Number of per-correlation fits that had too few unflagged line-free samples.
    pub skipped_fit_count: usize,
    /// Source column read from the input MS.
    pub source_column: String,
    /// Output visibility data column populated for line imaging.
    pub output_column: String,
    /// CASA-style line-free channel selector used for fitting.
    pub fit_spw: String,
    /// Polynomial order used for the row/correlation fits.
    pub fit_order: usize,
    /// Spectral-window ids touched by the request.
    pub spectral_window_ids: Vec<i32>,
    /// End-to-end runtime in nanoseconds.
    pub elapsed_ns: u64,
}

/// Errors returned by UV continuum subtraction.
#[derive(Debug, Error)]
pub enum ContinuumSubtractionError {
    /// Opening an MS failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Input and output roots are identical.
    #[error("input and output MeasurementSet paths must differ: {path}")]
    SameInputOutput {
        /// Duplicated path.
        path: String,
    },

    /// The requested data column is absent.
    #[error("MeasurementSet {path} does not contain {column}")]
    MissingDataColumn {
        /// Input MS path.
        path: String,
        /// Missing column.
        column: String,
    },

    /// The row selection failed.
    #[error("failed to select rows from MeasurementSet {path}: {source}")]
    SelectRows {
        /// Input MS path.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// The row selection was empty.
    #[error("continuum-subtraction selection produced no rows for {path}")]
    EmptySelection {
        /// Input MS path.
        path: String,
    },

    /// Parsing or resolving `fit_spw` failed.
    #[error("invalid line-free channel selection {selector:?}: {reason}")]
    InvalidFitSpw {
        /// CASA-style selector.
        selector: String,
        /// Human-readable reason.
        reason: String,
    },

    /// Required spectral metadata is missing or inconsistent.
    #[error("missing or invalid spectral metadata in {path}: {reason}")]
    SpectralMetadata {
        /// Input MS path.
        path: String,
        /// Human-readable reason.
        reason: String,
    },

    /// MAIN-table data/flag mutation failed.
    #[error("failed to continuum-subtract MAIN data for {path}: {source}")]
    MutateMeasurementSet {
        /// MS path.
        path: String,
        /// Underlying table error.
        #[source]
        source: Box<TableError>,
    },

    /// Output filesystem preparation failed.
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

/// Create a continuum-subtracted MS whose output `DATA` column contains line residuals.
pub fn continuum_subtract(
    request: &ContinuumSubtractionRequest,
) -> Result<ContinuumSubtractionReport, ContinuumSubtractionError> {
    if request.input_ms == request.output_ms {
        return Err(ContinuumSubtractionError::SameInputOutput {
            path: request.input_ms.display().to_string(),
        });
    }
    let target =
        MeasurementSetCreateTarget::prepare(&request.output_ms, true).map_err(|error| {
            ContinuumSubtractionError::PrepareOutput {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            }
        })?;
    let mut physical_request = request.clone();
    physical_request.output_ms = target.staging_path().to_path_buf();
    let mut report = continuum_subtract_staged(&physical_request)?;
    target
        .commit()
        .map_err(|error| ContinuumSubtractionError::PrepareOutput {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    report.output_ms = request.output_ms.clone();
    Ok(report)
}

fn continuum_subtract_staged(
    request: &ContinuumSubtractionRequest,
) -> Result<ContinuumSubtractionReport, ContinuumSubtractionError> {
    let started_at = Instant::now();
    if request.fit_order > 3 {
        return Err(ContinuumSubtractionError::InvalidFitSpw {
            selector: request.fit_spw.clone(),
            reason: "fit_order above 3 is not supported by the initial uvcontsub path".to_string(),
        });
    }

    let input = MeasurementSet::open(&request.input_ms).map_err(|source| {
        ContinuumSubtractionError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
    let source_column = request.data_column.visibility_column();
    if !input
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(source_column.name()))
    {
        return Err(ContinuumSubtractionError::MissingDataColumn {
            path: request.input_ms.display().to_string(),
            column: source_column.name().to_string(),
        });
    }
    let input_row_count = input.row_count();
    let selected_rows = resolve_calibration_selection(&input, &request.selection)
        .map_err(|error| ContinuumSubtractionError::SelectRows {
            path: request.input_ms.display().to_string(),
            source: match error {
                casa_ms::MsSelectionError::Domain(source) => source,
                other => MsError::InvalidInput(other.to_string()),
            },
        })?
        .row_indices()
        .collect::<Vec<_>>();
    if selected_rows.is_empty() {
        return Err(ContinuumSubtractionError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    let fit_channels_by_spw = resolve_fit_channels(&input, &request.fit_spw)?;
    drop(input);

    let full_selection = selected_rows.len() == input_row_count;
    if full_selection {
        prepare_output_root(&request.output_ms)?;
        copy_dir_recursive(&request.input_ms, &request.output_ms).map_err(|error| {
            ContinuumSubtractionError::PrepareOutput {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            }
        })?;
    } else {
        mstransform(&MsTransformRequest {
            input_ms: request.input_ms.clone(),
            output_ms: request.output_ms.clone(),
            spw: String::new(),
            width: 1,
            data_column: match request.data_column {
                ContinuumSubtractionDataColumn::Data => TransformDataColumn::Data,
                ContinuumSubtractionDataColumn::CorrectedData => TransformDataColumn::CorrectedData,
            },
            selection: request.selection.clone(),
            keep_flags: true,
        })
        .map_err(|error| ContinuumSubtractionError::SaveMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: MsError::InvalidInput(error.to_string()),
        })?;
    }

    let mut output = MeasurementSet::open(&request.output_ms).map_err(|source| {
        ContinuumSubtractionError::OpenMeasurementSet {
            path: request.output_ms.display().to_string(),
            source,
        }
    })?;
    let processing_source_column = if full_selection {
        source_column
    } else {
        VisibilityDataColumn::Data
    };
    let output_ddid_to_spw = data_description_spw_map(&output)?;
    let row_indices = (0..output.row_count()).collect::<Vec<_>>();
    let maximum_data_bytes = maximum_visibility_cell_elements(&output)
        .and_then(|elements| {
            elements
                .checked_mul(8)
                .ok_or(casa_ms::MeasurementSetWriteError::ByteOverflow)
        })
        .map_err(|error| ContinuumSubtractionError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(TableError::Storage(error.to_string())),
        })?;
    let resources = MeasurementSetWriteResources::from_system_memory(2).map_err(|error| {
        ContinuumSubtractionError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
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
    .map_err(|error| ContinuumSubtractionError::MutateMeasurementSet {
        path: request.output_ms.display().to_string(),
        source: Box::new(TableError::Storage(error.to_string())),
    })?;
    let mut write_session =
        MeasurementSetWriteSession::start_selected_row_mutation(&mut output, write_plan).map_err(
            |error| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(TableError::Storage(error.to_string())),
            },
        )?;

    let mut fitted_row_count = 0usize;
    let mut skipped_fit_count = 0usize;
    let mut touched_spws = BTreeSet::new();
    loop {
        let rows = write_session
            .next_mutation_rows()
            .map_err(|error| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(TableError::Storage(error.to_string())),
            })?
            .to_vec();
        if rows.is_empty() {
            break;
        }
        let data_values = output
            .main_table()
            .column_accessor(processing_source_column.name())
            .and_then(|column| column.array_cells_owned(&rows))
            .map_err(|source| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let flag_values = output
            .main_table()
            .column_accessor("FLAG")
            .and_then(|column| column.array_cells_owned(&rows))
            .map_err(|source| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let weight_values = output
            .main_table()
            .column_accessor("WEIGHT")
            .and_then(|column| column.array_cells_owned(&rows))
            .map_err(|source| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let weight_spectrum_values = if output
            .main_table()
            .schema()
            .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"))
        {
            Some(
                output
                    .main_table()
                    .column_accessor("WEIGHT_SPECTRUM")
                    .and_then(|column| column.array_cells_owned(&rows))
                    .map_err(|source| ContinuumSubtractionError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    })?,
            )
        } else {
            None
        };
        let flag_row_values = output
            .main_table()
            .column_accessor("FLAG_ROW")
            .and_then(|column| column.scalar_cells_owned_for_rows(&rows))
            .map_err(|source| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let data_desc_ids = output
            .main_table()
            .column_accessor("DATA_DESC_ID")
            .and_then(|column| column.scalar_cells_owned_for_rows(&rows))
            .map_err(|source| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let mut weight_spectrum_values = weight_spectrum_values.map(Vec::into_iter);
        let mut residuals = Vec::with_capacity(rows.len());
        for (((((row_index, data), flags), weights), flag_row), ddid) in rows
            .iter()
            .copied()
            .zip(data_values)
            .zip(flag_values)
            .zip(weight_values)
            .zip(flag_row_values)
            .zip(data_desc_ids)
        {
            let data = data.ok_or_else(|| {
                missing_column_error(
                    &request.output_ms,
                    row_index,
                    processing_source_column.name(),
                )
            })?;
            let flags =
                flags.ok_or_else(|| missing_column_error(&request.output_ms, row_index, "FLAG"))?;
            let weights = weights
                .ok_or_else(|| missing_column_error(&request.output_ms, row_index, "WEIGHT"))?;
            let weight_spectrum = weight_spectrum_values
                .as_mut()
                .map(|values| {
                    values.next().flatten().ok_or_else(|| {
                        missing_column_error(&request.output_ms, row_index, "WEIGHT_SPECTRUM")
                    })
                })
                .transpose()?;
            let flag_row = scalar_bool(flag_row.as_ref(), "FLAG_ROW", row_index)?;
            let ddid = scalar_i32(ddid.as_ref(), "DATA_DESC_ID", row_index)?;
            let spw_id = output_ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
                ContinuumSubtractionError::SpectralMetadata {
                    path: request.output_ms.display().to_string(),
                    reason: format!("MAIN row {row_index} references DATA_DESC_ID {ddid}, which has no DATA_DESCRIPTION row"),
                }
            })?;
            let fit = fit_channels_by_spw.get(&spw_id).ok_or_else(|| {
                ContinuumSubtractionError::InvalidFitSpw {
                    selector: request.fit_spw.clone(),
                    reason: format!(
                        "selected row {row_index} maps to spectral window {spw_id}, but fit_spw does not include that SPW"
                    ),
                }
            })?;
            touched_spws.insert(spw_id);
            let (residual, fitted, skipped) = subtract_row(SubtractRowRequest {
                data,
                flags,
                weights,
                weight_spectrum,
                flag_row,
                fit,
                fit_order: request.fit_order,
                row_index,
            })?;
            fitted_row_count += usize::from(fitted);
            skipped_fit_count += skipped;
            residuals.push(residual);
        }
        write_session
            .write_mutation_batch(
                &mut output,
                MeasurementSetMutationBatch {
                    row_indices: rows,
                    columns: vec![MeasurementSetMutationColumnBatch {
                        name: VisibilityDataColumn::Data.name().to_string(),
                        values: MeasurementSetMutationColumnValues::Arrays(residuals),
                    }],
                },
            )
            .map_err(|error| ContinuumSubtractionError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(TableError::Storage(error.to_string())),
            })?;
    }
    write_session.finish_mutation().map_err(|error| {
        ContinuumSubtractionError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(TableError::Storage(error.to_string())),
        }
    })?;

    Ok(ContinuumSubtractionReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count: output.row_count(),
        fitted_row_count,
        skipped_fit_count,
        source_column: request.data_column.name().to_string(),
        output_column: VisibilityDataColumn::Data.name().to_string(),
        fit_spw: request.fit_spw.clone(),
        fit_order: request.fit_order,
        spectral_window_ids: touched_spws.into_iter().collect(),
        elapsed_ns: started_at.elapsed().as_nanos() as u64,
    })
}

fn resolve_fit_channels(
    ms: &MeasurementSet,
    fit_spw: &str,
) -> Result<BTreeMap<i32, FitChannels>, ContinuumSubtractionError> {
    let selectors =
        parse_spw_selector(fit_spw).map_err(|source| ContinuumSubtractionError::InvalidFitSpw {
            selector: fit_spw.to_string(),
            reason: source.to_string(),
        })?;
    let spw =
        ms.spectral_window()
            .map_err(|source| ContinuumSubtractionError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: source.to_string(),
            })?;
    let mut by_spw = BTreeMap::new();
    for selector in selectors {
        if selector.spw_id < 0 {
            return Err(ContinuumSubtractionError::InvalidFitSpw {
                selector: fit_spw.to_string(),
                reason: format!("negative spectral-window id {}", selector.spw_id),
            });
        }
        let row = selector.spw_id as usize;
        if row >= spw.row_count() {
            return Err(ContinuumSubtractionError::InvalidFitSpw {
                selector: fit_spw.to_string(),
                reason: format!(
                    "spectral-window id {} is outside SPECTRAL_WINDOW with {} rows",
                    selector.spw_id,
                    spw.row_count()
                ),
            });
        }
        let frequencies_hz =
            spw.chan_freq(row)
                .map_err(|source| ContinuumSubtractionError::SpectralMetadata {
                    path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                    reason: source.to_string(),
                })?;
        if frequencies_hz.is_empty() {
            return Err(ContinuumSubtractionError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: format!(
                    "spectral-window {} has no channel frequencies",
                    selector.spw_id
                ),
            });
        }
        let indices = match selector.channels {
            Some(ChannelSelection { segments }) => ChannelSelection { segments }
                .indices(frequencies_hz.len())
                .map_err(|source| ContinuumSubtractionError::InvalidFitSpw {
                    selector: fit_spw.to_string(),
                    reason: source.to_string(),
                })?,
            None => (0..frequencies_hz.len()).collect(),
        };
        if indices.is_empty() {
            return Err(ContinuumSubtractionError::InvalidFitSpw {
                selector: fit_spw.to_string(),
                reason: format!(
                    "spectral-window {} selection produced no channels",
                    selector.spw_id
                ),
            });
        }
        by_spw.insert(
            selector.spw_id,
            FitChannels {
                indices,
                frequencies_hz,
            },
        );
    }
    Ok(by_spw)
}

fn data_description_spw_map(
    ms: &MeasurementSet,
) -> Result<BTreeMap<i32, i32>, ContinuumSubtractionError> {
    let data_description =
        ms.data_description()
            .map_err(|source| ContinuumSubtractionError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            })?;
    let mut map = BTreeMap::new();
    for row in 0..data_description.row_count() {
        let spw = data_description.spectral_window_id(row).map_err(|source| {
            ContinuumSubtractionError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            }
        })?;
        map.insert(row as i32, spw);
    }
    Ok(map)
}

struct SubtractRowRequest<'a> {
    data: ArrayValue,
    flags: ArrayValue,
    weights: ArrayValue,
    weight_spectrum: Option<ArrayValue>,
    flag_row: bool,
    fit: &'a FitChannels,
    fit_order: usize,
    row_index: usize,
}

fn subtract_row(
    request: SubtractRowRequest<'_>,
) -> Result<(ArrayValue, bool, usize), ContinuumSubtractionError> {
    let SubtractRowRequest {
        data,
        flags,
        weights,
        weight_spectrum,
        flag_row,
        fit,
        fit_order,
        row_index,
    } = request;
    let ArrayValue::Complex32(values) = data else {
        return Err(ContinuumSubtractionError::SpectralMetadata {
            path: "<measurement-set/DATA>".to_string(),
            reason: format!("row {row_index} DATA must be Complex32 array"),
        });
    };
    let shape = values.shape().to_vec();
    if shape.len() != 2 {
        return Err(ContinuumSubtractionError::SpectralMetadata {
            path: "<measurement-set/DATA>".to_string(),
            reason: format!("row {row_index} DATA must be rank-2 [corr, chan], found {shape:?}"),
        });
    }
    let ArrayValue::Bool(flag_values) = flags else {
        return Err(ContinuumSubtractionError::SpectralMetadata {
            path: "<measurement-set/FLAG>".to_string(),
            reason: format!("row {row_index} FLAG must be Bool array"),
        });
    };
    if flag_values.shape() != values.shape() {
        return Err(ContinuumSubtractionError::SpectralMetadata {
            path: "<measurement-set/FLAG>".to_string(),
            reason: format!(
                "row {row_index} FLAG shape {:?} does not match DATA shape {:?}",
                flag_values.shape(),
                values.shape()
            ),
        });
    }
    let num_corr = shape[0];
    let num_chan = shape[1];
    let ArrayValue::Float32(weight_values) = weights else {
        return Err(ContinuumSubtractionError::SpectralMetadata {
            path: "<measurement-set/WEIGHT>".to_string(),
            reason: format!("row {row_index} WEIGHT must be Float32 array"),
        });
    };
    if weight_values.shape() != [num_corr] {
        return Err(ContinuumSubtractionError::SpectralMetadata {
            path: "<measurement-set/WEIGHT>".to_string(),
            reason: format!(
                "row {row_index} WEIGHT shape {:?} does not match correlation count {num_corr}",
                weight_values.shape()
            ),
        });
    }
    let weight_spectrum_values = match weight_spectrum {
        Some(ArrayValue::Float32(values)) => {
            if values.shape() != [num_corr, num_chan] {
                return Err(ContinuumSubtractionError::SpectralMetadata {
                    path: "<measurement-set/WEIGHT_SPECTRUM>".to_string(),
                    reason: format!(
                        "row {row_index} WEIGHT_SPECTRUM shape {:?} does not match DATA shape {:?}",
                        values.shape(),
                        [num_corr, num_chan]
                    ),
                });
            }
            Some(values)
        }
        Some(_) => {
            return Err(ContinuumSubtractionError::SpectralMetadata {
                path: "<measurement-set/WEIGHT_SPECTRUM>".to_string(),
                reason: format!("row {row_index} WEIGHT_SPECTRUM must be Float32 array"),
            });
        }
        None => None,
    };
    if fit.frequencies_hz.len() != num_chan {
        return Err(ContinuumSubtractionError::SpectralMetadata {
            path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
            reason: format!(
                "row {row_index} DATA has {num_chan} channels but its SPECTRAL_WINDOW has {} frequencies",
                fit.frequencies_hz.len()
            ),
        });
    }

    let mut residual = values;
    if flag_row {
        return Ok((ArrayValue::Complex32(residual), false, num_corr));
    }
    let x_scale = normalized_frequency_axis(&fit.frequencies_hz);
    let mut fitted_any = false;
    let mut skipped = 0usize;
    for corr in 0..num_corr {
        let mut samples = Vec::with_capacity(fit.indices.len());
        for &chan in &fit.indices {
            if chan >= num_chan {
                return Err(ContinuumSubtractionError::InvalidFitSpw {
                    selector: format!("{:?}", fit.indices),
                    reason: format!(
                        "fit channel {chan} exceeds row {row_index} with {num_chan} channels"
                    ),
                });
            }
            if !flag_values[[corr, chan]] {
                let weight = weight_spectrum_values
                    .as_ref()
                    .map(|values| values[[corr, chan]])
                    .unwrap_or_else(|| weight_values[[corr]]);
                if weight.is_finite() && weight > 0.0 {
                    samples.push((x_scale[chan], residual[[corr, chan]], weight as f64));
                }
            }
        }
        if samples.len() < fit_order + 1 {
            skipped += 1;
            continue;
        }
        let real_coeffs = fit_polynomial(
            samples.iter().map(|(x, y, weight)| (*x, y.re as f64, *weight)),
            fit_order,
        )
        .ok_or_else(|| ContinuumSubtractionError::InvalidFitSpw {
            selector: format!("{:?}", fit.indices),
            reason: format!("line-free channels for row {row_index} correlation {corr} are singular for order {fit_order}"),
        })?;
        let imag_coeffs = fit_polynomial(
            samples.iter().map(|(x, y, weight)| (*x, y.im as f64, *weight)),
            fit_order,
        )
        .ok_or_else(|| ContinuumSubtractionError::InvalidFitSpw {
            selector: format!("{:?}", fit.indices),
            reason: format!("line-free channels for row {row_index} correlation {corr} are singular for order {fit_order}"),
        })?;
        for chan in 0..num_chan {
            let continuum = Complex32::new(
                evaluate_polynomial(&real_coeffs, x_scale[chan]) as f32,
                evaluate_polynomial(&imag_coeffs, x_scale[chan]) as f32,
            );
            residual[[corr, chan]] -= continuum;
        }
        fitted_any = true;
    }

    Ok((ArrayValue::Complex32(residual), fitted_any, skipped))
}

fn normalized_frequency_axis(frequencies_hz: &[f64]) -> Vec<f64> {
    let mean = frequencies_hz.iter().sum::<f64>() / frequencies_hz.len() as f64;
    let max_abs = frequencies_hz
        .iter()
        .map(|frequency| (frequency - mean).abs())
        .fold(0.0_f64, f64::max);
    let scale = if max_abs > 0.0 { max_abs } else { 1.0 };
    frequencies_hz
        .iter()
        .map(|frequency| (frequency - mean) / scale)
        .collect()
}

fn fit_polynomial(
    samples: impl IntoIterator<Item = (f64, f64, f64)>,
    order: usize,
) -> Option<Vec<f64>> {
    let size = order + 1;
    let mut rows = Vec::new();
    for (x, y, weight) in samples {
        let mut powers = vec![1.0_f64; size];
        for index in 1..size {
            powers[index] = powers[index - 1] * x;
        }
        rows.push((powers, y, weight));
    }
    solve_weighted_least_squares(&rows, size)
}

fn evaluate_polynomial(coeffs: &[f64], x: f64) -> f64 {
    coeffs
        .iter()
        .rev()
        .fold(0.0_f64, |accumulator, coefficient| {
            accumulator * x + coefficient
        })
}

fn scalar_bool(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<bool, ContinuumSubtractionError> {
    match value {
        Some(ScalarValue::Bool(value)) => Ok(*value),
        _ => Err(ContinuumSubtractionError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn scalar_i32(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<i32, ContinuumSubtractionError> {
    match value {
        Some(ScalarValue::Int32(value)) => Ok(*value),
        _ => Err(ContinuumSubtractionError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn missing_column_error(path: &Path, row_index: usize, column: &str) -> ContinuumSubtractionError {
    ContinuumSubtractionError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(TableError::ColumnNotFound {
            row_index,
            column: column.to_string(),
        }),
    }
}

fn prepare_output_root(path: &Path) -> Result<(), ContinuumSubtractionError> {
    if path.exists() {
        fs::remove_dir_all(path)
            .or_else(|_| fs::remove_file(path))
            .map_err(|error| ContinuumSubtractionError::PrepareOutput {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| ContinuumSubtractionError::PrepareOutput {
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

#[derive(Debug)]
struct FitChannels {
    indices: Vec<usize>,
    frequencies_hz: Vec<f64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::ArrayD;

    #[test]
    fn polynomial_fit_recovers_linear_trend() {
        let coeffs = fit_polynomial(
            [
                (-1.0, 0.0, 1.0),
                (0.0, 2.0, 1.0),
                (1.0, 4.0, 1.0),
                (2.0, 6.0, 1.0),
            ],
            1,
        )
        .unwrap();
        assert!((coeffs[0] - 2.0).abs() < 1.0e-12);
        assert!((coeffs[1] - 2.0).abs() < 1.0e-12);
    }

    #[test]
    fn polynomial_fit_uses_weights() {
        let coeffs = fit_polynomial([(0.0, 0.0, 3.0), (1.0, 10.0, 1.0)], 0).unwrap();
        assert!((coeffs[0] - 2.5).abs() < 1.0e-12);
    }

    #[test]
    fn subtract_row_preserves_line_residual_after_linear_fit() {
        let data = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                vec![1, 5],
                vec![
                    Complex32::new(1.0, 1.0),
                    Complex32::new(2.0, 2.0),
                    Complex32::new(13.0, 3.0),
                    Complex32::new(4.0, 4.0),
                    Complex32::new(5.0, 5.0),
                ],
            )
            .unwrap(),
        );
        let flags = ArrayValue::Bool(ArrayD::from_shape_vec(vec![1, 5], vec![false; 5]).unwrap());
        let weights = ArrayValue::Float32(ArrayD::from_shape_vec(vec![1], vec![1.0]).unwrap());
        let fit = FitChannels {
            indices: vec![0, 1, 3, 4],
            frequencies_hz: vec![1.0, 2.0, 3.0, 4.0, 5.0],
        };
        let (residual, fitted, skipped) = subtract_row(SubtractRowRequest {
            data,
            flags,
            weights,
            weight_spectrum: None,
            flag_row: false,
            fit: &fit,
            fit_order: 1,
            row_index: 0,
        })
        .unwrap();
        let ArrayValue::Complex32(residual) = residual else {
            panic!("expected complex residuals");
        };
        assert!(fitted);
        assert_eq!(skipped, 0);
        assert!(residual[[0, 0]].norm() < 1.0e-5);
        assert!((residual[[0, 2]].re - 10.0).abs() < 1.0e-5);
        assert!(residual[[0, 2]].im.abs() < 1.0e-5);
    }
}
