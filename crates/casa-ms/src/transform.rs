// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped `mstransform`-style MeasurementSet materialization.
//!
//! CASA routes `mstransform` through the `mstransformer` tool and a chain of
//! TVI layers. This module implements the IRC+10216 tutorial subset needed by
//! downstream line-imaging workflows: row selection plus per-SPW channel
//! selection and channel averaging into a new on-disk MeasurementSet while
//! preserving the standard subtables and updating spectral-window channel
//! metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use casa_tables::{
    ColumnOverrides, ColumnType, StreamedScalarType, StreamedTiledShapeValueType, Table,
    TableError, TableOptions,
};
use casa_types::{ArrayValue, PrimitiveType, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder, Slice};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::MsError;
use crate::ms::MeasurementSet;
use crate::ms::{measurement_set_main_table_bindings, measurement_set_table_options};
use crate::schema::SubtableId;
use crate::schema::main_table::OptionalMainColumn;
use crate::selection::{
    MsReadPlan, MsSelection, MsSelectionError, MsSelectionIoBudget, ResolvedMsSelectionRow,
    RowGrammar, SpwSelection,
};
use crate::write_session::{
    MeasurementSetArrayColumnPlan, MeasurementSetArrayShapePlan, MeasurementSetColumnStorage,
    MeasurementSetCreateTarget, MeasurementSetScalarColumnPlan, MeasurementSetWritePlan,
    MeasurementSetWriteResources, MeasurementSetWriteSession, MeasurementSetWriteTelemetry,
};

mod correlation_metadata;
mod selection_metadata;
mod spectral_metadata;
mod visibility_columns;

use correlation_metadata::{CorrelationSelection, select_visibility_axes};
use visibility_columns::VisibilityColumns;

/// Visibility column layout requested by the transform.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransformDataColumn {
    /// MAIN.DATA.
    Data,
    /// MAIN.CORRECTED_DATA.
    #[default]
    CorrectedData,
    /// Store MAIN.MODEL_DATA in output DATA.
    ModelData,
    /// Copy the three named complex visibility columns.
    DataModelCorrected,
    /// Preserve single-dish FLOAT_DATA.
    FloatData,
    /// Copy both FLOAT_DATA and DATA.
    FloatDataAndData,
    /// Preserve LAG_DATA and its lag axis.
    LagData,
    /// Copy both LAG_DATA and DATA.
    LagDataAndData,
    /// Copy every available physical visibility column.
    All,
}

/// Request for a native tutorial-scoped MeasurementSet transform.
#[derive(Debug, Clone)]
pub struct MsTransformRequest {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// CASA-style SPW/channel selector such as `0:7~58`.
    pub spw: String,
    /// Number of adjacent selected channels to average into each output channel.
    pub width: usize,
    /// Visibility layout to materialize.
    pub data_column: TransformDataColumn,
    /// Structured row selection.
    pub selection: MsSelection,
    /// Preserve fully flagged rows.
    ///
    /// CASA `split(keepflags=False)` drops rows whose `FLAG_ROW` is true or
    /// whose selected `FLAG` cube is fully true.
    pub keep_flags: bool,
}

/// Report returned after materializing a transformed MeasurementSet.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MsTransformReport {
    /// Input MeasurementSet root path.
    pub input_ms: PathBuf,
    /// Output MeasurementSet root path.
    pub output_ms: PathBuf,
    /// Number of MAIN rows in the output.
    pub row_count: usize,
    /// Comma-separated source column names.
    pub source_column: String,
    /// Output visibility data column populated.
    pub output_column: String,
    /// CASA-style SPW/channel selector used for the transform.
    pub spw: String,
    /// Spectral windows represented in the output.
    pub spectral_window_ids: Vec<i32>,
    /// Output channel counts by spectral window.
    pub output_channels_by_spw: BTreeMap<i32, usize>,
    /// Number of adjacent selected channels averaged into each output channel.
    pub width: usize,
    /// End-to-end runtime in nanoseconds.
    pub elapsed_ns: u64,
    /// Canonical bounded-writer telemetry for the output MAIN table.
    pub write_telemetry: MeasurementSetWriteTelemetry,
}

/// Errors returned by [`mstransform`].
#[derive(Debug, Error)]
pub enum MsTransformError {
    /// Opening an MS failed.
    #[error("failed to open MeasurementSet {path}: {source}")]
    OpenMeasurementSet {
        /// Path being opened.
        path: String,
        /// Underlying MS error.
        #[source]
        source: MsError,
    },

    /// Input and output paths are identical.
    #[error("input and output MeasurementSet paths must differ: {path}")]
    SameInputOutput {
        /// Duplicated path.
        path: String,
    },

    /// The output MeasurementSet exists. CASA's `mstransform` and `split` have
    /// no overwrite and refuse it with this message (#931).
    #[error("Output MS {path} already exists - will not overwrite it.")]
    OutputExists {
        /// Output MS path.
        path: String,
    },

    /// The requested source column is absent.
    #[error("MeasurementSet {path} does not contain {column}")]
    MissingDataColumn {
        /// Input MS path.
        path: String,
        /// Missing column.
        column: String,
    },

    /// The input has no visibility column in the requested layout.
    #[error("Did not find and select any data columns.")]
    NoDataColumns,

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
    #[error("MSSelectionNullSelection : The selected table has zero rows.")]
    EmptySelection {
        /// Input MS path.
        path: String,
    },

    /// Parsing or resolving `spw` failed.
    #[error("invalid spectral-window selection {selector:?}: {reason}")]
    InvalidSpw {
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

    /// MAIN or subtable mutation failed.
    #[error("failed to transform MeasurementSet data for {path}: {source}")]
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

    /// Bounded MAIN-column planning or execution failed.
    #[error("failed to write transformed MeasurementSet {path}: {reason}")]
    WriteMeasurementSet {
        /// Output MeasurementSet path.
        path: String,
        /// Physical planning or write failure.
        reason: String,
    },
}

/// Materialize a selected/channel-subset MeasurementSet.
pub fn mstransform(request: &MsTransformRequest) -> Result<MsTransformReport, MsTransformError> {
    materialize_transform(request, RowGrammar::Casa)
}

/// Materialize the selection with the CASA split and mstransform task grammar.
pub fn mstransform_task(
    request: &MsTransformRequest,
) -> Result<MsTransformReport, MsTransformError> {
    materialize_transform(request, RowGrammar::Transform)
}

fn materialize_transform(
    request: &MsTransformRequest,
    grammar: RowGrammar,
) -> Result<MsTransformReport, MsTransformError> {
    if request.input_ms == request.output_ms {
        return Err(MsTransformError::SameInputOutput {
            path: request.input_ms.display().to_string(),
        });
    }
    if request.output_ms.exists() {
        return Err(MsTransformError::OutputExists {
            path: request.output_ms.display().to_string(),
        });
    }
    let target =
        MeasurementSetCreateTarget::prepare(&request.output_ms, false).map_err(|error| {
            MsTransformError::PrepareOutput {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            }
        })?;
    let mut physical_request = request.clone();
    physical_request.output_ms = target.staging_path().to_path_buf();
    let mut report = mstransform_staged(&physical_request, grammar)?;
    target
        .commit()
        .map_err(|error| MsTransformError::PrepareOutput {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    report.output_ms = request.output_ms.clone();
    Ok(report)
}

#[expect(
    clippy::too_many_lines,
    reason = "predates the function-length limit; #705"
)]
fn mstransform_staged(
    request: &MsTransformRequest,
    grammar: RowGrammar,
) -> Result<MsTransformReport, MsTransformError> {
    let started_at = Instant::now();
    tracing::info!(
        input_ms = %request.input_ms.display(),
        output_ms = %request.output_ms.display(),
        spw = %request.spw,
        width = request.width,
        "mstransform started"
    );
    let stage_started_at = Instant::now();
    let input = MeasurementSet::open(&request.input_ms).map_err(|source| {
        MsTransformError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
    maybe_log_transform_progress(
        "open_measurement_set",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let selection_error = |error| MsTransformError::SelectRows {
        path: request.input_ms.display().to_string(),
        source: match error {
            MsSelectionError::Domain(source) => source,
            other => MsError::InvalidInput(other.to_string()),
        },
    };
    let budget = MsSelectionIoBudget::from_system_memory(
        2,
        std::mem::size_of::<ResolvedMsSelectionRow>(),
        None,
    )
    .map_err(selection_error)?;
    let spw = match request.spw.trim() {
        "" => "*",
        spw => spw,
    };
    let spw_selection =
        input
            .resolve_spw_selector(spw)
            .map_err(|source| MsTransformError::InvalidSpw {
                selector: request.spw.clone(),
                reason: source.to_string(),
            })?;
    let visibility =
        VisibilityColumns::resolve(input.main_table(), request.data_column, &request.input_ms)?;
    let mut selection = request.selection.clone();
    if selection.spw.is_none() {
        selection.spw = Some(spw.to_string());
    }
    let correlations = if grammar == RowGrammar::Transform {
        selection
            .correlation
            .take()
            .filter(|text| !text.is_empty())
            .map(|text| CorrelationSelection::resolve(&input, &text))
            .transpose()
            .map_err(|source| selection_error(MsSelectionError::Domain(source)))?
    } else {
        None
    };
    if let Some(correlations) = correlations.as_ref()
        && !correlations
            .constrain(&mut selection)
            .map_err(|source| selection_error(MsSelectionError::Domain(source)))?
    {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    let (resolved_rows, time_selection) = input
        .resolve_selection_with_time(&selection, budget, grammar)
        .map_err(selection_error)?;
    maybe_log_transform_progress(
        "select_rows",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let channel_selection = resolve_transform_channels(&input, &spw_selection, &request.spw)?;
    let channel_bins = channel_bins_by_spw(&channel_selection, request.width)?;
    let ddid_to_spw = data_description_spw_map(&input)?;
    let antenna_ids = selection_metadata::selected_antenna_ids(
        &input,
        selection.antenna.as_deref(),
        &resolved_rows.selected_rows,
    )?;
    let (mut selected_rows, mut selected_ddids): (Vec<_>, Vec<_>) = resolved_rows
        .selected_rows
        .into_iter()
        .filter(|row| {
            spw_selection.selects_row(row.data_desc_id)
                && channel_bins.contains_key(&row.spectral_window_id)
        })
        .map(|row| (row.row_index, row.data_desc_id))
        .unzip();
    maybe_log_transform_progress(
        "filter_rows_by_spw",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    if selected_rows.is_empty() {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    if !request.keep_flags {
        let stage_started_at = Instant::now();
        let flag_row_values = input
            .main_table()
            .column_accessor("FLAG_ROW")
            .and_then(|column| column.scalar_cells_owned_for_rows(&selected_rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let flag_values = input
            .main_table()
            .column_accessor("FLAG")
            .and_then(|column| column.array_cells_owned(&selected_rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.input_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let mut kept_rows = Vec::with_capacity(selected_rows.len());
        let mut kept_ddids = Vec::with_capacity(selected_ddids.len());
        for (((row_index, ddid), flag_row), flags) in selected_rows
            .into_iter()
            .zip(selected_ddids)
            .zip(flag_row_values)
            .zip(flag_values)
        {
            let is_flag_row = scalar_bool(flag_row.as_ref(), "FLAG_ROW", row_index)?;
            let is_fully_flagged = bool_array_all_true(flags.as_ref(), "FLAG", row_index)?;
            if !is_flag_row && !is_fully_flagged {
                kept_rows.push(row_index);
                kept_ddids.push(ddid);
            }
        }
        selected_rows = kept_rows;
        selected_ddids = kept_ddids;
        maybe_log_transform_progress(
            "drop_fully_flagged_rows",
            stage_started_at.elapsed(),
            started_at.elapsed(),
        );
        if selected_rows.is_empty() {
            tracing::warn!(
                input_ms = %request.input_ms.display(),
                "mstransform selected no rows after dropping fully flagged data"
            );
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    let stage_started_at = Instant::now();
    let (selected_rows, selected_ddids) = selection_metadata::sort_selected_rows(
        input.main_table(),
        request,
        selected_rows,
        selected_ddids,
    )?;
    let selected_field_ids_for_metadata =
        selected_field_ids_for_rows(input.main_table(), &selected_rows, &request.output_ms)?;
    maybe_log_transform_progress(
        "sort_selected_rows",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    prepare_output_root(&request.output_ms)?;
    let mut output_main = materialize_empty_main_table(
        &request.input_ms,
        selected_rows.len(),
        &request.output_ms,
        &visibility,
        grammar,
    )?;
    maybe_log_transform_progress(
        "materialize_selected_main",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    copy_subtables(&request.input_ms, &request.output_ms)?;
    maybe_log_transform_progress(
        "copy_subtables",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let compact_metadata = compact_spectral_subtables(
        &request.input_ms,
        &request.output_ms,
        &channel_bins,
        &ddid_to_spw,
        &spw_selection,
        correlations.as_ref(),
    )?;
    if let Some(correlations) = correlations.as_ref() {
        correlations.write_polarizations(request)?;
    }
    let field_id_map = selection_metadata::reindex_fields_and_sources(
        &input,
        request,
        selection.field.as_deref(),
        &compact_metadata.spw_map,
    )?;
    let antenna_id_map = selection_metadata::reindex_antenna_subtables(
        &input,
        request,
        antenna_ids.as_deref(),
        &compact_metadata.spw_map,
        time_selection.as_ref(),
    )?;
    let row_id_maps = selection_metadata::reindex_selected_row_ids(
        &input,
        request,
        antenna_id_map,
        grammar == RowGrammar::Transform,
    )?;
    maybe_log_transform_progress(
        "compact_spectral_subtables",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let row_count = selected_rows.len();
    let write_resources = MeasurementSetWriteResources::from_system_memory(2).map_err(|error| {
        MsTransformError::WriteMeasurementSet {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        }
    })?;
    let (write_plan, input_batch_rows) = transform_array_write_plan(
        &input,
        (&selected_rows, &selected_ddids),
        &ddid_to_spw,
        &channel_bins,
        correlations.as_ref(),
        &visibility,
        write_resources,
    )?;
    let batch_rows = write_plan.batch_rows().min(input_batch_rows);
    let mut write_session = MeasurementSetWriteSession::start(&request.output_ms, write_plan)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    let stage_started_at = Instant::now();
    let mut output_column_overrides = gather_selected_main_column_overrides(
        input.main_table(),
        &selected_rows,
        &request.output_ms,
        batch_rows,
        &mut write_session,
        &row_id_maps,
    )?;
    maybe_log_transform_progress(
        "load_main_column_overrides",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let spectrum_columns = spectrum_column_names(input.main_table());
    let stage_started_at = Instant::now();
    let mut touched_spws = BTreeSet::new();
    for batch_start in (0..row_count).step_by(batch_rows.max(1)) {
        let batch_end = (batch_start + batch_rows.max(1)).min(row_count);
        let input_rows = &selected_rows[batch_start..batch_end];
        let mut data_values = visibility
            .copies()
            .iter()
            .map(|column| {
                input
                    .main_table()
                    .column_accessor(column.source.name())
                    .and_then(|column| column.array_cells_owned_uncached(input_rows))
                    .map(|values| (column, values.into_iter()))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let flag_values = input
            .main_table()
            .column_accessor("FLAG")
            .and_then(|column| column.array_cells_owned_uncached(input_rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let flag_category_values = input
            .main_table()
            .column_accessor("FLAG_CATEGORY")
            .and_then(|column| column.array_cells_owned_uncached(input_rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let uvw_values = input
            .main_table()
            .column_accessor("UVW")
            .and_then(|column| column.array_cells_owned_uncached(input_rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let weight_values = input
            .main_table()
            .column_accessor("WEIGHT")
            .and_then(|column| column.array_cells_owned_uncached(input_rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let sigma_values = input
            .main_table()
            .column_accessor("SIGMA")
            .and_then(|column| column.array_cells_owned_uncached(input_rows))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
        let mut spectrum_values = spectrum_columns
            .iter()
            .map(|&name| {
                input
                    .main_table()
                    .column_accessor(name)
                    .and_then(|column| column.array_cells_owned_uncached(input_rows))
                    .map(|values| (name, values.into_iter()))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut flag_category_values = flag_category_values.into_iter();
        let mut uvw_values = uvw_values.into_iter();
        let mut weight_values = weight_values.into_iter();
        let mut sigma_values = sigma_values.into_iter();
        for (batch_offset, (flags, &ddid)) in flag_values
            .into_iter()
            .zip(&selected_ddids[batch_start..batch_end])
            .enumerate()
        {
            let row_index = batch_start + batch_offset;
            let axes = correlations
                .as_ref()
                .and_then(|selection| selection.axes(ddid));
            let select_correlations = |value, name| {
                select_visibility_axes(value, axes, name).map_err(|source| {
                    MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    }
                })
            };
            let flags =
                flags.ok_or_else(|| missing_column_error(&request.output_ms, row_index, "FLAG"))?;
            let flags = select_correlations(flags, "FLAG")?;
            let spw_id = ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: request.output_ms.display().to_string(),
                    reason: format!("MAIN row {row_index} references DATA_DESC_ID {ddid}, which has no DATA_DESCRIPTION row"),
                }
            })?;
            let bins = channel_bins.get(&spw_id).ok_or_else(|| MsTransformError::InvalidSpw {
                selector: request.spw.clone(),
                reason: format!("selected row {row_index} maps to spectral window {spw_id}, but --spw does not include that SPW"),
            })?;
            for (column, values) in &mut data_values {
                let source = column.source.name();
                let target = column.output.name();
                let Some(value) = values.next().flatten() else {
                    if column.source == OptionalMainColumn::LagData {
                        write_session.push_undefined_row(target).map_err(|error| {
                            MsTransformError::WriteMeasurementSet {
                                path: request.output_ms.display().to_string(),
                                reason: error.to_string(),
                            }
                        })?;
                        continue;
                    }
                    return Err(missing_column_error(&request.output_ms, row_index, source));
                };
                let value = select_correlations(value, source)?;
                let transformed = if column.source == OptionalMainColumn::LagData {
                    value
                } else {
                    transform_data_channels(value, &flags, bins).map_err(|source| {
                        MsTransformError::MutateMeasurementSet {
                            path: request.output_ms.display().to_string(),
                            source: Box::new(source),
                        }
                    })?
                };
                if column.source == OptionalMainColumn::FloatData {
                    push_f32_array_row(
                        &mut write_session,
                        target,
                        &transformed,
                        &request.output_ms,
                        row_index,
                    )?;
                } else {
                    push_complex32_array_row(
                        &mut write_session,
                        target,
                        &transformed,
                        &request.output_ms,
                        row_index,
                    )?;
                }
            }
            let transformed_flags =
                transform_flag_channels(flags.clone(), bins).map_err(|source| {
                    MsTransformError::MutateMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        source: Box::new(source),
                    }
                })?;
            push_bool_array_row(
                &mut write_session,
                "FLAG",
                &transformed_flags,
                &request.output_ms,
                row_index,
            )?;
            if let Some(flag_category) = flag_category_values
                .next()
                .flatten()
                .filter(|value| !value.is_empty())
            {
                let flag_category = select_correlations(flag_category, "FLAG_CATEGORY")?;
                let transformed =
                    transform_flag_category_channels(flag_category, bins).map_err(|source| {
                        MsTransformError::MutateMeasurementSet {
                            path: request.output_ms.display().to_string(),
                            source: Box::new(source),
                        }
                    })?;
                push_bool_array_row(
                    &mut write_session,
                    "FLAG_CATEGORY",
                    &transformed,
                    &request.output_ms,
                    row_index,
                )?;
            } else {
                write_session
                    .push_undefined_row("FLAG_CATEGORY")
                    .map_err(|error| MsTransformError::WriteMeasurementSet {
                        path: request.output_ms.display().to_string(),
                        reason: error.to_string(),
                    })?;
            }
            let uvw = uvw_values
                .next()
                .flatten()
                .ok_or_else(|| missing_column_error(&request.output_ms, row_index, "UVW"))?;
            push_f64_array_row(
                &mut write_session,
                "UVW",
                &uvw,
                &request.output_ms,
                row_index,
            )?;
            let weight = weight_values
                .next()
                .flatten()
                .ok_or_else(|| missing_column_error(&request.output_ms, row_index, "WEIGHT"))?;
            let sigma = sigma_values
                .next()
                .flatten()
                .ok_or_else(|| missing_column_error(&request.output_ms, row_index, "SIGMA"))?;
            let mut weight = select_correlations(weight, "WEIGHT")?;
            let mut sigma = select_correlations(sigma, "SIGMA")?;
            if grammar == RowGrammar::Transform && normalized_width(request.width) == 1 {
                visibility.align_scalar_weights(&mut weight, &mut sigma);
            }
            push_f32_array_row(
                &mut write_session,
                "WEIGHT",
                &weight,
                &request.output_ms,
                row_index,
            )?;
            push_f32_array_row(
                &mut write_session,
                "SIGMA",
                &sigma,
                &request.output_ms,
                row_index,
            )?;
            for (name, values) in &mut spectrum_values {
                if let Some(value) = values.next().flatten().filter(|value| !value.is_empty()) {
                    let value = select_correlations(value, name)?;
                    let transformed = if *name == "SIGMA_SPECTRUM" {
                        transform_sigma_channels(value, &flags, bins)
                    } else {
                        transform_weight_channels(value, &flags, bins)
                    }
                    .map_err(|source| {
                        MsTransformError::MutateMeasurementSet {
                            path: request.output_ms.display().to_string(),
                            source: Box::new(source),
                        }
                    })?;
                    push_f32_array_row(
                        &mut write_session,
                        name,
                        &transformed,
                        &request.output_ms,
                        row_index,
                    )?;
                } else {
                    write_session.push_undefined_row(name).map_err(|error| {
                        MsTransformError::WriteMeasurementSet {
                            path: request.output_ms.display().to_string(),
                            reason: error.to_string(),
                        }
                    })?;
                }
            }
            touched_spws.insert(spw_id);
        }
    }
    maybe_log_transform_progress(
        "select_channels",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let stage_started_at = Instant::now();
    for column in visibility.copies() {
        output_column_overrides.insert_deferred(column.output.name());
    }
    output_column_overrides.insert_deferred("FLAG");
    output_column_overrides.insert_deferred("FLAG_CATEGORY");
    output_column_overrides.insert_deferred("UVW");
    output_column_overrides.insert_deferred("WEIGHT");
    output_column_overrides.insert_deferred("SIGMA");
    for name in spectrum_columns {
        output_column_overrides.insert_deferred(name);
    }
    for ddid in &selected_ddids {
        let compact = compact_metadata
            .ddid_map
            .get(ddid)
            .copied()
            .ok_or_else(|| MsTransformError::SpectralMetadata {
                path: request.output_ms.display().to_string(),
                reason: format!(
                    "selected DATA_DESC_ID {ddid} was not present in compacted metadata"
                ),
            })?;
        write_session
            .push_scalar_row("DATA_DESC_ID", Some(ScalarValue::Int32(compact)))
            .map_err(|error| MsTransformError::WriteMeasurementSet {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    for field_id in &selected_field_ids_for_metadata {
        let compact = match &field_id_map {
            Some(ids) => ids.get(field_id).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: request.output_ms.display().to_string(),
                    reason: format!(
                        "selected FIELD_ID {field_id} was not present in compacted FIELD metadata"
                    ),
                }
            })?,
            None => *field_id,
        };
        write_session
            .push_scalar_row("FIELD_ID", Some(ScalarValue::Int32(compact)))
            .map_err(|error| MsTransformError::WriteMeasurementSet {
                path: request.output_ms.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    let output_bindings = measurement_set_main_table_bindings(&output_main);
    let write_telemetry = write_session
        .save_table_and_finish(
            &mut output_main,
            measurement_set_table_options(&request.output_ms),
            &output_bindings,
            &output_column_overrides,
        )
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: request.output_ms.display().to_string(),
            reason: error.to_string(),
        })?;
    maybe_log_transform_progress(
        "save_output_main",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    if normalized_width(request.width) == 1 {
        spectral_metadata::write_channel_selection(
            &request.output_ms,
            &compact_metadata.spw_map,
            &channel_selection,
        )?;
    } else {
        update_spectral_window_metadata(&compact_metadata.channel_bins, &request.output_ms)?;
    }
    maybe_log_transform_progress(
        "update_spectral_window",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    tracing::info!(
        input_ms = %request.input_ms.display(),
        output_ms = %request.output_ms.display(),
        rows = row_count,
        write_bytes = write_telemetry.bytes_written,
        write_peak_planned_resident_bytes = write_telemetry.maximum_resident_bytes,
        elapsed_ms = started_at.elapsed().as_millis() as u64,
        "mstransform completed"
    );
    Ok(MsTransformReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count,
        source_column: visibility.source_names(),
        output_column: visibility.output_names(),
        spw: request.spw.clone(),
        spectral_window_ids: touched_spws.into_iter().collect(),
        output_channels_by_spw: channel_bins
            .iter()
            .map(|(spw, bins)| (*spw, bins.len()))
            .collect(),
        width: normalized_width(request.width),
        elapsed_ns: started_at.elapsed().as_nanos() as u64,
        write_telemetry,
    })
}

/// Log one completed transform stage (`debug`, target
/// `casa_ms::mstransform_progress`).
fn maybe_log_transform_progress(stage: &str, stage_elapsed: Duration, total_elapsed: Duration) {
    tracing::debug!(
        target: "casa_ms::mstransform_progress",
        "mstransform stage={} stage_elapsed_s={:.3} total_elapsed_s={:.3}",
        stage,
        stage_elapsed.as_secs_f64(),
        total_elapsed.as_secs_f64(),
    );
}

struct TransformArrayDimensions {
    input_correlations: usize,
    input_channels: usize,
    output_correlations: usize,
    output_channels: usize,
}

fn transform_array_dimensions(
    input: &MeasurementSet,
    selected_ddids: &[i32],
    ddid_to_spw: &BTreeMap<i32, i32>,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    correlations: Option<&CorrelationSelection>,
) -> Result<BTreeMap<i32, TransformArrayDimensions>, MsTransformError> {
    let input_path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let invalid = |reason| MsTransformError::SpectralMetadata {
        path: input_path.display().to_string(),
        reason,
    };
    let data_description = input
        .data_description()
        .map_err(|error| invalid(error.to_string()))?;
    let polarization = input
        .polarization()
        .map_err(|error| invalid(error.to_string()))?;
    let spectral_window = input
        .spectral_window()
        .map_err(|error| invalid(error.to_string()))?;
    let mut dimensions = BTreeMap::new();
    for &ddid in selected_ddids {
        let std::collections::btree_map::Entry::Vacant(entry) = dimensions.entry(ddid) else {
            continue;
        };
        let row =
            usize::try_from(ddid).map_err(|_| invalid(format!("negative DATA_DESC_ID {ddid}")))?;
        let pol_id = data_description
            .polarization_id(row)
            .map_err(|error| invalid(format!("read DATA_DESCRIPTION row {ddid}: {error}")))?;
        let pol_row = usize::try_from(pol_id).map_err(|_| {
            invalid(format!(
                "DATA_DESCRIPTION row {ddid} has negative POLARIZATION_ID {pol_id}"
            ))
        })?;
        let input_correlations = usize::try_from(
            polarization
                .num_corr(pol_row)
                .map_err(|error| invalid(format!("read POLARIZATION row {pol_id}: {error}")))?,
        )
        .map_err(|_| invalid(format!("POLARIZATION row {pol_id} has negative NUM_CORR")))?;
        let spw = ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
            invalid(format!(
                "DATA_DESC_ID {ddid} has no spectral-window mapping"
            ))
        })?;
        let output_channels = channel_bins
            .get(&spw)
            .map(Vec::len)
            .ok_or_else(|| invalid(format!("spectral window {spw} has no output channel bins")))?;
        let input_channels = usize::try_from(
            spectral_window
                .num_chan(spw as usize)
                .map_err(|error| invalid(format!("read SPECTRAL_WINDOW row {spw}: {error}")))?,
        )
        .map_err(|_| invalid(format!("SPECTRAL_WINDOW row {spw} has negative NUM_CHAN")))?;
        entry.insert(TransformArrayDimensions {
            input_correlations,
            input_channels,
            output_correlations: correlations
                .and_then(|plan| plan.axes(ddid))
                .map_or(input_correlations, <[usize]>::len),
            output_channels,
        });
    }
    Ok(dimensions)
}

fn transform_array_write_plan(
    input: &MeasurementSet,
    (selected_rows, selected_ddids): (&[usize], &[i32]),
    ddid_to_spw: &BTreeMap<i32, i32>,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    correlations: Option<&CorrelationSelection>,
    visibility: &VisibilityColumns,
    resources: MeasurementSetWriteResources,
) -> Result<(MeasurementSetWritePlan, usize), MsTransformError> {
    let input_path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let dimensions = transform_array_dimensions(
        input,
        selected_ddids,
        ddid_to_spw,
        channel_bins,
        correlations,
    )?;
    let mut shape_counts = BTreeMap::<Vec<usize>, usize>::new();
    let mut weight_shape_counts = BTreeMap::<Vec<usize>, usize>::new();
    for &ddid in selected_ddids {
        let shape = &dimensions[&ddid];
        *shape_counts
            .entry(vec![shape.output_correlations, shape.output_channels])
            .or_default() += 1;
        *weight_shape_counts
            .entry(vec![shape.output_correlations])
            .or_default() += 1;
    }
    let shapes = shape_counts
        .into_iter()
        .map(|(cell_shape, row_count)| MeasurementSetArrayShapePlan {
            tile_shape: crate::ms::casa_visibility_tile_shape(cell_shape[0], cell_shape[1], ""),
            cell_shape,
            row_count,
        })
        .collect::<Vec<_>>();
    let weight_shapes = weight_shape_counts
        .into_iter()
        .map(|(cell_shape, row_count)| {
            let visibility_tile = crate::ms::casa_visibility_tile_shape(cell_shape[0], 1, "");
            MeasurementSetArrayShapePlan {
                tile_shape: crate::ms::casa_weight_tile_shape(&visibility_tile),
                cell_shape,
                row_count,
            }
        })
        .collect::<Vec<_>>();
    let (flag_category_shapes, largest_input_category) = plan_flag_category_shapes(
        input,
        selected_rows,
        selected_ddids,
        &dimensions,
        correlations.is_some(),
    )?;
    let spectrum_columns = spectrum_column_names(input.main_table());
    let io_error = |error: MsSelectionError| MsTransformError::WriteMeasurementSet {
        path: input_path.display().to_string(),
        reason: error.to_string(),
    };
    let (lag_shapes, maximum_lag_bytes) = if visibility.contains_output("LAG_DATA") {
        plan_lag_shapes(input, selected_rows, selected_ddids, &dimensions)?
    } else {
        (Vec::new(), 0)
    };
    let input_batch_rows = transform_input_batch_rows(
        selected_rows.len(),
        resources,
        dimensions.values(),
        spectrum_columns.len(),
        visibility.regular_bytes_per_sample(),
        largest_input_category.saturating_add(maximum_lag_bytes),
    )
    .map_err(io_error)?;
    let visibility_tile = shapes
        .first()
        .map(|shape| shape.tile_shape.clone())
        .unwrap_or_else(|| crate::ms::casa_visibility_tile_shape(1, 1, ""));
    let mut columns = vec![
        MeasurementSetArrayColumnPlan {
            name: "FLAG".to_string(),
            value_type: StreamedTiledShapeValueType::Bool,
            shapes: shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "FLAG_CATEGORY".to_string(),
            value_type: StreamedTiledShapeValueType::Bool,
            shapes: flag_category_shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "UVW".to_string(),
            value_type: StreamedTiledShapeValueType::Float64,
            shapes: vec![MeasurementSetArrayShapePlan {
                cell_shape: vec![3],
                row_count: selected_ddids.len(),
                tile_shape: crate::ms::casa_uvw_tile_shape(&visibility_tile),
            }],
            storage_manager: MeasurementSetColumnStorage::TiledColumn,
        },
        MeasurementSetArrayColumnPlan {
            name: "WEIGHT".to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: weight_shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
        MeasurementSetArrayColumnPlan {
            name: "SIGMA".to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: weight_shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        },
    ];
    for name in spectrum_columns {
        columns.push(MeasurementSetArrayColumnPlan {
            name: name.to_string(),
            value_type: StreamedTiledShapeValueType::Float32,
            shapes: shapes.clone(),
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        });
    }
    for column in visibility.copies() {
        let shapes = if column.source == OptionalMainColumn::LagData {
            lag_shapes.clone()
        } else {
            shapes.clone()
        };
        columns.push(MeasurementSetArrayColumnPlan {
            name: column.output.name().to_string(),
            value_type: column.value_type(),
            shapes,
            storage_manager: MeasurementSetColumnStorage::TiledShape,
        });
    }
    let writer = MeasurementSetWritePlan::variable_array_creation(
        selected_ddids.len(),
        columns,
        transform_scalar_column_plans(input.main_table()),
        resources,
    )
    .map_err(|error| MsTransformError::WriteMeasurementSet {
        path: input_path.display().to_string(),
        reason: error.to_string(),
    })?;
    Ok((writer, input_batch_rows))
}

fn spectrum_column_names(table: &Table) -> Vec<&'static str> {
    let Some(schema) = table.schema() else {
        return Vec::new();
    };
    [
        "WEIGHT_SPECTRUM",
        "SIGMA_SPECTRUM",
        "CORRECTED_WEIGHT_SPECTRUM",
    ]
    .into_iter()
    .filter(|name| schema.contains_column(name))
    .collect()
}

fn transform_input_batch_rows<'a>(
    rows: usize,
    resources: MeasurementSetWriteResources,
    dimensions: impl Iterator<Item = &'a TransformArrayDimensions>,
    spectrum_columns: usize,
    visibility_bytes_per_sample: usize,
    extra_array_bytes: usize,
) -> Result<usize, MsSelectionError> {
    // The input arrays are loaded before slicing, so a small output cell
    // does not imply a small source block. Allow space for array headers too.
    let elements = dimensions
        .map(|shape| {
            shape.input_correlations.saturating_mul(
                shape
                    .input_channels
                    .saturating_mul(1 + visibility_bytes_per_sample + 4 * spectrum_columns)
                    .saturating_add(8),
            )
        })
        .max()
        .unwrap_or(0);
    let overhead = (10 + spectrum_columns) * (std::mem::size_of::<Option<ArrayValue>>() + 32);
    let bytes_per_row = elements
        .saturating_add(24 + overhead)
        .saturating_add(extra_array_bytes);
    let plan = MsReadPlan::new(
        rows,
        MsSelectionIoBudget {
            available_bytes: resources.available_bytes,
            maximum_live_blocks: resources.maximum_live_batches,
            requested_bytes_per_row: bytes_per_row,
            storage_alignment_rows: None,
        },
    )?;
    Ok(plan.rows_per_block)
}

fn plan_flag_category_shapes(
    input: &MeasurementSet,
    selected_rows: &[usize],
    selected_ddids: &[i32],
    dimensions: &BTreeMap<i32, TransformArrayDimensions>,
    correlation_subset: bool,
) -> Result<(Vec<MeasurementSetArrayShapePlan>, usize), MsTransformError> {
    let main = input.main_table();
    let default_categories = match main
        .column_keywords("FLAG_CATEGORY")
        .and_then(|keywords| keywords.get("CATEGORY"))
    {
        Some(Value::Array(categories)) => categories.len().max(1),
        _ => 1,
    };
    let mut counts = BTreeMap::new();
    let mut largest_input = 0;
    for (&row, ddid) in selected_rows.iter().zip(selected_ddids) {
        let original = main
            .array_shape_uncached(row, "FLAG_CATEGORY")
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: input
                    .path()
                    .unwrap_or_else(|| Path::new("<memory>"))
                    .display()
                    .to_string(),
                source: Box::new(source),
            })?;
        let shape = &dimensions[ddid];
        let cell = match original.as_deref() {
            Some(&[corr, chan, categories]) if corr > 0 && chan > 0 && categories > 0 => {
                largest_input =
                    largest_input.max(corr.saturating_mul(chan).saturating_mul(categories));
                [
                    if correlation_subset {
                        shape.output_correlations
                    } else {
                        corr
                    },
                    if chan == 1 { 1 } else { shape.output_channels },
                    categories,
                ]
            }
            _ => [shape.output_correlations, 1, default_categories],
        };
        *counts.entry(cell).or_default() += 1;
    }
    let plans = counts
        .into_iter()
        .map(|([corr, chan, categories], row_count)| {
            let visibility_tile = crate::ms::casa_visibility_tile_shape(corr, chan, "");
            MeasurementSetArrayShapePlan {
                cell_shape: vec![corr, chan, categories],
                row_count,
                tile_shape: vec![
                    visibility_tile[0],
                    visibility_tile[1],
                    1,
                    visibility_tile[2],
                ],
            }
        })
        .collect();
    Ok((plans, largest_input))
}

fn plan_lag_shapes(
    input: &MeasurementSet,
    rows: &[usize],
    descriptions: &[i32],
    dimensions: &BTreeMap<i32, TransformArrayDimensions>,
) -> Result<(Vec<MeasurementSetArrayShapePlan>, usize), MsTransformError> {
    let path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let mut counts = BTreeMap::new();
    let mut max_bytes = 0;
    for (&row, &description) in rows.iter().zip(descriptions) {
        let shape = input
            .main_table()
            .array_shape_uncached(row, "LAG_DATA")
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: path.display().to_string(),
                source: Box::new(source),
            })?;
        let output_correlations = dimensions[&description].output_correlations;
        let lags = match shape.as_deref() {
            Some(&[stored_correlations, lags]) if lags > 0 => {
                max_bytes =
                    max_bytes.max(stored_correlations.saturating_mul(lags).saturating_mul(8));
                lags
            }
            None => 1, // An undefined cell needs no physical lag extent.
            _ => {
                return Err(MsTransformError::SpectralMetadata {
                    path: path.display().to_string(),
                    reason: format!("LAG_DATA row {row} has an invalid shape {shape:?}"),
                });
            }
        };
        *counts.entry([output_correlations, lags]).or_default() += 1;
    }
    let plans = counts
        .into_iter()
        .map(
            |([correlations, lags], row_count)| MeasurementSetArrayShapePlan {
                cell_shape: vec![correlations, lags],
                row_count,
                tile_shape: crate::ms::casa_visibility_tile_shape(correlations, lags, ""),
            },
        )
        .collect();
    Ok((plans, max_bytes))
}

fn push_complex32_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Complex32(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Complex32"),
        });
    };
    if array.ndim() != 2 {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not rank-2"),
        });
    }
    let values = (0..array.shape()[1])
        .flat_map(|channel| {
            (0..array.shape()[0]).map(move |correlation| array[[correlation, channel]])
        })
        .collect::<Vec<_>>();
    session
        .push_complex32_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn push_bool_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Bool(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Bool"),
        });
    };
    let values = array.t().iter().copied().collect::<Vec<_>>();
    session
        .push_bool_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn push_f64_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Float64(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Float64"),
        });
    };
    let values = array.t().iter().copied().collect::<Vec<_>>();
    session
        .push_f64_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn push_f32_array_row(
    session: &mut MeasurementSetWriteSession,
    column: &str,
    value: &ArrayValue,
    output_path: &Path,
    row: usize,
) -> Result<(), MsTransformError> {
    let ArrayValue::Float32(array) = value else {
        return Err(MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: format!("{column} row {row} is not Float32"),
        });
    };
    let values = match array.ndim() {
        1 => array.iter().copied().collect::<Vec<_>>(),
        2 => (0..array.shape()[1])
            .flat_map(|axis1| (0..array.shape()[0]).map(move |axis0| array[[axis0, axis1]]))
            .collect::<Vec<_>>(),
        rank => {
            return Err(MsTransformError::WriteMeasurementSet {
                path: output_path.display().to_string(),
                reason: format!("{column} row {row} has unsupported rank {rank}"),
            });
        }
    };
    session
        .push_f32_row(column, array.shape(), &values)
        .map_err(|error| MsTransformError::WriteMeasurementSet {
            path: output_path.display().to_string(),
            reason: error.to_string(),
        })
}

fn materialize_empty_main_table(
    input_path: &Path,
    row_count: usize,
    output_path: &Path,
    visibility: &VisibilityColumns,
    grammar: RowGrammar,
) -> Result<Table, MsTransformError> {
    let mut table = Table::open_metadata_for_construction(TableOptions::new(input_path), row_count)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    let renamed_keywords = visibility
        .copies()
        .iter()
        .find(|copy| {
            copy.source != copy.output
                && (grammar != RowGrammar::Transform
                    || copy.source == OptionalMainColumn::CorrectedData)
        })
        .and_then(|copy| table.column_keywords(copy.source.name()).cloned());
    let projected_columns = table
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| {
            is_visibility_column(column.name()) && !visibility.contains_output(column.name())
        })
        .map(|column| column.name().to_string())
        .collect::<Vec<_>>();
    for column in projected_columns {
        table
            .remove_column_metadata_for_construction(&column)
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: output_path.display().to_string(),
                source: Box::new(source),
            })?;
    }
    let mut arrays = vec![
        "FLAG",
        "FLAG_CATEGORY",
        "WEIGHT",
        "SIGMA",
        "WEIGHT_SPECTRUM",
        "SIGMA_SPECTRUM",
        "CORRECTED_WEIGHT_SPECTRUM",
    ];
    for copy in visibility.copies() {
        let name = copy.output.name();
        arrays.push(name);
        if !table
            .schema()
            .is_some_and(|schema| schema.contains_column(name))
        {
            table
                .add_column(
                    crate::column_def::column_schema(copy.output.column_def()),
                    None,
                )
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: output_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
        if copy.source != copy.output
            && let Some(source) = &renamed_keywords
        {
            let mut keywords = table.column_keywords(name).cloned().unwrap_or_default();
            for field in source.fields() {
                if grammar != RowGrammar::Transform
                    || matches!(field.value, Value::Scalar(ScalarValue::String(_)))
                {
                    keywords.upsert(field.name.clone(), field.value.clone());
                }
            }
            table.set_column_keywords(name, keywords);
        } else if grammar == RowGrammar::Transform
            && copy.output == OptionalMainColumn::ModelData
            && visibility.copies().len() > 1
        {
            // CASA constructs the independent MODEL_DATA column without input keywords.
            table.set_column_keywords(name, RecordValue::default());
        }
    }
    table
        .widen_array_metadata_for_construction(&arrays)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(table)
}

fn gather_selected_main_column_overrides(
    table: &Table,
    selected_rows: &[usize],
    output_path: &Path,
    batch_rows: usize,
    write_session: &mut MeasurementSetWriteSession,
    row_id_maps: &selection_metadata::RowIdMaps,
) -> Result<ColumnOverrides, MsTransformError> {
    let schema = table
        .schema()
        .ok_or_else(|| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(TableError::Schema(
                "MAIN table is missing schema metadata".to_string(),
            )),
        })?;
    let mut columns = ColumnOverrides::for_row_count(selected_rows.len());
    for column in schema.columns() {
        let column_started_at = Instant::now();
        let name = column.name();
        if is_deferred_visibility_column(name) {
            continue;
        }
        match column.column_type() {
            ColumnType::Scalar => {
                if is_remapped_scalar_column(name) {
                    continue;
                }
                if column.data_type().and_then(streamed_scalar_type).is_some() {
                    let id_map = row_id_maps.for_column(name);
                    let array_selection = row_id_maps.array.as_ref().filter(|_| name == "ARRAY_ID");
                    for rows in selected_rows.chunks(batch_rows.max(1)) {
                        let values = table
                            .column_accessor(name)
                            .and_then(|column| column.scalar_cells_owned_for_rows(rows))
                            .map_err(|source| MsTransformError::MutateMeasurementSet {
                                path: output_path.display().to_string(),
                                source: Box::new(source),
                            })?;
                        for (row, value) in rows.iter().zip(values) {
                            let value = match (id_map, array_selection) {
                                (Some(ids), _) => {
                                    let old_id = scalar_i32(value.as_ref(), name, *row)?;
                                    let new_id = ids.get(&old_id).ok_or_else(|| {
                                        MsTransformError::SpectralMetadata {
                                            path: output_path.display().to_string(),
                                            reason: format!("{name} {old_id} is absent from the selected metadata"),
                                        }
                                    })?;
                                    Some(ScalarValue::Int32(*new_id))
                                }
                                (_, Some(selection)) => {
                                    let old_id = scalar_i32(value.as_ref(), name, *row)?;
                                    let new_id = selection.reindex(old_id).map_err(|source| {
                                        MsTransformError::SpectralMetadata {
                                            path: output_path.display().to_string(),
                                            reason: source.to_string(),
                                        }
                                    })?;
                                    Some(ScalarValue::Int32(new_id))
                                }
                                _ => value,
                            };
                            write_session
                                .push_scalar_row(name, value)
                                .map_err(|source| MsTransformError::WriteMeasurementSet {
                                    path: output_path.display().to_string(),
                                    reason: source.to_string(),
                                })?;
                        }
                    }
                } else {
                    let values = table
                        .column_accessor(name)
                        .and_then(|column| column.scalar_cells_owned_for_rows(selected_rows))
                        .map_err(|source| MsTransformError::MutateMeasurementSet {
                            path: output_path.display().to_string(),
                            source: Box::new(source),
                        })?;
                    columns.insert_values(
                        name.to_string(),
                        values
                            .into_iter()
                            .map(|value| value.map(Value::Scalar))
                            .collect(),
                    );
                }
            }
            ColumnType::Array(_) => {
                let values = table
                    .column_accessor(name)
                    .and_then(|column| column.array_cells_owned(selected_rows))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: output_path.display().to_string(),
                        source: Box::new(source),
                    })?;
                columns.insert_values(
                    name.to_string(),
                    values
                        .into_iter()
                        .map(|value| value.map(Value::Array))
                        .collect(),
                );
            }
            ColumnType::Record => {
                let values = selected_rows
                    .iter()
                    .copied()
                    .map(|input_row| {
                        table
                            .column_accessor(name)
                            .and_then(|column| column.record_cell(input_row))
                            .map(|value| Some(Value::Record(value)))
                            .map_err(|source| MsTransformError::MutateMeasurementSet {
                                path: output_path.display().to_string(),
                                source: Box::new(source),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                columns.insert_values(name.to_string(), values);
            }
        }
        maybe_log_transform_progress(
            &format!("load_main_column_overrides/column/{name}"),
            column_started_at.elapsed(),
            column_started_at.elapsed(),
        );
    }
    Ok(columns)
}

fn streamed_scalar_type(primitive_type: PrimitiveType) -> Option<StreamedScalarType> {
    match primitive_type {
        PrimitiveType::Bool => Some(StreamedScalarType::Bool),
        PrimitiveType::Int32 => Some(StreamedScalarType::Int32),
        PrimitiveType::Float32 => Some(StreamedScalarType::Float32),
        PrimitiveType::Float64 => Some(StreamedScalarType::Float64),
        _ => None,
    }
}

fn transform_scalar_column_plans(table: &Table) -> Vec<MeasurementSetScalarColumnPlan> {
    table
        .schema()
        .into_iter()
        .flat_map(|schema| schema.columns())
        .filter(|column| {
            *column.column_type() == ColumnType::Scalar
                && !is_deferred_visibility_column(column.name())
        })
        .filter_map(|column| {
            Some(MeasurementSetScalarColumnPlan {
                name: column.name().to_string(),
                value_type: streamed_scalar_type(column.data_type()?)?,
            })
        })
        .collect()
}

fn is_remapped_scalar_column(column: &str) -> bool {
    matches!(column, "DATA_DESC_ID" | "FIELD_ID")
}

fn is_deferred_visibility_column(column: &str) -> bool {
    is_visibility_column(column)
        || [
            "FLAG",
            "FLAG_CATEGORY",
            "UVW",
            "WEIGHT",
            "SIGMA",
            "WEIGHT_SPECTRUM",
            "SIGMA_SPECTRUM",
            "CORRECTED_WEIGHT_SPECTRUM",
        ]
        .contains(&column)
}

fn is_visibility_column(column: &str) -> bool {
    matches!(
        column,
        "DATA" | "CORRECTED_DATA" | "MODEL_DATA" | "FLOAT_DATA" | "LAG_DATA"
    )
}

fn copy_subtables(input_path: &Path, output_path: &Path) -> Result<(), MsTransformError> {
    for id in SubtableId::ALL_REQUIRED
        .iter()
        .chain(SubtableId::ALL_OPTIONAL.iter())
    {
        let source = input_path.join(id.name());
        if !source.exists() {
            continue;
        }
        let destination = output_path.join(id.name());
        copy_dir_recursive(&source, &destination).map_err(|source| {
            MsTransformError::PrepareOutput {
                path: output_path.display().to_string(),
                reason: source.to_string(),
            }
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
            #[cfg(unix)]
            std::os::unix::fs::symlink(target, &destination_path)?;
        }
    }
    Ok(())
}

struct CompactSpectralMetadata {
    ddid_map: BTreeMap<i32, i32>,
    spw_map: BTreeMap<i32, i32>,
    channel_bins: BTreeMap<i32, Vec<Vec<usize>>>,
}

fn compact_spectral_subtables(
    input_ms: &Path,
    output_ms: &Path,
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    ddid_to_spw: &BTreeMap<i32, i32>,
    selection: &SpwSelection,
    correlations: Option<&CorrelationSelection>,
) -> Result<CompactSpectralMetadata, MsTransformError> {
    let selected_spws = channel_bins.keys().copied().collect::<Vec<_>>();
    let spw_map = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| (*old_id, new_id as i32))
        .collect::<BTreeMap<_, _>>();
    let compact_channel_bins = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| {
            (
                new_id as i32,
                channel_bins.get(old_id).cloned().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    rewrite_table_rows_with_variable_arrays(
        &input_ms.join(SubtableId::SpectralWindow.name()),
        &output_ms.join(SubtableId::SpectralWindow.name()),
        &selected_spws
            .iter()
            .map(|spw| *spw as usize)
            .collect::<Vec<_>>(),
        &["CHAN_FREQ", "CHAN_WIDTH", "RESOLUTION", "EFFECTIVE_BW"],
        |_, row| Ok(spectral_metadata::remap_associations(row, &spw_map)),
    )?;

    // CASA uses getSPWDDIDList in expression order, including repeated
    // entries. MAIN refers to the last such entry. An omitted spw is '*'.
    let ordered_ddids = selection
        .data_description_ids()
        .iter()
        .map(|&id| id as i32)
        .filter(|&id| correlations.is_none_or(|selection| selection.axes(id).is_some()))
        .collect::<Vec<_>>();
    let ddid_map = ordered_ddids
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| (*old_id, new_id as i32))
        .collect::<BTreeMap<_, _>>();
    rewrite_selected_table_rows(
        &input_ms.join(SubtableId::DataDescription.name()),
        &output_ms.join(SubtableId::DataDescription.name()),
        &ordered_ddids
            .iter()
            .map(|ddid| *ddid as usize)
            .collect::<Vec<_>>(),
        |old_row, mut row| {
            let old_ddid = old_row as i32;
            let old_spw = ddid_to_spw.get(&old_ddid).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: input_ms
                        .join(SubtableId::DataDescription.name())
                        .display()
                        .to_string(),
                    reason: format!("DATA_DESCRIPTION row {old_ddid} has no SPW mapping"),
                }
            })?;
            let new_spw = spw_map.get(&old_spw).copied().ok_or_else(|| {
                MsTransformError::SpectralMetadata {
                    path: output_ms
                        .join(SubtableId::DataDescription.name())
                        .display()
                        .to_string(),
                    reason: format!(
                        "DATA_DESCRIPTION row {old_ddid} references unselected SPW {old_spw}"
                    ),
                }
            })?;
            row.upsert(
                "SPECTRAL_WINDOW_ID",
                Value::Scalar(ScalarValue::Int32(new_spw)),
            );
            Ok(row)
        },
    )?;

    Ok(CompactSpectralMetadata {
        ddid_map,
        spw_map,
        channel_bins: compact_channel_bins,
    })
}

fn selected_field_ids_for_rows(
    table: &Table,
    selected_rows: &[usize],
    output_path: &Path,
) -> Result<Vec<i32>, MsTransformError> {
    table
        .column_accessor("FIELD_ID")
        .and_then(|column| column.scalar_cells_owned_for_rows(selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?
        .into_iter()
        .zip(selected_rows.iter().copied())
        .map(|(value, row_index)| scalar_i32(value.as_ref(), "FIELD_ID", row_index))
        .collect()
}

fn rewrite_selected_table_rows<F>(
    source_path: &Path,
    destination_path: &Path,
    selected_rows: &[usize],
    rewrite: F,
) -> Result<(), MsTransformError>
where
    F: FnMut(usize, RecordValue) -> Result<RecordValue, MsTransformError>,
{
    rewrite_table_rows_with_variable_arrays(
        source_path,
        destination_path,
        selected_rows,
        &[],
        rewrite,
    )
}

fn rewrite_table_rows_with_variable_arrays<F>(
    source_path: &Path,
    destination_path: &Path,
    selected_rows: &[usize],
    variable_arrays: &[&str],
    mut rewrite: F,
) -> Result<(), MsTransformError>
where
    F: FnMut(usize, RecordValue) -> Result<RecordValue, MsTransformError>,
{
    let source = Table::open(TableOptions::new(source_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: source_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    if destination_path.exists() {
        fs::remove_dir_all(destination_path).map_err(|source| MsTransformError::PrepareOutput {
            path: destination_path.display().to_string(),
            reason: source.to_string(),
        })?;
    }
    source
        .shallow_copy(TableOptions::new(destination_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    let mut destination = Table::open(TableOptions::new(destination_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    destination
        .widen_array_metadata_for_construction(variable_arrays)
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    let mut appender = destination.prepare_row_appender().map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for indices in selected_rows.chunks(256) {
        let records =
            selection_metadata::read_subtable_rows(&source, indices).map_err(|source| {
                MsTransformError::MutateMeasurementSet {
                    path: source_path.display().to_string(),
                    source: Box::new(source),
                }
            })?;
        for (&row_index, row) in indices.iter().zip(records) {
            appender
                .append(rewrite(row_index, row)?)
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: destination_path.display().to_string(),
                    source: Box::new(source),
                })?;
        }
    }
    drop(appender);
    destination
        .prepare_write()
        .save(TableOptions::new(destination_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

struct TransformChannelSelection {
    channels: Vec<usize>,
    metadata_ends: Vec<usize>,
}

fn resolve_transform_channels(
    ms: &MeasurementSet,
    selection: &SpwSelection,
    spw: &str,
) -> Result<BTreeMap<i32, TransformChannelSelection>, MsTransformError> {
    let spectral_window =
        ms.spectral_window()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: source.to_string(),
            })?;
    let mut by_spw = BTreeMap::new();
    for row in 0..spectral_window.row_count() {
        if selection.rows_restricted() && !selection.spw_ids().contains(&row) {
            continue;
        }
        let num_chan =
            spectral_window
                .num_chan(row)
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                    reason: source.to_string(),
                })?;
        if num_chan < 0 {
            return Err(MsTransformError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: format!("spectral-window {row} has negative NUM_CHAN {num_chan}",),
            });
        }
        let num_chan = num_chan as usize;
        let mut ranges = selection
            .channel_list()
            .iter()
            .filter(|range| range.spw_id == row)
            .peekable();
        let (channels, metadata_ends) = if ranges.peek().is_some() {
            let mut indices = Vec::new();
            let mut ends = Vec::new();
            for range in ranges {
                let channels = range
                    .channels()
                    .ok_or_else(|| MsTransformError::InvalidSpw {
                        selector: spw.to_string(),
                        reason: format!(
                            "spectral-window {row} range starts before its first channel"
                        ),
                    })?;
                for channel in channels {
                    indices.push(channel);
                    ends.push((channel + range.step.max(1) as usize - 1).min(range.stop as usize));
                }
            }
            (indices, ends)
        } else {
            ((0..num_chan).collect(), (0..num_chan).collect())
        };
        if channels.is_empty() {
            return Err(MsTransformError::InvalidSpw {
                selector: spw.to_string(),
                reason: format!("spectral-window {row} selection produced no channels",),
            });
        }
        by_spw.insert(
            row as i32,
            TransformChannelSelection {
                channels,
                metadata_ends,
            },
        );
    }
    Ok(by_spw)
}

fn normalized_width(width: usize) -> usize {
    width.max(1)
}

fn channel_bins_by_spw(
    channel_selection: &BTreeMap<i32, TransformChannelSelection>,
    width: usize,
) -> Result<BTreeMap<i32, Vec<Vec<usize>>>, MsTransformError> {
    let width = normalized_width(width);
    let mut by_spw = BTreeMap::new();
    for (&spw_id, selection) in channel_selection {
        let bins = selection
            .channels
            .chunks(width)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();
        if bins.is_empty() {
            return Err(MsTransformError::InvalidSpw {
                selector: format!("{spw_id}"),
                reason: format!("spectral-window {spw_id} selection produced no channel bins"),
            });
        }
        by_spw.insert(spw_id, bins);
    }
    Ok(by_spw)
}

fn data_description_spw_map(ms: &MeasurementSet) -> Result<BTreeMap<i32, i32>, MsTransformError> {
    let data_description =
        ms.data_description()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            })?;
    let mut map = BTreeMap::new();
    for row in 0..data_description.row_count() {
        let spw = data_description.spectral_window_id(row).map_err(|source| {
            MsTransformError::SpectralMetadata {
                path: "<measurement-set/DATA_DESCRIPTION>".to_string(),
                reason: source.to_string(),
            }
        })?;
        map.insert(row as i32, spw);
    }
    Ok(map)
}

fn select_channels(value: ArrayValue, channels: &[usize]) -> Result<ArrayValue, TableError> {
    match value {
        ArrayValue::Complex32(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "visibility DATA arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Complex32(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Complex32(values));
                }
                return Ok(ArrayValue::Complex32(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Complex32(values.select(Axis(1), channels)))
        }
        ArrayValue::Bool(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "FLAG arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Bool(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Bool(values));
                }
                return Ok(ArrayValue::Bool(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Bool(values.select(Axis(1), channels)))
        }
        ArrayValue::Float32(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "WEIGHT_SPECTRUM arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            let channel_count = values.shape()[1];
            if channel_count == 0 || all_channels_selected(channels, channel_count) {
                return Ok(ArrayValue::Float32(values));
            }
            if let Some((start, end)) = contiguous_channel_range(channels) {
                if let Some(values) = select_fortran_contiguous_axis1(values.view(), start, end)? {
                    return Ok(ArrayValue::Float32(values));
                }
                return Ok(ArrayValue::Float32(
                    values
                        .slice_axis(Axis(1), Slice::from(start..end))
                        .to_owned(),
                ));
            }
            Ok(ArrayValue::Float32(values.select(Axis(1), channels)))
        }
        other => Err(TableError::Schema(format!(
            "unsupported rank-2 channel selection for {:?}",
            other.primitive_type()
        ))),
    }
}

fn transform_data_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    match value {
        ArrayValue::Complex32(array) => {
            average_visibility(array, flags, bins).map(ArrayValue::Complex32)
        }
        ArrayValue::Float32(array) => {
            average_visibility(array, flags, bins).map(ArrayValue::Float32)
        }
        _ => Err(TableError::Schema(
            "visibility arrays must be Complex32 or Float32".to_string(),
        )),
    }
}

fn average_visibility<T>(
    values: ArrayD<T>,
    flags: &ArrayD<bool>,
    bins: &[Vec<usize>],
) -> Result<ArrayD<T>, TableError>
where
    T: Copy + Default + std::ops::AddAssign + std::ops::Div<f32, Output = T>,
{
    if values.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "DATA and FLAG arrays must have matching rank-2 [corr, chan] shapes".to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::from_elem((corr_count, bins.len()), T::default());
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            let mut sum = T::default();
            let mut count = 0usize;
            for &chan in bin {
                if !flags[[corr, chan]] {
                    sum += values[[corr, chan]];
                    count += 1;
                }
            }
            if count == 0 {
                for &chan in bin {
                    sum += values[[corr, chan]];
                }
                count = bin.len();
            }
            output[[corr, out_chan]] = sum / count as f32;
        }
    }
    Ok(output.into_dyn())
}

fn transform_flag_channels(
    value: ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Bool(values) = value else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 {
        return Err(TableError::Schema(
            "FLAG arrays must be rank-2 [corr, chan]".to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::<bool>::from_elem((corr_count, bins.len()), false);
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            output[[corr, out_chan]] = bin.iter().copied().all(|chan| values[[corr, chan]]);
        }
    }
    Ok(ArrayValue::Bool(output.into_dyn()))
}

fn transform_flag_category_channels(
    value: ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    let ArrayValue::Bool(values) = value else {
        return Err(TableError::Schema(
            "FLAG_CATEGORY arrays must be Bool rank-3 [corr, chan, category]".to_string(),
        ));
    };
    if values.ndim() != 3 {
        return Err(TableError::Schema(
            "FLAG_CATEGORY arrays must be rank-3 [corr, chan, category]".to_string(),
        ));
    }
    let correlation_count = values.shape()[0];
    let channel_count = values.shape()[1];
    let category_count = values.shape()[2];
    if channel_count == 1 {
        return Ok(ArrayValue::Bool(values));
    }
    validate_channel_bins(bins, channel_count)?;
    let mut output = ArrayD::from_elem(
        IxDyn(&[correlation_count, bins.len(), category_count]).f(),
        false,
    );
    for correlation in 0..correlation_count {
        for (output_channel, bin) in bins.iter().enumerate() {
            for category in 0..category_count {
                output[[correlation, output_channel, category]] = bin
                    .iter()
                    .copied()
                    .all(|channel| values[[correlation, channel, category]]);
            }
        }
    }
    Ok(ArrayValue::Bool(output))
}

fn transform_weight_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Float32(values) = value else {
        return Err(TableError::Schema(
            "WEIGHT_SPECTRUM arrays must be Float32 rank-2 [corr, chan]".to_string(),
        ));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 || flags.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "WEIGHT_SPECTRUM and FLAG arrays must have matching rank-2 [corr, chan] shapes"
                .to_string(),
        ));
    }
    let corr_count = values.shape()[0];
    let chan_count = values.shape()[1];
    validate_channel_bins(bins, chan_count)?;
    let mut output = ndarray::Array2::<f32>::zeros((corr_count, bins.len()));
    for corr in 0..corr_count {
        for (out_chan, bin) in bins.iter().enumerate() {
            let mut sum = 0.0f32;
            for &chan in bin {
                if !flags[[corr, chan]] {
                    sum += values[[corr, chan]];
                }
            }
            if sum == 0.0 {
                for &chan in bin {
                    sum += values[[corr, chan]];
                }
            }
            output[[corr, out_chan]] = sum;
        }
    }
    Ok(ArrayValue::Float32(output.into_dyn()))
}

fn transform_sigma_channels(
    value: ArrayValue,
    flags: &ArrayValue,
    bins: &[Vec<usize>],
) -> Result<ArrayValue, TableError> {
    if bins_are_single_channel(bins) {
        let channels = bins.iter().map(|bin| bin[0]).collect::<Vec<_>>();
        return select_channels(value, &channels);
    }
    let ArrayValue::Float32(values) = value else {
        return Err(TableError::Schema(
            "SIGMA_SPECTRUM arrays must be Float32 rank-2 [corr, chan]".to_string(),
        ));
    };
    let ArrayValue::Bool(flags) = flags else {
        return Err(TableError::Schema(
            "FLAG arrays must be Bool rank-2 [corr, chan]".to_string(),
        ));
    };
    if values.ndim() != 2 || flags.ndim() != 2 || values.shape() != flags.shape() {
        return Err(TableError::Schema(
            "SIGMA_SPECTRUM and FLAG arrays must have matching rank-2 shapes".to_string(),
        ));
    }
    validate_channel_bins(bins, values.shape()[1])?;
    let mut output = ndarray::Array2::<f32>::zeros((values.shape()[0], bins.len()));
    for correlation in 0..values.shape()[0] {
        for (output_channel, bin) in bins.iter().enumerate() {
            let mut inverse_variance = 0.0_f64;
            for &channel in bin {
                let sigma = values[[correlation, channel]];
                if !flags[[correlation, channel]] && sigma.is_finite() && sigma > 0.0 {
                    inverse_variance += 1.0 / f64::from(sigma).powi(2);
                }
            }
            output[[correlation, output_channel]] = if inverse_variance > 0.0 {
                (1.0 / inverse_variance.sqrt()) as f32
            } else {
                0.0
            };
        }
    }
    Ok(ArrayValue::Float32(output.into_dyn()))
}

fn bins_are_single_channel(bins: &[Vec<usize>]) -> bool {
    bins.iter().all(|bin| bin.len() == 1)
}

fn validate_channel_bins(bins: &[Vec<usize>], channel_count: usize) -> Result<(), TableError> {
    if bins.is_empty() {
        return Err(TableError::Schema(
            "channel averaging produced no output channels".to_string(),
        ));
    }
    for bin in bins {
        if bin.is_empty() {
            return Err(TableError::Schema(
                "channel averaging bin was empty".to_string(),
            ));
        }
        for &channel in bin {
            if channel >= channel_count {
                return Err(TableError::Schema(format!(
                    "channel {channel} is outside array with {channel_count} channels"
                )));
            }
        }
    }
    Ok(())
}

fn select_fortran_contiguous_axis1<T: Clone>(
    values: ndarray::ArrayViewD<'_, T>,
    start: usize,
    end: usize,
) -> Result<Option<ArrayD<T>>, TableError> {
    let shape = values.shape();
    if shape.len() != 2 || start > end || end > shape[1] {
        return Err(TableError::Schema(format!(
            "channel range {start}..{end} is outside array shape {shape:?}"
        )));
    }
    let corr_count = shape[0];
    if (corr_count > 1 && values.strides()[0] != 1)
        || (shape[1] > 1 && values.strides()[1] != corr_count as isize)
    {
        return Ok(None);
    }
    let Some(input) = values.as_slice_memory_order() else {
        return Ok(None);
    };
    let output_shape = [corr_count, end - start];
    let start_offset = start * corr_count;
    let end_offset = end * corr_count;
    ArrayD::from_shape_vec(
        IxDyn(&output_shape).f(),
        input[start_offset..end_offset].to_vec(),
    )
    .map(Some)
    .map_err(|error| TableError::Schema(format!("channel slice shape mismatch: {error}")))
}

fn contiguous_channel_range(channels: &[usize]) -> Option<(usize, usize)> {
    let (&start, rest) = channels.split_first()?;
    for (offset, &channel) in rest.iter().enumerate() {
        if channel != start + offset + 1 {
            return None;
        }
    }
    Some((start, start + channels.len()))
}

fn all_channels_selected(channels: &[usize], channel_count: usize) -> bool {
    channels.len() == channel_count && channels.iter().copied().enumerate().all(|(i, ch)| ch == i)
}

fn update_spectral_window_metadata(
    channel_bins: &BTreeMap<i32, Vec<Vec<usize>>>,
    output_ms: &Path,
) -> Result<(), MsTransformError> {
    let spw_path = output_ms.join(SubtableId::SpectralWindow.name());
    let mut spectral_window = Table::open(TableOptions::new(&spw_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for (&spw_id, bins) in channel_bins {
        let row = spw_id as usize;
        update_f64_frequency_column(&mut spectral_window, row, "CHAN_FREQ", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "CHAN_WIDTH", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "EFFECTIVE_BW", bins, &spw_path)?;
        update_f64_width_column(&mut spectral_window, row, "RESOLUTION", bins, &spw_path)?;
        spectral_window
            .column_accessor_mut("NUM_CHAN")
            .and_then(|mut column| {
                column.set(row, Value::Scalar(ScalarValue::Int32(bins.len() as i32)))
            })
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: spw_path.display().to_string(),
                reason: source.to_string(),
            })?;
        let chan_freq = f64_vector_cell(&spectral_window, row, "CHAN_FREQ", &spw_path)?;
        if let (Some(first), Some(last)) = (chan_freq.first(), chan_freq.last()) {
            spectral_window
                .column_accessor_mut("REF_FREQUENCY")
                .and_then(|mut column| column.set(row, Value::Scalar(ScalarValue::Float64(*first))))
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
            let total_bw = f64_vector_cell(&spectral_window, row, "CHAN_WIDTH", &spw_path)
                .map(|widths| widths.iter().map(|width| width.abs()).sum())
                .unwrap_or_else(|_| {
                    if chan_freq.len() > 1 {
                        (last - first).abs()
                    } else {
                        0.0
                    }
                });
            spectral_window
                .column_accessor_mut("TOTAL_BANDWIDTH")
                .and_then(|mut column| {
                    column.set(row, Value::Scalar(ScalarValue::Float64(total_bw)))
                })
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
        }
    }
    spectral_window
        .prepare_write()
        .save(TableOptions::new(&spw_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

fn update_f64_frequency_column(
    spectral_window: &mut Table,
    row: usize,
    column: &str,
    bins: &[Vec<usize>],
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    let values = spectral_window
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = values else {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    validate_bins_for_vector(bins, values.len(), column, spw_path)?;
    let averaged = bins
        .iter()
        .map(|bin| {
            bin.iter()
                .copied()
                .map(|channel| values[channel])
                .sum::<f64>()
                / bin.len() as f64
        })
        .collect::<Vec<_>>();
    spectral_window
        .column_accessor_mut(column)
        .and_then(|mut column| {
            column.set(
                row,
                Value::Array(ArrayValue::Float64(
                    ndarray::Array1::from(averaged).into_dyn(),
                )),
            )
        })
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
}

fn update_f64_width_column(
    spectral_window: &mut Table,
    row: usize,
    column: &str,
    bins: &[Vec<usize>],
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    let values = spectral_window
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = values else {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    validate_bins_for_vector(bins, values.len(), column, spw_path)?;
    let averaged = bins
        .iter()
        .map(|bin| {
            bin.iter()
                .copied()
                .map(|channel| values[channel])
                .sum::<f64>()
        })
        .collect::<Vec<_>>();
    spectral_window
        .column_accessor_mut(column)
        .and_then(|mut column| {
            column.set(
                row,
                Value::Array(ArrayValue::Float64(
                    ndarray::Array1::from(averaged).into_dyn(),
                )),
            )
        })
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
}

fn validate_bins_for_vector(
    bins: &[Vec<usize>],
    len: usize,
    column: &str,
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    for bin in bins {
        for &channel in bin {
            if channel >= len {
                return Err(MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: format!("{column} channel {channel} is outside vector length {len}"),
                });
            }
        }
    }
    Ok(())
}

fn f64_vector_cell(
    table: &Table,
    row: usize,
    column: &str,
    path: &Path,
) -> Result<Vec<f64>, MsTransformError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array().cloned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: path.display().to_string(),
            source: Box::new(source),
        })?;
    let ArrayValue::Float64(values) = value else {
        return Err(MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason: format!("{column} must be a Float64 vector"),
        });
    };
    if values.ndim() != 1 {
        return Err(MsTransformError::SpectralMetadata {
            path: path.display().to_string(),
            reason: format!("{column} must be rank-1"),
        });
    }
    Ok(values.iter().copied().collect())
}

fn scalar_i32(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<i32, MsTransformError> {
    match value {
        Some(ScalarValue::Int32(value)) => Ok(*value),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn scalar_bool(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<bool, MsTransformError> {
    match value {
        Some(ScalarValue::Bool(value)) => Ok(*value),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn bool_array_all_true(
    value: Option<&ArrayValue>,
    column: &str,
    row_index: usize,
) -> Result<bool, MsTransformError> {
    match value {
        Some(ArrayValue::Bool(values)) => Ok(values.iter().all(|value| *value)),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn scalar_f64(
    value: Option<&ScalarValue>,
    column: &str,
    row_index: usize,
) -> Result<f64, MsTransformError> {
    match value {
        Some(ScalarValue::Float64(value)) => Ok(*value),
        Some(ScalarValue::Float32(value)) => Ok(f64::from(*value)),
        _ => Err(MsTransformError::MutateMeasurementSet {
            path: "<measurement-set>".to_string(),
            source: Box::new(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }),
    }
}

fn missing_column_error(path: &Path, row_index: usize, column: &str) -> MsTransformError {
    MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(TableError::ColumnNotFound {
            row_index,
            column: column.to_string(),
        }),
    }
}

fn prepare_output_root(path: &Path) -> Result<(), MsTransformError> {
    if path.exists() {
        fs::remove_dir_all(path)
            .or_else(|_| fs::remove_file(path))
            .map_err(|error| MsTransformError::PrepareOutput {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| MsTransformError::PrepareOutput {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use casa_types::Complex32;

    #[test]
    fn select_channels_covers_contiguous_noncontiguous_and_full_width_paths() {
        let complex = ArrayValue::Complex32(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 4]).f(),
                (0..8)
                    .map(|value| Complex32::new(value as f32, value as f32 + 0.5))
                    .collect(),
            )
            .unwrap(),
        );
        let selected = select_channels(complex, &[1, 2]).expect("contiguous selection");
        let ArrayValue::Complex32(selected) = selected else {
            panic!("expected Complex32");
        };
        assert_eq!(selected.shape(), &[2, 2]);
        assert_eq!(selected[[0, 0]], Complex32::new(2.0, 2.5));
        assert_eq!(selected[[1, 1]], Complex32::new(5.0, 5.5));

        let flags = ArrayValue::Bool(
            ArrayD::from_shape_vec(
                IxDyn(&[2, 4]).f(),
                vec![false, true, false, true, true, false, true, false],
            )
            .unwrap(),
        );
        let selected = select_channels(flags, &[0, 3]).expect("non-contiguous selection");
        let ArrayValue::Bool(selected) = selected else {
            panic!("expected Bool");
        };
        assert_eq!(selected.shape(), &[2, 2]);
        assert!(!selected[[0, 0]]);
        assert!(!selected[[1, 1]]);

        let weights = ArrayValue::Float32(
            ArrayD::from_shape_vec(IxDyn(&[1, 3]).f(), vec![1.0, 2.0, 3.0]).unwrap(),
        );
        let selected = select_channels(weights, &[0, 1, 2]).expect("full-width selection");
        let ArrayValue::Float32(selected) = selected else {
            panic!("expected Float32");
        };
        assert_eq!(selected.shape(), &[1, 3]);
        assert_eq!(selected[[0, 2]], 3.0);
    }

    #[test]
    fn select_channels_rejects_wrong_rank_and_unsupported_arrays() {
        let wrong_rank =
            ArrayValue::Bool(ArrayD::from_shape_vec(IxDyn(&[2, 2, 1]), vec![false; 4]).unwrap());
        let err = select_channels(wrong_rank, &[0]).unwrap_err();
        assert!(err.to_string().contains("rank-2"));

        let unsupported =
            ArrayValue::Float64(ArrayD::from_shape_vec(IxDyn(&[2, 2]), vec![0.0; 4]).unwrap());
        let err = select_channels(unsupported, &[0]).unwrap_err();
        assert!(err.to_string().contains("unsupported rank-2"));
    }

    #[test]
    fn scalar_helpers_accept_expected_numeric_types_and_reject_missing_cells() {
        assert_eq!(
            scalar_i32(Some(&ScalarValue::Int32(7)), "DATA_DESC_ID", 3).unwrap(),
            7
        );
        assert_eq!(
            scalar_f64(Some(&ScalarValue::Float64(1.25)), "TIME", 3).unwrap(),
            1.25
        );
        assert_eq!(
            scalar_f64(Some(&ScalarValue::Float32(2.5)), "TIME", 3).unwrap(),
            2.5
        );
        assert!(scalar_i32(None, "DATA_DESC_ID", 3).is_err());
        assert!(scalar_f64(Some(&ScalarValue::Bool(false)), "TIME", 3).is_err());
    }

    #[test]
    fn channel_range_helpers_identify_full_and_contiguous_selections() {
        assert_eq!(contiguous_channel_range(&[2, 3, 4]), Some((2, 5)));
        assert_eq!(contiguous_channel_range(&[2, 4]), None);
        assert!(all_channels_selected(&[0, 1, 2], 3));
        assert!(!all_channels_selected(&[0, 2], 3));
    }
}

#[cfg(test)]
mod array_budget_tests {
    use super::*;

    #[test]
    fn transform_batch_budget_counts_unsliced_input_arrays() {
        let dimensions = [TransformArrayDimensions {
            input_correlations: 4,
            input_channels: 4096,
            output_correlations: 1,
            output_channels: 4096,
        }];
        let resources = MeasurementSetWriteResources {
            available_bytes: 1_200_000,
            maximum_live_batches: 2,
            tiled_column_buffer_bytes: 0,
        };
        let rows = transform_input_batch_rows(100, resources, dimensions.iter(), 2, 8, 32_768)
            .expect("a full input row fits in the read budget");
        assert_eq!(rows, 1);
    }
}
