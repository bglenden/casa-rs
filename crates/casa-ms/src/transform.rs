// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped `mstransform`-style MeasurementSet materialization.
//!
//! CASA routes `mstransform` through the `mstransformer` tool and a chain of
//! TVI layers. This module implements the IRC+10216 tutorial subset needed by
//! downstream line-imaging workflows: row selection plus per-SPW channel
//! selection into a new on-disk MeasurementSet while preserving the standard
//! subtables and updating spectral-window channel metadata.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use casa_tables::{ColumnType, Table, TableError, TableOptions};
use casa_types::{ArrayValue, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder, Slice};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::MsError;
use crate::ms::MeasurementSet;
use crate::ms::{measurement_set_main_table_bindings, measurement_set_table_options};
use crate::schema::SubtableId;
use crate::schema::main_table::VisibilityDataColumn;
use crate::selection::MsSelection;
use crate::selection_syntax::{ChannelSelection, parse_spw_selector};

/// Input visibility column to materialize as output `DATA`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TransformDataColumn {
    /// MAIN.DATA.
    Data,
    /// MAIN.CORRECTED_DATA.
    #[default]
    CorrectedData,
}

impl TransformDataColumn {
    fn source_name(self) -> &'static str {
        match self {
            Self::Data => VisibilityDataColumn::Data.name(),
            Self::CorrectedData => VisibilityDataColumn::CorrectedData.name(),
        }
    }
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
    /// Source visibility data column to copy into output `DATA`.
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
    /// Source column read from the input MS.
    pub source_column: String,
    /// Output visibility data column populated.
    pub output_column: String,
    /// CASA-style SPW/channel selector used for the transform.
    pub spw: String,
    /// Spectral windows represented in the output.
    pub spectral_window_ids: Vec<i32>,
    /// Output channel counts by spectral window.
    pub output_channels_by_spw: BTreeMap<i32, usize>,
    /// End-to-end runtime in nanoseconds.
    pub elapsed_ns: u64,
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

    /// The requested source column is absent.
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
    #[error("mstransform selection produced no rows for {path}")]
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
}

/// Materialize a selected/channel-subset MeasurementSet.
pub fn mstransform(request: &MsTransformRequest) -> Result<MsTransformReport, MsTransformError> {
    let started_at = Instant::now();
    if request.input_ms == request.output_ms {
        return Err(MsTransformError::SameInputOutput {
            path: request.input_ms.display().to_string(),
        });
    }

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
    let source_column = request.data_column.source_name();
    if !input
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(source_column))
    {
        return Err(MsTransformError::MissingDataColumn {
            path: request.input_ms.display().to_string(),
            column: source_column.to_string(),
        });
    }
    let stage_started_at = Instant::now();
    let mut selected_rows =
        request
            .selection
            .apply(&input)
            .map_err(|source| MsTransformError::SelectRows {
                path: request.input_ms.display().to_string(),
                source,
            })?;
    maybe_log_transform_progress(
        "select_rows",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    if selected_rows.is_empty() {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    let stage_started_at = Instant::now();
    let channel_selection = resolve_transform_channels(&input, &request.spw)?;
    let ddid_to_spw = data_description_spw_map(&input)?;
    let raw_ddids = input
        .main_table()
        .column_accessor("DATA_DESC_ID")
        .and_then(|column| column.scalar_cells_owned_for_rows(&selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.input_ms.display().to_string(),
            source: Box::new(source),
        })?;
    let mut filtered_rows = Vec::with_capacity(selected_rows.len());
    let mut filtered_ddids = Vec::with_capacity(selected_rows.len());
    for (row_index, ddid) in selected_rows.into_iter().zip(raw_ddids) {
        let ddid = scalar_i32(ddid.as_ref(), "DATA_DESC_ID", row_index)?;
        let Some(spw) = ddid_to_spw.get(&ddid) else {
            continue;
        };
        if channel_selection.contains_key(spw) {
            filtered_rows.push(row_index);
            filtered_ddids.push(ddid);
        }
    }
    selected_rows = filtered_rows;
    let mut selected_ddids = filtered_ddids;
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
            .zip(selected_ddids.into_iter())
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
            return Err(MsTransformError::EmptySelection {
                path: request.input_ms.display().to_string(),
            });
        }
    }
    let stage_started_at = Instant::now();
    let selected_times = input
        .main_table()
        .column_accessor("TIME")
        .and_then(|column| column.scalar_cells_owned_for_rows(&selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.input_ms.display().to_string(),
            source: Box::new(source),
        })?;
    let mut row_order = selected_rows
        .into_iter()
        .zip(selected_ddids)
        .zip(selected_times)
        .enumerate()
        .map(|(original_index, ((row_index, ddid), time))| {
            scalar_f64(time.as_ref(), "TIME", row_index)
                .map(|time| (time, original_index, row_index, ddid))
        })
        .collect::<Result<Vec<_>, _>>()?;
    row_order.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let mut selected_rows = Vec::with_capacity(row_order.len());
    let mut selected_ddids = Vec::with_capacity(row_order.len());
    for (_time, _original_index, row_index, ddid) in row_order {
        selected_rows.push(row_index);
        selected_ddids.push(ddid);
    }
    let selected_ddids_for_metadata = selected_ddids.clone();
    maybe_log_transform_progress(
        "sort_rows_by_time",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    prepare_output_root(&request.output_ms)?;
    let output_main =
        materialize_empty_main_table(&request.input_ms, selected_rows.len(), &request.output_ms)?;
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
        &channel_selection,
        &selected_ddids_for_metadata,
        &ddid_to_spw,
    )?;
    maybe_log_transform_progress(
        "compact_spectral_subtables",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let row_indices = (0..selected_rows.len()).collect::<Vec<_>>();
    let stage_started_at = Instant::now();
    let mut output_column_overrides = gather_selected_main_column_overrides(
        input.main_table(),
        &selected_rows,
        &request.output_ms,
    )?;
    maybe_log_transform_progress(
        "load_main_column_overrides",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    let source_values = input
        .main_table()
        .column_accessor(source_column)
        .and_then(|column| column.array_cells_owned(&selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(source),
        })?;
    let flag_values = input
        .main_table()
        .column_accessor("FLAG")
        .and_then(|column| column.array_cells_owned(&selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(source),
        })?;
    let weight_spectrum_values = if input
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column("WEIGHT_SPECTRUM"))
    {
        Some(
            input
                .main_table()
                .column_accessor("WEIGHT_SPECTRUM")
                .and_then(|column| column.array_cells_owned(&selected_rows))
                .map_err(|source| MsTransformError::MutateMeasurementSet {
                    path: request.output_ms.display().to_string(),
                    source: Box::new(source),
                })?,
        )
    } else {
        None
    };
    maybe_log_transform_progress(
        "load_visibility_columns",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let stage_started_at = Instant::now();
    let mut touched_spws = BTreeSet::new();
    let mut transformed_data = Vec::with_capacity(row_indices.len());
    let mut transformed_flags = Vec::with_capacity(row_indices.len());
    let mut transformed_weight_spectrum = weight_spectrum_values
        .as_ref()
        .map(|_| Vec::with_capacity(row_indices.len()));
    let mut weight_spectrum_values = weight_spectrum_values.map(Vec::into_iter);
    for (((row_index, data), flags), ddid) in row_indices
        .iter()
        .copied()
        .zip(source_values)
        .zip(flag_values)
        .zip(selected_ddids)
    {
        let data =
            data.ok_or_else(|| missing_column_error(&request.output_ms, row_index, source_column))?;
        let flags =
            flags.ok_or_else(|| missing_column_error(&request.output_ms, row_index, "FLAG"))?;
        let spw_id = ddid_to_spw.get(&ddid).copied().ok_or_else(|| {
            MsTransformError::SpectralMetadata {
                path: request.output_ms.display().to_string(),
                reason: format!("MAIN row {row_index} references DATA_DESC_ID {ddid}, which has no DATA_DESCRIPTION row"),
            }
        })?;
        let channels = channel_selection.get(&spw_id).ok_or_else(|| MsTransformError::InvalidSpw {
            selector: request.spw.clone(),
            reason: format!("selected row {row_index} maps to spectral window {spw_id}, but --spw does not include that SPW"),
        })?;
        transformed_data.push(select_channels(data, channels).map_err(|source| {
            MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            }
        })?);
        transformed_flags.push(select_channels(flags, channels).map_err(|source| {
            MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            }
        })?);
        if let Some(values) = weight_spectrum_values.as_mut() {
            let weight_spectrum = values.next().flatten();
            if let Some(transformed) = transformed_weight_spectrum.as_mut() {
                transformed.push(
                    weight_spectrum
                        .map(|value| {
                            select_channels(value, channels).map_err(|source| {
                                MsTransformError::MutateMeasurementSet {
                                    path: request.output_ms.display().to_string(),
                                    source: Box::new(source),
                                }
                            })
                        })
                        .transpose()?
                        .map(Value::Array),
                );
            }
        }
        touched_spws.insert(spw_id);
    }
    maybe_log_transform_progress(
        "select_channels",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    let stage_started_at = Instant::now();
    output_column_overrides.insert(
        VisibilityDataColumn::Data.name().to_string(),
        transformed_data
            .into_iter()
            .map(|value| Some(Value::Array(value)))
            .collect(),
    );
    output_column_overrides.insert(
        "FLAG".to_string(),
        transformed_flags
            .into_iter()
            .map(|value| Some(Value::Array(value)))
            .collect(),
    );
    if let Some(transformed_weight_spectrum) = transformed_weight_spectrum {
        output_column_overrides.insert(
            "WEIGHT_SPECTRUM".to_string(),
            transformed_weight_spectrum.into_iter().collect(),
        );
    }
    output_column_overrides.insert(
        "DATA_DESC_ID".to_string(),
        selected_ddids_for_metadata
            .iter()
            .map(|ddid| {
                compact_metadata
                    .ddid_map
                    .get(ddid)
                    .copied()
                    .ok_or_else(|| MsTransformError::SpectralMetadata {
                        path: request.output_ms.display().to_string(),
                        reason: format!(
                            "selected DATA_DESC_ID {ddid} was not present in compacted metadata"
                        ),
                    })
                    .map(|compact| Some(Value::Scalar(ScalarValue::Int32(compact))))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    output_main
        .save_with_bindings_and_column_overrides_assuming_valid(
            measurement_set_table_options(&request.output_ms),
            &measurement_set_main_table_bindings(&output_main),
            &output_column_overrides,
        )
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(source),
        })?;
    maybe_log_transform_progress(
        "save_output_main",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );
    let stage_started_at = Instant::now();
    update_spectral_window_metadata(&compact_metadata.channel_selection, &request.output_ms)?;
    maybe_log_transform_progress(
        "update_spectral_window",
        stage_started_at.elapsed(),
        started_at.elapsed(),
    );

    Ok(MsTransformReport {
        input_ms: request.input_ms.clone(),
        output_ms: request.output_ms.clone(),
        row_count: row_indices.len(),
        source_column: source_column.to_string(),
        output_column: VisibilityDataColumn::Data.name().to_string(),
        spw: request.spw.clone(),
        spectral_window_ids: touched_spws.into_iter().collect(),
        output_channels_by_spw: channel_selection
            .iter()
            .map(|(spw, channels)| (*spw, channels.len()))
            .collect(),
        elapsed_ns: started_at.elapsed().as_nanos() as u64,
    })
}

fn transform_progress_enabled() -> bool {
    std::env::var_os("CASA_RS_MSTRANSFORM_PROGRESS").is_some()
}

fn maybe_log_transform_progress(stage: &str, stage_elapsed: Duration, total_elapsed: Duration) {
    if transform_progress_enabled() {
        eprintln!(
            "mstransform stage={} stage_elapsed_s={:.3} total_elapsed_s={:.3}",
            stage,
            stage_elapsed.as_secs_f64(),
            total_elapsed.as_secs_f64(),
        );
    }
}

fn materialize_empty_main_table(
    input_path: &Path,
    row_count: usize,
    output_path: &Path,
) -> Result<Table, MsTransformError> {
    let mut main = Table::open_metadata_only(TableOptions::new(input_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: input_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    main.add_rows_assuming_valid((0..row_count).map(|_| RecordValue::default()))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(main)
}

fn gather_selected_main_column_overrides(
    table: &Table,
    selected_rows: &[usize],
    output_path: &Path,
) -> Result<HashMap<String, Vec<Option<Value>>>, MsTransformError> {
    let schema = table
        .schema()
        .ok_or_else(|| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(TableError::Schema(
                "MAIN table is missing schema metadata".to_string(),
            )),
        })?;
    let mut columns = HashMap::new();
    for column in schema.columns() {
        let column_started_at = Instant::now();
        let name = column.name();
        if is_deferred_visibility_column(name) {
            continue;
        }
        match column.column_type() {
            ColumnType::Scalar => {
                let values = table
                    .column_accessor(name)
                    .and_then(|column| column.scalar_cells_owned_for_rows(selected_rows))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: output_path.display().to_string(),
                        source: Box::new(source),
                    })?;
                columns.insert(
                    name.to_string(),
                    values
                        .into_iter()
                        .map(|value| value.map(Value::Scalar))
                        .collect(),
                );
            }
            ColumnType::Array(_) => {
                let values = table
                    .column_accessor(name)
                    .and_then(|column| column.array_cells_owned(selected_rows))
                    .map_err(|source| MsTransformError::MutateMeasurementSet {
                        path: output_path.display().to_string(),
                        source: Box::new(source),
                    })?;
                columns.insert(
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
                columns.insert(name.to_string(), values);
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

fn is_deferred_visibility_column(column: &str) -> bool {
    [
        "DATA",
        "CORRECTED_DATA",
        "MODEL_DATA",
        "FLOAT_DATA",
        "LAG_DATA",
        "FLAG",
        "WEIGHT_SPECTRUM",
        "SIGMA_SPECTRUM",
        "CORRECTED_WEIGHT_SPECTRUM",
    ]
    .contains(&column)
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
    channel_selection: BTreeMap<i32, Vec<usize>>,
}

fn compact_spectral_subtables(
    input_ms: &Path,
    output_ms: &Path,
    channel_selection: &BTreeMap<i32, Vec<usize>>,
    selected_ddids: &[i32],
    ddid_to_spw: &BTreeMap<i32, i32>,
) -> Result<CompactSpectralMetadata, MsTransformError> {
    let selected_spws = channel_selection.keys().copied().collect::<Vec<_>>();
    let spw_map = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| (*old_id, new_id as i32))
        .collect::<BTreeMap<_, _>>();
    let compact_channel_selection = selected_spws
        .iter()
        .enumerate()
        .map(|(new_id, old_id)| {
            (
                new_id as i32,
                channel_selection.get(old_id).cloned().unwrap_or_default(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    rewrite_selected_table_rows(
        &input_ms.join(SubtableId::SpectralWindow.name()),
        &output_ms.join(SubtableId::SpectralWindow.name()),
        &selected_spws
            .iter()
            .map(|spw| *spw as usize)
            .collect::<Vec<_>>(),
        |_, row| Ok(row),
    )?;

    let mut unique_ddids = selected_ddids.iter().copied().collect::<BTreeSet<_>>();
    let mut ordered_ddids = unique_ddids.iter().copied().collect::<Vec<_>>();
    ordered_ddids.sort_by_key(|ddid| ddid_to_spw.get(ddid).copied().unwrap_or(i32::MAX));
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

    unique_ddids.clear();
    Ok(CompactSpectralMetadata {
        ddid_map,
        channel_selection: compact_channel_selection,
    })
}

fn rewrite_selected_table_rows<F>(
    source_path: &Path,
    destination_path: &Path,
    selected_rows: &[usize],
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
    for &row_index in selected_rows {
        let row = source
            .rows()
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: source_path.display().to_string(),
                source: Box::new(source),
            })?
            .get(row_index)
            .cloned()
            .ok_or_else(|| MsTransformError::SpectralMetadata {
                path: source_path.display().to_string(),
                reason: format!(
                    "requested subtable row {row_index} outside {} rows",
                    source.row_count()
                ),
            })?;
        destination
            .add_row_assuming_valid(rewrite(row_index, row)?)
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: destination_path.display().to_string(),
                source: Box::new(source),
            })?;
    }
    destination
        .save_assuming_valid(TableOptions::new(destination_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: destination_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

fn resolve_transform_channels(
    ms: &MeasurementSet,
    spw: &str,
) -> Result<BTreeMap<i32, Vec<usize>>, MsTransformError> {
    let selectors = parse_spw_selector(spw).map_err(|source| MsTransformError::InvalidSpw {
        selector: spw.to_string(),
        reason: source.to_string(),
    })?;
    let spectral_window =
        ms.spectral_window()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: "<measurement-set/SPECTRAL_WINDOW>".to_string(),
                reason: source.to_string(),
            })?;
    let mut by_spw = BTreeMap::new();
    for selector in selectors {
        if selector.spw_id < 0 {
            return Err(MsTransformError::InvalidSpw {
                selector: spw.to_string(),
                reason: format!("negative spectral-window id {}", selector.spw_id),
            });
        }
        let row = selector.spw_id as usize;
        if row >= spectral_window.row_count() {
            return Err(MsTransformError::InvalidSpw {
                selector: spw.to_string(),
                reason: format!(
                    "spectral-window id {} is outside SPECTRAL_WINDOW with {} rows",
                    selector.spw_id,
                    spectral_window.row_count()
                ),
            });
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
                reason: format!(
                    "spectral-window {} has negative NUM_CHAN {num_chan}",
                    selector.spw_id
                ),
            });
        }
        let num_chan = num_chan as usize;
        let indices = match selector.channels {
            Some(ChannelSelection { segments }) => ChannelSelection { segments }
                .indices(num_chan)
                .map_err(|source| MsTransformError::InvalidSpw {
                    selector: spw.to_string(),
                    reason: source.to_string(),
                })?,
            None => (0..num_chan).collect(),
        };
        if indices.is_empty() {
            return Err(MsTransformError::InvalidSpw {
                selector: spw.to_string(),
                reason: format!(
                    "spectral-window {} selection produced no channels",
                    selector.spw_id
                ),
            });
        }
        by_spw.insert(selector.spw_id, indices);
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
    let Some(input) = values.as_slice_memory_order() else {
        return Ok(None);
    };
    let corr_count = shape[0];
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
    channel_selection: &BTreeMap<i32, Vec<usize>>,
    output_ms: &Path,
) -> Result<(), MsTransformError> {
    let spw_path = output_ms.join(SubtableId::SpectralWindow.name());
    let mut spectral_window = Table::open(TableOptions::new(&spw_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for (&spw_id, channels) in channel_selection {
        let row = spw_id as usize;
        update_f64_vector_column(&mut spectral_window, row, "CHAN_FREQ", channels, &spw_path)?;
        update_f64_vector_column(&mut spectral_window, row, "CHAN_WIDTH", channels, &spw_path)?;
        update_f64_vector_column(
            &mut spectral_window,
            row,
            "EFFECTIVE_BW",
            channels,
            &spw_path,
        )?;
        update_f64_vector_column(&mut spectral_window, row, "RESOLUTION", channels, &spw_path)?;
        spectral_window
            .column_accessor_mut("NUM_CHAN")
            .and_then(|mut column| {
                column.set_scalar_assuming_valid(row, ScalarValue::Int32(channels.len() as i32))
            })
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: spw_path.display().to_string(),
                reason: source.to_string(),
            })?;
        let chan_freq = f64_vector_cell(&spectral_window, row, "CHAN_FREQ", &spw_path)?;
        if let (Some(first), Some(last)) = (chan_freq.first(), chan_freq.last()) {
            spectral_window
                .column_accessor_mut("REF_FREQUENCY")
                .and_then(|mut column| {
                    column.set_scalar_assuming_valid(row, ScalarValue::Float64(*first))
                })
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
            let total_bw = if chan_freq.len() > 1 {
                (last - first).abs()
                    + f64_vector_cell(&spectral_window, row, "CHAN_WIDTH", &spw_path)
                        .ok()
                        .and_then(|widths| widths.first().copied())
                        .unwrap_or(0.0)
                        .abs()
            } else {
                f64_vector_cell(&spectral_window, row, "CHAN_WIDTH", &spw_path)
                    .ok()
                    .and_then(|widths| widths.first().copied())
                    .unwrap_or(0.0)
                    .abs()
            };
            spectral_window
                .column_accessor_mut("TOTAL_BANDWIDTH")
                .and_then(|mut column| {
                    column.set_scalar_assuming_valid(row, ScalarValue::Float64(total_bw))
                })
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
        }
    }
    spectral_window
        .save_assuming_valid(TableOptions::new(&spw_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: spw_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
}

fn update_f64_vector_column(
    spectral_window: &mut Table,
    row: usize,
    column: &str,
    channels: &[usize],
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
    spectral_window
        .column_accessor_mut(column)
        .and_then(|mut column| {
            column.set_array_assuming_valid(
                row,
                ArrayValue::Float64(values.select(Axis(0), channels)),
            )
        })
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
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
