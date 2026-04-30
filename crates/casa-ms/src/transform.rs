// SPDX-License-Identifier: LGPL-3.0-or-later
//! Tutorial-scoped `mstransform`-style MeasurementSet materialization.
//!
//! CASA routes `mstransform` through the `mstransformer` tool and a chain of
//! TVI layers. This module implements the IRC+10216 tutorial subset needed by
//! downstream line-imaging workflows: row selection plus per-SPW channel
//! selection into a new on-disk MeasurementSet while preserving the standard
//! subtables and updating spectral-window channel metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Instant;

use casa_tables::{Table, TableError, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use ndarray::Axis;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::MsError;
use crate::ms::MeasurementSet;
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

    let input = MeasurementSet::open(&request.input_ms).map_err(|source| {
        MsTransformError::OpenMeasurementSet {
            path: request.input_ms.display().to_string(),
            source,
        }
    })?;
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
    let mut selected_rows =
        request
            .selection
            .apply(&input)
            .map_err(|source| MsTransformError::SelectRows {
                path: request.input_ms.display().to_string(),
                source,
            })?;
    if selected_rows.is_empty() {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    let channel_selection = resolve_transform_channels(&input, &request.spw)?;
    let ddid_to_spw = data_description_spw_map(&input)?;
    let all_input_ddids = input
        .main_table()
        .column_accessor("DATA_DESC_ID")
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.input_ms.display().to_string(),
            source: Box::new(source),
        })?;
    selected_rows = selected_rows
        .into_iter()
        .filter_map(|row_index| {
            let ddid = all_input_ddids.get(row_index)?;
            let ddid = scalar_i32(ddid.as_ref(), "DATA_DESC_ID", row_index).ok()?;
            let spw = ddid_to_spw.get(&ddid)?;
            channel_selection.contains_key(spw).then_some(row_index)
        })
        .collect();
    if selected_rows.is_empty() {
        return Err(MsTransformError::EmptySelection {
            path: request.input_ms.display().to_string(),
        });
    }
    let all_input_times = input
        .main_table()
        .column_accessor("TIME")
        .and_then(|column| column.scalar_cells_owned())
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.input_ms.display().to_string(),
            source: Box::new(source),
        })?;
    selected_rows.sort_by(|left, right| {
        let left_time = all_input_times
            .get(*left)
            .and_then(Option::as_ref)
            .and_then(scalar_f64_value)
            .unwrap_or(f64::NAN);
        let right_time = all_input_times
            .get(*right)
            .and_then(Option::as_ref)
            .and_then(scalar_f64_value)
            .unwrap_or(f64::NAN);
        left_time
            .total_cmp(&right_time)
            .then_with(|| left.cmp(right))
    });
    prepare_output_root(&request.output_ms)?;
    materialize_selected_main_table(&input, &selected_rows, &request.output_ms)?;
    copy_subtables(&request.input_ms, &request.output_ms)?;

    let row_indices = (0..selected_rows.len()).collect::<Vec<_>>();
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
    let uvw_values = input
        .main_table()
        .column_accessor("UVW")
        .and_then(|column| column.array_cells_owned(&selected_rows))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(source),
        })?;
    let data_desc_ids = input
        .main_table()
        .column_accessor("DATA_DESC_ID")
        .and_then(|column| column.scalar_cells_owned_for_rows(&selected_rows))
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
        .zip(data_desc_ids)
    {
        let data =
            data.ok_or_else(|| missing_column_error(&request.output_ms, row_index, source_column))?;
        let flags =
            flags.ok_or_else(|| missing_column_error(&request.output_ms, row_index, "FLAG"))?;
        let ddid = scalar_i32(ddid.as_ref(), "DATA_DESC_ID", row_index)?;
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
        transformed_data.push(Value::Array(select_channels(data, channels).map_err(
            |source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            },
        )?));
        transformed_flags.push(Value::Array(select_channels(flags, channels).map_err(
            |source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            },
        )?));
        if let Some(values) = weight_spectrum_values.as_mut() {
            let weight_spectrum = values.next().flatten().ok_or_else(|| {
                missing_column_error(&request.output_ms, row_index, "WEIGHT_SPECTRUM")
            })?;
            if let Some(transformed) = transformed_weight_spectrum.as_mut() {
                transformed.push(Value::Array(
                    select_channels(weight_spectrum, channels).map_err(|source| {
                        MsTransformError::MutateMeasurementSet {
                            path: request.output_ms.display().to_string(),
                            source: Box::new(source),
                        }
                    })?,
                ));
            }
        }
        touched_spws.insert(spw_id);
    }

    let mut output = MeasurementSet::open(&request.output_ms).map_err(|source| {
        MsTransformError::OpenMeasurementSet {
            path: request.output_ms.display().to_string(),
            source,
        }
    })?;
    output
        .main_table_mut()
        .column_accessor_mut(VisibilityDataColumn::Data.name())
        .and_then(|mut column| column.put(transformed_data))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(source),
        })?;
    output
        .main_table_mut()
        .column_accessor_mut("FLAG")
        .and_then(|mut column| column.put(transformed_flags))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(source),
        })?;
    output
        .main_table_mut()
        .column_accessor_mut("UVW")
        .and_then(|mut column| {
            column.put(
                uvw_values
                    .into_iter()
                    .enumerate()
                    .map(|(row_index, value)| {
                        value
                            .map(Value::Array)
                            .ok_or_else(|| TableError::ColumnNotFound {
                                row_index,
                                column: "UVW".to_string(),
                            })
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        })
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: request.output_ms.display().to_string(),
            source: Box::new(source),
        })?;
    if let Some(transformed) = transformed_weight_spectrum {
        output
            .main_table_mut()
            .column_accessor_mut("WEIGHT_SPECTRUM")
            .and_then(|mut column| column.put(transformed))
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: request.output_ms.display().to_string(),
                source: Box::new(source),
            })?;
    }

    update_spectral_window_metadata(&mut output, &channel_selection, &request.output_ms)?;
    output
        .save_as_assuming_valid(&request.output_ms)
        .map_err(|source| MsTransformError::SaveMeasurementSet {
            path: request.output_ms.display().to_string(),
            source,
        })?;

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

fn materialize_selected_main_table(
    input: &MeasurementSet,
    selected_rows: &[usize],
    output_path: &Path,
) -> Result<(), MsTransformError> {
    input
        .main_table()
        .shallow_copy(TableOptions::new(output_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    let mut main = Table::open(TableOptions::new(output_path)).map_err(|source| {
        MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        }
    })?;
    for &row_index in selected_rows {
        let row = input
            .main_table()
            .row_accessor()
            .row(row_index)
            .map_err(|source| MsTransformError::MutateMeasurementSet {
                path: output_path.display().to_string(),
                source: Box::new(source),
            })?
            .clone();
        main.add_row_assuming_valid(row).map_err(|source| {
            MsTransformError::MutateMeasurementSet {
                path: output_path.display().to_string(),
                source: Box::new(source),
            }
        })?;
    }
    main.save_assuming_valid(TableOptions::new(output_path))
        .map_err(|source| MsTransformError::MutateMeasurementSet {
            path: output_path.display().to_string(),
            source: Box::new(source),
        })?;
    Ok(())
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
            if values.shape()[1] == 0 {
                return Ok(ArrayValue::Complex32(values));
            }
            Ok(ArrayValue::Complex32(values.select(Axis(1), channels)))
        }
        ArrayValue::Bool(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "FLAG arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            if values.shape()[1] == 0 {
                return Ok(ArrayValue::Bool(values));
            }
            Ok(ArrayValue::Bool(values.select(Axis(1), channels)))
        }
        ArrayValue::Float32(values) => {
            if values.ndim() != 2 {
                return Err(TableError::Schema(
                    "WEIGHT_SPECTRUM arrays must be rank-2 [corr, chan]".to_string(),
                ));
            }
            if values.shape()[1] == 0 {
                return Ok(ArrayValue::Float32(values));
            }
            Ok(ArrayValue::Float32(values.select(Axis(1), channels)))
        }
        other => Err(TableError::Schema(format!(
            "unsupported rank-2 channel selection for {:?}",
            other.primitive_type()
        ))),
    }
}

fn update_spectral_window_metadata(
    ms: &mut MeasurementSet,
    channel_selection: &BTreeMap<i32, Vec<usize>>,
    output_ms: &Path,
) -> Result<(), MsTransformError> {
    let spw_path = output_ms.join(SubtableId::SpectralWindow.name());
    let mut spectral_window =
        ms.spectral_window_mut()
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: spw_path.display().to_string(),
                reason: source.to_string(),
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
            .set_i32(row, "NUM_CHAN", channels.len() as i32)
            .map_err(|source| MsTransformError::SpectralMetadata {
                path: spw_path.display().to_string(),
                reason: source.to_string(),
            })?;
        let chan_freq = spectral_window.as_ref().chan_freq(row).map_err(|source| {
            MsTransformError::SpectralMetadata {
                path: spw_path.display().to_string(),
                reason: source.to_string(),
            }
        })?;
        if let (Some(first), Some(last)) = (chan_freq.first(), chan_freq.last()) {
            spectral_window
                .set_f64(row, "REF_FREQUENCY", *first)
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
            let total_bw = if chan_freq.len() > 1 {
                (last - first).abs()
                    + spectral_window
                        .as_ref()
                        .chan_width(row)
                        .ok()
                        .and_then(|widths| widths.first().copied())
                        .unwrap_or(0.0)
                        .abs()
            } else {
                spectral_window
                    .as_ref()
                    .chan_width(row)
                    .ok()
                    .and_then(|widths| widths.first().copied())
                    .unwrap_or(0.0)
                    .abs()
            };
            spectral_window
                .set_f64(row, "TOTAL_BANDWIDTH", total_bw)
                .map_err(|source| MsTransformError::SpectralMetadata {
                    path: spw_path.display().to_string(),
                    reason: source.to_string(),
                })?;
        }
    }
    Ok(())
}

fn update_f64_vector_column(
    spectral_window: &mut crate::subtables::MsSpectralWindowMut<'_>,
    row: usize,
    column: &str,
    channels: &[usize],
    spw_path: &Path,
) -> Result<(), MsTransformError> {
    let values = spectral_window
        .table_mut()
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
        .set_array(
            row,
            column,
            ArrayValue::Float64(values.select(Axis(0), channels)),
        )
        .map_err(|source| MsTransformError::SpectralMetadata {
            path: spw_path.display().to_string(),
            reason: source.to_string(),
        })
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

fn scalar_f64_value(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(value) => Some(*value),
        ScalarValue::Float32(value) => Some(f64::from(*value)),
        _ => None,
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
